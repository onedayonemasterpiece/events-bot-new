"""Shared fail-closed admission guards for public static-event projections.

These helpers intentionally detect only structural publication hazards. They do
not rewrite event copy or replace LLM-owned decisions about eventness, meaning,
admission, duplicates, or media roles.
"""
from __future__ import annotations

from datetime import date
import re
from typing import Any


PUBLIC_REVIEW_STATUS_MARKERS = {
    "cancelled",
    "deleted",
    "duplicate",
    "review",
    "needs_review",
    "manual_review",
    "eventness_review",
    "quality_review",
    "quarantine",
    "quarantined",
    "rejected",
}
PUBLIC_STATUS_COLUMNS = (
    "lifecycle_status",
    "status",
    "review_status",
    "moderation_status",
    "quality_status",
    "publication_status",
)
PUBLIC_TEXT_FIELDS = ("title", "location_name", "location_address", "city")
PROMPT_OR_CODE_LEAK_RE = re.compile(
    r"```|</?[a-z][^>]*>|(?:^|\s)(?:def|class|function|const|let|var|import)\s+|"
    r"\bselect\s+.+\s+from\b|\{\\?\"[a-z_][a-z0-9_]*\\?\"\s*:|"
    r"\b(?:system|assistant|user)\s*:|\bas an ai\b|"
    r"\b(?:return|output)\s+json\b|"
    r"(?:вот\s+(?:обновлен|обновлён|готовый)|обновл[её]нн\w+\s+текст|"
    r"я\s+не\s+могу|не\s+могу\s+помочь|сформируй\s+|ответь\s+)",
    re.I | re.U,
)
LOCATION_PROSE_START_RE = re.compile(
    r"^(?:в\s+программе|и\s+не\s+забывайте|не\s+забудьте|подробности|"
    r"приходите|жд[её]м|можно\s+будет|вы\s+сможете|здесь|там)\b",
    re.I | re.U,
)
LOCATION_PROSE_VERB_RE = re.compile(
    r"\b(?:будет|будут|можно|сможете|узнаете|приходите|жд[её]м|не\s+забывайте|не\s+забудьте)\b",
    re.I | re.U,
)


def clean_public_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def row_has_key(row: Any, key: str) -> bool:
    try:
        keys = row.keys()
    except Exception:
        return hasattr(row, key)
    return key in keys


def row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        if row_has_key(row, key):
            return row[key]
    except Exception:
        pass
    return getattr(row, key, default)


def normalize_date(value: Any) -> str | None:
    text = clean_public_text(value)
    if not re.fullmatch(r"20\d\d-\d\d-\d\d", text):
        return None
    try:
        date.fromisoformat(text)
    except ValueError:
        return None
    return text


def title_looks_prompt_leak(title: Any) -> bool:
    text = clean_public_text(title).lower()
    if not text:
        return False
    prompt_tokens = (
        "attendee-facing",
        "event_type=",
        "message_date",
        "as-of",
        "rather than",
        "ordered_event_ids",
    )
    if text.startswith("//") and any(token in text for token in prompt_tokens):
        return True
    return sum(1 for token in prompt_tokens if token in text) >= 3


def status_value_blocks_public_projection(value: Any) -> bool:
    status = clean_public_text(value).lower().replace("-", "_")
    if not status:
        return False
    return (
        status in PUBLIC_REVIEW_STATUS_MARKERS
        or status.endswith("_review")
        or status.startswith("review")
        or "quarantine" in status
        or status.startswith("rejected")
    )


def text_value_blocks_public_projection(field: str, value: Any) -> bool:
    text = clean_public_text(value)
    if not text:
        return False
    if PROMPT_OR_CODE_LEAK_RE.search(text):
        return True
    if field in {"location_name", "location_address", "city"}:
        lowered = text.lower()
        if LOCATION_PROSE_START_RE.search(lowered):
            return True
        if len(text) >= 28 and LOCATION_PROSE_VERB_RE.search(lowered):
            return True
        if len(text) >= 70 and re.search(r"[!?…]|[.]\s", text):
            return True
    return False


def public_projection_gate_reason(row: Any) -> str | None:
    """Return why a row must not enter any public static-site projection."""

    if row_has_key(row, "identity_status") and clean_public_text(row_get(row, "identity_status")).lower() != "canonical":
        return "identity_status:not_canonical"
    if row_has_key(row, "merged_into_event_id") and clean_public_text(row_get(row, "merged_into_event_id")).lower() not in {"", "0", "none", "null"}:
        return "merged_into_event_id"
    for field in PUBLIC_STATUS_COLUMNS:
        if row_has_key(row, field) and status_value_blocks_public_projection(row_get(row, field)):
            return f"{field}:blocked_status"
    if not normalize_date(row_get(row, "date")):
        return "date:invalid_iso"
    if row_has_key(row, "end_date") and clean_public_text(row_get(row, "end_date")) and not normalize_date(row_get(row, "end_date")):
        return "end_date:invalid_iso"
    for field in PUBLIC_TEXT_FIELDS:
        if row_has_key(row, field) and text_value_blocks_public_projection(field, row_get(row, field)):
            return f"{field}:leakage"
    if title_looks_prompt_leak(row_get(row, "title")):
        return "title:prompt_leak"
    return None


def public_occurrence_gate_reason(
    row: Any,
    current_date: str,
    current_time: str | None = None,
) -> str | None:
    """Add the exporter's active/non-silent current-or-ongoing occurrence gate."""

    reason = public_projection_gate_reason(row)
    if reason:
        return reason
    if bool(row_get(row, "silent", False)):
        return "silent"
    lifecycle = clean_public_text(row_get(row, "lifecycle_status", "active")).lower() or "active"
    if lifecycle != "active":
        return "lifecycle_status:not_active"
    today = normalize_date(current_date)
    if not today:
        return "current_date:invalid_iso"
    start_date = normalize_date(row_get(row, "date"))
    end_date = normalize_date(row_get(row, "end_date")) if clean_public_text(row_get(row, "end_date")) else None
    start_not_elapsed = bool(start_date and start_date > today)
    if start_date == today:
        raw_time = clean_public_text(row_get(row, "time"))
        start_not_elapsed = not current_time or not raw_time or raw_time[:5] >= current_time
    if start_not_elapsed or bool(end_date and end_date >= today):
        return None
    return "occurrence:elapsed"


def split_current_datetime(value: str | None, fallback_date: str) -> tuple[str, str | None]:
    raw = clean_public_text(value)
    if not raw:
        return fallback_date, None
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(?:[T\s](\d{2}:\d{2}))?", raw)
    if not match:
        return fallback_date, None
    return match.group(1), match.group(2)
