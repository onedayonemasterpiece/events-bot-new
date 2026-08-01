#!/usr/bin/env python3
"""Export a bounded real production event slice into Astro preview JSON fixtures.

The script intentionally keeps the public preview contract small: event pages are
static, discovery manifests are deterministic, and source engagement is already
aggregated into compact counters.
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import copy
import hashlib
import html
import json
import math
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

SCRIPT_PATH = Path(__file__).resolve()
for _candidate in (SCRIPT_PATH.parents[1], SCRIPT_PATH.parents[2] if len(SCRIPT_PATH.parents) > 2 else SCRIPT_PATH.parents[1]):
    if (_candidate / "google_ai").exists() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))

BUILD_TIME_ZONE = ZoneInfo("Europe/Kaliningrad")
CURRENT_DATE_DEFAULT = datetime.now(BUILD_TIME_ZONE).date().isoformat()
EXPECTED_IMAGE_GEOMETRY_MODEL = (
    os.getenv("EVENT_IMAGE_GEOMETRY_MODEL") or "gemma-4-31b-it"
).strip()
EXPECTED_IMAGE_GEOMETRY_PROMPT_VERSION = "event-image-geometry-v1"
CONTROL_EVENT_IDS = [
    5878, 5370, 6093, 6322, 4913, 4512, 5690, 6437, 6438, 3730, 698,
    # pgvector semantic retrieval golden anchors: urban-planning intent must
    # prefer the architecture/urbanism studio over lexical "город" music noise.
    6447, 6310, 5261, 5237,
]
SPARSE_RELATED_ALGORITHM = "event_sparse_related_chain_v1"
SPARSE_RELATED_SCHEMA_VERSION = "event_sparse_related_chain_v1"
SPARSE_RELATED_RETRIEVAL_METHOD = "local_tfidf_sparse_v1"
PGVECTOR_RELATED_ALGORITHM = "event_pgvector_related_chain_v2_two_doc"
PGVECTOR_RELATED_SCHEMA_VERSION = "event_pgvector_related_chain_v2"
PGVECTOR_RELATED_RETRIEVAL_METHOD = "supabase_pgvector_hnsw_cosine_v1"
PGVECTOR_RELATED_CACHE_SCHEMA_VERSION = "event_pgvector_related_chain_v2_cache_20260720_graph_reciprocity"
RELATED_CACHE_SCHEMA_VERSION = "event_sparse_related_chain_v1_cache_20260628b"
BGE_RELATED_ALGORITHM = "event_bge_m3_related_chain_v1"
BGE_RELATED_SCHEMA_VERSION = "event_bge_m3_related_chain_v1"
BGE_RELATED_RETRIEVAL_METHOD = "local_bge_m3_dense_cosine_v1"
BGE_RELATED_CACHE_SCHEMA_VERSION = "event_bge_m3_related_chain_v1_cache_20260727"
BGE_MODEL_ID_DEFAULT = "BAAI/bge-m3"
BGE_MODEL_REVISION_DEFAULT = "5617a9f61b028005a4858fdac845db406aefb181"
BGE_DIMENSION_DEFAULT = 1024
UNUSUAL_MANIFEST_SCHEMA_VERSION = "static_unusual_events_v1"
UNUSUAL_CACHE_SCHEMA_VERSION = "unusual-event-score-cache-v1"
COMPACT_RELATED_RPC = "event_related_candidates_compact_by_event_id_v1"
COMPACT_RELATED_FIELDS = frozenset({"event_id", "vector_similarity"})
DEFAULT_RELATED_RESPONSE_MAX_BYTES = 256 * 1024
DEFAULT_RELATED_TOTAL_RESPONSE_MAX_BYTES = 16 * 1024 * 1024
# Manual QA overrides from event-page media review: these posters contain either no
# meaningful OCR or text too small for OCR-safe preserve mode; crop them as visual.
FORCE_VISUAL_IMAGE_MODE_IDS = {5370, 6322, 4512, 3730, 4913}
EVENT_IMAGE_MEDIA_ROLES = {
    "event_identity_poster",
    "event_photo",
    "attendee_information",
    "program_or_schedule",
    "wayfinding",
    "sponsor_or_brand",
    "unknown_document",
    "unknown_visual",
}
TICKET_LINK_OVERRIDES = {
    # Production row currently points to the organiser chat, while the source
    # registration page is a first-party kgd80 URL provided by product QA.
    5077: "https://kgd80.ru/sobytiya/kaliningrad-i-oblast-kak-kinodekoratsiya-istoriya-semok-hudozhestvennyh-filmov-v-regione/?register=1",
}
IMAGE_URL_OVERRIDES = {
    # The original Tretyakov image URL for this event returns 404; keep preview
    # image loading reliable until the upstream parser backfills a fresh poster.
    5201: "https://kaliningrad.tretyakovgallery.ru/upload/iblock/d42/zgaiwharezzupzkt47zuydl5jtwgi7oe/jjjjjj.jpg",
}
TZ = "Europe/Kaliningrad"

TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i", "й": "y",
    "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u", "ф": "f",
    "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}

EVENT_TYPE_BY_TOPIC = {
    "CONCERTS": "концерт",
    "EXHIBITIONS": "выставка",
    "THEATRE": "театр",
    "MASTERCLASS": "мастер-класс",
    "OPEN_AIR": "на открытом воздухе",
    "FAMILY": "семейное",
    "KIDS_SCHOOL": "детям",
    "FESTIVAL": "фестиваль",
    "KRAEVEDENIE_KALININGRAD_OBLAST": "краеведение",
}

TOPIC_CATEGORY = {
    "CONCERTS": "music",
    "EXHIBITIONS": "exhibition",
    "THEATRE": "theatre",
    "MASTERCLASS": "workshop",
    "OPEN_AIR": "open_air",
    "FAMILY": "family",
    "KIDS_SCHOOL": "kids",
    "FESTIVAL": "festival",
    "KRAEVEDENIE_KALININGRAD_OBLAST": "local_history",
}

STOP_WORDS = {
    "это", "как", "для", "или", "что", "при", "над", "под", "без", "уже", "будет", "будут", "можно",
    "все", "всех", "где", "когда", "чтобы", "если", "также", "после", "перед", "между", "который",
    "которая", "которые", "которых", "событие", "события", "мероприятие", "мероприятия", "калининград",
    "калининграде", "калининградской", "области", "пройдет", "состоится", "начало", "вход", "билеты",
    "город", "города", "городе", "городской", "городская", "наш", "наша", "наше", "нашего", "нашей",
    "нашем", "место", "места", "встреча", "встречи",
}


def log_stage(stage: str, **payload: Any) -> None:
    """Emit compact structured logs for Kaggle/Fly investigations.

    The static-site builder captures stdout/stderr in Kaggle logs, so JSON lines
    with a stable prefix are enough for later grep by stage, event_id, model or
    cache state.
    """
    record = {
        "scope": "static_site.related_chain_builder",
        "stage": stage,
        "ts": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    print(json.dumps(record, ensure_ascii=False, sort_keys=True), file=sys.stderr)


def read_json(value: Any, fallback: Any) -> Any:
    if not value:
        return fallback
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(value)
    except Exception:
        return fallback
    return parsed if parsed is not None else fallback


def clean_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"[\uFE0F\u200D]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def canonical_event_media_cdn_url(value: Any) -> str | None:
    """Return a CDN event-media URL, rejecting every source/origin fallback."""

    url = clean_text(value)
    if not url:
        return None
    try:
        parsed = urllib.parse.urlsplit(url)
        cdn_base = clean_text(
            os.getenv("PUBLIC_ASSET_BASE_URL") or "https://static.kenigevents.ru"
        ).rstrip("/")
        cdn = urllib.parse.urlsplit(cdn_base)
    except Exception:
        return None
    if parsed.scheme not in {"http", "https"} or not cdn.netloc:
        return None
    if parsed.netloc.lower() == cdn.netloc.lower():
        return urllib.parse.urlunsplit(
            (cdn.scheme or "https", cdn.netloc, parsed.path, parsed.query, parsed.fragment)
        )
    current_bucket_prefix = "/kenigevents.ru/"
    if (
        parsed.netloc.lower() == "storage.yandexcloud.net"
        and parsed.path.startswith(current_bucket_prefix)
    ):
        object_path = parsed.path[len(current_bucket_prefix) :]
        return urllib.parse.urlunsplit(
            (
                cdn.scheme or "https",
                cdn.netloc,
                "/" + object_path,
                parsed.query,
                parsed.fragment,
            )
        )
    return None


def description_looks_truncated(description: str, source_text: str) -> bool:
    """Detect an incomplete LLM-written description and safely fall back to source text.

    This guard does not rewrite meaning; it only prevents an obviously cut-off
    generated field from replacing the fuller source fact text on static pages.
    """
    text = clean_text(description)
    source = clean_text(source_text)
    if not text or not source or len(source) <= len(text) + 80:
        return False
    if len(text) < 320 and not re.search(r"[.!?…»)]$", text):
        return True
    tail = text[-32:].lower()
    if re.search(r"\b[а-яёa-z]{1,3}$", tail, re.I) and not re.search(r"[.!?…»)]$", text):
        return True
    return False


def lead_excerpt(description: str, *, soft_limit: int = 320, hard_limit: int = 520) -> str:
    """Return a sentence-safe lead without inventing or completing source prose.

    Static event cards used to take ``description[:260]``.  That can turn a
    perfectly complete LLM description into a grammatically false sentence by
    appending a full stop at the cut boundary.  Prefer the first complete
    source sentence; when the source itself has no bounded sentence, disclose
    the excerpt with an ellipsis at a word boundary.
    """

    text = str(description or "").strip()
    if not text:
        return ""
    # Leads are prose, not Markdown section markers or blockquote chrome.
    text = re.sub(r"^(?:#{1,6}|>)\s*", "", text)
    text = clean_text(text)
    if not text:
        return ""
    for match in re.finditer(r"[.!?…](?=\s|$)", text):
        end = match.end()
        if end >= 48 and end <= hard_limit:
            return text[:end].strip()
        if end > hard_limit:
            break
    if len(text) <= soft_limit:
        return text
    prefix = text[:soft_limit + 1]
    boundary = prefix.rfind(" ")
    if boundary < max(48, soft_limit // 2):
        boundary = soft_limit
    return text[:boundary].rstrip(" ,;:.-") + "…"


def summary_is_false_terminal_prefix(summary: str, description: str) -> bool:
    """Detect a summary that is merely a punctuated cut of the description."""

    short = clean_text(summary)
    full = clean_text(description)
    stem = short.rstrip(".!?… ")
    if not stem or len(full) <= len(stem) or not full.startswith(stem):
        return False
    continuation = full[len(stem):].lstrip()
    return bool(continuation and continuation[0].islower())


def event_summary(short_description: Any, description: str) -> str:
    """Keep an authored short description unless it is a proven false cut."""

    short = clean_text(short_description)
    if short and not summary_is_false_terminal_prefix(short, description):
        return short
    return lead_excerpt(description)


STRUCTURED_SOURCE_TYPE_HEADINGS = {
    "о спектакле": "спектакль",
    "о концерте": "концерт",
    "о выставке": "выставка",
    "об экскурсии": "экскурсия",
    "о лекции": "лекция",
    "о мастер-классе": "мастер-класс",
}


def structured_occurrence_projection(row: sqlite3.Row) -> dict[str, str] | None:
    """Project an exact structured source occurrence when canonical fields conflict.

    This is deliberately a narrow source-consistency guard, not a keyword
    classifier.  It activates only for a structured first-party source whose
    date, time and ticket URL exactly match the canonical row.  The explicit
    ``О спектакле``/equivalent heading is source data, so using it cannot invent
    event meaning.
    """

    source = str(row_get(row, "source_text") or "").strip()
    if not source or "Описание:" not in source:
        return None
    fields: dict[str, str] = {}
    for label, key in (("Название", "title"), ("Дата", "date"), ("Время", "time"), ("Ссылка", "link")):
        match = re.search(rf"(?m)^{label}:\s*(.+?)\s*$", source)
        if not match:
            return None
        fields[key] = clean_text(match.group(1))
    row_date = normalize_date(row_get(row, "date")) or clean_text(row_get(row, "date"))
    row_time, _end_time, _display_time = split_time(row_get(row, "time"))
    row_link = clean_text(row_get(row, "ticket_link")).rstrip("/")
    if fields["date"] != row_date or fields["time"] != (row_time or ""):
        return None
    if not row_link or fields["link"].rstrip("/") != row_link:
        return None
    description = source.split("Описание:", 1)[1].strip()
    lines = [line.strip() for line in description.splitlines()]
    while lines and not lines[0]:
        lines.pop(0)
    heading = clean_text(lines[0]).lower() if lines else ""
    source_type = STRUCTURED_SOURCE_TYPE_HEADINGS.get(heading)
    if not source_type:
        return None
    lines = lines[1:]
    while lines and not lines[-1]:
        lines.pop()
    if lines and re.match(r"^Сцена:\s*", lines[-1], flags=re.I):
        lines.pop()
    description = "\n".join(lines).strip()
    if not fields["title"] or not description:
        return None
    return {**fields, "event_type": source_type, "description": description}


def source_event_type_conflicts(stored_type: Any, source_type: str) -> bool:
    stored = clean_text(stored_type).lower()
    if not stored:
        return False
    compatible = {
        "спектакль": {"спектакль", "театр", "театральное событие"},
        "концерт": {"концерт", "музыка", "музыкальное событие"},
        "выставка": {"выставка", "экспозиция"},
        "экскурсия": {"экскурсия", "тур"},
        "лекция": {"лекция"},
        "мастер-класс": {"мастер-класс"},
    }
    return stored not in compatible.get(source_type, {source_type})


def title_looks_prompt_leak(title: str) -> bool:
    """Skip obvious prompt/debug leakage before building public static pages.

    This is a narrow safety guard: it does not infer event semantics or repair
    titles. Rows caught here must be fixed at the source/Smart Update layer,
    but a preview export must not publish raw prompt-control text as an event.
    """
    text = clean_text(title).lower()
    if not text:
        return False
    prompt_tokens = [
        "attendee-facing",
        "event_type=",
        "message_date",
        "as-of",
        "rather than",
        "ordered_event_ids",
    ]
    if text.startswith("//") and any(token in text for token in prompt_tokens):
        return True
    return sum(1 for token in prompt_tokens if token in text) >= 3


def row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def event_organizer_names(value: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for raw_name in read_json(value, []):
        name = clean_text(raw_name)
        key = name.casefold()
        if not name or key in seen:
            continue
        seen.add(key)
        names.append(name)
        if len(names) >= 8:
            break
    return names


def event_age_projection(row: sqlite3.Row) -> dict[str, Any]:
    """Project stored fields only; never infer an age from rendered prose."""

    valid_age_values = {"0+", "6+", "12+", "16+", "18+"}
    age_status = clean_text(row_get(row, "age_restriction_status")) or "unknown"
    declared_age_raw = clean_text(row_get(row, "age_restriction"))
    declared_age = (
        declared_age_raw
        if declared_age_raw in valid_age_values and age_status == "declared"
        else None
    )
    public_age_policy = (os.getenv("STATIC_EVENT_AGE_POLICY") or "declared_only").strip().lower()
    assessed_age_raw = clean_text(row_get(row, "age_assessment"))
    assessed_age = assessed_age_raw if assessed_age_raw in valid_age_values else None
    age_recommendation = (
        assessed_age
        if public_age_policy == "declared_or_assessed_labeled" and not declared_age
        else None
    )
    return {
        "age_restriction": declared_age,
        "age_restriction_status": "declared" if declared_age else age_status,
        "age_restriction_provenance": (
            clean_text(row_get(row, "age_restriction_provenance")) if declared_age else None
        ),
        "age_restriction_decision_version": clean_text(
            row_get(row, "age_restriction_decision_version")
        ) or None,
        "age_recommendation": age_recommendation,
        "age_recommendation_label": (
            f"Рекомендуемый возраст: {age_recommendation} — оценка сервиса"
            if age_recommendation
            else None
        ),
    }


def strip_emoji_prefix(value: str) -> str:
    value = re.sub(r"^[^\wА-Яа-яЁё0-9\"«]+", "", value or "").strip()
    return clean_text(value)


def clean_place(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    text = re.sub(r"(?:^|[\s,;])#[\wА-Яа-яЁё-]+", "", text).strip(" ,;–—")
    return clean_text(text) or None


def drop_city_only_venue(venue: str | None, city: str | None) -> str | None:
    """Keep an honest city fallback without pretending that it is a venue."""
    venue_key = re.sub(r"\s+", " ", str(venue or "")).strip(" #,;–—").casefold()
    city_key = re.sub(r"\s+", " ", str(city or "")).strip(" #,;–—").casefold()
    if venue_key and city_key and venue_key == city_key:
        return None
    return venue


def slugify(value: str, fallback: str = "event") -> str:
    out = []
    for ch in value.lower():
        if ch in TRANSLIT:
            out.append(TRANSLIT[ch])
        elif ch.isascii() and ch.isalnum():
            out.append(ch)
        else:
            out.append("-")
    slug = re.sub(r"-+", "-", "".join(out)).strip("-")
    return slug[:74].strip("-") or fallback


def normalize_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not re.fullmatch(r"20\d\d-\d\d-\d\d", text):
        return None
    try:
        date.fromisoformat(text)
    except ValueError:
        return None
    return text


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


def row_has_key(row: sqlite3.Row, key: str) -> bool:
    try:
        return key in row.keys()
    except Exception:
        return False


def status_value_blocks_public_projection(value: Any) -> bool:
    status = clean_text(value).lower().replace("-", "_")
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
    text = clean_text(value)
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


def public_projection_gate_reason(row: sqlite3.Row) -> str | None:
    """Return why an event row must not enter public static-site projection."""
    if row_has_key(row, "identity_status") and clean_text(row_get(row, "identity_status")).lower() != "canonical":
        return "identity_status:not_canonical"
    if row_has_key(row, "merged_into_event_id") and clean_text(row_get(row, "merged_into_event_id")) not in {"", "0", "none", "null"}:
        return "merged_into_event_id"
    for field in PUBLIC_STATUS_COLUMNS:
        if row_has_key(row, field) and status_value_blocks_public_projection(row_get(row, field)):
            return f"{field}:blocked_status"
    if not normalize_date(row_get(row, "date")):
        return "date:invalid_iso"
    if row_has_key(row, "end_date") and clean_text(row_get(row, "end_date")) and not normalize_date(row_get(row, "end_date")):
        return "end_date:invalid_iso"
    for field in PUBLIC_TEXT_FIELDS:
        if row_has_key(row, field) and text_value_blocks_public_projection(field, row_get(row, field)):
            return f"{field}:leakage"
    return None


def split_time(value: Any) -> tuple[str | None, str | None, str | None]:
    raw = str(value or "").strip()
    if not raw:
        return None, None, None
    normalized = raw.replace("—", "..").replace("-", "..").replace("–", "..")
    parts = [part.strip() for part in normalized.split("..") if part.strip()]
    start = None
    end = None
    for part in parts[:2]:
        match = re.search(r"(\d{1,2})[:.](\d{2})", part)
        if match:
            hh = max(0, min(23, int(match.group(1))))
            mm = max(0, min(59, int(match.group(2))))
            if start is None:
                start = f"{hh:02d}:{mm:02d}"
            else:
                end = f"{hh:02d}:{mm:02d}"
    display = raw if raw and not start else (f"{start}–{end}" if end else start)
    return start, end, display


def explicit_event_duration_minutes(*values: Any) -> int | None:
    """Extract only a source-labeled event duration, never infer one from prose."""
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        match = re.search(
            r"продолжительность\s*:\s*(?:(\d{1,2})\s*(?:ч(?:ас(?:а|ов)?)?|h))?\s*(?:(\d{1,3})\s*(?:мин(?:ут(?:а|ы)?)?|m))?",
            text,
            flags=re.IGNORECASE,
        )
        if not match or not any(match.groups()):
            continue
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        duration = hours * 60 + minutes
        if 0 < duration <= 24 * 60:
            return duration
    return None


def event_end_from_duration(start_date: str, start_time: str | None, duration_minutes: int | None) -> tuple[str | None, str | None]:
    """Return (end_date, end_time) only for a source-explicit duration."""
    if not start_time or not duration_minutes:
        return None, None
    try:
        start = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None, None
    end = start + timedelta(minutes=duration_minutes)
    return end.strftime("%Y-%m-%d"), end.strftime("%H:%M")


def forecast_event_duration_minutes(value: Any) -> int | None:
    """Validate a persisted Smart Update forecast without inferring at build time."""
    if isinstance(value, bool):
        return None
    try:
        duration = int(value)
    except (TypeError, ValueError):
        return None
    return duration if 15 <= duration <= 12 * 60 else None


def price_label(row: sqlite3.Row) -> str | None:
    lo = row["ticket_price_min"]
    hi = row["ticket_price_max"]
    try:
        lo = int(lo) if lo is not None else None
        hi = int(hi) if hi is not None else None
    except Exception:
        return None
    if lo is None and hi is None:
        return None
    if lo is not None and lo <= 0 and bool(row["is_free"]):
        return None
    if lo is not None and hi is not None and lo != hi:
        return f"{lo}–{hi} ₽"
    value = lo if lo is not None else hi
    return f"{value} ₽" if value is not None else None


def is_sold_out_status(status: str) -> bool:
    return bool(re.search(r"sold|unavailable|not[_\s-]?available|нет\s+бил|законч|распрод", status or "", re.I))


def ticket_info(row: sqlite3.Row) -> dict[str, Any]:
    event_id = int(row["id"])
    status = clean_text(row["ticket_status"])
    status_l = status.lower()
    href = TICKET_LINK_OVERRIDES.get(event_id) or clean_text(row["ticket_link"]) or None
    # Admission semantics are materialized by Smart Update.  A status string
    # may describe registration, availability or one sub-program and must not
    # silently turn the whole event into a free-admission fact.
    free = bool(row["is_free"])
    price = price_label(row)
    has_registration = bool(re.search(r"регистрац|registration|запис", status_l))
    if is_sold_out_status(status):
        kind, label, href = "status", "Билеты закончились", None
    elif href and href.startswith("tel:"):
        kind, label = "phone", "Позвонить организатору"
    elif has_registration and href:
        kind, label = "registration", "Зарегистрироваться"
    elif free:
        kind, label = "free", "Источник события"
    elif has_registration:
        kind, label = "registration", "Зарегистрироваться"
    elif href:
        kind, label = "ticket", "Купить билет"
    else:
        kind, label = "status", "Условия уточняются"
    return {"kind": kind, "label": label, "href": href, "status": status or None, "is_free": free, "price_label": price, "note": None}


def status_label(row: sqlite3.Row, ticket: dict[str, Any]) -> str:
    status = clean_text(row["ticket_status"])
    if is_sold_out_status(status):
        return "Билеты закончились"
    if ticket["is_free"]:
        return "Бесплатно"
    if ticket["price_label"]:
        return ticket["price_label"]
    if re.search(r"регистрац|registration", status, re.I):
        return "Регистрация"
    if re.search(r"sale|available|продаж|билет", status, re.I):
        return "Билеты"
    return "Условия уточняются"


def apply_preview_overrides(event_id: int, ticket: dict[str, Any], status: str) -> tuple[dict[str, Any], str]:
    if event_id == 5370:
        ticket = {**ticket, "kind": "ticket", "label": "Купить билет", "is_free": False, "status": "paid"}
        if not ticket.get("price_label"):
            status = "Билеты"
    return ticket, status


def infer_event_type(row: sqlite3.Row, topics: list[str]) -> str | None:
    value = clean_text(row["event_type"])
    if value:
        return value.lower()
    for topic in topics:
        if topic in EVENT_TYPE_BY_TOPIC:
            return EVENT_TYPE_BY_TOPIC[topic]
    title = clean_text(row["title"]).lower()
    for pattern, label in [
        (r"концерт|музык", "концерт"), (r"выстав", "выставка"), (r"спектак|театр", "театр"),
        (r"лекц", "лекция"), (r"мастер", "мастер-класс"), (r"экскурс", "экскурсия"),
        (r"фестив|маркет|ярмарк", "фестиваль"),
    ]:
        if re.search(pattern, title):
            return label
    return "событие"


def markdownish_to_html(text: str) -> str:
    text = str(text or "").replace("\ufe0f", "").replace("\u200d", "")
    text = re.sub(r"\*\*(facts|факты(?:\s+о\s+событии)?)\*\*", "", text, flags=re.I)
    text = re.sub(r"\*\*([^*\n]{1,180})\*\*", r"\1", text)
    text = re.sub(r"__([^_\n]{1,180})__", r"\1", text)
    text = text.replace("***", "").strip()
    if not text:
        return ""
    blocks = [block.strip() for block in re.split(r"\n{2,}", text) if block.strip()]
    out: list[str] = []
    in_list = False
    for block in blocks[:18]:
        if block.startswith(">"):
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<blockquote>{html.escape(block.lstrip('> ').strip())}</blockquote>")
        elif block.startswith("###"):
            if in_list:
                out.append("</ul>"); in_list = False
            lines = [line.strip() for line in block.splitlines() if line.strip()]
            heading = re.sub(r"^#+\s*", "", lines[0] if lines else block).strip()
            body_lines = lines[1:]
            out.append(f"<h3>{html.escape(heading[:140]).strip()}</h3>")
            if body_lines:
                body = "\n".join(body_lines).strip()
                if re.match(r"^[-•*]\s+", body):
                    items = [
                        html.escape(re.sub(r"^[-•*]\s+", "", line).strip())
                        for line in body.splitlines()
                        if line.strip()
                    ]
                    if items:
                        out.append("<ul>")
                        out.extend(f"<li>{item}</li>" for item in items[:12])
                        out.append("</ul>")
                else:
                    out.append(f"<p>{html.escape(body).replace(chr(10), '<br />')}</p>")
        elif re.match(r"^[-•*]\s+", block):
            items = [html.escape(re.sub(r"^[-•*]\s+", "", line).strip()) for line in block.splitlines() if line.strip()]
            if items:
                out.append("<ul>")
                out.extend(f"<li>{item}</li>" for item in items[:12])
                out.append("</ul>")
                in_list = False
        else:
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<p>{html.escape(block).replace(chr(10), '<br />')}</p>")
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


def image_url_key(url: str) -> str:
    return (url or "").split("?", 1)[0]


def meaningful_ocr(value: Any) -> bool:
    text = clean_text(value).lower()
    if len(text) < 60:
        return False
    if "no readable text" in text or "no text" in text:
        return False
    letters = re.findall(r"[a-zа-яё]", text, flags=re.I)
    return len(letters) >= 20


IMAGE_DIMENSION_CACHE: dict[str, tuple[int | None, int | None]] = {}
SKIP_IMAGE_PROBES = False


def parse_png_dimensions(data: bytes) -> tuple[int, int] | None:
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        return int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")
    return None


def parse_webp_dimensions(data: bytes) -> tuple[int, int] | None:
    if len(data) < 30 or data[:4] != b"RIFF" or data[8:12] != b"WEBP":
        return None
    kind = data[12:16]
    if kind == b"VP8X" and len(data) >= 30:
        width = 1 + int.from_bytes(data[24:27], "little")
        height = 1 + int.from_bytes(data[27:30], "little")
        return width, height
    if kind == b"VP8 " and len(data) >= 30:
        # Lossy VP8 frame header after chunk header. Signature 9d 01 2a.
        start = 20
        sig = data.find(b"\x9d\x01\x2a", 20, 64)
        if sig >= 0 and len(data) >= sig + 7:
            width = int.from_bytes(data[sig + 3:sig + 5], "little") & 0x3fff
            height = int.from_bytes(data[sig + 5:sig + 7], "little") & 0x3fff
            return width, height
    if kind == b"VP8L" and len(data) >= 25:
        b0, b1, b2, b3 = data[21], data[22], data[23], data[24]
        width = 1 + (((b1 & 0x3F) << 8) | b0)
        height = 1 + (((b3 & 0x0F) << 10) | (b2 << 2) | ((b1 & 0xC0) >> 6))
        return width, height
    return None


def parse_jpeg_dimensions(data: bytes) -> tuple[int, int] | None:
    if not data.startswith(b"\xff\xd8"):
        return None
    i = 2
    n = len(data)
    while i + 9 < n:
        if data[i] != 0xFF:
            i += 1
            continue
        while i < n and data[i] == 0xFF:
            i += 1
        if i >= n:
            break
        marker = data[i]
        i += 1
        if marker in {0xD8, 0xD9, 0x01} or 0xD0 <= marker <= 0xD7:
            continue
        if i + 2 > n:
            break
        length = int.from_bytes(data[i:i + 2], "big")
        if length < 2 or i + length > n:
            break
        if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF} and length >= 7:
            height = int.from_bytes(data[i + 3:i + 5], "big")
            width = int.from_bytes(data[i + 5:i + 7], "big")
            return width, height
        i += length
    return None


def parse_image_dimensions(data: bytes) -> tuple[int, int] | None:
    return parse_png_dimensions(data) or parse_webp_dimensions(data) or parse_jpeg_dimensions(data)


def probe_image_dimensions(url: str, timeout: float = 4.0) -> tuple[int | None, int | None]:
    """Best-effort remote image dimension probe without full image decoding.

    Static previews need to avoid picking a tiny/blurred first media item as the
    hero when a later same-event image is much sharper. The probe reads only an
    initial byte range and parses PNG/JPEG/WebP headers. If networking or parsing
    fails, the caller falls back to the legacy 1080x1350 preview contract.
    """
    key = image_url_key(url)
    if key in IMAGE_DIMENSION_CACHE:
        return IMAGE_DIMENSION_CACHE[key]
    result: tuple[int | None, int | None] = (None, None)
    try:
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "KenigEventsStaticPreview/1.0 (+https://kenigevents.ru)",
                "Range": "bytes=0-65535",
            },
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            head = response.read(65536)
        parsed = parse_image_dimensions(head)
        if parsed and parsed[0] > 0 and parsed[1] > 0:
            result = parsed
    except Exception:
        result = (None, None)
    IMAGE_DIMENSION_CACHE[key] = result
    return result


PARTICIPANT_REQUIRED_TABLE_COLUMNS = {
    "artist_registry_entity": {
        "artist_id",
        "entity_type",
        "display_name",
        "verification_status",
        "photo_url",
        "photo_rights_status",
        "photo_rights_evidence_json",
    },
    "event_artist_appearance": {
        "event_id",
        "artist_id",
        "role",
        "status",
        "physical_visit_status",
        "participant_evidence_json",
        "eligibility_status",
        "cancelled_at",
        "media_identity_status",
    },
}
PARTICIPANT_PHOTO_ALLOWED_STATUSES = {
    "event_artist_verified",
    "press_kit_verified",
    "cc_verified",
    "informational_citation_reviewed",
}
PARTICIPANT_HEADLINER_ROLES = {"headliner", "keynote", "главный артист", "хедлайнер"}
PARTICIPANT_ROLE_LABELS = {
    "artist": "Артист",
    "performer": "Артист",
    "speaker": "Спикер",
    "keynote": "Ключевой спикер",
    "headliner": "Хедлайнер",
    "host": "Ведущий",
    "moderator": "Модератор",
    "author": "Автор",
}


def sqlite_table_columns(con: sqlite3.Connection, table_name: str) -> set[str]:
    exists = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if not exists:
        return set()
    return {
        str(row[1])
        for row in con.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    }


VIDEO_ASSET_REQUIRED_COLUMNS = {
    "id",
    "sha256",
    "cdn_url",
    "width",
    "height",
}
EVENT_VIDEO_LINK_REQUIRED_COLUMNS = {
    "event_id",
    "video_asset_id",
}
VIDEO_ASSET_OPTIONAL_EXPORT_COLUMNS = (
    "cdn_path",
    "mime_type",
    "duration_seconds",
    "aesthetic_score",
    "technical_score",
    "showcase_score",
    "description",
    "search_text",
)


def _finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _video_score(value: Any) -> float | None:
    """Project a bounded persisted model score without inventing a fallback."""

    result = _finite_float(value)
    if result is None or not 0 <= result <= 100:
        return None
    return round(result, 4)


def event_video_assets_for_events(
    con: sqlite3.Connection,
    event_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    """Project ranked public vertical video assets for multiple events.

    Static-site builds may consume SQLite snapshots created before the video
    ledger migration. Missing tables or a partially migrated required contract
    therefore fail closed to empty lists instead of aborting the whole build.
    Optional metadata columns are selected as ``NULL`` until the snapshot has
    them. One content-addressed ``video_asset`` may be returned for any number
    of events through ``event_video_link``.
    """

    unique_event_ids = sorted({int(event_id) for event_id in event_ids})
    empty = {event_id: [] for event_id in unique_event_ids}
    if not unique_event_ids:
        return empty

    asset_columns = sqlite_table_columns(con, "video_asset")
    link_columns = sqlite_table_columns(con, "event_video_link")
    if (
        not VIDEO_ASSET_REQUIRED_COLUMNS.issubset(asset_columns)
        or not EVENT_VIDEO_LINK_REQUIRED_COLUMNS.issubset(link_columns)
    ):
        return empty

    optional_asset_select = ",\n            ".join(
        f"asset.{column} AS {column}"
        if column in asset_columns
        else f"NULL AS {column}"
        for column in VIDEO_ASSET_OPTIONAL_EXPORT_COLUMNS
    )
    relevance_select = (
        "link.event_relevance_score AS event_relevance_score"
        if "event_relevance_score" in link_columns
        else "NULL AS event_relevance_score"
    )
    source_url_select = (
        "link.source_url AS source_url"
        if "source_url" in link_columns
        else "NULL AS source_url"
    )
    ranking_select = (
        "link.ranking_score AS ranking_score"
        if "ranking_score" in link_columns
        else "NULL AS ranking_score"
    )
    ranking_order = (
        "COALESCE(link.ranking_score, -1) DESC"
        if "ranking_score" in link_columns
        else "CAST(-1 AS REAL) DESC"
    )
    showcase_order = (
        "COALESCE(asset.showcase_score, -1) DESC"
        if "showcase_score" in asset_columns
        else "CAST(-1 AS REAL) DESC"
    )
    relevance_order = (
        ", COALESCE(link.event_relevance_score, -1) DESC"
        if "event_relevance_score" in link_columns
        else ""
    )
    placeholders = ",".join("?" for _ in unique_event_ids)
    accepted_status_where = (
        "AND lower(trim(COALESCE(asset.analysis_status, ''))) = 'accepted'"
        if "analysis_status" in asset_columns
        else ""
    )
    try:
        rows = con.execute(
            f"""
            SELECT
                link.event_id,
                asset.id AS video_asset_id,
                asset.sha256,
                asset.cdn_url,
                asset.width,
                asset.height,
                {optional_asset_select},
                {relevance_select},
                {ranking_select},
                {source_url_select}
            FROM event_video_link AS link
            JOIN video_asset AS asset
              ON asset.id = link.video_asset_id
            WHERE link.event_id IN ({placeholders})
              {accepted_status_where}
            ORDER BY
                link.event_id ASC,
                {ranking_order},
                {showcase_order}
                {relevance_order},
                asset.sha256 ASC,
                asset.id ASC
            """,
            unique_event_ids,
        ).fetchall()
    except sqlite3.DatabaseError:
        # A concurrently copied/pre-migration snapshot must never make the
        # complete static catalog unavailable.
        return empty

    projected = {event_id: [] for event_id in unique_event_ids}
    seen_by_event: dict[int, set[str]] = defaultdict(set)
    for row in rows:
        event_id = int(row["event_id"])
        sha256 = clean_text(row_get(row, "sha256")).lower()
        src = canonical_event_media_cdn_url(row_get(row, "cdn_url"))
        try:
            width = int(row_get(row, "width") or 0)
            height = int(row_get(row, "height") or 0)
        except (TypeError, ValueError):
            continue
        # asset_key is content-addressed, so malformed/non-hash rows are not a
        # stable public contract. Video UI is vertical-only by product policy.
        if not re.fullmatch(r"[0-9a-f]{64}", sha256) or not src or width <= 0 or height <= width:
            continue
        if sha256 in seen_by_event[event_id]:
            continue
        seen_by_event[event_id].add(sha256)

        duration = _finite_float(row_get(row, "duration_seconds"))
        if duration is not None and duration <= 0:
            duration = None
        projected[event_id].append(
            {
                "src": src,
                "asset_key": sha256,
                "cdn_path": clean_text(row_get(row, "cdn_path")) or None,
                "mime_type": clean_text(row_get(row, "mime_type")) or None,
                "width": width,
                "height": height,
                "duration_seconds": round(duration, 4) if duration is not None else None,
                "aesthetic_score": _video_score(row_get(row, "aesthetic_score")),
                "technical_score": _video_score(row_get(row, "technical_score")),
                "event_relevance_score": _video_score(row_get(row, "event_relevance_score")),
                "ranking_score": _video_score(row_get(row, "ranking_score")),
                "showcase_score": _video_score(row_get(row, "showcase_score")),
                "source_url": clean_text(row_get(row, "source_url")) or None,
                "description": clean_text(row_get(row, "description")) or None,
                "search_text": clean_text(row_get(row, "search_text")) or None,
            }
        )
    return projected


def participant_photo_metadata(row: sqlite3.Row) -> tuple[str | None, str | None, str | None]:
    """Admit only a registry portrait with explicit identity and rights evidence."""

    photo_url = clean_text(row_get(row, "photo_url"))
    rights_status = clean_text(row_get(row, "photo_rights_status")).casefold()
    identity_status = clean_text(row_get(row, "media_identity_status")).casefold()
    if (
        not photo_url
        or not photo_url.startswith(("https://", "/"))
        or rights_status not in PARTICIPANT_PHOTO_ALLOWED_STATUSES
        or identity_status != "verified"
    ):
        return None, None, None
    for evidence in read_json(row_get(row, "photo_rights_evidence_json"), []):
        if not isinstance(evidence, dict):
            continue
        source_url = clean_text(
            evidence.get("source_url")
            or evidence.get("source_page_url")
            or evidence.get("original_source_url")
        )
        credit_text = clean_text(
            evidence.get("credit_text")
            or evidence.get("author_or_rightsholder")
        )
        if source_url.startswith("https://") and credit_text:
            return photo_url, credit_text, source_url
    return None, None, None


def event_participants_for_events(
    con: sqlite3.Connection,
    event_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    """Project verified occurrence participants without making the site depend on the overlay.

    The artist registry work is deployed independently from the static site.  Old
    SQLite snapshots therefore remain valid and simply produce no participant
    block.  Rows under review, cancelled appearances and unverified identities
    always fail closed.
    """

    unique_event_ids = sorted({int(event_id) for event_id in event_ids})
    empty = {event_id: [] for event_id in unique_event_ids}
    if not unique_event_ids:
        return empty
    for table_name, required in PARTICIPANT_REQUIRED_TABLE_COLUMNS.items():
        if not required.issubset(sqlite_table_columns(con, table_name)):
            return empty
    placeholders = ",".join("?" for _ in unique_event_ids)
    rows = con.execute(
        f"""
        SELECT
            appearance.event_id,
            appearance.artist_id,
            appearance.role,
            appearance.participant_evidence_json,
            appearance.media_identity_status,
            artist.entity_type,
            artist.display_name,
            artist.photo_url,
            artist.photo_rights_status,
            artist.photo_rights_evidence_json
        FROM event_artist_appearance AS appearance
        JOIN artist_registry_entity AS artist
          ON artist.artist_id = appearance.artist_id
        WHERE appearance.event_id IN ({placeholders})
          AND appearance.status = 'confirmed'
          AND appearance.physical_visit_status = 'confirmed'
          AND appearance.eligibility_status = 'eligible'
          AND appearance.cancelled_at IS NULL
          AND artist.verification_status = 'verified'
        ORDER BY
          appearance.event_id,
          CASE WHEN lower(appearance.role) IN ('headliner', 'keynote') THEN 0 ELSE 1 END,
          lower(artist.display_name),
          artist.artist_id
        """,
        tuple(unique_event_ids),
    ).fetchall()
    participants_by_event = empty
    seen: dict[int, set[str]] = {event_id: set() for event_id in unique_event_ids}
    for row in rows:
        event_id = int(row["event_id"])
        participant_id = clean_text(row["artist_id"])
        name = clean_text(row["display_name"])
        evidence = [
            item
            for item in read_json(row["participant_evidence_json"], [])
            if isinstance(item, dict)
        ]
        if (
            not participant_id
            or not name
            or not evidence
            or participant_id in seen[event_id]
        ):
            continue
        seen[event_id].add(participant_id)
        raw_role = clean_text(row["role"]) or "participant"
        role_key = raw_role.casefold()
        role = PARTICIPANT_ROLE_LABELS.get(role_key, raw_role[:1].upper() + raw_role[1:])
        avatar_url, credit_text, credit_url = participant_photo_metadata(row)
        evidence_url = next(
            (
                clean_text(item.get("source_url") or item.get("url"))
                for item in evidence
                if clean_text(item.get("source_url") or item.get("url")).startswith("https://")
            ),
            None,
        )
        entity_kind = clean_text(row["entity_type"]).casefold()
        if entity_kind not in {"person", "group", "project"}:
            entity_kind = "person"
        participants_by_event[event_id].append(
            {
                "id": participant_id,
                "name": name,
                "role": role,
                "entity_kind": entity_kind,
                "is_headliner": role_key in PARTICIPANT_HEADLINER_ROLES,
                "avatar_url": avatar_url,
                "avatar_alt": f"{name} — {role}",
                "likes_count": 0,
                "profile_url": None,
                "credit_text": credit_text,
                "credit_url": credit_url,
                "evidence_url": evidence_url,
            }
        )
    return participants_by_event


def event_participants(con: sqlite3.Connection, event_id: int) -> list[dict[str, Any]]:
    """Single-event compatibility wrapper used by focused tests and tools."""

    return event_participants_for_events(con, [event_id]).get(int(event_id), [])


def image_area(asset: dict[str, Any]) -> int:
    try:
        width = int(asset.get("width") or 0)
        height = int(asset.get("height") or 0)
    except Exception:
        return 0
    return max(0, width) * max(0, height)


def hero_image_quality_score(asset: dict[str, Any]) -> float:
    area = image_area(asset)
    width = int(asset.get("width") or 0)
    height = int(asset.get("height") or 0)
    min_side = min(width, height) if width and height else 0
    score = math.log1p(area) if area else 0.0
    if min_side and min_side < 360:
        score -= 4.0
    elif min_side and min_side < 640:
        score -= 1.3
    if asset.get("image_text_mode") == "visual_only":
        score += 0.15
    if asset.get("media_role") == "event_photo":
        score += 0.45
    elif asset.get("media_role") == "event_identity_poster":
        score -= 0.08
    if str(asset.get("src") or "").startswith("https://storage.yandexcloud.net/kenigevents/"):
        score += 0.04
    return score


def choose_primary_image_asset(assets: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not assets:
        return None
    first = assets[0]
    first_area = image_area(first)
    first_score = hero_image_quality_score(first)
    # Theatre/catalog parsers may attach the same monthly advert or generic
    # venue gallery to many unrelated events.  When a row also has a strong
    # event-exclusive visual, prefer that evidence over cross-event boilerplate.
    # If no exclusive alternative exists we keep the shared asset, preserving
    # recurring-event and weak-media fallbacks.
    if int(first.get("_event_reuse_count") or first.get("event_reuse_count") or 1) > 1:
        exclusive_candidates = [
            candidate
            for candidate in assets[1:]
            if int(candidate.get("_event_reuse_count") or candidate.get("event_reuse_count") or 1) == 1
            and candidate.get("image_text_mode") == "visual_only"
            and min(int(candidate.get("width") or 0), int(candidate.get("height") or 0)) >= 720
            and image_area(candidate) >= 900_000
        ]
        if exclusive_candidates:
            return max(exclusive_candidates, key=hero_image_quality_score)
    # A classified utility/advertising/document image is useful gallery
    # material, but it must not monopolise the event hero when the same event
    # has a separately classified, crop-safe, full-resolution photograph.
    # This is a semantic-role guard supplied by the upstream LLM pass, not an
    # OCR/filename guess.
    first_is_non_photo_document = (
        first.get("media_semantic_status") == "classified"
        and first.get("media_role") not in {"event_photo", "event_identity_poster"}
    )
    if first_is_non_photo_document:
        photo_candidates = [
            candidate
            for candidate in assets[1:]
            if candidate.get("media_semantic_status") == "classified"
            and candidate.get("media_role") == "event_photo"
            and candidate.get("safe_crop") is True
            and min(int(candidate.get("width") or 0), int(candidate.get("height") or 0)) >= 720
            and image_area(candidate) >= 900_000
        ]
        if photo_candidates:
            return max(photo_candidates, key=hero_image_quality_score)
    # If the first item is tiny, rescue it with the earliest adequate later image,
    # not necessarily the largest one: source order still carries editorial intent.
    if first_area < 300_000:
        for candidate in assets[1:]:
            candidate_area = image_area(candidate)
            try:
                min_side = min(int(candidate.get("width") or 0), int(candidate.get("height") or 0))
            except Exception:
                min_side = 0
            if candidate_area >= 900_000 and min_side >= 720:
                return candidate
    best = max(assets, key=hero_image_quality_score)
    best_area = image_area(best)
    best_score = hero_image_quality_score(best)
    if best is first:
        return first
    if (
        first.get("media_role") != "event_identity_poster"
        and first.get("image_text_mode") == "ocr_text"
        and best.get("image_text_mode") != "ocr_text"
        and first_area >= 500_000
    ):
        return first
    # Strong quality rescue: low-quality first image vs a clearly better later image.
    if first_area and best_area >= max(first_area * 2.25, first_area + 600_000) and best_score >= first_score + 1.0:
        return best
    return first


def normalized_yxyx_box(value: Any) -> dict[str, float] | None:
    """Convert stored normalized [ymin, xmin, ymax, xmax] into the public x/y/w/h contract."""
    parsed = read_json(value, value)
    if not isinstance(parsed, list) or len(parsed) != 4:
        return None
    try:
        ymin, xmin, ymax, xmax = (float(item) for item in parsed)
    except (TypeError, ValueError):
        return None
    if not all(math.isfinite(item) and 0 <= item <= 1 for item in (ymin, xmin, ymax, xmax)):
        return None
    if ymax <= ymin or xmax <= xmin:
        return None
    return {
        "x": round(xmin, 6),
        "y": round(ymin, 6),
        "w": round(xmax - xmin, 6),
        "h": round(ymax - ymin, 6),
    }


def current_geometry_metadata(row: sqlite3.Row) -> dict[str, Any] | None:
    """Return geometry only when SQL joined a classified record for the current pixels."""
    geometry_id = row_get(row, "geometry_id")
    current_pixel = clean_text(row_get(row, "pixel_sha256"))
    geometry_pixel = clean_text(row_get(row, "geometry_pixel_sha256"))
    if not geometry_id or not current_pixel or geometry_pixel != current_pixel:
        return None
    if clean_text(row_get(row, "geometry_status")).lower() != "classified":
        return None
    if clean_text(row_get(row, "geometry_model")) != EXPECTED_IMAGE_GEOMETRY_MODEL:
        return None
    if clean_text(row_get(row, "geometry_prompt_version")) != EXPECTED_IMAGE_GEOMETRY_PROMPT_VERSION:
        return None
    face_values = read_json(row_get(row, "geometry_face_boxes_yxyx_json"), None)
    if not isinstance(face_values, list):
        return None
    face_boxes = [box for item in face_values if (box := normalized_yxyx_box(item)) is not None]
    valuable_region = normalized_yxyx_box(row_get(row, "geometry_valuable_region_yxyx_json"))
    if valuable_region is None:
        return None
    confidence = row_get(row, "geometry_valuable_region_confidence")
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError):
        confidence_value = None
    if confidence_value is not None and math.isfinite(confidence_value):
        valuable_region["confidence"] = round(max(0.0, min(1.0, confidence_value)), 6)
    return {
        "geometry_id": int(geometry_id),
        "current_pixel_sha256": current_pixel,
        "geometry_pixel_sha256": geometry_pixel,
        "geometry_model": clean_text(row_get(row, "geometry_model")),
        "geometry_prompt_version": clean_text(row_get(row, "geometry_prompt_version")),
        "geometry_status": "classified",
        "geometry_coordinate_space": "normalized_0_1",
        "geometry_source_width": int(row_get(row, "geometry_source_width") or 0) or None,
        "geometry_source_height": int(row_get(row, "geometry_source_height") or 0) or None,
        "face_boxes": face_boxes,
        "valuable_region": valuable_region,
        "geometry_reason_code": clean_text(row_get(row, "geometry_reason_code")) or None,
    }


def collect_images(con: sqlite3.Connection, event_id: int, photo_urls_raw: Any, title: str) -> tuple[str | None, str, str | None, list[dict[str, Any]]]:
    canonical_ledger_present = False
    try:
        eventposter_columns = {
            str(row[1]) for row in con.execute("pragma table_info('eventposter')").fetchall()
        }
        optional_fields = [
            "width",
            "height",
            "image_text_mode",
            "media_role",
            "media_role_confidence",
            "media_semantic_status",
            "focal_x",
            "focal_y",
            "safe_crop",
            "thumbnail_256_url",
            "thumbnail_256_width",
            "thumbnail_256_height",
            "thumbnail_512_url",
            "thumbnail_512_width",
            "thumbnail_512_height",
            "raw_sha256",
            "pixel_sha256",
            "canonical_object_path",
        ]
        optional_select = ", ".join(
            f"ep.{field}" if field in eventposter_columns else f"NULL AS {field}"
            for field in optional_fields
        )
        geometry_table_present = bool(
            con.execute(
                "select 1 from sqlite_master where type='table' and name='event_image_geometry'"
            ).fetchone()
        )
        geometry_available = (
            geometry_table_present
            and "image_geometry_id" in eventposter_columns
            and "pixel_sha256" in eventposter_columns
        )
        geometry_select = """
            g.id AS geometry_id,
            g.pixel_sha256 AS geometry_pixel_sha256,
            g.model AS geometry_model,
            g.prompt_version AS geometry_prompt_version,
            g.status AS geometry_status,
            g.source_width AS geometry_source_width,
            g.source_height AS geometry_source_height,
            g.face_boxes_yxyx_json AS geometry_face_boxes_yxyx_json,
            g.valuable_region_yxyx_json AS geometry_valuable_region_yxyx_json,
            g.valuable_region_confidence AS geometry_valuable_region_confidence,
            g.reason_code AS geometry_reason_code
        """ if geometry_available else """
            NULL AS geometry_id,
            NULL AS geometry_pixel_sha256,
            NULL AS geometry_model,
            NULL AS geometry_prompt_version,
            NULL AS geometry_status,
            NULL AS geometry_source_width,
            NULL AS geometry_source_height,
            NULL AS geometry_face_boxes_yxyx_json,
            NULL AS geometry_valuable_region_yxyx_json,
            NULL AS geometry_valuable_region_confidence,
            NULL AS geometry_reason_code
        """
        geometry_join = """
            left join event_image_geometry g
              on g.id=ep.image_geometry_id
             and g.status='classified'
             and g.pixel_sha256=ep.pixel_sha256
        """ if geometry_available else ""
        rows = con.execute(
            f"""
            select ep.supabase_url, ep.catbox_url, ep.ocr_text, {optional_select}, {geometry_select}
            from eventposter ep
            {geometry_join}
            where ep.event_id=? and ep.review_status='approved'
            order by ep.display_order asc, ep.id asc
            """,
            (event_id,),
        ).fetchall()
        canonical_ledger_present = bool(rows) or bool(
            con.execute(
                "select 1 from eventposter where event_id=? limit 1", (event_id,)
            ).fetchone()
        )
    except sqlite3.OperationalError:
        # Read-only preview of a pre-migration snapshot. Production init adds
        # review_status before the next build.
        rows = con.execute(
            "select supabase_url, catbox_url, ocr_text from eventposter where event_id=? order by id asc",
            (event_id,),
        ).fetchall()
        canonical_ledger_present = bool(rows)
    metadata_by_url: dict[str, dict[str, Any]] = {}
    poster_urls: list[str] = []
    approved_reuse_count_by_url: dict[str, int] = {}
    approved_urls = [clean_text(row_get(row, "supabase_url")) for row in rows]
    approved_urls = [url for url in approved_urls if url]
    if approved_urls:
        try:
            placeholders = ",".join("?" for _ in approved_urls)
            approved_reuse_count_by_url = {
                clean_text(reuse_row["supabase_url"]): int(reuse_row["event_count"] or 1)
                for reuse_row in con.execute(
                    f"""
                    select supabase_url, count(distinct event_id) as event_count
                    from eventposter
                    where review_status='approved' and supabase_url in ({placeholders})
                    group by supabase_url
                    """,
                    approved_urls,
                ).fetchall()
            }
        except sqlite3.OperationalError:
            approved_reuse_count_by_url = {}
    for row in rows:
        # One EventPoster is one logical gallery item.  Its managed and source
        # URLs are alternate locations, never two public images.
        url = canonical_event_media_cdn_url(row["supabase_url"])
        if not url:
            continue
        poster_urls.append(url)
        metadata_by_url[image_url_key(url)] = {
            key: row_get(row, key)
            for key in row.keys()
            if key not in {"supabase_url", "catbox_url"}
        }
        metadata_by_url[image_url_key(url)]["event_reuse_count"] = approved_reuse_count_by_url.get(
            clean_text(row_get(row, "supabase_url")),
            1,
        )
    photo_urls = read_json(photo_urls_raw, [])
    urls = []
    seen = set()
    # EventPoster is canonical. Legacy photo_urls is a temporary fallback only
    # for rows that have not yet been materialized by the audited backfill.
    source_urls = poster_urls if canonical_ledger_present else list(photo_urls or [])
    for url in source_urls:
        url = canonical_event_media_cdn_url(url)
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    override_url = canonical_event_media_cdn_url(IMAGE_URL_OVERRIDES.get(event_id))
    if override_url:
        urls = [override_url] + [url for url in urls if image_url_key(url) != image_url_key(override_url)]
    assets: list[dict[str, Any]] = []
    first_width: int | None = None
    first_height: int | None = None
    needs_rescue_scan = False
    if len(urls) > 1 and not SKIP_IMAGE_PROBES:
        probed_width, probed_height = probe_image_dimensions(urls[0])
        first_width = int(probed_width or 0) or None
        first_height = int(probed_height or 0) or None
        if first_width and first_height:
            first_area = first_width * first_height
            needs_rescue_scan = first_area < 900_000 or min(first_width, first_height) < 720
        else:
            needs_rescue_scan = True
    for index, url in enumerate(urls[:12]):
        metadata = metadata_by_url.get(image_url_key(url), {})
        geometry = current_geometry_metadata(metadata) if metadata else None
        ocr = str(metadata.get("ocr_text") or "")
        stored_mode = clean_text(metadata.get("image_text_mode")).lower()
        semantic_status = clean_text(metadata.get("media_semantic_status")).lower()
        raw_role = clean_text(metadata.get("media_role")).lower()
        # Missing OCR output is not positive evidence that an asset is a
        # photograph. Preserve explicit producer classifications and the
        # narrow reviewed legacy overrides, but otherwise export `unknown` so
        # every consuming surface fails closed to contain. A classified
        # event-photo role is also affirmative visual evidence from the LLM
        # semantic pass; pending/error rows with neither signal remain unknown.
        mode = (
            "visual_only"
            if event_id in FORCE_VISUAL_IMAGE_MODE_IDS
            else stored_mode
            if stored_mode in {"ocr_text", "visual_only"}
            else "ocr_text"
            if meaningful_ocr(ocr)
            else "visual_only"
            if semantic_status == "classified" and raw_role == "event_photo"
            else "unknown"
        )
        media_role = raw_role if semantic_status == "classified" and raw_role in EVENT_IMAGE_MEDIA_ROLES else None
        if index == 0 and not metadata.get("width"):
            probed_width, probed_height = first_width, first_height
        elif not metadata.get("width") and needs_rescue_scan and not SKIP_IMAGE_PROBES:
            probed_width, probed_height = probe_image_dimensions(url)
        else:
            probed_width, probed_height = (None, None)
        width = int(metadata.get("width") or probed_width or 1080)
        height = int(metadata.get("height") or probed_height or 1350)
        # Unknown media is deliberately no-crop. OCR/no-OCR heuristics are not
        # semantic evidence: only the classified event_photo role may unlock
        # cover cropping in card/hero UI.
        role_for_ui = media_role or "unknown_document"
        thumbnail_sources = []
        for size in (256, 512):
            thumb_url = canonical_event_media_cdn_url(metadata.get(f"thumbnail_{size}_url"))
            thumb_width = int(metadata.get(f"thumbnail_{size}_width") or 0)
            thumb_height = int(metadata.get(f"thumbnail_{size}_height") or 0)
            if thumb_url and thumb_width > 0 and thumb_height > 0:
                thumbnail_sources.append({"src": thumb_url, "width": thumb_width, "height": thumb_height})
        focal_x = metadata.get("focal_x")
        focal_y = metadata.get("focal_y")
        focal_point = None
        try:
            if focal_x is not None and focal_y is not None and 0 <= float(focal_x) <= 1 and 0 <= float(focal_y) <= 1:
                focal_point = {"x": round(float(focal_x), 5), "y": round(float(focal_y), 5)}
        except (TypeError, ValueError):
            focal_point = None
        if role_for_ui == "event_identity_poster":
            alt = f"Афиша события «{title}»"
        elif role_for_ui == "program_or_schedule":
            alt = f"Расписание события «{title}»"
        elif role_for_ui == "attendee_information":
            alt = f"Полезная информация для посетителей события «{title}»"
        else:
            alt = f"Изображение события «{title}»"
        crop_eligible = bool(
            geometry
            and mode == "visual_only"
            and semantic_status == "classified"
            and role_for_ui == "event_photo"
            and metadata.get("safe_crop")
        )
        asset = {
            "src": url,
            "width": width,
            "height": height,
            "alt": alt,
            "image_text_mode": mode,
            "media_role": role_for_ui,
            "media_role_confidence": round(float(metadata.get("media_role_confidence") or 0), 5),
            "media_semantic_status": semantic_status if semantic_status in {"pending", "classified", "error", "stale"} else "pending",
            "image_kind": "poster" if role_for_ui == "event_identity_poster" else ("photo" if role_for_ui == "event_photo" else "mixed"),
            "recommended_hero_fit": "cover" if crop_eligible else "contain",
            "safe_crop": crop_eligible,
            "source_order": index,
            "_event_reuse_count": max(1, int(metadata.get("event_reuse_count") or 1)),
        }
        if thumbnail_sources:
            asset["thumbnail_sources"] = thumbnail_sources
        if geometry:
            asset.update(geometry)
        if focal_point:
            asset["focal_point"] = focal_point
            asset["recommended_object_position"] = f"{round(focal_point['x'] * 100)}% {round(focal_point['y'] * 100)}%"
        asset_key = clean_text(metadata.get("raw_sha256") or metadata.get("pixel_sha256") or metadata.get("canonical_object_path"))
        if asset_key:
            asset["asset_key"] = asset_key
        asset["quality_score"] = round(hero_image_quality_score(asset), 4)
        assets.append(asset)
    primary_asset = choose_primary_image_asset(assets)
    if primary_asset and assets and primary_asset is not assets[0]:
        log_stage(
            "hero_image_promoted",
            event_id=event_id,
            old_src=assets[0].get("src"),
            old_width=assets[0].get("width"),
            old_height=assets[0].get("height"),
            old_quality=assets[0].get("quality_score"),
            new_src=primary_asset.get("src"),
            new_width=primary_asset.get("width"),
            new_height=primary_asset.get("height"),
            new_quality=primary_asset.get("quality_score"),
        )
        assets = [primary_asset] + [asset for asset in assets if asset is not primary_asset]
    primary = assets[0]["src"] if assets else None
    primary_mode = assets[0]["image_text_mode"] if assets else "unknown"
    primary_role = assets[0].get("media_role") if assets else None
    for asset in assets:
        asset.pop("_event_reuse_count", None)
    return primary, primary_mode, primary_role, assets


def collect_source_urls(con: sqlite3.Connection, event_id: int, row: sqlite3.Row) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        url = clean_text(value)
        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    add(row["source_post_url"])
    add(row["source_vk_post_url"])
    add(row_get(row, "tg_event_post_url"))
    add(row_get(row, "vk_repost_url"))
    try:
        for src in con.execute("select source_url from event_source where event_id=?", (event_id,)):
            add(src["source_url"])
    except sqlite3.OperationalError:
        pass
    try:
        for publication in con.execute(
            "select stored_url, live_url from event_publication where event_id=? and status='published'",
            (event_id,),
        ):
            add(publication["stored_url"])
            add(publication["live_url"])
    except sqlite3.OperationalError:
        pass
    # @kenigevents also contains digests, polls and editorial posts. Include
    # only exact event-forward ledgers, never arbitrary channel messages.
    try:
        for exposure in con.execute(
            """
            select details_json, public_targets_json
            from promo_exposure
            where event_id=? and surface='tg_repost' and publish_status='TG_FORWARDED'
            """,
            (event_id,),
        ):
            try:
                details = json.loads(str(exposure["details_json"] or "{}"))
            except (TypeError, ValueError):
                details = {}
            target_url = str(details.get("target_url") or "") if isinstance(details, dict) else ""
            if re.search(r"(?:https?://)?t\.me/kenigevents/\d+", target_url, flags=re.I):
                add(target_url)
                continue
            try:
                public_targets = json.loads(str(exposure["public_targets_json"] or "[]"))
            except (TypeError, ValueError):
                public_targets = []
            for target in public_targets if isinstance(public_targets, list) else []:
                target_url = str(target.get("url") or "") if isinstance(target, dict) else ""
                if re.search(r"(?:https?://)?t\.me/kenigevents/\d+", target_url, flags=re.I):
                    add(target_url)
    except sqlite3.OperationalError:
        pass
    # The official VK wall also contains multi-event formats. Attribute only
    # successful exact event repost exposures to this event; never fan out a
    # digest/video/story counter across its participants.
    try:
        for exposure in con.execute(
            """
            select details_json, public_targets_json
            from promo_exposure
            where event_id=? and surface='vk_repost' and publish_status='PUBLISHED_MAIN'
            """,
            (event_id,),
        ):
            try:
                details = json.loads(str(exposure["details_json"] or "{}"))
            except (TypeError, ValueError):
                details = {}
            candidates = [str(details.get("target_url") or "")] if isinstance(details, dict) else []
            declared_group = details.get("target_group_id") if isinstance(details, dict) else None
            if declared_group not in (None, ""):
                try:
                    if abs(int(declared_group)) != 231828790:
                        continue
                except (TypeError, ValueError):
                    continue
            try:
                public_targets = json.loads(str(exposure["public_targets_json"] or "[]"))
            except (TypeError, ValueError):
                public_targets = []
            candidates.extend(
                str(target.get("url") or "")
                for target in (public_targets if isinstance(public_targets, list) else [])
                if isinstance(target, dict)
            )
            for target_url in candidates:
                match = re.search(r"wall-?231828790_(\d+)", target_url, flags=re.I)
                if match:
                    add(f"https://vk.com/wall-231828790_{int(match.group(1))}")
                    break
    except sqlite3.OperationalError:
        pass
    try:
        for forwarded in con.execute(
            """
            select poll_chat_id, forwarded_message_id
            from poll_repost_run
            where chosen_event_id=? and status='forwarded' and forwarded_message_id is not null
            """,
            (event_id,),
        ):
            if str(forwarded["poll_chat_id"] or "").strip().lstrip("@").lower() == "kenigevents":
                add(f"https://t.me/kenigevents/{int(forwarded['forwarded_message_id'])}")
    except sqlite3.OperationalError:
        pass
    return urls


def collect_source_records(con: sqlite3.Connection, event_id: int) -> list[dict[str, Any]]:
    """Keep structured source identity/trust for exact registry adapters.

    Public cards continue to receive URL-only ``source_urls``.  These compact
    records stay inside the exporter and generated registry projection.
    """

    try:
        rows = con.execute(
            """
            select source_type, source_url, source_chat_username,
                   source_chat_id, source_message_id, trust_level
            from event_source
            where event_id=?
            order by id asc
            """,
            (event_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        try:
            rows = con.execute(
                "select source_type, source_url from event_source where event_id=? order by rowid asc",
                (event_id,),
            ).fetchall()
        except sqlite3.OperationalError:
            return []
    records: list[dict[str, Any]] = []
    for source in rows:
        source_type = clean_text(row_get(source, "source_type"))
        username = clean_text(row_get(source, "source_chat_username"))
        record = {
            "source_type": source_type or None,
            "source_url": clean_text(row_get(source, "source_url")) or None,
            "trust_level": clean_text(row_get(source, "trust_level")) or None,
            "source_chat_id": row_get(source, "source_chat_id"),
            "source_message_id": row_get(source, "source_message_id"),
        }
        if username and source_type.casefold() in {"telegram", "tg"}:
            record["telegram_username"] = username
        records.append({key: value for key, value in record.items() if value is not None})
    return records


def source_metrics(con: sqlite3.Connection, urls: list[str]) -> tuple[int, int, int, int]:
    # Managed reposts are distribution of one event inside one owned audience,
    # not independent sources. Keep the strongest counter per component for
    # that family; truly external publishers still add independent reach.
    families: dict[str, dict[str, int]] = {}
    for url in urls:
        best = None
        queries = [
            "select likes, views, forwards as shares, collected_ts from telegram_post_metric where source_url=? order by collected_ts desc limit 1",
            "select likes, views, reposts as shares, collected_ts from vk_post_metric where source_url=? order by collected_ts desc limit 1",
            "select likes, views, shares, collected_ts from social_metric_snapshot where source_url=? and status='collected' order by collected_ts desc limit 1",
        ]
        for query in queries:
            try:
                metric = con.execute(query, (url,)).fetchone()
            except sqlite3.OperationalError:
                metric = None
            if metric and (
                best is None
                or int(metric["collected_ts"] or 0) > int(best["collected_ts"] or 0)
            ):
                best = metric
        if not best:
            continue
        lower_url = str(url or "").lower()
        if re.search(r"t\.me/(?:kldevents|kenigevents)/\d+", lower_url) or re.search(
            r"wall-?(?:231920894|231828790)_\d+", lower_url
        ):
            family = "owned:kenigevents"
        else:
            family = f"url:{lower_url}"
        current = families.setdefault(family, {"likes": 0, "views": 0, "shares": 0})
        current["likes"] = max(current["likes"], int(best["likes"] or 0))
        current["views"] = max(current["views"], int(best["views"] or 0))
        current["shares"] = max(current["shares"], int(best["shares"] or 0))
    return (
        sum(item["likes"] for item in families.values()),
        sum(item["views"] for item in families.values()),
        sum(item["shares"] for item in families.values()),
        len(families),
    )


def popularity_signals(
    con: sqlite3.Connection,
    urls: list[str],
) -> tuple[list[str], float]:
    """Return bounded, explainable reasons derived from the four batch buckets."""
    if not urls:
        return [], 0.0
    placeholders = ",".join("?" for _ in urls)
    try:
        rows = con.execute(
            f"""
            select platform, publisher_id, source_url, age_bucket, views, likes,
                   comments, shares, collected_ts
            from social_metric_snapshot
            where status='collected' and source_url in ({placeholders})
            order by collected_ts asc
            """,
            tuple(urls),
        ).fetchall()
    except sqlite3.OperationalError:
        return [], 0.0
    if not rows:
        return [], 0.0

    by_url: dict[str, dict[str, sqlite3.Row]] = defaultdict(dict)
    independent_publishers: set[str] = set()
    row_families: dict[tuple[str, str], str] = {}
    for row in rows:
        url = str(row["source_url"] or "")
        if not url:
            continue
        by_url[url][str(row["age_bucket"])] = row
        platform = str(row["platform"] or "")
        publisher = str(row["publisher_id"] or "").lower()
        if (platform == "telegram" and publisher in {"kldevents", "kenigevents"}) or (
            platform == "vk" and publisher in {"231920894", "231828790"}
        ):
            family = "owned:kenigevents"
        else:
            family = f"{platform}:{publisher}"
        if family != "owned:kenigevents":
            independent_publishers.add(family)
        row_families[(url, str(row["age_bucket"]))] = family

    fast_growth = False
    latest_rows: list[sqlite3.Row] = []
    for bucket_rows in by_url.values():
        one, six = bucket_rows.get("1h"), bucket_rows.get("6h")
        if one and six:
            one_views, six_views = int(one["views"] or 0), int(six["views"] or 0)
            one_likes, six_likes = int(one["likes"] or 0), int(six["likes"] or 0)
            if (six_views - one_views >= max(100, one_views // 2)) or (
                six_likes - one_likes >= max(3, one_likes // 2)
            ):
                fast_growth = True
        latest_rows.append(max(bucket_rows.values(), key=lambda item: int(item["collected_ts"] or 0)))

    # Original @kldevents post and its managed @kenigevents forward are one
    # owned publisher family, not two independent sources. Use the strongest
    # current counter in that family for reason thresholds instead of blindly
    # summing an internally forwarded message twice.
    family_totals: dict[str, dict[str, int]] = {}
    for row in latest_rows:
        family = row_families.get(
            (str(row["source_url"] or ""), str(row["age_bucket"])),
            f"{row['platform']}:{row['publisher_id']}",
        )
        current = family_totals.setdefault(family, {"views": 0, "shares": 0, "comments": 0})
        current["views"] = max(current["views"], int(row["views"] or 0))
        current["shares"] = max(current["shares"], int(row["shares"] or 0))
        current["comments"] = max(current["comments"], int(row["comments"] or 0))
    total_views = sum(item["views"] for item in family_totals.values())
    total_shares = sum(item["shares"] for item in family_totals.values())
    total_comments = sum(item["comments"] for item in family_totals.values())
    frequently_shared = total_shares >= 2 and (
        total_shares >= 5 or total_views <= 0 or total_shares / max(1, total_views) >= 0.003
    )
    discussed = total_comments >= 3 and (
        total_comments >= 10 or total_views <= 0 or total_comments / max(1, total_views) >= 0.001
    )

    reasons: list[str] = []
    if fast_growth:
        reasons.append("fast_growth")
    if frequently_shared:
        reasons.append("frequently_shared")
    if discussed:
        reasons.append("discussed")
    # Publishing an imported event into our own distribution network is not
    # independent evidence that the event is popular. Keep owned counters for
    # growth/share/discussion thresholds, but require two distinct external
    # publisher families for the "multi_source" reason.
    if len(independent_publishers) >= 2:
        reasons.append("multi_source")
    weights = {
        "fast_growth": 3.0,
        "frequently_shared": 2.5,
        "discussed": 2.0,
        "multi_source": 1.0,
    }
    return reasons, round(sum(weights[reason] for reason in reasons), 4)

def split_current_datetime(value: str | None, current_date: str) -> tuple[str, str | None]:
    raw = str(value or "").strip()
    if not raw:
        return current_date, None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("current_datetime must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BUILD_TIME_ZONE)
    local = parsed.astimezone(BUILD_TIME_ZONE)
    effective_date = local.date().isoformat()
    if effective_date != current_date:
        raise ValueError("current_date/current_datetime mismatch")
    return effective_date, local.strftime("%H:%M")


def event_active_where(
    current_date: str,
    current_time: str | None = None,
    *,
    columns: set[str] | None = None,
) -> str:
    available = columns
    # Keep the whole local calendar day in the shared public catalog.
    # `/segodnya/` needs elapsed one-off events to render the accepted muted
    # state; removing them here can collapse its mobile rail late in the day.
    # Time-sensitive surfaces such as Popular apply their own start-instant
    # eligibility after export. `current_time` remains for caller compatibility
    # and ledger parity, but must not narrow the shared catalog.
    start_not_elapsed = f"(date >= '{current_date}')"
    clauses = ["date glob '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]'"]
    if available is None or "lifecycle_status" in available:
        clauses.append("coalesce(nullif(trim(lifecycle_status),''),'active') = 'active'")
    if available is None or "silent" in available:
        clauses.append("coalesce(silent,0) = 0")
    if available is None or "end_date" in available:
        clauses.append(
            f"({start_not_elapsed} or (end_date glob '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]' and end_date >= '{current_date}'))"
        )
    else:
        clauses.append(start_not_elapsed)
    return " and ".join(clauses)


def fetch_rows(
    con: sqlite3.Connection,
    limit: int | None,
    current_date: str,
    include_ids: list[int],
    *,
    current_time: str | None = None,
    focus_date_from: str | None = None,
    focus_date_to: str | None = None,
) -> list[sqlite3.Row]:
    """Return the public projection in deterministic order.

    ``limit=None`` is the only accepted full-catalog mode.  Keeping it distinct
    from ``0`` prevents a production build from silently turning an accidental
    zero into an empty but otherwise valid artifact.
    """
    if limit is not None and limit <= 0:
        raise ValueError("preview limit must be positive; use catalog_mode=full")
    rows_by_id: dict[int, sqlite3.Row] = {}
    ordered_rows: list[sqlite3.Row] = []

    def add_row(row: sqlite3.Row) -> bool:
        if public_projection_gate_reason(row):
            return False
        if title_looks_prompt_leak(row["title"]):
            return False
        event_id = int(row["id"])
        if event_id in rows_by_id:
            return False
        rows_by_id[event_id] = row
        ordered_rows.append(row)
        return True

    def add_query(query: str, params: tuple[Any, ...] = ()) -> None:
        for row in con.execute(query, params):
            add_row(row)
            if limit is not None and len(ordered_rows) >= limit:
                break

    event_columns = {
        str(row[1]) for row in con.execute("pragma table_info('event')").fetchall()
    }
    active = event_active_where(current_date, current_time, columns=event_columns)

    if include_ids:
        placeholders = ",".join("?" for _ in include_ids)
        for row in con.execute(f"select * from event where {active} and id in ({placeholders})", include_ids):
            add_row(row)

    time_order = (
        "coalesce(nullif(time,''),'23:59') asc, " if "time" in event_columns else ""
    )
    today_time_order = f"{time_order}id asc"
    single_day_clause = (
        "and (end_date is null or trim(end_date) = '' or end_date = date)"
        if "end_date" in event_columns
        else ""
    )
    if limit is None:
        for row in con.execute(
            f"select * from event where {active} order by date asc, {time_order}id asc"
        ):
            add_row(row)
        return ordered_rows

    focus_from = normalize_date(focus_date_from)
    focus_to = normalize_date(focus_date_to)
    if focus_from and focus_to:
        add_query(
            f"""
            select * from event
            where {active}
              and date between ? and ?
              {single_day_clause}
            order by date asc, {time_order}id asc
            limit ?
            """,
            (focus_from, focus_to, max(limit * 2, 80)),
        )
    elif focus_from:
        add_query(
            f"""
            select * from event
            where {active}
              and date >= ?
              {single_day_clause}
            order by date asc, {time_order}id asc
            limit ?
            """,
            (focus_from, max(limit * 2, 80)),
        )
    # Preview/focus-group builds must be testable as a real "today" page.  The
    # old date-ascending slice was quickly filled by long-running exhibitions,
    # hiding same-day concerts/workshops/meetings from `/segodnya/`.
    add_query(
        f"select * from event where {active} and date = ? order by {today_time_order} limit ?",
        (current_date, max(limit, 80)),
    )
    if len(ordered_rows) < limit:
        add_query(
            f"""
            select * from event
            where {active}
              and date > ?
              {single_day_clause}
            order by date asc, {time_order}id asc
            limit ?
            """,
            (current_date, max(limit * 2, 80)),
        )
    if len(ordered_rows) < limit and "end_date" in event_columns:
        add_query(
            f"""
            select * from event
            where {active}
              and date < ?
              and end_date glob '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
              and end_date >= ?
            order by end_date asc, date desc, {time_order}id asc
            limit ?
            """,
            (current_date, current_date, max(limit * 2, 80)),
        )
    if len(ordered_rows) < limit:
        add_query(
            f"select * from event where {active} order by date asc, {time_order}id asc limit ?",
            (max(limit * 3, limit + len(include_ids) + 20),),
        )
    return ordered_rows[:limit]


EVENT_DETAIL_ARCHIVE_DAYS = 30


def fetch_recent_event_detail_archive_rows(
    con: sqlite3.Connection,
    current_date: str,
    *,
    retention_days: int = EVENT_DETAIL_ARCHIVE_DAYS,
) -> list[sqlite3.Row]:
    """Return recently elapsed public events for direct detail URLs only.

    These rows are deliberately kept out of ``preview-events.json`` so they
    cannot leak into current listings, Search, Popular, recommendations or
    personalization. The small grace window prevents a shared/reviewed event
    link from becoming a 404 the morning after its occurrence.
    """
    current_day = date.fromisoformat(current_date)
    cutoff = (current_day - timedelta(days=max(1, retention_days))).isoformat()
    rows: list[sqlite3.Row] = []
    columns = {str(row[1]) for row in con.execute("pragma table_info('event')")}
    end_expression = (
        "coalesce(nullif(trim(end_date),''), date)"
        if "end_date" in columns
        else "date"
    )
    lifecycle_clause = (
        "and coalesce(nullif(trim(lifecycle_status),''),'active') = 'active'"
        if "lifecycle_status" in columns
        else ""
    )
    silent_clause = "and coalesce(silent,0) = 0" if "silent" in columns else ""
    query = f"""
        select * from event
        where date glob '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
          and {end_expression} >= ?
          and {end_expression} < ?
          {lifecycle_clause}
          {silent_clause}
        order by {end_expression} desc, id asc
    """
    for row in con.execute(query, (cutoff, current_date)):
        if public_projection_gate_reason(row):
            continue
        if title_looks_prompt_leak(row_get(row, "title")):
            continue
        rows.append(row)
    return rows


CATALOG_LEDGER_SCHEMA_VERSION = "static_event_catalog_ledger_v1"
ELIGIBILITY_PREDICATE_VERSION = "static_event_public_projection_v2"


def _candidate_catalog_rows(
    con: sqlite3.Connection,
    current_date: str,
    current_time: str | None,
) -> list[sqlite3.Row]:
    """Return date-relevant rows before public eligibility filtering."""
    columns = {str(row[1]) for row in con.execute("pragma table_info('event')")}
    # Match `event_active_where`: include all events on the current local date,
    # including already-started one-offs shown by Today as elapsed/muted.
    start_not_elapsed = f"date >= '{current_date}'"
    if "end_date" in columns:
        date_clause = (
            f"({start_not_elapsed} or "
            f"(end_date glob '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]' and end_date >= '{current_date}'))"
        )
    else:
        date_clause = start_not_elapsed
    return list(con.execute(f"select * from event where {date_clause} order by id asc"))


def catalog_exclusion_reason(row: sqlite3.Row) -> str | None:
    """Return one bounded reason code for a date-relevant non-public row."""
    if row_has_key(row, "silent") and bool(row_get(row, "silent")):
        return "silent"
    if row_has_key(row, "lifecycle_status"):
        # Match ``event_active_where`` exactly.  The source contract stores the
        # canonical lowercase value; accepting a differently-cased value only
        # in the ledger would make eligible/export parity impossible to prove.
        lifecycle = clean_text(row_get(row, "lifecycle_status")) or "active"
        if lifecycle != "active":
            return "lifecycle:not_active"
    reason = public_projection_gate_reason(row)
    if reason:
        return reason
    if title_looks_prompt_leak(row_get(row, "title")):
        return "title:prompt_leakage"
    return None


def build_catalog_ledger(
    con: sqlite3.Connection,
    exported_rows: list[sqlite3.Row],
    *,
    current_date: str,
    current_time: str | None,
    generated_at: str,
    repo_sha: str,
    run_id: str,
    build_id: str,
    snapshot_id: str,
    snapshot_sha256: str,
    snapshot_size: int | None,
) -> dict[str, Any]:
    exported_by_id = {int(row["id"]): row for row in exported_rows}
    eligible: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in _candidate_catalog_rows(con, current_date, current_time):
        event_id = int(row["id"])
        reason = catalog_exclusion_reason(row)
        if reason is None:
            if event_id not in exported_by_id:
                raise RuntimeError(f"eligible event {event_id} missing from full-catalog export")
            eligible.append({
                "event_id": event_id,
                "source_revision": clean_text(row_get(row, "revision")) or None,
                "updated_at": clean_text(row_get(row, "updated_at")) or clean_text(row_get(row, "added_at")) or None,
                # Record the accepted public projection, not an unaccepted raw
                # candidate whose status is still unknown/review.
                "age_restriction": event_age_projection(row)["age_restriction"],
            })
        else:
            excluded.append({"event_id": event_id, "reason": reason})
    extra = sorted(set(exported_by_id) - {item["event_id"] for item in eligible})
    if extra:
        raise RuntimeError(f"ineligible events leaked into full-catalog export: {extra[:20]}")
    max_updated_at = max((str(item["updated_at"]) for item in eligible if item["updated_at"]), default=None)
    numeric_revisions = [
        int(item["source_revision"])
        for item in eligible
        if str(item.get("source_revision") or "").isdigit()
    ]
    return {
        "schema_version": CATALOG_LEDGER_SCHEMA_VERSION,
        "eligibility_predicate_version": ELIGIBILITY_PREDICATE_VERSION,
        "generated_at": generated_at,
        "current_date": current_date,
        "current_time": current_time,
        "repo_sha": repo_sha,
        "run_id": run_id,
        "build_id": build_id,
        "snapshot": {
            "snapshot_id": snapshot_id,
            "sha256": snapshot_sha256,
            "size": snapshot_size,
            "max_event_revision": max(numeric_revisions, default=None),
            "max_event_updated_at": max_updated_at,
        },
        "eligible_count": len(eligible),
        "excluded_count": len(excluded),
        "eligible": eligible,
        "excluded": excluded,
    }


def _structured_unusual_semantic_record(
    row: sqlite3.Row | dict[str, Any],
) -> dict[str, Any]:
    """Project only canonical structured eligibility facts for semantic feeds."""

    identity_status = clean_text(row_get(row, "identity_status")).lower()
    merged_into_event_id = row_get(row, "merged_into_event_id")
    silent_value = row_get(row, "silent") if row_has_key(row, "silent") else None
    eligible = bool(
        row_has_key(row, "identity_status")
        and identity_status == "canonical"
        and row_has_key(row, "merged_into_event_id")
        and merged_into_event_id in {None, "", 0, "0"}
        and row_has_key(row, "silent")
        and silent_value in {False, 0}
        and clean_text(row_get(row, "lifecycle_status")).lower() == "active"
    )
    return {
        "semantic_record_version": "canonical-event-semantic-v1",
        "record_kind": "event",
        "eventness_status": "event" if eligible else "untrusted",
        "identity_status": identity_status or None,
        "merged_into_event_id": merged_into_event_id,
        "silent": bool(silent_value) if silent_value is not None else None,
        "is_public": eligible,
        "is_searchable": eligible,
        "publication_status": "published" if eligible else "untrusted",
    }


def build_event(
    con: sqlite3.Connection,
    row: sqlite3.Row,
    current_date: str,
    *,
    participants: list[dict[str, Any]] | None = None,
    video_assets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    event_id = int(row["id"])
    occurrence = structured_occurrence_projection(row)
    occurrence_conflict = bool(
        occurrence
        and source_event_type_conflicts(row_get(row, "event_type"), occurrence["event_type"])
    )
    title = strip_emoji_prefix(occurrence["title"] if occurrence_conflict else row["title"]) or f"Событие {event_id}"
    topics = [str(item) for item in read_json(row["topics"], []) if str(item).strip()]
    event_type = occurrence["event_type"] if occurrence_conflict and occurrence else infer_event_type(row, topics)
    city = clean_place(row["city"])
    venue = clean_place(row["location_name"])
    venue = drop_city_only_venue(venue, city)
    address = clean_place(row["location_address"])
    start_date = normalize_date(row["date"]) or current_date
    end_date = normalize_date(row["end_date"])
    start_time, time_end, display_time = split_time(row["time"])
    description = str(
        occurrence["description"]
        if occurrence_conflict and occurrence
        else row["description"] or row["source_text"] or ""
    ).strip()
    if not occurrence_conflict and description_looks_truncated(description, str(row["source_text"] or "")):
        description = str(row["source_text"] or description).strip()
    if not time_end and (not end_date or end_date == start_date):
        duration_minutes = explicit_event_duration_minutes(row["source_text"], description)
        derived_end_date, derived_end_time = event_end_from_duration(start_date, start_time, duration_minutes)
        if derived_end_time:
            time_end = derived_end_time
            end_date = derived_end_date if derived_end_date != start_date else end_date
    starts_at = f"{start_date}T{start_time}:00+02:00" if start_time else None
    end_at = f"{end_date or start_date}T{time_end}:00+02:00" if time_end else None
    ticket = ticket_info(row)
    status = status_label(row, ticket)
    ticket, status = apply_preview_overrides(event_id, ticket, status)
    primary_image, image_mode, image_role, image_assets = collect_images(con, event_id, row["photo_urls"], title)
    source_urls = collect_source_urls(con, event_id, row)
    source_likes, source_views, source_shares, engagement_sources = source_metrics(con, source_urls)
    popularity_reason_codes, popularity_signal_score = popularity_signals(con, source_urls)
    summary = event_summary(None if occurrence_conflict else row["short_description"], description)
    description = description or summary
    primary_asset = next((asset for asset in image_assets if asset.get("src") == primary_image), image_assets[0] if image_assets else None)
    primary_alt = primary_asset.get("alt") if primary_asset else f"Изображение события «{title}»"
    slug_parts = [slugify(title), slugify(city or venue or "kaliningrad")]
    slug = f"{'-'.join(part for part in slug_parts if part)}-{event_id}"
    source_url = source_urls[0] if source_urls else None
    organizer_names = event_organizer_names(row_get(row, "organizer_names"))
    participants = event_participants(con, event_id) if participants is None else participants
    video_assets = (
        event_video_assets_for_events(con, [event_id]).get(event_id, [])
        if video_assets is None
        else video_assets
    )
    age_projection = event_age_projection(row)
    structured_semantic_record = _structured_unusual_semantic_record(row)
    return {
        "id": event_id,
        "source_prod_id": event_id,
        "title": title,
        "slug": slug,
        "event_type": event_type,
        "festival": clean_text(row["festival"]) or None,
        "organizer_names": organizer_names,
        "participants": participants,
        "status_label": status,
        "lifecycle_status": clean_text(row["lifecycle_status"]) or "active",
        # Hash-bound semantic consumers must re-check the actual structured
        # Event/public-projection verdict instead of inferring eventness from
        # title/description keywords. Old snapshots missing these columns
        # export an explicit untrusted record and therefore fail closed.
        **structured_semantic_record,
        "starts_at": starts_at,
        "start_date": start_date,
        "start_time": start_time,
        "end_date": end_date,
        "end_at": end_at,
        "time_range_end": time_end,
        "duration_forecast_minutes": forecast_event_duration_minutes(
            row_get(row, "duration_forecast_minutes")
        ),
        "timezone": TZ,
        "display_date": start_date,
        "display_time": display_time,
        "city": city,
        "venue_name": venue,
        "address": address,
        "map_query": ", ".join([part for part in [city, venue, address] if part]) or None,
        "ticket": ticket,
        **age_projection,
        "source_url": source_url,
        "source_urls": source_urls,
        "source_count": len(source_urls),
        "telegraph_url": clean_text(row["telegraph_url"]) or None,
        "image_url": primary_image,
        "image_alt": primary_alt,
        "image_text_mode": image_mode,
        "image_media_role": image_role,
        "image_assets": image_assets,
        # Data-only contract for a future click-to-play vertical video rail.
        # Rendering stays deliberately out of this exporter change.
        "video_assets": video_assets,
        "face_boxes": list(primary_asset.get("face_boxes") or []) if primary_asset else [],
        "valuable_region": primary_asset.get("valuable_region") if primary_asset else None,
        "ocr_boxes": [],
        "focal_point": primary_asset.get("focal_point") if primary_asset else None,
        "image_object_position": primary_asset.get("recommended_object_position") if primary_asset else None,
        "safe_crop": bool(primary_asset and primary_asset.get("safe_crop")),
        "summary": summary,
        "meta_description": summary,
        "description_html": markdownish_to_html(description) or f"<p>{html.escape(summary or title)}</p>",
        "topics": topics,
        "pushkin_card": bool(row["pushkin_card"]),
        "other_date_ids": sorted({
            int(item)
            for item in read_json(row_get(row, "linked_event_ids"), [])
            if str(item).isdigit() and int(item) != event_id
        }),
        "data_quality_notes": ["structured_source_occurrence_conflict_guard"] if occurrence_conflict else [],
        "updated_at": clean_text(row_get(row, "updated_at")) or clean_text(row_get(row, "added_at")) or None,
        "likes_count": source_likes,
        "source_likes_count": source_likes,
        "service_likes_count": 0,
        "source_views_count": source_views,
        "source_engagement_sources_count": engagement_sources,
        "shares_count": source_shares,
        "popularity_reason_codes": popularity_reason_codes,
        "popularity_signal_score": popularity_signal_score,
    }


INTEREST_CLUBS_SCHEMA_VERSION = "interest-clubs-static-v1"
INTEREST_CLUBS_V2_SCHEMA_VERSION = "interest-clubs-static-v2"
INTEREST_CLUB_REQUIRED_COLUMNS = {
    "id",
    "slug",
    "canonical_name",
    "topic",
    "public_status",
}
INTEREST_CLUB_EVENT_REQUIRED_COLUMNS = {"club_id", "event_id", "status"}
INTEREST_CLUB_EVALUATION_REQUIRED_COLUMNS = {
    "club_id",
    "event_id",
    "status",
    "verdict",
    "policy_version",
    "input_hash",
}
INTEREST_CLUB_EVENT_SOURCE_COLUMNS = ("source_url", "url", "public_url")
FESTIVAL_TIMELINE_SCHEMA_VERSION = "festival-timeline-static-v1"
FESTIVAL_TIMELINE_REQUIRED_COLUMNS = {
    "id",
    "calendar_year",
    "slug",
    "title",
    "description",
    "start_date",
    "end_date",
    "date_precision",
    "date_label",
    "sort_date",
    "month_key",
    "display_order",
    "place_label",
    "category",
    "status",
    "status_label",
    "source_url",
    "source_label",
    "internal_event_id",
    "festival_id",
    "cover_key",
    "image_width",
    "image_height",
    "media_mode",
    "object_position",
    "catalog_version",
    "is_public",
}


def _sqlite_table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    """Return a SQLite table contract without interpolating untrusted names."""

    if table not in {
        "interest_club",
        "interest_club_event",
        "interest_club_evaluation",
        "event",
        "event_source",
        "festival",
        "festival_calendar_item",
    }:
        return set()
    try:
        return {str(row[1]) for row in con.execute(f"pragma table_info('{table}')")}
    except sqlite3.DatabaseError:
        return set()


def _club_event_source_url(
    con: sqlite3.Connection,
    event_id: int,
    event_row: sqlite3.Row,
    event_columns: set[str],
) -> str | None:
    """Select one already-public URL; never synthesize or expose provenance JSON."""

    for column in ("source_post_url", "source_vk_post_url", "tg_event_post_url", "vk_repost_url"):
        if column in event_columns:
            value = clean_text(row_get(event_row, column))
            if value.startswith(("https://", "http://")):
                return value
    source_columns = _sqlite_table_columns(con, "event_source")
    for column in INTEREST_CLUB_EVENT_SOURCE_COLUMNS:
        if column not in source_columns or "event_id" not in source_columns:
            continue
        try:
            source_row = con.execute(
                f"select {column} from event_source where event_id=? and {column} is not null order by rowid asc limit 1",
                (event_id,),
            ).fetchone()
        except sqlite3.DatabaseError:
            continue
        value = clean_text(source_row[0] if source_row else None)
        if value.startswith(("https://", "http://")):
            return value
    return None


def _club_event_is_public(row: sqlite3.Row) -> bool:
    """Apply the public event gates again at the club-relation boundary."""

    if public_projection_gate_reason(row):
        return False
    if row_has_key(row, "lifecycle_status") and clean_text(row_get(row, "lifecycle_status") or "active").lower() != "active":
        return False
    if row_has_key(row, "silent") and bool(row_get(row, "silent")):
        return False
    # Festival/program identity is independent from a club identity. A future
    # explicit product contract may allow a co-hosted relation; v1 fails closed.
    if row_has_key(row, "festival") and clean_text(row_get(row, "festival")):
        return False
    return True


def build_interest_clubs_projection(
    con: sqlite3.Connection,
    *,
    current_date: str,
    generated_at: str,
    exported_events: list[dict[str, Any]],
    enabled: bool = False,
) -> dict[str, Any]:
    """Build the versioned public club projection from canonical SQLite rows.

    This function is the single mapping seam shared with the core club model.
    Missing or incompatible tables return a valid empty projection: a static
    build remains deployable without turning shadow/deferred rows into content.
    """

    if not enabled:
        return {
            "schema_version": INTEREST_CLUBS_SCHEMA_VERSION,
            "projection_version": 1,
            "generated_at": generated_at,
            "current_date": current_date,
            "source": "disabled-by-build-gate",
            "clubs": [],
        }

    club_columns = _sqlite_table_columns(con, "interest_club")
    relation_columns = _sqlite_table_columns(con, "interest_club_event")
    evaluation_columns = _sqlite_table_columns(con, "interest_club_evaluation")
    event_columns = _sqlite_table_columns(con, "event")
    contract_available = (
        INTEREST_CLUB_REQUIRED_COLUMNS.issubset(club_columns)
        and INTEREST_CLUB_EVENT_REQUIRED_COLUMNS.issubset(relation_columns)
        and INTEREST_CLUB_EVALUATION_REQUIRED_COLUMNS.issubset(evaluation_columns)
        and {"id", "title", "date"}.issubset(event_columns)
    )
    projection = {
        "schema_version": INTEREST_CLUBS_SCHEMA_VERSION,
        "projection_version": 1,
        "generated_at": generated_at,
        "current_date": current_date,
        "source": "sqlite-interest-clubs-v1" if contract_available else "empty-contract-fallback",
        "clubs": [],
    }
    if not contract_available:
        return projection

    exported_slug_by_id = {
        int(item["id"]): str(item["slug"])
        for item in exported_events
        if str(item.get("id", "")).isdigit() and clean_text(item.get("slug"))
    }
    club_rows = con.execute(
        "select * from interest_club where public_status='approved' order by canonical_name collate nocase, id"
    ).fetchall()
    clubs: list[dict[str, Any]] = []
    for club_row in club_rows:
        slug = clean_text(row_get(club_row, "slug"))
        name = clean_text(row_get(club_row, "canonical_name"))
        topic = clean_text(row_get(club_row, "topic"))
        if not name or not topic or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug):
            continue
        club_id = int(row_get(club_row, "id"))
        relation_rows = con.execute(
            """
            select e.*
            from interest_club_event ice
            join event e on e.id = ice.event_id
            where ice.club_id=? and ice.status='active'
              and exists (
                select 1 from interest_club_evaluation ie
                where ie.club_id=ice.club_id
                  and ie.event_id=ice.event_id
                  and ie.status='accepted'
                  and ie.verdict='yes'
                  and ie.policy_version=ice.policy_version
                  and ie.input_hash=ice.input_hash
              )
            order by e.date asc, e.id asc
            """,
            (club_id,),
        ).fetchall()
        public_events = [row for row in relation_rows if _club_event_is_public(row)]
        if not public_events:
            continue

        observed_dates = sorted({str(row["date"]) for row in public_events})
        # Public v1 requires recurring evidence and current activity. This is a
        # retrieval/publication gate, not a semantic club decision: the latter
        # remains owned by the approved identity and active relation verdicts.
        if len(observed_dates) < 2:
            continue
        current_day = date.fromisoformat(current_date)
        last_observed_day = date.fromisoformat(observed_dates[-1])
        has_current_or_future_meeting = any(value >= current_date for value in observed_dates)
        if not has_current_or_future_meeting and (current_day - last_observed_day).days > 90:
            continue
        future_meetings: list[dict[str, Any]] = []
        for event_row in public_events:
            start_date = str(event_row["date"])
            if start_date < current_date:
                continue
            event_id = int(event_row["id"])
            event_slug = exported_slug_by_id.get(event_id)
            source_url = _club_event_source_url(con, event_id, event_row, event_columns)
            start_time, _, display_time = split_time(row_get(event_row, "time"))
            city = clean_place(row_get(event_row, "city"))
            venue = drop_city_only_venue(clean_place(row_get(event_row, "location_name")), city)
            future_meetings.append(
                {
                    "event_id": event_id,
                    "title": strip_emoji_prefix(row_get(event_row, "title")) or f"Событие {event_id}",
                    "start_date": start_date,
                    "start_time": start_time,
                    "display_time": display_time,
                    "city": city,
                    "venue_name": venue,
                    "event_path": f"/sobytiya/{event_slug}/" if event_slug else None,
                    "source_url": source_url,
                }
            )

        clubs.append(
            {
                "id": club_id,
                "slug": slug,
                "name": name,
                "topic": topic,
                "description": clean_text(row_get(club_row, "description")) or None,
                "city": clean_place(row_get(club_row, "city")),
                "typical_venue": clean_place(row_get(club_row, "typical_place")),
                "activity": {
                    "meeting_count": len(public_events),
                    "distinct_date_count": len(observed_dates),
                    "first_observed_date": observed_dates[0],
                    "last_observed_date": observed_dates[-1],
                    "future_meeting_count": len(future_meetings),
                },
                "future_meetings": future_meetings,
                "updated_at": clean_text(row_get(club_row, "updated_at")) or None,
            }
        )
    projection["clubs"] = clubs
    return projection


def _calendar_months_before(value: date, months: int) -> date:
    ordinal = value.year * 12 + (value.month - 1) - int(months)
    year, month0 = divmod(ordinal, 12)
    month = month0 + 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def _club_event_v2_exclusion_reason(row: sqlite3.Row) -> str | None:
    reason = public_projection_gate_reason(row)
    if reason:
        return "event_public_gate"
    if clean_text(row_get(row, "lifecycle_status") or "active").lower() != "active":
        return "event_lifecycle"
    if bool(row_get(row, "silent")):
        return "event_silent"
    return None


def build_interest_clubs_projection_v2(
    con: sqlite3.Connection,
    *,
    current_date: str,
    generated_at: str,
    exported_events: list[dict[str, Any]],
    enabled: bool = False,
) -> dict[str, Any]:
    """Build the six-calendar-month club registry projection.

    Relations remain semantic truth only when their exact hash/policy has an
    accepted evaluation. Festival containment is not a negative publication
    signal here: it neither creates nor invalidates a grounded relation.
    """

    receipt = {
        "approved_identity_count": 0,
        "invalid_identity_count": 0,
        "non_active_relation_count": 0,
        "unaccepted_relation_count": 0,
        "event_public_gate_count": 0,
        "event_lifecycle_count": 0,
        "event_silent_count": 0,
        "outside_six_month_window_count": 0,
        "dormant_identity_count": 0,
        "catalog_event_id_omitted_count": 0,
        "festival_relation_allowed_count": 0,
    }
    projection: dict[str, Any] = {
        "schema_version": INTEREST_CLUBS_V2_SCHEMA_VERSION,
        "projection_version": 2,
        "generated_at": generated_at,
        "current_date": current_date,
        "source": "disabled-by-build-gate",
        "window": {},
        "exclusion_receipts": receipt,
        "clubs": [],
    }
    if not enabled:
        return projection

    club_columns = _sqlite_table_columns(con, "interest_club")
    relation_columns = _sqlite_table_columns(con, "interest_club_event")
    evaluation_columns = _sqlite_table_columns(con, "interest_club_evaluation")
    event_columns = _sqlite_table_columns(con, "event")
    contract_available = (
        INTEREST_CLUB_REQUIRED_COLUMNS.issubset(club_columns)
        and INTEREST_CLUB_EVENT_REQUIRED_COLUMNS.issubset(relation_columns)
        and INTEREST_CLUB_EVALUATION_REQUIRED_COLUMNS.issubset(evaluation_columns)
        and {"id", "title", "date"}.issubset(event_columns)
    )
    projection["source"] = (
        "sqlite-interest-clubs-v2" if contract_available else "empty-contract-fallback"
    )
    if not contract_available:
        return projection

    current_day = date.fromisoformat(current_date)
    cutoff_6m = _calendar_months_before(current_day, 6)
    cutoff_12m = _calendar_months_before(current_day, 12)
    projection["window"] = {
        "six_months_start_inclusive": cutoff_6m.isoformat(),
        "twelve_months_start_inclusive": cutoff_12m.isoformat(),
        "as_of_inclusive": current_date,
    }
    exported_slug_by_id = {
        int(item["id"]): str(item["slug"])
        for item in exported_events
        if str(item.get("id", "")).isdigit() and clean_text(item.get("slug"))
    }
    club_rows = con.execute(
        "select * from interest_club where public_status='approved' "
        "order by canonical_name collate nocase, id"
    ).fetchall()
    receipt["approved_identity_count"] = len(club_rows)
    clubs: list[dict[str, Any]] = []
    for club_row in club_rows:
        club_id = int(row_get(club_row, "id"))
        slug = clean_text(row_get(club_row, "slug"))
        name = clean_text(row_get(club_row, "canonical_name"))
        topic = clean_text(row_get(club_row, "topic"))
        if not name or not topic or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug):
            receipt["invalid_identity_count"] += 1
            continue

        receipt["non_active_relation_count"] += int(
            con.execute(
                "select count(*) from interest_club_event where club_id=? and status<>'active'",
                (club_id,),
            ).fetchone()[0]
        )
        relation_rows = con.execute(
            """
            select e.*,
                   ice.input_hash as club_relation_input_hash,
                   ice.policy_version as club_relation_policy_version,
                   ice.updated_at as club_relation_updated_at,
                   ie.id as accepted_evaluation_id,
                   ie.updated_at as accepted_evaluation_updated_at
            from interest_club_event ice
            join event e on e.id=ice.event_id
            left join interest_club_evaluation ie
              on ie.club_id=ice.club_id
             and ie.event_id=ice.event_id
             and ie.status='accepted'
             and ie.verdict='yes'
             and ie.policy_version=ice.policy_version
             and ie.input_hash=ice.input_hash
            where ice.club_id=? and ice.status='active'
            order by e.date,e.id
            """,
            (club_id,),
        ).fetchall()
        eligible_rows: list[sqlite3.Row] = []
        history_rows_12m: list[sqlite3.Row] = []
        for event_row in relation_rows:
            if row_get(event_row, "accepted_evaluation_id") is None:
                receipt["unaccepted_relation_count"] += 1
                continue
            exclusion = _club_event_v2_exclusion_reason(event_row)
            if exclusion:
                receipt[f"{exclusion}_count"] += 1
                continue
            if clean_text(row_get(event_row, "festival")):
                receipt["festival_relation_allowed_count"] += 1
            event_end = date.fromisoformat(
                clean_text(row_get(event_row, "end_date"))
                or clean_text(row_get(event_row, "date"))
            )
            if event_end >= cutoff_12m:
                history_rows_12m.append(event_row)
            if event_end < cutoff_6m:
                receipt["outside_six_month_window_count"] += 1
                continue
            eligible_rows.append(event_row)

        if not eligible_rows:
            receipt["dormant_identity_count"] += 1
            continue

        dates = [date.fromisoformat(clean_text(row_get(row, "date"))) for row in eligible_rows]
        historical_dates = [value for value in dates if value <= current_day]
        future_dates = [value for value in dates if value >= current_day]
        count_6m = sum(cutoff_6m <= value <= current_day for value in dates)
        count_12m = sum(
            cutoff_12m <= date.fromisoformat(clean_text(row_get(row, "date"))) <= current_day
            for row in history_rows_12m
        )
        catalog_event_ids: list[int] = []
        future_meetings: list[dict[str, Any]] = []
        for event_row in eligible_rows:
            event_id = int(row_get(event_row, "id"))
            event_slug = exported_slug_by_id.get(event_id)
            if event_slug:
                catalog_event_ids.append(event_id)
            else:
                receipt["catalog_event_id_omitted_count"] += 1
            start_date = clean_text(row_get(event_row, "date"))
            if start_date < current_date or not event_slug:
                continue
            start_time, _, display_time = split_time(row_get(event_row, "time"))
            city = clean_place(row_get(event_row, "city"))
            venue = drop_city_only_venue(clean_place(row_get(event_row, "location_name")), city)
            future_meetings.append(
                {
                    "event_id": event_id,
                    "title": strip_emoji_prefix(row_get(event_row, "title")) or f"Событие {event_id}",
                    "start_date": start_date,
                    "start_time": start_time,
                    "display_time": display_time,
                    "city": city,
                    "venue_name": venue,
                    "event_path": f"/sobytiya/{event_slug}/",
                    "source_url": _club_event_source_url(con, event_id, event_row, event_columns),
                }
            )
        updated_candidates = [
            clean_text(row_get(club_row, "updated_at")),
            *[clean_text(row_get(row, "club_relation_updated_at")) for row in eligible_rows],
            *[clean_text(row_get(row, "accepted_evaluation_updated_at")) for row in eligible_rows],
            *[clean_text(row_get(row, "club_relation_updated_at")) for row in history_rows_12m],
            *[clean_text(row_get(row, "accepted_evaluation_updated_at")) for row in history_rows_12m],
        ]
        clubs.append(
            {
                "id": club_id,
                "slug": slug,
                "name": name,
                "topic": topic,
                "description": clean_text(row_get(club_row, "description")) or None,
                "city": clean_place(row_get(club_row, "city")),
                "typical_venue": clean_place(row_get(club_row, "typical_place")),
                "status": "active",
                "activity": {
                    "meeting_count_6m": count_6m,
                    "meeting_count_12m": count_12m,
                    "last_activity_date": max(historical_dates).isoformat() if historical_dates else None,
                    "next_activity_date": min(future_dates).isoformat() if future_dates else None,
                    "future_meeting_count": len(future_meetings),
                },
                "current_catalog_event_ids": sorted(set(catalog_event_ids)),
                "future_meetings": future_meetings,
                "data_updated_at": max(filter(None, updated_candidates), default=None),
            }
        )
    projection["clubs"] = clubs
    return projection


def build_festival_timeline_projection(
    con: sqlite3.Connection,
    *,
    current_date: str,
    generated_at: str,
    require_complete: bool,
) -> dict[str, Any]:
    """Export the public festival calendar from canonical core SQLite.

    A full production candidate fails closed when the edition table or its
    accepted initial 2026 catalog is missing.  Slice previews may still build an
    explicit empty projection, but never fall back to the old hardcoded page
    array.
    """

    columns = _sqlite_table_columns(con, "festival_calendar_item")
    contract_available = FESTIVAL_TIMELINE_REQUIRED_COLUMNS.issubset(columns)
    projection: dict[str, Any] = {
        "schema_version": FESTIVAL_TIMELINE_SCHEMA_VERSION,
        "projection_version": 1,
        "generated_at": generated_at,
        "current_date": current_date,
        "source": (
            "sqlite-festival-calendar-v1"
            if contract_available
            else "missing-db-contract"
        ),
        "catalog_versions": [],
        "database_row_count": 0,
        "festivals": [],
    }
    if not contract_available:
        if require_complete:
            raise ValueError(
                "full production export requires festival_calendar_item schema"
            )
        return projection

    current_year = date.fromisoformat(current_date).year
    all_public_rows = con.execute(
        """
        SELECT *
        FROM festival_calendar_item
        WHERE calendar_year>=? AND is_public=1
        ORDER BY calendar_year,display_order,id
        """,
        (current_year,),
    ).fetchall()
    projection["database_row_count"] = len(all_public_rows)
    projection["catalog_versions"] = sorted(
        {
            clean_text(row_get(row, "catalog_version"))
            for row in all_public_rows
            if clean_text(row_get(row, "catalog_version"))
        }
    )
    if require_complete:
        current_rows = [
            row for row in all_public_rows
            if int(row_get(row, "calendar_year") or 0) == current_year
        ]
        current_slugs = {clean_text(row_get(row, "slug")) for row in current_rows}
        current_orders = {int(row_get(row, "display_order") or 0) for row in current_rows}
        minimum_current_rows = 21 if current_year == 2026 else 1
        if (
            len(current_rows) < minimum_current_rows
            or len(current_slugs) != len(current_rows)
            or len(current_orders) != len(current_rows)
        ):
            raise ValueError(
                f"full production festival calendar requires unique {current_year} coverage"
            )

    festivals: list[dict[str, Any]] = []
    for row in all_public_rows:
        end_date = clean_text(row_get(row, "end_date"))
        calendar_year = int(row_get(row, "calendar_year") or 0)
        # Exact ended editions leave the current calendar. Broad/unknown-end
        # periods remain only through their declared calendar year.
        if end_date and end_date < current_date:
            continue
        if not end_date and calendar_year < current_year:
            continue
        slug = clean_text(row_get(row, "slug"))
        source_url = clean_text(row_get(row, "source_url"))
        cover_key = clean_text(row_get(row, "cover_key"))
        if (
            not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug)
            or not source_url.startswith(("https://", "http://"))
            or not cover_key.startswith("/assets/festivals/timeline/")
        ):
            if require_complete:
                raise ValueError(f"invalid festival calendar row: {slug or row_get(row, 'id')}")
            continue
        festivals.append(
            {
                "databaseId": int(row_get(row, "id")),
                "calendarYear": calendar_year,
                "festivalId": (
                    int(row_get(row, "festival_id"))
                    if row_get(row, "festival_id") is not None
                    else None
                ),
                "internalEventId": (
                    int(row_get(row, "internal_event_id"))
                    if row_get(row, "internal_event_id") is not None
                    else None
                ),
                "slug": slug,
                "title": clean_text(row_get(row, "title")),
                "description": clean_text(row_get(row, "description")),
                "startDate": clean_text(row_get(row, "start_date")) or None,
                "endDate": end_date or None,
                "datePrecision": clean_text(row_get(row, "date_precision")),
                "dateLabel": clean_text(row_get(row, "date_label")),
                "sortDate": clean_text(row_get(row, "sort_date")),
                "monthKey": clean_text(row_get(row, "month_key")),
                "displayOrder": int(row_get(row, "display_order")),
                "place": clean_text(row_get(row, "place_label")),
                "category": clean_text(row_get(row, "category")),
                "status": clean_text(row_get(row, "status")),
                "statusLabel": clean_text(row_get(row, "status_label")),
                "sourceHref": source_url,
                "sourceLabel": clean_text(row_get(row, "source_label")),
                "image": cover_key,
                "imageWidth": int(row_get(row, "image_width")),
                "imageHeight": int(row_get(row, "image_height")),
                "mediaMode": clean_text(row_get(row, "media_mode")),
                "objectPosition": clean_text(row_get(row, "object_position")) or None,
                "catalogVersion": clean_text(row_get(row, "catalog_version")),
            }
        )
    projection["festivals"] = festivals
    return projection


def category(event: dict[str, Any]) -> str:
    topics = event.get("topics") or []
    text = " ".join([str(event.get("event_type") or ""), event.get("title") or ""]).lower()
    for pattern, cat in [
        (r"архитект|урбан|городск\w*\s+сред|будущ\w*\s+город|общественн\w*\s+пространств|концепци|моделир", "urbanism"),
        (r"опера|опероман|вокал|концерт|музык", "music"),
        (r"выстав", "exhibition"),
        (r"театр|спектак", "theatre"),
        (r"лекц", "lecture"),
        (r"мастер", "workshop"),
        (r"экскурс", "excursion"),
        (r"фестив|маркет", "festival"),
    ]:
        if re.search(pattern, text):
            return cat
    for topic in topics:
        if topic in TOPIC_CATEGORY:
            return TOPIC_CATEGORY[topic]
    return "event"


def score_pair(left: dict[str, Any], right: dict[str, Any]) -> float:
    score = 0.0
    if category(left) == category(right):
        score += 0.40
    lt, rt = set(left.get("topics") or []), set(right.get("topics") or [])
    if lt or rt:
        score += 0.24 * (len(lt & rt) / max(1, len(lt | rt)))
    if left.get("city") and left.get("city") == right.get("city"):
        score += 0.12
    if left.get("venue_name") and left.get("venue_name") == right.get("venue_name"):
        score += 0.08
    try:
        delta = abs((datetime.fromisoformat(left["start_date"]) - datetime.fromisoformat(right["start_date"])).days)
        score += 0.12 * (1 if delta <= 2 else 0.75 if delta <= 7 else 0.45 if delta <= 21 else 0.15)
    except Exception:
        pass
    if bool(left.get("ticket", {}).get("is_free")) == bool(right.get("ticket", {}).get("is_free")):
        score += 0.04
    return round(min(score, 1.0), 4)


def plain_from_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def event_embedding_document(event: dict[str, Any]) -> str:
    """Canonical event text for offline sparse retrieval.

    Raw source text is intentionally not embedded verbatim: title/category/tags
    are repeated to make short real-catalog rows comparable to rich descriptions.
    """
    topics = " ".join(str(item) for item in event.get("topics") or [])
    admission = event.get("ticket", {}).get("price_label") or event.get("status_label") or ""
    parts = [
        event.get("title") or "",
        event.get("title") or "",
        event.get("event_type") or "",
        event.get("event_type") or "",
        category(event),
        topics,
        event.get("festival") or "",
        event.get("summary") or "",
        plain_from_html(event.get("description_html") or ""),
        event.get("venue_name") or "",
        event.get("city") or "",
        admission,
        "бесплатно" if event.get("ticket", {}).get("is_free") else "платно",
    ]
    return clean_text(" ".join(part for part in parts if part))


def event_fingerprint(event: dict[str, Any]) -> str:
    payload = {
        "title": event.get("title"),
        "event_type": event.get("event_type"),
        "topics": event.get("topics") or [],
        "summary": event.get("summary"),
        "description_html": event.get("description_html"),
        "city": event.get("city"),
        "venue_name": event.get("venue_name"),
        "start_date": event.get("start_date"),
        "end_date": event.get("end_date"),
        "ticket": event.get("ticket"),
        "lifecycle_status": event.get("lifecycle_status"),
        "age_restriction": event.get("age_restriction"),
        "age_restriction_status": event.get("age_restriction_status"),
        "age_recommendation": event.get("age_recommendation"),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def tokenize_for_sparse(text: str) -> list[str]:
    tokens = re.findall(r"[a-zа-яё0-9]{3,}", text.lower(), flags=re.I)
    return [token for token in tokens if token not in STOP_WORDS and not token.isdigit()]


def build_sparse_tfidf_index(events: list[dict[str, Any]]) -> dict[int, dict[str, float]]:
    docs = {int(event["id"]): tokenize_for_sparse(event_embedding_document(event)) for event in events}
    df: Counter[str] = Counter()
    for tokens in docs.values():
        df.update(set(tokens))
    total = max(1, len(docs))
    sparse_index: dict[int, dict[str, float]] = {}
    for event_id, tokens in docs.items():
        tf = Counter(tokens)
        vec: dict[str, float] = {}
        for token, count in tf.items():
            idf = math.log((1 + total) / (1 + df[token])) + 1.0
            vec[token] = (1.0 + math.log(count)) * idf
        norm = math.sqrt(sum(value * value for value in vec.values())) or 1.0
        sparse_index[event_id] = {token: value / norm for token, value in vec.items()}
    return sparse_index


def sparse_cosine(left: dict[str, float], right: dict[str, float]) -> float:
    if not left or not right:
        return 0.0
    if len(left) > len(right):
        left, right = right, left
    return float(sum(value * right.get(token, 0.0) for token, value in left.items()))


def eligible_related_pair(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if int(left["id"]) == int(right["id"]):
        return False
    if int(right["id"]) in [int(item) for item in left.get("other_date_ids", [])]:
        return False
    if int(left["id"]) in [int(item) for item in right.get("other_date_ids", [])]:
        return False
    if str(right.get("lifecycle_status") or "active") != "active":
        return False
    if is_sold_out_status(str(right.get("ticket", {}).get("status") or right.get("status_label") or "")):
        # Sold-out rows may remain indexable as event pages, but should not be
        # promoted as recommendations.
        return False
    return True


def stable_jitter(left_id: int, right_id: int, salt: str) -> float:
    raw = hashlib.sha1(f"{salt}:{left_id}:{right_id}".encode("utf-8")).hexdigest()[:8]
    value = int(raw, 16) / 0xFFFFFFFF
    return (value - 0.5) * 0.012


def normalized_related_title(value: Any) -> str:
    """Normalize a title only for graph-recall checks, never for dedup merge.

    Equal normalized titles are strong evidence that two still-separate public
    rows must at least be mutually discoverable.  They are *not* sufficient to
    merge records: venue/date/source adjudication remains owned by Smart Update.
    """

    text = clean_text(value or "").lower().replace("ё", "е")
    text = re.sub(r"[«»„“”\"'`]+", " ", text)
    text = re.sub(r"[^a-zа-я0-9]+", " ", text, flags=re.I | re.U)
    text = re.sub(r"\s+", " ", text).strip()
    # Importers may prepend a generic content-type wrapper to the same source
    # title (for example ``🖼️ Выставка «…»``).  Removing only one known leading
    # wrapper improves recall without turning arbitrary title words into a
    # dedup key.
    text = re.sub(
        r"^(?:выставка|спектакль|концерт|лекция|экскурсия|кинопоказ|мюзикл|опера|балет)\s+",
        "",
        text,
    )
    return text


def _reverse_related_item(
    item: dict[str, Any],
    *,
    event_id: int,
    reason: str,
    force_similar: bool,
) -> dict[str, Any]:
    related_score = float(item.get("related_score") or 0.0)
    if reason == "exact_normalized_title":
        related_score = max(0.97, related_score)
    elif reason == "high_confidence_reciprocal":
        related_score = max(0.84, related_score * 0.985)
    else:
        related_score = max(0.60, related_score * 0.965)
    slot_type = "pure_related" if force_similar else str(item.get("slot_type") or "adjacent_discovery")
    return {
        **item,
        "event_id": int(event_id),
        "related_score": round(min(1.0, related_score), 4),
        "slot_type": slot_type,
        "similarity_class": "same_domain" if force_similar else str(item.get("similarity_class") or "adjacent_discovery"),
        "reason_codes": list(dict.fromkeys([*(item.get("reason_codes") or []), f"mutual_link:{reason}"])),
        "retrieval_sources": list(dict.fromkeys([*(item.get("retrieval_sources") or []), "graph_reciprocity"])),
        "display_eligible": True,
    }


def _insert_forced_related(
    chain: list[dict[str, Any]],
    item: dict[str, Any],
    *,
    limit: int = 40,
) -> None:
    event_id = int(item["event_id"])
    existing = next((entry for entry in chain if int(entry.get("event_id") or 0) == event_id), None)
    if existing is not None:
        existing["reason_codes"] = list(dict.fromkeys([*(existing.get("reason_codes") or []), *(item.get("reason_codes") or [])]))
        existing["retrieval_sources"] = list(dict.fromkeys([*(existing.get("retrieval_sources") or []), *(item.get("retrieval_sources") or [])]))
        if item.get("slot_type") == "pure_related":
            existing["slot_type"] = "pure_related"
            existing["similarity_class"] = "same_domain"
        existing["related_score"] = round(max(float(existing.get("related_score") or 0), float(item.get("related_score") or 0)), 4)
        return
    chain.append(item)
    chain.sort(key=lambda entry: (-float(entry.get("related_score") or 0), -float(entry.get("vector_similarity") or entry.get("lexical_similarity") or 0), int(entry["event_id"])))
    if len(chain) > limit:
        # A forced graph-repair edge is an invariant, not a best-effort append.
        # Prefer removing the weakest ordinary edge over silently dropping it.
        removable = next(
            (index for index in range(len(chain) - 1, -1, -1) if "graph_reciprocity" not in (chain[index].get("retrieval_sources") or [])),
            len(chain) - 1,
        )
        del chain[removable]


def apply_pgvector_graph_reciprocity(
    events: list[dict[str, Any]],
    chains: dict[str, list[dict[str, Any]]],
    *,
    high_confidence_threshold: float = 0.88,
) -> dict[str, Any]:
    """Repair only high-signal graph asymmetry without creating an echo chamber."""

    by_id = {int(event["id"]): event for event in events}
    exact_title_groups: dict[str, list[int]] = defaultdict(list)
    for event in events:
        normalized = normalized_related_title(event.get("title"))
        if normalized:
            exact_title_groups[normalized].append(int(event["id"]))

    exact_links = 0
    high_confidence_links = 0
    rescue_links = 0

    def forward_item(left_id: int, right_id: int) -> dict[str, Any] | None:
        return next(
            (item for item in chains.get(str(left_id), []) if int(item.get("event_id") or 0) == right_id),
            None,
        )

    # Exact-title groups are often duplicate/occurrence suspects.  Connect each
    # row to the closest dates (all rows for small groups) so a missed upstream
    # dedup can never make the two records mutually invisible.
    for ids in exact_title_groups.values():
        if len(ids) < 2:
            continue
        ordered = sorted(ids, key=lambda event_id: (str(by_id[event_id].get("start_date") or "9999-12-31"), event_id))
        for index, left_id in enumerate(ordered):
            candidates = [right_id for right_id in ordered if right_id != left_id]
            if len(candidates) > 3:
                candidates = sorted(candidates, key=lambda right_id: (abs(ordered.index(right_id) - index), right_id))[:3]
            for right_id in candidates:
                left = by_id[left_id]
                right = by_id[right_id]
                if not eligible_related_pair(left, right):
                    continue
                source = forward_item(right_id, left_id) or forward_item(left_id, right_id) or {
                    "related_score": 0.97,
                    "vector_similarity": 0.0,
                    "deterministic_score": score_pair(left, right),
                    "confidence": 0.97,
                    "reason_codes": [],
                    "retrieval_sources": ["exact_title_recall"],
                }
                before = forward_item(left_id, right_id)
                _insert_forced_related(
                    chains.setdefault(str(left_id), []),
                    _reverse_related_item(source, event_id=right_id, reason="exact_normalized_title", force_similar=True),
                )
                if before is None:
                    exact_links += 1

    # Cosine is symmetric, but top-K truncation is not.  Restore only strong
    # reverse edges; weaker asymmetric edges remain asymmetric by design.
    for left_id_text, chain in list(chains.items()):
        left_id = int(left_id_text)
        for item in list(chain):
            right_id = int(item.get("event_id") or 0)
            if right_id not in by_id or float(item.get("vector_similarity") or 0) < high_confidence_threshold:
                continue
            if not eligible_related_pair(by_id[right_id], by_id[left_id]):
                continue
            before = forward_item(right_id, left_id)
            _insert_forced_related(
                chains.setdefault(str(right_id), []),
                _reverse_related_item(item, event_id=left_id, reason="high_confidence_reciprocal", force_similar=True),
            )
            if before is None:
                high_confidence_links += 1

    def incoming_counts() -> Counter[int]:
        result: Counter[int] = Counter()
        for chain in chains.values():
            result.update(int(item.get("event_id") or 0) for item in chain if int(item.get("event_id") or 0) in by_id)
        return result

    # Rescue dark nodes into the broader/adjacent graph through their own best
    # neighbour.  This does not label a weak pair as semantically similar.
    incoming = incoming_counts()
    for event_id in sorted(by_id):
        if incoming.get(event_id, 0) > 0:
            continue
        outgoing = next(
            (item for item in chains.get(str(event_id), []) if int(item.get("event_id") or 0) in by_id),
            None,
        )
        if not outgoing:
            continue
        neighbor_id = int(outgoing["event_id"])
        if not eligible_related_pair(by_id[neighbor_id], by_id[event_id]):
            continue
        before = forward_item(neighbor_id, event_id)
        _insert_forced_related(
            chains.setdefault(str(neighbor_id), []),
            _reverse_related_item(outgoing, event_id=event_id, reason="zero_incoming_rescue", force_similar=False),
        )
        if before is None:
            rescue_links += 1
            incoming[event_id] += 1

    final_incoming = incoming_counts()
    exact_missing: list[list[int]] = []
    for ids in exact_title_groups.values():
        if len(ids) != 2:
            continue
        left_id, right_id = ids
        if not eligible_related_pair(by_id[left_id], by_id[right_id]) or not eligible_related_pair(by_id[right_id], by_id[left_id]):
            continue
        if not forward_item(left_id, right_id) or not forward_item(right_id, left_id):
            exact_missing.append(sorted([left_id, right_id]))
    zero_incoming_event_ids = sorted(event_id for event_id in by_id if final_incoming.get(event_id, 0) == 0)
    required_chain_size = min(4, max(0, len(events) - 1))
    underfilled_event_ids = sorted(
        event_id
        for event_id in by_id
        if len(chains.get(str(event_id), [])) < required_chain_size
    )
    return {
        "policy": "pgvector_selective_reciprocity_v1",
        "event_count": len(by_id),
        "high_confidence_threshold": high_confidence_threshold,
        "exact_title_links_added": exact_links,
        "high_confidence_links_added": high_confidence_links,
        "zero_incoming_rescue_links_added": rescue_links,
        "zero_incoming_event_ids": zero_incoming_event_ids,
        "zero_incoming_rate": round(len(zero_incoming_event_ids) / max(1, len(by_id)), 6),
        "required_chain_size": required_chain_size,
        "underfilled_event_ids": underfilled_event_ids,
        "exact_title_pairs_missing": exact_missing,
    }


def validate_pgvector_graph_release(graph_meta: dict[str, Any]) -> None:
    """Fail a production candidate before publication on unhealthy topology."""

    failures: list[str] = []
    zero_rate = float(graph_meta.get("zero_incoming_rate") or 0.0)
    if zero_rate >= 0.05:
        failures.append(f"zero_incoming_rate={zero_rate:.4f} must be <0.05")
    exact_missing = graph_meta.get("exact_title_pairs_missing") or []
    if exact_missing:
        failures.append(f"exact_title_pairs_missing={exact_missing[:10]}")
    underfilled = graph_meta.get("underfilled_event_ids") or []
    if underfilled:
        failures.append(f"underfilled_event_ids={underfilled[:20]}")
    if failures:
        raise RuntimeError("pgvector related graph release gate failed: " + "; ".join(failures))


FACET_PATTERNS: dict[str, list[str]] = {
    "urbanism": [
        r"архитект", r"урбан", r"городск\w*\s+сред", r"общественн\w*\s+пространств",
        r"будущ\w*\s+город", r"концепци", r"моделир", r"микрорайон", r"планиров",
    ],
    "music": [r"концерт", r"музык", r"симфон", r"фортепиан", r"pianissimo", r"джаз", r"оркестр", r"филармон"],
    "cinema": [r"кино", r"фильм", r"съемк", r"съёмк", r"кинодекорац"],
    "local_history": [r"краевед", r"истори", r"прусс", r"советск", r"област", r"80\s+истор"],
    "art_exhibition": [r"выстав", r"галере", r"картина", r"худож", r"живопис", r"график", r"скульптур"],
    "kids_family": [r"дет", r"семейн", r"аниматор", r"школь", r"подрост"],
}


def event_text_blob(event: dict[str, Any]) -> str:
    return " ".join([
        str(event.get("title") or ""),
        str(event.get("event_type") or ""),
        category(event),
        " ".join(str(item) for item in event.get("topics") or []),
        str(event.get("summary") or ""),
        plain_from_html(event.get("description_html") or ""),
        str(event.get("venue_name") or ""),
        str(event.get("city") or ""),
    ]).lower()


def facet_set(event: dict[str, Any]) -> set[str]:
    text = event_text_blob(event)
    facets: set[str] = set()
    for name, patterns in FACET_PATTERNS.items():
        if any(re.search(pattern, text, flags=re.I | re.U) for pattern in patterns):
            facets.add(name)
    return facets


def build_sparse_related_chain(events: list[dict[str, Any]], *, cache_salt: str) -> dict[str, list[dict[str, Any]]]:
    sparse_index = build_sparse_tfidf_index(events)
    by_id = {int(event["id"]): event for event in events}
    facets_by_id = {int(event["id"]): facet_set(event) for event in events}
    chains: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        event_id = int(event["id"])
        scored: list[dict[str, Any]] = []
        for candidate in events:
            candidate_id = int(candidate["id"])
            if not eligible_related_pair(event, candidate):
                continue
            lexical_similarity = sparse_cosine(sparse_index.get(event_id, {}), sparse_index.get(candidate_id, {}))
            deterministic = score_pair(event, candidate)
            same_category = category(event) == category(candidate)
            anchor_facets = facets_by_id.get(event_id, set())
            candidate_facets = facets_by_id.get(candidate_id, set())
            shared_facets = sorted(anchor_facets & candidate_facets)
            facet_score = min(1.0, len(shared_facets) / 2) if shared_facets else 0.0
            mismatch_penalty = 0.10 if "music" in candidate_facets and "music" not in anchor_facets else 0.0
            strong_domain = bool(shared_facets) or (same_category and (lexical_similarity >= 0.025 or deterministic >= 0.50))
            slot_type = "pure_related" if strong_domain else "adjacent_discovery"
            related_score = (
                0.50 * lexical_similarity
                + 0.25 * deterministic
                + 0.25 * facet_score
                + (0.055 if same_category else 0.0)
                + (0.060 if shared_facets and not same_category else 0.0)
                - mismatch_penalty
                + stable_jitter(event_id, candidate_id, cache_salt)
            )
            reason_codes = [
                f"lexical:{SPARSE_RELATED_RETRIEVAL_METHOD}",
                f"category:{category(candidate)}",
            ]
            if same_category:
                reason_codes.append("same_category")
            for facet in shared_facets:
                reason_codes.append(f"facet:{facet}")
            if event.get("city") and event.get("city") == candidate.get("city"):
                reason_codes.append("same_city")
            if event.get("venue_name") and event.get("venue_name") == candidate.get("venue_name"):
                reason_codes.append("same_venue")
            scored.append({
                "event_id": candidate_id,
                "related_score": round(max(0.0, min(1.0, related_score)), 4),
                "lexical_similarity": round(max(0.0, min(1.0, lexical_similarity)), 4),
                "deterministic_score": round(deterministic, 4),
                "slot_type": slot_type,
                "similarity_class": "same_domain" if slot_type == "pure_related" else "adjacent_discovery",
                "confidence": round(max(0.15, min(0.95, 0.42 + lexical_similarity * 0.42 + (0.10 if same_category else 0.0) + 0.08 * facet_score)), 4),
                "reason_codes": reason_codes,
                "retrieval_sources": ["lexical_sparse", "deterministic"],
                "display_eligible": True,
            })
        scored.sort(key=lambda item: (-float(item["related_score"]), -float(item.get("lexical_similarity") or 0), int(item["event_id"])))
        chains[str(event_id)] = scored[:40]

    # Mutual relinking: if a new/changed event is a strong candidate for an old
    # anchor, make the reverse discoverable too. This keeps static pages coherent
    # between nightly rebuilds and Smart Update-triggered refreshes.
    for left_id, chain in list(chains.items()):
        for item in chain[:18]:
            right_id = str(item["event_id"])
            reverse = chains.setdefault(right_id, [])
            if any(str(existing["event_id"]) == left_id for existing in reverse):
                continue
            if left_id not in by_id or int(right_id) not in by_id:
                continue
            reverse.append({
                **item,
                "event_id": int(left_id),
                "related_score": round(max(0.0, float(item["related_score"]) * 0.965), 4),
                "reason_codes": list(dict.fromkeys([*(item.get("reason_codes") or []), "mutual_link"])),
                "retrieval_sources": list(dict.fromkeys([*(item.get("retrieval_sources") or []), "mutual_link"])),
            })
            reverse.sort(key=lambda entry: (-float(entry["related_score"]), -float(entry.get("lexical_similarity") or 0), int(entry["event_id"])))
            del reverse[40:]
    return chains


def personalization_supabase_request(
    function_name: str,
    payload: dict[str, Any],
    *,
    timeout: float = 30.0,
    response_max_bytes: int | None = None,
    total_response_max_bytes: int | None = None,
    metrics: dict[str, Any] | None = None,
    expected_row_fields: frozenset[str] | None = None,
) -> Any:
    base_url = (os.getenv("PERSONALIZATION_SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        os.getenv("PERSONALIZATION_SUPABASE_SECRET_KEY")
        or os.getenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY")
        or ""
    ).strip()
    if not base_url or not key:
        raise RuntimeError("PERSONALIZATION_SUPABASE_URL and PERSONALIZATION_SUPABASE_SECRET_KEY are required for pgvector related build")
    req = urllib.request.Request(
        f"{base_url}/rest/v1/rpc/{urllib.parse.quote(function_name, safe='')}",
        data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        method="POST",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    if metrics is not None:
        metrics["request_count"] = int(metrics.get("request_count") or 0) + 1
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            max_bytes = int(response_max_bytes or 0)
            if max_bytes <= 0:
                raw_bytes = response.read()
            else:
                declared_length = response.headers.get("Content-Length")
                if declared_length and declared_length.isdigit() and int(declared_length) > max_bytes:
                    raise RuntimeError(
                        f"Supabase RPC {function_name} response declares {declared_length} bytes; "
                        f"limit is {max_bytes}"
                    )
                raw_bytes = response.read(max_bytes + 1)
                if len(raw_bytes) > max_bytes:
                    raise RuntimeError(
                        f"Supabase RPC {function_name} response exceeds {max_bytes} bytes"
                    )
            if metrics is not None:
                metrics["response_bytes"] = int(metrics.get("response_bytes") or 0) + len(raw_bytes)
                metrics["max_single_response_bytes"] = max(
                    int(metrics.get("max_single_response_bytes") or 0), len(raw_bytes)
                )
                if (
                    total_response_max_bytes is not None
                    and int(total_response_max_bytes) > 0
                    and int(metrics["response_bytes"]) > int(total_response_max_bytes)
                ):
                    raise RuntimeError(
                        f"Supabase RPC {function_name} compact response total "
                        f"{metrics['response_bytes']} exceeds {int(total_response_max_bytes)} bytes"
                    )
            if not raw_bytes:
                parsed = None
            else:
                parsed = json.loads(raw_bytes.decode("utf-8", errors="replace"))
            if expected_row_fields is not None:
                if not isinstance(parsed, list):
                    raise RuntimeError(
                        f"Supabase RPC {function_name} must return a JSON row array"
                    )
                for index, row in enumerate(parsed):
                    if not isinstance(row, dict) or frozenset(row) != expected_row_fields:
                        actual = sorted(row) if isinstance(row, dict) else type(row).__name__
                        raise RuntimeError(
                            f"Supabase RPC {function_name} row {index} violates compact projection: "
                            f"expected={sorted(expected_row_fields)} actual={actual}"
                        )
            if metrics is not None and isinstance(parsed, list):
                metrics["row_count"] = int(metrics.get("row_count") or 0) + len(parsed)
            return parsed
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1500]
        raise RuntimeError(f"Supabase RPC {function_name} failed HTTP {exc.code}: {detail}") from exc


def build_pgvector_related_chain(
    events: list[dict[str, Any]],
    *,
    current_date: str,
    embedding_model: str = "gemini-embedding-2",
    match_count: int = 60,
    embedding_doc_kind: str = "related_v1",
    graph_meta_out: dict[str, Any] | None = None,
    retrieval_receipt_out: dict[str, Any] | None = None,
    response_max_bytes: int = DEFAULT_RELATED_RESPONSE_MAX_BYTES,
    total_response_max_bytes: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    by_id = {int(event["id"]): event for event in events}
    if len(by_id) != len(events):
        raise RuntimeError("pgvector related build requires unique anchor event ids")
    allowed_ids = set(by_id)
    facets_by_id = {int(event["id"]): facet_set(event) for event in events}
    chains: dict[str, list[dict[str, Any]]] = {}
    retrieval_metrics: dict[str, Any] = {
        "schema_version": "static_related_retrieval_receipt_v1",
        "rpc": COMPACT_RELATED_RPC,
        "projection": sorted(COMPACT_RELATED_FIELDS),
        "request_count": 0,
        "row_count": 0,
        "response_bytes": 0,
        "max_single_response_bytes": 0,
        "response_max_bytes": int(response_max_bytes),
        "total_response_max_bytes": (
            int(total_response_max_bytes) if total_response_max_bytes is not None else None
        ),
    }
    for event in events:
        event_id = int(event["id"])
        rows = personalization_supabase_request(
            COMPACT_RELATED_RPC,
            {
                "p_anchor_event_id": event_id,
                "p_embedding_model": embedding_model,
                "p_embedding_dim": 768,
                "p_match_count": match_count,
                "p_date_from": current_date,
                "p_date_to": None,
                "p_embedding_doc_kind": embedding_doc_kind,
            },
            timeout=45.0,
            response_max_bytes=response_max_bytes,
            total_response_max_bytes=total_response_max_bytes,
            metrics=retrieval_metrics,
            expected_row_fields=COMPACT_RELATED_FIELDS,
        ) or []
        scored: list[dict[str, Any]] = []
        excluded_ids = {event_id, *[int(item) for item in event.get("other_date_ids") or []]}
        anchor_facets = facets_by_id.get(event_id, set())
        for row in rows:
            try:
                candidate_id = int(row.get("event_id"))
            except Exception:
                continue
            candidate = by_id.get(candidate_id)
            if not candidate or candidate_id not in allowed_ids or candidate_id in excluded_ids:
                continue
            if int(event_id) in [int(item) for item in candidate.get("other_date_ids") or []]:
                continue
            if not eligible_related_pair(event, candidate):
                continue
            vector_similarity = max(0.0, min(1.0, float(row.get("vector_similarity") or 0)))
            deterministic = score_pair(event, candidate)
            same_category = category(event) == category(candidate)
            candidate_facets = facets_by_id.get(candidate_id, set())
            shared_facets = sorted(anchor_facets & candidate_facets)
            facet_score = min(1.0, len(shared_facets) / 2) if shared_facets else 0.0
            same_date = bool(event.get("start_date") and candidate.get("start_date") and event.get("start_date") == candidate.get("start_date"))
            strong_domain = vector_similarity >= 0.80 or bool(shared_facets) or (same_category and vector_similarity >= 0.76)
            slot_type = "pure_related" if strong_domain else "adjacent_discovery"
            # In pgvector mode semantic similarity is the retrieval contract.
            # Deterministic facets are only light tie-breakers; they must not
            # let a broad lexical/category overlap outrank a stronger semantic
            # neighbor (the 6447 urbanism/music regression).
            related_score = (
                0.90 * vector_similarity
                + 0.035 * facet_score
                + 0.025 * deterministic
                + (0.018 if same_category else 0.0)
                + (0.035 if "urbanism" in shared_facets else 0.0)
                + (0.015 if "music" in shared_facets else 0.0)
                + (0.006 if same_date else 0.0)
                + stable_jitter(event_id, candidate_id, f"{PGVECTOR_RELATED_ALGORITHM}:{embedding_model}")
            )
            reason_codes = [
                f"vector:{PGVECTOR_RELATED_RETRIEVAL_METHOD}",
                f"embedding:{embedding_model}",
                f"category:{category(candidate)}",
            ]
            if same_category:
                reason_codes.append("same_category")
            for facet in shared_facets:
                reason_codes.append(f"facet:{facet}")
            if event.get("city") and event.get("city") == candidate.get("city"):
                reason_codes.append("same_city")
            if event.get("venue_name") and event.get("venue_name") == candidate.get("venue_name"):
                reason_codes.append("same_venue")
            if same_date:
                reason_codes.append("same_date")
            scored.append({
                "event_id": candidate_id,
                "related_score": round(max(0.0, min(1.0, related_score)), 4),
                "vector_similarity": round(vector_similarity, 4),
                "deterministic_score": round(deterministic, 4),
                "slot_type": slot_type,
                "similarity_class": "same_domain" if slot_type == "pure_related" else "adjacent_discovery",
                "confidence": round(max(0.15, min(0.97, 0.35 + vector_similarity * 0.45 + (0.08 if same_category else 0.0) + 0.07 * facet_score)), 4),
                "reason_codes": reason_codes,
                "retrieval_sources": ["supabase_pgvector", "event_embedding"],
                "display_eligible": True,
            })
        scored.sort(key=lambda item: (-float(item["related_score"]), -float(item.get("vector_similarity") or 0), int(item["event_id"])))
        chains[str(event_id)] = scored[:40]
    graph_meta = apply_pgvector_graph_reciprocity(events, chains)
    if graph_meta_out is not None:
        graph_meta_out.clear()
        graph_meta_out.update(graph_meta)
    if retrieval_receipt_out is not None:
        retrieval_receipt_out.clear()
        retrieval_receipt_out.update(retrieval_metrics)
    chain_lengths = [len(value or []) for value in chains.values()]
    log_stage(
        "pgvector_rebuild_complete",
        event_count=len(events),
        min_chain=min(chain_lengths) if chain_lengths else 0,
        max_chain=max(chain_lengths) if chain_lengths else 0,
        avg_chain=round(sum(chain_lengths) / max(1, len(chain_lengths)), 2),
        embedding_model=embedding_model,
        embedding_doc_kind=embedding_doc_kind,
        graph_reciprocity=graph_meta,
    )
    return chains


def find_pgvector_sync_script() -> Path | None:
    candidates = [
        SCRIPT_PATH.parents[2] / "scripts" / "sync_event_search_vectors_to_supabase.py",
        SCRIPT_PATH.parent / "sync_event_search_vectors_to_supabase.py",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def sync_event_vectors_to_supabase(
    *,
    preview_events_json: Path,
    build_id: str,
    site_origin: str,
    ics_base_url: str,
    embedding_model: str,
    embedding_key_env: str,
    max_provider_calls: int,
) -> None:
    script = find_pgvector_sync_script()
    if not script:
        raise FileNotFoundError("sync_event_search_vectors_to_supabase.py not found in repo/site payload")
    sleep_seconds = os.getenv("STATIC_SITE_PGVECTOR_EMBEDDING_SLEEP_SECONDS", "3.0").strip() or "3.0"
    cmd = [
        sys.executable,
        str(script),
        "--apply",
        "--preview-events-json", str(preview_events_json),
        "--site-origin", site_origin,
        "--base-path", build_id,
        "--ics-base-url", ics_base_url,
        "--embedding-model", embedding_model,
        "--embedding-dim", "768",
        "--google-key-env", embedding_key_env,
        "--max-provider-calls", str(max(0, int(max_provider_calls))),
        "--sleep-seconds", sleep_seconds,
    ]
    log_stage(
        "pgvector_sync_start",
        preview_events_json=str(preview_events_json),
        embedding_model=embedding_model,
        embedding_key_env=embedding_key_env,
        max_provider_calls=max_provider_calls,
        sleep_seconds=sleep_seconds,
    )
    import subprocess

    subprocess.run(cmd, check=True)
    log_stage("pgvector_sync_complete", preview_events_json=str(preview_events_json), embedding_model=embedding_model)


def load_related_cache(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_related_cache(path: Path | None, payload: dict[str, Any], *, previous_event_count: int = 0) -> bool:
    if not path:
        return False
    new_event_count = len(payload.get("event_ids") or [])
    allow_shrink = str(os.getenv("STATIC_SITE_ALLOW_RELATED_CACHE_SHRINK", "")).strip().lower() in {"1", "true", "yes", "on"}
    if previous_event_count and new_event_count and new_event_count < previous_event_count and not allow_shrink:
        log_stage(
            "related_cache_write_skipped_shrink_guard",
            path=str(path),
            previous_event_count=previous_event_count,
            new_event_count=new_event_count,
        )
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(path, payload)
    return True


def supabase_limiter_client():
    from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client

    original_sys_path = list(sys.path)
    try:
        # The repository may contain a local `supabase/migrations/` directory,
        # which is a namespace package and can shadow the real supabase-py
        # package. Exclude such paths only for this import.
        sys.path = [
            item for item in sys.path
            if not ((Path(item or ".") / "supabase").is_dir() and not (Path(item or ".") / "supabase" / "__init__.py").exists())
        ]
        cached = sys.modules.get("supabase")
        if cached is not None and not getattr(cached, "__file__", None):
            sys.modules.pop("supabase", None)
        from supabase import create_client
    except Exception as exc:
        raise RuntimeError(f"supabase python client unavailable for limiter: {exc}") from exc
    finally:
        sys.path = original_sys_path

    def legacy_factory():
        url = (os.getenv("SUPABASE_URL") or "").strip()
        key = (
            os.getenv("SUPABASE_SERVICE_KEY")
            or os.getenv("SUPABASE_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_ANON_KEY")
            or ""
        ).strip()
        if not url or not key:
            raise RuntimeError("Supabase limiter env is missing: need SUPABASE_URL and SUPABASE_KEY/SUPABASE_SERVICE_KEY")
        return create_client(url, key)

    return build_google_ai_limiter_supabase_client(
        fallback_factory=legacy_factory,
        require_configured=True,
        client_factory=create_client,
    )


def _related_audit_text(value: Any, *, max_chars: int) -> str:
    text = clean_text(value or "") if not isinstance(value, str) else clean_text(value)
    text = re.sub(r"\s+", " ", text).strip()
    return html.escape(text[:max_chars], quote=False)


def _related_audit_event_block(event: dict[str, Any], *, tag: str, fact_max_chars: int) -> str:
    summary = _related_audit_text(event.get("summary") or event.get("description_html") or "", max_chars=fact_max_chars)
    title = _related_audit_text(event.get("title") or "", max_chars=180)
    event_type = _related_audit_text(event.get("event_type") or "", max_chars=80)
    event_category = _related_audit_text(category(event) or "", max_chars=80)
    venue = _related_audit_text(event.get("venue_name") or "", max_chars=120)
    city = _related_audit_text(event.get("city") or "", max_chars=80)
    date = _related_audit_text(event.get("start_date") or "", max_chars=40)
    event_id = int(event["id"])
    return (
        f'<{tag} id="{event_id}">\n'
        f"<title>{title}</title>\n"
        f"<type>{event_type}</type>\n"
        f"<category>{event_category}</category>\n"
        f"<summary>{summary}</summary>\n"
        f"<venue>{venue}</venue>\n"
        f"<city>{city}</city>\n"
        f"<date>{date}</date>\n"
        f"</{tag}>"
    )


def build_gemma_related_audit_prompt(
    *,
    anchor: dict[str, Any],
    candidates: list[dict[str, Any]],
    fact_max_chars: int,
) -> str:
    """Compact Gemma verifier prompt.

    The previous JSON-in/verbose-JSON-out contract produced many truncated JSON
    responses on Gemma 4 26B during full-site static related builds. Keep the
    semantic job LLM-first, but make the I/O small and tag-delimited so the
    provider spends tokens on judgement rather than boilerplate.
    """
    candidate_blocks = "\n".join(
        _related_audit_event_block(item, tag="candidate", fact_max_chars=fact_max_chars)
        for item in candidates
    )
    return (
        "Ты строгий verifier похожих событий для статической афиши Калининграда.\n"
        "Не добавляй event_id, которых нет в candidates. Верни только JSON по схеме.\n"
        "Задача: оценить, насколько каждый candidate тематически близок к anchor, и отсортировать ranked от наиболее похожего к менее похожему.\n"
        "Оценка: 0.90+ почти тот же интерес; 0.72+ можно показывать в блоке «Похожие»; 0.55-0.71 слабая/смежная рекомендация; <0.55 reject=true.\n"
        "Если событие просто другое, даже качественное, ставь reject=true.\n"
        "similarity_class выбери строго из: identical_or_near_identical, highly_similar, thematically_close, weak_or_adjacent_related, different_topic.\n"
        "confidence: 0..1, насколько достаточно фактов и насколько ты уверен в оценке; снижай при скудном описании или спорной похожести.\n"
        "Не объясняй решение, не возвращай лишние поля.\n\n"
        "<anchor>\n"
        f"{_related_audit_event_block(anchor, tag='event', fact_max_chars=fact_max_chars)}\n"
        "</anchor>\n\n"
        "<candidates>\n"
        f"{candidate_blocks}\n"
        "</candidates>"
    )


async def call_gemma_related_audit_async(
    *,
    model: str,
    key_env: str,
    anchor: dict[str, Any],
    candidates: list[dict[str, Any]],
    timeout_seconds: int = 45,
    candidate_limit: int = 18,
) -> dict[int, dict[str, Any]]:
    """Optional Gemma 4 audit/rerank for already-ranked sparse candidates.

    The page-view path never calls this. Export/build can enable it for changed
    anchors, and cache makes repeated rebuilds reuse the previous audit.

    Important: provider access goes only through GoogleAIClient with Supabase
    reserve/finalize. If limiter env/RPC is unavailable, this function fails
    before a provider call instead of falling back to a direct API request.
    """
    if not os.getenv(key_env, "").strip():
        raise RuntimeError(f"Gemma key env is missing: {key_env}")
    from google_ai.client import GoogleAIClient

    previous_env = {
        "GOOGLE_AI_ALLOW_RESERVE_FALLBACK": os.environ.get("GOOGLE_AI_ALLOW_RESERVE_FALLBACK"),
        "GOOGLE_AI_LOCAL_LIMITER_FALLBACK": os.environ.get("GOOGLE_AI_LOCAL_LIMITER_FALLBACK"),
        "GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR": os.environ.get("GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR"),
        "GOOGLE_AI_PROVIDER_TIMEOUT_SEC": os.environ.get("GOOGLE_AI_PROVIDER_TIMEOUT_SEC"),
        "GOOGLE_AI_FALLBACK_MODELS": os.environ.get("GOOGLE_AI_FALLBACK_MODELS"),
    }
    os.environ["GOOGLE_AI_ALLOW_RESERVE_FALLBACK"] = "0"
    os.environ["GOOGLE_AI_LOCAL_LIMITER_FALLBACK"] = "0"
    os.environ["GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR"] = "0"
    os.environ["GOOGLE_AI_PROVIDER_TIMEOUT_SEC"] = str(max(1, int(timeout_seconds)))
    os.environ["GOOGLE_AI_FALLBACK_MODELS"] = ""
    fact_max_chars = max(120, int(os.getenv("STATIC_SITE_GEMMA_RELATED_FACT_MAX_CHARS", "360") or "360"))
    candidate_limit = max(6, min(12, int(candidate_limit or 10)))
    candidates = candidates[:candidate_limit]
    prompt = build_gemma_related_audit_prompt(
        anchor=anchor,
        candidates=candidates,
        fact_max_chars=fact_max_chars,
    )
    schema = {
        "type": "OBJECT",
        "properties": {
            "ranked": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "event_id": {"type": "INTEGER"},
                        "llm_semantic_score": {"type": "NUMBER"},
                        "similarity_class": {"type": "STRING"},
                        "confidence": {"type": "NUMBER"},
                        "reject": {"type": "BOOLEAN"},
                    },
                    "required": ["event_id", "llm_semantic_score", "similarity_class", "confidence", "reject"],
                },
            }
        },
        "required": ["ranked"],
    }
    try:
        client = GoogleAIClient(
            supabase_client=supabase_limiter_client(),
            consumer="static_site_related_builder",
            account_name=os.getenv("STATIC_SITE_GEMMA_ACCOUNT_NAME") or "static-site-related",
            default_env_var_name=key_env,
        )
        max_output_tokens = max(384, int(os.getenv("STATIC_SITE_GEMMA_RELATED_MAX_OUTPUT_TOKENS", "768") or "768"))
        text, _usage = await client.generate_content_async(
            model=model,
            prompt=prompt,
            generation_config={
                "temperature": 0.1,
                "max_output_tokens": max_output_tokens,
                "response_mime_type": "application/json",
                "response_schema": schema,
                "thinking_config": {"include_thoughts": False, "thinking_level": "MINIMAL"},
            },
            max_output_tokens=max_output_tokens,
        )
    finally:
        for name, value in previous_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    if not str(text or "").strip():
        raise RuntimeError("Gemma related audit returned empty text after thought filtering")
    parsed = parse_gemma_json_object(str(text).strip())
    out: dict[int, dict[str, Any]] = {}
    for item in parsed.get("ranked") or []:
        try:
            event_id = int(item.get("event_id"))
        except Exception:
            continue
        out[event_id] = item
    return out


def call_gemma_related_audit(**kwargs: Any) -> dict[int, dict[str, Any]]:
    return asyncio.run(call_gemma_related_audit_async(**kwargs))


def parse_gemma_json_object(text: str) -> dict[str, Any]:
    """Parse one JSON object from Gemma structured output text.

    GoogleAIClient intentionally joins all non-thought text parts with newlines.
    Some Gemma 4 responses with native schema still arrive as duplicated JSON
    text parts; for this audit stage the first well-formed object is the useful
    contract and trailing duplicated text should not poison the whole anchor.
    """
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I).strip()
        raw = re.sub(r"\s*```$", "", raw).strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as first_exc:
        start = raw.find("{")
        if start < 0:
            raise
        try:
            parsed, _end = json.JSONDecoder().raw_decode(raw[start:])
        except json.JSONDecodeError:
            rescued = rescue_truncated_gemma_ranked(raw[start:])
            if rescued["ranked"]:
                parsed = rescued
            else:
                raise first_exc
    if not isinstance(parsed, dict):
        raise ValueError("Gemma audit JSON must be an object")
    return parsed


def rescue_truncated_gemma_ranked(raw: str) -> dict[str, Any]:
    """Best-effort syntax rescue for Gemma JSON cut after complete items.

    This is deliberately syntax-only: it never invents scores or ids and only
    salvages fully parseable candidate verdict objects already present in the
    response. A truncated tail therefore drops the worst unfinished candidates
    instead of wasting another full provider call.
    """
    ranked: list[dict[str, Any]] = []
    for match in re.finditer(r"\{[^{}]*\"event_id\"[^{}]*\"llm_semantic_score\"[^{}]*\"reject\"[^{}]*\}", raw, flags=re.S):
        chunk = match.group(0)
        try:
            item = json.loads(chunk)
        except Exception:
            continue
        if not isinstance(item, dict):
            continue
        try:
            event_id = int(item["event_id"])
            score = float(item["llm_semantic_score"])
            reject = bool(item["reject"])
        except Exception:
            continue
        rescued_item: dict[str, Any] = {
            "event_id": event_id,
            "llm_semantic_score": max(0.0, min(1.0, score)),
            "reject": reject,
        }
        if item.get("similarity_class"):
            rescued_item["similarity_class"] = str(item.get("similarity_class"))
        if item.get("confidence") is not None:
            try:
                rescued_item["confidence"] = max(0.0, min(1.0, float(item.get("confidence"))))
            except Exception:
                pass
        ranked.append(rescued_item)
    return {"ranked": ranked}


def classify_gemma_similarity(score: float, *, reject: bool) -> str:
    if reject or score < 0.55:
        return "different_topic"
    if score >= 0.90:
        return "identical_or_near_identical"
    if score >= 0.78:
        return "highly_similar"
    if score >= 0.72:
        return "thematically_close"
    return "weak_or_adjacent_related"


def normalize_gemma_similarity_class(value: Any, *, score: float, reject: bool) -> str:
    allowed = {
        "identical_or_near_identical",
        "highly_similar",
        "thematically_close",
        "weak_or_adjacent_related",
        "different_topic",
    }
    text = str(value or "").strip()
    return text if text in allowed else classify_gemma_similarity(score, reject=reject)


def gemma_related_policy_signature(*, model: str) -> str:
    """Stable cache signature for Gemma related rerank contract.

    A cache hit must represent the same prompt budget and candidate fan-in, not
    just the same model id. Otherwise a smoke run over one anchor can
    accidentally mark the whole related cache as verified.
    """
    payload = {
        "model": model,
        "candidate_limit": max(
            6,
            min(12, int(os.getenv("STATIC_SITE_GEMMA_RELATED_CANDIDATE_LIMIT", "10") or "10")),
        ),
        "pass_count": max(
            1,
            min(3, int(os.getenv("STATIC_SITE_GEMMA_RELATED_PASSES", "2") or "2")),
        ),
        "fact_max_chars": max(
            120,
            int(os.getenv("STATIC_SITE_GEMMA_RELATED_FACT_MAX_CHARS", "360") or "360"),
        ),
        "prompt_version": "related_gemma4_schema_v4_compact_xml_model_confidence_two_pass",
    }
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def maybe_apply_gemma_audit(
    events: list[dict[str, Any]],
    chains: dict[str, list[dict[str, Any]]],
    *,
    enabled: bool,
    model: str,
    key_env: str,
    cache: dict[str, Any],
    max_anchors: int,
    changed_event_ids: set[int] | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    changed_event_ids = set(changed_event_ids or set())
    meta = {
        "enabled": enabled,
        "model": model,
        "key_env": key_env,
        "status": "disabled",
        "audited_anchors": 0,
        "attempted_anchors": 0,
        "cache_hits": 0,
        "provider_calls": 0,
        "changed_event_ids": sorted(changed_event_ids),
        "skipped_unchanged_without_cache": 0,
        "errors": [],
        "verified_event_ids": [],
        "retry_policy": {
            "fallback_models": [],
            "max_attempts": max(
                1,
                int(os.getenv("STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS", "2") or "2"),
            ),
            "backoff_seconds": max(
                0.0,
                float(os.getenv("STATIC_SITE_GEMMA_RELATED_RETRY_BACKOFF_SEC", "10") or "10"),
            ),
            "timeout_seconds": max(
                5,
                int(os.getenv("STATIC_SITE_GEMMA_RELATED_TIMEOUT_SEC", "60") or "60"),
            ),
            "candidate_limit": max(
                6,
                min(
                    12,
                    int(os.getenv("STATIC_SITE_GEMMA_RELATED_CANDIDATE_LIMIT", "10") or "10"),
                ),
            ),
            "pass_count": max(
                1,
                min(3, int(os.getenv("STATIC_SITE_GEMMA_RELATED_PASSES", "2") or "2")),
            ),
        },
        "attempts": [],
    }
    if not enabled:
        return chains, meta
    if not os.getenv(key_env, "").strip():
        meta["status"] = "skipped_missing_key"
        return chains, meta
    by_id = {int(event["id"]): event for event in events}
    audit_cache = cache.setdefault("gemma_audit_cache", {})
    fingerprints = {str(event["id"]): event_fingerprint(event) for event in events}
    candidate_limit = int(meta["retry_policy"]["candidate_limit"])
    pass_count = int(meta["retry_policy"].get("pass_count") or 1)
    total_candidate_limit = candidate_limit * pass_count
    policy_signature = gemma_related_policy_signature(model=model)
    audited = 0
    attempted_anchors = 0
    for anchor in events:
        if max_anchors > 0 and attempted_anchors >= max_anchors:
            break
        anchor_id = str(anchor["id"])
        chain = chains.get(anchor_id) or []
        top_ids = [int(item["event_id"]) for item in chain[:total_candidate_limit]]
        if not top_ids:
            continue
        anchor_int_id = int(anchor["id"])
        affected_by_change = (
            not changed_event_ids
            or anchor_int_id in changed_event_ids
            or any(candidate_id in changed_event_ids for candidate_id in top_ids)
        )
        cache_key = hashlib.sha1(json.dumps({
            "model": model,
            "policy_signature": policy_signature,
            "anchor": fingerprints.get(anchor_id),
            "candidates": {str(cid): fingerprints.get(str(cid)) for cid in top_ids},
        }, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        cached = audit_cache.get(cache_key)
        if cached:
            meta["cache_hits"] += 1
            audit = {int(k): v for k, v in cached.items()}
            meta["verified_event_ids"].append(anchor_int_id)
        elif not affected_by_change:
            meta["skipped_unchanged_without_cache"] += 1
            continue
        else:
            attempted_anchors += 1
            try:
                candidate_events = [by_id[cid] for cid in top_ids if cid in by_id]
                max_attempts = int(meta["retry_policy"]["max_attempts"])
                backoff_seconds = float(meta["retry_policy"]["backoff_seconds"])
                audit: dict[int, dict[str, Any]] = {}
                pass_failures: list[dict[str, Any]] = []
                successful_passes = 0
                expected_passes = sum(
                    1
                    for pass_index in range(pass_count)
                    if candidate_events[pass_index * candidate_limit : (pass_index + 1) * candidate_limit]
                )
                for pass_index in range(pass_count):
                    pass_candidates = candidate_events[pass_index * candidate_limit : (pass_index + 1) * candidate_limit]
                    if not pass_candidates:
                        continue
                    last_exc: Exception | None = None
                    for attempt in range(1, max_attempts + 1):
                        started = time.monotonic()
                        try:
                            log_stage(
                                "gemma_audit_call",
                                anchor_event_id=anchor_int_id,
                                candidate_count=len(pass_candidates),
                                candidate_limit=candidate_limit,
                                total_candidate_limit=total_candidate_limit,
                                pass_index=pass_index + 1,
                                pass_count=pass_count,
                                model=model,
                                key_env=key_env,
                                attempt=attempt,
                                max_attempts=max_attempts,
                            )
                            pass_audit = call_gemma_related_audit(
                                model=model,
                                key_env=key_env,
                                anchor=anchor,
                                candidates=pass_candidates,
                                timeout_seconds=int(meta["retry_policy"]["timeout_seconds"]),
                                candidate_limit=candidate_limit,
                            )
                            elapsed_ms = int((time.monotonic() - started) * 1000)
                            audit.update(pass_audit)
                            successful_passes += 1
                            meta["provider_calls"] += 1
                            meta["attempts"].append({
                                "anchor_event_id": anchor_int_id,
                                "attempt": attempt,
                                "pass_index": pass_index + 1,
                                "pass_count": pass_count,
                                "model": model,
                                "status": "ok",
                                "elapsed_ms": elapsed_ms,
                                "candidate_count": len(pass_candidates),
                                "candidate_limit": candidate_limit,
                                "total_candidate_limit": total_candidate_limit,
                            })
                            log_stage(
                                "gemma_audit_call_complete",
                                anchor_event_id=anchor_int_id,
                                model=model,
                                attempt=attempt,
                                pass_index=pass_index + 1,
                                elapsed_ms=elapsed_ms,
                            )
                            break
                        except Exception as exc:
                            elapsed_ms = int((time.monotonic() - started) * 1000)
                            meta["attempts"].append({
                                "anchor_event_id": anchor_int_id,
                                "attempt": attempt,
                                "pass_index": pass_index + 1,
                                "pass_count": pass_count,
                                "model": model,
                                "status": "error",
                                "elapsed_ms": elapsed_ms,
                                "error": str(exc)[:200],
                            })
                            last_exc = exc
                            if attempt >= max_attempts:
                                pass_failures.append({
                                    "pass_index": pass_index + 1,
                                    "error": str(exc)[:500],
                                })
                                break
                            sleep_seconds = backoff_seconds * attempt
                            log_stage(
                                "gemma_audit_retry",
                                anchor_event_id=anchor_int_id,
                                model=model,
                                attempt=attempt,
                                next_attempt=attempt + 1,
                                pass_index=pass_index + 1,
                                sleep_seconds=sleep_seconds,
                                error=str(exc)[:500],
                            )
                            time.sleep(sleep_seconds)
                    else:
                        pass_failures.append({
                            "pass_index": pass_index + 1,
                            "error": str(last_exc)[:500] if last_exc else "Gemma related audit failed",
                        })
                if not audit:
                    raise RuntimeError(
                        pass_failures[0]["error"] if pass_failures else "Gemma related audit failed"
                    )
                if pass_failures:
                    meta["errors"].append({
                        "anchor_event_id": anchor_int_id,
                        "error": "partial_gemma_pass_failure",
                        "pass_failures": pass_failures,
                    })
                if successful_passes == expected_passes:
                    audit_cache[cache_key] = {str(k): v for k, v in audit.items()}
                meta["verified_event_ids"].append(anchor_int_id)
                # Respect free-tier RPM without introducing a queue.
                time.sleep(4.2)
            except Exception as exc:
                meta["errors"].append({"anchor_event_id": anchor_int_id, "error": str(exc)[:500]})
                log_stage("gemma_audit_error", anchor_event_id=anchor_int_id, error=str(exc)[:500])
                continue
        for item in chain:
            event_id = int(item["event_id"])
            verdict = audit.get(event_id)
            if not verdict:
                continue
            llm_score = max(0.0, min(1.0, float(verdict.get("llm_semantic_score") or 0)))
            item["llm_semantic_score"] = round(llm_score, 4)
            item["llm_confidence"] = round(max(0.0, min(1.0, float(verdict.get("confidence") or 0.0))), 4)
            item["similarity_class"] = normalize_gemma_similarity_class(
                verdict.get("similarity_class"),
                score=llm_score,
                reject=bool(verdict.get("reject")),
            )
            item["gemma_reject"] = bool(verdict.get("reject"))
            if item["gemma_reject"]:
                item["display_eligible"] = False
            item["reason_codes"] = list(dict.fromkeys([*(item.get("reason_codes") or []), "gemma4_26b_audit"]))
            item["slot_type"] = "pure_related" if (llm_score >= 0.72 and not item.get("gemma_reject")) else str(item.get("slot_type") or "adjacent_discovery")
            if llm_score < 0.35:
                item["slot_type"] = "adjacent_discovery"
                item["similarity_class"] = "adjacent_discovery"
            if llm_score < 0.55:
                item["display_eligible"] = False
            # After the LLM verifier has seen the pair, its semantic score is
            # the primary ordering signal. Vector similarity remains a small
            # stability/tie-break signal, while deterministic facets must not
            # overpower the LLM verdict (for example 6447 urbanism candidates).
            item["related_score"] = round(
                max(
                    0.0,
                    min(
                        1.0,
                        0.82 * llm_score
                        + 0.12 * float(item.get("vector_similarity") or item.get("lexical_similarity") or 0)
                        + 0.06 * float(item.get("llm_confidence") or 0),
                    ),
                ),
                4,
            )
        chain[:] = [item for item in chain if item.get("display_eligible", True)]
        chain.sort(key=lambda entry: (-float(entry["related_score"]), -float(entry.get("lexical_similarity") or entry.get("vector_similarity") or 0), int(entry["event_id"])))
        del chain[40:]
        audited += 1
    meta["audited_anchors"] = audited
    meta["verified_event_ids"] = sorted(set(int(item) for item in meta["verified_event_ids"]))
    meta["attempted_anchors"] = attempted_anchors
    meta["status"] = "ok" if audited > 0 and not meta["errors"] else ("partial" if audited > 0 else "failed")
    return chains, meta


def build_bge_related_chain(
    events: list[dict[str, Any]],
    shared_artifact: dict[str, Any],
    *,
    top_k: int = 40,
) -> dict[str, list[dict[str, Any]]]:
    """Build related chains from the exact in-memory matrix used by unusual.

    The semantic module returns vectors keyed by event id.  Converting that one
    object to a dense matrix does not invoke the encoder and deliberately
    happens before either consumer serializes a cache.
    """

    import numpy as np

    vector_rows = shared_artifact.get("event_vectors")
    if not isinstance(vector_rows, dict):
        raise RuntimeError("shared BGE artifact event_vectors missing")
    event_by_id = {int(event["id"]): event for event in events}
    event_ids = sorted(event_by_id)
    missing = [event_id for event_id in event_ids if str(event_id) not in vector_rows]
    if missing:
        raise RuntimeError(f"shared BGE artifact partial event matrix: missing={missing[:8]}")
    matrix = np.asarray(
        [vector_rows[str(event_id)]["vector"] for event_id in event_ids],
        dtype=np.float32,
    )
    metadata = shared_artifact.get("metadata") or {}
    expected_dim = int(metadata.get("embedding_dim") or 0)
    if (
        matrix.ndim != 2
        or matrix.shape[0] != len(event_ids)
        or matrix.shape[1] != expected_dim
        or not np.isfinite(matrix).all()
    ):
        raise RuntimeError("shared BGE artifact matrix dimension/value mismatch")
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if np.any(norms <= 0):
        raise RuntimeError("shared BGE artifact contains zero vectors")
    matrix = matrix / norms
    chains: dict[str, list[dict[str, Any]]] = {}
    batch = 256
    for offset in range(0, len(event_ids), batch):
        similarities = matrix[offset : offset + batch] @ matrix.T
        for local_index, event_id in enumerate(event_ids[offset : offset + batch]):
            left = event_by_id[event_id]
            scores = similarities[local_index]
            candidate_indexes = np.argpartition(
                -scores, min(len(scores) - 1, top_k * 3)
            )[: min(len(scores), top_k * 3 + 1)]
            ranked = sorted(
                (
                    (float(scores[index]), event_ids[int(index)])
                    for index in candidate_indexes
                    if event_ids[int(index)] != event_id
                ),
                key=lambda item: (-item[0], item[1]),
            )
            chain: list[dict[str, Any]] = []
            for similarity, candidate_id in ranked:
                right = event_by_id[candidate_id]
                if not eligible_related_pair(left, right):
                    continue
                same_category = bool(category(left) and category(left) == category(right))
                shared_facets = sorted(
                    set(str(value) for value in (left.get("topics") or []))
                    .intersection(str(value) for value in (right.get("topics") or []))
                )
                strong = similarity >= 0.72 or same_category or bool(shared_facets)
                chain.append(
                    {
                        "event_id": candidate_id,
                        "slot_type": "pure_related" if strong else "adjacent_discovery",
                        "related_score": round(max(0.0, min(1.0, similarity)), 4),
                        "vector_similarity": round(similarity, 4),
                        "similarity_class": "same_domain" if strong else "adjacent_discovery",
                        "reasons": [
                            f"vector:{BGE_RELATED_RETRIEVAL_METHOD}",
                            *(["same_category"] if same_category else []),
                            *(f"facet:{value}" for value in shared_facets[:3]),
                        ],
                        "retrieval_sources": ["shared_static_event_bge"],
                    }
                )
                if len(chain) >= top_k:
                    break
            chains[str(event_id)] = chain
    return chains


def build_related(
    events: list[dict[str, Any]],
    *,
    current_date: str,
    related_mode: str = "sparse",
    cache_path: Path | None = None,
    gemma_verify: bool = False,
    gemma_model: str = "models/gemma-4-26b-a4b-it",
    gemma_key_env: str = "GOOGLE_API_KEY4",
    gemma_max_anchors: int = 0,
    embedding_model: str = "gemini-embedding-2",
    related_corpus_revision: str = "",
    shared_bge_artifact: dict[str, Any] | None = None,
    related_response_max_bytes: int = DEFAULT_RELATED_RESPONSE_MAX_BYTES,
    related_total_response_max_bytes: int | None = None,
) -> dict[str, Any]:
    requested_mode = str(related_mode or "").strip().lower()
    related_mode = requested_mode if requested_mode in {"sparse", "pgvector", "bge"} else "sparse"
    if related_mode == "pgvector":
        algorithm = PGVECTOR_RELATED_ALGORITHM
        schema_version = PGVECTOR_RELATED_SCHEMA_VERSION
        retrieval_method = PGVECTOR_RELATED_RETRIEVAL_METHOD
        cache_schema_version = PGVECTOR_RELATED_CACHE_SCHEMA_VERSION
        embedding_doc_version = os.getenv("STATIC_SITE_PGVECTOR_RELATED_DOC_KIND", "related_v1")
    elif related_mode == "bge":
        algorithm = BGE_RELATED_ALGORITHM
        schema_version = BGE_RELATED_SCHEMA_VERSION
        retrieval_method = BGE_RELATED_RETRIEVAL_METHOD
        cache_schema_version = BGE_RELATED_CACHE_SCHEMA_VERSION
        embedding_doc_version = str(
            ((shared_bge_artifact or {}).get("metadata") or {}).get("document_version")
            or "related_v1"
        )
        embedding_model = str(
            ((shared_bge_artifact or {}).get("metadata") or {}).get("model_id")
            or embedding_model
        )
    else:
        algorithm = SPARSE_RELATED_ALGORITHM
        schema_version = SPARSE_RELATED_SCHEMA_VERSION
        retrieval_method = SPARSE_RELATED_RETRIEVAL_METHOD
        cache_schema_version = RELATED_CACHE_SCHEMA_VERSION
        embedding_doc_version = "event_embedding_doc_v1"
    related_corpus_revision = str(related_corpus_revision or "").strip()
    generated_at = datetime.now(timezone.utc).isoformat()
    fingerprints = {str(event["id"]): event_fingerprint(event) for event in events}
    event_ids = [int(event["id"]) for event in events]
    cache = load_related_cache(cache_path) or {}
    graph_meta: dict[str, Any] = {
        "policy": "not_applicable" if related_mode != "pgvector" else "pgvector_selective_reciprocity_v1",
    }
    retrieval_receipt: dict[str, Any] = {
        "schema_version": "static_related_retrieval_receipt_v1",
        "rpc": COMPACT_RELATED_RPC if related_mode == "pgvector" else None,
        "projection": sorted(COMPACT_RELATED_FIELDS) if related_mode == "pgvector" else [],
        "request_count": 0,
        "row_count": 0,
        "response_bytes": 0,
        "max_single_response_bytes": 0,
        "response_max_bytes": int(related_response_max_bytes),
        "total_response_max_bytes": (
            int(related_total_response_max_bytes)
            if related_total_response_max_bytes is not None
            else None
        ),
        "source": "not_applicable" if related_mode != "pgvector" else "cache",
    }
    previous_fingerprints = cache.get("event_fingerprints") if isinstance(cache.get("event_fingerprints"), dict) else {}
    previous_ids = [int(value) for value in (cache.get("event_ids") or []) if str(value).isdigit()]
    previous_id_set = set(previous_ids)
    current_id_set = set(event_ids)
    changed_event_ids = {
        int(event_id)
        for event_id, fingerprint in fingerprints.items()
        if previous_fingerprints.get(event_id) != fingerprint
    }
    changed_event_ids.update(previous_id_set - current_id_set)
    cache_valid = (
        cache.get("schema_version") == cache_schema_version
        and cache.get("algorithm") == algorithm
        and (related_mode == "sparse" or cache.get("embedding_model") == embedding_model)
        and (related_mode == "sparse" or cache.get("embedding_document_version") == embedding_doc_version)
        and (related_mode != "pgvector" or cache.get("related_corpus_revision") == related_corpus_revision)
        and (
            related_mode != "bge"
            or cache.get("shared_bge_artifact_sha256")
            == str(((shared_bge_artifact or {}).get("metadata") or {}).get("artifact_sha256") or "")
        )
        and cache.get("event_fingerprints") == fingerprints
        and cache.get("event_ids") == event_ids
        and isinstance(cache.get("chains"), dict)
    )
    gemma_policy_signature = gemma_related_policy_signature(model=gemma_model) if gemma_verify else None
    cached_verified_event_ids = {
        int(value)
        for value in (cache.get("gemma_verified_event_ids") or [])
        if str(value).isdigit()
    }
    gemma_cache_ready = bool(
        gemma_verify
        and cache.get("gemma_verified_model") == gemma_model
        and cache.get("gemma_policy_signature") == gemma_policy_signature
        and current_id_set.issubset(cached_verified_event_ids)
    )
    log_stage(
        "cache_check",
        cache_path=str(cache_path) if cache_path else None,
        cache_valid=cache_valid,
        event_count=len(events),
        previous_event_count=len(previous_ids),
        changed_event_ids=sorted(changed_event_ids),
        gemma_verify=gemma_verify,
        gemma_model=gemma_model,
        related_mode=related_mode,
        algorithm=algorithm,
    )
    raw_chains_for_cache: dict[str, list[dict[str, Any]]] | None = None
    cached_raw_chains = cache.get("raw_chains") if isinstance(cache.get("raw_chains"), dict) else None
    if cache_valid and (not gemma_verify or gemma_cache_ready):
        chains = cache["chains"]
        raw_chains_for_cache = copy.deepcopy(cached_raw_chains or chains)
        cache_state = "hit"
        gemma_meta = {
            "enabled": gemma_verify,
            "model": gemma_model,
            "key_env": gemma_key_env,
            "status": "cache_hit_no_provider" if gemma_verify else "disabled",
            "audited_anchors": 0,
            "cache_hits": len(events) if gemma_verify else 0,
            "provider_calls": 0,
            "changed_event_ids": [],
            "skipped_unchanged_without_cache": 0,
            "verified_event_ids": sorted(current_id_set) if gemma_verify else [],
            "errors": [],
        }
        graph_meta = copy.deepcopy(cache.get("graph_reciprocity") or graph_meta)
    elif cache_valid and cached_raw_chains:
        # Previous runs may contain partially Gemma-filtered display chains.
        # Always apply the verifier to the unfiltered pgvector chains so a
        # smoke/partial run cannot permanently hide candidate events.
        chains = copy.deepcopy(cached_raw_chains)
        raw_chains_for_cache = copy.deepcopy(cached_raw_chains)
        graph_meta = copy.deepcopy(cache.get("graph_reciprocity") or graph_meta)
        cache_state = "hit"
        log_stage("gemma_initial_audit_required", event_count=len(events), model=gemma_model)
        chains, gemma_meta = maybe_apply_gemma_audit(
            events,
            chains,
            enabled=gemma_verify,
            model=gemma_model,
            key_env=gemma_key_env,
            cache=cache,
            max_anchors=gemma_max_anchors,
            changed_event_ids=set(),
        )
    else:
        salt = hashlib.sha1(json.dumps({"ids": event_ids, "fp": fingerprints}, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:12]
        log_stage(f"{related_mode}_rebuild_start", event_count=len(events), cache_salt=salt, changed_event_count=len(changed_event_ids), embedding_model=embedding_model)
        if related_mode == "pgvector":
            graph_meta = {}
            chains = build_pgvector_related_chain(
                events,
                current_date=current_date,
                embedding_model=embedding_model,
                embedding_doc_kind=os.getenv("STATIC_SITE_PGVECTOR_RELATED_DOC_KIND", "related_v1") or "related_v1",
                graph_meta_out=graph_meta,
                retrieval_receipt_out=retrieval_receipt,
                response_max_bytes=related_response_max_bytes,
                total_response_max_bytes=related_total_response_max_bytes,
            )
            retrieval_receipt["source"] = "compact_rpc"
        elif related_mode == "bge":
            if not shared_bge_artifact:
                raise RuntimeError("BGE related mode requires the shared in-memory vector artifact")
            graph_meta = {
                "policy": "shared_bge_dense_graph_v1",
                "artifact_sha256": str(
                    (shared_bge_artifact.get("metadata") or {}).get("artifact_sha256") or ""
                ),
            }
            chains = build_bge_related_chain(events, shared_bge_artifact)
        else:
            chains = build_sparse_related_chain(events, cache_salt=salt)
        raw_chains_for_cache = copy.deepcopy(chains)
        cache_state = "miss_rebuilt"
        chain_lengths = [len(value or []) for value in chains.values()]
        log_stage(
            f"{related_mode}_rebuild_complete",
            event_count=len(events),
            min_chain=min(chain_lengths) if chain_lengths else 0,
            max_chain=max(chain_lengths) if chain_lengths else 0,
            avg_chain=round(sum(chain_lengths) / max(1, len(chain_lengths)), 2),
        )
        chains, gemma_meta = maybe_apply_gemma_audit(
            events,
            chains,
            enabled=gemma_verify,
            model=gemma_model,
            key_env=gemma_key_env,
            cache=cache,
            max_anchors=gemma_max_anchors,
            changed_event_ids=changed_event_ids,
        )
    log_stage("gemma_audit_complete", **gemma_meta)
    cache_payload = {
        "schema_version": cache_schema_version,
        "algorithm": algorithm,
        "retrieval_method": retrieval_method,
        "embedding_document_version": embedding_doc_version,
        "embedding_model": embedding_model if related_mode in {"pgvector", "bge"} else None,
        "related_corpus_revision": related_corpus_revision if related_mode == "pgvector" else None,
        "semantic_embeddings": related_mode in {"pgvector", "bge"},
        "shared_bge_artifact_sha256": (
            str(((shared_bge_artifact or {}).get("metadata") or {}).get("artifact_sha256") or "")
            if related_mode == "bge"
            else None
        ),
        "event_ids": event_ids,
        "event_fingerprints": fingerprints,
        "chains": chains,
        "raw_chains": raw_chains_for_cache or copy.deepcopy(chains),
        "graph_reciprocity": graph_meta,
        "retrieval_receipt": retrieval_receipt,
        "gemma_audit_cache": cache.get("gemma_audit_cache", {}),
        "gemma_verification": gemma_meta,
        "gemma_verified_model": (
            gemma_model
            if gemma_verify and current_id_set.issubset(set(int(value) for value in (gemma_meta.get("verified_event_ids") or [])))
            else cache.get("gemma_verified_model")
        ),
        "gemma_policy_signature": (
            gemma_policy_signature
            if gemma_verify and current_id_set.issubset(set(int(value) for value in (gemma_meta.get("verified_event_ids") or [])))
            else cache.get("gemma_policy_signature")
        ),
        "gemma_verified_event_ids": sorted(set(int(value) for value in (gemma_meta.get("verified_event_ids") or [])))
        if gemma_verify
        else [],
        "updated_at": generated_at,
    }
    cache_written = save_related_cache(cache_path, cache_payload, previous_event_count=len(previous_ids))
    related: dict[str, dict[str, Any]] = {}
    for event in events:
        chain = chains.get(str(event["id"]), [])
        if gemma_verify:
            # In strict Gemma mode the public "similar" list must contain only
            # candidates actually scored by the LLM as strong semantic matches.
            # Raw pgvector/deterministic candidates remain in `chain` for audit,
            # but are not rendered as "Похожие" on static event pages.
            pure = [
                int(item["event_id"])
                for item in chain
                if not item.get("gemma_reject")
                and item.get("llm_semantic_score") is not None
                and float(item.get("llm_semantic_score") or 0) >= 0.72
            ][:30]
            similar = pure
            explore = [
                int(item["event_id"])
                for item in chain
                if not item.get("gemma_reject")
                and item.get("llm_semantic_score") is not None
                and 0.55 <= float(item.get("llm_semantic_score") or 0) < 0.72
            ][:10]
        else:
            pure = [int(item["event_id"]) for item in chain if item.get("slot_type") == "pure_related"][:30]
            similar = pure or [int(item["event_id"]) for item in chain[:30]]
            explore = [int(item["event_id"]) for item in chain if item.get("slot_type") == "adjacent_discovery" or item.get("similarity_class") == "adjacent_discovery"][:10]
        if not gemma_verify and not explore:
            explore = [int(item["event_id"]) for item in chain[10:20]]
        related[str(event["id"])] = {
            "similar": similar[:30],
            "pure_related": pure[:30],
            "explore": explore[:10],
            "adjacent_discovery": explore[:10],
            "chain": chain[:40],
            "underfilled": len(chain) < min(20, max(0, len(events) - 1)),
            "strict_verified": bool(gemma_verify),
        }
    return {
        "schema_version": schema_version,
        "generated_at": generated_at,
        "algorithm": algorithm,
        "fallback_algorithm": "prod_sqlite_static_related_v1",
        "retrieval_method": retrieval_method,
        "semantic_embeddings": related_mode in {"pgvector", "bge"},
        "embedding_model": embedding_model if related_mode in {"pgvector", "bge"} else None,
        "embedding_document_version": embedding_doc_version,
        "related_corpus_revision": related_corpus_revision if related_mode == "pgvector" else None,
        "shared_bge": (
            {
                key: value
                for key, value in ((shared_bge_artifact or {}).get("metadata") or {}).items()
                if key
                in {
                    "encoder_contract",
                    "model_id",
                    "model_revision",
                    "embedding_dim",
                    "document_version",
                    "artifact_sha256",
                    "provider_calls",
                }
            }
            if related_mode == "bge"
            else None
        ),
        "graph_reciprocity": graph_meta,
        "retrieval_receipt": retrieval_receipt,
        "gemma_verification": gemma_meta,
        "strict_verified_related": bool(gemma_verify),
        "cache": {
            "state": cache_state,
            "path": str(cache_path) if cache_path else None,
            "written": cache_written,
            "previous_event_count": len(previous_ids),
        },
        "related": related,
    }


def normalize_linked_occurrences(events: list[dict[str, Any]]) -> None:
    """Keep only mutual links inside the exported eligible catalog.

    Canonical rows may retain links to past/ineligible occurrences.  Those
    source links remain in SQLite, but a static release must not emit dangling
    or one-way graph edges that point outside its immutable catalog.
    """

    by_id = {int(event["id"]): event for event in events}
    raw_links = {
        event_id: {
            int(value)
            for value in (event.get("other_date_ids") or [])
            if str(value).isdigit() and int(value) != event_id
        }
        for event_id, event in by_id.items()
    }
    for event_id, event in by_id.items():
        event["other_date_ids"] = sorted(
            linked_id
            for linked_id in raw_links[event_id]
            if linked_id in by_id and event_id in raw_links.get(linked_id, set())
        )


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _compact_unusual_metrics_for_log(metrics: Any) -> dict[str, Any]:
    """Keep release metrics observable without logging per-event evidence."""

    if not isinstance(metrics, dict):
        return {}
    compact = dict(metrics)
    ordinary = compact.get("ordinary_corpus_receipt")
    if isinstance(ordinary, dict):
        compact["ordinary_corpus_receipt"] = {
            key: value for key, value in ordinary.items() if key != "members"
        }
    gate = compact.get("quality_gate")
    if isinstance(gate, dict):
        compact_gate = dict(gate)
        observed = compact_gate.get("observed")
        if isinstance(observed, dict):
            compact_observed = {
                key: value for key, value in observed.items() if key != "predictions"
            }
            observed_ordinary = compact_observed.get("ordinary_corpus_receipt")
            if isinstance(observed_ordinary, dict):
                compact_observed["ordinary_corpus_receipt"] = {
                    key: value
                    for key, value in observed_ordinary.items()
                    if key != "members"
                }
            compact_gate["observed"] = compact_observed
        compact["quality_gate"] = compact_gate
    return compact


def _load_json_object(path: Path | None) -> dict[str, Any] | None:
    if not path or not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log_stage("unusual_feed_disabled", reason="cache_json_invalid", path=str(path), error=str(exc)[:240])
        return None
    return value if isinstance(value, dict) else None


def _load_cached_bge_artifact(
    *,
    npz_path: Path,
    receipt_path: Path,
    prototype_bank: dict[str, Any],
    classifier: dict[str, Any],
    events: list[dict[str, Any]],
    bge_module: Any,
    model_revision: str,
) -> dict[str, Any] | None:
    if not npz_path.is_file() or not receipt_path.is_file():
        return None
    try:
        import numpy as np

        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        documents = bge_module.build_related_v1_documents(events)
        current_keys = {
            str(row["event_id"]): str(row["text_hash"]) for row in documents
        }
        stored_keys = receipt.get("event_text_hashes")
        if not isinstance(stored_keys, dict):
            return None
        if (
            receipt.get("schema_version") != "static-event-bge-cache-receipt-v1"
            or receipt.get("model_revision") != model_revision
            or receipt.get("model_id") != bge_module.MODEL_ID
            or int(receipt.get("embedding_dim") or 0) != int(bge_module.EMBEDDING_DIM)
            or receipt.get("document_version") != bge_module.DOCUMENT_VERSION
            or receipt.get("encoder_contract") != bge_module.ENCODER_CONTRACT
            or receipt.get("prototype_bank_sha256") != bge_module.stable_hash(prototype_bank)
        ):
            return None
        with np.load(npz_path, allow_pickle=False) as stored:
            event_ids = [str(value) for value in stored["event_ids"].tolist()]
            prototype_ids = [str(value) for value in stored["prototype_ids"].tolist()]
            event_matrix = stored["event_vectors"]
            prototype_matrix = stored["prototype_vectors"]
        if event_matrix.shape != (len(event_ids), int(bge_module.EMBEDDING_DIM)):
            return None
        if prototype_matrix.shape != (len(prototype_ids), int(bge_module.EMBEDDING_DIM)):
            return None
        artifact = {
            "schema_version": "static-event-bge-v1",
            "metadata": dict(receipt.get("metadata") or {}),
            "event_vectors": {
                event_id: {
                    "text_hash": str(stored_keys[event_id]),
                    "vector": [float(value) for value in event_matrix[index].tolist()],
                }
                for index, event_id in enumerate(event_ids)
                if event_id in stored_keys
            },
            "prototype_vectors": {
                prototype_id: {
                    "text_hash": str((receipt.get("prototype_text_hashes") or {})[prototype_id]),
                    "vector": [float(value) for value in prototype_matrix[index].tolist()],
                }
                for index, prototype_id in enumerate(prototype_ids)
            },
        }
        validation = bge_module.validate_shared_bge_vector_artifact(
            artifact,
            prototype_bank=prototype_bank,
            # The embedding cache is bound to the encoder/document/prototype
            # contract. A classifier-only calibration change must reuse the
            # same vectors; build_shared_bge_vector_artifact will bind the
            # rebuilt receipt to the new head hash.
            expected_classifier_sha256=None,
        )
        if not validation.get("valid"):
            return None
        artifact["_cache_current_text_hashes"] = current_keys
        return artifact
    except Exception as exc:
        log_stage("unusual_feed_disabled", reason="bge_cache_invalid", error=str(exc)[:300])
        return None


def _write_bge_cache(
    *,
    artifact: dict[str, Any],
    npz_path: Path,
    receipt_path: Path,
) -> None:
    import numpy as np

    event_vectors = artifact["event_vectors"]
    prototype_vectors = artifact["prototype_vectors"]
    event_ids = sorted(event_vectors, key=int)
    prototype_ids = sorted(prototype_vectors)
    npz_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = npz_path.with_name(f".{npz_path.name}.{os.getpid()}.tmp")
    with temporary.open("wb") as handle:
        np.savez_compressed(
            handle,
            event_ids=np.asarray(event_ids, dtype=f"<U{max(1, max(map(len, event_ids), default=1))}"),
            event_vectors=np.asarray(
                [event_vectors[event_id]["vector"] for event_id in event_ids], dtype=np.float64
            ),
            prototype_ids=np.asarray(
                prototype_ids,
                dtype=f"<U{max(1, max(map(len, prototype_ids), default=1))}",
            ),
            prototype_vectors=np.asarray(
                [prototype_vectors[prototype_id]["vector"] for prototype_id in prototype_ids],
                dtype=np.float64,
            ),
        )
    os.replace(temporary, npz_path)
    metadata = dict(artifact.get("metadata") or {})
    receipt = {
        "schema_version": "static-event-bge-cache-receipt-v1",
        "model_id": metadata.get("model_id"),
        "model_revision": metadata.get("model_revision"),
        "embedding_dim": metadata.get("embedding_dim"),
        "document_version": metadata.get("document_version"),
        "encoder_contract": metadata.get("encoder_contract"),
        "prototype_bank_sha256": metadata.get("prototype_bank_sha256"),
        "classifier_sha256": metadata.get("classifier_sha256"),
        "artifact_sha256": metadata.get("artifact_sha256"),
        "event_text_hashes": {
            event_id: event_vectors[event_id]["text_hash"] for event_id in event_ids
        },
        "prototype_text_hashes": {
            prototype_id: prototype_vectors[prototype_id]["text_hash"]
            for prototype_id in prototype_ids
        },
        "metadata": metadata,
        "npz_sha256": hashlib.sha256(npz_path.read_bytes()).hexdigest(),
    }
    _atomic_write_json(receipt_path, receipt)


def _event_public_path(event: dict[str, Any]) -> str:
    explicit = clean_text(event.get("path") or event.get("href"))
    if explicit.startswith("/"):
        return explicit
    slug = clean_text(event.get("slug"))
    return f"/sobytiya/{slug}/" if slug else f"/sobytiya/{int(event['id'])}/"


def _normalise_unusual_manifest(
    manifest: dict[str, Any],
    *,
    events: list[dict[str, Any]],
    build_metadata: dict[str, Any],
    vector_metadata: dict[str, Any],
    prototype_bank: dict[str, Any],
    classifier: dict[str, Any],
    migration: bool,
) -> dict[str, Any]:
    by_id = {int(event["id"]): event for event in events}
    quality_gate = manifest.get("quality_gate")
    if not isinstance(quality_gate, dict):
        quality_gate = {
            "status": str(manifest.get("status") or "unavailable"),
            "metrics": manifest.get("evaluation") or {},
        }
    else:
        quality_gate = dict(quality_gate)
        quality_gate.setdefault(
            "status",
            str(
                quality_gate.get("approval_status")
                or manifest.get("evaluation_approval_status")
                or manifest.get("status")
                or "unavailable"
            ),
        )
        quality_gate.setdefault(
            "metrics",
            quality_gate.get("observed") or manifest.get("evaluation") or {},
        )
    output = {
        **manifest,
        "schema_version": UNUSUAL_MANIFEST_SCHEMA_VERSION,
        "build_id": build_metadata.get("build_id"),
        "generated_at": build_metadata.get("generated_at"),
        "source_snapshot_id": build_metadata.get("source_snapshot_id"),
        "source_snapshot_hash": build_metadata.get("source_snapshot_hash"),
        # Canonical reader aliases. Keep the verbose producer fields below for
        # receipts/backward compatibility, but the Astro boundary consumes
        # these short, stable names.
        "hash": build_metadata.get("source_snapshot_hash"),
        "input_fingerprint": build_metadata.get("input_fingerprint"),
        "taxonomy_version": prototype_bank.get("taxonomy_version")
        or prototype_bank.get("schema_version"),
        "policy_version": classifier.get("policy_version")
        or classifier.get("schema_version"),
        "embedding_model": vector_metadata.get("model_id"),
        "embedding_revision": vector_metadata.get("model_revision"),
        "embedding_dim": vector_metadata.get("embedding_dim"),
        "revision": vector_metadata.get("model_revision"),
        "dim": vector_metadata.get("embedding_dim"),
        "doc_kind": "related_v1",
        "document_version": vector_metadata.get("document_version"),
        "prototype_bank_hash": vector_metadata.get("prototype_bank_sha256"),
        "classifier_hash": vector_metadata.get("classifier_sha256")
        or hashlib.sha256(
            json.dumps(classifier, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "quality_gate": quality_gate,
        "provider_calls": 0,
        "migration": {
            "enabled": bool(migration),
            "notify": False if migration else bool(
                (manifest.get("migration") or {}).get("notify")
                if isinstance(manifest.get("migration"), dict)
                else False
            ),
        },
    }
    items: list[dict[str, Any]] = []
    for raw in manifest.get("items") or []:
        if not isinstance(raw, dict):
            continue
        try:
            event_id = int(raw.get("event_id") or 0)
        except (TypeError, ValueError):
            continue
        event = by_id.get(event_id)
        if event is None:
            continue
        item = dict(raw)
        if item.get("confidence") is None:
            item["confidence"] = item.get("calibrated_confidence")
        item["representative_event_id"] = int(
            raw.get("representative_event_id") or event_id
        )
        raw_families = raw.get("families") or []
        item["family_scores"] = raw.get("family_scores") or {
            str(row.get("id")): row.get("score")
            for row in raw_families
            if isinstance(row, dict) and row.get("id")
        }
        item["families"] = [
            str(row.get("id") if isinstance(row, dict) else row)
            for row in raw_families
            if str(row.get("id") if isinstance(row, dict) else row).strip()
        ]
        # Final notification eligibility is assigned by the durable concept
        # state pass after the quality gate and rollout baseline are known.
        item["notify_eligible"] = False
        item["event_snapshot"] = event
        item["path"] = _event_public_path(event)
        item["date"] = event.get("start_date") or event.get("date")
        item["lifecycle"] = event.get("lifecycle_status") or "active"
        items.append(item)
    output["items"] = items
    output["shadow_items"] = list(manifest.get("shadow_items") or [])
    return output


def _apply_unusual_concept_state(
    manifest: dict[str, Any],
    *,
    previous_cache: dict[str, Any] | None,
    generated_at: str,
    migration: bool,
    approved: bool,
) -> dict[str, dict[str, Any]]:
    """Bind public concepts to durable first-publication/notification state.

    A first rollout is a silent baseline. Later builds may mark only genuinely
    new, approved ``core_unusual`` concepts as notification candidates. A date
    change or representative-event swap inside an existing concept never
    creates a new notification.
    """

    previous = (
        previous_cache.get("concepts")
        if isinstance(previous_cache, dict)
        and isinstance(previous_cache.get("concepts"), dict)
        else {}
    )
    has_established_baseline = bool(
        isinstance(previous_cache, dict)
        and previous_cache.get("rollout_baseline_at")
    )
    states: dict[str, dict[str, Any]] = {
        str(key): dict(value)
        for key, value in previous.items()
        if isinstance(value, dict)
    }
    for item in manifest.get("items") or []:
        if not isinstance(item, dict):
            continue
        concept_id = str(item.get("concept_id") or "").strip()
        if not concept_id:
            continue
        prior = states.get(concept_id) or {}
        first_published_at = str(
            prior.get("first_published_at") or generated_at
        )
        is_new = concept_id not in states
        newly_notify_eligible = bool(
            approved
            and not migration
            and has_established_baseline
            and is_new
            and item.get("tier") == "core_unusual"
        )
        durable_notify_eligible = bool(
            (
                prior.get("notify_eligible") is True
                or newly_notify_eligible
            )
            and item.get("tier") == "core_unusual"
        )
        # Migration/backfill output is always silent, but it must not erase a
        # previously established concept-level eligibility bit. Browser-local
        # seen state decides whether each user still sees the red dot.
        notify_eligible = bool(durable_notify_eligible and not migration)
        item["first_published_at"] = first_published_at
        item["notify_eligible"] = notify_eligible
        states[concept_id] = {
            "first_published_at": first_published_at,
            "previous_tier": item.get("tier"),
            "previous_content_hash": item.get("content_hash"),
            "notify_eligible": durable_notify_eligible,
            "representative_event_id": item.get("representative_event_id")
            or item.get("event_id"),
            "policy_version": manifest.get("policy_version"),
            "model_revision": manifest.get("revision")
            or manifest.get("embedding_revision"),
            "prototype_bank_hash": manifest.get("prototype_bank_hash"),
            "classifier_hash": manifest.get("classifier_hash"),
            "last_seen_at": generated_at,
        }
    # Bound state without discarding currently published concepts.
    ordered = sorted(
        states.items(),
        key=lambda pair: str(pair[1].get("last_seen_at") or pair[1].get("first_published_at") or ""),
        reverse=True,
    )
    return dict(ordered[:512])


def build_shared_bge_and_unusual(
    events: list[dict[str, Any]],
    *,
    out_dir: Path,
    build_metadata: dict[str, Any],
    vector_cache_path: Path,
    vector_receipt_path: Path,
    unusual_cache_path: Path,
    unusual_last_good_path: Path,
    model_revision: str,
    batch_size: int,
    migration: bool,
    quality_fixture_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Narrow adapter over the L02 semantic API.

    Expected L02 API:
      static_event_bge.build_related_v1_documents
      static_event_bge.build_shared_bge_vector_artifact
      static_event_bge.validate_shared_bge_vector_artifact
      unusual_event_semantics.load_unusual_prototype_bank
      unusual_event_semantics.score_unusual_manifest
    """

    import static_event_bge as bge_module
    import unusual_event_semantics as unusual_module

    log_stage("unusual_prototype_load")
    prototype_bank = unusual_module.load_unusual_prototype_bank()
    classifier = unusual_module.load_unusual_classifier()
    fixture_path = quality_fixture_path or SCRIPT_PATH.parent / "unusual_events_golden_v1.json"
    quality_fixture = _load_json_object(fixture_path)
    semantic_build_metadata = dict(build_metadata)
    encoding_events = list(events)
    public_ids = {int(event["id"]) for event in events}
    if quality_fixture is not None:
        # This adapter has no encoder injection path: cache misses invoke the
        # pinned local BGE model, so the frozen fixture evaluation is a real
        # CPU canary rather than a synthetic unit probe.
        semantic_build_metadata["evidence_kind"] = "real_bge_canary"
        for case in quality_fixture.get("cases") or []:
            if not isinstance(case, dict):
                continue
            try:
                event_id = int(case.get("event_id") or case.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if event_id <= 0 or event_id in public_ids:
                continue
            facts = case.get("facts") if isinstance(case.get("facts"), dict) else {}
            encoding_events.append(
                {
                    "id": event_id,
                    "title": facts.get("title") or case.get("title") or f"fixture {event_id}",
                    "event_type": facts.get("event_type") or "",
                    "summary": facts.get("short_description") or facts.get("summary") or "",
                    "short_description": facts.get("short_description") or "",
                    "search_digest": facts.get("search_digest") or "",
                    "description": facts.get("description") or facts.get("short_description") or "",
                    "description_html": facts.get("description_html") or "",
                    "city": facts.get("city") or "",
                    "venue_name": facts.get("location_name") or facts.get("venue_name") or "",
                    "start_date": facts.get("date") or build_metadata.get("as_of_date"),
                    "end_date": facts.get("end_date") or facts.get("date") or build_metadata.get("as_of_date"),
                    "lifecycle_status": "active",
                    "topics": facts.get("topics") or [],
                }
            )
        semantic_build_metadata["publication_event_count"] = len(events)
        semantic_build_metadata["quality_fixture_event_count"] = len(encoding_events) - len(events)
    log_stage(
        "unusual_vector_reuse_start",
        event_count=len(events),
        model_id=bge_module.MODEL_ID,
        model_revision=model_revision,
        provider_calls=0,
    )
    artifact = _load_cached_bge_artifact(
        npz_path=vector_cache_path,
        receipt_path=vector_receipt_path,
        prototype_bank=prototype_bank,
        classifier=classifier,
        events=encoding_events,
        bge_module=bge_module,
        model_revision=model_revision,
    )
    if (
        artifact is not None
        and quality_fixture is not None
        and not (
            isinstance((artifact.get("metadata") or {}).get("build"), dict)
            and (artifact.get("metadata") or {}).get("build", {}).get("evidence_kind")
            == "real_bge_canary"
        )
    ):
        artifact = None
    previous_artifact = artifact
    build_kwargs = {
        "model_revision": model_revision,
        "classifier": classifier,
        "batch_size": batch_size,
        "build_metadata": semantic_build_metadata,
    }
    if previous_artifact is not None:
        build_kwargs["previous_artifact"] = previous_artifact
    artifact = bge_module.build_shared_bge_vector_artifact(
        encoding_events,
        prototype_bank,
        **build_kwargs,
    )
    encoded_event_count = int((artifact.get("metadata") or {}).get("encoded_event_count") or 0)
    encoded_prototype_count = int((artifact.get("metadata") or {}).get("encoded_prototype_count") or 0)
    cache_state = (
        "hit_reused"
        if previous_artifact is not None and encoded_event_count == 0 and encoded_prototype_count == 0
        else "partial_rebuild"
        if previous_artifact is not None
        else "miss_rebuilt"
    )
    validation = bge_module.validate_shared_bge_vector_artifact(
        artifact,
        prototype_bank=prototype_bank,
        expected_classifier_sha256=bge_module.stable_hash(classifier),
    )
    if not validation.get("valid"):
        raise RuntimeError(
            "shared BGE artifact validation failed: "
            + "; ".join(validation.get("errors") or [])
        )
    _write_bge_cache(
        artifact=artifact,
        npz_path=vector_cache_path,
        receipt_path=vector_receipt_path,
    )
    metadata = artifact.get("metadata") or {}
    if (
        int(metadata.get("event_count") or -1) != len(encoding_events)
        or int(metadata.get("provider_calls", -1)) != 0
        or metadata.get("model_revision") != model_revision
    ):
        raise RuntimeError("shared BGE artifact is partial or has mismatched metadata")

    previous_cache = _load_json_object(unusual_cache_path)
    quality_evaluation = None
    if quality_fixture is not None:
        try:
            quality_evaluation = unusual_module.evaluate_unusual_quality_fixture(
                quality_fixture,
                artifact,
                prototype_bank,
                classifier,
            )
        except Exception as exc:
            log_stage(
                "unusual_quality_gate",
                status="fixture_evaluation_failed",
                approved=False,
                error=str(exc)[:300],
            )
    try:
        scored = unusual_module.score_unusual_manifest(
            events,
            artifact["event_vectors"],
            artifact["prototype_vectors"],
            metadata,
            previous_cache=previous_cache,
            build_metadata={
                **semantic_build_metadata,
                "migration": bool(migration),
                "quality_evaluation": quality_evaluation,
            },
            prototype_bank=prototype_bank,
            classifier=classifier,
        )
        if not isinstance(scored, dict) or not isinstance(scored.get("manifest"), dict):
            raise RuntimeError("unusual scorer returned an invalid result")
        log_stage(
            "unusual_score_complete",
            **_compact_unusual_metrics_for_log(scored.get("metrics")),
        )
        manifest = _normalise_unusual_manifest(
            scored["manifest"],
            events=events,
            build_metadata=build_metadata,
            vector_metadata=metadata,
            prototype_bank=prototype_bank,
            classifier=classifier,
            migration=migration,
        )
        rollout_baseline_at = str(
            (previous_cache or {}).get("rollout_baseline_at")
            or build_metadata.get("generated_at")
            or ""
        )
        manifest["rollout_baseline_at"] = rollout_baseline_at or None
        gate_status = str((manifest.get("quality_gate") or {}).get("status") or "").lower()
        approved = gate_status in {"approved", "pass", "passed", "ok"}
        concept_states = _apply_unusual_concept_state(
            manifest,
            previous_cache=previous_cache,
            generated_at=str(build_metadata.get("generated_at") or ""),
            migration=migration,
            approved=approved,
        )
        log_stage("unusual_quality_gate", status=gate_status, approved=approved)
        log_stage(
            "unusual_concept_dedup",
            item_count=len(manifest.get("items") or []),
            shadow_count=len(manifest.get("shadow_items") or []),
        )
        unusual_cache = scored.get("cache")
        if not isinstance(unusual_cache, dict):
            raise RuntimeError("unusual scorer cache missing")
        unusual_cache.update(
            {
                "schema_version": UNUSUAL_CACHE_SCHEMA_VERSION,
                "model_revision": metadata.get("model_revision"),
                "prototype_bank_hash": metadata.get("prototype_bank_sha256"),
                "input_fingerprint": build_metadata.get("input_fingerprint"),
                "policy_version": manifest.get("policy_version"),
                "source_snapshot_hash": build_metadata.get("source_snapshot_hash"),
                "migration": bool(migration),
                "rollout_baseline_at": rollout_baseline_at or None,
                "concepts": concept_states,
            }
        )
        _atomic_write_json(unusual_cache_path, unusual_cache)
        log_stage("unusual_cache_written", path=str(unusual_cache_path))
        if approved:
            _atomic_write_json(unusual_last_good_path, manifest)
        else:
            # Keep complete shadow/decision evidence, but no public card or
            # notification may escape an unapproved quality gate.
            manifest["items"] = []
            manifest["migration"] = {"enabled": True, "notify": False}
            log_stage(
                "unusual_feed_disabled",
                reason=f"quality_gate_{gate_status or 'missing'}",
            )
    except Exception as exc:
        last_good = _load_json_object(unusual_last_good_path)
        generated_at = datetime.fromisoformat(
            str(build_metadata.get("generated_at") or "").replace("Z", "+00:00")
        )
        last_good_generated_at = None
        if last_good and last_good.get("generated_at"):
            try:
                last_good_generated_at = datetime.fromisoformat(
                    str(last_good["generated_at"]).replace("Z", "+00:00")
                )
            except ValueError:
                last_good_generated_at = None
        compatible = bool(
            last_good
            and (last_good.get("revision") or last_good.get("embedding_revision"))
            == metadata.get("model_revision")
            and last_good.get("prototype_bank_hash") == metadata.get("prototype_bank_sha256")
            and last_good.get("classifier_hash")
            == (
                metadata.get("classifier_sha256")
                or hashlib.sha256(
                    json.dumps(classifier, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
            )
            and last_good.get("policy_version")
            == (classifier.get("policy_version") or classifier.get("schema_version"))
            and last_good_generated_at is not None
            and timedelta(0) <= generated_at - last_good_generated_at
            and generated_at - last_good_generated_at <= timedelta(days=7)
        )
        if compatible:
            current = {int(event["id"]): event for event in events}
            fallback_items: list[dict[str, Any]] = []
            for raw in last_good.get("items") or []:
                if not isinstance(raw, dict) or int(raw.get("event_id") or 0) not in current:
                    continue
                event = current[int(raw["event_id"])]
                try:
                    current_content_hash = bge_module.build_related_v1_document(event)[
                        "text_hash"
                    ]
                except Exception:
                    continue
                last_date = str(
                    event.get("end_date") or event.get("start_date") or ""
                )[:10]
                if (
                    raw.get("content_hash") != current_content_hash
                    or str(event.get("lifecycle_status") or "").lower() != "active"
                    or not last_date
                    or last_date < str(build_metadata.get("as_of_date") or "")
                ):
                    continue
                fallback_items.append(
                    {
                        **raw,
                        "notify_eligible": False,
                        "event_snapshot": event,
                        "path": _event_public_path(event),
                        "date": event.get("start_date") or event.get("date"),
                        "lifecycle": event.get("lifecycle_status") or "active",
                    }
                )
            manifest = {
                **last_good,
                "build_id": build_metadata.get("build_id"),
                "generated_at": build_metadata.get("generated_at"),
                "attempted_source_snapshot_id": build_metadata.get("source_snapshot_id"),
                "attempted_source_snapshot_hash": build_metadata.get("source_snapshot_hash"),
                "delivery_status": "last_good_fallback",
                "last_good_approved": True,
                "quality_gate": {
                    "status": "approved",
                    "metrics": (last_good.get("quality_gate") or {}).get("metrics")
                    or {},
                    "reason": str(exc)[:300],
                },
                "provider_calls": 0,
                "migration": {"enabled": True, "notify": False},
                "items": fallback_items,
            }
            log_stage("unusual_feed_disabled", reason=str(exc)[:300], fallback="last_good")
            log_stage("last_good_fallback", reason=str(exc)[:300], item_count=len(fallback_items))
        else:
            manifest = {
                "schema_version": UNUSUAL_MANIFEST_SCHEMA_VERSION,
                **build_metadata,
                "taxonomy_version": prototype_bank.get("taxonomy_version")
                or prototype_bank.get("schema_version"),
                "policy_version": classifier.get("policy_version")
                or classifier.get("schema_version"),
                "embedding_model": metadata.get("model_id"),
                "embedding_revision": metadata.get("model_revision"),
                "embedding_dim": metadata.get("embedding_dim"),
                "doc_kind": "related_v1",
                "document_version": metadata.get("document_version"),
                "prototype_bank_hash": metadata.get("prototype_bank_sha256"),
                "classifier_hash": metadata.get("classifier_sha256")
                or hashlib.sha256(
                    json.dumps(classifier, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
                "quality_gate": {"status": "disabled", "reason": str(exc)[:300]},
                "provider_calls": 0,
                "migration": {"enabled": True, "notify": False},
                "rollout_baseline_at": build_metadata.get("generated_at"),
                "items": [],
                "shadow_items": [],
            }
            log_stage("unusual_feed_disabled", reason=str(exc)[:300])
    manifest_path = out_dir / "unusual-events.json"
    _atomic_write_json(manifest_path, manifest)
    log_stage("unusual_manifest_written", path=str(manifest_path), item_count=len(manifest.get("items") or []))
    return artifact, {
        "status": str((manifest.get("quality_gate") or {}).get("status") or "unknown"),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "artifact_sha256": metadata.get("artifact_sha256"),
        "provider_calls": 0,
        "cache_state": cache_state,
        "event_count": len(events),
        "artifact_event_count": int(metadata.get("event_count") or 0),
        "ordinary_corpus_sha256": (
            (manifest.get("ordinary_corpus_receipt") or {}).get(
                "corpus_sha256"
            )
            if isinstance(manifest.get("ordinary_corpus_receipt"), dict)
            else None
        ),
        "ordinary_corpus_policy_sha256": (
            (manifest.get("ordinary_corpus_receipt") or {}).get(
                "policy_sha256"
            )
            if isinstance(manifest.get("ordinary_corpus_receipt"), dict)
            else None
        ),
        "item_count": len(manifest.get("items") or []),
        "migration": bool(
            (manifest.get("migration") or {}).get("enabled")
            if isinstance(manifest.get("migration"), dict)
            else manifest.get("migration")
        ),
        "input_fingerprint": build_metadata.get("input_fingerprint"),
        "vector_cache_sha256": (
            hashlib.sha256(vector_cache_path.read_bytes()).hexdigest()
            if vector_cache_path.is_file()
            else None
        ),
        "vector_receipt_sha256": (
            hashlib.sha256(vector_receipt_path.read_bytes()).hexdigest()
            if vector_receipt_path.is_file()
            else None
        ),
        "unusual_cache_sha256": (
            hashlib.sha256(unusual_cache_path.read_bytes()).hexdigest()
            if unusual_cache_path.is_file()
            else None
        ),
        "last_good_sha256": (
            hashlib.sha256(unusual_last_good_path.read_bytes()).hexdigest()
            if unusual_last_good_path.is_file()
            else None
        ),
    }


def build_collection_semantic_outputs(
    events: list[dict[str, Any]],
    *,
    out_dir: Path,
    build_metadata: dict[str, Any],
    catalog_ledger: dict[str, Any],
    collection_decisions_by_id: dict[int, Any],
    theatre_event_ids: set[int],
    registry_sha256: str,
    vector_cache_path: Path,
    vector_receipt_path: Path,
    unusual_cache_path: Path,
    model_revision: str,
    batch_size: int,
    collection_batch_output: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run the single mandatory collection BGE pass and emit fail-closed receipts."""

    import static_collection_export as collection_module
    import static_collection_batch as collection_batch_module
    import static_event_bge as bge_module
    import unusual_event_semantics as unusual_module

    policy = collection_module.load_object(collection_module.DEFAULT_POLICY_PATH)
    extension = collection_module.load_object(collection_module.DEFAULT_PROTOTYPES_PATH)
    unusual_bank = unusual_module.load_unusual_prototype_bank()
    prototype_bank = collection_module.merged_prototype_bank(unusual_bank, extension)
    classifier_contract = {
        "schema_version": "static-collection-head-contract-v1",
        "policy_sha256": collection_module.stable_hash(policy),
    }
    previous_artifact = bge_module.load_collection_bge_cache(
        npz_path=vector_cache_path,
        receipt_path=vector_receipt_path,
    )
    artifact = bge_module.build_collection_bge_vector_artifact(
        events,
        prototype_bank,
        model_revision=model_revision,
        classifier=classifier_contract,
        batch_size=max(1, int(batch_size)),
        build_metadata=build_metadata,
        previous_artifact=previous_artifact,
    )
    validation = bge_module.validate_collection_bge_vector_artifact(
        artifact,
        prototype_bank=prototype_bank,
        expected_classifier_sha256=bge_module.stable_hash(classifier_contract),
    )
    if not validation.get("valid"):
        raise RuntimeError(
            "collection BGE artifact validation failed: "
            + "; ".join(validation.get("errors") or [])
        )
    receipt = bge_module.write_collection_bge_cache(
        artifact,
        npz_path=vector_cache_path,
        receipt_path=vector_receipt_path,
    )
    physical_validation = bge_module.validate_collection_bge_cache(
        npz_path=vector_cache_path,
        receipt=receipt,
    )
    if not physical_validation.get("valid"):
        raise RuntimeError(
            "collection BGE cache validation failed: "
            + "; ".join(physical_validation.get("errors") or [])
        )

    candidates = collection_module.score_semantic_candidates(artifact, policy)
    catalog_hash = collection_module.stable_hash(catalog_ledger)
    batch = collection_module.build_collection_batch_payload(
        events=events,
        collection_decisions_by_id=collection_decisions_by_id,
        theatre_event_ids=theatre_event_ids,
        semantic_candidates=candidates,
        artifact=artifact,
        policy=policy,
        catalog_hash=catalog_hash,
        generated_at=str(build_metadata.get("generated_at") or datetime.now(timezone.utc).isoformat()),
        snapshot=catalog_ledger.get("snapshot") if isinstance(catalog_ledger.get("snapshot"), dict) else {},
        registry_sha256=registry_sha256,
    )
    catalog_ids = [
        int(row["event_id"])
        for row in catalog_ledger.get("eligible") or []
        if isinstance(row, dict) and str(row.get("event_id") or "").isdigit()
    ]
    batch_validation = collection_batch_module.validate_collection_batch(
        batch,
        catalog_item_ids=catalog_ids,
        require_compute=True,
    )
    if not batch_validation.get("valid"):
        raise RuntimeError(
            "collection batch validation failed: "
            + "; ".join(batch_validation.get("errors") or [])
        )
    collection_batch_module.write_collection_batch(
        collection_batch_output,
        batch,
    )

    unusual_candidates = (candidates.get("unusual") or {}).get("item_ids") or []
    unusual_manifest = collection_module.unusual_shadow_manifest(
        events=events,
        candidate_ids=unusual_candidates,
        generated_at=str(build_metadata.get("generated_at") or ""),
        build_metadata=build_metadata,
        artifact=artifact,
    )
    unusual_path = out_dir / "unusual-events.json"
    _atomic_write_json(unusual_path, unusual_manifest)
    unusual_cache = {
        "schema_version": "unusual-event-score-cache-v1",
        "status": "blocked",
        "reason": "collection_document_recalibration_required",
        "model_revision": (artifact.get("metadata") or {}).get("model_revision"),
        "prototype_bank_hash": (artifact.get("metadata") or {}).get("prototype_bank_sha256"),
        "input_fingerprint": build_metadata.get("input_fingerprint"),
        "candidate_event_ids": sorted(int(value) for value in unusual_candidates),
        "provider_calls": 0,
    }
    _atomic_write_json(unusual_cache_path, unusual_cache)
    metadata = artifact.get("metadata") or {}
    encoded_events = int(metadata.get("encoded_event_count") or 0)
    encoded_prototypes = int(metadata.get("encoded_prototype_count") or 0)
    return artifact, {
        "status": "validated",
        "provider_calls": 0,
        "event_count": len(events),
        "artifact_event_count": int(metadata.get("event_count") or 0),
        "artifact_sha256": metadata.get("artifact_sha256"),
        "cache_state": (
            "hit_reused"
            if previous_artifact is not None and encoded_events == 0 and encoded_prototypes == 0
            else "partial_rebuild"
            if previous_artifact is not None
            else "miss_rebuilt"
        ),
        "encoded_event_count": encoded_events,
        "encoded_prototype_count": encoded_prototypes,
        "manifest_sha256": hashlib.sha256(unusual_path.read_bytes()).hexdigest(),
        "vector_cache_sha256": hashlib.sha256(vector_cache_path.read_bytes()).hexdigest(),
        "vector_receipt_sha256": hashlib.sha256(vector_receipt_path.read_bytes()).hexdigest(),
        "unusual_cache_sha256": hashlib.sha256(unusual_cache_path.read_bytes()).hexdigest(),
        "collection_batch_sha256": hashlib.sha256(collection_batch_output.read_bytes()).hexdigest(),
        "collection_batch_contract_sha256": batch.get("batch_sha256"),
        "input_fingerprint": build_metadata.get("input_fingerprint"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Path to production SQLite snapshot")
    parser.add_argument("--output-dir", default="site/src/data")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--catalog-mode", choices=["slice", "full"], default="slice")
    parser.add_argument("--current-date", default=CURRENT_DATE_DEFAULT)
    parser.add_argument(
        "--current-datetime",
        default=os.getenv("STATIC_SITE_CURRENT_DATETIME", ""),
        help=(
            "Optional local YYYY-MM-DDTHH:MM build clock; the shared catalog "
            "retains the whole current day and time-sensitive surfaces filter later"
        ),
    )
    parser.add_argument("--focus-date-from", default=os.getenv("STATIC_SITE_FOCUS_DATE_FROM", ""), help="Prioritize one-day events starting on/after this date before the normal future fill")
    parser.add_argument("--focus-date-to", default=os.getenv("STATIC_SITE_FOCUS_DATE_TO", ""), help="Prioritize one-day events starting on/before this date before the normal future fill")
    parser.add_argument("--include-ids", default=",".join(map(str, CONTROL_EVENT_IDS)))
    parser.add_argument("--related-cache", default="", help="Persistent JSON cache for event_sparse_related_chain_v1")
    parser.add_argument("--related-mode", choices=["sparse", "pgvector", "bge"], default=os.getenv("STATIC_SITE_RELATED_MODE", "sparse"))
    parser.add_argument("--sync-pgvector-vectors", action="store_true", default=(os.getenv("STATIC_SITE_SYNC_PGVECTOR_VECTORS", "").strip().lower() in {"1", "true", "yes", "on"}), help="Upsert event search docs/embeddings before pgvector related build")
    parser.add_argument("--pgvector-embedding-model", default=os.getenv("STATIC_SITE_PGVECTOR_EMBEDDING_MODEL", "gemini-embedding-2"))
    parser.add_argument("--pgvector-embedding-key-env", default=os.getenv("STATIC_SITE_PGVECTOR_EMBEDDING_KEY_ENV", "GOOGLE_API_KEY4"))
    parser.add_argument("--related-corpus-revision", default=os.getenv("STATIC_SITE_RELATED_CORPUS_REVISION", ""), help="SHA-256 revision from the completed related_v1 vector-sync receipt")
    parser.add_argument(
        "--related-response-max-bytes",
        type=int,
        default=int(
            os.getenv(
                "STATIC_SITE_RELATED_RESPONSE_MAX_BYTES",
                str(DEFAULT_RELATED_RESPONSE_MAX_BYTES),
            )
            or str(DEFAULT_RELATED_RESPONSE_MAX_BYTES)
        ),
        help="Maximum encoded body size for one compact related RPC response.",
    )
    parser.add_argument(
        "--related-total-response-max-bytes",
        type=int,
        default=int(
            os.getenv(
                "STATIC_SITE_RELATED_TOTAL_RESPONSE_MAX_BYTES",
                str(DEFAULT_RELATED_TOTAL_RESPONSE_MAX_BYTES),
            )
            or str(DEFAULT_RELATED_TOTAL_RESPONSE_MAX_BYTES)
        ),
        help="Maximum aggregate compact related RPC response bytes for a full rebuild.",
    )
    parser.add_argument("--pgvector-max-provider-calls", type=int, default=int(os.getenv("STATIC_SITE_PGVECTOR_MAX_PROVIDER_CALLS", "1000") or "1000"))
    parser.add_argument("--site-origin", default=os.getenv("PUBLIC_SITE_ORIGIN", "https://kenigevents.ru"))
    parser.add_argument("--base-path", default=os.getenv("PUBLIC_PREVIEW_BUILD_ID", ""))
    parser.add_argument("--ics-base-url", default=os.getenv("PUBLIC_ICS_BASE_URL", ""))
    parser.add_argument("--repo-sha", default=os.getenv("STATIC_SITE_REPO_SHA", ""))
    parser.add_argument("--run-id", default=os.getenv("STATIC_SITE_RUN_ID", ""))
    parser.add_argument("--build-id", default=os.getenv("STATIC_SITE_BUILD_ID", ""))
    parser.add_argument("--snapshot-id", default=os.getenv("STATIC_SITE_SNAPSHOT_ID", ""))
    parser.add_argument("--snapshot-sha256", default=os.getenv("STATIC_SITE_SNAPSHOT_SHA256", ""))
    parser.add_argument("--snapshot-size", type=int, default=int(os.getenv("STATIC_SITE_SNAPSHOT_SIZE", "0") or "0"))
    parser.add_argument("--input-fingerprint", default=os.getenv("STATIC_SITE_INPUT_FINGERPRINT", ""))
    parser.add_argument("--gemma-related-verify", action="store_true", help="Run optional Gemma 4 26B audit for changed related chains")
    parser.add_argument("--gemma-related-model", default="models/gemma-4-26b-a4b-it")
    parser.add_argument("--gemma-related-key-env", default="GOOGLE_API_KEY4")
    parser.add_argument("--gemma-related-max-anchors", type=int, default=0, help="0 = no cap for enabled audit")
    parser.add_argument(
        "--bge-vector-cache",
        default=os.getenv("STATIC_SITE_BGE_VECTOR_CACHE", ""),
        help="Persistent shared BGE event/prototype NPZ cache.",
    )
    parser.add_argument(
        "--bge-vector-receipt",
        default=os.getenv("STATIC_SITE_BGE_VECTOR_RECEIPT", ""),
        help="Hash-bound JSON receipt for --bge-vector-cache.",
    )
    parser.add_argument(
        "--bge-model-revision",
        default=os.getenv("STATIC_SITE_BGE_MODEL_REVISION", BGE_MODEL_REVISION_DEFAULT),
    )
    parser.add_argument(
        "--bge-batch-size",
        type=int,
        default=int(os.getenv("STATIC_SITE_BGE_BATCH_SIZE", "8") or "8"),
    )
    parser.add_argument(
        "--collection-semantic-compute",
        action="store_true",
        default=os.getenv("STATIC_SITE_COLLECTION_SEMANTIC_COMPUTE", "").strip().lower()
        in {"1", "true", "yes", "on"},
        help="Compute and validate the shared collection matrix even when publication is disabled.",
    )
    parser.add_argument(
        "--collection-batch-output",
        default=os.getenv("STATIC_SITE_COLLECTION_BATCH", ""),
    )
    parser.add_argument(
        "--collection-batch-last-good",
        default=os.getenv("STATIC_SITE_COLLECTION_LAST_GOOD", ""),
        help="Reserved durable last-good path; promotion occurs only after a ready quality gate.",
    )
    parser.add_argument(
        "--unusual-cache",
        default=os.getenv("STATIC_SITE_UNUSUAL_CACHE", ""),
    )
    parser.add_argument(
        "--unusual-last-good",
        default=os.getenv("STATIC_SITE_UNUSUAL_LAST_GOOD", ""),
    )
    parser.add_argument(
        "--unusual-enabled",
        action="store_true",
        default=os.getenv("STATIC_SITE_UNUSUAL_ENABLED", "0").strip().lower()
        in {"1", "true", "yes", "on"},
    )
    parser.add_argument(
        "--unusual-migration",
        action="store_true",
        default=os.getenv("STATIC_SITE_UNUSUAL_MIGRATION", "1").strip().lower()
        in {"1", "true", "yes", "on"},
        help="Suppress notification eligibility while adopting the first manifest.",
    )
    parser.add_argument(
        "--skip-related",
        action="store_true",
        help="Export preview-events.json only; used by the dedicated vector projection lane.",
    )
    parser.add_argument(
        "--skip-image-probes",
        action="store_true",
        help="Do not make remote image-dimension requests (vector projection fast path).",
    )
    args = parser.parse_args()

    if args.related_response_max_bytes < 1024:
        raise SystemExit("--related-response-max-bytes must be at least 1024")
    if args.related_total_response_max_bytes < args.related_response_max_bytes:
        raise SystemExit(
            "--related-total-response-max-bytes must be at least the per-response limit"
        )

    global SKIP_IMAGE_PROBES
    SKIP_IMAGE_PROBES = bool(args.skip_image_probes)

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    include_ids = [int(part) for part in args.include_ids.split(",") if part.strip().isdigit()]
    if args.catalog_mode == "full":
        # Full production export is a single deterministic catalog query; the
        # preview-only control-event priority must not affect its ordering.
        include_ids = []
    effective_date, effective_time = split_current_datetime(args.current_datetime, args.current_date)
    if args.catalog_mode == "full":
        required_metadata = {
            "repo_sha": args.repo_sha,
            "run_id": args.run_id,
            "build_id": args.build_id,
            "snapshot_id": args.snapshot_id,
            "snapshot_sha256": args.snapshot_sha256,
        }
        missing = [key for key, value in required_metadata.items() if not str(value or "").strip()]
        if missing:
            raise SystemExit(f"full catalog export requires metadata: {', '.join(missing)}")
        if not re.fullmatch(r"[0-9a-f]{40}", args.repo_sha):
            raise SystemExit("full catalog export requires a full 40-character repo SHA")
        if not re.fullmatch(r"[0-9a-f]{64}", args.snapshot_sha256):
            raise SystemExit("full catalog export requires a 64-character snapshot SHA-256")
        if args.related_mode == "pgvector" and not re.fullmatch(r"[0-9a-f]{64}", str(args.related_corpus_revision or "")):
            raise SystemExit("full pgvector export requires --related-corpus-revision from the completed vector receipt")
    rows = fetch_rows(
        con,
        None if args.catalog_mode == "full" else args.limit,
        effective_date,
        include_ids,
        current_time=effective_time,
        focus_date_from=args.focus_date_from,
        focus_date_to=args.focus_date_to,
    )
    archive_rows = (
        fetch_recent_event_detail_archive_rows(con, effective_date)
        if args.catalog_mode == "full"
        else []
    )
    participants_by_event = event_participants_for_events(
        con,
        [int(row["id"]) for row in [*rows, *archive_rows]],
    )
    videos_by_event = event_video_assets_for_events(
        con,
        [int(row["id"]) for row in [*rows, *archive_rows]],
    )
    events = [
        build_event(
            con,
            row,
            effective_date,
            participants=participants_by_event.get(int(row["id"]), []),
            video_assets=videos_by_event.get(int(row["id"]), []),
        )
        for row in rows
    ]
    normalize_linked_occurrences(events)
    archived_events = [
        build_event(
            con,
            row,
            effective_date,
            participants=participants_by_event.get(int(row["id"]), []),
            video_assets=videos_by_event.get(int(row["id"]), []),
        )
        for row in archive_rows
    ]
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    preview = {
        "build": {
            "generated_at": generated_at,
            "source": "prod-sqlite-static-site-export-v1",
            "current_date": args.current_date,
            "current_datetime": args.current_datetime or None,
            "effective_current_date": effective_date,
            "effective_current_time": effective_time,
            "focus_date_from": args.focus_date_from or None,
            "focus_date_to": args.focus_date_to or None,
            "catalog_mode": args.catalog_mode,
            "notes": [
                (
                    f"full eligible production catalog: {len(events)} real events"
                    if args.catalog_mode == "full"
                    else f"bounded production slice: {len(events)} real events"
                ),
                "source social likes/views are compact latest-metric aggregates",
                "service likes remain 0 until first-party backend ingest is enabled",
            ],
        },
        "events": events,
    }
    preview_events_path = out_dir / "preview-events.json"
    preview_events_path.write_text(json.dumps(preview, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    event_detail_archive = {
        "build": {
            "generated_at": generated_at,
            "source": "prod-sqlite-static-site-event-detail-archive-v1",
            "current_date": effective_date,
            "notes": [
                f"{len(archived_events)} recently elapsed canonical event detail routes",
                f"retention_days={EVENT_DETAIL_ARCHIVE_DAYS}",
                "detail/ICS only; excluded from listings, Search, Popular and recommendations",
            ],
        },
        "events": archived_events,
    }
    (out_dir / "preview-event-archive.json").write_text(
        json.dumps(event_detail_archive, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    ledger: dict[str, Any] = {
        "schema_version": CATALOG_LEDGER_SCHEMA_VERSION,
        "generated_at": generated_at,
        "current_date": effective_date,
        "snapshot": {
            "snapshot_id": args.snapshot_id or None,
            "sha256": args.snapshot_sha256 or None,
            "size": args.snapshot_size or None,
        },
        "eligible": [{"event_id": int(event["id"])} for event in events],
        "excluded": [],
    }
    if args.catalog_mode == "full":
        ledger = build_catalog_ledger(
            con,
            rows,
            current_date=effective_date,
            current_time=effective_time,
            generated_at=generated_at,
            repo_sha=args.repo_sha,
            run_id=args.run_id,
            build_id=args.build_id,
            snapshot_id=args.snapshot_id,
            snapshot_sha256=args.snapshot_sha256,
            snapshot_size=args.snapshot_size or None,
        )
        (out_dir / "production-catalog.json").write_text(
            json.dumps(ledger, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    import static_collection_export as collection_export_module
    from static_place_org_registry import load_registry, registry_hash

    place_org_registry = load_registry()
    source_records_by_id = {
        int(event["id"]): collect_source_records(con, int(event["id"]))
        for event in events
    }
    catalog_hash = collection_export_module.stable_hash(ledger)
    venue_pages, theatre_event_ids = collection_export_module.build_registry_projection(
        events,
        source_records_by_id=source_records_by_id,
        registry=place_org_registry,
        generated_at=generated_at,
        catalog_hash=catalog_hash,
    )
    _atomic_write_json(out_dir / "venue-pages-v1.json", venue_pages)
    collection_decisions_by_id = {
        int(row["id"]): row_get(row, "collection_decisions") for row in rows
    }
    clubs_projection = build_interest_clubs_projection(
        con,
        current_date=effective_date,
        generated_at=generated_at,
        exported_events=events,
        enabled=os.getenv("ENABLE_INTEREST_CLUB_STATIC_PROJECTION", "").strip().lower() in {"1", "true", "yes", "on"},
    )
    (out_dir / "interest-clubs.json").write_text(
        json.dumps(clubs_projection, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    clubs_projection_v2 = build_interest_clubs_projection_v2(
        con,
        current_date=effective_date,
        generated_at=generated_at,
        exported_events=events,
        enabled=os.getenv("ENABLE_INTEREST_CLUB_STATIC_PROJECTION", "").strip().lower() in {"1", "true", "yes", "on"},
    )
    (out_dir / "interest-clubs-static-v2.json").write_text(
        json.dumps(clubs_projection_v2, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    festival_projection = build_festival_timeline_projection(
        con,
        current_date=effective_date,
        generated_at=generated_at,
        require_complete=args.catalog_mode == "full",
    )
    (out_dir / "festival-timeline.json").write_text(
        json.dumps(festival_projection, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if args.skip_related and not args.collection_semantic_compute:
        print(f"Exported {len(events)} events to {out_dir}")
        print("IDs:", ",".join(str(event["id"]) for event in events))
        print("Related: skipped")
        return 0
    shared_bge_artifact: dict[str, Any] | None = None
    semantic_result: dict[str, Any] = {
        "status": "disabled",
        "provider_calls": 0,
    }
    if args.collection_semantic_compute:
        if not re.fullmatch(r"[0-9a-f]{40}", str(args.bge_model_revision or "")):
            raise SystemExit("--bge-model-revision must be a pinned 40-character commit")
        vector_cache = (
            Path(args.bge_vector_cache)
            if args.bge_vector_cache
            else out_dir.parent / "static_event_bge_vectors.npz"
        )
        vector_receipt = (
            Path(args.bge_vector_receipt)
            if args.bge_vector_receipt
            else out_dir.parent / "static_event_bge_vectors.receipt.json"
        )
        unusual_cache = (
            Path(args.unusual_cache)
            if args.unusual_cache
            else out_dir.parent / "unusual_events_cache.json"
        )
        collection_batch_output = (
            Path(args.collection_batch_output)
            if args.collection_batch_output
            else out_dir / "collection-batch-v1.json"
        )
        shared_bge_artifact, semantic_result = build_collection_semantic_outputs(
            events,
            out_dir=out_dir,
            build_metadata={
                "build_id": args.build_id or args.base_path or "local-static-build",
                "generated_at": generated_at,
                "as_of_date": effective_date,
                "source_snapshot_id": args.snapshot_id or None,
                "source_snapshot_hash": args.snapshot_sha256 or None,
                "input_fingerprint": args.input_fingerprint or None,
            },
            catalog_ledger=ledger,
            collection_decisions_by_id=collection_decisions_by_id,
            theatre_event_ids=theatre_event_ids,
            registry_sha256=registry_hash(place_org_registry),
            vector_cache_path=vector_cache,
            vector_receipt_path=vector_receipt,
            unusual_cache_path=unusual_cache,
            model_revision=args.bge_model_revision,
            batch_size=max(1, int(args.bge_batch_size)),
            collection_batch_output=collection_batch_output,
        )
        _atomic_write_json(out_dir / "static-semantic-build-result.json", semantic_result)
    elif args.related_mode == "bge" or args.unusual_enabled:
        if not re.fullmatch(r"[0-9a-f]{40}", str(args.bge_model_revision or "")):
            raise SystemExit("--bge-model-revision must be a pinned 40-character commit")
        vector_cache = (
            Path(args.bge_vector_cache)
            if args.bge_vector_cache
            else out_dir.parent / "static_event_bge_vectors.npz"
        )
        vector_receipt = (
            Path(args.bge_vector_receipt)
            if args.bge_vector_receipt
            else out_dir.parent / "static_event_bge_vectors.receipt.json"
        )
        unusual_cache = (
            Path(args.unusual_cache)
            if args.unusual_cache
            else out_dir.parent / "unusual_events_cache.json"
        )
        unusual_last_good = (
            Path(args.unusual_last_good)
            if args.unusual_last_good
            else out_dir.parent / "unusual_events_last_good.json"
        )
        shared_bge_artifact, semantic_result = build_shared_bge_and_unusual(
            events,
            out_dir=out_dir,
            build_metadata={
                "build_id": args.build_id or args.base_path or "local-static-build",
                "generated_at": generated_at,
                "as_of_date": effective_date,
                "source_snapshot_id": args.snapshot_id or None,
                "source_snapshot_hash": args.snapshot_sha256 or None,
                "input_fingerprint": args.input_fingerprint or None,
            },
            vector_cache_path=vector_cache,
            vector_receipt_path=vector_receipt,
            unusual_cache_path=unusual_cache,
            unusual_last_good_path=unusual_last_good,
            model_revision=args.bge_model_revision,
            batch_size=max(1, int(args.bge_batch_size)),
            migration=bool(args.unusual_migration),
        )
        _atomic_write_json(out_dir / "static-semantic-build-result.json", semantic_result)
    if args.skip_related:
        print(f"Exported {len(events)} events to {out_dir}")
        print("IDs:", ",".join(str(event["id"]) for event in events))
        print("Related: skipped")
        return 0
    if args.related_mode == "pgvector" and args.sync_pgvector_vectors:
        sync_event_vectors_to_supabase(
            preview_events_json=preview_events_path,
            build_id=args.base_path,
            site_origin=args.site_origin,
            ics_base_url=args.ics_base_url,
            embedding_model=args.pgvector_embedding_model,
            embedding_key_env=args.pgvector_embedding_key_env,
            max_provider_calls=args.pgvector_max_provider_calls,
        )
    related_cache = Path(args.related_cache) if args.related_cache else None
    related_payload = build_related(
        events,
        current_date=effective_date,
        related_mode=args.related_mode,
        cache_path=related_cache,
        gemma_verify=bool(args.gemma_related_verify),
        gemma_model=args.gemma_related_model,
        gemma_key_env=args.gemma_related_key_env,
        gemma_max_anchors=max(0, int(args.gemma_related_max_anchors or 0)),
        embedding_model=args.pgvector_embedding_model,
        related_corpus_revision=args.related_corpus_revision,
        shared_bge_artifact=shared_bge_artifact,
        related_response_max_bytes=args.related_response_max_bytes,
        related_total_response_max_bytes=(
            args.related_total_response_max_bytes
            if args.catalog_mode == "full" and args.related_mode == "pgvector"
            else None
        ),
    )
    if args.catalog_mode == "full" and args.related_mode == "pgvector":
        validate_pgvector_graph_release(related_payload.get("graph_reciprocity") or {})
    (out_dir / "preview-related.json").write_text(json.dumps(related_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Exported {len(events)} events to {out_dir}")
    print("IDs:", ",".join(str(event["id"]) for event in events))
    print("Related:", related_payload.get("algorithm"), related_payload.get("cache"), related_payload.get("gemma_verification"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
