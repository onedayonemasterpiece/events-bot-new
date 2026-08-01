#!/usr/bin/env python3
"""Backfill grounded copy for legacy Region Talk social candidates.

Only role-scoped discovery sessions may fetch Telegram source text.  Existing
Gemini acceptance and delivery fields are monotonic: this worker updates draft
and draft-backfill fields only, never replaces the original publication
verdict.  Provider calls use the shared Supabase limiter plus a durable YDB
request/budget ledger.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "kaggle" / "RegionTalkCandidateReport") not in sys.path:
    sys.path.insert(0, str(ROOT / "kaggle" / "RegionTalkCandidateReport"))

import region_talk_candidate_report as rt  # type: ignore  # noqa: E402
from region_talk_llm_runtime import DurableGeminiBudget  # noqa: E402
from scripts import region_talk_goal_notify as notify  # noqa: E402
from scripts.region_talk_vk_media_prefetch import local_vk_posts, parse_vk_post  # noqa: E402


DRAFT_BACKFILL_VERSION = "region_talk_publication_draft_backfill_v2_editorial"
EDITORIAL_WRITER_VERSION = "region_talk_editorial_onboarding_writer_v8_staged"
EDITORIAL_INPUT_CONTRACT = "region_talk_editorial_onboarding_input_v2"
EDITORIAL_OUTPUT_CONTRACT = "region_talk_editorial_onboarding_output_v2"
MEDIA_MATERIALIZATION_CONTRACT_VERSION = "region_talk_media_materialization_v1"
LEGACY_REVIEW_MIGRATION_VERSION = "region_talk_legacy_review_to_v8_v1"
DRAFT_FIELDS = (
    "publication_draft_status",
    "publication_draft_title",
    "publication_draft_source_attribution",
    "publication_draft_telegram_text",
    "publication_draft_vk_text",
    "publication_draft_fact_points_json",
    "publication_draft_prompt_version",
    "publication_draft_contract_version",
    "publication_draft_input_fingerprint",
    "publication_draft_evidence_hash",
    "publication_draft_evidence_json",
    "publication_draft_history_json",
    "publication_draft_editorial_plan_json",
    "publication_draft_grounding_map_json",
    "publication_draft_critic_json",
    "publication_draft_stage_audit_json",
    "publication_draft_generation_attempts",
    "publication_presentation_mode",
    "publication_media_materialization_status",
    "publication_media_materialization_reason",
    "publication_media_materialization_contract_version",
    "publication_presentation_manifest_json",
)
TERMINAL_BACKFILL_STATUSES = {
    "ready",
    "llm_not_accepted",
    "needs_grounding_review",
    "source_text_unavailable",
    "unsupported_surface",
}

_BANNED_COPY_PATTERNS = (
    r"\bуникальн\w*\b",
    r"\bневероятн\w*\b",
    r"\bобязательно\s+посмотр\w*\b",
    r"\bв\s+данной\s+статье\b",
    r"\bв\s+рамках\b",
)
_FIRST_PERSON_OWNERSHIP = re.compile(
    r"\b(?:мы|наш(?:а|е|и|его|ему|ими)?|нам|нами)\s+"
    r"(?:увидел\w*|заметил\w*|почувствовал\w*|проехал\w*|посетил\w*|снял\w*)\b",
    re.I,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def telegram_post_ref(value: str) -> tuple[str, int] | None:
    normalized = notify.canonical_post_url({"post_url": value})
    match = re.fullmatch(r"https://t\.me/([^/]+)/([0-9]+)", normalized, re.I)
    if not match or match.group(1).lower() in {"c", "joinchat"}:
        return None
    return match.group(1), int(match.group(2))


def social_post_surface(value: str) -> str:
    if telegram_post_ref(value) is not None:
        return "telegram"
    if parse_vk_post(value) is not None:
        return "vk"
    return ""


def content_lane(row: dict[str, Any]) -> str:
    origin = str(row.get("content_origin_type") or "").lower()
    return "article" if origin in {"editorial_publication", "academic_publication"} else "social"


def current_editorial_draft(row: dict[str, Any]) -> bool:
    return bool(
        notify.is_publication_draft_ready(row)
        and str(row.get("publication_draft_prompt_version") or "") == EDITORIAL_WRITER_VERSION
        and str(row.get("publication_draft_contract_version") or "") == EDITORIAL_OUTPUT_CONTRACT
    )


def _json_value(value: Any, default: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _canonical_url(row: dict[str, Any]) -> str:
    return notify.canonical_post_url({
        "post_url": row.get("post_url") or row.get("canonical_url") or row.get("url")
    })


def publication_history(rows: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    """Return bounded real publication/approval history, never mere queue rows."""

    selected: list[dict[str, Any]] = []
    for row in rows:
        statuses = {
            str(row.get("status") or "").lower(),
            str(row.get("plan_status") or "").lower(),
            str(row.get("target_publication_status") or "").lower(),
            str(row.get("public_publication_status") or "").lower(),
        }
        published = bool(statuses & {"published", "target_published", "completed"})
        approved = (
            str(row.get("operator_review_decision") or "") == "approved"
            and str(row.get("operator_review_rewrite_status") or "") == "clean"
            and bool(str(row.get("operator_review_fingerprint") or "").strip())
            and str(row.get("operator_review_fingerprint") or "").strip()
            == notify.publication_operator_review_fingerprint(row)
        )
        if not (published or approved):
            continue
        p1, p2 = editorial_paragraphs(str(row.get("publication_draft_telegram_text") or ""))
        editorial_plan = _json_value(row.get("publication_draft_editorial_plan_json"), {})
        selected.append({
            "candidate_url": _canonical_url(row),
            "source": str(row.get("publication_draft_source_attribution") or row.get("source_title") or "")[:220],
            "lane": content_lane(row),
            "paragraph_1": p1[:500],
            "paragraph_2": p2[:500],
            "title": str(row.get("publication_draft_title") or row.get("publication_title") or "")[:180],
            "throughline_mode": str(editorial_plan.get("throughline_mode") or "")[:80],
            "state": "published" if published else "operator_approved",
            "event_at": str(
                row.get("published_at") or row.get("target_published_at")
                or row.get("operator_review_observed_at") or row.get("updated_at") or ""
            ),
        })
    selected.sort(key=lambda item: item["event_at"], reverse=True)
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in selected:
        key = item["candidate_url"]
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)
        if len(result) >= max(0, min(5, int(limit))):
            break
    return result


def recent_history_requires_fresh_start(history: list[dict[str, Any]]) -> bool:
    """Two transitions among the latest three force a non-template opening."""

    return sum(
        1 for item in history[:3]
        if str(item.get("throughline_mode") or "") in {
            "explicit_transition", "contrast_or_scale_shift",
        }
    ) >= 2


def article_intake_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        keys = {
            str(row.get("external_publication_id") or "").strip(),
            _canonical_url(row),
        } - {""}
        for key in keys:
            previous = index.get(key)
            if previous is None or str(row.get("updated_at") or "") >= str(previous.get("updated_at") or ""):
                index[key] = row
    return index


def _source_name(row: dict[str, Any], intake: dict[str, Any] | None = None) -> str:
    publication = (intake or {}).get("publication") if isinstance((intake or {}).get("publication"), dict) else {}
    return str(
        row.get("publication_draft_source_attribution")
        or publication.get("source_name")
        or row.get("source_title")
        or "Источник"
    ).strip()[:220]


def build_editorial_evidence(
    row: dict[str, Any],
    *,
    source_text: str = "",
    fetched: dict[str, Any] | None = None,
    intake: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an ID-addressable lossless evidence pack for the writer stages."""

    evidence: list[dict[str, Any]] = []
    source_name = _source_name(row, intake)
    if source_name:
        evidence.append({"evidence_id": "source.name", "kind": "source_profile_fact", "text": source_name})
    source_url = str(row.get("source_url") or "").strip()
    if source_url:
        evidence.append({"evidence_id": "source.url", "kind": "source_profile_fact", "text": source_url})
    onboarding = str(row.get("source_onboarding_paragraph") or "").strip()
    if str(row.get("source_onboarding_status") or "") == "ready" and onboarding:
        evidence.append({"evidence_id": "source.profile", "kind": "source_profile_fact", "text": onboarding})
    if source_text.strip():
        evidence.append({
            "evidence_id": "content.exact_text",
            "kind": "content_fact",
            "text": source_text.strip(),
            "lossless": True,
        })
    if intake:
        for index, item in enumerate(intake.get("evidence") or [], 1):
            if not isinstance(item, dict):
                continue
            evidence_id = str(item.get("evidence_id") or f"article.evidence.{index}")
            text = str(item.get("paraphrase") or item.get("quote_short") or item.get("text") or "").strip()
            if text:
                evidence.append({
                    "evidence_id": evidence_id,
                    "kind": "content_fact",
                    "text": text,
                    "source_url": item.get("source_url") or item.get("url") or "",
                })
        publication = intake.get("publication") if isinstance(intake.get("publication"), dict) else {}
        for field in ("title", "abstract", "summary"):
            value = str(publication.get(field) or "").strip()
            if value:
                evidence.append({"evidence_id": f"article.{field}", "kind": "content_fact", "text": value})
    visual_ids = []
    if (
        str(row.get("original_photo_evidence") or "").lower() == "true"
        or row.get("selected_image_url") or row.get("publication_primary_image_url")
        or _json_value(row.get("selected_media_materialization_json"), [])
    ):
        evidence.append({
            "evidence_id": "visual.original_media",
            "kind": "visual_observation",
            "text": "У исходного материала есть отобранный и сопоставленный с ним визуальный ряд.",
        })
        visual_ids.append("visual.original_media")
    # Deduplicate by ID without weakening the first, most specific evidence.
    unique: dict[str, dict[str, Any]] = {}
    for item in evidence:
        unique.setdefault(str(item["evidence_id"]), item)
    return {
        "input_contract": EDITORIAL_INPUT_CONTRACT,
        "candidate": {
            "candidate_id": str(row.get("publication_candidate_id") or row.get("external_publication_id") or ""),
            "lane": content_lane(row),
            "url": _canonical_url(row),
            "externality_status": "verified",
        },
        "source_profile": {"name": source_name, "url": source_url},
        "evidence": list(unique.values()),
        "visual_hook_evidence_ids": visual_ids,
        "fetched": dict(fetched or {}),
    }


def editorial_paragraphs(text: str) -> tuple[str, str]:
    body = str(text or "").strip()
    body = re.split(r"\n(?:\*\*|<b>)?(?:Источник|Оригинал)(?:\*\*|</b>)?:", body, maxsplit=1, flags=re.I)[0].strip()
    parts = [part.strip() for part in re.split(r"\n\s*\n", body) if part.strip()]
    return (parts[0] if parts else "", parts[1] if len(parts) > 1 else "")


def validate_editorial_output(output: dict[str, Any], evidence_ids: set[str]) -> list[str]:
    violations: list[str] = []
    public_copy = output.get("public_copy") if isinstance(output.get("public_copy"), dict) else {}
    p1 = re.sub(r"\s+", " ", str(public_copy.get("paragraph_1") or "")).strip()
    p2 = re.sub(r"\s+", " ", str(public_copy.get("paragraph_2") or "")).strip()
    if not (150 <= len(p1) <= 500):
        violations.append("paragraph_1_length")
    if not (150 <= len(p2) <= 500):
        violations.append("paragraph_2_length")
    if len(p1) + len(p2) > 820:
        violations.append("editorial_copy_too_long")
    combined = p1 + " " + p2
    if any(re.search(pattern, combined, re.I) for pattern in _BANNED_COPY_PATTERNS):
        violations.append("banned_lexeme")
    if _FIRST_PERSON_OWNERSHIP.search(combined):
        violations.append("third_person_boundary")
    cyrillic = len(re.findall(r"[А-Яа-яЁё]", combined))
    letters = len(re.findall(r"[A-Za-zА-Яа-яЁё]", combined))
    if letters and cyrillic / letters < 0.95:
        violations.append("russian_language")
    grounding = output.get("grounding_map") if isinstance(output.get("grounding_map"), list) else []
    if not grounding:
        violations.append("grounding_map_missing")
    for item in grounding:
        if not isinstance(item, dict):
            violations.append("grounding_map_invalid")
            continue
        refs = {str(value) for value in (item.get("evidence_ids") or [])}
        if not refs or not refs.issubset(evidence_ids):
            violations.append("unknown_or_empty_evidence_id")
        if item.get("third_person_maintained") is not True:
            violations.append("third_person_not_confirmed")
    return sorted(set(violations))


def render_public_copy(row: dict[str, Any], output: dict[str, Any]) -> tuple[str, str, str]:
    public_copy = output.get("public_copy") if isinstance(output.get("public_copy"), dict) else {}
    p1 = re.sub(r"\s+", " ", str(public_copy.get("paragraph_1") or "")).strip()
    p2 = re.sub(r"\s+", " ", str(public_copy.get("paragraph_2") or "")).strip()
    source = _source_name(row)
    url = _canonical_url(row)
    source_url = str(row.get("source_url") or url).strip()
    plain = f"{p1}\n\n{p2}\n\nИсточник: {source}\nОригинал: {url}"
    visible = f"{p1}\n\n{p2}\n\nИсточник: {source}\nОригинал"
    if not (550 <= len(visible) <= 900):
        raise ValueError(f"caption_visible_length:{len(visible)}")
    links = json.dumps({"source_label": source, "source_url": source_url, "original_url": url}, ensure_ascii=False, separators=(",", ":"))
    return plain, plain, links


def backfill_is_actionable(
    row: dict[str, Any],
    *,
    now: datetime | None = None,
    surface: str = "all",
) -> bool:
    if not notify.is_confirmed_publication(row) or current_editorial_draft(row):
        return False
    row_surface = social_post_surface(str(row.get("post_url") or ""))
    row_lane = content_lane(row)
    if row_lane == "article":
        row_surface = "article"
    if not row_surface or surface not in {"all", row_surface, row_lane}:
        return False
    status = str(row.get("publication_draft_backfill_status") or "").strip().lower()
    if (
        not str(row.get("publication_draft_prompt_version") or "")
        or str(row.get("publication_draft_backfill_version") or "") == DRAFT_BACKFILL_VERSION
    ) and status in TERMINAL_BACKFILL_STATUSES:
        return False
    retry_at = parse_time(row.get("publication_draft_backfill_next_attempt_after"))
    return retry_at is None or retry_at <= (now or utc_now())


def select_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    now: datetime | None = None,
    surface: str = "all",
) -> list[dict[str, Any]]:
    selected = [
        row for row in rows if backfill_is_actionable(row, now=now, surface=surface)
    ]
    selected.sort(key=lambda row: (
        str(row.get("sent_to_chat") or "").lower() == "true",
        int(row.get("publication_rank") or 999999),
        -float(row.get("publication_score") or row.get("publication_pre_score") or 0),
        notify.canonical_post_url(row),
    ))
    return selected[: max(0, int(limit))]


def draft_request_fingerprint(row: dict[str, Any], text: str, *, model: str) -> str:
    payload = {
        "version": DRAFT_BACKFILL_VERSION,
        "post_url": notify.canonical_post_url(row),
        "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "original_llm_decision": row.get("llm_decision") or row.get("publication_llm_decision"),
        "prompt_version": EDITORIAL_WRITER_VERSION,
        "model": model,
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def upsert_publication_row(pool: Any, ydb: Any, table: str, row: dict[str, Any], updates: dict[str, Any]) -> None:
    now_iso = utc_now().isoformat()
    pk = str(row.get("_ydb_pk") or "")
    if not pk:
        raise RuntimeError("publication row has no durable YDB primary key")
    payload = {key: value for key, value in row.items() if not str(key).startswith("_")}
    payload.update(updates)
    payload["updated_at"] = now_iso
    query_text = f"""
DECLARE $pk AS Utf8;
DECLARE $kind AS Utf8;
DECLARE $payload_json AS Json;
DECLARE $updated_at AS Utf8;
UPSERT INTO `{table}` (pk, kind, payload_json, updated_at)
VALUES ($pk, $kind, $payload_json, $updated_at);
"""

    def op(session: Any) -> None:
        query = session.prepare(query_text)
        session.transaction(ydb.SerializableReadWrite()).execute(
            query,
            {
                "$pk": pk,
                "$kind": "publication_candidate_item",
                "$payload_json": json.dumps(payload, ensure_ascii=False),
                "$updated_at": now_iso,
            },
            commit_tx=True,
        )

    pool.retry_operation_sync(op)


def build_client(transport: str) -> Any:
    auth_env = notify.TELETHON_TRANSPORT_AUTH_ENVS.get(transport)
    if not auth_env:
        raise RuntimeError(f"unsupported Region Talk Telethon transport: {transport}")
    bundle = notify.decode_discovery_bundle(str(os.getenv(auth_env) or ""))
    api_id = str(os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID") or "").strip()
    api_hash = str(os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH") or "").strip()
    if not api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID/API_HASH (or TG_ aliases) are required")
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
    except Exception as exc:
        raise RuntimeError("Telethon is required for Region Talk draft backfill") from exc
    return TelegramClient(
        StringSession(str(bundle["session"])),
        int(api_id),
        api_hash,
        request_retries=0,
        connection_retries=0,
        retry_delay=0,
        auto_reconnect=False,
        flood_sleep_threshold=0,
        raise_last_call_error=True,
        receive_updates=False,
        sequential_updates=True,
        device_model=str(bundle.get("device_model") or "Region Talk draft backfill"),
        system_version=str(bundle.get("system_version") or "Linux"),
        app_version=str(bundle.get("app_version") or "1.0"),
        lang_code=str(bundle.get("lang_code") or "ru"),
        system_lang_code=str(bundle.get("system_lang_code") or "ru"),
    )


async def fetch_exact_text(client: Any, row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    ref = telegram_post_ref(str(row.get("post_url") or ""))
    if ref is None:
        raise RuntimeError("unsupported Telegram post URL")
    handle, message_id = ref
    message = await client.get_messages(handle, ids=message_id)
    if message is None or int(getattr(message, "id", 0) or 0) != message_id:
        raise RuntimeError("exact Telegram message is unavailable")
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    if not text:
        raise RuntimeError("exact Telegram message has no text")
    date = getattr(message, "date", None)
    return text, {
        "handle": handle,
        "post_id": str(message_id),
        "post_date": date.isoformat() if date is not None else str(row.get("post_date") or ""),
    }


def fetch_vk_text(row: dict[str, Any], posts: dict[str, dict[str, Any]], error: str) -> tuple[str, dict[str, Any]]:
    ref = parse_vk_post(str(row.get("post_url") or ""))
    if ref is None:
        raise RuntimeError("unsupported VK post URL")
    owner_id, post_id = ref
    post = posts.get(f"{owner_id}_{post_id}") or {}
    if not post:
        raise RuntimeError(error or "exact VK post is unavailable")
    if int(post.get("owner_id") or 0) != owner_id or int(post.get("id") or 0) != post_id:
        raise RuntimeError("VK API returned a different post")
    text = str(post.get("text") or "").strip()
    if not text:
        raise RuntimeError("exact VK post has no text")
    timestamp = int(post.get("date") or 0)
    return text, {
        "platform": "vk",
        "post_id": str(post_id),
        "post_date": (
            datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            if timestamp
            else str(row.get("post_date") or "")
        ),
    }


def retry_updates(row: dict[str, Any], *, transport: str, reason: str) -> dict[str, Any]:
    attempts = int(row.get("publication_draft_backfill_attempt_count") or 0) + 1
    terminal = attempts >= 3
    return {
        "publication_draft_backfill_status": "source_text_unavailable" if terminal else "retry_due",
        "publication_draft_backfill_reason": reason[:500],
        "publication_draft_backfill_transport": transport,
        "publication_draft_backfill_attempt_count": attempts,
        "publication_draft_backfill_last_attempt_at": utc_now().isoformat(),
        "publication_draft_backfill_next_attempt_after": "" if terminal else (utc_now() + timedelta(hours=24)).isoformat(),
        "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
    }


async def collect_source_texts(
    rows: list[dict[str, Any]],
    *,
    transport: str,
    delay_min: float,
    delay_max: float,
) -> tuple[dict[str, tuple[str, dict[str, Any], str]], dict[str, str]]:
    """Fetch exact social text before releasing the Telegram session lease."""

    fetched: dict[str, tuple[str, dict[str, Any], str]] = {}
    errors: dict[str, str] = {}
    telegram_rows = [row for row in rows if social_post_surface(str(row.get("post_url") or "")) == "telegram"]
    if telegram_rows:
        with notify.discovery_session_lease(transport):
            client = build_client(transport)
            await client.connect()
            try:
                if not await client.is_user_authorized():
                    raise RuntimeError(
                        f"{notify.TELETHON_TRANSPORT_AUTH_ENVS[transport]} is not authorized"
                    )
                for index, row in enumerate(telegram_rows):
                    url = notify.canonical_post_url(row)
                    try:
                        text, fields = await fetch_exact_text(client, row)
                        fetched[url] = (text, fields, transport)
                    except Exception as exc:
                        errors[url] = f"{type(exc).__name__}: {str(exc)[:400]}"
                    if index + 1 < len(telegram_rows):
                        await asyncio.sleep(random.uniform(delay_min, delay_max))
            finally:
                await client.disconnect()

    vk_rows = [row for row in rows if social_post_surface(str(row.get("post_url") or "")) == "vk"]
    if vk_rows:
        post_ids = []
        for row in vk_rows:
            ref = parse_vk_post(str(row.get("post_url") or ""))
            if ref:
                post_ids.append(f"{ref[0]}_{ref[1]}")
        posts, vk_error = local_vk_posts(post_ids)
        for row in vk_rows:
            url = notify.canonical_post_url(row)
            try:
                text, fields = fetch_vk_text(row, posts, vk_error)
                fetched[url] = (text, fields, "vk_api")
            except Exception as exc:
                errors[url] = f"{type(exc).__name__}: {str(exc)[:400]}"
    return fetched, errors


def publication_media_plan(row: dict[str, Any]) -> dict[str, Any]:
    """Describe the exact media revision the notifier must materialize.

    Source attribution is the reuse policy.  This function is a product and
    integrity gate only: an association/presentation manifest is never called
    reviewable unless the renderer has an exact URL/path or source-post ref.
    """

    lane = content_lane(row)
    post_url = _canonical_url(row)
    media_kind = str(row.get("media_kind") or "").lower()
    selected_ids = [str(value) for value in _json_value(row.get("selected_media_ids"), []) if str(value)]
    explicit_items: list[dict[str, Any]] = []
    for field in (
        "selected_media_materialization_json", "media_materialization_items_json",
        "publication_media_items", "selected_media", "media_manifest_items",
    ):
        for raw in _json_value(row.get(field), []):
            if not isinstance(raw, dict):
                continue
            locator = raw.get("refetch_locator") if isinstance(raw.get("refetch_locator"), dict) else {}
            ref = str(
                raw.get("url") or raw.get("source_url") or raw.get("local_path")
                or raw.get("path") or raw.get("source_post_url") or raw.get("source_ref")
                or locator.get("source_url") or locator.get("post_url") or ""
            ).strip()
            if ref:
                item = {
                    "media_id": str(raw.get("media_id") or raw.get("id") or ref),
                    "ordinal": int(raw.get("ordinal") or len(explicit_items) + 1),
                    "kind": str(raw.get("kind") or raw.get("media_kind") or "image"),
                    "ref": ref,
                }
                for evidence_key in (
                    "reviewed_content_sha256",
                    "materialization_fingerprint",
                    "refetch_locator",
                ):
                    value = raw.get(evidence_key)
                    if value not in (None, "", {}, []):
                        item[evidence_key] = value
                explicit_items.append(item)
        if field == "selected_media_materialization_json" and explicit_items:
            break
    scalar_ref = str(
        row.get("publication_primary_image_url") or row.get("selected_image_url")
        or row.get("image_url_or_local_path") or row.get("associated_image_url") or ""
    ).strip()
    if scalar_ref and not explicit_items:
        explicit_items.append({"media_id": "hero:1", "ordinal": 1, "kind": "image", "ref": scalar_ref})
    explicit_items.sort(key=lambda item: int(item.get("ordinal") or 0))

    if lane == "article":
        items = explicit_items[:1]
        terminal_fallback = rt.is_external_link_article_candidate(row)
        if not items and terminal_fallback:
            mode = "link_preview_fallback"
            status, reason = "fallback", "article_source_media_terminally_unavailable"
        elif not items:
            mode = "article_hero"
            status, reason = "pending", "associated_article_hero_not_materialized"
        else:
            mode = "article_hero"
            status, reason = "ready", "associated_article_hero_has_exact_ref"
    elif media_kind == "video":
        mode = "social_video"
        items = explicit_items[:1] or ([{"media_id": "source:video", "ordinal": 1, "kind": "video", "ref": post_url}] if telegram_post_ref(post_url) else [])
        status, reason = ("ready", "exact_source_video_ref") if items else ("pending", "source_video_not_materialized")
    else:
        try:
            expected = int(row.get("expected_image_count") or 0)
        except (TypeError, ValueError):
            expected = 0
        photo_led = bool(explicit_items or selected_ids or expected or str(row.get("original_photo_evidence") or "").lower() == "true")
        album = max(len(explicit_items), len(selected_ids), expected) >= 3
        if photo_led and album:
            mode = "social_album"
            if explicit_items:
                items = explicit_items[:6]
            elif telegram_post_ref(post_url):
                items = [{
                    "media_id": value,
                    "ordinal": index,
                    "kind": "image",
                    "ref": post_url,
                } for index, value in enumerate(selected_ids[:6] or ["source:album"], 1)]
            else:
                items = []
            status, reason = ("ready", "ordered_source_album_ref") if len(items) >= 3 else ("pending", "ordered_album_3_to_6_not_materialized")
        elif photo_led:
            mode = "social_hero"
            items = explicit_items[:1] or ([{"media_id": selected_ids[0] if selected_ids else "source:hero", "ordinal": 1, "kind": "image", "ref": post_url}] if telegram_post_ref(post_url) else [])
            status, reason = ("ready", "exact_source_hero_ref") if items else ("pending", "source_hero_not_materialized")
        else:
            mode = "link_preview_fallback"
            items = []
            status, reason = "fallback", "no_usable_source_media"
    manifest = {
        "contract_version": MEDIA_MATERIALIZATION_CONTRACT_VERSION,
        "mode": mode,
        "status": status,
        "reason": reason,
        "items": items,
    }
    return manifest


def _stage_prompt(stage: str, payload: dict[str, Any]) -> str:
    common = {
        "product": "Region Talk / О Калининграде говорят",
        "language": "Russian only",
        "contract_version": EDITORIAL_WRITER_VERSION,
        "strict_grounding": "Every factual sentence must cite existing evidence_ids; never infer profession, origin, emotion or image details.",
    }
    if stage == "strategy":
        task = {
            "task": "Do not write public copy. Return one editorial strategy as JSON.",
            "output": {
                "status": "ready|insufficient_evidence|policy_conflict",
                "throughline_mode": "explicit_transition|contrast_or_scale_shift|fresh_start",
                "source_angle_id": "string",
                "why_this_material_now": "one Russian sentence",
                "opening_device": "Russian instruction",
                "used_history_urls": ["only URLs from channel_context, max 1"],
                "visual_hook_evidence_ids": ["only existing IDs"],
            },
            "rules": [
                "Use a history bridge only when genuinely supported; fresh_start beats a forced transition.",
                "Paragraph 1 must eventually establish the external source/author and why this outside optic matters, not give a standard biography.",
                "Paragraph 2 must eventually sell the click through 1-2 specific details without exhausting the original.",
            ],
        }
    elif stage == "writer":
        task = {
            "task": "Write exactly two editorial paragraphs and a sentence-level grounding map as JSON.",
            "output": {
                "status": "draft_ready|insufficient_evidence|policy_conflict",
                "public_copy": {"paragraph_1": "150-500 chars", "paragraph_2": "150-500 chars"},
                "grounding_map": [{
                    "sentence_index": 1,
                    "sentence_text": "exact sentence",
                    "claim_type": "source_profile_fact|content_fact|source_impression|visual_observation|history_bridge",
                    "evidence_ids": ["existing ID"],
                    "third_person_maintained": True,
                }],
            },
            "rules": [
                "Paragraph 1: source/author, proven non-regional optic and optional honest bridge.",
                "Paragraph 2: 1-2 concrete observations from the material, strictly in third person, and a real reason to open the original.",
                "Do not use first-person plural for another author's experience.",
                "Warm observational editorial tone; no clickbait, PR jargon, dossier or exhaustive summary.",
                "The two paragraphs together must leave room for attribution and URL in a 550-900 character media caption.",
                "Mention photos/video only when visual_hook_evidence_ids is non-empty. Source media is intentionally reused with explicit attribution.",
            ],
        }
    else:
        task = {
            "task": "Critique the draft against evidence and strategy. Return JSON only.",
            "output": {"status": "pass|rewrite|reject", "reason_codes": ["string"], "feedback": "concise Russian rewrite instruction"},
            "hard_fails": ["unsupported_claim", "voice_violation", "visual_hallucination", "forced_history_bridge", "wrong_language", "not_exactly_two_paragraphs"],
            "pass_only_if": "Both paragraphs are grounded, distinct, editorial, specific and motivate opening the original without replacing it.",
        }
    return json.dumps({**common, **task, "input": payload}, ensure_ascii=False)


def call_editorial_stage(
    *,
    stage: str,
    payload: dict[str, Any],
    request_fingerprint: str,
    model: str,
    default_env: str,
    budget: DurableGeminiBudget,
) -> tuple[dict[str, Any], bool]:
    stage_fingerprint = hashlib.sha256(f"{request_fingerprint}|{stage}".encode("utf-8")).hexdigest()
    reservation = budget.reserve(stage_fingerprint)
    status = str(reservation.get("status") or "")
    if status == "replay":
        return dict(reservation.get("result") or {}), False
    if status in {"busy", "exhausted"}:
        return {"_stage_status": "rate_limited", "reason": "durable_budget_" + status}, False

    prompt = _stage_prompt(stage, payload)
    try:
        client = rt.get_region_talk_llm_gateway(default_env)

        async def invoke() -> tuple[str, Any]:
            return await client.generate_content_async(
                model=model,
                prompt=prompt,
                generation_config={"temperature": 0.2 if stage == "writer" else 0.1, "response_mime_type": "application/json"},
                max_output_tokens=1800,
            )

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            raw, _usage = executor.submit(lambda: asyncio.run(invoke())).result(timeout=75)
        result = rt.parse_llm_json(raw)
        result.update({
            "_stage_status": "ok",
            "_stage": stage,
            "_model": model,
            "_request_fingerprint": stage_fingerprint,
            "_prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "_usage_input_tokens": getattr(_usage, "input_tokens", ""),
            "_usage_output_tokens": getattr(_usage, "output_tokens", ""),
            "_usage_total_tokens": getattr(_usage, "total_tokens", ""),
        })
    except Exception as exc:
        result = {
            "_stage_status": "error", "_stage": stage, "_model": model,
            "_request_fingerprint": stage_fingerprint,
            "_prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "reason": f"{type(exc).__name__}: {str(exc)[:300]}",
        }
    budget.complete(stage_fingerprint, result)
    return result, True


def generate_editorial_draft(
    row: dict[str, Any],
    *,
    evidence_pack: dict[str, Any],
    history: list[dict[str, Any]],
    model: str,
    default_env: str,
    budget: DurableGeminiBudget,
) -> tuple[dict[str, Any], int]:
    evidence_raw = json.dumps(evidence_pack, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    history_raw = json.dumps(history[:5], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    evidence_hash = hashlib.sha256(evidence_raw.encode("utf-8")).hexdigest()
    strategy_input = {**evidence_pack, "channel_context": history[:5]}
    request_fp = hashlib.sha256(
        json.dumps({
            "version": EDITORIAL_WRITER_VERSION,
            "candidate": evidence_pack.get("candidate"),
            "evidence_hash": evidence_hash,
            "history_hash": hashlib.sha256(history_raw.encode("utf-8")).hexdigest(),
            "model": model,
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    calls = 0
    strategy, called = call_editorial_stage(
        stage="strategy", payload=strategy_input, request_fingerprint=request_fp,
        model=model, default_env=default_env, budget=budget,
    )
    calls += int(called)
    if strategy.get("_stage_status") != "ok" or strategy.get("status") != "ready":
        deferred = strategy.get("_stage_status") != "ok"
        return ({
            "publication_draft_backfill_status": "retry_due" if deferred else "needs_grounding_review",
            "publication_draft_backfill_reason": str(strategy.get("reason") or strategy.get("status") or "strategy_not_ready")[:500],
            "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat() if deferred else "",
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
        }, calls)

    evidence_ids = {str(item.get("evidence_id")) for item in evidence_pack.get("evidence") or [] if isinstance(item, dict)}
    used_history = set(str(value) for value in (strategy.get("used_history_urls") or []))
    allowed_history = {str(item.get("candidate_url") or "") for item in history}
    if (
        not used_history.issubset(allowed_history)
        or len(used_history) > 1
        or recent_history_requires_fresh_start(history)
    ):
        strategy["throughline_mode"] = "fresh_start"
        strategy["used_history_urls"] = []

    writer_payload = {"editorial_plan": strategy, **evidence_pack}
    writer, called = call_editorial_stage(
        stage="writer", payload=writer_payload, request_fingerprint=request_fp + "|writer1",
        model=model, default_env=default_env, budget=budget,
    )
    calls += int(called)
    if writer.get("_stage_status") != "ok":
        return ({
            "publication_draft_backfill_status": "retry_due",
            "publication_draft_backfill_reason": str(writer.get("reason") or "writer_stage_deferred")[:500],
            "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
        }, calls)
    violations = validate_editorial_output(writer, evidence_ids)
    attempts = 1
    if violations:
        retry_payload = {**writer_payload, "previous_draft": writer, "deterministic_feedback": violations}
        writer, called = call_editorial_stage(
            stage="writer", payload=retry_payload, request_fingerprint=request_fp + "|writer2",
            model=model, default_env=default_env, budget=budget,
        )
        calls += int(called)
        attempts = 2
        if writer.get("_stage_status") != "ok":
            return ({
                "publication_draft_backfill_status": "retry_due",
                "publication_draft_backfill_reason": str(writer.get("reason") or "writer_retry_deferred")[:500],
                "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
                "publication_draft_input_fingerprint": request_fp,
                "publication_draft_evidence_hash": evidence_hash,
                "publication_draft_evidence_json": evidence_raw,
                "publication_draft_history_json": history_raw,
            }, calls)
        violations = validate_editorial_output(writer, evidence_ids) if writer.get("_stage_status") == "ok" else ["writer_retry_failed"]
    if violations or writer.get("status") != "draft_ready":
        return ({
            "publication_draft_backfill_status": "needs_grounding_review",
            "publication_draft_backfill_reason": ",".join(violations or [str(writer.get("status") or "writer_not_ready")])[:500],
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_generation_attempts": attempts,
        }, calls)

    critic_payload = {"editorial_plan": strategy, "draft": writer, **evidence_pack}
    critic, called = call_editorial_stage(
        stage="critic", payload=critic_payload, request_fingerprint=request_fp + "|critic1",
        model=model, default_env=default_env, budget=budget,
    )
    calls += int(called)
    if critic.get("_stage_status") != "ok":
        return ({
            "publication_draft_backfill_status": "retry_due",
            "publication_draft_backfill_reason": str(critic.get("reason") or "critic_stage_deferred")[:500],
            "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
            "publication_draft_critic_json": json.dumps(critic, ensure_ascii=False, separators=(",", ":")),
        }, calls)
    if critic.get("_stage_status") == "ok" and critic.get("status") == "rewrite" and attempts < 2:
        writer, called = call_editorial_stage(
            stage="writer", payload={**writer_payload, "previous_draft": writer, "critic_feedback": critic},
            request_fingerprint=request_fp + "|writer2critic", model=model, default_env=default_env, budget=budget,
        )
        calls += int(called)
        attempts = 2
        if writer.get("_stage_status") != "ok":
            return ({
                "publication_draft_backfill_status": "retry_due",
                "publication_draft_backfill_reason": str(writer.get("reason") or "critic_rewrite_deferred")[:500],
                "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
                "publication_draft_input_fingerprint": request_fp,
                "publication_draft_evidence_hash": evidence_hash,
                "publication_draft_evidence_json": evidence_raw,
                "publication_draft_history_json": history_raw,
            }, calls)
        violations = validate_editorial_output(writer, evidence_ids)
        if not violations:
            critic_payload["draft"] = writer
            critic, called = call_editorial_stage(
                stage="critic", payload=critic_payload, request_fingerprint=request_fp + "|critic2",
                model=model, default_env=default_env, budget=budget,
            )
            calls += int(called)
            if critic.get("_stage_status") != "ok":
                return ({
                    "publication_draft_backfill_status": "retry_due",
                    "publication_draft_backfill_reason": str(critic.get("reason") or "critic_retry_deferred")[:500],
                    "publication_draft_backfill_next_attempt_after": (utc_now() + timedelta(hours=1)).isoformat(),
                    "publication_draft_input_fingerprint": request_fp,
                    "publication_draft_evidence_hash": evidence_hash,
                    "publication_draft_evidence_json": evidence_raw,
                    "publication_draft_history_json": history_raw,
                }, calls)
    if critic.get("_stage_status") != "ok" or critic.get("status") != "pass" or validate_editorial_output(writer, evidence_ids):
        return ({
            "publication_draft_backfill_status": "needs_grounding_review",
            "publication_draft_backfill_reason": str(critic.get("reason_codes") or critic.get("reason") or "critic_not_passed")[:500],
            "publication_draft_input_fingerprint": request_fp,
            "publication_draft_evidence_hash": evidence_hash,
            "publication_draft_evidence_json": evidence_raw,
            "publication_draft_history_json": history_raw,
            "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
            "publication_draft_critic_json": json.dumps(critic, ensure_ascii=False, separators=(",", ":")),
            "publication_draft_generation_attempts": attempts,
        }, calls)

    try:
        telegram_text, vk_text, link_meta = render_public_copy(row, writer)
    except ValueError as exc:
        return ({
            "publication_draft_backfill_status": "needs_grounding_review",
            "publication_draft_backfill_reason": str(exc),
            "publication_draft_input_fingerprint": request_fp,
        }, calls)
    media = publication_media_plan(row)
    media_reviewable = media["status"] in {"ready", "fallback"}
    title = str(row.get("publication_draft_title") or row.get("publication_title") or row.get("short_summary") or _source_name(row))[:140]
    fact_points = [{
        "claim": str(item.get("sentence_text") or "")[:500],
        "evidence_ids": item.get("evidence_ids") or [],
    } for item in writer.get("grounding_map") or [] if isinstance(item, dict) and item.get("sentence_text")]
    return ({
        "publication_draft_status": "ready_for_operator_review" if media_reviewable else "media_materialization_pending",
        "publication_draft_title": title,
        "publication_draft_source_attribution": _source_name(row),
        "publication_draft_telegram_text": telegram_text,
        "publication_draft_vk_text": vk_text,
        "publication_draft_fact_points_json": json.dumps(fact_points, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_prompt_version": EDITORIAL_WRITER_VERSION,
        "publication_draft_contract_version": EDITORIAL_OUTPUT_CONTRACT,
        "publication_draft_input_fingerprint": request_fp,
        "publication_draft_evidence_hash": evidence_hash,
        "publication_draft_evidence_json": evidence_raw,
        "publication_draft_history_json": history_raw,
        "publication_draft_editorial_plan_json": json.dumps(strategy, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_grounding_map_json": json.dumps(writer.get("grounding_map") or [], ensure_ascii=False, separators=(",", ":")),
        "publication_draft_critic_json": json.dumps(critic, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_stage_audit_json": json.dumps({"strategy": strategy, "writer": writer, "critic": critic}, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_generation_attempts": attempts,
        "publication_draft_link_metadata_json": link_meta,
        "publication_presentation_mode": media["mode"],
        "publication_media_materialization_status": media["status"],
        "publication_media_materialization_reason": media["reason"],
        "publication_media_materialization_contract_version": MEDIA_MATERIALIZATION_CONTRACT_VERSION,
        "publication_presentation_manifest_json": json.dumps(media, ensure_ascii=False, separators=(",", ":")),
        "publication_draft_backfill_status": "ready" if media_reviewable else "media_materialization_pending",
        "publication_draft_backfill_reason": "critic_passed" if media_reviewable else media["reason"],
        "publication_draft_backfill_next_attempt_after": "" if media_reviewable else (utc_now() + timedelta(hours=1)).isoformat(),
    }, calls)


def build_draft_updates(
    row: dict[str, Any],
    *,
    text: str,
    fetched: dict[str, Any],
    source_transport: str,
    intake: dict[str, Any] | None,
    history: list[dict[str, Any]],
    model: str,
    default_env: str,
    budget: DurableGeminiBudget,
) -> tuple[dict[str, Any], bool]:
    evidence_pack = build_editorial_evidence(
        row, source_text=text, fetched=fetched, intake=intake,
    )
    verdict, provider_calls = generate_editorial_draft(
        row,
        evidence_pack=evidence_pack,
        history=history,
        model=model,
        default_env=default_env,
        budget=budget,
    )
    fingerprint = str(verdict.get("publication_draft_input_fingerprint") or draft_request_fingerprint(row, text, model=model))
    base_updates = {
        "publication_draft_backfill_transport": source_transport,
        "publication_draft_backfill_attempt_count": int(row.get("publication_draft_backfill_attempt_count") or 0) + 1,
        "publication_draft_backfill_last_attempt_at": utc_now().isoformat(),
        "publication_draft_backfill_text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "publication_draft_backfill_request_fingerprint": fingerprint,
        "publication_draft_backfill_version": DRAFT_BACKFILL_VERSION,
        "publication_draft_backfill_provider_called": str(provider_calls > 0).lower(),
        "publication_draft_backfill_provider_call_count": provider_calls,
        # The writer is a copy stage, not a second publication verdict.
        "publication_draft_backfill_llm_gate_status": "ok" if verdict.get("publication_draft_backfill_status") in {"ready", "needs_grounding_review", "media_materialization_pending"} else "deferred",
    }
    if (
        str(row.get("publication_draft_prompt_version") or "") != EDITORIAL_WRITER_VERSION
        and str(row.get("legacy_review_migration_version") or "") != LEGACY_REVIEW_MIGRATION_VERSION
    ):
        # One-time audit projection requested by the product owner.  It makes
        # every historical candidate eligible for a rewrite, but is never an
        # approval of the new exact copy+media fingerprint.
        base_updates.update({
            "legacy_review_migration_version": LEGACY_REVIEW_MIGRATION_VERSION,
            "legacy_review_migrated_at": utc_now().isoformat(),
            "legacy_principle_status": "approved",
            "legacy_copy_status": "rewrite_requested",
            "legacy_operator_review_fingerprint": str(row.get("operator_review_fingerprint") or ""),
            "legacy_operator_review_decision": str(row.get("operator_review_decision") or ""),
            "legacy_operator_review_rewrite_status": str(row.get("operator_review_rewrite_status") or ""),
            "operator_review_fingerprint": "",
            "operator_review_decision": "pending",
            "operator_review_rewrite_status": "clean",
            "operator_review_positive": False,
            "operator_review_negative": False,
            "operator_review_rewrite_requested": False,
        })
    return ({**base_updates, **verdict}, provider_calls > 0)


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    ydb = driver = pool = table = None
    selected: list[dict[str, Any]] = []
    try:
        ydb, driver, pool, table, rows = notify.read_publication_rows(int(args.scan_limit))
        external_intakes = read_kind_rows(pool, ydb, table, "external_publication_intake_item", int(args.scan_limit))
        schedules = read_kind_rows(pool, ydb, table, "publication_schedule_item", int(args.history_limit))
        logs = read_kind_rows(pool, ydb, table, "publication_log_item", int(args.history_limit))
        logs += read_kind_rows(pool, ydb, table, "region_talk_publication_log", int(args.history_limit))
        history = publication_history([*schedules, *logs, *rows], limit=5)
        intakes = article_intake_index(external_intakes)
        selected = select_rows(rows, limit=int(args.limit), surface=str(args.surface))
        if args.dry_run or not selected:
            return {
                "ok": True,
                "stage": "publication_draft_backfill",
                "dry_run": bool(args.dry_run),
                "selected": [notify.canonical_post_url(row) for row in selected],
                "selected_total": len(selected),
                "ready_total": 0,
                "failed_total": 0,
                "transport": str(args.transport),
            }

        model = str(args.model or os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite")
        default_env = str(
            args.default_env_var_name
            or os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME")
            or "GOOGLE_API_KEY3"
        )
        budget = DurableGeminiBudget(
            pool,
            ydb,
            table,
            budget_id=str(args.llm_budget_id),
            budget_max=int(args.llm_budget_max),
            owner_prefix="region-talk-draft-backfill",
        )
        results: list[dict[str, Any]] = []
        social_selected = [row for row in selected if content_lane(row) == "social"]
        fetched_by_url, fetch_errors = await collect_source_texts(
            social_selected,
            transport=str(args.transport),
            delay_min=float(args.delay_min),
            delay_max=float(args.delay_max),
        )
        for index, row in enumerate(selected):
            url = notify.canonical_post_url(row)
            fetched_item = fetched_by_url.get(url)
            intake = intakes.get(str(row.get("external_publication_id") or "")) or intakes.get(url)
            if content_lane(row) == "article":
                if not intake:
                    updates = retry_updates(row, transport="retained_article_intake", reason="retained article evidence unavailable")
                    upsert_publication_row(pool, ydb, table, row, updates)
                    results.append({"post_url": url, "status": updates["publication_draft_backfill_status"]})
                    continue
                text, fetched, source_transport = "", {}, "retained_article_intake"
            elif fetched_item is None:
                source_transport = "vk_api" if social_post_surface(url) == "vk" else str(args.transport)
                updates = retry_updates(
                    row,
                    transport=source_transport,
                    reason=fetch_errors.get(url) or "exact source text unavailable",
                )
                upsert_publication_row(pool, ydb, table, row, updates)
                results.append({"post_url": url, "status": updates["publication_draft_backfill_status"]})
                continue
            text, fetched, source_transport = fetched_item
            updates, provider_called = build_draft_updates(
                row,
                text=text,
                fetched=fetched,
                source_transport=source_transport,
                intake=intake,
                history=history,
                model=model,
                default_env=default_env,
                budget=budget,
            )
            upsert_publication_row(pool, ydb, table, row, updates)
            results.append({
                "post_url": url,
                "status": updates["publication_draft_backfill_status"],
                "provider_called": provider_called,
            })
            if index + 1 < len(selected):
                await asyncio.sleep(random.uniform(float(args.delay_min), float(args.delay_max)))
        return {
            "ok": True,
            "stage": "publication_draft_backfill",
            "dry_run": False,
            "selected_total": len(selected),
            "ready_total": sum(1 for item in results if item["status"] == "ready"),
            "failed_total": sum(1 for item in results if item["status"] != "ready"),
            "transport": str(args.transport),
            "surface": str(args.surface),
            "llm_budget_id": str(args.llm_budget_id),
            "results": results,
        }
    finally:
        if driver is not None:
            driver.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill grounded Region Talk social drafts")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--transport", choices=tuple(notify.TELETHON_TRANSPORT_AUTH_ENVS), default=None)
    parser.add_argument("--surface", choices=("all", "telegram", "vk", "article", "social"), default="all")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--history-limit", type=int, default=5000)
    parser.add_argument("--model", default="")
    parser.add_argument("--default-env-var-name", default="")
    parser.add_argument("--llm-budget-id", default="")
    parser.add_argument("--llm-budget-max", type=int, default=20)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=5.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    notify.load_env(args.env_file)
    args.transport = args.transport or os.getenv("REGION_TALK_DRAFT_BACKFILL_TRANSPORT") or "telethon_discovery2"
    if args.transport not in notify.TELETHON_TRANSPORT_AUTH_ENVS:
        raise RuntimeError(f"unsupported REGION_TALK_DRAFT_BACKFILL_TRANSPORT: {args.transport}")
    args.limit = max(0, min(10, int(args.limit)))
    args.llm_budget_id = args.llm_budget_id or os.getenv("REGION_TALK_DRAFT_BACKFILL_BUDGET_ID") or utc_now().strftime("region-talk-draft-backfill-%Y%m%d")
    payload = asyncio.run(execute(args))
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
