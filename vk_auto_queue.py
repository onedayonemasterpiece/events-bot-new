from __future__ import annotations

import asyncio
import logging
import math
import os
import time
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence, Literal

from admin_chat import resolve_superadmin_chat_id
from db import Database
from heavy_ops import current_heavy_meta, describe_heavy_meta, heavy_operation
from ops_run import finish_ops_run, start_ops_run

import vk_intake
import vk_review
from smart_event_update import SmartUpdateTerminalOutcome
from smart_update_identity import canonicalize_identity_url
from source_parse_contract import (
    decision_from_provider_payload,
    EvidenceManifest,
    PARSE_VERSION,
    SourceDisposition,
    SourceNoEventReason,
    SourceParseDecision,
    SourceParseRetryReason,
)

logger = logging.getLogger(__name__)

_vk_auto_import_cancel_requests: set[tuple[int, int]] = set()


def _adapt_vk_draft_result(
    value: Any,
    *,
    source_text: str,
    attachment_count: int = 0,
) -> vk_intake.DraftParseResult:
    """Put legacy/mock VK draft results through the one validated adapter.

    The production builder already returns ``DraftParseResult``.  This boundary
    exists for rolling compatibility and tests; in particular a bare empty
    list, ``None`` or malformed object is uncertainty, never no-event proof.
    """

    if isinstance(value, vk_intake.DraftParseResult) and isinstance(
        getattr(value, "decision", None), SourceParseDecision
    ):
        return value

    manifest = EvidenceManifest.complete_source(
        source_text or "", attachment_count=max(0, int(attachment_count or 0))
    )
    legacy_drafts = list(value) if isinstance(value, (list, tuple)) else []
    provider_payload: Any
    if isinstance(value, (list, tuple)):
        provider_payload = [
            {
                key: field_value
                for key, field_value in vars(item).items()
                if key != "poster_media"
            }
            if isinstance(item, vk_intake.EventDraft)
            else item
            for item in value
        ]
    else:
        provider_payload = value
    decision = decision_from_provider_payload(
        provider_payload,
        evidence_manifest=manifest,
    )
    if decision.disposition is SourceDisposition.RETRY_REQUIRED:
        logger.warning(
            "vk_auto: untyped/invalid draft result requires reparse/retry "
            "payload_type=%s retry_reason=%s",
            type(value).__name__,
            getattr(decision.retry_reason, "value", decision.retry_reason),
        )
        legacy_drafts = []
    return vk_intake.DraftParseResult(legacy_drafts, decision=decision)


def _vk_auto_parse_gemma_model() -> str:
    """Model override for VK auto-import draft extraction only."""
    value = (os.getenv("VK_AUTO_IMPORT_PARSE_GEMMA_MODEL") or "").strip()
    return value or "models/gemma-4-31b-it"


async def _record_vk_auto_import_scheduler_skip(
    db: Database,
    *,
    ops_run_id: int | None = None,
    run_id: str | None,
    reason: str,
) -> None:
    details = {
        "run_id": run_id,
        "skip_reason": str(reason or "").strip() or "unknown",
        "scheduler_entrypoint": "vk_auto_import",
    }
    if not ops_run_id:
        ops_run_id = await start_ops_run(
            db,
            kind="vk_auto_import",
            trigger="scheduled",
            operator_id=0,
            details=details,
        )
    await finish_ops_run(
        db,
        run_id=ops_run_id,
        status="skipped",
        details=details,
    )

def _timings_enabled() -> bool:
    raw = (os.getenv("PIPELINE_TIMINGS") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _vk_auto_import_heavy_mode(trigger: str) -> str:
    raw = (os.getenv("VK_AUTO_IMPORT_HEAVY_MODE") or "").strip().lower()
    if not raw:
        return "off" if str(trigger or "").strip().lower() == "manual" else "wait"
    if raw in {"off", "none", "disabled", "0", "false", "no"}:
        return "off"
    if raw in {"try", "skip", "nonblocking", "non-blocking"}:
        return "try"
    return "wait"


class _NoopHeavyOperation:
    async def __aenter__(self) -> bool:
        return True

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


_VK_CANCEL_RE = re.compile(
    r"(?i)\b("
    r"отмен\w*|"
    r"не\s+состо\w*|"
    r"перенос\w*|"
    # Avoid false positives like "иллюстрации перенесут вас..." (transport you),
    # while still catching reschedule notices: "перенесено/перенесён/перенесли/перенесём".
    r"перенес(?:ен(?:а|о)?|ена|ено|ены|ён(?:а|о)?|ёна|ёно|ёны|ли|ем|ём)\b|"
    r"сдвинул\w*\s+срок\w*|"
    r"отложен\w*|"
    r"показ\s+не\s+состо\w*"
    r")\b"
)

_RU_MONTHS_GENITIVE_LOCAL: dict[str, int] = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}

_VK_TIME_RESCHEDULE_RE = re.compile(
    r"(?iu)\b(?:время\s+начала|начал[оа]?|старт)\b[^.!?\n]{0,100}"
    r"\b(?:перенос\w*|перенес(?:ен(?:а|о)?|ена|ено|ены|ён(?:а|о)?|ёна|ёно|ёны|ли|ем|ём))\b"
    r"[^.!?\n]{0,80}\b(?:на|к)\s+\d{1,2}[:.]\d{2}\b"
)

_VK_RETROSPECTIVE_RESCHEDULE_RE = re.compile(
    r"(?iu)\b(?:это|эта|данн\w+|наш\w+)?\s*"
    r"(?:лекци\w+|встреч\w+|мероприяти\w+|событи\w+)?\s*"
    r"(?:[-—:]\s*)?перенос\w*\b[^.!?\n]{0,120}"
    r"\b(?:несостоявш\w+|ранее\s+отмен[её]нн\w+|прошл\w+|апрельск\w+|мартовск\w+|февральск\w+)\b"
    r"[^.!?\n]{0,120}\b(?:встреч\w+|лекци\w+|мероприяти\w+|событи\w+)\b"
)


def _looks_like_retrospective_reschedule_context(text: str | None) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    # A post for a real new event can mention that this is a replacement for
    # an old missed meeting. That is context for extraction, not a signal to
    # mark another event as postponed.
    if re.search(
        r"(?iu)\b(?:отменяется|отмен[её]н[аоы]?|не\s+состо(?:ится|ит)|отложен[аоы]?)\b",
        raw,
    ):
        return False
    return bool(_VK_RETROSPECTIVE_RESCHEDULE_RE.search(raw))


def _looks_like_time_reschedule_notice(text: str | None) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    return bool(_VK_TIME_RESCHEDULE_RE.search(raw))


def _parse_ru_date_from_text(text: str, *, year_hint: int | None) -> str | None:
    raw = (text or "").strip()
    if not raw:
        return None
    m = re.search(r"\b(\d{1,2})\.(\d{1,2})(?:\.(20\d{2}))?\b", raw)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        year = int(m.group(3)) if (m.group(3) or "").strip() else year_hint
        if year:
            try:
                return datetime(year, month, day).date().isoformat()
            except Exception:
                # A time like "19.30" can look like dd.mm. Keep scanning for
                # Russian month dates such as "8 мая" instead of failing early.
                pass
    m = re.search(
        r"(?i)\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b",
        raw,
    )
    if not m:
        return None
    day = int(m.group(1))
    month_word = (m.group(2) or "").casefold()
    month = int(_RU_MONTHS_GENITIVE_LOCAL.get(month_word) or 0)
    if not month:
        return None
    year = year_hint
    if not year:
        return None
    try:
        return datetime(year, month, day).date().isoformat()
    except Exception:
        return None


def _extract_title_hint(text: str | None) -> str | None:
    raw = (text or "").strip()
    if not raw:
        return None
    # Common pattern: "кинофестиваля <Title>"
    m = re.search(
        r"(?i)\b(?:кинофестивал\w*|фестивал\w*|мероприят\w*|показ)\s+([A-Za-zА-Яа-яЁё][^\\n\\r.,!?:;]{3,80})",
        raw,
    )
    if m:
        value = (m.group(1) or "").strip().strip("«»\"'()[]")
        value = re.sub(r"\s+", " ", value).strip()
        if 4 <= len(value) <= 90:
            return value
    # Otherwise: prefer 2+ capitalized latin words (e.g. "Manhattan Short Online")
    candidates = re.findall(
        r"\b[A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){1,4}\b",
        raw,
    )
    if candidates:
        # Prefer the longest phrase (more specific).
        best = max(candidates, key=lambda s: len(s))
        best = re.sub(r"\s+", " ", best).strip()
        if 4 <= len(best) <= 80:
            return best
    return None


def _title_tokens(title: str | None) -> set[str]:
    if not title:
        return set()
    words = re.findall(r"[a-zа-яё0-9]{4,}", title.lower(), flags=re.IGNORECASE)
    return {w for w in words if w and not w.isdigit()}


async def _cancel_matching_event_from_notice(
    db: Database,
    *,
    notice_text: str,
    source_url: str,
    source_name: str | None,
    location_hint: str | None,
    published_at: datetime | None,
    lifecycle_action: Any | None = None,
) -> tuple[int | None, str | None]:
    """Try to find a matching event and mark it as cancelled/postponed (inactive)."""
    from sqlalchemy import select
    from models import Event, EventSource, EventSourceFact
    from smart_event_update import (
        EventCandidate,
        SmartUpdateIntent,
        smart_event_update,
    )
    import main as main_mod

    year_hint = None
    if published_at is not None:
        try:
            year_hint = int(published_at.astimezone(timezone.utc).year)
        except Exception:
            year_hint = None
    if year_hint is None:
        year_hint = datetime.now(timezone.utc).year

    date_hint = (
        str(getattr(lifecycle_action, "target_date", "") or "").strip()
        or _parse_ru_date_from_text(notice_text, year_hint=year_hint)
    )
    time_hint = None
    m_time = re.search(r"\b(\d{1,2})[:.](\d{2})\b", notice_text or "")
    if m_time:
        try:
            hh = int(m_time.group(1))
            mm = int(m_time.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                time_hint = f"{hh:02d}:{mm:02d}"
        except Exception:
            time_hint = None
    title_hint = (
        str(getattr(lifecycle_action, "target_title", "") or "").strip()
        or _extract_title_hint(notice_text)
    )
    time_hint = (
        str(getattr(lifecycle_action, "target_time", "") or "").strip()
        or time_hint
    )
    location_hint = (
        str(getattr(lifecycle_action, "target_location", "") or "").strip()
        or location_hint
    )
    if not date_hint and not title_hint:
        return None, "insufficient_anchors:no_date_no_title"
    action_name = str(getattr(getattr(lifecycle_action, "action", None), "value", "") or "")
    is_postponed = action_name == "POSTPONE" or bool(
        re.search(
            r"(?i)\b(?:перенос\w*|перенес(?:ен(?:а|о)?|ена|ено|ены|ён(?:а|о)?|ёна|ёно|ёны|ли|ем|ём)|сдвинул\w*)\b",
            notice_text or "",
        )
    )
    kind = "перенос" if is_postponed else "отмена"
    next_status = "postponed" if is_postponed else "cancelled"

    async with db.get_session() as session:
        stmt = select(Event).where(Event.lifecycle_status == "active")
        if date_hint:
            stmt = stmt.where(Event.date.like(f"{date_hint}%"))
        res = await session.execute(stmt)
        events = list(res.scalars().all())

        if not events:
            return None, f"no_events_for_date={date_hint or 'any'}"

        loc_norm = (location_hint or "").strip().casefold()
        title_tokens = _title_tokens(title_hint)

        scored: list[tuple[int, Event]] = []
        for ev in events:
            score = 0
            if time_hint and (ev.time or "").strip() == time_hint:
                score += 2
            if loc_norm:
                ev_loc = (ev.location_name or "").strip().casefold()
                if ev_loc == loc_norm:
                    score += 4
                elif loc_norm and (loc_norm in ev_loc or ev_loc in loc_norm):
                    score += 2
            if title_hint:
                ev_title = (ev.title or "").strip()
                if title_hint.casefold() in ev_title.casefold() or ev_title.casefold() in title_hint.casefold():
                    score += 5
                ev_tokens = _title_tokens(ev_title)
                overlap = len(title_tokens & ev_tokens) if title_tokens else 0
                score += min(6, overlap * 2)
            scored.append((score, ev))

        scored.sort(key=lambda x: (x[0], int(getattr(x[1], "id", 0) or 0)), reverse=True)
        best_score, best = scored[0]
        # Guardrail: require at least some matching signal.
        if best_score < 4 and title_hint:
            return None, f"low_confidence score={best_score} title_hint={title_hint!r} date={date_hint or ''}"
        if best_score < 2 and not title_hint:
            return None, f"low_confidence score={best_score} date={date_hint or ''}"

        canonical_source_url = canonicalize_identity_url(str(source_url))
        if not canonical_source_url:
            return None, "invalid_source_identity"
        best_id = int(best.id)
        if action_name == "CANCEL" or not action_name:
            best.lifecycle_status = "cancelled"
        elif action_name == "POSTPONE":
            best.lifecycle_status = "postponed"
        elif action_name == "RESCHEDULE_DATE":
            new_date = str(getattr(lifecycle_action, "new_date", "") or "").strip()
            if not new_date:
                return None, "typed_action_missing_new_date"
            best.date = new_date
            best.lifecycle_status = "active"
        elif action_name == "RESCHEDULE_TIME":
            new_time = str(getattr(lifecycle_action, "new_time", "") or "").strip()
            if not new_time:
                return None, "typed_action_missing_new_time"
            best.time = new_time
            best.lifecycle_status = "active"
        elif action_name == "UPDATE_DETAILS":
            best.lifecycle_status = best.lifecycle_status or "active"
        session.add(best)
        await session.commit()

        context_result = await smart_event_update(
            db,
            EventCandidate(
                intent=SmartUpdateIntent.ATTACH_CONTEXT,
                target_event_id=best_id,
                source_type="vk_lifecycle",
                source_url=str(source_url),
                source_text=(notice_text or "")[:4000],
                occurrence_key=f"lifecycle:{action_name or 'CANCEL'}:{best_id}:{canonical_source_url}",
            ),
            check_source_url=False,
            schedule_tasks=False,
        )
        note = f"❌ {kind}: событие помечено как {next_status} по источнику VK"
        if source_name:
            note += f" ({source_name})"
        if context_result.is_accepted:
            src = (
                (
                    await session.execute(
                        select(EventSource).where(
                            EventSource.event_id == best_id,
                            EventSource.canonical_source_url == canonical_source_url,
                        )
                    )
                )
                .scalars()
                .first()
            )
            if src is not None:
                session.add(
                    EventSourceFact(
                        event_id=best_id,
                        source_id=int(src.id),
                        fact=note,
                        status="note",
                    )
                )
                await session.commit()
        elif context_result.is_retry:
            logger.warning(
                "vk_auto.cancel context retry_scheduled event_id=%s reason=%s",
                best_id,
                context_result.reason,
            )

        try:
            async with db.get_session() as reload_session:
                updated_event = await reload_session.get(Event, best_id)
            if updated_event is not None:
                await main_mod.schedule_event_update_tasks(
                    db,
                    updated_event,
                    skip_vk_sync=True,
                )
        except Exception:
            logger.warning("vk_auto: failed to schedule rebuild after cancel", exc_info=True)

        return best_id, None


def request_vk_auto_import_cancel(*, chat_id: int, operator_id: int) -> None:
    """Request cancellation of the currently running VK auto import for this chat/operator."""
    try:
        key = (int(chat_id), int(operator_id))
    except Exception:
        return
    _vk_auto_import_cancel_requests.add(key)


def _vk_auto_import_cancelled(*, chat_id: int, operator_id: int) -> bool:
    try:
        key = (int(chat_id), int(operator_id))
    except Exception:
        return False
    return key in _vk_auto_import_cancel_requests


def _clear_vk_auto_import_cancel(*, chat_id: int, operator_id: int) -> None:
    try:
        key = (int(chat_id), int(operator_id))
    except Exception:
        return
    _vk_auto_import_cancel_requests.discard(key)


def _vk_wall_url(group_id: int, post_id: int, owner_type: str | None = "group") -> str:
    from vk_owner import vk_wall_url as _vk_wall_url_helper

    return _vk_wall_url_helper(group_id, post_id, owner_type)


def _best_url(sizes: Sequence[Mapping[str, Any]]) -> str:
    if not sizes:
        return ""
    best = max(
        sizes,
        key=lambda s: (s.get("width", 0) or 0) * (s.get("height", 0) or 0),
    )
    return str(best.get("url") or best.get("src") or "")


def _extract_media_urls(item: Mapping[str, Any], *, limit: int = 12) -> list[str]:
    """Extract image URLs from a VK wall item (photos + some common thumbnails)."""
    urls: list[str] = []
    seen: set[str] = set()

    def _add(url: str) -> None:
        u = (url or "").strip()
        if not u or u in seen:
            return
        seen.add(u)
        urls.append(u)

    def _process_atts(atts: Sequence[Mapping[str, Any]] | None) -> None:
        for att in atts or []:
            if len(urls) >= limit:
                return
            url = ""
            if att.get("type") == "photo":
                photo = att.get("photo") or {}
                url = _best_url(photo.get("sizes") or [])
            elif att.get("type") == "link":
                link = att.get("link") or {}
                url = _best_url(((link.get("photo") or {}).get("sizes") or []))
            elif att.get("type") == "video":
                video = att.get("video") or {}
                url = _best_url(video.get("first_frame") or video.get("image") or [])
            elif att.get("type") == "doc":
                sizes = (
                    ((att.get("doc") or {}).get("preview") or {})
                    .get("photo", {})
                    .get("sizes", [])
                )
                url = _best_url(sizes or [])
            if url:
                _add(url)

    _process_atts(item.get("attachments") or [])
    copy_history = item.get("copy_history") or []
    if copy_history and isinstance(copy_history, list):
        first = copy_history[0] if copy_history else None
        if isinstance(first, Mapping):
            _process_atts(first.get("attachments") or [])

    return urls


@dataclass(frozen=True)
class VkFetchStatus:
    ok: bool
    kind: Literal["ok", "not_found", "access_denied", "vk_api_error", "network_error"]
    error_code: int | None = None
    error: str | None = None
    attachment_count: int = 0
    unavailable_attachment_count: int = 0


def _vk_auto_allow_stale_inbox_text() -> bool:
    raw = (os.getenv("VK_AUTO_IMPORT_ALLOW_STALE_INBOX_TEXT_ON_FETCH_FAIL") or "").strip().lower()
    if not raw:
        return False
    return raw in {"1", "true", "yes", "on"}


async def fetch_vk_post_text_and_photos(
    group_id: int,
    post_id: int,
    *,
    db: Database | None = None,
    bot: Any | None = None,
    limit: int = 12,
) -> tuple[str, list[str], datetime | None, dict[str, Any] | None, VkFetchStatus]:
    """Fetch VK wall post (text + image URLs) via VK API.

    Uses `main.vk_api` so it can read via service token when configured.
    """
    import main as main_mod

    try:
        resp = await main_mod.vk_api("wall.getById", posts=f"-{int(group_id)}_{int(post_id)}")
    except Exception as exc:
        # NOTE: VKAPIError is defined in main.py. We inspect it dynamically to avoid
        # a hard import cycle and still keep error codes for decision-making.
        code = None
        msg = str(exc or "").strip()
        kind: VkFetchStatus["kind"] = "network_error"
        try:
            VKAPIError = getattr(main_mod, "VKAPIError", None)
            if VKAPIError is not None and isinstance(exc, VKAPIError):
                code = getattr(exc, "code", None)
                low = (getattr(exc, "message", None) or msg or "").casefold()
                # wall.getById may fail when a post is deleted/unavailable.
                if any(tok in low for tok in ("post was deleted", "post deleted", "has been deleted", "пост удал")):
                    kind = "not_found"
                elif int(code or 0) in {100, 113}:
                    kind = "not_found"
                elif int(code or 0) in {15, 30} or "access denied" in low:
                    kind = "access_denied"
                else:
                    kind = "vk_api_error"
        except Exception:
            kind = "network_error"
        logger.warning(
            "vk_auto: wall.getById failed -%s_%s kind=%s code=%s err=%s",
            group_id,
            post_id,
            kind,
            code,
            msg,
        )
        return "", [], None, None, VkFetchStatus(False, kind, error_code=code, error=msg or None)

    # `main.vk_api()` already returns the unwrapped VK "response" payload.
    # Keep compatibility with legacy callers that may still pass {"response": ...}.
    raw: Any = resp
    if isinstance(resp, Mapping) and "response" in resp:
        raw = resp.get("response")
    items: list[Mapping[str, Any]] = []
    if isinstance(raw, dict):
        raw_items = raw.get("items")
        if isinstance(raw_items, list):
            items = [it for it in raw_items if isinstance(it, Mapping)]
        elif any(k in raw for k in ("text", "attachments", "date")):
            items = [raw]
    elif isinstance(raw, list):
        items = [it for it in raw if isinstance(it, Mapping)]

    text = ""
    published_at: datetime | None = None
    photos: list[str] = []
    metrics: dict[str, Any] | None = None
    attachment_count = 0
    for it in items:
        direct_attachments = it.get("attachments") or ()
        if isinstance(direct_attachments, list):
            attachment_count += len(direct_attachments)
        copy_history_for_count = it.get("copy_history") or ()
        if isinstance(copy_history_for_count, list):
            for copied in copy_history_for_count:
                if isinstance(copied, Mapping) and isinstance(copied.get("attachments"), list):
                    attachment_count += len(copied.get("attachments") or ())
        candidate_text = it.get("text") if isinstance(it.get("text"), str) else ""
        repost_text = ""
        copy_history = it.get("copy_history")
        if isinstance(copy_history, list) and copy_history:
            first = copy_history[0]
            if isinstance(first, Mapping):
                rt = first.get("text")
                if isinstance(rt, str) and rt.strip():
                    repost_text = rt.strip()
        base = candidate_text.strip() if isinstance(candidate_text, str) else ""
        combined = base
        if repost_text:
            if not combined:
                combined = repost_text
            elif repost_text not in combined:
                combined = f"{combined}\n\n[Репост]\n{repost_text}".strip()
        if combined:
            text = combined
        ts = it.get("date")
        if isinstance(ts, (int, float)):
            try:
                published_at = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except Exception:
                published_at = None
        if metrics is None:
            m: dict[str, Any] = {}
            try:
                v = (it.get("views") or {}).get("count")
                if isinstance(v, int) and v >= 0:
                    m["views"] = v
            except Exception:
                pass
            try:
                l = (it.get("likes") or {}).get("count")
                if isinstance(l, int) and l >= 0:
                    m["likes"] = l
            except Exception:
                pass
            try:
                c = (it.get("comments") or {}).get("count")
                if isinstance(c, int) and c >= 0:
                    m["comments"] = c
            except Exception:
                pass
            try:
                r = (it.get("reposts") or {}).get("count")
                if isinstance(r, int) and r >= 0:
                    m["reposts"] = r
            except Exception:
                pass
            metrics = m or None
        photos.extend(_extract_media_urls(it, limit=limit))
        if text:
            break

    if not items:
        return "", [], None, None, VkFetchStatus(False, "not_found", error="empty_response")

    # Deduplicate photos while preserving order.
    out_photos: list[str] = []
    seen: set[str] = set()
    for u in photos:
        if u and u not in seen:
            seen.add(u)
            out_photos.append(u)
        if len(out_photos) >= limit:
            break

    return text, out_photos, published_at, metrics, VkFetchStatus(
        True,
        "ok",
        attachment_count=attachment_count,
        unavailable_attachment_count=max(0, attachment_count - len(out_photos)),
    )


async def _load_festival_hints(db: Database) -> tuple[list[str], list[tuple[str, int]]]:
    """Load festival names + alias pairs in the format expected by vk_intake.build_event_drafts()."""
    from sqlalchemy import select
    from models import Festival
    from main import normalize_alias

    async with db.get_session() as session:
        res_f = await session.execute(select(Festival))
        festivals = res_f.scalars().all()

    festival_names = sorted(
        {
            (fest.name or "").strip()
            for fest in festivals
            if (fest.name or "").strip()
        }
    )
    alias_pairs: list[tuple[str, int]] = []
    if not festival_names:
        return [], []

    index_map = {name: idx for idx, name in enumerate(festival_names)}
    for fest in festivals:
        name = (fest.name or "").strip()
        if not name:
            continue
        idx = index_map.get(name)
        if idx is None:
            continue
        base_norm = normalize_alias(name)
        for alias in getattr(fest, "aliases", None) or []:
            norm = normalize_alias(alias)
            if not norm or norm == base_norm:
                continue
            alias_pairs.append((norm, idx))

    if alias_pairs:
        seen_pairs: set[tuple[str, int]] = set()
        deduped: list[tuple[str, int]] = []
        for pair in alias_pairs:
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            deduped.append(pair)
        alias_pairs = deduped
    return festival_names, alias_pairs


@dataclass
class VkAutoImportReport:
    batch_id: str
    inbox_processed: int = 0
    inbox_imported: int = 0
    inbox_rejected: int = 0
    inbox_failed: int = 0
    inbox_deferred: int = 0
    skipped_requeued: int = 0
    cancelled: bool = False
    created_event_ids: list[int] = field(default_factory=list)
    updated_event_ids: list[int] = field(default_factory=list)
    inbox_ids: list[int] = field(default_factory=list)
    source_urls: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class VkInboxPrefetch:
    source_url: str
    source_name: str | None
    location_hint: str | None
    default_time: str | None
    default_ticket_link: str | None
    text: str
    photos: list[str]
    publish_ts: datetime | int | float | None
    published_at: datetime | None
    source_is_festival: bool = False
    metrics: dict[str, Any] | None = None
    vk_fetch: VkFetchStatus | None = None
    drafts: Any | None = None
    stage_sec: dict[str, float] = field(default_factory=dict)
    error: str | None = None


async def _resolve_bot_username(bot: Any | None) -> str | None:
    if not bot or not hasattr(bot, "get_me"):
        return None
    try:
        me = await bot.get_me()
    except Exception:
        return None
    username = getattr(me, "username", None)
    if not username:
        return None
    return str(username).lstrip("@") or None


def _log_deeplink(bot_username: str | None, event_id: int) -> str | None:
    if not bot_username:
        return None
    return f"https://t.me/{bot_username}?start=log_{int(event_id)}"


def _shorten_reason(value: str | None, *, limit: int = 220) -> str | None:
    if not value:
        return None
    text = " ".join(str(value).strip().split())
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _rate_limit_max_defers() -> int:
    raw = (os.getenv("VK_AUTO_IMPORT_RATE_LIMIT_MAX_DEFERS") or "3").strip()
    try:
        value = int(raw)
    except Exception:
        value = 3
    return max(0, min(value, 1000))


def _partial_import_max_attempts() -> int:
    raw = (os.getenv("VK_AUTO_IMPORT_PARTIAL_MAX_ATTEMPTS") or "3").strip()
    try:
        value = int(raw)
    except Exception:
        value = 3
    return max(1, min(value, 1000))


def _partial_import_retry_sec() -> float:
    raw = (os.getenv("VK_AUTO_IMPORT_PARTIAL_RETRY_SEC") or "60").strip()
    try:
        value = float(raw)
    except Exception:
        value = 60.0
    return max(0.0, min(value, 86_400.0))


def _vk_auto_import_max_photos() -> int:
    raw = (os.getenv("VK_AUTO_IMPORT_MAX_PHOTOS") or "4").strip()
    try:
        value = int(raw)
    except Exception:
        value = 4
    return max(1, min(value, 32))


def _vk_auto_import_schedule_max_photos() -> int:
    raw = (os.getenv("VK_AUTO_IMPORT_SCHEDULE_MAX_PHOTOS") or "10").strip()
    try:
        value = int(raw)
    except Exception:
        value = 10
    return max(_vk_auto_import_max_photos(), min(value, 32))


def _vk_auto_import_photo_limit_for_text(text: str | None) -> int:
    """Return a transport ceiling independent of semantic source text."""

    del text
    return 100


def _render_progress_text(
    icon: str,
    *,
    current_no: int,
    total_txt: str,
    source_url: str,
    extra_lines: Sequence[str] | None = None,
) -> str:
    header = f"{icon} Разбираю VK пост {current_no}/{total_txt}: {source_url}"
    lines = [header]
    for line in (extra_lines or []):
        line = (line or "").strip()
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


async def _update_progress_message(
    bot: Any,
    *,
    chat_id: int,
    message_id: int | None,
    text: str,
) -> None:
    """Best-effort: edit an existing progress message, fallback to sending a new message."""
    payload = (text or "").strip()
    if not payload:
        return
    if message_id and hasattr(bot, "edit_message_text"):
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(message_id),
                text=payload,
                disable_web_page_preview=True,
            )
            return
        except Exception:
            logger.warning("vk_auto: progress_edit_failed", exc_info=True)
    try:
        await bot.send_message(chat_id, payload, disable_web_page_preview=True)
    except Exception:
        logger.warning("vk_auto: progress_send_failed", exc_info=True)


async def _send_unified_event_report(
    db: Database,
    bot: Any,
    chat_id: int,
    *,
    created: list[int],
    updated: list[int],
    source_url: str,
    added_posters_by_event_id: Mapping[int, int] | None = None,
    post_metrics: Mapping[str, Any] | None = None,
    post_popularity: str | None = None,
) -> bool:
    from source_parsing.handlers import build_added_event_info, build_updated_event_info
    import html

    bot_username = await _resolve_bot_username(bot)
    lines: list[str] = []

    created = list(dict.fromkeys([int(eid) for eid in (created or []) if eid]))
    updated = list(dict.fromkeys([int(eid) for eid in (updated or []) if eid]))
    overlap = set(created) & set(updated)
    if overlap:
        # When one VK post yields multiple drafts that merge into the same event,
        # the event can be both "created" and then immediately "updated" within
        # the same run. Facts/logs reflect the LAST merge, so show such events in
        # the updated bucket to keep the report consistent.
        created = [eid for eid in created if eid not in overlap]

    ctx = None
    try:
        from source_parsing.smart_update_report import build_smart_update_report_context

        ctx = await build_smart_update_report_context(
            db,
            event_ids=(created + updated),
            source_urls=[source_url],
        )
    except Exception:
        ctx = None

    tz = getattr(ctx, "tz", None)
    sources_by_eid = getattr(ctx, "sources_by_event_id", None) or {}
    video_counts = getattr(ctx, "video_count_by_event_id", None) or {}
    ticket_queue_by_eid = getattr(ctx, "ticket_queue_by_event_id", None) or {}
    fest_queue_by_src = getattr(ctx, "festival_queue_by_source_url", None) or {}

    def _ics_line(url: str | None, *, has_time: bool) -> str:
        value = (url or "").strip()
        if value:
            safe = html.escape(value, quote=True)
            return f'ICS: <a href="{safe}">ics</a>'
        return "ICS: ⏳" if has_time else "ICS: —"

    def _vk_post_line(url: str | None) -> str | None:
        value = (url or "").strip()
        if not value:
            return None
        safe = html.escape(value, quote=True)
        return f'VK: <a href="{safe}">пост</a>'

    def _sources_lines(eid: int) -> list[str]:
        rows = list(sources_by_eid.get(int(eid)) or [])
        if not rows or not tz:
            return []
        from source_parsing.smart_update_report import format_dt_compact, short_url_label

        out: list[str] = ["  Источники:"]
        limit = 24
        shown = rows[:limit]
        for imported_at, url in shown:
            stamp = format_dt_compact(imported_at, tz)
            label = short_url_label(url) or url
            if str(url).strip().startswith(("http://", "https://")):
                safe_href = html.escape(str(url).strip(), quote=True)
                safe_label = html.escape(label)
                out.append(f"  {stamp} <a href=\"{safe_href}\">{safe_label}</a>")
            else:
                out.append(f"  {stamp} {html.escape(label)}")
        if len(rows) > limit:
            out.append(f"  … ещё {len(rows) - limit}")
        return out

    def _queue_lines(eid: int) -> list[str]:
        out: list[str] = []
        fest = fest_queue_by_src.get((source_url or "").strip())
        if fest:
            name = (getattr(fest, "festival_name", None) or getattr(fest, "festival_full", None) or "").strip()
            ctx2 = (getattr(fest, "festival_context", None) or "").strip()
            status = (getattr(fest, "status", None) or "").strip()
            fid = getattr(fest, "id", None)
            tail = name or ctx2
            extra = f" {tail}" if tail else ""
            id_part = f" (id={int(fid)})" if isinstance(fid, int) and fid > 0 else ""
            st_part = f" {status}" if status else ""
            out.append(f"  🎪 festival_queue:{st_part}{extra}{id_part}".strip())

        tickets = list(ticket_queue_by_eid.get(int(eid)) or [])
        if tickets:
            first = tickets[0]
            href = html.escape(str(getattr(first, 'url', '') or '').strip(), quote=True)
            label = html.escape(str(getattr(first, 'site_kind', '') or 'tickets').strip() or "tickets")
            extra = f" +{len(tickets)}" if len(tickets) > 1 else ""
            if href:
                out.append(f'  🎟 ticket_site_queue:{extra} <a href="{href}">{label}</a>')
            else:
                out.append(f"  🎟 ticket_site_queue:{extra}".strip())
        return out

    def _render_fact_stats(stats: Mapping[str, Any] | None) -> str:
        data = stats or {}
        if not data:
            return "Факты: —"
        added = int(data.get("added") or 0)
        dup = int(data.get("duplicate") or 0)
        conf = int(data.get("conflict") or 0)
        note = int(data.get("note") or 0)
        return f"Факты: ✅{added} ↩️{dup} ⚠️{conf} ℹ️{note}"

    def _render_facts_and_photos(info: Any, *, eid: int) -> str:
        stats_text = _render_fact_stats(getattr(info, "fact_stats", None))
        added_posters = getattr(info, "added_posters", None)
        try:
            added_posters_int = int(added_posters) if added_posters is not None else None
        except Exception:
            added_posters_int = None
        photo_count = getattr(info, "photo_count", None)
        try:
            photos = int(photo_count or 0)
        except Exception:
            photos = 0
        try:
            videos_total = int(video_counts.get(int(eid), 0) or 0)
        except Exception:
            videos_total = 0
        if added_posters_int is None:
            photos_label = f"Иллюстрации: {'⚠️0' if photos == 0 else photos}"
        else:
            photos_label = f"Иллюстрации: +{added_posters_int}, всего {'⚠️0' if photos == 0 else photos}"
        videos_label = f" | Видео: {videos_total}" if videos_total > 0 else ""
        return f"{stats_text} | {photos_label}{videos_label}"

    def _render_meta(date_value: str | None, time_value: str | None) -> str:
        meta: list[str] = []
        if date_value:
            meta.append(str(date_value))
        if time_value:
            meta.append(str(time_value))
        return f" — {' '.join(meta)}" if meta else ""

    def _render_source(source_url: str, info: Any) -> str:
        ord_value = getattr(info, "source_ordinal", None)
        total_value = getattr(info, "source_total", None)
        if isinstance(ord_value, int) and ord_value > 0:
            if isinstance(total_value, int) and total_value > 0:
                return f"Источник #{ord_value}/{total_value}: {source_url}"
            return f"Источник #{ord_value}: {source_url}"
        return f"Источник: {source_url}"

    if created or updated:
        lines.append("<b>Smart Update (детали событий):</b>")
        if post_metrics:
            parts: list[str] = []
            v = post_metrics.get("views") if isinstance(post_metrics, Mapping) else None
            l = post_metrics.get("likes") if isinstance(post_metrics, Mapping) else None
            if isinstance(v, int) and v >= 0:
                parts.append(f"views={v}")
            if isinstance(l, int) and l >= 0:
                parts.append(f"likes={l}")
            if parts:
                lines.append(f"Метрики поста: {html.escape(' '.join(parts))}")
    if created:
        lines.append(f"✅ Созданные события: {len(created)}")
        for eid in created[:12]:
            info = await build_added_event_info(db, int(eid), "vk", source_url=source_url)
            if not info:
                continue
            if added_posters_by_event_id is not None:
                info.added_posters = int(added_posters_by_event_id.get(int(eid), 0) or 0)
            title = html.escape(info.title or "Без названия")
            if (post_popularity or "").strip():
                title = f"{html.escape(str(post_popularity).strip())} {title}"
            tg_url = html.escape(info.telegraph_url or "", quote=True)
            meta = _render_meta(info.date, info.time)
            if info.telegraph_url:
                lines.append(f"• <a href=\"{tg_url}\">{title}</a> (id={info.event_id}){meta}")
            else:
                lines.append(f"• {title} (id={info.event_id}){meta}")
            lines.append(f"  {html.escape(_render_source(source_url, info))}")
            if not info.telegraph_url:
                lines.append("  Telegraph: ⏳ в очереди")
            lines.extend(_sources_lines(int(info.event_id)))
            if info.log_cmd:
                href = _log_deeplink(bot_username, int(info.event_id))
                if href:
                    lines.append(
                        f"  Лог: <a href=\"{html.escape(href, quote=True)}\">{html.escape(info.log_cmd)}</a>"
                    )
                else:
                    lines.append(f"  Лог: {html.escape(info.log_cmd)}")
            lines.append(f"  {_ics_line(info.ics_url, has_time=bool((info.time or '').strip()))}")
            vk_line = _vk_post_line(getattr(info, "vk_post_url", None))
            if vk_line:
                lines.append(f"  {vk_line}")
            lines.append(f"  {_render_facts_and_photos(info, eid=int(info.event_id))}")
            lines.extend(_queue_lines(int(info.event_id)))
            lines.append("")
        if len(created) > 12:
            lines.append(f"... ещё {len(created) - 12}")
    if updated:
        lines.append(f"🔄 Обновлённые события: {len(updated)}")
        for eid in updated[:12]:
            info = await build_updated_event_info(
                db, int(eid), "vk", "full_update", source_url=source_url
            )
            if not info:
                continue
            if added_posters_by_event_id is not None:
                info.added_posters = int(added_posters_by_event_id.get(int(eid), 0) or 0)
            title = html.escape(info.title or "Без названия")
            if (post_popularity or "").strip():
                title = f"{html.escape(str(post_popularity).strip())} {title}"
            tg_url = html.escape(info.telegraph_url or "", quote=True)
            meta = _render_meta(info.date, info.time)
            if info.telegraph_url:
                lines.append(f"• <a href=\"{tg_url}\">{title}</a> (id={info.event_id}){meta}")
            else:
                lines.append(f"• {title} (id={info.event_id}){meta}")
            lines.append(f"  {html.escape(_render_source(source_url, info))}")
            if not info.telegraph_url:
                lines.append("  Telegraph: ⏳ в очереди")
            lines.extend(_sources_lines(int(info.event_id)))
            if info.log_cmd:
                href = _log_deeplink(bot_username, int(info.event_id))
                if href:
                    lines.append(
                        f"  Лог: <a href=\"{html.escape(href, quote=True)}\">{html.escape(info.log_cmd)}</a>"
                    )
                else:
                    lines.append(f"  Лог: {html.escape(info.log_cmd)}")
            lines.append(f"  {_ics_line(info.ics_url, has_time=bool((info.time or '').strip()))}")
            vk_line = _vk_post_line(getattr(info, "vk_post_url", None))
            if vk_line:
                lines.append(f"  {vk_line}")
            lines.append(f"  {_render_facts_and_photos(info, eid=int(info.event_id))}")
            lines.extend(_queue_lines(int(info.event_id)))
            lines.append("")
        if len(updated) > 12:
            lines.append(f"... ещё {len(updated) - 12}")

    if not lines:
        return True
    text = "\n".join(lines).strip()
    if not text:
        return True
    try:
        await asyncio.wait_for(
            bot.send_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True),
            timeout=30,
        )
        logger.info(
            "vk_auto: unified_report_sent chat_id=%s created=%s updated=%s source=%s",
            chat_id,
            len(created),
            len(updated),
            source_url,
        )
        return True
    except asyncio.TimeoutError:
        logger.warning(
            "vk_auto: unified_report_timeout chat_id=%s created=%s updated=%s source=%s",
            chat_id,
            len(created),
            len(updated),
            source_url,
        )
        return False
    except Exception:
        logger.exception("vk_auto: failed to send unified report")
        return False


async def _prefetch_vk_inbox_row(
    db: Database,
    *,
    bot: Any | None,
    post: Any,
    source_url: str,
    festival_names: list[str] | None,
    festival_alias_pairs: Sequence[tuple[str, int]] | None,
) -> VkInboxPrefetch:
    """Best-effort prefetch for VK auto-import pipelining (N+1).

    Prefetch has no write side-effects in the DB; it only prepares data that would
    otherwise be computed inside `_process_vk_inbox_row`.
    """
    import main as main_mod

    stage: dict[str, float] = {}

    # Fetch VK source defaults.
    source_name_val: str | None = None
    location_hint_val: str | None = None
    default_time_val: str | None = None
    default_ticket_link_val: str | None = None
    source_is_festival = False
    t0 = time.monotonic()
    try:
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                "SELECT name, location, default_time, default_ticket_link, festival_source FROM vk_source WHERE group_id=?",
                (post.group_id,),
            )
            row = await cur.fetchone()
        if row:
            source_name_val, location_hint_val, default_time_val, default_ticket_link_val, source_is_festival = row
    except Exception as exc:
        logger.warning("vk_auto: prefetch db_source_defaults failed url=%s err=%s", source_url, exc)
    stage["db_source_defaults"] = float(time.monotonic() - t0)

    # Refresh text/photos from VK (best effort) to include attachments.
    t0 = time.monotonic()
    fetched_text, photos, published_at, metrics, vk_fetch = await fetch_vk_post_text_and_photos(
        post.group_id,
        post.post_id,
        db=db,
        bot=bot,
        limit=_vk_auto_import_photo_limit_for_text(getattr(post, "text", None)),
    )
    stage["vk_fetch_post"] = float(time.monotonic() - t0)
    allow_stale = _vk_auto_allow_stale_inbox_text()
    text = ""
    if vk_fetch.ok:
        text = (fetched_text or post.text or "").strip()
    elif allow_stale and vk_fetch.kind != "not_found":
        text = (post.text or "").strip()
    publish_ts: datetime | int | float | None = getattr(post, "date", None)
    if published_at is not None:
        publish_ts = int(published_at.timestamp())

    # Normalize configured hints to canonical venue lines when possible.
    if not (location_hint_val or "").strip() and (source_name_val or "").strip():
        try:
            matcher = getattr(main_mod, "_match_known_venue", None)
            if callable(matcher):
                venue = matcher(source_name_val)
                if venue is not None:
                    location_hint_val = getattr(venue, "canonical_line", None) or location_hint_val
        except Exception:
            logger.warning("vk_auto: prefetch infer location_hint failed", exc_info=True)
    elif (location_hint_val or "").strip():
        try:
            matcher = getattr(main_mod, "_match_known_venue", None)
            if callable(matcher):
                venue = matcher(location_hint_val)
                if venue is not None:
                    location_hint_val = getattr(venue, "canonical_line", None) or location_hint_val
        except Exception:
            logger.warning("vk_auto: prefetch canonicalize location_hint failed", exc_info=True)

    # Semantic parsing is deliberately not started by prefetch. The main
    # worker owns one durable parse receipt per packet revision; prefetch only
    # transports source/attachment evidence.
    drafts: Any | None = None
    err: str | None = None
    stage["build_drafts_total"] = 0.0

    return VkInboxPrefetch(
        source_url=source_url,
        source_name=source_name_val,
        location_hint=location_hint_val,
        default_time=default_time_val,
        default_ticket_link=default_ticket_link_val,
        source_is_festival=bool(source_is_festival),
        text=text,
        photos=list(photos or []),
        publish_ts=publish_ts,
        published_at=published_at,
        metrics=metrics if isinstance(metrics, dict) else None,
        vk_fetch=vk_fetch,
        drafts=drafts,
        stage_sec=stage,
        error=err,
    )


async def run_vk_auto_import(
    db: Database,
    bot: Any,
    *,
    chat_id: int,
    limit: int = 25,
    operator_id: int = 0,
    include_skipped: bool = False,
    trigger: str = "manual",
    run_id: str | None = None,
    ops_run_id: int | None = None,
) -> VkAutoImportReport:
    """Auto-import VK inbox queue sequentially via Smart Update (LLM).

    Intended usage:
    - scheduled job (admin chat)
    - manual command for E2E debugging
    """
    batch_id = f"auto:{int(time.time())}"
    report = VkAutoImportReport(batch_id=batch_id)
    if not ops_run_id:
        ops_run_id = await start_ops_run(
            db,
            kind="vk_auto_import",
            trigger=trigger,
            chat_id=chat_id,
            operator_id=operator_id,
            details={
                "batch_id": batch_id,
                "run_id": run_id,
                "limit_requested": limit,
                "include_skipped": int(bool(include_skipped)),
            },
        )
    _clear_vk_auto_import_cancel(chat_id=chat_id, operator_id=operator_id)
    try:
        limit_int = int(limit)
    except Exception:
        limit_int = 25
    unbounded = limit_int <= 0

    await vk_review.release_stale_locks(db)
    await vk_review.release_due_deferred(db, batch_id=batch_id)

    def _env_enabled(name: str, default: bool) -> bool:
        raw = (os.getenv(name) or "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "on"}

    send_progress = _env_enabled("VK_AUTO_IMPORT_SEND_PROGRESS", True)
    try:
        progress_every = int(os.getenv("VK_AUTO_IMPORT_PROGRESS_EVERY", "1") or "1")
    except Exception:
        progress_every = 1
    progress_every = max(1, min(progress_every, 50))
    try:
        row_timeout_raw = (os.getenv("VK_AUTO_IMPORT_ROW_TIMEOUT_SEC") or "").strip()
        row_timeout_sec = float(row_timeout_raw) if row_timeout_raw else 30.0 * 60.0
    except Exception:
        row_timeout_sec = 30.0 * 60.0
    if row_timeout_sec <= 0:
        row_timeout_sec = 0.0
    else:
        row_timeout_sec = min(float(row_timeout_sec), 6.0 * 60.0 * 60.0)

    # Optional: include previously skipped rows in the run. This is useful for
    # E2E over a prod DB snapshot where an operator may have skipped items
    # earlier, but we still want to validate Smart Update correctness.
    if include_skipped:
        async with db.raw_conn() as conn:
            cur = await conn.execute("SELECT COUNT(1) FROM vk_inbox WHERE status='pending'")
            row = await cur.fetchone()
            pending_count = int((row[0] if row else 0) or 0)
        # Do not inflate queue for this run: requeue only enough skipped rows
        # to fill the remaining batch up to `limit`.
        if unbounded:
            requeue_limit = 10**9
        else:
            requeue_limit = max(0, int(limit_int) - pending_count)
        if requeue_limit <= 0:
            requeue_limit = 0
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                """
                SELECT id
                FROM vk_inbox
                WHERE status='skipped'
                ORDER BY date ASC,
                         CASE WHEN event_ts_hint IS NULL THEN 1 ELSE 0 END,
                         event_ts_hint ASC,
                         id ASC
                LIMIT ?
                """,
                (requeue_limit,),
            )
            ids = [int(r[0]) for r in (await cur.fetchall() or [])]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                await conn.execute(
                    f"""
                    UPDATE vk_inbox
                    SET status='pending',
                        locked_by=NULL,
                        locked_at=NULL,
                        review_batch=NULL
                    WHERE id IN ({placeholders})
                    """,
                    tuple(ids),
                )
                await conn.commit()
                report.skipped_requeued = len(ids)
                logger.info(
                    "vk_auto: requeued_skipped=%s pending=%s limit=%s",
                    len(ids),
                    pending_count,
                    limit,
                )

    # Preload festival hints once per run.
    try:
        festival_names, festival_alias_pairs = await _load_festival_hints(db)
    except Exception as exc:
        festival_names, festival_alias_pairs = [], []
        report.errors.append(f"festival_hints_failed: {exc}")

    total_estimate = None
    try:
        statuses = ("pending", "skipped") if include_skipped else ("pending",)
        placeholders = ",".join(["?"] * len(statuses))
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                f"""
                SELECT COUNT(1)
                FROM vk_inbox
                WHERE status IN ({placeholders})
                """,
                statuses,
            )
            row = await cur.fetchone()
            total_estimate = int((row[0] if row else 0) or 0)
            if not unbounded:
                total_estimate = min(int(total_estimate), int(limit_int))
    except Exception:
        total_estimate = None

    start = time.time()
    heavy_mode = _vk_auto_import_heavy_mode(trigger)
    waiting_meta = current_heavy_meta() if heavy_mode != "off" else None
    if waiting_meta is not None and heavy_mode == "wait":
        waiting_text = (
            "⏳ VK auto import ждёт завершения другой тяжёлой операции\n"
            f"Сейчас занято: {describe_heavy_meta(waiting_meta)}\n"
            "После освобождения очереди разбор продолжится автоматически."
        )
        try:
            await bot.send_message(chat_id, waiting_text, disable_web_page_preview=True)
        except Exception:
            logger.warning("vk_auto: heavy_wait_notice_send_failed", exc_info=True)
        logger.info(
            "vk_auto: waiting_heavy kind=%s trigger=%s run_id=%s operator_id=%s",
            waiting_meta.kind,
            waiting_meta.trigger,
            waiting_meta.run_id,
            waiting_meta.operator_id,
        )
    gate = (
        _NoopHeavyOperation()
        if heavy_mode == "off"
        else heavy_operation(
            kind="vk_auto_import",
            trigger=trigger,
            mode="try" if heavy_mode == "try" else "wait",
            run_id=run_id,
            operator_id=operator_id,
            chat_id=chat_id,
        )
    )

    async with gate as acquired:
        if not acquired:
            try:
                await bot.send_message(
                    chat_id,
                    "⏳ VK auto import сейчас занят другой тяжёлой операцией. Повтори запуск позже.",
                    disable_web_page_preview=True,
                )
            except Exception:
                logger.warning("vk_auto: heavy_busy_notice_send_failed", exc_info=True)
            report.cancelled = True
            report.errors.append("heavy_busy")
            await finish_ops_run(
                db,
                run_id=ops_run_id,
                status="skipped",
                metrics={
                    "inbox_processed": 0,
                    "inbox_imported": 0,
                    "inbox_rejected": 0,
                    "inbox_failed": 0,
                    "inbox_deferred": 0,
                    "events_created": 0,
                    "events_updated": 0,
                    "cancelled": 1,
                    "skipped_requeued": int(report.skipped_requeued),
                    "include_skipped": int(bool(include_skipped)),
                    "limit": int(limit_int),
                    "duration_sec": round(float(time.time() - start), 3),
                },
                details={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "skip_reason": "heavy_busy",
                    "errors": list(report.errors or [])[:40],
                },
            )
            return report
        current_no = 0
        prefetch_enabled = _env_enabled("VK_AUTO_IMPORT_PREFETCH", False)

        async def _await_prefetch(task: asyncio.Task | None) -> VkInboxPrefetch | None:
            if task is None:
                return None
            try:
                res = await task
            except asyncio.CancelledError:
                return None
            except Exception:
                return None
            return res if isinstance(res, VkInboxPrefetch) else None

        def _start_prefetch(post_obj: Any) -> asyncio.Task | None:
            if not prefetch_enabled or not post_obj:
                return None
            next_url = _vk_wall_url(
                post_obj.group_id,
                post_obj.post_id,
                getattr(post_obj, "owner_type", None) or "group",
            )
            return asyncio.create_task(
                _prefetch_vk_inbox_row(
                    db,
                    bot=bot,
                    post=post_obj,
                    source_url=next_url,
                    festival_names=festival_names,
                    festival_alias_pairs=festival_alias_pairs,
                )
            )

        async def _await_process_task(
            *,
            post_obj: Any,
            source_url: str,
            process_task: asyncio.Task,
        ) -> None:
            try:
                if row_timeout_sec > 0:
                    await asyncio.wait_for(process_task, timeout=row_timeout_sec)
                else:
                    await process_task
            except asyncio.TimeoutError:
                report.inbox_failed += 1
                report.errors.append(
                    f"timeout_failed {source_url}: row timed out after {row_timeout_sec:.1f}s"
                )
                try:
                    await vk_review.schedule_retry(db, int(post_obj.id), typed_reason="ROW_TIMEOUT", batch_id=batch_id)
                except Exception:
                    logger.warning("vk_auto: schedule_retry failed after timeout", exc_info=True)
                logger.warning(
                    "vk_auto: inbox row timeout id=%s url=%s timeout_sec=%.1f",
                    getattr(post_obj, "id", None),
                    source_url,
                    row_timeout_sec,
                )
                try:
                    await bot.send_message(
                        chat_id,
                        (
                            "❌ VK auto import: таймаут обработки поста\n"
                            f"{source_url}\n"
                            f"timeout_sec={row_timeout_sec:.1f}"
                        ),
                        disable_web_page_preview=True,
                    )
                except Exception:
                    logger.warning("vk_auto: timeout_send_failed", exc_info=True)
            except Exception as exc:
                report.inbox_failed += 1
                report.errors.append(f"unexpected_failed {source_url}: {exc}")
                try:
                    await vk_review.schedule_retry(db, int(post_obj.id), typed_reason="UNEXPECTED_ERROR", batch_id=batch_id)
                except Exception:
                    logger.warning("vk_auto: schedule_retry failed after exception", exc_info=True)
                logger.exception(
                    "vk_auto: unexpected exception in inbox row processing id=%s url=%s",
                    getattr(post_obj, "id", None),
                    source_url,
                )
                try:
                    await bot.send_message(
                        chat_id,
                        f"❌ VK auto import: техническая ошибка при обработке поста\n{source_url}\n{exc}",
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass

        if unbounded:
            post = await vk_review.pick_next(
                db,
                operator_id,
                batch_id,
                requeue_skipped=False,
                prefer_oldest=True,
                strict_chronological=True,
            )
            prefetch_task = _start_prefetch(post) if post else None
            while post:
                if _vk_auto_import_cancelled(chat_id=chat_id, operator_id=operator_id):
                    report.cancelled = True
                    try:
                        await vk_review.mark_pending(db, int(post.id))
                    except Exception:
                        logger.warning("vk_auto: mark_pending failed on cancel", exc_info=True)
                    if prefetch_task:
                        prefetch_task.cancel()
                    break

                current_no += 1
                report.inbox_processed += 1
                source_url = _vk_wall_url(
                    post.group_id, post.post_id, getattr(post, "owner_type", None) or "group"
                )
                report.inbox_ids.append(int(post.id))
                report.source_urls.append(source_url)
                total_txt = str(int(total_estimate)) if isinstance(total_estimate, int) else "?"
                progress_mid: int | None = None
                if send_progress and (current_no % progress_every == 0):
                    try:
                        msg = await bot.send_message(
                            chat_id,
                            f"⏳ Разбираю VK пост {current_no}/{total_txt}: {source_url}",
                            disable_web_page_preview=True,
                        )
                        progress_mid = getattr(msg, "message_id", None) or getattr(msg, "id", None)
                        if progress_mid is not None:
                            progress_mid = int(progress_mid)
                    except Exception:
                        logger.warning("vk_auto: progress_send_failed", exc_info=True)

                prefetched_row = await _await_prefetch(prefetch_task)
                process_task = asyncio.create_task(
                    _process_vk_inbox_row(
                        db,
                        bot,
                        chat_id=chat_id,
                        operator_id=operator_id,
                        batch_id=batch_id,
                        post=post,
                        source_url=source_url,
                        report=report,
                        festival_names=festival_names,
                        festival_alias_pairs=festival_alias_pairs,
                        progress_message_id=progress_mid,
                        progress_current_no=current_no,
                        progress_total_txt=total_txt,
                        prefetched=prefetched_row,
                        ops_run_id=ops_run_id,
                    )
                )

                await _await_process_task(
                    post_obj=post,
                    source_url=source_url,
                    process_task=process_task,
                )

                next_post = None
                next_prefetch_task = None
                if prefetch_enabled:
                    next_post = await vk_review.pick_next(
                        db,
                        operator_id,
                        batch_id,
                        requeue_skipped=False,
                        prefer_oldest=True,
                        strict_chronological=True,
                        resume_locked=False,
                    )
                    next_prefetch_task = _start_prefetch(next_post) if next_post else None
                else:
                    next_post = await vk_review.pick_next(
                        db,
                        operator_id,
                        batch_id,
                        requeue_skipped=False,
                        prefer_oldest=True,
                        strict_chronological=True,
                    )

                post = next_post
                prefetch_task = next_prefetch_task
        else:
            remaining = max(1, int(limit_int))
            post = await vk_review.pick_next(
                db,
                operator_id,
                batch_id,
                requeue_skipped=False,
                prefer_oldest=True,
                strict_chronological=True,
            )
            prefetch_task = _start_prefetch(post) if post else None
            while post and remaining > 0:
                if _vk_auto_import_cancelled(chat_id=chat_id, operator_id=operator_id):
                    report.cancelled = True
                    try:
                        await vk_review.mark_pending(db, int(post.id))
                    except Exception:
                        logger.warning("vk_auto: mark_pending failed on cancel", exc_info=True)
                    if prefetch_task:
                        prefetch_task.cancel()
                    break

                current_no += 1
                remaining -= 1
                report.inbox_processed += 1
                source_url = _vk_wall_url(
                    post.group_id, post.post_id, getattr(post, "owner_type", None) or "group"
                )
                report.inbox_ids.append(int(post.id))
                report.source_urls.append(source_url)
                total_txt = str(int(total_estimate)) if isinstance(total_estimate, int) else str(int(limit_int))
                progress_mid = None
                if send_progress and (current_no % progress_every == 0):
                    try:
                        msg = await bot.send_message(
                            chat_id,
                            f"⏳ Разбираю VK пост {current_no}/{total_txt}: {source_url}",
                            disable_web_page_preview=True,
                        )
                        progress_mid = getattr(msg, "message_id", None) or getattr(msg, "id", None)
                        if progress_mid is not None:
                            progress_mid = int(progress_mid)
                    except Exception:
                        logger.warning("vk_auto: progress_send_failed", exc_info=True)

                prefetched_row = await _await_prefetch(prefetch_task)
                process_task = asyncio.create_task(
                    _process_vk_inbox_row(
                        db,
                        bot,
                        chat_id=chat_id,
                        operator_id=operator_id,
                        batch_id=batch_id,
                        post=post,
                        source_url=source_url,
                        report=report,
                        festival_names=festival_names,
                        festival_alias_pairs=festival_alias_pairs,
                        progress_message_id=progress_mid,
                        progress_current_no=current_no,
                        progress_total_txt=total_txt,
                        prefetched=prefetched_row,
                        ops_run_id=ops_run_id,
                    )
                )

                await _await_process_task(
                    post_obj=post,
                    source_url=source_url,
                    process_task=process_task,
                )

                next_post = None
                next_prefetch_task = None
                if remaining > 0:
                    if prefetch_enabled:
                        next_post = await vk_review.pick_next(
                            db,
                            operator_id,
                            batch_id,
                            requeue_skipped=False,
                            prefer_oldest=True,
                            strict_chronological=True,
                            resume_locked=False,
                        )
                        next_prefetch_task = _start_prefetch(next_post) if next_post else None
                    else:
                        next_post = await vk_review.pick_next(
                            db,
                            operator_id,
                            batch_id,
                            requeue_skipped=False,
                            prefer_oldest=True,
                            strict_chronological=True,
                        )

                post = next_post
                prefetch_task = next_prefetch_task

    took = time.time() - start
    total_txt = str(int(total_estimate)) if isinstance(total_estimate, int) else "?"
    summary = (
        "🏁 VK auto import завершён\n"
        f"batch: {batch_id}\n"
        f"limit: {'all' if unbounded else limit_int}\n"
        f"include_skipped: {1 if include_skipped else 0}\n"
        f"cancelled: {1 if report.cancelled else 0}\n"
        f"queue processed: {report.inbox_processed}/{total_txt}\n"
        f"inbox imported: {report.inbox_imported}\n"
        f"inbox rejected: {report.inbox_rejected}\n"
        f"inbox failed: {report.inbox_failed}\n"
        f"inbox deferred: {report.inbox_deferred}\n"
        f"events created: {len(set(report.created_event_ids))}\n"
        f"events updated: {len(set(report.updated_event_ids))}\n"
        f"took_sec: {took:.1f}"
    )
    try:
        await bot.send_message(chat_id, summary, disable_web_page_preview=True)
    except Exception:
        logger.exception("vk_auto: failed to send summary")

    _clear_vk_auto_import_cancel(chat_id=chat_id, operator_id=operator_id)
    terminal_status = (
        "canceled"
        if report.cancelled
        else "partial"
        if report.inbox_failed > 0 and report.inbox_processed > report.inbox_failed
        else "failed"
        if report.inbox_failed > 0
        else "success"
    )
    await finish_ops_run(
        db,
        run_id=ops_run_id,
        status=terminal_status,
        metrics={
            "inbox_processed": int(report.inbox_processed),
            "inbox_imported": int(report.inbox_imported),
            "inbox_rejected": int(report.inbox_rejected),
            "inbox_failed": int(report.inbox_failed),
            "inbox_deferred": int(report.inbox_deferred),
            "events_created": int(len(set(report.created_event_ids))),
            "events_updated": int(len(set(report.updated_event_ids))),
            "cancelled": int(bool(report.cancelled)),
            "skipped_requeued": int(report.skipped_requeued),
            "include_skipped": int(bool(include_skipped)),
            "limit": int(limit_int),
            "duration_sec": round(float(took), 3),
        },
        details={
            "batch_id": batch_id,
            "run_id": run_id,
            "inbox_ids": list(dict.fromkeys(report.inbox_ids)),
            "source_urls": list(dict.fromkeys(report.source_urls)),
            "created_event_ids": sorted(set(report.created_event_ids)),
            "updated_event_ids": sorted(set(report.updated_event_ids)),
            "errors": list(report.errors or [])[:40],
        },
    )
    logger.info(
        "vk_auto_import terminal ops_run_id=%s run_id=%s batch_id=%s status=%s inbox_ids=%s source_urls=%s created_event_ids=%s updated_event_ids=%s metrics=%s",
        ops_run_id,
        run_id,
        batch_id,
        terminal_status,
        list(dict.fromkeys(report.inbox_ids)),
        list(dict.fromkeys(report.source_urls)),
        sorted(set(report.created_event_ids)),
        sorted(set(report.updated_event_ids)),
        {
            "processed": report.inbox_processed,
            "imported": report.inbox_imported,
            "rejected": report.inbox_rejected,
            "failed": report.inbox_failed,
            "deferred": report.inbox_deferred,
            "duration_sec": round(float(took), 3),
        },
    )
    return report


async def _process_vk_inbox_row(
    db: Database,
    bot: Any,
    *,
    chat_id: int,
    operator_id: int,
    batch_id: str,
    post: Any,
    source_url: str,
    report: VkAutoImportReport,
    festival_names: list[str] | None,
    festival_alias_pairs: Sequence[tuple[str, int]] | None,
    progress_message_id: int | None,
    progress_current_no: int,
    progress_total_txt: str,
    prefetched: VkInboxPrefetch | None = None,
    ops_run_id: int | None = None,
) -> None:
    import main as main_mod

    start_ts = time.monotonic()
    timings_on = _timings_enabled()
    t_stage: dict[str, float] = {}
    try:
        slow_log_sec = float(os.getenv("VK_AUTO_IMPORT_SLOW_ROW_LOG_SEC", "60") or "60")
    except Exception:
        slow_log_sec = 60.0
    slow_log_sec = max(0.0, min(slow_log_sec, 3600.0))

    def _tmark(name: str, elapsed: float) -> None:
        t_stage[name] = float(elapsed)

    def _log_row_timing(*, drafts_count: int, ok_value: bool) -> None:
        took_total = time.monotonic() - start_ts
        slow_log_due = took_total >= slow_log_sec if slow_log_sec > 0 else True
        try:
            logger.info(
                "timing vk_auto_import_row ops_run_id=%s batch_id=%s inbox_id=%s group_id=%s post_id=%s source_url=%s drafts=%s ok=%s took_sec=%.3f stages=%s created_event_ids=%s updated_event_ids=%s",
                ops_run_id,
                batch_id,
                int(getattr(post, "id", 0) or 0),
                int(getattr(post, "group_id", 0) or 0),
                int(getattr(post, "post_id", 0) or 0),
                source_url,
                int(drafts_count),
                1 if ok_value else 0,
                float(took_total),
                {k: round(v, 3) for k, v in sorted(t_stage.items())},
                sorted(set(report.created_event_ids)),
                sorted(set(report.updated_event_ids)),
            )
        except Exception:
            pass

    async def _emit_progress(icon: str, extra_lines: Sequence[str] | None = None) -> None:
        if not progress_message_id:
            return
        text = _render_progress_text(
            icon,
            current_no=int(progress_current_no),
            total_txt=str(progress_total_txt),
            source_url=source_url,
            extra_lines=extra_lines,
        )
        await _update_progress_message(
            bot,
            chat_id=chat_id,
            message_id=progress_message_id,
            text=text,
        )

    # Fetch VK source defaults.
    source_name_val: str | None = None
    location_hint_val: str | None = None
    default_time_val: str | None = None
    default_ticket_link_val: str | None = None
    source_is_festival = False
    pf = prefetched if (prefetched and prefetched.source_url == source_url) else None

    if pf is not None:
        source_name_val = pf.source_name
        location_hint_val = pf.location_hint
        default_time_val = pf.default_time
        default_ticket_link_val = pf.default_ticket_link
        source_is_festival = bool(pf.source_is_festival)
        _tmark("db_source_defaults", float(pf.stage_sec.get("db_source_defaults", 0.0) or 0.0))

        text = (pf.text or "").strip()
        photos = list(pf.photos or [])
        published_at = pf.published_at
        metrics = pf.metrics if isinstance(pf.metrics, dict) else None
        vk_fetch = pf.vk_fetch
        publish_ts = pf.publish_ts if pf.publish_ts is not None else getattr(post, "date", None)
        _tmark("vk_fetch_post", float(pf.stage_sec.get("vk_fetch_post", 0.0) or 0.0))
    else:
        t0 = time.monotonic()
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                "SELECT name, location, default_time, default_ticket_link, festival_source FROM vk_source WHERE group_id=?",
                (post.group_id,),
            )
            row = await cur.fetchone()
        if row:
            source_name_val, location_hint_val, default_time_val, default_ticket_link_val, source_is_festival = row
        _tmark("db_source_defaults", time.monotonic() - t0)

        # Refresh text/photos from VK (best effort) to include attachments.
        t0 = time.monotonic()
        fetched_text, photos, published_at, metrics, vk_fetch = await fetch_vk_post_text_and_photos(
            post.group_id,
            post.post_id,
            db=db,
            bot=bot,
            limit=_vk_auto_import_photo_limit_for_text(getattr(post, "text", None)),
        )
        _tmark("vk_fetch_post", time.monotonic() - t0)
        allow_stale = _vk_auto_allow_stale_inbox_text()
        if vk_fetch.ok:
            text = (fetched_text or post.text or "").strip()
        elif allow_stale and vk_fetch.kind != "not_found":
            text = (post.text or "").strip()
        else:
            text = ""
        publish_ts = post.date
        if published_at is not None:
            publish_ts = int(published_at.timestamp())

        # If VK source has no explicit location hint configured, try to map its name
        # to a canonical location from docs/reference/locations.md.
        if not (location_hint_val or "").strip() and (source_name_val or "").strip():
            try:
                matcher = getattr(main_mod, "_match_known_venue", None)
                if callable(matcher):
                    venue = matcher(source_name_val)
                    if venue is not None:
                        location_hint_val = getattr(venue, "canonical_line", None) or location_hint_val
            except Exception:
                logger.warning("vk_auto: failed to infer location_hint from reference", exc_info=True)
        elif (location_hint_val or "").strip():
            # Normalize configured hints to canonical venue lines when possible,
            # so LLM gets a stable "name, address, city" format.
            try:
                matcher = getattr(main_mod, "_match_known_venue", None)
                if callable(matcher):
                    venue = matcher(location_hint_val)
                    if venue is not None:
                        location_hint_val = getattr(venue, "canonical_line", None) or location_hint_val
            except Exception:
                logger.warning("vk_auto: failed to canonicalize location_hint", exc_info=True)

    if vk_fetch is not None and not vk_fetch.ok:
        allow_stale = _vk_auto_allow_stale_inbox_text()
        if vk_fetch.kind == "not_found":
            report.inbox_deferred += 1
            await vk_review.schedule_retry(
                db,
                int(post.id),
                typed_reason="EVIDENCE_UNAVAILABLE",
                batch_id=batch_id,
                retry_after_sec=86400,
            )
            await _emit_progress(
                "🗑️",
                [
                    "Результат: evidence недоступен, повтор запланирован",
                    f"Причина: {vk_fetch.error_code or ''} {_shorten_reason(vk_fetch.error) or ''}".strip(),
                    f"took_sec: {(time.monotonic() - start_ts):.1f}",
                ],
            )
            return
        if not (allow_stale and (text or "").strip()):
            report.inbox_failed += 1
            report.errors.append(
                f"vk_fetch_failed {source_url}: kind={vk_fetch.kind} code={vk_fetch.error_code} err={vk_fetch.error}"
            )
            await vk_review.schedule_retry(db, int(post.id), typed_reason="SOURCE_FETCH_ERROR", batch_id=batch_id)
            await _emit_progress(
                "❌",
                [
                    "Результат: не удалось загрузить пост из VK (wall.getById)",
                    f"Причина: kind={vk_fetch.kind} code={vk_fetch.error_code}",
                    f"took_sec: {(time.monotonic() - start_ts):.1f}",
                ],
            )
            return

    post_popularity: str | None = None
    if isinstance(metrics, dict) and metrics:
        try:
            from source_parsing.post_metrics import (
                compute_age_day,
                load_vk_popularity_baseline,
                normalize_age_day,
                popularity_marks,
                upsert_vk_post_metric,
            )

            collected_ts = int(time.time())
            published_ts: int | None = None
            if isinstance(publish_ts, datetime):
                dt = publish_ts
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                published_ts = int(dt.timestamp())
            elif isinstance(publish_ts, (int, float)):
                published_ts = int(publish_ts)

            age_day = normalize_age_day(compute_age_day(published_ts=published_ts, collected_ts=collected_ts))
            if isinstance(age_day, int) and age_day >= 0:
                views = metrics.get("views")
                likes = metrics.get("likes")
                comments = metrics.get("comments")
                reposts = metrics.get("reposts")
                await upsert_vk_post_metric(
                    db,
                    group_id=int(post.group_id),
                    post_id=int(post.post_id),
                    age_day=int(age_day),
                    source_url=source_url,
                    post_ts=published_ts,
                    views=int(views) if isinstance(views, int) else None,
                    likes=int(likes) if isinstance(likes, int) else None,
                    comments=int(comments) if isinstance(comments, int) else None,
                    reposts=int(reposts) if isinstance(reposts, int) else None,
                    collected_ts=int(collected_ts),
                )
                baseline = await load_vk_popularity_baseline(
                    db,
                    group_id=int(post.group_id),
                    age_day=int(age_day),
                    now_ts=int(collected_ts),
                )
                marks = popularity_marks(
                    views=views if isinstance(views, int) else None,
                    likes=likes if isinstance(likes, int) else None,
                    baseline=baseline,
                )
                post_popularity = marks.text or None
        except Exception:
            logger.warning(
                "vk_auto: failed to persist/score post metrics gid=%s post_id=%s",
                getattr(post, "group_id", None),
                getattr(post, "post_id", None),
                exc_info=True,
            )

    # ``wall.getById`` may observe an edit after the crawler created the inbox
    # row. Never attach a new provider call (or an old successful receipt) to
    # that stale revision: append the fetched revision first, repoint the
    # carrier, and retain the current lease while this worker processes it.
    packet_id = getattr(post, "source_packet_id", None)
    if packet_id is not None and (vk_fetch is None or vk_fetch.ok):
        async with db.raw_conn() as conn:
            packet_row = await (await conn.execute(
                "SELECT raw_text,attachment_metadata_json,published_at FROM vk_source_packet WHERE id=?",
                (int(packet_id),),
            )).fetchone()
        packet_photos: list[str] = []
        if packet_row:
            try:
                packet_attachments = json.loads(packet_row[1] or "{}")
                packet_photos = [str(value) for value in (packet_attachments.get("photos") or ())]
            except Exception:
                packet_photos = []
        fetched_photos = [str(value) for value in photos]
        if packet_row and (
            str(packet_row[0] or "").strip() != str(text or "").strip()
            or packet_photos != fetched_photos
        ):
            current_packet_id, _is_new = await vk_intake._persist_vk_source_packet(
                db,
                group_id=int(post.group_id),
                owner_type=str(getattr(post, "owner_type", None) or "group"),
                post={
                    "date": int(publish_ts or packet_row[2] or getattr(post, "date", 0) or 0),
                    "post_id": int(post.post_id),
                    "text": str(text or ""),
                    "photos": fetched_photos,
                },
                source_url=source_url,
                keyword_hints=("hint:queue_refetch_revision",),
                date_hints=(),
                event_ts_hint=getattr(post, "event_ts_hint", None),
            )
            post.source_packet_id = int(current_packet_id)
            async with db.raw_conn() as conn:
                await conn.execute(
                    """
                    UPDATE vk_inbox SET status='locked',locked_by=?,locked_at=CURRENT_TIMESTAMP,
                        review_batch=? WHERE id=?
                    """,
                    (operator_id, batch_id, int(post.id)),
                )
                await conn.execute(
                    """
                    UPDATE vk_source_packet SET status='processing',lease_owner=?,
                        lease_expires_at=datetime('now','+15 minutes'),updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (str(operator_id), int(current_packet_id)),
                )
                await conn.commit()

    model_name = _vk_auto_parse_gemma_model()
    receipt = await vk_review.load_successful_parse_receipt(
        db,
        source_packet_id=getattr(post, "source_packet_id", None),
        prompt_version=PARSE_VERSION,
        model=model_name,
    )
    exact_parse_replay = receipt is not None
    if receipt is not None:
        try:
            drafts = vk_intake.DraftParseResult.from_receipt_payload(receipt)
        except Exception as exc:
            receipt = None
            exact_parse_replay = False
            logger.warning("vk_auto: invalid durable parse receipt; reparsing url=%s err=%s", source_url, exc)

    if receipt is None:
        try:
            parse_festival_names = festival_names if source_is_festival else None
            parse_festival_alias_pairs = festival_alias_pairs if source_is_festival else None
            t0 = time.monotonic()
            drafts, _festival_info = await vk_intake.build_event_drafts(
                text,
                photos=photos,
                source_name=source_name_val,
                location_hint=location_hint_val,
                default_time=default_time_val,
                default_ticket_link=default_ticket_link_val,
                operator_extra=None,
                festival_names=parse_festival_names,
                festival_alias_pairs=parse_festival_alias_pairs or None,
                festival_hint=bool(source_is_festival),
                publish_ts=publish_ts,
                event_ts_hint=post.event_ts_hint,
                rate_limit_max_wait_sec=0,
                parse_gemma_model=model_name,
                attachment_count_hint=int(getattr(vk_fetch, "attachment_count", 0) or len(photos)),
                unavailable_attachment_count_hint=int(
                    getattr(vk_fetch, "unavailable_attachment_count", 0) or 0
                ),
                db=db,
            )
            drafts = _adapt_vk_draft_result(
                drafts,
                source_text=text or "",
                attachment_count=int(
                    getattr(vk_fetch, "attachment_count", 0) or len(photos)
                ),
            )
            _tmark("build_drafts_total", time.monotonic() - t0)
            decision = getattr(drafts, "decision", None)
            if decision is None:
                # The adapter above owns every legacy shape.  Reaching this
                # branch means the builder violated the boundary contract.
                drafts = _adapt_vk_draft_result(
                    None,
                    source_text=text or "",
                    attachment_count=int(
                        getattr(vk_fetch, "attachment_count", 0) or len(photos)
                    ),
                )
                decision = drafts.decision
            manifest = getattr(decision, "evidence_manifest", None)
            provider_attempts = list(getattr(decision, "provider_attempts", ()) or ())
            if not provider_attempts:
                provider_attempts = [{}]
            decision_reason = (
                str(getattr(getattr(decision, "retry_reason", None), "value", ""))
                or None
            )
            final_parse_result = (
                drafts.to_receipt_payload()
                if hasattr(drafts, "to_receipt_payload") and not decision_reason
                else None
            )
            verification_reasons = [
                str(getattr(item, "value", item))
                for item in (getattr(decision, "verification_reasons", ()) or ())
            ]
            for attempt_index, provider_attempt in enumerate(provider_attempts):
                is_final_attempt = attempt_index == len(provider_attempts) - 1
                retry_after_ms = int(
                    provider_attempt.get("provider_retry_after_ms", 0) or 0
                )
                attempt_error = provider_attempt.get("error_type")
                finish_reason = provider_attempt.get("finish_reason")
                provider_completed = bool(
                    finish_reason
                    or provider_attempt.get("actual_total_tokens")
                    or provider_attempt.get("output_tokens")
                ) and not attempt_error
                await vk_review.record_source_parse_attempt(
                    db,
                    source_packet_id=getattr(post, "source_packet_id", None),
                    prompt_version=PARSE_VERSION,
                    model=str(provider_attempt.get("model") or model_name),
                    evidence_manifest=(
                        manifest.to_payload() if manifest is not None else {}
                    ),
                    parse_result=(final_parse_result if is_final_attempt else None),
                    disposition=(
                        str(getattr(decision.disposition, "value", decision.disposition))
                        if is_final_attempt
                        else SourceDisposition.RETRY_REQUIRED.value
                    ),
                    retry_reason=(
                        decision_reason
                        if is_final_attempt
                        else SourceParseRetryReason.MALFORMED_JSON.value
                    ),
                    event_child_count=(len(drafts or ()) if is_final_attempt else 0),
                    lifecycle_action_count=(
                        len(getattr(decision, "lifecycle_actions", ()) or ())
                        if is_final_attempt
                        else 0
                    ),
                    no_event_reason=(
                        str(getattr(decision.no_event_reason, "value", decision.no_event_reason))
                        if is_final_attempt and decision.no_event_reason is not None
                        else None
                    ),
                    quota_scope=provider_attempt.get("quota_scope"),
                    request_id=provider_attempt.get("request_id"),
                    response_id=provider_attempt.get("response_id"),
                    finish_reason=finish_reason,
                    input_tokens=provider_attempt.get("input_tokens"),
                    output_tokens=provider_attempt.get("output_tokens"),
                    thought_tokens=provider_attempt.get("thought_tokens"),
                    reserved_tokens=provider_attempt.get("reserved_tokens"),
                    provider_retry_after=(
                        int(math.ceil(retry_after_ms / 1000))
                        if retry_after_ms
                        else None
                    ),
                    attempt_kind=str(provider_attempt.get("attempt_kind") or "primary"),
                    llm_started=True,
                    llm_completed=(
                        provider_completed
                        or bool(final_parse_result and is_final_attempt)
                    ),
                    structured_response_valid=bool(
                        final_parse_result and is_final_attempt
                    ),
                    verification_triggered=bool(verification_reasons),
                    verification_reason=(
                        ",".join(verification_reasons) or None
                    ),
                    verification_disposition=(
                        str(getattr(decision.disposition, "value", decision.disposition))
                        if verification_reasons and is_final_attempt
                        else None
                    ),
                )
        except Exception as exc:
            retry_after_ms = int(getattr(exc, "retry_after_ms", 0) or 0)
            status_code = int(getattr(exc, "status_code", 0) or 0)
            typed_reason = "RATE_LIMITED" if (status_code == 429 or retry_after_ms) else "TECHNICAL_ERROR"
            await vk_review.record_source_parse_attempt(
                db,
                source_packet_id=getattr(post, "source_packet_id", None),
                prompt_version=PARSE_VERSION,
                model=model_name,
                evidence_manifest={"evidence_complete": False},
                parse_result=None,
                disposition="RETRY_REQUIRED",
                retry_reason=typed_reason,
                event_child_count=0,
                lifecycle_action_count=0,
                provider_retry_after=(int(retry_after_ms / 1000) if retry_after_ms else None),
            )
            _state, attempts = await vk_review.schedule_retry(
                db,
                int(post.id),
                typed_reason=typed_reason,
                batch_id=batch_id,
                retry_after_sec=(retry_after_ms / 1000 if retry_after_ms else None),
                provider_retry_after=(int(retry_after_ms / 1000) if retry_after_ms else None),
            )
            report.inbox_deferred += 1
            report.errors.append(f"retry_scheduled {source_url}: {typed_reason}: {exc}")
            await _emit_progress(
                "⏳",
                [
                    "Результат: технический повтор запланирован",
                    f"Причина: {typed_reason}",
                    f"Попытка: {attempts}",
                    f"took_sec: {(time.monotonic() - start_ts):.1f}",
                ],
            )
            _log_row_timing(drafts_count=0, ok_value=False)
            return

    decision = getattr(drafts, "decision", None)
    if decision is None:  # defensive compatibility for corrupted in-memory state
        drafts = _adapt_vk_draft_result(
            drafts,
            source_text=text or "",
            attachment_count=int(
                getattr(vk_fetch, "attachment_count", 0) or len(photos)
            ),
        )
        decision = drafts.decision

    if decision.disposition is SourceDisposition.RETRY_REQUIRED:
        reason = str(getattr(getattr(decision, "retry_reason", None), "value", "RETRY_REQUIRED"))
        latest_provider_attempt = (
            dict((getattr(decision, "provider_attempts", ()) or ())[-1])
            if getattr(decision, "provider_attempts", ())
            else {}
        )
        retry_after_ms = int(
            latest_provider_attempt.get("provider_retry_after_ms", 0) or 0
        )
        await vk_review.schedule_retry(
            db,
            int(post.id),
            typed_reason=reason,
            batch_id=batch_id,
            retry_after_sec=(retry_after_ms / 1000 if retry_after_ms else None),
            quota_scope=latest_provider_attempt.get("quota_scope"),
            provider_retry_after=(
                int(math.ceil(retry_after_ms / 1000)) if retry_after_ms else None
            ),
        )
        report.inbox_deferred += 1
        report.errors.append(f"source_retry {source_url}: {reason}")
        await vk_review.record_carrier_resolution(
            db,
            source_packet_id=getattr(post, "source_packet_id", None),
            child_outcomes=[],
            terminal_carrier_outcome="RETRY_SCHEDULED",
            typed_error_reason=reason,
        )
        return

    lifecycle_event_ids: list[int] = []
    lifecycle_unresolved: list[str] = []
    for action in tuple(getattr(decision, "lifecycle_actions", ()) or ()):
        action_evidence = "\n".join(
            value for value in (text, getattr(action, "evidence", "")) if value
        )
        event_id, error = await _cancel_matching_event_from_notice(
            db,
            notice_text=action_evidence,
            source_url=source_url,
            source_name=source_name_val,
            location_hint=location_hint_val,
            published_at=published_at,
            lifecycle_action=action,
        )
        if event_id is not None:
            lifecycle_event_ids.append(int(event_id))
        else:
            lifecycle_unresolved.append(error or "lifecycle_no_match")

    if exact_parse_replay:
        await vk_review.record_exact_parse_replay(
            db,
            source_packet_id=getattr(post, "source_packet_id", None),
            prompt_version=PARSE_VERSION,
            model=model_name,
        )
        logger.info("vk_auto: exact successful source parse replay packet=%s", getattr(post, "source_packet_id", None))

    if not drafts:
        if lifecycle_unresolved:
            reason = lifecycle_unresolved[0]
            await vk_review.schedule_retry(
                db,
                int(post.id),
                typed_reason="LIFECYCLE_NO_MATCH",
                batch_id=batch_id,
            )
            report.inbox_deferred += 1
            report.errors.append(f"lifecycle_retry {source_url}: {reason}")
            await vk_review.record_carrier_resolution(
                db,
                source_packet_id=getattr(post, "source_packet_id", None),
                child_outcomes=[],
                terminal_carrier_outcome="RETRY_SCHEDULED",
                typed_error_reason="LIFECYCLE_NO_MATCH",
            )
            return
        if decision.disposition is SourceDisposition.LIFECYCLE_ONLY and lifecycle_event_ids:
            report.inbox_imported += 1
            report.updated_event_ids.extend(lifecycle_event_ids)
            await vk_review.mark_imported_events(
                db,
                inbox_id=int(post.id),
                batch_id=batch_id,
                operator_id=operator_id,
                event_ids=lifecycle_event_ids,
                event_dates=[],
            )
            await vk_review.mark_carrier_outcome(
                db, inbox_id=int(post.id), outcome="LIFECYCLE_RESOLVED"
            )
            await vk_review.record_carrier_resolution(
                db,
                source_packet_id=getattr(post, "source_packet_id", None),
                child_outcomes=["LIFECYCLE_APPLIED" for _ in lifecycle_event_ids],
                terminal_carrier_outcome="LIFECYCLE_RESOLVED",
            )
            return
        if (
            decision.disposition is SourceDisposition.CONFIRMED_NO_EVENT
            and bool(getattr(decision, "evidence_complete", False))
            and isinstance(getattr(decision, "no_event_reason", None), SourceNoEventReason)
        ):
            report.inbox_rejected += 1
            no_event_reason = decision.no_event_reason.value
            await vk_review.mark_rejected(
                db,
                int(post.id),
                no_event_reason=no_event_reason,
            )
            await vk_review.record_carrier_resolution(
                db,
                source_packet_id=getattr(post, "source_packet_id", None),
                child_outcomes=[],
                terminal_carrier_outcome="CONFIRMED_NO_EVENT",
                typed_error_reason=f"CONFIRMED_NO_EVENT:{no_event_reason}",
            )
            await _emit_progress(
                "⏭️",
                [
                    "Результат: LLM подтвердил отсутствие события",
                    f"took_sec: {(time.monotonic() - start_ts):.1f}",
                ],
            )
            return
        await vk_review.schedule_retry(
            db,
            int(post.id),
            typed_reason="EVIDENCE_INCOMPLETE",
            batch_id=batch_id,
        )
        report.inbox_deferred += 1
        await vk_review.record_carrier_resolution(
            db,
            source_packet_id=getattr(post, "source_packet_id", None),
            child_outcomes=[],
            terminal_carrier_outcome="RETRY_SCHEDULED",
            typed_error_reason="EVIDENCE_INCOMPLETE",
        )
        return

    # Deterministic warnings have no authority to delete a positive LLM child
    # or its siblings.

    # If LLM returned drafts without location, use the source-level hint as a fallback.
    # This prevents Smart Update from rejecting otherwise valid events due to missing location.
    if (location_hint_val or "").strip():
        for draft in drafts:
            if not (getattr(draft, "venue", None) or "").strip():
                draft.venue = str(location_hint_val).strip()

    imported_event_ids: list[int] = []
    imported_event_dates: list[str | None] = []
    created_ids: list[int] = []
    updated_ids: list[int] = []
    added_posters_total = 0
    added_posters_by_event_id: dict[int, int] = {}
    partial_error: str | None = None
    smart_retry_reasons: list[str] = []
    semantic_rejections: list[str] = []
    child_outcomes: list[str] = []
    inline_jobs_enabled = (os.getenv("VK_AUTO_IMPORT_INLINE_JOBS", "1") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    ok = True
    persist_total_sec = 0.0
    for producer_ordinal, draft in enumerate(drafts):
        try:
            t0 = time.monotonic()
            res = await vk_intake.persist_event_and_pages(
                draft,
                photos,
                db,
                source_post_url=source_url,
                wait_for_telegraph_url=not inline_jobs_enabled,
                producer_ordinal=producer_ordinal,
            )
            took_one = time.monotonic() - t0
            persist_total_sec += float(took_one)

            smart_result = res.smart_result
            if smart_result is None:
                raise RuntimeError("persist boundary returned no typed Smart Update result")
            if smart_result.is_rejected:
                child_outcomes.append("CONFIRMED_PRODUCT_EXCLUSION")
                semantic_rejections.append(
                    smart_result.reason or "unspecified_product_policy"
                )
                continue
            if smart_result.is_retry:
                child_outcomes.append("RETRY_SCHEDULED")
                # A roundup contains independent candidates. Keep the durable
                # retry for this child while allowing valid siblings to finish.
                smart_retry_reasons.append(
                    smart_result.reason or "smart_update_retry"
                )
                continue
            if not smart_result.is_accepted or res.event_id is None:
                raise RuntimeError("invalid typed Smart Update result at VK boundary")

            imported_event_ids.append(int(res.event_id))
            child_outcomes.append(str(smart_result.outcome.value))
            imported_event_dates.append(res.event_date)
            if smart_result.outcome is SmartUpdateTerminalOutcome.CREATED:
                created_ids.append(int(res.event_id))
            else:
                updated_ids.append(int(res.event_id))
            added = int(getattr(res, "smart_added_posters", 0) or 0)
            added_posters_total += added
            added_posters_by_event_id[int(res.event_id)] = added
        except Exception as exc:
            ok = False
            exc_txt = str(exc)
            report.errors.append(f"persist_failed {source_url}: {exc_txt}")
            if not imported_event_ids:
                report.inbox_failed += 1
                await vk_review.schedule_retry(db, int(post.id), typed_reason="PERSIST_ERROR", batch_id=batch_id)
                await _emit_progress(
                    "❌",
                    [
                        "Результат: ошибка сохранения (persist)",
                        f"Причина: {_shorten_reason(exc_txt) or '—'}",
                        f"took_sec: {(time.monotonic() - start_ts):.1f}",
                    ],
                )
                _log_row_timing(drafts_count=len(drafts or []), ok_value=False)
                return
            # Partial success: keep already imported events linked to this inbox row.
            partial_error = exc_txt
            ok = True
            break
    if drafts:
        _tmark("persist_total", persist_total_sec)

    smart_retry_reason = smart_retry_reasons[0] if smart_retry_reasons else None
    if smart_retry_reason and not imported_event_ids:
        report.inbox_deferred += 1
        report.errors.append(f"retry_scheduled {source_url}: {smart_retry_reason}")
        await vk_review.mark_deferred(
            db,
            int(post.id),
            batch_id=batch_id,
            retry_after_sec=_partial_import_retry_sec(),
        )
        await _emit_progress(
            "⏳",
            [
                "Результат: Smart Update запланировал автоматический повтор",
                f"Причина: {_shorten_reason(smart_retry_reason) or 'transient'}",
                f"took_sec: {(time.monotonic() - start_ts):.1f}",
            ],
        )
        _log_row_timing(drafts_count=len(drafts or []), ok_value=True)
        return

    if semantic_rejections and not imported_event_ids:
        report.inbox_rejected += 1
        await vk_review.mark_carrier_outcome(
            db,
            inbox_id=int(post.id),
            outcome="CONFIRMED_PRODUCT_EXCLUSION",
            typed_reason=semantic_rejections[0],
        )
        await _emit_progress(
            "⏭️",
            [
                "Результат: Smart Update отклонил все события подборки",
                f"Причина: {_shorten_reason(semantic_rejections[0]) or '—'}",
                f"Отклонено карточек: {len(semantic_rejections)}",
                f"took_sec: {(time.monotonic() - start_ts):.1f}",
            ],
        )
        _log_row_timing(drafts_count=len(drafts or []), ok_value=False)
        return

    if not ok:
        _log_row_timing(drafts_count=len(drafts or []), ok_value=False)
        return

    t0 = time.monotonic()
    await vk_review.mark_imported_events(
        db,
        inbox_id=post.id,
        batch_id=batch_id,
        operator_id=operator_id,
        event_ids=imported_event_ids,
        event_dates=imported_event_dates,
    )
    _tmark("mark_imported_events", time.monotonic() - t0)
    report.updated_event_ids.extend(lifecycle_event_ids)
    enrichment_retry = (
        "EVIDENCE_INCOMPLETE"
        if not bool(getattr(decision, "evidence_complete", False))
        else None
    )
    lifecycle_retry = lifecycle_unresolved[0] if lifecycle_unresolved else None
    durable_retry_reason = smart_retry_reason or partial_error or lifecycle_retry or enrichment_retry
    if durable_retry_reason:
        await vk_review.mark_deferred(
            db,
            int(post.id),
            batch_id=batch_id,
            retry_after_sec=_partial_import_retry_sec(),
            typed_reason=(
                "LIFECYCLE_NO_MATCH" if lifecycle_retry
                else "EVIDENCE_INCOMPLETE" if enrichment_retry
                else "SMART_UPDATE_RETRY" if smart_retry_reason
                else "PERSIST_ERROR"
            ),
        )
        report.inbox_deferred += 1
        await vk_review.record_carrier_resolution(
            db,
            source_packet_id=getattr(post, "source_packet_id", None),
            child_outcomes=child_outcomes,
            terminal_carrier_outcome="RETRY_SCHEDULED",
            typed_error_reason=str(durable_retry_reason),
        )
    else:
        if exact_parse_replay and child_outcomes and all(
            outcome == "NOOP_EXACT_REPLAY" for outcome in child_outcomes
        ):
            carrier_outcome = "EXACT_REPLAY"
        elif lifecycle_event_ids:
            carrier_outcome = "MIXED_RESOLVED"
        else:
            carrier_outcome = "EVENTS_RESOLVED"
        await vk_review.mark_carrier_outcome(
            db,
            inbox_id=int(post.id),
            outcome=carrier_outcome,
        )
        await vk_review.record_carrier_resolution(
            db,
            source_packet_id=getattr(post, "source_packet_id", None),
            child_outcomes=child_outcomes,
            terminal_carrier_outcome=carrier_outcome,
        )
        report.inbox_imported += 1
    report.created_event_ids.extend(created_ids)
    report.updated_event_ids.extend(updated_ids)

    created_cnt = len(created_ids)
    updated_cnt = len(updated_ids)
    if created_cnt and not updated_cnt:
        icon = "✅"
    elif updated_cnt and not created_cnt:
        icon = "🔄"
    else:
        icon = "✅🔄"

    ids_preview = ", ".join(str(x) for x in (imported_event_ids[:5] or []))
    extra_lines = [
        f"Smart Update: ✅{created_cnt} 🔄{updated_cnt}",
        f"event_ids: {ids_preview}{'…' if len(imported_event_ids) > 5 else ''}",
        f"Иллюстрации: +{added_posters_total}",
        "Отчёт Smart Update: ⏳",
    ]
    if semantic_rejections:
        extra_lines.insert(0, f"⚠️ Отклонено независимых карточек: {len(semantic_rejections)}")
    effective_retry_reason = durable_retry_reason
    if effective_retry_reason:
        extra_lines.insert(
            0,
            "⚠️ Частично (автоматический повтор запланирован): "
            f"{_shorten_reason(effective_retry_reason) or 'transient'}",
        )
    await _emit_progress(icon, extra_lines)

    if inline_jobs_enabled:
        timeout_sec = float(os.getenv("VK_AUTO_IMPORT_INLINE_JOBS_TIMEOUT_SEC", "90") or "90")
        t0 = time.monotonic()
        try:
            # Inline jobs exist only to make the operator report reflect the final
            # public URLs right away. ICS publishing can be slow / flaky (and in
            # local E2E it may be intentionally misconfigured), so we do NOT wait
            # for it by default.
            allowed = {
                main_mod.JobTask.telegraph_build,
                main_mod.JobTask.tg_event_publish,
            }

            include_ics_inline = (os.getenv("VK_AUTO_IMPORT_INLINE_INCLUDE_ICS") or "").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            disable_ics_jobs = (os.getenv("DISABLE_ICS_JOBS") or "").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            if include_ics_inline and not disable_ics_jobs:
                allowed.add(main_mod.JobTask.ics_publish)
            for eid in imported_event_ids:
                await asyncio.wait_for(
                    main_mod.run_event_update_jobs(
                        db,
                        bot,
                        event_id=int(eid),
                        allowed_tasks=allowed,
                    ),
                    timeout=timeout_sec,
                )
        except asyncio.TimeoutError:
            logger.warning(
                "vk_auto: inline event update jobs timeout source=%s events=%s timeout_sec=%s",
                source_url,
                imported_event_ids,
                timeout_sec,
            )
        except Exception:
            logger.exception(
                "vk_auto: inline event update jobs failed source=%s events=%s",
                source_url,
                imported_event_ids,
            )
        _tmark("inline_jobs", time.monotonic() - t0)

    # Send the unified report after inline Telegraph/ICS jobs so the operator sees
    # the final (potentially recreated) Telegraph URL, not the stale snapshot value.
    t0 = time.monotonic()
    report_sent = await _send_unified_event_report(
        db,
        bot,
        chat_id,
        created=created_ids,
        updated=updated_ids,
        source_url=source_url,
        added_posters_by_event_id=added_posters_by_event_id,
        post_metrics=metrics,
        post_popularity=post_popularity,
    )
    _tmark("send_unified_report", time.monotonic() - t0)
    extra_lines[-1] = f"Отчёт Smart Update: {'✅' if report_sent else '⚠️'}"
    extra_lines.append(f"took_sec: {(time.monotonic() - start_ts):.1f}")
    await _emit_progress(icon, extra_lines)
    _log_row_timing(drafts_count=len(drafts or []), ok_value=True)


async def vk_auto_import_scheduler(
    db: Database,
    bot: Any | None = None,
    *,
    run_id: str | None = None,
) -> None:
    """Scheduled job entrypoint: imports VK inbox queue when enabled.

    The report goes to ADMIN chat because there is no operator context.
    """
    if os.getenv("ENABLE_VK_AUTO_IMPORT", "").strip().lower() not in {"1", "true", "yes"}:
        return
    logger.info("vk_auto.scheduler.entry run_id=%s bot=%s", run_id, bool(bot))
    ops_run_id = await start_ops_run(
        db,
        kind="vk_auto_import",
        trigger="scheduled",
        operator_id=0,
        details={
            "run_id": run_id,
            "scheduler_entrypoint": "vk_auto_import",
        },
    )
    if not bot:
        await _record_vk_auto_import_scheduler_skip(
            db,
            ops_run_id=ops_run_id,
            run_id=run_id,
            reason="missing_bot",
        )
        return
    try:
        chat_id = await resolve_superadmin_chat_id(db)
        if not chat_id:
            await _record_vk_auto_import_scheduler_skip(
                db,
                ops_run_id=ops_run_id,
                run_id=run_id,
                reason="missing_superadmin_chat",
            )
            return
        limit = int(os.getenv("VK_AUTO_IMPORT_LIMIT", "15") or "15")
        await run_vk_auto_import(
            db,
            bot,
            chat_id=chat_id,
            limit=limit,
            operator_id=0,
            trigger="scheduled",
            run_id=run_id,
            ops_run_id=ops_run_id,
        )
    except Exception as exc:
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="error",
            details={
                "run_id": run_id,
                "scheduler_entrypoint": "vk_auto_import",
                "fatal_error": f"{type(exc).__name__}: {exc}",
            },
        )
        logger.exception("vk_auto.scheduler failed run_id=%s", run_id)
