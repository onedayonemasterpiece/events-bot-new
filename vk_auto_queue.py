from __future__ import annotations

from linked_events import source_programme_batch

import asyncio
import copy
import json
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
from heavy_ops import current_heavy_meta, heavy_operation
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
from vk_source_envelope import (
    build_vk_source_envelope,
    is_vk_source_envelope,
    vk_source_envelope_replayability,
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


def _vk_auto_rate_limit_wait_sec() -> float:
    """Bound one explicit provider Retry-After inside the current row claim."""

    raw = (os.getenv("VK_AUTO_IMPORT_RATE_LIMIT_MAX_WAIT_SEC") or "60").strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = 60.0
    return max(0.0, min(value, 180.0))


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
    from sqlalchemy import or_, select
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
    kind = "перенос" if is_postponed or action_name.startswith("RESCHEDULE_") else "отмена"
    next_status = (
        "rescheduled"
        if action_name.startswith("RESCHEDULE_")
        else "postponed"
        if is_postponed
        else "cancelled"
    )

    async with db.get_session() as session:
        eligible_statuses = ["active"]
        if action_name == "CANCEL":
            eligible_statuses.append("cancelled")
        elif action_name == "POSTPONE":
            eligible_statuses.append("postponed")
        stmt = select(Event).where(Event.lifecycle_status.in_(eligible_statuses))
        if date_hint:
            candidate_dates = [date_hint]
            if action_name == "RESCHEDULE_DATE":
                new_date_hint = str(getattr(lifecycle_action, "new_date", "") or "").strip()
                if new_date_hint and new_date_hint not in candidate_dates:
                    candidate_dates.append(new_date_hint)
            stmt = stmt.where(
                or_(*(Event.date.like(f"{candidate_date}%") for candidate_date in candidate_dates))
            )
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
                title_contains = (
                    title_hint.casefold() in ev_title.casefold()
                    or ev_title.casefold() in title_hint.casefold()
                )
                ev_tokens = _title_tokens(ev_title)
                overlap = len(title_tokens & ev_tokens) if title_tokens else 0
                # A typed title is an identity anchor, not an optional scoring
                # bonus.  Exact location alone previously let lifecycle replay
                # mutate unrelated events after the real target had already
                # moved date or reached a terminal state.
                if not title_contains and overlap == 0:
                    continue
                if title_contains:
                    score += 5
                score += min(6, overlap * 2)
            scored.append((score, ev))

        if not scored:
            return None, f"title_mismatch title_hint={title_hint!r} date={date_hint or ''}"
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
                "vk_auto.cancel context legacy_unresolved_terminal event_id=%s reason=%s",
                best_id,
                context_result.reason,
            )
        elif context_result.is_failed_technical:
            logger.error(
                "vk_auto.cancel context failed_technical event_id=%s reason=%s",
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


def _extract_media_urls(item: Mapping[str, Any], *, limit: int = 12) -> list[str]:
    """Compatibility facade over the shared recursive envelope builder."""

    owner_signed = int(item.get("owner_id") or 0)
    return list(
        build_vk_source_envelope(
            item,
            owner_id=abs(owner_signed),
            owner_type="user" if owner_signed > 0 else "group",
            media_limit=limit,
        ).get("photos")
        or ()
    )


def _iter_vk_video_payloads(value: Any):
    """Yield mutable VK video payloads from a wall item and nested reposts."""

    if isinstance(value, dict):
        if value.get("type") == "video" and isinstance(value.get("video"), dict):
            yield value["video"]
        for child in value.values():
            yield from _iter_vk_video_payloads(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_vk_video_payloads(child)


async def _refresh_vk_video_evidence(
    main_mod: Any, item: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[tuple[int, int], str]]:
    """Resolve expiring video evidence through VK's canonical video.get.

    ``wall.getById`` can return stale signed OK CDN URLs even for a currently
    readable post.  VK's object schema exposes ``image``/``first_frame`` but
    does not promise durable URLs, so refresh every referenced video once,
    inside the same bounded carrier invocation, before building the evidence
    envelope.  Failure keeps the original payload and therefore remains
    fail-closed at the ordinary evidence boundary.
    """

    refreshed_item = copy.deepcopy(dict(item))
    payloads = list(_iter_vk_video_payloads(refreshed_item))
    refs: list[str] = []
    for payload in payloads:
        try:
            owner_id = int(payload.get("owner_id"))
            video_id = int(payload.get("id"))
        except (TypeError, ValueError):
            continue
        ref = f"{owner_id}_{video_id}"
        access_key = str(payload.get("access_key") or "").strip()
        if access_key:
            ref = f"{ref}_{access_key}"
        refs.append(ref)
    if not refs:
        return refreshed_item, {}
    try:
        # Keep autonomous video reads on the service credential. A service
        # response may omit playable files; preview evidence remains available
        # without putting the publishing user token under reader load.
        response = await main_mod.vk_api(
            "video.get",
            videos=",".join(dict.fromkeys(refs)),
        )
    except Exception as exc:
        logger.warning("vk_auto: video.get evidence refresh failed refs=%d err=%s", len(refs), exc)
        return refreshed_item, {}
    raw_items = response.get("items") if isinstance(response, Mapping) else None
    if not isinstance(raw_items, list):
        return refreshed_item, {}
    by_identity: dict[tuple[int, int], Mapping[str, Any]] = {}
    for fresh in raw_items:
        if not isinstance(fresh, Mapping):
            continue
        try:
            by_identity[(int(fresh.get("owner_id")), int(fresh.get("id")))] = fresh
        except (TypeError, ValueError):
            continue
    refreshed = 0
    video_files: dict[tuple[int, int], str] = {}
    for payload in payloads:
        try:
            fresh = by_identity.get((int(payload.get("owner_id")), int(payload.get("id"))))
        except (TypeError, ValueError):
            fresh = None
        if fresh is None:
            continue
        # Merge rather than replace: short-video responses sometimes omit the
        # semantic title while still returning the refreshed preview families.
        payload.update(dict(fresh))
        files = fresh.get("files") if isinstance(fresh.get("files"), Mapping) else {}
        file_url = next(
            (
                str(files[key]).strip()
                for key in ("mp4_144", "mp4_240", "mp4_360")
                if isinstance(files.get(key), str) and str(files.get(key)).strip()
            ),
            "",
        )
        if file_url:
            video_files[(int(payload.get("owner_id")), int(payload.get("id")))] = file_url
        refreshed += 1
    if refreshed:
        logger.info("vk_auto.video_refresh refs=%d refreshed=%d playable=%d actor=service", len(refs), refreshed, len(video_files))
    return refreshed_item, video_files


@dataclass(frozen=True)
class VkFetchStatus:
    ok: bool
    kind: Literal["ok", "not_found", "access_denied", "vk_api_error", "network_error"]
    error_code: int | None = None
    error: str | None = None
    attachment_count: int = 0
    unavailable_attachment_count: int = 0
    source_envelope: dict[str, Any] | None = None
    source_replayed: bool = False
    source_unavailable: bool = False
    video_urls: tuple[str, ...] = ()


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
    owner_type: str = "group",
) -> tuple[str, list[str], datetime | None, dict[str, Any] | None, VkFetchStatus]:
    """Fetch VK wall post (text + image URLs) via VK API.

    Uses `main.vk_api` so it can read via service token when configured.
    """
    import main as main_mod

    from vk_owner import normalize_owner_type, signed_owner_id

    normalized_owner_type = normalize_owner_type(owner_type)
    owner_signed = signed_owner_id(group_id, normalized_owner_type)
    try:
        resp = await main_mod.vk_api(
            "wall.getById", posts=f"{owner_signed}_{int(post_id)}"
        )
    except Exception as exc:
        # NOTE: VKAPIError is defined in main.py. We inspect it dynamically to avoid
        # a hard import cycle and still keep error codes for decision-making.
        code = None
        msg = str(exc or "").strip()
        kind: Literal[
            "ok", "not_found", "access_denied", "vk_api_error", "network_error"
        ] = "network_error"
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
            "vk_auto: wall.getById failed owner=%s post=%s kind=%s code=%s err=%s",
            owner_signed,
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

    if not items:
        return "", [], None, None, VkFetchStatus(False, "not_found", error="empty_response")

    # wall.getById is a one-post method. If a compatibility response contains
    # several items, prefer the requested id and otherwise use the first.
    item = next(
        (
            candidate
            for candidate in items
            if int(candidate.get("id") or 0) == int(post_id)
        ),
        items[0],
    )
    item, video_files = await _refresh_vk_video_evidence(main_mod, item)
    envelope = build_vk_source_envelope(
        item,
        owner_id=int(group_id),
        owner_type=normalized_owner_type,
        post_id=int(post_id),
        source_url=_vk_wall_url(group_id, post_id, normalized_owner_type),
        media_limit=limit,
    )
    published_at: datetime | None = None
    try:
        published_at = datetime.fromtimestamp(
            float(envelope.get("published_at") or 0), tz=timezone.utc
        )
    except (OSError, TypeError, ValueError, OverflowError):
        published_at = None
    counts = envelope.get("counts") if isinstance(envelope.get("counts"), Mapping) else {}
    visual_count = int(counts.get("visual_candidate_count") or 0)
    unavailable_visual = int(counts.get("unavailable_visual_count") or 0)
    omitted_media = int(counts.get("omitted_media_count") or 0)
    inventory = envelope.get("attachment_inventory")
    resolved_video_previews: set[str] = set()
    if isinstance(inventory, list):
        for attachment in inventory:
            if not isinstance(attachment, Mapping) or attachment.get("type") != "video":
                continue
            ids = attachment.get("ids") if isinstance(attachment.get("ids"), Mapping) else {}
            try:
                identity = (int(ids.get("owner_id")), int(ids.get("id")))
            except (TypeError, ValueError):
                continue
            if identity in video_files and isinstance(attachment.get("preview_url"), str):
                resolved_video_previews.add(str(attachment["preview_url"]))
    image_urls = [
        str(value)
        for value in (envelope.get("photos") or ())
        if str(value) not in resolved_video_previews
    ]
    return (
        str(envelope.get("text") or ""),
        image_urls,
        published_at,
        dict(envelope.get("metrics") or {}) or None,
        VkFetchStatus(
            True,
            "ok",
            # EvidenceManifest counts visual/OCR candidates, not every
            # semantic nonvisual attachment retained in the inventory.
            attachment_count=visual_count + unavailable_visual,
            unavailable_attachment_count=unavailable_visual + omitted_media,
            source_envelope=envelope,
            video_urls=tuple(video_files.values()),
        ),
    )


def _vk_auto_fetch_inline_attempts() -> int:
    raw = (os.getenv("VK_AUTO_IMPORT_FETCH_INLINE_ATTEMPTS") or "2").strip()
    try:
        value = int(raw)
    except Exception:
        value = 2
    return max(1, min(value, 3))


async def _fetch_vk_post_with_inline_retry(
    group_id: int,
    post_id: int,
    **kwargs: Any,
) -> tuple[str, list[str], datetime | None, dict[str, Any] | None, VkFetchStatus]:
    """Retry only transient VK transport failures inside the current row call."""

    attempts = _vk_auto_fetch_inline_attempts()
    result: tuple[
        str, list[str], datetime | None, dict[str, Any] | None, VkFetchStatus
    ] | None = None
    for attempt_no in range(1, attempts + 1):
        result = await fetch_vk_post_text_and_photos(group_id, post_id, **kwargs)
        status = result[4]
        if status.ok or status.kind not in {"network_error", "vk_api_error"}:
            return result
        if attempt_no < attempts:
            logger.warning(
                "vk_auto: transient fetch failure; bounded inline retry "
                "group=%s post=%s attempt=%s/%s kind=%s",
                group_id,
                post_id,
                attempt_no,
                attempts,
                status.kind,
            )
            await asyncio.sleep(min(1.0, 0.25 * attempt_no))
    assert result is not None
    return result


async def _load_vk_durable_packet_evidence(
    db: Database,
    *,
    group_id: int,
    post_id: int,
    source_packet_id: int | None,
    media_limit: int,
) -> dict[str, Any]:
    """Load the immutable packet for source-unavailable processing.

    A legacy text/photos projection is detectable but not lossless enough to
    support a terminal negative decision.  Only a complete v1 envelope is
    returned as parseable evidence.
    """

    async with db.raw_conn() as conn:
        row = None
        if source_packet_id is not None:
            row = await (await conn.execute(
                """
                SELECT id,raw_text,raw_payload_json,attachment_metadata_json,published_at
                FROM vk_source_packet WHERE id=?
                """,
                (int(source_packet_id),),
            )).fetchone()
        if row is None:
            row = await (await conn.execute(
                """
                SELECT id,raw_text,raw_payload_json,attachment_metadata_json,published_at
                FROM vk_source_packet
                WHERE source_type='vk' AND owner_id=? AND post_id=?
                ORDER BY revision DESC LIMIT 1
                """,
                (int(group_id), int(post_id)),
            )).fetchone()
    if row is None:
        return {"replayability": "unavailable"}
    try:
        from vk_packet_storage import decode_packet_json

        raw_payload = json.loads(decode_packet_json(row[2] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        raw_payload = {}
    replayability = vk_source_envelope_replayability(raw_payload)
    if replayability != "replayable_lossless" or not is_vk_source_envelope(raw_payload):
        return {
            "packet_id": int(row[0]),
            "replayability": "replayable_legacy_incomplete",
        }

    candidates = [
        item
        for item in (raw_payload.get("all_media_candidates") or ())
        if isinstance(item, Mapping) and str(item.get("url") or "").strip()
    ]
    selected = candidates[: max(0, int(media_limit))]
    counts = raw_payload.get("counts") if isinstance(raw_payload.get("counts"), Mapping) else {}
    unavailable_visual = int(counts.get("unavailable_visual_count") or 0)
    omitted = max(0, len(candidates) - len(selected))
    published_at: datetime | None = None
    try:
        published_at = datetime.fromtimestamp(float(row[4] or 0), tz=timezone.utc)
    except (OSError, TypeError, ValueError, OverflowError):
        published_at = None
    status = VkFetchStatus(
        True,
        "ok",
        attachment_count=len(candidates) + unavailable_visual,
        unavailable_attachment_count=omitted + unavailable_visual,
        source_envelope=dict(raw_payload),
        source_replayed=True,
        source_unavailable=True,
    )
    return {
        "packet_id": int(row[0]),
        "replayability": replayability,
        "text": str(raw_payload.get("text") or row[1] or ""),
        "photos": [str(item.get("url")) for item in selected],
        "published_at": published_at,
        "metrics": dict(raw_payload.get("metrics") or {}) or None,
        "status": status,
    }


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
    inbox_failed_technical: int = 0
    inbox_deferred: int = 0
    skipped_requeued: int = 0
    cancelled: bool = False
    created_event_ids: list[int] = field(default_factory=list)
    updated_event_ids: list[int] = field(default_factory=list)
    inbox_ids: list[int] = field(default_factory=list)
    source_urls: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def terminal_rows(self) -> int:
        return int(self.inbox_imported + self.inbox_rejected + self.inbox_failed)

    @property
    def unresolved_rows(self) -> int:
        return max(0, int(self.inbox_processed - self.terminal_rows))


async def _terminalize_vk_auto_failure(
    db: Database,
    *,
    post: Any,
    report: VkAutoImportReport,
    typed_reason: str,
    source_url: str,
    child_outcomes: Sequence[str] | None = None,
    error_detail: str | None = None,
) -> None:
    """Close one operator-visible auto-import row without a durable retry.

    Crawl continuation has its own infrastructure retry state before a carrier
    enters this product batch.  Once ``/vk_auto_import`` claims a row, however,
    the owner-facing contract is linear: success, proved product exclusion, or
    a visible technical terminal that requires an explicit operator re-drive.
    """

    reason = str(typed_reason or "FAILED_TECHNICAL").strip() or "FAILED_TECHNICAL"
    await vk_review.mark_carrier_outcome(
        db,
        inbox_id=int(post.id),
        outcome="FAILED_TECHNICAL",
        typed_reason=reason,
    )
    await vk_review.record_carrier_resolution(
        db,
        source_packet_id=getattr(post, "source_packet_id", None),
        child_outcomes=list(child_outcomes) if child_outcomes is not None else None,
        terminal_carrier_outcome="FAILED_TECHNICAL",
        typed_error_reason=reason,
    )
    report.inbox_failed += 1
    report.inbox_failed_technical += 1
    detail = _shorten_reason(error_detail)
    report.errors.append(
        f"failed_technical {source_url}: {reason}{f': {detail}' if detail else ''}"
    )


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
    fetch_kwargs: dict[str, Any] = {
        "db": db,
        "bot": bot,
        "limit": _vk_auto_import_photo_limit_for_text(getattr(post, "text", None)),
    }
    post_owner_type = str(getattr(post, "owner_type", None) or "group")
    # Keep the default group call shape compatible with rolling workers/tests;
    # personal pages must explicitly carry their positive signed owner id.
    if post_owner_type == "user":
        fetch_kwargs["owner_type"] = "user"
    fetched_text, photos, published_at, metrics, vk_fetch = await _fetch_vk_post_with_inline_retry(
        post.group_id, post.post_id, **fetch_kwargs
    )
    stage["vk_fetch_post"] = float(time.monotonic() - t0)
    allow_stale = _vk_auto_allow_stale_inbox_text()
    text = ""
    if vk_fetch.ok:
        text = (fetched_text or "").strip()
    elif allow_stale and vk_fetch.kind not in {"not_found", "access_denied"}:
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
    try:
        limit_int = int(limit)
    except Exception:
        limit_int = 25
    unbounded = limit_int <= 0

    start = time.time()
    total_estimate = None
    batch_error: BaseException | None = None
    try:
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
        await vk_review.release_stale_locks(db)
        await vk_review.release_due_deferred(db, batch_id=batch_id)

        def _env_enabled(name: str, default: bool) -> bool:
            raw = (os.getenv(name) or "").strip().lower()
            if not raw:
                return default
            return raw in {"1", "true", "yes", "on"}

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

        heavy_mode = _vk_auto_import_heavy_mode(trigger)
        waiting_meta = current_heavy_meta() if heavy_mode != "off" else None
        if waiting_meta is not None and heavy_mode == "wait":
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
            # Infrastructure admission belongs to the batch, not to a source
            # terminal. Check after waiting for the heavy gate, before any claim or
            # prefetch, so an unsafe volume cannot burn through pending carriers.
            try:
                vk_intake._require_vk_crawl_storage_headroom(db)
            except RuntimeError as exc:
                report.errors.append(str(exc))
                await finish_ops_run(
                    db,
                    run_id=ops_run_id,
                    status="error",
                    metrics={"inbox_processed": 0, "inbox_failed_technical": 0},
                    details={
                        "batch_id": batch_id,
                        "run_id": run_id,
                        "skip_reason": "storage_admission",
                        "errors": list(report.errors),
                    },
                )
                logger.warning("vk_auto: batch storage admission blocked: %s", exc)
                try:
                    await bot.send_message(
                        chat_id,
                        "⏸ VK auto import: недостаточно доступного места; "
                        "очередь не обработана и сохранена для следующего запуска.\n"
                        f"{exc}",
                        disable_web_page_preview=True,
                    )
                except Exception:
                    logger.warning("vk_auto: storage_notice_send_failed", exc_info=True)
                return report

            prefetch_enabled = _env_enabled("VK_AUTO_IMPORT_PREFETCH", False)

            async def _await_prefetch(task: asyncio.Task | None) -> VkInboxPrefetch | None:
                if task is None:
                    return None
                try:
                    res = await task
                except asyncio.CancelledError:
                    current_task = asyncio.current_task()
                    if current_task is not None and current_task.cancelling():
                        raise
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
                    try:
                        await _terminalize_vk_auto_failure(
                            db,
                            post=post_obj,
                            report=report,
                            typed_reason="ROW_TIMEOUT",
                            source_url=source_url,
                            error_detail=f"row timed out after {row_timeout_sec:.1f}s",
                        )
                    except Exception:
                        logger.exception("vk_auto: terminalization failed after timeout")
                        raise
                    logger.warning(
                        "vk_auto: inbox row timeout id=%s url=%s timeout_sec=%.1f",
                        getattr(post_obj, "id", None),
                        source_url,
                        row_timeout_sec,
                    )
                except Exception as exc:
                    try:
                        await _terminalize_vk_auto_failure(
                            db,
                            post=post_obj,
                            report=report,
                            typed_reason="UNEXPECTED_ERROR",
                            source_url=source_url,
                            error_detail=str(exc),
                        )
                    except Exception:
                        logger.exception("vk_auto: terminalization failed after exception")
                        raise
                    logger.exception(
                        "vk_auto: unexpected exception in inbox row processing id=%s url=%s",
                        getattr(post_obj, "id", None),
                        source_url,
                    )

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

                    report.inbox_processed += 1
                    source_url = _vk_wall_url(
                        post.group_id, post.post_id, getattr(post, "owner_type", None) or "group"
                    )
                    report.inbox_ids.append(int(post.id))
                    report.source_urls.append(source_url)

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

                    remaining -= 1
                    report.inbox_processed += 1
                    source_url = _vk_wall_url(
                        post.group_id, post.post_id, getattr(post, "owner_type", None) or "group"
                    )
                    report.inbox_ids.append(int(post.id))
                    report.source_urls.append(source_url)

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

    except (Exception, asyncio.CancelledError) as exc:
        batch_error = exc
        report.cancelled = report.cancelled or isinstance(exc, asyncio.CancelledError)
        report.errors.append(
            f"{'batch_cancelled' if report.cancelled else 'batch_failed'}: {type(exc).__name__}: "
            f"{_shorten_reason(str(exc)) or 'interrupted'}"
            + (f"; last_source_url={report.source_urls[-1]}" if report.source_urls else "")
        )
        logger.exception("vk_auto: batch interrupted batch_id=%s", batch_id)

    took = time.time() - start
    total_txt = str(int(total_estimate)) if isinstance(total_estimate, int) else "?"
    terminal_rows = report.terminal_rows
    unresolved_rows = report.unresolved_rows
    if unresolved_rows:
        balance_error = (
            "terminal_balance_violation "
            f"processed={report.inbox_processed} terminal={terminal_rows} "
            f"unresolved={unresolved_rows}"
        )
        report.errors.append(balance_error)
        logger.error("vk_auto: %s", balance_error)
    terminal_status = (
        "canceled"
        if report.cancelled
        else "failed"
        if unresolved_rows > 0
        else "partial"
        if (batch_error is not None or report.inbox_failed > 0) and report.inbox_processed > report.inbox_failed
        else "failed"
        if batch_error is not None or report.inbox_failed > 0
        else "success"
    )
    terminal_label = {
        "success": "успех", "partial": "частично",
        "failed": "ошибка", "canceled": "отменён",
    }[terminal_status]
    summary = (
        f"🏁 VK auto import завершён: {terminal_label}\n"
        f"batch: {batch_id}\n"
        f"limit: {'all' if unbounded else limit_int}\n"
        f"include_skipped: {1 if include_skipped else 0}\n"
        f"cancelled: {1 if report.cancelled else 0}\n"
        f"queue processed: {report.inbox_processed}/{total_txt}\n"
        f"inbox imported: {report.inbox_imported}\n"
        f"inbox rejected: {report.inbox_rejected}\n"
        f"inbox failed: {report.inbox_failed}\n"
        f"Smart Update technical terminals (без автоматического повтора): {report.inbox_failed_technical}\n"
        f"inbox deferred: {report.inbox_deferred}\n"
        f"terminal balance: {terminal_rows}/{report.inbox_processed}\n"
        f"events created: {len(set(report.created_event_ids))}\n"
        f"events updated: {len(set(report.updated_event_ids))}\n"
        f"took_sec: {took:.1f}"
    )
    if report.errors:
        summary += "\n\nОшибки:\n" + "\n".join(report.errors)
    # One logical report for the whole queue run; /log keeps event details
    # available without replaying a separate notification for every source.
    event_rows: dict[int, tuple] = {}
    source_rows: dict[int, tuple] = {}
    event_ids = sorted(set(report.created_event_ids + report.updated_event_ids))
    bot_username = await _resolve_bot_username(bot) if event_ids else None
    try:
        async with db.raw_conn() as conn:
            if event_ids:
                cur = await conn.execute(
                    "SELECT id,title,telegraph_url,tg_event_post_url FROM event WHERE id IN ("
                    + ",".join("?" for _ in event_ids) + ")", event_ids,
                )
                event_rows = {int(row[0]): row[1:] for row in await cur.fetchall()}
            if report.inbox_ids:
                cur = await conn.execute(
                    "SELECT id,status,last_typed_reason FROM vk_inbox WHERE id IN ("
                    + ",".join("?" for _ in report.inbox_ids) + ")", report.inbox_ids,
                )
                source_rows = {int(row[0]): row[1:] for row in await cur.fetchall()}
    except Exception:
        logger.warning("vk_auto: summary_details_read_failed", exc_info=True)
    for label, ids in (
        ("Созданы", report.created_event_ids), ("Обновлены", report.updated_event_ids)
    ):
        if ids:
            summary += f"\n\n{label}:"
        for eid in dict.fromkeys(ids):
            title, telegraph_url, tg_url = event_rows.get(eid, (f"ID {eid}", None, None))
            summary += f"\n{eid}: {title}"
            for url in dict.fromkeys(url for url in (telegraph_url, tg_url) if url):
                summary += f"\n{url}"
            summary += f"\n{_log_deeplink(bot_username, eid) or f'/log {eid}'}"
    if report.source_urls:
        summary += "\n\nИсточники:"
        for inbox_id, url in zip(report.inbox_ids, report.source_urls):
            status, reason = source_rows.get(inbox_id, ("unknown", None))
            summary += f"\n{url} — {status}" + (f": {reason}" if reason else "")
    try:
        from main import split_text

        for part in split_text(summary):
            await bot.send_message(chat_id, part, parse_mode=None, disable_web_page_preview=True)
    except Exception:
        logger.exception("vk_auto: failed to send summary")

    _clear_vk_auto_import_cancel(chat_id=chat_id, operator_id=operator_id)
    await finish_ops_run(
        db,
        run_id=ops_run_id,
        status=terminal_status,
        metrics={
            "inbox_processed": int(report.inbox_processed),
            "inbox_imported": int(report.inbox_imported),
            "inbox_rejected": int(report.inbox_rejected),
            "inbox_failed": int(report.inbox_failed),
            "inbox_failed_technical": int(report.inbox_failed_technical),
            "inbox_deferred": int(report.inbox_deferred),
            "terminal_rows": int(terminal_rows),
            "unresolved_rows": int(unresolved_rows),
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
            "terminal_rows": terminal_rows,
            "unresolved_rows": unresolved_rows,
            "duration_sec": round(float(took), 3),
        },
    )
    if isinstance(batch_error, asyncio.CancelledError):
        raise batch_error
    return report


@source_programme_batch
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
        fetch_kwargs: dict[str, Any] = {
            "db": db,
            "bot": bot,
            "limit": _vk_auto_import_photo_limit_for_text(getattr(post, "text", None)),
        }
        post_owner_type = str(getattr(post, "owner_type", None) or "group")
        if post_owner_type == "user":
            fetch_kwargs["owner_type"] = "user"
        fetched_text, photos, published_at, metrics, vk_fetch = await _fetch_vk_post_with_inline_retry(
            post.group_id, post.post_id, **fetch_kwargs
        )
        _tmark("vk_fetch_post", time.monotonic() - t0)
        allow_stale = _vk_auto_allow_stale_inbox_text()
        if vk_fetch.ok:
            text = (fetched_text or "").strip()
        elif allow_stale and vk_fetch.kind not in {"not_found", "access_denied"}:
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
        durable = await _load_vk_durable_packet_evidence(
            db,
            group_id=int(post.group_id),
            post_id=int(post.post_id),
            source_packet_id=getattr(post, "source_packet_id", None),
            media_limit=_vk_auto_import_photo_limit_for_text(getattr(post, "text", None)),
        )
        replayability = str(durable.get("replayability") or "unavailable")
        if replayability == "replayable_lossless":
            post.source_packet_id = int(durable["packet_id"])
            text = str(durable.get("text") or "")
            photos = list(durable.get("photos") or ())
            published_at = durable.get("published_at")
            metrics = durable.get("metrics")
            if published_at is not None:
                publish_ts = int(published_at.timestamp())
            vk_fetch = durable["status"]
            logger.info(
                "vk_auto: source unavailable; replaying complete durable packet gid=%s post=%s packet=%s",
                post.group_id,
                post.post_id,
                post.source_packet_id,
            )
        elif replayability == "replayable_legacy_incomplete":
            # Legacy projections prove that some material once existed but do
            # not prove complete outer/copy/attachment coverage. Never turn
            # that incomplete capture into a terminal semantic negative.
            await _terminalize_vk_auto_failure(
                db,
                post=post,
                report=report,
                typed_reason="EVIDENCE_INCOMPLETE",
                source_url=source_url,
                error_detail="legacy packet lacks complete VK attachment/copy evidence",
            )
            return
        else:
            # An inbox text projection is not a captured revision. With no
            # durable packet, retain the original typed fetch failure path
            # below instead of permitting the optional stale-text switch to
            # create an unrefreshed semantic negative.
            text = ""
            photos = []

    if vk_fetch is not None and not vk_fetch.ok:
        allow_stale = _vk_auto_allow_stale_inbox_text()
        if vk_fetch.kind == "not_found":
            fetch_detail = f"{vk_fetch.error_code or ''} {_shorten_reason(vk_fetch.error) or ''}".strip()
            await _terminalize_vk_auto_failure(
                db,
                post=post,
                report=report,
                typed_reason="EVIDENCE_UNAVAILABLE",
                source_url=source_url,
                error_detail=fetch_detail,
            )
            return
        if not (allow_stale and (text or "").strip()):
            fetch_detail = (
                f"kind={vk_fetch.kind} code={vk_fetch.error_code} err={vk_fetch.error}"
            )
            await _terminalize_vk_auto_failure(
                db,
                post=post,
                report=report,
                typed_reason="SOURCE_FETCH_ERROR",
                source_url=source_url,
                error_detail=fetch_detail,
            )
            return

    if isinstance(metrics, dict) and metrics:
        try:
            from source_parsing.post_metrics import (
                compute_age_day,
                normalize_age_day,
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
    fetched_envelope = getattr(vk_fetch, "source_envelope", None)
    if (
        vk_fetch is not None
        and vk_fetch.ok
        and not vk_fetch.source_replayed
        and is_vk_source_envelope(fetched_envelope)
    ):
        # Persist every successful provider fetch before any LLM call. The
        # semantic revision hash makes this idempotent for exact/counter-only
        # replay and appends on edits anywhere in the recursive source packet.
        current_packet_id, _is_new = await vk_intake._persist_vk_source_packet(
            db,
            group_id=int(post.group_id),
            owner_type=str(getattr(post, "owner_type", None) or "group"),
            post=dict(fetched_envelope),
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
                rate_limit_max_wait_sec=_vk_auto_rate_limit_wait_sec(),
                parse_gemma_model=model_name,
                attachment_count_hint=int(getattr(vk_fetch, "attachment_count", 0) or len(photos)),
                unavailable_attachment_count_hint=int(
                    getattr(vk_fetch, "unavailable_attachment_count", 0) or 0
                ),
                videos=tuple(getattr(vk_fetch, "video_urls", ()) or ()),
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
            await _terminalize_vk_auto_failure(
                db,
                post=post,
                report=report,
                typed_reason=typed_reason,
                source_url=source_url,
                error_detail=str(exc),
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
        await _terminalize_vk_auto_failure(
            db,
            post=post,
            report=report,
            typed_reason=reason,
            source_url=source_url,
            error_detail="source parse did not reach a complete typed decision",
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
            report.inbox_rejected += 1
            await vk_review.mark_carrier_outcome(
                db,
                inbox_id=int(post.id),
                outcome="LIFECYCLE_NO_MATCH_NOOP",
                typed_reason=f"LIFECYCLE_NO_MATCH_NOOP:{reason}",
            )
            await vk_review.record_carrier_resolution(
                db,
                source_packet_id=getattr(post, "source_packet_id", None),
                child_outcomes=["LIFECYCLE_NO_MATCH_NOOP"],
                terminal_carrier_outcome="LIFECYCLE_NO_MATCH_NOOP",
                typed_error_reason=f"LIFECYCLE_NO_MATCH_NOOP:{reason}",
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
            return
        await _terminalize_vk_auto_failure(
            db,
            post=post,
            report=report,
            typed_reason="EVIDENCE_INCOMPLETE",
            source_url=source_url,
            error_detail="empty source result without complete no-event proof",
        )
        return

    # Deterministic warnings have no authority to delete a positive LLM child
    # or its siblings.

    # A source-wide venue can fill one child, but cannot locate multiple children.
    if len(drafts) == 1 and (location_hint_val or "").strip():
        for draft in drafts:
            if not (getattr(draft, "venue", None) or "").strip():
                draft.venue = str(location_hint_val).strip()

    imported_event_ids: list[int] = []
    imported_event_dates: list[str | None] = []
    partial_error: str | None = None
    smart_retry_reasons: list[str] = []
    semantic_rejections: list[str] = []
    smart_technical_failures: list[str] = []
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
                # Preserve the typed upstream source decision across the VK
                # persist boundary. A specific child extracted from an
                # EVENTS_FOUND/MIXED programme is already an event candidate;
                # Smart Update must not reclassify the whole carrier digest as
                # a product no-event.
                source_disposition=str(
                    getattr(
                        getattr(decision, "disposition", None),
                        "value",
                        getattr(decision, "disposition", None),
                    )
                    or ""
                )
                or None,
                source_parse_version=(
                    str(getattr(decision, "parse_version", "") or "").strip()
                    or None
                ),
                source_evidence_complete=getattr(
                    decision, "evidence_complete", None
                ),
                source_verification_reasons=[
                    str(getattr(reason, "value", reason))
                    for reason in (
                        getattr(decision, "verification_reasons", ()) or ()
                    )
                ],
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
                # Compatibility guard for an old Smart Update caller result.
                # The visible carrier never inherits that durable retry: its
                # unresolved child becomes a typed technical terminal.
                child_outcomes.append("FAILED_TECHNICAL")
                smart_retry_reasons.append(
                    smart_result.reason or "smart_update_retry"
                )
                continue
            if smart_result.is_failed_technical:
                child_outcomes.append("FAILED_TECHNICAL")
                smart_technical_failures.append(
                    smart_result.reason or "smart_update_failed_technical"
                )
                continue
            if not smart_result.is_accepted or res.event_id is None:
                raise RuntimeError("invalid typed Smart Update result at VK boundary")

            imported_event_ids.append(int(res.event_id))
            child_outcomes.append(str(smart_result.outcome.value))
            imported_event_dates.append(res.event_date)
            if smart_result.outcome is SmartUpdateTerminalOutcome.CREATED:
                report.created_event_ids.append(int(res.event_id))
            else:
                report.updated_event_ids.append(int(res.event_id))
            # Preserve completed children before awaiting the next child: row
            # cancellation must not discard their existing receipt links.
            async with db.raw_conn() as conn:
                await conn.execute(
                    "INSERT OR IGNORE INTO vk_inbox_import_event(inbox_id,event_id) VALUES(?,?)",
                    (int(post.id), int(res.event_id)),
                )
                await conn.commit()
            logger.info(
                "vk_auto.child_committed inbox_id=%s event_id=%s outcome=%s",
                post.id, res.event_id, smart_result.outcome.value,
            )
        except Exception as exc:
            ok = False
            exc_txt = str(exc)
            child_outcomes.append("FAILED_TECHNICAL")
            report.errors.append(f"persist_failed {source_url}: {exc_txt}")
            if not imported_event_ids:
                await _terminalize_vk_auto_failure(
                    db,
                    post=post,
                    report=report,
                    typed_reason="PERSIST_ERROR",
                    source_url=source_url,
                    child_outcomes=child_outcomes,
                    error_detail=exc_txt,
                )
                _log_row_timing(drafts_count=len(drafts or []), ok_value=False)
                return
            # Partial success: keep already imported events linked to this inbox row.
            partial_error = exc_txt
            ok = True
            break
        finally:
            await vk_review.record_carrier_resolution(
                db, source_packet_id=getattr(post, "source_packet_id", None),
                child_outcomes=child_outcomes, terminal_carrier_outcome=None,
            )
    if drafts:
        _tmark("persist_total", persist_total_sec)

    smart_retry_reason = smart_retry_reasons[0] if smart_retry_reasons else None
    smart_technical_reason = (
        smart_technical_failures[0] if smart_technical_failures else None
    )
    if smart_technical_reason and not imported_event_ids:
        await _terminalize_vk_auto_failure(
            db,
            post=post,
            report=report,
            typed_reason=smart_technical_reason,
            source_url=source_url,
            child_outcomes=child_outcomes,
        )
        _log_row_timing(drafts_count=len(drafts or []), ok_value=False)
        return
    if smart_retry_reason and not imported_event_ids:
        terminal_reason = f"SMART_UPDATE_UNRESOLVED:{smart_retry_reason}"
        await _terminalize_vk_auto_failure(
            db,
            post=post,
            report=report,
            typed_reason=terminal_reason,
            source_url=source_url,
            child_outcomes=child_outcomes,
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
    if lifecycle_unresolved and imported_event_ids:
        # A mixed carrier may both announce a real new event and mention a
        # cancellation/update for a target absent from our catalogue.  The
        # accepted child remains a complete product success; the unmatched
        # lifecycle action is an explicit terminal no-op, not a technical
        # failure for the entire carrier.
        child_outcomes.extend(
            "LIFECYCLE_NO_MATCH_NOOP" for _ in lifecycle_unresolved
        )
    enrichment_retry = (
        "EVIDENCE_INCOMPLETE"
        if not bool(getattr(decision, "evidence_complete", False))
        else None
    )
    lifecycle_retry = (
        lifecycle_unresolved[0]
        if lifecycle_unresolved and not imported_event_ids
        else None
    )
    terminal_failure_detail = smart_retry_reason or partial_error or lifecycle_retry or enrichment_retry
    if smart_technical_reason:
        await _terminalize_vk_auto_failure(
            db,
            post=post,
            report=report,
            typed_reason=smart_technical_reason,
            source_url=source_url,
            child_outcomes=child_outcomes,
        )
    elif terminal_failure_detail:
        typed_terminal_reason = (
            "SMART_UPDATE_UNRESOLVED" if smart_retry_reason
            else "PERSIST_ERROR" if partial_error
            else "LIFECYCLE_NO_MATCH" if lifecycle_retry
            else "EVIDENCE_INCOMPLETE"
        )
        await _terminalize_vk_auto_failure(
            db,
            post=post,
            report=report,
            typed_reason=typed_terminal_reason,
            source_url=source_url,
            child_outcomes=child_outcomes,
            error_detail=str(terminal_failure_detail),
        )
    else:
        if exact_parse_replay and child_outcomes and all(
            outcome == "NOOP_EXACT_REPLAY" for outcome in child_outcomes
        ):
            carrier_outcome = "EXACT_REPLAY"
        elif lifecycle_event_ids or lifecycle_unresolved:
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
