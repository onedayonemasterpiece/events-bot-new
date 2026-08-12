import asyncio
import hashlib
import html
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from collections import defaultdict
from urllib.parse import urlparse

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError

from db import Database
from event_utils import strip_city_from_address
from festival_grounding import ground_kgd80_festival
from location_reference import (
    find_known_venue_in_text,
    match_known_venue,
    normalise_event_location_from_reference,
)
from markup import tel_href_for_phone_value
from models import (
    Channel,
    Event,
    EventVideoLink,
    EventSource,
    JobOutbox,
    JobStatus,
    JobTask,
    TelegramScannedMessage,
    TelegramSource,
    TelegramSourceForceMessage,
    VideoAsset,
)
from source_parsing.date_utils import normalize_implicit_iso_date_to_anchor
from smart_event_update import (
    EventCandidate,
    PosterCandidate,
    SmartUpdateIntent,
    SmartUpdateResult,
    SmartUpdateTerminalOutcome,
    smart_event_update,
)
from smart_update_identity import canonicalize_identity_url
from telegram_sources import canonicalize_tg_url, normalize_tg_username, parse_tg_post_url
from source_parsing.post_metrics import (
    PopularityBaseline,
    PopularityMarks,
    compute_age_day,
    load_telegram_popularity_baseline,
    normalize_age_day,
    popularity_marks,
    upsert_telegram_post_metric,
)
from source_parse_contract import (
    EvidenceManifest,
    SourceNoEventReason,
    SourceParseDecision,
    SourceParseRetryReason,
    decision_from_provider_payload,
)

logger = logging.getLogger(__name__)

_LONG_EVENT_TYPES = {"выставка", "ярмарка", "exhibition", "fair"}
_EVENT_TYPE_ALIASES = {
    "exhibition": "выставка",
    "fair": "ярмарка",
}

_LONG_EVENT_MAX_PAST_DAYS_WITHOUT_END = 45
_LONG_EVENT_SINGLE_EVENT_HINT_RE = re.compile(
    r"(?iu)\b("
    r"кинопоказ|показ\s+фильм\w*|фильм|"
    r"концерт|стендап|комик|спектакл\w*|"
    r"лекци\w*|встреч\w*|мастер[- ]?класс\w*|"
    r"экскурс\w*|квиз\w*|игр\w*|перформанс\w*"
    r")\b"
)


@dataclass(slots=True)
class TelegramMonitorReport:
    run_id: str | None = None
    generated_at: str | None = None
    sources_total: int = 0
    messages_scanned: int = 0
    messages_skipped: int = 0
    messages_with_events: int = 0
    events_extracted: int = 0
    events_created: int = 0
    events_merged: int = 0
    events_skipped: int = 0  # total non-imported events (past/invalid/rejected/skipped/nochange)
    events_past: int = 0
    events_invalid: int = 0
    events_rejected: int = 0
    events_nochange: int = 0
    events_filtered: int = 0
    events_errored: int = 0
    errors: list[str] = field(default_factory=list)
    created_events: list["TelegramMonitorEventInfo"] = field(default_factory=list)
    merged_events: list["TelegramMonitorEventInfo"] = field(default_factory=list)
    skipped_posts: list["TelegramMonitorSkippedPostInfo"] = field(default_factory=list)
    messages_new: int = 0
    messages_forced: int = 0
    messages_metrics_only: int = 0
    events_extracted_new: int = 0
    events_extracted_metrics_only: int = 0
    metrics_only_posts: list["TelegramMonitorSkippedPostInfo"] = field(default_factory=list)
    popular_posts: list["TelegramMonitorSkippedPostInfo"] = field(default_factory=list)


@dataclass(slots=True)
class TelegramMonitorEventInfo:
    event_id: int
    title: str
    date: str | None
    time: str | None
    source_link: str | None
    telegraph_url: str | None
    ics_url: str | None
    log_cmd: str | None
    fact_stats: dict[str, int] | None
    photo_count: int | None
    added_posters: int | None
    video_count: int | None = None
    added_videos: int | None = None
    metrics: dict[str, Any] | None = None
    source_excerpt: str | None = None
    queue_notes: list[str] = field(default_factory=list)
    popularity: str | None = None
    telegraph_job_status: str | None = None
    vk_post_url: str | None = None


@dataclass(slots=True)
class TelegramMonitorSkippedPostInfo:
    source_username: str
    source_title: str | None
    message_id: int
    source_link: str | None
    status: str  # skipped|partial|filtered
    reason: str | None
    events_extracted: int
    events_imported: int
    skip_breakdown: dict[str, int] = field(default_factory=dict)
    event_titles: list[str] = field(default_factory=list)
    source_excerpt: str | None = None
    metrics: dict[str, Any] | None = None
    popularity: str | None = None


@dataclass(slots=True)
class TelegramMonitorImportProgress:
    stage: str  # start|done
    status: str  # running|done|partial|skipped|metrics_only|filtered|error
    current_no: int
    total_no: int
    source_username: str
    source_title: str | None
    message_id: int
    source_link: str | None
    events_extracted: int = 0
    events_imported: int = 0
    created_events: list["TelegramMonitorEventInfo"] = field(default_factory=list)
    merged_events: list["TelegramMonitorEventInfo"] = field(default_factory=list)
    created_event_ids: list[int] = field(default_factory=list)
    merged_event_ids: list[int] = field(default_factory=list)
    added_posters_total: int = 0
    metrics: dict[str, Any] | None = None
    popularity: str | None = None
    skip_breakdown: dict[str, int] = field(default_factory=dict)
    reason: str | None = None
    took_sec: float | None = None
    report_events_created_total: int = 0
    report_events_merged_total: int = 0
    post_has_video: bool | None = None
    post_posters_total: int | None = None
    post_videos_total: int | None = None
    post_video_status: str | None = None


TelegramMonitorProgressCallback = Callable[[TelegramMonitorImportProgress], Awaitable[None]]


def _dt_to_ts(value: datetime | None) -> int | None:
    if not value:
        return None
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return int(dt.timestamp())
    except Exception:
        return None


async def _is_force_message(db: Database, *, source_id: int, message_id: int) -> bool:
    async with db.get_session() as session:
        row = await session.get(TelegramSourceForceMessage, (int(source_id), int(message_id)))
        return bool(row)


async def _clear_force_message(db: Database, *, source_id: int, message_id: int) -> None:
    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                row = await session.get(TelegramSourceForceMessage, (int(source_id), int(message_id)))
                if not row:
                    return
                await session.delete(row)
                await session.commit()
            return
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)


async def _ensure_force_message(db: Database, *, source_id: int, message_id: int) -> None:
    """Keep an unresolved carrier visible to the normal monitoring scheduler."""

    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                stmt = (
                    sqlite_insert(TelegramSourceForceMessage)
                    .values(source_id=int(source_id), message_id=int(message_id))
                    .on_conflict_do_nothing(
                        index_elements=["source_id", "message_id"]
                    )
                )
                await session.execute(stmt)
                await session.commit()
            return
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)


def _event_telegraph_url(event) -> str | None:
    url = getattr(event, "telegraph_url", None)
    if url:
        return url
    path = getattr(event, "telegraph_path", None)
    if path:
        return f"https://telegra.ph/{path.lstrip('/')}"
    return None


def _build_excerpt(text: str | None, *, max_len: int = 160) -> str | None:
    if not text:
        return None
    cleaned = " ".join(str(text).split())
    if not cleaned:
        return None
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 1].rstrip() + "…"
    return cleaned


def _parse_iso_date(value: str | None) -> date | None:
    raw = str(value or "").split("..", 1)[0].strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception:
        return None


def _candidate_is_long_event(candidate: EventCandidate) -> bool:
    event_type = str(candidate.event_type or "").strip().casefold()
    if not (event_type and event_type in _LONG_EVENT_TYPES):
        return False
    # Some extractors may mislabel single-date events as exhibitions/fairs when a post
    # contains a festival/program context. If the event has an explicit start time and
    # the title clearly looks like a one-off format (film/show/lecture), treat it as
    # a regular event for "past event" skipping.
    time_value = str(candidate.time or "").strip()
    if time_value and time_value not in {"00:00", "0:00"}:
        title = str(candidate.title or "").strip()
        if title and _LONG_EVENT_SINGLE_EVENT_HINT_RE.search(title):
            return False
    return True


def _normalize_event_type(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return _EVENT_TYPE_ALIASES.get(raw.casefold(), raw)


def _coerce_optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    raw = str(value).strip()
    return raw or None


def _ground_extracted_festival(
    festival: Any,
    *,
    source_payloads: tuple[Any, ...],
    source_username: str,
    message_id: int | None,
    title: Any,
    curated_festival_series: str | None = None,
) -> str | None:
    value, dropped = ground_kgd80_festival(
        festival,
        source_evidence=source_payloads,
        curated_festival_series=curated_festival_series,
    )
    if dropped:
        logger.warning(
            "telegram: dropped ungrounded KGD80 festival source=%s message_id=%s title=%r",
            source_username,
            message_id,
            title,
        )
    return value


def _should_skip_past_event_candidate(candidate: EventCandidate, *, today: date | None = None) -> bool:
    """Past single-date events are skipped; long-running events are valid until end_date."""
    now = today or date.today()
    start_date = _parse_iso_date(candidate.date)
    if start_date is None or start_date >= now:
        return False
    if _candidate_is_long_event(candidate):
        end_date = _parse_iso_date(candidate.end_date)
        # Ongoing long events are valid even if start date is in the past.
        if end_date is not None:
            return end_date < now
        # If end_date is missing, keep it only when the start date is recent.
        # This avoids creating/attempting merges for obviously stale or mis-extracted dates (e.g. wrong year).
        if (now - start_date).days <= _LONG_EVENT_MAX_PAST_DAYS_WITHOUT_END:
            return False
    return True


async def _load_latest_source_fact_stats(
    db: Database,
    *,
    event_id: int,
    source_url: str | None,
) -> dict[str, int] | None:
    """Return per-status fact counts for the most recent log batch for (event_id, source_url)."""
    if not source_url:
        return None
    from sqlalchemy import func
    from models import EventSourceFact

    async with db.get_session() as session:
        source = (
            await session.execute(
                select(EventSource).where(
                    EventSource.event_id == int(event_id),
                    EventSource.source_url == str(source_url),
                )
            )
        ).scalar_one_or_none()
        if not source:
            return None
        ts = await session.scalar(
            select(func.max(EventSourceFact.created_at)).where(
                EventSourceFact.event_id == int(event_id),
                EventSourceFact.source_id == int(source.id),
            )
        )
        if not ts:
            return None
        rows = (
            await session.execute(
                select(EventSourceFact.status, func.count())
                .where(
                    EventSourceFact.event_id == int(event_id),
                    EventSourceFact.source_id == int(source.id),
                    EventSourceFact.created_at == ts,
                )
                .group_by(EventSourceFact.status)
            )
        ).all()
    out: dict[str, int] = {}
    for status, cnt in rows:
        key = (str(status or "added")).strip().lower() or "added"
        out[key] = int(cnt or 0)
    return out or None


async def _build_event_info(
    db: Database,
    *,
    event_id: int | None,
    source_link: str | None,
    source_text: str | None,
    metrics: dict[str, Any] | None = None,
    added_posters: int | None = None,
    popularity: str | None = None,
    queue_notes: list[str] | None = None,
) -> TelegramMonitorEventInfo | None:
    if not event_id:
        return None
    from models import Event

    async with db.get_session() as session:
        event = await session.get(Event, event_id)
    if not event:
        return None
    # Best-effort: make Telegraph URL available for operator reports immediately,
    # so event titles can be shown as clickable links.
    if not _event_telegraph_url(event):
        try:
            from source_parsing.handlers import _ensure_telegraph_url  # local helper

            await _ensure_telegraph_url(db, int(event_id))
        except Exception:
            pass
        async with db.get_session() as session:
            event = await session.get(Event, int(event_id))
        if not event:
            return None
    telegraph_job_status = None
    if not _event_telegraph_url(event):
        # Operator-facing hint: if Telegraph wasn't created, show JobOutbox status.
        try:
            from models import JobOutbox, JobTask

            async with db.get_session() as session:
                row = (
                    await session.execute(
                        select(JobOutbox.status, JobOutbox.attempts, JobOutbox.last_error)
                        .where(
                            JobOutbox.event_id == int(event_id),
                            JobOutbox.task == JobTask.telegraph_build,
                        )
                        .order_by(JobOutbox.id.desc())
                        .limit(1)
                    )
                ).first()
            if row:
                status, attempts, last_error = row
                # status may be an enum (JobStatus.error) or a raw string; normalize to the value.
                try:
                    status_raw = status.value  # type: ignore[attr-defined]
                except Exception:
                    status_raw = str(status or "")
                status_s = str(status_raw or "").strip().lower() or "pending"
                if "." in status_s:
                    status_s = status_s.split(".")[-1]
                attempts_i = int(attempts or 0)
                if status_s in {"error", "failed"}:
                    err = str(last_error or "").strip().splitlines()[0] if last_error else ""
                    telegraph_job_status = f"error (attempts={attempts_i}) {err}".strip()
                elif status_s in {"done", "success"}:
                    telegraph_job_status = None
                else:
                    telegraph_job_status = f"{status_s} (attempts={attempts_i})".strip()
        except Exception:
            telegraph_job_status = None
    fact_stats = await _load_latest_source_fact_stats(
        db,
        event_id=int(event_id),
        source_url=source_link,
    )
    photo_count = None
    try:
        raw = getattr(event, "photo_count", None)
        if raw is None:
            urls = getattr(event, "photo_urls", None)
            if isinstance(urls, list):
                raw = len([u for u in urls if (str(u or "").strip())])
        if raw is not None:
            photo_count = int(raw or 0)
    except Exception:
        photo_count = None
    vk_post_url_value: str | None = None
    try:
        raw_vk_url = getattr(event, "source_vk_post_url", None)
        if raw_vk_url and str(raw_vk_url).strip():
            vk_post_url_value = str(raw_vk_url).strip()
    except Exception:
        vk_post_url_value = None
    return TelegramMonitorEventInfo(
        event_id=event_id,
        title=getattr(event, "title", "") or "",
        date=getattr(event, "date", None),
        time=getattr(event, "time", None),
        source_link=source_link,
        telegraph_url=_event_telegraph_url(event),
        ics_url=getattr(event, "ics_url", None),
        log_cmd=f"/log {event_id}",
        fact_stats=fact_stats,
        photo_count=photo_count,
        added_posters=added_posters,
        metrics=metrics,
        source_excerpt=_build_excerpt(source_text),
        queue_notes=list(queue_notes or []),
        popularity=(str(popularity).strip() or None) if popularity else None,
        telegraph_job_status=telegraph_job_status,
        vk_post_url=vk_post_url_value,
    )


async def refresh_telegram_monitor_event_info(
    db: Database,
    info: TelegramMonitorEventInfo,
) -> TelegramMonitorEventInfo:
    """Refresh Telegraph/ICS URLs + latest per-source fact stats for an existing report item."""
    from models import Event

    if not info or not getattr(info, "event_id", None):
        return info
    async with db.get_session() as session:
        event = await session.get(Event, int(info.event_id))
    if event:
        info.title = getattr(event, "title", "") or info.title
        info.date = getattr(event, "date", None)
        info.time = getattr(event, "time", None)
        info.telegraph_url = _event_telegraph_url(event)
        info.ics_url = getattr(event, "ics_url", None)
        try:
            raw = getattr(event, "photo_count", None)
            if raw is None:
                urls = getattr(event, "photo_urls", None)
                if isinstance(urls, list):
                    raw = len([u for u in urls if (str(u or "").strip())])
            if raw is not None:
                info.photo_count = int(raw or 0)
        except Exception:
            pass
        try:
            raw_vk_url = getattr(event, "source_vk_post_url", None)
            info.vk_post_url = (
                str(raw_vk_url).strip()
                if raw_vk_url and str(raw_vk_url).strip()
                else None
            )
        except Exception:
            pass
    info.log_cmd = f"/log {int(info.event_id)}"
    try:
        info.fact_stats = await _load_latest_source_fact_stats(
            db,
            event_id=int(info.event_id),
            source_url=info.source_link,
        )
    except Exception:
        info.fact_stats = info.fact_stats
    return info


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _order_messages_chronologically(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Smart Update must see older posts first, otherwise stale facts from an old post
    # can overwrite fresher event details processed earlier in the same run.
    indexed: list[tuple[int, dict[str, Any]]] = list(enumerate(messages))

    def _key(item: tuple[int, dict[str, Any]]) -> tuple[int, str, int, int]:
        idx, message = item
        dt = _parse_datetime(str(message.get("message_date") or "").strip())
        dt_ts = _dt_to_ts(dt)
        # Put undated posts after dated ones to preserve chronology where possible.
        missing_dt = 1 if dt_ts is None else 0
        source = normalize_tg_username(message.get("source_username")) or ""
        message_id = _to_int(message.get("message_id")) or -1
        ts_value = int(dt_ts) if dt_ts is not None else 2**62
        return (missing_dt, ts_value, source, message_id if message_id >= 0 else idx)

    ordered = [message for _idx, message in sorted(indexed, key=_key)]
    return ordered


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _clean_url(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if re.match(r"(?i)^tg://user\?id=\d+$", raw):
        return raw
    canonical_tg = canonicalize_tg_url(raw)
    if canonical_tg:
        raw = canonical_tg
    if not re.match(r"https?://", raw):
        return None
    if re.match(r"^https?://t\.me/addlist/", raw):
        return None
    return raw


_TRAILING_URL_PUNCT_RE = re.compile(r"[)\],.!?:;]+$", re.U)
_BARE_URL_RE = re.compile(
    r"(?i)\b("
    r"(?:t\.me|telegram\.me|vk\.cc|clck\.ru|timepad\.ru|kassir\.ru|qtickets\.ru|ticketland\.ru|ticketscloud\.com|intickets\.ru)"
    r"/[^\s<>()]+"
    r")"
)


def _coerce_url(value: str | None) -> str | None:
    """
    Ticket/registration links sometimes arrive without scheme (e.g. `clck.ru/xxxx`).
    Coerce to https when it looks like a real URL. This is a soft parse, not a rewrite.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.strip("<>\"' \t\r\n")
    raw = _TRAILING_URL_PUNCT_RE.sub("", raw).strip()
    if not raw:
        return None
    if re.match(r"(?i)^tg://user\?id=\d+$", raw):
        return raw
    if raw.lower().startswith(("http://", "https://")):
        return _clean_url(raw)
    if raw.lower().startswith(("t.me/", "telegram.me/", "vk.cc/", "clck.ru/")):
        return _clean_url(f"https://{raw}")
    m = _BARE_URL_RE.search(raw)
    if m:
        return _clean_url(f"https://{m.group(1)}")
    return None


def _extract_message_link_items(message: dict[str, Any]) -> list[dict[str, str | None]]:
    """Extract best-effort http(s) links with optional labels from the Kaggle payload.

    The payload may contain:
    - links: ["https://..."]
    - links: [{"url": "https://...", "text": "..."}]
    """
    out: list[dict[str, str | None]] = []
    seen: set[str] = set()
    payload = message.get("links")
    items: list[Any] = payload if isinstance(payload, list) else []
    # Backward-compat: older Kaggle outputs accidentally stored message-level links inside
    # posters[*].links. Best-effort pull them up so ticket inference still works on reimports.
    if not items:
        try:
            for p in (message.get("posters") or []):
                if isinstance(p, dict) and isinstance(p.get("links"), list):
                    items.extend(p.get("links") or [])
        except Exception:
            items = items or []
    if not items:
        return []
    for it in items:
        url = None
        text = None
        source = None
        if isinstance(it, str):
            url = it
        elif isinstance(it, dict):
            url = it.get("url") or it.get("link") or it.get("href")
            text = str(it.get("text") or "").strip() or None
            source = str(it.get("source") or "").strip() or None
        url = _coerce_url(str(url or "")) if url else None
        if not url:
            continue
        key = url.lower().rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        out.append({"url": url, "text": text, "source": source})
    return out


def _extract_message_links(message: dict[str, Any]) -> list[str]:
    return [str(item["url"]) for item in _extract_message_link_items(message) if item.get("url")]


def _link_is_ticketish(label: str | None, url: str | None) -> bool:
    text = str(label or "").strip().casefold()
    value = str(url or "").strip().casefold()
    if any(
        marker in text
        for marker in (
            "билет",
            "регист",
            "запис",
            "купить",
            "ticket",
            "register",
            "registration",
        )
    ):
        return True
    return any(
        domain in value
        for domain in (
            "timepad.ru",
            "kassir.ru",
            "qtickets.ru",
            "ticketland.ru",
            "ticketscloud.com",
            "intickets.ru",
        )
    )


def _link_is_explicitly_non_admission(label: str | None, url: str | None) -> bool:
    text = str(label or "").strip().casefold()
    value = str(url or "").strip().casefold()
    if any(
        marker in text
        for marker in (
            "донат",
            "пожертв",
            "поддержать",
            "поддержка проекта",
            "сбор средств",
            "donate",
            "donation",
            "support the project",
        )
    ):
        return True
    return any(domain in value for domain in ("boosty.to", "patreon.com"))


def _ticket_link_is_explicitly_non_admission(
    ticket_link: str | None,
    message_links: list[Any],
) -> bool:
    current = _coerce_url(ticket_link)
    if not current:
        return False
    key = current.casefold().rstrip("/")
    for item in message_links or []:
        if not isinstance(item, dict):
            continue
        url = _coerce_url(item.get("url") or item.get("link") or item.get("href"))
        if not url or url.casefold().rstrip("/") != key:
            continue
        if _link_is_explicitly_non_admission(item.get("text"), url):
            return True
    return False


def _is_more_specific_same_host_url(current: str | None, candidate: str | None) -> bool:
    cur = _coerce_url(current)
    cand = _coerce_url(candidate)
    if not cur or not cand or cur.rstrip("/") == cand.rstrip("/"):
        return False
    try:
        cur_p = urlparse(cur)
        cand_p = urlparse(cand)
    except Exception:
        return False
    if cur_p.netloc.casefold() != cand_p.netloc.casefold():
        return False
    cur_specificity = len((cur_p.path or "").strip("/")) + len(cur_p.query or "")
    cand_specificity = len((cand_p.path or "").strip("/")) + len(cand_p.query or "")
    return cand_specificity > cur_specificity


def _link_is_public_ticket_candidate(url: str | None) -> bool:
    value = str(url or "").strip().casefold()
    if not value:
        return False
    try:
        parsed = urlparse(value)
    except Exception:
        return True
    host = (parsed.netloc or "").casefold()
    path = (parsed.path or "").casefold()
    query = (parsed.query or "").casefold()
    # Telegram/VK entity extractors expose hashtags as synthetic search URLs;
    # those are navigation noise, not registration/ticket targets.
    if host in {"vk.com", "www.vk.com", "m.vk.com"} and path.startswith("/search/statuses"):
        if "q=%23" in query or "q=#" in query:
            return False
    return True


def _infer_ticket_link_from_message_links(
    message_links: list[Any], *, current: str | None = None
) -> str | None:
    """Pick a likely ticket/registration link from message-level links (best-effort)."""
    if not message_links:
        return None
    normalized: list[dict[str, str | None]] = []
    for item in message_links:
        if isinstance(item, dict):
            url = _coerce_url(item.get("url") or item.get("link") or item.get("href"))
            label = str(item.get("text") or "").strip() or None
        else:
            url = _coerce_url(str(item or ""))
            label = None
        if (
            url
            and _link_is_public_ticket_candidate(url)
            and not _link_is_explicitly_non_admission(label, url)
        ):
            normalized.append({"url": url, "text": label})
    external = [item for item in normalized if "t.me/" not in str(item.get("url") or "").lower()]
    if current:
        ticketish = [item for item in external if _link_is_ticketish(item.get("text"), item.get("url"))]
        if len(ticketish) == 1:
            candidate = ticketish[0].get("url")
            if _is_more_specific_same_host_url(current, candidate):
                return candidate
        return None
    ticketish = [item for item in external if _link_is_ticketish(item.get("text"), item.get("url"))]
    if len(ticketish) == 1:
        return ticketish[0].get("url")
    external_urls = [str(item.get("url") or "") for item in external if item.get("url")]
    # If there are multiple links, only pick when there is a single strong ticket-domain match.
    strong = [u for u in external_urls if _link_is_ticketish(None, u)]
    if len(strong) == 1:
        return strong[0]
    return None


def _extract_urls_from_text(text: str | None) -> list[str]:
    raw = str(text or "")
    if not raw.strip():
        return []
    out: list[str] = []
    seen: set[str] = set()
    # Full http(s) links.
    for u in re.findall(r"https?://[^\s<>()]+", raw, flags=re.I | re.U):
        cu = _coerce_url(u)
        if not cu:
            continue
        key = cu.lower().rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        out.append(cu)
    # Scheme-less links for common shorteners/domains used in posts.
    for m in _BARE_URL_RE.finditer(raw):
        cu = _coerce_url(m.group(1))
        if not cu:
            continue
        key = cu.lower().rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        out.append(cu)
    return out


def _parse_tg_source_url(value: str | None) -> tuple[str | None, int | None]:
    parsed = parse_tg_post_url(value)
    return parsed if parsed else (None, None)


def _normalize_video_status(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    raw = raw.replace(" ", "_")
    allowed = {
        "supabase",
        "uploaded",
        "too_large",
        "bucket_guard",
        "guard_denied",
        "upload_failed",
        "download_failed",
        "unsupported",
        "no_imported_event",
        "multi_event_message",
    }
    if raw in allowed:
        return raw
    if raw.startswith("skipped:"):
        tail = _normalize_video_status(raw.split(":", 1)[1])
        return f"skipped:{tail}" if tail else "skipped"
    return raw[:48]


_VIDEO_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_VIDEO_TERMINAL_STATUSES = {"accepted", "rejected"}


def _video_sha256(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    return raw if _VIDEO_SHA256_RE.fullmatch(raw) else None


def _video_score(value: Any, *, clamp: bool = True) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    if not clamp and not 0.0 <= parsed <= 100.0:
        return None
    return max(0.0, min(100.0, parsed))


def _video_confidence(value: Any) -> float | None:
    """Return a finite model confidence in the canonical ``0..1`` range.

    Unlike visual/relevance scores, confidence is a probability rather than a
    percentage.  Reject malformed values instead of clamping them so a payload
    that accidentally sends ``80`` cannot be persisted as high confidence.
    """

    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or not 0.0 <= parsed <= 1.0:
        return None
    return parsed


def _video_positive_int(value: Any, *, maximum: int) -> int | None:
    parsed = _to_int(value)
    if parsed is None or parsed <= 0:
        return None
    return min(int(parsed), int(maximum))


def _video_duration_seconds(item: dict[str, Any]) -> float | None:
    raw = item.get("duration_seconds")
    if raw is None:
        raw = item.get("duration_sec")
    if raw is None and item.get("duration_ms") is not None:
        try:
            raw = float(item.get("duration_ms")) / 1000.0
        except (TypeError, ValueError):
            raw = None
    try:
        parsed = float(raw) if raw is not None and not isinstance(raw, bool) else None
    except (TypeError, ValueError):
        parsed = None
    if parsed is None or not math.isfinite(parsed) or parsed <= 0:
        return None
    return min(parsed, 24 * 60 * 60.0)


def _video_text(value: Any, *, limit: int) -> str | None:
    raw = str(value or "").strip()
    return raw[:limit] if raw else None


def _video_event_indexes(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    out: list[int] = []
    seen: set[int] = set()
    for raw in value:
        if isinstance(raw, bool):
            continue
        try:
            idx = int(raw)
        except (TypeError, ValueError):
            continue
        if idx < 0 or idx > 999 or idx in seen:
            continue
        seen.add(idx)
        out.append(idx)
    return out


def _video_event_relevance_scores(value: Any) -> list[dict[str, Any]]:
    """Validate event-specific matching decisions without widening them.

    A malformed or out-of-range relationship score is rejected rather than
    clamped: silently turning e.g. 900 into a perfect match would attach media
    to an unrelated event.  General visual scores may still be normalized at
    the payload boundary, but relationship decisions are safety-critical.
    """

    if not isinstance(value, list):
        return []
    by_index: dict[int, dict[str, Any]] = {}
    for raw in value:
        if not isinstance(raw, dict):
            continue
        indexes = _video_event_indexes([raw.get("event_index")])
        if not indexes:
            continue
        relevance = _video_score(
            raw.get("relevance_score", raw.get("event_relevance_score")),
            clamp=False,
        )
        if relevance is None:
            continue
        relation: dict[str, Any] = {
            "event_index": indexes[0],
            "event_relevance_score": relevance,
            "match_reason": _video_text(
                raw.get("reason", raw.get("match_reason")), limit=500
            ),
            "relation_confidence": _video_confidence(
                raw.get("confidence", raw.get("relation_confidence"))
            ),
        }
        by_index[indexes[0]] = relation
    return [by_index[index] for index in sorted(by_index)]


def _video_terminal_status(item: dict[str, Any]) -> str | None:
    raw = _normalize_video_status(item.get("analysis_status") or item.get("status"))
    if raw in {"accepted", "cached_accepted", "cache_hit"}:
        return "accepted"
    if raw in {"rejected", "cached_rejected"}:
        return "rejected"
    # Backward-compatible accepted payload emitted by the old producer.  It is
    # allowed only at the payload boundary; new persisted rows use provider-
    # neutral CDN names and terminal analysis status.
    if raw in {"supabase", "uploaded"}:
        return "accepted"
    return None


def _extract_message_videos_payload(message: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    videos_payload = message.get("videos")
    items = videos_payload if isinstance(videos_payload, list) else []
    out: list[dict[str, Any]] = []
    statuses: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        status = _normalize_video_status(item.get("status"))
        if status:
            statuses.append(status)
        terminal_status = _video_terminal_status(item)
        sha256 = _video_sha256(item.get("sha256"))
        if terminal_status is None or sha256 is None:
            continue
        cdn_url = _clean_url(item.get("cdn_url") or item.get("supabase_url"))
        cdn_path = str(item.get("cdn_path") or item.get("supabase_path") or "").strip().lstrip("/") or None
        cdn_bucket = str(item.get("cdn_bucket") or item.get("storage_bucket") or "").strip() or None
        analysis_json = item.get("analysis_json")
        if not isinstance(analysis_json, dict):
            analysis_json = {}
        # A rejected row without any auditable analysis metadata belongs only in
        # the producer's durable SHA sidecar; do not create an ambiguous core row.
        if terminal_status == "rejected" and not (
            analysis_json
            or str(item.get("analysis_model") or item.get("model") or "").strip()
            or str(item.get("analysis_version") or item.get("prompt_version") or "").strip()
        ):
            continue
        if terminal_status == "accepted" and not (cdn_url or cdn_path):
            continue
        size_bytes = _to_int(item.get("size_bytes"))
        if size_bytes is not None and size_bytes < 0:
            size_bytes = None
        event_indexes = _video_event_indexes(item.get("event_indexes"))
        event_relevance_scores = _video_event_relevance_scores(
            item.get("event_relevance_scores")
        )
        for relation in event_relevance_scores:
            relation_index = int(relation["event_index"])
            if relation_index not in event_indexes:
                event_indexes.append(relation_index)
        raw_status = _normalize_video_status(item.get("analysis_status") or item.get("status"))
        if not event_indexes and raw_status in {"supabase", "uploaded"}:
            event_indexes = [0]
        out.append(
            {
                "analysis_status": terminal_status,
                "cdn_url": cdn_url,
                "cdn_path": cdn_path,
                "cdn_bucket": cdn_bucket,
                "sha256": sha256,
                "size_bytes": size_bytes,
                "mime_type": str(item.get("mime_type") or "").strip() or None,
                "width": _video_positive_int(item.get("width"), maximum=32768),
                "height": _video_positive_int(item.get("height"), maximum=32768),
                "duration_seconds": _video_duration_seconds(item),
                "aesthetic_score": _video_score(item.get("aesthetic_score")),
                "technical_score": _video_score(item.get("technical_score")),
                "showcase_score": _video_score(item.get("showcase_score")),
                # ``event_relevance_scores`` is the canonical M:N contract.
                # Keep the scalar fields only as a one-event compatibility
                # fallback; they are never copied across multiple event links.
                "event_relevance_score": _video_score(
                    item.get("event_relevance_score"), clamp=False
                ),
                "match_reason": _video_text(item.get("match_reason"), limit=500),
                "relation_confidence": _video_confidence(
                    item.get("relation_confidence")
                ),
                "event_relevance_scores": event_relevance_scores,
                "description": _video_text(item.get("description"), limit=4000),
                "search_text": _video_text(item.get("search_text"), limit=8000),
                "analysis_model": _video_text(
                    item.get("analysis_model") or item.get("model"), limit=160
                ),
                "analysis_version": _video_text(
                    item.get("analysis_version") or item.get("prompt_version"), limit=160
                ),
                "analysis_json": analysis_json,
                "analyzed_at": _parse_datetime(item.get("analyzed_at")),
                "event_indexes": event_indexes,
            }
        )
    msg_status = _normalize_video_status(message.get("video_status"))
    if msg_status:
        if msg_status in {
            "accepted",
            "cached_accepted",
            "cache_hit",
            "supabase",
            "uploaded",
        }:
            msg_status = "accepted"
        elif msg_status in {"rejected", "cached_rejected"}:
            msg_status = "rejected"
        return out, msg_status
    if statuses:
        unique = sorted(set(statuses))
        accepted_statuses = {
            "accepted",
            "cached_accepted",
            "cache_hit",
            "supabase",
            "uploaded",
        }
        if any(v in accepted_statuses for v in unique):
            if any(v not in accepted_statuses for v in unique):
                first = next(
                    (v for v in unique if v not in accepted_statuses),
                    None,
                )
                return out, f"partial:{first}" if first else "accepted"
            return out, "accepted"
        return out, f"skipped:{unique[0]}"
    if out:
        return out, "accepted"
    return out, None


async def _persist_event_video_assets(
    db: Database,
    *,
    event_ids_by_index: dict[int, int | set[int] | list[int] | tuple[int, ...]],
    videos: list[dict[str, Any]],
    source_url: str | None,
) -> tuple[int, int, dict[int, int]]:
    if not videos:
        return 0, 0, {}
    for attempt in range(1, 8):
        try:
            inserted_links = 0
            total = 0
            inserted_by_event: dict[int, int] = defaultdict(int)
            async with db.get_session() as session:
                for item in videos:
                    sha256 = _video_sha256(item.get("sha256"))
                    status = str(item.get("analysis_status") or "").strip().lower()
                    if sha256 is None or status not in _VIDEO_TERMINAL_STATUSES:
                        continue
                    total += 1
                    cdn_url = _clean_url(item.get("cdn_url"))
                    cdn_path = str(item.get("cdn_path") or "").strip().lstrip("/") or None
                    cdn_bucket = str(item.get("cdn_bucket") or "").strip() or None
                    if cdn_url and (not cdn_bucket or not cdn_path):
                        try:
                            from supabase_storage import parse_storage_object_url

                            parsed_storage = parse_storage_object_url(cdn_url)
                        except Exception:
                            parsed_storage = None
                        if parsed_storage:
                            parsed_bucket, parsed_path = parsed_storage
                            cdn_bucket = cdn_bucket or parsed_bucket
                            cdn_path = cdn_path or parsed_path

                    now = datetime.now(timezone.utc)
                    insert_values = {
                        "sha256": sha256,
                        "analysis_status": status,
                        "cdn_url": cdn_url if status == "accepted" else None,
                        "cdn_path": cdn_path if status == "accepted" else None,
                        "cdn_bucket": cdn_bucket if status == "accepted" else None,
                        "size_bytes": item.get("size_bytes"),
                        "mime_type": item.get("mime_type"),
                        "width": item.get("width"),
                        "height": item.get("height"),
                        "duration_seconds": item.get("duration_seconds"),
                        "aesthetic_score": item.get("aesthetic_score"),
                        "technical_score": item.get("technical_score"),
                        "showcase_score": item.get("showcase_score"),
                        "description": item.get("description"),
                        "search_text": item.get("search_text"),
                        "analysis_model": item.get("analysis_model"),
                        "analysis_version": item.get("analysis_version"),
                        "analysis_json": item.get("analysis_json") or {},
                        "analyzed_at": item.get("analyzed_at") or now,
                        "orphaned_at": None,
                        "created_at": now,
                        "updated_at": now,
                    }
                    await session.execute(
                        sqlite_insert(VideoAsset)
                        .values(**insert_values)
                        .on_conflict_do_nothing(index_elements=["sha256"])
                    )
                    asset = (
                        await session.execute(
                            select(VideoAsset).where(VideoAsset.sha256 == sha256).limit(1)
                        )
                    ).scalar_one()

                    existing_status = str(asset.analysis_status or "").strip().lower()
                    if existing_status not in _VIDEO_TERMINAL_STATUSES:
                        for key, value in insert_values.items():
                            if key not in {"sha256", "created_at"}:
                                setattr(asset, key, value)
                        session.add(asset)
                        existing_status = status
                    elif existing_status == "accepted" and status == "accepted":
                        # A terminal decision is immutable, but an orphaned binary
                        # may be materialized again without another model call.
                        for key in ("cdn_url", "cdn_path", "cdn_bucket"):
                            value = insert_values.get(key)
                            if value:
                                setattr(asset, key, value)
                        asset.orphaned_at = None
                        asset.updated_at = now
                        session.add(asset)

                    effective_path = str(asset.cdn_path or "").strip().lstrip("/") or None
                    effective_bucket = str(asset.cdn_bucket or "").strip() or None
                    if effective_bucket and effective_path:
                        from sqlalchemy import text

                        await session.execute(
                            text(
                                "DELETE FROM supabase_delete_queue "
                                "WHERE bucket=:bucket AND path=:path"
                            ),
                            {"bucket": effective_bucket, "path": effective_path},
                        )

                    if existing_status != "accepted" or not (asset.cdn_url or asset.cdn_path):
                        continue
                    if asset.id is None:
                        await session.flush()
                    showcase_score = _video_score(asset.showcase_score)
                    relations_by_index = {
                        int(relation["event_index"]): relation
                        for relation in list(item.get("event_relevance_scores") or [])
                        if isinstance(relation, dict)
                        and _video_event_indexes([relation.get("event_index")])
                    }
                    event_indexes = list(item.get("event_indexes") or [])
                    allow_scalar_fallback = len(event_indexes) == 1
                    for event_index in list(item.get("event_indexes") or []):
                        mapped_event_ids = event_ids_by_index.get(int(event_index))
                        if not mapped_event_ids:
                            continue
                        if isinstance(mapped_event_ids, int):
                            target_event_ids = [mapped_event_ids]
                        else:
                            target_event_ids = sorted(
                                {
                                    int(value)
                                    for value in mapped_event_ids
                                    if int(value) > 0
                                }
                            )
                        relation = relations_by_index.get(int(event_index), {})
                        relevance_score = _video_score(
                            relation.get("event_relevance_score"), clamp=False
                        )
                        match_reason = relation.get("match_reason")
                        relation_confidence = _video_confidence(
                            relation.get("relation_confidence")
                        )
                        if not relation and allow_scalar_fallback:
                            relevance_score = _video_score(
                                item.get("event_relevance_score"), clamp=False
                            )
                            match_reason = item.get("match_reason")
                            relation_confidence = _video_confidence(
                                item.get("relation_confidence")
                            )
                        ranking_score = (
                            float(
                                round(
                                    0.75 * showcase_score
                                    + 0.25 * relevance_score
                                )
                            )
                            if showcase_score is not None
                            and relevance_score is not None
                            else None
                        )
                        for event_id in target_event_ids:
                            existing_link = (
                                await session.execute(
                                    select(EventVideoLink).where(
                                        EventVideoLink.event_id == int(event_id),
                                        EventVideoLink.video_asset_id == int(asset.id),
                                    )
                                )
                            ).scalar_one_or_none()
                            if existing_link is None:
                                session.add(
                                    EventVideoLink(
                                        event_id=int(event_id),
                                        video_asset_id=int(asset.id),
                                        event_relevance_score=relevance_score,
                                        ranking_score=ranking_score,
                                        match_reason=match_reason,
                                        relation_confidence=relation_confidence,
                                        source_url=_clean_url(source_url),
                                        created_at=now,
                                    )
                                )
                                await session.flush()
                                inserted_links += 1
                                inserted_by_event[int(event_id)] += 1
                            else:
                                # Idempotent upsert: the relation identity is
                                # stable, while newer source-grounded ranking
                                # evidence replaces stale metadata.
                                existing_link.event_relevance_score = relevance_score
                                existing_link.ranking_score = ranking_score
                                existing_link.match_reason = match_reason
                                existing_link.relation_confidence = relation_confidence
                                existing_link.source_url = _clean_url(source_url)
                                session.add(existing_link)
                await session.commit()
            return inserted_links, total, dict(inserted_by_event)
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)


async def _requeue_event_telegraph_build(db: Database, *, event_id: int) -> None:
    """Ensure Telegraph rebuild runs after attaching new media assets.

    Telegram monitoring may attach videos after Smart Update has already enqueued (or even
    started running) the initial `telegraph_build` job. Re-queueing here guarantees that
    the final Telegraph page reflects newly attached video links.
    """
    try:
        from models import JobOutbox, JobStatus, JobTask

        now = datetime.now(timezone.utc)
        async with db.get_session() as session:
            job = (
                (
                    await session.execute(
                        select(JobOutbox)
                        .where(
                            JobOutbox.event_id == int(event_id),
                            JobOutbox.task == JobTask.telegraph_build,
                        )
                        .order_by(JobOutbox.id.desc())
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )

            status_raw = getattr(job.status, "value", job.status) if job else None
            status_s = str(status_raw or "").strip().lower()
            if job and status_s.endswith("running"):
                # Do not interrupt a running job; schedule a follow-up.
                session.add(
                    JobOutbox(
                        event_id=int(event_id),
                        task=JobTask.telegraph_build,
                        status=JobStatus.pending,
                        updated_at=now,
                        next_run_at=now,
                    )
                )
                await session.commit()
                return

            if job:
                job.status = JobStatus.pending
                job.attempts = 0
                job.last_error = None
                job.updated_at = now
                job.next_run_at = now
                session.add(job)
            else:
                session.add(
                    JobOutbox(
                        event_id=int(event_id),
                        task=JobTask.telegraph_build,
                        status=JobStatus.pending,
                        updated_at=now,
                        next_run_at=now,
                    )
                )
            await session.commit()
    except Exception:
        logger.debug(
            "tg_monitor.videos telegraph requeue failed event_id=%s",
            event_id,
            exc_info=True,
        )


async def _attach_linked_sources(
    db: Database,
    *,
    event_id: int | None,
    linked_urls: list[str] | None,
    trust_level: str | None,
    source_text: str | None = None,
) -> int:
    if not event_id:
        return 0

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in linked_urls or []:
        url = _clean_url(raw)
        if not url:
            continue
        key = url.lower().rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        normalized.append(url)
    if not normalized:
        return 0

    added = 0
    for producer_ordinal, url in enumerate(normalized):
        canonical = canonicalize_identity_url(url)
        if not canonical:
            continue
        username, message_id = _parse_tg_source_url(url)
        result = await smart_event_update(
            db,
            EventCandidate(
                intent=SmartUpdateIntent.ATTACH_CONTEXT,
                target_event_id=int(event_id),
                source_type="telegram",
                source_url=url,
                source_text=source_text or "",
                source_chat_username=username,
                source_message_id=message_id,
                trust_level=trust_level,
                producer_ordinal=producer_ordinal,
                occurrence_key=f"context:{int(event_id)}:{canonical}",
            ),
            check_source_url=False,
            schedule_tasks=False,
        )
        if result.outcome is SmartUpdateTerminalOutcome.MERGED:
            added += 1
        elif result.is_retry:
            logger.warning(
                "tg_monitor.linked_source retry_scheduled event_id=%s url=%s reason=%s",
                event_id,
                url,
                result.reason,
            )
    return added


def _norm_space(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())


def _location_matches(a: str | None, b: str | None) -> bool:
    na = _norm_space(a)
    nb = _norm_space(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if na in nb or nb in na:
        return True
    return False


_LOCATION_EXPLICIT_STOPWORDS = {
    "город",
    "городская",
    "городской",
    "областная",
    "областной",
    "центральная",
    "центральной",
    "информационно",
    "туристический",
    "туристического",
    "пространство",
    "площадка",
    "выставочное",
    "театр",
    "музей",
    "галерея",
    "центр",
    "замок",
    "библиотека",
    "библиотеке",
    "им",
}


def _normalize_location_probe_text(text: str | None) -> str:
    if not text:
        return ""
    raw = str(text).replace("ё", "е").lower()
    raw = re.sub(r"[«»\"'()]", " ", raw)
    raw = re.sub(r"[^\w\s]+", " ", raw, flags=re.UNICODE)
    return re.sub(r"\s+", " ", raw).strip()


def _location_probe_tokens(text: str | None) -> set[str]:
    norm = _normalize_location_probe_text(text)
    if not norm:
        return set()
    out: set[str] = set()
    for tok in norm.split():
        if tok.isdigit():
            out.add(tok)
            continue
        if len(tok) < 4:
            continue
        if tok in _LOCATION_EXPLICIT_STOPWORDS:
            continue
        out.add(tok)
    return out


def _source_text_explicitly_mentions_location(
    source_text: str | None,
    *,
    location_name: str | None,
    location_address: str | None,
) -> bool:
    source_norm = _normalize_location_probe_text(source_text)
    if not source_norm:
        return False

    addr_norm = _normalize_location_probe_text(location_address)
    if addr_norm and len(addr_norm) >= 5 and addr_norm in source_norm:
        return True

    name_norm = _normalize_location_probe_text(location_name)
    if name_norm and len(name_norm) >= 10 and name_norm in source_norm:
        return True

    source_tokens = set(source_norm.split())
    name_hits = _location_probe_tokens(location_name) & source_tokens
    if len(name_hits) >= 2:
        return True

    addr_hits = _location_probe_tokens(location_address) & source_tokens
    if name_hits and addr_hits:
        return True

    return False


_SCHED_LINE_RE = re.compile(r"(^|\s)(\d{1,2})[./](\d{1,2})\s*\|", re.IGNORECASE)
_SCHED_LINE_START_RE = re.compile(r"^\s*\d{1,2}[./]\d{1,2}\s*\|", re.IGNORECASE)
_TIME_TOKEN_RE = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
_EXPLICIT_UNKNOWN_START_TIME_RE = re.compile(
    r"(?iu)\b(?:"
    r"время\s+(?:начала|старта)|"
    r"(?:точное\s+)?(?:начало|старт)"
    r")\s+(?:пока\s+)?(?:уточняется|не\s+определен[оа]?|не\s+известн[оа]?|"
    r"будет\s+(?:уточнен[оа]?|объявлен[оа]?|известн[оа]?))\b"
)

_RECURRING_WORDS_RE = re.compile(
    r"\b(ежедневн|кажд(ый|ая|ое|ую)\s+д(ень|ня)|по\s+будням|кажд(ый|ую)\s+"
    r"(понедельник|вторник|сред(а|у)|четверг|пятниц(а|у)|суббот(а|у)|воскресень(е|я)))\b",
    re.IGNORECASE,
)
_EXCURSION_RE = re.compile(r"\bэкскурс", re.IGNORECASE)
_AD_MARK_RE = re.compile(r"(^|[\\s#])(реклама|ad)\\b", re.IGNORECASE)
_ESOTERICA_RE = re.compile(
    r"\b(таро|астролог|нумеролог|эзотерик|магия|ритуал|расклад|гадани(е|я)|"
    r"чакр(а|ы)|энерг(ия|етик)|рейки|регресси(я|и)|родов(ая|ые)\\s+программ)\\b",
    re.IGNORECASE,
)
_BRIDGE_RE = re.compile(
    r"\b(?:развод(?:ка|ки|ке|ку)?\s+мост(?:ов|ы|а)?|разводк[аеуи]\s+мостов|"
    r"развест[и]\s+мосты|разведут\s+мосты|мосты\s+разведут)\b",
    re.IGNORECASE,
)


def _date_tokens_from_iso(iso_date: str | None) -> list[str]:
    if not iso_date:
        return []
    raw = str(iso_date).split("..", 1)[0].strip()
    if not raw:
        return []
    try:
        y, m, d = raw.split("-", 2)
        mm = int(m)
        dd = int(d)
    except Exception:
        return []
    months_gen = {
        1: "января",
        2: "февраля",
        3: "марта",
        4: "апреля",
        5: "мая",
        6: "июня",
        7: "июля",
        8: "августа",
        9: "сентября",
        10: "октября",
        11: "ноября",
        12: "декабря",
    }
    month_word = months_gen.get(mm, "")
    tokens = [
        f"{dd:02d}.{mm:02d}",
        f"{dd}.{mm}",
        f"{dd:02d}/{mm:02d}",
        f"{dd}/{mm}",
    ]
    # Posters often use "12 июня" (month name) instead of "12.06".
    if month_word:
        tokens.extend(
            [
                f"{dd} {month_word}",
                f"{dd:02d} {month_word}",
                f"{dd}{month_word}",
                f"{dd:02d}{month_word}",
            ]
        )
    return tokens


def _extract_time_tokens(text: str | None) -> list[str]:
    raw = str(text or "")
    out: list[str] = []
    seen: set[str] = set()
    for hh, mm in _TIME_TOKEN_RE.findall(raw):
        token = f"{int(hh):02d}:{mm}"
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _time_is_supported_by_texts(time_value: str | None, *texts: str | None) -> bool:
    raw = str(time_value or "").strip()
    if not raw:
        return False
    wanted = _extract_time_tokens(raw)
    if not wanted:
        return False
    supported: set[str] = set()
    for text in texts:
        supported.update(_extract_time_tokens(text))
    if not supported:
        return False
    return any(token in supported for token in wanted)


def _source_explicitly_leaves_start_time_unknown(*texts: str | None) -> bool:
    """Narrow fail-closed check for an explicitly TBD event start.

    The producer/Smart Update LLM still owns time-role semantics. This guard only
    prevents a concrete time elsewhere in the same post (for example the parent
    festival window) from overwriting an exact source statement that the named
    activity's start is not known yet.
    """

    return any(_EXPLICIT_UNKNOWN_START_TIME_RE.search(str(text or "")) for text in texts)


def _time_value_matches_event_date_marker(time_value: str | None, event_date: str | None) -> bool:
    raw = str(time_value or "").strip()
    event_day = _parse_event_payload_date(event_date)
    if not raw or event_day is None:
        return False
    wanted = _extract_time_tokens(raw)
    if not wanted:
        return False
    return any(token == f"{event_day.day:02d}:{event_day.month:02d}" for token in wanted)


def _source_filters(source: TelegramSource) -> dict[str, Any]:
    value = getattr(source, "filters_json", None)
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}
    return {}


def _looks_like_recurring_excursion(text: str | None, candidate: EventCandidate) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    title = str(candidate.title or "")
    event_type = str(candidate.event_type or "")
    if not (_EXCURSION_RE.search(title) or _EXCURSION_RE.search(event_type)):
        return False
    return bool(_RECURRING_WORDS_RE.search(raw))


def _is_ads_message(text: str | None) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    return bool(_AD_MARK_RE.search(raw))


def _is_esoterica_message(text: str | None) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    return bool(_ESOTERICA_RE.search(raw))


def _is_bridge_notice_message(text: str | None, candidate: EventCandidate) -> bool:
    raw = str(text or "")
    if raw and _BRIDGE_RE.search(raw):
        return True
    title = str(candidate.title or "")
    return bool(title and _BRIDGE_RE.search(title))


def _format_bridge_notice(
    *,
    source_text: str | None,
    source_link: str | None,
    candidate: EventCandidate,
) -> str:
    times = _extract_time_tokens(source_text)
    bridges: list[str] = []
    low = str(source_text or "").lower()
    if "высок" in low:
        bridges.append("Высокий")
    if "юбилейн" in low:
        bridges.append("Юбилейный")
    bridges_part = ", ".join(bridges) if bridges else "мосты"
    date_part = (candidate.date or "").strip()
    pieces: list[str] = [f"Развод мостов: {bridges_part}"]
    if date_part and times:
        pieces.append(f"{date_part} • {', '.join(times[:6])}")
    elif date_part:
        pieces.append(date_part)
    elif times:
        pieces.append(", ".join(times[:6]))
    if source_link:
        pieces.append(f"Источник: {source_link}")
    return "\n".join(pieces).strip()


def _first_photo_url(message: dict[str, Any]) -> str | None:
    posters = message.get("posters") or []
    for item in posters:
        if not isinstance(item, dict):
            continue
        for key in ("catbox_url", "supabase_url"):
            url = str(item.get(key) or "").strip()
            if url.startswith(("http://", "https://")):
                return url
    return None


def _has_poster_urls(posters: list[PosterCandidate] | None) -> bool:
    for p in posters or []:
        if (p.supabase_url or p.catbox_url):
            return True
    return False


def _poster_candidate_key(poster: PosterCandidate) -> str | None:
    sha = str(poster.sha256 or "").strip()
    if sha:
        return f"sha:{sha}"
    url = str(poster.supabase_url or poster.catbox_url or "").strip().lower()
    if url:
        return f"url:{url}"
    return None


def _dedupe_poster_candidates(
    posters: list[PosterCandidate] | None,
    *,
    limit: int = 5,
) -> list[PosterCandidate]:
    out: list[PosterCandidate] = []
    seen: set[str] = set()
    for poster in posters or []:
        key = _poster_candidate_key(poster)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(poster)
        if len(out) >= max(1, int(limit or 1)):
            break
    return out


def _payload_to_poster_candidates(payload: list[dict[str, Any]] | None) -> list[PosterCandidate]:
    out: list[PosterCandidate] = []
    for item in payload or []:
        if not isinstance(item, dict):
            continue
        sha = str(item.get("sha256") or "").strip() or None
        out.append(
            PosterCandidate(
                catbox_url=item.get("catbox_url"),
                supabase_url=item.get("supabase_url"),
                supabase_path=item.get("supabase_path"),
                sha256=sha,
                phash=item.get("phash"),
                ocr_text=item.get("ocr_text"),
                ocr_title=item.get("ocr_title"),
            )
        )
    return _dedupe_poster_candidates(out, limit=8)


def _select_public_page_fallback_posters(
    candidate: EventCandidate,
    posters: list[PosterCandidate] | None,
    *,
    is_single_event_post: bool,
) -> list[PosterCandidate]:
    deduped = _dedupe_poster_candidates(list(posters or []), limit=5)
    if not deduped:
        return []
    if is_single_event_post:
        return deduped
    return _filter_posters_for_event(
        deduped,
        event_title=str(candidate.title or "").strip() or None,
        event_date=str(candidate.date or "").strip() or None,
        event_time=str(candidate.time or "").strip() or None,
    )


def _canonical_tg_post_url(value: str | None) -> str | None:
    username, message_id = _parse_tg_source_url(value)
    uname = normalize_tg_username(username)
    mid = int(message_id or 0)
    if not uname or mid <= 0:
        return None
    return f"https://t.me/{uname}/{mid}"


_TG_PUBLIC_IMAGE_RE = re.compile(
    r"https?://cdn\d+\.telesco\.pe/file/[^\s\"'()<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^\s\"'()<>]+)?",
    re.IGNORECASE,
)
_TG_PUBLIC_MESSAGE_START = '<div class="tgme_widget_message_wrap'
_TG_PUBLIC_MESSAGE_TEXT_RE = re.compile(
    r"<div\s+class=\"tgme_widget_message_text\b[^\"]*\"[^>]*>(.*?)</div>",
    re.IGNORECASE | re.DOTALL,
)
_TG_PUBLIC_PHOTO_WRAP_URL_RE = re.compile(
    r"tgme_widget_message_photo_wrap\b[^>]*?\bstyle=\"[^\"]*?background-image:url\((?:&quot;|\"|')?(https?://cdn\d+\.telesco\.pe/file/[^\")'<>]+)(?:&quot;|\"|')?\)",
    re.IGNORECASE,
)
_TG_PUBLIC_VIDEO_THUMB_URL_RE = re.compile(
    r"tgme_widget_message_video_thumb\b[^>]*?\bstyle=\"[^\"]*?background-image:url\((?:&quot;|\"|')?(https?://cdn\d+\.telesco\.pe/file/[^\")'<>]+)(?:&quot;|\"|')?\)",
    re.IGNORECASE,
)


def _extract_public_tg_message_block(
    html_text: str,
    *,
    username: str,
    message_id: int,
) -> str | None:
    uname = normalize_tg_username(username)
    mid = int(message_id or 0)
    if not uname or mid <= 0:
        return None

    marker = f'data-post="{uname}/{mid}"'
    idx = html_text.find(marker)
    if idx < 0:
        return None

    start_idx = html_text.rfind(_TG_PUBLIC_MESSAGE_START, 0, idx)
    if start_idx < 0:
        start_idx = html_text.rfind('<div class="tgme_widget_message', 0, idx)
    if start_idx < 0:
        start_idx = idx
    end_idx = html_text.find(_TG_PUBLIC_MESSAGE_START, idx + len(marker))
    if end_idx < 0:
        end_idx = len(html_text)

    block = html_text[start_idx:end_idx]
    return block or None


def _extract_photo_urls_from_public_tg_html(
    html_text: str,
    *,
    username: str,
    message_id: int,
    limit: int,
) -> list[str]:
    """Extract message photo URLs from public `t.me/s/...` HTML (single post only).

    Important: do not capture channel avatars (`...user_photo...`) or neighboring posts.
    We only accept URLs from the target post's media blocks (`photo_wrap`/`video_thumb`).
    """
    uname = normalize_tg_username(username)
    mid = int(message_id or 0)
    if not uname or mid <= 0:
        return []

    block = _extract_public_tg_message_block(html_text, username=uname, message_id=mid) or ""
    if not block:
        return []

    # Collect all "poster-like" images from this message block in order of appearance:
    # - normal photos (photo_wrap)
    # - video thumbnails (video_thumb)
    matches: list[tuple[int, str]] = []
    for match in _TG_PUBLIC_PHOTO_WRAP_URL_RE.finditer(block):
        url = str(match.group(1) or "").strip()
        if url:
            matches.append((match.start(), url))
    for match in _TG_PUBLIC_VIDEO_THUMB_URL_RE.finditer(block):
        url = str(match.group(1) or "").strip()
        if url:
            matches.append((match.start(), url))
    matches.sort(key=lambda t: t[0])
    urls = [u for _pos, u in matches if u]

    if not urls:
        return []

    seen: set[str] = set()
    out: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
        if len(out) >= max(0, int(limit or 0)):
            break
    return out


_TG_PUBLIC_ANCHOR_RE = re.compile(
    r"<a\b[^>]*?\bhref=\"([^\"]+)\"[^>]*>(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)


def _public_tg_inner_html_to_text(raw_html: str | None) -> str | None:
    """Convert a small subset of public `t.me/s/...` HTML to readable plain text.

    We keep this best-effort (no dependencies) and focus on:
    - preserving `<br>` as newlines
    - preserving hidden links: `label (url)`
    """
    raw = str(raw_html or "")
    if not raw.strip():
        return None

    # Line breaks first.
    cleaned = re.sub(r"(?i)<br\s*/?>", "\n", raw)

    def _a_repl(m: re.Match[str]) -> str:
        href = html.unescape(str(m.group(1) or "")).strip()
        inner = str(m.group(2) or "")
        inner = re.sub(r"(?s)<[^>]+>", "", inner)
        label = html.unescape(inner).strip()
        if not href:
            return label
        if not label:
            return href
        low = label.strip().lower()
        if href.lower() in low or low.startswith(("http://", "https://", "@")):
            return label
        return f"{label} ({href})"

    cleaned = _TG_PUBLIC_ANCHOR_RE.sub(_a_repl, cleaned)

    # Drop remaining tags and unescape entities.
    cleaned = re.sub(r"(?s)<[^>]+>", "", cleaned)
    cleaned = html.unescape(cleaned)

    cleaned = cleaned.replace("\r", "\n")
    cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned or None


def _extract_message_text_from_public_tg_html(
    html_text: str,
    *,
    username: str,
    message_id: int,
) -> str | None:
    """Extract message text from public `t.me/s/...` HTML (single post only)."""
    uname = normalize_tg_username(username)
    mid = int(message_id or 0)
    if not uname or mid <= 0:
        return None

    block = _extract_public_tg_message_block(html_text, username=uname, message_id=mid) or ""
    if not block:
        return None

    m = _TG_PUBLIC_MESSAGE_TEXT_RE.search(block)
    if not m:
        return None
    inner = str(m.group(1) or "")
    return _public_tg_inner_html_to_text(inner)


def _looks_like_supabase_url(url: str | None) -> bool:
    try:
        from poster_media import is_supabase_storage_url

        return bool(is_supabase_storage_url(url))
    except Exception:
        raw = str(url or "").strip().lower()
        if not raw:
            return False
        return (
            "/storage/v1/object/" in raw
            or "supabase.co/storage/" in raw
            or "storage.yandexcloud.net/" in raw
        )


async def _fallback_fetch_posters_from_public_tg_page(
    *,
    username: str,
    message_id: int,
    limit: int = 3,
    need_ocr: bool = False,
) -> list[PosterCandidate]:
    """Best-effort fallback: scrape poster URLs from public `t.me/s/...` HTML.

    This is used only when the Telegram monitoring payload contains no posters at all.
    It prevents events from being imported without images due to upstream media failures.
    For multi-event posts we can optionally OCR the scraped images, so downstream logic
    can keep image-only posters for every child event while still filtering schedule posters.
    """
    uname = normalize_tg_username(username)
    if not uname or not message_id:
        return []

    try:
        from net import http_call
    except Exception:
        return []

    page_url = f"https://t.me/s/{uname}/{int(message_id)}"
    try:
        resp = await http_call(
            "tg_public_page",
            "GET",
            page_url,
            timeout=15,
            retries=2,
            backoff=1.0,
            headers={"User-Agent": os.getenv("HTTP_IMAGE_UA", "Mozilla/5.0")},
        )
    except Exception:
        return []
    if int(getattr(resp, "status_code", 0) or 0) != 200:
        return []

    try:
        html_text = (getattr(resp, "content", b"") or b"").decode("utf-8", errors="ignore")
    except Exception:
        return []
    if not html_text:
        return []

    urls = _extract_photo_urls_from_public_tg_html(
        html_text,
        username=uname,
        message_id=int(message_id),
        limit=int(limit or 0),
    )
    if not urls:
        return []

    images: list[tuple[bytes, str]] = []
    image_sources: list[str] = []
    from urllib.parse import urlparse
    import os.path as _path

    for i, url in enumerate(urls):
        try:
            img = await http_call(
                "tg_public_img",
                "GET",
                url,
                timeout=20,
                retries=2,
                backoff=1.0,
                headers={"User-Agent": os.getenv("HTTP_IMAGE_UA", "Mozilla/5.0")},
            )
        except Exception:
            continue
        if int(getattr(img, "status_code", 0) or 0) != 200:
            continue
        data = getattr(img, "content", b"") or b""
        if not data:
            continue
        # Keep it safe: poster uploads are intended for small images.
        if len(data) > 8 * 1024 * 1024:
            continue
        try:
            ext = (_path.splitext(urlparse(url).path or "")[1] or "").lower()
        except Exception:
            ext = ""
        if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}:
            ext = ".jpg"
        images.append((data, f"tg_{uname}_{int(message_id)}_{i}{ext}"))
        image_sources.append(url)
        if len(images) >= max(1, min(int(limit or 0), 5)):
            break

    if not images:
        return []

    poster_items: list[Any] = []
    try:
        from poster_media import process_media

        poster_items, _msg = await process_media(
            images,
            need_catbox=True,
            need_ocr=bool(need_ocr),
        )
    except Exception:
        logger.debug(
            "tg_monitor.poster_fallback process_media failed source=%s message_id=%s",
            uname,
            message_id,
            exc_info=True,
        )
        poster_items = []

    posters: list[PosterCandidate] = []
    for item in poster_items or []:
        url = str(getattr(item, "catbox_url", None) or "").strip()
        sha = str(getattr(item, "digest", None) or "").strip() or None
        if not url:
            continue
        common_kwargs = {
            "sha256": sha,
            "ocr_text": getattr(item, "ocr_text", None),
            "ocr_title": getattr(item, "ocr_title", None),
            "prompt_tokens": int(getattr(item, "prompt_tokens", 0) or 0),
            "completion_tokens": int(getattr(item, "completion_tokens", 0) or 0),
            "total_tokens": int(getattr(item, "total_tokens", 0) or 0),
        }
        if _looks_like_supabase_url(url):
            posters.append(PosterCandidate(supabase_url=url, **common_kwargs))
        else:
            posters.append(PosterCandidate(catbox_url=url, **common_kwargs))
    if posters:
        return posters

    # Hard fallback: keep direct CDN URLs when media upload pipeline fails.
    # This is better than importing events without any image.
    direct_posters: list[PosterCandidate] = []
    for idx, src_url in enumerate(image_sources):
        raw = images[idx][0] if idx < len(images) else b""
        digest = hashlib.sha256(raw).hexdigest() if raw else None
        direct_posters.append(PosterCandidate(catbox_url=src_url, sha256=digest))
    return direct_posters


_TG_PUBLIC_TEXT_TRUNCATION_RE = re.compile(r"(?:\u2026|\.\.\.)\s*$")


def _looks_like_truncated_message_text(text: str | None) -> bool:
    raw = str(text or "").strip()
    if len(raw) < 160:
        return False
    return bool(_TG_PUBLIC_TEXT_TRUNCATION_RE.search(raw))


async def _fallback_fetch_full_text_from_public_tg_page(
    *,
    username: str,
    message_id: int,
    max_chars: int = 9000,
) -> str | None:
    """Best-effort fallback: fetch full message text from public `t.me/s/...` HTML.

    Some upstream payloads can include truncated `message["text"]`, which leads to
    missing performer lines and other important details in Smart Update.
    """
    uname = normalize_tg_username(username)
    if not uname or not message_id:
        return None

    try:
        from net import http_call
    except Exception:
        return None

    page_url = f"https://t.me/s/{uname}/{int(message_id)}"
    try:
        resp = await http_call(
            "tg_public_page",
            "GET",
            page_url,
            timeout=15,
            retries=2,
            backoff=1.0,
            headers={"User-Agent": os.getenv("HTTP_IMAGE_UA", "Mozilla/5.0")},
        )
    except Exception:
        return None
    if int(getattr(resp, "status_code", 0) or 0) != 200:
        return None

    try:
        html_text = (getattr(resp, "content", b"") or b"").decode("utf-8", errors="ignore")
    except Exception:
        return None
    if not html_text:
        return None

    extracted = _extract_message_text_from_public_tg_html(
        html_text,
        username=uname,
        message_id=int(message_id),
    )
    if not extracted:
        return None
    out = extracted.strip()
    if max_chars is not None and int(max_chars) > 0:
        out = out[: int(max_chars)]
    return out.strip() or None


async def _collect_linked_source_posters(
    *,
    source_username: str | None,
    source_message_id: int | None,
    linked_urls: list[str] | None,
    message_index: dict[str, dict[str, Any]],
    cache: dict[str, list[PosterCandidate]],
    per_post_limit: int = 2,
    total_limit: int = 5,
) -> list[PosterCandidate]:
    """Best-effort poster enrichment for linked Telegram source URLs.

    Strategy:
    1. Reuse posters from the same `telegram_results.json` if linked message is present.
    2. Otherwise fetch linked post media from public `t.me/s/...` page.
    """
    out: list[PosterCandidate] = []
    visited: set[str] = set()
    total_cap = max(1, int(total_limit or 1))
    per_cap = max(1, int(per_post_limit or 1))

    for raw_url in linked_urls or []:
        canonical = _canonical_tg_post_url(raw_url)
        if not canonical:
            continue
        if canonical in visited:
            continue
        visited.add(canonical)

        linked_posters: list[PosterCandidate] = []
        linked_message = message_index.get(canonical)
        if linked_message:
            linked_posters = _payload_to_poster_candidates(linked_message.get("posters") or [])
            if linked_posters:
                logger.info(
                    "tg_monitor.linked_posters source=%s message_id=%s linked=%s posters=%d via=payload",
                    source_username,
                    source_message_id,
                    canonical,
                    len(linked_posters),
                )

        if not linked_posters:
            cached = cache.get(canonical)
            if cached is None:
                uname, mid = _parse_tg_source_url(canonical)
                try:
                    fetched = await _fallback_fetch_posters_from_public_tg_page(
                        username=str(uname or ""),
                        message_id=int(mid or 0),
                        limit=per_cap,
                    )
                except Exception:
                    logger.debug(
                        "tg_monitor.linked_posters fallback failed source=%s message_id=%s linked=%s",
                        source_username,
                        source_message_id,
                        canonical,
                        exc_info=True,
                    )
                    fetched = []
                cache[canonical] = list(fetched or [])
            linked_posters = list(cache.get(canonical) or [])
            logger.info(
                "tg_monitor.linked_posters source=%s message_id=%s linked=%s posters=%d via=fallback",
                source_username,
                source_message_id,
                canonical,
                len(linked_posters),
            )

        if linked_posters:
            out.extend(linked_posters)
            out = _dedupe_poster_candidates(out, limit=total_cap)
            if len(out) >= total_cap:
                break

    return out


async def _collect_linked_source_texts(
    *,
    source_username: str | None,
    source_message_id: int | None,
    linked_urls: list[str] | None,
    message_index: dict[str, dict[str, Any]],
    cache: dict[str, str],
    total_limit: int = 2,
    max_chars: int = 9000,
    event_date: str | None = None,
    event_title: str | None = None,
) -> list[tuple[str, str, int, str]]:
    """Best-effort text enrichment for linked Telegram source URLs.

    Strategy:
    1. Reuse `message["text"]` from the same `telegram_results.json` if linked message is present.
    2. Otherwise fetch linked post text from public `t.me/s/...` page.
    """
    out: list[tuple[str, str, int, str]] = []
    visited: set[str] = set()
    total_cap = max(0, int(total_limit or 0))
    if total_cap <= 0:
        return []

    for raw_url in linked_urls or []:
        if len(out) >= total_cap:
            break
        canonical = _canonical_tg_post_url(raw_url)
        if not canonical:
            continue
        if canonical in visited:
            continue
        visited.add(canonical)

        if canonical not in cache:
            linked_message = message_index.get(canonical)
            payload_text = (
                str(linked_message.get("text") or "").strip() if isinstance(linked_message, dict) else ""
            )
            via = "payload" if payload_text else "fallback"
            text = payload_text

            if not text or _looks_like_truncated_message_text(text):
                uname, mid = _parse_tg_source_url(canonical)
                if uname and mid:
                    try:
                        fetched = await _fallback_fetch_full_text_from_public_tg_page(
                            username=str(uname),
                            message_id=int(mid),
                            max_chars=int(max_chars or 0) or 9000,
                        )
                    except Exception:
                        fetched = None
                    fetched_s = str(fetched or "").strip()
                    if (
                        fetched_s
                        and (len(fetched_s) > len(text) + 5)
                        and not _looks_like_truncated_message_text(fetched_s)
                    ):
                        text = fetched_s
                        via = "fallback"

            filtered = (
                _filter_schedule_source_text(text, event_date=event_date, event_title=event_title).strip()
                if text
                else ""
            )
            cache[canonical] = filtered
            logger.info(
                "tg_monitor.linked_text source=%s message_id=%s linked=%s chars=%d via=%s",
                source_username,
                source_message_id,
                canonical,
                len(filtered),
                via,
            )

        text = str(cache.get(canonical) or "").strip()
        if not text:
            continue
        uname, mid = _parse_tg_source_url(canonical)
        if not uname or not mid:
            continue
        out.append((canonical, str(uname), int(mid), text))

    return out


async def _send_bridge_notice_to_daily_channels(
    db: Database,
    *,
    bot: Any,
    notice_text: str,
    photo_url: str | None,
) -> int:
    if not bot or not notice_text:
        return 0
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(Channel.channel_id).where(Channel.daily_time.is_not(None))
            )
        ).all()
    channel_ids = [int(r[0]) for r in rows if r and r[0] is not None]
    if not channel_ids:
        return 0
    sent = 0
    for cid in channel_ids:
        try:
            if photo_url:
                await bot.send_photo(
                    cid,
                    photo=photo_url,
                    caption=notice_text[:950],
                    parse_mode="HTML",
                )
            else:
                await bot.send_message(
                    cid,
                    notice_text[:3800],
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            sent += 1
        except Exception as exc:
            msg = str(exc).lower()
            if "chat not found" in msg or "forbidden" in msg:
                logger.warning("bridge notice skipped for channel=%s: %s", cid, exc)
                continue
            logger.warning("bridge notice send failed for channel=%s: %s", cid, exc)
            continue
    return sent


def _infer_time_from_event_text(text: str | None, *, event_date: str | None) -> str | None:
    """Best-effort time fallback for per-event schedule snippets.

    Telegram monitor may extract date/title from OCR but occasionally leave `time`
    empty even when poster text contains a single explicit `HH:MM` for that date.
    """
    raw = str(text or "").strip()
    if not raw:
        return None

    # Prefer explicit "начало" time when present.
    start_times = re.findall(
        r"(?i)\b(?:начал[оа]|старт)\s*(?:в\s*)?([01]?\d|2[0-3]):([0-5]\d)\b",
        raw,
    )
    uniq_start = sorted({f"{int(hh):02d}:{mm}" for hh, mm in start_times})
    if len(uniq_start) == 1:
        return uniq_start[0]

    lines = [ln.strip() for ln in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n") if ln.strip()]
    date_tokens = _date_tokens_from_iso(event_date)
    if date_tokens and lines:
        dated_times: list[str] = []
        for ln in lines:
            low = ln.lower().replace("\xa0", " ")
            if any(tok in low for tok in date_tokens):
                dated_times.extend(_extract_time_tokens(ln))
        uniq = sorted(set(dated_times))
        if len(uniq) == 1:
            return uniq[0]

    uniq_all = sorted(set(_extract_time_tokens(raw)))
    if len(uniq_all) == 1:
        return uniq_all[0]
    return None


_RU_MONTHS_GEN: dict[str, int] = {
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


def _year_from_iso_date(value: str | None) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(raw.split("-", 1)[0])
    except Exception:
        return None


def _year_from_message_date(value: Any) -> int | None:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc).year
        raw = str(value).strip()
        if not raw:
            return None
        # ISO string.
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).year
    except Exception:
        return None


_IMPLICIT_YEAR_RECENT_PAST_DAYS = 92


def _date_from_message_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc).date()
        raw = str(value).strip()
        if not raw:
            return None
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _source_mentions_year_for_date(text: str | None, *, day: int, month: int, year: int) -> bool:
    """Return True when `day/month` is explicitly written with `year` in the source text.

    Used as a conservative guardrail: do not "fix" year rollover when the year is
    explicitly present for the same date.
    """
    raw = str(text or "")
    if not raw:
        return False

    dd = int(day)
    mm = int(month)
    yyyy = int(year)
    if not (1 <= dd <= 31 and 1 <= mm <= 12 and 1900 <= yyyy <= 2100):
        return False

    # Numeric formats: 27.12.2025 / 27/12/2025 / 27-12-2025 (also with leading zeros).
    num_re = re.compile(
        rf"(?iu)\b(?:{dd}[./-]{mm}|{dd:02d}[./-]{mm:02d})[./-]{yyyy}\b"
    )
    if num_re.search(raw):
        return True

    # Text formats: 27 декабря 2025 (Russian genitive month names).
    month_words = [w for w, n in _RU_MONTHS_GEN.items() if int(n) == mm]
    if month_words:
        mon_alt = "|".join(map(re.escape, month_words))
        txt_re = re.compile(
            rf"(?iu)\b{dd}\s*(?:{mon_alt})\s*(?:{yyyy})\b"
        )
        if txt_re.search(raw):
            return True
    return False


def _rollover_iso_date_to_anchor(value: Any, *, anchor: date) -> str | None:
    return normalize_implicit_iso_date_to_anchor(
        value,
        anchor_date=anchor,
        recent_past_days=_IMPLICIT_YEAR_RECENT_PAST_DAYS,
    )


def _poster_time_match_is_supported(tail: str, match: re.Match[str]) -> bool:
    token = match.group(0)
    if "." not in token:
        return True
    try:
        minute = int(match.group(2))
    except Exception:
        minute = 0
    if not (1 <= minute <= 12):
        return True
    before = tail[max(0, match.start() - 40) : match.start()]
    after = tail[match.end() : match.end() + 24]
    context = f"{before} {after}"
    return bool(
        re.search(
            r"(?iu)\b(?:начало|старт|сеанс|сбор|в|к|час(?:ов|а)?|ч)\b",
            context,
        )
    )


def _first_supported_poster_time(tail: str, time_re: re.Pattern[str]) -> re.Match[str] | None:
    for tm in time_re.finditer(tail):
        if _poster_time_match_is_supported(tail, tm):
            return tm
    return None


def _extract_poster_date_time_pairs(text: str | None) -> list[tuple[int, int, str]]:
    """Extract (month, day, HH:MM) pairs from poster OCR text."""
    raw = str(text or "").replace("\xa0", " ").strip()
    if not raw:
        return []
    low = raw.casefold()
    time_re = re.compile(r"\b([01]?\d|2[0-3])[:.](\d{2})\b")

    pairs: list[tuple[int, int, str]] = []
    seen: set[tuple[int, int, str]] = set()

    # 1) "12 июня ... 19:00"
    dm_re = re.compile(
        r"\b(\d{1,2})\s*(%s)\b" % "|".join(map(re.escape, _RU_MONTHS_GEN.keys())),
        re.IGNORECASE,
    )
    for m in dm_re.finditer(low):
        try:
            dd = int(m.group(1))
        except Exception:
            continue
        mon_word = str(m.group(2) or "").casefold()
        mm = _RU_MONTHS_GEN.get(mon_word)
        if not mm:
            continue
        tail = low[m.end() : m.end() + 160]
        tm = _first_supported_poster_time(tail, time_re)
        if not tm:
            continue
        token = f"{int(tm.group(1)):02d}:{tm.group(2)}"
        key = (mm, dd, token)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)

    # 2) "12.06 ... 19:00"
    num_re = re.compile(r"\b(\d{1,2})[./](\d{1,2})\b")
    for m in num_re.finditer(low):
        try:
            dd = int(m.group(1))
            mm = int(m.group(2))
        except Exception:
            continue
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            continue
        tail = low[m.end() : m.end() + 160]
        tm = _first_supported_poster_time(tail, time_re)
        if not tm:
            continue
        token = f"{int(tm.group(1)):02d}:{tm.group(2)}"
        key = (mm, dd, token)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)

    return pairs


def _expand_events_from_poster_datetime_pairs(
    message: dict[str, Any],
    events: list[dict[str, Any]],
    *,
    username: str | None,
    message_id: int | None,
) -> list[dict[str, Any]]:
    """Expand a single extracted event into multiple days using poster OCR.

    Used for cases like a concert poster with two separate dates/times in one post.
    The Kaggle extractor can occasionally collapse them into one date; this restores
    distinct event cards conservatively.
    """
    if not events:
        return events
    posters = message.get("posters") or []
    if not isinstance(posters, list) or not posters:
        return events

    # Keep it conservative: only when a post seems to describe one title.
    titles = {_dedupe_norm_title(ev.get("title")) for ev in events if str(ev.get("title") or "").strip()}
    if len(titles) != 1:
        return events
    title_norm = next(iter(titles), "")
    if not title_norm:
        return events

    parts: list[str] = []
    for item in posters:
        if not isinstance(item, dict):
            continue
        for k in ("ocr_title", "ocr_text"):
            v = str(item.get(k) or "").strip()
            if v:
                parts.append(v)
    ocr_text = "\n".join(parts).strip()
    if not ocr_text:
        return events

    pairs = _extract_poster_date_time_pairs(ocr_text)
    if len(pairs) < 2:
        return events
    # Avoid exploding schedule posters.
    if len(pairs) > 3:
        return events

    base_year = _year_from_iso_date(events[0].get("date")) or _year_from_message_date(message.get("message_date"))
    if not base_year:
        return events

    # Ensure at least one "meaningful" title token appears on the poster.
    tokens = [w for w in title_norm.split() if len(w) >= 5]
    ocr_norm = " ".join(ocr_text.casefold().split())
    if tokens and not any(tok in ocr_norm for tok in tokens[:6]):
        return events

    iso_pairs: list[tuple[str, str]] = []
    seen_iso: set[tuple[str, str]] = set()
    for mm, dd, tm in pairs:
        iso = f"{base_year:04d}-{mm:02d}-{dd:02d}"
        key = (iso, _dedupe_norm_time(tm))
        if key in seen_iso:
            continue
        seen_iso.add(key)
        iso_pairs.append((iso, tm))
    if len(iso_pairs) < 2:
        return events

    existing: set[tuple[str, str]] = set()
    for ev in events:
        existing.add((str(ev.get("date") or "").strip(), _dedupe_norm_time(ev.get("time"))))

    base = dict(events[0])
    added: list[dict[str, Any]] = []
    for iso, tm in iso_pairs:
        key = (iso, _dedupe_norm_time(tm))
        if key in existing:
            continue
        clone = dict(base)
        clone["date"] = iso
        clone["time"] = tm
        # Multi-day posters are usually separate performances, not a long-running event.
        if clone.get("end_date"):
            clone["end_date"] = None
        added.append(clone)
        existing.add(key)

    if added:
        logger.info(
            "tg_monitor.poster_multiday_expand source=%s message_id=%s title=%r before=%d after=%d pairs=%d",
            username,
            message_id,
            base.get("title"),
            len(events),
            len(events) + len(added),
            len(iso_pairs),
        )
        return list(events) + added
    return events


def _correct_single_event_from_poster_datetime_pair(
    message: dict[str, Any],
    events: list[dict[str, Any]],
    *,
    username: str | None,
    message_id: int | None,
) -> list[dict[str, Any]]:
    """Correct a single extracted event date/time from a single poster date/time pair.

    Telegram monitoring can sometimes extract the wrong date (e.g., an older linked post date)
    even though the poster OCR contains the real upcoming date/time. This helper keeps the
    logic conservative:
    - only when exactly 1 event card was extracted;
    - only when OCR yields exactly 1 (month, day, HH:MM) pair across posters;
    - only when the poster appears to match the event title (token hit);
    - only when the extracted date is missing or clearly too far in the past vs message_date.
    """
    if not events or len(events) != 1:
        return events
    posters = message.get("posters") or []
    if not isinstance(posters, list) or not posters:
        return events

    title_norm = _dedupe_norm_title(events[0].get("title"))
    if not title_norm:
        return events

    parts: list[str] = []
    for item in posters:
        if not isinstance(item, dict):
            continue
        for k in ("ocr_title", "ocr_text"):
            v = str(item.get(k) or "").strip()
            if v:
                parts.append(v)
    ocr_text = "\n".join(parts).strip()
    if not ocr_text:
        return events

    pairs = _extract_poster_date_time_pairs(ocr_text)
    if len(pairs) != 1:
        return events
    mm, dd, tm = pairs[0]

    base_year = _year_from_iso_date(events[0].get("date")) or _year_from_message_date(message.get("message_date"))
    if not base_year:
        return events
    try:
        poster_date = date(int(base_year), int(mm), int(dd))
    except Exception:
        return events

    # Ensure at least one meaningful title token appears on the poster (avoid foreign posters).
    tokens = [w for w in title_norm.split() if len(w) >= 5]
    ocr_norm = " ".join(ocr_text.casefold().split())
    if tokens and not any(tok in ocr_norm for tok in tokens[:6]):
        return events

    extracted_date_raw = str(events[0].get("date") or "").strip()
    extracted_date = _parse_iso_date(extracted_date_raw)
    anchor = _date_from_message_date(message.get("message_date"))

    should_fix_date = False
    if extracted_date is None:
        should_fix_date = True
    elif extracted_date != poster_date and anchor is not None:
        try:
            delta_extracted = (extracted_date - anchor).days
            delta_poster = (poster_date - anchor).days
        except Exception:
            delta_extracted = 0
            delta_poster = 0
        if delta_extracted < -2 and delta_poster >= -2:
            should_fix_date = True

    should_fix_time = False
    extracted_time_raw = str(events[0].get("time") or "").strip()
    if tm and (not extracted_time_raw or extracted_time_raw in {"00:00", "0:00"}):
        should_fix_time = True

    if not (should_fix_date or should_fix_time):
        return events

    fixed = dict(events[0])
    if should_fix_date:
        fixed["date"] = poster_date.isoformat()
    if should_fix_time:
        fixed["time"] = tm

    logger.info(
        "tg_monitor.poster_singlepair_correct source=%s message_id=%s title=%r date=%s->%s time=%s->%s",
        username,
        message_id,
        fixed.get("title"),
        extracted_date_raw,
        fixed.get("date"),
        extracted_time_raw,
        fixed.get("time"),
    )
    return [fixed]


def _filter_schedule_source_text(text: str, *, event_date: str | None, event_title: str | None) -> str:
    """Reduce multi-event schedule posts to the segment relevant to this event.

    Telegram repertoire posts often contain many lines like `DD.MM | Title`. Passing
    the whole post into Smart Update can leak other event titles into the description.
    """
    raw = (text or "").strip()
    if not raw:
        return ""

    # If this doesn't look like a schedule (multiple `DD.MM |` anchors), keep as-is.
    if len(_SCHED_LINE_RE.findall(raw)) < 2:
        return raw

    # Normalize and ensure schedule anchors appear on separate lines.
    normalized = raw.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\s+(?=\d{1,2}[./]\d{1,2}\s*\|)", "\n", normalized)
    lines = [ln.strip() for ln in normalized.splitlines() if ln.strip()]
    if not lines:
        return raw

    tokens = _date_tokens_from_iso(event_date)
    idx: int | None = None
    if tokens:
        for i, ln in enumerate(lines):
            if any(tok in ln for tok in tokens):
                idx = i
                break
    if idx is not None:
        out = [lines[idx]]
        for j in range(idx + 1, len(lines)):
            if _SCHED_LINE_START_RE.search(lines[j]):
                break
            out.append(lines[j])
        filtered = "\n".join(out).strip()
        return filtered or raw

    # Fallback: try to keep only lines that mention the title.
    title_norm = _norm_space(event_title)
    words = [w for w in title_norm.split() if len(w) >= 4]
    if words:
        matched = [ln for ln in lines if any(w in _norm_space(ln) for w in words)]
        if matched:
            return "\n".join(matched[:3]).strip()

    return "\n".join(lines[:3]).strip() or raw


_DATE_TITLE_PREFIX_RE = re.compile(r"^\s*\d{1,2}[./]\d{1,2}(?:[./](?:19|20)\d{2})?\s*(?:[|—–-]\s*)?", re.U)
_BAD_TITLE_RE = re.compile(r"^\s*[\W_]*\(?\s*\d*\s*(?:мест[ао]?)?\s*\)?\s*[\W_]*$", re.I | re.U)
_ADDRESS_HINT_RE = re.compile(
    r"(?i)\b(ул\.?|улица|пр-т|проспект|пл\.|площад|пер\.|переулок|наб\.|набереж|шоссе|бульвар|дом)\b"
)
_LOCATION_VENUE_CUE_RE = re.compile(
    r"(?iu)\b(?:театр\w*|музе[йяе]|галере[яи]|кино(?:театр|зал)|дк\b|дом\s+культур\w*|"
    r"центр\s+культур\w*|бар\w*|клуб\w*|пространств\w*|зал|сцена|"
    r"студи[яи]|ворот[а-я]*)\b"
)
_LOCATION_PROSE_VERB_RE = re.compile(
    r"(?iu)\b("
    r"анонсирован\w*|представ\w*|расскаж\w*|покаж\w*|приглаша\w*|"
    r"пройд[её]т|состоится|переносится|запланирован\w*|нужда[ею]тся|"
    r"выигра\w*|созда\w*|дарим|открыва\w*|пиш\w*|можно|будут|"
    r"жд\w*|известн\w*|телерадиоведущ\w*|концертмейстер\w*"
    r")\b"
)
_LOCATION_PROSE_START_RE = re.compile(
    r"(?iu)^\s*(?:которые|известн\w*|дарим|вместо|по\s+решению|это|аниме|мультфильм|"
    r"мастер[- ]?класс\w*|немого\s+кино|которые\s+не|"
    r"в\s+программе|программа\s+[–—-]|вы\s+услышите|"
    r"и\s+не\s+забывайте|не\s+забывайте|напоминаем)\b"
)
_LOCATION_LABEL_FRAGMENT_RE = re.compile(
    r"(?iu)^\s*(?:кинозал|мастерские|мастер[- ]?классы|расписание|программа|сцена|зал)\s*:?\s*$"
)
_LOCATION_SCHEDULE_FRAGMENT_RE = re.compile(
    r"(?iu)^\s*\d{1,2}[:.]\d{2}\s*[-–—]\s*\d{1,2}[:.]\d{2}\s*[-–—]\s+\S+"
)
_LOCATION_NON_VENUE_BULLET_RE = re.compile(
    r"(?u)^\s*(?!📍)[^\w\s#@,.:;!?()«»\"'`-]{1,4}\s+\S+"
)
_LOCATION_TOPIC_FRAGMENT_RE = re.compile(
    r"(?iu)^\s*(?:о|об|обо|про|по|для|к|ко|с|со)\s+\S+"
)
_LOCATION_PROGRAM_ITEM_RE = re.compile(
    r"(?iu)^\s*(?:[🎵🎶🎼•·▪️-]\s*)?(?:[A-ZА-ЯЁ]\.\s*){1,4}[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё-]+\b.*[–—-]\s*\S+"
)
_LOCATION_CATALOGUE_ADDRESS_RE = re.compile(
    r"(?iu)^\s*(?:соч\.?|op\.?|№)\s*[0-9IVXLCDMivxlcdmA-Za-zА-Яа-яЁё./ -]+\s*$"
)
_CITY_PREFIX_RE = re.compile(
    r"(?i)^\s*(?:г\.?|город|пос\.?|посёлок|поселок|пгт|село|деревня)\s+"
)
_LOCATION_GROUNDING_STOPWORDS = {
    "в",
    "во",
    "на",
    "у",
    "из",
    "под",
    "для",
    "или",
    "the",
    "and",
    "калининград",
    "светлогорск",
    "зеленоградск",
    "советск",
    "гусев",
    "черняховск",
    "ворота",
    "музей",
    "театр",
    "центр",
    "парк",
    "город",
}


def _infer_city_from_location_string(text: str | None) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        return None
    for part in reversed(parts):
        p = str(part or "").strip()
        if not p:
            continue
        p = p.lstrip("#").strip()
        p = _CITY_PREFIX_RE.sub("", p).strip()
        if not p:
            continue
        if re.search(r"\d", p):
            continue
        if p.casefold() in {"россия", "russia"}:
            continue
        if re.search(r"(?i)\bобласть\b|\bрайон\b", p):
            continue
        letters = sum(1 for ch in p if ch.isalpha())
        if letters < 3:
            continue
        return p
    return None


def _normalize_location_probe_text(text: str | None) -> str:
    raw = str(text or "").strip().casefold().replace("ё", "е")
    if not raw:
        return ""
    raw = re.sub(r"[^\w\s]+", " ", raw, flags=re.U)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _location_support_tokens(text: str | None) -> list[str]:
    raw = _normalize_location_probe_text(text)
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for token in raw.split():
        if token in _LOCATION_GROUNDING_STOPWORDS:
            continue
        if token.isdigit():
            continue
        if len(token) < 5:
            continue
        key = token[:7]
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _normalize_ocr_location_case(text: str | None) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    letters = [ch for ch in raw if ch.isalpha()]
    if letters:
        uppercase = sum(1 for ch in letters if ch.isupper())
        if uppercase / max(1, len(letters)) >= 0.8:
            return raw.title()
    return raw


def _location_is_grounded_in_text(location: str | None, text: str | None) -> bool:
    loc_tokens = _location_support_tokens(location)
    probe = _normalize_location_probe_text(text)
    if not loc_tokens or not probe:
        return False
    return all(tok in probe for tok in loc_tokens)


def _looks_like_bad_title(title: str | None) -> bool:
    raw = str(title or "").strip()
    if not raw:
        return True
    if "№" in raw and re.search(r"\d", raw):
        return False
    if _BAD_TITLE_RE.match(raw):
        return True
    if not re.search(r"[0-9A-Za-zА-Яа-яЁё№]", raw):
        return True
    return False


_MESSAGE_TITLE_INFERENCE_SKIP_RE = re.compile(
    r"(?iu)\b("
    r"(?:сегодня|завтра)\s+в\s+театр(?:е)?|"
    r"репертуар|"
    r"афиша|"
    r"анонс|"
    r"в\s+продаже|"
    r"появил[ао]сь\s+в\s+продаже"
    r")\b"
)


def _infer_title_from_message_text(text: str | None) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    for ln in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        s = (ln or "").strip()
        if not s:
            continue
        if s.lower().startswith(("билеты", "вход", "стоимость")):
            continue
        if _MESSAGE_TITLE_INFERENCE_SKIP_RE.search(s):
            continue
        s = _DATE_TITLE_PREFIX_RE.sub("", s).strip()
        s = re.sub(r"^[^\wА-Яа-яЁё]+", "", s).strip()
        if not s:
            continue
        if len(s) < 2:
            continue
        if _looks_like_bad_title(s):
            continue
        return s[:140].strip()
    return None


def _split_default_location_line(value: str | None) -> tuple[str | None, str | None, str | None]:
    raw = str(value or "").strip()
    if not raw or "," not in raw:
        return (raw or None), None, _infer_city_from_location_string(raw)
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if len(parts) < 2:
        return (raw or None), None, _infer_city_from_location_string(raw)
    city = _infer_city_from_location_string(parts[-1]) or _infer_city_from_location_string(raw)
    if city and len(parts) >= 3:
        return parts[0], ", ".join(parts[1:-1]).strip() or None, city
    return parts[0], ", ".join(parts[1:]).strip() or None, city


def _default_location_requires_source_grounding(value: str | None) -> bool:
    norm = _normalize_location_probe_text(value)
    return "калининград" in norm and "сити" in norm and "джаз" in norm


def _address_grounded_in_text(address: str | None, text: str | None) -> bool:
    address_norm = _normalize_location_probe_text(address)
    text_norm = _normalize_location_probe_text(text)
    if not address_norm or not text_norm:
        return False
    if address_norm in text_norm:
        return True
    address_tokens = [tok for tok in address_norm.split() if len(tok) >= 4 and not tok.isdigit()]
    address_nums = set(re.findall(r"\d{1,4}", address_norm))
    text_nums = set(re.findall(r"\d{1,4}", text_norm))
    if address_tokens and any(tok in text_norm for tok in address_tokens):
        if not address_nums or (address_nums & text_nums):
            return True
    return False


def _location_payload_grounded_in_text(
    *,
    location_name: str | None,
    location_address: str | None,
    text: str | None,
) -> bool:
    return (
        _source_text_explicitly_mentions_location(
            text,
            location_name=location_name,
            location_address=location_address,
        )
        or _location_is_grounded_in_text(location_name, text)
        or _address_grounded_in_text(location_address, text)
    )


def _event_local_location_candidate_ok(location_name: str | None, location_address: str | None) -> bool:
    raw = str(location_name or "").strip()
    if not raw:
        return False
    if "@" in raw or re.search(r"(?i)\b(?:t\.me|telegram\.me)/", raw):
        return False
    if (
        _looks_like_location_prose_fragment(raw)
        or _looks_like_location_program_fragment(raw, location_address)
        or _looks_like_location_person_name_fragment(raw)
    ):
        return False
    if str(location_address or "").strip():
        return True
    if _ADDRESS_HINT_RE.search(raw) or _LOCATION_VENUE_CUE_RE.search(raw):
        return True
    # A bare free-text fragment is not enough to override an LLM/default venue.
    # Allow only curated known venues without address cues; otherwise fail closed
    # and leave the semantic decision to the LLM/update pass.
    return match_known_venue(raw) is not None


def _location_override_candidate_ok(location_name: str | None, location_address: str | None) -> bool:
    """Stricter guard for replacing an LLM/default venue with regex/OCR evidence."""
    return _event_local_location_candidate_ok(location_name, location_address)


_CITY_SALUTATION_LOCATION_RE = re.compile(
    r"(?iu)^\s*"
    r"(?:калининград|светлогорск|зеленоградск|советск|гусев|черняховск|балтийск|пионерский|янтарный)"
    r"\s*,\s*"
    r"(?:спасибо|благодарим|любим|до\s+встречи|мы\s+вернулись|это\s+было)"
    r"\b"
)

_UNKNOWN_LOCATION_LINE_RE = re.compile(
    r"(?iu)^\s*(?:локаци[яю]|место|адрес)\s*(?:[:\-–—]\s*)?"
    r"(?:уточняется|сообщим|объявим|будет\s+позже|позже|tba|to\s+be\s+announced)"
    r"[.!?\s]*$"
)


def _infer_location_from_text(text: str | None) -> tuple[str | None, str | None]:
    raw = str(text or "").strip()
    if not raw:
        return None, None
    lines = [ln.strip() for ln in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n") if ln.strip()]
    if not lines:
        return None, None

    candidates: list[str] = []
    for ln in lines:
        cleaned = re.sub(r"^[•·▪️-]+\s*", "", ln).strip()
        if not cleaned:
            continue
        cleaned = cleaned.lstrip("📍").strip()
        cleaned = re.sub(r"(?iu)^\s*(?:место|адрес|локация)\s*[:：-]\s*", "", cleaned).strip()
        if not cleaned:
            continue
        low = cleaned.casefold()
        if low.startswith(("билеты", "вход", "стоимость", "сбор гостей", "начало")):
            continue
        if "зарегистрироваться" in low or "подписаться" in low:
            continue
        if (
            "📍" in ln
            or _ADDRESS_HINT_RE.search(cleaned)
            or (
                "," in cleaned
                and (
                    re.search(r"\b\d{1,3}\b", cleaned)
                    or _infer_city_from_location_string(cleaned.split(",", 1)[0])
                )
            )
        ):
            candidates.append(cleaned)

    for cleaned in candidates[:6]:
        # Split 'Venue, Address...' when it looks like a venue + street.
        if "," in cleaned:
            left, right = (part.strip() for part in cleaned.split(",", 1))
            if left and right:
                if _CITY_SALUTATION_LOCATION_RE.search(cleaned):
                    continue
                inline_venue = re.search(
                    r"(?iu)(?:^|\s)(?:в|на)\s+([A-Za-zА-Яа-яЁё0-9][A-Za-zА-Яа-яЁё0-9 &'’._-]{1,50})$",
                    left,
                )
                if inline_venue and (
                    _ADDRESS_HINT_RE.search(right) or re.search(r"\b\d{1,4}[A-Za-zА-Яа-яЁё]?\b", right)
                ):
                    venue = inline_venue.group(1).strip()
                    if venue and not _looks_like_location_prose_fragment(venue):
                        return venue, right
                if re.search(r"\b\d{1,2}:\d{2}\b", cleaned) and re.search(
                    r"\b\d{1,2}\s+[А-Яа-яЁё]+\b",
                    left,
                ):
                    continue
                if _ADDRESS_HINT_RE.search(left) or re.search(r"\b\d{1,3}\b", left):
                    right_before_time = re.split(r"\b\d{1,2}[:.]\d{2}\b", right, maxsplit=1)[0].strip(" ,;–—-")
                    if right_before_time and _LOCATION_VENUE_CUE_RE.search(right_before_time):
                        return _normalize_ocr_location_case(right_before_time), left
                    return cleaned, None
                left_city = _infer_city_from_location_string(left)
                if left_city and not _ADDRESS_HINT_RE.search(right) and not re.search(r"\b\d{1,3}\b", right):
                    normalized_right = _normalize_ocr_location_case(right)
                    if normalized_right and _event_local_location_candidate_ok(normalized_right, None):
                        return normalized_right, None
                    continue
                return left, right
        # No comma: treat the whole line as a venue/address blob.
        if len(cleaned) >= 3:
            return cleaned, None
    return None, None


def _infer_location_from_poster_payloads(payload: list[dict[str, Any]] | None) -> tuple[str | None, str | None]:
    for item in payload or []:
        if not isinstance(item, dict):
            continue
        for key in ("ocr_text", "ocr_title"):
            chunk = str(item.get(key) or "").strip()
            if not chunk:
                continue
            inferred_name, inferred_addr = _infer_location_from_text(chunk)
            if inferred_name:
                return inferred_name, inferred_addr
    return None, None


_LOCATION_TEMPORAL_FRAGMENT_RE = re.compile(
    r"(?iu)^\s*(?:"
    r"сегодня|завтра|послезавтра|вчера|"
    r"(?:в\s+)?(?:понедельник|вторник|среду|четверг|пятницу|субботу|воскресенье)|"
    r"\d{1,2}\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)"
    r")\s*[,.:;!?]?\s*$"
)


def _strip_location_temporal_decoration(value: str) -> str:
    """Remove leading emoji/bullets that LLMs often copy with date words.

    Public incidents showed values such as ``🤗Завтра`` surviving the temporal
    gate because the semantic token was decorated.  Keep this as a fail-closed
    shape normalizer only; it does not infer or replace a venue.
    """

    compact = re.sub(r"\s+", " ", value or "").strip()
    return re.sub(r"^[^0-9A-Za-zА-Яа-яЁё]+", "", compact).strip()


def _looks_like_location_temporal_fragment(value: str | None) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    probes = {raw, _strip_location_temporal_decoration(raw)}
    return any(_LOCATION_TEMPORAL_FRAGMENT_RE.fullmatch(probe) for probe in probes if probe)


def _looks_like_location_program_fragment(value: str | None, address: str | None = None) -> bool:
    raw = str(value or "").strip()
    addr = str(address or "").strip()
    if not raw:
        return False
    has_venue_or_address_cue = bool(_ADDRESS_HINT_RE.search(raw) or _LOCATION_VENUE_CUE_RE.search(raw))
    if _LOCATION_PROSE_START_RE.search(raw) and not has_venue_or_address_cue:
        return True
    if _LOCATION_PROGRAM_ITEM_RE.search(raw) and not has_venue_or_address_cue:
        return True
    if addr and _LOCATION_CATALOGUE_ADDRESS_RE.search(addr) and not (
        has_venue_or_address_cue or _ADDRESS_HINT_RE.search(addr)
    ):
        return True
    return False


def _looks_like_location_prose_fragment(value: str | None) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    if _looks_like_location_program_fragment(raw):
        return True
    if "\n" in raw:
        return True
    compact = re.sub(r"\s+", " ", raw)
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", compact)
    if _looks_like_location_temporal_fragment(compact):
        return True
    if _LOCATION_LABEL_FRAGMENT_RE.match(compact):
        return True
    if _LOCATION_SCHEDULE_FRAGMENT_RE.search(compact):
        return True
    has_venue_or_address_cue = bool(_ADDRESS_HINT_RE.search(compact) or _LOCATION_VENUE_CUE_RE.search(compact))
    if not has_venue_or_address_cue and (
        _LOCATION_NON_VENUE_BULLET_RE.search(compact)
        or _LOCATION_TOPIC_FRAGMENT_RE.search(compact)
        or (compact.count("(") > compact.count(")"))
    ):
        return True
    if len(compact) > 90:
        return True
    if re.search(r"[:]\s*$", compact) and len(words) <= 3 and not _ADDRESS_HINT_RE.search(compact):
        return True
    if "|" in compact or re.search(r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b", compact):
        return True
    if re.fullmatch(r"\d{4}\s*\([^)]{3,40}\)", compact):
        return True
    if _LOCATION_PROSE_START_RE.search(compact):
        return True
    if len(words) >= 8 and not _ADDRESS_HINT_RE.search(compact):
        return True
    if len(words) >= 4 and _LOCATION_PROSE_VERB_RE.search(compact):
        return True
    if len(words) >= 4 and re.search(r"[.!?]\s*$", compact):
        return True
    return False


def _looks_like_location_person_name_fragment(value: str | None) -> bool:
    """Syntactic safety-net for obvious speaker/person names in location_name."""
    raw = str(value or "").strip()
    if not raw:
        return False
    if _ADDRESS_HINT_RE.search(raw) or _LOCATION_VENUE_CUE_RE.search(raw):
        return False
    if re.search(r"\d|[,:;.!?/@#()]|[«»\"'`]", raw):
        return False
    words = re.findall(r"[A-Za-zА-Яа-яЁё-]+", raw)
    if not (2 <= len(words) <= 4):
        return False
    letters = "".join(re.findall(r"[A-Za-zА-Яа-яЁё]", raw))
    if len(letters) < 6:
        return False
    uppercase = sum(1 for ch in letters if ch.upper() == ch and ch.lower() != ch)
    return uppercase / max(1, len(letters)) >= 0.75


def _known_venue_payload_from_text(text: str | None, *, city: str | None = None) -> tuple[str | None, str | None, str | None]:
    probe = "\n".join(
        line
        for line in str(text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
        if not _UNKNOWN_LOCATION_LINE_RE.match(line.strip())
    )
    venue = find_known_venue_in_text(probe, city=city)
    if venue is None:
        return None, None, None
    return venue.name or None, venue.address or None, venue.city or None


_BOOKING_HANDLE_RE = re.compile(
    r"(?i)(?:"
    r"\b(?:запис(?:ь|аться)|регистрац(?:ия|ия\s+по)|брон(?:ь|ь\s+по)|забронировать|пишите|напиши|в\s*лс|в\s*личк\w*)\b[^\n@]{0,60}"
    r"|\b(?:билет\w*|регистрац\w*)\b[^\n@]{0,30}\b(?:у|в|через)\b[^\n@]{0,10}"
    r")(@[a-z0-9_]{4,32})"
)

_TICKET_CONTACT_LINE_RE = re.compile(
    r"(?iu)\b(билет\w*|вход\w*|регистрац\w*|запис(?:ь|аться)|брон(?:ь|ировать)|оплат\w*)\b"
)
_TG_HANDLE_IN_TEXT_RE = re.compile(r"(?i)@([a-z0-9_]{4,32})")
_TG_LINK_IN_TEXT_RE = re.compile(
    r"(?i)(?:https?://)?(?:t\.me|telegram\.me)/([a-z0-9_]{4,32})\b"
)
_PHONE_IN_TEXT_RE = re.compile(
    r"(?u)(?<!\d)(?:\+\s*7|8)\s*\(?\d{3}\)?[\s-]*\d{3}[\s-]*\d{2}[\s-]*\d{2}(?!\d)"
)
_EMAIL_IN_TEXT_RE = re.compile(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b")


def _extract_ticket_link_from_text(text: str | None) -> str | None:
    raw = str(text or "")
    if not raw.strip():
        return None
    if _TICKET_CONTACT_LINE_RE.search(raw):
        phone_match = _PHONE_IN_TEXT_RE.search(raw)
        if phone_match:
            phone_href = tel_href_for_phone_value(phone_match.group(0))
            if phone_href:
                return phone_href
    m = _BOOKING_HANDLE_RE.search(raw)
    if m:
        handle = (m.group(1) or "").strip().lstrip("@")
        if handle:
            return f"https://t.me/{handle}"

    # Fallback: explicit ticket lines like "Билеты: @username" / "Запись: t.me/username".
    for ln in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = (ln or "").strip()
        if not line or len(line) > 240:
            continue
        if not _TICKET_CONTACT_LINE_RE.search(line):
            continue
        mh = _TG_HANDLE_IN_TEXT_RE.search(line)
        if mh:
            handle = (mh.group(1) or "").strip()
            if handle:
                return f"https://t.me/{handle}"
        ml = _TG_LINK_IN_TEXT_RE.search(line)
        if ml:
            handle = (ml.group(1) or "").strip()
            if handle:
                return f"https://t.me/{handle}"
    return None


def _build_tg_user_link(username: str | None, user_id: Any) -> str | None:
    uname = normalize_tg_username(username)
    if uname:
        return f"https://t.me/{uname}"
    uid = _to_int(user_id)
    if uid and uid > 0:
        return f"tg://user?id={uid}"
    return None


def _infer_ticket_link_from_group_post_author(
    message: dict[str, Any],
    *,
    text: str | None,
) -> str | None:
    source_type = str(message.get("source_type") or "").strip().lower()
    if source_type not in {"group", "supergroup"}:
        return None
    author = message.get("post_author")
    if not isinstance(author, dict):
        return None
    if not bool(author.get("is_user")):
        return None
    raw_text = str(text or "").strip()
    if raw_text and (
        _PHONE_IN_TEXT_RE.search(raw_text) or _EMAIL_IN_TEXT_RE.search(raw_text)
    ):
        return None
    return _build_tg_user_link(author.get("username"), author.get("user_id"))


def _message_user_author_meta(message: dict[str, Any]) -> dict[str, Any] | None:
    author = message.get("post_author")
    if isinstance(author, dict) and author.get("is_user"):
        return author
    for key in ("sender", "sender_user", "from_user"):
        candidate = message.get(key)
        if isinstance(candidate, dict) and candidate.get("username"):
            return {**candidate, "is_user": True}
    return None


def _chat_post_author_username(message: dict[str, Any]) -> str | None:
    """Lowercased post-author username for group/supergroup messages.

    Used by the author-in-chat promo trigger. Only chats (group/supergroup)
    have per-message user authors; channels post as the channel itself, so we
    return ``None`` for them. Requires a resolved ``is_user`` author username.
    """
    source_type = str(message.get("source_type") or "").strip().lower()
    if source_type not in {"group", "supergroup"}:
        return None
    author = _message_user_author_meta(message)
    if not author:
        return None
    username = str(author.get("username") or "").strip().lstrip("@").lower()
    return username or None


def _norm_match(s: str | None) -> str:
    raw = (s or "").strip().lower()
    if not raw:
        return ""
    raw = raw.replace("ё", "е")
    raw = re.sub(r"[^\w\s:./-]+", " ", raw, flags=re.U)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _title_tokens(title: str | None) -> list[str]:
    t = _norm_match(title)
    if not t:
        return []
    stop = {"и", "в", "на", "по", "из", "от", "до", "для", "или", "о", "об", "про", "со", "к"}
    out: list[str] = []
    for w in t.split():
        if len(w) < 4:
            continue
        if w in stop:
            continue
        out.append(w)
    return out


def _looks_like_poster_only_non_event(message: dict[str, Any], event_data: dict[str, Any]) -> bool:
    """
    Kaggle may sometimes emit an "event" inferred purely from poster OCR title,
    without any date/time/ticket signals in the post text. These are typically
    artwork titles or promo fragments, not "events to attend".
    """
    title = str(event_data.get("title") or "").strip()
    if _looks_like_bad_title(title):
        return False
    # Strong anchors: keep.
    if str(event_data.get("time") or "").strip():
        return False
    if _coerce_url(str(event_data.get("ticket_link") or "")):
        return False
    if str(event_data.get("end_date") or "").strip():
        return False

    msg_text = _norm_match(str(message.get("text") or ""))
    if msg_text:
        tokens = _title_tokens(title)
        # If the title is present in the post text, it's not poster-only.
        if tokens and any(tok in msg_text for tok in tokens[:4]):
            return False

    posters = message.get("posters") or []
    if not isinstance(posters, list) or not posters:
        return False

    # Check whether title appears only in poster OCR.
    tokens = _title_tokens(title)
    if not tokens:
        return False
    best_ocr = ""
    best_hits = 0
    for p in posters:
        if not isinstance(p, dict):
            continue
        ocr = _norm_match(str(p.get("ocr_text") or "")) + " " + _norm_match(str(p.get("ocr_title") or ""))
        if not ocr.strip():
            continue
        hits = sum(1 for tok in tokens if tok in ocr)
        if hits > best_hits:
            best_hits = hits
            best_ocr = ocr
    if best_hits <= 0:
        return False

    # If the matched poster OCR contains date/time signals, it can be a real schedule card.
    if re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", best_ocr):
        return False
    if re.search(r"\b\d{1,2}[./]\d{1,2}\b", best_ocr):
        return False
    if re.search(
        r"\b(январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр)\w*\b",
        best_ocr,
    ):
        return False

    # Poster-only + no date/time on the poster itself => likely not an event.
    return True


def _looks_like_generic_schedule_poster(ocr_text: str) -> bool:
    t = _norm_match(ocr_text)
    if not t:
        return False
    if "неделя" in t and "театре" in t:
        return True
    if len(re.findall(r"\b\d{1,2}[./]\d{1,2}\b", t)) >= 3:
        return True
    return False


def _poster_match_score(
    poster: PosterCandidate,
    *,
    event_title: str | None,
    event_date: str | None,
    event_time: str | None,
) -> int:
    ocr = _norm_match(poster.ocr_text) or _norm_match(poster.ocr_title)
    if not ocr:
        return 0
    if _looks_like_generic_schedule_poster(ocr):
        return 0

    score = 0
    for tok in _date_tokens_from_iso(event_date):
        if tok and tok in ocr:
            score += 4
            break
    tm = _norm_match(event_time)
    if tm and re.search(rf"\b{re.escape(tm)}\b", ocr):
        score += 2

    tokens = _title_tokens(event_title)
    if tokens:
        hit = sum(1 for w in tokens if w in ocr)
        if hit:
            # Title hits should dominate for schedule posts; dates/times are often shared.
            score += 6
            if any(len(w) >= 7 and w in ocr for w in tokens):
                score += 2
            if hit >= max(2, int(len(tokens) * 0.6)):
                score += 2
    return score


def _filter_posters_for_event(
    posters: list[PosterCandidate],
    *,
    event_title: str | None,
    event_date: str | None,
    event_time: str | None,
) -> list[PosterCandidate]:
    if not posters:
        return []
    has_any_ocr = any(
        bool((getattr(p, "ocr_text", None) or "").strip() or (getattr(p, "ocr_title", None) or "").strip())
        for p in posters
    )
    if not has_any_ocr:
        # If OCR is empty for all posters, filtering would drop everything.
        # Keep the raw photos (capped) so multi-event posts without text on images
        # still get illustrations attached to each extracted event.
        return posters
    tokens = _title_tokens(event_title)
    has_many = len(posters) >= 2
    scored: list[tuple[int, int, PosterCandidate]] = []
    for i, p in enumerate(posters):
        scored.append(
            (
                _poster_match_score(
                    p, event_title=event_title, event_date=event_date, event_time=event_time
                ),
                i,
                p,
            )
        )
    scored.sort(key=lambda x: (-x[0], x[1]))
    kept: list[PosterCandidate] = []
    for s, _i, p in scored:
        if s < 6:
            continue
        if has_many and tokens:
            # In multi-poster schedule posts, require at least one solid title hit
            # (long token or multiple hits). Date/time alone is too weak.
            ocr = _norm_match(p.ocr_text) or _norm_match(p.ocr_title) or ""
            hits = [w for w in tokens if w in ocr]
            long_hit = any(len(w) >= 7 for w in hits)
            if not (long_hit or len(hits) >= 2):
                continue
        kept.append(p)
    if kept:
        return kept

    # No confident match: better to attach no posters than to attach the wrong ones.
    return []


async def _get_or_create_source(db: Database, username: str) -> TelegramSource:
    username = normalize_tg_username(username)
    if not username:
        raise ValueError("invalid telegram username")
    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                result = await session.execute(
                    select(TelegramSource).where(TelegramSource.username == username)
                )
                source = result.scalar_one_or_none()
                if source:
                    return source
                source = TelegramSource(username=username, enabled=True)
                session.add(source)
                await session.commit()
                await session.refresh(source)
                return source
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)
    raise RuntimeError("failed to create telegram source due to repeated SQLite locks")


async def _update_source_title(db: Database, *, source_id: int, title: str) -> None:
    clean = (title or "").strip()
    if not clean:
        return
    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                src = await session.get(TelegramSource, source_id)
                if not src:
                    return
                clean = clean[:140]
                if (src.title or "").strip() == clean:
                    return
                src.title = clean
                await session.commit()
            return
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)


def _normalize_source_meta_links(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        url = _clean_url(str(item or "").strip())
        if not url:
            continue
        key = url.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(url)
        if len(out) >= 12:
            break
    return out or None


def _normalize_source_suggestions(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        return None
    series = str(value.get("festival_series") or "").strip() or None
    website = _clean_url(str(value.get("website_url") or "").strip())
    if website and re.match(
        r"^https?://(?:t\.me|telegram\.me|telegra\.ph)/", website, flags=re.IGNORECASE
    ):
        website = None
    confidence = None
    try:
        raw_conf = value.get("confidence")
        if raw_conf is not None:
            confidence = float(raw_conf)
            confidence = max(0.0, min(1.0, confidence))
    except Exception:
        confidence = None
    rationale = str(value.get("rationale_short") or "").strip() or None
    return {
        "festival_series": series,
        "website_url": website,
        "confidence": confidence,
        "rationale_short": rationale,
    }


def _build_sources_meta_map(value: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not isinstance(value, list):
        return out
    for item in value:
        if not isinstance(item, dict):
            continue
        username = normalize_tg_username(item.get("username"))
        if not username:
            continue
        source_type = str(item.get("source_type") or "unknown").strip().lower()
        if source_type not in {"channel", "group", "supergroup", "unknown"}:
            source_type = "unknown"
        title = str(item.get("title") or "").strip() or None
        about = str(item.get("about") or "").strip() or None
        meta_hash = str(item.get("meta_hash") or "").strip() or None
        fetched_at = _parse_datetime(item.get("fetched_at"))
        out[username] = {
            "username": username,
            "source_type": source_type,
            "title": title[:140] if title else None,
            "about": about,
            "about_links": _normalize_source_meta_links(item.get("about_links")),
            "meta_hash": meta_hash,
            "fetched_at": fetched_at,
            "suggestions": _normalize_source_suggestions(item.get("suggestions")),
        }
    return out


async def _update_source_metadata(
    db: Database,
    *,
    source_id: int,
    meta: dict[str, Any],
) -> None:
    if not meta:
        return
    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                src = await session.get(TelegramSource, source_id)
                if not src:
                    return
                changed = False

                new_title = str(meta.get("title") or "").strip() or None
                if new_title:
                    new_title = new_title[:140]
                    if (src.title or "").strip() != new_title:
                        src.title = new_title
                        changed = True

                new_about = str(meta.get("about") or "").strip() or None
                if (src.about or None) != new_about:
                    src.about = new_about
                    changed = True

                new_links = meta.get("about_links")
                if not isinstance(new_links, list):
                    new_links = None
                current_links = src.about_links_json if isinstance(src.about_links_json, list) else None
                if current_links != new_links:
                    src.about_links_json = new_links
                    changed = True

                new_meta_hash = str(meta.get("meta_hash") or "").strip() or None
                if (src.meta_hash or None) != new_meta_hash:
                    src.meta_hash = new_meta_hash
                    changed = True

                new_fetched_at = meta.get("fetched_at") if isinstance(meta.get("fetched_at"), datetime) else None
                if src.meta_fetched_at != new_fetched_at:
                    src.meta_fetched_at = new_fetched_at
                    changed = True

                suggestions = meta.get("suggestions")
                if suggestions is None:
                    if src.suggested_festival_series is not None:
                        src.suggested_festival_series = None
                        changed = True
                    if src.suggested_website_url is not None:
                        src.suggested_website_url = None
                        changed = True
                    if src.suggestion_confidence is not None:
                        src.suggestion_confidence = None
                        changed = True
                    if src.suggestion_rationale is not None:
                        src.suggestion_rationale = None
                        changed = True
                elif isinstance(suggestions, dict):
                    new_suggested_series = str(suggestions.get("festival_series") or "").strip() or None
                    if src.suggested_festival_series != new_suggested_series:
                        src.suggested_festival_series = new_suggested_series
                        changed = True

                    new_suggested_website = _clean_url(str(suggestions.get("website_url") or "").strip())
                    if src.suggested_website_url != new_suggested_website:
                        src.suggested_website_url = new_suggested_website
                        changed = True

                    new_confidence = suggestions.get("confidence")
                    if new_confidence is not None:
                        try:
                            new_confidence = max(0.0, min(1.0, float(new_confidence)))
                        except Exception:
                            new_confidence = None
                    if src.suggestion_confidence != new_confidence:
                        src.suggestion_confidence = new_confidence
                        changed = True

                    new_rationale = str(suggestions.get("rationale_short") or "").strip() or None
                    if src.suggestion_rationale != new_rationale:
                        src.suggestion_rationale = new_rationale
                        changed = True

                if changed:
                    await session.commit()
            return
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)


async def _is_message_scanned(
    db: Database, source_id: int, message_id: int
) -> TelegramScannedMessage | None:
    async with db.get_session() as session:
        return await session.get(TelegramScannedMessage, (source_id, message_id))


def _parse_event_payload_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except Exception:
        return None


def _event_payload_can_still_be_imported(event_data: Any, *, today: date | None = None) -> bool:
    if not isinstance(event_data, dict):
        return False
    current = today or date.today()
    start = _parse_event_payload_date(event_data.get("date"))
    end = _parse_event_payload_date(event_data.get("end_date"))
    if end is not None:
        return end >= current
    if start is None:
        return True
    return start >= current


def _has_reprocessable_event_payload(events: Any) -> bool:
    if not isinstance(events, list):
        return False
    return any(_event_payload_can_still_be_imported(event_data) for event_data in events)


async def _source_url_already_attached(db: Database, source_url: str | None) -> bool:
    clean = str(source_url or "").strip()
    if not clean:
        return False
    async with db.get_session() as session:
        row = (
            await session.execute(
                select(EventSource.id)
                .where(EventSource.source_url == clean)
                .limit(1)
            )
        ).scalar_one_or_none()
    return row is not None


async def _should_reprocess_incomplete_scan(
    db: Database,
    *,
    existing: TelegramScannedMessage,
    source_url: str | None,
    events: Any,
) -> bool:
    status = str(getattr(existing, "status", "") or "").strip().lower()
    if status not in {
        "skipped",
        "partial",
        "retry_scheduled",
        "partial_retry_scheduled",
        "error",
    }:
        return False
    error_text = str(getattr(existing, "error", "") or "").strip()
    producer_zero_events = "producer_zero_events" in error_text
    if error_text and not producer_zero_events:
        return False
    try:
        extracted = int(getattr(existing, "events_extracted", 0) or 0)
        imported = int(getattr(existing, "events_imported", 0) or 0)
    except Exception:
        return False
    if producer_zero_events:
        if not _has_reprocessable_event_payload(events):
            return False
        if imported <= 0 and await _source_url_already_attached(db, source_url):
            return False
        return True
    if extracted <= 0 or imported >= extracted:
        return False
    if not _has_reprocessable_event_payload(events):
        return False
    if imported <= 0 and await _source_url_already_attached(db, source_url):
        return False
    return True


def _scan_error_from_breakdown(
    status: str,
    skip_breakdown: dict[str, int] | defaultdict[str, int] | None,
) -> str | None:
    if status not in {
        "skipped",
        "partial",
        "error",
        "retry_scheduled",
        "partial_retry_scheduled",
    }:
        return None
    if not skip_breakdown:
        return None
    payload = {
        "skip_breakdown": dict(sorted(dict(skip_breakdown).items())),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


_ZERO_EVENT_CLEAR_INVITE_RE = re.compile(
    r"(?iu)\b(?:спектакл[ьяею]|концерт\w*|кинопоказ\w*|показ\w*|лекци[яюи]|встреч[ауе]|"
    r"экскурси[яюи]|тур\w*|мастер[- ]?класс\w*|стендап\w*|приглашаем|билеты|регистрац\w*)\b"
)
_ZERO_EVENT_DATE_RE = re.compile(
    r"(?iu)\b(?:\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?|\d{1,2}\s+"
    r"(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря))\b"
)
_ZERO_EVENT_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}\b")
_ZERO_EVENT_VENUE_OR_TICKET_RE = re.compile(
    r"(?iu)\b(?:билет\w*|регистрац\w*|касс\w*|vk\.cc|qtickets|tickets?|театр\w*|центр\s+культур\w*|"
    r"дк\b|музе[йяе]|галере[яи]|бар\w*|клуб\w*|пространств\w*|ул\.?|улица|проспект|пр-т|площадь|пл\.?|наб\.?|аллея)\b|https?://"
)


def _message_has_event_like_zero_extraction_signals(message: dict[str, Any]) -> bool:
    """Structural diagnostics only: flags silent producer [] on clear event-shaped posts."""
    text = str(message.get("text") or "").strip()
    for item in message.get("posters") or []:
        if isinstance(item, dict):
            text += "\n" + str(item.get("ocr_text") or "")
            text += "\n" + str(item.get("ocr_title") or "")
    if len(text.strip()) < 40:
        return False
    if len(_ZERO_EVENT_TIME_RE.findall(text)) > 4:
        return False
    return bool(
        _ZERO_EVENT_CLEAR_INVITE_RE.search(text)
        and _ZERO_EVENT_DATE_RE.search(text)
        and _ZERO_EVENT_TIME_RE.search(text)
        and _ZERO_EVENT_VENUE_OR_TICKET_RE.search(text)
    )


def _decision_value(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _source_parse_decision(message: dict[str, Any]) -> SourceParseDecision | None:
    """Canonicalise a receipt through the central typed decision validator."""

    raw: Any | None = None
    for key in ("source_parse_decision", "parse_decision"):
        value = message.get(key)
        if value is not None:
            raw = value
            break
    if raw is None:
        return None

    if isinstance(raw, SourceParseDecision):
        decision = raw
        if decision.evidence_manifest is None:
            decision = SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                events=list(decision),
                lifecycle_actions=decision.lifecycle_actions,
            )
    elif isinstance(raw, dict):
        manifest_raw = raw.get("evidence_manifest")
        if not isinstance(manifest_raw, dict):
            decision = SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                events=(raw.get("events") if isinstance(raw.get("events"), list) else None),
            )
        else:
            manifest = EvidenceManifest.from_mapping(manifest_raw)
            decision = decision_from_provider_payload(
                raw,
                evidence_manifest=manifest,
            )
    else:
        return SourceParseDecision.retry(SourceParseRetryReason.SCHEMA_MISMATCH)

    manifest = decision.evidence_manifest
    if manifest is not None:
        source_text_present = "semantic_source_text" in message or "text" in message
        source_text = str(
            message.get("semantic_source_text")
            if message.get("semantic_source_text") is not None
            else message.get("text") or ""
        )
        hash_mismatch = bool(
            source_text_present
            and (
                manifest.raw_text_chars != len(source_text)
                or manifest.raw_text_hash
                != hashlib.sha256(source_text.encode("utf-8")).hexdigest()
            )
        )
        known_attachments = message.get("_source_attachment_count")
        attachment_mismatch = bool(
            isinstance(known_attachments, int)
            and not isinstance(known_attachments, bool)
            and known_attachments != manifest.attachment_count
        )
        if hash_mismatch or attachment_mismatch:
            return SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                evidence_manifest=manifest,
                events=list(decision),
                lifecycle_actions=decision.lifecycle_actions,
            )

    # The outer message and the typed decision are one receipt.  A mismatch
    # means the carrier is retryable; it must never terminally acknowledge a
    # different set of children than the consumer is about to persist.
    message_events = message.get("events")
    if not isinstance(message_events, list) or message_events != list(decision):
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=decision.evidence_manifest,
            events=list(decision),
            lifecycle_actions=decision.lifecycle_actions,
        )
    return decision


def _source_parse_disposition(decision: Any | None) -> str | None:
    raw = _decision_value(decision, "disposition") if decision is not None else None
    value = getattr(raw, "value", raw)
    clean = str(value or "").strip().upper()
    return clean or None


def _source_evidence_incomplete_reason(
    message: dict[str, Any], decision: Any | None
) -> str | None:
    """Return a technical retry reason when semantic evidence is incomplete."""

    if decision is not None and _decision_value(decision, "evidence_complete") is False:
        return "source_evidence_incomplete"

    verification = (
        _decision_value(decision, "verification")
        if decision is not None
        else None
    )
    if verification is not None:
        verification_disposition = _source_parse_disposition(verification)
        if verification_disposition == "RETRY_REQUIRED":
            return "source_verification_retry_required"
        if _decision_value(verification, "evidence_complete") is False:
            return "source_verification_evidence_incomplete"
        if _decision_value(verification, "error"):
            return "source_verification_error"

    manifest = (
        _decision_value(decision, "evidence_manifest")
        if decision is not None
        else None
    )
    if manifest is None:
        manifest = message.get("evidence_manifest")
    if manifest is not None:
        if _decision_value(manifest, "evidence_complete") is False:
            return "source_evidence_incomplete"
        if bool(
            _decision_value(manifest, "truncation_flag")
            or _decision_value(manifest, "truncated")
        ):
            return "source_evidence_truncated"
        if _decision_value(manifest, "omitted_blocks"):
            return "source_evidence_omitted"
        available = _decision_value(manifest, "ocr_blocks_available")
        included = _decision_value(manifest, "ocr_blocks_included")
        if isinstance(available, int) and isinstance(included, int) and included < available:
            return "source_ocr_incomplete"

    for status in (
        message.get("ocr_status"),
        message.get("media_status"),
        message.get("evidence_status"),
    ):
        clean = str(getattr(status, "value", status) or "").strip().lower()
        if clean in {"error", "failed", "incomplete", "missing", "timeout", "pending"}:
            return f"source_evidence_{clean}"

    return None


def _source_zero_event_is_confirmed(
    message: dict[str, Any], decision: Any | None
) -> bool:
    """Only a complete typed LLM verdict may resolve a zero-event carrier."""

    verification = (
        _decision_value(decision, "verification")
        if decision is not None
        else None
    )
    verification_disposition = (
        _source_parse_disposition(verification) if verification is not None else None
    )
    no_event_reason_raw = (
        _decision_value(decision, "no_event_reason") if decision is not None else None
    )
    no_event_reason = getattr(no_event_reason_raw, "value", no_event_reason_raw)
    return bool(
        decision is not None
        and _source_parse_disposition(decision) == "CONFIRMED_NO_EVENT"
        and _decision_value(decision, "evidence_complete") is True
        and no_event_reason in {item.value for item in SourceNoEventReason}
        and verification_disposition in {None, "CONFIRMED_NO_EVENT"}
        and _source_evidence_incomplete_reason(message, decision) is None
    )


async def _mark_message_scanned(
    db: Database,
    *,
    source_id: int,
    message_id: int,
    message_date: datetime | None,
    status: str,
    events_extracted: int,
    events_imported: int,
    error: str | None,
) -> None:
    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                row = await session.get(TelegramScannedMessage, (source_id, message_id))
                if row:
                    row.message_date = message_date or row.message_date
                    row.processed_at = datetime.now(timezone.utc)
                    row.status = status
                    row.events_extracted = events_extracted
                    row.events_imported = events_imported
                    row.error = error
                else:
                    row = TelegramScannedMessage(
                        source_id=source_id,
                        message_id=message_id,
                        message_date=message_date,
                        processed_at=datetime.now(timezone.utc),
                        status=status,
                        events_extracted=events_extracted,
                        events_imported=events_imported,
                        error=error,
                    )
                    session.add(row)
                await session.commit()
            return
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)


async def _update_source_scan_meta(
    db: Database, source_id: int, message_id: int | None
) -> None:
    if message_id is None:
        return
    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                source = await session.get(TelegramSource, source_id)
                if not source:
                    return
                if (
                    source.last_scanned_message_id is None
                    or message_id > source.last_scanned_message_id
                ):
                    source.last_scanned_message_id = message_id
                source.last_scan_at = datetime.now(timezone.utc)
                session.add(source)
                await session.commit()
            return
        except OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= 7:
                raise
            await asyncio.sleep(0.15 * attempt)


def _build_candidate(
    source: TelegramSource,
    message: dict[str, Any],
    event_data: dict[str, Any],
) -> EventCandidate:
    username = normalize_tg_username(message.get("source_username")) or str(message.get("source_username") or "").strip()
    message_id = _to_int(message.get("message_id"))
    source_link = message.get("source_link")
    if not source_link and username and message_id:
        source_link = f"https://t.me/{username}/{message_id}"
    title = event_data.get("title")
    date_raw = event_data.get("date")
    time_raw = event_data.get("time") or ""
    end_date = event_data.get("end_date")
    extracted_location = event_data.get("location_name")
    location_name = extracted_location or source.default_location
    location_from_source_default = bool(source.default_location and not str(extracted_location or "").strip())
    location_address = event_data.get("location_address")
    extracted_location_address = location_address
    location_overridden_by_default = False
    if not location_name and location_address:
        location_name, location_address = location_address, None
    extracted_city = event_data.get("city")
    default_city = _infer_city_from_location_string(source.default_location)
    if (
        source.default_location
        and str(location_name or "").strip() == str(source.default_location or "").strip()
        and not location_address
    ):
        default_name, default_address, parsed_default_city = _split_default_location_line(source.default_location)
        location_name = default_name
        location_address = default_address
        if parsed_default_city and not default_city:
            default_city = parsed_default_city
    city_overridden_by_default = False
    if not default_city and location_name and "," in str(location_name):
        # Best-effort: sometimes extractor puts "Venue, City" into location_name.
        # Avoid misclassifying "Venue, Street 10" (no city) as a city.
        try:
            last_part = str(location_name).rsplit(",", 1)[-1].strip()
        except Exception:
            last_part = ""
        if last_part and not re.search(r"\d", last_part):
            default_city = _infer_city_from_location_string(location_name)
    ticket_link = _coerce_url(event_data.get("ticket_link")) or _coerce_url(source.default_ticket_link)
    ticket_link_from_post_author = False
    ticket_price_min = _to_int(event_data.get("ticket_price_min"))
    ticket_price_max = _to_int(event_data.get("ticket_price_max"))
    ticket_status = event_data.get("ticket_status")
    raw_excerpt = event_data.get("raw_excerpt")
    event_type = _normalize_event_type(event_data.get("event_type"))
    emoji = event_data.get("emoji")
    is_free = event_data.get("is_free")
    pushkin_card = event_data.get("pushkin_card")
    search_digest = event_data.get("search_digest") or event_data.get("search_description")

    # Prefer per-event posters/source_text if the monitor provided them (assignment),
    # but keep the *message-level* poster scope for cleanup of previously-attached
    # "foreign" posters from the same album/schedule post.
    message_posters_payload = message.get("posters") or []
    assigned_posters_payload = event_data.get("posters") or []
    posters_payload = assigned_posters_payload or message_posters_payload or []
    ocr_source_text_parts: list[str] = []
    for key in ("ocr_text", "ocr_title"):
        chunk = str(message.get(key) or "").strip()
        if chunk:
            ocr_source_text_parts.append(chunk)
    for item in (posters_payload[:3] if isinstance(posters_payload, list) else []):
        if not isinstance(item, dict):
            continue
        for key in ("ocr_text", "ocr_title"):
            chunk = str(item.get(key) or "").strip()
            if chunk and chunk not in ocr_source_text_parts:
                ocr_source_text_parts.append(chunk)
    ocr_source_text = "\n".join(ocr_source_text_parts).strip()
    event_source_text = event_data.get("source_text") or event_data.get("description") or ""
    event_source_text_raw = str(event_source_text or "")
    message_text = message.get("text") or ""
    event_source_text_s = str(event_source_text or "").strip()
    message_text_s = str(message_text or "").strip()
    # Schedule monitor may provide per-event source_text as a short header
    # like "12.02 | Фигаро". Prefer the richer message text and then filter it
    # down to the event segment to keep factual lines for this event.
    if message_text_s and (
        not event_source_text_s
        or len(event_source_text_s) < 80
        or (
            len(_SCHED_LINE_RE.findall(message_text_s)) >= 2
            and len(message_text_s) > len(event_source_text_s) + 60
        )
    ):
        event_source_text = message_text_s
    elif not message_text_s and not event_source_text_s and ocr_source_text:
        event_source_text = ocr_source_text
    else:
        event_source_text = event_source_text_s
    event_source_text = _filter_schedule_source_text(
        str(event_source_text),
        event_date=str(date_raw).strip() if date_raw else None,
        event_title=str(title).strip() if title else None,
    )

    # Telegram Monitoring can re-import older posts (for metrics, pinned context, etc).
    # When a date has no explicit year in the text, we must resolve it relative to the
    # message publication date, not "today" (scan date), otherwise December posts can
    # incorrectly roll into the next year (e.g. "27 декабря" -> next year's Dec 27).
    anchor_date = _date_from_message_date(message.get("message_date"))
    if anchor_date and date_raw:
        clean_date = str(date_raw).split("..", 1)[0].strip()
        try:
            parsed_date = date.fromisoformat(clean_date)
        except Exception:
            parsed_date = None
        if parsed_date is not None:
            # Guardrail: only apply year rollover correction when the extracted date is
            # ~1 year away from the message date (typical "missing year" inference bug).
            # Avoid rolling genuine near-past dates (e.g. a post about "20 февраля" published on 23 февраля).
            try:
                delta_days = abs((parsed_date - anchor_date).days)
            except Exception:
                delta_days = 0
            if delta_days >= 330:
                rolled = _rollover_iso_date_to_anchor(clean_date, anchor=anchor_date)
                if rolled and rolled != clean_date and not _source_mentions_year_for_date(
                    event_source_text_raw or message_text_s,
                    day=parsed_date.day,
                    month=parsed_date.month,
                    year=parsed_date.year,
                ):
                    logger.info(
                        "telegram: corrected missing-year date by message anchor source=%s message_id=%s before=%s after=%s anchor=%s",
                        username,
                        message_id,
                        clean_date,
                        rolled,
                        anchor_date.isoformat(),
                    )
                    date_raw = rolled
    if anchor_date and end_date:
        clean_end = str(end_date).split("..", 1)[0].strip()
        try:
            parsed_end = date.fromisoformat(clean_end)
        except Exception:
            parsed_end = None
        if parsed_end is not None:
            try:
                delta_days = abs((parsed_end - anchor_date).days)
            except Exception:
                delta_days = 0
            if delta_days >= 330:
                rolled_end = _rollover_iso_date_to_anchor(clean_end, anchor=anchor_date)
                if rolled_end and rolled_end != clean_end and not _source_mentions_year_for_date(
                    event_source_text_raw or message_text_s,
                    day=parsed_end.day,
                    month=parsed_end.month,
                    year=parsed_end.year,
                ):
                    logger.info(
                        "telegram: corrected missing-year end_date by message anchor source=%s message_id=%s before=%s after=%s anchor=%s",
                        username,
                        message_id,
                        clean_end,
                        rolled_end,
                        anchor_date.isoformat(),
                    )
                    end_date = rolled_end

    # If the extractor produced a garbage title (e.g. "(4 места)"), infer it from the message text.
    if _looks_like_bad_title(title):
        inferred_title = _infer_title_from_message_text(message_text_s)
        if inferred_title:
            logger.info(
                "telegram: inferred bad title from message text source=%s message_id=%s title=%r -> %r",
                username,
                message_id,
                title,
                inferred_title,
            )
            title = inferred_title

    inferred_loc, inferred_addr = _infer_location_from_text(event_source_text or message_text_s)
    poster_loc, poster_addr = _infer_location_from_poster_payloads(
        assigned_posters_payload or posters_payload or message_posters_payload
    )
    probe_for_known_location = "\n".join(
        str(part)
        for part in (
            event_source_text,
            event_source_text_raw,
            raw_excerpt,
            message_text_s,
        )
        if str(part or "").strip()
    )
    known_loc, known_addr, known_city = _known_venue_payload_from_text(
        probe_for_known_location,
        city=str(extracted_city or "").strip() or None,
    )
    location_address_was_prose = False
    location_had_temporal_fragment = False
    if location_address and _looks_like_location_prose_fragment(location_address):
        logger.warning(
            "telegram: dropped prose-like extracted location_address source=%s message_id=%s title=%r address=%r",
            username,
            message_id,
            title,
            location_address,
        )
        location_address = None
        extracted_location_address = None
        location_address_was_prose = True
    if location_name and _looks_like_location_temporal_fragment(location_name):
        logger.warning(
            "telegram: dropped temporal extracted location source=%s message_id=%s title=%r location=%r",
            username,
            message_id,
            title,
            location_name,
        )
        location_name = None
        location_address = None
        extracted_location = None
        extracted_location_address = None
        location_had_temporal_fragment = True
    if location_name and (
        _looks_like_location_prose_fragment(location_name)
        or _looks_like_location_program_fragment(location_name, location_address)
        or _looks_like_location_person_name_fragment(location_name)
    ):
        logger.warning(
            "telegram: dropped prose/person-like extracted location source=%s message_id=%s title=%r location=%r",
            username,
            message_id,
            title,
            location_name,
        )
        if known_loc:
            location_name = known_loc
        elif source.default_location:
            default_name, default_address, parsed_default_city = _split_default_location_line(source.default_location)
            location_name = default_name
            if default_address:
                location_address = default_address
            if parsed_default_city and not extracted_city:
                extracted_city = parsed_default_city
        else:
            location_name = None
        if known_addr and (known_loc or not location_address or location_address_was_prose):
            location_address = known_addr
        if known_city and not extracted_city:
            extracted_city = known_city
        if extracted_location and (
            _looks_like_location_prose_fragment(extracted_location)
            or _looks_like_location_person_name_fragment(extracted_location)
        ):
            extracted_location = None

    default_grounding_parts = [str(event_source_text or "").strip(), str(raw_excerpt or "").strip()]
    if message_text_s and message_text_s not in default_grounding_parts:
        default_grounding_parts.append(message_text_s)
    for item in (assigned_posters_payload or posters_payload or message_posters_payload or [])[:3]:
        if not isinstance(item, dict):
            continue
        for key in ("ocr_text", "ocr_title"):
            chunk = str(item.get(key) or "").strip()
            if chunk:
                default_grounding_parts.append(chunk)
    default_grounding_text = "\n".join(part for part in default_grounding_parts if part).strip()
    if location_from_source_default and source.default_location:
        default_name, default_address, _default_city = _split_default_location_line(source.default_location)
        default_requires_grounding = _default_location_requires_source_grounding(source.default_location)
        default_is_grounded = _location_payload_grounded_in_text(
            location_name=default_name,
            location_address=default_address,
            text=default_grounding_text,
        )
        alternate_loc = None
        alternate_addr = None
        for candidate_loc, candidate_addr in (
            (inferred_loc, inferred_addr),
            (poster_loc, poster_addr),
            (known_loc, known_addr),
        ):
            if candidate_loc and not _location_matches(candidate_loc, default_name):
                alternate_loc = candidate_loc
                alternate_addr = candidate_addr
                break
        if default_requires_grounding and default_is_grounded:
            alternate_loc = None
            alternate_addr = None
        if alternate_loc and _event_local_location_candidate_ok(alternate_loc, alternate_addr) and _location_payload_grounded_in_text(
            location_name=alternate_loc,
            location_address=alternate_addr,
            text=default_grounding_text,
        ):
            logger.info(
                "telegram: overriding source default with event-grounded venue source=%s message_id=%s title=%r default=%r venue=%r",
                username,
                message_id,
                title,
                default_name,
                alternate_loc,
            )
            location_name = alternate_loc
            location_address = alternate_addr
            location_from_source_default = False
            if known_city and not extracted_city:
                extracted_city = known_city
        elif default_requires_grounding and not default_is_grounded:
            logger.warning(
                "telegram: dropping ungrounded risky source default source=%s message_id=%s title=%r default=%r",
                username,
                message_id,
                title,
                source.default_location,
            )
            location_name = None
            location_address = None
            location_from_source_default = False

    if not location_name and not location_had_temporal_fragment:
        fallback_loc = inferred_loc or poster_loc or known_loc
        fallback_addr = inferred_addr or poster_addr or known_addr
        if fallback_loc and _event_local_location_candidate_ok(fallback_loc, fallback_addr):
            location_name = fallback_loc
            if fallback_addr and not location_address:
                location_address = fallback_addr
            if known_city and not extracted_city:
                extracted_city = known_city
            logger.info(
                "telegram: inferred missing location source=%s message_id=%s title=%r location=%r",
                username,
                message_id,
                title,
                location_name,
            )

    if extracted_location and source.default_location and not _location_matches(
        extracted_location, source.default_location
    ):
        logger.warning(
            "telegram: location mismatch for @%s msg=%s extracted=%s default=%s",
            username,
            message_id,
            extracted_location,
            source.default_location,
        )
        explicit_offsite = _source_text_explicitly_mentions_location(
            message_text_s or event_source_text,
            location_name=extracted_location,
            location_address=extracted_location_address,
        )
        if explicit_offsite:
            logger.info(
                "telegram: keeping explicit extracted location over default for @%s msg=%s extracted=%s",
                username,
                message_id,
                extracted_location,
            )
            location_name = extracted_location
            location_address = extracted_location_address
        else:
            grounded_loc = inferred_loc or poster_loc
            grounded_addr = inferred_addr or poster_addr
            probe_parts = [str(message_text_s or event_source_text or "").strip()]
            for item in (assigned_posters_payload or posters_payload or message_posters_payload or [])[:3]:
                if not isinstance(item, dict):
                    continue
                for key in ("ocr_text", "ocr_title"):
                    chunk = str(item.get(key) or "").strip()
                    if chunk:
                        probe_parts.append(chunk)
            probe_text = "\n".join(part for part in probe_parts if part).strip()
            if (
                grounded_loc
                and _location_override_candidate_ok(grounded_loc, grounded_addr)
                and not _location_matches(extracted_location, grounded_loc)
                and _location_is_grounded_in_text(grounded_loc, probe_text)
                and not _location_is_grounded_in_text(extracted_location, probe_text)
            ):
                logger.warning(
                    "telegram: replacing unsupported extracted location for @%s msg=%s extracted=%s grounded=%s default=%s",
                    username,
                    message_id,
                    extracted_location,
                    grounded_loc,
                    source.default_location,
                )
                location_name = grounded_loc
                location_address = grounded_addr
            else:
                # Fail closed: an unsupported extracted venue should not be
                # converted into a possibly unrelated source default. Keep the
                # source default available in metadata for the LLM disambiguation
                # pass, but only write grounded venue evidence here.
                if known_loc:
                    location_name = known_loc
                    location_address = known_addr
                    if known_city and not extracted_city:
                        extracted_city = known_city
                else:
                    location_name = None
                    location_address = None
                location_overridden_by_default = bool(location_name is None)
    elif extracted_location:
        probe_parts = [str(message_text_s or event_source_text or "").strip()]
        for item in (assigned_posters_payload or posters_payload or message_posters_payload or [])[:3]:
            if not isinstance(item, dict):
                continue
            for key in ("ocr_text", "ocr_title"):
                chunk = str(item.get(key) or "").strip()
                if chunk:
                    probe_parts.append(chunk)
        probe_text = "\n".join(part for part in probe_parts if part).strip()
        grounded_loc = inferred_loc or poster_loc
        grounded_addr = inferred_addr or poster_addr
        if (
            grounded_loc
            and _location_override_candidate_ok(grounded_loc, grounded_addr)
            and not _location_matches(extracted_location, grounded_loc)
            and _location_is_grounded_in_text(grounded_loc, probe_text)
            and not _location_is_grounded_in_text(extracted_location, probe_text)
        ):
            logger.warning(
                "telegram: replaced unsupported extracted location source=%s message_id=%s title=%r extracted=%r grounded=%r",
                username,
                message_id,
                title,
                extracted_location,
                grounded_loc,
            )
            location_name = grounded_loc
            if grounded_addr:
                location_address = grounded_addr
    elif source.default_location and location_name:
        # LLM-first extraction may leave location_name empty, causing the import
        # boundary to seed the source default. If the post itself has an
        # explicit offsite address/venue line, fail toward that event-local
        # evidence instead of silently publishing the source default.
        probe_parts = [str(message_text_s or event_source_text or "").strip()]
        for item in (assigned_posters_payload or posters_payload or message_posters_payload or [])[:3]:
            if not isinstance(item, dict):
                continue
            for key in ("ocr_text", "ocr_title"):
                chunk = str(item.get(key) or "").strip()
                if chunk:
                    probe_parts.append(chunk)
        probe_text = "\n".join(part for part in probe_parts if part).strip()
        grounded_loc = inferred_loc or poster_loc
        grounded_addr = inferred_addr or poster_addr
        if (
            grounded_loc
            and _location_override_candidate_ok(grounded_loc, grounded_addr)
            and not _location_matches(grounded_loc, source.default_location)
            and _location_payload_grounded_in_text(
                location_name=grounded_loc,
                location_address=grounded_addr,
                text=probe_text,
            )
            and not _location_is_grounded_in_text(str(location_name), probe_text)
        ):
            logger.warning(
                "telegram: replaced source-default location with explicit event-local location source=%s message_id=%s title=%r default=%r grounded=%r",
                username,
                message_id,
                title,
                source.default_location,
                grounded_loc,
            )
            location_name = grounded_loc
            location_address = grounded_addr
            location_overridden_by_default = True

    kept_explicit_location = bool(
        extracted_location
        and location_name
        and str(extracted_location).strip().casefold()
        == str(location_name).strip().casefold()
        and source.default_location
        and not _location_matches(extracted_location, source.default_location)
    )

    if default_city:
        if (
            extracted_city
            and str(extracted_city).strip().casefold() != str(default_city).strip().casefold()
        ):
            logger.warning(
                "telegram: city mismatch for @%s msg=%s extracted=%s default=%s",
                username,
                message_id,
                extracted_city,
                default_city,
            )
            if kept_explicit_location:
                city = extracted_city
            else:
                city_overridden_by_default = True
                city = default_city
        else:
            city = default_city
    else:
        city = extracted_city or "Калининград"
    if location_address:
        location_address = strip_city_from_address(location_address, city)

    pre_normalized_location_name = str(location_name).strip() if location_name else None
    pre_normalized_location_address = str(location_address).strip() if location_address else None
    pre_normalized_city = str(city).strip() if city else None
    location_payload = {
        "location_name": pre_normalized_location_name,
        "location_address": pre_normalized_location_address,
        "city": pre_normalized_city,
    }
    matched_venue = normalise_event_location_from_reference(location_payload)
    normalized_location_name = (location_payload.get("location_name") or "").strip() or None
    normalized_location_address = (location_payload.get("location_address") or "").strip() or None
    normalized_city = (location_payload.get("city") or "").strip() or None
    if (
        matched_venue
        and pre_normalized_location_name
        and normalized_location_name
        and not _location_matches(pre_normalized_location_name, normalized_location_name)
        and default_grounding_text
        and _location_is_grounded_in_text(pre_normalized_location_name, default_grounding_text)
        and not _location_is_grounded_in_text(normalized_location_name, default_grounding_text)
    ):
        logger.warning(
            "telegram: keeping event-grounded venue over ungrounded reference normalization source=%s message_id=%s before=%r after=%r",
            username,
            message_id,
            pre_normalized_location_name,
            normalized_location_name,
        )
        matched_venue = None
        normalized_location_name = pre_normalized_location_name
        normalized_location_address = pre_normalized_location_address
        normalized_city = pre_normalized_city
    if matched_venue and (
        normalized_location_name != (str(location_name).strip() if location_name else None)
        or normalized_location_address != (str(location_address).strip() if location_address else None)
        or normalized_city != (str(city).strip() if city else None)
    ):
        logger.info(
            "telegram: normalized known venue source=%s message_id=%s before=%r/%r/%r after=%r/%r/%r",
            username,
            message_id,
            location_name,
            location_address,
            city,
            normalized_location_name,
            normalized_location_address,
            normalized_city,
        )
    location_name = normalized_location_name
    location_address = normalized_location_address
    city = normalized_city

    # Final-stage prose gate (INC-2026-05-16).
    # `_infer_location_from_text` / `_infer_city_from_location_string` are
    # permissive: a sentence like "Это точка, после которой ты уже не тот же."
    # gets accepted as candidate venue when the LLM extraction produced prose
    # and `normalise_event_location_from_reference` finds no known venue in it.
    # Only fire when normalisation did NOT resolve a canonical venue — a known
    # venue from reference is authoritative even if its name contains characters
    # the prose detector would otherwise flag (e.g. `Творческое пространство 12|55`).
    if matched_venue is None:
        if location_name and (
            _looks_like_location_prose_fragment(location_name)
            or _looks_like_location_program_fragment(location_name, location_address)
            or _looks_like_location_person_name_fragment(location_name)
        ):
            logger.warning(
                "telegram: dropping prose/person location_name after recovery source=%s message_id=%s title=%r location=%r",
                username,
                message_id,
                title,
                location_name,
            )
            location_name = None
            location_address = None
        if location_address and _looks_like_location_prose_fragment(location_address):
            logger.warning(
                "telegram: dropping prose location_address after recovery source=%s message_id=%s title=%r address=%r",
                username,
                message_id,
                title,
                location_address,
            )
            location_address = None
        if (
            not location_name
            and location_address
            and not (_ADDRESS_HINT_RE.search(location_address) or _LOCATION_VENUE_CUE_RE.search(location_address))
        ):
            logger.warning(
                "telegram: dropping unpaired non-address location_address after recovery source=%s message_id=%s title=%r address=%r",
                username,
                message_id,
                title,
                location_address,
            )
            location_address = None

    # Known-venue grounding gate (INC-2026-05-16).
    # When the LLM-extracted location matched a canonical venue from
    # `docs/reference/locations.md`, require that the venue is mentioned in the
    # source text or poster OCR. Without this, "Calling X is a known venue with
    # the city Калининград" becomes a universal fallback that the LLM keeps
    # picking for unrelated events (City Jazz Club / Bar Bastion family).
    if (
        matched_venue is not None
        and location_name
        and extracted_location
        and not source.default_location
        and _location_matches(extracted_location, location_name)
    ):
        grounding_parts = [str(message_text_s or event_source_text or "").strip()]
        for item in (assigned_posters_payload or posters_payload or message_posters_payload or [])[:3]:
            if not isinstance(item, dict):
                continue
            for key in ("ocr_text", "ocr_title"):
                chunk = str(item.get(key) or "").strip()
                if chunk:
                    grounding_parts.append(chunk)
        if str(raw_excerpt or "").strip():
            grounding_parts.append(str(raw_excerpt).strip())
        grounding_text = "\n".join(part for part in grounding_parts if part).strip()
        if grounding_text and not _location_is_grounded_in_text(location_name, grounding_text):
            logger.warning(
                "telegram: dropping ungrounded known venue source=%s message_id=%s title=%r location=%r address=%r",
                username,
                message_id,
                title,
                location_name,
                location_address,
            )
            location_name = None
            location_address = None

    message_link_items = _extract_message_link_items(message)
    if ticket_link and _ticket_link_is_explicitly_non_admission(ticket_link, message_link_items):
        logger.warning(
            "telegram: dropped donation/support link from ticket_link source=%s message_id=%s title=%r ticket_link=%s",
            username,
            message_id,
            title,
            ticket_link,
        )
        ticket_link = None

    refined_ticket_link = _infer_ticket_link_from_message_links(
        message_link_items,
        current=ticket_link,
    )
    if refined_ticket_link and refined_ticket_link != ticket_link:
        ticket_link = refined_ticket_link
        logger.info(
            "telegram: refined ticket link from message links source=%s message_id=%s title=%r ticket_link=%s",
            username,
            message_id,
            title,
            ticket_link,
        )

    # Extract a booking contact from the message when ticket_link is missing.
    if not ticket_link:
        inferred_from_links = _infer_ticket_link_from_message_links(
            message_link_items
        )
        if inferred_from_links:
            ticket_link = inferred_from_links
            logger.info(
                "telegram: inferred missing ticket link from message links source=%s message_id=%s title=%r ticket_link=%s",
                username,
                message_id,
                title,
                ticket_link,
            )

    if not ticket_link:
        inferred_ticket = _extract_ticket_link_from_text(message_text_s or event_source_text)
        if inferred_ticket:
            ticket_link = inferred_ticket
            logger.info(
                "telegram: inferred missing ticket link from message text source=%s message_id=%s title=%r ticket_link=%s",
                username,
                message_id,
                title,
                ticket_link,
            )

    if not ticket_link:
        inferred_text_url = _infer_ticket_link_from_message_links(
            _extract_urls_from_text(message_text_s or event_source_text)
        )
        if inferred_text_url:
            ticket_link = inferred_text_url
            logger.info(
                "telegram: inferred missing ticket link from message urls source=%s message_id=%s title=%r ticket_link=%s",
                username,
                message_id,
                title,
                ticket_link,
            )

    if not ticket_link:
        inferred_author_link = _infer_ticket_link_from_group_post_author(
            message,
            text="\n".join(
                part
                for part in (message_text_s, event_source_text, ocr_source_text)
                if str(part or "").strip()
            ),
        )
        if inferred_author_link:
            ticket_link = inferred_author_link
            ticket_link_from_post_author = True
            logger.info(
                "telegram: inferred missing ticket link from post author source=%s message_id=%s title=%r ticket_link=%s",
                username,
                message_id,
                title,
                ticket_link,
            )

    try:
        message_event_count = len(message.get("events") or [])
    except Exception:
        message_event_count = 1
    # Full-message TBD wording is safe only for a single extracted activity.
    # In a multi-activity digest it may belong to a sibling, so the server
    # leaves that semantic decision to the per-event producer/Smart Update LLM.
    explicit_unknown_start_time = _source_explicitly_leaves_start_time_unknown(
        raw_excerpt,
        event_source_text_raw if message_event_count <= 1 else None,
        event_source_text if message_event_count <= 1 else None,
        message_text_s if message_event_count <= 1 else None,
    )
    if explicit_unknown_start_time and str(time_raw or "").strip():
        logger.warning(
            "telegram: cleared extracted time conflicting with explicit unknown start "
            "source=%s message_id=%s title=%r time=%r",
            username,
            message_id,
            title,
            time_raw,
        )
        time_raw = ""

    try:
        current_time = str(time_raw or "").strip()
        if not current_time and not explicit_unknown_start_time:
            inferred_start = _infer_time_from_event_text(event_source_text, event_date=str(date_raw).strip() if date_raw else None)
            if inferred_start:
                time_raw = inferred_start
    except Exception:
        pass

    if not str(time_raw or "").strip() and not explicit_unknown_start_time:
        time_probe_text = str(event_source_text or "")
        raw_probe = event_source_text_raw.strip()
        if raw_probe and raw_probe not in time_probe_text:
            time_probe_text = f"{time_probe_text}\n{raw_probe}".strip()
        inferred_time = _infer_time_from_event_text(
            time_probe_text,
            event_date=str(date_raw).strip() if date_raw else None,
        )
        if inferred_time:
            time_raw = inferred_time
            logger.info(
                "telegram: inferred missing event time from source text source=%s message_id=%s title=%r time=%s",
                username,
                message_id,
                title,
                inferred_time,
            )

    # If time is still missing, try poster OCR (common in schedule posts where the body text
    # mentions only "в воскресенье" but the poster contains explicit date/time).
    if not str(time_raw or "").strip() and not explicit_unknown_start_time:
        try:
            probe_payload = assigned_posters_payload or posters_payload or []
            chunks: list[str] = []
            for item in probe_payload[:3]:
                if not isinstance(item, dict):
                    continue
                ocr_title = str(item.get("ocr_title") or "").strip()
                ocr_text = str(item.get("ocr_text") or "").strip()
                if ocr_title:
                    chunks.append(ocr_title)
                if ocr_text:
                    chunks.append(ocr_text)
            if chunks:
                poster_probe_text = "\n".join(chunks).strip()
                inferred_time = _infer_time_from_event_text(
                    poster_probe_text,
                    event_date=str(date_raw).strip() if date_raw else None,
                )
                if inferred_time:
                    time_raw = inferred_time
                    logger.info(
                        "telegram: inferred missing event time from poster OCR source=%s message_id=%s title=%r time=%s",
                        username,
                        message_id,
                        title,
                        inferred_time,
                    )
        except Exception:
            pass

    posters: list[PosterCandidate] = []
    seen_hashes: set[str] = set()
    scope_hashes: set[str] = set()

    def _payload_to_posters(payload: list[dict[str, Any]]) -> list[PosterCandidate]:
        out: list[PosterCandidate] = []
        local_seen: set[str] = set()
        for item in payload:
            sha = (item or {}).get("sha256")
            if sha and isinstance(sha, str):
                sha = sha.strip()
            if sha and sha in local_seen:
                continue
            if sha:
                local_seen.add(sha)
            out.append(
                PosterCandidate(
                    catbox_url=(item or {}).get("catbox_url"),
                    supabase_url=(item or {}).get("supabase_url"),
                    supabase_path=(item or {}).get("supabase_path"),
                    sha256=sha,
                    raw_sha256=(item or {}).get("raw_sha256"),
                    phash=(item or {}).get("phash"),
                    ocr_text=(item or {}).get("ocr_text"),
                    ocr_title=(item or {}).get("ocr_title"),
                )
            )
        return out

    # Scope hashes: use the message-level posters when available (album posts), otherwise
    # fall back to whatever posters_payload we have.
    scope_payload = message_posters_payload or posters_payload
    for item in scope_payload:
        sha = (item or {}).get("sha256")
        if sha and isinstance(sha, str):
            sha = sha.strip()
            if sha:
                scope_hashes.add(sha)
    for item in posters_payload:
        sha = item.get("sha256")
        if sha and sha in seen_hashes:
            continue
        if sha:
            seen_hashes.add(sha)
        posters.append(
            PosterCandidate(
                catbox_url=item.get("catbox_url"),
                supabase_url=item.get("supabase_url"),
                supabase_path=item.get("supabase_path"),
                sha256=sha,
                raw_sha256=item.get("raw_sha256"),
                phash=item.get("phash"),
                ocr_text=item.get("ocr_text"),
                ocr_title=item.get("ocr_title"),
            )
        )

    try:
        total_events = message.get("events") or []
        total_events_n = len(total_events) if isinstance(total_events, list) else 1
    except Exception:
        total_events_n = 1

    if total_events_n <= 1:
        # Single-event post: keep all photos (deduped). OCR is used only downstream
        # for prioritization (cover first), not for dropping content.
        posters = _payload_to_posters(posters_payload)
    else:
        # Multi-event post: avoid attaching "foreign" posters from schedules/albums to every
        # extracted event. Prefer monitor-assigned event-level posters; otherwise filter
        # by OCR/title/date/time.
        if assigned_posters_payload:
            posters = _payload_to_posters(assigned_posters_payload)
        else:
            posters = _filter_posters_for_event(
                posters,
                event_title=str(title).strip() if title else None,
                event_date=str(date_raw).strip() if date_raw else None,
                event_time=str(time_raw).strip() if time_raw else None,
            )

    if not posters and assigned_posters_payload:
        # Telegram monitor may already map posters to concrete event cards.
        # Keep this event-level assignment as a fallback when strict OCR matching
        # is inconclusive (e.g., missing event time in message).
        assigned = _payload_to_posters(assigned_posters_payload)
        relaxed: list[PosterCandidate] = []
        for p in assigned:
            score = _poster_match_score(
                p,
                event_title=str(title).strip() if title else None,
                event_date=str(date_raw).strip() if date_raw else None,
                event_time=str(time_raw).strip() if time_raw else None,
            )
            if score >= 4:
                relaxed.append(p)
        if not relaxed:
            for p in assigned:
                ocr = _norm_match(p.ocr_text) or _norm_match(p.ocr_title)
                if ocr and _looks_like_generic_schedule_poster(ocr):
                    continue
                relaxed.append(p)
        posters = relaxed[:3]
        if posters:
            logger.info(
                "telegram: posters fallback kept assigned posters for %s/%s title=%r count=%s",
                username,
                message_id,
                title,
                len(posters),
            )

    time_support_chunks: list[str | None] = [
        message_text_s,
        event_source_text,
        event_source_text_raw,
        raw_excerpt,
    ]
    for item in (assigned_posters_payload or posters_payload or message_posters_payload or [])[:5]:
        if not isinstance(item, dict):
            continue
        time_support_chunks.append(item.get("ocr_text"))
        time_support_chunks.append(item.get("ocr_title"))
    current_time_for_support = str(time_raw or "").strip()
    time_is_default = False
    if current_time_for_support and current_time_for_support not in {"00:00", "0:00"}:
        time_is_default = not _time_is_supported_by_texts(
            current_time_for_support,
            *time_support_chunks,
        )
        if time_is_default:
            logger.warning(
                "telegram: marked unsupported extracted time as default source=%s message_id=%s title=%r time=%r",
                username,
                message_id,
                title,
                current_time_for_support,
            )
            if _time_value_matches_event_date_marker(current_time_for_support, str(date_raw or "")):
                logger.warning(
                    "telegram: dropped date-marker extracted as time source=%s message_id=%s title=%r date=%r time=%r",
                    username,
                    message_id,
                    title,
                    date_raw,
                    current_time_for_support,
                )
                time_raw = ""
                time_is_default = False

    metrics_raw = message.get("metrics")
    metrics: dict[str, Any] = dict(metrics_raw) if isinstance(metrics_raw, dict) else {}
    metrics.update(
        {
            "tg_default_location": (source.default_location or "").strip() or None,
            "tg_default_city": str(default_city).strip() if default_city else None,
            "tg_extracted_city": str(extracted_city).strip() if extracted_city else None,
            "tg_extracted_location_name": str(extracted_location).strip() if extracted_location else None,
            "tg_extracted_location_address": str(extracted_location_address).strip()
            if extracted_location_address
            else None,
            "tg_location_overridden_by_default": bool(location_overridden_by_default),
            "tg_location_kept_extracted": kept_explicit_location,
            "tg_location_temporal_rejected": bool(location_had_temporal_fragment),
            "tg_city_overridden_by_default": bool(city_overridden_by_default),
            "tg_ticket_link_from_post_author": bool(ticket_link_from_post_author),
            "tg_time_is_default": bool(time_is_default),
            "tg_time_explicitly_unknown": bool(explicit_unknown_start_time),
        }
    )

    festival = _ground_extracted_festival(
        event_data.get("festival"),
        source_payloads=(
            message_text_s,
            event_source_text_raw,
            event_source_text,
            raw_excerpt,
            ocr_source_text,
            message.get("links"),
            assigned_posters_payload,
            message_posters_payload,
        ),
        source_username=username,
        message_id=message_id,
        title=title,
        curated_festival_series=(
            (getattr(source, "festival_series", None) or "").strip()
            if bool(getattr(source, "festival_source", False))
            else None
        ),
    )

    return EventCandidate(
        source_type="telegram",
        source_url=source_link or None,
        source_text=event_source_text,
        title=str(title).strip() if title else None,
        date=str(date_raw).strip() if date_raw else None,
        time=str(time_raw).strip() if time_raw else "",
        time_is_default=bool(time_is_default),
        end_date=str(end_date).strip() if end_date else None,
        festival=festival,
        festival_source=bool(getattr(source, "festival_source", False)),
        festival_series=(getattr(source, "festival_series", None) or "").strip() or None,
        location_name=str(location_name).strip() if location_name else None,
        location_address=str(location_address).strip() if location_address else None,
        city=str(city).strip() if city else None,
        ticket_link=ticket_link,
        ticket_price_min=ticket_price_min,
        ticket_price_max=ticket_price_max,
        ticket_status=str(ticket_status).strip() if ticket_status else None,
        event_type=str(event_type).strip() if event_type else None,
        emoji=str(emoji).strip() if emoji else None,
        is_free=is_free if isinstance(is_free, bool) else None,
        pushkin_card=pushkin_card if isinstance(pushkin_card, bool) else None,
        search_digest=str(search_digest).strip() if search_digest else None,
        raw_excerpt=str(raw_excerpt).strip() if raw_excerpt else None,
        posters=posters,
        poster_scope_hashes=sorted(scope_hashes or seen_hashes),
        source_chat_username=username or None,
        source_chat_id=_to_int(message.get("source_chat_id")),
        source_message_id=message_id,
        tg_source_author=_chat_post_author_username(message),
        trust_level=source.trust_level,
        metrics=metrics,
        links_payload=[message.get("links"), event_data.get("links")],
    )


_EVENT_DEDUPE_TITLE_RE = re.compile(r"[\"«»]")
_EVENT_DEDUPE_WS_RE = re.compile(r"\s+")
_EVENT_DEDUPE_TIME_RE = re.compile(r"^(\d{1,2})[:.](\d{2})$")


def _dedupe_norm_title(title: str | None) -> str:
    raw = str(title or "")
    raw = _EVENT_DEDUPE_TITLE_RE.sub("", raw)
    raw = _EVENT_DEDUPE_WS_RE.sub(" ", raw).strip().lower()
    return raw


def _dedupe_norm_time(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    m = _EVENT_DEDUPE_TIME_RE.match(raw)
    if not m:
        return raw.replace(".", ":")
    try:
        hh = int(m.group(1))
        mm = int(m.group(2))
    except Exception:
        return raw.replace(".", ":")
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return raw.replace(".", ":")
    return f"{hh:02d}:{mm:02d}"


def _dedupe_merge_event_dict(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)

    original_indexes: list[int] = []
    for payload in (
        out.get("_telegram_result_indexes"),
        [out.get("_telegram_result_index")],
        extra.get("_telegram_result_indexes"),
        [extra.get("_telegram_result_index")],
    ):
        for index in _video_event_indexes(payload):
            if index not in original_indexes:
                original_indexes.append(index)
    if original_indexes:
        out["_telegram_result_indexes"] = original_indexes

    def _prefer_longer_str(key: str) -> None:
        a = str(out.get(key) or "").strip()
        b = str(extra.get(key) or "").strip()
        if b and (not a or len(b) > len(a) + 40):
            out[key] = b

    def _prefer_any_str(key: str) -> None:
        a = str(out.get(key) or "").strip()
        b = str(extra.get(key) or "").strip()
        if not a and b:
            out[key] = b

    def _prefer_any_int(key: str) -> None:
        if out.get(key) is None and extra.get(key) is not None:
            out[key] = extra.get(key)

    for k in ("title", "date", "time", "end_date", "city", "event_type", "emoji"):
        _prefer_any_str(k)
    for k in ("location_name", "location_address", "ticket_link", "ticket_status", "search_digest", "raw_excerpt"):
        _prefer_any_str(k)
    for k in ("ticket_price_min", "ticket_price_max"):
        _prefer_any_int(k)

    _prefer_longer_str("source_text")
    _prefer_longer_str("description")

    if isinstance(extra.get("is_free"), bool) and not isinstance(out.get("is_free"), bool):
        out["is_free"] = extra.get("is_free")
    if isinstance(extra.get("pushkin_card"), bool) and not isinstance(out.get("pushkin_card"), bool):
        out["pushkin_card"] = extra.get("pushkin_card")

    posters: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for payload in (out.get("posters") or [], extra.get("posters") or []):
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            sha = str(item.get("sha256") or "").strip()
            key = sha or str(item.get("catbox_url") or item.get("supabase_url") or "").strip()
            if not key:
                continue
            if key in seen_hashes:
                continue
            seen_hashes.add(key)
            posters.append(item)
    if posters:
        out["posters"] = posters

    linked: list[str] = []
    seen_linked: set[str] = set()
    for payload in (out.get("linked_source_urls") or [], extra.get("linked_source_urls") or []):
        if not isinstance(payload, list):
            continue
        for url in payload:
            u = str(url or "").strip()
            if not u:
                continue
            k = u.casefold()
            if k in seen_linked:
                continue
            seen_linked.add(k)
            linked.append(u)
    if linked:
        out["linked_source_urls"] = linked

    return out


def _dedupe_message_events(
    events_payload: Any,
    *,
    username: str | None,
    message_id: int | None,
) -> list[dict[str, Any]]:
    events = [ev for ev in (events_payload or []) if isinstance(ev, dict)]
    if len(events) <= 1:
        return events

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    order: list[tuple[str, str, str]] = []
    deduped = 0
    for ev in events:
        key = (
            _dedupe_norm_title(ev.get("title")),
            str(ev.get("date") or "").strip(),
            _dedupe_norm_time(ev.get("time")),
        )
        if key not in grouped:
            grouped[key] = ev
            order.append(key)
            continue
        grouped[key] = _dedupe_merge_event_dict(grouped[key], ev)
        deduped += 1

    if deduped:
        logger.info(
            "tg_monitor.event_dedupe source=%s message_id=%s in=%d out=%d",
            username,
            message_id,
            len(events),
            len(order),
        )

    return [grouped[k] for k in order]


async def _schedule_primary_import_event_tasks(db: Database, event_id: int) -> None:
    """Ensure the primary Telegram import can repair missing publication jobs.

    Smart Update schedules tasks on create/merge. A replay can legitimately
    return ``skipped_nochange`` for an already-created event; in that case the
    import boundary still has to restore the standard publication pipeline.
    """

    from main import schedule_event_update_tasks

    async with db.get_session() as session:
        event = await session.get(Event, int(event_id))
    if event is not None:
        await schedule_event_update_tasks(db, event)


async def _reconcile_primary_import_vk_sync_jobs(db: Database) -> int:
    """Repair missing VK fanout for active Telegram-origin events.

    This is intentionally scoped to Telegram origins and current/future active
    events. A successful `/tg` replay should restore the whole downstream
    publication pipeline, including historical rows that were imported before a
    scheduler bug was fixed.
    """

    from main import schedule_event_update_tasks

    today = date.today().isoformat()
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(Event)
                .outerjoin(
                    JobOutbox,
                    (JobOutbox.event_id == Event.id)
                    & (JobOutbox.task == JobTask.vk_sync)
                    & (JobOutbox.status.in_([JobStatus.pending, JobStatus.running])),
                )
                .where(
                    Event.source_post_url.like("%t.me/%")
                    | Event.source_post_url.like("%telegram.me/%")
                )
                .where(Event.date >= today)
                .where(Event.lifecycle_status == "active")
                .where(Event.silent.is_(False))
                .where(JobOutbox.id.is_(None))
                .order_by(Event.id.desc())
                .limit(250)
            )
        ).scalars().all()
    repaired = 0
    for event in rows:
        try:
            result = await schedule_event_update_tasks(db, event)
            if JobTask.vk_sync in result:
                repaired += 1
        except Exception:
            logger.warning(
                "tg_monitor: failed to reconcile vk_sync for event_id=%s",
                getattr(event, "id", None),
                exc_info=True,
            )
    if repaired:
        logger.info("tg_monitor.reconciled_vk_sync_jobs count=%s", repaired)
    return repaired


def _global_vk_reconcile_enabled() -> bool:
    return (os.getenv("TG_MONITORING_GLOBAL_VK_RECONCILE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


async def _single_event_id_for_source_url(db: Database, source_url: str | None) -> int | None:
    source = _clean_url(source_url)
    if not source:
        return None
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(EventSource.event_id)
                .where(EventSource.source_type == "telegram")
                .where(EventSource.source_url == source)
                .limit(2)
            )
        ).all()
    ids = {int(row[0]) for row in rows if row and row[0]}
    if len(ids) == 1:
        return next(iter(ids))
    return None


async def process_telegram_results(
    results_path: str | Path,
    db: Database,
    *,
    bot: Any | None = None,
    progress_callback: TelegramMonitorProgressCallback | None = None,
) -> TelegramMonitorReport:
    path = Path(results_path)
    if not path.exists():
        raise FileNotFoundError(f"telegram_results.json not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    report = TelegramMonitorReport(
        run_id=data.get("run_id"),
        generated_at=data.get("generated_at"),
    )

    stats = data.get("stats") or {}
    report.sources_total = int(stats.get("sources_total") or 0)
    report.messages_scanned = int(stats.get("messages_scanned") or 0)
    report.messages_with_events = int(stats.get("messages_with_events") or 0)
    report.events_extracted = int(stats.get("events_extracted") or 0)
    logger.info(
        "tg_monitor.results run_id=%s generated_at=%s sources_total=%d messages_scanned=%d messages_with_events=%d events_extracted=%d",
        report.run_id,
        report.generated_at,
        report.sources_total,
        report.messages_scanned,
        report.messages_with_events,
        report.events_extracted,
    )

    keep_force_message_ids = (os.getenv("TG_MONITORING_KEEP_FORCE_MESSAGE_IDS") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    source_meta_map = _build_sources_meta_map(data.get("sources_meta"))
    if source_meta_map:
        for username, meta in source_meta_map.items():
            try:
                source = await _get_or_create_source(db, username)
                if source.id is None:
                    continue
                await _update_source_metadata(db, source_id=int(source.id), meta=meta)
            except Exception:
                logger.warning(
                    "tg_monitor: failed to persist source metadata username=%s",
                    username,
                    exc_info=True,
                )

    async def _emit_progress(payload: TelegramMonitorImportProgress) -> None:
        if not progress_callback:
            return
        try:
            await progress_callback(payload)
        except Exception:
            logger.warning("tg_monitor.import_progress callback failed", exc_info=True)

    raw_messages = data.get("messages") or []
    messages = _order_messages_chronologically(raw_messages)
    total_messages = len(messages)
    linked_message_index: dict[str, dict[str, Any]] = {}
    linked_posters_cache: dict[str, list[PosterCandidate]] = {}
    linked_text_cache: dict[str, str] = {}
    for msg in raw_messages:
        if not isinstance(msg, dict):
            continue
        source_link = _clean_url(msg.get("source_link"))
        canonical = _canonical_tg_post_url(source_link)
        if not canonical:
            username = normalize_tg_username(msg.get("source_username"))
            mid = _to_int(msg.get("message_id"))
            if username and mid:
                canonical = _canonical_tg_post_url(f"https://t.me/{username}/{int(mid)}")
        if canonical and canonical not in linked_message_index:
            linked_message_index[canonical] = msg
    if total_messages >= 2:
        first = messages[0]
        last = messages[-1]
        logger.info(
            "tg_monitor.messages_ordered run_id=%s total=%s first=%s/%s first_date=%s last=%s/%s last_date=%s",
            report.run_id,
            total_messages,
            normalize_tg_username(first.get("source_username")) or first.get("source_username"),
            first.get("message_id"),
            first.get("message_date"),
            normalize_tg_username(last.get("source_username")) or last.get("source_username"),
            last.get("message_id"),
            last.get("message_date"),
        )

    # Only process messages that can affect events/metrics:
    # - posts with extracted events
    # - forced posts
    # - posts already scanned (metrics-only refresh)
    processed_no = 0
    processable_total: int | None = None
    linked_text_enabled = (os.getenv("TG_MONITORING_LINKED_SOURCES_TEXT") or "").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    try:
        linked_text_limit = int(os.getenv("TG_MONITORING_LINKED_SOURCES_TEXT_LIMIT") or 2)
    except Exception:
        linked_text_limit = 2
    linked_text_limit = max(0, min(5, linked_text_limit))
    # Per-source "poster bridge": sometimes channels post a short text message and then
    # immediately forward a separate poster image (as a separate Telegram message).
    # Kaggle extraction may produce events only for the text message; this bridge
    # lets us attach posters from the next message to the previous imported event.
    poster_bridge: dict[str, dict[str, Any]] = {}

    def _has_events(msg: dict[str, Any]) -> bool:
        ev = msg.get("events")
        return isinstance(ev, list) and len(ev) > 0

    # Best-effort pre-count for nicer progress X/Y.
    try:
        processable_total = sum(1 for m in messages if _has_events(m))
    except Exception:
        processable_total = None

    for _message_no, message in enumerate(messages, start=1):
        message_started_ts = time.monotonic()
        username = normalize_tg_username(message.get("source_username"))
        if not username:
            continue
        post_has_video = message.get("has_video")
        if isinstance(post_has_video, str):
            post_has_video = post_has_video.strip().lower() in {"1", "true", "yes"}
        post_has_video = bool(post_has_video) if isinstance(post_has_video, (bool, int)) else None
        message_videos, post_video_status = _extract_message_videos_payload(message)
        if post_has_video and not post_video_status and message_videos:
            post_video_status = "supabase"
        post_videos_total: int | None = len(message_videos) if message_videos else None
        try:
            post_posters_total = int(len(message.get("posters") or []))
        except Exception:
            post_posters_total = None
        message_id = _to_int(message.get("message_id"))
        if message_id is None:
            report.errors.append(f"missing message_id for source {username}")
            continue
        source_link = message.get("source_link")
        if not source_link and username and message_id:
            source_link = f"https://t.me/{username}/{message_id}"
        source_text = message.get("text") or ""
        raw_events_payload = message.get("events") or []
        indexed_events_payload: list[dict[str, Any]] = []
        if isinstance(raw_events_payload, list):
            for original_index, raw_event in enumerate(raw_events_payload):
                if not isinstance(raw_event, dict):
                    continue
                indexed_event = dict(raw_event)
                indexed_event.setdefault("_telegram_result_index", int(original_index))
                indexed_event.setdefault("_telegram_result_indexes", [int(original_index)])
                indexed_events_payload.append(indexed_event)
        events = _dedupe_message_events(
            indexed_events_payload,
            username=username,
            message_id=message_id,
        )
        events = _expand_events_from_poster_datetime_pairs(
            message,
            events,
            username=username,
            message_id=message_id,
        )
        events = _correct_single_event_from_poster_datetime_pair(
            message,
            events,
            username=username,
            message_id=message_id,
        )
        events_extracted = int(len(events) if isinstance(events, list) else 0)
        source_decision = _source_parse_decision(message)
        source_disposition = _source_parse_disposition(source_decision)
        evidence_retry_reason = _source_evidence_incomplete_reason(
            message, source_decision
        )
        events_imported = 0
        message_created_events: list[TelegramMonitorEventInfo] = []
        message_merged_events: list[TelegramMonitorEventInfo] = []
        created_event_ids: list[int] = []
        merged_event_ids: list[int] = []
        event_ids_by_original_index: dict[int, set[int]] = defaultdict(set)
        accepted_candidate_identity_by_event_id: dict[int, tuple[str | None, str | None]] = {}
        added_posters_total = 0
        # Positive children may still be persisted from partial evidence, but
        # the carrier itself must remain due until enrichment is complete.
        message_retry_scheduled = bool(evidence_retry_reason)
        if events_extracted <= 0 and not _source_zero_event_is_confirmed(
            message, source_decision
        ):
            message_retry_scheduled = True

        def _sorted_breakdown(value: dict[str, int] | defaultdict[str, int] | None) -> dict[str, int]:
            if not value:
                return {}
            return dict(sorted(dict(value).items(), key=lambda kv: (-kv[1], kv[0])))

        progress_current_no = 0
        progress_total_no = 0

        async def _notify_done(
            *,
            status: str,
            reason: str | None = None,
            events_extracted_override: int | None = None,
            events_imported_override: int | None = None,
            metrics_payload: dict[str, Any] | None = None,
            popularity_payload: str | None = None,
            skip_breakdown_payload: dict[str, int] | defaultdict[str, int] | None = None,
        ) -> None:
            extracted_i = int(events_extracted if events_extracted_override is None else events_extracted_override)
            imported_i = int(events_imported if events_imported_override is None else events_imported_override)
            await _emit_progress(
                TelegramMonitorImportProgress(
                    stage="done",
                    status=status,
                    current_no=int(progress_current_no),
                    total_no=int(progress_total_no),
                    source_username=username,
                    source_title=source_title or None,
                    message_id=int(message_id),
                    source_link=source_link,
                    events_extracted=extracted_i,
                    events_imported=imported_i,
                    created_events=list(message_created_events),
                    merged_events=list(message_merged_events),
                    created_event_ids=list(created_event_ids),
                    merged_event_ids=list(merged_event_ids),
                    added_posters_total=int(added_posters_total),
                    metrics=metrics_payload,
                    popularity=popularity_payload,
                    skip_breakdown=_sorted_breakdown(skip_breakdown_payload),
                    reason=reason,
                    took_sec=float(time.monotonic() - message_started_ts),
                    report_events_created_total=int(report.events_created),
                    report_events_merged_total=int(report.events_merged),
                    post_has_video=post_has_video,
                    post_posters_total=post_posters_total,
                    post_videos_total=post_videos_total,
                    post_video_status=post_video_status,
                )
            )

        source = await _get_or_create_source(db, username)
        if source.id is None:
            report.errors.append(f"missing source_id for telegram source {username}")
            continue
        # Backfill channel title for operator UX. Prefer source-level metadata title (schema v2),
        # then message-provided title (schema v1/v2),
        # then DB. Title must come from Kaggle/Telethon (server must not call Telegram APIs).
        source_meta = source_meta_map.get(username) or {}
        source_title = str(source_meta.get("title") or "").strip()
        if not source_title:
            source_title = (message.get("source_title") or message.get("chat_title") or "").strip()
        if not source_title:
            source_title = (source.title or "").strip()
        if source_title and source.id:
            try:
                await _update_source_title(db, source_id=int(source.id), title=source_title)
            except Exception:
                logger.warning("tg_monitor: failed to persist channel title", exc_info=True)
        filters = _source_filters(source)

        forced = await _is_force_message(db, source_id=int(source.id), message_id=int(message_id))
        existing = await _is_message_scanned(db, int(source.id), int(message_id))

        # "Poster bridge" detection (before early-continue on no-events messages).
        message_dt = _parse_datetime(message.get("message_date"))
        bridge_target = None
        if events_extracted <= 0 and (not forced) and (not existing):
            try:
                posters_payload = message.get("posters") or []
                has_posters = isinstance(posters_payload, list) and len(posters_payload) > 0
            except Exception:
                has_posters = False
            if has_posters:
                tgt = poster_bridge.get(username)
                if isinstance(tgt, dict):
                    try:
                        tgt_dt = tgt.get("message_dt")
                        tgt_mid = int(tgt.get("message_id") or 0)
                        tgt_eid = int(tgt.get("event_id") or 0)
                        tgt_link = str(tgt.get("source_link") or "").strip()
                    except Exception:
                        tgt_dt = None
                        tgt_mid = 0
                        tgt_eid = 0
                        tgt_link = ""
                    # Tight constraints: next message (or near-next), small time delta, and only 1 target event.
                    if tgt_mid and tgt_eid and tgt_link:
                        ok_id = (int(message_id) == int(tgt_mid) + 1) or (0 < int(message_id) - int(tgt_mid) <= 2)
                        ok_dt = True
                        if isinstance(message_dt, datetime) and isinstance(tgt_dt, datetime):
                            ok_dt = abs((message_dt - tgt_dt).total_seconds()) <= 240.0
                        # Also require the poster-only message to be short; avoid accidental bridging on long posts.
                        text_s = str(message.get("text") or "").strip()
                        ok_text = (not text_s) or (len(text_s) <= 70)
                        if ok_id and ok_dt and ok_text:
                            bridge_target = tgt
                if bridge_target is None and int(message_id) > 1:
                    # Fallback for reimports: resolve the immediately previous post directly from DB anchor.
                    # This allows "text message already scanned previously" + "poster follow-up message" runs
                    # to still attach posters.
                    text_s = str(message.get("text") or "").strip()
                    if text_s and len(text_s) > 70:
                        # Avoid accidental bridging for long "real" posts.
                        pass
                    elif not isinstance(message_dt, datetime):
                        # Without a reliable timestamp, we can't enforce a safe time delta constraint.
                        pass
                    else:
                        prev_mid = int(message_id) - 1
                        prev_link = f"https://t.me/{username}/{prev_mid}"
                        try:
                            from models import Event

                            async with db.get_session() as session:
                                prev_scan = await session.get(
                                    TelegramScannedMessage, (int(source.id), int(prev_mid))
                                )
                                prev_dt = getattr(prev_scan, "message_date", None) if prev_scan else None
                                ok_dt = False
                                if isinstance(prev_dt, datetime):
                                    ok_dt = abs((message_dt - prev_dt).total_seconds()) <= 240.0
                                if not ok_dt:
                                    raise RuntimeError("bridge fallback time delta too large/unknown")

                                rows = (
                                    await session.execute(
                                        select(Event.id, Event.photo_count)
                                        .where(
                                            Event.source_post_url == prev_link,
                                            Event.source_message_id == prev_mid,
                                        )
                                        .limit(2)
                                    )
                                ).all()
                            if len(rows) == 1 and int(rows[0][1] or 0) <= 0:
                                bridge_target = {
                                    "event_id": int(rows[0][0]),
                                    "message_id": int(prev_mid),
                                    "message_dt": prev_dt,
                                    "source_link": str(prev_link),
                                }
                        except Exception:
                            pass

        # A zero-event carrier is terminal only after a complete typed LLM
        # CONFIRMED_NO_EVENT verdict.  Legacy/unknown/technical empty results
        # remain forced and therefore automatically reclaimable.
        if events_extracted <= 0 and not forced and not existing and not bridge_target:
            if _source_zero_event_is_confirmed(message, source_decision):
                await _mark_message_scanned(
                    db,
                    source_id=int(source.id),
                    message_id=int(message_id),
                    message_date=message_dt,
                    status="confirmed_no_event",
                    events_extracted=0,
                    events_imported=0,
                    error=None,
                )
                await _update_source_scan_meta(db, int(source.id), int(message_id))
                if forced and not keep_force_message_ids:
                    await _clear_force_message(
                        db, source_id=int(source.id), message_id=int(message_id)
                    )
                continue

            reason = evidence_retry_reason
            if not reason and source_disposition == "RETRY_REQUIRED":
                reason = "source_parse_retry_required"
            if not reason and _message_has_event_like_zero_extraction_signals(message):
                reason = "producer_zero_events:clear_event_signals"
            reason = reason or "producer_zero_events:untyped_or_unknown"
            logger.warning(
                "tg_monitor: zero-event carrier retained for retry source=%s "
                "message_id=%s disposition=%s reason=%s",
                username,
                message_id,
                source_disposition,
                reason,
            )
            await _ensure_force_message(
                db, source_id=int(source.id), message_id=int(message_id)
            )
            await _mark_message_scanned(
                db,
                source_id=int(source.id),
                message_id=int(message_id),
                message_date=message_dt,
                status="retry_scheduled",
                events_extracted=0,
                events_imported=0,
                error=reason,
            )
            report.skipped_posts.append(
                TelegramMonitorSkippedPostInfo(
                    source_username=username,
                    source_title=source_title or None,
                    message_id=int(message_id),
                    source_link=source_link,
                    status="retry_scheduled",
                    reason=reason,
                    events_extracted=0,
                    events_imported=0,
                    skip_breakdown={f"retry_scheduled:{reason}": 1},
                    event_titles=[],
                    source_excerpt=_build_excerpt(source_text),
                )
            )
            continue

        processed_no += 1
        total_no = (
            int(processable_total)
            if isinstance(processable_total, int) and processable_total > 0
            else int(total_messages)
        )
        total_no = max(int(processed_no), int(total_no))
        progress_current_no = int(processed_no)
        progress_total_no = int(total_no)
        await _emit_progress(
            TelegramMonitorImportProgress(
                stage="start",
                status="running",
                current_no=int(progress_current_no),
                total_no=int(progress_total_no),
                source_username=username,
                source_title=source_title or None,
                message_id=int(message_id),
                source_link=source_link,
                events_extracted=events_extracted,
                events_imported=0,
                report_events_created_total=int(report.events_created),
                report_events_merged_total=int(report.events_merged),
                post_has_video=post_has_video,
                post_posters_total=post_posters_total,
                post_videos_total=post_videos_total,
                post_video_status=post_video_status,
            )
        )

        metrics = message.get("metrics") if isinstance(message.get("metrics"), dict) else None
        collected_ts = int(datetime.now(timezone.utc).timestamp())
        # `message_dt` is computed above (used for poster bridge checks).
        message_ts = _dt_to_ts(message_dt)
        age_day = normalize_age_day(compute_age_day(published_ts=message_ts, collected_ts=collected_ts))
        popularity: str | None = None
        # `forced` + `existing` computed above (before early-continue)

        # Poster-only message bridged to a previous event: attach posters and stop.
        if bridge_target:
            try:
                from models import Event

                tgt_eid = int(bridge_target.get("event_id") or 0)
                tgt_mid = int(bridge_target.get("message_id") or 0)
                tgt_link = str(bridge_target.get("source_link") or "").strip()
                if not tgt_eid or not tgt_mid or not tgt_link:
                    raise RuntimeError("invalid poster bridge target")
                posters_payload = message.get("posters") or []
                posters: list[PosterCandidate] = []
                seen_sha: set[str] = set()
                if isinstance(posters_payload, list):
                    for item in posters_payload:
                        if not isinstance(item, dict):
                            continue
                        sha = item.get("sha256")
                        if sha and isinstance(sha, str):
                            sha = sha.strip()
                        if sha and sha in seen_sha:
                            continue
                        if sha:
                            seen_sha.add(sha)
                        posters.append(
                            PosterCandidate(
                                catbox_url=item.get("catbox_url"),
                                supabase_url=item.get("supabase_url"),
                                supabase_path=item.get("supabase_path"),
                                sha256=sha,
                                phash=item.get("phash"),
                                ocr_text=item.get("ocr_text"),
                                ocr_title=item.get("ocr_title"),
                            )
                        )

                async with db.get_session() as session:
                    ev = await session.get(Event, int(tgt_eid)) if tgt_eid else None
                if not ev:
                    raise RuntimeError(f"bridge target event not found: {tgt_eid}")
                if int(getattr(ev, "photo_count", 0) or 0) > 0:
                    skip_breakdown = {"poster_bridge_target_has_posters": 1}
                    await _mark_message_scanned(
                        db,
                        source_id=source.id,
                        message_id=message_id,
                        message_date=message_dt,
                        status="skipped",
                        events_extracted=0,
                        events_imported=0,
                        error="poster_bridge_target_has_posters",
                    )
                    await _update_source_scan_meta(db, source.id, message_id)
                    await _notify_done(
                        status="skipped",
                        reason="poster_bridge_target_has_posters",
                        metrics_payload=None,
                        popularity_payload=None,
                        skip_breakdown_payload=skip_breakdown,
                        events_extracted_override=0,
                        events_imported_override=0,
                    )
                    poster_bridge.pop(username, None)
                    continue

                has_any_ocr = any(
                    bool((p.ocr_text or "").strip() or (p.ocr_title or "").strip())
                    for p in posters
                )
                if not has_any_ocr:
                    skip_breakdown = {"retry_scheduled:poster_bridge_no_ocr": 1}
                    await _ensure_force_message(
                        db, source_id=int(source.id), message_id=int(message_id)
                    )
                    await _mark_message_scanned(
                        db,
                        source_id=source.id,
                        message_id=message_id,
                        message_date=message_dt,
                        status="retry_scheduled",
                        events_extracted=0,
                        events_imported=0,
                        error="poster_bridge_no_ocr",
                    )
                    await _notify_done(
                        status="retry_scheduled",
                        reason="poster_bridge_no_ocr",
                        metrics_payload=None,
                        popularity_payload=None,
                        skip_breakdown_payload=skip_breakdown,
                        events_extracted_override=0,
                        events_imported_override=0,
                    )
                    poster_bridge.pop(username, None)
                    continue

                filtered = _filter_posters_for_event(
                    posters,
                    event_title=str(getattr(ev, "title", "") or "").strip() or None,
                    event_date=str(getattr(ev, "date", "") or "").strip() or None,
                    event_time=str(getattr(ev, "time", "") or "").strip() or None,
                )
                if not filtered:
                    skip_breakdown = {"retry_scheduled:poster_bridge_no_match": 1}
                    await _ensure_force_message(
                        db, source_id=int(source.id), message_id=int(message_id)
                    )
                    await _mark_message_scanned(
                        db,
                        source_id=source.id,
                        message_id=message_id,
                        message_date=message_dt,
                        status="retry_scheduled",
                        events_extracted=0,
                        events_imported=0,
                        error="poster_bridge_no_match",
                    )
                    await _notify_done(
                        status="retry_scheduled",
                        reason="poster_bridge_no_match",
                        metrics_payload=None,
                        popularity_payload=None,
                        skip_breakdown_payload=skip_breakdown,
                        events_extracted_override=0,
                        events_imported_override=0,
                    )
                    poster_bridge.pop(username, None)
                    continue
                posters = list(filtered)

                # Use the *original* post anchor (source_url + source_message_id) so Smart Update
                # merges into the existing event and counts new posters properly.
                candidate = EventCandidate(
                    source_type="telegram",
                    source_url=tgt_link,
                    source_text="",
                    title=str(getattr(ev, "title", "") or "").strip() or None,
                    date=str(getattr(ev, "date", "") or "").strip() or None,
                    time=str(getattr(ev, "time", "") or "").strip(),
                    end_date=str(getattr(ev, "end_date", "") or "").strip() or None,
                    location_name=str(getattr(ev, "location_name", "") or "").strip() or None,
                    location_address=str(getattr(ev, "location_address", "") or "").strip() or None,
                    city=str(getattr(ev, "city", "") or "").strip() or None,
                    posters=posters,
                    poster_scope_hashes=[],
                    source_chat_username=username or None,
                    source_chat_id=_to_int(message.get("source_chat_id")),
                    source_message_id=int(tgt_mid) if tgt_mid else None,
                    trust_level=source.trust_level,
                    occurrence_key=str(bridge_target.get("occurrence_key") or "").strip()
                    or None,
                    candidate_key=str(bridge_target.get("candidate_key") or "").strip()
                    or None,
                )
                result = await smart_event_update(
                    db,
                    candidate,
                    check_source_url=False,
                    schedule_kwargs={"skip_vk_sync": True},
                )
                if result.is_accepted:
                    events_imported = 1
                    merged_event_ids.append(int(result.event_id))
                    report.events_merged += 1
                    try:
                        added_posters = int(getattr(result, "added_posters", 0) or 0)
                    except Exception:
                        added_posters = 0
                    added_posters_total += int(added_posters)
                    info = await _build_event_info(
                        db,
                        event_id=result.event_id,
                        source_link=tgt_link,
                        source_text=None,
                        metrics=None,
                        added_posters=added_posters,
                        popularity=None,
                        queue_notes=["🖼️ Афиша из пересланного сообщения прикреплена к предыдущему посту"],
                    )
                    if info:
                        message_merged_events.append(info)
                        report.merged_events.append(info)
                else:
                    terminal_status = (
                        "rejected_product_policy"
                        if result.is_rejected
                        else "retry_scheduled"
                    )
                    await _mark_message_scanned(
                        db,
                        source_id=source.id,
                        message_id=message_id,
                        message_date=message_dt,
                        status=terminal_status,
                        events_extracted=0,
                        events_imported=0,
                        error=result.reason,
                    )
                    if result.is_retry:
                        await _ensure_force_message(
                            db,
                            source_id=int(source.id),
                            message_id=int(message_id),
                        )
                    else:
                        await _update_source_scan_meta(db, source.id, message_id)
                    await _notify_done(
                        status=terminal_status,
                        reason=result.reason,
                        metrics_payload=None,
                        popularity_payload=None,
                        skip_breakdown_payload={terminal_status: 1},
                        events_extracted_override=0,
                        events_imported_override=0,
                    )
                    poster_bridge.pop(username, None)
                    continue
                skip_breakdown = {"poster_bridge": 1}
                await _mark_message_scanned(
                    db,
                    source_id=source.id,
                    message_id=message_id,
                    message_date=message_dt,
                    status="poster_bridge",
                    events_extracted=0,
                    events_imported=int(events_imported),
                    error=None,
                )
                await _update_source_scan_meta(db, source.id, message_id)
                await _notify_done(
                    status="done",
                    reason="poster_bridge",
                    metrics_payload=None,
                    popularity_payload=None,
                    skip_breakdown_payload=skip_breakdown,
                    events_extracted_override=0,
                    events_imported_override=int(events_imported),
                )
                # One-shot: clear the bridge target so we don't apply it to unrelated posts.
                poster_bridge.pop(username, None)
                continue
            except Exception:
                message_retry_scheduled = True
                evidence_retry_reason = "poster_bridge_exception"
                logger.warning(
                    "tg_monitor.poster_bridge failed source=%s message_id=%s",
                    username,
                    message_id,
                    exc_info=True,
                )

        # Legacy source filters are diagnostics only.  They may trigger
        # producer-side verification, but cannot terminate an LLM-produced
        # carrier or child at this caller boundary.
        if filters.get("skip_ads") and _is_ads_message(source_text):
            logger.info(
                "tg_monitor.message semantic_hint=possible_ad run_id=%s source=%s message_id=%s",
                report.run_id,
                username,
                message_id,
            )
        if filters.get("skip_esoterica") and _is_esoterica_message(source_text):
            logger.info(
                "tg_monitor.message semantic_hint=possible_esoterica run_id=%s source=%s message_id=%s",
                report.run_id,
                username,
                message_id,
            )
        if not source.enabled:
            report.messages_skipped += 1
            logger.info(
                "tg_monitor.message skip reason=source_disabled run_id=%s source=%s message_id=%s",
                report.run_id,
                username,
                message_id,
            )
            await _mark_message_scanned(
                db,
                source_id=source.id,
                message_id=message_id,
                message_date=_parse_datetime(message.get("message_date")),
                status="skipped",
                events_extracted=0,
                events_imported=0,
                error="source_disabled",
            )
            report.skipped_posts.append(
                TelegramMonitorSkippedPostInfo(
                    source_username=username,
                    source_title=source_title or None,
                    message_id=message_id,
                    source_link=source_link,
                    status="skipped",
                    reason="source_disabled",
                    events_extracted=0,
                    events_imported=0,
                    skip_breakdown={"source_disabled": 1},
                    event_titles=[],
                    source_excerpt=_build_excerpt(source_text),
                )
            )
            await _notify_done(
                status="skipped",
                reason="source_disabled",
                metrics_payload=metrics,
                popularity_payload=popularity,
                skip_breakdown_payload={"source_disabled": 1},
            )
            if forced and not keep_force_message_ids:
                await _clear_force_message(db, source_id=int(source.id), message_id=int(message_id))
            continue

        # Metrics: persist only for posts that represent real event content (events/forced/existing).
        # Poster-bridge messages are non-event "attachments" and must not pollute popularity baselines.
        if metrics and (events_extracted > 0 or forced or existing) and isinstance(message_id, int) and isinstance(age_day, int) and age_day >= 0:
            try:
                views = metrics.get("views")
                likes = metrics.get("likes")
                comments = metrics.get("comments")
                reactions = metrics.get("reactions")
                await upsert_telegram_post_metric(
                    db,
                    source_id=int(source.id),
                    message_id=int(message_id),
                    age_day=int(age_day),
                    source_url=source_link,
                    message_ts=message_ts,
                    views=int(views) if isinstance(views, int) else None,
                    likes=int(likes) if isinstance(likes, int) else None,
                    comments=int(comments) if isinstance(comments, int) else None,
                    reactions=reactions if isinstance(reactions, dict) else None,
                    collected_ts=int(collected_ts),
                )
                # Do not cache baselines during a run: as soon as the first messages of a
                # channel are imported, the DB baseline sample grows (and early caching would
                # freeze it at 0). This keeps ⭐/👍 markers responsive even on the first run.
                baseline = await load_telegram_popularity_baseline(
                    db,
                    source_id=int(source.id),
                    age_day=int(age_day),
                    now_ts=int(collected_ts),
                )
                marks: PopularityMarks = popularity_marks(
                    views=views if isinstance(views, int) else None,
                    likes=likes if isinstance(likes, int) else None,
                    baseline=baseline,
                )
                popularity = marks.text or None
            except Exception:
                logger.warning(
                    "tg_monitor.metrics persist failed source=%s message_id=%s",
                    username,
                    message_id,
                    exc_info=True,
                )

        if existing and not forced:
            if await _should_reprocess_incomplete_scan(
                db,
                existing=existing,
                source_url=source_link,
                events=events,
            ):
                logger.info(
                    "tg_monitor.message reprocess_incomplete_scan run_id=%s source=%s message_id=%s status=%s extracted=%s imported=%s",
                    report.run_id,
                    username,
                    message_id,
                    existing.status,
                    existing.events_extracted,
                    existing.events_imported,
                )
            else:
                report.messages_metrics_only += 1
                report.events_extracted_metrics_only += int(existing.events_extracted or 0)
                logger.info(
                    "tg_monitor.message metrics_only run_id=%s source=%s message_id=%s",
                    report.run_id,
                    username,
                    message_id,
                )
                info = TelegramMonitorSkippedPostInfo(
                    source_username=username,
                    source_title=source_title or None,
                    message_id=message_id,
                    source_link=source_link,
                    status="metrics_only",
                    reason="already_scanned",
                    events_extracted=int(existing.events_extracted or 0),
                    events_imported=int(existing.events_imported or 0),
                    skip_breakdown={"metrics_only": 1},
                    event_titles=[],
                    source_excerpt=_build_excerpt(source_text),
                    metrics=metrics,
                    popularity=popularity,
                )
                report.metrics_only_posts.append(info)
                if popularity:
                    report.popular_posts.append(info)
                await _update_source_scan_meta(db, int(source.id), int(message_id))
                await _notify_done(
                    status="metrics_only",
                    reason="already_scanned",
                    events_extracted_override=int(existing.events_extracted or 0),
                    events_imported_override=int(existing.events_imported or 0),
                    metrics_payload=metrics,
                    popularity_payload=popularity,
                    skip_breakdown_payload={"metrics_only": 1},
                )
                continue
        if forced:
            report.messages_forced += 1
        else:
            report.messages_new += 1

        try:
            report.events_extracted_new += int(events_extracted)
        except Exception:
            pass

        # If the monitor didn't pre-assign posters per event, try to do it here.
        # Multi-event posts (repertoire/schedule) often contain several posters;
        # attaching all posters to every extracted event produces "foreign" posters
        # on Telegraph pages.
        try:
            posters_payload = message.get("posters") or []
            has_event_level_posters = any(
                isinstance(ev, dict) and bool(ev.get("posters")) for ev in events
            )
            if posters_payload and len(events) >= 2 and not has_event_level_posters:
                poster_candidates: list[PosterCandidate] = []
                for item in posters_payload:
                    if not isinstance(item, dict):
                        continue
                    poster_candidates.append(
                        PosterCandidate(
                            catbox_url=item.get("catbox_url"),
                            supabase_url=item.get("supabase_url"),
                            supabase_path=item.get("supabase_path"),
                            sha256=item.get("sha256"),
                            phash=item.get("phash"),
                            ocr_text=item.get("ocr_text"),
                            ocr_title=item.get("ocr_title"),
                        )
                    )

                assignments: list[list[dict[str, Any]]] = [[] for _ in events]
                for idx, poster in enumerate(poster_candidates):
                    src_item = posters_payload[idx]
                    best_i: int | None = None
                    best_score = 0
                    for i, ev in enumerate(events):
                        if not isinstance(ev, dict):
                            continue
                        score = _poster_match_score(
                            poster,
                            event_title=str(ev.get("title") or "") or None,
                            event_date=str(ev.get("date") or "") or None,
                            event_time=str(ev.get("time") or "") or None,
                        )
                        if score > best_score:
                            best_score = score
                            best_i = i
                    # Use the same minimal threshold as _filter_posters_for_event.
                    if best_i is not None and best_score >= 6:
                        assignments[best_i].append(src_item)

                # OCR can be empty for text-heavy posters. In schedule-like posts where
                # monitor extracted N events and there are exactly N posters, prefer
                # deterministic positional mapping over dropping all posters.
                assigned_total = sum(len(items) for items in assignments)
                schedule_like = len(_SCHED_LINE_RE.findall(str(message.get("text") or ""))) >= 2
                if (
                    assigned_total == 0
                    and len(events) >= 2
                    and len(posters_payload) == len(events)
                ):
                    # Relaxed mapping: if we have exactly N posters for N extracted events, map
                    # them positionally even when the post body isn't strictly schedule-like.
                    # This prevents every event from inheriting all posters (foreign images).
                    unique_hashes = {
                        str((p or {}).get("sha256") or "").strip()
                        for p in posters_payload
                        if isinstance(p, dict) and str((p or {}).get("sha256") or "").strip()
                    }
                    allow_positional = schedule_like or (
                        len(unique_hashes) == len(posters_payload) and len(events) <= 4
                    )
                    if allow_positional:
                        for i, ev in enumerate(events):
                            if isinstance(ev, dict):
                                ev["posters"] = [posters_payload[i]]
                        logger.info(
                            "telegram: posters fallback positional mapping source=%s message_id=%s count=%s",
                            username,
                            message_id,
                            len(events),
                        )

                for i, ev in enumerate(events):
                    if not isinstance(ev, dict):
                        continue
                    if assignments[i]:
                        ev["posters"] = assignments[i]
        except Exception:
            logger.warning("telegram: poster assignment failed", exc_info=True)
        logger.info(
            "tg_monitor.message start run_id=%s source=%s message_id=%s events=%d",
            report.run_id,
            username,
            message_id,
            events_extracted,
        )

        bridge_notice_sent = False
        bridge_photo_url = _first_photo_url(message)

        skip_breakdown: dict[str, int] = defaultdict(int)
        event_titles: list[str] = []
        scraped_posters: list[PosterCandidate] | None = None
        scraped_full_text: str | None = None
        is_single_event_post = len(events) <= 1

        for transformed_event_index, event_data in enumerate(events):
            try:
                original_event_indexes = [transformed_event_index]
                if isinstance(event_data, dict):
                    original_event_indexes = _video_event_indexes(
                        event_data.get("_telegram_result_indexes")
                    ) or _video_event_indexes(
                        [event_data.get("_telegram_result_index", transformed_event_index)]
                    ) or [transformed_event_index]
                if _looks_like_poster_only_non_event(message, event_data if isinstance(event_data, dict) else {}):
                    report.events_filtered += 1
                    report.events_skipped += 1
                    skip_breakdown["skipped_non_event:poster_only"] += 1
                    logger.info(
                        "tg_monitor.event skip reason=poster_only source=%s message_id=%s title=%s",
                        username,
                        message_id,
                        str((event_data or {}).get("title") or "")[:80] if isinstance(event_data, dict) else "",
                    )
                    continue
                candidate = _build_candidate(source, message, event_data)
                candidate.producer_ordinal = transformed_event_index
                if candidate.title:
                    event_titles.append(str(candidate.title).strip())
                # Linked-source enrichment: when parser provides `linked_source_urls`,
                # import their posters as part of the same event candidate.
                linked_urls = (
                    list(event_data.get("linked_source_urls") or [])
                    if isinstance(event_data, dict)
                    else []
                )
                if is_single_event_post and linked_urls:
                    try:
                        linked_posters = await _collect_linked_source_posters(
                            source_username=username,
                            source_message_id=message_id,
                            linked_urls=linked_urls,
                            message_index=linked_message_index,
                            cache=linked_posters_cache,
                            per_post_limit=2,
                            total_limit=5,
                        )
                        if linked_posters:
                            merged = _dedupe_poster_candidates(
                                list(candidate.posters or []) + linked_posters,
                                limit=5,
                            )
                            if len(merged) > len(list(candidate.posters or [])):
                                candidate.posters = list(merged)
                                scope_hashes = {h for h in (candidate.poster_scope_hashes or []) if h}
                                scope_hashes.update(
                                    str(p.sha256 or "").strip()
                                    for p in linked_posters
                                    if str(p.sha256 or "").strip()
                                )
                                candidate.poster_scope_hashes = sorted(scope_hashes)
                    except Exception:
                        logger.debug(
                            "tg_monitor.linked_posters failed source=%s message_id=%s",
                            username,
                            message_id,
                            exc_info=True,
                        )
                # Fallback poster scraping: when the payload brought no poster URLs at all.
                # Do it once per message and reuse for all extracted events if needed.
                if not _has_poster_urls(list(candidate.posters or [])):
                    try:
                        if scraped_posters is None:
                            fallback_limit = 2 if is_single_event_post else 5
                            scraped_posters = await _fallback_fetch_posters_from_public_tg_page(
                                username=username,
                                message_id=int(message_id or 0),
                                limit=fallback_limit,
                                need_ocr=not is_single_event_post,
                            )
                            logger.info(
                                "tg_monitor.poster_fallback source=%s message_id=%s posters=%d",
                                username,
                                message_id,
                                len(scraped_posters or []),
                            )
                        selected_fallback_posters = _select_public_page_fallback_posters(
                            candidate,
                            scraped_posters,
                            is_single_event_post=is_single_event_post,
                        )
                        if selected_fallback_posters:
                            candidate.posters = list(selected_fallback_posters)
                            scope_hashes = {h for h in (candidate.poster_scope_hashes or []) if h}
                            scope_hashes.update(
                                str(p.sha256 or "").strip()
                                for p in (scraped_posters or [])
                                if str(p.sha256 or "").strip()
                            )
                            candidate.poster_scope_hashes = sorted(scope_hashes)
                    except Exception:
                        logger.debug(
                            "tg_monitor.poster_fallback failed source=%s message_id=%s",
                            username,
                            message_id,
                            exc_info=True,
                        )
                # Fallback full text fetch: some upstream exports can truncate `message["text"]`,
                # leading to missing lines (e.g. supporting acts). Fetch once per message.
                if is_single_event_post and _looks_like_truncated_message_text(candidate.source_text):
                    try:
                        if scraped_full_text is None:
                            scraped_full_text = await _fallback_fetch_full_text_from_public_tg_page(
                                username=username,
                                message_id=int(message_id or 0),
                            )
                        orig = str(candidate.source_text or "").strip()
                        scraped_s = str(scraped_full_text or "").strip()
                        if (
                            scraped_s
                            and len(scraped_s) >= len(orig) + 5
                            and not _TG_PUBLIC_TEXT_TRUNCATION_RE.search(scraped_s)
                        ):
                            candidate.source_text = _filter_schedule_source_text(
                                scraped_s,
                                event_date=str(candidate.date or "").strip() or None,
                                event_title=str(candidate.title or "").strip() or None,
                            )
                    except Exception:
                        logger.debug(
                            "tg_monitor.text_fallback failed source=%s message_id=%s",
                            username,
                            message_id,
                            exc_info=True,
                        )
                if filters.get("skip_recurring_excursions") and _looks_like_recurring_excursion(
                    source_text, candidate
                ):
                    logger.info(
                        "tg_monitor.event semantic_hint=possible_recurring_excursion "
                        "source=%s message_id=%s title=%s",
                        username,
                        message_id,
                        (candidate.title or "")[:80],
                    )
                if _should_skip_past_event_candidate(candidate):
                    logger.info(
                        "tg_monitor.event semantic_hint=possible_past_event source=%s "
                        "message_id=%s title=%s date=%s end_date=%s event_type=%s",
                        username,
                        message_id,
                        (candidate.title or "")[:80],
                        candidate.date,
                        candidate.end_date,
                        candidate.event_type,
                    )
                # The primary Telegram import is a real event-ingest boundary:
                # Smart Update must enqueue the standard publication jobs,
                # including VK sync, so promo events can flow through VK and
                # downstream promo surfaces. Auxiliary linked-source passes
                # below still skip VK sync to avoid duplicate publication work.
                result: SmartUpdateResult = await smart_event_update(
                    db,
                    candidate,
                    check_source_url=False,
                )
                linked_added = 0
                if result.is_accepted:
                    for original_event_index in original_event_indexes:
                        event_ids_by_original_index[int(original_event_index)].add(
                            int(result.event_id)
                        )
                    accepted_candidate_identity_by_event_id[int(result.event_id)] = (
                        candidate.occurrence_key,
                        candidate.candidate_key,
                    )
                    linked_added = await _attach_linked_sources(
                        db,
                        event_id=result.event_id,
                        linked_urls=list(event_data.get("linked_source_urls") or []),
                        trust_level=source.trust_level,
                    )
                    if is_single_event_post and linked_urls and linked_text_enabled and linked_text_limit:
                        current_url = _canonical_tg_post_url(candidate.source_url)
                        effective_linked_urls = [
                            u
                            for u in linked_urls
                            if (not current_url) or (_canonical_tg_post_url(u) != current_url)
                        ]
                        try:
                            linked_texts = await _collect_linked_source_texts(
                                source_username=username,
                                source_message_id=message_id,
                                linked_urls=effective_linked_urls,
                                message_index=linked_message_index,
                                cache=linked_text_cache,
                                total_limit=linked_text_limit,
                                max_chars=9000,
                                event_date=str(candidate.date or "").strip() or None,
                                event_title=str(candidate.title or "").strip() or None,
                            )
                        except Exception:
                            linked_texts = []
                            logger.debug(
                                "tg_monitor.linked_texts failed source=%s message_id=%s",
                                username,
                                message_id,
                                exc_info=True,
                            )
                        for linked_url, linked_uname, linked_mid, linked_text in linked_texts:
                            try:
                                await _attach_linked_sources(
                                    db,
                                    event_id=int(result.event_id),
                                    linked_urls=[linked_url],
                                    trust_level=source.trust_level,
                                    source_text=linked_text,
                                )
                            except Exception:
                                logger.debug(
                                    "tg_monitor.linked_text smart_update exception source=%s message_id=%s linked=%s",
                                    username,
                                    message_id,
                                    linked_url,
                                    exc_info=True,
                                )
                if result.outcome is SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY:
                    try:
                        await _schedule_primary_import_event_tasks(
                            db,
                            int(result.event_id),
                        )
                    except Exception:
                        logger.warning(
                            "tg_monitor: failed to rearm exact-replay event tasks event_id=%s",
                            result.event_id,
                            exc_info=True,
                        )
                if result.outcome is SmartUpdateTerminalOutcome.CREATED:
                    report.events_created += 1
                    events_imported += 1
                    created_event_ids.append(int(result.event_id))
                    try:
                        added_posters = int(getattr(result, "added_posters", 0) or 0)
                    except Exception:
                        added_posters = 0
                    added_posters_total += int(added_posters)
                    info = await _build_event_info(
                        db,
                        event_id=result.event_id,
                        source_link=source_link,
                        source_text=source_text,
                        metrics=metrics,
                        added_posters=added_posters,
                        popularity=popularity,
                        queue_notes=list(getattr(result, "queue_notes", None) or []),
                    )
                    if info:
                        report.created_events.append(info)
                        message_created_events.append(info)
                elif result.outcome is SmartUpdateTerminalOutcome.MERGED:
                    report.events_merged += 1
                    events_imported += 1
                    merged_event_ids.append(int(result.event_id))
                    try:
                        added_posters = int(getattr(result, "added_posters", 0) or 0)
                    except Exception:
                        added_posters = 0
                    added_posters_total += int(added_posters)
                    info = await _build_event_info(
                        db,
                        event_id=result.event_id,
                        source_link=source_link,
                        source_text=source_text,
                        metrics=metrics,
                        added_posters=added_posters,
                        popularity=popularity,
                        queue_notes=list(getattr(result, "queue_notes", None) or []),
                    )
                    if info:
                        report.merged_events.append(info)
                        message_merged_events.append(info)
                elif result.outcome is SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY:
                    report.events_nochange += 1
                    events_imported += 1
                elif result.outcome is SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY:
                    report.events_rejected += 1
                    report.events_skipped += 1
                    key = "rejected_product_policy"
                    if result.reason:
                        key = f"{key}:{result.reason}"
                    skip_breakdown[key] += 1
                else:
                    message_retry_scheduled = True
                    key = "retry_scheduled"
                    if result.reason:
                        key = f"{key}:{result.reason}"
                    skip_breakdown[key] += 1
                logger.info(
                    "tg_monitor.event outcome=%s event_id=%s source=%s message_id=%s title=%s linked_added=%s",
                    result.outcome.value,
                    result.event_id,
                    username,
                    message_id,
                    (candidate.title or "")[:80],
                    linked_added,
                )

                if (
                    bot
                    and not bridge_notice_sent
                    and username == "klgdcity"
                    and filters.get("bridge_notice_daily")
                    and result.is_changed
                    and _is_bridge_notice_message(source_text, candidate)
                ):
                    notice_text = _format_bridge_notice(
                        source_text=source_text,
                        source_link=source_link,
                        candidate=candidate,
                    )
                    try:
                        sent = await _send_bridge_notice_to_daily_channels(
                            db,
                            bot=bot,
                            notice_text=notice_text,
                            photo_url=bridge_photo_url,
                        )
                        logger.info(
                            "tg_monitor.bridge_notice sent=%s source=%s message_id=%s",
                            sent,
                            username,
                            message_id,
                        )
                        bridge_notice_sent = True
                    except Exception:
                        logger.exception(
                            "tg_monitor.bridge_notice failed source=%s message_id=%s",
                            username,
                            message_id,
                        )
            except Exception as exc:
                report.errors.append(f"{username}/{message_id}: retry_scheduled:{exc}")
                message_retry_scheduled = True
                skip_breakdown["retry_scheduled:exception"] += 1
                logger.exception("telegram_results: smart update retry scheduled")
        logger.info(
            "tg_monitor.message done run_id=%s source=%s message_id=%s imported=%d",
            report.run_id,
            username,
            message_id,
            events_imported,
        )

        if events_extracted and (events_imported < events_extracted or skip_breakdown):
            if message_retry_scheduled:
                status = (
                    "partial_retry_scheduled" if events_imported else "retry_scheduled"
                )
            else:
                status = "partial" if events_imported else "skipped"
            report.skipped_posts.append(
                TelegramMonitorSkippedPostInfo(
                    source_username=username,
                    source_title=source_title or None,
                    message_id=message_id,
                    source_link=source_link,
                    status=status,
                    reason=None,
                    events_extracted=int(events_extracted),
                    events_imported=int(events_imported),
                    skip_breakdown=_sorted_breakdown(skip_breakdown),
                    event_titles=event_titles[:6],
                    source_excerpt=_build_excerpt(source_text),
                )
            )

        if message_videos:
            try:
                inserted, total, inserted_by_event = await _persist_event_video_assets(
                    db,
                    event_ids_by_index=event_ids_by_original_index,
                    videos=message_videos,
                    source_url=source_link,
                )
                accepted_videos = [
                    item
                    for item in message_videos
                    if str(item.get("analysis_status") or "").strip().lower()
                    == "accepted"
                ]
                if total > 0 and not post_video_status:
                    post_video_status = "accepted" if accepted_videos else "rejected"
                linked_event_ids = sorted(inserted_by_event)
                for linked_event_id in linked_event_ids:
                    await _requeue_event_telegraph_build(
                        db, event_id=int(linked_event_id)
                    )

                video_totals: dict[int, int] = {}
                event_ids_for_report = {
                    int(info.event_id)
                    for info in list(message_created_events) + list(message_merged_events)
                    if getattr(info, "event_id", None)
                }
                if event_ids_for_report:
                    async with db.get_session() as session:
                        rows = (
                            await session.execute(
                                select(EventVideoLink.event_id, func.count())
                                .where(EventVideoLink.event_id.in_(event_ids_for_report))
                                .group_by(EventVideoLink.event_id)
                            )
                        ).all()
                    video_totals = {
                        int(event_id): int(count or 0) for event_id, count in rows
                    }
                for info in list(message_created_events) + list(message_merged_events):
                    info_event_id = int(getattr(info, "event_id", 0) or 0)
                    if not info_event_id:
                        continue
                    info.added_videos = int(inserted_by_event.get(info_event_id, 0))
                    info.video_count = int(video_totals.get(info_event_id, 0))
                logger.info(
                    "tg_monitor.videos attached source=%s message_id=%s events=%s inserted_links=%s assets=%s",
                    username,
                    message_id,
                    sorted(
                        (index, sorted(event_ids))
                        for index, event_ids in event_ids_by_original_index.items()
                    ),
                    int(inserted),
                    int(total),
                )
                if accepted_videos and not any(
                    item.get("event_indexes") for item in accepted_videos
                ):
                    post_video_status = "skipped:unmatched_event"
                elif accepted_videos and not event_ids_by_original_index:
                    post_video_status = "skipped:no_imported_event"
            except Exception:
                logger.warning(
                    "tg_monitor.videos attach failed source=%s message_id=%s",
                    username,
                    message_id,
                    exc_info=True,
                )
                if not post_video_status:
                    post_video_status = "skipped:attach_error"

        if message_retry_scheduled and not skip_breakdown:
            retry_reason = evidence_retry_reason
            if not retry_reason and source_disposition == "RETRY_REQUIRED":
                retry_reason = "source_parse_retry_required"
            retry_reason = retry_reason or "source_parse_unresolved"
            skip_breakdown[f"retry_scheduled:{retry_reason}"] += 1

        final_status = "done"
        if message_retry_scheduled:
            final_status = (
                "partial_retry_scheduled" if events_imported else "retry_scheduled"
            )
        elif events_extracted and events_imported <= 0:
            final_status = "skipped"
        elif events_extracted and events_imported < events_extracted:
            final_status = "partial"
        await _mark_message_scanned(
            db,
            source_id=source.id,
            message_id=message_id,
            message_date=message_dt,
            status=final_status,
            events_extracted=events_extracted,
            events_imported=events_imported,
            error=_scan_error_from_breakdown(final_status, skip_breakdown),
        )
        if popularity:
            # Keep a short list of posts whose metrics exceed per-channel baselines.
            # These entries power the "🔥 Популярные посты" operator block.
            try:
                report.popular_posts.append(
                    TelegramMonitorSkippedPostInfo(
                        source_username=username,
                        source_title=source_title or None,
                        message_id=int(message_id),
                        source_link=source_link,
                        status="popular",
                        reason=None,
                        events_extracted=int(events_extracted),
                        events_imported=int(events_imported),
                        event_titles=event_titles[:6],
                        source_excerpt=_build_excerpt(source_text),
                        metrics=metrics,
                        popularity=popularity,
                    )
                )
            except Exception:
                pass
        if message_retry_scheduled:
            await _ensure_force_message(
                db, source_id=int(source.id), message_id=int(message_id)
            )
        else:
            await _update_source_scan_meta(db, source.id, message_id)
        await _notify_done(
            status=final_status,
            metrics_payload=metrics,
            popularity_payload=popularity,
            skip_breakdown_payload=skip_breakdown,
        )
        if forced and not message_retry_scheduled and not keep_force_message_ids:
            try:
                await _clear_force_message(db, source_id=int(source.id), message_id=int(message_id))
            except Exception:
                logger.warning(
                    "tg_monitor.force_message cleanup failed source=%s message_id=%s",
                    username,
                    message_id,
                    exc_info=True,
                )

        # Store a bridge target for the next poster-only forwarded message.
        try:
            if (
                events_extracted > 0
                and (post_posters_total is not None and int(post_posters_total) <= 0)
                and int(events_imported or 0) == 1
                and int(added_posters_total or 0) <= 0
                and (len(created_event_ids) + len(merged_event_ids) == 1)
                and source_link
            ):
                eid = (created_event_ids + merged_event_ids)[0]
                occurrence_key, candidate_key = accepted_candidate_identity_by_event_id.get(
                    int(eid),
                    (None, None),
                )
                poster_bridge[username] = {
                    "event_id": int(eid),
                    "message_id": int(message_id),
                    "message_dt": message_dt,
                    "source_link": str(source_link).strip(),
                    "occurrence_key": occurrence_key,
                    "candidate_key": candidate_key,
                }
            else:
                # If the message had posters (or multiple events), do not keep a stale bridge target.
                if username in poster_bridge:
                    poster_bridge.pop(username, None)
        except Exception:
            pass

    if _global_vk_reconcile_enabled():
        try:
            await _reconcile_primary_import_vk_sync_jobs(db)
        except Exception:
            logger.warning("tg_monitor: failed to reconcile primary import vk_sync jobs", exc_info=True)

    return report
