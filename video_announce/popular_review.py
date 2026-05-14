from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
from urllib.parse import urlparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from handlers.popular_posts_cmd import _load_top_items, _resolve_telegraph_map
from models import Event, VideoAnnounceItem, VideoAnnounceSession, VideoAnnounceSessionStatus
from promo import resolve_video_promo_candidates
from .custom_types import RankedEvent

logger = logging.getLogger(__name__)

POPULAR_REVIEW_PROFILE = "popular_review"
POPULAR_REVIEW_TARGET_USERNAME = "keniggpt"
POPULAR_REVIEW_INTRO_TEXT = "ВЫБЕРИ СОБЫТИЕ"
POPULAR_REVIEW_MIN_EVENTS = 2
POPULAR_REVIEW_MAX_EVENTS = 6
POPULAR_REVIEW_ANTI_REPEAT_DAYS = 7
POPULAR_REVIEW_CANDIDATE_LIMIT = 40
POPULAR_REVIEW_WINDOW_CHAIN: tuple[tuple[int, int, str], ...] = (
    (1, 0, "24h"),
    (3, 2, "3d"),
    (7, 6, "7d"),
)
RECENT_PUBLISHED_VIDEO_SESSION_STATUSES = {
    VideoAnnounceSessionStatus.PUBLISHED_TEST,
    VideoAnnounceSessionStatus.PUBLISHED_MAIN,
}

POPULAR_REVIEW_RENDERABLE_IMAGE_LIMIT = 3
REHYDRATED_PHOTO_PERSIST_LOCK_DELAYS_SEC: tuple[float, ...] = (
    0.25,
    0.5,
    1.0,
    2.0,
    3.0,
    5.0,
)


@dataclass(frozen=True)
class PopularReviewPick:
    event: Event
    score: float
    source_window: str
    source_post_url: str
    source_label: str
    anti_repeat_status: str
    description: str
    promo_campaign_id: int | None = None
    promo_activity_id: int | None = None
    promo_placement_kind: str | None = None


@dataclass(frozen=True)
class PopularReviewSelection:
    picks: list[PopularReviewPick]
    ranked: list[RankedEvent]
    trace: dict[int, dict[str, Any]]

    @property
    def event_ids(self) -> list[int]:
        return [int(item.event.id) for item in self.picks if item.event.id is not None]


def _promo_starts_first(promo_picks: list[PopularReviewPick], *, now_utc: datetime) -> bool:
    ids = ",".join(
        str(int(pick.event.id))
        for pick in promo_picks
        if pick.event.id is not None
    )
    seed = f"{now_utc.date().isoformat()}|{ids}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    return int(digest[:2], 16) % 2 == 0


def _merge_promo_and_fresh_picks(
    promo_picks: list[PopularReviewPick],
    fresh: list[PopularReviewPick],
    *,
    max_events: int,
    now_utc: datetime,
) -> list[PopularReviewPick]:
    selected: list[PopularReviewPick] = []
    selected_event_ids: set[int] = set()

    def add_pick(pick: PopularReviewPick | None) -> None:
        if pick is None or len(selected) >= max_events:
            return
        event_id = int(pick.event.id)
        if event_id in selected_event_ids:
            return
        selected.append(pick)
        selected_event_ids.add(event_id)

    if promo_picks and fresh:
        promo_first = _promo_starts_first(promo_picks, now_utc=now_utc)
        promo_idx = 0
        fresh_idx = 0
        if promo_first:
            add_pick(promo_picks[promo_idx])
            promo_idx += 1
        while len(selected) < max_events and (fresh_idx < len(fresh) or promo_idx < len(promo_picks)):
            if fresh_idx < len(fresh):
                add_pick(fresh[fresh_idx])
                fresh_idx += 1
            if promo_idx < len(promo_picks):
                add_pick(promo_picks[promo_idx])
                promo_idx += 1
        return selected

    for candidate in [*promo_picks, *fresh]:
        add_pick(candidate)
        if len(selected) >= max_events:
            break
    return selected


def _parse_iso_date(raw: str | None) -> date | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value.split("..", 1)[0].strip())
    except ValueError:
        return None


def _starts_today_or_in_future(ev: Event, *, today: date) -> bool:
    start_day = _parse_iso_date(getattr(ev, "date", None))
    if start_day is None:
        return True
    return start_day >= today


def _normalize_description(text: str | None) -> str:
    value = " ".join(str(text or "").strip().split())
    return value


def _event_photo_urls(ev: Event) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for raw in getattr(ev, "photo_urls", None) or []:
        url = str(raw or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _is_catbox_url(url: str | None) -> bool:
    host = urlparse(str(url or "").strip()).netloc.lower()
    return host.endswith("files.catbox.moe")


def _renderable_photo_urls(urls: list[str]) -> list[str]:
    return [
        url
        for url in urls
        if url and not _is_catbox_url(url)
    ][:POPULAR_REVIEW_RENDERABLE_IMAGE_LIMIT]


def _is_sqlite_lock_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return (
        "database is locked" in text
        or "database table is locked" in text
        or "database is busy" in text
    )


async def _rehydrate_public_tg_photo_urls(source_post_url: str | None) -> list[str]:
    if not source_post_url:
        return []
    try:
        from source_parsing.telegram.handlers import (
            _fallback_fetch_posters_from_public_tg_page,
            _parse_tg_source_url,
        )
    except Exception:
        logger.warning(
            "video_announce.popular_review: failed to import telegram public poster fallback",
            exc_info=True,
        )
        return []

    username, message_id = _parse_tg_source_url(source_post_url)
    if not username or not message_id:
        return []

    try:
        posters = await _fallback_fetch_posters_from_public_tg_page(
            username=username,
            message_id=message_id,
            limit=POPULAR_REVIEW_RENDERABLE_IMAGE_LIMIT,
        )
    except Exception:
        logger.warning(
            "video_announce.popular_review: telegram poster rehydrate failed url=%s",
            source_post_url,
            exc_info=True,
        )
        return []

    urls: list[str] = []
    seen: set[str] = set()
    for poster in posters or []:
        candidate = str(
            getattr(poster, "supabase_url", None)
            or getattr(poster, "catbox_url", None)
            or ""
        ).strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        urls.append(candidate)
    return _renderable_photo_urls(urls)


async def _rehydrate_vk_photo_urls(source_post_url: str | None) -> list[str]:
    if not source_post_url:
        return []
    try:
        from vk_auto_queue import fetch_vk_post_text_and_photos
        from vk_intake import _vk_wall_source_ids_from_url
    except Exception:
        logger.warning(
            "video_announce.popular_review: failed to import vk poster fetch helpers",
            exc_info=True,
        )
        return []

    group_id, post_id = _vk_wall_source_ids_from_url(source_post_url)
    if not group_id or not post_id:
        return []

    try:
        _text, photos, _published_at, _metrics, status = await fetch_vk_post_text_and_photos(
            group_id,
            post_id,
            limit=POPULAR_REVIEW_RENDERABLE_IMAGE_LIMIT,
        )
    except Exception:
        logger.warning(
            "video_announce.popular_review: vk poster rehydrate failed url=%s",
            source_post_url,
            exc_info=True,
        )
        return []

    if not status.ok:
        logger.info(
            "video_announce.popular_review: vk poster rehydrate unavailable url=%s kind=%s",
            source_post_url,
            status.kind,
        )
        return []
    return _renderable_photo_urls([str(url or "").strip() for url in photos])


async def _persist_rehydrated_photo_urls(
    db,
    *,
    event_id: int | None,
    photo_urls: list[str],
) -> bool:
    if event_id is None or not photo_urls:
        return False

    last_lock_error: OperationalError | None = None
    for attempt in range(len(REHYDRATED_PHOTO_PERSIST_LOCK_DELAYS_SEC) + 1):
        try:
            async with db.get_session() as session:
                fresh = await session.get(Event, int(event_id))
                if fresh is None:
                    return False
                fresh.photo_urls = list(photo_urls)
                fresh.photo_count = len(photo_urls)
                session.add(fresh)
                await session.commit()
            return True
        except OperationalError as exc:
            if not _is_sqlite_lock_error(exc):
                raise
            last_lock_error = exc
            if attempt >= len(REHYDRATED_PHOTO_PERSIST_LOCK_DELAYS_SEC):
                break
            delay = REHYDRATED_PHOTO_PERSIST_LOCK_DELAYS_SEC[attempt]
            logger.warning(
                "video_announce.popular_review: sqlite locked while persisting rehydrated poster urls "
                "event_id=%s attempt=%s retry_in=%.2fs",
                event_id,
                attempt + 1,
                delay,
            )
            await asyncio.sleep(delay)

    logger.warning(
        "video_announce.popular_review: skipped rehydrated poster urls after sqlite lock retries "
        "event_id=%s count=%s error=%s",
        event_id,
        len(photo_urls),
        last_lock_error,
    )
    return False


async def _ensure_renderable_photo_urls(ev: Event, *, db=None) -> list[str]:
    direct_urls = _renderable_photo_urls(_event_photo_urls(ev))
    if direct_urls:
        return direct_urls

    source_urls: list[str] = []
    for raw in (
        getattr(ev, "source_post_url", None),
        getattr(ev, "source_vk_post_url", None),
    ):
        source_url = str(raw or "").strip()
        if source_url and source_url not in source_urls:
            source_urls.append(source_url)

    for source_url in source_urls:
        low = source_url.lower()
        if "t.me/" in low or "telegram.me/" in low:
            refreshed = await _rehydrate_public_tg_photo_urls(source_url)
        elif "vk.com/wall" in low:
            refreshed = await _rehydrate_vk_photo_urls(source_url)
        else:
            refreshed = []
        if refreshed:
            ev.photo_urls = list(refreshed)
            ev.photo_count = len(refreshed)
            if db is not None:
                persisted = await _persist_rehydrated_photo_urls(
                    db,
                    event_id=getattr(ev, "id", None),
                    photo_urls=refreshed,
                )
                if not persisted:
                    logger.warning(
                        "video_announce.popular_review: rehydrated poster urls are not durable; "
                        "skipping event_id=%s source=%s count=%s",
                        getattr(ev, "id", None),
                        source_url,
                        len(refreshed),
                    )
                    return []
            logger.info(
                "video_announce.popular_review: rehydrated poster urls event_id=%s source=%s count=%s",
                getattr(ev, "id", None),
                source_url,
                len(refreshed),
            )
            return refreshed
    return []


def preferred_scene_description(ev: Event) -> str:
    for candidate in (
        getattr(ev, "search_digest", None),
        getattr(ev, "short_description", None),
        getattr(ev, "description", None),
    ):
        normalized = _normalize_description(candidate)
        if normalized:
            return normalized
    return ""


async def _load_recent_popular_review_hits(
    db,
    *,
    anti_repeat_days: int,
    now_utc: datetime,
    profile_key: str = POPULAR_REVIEW_PROFILE,
) -> set[int]:
    threshold = now_utc - timedelta(days=max(1, anti_repeat_days))
    async with db.get_session() as session:
        result = await session.execute(
            select(VideoAnnounceItem.event_id)
            .join(
                VideoAnnounceSession,
                VideoAnnounceItem.session_id == VideoAnnounceSession.id,
            )
            .where(VideoAnnounceSession.profile_key == profile_key)
            .where(VideoAnnounceSession.status.in_(RECENT_PUBLISHED_VIDEO_SESSION_STATUSES))
            .where(VideoAnnounceItem.event_id.is_not(None))
            .where(
                func.coalesce(
                    VideoAnnounceSession.published_at,
                    VideoAnnounceSession.finished_at,
                    VideoAnnounceSession.started_at,
                    VideoAnnounceSession.created_at,
                )
                >= threshold,
            )
        )
        return {int(event_id) for event_id in result.scalars().all() if event_id is not None}


async def _resolve_filter_decision(event_filter, event):
    """Run a sync/async partner filter and return its FilterDecision."""
    result = event_filter(event)
    if inspect.isawaitable(result):
        result = await result
    return result


async def _load_events_map(db, event_ids: list[int]) -> dict[int, Event]:
    if not event_ids:
        return {}
    async with db.get_session() as session:
        result = await session.execute(select(Event).where(Event.id.in_(event_ids)))
        return {int(ev.id): ev for ev in result.scalars().all() if ev.id is not None}


async def _collect_popular_hits(
    db,
    *,
    candidate_limit: int,
    today: date,
) -> list[dict[str, Any]]:
    ordered_hits: list[dict[str, Any]] = []
    seen_event_ids: set[int] = set()
    for window_days, age_day, label in POPULAR_REVIEW_WINDOW_CHAIN:
        items, _debug = await _load_top_items(
            db,
            window_days=window_days,
            age_day=age_day,
            limit=max(1, candidate_limit),
        )
        source_urls = [
            str(getattr(item, "post_url", "") or "").strip()
            for item in items
            if str(getattr(item, "post_url", "") or "").strip()
        ]
        telegraph_map, _matched = await _resolve_telegraph_map(
            db,
            source_urls=source_urls,
            today=today,
        )
        for item in items:
            post_url = str(getattr(item, "post_url", "") or "").strip()
            if not post_url:
                continue
            linked = telegraph_map.get(post_url)
            if not linked:
                continue
            for ref in linked.events:
                event_id = int(ref.event_id)
                if event_id in seen_event_ids:
                    continue
                seen_event_ids.add(event_id)
                ordered_hits.append(
                    {
                        "event_id": event_id,
                        "source_window": label,
                        "source_post_url": post_url,
                        "source_label": str(getattr(item, "source_label", "") or "").strip(),
                        "score": float(getattr(item, "score", 0.0) or 0.0),
                    }
                )
    return ordered_hits


EventFilterFn = Callable[[Event], "FilterDecision | Awaitable[FilterDecision]"]


async def build_popular_review_selection(
    db,
    *,
    max_events: int = POPULAR_REVIEW_MAX_EVENTS,
    min_events: int = POPULAR_REVIEW_MIN_EVENTS,
    anti_repeat_days: int = POPULAR_REVIEW_ANTI_REPEAT_DAYS,
    candidate_limit: int = POPULAR_REVIEW_CANDIDATE_LIMIT,
    now_utc: datetime | None = None,
    profile_key: str = POPULAR_REVIEW_PROFILE,
    event_filter: EventFilterFn | None = None,
    partner_track_id: str | None = None,
    admit_manual_review: bool = True,
) -> PopularReviewSelection:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()
    max_events = max(1, min(int(max_events), POPULAR_REVIEW_MAX_EVENTS))
    min_events = max(1, min(int(min_events), max_events))
    anti_repeat_days = max(1, int(anti_repeat_days))
    candidate_limit = max(max_events, int(candidate_limit))

    recent_hits = await _load_recent_popular_review_hits(
        db,
        anti_repeat_days=anti_repeat_days,
        now_utc=now_utc,
        profile_key=profile_key,
    )
    ordered_hits = await _collect_popular_hits(
        db,
        candidate_limit=candidate_limit,
        today=today,
    )
    events_map = await _load_events_map(
        db,
        [int(item["event_id"]) for item in ordered_hits],
    )

    fresh: list[PopularReviewPick] = []
    filter_trace: dict[int, dict[str, Any]] = {}
    for hit in ordered_hits:
        event_id = int(hit["event_id"])
        event = events_map.get(event_id)
        if event is None:
            continue
        if event_id in recent_hits:
            logger.info(
                "video_announce.popular_review: skipped event due to cooldown "
                "event_id=%s anti_repeat_days=%s",
                event_id,
                anti_repeat_days,
            )
            continue
        if not _starts_today_or_in_future(event, today=today):
            continue
        photo_urls = await _ensure_renderable_photo_urls(event, db=db)
        if not photo_urls:
            logger.info(
                "video_announce.popular_review: skipped event without renderable posters event_id=%s source=%s",
                event_id,
                getattr(event, "source_post_url", None) or getattr(event, "source_vk_post_url", None) or "",
            )
            continue
        if event_filter is not None:
            decision = await _resolve_filter_decision(event_filter, event)
            filter_trace[event_id] = {
                "matched": decision.matched,
                "needs_manual_review": decision.needs_manual_review,
                "reason": decision.reason,
                "extra": decision.extra,
            }
            if not decision.matched and not (admit_manual_review and decision.needs_manual_review):
                logger.info(
                    "video_announce.popular_review: skipped event by partner filter "
                    "partner_track_id=%s event_id=%s reason=%s",
                    partner_track_id,
                    event_id,
                    decision.reason,
                )
                continue
            if decision.needs_manual_review and admit_manual_review:
                logger.warning(
                    "video_announce.popular_review: admitting manual_review event "
                    "partner_track_id=%s event_id=%s reason=%s",
                    partner_track_id,
                    event_id,
                    decision.reason,
                )
        pick = PopularReviewPick(
            event=event,
            score=float(hit["score"]),
            source_window=str(hit["source_window"]),
            source_post_url=str(hit["source_post_url"]),
            source_label=str(hit["source_label"]),
            anti_repeat_status="fresh",
            description=preferred_scene_description(event),
        )
        fresh.append(pick)

    promo_picks: list[PopularReviewPick] = []
    try:
        promo_candidates = await resolve_video_promo_candidates(
            db,
            profile_key=POPULAR_REVIEW_PROFILE,
            now_utc=now_utc,
        )
    except Exception:
        logger.exception("video_announce.popular_review: failed to resolve promo candidates")
        promo_candidates = []
    for candidate in promo_candidates:
        event = candidate.event
        event_id = int(event.id) if event.id is not None else None
        if event_id is None:
            continue
        if not _starts_today_or_in_future(event, today=today):
            logger.info(
                "video_announce.popular_review: skipped promo event in the past event_id=%s",
                event_id,
            )
            continue
        photo_urls = await _ensure_renderable_photo_urls(event, db=db)
        if not photo_urls:
            logger.info(
                "video_announce.popular_review: skipped promo event without renderable posters event_id=%s",
                event_id,
            )
            continue
        promo_picks.append(
            PopularReviewPick(
                event=event,
                score=999.0,
                source_window="promo",
                source_post_url=getattr(event, "source_post_url", None) or getattr(event, "source_vk_post_url", None) or "",
                source_label="promo",
                anti_repeat_status="promo",
                description=preferred_scene_description(event),
                promo_campaign_id=candidate.campaign_id,
                promo_activity_id=candidate.activity_id,
                promo_placement_kind=candidate.placement_kind,
            )
        )

    selected = _merge_promo_and_fresh_picks(
        promo_picks,
        fresh,
        max_events=max_events,
        now_utc=now_utc,
    )

    if len(selected) < min_events:
        raise RuntimeError(
            "CherryFlash popular review did not collect enough events "
            f"(selected={len(selected)} min={min_events})"
        )

    ranked: list[RankedEvent] = []
    trace: dict[int, dict[str, Any]] = {}
    for position, pick in enumerate(selected, start=1):
        event_id = int(pick.event.id)
        ranked.append(
            RankedEvent(
                event=pick.event,
                score=pick.score,
                position=position,
                reason=(
                    f"popular_review:{pick.source_window}"
                    + (f" {pick.source_label}" if pick.source_label else "")
                ),
                mandatory=bool(pick.promo_campaign_id),
                selected=True,
                selected_reason=pick.source_window,
                description=pick.description,
                promo_campaign_id=pick.promo_campaign_id,
                promo_activity_id=pick.promo_activity_id,
                promo_placement_kind=pick.promo_placement_kind,
            )
        )
        trace[event_id] = {
            "score": round(float(pick.score), 6),
            "source_window": pick.source_window,
            "source_post_url": pick.source_post_url,
            "source_label": pick.source_label,
            "anti_repeat_status": pick.anti_repeat_status,
        }
        if pick.promo_campaign_id:
            trace[event_id].update(
                {
                    "promo_campaign_id": pick.promo_campaign_id,
                    "promo_activity_id": pick.promo_activity_id,
                    "promo_placement_kind": pick.promo_placement_kind,
                }
            )
        partner_meta = filter_trace.get(event_id)
        if partner_meta:
            trace[event_id]["partner_filter"] = partner_meta

    logger.info(
        "video_announce.popular_review selected=%s windows=%s",
        [int(item.event.id) for item in selected if item.event.id is not None],
        [item.source_window for item in selected],
    )
    return PopularReviewSelection(picks=selected, ranked=ranked, trace=trace)
