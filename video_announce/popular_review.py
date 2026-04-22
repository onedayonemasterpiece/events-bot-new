from __future__ import annotations

import logging
from urllib.parse import urlparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select

from handlers.popular_posts_cmd import _load_top_items, _resolve_telegraph_map
from models import Event, VideoAnnounceItem, VideoAnnounceSession, VideoAnnounceSessionStatus
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


@dataclass(frozen=True)
class PopularReviewPick:
    event: Event
    score: float
    source_window: str
    source_post_url: str
    source_label: str
    anti_repeat_status: str
    description: str


@dataclass(frozen=True)
class PopularReviewSelection:
    picks: list[PopularReviewPick]
    ranked: list[RankedEvent]
    trace: dict[int, dict[str, Any]]

    @property
    def event_ids(self) -> list[int]:
        return [int(item.event.id) for item in self.picks if item.event.id is not None]


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


async def _ensure_renderable_photo_urls(ev: Event) -> list[str]:
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
) -> set[int]:
    threshold = now_utc - timedelta(days=max(1, anti_repeat_days))
    async with db.get_session() as session:
        result = await session.execute(
            select(VideoAnnounceItem.event_id)
            .join(
                VideoAnnounceSession,
                VideoAnnounceItem.session_id == VideoAnnounceSession.id,
            )
            .where(VideoAnnounceSession.profile_key == POPULAR_REVIEW_PROFILE)
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
        telegraph_map, _matched = await _resolve_telegraph_map(db, source_urls=source_urls)
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


async def build_popular_review_selection(
    db,
    *,
    max_events: int = POPULAR_REVIEW_MAX_EVENTS,
    min_events: int = POPULAR_REVIEW_MIN_EVENTS,
    anti_repeat_days: int = POPULAR_REVIEW_ANTI_REPEAT_DAYS,
    candidate_limit: int = POPULAR_REVIEW_CANDIDATE_LIMIT,
    now_utc: datetime | None = None,
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
    )
    ordered_hits = await _collect_popular_hits(db, candidate_limit=candidate_limit)
    events_map = await _load_events_map(
        db,
        [int(item["event_id"]) for item in ordered_hits],
    )

    fresh: list[PopularReviewPick] = []
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
        photo_urls = await _ensure_renderable_photo_urls(event)
        if not photo_urls:
            logger.info(
                "video_announce.popular_review: skipped event without renderable posters event_id=%s source=%s",
                event_id,
                getattr(event, "source_post_url", None) or getattr(event, "source_vk_post_url", None) or "",
            )
            continue
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

    selected: list[PopularReviewPick] = []
    for candidate in fresh:
        if len(selected) >= max_events:
            break
        selected.append(candidate)

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
                mandatory=False,
                selected=True,
                selected_reason=pick.source_window,
                description=pick.description,
            )
        )
        trace[event_id] = {
            "score": round(float(pick.score), 6),
            "source_window": pick.source_window,
            "source_post_url": pick.source_post_url,
            "source_label": pick.source_label,
            "anti_repeat_status": pick.anti_repeat_status,
        }

    logger.info(
        "video_announce.popular_review selected=%s windows=%s",
        [int(item.event.id) for item in selected if item.event.id is not None],
        [item.source_window for item in selected],
    )
    return PopularReviewSelection(picks=selected, ranked=ranked, trace=trace)
