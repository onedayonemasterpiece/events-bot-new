from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
from urllib.parse import urlparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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
ECO_NATURE_PARTNER_TRACK_ID = "partner_eco_nature_001"
KONB_LIBRARY_PARTNER_TRACK_ID = "partner_konb_library_001"
KONB_LIBRARY_SELECTION_POLICY = "konb_library"
KONB_LIBRARY_LOCAL_TZ = ZoneInfo("Europe/Kaliningrad")
# Score penalty applied to КОНБ candidates that were already shown in the
# last `anti_repeat_days` window. Keeps fresh events sorted ahead of repeats
# but does not exclude repeats — the operator explicitly allowed re-airing
# events as long as they're not in the same video and not back-to-back.
KONB_REPEAT_SCORE_PENALTY = 220.0
PARTNER_PROMO_OFF_FILTER_MIN_PROFILE_MATCHES = 3
PARTNER_PROMO_OFF_FILTER_MAX_PER_SELECTION = 1
PARTNER_PROMO_OFF_FILTER_PLACEMENT_KIND = "guaranteed_any_position"
PARTNER_ECO_RECALL_LOOKAHEAD_DAYS = 14
PARTNER_ECO_RECALL_LIMIT = 32
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
    priority_score: float = 0.0
    priority_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class EventPriority:
    score_boost: float = 0.0
    reasons: tuple[str, ...] = ()
    allow_repeat_after_days: int | None = None


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
    first_slot_promo = [
        pick for pick in promo_picks if pick.promo_placement_kind == "first_slot"
    ]
    top_slot_promo = [
        pick
        for pick in promo_picks
        if pick.promo_placement_kind not in {"guaranteed_any_position", "first_slot"}
    ]
    guaranteed_anywhere = [
        pick for pick in promo_picks if pick.promo_placement_kind == "guaranteed_any_position"
    ]

    def add_pick(pick: PopularReviewPick | None) -> None:
        if pick is None or len(selected) >= max_events:
            return
        event_id = int(pick.event.id)
        if event_id in selected_event_ids:
            return
        selected.append(pick)
        selected_event_ids.add(event_id)

    def guarantee_pick(pick: PopularReviewPick) -> None:
        if pick.event.id is None:
            return
        event_id = int(pick.event.id)
        if event_id in selected_event_ids:
            return
        while len(selected) >= max_events and selected:
            removable_idx = next(
                (
                    idx
                    for idx in range(len(selected) - 1, -1, -1)
                    if selected[idx].promo_placement_kind != "guaranteed_any_position"
                ),
                len(selected) - 1,
            )
            removed = selected.pop(removable_idx)
            selected_event_ids.discard(int(removed.event.id))
        add_pick(pick)

    if first_slot_promo:
        promo_idx = 0
        fresh_idx = 0
        while len(selected) < max_events and promo_idx < len(first_slot_promo):
            add_pick(first_slot_promo[promo_idx])
            promo_idx += 1
        while len(selected) < max_events and (fresh_idx < len(fresh) or top_slot_promo):
            if fresh_idx < len(fresh):
                add_pick(fresh[fresh_idx])
                fresh_idx += 1
            if top_slot_promo:
                add_pick(top_slot_promo.pop(0))
        for candidate in guaranteed_anywhere:
            guarantee_pick(candidate)
        while len(selected) < max_events and fresh_idx < len(fresh):
            add_pick(fresh[fresh_idx])
            fresh_idx += 1
        return selected

    if top_slot_promo and fresh:
        promo_first = _promo_starts_first(top_slot_promo, now_utc=now_utc)
        promo_idx = 0
        fresh_idx = 0
        if promo_first:
            add_pick(top_slot_promo[promo_idx])
            promo_idx += 1
        while len(selected) < max_events and (fresh_idx < len(fresh) or promo_idx < len(top_slot_promo)):
            if fresh_idx < len(fresh):
                add_pick(fresh[fresh_idx])
                fresh_idx += 1
            if promo_idx < len(top_slot_promo):
                add_pick(top_slot_promo[promo_idx])
                promo_idx += 1
        for candidate in guaranteed_anywhere:
            guarantee_pick(candidate)
        while len(selected) < max_events and fresh_idx < len(fresh):
            add_pick(fresh[fresh_idx])
            fresh_idx += 1
        return selected

    if guaranteed_anywhere and fresh:
        for candidate in fresh:
            add_pick(candidate)
            if len(selected) >= max_events:
                break
        for candidate in guaranteed_anywhere:
            guarantee_pick(candidate)
        return selected

    for candidate in [*top_slot_promo, *guaranteed_anywhere, *fresh]:
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


def _coerce_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


async def _load_recent_popular_review_hits(
    db,
    *,
    anti_repeat_days: int,
    now_utc: datetime,
    profile_key: str = POPULAR_REVIEW_PROFILE,
) -> dict[int, dict[str, Any]]:
    threshold = now_utc - timedelta(days=max(1, anti_repeat_days))
    async with db.get_session() as session:
        result = await session.execute(
            select(
                VideoAnnounceItem.event_id,
                VideoAnnounceItem.position,
                func.coalesce(
                    VideoAnnounceSession.published_at,
                    VideoAnnounceSession.finished_at,
                    VideoAnnounceSession.started_at,
                    VideoAnnounceSession.created_at,
                ),
            )
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
        rows = result.all()
    out: dict[int, dict[str, Any]] = {}
    for event_id, position, seen_at_raw in rows:
        if event_id is None:
            continue
        event_key = int(event_id)
        seen_at = _coerce_utc_datetime(seen_at_raw)
        position_int = int(position) if position is not None else None
        current = out.get(event_key)
        if current is None:
            out[event_key] = {
                "last_seen_at": seen_at,
                "best_recent_position": position_int,
            }
            continue
        current_seen = current.get("last_seen_at")
        if seen_at is not None and (
            not isinstance(current_seen, datetime) or seen_at > current_seen
        ):
            current["last_seen_at"] = seen_at
        current_pos = current.get("best_recent_position")
        if position_int is not None and (
            current_pos is None or int(position_int) < int(current_pos)
        ):
            current["best_recent_position"] = position_int
    return out


def _text_blob(event: Event) -> str:
    values: list[str] = []
    for attr in (
        "title",
        "description",
        "short_description",
        "search_digest",
        "source_text",
    ):
        raw = getattr(event, attr, None)
        if raw:
            values.append(str(raw))
    for raw in getattr(event, "source_texts", None) or []:
        if raw:
            values.append(str(raw))
    return "\n".join(values).casefold()


def _has_ticket_or_price(event: Event) -> bool:
    if str(getattr(event, "ticket_link", None) or "").strip():
        return True
    for attr in ("ticket_price_min", "ticket_price_max"):
        raw = getattr(event, attr, None)
        if isinstance(raw, int) and raw > 0:
            return True
    return False


SPECIAL_GUEST_HINTS: tuple[str, ...] = (
    "специальный гость",
    "особый гость",
    "приглашенный гость",
    "приглашённый гость",
    "гость встречи",
    "встреча с",
    "лекцию прочитает",
    "лекцию проведет",
    "лекцию проведёт",
)


def _event_days_until(event: Event, *, today: date) -> int | None:
    event_day = _parse_iso_date(getattr(event, "date", None))
    if event_day is None:
        return None
    return (event_day - today).days


def _konb_event_priority(event: Event, *, today: date) -> EventPriority:
    boost = 0.0
    reasons: list[str] = []
    # КОНБ default: ALL events are allowed to repeat after one calendar-day
    # boundary in Europe/Kaliningrad. The operator explicitly stated that
    # re-airing events is fine — the only hard constraint is "not in the
    # same video, ideally not back-to-back". `_priority_repeat_allowed`
    # honours the calendar-day boundary, and `KONB_REPEAT_SCORE_PENALTY`
    # below biases fresh events to sort ahead of repeats when both exist.
    allow_repeat_after_days: int | None = 1
    days_until = _event_days_until(event, today=today)
    if days_until == 1:
        boost += 700.0
        reasons.append("due_in_1_day")
    elif days_until == 3:
        boost += 620.0
        reasons.append("due_in_3_days")
    if _has_ticket_or_price(event):
        boost += 520.0
        reasons.append("ticket_or_price")
    if any(hint in _text_blob(event) for hint in SPECIAL_GUEST_HINTS):
        boost += 280.0
        reasons.append("special_guest_hint")
    return EventPriority(
        score_boost=boost,
        reasons=tuple(reasons),
        allow_repeat_after_days=allow_repeat_after_days,
    )


def _event_priority(
    event: Event,
    *,
    today: date,
    selection_policy_id: str | None,
) -> EventPriority:
    if selection_policy_id == KONB_LIBRARY_SELECTION_POLICY:
        return _konb_event_priority(event, today=today)
    return EventPriority()


def _priority_repeat_allowed(
    priority: EventPriority,
    recent_meta: dict[str, Any] | None,
    *,
    now_utc: datetime,
) -> bool:
    if not recent_meta or priority.allow_repeat_after_days is None:
        return False
    last_seen_at = recent_meta.get("last_seen_at")
    if not isinstance(last_seen_at, datetime):
        return False
    # Calendar-day spacing in Europe/Kaliningrad: an event shown yesterday or
    # earlier is allowed to repeat today. Using calendar boundaries instead
    # of strict 24h seconds prevents the "next-day run that happens slightly
    # earlier than yesterday's run leaves nothing eligible" failure mode
    # (the КОНБ test produced `selected=0` exactly because of this).
    last_local_day = last_seen_at.astimezone(KONB_LIBRARY_LOCAL_TZ).date()
    today_local_day = now_utc.astimezone(KONB_LIBRARY_LOCAL_TZ).date()
    days_apart = (today_local_day - last_local_day).days
    return days_apart >= max(1, priority.allow_repeat_after_days)


def _first_position_penalty(
    recent_meta: dict[str, Any] | None,
    *,
    selection_policy_id: str | None,
) -> float:
    if selection_policy_id != KONB_LIBRARY_SELECTION_POLICY or not recent_meta:
        return 0.0
    if recent_meta.get("best_recent_position") == 1:
        return 360.0
    return 0.0


def _repeat_score_penalty(
    recent_meta: dict[str, Any] | None,
    *,
    selection_policy_id: str | None,
) -> float:
    """Bias КОНБ sort so any recently-shown event sits below fresh ones,
    even when repeats are admitted. Combined with `_first_position_penalty`,
    a repeat at slot 1 pays both penalties (~580 pts) and only re-appears
    when there are no fresh candidates."""
    if selection_policy_id != KONB_LIBRARY_SELECTION_POLICY or not recent_meta:
        return 0.0
    return KONB_REPEAT_SCORE_PENALTY


def _stable_daily_jitter(event_id: int, *, now_utc: datetime, profile_key: str) -> float:
    seed = f"{now_utc.date().isoformat()}|{profile_key}|{event_id}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    return int(digest[:6], 16) / 16_777_215.0



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


def _event_date_or_max(event: Event) -> date:
    raw = str(getattr(event, "date", "") or "").strip()
    try:
        return date.fromisoformat(raw)
    except Exception:
        return date.max


def _event_time_sort_key(event: Event) -> str:
    return str(getattr(event, "time", "") or "").strip() or "99:99"


async def _collect_partner_eco_recall_hits(
    db,
    *,
    today: date,
    exclude_event_ids: set[int],
    limit: int = PARTNER_ECO_RECALL_LIMIT,
) -> list[dict[str, Any]]:
    """Recall current/future eco-track candidates whose source post is old.

    The base CherryFlash pool is intentionally popularity-post-window driven:
    it looks at posts published in the last 1/3/7 days. Partner eco stories
    also need event-date recall because a highly relevant current event can be
    announced weeks earlier and still be exactly what the partner audience
    expects today. This helper only widens the LLM candidate universe; the
    semantic include/exclude decision remains owned by the partner LLM filter.
    """

    try:
        from .partner_filters import _eco_event_text, _has_keyword_hint
    except Exception:
        logger.exception("video_announce.popular_review: eco recall helpers unavailable")
        return []

    until = today + timedelta(days=PARTNER_ECO_RECALL_LOOKAHEAD_DAYS)
    async with db.get_session() as session:
        result = await session.execute(
            select(Event)
            .where(Event.lifecycle_status == "active")
            .where(Event.silent.is_(False))
            .where(Event.date >= today.isoformat())
            .where(Event.date <= until.isoformat())
            .order_by(Event.date.asc(), Event.time.asc(), Event.id.desc())
            .limit(max(1, int(limit)) * 4)
        )
        events = list(result.scalars().all())

    recalled: list[Event] = []
    for event in events:
        try:
            event_id = int(event.id or 0)
        except Exception:
            event_id = 0
        if event_id <= 0 or event_id in exclude_event_ids:
            continue
        text = _eco_event_text(event)
        if not _has_keyword_hint(text):
            continue
        recalled.append(event)
        if len(recalled) >= max(1, int(limit)):
            break

    def _score(event: Event) -> float:
        event_day = _event_date_or_max(event)
        days_until = 999 if event_day == date.max else max(0, (event_day - today).days)
        # Keep recall below real popularity scores while preserving soonness.
        return max(0.01, 0.75 - min(days_until, PARTNER_ECO_RECALL_LOOKAHEAD_DAYS) * 0.03)

    hits: list[dict[str, Any]] = []
    for event in sorted(recalled, key=lambda ev: (_event_date_or_max(ev), _event_time_sort_key(ev), -(int(ev.id or 0)))):
        event_id = int(event.id or 0)
        hits.append(
            {
                "event_id": event_id,
                "source_window": "partner_event_date_recall",
                "source_post_url": str(
                    getattr(event, "source_post_url", None)
                    or getattr(event, "source_vk_post_url", None)
                    or ""
                ),
                "source_label": "event-date recall",
                "score": _score(event),
            }
        )
    if hits:
        logger.info(
            "video_announce.popular_review: partner eco event-date recall candidates=%s ids=%s",
            len(hits),
            [int(item["event_id"]) for item in hits[:12]],
        )
    return hits


async def _collect_partner_konb_recycle_hits(
    db,
    *,
    today: date,
    exclude_event_ids: set[int],
    lookback_days: int = 90,
    limit: int = 60,
) -> list[dict[str, Any]]:
    """Re-air pool for КОНБ when the fresh popularity pool is dry.

    Loads event_ids that have ever appeared in the КОНБ video profile in the
    last ``lookback_days`` days and are still upcoming. Going through
    ``_consider_hit`` re-applies the КОНБ partner filter and the calendar-day
    cooldown, so events shown TODAY are still skipped (operator contract
    «в один день не делать повтор»), but events from yesterday or earlier
    are allowed to repeat — the operator explicitly preferred a cross-day
    repeat over a missed publication (2026-05-18 incident).
    """
    threshold = datetime.now(timezone.utc) - timedelta(days=max(1, int(lookback_days)))
    async with db.get_session() as session:
        result = await session.execute(
            select(VideoAnnounceItem.event_id)
            .join(
                VideoAnnounceSession,
                VideoAnnounceItem.session_id == VideoAnnounceSession.id,
            )
            .where(VideoAnnounceSession.profile_key == "popular_review_konb")
            .where(
                func.coalesce(
                    VideoAnnounceSession.published_at,
                    VideoAnnounceSession.finished_at,
                    VideoAnnounceSession.started_at,
                    VideoAnnounceSession.created_at,
                )
                >= threshold,
            )
            .where(VideoAnnounceItem.event_id.is_not(None))
        )
        rows = result.all()
    seen: set[int] = set()
    candidate_ids: list[int] = []
    for (event_id,) in rows:
        if event_id is None:
            continue
        eid = int(event_id)
        if eid in exclude_event_ids or eid in seen:
            continue
        seen.add(eid)
        candidate_ids.append(eid)
    if not candidate_ids:
        return []
    async with db.get_session() as session:
        result = await session.execute(
            select(Event)
            .where(Event.id.in_(candidate_ids))
            .where(Event.lifecycle_status == "active")
            .where(Event.date >= today.isoformat())
            .order_by(Event.date.asc(), Event.time.asc(), Event.id.asc())
            .limit(max(1, int(limit)))
        )
        events = result.scalars().all()
    hits: list[dict[str, Any]] = []
    for event in events:
        event_id = int(event.id or 0)
        if not event_id:
            continue
        hits.append(
            {
                "event_id": event_id,
                "source_window": "konb_recycle",
                "source_post_url": (
                    getattr(event, "source_post_url", None)
                    or getattr(event, "source_vk_post_url", None)
                    or ""
                ),
                "source_label": "konb_recycle",
                "score": 0.0,
            }
        )
    return hits


async def _collect_future_event_hits(
    db,
    *,
    today: date,
    exclude_event_ids: set[int],
    limit: int,
) -> list[dict[str, Any]]:
    async with db.get_session() as session:
        result = await session.execute(
            select(Event)
            .where(Event.date >= today.isoformat())
            .where(Event.lifecycle_status == "active")
            .order_by(Event.date.asc(), Event.time.asc(), Event.id.asc())
            .limit(max(1, int(limit)))
        )
        events = result.scalars().all()
    hits: list[dict[str, Any]] = []
    for event in events:
        event_id = int(event.id or 0)
        if not event_id or event_id in exclude_event_ids:
            continue
        hits.append(
            {
                "event_id": event_id,
                "source_window": "future_fallback",
                "source_post_url": (
                    getattr(event, "source_post_url", None)
                    or getattr(event, "source_vk_post_url", None)
                    or ""
                ),
                "source_label": "future_library",
                "score": 0.0,
            }
        )
    return hits


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
    selection_policy_id: str | None = None,
    allow_same_day_recycle: bool = False,
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

    filter_trace: dict[int, dict[str, Any]] = {}
    fresh: list[PopularReviewPick] = []
    processed_event_ids: set[int] = set()

    async def _consider_hit(
        hit: dict[str, Any],
        event: Event | None,
        *,
        allow_reprocess: bool = False,
        ignore_recent_cooldown: bool = False,
        anti_repeat_status_override: str | None = None,
    ) -> None:
        event_id = int(hit["event_id"])
        if event_id in processed_event_ids and not allow_reprocess:
            return
        processed_event_ids.add(event_id)
        if event is None:
            return
        priority = _event_priority(
            event,
            today=today,
            selection_policy_id=selection_policy_id,
        )
        recent_meta = recent_hits.get(event_id)
        if (
            recent_meta
            and not ignore_recent_cooldown
            and not _priority_repeat_allowed(
                priority,
                recent_meta,
                now_utc=now_utc,
            )
        ):
            logger.info(
                "video_announce.popular_review: skipped event due to cooldown "
                "event_id=%s anti_repeat_days=%s",
                event_id,
                anti_repeat_days,
            )
            return
        if not _starts_today_or_in_future(event, today=today):
            return
        photo_urls = await _ensure_renderable_photo_urls(event, db=db)
        if not photo_urls:
            logger.info(
                "video_announce.popular_review: skipped event without renderable posters event_id=%s source=%s",
                event_id,
                getattr(event, "source_post_url", None) or getattr(event, "source_vk_post_url", None) or "",
            )
            return
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
                return
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
            score=(
                float(hit["score"])
                + float(priority.score_boost)
                - _first_position_penalty(
                    recent_meta,
                    selection_policy_id=selection_policy_id,
                )
                - _repeat_score_penalty(
                    recent_meta,
                    selection_policy_id=selection_policy_id,
                )
                + _stable_daily_jitter(
                    event_id,
                    now_utc=now_utc,
                    profile_key=profile_key,
                )
            ),
            source_window=str(hit["source_window"]),
            source_post_url=str(hit["source_post_url"]),
            source_label=str(hit["source_label"]),
            anti_repeat_status=(
                anti_repeat_status_override
                or ("priority_repeat" if recent_meta else "fresh")
            ),
            description=preferred_scene_description(event),
            priority_score=float(priority.score_boost),
            priority_reasons=priority.reasons,
        )
        fresh.append(pick)

    for hit in ordered_hits:
        event_id = int(hit["event_id"])
        await _consider_hit(hit, events_map.get(event_id))

    if (
        event_filter is not None
        and partner_track_id == ECO_NATURE_PARTNER_TRACK_ID
        and len(fresh) < max_events
    ):
        exclude_event_ids = {int(item["event_id"]) for item in ordered_hits}
        exclude_event_ids.update(int(event_id) for event_id in recent_hits)
        recall_hits = await _collect_partner_eco_recall_hits(
            db,
            today=today,
            exclude_event_ids=exclude_event_ids,
        )
        if recall_hits:
            recall_map = await _load_events_map(
                db,
                [int(item["event_id"]) for item in recall_hits],
            )
            for hit in recall_hits:
                if len(fresh) >= max_events:
                    break
                event_id = int(hit["event_id"])
                await _consider_hit(hit, recall_map.get(event_id))

    if selection_policy_id == KONB_LIBRARY_SELECTION_POLICY and len(fresh) < max_events:
        fallback_hits = await _collect_future_event_hits(
            db,
            today=today,
            exclude_event_ids=set(processed_event_ids),
            limit=max(candidate_limit * 3, max_events * 8),
        )
        if fallback_hits:
            fallback_map = await _load_events_map(
                db,
                [int(item["event_id"]) for item in fallback_hits],
            )
            for hit in fallback_hits:
                await _consider_hit(hit, fallback_map.get(int(hit["event_id"])))
                if len(fresh) >= max_events * 2:
                    break
        # Last-resort recycle pool: if the broad future scan still produced
        # nothing, re-air events previously shown in the КОНБ profile that
        # are still upcoming (calendar-day cooldown is enforced inside
        # `_consider_hit`, so events shown TODAY are still excluded).
        # Operator contract: «лучше повтор чем не выпустить вообще».
        if len(fresh) < min_events:
            recycle_hits = await _collect_partner_konb_recycle_hits(
                db,
                today=today,
                exclude_event_ids=set(processed_event_ids),
            )
            if recycle_hits:
                recycle_map = await _load_events_map(
                    db,
                    [int(item["event_id"]) for item in recycle_hits],
                )
                for hit in recycle_hits:
                    await _consider_hit(hit, recycle_map.get(int(hit["event_id"])))
                    if len(fresh) >= max_events:
                        break
        # Final аварийный слой: если обычный recycle тоже пустой, КОНБ-трек
        # может переиспользовать сегодняшние уже показанные события, чтобы не
        # сорвать daily slot. Дубли внутри одного выпуска всё равно отсекаются
        # через `fresh`/`max_events` и уникальный список recycle-кандидатов.
        if len(fresh) < min_events and allow_same_day_recycle:
            selected_ids = {int(pick.event.id) for pick in fresh if pick.event.id is not None}
            same_day_hits = await _collect_partner_konb_recycle_hits(
                db,
                today=today,
                exclude_event_ids=selected_ids,
            )
            if same_day_hits:
                same_day_map = await _load_events_map(
                    db,
                    [int(item["event_id"]) for item in same_day_hits],
                )
                for hit in same_day_hits:
                    hit = dict(hit)
                    hit["source_window"] = "konb_same_day_recycle"
                    hit["source_label"] = "konb_same_day_recycle"
                    await _consider_hit(
                        hit,
                        same_day_map.get(int(hit["event_id"])),
                        allow_reprocess=True,
                        ignore_recent_cooldown=True,
                        anti_repeat_status_override="same_day_recycle",
                    )
                    if len(fresh) >= max_events:
                        break

    if selection_policy_id == KONB_LIBRARY_SELECTION_POLICY:
        fresh.sort(
            key=lambda pick: (
                float(pick.score),
                -(_event_days_until(pick.event, today=today) or 9999),
            ),
            reverse=True,
        )

    allow_partner_off_filter_promo = partner_track_id == ECO_NATURE_PARTNER_TRACK_ID
    promo_picks: list[PopularReviewPick] = []
    off_filter_partner_promo: list[PopularReviewPick] = []
    partner_filter_matched_count = len(fresh) if event_filter is not None else 0
    try:
        promo_candidates = await resolve_video_promo_candidates(
            db,
            profile_key=profile_key,
            now_utc=now_utc,
            include_global_profile=(partner_track_id is None or allow_partner_off_filter_promo),
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
        if event_filter is not None:
            decision = await _resolve_filter_decision(event_filter, event)
            filter_trace[event_id] = {
                "matched": decision.matched,
                "needs_manual_review": decision.needs_manual_review,
                "reason": decision.reason,
                "extra": decision.extra,
            }
            if (
                allow_partner_off_filter_promo
                and not decision.matched
                and not decision.needs_manual_review
            ):
                off_filter_partner_promo.append(
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
                        promo_placement_kind=PARTNER_PROMO_OFF_FILTER_PLACEMENT_KIND,
                    )
                )
                continue
            if not decision.matched and not (admit_manual_review and decision.needs_manual_review):
                logger.info(
                    "video_announce.popular_review: skipped promo event by partner filter "
                    "partner_track_id=%s event_id=%s reason=%s",
                    partner_track_id,
                    event_id,
                    decision.reason,
                )
                continue
            if decision.needs_manual_review and admit_manual_review:
                logger.warning(
                    "video_announce.popular_review: admitting manual_review promo event "
                    "partner_track_id=%s event_id=%s reason=%s",
                    partner_track_id,
                    event_id,
                    decision.reason,
                )
            partner_filter_matched_count += 1
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

    if event_filter is not None and off_filter_partner_promo:
        if (
            partner_filter_matched_count >= PARTNER_PROMO_OFF_FILTER_MIN_PROFILE_MATCHES
            and max_events > PARTNER_PROMO_OFF_FILTER_MIN_PROFILE_MATCHES
        ):
            allowed = off_filter_partner_promo[:PARTNER_PROMO_OFF_FILTER_MAX_PER_SELECTION]
            promo_picks.extend(allowed)
            for pick in allowed:
                event_id = int(pick.event.id) if pick.event.id is not None else None
                if event_id is not None:
                    filter_trace.setdefault(event_id, {})
                    filter_trace[event_id].update(
                        {
                            "matched": False,
                            "needs_manual_review": False,
                            "partner_promo_off_filter_admitted": True,
                            "reason": (
                                "promo admitted after "
                                f"{partner_filter_matched_count} partner-filter matches"
                            ),
                        }
                    )
                    logger.info(
                        "video_announce.popular_review: admitted one off-filter partner promo "
                        "partner_track_id=%s event_id=%s matched_count=%s",
                        partner_track_id,
                        event_id,
                        partner_filter_matched_count,
                    )
        else:
            logger.info(
                "video_announce.popular_review: skipped off-filter partner promo candidates "
                "partner_track_id=%s matched_count=%s required=%s skipped=%s",
                partner_track_id,
                partner_filter_matched_count,
                PARTNER_PROMO_OFF_FILTER_MIN_PROFILE_MATCHES,
                len(off_filter_partner_promo),
            )

    selected = _merge_promo_and_fresh_picks(
        promo_picks,
        fresh,
        max_events=max_events,
        now_utc=now_utc,
    )

    if len(selected) < min_events:
        # For КОНБ partner track the operator contract is «лучше повтор/skip,
        # чем не выпустить вообще»: never crash the publish job — log loudly
        # and let the caller decide whether to ship a partial issue or skip.
        # All non-partner callers historically expected a RuntimeError, so we
        # preserve that contract whenever the selection is below its minimum.
        # With partner policies the caller (scenario.py) handles the empty
        # case by skipping without starting a zero-scene render.
        logger.warning(
            "video_announce.popular_review.selection_below_min "
            "selected=%s min=%s max=%s policy=%s partner_track=%s",
            len(selected),
            min_events,
            max_events,
            selection_policy_id or "",
            partner_track_id or "",
        )
        if not selection_policy_id:
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
        if pick.priority_score or pick.priority_reasons:
            trace[event_id]["priority_score"] = round(float(pick.priority_score), 6)
            trace[event_id]["priority_reasons"] = list(pick.priority_reasons)
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
