from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import os
import re
import textwrap
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Collection, Iterable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select

from db import Database
from models import (
    Event,
    EventSource,
    Festival,
    JobOutbox,
    JobStatus,
    JobTask,
    PromoActivity,
    PromoCampaign,
    PromoExposure,
    PromoTarget,
    VideoAnnounceItem,
)

logger = logging.getLogger(__name__)

INITIAL_80_STORIES_FESTIVAL = "80 историй о главном"
INITIAL_80_STORIES_TITLE = "80 историй о главном / summer visibility"
INITIAL_80_STORIES_END_DATE = date(2026, 7, 18)
DEFAULT_PROMO_DAYS = 90
PROMO_PRIORITY_MIN = 0
PROMO_PRIORITY_MAX = 3
PROMO_DEFAULT_PRIORITY = 2
INITIAL_80_STORIES_PRIORITY = 1
VIDEO_PROMO_GLOBAL_MAX_PER_PUBLISH = 2
PROMO_POLICY_GUARANTEED_ANY_POSITION = "guaranteed_any_position"
PROMO_POLICY_DIVERSE_SHUFFLE = "diverse_shuffle"
PROMO_POLICY_FIRST_SLOT = "first_slot"
PROMO_POLICY_FIRST_TWO_SLOTS = "first_two_slots"
PROMO_POLICY_WEIGHTED_POPULARITY = "weighted_popularity"
PROMO_DAILY_TZ = "Europe/Kaliningrad"
PROMO_SURFACE_VIDEO_GENERAL = "video_general"
PROMO_SURFACE_HERO_TALK = "hero_talk"
PROMO_HERO_PLACEMENTS = frozenset({"home_hero", "page_end"})
PROMO_HERO_POLICY = "qualified_visibility"
PROMO_SURFACE_DAILY_RECOMMEND_TODAY = "daily_recommend_today"
PROMO_SURFACE_VK_PUBLICATION = "vk_publication"
PROMO_SURFACE_TG_EVENT_PUBLISH = "tg_event_publish"
PROMO_SURFACE_TG_BUTTON_HIGHLIGHT = "tg_button_highlight"
PROMO_SURFACE_TG_REPOST = "tg_repost"
PROMO_SURFACE_VK_CHANNEL_PUBLISH = "vk_channel_publish"
PROMO_SURFACE_VK_REPOST = "vk_repost"
PROMO_SURFACE_VK_STORY = "vk_story"
PROMO_SURFACE_AFISHA_ENGAGEMENT = "afishaengagement"
PROMO_SURFACE_VK_FESTIVAL_CAROUSEL = "vk_festival_carousel"
PROMO_VK_DEFAULT_WINDOW_HOURS = 24
PROMO_VK_REPOST_DEDUP_HOURS = 72
PROMO_VK_ACTIVE_START_HOUR = 9
PROMO_VK_ACTIVE_END_HOUR = 21
PROMO_VK_80_PUBLICATION_PROFILE = "klgdevents"
PROMO_VK_80_CHANNEL_PROFILE = "klgdevents:vk_channel"
PROMO_VK_80_REPOST_PROFILE = "klgdevents->kenigeventsofficial"
PROMO_VK_80_STORY_KLGD_PROFILE = "klgdevents:story"
PROMO_VK_80_STORY_MAIN_PROFILE = "klgdevents->kenigeventsofficial:story"
PROMO_VK_80_AFISHAENGAGEMENT_PROFILE = "klgdevents:afishaengagement"
PROMO_TG_80_EVENT_PUBLISH_PROFILE = "kldevents:80stories"
PROMO_TG_80_REPOST_PROFILE = "kldevents->kenigevents:80stories"
PROMO_TG_BUTTON_HIGHLIGHT_PROFILE = "kldevents:details-button"
PROMO_VK_80_AFISHAENGAGEMENT_LEGACY_PROFILES = {
    "klgdevents:motivation:80stories",
}
NONPUBLIC_PROMO_DELIVERY_STATUSES = frozenset({"VK_CHANNEL_DRAFT_SENT"})
VK_SYNC_MISSING_TG_MEDIA_ERROR = "vk_sync_missing_media_for_telegram_event"
PUBLIC_PROMO_EXPOSURE_STATUSES = frozenset(
    {
        "VK_SCHEDULED",
        "PUBLISHED",
        "PUBLISHED_MAIN",
        "PUBLISHED_TEST",
        "TG_PUBLISHED",
        "TG_FORWARDED",
        "DAILY_RECOMMENDED",
    }
)
DEBUG_PROMO_EXPOSURE_STATUSES = frozenset({"VK_SCHEDULED_DEBUG"})

# Target that matches events by their Telegram source chat + post author.
# query_text holds "<chat_username>:<author_username>" (both lowercased, no @).
PROMO_TARGET_TYPE_TG_CHAT_AUTHOR = "tg_chat_author"
PROMO_TARGET_TYPE_ALL = "all"
# Concrete kraftmarket39 / @LANGEANNA -> video announce campaign.
KRAFTMARKET_AUTHOR_CHAT = "kraftmarket39"
KRAFTMARKET_AUTHOR_USERNAME = "langeanna"
KRAFTMARKET_AUTHOR_CAMPAIGN_TITLE = "kraftmarket39 · @LANGEANNA → видеоанонс"


def clamp_campaign_end_to_event(
    requested_end: date, event: Event
) -> date:
    """Clamp campaign end date to the last day the event is still on.

    For ``target=event`` campaigns there is no point promoting past the
    event itself. ``event.end_date`` covers multi-day events; otherwise
    fall back to ``event.date``. Returns the original requested end if it
    is already on or before the event's last day.
    """

    last_iso = (getattr(event, "end_date", None) or getattr(event, "date", "") or "").split("..", 1)[0].strip()
    if not last_iso:
        return requested_end
    try:
        last = date.fromisoformat(last_iso)
    except ValueError:
        return requested_end
    return min(requested_end, last)


@dataclass(frozen=True)
class PromoCandidate:
    event: Event
    campaign_id: int
    activity_id: int
    placement_kind: str
    reason: str
    priority: int = PROMO_DEFAULT_PRIORITY


def normalize_promo_priority(value: int | str | None) -> int:
    try:
        parsed = int(value) if value is not None else PROMO_DEFAULT_PRIORITY
    except (TypeError, ValueError):
        parsed = PROMO_DEFAULT_PRIORITY
    return max(PROMO_PRIORITY_MIN, min(PROMO_PRIORITY_MAX, parsed))


@dataclass(frozen=True)
class PromoCreateResult:
    campaign: PromoCampaign | None
    status: str
    message: str
    matches: tuple[Event, ...] = ()


@dataclass(frozen=True)
class PromoVkActionResult:
    campaign_id: int
    activity_id: int
    surface: str
    event_id: int
    status: str
    source_url: str | None = None
    target_url: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class PromoPopularityScore:
    score: float
    source_score: float = 0.0
    owned_vk_score: float = 0.0
    source_count: int = 0
    owned_vk_count: int = 0


@dataclass(frozen=True)
class DailyPromoRecommendation:
    event: Event
    campaign_id: int
    activity_id: int
    target_url: str | None = None


def _campaign_end_dt(day: date) -> datetime:
    return datetime.combine(day, time(23, 59, 59), tzinfo=timezone.utc)


def _promo_day_bounds(now_utc: datetime) -> tuple[datetime, datetime]:
    tz = ZoneInfo(PROMO_DAILY_TZ)
    local = now_utc.astimezone(tz)
    start_local = datetime.combine(local.date(), time.min, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def default_campaign_end(now_utc: datetime | None = None) -> date:
    now_utc = now_utc or datetime.now(timezone.utc)
    return now_utc.date() + timedelta(days=DEFAULT_PROMO_DAYS)


def _event_start_date(ev: Event) -> date | None:
    raw = str(getattr(ev, "date", "") or "").split("..", 1)[0].strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def event_is_future_for_promo(ev: Event, *, today: date) -> bool:
    start = _event_start_date(ev)
    return bool(start and start >= today)


def _event_start_local_datetime_for_repost(ev: Event) -> datetime | None:
    start = _event_start_date(ev)
    if start is None:
        return None
    raw_time = str(getattr(ev, "time", "") or "").strip()
    if not raw_time:
        return None
    try:
        hour_raw, minute_raw = raw_time.split(":", 1)
        hour = int(hour_raw)
        minute = int(minute_raw)
    except Exception:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return datetime.combine(start, time(hour, minute), tzinfo=ZoneInfo(PROMO_DAILY_TZ))


def event_is_repostable_for_promo(
    ev: Event,
    *,
    now_utc: datetime,
    min_lead_hours: float = 4,
) -> bool:
    """Return whether a source event is still timely enough to repost.

    Date-only future events remain eligible for future days, but same-day
    events without a reliable start time are not reposted because the 4-hour
    lead-time promise cannot be verified.
    """

    start = _event_start_date(ev)
    if start is None:
        return False
    local_now = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ))
    if start < local_now.date():
        return False
    start_dt = _event_start_local_datetime_for_repost(ev)
    if start_dt is None:
        return start > local_now.date()
    return start_dt >= local_now + timedelta(hours=max(0.0, float(min_lead_hours)))


def event_has_not_started_for_promo(
    ev: Event,
    *,
    now_utc: datetime,
) -> bool:
    """Return whether a promo/event wall post is still timely.

    Date-only same-day events remain eligible because there is no reliable
    start time to compare against. Timed same-day events, however, must not be
    selected once their start time has already passed.
    """

    start = _event_start_date(ev)
    if start is None:
        return False
    local_now = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ))
    if start < local_now.date():
        return False
    start_dt = _event_start_local_datetime_for_repost(ev)
    if start_dt is None:
        return start >= local_now.date()
    return start_dt >= local_now


def _event_is_not_after_campaign(ev: Event, *, campaign: PromoCampaign) -> bool:
    if campaign.ends_at is None:
        return True
    start = _event_start_date(ev)
    if start is None:
        return False
    return start <= campaign.ends_at.date()


def _is_sold_out_status(value: str | None) -> bool:
    text = (value or "").strip().casefold()
    if not text:
        return False
    text = text.replace("-", "_").replace(" ", "_")
    return text in {"sold_out", "soldout", "распродано", "билетов_нет", "нет_билетов"}


def _event_is_promo_eligible(
    ev: Event,
    *,
    today: date,
    campaign: PromoCampaign,
    enforce_event_date_lte_campaign: bool = True,
) -> bool:
    if not event_is_future_for_promo(ev, today=today):
        return False
    if enforce_event_date_lte_campaign and not _event_is_not_after_campaign(ev, campaign=campaign):
        return False
    if getattr(ev, "silent", False):
        return False
    if (getattr(ev, "lifecycle_status", "") or "active") != "active":
        return False
    if _is_sold_out_status(getattr(ev, "ticket_status", None)):
        return False
    return True


def _event_has_stored_poster(ev: Event) -> bool:
    if int(getattr(ev, "photo_count", 0) or 0) > 0:
        return True
    return any(str(url or "").strip() for url in (getattr(ev, "photo_urls", None) or []))


def _is_telegram_origin_event(ev: Event) -> bool:
    source_url = str(getattr(ev, "source_post_url", None) or "").strip().lower()
    if "t.me/" in source_url or "telegram.me/" in source_url:
        return True
    if getattr(ev, "source_chat_id", None) or getattr(ev, "source_message_id", None):
        return True
    return False


def _require_media_for_telegram_vk_posts() -> bool:
    raw = os.getenv("VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _promo_vk_publication_missing_required_media(ev: Event) -> bool:
    if not _require_media_for_telegram_vk_posts() or not _is_telegram_origin_event(ev):
        return False
    return not any(str(url or "").strip() for url in (getattr(ev, "photo_urls", None) or []))


async def _ensure_promo_vk_photo_urls(db: Database, ev: Event) -> list[str]:
    existing = [
        str(url or "").strip()
        for url in (getattr(ev, "photo_urls", None) or [])
        if str(url or "").strip()
    ]
    if existing:
        return existing

    telegraph_source = str(
        getattr(ev, "telegraph_url", None) or getattr(ev, "telegraph_path", None) or ""
    ).strip()
    if not telegraph_source:
        return []

    try:
        from main import extract_telegraph_image_urls

        recovered = await extract_telegraph_image_urls(telegraph_source)
    except Exception:
        logger.exception(
            "promo.vk media recovery failed source=telegraph event_id=%s telegraph_url=%s",
            getattr(ev, "id", None),
            telegraph_source,
        )
        return []

    recovered_urls = [str(url or "").strip() for url in recovered if str(url or "").strip()]
    if not recovered_urls:
        return []

    event_id = getattr(ev, "id", None)
    if event_id:
        from event_media import get_event_gallery_urls, ingest_event_media_urls

        async with db.get_session() as session:
            await ingest_event_media_urls(
                session,
                int(event_id),
                recovered_urls,
                source="promo_telegraph_recovery",
            )
            await session.commit()
            recovered_urls = await get_event_gallery_urls(
                session,
                int(event_id),
                legacy_fallback=False,
            )
        ev.photo_urls = list(recovered_urls)
        ev.photo_count = len(recovered_urls)
    logger.info(
        "promo.vk media recovered source=telegraph event_id=%s telegraph_url=%s photo_urls_count=%s",
        event_id,
        telegraph_source,
        len(recovered_urls),
    )
    return recovered_urls


def _stable_shuffle_key(*parts: object) -> str:
    text = "|".join(str(part) for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _activity_config(activity: PromoActivity) -> dict[str, Any]:
    value = getattr(activity, "config_json", None)
    return value if isinstance(value, dict) else {}


def _vk_group_ref(value: str | int | None) -> str:
    return str(value or "").strip().removeprefix("https://vk.com/").removeprefix("http://vk.com/").strip("/")


def _vk_wall_url_from_parts(owner_id: int, post_id: int) -> str:
    sign = "" if owner_id > 0 else "-"
    return f"https://vk.com/wall{sign}{abs(int(owner_id))}_{int(post_id)}"


def _vk_activity_window(activity: PromoActivity) -> int:
    cfg = _activity_config(activity)
    try:
        return max(1, int(cfg.get("window_hours") or PROMO_VK_DEFAULT_WINDOW_HOURS))
    except (TypeError, ValueError):
        return PROMO_VK_DEFAULT_WINDOW_HOURS


def _activity_min_lead_hours(activity: PromoActivity, default: float = 4) -> float:
    cfg = _activity_config(activity)
    try:
        return max(0.0, float(cfg.get("min_lead_hours") or default))
    except (TypeError, ValueError):
        return default


def _csv_ints(value: object) -> list[int]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_parts = list(value)
    else:
        raw_parts = str(value).replace(";", ",").split(",")
    out: list[int] = []
    for part in raw_parts:
        text = str(part or "").strip()
        if not text:
            continue
        try:
            out.append(int(text))
        except (TypeError, ValueError):
            continue
    return out


def _median_int(values: Iterable[int]) -> int | None:
    data = sorted(int(v) for v in values)
    if not data:
        return None
    mid = len(data) // 2
    if len(data) % 2:
        return int(data[mid])
    return int((data[mid - 1] + data[mid]) // 2)


def _safe_ratio(value: int, denom: int | None) -> float:
    d = int(denom or 0)
    if d <= 0:
        d = 1
    return float(value) / float(d)


def _popularity_metric_score(
    *,
    views: int | None,
    likes: int | None,
    median_views: int | None,
    median_likes: int | None,
    sample: int,
    min_sample: int,
) -> float:
    """Return the normalized /popular_posts-style score for one metric row.

    The candidate must be strictly above the per-source median on views or likes.
    The formula intentionally mirrors handlers.popular_posts_cmd:
    max(above-median ratio) + a small combined-ratio tie-breaker.
    """

    if int(sample or 0) < int(min_sample or 0):
        return 0.0
    if median_views is None or median_likes is None:
        return 0.0
    if not isinstance(views, int) or not isinstance(likes, int) or views < 0 or likes < 0:
        return 0.0
    above_views = int(views) > int(median_views)
    above_likes = int(likes) > int(median_likes)
    if not (above_views or above_likes):
        return 0.0
    v_ratio = _safe_ratio(int(views), int(median_views))
    l_ratio = _safe_ratio(int(likes), int(median_likes))
    return float(max(v_ratio if above_views else 0.0, l_ratio if above_likes else 0.0) + 0.01 * (v_ratio + l_ratio))


def _vk_wall_ids_from_url(url: str | None) -> tuple[int, int] | None:
    text = str(url or "").strip()
    if not text:
        return None
    match = re.search(r"vk\.com/wall(-?\d+)_(\d+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        owner_id = int(match.group(1))
        post_id = int(match.group(2))
    except ValueError:
        return None
    group_id = abs(owner_id)
    if group_id <= 0 or post_id <= 0:
        return None
    return group_id, post_id


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def _vk_channel_peer_ids(cfg: dict[str, Any]) -> list[int]:
    """Return legacy explicit VK messenger peer ids from config/env.

    This helper is intentionally not used for publication while VK community
    Channels do not have a documented posting API in Open API.  A messenger
    ``peer_id`` is not proof that the target is the community Channel surface.
    """

    peer_ids = _csv_ints(cfg.get("peer_ids"))
    peer_ids.extend(_csv_ints(cfg.get("peer_id")))
    for env_key in (
        str(cfg.get("draft_peer_ids_env") or "").strip(),
        str(cfg.get("draft_peer_id_env") or "").strip(),
        str(cfg.get("peer_ids_env") or "").strip(),
        str(cfg.get("peer_id_env") or "").strip(),
    ):
        if env_key:
            peer_ids.extend(_csv_ints(os.getenv(env_key)))
    seen: set[int] = set()
    unique: list[int] = []
    for peer_id in peer_ids:
        if peer_id in seen:
            continue
        seen.add(peer_id)
        unique.append(peer_id)
    return unique


def _vk_activity_due_count(activity: PromoActivity, now_utc: datetime) -> int:
    """Return how many placements should be due by now in the local day.

    The default active window is 09:00-21:00 Europe/Kaliningrad. Slots are
    spread at the midpoint of each equal slice: for N=2 the due moments are
    12:00 and 18:00, for N=1 it is 15:00. Outside the active window, new
    promo VK actions are not started; already scheduled VK postponed posts are
    left to VK's queue.
    """

    cfg = _activity_config(activity)
    try:
        start_hour = int(cfg.get("active_start_hour", PROMO_VK_ACTIVE_START_HOUR))
    except (TypeError, ValueError):
        start_hour = PROMO_VK_ACTIVE_START_HOUR
    try:
        end_hour = int(cfg.get("active_end_hour", PROMO_VK_ACTIVE_END_HOUR))
    except (TypeError, ValueError):
        end_hour = PROMO_VK_ACTIVE_END_HOUR
    start_hour = max(0, min(23, start_hour))
    end_hour = max(start_hour + 1, min(24, end_hour))
    total = max(1, int(activity.max_per_publish or 1))
    local_now = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ))
    start = datetime.combine(local_now.date(), time(start_hour, 0), tzinfo=local_now.tzinfo)
    end = datetime.combine(local_now.date(), time.min, tzinfo=local_now.tzinfo) + timedelta(hours=end_hour)
    if local_now < start or local_now > end:
        return 0
    window_seconds = (end - start).total_seconds()
    due = 0
    for idx in range(total):
        slot = start + timedelta(seconds=window_seconds * ((idx + 0.5) / total))
        if local_now >= slot:
            due += 1
    return due


def _norm_text(value: str | None) -> str:
    return " ".join(str(value or "").casefold().replace("ё", "е").split())


def _event_match_score(ev: Event, query: str) -> float:
    needle = _norm_text(query)
    if not needle:
        return 0.0
    hay_parts = [
        getattr(ev, "title", None),
        getattr(ev, "short_description", None),
        getattr(ev, "search_digest", None),
    ]
    hay = _norm_text(" ".join(str(part or "") for part in hay_parts))
    if not hay:
        return 0.0
    if needle in hay:
        return min(1.0, 0.72 + (len(needle) / max(len(hay), 1)) * 0.28)
    return SequenceMatcher(None, needle, hay[: max(len(needle) * 3, 80)]).ratio()


async def _campaign_by_title(db: Database, title: str) -> PromoCampaign | None:
    async with db.get_session() as session:
        res = await session.execute(select(PromoCampaign).where(PromoCampaign.title == title))
        return res.scalars().first()


async def _create_campaign_with_target(
    db: Database,
    *,
    title: str,
    goal_comment: str,
    ends_at: date,
    target_type: str,
    event_id: int | None = None,
    festival_name: str | None = None,
    query_text: str | None = None,
    max_per_publish: int = 1,
    priority: int = PROMO_DEFAULT_PRIORITY,
    video_selection_policy: str = PROMO_POLICY_DIVERSE_SHUFFLE,
    now_utc: datetime | None = None,
    created_by: int | None = None,
) -> PromoCampaign:
    now_utc = now_utc or datetime.now(timezone.utc)
    campaign = PromoCampaign(
        title=title,
        status="active",
        goal_comment=goal_comment,
        starts_at=now_utc,
        ends_at=_campaign_end_dt(ends_at),
        priority=normalize_promo_priority(priority),
        created_by=created_by,
    )
    async with db.get_session() as session:
        session.add(campaign)
        await session.commit()
        await session.refresh(campaign)

        session.add(
            PromoTarget(
                campaign_id=int(campaign.id),
                target_type=target_type,
                event_id=event_id,
                festival_name=festival_name,
                query_text=query_text,
            )
        )
        session.add_all(
            [
                PromoActivity(
                    campaign_id=int(campaign.id),
                    surface="video_general",
                    profile_key="popular_review",
                    max_per_publish=max(1, min(int(max_per_publish), 2)),
                    selection_policy=video_selection_policy,
                    enabled=True,
                ),
                PromoActivity(
                    campaign_id=int(campaign.id),
                    surface="daily_highlight",
                    max_per_publish=max(1, min(int(max_per_publish), 2)),
                    selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
                    enabled=True,
                ),
                PromoActivity(
                    campaign_id=int(campaign.id),
                    surface="telegraph_month",
                    max_per_publish=max(1, min(int(max_per_publish), 2)),
                    selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
                    enabled=True,
                ),
                PromoActivity(
                    campaign_id=int(campaign.id),
                    surface="telegraph_weekend",
                    max_per_publish=max(1, min(int(max_per_publish), 2)),
                    selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
                    enabled=True,
                ),
                _default_tg_button_highlight_activity(int(campaign.id)),
            ]
        )
        await session.commit()
        await session.refresh(campaign)
        return campaign


async def create_festival_promo_campaign(
    db: Database,
    *,
    festival_name: str,
    ends_at: date | None = None,
    now_utc: datetime | None = None,
    created_by: int | None = None,
) -> PromoCreateResult:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date()
    name = " ".join(str(festival_name or "").split())
    if not name:
        return PromoCreateResult(None, "invalid", "Нужно указать название фестиваля.")
    ends = ends_at or default_campaign_end(now_utc)

    async with db.get_session() as session:
        fest_res = await session.execute(select(Festival).where(Festival.name == name))
        festival_exists = fest_res.scalars().first() is not None
        future_res = await session.execute(
            select(func.count())
            .select_from(Event)
            .where(Event.festival == name)
            .where(Event.date >= today.isoformat())
            .where(Event.date <= ends.isoformat())
            .where(Event.lifecycle_status == "active")
            .where(Event.silent.is_(False))
        )
        future_count = int(future_res.scalar() or 0)
        if not festival_exists and future_count <= 0:
            return PromoCreateResult(
                None,
                "missing_festival",
                f"Не создал кампанию: фестиваль {name!r} должен уже существовать в системе.",
            )
        if future_count <= 0:
            return PromoCreateResult(
                None,
                "no_future_events",
                f"Не создал кампанию: у фестиваля {name!r} нет будущих событий до {ends.isoformat()}.",
            )

    title = f"{name} / promo until {ends.isoformat()}"
    existing = await _campaign_by_title(db, title)
    if existing is not None:
        return PromoCreateResult(existing, "existing", f"Кампания уже есть: #{existing.id} {existing.title}")

    campaign = await _create_campaign_with_target(
        db,
        title=title,
        goal_comment=f"дать больше охвата событиям фестиваля {name}",
        ends_at=ends,
        target_type="festival",
        festival_name=name,
        query_text=name,
        max_per_publish=2,
        priority=PROMO_DEFAULT_PRIORITY,
        now_utc=now_utc,
        created_by=created_by,
    )
    return PromoCreateResult(
        campaign,
        "created",
        f"Создал кампанию #{campaign.id}: {name}, будущих событий сейчас: {future_count}, до {ends.isoformat()}.",
    )


async def create_event_promo_campaign(
    db: Database,
    *,
    query_text: str,
    ends_at: date | None = None,
    now_utc: datetime | None = None,
    created_by: int | None = None,
) -> PromoCreateResult:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()
    query = " ".join(str(query_text or "").split())
    if not query:
        return PromoCreateResult(None, "invalid", "Нужно указать примерное название события.")
    ends = ends_at or default_campaign_end(now_utc)

    async with db.get_session() as session:
        res = await session.execute(
            select(Event)
            .where(Event.date >= today.isoformat())
            .where(Event.lifecycle_status == "active")
            .where(Event.silent.is_(False))
            .order_by(Event.date, Event.time, Event.id)
            .limit(300)
        )
        candidates = list(res.scalars().all())

    scored = sorted(
        ((ev, _event_match_score(ev, query)) for ev in candidates),
        key=lambda item: (-item[1], str(getattr(item[0], "date", "") or ""), int(item[0].id or 0)),
    )
    matches = [(ev, score) for ev, score in scored if score >= 0.48]
    if not matches:
        return PromoCreateResult(
            None,
            "not_found",
            f"Не нашёл будущего события по запросу {query!r} до {ends.isoformat()}.",
        )

    top_event, top_score = matches[0]
    ambiguous = [ev for ev, score in matches[:5] if top_score - score < 0.04]
    if len(ambiguous) > 1:
        return PromoCreateResult(
            None,
            "ambiguous",
            "Нашёл несколько похожих будущих событий. Уточните название или используйте id.",
            matches=tuple(ambiguous),
        )

    event_id = int(top_event.id)
    title = f"{top_event.title} / promo until {ends.isoformat()}"
    existing = await _campaign_by_title(db, title)
    if existing is not None:
        return PromoCreateResult(existing, "existing", f"Кампания уже есть: #{existing.id} {existing.title}")

    campaign = await _create_campaign_with_target(
        db,
        title=title,
        goal_comment=f"дать больше охвата событию {top_event.title}",
        ends_at=ends,
        target_type="event",
        event_id=event_id,
        query_text=query,
        max_per_publish=1,
        priority=PROMO_DEFAULT_PRIORITY,
        now_utc=now_utc,
        created_by=created_by,
    )
    return PromoCreateResult(
        campaign,
        "created",
        f"Создал кампанию #{campaign.id}: событие #{event_id} {top_event.title}, до {ends.isoformat()}.",
    )


PARTNER_PROMO_VIDEO_PROFILES: dict[str, str] = {
    "popular_review": "Видеоанонс — популярное",
    "default": "Видеоанонс — завтра",
    "konb": "Видеоанонс — КОНБ",
}

PARTNER_PROMO_SLOT_POLICIES: dict[str, str] = {
    PROMO_POLICY_GUARANTEED_ANY_POSITION: "Любая позиция",
    PROMO_POLICY_FIRST_TWO_SLOTS: "Слот 1–2",
    PROMO_POLICY_FIRST_SLOT: "Только слот 1",
}


@dataclass(frozen=True)
class PartnerPromoSpec:
    event_id: int
    creator_user_id: int
    organization_name: str | None
    surface: str
    profile_key: str | None
    slot_policy: str
    count: int
    ends_at: date
    is_editorial: bool
    sponsorship_disclosure: str | None
    title_override: str | None = None
    priority: int = PROMO_DEFAULT_PRIORITY


def build_partner_campaign_title(
    *,
    organization_name: str | None,
    partner_username: str | None,
    event_title: str,
    created_date: date,
    is_superadmin: bool,
) -> str:
    if is_superadmin:
        prefix = "editorial"
    else:
        prefix = (organization_name or partner_username or "partner").strip() or "partner"
    short_title = (event_title or "").strip()
    if len(short_title) > 40:
        short_title = short_title[:39].rstrip() + "…"
    return f"{prefix} · {short_title} · {created_date.isoformat()}"


def _activity_slot_for_policy(policy: str) -> int | None:
    if policy == PROMO_POLICY_FIRST_SLOT:
        return 1
    return None


async def create_partner_event_promo_campaign(
    db: Database,
    spec: PartnerPromoSpec,
    *,
    now_utc: datetime | None = None,
    session: Any | None = None,
) -> PromoCreateResult:
    """Create an event-targeted partner promo campaign from a confirmed FSM spec.

    The caller (FSM step 6) is responsible for authorization. This function
    only validates business rules: event must exist, be future and active,
    ``ends_at`` is clamped to the event end date, count is positive.
    Campaign, target and activities commit atomically. An explicit caller-owned
    session is flushed but never committed, allowing the same transaction to
    carry current authorization and an operation receipt without another engine.
    """

    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()

    if spec.count <= 0:
        return PromoCreateResult(None, "invalid", "Количество показов должно быть положительным.")
    if spec.surface not in {PROMO_SURFACE_VIDEO_GENERAL, PROMO_SURFACE_VK_REPOST}:
        return PromoCreateResult(None, "invalid", f"Неизвестная поверхность: {spec.surface!r}.")
    if spec.surface == PROMO_SURFACE_VIDEO_GENERAL and spec.slot_policy not in {
        PROMO_POLICY_GUARANTEED_ANY_POSITION,
        PROMO_POLICY_FIRST_TWO_SLOTS,
        PROMO_POLICY_FIRST_SLOT,
    }:
        return PromoCreateResult(None, "invalid", f"Неизвестная политика слота: {spec.slot_policy!r}.")

    if session is None:
        async with db.get_session() as owned_session:
            result = await create_partner_event_promo_campaign(
                db, spec, now_utc=now_utc, session=owned_session,
            )
            if result.campaign is not None:
                await owned_session.commit()
            return result

    from models import User  # local import to avoid cycle at import-time

    event = await session.get(Event, int(spec.event_id))
    if event is None:
        return PromoCreateResult(None, "not_found", "Событие не найдено.")
    if not _event_is_promo_eligible(
        event,
        today=today,
        campaign=PromoCampaign(
            title="_probe_",
            status="active",
            starts_at=now_utc,
            ends_at=_campaign_end_dt(spec.ends_at),
        ),
        enforce_event_date_lte_campaign=False,
    ):
        return PromoCreateResult(
            None,
            "not_eligible",
            "Событие не подходит под промо: либо прошло, либо закрыто, либо silent.",
        )
    partner = await session.get(User, int(spec.creator_user_id))
    partner_username = partner.username if partner is not None else None
    is_superadmin = bool(partner.is_superadmin) if partner is not None else False
    event_title = str(event.title or "")
    event_id = int(event.id)

    clamped_end = clamp_campaign_end_to_event(spec.ends_at, event)
    if clamped_end < today:
        return PromoCreateResult(
            None,
            "invalid",
            "Дата окончания кампании уже в прошлом после клампа на дату события.",
        )

    title = spec.title_override or build_partner_campaign_title(
        organization_name=spec.organization_name,
        partner_username=partner_username,
        event_title=event_title,
        created_date=today,
        is_superadmin=is_superadmin,
    )

    sponsorship = None if spec.is_editorial else (spec.sponsorship_disclosure or "Партнёрский материал")

    campaign = PromoCampaign(
        title=title,
        status="active",
        goal_comment=f"партнёрское промо: событие #{event_id} {event_title}",
        starts_at=now_utc,
        ends_at=_campaign_end_dt(clamped_end),
        total_exposure_goal=int(spec.count),
        priority=normalize_promo_priority(spec.priority),
        sponsorship_disclosure=sponsorship,
        created_by=int(spec.creator_user_id),
    )

    session.add(campaign)
    await session.flush()
    campaign_id = int(campaign.id)

    target = PromoTarget(
        campaign_id=campaign_id,
        target_type="event",
        event_id=event_id,
        query_text=event_title,
    )

    if spec.surface == PROMO_SURFACE_VIDEO_GENERAL:
        activity = PromoActivity(
            campaign_id=campaign_id,
            surface=PROMO_SURFACE_VIDEO_GENERAL,
            profile_key=spec.profile_key,
            slot=_activity_slot_for_policy(spec.slot_policy),
            max_per_publish=1,
            target_exposure_goal=int(spec.count),
            selection_policy=spec.slot_policy,
            enabled=True,
        )
    else:
        activity = PromoActivity(
            campaign_id=campaign_id,
            surface=PROMO_SURFACE_VK_REPOST,
            profile_key=None,
            slot=None,
            max_per_publish=1,
            target_exposure_goal=int(spec.count),
            selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
            enabled=True,
        )
    session.add(target)
    session.add(activity)
    session.add(_default_tg_button_highlight_activity(campaign_id))
    await session.flush()

    return PromoCreateResult(
        campaign,
        "created",
        f"Создал кампанию #{campaign.id}: {title}, показов {spec.count}, до {clamped_end.isoformat()}.",
    )


@dataclass(frozen=True)
class PartnerActivitySpec:
    """Spec for adding one PromoActivity to an existing campaign.

    Period, mode, sponsorship_disclosure and target event are inherited
    from the campaign — only the placement-specific knobs are user-input.
    """

    campaign_id: int
    surface: str
    profile_key: str | None
    slot_policy: str
    count: int | None
    config: dict | None = None


def validate_hero_activity_config(value: Any) -> dict[str, Any]:
    """Validate placement/content binding, never a second campaign lifecycle."""
    if not isinstance(value, dict) or set(value) - {"placements", "content_ref", "session_cap"}:
        raise ValueError("HERO_CONFIG_INVALID")
    placements = value.get("placements")
    if (not isinstance(placements, dict) or not placements
            or set(placements) - PROMO_HERO_PLACEMENTS
            or any(type(enabled) is not bool for enabled in placements.values())):
        raise ValueError("HERO_PLACEMENTS_INVALID")
    content_ref = value.get("content_ref")
    if not isinstance(content_ref, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", content_ref):
        raise ValueError("HERO_CONTENT_REF_INVALID")
    cap = value.get("session_cap", 1)
    if type(cap) is not int or not 1 <= cap <= 3:
        raise ValueError("HERO_SESSION_CAP_INVALID")
    return {"placements": {key: placements.get(key, False) for key in sorted(PROMO_HERO_PLACEMENTS)},
            "content_ref": content_ref, "session_cap": cap,
            "accounting_unit": "qualified_browser_visibility"}


async def hero_activity_eligibility(
    db: Database, *, activity_id: int, placement: str,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Fresh campaign gates for preview/control; no exposure, seed or writes.

    A eligible result is NOT a browser permit: content validity, consent/cap and
    current actor policy must still pass at the caller's mutation/delivery boundary.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        raise ValueError("Timezone-aware now_utc required")
    result = {"activity_id": activity_id, "placement": placement, "eligible": False,
              "data_as_of": now_utc.isoformat(), "accounting_unit": "qualified_browser_visibility"}
    if placement not in PROMO_HERO_PLACEMENTS:
        return dict(result, reason="placement_unknown")
    async with db.get_session() as session:
        activity = await session.get(PromoActivity, activity_id)
        if activity is None or activity.surface != PROMO_SURFACE_HERO_TALK:
            return dict(result, reason="activity_not_found")
        campaign = await session.get(PromoCampaign, activity.campaign_id)
        result.update(campaign_id=activity.campaign_id, activity_enabled=activity.enabled,
                      content_ref=(activity.config_json or {}).get("content_ref"))
        if campaign is None:
            return dict(result, reason="campaign_not_found")
        result.update(campaign_status=campaign.status, priority=campaign.priority)
        if campaign.status != "active":
            return dict(result, reason="campaign_" + campaign.status)
        def utc(dt):
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        if utc(campaign.starts_at) > now_utc:
            return dict(result, reason="campaign_not_started")
        if campaign.ends_at is not None and utc(campaign.ends_at) < now_utc:
            return dict(result, reason="campaign_expired")
        if not activity.enabled:
            return dict(result, reason="activity_off")
        try:
            raw_config = dict(activity.config_json or {})
            raw_config.pop("accounting_unit", None)
            config = validate_hero_activity_config(raw_config)
        except ValueError:
            return dict(result, reason="activity_config_invalid")
        if not config["placements"][placement]:
            return dict(result, reason="placement_off")
        if (campaign.total_exposure_goal is not None or campaign.daily_exposure_cap is not None
                or activity.target_exposure_goal is not None or activity.daily_cap is not None
                or activity.selection_policy != PROMO_HERO_POLICY):
            return dict(result, reason="publication_cap_policy_unsupported")
        targets = list((await session.execute(select(PromoTarget).where(
            PromoTarget.campaign_id == campaign.id))).scalars())
    ids = set()
    today = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date()
    for target in targets:
        for event in await _events_for_target(db, target=target, campaign=campaign, today=today, now_utc=now_utc):
            ids.add(int(event.id))
    return dict(result, eligible=bool(ids), reason="eligible" if ids else "target_no_eligible_events",
                event_ids=sorted(ids), placements=config["placements"], session_cap=config["session_cap"])


async def add_partner_activity_to_campaign(
    db: Database,
    spec: PartnerActivitySpec,
    *,
    actor_user_id: int,
    now_utc: datetime | None = None,
    session: Any | None = None,
) -> PromoCreateResult:
    """Append a new PromoActivity to an existing partner campaign.

    Authorization: caller must verify the user owns the campaign or is
    superadmin; this function only enforces business rules (campaign
    exists, not archived, surface/slot_policy known, count positive). A supplied
    session remains caller-owned: flush only, with no independent commit.
    """

    now_utc = now_utc or datetime.now(timezone.utc)
    if spec.surface != PROMO_SURFACE_HERO_TALK and (spec.count is None or spec.count <= 0):
        return PromoCreateResult(None, "invalid", "Количество показов должно быть положительным.")
    if spec.surface not in {PROMO_SURFACE_VIDEO_GENERAL, PROMO_SURFACE_VK_REPOST, PROMO_SURFACE_HERO_TALK}:
        return PromoCreateResult(None, "invalid", f"Неизвестная поверхность: {spec.surface!r}.")
    if spec.surface == PROMO_SURFACE_VIDEO_GENERAL and spec.slot_policy not in {
        PROMO_POLICY_GUARANTEED_ANY_POSITION,
        PROMO_POLICY_FIRST_TWO_SLOTS,
        PROMO_POLICY_FIRST_SLOT,
    }:
        return PromoCreateResult(None, "invalid", f"Неизвестная политика слота: {spec.slot_policy!r}.")

    hero_config = None
    if spec.surface == PROMO_SURFACE_HERO_TALK:
        try:
            hero_config = validate_hero_activity_config(spec.config)
        except ValueError as exc:
            return PromoCreateResult(None, "invalid", str(exc))
        if spec.count is not None or spec.slot_policy != PROMO_HERO_POLICY or spec.profile_key is not None:
            return PromoCreateResult(None, "invalid", "hero_talk requires qualified_visibility; publication count/profile/slot are unsupported.")

    if session is None:
        async with db.get_session() as owned_session:
            result = await add_partner_activity_to_campaign(
                db, spec, actor_user_id=actor_user_id, now_utc=now_utc,
                session=owned_session,
            )
            if result.campaign is not None:
                await owned_session.commit()
            return result

    campaign = await session.get(PromoCampaign, int(spec.campaign_id))
    if campaign is None:
        return PromoCreateResult(None, "not_found", "Кампания не найдена.")
    if campaign.status == "archived":
        return PromoCreateResult(
            None,
            "invalid",
            "Кампания в архиве — нельзя добавить активность. Восстановите её сначала.",
        )

    if spec.surface == PROMO_SURFACE_HERO_TALK:
        if campaign.total_exposure_goal is not None or campaign.daily_exposure_cap is not None:
            return PromoCreateResult(None, "invalid", "HERO_CAMPAIGN_PUBLICATION_CAP_UNSUPPORTED: browser visibility cannot consume publication units.")
        activity = PromoActivity(
            campaign_id=int(campaign.id), surface=PROMO_SURFACE_HERO_TALK,
            selection_policy=PROMO_HERO_POLICY, config_json=hero_config,
            max_per_publish=1, enabled=True,
        )
    elif spec.surface == PROMO_SURFACE_VIDEO_GENERAL:
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VIDEO_GENERAL,
            profile_key=spec.profile_key,
            slot=1 if spec.slot_policy == PROMO_POLICY_FIRST_SLOT else None,
            max_per_publish=1,
            target_exposure_goal=int(spec.count),
            selection_policy=spec.slot_policy,
            enabled=True,
        )
    else:
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VK_REPOST,
            profile_key=None,
            slot=None,
            max_per_publish=1,
            target_exposure_goal=int(spec.count),
            selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
            enabled=True,
        )
    campaign.updated_at = now_utc
    session.add(campaign)
    session.add(activity)
    await session.flush()

    return PromoCreateResult(
        campaign,
        "created",
        f"Добавил активность к #{campaign.id}: {spec.surface}"
        + (f"/{spec.profile_key}" if spec.profile_key else "")
        + (" · квалифицированная видимость." if spec.surface == PROMO_SURFACE_HERO_TALK else f" · {spec.count} показ(ов)."),
    )


def _default_tg_button_highlight_activity(
    campaign_id: int,
    *,
    enabled: bool = True,
) -> PromoActivity:
    """Marker activity enabling the inline `✨ Подробнее` button on normal TG posts."""

    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_TG_BUTTON_HIGHLIGHT,
        profile_key=PROMO_TG_BUTTON_HIGHLIGHT_PROFILE,
        max_per_publish=1,
        daily_cap=None,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=enabled,
        config_json={
            "target_chat": "@kldevents",
            "button_text": "✨ Подробнее",
            "purpose": "move_details_link_to_inline_button",
        },
    )


def _initial_80_vk_publication_activity(campaign_id: int) -> PromoActivity:
    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_VK_PUBLICATION,
        profile_key=PROMO_VK_80_PUBLICATION_PROFILE,
        max_per_publish=2,
        daily_cap=2,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=True,
        config_json={
            "target_group": "klgdevents",
            "window_hours": PROMO_VK_DEFAULT_WINDOW_HOURS,
            "active_start_hour": PROMO_VK_ACTIVE_START_HOUR,
            "active_end_hour": PROMO_VK_ACTIVE_END_HOUR,
            "count_organic": True,
            "post_style": "smart_update_source",
        },
    )


def _initial_80_vk_channel_publish_activity(campaign_id: int) -> PromoActivity:
    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_VK_CHANNEL_PUBLISH,
        profile_key=PROMO_VK_80_CHANNEL_PROFILE,
        max_per_publish=1,
        daily_cap=1,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=True,
        config_json={
            "target_group": "klgdevents",
            "target_channel": "Полюбить Калининград Афиша",
            "draft_peer_id_env": "VK_AFISHA_CHANNEL_DRAFT_PEER_ID",
            "peer_id_env": "VK_AFISHA_CHANNEL_DRAFT_PEER_ID",
            "window_hours": PROMO_VK_DEFAULT_WINDOW_HOURS,
            "active_start_hour": PROMO_VK_ACTIVE_START_HOUR,
            "active_end_hour": PROMO_VK_ACTIVE_END_HOUR,
            "post_style": "vk_channel_short_event_one_link",
            "delivery_mode": "vk_messages_manual_copy_draft",
            "api_contract": "manual_copy_draft_until_documented_vk_community_channel_post_api",
        },
    )


def _initial_80_vk_repost_activity(campaign_id: int) -> PromoActivity:
    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_VK_REPOST,
        profile_key=PROMO_VK_80_REPOST_PROFILE,
        max_per_publish=1,
        daily_cap=1,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=True,
        config_json={
            "source_group": "klgdevents",
            "target_group": "kenigeventsofficial",
            "window_hours": PROMO_VK_DEFAULT_WINDOW_HOURS,
            "active_start_hour": PROMO_VK_ACTIVE_START_HOUR,
            "active_end_hour": PROMO_VK_ACTIVE_END_HOUR,
            "dedup_hours": PROMO_VK_REPOST_DEDUP_HOURS,
            "caption": "short_rewrite_text",
        },
    )


def _initial_80_vk_story_activity(
    campaign_id: int,
    *,
    profile_key: str,
    source_group: str,
    target_group: str,
) -> PromoActivity:
    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_VK_STORY,
        profile_key=profile_key,
        max_per_publish=2,
        daily_cap=2,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=True,
        config_json={
            "source_group": source_group,
            "target_group": target_group,
            "window_hours": PROMO_VK_DEFAULT_WINDOW_HOURS,
            "active_start_hour": PROMO_VK_ACTIVE_START_HOUR,
            "active_end_hour": PROMO_VK_ACTIVE_END_HOUR,
            "dedup_hours": PROMO_VK_REPOST_DEDUP_HOURS,
            "link_text": "Подробнее",
        },
    )


def _initial_80_tg_event_publish_activity(campaign_id: int) -> PromoActivity:
    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
        profile_key=PROMO_TG_80_EVENT_PUBLISH_PROFILE,
        max_per_publish=2,
        daily_cap=2,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=True,
        config_json={
            "target_chat": "@kldevents",
            "window_hours": PROMO_VK_DEFAULT_WINDOW_HOURS,
            "active_start_hour": PROMO_VK_ACTIVE_START_HOUR,
            "active_end_hour": PROMO_VK_ACTIVE_END_HOUR,
            "campaign_scope": "80stories",
            "mode": "self_forward_existing_event_post",
        },
    )


def _initial_80_tg_repost_activity(campaign_id: int) -> PromoActivity:
    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_TG_REPOST,
        profile_key=PROMO_TG_80_REPOST_PROFILE,
        max_per_publish=1,
        daily_cap=1,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=True,
        config_json={
            "source_chat": "@kldevents",
            "target_chat": "@kenigevents",
            "window_hours": 72,
            "active_start_hour": PROMO_VK_ACTIVE_START_HOUR,
            "active_end_hour": PROMO_VK_ACTIVE_END_HOUR,
            "dedup_hours": PROMO_VK_REPOST_DEDUP_HOURS,
            "campaign_scope": "80stories",
        },
    )


def _initial_80_afishaengagement_activity(campaign_id: int) -> PromoActivity:
    return PromoActivity(
        campaign_id=campaign_id,
        surface=PROMO_SURFACE_AFISHA_ENGAGEMENT,
        profile_key=PROMO_VK_80_AFISHAENGAGEMENT_PROFILE,
        max_per_publish=1,
        daily_cap=None,
        selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
        enabled=True,
        config_json={
            "target_group": "klgdevents",
            "debug_shadow": True,
            "apply_rate": 0.70,
            "debug_marker": "#afishaengagement_shadow",
            "debug_cleanup_before": False,
            "debug_cap": 500,
            "debug_publish_delay_days": 3,
            "debug_slot_spacing_minutes": 5,
            "formats": [
                "right_extension",
                "bottom_overlay",
                "bottom_extension",
                "hook_swipe_cta",
            ],
            "mechanic_weights": {"comments": 0, "likes": 100, "reposts": 0},
            "cta_templates": {
                "by_event_type": {
                    "*": {
                        "likes": [
                            "Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}."
                        ]
                    }
                }
            },
            "prefer_configured_cta_templates": True,
            "apply_salt": "80-registration-like-v1",
        },
    )


def _sync_initial_80_afishaengagement_activity(
    current: PromoActivity,
    required: PromoActivity,
) -> bool:
    changed = False
    for attr in (
        "profile_key",
        "max_per_publish",
        "daily_cap",
        "selection_policy",
        "enabled",
    ):
        if getattr(current, attr) != getattr(required, attr):
            setattr(current, attr, getattr(required, attr))
            changed = True
    if (current.config_json or {}) != (required.config_json or {}):
        current.config_json = dict(required.config_json or {})
        changed = True
    return changed


def _sync_required_promo_activity(
    current: PromoActivity,
    required: PromoActivity,
) -> bool:
    changed = False
    for attr in (
        "profile_key",
        "max_per_publish",
        "daily_cap",
        "selection_policy",
        "enabled",
    ):
        if getattr(current, attr) != getattr(required, attr):
            setattr(current, attr, getattr(required, attr))
            changed = True
    if (current.config_json or {}) != (required.config_json or {}):
        current.config_json = dict(required.config_json or {})
        changed = True
    return changed


async def ensure_initial_80_stories_campaign(
    db: Database,
    *,
    now_utc: datetime | None = None,
    created_by: int | None = None,
) -> PromoCampaign | None:
    """Create the initial 80 Stories campaign only when real future events exist."""

    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()
    async with db.get_session() as session:
        existing_res = await session.execute(
            select(PromoCampaign).where(PromoCampaign.title == INITIAL_80_STORIES_TITLE)
        )
        existing = existing_res.scalars().first()
        if existing is not None:
            changed = False
            if normalize_promo_priority(getattr(existing, "priority", None)) != INITIAL_80_STORIES_PRIORITY:
                existing.priority = INITIAL_80_STORIES_PRIORITY
                changed = True
            expected_end = _campaign_end_dt(INITIAL_80_STORIES_END_DATE)
            if existing.ends_at != expected_end:
                existing.ends_at = expected_end
                changed = True
            activity_res = await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == existing.id,
                )
            )
            activities = list(activity_res.scalars().all())
            for activity in activities:
                if activity.surface == "video_general" and activity.profile_key == "popular_review":
                    if activity.selection_policy != PROMO_POLICY_GUARANTEED_ANY_POSITION:
                        activity.selection_policy = PROMO_POLICY_GUARANTEED_ANY_POSITION
                        changed = True
                    if int(activity.max_per_publish or 1) != 2:
                        activity.max_per_publish = 2
                        changed = True
                    session.add(activity)
            existing_keys = {
                (str(activity.surface or ""), str(activity.profile_key or ""))
                for activity in activities
            }
            existing_by_key = {
                (str(activity.surface or ""), str(activity.profile_key or "")): activity
                for activity in activities
            }
            legacy_afisha_activities = [
                activity
                for activity in activities
                if str(activity.surface or "") == PROMO_SURFACE_AFISHA_ENGAGEMENT
                and str(activity.profile_key or "") in PROMO_VK_80_AFISHAENGAGEMENT_LEGACY_PROFILES
            ]
            for required in (
                _initial_80_vk_publication_activity(int(existing.id)),
                _initial_80_vk_channel_publish_activity(int(existing.id)),
                _initial_80_vk_repost_activity(int(existing.id)),
                _initial_80_vk_story_activity(
                    int(existing.id),
                    profile_key=PROMO_VK_80_STORY_KLGD_PROFILE,
                    source_group="klgdevents",
                    target_group="klgdevents",
                ),
                _initial_80_vk_story_activity(
                    int(existing.id),
                    profile_key=PROMO_VK_80_STORY_MAIN_PROFILE,
                    source_group="klgdevents",
                    target_group="kenigeventsofficial",
                ),
                _initial_80_afishaengagement_activity(int(existing.id)),
                _initial_80_tg_event_publish_activity(int(existing.id)),
                _initial_80_tg_repost_activity(int(existing.id)),
                _default_tg_button_highlight_activity(int(existing.id)),
            ):
                key = (str(required.surface or ""), str(required.profile_key or ""))
                current = existing_by_key.get(key)
                if current is None and required.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT:
                    current = legacy_afisha_activities[0] if legacy_afisha_activities else None
                if current is None:
                    session.add(required)
                    changed = True
                elif required.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT:
                    if _sync_initial_80_afishaengagement_activity(current, required):
                        changed = True
                    session.add(current)
                    for legacy_duplicate in legacy_afisha_activities:
                        if legacy_duplicate is current:
                            continue
                        if legacy_duplicate.enabled:
                            legacy_duplicate.enabled = False
                            changed = True
                        session.add(legacy_duplicate)
                elif required.surface in {PROMO_SURFACE_TG_EVENT_PUBLISH, PROMO_SURFACE_TG_REPOST}:
                    if _sync_required_promo_activity(current, required):
                        changed = True
                    session.add(current)
            if changed:
                existing.updated_at = now_utc
                session.add(existing)
                await session.commit()
            return existing

        future_count_res = await session.execute(
            select(func.count())
            .select_from(Event)
            .where(Event.festival == INITIAL_80_STORIES_FESTIVAL)
            .where(Event.date >= today.isoformat())
            .where(Event.date <= INITIAL_80_STORIES_END_DATE.isoformat())
            .where(Event.lifecycle_status == "active")
            .where(Event.silent.is_(False))
        )
        future_count = int(future_count_res.scalar() or 0)
        if future_count <= 0:
            logger.info(
                "promo.seed skipped: no future festival events name=%s today=%s",
                INITIAL_80_STORIES_FESTIVAL,
                today.isoformat(),
            )
            return None

        campaign = PromoCampaign(
            title=INITIAL_80_STORIES_TITLE,
            status="active",
            goal_comment=(
                "дать фестивалю устойчивое присутствие в видеоанонсах и заметность "
                "в ежедневных/Telegraph поверхностях до 18 июля"
            ),
            starts_at=now_utc,
            ends_at=_campaign_end_dt(INITIAL_80_STORIES_END_DATE),
            priority=INITIAL_80_STORIES_PRIORITY,
            created_by=created_by,
        )
        session.add(campaign)
        await session.commit()
        await session.refresh(campaign)

        target = PromoTarget(
            campaign_id=int(campaign.id),
            target_type="festival",
            festival_name=INITIAL_80_STORIES_FESTIVAL,
            query_text=INITIAL_80_STORIES_FESTIVAL,
        )
        popular_review = PromoActivity(
            campaign_id=int(campaign.id),
            surface="video_general",
            profile_key="popular_review",
            max_per_publish=2,
            selection_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
            enabled=True,
        )
        daily = PromoActivity(
            campaign_id=int(campaign.id),
            surface="daily_highlight",
            max_per_publish=2,
            selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
            enabled=True,
        )
        telegraph_month = PromoActivity(
            campaign_id=int(campaign.id),
            surface="telegraph_month",
            max_per_publish=2,
            selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
            enabled=True,
        )
        telegraph_weekend = PromoActivity(
            campaign_id=int(campaign.id),
            surface="telegraph_weekend",
            max_per_publish=2,
            selection_policy=PROMO_POLICY_DIVERSE_SHUFFLE,
            enabled=True,
        )
        session.add_all(
            [
                target,
                popular_review,
                daily,
                telegraph_month,
                telegraph_weekend,
                _initial_80_vk_publication_activity(int(campaign.id)),
                _initial_80_vk_channel_publish_activity(int(campaign.id)),
                _initial_80_vk_repost_activity(int(campaign.id)),
                _initial_80_vk_story_activity(
                    int(campaign.id),
                    profile_key=PROMO_VK_80_STORY_KLGD_PROFILE,
                    source_group="klgdevents",
                    target_group="klgdevents",
                ),
                _initial_80_vk_story_activity(
                    int(campaign.id),
                    profile_key=PROMO_VK_80_STORY_MAIN_PROFILE,
                    source_group="klgdevents",
                    target_group="kenigeventsofficial",
                ),
                _initial_80_afishaengagement_activity(int(campaign.id)),
                _initial_80_tg_event_publish_activity(int(campaign.id)),
                _initial_80_tg_repost_activity(int(campaign.id)),
                _default_tg_button_highlight_activity(int(campaign.id)),
            ]
        )
        await session.commit()
        logger.info(
            "promo.seed created campaign_id=%s festival=%s future_events=%s",
            campaign.id,
            INITIAL_80_STORIES_FESTIVAL,
            future_count,
        )
        return campaign


async def ensure_kraftmarket_langeanna_campaign(
    db: Database,
    *,
    now_utc: datetime | None = None,
    created_by: int | None = None,
) -> PromoCampaign:
    """Idempotently ensure the kraftmarket39 / @LANGEANNA → video announce campaign.

    Trigger: events sourced from the Telegram chat ``kraftmarket39`` whose post
    author is ``@LANGEANNA`` (``Event.tg_source_author``). The activity is a
    single guaranteed-any-position video promo (``max_per_publish=1``); the event
    still has to pass each video announce's own content filter — that is enforced
    by the shared video promo pipeline (`resolve_video_promo_candidates` +
    `video_announce/popular_review.py`), not bypassed here.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    query_text = f"{KRAFTMARKET_AUTHOR_CHAT}:{KRAFTMARKET_AUTHOR_USERNAME}"
    async with db.get_session() as session:
        campaign = (
            await session.execute(
                select(PromoCampaign).where(
                    PromoCampaign.title == KRAFTMARKET_AUTHOR_CAMPAIGN_TITLE
                )
            )
        ).scalars().first()
        if campaign is None:
            campaign = PromoCampaign(
                title=KRAFTMARKET_AUTHOR_CAMPAIGN_TITLE,
                status="active",
                goal_comment=(
                    "автоматически продвигать в видеоанонс события автора "
                    f"@{KRAFTMARKET_AUTHOR_USERNAME} из чата {KRAFTMARKET_AUTHOR_CHAT}"
                ),
                starts_at=now_utc,
                ends_at=None,
                priority=normalize_promo_priority(2),
                created_by=created_by,
            )
            session.add(campaign)
            await session.commit()
            await session.refresh(campaign)
        campaign_id = int(campaign.id)
        changed = False
        target = (
            await session.execute(
                select(PromoTarget)
                .where(PromoTarget.campaign_id == campaign_id)
                .where(PromoTarget.target_type == PROMO_TARGET_TYPE_TG_CHAT_AUTHOR)
            )
        ).scalars().first()
        if target is None:
            session.add(
                PromoTarget(
                    campaign_id=campaign_id,
                    target_type=PROMO_TARGET_TYPE_TG_CHAT_AUTHOR,
                    query_text=query_text,
                )
            )
            changed = True
        activity = (
            await session.execute(
                select(PromoActivity)
                .where(PromoActivity.campaign_id == campaign_id)
                .where(PromoActivity.surface == PROMO_SURFACE_VIDEO_GENERAL)
            )
        ).scalars().first()
        if activity is None:
            session.add(
                PromoActivity(
                    campaign_id=campaign_id,
                    surface=PROMO_SURFACE_VIDEO_GENERAL,
                    profile_key=None,
                    max_per_publish=1,
                    selection_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
                    enabled=True,
                )
            )
            changed = True
        button_activity = (
            await session.execute(
                select(PromoActivity)
                .where(PromoActivity.campaign_id == campaign_id)
                .where(PromoActivity.surface == PROMO_SURFACE_TG_BUTTON_HIGHLIGHT)
                .where(PromoActivity.profile_key == PROMO_TG_BUTTON_HIGHLIGHT_PROFILE)
            )
        ).scalars().first()
        if button_activity is None:
            session.add(_default_tg_button_highlight_activity(campaign_id))
            changed = True
        if changed:
            await session.commit()
        return campaign


def _parse_chat_author_query(query_text: str | None) -> tuple[str, str]:
    """Split a ``<chat>:<author>`` tg_chat_author target into lowercased parts."""
    raw = str(query_text or "").strip()
    if ":" not in raw:
        return "", ""
    chat, _, author = raw.partition(":")
    return chat.strip().lstrip("@").lower(), author.strip().lstrip("@").lower()


async def _events_for_target(
    db: Database,
    *,
    target: PromoTarget,
    campaign: PromoCampaign,
    today: date,
    now_utc: datetime | None = None,
) -> list[Event]:
    now_utc = now_utc or datetime.now(timezone.utc)

    def timely(ev: Event) -> bool:
        return event_has_not_started_for_promo(ev, now_utc=now_utc)

    async with db.get_session() as session:
        if target.target_type == "event" and target.event_id:
            ev = await session.get(Event, int(target.event_id))
            if not ev or not _event_is_promo_eligible(
                ev,
                today=today,
                campaign=campaign,
                enforce_event_date_lte_campaign=False,
            ) or not timely(ev):
                return []
            return [ev]
        if target.target_type == PROMO_TARGET_TYPE_TG_CHAT_AUTHOR:
            chat, author = _parse_chat_author_query(target.query_text)
            if not chat or not author:
                return []
            query = (
                select(Event)
                .join(EventSource, EventSource.event_id == Event.id)
                .where(func.lower(EventSource.source_chat_username) == chat)
                .where(func.lower(Event.tg_source_author) == author)
                .where(Event.date >= today.isoformat())
                .where(Event.lifecycle_status == "active")
                .where(Event.silent.is_(False))
                .order_by(Event.date, Event.time, Event.id)
                .distinct()
            )
            if campaign.ends_at is not None:
                query = query.where(Event.date <= campaign.ends_at.date().isoformat())
            res = await session.execute(query)
            return [
                ev
                for ev in res.scalars().all()
                if _event_is_promo_eligible(ev, today=today, campaign=campaign)
                and timely(ev)
            ]
        if target.target_type == PROMO_TARGET_TYPE_ALL:
            query = (
                select(Event)
                .where(Event.date >= today.isoformat())
                .where(Event.lifecycle_status == "active")
                .where(Event.silent.is_(False))
                .order_by(Event.date, Event.time, Event.id)
            )
            if campaign.ends_at is not None:
                query = query.where(Event.date <= campaign.ends_at.date().isoformat())
            res = await session.execute(query)
            return [
                ev
                for ev in res.scalars().all()
                if _event_is_promo_eligible(ev, today=today, campaign=campaign)
                and timely(ev)
            ]
        if target.target_type != "festival" or not target.festival_name:
            return []
        query = (
            select(Event)
            .where(Event.festival == target.festival_name)
            .where(Event.date >= today.isoformat())
            .where(Event.lifecycle_status == "active")
            .where(Event.silent.is_(False))
            .order_by(Event.date, Event.time, Event.id)
        )
        if campaign.ends_at is not None:
            query = query.where(Event.date <= campaign.ends_at.date().isoformat())
        res = await session.execute(query)
        return [
            ev
            for ev in res.scalars().all()
            if _event_is_promo_eligible(ev, today=today, campaign=campaign)
            and timely(ev)
        ]


async def _load_public_exposure_stats(
    db: Database,
    *,
    campaign_id: int,
    activity_id: int,
    event_ids: Iterable[int],
    statuses: Collection[str] | None = None,
) -> dict[int, tuple[int, datetime | None]]:
    ids = [int(event_id) for event_id in event_ids if event_id is not None]
    if not ids:
        return {}
    async with db.get_session() as session:
        res = await session.execute(
            select(
                PromoExposure.event_id,
                func.count(PromoExposure.id),
                func.max(PromoExposure.published_at),
            )
            .where(PromoExposure.campaign_id == campaign_id)
            .where(PromoExposure.activity_id == activity_id)
            .where(PromoExposure.event_id.in_(ids))
            .where(PromoExposure.publish_status.in_(statuses or PUBLIC_PROMO_EXPOSURE_STATUSES))
            .group_by(PromoExposure.event_id)
        )
        rows = res.all()
    stats: dict[int, tuple[int, datetime | None]] = {}
    for event_id, count, last_at in rows:
        stats[int(event_id)] = (int(count or 0), last_at)
    return stats


async def _count_public_exposures(
    db: Database,
    *,
    campaign_id: int,
    activity_id: int | None = None,
    since_utc: datetime | None = None,
    until_utc: datetime | None = None,
) -> int:
    async with db.get_session() as session:
        query = (
            select(func.count(PromoExposure.id))
            .where(PromoExposure.campaign_id == campaign_id)
            .where(PromoExposure.publish_status.in_(PUBLIC_PROMO_EXPOSURE_STATUSES))
        )
        if activity_id is not None:
            query = query.where(PromoExposure.activity_id == activity_id)
        if since_utc is not None:
            query = query.where(PromoExposure.published_at >= since_utc)
        if until_utc is not None:
            query = query.where(PromoExposure.published_at < until_utc)
        return int((await session.execute(query)).scalar() or 0)


async def resolve_video_promo_candidates(
    db: Database,
    *,
    profile_key: str,
    now_utc: datetime | None = None,
    surface: str = "video_general",
    include_global_profile: bool = True,
) -> list[PromoCandidate]:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date()
    day_start_utc, day_end_utc = _promo_day_bounds(now_utc)
    await ensure_initial_80_stories_campaign(db, now_utc=now_utc)
    await ensure_kraftmarket_langeanna_campaign(db, now_utc=now_utc)

    async with db.get_session() as session:
        query = (
            select(PromoCampaign, PromoActivity, PromoTarget)
            .join(PromoActivity, PromoActivity.campaign_id == PromoCampaign.id)
            .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
            .where(PromoCampaign.status == "active")
            .where(PromoCampaign.starts_at <= now_utc)
            .where(or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc))
            .where(PromoActivity.enabled.is_(True))
            .where(PromoActivity.surface == surface)
        )
        if include_global_profile:
            query = query.where(or_(PromoActivity.profile_key.is_(None), PromoActivity.profile_key == profile_key))
        else:
            query = query.where(PromoActivity.profile_key == profile_key)
        res = await session.execute(
            query.order_by(PromoCampaign.priority, PromoCampaign.created_at, PromoActivity.id, PromoTarget.id)
        )
        rows = list(res.all())

    buckets: list[list[PromoCandidate]] = []
    global_budget = VIDEO_PROMO_GLOBAL_MAX_PER_PUBLISH
    for campaign, activity, target in rows:
        if campaign.id is None or activity.id is None:
            continue
        campaign_id = int(campaign.id)
        activity_id = int(activity.id)
        if campaign.total_exposure_goal is not None:
            total_count = await _count_public_exposures(db, campaign_id=campaign_id)
            if total_count >= max(0, int(campaign.total_exposure_goal)):
                continue
        if campaign.daily_exposure_cap is not None:
            daily_count = await _count_public_exposures(
                db,
                campaign_id=campaign_id,
                since_utc=day_start_utc,
                until_utc=day_end_utc,
            )
            if daily_count >= max(0, int(campaign.daily_exposure_cap)):
                continue
        if activity.target_exposure_goal is not None:
            activity_total = await _count_public_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
            )
            if activity_total >= max(0, int(activity.target_exposure_goal)):
                continue
        if activity.daily_cap is not None:
            activity_daily = await _count_public_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                since_utc=day_start_utc,
                until_utc=day_end_utc,
            )
            if activity_daily >= max(0, int(activity.daily_cap)):
                continue
        max_per_publish = max(1, min(int(activity.max_per_publish or 1), 2))
        events = await _events_for_target(
            db,
            target=target,
            campaign=campaign,
            today=today,
            now_utc=now_utc,
        )
        events = [
            ev
            for ev in events
            if ev.id is not None
            and _event_has_stored_poster(ev)
        ]
        if not events:
            continue
        stats = await _load_public_exposure_stats(
            db,
            campaign_id=int(campaign.id),
            activity_id=int(activity.id),
            event_ids=[int(ev.id) for ev in events if ev.id is not None],
        )

        def sort_key(ev: Event) -> tuple[int, datetime, str, str, str, int]:
            count, last_at = stats.get(int(ev.id), (0, None))
            last_key = last_at or datetime.min.replace(tzinfo=timezone.utc)
            start = str(getattr(ev, "date", "") or "")
            time_value = str(getattr(ev, "time", "") or "")
            shuffle = _stable_shuffle_key(campaign.id, activity.id, today.isoformat(), ev.id)
            return (count, last_key, shuffle, start, time_value, int(ev.id))

        picked = sorted(events, key=sort_key)[:max_per_publish]
        if int(activity.slot or 0) == 1 or str(activity.selection_policy or "") == PROMO_POLICY_FIRST_SLOT:
            placement_kind = PROMO_POLICY_FIRST_SLOT
        elif str(activity.selection_policy or "") == PROMO_POLICY_GUARANTEED_ANY_POSITION:
            placement_kind = PROMO_POLICY_GUARANTEED_ANY_POSITION
        else:
            placement_kind = "general_boost"
        bucket: list[PromoCandidate] = []
        for ev in picked:
            if ev.id is None:
                continue
            bucket.append(
                PromoCandidate(
                    event=ev,
                    campaign_id=campaign_id,
                    activity_id=activity_id,
                    placement_kind=placement_kind,
                    reason=(
                        f"promo:{surface}"
                        + (f":festival:{target.festival_name}" if target.festival_name else "")
                    ),
                    priority=normalize_promo_priority(getattr(campaign, "priority", None)),
                )
            )
        if bucket:
            buckets.append(bucket)

    result: list[PromoCandidate] = []
    used_event_ids: set[int] = set()
    max_rounds = max((len(bucket) for bucket in buckets), default=0)
    for round_idx in range(max_rounds):
        for bucket in buckets:
            if len(result) >= global_budget:
                return result
            if round_idx >= len(bucket):
                continue
            candidate = bucket[round_idx]
            event_id = int(candidate.event.id) if candidate.event.id is not None else None
            if event_id is None or event_id in used_event_ids:
                continue
            used_event_ids.add(event_id)
            result.append(candidate)
    return result


async def resolve_surface_promo_event_ids(
    db: Database,
    *,
    surface: str,
    now_utc: datetime | None = None,
    profile_key: str | None = None,
) -> set[int]:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date()
    await ensure_initial_80_stories_campaign(db, now_utc=now_utc)

    async with db.get_session() as session:
        query = (
            select(PromoCampaign, PromoActivity, PromoTarget)
            .join(PromoActivity, PromoActivity.campaign_id == PromoCampaign.id)
            .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
            .where(PromoCampaign.status == "active")
            .where(PromoCampaign.starts_at <= now_utc)
            .where(or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc))
            .where(PromoActivity.enabled.is_(True))
            .where(PromoActivity.surface == surface)
        )
        if profile_key is not None:
            query = query.where(
                or_(PromoActivity.profile_key.is_(None), PromoActivity.profile_key == profile_key)
            )
        rows = list((await session.execute(query.order_by(PromoCampaign.created_at))).all())

    result: set[int] = set()
    for campaign, activity, target in rows:
        events = await _events_for_target(
            db,
            target=target,
            campaign=campaign,
            today=today,
            now_utc=now_utc,
        )
        max_per_publish = max(1, min(int(activity.max_per_publish or 1), 2))
        for ev in events[:max_per_publish]:
            if ev.id is not None:
                result.add(int(ev.id))
    return result


def _event_occurs_on_date(ev: Event, day: date) -> bool:
    start = _event_start_date(ev)
    if start is None:
        return False
    raw_end = str(getattr(ev, "end_date", "") or "").split("..", 1)[0].strip()
    end: date | None = None
    if raw_end:
        try:
            end = date.fromisoformat(raw_end)
        except ValueError:
            end = None
    if end is None:
        end = start
    return start <= day <= end


def _preferred_event_rank(activity: PromoActivity, local_day: date, event_id: int) -> int:
    preferred_ids = _preferred_event_ids_for_date(activity, local_day)
    if preferred_ids is None:
        return 10_000
    try:
        return preferred_ids.index(int(event_id))
    except (ValueError, TypeError):
        return 10_000


def _preferred_event_ids_for_date(activity: PromoActivity, local_day: date) -> list[int] | None:
    cfg = _activity_config(activity)
    by_date = cfg.get("preferred_event_ids_by_date")
    if not isinstance(by_date, dict):
        return None
    raw_ids = by_date.get(local_day.isoformat())
    if not isinstance(raw_ids, list):
        return None
    try:
        return [int(value) for value in raw_ids]
    except (ValueError, TypeError):
        return None


async def resolve_daily_promo_recommendations(
    db: Database,
    *,
    now_utc: datetime | None = None,
    profile_key: str | None = None,
) -> list[DailyPromoRecommendation]:
    now_utc = now_utc or datetime.now(timezone.utc)
    local_day = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date()
    day_start_utc, day_end_utc = _promo_day_bounds(now_utc)

    async with db.get_session() as session:
        query = (
            select(PromoCampaign, PromoActivity, PromoTarget)
            .join(PromoActivity, PromoActivity.campaign_id == PromoCampaign.id)
            .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
            .where(PromoCampaign.status == "active")
            .where(PromoCampaign.starts_at <= now_utc)
            .where(or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc))
            .where(PromoActivity.enabled.is_(True))
            .where(PromoActivity.surface == PROMO_SURFACE_DAILY_RECOMMEND_TODAY)
        )
        if profile_key is not None:
            query = query.where(or_(PromoActivity.profile_key.is_(None), PromoActivity.profile_key == profile_key))
        rows = list(
            (
                await session.execute(
                    query.order_by(PromoCampaign.priority, PromoCampaign.created_at, PromoActivity.id)
                )
            ).all()
        )

    grouped: list[tuple[PromoCampaign, PromoActivity, list[PromoTarget]]] = []
    group_index: dict[tuple[int, int], int] = {}
    for campaign, activity, target in rows:
        if campaign.id is None or activity.id is None:
            continue
        campaign_id = int(campaign.id)
        activity_id = int(activity.id)
        key = (campaign_id, activity_id)
        idx = group_index.get(key)
        if idx is None:
            group_index[key] = len(grouped)
            grouped.append((campaign, activity, [target]))
        else:
            grouped[idx][2].append(target)

    recommendations: list[DailyPromoRecommendation] = []
    used_event_ids: set[int] = set()
    for campaign, activity, targets in grouped:
        campaign_id = int(campaign.id)
        activity_id = int(activity.id)
        max_per_publish = max(1, min(int(activity.max_per_publish or 1), 2))
        if campaign.total_exposure_goal is not None:
            total_count = await _count_public_exposures(db, campaign_id=campaign_id)
            if total_count >= max(0, int(campaign.total_exposure_goal)):
                continue
        if campaign.daily_exposure_cap is not None:
            daily_count = await _count_public_exposures(
                db,
                campaign_id=campaign_id,
                since_utc=day_start_utc,
                until_utc=day_end_utc,
            )
            if daily_count >= max(0, int(campaign.daily_exposure_cap)):
                continue
        if activity.target_exposure_goal is not None:
            activity_total = await _count_public_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
            )
            if activity_total >= max(0, int(activity.target_exposure_goal)):
                continue
        if activity.daily_cap is not None:
            activity_daily = await _count_public_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                since_utc=day_start_utc,
                until_utc=day_end_utc,
            )
            if activity_daily >= max(0, int(activity.daily_cap)):
                continue
        events_by_id: dict[int, Event] = {}
        for target in targets:
            target_events = await _events_for_target(
                db,
                target=target,
                campaign=campaign,
                today=local_day,
                now_utc=now_utc,
            )
            for ev in target_events:
                if ev.id is None:
                    continue
                event_id = int(ev.id)
                if event_id not in events_by_id:
                    events_by_id[event_id] = ev
        events = [
            ev
            for ev in events_by_id.values()
            if ev.id is not None
            and int(ev.id) not in used_event_ids
            and _event_occurs_on_date(ev, local_day)
        ]
        if not events:
            continue
        stats = await _load_public_exposure_stats(
            db,
            campaign_id=campaign_id,
            activity_id=activity_id,
            event_ids=[int(ev.id) for ev in events if ev.id is not None],
        )

        def sort_key(ev: Event) -> tuple[int, int, datetime, str, str, int]:
            event_id = int(ev.id)
            count, last_at = stats.get(event_id, (0, None))
            last_key = last_at or datetime.min.replace(tzinfo=timezone.utc)
            return (
                _preferred_event_rank(activity, local_day, event_id),
                count,
                last_key,
                str(ev.time or ""),
                _stable_shuffle_key(campaign_id, activity_id, local_day.isoformat(), event_id),
                event_id,
            )

        for ev in sorted(events, key=sort_key)[:max_per_publish]:
            event_id = int(ev.id)
            used_event_ids.add(event_id)
            target_url = str(getattr(ev, "telegraph_url", "") or "").strip() or None
            recommendations.append(
                DailyPromoRecommendation(
                    event=ev,
                    campaign_id=campaign_id,
                    activity_id=activity_id,
                    target_url=target_url,
                )
            )
    return recommendations


async def record_daily_promo_recommendation_exposures(
    db: Database,
    *,
    now_utc: datetime | None = None,
) -> int:
    now_utc = now_utc or datetime.now(timezone.utc)
    day_start_utc, day_end_utc = _promo_day_bounds(now_utc)
    recommendations = await resolve_daily_promo_recommendations(db, now_utc=now_utc)
    recorded = 0
    for item in recommendations:
        event_id = int(item.event.id)
        async with db.get_session() as session:
            exists = (
                await session.execute(
                    select(PromoExposure.id)
                    .where(PromoExposure.campaign_id == item.campaign_id)
                    .where(PromoExposure.activity_id == item.activity_id)
                    .where(PromoExposure.event_id == event_id)
                    .where(PromoExposure.surface == PROMO_SURFACE_DAILY_RECOMMEND_TODAY)
                    .where(PromoExposure.published_at >= day_start_utc)
                    .where(PromoExposure.published_at < day_end_utc)
                )
            ).scalars().first()
            if exists is not None:
                continue
            session.add(
                PromoExposure(
                    campaign_id=item.campaign_id,
                    activity_id=item.activity_id,
                    event_id=event_id,
                    surface=PROMO_SURFACE_DAILY_RECOMMEND_TODAY,
                    placement_kind="daily_today_summary",
                    publish_status="DAILY_RECOMMENDED",
                    public_target_count=1,
                    public_targets_json=[
                        {"type": "telegram_daily", "url": item.target_url}
                    ],
                    published_at=now_utc,
                    details_json={
                        "target_url": item.target_url,
                        "local_day": now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date().isoformat(),
                    },
                )
            )
            await session.commit()
            recorded += 1
    return recorded


async def resolve_campaign_promo_event_ids(
    db: Database,
    *,
    now_utc: datetime | None = None,
) -> set[int]:
    """Return events covered by any active promo campaign.

    This is used by channel-specific renderers that should react to the product
    fact "the event is promoted" without requiring a separate activity/surface
    toggle per channel.
    """

    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date()
    await ensure_initial_80_stories_campaign(db, now_utc=now_utc)

    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(PromoCampaign, PromoTarget)
                    .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
                    .where(PromoCampaign.status == "active")
                    .where(PromoCampaign.starts_at <= now_utc)
                    .where(
                        or_(
                            PromoCampaign.ends_at.is_(None),
                            PromoCampaign.ends_at >= now_utc,
                        )
                    )
                    .order_by(PromoCampaign.created_at, PromoTarget.id)
                )
            ).all()
        )

    result: set[int] = set()
    for campaign, target in rows:
        events = await _events_for_target(
            db,
            target=target,
            campaign=campaign,
            today=today,
            now_utc=now_utc,
        )
        for ev in events:
            if ev.id is not None:
                result.add(int(ev.id))
    return result


async def resolve_tg_button_highlight_event_ids(
    db: Database,
    *,
    now_utc: datetime | None = None,
) -> set[int]:
    """Return events whose campaign explicitly enables TG details-button highlight."""

    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.astimezone(ZoneInfo(PROMO_DAILY_TZ)).date()
    await ensure_initial_80_stories_campaign(db, now_utc=now_utc)

    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(PromoCampaign, PromoTarget)
                    .join(PromoActivity, PromoActivity.campaign_id == PromoCampaign.id)
                    .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
                    .where(PromoCampaign.status == "active")
                    .where(PromoCampaign.starts_at <= now_utc)
                    .where(
                        or_(
                            PromoCampaign.ends_at.is_(None),
                            PromoCampaign.ends_at >= now_utc,
                        )
                    )
                    .where(PromoActivity.surface == PROMO_SURFACE_TG_BUTTON_HIGHLIGHT)
                    .where(PromoActivity.enabled.is_(True))
                    .order_by(PromoCampaign.created_at, PromoTarget.id)
                )
            ).all()
        )

    result: set[int] = set()
    for campaign, target in rows:
        events = await _events_for_target(
            db,
            target=target,
            campaign=campaign,
            today=today,
            now_utc=now_utc,
        )
        for ev in events:
            if ev.id is not None:
                result.add(int(ev.id))
    return result


async def record_video_promo_exposures(
    db: Database,
    *,
    session_id: int,
    publish_status: str,
    published_at: datetime,
    public_target_count: int = 0,
    public_targets: list[dict] | None = None,
) -> int:
    async with db.get_session() as session:
        res = await session.execute(
            select(VideoAnnounceItem)
            .where(VideoAnnounceItem.session_id == session_id)
            .where(VideoAnnounceItem.promo_campaign_id.is_not(None))
        )
        items = res.scalars().all()
        added = 0
        for item in items:
            if not item.promo_campaign_id:
                continue
            exists_res = await session.execute(
                select(PromoExposure.id)
                .where(PromoExposure.video_item_id == item.id)
                .where(PromoExposure.publish_status == publish_status)
            )
            if exists_res.scalars().first() is not None:
                continue
            session.add(
                PromoExposure(
                    campaign_id=int(item.promo_campaign_id),
                    activity_id=int(item.promo_activity_id) if item.promo_activity_id else None,
                    event_id=int(item.event_id),
                    surface="video",
                    placement_kind=item.promo_placement_kind or "general_boost",
                    video_session_id=session_id,
                    video_item_id=int(item.id) if item.id is not None else None,
                    position=int(item.position or 0),
                    publish_status=publish_status,
                    public_target_count=public_target_count,
                    public_targets_json=public_targets or [],
                    published_at=published_at,
                )
            )
            added += 1
        if added:
            await session.commit()
        return added


def _vk_owner_post_from_url(url: str | None) -> tuple[int, int] | None:
    import re

    match = re.search(r"wall(-?\d+)_(\d+)", str(url or ""))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


async def _resolve_vk_group_id(ref: str | int | None) -> int | None:
    raw = _vk_group_ref(ref)
    if not raw:
        return None
    if raw.lstrip("-").isdigit():
        return abs(int(raw))
    from main import vk_resolve_group

    group_id, _name, _screen_name, owner_type = await vk_resolve_group(raw)
    if owner_type != "group":
        return None
    return int(group_id)


async def _vk_post_datetime(url: str | None) -> datetime | None:
    ids = _vk_owner_post_from_url(url)
    if not ids:
        return None
    owner_id, post_id = ids
    from main import vk_api

    response = await vk_api("wall.getById", posts=f"{owner_id}_{post_id}")
    items = response.get("response") if isinstance(response, dict) else response
    if not isinstance(items, list):
        items = [items] if items else []
    if not items:
        try:
            from main import VK_USER_TOKEN, _vk_api
        except Exception:
            VK_USER_TOKEN = None
            _vk_api = None
        if VK_USER_TOKEN and _vk_api is not None:
            try:
                response = await _vk_api(
                    "wall.getById",
                    {"posts": f"{owner_id}_{post_id}"},
                    token=VK_USER_TOKEN,
                    token_kind="user",
                )
                items = response.get("response") if isinstance(response, dict) else response
                if not isinstance(items, list):
                    items = [items] if items else []
            except Exception:
                logger.warning("promo.vk user post date lookup failed url=%s", url, exc_info=True)
    if not items:
        return None
    raw_ts = items[0].get("date") or items[0].get("publish_date")
    if not isinstance(raw_ts, int):
        return None
    return datetime.fromtimestamp(raw_ts, timezone.utc)


def _vk_url_matches_group(url: str | None, group_id: int) -> bool:
    ids = _vk_owner_post_from_url(url)
    return bool(ids and abs(int(ids[0])) == abs(int(group_id)))


async def _recent_event_vk_posts(
    events: list[Event],
    *,
    group_id: int,
    since_utc: datetime,
    until_utc: datetime,
    db: Database | None = None,
) -> list[tuple[Event, str, datetime]]:
    rows: list[tuple[Event, str, datetime]] = []
    for ev in events:
        url = str(getattr(ev, "source_vk_post_url", None) or "").strip()
        if not url or not _vk_url_matches_group(url, group_id):
            continue
        try:
            posted_at = await _vk_post_datetime(url)
        except Exception:
            logger.warning("promo.vk post date lookup failed url=%s", url, exc_info=True)
            posted_at = None
        if posted_at is None:
            try:
                from main import _resolve_existing_vk_post_url

                resolved_url = await _resolve_existing_vk_post_url(
                    url,
                    target_group_id=str(abs(int(group_id))),
                    db=db,
                    bot=None,
                )
            except Exception:
                logger.warning(
                    "promo.vk post postponed-id resolution failed url=%s",
                    url,
                    exc_info=True,
                )
                resolved_url = url
            if resolved_url and resolved_url != url and _vk_url_matches_group(resolved_url, group_id):
                try:
                    resolved_posted_at = await _vk_post_datetime(resolved_url)
                except Exception:
                    logger.warning(
                        "promo.vk resolved post date lookup failed url=%s",
                        resolved_url,
                        exc_info=True,
                    )
                    resolved_posted_at = None
                if resolved_posted_at is not None:
                    logger.info(
                        "promo.vk resolved stale event source post event_id=%s %s -> %s",
                        getattr(ev, "id", None),
                        url,
                        resolved_url,
                    )
                    url = resolved_url
                    posted_at = resolved_posted_at
                    ev.source_vk_post_url = resolved_url
                    if db is not None and getattr(ev, "id", None) is not None:
                        async with db.get_session() as session:
                            fresh = await session.get(Event, int(ev.id))
                            if fresh is not None and (fresh.source_vk_post_url or "").strip() != resolved_url:
                                fresh.source_vk_post_url = resolved_url
                                session.add(fresh)
                                await session.commit()
        if posted_at is None:
            continue
        if posted_at and since_utc <= posted_at <= until_utc:
            rows.append((ev, url, posted_at))
    return rows


def _tg_channel_message_link(target_chat: str, message_id: int) -> str:
    chat = str(target_chat or "").strip()
    if chat.startswith("@"):
        return f"https://t.me/{chat.lstrip('@')}/{message_id}"
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", chat):
        return f"https://t.me/{chat}/{message_id}"
    if chat.startswith("-100"):
        return f"https://t.me/c/{chat[4:]}/{message_id}"
    return f"tg:{chat}/{message_id}"


def _tg_message_id_from_url(url: str | None) -> int | None:
    text = str(url or "").strip()
    match = re.search(
        r"https?://(?:t\.me|telegram\.me)/(?:c/\d+|[A-Za-z0-9_]+)/(\d+)(?:\D|$)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _promo_repeat_key(ev: Event) -> str:
    title = _norm_text(getattr(ev, "title", None))
    return " ".join(re.sub(r"[^\w\s]+", " ", title, flags=re.UNICODE).split())


async def _recent_activity_event_repeat_keys(
    db: Database,
    *,
    campaign_id: int,
    activity_id: int,
    surface: str,
    since_utc: datetime,
    until_utc: datetime,
) -> set[str]:
    async with db.get_session() as session:
        res = await session.execute(
            select(PromoExposure)
            .where(PromoExposure.campaign_id == campaign_id)
            .where(PromoExposure.activity_id == activity_id)
            .where(PromoExposure.surface == surface)
        )
        exposures = list(res.scalars().all())
    filtered: list[PromoExposure] = []
    for exposure in exposures:
        published_at = exposure.published_at
        if published_at is None:
            continue
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        if since_utc <= published_at <= until_utc:
            filtered.append(exposure)
    exposures = filtered
    event_ids = sorted({int(exposure.event_id) for exposure in exposures if exposure.event_id is not None})
    if not event_ids:
        return set()
    async with db.get_session() as session:
        res = await session.execute(select(Event).where(Event.id.in_(event_ids)))
        events = list(res.scalars().all())
    return {key for key in (_promo_repeat_key(ev) for ev in events) if key}


def _tg_url_matches_chat(url: str | None, chat: str | None) -> bool:
    text = str(url or "").strip().lower()
    if not text:
        return False
    source_name = str(chat or "").strip().lstrip("@").lower()
    if source_name and (
        f"t.me/{source_name}/" in text or f"telegram.me/{source_name}/" in text
    ):
        return True
    # Private/internal channel links do not carry the public username.  Treat
    # them as forwardable for the configured channel, matching tg_repost source
    # selection behavior.
    return "t.me/c/" in text or "telegram.me/c/" in text


def _tg_source_url_for_chat(ev: Event, chat: str | None) -> str | None:
    url = str(getattr(ev, "tg_event_post_url", "") or "").strip()
    if not url or _tg_message_id_from_url(url) is None:
        return None
    if not _tg_url_matches_chat(url, chat):
        return None
    return url


_TG_SOURCE_MONTHS = {
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
_TG_SOURCE_DATE_RE = re.compile(
    r"(?iu)(?<!\d)(\d{1,2})(?:\s*[-–—]\s*(\d{1,2}))?\s+"
    r"(" + "|".join(_TG_SOURCE_MONTHS) + r")(?:\s+((?:19|20)\d{2}))?\b"
)


def _tg_source_snapshot_dates(text: str | None, *, year_hint: int) -> set[date]:
    """Extract dates from our generated Telegram event-post snapshot.

    The source row is immutable evidence of what the already-published post
    says.  This parser is intentionally narrow: it validates the bot's stable
    ``23 августа 10:00`` rendering rather than making a new semantic decision
    from arbitrary social copy.
    """

    dates: set[date] = set()
    for match in _TG_SOURCE_DATE_RE.finditer(str(text or "")):
        month = _TG_SOURCE_MONTHS.get(match.group(3).casefold())
        year = int(match.group(4) or year_hint)
        if not month:
            continue
        for raw_day in (match.group(1), match.group(2)):
            if not raw_day:
                continue
            try:
                dates.add(date(year, month, int(raw_day)))
            except ValueError:
                continue
    return dates


async def _tg_source_snapshots_for_events(
    db: Database,
    *,
    events: list[Event],
    source_chat: str,
) -> dict[int, tuple[str, datetime]]:
    event_ids = [int(ev.id) for ev in events if ev.id is not None]
    if not event_ids:
        return {}
    source_name = str(source_chat or "").strip().lstrip("@").casefold()
    async with db.get_session() as session:
        res = await session.execute(
            select(EventSource)
            .where(EventSource.event_id.in_(event_ids))
            .where(EventSource.source_type == "telegram")
            .order_by(EventSource.imported_at.asc(), EventSource.id.asc())
        )
        sources = list(res.scalars().all())
    by_id: dict[int, tuple[str, datetime]] = {}
    event_by_id = {int(ev.id): ev for ev in events if ev.id is not None}
    for source in sources:
        event_id = int(source.event_id)
        if event_id in by_id:
            continue
        ev = event_by_id.get(event_id)
        if ev is None:
            continue
        post_id = _tg_message_id_from_url(str(getattr(ev, "tg_event_post_url", "") or ""))
        if post_id is None or int(getattr(source, "source_message_id", 0) or 0) != post_id:
            continue
        source_username = str(getattr(source, "source_chat_username", "") or "").lstrip("@").casefold()
        if source_name and source_username and source_username != source_name:
            continue
        observed_at = getattr(source, "imported_at", None)
        if not isinstance(observed_at, datetime):
            continue
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        else:
            observed_at = observed_at.astimezone(timezone.utc)
        by_id[event_id] = (str(getattr(source, "source_text", "") or ""), observed_at)
    return by_id


async def _recent_event_tg_organic_posts(
    db: Database,
    *,
    events: list[Event],
    target_chat: str,
    since_utc: datetime,
    until_utc: datetime,
) -> list[tuple[Event, str, datetime]]:
    """Return ordinary Telegram event posts in ``target_chat`` within a window.

    ``event.tg_event_post_url`` stores the source message link but does not
    store the publication timestamp.  For organic Smart Update publications the
    reliable timestamp is the completed ``JobOutbox.tg_event_publish`` row.  If
    an old event has only a stored URL and no recent done job, it is not counted
    as already satisfying today's rolling minimum; the promo publisher may then
    self-forward that original message instead of rendering a duplicate post.
    """

    event_by_id = {
        int(ev.id): ev
        for ev in events
        if ev.id is not None and _tg_source_url_for_chat(ev, target_chat)
    }
    if not event_by_id:
        return []
    async with db.get_session() as session:
        res = await session.execute(
            select(JobOutbox.event_id, func.max(JobOutbox.updated_at))
            .where(JobOutbox.event_id.in_(list(event_by_id)))
            .where(JobOutbox.task == JobTask.tg_event_publish)
            .where(JobOutbox.status == JobStatus.done)
            .where(JobOutbox.updated_at >= since_utc)
            .where(JobOutbox.updated_at <= until_utc)
            .group_by(JobOutbox.event_id)
        )
        rows = list(res.all())
    out: list[tuple[Event, str, datetime]] = []
    for event_id, updated_at in rows:
        ev = event_by_id.get(int(event_id))
        if ev is None or updated_at is None:
            continue
        url = _tg_source_url_for_chat(ev, target_chat)
        if not url:
            continue
        out.append((ev, url, updated_at))
    return out


async def _recent_event_tg_posts(
    db: Database,
    *,
    campaign_id: int,
    events: list[Event],
    source_chat: str,
    since_utc: datetime,
    until_utc: datetime,
    min_lead_hours: float = 4,
) -> list[tuple[Event, str, datetime]]:
    rows: list[tuple[Event, str, datetime]] = []
    source_name = str(source_chat or "").strip().lstrip("@").lower()
    event_by_id = {int(ev.id): ev for ev in events if ev.id is not None}
    source_snapshots = await _tg_source_snapshots_for_events(
        db,
        events=events,
        source_chat=source_chat,
    )
    for ev in events:
        if not event_is_repostable_for_promo(
            ev,
            now_utc=until_utc,
            min_lead_hours=min_lead_hours,
        ):
            continue
        url = str(getattr(ev, "tg_event_post_url", "") or "").strip()
        if not url:
            continue
        url_l = url.lower()
        is_public_source_url = bool(
            source_name
            and (
                f"t.me/{source_name}/" in url_l
                or f"telegram.me/{source_name}/" in url_l
            )
        )
        is_internal_tme_c_url = "t.me/c/" in url_l or "telegram.me/c/" in url_l
        if source_name and not (is_public_source_url or is_internal_tme_c_url):
            continue
        observed_at = until_utc
        snapshot = source_snapshots.get(int(ev.id or 0))
        if snapshot is not None:
            source_text, observed_at = snapshot
            event_date = _event_start_date(ev)
            snapshot_dates = _tg_source_snapshot_dates(
                source_text,
                year_hint=event_date.year if event_date else until_utc.year,
            )
            if event_date is not None and snapshot_dates and event_date not in snapshot_dates:
                logger.warning(
                    "promo.tg repost skip stale source snapshot event_id=%s event_date=%s source_dates=%s source_url=%s",
                    getattr(ev, "id", None),
                    event_date.isoformat(),
                    sorted(item.isoformat() for item in snapshot_dates),
                    url,
                )
                continue
        rows.append((ev, url, observed_at))

    publication_exposures = await _recent_activity_exposures(
        db,
        campaign_id=campaign_id,
        activity_id=None,
        surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
        since_utc=since_utc,
        until_utc=until_utc,
    )
    seen_urls = {url for _ev, url, _dt in rows}
    for exposure in publication_exposures:
        ev = event_by_id.get(int(exposure.event_id))
        if ev is None:
            continue
        if not event_is_repostable_for_promo(
            ev,
            now_utc=until_utc,
            min_lead_hours=min_lead_hours,
        ):
            continue
        details = exposure.details_json if isinstance(exposure.details_json, dict) else {}
        url = str(details.get("target_url") or (exposure.public_targets_json or [{}])[0].get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        url_l = url.lower()
        is_public_source_url = bool(
            source_name
            and (
                f"t.me/{source_name}/" in url_l
                or f"telegram.me/{source_name}/" in url_l
            )
        )
        is_internal_tme_c_url = "t.me/c/" in url_l or "telegram.me/c/" in url_l
        if source_name and not (is_public_source_url or is_internal_tme_c_url):
            continue
        rows.append((ev, url, exposure.published_at or until_utc))
        seen_urls.add(url)
    return rows


async def _weighted_popularity_scores_for_events(
    db: Database,
    *,
    event_ids: Iterable[int],
    activity: PromoActivity,
    now_utc: datetime,
) -> dict[int, PromoPopularityScore]:
    """Score events using /popular_posts-style source popularity plus owned VK boost.

    Source TG/VK metrics use the same above-per-source-median idea as
    `/popular_posts`. Owned VK posts (configured group ids, normally
    klgdevents + kenigeventsofficial) are added as a separate signal with a
    higher weight because they reflect the bot's own audience.
    """

    ids = sorted({int(event_id) for event_id in event_ids if event_id is not None})
    if not ids:
        return {}
    id_set = set(ids)
    cfg = _activity_config(activity)
    window_days = max(1, int(cfg.get("popularity_window_days") or 7))
    preferred_age_day = max(0, int(cfg.get("popularity_preferred_age_day") or min(6, window_days - 1)))
    min_sample = max(2, int(cfg.get("popularity_min_sample") or _env_int("POST_POPULARITY_MIN_SAMPLE", 2)))
    source_weight = float(cfg.get("source_popularity_weight") or 1.0)
    owned_vk_weight = float(cfg.get("owned_vk_popularity_weight") or 4.0)
    owned_vk_group_ids = set(_csv_ints(cfg.get("owned_vk_group_ids")))
    since_ts = int(now_utc.timestamp()) - window_days * 86400

    scores: dict[int, dict[str, float | int]] = {
        event_id: {
            "source_score": 0.0,
            "owned_vk_score": 0.0,
            "source_count": 0,
            "owned_vk_count": 0,
        }
        for event_id in ids
    }

    def dedupe_latest(
        rows: list[tuple],
        *,
        key_indexes: tuple[int, int],
        age_idx: int,
    ) -> list[tuple]:
        best: dict[tuple[int, int], tuple[int, tuple]] = {}
        for row in rows:
            try:
                key = (int(row[key_indexes[0]]), int(row[key_indexes[1]]))
                age = int(row[age_idx])
            except Exception:
                continue
            current = best.get(key)
            if current is None or age > current[0]:
                best[key] = (age, row)
        return [item[1] for item in best.values()]

    def add_score(event_id: int, value: float, *, owned: bool) -> None:
        if event_id not in scores or value <= 0:
            return
        key = "owned_vk_score" if owned else "source_score"
        count_key = "owned_vk_count" if owned else "source_count"
        scores[event_id][key] = float(scores[event_id][key]) + float(value)
        scores[event_id][count_key] = int(scores[event_id][count_key]) + 1

    placeholders = ",".join("?" for _ in ids)

    async with db.raw_conn() as conn:
        # Telegram source posts: baseline over all imported metric rows, then map
        # source_chat_username/message_id to the requested event ids.
        tg_rows: list[tuple] = []
        try:
            cur = await conn.execute(
                """
                SELECT
                    m.source_id,
                    m.message_id,
                    m.age_day,
                    m.views,
                    m.likes,
                    COALESCE(t.username, '') AS username
                FROM telegram_post_metric m
                JOIN telegram_scanned_message s
                  ON s.source_id = m.source_id
                 AND s.message_id = m.message_id
                JOIN telegram_source t
                  ON t.id = m.source_id
                WHERE m.age_day <= ?
                  AND m.message_ts IS NOT NULL
                  AND m.message_ts >= ?
                  AND COALESCE(s.events_imported, 0) > 0
                """,
                (preferred_age_day, since_ts),
            )
            tg_rows = await cur.fetchall()
        except Exception:
            logger.debug("promo.weighted_popularity: TG metric load failed", exc_info=True)
            tg_rows = []
        tg_rows = dedupe_latest(tg_rows, key_indexes=(0, 1), age_idx=2)

        tg_event_map: dict[tuple[str, int], set[int]] = {}
        try:
            cur = await conn.execute(
                f"""
                SELECT source_chat_username, source_message_id, event_id
                FROM event_source
                WHERE event_id IN ({placeholders})
                  AND source_chat_username IS NOT NULL
                  AND source_message_id IS NOT NULL
                """,
                tuple(ids),
            )
            for username, message_id, event_id in await cur.fetchall():
                try:
                    eid = int(event_id)
                    mid = int(message_id)
                except Exception:
                    continue
                if eid not in id_set:
                    continue
                uname = str(username or "").strip().lstrip("@").lower()
                if not uname or mid <= 0:
                    continue
                tg_event_map.setdefault((uname, mid), set()).add(eid)
        except Exception:
            logger.debug("promo.weighted_popularity: TG event map load failed", exc_info=True)

        tg_by_source: dict[int, list[tuple]] = {}
        for row in tg_rows:
            try:
                tg_by_source.setdefault(int(row[0]), []).append(row)
            except Exception:
                continue
        for rows in tg_by_source.values():
            sample = len({int(row[1]) for row in rows})
            median_views = _median_int(int(row[3]) for row in rows if isinstance(row[3], int) and int(row[3]) >= 0)
            median_likes = _median_int(int(row[4]) for row in rows if isinstance(row[4], int) and int(row[4]) >= 0)
            for source_id, message_id, _age, views, likes, username in rows:
                score = _popularity_metric_score(
                    views=views if isinstance(views, int) else None,
                    likes=likes if isinstance(likes, int) else None,
                    median_views=median_views,
                    median_likes=median_likes,
                    sample=sample,
                    min_sample=min_sample,
                )
                if score <= 0:
                    continue
                uname = str(username or "").strip().lstrip("@").lower()
                for event_id in tg_event_map.get((uname, int(message_id)), set()):
                    add_score(event_id, score * source_weight, owned=False)

        # VK source posts imported from monitored communities, matching /popular_posts.
        vk_rows: list[tuple] = []
        try:
            cur = await conn.execute(
                """
                SELECT DISTINCT
                    m.group_id,
                    m.post_id,
                    m.age_day,
                    m.views,
                    m.likes
                FROM vk_post_metric m
                JOIN vk_inbox i
                  ON i.group_id = m.group_id
                 AND i.post_id = m.post_id
                JOIN vk_inbox_import_event ie
                  ON ie.inbox_id = i.id
                WHERE m.age_day <= ?
                  AND m.post_ts IS NOT NULL
                  AND m.post_ts >= ?
                """,
                (preferred_age_day, since_ts),
            )
            vk_rows = await cur.fetchall()
        except Exception:
            logger.debug("promo.weighted_popularity: VK metric load failed", exc_info=True)
            vk_rows = []
        vk_rows = dedupe_latest(vk_rows, key_indexes=(0, 1), age_idx=2)

        vk_event_map: dict[tuple[int, int], set[int]] = {}
        try:
            cur = await conn.execute(
                f"""
                SELECT i.group_id, i.post_id, ie.event_id
                FROM vk_inbox i
                JOIN vk_inbox_import_event ie ON ie.inbox_id = i.id
                WHERE ie.event_id IN ({placeholders})
                """,
                tuple(ids),
            )
            for group_id, post_id, event_id in await cur.fetchall():
                try:
                    eid = int(event_id)
                    key = (int(group_id), int(post_id))
                except Exception:
                    continue
                if eid in id_set:
                    vk_event_map.setdefault(key, set()).add(eid)
        except Exception:
            logger.debug("promo.weighted_popularity: VK event map load failed", exc_info=True)

        vk_by_group: dict[int, list[tuple]] = {}
        for row in vk_rows:
            try:
                vk_by_group.setdefault(int(row[0]), []).append(row)
            except Exception:
                continue
        for rows in vk_by_group.values():
            sample = len({int(row[1]) for row in rows})
            median_views = _median_int(int(row[3]) for row in rows if isinstance(row[3], int) and int(row[3]) >= 0)
            median_likes = _median_int(int(row[4]) for row in rows if isinstance(row[4], int) and int(row[4]) >= 0)
            for group_id, post_id, _age, views, likes in rows:
                score = _popularity_metric_score(
                    views=views if isinstance(views, int) else None,
                    likes=likes if isinstance(likes, int) else None,
                    median_views=median_views,
                    median_likes=median_likes,
                    sample=sample,
                    min_sample=min_sample,
                )
                if score <= 0:
                    continue
                owned = int(group_id) in owned_vk_group_ids
                # Imported posts from owned groups are rare, but if present they
                # should be treated as owned-audience signal rather than internet-source signal.
                weight = owned_vk_weight if owned else source_weight
                for event_id in vk_event_map.get((int(group_id), int(post_id)), set()):
                    add_score(event_id, score * weight, owned=owned)

        # Owned VK posts: event.source_vk_post_url / event.vk_repost_url plus
        # promo vk_repost target URLs. These posts are not necessarily in vk_inbox,
        # but vk_post_metric can still contain audience metrics for them.
        owned_post_map: dict[tuple[int, int], set[int]] = {}
        if owned_vk_group_ids:
            try:
                cur = await conn.execute(
                    f"""
                    SELECT id, source_vk_post_url, vk_repost_url
                    FROM event
                    WHERE id IN ({placeholders})
                    """,
                    tuple(ids),
                )
                for event_id, source_vk_post_url, vk_repost_url in await cur.fetchall():
                    try:
                        eid = int(event_id)
                    except Exception:
                        continue
                    for url in (source_vk_post_url, vk_repost_url):
                        ids_pair = _vk_wall_ids_from_url(url)
                        if ids_pair and ids_pair[0] in owned_vk_group_ids:
                            owned_post_map.setdefault(ids_pair, set()).add(eid)
            except Exception:
                logger.debug("promo.weighted_popularity: owned event VK urls load failed", exc_info=True)

            try:
                cur = await conn.execute(
                    f"""
                    SELECT event_id, public_targets_json, details_json
                    FROM promo_exposure
                    WHERE event_id IN ({placeholders})
                      AND surface IN ('vk_repost', 'vk_publication')
                    """,
                    tuple(ids),
                )
                for event_id, public_targets_json, details_json in await cur.fetchall():
                    try:
                        eid = int(event_id)
                    except Exception:
                        continue
                    urls: list[str] = []
                    for raw in (public_targets_json, details_json):
                        try:
                            data = json.loads(raw) if isinstance(raw, str) else raw
                        except Exception:
                            data = None
                        if isinstance(data, dict):
                            urls.extend(str(data.get(key) or "") for key in ("target_url", "source_url"))
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    urls.append(str(item.get("url") or ""))
                    for url in urls:
                        ids_pair = _vk_wall_ids_from_url(url)
                        if ids_pair and ids_pair[0] in owned_vk_group_ids:
                            owned_post_map.setdefault(ids_pair, set()).add(eid)
            except Exception:
                logger.debug("promo.weighted_popularity: owned promo exposure URLs load failed", exc_info=True)

        if owned_post_map:
            owned_rows: list[tuple] = []
            group_placeholders = ",".join("?" for _ in owned_vk_group_ids)
            try:
                cur = await conn.execute(
                    f"""
                    SELECT group_id, post_id, age_day, views, likes
                    FROM vk_post_metric
                    WHERE group_id IN ({group_placeholders})
                      AND age_day <= ?
                      AND post_ts IS NOT NULL
                      AND post_ts >= ?
                    """,
                    tuple(sorted(owned_vk_group_ids)) + (preferred_age_day, since_ts),
                )
                owned_rows = await cur.fetchall()
            except Exception:
                logger.debug("promo.weighted_popularity: owned VK metrics load failed", exc_info=True)
                owned_rows = []
            owned_rows = dedupe_latest(owned_rows, key_indexes=(0, 1), age_idx=2)
            owned_by_group: dict[int, list[tuple]] = {}
            for row in owned_rows:
                try:
                    owned_by_group.setdefault(int(row[0]), []).append(row)
                except Exception:
                    continue
            for rows in owned_by_group.values():
                sample = len({int(row[1]) for row in rows})
                median_views = _median_int(int(row[3]) for row in rows if isinstance(row[3], int) and int(row[3]) >= 0)
                median_likes = _median_int(int(row[4]) for row in rows if isinstance(row[4], int) and int(row[4]) >= 0)
                for group_id, post_id, _age, views, likes in rows:
                    mapped_ids = owned_post_map.get((int(group_id), int(post_id)), set())
                    if not mapped_ids:
                        continue
                    score = _popularity_metric_score(
                        views=views if isinstance(views, int) else None,
                        likes=likes if isinstance(likes, int) else None,
                        median_views=median_views,
                        median_likes=median_likes,
                        sample=sample,
                        min_sample=min_sample,
                    )
                    for event_id in mapped_ids:
                        add_score(event_id, score * owned_vk_weight, owned=True)

    out: dict[int, PromoPopularityScore] = {}
    for event_id, parts in scores.items():
        source_score = float(parts["source_score"])
        owned_score = float(parts["owned_vk_score"])
        total = source_score + owned_score
        if total <= 0:
            continue
        out[event_id] = PromoPopularityScore(
            score=total,
            source_score=source_score,
            owned_vk_score=owned_score,
            source_count=int(parts["source_count"]),
            owned_vk_count=int(parts["owned_vk_count"]),
        )
    return out


async def _publish_tg_repost(
    bot: object | None,
    *,
    source_chat: str,
    target_chat: str,
    source_url: str,
) -> str | None:
    if bot is None:
        return None
    message_id = _tg_message_id_from_url(source_url)
    if message_id is None:
        raise RuntimeError(f"telegram source message id not found: {source_url}")
    sent = await bot.forward_message(
        chat_id=target_chat,
        from_chat_id=source_chat,
        message_id=message_id,
    )
    return _tg_channel_message_link(target_chat, int(sent.message_id))


def _post_text_matches_event(text: str | None, ev: Event) -> bool:
    """Whether a VK wall post text is the source post for ``ev``.

    Promo publications post the event via ``build_vk_source_message`` whose
    body contains the event title, so a normalized title-substring match
    identifies the published post even after VK reassigns the wall id on
    postponed publication. Short/generic titles (<8 chars) are not trusted.
    """

    title = _norm_text(getattr(ev, "title", None))
    if len(title) < 8:
        return False
    return title in _norm_text(text)


def _match_published_post_for_event(
    recent_wall: list[dict], ev: Event
) -> dict | None:
    """Pick the most recent wall post (from ``vk_wall_since``) matching ``ev``."""
    best: dict | None = None
    for post in recent_wall:
        if not isinstance(post, dict):
            continue
        if not _post_text_matches_event(post.get("text"), ev):
            continue
        if best is None or int(post.get("date") or 0) > int(best.get("date") or 0):
            best = post
    return best


async def _reconcile_exposure_target_url(
    db: Database,
    *,
    exposure_id: int,
    url: str,
    published_at: datetime,
) -> None:
    """Repoint a vk_publication exposure to its live published wall URL.

    Fixes both repost eligibility and the ``/promo`` stats links, which
    otherwise reference the no-longer-resolvable postponed-draft id.
    """
    async with db.get_session() as session:
        exposure = await session.get(PromoExposure, exposure_id)
        if exposure is None:
            return
        details = dict(exposure.details_json) if isinstance(exposure.details_json, dict) else {}
        details["target_url"] = url
        details["vk_post_date"] = published_at.isoformat()
        details["reconciled_published_url"] = True
        exposure.details_json = details
        exposure.public_targets_json = [{"type": "vk_wall", "url": url}]
        session.add(exposure)
        await session.commit()


async def _recent_activity_exposures(
    db: Database,
    *,
    campaign_id: int,
    activity_id: int | None,
    surface: str,
    since_utc: datetime,
    until_utc: datetime | None = None,
    public_only: bool = True,
    statuses: Collection[str] | None = None,
) -> list[PromoExposure]:
    async with db.get_session() as session:
        query = (
            select(PromoExposure)
            .where(PromoExposure.campaign_id == campaign_id)
            .where(PromoExposure.surface == surface)
            .where(PromoExposure.published_at >= since_utc)
        )
        if activity_id is not None:
            query = query.where(PromoExposure.activity_id == activity_id)
        if until_utc is not None:
            query = query.where(PromoExposure.published_at <= until_utc)
        if statuses is not None:
            query = query.where(PromoExposure.publish_status.in_(statuses))
        elif public_only:
            query = query.where(PromoExposure.publish_status.in_(PUBLIC_PROMO_EXPOSURE_STATUSES))
        res = await session.execute(query.order_by(PromoExposure.published_at.desc()))
        return list(res.scalars().all())


async def _activity_day_exposures(
    db: Database,
    *,
    campaign_id: int,
    activity_id: int,
    surface: str,
    now_utc: datetime,
    statuses: Collection[str] | None = None,
) -> list[PromoExposure]:
    day_start_utc, day_end_utc = _promo_day_bounds(now_utc)
    return await _recent_activity_exposures(
        db,
        campaign_id=campaign_id,
        activity_id=activity_id,
        surface=surface,
        since_utc=day_start_utc,
        until_utc=day_end_utc,
        statuses=statuses,
    )


async def _build_promo_vk_source_post(
    db: Database,
    bot: object | None,
    ev: Event,
    *,
    campaign_id: int | None = None,
    activity_id: int | None = None,
    target_group_id: int,
) -> str | None:
    from main import (
        VK_MAX_ATTACHMENTS,
        VK_PHOTOS_ENABLED,
        _vk_api,
        _dedupe_event_photo_urls_for_publish,
        build_vk_source_message,
        post_to_vk,
        upload_images,
        upload_vk_photo,
        upload_vk_photo_bytes,
    )

    festival = None
    if ev.festival:
        async with db.get_session() as session:
            res = await session.execute(select(Festival).where(Festival.name == ev.festival))
            festival = res.scalars().first()
    text_for_vk = (getattr(ev, "description", None) or "").strip() or (ev.source_text or "")
    message = build_vk_source_message(ev, text_for_vk, festival=festival)
    attachments: list[str] = []
    photo_urls = (
        await _dedupe_event_photo_urls_for_publish(
            await _ensure_promo_vk_photo_urls(db, ev)
        )
    )[:VK_MAX_ATTACHMENTS]
    if VK_PHOTOS_ENABLED:
        for photo_url in photo_urls:
            photo_id = await upload_vk_photo(str(target_group_id), photo_url, db, bot)
            if photo_id:
                attachments.append(photo_id)
    source_kind = "telegram" if _is_telegram_origin_event(ev) else "other"
    require_media = _require_media_for_telegram_vk_posts()
    logger.info(
        "promo.vk publication media campaign_id=%s activity_id=%s event_id=%s target_group_id=%s "
        "source_kind=%s source_post_url=%s photos_enabled=%s photo_urls_count=%s attachments_count=%s require_media=%s",
        campaign_id,
        activity_id,
        getattr(ev, "id", None),
        target_group_id,
        source_kind,
        getattr(ev, "source_post_url", None),
        bool(VK_PHOTOS_ENABLED),
        len(photo_urls),
        len(attachments),
        require_media,
    )
    missing_required_telegram_media = source_kind == "telegram" and require_media and not attachments
    lost_available_media = bool(VK_PHOTOS_ENABLED and photo_urls and not attachments)
    if missing_required_telegram_media or lost_available_media:
        logger.error(
            "promo.vk publication missing media campaign_id=%s activity_id=%s event_id=%s target_group_id=%s "
            "source_post_url=%s photo_urls_count=%s attachments_count=%s reason=%s",
            campaign_id,
            activity_id,
            getattr(ev, "id", None),
            target_group_id,
            getattr(ev, "source_post_url", None),
            len(photo_urls),
            len(attachments),
            VK_SYNC_MISSING_TG_MEDIA_ERROR,
        )
        raise RuntimeError(VK_SYNC_MISSING_TG_MEDIA_ERROR)
    try:
        from afishaengagement import maybe_publish_shadow_debug_copy

        public_url = await maybe_publish_shadow_debug_copy(
            event=ev,
            db=db,
            bot=bot,
            target_group_id=str(target_group_id),
            message=message,
            photo_urls=photo_urls[:VK_MAX_ATTACHMENTS],
            post_to_vk_fn=post_to_vk,
            upload_vk_photo_fn=upload_vk_photo,
            upload_images_fn=upload_images,
            vk_api_fn=_vk_api,
            upload_vk_photo_bytes_fn=upload_vk_photo_bytes,
            public_only=True,
        )
        if public_url:
            logger.info(
                "promo.vk publication afishaengagement public selected campaign_id=%s activity_id=%s "
                "event_id=%s target_group_id=%s url=%s",
                campaign_id,
                activity_id,
                getattr(ev, "id", None),
                target_group_id,
                public_url,
            )
            return public_url
    except Exception:
        logger.exception(
            "promo.vk publication afishaengagement public preflight failed campaign_id=%s activity_id=%s "
            "event_id=%s target_group_id=%s",
            campaign_id,
            activity_id,
            getattr(ev, "id", None),
            target_group_id,
        )

    url = await post_to_vk(
        str(target_group_id),
        message,
        db,
        bot,
        attachments or None,
    )
    if url:
        try:
            shadow_url = await maybe_publish_shadow_debug_copy(
                event=ev,
                db=db,
                bot=bot,
                target_group_id=str(target_group_id),
                message=message,
                photo_urls=photo_urls[:VK_MAX_ATTACHMENTS],
                post_to_vk_fn=post_to_vk,
                upload_vk_photo_fn=upload_vk_photo,
                upload_images_fn=upload_images,
                vk_api_fn=_vk_api,
                upload_vk_photo_bytes_fn=upload_vk_photo_bytes,
                shadow_only=True,
            )
            logger.info(
                "promo.vk publication afishaengagement checked campaign_id=%s activity_id=%s event_id=%s "
                "target_group_id=%s source_url=%s shadow_url=%s",
                campaign_id,
                activity_id,
                getattr(ev, "id", None),
                target_group_id,
                url,
                shadow_url,
            )
        except Exception:
            logger.exception(
                "promo.vk publication afishaengagement failed campaign_id=%s activity_id=%s event_id=%s "
                "target_group_id=%s source_url=%s",
                campaign_id,
                activity_id,
                getattr(ev, "id", None),
                target_group_id,
                url,
            )
    return url


async def _build_promo_vk_repost_caption(ev: Event) -> str:
    from main import build_short_vk_text

    source_text = (
        str(getattr(ev, "source_text", None) or "").strip()
        or str(getattr(ev, "description", None) or "").strip()
        or str(getattr(ev, "search_digest", None) or "").strip()
        or str(getattr(ev, "title", None) or "").strip()
    )
    return (await build_short_vk_text(ev, source_text, max_sentences=2)).strip()


async def _publish_vk_repost(
    db: Database,
    bot: object | None,
    *,
    source_url: str,
    target_group_id: int,
    message: str,
) -> str | None:
    ids = _vk_owner_post_from_url(source_url)
    if not ids:
        return None
    source_owner_id, source_post_id = ids
    from main import VK_USER_TOKEN, _vk_api, choose_vk_actor

    actors: list[tuple[str, str]] = []
    if VK_USER_TOKEN:
        actors.append(("user", VK_USER_TOKEN))
    actors.extend(
        (actor.kind, actor.token)
        for actor in choose_vk_actor(-abs(int(target_group_id)), "wall.post")
        if actor.kind != "user"
    )
    if not actors:
        raise RuntimeError("VK token missing for wall.repost")
    params = {
        "object": f"wall{source_owner_id}_{source_post_id}",
        "group_id": int(target_group_id),
        "message": message,
    }
    for kind, token in actors:
        data = await _vk_api(
            "wall.repost",
            params,
            db,
            bot,
            token=token,
            token_kind=kind,
            skip_captcha=(kind == "group"),
        )
        post_id = (data.get("response") or {}).get("post_id") if isinstance(data, dict) else None
        if post_id:
            return _vk_wall_url_from_parts(-abs(int(target_group_id)), int(post_id))
    return None


async def _record_vk_promo_exposure(
    db: Database,
    *,
    campaign_id: int,
    activity_id: int,
    event_id: int,
    surface: str,
    placement_kind: str,
    status: str,
    url: str | None,
    published_at: datetime,
    details: dict[str, Any],
    target_type: str = "vk_wall",
    public: bool = True,
) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, 4):
        try:
            async with db.get_session() as session:
                session.add(
                    PromoExposure(
                        campaign_id=campaign_id,
                        activity_id=activity_id,
                        event_id=event_id,
                        surface=surface,
                        placement_kind=placement_kind,
                        publish_status=status,
                        public_target_count=1 if (public and url) else 0,
                        public_targets_json=[{"type": target_type, "url": url}] if (public and url) else [],
                        published_at=published_at,
                        details_json=details,
                    )
                )
                await session.commit()
                return
        except Exception as exc:
            last_exc = exc
            if "database is locked" not in str(exc).lower() or attempt >= 3:
                raise
            delay = 0.35 * attempt
            logger.warning(
                "promo.vk exposure sqlite locked surface=%s event_id=%s attempt=%s/3 retry_in=%.2fs",
                surface,
                event_id,
                attempt,
                delay,
            )
            await asyncio.sleep(delay)
    if last_exc is not None:
        raise last_exc


def _config_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "да"}:
        return True
    if text in {"0", "false", "no", "off", "нет"}:
        return False
    return default


def _extract_json_object(text: str) -> dict[str, Any] | None:
    cleaned = re.sub(r"^```(?:json)?|```$", "", str(text or "").strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
    candidates = [cleaned]
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if match:
        candidates.append(match.group(0))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _is_vk_short_url(url: str | None) -> bool:
    try:
        host = urlparse(str(url or "")).netloc.lower()
    except Exception:
        return False
    return host in {"vk.cc", "vk.link", "go.vk.com", "l.vk.com"}


def _display_vk_url(url: str | None) -> str:
    return re.sub(r"^https?://", "", str(url or "").strip(), flags=re.IGNORECASE)


async def _shorten_vk_url_for_display(
    url: str | None,
    *,
    db: Database,
    bot: object | None,
) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    if _is_vk_short_url(raw):
        return _display_vk_url(raw)
    try:
        from main import _vk_api

        data = await _vk_api("utils.getShortLink", {"url": raw}, db, bot)
        payload = data.get("response", data) if isinstance(data, dict) else {}
        if isinstance(payload, dict):
            short_url = str(payload.get("short_url") or "").strip()
            key = str(payload.get("key") or "").strip()
            if short_url:
                return _display_vk_url(short_url)
            if key:
                return f"vk.cc/{key}"
    except Exception:
        logger.warning("promo.vk carousel shortlink fallback url=%s", raw, exc_info=True)
    return raw


async def _event_vk_cta_display_url(
    ev: Event,
    *,
    db: Database,
    bot: object | None,
    configured_url: str | None = None,
) -> str:
    if configured_url:
        return await _shorten_vk_url_for_display(configured_url, db=db, bot=bot)
    if getattr(ev, "vk_ticket_short_url", None):
        return _display_vk_url(ev.vk_ticket_short_url)
    ticket_link = str(getattr(ev, "ticket_link", None) or "").strip()
    if ticket_link:
        try:
            from main import _vk_api
            from shortlinks import ensure_vk_short_ticket_link

            short_result = await ensure_vk_short_ticket_link(ev, db, bot=bot, vk_api_fn=_vk_api)
            if short_result and short_result[0]:
                return _display_vk_url(short_result[0])
        except Exception:
            logger.warning(
                "promo.vk carousel ticket shortlink failed event_id=%s",
                getattr(ev, "id", None),
                exc_info=True,
            )
        return _display_vk_url(ticket_link) if _is_vk_short_url(ticket_link) else ticket_link
    for attr in ("source_vk_post_url", "source_post_url", "telegraph_url"):
        value = str(getattr(ev, attr, None) or "").strip()
        if value:
            return await _shorten_vk_url_for_display(value, db=db, bot=bot)
    return ""


def _festival_carousel_program_phrase(campaign: PromoCampaign, cfg: dict[str, Any]) -> str:
    phrase = str(cfg.get("program_phrase") or "").strip()
    if phrase:
        return phrase
    program_name = str(cfg.get("program_name") or "").strip()
    festival_name = str(cfg.get("festival_name") or "").strip()
    if not festival_name:
        title = str(getattr(campaign, "title", "") or "")
        if "кантат" in title.casefold():
            festival_name = "Кантата"
    if program_name and festival_name:
        return f"{program_name} фестиваля «{festival_name}»"
    if program_name:
        return program_name
    if festival_name:
        return f"программа фестиваля «{festival_name}»"
    return "программа фестиваля"


def _sanitize_carousel_hook(text: str) -> str:
    text = re.sub(r"\s+", " ", str(text or "").strip())
    text = text.replace(" ?", "?").replace(" !", "!")
    return text[:140].strip()


def _fallback_festival_carousel_hook(variant: str, program_phrase: str) -> str:
    phrase = program_phrase.strip()
    variants = {
        "visited": f"Вы уже были или планируете пойти на {phrase}?",
        "registration": f"Вы уже записались на {phrase}?",
        "celebrity": f"Знаете, кто ведёт {phrase}?",
        "all_posters": f"Собрали события: {phrase}",
    }
    return variants.get(variant, variants["all_posters"])


def _vk_festival_carousel_configured_publish_date(
    cfg: dict[str, Any],
    *,
    now_utc: datetime,
) -> tuple[int | None, dict[str, Any]]:
    raw = (
        cfg.get("scheduled_at")
        or cfg.get("publish_at")
        or cfg.get("publish_datetime")
        or cfg.get("publish_date")
    )
    if raw in (None, ""):
        return None, {}
    source = "scheduled_at"
    publish_ts: int | None = None
    if isinstance(raw, (int, float)):
        publish_ts = int(raw)
        source = "unix"
    else:
        text = str(raw).strip()
        if text.isdigit():
            publish_ts = int(text)
            source = "unix"
        else:
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                publish_ts = int(dt.astimezone(timezone.utc).timestamp())
                source = "iso"
            except ValueError:
                logger.warning("promo.vk carousel configured publish date is invalid: %r", raw)
                return None, {"configured_publish_date_invalid": raw}
    if publish_ts is None:
        return None, {}
    if publish_ts <= int(now_utc.timestamp()) + 60:
        return None, {"configured_publish_date_ignored": raw, "reason": "not_future"}
    return publish_ts, {"configured_publish_date": raw, "configured_publish_date_source": source}


async def _festival_carousel_hook_text(
    *,
    campaign: PromoCampaign,
    cfg: dict[str, Any],
    variant: str,
    events: list[Event],
) -> tuple[str, str]:
    program_phrase = _festival_carousel_program_phrase(campaign, cfg)
    hook_texts = cfg.get("hook_texts")
    if isinstance(hook_texts, dict):
        configured = _sanitize_carousel_hook(str(hook_texts.get(variant) or ""))
        if configured:
            return configured, "config"
    configured = _sanitize_carousel_hook(str(cfg.get("hook_text") or ""))
    if configured:
        return configured, "config"
    fallback = _fallback_festival_carousel_hook(variant, program_phrase)
    if not _config_bool(cfg.get("llm_hook_enabled"), default=False):
        return fallback, "deterministic_fallback"
    try:
        import main as main_mod

        ask_4o = getattr(main_mod, "ask_4o", None)
        if ask_4o is None:
            return fallback, "fallback_no_ask_4o"
        event_brief = "\n".join(f"- {ev.title} ({ev.date} {ev.time or ''})" for ev in events[:12])
        prompt = (
            "Ты пишешь первый слайд VK-карусели для промо образовательной/фестивальной программы. "
            "Верни только JSON без markdown: {\"hook_text\":\"...\"}. "
            "Нужен один вопрос или короткий хук до 120 символов, естественный русский, без агрессии, без кликбейта, "
            "без обещаний, которых нет в данных. Можно учитывать название программы/фестиваля. "
            "Не добавляй ссылки, даты, эмодзи и служебные инструкции.\n"
            f"Тип хука: {variant!r}.\n"
            f"Программа: {program_phrase!r}.\n"
            f"Кампания: {campaign.title!r}.\n"
            f"События:\n{event_brief}"
        )
        raw = await ask_4o(
            prompt,
            max_tokens=100,
            temperature=0.2,
            meta={"feature": "promo", "stage": "vk_festival_carousel_hook", "campaign_id": campaign.id},
        )
        payload = _extract_json_object(str(raw or ""))
        hook = _sanitize_carousel_hook(str((payload or {}).get("hook_text") or ""))
        if hook and 20 <= len(hook) <= 140 and "{" not in hook:
            return hook, "llm_hook"
    except Exception:
        logger.warning("promo.vk carousel llm hook failed campaign_id=%s", getattr(campaign, "id", None), exc_info=True)
    return fallback, "fallback_llm_error"


def _load_carousel_font(size: int, *, bold: bool = False):
    from PIL import ImageFont

    try:
        from afishaengagement import _load_font

        return _load_font("Cygre-Bold.ttf" if bold else "Cygre-Medium.ttf", size)
    except Exception:
        pass
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def _wrap_draw_text(draw: Any, text: str, font: Any, max_width: int) -> list[str]:
    words = str(text or "").split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if current and (bbox[2] - bbox[0]) > max_width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return lines or [""]


VK_FESTIVAL_CAROUSEL_DEFAULT_PALETTES = [
    "ivory_navy_ochre",
    "cloud_plum_wasabi",
    "cobalt_clay_ivory",
    "smoky_jade_terracotta",
    "petrol_pearl",
    "sage_black_lilac",
    "butter_ink_cherry",
]


def _carousel_palette_candidates(cfg: dict[str, Any]) -> list[str]:
    raw = cfg.get("palette_ids")
    if isinstance(raw, list):
        candidates = [str(item or "").strip() for item in raw if str(item or "").strip()]
    else:
        candidates = []
    return candidates or VK_FESTIVAL_CAROUSEL_DEFAULT_PALETTES


def _carousel_palette_id(activity: PromoActivity, cfg: dict[str, Any], hook_variant: str) -> str:
    by_variant = cfg.get("palette_id_by_hook_variant")
    if isinstance(by_variant, dict):
        configured = str(by_variant.get(hook_variant) or "").strip()
        if configured:
            return configured
    configured = str(cfg.get("palette_id") or "").strip()
    if configured:
        return configured
    candidates = _carousel_palette_candidates(cfg)
    if not candidates:
        return "ivory_navy_ochre"
    if activity.id is not None:
        return candidates[int(activity.id) % len(candidates)]
    seed = f"{activity.profile_key or ''}:{hook_variant}:{activity.surface}"
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return candidates[int.from_bytes(digest[:2], "big") % len(candidates)]


def _event_celebrity_text(ev: Event) -> str:
    parts = [
        getattr(ev, "title", "") or "",
        getattr(ev, "short_description", "") or "",
        getattr(ev, "search_digest", "") or "",
        getattr(ev, "description", "") or "",
        getattr(ev, "source_text", "") or "",
    ]
    return "\n".join(str(part) for part in parts if str(part or "").strip())


def _event_has_explicit_celebrity_signal(ev: Event) -> bool:
    text = _event_celebrity_text(ev)
    if not text:
        return False
    lower = text.casefold()
    role_markers = (
        "блогер",
        "ведущ",
        "модератор",
        "спикер",
        "гость",
        "гостем",
        "дириж",
        "режисс",
        "продюсер",
        "артист",
        "актёр",
        "актер",
        "ректор",
        "художественный руководитель",
        "доктор искусствоведения",
        "лауреат",
        "творческая встреча с",
        "встреча с",
    )
    if not any(marker in lower for marker in role_markers):
        return False
    return bool(re.search(r"\b[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,2}\b", text))


def _filter_vk_festival_carousel_events_for_variant(
    events: list[Event],
    *,
    cfg: dict[str, Any],
    hook_variant: str,
) -> list[Event]:
    if hook_variant != "celebrity":
        return events
    raw_ids = cfg.get("celebrity_event_ids")
    explicit_ids: set[int] = set()
    if isinstance(raw_ids, list):
        for value in raw_ids:
            try:
                explicit_ids.add(int(value))
            except (TypeError, ValueError):
                continue
    if explicit_ids:
        return [ev for ev in events if ev.id is not None and int(ev.id) in explicit_ids]
    return [ev for ev in events if _event_has_explicit_celebrity_signal(ev)]


def _url_list_from_config(value: Any) -> list[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    return []


def _event_configured_poster_urls(
    ev: Event,
    *,
    cfg: dict[str, Any],
    hook_variant: str,
) -> list[str]:
    if ev.id is None:
        return []
    event_id = int(ev.id)
    config_keys = ["poster_urls_by_event_id", "photo_urls_by_event_id"]
    if hook_variant == "celebrity":
        config_keys = [
            "celebrity_poster_urls_by_event_id",
            "celebrity_photo_urls_by_event_id",
            *config_keys,
        ]
    for key in config_keys:
        raw = cfg.get(key)
        if not isinstance(raw, dict):
            continue
        urls = _url_list_from_config(raw.get(str(event_id)) or raw.get(event_id))
        if urls:
            return urls
    return []


@dataclass(frozen=True)
class _VkFestivalCarouselPersonCard:
    name: str
    role: str
    event_id: int | None = None


def _normalize_vk_festival_carousel_person_name(value: str) -> str:
    text = str(value or "").casefold().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _iter_vk_festival_carousel_person_card_payloads(raw: Any) -> Iterable[dict[str, Any]]:
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                yield item
    elif isinstance(raw, dict):
        for event_key, items in raw.items():
            event_id = _int_or_none(event_key)
            if isinstance(items, dict):
                payload = dict(items)
                payload.setdefault("event_id", event_id)
                yield payload
            elif isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        payload = dict(item)
                        payload.setdefault("event_id", event_id)
                        yield payload


def _vk_festival_carousel_person_cards_from_config(
    cfg: dict[str, Any],
) -> list[_VkFestivalCarouselPersonCard]:
    raw = cfg.get("celebrity_person_cards")
    if raw is None:
        raw = cfg.get("person_cards")
    cards: list[_VkFestivalCarouselPersonCard] = []
    for payload in _iter_vk_festival_carousel_person_card_payloads(raw):
        name = str(payload.get("name") or payload.get("title") or "").strip()
        role = str(
            payload.get("role")
            or payload.get("subtitle")
            or payload.get("description")
            or ""
        ).strip()
        if not name or not role:
            continue
        cards.append(
            _VkFestivalCarouselPersonCard(
                name=name,
                role=role,
                event_id=_int_or_none(payload.get("event_id")),
            )
        )
    return cards


def _covered_vk_festival_carousel_person_names(
    cfg: dict[str, Any],
    selected_events: list[Event],
) -> set[str]:
    covered: set[str] = set()
    for key in ("covered_celebrity_names", "celebrity_names_on_posters"):
        for name in _url_list_from_config(cfg.get(key)):
            normalized = _normalize_vk_festival_carousel_person_name(name)
            if normalized:
                covered.add(normalized)
    selected_event_ids = {int(ev.id) for ev in selected_events if ev.id is not None}
    by_event = (
        cfg.get("covered_celebrity_names_by_event_id")
        or cfg.get("celebrity_names_on_posters_by_event_id")
        or cfg.get("poster_celebrity_names_by_event_id")
    )
    if not isinstance(by_event, dict):
        return covered
    for event_id in selected_event_ids:
        for name in _url_list_from_config(by_event.get(str(event_id)) or by_event.get(event_id)):
            normalized = _normalize_vk_festival_carousel_person_name(name)
            if normalized:
                covered.add(normalized)
    return covered


def _select_vk_festival_carousel_person_cards(
    cfg: dict[str, Any],
    *,
    selected_events: list[Event],
    limit: int,
    candidate_cards: list[_VkFestivalCarouselPersonCard] | None = None,
) -> list[_VkFestivalCarouselPersonCard]:
    if limit <= 0:
        return []
    covered = _covered_vk_festival_carousel_person_names(cfg, selected_events)
    selected: list[_VkFestivalCarouselPersonCard] = []
    seen = set(covered)
    cards = candidate_cards if candidate_cards is not None else _vk_festival_carousel_person_cards_from_config(cfg)
    for card in cards:
        normalized = _normalize_vk_festival_carousel_person_name(card.name)
        if not normalized or normalized in seen:
            continue
        selected.append(card)
        seen.add(normalized)
        if len(selected) >= limit:
            break
    return selected


def _has_configured_vk_festival_carousel_person_cards(cfg: dict[str, Any]) -> bool:
    return cfg.get("celebrity_person_cards") is not None or cfg.get("person_cards") is not None


def _event_brief_for_celebrity_llm(ev: Event) -> str:
    parts = [
        f"id={getattr(ev, 'id', None)}",
        f"title={getattr(ev, 'title', '')!r}",
        f"date={getattr(ev, 'date', '')!r}",
        f"time={getattr(ev, 'time', '')!r}",
    ]
    source_text = re.sub(r"\s+", " ", str(getattr(ev, "source_text", "") or "").strip())
    if source_text:
        parts.append(f"source_text={source_text[:1800]!r}")
    else:
        description = re.sub(r"\s+", " ", str(getattr(ev, "description", "") or "").strip())
        if description:
            parts.append(f"description={description[:1400]!r}")
    short_description = re.sub(r"\s+", " ", str(getattr(ev, "short_description", "") or "").strip())
    if short_description:
        parts.append(f"short_description={short_description[:500]!r}")
    return "; ".join(parts)


async def _select_vk_festival_carousel_person_source_events(
    db: Database,
    *,
    campaign: PromoCampaign,
    cfg: dict[str, Any],
    fallback_events: list[Event],
) -> list[Event]:
    raw_ids = cfg.get("celebrity_person_source_event_ids") or cfg.get("person_source_event_ids")
    explicit_ids: list[int] = []
    if isinstance(raw_ids, list):
        for value in raw_ids:
            event_id = _int_or_none(value)
            if event_id is not None and event_id not in explicit_ids:
                explicit_ids.append(event_id)
    if not explicit_ids and _config_bool(cfg.get("celebrity_person_source_from_campaign_targets"), default=False):
        campaign_id = int(campaign.id or 0)
        async with db.get_session() as session:
            rows = (
                await session.execute(
                    select(PromoTarget.event_id)
                    .where(PromoTarget.campaign_id == campaign_id)
                    .where(PromoTarget.event_id.is_not(None))
                    .order_by(PromoTarget.id)
                )
            ).all()
        for (event_id,) in rows:
            parsed = _int_or_none(event_id)
            if parsed is not None and parsed not in explicit_ids:
                explicit_ids.append(parsed)
    if not explicit_ids:
        return fallback_events
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(Event).where(Event.id.in_(explicit_ids))
            )
        ).scalars().all()
    by_id = {int(ev.id): ev for ev in rows if ev.id is not None}
    return [by_id[event_id] for event_id in explicit_ids if event_id in by_id]


async def _llm_vk_festival_carousel_person_cards(
    *,
    campaign: PromoCampaign,
    cfg: dict[str, Any],
    source_events: list[Event],
    selected_events: list[Event],
    limit: int,
) -> tuple[list[_VkFestivalCarouselPersonCard], str]:
    if limit <= 0:
        return [], "no_slots"
    if not _config_bool(cfg.get("celebrity_person_cards_llm_enabled"), default=True):
        return [], "disabled"
    try:
        import main as main_mod

        ask_4o = getattr(main_mod, "ask_4o", None)
        if ask_4o is None:
            return [], "missing_ask_4o"
        program_phrase = _festival_carousel_program_phrase(campaign, cfg)
        covered_names = sorted(_covered_vk_festival_carousel_person_names(cfg, selected_events))
        selected_event_ids = [int(ev.id) for ev in selected_events if ev.id is not None]
        selected_event_id_set = set(selected_event_ids)
        source_events_with_ids = [ev for ev in source_events if ev.id is not None]
        events_without_posters = [
            ev for ev in source_events_with_ids if int(ev.id) not in selected_event_id_set
        ]
        events_with_posters = [
            ev for ev in source_events_with_ids if int(ev.id) in selected_event_id_set
        ]
        ordered_source_events = [*events_without_posters, *events_with_posters]
        events_without_poster_ids = [int(ev.id) for ev in events_without_posters]
        per_event = _config_bool(cfg.get("celebrity_person_cards_llm_per_event"), default=True)

        async def ask_cards(prompt: str, *, max_tokens: int, stage: str) -> list[_VkFestivalCarouselPersonCard]:
            raw = await ask_4o(
                prompt,
                max_tokens=max_tokens,
                temperature=0.1,
                meta={"feature": "promo", "stage": stage, "campaign_id": campaign.id},
            )
            payload = _extract_json_object(str(raw or ""))
            raw_cards = (payload or {}).get("cards")
            if not isinstance(raw_cards, list):
                return []
            return [
                _VkFestivalCarouselPersonCard(
                    name=card.name[:80].strip(),
                    role=card.role[:160].strip(),
                    event_id=card.event_id,
                )
                for card in _vk_festival_carousel_person_cards_from_config({"celebrity_person_cards": raw_cards})
                if card.name.strip() and card.role.strip()
            ]

        if per_event:
            cards: list[_VkFestivalCarouselPersonCard] = []
            for ev in ordered_source_events[:12]:
                selected_so_far = _select_vk_festival_carousel_person_cards(
                    cfg,
                    selected_events=selected_events,
                    limit=limit,
                    candidate_cards=cards,
                )
                remaining = limit - len(selected_so_far)
                if remaining <= 0:
                    break
                event_id = int(ev.id) if ev.id is not None else None
                event_without_poster = event_id is not None and event_id not in selected_event_id_set
                prompt = (
                    "Ты выбираешь карточки персон для VK-карусели промо образовательной/фестивальной программы. "
                    "Сейчас проверяется ОДНО событие. Верни только JSON без markdown: "
                    "{\"cards\":[{\"name\":\"...\",\"role\":\"...\",\"event_id\":123,\"evidence\":\"...\"}]}. "
                    f"Для этого события можно вернуть не больше {min(2, remaining)} cards; общий оставшийся бюджет {remaining}. "
                    "Если в title/source_text события назван гость, спикер, модератор или ведущий с ролью, верни карточку. "
                    "Если явного человека с ролью нет, верни {\"cards\":[]}. "
                    "Не добавляй названия фильмов, организаций, фестивалей, вымышленные роли или людей без явной роли. "
                    "Не повторяй людей, которые уже видны на афишах выбранных poster cards. "
                    "role должен быть коротким: кто это/почему важен для программы, до 120 символов. "
                    "evidence — короткая цитата/фрагмент из данных, по которому видно имя и роль.\n"
                    f"Программа: {program_phrase!r}.\n"
                    f"Кампания: {campaign.title!r}.\n"
                    f"Poster event ids: {selected_event_ids!r}.\n"
                    f"This event without poster card: {event_without_poster!r}.\n"
                    f"Names already covered on posters (normalized): {covered_names!r}.\n"
                    f"Event:\n- {_event_brief_for_celebrity_llm(ev)}"
                )
                cards.extend(
                    await ask_cards(
                        prompt,
                        max_tokens=260,
                        stage="vk_festival_carousel_person_cards_event",
                    )
                )
            selected_cards = _select_vk_festival_carousel_person_cards(
                cfg,
                selected_events=selected_events,
                limit=limit,
                candidate_cards=cards,
            )
            return selected_cards, "llm_per_event"

        event_brief = "\n".join(f"- {_event_brief_for_celebrity_llm(ev)}" for ev in ordered_source_events[:12])
        prompt = (
            "Ты выбираешь карточки персон для VK-карусели промо образовательной/фестивальной программы. "
            "Верни только JSON без markdown: {\"cards\":[{\"name\":\"...\",\"role\":\"...\",\"event_id\":123,\"evidence\":\"...\"}]}. "
            f"Нужно не больше {limit} cards. Это жёсткий лимит: не предлагай больше. "
            "Заполни список максимально полно в пределах лимита: если в данных есть 4-5 релевантных людей, верни 4-5, "
            "не останавливайся на первых трёх. Приоритет — гости, спикеры, модераторы и ведущие событий, "
            "особенно из событий, не представленных выбранными poster cards. "
            "Обязательная проверка: пройди по каждому event_id из Events without poster cards; "
            "если в title/source_text этого события назван гость, спикер, модератор или ведущий с ролью, включи его, пока не исчерпан лимит. "
            "События с poster cards используй после непостерных, чтобы не дублировать уже видимых людей. "
            "Выбирай только реальных людей, явно названных в предоставленных событиях, и только если рядом есть роль/статус. "
            "Не добавляй названия фильмов, организаций, фестивалей, вымышленные роли или людей без явной роли. "
            "Не повторяй людей, которые уже видны на афишах выбранных poster cards. "
            "role должен быть коротким: кто это/почему важен для программы, до 120 символов. "
            "evidence — короткая цитата/фрагмент из данных, по которому видно имя и роль.\n"
            f"Программа: {program_phrase!r}.\n"
            f"Кампания: {campaign.title!r}.\n"
            f"Poster event ids: {selected_event_ids!r}.\n"
            f"Events without poster cards: {events_without_poster_ids!r}.\n"
            f"Names already covered on posters (normalized): {covered_names!r}.\n"
            f"Events:\n{event_brief}"
        )
        cards = await ask_cards(
            prompt,
            max_tokens=700,
            stage="vk_festival_carousel_person_cards",
        )
        if not cards:
            return [], "llm_empty"
        return (
            _select_vk_festival_carousel_person_cards(
                cfg,
                selected_events=selected_events,
                limit=limit,
                candidate_cards=cards,
            ),
            "llm",
        )
    except Exception:
        logger.warning(
            "promo.vk carousel llm person cards failed campaign_id=%s",
            getattr(campaign, "id", None),
            exc_info=True,
        )
        return [], "llm_error"


def _render_vk_festival_carousel_card(
    title: str,
    *,
    subtitle: str = "",
    footer: str = "",
    variant: str = "hook",
    palette_id: str | None = None,
    badge_label: str = "",
) -> bytes:
    from PIL import Image, ImageDraw

    width, height = 1080, 1350

    def rgb(hex_value: str) -> tuple[int, int, int]:
        value = hex_value.lstrip("#")
        return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)

    try:
        from afishaengagement import (
            CTA_EDITORIAL_PALETTES,
            _apply_cta_grain,
            _compose_cta_edge,
            _draw_badge_on_image,
            _draw_down_arrow,
            _draw_right_arrow,
            _fit_badge_font,
            _hex_to_rgb,
            _mix_rgb,
            fit_text,
        )

        chosen_palette_id = palette_id or ("ivory_navy_ochre" if variant == "hook" else "ivory_charcoal_oxblood")
        roles = CTA_EDITORIAL_PALETTES.get(chosen_palette_id) or CTA_EDITORIAL_PALETTES["ivory_navy_ochre"]
        bg = _hex_to_rgb(roles["surface"])
        ink = _hex_to_rgb(roles["ink"])
        accent = _hex_to_rgb(roles["signal"])
        rim = _hex_to_rgb(roles.get("rim") or roles["ink"])
        image = Image.new("RGB", (width, height), bg)
        block_polygon = [(0, 0), (width, 0), (width, height), (0, height)]
        image = _apply_cta_grain(image, polygon=block_polygon, seed=f"vk_festival_carousel:{variant}:{title[:32]}")
        image = _compose_cta_edge(
            image,
            seam_start=(0, 28),
            seam_end=(width, 28),
            cta_normal=(0.0, 1.0),
            surface=bg,
            ink=ink,
            seam=accent,
            accent=accent,
            rim=rim,
            scale=1.0,
            include_accent_stripe=True,
        )
        draw = ImageDraw.Draw(image)
        if variant == "cta":
            label = badge_label or "ЗАПИСЬ"
            badge_font = _fit_badge_font(
                label,
                scale=1.0,
                max_width=width - 172,
                preferred_px=42,
                trailing_icon="right_arrow",
            )
            image, _, _ = _draw_badge_on_image(
                image,
                x=86,
                y=96,
                label=label,
                font=badge_font,
                bg=bg,
                fg=_hex_to_rgb(roles.get("signal_ink") or roles["surface"]),
                accent=accent,
                scale=1.0,
                max_width=width - 172,
                button=True,
                trailing_icon="right_arrow",
            )
            draw = ImageDraw.Draw(image)
        if variant != "cta":
            draw.rectangle((72, 112, 178, 126), fill=accent)
        title_fit = fit_text(
            title,
            box_width=width - 144,
            box_height=560,
            preferred_px=82 if len(title) < 85 else 68,
            min_px=46,
            max_lines=7,
            font_name="Cygre-Bold.ttf",
            avoid_orphan_lines=True,
        )
        if title_fit is None:
            raise ValueError("carousel_title_fit_failed")
        title_font = _load_carousel_font(title_fit.font_px, bold=True)
        y = max(210, (height - title_fit.height) // 2 - 110)
        for line in title_fit.lines:
            draw.text((72, y), line, fill=ink, font=title_font)
            bbox = draw.textbbox((72, y), line, font=title_font)
            y += (bbox[3] - bbox[1]) + int(title_fit.font_px * 0.18)
        if subtitle:
            subtitle_fit = fit_text(
                subtitle,
                box_width=width - 144,
                box_height=230,
                preferred_px=38,
                min_px=30,
                max_lines=4,
                font_name="Cygre-Medium.ttf",
            )
            if subtitle_fit is not None:
                subtitle_font = _load_carousel_font(subtitle_fit.font_px)
                y += 52
                for line in subtitle_fit.lines:
                    draw.text((72, y), line, fill=ink, font=subtitle_font)
                    bbox = draw.textbbox((72, y), line, font=subtitle_font)
                    y += (bbox[3] - bbox[1]) + int(subtitle_fit.font_px * 0.18)
        footer_text = footer or ("листай" if variant == "hook" else "Ссылки — в тексте поста")
        footer_font = _load_carousel_font(44 if variant == "hook" else 34, bold=True)
        footer_rule_y = height - 164
        if variant in {"hook", "person"}:
            draw.line((72, footer_rule_y, width - 72, footer_rule_y), fill=accent, width=5)
            label = footer_text or "листай"
            label_w = draw.textlength(label, font=footer_font)
            y0 = height - 120
            right_x = width - 78
            arrow_start = right_x - 92
            text_x = arrow_start - int(label_w) - 24
            draw.text((text_x, y0 - 24), label, fill=accent, font=footer_font)
            _draw_right_arrow(draw, arrow_start, right_x, y0, accent, width=14, head=28)
        else:
            arrow_x = width // 2
            arrow_gap = 150
            draw.line((72, footer_rule_y, arrow_x - arrow_gap, footer_rule_y), fill=accent, width=5)
            draw.line((arrow_x + arrow_gap, footer_rule_y, width - 72, footer_rule_y), fill=accent, width=5)
            if footer_text:
                draw.text((72, height - 136), footer_text, fill=ink, font=footer_font)
            arrow_top = height - 344
            _draw_down_arrow(draw, arrow_x, arrow_top, height - 132, accent, width=18, head=56)
        out = io.BytesIO()
        image.save(out, format="JPEG", quality=94, optimize=True)
        return out.getvalue()
    except Exception:
        palettes = {
            "hook": ("#17324D", "#F7F0DD", "#E0B65A"),
            "cta": ("#F4ECDB", "#1A1A1A", "#8B1A1A"),
        }
        bg_hex, ink_hex, accent_hex = palettes.get(variant, palettes["hook"])

    image = Image.new("RGB", (width, height), rgb(bg_hex))
    draw = ImageDraw.Draw(image)
    accent = rgb(accent_hex)
    ink = rgb(ink_hex)
    draw.rectangle((0, 0, width, 28), fill=accent)
    draw.rectangle((72, 112, 178, 126), fill=accent)
    title_font_size = 74 if len(title) < 85 else 62
    title_font = _load_carousel_font(title_font_size, bold=True)
    subtitle_font = _load_carousel_font(38)
    footer_font = _load_carousel_font(32, bold=True)
    max_width = width - 144
    title_lines = _wrap_draw_text(draw, title, title_font, max_width)
    while len(title_lines) > 7 and title_font_size > 46:
        title_font_size -= 4
        title_font = _load_carousel_font(title_font_size, bold=True)
        title_lines = _wrap_draw_text(draw, title, title_font, max_width)
    line_h = int(title_font_size * 1.18)
    block_h = line_h * len(title_lines)
    y = max(210, (height - block_h) // 2 - 80)
    for line in title_lines:
        draw.text((72, y), line, fill=ink, font=title_font)
        y += line_h
    if subtitle:
        y += 52
        for line in _wrap_draw_text(draw, subtitle, subtitle_font, max_width)[:4]:
            draw.text((72, y), line, fill=ink, font=subtitle_font)
            y += 54
    footer_text = footer or ("Листайте афиши" if variant == "hook" else "Ссылки — в тексте поста")
    footer_rule_y = height - 160
    if variant == "cta":
        arrow_x = width // 2
        arrow_gap = 150
        draw.line((72, footer_rule_y, arrow_x - arrow_gap, footer_rule_y), fill=accent, width=3)
        draw.line((arrow_x + arrow_gap, footer_rule_y, width - 72, footer_rule_y), fill=accent, width=3)
    else:
        draw.rectangle((72, height - 160, width - 72, height - 88), outline=accent, width=3)
    draw.text((104, height - 142), footer_text, fill=ink, font=footer_font)
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=94, optimize=True)
    return out.getvalue()


def _render_vk_festival_carousel_poster_card(
    source_image: bytes,
    *,
    palette_id: str,
    swipe_label: str = "листай",
    seed: str = "",
) -> bytes:
    from PIL import Image, ImageOps

    try:
        from afishaengagement import (
            CTA_EDITORIAL_PALETTES,
            _draw_badge_on_image,
            _fit_badge_font,
            _hex_to_rgb,
        )

        roles = CTA_EDITORIAL_PALETTES.get(palette_id) or CTA_EDITORIAL_PALETTES["ivory_navy_ochre"]
        bg = _hex_to_rgb(roles["surface"])
        accent = _hex_to_rgb(roles["signal"])
        signal_ink = _hex_to_rgb(roles.get("signal_ink") or roles["surface"])
        with Image.open(io.BytesIO(source_image)) as opened:
            poster = ImageOps.exif_transpose(opened).convert("RGB")
        max_side = 1600
        if max(poster.size) > max_side:
            scale_down = max_side / max(poster.size)
            poster = poster.resize(
                (max(1, int(poster.width * scale_down)), max(1, int(poster.height * scale_down))),
                Image.Resampling.LANCZOS,
            )
        canvas = poster.copy()
        card_w, card_h = canvas.size
        scale = max(0.65, min(1.25, card_w / 1080))
        max_badge_w = max(160, min(int(390 * scale), card_w - int(48 * scale)))
        badge_font = _fit_badge_font(
            swipe_label,
            scale=scale,
            max_width=max_badge_w,
            preferred_px=max(26, int(42 * scale)),
            trailing_icon="right_arrow",
        )
        badge_w_probe = max_badge_w
        badge_x = card_w - int(72 * scale) - badge_w_probe
        badge_y = card_h - int(174 * scale)
        canvas, _, _ = _draw_badge_on_image(
            canvas,
            x=max(int(24 * scale), badge_x),
            y=max(int(24 * scale), badge_y),
            label=swipe_label,
            font=badge_font,
            bg=bg,
            fg=signal_ink,
            accent=accent,
            scale=scale,
            max_width=badge_w_probe,
            button=True,
            trailing_icon="right_arrow",
        )
        out = io.BytesIO()
        canvas.save(out, format="JPEG", quality=94, optimize=True)
        return out.getvalue()
    except Exception:
        logger.warning("promo.vk carousel poster badge render failed seed=%s", seed, exc_info=True)
        with Image.open(io.BytesIO(source_image)) as opened:
            poster = ImageOps.exif_transpose(opened).convert("RGB")
            out = io.BytesIO()
            poster.save(out, format="JPEG", quality=94, optimize=True)
            return out.getvalue()


def _event_compact_label(ev: Event) -> str:
    title = re.sub(r"\s+", " ", str(ev.title or "").strip())
    when = " ".join(part for part in [str(ev.date or "").strip(), str(ev.time or "").strip()] if part)
    return f"{when} — {title}".strip(" —")


async def _build_vk_festival_carousel_message(
    *,
    db: Database,
    bot: object | None,
    campaign: PromoCampaign,
    cfg: dict[str, Any],
    hook_text: str,
    events: list[Event],
) -> tuple[str, dict[int, str]]:
    explicit_by_event = cfg.get("cta_urls_by_event_id") or cfg.get("links_by_event_id") or {}
    if not isinstance(explicit_by_event, dict):
        explicit_by_event = {}
    cta_by_event: dict[int, str] = {}
    for ev in events:
        if ev.id is None:
            continue
        configured = explicit_by_event.get(str(ev.id)) or explicit_by_event.get(int(ev.id))
        cta = await _event_vk_cta_display_url(ev, db=db, bot=bot, configured_url=configured)
        if cta:
            cta_by_event[int(ev.id)] = cta

    program_url = str(cfg.get("program_vk_url") or cfg.get("vk_program_url") or "").strip()
    if not program_url:
        program_url = str(cfg.get("program_url") or cfg.get("cta_url") or "").strip()
    program_display = await _shorten_vk_url_for_display(program_url, db=db, bot=bot) if program_url else ""

    lines = [
        hook_text,
        "",
        "Листайте карусель: собрали афиши событий и ссылки для записи.",
    ]
    if program_display:
        lines.extend(["", f"Вся программа: {program_display}"])
    event_lines = []
    for ev in events:
        if ev.id is None:
            continue
        cta = cta_by_event.get(int(ev.id), "")
        label = _event_compact_label(ev)
        event_lines.append(f"• {label}" + (f" — {cta}" if cta else ""))
    if event_lines:
        lines.extend(["", "Регистрация на события:", *event_lines])
    if _config_bool(cfg.get("debug_shadow"), default=False):
        marker = str(cfg.get("debug_marker") or "#vk_festival_carousel_shadow")
        lines.extend(["", "[VK FESTIVAL CAROUSEL DEBUG COPY — DELETE BEFORE PUBLISH]", marker])
    return "\n".join(lines).strip(), cta_by_event


async def _select_vk_festival_carousel_events(
    db: Database,
    *,
    campaign: PromoCampaign,
    activity: PromoActivity,
    target: PromoTarget,
    today: date,
    now_utc: datetime | None = None,
) -> list[Event]:
    now_utc = now_utc or datetime.now(timezone.utc)
    cfg = _activity_config(activity)
    raw_ids = cfg.get("carousel_event_ids") or cfg.get("event_ids")
    ordered_ids: list[int] = []
    if isinstance(raw_ids, list):
        for value in raw_ids:
            try:
                ordered_ids.append(int(value))
            except (TypeError, ValueError):
                continue
    if ordered_ids:
        async with db.get_session() as session:
            res = await session.execute(select(Event).where(Event.id.in_(ordered_ids)))
            by_id = {int(ev.id): ev for ev in res.scalars().all() if ev.id is not None}
        return [
            by_id[event_id]
            for event_id in ordered_ids
            if event_id in by_id and _event_is_promo_eligible(by_id[event_id], today=today, campaign=campaign)
            and event_has_not_started_for_promo(by_id[event_id], now_utc=now_utc)
        ]
    events = await _events_for_target(
        db,
        target=target,
        campaign=campaign,
        today=today,
        now_utc=now_utc,
    )
    preferred_ids = _preferred_event_ids_for_date(activity, today)
    if preferred_ids:
        by_id = {int(ev.id): ev for ev in events if ev.id is not None}
        preferred_set = set(preferred_ids)
        ordered = [by_id[event_id] for event_id in preferred_ids if event_id in by_id]
        ordered.extend(ev for ev in events if ev.id is not None and int(ev.id) not in preferred_set)
        events = ordered
    return events


async def _publish_vk_festival_carousel(
    db: Database,
    bot: object | None,
    *,
    campaign: PromoCampaign,
    activity: PromoActivity,
    target: PromoTarget,
    now_utc: datetime,
    today: date,
) -> PromoVkActionResult:
    from main import VK_PHOTOS_ENABLED, _vk_api, post_to_vk, upload_vk_photo, upload_vk_photo_bytes

    campaign_id = int(campaign.id or 0)
    activity_id = int(activity.id or 0)
    cfg = _activity_config(activity)
    target_group_id = await _resolve_vk_group_id(cfg.get("target_group") or activity.profile_key)
    if not target_group_id:
        return PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="target_group_missing")

    recent = await _recent_activity_exposures(
        db,
        campaign_id=campaign_id,
        activity_id=activity_id,
        surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
        since_utc=now_utc - timedelta(days=14),
        public_only=False,
    )
    target_goal = int(activity.target_exposure_goal or 1)
    counted_statuses = PUBLIC_PROMO_EXPOSURE_STATUSES | DEBUG_PROMO_EXPOSURE_STATUSES
    if len([row for row in recent if row.publish_status in counted_statuses]) >= target_goal:
        return PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="target_goal_reached")

    day_rows = await _recent_activity_exposures(
        db,
        campaign_id=campaign_id,
        activity_id=activity_id,
        surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
        since_utc=_promo_day_bounds(now_utc)[0],
        until_utc=_promo_day_bounds(now_utc)[1],
        public_only=False,
    )
    active_day_rows = [row for row in day_rows if row.publish_status in counted_statuses]
    if activity.daily_cap is not None and len(active_day_rows) >= int(activity.daily_cap):
        return PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="daily_cap_reached")

    events = await _select_vk_festival_carousel_events(
        db,
        campaign=campaign,
        activity=activity,
        target=target,
        today=today,
        now_utc=now_utc,
    )
    events = [ev for ev in events if ev.id is not None]
    if not events:
        return PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="events_missing")

    hook_variant = str(cfg.get("hook_variant") or "all_posters").strip() or "all_posters"
    person_source_events = events
    events = _filter_vk_festival_carousel_events_for_variant(
        events,
        cfg=cfg,
        hook_variant=hook_variant,
    )
    if not events:
        return PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="events_missing")
    hook_text, hook_source = await _festival_carousel_hook_text(
        campaign=campaign,
        cfg=cfg,
        variant=hook_variant,
        events=events,
    )

    max_cards_cap = 9 if hook_variant == "celebrity" else 10
    max_cards = max(2, min(max_cards_cap, int(cfg.get("max_cards") or max_cards_cap)))
    include_cta_card = _config_bool(cfg.get("include_cta_card"), default=True)
    max_event_cards = max(1, max_cards - 1 - (1 if include_cta_card else 0))
    palette_id = _carousel_palette_id(activity, cfg, hook_variant)
    poster_swipe_badge = _config_bool(cfg.get("poster_swipe_badge"), default=True)
    swipe_label = str(cfg.get("swipe_label") or "листай").strip() or "листай"
    celebrity_requires_configured_poster = (
        hook_variant == "celebrity"
        and _config_bool(cfg.get("celebrity_requires_image_evidence"), default=True)
    )
    selected_events: list[Event] = []
    event_attachments: list[str] = []
    selected_photo_urls: dict[int, str] = {}
    person_cards_added: list[_VkFestivalCarouselPersonCard] = []
    person_attachments: list[str] = []
    person_cards_source = "not_requested"
    if VK_PHOTOS_ENABLED:
        for ev in events:
            if len(selected_events) >= max_event_cards:
                break
            configured_photo_urls = _event_configured_poster_urls(ev, cfg=cfg, hook_variant=hook_variant)
            if celebrity_requires_configured_poster and not configured_photo_urls:
                continue
            photo_urls = configured_photo_urls or await _ensure_promo_vk_photo_urls(db, ev)
            first_photo = next((str(url or "").strip() for url in photo_urls if str(url or "").strip()), "")
            if not first_photo:
                continue
            attachment = None
            if poster_swipe_badge:
                try:
                    from afishaengagement import _default_fetch_image

                    source_image = await _default_fetch_image(first_photo)
                    poster_bytes = _render_vk_festival_carousel_poster_card(
                        source_image,
                        palette_id=palette_id,
                        swipe_label=swipe_label,
                        seed=f"{activity_id}:{getattr(ev, 'id', '')}",
                    )
                    attachment = await upload_vk_photo_bytes(
                        str(target_group_id),
                        poster_bytes,
                        db,
                        bot,
                        filename=f"vk_festival_carousel_{activity_id}_{ev.id}_poster.jpg",
                    )
                except Exception:
                    logger.warning(
                        "promo.vk carousel poster badge upload failed activity_id=%s event_id=%s",
                        activity_id,
                        getattr(ev, "id", None),
                        exc_info=True,
                    )
            if not attachment:
                attachment = await upload_vk_photo(str(target_group_id), first_photo, db, bot)
            if not attachment:
                continue
            selected_events.append(ev)
            event_attachments.append(attachment)
            selected_photo_urls[int(ev.id)] = first_photo
    if not selected_events:
        return PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "failed", reason="event_posters_missing")

    if VK_PHOTOS_ENABLED and hook_variant == "celebrity":
        person_slots = max_cards - 1 - len(event_attachments) - (1 if include_cta_card else 0)
        if _has_configured_vk_festival_carousel_person_cards(cfg):
            person_candidates = _select_vk_festival_carousel_person_cards(
                cfg,
                selected_events=selected_events,
                limit=max(0, person_slots),
            )
            person_cards_source = "config"
        else:
            person_source_events = await _select_vk_festival_carousel_person_source_events(
                db,
                campaign=campaign,
                cfg=cfg,
                fallback_events=person_source_events,
            )
            person_candidates, person_cards_source = await _llm_vk_festival_carousel_person_cards(
                campaign=campaign,
                cfg=cfg,
                source_events=person_source_events,
                selected_events=selected_events,
                limit=max(0, person_slots),
            )
        for index, card in enumerate(person_candidates, start=1):
            if len(person_attachments) >= person_slots:
                break
            try:
                person_bytes = _render_vk_festival_carousel_card(
                    card.name,
                    subtitle=card.role,
                    footer=swipe_label,
                    variant="person",
                    palette_id=palette_id,
                )
                attachment = await upload_vk_photo_bytes(
                    str(target_group_id),
                    person_bytes,
                    db,
                    bot,
                    filename=f"vk_festival_carousel_{activity_id}_person_{index}.jpg",
                )
            except Exception:
                logger.warning(
                    "promo.vk carousel person card upload failed activity_id=%s name=%s",
                    activity_id,
                    card.name,
                    exc_info=True,
                )
                continue
            if not attachment:
                continue
            person_cards_added.append(card)
            person_attachments.append(attachment)

    hook_subtitle = str(cfg.get("hook_subtitle") or "Бесплатные события, лекции, встречи и кинопоказы").strip()
    hook_bytes = _render_vk_festival_carousel_card(
        hook_text,
        subtitle=hook_subtitle,
        footer=swipe_label,
        variant="hook",
        palette_id=palette_id,
    )
    hook_attachment = await upload_vk_photo_bytes(
        str(target_group_id),
        hook_bytes,
        db,
        bot,
        filename=f"vk_festival_carousel_{activity_id}_hook.jpg",
    )
    if not hook_attachment:
        return PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "failed", reason="hook_upload_failed")
    attachments = [hook_attachment, *event_attachments, *person_attachments]
    cta_card_added = False
    if include_cta_card and len(attachments) < max_cards:
        cta_bytes = _render_vk_festival_carousel_card(
            str(cfg.get("cta_card_title") or "Выберите событие и записывайтесь"),
            subtitle=str(cfg.get("cta_card_subtitle") or "Ссылки на регистрацию — в тексте поста"),
            footer="Ссылки ниже",
            variant="cta",
            palette_id=palette_id,
            badge_label=str(cfg.get("cta_badge_label") or "ЗАПИСЬ"),
        )
        cta_attachment = await upload_vk_photo_bytes(
            str(target_group_id),
            cta_bytes,
            db,
            bot,
            filename=f"vk_festival_carousel_{activity_id}_cta.jpg",
        )
        if cta_attachment:
            attachments.append(cta_attachment)
            cta_card_added = True

    message, cta_by_event = await _build_vk_festival_carousel_message(
        db=db,
        bot=bot,
        campaign=campaign,
        cfg=cfg,
        hook_text=hook_text,
        events=selected_events,
    )

    debug_shadow = _config_bool(cfg.get("debug_shadow"), default=False)
    publish_date: int | None = None
    schedule_meta: dict[str, Any] = {}
    looks_like_collision = None
    if debug_shadow:
        try:
            from afishaengagement import _looks_like_vk_schedule_collision, _next_shadow_schedule

            looks_like_collision = _looks_like_vk_schedule_collision
            publish_date, schedule_meta = await _next_shadow_schedule(
                config=cfg,
                now_utc=now_utc,
                owner_id=-abs(int(target_group_id)),
                vk_api_fn=_vk_api,
                db=db,
                bot=bot,
            )
        except Exception:
            logger.warning("promo.vk carousel shadow schedule lookup failed", exc_info=True)
            delay_days = max(2, int(cfg.get("debug_publish_delay_days") or 3))
            publish_date = int((now_utc + timedelta(days=delay_days)).replace(second=0, microsecond=0).timestamp())
            schedule_meta = {"fallback": True, "selected_ts": publish_date}
    else:
        publish_date, schedule_meta = _vk_festival_carousel_configured_publish_date(cfg, now_utc=now_utc)

    vk_url = None
    attempts: list[int | None] = []
    spacing = int(schedule_meta.get("slot_spacing_seconds") or max(1, int(cfg.get("debug_slot_spacing_minutes") or 5)) * 60)
    max_attempts = 6 if debug_shadow else 1
    for attempt in range(max_attempts):
        attempts.append(publish_date)
        try:
            vk_url = await post_to_vk(
                str(target_group_id),
                message,
                db,
                bot,
                attachments,
                carousel=True,
                publish_date=publish_date,
            )
            break
        except Exception as exc:
            if not debug_shadow or looks_like_collision is None or not looks_like_collision(exc) or attempt >= max_attempts - 1:
                raise
            publish_date = int(publish_date or 0) + spacing
    if not vk_url:
        raise RuntimeError("wall.post returned no URL")

    scheduled_at = datetime.fromtimestamp(int(publish_date), timezone.utc) if publish_date else now_utc
    status = "VK_SCHEDULED_DEBUG" if debug_shadow else "VK_SCHEDULED"
    placement_kind = "vk_shadow_debug_carousel" if debug_shadow else "vk_festival_carousel"
    event_ids = [int(ev.id) for ev in selected_events if ev.id is not None]
    await _record_vk_promo_exposure(
        db,
        campaign_id=campaign_id,
        activity_id=activity_id,
        event_id=event_ids[0],
        surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
        placement_kind=placement_kind,
        status=status,
        url=vk_url,
        published_at=scheduled_at,
        details={
            "target_group_id": target_group_id,
            "target_url": vk_url,
            "event_ids": event_ids,
            "hook_variant": hook_variant,
            "hook_text": hook_text,
            "hook_source": hook_source,
            "palette_id": palette_id,
            "poster_swipe_badge": poster_swipe_badge,
            "swipe_label": swipe_label,
            "celebrity_requires_image_evidence": celebrity_requires_configured_poster,
            "person_cards": [
                {"name": card.name, "role": card.role, "event_id": card.event_id}
                for card in person_cards_added
            ],
            "person_cards_source": person_cards_source,
            "selected_photo_urls_by_event_id": {str(k): v for k, v in selected_photo_urls.items()},
            "cta_by_event_id": {str(k): v for k, v in cta_by_event.items()},
            "attachments_count": len(attachments),
            "max_cards": max_cards,
            "include_cta_card": cta_card_added,
            "debug_shadow": debug_shadow,
            "scheduled_ts": publish_date,
            "publish_attempts": attempts,
            "schedule": schedule_meta,
        },
        target_type="vk_wall_debug" if debug_shadow else "vk_wall",
    )
    return PromoVkActionResult(
        campaign_id,
        activity_id,
        activity.surface,
        event_ids[0],
        "scheduled_debug" if debug_shadow else "scheduled",
        target_url=vk_url,
    )


def _first_event_photo_url(ev: Event) -> str | None:
    for url in list(getattr(ev, "photo_urls", None) or []):
        text = str(url or "").strip()
        if text:
            return text
    return None


async def _source_wall_photo_url(source_url: str | None) -> str | None:
    ids = _vk_owner_post_from_url(source_url)
    if not ids:
        return None
    owner_id, post_id = ids
    from main import vk_api

    response = await vk_api("wall.getById", posts=f"{owner_id}_{post_id}")
    items = response.get("response") if isinstance(response, dict) else response
    if not isinstance(items, list):
        items = [items] if items else []
    if not items:
        return None
    for attachment in items[0].get("attachments") or []:
        if not isinstance(attachment, dict) or attachment.get("type") != "photo":
            continue
        sizes = (attachment.get("photo") or {}).get("sizes") or []
        candidates = [item for item in sizes if isinstance(item, dict) and item.get("url")]
        if not candidates:
            continue
        best = max(
            candidates,
            key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0),
        )
        return str(best.get("url") or "").strip() or None
    return None


async def _download_story_source_image(url: str) -> bytes:
    from main import HTTP_SEMAPHORE, MAX_DOWNLOAD_SIZE, get_http_session, span

    session = get_http_session()
    async with span("http"):
        async with HTTP_SEMAPHORE:
            async with session.get(url) as resp:
                resp.raise_for_status()
                buf = bytearray()
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    buf.extend(chunk)
                    if len(buf) > MAX_DOWNLOAD_SIZE:
                        raise ValueError("file too large")
                return bytes(buf)


def _promo_story_font(size: int):
    from PIL import ImageFont

    for path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
    ):
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _wrap_story_text(text: str, *, width: int) -> list[str]:
    lines: list[str] = []
    for paragraph in " ".join(str(text or "").split()).split("\n"):
        lines.extend(textwrap.wrap(paragraph, width=width) or [""])
    return lines


def _story_event_date_line(ev: Event) -> str:
    months = (
        "января",
        "февраля",
        "марта",
        "апреля",
        "мая",
        "июня",
        "июля",
        "августа",
        "сентября",
        "октября",
        "ноября",
        "декабря",
    )
    raw = str(getattr(ev, "date", "") or "").split("..", 1)[0].strip()
    try:
        day = date.fromisoformat(raw)
        value = f"{day.day} {months[day.month - 1]}"
    except ValueError:
        value = raw
    ev_time = str(getattr(ev, "time", "") or "").strip()
    if ev_time:
        value = f"{value} в {ev_time}" if value else ev_time
    return value


async def _build_vk_story_image_bytes(ev: Event, *, source_url: str) -> bytes:
    photo_url = await _source_wall_photo_url(source_url)
    if not photo_url:
        photo_url = _first_event_photo_url(ev)
    if not photo_url:
        raise RuntimeError("event has no photo for VK story")
    return await _download_story_source_image(photo_url)


async def _publish_vk_story_photo(
    db: Database,
    bot: object | None,
    *,
    target_group_id: int,
    image_bytes: bytes,
    source_url: str | None = None,
    link_text: str | None = None,
    include_source_link: bool = True,
) -> dict[str, Any]:
    from aiohttp import FormData

    from main import HTTP_SEMAPHORE, VK_USER_TOKEN, _vk_api, choose_vk_actor, get_http_session, span

    actors = [
        actor
        for actor in choose_vk_actor(-abs(int(target_group_id)), "stories.getPhotoUploadServer")
        if actor.kind == "user"
    ]
    if not actors and VK_USER_TOKEN:
        from types import SimpleNamespace

        actors = [SimpleNamespace(kind="user", token=None, label="user")]
    if not actors:
        raise RuntimeError("VK user token missing for community story")

    params: dict[str, Any] = {
        "group_id": abs(int(target_group_id)),
        "add_to_news": 1,
    }
    if include_source_link and source_url and str(source_url).startswith("https://vk.com/"):
        params["link_url"] = source_url
        params["link_text"] = link_text or "Подробнее"

    last_error: Exception | None = None
    for actor in actors:
        token = getattr(actor, "token", None) or VK_USER_TOKEN
        try:
            data = await _vk_api(
                "stories.getPhotoUploadServer",
                params,
                db,
                bot,
                token=token,
                token_kind="user",
            )
            upload_url = ((data.get("response") or {}).get("upload_url") if isinstance(data, dict) else None)
            if not upload_url:
                raise RuntimeError("stories.getPhotoUploadServer returned no upload_url")
            form = FormData()
            form.add_field("file", image_bytes, filename="story.jpg", content_type="image/jpeg")
            session = get_http_session()

            async def _upload() -> dict[str, Any]:
                async with span("http"):
                    async with HTTP_SEMAPHORE:
                        async with session.post(upload_url, data=form) as up:
                            return await up.json()

            upload_result_data = await _upload()
            upload_result = None
            if isinstance(upload_result_data, dict):
                response = upload_result_data.get("response")
                if isinstance(response, dict):
                    upload_result = response.get("upload_result")
                upload_result = upload_result or upload_result_data.get("upload_result")
            if not upload_result:
                raise RuntimeError("VK story upload returned no upload_result")
            saved = await _vk_api(
                "stories.save",
                {"upload_results": upload_result, "extended": 1},
                db,
                bot,
                token=token,
                token_kind="user",
            )
            response = saved.get("response") if isinstance(saved, dict) else None
            items = response.get("items") if isinstance(response, dict) else None
            count = int(response.get("count") or 0) if isinstance(response, dict) else 0
            if count < 1 and not items:
                raise RuntimeError("stories.save returned no saved story")
            item = items[0] if isinstance(items, list) and items else {}
            owner_id = int(item.get("owner_id") or -abs(int(target_group_id)))
            story_id = int(item.get("id") or 0)
            url = f"https://vk.com/story{owner_id}_{story_id}" if story_id else f"vk:story:{owner_id}"
            return {
                "url": url,
                "owner_id": owner_id,
                "story_id": story_id,
                "expires_at": item.get("expires_at"),
                "raw_count": count,
            }
        except Exception as exc:
            last_error = exc
            logger.warning(
                "promo.vk story actor failed group_id=%s actor=%s error=%s",
                target_group_id,
                getattr(actor, "label", "user"),
                exc,
            )
    raise last_error or RuntimeError("VK story publish failed")


async def _recent_campaign_source_wall_candidates(
    db: Database,
    *,
    campaign_id: int,
    events: list[Event],
    source_group_id: int,
    since_utc: datetime,
    until_utc: datetime,
) -> list[tuple[Event, str, datetime]]:
    source_candidates: list[tuple[Event, str, datetime]] = await _recent_event_vk_posts(
        events,
        group_id=source_group_id,
        since_utc=since_utc,
        until_utc=until_utc,
        db=db,
    )
    publication_exposures = await _recent_activity_exposures(
        db,
        campaign_id=campaign_id,
        activity_id=None,
        surface=PROMO_SURFACE_VK_PUBLICATION,
        since_utc=since_utc,
    )
    event_by_id = {int(ev.id): ev for ev in events if ev.id is not None}
    recent_wall: list[dict] = []
    if publication_exposures:
        try:
            from main import vk_wall_since

            recent_wall = await vk_wall_since(
                source_group_id,
                int(since_utc.timestamp()),
                owner_type="group",
                count=80,
            )
        except Exception:
            logger.warning(
                "promo.vk source candidates: vk_wall_since failed group_id=%s",
                source_group_id,
                exc_info=True,
            )
            recent_wall = []
    for exposure in publication_exposures:
        ev = event_by_id.get(int(exposure.event_id))
        if ev is None:
            continue
        details = exposure.details_json if isinstance(exposure.details_json, dict) else {}
        url = str(details.get("target_url") or (exposure.public_targets_json or [{}])[0].get("url") or "").strip()
        source_at: datetime | None = None
        if url and _vk_url_matches_group(url, source_group_id):
            try:
                source_at = await _vk_post_datetime(url)
            except Exception:
                source_at = None
        if source_at is None:
            match = _match_published_post_for_event(recent_wall, ev)
            if match is not None:
                live_url = str(match.get("url") or "").strip()
                ts = match.get("date")
                live_at = (
                    datetime.fromtimestamp(int(ts), timezone.utc)
                    if isinstance(ts, int)
                    else None
                )
                if live_url and live_at is not None and _vk_url_matches_group(live_url, source_group_id):
                    url, source_at = live_url, live_at
                    if exposure.id is not None:
                        try:
                            await _reconcile_exposure_target_url(
                                db,
                                exposure_id=int(exposure.id),
                                url=live_url,
                                published_at=live_at,
                            )
                        except Exception:
                            logger.warning(
                                "promo.vk source candidates: exposure url reconcile failed id=%s",
                                exposure.id,
                                exc_info=True,
                            )
        if source_at is not None and source_at <= until_utc and _vk_url_matches_group(url, source_group_id):
            source_candidates.append((ev, url, source_at))
    return source_candidates


async def run_promo_vk_activities(
    db: Database,
    bot: object | None = None,
    *,
    now_utc: datetime | None = None,
) -> list[PromoVkActionResult]:
    """Run due VK promo activities.

    ``vk_publication`` counts existing Smart Update posts in the target
    community during the rolling window, then schedules source-style VK posts
    for the deficit. ``vk_repost`` then reposts one recent source-community
    festival post to the configured target community with a short rewrite-only
    caption.
    """

    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()
    await ensure_initial_80_stories_campaign(db, now_utc=now_utc)
    results: list[PromoVkActionResult] = []

    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(PromoCampaign, PromoActivity, PromoTarget)
                    .join(PromoActivity, PromoActivity.campaign_id == PromoCampaign.id)
                    .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
                    .where(PromoCampaign.status == "active")
                    .where(PromoCampaign.starts_at <= now_utc)
                    .where(or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc))
                    .where(PromoActivity.enabled.is_(True))
                    .where(
                        PromoActivity.surface.in_(
                            [
                                PROMO_SURFACE_VK_PUBLICATION,
                                PROMO_SURFACE_TG_EVENT_PUBLISH,
                                PROMO_SURFACE_TG_REPOST,
                                PROMO_SURFACE_VK_CHANNEL_PUBLISH,
                                PROMO_SURFACE_VK_REPOST,
                                PROMO_SURFACE_VK_STORY,
                                PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
                            ]
                        )
                    )
                    .order_by(PromoCampaign.priority, PromoCampaign.created_at, PromoActivity.id)
                )
            ).all()
        )

    processed_carousels: set[tuple[int, int]] = set()
    for campaign, activity, target in rows:
        if campaign.id is None or activity.id is None:
            continue
        campaign_id = int(campaign.id)
        activity_id = int(activity.id)
        if activity.surface == PROMO_SURFACE_VK_FESTIVAL_CAROUSEL:
            key = (campaign_id, activity_id)
            if key in processed_carousels:
                continue
            processed_carousels.add(key)
            due_count = _vk_activity_due_count(activity, now_utc)
            if due_count <= 0:
                continue
            try:
                result = await _publish_vk_festival_carousel(
                    db,
                    bot,
                    campaign=campaign,
                    activity=activity,
                    target=target,
                    now_utc=now_utc,
                    today=today,
                )
                if result.status not in {"skipped"}:
                    results.append(result)
            except Exception as exc:
                logger.exception(
                    "promo.vk festival carousel failed campaign_id=%s activity_id=%s",
                    campaign_id,
                    activity_id,
                )
                results.append(
                    PromoVkActionResult(
                        campaign_id,
                        activity_id,
                        activity.surface,
                        0,
                        "failed",
                        reason=str(exc) or type(exc).__name__,
                    )
                )
            continue
        window_hours = _vk_activity_window(activity)
        since_utc = now_utc - timedelta(hours=window_hours)
        events = await _events_for_target(
            db,
            target=target,
            campaign=campaign,
            today=today,
            now_utc=now_utc,
        )
        events = [ev for ev in events if ev.id is not None]
        if not events:
            continue

        if activity.surface == PROMO_SURFACE_VK_PUBLICATION:
            cfg = _activity_config(activity)
            target_group_id = await _resolve_vk_group_id(cfg.get("target_group") or activity.profile_key)
            if not target_group_id:
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="target_group_missing")
                )
                continue
            due_count = _vk_activity_due_count(activity, now_utc)
            if due_count <= 0:
                continue
            organic = await _recent_event_vk_posts(
                events,
                group_id=target_group_id,
                since_utc=since_utc,
                until_utc=now_utc,
                db=db,
            )
            recent_exposures = await _activity_day_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_PUBLICATION,
                now_utc=now_utc,
            )
            recent_audit_exposures = await _recent_activity_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_PUBLICATION,
                since_utc=since_utc,
                public_only=False,
            )
            recent_failed_no_media_event_ids = {
                int(exposure.event_id)
                for exposure in recent_audit_exposures
                if str(exposure.publish_status or "") == "FAILED_NO_MEDIA"
            }
            needed = max(0, due_count - len(organic) - len(recent_exposures))
            needed = min(needed, 1)
            if needed <= 0:
                continue
            recent_event_ids = {int(ev.id) for ev, _url, _dt in organic if ev.id is not None}
            for exposure in recent_exposures:
                recent_event_ids.add(int(exposure.event_id))
            preferred_ids = _preferred_event_ids_for_date(activity, today)
            preferred_id_set = set(preferred_ids or [])
            stats = await _load_public_exposure_stats(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                event_ids=[int(ev.id) for ev in events],
            )

            def sort_key(ev: Event) -> tuple[int, int, str, str, datetime, str, int]:
                event_id = int(ev.id)
                count, last_at = stats.get(event_id, (0, None))
                last_key = last_at or datetime.min.replace(tzinfo=timezone.utc)
                return (
                    _preferred_event_rank(activity, today, event_id),
                    count,
                    str(ev.date or ""),
                    str(getattr(ev, "time", "") or ""),
                    last_key,
                    _stable_shuffle_key(campaign_id, activity_id, today.isoformat(), ev.id),
                    event_id,
                )

            candidates: list[Event] = []
            for ev in sorted(events, key=sort_key):
                if int(ev.id) in recent_event_ids:
                    continue
                if preferred_ids is not None and int(ev.id) not in preferred_id_set:
                    continue
                if _promo_vk_publication_missing_required_media(ev):
                    recovered_urls = await _ensure_promo_vk_photo_urls(db, ev)
                    if recovered_urls:
                        candidates.append(ev)
                        continue
                    logger.warning(
                        "promo.vk publication candidate missing media campaign_id=%s activity_id=%s event_id=%s "
                        "source_post_url=%s photo_urls_count=0 reason=%s",
                        campaign_id,
                        activity_id,
                        getattr(ev, "id", None),
                        getattr(ev, "source_post_url", None),
                        VK_SYNC_MISSING_TG_MEDIA_ERROR,
                    )
                    if int(ev.id) not in recent_failed_no_media_event_ids:
                        await _record_vk_promo_exposure(
                            db,
                            campaign_id=campaign_id,
                            activity_id=activity_id,
                            event_id=int(ev.id),
                            surface=PROMO_SURFACE_VK_PUBLICATION,
                            placement_kind="rolling_window_deficit",
                            status="FAILED_NO_MEDIA",
                            url=None,
                            published_at=now_utc,
                            details={
                                "target_group_id": target_group_id,
                                "source_post_url": getattr(ev, "source_post_url", None),
                                "photo_urls_count": 0,
                                "attachments_count": 0,
                                "reason": VK_SYNC_MISSING_TG_MEDIA_ERROR,
                                "action": "investigate_source_media_and_rehydrate_before_publication",
                            },
                        )
                        recent_failed_no_media_event_ids.add(int(ev.id))
                    results.append(
                        PromoVkActionResult(
                            campaign_id,
                            activity_id,
                            activity.surface,
                            int(ev.id),
                            "failed",
                            reason=VK_SYNC_MISSING_TG_MEDIA_ERROR,
                        )
                    )
                    continue
                candidates.append(ev)
            for ev in candidates[:needed]:
                try:
                    url = await _build_promo_vk_source_post(
                        db,
                        bot,
                        ev,
                        campaign_id=campaign_id,
                        activity_id=activity_id,
                        target_group_id=target_group_id,
                    )
                    if not url:
                        raise RuntimeError("wall.post returned no URL")
                    try:
                        vk_post_date = await _vk_post_datetime(url)
                    except Exception:
                        vk_post_date = None
                    await _record_vk_promo_exposure(
                        db,
                        campaign_id=campaign_id,
                        activity_id=activity_id,
                        event_id=int(ev.id),
                        surface=PROMO_SURFACE_VK_PUBLICATION,
                        placement_kind="rolling_window_deficit",
                        status="VK_SCHEDULED",
                        url=url,
                        published_at=now_utc,
                        details={
                            "target_group_id": target_group_id,
                            "target_url": url,
                            "vk_post_date": vk_post_date.isoformat() if vk_post_date else None,
                            "window_hours": window_hours,
                            "organic_count": len(organic),
                        },
                    )
                    results.append(
                        PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "scheduled", target_url=url)
                    )
                except Exception as exc:
                    logger.exception("promo.vk publication failed campaign_id=%s activity_id=%s event_id=%s", campaign_id, activity_id, ev.id)
                    results.append(
                        PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "failed", reason=str(exc) or type(exc).__name__)
                    )

        elif activity.surface == PROMO_SURFACE_TG_EVENT_PUBLISH:
            cfg = _activity_config(activity)
            target_chat = str(cfg.get("target_chat") or activity.profile_key or "").strip()
            if not target_chat:
                results.append(
                    PromoVkActionResult(
                        campaign_id,
                        activity_id,
                        activity.surface,
                        0,
                        "skipped",
                        reason="target_chat_missing",
                    )
                )
                continue
            if bot is None:
                results.append(
                    PromoVkActionResult(
                        campaign_id,
                        activity_id,
                        activity.surface,
                        0,
                        "skipped",
                        reason="bot_missing",
                    )
                )
                continue
            due_count = _vk_activity_due_count(activity, now_utc)
            if due_count <= 0:
                continue
            organic = await _recent_event_tg_organic_posts(
                db,
                events=events,
                target_chat=target_chat,
                since_utc=since_utc,
                until_utc=now_utc,
            )
            recent_exposures = await _activity_day_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
                now_utc=now_utc,
            )
            satisfied_event_ids = {int(ev.id) for ev, _url, _dt in organic if ev.id is not None}
            satisfied_event_ids.update(int(exposure.event_id) for exposure in recent_exposures)
            if len(satisfied_event_ids) >= due_count:
                continue
            recent_event_ids = satisfied_event_ids
            preferred_ids = _preferred_event_ids_for_date(activity, today)
            preferred_id_set = set(preferred_ids or [])
            stats = await _load_public_exposure_stats(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                event_ids=[int(ev.id) for ev in events],
            )

            def sort_key(ev: Event) -> tuple[int, int, datetime, str, str, str, int]:
                event_id = int(ev.id)
                count, last_at = stats.get(event_id, (0, None))
                last_key = last_at or datetime.min.replace(tzinfo=timezone.utc)
                date_key = str(ev.date or "")
                time_key = str(getattr(ev, "time", "") or "")
                return (
                    _preferred_event_rank(activity, today, event_id),
                    count,
                    last_key,
                    date_key,
                    time_key,
                    _stable_shuffle_key(campaign_id, activity_id, today.isoformat(), ev.id),
                    event_id,
                )

            candidates = [
                ev
                for ev in sorted(events, key=sort_key)
                if int(ev.id) not in recent_event_ids
                and (preferred_ids is None or int(ev.id) in preferred_id_set)
            ]
            for ev in candidates[:1]:
                source_url = _tg_source_url_for_chat(ev, target_chat)
                if source_url:
                    try:
                        url = await _publish_tg_repost(
                            bot,
                            source_chat=target_chat,
                            target_chat=target_chat,
                            source_url=source_url,
                        )
                        if not url:
                            raise RuntimeError("telegram self-forward returned no URL")
                        await _record_vk_promo_exposure(
                            db,
                            campaign_id=campaign_id,
                            activity_id=activity_id,
                            event_id=int(ev.id),
                            surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
                            placement_kind="rolling_window_self_forward",
                            status="TG_FORWARDED",
                            url=url,
                            published_at=now_utc,
                            details={
                                "target_chat": target_chat,
                                "source_chat": target_chat,
                                "source_url": source_url,
                                "target_url": url,
                                "window_hours": window_hours,
                                "organic_count": len(organic),
                                "mode": "self_forward_existing_event_post",
                            },
                            target_type="telegram_forward",
                        )
                        results.append(
                            PromoVkActionResult(
                                campaign_id,
                                activity_id,
                                activity.surface,
                                int(ev.id),
                                "forwarded",
                                source_url=source_url,
                                target_url=url,
                            )
                        )
                        continue
                    except Exception:
                        logger.warning(
                            "promo.tg self-forward failed; fallback to full publish campaign_id=%s activity_id=%s event_id=%s source_url=%s",
                            campaign_id,
                            activity_id,
                            ev.id,
                            source_url,
                            exc_info=True,
                        )
                try:
                    from main import publish_tg_promo_event_publication

                    if not callable(publish_tg_promo_event_publication):
                        raise RuntimeError("telegram event publish function unavailable")
                    url = await publish_tg_promo_event_publication(
                        ev,
                        db,
                        bot,
                        target_chat=target_chat,
                    )
                    if not url:
                        raise RuntimeError("telegram event publish returned no URL")
                    await _record_vk_promo_exposure(
                        db,
                        campaign_id=campaign_id,
                        activity_id=activity_id,
                        event_id=int(ev.id),
                        surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
                        placement_kind="rolling_window_deficit",
                        status="TG_PUBLISHED",
                        url=url,
                        published_at=now_utc,
                        details={
                            "target_chat": target_chat,
                            "target_url": url,
                            "window_hours": window_hours,
                            "organic_count": len(organic),
                        },
                        target_type="telegram_channel",
                    )
                    async with db.get_session() as session:
                        obj = await session.get(Event, int(ev.id))
                        if obj is not None:
                            obj.tg_event_post_url = url
                            post_id = _tg_message_id_from_url(url)
                            if post_id is not None:
                                obj.tg_event_post_id = post_id
                            obj.tg_event_post_mode = "promo"
                            session.add(obj)
                            await session.commit()
                    results.append(
                        PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "published", target_url=url)
                    )
                except Exception as exc:
                    logger.exception("promo.tg publication failed campaign_id=%s activity_id=%s event_id=%s", campaign_id, activity_id, ev.id)
                    results.append(
                        PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "failed", reason=str(exc) or type(exc).__name__)
                    )

        elif activity.surface == PROMO_SURFACE_TG_REPOST:
            cfg = _activity_config(activity)
            source_chat = str(cfg.get("source_chat") or "").strip()
            target_chat = str(cfg.get("target_chat") or activity.profile_key or "").strip()
            if not source_chat or not target_chat:
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="source_or_target_chat_missing")
                )
                continue
            if bot is None:
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="bot_missing")
                )
                continue
            due_count = _vk_activity_due_count(activity, now_utc)
            if due_count <= 0:
                continue
            recent_reposts = await _activity_day_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_TG_REPOST,
                now_utc=now_utc,
            )
            if len(recent_reposts) >= due_count:
                continue
            source_candidates = await _recent_event_tg_posts(
                db,
                campaign_id=campaign_id,
                events=events,
                source_chat=source_chat,
                since_utc=since_utc,
                until_utc=now_utc,
                min_lead_hours=_activity_min_lead_hours(activity),
            )
            dedup_since = now_utc - timedelta(hours=int(cfg.get("dedup_hours") or PROMO_VK_REPOST_DEDUP_HOURS))
            prior = await _recent_activity_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_TG_REPOST,
                since_utc=dedup_since,
            )
            forwarded_source_urls = {
                str((exposure.details_json or {}).get("source_url") or "").strip()
                for exposure in prior
                if isinstance(exposure.details_json, dict)
            }
            repeat_cooldown_raw = cfg.get("repeat_cooldown_hours")
            try:
                repeat_cooldown_hours = int(repeat_cooldown_raw)
            except (TypeError, ValueError):
                try:
                    repeat_cooldown_hours = int(cfg.get("repeat_cooldown_days", 7)) * 24
                except (TypeError, ValueError):
                    repeat_cooldown_hours = 7 * 24
            recent_repeat_keys: set[str] = set()
            if repeat_cooldown_hours > 0:
                recent_repeat_keys = await _recent_activity_event_repeat_keys(
                    db,
                    campaign_id=campaign_id,
                    activity_id=activity_id,
                    surface=PROMO_SURFACE_TG_REPOST,
                    since_utc=now_utc - timedelta(hours=repeat_cooldown_hours),
                    until_utc=now_utc,
                )
                repeat_prior = await _recent_activity_exposures(
                    db,
                    campaign_id=campaign_id,
                    activity_id=activity_id,
                    surface=PROMO_SURFACE_TG_REPOST,
                    since_utc=now_utc - timedelta(hours=repeat_cooldown_hours),
                )
                repeat_source_urls = {
                    str((exposure.details_json or {}).get("source_url") or "").strip()
                    for exposure in repeat_prior
                    if isinstance(exposure.details_json, dict)
                }
                recent_repeat_keys.update(
                    key
                    for key in (
                        _promo_repeat_key(candidate[0])
                        for candidate in source_candidates
                        if candidate[1] in repeat_source_urls
                    )
                    if key
                )
            picked: tuple[Event, str, datetime] | None = None
            popularity_scores: dict[int, PromoPopularityScore] = {}
            selection_policy = str(cfg.get("selection_policy") or activity.selection_policy or "").strip()
            if selection_policy == PROMO_POLICY_WEIGHTED_POPULARITY:
                popularity_scores = await _weighted_popularity_scores_for_events(
                    db,
                    event_ids=[int(candidate[0].id) for candidate in source_candidates if candidate[0].id is not None],
                    activity=activity,
                    now_utc=now_utc,
                )

                def weighted_key(item: tuple[Event, str, datetime]) -> tuple[float, float, datetime, int]:
                    ev = item[0]
                    event_id = int(ev.id or 0)
                    score = popularity_scores.get(event_id)
                    return (
                        float(score.score if score else 0.0),
                        float(score.owned_vk_score if score else 0.0),
                        item[2],
                        -event_id,
                    )

                ranked_candidates = [
                    item for item in sorted(source_candidates, key=weighted_key, reverse=True)
                ]
            else:
                ranked_candidates = sorted(source_candidates, key=lambda item: (item[2], int(item[0].id or 0)))
            eligible_ranked_candidates = [
                candidate for candidate in ranked_candidates if candidate[1] not in forwarded_source_urls
            ]
            for candidate in ranked_candidates:
                repeat_key = _promo_repeat_key(candidate[0])
                if candidate[1] not in forwarded_source_urls and repeat_key not in recent_repeat_keys:
                    picked = candidate
                    break
            if picked is None:
                # Diversity is preferred, but if the campaign genuinely has no
                # other forwardable candidate, allow a repeat rather than
                # dropping the slot completely.
                picked = eligible_ranked_candidates[0] if eligible_ranked_candidates else None
            if picked is None:
                continue
            ev, source_url, source_at = picked
            selected_popularity = popularity_scores.get(int(ev.id or 0)) if popularity_scores else None
            repeat_key = _promo_repeat_key(ev)
            try:
                url = await _publish_tg_repost(
                    bot,
                    source_chat=source_chat,
                    target_chat=target_chat,
                    source_url=source_url,
                )
                if not url:
                    raise RuntimeError("telegram forward returned no URL")
                await _record_vk_promo_exposure(
                    db,
                    campaign_id=campaign_id,
                    activity_id=activity_id,
                    event_id=int(ev.id),
                    surface=PROMO_SURFACE_TG_REPOST,
                    placement_kind="rolling_window_repost",
                    status="TG_FORWARDED",
                    url=url,
                    published_at=now_utc,
                    details={
                        "source_chat": source_chat,
                        "target_chat": target_chat,
                        "source_url": source_url,
                        "source_published_at": source_at.isoformat(),
                        "target_url": url,
                        "selection_policy": selection_policy or None,
                        "popularity_score": selected_popularity.score if selected_popularity else None,
                        "source_popularity_score": selected_popularity.source_score if selected_popularity else None,
                        "owned_vk_popularity_score": selected_popularity.owned_vk_score if selected_popularity else None,
                        "source_popularity_count": selected_popularity.source_count if selected_popularity else None,
                        "owned_vk_popularity_count": selected_popularity.owned_vk_count if selected_popularity else None,
                        "repeat_key": repeat_key or None,
                        "repeat_cooldown_hours": repeat_cooldown_hours,
                        "repeat_cooldown_bypassed": repeat_key in recent_repeat_keys,
                    },
                    target_type="telegram_forward",
                )
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "published", source_url=source_url, target_url=url)
                )
            except Exception as exc:
                logger.exception("promo.tg repost failed campaign_id=%s activity_id=%s event_id=%s", campaign_id, activity_id, ev.id)
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "failed", source_url=source_url, reason=str(exc) or type(exc).__name__)
                )

        elif activity.surface == PROMO_SURFACE_VK_CHANNEL_PUBLISH:
            cfg = _activity_config(activity)
            delivery_mode = str(cfg.get("delivery_mode") or "").strip()
            if delivery_mode != "vk_messages_manual_copy_draft":
                logger.warning(
                    "promo.vk channel publish skipped: unsupported VK community Channel API "
                    "campaign_id=%s activity_id=%s target_group=%s target_channel=%s",
                    campaign_id,
                    activity_id,
                    cfg.get("target_group") or activity.profile_key,
                    cfg.get("target_channel") or activity.profile_key,
                )
                results.append(
                    PromoVkActionResult(
                        campaign_id,
                        activity_id,
                        activity.surface,
                        0,
                        "skipped",
                        reason="vk_community_channel_post_api_unsupported",
                    )
                )
                continue
            target_group_id = await _resolve_vk_group_id(cfg.get("target_group") or activity.profile_key)
            peer_ids = _vk_channel_peer_ids(cfg)
            if not target_group_id:
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="target_group_missing")
                )
                continue
            if not peer_ids:
                logger.info(
                    "promo.vk channel manual draft skipped: peer id missing campaign_id=%s activity_id=%s",
                    campaign_id,
                    activity_id,
                )
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="manual_draft_peer_id_missing")
                )
                continue
            due_count = _vk_activity_due_count(activity, now_utc)
            if due_count <= 0:
                continue
            counted_statuses = PUBLIC_PROMO_EXPOSURE_STATUSES | NONPUBLIC_PROMO_DELIVERY_STATUSES
            recent_exposures = await _activity_day_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_CHANNEL_PUBLISH,
                now_utc=now_utc,
                statuses=counted_statuses,
            )
            if len(recent_exposures) >= due_count:
                continue
            recent_event_ids = {int(exposure.event_id) for exposure in recent_exposures}
            preferred_ids = _preferred_event_ids_for_date(activity, today)
            preferred_id_set = set(preferred_ids or [])
            stats = await _load_public_exposure_stats(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                event_ids=[int(ev.id) for ev in events],
                statuses=counted_statuses,
            )

            def sort_key(ev: Event) -> tuple[int, int, datetime, str, str, int]:
                event_id = int(ev.id)
                count, last_at = stats.get(event_id, (0, None))
                last_key = last_at or datetime.min.replace(tzinfo=timezone.utc)
                return (
                    _preferred_event_rank(activity, today, event_id),
                    count,
                    last_key,
                    _stable_shuffle_key(campaign_id, activity_id, today.isoformat(), ev.id),
                    str(ev.date or ""),
                    event_id,
                )

            candidates = [
                ev
                for ev in sorted(events, key=sort_key)
                if int(ev.id) not in recent_event_ids
                and (preferred_ids is None or int(ev.id) in preferred_id_set)
            ]
            for ev in candidates:
                try:
                    from main import (
                        VkChannelManualDraftMissingRegistrationLink,
                        publish_vk_channel_promo_event_publication,
                    )

                    url = await publish_vk_channel_promo_event_publication(
                        ev,
                        db,
                        bot,
                        target_group_id=target_group_id,
                        peer_ids=peer_ids,
                        channel_ref=str(cfg.get("target_channel") or activity.profile_key or ""),
                    )
                    if not url:
                        raise RuntimeError("VK channel manual draft returned no URL")
                    details = {
                        "target_group_id": target_group_id,
                        "target_channel": cfg.get("target_channel") or activity.profile_key,
                        "target_url": url,
                        "window_hours": window_hours,
                        "peer_count": len(peer_ids),
                        "api_contract": cfg.get("api_contract"),
                        "delivery_mode": delivery_mode,
                        "manual_copy_draft": True,
                    }
                    await _record_vk_promo_exposure(
                        db,
                        campaign_id=campaign_id,
                        activity_id=activity_id,
                        event_id=int(ev.id),
                        surface=PROMO_SURFACE_VK_CHANNEL_PUBLISH,
                        placement_kind="manual_copy_channel_draft",
                        status="VK_CHANNEL_DRAFT_SENT",
                        url=url,
                        published_at=now_utc,
                        details=details,
                        target_type="vk_manual_draft",
                        public=False,
                    )
                    results.append(
                        PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "draft_sent", target_url=url)
                    )
                    break
                except VkChannelManualDraftMissingRegistrationLink as exc:
                    logger.warning(
                        "promo.vk channel manual draft skipped missing direct CTA campaign_id=%s activity_id=%s event_id=%s",
                        campaign_id,
                        activity_id,
                        ev.id,
                    )
                    results.append(
                        PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "failed", reason=str(exc) or type(exc).__name__)
                    )
                    continue
                except Exception as exc:
                    logger.exception("promo.vk channel manual draft failed campaign_id=%s activity_id=%s event_id=%s", campaign_id, activity_id, ev.id)
                    results.append(
                        PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "failed", reason=str(exc) or type(exc).__name__)
                    )
                    break

        elif activity.surface == PROMO_SURFACE_VK_REPOST:
            cfg = _activity_config(activity)
            source_group_id = await _resolve_vk_group_id(cfg.get("source_group"))
            target_group_id = await _resolve_vk_group_id(cfg.get("target_group") or activity.profile_key)
            if not source_group_id or not target_group_id:
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="source_or_target_group_missing")
                )
                continue
            due_count = _vk_activity_due_count(activity, now_utc)
            if due_count <= 0:
                continue
            recent_reposts = await _activity_day_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_REPOST,
                now_utc=now_utc,
            )
            if len(recent_reposts) >= due_count:
                continue
            source_candidates: list[tuple[Event, str, datetime]] = await _recent_event_vk_posts(
                events,
                group_id=source_group_id,
                since_utc=since_utc,
                until_utc=now_utc,
                db=db,
            )
            min_lead_hours = _activity_min_lead_hours(activity)
            source_candidates = [
                item
                for item in source_candidates
                if event_is_repostable_for_promo(
                    item[0],
                    now_utc=now_utc,
                    min_lead_hours=min_lead_hours,
                )
            ]
            publication_exposures = await _recent_activity_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=None,
                surface=PROMO_SURFACE_VK_PUBLICATION,
                since_utc=since_utc,
            )
            event_by_id = {int(ev.id): ev for ev in events if ev.id is not None}
            # Recent source-community wall, used to reconcile promo publications
            # to their live published URL (post_to_vk returns the postponed-draft
            # id, which VK reassigns when the postponed post actually publishes).
            recent_wall: list[dict] = []
            if publication_exposures:
                try:
                    from main import vk_wall_since

                    recent_wall = await vk_wall_since(
                        source_group_id,
                        int(since_utc.timestamp()),
                        owner_type="group",
                        count=80,
                    )
                except Exception:
                    logger.warning(
                        "promo.vk repost: vk_wall_since failed group_id=%s",
                        source_group_id,
                        exc_info=True,
                    )
                    recent_wall = []
            for exposure in publication_exposures:
                ev = event_by_id.get(int(exposure.event_id))
                if ev is None:
                    continue
                if not event_is_repostable_for_promo(
                    ev,
                    now_utc=now_utc,
                    min_lead_hours=min_lead_hours,
                ):
                    continue
                details = exposure.details_json if isinstance(exposure.details_json, dict) else {}
                url = str(details.get("target_url") or (exposure.public_targets_json or [{}])[0].get("url") or "").strip()
                source_at: datetime | None = None
                if url and _vk_url_matches_group(url, source_group_id):
                    try:
                        source_at = await _vk_post_datetime(url)
                    except Exception:
                        source_at = None
                if source_at is None:
                    # Stored postponed-draft id no longer resolves once published
                    # under a new id; find the live post on the source wall.
                    match = _match_published_post_for_event(recent_wall, ev)
                    if match is not None:
                        live_url = str(match.get("url") or "").strip()
                        ts = match.get("date")
                        live_at = (
                            datetime.fromtimestamp(int(ts), timezone.utc)
                            if isinstance(ts, int)
                            else None
                        )
                        if live_url and live_at is not None and _vk_url_matches_group(live_url, source_group_id):
                            url, source_at = live_url, live_at
                            if exposure.id is not None:
                                try:
                                    await _reconcile_exposure_target_url(
                                        db,
                                        exposure_id=int(exposure.id),
                                        url=live_url,
                                        published_at=live_at,
                                    )
                                except Exception:
                                    logger.warning(
                                        "promo.vk repost: exposure url reconcile failed id=%s",
                                        exposure.id,
                                        exc_info=True,
                                    )
                if source_at is not None and source_at <= now_utc and _vk_url_matches_group(url, source_group_id):
                    source_candidates.append((ev, url, source_at))

            dedup_since = now_utc - timedelta(hours=int(cfg.get("dedup_hours") or PROMO_VK_REPOST_DEDUP_HOURS))
            prior = await _recent_activity_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_REPOST,
                since_utc=dedup_since,
            )
            reposted_source_urls = {
                str((exposure.details_json or {}).get("source_url") or "").strip()
                for exposure in prior
                if isinstance(exposure.details_json, dict)
            }
            picked: tuple[Event, str, datetime] | None = None
            for candidate in sorted(source_candidates, key=lambda item: (item[2], int(item[0].id or 0))):
                if candidate[1] not in reposted_source_urls:
                    picked = candidate
                    break
            if picked is None:
                continue
            ev, source_url, source_at = picked
            try:
                caption = await _build_promo_vk_repost_caption(ev)
                url = await _publish_vk_repost(
                    db,
                    bot,
                    source_url=source_url,
                    target_group_id=target_group_id,
                    message=caption,
                )
                if not url:
                    raise RuntimeError("wall.repost returned no URL")
                await _record_vk_promo_exposure(
                    db,
                    campaign_id=campaign_id,
                    activity_id=activity_id,
                    event_id=int(ev.id),
                    surface=PROMO_SURFACE_VK_REPOST,
                    placement_kind="rolling_window_repost",
                    status="PUBLISHED_MAIN",
                    url=url,
                    published_at=now_utc,
                    details={
                        "source_group_id": source_group_id,
                        "target_group_id": target_group_id,
                        "source_url": source_url,
                        "source_published_at": source_at.isoformat(),
                        "target_url": url,
                    },
                )
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "published", source_url=source_url, target_url=url)
                )
            except Exception as exc:
                logger.exception("promo.vk repost failed campaign_id=%s activity_id=%s event_id=%s", campaign_id, activity_id, ev.id)
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "failed", source_url=source_url, reason=str(exc) or type(exc).__name__)
                )
        elif activity.surface == PROMO_SURFACE_VK_STORY:
            cfg = _activity_config(activity)
            source_group_id = await _resolve_vk_group_id(cfg.get("source_group"))
            target_group_id = await _resolve_vk_group_id(cfg.get("target_group") or activity.profile_key)
            if not source_group_id or not target_group_id:
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, 0, "skipped", reason="source_or_target_group_missing")
                )
                continue
            due_count = _vk_activity_due_count(activity, now_utc)
            if due_count <= 0:
                continue
            recent_stories = await _activity_day_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_STORY,
                now_utc=now_utc,
            )
            if len(recent_stories) >= due_count:
                continue
            source_candidates = await _recent_campaign_source_wall_candidates(
                db,
                campaign_id=campaign_id,
                events=events,
                source_group_id=source_group_id,
                since_utc=since_utc,
                until_utc=now_utc,
            )
            dedup_since = now_utc - timedelta(hours=int(cfg.get("dedup_hours") or PROMO_VK_REPOST_DEDUP_HOURS))
            prior = await _recent_activity_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_STORY,
                since_utc=dedup_since,
            )
            used_source_urls = {
                str((exposure.details_json or {}).get("source_url") or "").strip()
                for exposure in prior
                if isinstance(exposure.details_json, dict)
            }
            picked: tuple[Event, str, datetime] | None = None
            for candidate in sorted(source_candidates, key=lambda item: (item[2], int(item[0].id or 0))):
                if candidate[1] not in used_source_urls and _first_event_photo_url(candidate[0]):
                    picked = candidate
                    break
            if picked is None:
                continue
            ev, source_url, source_at = picked
            try:
                image_bytes = await _build_vk_story_image_bytes(ev, source_url=source_url)
                story = await _publish_vk_story_photo(
                    db,
                    bot,
                    target_group_id=target_group_id,
                    image_bytes=image_bytes,
                    source_url=source_url,
                    link_text=str(cfg.get("link_text") or "Подробнее"),
                    include_source_link=bool(cfg.get("include_source_link") or False),
                )
                url = str(story.get("url") or "").strip()
                if not url:
                    raise RuntimeError("stories.save returned no story URL")
                await _record_vk_promo_exposure(
                    db,
                    campaign_id=campaign_id,
                    activity_id=activity_id,
                    event_id=int(ev.id),
                    surface=PROMO_SURFACE_VK_STORY,
                    placement_kind="rolling_window_story",
                    status="PUBLISHED_MAIN",
                    url=url,
                    published_at=now_utc,
                    details={
                        "source_group_id": source_group_id,
                        "target_group_id": target_group_id,
                        "source_url": source_url,
                        "source_published_at": source_at.isoformat(),
                        "target_url": url,
                        "owner_id": story.get("owner_id"),
                        "story_id": story.get("story_id"),
                        "expires_at": story.get("expires_at"),
                    },
                    target_type="vk_story",
                )
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "published", source_url=source_url, target_url=url)
                )
            except Exception as exc:
                logger.exception("promo.vk story failed campaign_id=%s activity_id=%s event_id=%s", campaign_id, activity_id, ev.id)
                results.append(
                    PromoVkActionResult(campaign_id, activity_id, activity.surface, int(ev.id), "failed", source_url=source_url, reason=str(exc) or type(exc).__name__)
                )
    return results
