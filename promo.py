from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select

from db import Database
from models import (
    Event,
    EventSource,
    Festival,
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
PROMO_DAILY_TZ = "Europe/Kaliningrad"
PROMO_SURFACE_VIDEO_GENERAL = "video_general"
PROMO_SURFACE_VK_PUBLICATION = "vk_publication"
PROMO_SURFACE_VK_REPOST = "vk_repost"
PROMO_VK_DEFAULT_WINDOW_HOURS = 24
PROMO_VK_REPOST_DEDUP_HOURS = 72
PROMO_VK_ACTIVE_START_HOUR = 9
PROMO_VK_ACTIVE_END_HOUR = 21
PROMO_VK_80_PUBLICATION_PROFILE = "klgdevents"
PROMO_VK_80_REPOST_PROFILE = "klgdevents->kenigeventsofficial"

# Target that matches events by their Telegram source chat + post author.
# query_text holds "<chat_username>:<author_username>" (both lowercased, no @).
PROMO_TARGET_TYPE_TG_CHAT_AUTHOR = "tg_chat_author"
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
    today = now_utc.date()
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
) -> PromoCreateResult:
    """Create an event-targeted partner promo campaign from a confirmed FSM spec.

    The caller (FSM step 6) is responsible for authorization. This function
    only validates business rules: event must exist, be future and active,
    ``ends_at`` is clamped to the event end date, count is positive.
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

    async with db.get_session() as session:
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

    async with db.get_session() as session:
        session.add(campaign)
        await session.commit()
        await session.refresh(campaign)
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
        await session.commit()
        await session.refresh(campaign)

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
    count: int


async def add_partner_activity_to_campaign(
    db: Database,
    spec: PartnerActivitySpec,
    *,
    actor_user_id: int,
    now_utc: datetime | None = None,
) -> PromoCreateResult:
    """Append a new PromoActivity to an existing partner campaign.

    Authorization: caller must verify the user owns the campaign or is
    superadmin; this function only enforces business rules (campaign
    exists, not archived, surface/slot_policy known, count positive).
    """

    now_utc = now_utc or datetime.now(timezone.utc)
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

    async with db.get_session() as session:
        campaign = await session.get(PromoCampaign, int(spec.campaign_id))
        if campaign is None:
            return PromoCreateResult(None, "not_found", "Кампания не найдена.")
        if campaign.status == "archived":
            return PromoCreateResult(
                None,
                "invalid",
                "Кампания в архиве — нельзя добавить активность. Восстановите её сначала.",
            )

        if spec.surface == PROMO_SURFACE_VIDEO_GENERAL:
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
        await session.commit()
        await session.refresh(campaign)

    return PromoCreateResult(
        campaign,
        "created",
        f"Добавил активность к #{campaign.id}: {spec.surface}"
        + (f"/{spec.profile_key}" if spec.profile_key else "")
        + f" · {spec.count} показ(ов).",
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
            for required in (
                _initial_80_vk_publication_activity(int(existing.id)),
                _initial_80_vk_repost_activity(int(existing.id)),
            ):
                key = (str(required.surface or ""), str(required.profile_key or ""))
                if key not in existing_keys:
                    session.add(required)
                    changed = True
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
                _initial_80_vk_repost_activity(int(campaign.id)),
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
) -> list[Event]:
    async with db.get_session() as session:
        if target.target_type == "event" and target.event_id:
            ev = await session.get(Event, int(target.event_id))
            if not ev or not _event_is_promo_eligible(
                ev,
                today=today,
                campaign=campaign,
                enforce_event_date_lte_campaign=False,
            ):
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
        ]


async def _load_public_exposure_stats(
    db: Database,
    *,
    campaign_id: int,
    activity_id: int,
    event_ids: Iterable[int],
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
            .where(PromoExposure.publish_status.in_(("PUBLISHED_MAIN", "PUBLISHED_TEST", "VK_SCHEDULED")))
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
            .where(PromoExposure.publish_status.in_(("PUBLISHED_MAIN", "PUBLISHED_TEST", "VK_SCHEDULED")))
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
    today = now_utc.date()
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

    result: list[PromoCandidate] = []
    used_event_ids: set[int] = set()
    global_budget = VIDEO_PROMO_GLOBAL_MAX_PER_PUBLISH
    for campaign, activity, target in rows:
        if campaign.id is None or activity.id is None:
            continue
        if len(result) >= global_budget:
            break
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
        max_per_publish = min(max_per_publish, global_budget - len(result))
        events = await _events_for_target(
            db,
            target=target,
            campaign=campaign,
            today=today,
        )
        events = [
            ev
            for ev in events
            if ev.id is not None
            and int(ev.id) not in used_event_ids
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
        for ev in picked:
            if ev.id is None:
                continue
            used_event_ids.add(int(ev.id))
            result.append(
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
    return result


async def resolve_surface_promo_event_ids(
    db: Database,
    *,
    surface: str,
    now_utc: datetime | None = None,
    profile_key: str | None = None,
) -> set[int]:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()
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
        events = await _events_for_target(db, target=target, campaign=campaign, today=today)
        max_per_publish = max(1, min(int(activity.max_per_publish or 1), 2))
        for ev in events[:max_per_publish]:
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
        res = await session.execute(query.order_by(PromoExposure.published_at.desc()))
        return list(res.scalars().all())


async def _build_promo_vk_source_post(
    db: Database,
    bot: object | None,
    ev: Event,
    *,
    target_group_id: int,
) -> str | None:
    from main import (
        VK_MAX_ATTACHMENTS,
        VK_PHOTOS_ENABLED,
        build_vk_source_message,
        post_to_vk,
        upload_vk_photo,
    )

    festival = None
    if ev.festival:
        async with db.get_session() as session:
            res = await session.execute(select(Festival).where(Festival.name == ev.festival))
            festival = res.scalars().first()
    text_for_vk = (getattr(ev, "description", None) or "").strip() or (ev.source_text or "")
    message = build_vk_source_message(ev, text_for_vk, festival=festival)
    attachments: list[str] = []
    if VK_PHOTOS_ENABLED:
        for photo_url in list(getattr(ev, "photo_urls", None) or [])[:VK_MAX_ATTACHMENTS]:
            photo_id = await upload_vk_photo(str(target_group_id), photo_url, db, bot)
            if photo_id:
                attachments.append(photo_id)
    return await post_to_vk(
        str(target_group_id),
        message,
        db,
        bot,
        attachments or None,
    )


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
    url: str,
    published_at: datetime,
    details: dict[str, Any],
) -> None:
    async with db.get_session() as session:
        session.add(
            PromoExposure(
                campaign_id=campaign_id,
                activity_id=activity_id,
                event_id=event_id,
                surface=surface,
                placement_kind=placement_kind,
                publish_status=status,
                public_target_count=1,
                public_targets_json=[{"type": "vk_wall", "url": url}],
                published_at=published_at,
                details_json=details,
            )
        )
        await session.commit()


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
                    .where(PromoActivity.surface.in_([PROMO_SURFACE_VK_PUBLICATION, PROMO_SURFACE_VK_REPOST]))
                    .order_by(PromoCampaign.priority, PromoCampaign.created_at, PromoActivity.id)
                )
            ).all()
        )

    for campaign, activity, target in rows:
        if campaign.id is None or activity.id is None:
            continue
        campaign_id = int(campaign.id)
        activity_id = int(activity.id)
        window_hours = _vk_activity_window(activity)
        since_utc = now_utc - timedelta(hours=window_hours)
        events = await _events_for_target(db, target=target, campaign=campaign, today=today)
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
            recent_exposures = await _recent_activity_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_PUBLICATION,
                since_utc=since_utc,
            )
            needed = max(0, due_count - len(organic) - len(recent_exposures))
            needed = min(needed, 1)
            if needed <= 0:
                continue
            recent_event_ids = {int(ev.id) for ev, _url, _dt in organic if ev.id is not None}
            for exposure in recent_exposures:
                recent_event_ids.add(int(exposure.event_id))
            stats = await _load_public_exposure_stats(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                event_ids=[int(ev.id) for ev in events],
            )

            def sort_key(ev: Event) -> tuple[int, datetime, str, str, int]:
                count, last_at = stats.get(int(ev.id), (0, None))
                last_key = last_at or datetime.min.replace(tzinfo=timezone.utc)
                return (
                    count,
                    last_key,
                    _stable_shuffle_key(campaign_id, activity_id, today.isoformat(), ev.id),
                    str(ev.date or ""),
                    int(ev.id),
                )

            candidates = [ev for ev in sorted(events, key=sort_key) if int(ev.id) not in recent_event_ids]
            for ev in candidates[:needed]:
                try:
                    url = await _build_promo_vk_source_post(
                        db,
                        bot,
                        ev,
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
            recent_reposts = await _recent_activity_exposures(
                db,
                campaign_id=campaign_id,
                activity_id=activity_id,
                surface=PROMO_SURFACE_VK_REPOST,
                since_utc=since_utc,
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
    return results
