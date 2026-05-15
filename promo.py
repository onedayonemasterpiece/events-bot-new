from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from difflib import SequenceMatcher
from typing import Iterable

from sqlalchemy import func, or_, select

from db import Database
from models import (
    Event,
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


def _campaign_end_dt(day: date) -> datetime:
    return datetime.combine(day, time(23, 59, 59), tzinfo=timezone.utc)


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


def _event_is_promo_eligible(ev: Event, *, today: date, campaign: PromoCampaign) -> bool:
    if not event_is_future_for_promo(ev, today=today):
        return False
    if not _event_is_not_after_campaign(ev, campaign=campaign):
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
            .where(Event.date <= ends.isoformat())
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
                    PromoActivity.surface == "video_general",
                    PromoActivity.profile_key == "popular_review",
                )
            )
            for activity in activity_res.scalars().all():
                if activity.selection_policy != PROMO_POLICY_GUARANTEED_ANY_POSITION:
                    activity.selection_policy = PROMO_POLICY_GUARANTEED_ANY_POSITION
                    changed = True
                if int(activity.max_per_publish or 1) != 2:
                    activity.max_per_publish = 2
                    changed = True
                session.add(activity)
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
        session.add_all([target, popular_review, daily, telegraph_month, telegraph_weekend])
        await session.commit()
        logger.info(
            "promo.seed created campaign_id=%s festival=%s future_events=%s",
            campaign.id,
            INITIAL_80_STORIES_FESTIVAL,
            future_count,
        )
        return campaign


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
            if not ev or not _event_is_promo_eligible(ev, today=today, campaign=campaign):
                return []
            return [ev]
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
            .where(PromoExposure.publish_status.in_(("PUBLISHED_MAIN", "PUBLISHED_TEST")))
            .group_by(PromoExposure.event_id)
        )
        rows = res.all()
    stats: dict[int, tuple[int, datetime | None]] = {}
    for event_id, count, last_at in rows:
        stats[int(event_id)] = (int(count or 0), last_at)
    return stats


async def resolve_video_promo_candidates(
    db: Database,
    *,
    profile_key: str,
    now_utc: datetime | None = None,
    surface: str = "video_general",
) -> list[PromoCandidate]:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()
    await ensure_initial_80_stories_campaign(db, now_utc=now_utc)

    async with db.get_session() as session:
        res = await session.execute(
            select(PromoCampaign, PromoActivity, PromoTarget)
            .join(PromoActivity, PromoActivity.campaign_id == PromoCampaign.id)
            .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
            .where(PromoCampaign.status == "active")
            .where(PromoCampaign.starts_at <= now_utc)
            .where(or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc))
            .where(PromoActivity.enabled.is_(True))
            .where(PromoActivity.surface == surface)
            .where(or_(PromoActivity.profile_key.is_(None), PromoActivity.profile_key == profile_key))
            .order_by(PromoCampaign.priority, PromoCampaign.created_at, PromoActivity.id, PromoTarget.id)
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
        placement_kind = (
            PROMO_POLICY_GUARANTEED_ANY_POSITION
            if str(activity.selection_policy or "") == PROMO_POLICY_GUARANTEED_ANY_POSITION
            else "general_boost"
        )
        for ev in picked:
            if ev.id is None:
                continue
            used_event_ids.add(int(ev.id))
            result.append(
                PromoCandidate(
                    event=ev,
                    campaign_id=int(campaign.id),
                    activity_id=int(activity.id),
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
