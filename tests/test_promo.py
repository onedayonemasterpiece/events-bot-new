from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from db import Database
from handlers.promo_cmd import _campaign_lines, _parse_until_date
from models import (
    Event,
    Festival,
    PromoActivity,
    PromoCampaign,
    PromoTarget,
    VideoAnnounceItem,
    VideoAnnounceItemStatus,
    VideoAnnounceSession,
    VideoAnnounceSessionStatus,
)
from promo import (
    PROMO_POLICY_GUARANTEED_ANY_POSITION,
    create_event_promo_campaign,
    create_festival_promo_campaign,
    ensure_initial_80_stories_campaign,
    resolve_video_promo_candidates,
)


def _event(title: str, day: str, *, festival: str | None = None) -> Event:
    return Event(
        title=title,
        description="Description",
        short_description="Short",
        search_digest="Digest",
        festival=festival,
        source_text="source",
        date=day,
        time="19:00",
        location_name="Venue",
        city="Калининград",
        photo_urls=["https://example.com/poster.jpg"],
        photo_count=1,
    )


def test_parse_until_date_accepts_russian_month() -> None:
    query, end = _parse_until_date(
        '"80 историй о главном" до 18 июля',
        today=date(2026, 5, 14),
    )

    assert query == '"80 историй о главном"'
    assert end == date(2026, 7, 18)


@pytest.mark.asyncio
async def test_create_festival_promo_requires_existing_future_events(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 14, 8, 0, tzinfo=timezone.utc)

    missing = await create_festival_promo_campaign(
        db,
        festival_name="Кантата",
        now_utc=now_utc,
    )
    assert missing.status == "missing_festival"

    async with db.get_session() as session:
        session.add(Festival(name="Кантата"))
        session.add(_event("Past Festival Event", "2026-05-01", festival="Кантата"))
        await session.commit()

    no_future = await create_festival_promo_campaign(
        db,
        festival_name="Кантата",
        now_utc=now_utc,
    )
    assert no_future.status == "no_future_events"

    async with db.get_session() as session:
        session.add(_event("Future Festival Event", "2026-06-01", festival="Кантата"))
        await session.commit()

    created = await create_festival_promo_campaign(
        db,
        festival_name="Кантата",
        ends_at=date(2026, 7, 18),
        now_utc=now_utc,
    )

    assert created.status == "created"
    assert created.campaign is not None
    async with db.get_session() as session:
        targets = (await session.execute(select(PromoTarget))).scalars().all()
        activities = (await session.execute(select(PromoActivity))).scalars().all()
    assert [(target.target_type, target.festival_name) for target in targets] == [("festival", "Кантата")]
    assert {activity.surface for activity in activities} == {
        "video_general",
        "daily_highlight",
        "telegraph_month",
        "telegraph_weekend",
    }
    await db.close()


@pytest.mark.asyncio
async def test_create_event_promo_matches_only_future_events(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 14, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_event("Камерная лекция про море", "2026-05-01"))
        future = _event("Камерная лекция про море", "2026-06-01")
        session.add(future)
        await session.commit()
        await session.refresh(future)
        future_id = int(future.id)

    created = await create_event_promo_campaign(
        db,
        query_text="лекция про море",
        now_utc=now_utc,
    )

    assert created.status == "created"
    assert created.campaign is not None
    async with db.get_session() as session:
        campaign = await session.get(PromoCampaign, int(created.campaign.id))
        target = (await session.execute(select(PromoTarget))).scalars().one()
    assert campaign is not None
    assert campaign.status == "active"
    assert target.target_type == "event"
    assert target.event_id == future_id
    await db.close()


@pytest.mark.asyncio
async def test_create_festival_promo_accepts_existing_future_event_festival_label(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 14, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_event("Future Labeled Festival Event", "2026-06-01", festival="80 историй о главном"))
        await session.commit()

    created = await create_festival_promo_campaign(
        db,
        festival_name="80 историй о главном",
        ends_at=date(2026, 7, 18),
        now_utc=now_utc,
    )

    assert created.status == "created"
    assert created.campaign is not None
    await db.close()


@pytest.mark.asyncio
async def test_initial_80_stories_campaign_priority_and_any_position_policy(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 14, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(Festival(name="80 историй о главном"))
        session.add(_event("Future Labeled Festival Event", "2026-06-01", festival="80 историй о главном"))
        await session.commit()

    campaign = await ensure_initial_80_stories_campaign(db, now_utc=now_utc)

    assert campaign is not None
    async with db.get_session() as session:
        stored = await session.get(PromoCampaign, int(campaign.id))
        activity = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == campaign.id,
                    PromoActivity.surface == "video_general",
                )
            )
        ).scalars().one()
    assert stored is not None
    assert stored.priority == 1
    assert activity.selection_policy == PROMO_POLICY_GUARANTEED_ANY_POSITION
    assert activity.max_per_publish == 2
    await db.close()


@pytest.mark.asyncio
async def test_video_promo_resolver_uses_priority_and_global_budget(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 14, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        high = PromoCampaign(
            title="high",
            status="active",
            priority=0,
            starts_at=now_utc,
            ends_at=datetime(2026, 7, 1, 23, 59, tzinfo=timezone.utc),
        )
        low = PromoCampaign(
            title="low",
            status="active",
            priority=3,
            starts_at=now_utc,
            ends_at=datetime(2026, 7, 1, 23, 59, tzinfo=timezone.utc),
        )
        high_event = _event("High Event", "2026-06-01")
        low_event = _event("Low Event", "2026-06-01")
        session.add_all([high, low, high_event, low_event])
        await session.commit()
        await session.refresh(high)
        await session.refresh(low)
        await session.refresh(high_event)
        await session.refresh(low_event)
        session.add_all(
            [
                PromoTarget(
                    campaign_id=int(high.id),
                    target_type="event",
                    event_id=int(high_event.id),
                ),
                PromoActivity(
                    campaign_id=int(high.id),
                    surface="video_general",
                    profile_key="popular_review",
                    max_per_publish=1,
                    enabled=True,
                ),
                PromoTarget(
                    campaign_id=int(low.id),
                    target_type="event",
                    event_id=int(low_event.id),
                ),
                PromoActivity(
                    campaign_id=int(low.id),
                    surface="video_general",
                    profile_key="popular_review",
                    max_per_publish=1,
                    enabled=True,
                ),
            ]
        )
        await session.commit()

    picks = await resolve_video_promo_candidates(
        db,
        profile_key="popular_review",
        now_utc=now_utc,
    )

    assert [pick.event.title for pick in picks[:2]] == ["High Event", "Low Event"]
    assert len(picks) <= 2
    assert picks[0].priority == 0
    await db.close()


@pytest.mark.asyncio
async def test_promo_report_counts_viewer_facing_cherryflash_test_status(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        campaign = PromoCampaign(
            title="80 историй о главном / summer visibility",
            status="active",
            starts_at=datetime(2026, 5, 14, 7, 0, tzinfo=timezone.utc),
            ends_at=datetime(2026, 7, 18, 23, 59, tzinfo=timezone.utc),
        )
        event = _event("Заводы и пароходы", "2026-05-16", festival="80 историй о главном")
        session.add_all([campaign, event])
        await session.commit()
        await session.refresh(campaign)
        await session.refresh(event)
        session.add(
            PromoTarget(
                campaign_id=int(campaign.id),
                target_type="festival",
                festival_name="80 историй о главном",
            )
        )
        session.add(
            PromoActivity(
                campaign_id=int(campaign.id),
                surface="video_general",
                profile_key="popular_review",
                max_per_publish=2,
                enabled=True,
            )
        )
        video_session = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
            profile_key="popular_review",
            published_at=datetime(2026, 5, 14, 9, 5, tzinfo=timezone.utc),
            test_chat_id=-1002210431821,
            selection_params={"mode": "popular_review"},
        )
        session.add(video_session)
        await session.commit()
        await session.refresh(video_session)
        session.add(
            VideoAnnounceItem(
                session_id=int(video_session.id),
                event_id=int(event.id),
                status=VideoAnnounceItemStatus.READY,
                position=2,
                promo_campaign_id=int(campaign.id),
                promo_activity_id=1,
                promo_placement_kind="general_boost",
            )
        )
        await session.commit()

    lines = await _campaign_lines(db, include_archived=True, include_details=True)
    report = "\n\n".join(lines)

    assert "видео-публикаций: 1; промо-показов: 1" in report
    assert "session #" in report
    assert "статус PUBLISHED_TEST" in report
    assert "Заводы и пароходы" in report
    await db.close()
