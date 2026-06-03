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
    PromoExposure,
    PromoTarget,
    VideoAnnounceItem,
    VideoAnnounceItemStatus,
    VideoAnnounceSession,
    VideoAnnounceSessionStatus,
)
from promo import (
    PROMO_POLICY_GUARANTEED_ANY_POSITION,
    PROMO_POLICY_FIRST_SLOT,
    PROMO_SURFACE_VK_PUBLICATION,
    PROMO_SURFACE_VK_REPOST,
    create_event_promo_campaign,
    create_festival_promo_campaign,
    ensure_initial_80_stories_campaign,
    run_promo_vk_activities,
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
    async with db.get_session() as session:
        vk_activities = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == campaign.id,
                    PromoActivity.surface.in_([PROMO_SURFACE_VK_PUBLICATION, PROMO_SURFACE_VK_REPOST]),
                )
            )
        ).scalars().all()
    assert {activity.surface for activity in vk_activities} == {
        PROMO_SURFACE_VK_PUBLICATION,
        PROMO_SURFACE_VK_REPOST,
    }
    await db.close()


@pytest.mark.asyncio
async def test_promo_vk_runner_schedules_publications_and_repost(tmp_path, monkeypatch) -> None:
    import main
    from types import SimpleNamespace

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 3, 16, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(Festival(name="80 историй о главном"))
        session.add(_event("Фестиваль 1", "2026-06-10", festival="80 историй о главном"))
        session.add(_event("Фестиваль 2", "2026-06-11", festival="80 историй о главном"))
        await session.commit()

    async def fake_resolve(ref: str):
        if ref == "klgdevents":
            return 111, "Events", "klgdevents", "group"
        if ref == "kenigeventsofficial":
            return 222, "Main", "kenigeventsofficial", "group"
        raise AssertionError(ref)

    posted: list[tuple[str, str]] = []

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None):
        post_id = len(posted) + 1
        posted.append((group_id, message))
        return f"https://vk.com/wall-{group_id}_{post_id}"

    async def fake_repost_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        assert method == "wall.repost"
        assert params["group_id"] == 222
        assert params["message"] == "короткий рерайт?"
        return {"response": {"post_id": 9}}

    async def fake_vk_api(method, **params):
        assert method == "wall.getById"
        return {"response": [{"date": int(now_utc.timestamp())}]}

    async def fake_short_text(*args, **kwargs):
        return "короткий рерайт?"

    monkeypatch.setattr(main, "vk_resolve_group", fake_resolve)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", False)
    monkeypatch.setattr(main, "upload_vk_photo", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "build_short_vk_text", fake_short_text)
    monkeypatch.setattr(main, "choose_vk_actor", lambda owner_id, intent: [SimpleNamespace(kind="group", token="tok", label="group")])
    monkeypatch.setattr(main, "_vk_api", fake_repost_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)
    results.extend(await run_promo_vk_activities(db, None, now_utc=now_utc))

    assert [item.status for item in results].count("scheduled") == 2
    assert [item.status for item in results].count("published") == 1
    assert len(posted) == 2
    async with db.get_session() as session:
        exposures = (await session.execute(select(PromoExposure))).scalars().all()
    assert {exposure.surface for exposure in exposures} == {
        PROMO_SURFACE_VK_PUBLICATION,
        PROMO_SURFACE_VK_REPOST,
    }
    repost = [exposure for exposure in exposures if exposure.surface == PROMO_SURFACE_VK_REPOST][0]
    assert repost.details_json["source_url"].startswith("https://vk.com/wall-111_")
    assert repost.details_json["target_url"] == "https://vk.com/wall-222_9"
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
async def test_video_promo_resolver_honors_slot_total_and_daily_caps(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 17, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        campaign = PromoCampaign(
            title="8 women",
            status="active",
            priority=0,
            starts_at=now_utc,
            ends_at=datetime(2026, 5, 21, 23, 59, tzinfo=timezone.utc),
            total_exposure_goal=2,
            daily_exposure_cap=1,
        )
        event = _event("Спектакль 8 ЖЕНЩИН", "2026-05-22")
        session.add_all([campaign, event])
        await session.commit()
        await session.refresh(campaign)
        await session.refresh(event)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id)))
        first = PromoActivity(
            campaign_id=int(campaign.id),
            surface="video_general",
            profile_key="popular_review",
            slot=1,
            max_per_publish=1,
            target_exposure_goal=1,
            daily_cap=1,
            selection_policy=PROMO_POLICY_FIRST_SLOT,
            enabled=True,
        )
        any_slot = PromoActivity(
            campaign_id=int(campaign.id),
            surface="video_general",
            profile_key="popular_review",
            max_per_publish=1,
            target_exposure_goal=1,
            daily_cap=1,
            selection_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
            enabled=True,
        )
        session.add_all([first, any_slot])
        await session.commit()
        await session.refresh(first)
        await session.refresh(any_slot)
        first_id = int(first.id)
        any_id = int(any_slot.id)
        event_id = int(event.id)
        campaign_id = int(campaign.id)

    first_day = await resolve_video_promo_candidates(
        db,
        profile_key="popular_review",
        now_utc=now_utc,
    )
    assert [(pick.activity_id, pick.placement_kind) for pick in first_day] == [
        (first_id, PROMO_POLICY_FIRST_SLOT)
    ]

    async with db.get_session() as session:
        session.add(
            PromoExposure(
                campaign_id=campaign_id,
                activity_id=first_id,
                event_id=event_id,
                surface="video",
                placement_kind=PROMO_POLICY_FIRST_SLOT,
                publish_status="PUBLISHED_TEST",
                published_at=now_utc,
            )
        )
        await session.commit()

    same_day = await resolve_video_promo_candidates(
        db,
        profile_key="popular_review",
        now_utc=now_utc.replace(hour=12),
    )
    assert same_day == []

    next_day = await resolve_video_promo_candidates(
        db,
        profile_key="popular_review",
        now_utc=datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc),
    )
    assert [(pick.activity_id, pick.placement_kind) for pick in next_day] == [
        (any_id, PROMO_POLICY_GUARANTEED_ANY_POSITION)
    ]
    await db.close()


@pytest.mark.asyncio
async def test_video_promo_resolver_can_exclude_global_profile_for_partner_tracks(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 17, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        campaign = PromoCampaign(
            title="base promo",
            status="active",
            starts_at=now_utc,
            ends_at=datetime(2026, 6, 1, 23, 59, tzinfo=timezone.utc),
        )
        event = _event("Base Promo", "2026-05-22")
        session.add_all([campaign, event])
        await session.commit()
        await session.refresh(campaign)
        await session.refresh(event)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id)))
        session.add(
            PromoActivity(
                campaign_id=int(campaign.id),
                surface="video_general",
                profile_key="popular_review",
                max_per_publish=1,
                enabled=True,
            )
        )
        await session.commit()

    partner_picks = await resolve_video_promo_candidates(
        db,
        profile_key="popular_review_eco",
        now_utc=now_utc,
        include_global_profile=False,
    )
    assert partner_picks == []
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


def test_repost_matches_published_post_after_postponed_id_shift() -> None:
    """Promo publication reconciles to the live wall post by event title.

    Regression: post_to_vk returns the postponed-draft id; VK reassigns the id
    when the postponed post publishes, so the stored URL stops resolving and the
    repost found no eligible source. The wall-scan matcher recovers the live post.
    """
    from promo import _match_published_post_for_event, _post_text_matches_event

    ev = _event("Большой летний крафт-маркет «Полюбить 39»", "2026-06-20")
    # Stored draft was wall-231920894_1938; the live published post is _1939.
    recent_wall = [
        {"post_id": 1937, "date": 1000, "text": "Другое событие", "url": "https://vk.com/wall-231920894_1937"},
        {"post_id": 1939, "date": 2000, "text": "🎪 Большой летний крафт-маркет «Полюбить 39»\n20 июня", "url": "https://vk.com/wall-231920894_1939"},
    ]
    assert _post_text_matches_event(recent_wall[1]["text"], ev) is True
    assert _post_text_matches_event(recent_wall[0]["text"], ev) is False
    match = _match_published_post_for_event(recent_wall, ev)
    assert match is not None
    assert match["url"] == "https://vk.com/wall-231920894_1939"

    # Short/generic titles are not trusted for substring matching.
    short = _event("Шоу", "2026-06-20")
    assert _post_text_matches_event("Шоу сегодня", short) is False
    assert _match_published_post_for_event(recent_wall, short) is None
