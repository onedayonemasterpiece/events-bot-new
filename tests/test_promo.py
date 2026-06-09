from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from db import Database
from handlers.promo_cmd import _campaign_lines, _parse_until_date
from models import (
    Event,
    EventSource,
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
    PROMO_SURFACE_VK_STORY,
    create_event_promo_campaign,
    create_festival_promo_campaign,
    ensure_initial_80_stories_campaign,
    run_promo_vk_activities,
    resolve_video_promo_candidates,
)
from video_announce.popular_review import PopularReviewPick, _merge_promo_and_fresh_picks


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


def _popular_review_pick(
    event_id: int,
    *,
    placement: str | None = None,
) -> PopularReviewPick:
    event = _event(f"Event {event_id}", "2026-06-20")
    event.id = event_id
    return PopularReviewPick(
        event=event,
        score=100.0 - event_id,
        source_window="24h",
        source_post_url=f"https://example.com/{event_id}",
        source_label="test",
        anti_repeat_status="fresh",
        description="test",
        promo_campaign_id=1 if placement else None,
        promo_activity_id=1 if placement else None,
        promo_placement_kind=placement,
    )


def test_parse_until_date_accepts_russian_month() -> None:
    query, end = _parse_until_date(
        '"80 историй о главном" до 18 июля',
        today=date(2026, 5, 14),
    )

    assert query == '"80 историй о главном"'
    assert end == date(2026, 7, 18)


def test_popular_review_guaranteed_any_position_is_mixed_stably() -> None:
    positions: list[int] = []
    for day in range(9, 22):
        promo = _popular_review_pick(80, placement=PROMO_POLICY_GUARANTEED_ANY_POSITION)
        fresh = [_popular_review_pick(event_id) for event_id in range(1, 7)]

        selected = _merge_promo_and_fresh_picks(
            [promo],
            fresh,
            max_events=6,
            now_utc=datetime(2026, 6, day, 8, 0, tzinfo=timezone.utc),
        )

        ids = [int(pick.event.id) for pick in selected]
        assert len(ids) == 6
        assert 80 in ids
        positions.append(ids.index(80) + 1)

    assert min(positions) >= 2
    assert any(position < 6 for position in positions)
    assert len(set(positions)) > 1


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
                    PromoActivity.surface.in_(
                        [
                            PROMO_SURFACE_VK_PUBLICATION,
                            PROMO_SURFACE_VK_REPOST,
                            PROMO_SURFACE_VK_STORY,
                        ]
                    ),
                )
            )
        ).scalars().all()
    assert [activity.surface for activity in vk_activities] == [
        PROMO_SURFACE_VK_PUBLICATION,
        PROMO_SURFACE_VK_REPOST,
        PROMO_SURFACE_VK_STORY,
        PROMO_SURFACE_VK_STORY,
    ]
    assert {activity.profile_key for activity in vk_activities if activity.surface == PROMO_SURFACE_VK_STORY} == {
        "klgdevents:story",
        "klgdevents->kenigeventsofficial:story",
    }
    await db.close()


@pytest.mark.asyncio
async def test_promo_vk_publication_blocks_text_only_telegram_event(tmp_path, monkeypatch, caplog) -> None:
    import main
    from promo import _build_promo_vk_source_post

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setattr(main, "VK_MAX_ATTACHMENTS", 10)
    monkeypatch.delenv("VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS", raising=False)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")

    async def fake_post_to_vk(*args, **kwargs):
        raise AssertionError("Telegram-origin promo VK publication must not post without media")

    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    ev = _event("Калининградский порт", "2026-07-07", festival="80 историй о главном")
    ev.id = 4417
    ev.source_post_url = "https://t.me/kraftmarket39/199"
    ev.photo_urls = []
    ev.photo_count = 0

    caplog.set_level(logging.INFO, logger="promo")
    with pytest.raises(RuntimeError, match="vk_sync_missing_media_for_telegram_event"):
        await _build_promo_vk_source_post(
            db,
            None,
            ev,
            campaign_id=1,
            activity_id=8,
            target_group_id=231920894,
        )

    assert "promo.vk publication media" in caplog.text
    assert "source_kind=telegram" in caplog.text
    assert "photo_urls_count=0" in caplog.text
    assert "attachments_count=0" in caplog.text
    await db.close()


@pytest.mark.asyncio
async def test_promo_vk_publication_recovers_telegraph_media_before_posting(tmp_path, monkeypatch) -> None:
    import main
    from promo import _build_promo_vk_source_post

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setattr(main, "VK_MAX_ATTACHMENTS", 10)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")

    async def fake_extract(url: str):
        assert url == "https://telegra.ph/Port-07-07"
        return ["https://example.com/recovered.jpg"]

    async def fake_upload(group_id, photo_url, db_arg=None, bot_arg=None):
        assert photo_url == "https://example.com/recovered.jpg"
        return "photo-231920894_1"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None):
        assert attachments == ["photo-231920894_1"]
        return "https://vk.com/wall-231920894_1"

    monkeypatch.setattr(main, "extract_telegraph_image_urls", fake_extract)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)

    ev = _event("Калининградский порт", "2026-07-07", festival="80 историй о главном")
    ev.id = 4417
    ev.source_post_url = "https://t.me/kraftmarket39/199"
    ev.telegraph_url = "https://telegra.ph/Port-07-07"
    ev.photo_urls = []
    ev.photo_count = 0

    url = await _build_promo_vk_source_post(
        db,
        None,
        ev,
        campaign_id=1,
        activity_id=8,
        target_group_id=231920894,
    )

    assert url == "https://vk.com/wall-231920894_1"
    assert ev.photo_urls == ["https://example.com/recovered.jpg"]
    assert ev.photo_count == 1
    await db.close()


def test_promo_vk_publication_candidate_requires_media_only_for_telegram(monkeypatch) -> None:
    from promo import _promo_vk_publication_missing_required_media

    monkeypatch.delenv("VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS", raising=False)
    telegram_no_media = _event("No media", "2026-07-07", festival="80 историй о главном")
    telegram_no_media.source_post_url = "https://t.me/kraftmarket39/199"
    telegram_no_media.photo_urls = []
    assert _promo_vk_publication_missing_required_media(telegram_no_media) is True

    telegram_with_media = _event("With media", "2026-07-07", festival="80 историй о главном")
    telegram_with_media.source_post_url = "https://t.me/kraftmarket39/200"
    assert _promo_vk_publication_missing_required_media(telegram_with_media) is False

    vk_no_media = _event("VK no media", "2026-07-07", festival="80 историй о главном")
    vk_no_media.source_post_url = "https://vk.com/wall-1_2"
    vk_no_media.photo_urls = []
    assert _promo_vk_publication_missing_required_media(vk_no_media) is False

    monkeypatch.setenv("VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS", "0")
    assert _promo_vk_publication_missing_required_media(telegram_no_media) is False


@pytest.mark.asyncio
async def test_recent_activity_exposures_ignore_failed_status_by_default(tmp_path) -> None:
    from promo import _recent_activity_exposures

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 5, 8, 0, tzinfo=timezone.utc)
    async with db.get_session() as session:
        campaign = PromoCampaign(title="80 stories", status="active", starts_at=now_utc)
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VK_PUBLICATION,
            enabled=True,
        )
        failed_event = _event("Failed event", "2026-07-07", festival="80 историй о главном")
        scheduled_event = _event("Scheduled event", "2026-07-08", festival="80 историй о главном")
        session.add_all([activity, failed_event, scheduled_event])
        await session.flush()
        campaign_id = int(campaign.id)
        activity_id = int(activity.id)
        failed_event_id = int(failed_event.id)
        scheduled_event_id = int(scheduled_event.id)
        session.add(
            PromoExposure(
                campaign_id=campaign_id,
                activity_id=activity_id,
                event_id=failed_event_id,
                surface=PROMO_SURFACE_VK_PUBLICATION,
                placement_kind="rolling_window_deficit",
                publish_status="FAILED_NO_MEDIA",
                published_at=now_utc,
            )
        )
        session.add(
            PromoExposure(
                campaign_id=campaign_id,
                activity_id=activity_id,
                event_id=scheduled_event_id,
                surface=PROMO_SURFACE_VK_PUBLICATION,
                placement_kind="rolling_window_deficit",
                publish_status="VK_SCHEDULED",
                published_at=now_utc,
            )
        )
        await session.commit()

    public_rows = await _recent_activity_exposures(
        db,
        campaign_id=campaign_id,
        activity_id=activity_id,
        surface=PROMO_SURFACE_VK_PUBLICATION,
        since_utc=now_utc.replace(hour=0),
    )
    all_rows = await _recent_activity_exposures(
        db,
        campaign_id=campaign_id,
        activity_id=activity_id,
        surface=PROMO_SURFACE_VK_PUBLICATION,
        since_utc=now_utc.replace(hour=0),
        public_only=False,
    )

    assert [row.event_id for row in public_rows] == [scheduled_event_id]
    assert {row.event_id for row in all_rows} == {failed_event_id, scheduled_event_id}
    await db.close()


@pytest.mark.asyncio
async def test_promo_vk_publication_records_no_media_candidate_as_failed(tmp_path, monkeypatch) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 5, 16, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        campaign = PromoCampaign(
            title="80 stories",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=now_utc.replace(month=9, day=1),
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VK_PUBLICATION,
            enabled=True,
            profile_key="klgdevents",
            config_json={"target_group": "klgdevents", "window_hours": 24, "daily_slots": ["12:00"]},
        )
        missing = _event("No media", "2026-07-07", festival="80 историй о главном")
        missing.source_post_url = "https://t.me/kraftmarket39/199"
        missing.photo_urls = []
        missing.photo_count = 0
        ok = _event("With media", "2026-07-08", festival="80 историй о главном")
        ok.source_post_url = "https://t.me/kraftmarket39/200"
        session.add_all([activity, missing, ok])
        await session.commit()

    async def fake_resolve(ref: str):
        return 231920894, "Events", "klgdevents", "group"

    async def fake_upload(*args, **kwargs):
        return "photo-231920894_1"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None):
        return "https://vk.com/wall-231920894_99"

    async def fake_extract(url: str):
        return []

    monkeypatch.setattr(main, "vk_resolve_group", fake_resolve)
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setattr(main, "VK_MAX_ATTACHMENTS", 10)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")
    monkeypatch.setattr(main, "extract_telegraph_image_urls", fake_extract)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)

    assert [item.status for item in results] == ["failed", "scheduled"]
    async with db.get_session() as session:
        exposures = (await session.execute(select(PromoExposure).order_by(PromoExposure.id))).scalars().all()
    assert [row.publish_status for row in exposures] == ["FAILED_NO_MEDIA", "VK_SCHEDULED"]
    assert exposures[0].public_target_count == 0
    assert exposures[0].public_targets_json == []
    assert exposures[0].details_json["action"] == "investigate_source_media_and_rehydrate_before_publication"
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
        assert kwargs["token"] == "user-token"
        assert kwargs["token_kind"] == "user"
        return {"response": {"post_id": 9}}

    async def fake_vk_api(method, **params):
        assert method == "wall.getById"
        return {"response": [{"date": int(now_utc.timestamp())}]}

    async def fake_short_text(*args, **kwargs):
        return "короткий рерайт?"

    async def fake_story_image(ev, *, source_url):
        return f"story:{ev.id}:{source_url}".encode()

    story_posts: list[tuple[int, bytes, str | None]] = []

    async def fake_story_publish(
        db_arg,
        bot_arg,
        *,
        target_group_id,
        image_bytes,
        source_url=None,
        link_text=None,
        include_source_link=True,
    ):
        assert include_source_link is False
        story_posts.append((target_group_id, image_bytes, source_url))
        return {
            "url": f"https://vk.com/story-{target_group_id}_{len(story_posts)}",
            "owner_id": -target_group_id,
            "story_id": len(story_posts),
            "expires_at": 1780600000,
        }

    monkeypatch.setattr(main, "vk_resolve_group", fake_resolve)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", False)
    monkeypatch.setattr(main, "upload_vk_photo", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "build_short_vk_text", fake_short_text)
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")
    monkeypatch.setattr(main, "choose_vk_actor", lambda owner_id, intent: [SimpleNamespace(kind="group", token="tok", label="group")])
    monkeypatch.setattr(main, "_vk_api", fake_repost_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr("promo._build_vk_story_image_bytes", fake_story_image)
    monkeypatch.setattr("promo._publish_vk_story_photo", fake_story_publish)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)
    results.extend(await run_promo_vk_activities(db, None, now_utc=now_utc))

    assert [item.status for item in results].count("scheduled") == 2
    assert [item.status for item in results].count("published") == 5
    assert len(posted) == 2
    assert [(target, source) for target, _bytes, source in story_posts] == [
        (111, "https://vk.com/wall-111_1"),
        (222, "https://vk.com/wall-111_1"),
        (111, "https://vk.com/wall-111_2"),
        (222, "https://vk.com/wall-111_2"),
    ]
    async with db.get_session() as session:
        exposures = (await session.execute(select(PromoExposure))).scalars().all()
    assert [exposure.surface for exposure in exposures] == [
        PROMO_SURFACE_VK_PUBLICATION,
        PROMO_SURFACE_VK_REPOST,
        PROMO_SURFACE_VK_STORY,
        PROMO_SURFACE_VK_STORY,
        PROMO_SURFACE_VK_PUBLICATION,
        PROMO_SURFACE_VK_STORY,
        PROMO_SURFACE_VK_STORY,
    ]
    repost = [exposure for exposure in exposures if exposure.surface == PROMO_SURFACE_VK_REPOST][0]
    assert repost.details_json["source_url"].startswith("https://vk.com/wall-111_")
    assert repost.details_json["target_url"] == "https://vk.com/wall-222_9"
    stories = [exposure for exposure in exposures if exposure.surface == PROMO_SURFACE_VK_STORY]
    assert len(stories) == 4
    assert {story.public_targets_json[0]["type"] for story in stories} == {"vk_story"}
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
        high_event_2 = _event("High Event 2", "2026-06-02")
        low_event = _event("Low Event", "2026-06-01")
        session.add_all([high, low, high_event, high_event_2, low_event])
        await session.commit()
        await session.refresh(high)
        await session.refresh(low)
        await session.refresh(high_event)
        await session.refresh(high_event_2)
        await session.refresh(low_event)
        session.add_all(
            [
                PromoTarget(
                    campaign_id=int(high.id),
                    target_type="festival",
                    festival_name="high-festival",
                ),
                PromoActivity(
                    campaign_id=int(high.id),
                    surface="video_general",
                    profile_key="popular_review",
                    max_per_publish=2,
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
        high_event.festival = "high-festival"
        high_event_2.festival = "high-festival"
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


@pytest.mark.asyncio
async def test_vk_story_image_uses_source_post_photo_without_text_panel(monkeypatch) -> None:
    import promo as promo_module

    ev = _event("Калининград корабельный", "2026-07-08")
    calls: list[str] = []

    async def fake_source_photo(source_url):
        calls.append(f"source:{source_url}")
        return "https://example.test/source.jpg"

    async def fake_download(url):
        calls.append(f"download:{url}")
        return b"raw-source-image"

    monkeypatch.setattr(promo_module, "_source_wall_photo_url", fake_source_photo)
    monkeypatch.setattr(promo_module, "_download_story_source_image", fake_download)

    data = await promo_module._build_vk_story_image_bytes(
        ev,
        source_url="https://vk.com/wall-231920894_1974",
    )

    assert data == b"raw-source-image"
    assert calls == [
        "source:https://vk.com/wall-231920894_1974",
        "download:https://example.test/source.jpg",
    ]


@pytest.mark.asyncio
async def test_vk_post_datetime_falls_back_to_user_actor(monkeypatch) -> None:
    import main

    from promo import _vk_post_datetime

    async def fake_service_vk_api(method, **kwargs):
        assert method == "wall.getById"
        return {"response": []}

    async def fake_user_vk_api(method, params, **kwargs):
        assert method == "wall.getById"
        assert params["posts"] == "-231920894_1974"
        assert kwargs["token"] == "user-token"
        assert kwargs["token_kind"] == "user"
        return {"response": [{"date": 1780570980}]}

    monkeypatch.setattr(main, "vk_api", fake_service_vk_api)
    monkeypatch.setattr(main, "_vk_api", fake_user_vk_api)
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")

    assert await _vk_post_datetime("https://vk.com/wall-231920894_1974") == datetime.fromtimestamp(
        1780570980,
        timezone.utc,
    )


@pytest.mark.asyncio
async def test_recent_event_vk_posts_resolves_stale_postponed_event_url(tmp_path, monkeypatch) -> None:
    import main
    import promo as promo_module

    from promo import _recent_event_vk_posts

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 4, 11, 5, tzinfo=timezone.utc)

    async with db.get_session() as session:
        ev = _event("Калининград корабельный", "2026-07-08", festival="80 историй о главном")
        ev.source_vk_post_url = "https://vk.com/wall-231920894_1973"
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        event_id = int(ev.id)

    async def fake_post_datetime(url: str | None):
        if url == "https://vk.com/wall-231920894_1974":
            return datetime(2026, 6, 4, 11, 3, tzinfo=timezone.utc)
        return None

    async def fake_resolve_existing_vk_post_url(url, *, target_group_id, db, bot):
        assert url == "https://vk.com/wall-231920894_1973"
        assert target_group_id == "231920894"
        return "https://vk.com/wall-231920894_1974"

    monkeypatch.setattr(promo_module, "_vk_post_datetime", fake_post_datetime)
    monkeypatch.setattr(main, "_resolve_existing_vk_post_url", fake_resolve_existing_vk_post_url)

    rows = await _recent_event_vk_posts(
        [ev],
        group_id=231920894,
        since_utc=now_utc.replace(hour=10),
        until_utc=now_utc,
        db=db,
    )

    assert [(int(item.id), url, posted_at) for item, url, posted_at in rows] == [
        (
            event_id,
            "https://vk.com/wall-231920894_1974",
            datetime(2026, 6, 4, 11, 3, tzinfo=timezone.utc),
        )
    ]
    async with db.get_session() as session:
        saved = await session.get(Event, event_id)
    assert saved.source_vk_post_url == "https://vk.com/wall-231920894_1974"
    await db.close()


def test_parse_chat_author_query_splits_and_normalizes() -> None:
    from promo import _parse_chat_author_query

    assert _parse_chat_author_query("kraftmarket39:langeanna") == ("kraftmarket39", "langeanna")
    assert _parse_chat_author_query("@Kraftmarket39:@LangeAnna") == ("kraftmarket39", "langeanna")
    assert _parse_chat_author_query("nocolon") == ("", "")
    assert _parse_chat_author_query(None) == ("", "")


def test_chat_post_author_username_chat_only() -> None:
    from source_parsing.telegram.handlers import _chat_post_author_username

    chat_msg = {"source_type": "supergroup", "post_author": {"is_user": True, "username": "LangeAnna"}}
    assert _chat_post_author_username(chat_msg) == "langeanna"
    telethon_like_msg = {
        "source_type": "supergroup",
        "post_author": None,
        "sender": {"type": "User", "username": "LANGEANNA"},
    }
    assert _chat_post_author_username(telethon_like_msg) == "langeanna"
    # Channels: author is the channel, not a user -> no trigger.
    channel_msg = {"source_type": "channel", "post_author": {"is_channel": True, "username": "kraftmarket39"}}
    assert _chat_post_author_username(channel_msg) is None
    # Group message without a resolved user author.
    assert _chat_post_author_username({"source_type": "group", "post_author": None}) is None


@pytest.mark.asyncio
async def test_tg_chat_author_target_matches_only_that_author(tmp_path) -> None:
    from models import EventSource, PromoTarget
    from promo import (
        PROMO_TARGET_TYPE_TG_CHAT_AUTHOR,
        _events_for_target,
        ensure_kraftmarket_langeanna_campaign,
    )

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 4, 10, 0, tzinfo=timezone.utc)
    today = now_utc.date()

    campaign = await ensure_kraftmarket_langeanna_campaign(db, now_utc=now_utc)
    # Idempotent: second call keeps a single target + activity.
    await ensure_kraftmarket_langeanna_campaign(db, now_utc=now_utc)

    async with db.get_session() as session:
        targets = (
            await session.execute(
                select(PromoTarget).where(PromoTarget.campaign_id == campaign.id)
            )
        ).scalars().all()
        assert len(targets) == 1
        assert targets[0].target_type == PROMO_TARGET_TYPE_TG_CHAT_AUTHOR
        activities = (
            await session.execute(
                select(PromoActivity).where(PromoActivity.campaign_id == campaign.id)
            )
        ).scalars().all()
        assert len(activities) == 1
        assert activities[0].surface == "video_general"
        assert activities[0].profile_key is None
        assert int(activities[0].max_per_publish) == 1
        assert activities[0].selection_policy == PROMO_POLICY_GUARANTEED_ANY_POSITION

        # Matching event: kraftmarket39 chat + langeanna author.
        ev_match = _event("Большой крафт-маркет «Полюбить 39»", "2026-06-20")
        ev_match.tg_source_author = "langeanna"
        # Same chat, different author -> must NOT match.
        ev_other = _event("Чужое событие из того же чата", "2026-06-21")
        ev_other.tg_source_author = "someoneelse"
        session.add_all([ev_match, ev_other])
        await session.commit()
        await session.refresh(ev_match)
        await session.refresh(ev_other)
        session.add_all([
            EventSource(
                event_id=int(ev_match.id),
                source_type="telegram",
                source_url="https://t.me/kraftmarket39/95",
                source_chat_username="kraftmarket39",
                source_message_id=95,
            ),
            EventSource(
                event_id=int(ev_other.id),
                source_type="telegram",
                source_url="https://t.me/kraftmarket39/96",
                source_chat_username="kraftmarket39",
                source_message_id=96,
            ),
        ])
        await session.commit()
        target = targets[0]

    matched = await _events_for_target(db, target=target, campaign=campaign, today=today)
    matched_ids = {int(e.id) for e in matched}
    assert int(ev_match.id) in matched_ids
    assert int(ev_other.id) not in matched_ids


@pytest.mark.asyncio
async def test_promo_report_counts_tg_chat_author_future_events(tmp_path) -> None:
    from promo import ensure_kraftmarket_langeanna_campaign

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    campaign = await ensure_kraftmarket_langeanna_campaign(
        db,
        now_utc=datetime(2026, 6, 4, 10, 0, tzinfo=timezone.utc),
    )
    async with db.get_session() as session:
        ev_match = _event("ANNA LANGE future event", "2099-06-20")
        ev_match.tg_source_author = "langeanna"
        ev_other = _event("Other author future event", "2099-06-21")
        ev_other.tg_source_author = "confidentmax"
        session.add_all([ev_match, ev_other])
        await session.commit()
        await session.refresh(ev_match)
        await session.refresh(ev_other)
        session.add_all(
            [
                EventSource(
                    event_id=int(ev_match.id),
                    source_type="telegram",
                    source_url="https://t.me/kraftmarket39/275",
                    source_chat_username="kraftmarket39",
                    source_message_id=275,
                ),
                EventSource(
                    event_id=int(ev_other.id),
                    source_type="telegram",
                    source_url="https://t.me/kraftmarket39/276",
                    source_chat_username="kraftmarket39",
                    source_message_id=276,
                ),
            ]
        )
        await session.commit()

    lines = await _campaign_lines(db, include_archived=True, include_details=True)
    campaign_block = next(line for line in lines if f"#{campaign.id} " in line)
    assert "Будущих событий сейчас: 1" in campaign_block
