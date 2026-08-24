from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from db import Database
from handlers.promo_cmd import _campaign_lines, _parse_until_date
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
    VideoAnnounceItemStatus,
    VideoAnnounceSession,
    VideoAnnounceSessionStatus,
)
from promo import (
    PROMO_POLICY_GUARANTEED_ANY_POSITION,
    PROMO_POLICY_FIRST_SLOT,
    PROMO_SURFACE_DAILY_RECOMMEND_TODAY,
    PROMO_SURFACE_AFISHA_ENGAGEMENT,
    PROMO_POLICY_WEIGHTED_POPULARITY,
    PROMO_TARGET_TYPE_ALL,
    PROMO_SURFACE_TG_BUTTON_HIGHLIGHT,
    PROMO_SURFACE_TG_EVENT_PUBLISH,
    PROMO_SURFACE_TG_REPOST,
    PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
    PROMO_SURFACE_VK_PUBLICATION,
    PROMO_SURFACE_VK_CHANNEL_PUBLISH,
    PROMO_SURFACE_VK_REPOST,
    PROMO_SURFACE_VK_STORY,
    create_event_promo_campaign,
    create_festival_promo_campaign,
    ensure_initial_80_stories_campaign,
    record_daily_promo_recommendation_exposures,
    run_promo_vk_activities,
    _filter_vk_festival_carousel_events_for_variant,
    _render_vk_festival_carousel_card,
    _render_vk_festival_carousel_poster_card,
    _publish_vk_festival_carousel,
    _recent_event_tg_posts,
    _vk_festival_carousel_configured_publish_date,
    resolve_video_promo_candidates,
)
from video_announce.popular_review import PopularReviewPick, _merge_promo_and_fresh_picks


def test_promo_docs_forbid_event_id_only_live_programme_campaigns() -> None:
    contract = Path("docs/features/promo-campaigns/README.md").read_text(encoding="utf-8")
    debt = Path("docs/backlog/features/festival-monitoring-debt/README.md").read_text(
        encoding="utf-8"
    )
    contract_text = " ".join(contract.split())
    debt_text = " ".join(debt.split())

    assert "Live festival/program campaigns must not be modelled as `event.id`-only" in contract_text
    assert "A fixed event-id target set is valid only for a closed, already-audited set" in contract_text
    assert "dynamic anchor (`festival`, `festival_series`, source/author trigger" in contract_text
    assert "they must not be the only eligibility mechanism for the whole live campaign" in contract_text
    assert 'event.festival="Кантата"' in contract_text
    assert "then apply an education" in contract_text
    assert "separates lectures/talks/education events from concerts" in contract_text
    assert "frozen list of the event ids known at campaign creation time" in contract_text
    assert "fixed `event_id`-only target set" in debt_text
    assert "newly imported event" in debt_text
    assert 'event.festival="Кантата"' in debt_text
    assert "lecture/talk under" in debt_text
    assert "concert under the same festival marker is not selected" in debt_text


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


@pytest.mark.asyncio
async def test_tg_repost_rejects_stale_source_snapshot_and_keeps_observed_timestamp(
    tmp_path,
) -> None:
    db = Database(str(tmp_path / "tg-source-snapshot.sqlite"))
    await db.init()
    observed_at = datetime(2026, 8, 23, 2, 21, tzinfo=timezone.utc)

    stale = _event("Старый анонс", "2026-09-05")
    stale.time = "10:00"
    stale.tg_event_post_url = "https://t.me/c/3954607218/3716"
    current = _event("Текущий анонс", "2026-09-05")
    current.time = "11:00"
    current.tg_event_post_url = "https://t.me/c/3954607218/3717"
    async with db.get_session() as session:
        session.add_all([stale, current])
        await session.flush()
        session.add_all(
            [
                EventSource(
                    event_id=int(stale.id),
                    source_type="telegram",
                    source_url="https://t.me/kldevents/3716",
                    source_chat_username="kldevents",
                    source_message_id=3716,
                    source_text="Старый анонс\n23 августа 10:00",
                    imported_at=observed_at,
                ),
                EventSource(
                    event_id=int(current.id),
                    source_type="telegram",
                    source_url="https://t.me/kldevents/3717",
                    source_chat_username="kldevents",
                    source_message_id=3717,
                    source_text="Текущий анонс\n5 сентября 11:00",
                    imported_at=observed_at,
                ),
            ]
        )
        await session.commit()

    rows = await _recent_event_tg_posts(
        db,
        campaign_id=11,
        events=[stale, current],
        source_chat="@kldevents",
        since_utc=datetime(2026, 8, 17, tzinfo=timezone.utc),
        until_utc=datetime(2026, 8, 24, 10, 0, tzinfo=timezone.utc),
    )

    assert [(int(event.id), url, source_at) for event, url, source_at in rows] == [
        (int(current.id), current.tg_event_post_url, observed_at)
    ]
    await db.close()


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


@pytest.mark.asyncio
async def test_campaign_lines_hide_ended_active_campaigns_by_default(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add_all(
            [
                PromoCampaign(
                    title="Past but active",
                    status="active",
                    starts_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                    ends_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
                ),
                PromoCampaign(
                    title="Future active",
                    status="active",
                    starts_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                    ends_at=datetime(2099, 1, 2, tzinfo=timezone.utc),
                ),
            ]
        )
        await session.commit()

    current_lines = "\n".join(await _campaign_lines(db))
    report_lines = "\n".join(
        await _campaign_lines(db, include_archived=True, include_details=True)
    )

    assert "Future active" in current_lines
    assert "Past but active" not in current_lines
    assert "Past but active" in report_lines
    await db.close()


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
        PROMO_SURFACE_TG_BUTTON_HIGHLIGHT,
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
                            PROMO_SURFACE_VK_CHANNEL_PUBLISH,
                            PROMO_SURFACE_VK_REPOST,
                            PROMO_SURFACE_VK_STORY,
                        ]
                    ),
                )
                .order_by(PromoActivity.id)
            )
        ).scalars().all()
    assert [activity.surface for activity in vk_activities] == [
        PROMO_SURFACE_VK_PUBLICATION,
        PROMO_SURFACE_VK_CHANNEL_PUBLISH,
        PROMO_SURFACE_VK_REPOST,
        PROMO_SURFACE_VK_STORY,
        PROMO_SURFACE_VK_STORY,
    ]
    assert {activity.profile_key for activity in vk_activities if activity.surface == PROMO_SURFACE_VK_STORY} == {
        "klgdevents:story",
        "klgdevents->kenigeventsofficial:story",
    }
    async with db.get_session() as session:
        tg_activities = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == campaign.id,
                    PromoActivity.surface.in_(
                        [
                            PROMO_SURFACE_TG_EVENT_PUBLISH,
                            PROMO_SURFACE_TG_REPOST,
                        ]
                    ),
                )
            )
        ).scalars().all()
    tg_by_surface = {activity.surface: activity for activity in tg_activities}
    assert {
        surface: (activity.profile_key, activity.max_per_publish, activity.daily_cap)
        for surface, activity in tg_by_surface.items()
    } == {
        PROMO_SURFACE_TG_REPOST: ("kldevents->kenigevents:80stories", 1, 1),
        PROMO_SURFACE_TG_EVENT_PUBLISH: ("kldevents:80stories", 2, 2),
    }
    tg_publish = tg_by_surface[PROMO_SURFACE_TG_EVENT_PUBLISH]
    assert tg_publish.config_json["target_chat"] == "@kldevents"
    assert tg_publish.config_json["mode"] == "self_forward_existing_event_post"
    async with db.get_session() as session:
        afisha_activity = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == campaign.id,
                    PromoActivity.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT,
                )
            )
        ).scalars().one()
    assert afisha_activity.profile_key == "klgdevents:afishaengagement"
    assert afisha_activity.enabled is True
    assert afisha_activity.config_json["target_group"] == "klgdevents"
    assert afisha_activity.config_json["debug_shadow"] is True
    assert afisha_activity.config_json["apply_rate"] == 0.70
    assert afisha_activity.config_json["mechanic_weights"] == {"comments": 0, "likes": 100, "reposts": 0}
    assert afisha_activity.config_json["cta_templates"]["by_event_type"]["*"]["likes"] == [
        "Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}."
    ]
    async with db.get_session() as session:
        button_activity = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == campaign.id,
                    PromoActivity.surface == PROMO_SURFACE_TG_BUTTON_HIGHLIGHT,
                )
            )
        ).scalars().one()
    assert button_activity.enabled is True
    assert button_activity.profile_key == "kldevents:details-button"
    await db.close()


@pytest.mark.asyncio
async def test_initial_80_stories_campaign_updates_existing_afishaengagement_activity(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 10, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(Festival(name="80 историй о главном"))
        session.add(_event("Future Labeled Festival Event", "2026-06-20", festival="80 историй о главном"))
        campaign = PromoCampaign(
            title="80 историй о главном / summer visibility",
            status="active",
            starts_at=now_utc,
            ends_at=datetime(2026, 6, 13, 23, 59, 59, tzinfo=timezone.utc),
            priority=3,
        )
        session.add(campaign)
        await session.commit()
        await session.refresh(campaign)
        session.add(
            PromoActivity(
                campaign_id=int(campaign.id),
                surface=PROMO_SURFACE_AFISHA_ENGAGEMENT,
                profile_key="klgdevents:motivation:80stories",
                max_per_publish=1,
                daily_cap=5,
                selection_policy="diverse_shuffle",
                enabled=True,
                config_json={
                    "target_group": "231920894",
                    "apply_rate": 0.7,
                    "cta_templates": {
                        "by_event_type": {
                            "*": {
                                "likes": [
                                    "Поставь лайк ❤️,\nесли уже зарегистри-\nровался на {THIS_EVENT}."
                                ]
                            }
                        }
                    },
                },
            )
        )
        session.add(
            PromoActivity(
                campaign_id=int(campaign.id),
                surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
                profile_key="kldevents:80stories",
                max_per_publish=1,
                daily_cap=1,
                selection_policy="diverse_shuffle",
                enabled=True,
                config_json={
                    "target_chat": "@kldevents",
                    "window_hours": 72,
                    "active_start_hour": 9,
                    "active_end_hour": 21,
                    "campaign_scope": "80stories",
                    "mode": "self_forward_existing_event_post",
                },
            )
        )
        await session.commit()

    updated = await ensure_initial_80_stories_campaign(db, now_utc=now_utc)

    assert updated is not None
    async with db.get_session() as session:
        stored = await session.get(PromoCampaign, int(updated.id))
        activity = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == updated.id,
                    PromoActivity.surface == PROMO_SURFACE_AFISHA_ENGAGEMENT,
                    PromoActivity.enabled.is_(True),
                )
            )
        ).scalars().one()
        tg_publish = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == updated.id,
                    PromoActivity.surface == PROMO_SURFACE_TG_EVENT_PUBLISH,
                    PromoActivity.profile_key == "kldevents:80stories",
                )
            )
        ).scalars().one()
    assert stored is not None
    assert stored.priority == 1
    assert stored.ends_at.replace(tzinfo=timezone.utc) == datetime(2026, 7, 18, 23, 59, 59, tzinfo=timezone.utc)
    assert activity.profile_key == "klgdevents:afishaengagement"
    assert activity.daily_cap is None
    assert activity.config_json["target_group"] == "klgdevents"
    assert activity.config_json["cta_templates"]["by_event_type"]["*"]["likes"] == [
        "Поставь лайк ❤️, если уже зарегистрировался на {THIS_EVENT}."
    ]
    assert tg_publish.max_per_publish == 2
    assert tg_publish.daily_cap == 2
    assert tg_publish.config_json["window_hours"] == 24
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
    async with db.get_session() as session:
        session.add(ev)
        await session.commit()

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
    async with db.get_session() as session:
        session.add(ev)
        await session.commit()

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


@pytest.mark.asyncio
async def test_promo_vk_publication_runs_afishaengagement_shadow(tmp_path, monkeypatch) -> None:
    import main
    from promo import _build_promo_vk_source_post

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", False)
    monkeypatch.setattr(main, "VK_MAX_ATTACHMENTS", 10)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None):
        assert group_id == "231920894"
        assert attachments is None
        return "https://vk.com/wall-231920894_42"

    shadow_calls: list[dict] = []

    async def fake_shadow(**kwargs):
        shadow_calls.append(kwargs)
        if kwargs.get("public_only"):
            return None
        return "https://vk.com/wall-231920894_142"

    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr("afishaengagement.maybe_publish_shadow_debug_copy", fake_shadow)

    ev = _event("Фестиваль 1", "2026-06-10", festival="80 историй о главном")
    ev.id = 6101
    ev.photo_urls = ["https://example.com/promo-poster.jpg"]
    ev.photo_count = 1

    url = await _build_promo_vk_source_post(
        db,
        None,
        ev,
        campaign_id=80,
        activity_id=8,
        target_group_id=231920894,
    )

    assert url == "https://vk.com/wall-231920894_42"
    assert len(shadow_calls) == 2
    assert shadow_calls[0]["public_only"] is True
    call = shadow_calls[1]
    assert call["shadow_only"] is True
    assert call["event"] is ev
    assert call["target_group_id"] == "231920894"
    assert call["message"] == "SOURCE Фестиваль 1"
    assert call["photo_urls"] == ["https://example.com/promo-poster.jpg"]
    assert call["post_to_vk_fn"] is fake_post_to_vk
    await db.close()


@pytest.mark.asyncio
async def test_promo_vk_publication_uses_public_afishaengagement_as_primary_post(
    tmp_path, monkeypatch
) -> None:
    import main
    from promo import _build_promo_vk_source_post

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setattr(main, "VK_MAX_ATTACHMENTS", 10)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")

    async def fake_upload(group_id, photo_url, db_arg=None, bot_arg=None):
        assert photo_url == "https://example.com/promo-poster.jpg"
        return "photo-231920894_1"

    async def fail_plain_post(*args, **kwargs):
        raise AssertionError("plain VK post must not be created after public CTA succeeds")

    engagement_calls: list[dict] = []

    async def fake_engagement(**kwargs):
        engagement_calls.append(kwargs)
        assert kwargs["public_only"] is True
        assert not kwargs.get("shadow_only")
        return "https://vk.com/wall-231920894_3369"

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fail_plain_post)
    monkeypatch.setattr("afishaengagement.maybe_publish_shadow_debug_copy", fake_engagement)

    ev = _event("Великие учителя", "2026-07-04", festival="80 историй о главном")
    ev.id = 5783
    ev.source_post_url = "https://t.me/kraftmarket39/274"
    ev.photo_urls = ["https://example.com/promo-poster.jpg"]
    ev.photo_count = 1

    url = await _build_promo_vk_source_post(
        db,
        None,
        ev,
        campaign_id=1,
        activity_id=8,
        target_group_id=231920894,
    )

    assert url == "https://vk.com/wall-231920894_3369"
    assert len(engagement_calls) == 1
    call = engagement_calls[0]
    assert call["event"] is ev
    assert call["target_group_id"] == "231920894"
    assert call["message"] == "SOURCE Великие учителя"
    assert call["photo_urls"] == ["https://example.com/promo-poster.jpg"]
    await db.close()


@pytest.mark.asyncio
async def test_vk_festival_carousel_shadow_posts_hook_posters_and_cta(tmp_path, monkeypatch) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 13, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(Festival(name="Кантата"))
        first = _event("Диалог «Опережая время»", "2026-06-13", festival="Кантата")
        first.ticket_link = "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46524/2026-06-13/12:00:00"
        second = _event("Павел Третьяков и его галерея", "2026-06-13", festival="Кантата")
        second.ticket_link = "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46534/2026-06-13/13:00:00"
        second.vk_ticket_short_url = "https://vk.cc/existing"
        session.add_all([first, second])
        await session.flush()
        first_id = int(first.id)
        second_id = int(second.id)
        campaign = PromoCampaign(
            title="Кантата · образовательная программа",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 6, 16, 23, 59, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
            profile_key="klgdevents:kantata:carousel",
            max_per_publish=1,
            target_exposure_goal=1,
            daily_cap=1,
            config_json={
                "target_group": "231920894",
                "debug_shadow": True,
                "debug_marker": "#vk_festival_carousel_shadow",
                "debug_publish_delay_days": 3,
                "debug_slot_spacing_minutes": 5,
                "active_start_hour": 9,
                "active_end_hour": 21,
                "hook_variant": "registration",
                "program_phrase": "образовательную программу фестиваля «Кантата»",
                "hook_texts": {
                    "registration": "Вы уже записались на образовательную программу фестиваля «Кантата»?"
                },
                "carousel_event_ids": [first_id, second_id],
                "include_cta_card": True,
                "program_url": "https://kantatafest.ru/obrazovatelnaya-programma",
            },
        )
        session.add(activity)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="festival", festival_name="Кантата"))
        await session.commit()

    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)

    uploaded_urls: list[str] = []
    uploaded_bytes: list[tuple[str, int]] = []
    post_calls: list[dict] = []

    def sample_image_bytes() -> bytes:
        from io import BytesIO

        from PIL import Image, ImageDraw

        image = Image.new("RGB", (720, 1080), "#ece3d0")
        draw = ImageDraw.Draw(image)
        draw.rectangle((60, 60, 660, 1020), outline="#18201d", width=8)
        draw.text((120, 480), "Poster", fill="#18201d")
        out = BytesIO()
        image.save(out, format="JPEG")
        return out.getvalue()

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        if method == "utils.getShortLink":
            return {"response": {"short_url": "https://vk.cc/generated", "key": "generated"}}
        if method == "wall.get":
            return {"response": {"items": []}}
        raise AssertionError(method)

    async def fake_upload_vk_photo(group_id, photo_url, db_arg=None, bot_arg=None):
        uploaded_urls.append(photo_url)
        return f"photo-{group_id}_{len(uploaded_urls)}"

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db_arg=None, bot_arg=None, *, filename="image.jpg"):
        uploaded_bytes.append((filename, len(image_bytes)))
        return f"photo-{group_id}_bytes{len(uploaded_bytes)}"

    async def fake_fetch_image(url):
        return sample_image_bytes()

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, carousel=False, publish_date=None):
        post_calls.append(
            {
                "group_id": group_id,
                "message": message,
                "attachments": list(attachments or []),
                "carousel": carousel,
                "publish_date": publish_date,
            }
        )
        return "https://vk.com/wall-231920894_700"

    async def fail_afishaengagement(*args, **kwargs):
        raise AssertionError("vk_festival_carousel must not layer afishaengagement on top")

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload_vk_photo)
    monkeypatch.setattr(main, "upload_vk_photo_bytes", fake_upload_vk_photo_bytes)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr("afishaengagement._default_fetch_image", fake_fetch_image)
    monkeypatch.setattr("afishaengagement.maybe_publish_shadow_debug_copy", fail_afishaengagement)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)

    assert [(item.surface, item.status) for item in results] == [
        (PROMO_SURFACE_VK_FESTIVAL_CAROUSEL, "scheduled_debug")
    ]
    assert uploaded_urls == []
    assert len(uploaded_bytes) == 4
    assert [name for name, _ in uploaded_bytes] == [
        f"vk_festival_carousel_{int(activity.id)}_{first_id}_poster.jpg",
        f"vk_festival_carousel_{int(activity.id)}_{second_id}_poster.jpg",
        f"vk_festival_carousel_{int(activity.id)}_hook.jpg",
        f"vk_festival_carousel_{int(activity.id)}_cta.jpg",
    ]
    assert post_calls[0]["carousel"] is True
    assert post_calls[0]["publish_date"] is not None
    assert len(post_calls[0]["attachments"]) == 4
    assert "Вы уже записались" in post_calls[0]["message"]
    assert "vk.cc/generated" in post_calls[0]["message"]
    assert "vk.cc/existing" in post_calls[0]["message"]
    assert "https://kaliningrad.tretyakovgallery.ru/tickets/" not in post_calls[0]["message"]
    assert "[VK FESTIVAL CAROUSEL DEBUG COPY" in post_calls[0]["message"]

    async with db.get_session() as session:
        exposure = (await session.execute(select(PromoExposure))).scalars().one()
    assert exposure.surface == PROMO_SURFACE_VK_FESTIVAL_CAROUSEL
    assert exposure.publish_status == "VK_SCHEDULED_DEBUG"
    assert exposure.public_targets_json == [{"type": "vk_wall_debug", "url": "https://vk.com/wall-231920894_700"}]
    assert exposure.details_json["event_ids"] == [first_id, second_id]
    assert exposure.details_json["include_cta_card"] is True
    assert exposure.details_json["poster_swipe_badge"] is True
    assert exposure.details_json["swipe_label"] == "листай"
    assert exposure.details_json["palette_id"]
    await db.close()


@pytest.mark.asyncio
async def test_vk_festival_carousel_prod_uses_scheduled_at_after_superseded_debug(
    tmp_path,
    monkeypatch,
) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 13, 0, tzinfo=timezone.utc)
    scheduled_at = "2026-06-14T08:40:00+00:00"
    scheduled_ts = int(datetime(2026, 6, 14, 8, 40, tzinfo=timezone.utc).timestamp())

    async with db.get_session() as session:
        session.add(Festival(name="Кантата"))
        event = _event("Диалог «Опережая время»", "2026-06-14", festival="Кантата")
        session.add(event)
        await session.flush()
        event_id = int(event.id)
        campaign = PromoCampaign(
            title="Кантата · образовательная программа",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 6, 16, 23, 59, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
            profile_key="klgdevents:kantata:carousel:registration",
            max_per_publish=1,
            target_exposure_goal=1,
            daily_cap=1,
            config_json={
                "target_group": "231920894",
                "debug_shadow": False,
                "active_start_hour": 9,
                "active_end_hour": 23,
                "hook_variant": "registration",
                "program_phrase": "образовательную программу фестиваля «Кантата»",
                "carousel_event_ids": [event_id],
                "include_cta_card": True,
                "scheduled_at": scheduled_at,
            },
        )
        session.add(activity)
        await session.flush()
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=event_id))
        session.add(
            PromoExposure(
                campaign_id=int(campaign.id),
                activity_id=int(activity.id),
                event_id=event_id,
                surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
                placement_kind="vk_shadow_debug_carousel",
                publish_status="SUPERSEDED_DEBUG",
                public_target_count=1,
                public_targets_json=[{"type": "vk_wall_debug", "url": "https://vk.com/wall-231920894_1"}],
                published_at=now_utc,
                details_json={"debug_shadow": True},
            )
        )
        await session.commit()

    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    post_calls: list[dict] = []

    def sample_image_bytes() -> bytes:
        from io import BytesIO

        from PIL import Image, ImageDraw

        image = Image.new("RGB", (720, 1080), "#ece3d0")
        draw = ImageDraw.Draw(image)
        draw.text((120, 480), "Poster", fill="#18201d")
        out = BytesIO()
        image.save(out, format="JPEG")
        return out.getvalue()

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        if method == "wall.get":
            return {"response": {"items": []}}
        raise AssertionError(method)

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db_arg=None, bot_arg=None, *, filename="image.jpg"):
        return f"photo-{group_id}_{filename}"

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, carousel=False, publish_date=None):
        post_calls.append(
            {
                "group_id": group_id,
                "attachments": list(attachments or []),
                "carousel": carousel,
                "publish_date": publish_date,
            }
        )
        return "https://vk.com/wall-231920894_800"

    async def fake_fetch_image(url):
        return sample_image_bytes()

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo_bytes", fake_upload_vk_photo_bytes)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr("afishaengagement._default_fetch_image", fake_fetch_image)

    async with db.get_session() as session:
        campaign = (await session.execute(select(PromoCampaign))).scalars().one()
        activity = (await session.execute(select(PromoActivity))).scalars().one()
        target = (await session.execute(select(PromoTarget))).scalars().one()

    result = await _publish_vk_festival_carousel(
        db,
        None,
        campaign=campaign,
        activity=activity,
        target=target,
        now_utc=now_utc,
        today=now_utc.date(),
    )

    assert (result.surface, result.status, result.reason) == (
        PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
        "scheduled",
        None,
    )
    assert post_calls[0]["carousel"] is True
    assert post_calls[0]["publish_date"] == scheduled_ts

    async with db.get_session() as session:
        exposures = (
            await session.execute(select(PromoExposure).order_by(PromoExposure.id))
        ).scalars().all()
    assert [row.publish_status for row in exposures] == ["SUPERSEDED_DEBUG", "VK_SCHEDULED"]
    assert exposures[-1].placement_kind == "vk_festival_carousel"
    assert exposures[-1].public_targets_json == [{"type": "vk_wall", "url": "https://vk.com/wall-231920894_800"}]
    assert exposures[-1].details_json["debug_shadow"] is False
    assert exposures[-1].details_json["schedule"] == {
        "configured_publish_date": scheduled_at,
        "configured_publish_date_source": "iso",
    }
    await db.close()


def test_vk_festival_carousel_celebrity_variant_uses_explicit_ids() -> None:
    celebrity = _event("Творческая встреча с Иваном Никифорчиным", "2026-06-15")
    celebrity.id = 10
    film = _event("Народные художественные промыслы", "2026-06-15")
    film.id = 11

    filtered = _filter_vk_festival_carousel_events_for_variant(
        [celebrity, film],
        cfg={"celebrity_event_ids": [10]},
        hook_variant="celebrity",
    )

    assert [ev.id for ev in filtered] == [10]


def test_vk_festival_carousel_celebrity_variant_requires_explicit_signal() -> None:
    celebrity = _event("Творческая встреча с Иваном Никифорчиным", "2026-06-15")
    celebrity.id = 10
    celebrity.description = "Гостем станет дирижёр Иван Никифорчин."
    film = _event("Народные художественные промыслы", "2026-06-15")
    film.id = 11
    film.description = "Документальный сериал-путешествие о старинных российских ремёслах."

    filtered = _filter_vk_festival_carousel_events_for_variant(
        [celebrity, film],
        cfg={},
        hook_variant="celebrity",
    )

    assert [ev.id for ev in filtered] == [10]


def test_vk_festival_carousel_poster_card_preserves_bottom_without_rail() -> None:
    from io import BytesIO

    from PIL import Image, ImageDraw

    source = Image.new("RGB", (1080, 1350), "#101010")
    draw = ImageDraw.Draw(source)
    draw.rectangle((0, 1210, 1080, 1350), fill="#C0182D")
    draw.text((96, 1260), "native poster footer", fill="#ffffff")
    buf = BytesIO()
    source.save(buf, format="JPEG", quality=95)

    rendered = _render_vk_festival_carousel_poster_card(
        buf.getvalue(),
        palette_id="butter_ink_cherry",
        swipe_label="листай",
    )

    with Image.open(BytesIO(rendered)).convert("RGB") as image:
        # The bottom center must still be the poster's own footer, not a full
        # generated palette rail.
        r, g, b = image.getpixel((540, 1280))
    assert r > 120 and g < 80 and b < 90


def test_vk_festival_carousel_cta_card_has_large_down_arrow() -> None:
    from io import BytesIO

    from PIL import Image

    rendered = _render_vk_festival_carousel_card(
        "Выберите событие и записывайтесь",
        subtitle="Ссылки на регистрацию — в тексте поста",
        footer="Ссылки ниже",
        variant="cta",
        palette_id="butter_ink_cherry",
        badge_label="ЗАПИСЬ",
    )

    with Image.open(BytesIO(rendered)).convert("RGB") as image:
        center_column = [image.getpixel((540, y)) for y in range(1010, 1210, 8)]
    accent_hits = sum(1 for r, g, b in center_column if r > 120 and g < 80 and b < 90)
    assert accent_hits >= 12


def test_vk_festival_carousel_cta_rule_leaves_arrow_gap() -> None:
    from io import BytesIO

    from PIL import Image

    rendered = _render_vk_festival_carousel_card(
        "Выберите событие и записывайтесь",
        subtitle="Ссылки на регистрацию — в тексте поста",
        footer="Ссылки ниже",
        variant="cta",
        palette_id="butter_ink_cherry",
        badge_label="ЗАПИСЬ",
    )

    with Image.open(BytesIO(rendered)).convert("RGB") as image:
        y = image.height - 164
        left_rule = image.getpixel((300, y))
        right_rule = image.getpixel((780, y))
        left_gap = image.getpixel((430, y))
        right_gap = image.getpixel((650, y))

    def is_cherry(pixel: tuple[int, int, int]) -> bool:
        r, g, b = pixel
        return r > 120 and g < 80 and b < 90

    assert is_cherry(left_rule)
    assert is_cherry(right_rule)
    assert not is_cherry(left_gap)
    assert not is_cherry(right_gap)


def test_vk_festival_carousel_configured_publish_date_iso() -> None:
    now_utc = datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc)
    publish_at = "2026-06-13T08:40:00+00:00"

    publish_ts, meta = _vk_festival_carousel_configured_publish_date(
        {"scheduled_at": publish_at},
        now_utc=now_utc,
    )

    assert publish_ts == int(datetime(2026, 6, 13, 8, 40, tzinfo=timezone.utc).timestamp())
    assert meta == {"configured_publish_date": publish_at, "configured_publish_date_source": "iso"}


def test_vk_festival_carousel_configured_publish_date_ignores_past() -> None:
    publish_ts, meta = _vk_festival_carousel_configured_publish_date(
        {"scheduled_at": "2026-06-12T08:00:00+00:00"},
        now_utc=datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc),
    )

    assert publish_ts is None
    assert meta == {
        "configured_publish_date_ignored": "2026-06-12T08:00:00+00:00",
        "reason": "not_future",
    }


@pytest.mark.asyncio
async def test_vk_festival_carousel_celebrity_requires_image_evidence_config(tmp_path, monkeypatch) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 13, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        celebrity = _event("Творческая встреча с Иваном Никифорчиным", "2026-06-15", festival="Кантата")
        celebrity.description = "Гостем станет дирижёр Иван Никифорчин."
        celebrity.photo_urls = ["https://example.com/generic-festival-poster.jpg"]
        session.add(celebrity)
        await session.flush()
        campaign = PromoCampaign(
            title="Кантата · образовательная программа",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 6, 16, 23, 59, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        session.add(
            PromoActivity(
                campaign_id=int(campaign.id),
                surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
                profile_key="klgdevents:kantata:carousel:celebrity",
                max_per_publish=1,
                target_exposure_goal=1,
                daily_cap=1,
                config_json={
                    "target_group": "231920894",
                    "hook_variant": "celebrity",
                    "carousel_event_ids": [int(celebrity.id)],
                    "celebrity_event_ids": [int(celebrity.id)],
                    "celebrity_requires_image_evidence": True,
                    "debug_shadow": True,
                },
            )
        )
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="festival", festival_name="Кантата"))
        await session.commit()

    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)

    async def fail_upload(*args, **kwargs):
        raise AssertionError("generic celebrity poster must not be uploaded without image evidence config")

    monkeypatch.setattr(main, "upload_vk_photo", fail_upload)
    monkeypatch.setattr(main, "upload_vk_photo_bytes", fail_upload)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)

    assert [(item.surface, item.status, item.reason) for item in results] == [
        (PROMO_SURFACE_VK_FESTIVAL_CAROUSEL, "failed", "event_posters_missing")
    ]
    await db.close()


@pytest.mark.asyncio
async def test_vk_festival_carousel_celebrity_llm_adds_person_cards_with_budget(
    tmp_path,
    monkeypatch,
) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 13, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = _event("Диалог «Опережая время»", "2026-06-13", festival="Кантата")
        event.source_text = (
            "Модератор встречи — культурный ведущий Дарья Костина. "
            "В разговоре участвуют: Андрей Борисов — генеральный продюсер фестиваля; "
            "Фабио Мастранджело — художественный руководитель фестиваля."
        )
        missing_event = _event("Сцена как пространство трансформации", "2026-06-16", festival="Кантата")
        missing_event.source_text = (
            "Её гостем станет Евгений Князев — народный артист России, "
            "лауреат Государственной премии, ректор Театрального института им. Б. В. Щукина."
        )
        session.add_all([event, missing_event])
        await session.flush()
        event_id = int(event.id)
        missing_event_id = int(missing_event.id)
        campaign = PromoCampaign(
            title="Кантата · образовательная программа",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 6, 16, 23, 59, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        session.add(
            PromoActivity(
                campaign_id=int(campaign.id),
                surface=PROMO_SURFACE_VK_FESTIVAL_CAROUSEL,
                profile_key="klgdevents:kantata:carousel:celebrity",
                max_per_publish=1,
                target_exposure_goal=1,
                daily_cap=1,
                config_json={
                    "target_group": "231920894",
                    "hook_variant": "celebrity",
                    "hook_text": "Знаете, кто ведёт образовательную программу «Кантаты»?",
                    "carousel_event_ids": [event_id],
                    "celebrity_event_ids": [event_id],
                    "celebrity_poster_urls_by_event_id": {
                        str(event_id): "https://example.com/curated-celebrity-poster.jpg"
                    },
                    "covered_celebrity_names_by_event_id": {
                        str(event_id): ["Андрей Борисов", "Фабио Мастранджело"]
                    },
                    "celebrity_person_source_from_campaign_targets": True,
                    "max_cards": 10,
                    "include_cta_card": True,
                    "debug_shadow": True,
                },
            )
        )
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=event_id))
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=missing_event_id))
        await session.commit()

    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)

    post_calls: list[dict] = []
    uploaded_bytes: list[str] = []
    llm_prompts: list[str] = []

    def sample_image_bytes() -> bytes:
        from io import BytesIO

        from PIL import Image

        image = Image.new("RGB", (720, 1080), "#ece3d0")
        out = BytesIO()
        image.save(out, format="JPEG")
        return out.getvalue()

    async def fake_vk_api(method, params, db_arg=None, bot_arg=None, **kwargs):
        if method == "wall.get":
            return {"response": {"items": []}}
        if method == "utils.getShortLink":
            return {"response": {"short_url": "https://vk.cc/generated"}}
        raise AssertionError(method)

    async def fake_fetch_image(url):
        return sample_image_bytes()

    async def fake_upload_vk_photo_bytes(group_id, image_bytes, db_arg=None, bot_arg=None, *, filename="image.jpg"):
        uploaded_bytes.append(filename)
        return f"photo-{group_id}_bytes{len(uploaded_bytes)}"

    async def fake_upload_vk_photo(*args, **kwargs):
        raise AssertionError("celebrity carousel must use configured poster bytes")

    async def fake_post_to_vk(group_id, message, db_arg=None, bot_arg=None, attachments=None, carousel=False, publish_date=None):
        post_calls.append(
            {
                "message": message,
                "attachments": list(attachments or []),
                "carousel": carousel,
                "publish_date": publish_date,
            }
        )
        return "https://vk.com/wall-231920894_701"

    async def fake_ask_4o(prompt, **kwargs):
        llm_prompts.append(prompt)
        return """
        {
          "cards": [
            {"name": "Андрей Борисов", "role": "генеральный продюсер фестиваля «Кантата»", "event_id": 1, "evidence": "Андрей Борисов — генеральный продюсер"},
            {"name": "Евгений Князев", "role": "народный артист России", "event_id": 2, "evidence": "Евгений Князев — народный артист России"},
            {"name": "Татьяна Юденкова", "role": "доктор искусствоведения", "event_id": 3, "evidence": "Татьяна Юденкова — доктор искусствоведения"},
            {"name": "Максим Шостакович", "role": "продюсер Большого театра России", "event_id": 4, "evidence": "Максим Шостакович, продюсер"},
            {"name": "Дарья Костина", "role": "культурный ведущий и модератор", "event_id": 1, "evidence": "Модератор встречи — Дарья Костина"},
            {"name": "Иван Никифорчин", "role": "дирижёр", "event_id": 5, "evidence": "Иван Никифорчин"},
            {"name": "Фабио Мастранджело", "role": "художественный руководитель фестиваля", "event_id": 1, "evidence": "Фабио Мастранджело"},
            {"name": "Марк Ваза", "role": "деятель искусства", "event_id": 1, "evidence": "Марк Ваза"},
            {"name": "Наталья Патрушева", "role": "деятель искусства", "event_id": 1, "evidence": "Наталья Патрушева"}
          ]
        }
        """

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload_vk_photo)
    monkeypatch.setattr(main, "upload_vk_photo_bytes", fake_upload_vk_photo_bytes)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr(main, "ask_4o", fake_ask_4o)
    monkeypatch.setattr("afishaengagement._default_fetch_image", fake_fetch_image)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)

    assert [(item.surface, item.status) for item in results] == [
        (PROMO_SURFACE_VK_FESTIVAL_CAROUSEL, "scheduled_debug")
    ]
    assert post_calls[0]["carousel"] is True
    assert len(post_calls[0]["attachments"]) == 9
    assert len(uploaded_bytes) == 9
    assert sum("_person_" in name for name in uploaded_bytes) == 6
    assert all("person_7" not in name for name in uploaded_bytes)
    assert any("This event without poster card: True" in prompt for prompt in llm_prompts)
    assert any("Евгений Князев" in prompt for prompt in llm_prompts)

    async with db.get_session() as session:
        exposure = (await session.execute(select(PromoExposure))).scalars().one()
    assert exposure.details_json["max_cards"] == 9
    assert exposure.details_json["attachments_count"] == 9
    assert exposure.details_json["person_cards_source"] == "llm_per_event"
    assert [item["name"] for item in exposure.details_json["person_cards"]] == [
        "Евгений Князев",
        "Татьяна Юденкова",
        "Максим Шостакович",
        "Дарья Костина",
        "Иван Никифорчин",
        "Марк Ваза",
    ]
    await db.close()


@pytest.mark.asyncio
async def test_promo_vk_publication_uses_projection_as_is_and_blocks_empty_upload(tmp_path, monkeypatch) -> None:
    import main
    from promo import _build_promo_vk_source_post

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    managed = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/80/"
        "8001c001000c9c09430561ac78e858358b0706a338e534c498c0d06819000800.webp"
    )
    vk_cdn = "https://sun9-78.userapi.com/s/v1/ig2/source-copy.jpg?cs=1080x0"
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setattr(main, "VK_MAX_ATTACHMENTS", 10)
    monkeypatch.setattr(main, "build_vk_source_message", lambda ev, text, festival=None: f"SOURCE {ev.title}")

    upload_calls: list[str] = []

    async def fake_upload(group_id, photo_url, db_arg=None, bot_arg=None):
        upload_calls.append(photo_url)
        return None

    async def fake_post_to_vk(*args, **kwargs):
        raise AssertionError("promo VK publication must fail closed when media upload is empty")

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)

    ev = _event("Благотворительный концерт", "2026-06-11")
    ev.id = 5282
    ev.source_post_url = "https://vk.com/wall-214027639_11341"
    ev.photo_urls = [managed, vk_cdn]
    ev.photo_count = 2

    with pytest.raises(RuntimeError, match="vk_sync_missing_media_for_telegram_event"):
        await _build_promo_vk_source_post(
            db,
            None,
            ev,
            campaign_id=1,
            activity_id=8,
            target_group_id=231920894,
        )

    assert upload_calls == [managed, vk_cdn]
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

    vk_results = [item for item in results if item.surface == PROMO_SURFACE_VK_PUBLICATION]
    assert [item.status for item in vk_results] == ["failed", "scheduled"]
    async with db.get_session() as session:
        exposures = (await session.execute(select(PromoExposure).order_by(PromoExposure.id))).scalars().all()
    assert [row.publish_status for row in exposures] == ["FAILED_NO_MEDIA", "VK_SCHEDULED"]
    assert exposures[0].public_target_count == 0
    assert exposures[0].public_targets_json == []
    assert exposures[0].details_json["action"] == "investigate_source_media_and_rehydrate_before_publication"
    await db.close()


@pytest.mark.asyncio
async def test_promo_target_skips_same_day_event_after_start(tmp_path) -> None:
    from promo import _events_for_target

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 24, 12, 30, tzinfo=timezone.utc)  # 14:30 Kaliningrad
    async with db.get_session() as session:
        campaign = PromoCampaign(
            title="80 stories",
            status="active",
            starts_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        )
        session.add(campaign)
        await session.flush()
        target = PromoTarget(
            campaign_id=int(campaign.id),
            target_type="festival",
            festival_name="80 историй о главном",
        )
        past_start = _event("Already started", "2026-06-24", festival="80 историй о главном")
        past_start.time = "13:00"
        future_start = _event("Still upcoming", "2026-06-24", festival="80 историй о главном")
        future_start.time = "18:30"
        session.add_all([target, past_start, future_start])
        await session.commit()

    matched = await _events_for_target(
        db,
        target=target,
        campaign=campaign,
        today=date(2026, 6, 24),
        now_utc=now_utc,
    )

    assert [event.title for event in matched] == ["Still upcoming"]
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
async def test_daily_recommend_today_appends_summary_block(tmp_path) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 7, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = _event("Лекция о Третьякове", "2026-06-13", festival="Кантата")
        event.time = "13:00"
        event.telegraph_url = "https://telegra.ph/kantata-lecture"
        fallback = _event("Общий анонс", "2026-06-13", festival="Кантата")
        session.add_all([event, fallback])
        await session.flush()
        campaign = PromoCampaign(
            title="Кантата · образование",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=now_utc.replace(day=16),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_DAILY_RECOMMEND_TODAY,
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "preferred_event_ids_by_date": {
                    "2026-06-13": [int(event.id), int(fallback.id)]
                }
            },
        )
        session.add(activity)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(event.id)))
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(fallback.id)))
        await session.commit()

    posts = await main.build_daily_posts(
        db,
        timezone.utc,
        now=now_utc,
    )
    text = posts[0][0]

    assert "ИТОГО РЕКОМЕНДУЕМ ПОСЕТИТЬ СЕГОДНЯ" in text
    assert '• <a href="https://telegra.ph/kantata-lecture">Лекция о Третьякове</a> — 13:00, Venue' in text
    assert text.index("ИТОГО РЕКОМЕНДУЕМ") < text.index("#Афиша_Калининград")

    recorded = await record_daily_promo_recommendation_exposures(db, now_utc=now_utc)
    assert recorded == 1
    recorded_again = await record_daily_promo_recommendation_exposures(db, now_utc=now_utc)
    assert recorded_again == 0
    await db.close()


@pytest.mark.asyncio
async def test_promo_runner_publishes_and_reposts_telegram_event(tmp_path, monkeypatch) -> None:
    import main
    from types import SimpleNamespace

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 17, 30, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = _event("Кантата лекция", "2026-06-14", festival="Кантата")
        session.add(event)
        await session.flush()
        event_id = int(event.id)
        campaign = PromoCampaign(
            title="Кантата · образование",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=now_utc.replace(day=16),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        publish = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
            profile_key="kldevents",
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "target_chat": "@kldevents",
                "active_start_hour": 18,
                "active_end_hour": 20,
            },
        )
        repost = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_REPOST,
            profile_key="kldevents->kenigevents",
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "source_chat": "@kldevents",
                "target_chat": "@kenigevents",
                "active_start_hour": 18,
                "active_end_hour": 20,
                "dedup_hours": 72,
            },
        )
        session.add_all([publish, repost])
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=event_id))
        await session.commit()

    async def fake_publish(ev, db_arg, bot_arg, *, target_chat):
        assert target_chat == "@kldevents"
        return f"https://t.me/kldevents/{1000 + int(ev.id)}"

    class DummyBot:
        def __init__(self) -> None:
            self.forwarded: list[tuple[str, str, int]] = []

        async def forward_message(self, *, chat_id, from_chat_id, message_id):
            self.forwarded.append((chat_id, from_chat_id, message_id))
            return SimpleNamespace(message_id=2000 + message_id)

    bot = DummyBot()
    monkeypatch.setattr(main, "publish_tg_promo_event_publication", fake_publish)

    results = await run_promo_vk_activities(db, bot, now_utc=now_utc)

    assert [(item.surface, item.status) for item in results] == [
        (PROMO_SURFACE_TG_EVENT_PUBLISH, "published"),
        (PROMO_SURFACE_TG_REPOST, "published"),
    ]
    assert bot.forwarded == [("@kenigevents", "@kldevents", 1000 + event_id)]
    async with db.get_session() as session:
        exposures = (await session.execute(select(PromoExposure).order_by(PromoExposure.id))).scalars().all()
        saved = await session.get(Event, event_id)
    assert [row.surface for row in exposures] == [
        PROMO_SURFACE_TG_EVENT_PUBLISH,
        PROMO_SURFACE_TG_REPOST,
    ]
    assert exposures[1].details_json["source_url"] == f"https://t.me/kldevents/{1000 + event_id}"
    assert saved.tg_event_post_url == f"https://t.me/kldevents/{1000 + event_id}"
    await db.close()


@pytest.mark.asyncio
async def test_tg_event_publish_self_forwards_existing_source_post(tmp_path, monkeypatch) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 14, 30, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = _event("Кантата с уже опубликованным постом", "2026-06-14", festival="Кантата")
        event.tg_event_post_url = "https://t.me/kldevents/777"
        event.tg_event_post_id = 777
        event.tg_event_post_mode = "smart_update"
        session.add(event)
        await session.flush()
        event_id = int(event.id)
        campaign = PromoCampaign(
            title="Кантата · source amplification",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=now_utc.replace(day=16),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
            profile_key="kldevents",
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "target_chat": "@kldevents",
                "active_start_hour": 10,
                "active_end_hour": 20,
            },
        )
        session.add(activity)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=event_id))
        await session.commit()

    published_ids: list[int] = []

    async def fake_publish(ev, db_arg, bot_arg, *, target_chat):
        published_ids.append(int(ev.id))
        return f"https://t.me/kldevents/{1000 + int(ev.id)}"

    class DummyBot:
        def __init__(self) -> None:
            self.forwarded: list[tuple[str, str, int]] = []

        async def forward_message(self, *, chat_id, from_chat_id, message_id):
            self.forwarded.append((chat_id, from_chat_id, message_id))
            return SimpleNamespace(message_id=1777)

    bot = DummyBot()
    monkeypatch.setattr(main, "publish_tg_promo_event_publication", fake_publish)

    results = await run_promo_vk_activities(db, bot, now_utc=now_utc)

    assert [(item.surface, item.status, item.event_id, item.source_url, item.target_url) for item in results] == [
        (
            PROMO_SURFACE_TG_EVENT_PUBLISH,
            "forwarded",
            event_id,
            "https://t.me/kldevents/777",
            "https://t.me/kldevents/1777",
        )
    ]
    assert bot.forwarded == [("@kldevents", "@kldevents", 777)]
    assert published_ids == []
    async with db.get_session() as session:
        exposure = (await session.execute(select(PromoExposure))).scalars().one()
        saved = await session.get(Event, event_id)
    assert exposure.surface == PROMO_SURFACE_TG_EVENT_PUBLISH
    assert exposure.publish_status == "TG_FORWARDED"
    assert exposure.placement_kind == "rolling_window_self_forward"
    assert exposure.details_json["source_url"] == "https://t.me/kldevents/777"
    assert exposure.details_json["target_url"] == "https://t.me/kldevents/1777"
    assert exposure.details_json["mode"] == "self_forward_existing_event_post"
    assert saved.tg_event_post_url == "https://t.me/kldevents/777"
    assert saved.tg_event_post_mode == "smart_update"
    await db.close()


@pytest.mark.asyncio
async def test_tg_event_publish_counts_recent_organic_smart_update_post(tmp_path, monkeypatch) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 14, 30, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = _event("Кантата уже закрыла слот органикой", "2026-06-14", festival="Кантата")
        event.tg_event_post_url = "https://t.me/kldevents/778"
        event.tg_event_post_id = 778
        event.tg_event_post_mode = "smart_update"
        session.add(event)
        await session.flush()
        event_id = int(event.id)
        campaign = PromoCampaign(
            title="Кантата · organic count",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=now_utc.replace(day=16),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
            profile_key="kldevents",
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "target_chat": "@kldevents",
                "active_start_hour": 10,
                "active_end_hour": 20,
            },
        )
        session.add(activity)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=event_id))
        session.add(
            JobOutbox(
                event_id=event_id,
                task=JobTask.tg_event_publish,
                status=JobStatus.done,
                updated_at=now_utc,
                next_run_at=now_utc,
            )
        )
        await session.commit()

    class DummyBot:
        async def forward_message(self, **kwargs):
            raise AssertionError("recent organic Telegram post must satisfy the due slot")

    async def fake_publish(ev, db_arg, bot_arg, *, target_chat):
        raise AssertionError("recent organic Telegram post must suppress duplicate publication")

    monkeypatch.setattr(main, "publish_tg_promo_event_publication", fake_publish)

    results = await run_promo_vk_activities(db, DummyBot(), now_utc=now_utc)

    assert results == []
    async with db.get_session() as session:
        exposures = (await session.execute(select(PromoExposure))).scalars().all()
    assert exposures == []
    await db.close()


@pytest.mark.asyncio
async def test_promo_runner_sends_vk_channel_manual_draft_nonpublic(tmp_path, monkeypatch) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 17, 30, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = _event("Завтрашняя история", "2026-06-14", festival="Кантата")
        event.telegraph_url = "https://telegra.ph/story"
        event.ticket_link = "https://example.com/register"
        session.add(event)
        await session.flush()
        event_id = int(event.id)
        later = _event("Поздняя история", "2026-07-04", festival="Кантата")
        later.ticket_link = "https://example.com/later"
        session.add(later)
        campaign = PromoCampaign(
            title="VK channel smoke",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=now_utc.replace(day=16),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VK_CHANNEL_PUBLISH,
            profile_key="klgdevents:vk_channel",
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "target_group": "klgdevents",
                "target_channel": "Полюбить Калининград Афиша",
                "peer_id": 2000000123,
                "delivery_mode": "vk_messages_manual_copy_draft",
                "active_start_hour": 18,
                "active_end_hour": 20,
            },
        )
        session.add(activity)
        session.add(
            PromoTarget(
                campaign_id=int(campaign.id),
                target_type="festival",
                festival_name="Кантата",
            )
        )
        await session.commit()

    async def fake_resolve(ref: str):
        assert ref == "klgdevents"
        return 111, "Events", "klgdevents", "group"

    async def fake_publish(ev, db_arg, bot_arg, **kwargs):
        assert int(ev.id) == event_id
        assert kwargs["target_group_id"] == 111
        assert kwargs["peer_ids"] == [2000000123]
        assert kwargs["channel_ref"] == "Полюбить Калининград Афиша"
        return f"https://vk.com/im?sel=2000000123&msgid={3000 + int(ev.id)}"

    monkeypatch.setattr(main, "vk_resolve_group", fake_resolve)
    monkeypatch.setattr(main, "publish_vk_channel_promo_event_publication", fake_publish)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)

    assert [(item.surface, item.status, item.event_id) for item in results] == [
        (PROMO_SURFACE_VK_CHANNEL_PUBLISH, "draft_sent", event_id)
    ]
    async with db.get_session() as session:
        exposures = (await session.execute(select(PromoExposure))).scalars().all()
    assert len(exposures) == 1
    exposure = exposures[0]
    assert exposure.publish_status == "VK_CHANNEL_DRAFT_SENT"
    assert exposure.placement_kind == "manual_copy_channel_draft"
    assert exposure.public_target_count == 0
    assert exposure.public_targets_json == []
    assert exposure.details_json["manual_copy_draft"] is True
    assert exposure.details_json["target_url"] == f"https://vk.com/im?sel=2000000123&msgid={3000 + event_id}"
    await db.close()


def test_vk_channel_manual_draft_prefers_registration_link_over_telegraph() -> None:
    import main

    event = _event("Завтрашняя история", "2026-06-14", festival="Кантата")
    event.telegraph_url = "https://telegra.ph/story"
    event.ticket_link = "https://example.com/register"

    message = main.build_vk_channel_promo_event_publication_message(event)

    assert "https://example.com/register" in message
    assert "https://telegra.ph/story" not in message


def test_vk_channel_manual_draft_refuses_80_stories_telegraph_fallback() -> None:
    import main

    event = _event("История без ссылки", "2026-07-07", festival="80 историй о главном")
    event.source_text = "Открыли регистрацию на лекцию. Бесплатно, по регистрации."
    event.telegraph_url = "https://telegra.ph/story"
    event.ticket_link = None

    with pytest.raises(main.VkChannelManualDraftMissingRegistrationLink):
        main.build_vk_channel_promo_event_publication_message(event)


def test_vk_channel_manual_draft_extracts_registration_link_from_source_text() -> None:
    import main

    event = _event("История со ссылкой", "2026-07-07", festival="80 историй о главном")
    event.source_text = (
        "Открыли регистрацию: "
        "https://kgd80.ru/sobytiya/example/?register=1"
    )
    event.telegraph_url = "https://telegra.ph/story"
    event.ticket_link = None

    message = main.build_vk_channel_promo_event_publication_message(event)

    assert "https://kgd80.ru/sobytiya/example/?register=1" in message
    assert "https://telegra.ph/story" not in message


@pytest.mark.asyncio
async def test_vk_channel_manual_draft_sends_poster_attachment(monkeypatch) -> None:
    import main

    event = _event("С афишей", "2026-06-14", festival="Кантата")
    event.id = 123
    event.ticket_link = "https://example.com/register"
    event.photo_urls = ["https://example.com/poster.jpg"]
    sent: dict[str, object] = {}

    async def fake_upload_vk_message_photo(**kwargs):
        assert kwargs["peer_id"] == 868977531
        assert kwargs["photo_url"] == "https://example.com/poster.jpg"
        assert kwargs["token"] == "user-token"
        assert kwargs["token_kind"] == "user"
        return "photo1_2_access"

    async def fake_vk_api(method, params, db, bot, **kwargs):
        assert method == "messages.send"
        sent.update(params)
        assert kwargs["token"] == "user-token"
        assert kwargs["token_kind"] == "user"
        return {"response": 265}

    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")
    monkeypatch.setattr(main, "_upload_vk_message_photo", fake_upload_vk_message_photo)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.publish_vk_channel_promo_event_publication(
        event,
        None,
        None,
        target_group_id=231920894,
        peer_ids=[868977531],
        channel_ref="Полюбить Калининград Афиша",
    )

    assert url == "https://vk.com/im?sel=868977531&msgid=265"
    assert sent["attachment"] == "photo1_2_access"
    assert sent["dont_parse_links"] == 0
    assert "https://example.com/register" in str(sent["message"])


@pytest.mark.asyncio
async def test_promo_tg_repost_skips_event_inside_four_hour_lead_time(tmp_path, monkeypatch) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 17, 30, tzinfo=timezone.utc)

    async with db.get_session() as session:
        soon = _event("Скоро начнётся", "2026-06-13", festival="Кантата")
        soon.time = "20:00"  # 2.5 hours away in Europe/Kaliningrad at now_utc.
        soon.tg_event_post_url = "https://t.me/kldevents/901"
        soon.tg_event_post_id = 901
        later = _event("Завтра", "2026-06-14", festival="Кантата")
        later.tg_event_post_url = "https://t.me/kldevents/902"
        later.tg_event_post_id = 902
        session.add_all([soon, later])
        await session.flush()
        campaign = PromoCampaign(
            title="Кантата · репост",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 6, 16, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_REPOST,
            profile_key="kldevents->kenigevents",
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "source_chat": "@kldevents",
                "target_chat": "@kenigevents",
                "active_start_hour": 18,
                "active_end_hour": 20,
                "dedup_hours": 72,
            },
        )
        session.add(activity)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(soon.id)))
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="event", event_id=int(later.id)))
        await session.commit()
        later_id = int(later.id)

    class DummyBot:
        def __init__(self) -> None:
            self.forwarded: list[tuple[str, str, int]] = []

        async def forward_message(self, *, chat_id, from_chat_id, message_id):
            self.forwarded.append((chat_id, from_chat_id, message_id))
            return SimpleNamespace(message_id=2000 + message_id)

    monkeypatch.setattr(main, "publish_tg_promo_event_publication", None, raising=False)
    bot = DummyBot()

    results = await run_promo_vk_activities(db, bot, now_utc=now_utc)

    assert [(item.surface, item.status, item.event_id) for item in results] == [
        (PROMO_SURFACE_TG_REPOST, "published", later_id)
    ]
    assert bot.forwarded == [("@kenigevents", "@kldevents", 902)]
    await db.close()


@pytest.mark.asyncio
async def test_tg_event_publish_honors_preferred_ids_by_date(tmp_path, monkeypatch) -> None:
    import main
    from promo import _stable_shuffle_key

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 14, 30, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(Festival(name="Кантата"))
        first = _event("Shuffle first", "2026-06-13", festival="Кантата")
        preferred = _event("Preferred today", "2026-06-13", festival="Кантата")
        session.add_all([first, preferred])
        campaign = PromoCampaign(
            title="Кантата · preferred",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 6, 16, 23, 59, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_EVENT_PUBLISH,
            profile_key="kldevents",
            max_per_publish=3,
            daily_cap=3,
            config_json={
                "target_chat": "@kldevents",
                "active_start_hour": 10,
                "active_end_hour": 17,
            },
        )
        session.add(activity)
        await session.flush()
        ids = [int(first.id), int(preferred.id)]
        default_first = min(
            ids,
            key=lambda event_id: _stable_shuffle_key(
                int(campaign.id), int(activity.id), now_utc.date().isoformat(), event_id
            ),
        )
        preferred_id = next(event_id for event_id in ids if event_id != default_first)
        activity.config_json = {
            **activity.config_json,
            "preferred_event_ids_by_date": {now_utc.date().isoformat(): [preferred_id]},
        }
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type="festival", festival_name="Кантата"))
        await session.commit()

    published_ids: list[int] = []

    async def fake_publish(ev, db_arg, bot_arg, *, target_chat):
        published_ids.append(int(ev.id))
        return f"https://t.me/kldevents/{ev.id}"

    monkeypatch.setattr(main, "publish_tg_promo_event_publication", fake_publish)

    results = await run_promo_vk_activities(db, object(), now_utc=now_utc)
    second_results = await run_promo_vk_activities(db, object(), now_utc=now_utc)

    assert [(item.surface, item.status, item.event_id) for item in results] == [
        (PROMO_SURFACE_TG_EVENT_PUBLISH, "published", preferred_id)
    ]
    assert second_results == []
    assert published_ids == [preferred_id]
    await db.close()


@pytest.mark.asyncio
async def test_tg_repost_weighted_popularity_uses_owned_vk_boost_and_tme_c_source(
    tmp_path, monkeypatch
) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 13, 13, 30, tzinfo=timezone.utc)
    owned_group_id = 231920894

    async with db.get_session() as session:
        internet_popular_missing_post = _event("Internet popular but no kldevents post", "2026-06-15")
        owned_popular = _event("Owned audience winner", "2026-06-15")
        owned_popular.tg_event_post_url = "https://t.me/c/3954607218/77"
        owned_popular.tg_event_post_id = 77
        owned_popular.source_vk_post_url = f"https://vk.com/wall-{owned_group_id}_10"
        source_popular = _event("Internet source fallback", "2026-06-15")
        source_popular.tg_event_post_url = "https://t.me/kldevents/88"
        source_popular.tg_event_post_id = 88
        session.add_all([internet_popular_missing_post, owned_popular, source_popular])
        await session.flush()
        missing_id = int(internet_popular_missing_post.id)
        owned_id = int(owned_popular.id)
        source_id = int(source_popular.id)

        campaign = PromoCampaign(
            title="Popular TG reposts",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 6, 30, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_REPOST,
            profile_key="kldevents->kenigevents",
            max_per_publish=1,
            daily_cap=1,
            selection_policy=PROMO_POLICY_WEIGHTED_POPULARITY,
            config_json={
                "source_chat": "@kldevents",
                "target_chat": "@kenigevents",
                "active_start_hour": 10,
                "active_end_hour": 20,
                "dedup_hours": 72,
                "selection_policy": PROMO_POLICY_WEIGHTED_POPULARITY,
                "owned_vk_group_ids": [owned_group_id, 231828790],
                "owned_vk_popularity_weight": 4,
                "popularity_window_days": 7,
                "popularity_preferred_age_day": 0,
            },
        )
        session.add(activity)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type=PROMO_TARGET_TYPE_ALL))
        session.add(
            EventSource(
                event_id=missing_id,
                source_type="telegram",
                source_url="https://t.me/weighted_source/1",
                source_chat_username="weighted_source",
                source_message_id=1,
            )
        )
        session.add(
            EventSource(
                event_id=source_id,
                source_type="telegram",
                source_url="https://t.me/weighted_source/3",
                source_chat_username="weighted_source",
                source_message_id=3,
            )
        )
        await session.commit()

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "INSERT INTO telegram_source(username, title, enabled) VALUES(?,?,1)",
            ("weighted_source", "Weighted Source"),
        )
        weighted_source_id = int(cur.lastrowid)
        for message_id, views, likes in [
            (1, 1000, 100),  # would win, but has no @kldevents/t.me/c post to forward
            (2, 10, 1),
            (3, 50, 5),
        ]:
            await conn.execute(
                """
                INSERT INTO telegram_scanned_message(source_id, message_id, status, events_extracted, events_imported)
                VALUES(?,?,?,?,?)
                """,
                (weighted_source_id, message_id, "imported", 1, 1),
            )
            await conn.execute(
                """
                INSERT INTO telegram_post_metric(source_id, message_id, age_day, source_url, message_ts, collected_ts, views, likes)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    weighted_source_id,
                    message_id,
                    0,
                    f"https://t.me/weighted_source/{message_id}",
                    int(now_utc.timestamp()) - 3600,
                    int(now_utc.timestamp()),
                    views,
                    likes,
                ),
            )
        for post_id, views, likes in [
            (10, 300, 30),  # owned_popular gets 4x weighted owned signal
            (11, 20, 2),
        ]:
            await conn.execute(
                """
                INSERT INTO vk_post_metric(group_id, post_id, age_day, source_url, post_ts, collected_ts, views, likes)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    owned_group_id,
                    post_id,
                    0,
                    f"https://vk.com/wall-{owned_group_id}_{post_id}",
                    int(now_utc.timestamp()) - 3600,
                    int(now_utc.timestamp()),
                    views,
                    likes,
                ),
            )
        await conn.commit()

    class DummyBot:
        def __init__(self) -> None:
            self.forwarded: list[tuple[str, str, int]] = []

        async def forward_message(self, *, chat_id, from_chat_id, message_id):
            self.forwarded.append((chat_id, from_chat_id, message_id))
            return SimpleNamespace(message_id=2000 + int(message_id))

    monkeypatch.setattr(main, "publish_tg_promo_event_publication", None, raising=False)
    bot = DummyBot()

    results = await run_promo_vk_activities(db, bot, now_utc=now_utc)

    assert [(item.surface, item.status, item.event_id) for item in results] == [
        (PROMO_SURFACE_TG_REPOST, "published", owned_id)
    ]
    assert bot.forwarded == [("@kenigevents", "@kldevents", 77)]
    async with db.get_session() as session:
        exposure = (await session.execute(select(PromoExposure))).scalars().one()
    assert exposure.details_json["source_url"] == "https://t.me/c/3954607218/77"
    assert exposure.details_json["popularity_score"] > exposure.details_json["source_popularity_score"]
    assert exposure.details_json["owned_vk_popularity_score"] > 0
    await db.close()


@pytest.mark.asyncio
async def test_tg_repost_weighted_popularity_prefers_diverse_title_within_week(
    tmp_path, monkeypatch
) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc)

    async with db.get_session() as session:
        previous_alice = _event("Мюзикл «Алиса в Стране чудес»", "2026-07-08")
        previous_alice.tg_event_post_url = "https://t.me/kldevents/420"
        previous_alice.tg_event_post_id = 420
        alice_again = _event("Мюзикл «Алиса в Стране чудес»", "2026-07-11")
        alice_again.tg_event_post_url = "https://t.me/kldevents/421"
        alice_again.tg_event_post_id = 421
        different = _event("Концерт органной музыки", "2026-07-12")
        different.tg_event_post_url = "https://t.me/kldevents/422"
        different.tg_event_post_id = 422
        session.add_all([previous_alice, alice_again, different])
        await session.flush()
        previous_id = int(previous_alice.id)
        alice_again_id = int(alice_again.id)
        different_id = int(different.id)
        campaign = PromoCampaign(
            title="Popular TG reposts",
            status="active",
            starts_at=now_utc.replace(hour=0),
            ends_at=datetime(2026, 7, 30, tzinfo=timezone.utc),
            priority=0,
        )
        session.add(campaign)
        await session.flush()
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_TG_REPOST,
            profile_key="kldevents->kenigevents",
            max_per_publish=1,
            daily_cap=1,
            selection_policy=PROMO_POLICY_WEIGHTED_POPULARITY,
            config_json={
                "source_chat": "@kldevents",
                "target_chat": "@kenigevents",
                "active_start_hour": 9,
                "active_end_hour": 21,
                "dedup_hours": 72,
                "repeat_cooldown_days": 7,
                "selection_policy": PROMO_POLICY_WEIGHTED_POPULARITY,
                "popularity_window_days": 7,
            },
        )
        session.add(activity)
        session.add(PromoTarget(campaign_id=int(campaign.id), target_type=PROMO_TARGET_TYPE_ALL))
        await session.flush()
        activity_id = int(activity.id)
        session.add(
            PromoExposure(
                campaign_id=int(campaign.id),
                activity_id=activity_id,
                event_id=previous_id,
                surface=PROMO_SURFACE_TG_REPOST,
                placement_kind="rolling_window_repost",
                publish_status="TG_FORWARDED",
                public_target_count=1,
                public_targets_json=[{"type": "telegram_forward", "url": "https://t.me/kenigevents/1"}],
                published_at=now_utc.replace(day=28),
                details_json={
                    "source_url": "https://t.me/kldevents/420",
                    "target_url": "https://t.me/kenigevents/1",
                },
            )
        )
        for event_id, source_message_id in [(alice_again_id, 421), (different_id, 422)]:
            session.add(
                EventSource(
                    event_id=event_id,
                    source_type="telegram",
                    source_url=f"https://t.me/weighted_source/{source_message_id}",
                    source_chat_username="weighted_source",
                    source_message_id=source_message_id,
                )
            )
        await session.commit()

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "INSERT INTO telegram_source(username, title, enabled) VALUES(?,?,1)",
            ("weighted_source", "Weighted Source"),
        )
        source_id = int(cur.lastrowid)
        for message_id, views, likes in [
            (421, 1000, 100),
            (422, 20, 2),
        ]:
            await conn.execute(
                """
                INSERT INTO telegram_scanned_message(source_id, message_id, status, events_extracted, events_imported)
                VALUES(?,?,?,?,?)
                """,
                (source_id, message_id, "imported", 1, 1),
            )
            await conn.execute(
                """
                INSERT INTO telegram_post_metric(source_id, message_id, age_day, source_url, message_ts, collected_ts, views, likes)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    source_id,
                    message_id,
                    0,
                    f"https://t.me/weighted_source/{message_id}",
                    int(now_utc.timestamp()) - 3600,
                    int(now_utc.timestamp()),
                    views,
                    likes,
                ),
            )
        await conn.commit()

    class DummyBot:
        def __init__(self) -> None:
            self.forwarded: list[tuple[str, str, int]] = []

        async def forward_message(self, *, chat_id, from_chat_id, message_id):
            self.forwarded.append((chat_id, from_chat_id, message_id))
            return SimpleNamespace(message_id=5000 + int(message_id))

    monkeypatch.setattr(main, "publish_tg_promo_event_publication", None, raising=False)
    bot = DummyBot()

    results = await run_promo_vk_activities(db, bot, now_utc=now_utc)

    assert [(item.surface, item.status, item.event_id) for item in results] == [
        (PROMO_SURFACE_TG_REPOST, "published", different_id)
    ]
    assert bot.forwarded == [("@kenigevents", "@kldevents", 422)]
    async with db.get_session() as session:
        exposure = (
            await session.execute(
                select(PromoExposure).where(PromoExposure.event_id == different_id)
            )
        ).scalars().one()
    assert exposure.details_json["repeat_key"] == "концерт органной музыки"
    assert exposure.details_json["repeat_cooldown_bypassed"] is False
    await db.close()


@pytest.mark.asyncio
async def test_promo_vk_repost_uses_local_day_count_not_rolling_window(
    tmp_path, monkeypatch
) -> None:
    import main

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 10, 13, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = _event("Сегодняшняя лекция", "2026-06-11")
        event.source_vk_post_url = "https://vk.com/wall-111_20"
        session.add(event)
        campaign = PromoCampaign(
            title="VK repost local day",
            status="active",
            starts_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            ends_at=datetime(2026, 6, 30, tzinfo=timezone.utc),
        )
        session.add(campaign)
        await session.flush()
        target = PromoTarget(
            campaign_id=int(campaign.id),
            target_type="event",
            event_id=int(event.id),
        )
        activity = PromoActivity(
            campaign_id=int(campaign.id),
            surface=PROMO_SURFACE_VK_REPOST,
            profile_key="klgdevents->kenigeventsofficial",
            max_per_publish=1,
            daily_cap=1,
            config_json={
                "source_group": "klgdevents",
                "target_group": "kenigeventsofficial",
                "window_hours": 24,
                "active_start_hour": 9,
                "active_end_hour": 21,
                "dedup_hours": 72,
            },
        )
        session.add_all([target, activity])
        await session.commit()
        await session.refresh(activity)
        yesterday_repost = PromoExposure(
            campaign_id=int(campaign.id),
            activity_id=int(activity.id),
            event_id=int(event.id),
            surface=PROMO_SURFACE_VK_REPOST,
            placement_kind="rolling_window_repost",
            publish_status="PUBLISHED_MAIN",
            public_target_count=1,
            public_targets_json=[{"type": "vk_wall", "url": "https://vk.com/wall-222_1"}],
            published_at=datetime(2026, 6, 9, 18, 46, tzinfo=timezone.utc),
            details_json={
                "source_url": "https://vk.com/wall-111_older",
                "target_url": "https://vk.com/wall-222_1",
            },
        )
        session.add(yesterday_repost)
        await session.commit()

    async def fake_resolve(ref: str):
        if ref == "klgdevents":
            return 111, "Events", "klgdevents", "group"
        if ref == "kenigeventsofficial":
            return 222, "Main", "kenigeventsofficial", "group"
        raise AssertionError(ref)

    async def fake_vk_api(method: str, **_params):
        assert method == "wall.getById"
        return {"response": [{"date": int(now_utc.timestamp())}]}

    reposted: list[str] = []

    async def fake_publish_repost(_db, _bot, *, source_url, target_group_id, message):
        reposted.append(source_url)
        assert target_group_id == 222
        assert message == "caption"
        return "https://vk.com/wall-222_2"

    async def fake_caption(_ev):
        return "caption"

    monkeypatch.setattr(main, "vk_resolve_group", fake_resolve)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr("promo._build_promo_vk_repost_caption", fake_caption)
    monkeypatch.setattr("promo._publish_vk_repost", fake_publish_repost)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)

    assert [item.status for item in results] == ["published"]
    assert reposted == ["https://vk.com/wall-111_20"]
    async with db.get_session() as session:
        exposures = (
            await session.execute(
                select(PromoExposure)
                .where(PromoExposure.surface == PROMO_SURFACE_VK_REPOST)
                .order_by(PromoExposure.id)
            )
        ).scalars().all()
    assert [row.details_json["source_url"] for row in exposures] == [
        "https://vk.com/wall-111_older",
        "https://vk.com/wall-111_20",
    ]
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
    # Idempotent: second call keeps a single target and one activity per default surface.
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
        activities_by_surface = {activity.surface: activity for activity in activities}
        assert set(activities_by_surface) == {"video_general", PROMO_SURFACE_TG_BUTTON_HIGHLIGHT}
        video_activity = activities_by_surface["video_general"]
        assert video_activity.profile_key is None
        assert int(video_activity.max_per_publish) == 1
        assert video_activity.selection_policy == PROMO_POLICY_GUARANTEED_ANY_POSITION
        assert activities_by_surface[PROMO_SURFACE_TG_BUTTON_HIGHLIGHT].enabled is True

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

    matched = await _events_for_target(
        db,
        target=target,
        campaign=campaign,
        today=today,
        now_utc=now_utc,
    )
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
