from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from db import Database
from models import (
    Event,
    EventSource,
    Festival,
    PromoCampaign,
    TelegramPostMetric,
    TelegramScannedMessage,
    TelegramSource,
    VideoAnnounceItem,
    VideoAnnounceItemStatus,
    VideoAnnounceSession,
    VideoAnnounceSessionStatus,
)
from promo import PromoCandidate
from video_announce.partner_filters import FilterDecision
from video_announce.partner_filters import classify_event_konb_library
from video_announce.popular_review import build_popular_review_selection
from video_announce import popular_review as popular_review_module
from handlers import popular_posts_cmd as popular_posts_module


def _freeze_popular_posts_now(monkeypatch, now_utc: datetime) -> None:
    monkeypatch.setattr(
        popular_posts_module,
        "_utc_now_ts",
        lambda: int(now_utc.timestamp()),
    )


def _pick_for_merge(
    event_id: int,
    *,
    promo: bool,
    placement_kind: str = "general_boost",
) -> popular_review_module.PopularReviewPick:
    event = Event(
        title=f"Event {event_id}",
        description="Description",
        short_description="Short",
        search_digest="Digest",
        source_text="source",
        date="2026-06-01",
        time="19:00",
        location_name="Venue",
        city="Калининград",
        photo_urls=["https://example.com/poster.jpg"],
        photo_count=1,
    )
    event.id = event_id
    return popular_review_module.PopularReviewPick(
        event=event,
        score=999.0 if promo else 1.0,
        source_window="promo" if promo else "24h",
        source_post_url="",
        source_label="promo" if promo else "organic",
        anti_repeat_status="promo" if promo else "fresh",
        description="Description",
        promo_campaign_id=1 if promo else None,
        promo_activity_id=1 if promo else None,
        promo_placement_kind=placement_kind if promo else None,
    )


def test_popular_review_interleaves_promo_with_organic_first_or_second() -> None:
    now_utc = datetime(2026, 5, 14, 8, 0, tzinfo=timezone.utc)
    promo = [_pick_for_merge(1, promo=True), _pick_for_merge(2, promo=True)]
    fresh = [_pick_for_merge(3, promo=False), _pick_for_merge(4, promo=False)]

    selected = popular_review_module._merge_promo_and_fresh_picks(
        promo,
        fresh,
        max_events=4,
        now_utc=now_utc,
    )

    promo_positions = [
        idx
        for idx, pick in enumerate(selected, start=1)
        if pick.promo_campaign_id is not None
    ]
    assert promo_positions[0] in {1, 2}
    assert promo_positions != [1, 2]


def test_popular_review_guaranteed_any_position_uses_tail_slots() -> None:
    now_utc = datetime(2026, 5, 15, 8, 0, tzinfo=timezone.utc)
    promo = [
        _pick_for_merge(1, promo=True, placement_kind="guaranteed_any_position"),
        _pick_for_merge(2, promo=True, placement_kind="guaranteed_any_position"),
    ]
    fresh = [
        _pick_for_merge(3, promo=False),
        _pick_for_merge(4, promo=False),
        _pick_for_merge(5, promo=False),
        _pick_for_merge(6, promo=False),
        _pick_for_merge(7, promo=False),
        _pick_for_merge(8, promo=False),
    ]

    selected = popular_review_module._merge_promo_and_fresh_picks(
        promo,
        fresh,
        max_events=6,
        now_utc=now_utc,
    )

    assert [pick.event.id for pick in selected[:4]] == [3, 4, 5, 6]
    assert [pick.event.id for pick in selected[4:]] == [1, 2]


def test_popular_review_first_slot_promo_forces_position_one() -> None:
    now_utc = datetime(2026, 5, 17, 8, 0, tzinfo=timezone.utc)
    promo = [
        _pick_for_merge(1, promo=True, placement_kind="first_slot"),
        _pick_for_merge(2, promo=True, placement_kind="guaranteed_any_position"),
    ]
    fresh = [
        _pick_for_merge(3, promo=False),
        _pick_for_merge(4, promo=False),
        _pick_for_merge(5, promo=False),
    ]

    selected = popular_review_module._merge_promo_and_fresh_picks(
        promo,
        fresh,
        max_events=4,
        now_utc=now_utc,
    )

    assert [pick.event.id for pick in selected] == [1, 3, 4, 2]


@pytest.mark.asyncio
async def test_popular_review_partner_filter_applies_to_promo_candidates(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 17, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        event = Event(
            title="Base Promo",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-05-22",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/base-promo.jpg"],
            photo_count=1,
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    async def fake_resolve_video_promo_candidates(*_args, **kwargs):
        assert kwargs["profile_key"] == "popular_review_eco"
        assert kwargs["include_global_profile"] is True
        return [
            PromoCandidate(
                event=event,
                campaign_id=1,
                activity_id=2,
                placement_kind="guaranteed_any_position",
                reason="promo",
            )
        ]

    def reject_promo(ev: Event) -> FilterDecision:
        return FilterDecision(
            event_id=int(ev.id or 0),
            matched=False,
            needs_manual_review=False,
            reason="not eco",
            extra={},
        )

    monkeypatch.setattr(
        popular_review_module,
        "resolve_video_promo_candidates",
        fake_resolve_video_promo_candidates,
    )

    with pytest.raises(RuntimeError, match="did not collect enough events"):
        await build_popular_review_selection(
            db,
            max_events=1,
            min_events=1,
            anti_repeat_days=7,
            candidate_limit=1,
            now_utc=now_utc,
            profile_key="popular_review_eco",
            partner_track_id="partner_eco_nature_001",
            event_filter=reject_promo,
            admit_manual_review=False,
        )

    await db.close()


@pytest.mark.asyncio
async def test_popular_review_partner_allows_one_off_filter_promo_after_three_matches(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 17, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        profile_events = [
            Event(
                title=f"Eco Event {idx}",
                description="Description",
                short_description="Short",
                search_digest="Digest",
                source_text="source",
                date="2026-05-22",
                time="19:00",
                location_name="Venue",
                city="Калининград",
                photo_urls=[f"https://example.com/eco-{idx}.jpg"],
                photo_count=1,
            )
            for idx in range(3)
        ]
        promo_events = [
            Event(
                title=f"Festival Promo {idx}",
                description="Description",
                short_description="Short",
                search_digest="Digest",
                source_text="source",
                date="2026-05-23",
                time="19:00",
                location_name="Venue",
                city="Калининград",
                photo_urls=[f"https://example.com/promo-{idx}.jpg"],
                photo_count=1,
            )
            for idx in range(2)
        ]
        session.add_all([*profile_events, *promo_events])
        await session.commit()
        for event in [*profile_events, *promo_events]:
            await session.refresh(event)

    async def fake_collect_popular_hits(*_args, **_kwargs):
        return [
            {
                "event_id": int(event.id),
                "score": 100.0 - idx,
                "source_window": "24h",
                "source_post_url": f"https://t.me/popular/{idx}",
                "source_label": "organic",
            }
            for idx, event in enumerate(profile_events, start=1)
        ]

    async def fake_load_events_map(_db, event_ids):
        all_events = {int(event.id): event for event in [*profile_events, *promo_events]}
        return {int(event_id): all_events[int(event_id)] for event_id in event_ids}

    async def fake_resolve_video_promo_candidates(*_args, **kwargs):
        assert kwargs["profile_key"] == "popular_review_eco"
        assert kwargs["include_global_profile"] is True
        return [
            PromoCandidate(
                event=promo_events[0],
                campaign_id=80,
                activity_id=1,
                placement_kind="guaranteed_any_position",
                reason="promo:80-stories",
            ),
            PromoCandidate(
                event=promo_events[1],
                campaign_id=80,
                activity_id=2,
                placement_kind="guaranteed_any_position",
                reason="promo:80-stories",
            ),
        ]

    def eco_filter(ev: Event) -> FilterDecision:
        matched = str(ev.title or "").startswith("Eco")
        return FilterDecision(
            event_id=int(ev.id or 0),
            matched=matched,
            needs_manual_review=False,
            reason="eco" if matched else "not eco",
            extra={},
        )

    monkeypatch.setattr(
        popular_review_module,
        "resolve_video_promo_candidates",
        fake_resolve_video_promo_candidates,
    )
    monkeypatch.setattr(popular_review_module, "_collect_popular_hits", fake_collect_popular_hits)
    monkeypatch.setattr(popular_review_module, "_load_events_map", fake_load_events_map)

    selection = await build_popular_review_selection(
        db,
        max_events=4,
        min_events=4,
        anti_repeat_days=7,
        candidate_limit=10,
        now_utc=now_utc,
        profile_key="popular_review_eco",
        partner_track_id="partner_eco_nature_001",
        event_filter=eco_filter,
        admit_manual_review=False,
    )

    profile_ids = {int(event.id) for event in profile_events}
    promo_ids = [int(event.id) for event in promo_events]
    assert profile_ids.issubset(set(selection.event_ids))
    assert selection.event_ids.count(promo_ids[0]) == 1
    assert promo_ids[1] not in selection.event_ids
    assert selection.trace[promo_ids[0]]["promo_placement_kind"] == "guaranteed_any_position"
    assert selection.trace[promo_ids[0]]["partner_filter"]["partner_promo_off_filter_admitted"] is True
    await db.close()


@pytest.mark.asyncio
async def test_popular_review_partner_eco_recalls_current_event_with_old_source_post(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 17, 8, 0, tzinfo=timezone.utc)
    _freeze_popular_posts_now(monkeypatch, now_utc)

    async with db.get_session() as session:
        green = Event(
            title="Фестиваль «Зелёный Кёнигсберг»",
            description="Фестиваль про растения, ботанику, науку и обмен растениями.",
            short_description="Ботанический фестиваль с обменом растениями.",
            search_digest="Растения, ботаника, обмен растениями, наука и технологии.",
            source_text="source",
            date="2026-05-17",
            end_date="2026-05-17",
            time="",
            location_name="культурный квартал «Понарт»",
            location_address="Судостроительная 6/2",
            city="Калининград",
            photo_urls=["https://example.com/green.jpg"],
            photo_count=1,
            source_post_url="https://t.me/kulturnaya_chaika/7603",
        )
        unrelated = Event(
            title="Обычный концерт",
            description="Музыкальная программа без эко или краеведческой темы.",
            short_description="Концерт.",
            search_digest="Музыка и концерт.",
            source_text="source",
            date="2026-05-17",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/concert.jpg"],
            photo_count=1,
            source_post_url="https://t.me/music/1",
        )
        session.add_all([green, unrelated])
        await session.commit()
        await session.refresh(green)
        await session.refresh(unrelated)

    async def no_recent_popular_hits(*_args, **_kwargs):
        return []

    def eco_filter(ev: Event) -> FilterDecision:
        matched = "растен" in (
            f"{ev.title} {ev.description} {ev.search_digest}".casefold()
        )
        return FilterDecision(
            event_id=int(ev.id or 0),
            matched=matched,
            needs_manual_review=False,
            reason="eco plants" if matched else "not eco",
            extra={},
        )

    monkeypatch.setattr(
        popular_review_module,
        "_collect_popular_hits",
        no_recent_popular_hits,
    )

    selection = await build_popular_review_selection(
        db,
        max_events=2,
        min_events=1,
        anti_repeat_days=7,
        candidate_limit=2,
        now_utc=now_utc,
        profile_key="popular_review_eco",
        partner_track_id="partner_eco_nature_001",
        event_filter=eco_filter,
        admit_manual_review=False,
    )

    assert selection.event_ids == [int(green.id)]
    assert selection.trace[int(green.id)]["source_window"] == "partner_event_date_recall"
    assert int(unrelated.id) not in selection.event_ids
    await db.close()


async def _seed_popular_post(
    db: Database,
    *,
    event: Event,
    source_id: int,
    message_id: int,
    views: int,
    likes: int = 10,
    now_utc: datetime,
) -> None:
    source_url = f"https://t.me/popular/{message_id}"
    async with db.get_session() as session:
        session.add(
            TelegramScannedMessage(
                source_id=source_id,
                message_id=message_id,
                message_date=now_utc,
                events_extracted=1,
                events_imported=1,
            )
        )
        session.add(
            TelegramPostMetric(
                source_id=source_id,
                message_id=message_id,
                age_day=0,
                source_url=source_url,
                message_ts=int(now_utc.timestamp()),
                collected_ts=int(now_utc.timestamp()),
                views=views,
                likes=likes,
            )
        )
        session.add(
            EventSource(
                event_id=int(event.id),
                source_type="telegram",
                source_url=source_url,
                source_chat_username="popular",
                source_message_id=message_id,
            )
        )
        await session.commit()


async def _seed_recent_cherryflash_item(
    db: Database,
    *,
    event: Event,
    status: VideoAnnounceSessionStatus,
    created_at: datetime,
) -> None:
    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=status,
            profile_key="popular_review",
            created_at=created_at,
            selection_params={"mode": "popular_review"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session.add(
            VideoAnnounceItem(
                session_id=int(sess.id),
                event_id=int(event.id),
                status=VideoAnnounceItemStatus.READY,
                position=1,
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_popular_review_cooldown_excludes_recent_published_selection(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc)
    _freeze_popular_posts_now(monkeypatch, now_utc)

    async with db.get_session() as session:
        source = TelegramSource(username="popular", title="Popular")
        session.add(source)
        await session.commit()
        await session.refresh(source)
        source_id = int(source.id)

        events: list[Event] = []
        for idx, title in enumerate(
            ["Repeated QUEEN", "Fresh One", "Fresh Two", "Low Baseline", "Lower Baseline"],
            start=1,
        ):
            event = Event(
                title=title,
                description="Description",
                short_description="Short",
                search_digest="Digest",
                source_text="source",
                date="2026-04-30",
                time="19:00",
                location_name="Venue",
                city="Калининград",
                photo_urls=[f"https://example.com/{idx}.jpg"],
                photo_count=1,
            )
            session.add(event)
            events.append(event)
        await session.commit()
        for event in events:
            await session.refresh(event)

    repeated, fresh_one, fresh_two, low, lower = events
    await _seed_recent_cherryflash_item(
        db,
        event=repeated,
        status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
        created_at=now_utc - timedelta(days=2),
    )
    await _seed_popular_post(db, event=repeated, source_id=source_id, message_id=1, views=300, now_utc=now_utc)
    await _seed_popular_post(db, event=fresh_one, source_id=source_id, message_id=2, views=250, now_utc=now_utc)
    await _seed_popular_post(
        db,
        event=fresh_two,
        source_id=source_id,
        message_id=3,
        views=220,
        likes=20,
        now_utc=now_utc,
    )
    await _seed_popular_post(db, event=low, source_id=source_id, message_id=4, views=10, now_utc=now_utc)
    await _seed_popular_post(db, event=lower, source_id=source_id, message_id=5, views=5, now_utc=now_utc)

    selection = await build_popular_review_selection(
        db,
        max_events=3,
        min_events=2,
        anti_repeat_days=7,
        candidate_limit=10,
        now_utc=now_utc,
    )

    assert set(selection.event_ids) == {int(fresh_one.id), int(fresh_two.id)}
    assert int(repeated.id) not in selection.event_ids
    assert all(meta["anti_repeat_status"] == "fresh" for meta in selection.trace.values())
    await db.close()


@pytest.mark.asyncio
async def test_popular_review_raises_instead_of_repeat_fill_when_cooldown_leaves_too_few(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc)
    _freeze_popular_posts_now(monkeypatch, now_utc)

    async with db.get_session() as session:
        source = TelegramSource(username="popular", title="Popular")
        session.add(source)
        await session.commit()
        await session.refresh(source)
        source_id = int(source.id)

        repeated = Event(
            title="Repeated",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-04-30",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/repeated.jpg"],
            photo_count=1,
        )
        fresh = Event(
            title="Only Fresh",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-04-30",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/fresh.jpg"],
            photo_count=1,
        )
        low = Event(
            title="Low Baseline",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-04-30",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/low.jpg"],
            photo_count=1,
        )
        lower = Event(
            title="Lower Baseline",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-04-30",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/lower.jpg"],
            photo_count=1,
        )
        session.add_all([repeated, fresh, low, lower])
        await session.commit()
        for event in (repeated, fresh, low, lower):
            await session.refresh(event)

    await _seed_recent_cherryflash_item(
        db,
        event=repeated,
        status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
        created_at=now_utc - timedelta(days=1),
    )
    await _seed_popular_post(db, event=repeated, source_id=source_id, message_id=1, views=300, now_utc=now_utc)
    await _seed_popular_post(db, event=fresh, source_id=source_id, message_id=2, views=250, now_utc=now_utc)
    await _seed_popular_post(db, event=low, source_id=source_id, message_id=3, views=10, now_utc=now_utc)
    await _seed_popular_post(db, event=lower, source_id=source_id, message_id=4, views=5, now_utc=now_utc)

    with pytest.raises(RuntimeError, match="did not collect enough events"):
        await build_popular_review_selection(
            db,
            max_events=2,
            min_events=2,
            anti_repeat_days=7,
            candidate_limit=10,
            now_utc=now_utc,
        )
    await db.close()


@pytest.mark.asyncio
async def test_popular_review_persists_rehydrated_photo_urls(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc)
    _freeze_popular_posts_now(monkeypatch, now_utc)

    async def fake_rehydrate_public_tg_photo_urls(source_post_url: str | None) -> list[str]:
        if str(source_post_url or "").endswith("/1"):
            return ["https://example.com/rehydrated.jpg"]
        return []

    monkeypatch.setattr(
        popular_review_module,
        "_rehydrate_public_tg_photo_urls",
        fake_rehydrate_public_tg_photo_urls,
    )

    async with db.get_session() as session:
        source = TelegramSource(username="popular", title="Popular")
        session.add(source)
        await session.commit()
        await session.refresh(source)
        source_id = int(source.id)

        rehydrated = Event(
            title="Rehydrated Poster",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-04-30",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=[],
            photo_count=0,
            source_post_url="https://t.me/popular/1",
        )
        direct = Event(
            title="Direct Poster",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-04-30",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/direct.jpg"],
            photo_count=1,
            source_post_url="https://t.me/popular/2",
        )
        missing = Event(
            title="Missing Poster",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-04-30",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=[],
            photo_count=0,
            source_post_url="https://t.me/popular/3",
        )
        session.add_all([rehydrated, direct, missing])
        await session.commit()
        for event in (rehydrated, direct, missing):
            await session.refresh(event)

    await _seed_popular_post(
        db,
        event=rehydrated,
        source_id=source_id,
        message_id=1,
        views=300,
        now_utc=now_utc,
    )
    await _seed_popular_post(
        db,
        event=direct,
        source_id=source_id,
        message_id=2,
        views=250,
        now_utc=now_utc,
    )
    await _seed_popular_post(
        db,
        event=missing,
        source_id=source_id,
        message_id=3,
        views=240,
        now_utc=now_utc,
    )

    selection = await build_popular_review_selection(
        db,
        max_events=1,
        min_events=1,
        anti_repeat_days=7,
        candidate_limit=10,
        now_utc=now_utc,
    )

    assert selection.event_ids == [int(rehydrated.id)]
    assert int(missing.id) not in selection.event_ids

    async with db.get_session() as session:
        persisted = await session.get(Event, int(rehydrated.id))

    assert persisted is not None
    assert persisted.photo_urls == ["https://example.com/rehydrated.jpg"]
    assert persisted.photo_count == 1
    await db.close()


@pytest.mark.asyncio
async def test_popular_review_skips_rehydrated_event_when_persist_fails(monkeypatch):
    async def fake_rehydrate_public_tg_photo_urls(source_post_url: str | None) -> list[str]:
        return ["https://example.com/rehydrated.jpg"]

    async def fake_persist_rehydrated_photo_urls(*args, **kwargs) -> bool:
        return False

    monkeypatch.setattr(
        popular_review_module,
        "_rehydrate_public_tg_photo_urls",
        fake_rehydrate_public_tg_photo_urls,
    )
    monkeypatch.setattr(
        popular_review_module,
        "_persist_rehydrated_photo_urls",
        fake_persist_rehydrated_photo_urls,
    )

    event = Event(
        title="Locked Persist",
        description="Description",
        short_description="Short",
        search_digest="Digest",
        source_text="source",
        date="2026-04-30",
        time="19:00",
        location_name="Venue",
        city="Калининград",
        photo_urls=[],
        photo_count=0,
        source_post_url="https://t.me/popular/1",
    )
    event.id = 123

    urls = await popular_review_module._ensure_renderable_photo_urls(event, db=object())

    assert urls == []


@pytest.mark.asyncio
async def test_popular_review_seeded_promo_uses_only_future_festival_events(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 13, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(Festival(name="80 историй о главном"))
        past = Event(
            title="Past 80 Stories",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            festival="80 историй о главном",
            source_text="source",
            date="2026-05-01",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/past.jpg"],
            photo_count=1,
        )
        future_one = Event(
            title="Future 80 Stories One",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            festival="80 историй о главном",
            source_text="source",
            date="2026-06-01",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/future-one.jpg"],
            photo_count=1,
        )
        future_two = Event(
            title="Future 80 Stories Two",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            festival="80 историй о главном",
            source_text="source",
            date="2026-06-02",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=["https://example.com/future-two.jpg"],
            photo_count=1,
        )
        future_without_poster = Event(
            title="Future 80 Stories Without Poster",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            festival="80 историй о главном",
            source_text="source",
            date="2026-06-03",
            time="19:00",
            location_name="Venue",
            city="Калининград",
            photo_urls=[],
            photo_count=0,
        )
        session.add_all([past, future_one, future_two, future_without_poster])
        await session.commit()
        for event in (past, future_one, future_two, future_without_poster):
            await session.refresh(event)

    selection = await build_popular_review_selection(
        db,
        max_events=3,
        min_events=1,
        anti_repeat_days=7,
        candidate_limit=10,
        now_utc=now_utc,
    )

    assert int(past.id) not in selection.event_ids
    assert int(future_without_poster.id) not in selection.event_ids
    assert set(selection.event_ids) == {int(future_one.id), int(future_two.id)}
    assert all(row.mandatory for row in selection.ranked)
    assert all(row.promo_campaign_id for row in selection.ranked)
    assert all(selection.trace[event_id]["source_window"] == "promo" for event_id in selection.event_ids)

    async with db.get_session() as session:
        campaigns = (await session.execute(select(PromoCampaign))).scalars().all()
    assert len(campaigns) == 1
    assert campaigns[0].status == "active"
    await db.close()


@pytest.mark.asyncio
async def test_konb_selection_falls_back_to_future_library_events(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 17, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        konb_ticketed = Event(
            title="Лекция со специальным гостем",
            description="Встреча с приглашённым гостем",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-05-20",
            time="19:00",
            location_name="Научная библиотека, Мира 9, Калининград",
            city="Калининград",
            ticket_link="https://tickets.example/konb",
            photo_urls=["https://example.com/konb-ticketed.jpg"],
            photo_count=1,
        )
        other_library = Event(
            title="Другая библиотека",
            description="Description",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-05-20",
            time="19:00",
            location_name="Библиотека им. Лунина, Калинина 4, Черняховск",
            city="Черняховск",
            photo_urls=["https://example.com/other.jpg"],
            photo_count=1,
        )
        session.add_all([konb_ticketed, other_library])
        await session.commit()
        await session.refresh(konb_ticketed)
        await session.refresh(other_library)

    async def no_popular_hits(*_args, **_kwargs):
        return []

    async def no_promos(*_args, **_kwargs):
        return []

    monkeypatch.setattr(popular_review_module, "_collect_popular_hits", no_popular_hits)
    monkeypatch.setattr(popular_review_module, "resolve_video_promo_candidates", no_promos)

    selection = await build_popular_review_selection(
        db,
        max_events=2,
        min_events=1,
        anti_repeat_days=1,
        candidate_limit=5,
        now_utc=now_utc,
        profile_key="popular_review_konb",
        partner_track_id="partner_konb_library_001",
        event_filter=classify_event_konb_library,
        admit_manual_review=False,
        selection_policy_id="konb_library",
    )

    assert selection.event_ids == [int(konb_ticketed.id)]
    meta = selection.trace[int(konb_ticketed.id)]
    assert meta["source_window"] == "future_fallback"
    assert "ticket_or_price" in meta["priority_reasons"]
    assert "special_guest_hint" in meta["priority_reasons"]
    assert "due_in_3_days" in meta["priority_reasons"]
    await db.close()


@pytest.mark.asyncio
async def test_konb_selection_recycles_previous_future_event_when_fresh_pool_empty(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        recycled = Event(
            title="КОНБ: лекция о литературе",
            description="Встреча в Калининградской областной научной библиотеке",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-05-20",
            time="19:00",
            location_name="Научная библиотека, Мира 9, Калининград",
            city="Калининград",
            photo_urls=["https://example.com/recycled.jpg"],
            photo_count=1,
        )
        session.add(recycled)
        await session.commit()
        await session.refresh(recycled)
        previous = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
            profile_key="popular_review_konb",
            created_at=datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc),
            started_at=datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 5, 17, 10, 30, tzinfo=timezone.utc),
        )
        session.add(previous)
        await session.commit()
        await session.refresh(previous)
        session.add(
            VideoAnnounceItem(
                session_id=previous.id,
                event_id=recycled.id,
                position=1,
                status=VideoAnnounceItemStatus.READY,
            )
        )
        await session.commit()

    async def no_popular_hits(*_args, **_kwargs):
        return []

    async def no_future_hits(*_args, **_kwargs):
        return []

    async def no_promos(*_args, **_kwargs):
        return []

    monkeypatch.setattr(popular_review_module, "_collect_popular_hits", no_popular_hits)
    monkeypatch.setattr(popular_review_module, "_collect_future_event_hits", no_future_hits)
    monkeypatch.setattr(popular_review_module, "resolve_video_promo_candidates", no_promos)

    selection = await build_popular_review_selection(
        db,
        max_events=2,
        min_events=1,
        anti_repeat_days=1,
        candidate_limit=5,
        now_utc=now_utc,
        profile_key="popular_review_konb",
        partner_track_id="partner_konb_library_001",
        event_filter=classify_event_konb_library,
        admit_manual_review=False,
        selection_policy_id="konb_library",
    )

    assert selection.event_ids == [int(recycled.id)]
    assert selection.trace[int(recycled.id)]["source_window"] == "konb_recycle"
    await db.close()


@pytest.mark.asyncio
async def test_konb_selection_recycle_does_not_repeat_same_calendar_day(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        same_day = Event(
            title="КОНБ: встреча",
            description="Калининградская областная научная библиотека",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-05-20",
            time="19:00",
            location_name="Научная библиотека, Мира 9, Калининград",
            city="Калининград",
            photo_urls=["https://example.com/same-day.jpg"],
            photo_count=1,
        )
        session.add(same_day)
        await session.commit()
        await session.refresh(same_day)
        previous = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
            profile_key="popular_review_konb",
            created_at=now_utc,
            started_at=now_utc,
            finished_at=now_utc,
        )
        session.add(previous)
        await session.commit()
        await session.refresh(previous)
        session.add(
            VideoAnnounceItem(
                session_id=previous.id,
                event_id=same_day.id,
                position=1,
                status=VideoAnnounceItemStatus.READY,
            )
        )
        await session.commit()

    async def no_popular_hits(*_args, **_kwargs):
        return []

    async def no_future_hits(*_args, **_kwargs):
        return []

    async def no_promos(*_args, **_kwargs):
        return []

    monkeypatch.setattr(popular_review_module, "_collect_popular_hits", no_popular_hits)
    monkeypatch.setattr(popular_review_module, "_collect_future_event_hits", no_future_hits)
    monkeypatch.setattr(popular_review_module, "resolve_video_promo_candidates", no_promos)

    selection = await build_popular_review_selection(
        db,
        max_events=2,
        min_events=1,
        anti_repeat_days=1,
        candidate_limit=5,
        now_utc=now_utc,
        profile_key="popular_review_konb",
        partner_track_id="partner_konb_library_001",
        event_filter=classify_event_konb_library,
        admit_manual_review=False,
        selection_policy_id="konb_library",
    )

    assert selection.event_ids == []
    await db.close()


@pytest.mark.asyncio
async def test_konb_selection_can_use_same_day_recycle_as_last_resort(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        same_day = Event(
            title="КОНБ: лекция",
            description="Калининградская областная научная библиотека",
            short_description="Short",
            search_digest="Digest",
            source_text="source",
            date="2026-05-20",
            time="19:00",
            location_name="Научная библиотека, Мира 9, Калининград",
            city="Калининград",
            photo_urls=["https://example.com/same-day-repeat.jpg"],
            photo_count=1,
        )
        session.add(same_day)
        await session.commit()
        await session.refresh(same_day)
        previous = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
            profile_key="popular_review_konb",
            created_at=now_utc,
            started_at=now_utc,
            finished_at=now_utc,
        )
        session.add(previous)
        await session.commit()
        await session.refresh(previous)
        session.add(
            VideoAnnounceItem(
                session_id=previous.id,
                event_id=same_day.id,
                position=1,
                status=VideoAnnounceItemStatus.READY,
            )
        )
        await session.commit()

    async def no_popular_hits(*_args, **_kwargs):
        return []

    async def no_future_hits(*_args, **_kwargs):
        return []

    async def no_promos(*_args, **_kwargs):
        return []

    monkeypatch.setattr(popular_review_module, "_collect_popular_hits", no_popular_hits)
    monkeypatch.setattr(popular_review_module, "_collect_future_event_hits", no_future_hits)
    monkeypatch.setattr(popular_review_module, "resolve_video_promo_candidates", no_promos)

    selection = await build_popular_review_selection(
        db,
        max_events=2,
        min_events=1,
        anti_repeat_days=1,
        candidate_limit=5,
        now_utc=now_utc,
        profile_key="popular_review_konb",
        partner_track_id="partner_konb_library_001",
        event_filter=classify_event_konb_library,
        admit_manual_review=False,
        selection_policy_id="konb_library",
        allow_same_day_recycle=True,
    )

    assert selection.event_ids == [int(same_day.id)]
    assert selection.trace[int(same_day.id)]["source_window"] == "konb_same_day_recycle"
    assert selection.trace[int(same_day.id)]["anti_repeat_status"] == "same_day_recycle"
    await db.close()


def test_konb_first_position_penalty_demotes_recent_leader():
    penalty = popular_review_module._first_position_penalty(
        {"best_recent_position": 1},
        selection_policy_id="konb_library",
    )
    no_penalty = popular_review_module._first_position_penalty(
        {"best_recent_position": 2},
        selection_policy_id="konb_library",
    )

    assert penalty > 0
    assert no_penalty == 0.0
