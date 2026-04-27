from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from db import Database
from models import (
    Event,
    EventSource,
    TelegramPostMetric,
    TelegramScannedMessage,
    TelegramSource,
    VideoAnnounceItem,
    VideoAnnounceItemStatus,
    VideoAnnounceSession,
    VideoAnnounceSessionStatus,
)
from video_announce.popular_review import build_popular_review_selection
from video_announce import popular_review as popular_review_module


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
async def test_popular_review_cooldown_excludes_recent_published_selection(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc)

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


@pytest.mark.asyncio
async def test_popular_review_raises_instead_of_repeat_fill_when_cooldown_leaves_too_few(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 4, 22, 8, 0, tzinfo=timezone.utc)

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


@pytest.mark.asyncio
async def test_popular_review_persists_rehydrated_photo_urls(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc)

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
