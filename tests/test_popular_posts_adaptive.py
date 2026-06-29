from __future__ import annotations

from datetime import datetime, timezone

import pytest

from db import Database
from handlers import popular_posts_cmd
from models import TelegramPostMetric, TelegramScannedMessage, TelegramSource


async def _seed_tg_metric_run(
    db: Database,
    *,
    source_id: int,
    likes_values: list[int],
    now_utc: datetime,
    views: int = 100,
) -> None:
    async with db.get_session() as session:
        for idx, likes in enumerate(likes_values, start=1):
            session.add(
                TelegramScannedMessage(
                    source_id=source_id,
                    message_id=idx,
                    message_date=now_utc,
                    events_extracted=1,
                    events_imported=1,
                )
            )
            session.add(
                TelegramPostMetric(
                    source_id=source_id,
                    message_id=idx,
                    age_day=0,
                    source_url=f"https://t.me/adaptive/{idx}",
                    message_ts=int(now_utc.timestamp()),
                    collected_ts=int(now_utc.timestamp()),
                    views=views,
                    likes=int(likes),
                )
            )
        await session.commit()


@pytest.mark.asyncio
async def test_popular_posts_adaptively_adds_next_best_when_strict_share_is_low(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 29, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(popular_posts_cmd, "_utc_now_ts", lambda: int(now_utc.timestamp()))
    monkeypatch.setenv("POST_POPULARITY_MIN_SELECTED_SHARE", "0.20")
    monkeypatch.setenv("POST_POPULARITY_RELAXED_MIN_MULT", "0.75")
    monkeypatch.setenv("POST_POPULARITY_RELAXED_MAX_ADDED", "3")

    async with db.get_session() as session:
        source = TelegramSource(username="adaptive", title="Adaptive")
        session.add(source)
        await session.commit()
        await session.refresh(source)
        source_id = int(source.id)

    await _seed_tg_metric_run(
        db,
        source_id=source_id,
        likes_values=[200, 100, 100, 100, 100, 100, 100, 100, 100, 100],
        now_utc=now_utc,
    )

    items, dbg = await popular_posts_cmd._load_top_items(
        db,
        window_days=1,
        age_day=0,
        limit=30,
    )

    assert int(dbg["checked_posts"]) == 10
    assert int(dbg["strict_selected"]) == 1
    assert int(dbg["adaptive_added"]) == 1
    assert int(dbg["adaptive_selected"]) == 2
    assert len(items) == 2
    assert items[0].likes == 200
    assert items[1].likes == 100
    assert "👍" in items[1].popularity
    await db.close()


@pytest.mark.asyncio
async def test_popular_posts_keeps_strict_threshold_when_inventory_is_enough(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 6, 29, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(popular_posts_cmd, "_utc_now_ts", lambda: int(now_utc.timestamp()))
    monkeypatch.setenv("POST_POPULARITY_MIN_SELECTED_SHARE", "0.20")

    async with db.get_session() as session:
        source = TelegramSource(username="strict", title="Strict")
        session.add(source)
        await session.commit()
        await session.refresh(source)
        source_id = int(source.id)

    await _seed_tg_metric_run(
        db,
        source_id=source_id,
        likes_values=[50, 60, 70, 80, 90, 100, 110, 120, 130, 140],
        now_utc=now_utc,
    )

    items, dbg = await popular_posts_cmd._load_top_items(
        db,
        window_days=1,
        age_day=0,
        limit=30,
    )

    assert int(dbg["strict_selected"]) == 5
    assert int(dbg["adaptive_added"]) == 0
    assert len(items) == 5
    assert min(int(item.likes or 0) for item in items) == 100
    await db.close()
