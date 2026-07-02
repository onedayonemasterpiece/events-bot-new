from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

import poll_to_forward_popularity as pfp
from db import Database
from models import Event
from source_parsing.post_metrics import upsert_vk_post_metric


TARGET_DATE = date(2026, 6, 14)
NOW = datetime(2026, 6, 13, 10, 0, tzinfo=timezone.utc)


def _event_model(event_id: int, *, title: str, post_id: int, stored_vk_id: int | None = None) -> Event:
    return Event(
        id=event_id,
        title=title,
        description=f"Описание {title}",
        date=TARGET_DATE.isoformat(),
        time="19:00",
        location_name=f"Площадка {event_id}",
        city="Калининград",
        source_text=f"source {title}",
        event_type="концерт",
        tg_event_post_id=post_id,
        tg_event_post_url=f"https://t.me/kldevents/{post_id}",
        source_vk_post_url=(
            f"https://vk.com/wall-231920894_{stored_vk_id}"
            if stored_vk_id is not None
            else None
        ),
        telegraph_url=f"https://telegra.ph/event-{event_id}",
        lifecycle_status="active",
        silent=False,
    )


def _candidate(event: Event) -> SimpleNamespace:
    return SimpleNamespace(
        id=event.id,
        title=event.title,
        date=event.date,
        time=event.time,
        location_name=event.location_name,
        source_vk_post_url=event.source_vk_post_url,
    )


def _vk_item(post_id: int, *, title: str, location: str, views: int) -> dict:
    return {
        "id": post_id,
        "date": int(NOW.timestamp()) - 3600,
        "text": f"{title}\n14 июня 19:00\n{location}",
        "views": {"count": views},
        "likes": {"count": max(0, views // 10)},
        "comments": {"count": max(0, views // 100)},
        "reposts": {"count": max(0, views // 200)},
    }


@pytest.mark.asyncio
async def test_kldevents_wall_scan_recovers_stale_ids_fixture(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    events: list[Event] = []
    async with db.get_session() as session:
        for idx in range(1, 24):
            stored_id = 1000 + idx if idx <= 22 else None
            event = _event_model(
                idx,
                title=f"Событие {idx}",
                post_id=500 + idx,
                stored_vk_id=stored_id,
            )
            events.append(event)
            session.add(event)
        await session.commit()

    direct = {
        1001: _vk_item(
            1001,
            title="Событие 1",
            location="Площадка 1",
            views=500,
        )
    }
    wall = [
        _vk_item(
            1001 if idx == 1 else 2000 + idx,
            title=f"Событие {idx}",
            location=f"Площадка {idx}",
            views=100 + idx * 40,
        )
        for idx in range(1, 15)
    ]
    wall.extend(
        _vk_item(3000 + idx, title=f"Другой анонс {idx}", location="Другая площадка", views=5)
        for idx in range(20)
    )

    async def fake_get_by_ids(_group_id, post_ids):
        return {post_id: direct[post_id] for post_id in post_ids if post_id in direct}

    async def fake_scan_wall(_group_id, *, limit):
        assert limit >= 1000
        return wall

    monkeypatch.setattr(pfp, "_vk_get_by_ids", fake_get_by_ids)
    monkeypatch.setattr(pfp, "_vk_scan_wall", fake_scan_wall)

    result = await pfp.build_event_popularity(
        db,
        [_candidate(event) for event in events],
        target_date=TARGET_DATE,
        now_utc=NOW,
    )

    diag = result.diagnostics
    assert diag["kldevents_direct_found"] == 1
    assert diag["kldevents_wall_scan_found"] == 14
    assert diag["kldevents_changed_id_count"] == 13
    assert diag["kldevents_unmatched"] == 9
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT COUNT(*)
            FROM event_publication
            WHERE target='klgdevents' AND status='published' AND live_post_id IS NOT NULL
            """
        )
        assert (await cur.fetchone())[0] == 14
    await db.close()


def test_kldevents_match_rejects_title_date_only_when_other_anchors_exist():
    event = SimpleNamespace(title="Фестиваль Кантата", time="19:00", location_name="Кафедральный собор")
    item = {"text": "Фестиваль Кантата\n14 июня\nБольшая программа"}

    score, confidence = pfp._match_post_score(event, item, TARGET_DATE)

    assert (score, confidence) == (0, 0.0)


@pytest.mark.asyncio
async def test_kldevents_duplicate_events_share_live_post_group(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    events = [
        _event_model(1, title="Общий концерт", post_id=501, stored_vk_id=1001),
        _event_model(2, title="Общий концерт", post_id=502, stored_vk_id=1002),
    ]
    async with db.get_session() as session:
        session.add_all(events)
        await session.commit()

    wall = [
        _vk_item(2001, title="Общий концерт", location="Площадка 1", views=500),
        _vk_item(3001, title="Фоновый анонс", location="Площадка", views=1),
    ]

    async def fake_get_by_ids(_group_id, _post_ids):
        return {}

    async def fake_scan_wall(_group_id, *, limit):
        return wall

    monkeypatch.setattr(pfp, "_vk_get_by_ids", fake_get_by_ids)
    monkeypatch.setattr(pfp, "_vk_scan_wall", fake_scan_wall)

    result = await pfp.build_event_popularity(
        db,
        [_candidate(event) for event in events],
        target_date=TARGET_DATE,
        now_utc=NOW,
    )

    assert result.by_event_id[1].group_key == result.by_event_id[2].group_key
    assert result.diagnostics["kldevents_collapsed_live_posts"] == 1
    await db.close()


@pytest.mark.asyncio
async def test_kldevents_tiny_db_baseline_falls_back_to_wall_scan(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event = _event_model(1, title="Тихий концерт", post_id=501, stored_vk_id=1001)
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
    for post_id in range(1, 5):
        await upsert_vk_post_metric(
            db,
            group_id=231920894,
            post_id=post_id,
            age_day=0,
            source_url=f"https://vk.com/wall-231920894_{post_id}",
            post_ts=int(NOW.timestamp()) - post_id,
            views=30,
            likes=1,
            comments=0,
            reposts=0,
            collected_ts=int(NOW.timestamp()),
        )

    wall = [_vk_item(2001, title="Тихий концерт", location="Площадка 1", views=33)]
    wall.extend(
        _vk_item(3000 + idx, title=f"Фоновый анонс {idx}", location="Другая площадка", views=500)
        for idx in range(40)
    )

    async def fake_get_by_ids(_group_id, _post_ids):
        return {}

    async def fake_scan_wall(_group_id, *, limit):
        return wall

    monkeypatch.setenv("POLL_TO_FORWARD_KLDEVENTS_BASELINE_MIN_SAMPLE", "30")
    monkeypatch.setattr(pfp, "_vk_get_by_ids", fake_get_by_ids)
    monkeypatch.setattr(pfp, "_vk_scan_wall", fake_scan_wall)

    result = await pfp.build_event_popularity(
        db,
        [_candidate(event)],
        target_date=TARGET_DATE,
        now_utc=NOW,
    )

    assert result.diagnostics["kldevents_baseline_source"] == "wall_scan_bootstrap"
    assert result.diagnostics["kldevents_baseline_confidence"] == "low"
    assert result.diagnostics["kldevents_baseline_sample"] >= 30
    assert 1 not in result.by_event_id
    await db.close()


@pytest.mark.asyncio
async def test_vk_metric_storage_keeps_comments_and_reposts(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    await upsert_vk_post_metric(
        db,
        group_id=231920894,
        post_id=42,
        age_day=0,
        source_url="https://vk.com/wall-231920894_42",
        post_ts=int(NOW.timestamp()),
        views=100,
        likes=7,
        comments=3,
        reposts=2,
        collected_ts=int(NOW.timestamp()),
    )

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT views, likes, comments, reposts FROM vk_post_metric WHERE group_id=? AND post_id=?",
            (231920894, 42),
        )
        assert await cur.fetchone() == (100, 7, 3, 2)
    await db.close()
