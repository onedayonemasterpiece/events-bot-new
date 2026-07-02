from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from db import Database
from source_parsing.telegram import on_demand


def _message(username: str, message_id: int, *, dt: datetime | None = None):
    return SimpleNamespace(
        chat=SimpleNamespace(username=username, id=-100123),
        message_id=message_id,
        date=dt or datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc),
    )


async def _queue_row(db: Database, username: str = "kraftmarket39"):
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT source_username, latest_message_id, next_run_at, status, attempts, last_error
            FROM telegram_monitoring_on_demand_queue
            WHERE source_username=?
            """,
            (username,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row


@pytest.mark.asyncio
async def test_on_demand_enqueue_debounces_and_forces_message(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_DEBOUNCE_SECONDS", "600")
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_SOURCES", "kraftmarket39")

    db = Database(str(tmp_path / "test.sqlite"))
    await db.init()
    now = datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc)

    queued = await on_demand.enqueue_on_demand_channel_post(
        db,
        _message("kraftmarket39", 100, dt=now),
        now=now,
    )

    assert queued is True
    row = await _queue_row(db)
    assert row is not None
    assert row[0] == "kraftmarket39"
    assert row[1] == 100
    assert row[2] == "2026-06-20 08:10:00"
    assert row[3] == "pending"

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT COUNT(*)
            FROM telegram_source_force_message f
            JOIN telegram_source s ON s.id = f.source_id
            WHERE s.username='kraftmarket39' AND f.message_id=100
            """
        )
        count = (await cur.fetchone())[0]
        await cur.close()
    assert count == 1
    await db.close()


@pytest.mark.asyncio
async def test_on_demand_enqueue_coalesces_per_source(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_DEBOUNCE_SECONDS", "600")
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_SOURCES", "kraftmarket39")

    db = Database(str(tmp_path / "test.sqlite"))
    await db.init()
    now = datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc)

    await on_demand.enqueue_on_demand_channel_post(db, _message("kraftmarket39", 100, dt=now), now=now)
    await on_demand.enqueue_on_demand_channel_post(
        db,
        _message("kraftmarket39", 101, dt=now + timedelta(minutes=1)),
        now=now + timedelta(minutes=1),
    )

    row = await _queue_row(db)
    assert row is not None
    assert row[1] == 101
    assert row[2] == "2026-06-20 08:11:00"

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT COUNT(*)
            FROM telegram_source_force_message f
            JOIN telegram_source s ON s.id = f.source_id
            WHERE s.username='kraftmarket39'
            """
        )
        count = (await cur.fetchone())[0]
        await cur.close()
    assert count == 2
    await db.close()


@pytest.mark.asyncio
async def test_on_demand_dispatch_busy_requeues(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_DEBOUNCE_SECONDS", "0")
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_RETRY_SECONDS", "600")
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_SOURCES", "kraftmarket39")

    db = Database(str(tmp_path / "test.sqlite"))
    await db.init()
    now = datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc)
    await on_demand.enqueue_on_demand_channel_post(db, _message("kraftmarket39", 100, dt=now), now=now)

    calls = []

    async def fake_run_telegram_monitor(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(errors=["remote_telegram_session_busy"])

    async def fake_chat_id(_db):
        return 123

    monkeypatch.setattr(on_demand, "run_telegram_monitor", fake_run_telegram_monitor)
    monkeypatch.setattr(on_demand, "resolve_superadmin_chat_id", fake_chat_id)
    monkeypatch.setattr(on_demand, "_utc_now", lambda: now)

    attempted = await on_demand.dispatch_due_on_demand_monitoring(db, bot=object(), run_id="sched-1")

    assert attempted == 1
    assert calls and calls[0]["source_usernames"] == ["kraftmarket39"]
    assert calls[0]["trigger"] == "on_demand"
    row = await _queue_row(db)
    assert row[3] == "pending"
    assert row[4] == 1
    assert row[5] == "remote_telegram_session_busy"
    assert row[2] == "2026-06-20 08:10:00"
    await db.close()


@pytest.mark.asyncio
async def test_on_demand_dispatch_success_marks_done(tmp_path, monkeypatch):
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_DEBOUNCE_SECONDS", "0")
    monkeypatch.setenv("TG_MONITORING_ON_DEMAND_SOURCES", "kraftmarket39")

    db = Database(str(tmp_path / "test.sqlite"))
    await db.init()
    now = datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc)
    await on_demand.enqueue_on_demand_channel_post(db, _message("kraftmarket39", 100, dt=now), now=now)

    async def fake_run_telegram_monitor(*args, **kwargs):
        return SimpleNamespace(errors=[])

    async def fake_chat_id(_db):
        return 123

    monkeypatch.setattr(on_demand, "run_telegram_monitor", fake_run_telegram_monitor)
    monkeypatch.setattr(on_demand, "resolve_superadmin_chat_id", fake_chat_id)
    monkeypatch.setattr(on_demand, "_utc_now", lambda: now)

    attempted = await on_demand.dispatch_due_on_demand_monitoring(db, bot=object(), run_id="sched-1")

    assert attempted == 1
    row = await _queue_row(db)
    assert row[3] == "done"
    assert row[4] == 1
    assert row[5] is None
    await db.close()


def test_private_forward_message_filter_blocks_group_and_channel_reposts():
    private_forward = SimpleNamespace(
        chat=SimpleNamespace(type="private"),
        forward_date=datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc),
        model_extra={},
    )
    group_forward = SimpleNamespace(
        chat=SimpleNamespace(type="supergroup"),
        forward_date=datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc),
        model_extra={},
    )
    channel_forward = SimpleNamespace(
        chat=SimpleNamespace(type="channel"),
        forward_date=datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc),
        model_extra={},
    )

    assert on_demand.is_private_forward_message(private_forward) is True
    assert on_demand.is_private_forward_message(group_forward) is False
    assert on_demand.is_private_forward_message(channel_forward) is False


def test_private_forward_message_filter_supports_forward_origin_model_extra():
    private_forward = SimpleNamespace(
        chat=SimpleNamespace(type="private"),
        forward_date=None,
        forward_from_chat=None,
        forward_origin=None,
        model_extra={"forward_origin": {"type": "channel"}},
    )
    group_forward = SimpleNamespace(
        chat=SimpleNamespace(type="group"),
        forward_date=None,
        forward_from_chat=None,
        forward_origin=None,
        model_extra={"forward_origin": {"type": "channel"}},
    )

    assert on_demand.is_private_forward_message(private_forward) is True
    assert on_demand.is_private_forward_message(group_forward) is False
