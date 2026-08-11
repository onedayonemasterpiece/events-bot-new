from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

import pytest

import main
from db import Database
import vk_intake


async def _db(path: Path) -> Database:
    db = Database(str(path))
    await db.init()
    return db


async def _schedule(
    db: Database,
    *,
    mode: str = "incremental",
    page_size: int = 3,
    offset: int = 0,
    since_ts: int = 1,
    horizon_ts: int = 1,
    cursor_ts: int = 1,
    cursor_post_id: int = 0,
) -> None:
    await vk_intake._schedule_vk_crawl_continuation(
        db,
        group_id=1,
        owner_type="group",
        scan_mode=mode,
        page_size=page_size,
        since_ts=since_ts,
        offset=offset,
        horizon_ts=horizon_ts,
        original_cursor_ts=cursor_ts,
        original_cursor_post_id=cursor_post_id,
        reason="test_cap",
    )


def _posts(count: int, *, first_id: int = 1, newest_ts: int | None = None):
    newest = int(newest_ts or time.time())
    return [
        {
            "post_id": first_id + index,
            "date": newest - index,
            "text": f"raw {first_id + index}",
            "photos": [],
        }
        for index in range(count)
    ]


@pytest.mark.asyncio
async def test_continuation_schema_migrates_legacy_table_and_init_is_repeatable(tmp_path):
    path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE vk_crawl_continuation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL DEFAULT 'vk', owner_id INTEGER NOT NULL,
                owner_type TEXT NOT NULL DEFAULT 'group', since_ts INTEGER NOT NULL,
                offset INTEGER NOT NULL, horizon_ts INTEGER NOT NULL, reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                lease_owner TEXT, lease_expires_at TIMESTAMP, last_typed_reason TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_type,owner_id,since_ts,offset,horizon_ts)
            )
            """
        )
        conn.execute(
            "INSERT INTO vk_crawl_continuation(owner_id,since_ts,offset,horizon_ts,reason) "
            "VALUES(1,10,30,10,'legacy')"
        )
    db = Database(str(path))
    await db.init()
    await db.init()
    async with db.raw_conn() as conn:
        columns = {
            row[1]
            for row in await (await conn.execute("PRAGMA table_info(vk_crawl_continuation)")).fetchall()
        }
        row = await (
            await conn.execute(
                "SELECT scan_mode,page_size,original_cursor_ts,original_cursor_post_id "
                "FROM vk_crawl_continuation WHERE reason='legacy'"
            )
        ).fetchone()
    assert {
        "continuation_key", "scan_mode", "page_size", "original_cursor_ts",
        "original_cursor_post_id", "locked_at", "locked_by", "run_id",
        "last_page_fingerprint", "completed_at",
    } <= columns
    assert row[0] == "incremental" and row[1] == 30


@pytest.mark.asyncio
async def test_backlog_beyond_hard_cap_is_fully_persisted_by_sequential_workers(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path / "backlog.sqlite")
    now = int(time.time())
    backlog = _posts(25, newest_ts=now + 100)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'g','G')"
        )
        await conn.execute(
            "INSERT INTO vk_crawl_cursor(group_id,last_seen_ts,last_post_id,updated_at) "
            "VALUES(1,10,1,CURRENT_TIMESTAMP)"
        )
        await conn.commit()

    async def wall(_gid, _since, *, count, offset, **_kwargs):
        return backlog[offset : offset + count]

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_PAGE_SIZE", 2)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_MAX_PAGES_INC", 1)

    await vk_intake.crawl_once(db)
    for index in range(10):
        outcome = await vk_intake.process_vk_crawl_continuations(
            db,
            max_jobs=1,
            max_pages_per_job=1,
            worker_id=f"sequential-{index}",
            run_id=f"run-{index}",
        )
        if outcome["claimed"] == 0:
            break

    async with db.raw_conn() as conn:
        ids = await (
            await conn.execute("SELECT post_id FROM vk_source_packet ORDER BY post_id")
        ).fetchall()
        continuation = await (
            await conn.execute(
                "SELECT status,scan_mode,original_cursor_ts,original_cursor_post_id "
                "FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert [row[0] for row in ids] == list(range(1, 26))
    assert continuation == ("done", "incremental", 10, 1)


@pytest.mark.asyncio
async def test_backfill_continues_past_page_cap_until_horizon(tmp_path, monkeypatch):
    db = await _db(tmp_path / "backfill.sqlite")
    now = int(time.time())
    horizon = now - 86400
    backlog = _posts(5, newest_ts=now)
    backlog.append(
        {"post_id": 6, "date": horizon - 1, "text": "boundary", "photos": []}
    )
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'g','G')"
        )
        await conn.commit()

    async def wall(_gid, _since, *, count, offset, **_kwargs):
        return backlog[offset : offset + count]

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_PAGE_SIZE_BACKFILL", 2)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_MAX_PAGES_BACKFILL", 2)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_BACKFILL_DAYS", 1)

    await vk_intake.crawl_once(db)
    outcome = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=5, worker_id="backfill", run_id="backfill"
    )
    async with db.raw_conn() as conn:
        ids = await (
            await conn.execute("SELECT post_id FROM vk_source_packet ORDER BY post_id")
        ).fetchall()
        row = await (
            await conn.execute(
                "SELECT status,scan_mode,last_typed_reason FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert outcome["completed"] == 1
    assert [item[0] for item in ids] == list(range(1, 7))
    assert row == ("done", "backfill", "HORIZON_REACHED")


@pytest.mark.asyncio
async def test_mid_page_persistence_failure_does_not_advance_offset(tmp_path, monkeypatch):
    db = await _db(tmp_path / "failure.sqlite")
    page = _posts(3, newest_ts=100)
    await _schedule(db, page_size=3)

    async def wall(_gid, _since, *, count, offset, **_kwargs):
        return page[offset : offset + count]

    real_persist = vk_intake._persist_vk_source_packet
    calls = 0

    async def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("disk full")
        return await real_persist(*args, **kwargs)

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake, "_persist_vk_source_packet", fail_second)
    first = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=1, worker_id="w1", run_id="r1"
    )
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT status,offset,last_typed_reason FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert first["retried"] == 1
    assert row == ("retry", 0, "VK_CRAWL_PERSIST_FAILED")

    monkeypatch.setattr(vk_intake, "_persist_vk_source_packet", real_persist)
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE vk_crawl_continuation SET next_attempt_at=CURRENT_TIMESTAMP"
        )
        await conn.commit()
    await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=1, worker_id="w2", run_id="r2"
    )
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute("SELECT status,offset FROM vk_crawl_continuation")
        ).fetchone()
        count = await (
            await conn.execute("SELECT COUNT(*) FROM vk_source_packet")
        ).fetchone()
    assert row == ("pending", 3)
    assert count[0] == 3


@pytest.mark.asyncio
async def test_stale_running_lease_is_recovered(tmp_path, monkeypatch):
    db = await _db(tmp_path / "stale.sqlite")
    await _schedule(db, page_size=2)
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE vk_crawl_continuation SET status='running',attempts=2,"
            "lease_owner='dead',locked_at=datetime('now','-1 hour'),"
            "lease_expires_at=datetime('now','-1 minute'),run_id='dead-run'"
        )
        await conn.commit()

    async def wall(*_args, **_kwargs):
        return []

    monkeypatch.setattr(main, "vk_wall_since", wall)
    outcome = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, worker_id="alive", run_id="alive-run"
    )
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT status,attempts,lease_owner,run_id,completed_at FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert outcome["stale_recovered"] == 1
    assert row[:4] == ("done", 3, None, None)
    assert row[4] is not None


@pytest.mark.asyncio
async def test_fetch_failure_records_typed_capped_retry(tmp_path, monkeypatch):
    db = await _db(tmp_path / "retry.sqlite")
    await _schedule(db, page_size=2)

    class RateLimited(RuntimeError):
        status = 429
        retry_after = 1000

    async def wall(*_args, **_kwargs):
        raise RateLimited("provider throttle")

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setenv("VK_CRAWL_CONTINUATION_BACKOFF_BASE_SECONDS", "2")
    monkeypatch.setenv("VK_CRAWL_CONTINUATION_BACKOFF_MAX_SECONDS", "5")
    outcome = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, worker_id="retry", run_id="retry"
    )
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT status,last_typed_reason,lease_owner,locked_by,run_id,"
                "CAST((julianday(next_attempt_at)-julianday(CURRENT_TIMESTAMP))*86400 AS INTEGER) "
                "FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert outcome["retried"] == 1
    assert row[:5] == ("retry", "VK_CRAWL_RATE_LIMITED", None, None, None)
    assert 3 <= row[5] <= 5


@pytest.mark.asyncio
async def test_concurrent_workers_cannot_process_same_row(tmp_path, monkeypatch):
    path = tmp_path / "concurrent.sqlite"
    db1 = await _db(path)
    db2 = Database(str(path))
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    await _schedule(db1, page_size=2)

    async def wall(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return []

    monkeypatch.setattr(main, "vk_wall_since", wall)
    first_task = asyncio.create_task(
        vk_intake.process_vk_crawl_continuations(
            db1, max_jobs=1, worker_id="worker-1", run_id="run-1"
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    second = await vk_intake.process_vk_crawl_continuations(
        db2, max_jobs=1, worker_id="worker-2", run_id="run-2"
    )
    release.set()
    first = await first_task
    assert first["claimed"] == 1
    assert second["claimed"] == 0
    assert calls == 1


@pytest.mark.asyncio
async def test_completed_continuation_repeat_is_idempotent(tmp_path, monkeypatch):
    db = await _db(tmp_path / "repeat.sqlite")
    page = _posts(1, newest_ts=100)
    await _schedule(db, page_size=3)

    async def wall(*_args, **_kwargs):
        return page

    monkeypatch.setattr(main, "vk_wall_since", wall)
    first = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, worker_id="first", run_id="first"
    )
    second = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, worker_id="second", run_id="second"
    )
    # Re-emitting the same immutable scan boundary after completion must not
    # recreate work even though the row's mutable offset may have advanced.
    await _schedule(db, page_size=3)
    async with db.raw_conn() as conn:
        counts = await (
            await conn.execute(
                "SELECT (SELECT COUNT(*) FROM vk_source_packet),"
                "(SELECT COUNT(*) FROM vk_inbox),"
                "(SELECT COUNT(*) FROM vk_crawl_continuation WHERE status='done')"
            )
        ).fetchone()
    assert first["completed"] == 1
    assert second["claimed"] == 0
    assert counts == (1, 1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["cursor", "replay"])
async def test_full_page_terminates_only_on_proven_boundary(tmp_path, monkeypatch, terminal):
    db = await _db(tmp_path / f"terminal-{terminal}.sqlite")
    if terminal == "cursor":
        page = [
            {"post_id": 6, "date": 101, "text": "new", "photos": []},
            {"post_id": 5, "date": 100, "text": "boundary", "photos": []},
        ]
        await _schedule(
            db, page_size=2, cursor_ts=100, cursor_post_id=5, since_ts=1
        )
        expected = "ORIGINAL_CURSOR_OVERLAP"
    else:
        page = _posts(2, newest_ts=200)
        await vk_intake._schedule_vk_crawl_continuation(
            db,
            group_id=1,
            owner_type="group",
            scan_mode="incremental",
            page_size=2,
            since_ts=1,
            offset=2,
            horizon_ts=1,
            original_cursor_ts=1,
            original_cursor_post_id=0,
            reason="test_replay",
            last_page_fingerprint=vk_intake._vk_continuation_page_fingerprint(page),
        )
        expected = "EXACT_PAGE_REPLAY"

    async def wall(*_args, **_kwargs):
        return page

    monkeypatch.setattr(main, "vk_wall_since", wall)
    outcome = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=3, worker_id=terminal, run_id=terminal
    )
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT status,last_typed_reason FROM vk_crawl_continuation"
            )
        ).fetchone()
        count = await (
            await conn.execute("SELECT COUNT(*) FROM vk_source_packet")
        ).fetchone()
    assert outcome["completed"] == 1
    assert row == ("done", expected)
    assert count[0] == 2


@pytest.mark.asyncio
async def test_incremental_natural_completion_does_not_schedule_spurious_continuation(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path / "natural.sqlite")
    backlog = _posts(3, newest_ts=int(time.time()) + 10)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'g','G')"
        )
        await conn.execute(
            "INSERT INTO vk_crawl_cursor(group_id,last_seen_ts,last_post_id,updated_at) "
            "VALUES(1,10,1,CURRENT_TIMESTAMP)"
        )
        await conn.commit()

    async def wall(_gid, _since, *, count, offset, **_kwargs):
        return backlog[offset : offset + count]

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_PAGE_SIZE", 2)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_MAX_PAGES_INC", 1)
    await vk_intake.crawl_once(db)
    async with db.raw_conn() as conn:
        count = await (
            await conn.execute("SELECT COUNT(*) FROM vk_crawl_continuation")
        ).fetchone()
    assert count[0] == 0
