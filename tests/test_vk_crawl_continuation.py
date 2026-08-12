from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path

import pytest
import pytest_asyncio

import main
from db import Database
import vk_intake
from vk_source_envelope import build_vk_source_envelope


@pytest_asyncio.fixture(autouse=True)
async def _dispose_test_databases(monkeypatch):
    """Close every Database created by this module before interpreter shutdown."""

    instances: list[Database] = []
    original_init = Database.__init__

    def tracked_init(instance, *args, **kwargs):
        original_init(instance, *args, **kwargs)
        instances.append(instance)

    monkeypatch.setattr(Database, "__init__", tracked_init)
    yield
    for instance in instances:
        await instance.close()


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


async def _persist_posts(db: Database, posts) -> None:
    for post in posts:
        await vk_intake._persist_vk_source_packet(
            db,
            group_id=1,
            owner_type="group",
            post=post,
            source_url=f"https://vk.com/wall-1_{post['post_id']}",
            keyword_hints=(),
            date_hints=(),
            event_ts_hint=None,
        )


@pytest.mark.asyncio
async def test_continuation_persists_same_v1_envelope_contract_as_primary_crawl(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path / "envelope.sqlite")
    await _schedule(db, page_size=1)
    envelope = build_vk_source_envelope(
        {
            "id": 77,
            "date": int(time.time()),
            "text": "outer event",
            "attachments": [],
            "copy_history": [
                {"id": 78, "text": "nested details", "attachments": []}
            ],
        },
        owner_id=1,
        media_limit=None,
    )
    calls = 0

    async def wall(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return [envelope]

    monkeypatch.setattr(main, "vk_wall_since", wall)
    result = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=1, worker_id="envelope", run_id="r"
    )
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT raw_payload_json,envelope_version,capture_complete,evidence_replayability "
            "FROM vk_source_packet WHERE post_id=77"
        )).fetchone()
    assert calls == 1
    assert result["added"] == 1
    assert '"schema":"vk_source_envelope"' in row[0]
    assert row[1:] == (1, 1, "replayable_lossless")


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
        "last_page_fingerprint", "deepest_page_ts", "deepest_page_post_id",
        "completed_at",
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
async def test_full_page_original_cursor_is_a_proven_terminal(tmp_path, monkeypatch):
    db = await _db(tmp_path / "terminal-cursor.sqlite")
    page = [
        {"post_id": 6, "date": 101, "text": "new", "photos": []},
        {"post_id": 5, "date": 100, "text": "boundary", "photos": []},
    ]
    await _schedule(
        db, page_size=2, cursor_ts=100, cursor_post_id=5, since_ts=1
    )

    async def wall(*_args, **_kwargs):
        return page

    monkeypatch.setattr(main, "vk_wall_since", wall)
    outcome = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=3, worker_id="cursor", run_id="cursor"
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
    assert row == ("done", "ORIGINAL_CURSOR_OVERLAP")
    assert count[0] == 2


@pytest.mark.asyncio
async def test_exact_full_page_rebases_with_retry_and_never_completes(tmp_path, monkeypatch):
    db = await _db(tmp_path / "exact-rebase.sqlite")
    page = _posts(2, newest_ts=200)
    await _persist_posts(db, page)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_crawl_cursor(group_id,last_seen_ts,last_post_id,updated_at) "
            "VALUES(1,123,45,CURRENT_TIMESTAMP)"
        )
        await conn.commit()
    deepest = vk_intake._vk_continuation_deepest_boundary(page)
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
        deepest_page_ts=deepest[0],
        deepest_page_post_id=deepest[1],
    )

    async def wall(*_args, **_kwargs):
        return page

    monkeypatch.setattr(main, "vk_wall_since", wall)
    outcome = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=3, worker_id="replay", run_id="replay"
    )
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
        reason="repeat_producer",
        deepest_page_ts=deepest[0],
        deepest_page_post_id=deepest[1],
    )
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT status,offset,last_typed_reason,completed_at,deepest_page_ts,"
                "deepest_page_post_id,(SELECT COUNT(*) FROM vk_crawl_continuation),"
                "CAST((julianday(next_attempt_at)-julianday(CURRENT_TIMESTAMP))*86400 "
                "AS INTEGER) "
                "FROM vk_crawl_continuation"
            )
        ).fetchone()
        cursor = await (
            await conn.execute(
                "SELECT last_seen_ts,last_post_id FROM vk_crawl_cursor WHERE group_id=1"
            )
        ).fetchone()
    assert outcome["completed"] == 0
    assert outcome["rebased"] == outcome["retried"] == 1
    assert row[:7] == ("retry", 4, "OFFSET_DRIFT", None, deepest[0], deepest[1], 1)
    assert 28 <= row[7] <= 30
    assert cursor == (123, 45)


@pytest.mark.asyncio
@pytest.mark.parametrize("inserted_count", [1, 2, 5])
async def test_head_insert_drift_less_equal_and_greater_than_page_drains_tail(
    tmp_path, monkeypatch, inserted_count
):
    page_size = 2
    db = await _db(tmp_path / f"insert-{inserted_count}.sqlite")
    old_wall = _posts(13, newest_ts=1000)
    primary_prefix = old_wall[:page_size]
    inserted = _posts(inserted_count, first_id=100, newest_ts=2000)

    # The ordinary primary crawl owns the mutable head; the continuation owns
    # the older frozen tail. Seed both raw-first sides before resuming the row.
    await _persist_posts(db, primary_prefix)
    await _persist_posts(db, inserted)
    deepest = vk_intake._vk_continuation_deepest_boundary(primary_prefix)
    await vk_intake._schedule_vk_crawl_continuation(
        db,
        group_id=1,
        owner_type="group",
        scan_mode="incremental",
        page_size=page_size,
        since_ts=1,
        offset=page_size,
        horizon_ts=1,
        original_cursor_ts=1,
        original_cursor_post_id=0,
        reason="head_insert_drift",
        last_page_fingerprint=vk_intake._vk_continuation_page_fingerprint(
            primary_prefix
        ),
        deepest_page_ts=deepest[0],
        deepest_page_post_id=deepest[1],
    )
    mutable_wall = inserted + old_wall

    async def wall(_gid, _since, *, count, offset, **_kwargs):
        return mutable_wall[offset : offset + count]

    monkeypatch.setattr(main, "vk_wall_since", wall)
    saw_rebase = False
    for index in range(30):
        outcome = await vk_intake.process_vk_crawl_continuations(
            db,
            max_jobs=1,
            max_pages_per_job=1,
            worker_id=f"insert-{inserted_count}-{index}",
            run_id=f"insert-{inserted_count}-{index}",
        )
        saw_rebase = saw_rebase or bool(outcome["rebased"])
        async with db.raw_conn() as conn:
            state = await (
                await conn.execute(
                    "SELECT status,last_typed_reason FROM vk_crawl_continuation"
                )
            ).fetchone()
            if state[0] == "done":
                assert state[1] in {"EMPTY_PAGE", "SHORT_PAGE"}
                break
            if state[0] == "retry":
                await conn.execute(
                    "UPDATE vk_crawl_continuation SET next_attempt_at=CURRENT_TIMESTAMP"
                )
                await conn.commit()
    else:
        pytest.fail("continuation did not reach a real wall boundary")

    async with db.raw_conn() as conn:
        ids = {
            row[0]
            for row in await (
                await conn.execute("SELECT post_id FROM vk_source_packet")
            ).fetchall()
        }
        row = await (
            await conn.execute(
                "SELECT status,last_typed_reason,deepest_page_ts,deepest_page_post_id "
                "FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert ids == {post["post_id"] for post in mutable_wall}
    assert row[0] == "done" and row[1] in {"EMPTY_PAGE", "SHORT_PAGE"}
    assert (row[2], row[3]) == vk_intake._vk_continuation_deepest_boundary(old_wall)
    assert saw_rebase is (inserted_count >= page_size)


@pytest.mark.asyncio
async def test_provider_ignoring_offset_is_bounded_retry_not_done(tmp_path, monkeypatch):
    db = await _db(tmp_path / "ignored-offset.sqlite")
    page = _posts(2, newest_ts=300)
    await _persist_posts(db, page)
    deepest = vk_intake._vk_continuation_deepest_boundary(page)
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
        reason="ignored_offset",
        last_page_fingerprint=vk_intake._vk_continuation_page_fingerprint(page),
        deepest_page_ts=deepest[0],
        deepest_page_post_id=deepest[1],
    )
    calls = 0

    async def wall(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return page

    monkeypatch.setattr(main, "vk_wall_since", wall)
    for index in range(2):
        outcome = await vk_intake.process_vk_crawl_continuations(
            db,
            max_jobs=5,
            max_pages_per_job=5,
            worker_id=f"ignored-{index}",
            run_id=f"ignored-{index}",
        )
        assert outcome["claimed"] == outcome["rebased"] == 1
        async with db.raw_conn() as conn:
            row = await (
                await conn.execute(
                    "SELECT status,offset,completed_at FROM vk_crawl_continuation"
                )
            ).fetchone()
            assert row == ("retry", 4 + index * 2, None)
            await conn.execute(
                "UPDATE vk_crawl_continuation SET next_attempt_at=CURRENT_TIMESTAMP"
            )
            await conn.commit()
    assert calls == 2


@pytest.mark.asyncio
async def test_rebased_offset_survives_restart_and_drains_tail(tmp_path, monkeypatch):
    path = tmp_path / "restart.sqlite"
    db = await _db(path)
    page_size = 2
    old_wall = _posts(6, newest_ts=600)
    inserted = _posts(2, first_id=100, newest_ts=900)
    await _persist_posts(db, old_wall[:page_size])
    await _persist_posts(db, inserted)
    deepest = vk_intake._vk_continuation_deepest_boundary(old_wall[:page_size])
    await vk_intake._schedule_vk_crawl_continuation(
        db,
        group_id=1,
        owner_type="group",
        scan_mode="incremental",
        page_size=page_size,
        since_ts=1,
        offset=page_size,
        horizon_ts=1,
        original_cursor_ts=1,
        original_cursor_post_id=0,
        reason="restart",
        last_page_fingerprint=vk_intake._vk_continuation_page_fingerprint(
            old_wall[:page_size]
        ),
        deepest_page_ts=deepest[0],
        deepest_page_post_id=deepest[1],
    )
    mutable_wall = inserted + old_wall

    async def wall(_gid, _since, *, count, offset, **_kwargs):
        return mutable_wall[offset : offset + count]

    monkeypatch.setattr(main, "vk_wall_since", wall)
    first = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, max_pages_per_job=1, worker_id="before", run_id="before"
    )
    assert first["rebased"] == 1
    await db.close()

    restarted = Database(str(path))
    await restarted.init()
    async with restarted.raw_conn() as conn:
        await conn.execute(
            "UPDATE vk_crawl_continuation SET next_attempt_at=CURRENT_TIMESTAMP"
        )
        await conn.commit()
    for index in range(10):
        await vk_intake.process_vk_crawl_continuations(
            restarted,
            max_jobs=1,
            max_pages_per_job=1,
            worker_id=f"after-{index}",
            run_id=f"after-{index}",
        )
        async with restarted.raw_conn() as conn:
            state = await (
                await conn.execute(
                    "SELECT status FROM vk_crawl_continuation"
                )
            ).fetchone()
            if state[0] == "done":
                break
            await conn.execute(
                "UPDATE vk_crawl_continuation SET next_attempt_at=CURRENT_TIMESTAMP"
            )
            await conn.commit()
    async with restarted.raw_conn() as conn:
        state = await (
            await conn.execute(
                "SELECT status,last_typed_reason FROM vk_crawl_continuation"
            )
        ).fetchone()
        ids = {
            row[0]
            for row in await (
                await conn.execute("SELECT post_id FROM vk_source_packet")
            ).fetchall()
        }
    assert state[0] == "done" and state[1] in {"EMPTY_PAGE", "SHORT_PAGE"}
    assert ids == {post["post_id"] for post in mutable_wall}


@pytest.mark.asyncio
async def test_legacy_exact_done_reopens_on_initx2_and_schedule(tmp_path):
    path = tmp_path / "legacy-exact.sqlite"
    db = await _db(path)
    await _schedule(db, page_size=2)
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE vk_crawl_continuation SET status='done',"
            "last_typed_reason='EXACT_PAGE_REPLAY',completed_at=CURRENT_TIMESTAMP"
        )
        await conn.commit()
    await db.close()

    reopened = Database(str(path))
    await reopened.init()
    await reopened.init()
    async with reopened.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT status,last_typed_reason,completed_at,COUNT(*) "
                "FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert row == ("retry", "LEGACY_EXACT_PAGE_REPLAY_REOPENED", None, 1)

    # Defensive producer adoption repairs a poisoned row even without restart.
    async with reopened.raw_conn() as conn:
        await conn.execute(
            "UPDATE vk_crawl_continuation SET status='done',"
            "last_typed_reason='EXACT_PAGE_REPLAY',completed_at=CURRENT_TIMESTAMP"
        )
        await conn.commit()
    await _schedule(reopened, page_size=2)
    async with reopened.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT status,last_typed_reason,completed_at,COUNT(*) "
                "FROM vk_crawl_continuation"
            )
        ).fetchone()
    assert row == ("retry", "LEGACY_EXACT_PAGE_REPLAY_REOPENED", None, 1)


@pytest.mark.asyncio
async def test_legacy_target_offset_collision_stays_retry_not_stale_or_done(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path / "offset-collision.sqlite")
    page = _posts(2, newest_ts=400)
    await _persist_posts(db, page)
    deepest = vk_intake._vk_continuation_deepest_boundary(page)
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
        reason="collision_source",
        last_page_fingerprint=vk_intake._vk_continuation_page_fingerprint(page),
        deepest_page_ts=deepest[0],
        deepest_page_post_id=deepest[1],
    )
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO vk_crawl_continuation(
                source_type,owner_id,owner_type,scan_mode,page_size,since_ts,offset,
                horizon_ts,original_cursor_ts,original_cursor_post_id,reason,status
            ) VALUES('vk',1,'group','incremental',2,1,4,1,1,0,'legacy_duplicate','pending')
            """
        )
        await conn.commit()

    async def wall(*_args, **_kwargs):
        return page

    monkeypatch.setattr(main, "vk_wall_since", wall)
    outcome = await vk_intake.process_vk_crawl_continuations(
        db, max_jobs=1, worker_id="collision", run_id="collision"
    )
    async with db.raw_conn() as conn:
        rows = await (
            await conn.execute(
                "SELECT offset,status,last_typed_reason FROM vk_crawl_continuation "
                "ORDER BY id"
            )
        ).fetchall()
    assert outcome["completed"] == 0
    assert rows[0] == (2, "retry", "OFFSET_DRIFT_COLLISION")
    assert rows[1][:2] == (4, "pending")


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
