import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from db import Database

import vk_review


def _patch_raw_conn_commit_locked_once(monkeypatch, db: Database):
    original_raw_conn = db.raw_conn
    commit_calls = 0
    rollback_calls = 0

    class LockingConnWrapper:
        def __init__(self, conn):
            self._conn = conn

        async def execute(self, *args, **kwargs):
            return await self._conn.execute(*args, **kwargs)

        async def commit(self):
            nonlocal commit_calls
            commit_calls += 1
            if commit_calls == 1:
                raise sqlite3.OperationalError("database is locked")
            return await self._conn.commit()

        async def rollback(self):
            nonlocal rollback_calls
            rollback_calls += 1
            return await self._conn.rollback()

        def __getattr__(self, item):
            return getattr(self._conn, item)

    def locking_raw_conn():
        context = original_raw_conn()

        class _Ctx:
            async def __aenter__(self_nonlocal):
                conn = await context.__aenter__()
                return LockingConnWrapper(conn)

            async def __aexit__(self_nonlocal, exc_type, exc, tb):
                return await context.__aexit__(exc_type, exc, tb)

        return _Ctx()

    monkeypatch.setattr(db, "raw_conn", locking_raw_conn)
    return lambda: (commit_calls, rollback_calls)


@pytest.mark.asyncio
async def test_release_stale_locks_retries_on_locked_commit(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    stale_locked_at = (
        datetime.now(timezone.utc) - timedelta(seconds=vk_review.LOCK_TIMEOUT_SECONDS + 60)
    ).strftime("%Y-%m-%d %H:%M:%S")
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO vk_inbox(
                id, group_id, post_id, date, text, matched_kw, has_date,
                event_ts_hint, status, locked_by, locked_at, review_batch
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'locked', ?, ?, ?)
            """,
            (1, 1, 1, 0, "text", "k", 1, 9999999999, 77, stale_locked_at, "batch-x"),
        )
        await conn.commit()

    lock_stats = _patch_raw_conn_commit_locked_once(monkeypatch, db)

    released = await vk_review.release_stale_locks(db)

    commit_calls, rollback_calls = lock_stats()
    assert released == 1
    assert commit_calls >= 2
    assert rollback_calls >= 1

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, locked_by, locked_at, review_batch FROM vk_inbox WHERE id=1"
        )
        row = await cur.fetchone()
    assert row == ("pending", None, None, None)
