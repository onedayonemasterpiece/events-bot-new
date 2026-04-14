import os
import sqlite3
import sys

import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from db import Database
from ops_run import start_ops_run


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
async def test_start_ops_run_retries_on_locked_commit(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    lock_stats = _patch_raw_conn_commit_locked_once(monkeypatch, db)

    run_id = await start_ops_run(
        db,
        kind="vk_auto_import",
        trigger="scheduled",
        details={"run_id": "retry-test"},
    )

    commit_calls, rollback_calls = lock_stats()
    assert int(run_id or 0) > 0
    assert commit_calls >= 2
    assert rollback_calls >= 1

    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT kind, trigger FROM ops_run WHERE id=?", (run_id,))
        row = await cur.fetchone()
    assert row == ("vk_auto_import", "scheduled")
