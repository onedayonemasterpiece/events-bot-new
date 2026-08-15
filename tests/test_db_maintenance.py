from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import db as db_module
from db import Database, full_vacuum_with_safety


@pytest.mark.parametrize(
    ("configured", "expected_mb"),
    [(None, 64), ("invalid", 64), ("0", 4), ("1024", 256)],
)
def test_journal_size_limit_configuration_is_bounded(
    monkeypatch, configured, expected_mb
):
    if configured is None:
        monkeypatch.delenv("DB_WAL_JOURNAL_SIZE_LIMIT_MB", raising=False)
    else:
        monkeypatch.setenv("DB_WAL_JOURNAL_SIZE_LIMIT_MB", configured)

    assert Database._sqlite_journal_size_limit_bytes() == expected_mb * 1024 * 1024


@pytest.mark.asyncio
async def test_journal_size_limit_is_bounded_and_applied_to_all_connection_paths(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DB_WAL_JOURNAL_SIZE_LIMIT_MB", "2")
    database = Database(str(tmp_path / "db.sqlite"))
    await database.init()
    try:
        async with database.raw_conn() as conn:
            cursor = await conn.execute("PRAGMA journal_size_limit")
            assert (await cursor.fetchone())[0] == 4 * 1024 * 1024
            await cursor.close()

        async with database.engine.connect() as conn:
            result = await conn.exec_driver_sql("PRAGMA journal_size_limit")
            assert result.scalar_one() == 4 * 1024 * 1024
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_full_vacuum_skips_before_checkpoint_when_capacity_is_insufficient(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "db.sqlite"
    db_path.write_bytes(b"x" * 100)
    checkpoint = AsyncMock()
    run_vacuum = AsyncMock()
    monkeypatch.setattr(db_module, "wal_checkpoint_truncate", checkpoint)
    monkeypatch.setattr(db_module, "vacuum", run_vacuum)
    monkeypatch.setattr(
        db_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=249),
    )

    receipt = await full_vacuum_with_safety(
        object(), str(db_path), min_free_bytes=50
    )

    assert receipt["status"] == "skipped"
    assert receipt["reason"] == "insufficient_capacity"
    assert receipt["required_free_bytes"] == 250
    checkpoint.assert_not_awaited()
    run_vacuum.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_vacuum_skips_when_pre_checkpoint_is_busy(tmp_path, monkeypatch):
    db_path = tmp_path / "db.sqlite"
    db_path.write_bytes(b"x" * 100)
    checkpoint = AsyncMock(return_value=[(1, 20, 0)])
    run_vacuum = AsyncMock()
    monkeypatch.setattr(db_module, "wal_checkpoint_truncate", checkpoint)
    monkeypatch.setattr(db_module, "vacuum", run_vacuum)
    monkeypatch.setattr(
        db_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=10_000),
    )

    receipt = await full_vacuum_with_safety(
        object(), str(db_path), min_free_bytes=50
    )

    assert receipt["status"] == "skipped"
    assert receipt["reason"] == "checkpoint_pre_busy"
    assert receipt["checkpoint_pre"] == {
        "rows": [(1, 20, 0)],
        "busy": 1,
        "log_frames": 20,
        "checkpointed_frames": 0,
        "ok": False,
    }
    run_vacuum.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_vacuum_returns_pre_and_post_checkpoint_receipts(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "db.sqlite"
    db_path.write_bytes(b"x" * 100)
    checkpoint = AsyncMock(side_effect=[[(0, 0, 0)], [(0, 0, 0)]])
    run_vacuum = AsyncMock()
    monkeypatch.setattr(db_module, "wal_checkpoint_truncate", checkpoint)
    monkeypatch.setattr(db_module, "vacuum", run_vacuum)
    monkeypatch.setattr(
        db_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=10_000),
    )

    receipt = await full_vacuum_with_safety(
        object(), str(db_path), min_free_bytes=50
    )

    assert receipt["status"] == "success"
    assert receipt["checkpoint_pre"]["ok"] is True
    assert receipt["checkpoint_post"]["ok"] is True
    assert receipt["before"]["db_bytes"] == 100
    assert receipt["required_free_bytes"] == 250
    assert isinstance(receipt["duration_ms"], int)
    assert checkpoint.await_count == 2
    run_vacuum.assert_awaited_once()


@pytest.mark.asyncio
async def test_full_vacuum_rechecks_capacity_after_pre_checkpoint(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "db.sqlite"
    db_path.write_bytes(b"x" * 100)
    checkpoint = AsyncMock(return_value=[(0, 0, 0)])
    run_vacuum = AsyncMock()
    free_values = iter((10_000, 249))
    monkeypatch.setattr(db_module, "wal_checkpoint_truncate", checkpoint)
    monkeypatch.setattr(db_module, "vacuum", run_vacuum)
    monkeypatch.setattr(
        db_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=next(free_values)),
    )

    receipt = await full_vacuum_with_safety(
        object(), str(db_path), min_free_bytes=50
    )

    assert receipt["status"] == "skipped"
    assert receipt["reason"] == "capacity_changed_after_checkpoint"
    assert receipt["free_after_checkpoint_bytes"] == 249
    run_vacuum.assert_not_awaited()


def test_db_vacuum_uses_heavy_job_serialization_policy():
    import scheduling

    assert "db_vacuum" in scheduling._HEAVY_JOB_IDS


@pytest.mark.asyncio
async def test_full_vacuum_safety_wrapper_runs_against_sqlite(tmp_path):
    database = Database(str(tmp_path / "db.sqlite"))
    await database.init()
    try:
        async with database.raw_conn() as conn:
            await conn.execute("CREATE TABLE vacuum_probe(value TEXT)")
            await conn.execute(
                "INSERT INTO vacuum_probe(value) VALUES (?)",
                ("x" * 4096,),
            )
            await conn.commit()

        receipt = await full_vacuum_with_safety(
            database.engine,
            database.path,
            min_free_bytes=0,
        )

        assert receipt["status"] == "success"
        assert receipt["checkpoint_pre"]["ok"] is True
        assert receipt["checkpoint_post"]["ok"] is True
    finally:
        await database.close()
