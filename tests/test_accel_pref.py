from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from db import Database, close_known_databases
from video_announce import accel_pref


@pytest_asyncio.fixture
async def db(tmp_path: Path) -> Database:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    return db


@pytest_asyncio.fixture(autouse=True)
async def _close_databases_after_test():
    yield
    await close_known_databases()


@pytest.mark.asyncio
async def test_read_pref_returns_none_when_unset(db: Database):
    assert await accel_pref.read_active_pref(db, "zigomaro/cherryflash") is None


@pytest.mark.asyncio
async def test_write_and_read_roundtrip(db: Database):
    pref = accel_pref.AccelPref(
        tier=accel_pref.TIER_T4,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=12),
        reason="unit-test",
    )
    await accel_pref.write_pref(db, "zigomaro/cherryflash", pref)

    got = await accel_pref.read_active_pref(db, "zigomaro/cherryflash")
    assert got is not None
    assert got.tier == accel_pref.TIER_T4
    assert got.reason == "unit-test"


@pytest.mark.asyncio
async def test_expired_pref_returns_none(db: Database):
    pref = accel_pref.AccelPref(
        tier=accel_pref.TIER_T4,
        expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        reason="already-expired",
    )
    await accel_pref.write_pref(db, "zigomaro/cherryflash", pref)
    assert await accel_pref.read_active_pref(db, "zigomaro/cherryflash") is None


@pytest.mark.asyncio
async def test_demote_p100_to_t4(db: Database):
    new_pref = await accel_pref.demote(
        db,
        "zigomaro/cherryflash",
        current_tier=accel_pref.TIER_DEFAULT,
        reason="queue >300s",
    )
    assert new_pref is not None
    assert new_pref.tier == accel_pref.TIER_T4
    assert new_pref.expires_at > datetime.now(timezone.utc) + timedelta(hours=17)
    assert new_pref.expires_at < datetime.now(timezone.utc) + timedelta(hours=31)

    persisted = await accel_pref.read_active_pref(db, "zigomaro/cherryflash")
    assert persisted is not None
    assert persisted.tier == accel_pref.TIER_T4


@pytest.mark.asyncio
async def test_demote_t4_exhausts_ladder(db: Database):
    result = await accel_pref.demote(
        db,
        "zigomaro/cherryflash",
        current_tier=accel_pref.TIER_T4,
        reason="queue >300s on t4",
    )
    assert result is None


@pytest.mark.asyncio
async def test_demote_unknown_slug_returns_none(db: Database):
    result = await accel_pref.demote(
        db,
        "zigomaro/some-unmapped-kernel",
        current_tier=accel_pref.TIER_DEFAULT,
        reason="test",
    )
    assert result is None


@pytest.mark.asyncio
async def test_random_ttl_varies(monkeypatch):
    monkeypatch.setenv("KAGGLE_ACCEL_PREF_TTL_HOURS_MIN", "18")
    monkeypatch.setenv("KAGGLE_ACCEL_PREF_TTL_HOURS_MAX", "30")
    samples = {accel_pref.random_ttl().total_seconds() for _ in range(50)}
    # With a 12-hour range the chance of all samples landing on a single value
    # is effectively zero.
    assert len(samples) > 1
    for s in samples:
        assert 18 * 3600 <= s <= 30 * 3600


def test_tier_to_machine_shape_known():
    assert accel_pref.tier_to_machine_shape(accel_pref.TIER_DEFAULT) == "NvidiaTeslaP100"
    assert accel_pref.tier_to_machine_shape(accel_pref.TIER_T4) == "NvidiaTeslaT4"
    assert accel_pref.tier_to_machine_shape("unknown") is None


def test_ladder_for_known_slug():
    assert accel_pref.ladder_for("zigomaro/cherryflash") == [
        accel_pref.TIER_DEFAULT,
        accel_pref.TIER_T4,
    ]
    assert accel_pref.ladder_for("zigomaro/unknown") == []


def test_next_tier_step():
    assert accel_pref.next_tier(
        "zigomaro/cherryflash", accel_pref.TIER_DEFAULT
    ) == accel_pref.TIER_T4
    assert accel_pref.next_tier("zigomaro/cherryflash", accel_pref.TIER_T4) is None


def test_queue_demote_threshold_default():
    # Without env override, default 300s
    assert accel_pref.queue_demote_threshold_sec() == 300


def test_queue_demote_threshold_env_override(monkeypatch):
    monkeypatch.setenv("KAGGLE_QUEUE_DEMOTE_THRESHOLD_SEC", "60")
    assert accel_pref.queue_demote_threshold_sec() == 60
    monkeypatch.setenv("KAGGLE_QUEUE_DEMOTE_THRESHOLD_SEC", "garbage")
    assert accel_pref.queue_demote_threshold_sec() == 300
