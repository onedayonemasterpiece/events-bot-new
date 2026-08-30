from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import scheduling
import video_announce.scenario as scenario_module
from db import Database
from models import User


class _FixedPartnerEcoMidday(datetime):
    """13:00 Europe/Kaliningrad → after the eco cron at 12:30 fired."""

    fixed_now = datetime(2026, 5, 14, 11, 0, tzinfo=timezone.utc)  # 13:00 local

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        if tz is not None:
            return value.astimezone(tz)
        return value.replace(tzinfo=None)


class _FixedPartnerEastEvening(datetime):
    """19:00 Europe/Kaliningrad → after the east cron at 18:30 fired."""

    fixed_now = datetime(2026, 5, 14, 17, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        if tz is not None:
            return value.astimezone(tz)
        return value.replace(tzinfo=None)


class _FixedPartnerLateNight(datetime):
    """23:00 Europe/Kaliningrad → past the 22:00 retry deadline."""

    fixed_now = datetime(2026, 5, 14, 21, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        if tz is not None:
            return value.astimezone(tz)
        return value.replace(tzinfo=None)


class _FixedPartnerBeforeSlot(datetime):
    """11:00 Europe/Kaliningrad → before the eco cron at 12:30."""

    fixed_now = datetime(2026, 5, 14, 9, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        if tz is not None:
            return value.astimezone(tz)
        return value.replace(tzinfo=None)


async def _insert_partner_session(
    db: Database,
    *,
    profile_key: str,
    target_date: str,
    status: str,
    created_at: str,
    kaggle_dataset: str | None = None,
    kaggle_kernel_ref: str | None = None,
) -> int:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            INSERT INTO videoannounce_session(
                status, profile_key, selection_params, created_at,
                kaggle_dataset, kaggle_kernel_ref
            )
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                status,
                profile_key,
                json.dumps({"target_date": target_date, "mode": "popular_review"}),
                created_at,
                kaggle_dataset,
                kaggle_kernel_ref,
            ),
        )
        await conn.commit()
        return int(cur.lastrowid)


async def _insert_partner_skip_ops_run(
    db: Database,
    *,
    kind: str,
    partner_track_id: str,
    skip_reason: str,
    started_at: str,
) -> int:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            INSERT INTO ops_run(kind, trigger, started_at, finished_at, status, details_json)
            VALUES(?, 'scheduled', ?, ?, 'skipped', ?)
            """,
            (
                kind,
                started_at,
                started_at,
                json.dumps(
                    {
                        "partner_track_id": partner_track_id,
                        "skip_reason": skip_reason,
                    }
                ),
            ),
        )
        await conn.commit()
    return int(cur.lastrowid)


async def _insert_partner_failed_ops_run(
    db: Database,
    *,
    kind: str,
    partner_track_id: str,
    error: str,
    started_at: str,
) -> int:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            INSERT INTO ops_run(kind, trigger, started_at, finished_at, status, details_json)
            VALUES(?, 'scheduled', ?, ?, 'failed', ?)
            """,
            (
                kind,
                started_at,
                started_at,
                json.dumps(
                    {
                        "partner_track_id": partner_track_id,
                        "error": error,
                    }
                ),
            ),
        )
        await conn.commit()
        return int(cur.lastrowid)


@pytest.mark.asyncio
async def test_scheduled_partner_track_lane_busy_is_explicit_skip(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        await session.commit()

    class FakeScenario:
        last_partner_track_skip_reason = "video_lanes_busy"

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def run_partner_track_pipeline(self, *_args, **_kwargs) -> None:
            return None

    monkeypatch.setattr(scenario_module, "VideoAnnounceScenario", FakeScenario)

    await scheduling._run_scheduled_partner_track(
        db,
        bot=object(),
        partner_track_id="partner_eco_nature_001",
    )

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, details_json FROM ops_run WHERE kind='video_partner_eco' ORDER BY id DESC LIMIT 1"
        )
        row = await cur.fetchone()
    assert row is not None
    status, details_raw = row
    details = json.loads(details_raw)
    assert status == "skipped"
    assert details["partner_track_id"] == "partner_eco_nature_001"
    assert details["skip_reason"] == "video_lanes_busy"


@pytest.mark.asyncio
async def test_partner_eco_watchdog_dispatches_after_missed_slot(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerEcoMidday)

    calls: list[tuple[str, dict]] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append((partner_track_id, kwargs))

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_eco_nature_001"
    )

    assert dispatched is True
    assert calls == [("partner_eco_nature_001", {"startup_catchup": False})]


@pytest.mark.asyncio
async def test_partner_eco_watchdog_defers_recent_busy_lane_skip(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerEcoMidday)
    monkeypatch.setenv("V_TOMORROW_WATCHDOG_BUSY_RETRY_SECONDS", "900")
    await _insert_partner_skip_ops_run(
        db,
        kind="video_partner_eco",
        partner_track_id="partner_eco_nature_001",
        skip_reason="video_lanes_busy",
        started_at="2026-05-14 10:55:00",
    )
    calls: list[str] = []

    async def fake_run(_db, _bot, partner_track_id, **_kwargs):
        calls.append(partner_track_id)

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_eco_nature_001"
    )

    assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_partner_eco_watchdog_skips_if_session_already_published_today(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerEcoMidday)
    await _insert_partner_session(
        db,
        profile_key="popular_review_eco",
        target_date="2026-05-14",
        status="PUBLISHED_TEST",
        created_at="2026-05-14 10:35:00",
        kaggle_dataset="zigomaro/cherryflash-session-901",
        kaggle_kernel_ref="zigomaro/cherryflash",
    )

    calls: list[tuple[str, dict]] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append((partner_track_id, kwargs))

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_eco_nature_001"
    )

    assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_partner_east_watchdog_dispatches_after_missed_slot(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerEastEvening)

    calls: list[str] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append(partner_track_id)

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_region_east_001"
    )

    assert dispatched is True
    assert calls == ["partner_region_east_001"]


@pytest.mark.asyncio
async def test_partner_east_watchdog_defers_after_two_missing_business_attempts(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerEastEvening)
    await _insert_partner_skip_ops_run(
        db,
        kind="video_partner_east",
        partner_track_id="partner_region_east_001",
        skip_reason="missing_business_target",
        started_at="2026-05-14 16:35:00",
    )
    await _insert_partner_skip_ops_run(
        db,
        kind="video_partner_east",
        partner_track_id="partner_region_east_001",
        skip_reason="missing_business_target",
        started_at="2026-05-14 16:45:00",
    )

    calls: list[str] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append(partner_track_id)

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_region_east_001"
    )

    assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_partner_east_watchdog_defers_after_legacy_failed_no_session_attempts(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerEastEvening)
    for started_at in ("2026-05-14 16:35:00", "2026-05-14 16:45:00"):
        await _insert_partner_failed_ops_run(
            db,
            kind="video_partner_east",
            partner_track_id="partner_region_east_001",
            error="partner track partner_region_east_001 did not create a session",
            started_at=started_at,
        )

    calls: list[str] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append(partner_track_id)

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_region_east_001"
    )

    assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_partner_watchdog_stops_retrying_after_22_00_local(
    tmp_path, monkeypatch
):
    """Hard deadline: after 22:00 local the watchdog must not relaunch even
    when no session exists for today — we do not publish stories at midnight."""
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerLateNight)

    calls: list[str] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append(partner_track_id)

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    for track_id in ("partner_eco_nature_001", "partner_region_east_001"):
        dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
            db, bot=object(), partner_track_id=track_id
        )
        assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_partner_watchdog_silent_before_scheduled_slot(
    tmp_path, monkeypatch
):
    """Watchdog must not fire before the cron slot itself fires."""
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerBeforeSlot)

    calls: list[str] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append(partner_track_id)

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_eco_nature_001"
    )

    assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_partner_watchdog_only_counts_matching_profile_key(
    tmp_path, monkeypatch
):
    """A successful base CherryFlash session today must not satisfy the
    partner-eco watchdog — each track has its own profile_key scope."""
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedPartnerEcoMidday)
    await _insert_partner_session(
        db,
        profile_key="popular_review",  # base CherryFlash, not eco
        target_date="2026-05-14",
        status="PUBLISHED_TEST",
        created_at="2026-05-14 08:30:00",
        kaggle_dataset="zigomaro/cherryflash-session-900",
        kaggle_kernel_ref="zigomaro/cherryflash",
    )

    calls: list[str] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append(partner_track_id)

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_eco_nature_001"
    )
    assert dispatched is True
    assert calls == ["partner_eco_nature_001"]


def test_partner_track_default_times():
    assert (
        scheduling.PARTNER_TRACK_DEFAULT_TIMES["partner_eco_nature_001"] == "12:30"
    )
    assert (
        scheduling.PARTNER_TRACK_DEFAULT_TIMES["partner_konb_library_001"] == "12:37"
    )
    assert (
        scheduling.PARTNER_TRACK_DEFAULT_TIMES["partner_region_east_001"] == "18:30"
    )
    assert scheduling.PARTNER_TRACK_TZ == "Europe/Kaliningrad"


def test_partner_track_time_env_override(monkeypatch):
    monkeypatch.setenv("V_PARTNER_TRACK_ECO_TIME_LOCAL", "13:45")
    assert scheduling._partner_track_time_local("partner_eco_nature_001") == "13:45"
    monkeypatch.delenv("V_PARTNER_TRACK_ECO_TIME_LOCAL")
    assert scheduling._partner_track_time_local("partner_eco_nature_001") == "12:30"
    monkeypatch.setenv("V_PARTNER_TRACK_KONB_TIME_LOCAL", "12:55")
    assert scheduling._partner_track_time_local("partner_konb_library_001") == "12:55"
