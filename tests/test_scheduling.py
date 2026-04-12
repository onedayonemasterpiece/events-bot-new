from __future__ import annotations

import json
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import scheduling
import vk_intake
from db import Database
from heavy_ops import HeavyOpMeta
from ops_run import finish_ops_run, start_ops_run


def test_scheduler_and_extract_do_not_import_main(monkeypatch):
    original_main = sys.modules.pop("main", None)
    monkeypatch.delenv("ENABLE_NIGHTLY_PAGE_SYNC", raising=False)

    class DummyExecutor:
        pass

    class DummyJob:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = None

    class DummyScheduler:
        def __init__(self, executors=None, timezone=None):
            self.executors = executors
            self.timezone = timezone
            self.jobs: dict[str, DummyJob] = {}
            self.listeners = []
            self.started = False

        def configure(self, job_defaults=None):
            self.job_defaults = job_defaults

        def add_job(self, func, trigger, id, args=None, **kwargs):
            job = DummyJob(id)
            self.jobs[id] = job
            return job

        def get_job(self, job_id):
            return self.jobs.get(job_id)

        def add_listener(self, listener, mask):
            self.listeners.append((listener, mask))

        def start(self):
            self.started = True

        def shutdown(self, wait=False):
            self.started = False

    monkeypatch.setattr(scheduling, "AsyncIOExecutor", lambda: DummyExecutor())
    monkeypatch.setattr(scheduling, "AsyncIOScheduler", DummyScheduler)
    monkeypatch.setattr(scheduling, "_scheduler", None)

    try:
        scheduler = scheduling.startup(
            db=None,
            bot=None,
            vk_scheduler=lambda *a, **k: None,
            vk_poll_scheduler=lambda *a, **k: None,
            vk_crawl_cron=lambda *a, **k: None,
            cleanup_scheduler=lambda *a, **k: None,
            partner_notification_scheduler=lambda *a, **k: None,
            nightly_page_sync=lambda *a, **k: None,
            rebuild_fest_nav_if_changed=lambda *a, **k: None,
        )
        assert isinstance(scheduler, DummyScheduler)
        assert "main" not in sys.modules

        tz = ZoneInfo("UTC")
        ts_hint = vk_intake.extract_event_ts_hint("завтра", tz=tz)
        assert ts_hint is not None
        assert "main" not in sys.modules
    finally:
        scheduling.cleanup()
        if original_main is not None:
            sys.modules["main"] = original_main
        else:
            sys.modules.pop("main", None)


@pytest.mark.asyncio
async def test_job_wrapper_records_skipped_heavy_ops_run(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    monkeypatch.setenv("SCHED_HEAVY_GUARD_MODE", "skip")
    monkeypatch.delenv("SCHED_SERIALIZE_HEAVY_JOBS", raising=False)

    @asynccontextmanager
    async def fake_heavy_operation(**_kwargs):
        yield False

    async def should_not_run(*_args, **_kwargs):
        raise AssertionError("scheduled job body must not run when heavy guard skips it")

    blocked_meta = HeavyOpMeta(
        kind="tg_monitoring",
        trigger="scheduled",
        started_monotonic=time.monotonic(),
        run_id="blocked-run",
        operator_id=0,
        chat_id=None,
    )

    monkeypatch.setattr(scheduling, "heavy_operation", fake_heavy_operation)
    monkeypatch.setattr(scheduling, "current_heavy_meta", lambda: blocked_meta)

    wrapped = scheduling._job_wrapper("vk_auto_import", should_not_run)
    await wrapped(db, None)

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT kind, trigger, status, details_json FROM ops_run ORDER BY id ASC"
        )
        row = await cur.fetchone()

    assert row is not None
    kind, trigger, status, details_raw = row
    details = json.loads(details_raw)
    assert kind == "vk_auto_import"
    assert trigger == "scheduled"
    assert status == "skipped"
    assert details["skip_reason"] == "heavy_busy"
    assert details["blocked_by_kind"] == "tg_monitoring"


@pytest.mark.asyncio
async def test_job_wrapper_records_skipped_guide_monitoring_ops_run(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    monkeypatch.setenv("SCHED_HEAVY_GUARD_MODE", "skip")
    monkeypatch.delenv("SCHED_SERIALIZE_HEAVY_JOBS", raising=False)

    @asynccontextmanager
    async def fake_heavy_operation(**_kwargs):
        yield False

    async def should_not_run(*_args, **_kwargs):
        raise AssertionError("guide scheduler body must not run when heavy guard skips it")

    blocked_meta = HeavyOpMeta(
        kind="vk_auto_import",
        trigger="scheduled",
        started_monotonic=time.monotonic(),
        run_id="vk-stuck-run",
        operator_id=0,
        chat_id=None,
    )

    monkeypatch.setattr(scheduling, "heavy_operation", fake_heavy_operation)
    monkeypatch.setattr(scheduling, "current_heavy_meta", lambda: blocked_meta)

    wrapped = scheduling._job_wrapper("guide_excursions_full", should_not_run)
    await wrapped(db, None)

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT kind, trigger, status, details_json FROM ops_run ORDER BY id ASC"
        )
        row = await cur.fetchone()

    assert row is not None
    kind, trigger, status, details_raw = row
    details = json.loads(details_raw)
    assert kind == "guide_monitoring"
    assert trigger == "scheduled"
    assert status == "skipped"
    assert details["skip_reason"] == "heavy_busy"
    assert details["blocked_by_kind"] == "vk_auto_import"


class _FixedVideoTomorrowDatetime(datetime):
    fixed_now = datetime(2026, 4, 12, 15, 30, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        if tz is not None:
            return value.astimezone(tz)
        return value.replace(tzinfo=None)


async def _insert_video_tomorrow_session(
    db: Database,
    *,
    status: str,
    target_date: str,
    profile_key: str = "default",
    error: str | None = None,
    created_at: str,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO videoannounce_session(status, profile_key, selection_params, created_at, error)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                status,
                profile_key,
                json.dumps({"target_date": target_date}),
                created_at,
                error,
            ),
        )
        await conn.commit()


def _configure_video_tomorrow_env(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_V_TOMORROW_SCHEDULED", "1")
    monkeypatch.setenv("V_TOMORROW_TZ", "Europe/Kaliningrad")
    monkeypatch.setenv("V_TOMORROW_TIME_LOCAL", "16:45")
    monkeypatch.delenv("ENABLE_V_TEST_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.setattr(scheduling, "datetime", _FixedVideoTomorrowDatetime)


@pytest.mark.asyncio
async def test_video_tomorrow_startup_catchup_retries_single_recoverable_failed_session(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_video_tomorrow_env(monkeypatch)

    run_id = await start_ops_run(
        db,
        kind="video_tomorrow",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 4, 12, 14, 45, tzinfo=timezone.utc),
    )
    await finish_ops_run(db, run_id=run_id, status="success")
    await _insert_video_tomorrow_session(
        db,
        status="FAILED",
        target_date="2026-04-13",
        error="missing video output",
        created_at="2026-04-12 14:46:00",
    )

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_video_tomorrow", fake_run)

    dispatched = await scheduling._maybe_catch_up_video_tomorrow_on_startup(
        db, bot=object()
    )

    assert dispatched is True
    assert calls == [
        {"profile_key": "default", "test_mode": False, "startup_catchup": True}
    ]


@pytest.mark.asyncio
async def test_video_tomorrow_watchdog_retries_single_recoverable_failed_session(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_video_tomorrow_env(monkeypatch)

    run_id = await start_ops_run(
        db,
        kind="video_tomorrow",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 4, 12, 14, 45, tzinfo=timezone.utc),
    )
    await finish_ops_run(db, run_id=run_id, status="success")
    await _insert_video_tomorrow_session(
        db,
        status="FAILED",
        target_date="2026-04-13",
        error="kaggle push failed",
        created_at="2026-04-12 14:46:00",
    )

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_video_tomorrow", fake_run)

    dispatched = await scheduling.maybe_dispatch_video_tomorrow_watchdog(
        db, bot=object()
    )

    assert dispatched is True
    assert calls == [
        {"profile_key": "default", "test_mode": False, "startup_catchup": False}
    ]


@pytest.mark.asyncio
async def test_video_tomorrow_startup_catchup_skips_second_recoverable_retry_same_day(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_video_tomorrow_env(monkeypatch)

    run_id = await start_ops_run(
        db,
        kind="video_tomorrow",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 4, 12, 14, 45, tzinfo=timezone.utc),
    )
    await finish_ops_run(db, run_id=run_id, status="success")
    await _insert_video_tomorrow_session(
        db,
        status="FAILED",
        target_date="2026-04-13",
        error="missing video output",
        created_at="2026-04-12 14:46:00",
    )
    await _insert_video_tomorrow_session(
        db,
        status="FAILED",
        target_date="2026-04-13",
        error="missing video output",
        created_at="2026-04-12 15:00:00",
    )

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_video_tomorrow", fake_run)

    dispatched = await scheduling._maybe_catch_up_video_tomorrow_on_startup(
        db, bot=object()
    )

    assert dispatched is False
    assert calls == []
