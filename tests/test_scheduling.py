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


class _FixedPopularReviewDatetime(datetime):
    fixed_now = datetime(2026, 4, 12, 9, 0, tzinfo=timezone.utc)

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


async def _insert_popular_review_session(
    db: Database,
    *,
    status: str,
    target_date: str,
    created_at: str,
    kaggle_dataset: str | None = None,
    kaggle_kernel_ref: str | None = None,
    error: str | None = None,
) -> int:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            INSERT INTO videoannounce_session(
                status, profile_key, selection_params, created_at,
                kaggle_dataset, kaggle_kernel_ref, error
            )
            VALUES(?, 'popular_review', ?, ?, ?, ?, ?)
            """,
            (
                status,
                json.dumps({"target_date": target_date, "mode": "popular_review"}),
                created_at,
                kaggle_dataset,
                kaggle_kernel_ref,
                error,
            ),
        )
        await conn.commit()
        return int(cur.lastrowid)


def _configure_video_tomorrow_env(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_V_TOMORROW_SCHEDULED", "1")
    monkeypatch.setenv("V_TOMORROW_TZ", "Europe/Kaliningrad")
    monkeypatch.setenv("V_TOMORROW_TIME_LOCAL", "16:45")
    monkeypatch.delenv("ENABLE_V_TEST_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.setattr(scheduling, "datetime", _FixedVideoTomorrowDatetime)


def _configure_popular_review_env(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_V_POPULAR_REVIEW_SCHEDULED", "1")
    monkeypatch.setenv("V_POPULAR_REVIEW_TZ", "Europe/Kaliningrad")
    monkeypatch.setenv("V_POPULAR_REVIEW_TIME_LOCAL", "10:15")
    monkeypatch.setattr(scheduling, "datetime", _FixedPopularReviewDatetime)


class _FixedCriticalSchedulerDatetime(datetime):
    fixed_now = datetime(2026, 4, 13, 21, 5, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        if tz is not None:
            return value.astimezone(tz)
        return value.replace(tzinfo=None)


def _configure_guide_critical_env(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_GUIDE_EXCURSIONS_SCHEDULED", "1")
    monkeypatch.setenv("GUIDE_EXCURSIONS_TZ", "Europe/Kaliningrad")
    monkeypatch.setenv("GUIDE_EXCURSIONS_FULL_TIME_LOCAL", "20:10")
    monkeypatch.delenv("CRITICAL_SCHED_WATCHDOG_GRACE_SECONDS", raising=False)
    monkeypatch.delenv("GUIDE_MONITORING_MISFIRE_GRACE_SECONDS", raising=False)
    monkeypatch.setattr(scheduling, "datetime", _FixedCriticalSchedulerDatetime)


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


@pytest.mark.asyncio
async def test_scheduled_popular_review_waits_for_confirmed_kaggle_handoff(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ADMIN_CHAT_ID", "123")

    class FakeScenario:
        def __init__(self, db_obj, bot_obj, *, chat_id: int, user_id: int):
            self.db = db_obj
            self.chat_id = chat_id
            self.user_id = user_id

        async def run_popular_review_pipeline(self, *, wait_for_handoff: bool = False):
            assert wait_for_handoff is True
            return await _insert_popular_review_session(
                self.db,
                status="RENDERING",
                target_date="2026-04-12",
                created_at="2026-04-12 07:44:00",
                kaggle_dataset="zigomaro/cherryflash-session-200",
                kaggle_kernel_ref="zigomaro/cherryflash",
            )

    monkeypatch.setattr("video_announce.scenario.VideoAnnounceScenario", FakeScenario)

    await scheduling._run_scheduled_popular_review(db, bot=object())

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, details_json FROM ops_run WHERE kind='video_popular_review'"
        )
        row = await cur.fetchone()

    assert row is not None
    status, details_raw = row
    details = json.loads(details_raw)
    assert status == "success"
    assert details["session_status"] == "RENDERING"
    assert details["kaggle_dataset"] == "zigomaro/cherryflash-session-200"
    assert details["kaggle_kernel_ref"] == "zigomaro/cherryflash"


@pytest.mark.asyncio
async def test_scheduled_popular_review_fails_ops_run_without_kaggle_handoff(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ADMIN_CHAT_ID", "123")

    class FakeScenario:
        def __init__(self, db_obj, bot_obj, *, chat_id: int, user_id: int):
            self.db = db_obj

        async def run_popular_review_pipeline(self, *, wait_for_handoff: bool = False):
            assert wait_for_handoff is True
            return await _insert_popular_review_session(
                self.db,
                status="FAILED",
                target_date="2026-04-12",
                created_at="2026-04-12 07:44:00",
                kaggle_kernel_ref="local:CherryFlash",
                error="runtime restart before Kaggle handoff; rerun required",
            )

    monkeypatch.setattr("video_announce.scenario.VideoAnnounceScenario", FakeScenario)

    with pytest.raises(RuntimeError, match="did not reach confirmed Kaggle handoff"):
        await scheduling._run_scheduled_popular_review(db, bot=object())

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, details_json FROM ops_run WHERE kind='video_popular_review'"
        )
        row = await cur.fetchone()

    assert row is not None
    status, details_raw = row
    details = json.loads(details_raw)
    assert status == "failed"
    assert details["session_status"] == "FAILED"
    assert details["kaggle_kernel_ref"] == "local:CherryFlash"
    assert "confirmed Kaggle handoff" in details["error"]


@pytest.mark.asyncio
async def test_scheduled_popular_review_fails_ops_run_without_created_session(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ADMIN_CHAT_ID", "123")

    class FakeScenario:
        def __init__(self, db_obj, bot_obj, *, chat_id: int, user_id: int):
            self.db = db_obj

        async def run_popular_review_pipeline(self, *, wait_for_handoff: bool = False):
            assert wait_for_handoff is True
            return None

    monkeypatch.setattr("video_announce.scenario.VideoAnnounceScenario", FakeScenario)

    with pytest.raises(RuntimeError, match="did not create a popular_review session"):
        await scheduling._run_scheduled_popular_review(db, bot=object())

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, details_json FROM ops_run WHERE kind='video_popular_review'"
        )
        row = await cur.fetchone()

    assert row is not None
    status, details_raw = row
    details = json.loads(details_raw)
    assert status == "failed"
    assert details["error"] == "CherryFlash did not create a popular_review session"
    assert "session_id" not in details


@pytest.mark.asyncio
async def test_popular_review_startup_catchup_retries_failed_local_handoff(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_popular_review_env(monkeypatch)
    await _insert_popular_review_session(
        db,
        status="FAILED",
        target_date="2026-04-12",
        created_at="2026-04-12 07:44:00",
        kaggle_kernel_ref="local:CherryFlash",
        error="runtime restart before Kaggle handoff; rerun required",
    )

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_popular_review", fake_run)

    dispatched = await scheduling._maybe_catch_up_popular_review_on_startup(
        db, bot=object()
    )

    assert dispatched is True
    assert calls == [{"startup_catchup": True}]


@pytest.mark.asyncio
async def test_popular_review_watchdog_skips_existing_remote_handoff(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_popular_review_env(monkeypatch)
    await _insert_popular_review_session(
        db,
        status="FAILED",
        target_date="2026-04-12",
        created_at="2026-04-12 07:44:00",
        kaggle_dataset="zigomaro/cherryflash-session-181",
        kaggle_kernel_ref="zigomaro/cherryflash",
        error="runtime restart before Kaggle handoff; rerun required",
    )

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_popular_review", fake_run)

    dispatched = await scheduling.maybe_dispatch_popular_review_watchdog(
        db, bot=object()
    )

    assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_dispatches_guide_full_after_light_run_only(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()

    run_id = await start_ops_run(
        db,
        kind="guide_monitoring",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 4, 13, 18, 20, tzinfo=timezone.utc),
        details={"mode": "light"},
    )
    await finish_ops_run(
        db,
        run_id=run_id,
        status="success",
        finished_at=datetime(2026, 4, 13, 18, 27, tzinfo=timezone.utc),
        details={"mode": "light"},
    )

    calls: list[dict[str, str]] = []

    async def fake_run(_db, _bot, *, mode: str) -> None:
        calls.append({"mode": mode})

    @asynccontextmanager
    async def fake_heavy_operation(**kwargs):
        calls.append({"kind": kwargs["kind"], "guard": kwargs["mode"]})
        yield

    monkeypatch.setattr(scheduling, "_run_scheduled_guide_excursions", fake_run)
    monkeypatch.setattr(scheduling, "heavy_operation", fake_heavy_operation)

    dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert dispatched == 1
    assert calls == [
        {"kind": "guide_monitoring", "guard": "wait"},
        {"mode": "full"},
    ]


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_skips_guide_when_full_run_exists(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()

    run_id = await start_ops_run(
        db,
        kind="guide_monitoring",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 4, 13, 18, 20, tzinfo=timezone.utc),
        details={"mode": "full"},
    )
    await finish_ops_run(
        db,
        run_id=run_id,
        status="success",
        finished_at=datetime(2026, 4, 13, 18, 27, tzinfo=timezone.utc),
        details={"mode": "full"},
    )

    calls: list[dict[str, str]] = []

    async def fake_run(_db, _bot, *, mode: str) -> None:
        calls.append({"mode": mode})

    @asynccontextmanager
    async def fake_heavy_operation(**kwargs):
        calls.append({"kind": kwargs["kind"], "guard": kwargs["mode"]})
        yield

    monkeypatch.setattr(scheduling, "_run_scheduled_guide_excursions", fake_run)
    monkeypatch.setattr(scheduling, "heavy_operation", fake_heavy_operation)

    dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert dispatched == 0
    assert calls == []


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_retries_guide_after_remote_busy_skip(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()

    calls: list[dict[str, str]] = []

    async def fake_run(_db, _bot, *, mode: str) -> None:
        calls.append({"mode": mode})
        run_id = await start_ops_run(
            db,
            kind="guide_monitoring",
            trigger="scheduled",
            operator_id=0,
            started_at=datetime(2026, 4, 13, 18, 20, tzinfo=timezone.utc),
            details={
                "mode": "full",
                "errors": ["remote_telegram_session_busy: tg_monitoring"],
                "remote_telegram_session_conflicts": [
                    {
                        "job_type": "tg_monitoring",
                        "kernel_ref": "zigomaro/telegram-monitor-bot",
                    }
                ],
            },
        )
        await finish_ops_run(
            db,
            run_id=run_id,
            status="skipped",
            finished_at=datetime(2026, 4, 13, 18, 21, tzinfo=timezone.utc),
            details={
                "mode": "full",
                "errors": ["remote_telegram_session_busy: tg_monitoring"],
                "remote_telegram_session_conflicts": [
                    {
                        "job_type": "tg_monitoring",
                        "kernel_ref": "zigomaro/telegram-monitor-bot",
                    }
                ],
            },
        )

    @asynccontextmanager
    async def fake_heavy_operation(**kwargs):
        calls.append({"kind": kwargs["kind"], "guard": kwargs["mode"]})
        yield

    monkeypatch.setattr(scheduling, "_run_scheduled_guide_excursions", fake_run)
    monkeypatch.setattr(scheduling, "heavy_operation", fake_heavy_operation)

    first_dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )
    second_dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert first_dispatched == 1
    assert second_dispatched == 1
    assert calls == [
        {"kind": "guide_monitoring", "guard": "wait"},
        {"mode": "full"},
        {"kind": "guide_monitoring", "guard": "wait"},
        {"mode": "full"},
    ]
