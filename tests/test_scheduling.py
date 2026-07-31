from __future__ import annotations

import asyncio
import json
import sys
import time
from types import SimpleNamespace
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

import scheduling
import vk_intake
import video_announce.scenario as scenario_module
from db import Database
from heavy_ops import HeavyOpMeta
from models import User
from ops_run import finish_ops_run, start_ops_run


@pytest.mark.asyncio
async def test_poll_to_forward_debug_tick_accepts_scheduler_run_id(monkeypatch):
    calls = []

    async def fake_run_debug_tick(db, bot):
        calls.append((db, bot))
        return {"resolved": 1}

    monkeypatch.setitem(
        sys.modules,
        "poll_to_forward",
        SimpleNamespace(run_debug_tick=fake_run_debug_tick),
    )

    await scheduling._run_poll_to_forward_debug_tick("db", "bot", run_id="sched-run")

    assert calls == [("db", "bot")]


def test_scheduler_and_extract_do_not_import_main(monkeypatch):
    original_main = sys.modules.pop("main", None)
    monkeypatch.delenv("ENABLE_NIGHTLY_PAGE_SYNC", raising=False)

    class DummyExecutor:
        pass

    class DummyJob:
        def __init__(self, job_id: str, *, trigger=None, args=None, kwargs=None) -> None:
            self.id = job_id
            self.next_run_time = None
            self.trigger = trigger
            self.args = args
            self.kwargs = kwargs or {}

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
            job = DummyJob(id, trigger=trigger, args=args, kwargs=kwargs)
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
    monkeypatch.setenv("ENABLE_REGION_TALK_SCHEDULED", "1")
    monkeypatch.setenv("REGION_TALK_TIMES_LOCAL", "06:20,13:20,21:20")
    monkeypatch.setenv("REGION_TALK_TZ", "Europe/Kaliningrad")

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
        assert {"region_talk_0", "region_talk_1", "region_talk_2"} <= set(scheduler.jobs)
        assert "region_talk_watchdog" in scheduler.jobs
        assert scheduler.jobs["region_talk_0"].kwargs["hour"] == "4"
        assert scheduler.jobs["region_talk_0"].kwargs["minute"] == "20"
        assert scheduling._ops_run_kind_for_job("region_talk") == "region_talk"

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


class _FixedKenigsbergStoryDatetime(datetime):
    fixed_now = datetime(2026, 6, 12, 20, 45, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        if tz is not None:
            return value.astimezone(tz)
        return value.replace(tzinfo=None)


class _FixedKenigsbergStorySaturdayDatetime(_FixedKenigsbergStoryDatetime):
    fixed_now = datetime(2026, 6, 13, 20, 45, tzinfo=timezone.utc)


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


async def _insert_kenigsberg_story_session(
    db: Database,
    *,
    status: str,
    created_at: str,
    trigger: str = "startup_catchup",
    story_publish_requested: bool = True,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO videoannounce_session(status, profile_key, selection_params, created_at)
            VALUES(?, 'kenigsberg_story', ?, ?)
            """,
            (
                status,
                json.dumps(
                    {
                        "mode": "kenigsberg_story",
                        "trigger": trigger,
                        "story_publish_requested": story_publish_requested,
                    }
                ),
                created_at,
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
    video_url: str | None = None,
    error: str | None = None,
) -> int:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            INSERT INTO videoannounce_session(
                status, profile_key, selection_params, created_at,
                kaggle_dataset, kaggle_kernel_ref, video_url, error
            )
            VALUES(?, 'popular_review', ?, ?, ?, ?, ?, ?)
            """,
            (
                status,
                json.dumps({"target_date": target_date, "mode": "popular_review"}),
                created_at,
                kaggle_dataset,
                kaggle_kernel_ref,
                video_url,
                error,
            ),
        )
        await conn.commit()
        return int(cur.lastrowid)


async def _insert_video_session(
    db: Database,
    *,
    status: str,
    profile_key: str,
    target_date: str,
    created_at: str,
    kaggle_dataset: str | None = None,
    kaggle_kernel_ref: str | None = None,
    video_url: str | None = None,
    error: str | None = None,
) -> int:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            INSERT INTO videoannounce_session(
                status, profile_key, selection_params, created_at,
                kaggle_dataset, kaggle_kernel_ref, video_url, error
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                status,
                profile_key,
                json.dumps(
                    {
                        "target_date": target_date,
                        "mode": "popular_review",
                        "trigger": "scheduled",
                    }
                ),
                created_at,
                kaggle_dataset,
                kaggle_kernel_ref,
                video_url,
                error,
            ),
        )
        await conn.commit()
        return int(cur.lastrowid)


async def _insert_terminal_kaggle_ledger(db: Database, *, session_id: int) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO kaggle_run_ledger(
                run_id, session_id, kind, notebook, status, phase, token_hash,
                terminal_at
            )
            VALUES(?, ?, 'cherryflash', 'CherryFlash', 'done', 'cleanup', 'test', ?)
            """,
            (
                f"videoannounce:{session_id}",
                session_id,
                "2026-04-12T09:26:44+00:00",
            ),
        )
        await conn.commit()


async def _insert_live_kaggle_ledger(
    db: Database,
    *,
    session_id: int,
    heartbeat_at: str,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO kaggle_run_ledger(
                run_id, session_id, kind, notebook, status, phase, token_hash,
                updated_at, last_heartbeat_at
            )
            VALUES(?, ?, 'cherryflash', 'CherryFlash', 'alive', 'render', 'test', ?, ?)
            """,
            (
                f"videoannounce:{session_id}",
                session_id,
                heartbeat_at,
                heartbeat_at,
            ),
        )
        await conn.commit()


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


class _FixedCriticalAfterMidnightDatetime(datetime):
    fixed_now = datetime(2026, 6, 12, 22, 20, tzinfo=timezone.utc)

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
    _FixedCriticalSchedulerDatetime.fixed_now = datetime(2026, 4, 13, 21, 5, tzinfo=timezone.utc)
    monkeypatch.setattr(scheduling, "datetime", _FixedCriticalSchedulerDatetime)


def test_runtime_health_status_reports_guide_jobs(monkeypatch):
    monkeypatch.setenv("ENABLE_GUIDE_EXCURSIONS_SCHEDULED", "1")
    monkeypatch.delenv("ENABLE_V_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.delenv("ENABLE_V_TEST_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.setenv("DEV_MODE", "1")

    class Job:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = datetime(2026, 6, 6, 20, 10, tzinfo=timezone.utc)

    class Scheduler:
        running = True

        def __init__(self) -> None:
            self.jobs = {
                "guide_excursions_light_0": Job("guide_excursions_light_0"),
                "guide_excursions_full": Job("guide_excursions_full"),
            }

        def get_job(self, job_id: str):
            return self.jobs.get(job_id)

        def get_jobs(self):
            return list(self.jobs.values())

    monkeypatch.setattr(scheduling, "_scheduler", Scheduler())

    payload = scheduling.runtime_health_status()

    assert payload["scheduler"] == "ok"
    assert payload["guide_excursions_light"] == "ok"
    assert payload["guide_excursions_full"] == "ok"
    assert "guide_excursions_light_next_run" in payload
    assert "guide_excursions_full_next_run" in payload


def test_runtime_health_status_reports_cherryflash_and_promo_jobs(monkeypatch):
    monkeypatch.setenv("ENABLE_V_POPULAR_REVIEW_SCHEDULED", "1")
    monkeypatch.setenv("ENABLE_PROMO_VK_SCHEDULER", "1")
    monkeypatch.delenv("ENABLE_V_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.delenv("ENABLE_V_TEST_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.setenv("DEV_MODE", "1")

    class Job:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc)

    class Scheduler:
        running = True

        def __init__(self) -> None:
            self.jobs = {
                "video_popular_review": Job("video_popular_review"),
                "video_popular_review_watchdog": Job("video_popular_review_watchdog"),
                "promo_vk": Job("promo_vk"),
            }

        def get_job(self, job_id: str):
            return self.jobs.get(job_id)

        def get_jobs(self):
            return list(self.jobs.values())

    monkeypatch.setattr(scheduling, "_scheduler", Scheduler())

    payload = scheduling.runtime_health_status()

    assert payload["scheduler"] == "ok"
    assert payload["video_popular_review"] == "ok"
    assert payload["video_popular_review_watchdog"] == "ok"
    assert payload["promo_vk"] == "ok"
    assert "video_popular_review_next_run" in payload
    assert "video_popular_review_watchdog_next_run" in payload
    assert "promo_vk_next_run" in payload


def test_runtime_health_status_reports_critical_monitoring_jobs(monkeypatch):
    monkeypatch.setenv("ENABLE_TG_MONITORING", "1")
    monkeypatch.setenv("ENABLE_VK_AUTO_IMPORT", "1")
    monkeypatch.delenv("ENABLE_V_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.delenv("ENABLE_V_TEST_TOMORROW_SCHEDULED", raising=False)
    monkeypatch.setenv("DEV_MODE", "1")

    class Job:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = datetime(2026, 6, 13, 12, 0, tzinfo=timezone.utc)

    class Scheduler:
        running = True

        def __init__(self) -> None:
            self.jobs = {
                "critical_scheduler_watchdog": Job("critical_scheduler_watchdog"),
                "tg_monitoring": Job("tg_monitoring"),
                "vk_auto_import_0": Job("vk_auto_import_0"),
            }

        def get_job(self, job_id: str):
            return self.jobs.get(job_id)

        def get_jobs(self):
            return list(self.jobs.values())

    monkeypatch.setattr(scheduling, "_scheduler", Scheduler())

    payload = scheduling.runtime_health_status()

    assert payload["critical_scheduler_watchdog"] == "ok"
    assert payload["tg_monitoring"] == "ok"
    assert payload["vk_auto_import"] == "ok"
    assert "critical_scheduler_watchdog_next_run" in payload
    assert "tg_monitoring_next_run" in payload
    assert "vk_auto_import_next_run" in payload


def test_runtime_health_status_reports_region_talk_jobs(monkeypatch):
    monkeypatch.setenv("ENABLE_REGION_TALK_SCHEDULED", "1")
    monkeypatch.setenv("DEV_MODE", "1")

    class Job:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = datetime(2026, 7, 31, 19, 20, tzinfo=timezone.utc)

    class Scheduler:
        running = True

        def __init__(self) -> None:
            self.jobs = {
                "region_talk_0": Job("region_talk_0"),
                "region_talk_1": Job("region_talk_1"),
                "region_talk_2": Job("region_talk_2"),
                "region_talk_watchdog": Job("region_talk_watchdog"),
            }

        def get_job(self, job_id: str):
            return self.jobs.get(job_id)

        def get_jobs(self):
            return list(self.jobs.values())

    monkeypatch.setattr(scheduling, "_scheduler", Scheduler())

    payload = scheduling.runtime_health_status()

    assert payload["region_talk"] == "ok"
    assert payload["region_talk_next_run"] == "2026-07-31T19:20:00+00:00"
    assert payload["region_talk_watchdog"] == "ok"


def test_last_region_talk_slot_selects_latest_due_time(monkeypatch):
    monkeypatch.setenv("REGION_TALK_TIMES_LOCAL", "06:20,13:20,21:20")
    monkeypatch.setenv("REGION_TALK_TZ", "Europe/Kaliningrad")

    _now_local, scheduled_local, scheduled_utc = scheduling._last_region_talk_slot(
        datetime(2026, 7, 31, 20, 30, tzinfo=timezone.utc)
    )

    assert scheduled_local.isoformat() == "2026-07-31T21:20:00+02:00"
    assert scheduled_utc.isoformat() == "2026-07-31T19:20:00+00:00"


@pytest.mark.asyncio
async def test_region_talk_watchdog_resumes_crashed_slot(monkeypatch):
    monkeypatch.setenv("ENABLE_REGION_TALK_SCHEDULED", "1")
    monkeypatch.setenv("REGION_TALK_TIMES_LOCAL", "06:20,13:20,21:20")
    monkeypatch.setenv("REGION_TALK_TZ", "Europe/Kaliningrad")
    monkeypatch.setattr(
        scheduling,
        "_region_talk_slot_runs",
        lambda *_args, **_kwargs: asyncio.sleep(0, result=[("crashed", "scheduled")]),
    )
    calls = []

    async def fake_run(db, bot, **kwargs):
        calls.append((db, bot, kwargs))
        return {"ok": True, "status": "success"}

    monkeypatch.setattr(
        "scripts.region_talk_scheduled_runner.run_region_talk_scheduled",
        fake_run,
    )
    scheduling._region_talk_catchup_inflight.clear()

    result = await scheduling.maybe_dispatch_region_talk_watchdog(
        object(),
        object(),
        now_utc=datetime(2026, 7, 31, 20, 30, tzinfo=timezone.utc),
    )

    assert result is True
    assert len(calls) == 1
    assert calls[0][2]["ops_trigger"] == "watchdog_catchup"
    assert calls[0][2]["scheduler_run_id"].startswith("watchdog-catchup-20260731T192000Z-")


@pytest.mark.asyncio
async def test_region_talk_watchdog_skips_running_or_completed_slot(monkeypatch):
    monkeypatch.setenv("ENABLE_REGION_TALK_SCHEDULED", "1")
    monkeypatch.setattr(
        scheduling,
        "_region_talk_slot_runs",
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=[("crashed", "scheduled"), ("running", "watchdog_catchup")],
        ),
    )
    run = AsyncMock()
    monkeypatch.setattr("scripts.region_talk_scheduled_runner.run_region_talk_scheduled", run)

    result = await scheduling.maybe_dispatch_region_talk_watchdog(
        object(),
        object(),
        now_utc=datetime(2026, 7, 31, 20, 30, tzinfo=timezone.utc),
    )

    assert result is False
    run.assert_not_awaited()


def test_runtime_health_status_reports_email_worker_and_monitor(monkeypatch):
    monkeypatch.setenv("ENABLE_EMAIL_OUTBOX_WORKER", "1")
    monkeypatch.setenv("ENABLE_EMAIL_OUTBOX_MONITOR", "1")
    monkeypatch.setenv("DEV_MODE", "1")

    class Job:
        def __init__(self, job_id: str) -> None:
            self.id = job_id
            self.next_run_time = datetime(2026, 7, 12, 8, 45, tzinfo=timezone.utc)

    class Scheduler:
        running = True

        def __init__(self) -> None:
            self.jobs = {
                "email_outbox_worker": Job("email_outbox_worker"),
                "email_outbox_monitor": Job("email_outbox_monitor"),
            }

        def get_job(self, job_id: str):
            return self.jobs.get(job_id)

        def get_jobs(self):
            return list(self.jobs.values())

    monkeypatch.setattr(scheduling, "_scheduler", Scheduler())

    payload = scheduling.runtime_health_status()

    assert payload["email_outbox_worker"] == "ok"
    assert payload["email_outbox_monitor"] == "ok"
    assert "email_outbox_worker_next_run" in payload
    assert "email_outbox_monitor_next_run" in payload


@pytest.mark.asyncio
async def test_scheduled_video_tomorrow_fails_if_session_has_no_remote_handoff(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        await session.commit()

    class FakeScenario:
        last_tomorrow_skip_reason = ""

        def __init__(self, db, bot, chat_id: int, user_id: int) -> None:  # noqa: ANN001
            self.db = db

        async def run_tomorrow_pipeline(self, **_kwargs) -> int:
            async with self.db.raw_conn() as conn:
                cur = await conn.execute(
                    """
                    INSERT INTO videoannounce_session(
                        status, profile_key, selection_params, kaggle_kernel_ref
                    )
                    VALUES('SELECTED', 'default', '{"target_date":"2026-04-13"}', 'local:CrumpleVideo')
                    """
                )
                await conn.commit()
                return int(cur.lastrowid)

    monkeypatch.setattr(scenario_module, "VideoAnnounceScenario", FakeScenario)

    with pytest.raises(RuntimeError, match="confirmed Kaggle handoff"):
        await scheduling._run_scheduled_video_tomorrow(
            db, bot=object(), profile_key="default"
        )

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, details_json FROM ops_run WHERE kind='video_tomorrow' ORDER BY id DESC LIMIT 1"
        )
        row = await cur.fetchone()
    assert row is not None
    status, details_raw = row
    details = json.loads(details_raw)
    assert status == "failed"
    assert details["session_status"] == "SELECTED"
    assert "confirmed Kaggle handoff" in details["error"]


@pytest.mark.asyncio
async def test_scheduled_video_tomorrow_lane_busy_is_explicit_skip(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        await session.commit()

    class FakeScenario:
        last_tomorrow_skip_reason = "video_lanes_busy"

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def run_tomorrow_pipeline(self, **_kwargs) -> None:
            return None

    monkeypatch.setattr(scenario_module, "VideoAnnounceScenario", FakeScenario)

    await scheduling._run_scheduled_video_tomorrow(
        db, bot=object(), profile_key="default"
    )

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, details_json FROM ops_run WHERE kind='video_tomorrow' ORDER BY id DESC LIMIT 1"
        )
        row = await cur.fetchone()
    assert row is not None
    status, details_raw = row
    details = json.loads(details_raw)
    assert status == "skipped"
    assert details["skip_reason"] == "video_lanes_busy"


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
async def test_kenigsberg_story_startup_catchup_retries_single_failed_session(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedKenigsbergStoryDatetime)
    await _insert_kenigsberg_story_session(
        db,
        status="FAILED",
        created_at="2026-06-12 18:15:00",
    )

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_kenigsberg_story", fake_run)

    dispatched = await scheduling._maybe_catch_up_kenigsberg_story_on_startup(
        db, bot=object()
    )

    assert dispatched is True
    assert calls == [{"startup_catchup": True}]


@pytest.mark.asyncio
async def test_kenigsberg_story_startup_catchup_skips_after_two_failed_sessions(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedKenigsbergStoryDatetime)
    await _insert_kenigsberg_story_session(
        db,
        status="FAILED",
        created_at="2026-06-12 18:15:00",
    )
    await _insert_kenigsberg_story_session(
        db,
        status="FAILED",
        created_at="2026-06-12 18:30:00",
    )

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_kenigsberg_story", fake_run)

    dispatched = await scheduling._maybe_catch_up_kenigsberg_story_on_startup(
        db, bot=object()
    )

    assert dispatched is False
    assert calls == []


@pytest.mark.asyncio
async def test_kenigsberg_story_startup_catchup_skips_non_weekly_day(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(scheduling, "datetime", _FixedKenigsbergStorySaturdayDatetime)

    calls: list[dict] = []

    async def fake_run(_db, _bot, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduling, "_run_scheduled_kenigsberg_story", fake_run)

    dispatched = await scheduling._maybe_catch_up_kenigsberg_story_on_startup(
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
async def test_popular_review_watchdog_retries_failed_remote_handoff(
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

    assert dispatched is True
    assert calls == [{"startup_catchup": False}]


@pytest.mark.asyncio
async def test_popular_review_watchdog_skips_failed_session_with_video_output(
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
        kaggle_dataset="zigomaro/cherryflash-session-682",
        kaggle_kernel_ref="zigomaro/cherryflash",
        video_url="cherryflash_full_final.mp4",
        error="kernel output download failed",
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
async def test_popular_review_watchdog_skips_failed_session_with_terminal_ledger(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_popular_review_env(monkeypatch)
    session_id = await _insert_popular_review_session(
        db,
        status="FAILED",
        target_date="2026-04-12",
        created_at="2026-04-12 07:44:00",
        kaggle_dataset="zigomaro/cherryflash-session-682",
        kaggle_kernel_ref="zigomaro/cherryflash",
        error="post-render bot delivery failed",
    )
    await _insert_terminal_kaggle_ledger(db, session_id=session_id)

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
async def test_popular_review_watchdog_skips_failed_session_with_live_heartbeat(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_popular_review_env(monkeypatch)
    session_id = await _insert_popular_review_session(
        db,
        status="FAILED",
        target_date="2026-04-12",
        created_at="2026-04-12 07:44:00",
        kaggle_dataset="zigomaro/cherryflash-session-714",
        kaggle_kernel_ref="zigomaro/cherryflash",
        error="missing video output",
    )
    await _insert_live_kaggle_ledger(
        db,
        session_id=session_id,
        heartbeat_at="2026-04-12T08:58:00+00:00",
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
async def test_popular_review_watchdog_skips_publish_blocked_session(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_popular_review_env(monkeypatch)
    await _insert_popular_review_session(
        db,
        status="PUBLISH_BLOCKED",
        target_date="2026-04-12",
        created_at="2026-04-12 07:44:00",
        kaggle_dataset="zigomaro/cherryflash-session-682",
        kaggle_kernel_ref="zigomaro/cherryflash",
        video_url="cherryflash_full_final.mp4",
        error="story publish failed: @kenigevents BOOSTS_REQUIRED",
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
async def test_popular_review_watchdog_skips_rendering_remote_handoff(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_popular_review_env(monkeypatch)
    await _insert_popular_review_session(
        db,
        status="RENDERING",
        target_date="2026-04-12",
        created_at="2026-04-12 07:44:00",
        kaggle_dataset="zigomaro/cherryflash-session-181",
        kaggle_kernel_ref="zigomaro/cherryflash",
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
async def test_partner_track_watchdog_retries_failed_remote_handoff(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(
        scheduling,
        "datetime",
        type(
            "_FixedPartnerTrackDatetime",
            (datetime,),
            {
                "now": classmethod(
                    lambda cls, tz=None: (
                        datetime(2026, 4, 12, 11, 0, tzinfo=timezone.utc).astimezone(tz)
                        if tz is not None
                        else datetime(2026, 4, 12, 11, 0)
                    )
                )
            },
        ),
    )
    await _insert_video_session(
        db,
        status="FAILED",
        profile_key="popular_review_konb",
        target_date="2026-04-12",
        created_at="2026-04-12 10:47:22",
        kaggle_dataset="zigomaro/cherryflash-session-635",
        kaggle_kernel_ref="zigomaro/cherryflash",
        error="{'status': 'ERROR'}",
    )

    calls: list[tuple[str, dict]] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append((partner_track_id, kwargs))

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_konb_library_001"
    )

    assert dispatched is True
    assert calls == [
        ("partner_konb_library_001", {"startup_catchup": False})
    ]


@pytest.mark.asyncio
async def test_partner_track_watchdog_stops_after_two_failed_sessions(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(
        scheduling,
        "datetime",
        type(
            "_FixedPartnerTrackDatetime",
            (datetime,),
            {
                "now": classmethod(
                    lambda cls, tz=None: (
                        datetime(2026, 4, 12, 11, 0, tzinfo=timezone.utc).astimezone(tz)
                        if tz is not None
                        else datetime(2026, 4, 12, 11, 0)
                    )
                )
            },
        ),
    )
    for suffix in (635, 636):
        await _insert_video_session(
            db,
            status="FAILED",
            profile_key="popular_review_konb",
            target_date="2026-04-12",
            created_at=f"2026-04-12 10:{suffix - 600:02d}:22",
            kaggle_dataset=f"zigomaro/cherryflash-session-{suffix}",
            kaggle_kernel_ref="zigomaro/cherryflash",
            error="missing video output",
        )

    calls: list[tuple[str, dict]] = []

    async def fake_run(_db, _bot, partner_track_id, **kwargs):
        calls.append((partner_track_id, kwargs))

    monkeypatch.setattr(scheduling, "_run_scheduled_partner_track", fake_run)

    dispatched = await scheduling.maybe_dispatch_partner_track_watchdog(
        db, bot=object(), partner_track_id="partner_konb_library_001"
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
    scheduling._critical_catchup_deferred_until.clear()

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
async def test_critical_scheduler_watchdog_dispatches_tg_monitoring_after_crash(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_TG_MONITORING", "1")
    monkeypatch.setenv("TG_MONITORING_TZ", "Europe/Kaliningrad")
    monkeypatch.setenv("TG_MONITORING_TIME_LOCAL", "23:40")
    monkeypatch.setenv("TG_MONITORING_MISFIRE_GRACE_SECONDS", "60")
    monkeypatch.setenv("CRITICAL_SCHED_WATCHDOG_GRACE_SECONDS", "60")
    monkeypatch.setenv("DEV_MODE", "1")
    monkeypatch.setattr(scheduling, "datetime", _FixedCriticalAfterMidnightDatetime)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

    crashed_run_id = await start_ops_run(
        db,
        kind="tg_monitoring",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 6, 12, 21, 40, tzinfo=timezone.utc),
        details={"run_id": "deploy-killed"},
    )
    await finish_ops_run(
        db,
        run_id=crashed_run_id,
        status="crashed",
        finished_at=datetime(2026, 6, 12, 21, 41, tzinfo=timezone.utc),
        details={"run_id": "deploy-killed"},
    )

    calls: list[str] = []

    async def fake_telegram_monitor_scheduler(_db, _bot, *, run_id=None):
        calls.append(run_id or "")
        recovery_id = await start_ops_run(
            db,
            kind="tg_monitoring",
            trigger="scheduled",
            operator_id=0,
            started_at=_FixedCriticalAfterMidnightDatetime.fixed_now,
            details={"run_id": run_id},
        )
        await finish_ops_run(
            db,
            run_id=recovery_id,
            status="success",
            finished_at=_FixedCriticalAfterMidnightDatetime.fixed_now,
            details={"run_id": run_id},
        )

    @asynccontextmanager
    async def fake_heavy_operation(**kwargs):
        calls.append(f"{kwargs['kind']}:{kwargs['mode']}")
        yield

    monkeypatch.setitem(
        sys.modules,
        "source_parsing.telegram.service",
        SimpleNamespace(telegram_monitor_scheduler=fake_telegram_monitor_scheduler),
    )
    monkeypatch.setattr(scheduling, "heavy_operation", fake_heavy_operation)

    dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert dispatched == 1
    assert calls[0] == "tg_monitoring:wait"
    assert calls[1].startswith("catchup-")


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_defers_tg_monitoring_when_recovery_job_exists(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_TG_MONITORING", "1")
    monkeypatch.setenv("TG_MONITORING_TZ", "Europe/Kaliningrad")
    monkeypatch.setenv("TG_MONITORING_TIME_LOCAL", "23:40")
    monkeypatch.setenv("TG_MONITORING_MISFIRE_GRACE_SECONDS", "60")
    monkeypatch.setenv("TG_MONITORING_REMOTE_BUSY_RETRY_SECONDS", "300")
    monkeypatch.setenv("CRITICAL_SCHED_WATCHDOG_GRACE_SECONDS", "60")
    monkeypatch.setenv("DEV_MODE", "1")
    monkeypatch.setattr(scheduling, "datetime", _FixedCriticalAfterMidnightDatetime)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

    crashed_run_id = await start_ops_run(
        db,
        kind="tg_monitoring",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 6, 12, 21, 40, tzinfo=timezone.utc),
        details={"run_id": "deploy-killed"},
    )
    await finish_ops_run(
        db,
        run_id=crashed_run_id,
        status="error",
        finished_at=datetime(2026, 6, 12, 21, 41, tzinfo=timezone.utc),
        details={"run_id": "deploy-killed", "errors": ["cancelled"]},
    )

    calls: list[str] = []

    async def fake_telegram_monitor_scheduler(_db, _bot, *, run_id=None):
        calls.append(run_id or "")

    monkeypatch.setitem(
        sys.modules,
        "source_parsing.telegram.service",
        SimpleNamespace(telegram_monitor_scheduler=fake_telegram_monitor_scheduler),
    )
    monkeypatch.setattr(scheduling, "_tg_monitoring_recovery_job_exists", AsyncMock(return_value=True))

    dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert dispatched == 0
    assert calls == []
    assert scheduling._critical_catchup_deferred_until


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_dispatches_vk_auto_import_after_slot_crash(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_VK_AUTO_IMPORT", "1")
    monkeypatch.setenv("VK_AUTO_IMPORT_TZ", "Europe/Kaliningrad")
    monkeypatch.setenv("VK_AUTO_IMPORT_TIMES_LOCAL", "10:15,23:40")
    monkeypatch.setenv("VK_AUTO_IMPORT_MISFIRE_GRACE_SECONDS", "60")
    monkeypatch.setenv("CRITICAL_SCHED_WATCHDOG_GRACE_SECONDS", "60")
    monkeypatch.setenv("DEV_MODE", "1")
    monkeypatch.setattr(scheduling, "datetime", _FixedCriticalAfterMidnightDatetime)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

    morning_run_id = await start_ops_run(
        db,
        kind="vk_auto_import",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 6, 12, 8, 15, tzinfo=timezone.utc),
        details={"run_id": "morning-ok"},
    )
    await finish_ops_run(
        db,
        run_id=morning_run_id,
        status="success",
        finished_at=datetime(2026, 6, 12, 8, 45, tzinfo=timezone.utc),
        details={"run_id": "morning-ok"},
    )
    crashed_run_id = await start_ops_run(
        db,
        kind="vk_auto_import",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 6, 12, 21, 40, tzinfo=timezone.utc),
        details={"run_id": "evening-crashed"},
    )
    await finish_ops_run(
        db,
        run_id=crashed_run_id,
        status="crashed",
        finished_at=datetime(2026, 6, 12, 21, 41, tzinfo=timezone.utc),
        details={"run_id": "evening-crashed"},
    )

    calls: list[str] = []

    async def fake_vk_auto_import_scheduler(_db, _bot, *, run_id=None):
        calls.append(run_id or "")
        recovery_id = await start_ops_run(
            db,
            kind="vk_auto_import",
            trigger="scheduled",
            operator_id=0,
            started_at=_FixedCriticalAfterMidnightDatetime.fixed_now,
            details={"run_id": run_id},
        )
        await finish_ops_run(
            db,
            run_id=recovery_id,
            status="success",
            finished_at=_FixedCriticalAfterMidnightDatetime.fixed_now,
            details={"run_id": run_id},
        )

    monkeypatch.setitem(
        sys.modules,
        "vk_auto_queue",
        SimpleNamespace(vk_auto_import_scheduler=fake_vk_auto_import_scheduler),
    )

    dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert dispatched == 1
    assert calls[0].startswith("catchup-")


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_skips_guide_when_full_run_exists(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

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
async def test_critical_scheduler_watchdog_skips_guide_when_recovery_import_exists(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

    run_id = await start_ops_run(
        db,
        kind="guide_monitoring",
        trigger="recovery_import",
        operator_id=0,
        started_at=datetime(2026, 4, 13, 21, 23, tzinfo=timezone.utc),
        details={"mode": "full", "run_id": "f18774f300c7"},
    )
    await finish_ops_run(
        db,
        run_id=run_id,
        status="partial",
        finished_at=datetime(2026, 4, 13, 21, 24, tzinfo=timezone.utc),
        details={"mode": "full", "run_id": "f18774f300c7"},
    )

    calls: list[dict[str, str]] = []

    async def fake_run(_db, _bot, *, mode: str) -> None:
        calls.append({"mode": mode})

    monkeypatch.setattr(scheduling, "_run_scheduled_guide_excursions", fake_run)

    dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert dispatched == 0
    assert calls == []


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_defers_guide_after_remote_busy_skip(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    monkeypatch.setenv("GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS", "300")
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

    calls: list[dict[str, str]] = []

    async def fake_run(_db, _bot, *, mode: str) -> None:
        calls.append({"mode": mode})
        run_id = await start_ops_run(
            db,
            kind="guide_monitoring",
            trigger="scheduled",
            operator_id=0,
            started_at=_FixedCriticalSchedulerDatetime.fixed_now,
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
    _FixedCriticalSchedulerDatetime.fixed_now = datetime(2026, 4, 13, 21, 11, tzinfo=timezone.utc)
    scheduling._critical_catchup_deferred_until.clear()
    third_dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert first_dispatched == 1
    assert second_dispatched == 0
    assert third_dispatched == 1
    assert calls == [
        {"kind": "guide_monitoring", "guard": "wait"},
        {"mode": "full"},
        {"kind": "guide_monitoring", "guard": "wait"},
        {"mode": "full"},
    ]


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_defers_guide_remote_busy_after_local_midnight(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    monkeypatch.setenv("GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS", "300")
    monkeypatch.setattr(scheduling, "datetime", _FixedCriticalAfterMidnightDatetime)
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

    calls: list[dict[str, str]] = []

    async def fake_run(_db, _bot, *, mode: str) -> None:
        calls.append({"mode": mode})
        run_id = await start_ops_run(
            db,
            kind="guide_monitoring",
            trigger="scheduled",
            operator_id=0,
            started_at=_FixedCriticalAfterMidnightDatetime.fixed_now,
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
            finished_at=_FixedCriticalAfterMidnightDatetime.fixed_now,
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
    assert second_dispatched == 0
    assert calls == [
        {"kind": "guide_monitoring", "guard": "wait"},
        {"mode": "full"},
    ]


@pytest.mark.asyncio
async def test_critical_scheduler_watchdog_defers_after_crashed_guide_full_run(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    _configure_guide_critical_env(monkeypatch)
    monkeypatch.setenv("GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS", "300")
    scheduling._critical_catchup_inflight.clear()
    scheduling._critical_catchup_completed.clear()
    scheduling._critical_catchup_deferred_until.clear()

    run_id = await start_ops_run(
        db,
        kind="guide_monitoring",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 4, 13, 21, 4, tzinfo=timezone.utc),
        details={"mode": "full", "run_id": "lost-kaggle-owner"},
    )
    await finish_ops_run(
        db,
        run_id=run_id,
        status="crashed",
        finished_at=datetime(2026, 4, 13, 21, 4, 30, tzinfo=timezone.utc),
        details={"mode": "full", "run_id": "lost-kaggle-owner"},
    )
    skipped_run_id = await start_ops_run(
        db,
        kind="guide_monitoring",
        trigger="scheduled",
        operator_id=0,
        started_at=datetime(2026, 4, 13, 21, 4, 40, tzinfo=timezone.utc),
        details={"mode": "full", "skip_reason": "heavy_busy"},
    )
    await finish_ops_run(
        db,
        run_id=skipped_run_id,
        status="skipped",
        finished_at=datetime(2026, 4, 13, 21, 4, 45, tzinfo=timezone.utc),
        details={"mode": "full", "skip_reason": "heavy_busy"},
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

    deferred = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )
    _FixedCriticalSchedulerDatetime.fixed_now = datetime(2026, 4, 13, 21, 10, tzinfo=timezone.utc)
    scheduling._critical_catchup_deferred_until.clear()
    dispatched = await scheduling.maybe_dispatch_critical_scheduler_watchdog(
        db, bot=object()
    )

    assert deferred == 0
    assert dispatched == 1
    assert calls == [
        {"kind": "guide_monitoring", "guard": "wait"},
        {"mode": "full"},
    ]
