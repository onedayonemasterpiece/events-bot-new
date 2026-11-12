"""An earlier delivered slot must not strand a later durable handoff."""
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from types import SimpleNamespace
import sys

import pytest

from db import Database
from ops_run import finish_ops_run, start_ops_run
import scheduling


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_pending", [True, False])
async def test_existing_handoff_precedes_slot_delivery_and_new_launch_retry(
    tmp_path, monkeypatch, registry_pending
):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 12, 23, 25, tzinfo=timezone.utc)

    monkeypatch.setattr(scheduling, "datetime", Clock)
    monkeypatch.setenv("ENABLE_TG_MONITORING", "1")
    monkeypatch.setenv("TG_MONITORING_TIME_LOCAL", "23:40")
    monkeypatch.setenv("TG_MONITORING_TZ", "Europe/Kaliningrad")
    monkeypatch.setattr(scheduling, "_critical_catchup_inflight", set())
    monkeypatch.setattr(scheduling, "_critical_catchup_completed", set())
    monkeypatch.setattr(scheduling, "_critical_catchup_deferred_until", {})
    resume = AsyncMock(return_value=registry_pending)
    monkeypatch.setattr(scheduling, "_tg_monitoring_recovery_job_exists", resume)
    launch = AsyncMock()
    monkeypatch.setitem(sys.modules, "source_parsing.telegram.service",
                        SimpleNamespace(telegram_monitor_scheduler=launch))
    db = Database(str(tmp_path / "delivery-priority.sqlite"))
    await db.init()
    try:
        for trigger, status, started in [
            ("recovery_import", "partial", datetime(2026, 11, 12, 22, 12, tzinfo=timezone.utc)),
            ("scheduled", "error", datetime(2026, 11, 12, 23, 0, tzinfo=timezone.utc)),
        ]:
            rid = await start_ops_run(db, kind="tg_monitoring", trigger=trigger, started_at=started)
            await finish_ops_run(db, run_id=rid, status=status, finished_at=started,
                                 details={"errors": ["cancelled"]} if status == "error" else {})
        assert await scheduling._maybe_dispatch_tg_monitoring_watchdog(db, None) == 0
        if registry_pending:
            resume.assert_awaited_once_with(db=db, bot=None)
        launch.assert_not_awaited()
        # No new kernel: an outstanding registry handoff defers; an empty registry
        # still respects the earlier partial delivery of this scheduled slot.
        assert bool(scheduling._critical_catchup_deferred_until) is registry_pending
    finally:
        await db.close()
