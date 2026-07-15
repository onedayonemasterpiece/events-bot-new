"""Env-gated registration hook for the preview service-share daily renderer."""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent


def enabled() -> bool:
    return (os.getenv("ENABLE_SERVICE_SHARE_CARD_DAILY") or "0").strip().lower() in {"1", "true", "yes", "on"}


def _local_time() -> tuple[int, int]:
    raw = (os.getenv("SERVICE_SHARE_CARD_TIME_LOCAL") or "08:45").strip()
    hour, minute = raw.split(":", 1)
    return int(hour), int(minute)


async def run_service_share_daily_job() -> None:
    process = await asyncio.create_subprocess_exec(sys.executable, str(ROOT / "scripts/run_service_share_card_daily.py"))
    code = await process.wait()
    if code:
        raise RuntimeError(f"service-share daily renderer exited {code}")


def register_service_share_daily_job(scheduler):
    """Register nothing unless explicitly enabled; preview defaults OFF."""
    if not enabled():
        return None
    hour, minute = _local_time()
    timezone = ZoneInfo(os.getenv("SERVICE_SHARE_CARD_TZ", "Europe/Kaliningrad"))
    return scheduler.add_job(
        run_service_share_daily_job, trigger="cron", id="service_share_card_daily",
        hour=hour, minute=minute, timezone=timezone, max_instances=1,
        coalesce=True, misfire_grace_time=3600, replace_existing=True,
    )
