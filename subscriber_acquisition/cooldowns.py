from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .config import AcqConfig, load_config


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def next_surface_scan_after(status: str, *, config: AcqConfig | None = None, now: datetime | None = None) -> datetime:
    cfg = config or load_config()
    now = now or utc_now()
    if status == "rejected":
        return now + timedelta(days=cfg.rejected_surface_cooldown_days)
    if status == "paused":
        return now + timedelta(days=cfg.paused_surface_days)
    if status == "approved":
        return now + timedelta(hours=cfg.approved_surface_reshow_cooldown_h)
    return now + timedelta(hours=cfg.surface_rescan_cooldown_h)


def opportunity_expires_at(context_created_at: datetime | None = None, *, config: AcqConfig | None = None, now: datetime | None = None) -> datetime:
    cfg = config or load_config()
    base = context_created_at or now or utc_now()
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    return base + timedelta(hours=cfg.opportunity_expires_h)
