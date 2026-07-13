from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any


def _env_nonnegative_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return max(0, int(raw) if raw else int(default))
    except (TypeError, ValueError):
        return max(0, int(default))


def runtime_disk_health(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path or (os.getenv("RUNTIME_DISK_PATH") or "/data"))
    warn_mb = _env_nonnegative_int("RUNTIME_DISK_WARN_FREE_MB", 350)
    critical_mb = _env_nonnegative_int("RUNTIME_DISK_CRITICAL_FREE_MB", 256)
    if warn_mb < critical_mb:
        warn_mb = critical_mb
    payload: dict[str, Any] = {
        "path": str(target),
        "status": "unknown",
        "free_mb": None,
        "total_mb": None,
        "used_percent": None,
        "warn_free_mb": warn_mb,
        "critical_free_mb": critical_mb,
    }
    try:
        usage = shutil.disk_usage(target)
    except Exception as exc:
        payload["error"] = type(exc).__name__
        return payload
    free_mb = int(usage.free // (1024 * 1024))
    total_mb = int(usage.total // (1024 * 1024))
    used_percent = round((usage.used / usage.total * 100.0), 2) if usage.total else 0.0
    status = "ok"
    if free_mb < critical_mb:
        status = "critical"
    elif free_mb < warn_mb:
        status = "warning"
    payload.update(
        {
            "status": status,
            "free_mb": free_mb,
            "total_mb": total_mb,
            "used_percent": used_percent,
        }
    )
    return payload
