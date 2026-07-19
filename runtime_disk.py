from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any


def _env_nonnegative_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return max(0, int(raw) if raw else int(default))
    except (TypeError, ValueError):
        return max(0, int(default))


def writable_disk_health(
    path: str | Path,
    *,
    warn_free_mb: int,
    critical_free_mb: int,
    tempfile_probe: bool = False,
) -> dict[str, Any]:
    """Return capacity and optional real write/fsync/remove readiness.

    Only exception class names are exposed.  Filesystem paths can be included
    in health output, but neither file contents nor environment values are.
    """

    target = Path(path)
    warn_mb = max(0, int(warn_free_mb))
    critical_mb = max(0, int(critical_free_mb))
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
    if tempfile_probe:
        payload["tempfile_status"] = "unknown"
        try:
            target.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w+b",
                prefix=".runtime-write-probe-",
                dir=target,
                delete=True,
            ) as handle:
                handle.write(b"ok")
                handle.flush()
                os.fsync(handle.fileno())
            payload["tempfile_status"] = "ok"
        except Exception as exc:
            payload["tempfile_status"] = "error"
            payload["tempfile_error"] = type(exc).__name__
            payload["status"] = "critical"
    return payload


def runtime_disk_health(path: str | Path | None = None) -> dict[str, Any]:
    """Persistent runtime volume capacity (normally ``/data``)."""

    return writable_disk_health(
        path or (os.getenv("RUNTIME_DISK_PATH") or "/data"),
        warn_free_mb=_env_nonnegative_int("RUNTIME_DISK_WARN_FREE_MB", 350),
        critical_free_mb=_env_nonnegative_int("RUNTIME_DISK_CRITICAL_FREE_MB", 256),
    )


def runtime_scratch_health(path: str | Path | None = None) -> dict[str, Any]:
    """Root/scratch filesystem capacity plus an actual tempfile write probe."""

    return writable_disk_health(
        path or (os.getenv("RUNTIME_SCRATCH_PATH") or "/tmp"),
        warn_free_mb=_env_nonnegative_int("RUNTIME_SCRATCH_WARN_FREE_MB", 1024),
        critical_free_mb=_env_nonnegative_int("RUNTIME_SCRATCH_CRITICAL_FREE_MB", 512),
        tempfile_probe=True,
    )
