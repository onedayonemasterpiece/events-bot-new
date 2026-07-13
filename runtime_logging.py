from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path


_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
_MIB = 1024 * 1024


def _env_enabled(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _runtime_log_candidates(base_path: Path) -> list[Path]:
    prefix = f"{base_path.name}."
    try:
        return [
            candidate
            for candidate in base_path.parent.iterdir()
            if candidate.is_file()
            and candidate != base_path
            and candidate.name.startswith(prefix)
        ]
    except Exception:
        return []


def _cleanup_old_runtime_logs(base_path: Path, *, retention_hours: int) -> None:
    cutoff_ts = time.time() - max(1, int(retention_hours)) * 3600
    for candidate in _runtime_log_candidates(base_path):
        try:
            if candidate.stat().st_mtime < cutoff_ts:
                candidate.unlink(missing_ok=True)
        except Exception:
            # Logging setup must stay best-effort; cleanup failures should not block startup.
            continue


def _enforce_runtime_log_budget(base_path: Path, *, max_total_bytes: int) -> None:
    """Prune oldest rotated logs until the explicit directory budget is met.

    The active file is never deleted here. ``RotatingFileHandler`` bounds it
    separately with ``maxBytes``. Unknown files in the same directory are not
    considered part of this logger's budget and are never touched.
    """

    budget = max(1, int(max_total_bytes))
    files: list[tuple[float, int, Path]] = []
    active_size = 0
    try:
        if base_path.is_file():
            active_size = int(base_path.stat().st_size)
    except Exception:
        active_size = 0
    for candidate in _runtime_log_candidates(base_path):
        try:
            stat = candidate.stat()
            files.append((float(stat.st_mtime), int(stat.st_size), candidate))
        except Exception:
            continue
    total = active_size + sum(size for _, size, _ in files)
    for _mtime, size, candidate in sorted(files, key=lambda item: item[0]):
        if total <= budget:
            break
        try:
            candidate.unlink(missing_ok=True)
            total -= size
        except Exception:
            continue


class BudgetedRuntimeFileHandler(RotatingFileHandler):
    """Size-bounded file mirror with retention and a volume free-space floor."""

    def __init__(
        self,
        filename: str,
        *,
        max_bytes: int,
        max_total_bytes: int,
        min_free_bytes: int,
        retention_hours: int,
        encoding: str = "utf-8",
        delay: bool = True,
        check_interval_seconds: float = 60.0,
    ) -> None:
        max_bytes = max(1, int(max_bytes))
        max_total_bytes = max(max_bytes, int(max_total_bytes))
        # active + N rotations cannot exceed the configured budget by design.
        backup_count = max(1, max_total_bytes // max_bytes - 1)
        super().__init__(
            filename=filename,
            mode="a",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding=encoding,
            delay=delay,
        )
        self.max_total_bytes = max_total_bytes
        self.min_free_bytes = max(0, int(min_free_bytes))
        self.retention_hours = max(1, int(retention_hours))
        self.check_interval_seconds = max(1.0, float(check_interval_seconds))
        self._next_budget_check_monotonic = 0.0
        self._paused_for_space = False

    @property
    def runtime_base_path(self) -> Path:
        return Path(self.baseFilename)

    def _has_free_space(self) -> bool:
        if self.min_free_bytes <= 0:
            return True
        try:
            free = int(shutil.disk_usage(self.runtime_base_path.parent).free)
        except Exception:
            # Failure to measure should not silently discard all observability.
            return True
        return free >= self.min_free_bytes

    def _maintenance(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and now < self._next_budget_check_monotonic:
            return not self._paused_for_space
        self._next_budget_check_monotonic = now + self.check_interval_seconds
        _cleanup_old_runtime_logs(
            self.runtime_base_path,
            retention_hours=self.retention_hours,
        )
        _enforce_runtime_log_budget(
            self.runtime_base_path,
            max_total_bytes=self.max_total_bytes,
        )
        has_space = self._has_free_space()
        if not has_space and not self._paused_for_space:
            try:
                sys.stderr.write(
                    "runtime_logging: paused file mirror because volume free-space floor was reached\n"
                )
            except Exception:
                pass
        elif has_space and self._paused_for_space:
            try:
                sys.stderr.write("runtime_logging: resumed file mirror after free-space recovery\n")
            except Exception:
                pass
        self._paused_for_space = not has_space
        return has_space

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if not self._maintenance():
                return
            super().emit(record)
        except Exception:
            self.handleError(record)

    def doRollover(self) -> None:
        super().doRollover()
        self._maintenance(force=True)


def install_runtime_file_logging(logger: logging.Logger | None = None) -> logging.Handler | None:
    if not _env_enabled("ENABLE_RUNTIME_FILE_LOGGING", default=False):
        return None

    target_logger = logger or logging.getLogger()
    log_dir = Path((os.getenv("RUNTIME_LOG_DIR") or "/data/runtime_logs").strip() or "/data/runtime_logs")
    log_name = (os.getenv("RUNTIME_LOG_BASENAME") or "events-bot.log").strip() or "events-bot.log"
    retention_hours = max(1, _env_int("RUNTIME_LOG_RETENTION_HOURS", 48))
    max_file_mb = max(1, _env_int("RUNTIME_LOG_MAX_FILE_MB", 8))
    max_total_mb = max(max_file_mb * 2, _env_int("RUNTIME_LOG_MAX_TOTAL_MB", 64))
    min_free_mb = max(0, _env_int("RUNTIME_LOG_MIN_FREE_MB", 256))
    log_level_name = (os.getenv("RUNTIME_LOG_LEVEL") or "INFO").strip().upper()
    log_level = getattr(logging, log_level_name, target_logger.level or logging.INFO)

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "runtime_logging: failed to create log dir %s: %s",
            log_dir,
            exc,
        )
        return None

    log_path = log_dir / log_name
    resolved_log_path = str(log_path.resolve())
    for handler in target_logger.handlers:
        if getattr(handler, "_evbot_runtime_log_path", None) == resolved_log_path:
            return handler

    try:
        _cleanup_old_runtime_logs(log_path, retention_hours=retention_hours)
        _enforce_runtime_log_budget(log_path, max_total_bytes=max_total_mb * _MIB)
        handler = BudgetedRuntimeFileHandler(
            filename=resolved_log_path,
            max_bytes=max_file_mb * _MIB,
            max_total_bytes=max_total_mb * _MIB,
            min_free_bytes=min_free_mb * _MIB,
            retention_hours=retention_hours,
            encoding="utf-8",
            delay=True,
        )
        handler.setLevel(log_level)
        handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
        handler._evbot_runtime_log_path = resolved_log_path  # type: ignore[attr-defined]
        target_logger.addHandler(handler)
        logging.getLogger(__name__).info(
            "runtime_logging: enabled path=%s retention_hours=%d max_file_mb=%d max_total_mb=%d min_free_mb=%d backup_count=%d level=%s",
            resolved_log_path,
            retention_hours,
            max_file_mb,
            max_total_mb,
            min_free_mb,
            handler.backupCount,
            logging.getLevelName(log_level),
        )
        return handler
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "runtime_logging: failed to init file handler path=%s error=%s",
            resolved_log_path,
            exc,
        )
        return None
