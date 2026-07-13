from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from types import SimpleNamespace

import runtime_logging


def _logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def _close_logger(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)


def test_runtime_file_logging_disabled_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("ENABLE_RUNTIME_FILE_LOGGING", raising=False)
    logger = _logger("test.runtime.disabled")
    try:
        assert runtime_logging.install_runtime_file_logging(logger) is None
        assert logger.handlers == []
    finally:
        _close_logger(logger)


def test_runtime_file_logging_writes_with_explicit_budget(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_RUNTIME_FILE_LOGGING", "1")
    monkeypatch.setenv("RUNTIME_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("RUNTIME_LOG_MAX_FILE_MB", "1")
    monkeypatch.setenv("RUNTIME_LOG_MAX_TOTAL_MB", "3")
    monkeypatch.setenv("RUNTIME_LOG_MIN_FREE_MB", "0")
    monkeypatch.setenv("RUNTIME_LOG_RETENTION_HOURS", "48")
    logger = _logger("test.runtime.enabled")
    try:
        handler = runtime_logging.install_runtime_file_logging(logger)
        assert isinstance(handler, runtime_logging.BudgetedRuntimeFileHandler)
        logger.info("vk_auto_import ops_run_id=42")
        handler.flush()
        text = (tmp_path / "events-bot.log").read_text(encoding="utf-8")
        assert "vk_auto_import ops_run_id=42" in text
        assert handler.maxBytes == 1024 * 1024
        assert handler.max_total_bytes == 3 * 1024 * 1024
        assert handler.backupCount == 2
    finally:
        _close_logger(logger)


def test_budget_prunes_only_oldest_matching_rotations(tmp_path: Path) -> None:
    base = tmp_path / "events-bot.log"
    base.write_bytes(b"a" * 30)
    old = tmp_path / "events-bot.log.2026-old"
    middle = tmp_path / "events-bot.log.2"
    newest = tmp_path / "events-bot.log.1"
    unrelated = tmp_path / "important.sqlite"
    old.write_bytes(b"b" * 30)
    middle.write_bytes(b"c" * 30)
    newest.write_bytes(b"d" * 30)
    unrelated.write_bytes(b"keep")
    now = time.time()
    os.utime(old, (now - 300, now - 300))
    os.utime(middle, (now - 200, now - 200))
    os.utime(newest, (now - 100, now - 100))

    runtime_logging._enforce_runtime_log_budget(base, max_total_bytes=70)

    assert not old.exists()
    assert not middle.exists()
    assert newest.exists()
    assert base.exists()
    assert unrelated.read_bytes() == b"keep"


def test_handler_pauses_below_free_space_floor_and_resumes(
    tmp_path: Path, monkeypatch
) -> None:
    free = {"value": 0}

    def fake_disk_usage(_path):
        return SimpleNamespace(total=10_000, used=10_000 - free["value"], free=free["value"])

    monkeypatch.setattr(runtime_logging.shutil, "disk_usage", fake_disk_usage)
    handler = runtime_logging.BudgetedRuntimeFileHandler(
        str(tmp_path / "events-bot.log"),
        max_bytes=1024,
        max_total_bytes=4096,
        min_free_bytes=500,
        retention_hours=48,
        check_interval_seconds=1,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger = _logger("test.runtime.floor")
    logger.addHandler(handler)
    try:
        logger.info("must be dropped")
        assert not (tmp_path / "events-bot.log").exists()
        free["value"] = 1_000
        handler._next_budget_check_monotonic = 0
        logger.info("must be retained")
        handler.flush()
        assert (tmp_path / "events-bot.log").read_text(encoding="utf-8").strip() == "must be retained"
    finally:
        _close_logger(logger)


def test_old_rotations_are_removed_but_active_log_is_preserved(tmp_path: Path) -> None:
    base = tmp_path / "events-bot.log"
    base.write_text("active", encoding="utf-8")
    rotated = tmp_path / "events-bot.log.1"
    rotated.write_text("old", encoding="utf-8")
    old = time.time() - 3 * 3600
    os.utime(base, (old, old))
    os.utime(rotated, (old, old))

    runtime_logging._cleanup_old_runtime_logs(base, retention_hours=1)

    assert base.exists()
    assert not rotated.exists()
