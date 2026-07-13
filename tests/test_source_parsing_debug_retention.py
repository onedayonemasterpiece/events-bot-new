from __future__ import annotations

import os
import time
from pathlib import Path

from source_parsing.handlers import prune_source_parsing_debug_logs


def _write(path: Path, size: int, *, age_days: int = 0) -> None:
    path.write_bytes(b"x" * size)
    ts = time.time() - age_days * 86400
    os.utime(path, (ts, ts))


def test_prune_source_parsing_logs_applies_age_and_budget_without_unknown_files(tmp_path: Path) -> None:
    old = tmp_path / "source_parsing_old.log"
    first = tmp_path / "source_parsing_first.log"
    second = tmp_path / "source_parsing_second.log"
    keep = tmp_path / "source_parsing_keep.log"
    guard = tmp_path / "source_parsing_guard.json"
    unrelated = tmp_path / "operator.log"
    _write(old, 400, age_days=9)
    _write(first, 700)
    _write(second, 700)
    _write(keep, 700)
    guard.write_text("{}", encoding="utf-8")
    unrelated.write_text("keep", encoding="utf-8")
    now = time.time()
    os.utime(first, (now - 300, now - 300))
    os.utime(second, (now - 200, now - 200))
    os.utime(keep, (now - 100, now - 100))

    result = prune_source_parsing_debug_logs(
        tmp_path,
        retention_days=7,
        max_total_mb=1,
        exclude=keep,
    )

    assert result["deleted_files"] == 1
    assert not old.exists()
    assert first.exists() and second.exists() and keep.exists()
    assert guard.read_text(encoding="utf-8") == "{}"
    assert unrelated.read_text(encoding="utf-8") == "keep"


def test_prune_source_parsing_logs_removes_oldest_until_megabyte_budget(tmp_path: Path) -> None:
    files = [tmp_path / f"source_parsing_{idx}.log" for idx in range(3)]
    now = time.time()
    for idx, path in enumerate(files):
        _write(path, 600_000)
        os.utime(path, (now - (300 - idx * 100), now - (300 - idx * 100)))

    result = prune_source_parsing_debug_logs(
        tmp_path,
        retention_days=7,
        max_total_mb=1,
    )

    assert result["deleted_files"] == 2
    assert not files[0].exists()
    assert not files[1].exists()
    assert files[2].exists()
    assert result["remaining_bytes"] == 600_000
