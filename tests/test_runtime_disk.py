from __future__ import annotations

from types import SimpleNamespace

import runtime_disk


def test_runtime_disk_health_reports_ok_warning_and_critical(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_DISK_WARN_FREE_MB", "350")
    monkeypatch.setenv("RUNTIME_DISK_CRITICAL_FREE_MB", "256")
    free = {"mb": 400}

    def fake_usage(_path):
        total = 1024 * 1024 * 1024
        free_bytes = free["mb"] * 1024 * 1024
        return SimpleNamespace(total=total, used=total - free_bytes, free=free_bytes)

    monkeypatch.setattr(runtime_disk.shutil, "disk_usage", fake_usage)
    assert runtime_disk.runtime_disk_health()["status"] == "ok"
    free["mb"] = 300
    assert runtime_disk.runtime_disk_health()["status"] == "warning"
    free["mb"] = 200
    assert runtime_disk.runtime_disk_health()["status"] == "critical"


def test_runtime_disk_health_handles_bad_env_and_probe_failure(monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_DISK_WARN_FREE_MB", "bad")
    monkeypatch.setenv("RUNTIME_DISK_CRITICAL_FREE_MB", "500")

    def fail(_path):
        raise FileNotFoundError

    monkeypatch.setattr(runtime_disk.shutil, "disk_usage", fail)
    payload = runtime_disk.runtime_disk_health("/missing")
    assert payload["status"] == "unknown"
    assert payload["warn_free_mb"] == 500
    assert payload["critical_free_mb"] == 500
    assert payload["error"] == "FileNotFoundError"


def test_runtime_scratch_health_writes_fsyncs_and_removes_probe(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("RUNTIME_SCRATCH_WARN_FREE_MB", "1")
    monkeypatch.setenv("RUNTIME_SCRATCH_CRITICAL_FREE_MB", "0")

    payload = runtime_disk.runtime_scratch_health(tmp_path)

    assert payload["status"] == "ok"
    assert payload["tempfile_status"] == "ok"
    assert list(tmp_path.iterdir()) == []


def test_runtime_scratch_health_is_critical_when_tempfile_probe_fails(
    tmp_path, monkeypatch
) -> None:
    class ProbeFailure:
        def __enter__(self):
            raise OSError("disk full")

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        runtime_disk.tempfile,
        "NamedTemporaryFile",
        lambda **_kwargs: ProbeFailure(),
    )

    payload = runtime_disk.runtime_scratch_health(tmp_path)

    assert payload["status"] == "critical"
    assert payload["tempfile_status"] == "error"
    assert payload["tempfile_error"] == "OSError"
