import os
from pathlib import Path

import pytest

from source_parsing.telegram import service as tg_service


@pytest.mark.asyncio
async def test_recovery_imports_stale_same_pid_job_when_run_lock_is_free(monkeypatch, tmp_path: Path) -> None:
    imported: list[tuple[str, str]] = []
    removed: list[tuple[str, str]] = []

    class DummyKaggleClient:
        def get_kernel_status(self, kernel_ref: str) -> dict:
            assert kernel_ref == "zigomaro/telegram-monitor-bot"
            return {"status": "complete"}

    async def fake_list_jobs(job_type: str | None = None):
        assert job_type == "tg_monitoring"
        return [
            {
                "kernel_ref": "zigomaro/telegram-monitor-bot",
                "meta": {"run_id": "run-stale", "pid": os.getpid()},
            }
        ]

    async def fake_download_results(client, kernel_ref: str, run_id: str) -> Path:
        assert isinstance(client, DummyKaggleClient)
        assert kernel_ref == "zigomaro/telegram-monitor-bot"
        assert run_id == "run-stale"
        path = tmp_path / "telegram_results.json"
        path.write_text('{"messages":[]}', encoding="utf-8")
        return path

    async def fake_import_from_results(db, *, results_path, run_id: str, **kwargs):
        imported.append((run_id, str(results_path)))

    async def fake_remove_job(job_type: str, kernel_ref: str) -> None:
        removed.append((job_type, kernel_ref))

    monkeypatch.setattr(tg_service, "KaggleClient", DummyKaggleClient)
    monkeypatch.setattr(tg_service, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(tg_service, "_download_results", fake_download_results)
    monkeypatch.setattr(tg_service, "run_telegram_import_from_results", fake_import_from_results)
    monkeypatch.setattr(tg_service, "remove_job", fake_remove_job)

    assert not tg_service._RUN_LOCK.locked()

    recovered = await tg_service.resume_telegram_monitor_jobs(object(), bot=None, chat_id=123)

    assert recovered == 1
    assert imported == [("run-stale", str(tmp_path / "telegram_results.json"))]
    assert removed == [("tg_monitoring", "zigomaro/telegram-monitor-bot")]


@pytest.mark.asyncio
async def test_recovery_imports_output_when_status_lookup_fails(monkeypatch, tmp_path: Path) -> None:
    imported: list[tuple[str, str]] = []
    removed: list[tuple[str, str]] = []

    class DummyKaggleClient:
        def get_kernel_status(self, kernel_ref: str) -> dict:
            assert kernel_ref == "zigomaro/telegram-monitor-bot"
            raise RuntimeError("GetKernelSessionStatus 500")

    async def fake_list_jobs(job_type: str | None = None):
        assert job_type == "tg_monitoring"
        return [
            {
                "kernel_ref": "zigomaro/telegram-monitor-bot",
                "meta": {"run_id": "run-output-ready", "pid": 999999},
            }
        ]

    async def fake_download_results(client, kernel_ref: str, run_id: str) -> Path:
        assert isinstance(client, DummyKaggleClient)
        assert kernel_ref == "zigomaro/telegram-monitor-bot"
        assert run_id == "run-output-ready"
        path = tmp_path / "telegram_results.json"
        path.write_text('{"messages":[]}', encoding="utf-8")
        return path

    async def fake_import_from_results(db, *, results_path, run_id: str, **kwargs):
        imported.append((run_id, str(results_path)))

    async def fake_remove_job(job_type: str, kernel_ref: str) -> None:
        removed.append((job_type, kernel_ref))

    monkeypatch.setattr(tg_service, "KaggleClient", DummyKaggleClient)
    monkeypatch.setattr(tg_service, "list_jobs", fake_list_jobs)
    monkeypatch.setattr(tg_service, "_download_results", fake_download_results)
    monkeypatch.setattr(tg_service, "run_telegram_import_from_results", fake_import_from_results)
    monkeypatch.setattr(tg_service, "remove_job", fake_remove_job)

    recovered = await tg_service.resume_telegram_monitor_jobs(object(), bot=None, chat_id=123)

    assert recovered == 1
    assert imported == [("run-output-ready", str(tmp_path / "telegram_results.json"))]
    assert removed == [("tg_monitoring", "zigomaro/telegram-monitor-bot")]


@pytest.mark.asyncio
async def test_recovery_skips_same_pid_job_while_run_lock_is_held(monkeypatch) -> None:
    class DummyKaggleClient:
        def get_kernel_status(self, kernel_ref: str) -> dict:
            raise AssertionError("active same-process monitor must not be polled by recovery")

    async def fake_list_jobs(job_type: str | None = None):
        return [
            {
                "kernel_ref": "zigomaro/telegram-monitor-bot",
                "meta": {"run_id": "run-active", "pid": os.getpid()},
            }
        ]

    monkeypatch.setattr(tg_service, "KaggleClient", DummyKaggleClient)
    monkeypatch.setattr(tg_service, "list_jobs", fake_list_jobs)

    async with tg_service._RUN_LOCK:
        recovered = await tg_service.resume_telegram_monitor_jobs(object(), bot=None, chat_id=123)

    assert recovered == 0
