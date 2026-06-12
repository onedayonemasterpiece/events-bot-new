from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

import telegraph_cache_sanitizer as sanitizer


@asynccontextmanager
async def _noop_heavy_operation(**kwargs):  # noqa: ANN003
    yield


@pytest.mark.asyncio
async def test_telegraph_cache_preflight_uses_selected_auth_scope(monkeypatch):
    seen: dict[str, str | None] = {}
    finished: list[dict] = []

    async def fake_start_ops_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return 123

    async def fake_finish_ops_run(*args, **kwargs):  # noqa: ANN002, ANN003
        finished.append(dict(kwargs))

    async def fake_collect_probe_targets(*args, **kwargs):  # noqa: ANN002, ANN003
        return [sanitizer.ProbeTarget(kind="event", url="https://telegra.ph/x")], {}

    async def fake_guard(**kwargs):  # noqa: ANN003
        seen.update(kwargs)
        raise RuntimeError("remote Telegram session is busy")

    monkeypatch.setenv("TELEGRAPH_CACHE_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_S22")
    monkeypatch.setattr(sanitizer, "start_ops_run", fake_start_ops_run)
    monkeypatch.setattr(sanitizer, "finish_ops_run", fake_finish_ops_run)
    monkeypatch.setattr(sanitizer, "heavy_operation", _noop_heavy_operation)
    monkeypatch.setattr(sanitizer, "collect_probe_targets", fake_collect_probe_targets)
    monkeypatch.setattr(sanitizer, "raise_if_remote_telegram_session_busy", fake_guard)

    with pytest.raises(RuntimeError, match="remote Telegram session is busy"):
        await sanitizer.run_telegraph_cache_sanitizer(
            db=object(),
            bot=None,
            chat_id=42,
            operator_id=7,
            trigger="manual",
            enqueue_regen=False,
        )

    assert seen == {
        "current_job_type": "telegraph_cache_probe",
        "current_auth_scope": "TELEGRAM_AUTH_BUNDLE_S22",
    }
    assert finished[-1]["status"] == "error"


@pytest.mark.asyncio
async def test_telegraph_cache_registers_remote_auth_scope(monkeypatch, tmp_path):
    registered: list[tuple[str, str, dict]] = []
    removed: list[tuple[str, str]] = []
    finished: list[dict] = []
    report_path = tmp_path / "telegraph_cache_report.json"
    report_path.write_text("{}", encoding="utf-8")

    async def fake_start_ops_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return 123

    async def fake_finish_ops_run(*args, **kwargs):  # noqa: ANN002, ANN003
        finished.append(dict(kwargs))

    async def fake_collect_probe_targets(*args, **kwargs):  # noqa: ANN002, ANN003
        return [sanitizer.ProbeTarget(kind="event", url="https://telegra.ph/x")], {}

    async def fake_guard(**kwargs):  # noqa: ANN003
        assert kwargs["current_job_type"] == "telegraph_cache_probe"
        assert kwargs["current_auth_scope"] == "TELEGRAM_AUTH_BUNDLE_S22"

    async def fake_prepare(*args, **kwargs):  # noqa: ANN002, ANN003
        return "zigomaro/telegraph-cache-probe-cipher", "zigomaro/telegraph-cache-probe-key"

    async def fake_push(*args, **kwargs):  # noqa: ANN002, ANN003
        return "zigomaro/telegraph-cache-probe"

    async def fake_poll(*args, **kwargs):  # noqa: ANN002, ANN003
        return "complete", {"status": "COMPLETE"}, 1.0

    async def fake_download(*args, **kwargs):  # noqa: ANN002, ANN003
        return report_path

    async def fake_import(*args, **kwargs):  # noqa: ANN002, ANN003
        return {"total": 1, "ok": 1, "fail": 0, "cached_ok": 1, "photo_ok": 1}

    async def fake_cleanup(_slugs):
        return None

    async def fake_remove_job(job_type: str, kernel_ref: str):
        removed.append((job_type, kernel_ref))

    async def fake_register_job(job_type: str, kernel_ref: str, *, meta=None):
        registered.append((job_type, kernel_ref, dict(meta or {})))

    monkeypatch.setenv("TELEGRAPH_CACHE_AUTH_BUNDLE_ENV", "TELEGRAM_AUTH_BUNDLE_S22")
    monkeypatch.setattr(sanitizer, "KAGGLE_DATASET_WAIT_SECONDS", 0)
    monkeypatch.setattr(sanitizer, "start_ops_run", fake_start_ops_run)
    monkeypatch.setattr(sanitizer, "finish_ops_run", fake_finish_ops_run)
    monkeypatch.setattr(sanitizer, "heavy_operation", _noop_heavy_operation)
    monkeypatch.setattr(sanitizer, "collect_probe_targets", fake_collect_probe_targets)
    monkeypatch.setattr(sanitizer, "raise_if_remote_telegram_session_busy", fake_guard)
    monkeypatch.setattr(sanitizer, "_build_secrets_payload", lambda: "{}")
    monkeypatch.setattr(sanitizer, "_prepare_kaggle_datasets", fake_prepare)
    monkeypatch.setattr(sanitizer, "_push_kernel", fake_push)
    monkeypatch.setattr(sanitizer, "_poll_kaggle_kernel", fake_poll)
    monkeypatch.setattr(sanitizer, "_download_results", fake_download)
    monkeypatch.setattr(sanitizer, "import_probe_results", fake_import)
    monkeypatch.setattr(sanitizer, "_cleanup_datasets", fake_cleanup)
    monkeypatch.setattr(sanitizer, "register_job", fake_register_job)
    monkeypatch.setattr(sanitizer, "remove_job", fake_remove_job)

    result = await sanitizer.run_telegraph_cache_sanitizer(
        db=object(),
        bot=None,
        chat_id=42,
        operator_id=7,
        trigger="manual",
        enqueue_regen=False,
    )

    assert result["imported"]["ok"] == 1
    assert registered == [
        (
            "telegraph_cache_probe",
            "zigomaro/telegraph-cache-probe",
            {
                "run_id": result["run_id"],
                "chat_id": 42,
                "pid": registered[0][2]["pid"],
                "remote_telegram_auth_scope": "TELEGRAM_AUTH_BUNDLE_S22",
            },
        )
    ]
    assert removed == [("telegraph_cache_probe", "zigomaro/telegraph-cache-probe")]
    assert finished[-1]["status"] == "success"
