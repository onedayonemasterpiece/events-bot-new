from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from db import Database
from models import TelegramSource
from source_parsing.telegram.handlers import TelegramMonitorEventInfo
from source_parsing.telegram.service import (
    _build_config_payload,
    _build_secrets_payload,
    _format_event_block,
    _poll_kaggle_kernel,
)


def test_google_ai_bundle_contains_complete_deterministic_source_tree(tmp_path) -> None:
    import source_parsing.telegram.service as tg_service

    expected = sorted(
        path.relative_to(tg_service.GOOGLE_AI_PACKAGE_PATH).as_posix()
        for path in tg_service.GOOGLE_AI_PACKAGE_PATH.rglob("*.py")
        if "__pycache__" not in path.parts
    )

    embedded = tg_service._embedded_google_ai_sources()

    assert list(embedded) == expected
    assert "__init__.py" in embedded
    assert "limiter_supabase.py" in embedded
    assert "interactions.py" in embedded

    staged_root = tmp_path / "staged"
    tg_service._stage_google_ai_bundle(staged_root)
    staged = sorted(
        path.relative_to(staged_root / "google_ai").as_posix()
        for path in (staged_root / "google_ai").rglob("*")
        if path.is_file()
    )
    assert staged == expected
    assert not list(staged_root.rglob("*.pyc"))


def test_generated_telegram_notebook_imports_complete_google_ai_package(tmp_path) -> None:
    import source_parsing.telegram.service as tg_service

    runner = tmp_path / "telegram_monitor.py"
    runner.write_text(
        """from __future__ import annotations

from google_ai import AntigravityInteractionsClient, GoogleAIClient
from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client
from source_contradiction_facts import derive_source_contradiction_facts
from source_parse_contract import EvidenceManifest, SourceParseDecision

assert GoogleAIClient is not None
assert AntigravityInteractionsClient is not None
assert build_google_ai_limiter_supabase_client is not None
assert derive_source_contradiction_facts(
    "clean", [], {"today": "2026-08-12"},
    SourceParseDecision([{"title": "Named event"}], evidence_manifest=EvidenceManifest.complete_source("clean")),
    EvidenceManifest.complete_source("clean"),
) == ()
print("tg-shared-source-parse-import-closure-ok")
""",
        encoding="utf-8",
    )
    notebook = tg_service._build_notebook_payload_from_script(runner)
    generated_runner = tmp_path / "generated_telegram_notebook.py"
    generated_runner.write_text(
        "".join(notebook["cells"][1]["source"]),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-I", str(generated_runner)],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert "tg-shared-source-parse-import-closure-ok" in completed.stdout


@pytest.fixture(autouse=True)
def _video_monitoring_security_config(monkeypatch):
    monkeypatch.setenv(
        "TG_MONITORING_VIDEO_REPUBLICATION_ALLOWED_SOURCES",
        "meowafisha",
    )
    monkeypatch.setenv(
        "TG_MONITORING_VIDEO_ANALYSIS_CACHE_KEY",
        "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
    )


@pytest.mark.asyncio
async def test_build_config_payload_can_scope_to_single_source(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(TelegramSource(username="testsourceurl", enabled=True, trust_level="high"))
        session.add(TelegramSource(username="otherchannel", enabled=True, trust_level="low"))
        session.add(TelegramSource(username="disabled", enabled=False, trust_level="high"))
        await session.commit()

    payload = await _build_config_payload(
        db,
        run_id="single_source_test",
        source_usernames=["https://telegram.me/testsourceurl", "disabled"],
    )

    assert payload["channels"] == ["testsourceurl"]
    assert [item["username"] for item in payload["sources"]] == ["testsourceurl"]
    assert payload["requested_source_usernames"] == ["disabled", "testsourceurl"]
    await db.engine.dispose()


@pytest.mark.asyncio
async def test_post_push_registry_failure_preserves_tg_intent_and_datasets(
    monkeypatch,
):
    import source_parsing.telegram.service as tg_service

    calls: list[tuple] = []

    class DummyKaggleClient:
        def get_kernel_revision(self, _kernel_ref):
            return 8

    async def fake_build_config(*_args, **_kwargs):
        return {"sources": [], "telegraph_urls": []}

    async def fake_not_busy(**_kwargs):
        return None

    async def fake_prepare(**_kwargs):
        return "owner/cipher-run", "owner/key-run"

    async def fake_intent(*args, **kwargs):
        calls.append(("intent", args, kwargs))

    async def fake_push(*_args, **_kwargs):
        calls.append(("push",))
        return (
            "zigomaro/telegram-monitor-bot",
            {"ref": "zigomaro/telegram-monitor-bot", "version_number": 9},
        )

    async def fake_promote(*_args, **_kwargs):
        calls.append(("promotion",))
        raise RuntimeError("registry promotion failed")

    async def fake_cleanup(slugs):
        calls.append(("cleanup", tuple(slugs)))

    monkeypatch.setattr(tg_service, "KaggleClient", DummyKaggleClient)
    monkeypatch.setattr(tg_service, "_build_config_payload", fake_build_config)
    monkeypatch.setattr(tg_service, "_build_secrets_payload", lambda: "{}")
    monkeypatch.setattr(tg_service, "raise_if_remote_telegram_session_busy", fake_not_busy)
    monkeypatch.setattr(tg_service, "_prepare_kaggle_datasets", fake_prepare)
    monkeypatch.setattr(tg_service, "register_launch_intent", fake_intent)
    monkeypatch.setattr(tg_service, "_push_kernel", fake_push)
    monkeypatch.setattr(tg_service, "promote_launch_intent", fake_promote)
    monkeypatch.setattr(tg_service, "_cleanup_datasets", fake_cleanup)
    monkeypatch.setattr(tg_service, "DATASET_PROPAGATION_WAIT_SECONDS", 0)

    with pytest.raises(RuntimeError, match="registry promotion failed"):
        await tg_service._run_telegram_monitor_locked(
            object(), run_id="promotion-run", send_progress=False
        )

    assert [call[0] for call in calls] == ["intent", "push", "promotion"]


def test_build_secrets_payload_includes_yandex_storage_env(monkeypatch):
    monkeypatch.setenv("TG_API_ID", "123")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("GOOGLE_API_KEY3", "google")
    monkeypatch.setenv("GOOGLE_API_KEY5", "video-google")
    monkeypatch.setenv("TG_SESSION", "session")
    monkeypatch.setenv("YC_SA_BOT_STORAGE", "access")
    monkeypatch.setenv("YC_SA_BOT_STORAGE_KEY", "secret")
    monkeypatch.setenv("YC_STORAGE_BUCKET", "kenigevents")
    monkeypatch.setenv("YC_STORAGE_ENDPOINT", "https://storage.yandexcloud.net")
    monkeypatch.setenv(
        "GOOGLE_AI_LIMITER_SUPABASE_URL", "https://limiter.supabase.co"
    )
    monkeypatch.setenv(
        "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", "limiter-service-key"
    )

    payload = json.loads(_build_secrets_payload())

    assert payload["YC_SA_BOT_STORAGE"] == "access"
    assert payload["YC_SA_BOT_STORAGE_KEY"] == "secret"
    assert payload["YC_STORAGE_BUCKET"] == "kenigevents"
    assert payload["YC_STORAGE_ENDPOINT"] == "https://storage.yandexcloud.net"
    assert payload["GOOGLE_AI_LIMITER_SUPABASE_URL"] == "https://limiter.supabase.co"
    assert payload["GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY"] == "limiter-service-key"


def test_build_secrets_payload_ships_exact_declared_video_pool(monkeypatch):
    monkeypatch.setenv("TG_API_ID", "123")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("TG_SESSION", "session")
    monkeypatch.setenv("TG_MONITORING_GOOGLE_KEY_ENV", "GOOGLE_API_KEY3")
    monkeypatch.setenv("TG_MONITORING_GOOGLE_FALLBACK_KEY_ENV", "GOOGLE_API_KEY4")
    monkeypatch.setenv(
        "TG_MONITORING_VIDEO_GOOGLE_KEY_ENVS",
        "GOOGLE_API_KEY3,GOOGLE_API_KEY5,GOOGLE_API_KEY5",
    )
    monkeypatch.setenv("GOOGLE_API_KEY3", "text-primary")
    monkeypatch.setenv("GOOGLE_API_KEY4", "text-fallback")
    monkeypatch.setenv("GOOGLE_API_KEY5", "video-secondary")
    monkeypatch.setenv("GOOGLE_API_KEY2", "must-not-ship")

    payload = json.loads(_build_secrets_payload())

    assert payload["TG_MONITORING_VIDEO_GOOGLE_KEY_ENVS"] == "GOOGLE_API_KEY3,GOOGLE_API_KEY5"
    assert payload["GOOGLE_API_KEY3"] == "text-primary"
    assert payload["GOOGLE_API_KEY4"] == "text-fallback"
    assert payload["GOOGLE_API_KEY5"] == "video-secondary"
    assert "GOOGLE_API_KEY2" not in payload
    assert payload["TG_MONITORING_VIDEO_REPUBLICATION_ALLOWED_SOURCES"] == "meowafisha"
    assert payload["TG_MONITORING_VIDEO_ANALYSIS_CACHE_KEY"]


def test_build_secrets_payload_requires_every_declared_video_key(monkeypatch):
    import source_parsing.telegram.service as tg_service

    monkeypatch.setenv("TG_API_ID", "123")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("TG_SESSION", "session")
    monkeypatch.setenv("GOOGLE_API_KEY3", "text-primary")
    monkeypatch.delenv("GOOGLE_API_KEY5", raising=False)
    monkeypatch.setattr(tg_service, "_read_env_file_value", lambda _key: None)
    monkeypatch.setenv(
        "TG_MONITORING_VIDEO_GOOGLE_KEY_ENVS",
        "GOOGLE_API_KEY3,GOOGLE_API_KEY5",
    )

    with pytest.raises(RuntimeError, match="GOOGLE_API_KEY5"):
        _build_secrets_payload()


def test_build_secrets_payload_requires_multiple_video_keys(monkeypatch):
    monkeypatch.setenv("TG_API_ID", "123")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("TG_SESSION", "session")
    monkeypatch.setenv("GOOGLE_API_KEY3", "text-primary")
    monkeypatch.setenv("TG_MONITORING_VIDEO_GOOGLE_KEY_ENVS", "GOOGLE_API_KEY3")

    with pytest.raises(RuntimeError, match="at least two distinct keys"):
        _build_secrets_payload()


@pytest.mark.asyncio
async def test_poll_kaggle_kernel_retries_status_http_500(monkeypatch):
    import source_parsing.telegram.service as tg_service

    class Response:
        status_code = 500

    class StatusError(RuntimeError):
        response = Response()

    class FakeClient:
        def __init__(self):
            self.calls = 0

        def get_kernel_status(self, _kernel_ref):
            self.calls += 1
            if self.calls == 1:
                raise StatusError(
                    "500 Server Error: Internal Server Error for url: "
                    "https://api.kaggle.com/v1/kernels.KernelsApiService/GetKernelSessionStatus"
                )
            return {"status": "COMPLETE"}

    statuses = []

    async def status_callback(phase, kernel_ref, status):
        statuses.append((phase, kernel_ref, status))

    monkeypatch.setattr(tg_service, "POLL_INTERVAL_SECONDS", 0)
    client = FakeClient()

    status, status_data, _duration = await _poll_kaggle_kernel(
        client,
        "zigomaro/telegram-monitor-bot",
        run_id="run-http-500",
        timeout_minutes=1,
        status_callback=status_callback,
    )

    assert status == "complete"
    assert status_data == {"status": "COMPLETE"}
    assert client.calls == 2
    assert any(phase == "poll_error" for phase, _kernel, _status in statuses)


@pytest.mark.asyncio
async def test_poll_kaggle_kernel_completes_from_output_when_status_api_keeps_500(
    tmp_path, monkeypatch
):
    import source_parsing.telegram.service as tg_service

    class Response:
        status_code = 500

    class StatusError(RuntimeError):
        response = Response()

    class FakeClient:
        def __init__(self):
            self.status_calls = 0
            self.download_calls = 0

        def get_kernel_status(self, _kernel_ref):
            self.status_calls += 1
            raise StatusError(
                "500 Server Error: Internal Server Error for url: "
                "https://api.kaggle.com/v1/kernels.KernelsApiService/GetKernelSessionStatus"
            )

        def download_kernel_output(self, _kernel_ref, *, path, force):
            self.download_calls += 1
            result_path = Path(path) / "telegram_results.json"
            result_path.write_text(
                json.dumps({"run_id": "run-output-ready"}),
                encoding="utf-8",
            )
            return ["telegram_results.json"]

    statuses = []

    async def status_callback(phase, kernel_ref, status):
        statuses.append((phase, kernel_ref, status))

    monkeypatch.setattr(tg_service, "POLL_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(tg_service.tempfile, "gettempdir", lambda: str(tmp_path))
    client = FakeClient()

    status, status_data, _duration = await _poll_kaggle_kernel(
        client,
        "zigomaro/telegram-monitor-bot",
        run_id="run-output-ready",
        timeout_minutes=1,
        status_callback=status_callback,
    )

    assert status == "complete"
    assert status_data["status"] == "COMPLETE"
    assert status_data["_completion_source"] == "kaggle_output_after_status_error"
    assert client.status_calls == 1
    assert client.download_calls == 1
    assert any(phase == "poll_error" for phase, _kernel, _status in statuses)
    assert any(phase == "complete" for phase, _kernel, _status in statuses)


def test_format_event_block_shows_vk_and_tg_posts_line():
    event = TelegramMonitorEventInfo(
        event_id=42,
        title="Лекция",
        date="2026-06-20",
        time="18:00",
        source_link="https://t.me/source/1",
        telegraph_url="https://telegra.ph/event",
        ics_url="https://example.com/event.ics",
        log_cmd="/log 42",
        fact_stats=None,
        photo_count=1,
        added_posters=1,
        vk_post_url="https://vk.com/wall-231920894_2403",
    )
    ctx = SimpleNamespace(
        event_posts_by_event_id={
            42: SimpleNamespace(
                vk_post_url="https://vk.com/wall-231920894_2403",
                tg_post_url="https://t.me/c/3954607218/7",
            )
        },
        sources_by_event_id={},
        video_count_by_event_id={},
        ticket_queue_by_event_id={},
        festival_queue_by_source_url={},
        tz=None,
    )

    lines = _format_event_block("Созданные события", [event], icon="✅", ctx=ctx)
    text = "\n".join(lines)

    assert 'Посты: VK <a href="https://vk.com/wall-231920894_2403">пост</a>' in text
    assert 'TG <a href="https://t.me/c/3954607218/7">пост</a>' in text


def test_format_event_block_marks_deferred_tg_post_pending():
    event = TelegramMonitorEventInfo(
        event_id=43,
        title="Спектакль",
        date="2026-06-21",
        time="19:00",
        source_link="https://t.me/source/2",
        telegraph_url="https://telegra.ph/event2",
        ics_url=None,
        log_cmd="/log 43",
        fact_stats=None,
        photo_count=0,
        added_posters=0,
        vk_post_url="https://vk.com/wall-231920894_2404",
    )
    ctx = SimpleNamespace(
        event_posts_by_event_id={
            43: SimpleNamespace(
                vk_post_url="https://vk.com/wall-231920894_2404",
                tg_post_url=None,
            )
        },
        sources_by_event_id={},
        video_count_by_event_id={},
        ticket_queue_by_event_id={},
        festival_queue_by_source_url={},
        tz=None,
    )

    lines = _format_event_block("Созданные события", [event], icon="✅", ctx=ctx)
    text = "\n".join(lines)

    assert 'Посты: VK <a href="https://vk.com/wall-231920894_2404">пост</a> · TG ⏳' in text
