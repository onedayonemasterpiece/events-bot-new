from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import region_talk_scheduled_runner as runner


def complete_env(tmp_path: Path) -> dict[str, str]:
    return {
        "REGION_TALK_YDB_ENDPOINT": "grpcs://ydb.example:2135",
        "REGION_TALK_YDB_DATABASE": "/ru-central1/example/db",
        "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON": "{}",
        "KAGGLE_USERNAME": "operator",
        "KAGGLE_KEY": "secret",
        "TELEGRAM_AUTH_BUNDLE_DISCOVERY1": "bundle-1",
        "TELEGRAM_AUTH_BUNDLE_DISCOVERY2": "bundle-2",
        "TELEGRAM_BOT_TOKEN": "bot-token",
        "TELEGRAM_API_ID": "123",
        "TELEGRAM_API_HASH": "hash",
        "SUPABASE_URL": "https://example.supabase.co",
        "SUPABASE_SERVICE_KEY": "service-key",
        "GOOGLE_API_KEY3": "google-key",
        "REGION_TALK_SCHEDULED_LOCK_FILE": str(tmp_path / "region-talk.lock"),
        "REGION_TALK_SCHEDULED_LOG_DIR": str(tmp_path / "logs"),
        "REGION_TALK_EXTERNAL_RESEARCH_ENABLED": "0",
    }


def test_missing_autonomy_config_reports_names_only(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env.pop("TELEGRAM_AUTH_BUNDLE_DISCOVERY2")
    env.pop("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON")

    missing = runner.missing_autonomy_config(env)

    assert "TELEGRAM_AUTH_BUNDLE_DISCOVERY2" in missing
    assert "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON" in missing
    assert "secret" not in " ".join(missing)


def test_scheduled_preflight_requires_bot_not_remote_human_session(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env.pop("TELEGRAM_BOT_TOKEN")
    env["TELEGRAM_AUTH_BUNDLE_E2E"] = "must-not-count-for-remote-delivery"
    env["TELEGRAM_SESSION"] = "must-not-count-either"

    missing = runner.missing_autonomy_config(env)

    assert "TELEGRAM_BOT_TOKEN" in missing
    assert "TELEGRAM_AUTH_BUNDLE_E2E|TELEGRAM_SESSION" not in missing


def test_build_command_does_not_require_dotenv(monkeypatch, tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("REGION_TALK_ORCHESTRATOR_ENV_FILE", raising=False)

    command = runner.build_orchestrator_command()

    assert "--loop" in command
    assert "--execute-ready" in command
    assert "--env-file" not in command
    assert command[command.index("--target-confirmed") + 1] == "0"


def test_external_research_command_is_server_side_and_has_no_telegram_session(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    command = runner.build_external_research_command(env)

    assert command[-1] == "--execute"
    assert command[1].endswith("region_talk_external_research_autorun.py")
    assert all("TELEGRAM" not in part for part in command)


def test_cli_preflight_is_redacted(monkeypatch, tmp_path: Path, capsys) -> None:
    env = complete_env(tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["region_talk_scheduled_runner.py", "--preflight-only"])

    assert runner.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload == {"ok": True, "missing": []}
    assert "google-key" not in json.dumps(payload)


def test_cli_runs_by_absolute_path_outside_repo(tmp_path: Path) -> None:
    env = {**os.environ, **complete_env(tmp_path)}
    completed = subprocess.run(
        [sys.executable, str(Path(runner.__file__).resolve()), "--preflight-only"],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {"ok": True, "missing": []}


@pytest.mark.asyncio
async def test_scheduled_run_writes_cycle_log_and_returns_metrics(monkeypatch, tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    class FakeProcess:
        returncode = 0

        def __init__(self) -> None:
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_data(
                b'{"ok":true,"cycle":1,"selected_actions":["launch_candidate_report"],'
                b'"metrics":{"publication_candidate_total":125,"publication_unsent_confirmed_total":0}}\n'
            )
            self.stdout.feed_eof()

        async def wait(self) -> int:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    async def fake_subprocess(*args, **kwargs):
        assert "--env-file" not in args
        assert kwargs["env"]["REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL"] == "1"
        assert kwargs["env"]["REGION_TALK_NOTIFY_TRANSPORT"] == "bot_api"
        return FakeProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)

    result = await runner.run_region_talk_scheduled(None, scheduler_run_id="sched-1")

    assert result["ok"] is True
    assert result["status"] == "success"
    assert result["metrics"]["publication_candidate_total"] == 125
    output_path = Path(result["output_path"])
    assert output_path.is_file()
    assert output_path.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_external_research_failure_does_not_stop_social_orchestrator(monkeypatch, tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env["REGION_TALK_EXTERNAL_RESEARCH_ENABLED"] = "1"
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    class ResearchProcess:
        returncode = 1

        async def communicate(self):
            return (
                b'{"ok":false,"stage":"external_research","status":"failed",'
                b'"error":"ProviderError: 429"}\n',
                None,
            )

        async def wait(self) -> int:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    class OrchestratorProcess:
        returncode = 0

        def __init__(self) -> None:
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_data(
                b'{"ok":true,"cycle":1,"selected_actions":["launch_candidate_report"],'
                b'"metrics":{"publication_candidate_total":128}}\n'
            )
            self.stdout.feed_eof()

        async def wait(self) -> int:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    processes = [ResearchProcess(), OrchestratorProcess()]

    async def fake_subprocess(*args, **kwargs):
        return processes.pop(0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)

    result = await runner.run_region_talk_scheduled(None, scheduler_run_id="research-failure")

    assert result["ok"] is True
    assert result["external_research_ok"] is False
    assert result["external_research_status"] == "failed"
    assert result["external_research_exit_code"] == 1
    assert result["metrics"]["publication_candidate_total"] == 128
