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
        "GOOGLE_AI_LIMITER_SUPABASE_URL": "https://limiter.supabase.co",
        "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY": "limiter-service-key",
        "GOOGLE_API_KEY3": "google-key",
        "REGION_TALK_SCHEDULED_LOCK_FILE": str(tmp_path / "region-talk.lock"),
        "REGION_TALK_SCHEDULED_LOG_DIR": str(tmp_path / "logs"),
        "REGION_TALK_EXTERNAL_RESEARCH_ENABLED": "0",
        "REGION_TALK_PUBLICATION_PLAN_ENABLED": "0",
        "REGION_TALK_REACTION_SYNC_ENABLED": "0",
    }


def test_missing_autonomy_config_reports_names_only(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env.pop("TELEGRAM_AUTH_BUNDLE_DISCOVERY2")
    env.pop("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON")

    missing = runner.missing_autonomy_config(env)

    assert "TELEGRAM_AUTH_BUNDLE_DISCOVERY2" in missing
    assert "REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON" in missing
    assert "secret" not in " ".join(missing)


def test_scheduled_preflight_requires_bot_only_for_bot_api_transport(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env.pop("TELEGRAM_BOT_TOKEN")
    env["REGION_TALK_NOTIFY_TRANSPORT"] = "bot_api"
    env["TELEGRAM_AUTH_BUNDLE_E2E"] = "must-not-count-for-remote-delivery"
    env["TELEGRAM_SESSION"] = "must-not-count-either"

    missing = runner.missing_autonomy_config(env)

    assert "TELEGRAM_BOT_TOKEN" in missing
    assert "TELEGRAM_AUTH_BUNDLE_E2E|TELEGRAM_SESSION" not in missing


def test_scheduled_preflight_uses_discovery_bundle_for_telethon_transport(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env.pop("TELEGRAM_BOT_TOKEN")
    env["REGION_TALK_NOTIFY_TRANSPORT"] = "telethon_discovery2"

    assert runner.missing_autonomy_config(env) == []


def test_scheduled_preflight_rejects_unknown_notification_transport(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env["REGION_TALK_NOTIFY_TRANSPORT"] = "generic_human_session"

    assert "REGION_TALK_NOTIFY_TRANSPORT(valid)" in runner.missing_autonomy_config(env)


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


def test_publication_plan_command_is_server_side_and_has_no_telegram_session(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env["REGION_TALK_PUBLICATION_PLAN_DAYS"] = "21"
    command = runner.build_publication_plan_command(env)

    assert "--execute" in command
    assert "--days" in command
    assert command[command.index("--days") + 1] == "21"
    assert command[1].endswith("region_talk_publication_plan.py")
    assert all("TELEGRAM" not in part for part in command)


def test_reaction_sync_command_is_execute_only_and_never_loads_dotenv(tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env["REGION_TALK_REACTION_SYNC_LIMIT"] = "75"
    command = runner.build_reaction_sync_command(env)

    assert command[1].endswith("region_talk_reaction_sync.py")
    assert command[command.index("--env-file") + 1] == "/dev/null"
    assert "--execute" in command
    assert command[command.index("--limit") + 1] == "75"


def test_reaction_sync_busy_is_nonfatal_deferred_status() -> None:
    payload = {
        "ok": False,
        "error": "RuntimeError: region-talk-image-diagnostic is RUNNING; refusing concurrent use of its Telegram auth bundle",
    }
    assert runner.reaction_sync_status(payload, 1) == "deferred_d2_or_image_diagnostic_busy"


@pytest.mark.asyncio
async def test_reaction_sync_runs_after_orchestrator_before_publication_plan(monkeypatch, tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env["REGION_TALK_REACTION_SYNC_ENABLED"] = "1"
    env["REGION_TALK_PUBLICATION_PLAN_ENABLED"] = "1"
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    sync_script = tmp_path / "region_talk_reaction_sync.py"
    sync_script.write_text("# test placeholder\n", encoding="utf-8")
    monkeypatch.setattr(runner, "reaction_sync_script_path", lambda: sync_script)
    stages: list[str] = []

    class StreamProcess:
        returncode = 0

        def __init__(self, payload: dict[str, object], stage: str) -> None:
            self.stage = stage
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_data((json.dumps(payload) + "\n").encode())
            self.stdout.feed_eof()

        async def wait(self) -> int:
            stages.append(self.stage)
            return self.returncode

        async def communicate(self):
            stages.append(self.stage)
            raw = await self.stdout.read()
            return raw, None

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    processes = [
        StreamProcess({"ok": True, "cycle": 1, "metrics": {}}, "orchestrator"),
        StreamProcess({
            "ok": True,
            "stage": "operator_reaction_sync",
            "deliveries_observed_complete": 4,
            "candidate_projections_changed": 2,
        }, "reaction"),
        StreamProcess({"ok": True, "stage": "publication_plan", "counts": {}}, "plan"),
    ]

    async def fake_subprocess(*args, **kwargs):
        if str(args[1]).endswith("region_talk_reaction_sync.py"):
            assert args[args.index("--env-file") + 1] == "/dev/null"
            assert "TELEGRAM_SESSION" not in kwargs["env"]
            assert "TG_SESSION" not in kwargs["env"]
        return processes.pop(0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)
    result = await runner.run_region_talk_scheduled(None, scheduler_run_id="reaction-order")

    assert stages == ["orchestrator", "reaction", "plan"]
    assert result["ok"] is True
    assert result["reaction_sync_status"] == "complete"
    assert result["reaction_sync_deliveries_observed"] == 4
    assert result["metrics"]["reaction_candidate_projections_changed"] == 2


@pytest.mark.asyncio
async def test_failed_reaction_sync_preserves_existing_plan_and_exposes_new_intake(monkeypatch, tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env.update({
        "REGION_TALK_EXTERNAL_RESEARCH_ENABLED": "1",
        "REGION_TALK_REACTION_SYNC_ENABLED": "1",
        "REGION_TALK_PUBLICATION_PLAN_ENABLED": "1",
    })
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    sync_script = tmp_path / "region_talk_reaction_sync.py"
    sync_script.write_text("# unit\n", encoding="utf-8")
    monkeypatch.setattr(runner, "reaction_sync_script_path", lambda: sync_script)

    class Process:
        def __init__(self, payload: dict, returncode: int = 0):
            self.returncode = returncode
            self.raw = (json.dumps(payload) + "\n").encode()
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_data(self.raw)
            self.stdout.feed_eof()

        async def wait(self):
            return self.returncode

        async def communicate(self):
            return self.raw, None

        def terminate(self):
            self.returncode = -15

        def kill(self):
            self.returncode = -9

    processes = [
        Process({
            "ok": True, "stage": "external_research", "status": "complete",
            "ready_for_region_talk_scoring": 2, "new_intake_count": 2,
            "new_intake_ids": ["ext-b", "ext-a"],
        }),
        Process({"ok": True, "cycle": 1, "metrics": {}}),
        Process({
            "ok": False, "stage": "operator_reaction_sync",
            "error": "incomplete reaction page",
        }, returncode=1),
    ]
    launched: list[str] = []

    async def fake_subprocess(*args, **_kwargs):
        launched.append(str(args[1]))
        return processes.pop(0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)

    result = await runner.run_region_talk_scheduled(None, scheduler_run_id="reaction-fail-closed")

    assert not any(path.endswith("region_talk_publication_plan.py") for path in launched)
    assert result["publication_plan_ok"] is False
    assert result["publication_plan_status"] == "deferred_reaction_sync_not_current"
    assert result["publication_plan_counts"] == {}
    assert result["external_publication_intake_new_count"] == 2
    assert result["external_publication_intake_new_ids"] == ["ext-a", "ext-b"]


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
    env["TELEGRAM_AUTH_BUNDLE_E2E"] = "codex-only-must-be-stripped"
    env["TELEGRAM_SESSION"] = "generic-human-session-must-be-stripped"
    env["TG_SESSION"] = "generic-human-session-must-also-be-stripped"
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
        assert kwargs["limit"] >= 256 * 1024
        assert kwargs["env"]["REGION_TALK_REQUIRE_NONINTERACTIVE_YDB_CREDENTIAL"] == "1"
        assert kwargs["env"]["REGION_TALK_NOTIFY_TRANSPORT"] == "telethon_discovery2"
        assert "TELEGRAM_AUTH_BUNDLE_E2E" not in kwargs["env"]
        assert "TELEGRAM_SESSION" not in kwargs["env"]
        assert "TG_SESSION" not in kwargs["env"]
        assert kwargs["env"]["REGION_TALK_AUTH_BUNDLE_ENV"] == "TELEGRAM_AUTH_BUNDLE_DISCOVERY1"
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
async def test_scheduled_run_drains_orchestrator_json_line_larger_than_asyncio_default(
    monkeypatch, tmp_path: Path
) -> None:
    env = complete_env(tmp_path)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    command = [
        sys.executable,
        "-c",
        (
            "import json; "
            "print(json.dumps({'ok': True, 'cycle': 1, "
            "'selected_actions': ['launch_candidate_report'], "
            "'metrics': {'publication_candidate_total': 130}, "
            "'padding': 'x' * 300000}))"
        ),
    ]
    monkeypatch.setattr(runner, "build_orchestrator_command", lambda: command)

    result = await asyncio.wait_for(
        runner.run_region_talk_scheduled(None, scheduler_run_id="oversized-cycle-line"),
        timeout=10,
    )

    assert result["ok"] is True
    assert result["metrics"]["publication_candidate_total"] == 130
    output_path = Path(result["output_path"])
    assert output_path.stat().st_size > 300_000


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


@pytest.mark.asyncio
async def test_scheduled_run_recalculates_publication_plan_after_orchestrator(monkeypatch, tmp_path: Path) -> None:
    env = complete_env(tmp_path)
    env["REGION_TALK_PUBLICATION_PLAN_ENABLED"] = "1"
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    class OrchestratorProcess:
        returncode = 0

        def __init__(self) -> None:
            self.stdout = asyncio.StreamReader()
            self.stdout.feed_data(b'{"ok":true,"cycle":1,"metrics":{"publication_candidate_total":129}}\n')
            self.stdout.feed_eof()

        async def wait(self) -> int:
            return 0

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    class PlanProcess:
        returncode = 0

        async def communicate(self):
            return (
                b'{"ok":true,"stage":"publication_plan","snapshot_id":"rtdayplan_test",'
                b'"counts":{"planned_article":3,"planned_social":14}}\n',
                None,
            )

        async def wait(self) -> int:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    processes = [OrchestratorProcess(), PlanProcess()]

    async def fake_subprocess(*args, **kwargs):
        return processes.pop(0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)
    result = await runner.run_region_talk_scheduled(None, scheduler_run_id="plan-after-discovery")

    assert result["ok"] is True
    assert result["publication_plan_ok"] is True
    assert result["publication_plan_snapshot_id"] == "rtdayplan_test"
    assert result["publication_plan_counts"]["planned_article"] == 3
