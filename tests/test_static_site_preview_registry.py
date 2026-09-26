from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "sync_static_site_preview_registry.py"
SPEC = importlib.util.spec_from_file_location("sync_static_site_preview_registry", MODULE_PATH)
assert SPEC and SPEC.loader
registry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(registry)


def test_operator_preflight_uses_boolean_remote_credential_presence(monkeypatch, tmp_path):
    flyctl = tmp_path / "flyctl"
    flyctl.write_text("#!/bin/sh\n")
    flyctl.chmod(0o700)
    monkeypatch.setattr(registry, "find_flyctl", lambda: flyctl)

    flags = {name: True for name in registry.REMOTE_OPERATOR_ENV}
    flags["KAGGLE_API_TOKEN"] = False

    def fake_run(args, **kwargs):
        if args[1] == "status":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout=registry.OPERATOR_PREFLIGHT_MARKER + json.dumps(flags) + "\n",
            stderr="",
        )

    monkeypatch.setattr(registry.subprocess, "run", fake_run)
    report = registry.operator_preflight("events-bot-new-wngqia")

    assert report["ready"] is True
    assert report["blockers"] == []
    assert report["fly_auth_ok"] is True
    assert report["flyctl"] == str(flyctl)
    assert report["remote_credentials"] == flags
    assert all(isinstance(value, bool) for value in report["remote_credentials"].values())


def test_operator_preflight_requires_one_kaggle_credential(monkeypatch, tmp_path):
    flyctl = tmp_path / "flyctl"
    flyctl.write_text("#!/bin/sh\n")
    flyctl.chmod(0o700)
    monkeypatch.setattr(registry, "find_flyctl", lambda: flyctl)

    flags = {name: True for name in registry.REMOTE_OPERATOR_ENV}
    flags["KAGGLE_KEY"] = False
    flags["KAGGLE_API_TOKEN"] = False

    def fake_run(args, **kwargs):
        if args[1] == "status":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout=registry.OPERATOR_PREFLIGHT_MARKER + json.dumps(flags) + "\n",
            stderr="",
        )

    monkeypatch.setattr(registry.subprocess, "run", fake_run)
    report = registry.operator_preflight("events-bot-new-wngqia")

    assert report["ready"] is False
    assert report["blockers"] == ["missing_remote_env:KAGGLE_KEY_or_KAGGLE_API_TOKEN"]
