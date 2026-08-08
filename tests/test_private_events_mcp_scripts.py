from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.generate_private_events_mcp_credentials import (
    create_private_output_dir,
    write_private_text,
)
from scripts.smoke_private_events_mcp import sanitized_endpoint_receipt


def test_generator_stdout_redacts_private_endpoint(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[1]
    output = tmp_path / "generated"
    completed = subprocess.run(
        [
            sys.executable,
            str(repo / "scripts/generate_private_events_mcp_credentials.py"),
            "--base-url",
            "https://events.example",
            "--output-dir",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    receipt = json.loads(completed.stdout)
    generated = json.loads(
        (output / "chatgpt-private-app-credentials.json").read_text(encoding="utf-8")
    )
    private_url = generated["chatgpt"]["mcp_url"]
    codex_url = generated["codex"]["mcp_url"]
    path_secret = generated["deploy"]["PRIVATE_EVENTS_MCP_PATH_SECRET"]
    assert private_url not in completed.stdout
    assert codex_url not in completed.stdout
    assert path_secret not in completed.stdout
    assert receipt["public_origin"] == "https://events.example"
    assert receipt["mcp_path"] == "/_private/<redacted>/mcp"
    assert len(receipt["endpoint_fingerprint"]) == 12
    assert len(receipt["codex_endpoint_fingerprint"]) == 12
    assert codex_url.endswith("/codex/mcp")
    assert "telegram:publish" not in generated["chatgpt"]["oauth_scopes"]
    assert "telegram:publish" not in generated["codex"]["oauth_scopes"]
    assert output.stat().st_mode & 0o777 == 0o700

    social_output = tmp_path / "generated-social"
    subprocess.run(
        [
            sys.executable,
            str(repo / "scripts/generate_private_events_mcp_credentials.py"),
            "--base-url",
            "https://events.example",
            "--output-dir",
            str(social_output),
            "--enable-chatgpt-social",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    social = json.loads(
        (social_output / "chatgpt-private-app-credentials.json").read_text(
            encoding="utf-8"
        )
    )
    assert "telegram:publish" in social["chatgpt"]["oauth_scopes"]
    assert "vk:publish" in social["chatgpt"]["oauth_scopes"]


def test_smoke_receipt_redacts_private_endpoint() -> None:
    endpoint = "https://events.example/_private/mcp_super_secret_value/mcp"
    receipt = sanitized_endpoint_receipt(endpoint)
    encoded = json.dumps(receipt)
    assert endpoint not in encoded
    assert "mcp_super_secret_value" not in encoded
    assert receipt["public_origin"] == "https://events.example"
    assert receipt["mcp_path"] == "/_private/<redacted>/mcp"

    codex_endpoint = "https://events.example/_private/mcp_super_secret_value/codex/mcp"
    codex_receipt = sanitized_endpoint_receipt(codex_endpoint)
    assert codex_endpoint not in json.dumps(codex_receipt)
    assert codex_receipt["mcp_path"] == "/_private/<redacted>/codex/mcp"


def test_private_artifact_is_created_exclusively_with_mode_0600(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[tuple[int, int]] = []
    real_open = os.open

    def tracked_open(path, flags, mode=0o777):
        calls.append((flags, mode))
        return real_open(path, flags, mode)

    monkeypatch.setattr("scripts.generate_private_events_mcp_credentials.os.open", tracked_open)
    target = tmp_path / "secret.json"
    write_private_text(target, '{"secret":"value"}\n')
    assert target.stat().st_mode & 0o777 == 0o600
    assert len(calls) == 1
    flags, mode = calls[0]
    assert flags & os.O_CREAT
    assert flags & os.O_EXCL
    assert mode == 0o600
    with pytest.raises(FileExistsError):
        write_private_text(target, "replacement forbidden")


def test_private_output_directory_is_fresh_and_mode_0700(tmp_path: Path) -> None:
    output = tmp_path / "private-output"
    create_private_output_dir(output)
    assert output.stat().st_mode & 0o777 == 0o700
    with pytest.raises(FileExistsError):
        create_private_output_dir(output)
