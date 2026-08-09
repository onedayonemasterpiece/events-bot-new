from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.apply_private_events_mcp_overlay import patch_main
from scripts.generate_private_events_mcp_credentials import (
    create_private_output_dir,
    write_private_text,
)
from scripts.smoke_private_events_mcp import sanitized_endpoint_receipt


def run_generator(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    repo = Path(__file__).resolve().parents[1]
    return subprocess.run(
        [
            sys.executable,
            str(repo / "scripts/generate_private_events_mcp_credentials.py"),
            *arguments,
        ],
        check=check,
        capture_output=True,
        text=True,
    )


def test_generator_stdout_redacts_private_endpoint(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[1]
    output = tmp_path / "generated"
    completed = subprocess.run(
        [
            sys.executable,
            str(repo / "scripts/generate_private_events_mcp_credentials.py"),
            "--new-install",
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
    assert receipt["mode"] == "new_install"
    generated = json.loads(
        (output / "chatgpt-private-app-credentials.json").read_text(encoding="utf-8")
    )
    private_url = generated["chatgpt"]["mcp_url"]
    codex_url = generated["codex"]["mcp_url"]
    path_secret = generated["deploy"]["PRIVATE_EVENTS_MCP_PATH_SECRET"]
    approval_token = generated["deploy"]["PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN"]
    signing_key = generated["deploy"]["PRIVATE_EVENTS_MCP_SIGNING_KEY"]
    client_secret = generated["deploy"]["PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET"]
    operator_token = generated["deploy"]["PRIVATE_EVENTS_MCP_OPERATOR_TOKEN"]
    assert private_url not in completed.stdout
    assert codex_url not in completed.stdout
    assert path_secret not in completed.stdout
    assert approval_token not in completed.stdout
    assert signing_key not in completed.stdout
    assert client_secret not in completed.stdout
    assert operator_token not in completed.stdout
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
            "--new-install",
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
    assert "telegram:post:publish" in social["chatgpt"]["oauth_scopes"]
    assert "telegram:dm:send" in social["chatgpt"]["oauth_scopes"]
    assert "vk:notifications:read" in social["chatgpt"]["oauth_scopes"]
    assert "vk:notifications:read" in social["chatgpt"]["available_optional_scopes"]
    assert "vk:story:write" not in social["chatgpt"]["oauth_scopes"]
    assert "telegram:publish" not in social["chatgpt"]["oauth_scopes"]


@pytest.mark.parametrize(
    "unsafe_origin",
    [
        "https://user:stdout-secret@events.example",
        "https://events.example?token=stdout-secret",
        "https://events.example#stdout-secret",
        "https://events.example:444",
        "https://events.example\nstdout-secret.example",
        "https://events.example\tstdout-secret.example",
        "https://example..com",
        "https://-bad.example",
        "https://bad-.example",
        "https://127.1",
        "https://127.000.000.001",
        "https://2130706433",
        "https://0x7f000001",
        "https://[fe80::1%25eth0]",
    ],
)
def test_generator_rejects_noncanonical_or_secret_bearing_origins_without_output(
    tmp_path: Path, unsafe_origin: str
) -> None:
    output = tmp_path / "rejected-origin"
    completed = run_generator(
        "--new-install",
        "--base-url",
        unsafe_origin,
        "--output-dir",
        str(output),
        check=False,
    )
    assert completed.returncode != 0
    assert not output.exists()
    assert "stdout-secret" not in completed.stdout
    assert "stdout-secret" not in completed.stderr


def test_generator_requires_one_explicit_identity_mode(tmp_path: Path) -> None:
    implicit_output = tmp_path / "implicit"
    implicit = run_generator(
        "--base-url",
        "https://events.example",
        "--output-dir",
        str(implicit_output),
        check=False,
    )
    assert implicit.returncode != 0
    assert "--new-install" in implicit.stderr
    assert not implicit_output.exists()

    missing_base_output = tmp_path / "missing-base"
    missing_base = run_generator(
        "--new-install",
        "--output-dir",
        str(missing_base_output),
        check=False,
    )
    assert missing_base.returncode != 0
    assert "requires --base-url" in missing_base.stderr
    assert not missing_base_output.exists()


def test_bootstrap_rotation_preserves_every_stable_identity_value(
    tmp_path: Path,
) -> None:
    original_dir = tmp_path / "original"
    original_receipt = run_generator(
        "--new-install",
        "--base-url",
        "https://events.example",
        "--output-dir",
        str(original_dir),
        "--enable-chatgpt-social",
    )
    source = original_dir / "chatgpt-private-app-credentials.json"
    before = json.loads(source.read_text(encoding="utf-8"))
    before["deploy"]["PRIVATE_EVENTS_MCP_FUTURE_SAFE_FIELD"] = "future-safe-value"
    source.write_text(json.dumps(before), encoding="utf-8")

    rotated_dir = tmp_path / "rotated"
    completed = run_generator(
        "--rotate-bootstrap-only",
        str(source),
        "--output-dir",
        str(rotated_dir),
    )
    after = json.loads(
        (rotated_dir / "chatgpt-private-app-credentials.json").read_text(
            encoding="utf-8"
        )
    )
    old_operator = before["deploy"]["PRIVATE_EVENTS_MCP_OPERATOR_TOKEN"]
    new_operator = after["deploy"]["PRIVATE_EVENTS_MCP_OPERATOR_TOKEN"]
    assert new_operator.startswith("operator_")
    assert new_operator != old_operator
    assert after["chatgpt"]["bootstrap_operator_token"] == new_operator
    assert after["codex"]["bootstrap_operator_token"] == new_operator

    expected = json.loads(json.dumps(before))
    expected["deploy"]["PRIVATE_EVENTS_MCP_OPERATOR_TOKEN"] = new_operator
    expected["chatgpt"]["bootstrap_operator_token"] = new_operator
    expected["codex"]["bootstrap_operator_token"] = new_operator
    assert after == expected
    assert (
        after["deploy"]["PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN"]
        == before["deploy"]["PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN"]
    )
    assert (
        after["deploy"]["PRIVATE_EVENTS_MCP_SIGNING_KEY"]
        == before["deploy"]["PRIVATE_EVENTS_MCP_SIGNING_KEY"]
    )

    original_public = json.loads(original_receipt.stdout)
    rotated_public = json.loads(completed.stdout)
    assert rotated_public["mode"] == "rotate_bootstrap_only"
    assert rotated_public["endpoint_fingerprint"] == original_public["endpoint_fingerprint"]
    assert (
        rotated_public["codex_endpoint_fingerprint"]
        == original_public["codex_endpoint_fingerprint"]
    )
    sensitive_values = {
        old_operator,
        new_operator,
        before["deploy"]["PRIVATE_EVENTS_MCP_PATH_SECRET"],
        before["deploy"]["PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET"],
        before["deploy"]["PRIVATE_EVENTS_MCP_SIGNING_KEY"],
        before["deploy"]["PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN"],
        before["chatgpt"]["mcp_url"],
        before["codex"]["mcp_url"],
    }
    assert all(value not in completed.stdout for value in sensitive_values)
    assert rotated_dir.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in rotated_dir.iterdir())


@pytest.mark.parametrize(
    ("section", "missing_key"),
    [
        ("deploy", "PRIVATE_EVENTS_MCP_SIGNING_KEY"),
        ("deploy", "PRIVATE_EVENTS_MCP_SOCIAL_APPROVAL_TOKEN"),
        ("chatgpt", "social_approval_operator_token"),
    ],
)
def test_bootstrap_rotation_rejects_incomplete_full_credentials_without_output(
    tmp_path: Path, section: str, missing_key: str
) -> None:
    source_dir = tmp_path / "source"
    run_generator(
        "--new-install",
        "--base-url",
        "https://events.example",
        "--output-dir",
        str(source_dir),
    )
    full = json.loads(
        (source_dir / "chatgpt-private-app-credentials.json").read_text(
            encoding="utf-8"
        )
    )
    secret = full["deploy"]["PRIVATE_EVENTS_MCP_SIGNING_KEY"]
    del full[section][missing_key]
    incomplete = tmp_path / f"incomplete-{section}.json"
    incomplete.write_text(json.dumps(full), encoding="utf-8")
    output = tmp_path / f"rejected-{section}"
    completed = run_generator(
        "--rotate-bootstrap-only",
        str(incomplete),
        "--output-dir",
        str(output),
        check=False,
    )
    assert completed.returncode != 0
    assert not output.exists()
    assert secret not in completed.stdout
    assert secret not in completed.stderr


def test_bootstrap_rotation_rejects_unknown_deploy_env_injection_without_output(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    run_generator(
        "--new-install",
        "--base-url",
        "https://events.example",
        "--output-dir",
        str(source_dir),
    )
    full = json.loads(
        (source_dir / "chatgpt-private-app-credentials.json").read_text(
            encoding="utf-8"
        )
    )
    full["deploy"]["PRIVATE_EVENTS_MCP_FUTURE_SAFE_FIELD"] = (
        "safe-prefix\nINJECTED_ENV=must-not-appear"
    )
    poisoned = tmp_path / "poisoned.json"
    poisoned.write_text(json.dumps(full), encoding="utf-8")
    output = tmp_path / "rejected-injection"
    completed = run_generator(
        "--rotate-bootstrap-only",
        str(poisoned),
        "--output-dir",
        str(output),
        check=False,
    )
    assert completed.returncode != 0
    assert not output.exists()
    assert "must-not-appear" not in completed.stdout


def test_bootstrap_rotation_rejects_symlinks_and_source_output_overlap(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    run_generator(
        "--new-install",
        "--base-url",
        "https://events.example",
        "--output-dir",
        str(source_dir),
    )
    source = source_dir / "chatgpt-private-app-credentials.json"
    source_secret = json.loads(source.read_text(encoding="utf-8"))["deploy"][
        "PRIVATE_EVENTS_MCP_SIGNING_KEY"
    ]

    linked_source = tmp_path / "credentials-link.json"
    linked_source.symlink_to(source)
    linked_output = tmp_path / "linked-source-output"
    linked = run_generator(
        "--rotate-bootstrap-only",
        str(linked_source),
        "--output-dir",
        str(linked_output),
        check=False,
    )
    assert linked.returncode != 0
    assert not linked_output.exists()

    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    parent_link = run_generator(
        "--rotate-bootstrap-only",
        str(source),
        "--output-dir",
        str(linked_parent / "output"),
        check=False,
    )
    assert parent_link.returncode != 0
    assert not (real_parent / "output").exists()

    overlap = run_generator(
        "--rotate-bootstrap-only",
        str(source),
        "--output-dir",
        str(source / "nested-output"),
        check=False,
    )
    assert overlap.returncode != 0
    assert "must not overlap" in overlap.stderr
    for completed in (linked, parent_link, overlap):
        assert source_secret not in completed.stdout
        assert source_secret not in completed.stderr


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

    protected = tmp_path / "protected"
    protected.write_text("unchanged", encoding="utf-8")
    linked_target = tmp_path / "secret-link"
    linked_target.symlink_to(protected)
    with pytest.raises(FileExistsError):
        write_private_text(linked_target, "symlink replacement forbidden")
    assert protected.read_text(encoding="utf-8") == "unchanged"


def test_private_output_directory_is_fresh_and_mode_0700(tmp_path: Path) -> None:
    output = tmp_path / "private-output"
    create_private_output_dir(output)
    assert output.stat().st_mode & 0o777 == 0o700
    with pytest.raises(FileExistsError):
        create_private_output_dir(output)


def test_overlay_inserts_enabled_only_provider_adapters_idempotently(tmp_path: Path) -> None:
    app_module = tmp_path / "main_part2.py"
    app_module.write_text(
        "from aiohttp import web\n\ndef create_app():\n    app = web.Application()\n    return app\n",
        encoding="utf-8",
    )
    assert patch_main(app_module) is True
    content = app_module.read_text(encoding="utf-8")
    assert "PrivateEventsMCPConfig.from_env()" in content
    assert "if private_mcp_config.enabled:" in content
    assert "build_private_events_mcp_social_adapters(vk_api)" in content
    assert "build_private_events_mcp_workspace_adapters" in content
    assert "social_workspace_adapters=private_mcp_workspace_adapters" in content
    assert content.count("attach_private_events_mcp(") == 1
    assert patch_main(app_module) is False
