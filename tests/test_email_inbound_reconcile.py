from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "infra" / "yandex" / "email-inbound" / "reconcile.py"
SPEC = importlib.util.spec_from_file_location("email_inbound_reconcile", MODULE_PATH)
assert SPEC and SPEC.loader
reconcile = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = reconcile
SPEC.loader.exec_module(reconcile)


def manifest(tmp_path: Path) -> dict:
    source = ROOT / "infra" / "yandex" / "email-inbound" / "desired-state.json"
    path = tmp_path / "desired-state.json"
    path.write_text(source.read_text())
    return reconcile.load_manifest(
        path,
        {
            "EMAIL_INBOUND_YC_CLOUD_ID": "cloud-fixture",
            "EMAIL_INBOUND_BUCKET_NAME": "kenigevents-email-inbound-fixture",
        },
    )


def full_inventory(value: dict) -> dict:
    return {
        "folder": {"id": "folder-fixture", "name": value["folder"]["name"]},
        "service_accounts": [{"name": name} for name in value["service_accounts"]],
        "kms_keys": [{"name": name} for name in value["kms_keys"]],
        "buckets": [{"name": name} for name in value["buckets"]],
        "functions": [
            {"name": name, "status": "ACTIVE"} for name in value["functions"]
        ],
        "lockbox_secrets": [{"name": name} for name in value["lockbox_secrets"]],
        "queues": [{"name": name} for name in value["queues"]],
        "triggers": [
            {"name": name, "status": "ACTIVE"} for name in value["triggers"]
        ],
    }


def test_all_present_plan_is_idempotent(tmp_path: Path) -> None:
    value = manifest(tmp_path)
    actions = reconcile.plan(value, full_inventory(value), yc="yc")
    assert actions
    assert {action.status for action in actions} == {"ready"}


def test_present_but_paused_trigger_is_reported_as_drift(tmp_path: Path) -> None:
    value = manifest(tmp_path)
    inventory = full_inventory(value)
    inventory["triggers"][0]["status"] = "PAUSED"

    actions = reconcile.plan(value, inventory, yc="yc")

    trigger = next(
        action
        for action in actions
        if action.kind == "trigger" and action.name == value["triggers"][0]
    )
    assert trigger.status == "drift"
    assert "PAUSED" in trigger.reason
    assert trigger.command == ()


def test_present_but_inactive_function_is_reported_as_drift(tmp_path: Path) -> None:
    value = manifest(tmp_path)
    inventory = full_inventory(value)
    inventory["functions"][0]["status"] = "STOPPED"

    actions = reconcile.plan(value, inventory, yc="yc")

    function = next(
        action
        for action in actions
        if action.kind == "function" and action.name == value["functions"][0]
    )
    assert function.status == "drift"
    assert "STOPPED" in function.reason
    assert function.command == ()


def test_missing_folder_is_only_first_stage(tmp_path: Path) -> None:
    value = manifest(tmp_path)
    actions = reconcile.plan(value, {"folder": None}, yc="yc")
    assert len(actions) == 1
    assert actions[0].kind == "folder"
    assert actions[0].status == "create"
    assert "--cloud-id" in actions[0].command


def test_missing_resources_keep_secretful_steps_operator_gated(tmp_path: Path) -> None:
    value = manifest(tmp_path)
    inventory = full_inventory(value)
    for collection in inventory:
        if collection != "folder":
            inventory[collection] = []
    actions = reconcile.plan(value, inventory, yc="yc")
    by_kind = {(action.kind, action.name): action for action in actions}
    assert by_kind[("bucket", value["buckets"][0])].status == "create"
    assert by_kind[("queue", value["queues"][0])].status == "operator"
    assert by_kind[("lockbox_secret", value["lockbox_secrets"][0])].status == "operator"
    assert by_kind[("trigger", value["triggers"][0])].status == "operator"


def test_apply_requires_exact_confirmation(tmp_path: Path) -> None:
    value = manifest(tmp_path)
    actions = reconcile.plan(value, {"folder": None}, yc="yc")
    calls = []

    def runner(command):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "{}", "")

    with pytest.raises(reconcile.ReconcileError, match="apply_confirmation_invalid"):
        reconcile.apply_actions(actions, confirmation="yes", runner=runner)
    assert calls == []
    reconcile.apply_actions(
        actions, confirmation=reconcile.CONFIRMATION, runner=runner
    )
    assert len(calls) == 1


def test_inventory_file_does_not_require_live_yc(tmp_path: Path, monkeypatch, capsys) -> None:
    value = manifest(tmp_path)
    manifest_path = tmp_path / "expanded.json"
    manifest_path.write_text(json.dumps(value))
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(json.dumps(full_inventory(value)))
    assert reconcile.main(
        [
            "--manifest",
            str(manifest_path),
            "--inventory-file",
            str(inventory_path),
            "--json",
        ]
    ) == 0
    assert '"status": "ready"' in capsys.readouterr().out
