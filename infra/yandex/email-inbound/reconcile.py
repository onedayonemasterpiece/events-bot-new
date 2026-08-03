#!/usr/bin/env python3
"""Conservative, plan-by-default desired-state reconciler for email ingress.

It creates only empty resource shells that do not contain credentials or invoke
production workloads. Queue creation, secret payloads, IAM bindings, function
versions and triggers remain explicit operator gates documented in README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA = "kenigevents.email_inbound.yc_desired_state.v1"
CONFIRMATION = "APPLY-kenigevents-email-prod"
_ENV_RE = re.compile(r"^\$\{([A-Z][A-Z0-9_]*)\}$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{2,62}$")


class ReconcileError(RuntimeError):
    pass


@dataclass(frozen=True)
class Action:
    kind: str
    name: str
    status: str
    reason: str
    command: tuple[str, ...] = ()


Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _expand(value: Any, env: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        match = _ENV_RE.fullmatch(value)
        if not match:
            return value
        resolved = str(env.get(match.group(1)) or "").strip()
        if not resolved:
            raise ReconcileError(f"missing_env:{match.group(1).lower()}")
        return resolved
    if isinstance(value, list):
        return [_expand(item, env) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item, env) for key, item in value.items()}
    return value


def load_manifest(path: Path, env: Mapping[str, str]) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReconcileError("manifest_invalid") from exc
    manifest = _expand(raw, env)
    if manifest.get("schema") != SCHEMA:
        raise ReconcileError("manifest_schema_invalid")
    cloud_id = str(manifest.get("cloud_id") or "")
    if not cloud_id or len(cloud_id) > 50:
        raise ReconcileError("cloud_id_invalid")
    folder = manifest.get("folder")
    if not isinstance(folder, dict) or not _NAME_RE.fullmatch(
        str(folder.get("name") or "")
    ):
        raise ReconcileError("folder_invalid")
    for collection in (
        "service_accounts",
        "kms_keys",
        "buckets",
        "functions",
        "lockbox_secrets",
        "queues",
        "triggers",
    ):
        values = manifest.get(collection)
        if not isinstance(values, list) or not values:
            raise ReconcileError(f"manifest_collection_invalid:{collection}")
        if len(values) != len(set(values)):
            raise ReconcileError(f"manifest_duplicate:{collection}")
        if not all(_NAME_RE.fullmatch(str(name)) for name in values):
            raise ReconcileError(f"manifest_name_invalid:{collection}")
    return manifest


def _yc_json(
    runner: Runner, yc: str, arguments: Sequence[str]
) -> Any:
    command = [yc, *arguments, "--format", "json"]
    result = runner(command)
    if result.returncode != 0:
        raise ReconcileError("yc_read_failed:" + ":".join(arguments[:3]))
    try:
        return json.loads(result.stdout or "null")
    except json.JSONDecodeError as exc:
        raise ReconcileError("yc_json_invalid:" + ":".join(arguments[:3])) from exc


def collect_inventory(
    manifest: Mapping[str, Any], *, yc: str, runner: Runner = default_runner
) -> dict[str, Any]:
    cloud_id = manifest["cloud_id"]
    folder_name = manifest["folder"]["name"]
    folders = _yc_json(
        runner,
        yc,
        [
            "resource-manager",
            "folder",
            "list",
            "--cloud-id",
            cloud_id,
        ],
    )
    folder = next(
        (
            row
            for row in folders
            if isinstance(row, Mapping) and row.get("name") == folder_name
        ),
        None,
    ) if isinstance(folders, list) else None
    if not isinstance(folder, dict) or not folder.get("id"):
        return {"folder": None}
    folder_id = str(folder["id"])
    commands = {
        "service_accounts": ["iam", "service-account", "list"],
        "kms_keys": ["kms", "symmetric-key", "list"],
        "buckets": ["storage", "bucket", "list"],
        "functions": ["serverless", "function", "list"],
        "lockbox_secrets": ["lockbox", "secret", "list"],
        "triggers": ["serverless", "trigger", "list"],
    }
    inventory: dict[str, Any] = {"folder": {"id": folder_id, "name": folder_name}}
    for collection, command in commands.items():
        rows = _yc_json(runner, yc, [*command, "--folder-id", folder_id])
        inventory[collection] = rows if isinstance(rows, list) else []
    # The installed yc CLI has no YMQ data-plane group. Queue inventory must be
    # supplied by an operator-generated, redacted inventory file.
    inventory["queues"] = None
    return inventory


def load_inventory(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReconcileError("inventory_invalid") from exc
    if not isinstance(value, dict):
        raise ReconcileError("inventory_invalid")
    return value


def _resources_by_name(
    inventory: Mapping[str, Any], collection: str
) -> dict[str, Mapping[str, Any]] | None:
    rows = inventory.get(collection)
    if rows is None:
        return None
    if not isinstance(rows, list):
        raise ReconcileError(f"inventory_collection_invalid:{collection}")
    resources: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if isinstance(row, str):
            resources[row] = {"name": row}
        elif isinstance(row, Mapping) and row.get("name"):
            resources[str(row["name"])] = row
    return resources


def plan(
    manifest: Mapping[str, Any], inventory: Mapping[str, Any], *, yc: str
) -> list[Action]:
    cloud_id = manifest["cloud_id"]
    folder_spec = manifest["folder"]
    folder = inventory.get("folder")
    if not isinstance(folder, Mapping) or not folder.get("id"):
        return [
            Action(
                "folder",
                folder_spec["name"],
                "create",
                "isolated email folder is absent; apply only this action, then re-plan",
                (
                    yc,
                    "resource-manager",
                    "folder",
                    "create",
                    "--name",
                    folder_spec["name"],
                    "--description",
                    folder_spec["description"],
                    "--cloud-id",
                    cloud_id,
                ),
            )
        ]
    folder_id = str(folder["id"])
    actions: list[Action] = [
        Action("folder", folder_spec["name"], "ready", "folder exists")
    ]
    create_templates: dict[str, tuple[str, ...]] = {
        "service_accounts": (yc, "iam", "service-account", "create", "--name"),
        "kms_keys": (
            yc,
            "kms",
            "symmetric-key",
            "create",
            "--name",
        ),
        "buckets": (yc, "storage", "bucket", "create", "--name"),
        "functions": (yc, "serverless", "function", "create", "--name"),
    }
    kind_names = {
        "service_accounts": "service_account",
        "kms_keys": "kms_key",
        "buckets": "bucket",
        "functions": "function",
        "lockbox_secrets": "lockbox_secret",
        "queues": "queue",
        "triggers": "trigger",
    }
    for collection, kind in kind_names.items():
        observed = _resources_by_name(inventory, collection)
        for name in manifest[collection]:
            if observed is not None and name in observed:
                if collection in {"functions", "triggers"}:
                    runtime_status = str(observed[name].get("status") or "UNKNOWN").upper()
                    if runtime_status != "ACTIVE":
                        actions.append(
                            Action(
                                kind,
                                name,
                                "drift",
                                f"resource exists but runtime status is {runtime_status}; "
                                "manual recovery and acceptance checks are required",
                            )
                        )
                        continue
                actions.append(Action(kind, name, "ready", "resource exists"))
                continue
            if collection in create_templates:
                command = (*create_templates[collection], name, "--folder-id", folder_id)
                if collection == "kms_keys":
                    command += (
                        "--default-algorithm",
                        "aes-256",
                        "--rotation-period",
                        "8760h",
                        "--deletion-protection",
                    )
                actions.append(Action(kind, name, "create", "resource shell is absent", command))
            else:
                reason = {
                    "lockbox_secrets": "secret payload/version is an explicit operator gate",
                    "queues": "YMQ requires AWS-compatible credentials and redrive configuration",
                    "triggers": "trigger creation waits for function versions, queue ARNs and IAM bindings",
                }[collection]
                actions.append(Action(kind, name, "operator", reason))
    return actions


def apply_actions(
    actions: Sequence[Action], *, confirmation: str, runner: Runner = default_runner
) -> None:
    if confirmation != CONFIRMATION:
        raise ReconcileError("apply_confirmation_invalid")
    for action in actions:
        if action.status != "create" or not action.command:
            continue
        result = runner(action.command)
        if result.returncode != 0:
            raise ReconcileError(f"yc_apply_failed:{action.kind}:{action.name}")


def _print(actions: Sequence[Action], *, json_output: bool) -> None:
    if json_output:
        print(json.dumps([asdict(action) for action in actions], indent=2))
        return
    for action in actions:
        print(f"{action.status:8} {action.kind:18} {action.name} — {action.reason}")
        if action.command:
            print("         command:", " ".join(action.command))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=here / "desired-state.json")
    parser.add_argument("--inventory-file", type=Path)
    parser.add_argument("--yc", default=os.getenv("YC_CLI", "/home/dev/yandex-cloud/bin/yc"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        manifest = load_manifest(args.manifest, os.environ)
        inventory = (
            load_inventory(args.inventory_file)
            if args.inventory_file
            else collect_inventory(manifest, yc=args.yc)
        )
        actions = plan(manifest, inventory, yc=args.yc)
        _print(actions, json_output=args.json)
        if args.apply:
            apply_actions(actions, confirmation=args.confirm)
            print("Apply stage completed. Re-run without --apply to refresh the plan.")
        return 0
    except ReconcileError as exc:
        print(f"reconcile_error:{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
