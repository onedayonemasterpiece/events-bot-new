from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from static_site_atomic_root import (
    ROOT_REQUIRED_CHECKS,
    ROOT_REQUIRED_ROUTES,
    ROOT_RECEIPT_SCHEMA,
    CheckedRootRelease,
    RootPublisherConfig,
    RootSlot,
    RoutingState,
    SwitchResult,
    YandexAlbCliAdapter,
    publish_atomic_root_archive,
)
from static_site_release import StaticSiteRetryableError


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _tree(files: list[dict[str, object]]) -> str:
    payload = "".join(
        f"{item['key']}\0{item['sha256']}\0{item['size']}\n"
        for item in sorted(files, key=lambda item: str(item["key"]))
    )
    return _sha(payload.encode())


def _mime(key: str) -> str:
    if key.endswith(".html"):
        return "text/html; charset=utf-8"
    if key.endswith(".webmanifest"):
        return "application/manifest+json; charset=utf-8"
    if key.endswith(".png"):
        return "image/png"
    if key.endswith(".txt"):
        return "text/plain; charset=utf-8"
    if key.endswith(".xml"):
        return "application/xml; charset=utf-8"
    return "application/octet-stream"


def _root_archive(tmp_path: Path, *, build_id: str = "production-atomic-test") -> tuple[Path, dict]:
    root = tmp_path / f"{build_id}-root"
    root.mkdir()
    files: dict[str, bytes] = {
        key: (f"complete:{build_id}:{key}").encode() for key in ROOT_REQUIRED_ROUTES
    }
    pwa = {
        "id": "/",
        "name": "Анонсы",
        "short_name": "Анонсы",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "icons": [
            {"src": "/assets/pwa/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/assets/pwa/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ],
    }
    files["manifest.webmanifest"] = json.dumps(pwa).encode()
    focus_pwa = {
        "id": "/fokus-gruppa/pwa",
        "name": "Анонсы",
        "short_name": "Анонсы",
        "start_url": "/fokus-gruppa/priglashenie/?launch=pwa",
        "scope": "/",
        "display": "standalone",
        "icons": [
            {"src": "/assets/pwa/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/assets/pwa/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ],
    }
    files["fokus-gruppa/manifest.webmanifest"] = json.dumps(focus_pwa).encode()
    files["assets/pwa/icon-192.png"] = b"png-192"
    files["assets/pwa/icon-512.png"] = b"png-512"
    inventory: list[dict[str, object]] = []
    for key, body in files.items():
        path = root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        inventory.append(
            {
                "key": key,
                "sha256": _sha(body),
                "size": len(body),
                "content_type": _mime(key),
                "cache_control": "public, max-age=60, must-revalidate",
            }
        )
    manifest = {
        "schema_version": "static_release_manifest_v1",
        "publication_mode": "artifact_only",
        "site_mode": "production",
        "build_id": build_id,
        "run_id": "static-site:atomic-test:12345678",
        "repo_sha": "a" * 40,
        "base_path": "/",
        "site_origin": "https://kenigevents.ru",
        "hash_algorithm": "sha256",
        "snapshot": {"snapshot_id": "snapshot-atomic", "sha256": "b" * 64},
        "checks": {key: "ok" for key in ROOT_REQUIRED_CHECKS},
        "files": inventory,
        "tree_sha256": _tree(inventory),
    }
    (root / "static-release-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    archive = tmp_path / f"{build_id}.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(root, arcname=root.name)
    result = {
        "build_id": build_id,
        "run_id": manifest["run_id"],
        "repo_sha": manifest["repo_sha"],
        "snapshot": {"snapshot_id": "snapshot-atomic", "snapshot_sha256": "b" * 64},
    }
    return archive, result


def _old_manifest(build_id: str = "production-old") -> bytes:
    body = b"old"
    files = [
        {
            "key": "old-complete.html",
            "sha256": _sha(body),
            "size": len(body),
            "content_type": "text/html; charset=utf-8",
            "cache_control": "public, max-age=60, must-revalidate",
        }
    ]
    return json.dumps(
        {
            "schema_version": "static_release_manifest_v1",
            "site_mode": "production",
            "build_id": build_id,
            "run_id": "static-site:old:12345678",
            "repo_sha": "c" * 40,
            "snapshot": {"snapshot_id": "snapshot-old"},
            "tree_sha256": _tree(files),
            "files": files,
        }
    ).encode()


class MemoryStore:
    def __init__(self):
        self.objects: dict[str, dict[str, tuple[bytes, str, dict[str, str]]]] = {
            "root-blue": {
                "static-release-manifest.json": (
                    _old_manifest(),
                    "application/json; charset=utf-8",
                    {},
                ),
                "old-complete.html": (
                    b"old",
                    "text/html; charset=utf-8",
                    {"sha256": _sha(b"old")},
                ),
            },
            "root-green": {"stale.html": (b"stale", "text/html; charset=utf-8", {})},
        }
        self.writes: list[tuple[str, str]] = []
        self.deletes: list[tuple[str, tuple[str, ...]]] = []
        self.fail_write = False

    def check_bucket(self, bucket: str) -> None:
        assert bucket in self.objects

    def list_keys(self, bucket: str) -> list[str]:
        return sorted(self.objects[bucket])

    def read(self, bucket: str, key: str):
        return self.objects[bucket][key]

    def stat(self, bucket: str, key: str):
        body, content_type, metadata = self.objects[bucket][key]
        return len(body), content_type, metadata

    def write(self, bucket: str, item):
        self.writes.append((bucket, item["key"]))
        if self.fail_write:
            raise StaticSiteRetryableError("injected_write_failure")
        body = Path(item["local_path"]).read_bytes()
        self.objects[bucket][item["key"]] = (
            body,
            item["content_type"],
            {"sha256": item["sha256"]},
        )

    def delete(self, bucket: str, keys):
        self.deletes.append((bucket, tuple(keys)))
        for key in keys:
            self.objects[bucket].pop(key, None)


class MemoryControl:
    def __init__(self):
        self.weights = {"root-blue": 100, "root-green": 0}
        self.buckets = {"root-blue": "root-blue", "root-green": "root-green"}
        self.calls: list[tuple[str, str]] = []
        self.weight_history: list[dict[str, int]] = [dict(self.weights)]

    def inspect(self):
        encoded = json.dumps(self.weights, sort_keys=True).encode()
        return RoutingState(dict(self.weights), dict(self.buckets), _sha(encoded))

    def converge(self, *, target_backend: str, source_backend: str, active_weight: int):
        self.calls.append((target_backend, source_backend))
        operations = []
        if self.weights[target_backend] != active_weight:
            self.weights[target_backend] = active_weight
            operations.append(f"op-{len(self.calls)}-enable")
            self.weight_history.append(dict(self.weights))
        if self.weights[source_backend] != 0:
            self.weights[source_backend] = 0
            operations.append(f"op-{len(self.calls)}-disable")
            self.weight_history.append(dict(self.weights))
        return SwitchResult(tuple(operations), self.inspect())


class Smoke:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = 0

    def verify(self, *, release: CheckedRootRelease, nonce: str) -> None:
        self.calls += 1
        assert nonce == release.manifest_sha256[:16]
        if self.fail:
            raise StaticSiteRetryableError("injected_smoke_failure")


def _config(tmp_path: Path, *, mode: str = "apply") -> RootPublisherConfig:
    return RootPublisherConfig(
        mode=mode,
        blue=RootSlot("blue", "root-blue", "root-blue"),
        green=RootSlot("green", "root-green", "root-green"),
        shared_asset_bucket="media-and-ics",
        backend_group_id="backend-group",
        folder_id="folder",
        public_base_url="https://kenigevents.ru",
        receipt_path=tmp_path / "receipt.json",
        extraction_root=tmp_path / "extract",
        yc_cli="yc",
        endpoint="https://storage.yandexcloud.net",
        region="ru-central1",
        access_key_id="key",
        secret_access_key="secret",
    )


def _publish(tmp_path: Path, store: MemoryStore, control: MemoryControl, smoke: Smoke, *, mode="apply"):
    archive, result = _root_archive(tmp_path)
    return publish_atomic_root_archive(
        archive,
        build_result=result,
        config=_config(tmp_path, mode=mode),
        control=control,
        store=store,
        smoke=smoke,
    )


def test_active_bucket_is_never_written_and_stale_cleanup_is_inactive_only(tmp_path: Path) -> None:
    store, control = MemoryStore(), MemoryControl()

    receipt = _publish(tmp_path, store, control, Smoke())

    assert receipt["status"] == "promoted"
    assert {bucket for bucket, _key in store.writes} == {"root-green"}
    assert store.deletes == [("root-green", ("stale.html",))]
    assert receipt["cleanup"]["slot"] == "green"
    assert receipt["operation_id"] == "op-1-disable"
    assert receipt["operation_ids"] == ["op-1-enable", "op-1-disable"]
    assert len(receipt["revision"]) == 64
    assert receipt["current"]["build_id"] == "production-atomic-test"
    assert receipt["previous"]["build_id"] == "production-old"


def test_failure_before_switch_preserves_active(tmp_path: Path) -> None:
    store, control = MemoryStore(), MemoryControl()
    store.fail_write = True

    with pytest.raises(StaticSiteRetryableError, match="injected_write_failure"):
        _publish(tmp_path, store, control, Smoke())

    assert control.weights == {"root-blue": 100, "root-green": 0}
    assert control.calls == []
    assert not any(bucket == "root-blue" for bucket, _key in store.writes)


def test_weight_convergence_can_serve_only_two_complete_trees(tmp_path: Path) -> None:
    store, control = MemoryStore(), MemoryControl()

    _publish(tmp_path, store, control, Smoke())

    # The target was complete before it received weight.  During overlap both
    # complete roots may be served; neither history entry points at a partial tree.
    assert control.weight_history == [
        {"root-blue": 100, "root-green": 0},
        {"root-blue": 100, "root-green": 100},
        {"root-blue": 0, "root-green": 100},
    ]
    assert "static-release-manifest.json" in store.objects["root-blue"]
    assert "static-release-manifest.json" in store.objects["root-green"]


def test_idempotent_retry_does_not_rewrite_or_reswitch(tmp_path: Path) -> None:
    store, control = MemoryStore(), MemoryControl()
    first = _publish(tmp_path, store, control, Smoke())
    write_count = len(store.writes)
    switch_count = len(control.calls)

    second_dir = tmp_path / "retry"
    second_dir.mkdir()
    second = _publish(second_dir, store, control, Smoke())

    assert first["status"] == "promoted"
    assert second["status"] == "noop"
    assert len(store.writes) == write_count
    assert len(control.calls) == switch_count
    assert second["operation_ids"] == []


def test_stable_smoke_failure_rolls_back_and_records_both_roots(tmp_path: Path) -> None:
    store, control = MemoryStore(), MemoryControl()

    receipt = _publish(tmp_path, store, control, Smoke(fail=True))

    assert receipt["schema_version"] == ROOT_RECEIPT_SCHEMA
    assert receipt["status"] == "rolled_back"
    assert control.weights == {"root-blue": 100, "root-green": 0}
    assert control.calls == [("root-green", "root-blue"), ("root-blue", "root-green")]
    assert receipt["current"]["bucket"] == "root-blue"
    assert receipt["previous"]["bucket"] == "root-green"
    assert receipt["previous"]["healthy"] is False


def test_current_and_previous_complete_roots_are_never_gc_targets(tmp_path: Path) -> None:
    store, control = MemoryStore(), MemoryControl()

    receipt = _publish(tmp_path, store, control, Smoke())

    assert receipt["current"]["bucket"] == "root-green"
    assert receipt["previous"]["bucket"] == "root-blue"
    assert store.objects["root-blue"]  # previous known-good tree retained
    assert all(bucket != "root-blue" for bucket, _keys in store.deletes)


def test_plan_mode_is_inventory_only_and_writes_receipt(tmp_path: Path) -> None:
    store, control = MemoryStore(), MemoryControl()

    receipt = _publish(tmp_path, store, control, Smoke(), mode="plan")

    assert receipt["status"] == "planned"
    assert store.writes == []
    assert store.deletes == []
    assert control.calls == []
    assert json.loads((tmp_path / "receipt.json").read_text())["status"] == "planned"


def test_disabled_mode_is_default_off_and_main_calls_only_under_flag(monkeypatch) -> None:
    monkeypatch.delenv("ENABLE_STATIC_SITE_ROOT_PROMOTION", raising=False)
    import main

    assert main._env_flag("ENABLE_STATIC_SITE_ROOT_PROMOTION") is False
    source = Path(main.__file__).read_text(encoding="utf-8")
    guarded = source.index('if _env_flag("ENABLE_STATIC_SITE_ROOT_PROMOTION")')
    publisher = source.index("publish_atomic_root_from_env", guarded)
    assert publisher > guarded


def test_yandex_alb_adapter_enables_complete_target_before_disabling_source(tmp_path: Path) -> None:
    config = _config(tmp_path)
    weights = {"root-blue": 100, "root-green": 0}
    commands: list[list[str]] = []
    operation_counter = 0

    def runner(command, **_kwargs):
        nonlocal operation_counter
        commands.append(command)
        if "update-http-backend" in command:
            backend = command[command.index("--name") + 1]
            weight = int(command[command.index("--weight") + 1])
            weights[backend] = weight
            operation_counter += 1
            payload = {"id": f"yc-operation-{operation_counter}"}
        elif "operation" in command and "wait" in command:
            payload = {"done": True}
        else:
            payload = {
                "http": {
                    "backends": [
                        {
                            "name": name,
                            "backend_weight": str(weight),
                            "storage_bucket": {"bucket": name},
                        }
                        for name, weight in weights.items()
                    ]
                }
            }
        return SimpleNamespace(stdout=json.dumps(payload))

    adapter = YandexAlbCliAdapter(config, runner=runner)
    switched = adapter.converge(
        target_backend="root-green",
        source_backend="root-blue",
        active_weight=100,
    )

    updates = [command for command in commands if "update-http-backend" in command]
    assert updates[0][updates[0].index("--name") + 1 : updates[0].index("--weight") + 2] == [
        "root-green",
        "--weight",
        "100",
    ]
    assert updates[1][updates[1].index("--name") + 1 : updates[1].index("--weight") + 2] == [
        "root-blue",
        "--weight",
        "0",
    ]
    assert switched.operation_ids == ("yc-operation-1", "yc-operation-2")
    assert switched.state.weights == {"root-blue": 0, "root-green": 100}
