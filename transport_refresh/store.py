from __future__ import annotations

import fcntl
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .schema import (
    COMBINED_SCHEMA_VERSION,
    POINTER_SCHEMA_VERSION,
    PROVIDERS,
    ManifestValidationError,
    canonical_json,
    digest,
    semantic_provider_payload,
    validate_provider_manifest,
)

EnqueueCallback = Callable[[str, dict[str, Any]], Any]


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8") + b"\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


class TransportManifestStore:
    """Filesystem-backed immutable snapshots plus an atomic current pointer.

    The root is suitable for a Fly volume or a staging directory synchronized
    to Object Storage. All mutation is serialized locally. Static rebuild
    deduplication additionally uses the existing outbox coalesce key passed to
    ``enqueue``.
    """

    def __init__(self, root: str | Path, *, max_age_hours: int = 72):
        self.root = Path(root)
        self.max_age_hours = max_age_hours
        self.root.mkdir(parents=True, exist_ok=True)

    def _read(self, path: Path) -> dict[str, Any] | None:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None

    def provider_pointer(self, provider: str) -> dict[str, Any] | None:
        return self._read(self.root / "providers" / provider / "current.json")

    def combined_pointer(self) -> dict[str, Any] | None:
        return self._read(self.root / "combined" / "current.json")

    def _provider_manifest(self, provider: str) -> dict[str, Any] | None:
        pointer = self.provider_pointer(provider)
        if not pointer:
            return None
        return self._read(self.root / str(pointer["manifest_path"]))

    def publish(
        self,
        provider: str,
        candidate: dict[str, Any] | None,
        *,
        now: datetime | None = None,
        failure_reason: str | None = None,
        enqueue: EnqueueCallback | None = None,
    ) -> dict[str, Any]:
        if provider not in PROVIDERS:
            raise ValueError(f"unsupported provider: {provider}")
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        lock_path = self.root / ".transport-refresh.lock"
        with lock_path.open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            return self._publish_locked(provider, candidate, now=now, failure_reason=failure_reason, enqueue=enqueue)

    def _publish_locked(
        self,
        provider: str,
        candidate: dict[str, Any] | None,
        *,
        now: datetime,
        failure_reason: str | None,
        enqueue: EnqueueCallback | None,
    ) -> dict[str, Any]:
        accepted = False
        reasons: list[str] = []
        normalized = None
        if candidate is None:
            reasons.append(f"provider:{failure_reason or 'no_result'}")
        else:
            try:
                normalized = validate_provider_manifest(
                    candidate,
                    expected_provider=provider,
                    now=now,
                    max_age_hours=self.max_age_hours,
                )
            except ManifestValidationError as exc:
                reasons.extend(exc.reasons)
        if normalized is not None:
            snapshot_hash = normalized["snapshot_hash"]
            relative = Path("providers") / provider / "manifests" / f"{snapshot_hash}.json"
            target = self.root / relative
            if not target.exists():
                _atomic_json(target, normalized)
            pointer = {
                "schema_version": POINTER_SCHEMA_VERSION,
                "provider": provider,
                "snapshot_id": normalized["snapshot_id"],
                "snapshot_hash": snapshot_hash,
                "content_hash": normalized["content_hash"],
                "manifest_path": relative.as_posix(),
                "fetched_at": normalized["fetched_at"],
                "checked_at": now.isoformat(),
                "freshness": {"status": "fresh", "reasons": []},
            }
            _atomic_json(self.root / "providers" / provider / "current.json", pointer)
            accepted = True

        providers: dict[str, dict[str, Any]] = {}
        freshness: dict[str, dict[str, Any]] = {}
        for required in PROVIDERS:
            manifest = self._provider_manifest(required)
            if manifest is None:
                freshness[required] = {"status": "missing", "reasons": ["last_good:missing"]}
                continue
            try:
                providers[required] = validate_provider_manifest(
                    manifest,
                    expected_provider=required,
                    now=now,
                    max_age_hours=self.max_age_hours,
                )
                freshness[required] = {"status": "fresh", "reasons": []}
            except ManifestValidationError as exc:
                freshness[required] = {"status": "stale", "reasons": list(exc.reasons)}

        previous = self.combined_pointer()
        if not accepted and provider in providers:
            freshness[provider] = {
                "status": "last_good",
                "reasons": list(reasons),
            }
        if len(providers) != len(PROVIDERS):
            if not accepted:
                state = freshness.get(provider) or {"status": "missing", "reasons": []}
                state["reasons"] = list(dict.fromkeys([*state.get("reasons", []), *reasons]))
                freshness[provider] = state
            return {
                "status": "provider_accepted_waiting_for_fan_in" if accepted else "provider_rejected_last_good_preserved",
                "provider": provider,
                "provider_accepted": accepted,
                "published": False,
                "rebuild_enqueued": False,
                "combined_hash": previous.get("content_hash") if previous else None,
                "freshness": freshness,
                "reasons": reasons,
            }

        semantic = {
            "schema_version": COMBINED_SCHEMA_VERSION,
            "providers": {name: semantic_provider_payload(providers[name]) for name in sorted(providers)},
        }
        combined_content_hash = digest(semantic)
        combined = {
            **semantic,
            "combined_id": f"transport-{combined_content_hash[:20]}",
            "content_hash": combined_content_hash,
            "provider_snapshots": {
                name: {
                    "snapshot_id": providers[name]["snapshot_id"],
                    "snapshot_hash": providers[name]["snapshot_hash"],
                    "content_hash": providers[name]["content_hash"],
                    "fetched_at": providers[name]["fetched_at"],
                }
                for name in sorted(providers)
            },
            "services": sorted(
                [dict(service, provider=name) for name in sorted(providers) for service in providers[name]["services"]],
                key=lambda item: (str(item.get("service_date")), str(item.get("service_id"))),
            ),
        }
        combined_snapshot_hash = digest(combined)
        relative = Path("combined") / "manifests" / f"{combined_snapshot_hash}.json"
        target = self.root / relative
        if not target.exists():
            _atomic_json(target, combined)
        changed = not previous or previous.get("content_hash") != combined_content_hash
        pointer = {
            "schema_version": POINTER_SCHEMA_VERSION,
            "combined_id": combined["combined_id"],
            "snapshot_hash": combined_snapshot_hash,
            "content_hash": combined_content_hash,
            "manifest_path": relative.as_posix(),
            "checked_at": now.isoformat(),
            "freshness": freshness,
        }
        _atomic_json(self.root / "combined" / "current.json", pointer)
        enqueued = False
        if changed and enqueue is not None:
            enqueue("static_site_build:prod", {
                "reason": "transport_schedule_changed",
                "transport_combined_id": combined["combined_id"],
                "transport_content_hash": combined_content_hash,
            })
            enqueued = True
        return {
            "status": "published_changed" if changed else "published_unchanged",
            "provider": provider,
            "provider_accepted": accepted,
            "published": True,
            "rebuild_enqueued": enqueued,
            "combined_hash": combined_content_hash,
            "combined_snapshot_hash": combined_snapshot_hash,
            "freshness": freshness,
            "reasons": reasons,
        }
