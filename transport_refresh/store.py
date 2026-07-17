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
PROVIDER_STATUS_SCHEMA_VERSION = "kenigevents.transport_provider_status.v1"
FAN_IN_STATUS_SCHEMA_VERSION = "kenigevents.transport_fan_in_status.v1"
REBUILD_STATE_SCHEMA_VERSION = "kenigevents.transport_rebuild_state.v1"


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8") + b"\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp_name, path)
        # The file fsync above protects its contents. Syncing the containing
        # directory makes the rename durable as well (important on Fly volumes).
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _immutable_json(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise RuntimeError(f"immutable manifest is unreadable: {path}") from exc
        if canonical_json(existing) != canonical_json(value):
            raise RuntimeError(f"immutable manifest collision: {path}")
        return
    _atomic_json(path, value)


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
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else None
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None

    def provider_pointer(self, provider: str) -> dict[str, Any] | None:
        return self._read(self.root / "providers" / provider / "current.json")

    def combined_pointer(self) -> dict[str, Any] | None:
        return self._read(self.root / "combined" / "current.json")

    def _manifest_path(self, relative: str, *, prefix: str) -> Path | None:
        if not relative or Path(relative).is_absolute() or not relative.startswith(prefix):
            return None
        target = (self.root / relative).resolve()
        expected_root = (self.root / prefix).resolve()
        try:
            target.relative_to(expected_root)
        except ValueError:
            return None
        return target

    def _provider_manifest(self, provider: str) -> dict[str, Any] | None:
        pointer = self.provider_pointer(provider)
        if not pointer:
            return None
        manifest_path = self._manifest_path(
            str(pointer.get("manifest_path") or ""),
            prefix=f"providers/{provider}/manifests/",
        )
        if manifest_path is None:
            return None
        manifest = self._read(manifest_path)
        if not manifest:
            return None
        hash_payload = dict(manifest)
        embedded_hash = hash_payload.pop("snapshot_hash", None)
        if embedded_hash != pointer.get("snapshot_hash") or digest(hash_payload) != embedded_hash:
            return None
        return manifest

    def combined_manifest(self) -> dict[str, Any] | None:
        pointer = self.combined_pointer()
        if not pointer:
            return None
        manifest_path = self._manifest_path(
            str(pointer.get("manifest_path") or ""),
            prefix="combined/manifests/",
        )
        if manifest_path is None:
            return None
        manifest = self._read(manifest_path)
        if not manifest or digest(manifest) != pointer.get("snapshot_hash"):
            return None
        if manifest.get("content_hash") != pointer.get("content_hash"):
            return None
        return manifest

    def provider_status(self, provider: str) -> dict[str, Any] | None:
        return self._read(self.root / "providers" / provider / "status.json")

    def fan_in_status(self) -> dict[str, Any] | None:
        return self._read(self.root / "combined" / "status.json")

    def rebuild_state(self) -> dict[str, Any] | None:
        return self._read(self.root / "combined" / "rebuild.json")

    def _mark_rebuild_desired(self, *, combined: dict[str, Any], now: datetime) -> dict[str, Any]:
        previous = self.rebuild_state() or {}
        content_hash = str(combined["content_hash"])
        if previous.get("desired_hash") == content_hash:
            return previous
        state = {
            "schema_version": REBUILD_STATE_SCHEMA_VERSION,
            "desired_hash": content_hash,
            "desired_combined_id": combined["combined_id"],
            "desired_at": now.isoformat(),
            "acknowledged_hash": previous.get("acknowledged_hash"),
            "acknowledged_at": previous.get("acknowledged_at"),
            "status": "pending",
            "payload": {
                "reason": "transport_schedule_changed",
                "transport_combined_id": combined["combined_id"],
                "transport_content_hash": content_hash,
            },
        }
        _atomic_json(self.root / "combined" / "rebuild.json", state)
        return state

    def _enqueue_rebuild_if_needed(
        self,
        *,
        now: datetime,
        enqueue: EnqueueCallback | None,
    ) -> tuple[bool, str | None]:
        state = self.rebuild_state()
        if not state or (
            state.get("status") == "acknowledged"
            and state.get("desired_hash") == state.get("acknowledged_hash")
        ):
            return False, None
        if enqueue is None:
            return False, None
        try:
            enqueue("static_site_build:prod", dict(state.get("payload") or {}))
        except Exception as exc:  # state remains pending and is retried on the next fan-in
            return False, f"{exc.__class__.__name__}: {exc}"
        state.update({
            "status": "acknowledged",
            "acknowledged_hash": state["desired_hash"],
            "acknowledged_at": now.isoformat(),
        })
        _atomic_json(self.root / "combined" / "rebuild.json", state)
        return True, None

    def _write_attempt_status(
        self,
        provider: str,
        *,
        now: datetime,
        attempt_status: str,
        serving_status: str,
        fan_in_status: str,
        reasons: list[str],
        candidate: dict[str, Any] | None,
        last_good: dict[str, Any] | None,
        freshness: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        candidate_hash = digest(candidate) if isinstance(candidate, dict) else None
        attempt_id = digest({
            "provider": provider,
            "checked_at": now.isoformat(),
            "candidate_hash": candidate_hash,
            "attempt_status": attempt_status,
            "reasons": reasons,
        })
        status = {
            "schema_version": PROVIDER_STATUS_SCHEMA_VERSION,
            "attempt_id": attempt_id,
            "provider": provider,
            "checked_at": now.isoformat(),
            "attempt_status": attempt_status,
            "serving_status": serving_status,
            "fan_in_status": fan_in_status,
            "reasons": list(dict.fromkeys(reasons)),
            "candidate_hash": candidate_hash,
            "candidate_snapshot_id": candidate.get("snapshot_id") if isinstance(candidate, dict) else None,
            "last_good": {
                key: last_good.get(key)
                for key in ("snapshot_id", "snapshot_hash", "content_hash", "manifest_path", "fetched_at")
            } if last_good else None,
            "provider_freshness": freshness,
        }
        history = self.root / "providers" / provider / "attempts" / f"{attempt_id}.json"
        _immutable_json(history, status)
        _atomic_json(self.root / "providers" / provider / "status.json", status)

        fan_in = {
            "schema_version": FAN_IN_STATUS_SCHEMA_VERSION,
            "checked_at": now.isoformat(),
            "trigger_provider": provider,
            "status": fan_in_status,
            "provider_freshness": freshness,
            "provider_attempt_id": attempt_id,
        }
        _atomic_json(self.root / "combined" / "status.json", fan_in)
        return status

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
            _immutable_json(target, normalized)
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

        previous_pointer = self.combined_pointer()
        previous = previous_pointer if self.combined_manifest() is not None else None
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
            attempt_status = "fresh" if accepted else self._attempt_status(reasons)
            provider_pointer = self.provider_pointer(provider)
            serving_status = freshness.get(provider, {}).get("status", "missing")
            status_record = self._write_attempt_status(
                provider,
                now=now,
                attempt_status=attempt_status,
                serving_status=serving_status,
                fan_in_status="partial",
                reasons=reasons,
                candidate=candidate,
                last_good=provider_pointer,
                freshness=freshness,
            )
            return {
                "status": "provider_accepted_waiting_for_fan_in" if accepted else "provider_rejected_last_good_preserved",
                "provider": provider,
                "provider_accepted": accepted,
                "published": False,
                "rebuild_enqueued": False,
                "combined_hash": previous.get("content_hash") if previous else None,
                "freshness": freshness,
                "reasons": reasons,
                "attempt_id": status_record["attempt_id"],
                "attempt_status": attempt_status,
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
        _immutable_json(target, combined)
        changed = not previous or previous.get("content_hash") != combined_content_hash
        if changed:
            # Persist intent before moving the public pointer. If SQLite/outbox
            # enqueue then fails, any later unchanged refresh retries the same
            # idempotent coalesce key instead of losing the rebuild forever.
            self._mark_rebuild_desired(combined=combined, now=now)
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
        enqueued, enqueue_error = self._enqueue_rebuild_if_needed(now=now, enqueue=enqueue)
        if enqueue_error:
            reasons.append(f"rebuild_enqueue:{enqueue_error}")
        attempt_status = "fresh" if accepted else self._attempt_status(reasons)
        provider_pointer = self.provider_pointer(provider)
        serving_status = freshness.get(provider, {}).get("status", "missing")
        status_record = self._write_attempt_status(
            provider,
            now=now,
            attempt_status=attempt_status,
            serving_status=serving_status,
            fan_in_status="complete",
            reasons=reasons,
            candidate=candidate,
            last_good=provider_pointer,
            freshness=freshness,
        )
        return {
            "status": (
                "published_changed_rebuild_pending"
                if enqueue_error
                else "published_changed"
                if changed
                else "published_unchanged"
            ),
            "provider": provider,
            "provider_accepted": accepted,
            "published": True,
            "rebuild_enqueued": enqueued,
            "combined_hash": combined_content_hash,
            "combined_snapshot_hash": combined_snapshot_hash,
            "freshness": freshness,
            "reasons": reasons,
            "attempt_id": status_record["attempt_id"],
            "attempt_status": attempt_status,
            "rebuild_pending": bool(
                (self.rebuild_state() or {}).get("status") != "acknowledged"
            ),
        }

    @staticmethod
    def _attempt_status(reasons: list[str]) -> str:
        if any(reason.startswith("freshness:") for reason in reasons):
            return "stale"
        if "services:empty" in reasons:
            return "partial"
        if any(reason.startswith("provider:") for reason in reasons):
            return "failed"
        return "invalid"
