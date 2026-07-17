"""Durable request metadata and immutable SQLite snapshots for static pages.

This module deliberately has no dependency on ``main``/SQLModel so it can be
used by the outbox worker, operator scripts and focused tests without importing
the bot.  The outbox row remains the durable scheduling record; its bounded JSON
payload carries the effect watermark and correlation evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


REQUEST_SCHEMA = "static_site_build_request_v1"
SNAPSHOT_SCHEMA = "static_site_sqlite_snapshot_v1"
RELEASE_CHANNEL_SECRET = "secret_preview"
MAX_REASONS = 24
MAX_EVENT_IDS = 256
MAX_CORRELATIONS = 128
MAX_EVENT_REVISIONS = 256


class StaticSiteReleaseError(RuntimeError):
    """Base error whose failure class is safe to persist in outbox evidence."""


class StaticSiteRetryableError(StaticSiteReleaseError):
    """A transient external/dependency failure that may be retried."""


class StaticSitePermanentError(StaticSiteReleaseError):
    """An invalid immutable input/result that must not be retried unchanged."""


@dataclass(frozen=True)
class FailureDisposition:
    failure_class: str
    retryable: bool


@dataclass(frozen=True)
class SnapshotMetadata:
    schema_version: str
    snapshot_id: str
    source_database_name: str
    sqlite_filename: str
    created_at: str
    quick_check: str
    sha256: str
    size_bytes: int
    target_watermark: str
    latest_effect_at: str | None
    max_event_id: int | None
    max_event_updated_at: str | None
    max_event_revision: str | None
    event_revisions: dict[str, str]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | str | None = None) -> str:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = value or utc_now()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_token(value: Any, *, max_len: int = 200) -> str:
    return " ".join(str(value or "").strip().split())[:max_len]


def _bounded_unique(values: Iterable[Any], *, limit: int, max_len: int = 200) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = _clean_token(raw, max_len=max_len)
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def event_public_revision(event: Any) -> str:
    """Return a deterministic revision hash for fields consumed by static pages.

    Smart Update remains the semantic owner.  This hash is correlation evidence,
    not a second semantic gate.  It intentionally excludes publication URLs and
    counters whose refresh does not require a full page rebuild.
    """

    fields = (
        "id",
        "title",
        "description",
        "short_description",
        "search_digest",
        "festival",
        "date",
        "end_date",
        "time",
        "time_is_default",
        "location_name",
        "location_address",
        "city",
        "ticket_price_min",
        "ticket_price_max",
        "ticket_link",
        "ticket_status",
        "event_type",
        "is_free",
        "pushkin_card",
        "silent",
        "lifecycle_status",
        "identity_status",
        "merged_into_event_id",
        "age_restriction",
        "age_restriction_status",
        "age_restriction_decision_version",
        "age_restriction_input_hash",
        "age_assessment",
        "age_assessment_status",
        "age_assessment_decision_version",
        "age_assessment_input_hash",
        "linked_event_ids",
        "photo_urls",
        "photo_count",
        "topics",
    )
    payload = {name: getattr(event, name, None) for name in fields}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _watermark_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "latest_effect_at": payload.get("latest_effect_at"),
        "event_ids": payload.get("event_ids") or [],
        "event_revisions": payload.get("event_revisions") or {},
        "correlation_ids": payload.get("correlation_ids") or [],
        "reasons": payload.get("reasons") or [],
        "release_channel": payload.get("release_channel") or RELEASE_CHANNEL_SECRET,
    }


def request_watermark(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(_watermark_payload(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def make_request_payload(
    *,
    reason: str,
    event_ids: Iterable[int] = (),
    event_revisions: Mapping[int | str, str] | None = None,
    correlation_id: str | None = None,
    effect_at: datetime | str | None = None,
    trigger: str = "smart_update",
    release_channel: str = RELEASE_CHANNEL_SECRET,
    require_vector_barrier: bool = False,
    expected_search_v3_hash: str | None = None,
    expected_related_v1_hash: str | None = None,
) -> dict[str, Any]:
    valid_ids: set[int] = set()
    for value in event_ids:
        try:
            event_id = int(value)
        except (TypeError, ValueError):
            continue
        if event_id > 0:
            valid_ids.add(event_id)
    ids = sorted(valid_ids)[:MAX_EVENT_IDS]
    revisions: dict[str, str] = {}
    for key, value in (event_revisions or {}).items():
        try:
            event_id = int(key)
        except (TypeError, ValueError):
            continue
        revision = _clean_token(value, max_len=128)
        if event_id > 0 and revision:
            revisions[str(event_id)] = revision
        if len(revisions) >= MAX_EVENT_REVISIONS:
            break
    correlation = _clean_token(correlation_id, max_len=200) or uuid.uuid4().hex
    payload: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "release_channel": RELEASE_CHANNEL_SECRET if release_channel != RELEASE_CHANNEL_SECRET else release_channel,
        "trigger": _clean_token(trigger, max_len=64) or "unknown",
        "reasons": _bounded_unique([reason], limit=MAX_REASONS),
        "event_ids": ids,
        "event_revisions": revisions,
        "correlation_ids": [correlation],
        "latest_effect_at": iso_utc(effect_at),
        "vector_barrier": {
            "required": bool(require_vector_barrier),
            "expected_search_v3_hash": _clean_token(expected_search_v3_hash, max_len=128) or None,
            "expected_related_v1_hash": _clean_token(expected_related_v1_hash, max_len=128) or None,
        },
    }
    payload["target_watermark"] = request_watermark(payload)
    return payload


def merge_request_payload(current: Mapping[str, Any] | None, incoming: Mapping[str, Any] | None) -> dict[str, Any]:
    """Union effect evidence without allowing unbounded outbox JSON growth."""

    left = dict(current or {})
    right = dict(incoming or {})
    if not left:
        merged = right
    elif not right:
        merged = left
    else:
        merged = dict(left)
        merged["schema_version"] = REQUEST_SCHEMA
        merged["release_channel"] = RELEASE_CHANNEL_SECRET
        merged["trigger"] = right.get("trigger") or left.get("trigger") or "unknown"
        merged["reasons"] = _bounded_unique(
            [*(left.get("reasons") or []), *(right.get("reasons") or [])], limit=MAX_REASONS
        )
        merged["event_ids"] = sorted(
            {
                int(value)
                for value in [*(left.get("event_ids") or []), *(right.get("event_ids") or [])]
                if str(value).isdigit() and int(value) > 0
            }
        )[:MAX_EVENT_IDS]
        revisions: dict[str, str] = {}
        for source in (left.get("event_revisions") or {}, right.get("event_revisions") or {}):
            if not isinstance(source, Mapping):
                continue
            for key, value in source.items():
                if str(key).isdigit() and _clean_token(value, max_len=128):
                    revisions[str(int(key))] = _clean_token(value, max_len=128)
        merged["event_revisions"] = dict(list(sorted(revisions.items(), key=lambda item: int(item[0])))[:MAX_EVENT_REVISIONS])
        merged["correlation_ids"] = _bounded_unique(
            [*(left.get("correlation_ids") or []), *(right.get("correlation_ids") or [])],
            limit=MAX_CORRELATIONS,
        )
        effect_values = [value for value in (left.get("latest_effect_at"), right.get("latest_effect_at")) if value]
        merged["latest_effect_at"] = max((iso_utc(value) for value in effect_values), default=iso_utc())
        left_barrier = left.get("vector_barrier") if isinstance(left.get("vector_barrier"), Mapping) else {}
        right_barrier = right.get("vector_barrier") if isinstance(right.get("vector_barrier"), Mapping) else {}
        merged["vector_barrier"] = {
            "required": bool(left_barrier.get("required") or right_barrier.get("required")),
            "expected_search_v3_hash": right_barrier.get("expected_search_v3_hash") or left_barrier.get("expected_search_v3_hash"),
            "expected_related_v1_hash": right_barrier.get("expected_related_v1_hash") or left_barrier.get("expected_related_v1_hash"),
        }
    merged.setdefault("schema_version", REQUEST_SCHEMA)
    merged["release_channel"] = RELEASE_CHANNEL_SECRET
    merged.setdefault("reasons", [])
    merged.setdefault("event_ids", [])
    merged.setdefault("event_revisions", {})
    merged.setdefault("correlation_ids", [])
    merged.setdefault("latest_effect_at", iso_utc())
    merged["target_watermark"] = request_watermark(merged)
    return merged


def _readonly_sqlite_connection(path: Path) -> sqlite3.Connection:
    # Path.as_uri cannot be used for relative paths and quoting by hand is easy
    # to get wrong.  Resolve first; SQLite accepts the POSIX file URI directly.
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=60.0)


def _quick_check(connection: sqlite3.Connection) -> str:
    rows = [str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()]
    if rows != ["ok"]:
        raise StaticSitePermanentError("snapshot_quick_check_failed:" + " | ".join(rows[:10]))
    return "ok"


def _event_snapshot_facts(connection: sqlite3.Connection) -> tuple[int | None, str | None]:
    table = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='event'").fetchone()
    if not table:
        return None, None
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(event)").fetchall()}
    max_event_id = connection.execute("SELECT max(id) FROM event").fetchone()[0] if "id" in columns else None
    timestamps = [
        name
        for name in (
            "updated_at",
            "added_at",
            "age_restriction_updated_at",
            "age_assessment_updated_at",
            "ics_updated_at",
        )
        if name in columns
    ]
    max_updated: str | None = None
    for name in timestamps:
        value = connection.execute(f'SELECT max("{name}") FROM event').fetchone()[0]
        if value is not None:
            clean = str(value)
            max_updated = clean if max_updated is None else max(max_updated, clean)
    return (int(max_event_id) if max_event_id is not None else None), max_updated


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def create_immutable_snapshot(
    source_db: str | os.PathLike[str],
    snapshot_dir: str | os.PathLike[str],
    *,
    request_payload: Mapping[str, Any],
    snapshot_id: str | None = None,
    now: datetime | None = None,
) -> tuple[Path, Path, SnapshotMetadata]:
    """Create one consistent online-backup snapshot and hash-bound manifest."""

    source = Path(source_db).resolve()
    if not source.is_file():
        raise StaticSitePermanentError(f"snapshot_source_missing:{source.name}")
    target_dir = Path(snapshot_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    created_at = iso_utc(now)
    clean_id = _clean_token(snapshot_id, max_len=100) or f"snapshot-{uuid.uuid4().hex}"
    if not all(ch.isalnum() or ch in "._-" for ch in clean_id):
        raise StaticSitePermanentError("invalid_snapshot_id")
    final_path = target_dir / f"{clean_id}.sqlite"
    manifest_path = target_dir / f"{clean_id}.manifest.json"
    if final_path.exists() or manifest_path.exists():
        raise StaticSitePermanentError(f"immutable_snapshot_exists:{clean_id}")

    fd, tmp_name = tempfile.mkstemp(prefix=f".{clean_id}.", suffix=".tmp", dir=target_dir)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        source_connection = _readonly_sqlite_connection(source)
        destination = sqlite3.connect(tmp_path, timeout=60.0)
        try:
            source_connection.backup(destination, pages=1024, sleep=0.01)
            destination.commit()
            quick_check = _quick_check(destination)
            max_event_id, max_event_updated_at = _event_snapshot_facts(destination)
        finally:
            destination.close()
            source_connection.close()

        with tmp_path.open("rb") as handle:
            os.fsync(handle.fileno())
        size_bytes = tmp_path.stat().st_size
        if size_bytes <= 0:
            raise StaticSitePermanentError("snapshot_is_empty")
        sha256 = _sha256_file(tmp_path)
        revisions = {
            str(key): str(value)
            for key, value in (request_payload.get("event_revisions") or {}).items()
            if str(key).isdigit() and value
        }
        max_revision = max(revisions.values(), default=None)
        metadata = SnapshotMetadata(
            schema_version=SNAPSHOT_SCHEMA,
            snapshot_id=clean_id,
            source_database_name=source.name,
            sqlite_filename=final_path.name,
            created_at=created_at,
            quick_check=quick_check,
            sha256=sha256,
            size_bytes=size_bytes,
            target_watermark=str(request_payload.get("target_watermark") or request_watermark(request_payload)),
            latest_effect_at=str(request_payload.get("latest_effect_at") or "") or None,
            max_event_id=max_event_id,
            max_event_updated_at=max_event_updated_at,
            max_event_revision=max_revision,
            event_revisions=revisions,
        )
        os.replace(tmp_path, final_path)
        manifest_tmp = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
        manifest_tmp.write_text(json.dumps(asdict(metadata), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with manifest_tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(manifest_tmp, manifest_path)
        return final_path, manifest_path, metadata
    except StaticSiteReleaseError:
        tmp_path.unlink(missing_ok=True)
        raise
    except sqlite3.Error as exc:
        tmp_path.unlink(missing_ok=True)
        raise StaticSiteRetryableError(f"sqlite_snapshot_failed:{exc.__class__.__name__}:{exc}") from exc
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def validate_snapshot(snapshot_path: str | os.PathLike[str], manifest_path: str | os.PathLike[str]) -> SnapshotMetadata:
    snapshot = Path(snapshot_path)
    manifest = Path(manifest_path)
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        metadata = SnapshotMetadata(**payload)
    except Exception as exc:
        raise StaticSitePermanentError(f"invalid_snapshot_manifest:{exc}") from exc
    if metadata.schema_version != SNAPSHOT_SCHEMA or metadata.sqlite_filename != snapshot.name:
        raise StaticSitePermanentError("snapshot_manifest_identity_mismatch")
    if snapshot.stat().st_size != metadata.size_bytes or _sha256_file(snapshot) != metadata.sha256:
        raise StaticSitePermanentError("snapshot_hash_or_size_mismatch")
    connection = _readonly_sqlite_connection(snapshot)
    try:
        _quick_check(connection)
    finally:
        connection.close()
    return metadata


def validate_vector_barrier(request_payload: Mapping[str, Any], receipt_path: str | os.PathLike[str] | None) -> dict[str, Any]:
    """Validate an optional, hash-bound vector projection receipt.

    Disabled related/vector mode never blocks base pages.  Missing receipts are
    retryable because the independent vector lane may still be finishing;
    malformed/mismatched completed receipts are permanent for this immutable
    request and must be regenerated instead of silently trusted.
    """

    barrier = request_payload.get("vector_barrier")
    barrier = barrier if isinstance(barrier, Mapping) else {}
    if not barrier.get("required"):
        return {"status": "disabled", "required": False}
    if not receipt_path or not Path(receipt_path).is_file():
        raise StaticSiteRetryableError("vector_barrier_receipt_pending")
    try:
        receipt = json.loads(Path(receipt_path).read_text(encoding="utf-8"))
    except Exception as exc:
        raise StaticSitePermanentError(f"vector_barrier_receipt_invalid:{exc}") from exc
    if receipt.get("status") not in {"complete", "success"} or receipt.get("complete") is False:
        raise StaticSiteRetryableError("vector_barrier_incomplete")
    for key in ("expected_search_v3_hash", "expected_related_v1_hash"):
        expected = barrier.get(key)
        actual = receipt.get(key.removeprefix("expected_"))
        if expected and actual != expected:
            raise StaticSitePermanentError(f"vector_barrier_hash_mismatch:{key}")
    expected_revisions = request_payload.get("event_revisions") or {}
    actual_revisions = receipt.get("event_revisions") or {}
    for event_id, revision in expected_revisions.items():
        if str(actual_revisions.get(str(event_id)) or "") != str(revision):
            raise StaticSiteRetryableError(f"vector_barrier_revision_pending:{event_id}")
    return {
        "status": "complete",
        "required": True,
        "projection_run_id": receipt.get("run_id") or receipt.get("projection_run_id"),
        "search_v3_hash": receipt.get("search_v3_hash"),
        "related_v1_hash": receipt.get("related_v1_hash"),
    }


def classify_failure(exc: BaseException) -> FailureDisposition:
    if isinstance(exc, StaticSitePermanentError):
        return FailureDisposition("permanent_input_or_result", False)
    if isinstance(exc, StaticSiteRetryableError):
        return FailureDisposition("retryable_dependency", True)
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return FailureDisposition("retryable_external", True)
    return FailureDisposition("retryable_unclassified", True)


def max_attempts() -> int:
    try:
        return max(1, min(10, int(os.getenv("STATIC_SITE_MAX_ATTEMPTS") or "4")))
    except ValueError:
        return 4


def freshness_state(
    *,
    latest_effect_at: str | None,
    latest_success_at: str | None,
    has_active_request: bool,
    now: datetime | None = None,
    max_staleness_seconds: int | None = None,
) -> dict[str, Any]:
    current = now or utc_now()
    limit = max_staleness_seconds
    if limit is None:
        try:
            limit = max(900, int(os.getenv("STATIC_SITE_MAX_STALENESS_SECONDS") or "3600"))
        except ValueError:
            limit = 3600
    if not latest_effect_at:
        return {"status": "idle", "stale": False, "age_seconds": 0, "active": has_active_request}
    effect = datetime.fromisoformat(latest_effect_at.replace("Z", "+00:00"))
    if effect.tzinfo is None:
        effect = effect.replace(tzinfo=timezone.utc)
    success = None
    if latest_success_at:
        success = datetime.fromisoformat(latest_success_at.replace("Z", "+00:00"))
        if success.tzinfo is None:
            success = success.replace(tzinfo=timezone.utc)
    covered = bool(success and success >= effect)
    age = max(0, int((current.astimezone(timezone.utc) - effect.astimezone(timezone.utc)).total_seconds()))
    stale = bool(not covered and age > int(limit))
    return {
        "status": "fresh" if covered else ("stale" if stale else "pending"),
        "stale": stale,
        "age_seconds": age,
        "active": bool(has_active_request),
    }
