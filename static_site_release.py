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
import re
import sqlite3
import tempfile
import tarfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
from typing import Any, Iterable, Mapping


REQUEST_SCHEMA = "static_site_build_request_v1"
SNAPSHOT_SCHEMA = "static_site_sqlite_snapshot_v1"
RELEASE_CHANNEL_SECRET = "secret_preview"
MAX_REASONS = 24
MAX_EVENT_IDS = 256
MAX_CORRELATIONS = 128
MAX_EVENT_REVISIONS = 256
SECRET_CANDIDATE_RESULT_SCHEMA = "static_site_build_result_v2"
SECRET_CANDIDATE_MANIFEST_SCHEMA = "static_secret_candidate_manifest_v1"
SECRET_CANDIDATE_TOKEN_RE = r"[A-Za-z0-9_-]{43}"


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


@dataclass(frozen=True)
class SecretCandidateReceipt:
    schema_version: str
    build_id: str
    run_id: str
    snapshot_id: str
    token_sha256: str
    manifest_sha256: str
    object_count: int
    public_url: str
    verified_at: str
    root_mutation: bool = False
    stable_ics_mutation: bool = False


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


def validate_production_candidate_result(
    result_path: str | os.PathLike[str],
    *,
    output_dir: str | os.PathLike[str],
    build_id: str,
    run_id: str,
    repo_sha: str,
    snapshot: SnapshotMetadata,
    candidate_token: str,
) -> tuple[dict[str, Any], Path]:
    """Revalidate the bounded Kaggle receipt at the trusted publisher boundary."""

    path = Path(result_path)
    if not path.is_file() or path.stat().st_size > 256 * 1024:
        raise StaticSitePermanentError("static_site_result_receipt_missing_or_unbounded")
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise StaticSitePermanentError(f"static_site_result_receipt_invalid:{exc}") from exc
    expected = {
        "schema_version": SECRET_CANDIDATE_RESULT_SCHEMA,
        "ok": True,
        "profile": "production-candidate",
        "build_id": build_id,
        "run_id": run_id,
        "repo_sha": repo_sha,
    }
    for key, value in expected.items():
        if result.get(key) != value:
            raise StaticSitePermanentError(f"static_site_result_identity_mismatch:{key}")
    result_snapshot = result.get("snapshot") if isinstance(result.get("snapshot"), Mapping) else {}
    if (
        result_snapshot.get("snapshot_id") != snapshot.snapshot_id
        or result_snapshot.get("snapshot_sha256") != snapshot.sha256
        or int(result_snapshot.get("size") or -1) != snapshot.size_bytes
    ):
        raise StaticSitePermanentError("static_site_result_snapshot_mismatch")
    candidate = result.get("candidate") if isinstance(result.get("candidate"), Mapping) else {}
    if candidate.get("token") != candidate_token or not re.fullmatch(SECRET_CANDIDATE_TOKEN_RE, candidate_token):
        raise StaticSitePermanentError("static_site_result_candidate_token_mismatch")
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2 or {item.get("kind") for item in artifacts if isinstance(item, Mapping)} != {
        "production_root",
        "secret_candidate",
    }:
        raise StaticSitePermanentError("static_site_result_artifact_set_mismatch")
    base = Path(output_dir).resolve()
    candidate_archive: Path | None = None
    for item in artifacts:
        if not isinstance(item, Mapping):
            raise StaticSitePermanentError("static_site_result_artifact_invalid")
        name = str(item.get("filename") or "")
        artifact = (base / name).resolve()
        if artifact.parent != base:
            raise StaticSitePermanentError("static_site_result_artifact_path_invalid")
        if not name.endswith(".tar.gz") or not artifact.is_file():
            raise StaticSitePermanentError("static_site_result_artifact_missing")
        if artifact.stat().st_size != int(item.get("size") or -1) or _sha256_file(artifact) != item.get("sha256"):
            raise StaticSitePermanentError("static_site_result_artifact_hash_mismatch")
        if item.get("kind") == "secret_candidate":
            candidate_archive = artifact
    if candidate_archive is None:
        raise StaticSitePermanentError("static_site_result_candidate_archive_missing")
    return dict(result), candidate_archive


def _safe_extract_candidate_archive(archive: Path, destination: Path, token: str) -> Path:
    prefix = f"_review/{token}/"
    if destination.exists():
        raise StaticSitePermanentError("secret_candidate_extract_destination_exists")
    destination.mkdir(parents=True)
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        if not members or len(members) > 100_000:
            raise StaticSitePermanentError("secret_candidate_archive_member_count_invalid")
        for member in members:
            name = member.name.lstrip("./")
            if name.rstrip("/") == prefix.rstrip("/") and member.isdir():
                continue
            if not name.startswith(prefix) or member.issym() or member.islnk() or not (member.isdir() or member.isfile()):
                raise StaticSitePermanentError(f"secret_candidate_archive_path_invalid:{name[:160]}")
            relative = name[len(prefix) :]
            parts = Path(relative).parts
            if not relative or any(part in {"", ".", ".."} for part in parts):
                raise StaticSitePermanentError("secret_candidate_archive_relative_path_invalid")
            target = destination.joinpath(*parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            source = bundle.extractfile(member)
            if source is None:
                raise StaticSitePermanentError("secret_candidate_archive_member_unreadable")
            with source, target.open("xb") as handle:
                while chunk := source.read(1024 * 1024):
                    handle.write(chunk)
    return destination


def _candidate_public_list_disabled(endpoint: str, bucket: str) -> bool:
    url = f"{endpoint.rstrip('/')}/{quote(bucket, safe='')}?list-type=2&max-keys=1"
    try:
        with urlopen(Request(url, headers={"Cache-Control": "no-cache"}), timeout=20) as response:
            if 200 <= int(response.status) < 300:
                raise StaticSitePermanentError("secret_candidate_bucket_anonymous_listing_enabled")
            raise StaticSiteRetryableError(f"secret_candidate_list_preflight_http:{response.status}")
    except HTTPError as exc:
        body = exc.read(4096).decode("utf-8", errors="replace")
        if exc.code in {401, 403} or "AccessDenied" in body or "Forbidden" in body:
            return True
        raise StaticSiteRetryableError(f"secret_candidate_list_preflight_http:{exc.code}") from exc


def publish_secret_candidate_archive(
    archive_path: str | os.PathLike[str],
    *,
    build_result: Mapping[str, Any],
    extraction_root: str | os.PathLike[str],
    bucket: str,
    endpoint: str,
    region: str,
    access_key_id: str,
    secret_access_key: str,
    public_base_url: str = "https://kenigevents.ru",
    s3_client: Any | None = None,
    list_preflight: Any | None = None,
    public_probe: Any | None = None,
) -> SecretCandidateReceipt:
    """Upload a checked candidate create-only; no root/control key is expressible."""

    candidate = build_result.get("candidate") if isinstance(build_result.get("candidate"), Mapping) else {}
    token = str(candidate.get("token") or "")
    if not re.fullmatch(SECRET_CANDIDATE_TOKEN_RE, token):
        raise StaticSitePermanentError("secret_candidate_token_invalid")
    (list_preflight or _candidate_public_list_disabled)(endpoint, bucket)
    root = _safe_extract_candidate_archive(
        Path(archive_path), Path(extraction_root) / f"candidate-{token}", token
    )
    manifest_path = root / "secret-candidate-manifest.json"
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
    except Exception as exc:
        raise StaticSitePermanentError(f"secret_candidate_manifest_invalid:{exc}") from exc
    if (
        manifest.get("schema_version") != SECRET_CANDIDATE_MANIFEST_SCHEMA
        or manifest.get("site_mode") != "secret_candidate"
        or manifest.get("publication_mode") != "secret_link"
        or manifest.get("token_sha256") != hashlib.sha256(token.encode()).hexdigest()
        or manifest.get("build_id") != build_result.get("build_id")
        or manifest.get("run_id") != build_result.get("run_id")
    ):
        raise StaticSitePermanentError("secret_candidate_manifest_identity_mismatch")
    for check in ("candidate_contract", "catalog_parity", "noindex", "no_referrer", "prefix_containment", "root_isolation"):
        if (manifest.get("checks") or {}).get(check) != "ok":
            raise StaticSitePermanentError(f"secret_candidate_manifest_unchecked:{check}")
    files = manifest.get("files")
    if not isinstance(files, list) or not files or len(files) > 100_000:
        raise StaticSitePermanentError("secret_candidate_manifest_files_invalid")
    objects: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in [*files, {
        "key": "secret-candidate-manifest.json",
        "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "size": len(manifest_bytes),
        "content_type": "application/json; charset=utf-8",
        "cache_control": "private, no-store, max-age=0",
    }]:
        key = str(item.get("key") or "")
        parts = Path(key).parts
        if not key or key.startswith("/") or "\\" in key or any(part in {"", ".", ".."} for part in parts):
            raise StaticSitePermanentError("secret_candidate_object_key_invalid")
        if key.startswith(("_static/", "ics/")) or key in {"current.json", "previous.json", "promotion-lease.json"}:
            raise StaticSitePermanentError("secret_candidate_protected_key_rejected")
        object_key = f"_review/{token}/{key}"
        if object_key in seen:
            raise StaticSitePermanentError("secret_candidate_duplicate_object")
        seen.add(object_key)
        local_path = root.joinpath(*parts)
        if not local_path.is_file() or local_path.stat().st_size != int(item.get("size") or -1) or _sha256_file(local_path) != item.get("sha256"):
            raise StaticSitePermanentError(f"secret_candidate_file_hash_mismatch:{key}")
        objects.append({**item, "object_key": object_key, "local_path": local_path})
    if s3_client is None:
        import boto3

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
    for item in objects:
        with item["local_path"].open("rb") as handle:
            s3_client.put_object(
                Bucket=bucket,
                Key=item["object_key"],
                Body=handle,
                IfNoneMatch="*",
                ContentType=str(item.get("content_type") or "application/octet-stream"),
                CacheControl="private, no-store, max-age=0",
                Metadata={"sha256": str(item["sha256"])},
            )
    for item in objects:
        response = s3_client.get_object(Bucket=bucket, Key=item["object_key"])
        body = response["Body"].read()
        if len(body) != int(item["size"]) or hashlib.sha256(body).hexdigest() != item["sha256"]:
            raise StaticSiteRetryableError(f"secret_candidate_uploaded_hash_mismatch:{item['object_key']}")
        if str(response.get("ContentType") or "") != str(item.get("content_type") or "application/octet-stream"):
            raise StaticSiteRetryableError(f"secret_candidate_uploaded_mime_mismatch:{item['object_key']}")
    public_url = f"{public_base_url.rstrip('/')}/_review/{token}/"
    if public_probe is not None:
        public_probe(public_url)
    else:
        try:
            with urlopen(Request(public_url, headers={"Cache-Control": "no-cache"}), timeout=30) as response:
                if response.status != 200 or not response.read(16):
                    raise StaticSiteRetryableError(f"secret_candidate_public_http:{response.status}")
        except HTTPError as exc:
            raise StaticSiteRetryableError(f"secret_candidate_public_http:{exc.code}") from exc
    return SecretCandidateReceipt(
        schema_version="static_secret_candidate_receipt_v1",
        build_id=str(build_result.get("build_id") or ""),
        run_id=str(build_result.get("run_id") or ""),
        snapshot_id=str((build_result.get("snapshot") or {}).get("snapshot_id") or ""),
        token_sha256=hashlib.sha256(token.encode()).hexdigest(),
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        object_count=len(objects),
        public_url=public_url,
        verified_at=iso_utc(),
    )


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
