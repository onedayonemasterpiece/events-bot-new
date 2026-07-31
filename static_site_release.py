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
import shutil
import sqlite3
import tempfile
import tarfile
import time as unix_time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


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
STATIC_SITE_TIME_ZONE_NAME = "Europe/Kaliningrad"
STATIC_SITE_TIME_ZONE = ZoneInfo(STATIC_SITE_TIME_ZONE_NAME)
STATIC_SITE_FINGERPRINT_SCHEMA = "static_site_input_fingerprint_v1"
STATIC_SITE_STATE_SCHEMA = "static_site_build_state_v1"
STATIC_SITE_PROJECTION_VERSION = "static_event_public_projection_v2"
CURRENT_SECRET_CANDIDATE_RECEIPT_SCHEMA = "static_site_current_secret_candidate_v1"
STATIC_SITE_OUTPUT_NAME_RE = re.compile(
    r"output-(production-[A-Za-z0-9][A-Za-z0-9._-]{0,191})"
)
STATIC_SITE_COUNT_KEYS = (
    "event_count",
    "event_page_count",
    "page_count",
    "file_count",
    "object_count",
    "bytes",
)


@dataclass(frozen=True)
class StaticSiteBuildClock:
    time_zone: str
    effective_date: str
    current_datetime: str


@dataclass(frozen=True)
class StaticSiteBuildClaim:
    action: str
    input_fingerprint: str
    claim_token: str | None = None
    blocking_run_id: str | None = None
    blocking_fingerprint: str | None = None
    previous_run_id: str | None = None


@dataclass(frozen=True)
class StaticSiteRecoveryClaim:
    claim_token: str
    job_id: int
    run_id: str
    input_fingerprint: str
    effective_date: str
    kernel_ref: str | None = None
    dataset_ref: str | None = None
    remote_status: str | None = None
    remote_terminal_at: str | None = None


def resolve_build_clock(
    *,
    now: datetime | None = None,
    current_date: str | date | None = None,
    current_datetime: str | datetime | None = None,
) -> StaticSiteBuildClock:
    """Resolve one explicit Europe/Kaliningrad clock for a build.

    A date-only override is normalized to local midnight, which preserves
    deterministic historical/manual builds.  When a datetime is supplied its
    local date must agree with ``current_date`` instead of silently letting the
    exporter use two different days.
    """

    requested_date: date | None = None
    if current_date not in (None, ""):
        try:
            requested_date = (
                current_date if isinstance(current_date, date) else date.fromisoformat(str(current_date))
            )
        except (TypeError, ValueError) as exc:
            raise StaticSitePermanentError("static_site_current_date_invalid") from exc

    if current_datetime not in (None, ""):
        try:
            parsed = (
                current_datetime
                if isinstance(current_datetime, datetime)
                else datetime.fromisoformat(str(current_datetime).replace("Z", "+00:00"))
            )
        except (TypeError, ValueError) as exc:
            raise StaticSitePermanentError("static_site_current_datetime_invalid") from exc
        if parsed.tzinfo is None:
            # CLI values without an offset are explicitly Kaliningrad local,
            # never host-local or UTC by accident.
            parsed = parsed.replace(tzinfo=STATIC_SITE_TIME_ZONE)
        local = parsed.astimezone(STATIC_SITE_TIME_ZONE)
        if requested_date is not None and local.date() != requested_date:
            raise StaticSitePermanentError("static_site_build_clock_date_mismatch")
    elif requested_date is not None:
        local = datetime.combine(requested_date, time.min, tzinfo=STATIC_SITE_TIME_ZONE)
    else:
        observed = now or datetime.now(timezone.utc)
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        local = observed.astimezone(STATIC_SITE_TIME_ZONE)
        requested_date = local.date()

    effective = requested_date or local.date()
    return StaticSiteBuildClock(
        time_zone=STATIC_SITE_TIME_ZONE_NAME,
        effective_date=effective.isoformat(),
        current_datetime=local.isoformat(timespec="seconds"),
    )


class StaticSiteReleaseError(RuntimeError):
    """Base error whose failure class is safe to persist in outbox evidence."""


class StaticSiteRetryableError(StaticSiteReleaseError):
    """A transient external/dependency failure that may be retried."""


class StaticSiteSingleFlightDeferred(StaticSiteRetryableError):
    """Another remote build owns the lease; retain this request unchanged."""


class StaticSitePermanentError(StaticSiteReleaseError):
    """An invalid immutable input/result that must not be retried unchanged."""


def static_site_artifact_root(
    repo_root: str | os.PathLike[str] | None = None,
) -> Path:
    """Resolve the shared Fly-side builder root.

    Production config points this at the persistent volume.  The repository
    fallback preserves local developer behavior and remains independent of the
    caller's current working directory.
    """

    configured = (os.getenv("STATIC_SITE_ARTIFACT_ROOT") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    base = Path(repo_root).resolve() if repo_root is not None else Path(__file__).resolve().parent
    return (base / "artifacts" / "codex" / "static-site-builder").resolve()


def static_site_scratch_root(
    artifact_root: str | os.PathLike[str] | None = None,
) -> Path:
    configured = (os.getenv("STATIC_SITE_SCRATCH_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    root = Path(artifact_root).resolve() if artifact_root is not None else static_site_artifact_root()
    return root / ".tmp"


def static_site_result_counts(
    result: Mapping[str, Any],
    *,
    object_count: int | None = None,
) -> dict[str, int]:
    """Copy only bounded numeric build counts into durable diagnostics."""

    raw_counts = result.get("counts")
    raw_counts = raw_counts if isinstance(raw_counts, Mapping) else {}
    counts: dict[str, int] = {}
    for key in STATIC_SITE_COUNT_KEYS:
        value = raw_counts.get(key)
        if key == "event_count" and value is None:
            value = result.get("event_count")
        if key == "object_count" and object_count is not None:
            value = object_count
        if isinstance(value, bool) or value is None:
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if 0 <= parsed <= 10**12:
            counts[key] = parsed
    return counts


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


@dataclass(frozen=True)
class CurrentSecretCandidate:
    """The latest fully checked and published immutable review target.

    This is an internal durable pointer, not a public redirect.  The bearer
    token exists only inside ``public_url``; callers must not log or persist it
    separately.
    """

    schema_version: str
    release_channel: str
    build_id: str
    run_id: str
    repo_sha: str
    snapshot_id: str
    input_fingerprint: str
    effective_date: str
    result_sha256: str
    manifest_sha256: str
    token_sha256: str
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
    daily_share_refresh = (
        payload.get("daily_share_refresh")
        if isinstance(payload.get("daily_share_refresh"), Mapping)
        else {}
    )
    return {
        "latest_effect_at": payload.get("latest_effect_at"),
        "event_ids": payload.get("event_ids") or [],
        "event_revisions": payload.get("event_revisions") or {},
        "correlation_ids": payload.get("correlation_ids") or [],
        "reasons": payload.get("reasons") or [],
        "release_channel": payload.get("release_channel") or RELEASE_CHANNEL_SECRET,
        "force_rebuild": bool(payload.get("force_rebuild")),
        "daily_share_refresh": {
            "local_date": daily_share_refresh.get("local_date"),
            "time_zone": daily_share_refresh.get("time_zone"),
            "force_fingerprint": daily_share_refresh.get("force_fingerprint"),
        }
        if daily_share_refresh
        else None,
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
    force_rebuild: bool = False,
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
    effect_timestamp = iso_utc(effect_at)
    payload: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "release_channel": RELEASE_CHANNEL_SECRET if release_channel != RELEASE_CHANNEL_SECRET else release_channel,
        "trigger": _clean_token(trigger, max_len=64) or "unknown",
        "reasons": _bounded_unique([reason], limit=MAX_REASONS),
        "event_ids": ids,
        "event_revisions": revisions,
        "correlation_ids": [correlation],
        "first_effect_at": effect_timestamp,
        "latest_effect_at": effect_timestamp,
        "force_rebuild": bool(force_rebuild),
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
        left_trigger = _clean_token(left.get("trigger"), max_len=64) or ""
        right_trigger = _clean_token(right.get("trigger"), max_len=64) or ""
        immediate_triggers = {"operator_request", "calendar_rollover", "startup_catchup"}
        if bool(left.get("force_rebuild") or right.get("force_rebuild")):
            merged["trigger"] = "operator_request"
        elif left_trigger in immediate_triggers:
            merged["trigger"] = left_trigger
        elif right_trigger in immediate_triggers:
            merged["trigger"] = right_trigger
        else:
            merged["trigger"] = right_trigger or left_trigger or "unknown"
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
        first_effect_values = [
            value
            for value in (
                left.get("first_effect_at") or left.get("latest_effect_at"),
                right.get("first_effect_at") or right.get("latest_effect_at"),
            )
            if value
        ]
        merged["first_effect_at"] = min(
            (iso_utc(value) for value in first_effect_values),
            default=merged["latest_effect_at"],
        )
        left_barrier = left.get("vector_barrier") if isinstance(left.get("vector_barrier"), Mapping) else {}
        right_barrier = right.get("vector_barrier") if isinstance(right.get("vector_barrier"), Mapping) else {}
        merged["vector_barrier"] = {
            "required": bool(left_barrier.get("required") or right_barrier.get("required")),
            "expected_search_v3_hash": right_barrier.get("expected_search_v3_hash") or left_barrier.get("expected_search_v3_hash"),
            "expected_related_v1_hash": right_barrier.get("expected_related_v1_hash") or left_barrier.get("expected_related_v1_hash"),
        }
        merged["force_rebuild"] = bool(left.get("force_rebuild") or right.get("force_rebuild"))
        left_daily = (
            left.get("daily_share_refresh")
            if isinstance(left.get("daily_share_refresh"), Mapping)
            else None
        )
        right_daily = (
            right.get("daily_share_refresh")
            if isinstance(right.get("daily_share_refresh"), Mapping)
            else None
        )
        if right_daily or left_daily:
            # Every enabled build can cover the daily share. Preserve that
            # durable calendar evidence across coalescing, while a newer
            # request replaces the previous day's marker.
            merged["daily_share_refresh"] = dict(right_daily or left_daily or {})
        merged["daily_share_idempotent"] = bool(
            left.get("daily_share_idempotent")
            or right.get("daily_share_idempotent")
        )
    merged.setdefault("schema_version", REQUEST_SCHEMA)
    merged["release_channel"] = RELEASE_CHANNEL_SECRET
    merged.setdefault("reasons", [])
    merged.setdefault("event_ids", [])
    merged.setdefault("event_revisions", {})
    merged.setdefault("correlation_ids", [])
    merged.setdefault("latest_effect_at", iso_utc())
    merged.setdefault("first_effect_at", merged["latest_effect_at"])
    merged.setdefault("force_rebuild", False)
    merged.setdefault("daily_share_idempotent", False)
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


# Columns that can reach the public static projection.  Deliberately absent are
# outbox attempts/status, metrics polling timestamps, Kaggle ids, cache access
# times and other operational churn that must not trigger an effectful build.
_PUBLIC_FINGERPRINT_TABLES: dict[str, tuple[str, ...]] = {
    "event": (
        "id", "title", "description", "short_description", "search_digest",
        "festival", "date", "end_date", "time", "time_is_default",
        "location_name", "location_address", "city", "ticket_price_min",
        "ticket_price_max", "ticket_link", "ticket_status", "event_type",
        "is_free", "pushkin_card", "silent", "lifecycle_status", "status",
        "moderation_status", "identity_status", "merged_into_event_id",
        "age_restriction", "age_restriction_status", "age_restriction_decision_version",
        "age_restriction_input_hash", "age_assessment", "age_assessment_status",
        "age_assessment_decision_version", "age_assessment_input_hash",
        "linked_event_ids", "other_date_ids", "photo_urls", "photo_count", "topics",
        "source_post_url", "source_vk_post_url", "tg_event_post_url", "vk_repost_url",
    ),
    "eventposter": (
        "id", "event_id", "supabase_url", "catbox_url", "ocr_text", "review_status",
        "display_order", "media_role", "recommended_hero_fit", "width", "height",
    ),
    "event_source": ("event_id", "source_url"),
    "event_publication": ("event_id", "status", "stored_url", "live_url"),
    "interest_club": (
        "id", "slug", "canonical_name", "topic", "description", "city", "typical_place",
        "public_status",
    ),
    "interest_club_event": ("club_id", "event_id", "status"),
    "festival_calendar_item": (
        "id", "calendar_year", "slug", "title", "description", "start_date",
        "end_date", "date_precision", "date_label", "sort_date", "month_key",
        "display_order", "place_label", "category", "status", "status_label",
        "source_url", "source_label", "internal_event_id", "festival_id",
        "cover_key", "image_width", "image_height", "media_mode",
        "object_position", "catalog_version", "is_public",
    ),
}


def _canonical_scalar(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "size": len(value)}
    if not isinstance(value, str):
        return value
    clean = value.strip()
    if clean[:1] in {"[", "{"}:
        try:
            return json.loads(clean)
        except (TypeError, ValueError):
            pass
    return value


def _table_projection_digest(
    connection: sqlite3.Connection,
    table: str,
    requested: tuple[str, ...],
    *,
    effective_date: str,
) -> str:
    columns = {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}
    selected = [name for name in requested if name in columns]
    digest = hashlib.sha256()
    digest.update(json.dumps({"table": table, "columns": selected}, separators=(",", ":")).encode())
    if not selected:
        return digest.hexdigest()
    quoted = ",".join(f'"{name}"' for name in selected)
    order = ",".join(f'"{name}"' for name in selected)
    where = ""
    params: tuple[Any, ...] = ()
    event_columns = {
        str(row[1]) for row in connection.execute('PRAGMA table_info("event")')
    }
    if table == "event" and "date" in columns:
        if "end_date" in columns:
            where = " WHERE date >= ? OR COALESCE(NULLIF(end_date, ''), date) >= ?"
            params = (effective_date, effective_date)
        else:
            where = " WHERE date >= ?"
            params = (effective_date,)
    elif table in {"eventposter", "event_source", "event_publication"} and {
        "id",
        "date",
    }.issubset(event_columns):
        end_clause = (
            " OR COALESCE(NULLIF(end_date, ''), date) >= ?" if "end_date" in event_columns else ""
        )
        where = (
            f" WHERE event_id IN (SELECT id FROM event WHERE date >= ?{end_clause})"
        )
        params = (effective_date, effective_date) if end_clause else (effective_date,)
    elif table == "interest_club" and "public_status" in columns:
        where = " WHERE public_status='approved'"
    elif table == "interest_club_event" and {"id", "date"}.issubset(event_columns):
        end_clause = (
            " OR COALESCE(NULLIF(end_date, ''), date) >= ?" if "end_date" in event_columns else ""
        )
        where = (
            f" WHERE event_id IN (SELECT id FROM event WHERE date >= ?{end_clause})"
        )
        params = (effective_date, effective_date) if end_clause else (effective_date,)
    elif table == "festival_calendar_item" and {
        "calendar_year",
        "is_public",
    }.issubset(columns):
        current_year = date.fromisoformat(effective_date).year
        where = (
            " WHERE is_public=1 AND ("
            "COALESCE(NULLIF(end_date,''),'') >= ? OR "
            "(COALESCE(NULLIF(end_date,''),'')='' AND calendar_year>=?))"
        )
        params = (effective_date, current_year)
    for row in connection.execute(
        f'SELECT {quoted} FROM "{table}"{where} ORDER BY {order}', params
    ):
        canonical = [_canonical_scalar(value) for value in row]
        digest.update(json.dumps(canonical, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def compute_static_site_input_fingerprint(
    database_path: str | os.PathLike[str],
    *,
    effective_date: str,
    repo_sha: str,
    build_config: Mapping[str, Any],
    related_cache_path: str | os.PathLike[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Hash the canonical public projection and stable build policy inputs."""

    # Validate the date with the same zoned clock contract used by handoff.
    clock = resolve_build_clock(current_date=effective_date)
    connection = _readonly_sqlite_connection(Path(database_path))
    try:
        table_digests = {
            table: _table_projection_digest(
                connection, table, columns, effective_date=clock.effective_date
            )
            for table, columns in sorted(_PUBLIC_FINGERPRINT_TABLES.items())
        }
    finally:
        connection.close()
    cache_path = Path(related_cache_path).resolve() if related_cache_path else None
    related_digest = _sha256_file(cache_path) if cache_path and cache_path.is_file() else "absent"
    stable_config = {
        str(key): _canonical_scalar(value)
        for key, value in sorted(build_config.items())
        if key not in {
            "build_id", "run_id", "candidate_token", "queued_at", "generated_at",
            "snapshot_id", "snapshot_sha256", "output_dir", "status_callback_url",
        }
    }
    evidence: dict[str, Any] = {
        "schema_version": STATIC_SITE_FINGERPRINT_SCHEMA,
        "time_zone": clock.time_zone,
        "effective_date": clock.effective_date,
        "repo_sha": str(repo_sha),
        "projection_version": STATIC_SITE_PROJECTION_VERSION,
        "export_version": "export-production-preview-data-v1",
        "policy_version": "static-site-release-v12",
        "table_digests": table_digests,
        "related_digest": related_digest,
        "config": stable_config,
    }
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    fingerprint = hashlib.sha256(raw).hexdigest()
    return fingerprint, {**evidence, "input_fingerprint": fingerprint}


def _ensure_static_site_state_schema(connection: sqlite3.Connection) -> None:
    statements = (
        """
        CREATE TABLE IF NOT EXISTS static_site_build_state(
            release_channel TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            last_success_fingerprint TEXT,
            last_success_run_id TEXT,
            last_success_at TEXT,
            last_success_receipt_json TEXT NOT NULL DEFAULT '{}',
            current_secret_candidate_receipt_json TEXT,
            active_claim_token TEXT,
            active_job_id INTEGER,
            active_run_id TEXT,
            active_fingerprint TEXT,
            active_effective_date TEXT,
            active_claimed_at TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS static_site_build_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_channel TEXT NOT NULL,
            job_id INTEGER,
            request_watermark TEXT,
            input_fingerprint TEXT NOT NULL,
            effective_date TEXT NOT NULL,
            force_rebuild INTEGER NOT NULL DEFAULT 0,
            outcome TEXT NOT NULL,
            run_id TEXT,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_static_site_build_history_fingerprint
            ON static_site_build_history(input_fingerprint, outcome, created_at)
        """,
    )
    for statement in statements:
        connection.execute(statement)
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(static_site_build_state)").fetchall()
    }
    if "current_secret_candidate_receipt_json" not in columns:
        connection.execute(
            "ALTER TABLE static_site_build_state "
            "ADD COLUMN current_secret_candidate_receipt_json TEXT"
        )


def _validated_current_secret_candidate(
    value: Mapping[str, Any],
) -> CurrentSecretCandidate:
    """Validate the durable review pointer without trusting a URL alone."""

    required_text = {
        "schema_version": CURRENT_SECRET_CANDIDATE_RECEIPT_SCHEMA,
        "release_channel": RELEASE_CHANNEL_SECRET,
    }
    for key, expected in required_text.items():
        if value.get(key) != expected:
            raise StaticSitePermanentError(f"current_secret_candidate_identity_mismatch:{key}")
    build_id = str(value.get("build_id") or "")
    run_id = str(value.get("run_id") or "")
    repo_sha = str(value.get("repo_sha") or "")
    snapshot_id = str(value.get("snapshot_id") or "")
    input_fingerprint = str(value.get("input_fingerprint") or "")
    effective_date = str(value.get("effective_date") or "")
    result_sha256 = str(value.get("result_sha256") or "")
    manifest_sha256 = str(value.get("manifest_sha256") or "")
    token_sha256 = str(value.get("token_sha256") or "")
    public_url = str(value.get("public_url") or "")
    verified_at = str(value.get("verified_at") or "")
    if not re.fullmatch(r"production-[A-Za-z0-9][A-Za-z0-9._-]*", build_id):
        raise StaticSitePermanentError("current_secret_candidate_build_id_invalid")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,159}", run_id):
        raise StaticSitePermanentError("current_secret_candidate_run_id_invalid")
    if not re.fullmatch(r"[0-9a-f]{40}", repo_sha):
        raise StaticSitePermanentError("current_secret_candidate_repo_sha_invalid")
    if not snapshot_id or not re.fullmatch(r"[0-9a-f]{64}", input_fingerprint):
        raise StaticSitePermanentError("current_secret_candidate_snapshot_or_fingerprint_invalid")
    try:
        date.fromisoformat(effective_date)
        iso_utc(verified_at)
    except (TypeError, ValueError) as exc:
        raise StaticSitePermanentError("current_secret_candidate_timestamp_invalid") from exc
    for label, digest in (
        ("result", result_sha256),
        ("manifest", manifest_sha256),
        ("token", token_sha256),
    ):
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise StaticSitePermanentError(f"current_secret_candidate_{label}_sha256_invalid")
    parsed = urlsplit(public_url)
    path_match = re.fullmatch(rf"/_review/({SECRET_CANDIDATE_TOKEN_RE})/", parsed.path)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
        or not path_match
        or hashlib.sha256(path_match.group(1).encode()).hexdigest() != token_sha256
    ):
        raise StaticSitePermanentError("current_secret_candidate_public_url_invalid")
    try:
        object_count = int(value.get("object_count") or 0)
    except (TypeError, ValueError) as exc:
        raise StaticSitePermanentError("current_secret_candidate_object_count_invalid") from exc
    if object_count <= 0:
        raise StaticSitePermanentError("current_secret_candidate_object_count_invalid")
    if value.get("root_mutation") is not False or value.get("stable_ics_mutation") is not False:
        raise StaticSitePermanentError("current_secret_candidate_root_isolation_invalid")
    return CurrentSecretCandidate(
        schema_version=CURRENT_SECRET_CANDIDATE_RECEIPT_SCHEMA,
        release_channel=RELEASE_CHANNEL_SECRET,
        build_id=build_id,
        run_id=run_id,
        repo_sha=repo_sha,
        snapshot_id=snapshot_id,
        input_fingerprint=input_fingerprint,
        effective_date=effective_date,
        result_sha256=result_sha256,
        manifest_sha256=manifest_sha256,
        token_sha256=token_sha256,
        object_count=object_count,
        public_url=public_url,
        verified_at=iso_utc(verified_at),
        root_mutation=False,
        stable_ics_mutation=False,
    )


def resolve_current_secret_candidate(
    database_path: str | os.PathLike[str],
) -> CurrentSecretCandidate | None:
    """Return the canonical immutable preproduction review target.

    The resolver is read-only and fails closed for an absent, legacy or
    incomplete receipt.  Failed, no-op and artifact-only builds therefore
    cannot replace the last fully published candidate.
    """

    path = Path(database_path)
    if not path.is_file():
        return None
    connection = _readonly_sqlite_connection(path)
    try:
        state_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='static_site_build_state'"
        ).fetchone()
        if not state_exists:
            return None
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(static_site_build_state)").fetchall()
        }
        if "current_secret_candidate_receipt_json" not in columns:
            return None
        row = connection.execute(
            "SELECT current_secret_candidate_receipt_json "
            "FROM static_site_build_state WHERE release_channel=?",
            (RELEASE_CHANNEL_SECRET,),
        ).fetchone()
        if not row or not row[0]:
            return None
        try:
            payload = json.loads(str(row[0]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(payload, Mapping):
            return None
        try:
            return _validated_current_secret_candidate(payload)
        except StaticSitePermanentError:
            return None
    finally:
        connection.close()


def _parse_db_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _kaggle_run_is_live(connection: sqlite3.Connection, run_id: str, *, now: datetime, stale_seconds: int) -> bool:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='kaggle_run_ledger'"
    ).fetchone()
    if not exists:
        return False
    row = connection.execute(
        "SELECT status, updated_at, last_heartbeat_at, terminal_at FROM kaggle_run_ledger WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if not row or row[3]:
        return False
    if str(row[0] or "").lower() in {"done", "failed", "error", "cancelled", "complete"}:
        return False
    freshest = max(
        (value for value in (_parse_db_timestamp(row[1]), _parse_db_timestamp(row[2])) if value),
        default=None,
    )
    return bool(freshest and (now - freshest).total_seconds() <= stale_seconds)


def _kaggle_run_terminal_status(connection: sqlite3.Connection, run_id: str) -> str | None:
    """Return durable terminal evidence for a remote run, when available.

    A fresh local claim is not proof of live work after Kaggle has already
    written a terminal ledger row. This distinction lets a later coalesced
    Smart Update replace an orphaned claim immediately instead of waiting for
    the two-hour claim TTL.
    """

    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='kaggle_run_ledger'"
    ).fetchone()
    if not exists:
        return None
    row = connection.execute(
        "SELECT status, terminal_at FROM kaggle_run_ledger WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if not row:
        return None
    status = str(row[0] or "").strip().lower()
    if row[1] or status in {"done", "failed", "error", "cancelled", "complete"}:
        return status or "terminal"
    return None


def active_static_site_remote_run(
    database_path: str | os.PathLike[str],
    *,
    stale_seconds: int = 7200,
    now: datetime | None = None,
) -> str | None:
    """Return the active remote run only when its durable ledger is live."""

    now_utc = (now or utc_now()).astimezone(timezone.utc)
    connection = sqlite3.connect(str(database_path), timeout=30)
    try:
        state_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='static_site_build_state'"
        ).fetchone()
        if not state_exists:
            return None
        row = connection.execute(
            "SELECT active_run_id FROM static_site_build_state WHERE release_channel=?",
            (RELEASE_CHANNEL_SECRET,),
        ).fetchone()
        run_id = str(row[0] or "") if row else ""
        if not run_id:
            return None
        return run_id if _kaggle_run_is_live(
            connection, run_id, now=now_utc, stale_seconds=stale_seconds
        ) else None
    finally:
        connection.close()


def recoverable_static_site_build(
    database_path: str | os.PathLike[str],
    *,
    job_id: int,
) -> StaticSiteRecoveryClaim | None:
    """Return the previous attempt's exact handoff before permitting a new push.

    A Fly process can disappear after ``kernels_push`` while the fixed Kaggle
    kernel keeps running.  The active durable claim is therefore recoverable
    even when its callback heartbeat is stale or terminal: the caller must
    first reconcile the exact kernel dataset/output identity and only then
    either adopt it or release the claim for a new push.
    """

    connection = sqlite3.connect(str(database_path), timeout=30)
    try:
        state_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='static_site_build_state'"
        ).fetchone()
        if not state_exists:
            return None
        row = connection.execute(
            """
            SELECT active_claim_token, active_job_id, active_run_id,
                   active_fingerprint, active_effective_date
            FROM static_site_build_state WHERE release_channel=?
            """,
            (RELEASE_CHANNEL_SECRET,),
        ).fetchone()
        if not row or not row[0] or int(row[1] or 0) != int(job_id):
            return None
        run_id = str(row[2] or "").strip()
        fingerprint = str(row[3] or "").strip()
        effective_date = str(row[4] or "").strip()
        if not run_id or not re.fullmatch(r"[0-9a-f]{64}", fingerprint) or not effective_date:
            return None
        ledger = None
        ledger_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='kaggle_run_ledger'"
        ).fetchone()
        if ledger_exists:
            ledger = connection.execute(
                """
                SELECT kernel_ref, dataset_ref, status, terminal_at
                FROM kaggle_run_ledger WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
        return StaticSiteRecoveryClaim(
            claim_token=str(row[0]),
            job_id=int(row[1]),
            run_id=run_id,
            input_fingerprint=fingerprint,
            effective_date=effective_date,
            kernel_ref=str(ledger[0] or "").strip() or None if ledger else None,
            dataset_ref=str(ledger[1] or "").strip() or None if ledger else None,
            remote_status=str(ledger[2] or "").strip() or None if ledger else None,
            remote_terminal_at=str(ledger[3] or "").strip() or None if ledger else None,
        )
    finally:
        connection.close()


def claim_static_site_build(
    database_path: str | os.PathLike[str],
    *,
    job_id: int,
    run_id: str,
    input_fingerprint: str,
    effective_date: str,
    request_watermark: str | None,
    force_rebuild: bool = False,
    stale_seconds: int = 7200,
    now: datetime | None = None,
) -> StaticSiteBuildClaim:
    """Atomically perform durable no-op comparison and server-side single flight."""

    if not re.fullmatch(r"[0-9a-f]{64}", input_fingerprint):
        raise StaticSitePermanentError("static_site_input_fingerprint_invalid")
    now_utc = (now or utc_now()).astimezone(timezone.utc)
    now_iso = iso_utc(now_utc)
    token = uuid.uuid4().hex
    connection = sqlite3.connect(str(database_path), timeout=30, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("BEGIN IMMEDIATE")
        _ensure_static_site_state_schema(connection)
        connection.execute(
            "INSERT OR IGNORE INTO static_site_build_state(release_channel, schema_version, updated_at) VALUES(?, ?, ?)",
            (RELEASE_CHANNEL_SECRET, STATIC_SITE_STATE_SCHEMA, now_iso),
        )
        row = connection.execute(
            """
            SELECT last_success_fingerprint, last_success_run_id, active_claim_token,
                   active_run_id, active_fingerprint, active_claimed_at,
                   active_job_id, active_effective_date
            FROM static_site_build_state WHERE release_channel=?
            """,
            (RELEASE_CHANNEL_SECRET,),
        ).fetchone()
        assert row is not None
        (
            last_fingerprint,
            last_run_id,
            active_token,
            active_run_id,
            active_fingerprint,
            active_claimed_at,
            active_job_id,
            active_effective_date,
        ) = row
        if active_token:
            claimed_at = _parse_db_timestamp(active_claimed_at)
            claim_fresh = bool(claimed_at and (now_utc - claimed_at).total_seconds() <= stale_seconds)
            terminal_status = (
                _kaggle_run_terminal_status(connection, str(active_run_id))
                if active_run_id
                else None
            )
            remote_live = bool(
                active_run_id
                and _kaggle_run_is_live(
                    connection, str(active_run_id), now=now_utc, stale_seconds=stale_seconds
                )
            )
            if not terminal_status and (claim_fresh or remote_live):
                evidence = {"reason": "active_single_flight", "blocking_run_id": active_run_id}
                connection.execute(
                    """
                    INSERT INTO static_site_build_history(
                        release_channel, job_id, request_watermark, input_fingerprint,
                        effective_date, force_rebuild, outcome, run_id, evidence_json, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, 'busy', ?, ?, ?)
                    """,
                    (
                        RELEASE_CHANNEL_SECRET, job_id, request_watermark, input_fingerprint,
                        effective_date, int(force_rebuild), run_id,
                        json.dumps(evidence, sort_keys=True), now_iso,
                    ),
                )
                connection.commit()
                return StaticSiteBuildClaim(
                    "busy",
                    input_fingerprint,
                    blocking_run_id=str(active_run_id or "") or None,
                    blocking_fingerprint=str(active_fingerprint or "") or None,
                )
            if terminal_status:
                evidence = {
                    "reason": "terminal_remote_claim_superseded",
                    "remote_status": terminal_status,
                    "superseded_by_run_id": run_id,
                }
                connection.execute(
                    """
                    INSERT INTO static_site_build_history(
                        release_channel, job_id, request_watermark, input_fingerprint,
                        effective_date, force_rebuild, outcome, run_id, evidence_json, created_at
                    ) VALUES(?, ?, NULL, ?, ?, 0, 'failed', ?, ?, ?)
                    """,
                    (
                        RELEASE_CHANNEL_SECRET,
                        int(active_job_id or 0) or None,
                        str(active_fingerprint or "") or None,
                        str(active_effective_date or "") or None,
                        str(active_run_id or "") or None,
                        json.dumps(evidence, sort_keys=True),
                        now_iso,
                    ),
                )
        if not force_rebuild and last_fingerprint == input_fingerprint:
            evidence = {"reason": "unchanged_public_inputs", "previous_run_id": last_run_id}
            connection.execute(
                """
                INSERT INTO static_site_build_history(
                    release_channel, job_id, request_watermark, input_fingerprint,
                    effective_date, force_rebuild, outcome, run_id, evidence_json, created_at
                ) VALUES(?, ?, ?, ?, ?, 0, 'noop', ?, ?, ?)
                """,
                (
                    RELEASE_CHANNEL_SECRET, job_id, request_watermark, input_fingerprint,
                    effective_date, run_id, json.dumps(evidence, sort_keys=True), now_iso,
                ),
            )
            connection.execute(
                "UPDATE static_site_build_state SET updated_at=? WHERE release_channel=?",
                (now_iso, RELEASE_CHANNEL_SECRET),
            )
            connection.commit()
            return StaticSiteBuildClaim(
                "noop", input_fingerprint, previous_run_id=str(last_run_id or "") or None
            )
        connection.execute(
            """
            UPDATE static_site_build_state
            SET active_claim_token=?, active_job_id=?, active_run_id=?, active_fingerprint=?,
                active_effective_date=?, active_claimed_at=?, updated_at=?
            WHERE release_channel=?
            """,
            (
                token, job_id, run_id, input_fingerprint, effective_date, now_iso, now_iso,
                RELEASE_CHANNEL_SECRET,
            ),
        )
        connection.execute(
            """
            INSERT INTO static_site_build_history(
                release_channel, job_id, request_watermark, input_fingerprint,
                effective_date, force_rebuild, outcome, run_id, evidence_json, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'claimed', ?, '{}', ?)
            """,
            (
                RELEASE_CHANNEL_SECRET, job_id, request_watermark, input_fingerprint,
                effective_date, int(force_rebuild), run_id, now_iso,
            ),
        )
        connection.commit()
        return StaticSiteBuildClaim("claimed", input_fingerprint, claim_token=token)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def finish_static_site_build_claim(
    database_path: str | os.PathLike[str],
    *,
    claim_token: str,
    run_id: str,
    input_fingerprint: str,
    effective_date: str,
    success: bool,
    receipt: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> None:
    now_iso = iso_utc(now)
    evidence = dict(receipt or {})
    current_candidate_payload = evidence.get("current_secret_candidate")
    current_candidate = None
    if current_candidate_payload is not None:
        if not success or not isinstance(current_candidate_payload, Mapping):
            raise StaticSitePermanentError("current_secret_candidate_receipt_invalid")
        current_candidate = _validated_current_secret_candidate(current_candidate_payload)
        if (
            current_candidate.run_id != run_id
            or current_candidate.input_fingerprint != input_fingerprint
            or current_candidate.effective_date != effective_date
        ):
            raise StaticSitePermanentError("current_secret_candidate_claim_identity_mismatch")
    connection = sqlite3.connect(str(database_path), timeout=30, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("BEGIN IMMEDIATE")
        _ensure_static_site_state_schema(connection)
        row = connection.execute(
            "SELECT active_claim_token FROM static_site_build_state WHERE release_channel=?",
            (RELEASE_CHANNEL_SECRET,),
        ).fetchone()
        if not row or row[0] != claim_token:
            raise StaticSitePermanentError("static_site_claim_token_mismatch")
        if success:
            if current_candidate is not None:
                connection.execute(
                    """
                    UPDATE static_site_build_state
                    SET last_success_fingerprint=?, last_success_run_id=?, last_success_at=?,
                        last_success_receipt_json=?, current_secret_candidate_receipt_json=?,
                        active_claim_token=NULL, active_job_id=NULL, active_run_id=NULL,
                        active_fingerprint=NULL, active_effective_date=NULL,
                        active_claimed_at=NULL, updated_at=?
                    WHERE release_channel=? AND active_claim_token=?
                    """,
                    (
                        input_fingerprint, run_id, now_iso,
                        json.dumps(evidence, ensure_ascii=False, sort_keys=True, default=str),
                        json.dumps(asdict(current_candidate), ensure_ascii=False, sort_keys=True),
                        now_iso, RELEASE_CHANNEL_SECRET, claim_token,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE static_site_build_state
                    SET last_success_fingerprint=?, last_success_run_id=?, last_success_at=?,
                        last_success_receipt_json=?, active_claim_token=NULL, active_job_id=NULL,
                        active_run_id=NULL, active_fingerprint=NULL, active_effective_date=NULL,
                        active_claimed_at=NULL, updated_at=?
                    WHERE release_channel=? AND active_claim_token=?
                    """,
                    (
                        input_fingerprint, run_id, now_iso,
                        json.dumps(evidence, ensure_ascii=False, sort_keys=True, default=str),
                        now_iso, RELEASE_CHANNEL_SECRET, claim_token,
                    ),
                )
        else:
            connection.execute(
                """
                UPDATE static_site_build_state
                SET active_claim_token=NULL, active_job_id=NULL, active_run_id=NULL,
                    active_fingerprint=NULL, active_effective_date=NULL,
                    active_claimed_at=NULL, updated_at=?
                WHERE release_channel=? AND active_claim_token=?
                """,
                (now_iso, RELEASE_CHANNEL_SECRET, claim_token),
            )
        connection.execute(
            """
            INSERT INTO static_site_build_history(
                release_channel, input_fingerprint, effective_date, force_rebuild,
                outcome, run_id, evidence_json, created_at
            ) VALUES(?, ?, ?, 0, ?, ?, ?, ?)
            """,
            (
                RELEASE_CHANNEL_SECRET, input_fingerprint, effective_date,
                "success" if success else "failed", run_id,
                json.dumps(evidence, ensure_ascii=False, sort_keys=True, default=str), now_iso,
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


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


def delete_immutable_snapshot(
    snapshot_path: str | os.PathLike[str],
    manifest_path: str | os.PathLike[str],
) -> int:
    """Delete one terminal/regenerable snapshot and its SQLite sidecars.

    The caller owns lifecycle safety (the snapshot must no longer be used by a
    live handoff). Returning the removed byte count makes volume evidence
    explicit without weakening immutable creation semantics.
    """

    snapshot = Path(snapshot_path)
    manifest = Path(manifest_path)
    removed_bytes = 0
    for path in (
        snapshot,
        manifest,
        Path(f"{snapshot}-wal"),
        Path(f"{snapshot}-shm"),
    ):
        try:
            removed_bytes += path.stat().st_size
        except FileNotFoundError:
            continue
        path.unlink(missing_ok=True)
    return removed_bytes


def prune_immutable_snapshots(
    snapshot_dir: str | os.PathLike[str],
    *,
    preserve_paths: Iterable[str | os.PathLike[str]] = (),
    keep_latest_terminal: int = 1,
    stale_incomplete_seconds: int = 900,
) -> dict[str, Any]:
    """Bound leaked static-site snapshots without touching active handoffs.

    A normal terminal path deletes its own snapshot. This guard handles process
    death between snapshot creation and terminal cleanup. At most one newest
    unreferenced complete pair is retained for diagnosis; exact active paths
    supplied by the durable build state are always preserved.
    """

    root = Path(snapshot_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    keep_latest = max(0, int(keep_latest_terminal))
    preserved: set[Path] = set()
    for raw in preserve_paths:
        candidate = Path(raw).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            continue
        preserved.add(candidate)

    complete: list[tuple[float, Path, Path]] = []
    for snapshot in root.glob("snapshot-*.sqlite"):
        manifest = snapshot.with_name(f"{snapshot.stem}.manifest.json")
        if manifest.is_file():
            complete.append((snapshot.stat().st_mtime, snapshot, manifest))
    terminal = [
        item for item in sorted(complete, key=lambda item: item[0], reverse=True)
        if item[1].resolve() not in preserved and item[2].resolve() not in preserved
    ]
    removed: list[str] = []
    removed_bytes = 0
    for _mtime, snapshot, manifest in terminal[keep_latest:]:
        removed.append(snapshot.stem)
        removed_bytes += delete_immutable_snapshot(snapshot, manifest)
    complete_paths = {
        path.resolve()
        for _mtime, snapshot, manifest in complete
        for path in (snapshot, manifest, Path(f"{snapshot}-wal"), Path(f"{snapshot}-shm"))
    }
    stale_cutoff = unix_time.time() - max(60, int(stale_incomplete_seconds))
    removed_incomplete: list[str] = []
    for path in root.iterdir():
        resolved = path.resolve()
        if resolved in preserved or resolved in complete_paths or not path.is_file():
            continue
        name = path.name
        recognized = (
            (name.startswith(".snapshot-") and name.endswith(".tmp"))
            or (name.startswith("snapshot-") and name.endswith(".manifest.json.tmp"))
            or (name.startswith("snapshot-") and name.endswith(".sqlite"))
            or (name.startswith("snapshot-") and name.endswith((".sqlite-wal", ".sqlite-shm")))
            or (name.startswith("snapshot-") and name.endswith(".manifest.json"))
        )
        if not recognized or path.stat().st_mtime > stale_cutoff:
            continue
        removed_incomplete.append(name)
        removed_bytes += path.stat().st_size
        path.unlink(missing_ok=True)
    return {
        "removed_snapshot_ids": removed,
        "removed_incomplete_files": sorted(removed_incomplete),
        "removed_bytes": removed_bytes,
        "preserved_paths": sorted(str(path) for path in preserved),
        "retained_terminal_count": min(len(terminal), keep_latest),
    }


def _static_site_output_identity(path: Path) -> str | None:
    match = STATIC_SITE_OUTPUT_NAME_RE.fullmatch(path.name)
    return match.group(1) if match else None


def _directory_size_bytes(path: Path) -> int:
    total = 0
    for parent, directories, files in os.walk(path, followlinks=False):
        # Never follow an unexpected symlink hidden inside a recognized tree.
        directories[:] = [
            name for name in directories if not (Path(parent) / name).is_symlink()
        ]
        for name in files:
            candidate = Path(parent) / name
            if candidate.is_symlink():
                continue
            try:
                total += candidate.stat().st_size
            except FileNotFoundError:
                continue
    return total


def delete_static_site_output(
    artifact_root: str | os.PathLike[str],
    build_id: str,
) -> int:
    """Delete exactly one asserted production output directory.

    Lock files, staging trees, caches, symlinks and unrecognized operator paths
    are outside this function's deletion language.
    """

    root = Path(artifact_root).resolve()
    clean_build_id = str(build_id or "").strip()
    candidate = root / f"output-{clean_build_id}"
    if _static_site_output_identity(candidate) != clean_build_id:
        raise StaticSitePermanentError("static_site_output_identity_invalid")
    if candidate.is_symlink():
        raise StaticSitePermanentError("static_site_output_symlink_rejected")
    try:
        candidate.resolve().relative_to(root)
    except ValueError as exc:
        raise StaticSitePermanentError("static_site_output_path_escape") from exc
    if not candidate.exists():
        return 0
    if not candidate.is_dir():
        raise StaticSitePermanentError("static_site_output_not_directory")
    removed_bytes = _directory_size_bytes(candidate)
    shutil.rmtree(candidate)
    return removed_bytes


def prune_static_site_outputs(
    artifact_root: str | os.PathLike[str],
    *,
    preserve_build_ids: Iterable[str] = (),
    keep_latest_terminal: int = 0,
) -> dict[str, Any]:
    """Bound terminal production outputs while preserving exact handoffs.

    Only directories matching ``output-production-*`` are recognized.  A
    malformed preserve identity fails closed because it may represent an
    unreadable durable handoff; unknown filesystem entries are never removed.
    """

    root = Path(artifact_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    preserved: set[str] = set()
    for raw in preserve_build_ids:
        build_id = str(raw or "").strip()
        if not build_id or _static_site_output_identity(root / f"output-{build_id}") != build_id:
            raise StaticSitePermanentError("static_site_output_preserve_identity_invalid")
        preserved.add(build_id)

    recognized: list[tuple[float, str, Path]] = []
    skipped_symlinks: list[str] = []
    for path in root.iterdir():
        build_id = _static_site_output_identity(path)
        if build_id is None:
            continue
        if path.is_symlink():
            skipped_symlinks.append(build_id)
            continue
        if not path.is_dir():
            continue
        recognized.append((path.stat().st_mtime, build_id, path))

    keep_latest = max(0, int(keep_latest_terminal))
    terminal = sorted(
        (item for item in recognized if item[1] not in preserved),
        key=lambda item: item[0],
        reverse=True,
    )
    removed: list[str] = []
    removed_bytes = 0
    for _mtime, build_id, _path in terminal[keep_latest:]:
        removed_bytes += delete_static_site_output(root, build_id)
        removed.append(build_id)
    return {
        "removed_build_ids": removed,
        "removed_bytes": removed_bytes,
        "preserved_build_ids": sorted(preserved),
        "retained_terminal_count": min(len(terminal), keep_latest),
        "skipped_symlink_build_ids": sorted(skipped_symlinks),
    }


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
    if receipt.get("schema_version") != "event_vector_sync_receipt_v1":
        raise StaticSitePermanentError("vector_barrier_receipt_schema_mismatch")
    for key in ("search_v3_hash", "related_v1_hash"):
        if not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get(key) or "")):
            raise StaticSitePermanentError(f"vector_barrier_hash_invalid:{key}")
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
    input_fingerprint: str | None = None,
    build_clock: StaticSiteBuildClock | None = None,
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
    if input_fingerprint is not None and result.get("input_fingerprint") != input_fingerprint:
        raise StaticSitePermanentError("static_site_result_input_fingerprint_mismatch")
    if build_clock is not None:
        result_clock = result.get("build_clock") if isinstance(result.get("build_clock"), Mapping) else {}
        if (
            result_clock.get("time_zone") != build_clock.time_zone
            or result_clock.get("effective_date") != build_clock.effective_date
            or result_clock.get("current_datetime") != build_clock.current_datetime
        ):
            raise StaticSitePermanentError("static_site_result_build_clock_mismatch")
    semantic = result.get("semantic") if isinstance(result.get("semantic"), Mapping) else {}
    if semantic and semantic.get("status") != "disabled":
        if (
            int(semantic.get("provider_calls", -1)) != 0
            or int(semantic.get("event_count") or -1) <= 0
            or not re.fullmatch(r"[0-9a-f]{64}", str(semantic.get("artifact_sha256") or ""))
            or not re.fullmatch(r"[0-9a-f]{64}", str(semantic.get("manifest_sha256") or ""))
        ):
            raise StaticSitePermanentError("static_site_result_semantic_metadata_mismatch")
    service_share = (
        result.get("service_share")
        if isinstance(result.get("service_share"), Mapping)
        else {}
    )
    if service_share:
        if (
            service_share.get("status") != "ready"
            or int(service_share.get("width") or 0) != 1080
            or int(service_share.get("height") or 0) != 1350
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(service_share.get("manifest_payload_hash") or ""),
            )
        ):
            raise StaticSitePermanentError("static_site_result_service_share_metadata_mismatch")
        if build_clock is not None and service_share.get("local_date") != build_clock.effective_date:
            raise StaticSitePermanentError("static_site_result_service_share_freshness_mismatch")
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
    checks = result.get("checks") if isinstance(result.get("checks"), Mapping) else {}
    preview_contract = checks.get("preview_contract")
    preview_contract_status = (
        preview_contract.get("status")
        if isinstance(preview_contract, Mapping)
        else preview_contract
    )
    if preview_contract_status != "ok":
        raise StaticSitePermanentError(
            "static_site_result_preview_contract_incomplete"
        )
    if isinstance(preview_contract, Mapping) and (
        preview_contract.get("archived") is not False
        or preview_contract.get("published") is not False
    ):
        raise StaticSitePermanentError(
            "static_site_result_preview_contract_release_leak"
        )
    production_checks = (
        checks.get("production") if isinstance(checks.get("production"), Mapping) else {}
    )
    candidate_checks = (
        checks.get("secret_candidate")
        if isinstance(checks.get("secret_candidate"), Mapping)
        else {}
    )
    for check in (
        "astro_build",
        "template_matrix",
        "production_contract",
        "catalog_parity",
        "fixture_isolation",
        "canonical_and_indexing",
        "tree_hashes",
        "browser_visual",
    ):
        if production_checks.get(check) != "ok":
            raise StaticSitePermanentError(f"static_site_result_production_check_incomplete:{check}")
    for check in (
        "astro_build",
        "candidate_contract",
        "catalog_parity",
        "noindex",
        "no_referrer",
        "prefix_containment",
        "root_isolation",
        "browser_visual",
    ):
        if candidate_checks.get(check) != "ok":
            raise StaticSitePermanentError(f"static_site_result_candidate_check_incomplete:{check}")
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 3 or {item.get("kind") for item in artifacts if isinstance(item, Mapping)} != {
        "production_root",
        "secret_candidate",
        "browser_evidence",
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


def resolve_checked_static_site_artifact(
    build_result: Mapping[str, Any],
    *,
    output_dir: str | os.PathLike[str],
    kind: str,
) -> Path:
    """Resolve one artifact already covered by the checked build receipt.

    This repeats the bounded path/hash/size checks at the point of use so a
    caller cannot substitute a file between result validation and publication.
    """

    if kind not in {"production_root", "secret_candidate", "browser_evidence"}:
        raise StaticSitePermanentError("static_site_artifact_kind_invalid")
    artifacts = build_result.get("artifacts")
    if not isinstance(artifacts, list):
        raise StaticSitePermanentError("static_site_result_artifact_set_mismatch")
    matches = [
        item
        for item in artifacts
        if isinstance(item, Mapping) and item.get("kind") == kind
    ]
    if len(matches) != 1:
        raise StaticSitePermanentError(f"static_site_result_artifact_missing:{kind}")
    item = matches[0]
    name = str(item.get("filename") or "")
    base = Path(output_dir).resolve()
    artifact = (base / name).resolve()
    if (
        artifact.parent != base
        or not name.endswith(".tar.gz")
        or not artifact.is_file()
        or artifact.stat().st_size != int(item.get("size") or -1)
        or _sha256_file(artifact) != item.get("sha256")
    ):
        raise StaticSitePermanentError(f"static_site_result_artifact_hash_mismatch:{kind}")
    return artifact


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
    for check in ("candidate_contract", "catalog_parity", "noindex", "no_referrer", "prefix_containment", "root_isolation", "browser_visual"):
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

    def _is_create_conflict(exc: BaseException) -> bool:
        response = getattr(exc, "response", None)
        error = response.get("Error") if isinstance(response, Mapping) else None
        code = str(error.get("Code") or "") if isinstance(error, Mapping) else ""
        status = (
            (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
            if isinstance(response, Mapping)
            else None
        )
        return code in {"PreconditionFailed", "ConditionalRequestConflict", "412", "409"} or status in {
            409,
            412,
        }

    for item in objects:
        try:
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
        except Exception as exc:
            # A host retry after an SQLite receipt-write collision must adopt
            # only the exact immutable objects it created on the first pass.
            # The unconditional verification loop below binds bytes and MIME;
            # every other storage error remains visible.
            if not _is_create_conflict(exc):
                raise
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
