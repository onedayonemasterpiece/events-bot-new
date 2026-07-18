"""Read-only, redacted diagnostics for the static-site build pipeline.

The release state is intentionally inspected through SQLite's read-only mode.
This module does not import the bot or the release mutators, and it never
returns bearer URLs or candidate tokens.  Optional manifest and object-listing
files add artifact/bucket evidence without granting this tool publish access.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DIAGNOSTICS_SCHEMA = "static_site_build_diagnostics_v1"
STATIC_SITE_KIND = "static_site_builder"
TERMINAL_SUCCESS = {"complete", "done"}
TERMINAL_FAILURE = {"failed", "error", "cancelled", "canceled"}
TERMINAL_LEDGER = TERMINAL_SUCCESS | TERMINAL_FAILURE
COUNT_KEYS = (
    "event_count",
    "event_page_count",
    "page_count",
    "file_count",
    "object_count",
    "bytes",
)
_SECRET_PREFIX = re.compile(r"(?:^|/)_review/([A-Za-z0-9_-]{20,})(?:/|\b)")
_RELEASE_PREFIX = re.compile(r"(?:^|/)_static/releases/([^/]+)/")
_URL = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_BEARER = re.compile(r"\bBearer\s+[^\s\"']+", re.IGNORECASE)


def _utc(value: datetime | None = None) -> datetime:
    observed = value or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return observed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_json(value: Any) -> tuple[dict[str, Any], bool]:
    if isinstance(value, Mapping):
        return dict(value), True
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}, False
    return (dict(parsed), True) if isinstance(parsed, Mapping) else ({}, False)


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number >= 0 else None


def _redact_string(value: str) -> str:
    text = _BEARER.sub("Bearer <redacted>", value)
    text = _SECRET_PREFIX.sub("/_review/<redacted>/", text)
    return _URL.sub("<redacted-url>", text)


def redact(value: Any, *, key: str = "") -> Any:
    """Defense-in-depth redaction for every CLI serialization path."""

    key_l = key.casefold()
    if any(part in key_l for part in ("public_url", "bearer_url", "access_key", "secret_key", "password")):
        return "<redacted>"
    if "token" in key_l and not key_l.endswith(("token_sha256", "token_hash")):
        return "<redacted>"
    if key_l.endswith(("url", "endpoint")):
        return "<redacted-url>"
    if isinstance(value, Mapping):
        return {str(item_key): redact(item_value, key=str(item_key)) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return _redact_string(value)
    return value


def _connect_read_only(database_path: str | Path) -> sqlite3.Connection:
    path = Path(database_path).expanduser().resolve(strict=True)
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=10000")
    return connection


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _columns(connection: sqlite3.Connection, name: str) -> set[str]:
    if not _table_exists(connection, name):
        return set()
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({name})")}


def _history_rows(connection: sqlite3.Connection, cutoff: str) -> list[dict[str, Any]]:
    columns = _columns(connection, "static_site_build_history")
    required = {"id", "outcome", "created_at"}
    if not required.issubset(columns):
        return []
    rows = connection.execute(
        """
        SELECT * FROM static_site_build_history
        WHERE julianday(created_at) >= julianday(?)
        ORDER BY julianday(created_at) DESC, id DESC
        """,
        (cutoff,),
    ).fetchall()
    return [dict(row) for row in rows]


def _state_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _columns(connection, "static_site_build_state"):
        return []
    return [dict(row) for row in connection.execute("SELECT * FROM static_site_build_state")]


def _ledger_rows(
    connection: sqlite3.Connection,
    cutoff: str,
    referenced_run_ids: Iterable[str],
) -> list[dict[str, Any]]:
    columns = _columns(connection, "kaggle_run_ledger")
    if not {"run_id", "status"}.issubset(columns):
        return []
    rows_by_run: dict[str, dict[str, Any]] = {}
    if "kind" in columns:
        timestamp = None
        if {"updated_at", "created_at"}.issubset(columns):
            timestamp = "COALESCE(updated_at, created_at)"
        elif "updated_at" in columns:
            timestamp = "updated_at"
        elif "created_at" in columns:
            timestamp = "created_at"
        if timestamp:
            rows = connection.execute(
                f"SELECT * FROM kaggle_run_ledger WHERE kind=? "
                f"AND julianday({timestamp}) >= julianday(?)",
                (STATIC_SITE_KIND, cutoff),
            ).fetchall()
            rows_by_run.update({str(row["run_id"]): dict(row) for row in rows})
    run_ids = sorted({str(run_id) for run_id in referenced_run_ids if str(run_id).strip()})
    for offset in range(0, len(run_ids), 500):
        chunk = run_ids[offset : offset + 500]
        rows = connection.execute(
            f"SELECT * FROM kaggle_run_ledger WHERE run_id IN ({','.join('?' for _ in chunk)})",
            chunk,
        ).fetchall()
        rows_by_run.update({str(row["run_id"]): dict(row) for row in rows})
    return list(rows_by_run.values())


def _direct_counts(value: Mapping[str, Any], *, allow_bytes: bool = False) -> dict[str, int]:
    found: dict[str, int] = {}
    for key in COUNT_KEYS:
        if key == "bytes" and not allow_bytes:
            continue
        number = _positive_int(value.get(key))
        if number is not None:
            found[key] = number
    for alias in ("byte_count", "total_bytes"):
        if allow_bytes and "bytes" not in found:
            number = _positive_int(value.get(alias))
            if number is not None:
                found["bytes"] = number
    return found


def _extract_counts(value: Mapping[str, Any]) -> dict[str, int]:
    """Extract only explicit generated-output counts, never archive/snapshot size."""

    result = _direct_counts(value)
    counts = value.get("counts")
    if isinstance(counts, Mapping):
        result.update(_direct_counts(counts, allow_bytes=True))
    for key in ("build_receipt", "current_secret_candidate", "publication", "result"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            result.update(_extract_counts(nested))
    return result


def _safe_pointer(receipt: Mapping[str, Any] | None) -> dict[str, Any]:
    if not receipt:
        return {"status": "absent"}
    fields = (
        "release_channel",
        "build_id",
        "run_id",
        "repo_sha",
        "snapshot_id",
        "input_fingerprint",
        "effective_date",
        "result_sha256",
        "manifest_sha256",
        "token_sha256",
        "object_count",
        "verified_at",
        "root_mutation",
        "stable_ics_mutation",
    )
    pointer = {field: receipt.get(field) for field in fields if receipt.get(field) is not None}
    required = {"build_id", "run_id", "manifest_sha256", "token_sha256", "object_count"}
    pointer["status"] = "available" if required.issubset(pointer) else "incomplete"
    pointer["location"] = "/_review/<redacted>/"
    return pointer


def _manifest_records(paths: Sequence[str | Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    for index, raw_path in enumerate(paths, start=1):
        try:
            manifest_bytes = Path(raw_path).read_bytes()
            data = json.loads(manifest_bytes)
        except Exception:
            issues.append({"code": "manifest_input_invalid", "source_index": index})
            continue
        candidates = data if isinstance(data, list) else [data]
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                issues.append({"code": "manifest_entry_invalid", "source_index": index})
                continue
            schema = str(candidate.get("schema_version") or "unknown")
            site_mode = str(candidate.get("site_mode") or "")
            channel = "secret_preview" if "secret" in schema or site_mode == "secret_candidate" else "stable_candidate"
            counts = _extract_counts(candidate)
            files = candidate.get("files")
            if isinstance(files, list) and "file_count" not in counts:
                counts["file_count"] = len(files)
            record = {
                "source_index": index,
                "schema_version": schema,
                "channel": channel,
                "build_id": candidate.get("build_id"),
                "run_id": candidate.get("run_id"),
                "repo_sha": candidate.get("repo_sha"),
                "tree_sha256": candidate.get("tree_sha256"),
                "manifest_sha256": (
                    hashlib.sha256(manifest_bytes).hexdigest()
                    if len(candidates) == 1
                    else hashlib.sha256(
                        json.dumps(
                            candidate,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode()
                    ).hexdigest()
                ),
                "counts": counts,
            }
            token_hash = candidate.get("token_sha256")
            if channel == "secret_preview" and token_hash:
                record["token_sha256"] = token_hash
            records.append(record)
    return records, issues


def _object_entries(data: Any) -> list[Mapping[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, Mapping)]
    if not isinstance(data, Mapping):
        return []
    for key in ("objects", "Contents", "contents", "items"):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
    return []


def _safe_stable_pointer(data: Mapping[str, Any]) -> dict[str, Any]:
    pointer: Any = data.get("current_pointer")
    if pointer is None and isinstance(data.get("pointers"), Mapping):
        pointer = data["pointers"].get("current")
    if not isinstance(pointer, Mapping):
        return {"status": "absent"}
    allowed = (
        "release_id",
        "build_id",
        "run_id",
        "repo_sha",
        "manifest_sha256",
        "tree_sha256",
        "updated_at",
    )
    result = {key: pointer.get(key) for key in allowed if pointer.get(key) is not None}
    result["status"] = "observed_from_input" if result else "incomplete"
    return result


def _bucket_inventory(paths: Sequence[str | Path]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    secret: dict[str, dict[str, int]] = defaultdict(lambda: {"object_count": 0, "bytes": 0})
    releases: dict[str, dict[str, int]] = defaultdict(lambda: {"object_count": 0, "bytes": 0})
    total_objects = 0
    total_bytes = 0
    current_object_present = False
    stable_ics_objects = 0
    pointers: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    valid_sources = 0
    for index, raw_path in enumerate(paths, start=1):
        try:
            data = json.loads(Path(raw_path).read_text(encoding="utf-8"))
        except Exception:
            issues.append({"code": "bucket_inventory_input_invalid", "source_index": index})
            continue
        valid_sources += 1
        if isinstance(data, Mapping):
            pointer = _safe_stable_pointer(data)
            if pointer.get("status") != "absent":
                pointers.append(pointer)
        for item in _object_entries(data):
            key = str(item.get("key") or item.get("Key") or "").lstrip("/")
            if not key:
                continue
            size = _positive_int(item.get("size") if "size" in item else item.get("Size")) or 0
            total_objects += 1
            total_bytes += size
            current_object_present = current_object_present or key == "current.json"
            stable_ics_objects += int(key.startswith("ics/"))
            secret_match = _SECRET_PREFIX.search("/" + key)
            if secret_match:
                token_hash = hashlib.sha256(secret_match.group(1).encode()).hexdigest()
                secret[token_hash]["object_count"] += 1
                secret[token_hash]["bytes"] += size
            release_match = _RELEASE_PREFIX.search("/" + key)
            if release_match:
                release_id = release_match.group(1)[:200]
                releases[release_id]["object_count"] += 1
                releases[release_id]["bytes"] += size
    stable_pointer = pointers[-1] if pointers else {"status": "absent"}
    if stable_pointer["status"] == "absent" and current_object_present:
        stable_pointer = {"status": "pointer_object_present_unresolved", "object_key": "current.json"}
    return {
        "status": "available" if valid_sources else ("not_provided" if not paths else "invalid"),
        "source_count": valid_sources,
        "total_object_count": total_objects,
        "total_bytes": total_bytes,
        "secret_prefixes": [
            {"token_sha256": token_hash, **counts}
            for token_hash, counts in sorted(secret.items())
        ],
        "stable_releases": [
            {"release_id": release_id, **counts}
            for release_id, counts in sorted(releases.items())
        ],
        "stable_ics_object_count": stable_ics_objects,
        "current_object_present": current_object_present,
        "stable_pointer": stable_pointer,
    }, issues


def _add_issue(
    issues: list[dict[str, Any]],
    code: str,
    *,
    severity: str = "warning",
    run_id: str | None = None,
    **details: Any,
) -> None:
    issue = {"code": code, "severity": severity}
    if run_id:
        issue["run_id"] = run_id
    issue.update(details)
    issues.append(issue)


def collect_static_site_diagnostics(
    database_path: str | Path,
    *,
    hours: int = 24,
    now: datetime | None = None,
    manifest_paths: Sequence[str | Path] = (),
    bucket_inventory_paths: Sequence[str | Path] = (),
    detail_limit: int = 100,
) -> dict[str, Any]:
    """Collect a bounded report without mutating SQLite or external storage."""

    if not 1 <= int(hours) <= 24 * 31:
        raise ValueError("hours must be between 1 and 744")
    detail_limit = max(1, min(int(detail_limit), 1000))
    observed_at = _utc(now)
    cutoff = observed_at - timedelta(hours=int(hours))
    manifests, manifest_issues = _manifest_records(manifest_paths)
    bucket, bucket_issues = _bucket_inventory(bucket_inventory_paths)
    issues: list[dict[str, Any]] = [*manifest_issues, *bucket_issues]

    connection = _connect_read_only(database_path)
    try:
        available_tables = {
            table: _table_exists(connection, table)
            for table in (
                "static_site_build_history",
                "static_site_build_state",
                "kaggle_run_ledger",
            )
        }
        history = _history_rows(connection, _iso(cutoff))
        states = _state_rows(connection)
        referenced = {
            str(row.get("run_id") or "") for row in history if row.get("run_id")
        }
        for state in states:
            referenced.update(
                str(state.get(key) or "")
                for key in ("active_run_id", "last_success_run_id")
                if state.get(key)
            )
            current, valid = _safe_json(state.get("current_secret_candidate_receipt_json"))
            if valid and current.get("run_id"):
                referenced.add(str(current["run_id"]))
        ledger = _ledger_rows(connection, _iso(cutoff), referenced)
    finally:
        connection.close()

    ledger_by_run = {str(row.get("run_id")): row for row in ledger if row.get("run_id")}
    history_by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    outcomes = Counter()
    request_keys: set[str] = set()
    count_evidence: dict[str, dict[str, int]] = defaultdict(dict)
    count_sources: dict[str, set[str]] = defaultdict(set)
    for row in history:
        outcome = str(row.get("outcome") or "unknown").casefold()
        outcomes[outcome] += 1
        run_id = str(row.get("run_id") or "").strip()
        request_key = f"run:{run_id}" if run_id else (
            f"job:{row.get('job_id')}" if row.get("job_id") is not None else f"history:{row.get('id')}"
        )
        request_keys.add(request_key)
        if run_id:
            history_by_run[run_id].append(row)
        evidence, valid = _safe_json(row.get("evidence_json"))
        if not valid:
            _add_issue(issues, "history_evidence_invalid", history_id=row.get("id"))
        elif run_id:
            extracted = _extract_counts(evidence)
            if extracted:
                count_evidence[run_id].update(extracted)
                count_sources[run_id].add("history")

    state_by_channel = {str(row.get("release_channel") or "unknown"): row for row in states}
    secret_state = state_by_channel.get("secret_preview", {})
    last_receipt, last_receipt_valid = _safe_json(secret_state.get("last_success_receipt_json"))
    if secret_state and not last_receipt_valid:
        _add_issue(issues, "state_last_success_receipt_invalid")
    current_receipt, current_receipt_valid = _safe_json(
        secret_state.get("current_secret_candidate_receipt_json")
    )
    if secret_state.get("current_secret_candidate_receipt_json") and not current_receipt_valid:
        _add_issue(issues, "state_current_secret_pointer_invalid", severity="error")
    last_run = str(secret_state.get("last_success_run_id") or last_receipt.get("run_id") or "").strip()
    if last_run:
        extracted = _extract_counts(last_receipt)
        if extracted:
            count_evidence[last_run].update(extracted)
            count_sources[last_run].add("state")

    for run_id, row in ledger_by_run.items():
        progress, valid = _safe_json(row.get("progress_json"))
        if row.get("progress_json") and not valid:
            _add_issue(issues, "ledger_progress_invalid", run_id=run_id)
        extracted = _extract_counts(progress)
        if extracted:
            count_evidence[run_id].update(extracted)
            count_sources[run_id].add("ledger")

    for manifest in manifests:
        run_id = str(manifest.get("run_id") or "").strip()
        if run_id and manifest.get("counts"):
            count_evidence[run_id].update(manifest["counts"])
            count_sources[run_id].add("manifest")

    active_run = str(secret_state.get("active_run_id") or "").strip()
    for run_id, rows in history_by_run.items():
        run_outcomes = {str(row.get("outcome") or "").casefold() for row in rows}
        ledger_row = ledger_by_run.get(run_id)
        ledger_status = str((ledger_row or {}).get("status") or "").casefold()
        needs_ledger = bool(run_outcomes & {"claimed", "success", "failed"})
        if needs_ledger and ledger_row is None:
            _add_issue(issues, "history_run_missing_ledger", run_id=run_id)
        if "success" in run_outcomes and ledger_row:
            if ledger_status in TERMINAL_FAILURE:
                _add_issue(issues, "history_success_ledger_failed", severity="error", run_id=run_id, ledger_status=ledger_status)
            elif ledger_status not in TERMINAL_SUCCESS:
                _add_issue(issues, "history_success_ledger_nonterminal", run_id=run_id, ledger_status=ledger_status or "unknown")
        if "failed" in run_outcomes and ledger_status in TERMINAL_SUCCESS:
            _add_issue(issues, "history_failed_ledger_success", severity="error", run_id=run_id, ledger_status=ledger_status)
        if "claimed" in run_outcomes and not (run_outcomes & {"success", "failed"}) and run_id != active_run:
            _add_issue(
                issues,
                "claimed_run_missing_terminal_history" if ledger_status in TERMINAL_LEDGER else "orphan_claimed_history",
                run_id=run_id,
                ledger_status=ledger_status or "missing",
            )

    for run_id, ledger_row in ledger_by_run.items():
        if str(ledger_row.get("kind") or STATIC_SITE_KIND) != STATIC_SITE_KIND:
            continue
        run_outcomes = {
            str(row.get("outcome") or "").casefold() for row in history_by_run.get(run_id, [])
        }
        status = str(ledger_row.get("status") or "").casefold()
        if not run_outcomes:
            _add_issue(issues, "ledger_run_missing_history", run_id=run_id, ledger_status=status)
        if status in TERMINAL_SUCCESS and "success" not in run_outcomes:
            _add_issue(issues, "ledger_success_missing_history", run_id=run_id)
        elif status in TERMINAL_FAILURE and "failed" not in run_outcomes:
            _add_issue(issues, "ledger_failure_missing_history", run_id=run_id)

    if active_run and active_run not in ledger_by_run:
        _add_issue(issues, "active_state_run_missing_ledger", severity="error", run_id=active_run)
    if last_run:
        last_at = _parse_timestamp(secret_state.get("last_success_at"))
        if last_at and last_at >= cutoff and "success" not in {
            str(row.get("outcome") or "").casefold() for row in history_by_run.get(last_run, [])
        }:
            _add_issue(issues, "recent_state_success_missing_history", run_id=last_run)

    pointer_token_hash = str(current_receipt.get("token_sha256") or "")
    bucket_token_hashes = {
        str(item.get("token_sha256") or "") for item in bucket.get("secret_prefixes", [])
    }
    if bucket_inventory_paths and pointer_token_hash and pointer_token_hash not in bucket_token_hashes:
        _add_issue(issues, "current_secret_pointer_prefix_missing", severity="error", run_id=str(current_receipt.get("run_id") or "") or None)
    if bucket_inventory_paths:
        for token_hash in sorted(bucket_token_hashes - ({pointer_token_hash} if pointer_token_hash else set())):
            _add_issue(issues, "unreferenced_secret_prefix", severity="info", token_sha256=token_hash)
    current_pointer_run = str(current_receipt.get("run_id") or "").strip()
    if current_pointer_run and pointer_token_hash:
        current_prefix = next(
            (
                item
                for item in bucket.get("secret_prefixes", [])
                if item.get("token_sha256") == pointer_token_hash
            ),
            None,
        )
        if current_prefix:
            expected_objects = _positive_int(current_receipt.get("object_count"))
            observed_objects = _positive_int(current_prefix.get("object_count"))
            if expected_objects is not None and observed_objects != expected_objects:
                _add_issue(
                    issues,
                    "current_secret_pointer_object_count_mismatch",
                    severity="error",
                    run_id=current_pointer_run,
                    expected=expected_objects,
                    observed=observed_objects,
                )
            inserted = False
            for key in ("object_count", "bytes"):
                value = _positive_int(current_prefix.get(key))
                if value is not None and key not in count_evidence[current_pointer_run]:
                    count_evidence[current_pointer_run][key] = value
                    inserted = True
            if inserted:
                count_sources[current_pointer_run].add("bucket_inventory")

    builds: list[dict[str, Any]] = []
    for run_id, rows in history_by_run.items():
        timestamps = sorted(
            timestamp for timestamp in (_parse_timestamp(row.get("created_at")) for row in rows) if timestamp
        )
        ledger_row = ledger_by_run.get(run_id, {})
        builds.append(
            {
                "run_id": run_id,
                "job_ids": sorted({int(row["job_id"]) for row in rows if row.get("job_id") is not None}),
                "outcomes": sorted({str(row.get("outcome") or "unknown") for row in rows}),
                "first_seen_at": _iso(timestamps[0]) if timestamps else None,
                "last_seen_at": _iso(timestamps[-1]) if timestamps else None,
                "ledger": {
                    "status": ledger_row.get("status") or "missing",
                    "phase": ledger_row.get("phase"),
                    "last_heartbeat_at": ledger_row.get("last_heartbeat_at"),
                    "terminal_at": ledger_row.get("terminal_at"),
                },
                "generated_counts": count_evidence.get(run_id, {}),
                "count_sources": sorted(count_sources.get(run_id, set())),
            }
        )
    builds.sort(key=lambda item: str(item.get("last_seen_at") or ""), reverse=True)

    generated_totals = Counter()
    generated_builds = Counter()
    for run_id, rows in history_by_run.items():
        if not any(str(row.get("outcome") or "").casefold() == "success" for row in rows):
            continue
        for key, value in count_evidence.get(run_id, {}).items():
            generated_totals[key] += value
            generated_builds[key] += 1

    issue_counts = Counter(issue["code"] for issue in issues)
    report = {
        "schema_version": DIAGNOSTICS_SCHEMA,
        "generated_at": _iso(observed_at),
        "window": {"hours": int(hours), "from": _iso(cutoff), "to": _iso(observed_at)},
        "sources": {
            "database": {"status": "read_only", "tables": available_tables},
            "manifests": {"provided": len(manifest_paths), "valid": len(manifests)},
            "bucket_inventory": {"provided": len(bucket_inventory_paths), "valid": bucket.get("source_count", 0)},
        },
        "last_24h" if int(hours) == 24 else "window_summary": {
            "requests": len(request_keys),
            "outcomes": dict(sorted(outcomes.items())),
            "succeeded": outcomes.get("success", 0),
            "failed": outcomes.get("failed", 0),
            "noop": outcomes.get("noop", 0),
            "generated_totals": {key: generated_totals.get(key, 0) for key in COUNT_KEYS},
            "generated_evidence_builds": {key: generated_builds.get(key, 0) for key in COUNT_KEYS},
            "builds": builds[:detail_limit],
            "builds_truncated": max(0, len(builds) - detail_limit),
        },
        "channels": {
            "secret_preview": {
                "state": "available" if secret_state else "absent",
                "current_pointer": _safe_pointer(current_receipt if current_receipt_valid else None),
                "last_success": {
                    "run_id": secret_state.get("last_success_run_id"),
                    "at": secret_state.get("last_success_at"),
                    "fingerprint": secret_state.get("last_success_fingerprint"),
                },
                "active_claim": {
                    "status": "active" if secret_state.get("active_claim_token") else "idle",
                    "run_id": secret_state.get("active_run_id"),
                    "job_id": secret_state.get("active_job_id"),
                    "claimed_at": secret_state.get("active_claimed_at"),
                },
                "manifests": [item for item in manifests if item.get("channel") == "secret_preview"],
                "bucket_prefixes": bucket.get("secret_prefixes", []),
            },
            "stable": {
                "lifecycle": "diagnostics_only_no_activation",
                "current_pointer": bucket.get("stable_pointer", {"status": "absent"}),
                "manifests": [item for item in manifests if item.get("channel") == "stable_candidate"],
                "bucket_releases": bucket.get("stable_releases", []),
                "current_object_present": bucket.get("current_object_present", False),
                "stable_ics_object_count": bucket.get("stable_ics_object_count", 0),
            },
        },
        "bucket_inventory": {
            key: value for key, value in bucket.items() if key not in {"secret_prefixes", "stable_releases", "stable_pointer"}
        },
        "consistency": {
            "status": "issues" if issues else "ok",
            "issue_count": len(issues),
            "issue_types": dict(sorted(issue_counts.items())),
            "issues": issues[:detail_limit],
            "issues_truncated": max(0, len(issues) - detail_limit),
        },
    }
    return redact(report)


def format_static_site_diagnostics(report: Mapping[str, Any]) -> str:
    """Render a compact operator view; final redaction is still applied."""

    report = redact(report)
    summary = report.get("last_24h") or report.get("window_summary") or {}
    secret = (report.get("channels") or {}).get("secret_preview", {})
    stable = (report.get("channels") or {}).get("stable", {})
    consistency = report.get("consistency") or {}
    counts = summary.get("generated_totals") or {}
    lines = [
        f"Static-site builds ({report.get('window', {}).get('hours', '?')}h)",
        (
            f"requests={summary.get('requests', 0)} success={summary.get('succeeded', 0)} "
            f"failed={summary.get('failed', 0)} noop={summary.get('noop', 0)}"
        ),
        "generated " + " ".join(f"{key}={counts.get(key, 0)}" for key in COUNT_KEYS),
        (
            f"secret_pointer={secret.get('current_pointer', {}).get('status', 'absent')} "
            f"stable_pointer={stable.get('current_pointer', {}).get('status', 'absent')}"
        ),
        f"consistency={consistency.get('status', 'unknown')} issues={consistency.get('issue_count', 0)}",
    ]
    for issue in consistency.get("issues", [])[:20]:
        suffix = f" run_id={issue['run_id']}" if issue.get("run_id") else ""
        lines.append(f"- {issue.get('severity', 'warning')} {issue.get('code', 'unknown')}{suffix}")
    return _redact_string("\n".join(lines))
