#!/usr/bin/env python3
"""Read-only, in-container Smart Update production health auditor.

This module deliberately depends only on the Python standard library.  It does
not import the application (whose imports may start clients or workers), never
writes on Fly, and opens the core database through SQLite's immutable access
boundary ``file:/data/db.sqlite?mode=ro``.

The sole stdout record is a sentinel followed by a base64 encoded JSON envelope.
The workflow materializes the already-sanitized files outside the production
machine.
"""

from __future__ import annotations

import argparse
import base64
import collections
import datetime as dt
import glob
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SENTINEL = "SMART_UPDATE_AUDIT_BUNDLE_V1:"
DB_URI = "file:/data/db.sqlite?mode=ro"
EVIDENCE_FILES = (
    "manifest.json",
    "run.json",
    "metrics.json",
    "findings.json",
    "samples.jsonl",
    "sanitized-runtime-excerpts.log",
    "redaction-audit.json",
    "qa-summary.json",
    "smart-update-prod-audit.md",
)
PUBLIC_EVENT_FIELDS = frozenset(
    {
        "title", "description", "short_description", "search_digest", "date",
        "end_date", "time", "location_name", "location_address", "city",
        "ticket_link", "ticket_status", "is_free", "price", "age_restriction",
        "topics", "photo_urls", "festival", "festival_series",
    }
)
TERMINAL_KAGGLE = frozenset(
    {"done", "complete", "completed", "success", "succeeded", "failed", "error", "cancelled", "canceled"}
)
JOB_MAX_RUNTIME_SECONDS = {
    "event_media_review": 180,
    "telegraph_build": 900,
    "vk_sync": 900,
    "tg_event_publish": 180,
    "tg_premium_emoji_edit": 180,
    "ics_publish": 60,
    "tg_ics_post": 60,
    "month_pages": 180,
    "week_pages": 180,
    "weekend_pages": 180,
    "event_vector_sync": 7200,
    "static_site_build": 5400,
    "event_age_bge_assessment": 4200,
    "interest_club_relation": 600,
}
DEFAULT_JOB_MAX_RUNTIME_SECONDS = 900


def utc_iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        # Common logging formatter: ``2026-08-04 12:34:56,123``.
        try:
            parsed = dt.datetime.strptime(text[:23], "%Y-%m-%d %H:%M:%S,%f")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def stable_alias(kind: str, value: Any, length: int = 16) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    digest = hashlib.sha256(("smart-update-prod-audit-v1\0" + kind + "\0" + text).encode()).hexdigest()
    return f"{kind}_{digest[:length]}"


def safe_token(value: Any, *, default: str = "unknown", max_len: int = 80) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return default
    text = re.sub(r"[^a-z0-9_.:-]+", "_", text)[:max_len].strip("_")
    return text or default


def json_text(value: Any, *, pretty: bool = True) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    ) + ("\n" if pretty else "")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def add_gap(gaps: list[dict[str, str]], area: str, code: str) -> None:
    item = {"area": safe_token(area), "code": safe_token(code)}
    if item not in gaps:
        gaps.append(item)


class QueryRecorder:
    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []

    def execute(self, connection: sqlite3.Connection, label: str, sql: str, params: Sequence[Any] = ()):
        normalized = " ".join(sql.split())
        self.items.append({"engine": "sqlite", "label": label, "statement": normalized})
        return connection.execute(sql, params)


def sqlite_inventory(connection: sqlite3.Connection, qr: QueryRecorder) -> tuple[list[dict[str, Any]], dict[str, set[str]], str]:
    names = [
        str(row[0]) for row in qr.execute(
            connection,
            "schema.table_inventory",
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        ).fetchall()
    ]
    inventory: list[dict[str, Any]] = []
    columns: dict[str, set[str]] = {}
    for name in names:
        # Names originate in sqlite_master, not external input; quote defensively.
        quoted = '"' + name.replace('"', '""') + '"'
        rows = qr.execute(connection, f"schema.columns.{safe_token(name)}", f"PRAGMA table_info({quoted})").fetchall()
        cols = [
            {"name": str(r[1]), "type": str(r[2] or ""), "not_null": bool(r[3]), "pk": int(r[5] or 0)}
            for r in rows
        ]
        columns[name] = {c["name"] for c in cols}
        inventory.append({"name": name, "columns": cols})
    schema_hash = sha256_text(json_text(inventory, pretty=False))
    return inventory, columns, schema_hash


def chunks(values: Sequence[int], size: int = 500) -> Iterable[Sequence[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def extract_changed_fields(payload: Any) -> set[str]:
    result: set[str] = set()
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            return result
    if not isinstance(payload, Mapping):
        return result
    for key in ("changed_fields", "changed_field_names", "updated_fields", "mutations", "field_changes"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            candidates = value.keys()
        elif isinstance(value, list):
            candidates = value
        else:
            continue
        for candidate in candidates:
            field = str(candidate).strip()
            if field in PUBLIC_EVENT_FIELDS:
                result.add(field)
    # Recurse only through shallow, known decision containers.
    for key in ("decision", "result", "identity", "update"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            result.update(extract_changed_fields(nested))
    return result


def identity_bucket(decision: Any) -> str:
    value = safe_token(decision)
    if any(x in value for x in ("pending_review", "manual_review", "review", "ambiguous")):
        return "review"
    if any(x in value for x in ("reject", "veto", "skip")):
        return "reject"
    if "conflict" in value:
        return "conflict"
    if any(x in value for x in ("merge", "match", "existing")):
        return "merge"
    if any(x in value for x in ("create", "new")):
        return "create"
    return "other"


def safe_error_signature(value: Any) -> str:
    text = str(value or "")
    cls = re.search(r"\b([A-Z][A-Za-z0-9_]{1,60}(?:Error|Exception|Timeout))\b", text)
    category = cls.group(1) if cls else "Error"
    normalized = re.sub(r"https?://\S+|\b\d{3,}\b|[0-9a-fA-F-]{16,}", "#", text.lower())
    return f"{category}:{stable_alias('err', normalized) or 'err_unknown'}"


def _count_nested_statuses(value: Any, counter: collections.Counter[str]) -> None:
    if isinstance(value, str):
        status = safe_token(value)
        if any(mark in status for mark in ("pending", "running", "queued", "error", "failed")):
            counter[status] += 1
    elif isinstance(value, Mapping):
        for key, child in value.items():
            if safe_token(key) in {"status", "state", "result_status", "error"}:
                _count_nested_statuses(child, counter)
            elif isinstance(child, (Mapping, list)):
                _count_nested_statuses(child, counter)
    elif isinstance(value, list):
        for child in value:
            _count_nested_statuses(child, counter)


def collect_database(
    start: dt.datetime,
    end: dt.datetime,
    gaps: list[dict[str, str]],
    *,
    db_uri: str = DB_URI,
) -> tuple[dict[str, Any], dict[int, dict[str, Any]], dict[str, Any], list[dict[str, str]], bool]:
    metrics: dict[str, Any] = {}
    sample_state: dict[int, dict[str, Any]] = {}
    manifest_db: dict[str, Any] = {"uri": DB_URI, "query_only": True, "consistent_read_transaction": True}
    qr = QueryRecorder()
    available = False
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(db_uri, uri=True, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        quick = [str(r[0]) for r in qr.execute(connection, "integrity.quick_check", "PRAGMA quick_check").fetchall()]
        metrics["quick_check"] = quick
        available = True
        inventory, columns, schema_hash = sqlite_inventory(connection, qr)
        manifest_db.update({"quick_check": quick, "schema_hash": schema_hash, "table_inventory": inventory})
        start_s, end_s = utc_iso(start), utc_iso(end)

        if "event_source" not in columns or not {"id", "event_id", "source_type", "source_url", "imported_at"}.issubset(columns["event_source"]):
            add_gap(gaps, "database", "event_source_schema_missing")
            import_rows: list[sqlite3.Row] = []
        else:
            import_rows = qr.execute(
                connection,
                "event_source.imports_in_window",
                "SELECT id,event_id,source_type,source_url,imported_at FROM event_source WHERE julianday(imported_at)>=julianday(?) AND julianday(imported_at)<julianday(?) ORDER BY julianday(imported_at),id LIMIT 50001",
                (start_s, end_s),
            ).fetchall()
            if len(import_rows) > 50000:
                add_gap(gaps, "database", "event_source_window_truncated")
                import_rows = import_rows[:50000]
        imports_by_source = collections.Counter(safe_token(r["source_type"]) for r in import_rows)
        touched = sorted({int(r["event_id"]) for r in import_rows})
        metrics["event_source"] = {
            "imports_by_source_type": dict(sorted(imports_by_source.items())),
            "imports_total": len(import_rows),
            "unique_touched_event_id": len(touched),
        }
        for row in import_rows:
            event_id = int(row["event_id"])
            state = sample_state.setdefault(event_id, {"source_types": set(), "source_url_hashes": set(), "changed_fields": set()})
            state["source_types"].add(safe_token(row["source_type"]))
            alias = stable_alias("url", row["source_url"])
            if alias:
                state["source_url_hashes"].add(alias)

        first_import: dict[int, dt.datetime | None] = {}
        if touched:
            for group in chunks(touched):
                marks = ",".join("?" for _ in group)
                rows = qr.execute(
                    connection,
                    "event_source.first_imported_at_for_touched",
                    f"SELECT event_id,MIN(imported_at) AS first_imported_at FROM event_source WHERE event_id IN ({marks}) GROUP BY event_id",
                    tuple(group),
                ).fetchall()
                for row in rows:
                    first_import[int(row["event_id"])] = parse_utc(row["first_imported_at"])
        lifecycle = collections.Counter()
        for event_id in touched:
            first = first_import.get(event_id)
            decision = "create" if first is not None and start <= first < end else "merge_existing"
            lifecycle[decision] += 1
            sample_state[event_id]["decision"] = decision
            sample_state[event_id]["lifecycle_status"] = decision
        metrics["event_source"]["create_vs_merge_existing_by_first_imported_at"] = dict(sorted(lifecycle.items()))

        fact_counts: collections.Counter[str] = collections.Counter()
        if "event_source_fact" in columns and {"source_id", "status"}.issubset(columns["event_source_fact"]):
            source_ids = sorted({int(r["id"]) for r in import_rows})
            for group in chunks(source_ids):
                marks = ",".join("?" for _ in group)
                for row in qr.execute(
                    connection,
                    "event_source_fact.status_for_window_sources",
                    f"SELECT status,COUNT(*) AS n FROM event_source_fact WHERE source_id IN ({marks}) GROUP BY status",
                    tuple(group),
                ).fetchall():
                    fact_counts[safe_token(row["status"])] += int(row["n"])
        else:
            add_gap(gaps, "database", "event_source_fact_schema_missing")
        metrics["event_source_fact"] = {"by_status": dict(sorted(fact_counts.items())), "total": sum(fact_counts.values())}

        identity_counts: collections.Counter[str] = collections.Counter()
        critical_false_merges = 0
        ambiguous_auto_merges = 0
        if "event_identity_decision_log" in columns:
            required = {"event_id", "decision", "created_at"}
            if required.issubset(columns["event_identity_decision_log"]):
                select_cols = ["event_id", "decision"]
                for optional in ("source_type", "source_url", "decision_reason", "confidence", "decision_payload"):
                    if optional in columns["event_identity_decision_log"]:
                        select_cols.append(optional)
                rows = qr.execute(
                    connection,
                    "identity.decisions_in_window",
                    f"SELECT {','.join(select_cols)} FROM event_identity_decision_log WHERE julianday(created_at)>=julianday(?) AND julianday(created_at)<julianday(?) ORDER BY julianday(created_at) LIMIT 50001",
                    (start_s, end_s),
                ).fetchall()
                if len(rows) > 50000:
                    add_gap(gaps, "database", "identity_window_truncated")
                    rows = rows[:50000]
                for row in rows:
                    decision = str(row["decision"] or "")
                    bucket = identity_bucket(decision)
                    identity_counts[bucket] += 1
                    event_id = int(row["event_id"]) if row["event_id"] is not None else None
                    if event_id is not None:
                        state = sample_state.setdefault(event_id, {"source_types": set(), "source_url_hashes": set(), "changed_fields": set()})
                        state["identity_status"] = bucket
                        if "source_type" in row.keys() and row["source_type"]:
                            state["source_types"].add(safe_token(row["source_type"]))
                        if "source_url" in row.keys() and row["source_url"]:
                            state["source_url_hashes"].add(stable_alias("url", row["source_url"]))
                        if "decision_payload" in row.keys():
                            state["changed_fields"].update(extract_changed_fields(row["decision_payload"]))
                    evidence = " ".join(
                        str(row[key] or "") for key in ("decision", "decision_reason", "decision_payload") if key in row.keys()
                    ).lower()
                    is_merge = bucket == "merge"
                    if is_merge and any(x in evidence for x in ("critical_false_merge", "false_merge", "hard_veto", "anchor_conflict", "identity_conflict")):
                        critical_false_merges += 1
                    if is_merge and any(x in evidence for x in ("ambiguous", "ambiguity", "uncertain", "pending_review")):
                        ambiguous_auto_merges += 1
            else:
                add_gap(gaps, "database", "identity_decision_columns_missing")
        metrics["identity"] = {
            "table_present": "event_identity_decision_log" in columns,
            "by_outcome": {name: int(identity_counts.get(name, 0)) for name in ("create", "merge", "review", "reject", "conflict", "other")},
            "critical_false_merge_evidence": critical_false_merges,
            "ambiguous_auto_merge_evidence": ambiguous_auto_merges,
        }

        changed = {str(event_id): sorted(state.get("changed_fields", set())) for event_id, state in sample_state.items() if state.get("changed_fields")}
        metrics["public_field_changes"] = {"events_with_evidence": len(changed), "by_field": dict(sorted(collections.Counter(f for fields in changed.values() for f in fields).items()))}

        outbox_rows: list[sqlite3.Row] = []
        if "joboutbox" in columns and {"event_id", "task", "status"}.issubset(columns["joboutbox"]):
            select_cols = [c for c in ("event_id", "task", "status", "attempts", "last_error", "updated_at", "next_run_at") if c in columns["joboutbox"]]
            for group in chunks(touched):
                marks = ",".join("?" for _ in group)
                outbox_rows.extend(qr.execute(connection, "joboutbox.for_touched_events", f"SELECT {','.join(select_cols)} FROM joboutbox WHERE event_id IN ({marks})", tuple(group)).fetchall())
        else:
            add_gap(gaps, "database", "joboutbox_schema_missing")
        grouped = collections.Counter()
        attempt_groups = collections.Counter()
        errors = collections.Counter()
        stale = collections.Counter()
        for row in outbox_rows:
            task, status = safe_token(row["task"]), safe_token(row["status"])
            attempts = int(row["attempts"] or 0) if "attempts" in row.keys() else 0
            grouped[(task, status)] += 1
            attempt_groups[(task, status, str(attempts))] += 1
            if "last_error" in row.keys() and row["last_error"]:
                errors[(task, safe_error_signature(row["last_error"]))] += 1
            updated = parse_utc(row["updated_at"]) if "updated_at" in row.keys() else None
            next_run = parse_utc(row["next_run_at"]) if "next_run_at" in row.keys() else None
            runtime = JOB_MAX_RUNTIME_SECONDS.get(task, DEFAULT_JOB_MAX_RUNTIME_SECONDS)
            if status == "pending" and next_run and next_run < end:
                stale[(task, "pending_due")] += 1
            elif status == "running" and updated and (end - updated).total_seconds() > runtime:
                stale[(task, "running_over_max_runtime")] += 1
            elif status == "error" and next_run and next_run < end:
                stale[(task, "error_due_for_retry")] += 1
        metrics["joboutbox"] = {
            "by_task_status": [{"task": k[0], "status": k[1], "count": n} for k, n in sorted(grouped.items())],
            "by_task_status_attempts": [{"task": k[0], "status": k[1], "attempts": int(k[2]), "count": n} for k, n in sorted(attempt_groups.items())],
            "stale": [{"task": k[0], "kind": k[1], "count": n} for k, n in sorted(stale.items())],
            "redacted_last_error_groups": [{"task": k[0], "signature": k[1], "count": n} for k, n in sorted(errors.items())],
            "max_runtime_seconds": dict(sorted(JOB_MAX_RUNTIME_SECONDS.items())),
        }

        domain: dict[str, Any] = {}
        for name, task_names in {
            "media": {"event_media_review"},
            "age": {"event_age_bge_assessment"},
            "collection": {"month_pages", "week_pages", "weekend_pages", "festival_pages", "interest_club_relation"},
        }.items():
            counter = collections.Counter(safe_token(r["status"]) for r in outbox_rows if safe_token(r["task"]) in task_names)
            domain[name] = {"outbox_by_status": dict(sorted(counter.items()))}
        domain["facts"] = {"by_status": dict(sorted(fact_counts.items()))}

        if touched and "event" in columns:
            status_cols = [c for c in ("age_restriction_status", "age_assessment_status", "collection_decisions") if c in columns["event"]]
            for group in chunks(touched):
                marks = ",".join("?" for _ in group)
                if status_cols:
                    rows = qr.execute(connection, "event.safe_lifecycle_status_for_touched", f"SELECT id,{','.join(status_cols)} FROM event WHERE id IN ({marks})", tuple(group)).fetchall()
                    for row in rows:
                        event_id = int(row["id"])
                        state = sample_state.setdefault(event_id, {"source_types": set(), "source_url_hashes": set(), "changed_fields": set()})
                        lifecycle_parts = []
                        for col in ("age_restriction_status", "age_assessment_status"):
                            if col in row.keys():
                                value = safe_token(row[col])
                                lifecycle_parts.append(f"{col}:{value}")
                        if lifecycle_parts:
                            state["lifecycle_status"] = ",".join(lifecycle_parts)
                        if "age_assessment_status" in row.keys():
                            domain["age"].setdefault("event_by_status", collections.Counter())[safe_token(row["age_assessment_status"])] += 1
                        if "collection_decisions" in row.keys() and row["collection_decisions"]:
                            try:
                                value = json.loads(row["collection_decisions"]) if isinstance(row["collection_decisions"], str) else row["collection_decisions"]
                            except (TypeError, ValueError, json.JSONDecodeError):
                                value = None
                            counter = domain["collection"].setdefault("decision_statuses", collections.Counter())
                            _count_nested_statuses(value, counter)
        for values in domain.values():
            for key, value in list(values.items()):
                if isinstance(value, collections.Counter):
                    values[key] = dict(sorted(value.items()))

        if "event_media_pair_review" in columns and {"event_id", "status"}.issubset(columns["event_media_pair_review"]):
            counter = collections.Counter()
            for group in chunks(touched):
                marks = ",".join("?" for _ in group)
                for row in qr.execute(connection, "media_pair_review.for_touched", f"SELECT status,COUNT(*) AS n FROM event_media_pair_review WHERE event_id IN ({marks}) GROUP BY status", tuple(group)).fetchall():
                    counter[safe_token(row["status"])] += int(row["n"])
            domain["media"]["pair_review_by_status"] = dict(sorted(counter.items()))
        metrics["touched_event_downstream_domains"] = domain

        static: dict[str, Any] = {"state_table_present": "static_site_build_state" in columns}
        active_run_ids: list[str] = []
        if "static_site_build_state" in columns:
            safe_cols = [c for c in ("release_channel", "schema_version", "last_success_at", "active_job_id", "active_run_id", "active_claimed_at", "updated_at") if c in columns["static_site_build_state"]]
            rows = qr.execute(connection, "static_site.state", f"SELECT {','.join(safe_cols)} FROM static_site_build_state ORDER BY release_channel").fetchall()
            states = []
            latest_import = max((parse_utc(r["imported_at"]) for r in import_rows), default=None)
            for row in rows:
                last_success = parse_utc(row["last_success_at"]) if "last_success_at" in row.keys() else None
                raw_active_run = str(row["active_run_id"] or "").strip() if "active_run_id" in row.keys() else ""
                if raw_active_run:
                    active_run_ids.append(raw_active_run)
                states.append({
                    "release_channel": safe_token(row["release_channel"]),
                    "schema_version": safe_token(row["schema_version"]) if "schema_version" in row.keys() else None,
                    "last_success_at": utc_iso(last_success) if last_success else None,
                    "active_job_id": int(row["active_job_id"]) if "active_job_id" in row.keys() and row["active_job_id"] is not None else None,
                    "active_run_alias": stable_alias("run", raw_active_run),
                    "active_claimed_at": utc_iso(parse_utc(row["active_claimed_at"])) if "active_claimed_at" in row.keys() and parse_utc(row["active_claimed_at"]) else None,
                    "candidate_lag_seconds": max(0, int((latest_import - last_success).total_seconds())) if latest_import and last_success else None,
                })
            static["states"] = states
        else:
            add_gap(gaps, "database", "static_site_build_state_missing")
        active_static_jobs = [r for r in outbox_rows if safe_token(r["task"]) == "static_site_build" and safe_token(r["status"]) in {"pending", "running"}]
        static["active_jobs_for_touched_events"] = len(active_static_jobs)
        metrics["static_site_build"] = static

        ledger: dict[str, Any] = {"table_present": "kaggle_run_ledger" in columns}
        if "kaggle_run_ledger" in columns and {"status", "created_at"}.issubset(columns["kaggle_run_ledger"]):
            safe_ledger_cols = [c for c in ("status", "phase", "updated_at", "last_heartbeat_at", "terminal_at") if c in columns["kaggle_run_ledger"]]
            rows = qr.execute(connection, "kaggle.run_ledger_window", f"SELECT {','.join(safe_ledger_cols)} FROM kaggle_run_ledger WHERE julianday(created_at)>=julianday(?) AND julianday(created_at)<julianday(?)", (start_s, end_s)).fetchall()
            status_counts = collections.Counter(safe_token(r["status"]) for r in rows)
            terminal = sum(n for status, n in status_counts.items() if status in TERMINAL_KAGGLE)
            ledger.update({"by_status": dict(sorted(status_counts.items())), "terminal": terminal, "nonterminal": len(rows) - terminal})
            remote_handoffs = []
            if active_run_ids and "run_id" in columns["kaggle_run_ledger"]:
                for group in [active_run_ids[i : i + 100] for i in range(0, len(active_run_ids), 100)]:
                    marks = ",".join("?" for _ in group)
                    remote_cols = [c for c in ("run_id", "status", "phase", "updated_at", "last_heartbeat_at", "terminal_at") if c in columns["kaggle_run_ledger"]]
                    for remote in qr.execute(connection, "kaggle.active_static_remote_handoff", f"SELECT {','.join(remote_cols)} FROM kaggle_run_ledger WHERE run_id IN ({marks})", tuple(group)).fetchall():
                        remote_handoffs.append({
                            "run_alias": stable_alias("run", remote["run_id"]),
                            "status": safe_token(remote["status"]) if "status" in remote.keys() else "unknown",
                            "phase": safe_token(remote["phase"]) if "phase" in remote.keys() else "unknown",
                            "terminal": bool(("terminal_at" in remote.keys() and remote["terminal_at"]) or ("status" in remote.keys() and safe_token(remote["status"]) in TERMINAL_KAGGLE)),
                            "last_heartbeat_at": utc_iso(parse_utc(remote["last_heartbeat_at"])) if "last_heartbeat_at" in remote.keys() and parse_utc(remote["last_heartbeat_at"]) else None,
                        })
            ledger["remote_handoff"] = {
                "active_state_run_count": len(active_run_ids),
                "ledger_match_count": len(remote_handoffs),
                "runs": remote_handoffs,
            }
        else:
            add_gap(gaps, "database", "kaggle_run_ledger_missing")
        metrics["kaggle_run_ledger"] = ledger
    except (sqlite3.Error, OSError) as exc:
        metrics["observer_error"] = safe_error_signature(exc)
        add_gap(gaps, "database", "observer_access_failed")
        available = False
    finally:
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
            connection.close()
    return metrics, sample_state, manifest_db, qr.items, available


LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?)")
EVENT_ID_RE = re.compile(r"\bevent_id[=: ]+(\d{1,12})\b", re.I)


def parse_log_timestamp(line: str) -> dt.datetime | None:
    match = LOG_TS_RE.match(line)
    return parse_utc(match.group(1).replace(",", ".")) if match else None


def collect_runtime_logs(start: dt.datetime, end: dt.datetime, gaps: list[dict[str, str]]) -> tuple[dict[str, Any], str, bool]:
    log_dir = os.environ.get("RUNTIME_LOG_DIR") or "/data/runtime_logs"
    basename = os.environ.get("RUNTIME_LOG_BASENAME") or "events-bot.log"
    paths = sorted(glob.glob(os.path.join(log_dir, basename + "*")))
    inventory = []
    for path in paths:
        try:
            stat = os.stat(path)
        except OSError:
            continue
        inventory.append({"name": os.path.basename(path), "size_bytes": stat.st_size, "mtime_utc": utc_iso(dt.datetime.fromtimestamp(stat.st_mtime, dt.timezone.utc))})
    counters: collections.Counter[str] = collections.Counter()
    outcomes: collections.Counter[str] = collections.Counter()
    exceptions: collections.Counter[tuple[str, str]] = collections.Counter()
    downstream: collections.Counter[tuple[str, str]] = collections.Counter()
    correlations: set[str] = set()
    touched_ids: set[int] = set()
    mutations: collections.Counter[tuple[int, str]] = collections.Counter()
    event_outcomes: dict[int, str] = {}
    event_source_types: dict[int, str] = {}
    excerpts: list[str] = []
    parsed_in_window = 0
    earliest: dt.datetime | None = None
    latest: dt.datetime | None = None
    for path in paths:
        try:
            handle = open(path, "r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        with handle:
            for raw in handle:
                line = raw[:65536]
                timestamp = parse_log_timestamp(line)
                if timestamp is None or not (start <= timestamp < end):
                    continue
                parsed_in_window += 1
                earliest = timestamp if earliest is None or timestamp < earliest else earliest
                latest = timestamp if latest is None or timestamp > latest else latest
                lower = line.lower()
                if "smart_update" not in lower and not any(x in lower for x in ("outbox", " enq ", "enqueue")):
                    continue
                event_match = EVENT_ID_RE.search(line)
                event_id = int(event_match.group(1)) if event_match else None
                if event_id is not None:
                    touched_ids.add(event_id)
                source_match = re.search(r"\bsource_type[=: ]+([a-zA-Z0-9_.:-]{1,40})", line)
                source_type = safe_token(source_match.group(1)) if source_match else "unknown"
                if event_id is not None and source_type != "unknown":
                    event_source_types[event_id] = source_type
                corr_match = re.search(r"\b(?:correlation_id|request_uid|run_id|trace_id)[=: ]+([a-zA-Z0-9_.:-]{6,128})", line, re.I)
                corr = stable_alias("corr", corr_match.group(1)) if corr_match else None
                if corr:
                    correlations.add(corr)
                kind: str | None = None
                if "smart_update.start" in lower:
                    counters["starts"] += 1; kind = "smart_update.start"
                terminal_map = {
                    "created": ("smart_update.created", "status=created"),
                    "merged": ("status=merged",),
                    "no_op": ("smart_update.no_op", "smart_update.no-op", "status=no_op", "status=no-op", "status=skipped_nochange"),
                    "reject": ("smart_update.rejected", "smart_update.invalid", "smart_update.skip", "status=rejected"),
                    "pending_review": ("pending_review", "pending-review"),
                }
                for outcome, needles in terminal_map.items():
                    if any(n in lower for n in needles):
                        outcomes[outcome] += 1
                        kind = f"smart_update.terminal.{outcome}"
                        if event_id is not None:
                            event_outcomes[event_id] = outcome
                        break
                if "retry" in lower or "attempt=" in lower:
                    counters["retries"] += 1
                if "timeout" in lower or "timed out" in lower:
                    counters["timeouts"] += 1
                if "anchor_conflict" in lower or "anchor conflict" in lower:
                    counters["anchor_conflicts"] += 1
                if "hard_veto" in lower or "hard veto" in lower:
                    counters["hard_veto"] += 1
                if "exact_packet" in lower or "exact-packet" in lower or "warm_replay" in lower or "warm replay" in lower:
                    counters["exact_packet_replay"] += 1
                    if event_id is not None:
                        counters[f"warm_event:{event_id}"] += 1
                changed_on_line: set[str] = set()
                field_match = re.search(r"\b(?:changed_field|field)[=: ]+([a-z_]{2,40})", lower)
                if field_match and field_match.group(1) in PUBLIC_EVENT_FIELDS:
                    changed_on_line.add(field_match.group(1))
                keys_match = re.search(r"\bupdated_keys=([a-z_,]{1,400})", lower)
                if keys_match:
                    changed_on_line.update(key for key in keys_match.group(1).split(",") if key in PUBLIC_EVENT_FIELDS)
                if event_id is not None:
                    for field in changed_on_line:
                        mutations[(event_id, field)] += 1
                cls_match = re.search(r"\b([A-Z][A-Za-z0-9_]{1,60}(?:Error|Exception|Timeout))\b", line)
                stage_match = re.search(r"\b(?:stage|label)[=: ]+([a-zA-Z0-9_.:-]{1,80})", line)
                if cls_match:
                    stage = safe_token(stage_match.group(1)) if stage_match else "unknown"
                    exceptions[(cls_match.group(1), stage)] += 1
                    kind = "smart_update.exception"
                if any(x in lower for x in ("outbox", "enqueue", " enq ")):
                    task_match = re.search(r"\btask[=: ]+([a-zA-Z0-9_.:-]{1,60})", line)
                    task = safe_token(task_match.group(1)) if task_match else "unknown"
                    state = "enqueue" if any(x in lower for x in ("enqueue", " enq ", "queued")) else (
                        "terminal_error" if "error" in lower or "failed" in lower else "terminal_done" if "done" in lower or "success" in lower else "observed"
                    )
                    downstream[(task, state)] += 1
                if kind and len(excerpts) < 200:
                    parts = [utc_iso(timestamp), kind, f"source_type={source_type}"]
                    if event_id is not None:
                        parts.append(f"event_id={event_id}")
                    if corr:
                        parts.append(f"correlation={corr}")
                    if cls_match:
                        parts.append(f"exception_class={cls_match.group(1)}")
                    if stage_match:
                        parts.append(f"stage={safe_token(stage_match.group(1))}")
                    excerpts.append(" ".join(parts))
    repeated = sum(1 for count in mutations.values() if count > 1)
    counters["repeated_prose_mutations"] = repeated
    available = bool(inventory and parsed_in_window)
    if not inventory:
        add_gap(gaps, "runtime_logs", "events_bot_log_files_missing")
    elif not parsed_in_window:
        add_gap(gaps, "runtime_logs", "no_parseable_records_in_exact_window")
    metrics = {
        "inventory": inventory,
        "window_records": parsed_in_window,
        "observed_first_utc": utc_iso(earliest) if earliest else None,
        "observed_last_utc": utc_iso(latest) if latest else None,
        "smart_update": {
            "starts": counters["starts"],
            "terminal_outcomes": {name: int(outcomes.get(name, 0)) for name in ("created", "merged", "no_op", "reject", "pending_review")},
            "correlation_aliases": sorted(correlations),
            "exceptions_by_class_stage": [{"exception_class": key[0], "stage": key[1], "count": count} for key, count in sorted(exceptions.items())],
            "retries": counters["retries"], "timeouts": counters["timeouts"],
            "anchor_conflicts": counters["anchor_conflicts"], "hard_veto": counters["hard_veto"],
            "exact_packet_replay": counters["exact_packet_replay"],
            "repeated_prose_mutations": counters["repeated_prose_mutations"],
        },
        "downstream_for_observed_event_ids": [{"task": key[0], "state": key[1], "count": count} for key, count in sorted(downstream.items())],
        "observed_touched_event_id_count": len(touched_ids),
        "warm_replay_event_ids": sorted(int(key.split(":", 1)[1]) for key in counters if key.startswith("warm_event:")),
        "public_field_changes_by_event": [
            {
                "event_id": event_id,
                "changed_field_names": sorted(
                    field for (candidate, field), count in mutations.items()
                    if candidate == event_id and count
                ),
            }
            for event_id in sorted({candidate for candidate, _field in mutations})
        ],
        "safe_event_evidence": [
            {
                "event_id": event_id,
                "decision": event_outcomes.get(event_id, "observed"),
                "source_type": event_source_types.get(event_id, "unknown"),
            }
            for event_id in sorted(touched_ids)
        ],
    }
    return metrics, ("\n".join(excerpts) + ("\n" if excerpts else "")), available


def command_capacity(gaps: list[dict[str, str]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, argv in (("df", ["df", "-Pk", "/data"]), ("du", ["du", "-sk", "/data"])):
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, timeout=30, check=False)
            lines = proc.stdout.strip().splitlines()
            if proc.returncode != 0 or not lines:
                raise OSError(f"{name}_failed")
            if name == "df" and len(lines) >= 2:
                fields = lines[-1].split()
                result[name] = {"blocks_1k": int(fields[-5]), "used_1k": int(fields[-4]), "available_1k": int(fields[-3]), "capacity": fields[-2]}
            elif name == "du":
                result[name] = {"used_1k": int(lines[-1].split()[0])}
        except (OSError, ValueError, subprocess.SubprocessError):
            add_gap(gaps, "capacity", f"{name}_unavailable")
    try:
        vfs = os.statvfs("/data")
        result["statvfs"] = {"total_bytes": vfs.f_blocks * vfs.f_frsize, "free_bytes": vfs.f_bavail * vfs.f_frsize}
    except OSError:
        add_gap(gaps, "capacity", "statvfs_unavailable")
    try:
        result["database_size_bytes"] = os.stat("/data/db.sqlite").st_size
    except OSError:
        result["database_size_bytes"] = None
    return result


def internal_health(gaps: list[dict[str, str]]) -> tuple[dict[str, Any], bool]:
    for url in ("http://127.0.0.1:8080/healthz", "http://127.0.0.1:8000/healthz"):
        body = b""
        status = 0
        try:
            with urllib.request.urlopen(url, timeout=8) as response:
                status = int(response.status)
                body = response.read(262144)
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            body = exc.read(262144)
        except (urllib.error.URLError, TimeoutError, OSError):
            continue
        summary: dict[str, Any] = {"http_status": status}
        try:
            data = json.loads(body)
        except (ValueError, json.JSONDecodeError):
            data = {}
        if isinstance(data, Mapping):
            for key in ("ok", "ready", "status"):
                value = data.get(key)
                if isinstance(value, bool):
                    summary[key] = value
                elif key == "status" and isinstance(value, str):
                    summary[key] = safe_token(value)
            components = data.get("components") or data.get("checks") or data.get("tasks")
            if isinstance(components, Mapping):
                safe_components = {}
                for key, value in components.items():
                    name = safe_token(key)
                    if isinstance(value, bool):
                        safe_components[name] = value
                    elif isinstance(value, Mapping):
                        safe_components[name] = {k: v for k, v in value.items() if k in {"ok", "ready", "alive"} and isinstance(v, bool)}
                    elif isinstance(value, str):
                        safe_components[name] = safe_token(value)
                summary["components"] = safe_components
        ready = status == 200 and summary.get("ready", summary.get("ok", True)) is not False
        return summary, ready
    add_gap(gaps, "internal_health", "healthz_unreachable")
    return {"http_status": None}, False


def deployed_identity(expected_sha: str, gaps: list[dict[str, str]]) -> tuple[dict[str, Any], str | None, bool]:
    candidates: list[tuple[str, str]] = []
    for env_name in ("APP_REPO_SHA", "GIT_SHA", "RELEASE_SHA", "SOURCE_VERSION", "STATIC_SITE_REPO_SHA"):
        value = os.environ.get(env_name, "").strip().lower()
        if value:
            candidates.append((f"env:{env_name}", value))
    configured_revision_path = os.environ.get("STATIC_SITE_IMAGE_REPO_SHA_FILE", "").strip()
    revision_paths = [p for p in (configured_revision_path, "/app/.static-site-repo-sha", "/app/.repo-sha", "/app/REVISION") if p]
    for path in revision_paths:
        try:
            value = Path(path).read_text(encoding="utf-8").strip().lower()
        except OSError:
            continue
        if value:
            candidates.append((f"file:{path}", value))
    exact = next(((source, value) for source, value in candidates if re.fullmatch(r"[0-9a-f]{40}", value)), None)
    sha = exact[1] if exact else None
    if sha is None:
        add_gap(gaps, "exact_deployed_sha", "exact_sha_unavailable")
    identity = {
        "sha_source": exact[0] if exact else None,
        "machine_id": safe_token(os.environ.get("FLY_MACHINE_ID"), default="unavailable", max_len=128),
        "machine_version": safe_token(os.environ.get("FLY_MACHINE_VERSION") or os.environ.get("FLY_IMAGE_VERSION"), default="unavailable", max_len=128),
        "image_identity": safe_token(os.environ.get("FLY_IMAGE_REF") or os.environ.get("FLY_IMAGE"), default="unavailable", max_len=200),
        "region": safe_token(os.environ.get("FLY_REGION"), default="unavailable"),
        "app": safe_token(os.environ.get("FLY_APP_NAME"), default="unavailable"),
        "matches_expected": bool(sha and sha == expected_sha),
    }
    return identity, sha, sha is not None


def postgrest_get(base_url: str, service_key: str, table: str, params: Sequence[tuple[str, str]], *, max_rows: int = 50000) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    page = 1000
    offset = 0
    truncated = False
    while offset < max_rows:
        query = list(params) + [("limit", str(page)), ("offset", str(offset))]
        url = base_url.rstrip("/") + "/rest/v1/" + urllib.parse.quote(table) + "?" + urllib.parse.urlencode(query, safe="(),.*:-_")
        request = urllib.request.Request(url, method="GET", headers={"apikey": service_key, "Authorization": "Bearer " + service_key, "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read(8 * 1024 * 1024))
        if not isinstance(payload, list):
            raise ValueError("postgrest_non_list_response")
        rows.extend(row for row in payload if isinstance(row, dict))
        if len(payload) < page:
            break
        offset += page
    else:
        truncated = True
    return rows[:max_rows], truncated


def collect_limiter(start: dt.datetime, end: dt.datetime, gaps: list[dict[str, str]]) -> tuple[dict[str, Any], bool, list[dict[str, str]]]:
    base_url = os.environ.get("GOOGLE_AI_LIMITER_SUPABASE_URL", "").strip()
    key = os.environ.get("GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", "").strip()
    queries: list[dict[str, str]] = []
    if not base_url or not key:
        add_gap(gaps, "limiter_ledger", "dedicated_env_pair_missing")
        return {"available": False}, False, queries
    if not re.match(r"^https://", base_url, re.I):
        add_gap(gaps, "limiter_ledger", "invalid_https_origin")
        return {"available": False}, False, queries
    start_s, end_s = utc_iso(start), utc_iso(end)
    try:
        request_params = [
            ("select", "request_uid,consumer,model,status,attempts,finalized_at,quota_scope,last_error_kind,last_error_code,meta,created_at"),
            ("created_at", "gte." + start_s), ("created_at", "lt." + end_s), ("order", "created_at.asc"),
        ]
        queries.append({"engine": "postgrest_select", "table": "google_ai_requests", "filter": "created_at in [start_utc,end_utc)"})
        requests, truncated_requests = postgrest_get(base_url, key, "google_ai_requests", request_params)
        attempt_params = [
            ("select", "request_uid,attempt_no,status,blocked_reason,quota_scope,provider_status,provider_error_type,provider_error_code,completed_at,started_at"),
            ("started_at", "gte." + start_s), ("started_at", "lt." + end_s), ("order", "started_at.asc"),
        ]
        queries.append({"engine": "postgrest_select", "table": "google_ai_request_attempts", "filter": "started_at in [start_utc,end_utc)"})
        attempts, truncated_attempts = postgrest_get(base_url, key, "google_ai_request_attempts", attempt_params)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
        add_gap(gaps, "limiter_ledger", "select_access_failed")
        return {"available": False}, False, queries
    if truncated_requests or truncated_attempts:
        add_gap(gaps, "limiter_ledger", "window_rows_truncated")
    request_context_by_uid: dict[str, tuple[str, str, str]] = {}
    request_groups = collections.Counter()
    finalized = collections.Counter()
    scopes: set[str] = set()
    multi_attempt = 0
    unfinalized = 0
    unfinished_by_status: collections.Counter[str] = collections.Counter()
    for row in requests:
        meta = row.get("meta") if isinstance(row.get("meta"), Mapping) else {}
        operation = safe_token(meta.get("operation") or meta.get("label") or meta.get("stage"), default="unspecified")
        uid = str(row.get("request_uid") or "")
        consumer, model, status = safe_token(row.get("consumer")), safe_token(row.get("model")), safe_token(row.get("status"))
        request_context_by_uid[uid] = (consumer, operation, model)
        request_groups[(consumer, operation, model, status)] += 1
        finalized["finalized" if row.get("finalized_at") else "unfinalized"] += 1
        unfinalized += 0 if row.get("finalized_at") else 1
        if not row.get("finalized_at"):
            unfinished_by_status[status] += 1
        scope = stable_alias("scope", row.get("quota_scope"))
        if scope:
            scopes.add(scope)
        if int(row.get("attempts") or 0) > 1:
            multi_attempt += 1
    attempt_groups = collections.Counter()
    failure_kinds = collections.Counter()
    attempts_by_request_scope = collections.Counter()
    for row in attempts:
        uid = str(row.get("request_uid") or "")
        consumer, operation, model = request_context_by_uid.get(uid, ("unknown", "unspecified", "unknown"))
        status = safe_token(row.get("status"))
        scope = stable_alias("scope", row.get("quota_scope")) or "scope_unknown"
        scopes.add(scope)
        attempt_groups[(consumer, operation, model, status)] += 1
        attempts_by_request_scope[(stable_alias("req", uid) or "req_unknown", scope)] += 1
        haystack = " ".join(str(row.get(k) or "") for k in ("status", "blocked_reason", "provider_status", "provider_error_type", "provider_error_code")).lower()
        if "429" in haystack or "resource_exhausted" in haystack:
            failure_kinds["provider_429"] += 1
        if "timeout" in haystack or "deadline" in haystack:
            failure_kinds["timeout"] += 1
        if re.search(r"(?:^|\D)5\d\d(?:\D|$)", haystack):
            failure_kinds["provider_5xx"] += 1
        if any(x in haystack for x in ("admission", "blocked", "quota_exhausted", "rpm_exhausted", "tpm_exhausted", "rpd_exhausted", "cooldown")):
            failure_kinds["admission_denied"] += 1
    logical_multi_same_scope = sum(1 for count in attempts_by_request_scope.values() if count > 1)
    cooldowns: list[dict[str, Any]] = []
    try:
        params = [("select", "quota_scope,model,blocked_until,reason"), ("blocked_until", "gt." + end_s), ("order", "blocked_until.asc")]
        queries.append({"engine": "postgrest_select", "table": "google_ai_provider_cooldowns", "filter": "blocked_until > end_utc"})
        rows, _ = postgrest_get(base_url, key, "google_ai_provider_cooldowns", params, max_rows=1000)
        cooldowns = [{"quota_scope_alias": stable_alias("scope", r.get("quota_scope")), "model": safe_token(r.get("model")), "blocked_until": utc_iso(parse_utc(r.get("blocked_until"))) if parse_utc(r.get("blocked_until")) else None, "reason": safe_token(r.get("reason"))} for r in rows]
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
        add_gap(gaps, "limiter_ledger", "cooldown_select_unavailable")
    metrics = {
        "available": True,
        "requests_total": len(requests),
        "attempts_total": len(attempts),
        "requests_by_consumer_operation_model_status": [{"consumer": k[0], "operation": k[1], "model": k[2], "status": k[3], "count": n} for k, n in sorted(request_groups.items())],
        "attempts_by_consumer_operation_model_status": [{"consumer": k[0], "operation": k[1], "model": k[2], "status": k[3], "count": n} for k, n in sorted(attempt_groups.items())],
        "finalization": {"finalized": finalized["finalized"], "unfinalized": finalized["unfinalized"]},
        "failure_classes": {name: int(failure_kinds.get(name, 0)) for name in ("provider_429", "timeout", "provider_5xx", "admission_denied")},
        "distinct_quota_scopes": len(scopes), "quota_scope_aliases": sorted(scopes),
        "logical_requests_with_multiple_attempts": multi_attempt,
        "logical_requests_with_multiple_physical_attempts_in_one_scope": logical_multi_same_scope,
        "unfinished_reservations": unfinalized,
        "unfinished_reservations_by_status": dict(sorted(unfinished_by_status.items())),
        "active_cooldowns": cooldowns,
        "truncated": truncated_requests or truncated_attempts,
    }
    return metrics, True, queries


def make_samples(sample_state: dict[int, dict[str, Any]], log_metrics: Mapping[str, Any]) -> str:
    warm_ids = set(int(x) for x in log_metrics.get("warm_replay_event_ids", []) if isinstance(x, int))
    candidates = []
    # Deterministic stratification: one pass per decision/source pair, then fill.
    ordered = sorted(sample_state.items(), key=lambda item: (str(item[1].get("decision", "other")), sorted(item[1].get("source_types", {"unknown"}))[0], item[0]))
    seen_strata: set[tuple[str, str]] = set()
    selected: list[tuple[int, dict[str, Any]]] = []
    for event_id, state in ordered:
        source = sorted(x for x in state.get("source_types", set()) if x)[0] if state.get("source_types") else "unknown"
        stratum = (str(state.get("decision", "other")), source)
        if stratum not in seen_strata:
            selected.append((event_id, state)); seen_strata.add(stratum)
        if len(selected) >= 15:
            break
    for item in ordered:
        if item not in selected and len(selected) < 15:
            selected.append(item)
    for event_id in sorted(warm_ids):
        state = sample_state.get(event_id, {"source_types": set(), "source_url_hashes": set(), "changed_fields": set(), "decision": "warm_replay"})
        if all(existing_id != event_id for existing_id, _ in selected) and len(selected) < 20:
            selected.append((event_id, state))
    lines = []
    for event_id, state in selected[:20]:
        source_types = sorted(x for x in state.get("source_types", set()) if x)
        hashes = sorted(x for x in state.get("source_url_hashes", set()) if x)
        lines.append(json.dumps({
            "sample_kind": "exact_warm_replay" if event_id in warm_ids else "stratified_product",
            "event_id": event_id,
            "decision": safe_token(state.get("decision")),
            "changed_field_names": sorted(state.get("changed_fields", set())),
            "source_type": source_types[0] if source_types else "unknown",
            "source_url_hash": hashes[0] if hashes else None,
            "lifecycle_status": safe_token(state.get("lifecycle_status")),
            "identity_status": safe_token(state.get("identity_status")),
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return "\n".join(lines) + ("\n" if lines else "")


def redaction_scan(files: Mapping[str, str]) -> dict[str, Any]:
    patterns = {
        "secret_assignment": re.compile(r"(?i)(?:api[_-]?key|token|secret|authorization|bearer)\s*[=:]\s*[^\s,}\"]{8,}"),
        "jwt": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}\b"),
        "telegram_or_vk_url": re.compile(r"https?://(?:t\.me|telegram\.me|vk\.com)/\S+", re.I),
        "email": re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I),
        "phone": re.compile(r"(?<!\d)(?:\+7|8)[\s()\-]*\d{3}[\s()\-]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}(?!\d)"),
        "prompt_completion_payload": re.compile(r'(?i)"(?:prompt|completion|source_text)"\s*:\s*"[^\"]+"'),
    }
    by_kind = {name: 0 for name in patterns}
    files_with_hits: set[str] = set()
    for filename, content in files.items():
        for name, pattern in patterns.items():
            count = len(pattern.findall(content))
            if count:
                by_kind[name] += count; files_with_hits.add(filename)
    return {
        "policy": "pii_free_sanitized_v1",
        "passed": not files_with_hits,
        "files_scanned": len(files),
        "violations_by_category": by_kind,
        "files_with_violations": sorted(files_with_hits),
        "stable_alias_scheme": "sha256_domain_separated_prefix16",
        "raw_private_payloads_emitted": False,
    }


def emergency_bundle(
    classification: str,
    observer_access: Mapping[str, bool],
    code: str,
    *,
    repo_sha: str | None = None,
    in_container_sha: str | None = None,
) -> dict[str, Any]:
    """Return a contract-valid, content-minimal bundle after an auditor fault."""

    files: dict[str, str] = {
        "run.json": json_text({"classification": classification, "auditor_status": safe_token(code)}),
        "metrics.json": json_text({"metrics_available": False}),
        "findings.json": json_text({"classification": classification, "findings": [{"severity": "FAIL", "code": "auditor_internal_failure"}]}),
        "samples.jsonl": "",
        "sanitized-runtime-excerpts.log": "",
        "qa-summary.json": json_text({"classification": classification, "observer_access": dict(observer_access), "redaction_passed": True}),
        "smart-update-prod-audit.md": f"# Smart Update production audit\n\nClassification: **{classification}**\n\nAuditor status: `{safe_token(code)}`.\n",
    }
    files["redaction-audit.json"] = json_text(redaction_scan(files))
    manifest = {
        "schema": "smart_update_prod_audit_manifest_v1",
        "repo_sha": repo_sha,
        "in_container_sha": in_container_sha,
        "evidence_policy": "restricted",
        "artifact_sha256": {name: sha256_text(content) for name, content in sorted(files.items())},
        "manifest_self_hash": "excluded_self_reference",
    }
    files["manifest.json"] = json_text(manifest)
    return {"classification": classification, "exit_code": 3 if classification == "BLOCKED_OBSERVER_ACCESS" else 2, "files": files}


def parse_public_health(value: str | None) -> dict[str, Any]:
    if not value:
        return {"provided": False, "ok": False, "http_status": None}
    text = value.strip()
    try:
        data = json.loads(base64.b64decode(text, validate=True))
    except (ValueError, json.JSONDecodeError, base64.binascii.Error):
        data = None
    if isinstance(data, Mapping):
        status = data.get("http_status")
        ok = bool(data.get("ok", status == 200))
        return {"provided": True, "ok": ok, "http_status": int(status) if isinstance(status, int) else None}
    token = safe_token(value)
    return {"provided": True, "ok": token in {"ok", "ready", "200", "pass"}, "http_status": 200 if token == "200" else None}


def build_bundle(args: argparse.Namespace) -> dict[str, Any]:
    end = parse_utc(args.end_utc) if args.end_utc else dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    if end is None:
        raise ValueError("end_utc must be an ISO-8601 timestamp with an optional UTC offset")
    start = end - dt.timedelta(hours=args.hours)
    expected_sha = args.expected_repo_sha.lower()
    gaps: list[dict[str, str]] = []
    started_at = dt.datetime.now(dt.timezone.utc)

    identity, in_container_sha, sha_available = deployed_identity(expected_sha, gaps)
    health, health_ready = internal_health(gaps)
    capacity = command_capacity(gaps)
    db_metrics, sample_state, manifest_db, sqlite_queries, db_available = collect_database(start, end, gaps)
    log_metrics, excerpts, logs_available = collect_runtime_logs(start, end, gaps)
    for item in log_metrics.get("safe_event_evidence", []):
        event_id = int(item["event_id"])
        state = sample_state.setdefault(event_id, {"source_types": set(), "source_url_hashes": set(), "changed_fields": set()})
        state.setdefault("decision", safe_token(item.get("decision")))
        if safe_token(item.get("source_type")) != "unknown":
            state["source_types"].add(safe_token(item.get("source_type")))
    for item in log_metrics.get("public_field_changes_by_event", []):
        event_id = int(item["event_id"])
        state = sample_state.setdefault(event_id, {"source_types": set(), "source_url_hashes": set(), "changed_fields": set()})
        state["changed_fields"].update(field for field in item.get("changed_field_names", []) if field in PUBLIC_EVENT_FIELDS)
    limiter_metrics, limiter_available, limiter_queries = collect_limiter(start, end, gaps)
    public_health = parse_public_health(args.public_health_status)
    if not public_health["provided"]:
        add_gap(gaps, "public_health", "workflow_result_not_provided")

    findings: list[dict[str, Any]] = []
    def finding(severity: str, code: str, evidence: Mapping[str, Any] | None = None) -> None:
        findings.append({"severity": severity, "code": code, "evidence": dict(evidence or {})})

    if db_metrics.get("quick_check") not in (["ok"], None):
        finding("FAIL", "sqlite_quick_check_not_ok", {"result_count": len(db_metrics.get("quick_check", []))})
    if sha_available and in_container_sha != expected_sha:
        finding("FAIL", "deployed_sha_mismatch", {"expected": expected_sha, "observed": in_container_sha})
    if not health_ready:
        finding("FAIL", "internal_health_not_ready", {"http_status": health.get("http_status")})
    if public_health["provided"] and not public_health["ok"]:
        finding("FAIL", "public_health_not_ready", {"http_status": public_health.get("http_status")})
    identity_metrics = db_metrics.get("identity", {})
    if identity_metrics.get("critical_false_merge_evidence", 0):
        finding("FAIL", "critical_false_merge_evidence", {"count": identity_metrics["critical_false_merge_evidence"]})
    if identity_metrics.get("ambiguous_auto_merge_evidence", 0):
        finding("FAIL", "ambiguity_auto_merged_instead_of_pending_review", {"count": identity_metrics["ambiguous_auto_merge_evidence"]})
    stale_total = sum(int(item.get("count", 0)) for item in db_metrics.get("joboutbox", {}).get("stale", []))
    if stale_total:
        finding("WATCH", "stale_outbox_work", {"count": stale_total})
    if log_metrics.get("smart_update", {}).get("timeouts", 0):
        finding("WATCH", "smart_update_timeouts_observed", {"count": log_metrics["smart_update"]["timeouts"]})
    if limiter_metrics.get("unfinished_reservations", 0) or limiter_metrics.get("active_cooldowns"):
        finding("WATCH", "limiter_unfinished_or_cooldown", {"unfinished_reservations": limiter_metrics.get("unfinished_reservations", 0), "active_cooldowns": len(limiter_metrics.get("active_cooldowns", []))})
    if gaps:
        finding("WATCH", "observability_gaps", {"count": len(gaps)})

    observer_access = {
        "runtime_logs": bool(logs_available),
        "database": bool(db_available),
        "limiter_ledger": bool(limiter_available),
        "exact_deployed_sha": bool(sha_available),
    }
    if not all(observer_access.values()):
        classification, exit_code = "BLOCKED_OBSERVER_ACCESS", 3
    elif any(item["severity"] == "FAIL" for item in findings):
        classification, exit_code = "FAIL", 2
    elif any(item["severity"] == "WATCH" for item in findings):
        classification, exit_code = "WATCH", 0
    else:
        classification, exit_code = "PASS", 0

    finished_at = dt.datetime.now(dt.timezone.utc)
    run = {
        "schema": "smart_update_prod_audit_run_v1", "classification": classification,
        "started_at": utc_iso(started_at), "finished_at": utc_iso(finished_at),
        "window": {"start_utc": utc_iso(start), "end_utc": utc_iso(end), "hours": args.hours},
        "expected_repo_sha": expected_sha, "public_health": public_health,
    }
    metrics = {
        "schema": "smart_update_prod_audit_metrics_v1", "database": db_metrics,
        "runtime_logs": log_metrics, "google_ai_limiter": limiter_metrics,
        "internal_health": health, "capacity": capacity,
    }
    findings_doc = {"schema": "smart_update_prod_audit_findings_v1", "classification": classification, "findings": findings, "observability_gaps": gaps}
    qa = {
        "schema": "smart_update_prod_audit_qa_v1", "classification": classification,
        "observer_access": observer_access,
        "critical_false_merge_count": int(identity_metrics.get("critical_false_merge_evidence", 0)),
        "ambiguous_auto_merge_count": int(identity_metrics.get("ambiguous_auto_merge_evidence", 0)),
        "ambiguity_gate": "pending_review_required",
        "sample_limit": 20,
    }
    samples = make_samples(sample_state, log_metrics)
    report = (
        "# Smart Update production audit\n\n"
        f"- Classification: **{classification}**\n"
        f"- Tested repository SHA: `{expected_sha}`\n"
        f"- In-container SHA: `{in_container_sha or 'unavailable'}`\n"
        f"- UTC window: `{utc_iso(start)}` — `{utc_iso(end)}`\n"
        f"- Evidence policy: `restricted`\n\n"
        "## Observer access\n\n" + "\n".join(f"- {key}: {'yes' if value else 'no'}" for key, value in observer_access.items()) +
        "\n\n## Findings\n\n" + ("\n".join(f"- {item['severity']}: `{item['code']}`" for item in findings) or "- None.\n") + "\n"
    )
    files: dict[str, str] = {
        "run.json": json_text(run), "metrics.json": json_text(metrics),
        "findings.json": json_text(findings_doc), "samples.jsonl": samples,
        "sanitized-runtime-excerpts.log": excerpts, "qa-summary.json": json_text(qa),
        "smart-update-prod-audit.md": report,
    }
    redaction = redaction_scan(files)
    qa["redaction_passed"] = redaction["passed"]
    files["qa-summary.json"] = json_text(qa)
    files["redaction-audit.json"] = json_text(redaction)
    manifest = {
        "schema": "smart_update_prod_audit_manifest_v1",
        "repo_sha": expected_sha, "in_container_sha": in_container_sha,
        "fly_identity": identity,
        "utc_window": {"start_utc": utc_iso(start), "end_utc": utc_iso(end)},
        "schema_hash": manifest_db.get("schema_hash"),
        "table_inventory": manifest_db.get("table_inventory", []),
        "database_access": {k: manifest_db.get(k) for k in ("uri", "query_only", "consistent_read_transaction", "quick_check")},
        "evidence_policy": "restricted",
        "commands": ["GET public /healthz (workflow evidence)", "GET http://127.0.0.1:{8080,8000}/healthz", "df -Pk /data", "du -sk /data", "sqlite read-only transaction", "PostgREST GET/SELECT dedicated limiter ledger"],
        "queries": sqlite_queries + limiter_queries,
        "artifact_sha256": {name: sha256_text(content) for name, content in sorted(files.items())},
        "manifest_self_hash": "excluded_self_reference",
    }
    files["manifest.json"] = json_text(manifest)
    # The manifest must hash exactly the other eight files, no more and no less.
    assert set(manifest["artifact_sha256"]) == set(EVIDENCE_FILES) - {"manifest.json"}
    assert set(files) == set(EVIDENCE_FILES)
    final_redaction = redaction_scan(files)
    if not final_redaction["passed"]:
        # Fail closed without echoing offending text.  This branch should be
        # unreachable for constructed evidence and makes local misuse obvious.
        return emergency_bundle(
            "FAIL", observer_access, "redaction_gate_failed",
            repo_sha=expected_sha, in_container_sha=in_container_sha,
        )
    return {"classification": classification, "exit_code": exit_code, "files": files}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--hours", type=int, default=24)
    result.add_argument("--end-utc")
    result.add_argument("--expected-repo-sha", required=True)
    result.add_argument("--public-health-status", help="base64 JSON {ok,http_status}, or a safe status token")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not 1 <= args.hours <= 168:
        raise SystemExit("--hours must be between 1 and 168")
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.expected_repo_sha):
        raise SystemExit("--expected-repo-sha must be exactly 40 hexadecimal characters")
    try:
        envelope = build_bundle(args)
    except Exception as exc:  # last-resort sanitized envelope; no traceback/private data
        observer = {"runtime_logs": False, "database": False, "limiter_ledger": False, "exact_deployed_sha": False}
        envelope = emergency_bundle(
            "BLOCKED_OBSERVER_ACCESS", observer, safe_error_signature(exc),
            repo_sha=args.expected_repo_sha.lower(), in_container_sha=None,
        )
    payload = base64.b64encode(json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).decode()
    sys.stdout.write(SENTINEL + payload + "\n")
    # Workflow uses envelope.exit_code after safely materializing the bundle.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
