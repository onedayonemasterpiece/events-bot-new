#!/usr/bin/env python3
"""Requeue Smart Update identity losses without reopening product-policy rejects.

The recovery is intentionally a queue/state transition, not a second identity
implementation.  Durable ``RETRY_SCHEDULED`` candidates remain owned by the
normal Smart Update retry worker; this tool only releases an expired claim.
Legacy Telegram incomplete identity scans are forced into the next monitor
packet, VK ``failed``/due ``deferred`` rows are returned to ``pending``, and
technical ticket/festival queue errors are rearmed.  Official source parsers
are full-catalog producers, so their affected sources are reported for the
ordinary scheduled full refresh rather than fabricating non-existent row
payloads.  Accepted/product-policy terminal rows are never selected.

Output is aggregate-only: source text, URLs, exception messages and payloads
are neither printed nor copied.  Dry-run is the default and opens SQLite in
read-only/query-only mode.  Applying changes requires an explicit ``--apply``.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any, Iterable, Mapping, Sequence


REPORT_SCHEMA = "kenigevents.smart_update_identity_recovery.v2"
DEFAULT_SINCE = "2026-08-04"
MAX_BATCH_SIZE = 10_000


class RecoveryError(RuntimeError):
    """A fail-closed input or schema error safe to expose by reason code."""


def _parse_utc(value: str, *, field: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise RecoveryError(f"invalid_{field}")
    try:
        if len(raw) == 10:
            parsed = datetime.combine(date.fromisoformat(raw), datetime.min.time())
        else:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecoveryError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _render_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _quoted(identifier: str) -> str:
    if not identifier or not identifier.replace("_", "").isalnum():
        raise RecoveryError("unsafe_schema_identifier")
    return f'"{identifier}"'


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(con, table):
        return set()
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info({_quoted(table)})")}


def _first(columns: set[str], candidates: Sequence[str]) -> str | None:
    return next((name for name in candidates if name in columns), None)


def _count(con: sqlite3.Connection, table: str, where: str, params: Iterable[Any]) -> int:
    row = con.execute(
        f"SELECT COUNT(*) FROM {_quoted(table)} WHERE {where}", tuple(params)
    ).fetchone()
    return int((row or (0,))[0] or 0)


def _window_predicate(column: str, *, unix_epoch: bool = False) -> str:
    quoted = _quoted(column)
    if unix_epoch:
        rendered = f"datetime({quoted}, 'unixepoch')"
    else:
        rendered = f"datetime({quoted})"
    return f"{rendered} >= datetime(?) AND {rendered} < datetime(?)"


def _normalized_filters(values: Sequence[str] | None, *, field: str) -> tuple[str, ...]:
    result: list[str] = []
    for value in values or ():
        for item in str(value).split(","):
            clean = item.strip().upper() if field == "loss_class" else item.strip().lower()
            if clean and clean not in result:
                result.append(clean)
    if field == "loss_class":
        known = set("ABCDEFGHIJKLMNOPQRST") | {
            "DISCOVERY_NO_KEYWORDS", "DISCOVERY_NO_DATE", "DISCOVERY_PAST_HINT",
            "DISCOVERY_TOO_FAR_HINT", "DISCOVERY_CURSOR_OR_PAGE_GAP",
            "INBOX_HINT_AUTO_REJECT", "PRE_LLM_HISTORY_PREFILTER",
            "PRE_LLM_ADMIN_PREFILTER", "PRE_LLM_CANCELLATION_SHORT_CIRCUIT",
            "PRE_LLM_PAYLOAD_OR_OCR_FAILURE", "LLM_PROVIDER_OR_SCHEMA_FAILURE",
            "LLM_OUTPUT_TRUNCATION", "EVIDENCE_OMITTED_FROM_PROMPT",
            "POST_LLM_REJECT_REASON_FULL", "POST_LLM_REJECT_REASON_PARTIAL",
            "SMART_UPDATE_IDENTITY_LOSS", "TECHNICAL_TERMINAL_FAILED",
            "VALID_CONFIRMED_NO_EVENT", "EXACT_REPLAY_OR_ALREADY_IMPORTED",
            "UNKNOWN_EVIDENCE_UNAVAILABLE",
        }
        if any(item not in known for item in result):
            raise RecoveryError("invalid_loss_class")
    return tuple(sorted(result))


def _source_enabled(filters: Sequence[str], source: str) -> bool:
    return not filters or source.lower() in filters


def _class_enabled(filters: Sequence[str], *classes: str) -> bool:
    if not filters:
        return True
    codes = "ABCDEFGHIJKLMNOPQRST"
    names = (
        "DISCOVERY_NO_KEYWORDS", "DISCOVERY_NO_DATE", "DISCOVERY_PAST_HINT",
        "DISCOVERY_TOO_FAR_HINT", "DISCOVERY_CURSOR_OR_PAGE_GAP",
        "INBOX_HINT_AUTO_REJECT", "PRE_LLM_HISTORY_PREFILTER",
        "PRE_LLM_ADMIN_PREFILTER", "PRE_LLM_CANCELLATION_SHORT_CIRCUIT",
        "PRE_LLM_PAYLOAD_OR_OCR_FAILURE", "LLM_PROVIDER_OR_SCHEMA_FAILURE",
        "LLM_OUTPUT_TRUNCATION", "EVIDENCE_OMITTED_FROM_PROMPT",
        "POST_LLM_REJECT_REASON_FULL", "POST_LLM_REJECT_REASON_PARTIAL",
        "SMART_UPDATE_IDENTITY_LOSS", "TECHNICAL_TERMINAL_FAILED",
        "VALID_CONFIRMED_NO_EVENT", "EXACT_REPLAY_OR_ALREADY_IMPORTED",
        "UNKNOWN_EVIDENCE_UNAVAILABLE",
    )
    expanded = {value for value in classes}
    for value in tuple(expanded):
        if value in names:
            expanded.add(codes[names.index(value)])
        elif value in codes:
            expanded.add(names[codes.index(value)])
    return bool(set(filters) & expanded)


def _product_policy_predicate(columns: set[str]) -> str:
    evidence_columns = [
        name
        for name in ("error", "last_error", "last_result_json", "decision_reason", "reason")
        if name in columns
    ]
    if not evidence_columns:
        return "0"
    markers = (
        "rejected_product_policy",
        "confirmed_product_reject",
        "product_policy_reject",
    )
    clauses = [
        f"lower(COALESCE({_quoted(column)}, '')) LIKE '%{marker}%'"
        for column in evidence_columns
        for marker in markers
    ]
    return "(" + " OR ".join(clauses) + ")"


def _marker_predicate(columns: set[str], markers: Sequence[str]) -> str:
    evidence_columns = [
        name
        for name in ("error", "last_error", "last_result_json", "decision_reason", "reason")
        if name in columns
    ]
    if not evidence_columns:
        return "0"
    clauses = [
        f"lower(COALESCE({_quoted(column)}, '')) LIKE '%{marker}%'"
        for column in evidence_columns
        for marker in markers
    ]
    return "(" + " OR ".join(clauses) + ")"


def _load_census_module():
    path = Path(__file__).with_name("smart_update_loss_census.py")
    spec = importlib.util.spec_from_file_location("smart_update_loss_census_for_recovery", path)
    if not spec or not spec.loader:
        raise RecoveryError("census_module_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _is_nonempty(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _expired(value: Any, now: datetime) -> bool:
    if not _is_nonempty(value):
        return False
    try:
        parsed = _parse_utc(str(value), field="claim_expiry")
    except RecoveryError:
        # Unknown timestamp encodings are treated as active, never force-cleared.
        return False
    return parsed <= now


def _empty_durable(*, supported: bool) -> dict[str, Any]:
    return {
        "eligible_due": 0,
        "selected": 0,
        "would_requeue": 0,
        "requeued": 0,
        "already_available": 0,
        "active_claims_skipped": 0,
        "schema_supported": supported,
    }


def _recover_durable(
    con: sqlite3.Connection,
    *,
    since: datetime,
    until: datetime,
    now: datetime,
    dry_run: bool,
    limit: int,
) -> dict[str, Any]:
    table = "smart_update_candidate_state"
    columns = _columns(con, table)
    if not columns:
        return _empty_durable(supported=False)

    id_column = _first(columns, ("id", "candidate_state_id", "candidate_id"))
    terminal_column = _first(
        columns,
        (
            "terminal_outcome",
            "current_outcome",
            "current_terminal",
            "terminal",
            "outcome",
        ),
    )
    since_column = _first(columns, ("created_at", "first_seen_at", "updated_at"))
    retry_column = _first(
        columns, ("next_retry_at", "retry_available_at", "next_attempt_at")
    )
    if not id_column or not terminal_column or not since_column:
        return _empty_durable(supported=False)

    terminal_q = _quoted(terminal_column)
    since_sql = _window_predicate(since_column)
    where_parts = [f"upper(COALESCE({terminal_q}, ''))='RETRY_SCHEDULED'", since_sql]
    params: list[Any] = [_render_utc(since), _render_utc(until)]
    if retry_column:
        retry_q = _quoted(retry_column)
        where_parts.append(
            f"({retry_q} IS NULL OR datetime({retry_q}) <= datetime(?))"
        )
        params.append(_render_utc(now))
    where = " AND ".join(where_parts)
    eligible = _count(con, table, where, params)
    if limit <= 0 or eligible == 0:
        result = _empty_durable(supported=True)
        result["eligible_due"] = eligible
        return result

    lock_columns = [
        name
        for name in (
            "claimed_by",
            "claim_token",
            "retry_claimed_by",
            "locked_by",
            "claimed_at",
            "retry_claimed_at",
            "locked_at",
        )
        if name in columns
    ]
    expiry_column = _first(
        columns,
        ("claim_expires_at", "lease_expires_at", "lease_until", "locked_until"),
    )
    selected_columns = [id_column, *lock_columns]
    if expiry_column and expiry_column not in selected_columns:
        selected_columns.append(expiry_column)
    select_sql = ",".join(_quoted(name) for name in selected_columns)
    rows = con.execute(
        f"SELECT {select_sql} FROM {_quoted(table)} WHERE {where} "
        f"ORDER BY {_quoted(id_column)} LIMIT ?",
        (*params, int(limit)),
    ).fetchall()

    would_requeue = 0
    requeued = 0
    already_available = 0
    active_claims = 0
    for raw_row in rows:
        row = dict(raw_row)
        locked = any(_is_nonempty(row.get(name)) for name in lock_columns)
        if not locked:
            already_available += 1
            continue
        if not expiry_column or not _expired(row.get(expiry_column), now):
            active_claims += 1
            continue
        would_requeue += 1
        if dry_run:
            continue
        assignments = [f"{_quoted(name)}=NULL" for name in lock_columns]
        if expiry_column:
            assignments.append(f"{_quoted(expiry_column)}=NULL")
        update_params: list[Any] = [row[id_column]]
        update_where = (
            f"{_quoted(id_column)}=? AND "
            f"upper(COALESCE({terminal_q}, ''))='RETRY_SCHEDULED'"
        )
        if expiry_column:
            update_where += (
                f" AND {_quoted(expiry_column)} IS NOT NULL"
                f" AND datetime({_quoted(expiry_column)}) <= datetime(?)"
            )
            update_params.append(_render_utc(now))
        cursor = con.execute(
            f"UPDATE {_quoted(table)} SET {','.join(assignments)} WHERE {update_where}",
            tuple(update_params),
        )
        requeued += int(cursor.rowcount or 0)

    return {
        "eligible_due": eligible,
        "selected": len(rows),
        "would_requeue": would_requeue,
        "requeued": requeued,
        "already_available": already_available,
        "active_claims_skipped": active_claims,
        "schema_supported": True,
    }


def _empty_vk(*, supported: bool) -> dict[str, Any]:
    return {
        "eligible": 0,
        "selected": 0,
        "would_requeue": 0,
        "requeued": 0,
        "selected_with_existing_imports": 0,
        "excluded_product_policy": 0,
        "not_due": 0,
        "schema_supported": supported,
    }


def _empty_telegram(*, supported: bool) -> dict[str, Any]:
    return {
        "eligible": 0,
        "selected": 0,
        "would_requeue": 0,
        "requeued": 0,
        "already_available": 0,
        "selected_with_existing_imports": 0,
        "excluded_product_policy": 0,
        "schema_supported": supported,
    }


def _recover_legacy_telegram(
    con: sqlite3.Connection,
    *,
    since: datetime,
    until: datetime,
    now: datetime,
    dry_run: bool,
    limit: int,
) -> dict[str, Any]:
    scan_table = "telegram_scanned_message"
    force_table = "telegram_source_force_message"
    columns = _columns(con, scan_table)
    force_columns = _columns(con, force_table)
    required = {"source_id", "message_id", "processed_at", "status"}
    if not required.issubset(columns) or not {"source_id", "message_id"}.issubset(
        force_columns
    ):
        return _empty_telegram(supported=False)

    technical_markers = (
        "review_required",
        "skipped_identity_gate",
        "source_binding_conflict",
        "identity_gate",
        "merge_identity",
        "dedup_adjudicator",
        "retry_scheduled",
        "smart_update_processing",
        "database",
        "vector_error",
    )
    technical = _marker_predicate(columns, technical_markers)
    policy = _product_policy_predicate(columns)
    base = (
        "datetime(processed_at)>=datetime(?) AND datetime(processed_at)<datetime(?) AND ("
        "lower(COALESCE(status,'')) IN ('retry_scheduled','partial_retry_scheduled','error') "
        "OR (lower(COALESCE(status,'')) IN ('skipped','partial') AND "
        f"{technical}))"
    )
    params = (_render_utc(since), _render_utc(until))
    eligible_where = f"({base}) AND NOT ({policy})"
    eligible = _count(con, scan_table, eligible_where, params)
    excluded = _count(con, scan_table, f"({base}) AND ({policy})", params)
    if limit <= 0 or eligible == 0:
        result = _empty_telegram(supported=True)
        result.update(eligible=eligible, excluded_product_policy=excluded)
        return result

    imported_expr = (
        "CASE WHEN COALESCE(events_imported,0)>0 THEN 1 ELSE 0 END"
        if "events_imported" in columns
        else "0"
    )
    rows = con.execute(
        f"SELECT source_id,message_id,{imported_expr} AS has_existing_import,"
        "CASE WHEN EXISTS(SELECT 1 FROM telegram_source_force_message f "
        "WHERE f.source_id=telegram_scanned_message.source_id "
        "AND f.message_id=telegram_scanned_message.message_id) THEN 1 ELSE 0 END "
        f"AS already_forced FROM {_quoted(scan_table)} WHERE {eligible_where} "
        "ORDER BY processed_at,source_id,message_id LIMIT ?",
        (*params, int(limit)),
    ).fetchall()
    already_available = sum(int(row[3] or 0) for row in rows)
    would_requeue = len(rows) - already_available
    requeued = 0
    if not dry_run:
        created_at_supported = "created_at" in force_columns
        for row in rows:
            if int(row[3] or 0):
                continue
            if created_at_supported:
                cursor = con.execute(
                    "INSERT OR IGNORE INTO telegram_source_force_message"
                    "(source_id,message_id,created_at) VALUES(?,?,?)",
                    (int(row[0]), int(row[1]), _render_utc(now)),
                )
            else:
                cursor = con.execute(
                    "INSERT OR IGNORE INTO telegram_source_force_message"
                    "(source_id,message_id) VALUES(?,?)",
                    (int(row[0]), int(row[1])),
                )
            requeued += int(cursor.rowcount or 0)

    return {
        "eligible": eligible,
        "selected": len(rows),
        "would_requeue": would_requeue,
        "requeued": requeued,
        "already_available": already_available,
        "selected_with_existing_imports": sum(int(row[2] or 0) for row in rows),
        "excluded_product_policy": excluded,
        "schema_supported": True,
    }


def _empty_queue(*, supported: bool) -> dict[str, Any]:
    return {
        "eligible": 0,
        "selected": 0,
        "would_requeue": 0,
        "requeued": 0,
        "already_available": 0,
        "excluded_product_policy": 0,
        "schema_supported": supported,
    }


def _recover_legacy_queue(
    con: sqlite3.Connection,
    *,
    table: str,
    ready_status: str,
    since: datetime,
    until: datetime,
    now: datetime,
    dry_run: bool,
    limit: int,
) -> dict[str, Any]:
    columns = _columns(con, table)
    if not {"id", "status", "updated_at"}.issubset(columns):
        return _empty_queue(supported=False)
    policy = _product_policy_predicate(columns)
    since_sql = _window_predicate("updated_at")
    base = f"lower(COALESCE(status,''))='error' AND {since_sql}"
    params = (_render_utc(since), _render_utc(until))
    eligible_where = f"({base}) AND NOT ({policy})"
    eligible = _count(con, table, eligible_where, params)
    excluded = _count(con, table, f"({base}) AND ({policy})", params)
    already_available = _count(
        con,
        table,
        f"lower(COALESCE(status,''))=? AND {since_sql}",
        (ready_status.lower(), _render_utc(since), _render_utc(until)),
    )
    if limit <= 0 or eligible == 0:
        result = _empty_queue(supported=True)
        result.update(
            eligible=eligible,
            already_available=already_available,
            excluded_product_policy=excluded,
        )
        return result

    rows = con.execute(
        f"SELECT id FROM {_quoted(table)} WHERE {eligible_where} ORDER BY updated_at,id LIMIT ?",
        (*params, int(limit)),
    ).fetchall()
    requeued = 0
    if not dry_run and rows:
        ids = [int(row[0]) for row in rows]
        placeholders = ",".join("?" for _ in ids)
        assignments = ["status=?"]
        values: list[Any] = [ready_status]
        if "next_run_at" in columns:
            assignments.append("next_run_at=?")
            values.append(_render_utc(now))
        if "last_error" in columns:
            assignments.append("last_error=NULL")
        if "attempts" in columns:
            assignments.append("attempts=0")
        cursor = con.execute(
            f"UPDATE {_quoted(table)} SET {','.join(assignments)} "
            f"WHERE id IN ({placeholders}) AND lower(COALESCE(status,''))='error' "
            f"AND NOT ({policy})",
            (*values, *ids),
        )
        requeued = int(cursor.rowcount or 0)
    return {
        "eligible": eligible,
        "selected": len(rows),
        "would_requeue": len(rows),
        "requeued": requeued,
        "already_available": already_available,
        "excluded_product_policy": excluded,
        "schema_supported": True,
    }


def _recover_source_parser_refresh(
    con: sqlite3.Connection,
    *,
    since: datetime,
    until: datetime,
    now: datetime,
    dry_run: bool,
    limit: int,
) -> dict[str, Any]:
    columns = _columns(con, "ops_run")
    if not {"kind", "started_at", "details_json"}.issubset(columns):
        return {
            "runs_seen": 0,
            "failed_items_observed": 0,
            "retry_items_observed": 0,
            "affected_sources": [],
            "recovery_mode": "unavailable",
            "eligible": 0,
            "selected": 0,
            "would_requeue": 0,
            "requeued": 0,
            "already_available": 0,
            "queue_schema_supported": False,
            "deployment_required": False,
            "schema_supported": False,
        }
    rows = con.execute(
        "SELECT details_json FROM ops_run WHERE kind='parse' "
        "AND datetime(started_at)>=datetime(?) AND datetime(started_at)<datetime(?)",
        (_render_utc(since), _render_utc(until)),
    ).fetchall()
    failed = 0
    retries = 0
    sources: set[str] = set()
    for row in rows:
        raw = row[0]
        try:
            payload = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
        except Exception:
            continue
        source_payload = payload.get("sources") if isinstance(payload, dict) else None
        if not isinstance(source_payload, dict):
            continue
        for source, values in source_payload.items():
            if not isinstance(values, dict):
                continue
            source_failed = int(values.get("failed") or 0)
            source_retries = int(values.get("retry_scheduled") or 0)
            failed += source_failed
            retries += source_retries
            if source_failed or source_retries:
                clean = str(source or "").strip().lower()
                if clean and clean.replace("_", "").isalnum():
                    sources.add(clean)
    queue_columns = _columns(con, "source_parser_recovery_request")
    queue_supported = {
        "source_type",
        "requested_since",
        "status",
        "attempts",
        "next_run_at",
    }.issubset(queue_columns)
    affected = sorted(sources)
    selected_sources = affected[: max(0, int(limit))] if queue_supported else []
    already_available = 0
    missing: list[str] = []
    if queue_supported and selected_sources:
        placeholders = ",".join("?" for _ in selected_sources)
        active_rows = con.execute(
            "SELECT source_type FROM source_parser_recovery_request "
            f"WHERE source_type IN ({placeholders}) AND status IN ('pending','running')",
            tuple(selected_sources),
        ).fetchall()
        active = {str(row[0]) for row in active_rows}
        already_available = len(active)
        missing = [source for source in selected_sources if source not in active]
    requeued = 0
    if queue_supported and not dry_run:
        for source in missing:
            cursor = con.execute(
                """
                INSERT INTO source_parser_recovery_request(
                    source_type,requested_since,status,attempts,next_run_at,
                    last_error,created_at,updated_at
                ) VALUES(?,?,'pending',0,?,NULL,?,?)
                ON CONFLICT(source_type) DO UPDATE SET
                    requested_since=excluded.requested_since,
                    status='pending',
                    attempts=0,
                    next_run_at=excluded.next_run_at,
                    last_error=NULL,
                    updated_at=excluded.updated_at
                WHERE source_parser_recovery_request.status NOT IN ('pending','running')
                """,
                (
                    source,
                    _render_utc(since),
                    _render_utc(now),
                    _render_utc(now),
                    _render_utc(now),
                ),
            )
            requeued += int(cursor.rowcount or 0)
    return {
        "runs_seen": len(rows),
        "failed_items_observed": failed,
        "retry_items_observed": retries,
        "affected_sources": affected,
        "eligible": len(affected),
        "selected": len(selected_sources),
        "would_requeue": len(missing),
        "requeued": requeued,
        "already_available": already_available,
        # Official parsers fetch complete current catalogues each scheduled run;
        # requests therefore replay a source, not unavailable individual payloads.
        "recovery_mode": "queued_full_catalog_refresh",
        "queue_schema_supported": queue_supported,
        "deployment_required": bool(affected and not queue_supported),
        "schema_supported": True,
    }


def _recover_legacy_vk(
    con: sqlite3.Connection,
    *,
    since: datetime,
    until: datetime,
    now: datetime,
    dry_run: bool,
    limit: int,
) -> dict[str, Any]:
    table = "vk_inbox"
    columns = _columns(con, table)
    if not {"id", "status"}.issubset(columns):
        return _empty_vk(supported=False)
    since_column = _first(columns, ("created_at", "date"))
    if not since_column:
        # Without a durable time boundary the incident window cannot be honored.
        return _empty_vk(supported=False)
    unix_epoch = since_column == "date"
    since_sql = _window_predicate(since_column, unix_epoch=unix_epoch)
    due_deferred = "lower(status)='deferred'"
    due_params: list[Any] = []
    if "locked_at" in columns:
        due_deferred += " AND (locked_at IS NULL OR datetime(locked_at) <= datetime(?))"
        due_params.append(_render_utc(now))
    base = f"(lower(status)='failed' OR ({due_deferred})) AND {since_sql}"
    base_params = [*due_params, _render_utc(since), _render_utc(until)]
    policy = _product_policy_predicate(columns)
    eligible_where = f"({base}) AND NOT ({policy})"
    eligible = _count(con, table, eligible_where, base_params)
    excluded = _count(con, table, f"({base}) AND ({policy})", base_params)

    not_due = 0
    if "locked_at" in columns:
        not_due = _count(
            con,
            table,
            "lower(status)='deferred' AND locked_at IS NOT NULL "
            "AND datetime(locked_at) > datetime(?) AND " + since_sql,
            (_render_utc(now), _render_utc(since), _render_utc(until)),
        )
    if limit <= 0 or eligible == 0:
        result = _empty_vk(supported=True)
        result.update(
            eligible=eligible,
            excluded_product_policy=excluded,
            not_due=not_due,
        )
        return result

    import_expr = (
        "CASE WHEN imported_event_id IS NOT NULL THEN 1 ELSE 0 END"
        if "imported_event_id" in columns
        else "0"
    )
    mapping_columns = _columns(con, "vk_inbox_import_event")
    if {"inbox_id", "event_id"}.issubset(mapping_columns):
        import_expr = (
            f"CASE WHEN ({import_expr})=1 OR EXISTS(SELECT 1 FROM vk_inbox_import_event m "
            f"WHERE m.inbox_id={_quoted(table)}.id) THEN 1 ELSE 0 END"
        )
    rows = con.execute(
        f"SELECT id,{import_expr} AS has_existing_import FROM {_quoted(table)} "
        f"WHERE {eligible_where} ORDER BY id LIMIT ?",
        (*base_params, int(limit)),
    ).fetchall()
    existing_imports = sum(int(row[1] or 0) for row in rows)
    requeued = 0
    if not dry_run and rows:
        assignments = ["status='pending'"]
        assignments.extend(
            f"{_quoted(name)}=NULL"
            for name in ("locked_by", "locked_at", "review_batch")
            if name in columns
        )
        if "attempts" in columns:
            assignments.append("attempts=0")
        ids = [int(row[0]) for row in rows]
        placeholders = ",".join("?" for _ in ids)
        # Recheck the terminal class and product-policy exclusion in the write.
        update_where = (
            f"id IN ({placeholders}) AND lower(status) IN ('failed','deferred') "
            f"AND NOT ({policy})"
        )
        cursor = con.execute(
            f"UPDATE {_quoted(table)} SET {','.join(assignments)} WHERE {update_where}",
            tuple(ids),
        )
        requeued = int(cursor.rowcount or 0)

    return {
        "eligible": eligible,
        "selected": len(rows),
        "would_requeue": len(rows),
        "requeued": requeued,
        "selected_with_existing_imports": existing_imports,
        "excluded_product_policy": excluded,
        "not_due": not_due,
        "schema_supported": True,
    }


def run(
    db_path: str | Path,
    *,
    since: str = DEFAULT_SINCE,
    until: str | None = None,
    dry_run: bool = True,
    read_only: bool = False,
    batch_size: int = 100,
    now: str | datetime | None = None,
    sources: Sequence[str] | None = None,
    loss_classes: Sequence[str] | None = None,
    include_discovery_misses: bool = False,
    evidence_paths: Sequence[str | Path] = (),
    supabase_evidence_paths: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Plan or apply a bounded, idempotent recovery transaction."""

    try:
        normalized_batch = int(batch_size)
    except (TypeError, ValueError) as exc:
        raise RecoveryError("invalid_batch_size") from exc
    if normalized_batch <= 0 or normalized_batch > MAX_BATCH_SIZE:
        raise RecoveryError("invalid_batch_size")
    if read_only and not dry_run:
        raise RecoveryError("read_only_apply_conflict")
    since_dt = _parse_utc(since, field="since")
    if isinstance(now, datetime):
        now_dt = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)
    elif now is None:
        now_dt = datetime.now(timezone.utc)
    else:
        now_dt = _parse_utc(now, field="now")
    until_dt = _parse_utc(until, field="until") if until else now_dt
    if until_dt <= since_dt:
        raise RecoveryError("invalid_window")
    source_filters = _normalized_filters(sources, field="source")
    class_filters = _normalized_filters(loss_classes, field="loss_class")

    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise RecoveryError("database_not_found")
    uri = path.as_uri() + ("?mode=ro" if dry_run else "?mode=rw")
    con = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA foreign_keys=ON")
    if dry_run:
        con.execute("PRAGMA query_only=ON")
    else:
        con.execute("BEGIN IMMEDIATE")
    try:
        durable_enabled = _source_enabled(source_filters, "smart_update") and _class_enabled(
            class_filters, "P", "Q", "SMART_UPDATE_IDENTITY_LOSS", "TECHNICAL_TERMINAL_FAILED"
        )
        durable = _recover_durable(
            con,
            since=since_dt,
            until=until_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=normalized_batch if durable_enabled else 0,
        )
        remaining = max(0, normalized_batch - int(durable["selected"]))
        telegram_enabled = _source_enabled(source_filters, "telegram") and _class_enabled(
            class_filters, "N", "O", "P", "Q"
        )
        legacy_telegram = _recover_legacy_telegram(
            con,
            since=since_dt,
            until=until_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=remaining if telegram_enabled else 0,
        )
        remaining = max(0, remaining - int(legacy_telegram["selected"]))
        parser_enabled = _source_enabled(source_filters, "parser") and _class_enabled(class_filters, "Q")
        source_parser = _recover_source_parser_refresh(
            con,
            since=since_dt,
            until=until_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=remaining if parser_enabled else 0,
        )
        remaining = max(0, remaining - int(source_parser["selected"]))
        vk_enabled = _source_enabled(source_filters, "vk") and _class_enabled(
            class_filters, "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"
        )
        legacy_vk = _recover_legacy_vk(
            con,
            since=since_dt,
            until=until_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=remaining if vk_enabled else 0,
        )
        remaining = max(0, remaining - int(legacy_vk["selected"]))
        ticket_enabled = _source_enabled(source_filters, "ticket") and _class_enabled(class_filters, "Q")
        legacy_ticket_queue = _recover_legacy_queue(
            con,
            table="ticket_site_queue",
            ready_status="active",
            since=since_dt,
            until=until_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=remaining if ticket_enabled else 0,
        )
        remaining = max(0, remaining - int(legacy_ticket_queue["selected"]))
        festival_enabled = _source_enabled(source_filters, "festival") and _class_enabled(class_filters, "Q")
        legacy_festival_queue = _recover_legacy_queue(
            con,
            table="festival_queue",
            ready_status="pending",
            since=since_dt,
            until=until_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=remaining if festival_enabled else 0,
        )
        mutable_results = (
            durable,
            legacy_telegram,
            source_parser,
            legacy_vk,
            legacy_ticket_queue,
            legacy_festival_queue,
        )
        would_change = sum(int(item["would_requeue"]) for item in mutable_results)
        changed = sum(int(item["requeued"]) for item in mutable_results)
        selected_total = sum(int(item["selected"]) for item in mutable_results)
        if not dry_run:
            con.commit()
        census = None
        replay_inventory: list[dict[str, Any]] = []
        if dry_run:
            census_module = _load_census_module()
            census = census_module.run(
                path,
                since=_render_utc(since_dt),
                until=_render_utc(until_dt),
                evidence_paths=evidence_paths,
                supabase_evidence_paths=supabase_evidence_paths,
            )
            normalized_class_codes = {
                census_module.normalize_class(item) for item in class_filters
            } - {None}
            for item in census["inventory"]:
                if source_filters and str(item["source_type"]).lower() not in source_filters:
                    continue
                if normalized_class_codes and item["loss_class"] not in normalized_class_codes:
                    continue
                if not include_discovery_misses and item["loss_class"] in {"A", "B", "C", "D", "E"}:
                    continue
                replay_inventory.append(item)
            replay_inventory = replay_inventory[:normalized_batch]
        pipeline = (
            "RESTORE_RAW_PAYLOAD", "RESTORE_ATTACHMENTS_AND_OCR",
            "TYPED_LLM_SOURCE_DECISION", "SMART_UPDATE_PLAN",
        )
        stable_plan = {
            "sources": list(source_filters), "loss_classes": list(class_filters),
            "include_discovery_misses": bool(include_discovery_misses),
            "pipeline": pipeline,
            "carrier_revision_keys": [item["carrier_revision_key"] for item in replay_inventory],
            "direct_event_insert": False,
        }
        plan_hash = hashlib.sha256(
            json.dumps(stable_plan, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        result = {
            "schema": REPORT_SCHEMA,
            "mode": "dry-run" if dry_run else "apply",
            "status": "ready" if dry_run and would_change else "applied" if changed else "noop",
            "changed": bool(changed),
            "since": _render_utc(since_dt),
            "until_exclusive": _render_utc(until_dt),
            "batch_size": normalized_batch,
            "filters": {
                "sources": list(source_filters),
                "loss_classes": list(class_filters),
                "include_discovery_misses": bool(include_discovery_misses),
            },
            "features": {
                "smart_update_candidate_state": bool(
                    durable["schema_supported"]
                ),
                "telegram_scanned_message": bool(
                    legacy_telegram["schema_supported"]
                ),
                "vk_inbox": bool(legacy_vk["schema_supported"]),
                "ticket_site_queue": bool(legacy_ticket_queue["schema_supported"]),
                "festival_queue": bool(legacy_festival_queue["schema_supported"]),
                "source_parser_ops_run": bool(source_parser["schema_supported"]),
                "source_parser_recovery_request": bool(
                    source_parser["queue_schema_supported"]
                ),
            },
            "durable_candidates": durable,
            "legacy_telegram": legacy_telegram,
            "legacy_vk": legacy_vk,
            "legacy_ticket_queue": legacy_ticket_queue,
            "legacy_festival_queue": legacy_festival_queue,
            "source_parser": source_parser,
            "loss_census": census,
            "replay_plan": {
                "carrier_count": len(replay_inventory),
                "event_occurrence_count": sum(
                    int(item.get("extracted_event_occurrences") or 0) for item in replay_inventory
                ),
                "lifecycle_action_count": sum(
                    int(item.get("lifecycle_actions") or 0) for item in replay_inventory
                ),
                "unavailable_evidence_count": sum(
                    not bool(item.get("payload_available")) for item in replay_inventory
                ),
                "stages": list(pipeline),
                "execution": "plan_only_requires_deployed_llm_first_pipeline",
                "direct_event_insert": False,
                "production_writes": False if dry_run else bool(changed),
                "inventory_hash": census.get("inventory_hash") if census else None,
                "plan_hash": plan_hash,
            },
            "aggregate": {
                "selected": selected_total,
                "would_change": would_change,
                "changed": changed,
                "remaining_capacity": max(0, normalized_batch - selected_total),
            },
        }
        return result
    except Exception:
        if not dry_run:
            con.rollback()
        raise
    finally:
        con.close()


def _write_result(result: Mapping[str, Any], output: str) -> None:
    rendered = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if output == "-":
        sys.stdout.write(rendered)
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")
    sys.stdout.write(
        json.dumps(
            {
                "schema": REPORT_SCHEMA,
                "status": result.get("status"),
                "changed": result.get("changed"),
                "output": str(path),
            },
            sort_keys=True,
        )
        + "\n"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="read-only plan (default)")
    mode.add_argument(
        "--read-only", action="store_true",
        help="explicit strict read-only plan; mutually exclusive with --apply",
    )
    mode.add_argument("--apply", action="store_true", help="commit bounded requeue transitions")
    parser.add_argument("--since", default=DEFAULT_SINCE, help="UTC ISO date/time boundary")
    parser.add_argument("--until", help="exclusive UTC ISO date/time boundary")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--loss-class", action="append", default=[])
    parser.add_argument("--include-discovery-misses", action="store_true")
    parser.add_argument("--evidence-json", action="append", default=[])
    parser.add_argument("--supabase-evidence-json", action="append", default=[])
    parser.add_argument("--db", default=os.getenv("DB_PATH", "/data/db.sqlite"))
    parser.add_argument("--output", default="-")
    args = parser.parse_args(argv)
    dry_run = not bool(args.apply)
    try:
        result = run(
            args.db,
            since=args.since,
            until=args.until,
            dry_run=dry_run,
            read_only=bool(args.read_only),
            batch_size=args.batch_size,
            sources=args.source,
            loss_classes=args.loss_class,
            include_discovery_misses=bool(args.include_discovery_misses),
            evidence_paths=args.evidence_json,
            supabase_evidence_paths=args.supabase_evidence_json,
        )
        _write_result(result, args.output)
        return 0
    except RecoveryError as exc:
        sys.stderr.write(
            json.dumps(
                {"schema": REPORT_SCHEMA, "status": "blocked", "reason": str(exc)},
                sort_keys=True,
            )
            + "\n"
        )
        return 2
    except (sqlite3.DatabaseError, OSError) as exc:
        # SQLite error messages can contain constrained values; expose class only.
        sys.stderr.write(
            json.dumps(
                {
                    "schema": REPORT_SCHEMA,
                    "status": "error",
                    "exception_class": type(exc).__name__,
                },
                sort_keys=True,
            )
            + "\n"
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
