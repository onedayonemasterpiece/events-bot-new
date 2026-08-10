#!/usr/bin/env python3
"""Requeue Smart Update identity losses without reopening product-policy rejects.

The recovery is intentionally a queue/state transition, not a second identity
implementation.  Durable ``RETRY_SCHEDULED`` candidates remain owned by the
normal Smart Update retry worker; this tool only releases an expired claim.
Legacy VK ``failed`` rows and *due* ``deferred`` rows are moved back to
``pending``.  ``imported`` and ``rejected`` rows are never selected.

Output is aggregate-only: source text, URLs, exception messages and payloads
are neither printed nor copied.  Dry-run is the default and opens SQLite in
read-only/query-only mode.  Applying changes requires an explicit ``--apply``.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any, Iterable, Mapping, Sequence


REPORT_SCHEMA = "kenigevents.smart_update_identity_recovery.v1"
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


def _since_predicate(column: str, *, unix_epoch: bool = False) -> str:
    quoted = _quoted(column)
    if unix_epoch:
        return f"datetime({quoted}, 'unixepoch') >= datetime(?)"
    return f"datetime({quoted}) >= datetime(?)"


def _product_policy_predicate(columns: set[str]) -> str:
    evidence_columns = [
        name
        for name in ("last_error", "last_result_json", "decision_reason", "reason")
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
    since_sql = _since_predicate(since_column)
    where_parts = [f"upper(COALESCE({terminal_q}, ''))='RETRY_SCHEDULED'", since_sql]
    params: list[Any] = [_render_utc(since)]
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


def _recover_legacy_vk(
    con: sqlite3.Connection,
    *,
    since: datetime,
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
    since_sql = _since_predicate(since_column, unix_epoch=unix_epoch)
    due_deferred = "lower(status)='deferred'"
    due_params: list[Any] = []
    if "locked_at" in columns:
        due_deferred += " AND (locked_at IS NULL OR datetime(locked_at) <= datetime(?))"
        due_params.append(_render_utc(now))
    base = f"(lower(status)='failed' OR ({due_deferred})) AND {since_sql}"
    base_params = [*due_params, _render_utc(since)]
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
            (_render_utc(now), _render_utc(since)),
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
    dry_run: bool = True,
    batch_size: int = 100,
    now: str | datetime | None = None,
) -> dict[str, Any]:
    """Plan or apply a bounded, idempotent recovery transaction."""

    try:
        normalized_batch = int(batch_size)
    except (TypeError, ValueError) as exc:
        raise RecoveryError("invalid_batch_size") from exc
    if normalized_batch <= 0 or normalized_batch > MAX_BATCH_SIZE:
        raise RecoveryError("invalid_batch_size")
    since_dt = _parse_utc(since, field="since")
    if isinstance(now, datetime):
        now_dt = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)
    elif now is None:
        now_dt = datetime.now(timezone.utc)
    else:
        now_dt = _parse_utc(now, field="now")

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
        durable = _recover_durable(
            con,
            since=since_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=normalized_batch,
        )
        remaining = max(0, normalized_batch - int(durable["selected"]))
        legacy_vk = _recover_legacy_vk(
            con,
            since=since_dt,
            now=now_dt,
            dry_run=dry_run,
            limit=remaining,
        )
        would_change = int(durable["would_requeue"]) + int(legacy_vk["would_requeue"])
        changed = int(durable["requeued"]) + int(legacy_vk["requeued"])
        if not dry_run:
            con.commit()
        result = {
            "schema": REPORT_SCHEMA,
            "mode": "dry-run" if dry_run else "apply",
            "status": "ready" if dry_run and would_change else "applied" if changed else "noop",
            "changed": bool(changed),
            "since": _render_utc(since_dt),
            "batch_size": normalized_batch,
            "features": {
                "smart_update_candidate_state": bool(
                    durable["schema_supported"]
                ),
                "vk_inbox": bool(legacy_vk["schema_supported"]),
            },
            "durable_candidates": durable,
            "legacy_vk": legacy_vk,
            "aggregate": {
                "selected": int(durable["selected"]) + int(legacy_vk["selected"]),
                "would_change": would_change,
                "changed": changed,
                "remaining_capacity": max(
                    0,
                    normalized_batch
                    - int(durable["selected"])
                    - int(legacy_vk["selected"]),
                ),
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
    mode.add_argument("--apply", action="store_true", help="commit bounded requeue transitions")
    parser.add_argument("--since", default=DEFAULT_SINCE, help="UTC ISO date/time boundary")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--db", default=os.getenv("DB_PATH", "/data/db.sqlite"))
    parser.add_argument("--output", default="-")
    args = parser.parse_args(argv)
    dry_run = not bool(args.apply)
    try:
        result = run(
            args.db,
            since=args.since,
            dry_run=dry_run,
            batch_size=args.batch_size,
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
