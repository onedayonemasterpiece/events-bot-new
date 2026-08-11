#!/usr/bin/env python3
"""Fail-closed Smart Update migration rehearsal on an offline DB clone.

The original snapshot and any ``-wal``/``-shm`` sidecars are only byte-read.
All SQLite writes, including ``Database.init()``, are confined to ``--clone``.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Mapping, Sequence
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
from typing import Any


REPORT_SCHEMA = "kenigevents.smart_update_migration_rehearsal.v1"


class RehearsalError(RuntimeError):
    pass


def _hash(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def _bundle_proof(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for suffix in ("", "-wal", "-shm"):
        item = Path(str(path) + suffix)
        result[suffix or "main"] = {
            "present": item.exists(),
            "size": item.stat().st_size if item.exists() else 0,
            "sha256": _hash(item),
        }
    return result


def _read_connection(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    return con


def _quick_check(con: sqlite3.Connection) -> list[str]:
    return [str(row[0]) for row in con.execute("PRAGMA quick_check")]


def _inventory(con: sqlite3.Connection) -> tuple[dict[str, list[str]], dict[str, int]]:
    schema: dict[str, list[str]] = {}
    counts: dict[str, int] = {}
    names = [
        str(row[0]) for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    for name in names:
        quoted = '"' + name.replace('"', '""') + '"'
        schema[name] = [str(row[1]) for row in con.execute(f"PRAGMA table_info({quoted})")]
        counts[name] = int(con.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0])
    return schema, counts


def _schema_hash(schema: Mapping[str, Sequence[str]]) -> str:
    rendered = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _index_constraint_inventory(con: sqlite3.Connection) -> dict[str, Any]:
    objects = [
        {"type": str(row[0]), "name": str(row[1]), "table": str(row[2]), "sql": str(row[3] or "")}
        for row in con.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE type IN ('index','trigger') ORDER BY type,name"
        )
    ]
    return {
        "objects": objects,
        "object_count": len(objects),
        "sha256": hashlib.sha256(
            json.dumps(objects, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def _query_only_rejection(con: sqlite3.Connection) -> bool:
    try:
        con.execute("CREATE TABLE __smart_update_forbidden_write(id INTEGER)")
    except sqlite3.DatabaseError:
        return True
    return False


def _copy_snapshot_bundle(original: Path, staging: Path) -> None:
    shutil.copy2(original, staging)
    for suffix in ("-wal", "-shm"):
        source = Path(str(original) + suffix)
        if source.exists():
            shutil.copy2(source, Path(str(staging) + suffix))


def _backup_clone(staging: Path, clone: Path) -> None:
    source = _read_connection(staging)
    try:
        target = sqlite3.connect(clone)
        try:
            source.backup(target)
            target.commit()
        finally:
            target.close()
    finally:
        source.close()


def _default_initializer(clone: Path) -> None:
    from db import Database

    async def initialize_once() -> None:
        database = Database(str(clone))
        await database.init()

    asyncio.run(initialize_once())


def _load_sibling(name: str):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location(name + "_for_rehearsal", path)
    if not spec or not spec.loader:
        raise RehearsalError(f"{name}_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _conflicts(con: sqlite3.Connection, schema: Mapping[str, Sequence[str]]) -> dict[str, int]:
    result: dict[str, int] = {}
    state = set(schema.get("smart_update_candidate_state", ()))
    candidate_key = next(
        (name for name in ("candidate_key", "idempotency_key", "source_candidate_key") if name in state), None
    )
    if candidate_key:
        result["smart_update_candidate_key_duplicates"] = int(
            con.execute(
                f'SELECT COUNT(*) FROM (SELECT "{candidate_key}" FROM smart_update_candidate_state '
                f'WHERE "{candidate_key}" IS NOT NULL GROUP BY "{candidate_key}" HAVING COUNT(*)>1)'
            ).fetchone()[0]
        )
    event_source = set(schema.get("event_source", ()))
    occurrence_cols = [name for name in ("source_type", "source_url", "occurrence_key") if name in event_source]
    if len(occurrence_cols) == 3:
        rendered = ",".join(f'"{name}"' for name in occurrence_cols)
        result["event_source_occurrence_duplicates"] = int(
            con.execute(
                f"SELECT COUNT(*) FROM (SELECT {rendered} FROM event_source "
                f"WHERE occurrence_key IS NOT NULL GROUP BY {rendered} HAVING COUNT(*)>1)"
            ).fetchone()[0]
        )
    result["foreign_key_violations"] = len(con.execute("PRAGMA foreign_key_check").fetchall())
    return result


def _rollback_compatibility_rehearsal(
    clone: Path, original_schema: Mapping[str, Sequence[str]]
) -> dict[str, Any]:
    con = sqlite3.connect(clone)
    before_schema, _ = _inventory(con)
    try:
        con.execute("BEGIN IMMEDIATE")
        for table, columns in original_schema.items():
            quoted_table = '"' + table.replace('"', '""') + '"'
            rendered = ",".join('"' + col.replace('"', '""') + '"' for col in columns) or "1"
            con.execute(f"SELECT {rendered} FROM {quoted_table} LIMIT 0").fetchall()
        con.execute("CREATE TABLE __smart_update_rollback_probe(id INTEGER)")
        con.rollback()
        after_schema, _ = _inventory(con)
        probe_absent = "__smart_update_rollback_probe" not in after_schema
        return {
            "legacy_schema_compatible": True,
            "transaction_rollback_probe_absent": probe_absent,
            "schema_hash_unchanged": _schema_hash(before_schema) == _schema_hash(after_schema),
        }
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def run(
    original_path: str | Path,
    clone_path: str | Path,
    *,
    since: str,
    until: str,
    initializer: Callable[[Path], None] | None = None,
    count_allowlist: Sequence[str] = (),
) -> dict[str, Any]:
    original, clone = Path(original_path).resolve(), Path(clone_path).resolve()
    if not original.is_file():
        raise RehearsalError("snapshot_not_found")
    if clone.exists() or Path(str(clone) + "-wal").exists() or Path(str(clone) + "-shm").exists():
        raise RehearsalError("clone_must_not_exist")
    clone.parent.mkdir(parents=True, exist_ok=True)
    before_bundle = _bundle_proof(original)
    with tempfile.TemporaryDirectory(prefix="smart-update-rehearsal-") as temp:
        staging = Path(temp) / "snapshot.sqlite"
        _copy_snapshot_bundle(original, staging)
        staging_con = _read_connection(staging)
        try:
            original_quick = _quick_check(staging_con)
            original_schema, original_counts = _inventory(staging_con)
            query_only_rejected = _query_only_rejection(staging_con)
        finally:
            staging_con.close()
        if original_quick != ["ok"] or not query_only_rejected:
            raise RehearsalError("original_read_only_gate_failed")
        _backup_clone(staging, clone)

    clone_con = _read_connection(clone)
    try:
        clone_pre_quick = _quick_check(clone_con)
    finally:
        clone_con.close()
    if clone_pre_quick != ["ok"]:
        raise RehearsalError("clone_quick_check_failed")

    init = initializer or _default_initializer
    init(clone)
    init(clone)

    clone_con = _read_connection(clone)
    try:
        migrated_schema, migrated_counts = _inventory(clone_con)
        index_constraints = _index_constraint_inventory(clone_con)
        conflicts = _conflicts(clone_con, migrated_schema)
    finally:
        clone_con.close()
    count_changes = {
        table: {"before": count, "after": migrated_counts.get(table)}
        for table, count in original_counts.items()
        if migrated_counts.get(table) != count
    }
    disallowed_changes = sorted(set(count_changes) - set(count_allowlist))
    removed_tables = sorted(set(original_schema) - set(migrated_schema))
    removed_columns = {
        table: sorted(set(columns) - set(migrated_schema.get(table, ())))
        for table, columns in original_schema.items()
        if set(columns) - set(migrated_schema.get(table, ()))
    }
    nonzero_conflicts = {name: value for name, value in conflicts.items() if value}
    if disallowed_changes or removed_tables or removed_columns or nonzero_conflicts:
        raise RehearsalError("migration_conflict_requires_repair_plan")

    census = _load_sibling("smart_update_loss_census").run(
        clone, since=since, until=until
    )
    recovery = _load_sibling("recover_smart_update_identity_losses").run(
        clone, since=since, until=until, dry_run=True, read_only=True
    )
    rollback = _rollback_compatibility_rehearsal(clone, original_schema)
    if not all(rollback.values()):
        raise RehearsalError("rollback_compatibility_failed")
    final_con = _read_connection(clone)
    try:
        final_quick = _quick_check(final_con)
    finally:
        final_con.close()
    after_bundle = _bundle_proof(original)
    if before_bundle != after_bundle:
        raise RehearsalError("original_snapshot_changed")
    if final_quick != ["ok"]:
        raise RehearsalError("final_quick_check_failed")
    return {
        "schema": REPORT_SCHEMA,
        "status": "passed",
        "original": {
            "path": str(original), "bundle_before": before_bundle, "bundle_after": after_bundle,
            "byte_unchanged": before_bundle == after_bundle, "query_only_write_rejected": query_only_rejected,
            "quick_check": original_quick,
        },
        "clone": {
            "path": str(clone), "pre_quick_check": clone_pre_quick,
            "database_init_runs": 2, "final_quick_check": final_quick,
        },
        "migration": {
            "original_schema_hash": _schema_hash(original_schema),
            "migrated_schema_hash": _schema_hash(migrated_schema),
            "removed_tables": removed_tables, "removed_columns": removed_columns,
            "count_changes": count_changes, "count_allowlist": sorted(count_allowlist),
            "indexes_and_constraints": index_constraints,
            "conflicts": conflicts,
        },
        "census": {
            "inventory_hash": census["inventory_hash"], "carrier_count": census["totals"]["carrier_count"],
            "changed_rows": census["changed_rows"],
        },
        "recovery": {
            "plan_hash": recovery["replay_plan"]["plan_hash"],
            "carrier_count": recovery["replay_plan"]["carrier_count"],
            "changed": recovery["changed"],
        },
        "rollback_rehearsal": rollback,
        "mutations_confined_to_clone": True,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--clone", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--allow-count-change", action="append", default=[])
    parser.add_argument("--output", default="-")
    args = parser.parse_args(argv)
    try:
        result = run(
            args.snapshot, args.clone, since=args.since, until=args.until,
            count_allowlist=args.allow_count_change,
        )
    except (RehearsalError, sqlite3.DatabaseError, OSError) as exc:
        sys.stderr.write(json.dumps({"schema": REPORT_SCHEMA, "status": "blocked", "reason": str(exc)}, sort_keys=True) + "\n")
        return 2
    rendered = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if args.output == "-":
        sys.stdout.write(rendered)
    else:
        Path(args.output).write_text(rendered, encoding="utf-8")
        sys.stdout.write(json.dumps({"schema": REPORT_SCHEMA, "status": "passed", "output": args.output}, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
