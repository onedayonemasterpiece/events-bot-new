#!/usr/bin/env python3
"""Normalize duplicate Region Talk durable post rows in YDB.

Dry-run is the default. ``--execute`` first UPSERTs one merged stable row per
post and only then deletes redundant fetch-path-dependent rows, so an
interrupted run can leave duplicates but cannot lose the post.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    ydb_credentials,
    ydb_endpoint_database,
)


def load_candidate_module() -> Any:
    path = ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"
    spec = importlib.util.spec_from_file_location("region_talk_candidate_report_post_normalize", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"cannot load CandidateReport module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


rt = load_candidate_module()

def clean_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if not key.startswith("_ydb_")}


def normalize_plan(rows: list[dict[str, Any]], *, max_groups: int = 0) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        durable_key = rt.durable_processed_post_key(row)
        if durable_key:
            grouped.setdefault(durable_key, []).append(row)
    migration_groups: list[tuple[str, list[dict[str, Any]]]] = []
    for key, values in grouped.items():
        source_pks = {str(value.get("_ydb_pk") or "") for value in values if value.get("_ydb_pk")}
        canonical_pk = "processed_post_item:" + key
        if len(source_pks) > 1 or (source_pks and source_pks != {canonical_pk}):
            migration_groups.append((key, values))
    migration_groups.sort(key=lambda item: item[0])
    if max_groups > 0:
        migration_groups = migration_groups[:max_groups]
    operations: list[dict[str, Any]] = []
    for durable_key, values in migration_groups:
        values = sorted(
            values,
            key=lambda row: (
                str(row.get("_ydb_updated_at") or row.get("updated_at") or ""),
                sum(value not in (None, "", [], {}) for value in row.values()),
            ),
        )
        merged: dict[str, Any] = {}
        for row in values:
            merged = rt.merge_durable_post_records(merged, clean_payload(row))
        canonical_pk = "processed_post_item:" + durable_key
        old_pks = sorted({str(row.get("_ydb_pk") or "") for row in values if row.get("_ydb_pk")})
        operations.append({
            "durable_key": durable_key,
            "canonical_pk": canonical_pk,
            "source_pks": old_pks,
            "delete_pks": [pk for pk in old_pks if pk != canonical_pk],
            "merged_payload": merged,
        })
    return {
        "rows_total": len(rows),
        "unique_durable_posts_total": len(grouped),
        "migration_groups_selected": len(operations),
        "duplicate_groups_selected": sum(1 for op in operations if len(op["source_pks"]) > 1),
        "legacy_singleton_groups_selected": sum(
            1 for op in operations
            if len(op["source_pks"]) == 1 and op["source_pks"][0] != op["canonical_pk"]
        ),
        "duplicate_rows_selected": sum(len(op["source_pks"]) - 1 for op in operations),
        "legacy_rows_to_delete_selected": sum(len(op["delete_pks"]) for op in operations),
        "operations": operations,
    }


def read_rows(pool: Any, ydb: Any, table_path: str, *, limit: int) -> list[dict[str, Any]]:
    def op(session: Any) -> list[dict[str, Any]]:
        return list(rt.ydb_select_kind_items(session, ydb, table_path, "processed_post_item", limit=limit).values())
    return list(pool.retry_operation_sync(op) or [])


def execute_plan(pool: Any, ydb: Any, table_path: str, plan: dict[str, Any], *, driver: Any | None = None) -> dict[str, int]:
    operations = list(plan.get("operations") or [])
    if not operations:
        return {"upserted": 0, "deleted": 0}
    now = datetime.now(timezone.utc).isoformat()
    upserts = [
        (str(op["canonical_pk"]), "processed_post_item", dict(op["merged_payload"]))
        for op in operations
    ]

    if driver is not None:
        upserted = int(rt.ydb_bulk_upsert_json_many(driver, ydb, table_path, upserts, now, chunk_size=500) or 0)
    else:
        def write_op(session: Any) -> int:
            rt.ensure_ydb_kv_table(ydb, session, table_path)
            return rt.ydb_upsert_json_many(session, ydb, table_path, upserts, now, chunk_size=50, timeout_seconds=12)
        upserted = int(pool.retry_operation_sync(write_op) or 0)
    delete_pks = [pk for op in operations for pk in op.get("delete_pks") or []]

    def delete_op(session: Any) -> int:
        query = session.prepare(f"""
DECLARE $rows AS List<Struct<pk:Utf8>>;
DELETE FROM `{table_path}` ON
SELECT pk FROM AS_TABLE($rows);
""")
        deleted = 0
        for start in range(0, len(delete_pks), 500):
            batch = delete_pks[start:start + 500]
            session.transaction(ydb.SerializableReadWrite()).execute(
                query,
                {"$rows": [{"pk": pk} for pk in batch]},
                commit_tx=True,
            )
            deleted += len(batch)
        return deleted

    deleted = int(pool.retry_operation_sync(delete_op) or 0) if delete_pks else 0
    return {"upserted": upserted, "deleted": deleted}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--max-groups", type=int, default=200)
    parser.add_argument("--scan-limit", type=int, default=25000)
    parser.add_argument("--artifact", default="")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--no-yc-fallback", action="store_true")
    args = parser.parse_args()
    load_env(Path(args.env_file))
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database(allow_yc_fallback=not args.no_yc_fallback)
    driver = ydb.Driver(
        endpoint=endpoint,
        database=database,
        credentials=ydb_credentials(ydb, allow_yc_fallback=not args.no_yc_fallback),
    )
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table_path = database.rstrip("/") + "/" + rt.ydb_table_name("state_kv")
    try:
        rows = read_rows(pool, ydb, table_path, limit=max(1, args.scan_limit))
        plan = normalize_plan(rows, max_groups=max(0, args.max_groups))
        result = execute_plan(pool, ydb, table_path, plan, driver=driver) if args.execute else {"upserted": 0, "deleted": 0}
        output = {
            "mode": "execute" if args.execute else "dry_run",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            **{key: value for key, value in plan.items() if key != "operations"},
            **result,
            "sample_operations": [
                {key: value for key, value in op.items() if key != "merged_payload"}
                for op in plan["operations"][:20]
            ],
        }
        artifact = Path(args.artifact) if args.artifact else ROOT / "artifacts" / "codex" / "region-talk-post-row-normalize.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({**output, "artifact": str(artifact)}, ensure_ascii=False, indent=2))
    finally:
        driver.stop(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
