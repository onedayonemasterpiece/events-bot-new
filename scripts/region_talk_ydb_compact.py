#!/usr/bin/env python3
"""Copy Region Talk KV state into a compact, LZ4-compressed namespace.

The command is dry-run by default.  It never mutates the source table and it
does not drop either table.  ``--execute`` creates/replaces only the explicitly
named target table, transforms current logical rows, validates the copy and
writes an audit JSON report.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import struct
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_ydb_cost import (  # noqa: E402
    YdbCostBudget,
    payload_size_bytes,
    validate_expected_database,
)


RESEARCH_KINDS = {
    "embeddinggemma_300m_enrichment_item",
    "embeddinggemma_300m_enrichment_result",
    "e5_fulltext_multilingual_base_enrichment_item",
    "e5_fulltext_multilingual_base_enrichment_result",
    "e5_multilingual_base_enrichment_item",
    "e5_multilingual_base_enrichment_result",
    "fulltext_validation_item",
    "qwen3_embedding_0_6b_enrichment_item",
    "qwen3_embedding_0_6b_enrichment_result",
    "qwen3_embedding_4b_enrichment_item",
    "qwen3_embedding_4b_enrichment_result",
    "qwen3_embedding_8b_enrichment_item",
    "qwen3_embedding_8b_enrichment_result",
    "vector_probe_result",
}

RETENTION = {
    "run_state_snapshot": 1,
    "run_metrics": 90,
    "business_event": 500,
    "business_heartbeat": 20,
    "business_heartbeat_bge_m3_enrichment": 20,
    "business_heartbeat_image_diagnostic": 20,
    "online_stats": 20,
    "queue_cursor": 100,
    "bge_m3_enrichment_result": 4,
    "region_talk_stats_snapshot": 1,
    "queue_metrics": 1,
}

CRITICAL_KINDS = {
    "source_queue_item",
    "processed_post_item",
    "candidate_memory_item",
    "post_link_queue_item",
    "text_vector_enrichment_item",
    "image_queue_item",
    "publication_candidate_item",
    "publication_delivery_item",
    "region_talk_llm_budget_item",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key:
            os.environ.setdefault(key, value)


def namespace_table(database: str, namespace: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", namespace).strip("_") or "region_talk"
    return database.rstrip("/") + "/" + safe + "_state_kv"


def ydb_credentials(ydb: Any) -> Any:
    token = (os.getenv("REGION_TALK_YDB_IAM_TOKEN") or os.getenv("YC_IAM_TOKEN") or os.getenv("YDB_ACCESS_TOKEN") or "").strip()
    if token:
        return ydb.AccessTokenCredentials(token)
    key_json = (os.getenv("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON") or "").strip()
    if key_json:
        return ydb.ServiceAccountCredentials.from_file(key_json) if Path(key_json).exists() else ydb.ServiceAccountCredentials.from_json(key_json)
    return ydb.credentials_from_env_variables()


def connect_ydb() -> tuple[Any, Any, str]:
    import ydb  # type: ignore

    endpoint = (os.getenv("REGION_TALK_YDB_ENDPOINT") or os.getenv("YDB_ENDPOINT") or "").strip()
    database = (os.getenv("REGION_TALK_YDB_DATABASE") or os.getenv("YDB_DATABASE") or "").strip()
    if "?database=" in endpoint and not database:
        endpoint, database = endpoint.split("?database=", 1)
    if not endpoint or not database:
        raise RuntimeError("REGION_TALK_YDB_ENDPOINT and REGION_TALK_YDB_DATABASE are required")
    database = validate_expected_database(database, require_expected=True)
    driver = ydb.Driver(endpoint=endpoint.rstrip("/?"), database=database, credentials=ydb_credentials(ydb))
    driver.wait(timeout=10, fail_fast=True)
    return ydb, driver, database


def payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def read_all_rows(
    ydb: Any,
    driver: Any,
    table_path: str,
    *,
    page_size: int = 500,
    budget: YdbCostBudget | None = None,
) -> list[dict[str, Any]]:
    pool = ydb.SessionPool(driver)

    def op(session: Any) -> list[dict[str, Any]]:
        after = ""
        out: list[dict[str, Any]] = []
        while True:
            if budget is not None:
                budget.before_query("compactor.read_page")
            query = session.prepare(f"""
DECLARE $after AS Utf8;
SELECT pk, kind, payload_json, updated_at FROM `{table_path}`
WHERE pk > $after
  AND kind NOT IN ('state_snapshot', 'run_state_snapshot', 'bge_m3_enrichment_result')
ORDER BY pk
LIMIT {max(1, int(page_size))};
""")
            result_sets = session.transaction(ydb.StaleReadOnly()).execute(query, {"$after": after}, commit_tx=True)
            rows = result_sets[0].rows if result_sets else []
            if budget is not None:
                budget.record_read(
                    "compactor.read_page",
                    len(rows),
                    sum(
                        payload_size_bytes(getattr(row, "payload_json", ""))
                        + payload_size_bytes(getattr(row, "pk", ""))
                        + payload_size_bytes(getattr(row, "kind", ""))
                        for row in rows
                    ),
                )
            if not rows:
                break
            for row in rows:
                out.append({
                    "pk": str(row.pk),
                    "kind": str(row.kind),
                    "payload": payload_dict(row.payload_json),
                    "updated_at": str(row.updated_at or ""),
                })
                after = str(row.pk)
            if len(rows) < page_size:
                break
        # Large historical snapshots/results can exceed the YDB response limit
        # when paged together. Read only the current singleton rows needed by
        # the compact production state; old run copies are intentionally
        # discarded by retention.
        special_queries = [
            f"SELECT pk, kind, payload_json, updated_at FROM `{table_path}` WHERE pk = 'latest_state';",
            f"SELECT pk, kind, payload_json, updated_at FROM `{table_path}` WHERE kind = 'run_state_snapshot' ORDER BY updated_at DESC LIMIT 1;",
            f"SELECT pk, kind, payload_json, updated_at FROM `{table_path}` WHERE pk = 'bge_m3_enrichment_result:latest';",
        ]
        seen = {row["pk"] for row in out}
        for sql in special_queries:
            if budget is not None:
                budget.before_query("compactor.read_special")
            result_sets = session.transaction(ydb.StaleReadOnly()).execute(sql, commit_tx=True)
            special_rows = result_sets[0].rows if result_sets else []
            if budget is not None:
                budget.record_read(
                    "compactor.read_special",
                    len(special_rows),
                    sum(payload_size_bytes(getattr(row, "payload_json", "")) for row in special_rows),
                )
            for row in special_rows:
                pk = str(row.pk)
                if pk in seen:
                    continue
                out.append({
                    "pk": pk,
                    "kind": str(row.kind),
                    "payload": payload_dict(row.payload_json),
                    "updated_at": str(row.updated_at or ""),
                })
                seen.add(pk)
        return out

    return pool.retry_operation_sync(op)


def table_max_updated_at(
    ydb: Any,
    driver: Any,
    table_path: str,
    *,
    budget: YdbCostBudget | None = None,
) -> str:
    pool = ydb.SessionPool(driver)

    def op(session: Any) -> str:
        if budget is not None:
            budget.before_query("compactor.watermark")
        result_sets = session.transaction(ydb.StaleReadOnly()).execute(
            f"SELECT MAX(updated_at) AS max_updated_at FROM `{table_path}`;",
            commit_tx=True,
        )
        rows = result_sets[0].rows if result_sets else []
        if budget is not None:
            budget.record_read(
                "compactor.watermark",
                len(rows),
                sum(payload_size_bytes(getattr(row, "max_updated_at", "")) for row in rows),
            )
        return str(rows[0].max_updated_at or "") if rows else ""

    return pool.retry_operation_sync(op)


def slim_checkpoint(payload: dict[str, Any]) -> dict[str, Any]:
    source_queue = payload.get("unified_source_queue") if isinstance(payload.get("unified_source_queue"), dict) else {}
    image_queue = payload.get("image_candidate_queue") if isinstance(payload.get("image_candidate_queue"), dict) else {}
    return {
        "run_id": payload.get("run_id"),
        "state_schema_version": "region-talk-ydb-checkpoint-v4",
        "queue_contract_version": payload.get("queue_contract_version") or "ydb_row_level_unified_source_queue_v2_and_image_candidate_queue_v2",
        "full_state_schema_version": payload.get("full_state_schema_version"),
        "updated_at": payload.get("updated_at"),
        "all_time_metrics": payload.get("all_time_metrics") or {},
        "run_funnel_metrics": payload.get("run_funnel_metrics") or {},
        "source_cursors": payload.get("source_cursors") or {},
        "telegram_cooldowns": payload.get("telegram_cooldowns") or {},
        "publication_goal": payload.get("publication_goal") or {},
        "unified_source_queue_cursor_position": payload.get("unified_source_queue_cursor_position", 0),
        "unified_source_queue_cursor_key": payload.get("unified_source_queue_cursor_key", ""),
        "unified_source_queue_total": int(payload.get("unified_source_queue_total") or len(source_queue)),
        "image_candidate_queue_cursor_position": payload.get("image_candidate_queue_cursor_position", 0),
        "image_candidate_queue_cursor_key": payload.get("image_candidate_queue_cursor_key", ""),
        "image_candidate_queue_total": int(payload.get("image_candidate_queue_total") or len(image_queue)),
        "queue_cursors": payload.get("queue_cursors") or {},
        "row_level_state_contract": "all_product_entities_reconstructed_from_kind_rows",
    }


def f16_b64(vector: list[Any]) -> str:
    values = [float(value) for value in vector]
    if not values:
        return ""
    return base64.b64encode(struct.pack("<" + ("e" * len(values)), *values)).decode("ascii")


def durable_post_key(payload: dict[str, Any], fallback_pk: str = "") -> str:
    platform_key = str(payload.get("platform_post_key") or "").strip()
    match = re.fullmatch(r"(?:tg|telegram):@?([^:]+):(\d+)", platform_key, flags=re.I)
    if match:
        return f"tg:{match.group(1).lower()}:{match.group(2)}"
    match = re.fullmatch(r"vk:([^:]+):(\d+)", platform_key, flags=re.I)
    if match:
        return f"vk:{match.group(1).lower()}:{match.group(2)}"
    post_url = str(payload.get("post_url") or "").strip().split("?", 1)[0].rstrip("/")
    match = re.search(r"(?:https?://)?(?:t|telegram)\.me/(?:s/)?(?!c/)([a-z0-9_]+)/(\d+)$", post_url, flags=re.I)
    if match:
        return f"tg:{match.group(1).lower()}:{match.group(2)}"
    match = re.search(r"(?:https?://)?vk\.com/wall(-?\d+)_(\d+)$", post_url, flags=re.I)
    if match:
        return f"vk:{match.group(1)}:{match.group(2)}"
    fallback = re.sub(r"^(?:processed_post_item|post_live_item):", "", fallback_pk)
    return str(payload.get("post_id") or fallback or post_url).strip()


def merge_nonempty(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    merged = dict(previous)
    for key, value in current.items():
        if value not in (None, "", [], {}) or key not in merged:
            merged[key] = value
    return merged


def transform_vector(payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
    out = dict(payload)
    saved = 0
    vector = out.pop("embedding_vector", None)
    if isinstance(vector, list) and vector:
        before = len(json.dumps(vector, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        encoded = f16_b64(vector)
        out["embedding_vector_f16_b64"] = encoded
        out["embedding_vector_encoding"] = "f16_le_base64"
        out["embedding_dim"] = int(out.get("embedding_dim") or len(vector))
        saved += max(0, before - len(encoded.encode("ascii")))
    if str(out.get("model_short") or "") == "bge_m3" or str(out.get("model_id") or "") == "BAAI/bge-m3":
        excerpt = out.pop("text_excerpt", None)
        if excerpt:
            saved += len(str(excerpt).encode("utf-8"))
    return out, saved


def retention_keep_pks(rows: list[dict[str, Any]]) -> set[str]:
    keep = {row["pk"] for row in rows if row["kind"] not in RETENTION}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["kind"] in RETENTION:
            grouped[row["kind"]].append(row)
    protected = {
        "latest_business_heartbeat",
        "online_stats:latest",
        "queue_cursor:source",
        "queue_cursor:image",
        "queue_cursor:source_scan",
        "queue_cursor:image_diagnostic",
        "bge_m3_enrichment_result:latest",
        "queue_metrics:latest",
    }
    keep.update(pk for pk in protected if any(row["pk"] == pk for row in rows))
    for kind, kind_rows in grouped.items():
        kind_rows.sort(key=lambda row: (row.get("updated_at") or "", row["pk"]), reverse=True)
        keep.update(row["pk"] for row in kind_rows[: RETENTION[kind]])
    return keep


def transform_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    keep_pks = retention_keep_pks(rows)
    transformed: list[dict[str, Any]] = []
    dropped = Counter()
    saved_vector_bytes = 0
    # Reconcile both historical post projections before dropping the mirror.
    # This proves that a legacy-only observation is carried into the sole
    # ``processed_post_item`` representation instead of being discarded.
    post_groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row["kind"] not in {"processed_post_item", "post_live_item"}:
            continue
        key = durable_post_key(row["payload"], row["pk"])
        if not key:
            continue
        current = post_groups.get(key)
        if current is None:
            post_groups[key] = {**row, "pk": "processed_post_item:" + key, "kind": "processed_post_item"}
        else:
            newer = row if (row.get("updated_at") or "") >= (current.get("updated_at") or "") else current
            older = current if newer is row else row
            post_groups[key] = {
                "pk": "processed_post_item:" + key,
                "kind": "processed_post_item",
                "payload": merge_nonempty(older["payload"], newer["payload"]),
                "updated_at": max(str(current.get("updated_at") or ""), str(row.get("updated_at") or "")),
            }
    for row in rows:
        kind = row["kind"]
        if kind in {"processed_post_item", "post_live_item"}:
            if kind == "post_live_item":
                dropped["duplicate_post_live_item"] += 1
            continue
        if kind in RESEARCH_KINDS or kind.startswith("business_heartbeat_qwen3_") or kind.startswith("business_heartbeat_embeddinggemma_") or kind.startswith("business_heartbeat_e5_"):
            dropped["completed_embedding_research"] += 1
            continue
        if row["pk"] not in keep_pks:
            dropped["retention"] += 1
            continue
        payload = row["payload"]
        if kind in {"state_snapshot", "run_state_snapshot"}:
            payload = slim_checkpoint(payload)
        elif kind == "text_vector_enrichment_item":
            payload, saved = transform_vector(payload)
            saved_vector_bytes += saved
        elif kind == "bge_m3_enrichment_result":
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            payload = {"summary": summary, "row_count": payload.get("row_count") or summary.get("rows_written") or 0}
        transformed.append({**row, "payload": payload})
    for row in post_groups.values():
        payload = dict(row.get("payload") or {})
        for field in ("text", "full_text", "text_excerpt", "short_summary", "raw"):
            payload.pop(field, None)
        row["payload"] = payload
    transformed.extend(post_groups.values())
    return transformed, {
        "dropped_rows_by_reason": dict(dropped),
        "estimated_vector_bytes_saved": saved_vector_bytes,
        "post_projection_source_rows": sum(1 for row in rows if row["kind"] in {"processed_post_item", "post_live_item"}),
        "post_projection_canonical_rows": len(post_groups),
    }


def validate_target_replacement(*, exists: bool, replace: bool, bootstrap_ack: bool, table_path: str) -> None:
    if exists and not replace:
        raise RuntimeError(f"target table already exists: {table_path}; pass --replace-target")
    if exists and not bootstrap_ack:
        raise RuntimeError(
            "refusing to replace an existing target without "
            "--bootstrap-acknowledge-target-replacement"
        )


def ensure_target_table(ydb: Any, driver: Any, table_path: str, *, replace: bool, bootstrap_ack: bool) -> None:
    pool = ydb.SessionPool(driver)

    def op(session: Any) -> None:
        exists = True
        try:
            session.describe_table(table_path)
        except Exception:
            exists = False
        validate_target_replacement(exists=exists, replace=replace, bootstrap_ack=bootstrap_ack, table_path=table_path)
        if exists:
            session.drop_table(table_path)
        desc = (
            ydb.TableDescription()
            .with_column(ydb.Column("pk", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
            .with_column(ydb.Column("kind", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
            .with_column(ydb.Column("payload_json", ydb.OptionalType(ydb.PrimitiveType.Json)))
            .with_column(ydb.Column("updated_at", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
            .with_primary_key("pk")
        )
        session.create_table(table_path, desc)
        session.execute_scheme(f'ALTER TABLE `{table_path}` ALTER FAMILY default SET COMPRESSION "lz4";')

    pool.retry_operation_sync(op)


def bulk_write(
    ydb: Any,
    driver: Any,
    table_path: str,
    rows: list[dict[str, Any]],
    *,
    chunk_size: int,
    budget: YdbCostBudget | None = None,
) -> int:
    columns = (
        ydb.BulkUpsertColumns()
        .add_column("pk", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("kind", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column("payload_json", ydb.OptionalType(ydb.PrimitiveType.Json))
        .add_column("updated_at", ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )
    written = 0
    for start in range(0, len(rows), max(1, chunk_size)):
        chunk = [
            {
                "pk": row["pk"],
                "kind": row["kind"],
                "payload_json": json.dumps(row["payload"], ensure_ascii=False, separators=(",", ":")),
                "updated_at": row["updated_at"] or utc_now_iso(),
            }
            for row in rows[start:start + chunk_size]
        ]
        if budget is not None:
            budget.before_query("compactor.bulk_upsert")
            budget.record_write(
                "compactor.bulk_upsert",
                len(chunk),
                sum(payload_size_bytes(row) for row in chunk),
            )
        driver.table_client.bulk_upsert(table_path, chunk, columns)
        written += len(chunk)
        if written % 5000 == 0 or written == len(rows):
            print(f"[region-talk-ydb-compact] written={written}/{len(rows)}", flush=True)
    return written


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_kind = Counter(row["kind"] for row in rows)
    payload_bytes = sum(len(json.dumps(row["payload"], ensure_ascii=False, separators=(",", ":")).encode("utf-8")) for row in rows)
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda item: item["pk"]):
        digest.update(row["pk"].encode("utf-8"))
        digest.update(row["kind"].encode("utf-8"))
        digest.update(json.dumps(row["payload"], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return {"rows_total": len(rows), "payload_bytes": payload_bytes, "rows_by_kind": dict(sorted(by_kind.items())), "logical_digest": digest.hexdigest()}


def validate(source: list[dict[str, Any]], target: list[dict[str, Any]]) -> dict[str, Any]:
    source_counts = Counter(row["kind"] for row in source)
    target_counts = Counter(row["kind"] for row in target)
    source_post_keys = {
        durable_post_key(row["payload"], row["pk"])
        for row in source if row["kind"] in {"processed_post_item", "post_live_item"}
    }
    target_post_keys = {
        durable_post_key(row["payload"], row["pk"])
        for row in target if row["kind"] == "processed_post_item"
    }
    mismatches = {
        kind: {"source": source_counts[kind], "target": target_counts[kind]}
        for kind in sorted(CRITICAL_KINDS)
        if kind != "processed_post_item" and source_counts[kind] != target_counts[kind]
    }
    target_pks = {row["pk"] for row in target}
    required_pks = {"latest_state"}
    missing_required = sorted(required_pks - target_pks)
    return {
        "critical_kind_count_mismatches": mismatches,
        "source_post_canonical_total": len(source_post_keys),
        "target_post_canonical_total": len(target_post_keys),
        "missing_post_canonical_keys": sorted(source_post_keys - target_post_keys)[:100],
        "missing_required_pks": missing_required,
        "ok": not mismatches and not missing_required and source_post_keys == target_post_keys,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--source-namespace", default="region_talk")
    parser.add_argument("--target-namespace", default="region_talk_compact")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--replace-target", action="store_true")
    parser.add_argument("--bootstrap-acknowledge-target-replacement", action="store_true")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--max-queries", type=int, default=500)
    parser.add_argument("--max-rows-read", type=int, default=100000)
    parser.add_argument("--max-bytes-read", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--max-rows-written", type=int, default=100000)
    parser.add_argument("--max-bytes-written", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--max-estimated-io-ru", type=int, default=250000)
    parser.add_argument("--report", default="artifacts/codex/region-talk-ydb-compact-report.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(Path(args.env_file))
    budget = YdbCostBudget(
        max_queries=max(1, int(args.max_queries)),
        max_rows_read=max(1, int(args.max_rows_read)),
        max_bytes_read=max(1, int(args.max_bytes_read)),
        max_rows_written=max(1, int(args.max_rows_written)),
        max_bytes_written=max(1, int(args.max_bytes_written)),
        max_estimated_io_ru=max(1, int(args.max_estimated_io_ru)),
        label="region_talk_compactor",
    )
    ydb, driver, database = connect_ydb()
    source_table = namespace_table(database, args.source_namespace)
    target_table = namespace_table(database, args.target_namespace)
    try:
        source_watermark_before = table_max_updated_at(ydb, driver, source_table, budget=budget)
        source_rows = read_all_rows(ydb, driver, source_table, page_size=args.page_size, budget=budget)
        source_watermark_after = table_max_updated_at(ydb, driver, source_table, budget=budget)
        if source_watermark_before != source_watermark_after:
            raise RuntimeError(
                "source table changed during migration read; stop writers and retry: "
                f"before={source_watermark_before} after={source_watermark_after}"
            )
        transformed, transform_meta = transform_rows(source_rows)
        report: dict[str, Any] = {
            "generated_at": utc_now_iso(),
            "execute": bool(args.execute),
            "source_table": source_table,
            "target_table": target_table,
            "source": summarize(source_rows),
            "planned_target": summarize(transformed),
            "transform": transform_meta,
            "source_watermark_before": source_watermark_before,
            "source_watermark_after": source_watermark_after,
            "ydb_cost_budget": budget.snapshot(),
        }
        if args.execute:
            ensure_target_table(
                ydb,
                driver,
                target_table,
                replace=bool(args.replace_target),
                bootstrap_ack=bool(args.bootstrap_acknowledge_target_replacement),
            )
            report["written_rows"] = bulk_write(
                ydb, driver, target_table, transformed,
                chunk_size=args.chunk_size, budget=budget,
            )
            target_rows = read_all_rows(
                ydb, driver, target_table,
                page_size=args.page_size, budget=budget,
            )
            report["actual_target"] = summarize(target_rows)
            report["validation"] = validate(source_rows, target_rows)
            report["ydb_cost_budget"] = budget.snapshot()
        out = Path(args.report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if not args.execute or report.get("validation", {}).get("ok") else 2
    finally:
        driver.stop(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
