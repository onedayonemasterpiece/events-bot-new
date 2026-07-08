#!/usr/bin/env python3
"""Dry-run Region Talk queue orchestrator.

Reads live YDB queues and prints a machine-readable decision plan. By default it
never launches Kaggle kernels or sends Telegram messages; add ``--execute`` only
after the dry-run plan looks correct.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (  # noqa: E402
    build_stats_message,
    is_confirmed_publication,
    is_unsent_confirmed_publication,
    ensure_ydb_module,
    load_env,
    read_kind_rows,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_has_direct_credential,
    ydb_table_path,
)


def _load_bge_module() -> Any:
    path = ROOT / "kaggle" / "RegionTalkBgeM3Enrichment" / "region_talk_bge_m3_enrichment.py"
    spec = importlib.util.spec_from_file_location("region_talk_bge_m3_enrichment_orchestrator", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rows_by_pk(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(r.get("_ydb_pk") or r.get("post_url") or r.get("post_id") or i): r for i, r in enumerate(rows)}


def _run_cmd(cmd: list[str], *, dry_run: bool) -> dict[str, Any]:
    if dry_run:
        return {"cmd": cmd, "status": "dry_run"}
    proc = subprocess.run(cmd, cwd=str(ROOT), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
    return {"cmd": cmd, "status": "ok" if proc.returncode == 0 else "failed", "returncode": proc.returncode, "output_tail": proc.stdout[-4000:]}


def read_region_talk_queue_metrics(limit: int, *, bge_sample_limit: int, allow_yc_fallback: bool = False) -> dict[str, Any]:
    ydb = ensure_ydb_module()

    endpoint, database = ydb_endpoint_database(allow_yc_fallback=allow_yc_fallback)
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb, allow_yc_fallback=allow_yc_fallback))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    try:
        kinds = [
            "candidate_memory_item",
            "image_queue_item",
            "publication_candidate_item",
            "text_vector_enrichment_item",
            "processed_post_item",
            "post_live_item",
        ]
        rows_by_kind = {kind: read_kind_rows(pool, ydb, table, kind, limit) for kind in kinds}
    finally:
        driver.stop()

    candidates = rows_by_kind["candidate_memory_item"]
    images = rows_by_kind["image_queue_item"]
    publications = rows_by_kind["publication_candidate_item"]
    vectors = rows_by_kind["text_vector_enrichment_item"]
    bge_mod = _load_bge_module()
    item_kinds_for_bge = {
        "text_vector_enrichment_item": _rows_by_pk(vectors),
        "publication_candidate_item": _rows_by_pk(publications),
        "candidate_memory_item": _rows_by_pk(candidates),
        "image_queue_item": _rows_by_pk(images),
        "processed_post_item": _rows_by_pk(rows_by_kind["processed_post_item"]),
        "post_live_item": _rows_by_pk(rows_by_kind["post_live_item"]),
    }
    bge_pending_rows = bge_mod.collect_text_rows(
        item_kinds_for_bge,
        existing_pks={str(v.get("_ydb_pk") or "") for v in vectors},
        limit=bge_sample_limit,
    )

    image_pending = [r for r in images if str(r.get("image_queue_status") or "") in {"", "needs_actual_image_fetch", "selected_for_next_image_batch"}]
    image_in_progress = [r for r in images if str(r.get("image_queue_status") or "") == "image_analysis_in_progress"]
    image_actual = [r for r in images if str(r.get("image_queue_status") or "") == "actual_scored" and str(r.get("image_model_input_type") or "") == "actual_image"]
    confirmed = [r for r in publications if is_confirmed_publication(r)]
    unsent_confirmed = [r for r in publications if is_unsent_confirmed_publication(r)]
    sent = [r for r in publications if str(r.get("sent_to_chat") or "").lower() == "true" or str(r.get("publication_candidate_status") or "") == "sent_to_chat"]
    e5_vectors = [r for r in vectors if str(r.get("model_short") or "") == "e5" or str(r.get("model_id") or "").startswith("intfloat/multilingual-e5")]
    bge_vectors = [r for r in vectors if str(r.get("model_short") or "") == "bge_m3" or str(r.get("model_id") or "") == "BAAI/bge-m3"]

    return {
        "candidate_memory_total": len(candidates),
        "image_queue_total": len(images),
        "image_pending_total": len(image_pending),
        "image_in_progress_total": len(image_in_progress),
        "image_actual_scored_total": len(image_actual),
        "publication_candidate_total": len(publications),
        "publication_confirmed_total": len(confirmed),
        "publication_sent_total": len(sent),
        "publication_unsent_confirmed_total": len(unsent_confirmed),
        "text_vector_enrichment_total": len(vectors),
        "text_vector_e5_total": len(e5_vectors),
        "text_vector_bge_m3_total": len(bge_vectors),
        "bge_pending_sample_total": len(bge_pending_rows),
        "bge_pending_sample_limit": bge_sample_limit,
    }


def build_decision_plan(metrics: dict[str, Any], *, target_confirmed: int, bge_threshold: int, image_threshold: int) -> list[dict[str, Any]]:
    if int(metrics.get("publication_sent_total") or 0) >= target_confirmed or int(metrics.get("publication_confirmed_total") or 0) >= target_confirmed:
        return [{"action": "stop", "reason": "target_confirmed_reached"}]
    actions: list[dict[str, Any]] = []
    if int(metrics.get("publication_unsent_confirmed_total") or 0) > 0:
        actions.append({"action": "notify_confirmed", "cmd": ["python3", "scripts/region_talk_goal_notify.py", "--limit", "20"], "reason": "confirmed rows not sent to operator chat"})
    if int(metrics.get("image_actual_scored_total") or 0) > int(metrics.get("publication_candidate_total") or 0):
        actions.append({"action": "run_finalizer", "cmd": ["python3", "scripts/region_talk_publication_finalizer.py", "--max-llm", "3"], "reason": "actual images exist beyond publication rows"})
    if int(metrics.get("bge_pending_sample_total") or 0) >= bge_threshold:
        actions.append({"action": "launch_bge_m3", "cmd": ["python3", "kaggle/execute_region_talk_bge_m3_enrichment.py", "--batch-size", "12"], "reason": "pending E5/candidate text rows need BGE"})
    if int(metrics.get("image_pending_total") or 0) >= image_threshold:
        actions.append({"action": "launch_image_diagnostic", "cmd": ["python3", "kaggle/execute_region_talk_image_diagnostic.py", "--source", "ydb", "--max-items-per-run", "30", "--batch-size", "10"], "reason": "text-confirmed image queue has pending rows"})
    if not actions:
        actions.append({"action": "launch_candidate_report", "cmd": ["python3", "kaggle/execute_region_talk_candidate_report.py", "--max-sources", "220"], "reason": "no ready consumer action; produce new E5/discovery rows"})
    return actions


def main() -> int:
    parser = argparse.ArgumentParser(description="Region Talk YDB orchestrator dry-run")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--limit", type=int, default=20000)
    parser.add_argument("--bge-sample-limit", type=int, default=100)
    parser.add_argument("--target-confirmed", type=int, default=20)
    parser.add_argument("--bge-threshold", type=int, default=1)
    parser.add_argument("--image-threshold", type=int, default=1)
    parser.add_argument("--stats-message", action="store_true", help="also include human stats text")
    parser.add_argument("--allow-yc-fallback", action="store_true", help="allow local /home/dev/yandex-cloud/bin/yc to discover endpoint/database and mint IAM token")
    parser.add_argument("--execute", action="store_true", help="execute the first planned action (default: dry-run only)")
    args = parser.parse_args()
    load_env(Path(args.env_file))
    allow_yc_fallback = bool(args.allow_yc_fallback or (os.getenv("REGION_TALK_ALLOW_LOCAL_YC_FALLBACK") or "").strip().lower() in {"1", "true", "yes", "on"})
    missing = [
        name for name in ["REGION_TALK_YDB_ENDPOINT", "REGION_TALK_YDB_DATABASE"]
        if not (os.getenv(name) or "").strip()
    ]
    if allow_yc_fallback:
        missing = []
    if not ydb_has_direct_credential() and not allow_yc_fallback:
        missing.append("REGION_TALK_YDB_SERVICE_ACCOUNT_KEY_JSON|REGION_TALK_YDB_IAM_TOKEN|YC_IAM_TOKEN|YDB_ACCESS_TOKEN|YDB_USER")
    if missing:
        print(json.dumps({
            "ok": False,
            "dry_run": not args.execute,
            "error": "missing_ydb_config",
            "missing": missing,
            "next_action": "run from the configured server, export live YDB endpoint/database plus service-account/token credentials, or pass --allow-yc-fallback for local debug",
        }, ensure_ascii=False, indent=2))
        return 2
    try:
        metrics = read_region_talk_queue_metrics(args.limit, bge_sample_limit=args.bge_sample_limit, allow_yc_fallback=allow_yc_fallback)
    except Exception as exc:
        print(json.dumps({
            "ok": False,
            "dry_run": not args.execute,
            "error": f"{type(exc).__name__}: {str(exc)[:500]}",
            "next_action": "provide REGION_TALK_YDB_ENDPOINT/REGION_TALK_YDB_DATABASE and service-account/token credentials, or retry local debug with --allow-yc-fallback",
        }, ensure_ascii=False, indent=2))
        return 2
    actions = build_decision_plan(metrics, target_confirmed=args.target_confirmed, bge_threshold=args.bge_threshold, image_threshold=args.image_threshold)
    result: dict[str, Any] = {"ok": True, "dry_run": not args.execute, "metrics": metrics, "actions": actions}
    if args.stats_message:
        result["stats_message"] = build_stats_message(limit=args.limit)
    if args.execute and actions and actions[0].get("cmd"):
        result["execution"] = _run_cmd(list(actions[0]["cmd"]), dry_run=False)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
