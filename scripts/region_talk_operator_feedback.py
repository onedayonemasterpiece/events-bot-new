#!/usr/bin/env python3
"""Persist idempotent operator feedback for Region Talk post URLs.

The workbook previously exposed a ``manual_decision`` column without an
importer, so a rejected exact link could be fetched again. This command writes
an append-only audit event plus a URL-level latest projection and applies a
monotonic terminal state to existing funnel rows. It is dry-run unless
``--execute`` is supplied.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.region_talk_orchestrator as orch  # noqa: E402
from scripts.region_talk_goal_notify import load_env, read_kind_rows  # noqa: E402
from scripts.region_talk_publication_finalizer import normalize_post_url, rt  # noqa: E402


KINDS = [
    "post_link_queue_item",
    "processed_post_item",
    "candidate_memory_item",
    "image_queue_item",
    "publication_candidate_item",
]


def stable_id(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()[:24]


def apply_decision(kind: str, row: dict[str, Any], *, decision: str, reason: str, at: str) -> dict[str, Any]:
    out = {k: v for k, v in row.items() if not str(k).startswith("_ydb_")}
    out.update({
        "operator_decision": decision,
        "operator_decision_reason": reason,
        "operator_decision_at": at,
        "operator_decision_version": "region_talk_operator_feedback_v1",
    })
    if decision == "reject":
        out.pop("text", None)
        out.pop("full_text", None)
        out.pop("text_excerpt", None)
        out.pop("short_summary", None)
        out["manual_decision"] = "reject"
        if kind == "post_link_queue_item":
            out.update({
                "post_link_status": "operator_rejected",
                "next_action": "do_not_fetch_operator_rejected_post",
                "fetch_error_code": "operator_rejected",
                "fetch_error_message": reason,
            })
        elif kind == "processed_post_item":
            out.update({
                "current_stage": "operator_rejected",
                "post_observation_status": "operator_rejected",
                "drop_gate": "operator_feedback",
                "rejection_reason": "operator_rejected",
                "rejection_reason_primary": "operator_rejected",
            })
        elif kind == "candidate_memory_item":
            out.update({
                "current_stage": "operator_rejected",
                "current_lifecycle_status": "manual_reject",
                "next_action": "do_not_reopen_operator_rejected_post",
            })
        elif kind == "image_queue_item":
            out.update({
                "image_queue_status": "rejected_publication_eligibility",
                "publication_eligibility_decision": "reject",
                "publication_eligibility_reason": "operator_rejected",
                "next_action": "do_not_score_operator_rejected_post",
            })
        elif kind == "publication_candidate_item":
            out.update({
                "publication_status": "operator_rejected",
                "publication_candidate_status": "tombstoned_reject",
                "finalization_status": "terminal",
                "finalization_trigger": "operator_feedback",
                "publication_tombstone": "true",
            })
    else:
        # Human calibration evidence is not an automatic semantic override.
        # It makes the row visible for review while retaining freshness/source
        # and Gemini safeguards.
        out["manual_decision"] = "manual_review"
        out["next_action"] = "operator_review_requested"
        if kind == "post_link_queue_item":
            out.update({
                "post_link_status": "retry_fetch",
                "post_link_priority": 0,
                "priority_reason": "operator_calibration_review",
                "next_attempt_after": at,
                "next_action": "refetch_and_rescore_operator_calibration_case",
            })
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path("/home/dev/projects/events-bot-new/.env"))
    parser.add_argument("--decision", choices=["reject", "review"], required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--reviewer", default="product_owner")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("urls", nargs="+")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env(args.env_file)
    orch.ensure_child_ydb_env(allow_yc_fallback=True)
    ydb = orch.ensure_ydb_module()
    endpoint, database = orch.ydb_endpoint_database(allow_yc_fallback=True)
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=orch.ydb_credentials(ydb, allow_yc_fallback=True))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = orch.ydb_table_path(database)
    now = datetime.now(timezone.utc).isoformat()
    urls = sorted({normalize_post_url(url) for url in args.urls if normalize_post_url(url)})
    rows_by_kind = {kind: read_kind_rows(pool, ydb, table, kind, 20000) for kind in KINDS}
    items: list[tuple[str, str, dict[str, Any]]] = []
    matched: dict[str, dict[str, int]] = {url: {} for url in urls}
    for url in urls:
        event_id = stable_id(url, args.decision, args.reason)
        event = {
            "operator_feedback_id": event_id,
            "post_url": url,
            "decision": args.decision,
            "reason": args.reason,
            "reviewer": args.reviewer,
            "created_at": now,
            "feedback_version": "region_talk_operator_feedback_v1",
        }
        items.append((f"operator_feedback_item:{event_id}", "operator_feedback_item", event))
        items.append((f"operator_feedback_latest_item:{stable_id(url)}", "operator_feedback_latest_item", event))
        for kind, rows in rows_by_kind.items():
            count = 0
            for row in rows:
                if normalize_post_url(str(row.get("post_url") or row.get("keyword_hit_post_url") or "")) != url:
                    continue
                pk = str(row.get("_ydb_pk") or "")
                if not pk:
                    continue
                payload = apply_decision(kind, row, decision=args.decision, reason=args.reason, at=now)
                items.append((pk, kind, payload))
                count += 1
            if count:
                matched[url][kind] = count

    written = 0
    if args.execute and items:
        def op(session: Any) -> int:
            rt.ensure_ydb_kv_table(ydb, session, table)
            return rt.ydb_upsert_json_many(session, ydb, table, items, now, chunk_size=50, timeout_seconds=10)
        written = int(pool.retry_operation_sync(op) or 0)
    driver.stop(timeout=5)
    print(json.dumps({
        "decision": args.decision,
        "urls": urls,
        "matched_rows": matched,
        "planned_writes": len(items),
        "written": written,
        "executed": bool(args.execute),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
