#!/usr/bin/env python3
"""Compact Region Talk product state without losing funnel decisions.

Dry-run by default. With ``--execute`` it:

* converges duplicate publication rows to one normalized URL primary key;
* removes post text from terminal rows while retaining hashes/scores/reasons;
* labels historical videos as manual-review media instead of failed images;
* compacts terminal image-ledger payloads.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
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
    "processed_post_item", "candidate_memory_item", "image_queue_item",
    "publication_candidate_item", "text_vector_enrichment_item", "post_link_queue_item",
    "online_source_item",
]
TEXT_FIELDS = {
    "text", "full_text", "text_excerpt", "short_summary", "raw",
    "why_keep_in_memory", "keyword_hit_text_excerpt",
}
IMAGE_VERBOSE_TERMINAL_FIELDS = {
    "publication_eligibility_evidence_json", "image_url_or_local_path",
    "primary_media_path", "vk_media_photo_urls", "media_fetch_error",
}
IMAGE_TERMINAL = {
    "actual_scored", "not_reviewable_no_media", "not_reviewable_unsupported_media",
    "rejected_text_gate", "rejected_publication_eligibility", "rejected_low_score",
    "rejected_image_quality", "broken_media",
}


def is_publication_terminal(row: dict[str, Any]) -> bool:
    if str(row.get("publication_status") or "").lower() in {
        "no_text_for_gemini",
        "text_restore_pending",
    }:
        # Missing working text before the Gemini verdict is recoverable exact-
        # post fetch work, not a semantic rejection and not a reason to erase
        # active candidate/vector text from every projection.
        return False
    return bool(
        str(row.get("sent_to_chat") or "").lower() == "true"
        or str(row.get("finalization_status") or "").lower() == "terminal"
        or str(row.get("llm_decision") or row.get("publication_llm_decision") or "").lower() in {"accept", "reject", "needs_review"}
        or str(row.get("publication_status") or "").lower().startswith(("gemini_", "operator_rejected", "eligibility_"))
        or str(row.get("publication_candidate_status") or "").lower() in {
            "llm_confirmed", "llm_rejected", "llm_needs_review", "filtered_before_llm",
            "sent_to_chat", "accepted_for_publication", "tombstoned_reject", "revoked",
        }
    )


def row_has_terminal_text_verdict(kind: str, row: dict[str, Any], publication_terminal_urls: set[str]) -> bool:
    url = normalize_post_url(str(row.get("post_url") or ""))
    if url and url in publication_terminal_urls:
        return True
    if kind == "processed_post_item":
        stage = str(row.get("current_stage") or "").lower()
        vector = str(row.get("vector_gate_status") or "").lower()
        return bool(
            stage.startswith(("dropped_", "operator_rejected"))
            or vector.startswith("vector_reject")
            or str(row.get("fresh_enough") or "").lower() in {"0", "false", "no"}
        )
    if kind == "candidate_memory_item":
        lifecycle = str(row.get("current_lifecycle_status") or "").lower()
        return bool(
            str(row.get("manual_decision") or "").lower() == "reject"
            or lifecycle.startswith("source_terminal_")
            or lifecycle in {"manual_reject", "vector_reject_after_bge_m3", "expired"}
        )
    if kind == "image_queue_item":
        return str(row.get("image_queue_status") or "") in IMAGE_TERMINAL and not rt.is_video_media_candidate(row)
    if kind == "post_link_queue_item":
        status = str(row.get("post_link_status") or "")
        return status in {
            "fetched", "scored", "terminal_no_text", "terminal_bad_url",
            "terminal_source_rejected", "operator_rejected",
        } or status.startswith("terminal_")
    return False


def publication_precedence(row: dict[str, Any]) -> tuple[int, str]:
    if str(row.get("sent_to_chat") or "").lower() == "true":
        rank = 5
    elif str(row.get("publication_status") or "").lower() in {"gemini_accept", "gemini_reject", "gemini_needs_review"}:
        rank = 4
    elif str(row.get("publication_status") or "").lower().startswith("operator_rejected"):
        rank = 4
    elif str(row.get("finalizer_state_version") or ""):
        rank = 3
    elif is_publication_terminal(row):
        rank = 2
    else:
        rank = 1
    return rank, str(row.get("updated_at") or row.get("_ydb_updated_at") or "")


def merge_publications(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=publication_precedence)
    merged: dict[str, Any] = {}
    for row in ordered:
        for key, value in row.items():
            if not str(key).startswith("_ydb_") and value not in (None, ""):
                merged[key] = value
    merged["post_url"] = normalize_post_url(str(merged.get("post_url") or ""))
    return merged


def is_video_payload(row: dict[str, Any]) -> bool:
    if rt.is_video_media_candidate(row):
        return True
    evidence = " ".join(str(row.get(key) or "") for key in [
        "media_fetch_error", "unsupported_media_path", "primary_media_path", "image_url_or_local_path",
    ]).lower()
    return any(suffix in evidence for suffix in (".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"))


def is_placeholder_media_decode_failure(row: dict[str, Any]) -> bool:
    """Recognize the historical ``#media`` marker downloaded as HTML/JPG."""
    ref = str(row.get("image_url_or_local_path") or row.get("primary_media_path") or "").strip().lower()
    error = str(row.get("media_fetch_error") or "").lower()
    return bool(
        ref.endswith("#media")
        and str(row.get("image_queue_status") or "") == "not_reviewable_unsupported_media"
        and str(row.get("media_fetch_status") or "") == "decode_failed"
        and ("unidentifiedimageerror" in error or "cannot identify image" in error)
    )


def reopen_placeholder_media_row(payload: dict[str, Any], now: str) -> bool:
    if not is_placeholder_media_decode_failure(payload):
        return False
    payload["previous_image_queue_status"] = payload.get("image_queue_status") or "not_reviewable_unsupported_media"
    payload["image_queue_status"] = "needs_actual_image_fetch"
    payload["current_stage"] = "image_fetch_retry_needed"
    payload["current_lifecycle_status"] = "image_fetch_retry_needed"
    payload["media_fetch_status"] = "needs_actual_image_fetch"
    payload["media_acquisition_status"] = "needs_actual_image_fetch"
    payload["media_fetch_error"] = "retry_via_telegram_after_placeholder_media_ref"
    payload["media_fetch_attempt_count"] = 0
    payload["actual_image_count"] = 0
    payload["images_scored_actual_count"] = 0
    payload["next_action"] = "download_actual_telegram_media_bytes_next"
    payload["placeholder_media_retry_reopened_at"] = now
    for field in [
        "actual_media_path", "unsupported_media_path", "final_visual_status",
        "image_diagnostic_error", "image_decode_seconds", "lease_run_id", "lease_at",
    ]:
        payload.pop(field, None)
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=Path("/home/dev/projects/events-bot-new/.env"))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--artifact", type=Path, default=ROOT / "artifacts" / "codex" / "region-talk-state-maintenance.json")
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
    rows = {kind: read_kind_rows(pool, ydb, table, kind, 25000) for kind in KINDS}
    publication_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows["publication_candidate_item"]:
        url = normalize_post_url(str(row.get("post_url") or ""))
        if url:
            publication_groups[url].append(row)
    publication_terminal_urls = {
        url for url, group in publication_groups.items() if any(is_publication_terminal(row) for row in group)
    }
    now = datetime.now(timezone.utc).isoformat()
    upserts: list[tuple[str, str, dict[str, Any]]] = []
    deletes: list[str] = []
    counters: Counter[str] = Counter()

    for row in rows["online_source_item"]:
        pk = str(row.get("_ydb_pk") or "")
        if pk:
            deletes.append(pk)
            counters["legacy_online_source_rows_removed"] += 1

    for url, group in publication_groups.items():
        merged = merge_publications(group)
        canonical_pk = "publication_candidate_item:" + url
        if is_publication_terminal(merged):
            for field in TEXT_FIELDS:
                merged.pop(field, None)
        canonical_current = next(
            ({k: v for k, v in row.items() if not str(k).startswith("_ydb_")} for row in group if str(row.get("_ydb_pk") or "") == canonical_pk),
            None,
        )
        if canonical_current != merged or len(group) > 1:
            upserts.append((canonical_pk, "publication_candidate_item", merged))
        for row in group:
            old_pk = str(row.get("_ydb_pk") or "")
            if old_pk and old_pk != canonical_pk:
                deletes.append(old_pk)
        if len(group) > 1 or any(str(row.get("_ydb_pk") or "") != canonical_pk for row in group):
            counters["publication_urls_normalized"] += 1
            counters["publication_duplicate_rows_removed"] += max(0, len(group) - 1)

    for kind in [
        "processed_post_item", "candidate_memory_item", "image_queue_item",
        "text_vector_enrichment_item", "post_link_queue_item",
    ]:
        for row in rows[kind]:
            pk = str(row.get("_ydb_pk") or "")
            if not pk:
                continue
            payload = {k: v for k, v in row.items() if not str(k).startswith("_ydb_")}
            changed = False
            text_is_consumed = row_has_terminal_text_verdict(kind, row, publication_terminal_urls)
            # processed_post_item is identity/status only. Image rows are also
            # downstream of the text verdict and do not need another durable
            # copy of the post body; active candidate/vector rows retain the
            # exact working text until publication finalization.
            if kind in {"processed_post_item", "image_queue_item"}:
                text_is_consumed = True
            if text_is_consumed:
                for field in TEXT_FIELDS:
                    if field in payload:
                        payload.pop(field, None)
                        changed = True
                if changed:
                    payload["text_payload_pruned_terminal"] = True
                    payload["text_payload_pruned_at"] = now
                    counters[f"{kind}_text_rows_pruned"] += 1
            if kind == "image_queue_item":
                if reopen_placeholder_media_row(payload, now):
                    counters["placeholder_media_decode_failures_reopened"] += 1
                    changed = True
                status = str(payload.get("image_queue_status") or "")
                if status == "not_reviewable_unsupported_media":
                    if is_video_payload(payload):
                        desired = {
                            "media_kind": "video",
                            "media_review_mode": "operator_video_review",
                            "manual_media_review_required": "true",
                            "video_manual_review_eligible": "true",
                            "next_action": "operator_reviews_video_if_text_is_accepted",
                        }
                        if any(payload.get(key) != value for key, value in desired.items()):
                            payload.update(desired)
                            counters["historical_video_rows_labelled"] += 1
                            changed = True
                    else:
                        payload["image_queue_status"] = "broken_media"
                        payload["next_action"] = "terminal_broken_media_no_retry"
                        counters["historical_broken_media_rows_labelled"] += 1
                        changed = True
                if str(payload.get("image_queue_status") or "") in IMAGE_TERMINAL:
                    for field in IMAGE_VERBOSE_TERMINAL_FIELDS:
                        if field in payload:
                            payload.pop(field, None)
                            changed = True
                    payload["image_ledger_compacted_at"] = now
            if changed:
                upserts.append((pk, kind, payload))

    if args.execute and upserts:
        def write(session: Any) -> int:
            rt.ensure_ydb_kv_table(ydb, session, table)
            return rt.ydb_upsert_json_many(session, ydb, table, upserts, now, chunk_size=100, timeout_seconds=15)
        counters["rows_upserted"] = int(pool.retry_operation_sync(write) or 0)
    if args.execute and deletes:
        def remove(session: Any) -> int:
            deleted = 0
            for start in range(0, len(deletes), 100):
                chunk = deletes[start:start + 100]
                literals = ",".join(json.dumps(pk) for pk in chunk)
                session.transaction().execute(f"DELETE FROM `{table}` WHERE pk IN ({literals});", commit_tx=True)
                deleted += len(chunk)
            return deleted
        counters["rows_deleted"] = int(pool.retry_operation_sync(remove) or 0)
    report = {
        "executed": bool(args.execute),
        "table": table,
        "generated_at": now,
        "rows_by_kind": {kind: len(items) for kind, items in rows.items()},
        "publication_terminal_urls": len(publication_terminal_urls),
        "planned_upserts": len(upserts),
        "planned_deletes": len(deletes),
        "counters": dict(sorted(counters.items())),
    }
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    driver.stop(timeout=5)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
