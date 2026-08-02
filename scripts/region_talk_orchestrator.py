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
import asyncio
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (  # noqa: E402
    attach_live_source_fingerprints,
    authoritative_source_fingerprint,
    canonical_source_key_for_row,
    AUTHORITATIVE_SOURCE_FINGERPRINT_VERSION,
    is_confirmed_publication,
    is_publication_draft_ready,
    is_unsent_confirmed_publication,
    ensure_ydb_module,
    load_env,
    read_kind_rows as _stale_read_kind_rows,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_has_direct_credential,
    ydb_table_path,
    ydb_token,
)


ACTIVE_KERNEL_STATUSES = {"RUNNING", "PENDING", "QUEUED", "INITIALIZING"}
UNVERIFIED_KERNEL_STATUS_PREFIXES = ("UNVERIFIED",)
ACTION_KERNEL_SLUGS = {
    "launch_candidate_report": "region-talk-candidate-report",
    "launch_bge_m3": "region-talk-bge-m3-enrichment",
    "launch_image_diagnostic": "region-talk-image-diagnostic",
}
KERNEL_TELEGRAM_RESOURCES = {
    "region-talk-candidate-report": "telegram:DISCOVERY1",
    "region-talk-image-diagnostic": "telegram:DISCOVERY2",
}
NOTIFY_TRANSPORT_RESOURCES = {
    "bot_api": "telegram:bot_api",
    "telethon_discovery1": "telegram:DISCOVERY1",
    "telethon_discovery2": "telegram:DISCOVERY2",
}

CURRENT_E5_ENCODER_CONTRACT = "e5_semantic_bank_scores_v1"
CURRENT_BGE_M3_ENCODER_CONTRACT = "bge_m3_flagembedding_dense_v1"
CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION = "region_talk_publication_eligibility_v5"
CURRENT_PUBLICATION_DRAFT_BACKFILL_VERSION = "region_talk_publication_draft_backfill_v4_publisher_reader_brief"
POST_LINK_READY_STATUSES = {"", "pending_fetch", "retry_fetch", "fetch_error"}
POST_LINK_TERMINAL_STATUSES = {
    "fetched", "scored", "terminal_no_text", "terminal_bad_url",
    "terminal_source_rejected", "operator_rejected",
}

MAIN_DISCOVERY_YDB_BUDGET_ENV = {
    "REGION_TALK_STATE_BACKEND": "ydb",
    "REGION_TALK_REQUIRE_YDB_STATE": "1",
    "REGION_TALK_TEXT_EMBEDDING_MODEL_IDS": "intfloat/multilingual-e5-base",
    "REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS": "0",
    "REGION_TALK_EXTERNAL_BGE_M3_FUSION_ENABLED": "1",
    "REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE": "1",
    "REGION_TALK_ENABLE_POST_WORK_IDEMPOTENCY": "1",
    "REGION_TALK_PUBLICATION_ELIGIBILITY_GATE_VERSION": CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION,
    "REGION_TALK_PUBLICATION_SOURCE_MIN_SCANNED_POSTS": "5",
    # CandidateReport owns discovery/E5/fusion/image handoff only. The local
    # finalizer is the single owner of strict Gemini publication verification.
    "REGION_TALK_ENABLE_EARLY_LLM": "0",
    "REGION_TALK_ENABLE_FINAL_LLM_VERIFIER": "0",
    "REGION_TALK_PUBLICATION_MAX_LLM_VERIFY_PER_RUN": "0",
    # Debug/product runs must finish quickly enough to be observable.  The main
    # notebook still reaches the source-queue/discovery tail, but with smaller
    # batches and bounded YDB writes; otherwise Kaggle spends >30 minutes in
    # tail assembly and YDB row upserts before the next iteration can start.
    # Neither early-tail switch may cut off the publication-candidate handoff.
    # The bounded 20-minute work budget leaves ten minutes for image/source/
    # publication queue assembly and its row-level YDB writes.
    "REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF": "0",
    "REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF": "0",
    "REGION_TALK_STACK_WATCHDOG_REPEAT": "0",
    "REGION_TALK_SOURCE_QUEUE_RECLASSIFY_FULL": "0",
    "REGION_TALK_SOURCE_QUEUE_PROGRESS_EVERY_ROWS": "500",
    "REGION_TALK_BUILD_IMAGE_QUEUE_BEFORE_SOURCE_QUEUE": "1",
    # Full queue state is already durable in YDB. Keep per-run artifacts to the
    # lightweight product/debug shortlist for explicit manual runs. Automated
    # orchestration needs only durable YDB state plus minimal completion JSON;
    # it must not serialize XLSX/CSV/full JSON/Markdown/HTML on every cycle.
    "REGION_TALK_LIGHTWEIGHT_REPORT": "1",
    "REGION_TALK_WRITE_REPORT_ARTIFACTS": "0",
    # Keep under the product requirement of <=30 minutes, but do not starve the
    # high-yield discovery/vector/image handoff stages. A 12-minute debug window
    # caused keyword discovery to be skipped and E5 to be capped to ~49 seconds
    # after source/similar work; 20 minutes gives all methods a chance to run.
    "REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS": "1200",
    "REGION_TALK_TEXT_EMBEDDING_MODEL_TIMEOUT_SECONDS": "420",
    "REGION_TALK_TEXT_EMBEDDING_PRIORITY_MIN_MODEL_TIMEOUT_SECONDS": "420",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_REPORT_SECONDS": "210",
    "REGION_TALK_RUNTIME_LOW_BUDGET_MAX_POSTS_TO_SCORE": "25",
    # Source count is not a work estimate: exact/keyword/history lanes may
    # produce 40+ posts from five sources. Preserve the measured queue/YDB tail
    # and dynamically cap E5 scoring while keeping KO-evidence posts first.
    "REGION_TALK_RUNTIME_FIXED_TAIL_SECONDS": "300",
    "REGION_TALK_RUNTIME_SECONDS_PER_SCORED_POST": "5",
    "REGION_TALK_RUNTIME_MIN_POSTS_TO_SCORE": "8",
    "REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN": "90",
    # checkpoint-v4 contains no embedded row collections; CandidateReport must
    # reconstruct the complete durable post/vector/candidate population.
    "REGION_TALK_YDB_MAX_POST_ROWS": "20000",
    "REGION_TALK_YDB_MAX_SOURCE_ROWS": "20000",
    "REGION_TALK_YDB_SOURCE_QUEUE_FULL_READ_LIMIT": "20000",
    "REGION_TALK_YDB_MAX_CANDIDATE_ROWS": "5000",
    "REGION_TALK_YDB_MAX_TEXT_VECTOR_ROWS": "20000",
    "REGION_TALK_YDB_SELECT_PAGE_SIZE": "300",
    "REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS": "6",
    "REGION_TALK_YDB_HEARTBEAT_MAX_RETRIES": "1",
    "REGION_TALK_YDB_QUEUE_REQUEST_TIMEOUT_SECONDS": "6",
    "REGION_TALK_YDB_QUEUE_MAX_RETRIES": "1",
    "REGION_TALK_YDB_STATE_WRITE_REQUEST_TIMEOUT_SECONDS": "20",
    "REGION_TALK_YDB_STATE_WRITE_MAX_RETRIES": "1",
    "REGION_TALK_YDB_ROW_UPSERT_CHUNK_SIZE": "25",
    "REGION_TALK_YDB_ONLINE_QUEUE_BULK_UPSERT": "1",
    "REGION_TALK_YDB_ONLINE_QUEUE_BULK_CHUNK_SIZE": "100",
    "REGION_TALK_YDB_SNAPSHOT_BULK_CHUNK_SIZE": "500",
    "REGION_TALK_YDB_STATE_LOAD_ATTEMPTS": "4",
    "REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS": "20",
    "REGION_TALK_YDB_STATE_LOAD_REQUEST_TIMEOUT_SECONDS": "12",
    "REGION_TALK_YDB_STATE_LOAD_MAX_RETRIES": "1",
    "REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS": "80",
    "REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY": "1",
    "REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS": "80",
    "REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS": "80",
    "REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL": "0",
    "REGION_TALK_SOURCE_QUEUE_REPAIR_BULK_CHUNK_SIZE": "500",
    # Retention and legacy payload cleanup are maintenance jobs, not part of a
    # latency-sensitive CandidateReport transaction tail.
    "REGION_TALK_YDB_RETENTION_PRUNE": "0",
    "REGION_TALK_YDB_PRUNE_LEGACY_QUEUE_PAYLOADS": "0",
    "REGION_TALK_WRITE_SOURCE_STATUS_QUEUE_MIRROR": "0",
    "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES": "120",
    "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_EDGES": "120",
    "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_COMMENT_LINKS": "40",
    # Keep main runs short enough to reach the discovery tail, but still gentle
    # toward Telegram: a few similar-channel seeds and a few travel-intent
    # keyword queries per run are enough to make the public/source frontier grow
    # without filling the queue with local Kaliningrad-only publics.
    # During funnel calibration, breadth from direct KO evidence is more
    # productive than repeatedly reading deep generic histories. Generic
    # discovery remains enabled; it receives a smaller bounded share.
    "REGION_TALK_HISTORY_SOURCES_TARGET": "4",
    "REGION_TALK_SOURCE_SELECTION_YDB_QUEUE_ONLY": "1",
    "REGION_TALK_MAX_POSTS_PER_SOURCE": "10",
    "REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN": "4",
    "REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE": "10",
    "REGION_TALK_HISTORY_MAX_POST_AGE_DAYS": "365",
    "REGION_TALK_HIGH_VOLUME_TEXT_POSTS_PER_DAY_REJECT_THRESHOLD": "30",
    "REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN": "6",
    "REGION_TALK_TG_SIMILAR_ENABLED": "1",
    "REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN": "3",
    "REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN": "3",
    "REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED": "4",
    "REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN": "15",
    "REGION_TALK_MAX_NEW_FRONTIER_PER_RUN": "15",
    "REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY": "1",
    "REGION_TALK_TELEGRAM_QUERY_SOURCE": "place_lexicon",
    "REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES": "6",
    "REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES": "4",
    "REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN": "2",
    "REGION_TALK_TELEGRAM_QUERY_ROTATE": "1",
    "REGION_TALK_TELEGRAM_KEYWORD_RESULTS_PER_QUERY": "8",
    "REGION_TALK_MAX_KEYWORD_DISCOVERED_SOURCES_PER_RUN": "20",
    # Manual and discovered exact URLs are the first bounded intake lane.  They
    # do not replace source/history/similar/keyword/hashtag discovery below.
    "REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST": "1",
    # Exact links, fast-check hits and confirmed-source posts are already the
    # highest-probability input.  Finish their E5/YDB handoff in the same run;
    # discovery remains enabled and runs in cycles without critical work.
    "REGION_TALK_DEFER_DISCOVERY_ON_CRITICAL_WORK": "1",
    "REGION_TALK_CRITICAL_WORK_DEFER_MIN_POSTS": "8",
    "REGION_TALK_CRITICAL_WORK_CONTINUE_MIN_REMAINING_SECONDS": "600",
    "REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT": "8",
    "REGION_TALK_POST_LINK_QUEUE_SCAN_LIMIT": "5000",
    "REGION_TALK_TG_CACHED_ENTITY_ONLY": "1",
    "REGION_TALK_SOURCE_QUEUE_UNCACHED_RESOLVE_LANE_PER_RUN": "1",
    "REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN": "1",
    "REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN": "1",
    "REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MIN_SECONDS": "8",
    "REGION_TALK_TG_EXACT_POST_FETCH_DELAY_MAX_SECONDS": "18",
    "REGION_TALK_TG_RESOLVE_DELAY_MIN_SECONDS": "20",
    "REGION_TALK_TG_RESOLVE_DELAY_MAX_SECONDS": "45",
    "REGION_TALK_TG_PUBLIC_WEB_FETCH_FIRST": "0",
    "REGION_TALK_TG_PUBLIC_WEB_FALLBACK": "0",
    "REGION_TALK_TG_PUBLIC_WEB_TIMEOUT_SECONDS": "8",
    "REGION_TALK_YDB_DISABLE_ONLINE_WRITES_AFTER_AUTH_ERROR": "1",
    "REGION_TALK_FAST_CHECK_KO_ENABLED": "1",
    "REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN": "10",
    "REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE": "2",
    "REGION_TALK_FAST_CHECK_KO_RESULTS_PER_QUERY": "2",
    "REGION_TALK_FAST_CHECK_QUERY_STRATEGY": "adaptive_cursor_v1",
    "REGION_TALK_FAST_CHECK_ADAPTIVE_PREFER_CONTINUATIONS": "0",
    # Keep breadth dominant, but advance at least two persisted no-hit cursors
    # per run once the higher-priority confirmed-blogger cohort is drained.
    "REGION_TALK_FAST_CHECK_CONTINUATION_SOURCES_PER_RUN": "2",
    "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_ENABLED": "1",
    # The curated confirmed-external registry is the highest-probability
    # acquisition input.  Its first scans own the bounded history lane even
    # when generic known-KO delta rescans are disabled.
    "REGION_TALK_CONFIRMED_BLOGGER_PRIORITY_ENABLED": "1",
    "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_TABLE": "region_talk_external_blogger_evidence",
    "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_MAX_ROWS": "2000",
    # The durable adaptive cursor preserves the full low-frequency place bank
    # across runs.  Keep each confirmed-blogger wave bounded so a no-hit source
    # cannot spend most of a 20-minute CandidateReport run sleeping between
    # human-like Telegram searches.
    "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_QUERIES_PER_SOURCE": "8",
    "REGION_TALK_FAST_CHECK_STAGE_MAX_SECONDS": "180",
    "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_RESULTS_PER_QUERY": "20",
    "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_COLLECT_MULTIPLE_POSTS": "1",
    "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_POSTS_PER_QUERY": "2",
    "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_MAX_POSTS_PER_SOURCE": "12",
    "REGION_TALK_CONFIRMED_BLOGGER_HISTORY_SLOTS_PER_RUN": "4",
    "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_ENABLED": "1",
    "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_QUERIES_PER_SOURCE": "8",
    "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_RESULTS_PER_QUERY": "20",
    "REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MIN_SECONDS": "5",
    "REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MAX_SECONDS": "9",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_FAST_CHECK_KO_SECONDS": "330",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS": "300",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_KEYWORD_QUERY_SECONDS": "240",
    # Known-KO/direct-evidence sources may use a small additional scan slot;
    # generic similar recommendations never receive it merely for being
    # similar. Telegram governor/cooldown limits remain unchanged.
    "REGION_TALK_PUBLICATION_GOAL_RESCAN_KO_SOURCES": "1",
    "REGION_TALK_SIMILAR_DIRECT_EVIDENCE_EXTRA_SOURCES_PER_RUN": "1",
}

ORCHESTRATOR_YDB_METRIC_LIMITS = {
    "candidate_memory_item": 2500,
    "image_queue_item": 2500,
    "publication_candidate_item": 2500,
    "post_link_queue_item": 2500,
    "text_vector_enrichment_item": 20000,
    "processed_post_item": 20000,
    "post_live_item": 20000,
    "source_queue_item": 20000,
    "source_status_item": 20000,
    "online_source_item": 20000,
    "source_candidate_item": 20000,
    "source_edge_item": 20000,
    "comment_link_item": 4000,
    "telegram_entity_cache_item": 20000,
    "source_onboarding_evidence_item": 2500,
    "source_onboarding_profile_item": 2500,
    "external_publication_source_item": 2500,
    "external_publication_intake_item": 2500,
}

# The vector payload also contains the dense embedding. Loading thousands of
# those arrays merely to count E5/BGE pairs can exceed the local orchestrator's
# memory even though the useful metric fields are tiny. Keep this list scalar-
# only and let YDB project it with JSON_VALUE.
TEXT_VECTOR_METRIC_FIELDS = (
    "model_short", "model_id", "encoder_contract", "post_url", "post_id",
    "text_vector_enrichment_id", "paired_e5_text_hash", "text_hash",
    "semantic_bank_version", "created_at", "updated_at",
    "source_terminal_excluded", "source_queue_status", "fetch_status",
    "source_scope", "source_geo_class", "source_quick_class", "source_topic_class",
    "discovery_method", "priority_reason", "post_link_priority",
    "keyword_hit_query", "keyword_hit_hashtag",
    "text", "full_text", "text_excerpt", "short_summary", "why_keep_in_memory",
    "why_this_is_about_kaliningrad", "what_positive", "what_neutral_or_useful",
    "llm_reason", "publication_story_reason", "model_short_explanation",
)


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _orchestrator_kind_limit(kind: str, requested_limit: int) -> int:
    default_cap = ORCHESTRATOR_YDB_METRIC_LIMITS.get(kind, 2000)
    key = "REGION_TALK_ORCHESTRATOR_YDB_MAX_" + re.sub(r"[^A-Za-z0-9]+", "_", kind).upper() + "_ROWS"
    cap = _env_int(key, _env_int("REGION_TALK_ORCHESTRATOR_YDB_MAX_ROWS_PER_KIND", default_cap))
    if kind in {
        "processed_post_item", "post_live_item", "source_queue_item",
        "source_status_item", "online_source_item", "source_candidate_item",
        "source_edge_item", "telegram_entity_cache_item", "text_vector_enrichment_item",
    }:
        # Processed/live post totals are goal metrics. They must not be silently
        # flattened by a low source/frontier debug --limit, otherwise the loop
        # can report zero processed-post progress while CandidateReport is
        # actually fetching/writing posts.
        return max(1, cap)
    return max(1, min(max(1, int(requested_limit)), cap))


def read_kind_rows(pool: Any, ydb: Any, table: str, kind: str, limit: int) -> list[dict[str, Any]]:
    """Strongly read current metric rows; callers use limit+1 for completeness."""

    if not re.fullmatch(r"[A-Za-z0-9_:-]+", kind):
        raise ValueError(f"unsafe YDB kind: {kind!r}")
    max_items = max(1, int(limit))
    page_size = max(1, min(500, _env_int("REGION_TALK_YDB_SELECT_PAGE_SIZE", 200), max_items))
    prefix = kind + ":"
    prefix_upper = kind + ";"

    def op(session: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        local_after = prefix
        tx = session.transaction(ydb.SnapshotReadOnly())
        try:
            while len(out) < max_items:
                query_text = (
                    "DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; DECLARE $after AS Utf8; "
                    f"SELECT pk, payload_json, updated_at FROM `{table}` "
                    "WHERE pk >= $prefix AND pk < $prefix_upper AND pk > $after "
                    f"ORDER BY pk LIMIT {min(page_size, max_items - len(out))};"
                )
                query = session.prepare(query_text)
                result_sets = tx.execute(
                    query,
                    {"$prefix": prefix, "$prefix_upper": prefix_upper, "$after": local_after},
                    commit_tx=False,
                )
                rows = result_sets[0].rows if result_sets else []
                if not rows:
                    break
                for raw in rows:
                    local_after = str(raw.pk)
                    payload = raw.payload_json
                    item = json.loads(payload) if isinstance(payload, str) else dict(payload or {})
                    if isinstance(item, dict):
                        item.setdefault("_ydb_pk", local_after)
                        item.setdefault("_ydb_updated_at", str(getattr(raw, "updated_at", "") or ""))
                        out.append(item)
                if len(rows) < page_size:
                    break
            tx.commit()
            return out
        except Exception:
            try:
                tx.rollback()
            except Exception:
                pass
            raise

    return list(pool.retry_operation_sync(op) or [])


def read_text_vector_metric_rows(pool: Any, ydb: Any, table: str, limit: int) -> list[dict[str, Any]]:
    """Read vector metadata without materializing dense embedding arrays."""
    max_items = max(1, int(limit))
    page_size = max(1, min(500, _env_int("REGION_TALK_YDB_SELECT_PAGE_SIZE", 200), max_items))
    prefix = "text_vector_enrichment_item:"
    prefix_upper = "text_vector_enrichment_item;"
    after = prefix
    projections = ", ".join(
        f'JSON_VALUE(payload_json, "$.{field}") AS `{field}`'
        for field in TEXT_VECTOR_METRIC_FIELDS
    )
    out: list[dict[str, Any]] = []
    while len(out) < max_items:
        query_text = (
            "DECLARE $prefix AS Utf8; DECLARE $prefix_upper AS Utf8; DECLARE $after AS Utf8; "
            f"SELECT pk, updated_at AS `_row_updated_at`, {projections} FROM `{table}` "
            "WHERE pk >= $prefix AND pk < $prefix_upper AND pk > $after "
            f"ORDER BY pk LIMIT {min(page_size, max_items - len(out))};"
        )

        def op(session: Any) -> Any:
            query = session.prepare(query_text)
            return session.transaction(ydb.SnapshotReadOnly()).execute(
                query,
                {"$prefix": prefix, "$prefix_upper": prefix_upper, "$after": after},
                commit_tx=True,
            )

        rows = pool.retry_operation_sync(op)[0].rows
        if not rows:
            break
        for row in rows:
            after = str(row.pk)
            item = {
                field: getattr(row, field, None)
                for field in TEXT_VECTOR_METRIC_FIELDS
                if getattr(row, field, None) not in (None, "")
            }
            item["_ydb_pk"] = after
            item["_ydb_updated_at"] = str(getattr(row, "_row_updated_at", "") or "")
            out.append(item)
        if len(rows) < page_size:
            break
    return out


def ensure_decision_metric_reads_complete(truncated_kinds: list[str]) -> None:
    """Fail only when asynchronous intake completeness cannot be proven."""

    if "external_publication_intake_item" in truncated_kinds:
        raise RuntimeError(
            "decision-critical YDB intake read truncated: external_publication_intake_item"
        )


def build_orchestrator_stats_message(metrics: dict[str, Any]) -> str:
    """Render the full product funnel in plain operator language.

    Metric keys remain stable for automation, but this message deliberately
    avoids implementation shorthand such as ``strict gate``, ``fetched`` or
    an unlabeled historical ``image rows`` total.
    """
    value = lambda key: _safe_int(metrics.get(key))
    decimal_value = lambda key: (
        f"{float(_safe_float(metrics.get(key)) or 0):.2f}".rstrip("0").rstrip(".")
    )
    source_total = value("publics_total")
    # Ever-scanned is evidence-owned (source counters or canonical processed
    # post source keys), not `total - pending`. A source temporarily selected
    # for a rescan can re-enter a pending status and must not make historical
    # scan coverage move backwards.
    source_ever_scanned = min(source_total, value("publics_scanned_with_posts_total"))
    source_never_scanned = max(0, source_total - source_ever_scanned)
    source_scanned_percent = int(round((source_ever_scanned / source_total) * 100)) if source_total else 0
    latest_outcomes = metrics.get("heuristic_ko_latest_run_outcome_counts") or {}
    if not isinstance(latest_outcomes, dict):
        latest_outcomes = {}
    top_latest_outcomes = ", ".join(
        f"{reason}={count}"
        for reason, count in sorted(latest_outcomes.items(), key=lambda item: (-_safe_int(item[1]), str(item[0])))[:8]
    ) or "none"
    return "\n".join([
        "📊 Region Talk — полная продуктовая воронка из live YDB",
        (
            "Источники — всего / хотя бы раз реально просмотрены / ещё ни разу не просмотрены: "
            f"{source_total}/{source_ever_scanned}/{source_never_scanned} "
            f"(просмотрено {source_scanned_percent}%)"
        ),
        (
            "Итог источников — завершены / нужен повторный просмотр / локальные / спам: "
            f"{value('publics_terminal_processed_total')}/"
            f"{value('publics_needs_rescan_or_retry_total')}/"
            f"{value('publics_rejected_local_region_source_total')}/"
            f"{value('publics_rejected_spam_source_total')}"
        ),
        (
            "Технический backlog первичного прохода — pending без source-history evidence / pending с source-history evidence: "
            f"{value('publics_primary_unscanned_pending_total')}/"
            f"{value('publics_pending_with_scan_evidence_waiting_rescan_total')}"
        ),
        (
            "Из primary pending уже имеют только точечный post-level probe (это не просмотр истории источника): "
            f"{value('publics_pending_post_probe_only_total')}"
        ),
        f"Источники, где уже найден хотя бы один возможный пост о КО: {value('publics_with_ko_candidates_total')}",
        (
            "Реестр людей/проектов — всего записей / confirmed / needs review / "
            "confirmed, но местные / eligible confirmed non-local: "
            f"{value('external_blogger_registry_records_total')}/"
            f"{value('external_blogger_registry_confirmed_records_total')}/"
            f"{value('external_blogger_registry_needs_review_records_total')}/"
            f"{value('external_blogger_registry_confirmed_local_excluded_records_total')}/"
            f"{value('external_blogger_registry_eligible_records_total')}"
        ),
        (
            "Eligible-записи реестра — есть поддерживаемый Telegram/VK источник / "
            "нет поддерживаемой TG/VK-ссылки / заведены в единую очередь / "
            "хотя бы один источник просмотрен / найден возможный KO-пост: "
            f"{value('external_blogger_registry_records_with_supported_tg_vk_source_total')}/"
            f"{value('external_blogger_registry_records_without_supported_tg_vk_source_total')}/"
            f"{value('external_blogger_registry_supported_records_in_queue_total')}/"
            f"{value('external_blogger_registry_supported_records_with_scanned_source_total')}/"
            f"{value('external_blogger_registry_supported_records_with_ko_source_total')}"
        ),
        (
            "Физически всё ещё помечены ingestion-полем pipeline_status=stored_only "
            "(это не операционный статус обработки): "
            f"{value('external_blogger_registry_pipeline_stored_only_records_total')}"
        ),
        (
            "Уникальные поддерживаемые TG+VK источники реестра — всего / в очереди / "
            "не заведены / просмотрены / с возможным KO-постом: "
            f"{value('external_blogger_registry_canonical_tg_vk_sources_total')}/"
            f"{value('external_blogger_registry_canonical_sources_in_queue_total')}/"
            f"{value('external_blogger_registry_canonical_sources_missing_from_queue_total')}/"
            f"{value('external_blogger_registry_canonical_sources_scanned_total')}/"
            f"{value('external_blogger_registry_canonical_sources_with_ko_total')}"
        ),
        (
            "Подтверждённые внешние блогеры — источников в единой очереди / просмотрено / найден KO / "
            "fast-check дал точную ссылку / VK-поиск проверен / VK-поиск дал пост / ещё активно ждут просмотра / ошибочно локальный / спам / недоступный TG / недоступный VK: "
            f"{value('confirmed_external_blogger_sources_total')}/"
            f"{value('confirmed_external_blogger_scanned_total')}/"
            f"{value('confirmed_external_blogger_with_ko_total')}/"
            f"{value('confirmed_external_blogger_fast_check_hit_total')}/"
            f"{value('confirmed_external_blogger_vk_search_checked_total')}/"
            f"{value('confirmed_external_blogger_vk_search_hit_total')}/"
            f"{value('confirmed_external_blogger_pending_total')}/"
            f"{value('confirmed_external_blogger_rejected_local_total')}/"
            f"{value('confirmed_external_blogger_rejected_spam_total')}/"
            f"{value('confirmed_external_blogger_rejected_unresolvable_telegram_total')}/"
            f"{value('confirmed_external_blogger_rejected_unresolvable_vk_total')}"
        ),
        (
            "Посты подтверждённых внешних блогеров — обработано / текстовый dual-вектор пропустил / "
            "передано на медиа / дошло до публикационного реестра / подтверждено Gemini / отправлено в чат: "
            f"{value('confirmed_external_blogger_posts_processed_total')}/"
            f"{value('confirmed_external_blogger_vector_accepted_posts_total')}/"
            f"{value('confirmed_external_blogger_image_queue_posts_total')}/"
            f"{value('confirmed_external_blogger_publication_posts_total')}/"
            f"{value('confirmed_external_blogger_publication_confirmed_posts_total')}/"
            f"{value('confirmed_external_blogger_delivery_completed_posts_total')}"
        ),
        (
            "Конверсия подтверждённых блогеров по уникальным источникам — есть прочитанные посты / "
            "есть dual-текстовый кандидат / передано медиа / публикационный реестр / Gemini / доставлено: "
            f"{value('confirmed_external_blogger_sources_with_processed_posts_total')}/"
            f"{value('confirmed_external_blogger_sources_with_vector_accepted_posts_total')}/"
            f"{value('confirmed_external_blogger_sources_with_image_queue_posts_total')}/"
            f"{value('confirmed_external_blogger_sources_with_publication_posts_total')}/"
            f"{value('confirmed_external_blogger_sources_with_publication_confirmed_posts_total')}/"
            f"{value('confirmed_external_blogger_sources_with_delivery_completed_posts_total')}"
        ),
        (
            "Поиск по словам/хештегам — найдено источников / реально просмотрено / "
            "есть предварительный KO-пост / KO подтверждён / KO подтверждён у внешнего источника: "
            f"{value('publics_keyword_discovered_total')}/"
            f"{value('publics_keyword_scanned_with_posts_total')}/"
            f"{value('keyword_sources_with_preliminary_candidates_total')}/"
            f"{value('keyword_sources_with_confirmed_ko_posts_total')}/"
            f"{value('keyword_external_sources_with_confirmed_ko_posts_total')}"
        ),
        (
            "Прямой KO-маршрут — источников с найденной ссылкой / тексты постов прочитаны / "
            "посчитаны E5+BGE / обе модели считают текст постом о КО / "
            "текст полностью подходит до проверки медиа / передано фото / передано видео / дошло до Gemini: "
            f"{value('fast_check_keyword_match_sources_total')}/"
            f"{value('fast_check_exact_posts_processed_unique_total')}/"
            f"{value('fast_check_exact_posts_dual_vectorized_total')}/"
            f"{value('fast_check_exact_posts_dual_semantic_accept_total')}/"
            f"{value('fast_check_exact_posts_strict_text_accepted_total')}/"
            f"{value('fast_check_exact_posts_image_queue_total')}/"
            f"{value('fast_check_exact_posts_video_manual_review_total')}/"
            f"{value('fast_check_exact_posts_publication_queue_total')}"
        ),
        (
            "Решение по прочитанным прямым KO-постам — текст подходит / отклонён / ещё обрабатывается: "
            f"{value('fast_check_exact_posts_strict_text_accepted_total')}/"
            f"{value('fast_check_exact_posts_text_rejected_total')}/"
            f"{value('fast_check_exact_posts_text_pending_total')}"
        ),
        (
            "Как источники когда-либо попали в базу — импорт/ручной ввод / ключевые слова / хештеги / похожие каналы "
            "(исторический состав, не прирост запуска): "
            f"{value('discovery_inflow_manual_total')}/"
            f"{value('discovery_inflow_keyword_total')}/"
            f"{value('discovery_inflow_hashtag_total')}/"
            f"{value('discovery_inflow_similar_total')}"
        ),
        (
            "Целостность очереди — без порядкового номера / дубли порядка: "
            f"{value('source_queue_integrity_unordered_total')}/"
            f"{value('source_queue_integrity_duplicate_order_rows_total')}"
        ),
        (
            "Очередь конкретных ссылок — новые тексты к чтению / готовы к повторному решению после BGE / "
            "надо только закрыть из уже отклонённых источников (вся очередь / BGE-rescore) / "
            "ждут Telegram cooldown / ждут Telegram entity / terminal / неизвестный статус / "
            "исторически тексты уже читались: "
            f"{value('post_link_queue_exact_ready_total')}/"
            f"{value('post_link_queue_bge_ready_rescore_total')}/"
            f"{value('post_link_queue_source_terminal_cleanup_total')}/"
            f"{value('post_link_queue_bge_ready_rescore_source_terminal_cleanup_total')}/"
            f"{value('post_link_queue_cooldown_total')}/"
            f"{value('post_link_queue_entity_wait_total')}/"
            f"{value('post_link_queue_terminal_total')}/"
            f"{value('post_link_queue_unknown_status_total')}/"
            f"{value('post_link_queue_fetched_total')}"
        ),
        (
            "Посты за всё время — уникально обработано / строк состояния / дублей идентичности: "
            f"{value('processed_posts_unique_total')}/"
            f"{value('processed_post_rows_total')}/"
            f"{value('processed_post_duplicate_identity_rows_total')}"
        ),
        (
            "Конверсия обработанных постов в выявленный KO scope до content/media/Gemini-фильтров — "
            "KO / обработано / доля / на 1000: "
            f"{value('ko_scope_detected_posts_unique_total')}/"
            f"{value('processed_posts_unique_total')}/"
            f"{decimal_value('processed_to_ko_scope_conversion_percent')}%/"
            f"{decimal_value('processed_to_ko_scope_detected_per_1000')}"
        ),
        (
            "Покрытие текущим KO scope-контрактом — оценено / обработано / покрытие; "
            "KO среди оценённых: "
            f"{value('ko_scope_evaluated_posts_unique_total')}/"
            f"{value('processed_posts_unique_total')}/"
            f"{decimal_value('ko_scope_evaluation_coverage_percent')}%; "
            f"{decimal_value('evaluated_to_ko_scope_conversion_percent')}%"
        ),
        (
            "Последний основной ноутбук — впервые добавлено новых постов / повторно обновлено известных / "
            "всего затронуто уникальных / дублей идентичности: "
            f"{value('processed_posts_new_latest_candidate_run_total')}/"
            f"{value('processed_posts_reprocessed_latest_candidate_run_total')}/"
            f"{value('processed_posts_unique_latest_candidate_run_total')}/"
            f"{value('processed_post_duplicate_identity_rows_latest_candidate_run_total')}"
        ),
        (
            "Исполнение последнего основного ноутбука — успешно прочитана история источников / "
            "постов получено / постов пропущено через E5 / runtime, секунд: "
            f"{value('candidate_heartbeat_sources_history_fetched_ok')}/"
            f"{value('candidate_heartbeat_posts_fetched')}/"
            f"{value('candidate_heartbeat_posts_scored')}/"
            f"{value('candidate_heartbeat_runtime_elapsed_seconds')}"
        ),
        (
            "Полезная работа с постами последнего запуска — actionable / новый E5 / fusion готового BGE / "
            "policy refresh / ждут BGE без повтора E5 / пропущены как неизменные / legacy dual уже готов: "
            f"{value('candidate_run_posts_actionable_after_idempotency')}/"
            f"{value('candidate_run_posts_needing_new_e5')}/"
            f"{value('candidate_run_posts_reusing_e5_for_bge_fusion')}/"
            f"{value('candidate_run_posts_reusing_e5_for_policy_refresh')}/"
            f"{value('candidate_run_posts_waiting_for_bge_without_e5_recompute')}/"
            f"{value('candidate_run_posts_skipped_unchanged_current')}/"
            f"{value('candidate_run_posts_skipped_legacy_current_dual')}"
        ),
        (
            "Последний основной ноутбук, KO-воронка — эвристически KO / проверены векторами / "
            "текст подходит / передано медиа / дошло до публикационного отбора / отправлено в чат: "
            f"{value('heuristic_ko_latest_run_raw_posts_total')}/"
            f"{value('heuristic_ko_latest_run_vector_evaluated_total')}/"
            f"{value('heuristic_ko_latest_run_text_accepted_total')}/"
            f"{value('heuristic_ko_latest_run_image_queue_total')}/"
            f"{value('heuristic_ko_latest_run_publication_total')}/"
            f"{value('heuristic_ko_latest_run_sent_total')}"
        ),
        (
            "Конверсия последнего запуска — KO среди обработанных / подходящий текст среди KO / публикация среди KO: "
            f"{value('latest_candidate_heuristic_ko_hit_rate_percent')}%/"
            f"{value('latest_candidate_heuristic_to_text_accept_rate_percent')}%/"
            f"{value('latest_candidate_heuristic_to_publication_rate_percent')}%"
        ),
        (
            "Последний запуск — быстро проверено источников / с KO / доля: "
            f"{value('fast_check_latest_run_sources_total')}/"
            f"{value('fast_check_latest_run_hit_sources_total')}/"
            f"{value('fast_check_latest_run_hit_rate_percent')}%"
        ),
        (
            "Fast-check последнего запуска — запросов / время запросов, сек / всё время стадии, сек / "
            "лимит стадии, сек / лимит исчерпан: "
            f"{value('fast_check_latest_run_queries_total')}/"
            f"{decimal_value('fast_check_latest_run_query_elapsed_seconds')}/"
            f"{decimal_value('fast_check_latest_run_stage_elapsed_seconds')}/"
            f"{decimal_value('fast_check_latest_run_stage_max_seconds')}/"
            f"{value('fast_check_latest_run_stage_budget_exhausted')}"
        ),
        (
            "Технические source-overlay обновления последнего run_id — строк с scan evidence / с KO / доля "
            "(это НЕ число глубоко прочитанных источников): "
            f"{value('source_latest_scan_run_sources_total')}/"
            f"{value('source_latest_scan_run_ko_sources_total')}/"
            f"{value('source_latest_scan_run_ko_source_yield_percent')}%"
        ),
        f"Причины результата эвристических KO-постов последнего запуска: {top_latest_outcomes}",
        (
            "Реестр текстовых кандидатов — исторических строк / активных / локальных для аудита / "
            "спам для аудита / ждут второй вектор / ждут медиа: "
            f"{value('candidate_memory_total')}/"
            f"{value('candidate_memory_operational_total')}/"
            f"{value('candidate_memory_terminal_local_audit_total')}/"
            f"{value('candidate_memory_terminal_spam_audit_total')}/"
            f"{value('candidate_memory_dual_pending_total')}/"
            f"{value('candidate_memory_image_wait_total')}"
        ),
        (
            "Покрытие двумя моделями E5+BGE — всех строк / реально обрабатываемых: "
            f"{value('text_vector_current_version_dual_coverage_percent')}%/"
            f"{value('text_vector_current_version_dual_actionable_coverage_percent')}% "
            f"(без BGE всего {value('text_vector_current_version_e5_without_bge_total')}; "
            f"из них надо обработать {value('text_vector_current_version_e5_without_bge_actionable_total')}; "
            f"слишком коротких {value('text_vector_current_version_e5_below_bge_min_text_total')}; "
            f"из уже отклонённых источников {value('text_vector_current_version_e5_without_bge_source_terminal_total')})"
        ),
        (
            "BGE — срочно не хватает пар / вместимость следующего CPU-запуска / загрузка; "
            "отдельно фоновый stale-bank backlog / выбрано в диагностическую выборку / "
            "пропущено из отклонённых источников: "
            f"{value('bge_immediate_pair_backlog_total')}/"
            f"{value('bge_capacity_rows')}/"
            f"{value('bge_immediate_pair_backlog_capacity_percent')}%; "
            f"{value('bge_stale_maintenance_backlog_total')}/"
            f"{value('bge_stale_maintenance_selected_sample_total')}/"
            f"{value('bge_source_terminal_skipped_sample_total')}"
        ),
        (
            "Медиа — исторических постов / ждут оценки / отложено до полного text/source gate / постов реально оценено / отдельных кадров оценено / "
            "legacy auto-accept / visual-review raw/active/tombstoned / partial albums raw/active / "
            "повторная оценка / визуальная проверка Gemini: очередь/завершено/принято/отклонено/нужен ручной просмотр/ошибка или лимит / видео вручную: "
            f"{value('image_ledger_rows_total')}/"
            f"{value('image_pending_total')}/"
            f"{value('image_deferred_text_gate_total')}/"
            f"{value('image_actual_scored_total')}/"
            f"{value('image_actual_frames_scored_total')}/"
            f"{value('image_legacy_auto_accept_total')}/"
            f"{value('image_visual_review_pending_total')}/"
            f"{value('image_visual_review_active_total')}/"
            f"{value('image_visual_review_tombstoned_total')}/"
            f"{value('image_partial_album_acquisition_total')}/"
            f"{value('image_partial_album_active_total')}/"
            f"{value('image_scoring_retry_total')}/"
            f"{value('image_vlm_backlog_total')}/"
            f"{value('image_vlm_completed_total')}/"
            f"{value('image_vlm_accept_total')}/"
            f"{value('image_vlm_reject_nonterminal_total')}/"
            f"{value('image_vlm_review_total')}/"
            f"{value('image_vlm_error_or_budget_deferred_total')}/"
            f"{value('video_manual_review_candidate_urls_total')}"
        ),
        (
            "Статьи с JS-медиа — ждут браузера / готовы сейчас / заняты lease / ждут retry / "
            "исчерпали попытки / изображения извлечены / terminal fallback: "
            f"{value('image_browser_materialization_waiting_total')}/"
            f"{value('image_browser_materialization_due_total')}/"
            f"{value('image_browser_materialization_leased_total')}/"
            f"{value('image_browser_materialization_retry_wait_total')}/"
            f"{value('image_browser_materialization_attempts_exhausted_total')}/"
            f"{value('image_browser_materialized_total')}/"
            f"{value('image_browser_materialization_terminal_total')}"
        ),
        (
            "Публикационный отбор — исторических строк / сейчас подтверждены Gemini / "
            "когда-либо отмечены отправленными / подтверждены, но не отправлены / фактически доставлено сообщений: "
            f"{value('publication_candidate_total')}/"
            f"{value('publication_confirmed_total')}/"
            f"{value('publication_sent_total')}/"
            f"{value('publication_ready_total')}/"
            f"{value('publication_delivery_completed_total')}"
        ),
        (
            "Целостность публикационного lifecycle — противоречивых активных строк: "
            f"{value('publication_lifecycle_contradiction_total')}"
        ),
        (
            "Финализатор — постов с оценённым фото / видео / non-terminal visual review / "
            "text-restore raw/active/tombstoned / ещё требуют решения или обновления: "
            f"{value('image_actual_scored_urls_total')}/"
            f"{value('video_manual_review_candidate_urls_total')}/"
            f"{value('publication_visual_review_pending_total')}/"
            f"{value('publication_text_restore_pending_raw_total')}/"
            f"{value('publication_text_restore_pending_total')}/"
            f"{value('publication_text_restore_tombstoned_total')}/"
            f"{value('finalizer_pending_url_total')}"
        ),
        (
            "Онбординг источника — evidence packs / профилей / профилей готовы / "
            "кандидатов с готовым абзацем / требуют проверки / приняты и не отправлены, ждут абзац: "
            f"{value('source_onboarding_evidence_total')}/"
            f"{value('source_onboarding_profile_total')}/"
            f"{value('source_onboarding_profile_ready_total')}/"
            f"{value('publication_onboarding_ready_total')}/"
            f"{value('publication_onboarding_needs_review_total')}/"
            f"{value('publication_onboarding_pending_unsent_total')}"
        ),
    ])


def ensure_kaggle_username_env() -> str:
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if username:
        return username
    config_path = Path(os.getenv("KAGGLE_CONFIG_DIR") or (Path.home() / ".kaggle")) / "kaggle.json"
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
        username = str(data.get("username") or "").strip()
    except Exception:
        username = ""
    if username:
        os.environ["KAGGLE_USERNAME"] = username
    return username


def ensure_child_ydb_env(*, allow_yc_fallback: bool) -> None:
    endpoint, database = ydb_endpoint_database(allow_yc_fallback=allow_yc_fallback)
    os.environ.setdefault("REGION_TALK_YDB_ENDPOINT", endpoint)
    os.environ.setdefault("REGION_TALK_YDB_DATABASE", database)
    if not ydb_has_direct_credential() and allow_yc_fallback:
        os.environ.setdefault("YC_IAM_TOKEN", ydb_token(allow_yc_fallback=True))


def _normalize_kaggle_status_payload(status: Any) -> str:
    if isinstance(status, dict):
        raw = status.get("status") or status.get("state") or ""
    else:
        raw = getattr(status, "status", None) or status
    if hasattr(raw, "name"):
        value = str(raw.name)
    else:
        value = str(raw or "")
    return value.upper().replace("KERNELWORKERSTATUS.", "")


def _make_kaggle_status_reader() -> Any:
    try:
        from video_announce.kaggle_client import KaggleClient  # type: ignore
        client = KaggleClient()
        return lambda ref: client.get_kernel_status(ref)
    except Exception:
        from kaggle.api.kaggle_api_extended import KaggleApi  # type: ignore
        api = KaggleApi()
        api.authenticate()
        return lambda ref: api.kernels_status(ref)


def read_kaggle_kernel_statuses(username: str) -> dict[str, str]:
    """Read Region Talk kernel statuses through the shared Kaggle client path.

    In slim local virtualenvs `video_announce.kaggle_client` may be unavailable
    because app DB dependencies are not installed. Status polling must still be
    reliable, so fall back to the official Kaggle API only for this read-only
    status check. Notebook launchers still use the established launcher code.
    """
    if not username:
        return {}
    read_status = _make_kaggle_status_reader()
    out: dict[str, str] = {}
    for slug in ACTION_KERNEL_SLUGS.values():
        ref = f"{username}/{slug}"
        try:
            out[slug] = _normalize_kaggle_status_payload(read_status(ref))
        except Exception as exc:
            out[slug] = f"UNVERIFIED:{type(exc).__name__}"
    return out


def filter_actions_for_active_kernels(
    actions: list[dict[str, Any]],
    kaggle_statuses: dict[str, str],
    *,
    block_unverified: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not kaggle_statuses:
        return actions, []
    kept: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    active_resources = {
        resource
        for kernel, resource in KERNEL_TELEGRAM_RESOURCES.items()
        if str(kaggle_statuses.get(kernel) or "").upper() in ACTIVE_KERNEL_STATUSES
    }
    unverified_resources = {
        resource
        for kernel, resource in KERNEL_TELEGRAM_RESOURCES.items()
        if str(kaggle_statuses.get(kernel) or "").upper().startswith(UNVERIFIED_KERNEL_STATUS_PREFIXES)
    }
    for action in actions:
        slug = ACTION_KERNEL_SLUGS.get(str(action.get("action") or ""))
        status = str(kaggle_statuses.get(slug or "") or "").upper()
        if slug and status in ACTIVE_KERNEL_STATUSES:
            skipped.append({"action": action.get("action"), "kernel_slug": slug, "status": status, "reason": "kernel_already_active"})
            continue
        if slug and block_unverified and status.startswith(UNVERIFIED_KERNEL_STATUS_PREFIXES):
            skipped.append({"action": action.get("action"), "kernel_slug": slug, "status": status, "reason": "kernel_status_unverified"})
            continue
        resource = str(action.get("resource") or "")
        if resource and resource in active_resources:
            skipped.append({
                "action": action.get("action"),
                "resource": resource,
                "reason": "telegram_auth_bundle_in_use_by_active_kernel",
            })
            continue
        if resource and block_unverified and resource in unverified_resources:
            skipped.append({
                "action": action.get("action"),
                "resource": resource,
                "reason": "telegram_auth_bundle_kernel_status_unverified",
            })
            continue
        kept.append(action)
    return kept, skipped

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


def _source_merge_key(row: dict[str, Any]) -> str:
    raw = (
        row.get("source_id")
        or row.get("canonical_source_key")
        or row.get("source_url")
        or row.get("canonical_url")
        or row.get("username")
        or row.get("_ydb_pk")
        or ""
    )
    key = str(raw or "").strip()
    if key.startswith(("source_queue_item:", "source_status_item:", "online_source_item:")):
        key = key.split(":", 1)[1]
    return key


def _post_source_merge_key(row: dict[str, Any]) -> str:
    raw = (
        row.get("canonical_source_key")
        or row.get("source_url")
        or row.get("canonical_url")
        or row.get("source_id")
        or ""
    )
    key = str(raw or "").strip().rstrip("/")
    if key.startswith(("source_queue_item:", "source_status_item:", "online_source_item:")):
        key = key.split(":", 1)[1]
    return key


def _telegram_handle_from_url(value: str) -> str:
    raw = str(value or "").strip().rstrip("/")
    match = re.search(r"(?:https?://)?(?:t\.me|telegram\.me)/(@?[A-Za-z0-9_]{4,})", raw, re.I)
    if not match:
        return ""
    handle = match.group(1).lstrip("@").lower()
    if handle in {"s", "c", "joinchat", "+"} or handle.startswith("+"):
        return ""
    return handle


def _source_alias_keys(row: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    platform = str(row.get("platform") or row.get("platform_guess") or "").strip().lower()
    for field in [
        "canonical_source_key", "source_id", "source_url", "canonical_url",
        "url", "keyword_hit_source_url", "recommended_canonical_url",
    ]:
        value = str(row.get(field) or "").strip().rstrip("/")
        if not value:
            continue
        aliases.add(value)
        if value.startswith(("source_queue_item:", "source_status_item:", "online_source_item:")):
            aliases.add(value.split(":", 1)[1])
        handle = _telegram_handle_from_url(value)
        if handle:
            aliases.add(f"telegram:{handle}")
            aliases.add(f"https://t.me/{handle}")
    for field in ["handle", "username", "username_or_handle", "recommended_username"]:
        handle = str(row.get(field) or "").strip().lstrip("@").lower()
        # A VK screen name is not a Telegram username.  The previous generic
        # alias expansion mapped vk.com/figarotravel to telegram:figarotravel,
        # so one delivered Telegram post was reported as two unique sources.
        if handle and platform not in {"vk", "vkvideo"}:
            aliases.add(f"telegram:{handle}")
            aliases.add(f"https://t.me/{handle}")
    post_url_handle = _telegram_handle_from_url(str(row.get("post_url") or ""))
    if post_url_handle:
        aliases.add(f"telegram:{post_url_handle}")
        aliases.add(f"https://t.me/{post_url_handle}")
    return {a.lower().rstrip("/") for a in aliases if a}


def _evidence_relation_is_local(value: Any) -> bool:
    relation = re.sub(r"\s+", " ", str(value or "").strip().lower().replace("ё", "е"))
    return any(marker in relation for marker in (
        "lives in region", "living in region", "resident of region", "local resident", "local blogger",
        "живет в регионе", "житель региона", "местный", "переехал в калининград", "переехала в калининград",
    ))


def _vk_source_url_from_video_profile(value: Any) -> str:
    """Return a scan-capable VK wall URL for a VK Video author profile."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urllib.parse.urlparse(
            raw if re.match(r"^[a-z][a-z0-9+.-]*://", raw, re.I) else "https://" + raw.lstrip("/")
        )
    except Exception:
        return ""
    if (parsed.netloc or "").strip().lower() not in {"vkvideo.ru", "www.vkvideo.ru"}:
        return ""
    first = (parsed.path or "").strip("/").split("/", 1)[0]
    if not first.startswith("@"):
        return ""
    identity = first[1:].strip().lower()
    if not re.fullmatch(r"[a-z0-9_.-]{3,}", identity, re.I):
        return ""
    return f"https://vk.com/{identity}"


def _evidence_canonical_source_keys(row: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    telegram_url = str(row.get("telegram_url") or "").strip()
    handle = _telegram_handle_from_url(telegram_url)
    if handle:
        keys.add(f"telegram:{handle}")
    public_keys: set[str] = set()
    vk_public_url = str(row.get("vk_public_url") or "").strip()
    for vk_url in (vk_public_url,):
        match = re.search(r"(?:https?://)?(?:www\.)?vk\.(?:com|ru)/([^/?#]+)", vk_url, re.I)
        if match:
            identity = match.group(1).strip().lower()
            if identity and identity not in {"wall", "feed", "video"}:
                public_keys.add(f"vk:{identity}")
    keys.update(public_keys)
    if not public_keys:
        video_profile_url = _vk_source_url_from_video_profile(row.get("vk_video_url"))
        match = re.search(r"(?:https?://)?(?:www\.)?vk\.com/([^/?#]+)", video_profile_url, re.I)
        if match:
            identity = match.group(1).strip().lower()
            if identity and identity not in {"wall", "feed", "video"}:
                keys.add(f"vk:{identity}")
    return keys


def _external_blogger_registry_metrics(
    evidence_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
) -> dict[str, int]:
    confirmed = [row for row in evidence_rows if str(row.get("confirmation_status") or "").strip().lower() == "confirmed_external"]
    review = [row for row in evidence_rows if str(row.get("confirmation_status") or "").strip().lower() == "needs_externality_review"]
    eligible_records = [row for row in confirmed if not _evidence_relation_is_local(row.get("region_relation_status"))]
    record_keys = [(row, _evidence_canonical_source_keys(row)) for row in eligible_records]
    canonical_keys = set().union(*(keys for _, keys in record_keys)) if record_keys else set()
    queue_keys: set[str] = set()
    scanned_keys: set[str] = set()
    ko_keys: set[str] = set()
    for row in source_rows:
        row_keys: set[str] = set()
        canonical = str(row.get("canonical_source_key") or "").strip().lower().rstrip("/")
        if canonical:
            row_keys.add(canonical)
        for alias in _source_alias_keys(row):
            if alias.startswith(("telegram:", "vk:")):
                row_keys.add(alias)
        queue_keys.update(row_keys)
        if _source_has_scan_evidence(row):
            scanned_keys.update(row_keys)
        if _source_has_ko_candidate(row):
            ko_keys.update(row_keys)
    queued = canonical_keys & queue_keys
    supported_records = [(row, keys) for row, keys in record_keys if keys]
    records_in_queue = [(row, keys) for row, keys in supported_records if keys & queue_keys]
    records_scanned = [(row, keys) for row, keys in supported_records if keys & scanned_keys]
    records_with_ko = [(row, keys) for row, keys in supported_records if keys & ko_keys]
    return {
        "external_blogger_registry_records_total": len(evidence_rows),
        "external_blogger_registry_confirmed_records_total": len(confirmed),
        "external_blogger_registry_needs_review_records_total": len(review),
        "external_blogger_registry_confirmed_local_excluded_records_total": len(confirmed) - len(eligible_records),
        "external_blogger_registry_eligible_records_total": len(eligible_records),
        "external_blogger_registry_records_with_supported_tg_vk_source_total": len(supported_records),
        "external_blogger_registry_records_without_supported_tg_vk_source_total": len(eligible_records) - len(supported_records),
        "external_blogger_registry_supported_records_in_queue_total": len(records_in_queue),
        "external_blogger_registry_supported_records_missing_from_queue_total": len(supported_records) - len(records_in_queue),
        "external_blogger_registry_supported_records_with_scanned_source_total": len(records_scanned),
        "external_blogger_registry_supported_records_without_scanned_source_total": len(supported_records) - len(records_scanned),
        "external_blogger_registry_supported_records_with_ko_source_total": len(records_with_ko),
        "external_blogger_registry_pipeline_stored_only_records_total": sum(
            1 for row in evidence_rows if str(row.get("pipeline_status") or "").strip().lower() == "stored_only"
        ),
        "external_blogger_registry_canonical_tg_vk_sources_total": len(canonical_keys),
        "external_blogger_registry_canonical_sources_in_queue_total": len(queued),
        "external_blogger_registry_canonical_sources_missing_from_queue_total": len(canonical_keys - queue_keys),
        "external_blogger_registry_canonical_sources_scanned_total": len(canonical_keys & scanned_keys),
        "external_blogger_registry_canonical_sources_with_ko_total": len(canonical_keys & ko_keys),
    }


def _confirmed_external_blogger_funnel_metrics(
    source_rows: list[dict[str, Any]],
    processed_post_rows: list[dict[str, Any]],
    images: list[dict[str, Any]],
    publications: list[dict[str, Any]],
    deliveries: list[dict[str, Any]],
) -> dict[str, int]:
    sources = [
        row for row in source_rows
        if str(row.get("external_blogger_evidence_status") or "").strip().lower() == "confirmed_external"
    ]
    scanned = [row for row in sources if _source_has_scan_evidence(row)]
    terminal_sources = [
        row for row in sources
        if str(row.get("source_queue_status") or row.get("fetch_status") or "").strip().lower().startswith("rejected_")
    ]
    active_pending = [row for row in sources if row not in scanned and row not in terminal_sources]
    source_aliases: dict[str, set[str]] = {}
    source_platforms: dict[str, str] = {}
    for index, row in enumerate(sources):
        source_key = str(
            row.get("canonical_source_key")
            or row.get("source_url")
            or row.get("canonical_url")
            or _source_merge_key(row)
            or f"source-{index}"
        ).strip().lower().rstrip("/")
        source_aliases[source_key] = _source_alias_keys(row)
        source_platforms[source_key] = str(row.get("platform") or "").strip().lower()
    aliases = set().union(*source_aliases.values()) if source_aliases else set()
    posts = [row for row in processed_post_rows if _source_alias_keys(row) & aliases]

    def matching_source_keys(row: dict[str, Any]) -> set[str]:
        row_aliases = _source_alias_keys(row)
        row_platform = str(row.get("platform") or "").strip().lower()
        return {
            key for key, values in source_aliases.items()
            if row_aliases & values
            and (not row_platform or not source_platforms.get(key) or source_platforms[key] == row_platform)
        }

    def identity(row: dict[str, Any]) -> str:
        return str(row.get("platform_post_key") or row.get("post_url") or row.get("post_id") or "").strip().rstrip("/")

    unique_posts = {identity(row): row for row in posts if identity(row)}
    processed_source_keys: set[str] = set()
    vector_accepted_source_keys: set[str] = set()
    post_url_source_keys: dict[str, set[str]] = {}
    for row in unique_posts.values():
        keys = matching_source_keys(row)
        processed_source_keys.update(keys)
        if (
            str(row.get("vector_gate_status") or "") == "vector_accept_candidate"
            and str(row.get("text_vector_fusion_status") or "") == "fused_e5_bge_m3"
        ):
            vector_accepted_source_keys.update(keys)
        url = str(row.get("post_url") or "").strip().rstrip("/")
        if url:
            post_url_source_keys.setdefault(url, set()).update(keys)
    post_urls = {
        str(row.get("post_url") or "").strip().rstrip("/")
        for row in unique_posts.values() if str(row.get("post_url") or "").strip()
    }
    image_urls = {
        str(row.get("post_url") or "").strip().rstrip("/")
        for row in images if str(row.get("post_url") or "").strip().rstrip("/") in post_urls
    }
    publication_rows = [
        row for row in publications
        if str(row.get("post_url") or "").strip().rstrip("/") in post_urls
    ]
    publication_urls = {
        str(row.get("post_url") or "").strip().rstrip("/") for row in publication_rows
    }
    publication_confirmed_urls = {
        str(row.get("post_url") or "").strip().rstrip("/")
        for row in publication_rows if is_confirmed_publication(row)
    }
    delivery_urls = {
        str(row.get("post_url") or "").strip().rstrip("/")
        for row in deliveries
        if str(row.get("post_url") or "").strip().rstrip("/") in post_urls
        and str(row.get("delivery_status") or row.get("status") or "").strip().lower() in {"completed", "sent", "delivered"}
    }
    image_source_keys = set().union(*(post_url_source_keys.get(url, set()) for url in image_urls)) if image_urls else set()
    publication_source_keys = set().union(*(post_url_source_keys.get(url, set()) for url in publication_urls)) if publication_urls else set()
    confirmed_source_keys = set().union(*(post_url_source_keys.get(url, set()) for url in publication_confirmed_urls)) if publication_confirmed_urls else set()
    delivered_source_keys = set().union(*(post_url_source_keys.get(url, set()) for url in delivery_urls)) if delivery_urls else set()
    source_level_ko_keys = {
        str(
            row.get("canonical_source_key")
            or row.get("source_url")
            or row.get("canonical_url")
            or _source_merge_key(row)
            or ""
        ).strip().lower().rstrip("/")
        for row in sources
        if _source_has_ko_candidate(row) or str(row.get("fast_check_status") or "") == "ko_hit"
    }
    sources_with_ko_keys = {key for key in source_level_ko_keys if key} | vector_accepted_source_keys
    return {
        "confirmed_external_blogger_sources_total": len(sources),
        "confirmed_external_blogger_telegram_total": sum(1 for row in sources if str(row.get("platform") or "") == "telegram"),
        "confirmed_external_blogger_vk_total": sum(1 for row in sources if str(row.get("platform") or "") == "vk"),
        "confirmed_external_blogger_pending_total": len(active_pending),
        "confirmed_external_blogger_unscanned_total": len(active_pending),
        "confirmed_external_blogger_terminal_total": len(terminal_sources),
        "confirmed_external_blogger_queue_pending_status_total": sum(1 for row in sources if str(row.get("source_queue_status") or "") == "pending_scan"),
        "confirmed_external_blogger_scanned_total": len(scanned),
        "confirmed_external_blogger_with_ko_total": len(sources_with_ko_keys),
        "confirmed_external_blogger_fast_check_hit_total": sum(1 for row in sources if str(row.get("fast_check_status") or "") == "ko_hit"),
        "confirmed_external_blogger_vk_search_checked_total": sum(1 for row in sources if _safe_int(row.get("vk_wall_search_query_count")) > 0),
        "confirmed_external_blogger_vk_search_hit_total": sum(1 for row in sources if _safe_int(row.get("vk_wall_search_hits")) > 0),
        "confirmed_external_blogger_rejected_local_total": sum(1 for row in sources if str(row.get("source_queue_status") or "") == "rejected_local_region_source"),
        "confirmed_external_blogger_rejected_spam_total": sum(1 for row in sources if str(row.get("source_queue_status") or "") == "rejected_spam_source"),
        "confirmed_external_blogger_rejected_unresolvable_vk_total": sum(
            1 for row in sources
            if str(row.get("source_queue_status") or row.get("fetch_status") or "") == "rejected_unresolvable_vk_source"
        ),
        "confirmed_external_blogger_rejected_unresolvable_telegram_total": sum(
            1 for row in sources
            if str(row.get("source_queue_status") or row.get("fetch_status") or "") == "rejected_unresolvable_telegram_source"
        ),
        "confirmed_external_blogger_fetch_error_total": sum(
            1 for row in sources
            if str(row.get("fetch_status") or row.get("last_scan_status") or "").startswith("error")
        ),
        "confirmed_external_blogger_posts_processed_total": len(unique_posts),
        "confirmed_external_blogger_sources_with_processed_posts_total": len(processed_source_keys),
        "confirmed_external_blogger_vector_accepted_posts_total": len({
            identity(row) for row in unique_posts.values()
            if (
                str(row.get("vector_gate_status") or "") == "vector_accept_candidate"
                and str(row.get("text_vector_fusion_status") or "") == "fused_e5_bge_m3"
            )
        }),
        "confirmed_external_blogger_sources_with_vector_accepted_posts_total": len(vector_accepted_source_keys),
        "confirmed_external_blogger_image_queue_posts_total": len(image_urls),
        "confirmed_external_blogger_sources_with_image_queue_posts_total": len(image_source_keys),
        "confirmed_external_blogger_publication_posts_total": len(publication_urls),
        "confirmed_external_blogger_sources_with_publication_posts_total": len(publication_source_keys),
        "confirmed_external_blogger_publication_confirmed_posts_total": len(publication_confirmed_urls),
        "confirmed_external_blogger_sources_with_publication_confirmed_posts_total": len(confirmed_source_keys),
        "confirmed_external_blogger_delivery_completed_posts_total": len(delivery_urls),
        "confirmed_external_blogger_sources_with_delivery_completed_posts_total": len(delivered_source_keys),
    }


def _row_has_ko_candidate_evidence(row: dict[str, Any]) -> bool:
    return (
        str(row.get("kaliningrad_oblast_only_scope") or "").lower() in {"1", "true", "yes"}
        or str(row.get("text_region_confirmation_status") or "") == "text_confirmed_ko_only_for_image_analysis"
        or str(row.get("vector_gate_status") or "") == "vector_accept_candidate"
        or is_confirmed_publication(row)
    )


def _ko_candidate_source_keys(rows: Iterable[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for row in rows:
        key = _post_source_merge_key(row)
        if key and _row_has_ko_candidate_evidence(row):
            keys.add(key)
    return keys


def _merge_source_rows(*row_lists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    canonical_queue_updated_at: dict[str, str] = {}
    # ``source_queue_item`` is the canonical durable source verdict and is
    # passed first.  Later ``source_status_item`` / ``online_source_item`` rows
    # are deliberately sparse execution overlays.  A historical online row can
    # therefore still say ``processed_found_ko_candidate`` + ``unknown`` after
    # the canonical queue has terminally classified the same source as local or
    # spam.  Such an overlay may add counters/fetch details, but it must never
    # resurrect the source into the external publication funnel.
    terminal_classification_fields = {
        "source_queue_status",
        "source_scope",
        "source_geo_class",
        "source_topic_class",
        "source_quick_class",
        "source_surface_filter_version",
        "source_surface_filter_reason",
        "source_local_hits",
        "source_spam_hits",
        "source_hard_spam_hits",
        "source_commercial_promo_hits",
        "source_commercial_promo_dominant",
        "source_commercial_editorial_prefix_chars",
        "source_commercial_dominant_posts",
        "source_commercial_sampled_text_posts",
        "source_commercial_dominant_ratio",
        "source_spam_reopen_status",
        "source_spam_reopen_previous_reason",
        "source_spam_hashtags",
        "monitoring_exclusion_reason",
        "source_locality_reconciliation_status",
        "source_locality_reconciliation_reason",
        "source_locality_surface_class",
        "source_locality_class",
        "source_repeated_ko_evidence_status",
        "source_repeated_ko_sampled_posts",
        "source_repeated_ko_posts",
        "source_repeated_ko_dated_posts",
        "source_repeated_ko_ratio",
        "source_repeated_ko_span_days",
        "source_repeated_ko_oldest_date",
        "source_repeated_ko_newest_date",
        "next_action",
    }
    for rows in row_lists:
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = _source_merge_key(row)
            if not key:
                continue
            current = dict(merged.get(key) or {})
            numeric_max_fields = {
                "posts_scanned",
                "ko_posts_found",
                "candidate_posts_found",
                "actual_images_scored_count",
                "low_actual_image_count",
            }
            current_is_terminal = _vector_source_terminal_excluded(current)
            incoming_is_terminal = _vector_source_terminal_excluded(row)
            incoming_updated_at = max(
                str(row.get("updated_at") or ""),
                str(row.get("queue_item_updated_at") or ""),
                str(row.get("source_status_updated_at") or ""),
                str(row.get("_ydb_updated_at") or ""),
            )
            incoming_is_canonical_queue = str(row.get("_ydb_pk") or "").startswith("source_queue_item:")
            incoming_is_status_overlay = str(row.get("_ydb_pk") or "").startswith(("source_status_item:", "online_source_item:"))
            if incoming_is_canonical_queue:
                canonical_queue_updated_at[key] = incoming_updated_at
            canonical_updated_at = canonical_queue_updated_at.get(key, "")
            for k, v in row.items():
                if v in (None, ""):
                    continue
                if k in numeric_max_fields:
                    current[k] = max(_safe_int(current.get(k)), _safe_int(v))
                elif (
                    k in terminal_classification_fields
                    and incoming_is_status_overlay
                    and canonical_updated_at
                    and incoming_updated_at
                    and incoming_updated_at <= canonical_updated_at
                ):
                    # A newer canonical queue repair (for example reopening a
                    # false commercial-spam verdict) owns classification. A
                    # stale source_status heartbeat may still contribute
                    # counters, but cannot immediately re-tombstone the source.
                    continue
                elif current_is_terminal and not incoming_is_terminal and k in terminal_classification_fields:
                    continue
                else:
                    current[k] = v
            merged[key] = current
    return list(merged.values())


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _latest_llm_budget_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the active/latest product budget instead of summing daily limits."""
    return max(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: (str(row.get("updated_at") or ""), str(row.get("budget_id") or "")),
        default={},
    )


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


LEGACY_IMAGE_PUBLICATION_GATES = frozenset({
    "region_talk_publication_eligibility_v2",
    "region_talk_publication_eligibility_v3",
    "region_talk_publication_eligibility_v4",
})
IMAGE_VLM_PROMPT_VERSION = "region_talk_visual_adjudicator_v2"
IMAGE_VLM_DECISION_VERSION = "region_talk_visual_decision_v2"


def _image_vlm_verdict_is_current(row: dict[str, Any]) -> bool:
    return bool(
        str(row.get("image_vlm_status") or "").lower() == "completed"
        and str(row.get("image_vlm_decision") or "").lower() in {"accept", "reject", "review", "needs_review"}
        and str(row.get("image_vlm_prompt_version") or "") == IMAGE_VLM_PROMPT_VERSION
        and str(row.get("image_vlm_decision_version") or "") == IMAGE_VLM_DECISION_VERSION
        and str(row.get("image_vlm_request_fingerprint") or "").strip()
        and str(row.get("image_vlm_media_manifest_hash") or "").strip()
        == str(row.get("input_media_manifest_hash") or "").strip()
    )


def _image_vlm_backlog_candidate(row: dict[str, Any]) -> bool:
    """Mirror the Kaggle visual-adjudicator admission contract for planning."""

    if str(row.get("image_quality_decision") or "") != "needs_visual_review":
        return False
    if str(row.get("image_quality_reason") or "") != "uncalibrated_legacy_low_score_requires_visual_review":
        return False
    if str(row.get("publication_eligibility_decision") or "") != "accept":
        return False
    if str(row.get("publication_eligibility_gate_version") or "") != CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION:
        return False
    if str(row.get("vector_gate_status") or "") != "vector_accept_candidate":
        return False
    if str(row.get("text_vector_fusion_status") or "") != "fused_e5_bge_m3":
        return False
    if str(row.get("image_model_input_type") or "") != "actual_image":
        return False
    if str(row.get("image_acquisition_status") or "") != "complete":
        return False
    if str(row.get("image_component_bundle_complete") or "").lower() != "true":
        return False
    expected = _safe_int(row.get("expected_image_count"))
    fetched = _safe_int(row.get("fetched_image_count"))
    if expected <= 0 or expected != fetched or not str(row.get("input_media_manifest_hash") or "").strip():
        return False
    if _image_vlm_verdict_is_current(row):
        return False
    overall = _safe_float(row.get("overall_media_score") or row.get("final_visual_score")) or 0.0
    postcard = _safe_float(row.get("postcardness_score") or row.get("clip_postcardness_score") or row.get("cv_postcardness_score")) or 0.0
    best = _safe_float(row.get("shadow_best_frame_score")) or 0.0
    return overall >= 0.58 or postcard >= 0.85 or best >= 0.66


def _image_contract_rescore_candidate(row: dict[str, Any]) -> bool:
    """Keep old accepted low actual-image rows visible during v5 migration.

    ImageDiagnostic may temporarily mark a stale producer attestation as
    deferred while CandidateReport refreshes it.  The orchestrator therefore
    reads both the current producer fields and the image consumer's observed
    legacy attestation instead of hiding migration work behind the current
    queue status.
    """
    score = _safe_float(row.get("overall_media_score") or row.get("final_visual_score"))
    current_legacy_accept = (
        str(row.get("publication_eligibility_decision") or "").lower() == "accept"
        and str(row.get("publication_eligibility_gate_version") or "") in LEGACY_IMAGE_PUBLICATION_GATES
    )
    audited_legacy_accept = (
        str(row.get("image_eligibility_decision") or "").lower() == "accept"
        and str(row.get("image_eligibility_gate_version") or "") in LEGACY_IMAGE_PUBLICATION_GATES
    )
    current_decision = str(row.get("publication_eligibility_decision") or "").lower()
    if current_decision not in {"", "accept"}:
        circular_reason = str(
            row.get("publication_eligibility_reason")
            or row.get("publication_eligibility_primary_reason")
            or ""
        )
        if circular_reason not in {
            "image_queue_not_actual_scored",
            "actual_image_required",
            "image_quality_contract_decision_missing",
        }:
            return False
    return bool(
        str(row.get("image_model_input_type") or "") == "actual_image"
        and str(row.get("image_decision_contract_version") or "") != "region_talk_image_album_guard_v2"
        and score is not None
        and score < float(os.getenv("REGION_TALK_PUBLICATION_MIN_OVERALL_MEDIA_SCORE") or "0.66")
        and (current_legacy_accept or audited_legacy_accept)
    )


def _avg_numeric(rows: list[dict[str, Any]], field: str) -> float:
    values = [_safe_float(r.get(field)) for r in rows]
    nums = [v for v in values if v is not None]
    return round(sum(nums) / len(nums), 2) if nums else 0.0


def _max_numeric(rows: list[dict[str, Any]], field: str) -> float:
    values = [_safe_float(r.get(field)) for r in rows]
    nums = [v for v in values if v is not None]
    return round(max(nums), 2) if nums else 0.0


def _min_numeric(rows: list[dict[str, Any]], field: str) -> float:
    values = [_safe_float(r.get(field)) for r in rows]
    nums = [v for v in values if v is not None]
    return round(min(nums), 2) if nums else 0.0


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _image_browser_materialization_metrics(
    images: list[dict[str, Any]], *, now: datetime | None = None
) -> dict[str, int]:
    """Expose the bounded browser bridge without mixing it into VLM backlog.

    The predicate intentionally mirrors ``region_talk_article_browser_materialize``:
    only a row whose finite retry window is due and whose lease is free can
    launch Chromium.  Rows waiting for that bridge are not ordinary
    ImageDiagnostic work because they do not have a fetchable media reference
    yet.
    """

    current = now or datetime.now(timezone.utc)
    terminal_statuses = {"terminal_no_associated_images", "terminal_fetch_failed"}
    waiting = [
        row for row in images
        if str(row.get("image_queue_status") or "") == "needs_browser_materialization"
    ]

    def active_lease(row: dict[str, Any]) -> bool:
        lease_until = _parse_iso_datetime(row.get("browser_materialization_lease_expires_at"))
        return bool(
            str(row.get("browser_materialization_lease_run_id") or "")
            and lease_until
            and lease_until > current
        )

    def is_due(row: dict[str, Any]) -> bool:
        if str(row.get("browser_materialization_status") or "") in terminal_statuses:
            return False
        if _safe_int(row.get("browser_materialization_attempt_count")) >= 3:
            return False
        next_attempt = _parse_iso_datetime(row.get("browser_materialization_next_attempt_after"))
        if next_attempt and next_attempt > current:
            return False
        return not active_lease(row) and bool(str(row.get("post_url") or "").strip())

    return {
        "image_browser_materialization_waiting_total": len(waiting),
        "image_browser_materialization_due_total": sum(1 for row in waiting if is_due(row)),
        "image_browser_materialization_leased_total": sum(1 for row in waiting if active_lease(row)),
        "image_browser_materialization_retry_wait_total": sum(
            1 for row in waiting
            if str(row.get("browser_materialization_status") or "") == "retry_wait"
            and not is_due(row)
            and not active_lease(row)
        ),
        "image_browser_materialization_attempts_exhausted_total": sum(
            1 for row in waiting
            if _safe_int(row.get("browser_materialization_attempt_count")) >= 3
        ),
        "image_browser_materialized_total": sum(
            1 for row in images
            if str(row.get("browser_materialization_status") or "") == "materialized"
        ),
        "image_browser_materialization_terminal_total": sum(
            1 for row in images
            if str(row.get("browser_materialization_status") or "") in terminal_statuses
        ),
    }


def _canonical_post_url(row_or_url: dict[str, Any] | str) -> str:
    if isinstance(row_or_url, dict):
        raw = str(row_or_url.get("post_url") or row_or_url.get("keyword_hit_post_url") or "")
    else:
        raw = str(row_or_url or "")
    return raw.strip().split("?", 1)[0].rstrip("/").lower()


def _is_public_telegram_post_url(url: str) -> bool:
    return bool(re.fullmatch(r"https?://t\.me/(?!c/)[a-z0-9_]+/[0-9]+", str(url or ""), flags=re.I))


def _source_aliases(row: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    for field in ("entity_cache_key", "private_state_key", "canonical_source_key", "source_key"):
        value = str(row.get(field) or "").strip().lower()
        if value:
            aliases.add(value)
    for field in ("username", "username_or_handle", "handle"):
        value = str(row.get(field) or "").strip().lower().lstrip("@")
        if value:
            aliases.update({value, "telegram:username:" + value, "telegram:" + value})
    for field in ("canonical_url", "source_url", "keyword_hit_source_url", "post_url"):
        value = str(row.get(field) or "").strip().lower().split("?", 1)[0].rstrip("/")
        match = re.match(r"https?://t\.me/([a-z0-9_]+)(?:/[0-9]+)?$", value, flags=re.I)
        if match:
            handle = match.group(1).lower()
            aliases.update({handle, "telegram:username:" + handle, "telegram:" + handle, "https://t.me/" + handle})
    return aliases


def _entity_cache_metrics(post_links: list[dict[str, Any]], entity_cache_rows: list[dict[str, Any]]) -> dict[str, int]:
    valid_rows = [
        row for row in entity_cache_rows
        if str(row.get("channel_id_private") or "").strip() and str(row.get("access_hash_private") or "").strip()
    ]
    cached_aliases: set[str] = set()
    for row in valid_rows:
        cached_aliases.update(_source_aliases(row))
    active_rows = [row for row in post_links if _post_link_state(row) not in {"terminal", "unknown"}]
    cache_hits = [row for row in active_rows if _source_aliases(row) & cached_aliases]
    entity_wait_rows = [row for row in active_rows if _post_link_state(row) == "entity_wait"]
    return {
        "telegram_entity_cache_rows_total": len(entity_cache_rows),
        "telegram_entity_cache_valid_rows_total": len(valid_rows),
        "telegram_entity_cache_invalid_rows_total": len(entity_cache_rows) - len(valid_rows),
        "post_link_queue_entity_cache_hit_total": len(cache_hits),
        "post_link_queue_entity_cache_miss_total": max(0, len(active_rows) - len(cache_hits)),
        "post_link_queue_entity_wait_cache_now_available_total": sum(1 for row in entity_wait_rows if _source_aliases(row) & cached_aliases),
    }


def _post_link_state(row: dict[str, Any], *, now: datetime | None = None) -> str:
    status = str(row.get("post_link_status") or "").strip().lower()
    if status in POST_LINK_TERMINAL_STATUSES or status.startswith("terminal_"):
        return "terminal"
    if status == "retry_wait_entity_cache" or "entity_cache" in str(row.get("fetch_error_code") or "").lower():
        return "entity_wait"
    next_attempt = _parse_iso_datetime(row.get("next_attempt_after"))
    current = now or datetime.now(timezone.utc)
    if (next_attempt and next_attempt > current) or "cooldown" in status:
        return "cooldown"
    if status in POST_LINK_READY_STATUSES:
        return "ready" if _is_public_telegram_post_url(_canonical_post_url(row)) else "unknown"
    return "unknown"


def _source_terminal_post_link_urls(
    post_links: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
) -> set[str]:
    terminal_aliases: set[str] = set()
    for source in source_rows:
        if _vector_source_terminal_excluded(source):
            terminal_aliases.update(_source_aliases(source))
    if not terminal_aliases:
        return set()
    return {
        _canonical_post_url(row)
        for row in post_links
        if _canonical_post_url(row) and (_source_aliases(row) & terminal_aliases)
    }


def _post_link_queue_metrics(
    post_links: list[dict[str, Any]],
    entity_cache_rows: list[dict[str, Any]] | None = None,
    source_rows: list[dict[str, Any]] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    urls = [_canonical_post_url(row) for row in post_links]
    raw_states = [_post_link_state(row, now=current) for row in post_links]
    source_terminal_urls = _source_terminal_post_link_urls(post_links, source_rows or [])
    states = [
        "terminal" if url and url in source_terminal_urls else state
        for url, state in zip(urls, raw_states)
    ]
    url_counts: dict[str, int] = {}
    for url in urls:
        if url:
            url_counts[url] = url_counts.get(url, 0) + 1

    def queue_key(item: tuple[dict[str, Any], str]) -> tuple[int, str, str]:
        row, _state = item
        return (
            _safe_int(row.get("post_link_priority")),
            str(row.get("first_seen_at") or row.get("created_at") or row.get("updated_at") or row.get("first_seen_run_id") or ""),
            _canonical_post_url(row),
        )

    active = sorted(
        [(row, state) for row, state in zip(post_links, states) if state != "terminal"],
        key=queue_key,
    )
    blocked_prefix: list[tuple[dict[str, Any], str]] = []
    for item in active:
        if item[1] == "ready":
            break
        blocked_prefix.append(item)
    if not any(state == "ready" for _, state in active):
        blocked_prefix = active

    metrics: dict[str, Any] = {
        "post_link_queue_total": len(post_links),
        "post_link_queue_exact_ready_total": states.count("ready"),
        "post_link_queue_cooldown_total": states.count("cooldown"),
        "post_link_queue_entity_wait_total": states.count("entity_wait"),
        "post_link_queue_terminal_total": states.count("terminal"),
        "post_link_queue_source_terminal_cleanup_total": sum(
            1 for url, state in zip(urls, raw_states)
            if url in source_terminal_urls and state != "terminal"
        ),
        "post_link_queue_unknown_status_total": states.count("unknown"),
        # Backward-compatible alias: pending means actionable now, not every
        # retry/cooldown/entity-wait row.
        "post_link_queue_pending_total": states.count("ready"),
        "post_link_queue_fetched_total": sum(1 for row in post_links if str(row.get("post_link_status") or "").lower() == "fetched"),
        "post_link_queue_unique_urls_total": len(url_counts),
        "post_link_queue_integrity_missing_url_total": urls.count(""),
        "post_link_queue_integrity_invalid_url_total": sum(1 for url in urls if url and not _is_public_telegram_post_url(url)),
        "post_link_queue_integrity_duplicate_url_values_total": sum(1 for count in url_counts.values() if count > 1),
        "post_link_queue_integrity_duplicate_url_rows_total": sum(max(0, count - 1) for count in url_counts.values()),
        "post_link_queue_head_blocked_total": len(blocked_prefix),
        "post_link_queue_head_blocked_cooldown_total": sum(1 for _, state in blocked_prefix if state == "cooldown"),
        "post_link_queue_head_blocked_entity_wait_total": sum(1 for _, state in blocked_prefix if state == "entity_wait"),
        "post_link_queue_head_blocked_integrity_total": sum(1 for _, state in blocked_prefix if state == "unknown"),
        "post_link_queue_head_ready": int(bool(active and active[0][1] == "ready")),
        "post_link_queue_head_url": _canonical_post_url(active[0][0]) if active else "",
        "post_link_queue_head_state": active[0][1] if active else "empty",
    }
    metrics.update(_entity_cache_metrics(post_links, entity_cache_rows or []))
    return metrics


def _bge_ready_exact_rescore_metrics(
    post_links: list[dict[str, Any]],
    processed_rows: list[dict[str, Any]],
    vector_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    deferred = {
        _canonical_post_url(row)
        for row in processed_rows
        if str(row.get("vector_gate_status") or "").startswith("vector_defer")
        or str(row.get("current_stage") or "") == "dual_model_vector_enrichment_pending"
    }
    bge = {
        _canonical_post_url(row)
        for row in vector_rows
        if str(row.get("model_short") or "") == "bge_m3"
        or str(row.get("model_id") or "") == "BAAI/bge-m3"
    }
    terminal = {
        _canonical_post_url(row)
        for row in publication_rows
        if str(row.get("publication_status") or "").startswith(("gemini_", "operator_rejected", "eligibility_"))
        or str(row.get("publication_candidate_status") or "") in {
            "llm_confirmed", "llm_rejected", "llm_needs_review", "sent_to_chat",
            "accepted_for_publication", "tombstoned_reject", "revoked",
        }
    }
    fetched = {
        _canonical_post_url(row)
        for row in post_links
        if str(row.get("post_link_status") or "") == "fetched"
    }
    source_terminal = _source_terminal_post_link_urls(post_links, source_rows or [])
    source_terminal_cleanup = ((deferred & bge & fetched) - terminal) & source_terminal
    ready = sorted((((deferred & bge & fetched) - terminal) - source_terminal) - {""})
    return {
        "post_link_queue_bge_ready_rescore_total": len(ready),
        "post_link_queue_bge_ready_rescore_urls": ready,
        "post_link_queue_bge_ready_rescore_source_terminal_cleanup_total": len(source_terminal_cleanup - {""}),
    }


def _source_queue_integrity_metrics(source_rows: list[dict[str, Any]], cursor_position: int) -> dict[str, int]:
    sequences = [_safe_int(row.get("queue_seq")) for row in source_rows]
    positive_sequences = [sequence for sequence in sequences if sequence > 0]
    sequence_counts: dict[int, int] = {}
    for sequence in positive_sequences:
        sequence_counts[sequence] = sequence_counts.get(sequence, 0) + 1
    orders = [_safe_int(row.get("queue_order")) for row in source_rows]
    positive = [order for order in orders if order > 0]
    order_counts: dict[int, int] = {}
    for order in positive:
        order_counts[order] = order_counts.get(order, 0) + 1
    return {
        "source_queue_integrity_missing_seq_total": sum(1 for sequence in sequences if sequence <= 0),
        "source_queue_integrity_duplicate_seq_values_total": sum(1 for count in sequence_counts.values() if count > 1),
        "source_queue_integrity_duplicate_seq_rows_total": sum(max(0, count - 1) for count in sequence_counts.values()),
        # Canonical compatibility aliases now refer to immutable queue_seq.
        "source_queue_integrity_unordered_total": sum(1 for sequence in sequences if sequence <= 0),
        "source_queue_integrity_duplicate_order_values_total": sum(1 for count in sequence_counts.values() if count > 1),
        "source_queue_integrity_duplicate_order_rows_total": sum(max(0, count - 1) for count in sequence_counts.values()),
        "source_queue_integrity_legacy_order_missing_total": sum(1 for order in orders if order <= 0),
        "source_queue_integrity_legacy_order_duplicate_rows_total": sum(max(0, count - 1) for count in order_counts.values()),
        "source_queue_integrity_cursor_past_max_total": int(bool(positive and cursor_position > max(positive))),
    }


def _inflow_kind(row: dict[str, Any]) -> str:
    haystack = " ".join(str(row.get(field) or "") for field in (
        "inflow_type", "intake_type", "added_from", "insertion_policy", "discovery_type", "edge_type",
        "frontier_reason", "priority_reason", "source", "source_kind", "matched_query", "matched_hashtag",
    )).lower()
    if any(token in haystack for token in ("manual", "operator", "user_provided", "human_intake")):
        return "manual"
    if "hashtag" in haystack:
        return "hashtag"
    if "similar" in haystack or "recommendation" in haystack:
        return "similar"
    if "keyword" in haystack or "fast_check" in haystack:
        return "keyword"
    return "source"


def _discovery_inflow_metrics(rows: list[dict[str, Any]]) -> dict[str, int]:
    evidence_counts: dict[str, int] = {}
    unique: dict[str, set[str]] = {}
    for index, row in enumerate(rows):
        kind = _inflow_kind(row)
        evidence_counts[kind] = evidence_counts.get(kind, 0) + 1
        source_key = (
            _source_merge_key(row)
            or str(row.get("canonical_source_key") or row.get("source_candidate_id") or row.get("to_source_candidate_id") or "")
            or "evidence:" + str(index)
        )
        unique.setdefault(kind, set()).add(source_key)
    out: dict[str, int] = {}
    for kind in ("manual", "keyword", "hashtag", "similar", "source"):
        out[f"discovery_inflow_{kind}_total"] = len(unique.get(kind, set()))
        out[f"discovery_inflow_{kind}_evidence_rows_total"] = evidence_counts.get(kind, 0)
    return out


def _vector_post_key(row: dict[str, Any]) -> str:
    post_url = str(row.get("post_url") or "").strip()
    if post_url:
        return "url:" + post_url
    post_id = str(row.get("post_id") or "").strip()
    if post_id:
        return "id:" + post_id
    text_sha = str(row.get("paired_e5_text_hash") or row.get("text_hash") or "").strip()
    return "hash:" + text_sha if text_sha else ""


def _vector_exact_text_key(row: dict[str, Any]) -> str:
    post_url = str(row.get("post_url") or "").strip()
    post_id = str(row.get("post_id") or "").strip()
    text_sha = str(row.get("paired_e5_text_hash") or row.get("text_hash") or "").strip()
    if not text_sha:
        return _vector_post_key(row)
    if post_url:
        return f"url+hash:{post_url}|{text_sha}"
    if post_id:
        return f"id+hash:{post_id}|{text_sha}"
    return "hash:" + text_sha


def _vector_row_timestamp(row: dict[str, Any]) -> datetime | None:
    return _parse_iso_datetime(row.get("created_at") or row.get("_ydb_updated_at") or row.get("updated_at"))


def _vector_source_terminal_excluded(row: dict[str, Any]) -> bool:
    return bool(
        row.get("source_terminal_excluded") is True
        or str(row.get("source_terminal_excluded") or "").lower() in {"1", "true", "yes"}
        or str(row.get("source_queue_status") or row.get("fetch_status") or "")
        in {"rejected_local_region_source", "rejected_spam_source"}
        or str(row.get("source_scope") or "") in {"local_region", "spam"}
        or str(row.get("source_geo_class") or "") == "kaliningrad_local"
        or str(row.get("source_quick_class") or "") in {"local_region_source", "spam_source_reject"}
    )


def _text_vector_pair_metrics(
    e5_vectors: list[dict[str, Any]],
    bge_vectors: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    e5_post = {k for k in (_vector_post_key(r) for r in e5_vectors) if k}
    bge_post = {k for k in (_vector_post_key(r) for r in bge_vectors) if k}
    e5_exact = {k for k in (_vector_exact_text_key(r) for r in e5_vectors) if k}
    bge_exact = {k for k in (_vector_exact_text_key(r) for r in bge_vectors) if k}
    post_paired = e5_post & bge_post
    exact_paired = e5_exact & bge_exact
    metrics = {
        "text_vector_e5_unique_posts_total": len(e5_post),
        "text_vector_bge_m3_unique_posts_total": len(bge_post),
        "text_vector_dual_post_paired_total": len(post_paired),
        "text_vector_e5_without_bge_post_total": len(e5_post - bge_post),
        "text_vector_bge_without_e5_post_total": len(bge_post - e5_post),
        "text_vector_dual_exact_text_paired_total": len(exact_paired),
        "text_vector_e5_without_bge_exact_text_total": len(e5_exact - bge_exact),
        "text_vector_bge_without_e5_exact_text_total": len(bge_exact - e5_exact),
        "text_vector_dual_post_coverage_percent": int(round((len(post_paired) / len(e5_post)) * 100)) if e5_post else 0,
        "text_vector_dual_exact_text_coverage_percent": int(round((len(exact_paired) / len(e5_exact)) * 100)) if e5_exact else 0,
    }

    current_e5_rows = [row for row in e5_vectors if str(row.get("encoder_contract") or "") == CURRENT_E5_ENCODER_CONTRACT]
    current_bge_rows = [row for row in bge_vectors if str(row.get("encoder_contract") or "") == CURRENT_BGE_M3_ENCODER_CONTRACT]
    # Current means the newest E5 text per post under the active encoder
    # contract. Historical hashes remain visible in raw metrics but must not
    # masquerade as current BGE lag.
    latest_e5_by_post: dict[str, dict[str, Any]] = {}
    for row in current_e5_rows:
        post_key = _vector_post_key(row)
        exact_key = _vector_exact_text_key(row)
        if not post_key or not exact_key:
            continue
        previous = latest_e5_by_post.get(post_key)
        if previous is None or str(row.get("created_at") or row.get("_ydb_updated_at") or row.get("updated_at") or "") >= str(previous.get("created_at") or previous.get("_ydb_updated_at") or previous.get("updated_at") or ""):
            latest_e5_by_post[post_key] = row

    bge_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for row in current_bge_rows:
        exact_key = _vector_exact_text_key(row)
        version = str(row.get("semantic_bank_version") or "")
        if not exact_key:
            continue
        key = (exact_key, version)
        previous = bge_by_pair.get(key)
        if previous is None or str(row.get("created_at") or row.get("_ydb_updated_at") or row.get("updated_at") or "") >= str(previous.get("created_at") or previous.get("_ydb_updated_at") or previous.get("updated_at") or ""):
            bge_by_pair[key] = row

    paired: list[tuple[dict[str, Any], dict[str, Any]]] = []
    pending: list[dict[str, Any]] = []
    semantic_version_mismatch = 0
    for e5_row in latest_e5_by_post.values():
        exact_key = _vector_exact_text_key(e5_row)
        version = str(e5_row.get("semantic_bank_version") or "")
        bge_row = bge_by_pair.get((exact_key, version))
        if bge_row is not None:
            paired.append((e5_row, bge_row))
            continue
        if any(key[0] == exact_key for key in bge_by_pair):
            semantic_version_mismatch += 1
        pending.append(e5_row)

    paired_lags = []
    for e5_row, bge_row in paired:
        e5_at = _vector_row_timestamp(e5_row)
        bge_at = _vector_row_timestamp(bge_row)
        if e5_at and bge_at:
            paired_lags.append(max(0, int((bge_at - e5_at).total_seconds())))
    current = now or datetime.now(timezone.utc)
    pending_lags = [
        max(0, int((current - created_at).total_seconds()))
        for row in pending
        if (created_at := _vector_row_timestamp(row)) is not None
    ]
    current_bge_posts = {key for key in (_vector_post_key(row) for row in current_bge_rows) if key}
    current_total = len(latest_e5_by_post)
    current_paired = len(paired)
    bge_min_text_chars = max(1, _env_int("REGION_TALK_BGE_MIN_TEXT_CHARS", 24))

    def bge_input_text_length(row: dict[str, Any]) -> int:
        parts: list[str] = []
        for field in (
            "text", "full_text", "text_excerpt", "short_summary", "why_keep_in_memory",
            "why_this_is_about_kaliningrad", "what_positive", "what_neutral_or_useful",
            "llm_reason", "publication_story_reason", "model_short_explanation",
        ):
            value = re.sub(r"\s+", " ", str(row.get(field) or "")).strip()
            if value and value not in parts:
                parts.append(value)
        return len(re.sub(r"\s+", " ", ". ".join(parts)).strip())

    source_terminal_pending_rows = [row for row in pending if _vector_source_terminal_excluded(row)]
    actionable_e5_rows = [
        row for row in latest_e5_by_post.values()
        if not _vector_source_terminal_excluded(row) and bge_input_text_length(row) >= bge_min_text_chars
    ]
    actionable_pending_rows = [
        row for row in pending
        if not _vector_source_terminal_excluded(row) and bge_input_text_length(row) >= bge_min_text_chars
    ]
    below_bge_min_rows = [
        row for row in pending
        if not _vector_source_terminal_excluded(row) and bge_input_text_length(row) < bge_min_text_chars
    ]
    actionable_paired = len(actionable_e5_rows) - len(actionable_pending_rows)
    metrics.update({
        "text_vector_current_version_e5_unique_posts_total": current_total,
        "text_vector_current_version_bge_m3_unique_posts_total": len(current_bge_posts),
        "text_vector_current_version_dual_paired_total": current_paired,
        "text_vector_current_version_e5_without_bge_total": len(pending),
        "text_vector_current_version_semantic_bank_mismatch_total": semantic_version_mismatch,
        "text_vector_current_version_dual_coverage_percent": int(round((current_paired / current_total) * 100)) if current_total else 0,
        "text_vector_current_version_e5_below_bge_min_text_total": len(below_bge_min_rows),
        "text_vector_current_version_e5_without_bge_source_terminal_total": len(source_terminal_pending_rows),
        "text_vector_current_version_e5_without_bge_actionable_total": len(actionable_pending_rows),
        "text_vector_current_version_dual_actionable_coverage_percent": int(round((actionable_paired / len(actionable_e5_rows)) * 100)) if actionable_e5_rows else 0,
        "text_vector_current_version_bge_pair_lag_seconds_avg": int(round(sum(paired_lags) / len(paired_lags))) if paired_lags else 0,
        "text_vector_current_version_bge_pair_lag_seconds_max": max(paired_lags) if paired_lags else 0,
        "text_vector_current_version_bge_pending_lag_seconds_avg": int(round(sum(pending_lags) / len(pending_lags))) if pending_lags else 0,
        "text_vector_current_version_bge_pending_lag_seconds_max": max(pending_lags) if pending_lags else 0,
        "text_vector_stale_version_e5_rows_total": len(e5_vectors) - len(current_e5_rows),
        "text_vector_stale_version_bge_m3_rows_total": len(bge_vectors) - len(current_bge_rows),
    })
    return metrics


REGEX_KO_PATTERNS: tuple[re.Pattern[str], ...] = tuple(re.compile(p, re.I) for p in [
    r"(?<![а-яёa-z])калининград(?:ск(?:ая|ой|ую|ую|ом|ими?)?|а|е|у|ом)?(?:\s+обл(?:асть|асти|астью|\.)?)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])к[её]нигсберг(?:а|е|ом|ский|ская|ское|ские)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])куршск(?:ая|ой|ую|ою)?\s+кос(?:а|ы|е|у|ой)(?![а-яёa-z])",
    r"(?<![а-яёa-z])балтийск(?:ое\s+море|ая\s+коса|ой\s+косе|ую\s+косу|ой\s+косой|а|е|ом)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])зеленоградск(?:а|е|ом|ий|ая|ое|ие)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])светлогорск(?:а|е|ом|ий|ая|ое|ие)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])янтарн(?:ый|ого|ом|ому|ым|ая|ое|ые|ых)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])балтийск(?:а|е|ом|ий|ая|ое|ие)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])советск(?:а|е|ом|ий|ая|ое|ие)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])неман(?:а|е|ом)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])черняховск(?:а|е|ом|ий|ая|ое|ие)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])правдинск(?:а|е|ом|ий|ая|ое|ие)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])виштынец(?:кое\s+озеро|кий|кого|ком)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])роминтенск(?:ая|ой|ую)?\s+пущ(?:а|и|е|у|ей)(?![а-яёa-z])",
    r"(?<![а-яёa-z])краснолесь(?:е|я|ю|ем)(?![а-яёa-z])",
])
REGEX_EXTERNAL_GEO_PATTERNS: tuple[re.Pattern[str], ...] = tuple(re.compile(p, re.I) for p in [
    r"(?<![а-яёa-z])(?:байкал|дагестан|алтай|камчатк[аиуой]?|сахалин|кавказ|крым|сочи|казань|татарстан|суздаль|ярославль|кострома|псков|мурманск|териберк[аиуой]?|карели[яию]|архангельск|вологда|урал|сибирь|владивосток|краснодарск(?:ий\s+край)?|адыге[яию]|эльбрус)(?![а-яёa-z])",
    r"(?<![а-яёa-z])(?:москв[аеуойы]?|московск(?:ая|ой)\s+обл(?:асть|асти)?|санкт-петербург|петербург|ленинградск(?:ая|ой)\s+обл(?:асть|асти)?)(?![а-яёa-z])",
    r"(?<![а-яёa-z])(?:новосибирск|омск|томск|кемерово|красноярск|иркутск|буряти[яию]|хабаровск|магадан|чукотк[аиуой]?)(?:ая|ой|ую|им|ом|\s+обл(?:асть|асти)?|\s+край)?(?![а-яёa-z])",
    r"(?<![а-яёa-z])(?:польш[аиуеой]?|литв[аиуеой]?|латви[яиюе]|эстони[яиюе]|германи[яиюе]|беларус[ьи]?|грузи[яиюе]|армени[яиюе]|турци[яиюе]|итали[яиюе]|франци[яиюе]|испани[яиюе]|китай)(?![а-яёa-z])",
])
REGEX_MULTI_REGION_PATTERNS: tuple[re.Pattern[str], ...] = tuple(re.compile(p, re.I) for p in [
    r"(?<![а-яёa-z])(?:подборк[аиуой]?|топ\s*[-—]?\s*\d+|куда\s+поехать|мест[а]?\s+россии|регион(?:ы|ов)\s+россии|направлени[яй]|маршрут(?:ы|ов)?\s+по\s+россии)(?![а-яёa-z])",
    r"(?<![а-яёa-z])(?:от\s+калининграда\s+до|от\s+байкала\s+до|по\s+разным\s+регионам)(?![а-яёa-z])",
])
REGEX_AD_PROMO_PATTERNS: tuple[re.Pattern[str], ...] = tuple(re.compile(p, re.I) for p in [
    r"(?<![а-яёa-z])(?:реклама|на\s+правах\s+рекламы|партн[её]рский\s+материал|промокод|скидк[аиуой]?|акци[ияю]|розыгрыш|конкурс)(?![а-яёa-z])",
    r"(?<![а-яёa-z])(?:купить|заказать|забронировать|бронь|билеты?|регистраци[яию]|зарегистр(?:ироваться|ируйтесь)|приходите|участвуйте)(?![а-яёa-z])",
    r"(?:\b\d[\d\s]*(?:₽|руб(?:\.|лей|ля|ль)?\b)|(?:стоимость|цена|оплата|оплатить)\b)",
])
REGEX_NEWS_EVENT_PATTERNS: tuple[re.Pattern[str], ...] = tuple(re.compile(p, re.I) for p in [
    r"(?<![а-яёa-z])(?:происшеств|дтп|авари[яию]|полици[яию]|суд|задержан|штраф|пожар|прокуратур|следств|уголовн|сообщили|официально|губернатор|администраци[яию]|анонс|афиша|состоится)(?![а-яёa-z])",
])
REGEX_SUBSTANCE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(re.compile(p, re.I) for p in [
    r"(?<![а-яёa-z])(?:побывал[аи]?|посетил[аи]?|ездил[аи]?|поехал[аи]?|приехал[аи]?|гулял[аи]?|увидел[аи]?|запомнил(?:ось|ась|ся)?|впечатлен(?:и[ея])?|маршрут|добраться|совет|что\s+посмотреть|красив|атмосфер|удивител|особенно|неожиданно)(?![а-яёa-z])",
])


def _regex_hit_labels(text: str, patterns: tuple[re.Pattern[str], ...], *, max_labels: int = 8) -> list[str]:
    labels: list[str] = []
    for pattern in patterns:
        match = pattern.search(text or "")
        if match:
            labels.append(match.group(0)[:80])
            if len(labels) >= max_labels:
                break
    return labels


def _regex_ko_diagnostic(text: str) -> dict[str, Any]:
    ko_hits = _regex_hit_labels(text, REGEX_KO_PATTERNS)
    external_hits = _regex_hit_labels(text, REGEX_EXTERNAL_GEO_PATTERNS)
    multiregion_hits = _regex_hit_labels(text, REGEX_MULTI_REGION_PATTERNS)
    ad_hits = _regex_hit_labels(text, REGEX_AD_PROMO_PATTERNS)
    news_hits = _regex_hit_labels(text, REGEX_NEWS_EVENT_PATTERNS)
    substance_hits = _regex_hit_labels(text, REGEX_SUBSTANCE_PATTERNS)
    raw_ko = bool(ko_hits)
    multiregion = bool(multiregion_hits or external_hits)
    filtered_ko = raw_ko and not multiregion and not ad_hits and not news_hits and bool(substance_hits)
    return {
        "regex_ko_raw": raw_ko,
        "regex_ko_filtered": filtered_ko,
        "regex_has_external_geo": bool(external_hits),
        "regex_is_multi_region": multiregion,
        "regex_is_ad_or_promo": bool(ad_hits),
        "regex_is_news_or_event": bool(news_hits),
        "regex_has_substance": bool(substance_hits),
        "regex_ko_hits": ko_hits,
        "regex_external_hits": external_hits,
        "regex_multiregion_hits": multiregion_hits,
        "regex_filter_hits": {"ad": ad_hits, "news_event": news_hits, "substance": substance_hits},
    }


def _post_merge_key(row: dict[str, Any]) -> str:
    post_url = str(row.get("post_url") or "").strip()
    if post_url:
        return "url:" + post_url
    post_id = str(row.get("post_id") or row.get("candidate_memory_id") or row.get("publication_candidate_id") or "").strip()
    if post_id:
        return "id:" + post_id
    return str(row.get("_ydb_pk") or "").strip()


def _latest_processed_post_metrics(
    rows: list[dict[str, Any]],
    run_id: str,
) -> tuple[dict[str, int], list[dict[str, Any]], set[str]]:
    """Separate newly discovered posts from refreshes of durable rows.

    ``run_id``/``last_seen_run_id`` means a row was touched by the notebook;
    it does not mean that the post first entered YDB in that run. Reporting the
    touched count as new work overstates product throughput whenever a known
    source is rescanned. ``first_seen_run_id`` owns that distinction.
    """
    latest_rows = [
        row for row in rows
        if run_id and run_id in {
            str(row.get("run_id") or ""),
            str(row.get("last_seen_run_id") or ""),
            str(row.get("current_run_id") or ""),
        }
    ]
    latest_keys = {_post_merge_key(row) for row in latest_rows if _post_merge_key(row)}
    new_keys = {
        _post_merge_key(row) for row in latest_rows
        if _post_merge_key(row) and str(row.get("first_seen_run_id") or "") == run_id
    }
    return ({
        "processed_post_rows_latest_candidate_run_total": len(latest_rows),
        "processed_posts_unique_latest_candidate_run_total": len(latest_keys),
        "processed_posts_new_latest_candidate_run_total": len(new_keys),
        "processed_posts_reprocessed_latest_candidate_run_total": len(latest_keys - new_keys),
        "processed_post_duplicate_identity_rows_latest_candidate_run_total": max(0, len(latest_rows) - len(latest_keys)),
    }, latest_rows, latest_keys)


def _ko_scope_conversion_metrics(rows: list[dict[str, Any]]) -> dict[str, int | float]:
    """Measure unique processed-post yield into the pre-content KO scope gate.

    ``kaliningrad_oblast_only_scope`` is the canonical geographic decision
    before ad/news/substance/media/Gemini filtering. It is intentionally
    stricter than a raw toponym hit because other-region, multiregion and
    comparison-only mentions have already been excluded. Evaluation coverage
    is reported separately so a low end-to-end yield cannot hide that many
    historical processed rows never reached the current vector/scope contract.
    """
    by_key: dict[str, dict[str, bool]] = {}
    for row in rows:
        key = _post_merge_key(row)
        if not key:
            continue
        state = by_key.setdefault(key, {"evaluated": False, "ko_scope": False})
        if str(row.get("vector_gate_status") or "").strip():
            state["evaluated"] = True
        if str(row.get("kaliningrad_oblast_only_scope") or "").strip().lower() in {"1", "true", "yes"}:
            state["ko_scope"] = True

    processed_total = len(by_key)
    evaluated_total = sum(1 for state in by_key.values() if state["evaluated"])
    ko_scope_total = sum(1 for state in by_key.values() if state["ko_scope"])
    return {
        "ko_scope_detected_posts_unique_total": ko_scope_total,
        "ko_scope_evaluated_posts_unique_total": evaluated_total,
        "processed_to_ko_scope_conversion_percent": round(
            (ko_scope_total / processed_total) * 100, 2
        ) if processed_total else 0.0,
        "processed_to_ko_scope_detected_per_1000": round(
            (ko_scope_total / processed_total) * 1000, 1
        ) if processed_total else 0.0,
        "ko_scope_evaluation_coverage_percent": round(
            (evaluated_total / processed_total) * 100, 2
        ) if processed_total else 0.0,
        "evaluated_to_ko_scope_conversion_percent": round(
            (ko_scope_total / evaluated_total) * 100, 2
        ) if evaluated_total else 0.0,
    }


def _row_text_for_regex(row: dict[str, Any]) -> str:
    parts: list[str] = []
    for field in ["text", "full_text", "text_excerpt", "short_summary", "why_this_is_about_kaliningrad", "why_keep_in_memory", "publication_story_reason", "model_short_explanation"]:
        value = str(row.get(field) or "").strip()
        if value and value not in parts:
            parts.append(value)
    return "\n".join(parts)


def _merge_post_rows_for_diagnostics(*row_lists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for rows in row_lists:
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = _post_merge_key(row)
            if not key:
                continue
            current = dict(merged.get(key) or {})
            for k, v in row.items():
                if v in (None, "", [], {}):
                    continue
                if k not in current or current.get(k) in (None, "", [], {}):
                    current[k] = v
                elif k in {"vector_gate_status", "text_region_confirmation_status", "kaliningrad_oblast_only_scope"}:
                    # Prefer downstream enriched decisions over raw post rows.
                    current[k] = v
            merged[key] = current
    return list(merged.values())


def _is_vector_ko_candidate(row: dict[str, Any]) -> bool:
    status = str(row.get("vector_gate_status") or "")
    if status == "vector_accept_candidate":
        return True
    return bool(
        str(row.get("text_region_confirmation_status") or "") == "text_confirmed_ko_only_for_image_analysis"
        and str(row.get("kaliningrad_oblast_only_scope") or "").lower() in {"1", "true", "yes"}
    )


def _regex_vector_comparison_metrics(rows: list[dict[str, Any]]) -> dict[str, int]:
    rows_with_text: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        text = _row_text_for_regex(row)
        if not text.strip():
            continue
        rows_with_text.append((row, _regex_ko_diagnostic(text)))
    regex_raw_pairs = [(r, d) for r, d in rows_with_text if d["regex_ko_raw"]]
    regex_raw = [r for r, _d in regex_raw_pairs]
    regex_filtered = [r for r, d in regex_raw_pairs if d["regex_ko_filtered"]]
    regex_external = [r for r, d in regex_raw_pairs if d["regex_has_external_geo"]]
    regex_multi = [r for r, d in regex_raw_pairs if d["regex_is_multi_region"]]
    regex_ad = [r for r, d in regex_raw_pairs if d["regex_is_ad_or_promo"]]
    regex_news = [r for r, d in regex_raw_pairs if d["regex_is_news_or_event"]]
    regex_thin = [r for r, d in regex_raw_pairs if not d["regex_has_substance"]]
    vector_ko = [r for r in rows if _is_vector_ko_candidate(r)]
    vector_keys = {_post_merge_key(r) for r in vector_ko if _post_merge_key(r)}
    regex_filtered_keys = {_post_merge_key(r) for r in regex_filtered if _post_merge_key(r)}
    return {
        "regex_diagnostic_posts_with_text_total": len(rows_with_text),
        "regex_ko_raw_posts_total": len(regex_raw),
        "regex_ko_filtered_posts_total": len(regex_filtered),
        "regex_ko_external_geo_filtered_posts_total": len(regex_external),
        "regex_ko_multiregion_filtered_posts_total": len(regex_multi),
        "regex_ko_ad_filtered_posts_total": len(regex_ad),
        "regex_ko_news_event_filtered_posts_total": len(regex_news),
        "regex_ko_low_substance_filtered_posts_total": len(regex_thin),
        "vector_ko_candidate_posts_total": len(vector_ko),
        "regex_filtered_without_vector_posts_total": len(regex_filtered_keys - vector_keys),
        "vector_without_regex_filtered_posts_total": len(vector_keys - regex_filtered_keys),
        "regex_to_vector_filtered_ratio_percent": int(round((len(regex_filtered_keys) / len(vector_keys)) * 100)) if vector_keys else 0,
    }


def _heuristic_ko_outcome(row: dict[str, Any], diagnostic: dict[str, Any], source: dict[str, Any]) -> str:
    """Assign one final, mutually exclusive product outcome to a lexical KO hit."""
    source_status = str(source.get("source_queue_status") or source.get("fetch_status") or "")
    source_scope = str(source.get("source_scope") or row.get("source_scope") or "")
    source_geo = str(source.get("source_geo_class") or row.get("source_geo_class") or "")
    if source_status == "rejected_local_region_source" or source_scope == "local_region" or source_geo == "kaliningrad_local":
        return "source_local"
    if source_status == "rejected_spam_source" or source_scope == "spam":
        return "source_spam"

    rejection = str(row.get("rejection_reason") or row.get("memory_product_exclusion_reason") or "")
    if rejection == "reject_stale_or_missing_date" or str(row.get("fresh_enough") or "").lower() in {"0", "false", "no"}:
        return "stale"

    publication_status = str(row.get("publication_status") or "")
    publication_candidate_status = str(row.get("publication_candidate_status") or "")
    if publication_candidate_status == "sent_to_chat" or str(row.get("sent_to_chat") or "").lower() == "true":
        return "publication_sent"
    if publication_candidate_status in {"llm_confirmed", "accepted_for_publication"} or publication_status == "gemini_accept":
        return "publication_confirmed"
    if publication_candidate_status in {"llm_rejected", "tombstoned_reject", "rejected_local_source_after_operator_audit"} or publication_status in {"gemini_reject", "eligibility_reject_tombstone", "operator_rejected_local_source"}:
        return "publication_rejected"

    vector_status = str(row.get("vector_gate_status") or "")
    vector_reason = {
        "vector_reject_not_kaliningrad_oblast": "vector_not_ko",
        "vector_reject_multi_region_roundup": "vector_multi_region",
        "vector_reject_roundup": "vector_multi_region",
        "vector_reject_ad_promo": "vector_ad_promo",
        "vector_reject_news_event": "vector_news_event",
        "vector_reject_low_substance": "vector_low_substance",
    }.get(vector_status)
    if vector_reason:
        return vector_reason
    if vector_status.startswith("vector_defer") or str(row.get("current_stage") or "") == "dual_model_vector_enrichment_pending":
        return "dual_vector_pending"

    text_accepted = bool(
        vector_status == "vector_accept_candidate"
        or str(row.get("text_region_confirmation_status") or "") == "text_confirmed_ko_only_for_image_analysis"
    )
    image_status = str(row.get("image_queue_status") or "")
    current_stage = str(row.get("current_stage") or "")
    if text_accepted:
        if str(row.get("media_review_mode") or "") == "operator_video_review" or _is_video_manual_review_row(row):
            return "video_manual_review"
        if image_status in {"not_reviewable_no_media"}:
            return "no_media"
        if image_status in {"not_reviewable_unsupported_media"}:
            return "unsupported_media"
        if current_stage == "image_fetch_retry_needed" or image_status in {"needs_actual_image_fetch", "selected_for_next_image_batch", "image_analysis_in_progress"}:
            return "image_pending_or_fetch"
        if current_stage == "good_text_weak_media" or image_status in {"rejected_low_score", "rejected_image_quality"}:
            return "weak_image"
        if image_status == "actual_scored":
            return "image_scored_waiting_finalization"
        return "text_accepted_waiting_downstream"

    if diagnostic.get("regex_is_multi_region") or diagnostic.get("regex_has_external_geo"):
        return "heuristic_multi_region"
    if diagnostic.get("regex_is_ad_or_promo"):
        return "heuristic_ad_promo"
    if diagnostic.get("regex_is_news_or_event"):
        return "heuristic_news_event"
    if not diagnostic.get("regex_has_substance"):
        return "heuristic_low_substance"
    return "unclassified_pending"


def _heuristic_ko_funnel_metrics(
    rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    *,
    latest_candidate_run_id: str = "",
    latest_processed_post_keys: set[str] | None = None,
) -> dict[str, Any]:
    source_by_key = {_source_merge_key(row): row for row in source_rows if _source_merge_key(row)}
    heuristic_rows: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    for row in rows:
        text = _row_text_for_regex(row)
        if text:
            diagnostic = _regex_ko_diagnostic(text)
        elif str(row.get("regex_ko_raw") or "").lower() in {"1", "true", "yes"}:
            diagnostic = {
                "regex_ko_raw": True,
                "regex_ko_filtered": str(row.get("regex_ko_filtered") or "").lower() in {"1", "true", "yes"},
                "regex_has_substance": str(row.get("regex_ko_has_substance") or "").lower() in {"1", "true", "yes"},
                "regex_is_ad_or_promo": str(row.get("regex_ko_is_ad_or_promo") or "").lower() in {"1", "true", "yes"},
                "regex_is_news_or_event": str(row.get("regex_ko_is_news_or_event") or "").lower() in {"1", "true", "yes"},
                "regex_is_multi_region": str(row.get("regex_ko_is_multi_region") or "").lower() in {"1", "true", "yes"},
                "regex_has_external_geo": bool(str(row.get("external_geo_mentions") or row.get("mentioned_external_regions") or "").strip()),
            }
        else:
            continue
        if not diagnostic.get("regex_ko_raw"):
            continue
        source = source_by_key.get(_post_source_merge_key(row), {})
        heuristic_rows.append((row, diagnostic, _heuristic_ko_outcome(row, diagnostic, source)))

    def belongs_to_run(row: dict[str, Any]) -> bool:
        if not latest_candidate_run_id:
            return False
        # Candidate/image/publication projections may be reconciled and
        # stamped with the current run even when the post itself was not read
        # or rescored.  Latest-run product conversion must therefore be owned
        # by the authoritative processed-post ledger, not by a downstream
        # overlay timestamp.
        if latest_processed_post_keys is not None:
            key = _post_merge_key(row)
            return bool(key and key in latest_processed_post_keys)
        return latest_candidate_run_id in {
            str(row.get("run_id") or ""),
            str(row.get("last_seen_run_id") or ""),
            str(row.get("current_run_id") or ""),
        }

    latest_rows = [item for item in heuristic_rows if belongs_to_run(item[0])]

    def summarize(items: list[tuple[dict[str, Any], dict[str, Any], str]], prefix: str) -> dict[str, Any]:
        reason_counts: dict[str, int] = {}
        for _row, _diagnostic, reason in items:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        total = len(items)
        text_accepted = sum(
            1 for row, _diagnostic, _reason in items
            if _is_vector_ko_candidate(row)
        )
        return {
            f"{prefix}_raw_posts_total": total,
            f"{prefix}_vector_evaluated_total": sum(1 for row, _diagnostic, _reason in items if str(row.get("vector_gate_status") or "") and not str(row.get("vector_gate_status") or "").startswith("vector_defer")),
            f"{prefix}_dual_vector_pending_total": reason_counts.get("dual_vector_pending", 0),
            f"{prefix}_text_accepted_total": text_accepted,
            f"{prefix}_image_queue_total": sum(1 for row, _diagnostic, _reason in items if str(row.get("image_queue_status") or "")),
            f"{prefix}_publication_total": sum(1 for row, _diagnostic, _reason in items if str(row.get("publication_status") or row.get("publication_candidate_status") or "")),
            f"{prefix}_sent_total": reason_counts.get("publication_sent", 0),
            f"{prefix}_outcome_counts": dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            f"{prefix}_classified_total": sum(reason_counts.values()),
            f"{prefix}_classification_coverage_percent": int(round((sum(reason_counts.values()) / total) * 100)) if total else 0,
        }

    return {
        **summarize(heuristic_rows, "heuristic_ko"),
        **summarize(latest_rows, "heuristic_ko_latest_run"),
        "heuristic_ko_latest_run_id": latest_candidate_run_id,
    }


def _image_queue_status_metrics(images: list[dict[str, Any]]) -> dict[str, int]:
    terminal_statuses = {
        "actual_scored", "not_reviewable_no_media", "not_reviewable_unsupported_media",
        "rejected_text_gate", "rejected_publication_eligibility", "rejected_low_score",
        "rejected_image_quality", "broken_media",
    }
    return {
        # This is a durable historical ledger, not an active queue.
        "image_ledger_rows_total": len(images),
        "image_ledger_terminal_rows_total": sum(
            1 for r in images if str(r.get("image_queue_status") or "") in terminal_statuses
        ),
        "image_not_reviewable_no_media_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "not_reviewable_no_media"),
        "image_not_reviewable_unsupported_media_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "not_reviewable_unsupported_media"),
        "image_rejected_text_gate_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "rejected_text_gate"),
        "image_deferred_text_gate_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "deferred_text_gate"),
    }


def _latest_rows_by_post_url(rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], int]:
    latest: dict[str, dict[str, Any]] = {}
    missing_url = 0
    for row in rows:
        url = _canonical_post_url(row)
        if not url:
            missing_url += 1
            continue
        previous = latest.get(url)
        if previous is None or str(row.get("updated_at") or row.get("_ydb_updated_at") or row.get("created_at") or "") >= str(previous.get("updated_at") or previous.get("_ydb_updated_at") or previous.get("created_at") or ""):
            latest[url] = row
    return latest, missing_url


def _publication_handoff_metrics(
    images: list[dict[str, Any]],
    publications: list[dict[str, Any]],
    source_rows: list[dict[str, Any]] | None = None,
    candidate_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if source_rows is not None:
        attach_live_source_fingerprints(publications, source_rows)
    actual_rows = [
        row for row in images
        if str(row.get("image_queue_status") or "") == "actual_scored"
        and str(row.get("image_model_input_type") or "") == "actual_image"
    ]
    actual_by_url, actual_missing_url = _latest_rows_by_post_url(actual_rows)
    video_rows = [row for row in images if _is_video_manual_review_row(row)]
    video_by_url, video_missing_url = _latest_rows_by_post_url(video_rows)
    finalizer_input_by_url = {**actual_by_url, **video_by_url}
    publication_by_url, publication_missing_url = _latest_rows_by_post_url(publications)
    candidate_by_url, _candidate_missing_url = _latest_rows_by_post_url(candidate_rows or [])
    now = datetime.now(timezone.utc)
    source_by_key = {
        canonical_source_key_for_row(row): row
        for row in (source_rows or [])
        if canonical_source_key_for_row(row)
    }

    def needs_source_evidence(row: dict[str, Any]) -> bool:
        try:
            visual = float(row.get("overall_media_score") or row.get("final_visual_score") or 0)
            postcard = float(row.get("postcardness_score") or 0)
        except (TypeError, ValueError):
            return False
        if visual < float(os.getenv("REGION_TALK_PUBLICATION_MIN_OVERALL_MEDIA_SCORE") or "0.66"):
            return False
        if postcard < float(os.getenv("REGION_TALK_PUBLICATION_MIN_POSTCARDNESS_SCORE") or "0.55"):
            return False
        preimage_decision = str(row.get("publication_eligibility_decision") or "").lower()
        if preimage_decision and preimage_decision not in {"accept", "eligible", "allow", "allowed", "pass"}:
            return False
        source = source_by_key.get(canonical_source_key_for_row(row))
        if not source:
            return False
        scope = str(source.get("source_scope") or "").lower().replace("-", "_")
        geo = str(source.get("source_geo_class") or "").lower().replace("-", "_")
        status = str(source.get("source_queue_status") or source.get("fetch_status") or "").lower()
        quick = str(source.get("source_quick_class") or "").lower()
        external = {"external", "nonlocal", "nonlocal_russia", "confirmed_nonlocal", "mixed_external", "nonlocal_mixed", "external_mixed"}
        if scope in external or geo in external:
            return False
        if any(marker in status for marker in ("local_region", "spam_source")) or quick in {"local_region_source", "spam_source_reject"}:
            return False
        try:
            scanned = int(float(source.get("posts_scanned") or 0))
        except (TypeError, ValueError):
            scanned = 0
        return scanned < max(1, int(os.getenv("REGION_TALK_PUBLICATION_SOURCE_MIN_SCANNED_POSTS") or "5"))

    def publication_is_terminal_non_candidate(url: str) -> bool:
        publication = publication_by_url.get(url) or {}
        candidate_status = str(publication.get("publication_candidate_status") or "")
        publication_status = str(publication.get("publication_status") or "")
        return (
            candidate_status in {"llm_rejected", "llm_needs_review", "filtered_before_llm", "revoked"}
            or candidate_status.startswith(("tombstoned", "revoked"))
            or publication_status in {"gemini_reject", "gemini_needs_review", "eligibility_revoked"}
            or publication_status.startswith("eligibility_")
        )

    source_evidence_urls = sorted(
        url for url, row in actual_by_url.items()
        if needs_source_evidence(row) and not publication_is_terminal_non_candidate(url)
    )

    def restored_text_is_ready(url: str) -> bool:
        memory = candidate_by_url.get(url) or {}
        return bool(str(memory.get("full_text") or memory.get("text") or "").strip())

    def current_text_gate_is_eligible(url: str) -> bool:
        memory = candidate_by_url.get(url) or {}
        # A missing memory row means the governed restore has not rebuilt the
        # current text projection yet, so keep the request actionable.  Once a
        # current row exists it is authoritative: a stale publication restore
        # marker must not remain an active backlog after dual scoring rejects
        # the post.
        if not memory:
            return True
        if str(memory.get("vector_gate_status") or "").lower() != "vector_accept_candidate":
            return False
        if str(memory.get("text_vector_fusion_status") or "").lower() != "fused_e5_bge_m3":
            return False
        if str(memory.get("kaliningrad_oblast_only_scope") or "").lower() not in {"1", "true", "yes"}:
            return False
        if str(memory.get("is_ad_or_promo") or "").lower() in {"1", "true", "yes"}:
            return False
        if str(memory.get("is_multi_region_roundup") or "").lower() in {"1", "true", "yes"}:
            return False
        return True

    def visual_review_is_now_accepted(url: str) -> bool:
        image = actual_by_url.get(url) or {}
        memory = candidate_by_url.get(url) or {}
        # A visual accept resolves only the visual part of the old review. The
        # current text/source decision remains authoritative; otherwise a
        # stale accepted image snapshot can repeatedly reopen a post that was
        # later rejected as multi-region, advertising or not-KO-only.
        if str(memory.get("vector_gate_status") or "").lower() != "vector_accept_candidate":
            return False
        if str(memory.get("text_vector_fusion_status") or "").lower() != "fused_e5_bge_m3":
            return False
        if str(memory.get("kaliningrad_oblast_only_scope") or "").lower() not in {"1", "true", "yes"}:
            return False
        if str(memory.get("is_ad_or_promo") or "").lower() in {"1", "true", "yes"}:
            return False
        if str(memory.get("is_multi_region_roundup") or "").lower() in {"1", "true", "yes"}:
            return False
        decision = str(image.get("image_quality_decision") or "").lower()
        if decision == "vlm_visual_accept":
            return _image_vlm_verdict_is_current(image)
        return decision == "legacy_auto_accept"

    def needs_finalizer(url: str, row: dict[str, Any] | None) -> bool:
        if not row:
            return True
        candidate_status = str(row.get("publication_candidate_status") or "").lower()
        sent = str(row.get("sent_to_chat") or "").lower() == "true" or candidate_status == "sent_to_chat"
        if sent:
            live_source = source_by_key.get(canonical_source_key_for_row(row)) or {}
            if not live_source:
                return False
            live_fingerprint = authoritative_source_fingerprint(live_source)
            persisted_fingerprint = str(row.get("authoritative_source_fingerprint") or "")
            gate_version = str(row.get("publication_eligibility_gate_version") or "")
            # Delivery is immutable, but its source/eligibility attestation is
            # not. Refresh a stale sent row without another Gemini call so
            # current confirmed metrics neither silently fall nor retain an
            # obsolete local/spam verdict.
            return bool(
                live_fingerprint
                and (
                    live_fingerprint != persisted_fingerprint
                    or gate_version != CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION
                )
            )
        publication_status = str(row.get("publication_status") or "").lower()
        if publication_status == "text_restore_pending" or candidate_status == "awaiting_text_restore":
            # CandidateReport owns the governed Telethon refetch, but once it
            # has restored the exact body into current candidate memory the
            # normal finalizer owns the next operation.  Looking only at the
            # stale publication status leaves a completed restore in a
            # permanent no-action gap.
            return current_text_gate_is_eligible(url) and restored_text_is_ready(url)
        if publication_status == "needs_visual_review" or candidate_status == "visual_review_pending":
            # The publication row is a snapshot of the old image outcome. A
            # later bounded album/VLM pass can resolve that review without
            # changing the publication row itself. Re-enter the finalizer when
            # the current image ledger now carries a valid accept attestation.
            return visual_review_is_now_accepted(url)
        llm_terminal_non_candidate = (
            candidate_status in {"llm_rejected", "llm_needs_review"}
            or publication_status in {"gemini_reject", "gemini_needs_review"}
        )
        # A source-counter/fingerprint refresh cannot change Gemini's existing
        # content verdict. Reverification of a durable LLM terminal row is an
        # explicit operator action (`--reverify-existing`), never orchestrator
        # backlog. This also keeps restored image evidence from resurrecting
        # old rejected/review rows and spending the shared budget again.
        if llm_terminal_non_candidate:
            return False
        gate_version = str(row.get("publication_eligibility_gate_version") or "")
        terminal_non_candidate = (
            candidate_status in {"llm_rejected", "llm_needs_review", "filtered_before_llm", "revoked"}
            or candidate_status.startswith(("tombstoned", "revoked"))
            or publication_status in {"gemini_reject", "gemini_needs_review", "eligibility_revoked"}
            or publication_status.startswith("eligibility_")
        )
        live_source = source_by_key.get(canonical_source_key_for_row(row)) or {}
        live_status = str(live_source.get("source_queue_status") or live_source.get("fetch_status") or "").lower()
        live_scope = str(live_source.get("source_scope") or "").lower().replace("-", "_")
        live_geo = str(live_source.get("source_geo_class") or "").lower().replace("-", "_")
        live_quick = str(live_source.get("source_quick_class") or "").lower()
        live_source_still_terminal = (
            live_status in {"rejected_local_region_source", "rejected_spam_source"}
            or live_scope in {"local", "local_region", "kaliningrad_local"}
            or live_geo in {"local", "local_region", "kaliningrad_local"}
            or live_quick in {"local_region_source", "spam_source_reject"}
        )
        # Source counters legitimately continue to grow after a local/spam
        # tombstone. That changes the audit fingerprint but cannot make the
        # post publishable, so it must not create a phantom finalizer backlog.
        # A gate-version change or a changed source classification still
        # reopens the row for a real eligibility refresh.
        if terminal_non_candidate and live_source_still_terminal and gate_version == CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION:
            return False
        live_fingerprint = str(row.get("_live_authoritative_source_fingerprint") or "")
        persisted_fingerprint = str(row.get("authoritative_source_fingerprint") or "")
        if (
            str(row.get("_live_authoritative_source_found") or "").lower() == "true"
            and live_fingerprint
            and live_fingerprint != persisted_fingerprint
        ):
            return True
        eligibility_verdict = str(row.get("publication_eligibility_verdict") or "").lower()
        if not eligibility_verdict or gate_version != CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION:
            return True
        retryable = publication_status in {"gemini_rate_limited", "gemini_error", "gemini_unknown", "no_text_for_gemini"} or candidate_status in {
            "llm_budget_deferred", "llm_error", "retry_due",
        }
        if not retryable:
            return False
        retry_at = _parse_iso_datetime(row.get("next_attempt_after"))
        return retry_at is None or retry_at <= now

    finalizer_pending_urls = sorted(
        url for url in finalizer_input_by_url
        if needs_finalizer(url, publication_by_url.get(url))
    )
    text_restore_raw_urls = sorted(
        url for url, row in publication_by_url.items()
        if (
            str(row.get("publication_status") or "").lower() == "text_restore_pending"
            or str(row.get("publication_candidate_status") or "").lower() == "awaiting_text_restore"
        )
    )
    text_restore_active_urls = sorted(
        url for url in text_restore_raw_urls
        if current_text_gate_is_eligible(url)
    )
    text_restore_ready_urls = sorted(
        url for url, row in publication_by_url.items()
        if (
            str(row.get("publication_status") or "").lower() == "text_restore_pending"
            or str(row.get("publication_candidate_status") or "").lower() == "awaiting_text_restore"
        )
        and current_text_gate_is_eligible(url)
        and restored_text_is_ready(url)
    )
    visual_review_resolved_urls = sorted(
        url for url, row in publication_by_url.items()
        if (
            str(row.get("publication_status") or "").lower() == "needs_visual_review"
            or str(row.get("publication_candidate_status") or "").lower() == "visual_review_pending"
        )
        and visual_review_is_now_accepted(url)
    )

    confirmed_urls = {url for url, row in publication_by_url.items() if is_confirmed_publication(row)}
    draft_ready_confirmed_urls = {
        url for url in confirmed_urls if is_publication_draft_ready(publication_by_url[url])
    }
    draft_missing_confirmed_urls = confirmed_urls - draft_ready_confirmed_urls
    draft_missing_telegram_urls = {
        url for url in draft_missing_confirmed_urls
        if re.fullmatch(r"https://t\.me/[^/]+/[0-9]+", url, re.I)
    }
    draft_missing_vk_urls = {
        url for url in draft_missing_confirmed_urls
        if re.fullmatch(r"https://vk\.com/wall-?[0-9]+_[0-9]+", url, re.I)
    }
    draft_missing_article_urls = {
        url for url in draft_missing_confirmed_urls
        if str(publication_by_url[url].get("content_origin_type") or "").lower()
        in {"editorial_publication", "academic_publication"}
    }

    def draft_backfill_is_actionable(url: str) -> bool:
        row = publication_by_url[url]
        status = str(row.get("publication_draft_backfill_status") or "").lower()
        if (
            str(row.get("publication_draft_backfill_version") or "")
            == CURRENT_PUBLICATION_DRAFT_BACKFILL_VERSION
            and status in {
            "ready", "llm_not_accepted", "needs_grounding_review",
            "source_text_unavailable", "unsupported_surface",
            }
        ):
            # A v3 row marked ready can still be invalidated by a newer
            # deterministic final-copy guard. It is already in
            # draft_missing_confirmed_urls, so schedule one corrective pass.
            return status == "ready"
        retry_at = _parse_iso_datetime(row.get("publication_draft_backfill_next_attempt_after"))
        return retry_at is None or retry_at <= now

    draft_backfill_actionable_telegram_urls = {
        url for url in draft_missing_telegram_urls if draft_backfill_is_actionable(url)
    }
    draft_backfill_actionable_vk_urls = {
        url for url in draft_missing_vk_urls if draft_backfill_is_actionable(url)
    }
    draft_backfill_actionable_article_urls = {
        url for url in draft_missing_article_urls if draft_backfill_is_actionable(url)
    }
    draft_backfill_actionable_urls = (
        draft_backfill_actionable_telegram_urls | draft_backfill_actionable_vk_urls
        | draft_backfill_actionable_article_urls
    )
    unsent_confirmed_urls = {
        url for url in draft_ready_confirmed_urls
        if is_unsent_confirmed_publication(publication_by_url[url])
    }
    sent_urls = {
        url for url, row in publication_by_url.items()
        if str(row.get("sent_to_chat") or "").lower() == "true"
        or str(row.get("publication_candidate_status") or "") == "sent_to_chat"
    }
    status_by_url = {url: str(row.get("publication_candidate_status") or "") for url, row in publication_by_url.items()}
    active_candidate_urls = set(unsent_confirmed_urls)
    for url, row in publication_by_url.items():
        candidate_status = str(row.get("publication_candidate_status") or "").lower()
        publication_status = str(row.get("publication_status") or "").lower()
        if candidate_status in {"ready_for_llm", "visual_review_pending", "llm_needs_review", "llm_budget_deferred", "llm_error", "retry_due"}:
            active_candidate_urls.add(url)
        elif candidate_status == "awaiting_text_restore" and current_text_gate_is_eligible(url):
            active_candidate_urls.add(url)
        elif publication_status in {"gemini_rate_limited", "gemini_error", "gemini_unknown"}:
            active_candidate_urls.add(url)
    image_product_ready_urls = {
        url for url, row in actual_by_url.items()
        if str(row.get("image_publication_ready") or "").lower() == "true"
    }
    return {
        "image_actual_scored_urls_total": len(actual_by_url),
        "image_actual_scored_missing_url_total": actual_missing_url,
        "video_manual_review_candidate_urls_total": len(video_by_url),
        "video_manual_review_missing_url_total": video_missing_url,
        "publication_finalizer_input_urls_total": len(finalizer_input_by_url),
        "image_publication_ready_urls_total": len(image_product_ready_urls),
        "publication_candidate_rows_total": len(publications),
        "publication_candidate_total": len(publication_by_url),
        "publication_active_candidate_total": len(active_candidate_urls),
        "publication_candidate_missing_url_total": publication_missing_url,
        # Publication-ready is the final, unsent verifier-accepted taxonomy. An
        # image-ready or ready_for_llm row is not publication-ready yet.
        "publication_ready_total": len(unsent_confirmed_urls),
        "publication_confirmed_total": len(confirmed_urls),
        "publication_draft_ready_confirmed_total": len(draft_ready_confirmed_urls),
        "publication_draft_missing_confirmed_total": len(draft_missing_confirmed_urls),
        "publication_draft_missing_telegram_total": len(draft_missing_telegram_urls),
        "publication_draft_missing_vk_total": len(draft_missing_vk_urls),
        "publication_draft_missing_article_total": len(draft_missing_article_urls),
        "publication_draft_backfill_actionable_total": len(draft_backfill_actionable_urls),
        "publication_draft_backfill_actionable_telegram_total": len(draft_backfill_actionable_telegram_urls),
        "publication_draft_backfill_actionable_vk_total": len(draft_backfill_actionable_vk_urls),
        "publication_draft_backfill_actionable_article_total": len(draft_backfill_actionable_article_urls),
        "publication_draft_backfill_actionable_urls": sorted(draft_backfill_actionable_urls),
        "publication_sent_total": len(sent_urls),
        "publication_unsent_confirmed_total": len(unsent_confirmed_urls),
        "publication_verifier_pending_total": sum(1 for status in status_by_url.values() if status == "ready_for_llm"),
        "publication_visual_review_pending_total": sum(1 for status in status_by_url.values() if status == "visual_review_pending"),
        "publication_text_restore_pending_raw_total": len(text_restore_raw_urls),
        "publication_text_restore_pending_total": len(text_restore_active_urls),
        "publication_text_restore_tombstoned_total": max(0, len(text_restore_raw_urls) - len(text_restore_active_urls)),
        "publication_text_restore_ready_for_finalizer_total": len(text_restore_ready_urls),
        "publication_text_restore_ready_for_finalizer_urls": text_restore_ready_urls,
        "publication_visual_review_resolved_ready_for_finalizer_total": len(visual_review_resolved_urls),
        "publication_visual_review_resolved_ready_for_finalizer_urls": visual_review_resolved_urls,
        "publication_review_or_retry_total": sum(1 for status in status_by_url.values() if status in {"visual_review_pending", "llm_needs_review", "llm_budget_deferred", "llm_error"}),
        "publication_rejected_total": sum(1 for status in status_by_url.values() if status in {"filtered_before_llm", "llm_rejected"}),
        "publication_source_evidence_backlog_total": len(source_evidence_urls),
        "publication_source_evidence_backlog_urls": source_evidence_urls,
        "finalizer_pending_url_total": len(finalizer_pending_urls),
        "finalizer_pending_urls": finalizer_pending_urls,
    }


def _image_review_lifecycle_metrics(
    images: list[dict[str, Any]],
    publications: list[dict[str, Any]],
) -> dict[str, int]:
    """Separate immutable image-ledger history from current review work."""
    raw_review_rows = [
        row for row in images
        if str(row.get("image_quality_decision") or "").lower() == "needs_visual_review"
    ]
    partial_rows = [
        row for row in images
        if str(row.get("image_acquisition_status") or "").lower() == "partial"
    ]
    publication_by_url, _ = _latest_rows_by_post_url(publications)
    review_by_url, _ = _latest_rows_by_post_url(raw_review_rows)
    partial_by_url, _ = _latest_rows_by_post_url(partial_rows)
    active_review_urls = {
        url for url, row in publication_by_url.items()
        if str(row.get("publication_candidate_status") or "").lower() == "visual_review_pending"
    }
    active_image_review_urls = set(review_by_url) & active_review_urls
    contradiction_urls: set[str] = set()
    for url, row in publication_by_url.items():
        candidate_status = str(row.get("publication_candidate_status") or "").lower()
        decision = str(row.get("llm_decision") or row.get("publication_llm_decision") or "").lower()
        sent = str(row.get("sent_to_chat") or "").lower() == "true" or candidate_status == "sent_to_chat"
        if candidate_status == "awaiting_text_restore" and (decision in {"accept", "reject"} or sent):
            contradiction_urls.add(url)
        elif candidate_status == "visual_review_pending" and (decision == "reject" or sent):
            contradiction_urls.add(url)
    return {
        "image_visual_review_raw_urls_total": len(review_by_url),
        "image_visual_review_active_total": len(active_image_review_urls),
        "image_visual_review_tombstoned_total": len(set(review_by_url) - active_image_review_urls),
        "image_partial_album_active_total": len(set(partial_by_url) & active_review_urls),
        "publication_lifecycle_contradiction_total": len(contradiction_urls),
    }


def _is_keyword_discovered_source(row: dict[str, Any]) -> bool:
    haystack = " ".join(str(row.get(k) or "") for k in [
        "added_from", "insertion_policy", "discovery_type", "edge_type", "frontier_reason",
        "matched_query", "matched_hashtag", "keyword_hit_post_url", "keyword_evidence_excerpt",
    ]).lower()
    return "keyword" in haystack or "hashtag" in haystack or "telegram_keyword_search" in haystack


def _is_similar_discovered_source(row: dict[str, Any]) -> bool:
    haystack = " ".join(str(row.get(k) or "") for k in [
        "added_from", "insertion_policy", "discovery_type", "edge_type", "frontier_reason",
        "recommendation_source_channel_url", "similarity_seed_source_id",
    ]).lower()
    return "similar" in haystack or "recommendation" in haystack or "telegram_similar_channel" in haystack


def _latest_discovery_run_metrics(
    rows: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    *,
    prefix: str,
    predicate: Any,
) -> dict[str, Any]:
    relevant_rows = [r for r in rows if predicate(r)]
    relevant_edges = [r for r in edges if predicate(r)]
    run_ids = [
        str(r.get("run_id") or "").strip()
        for r in [*relevant_rows, *relevant_edges]
        if str(r.get("run_id") or "").strip()
    ]
    latest = max(run_ids) if run_ids else ""
    latest_rows = [r for r in relevant_rows if latest and str(r.get("run_id") or "") == latest]
    latest_edges = [r for r in relevant_edges if latest and str(r.get("run_id") or "") == latest]
    latest_keys = {
        _source_merge_key(r) or str(r.get("source_candidate_id") or r.get("to_source_candidate_id") or "")
        for r in latest_rows
        if (_source_merge_key(r) or str(r.get("source_candidate_id") or r.get("to_source_candidate_id") or ""))
    }
    latest_keys.update(
        str(r.get("to_source_candidate_id") or r.get("source_candidate_id") or "").strip()
        for r in latest_edges
        if str(r.get("to_source_candidate_id") or r.get("source_candidate_id") or "").strip()
    )
    return {
        f"{prefix}_latest_run_id": latest,
        f"{prefix}_latest_run_sources_total": len(latest_keys),
        f"{prefix}_latest_run_rows_total": len(latest_rows),
        f"{prefix}_latest_run_edges_total": len(latest_edges),
    }


def _source_has_ko_candidate(row: dict[str, Any]) -> bool:
    status = str(row.get("source_queue_status") or row.get("queue_status") or row.get("fetch_status") or "")
    return bool(
        status in {"processed_found_ko_candidate", "processed_found_ko_low_image_quality"}
        or _safe_int(row.get("ko_posts_found")) > 0
        or _safe_int(row.get("candidate_posts_found")) > 0
    )


def _source_has_scan_evidence(row: dict[str, Any]) -> bool:
    if str(row.get("source_history_scan_ever_completed") or "").strip().lower() in {"1", "true", "yes"}:
        return True
    explicit_history = bool(
        str(row.get("last_history_fetch_at") or "").strip()
        or str(row.get("primary_scan_completed_at") or "").strip()
        or str(row.get("last_successful_delta_scan_at") or "").strip()
    )
    if explicit_history:
        return True
    if _source_is_post_probe_only(row):
        return False
    return bool(
        max(
            _safe_int(row.get("posts_scanned")),
            _safe_int(row.get("source_history_posts_scanned_max")),
        ) > 0
    )


def _source_is_post_probe_only(row: dict[str, Any]) -> bool:
    """Separate exact-post/fast-check evidence from source-history work."""
    mode = str(row.get("history_fetch_mode") or "").strip().lower()
    if mode in {
        "exact_post_link_fetch",
        "ydb_candidate_links_only_no_discovery",
        "history_disabled_discovery_only",
    }:
        return True
    context = " ".join(
        str(row.get(field) or "").strip().lower()
        for field in ("discovery_type", "edge_type", "online_update_stage", "source_seed_id")
    )
    return bool(
        "source_local_fast_check" in context
        or "post_link_queue_exact_fetch" in context
        or "ydb_candidate_links" in context
    )


def _queue_cursor_short_name(name: str) -> str:
    clean = str(name or "").replace("queue_cursor:", "").strip()
    if clean in {"source", "unified_source_queue", "canonical_source_queue"}:
        return "unified_source_queue"
    if clean in {"image", "image_candidate_queue"}:
        return "image_candidate_queue"
    return clean


def _is_canonical_cursor_row(row: dict[str, Any], name: str) -> bool:
    pk_tail = str(row.get("_ydb_pk") or "").replace("queue_cursor:", "")
    if ":" in pk_tail:
        return False
    return _queue_cursor_short_name(pk_tail) == _queue_cursor_short_name(name)


def _cursor_row_is_better(current: dict[str, Any] | None, candidate: dict[str, Any], name: str) -> bool:
    if not current:
        return True
    current_canonical = _is_canonical_cursor_row(current, name)
    candidate_canonical = _is_canonical_cursor_row(candidate, name)
    if current_canonical != candidate_canonical:
        return candidate_canonical
    canonical_queue = _queue_cursor_short_name(name) in {"unified_source_queue", "image_candidate_queue"}
    if canonical_queue and current_canonical and candidate_canonical:
        current_pos = _safe_int(current.get("cursor_position") or current.get("done") or 0)
        candidate_pos = _safe_int(candidate.get("cursor_position") or candidate.get("done") or 0)
        if candidate_pos != current_pos:
            return candidate_pos > current_pos
    return str(candidate.get("_ydb_updated_at") or candidate.get("updated_at") or "") >= str(current.get("_ydb_updated_at") or current.get("updated_at") or "")


def _keyword_source_metrics(
    source_rows: list[dict[str, Any]],
    cursor_position: int,
    source_candidates: list[dict[str, Any]] | None = None,
    source_edges: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    source_by_key = {_source_merge_key(r): r for r in source_rows if _source_merge_key(r)}
    keyword_row_keys = {_source_merge_key(r) for r in source_rows if _is_keyword_discovered_source(r) and _source_merge_key(r)}
    keyword_candidate_keys = {
        _source_merge_key(r) for r in (source_candidates or [])
        if _is_keyword_discovered_source(r) and _source_merge_key(r)
    }
    candidate_key_by_id = {
        str(r.get("source_candidate_id") or ""): _source_merge_key(r)
        for r in (source_candidates or [])
        if str(r.get("source_candidate_id") or "") and _source_merge_key(r)
    }
    keyword_edge_target_keys = {
        candidate_key_by_id.get(str(r.get("to_source_candidate_id") or ""))
        for r in (source_edges or [])
        if _is_keyword_discovered_source(r)
    }
    keyword_edge_target_keys = {k for k in keyword_edge_target_keys if k}
    keyword_evidence_keys = keyword_row_keys | keyword_candidate_keys | keyword_edge_target_keys
    keyword_scanned_keys = {
        k for k in keyword_evidence_keys
        if k in source_by_key and (
            _source_has_scan_evidence(source_by_key[k])
        )
    }
    keyword_fake_processed_without_scan_keys = {
        k for k in keyword_evidence_keys
        if k in source_by_key
        and str(source_by_key[k].get("source_queue_status") or "").startswith("processed")
        and not _source_has_scan_evidence(source_by_key[k])
    }
    keyword_ko_keys = {k for k in keyword_evidence_keys if k in source_by_key and _source_has_ko_candidate(source_by_key[k])}
    keyword_pending_after_cursor_keys = {
        k for k in keyword_evidence_keys
        if k in source_by_key
        and int(float(source_by_key[k].get("queue_order") or 0)) > cursor_position
        and str(source_by_key[k].get("source_queue_status") or source_by_key[k].get("queue_status") or "pending_scan") in {"", "pending_scan", "needs_rescan_or_retry"}
    }
    return {
        "publics_keyword_discovered_total": len(keyword_evidence_keys),
        "publics_keyword_queue_rows_total": len(keyword_row_keys),
        "publics_keyword_candidate_rows_total": len(keyword_candidate_keys),
        "publics_keyword_edge_targets_total": len(keyword_edge_target_keys),
        "publics_keyword_queue_missing_total": len(keyword_evidence_keys - set(source_by_key)),
        "publics_keyword_fake_processed_without_scan_evidence_total": len(keyword_fake_processed_without_scan_keys),
        "publics_keyword_scanned_with_posts_total": len(keyword_scanned_keys),
        "publics_keyword_with_ko_candidates_total": len(keyword_ko_keys),
        "publics_keyword_pending_after_cursor_total": len(keyword_pending_after_cursor_keys),
        "publics_keyword_ko_yield_percent": int(round((len(keyword_ko_keys) / len(keyword_scanned_keys)) * 100)) if keyword_scanned_keys else 0,
        # Honest product-grain counters. ``candidate_posts_found`` is a broad
        # lexical/preselection counter and must not be labelled as confirmed
        # KO evidence.
        "keyword_sources_with_preliminary_candidates_total": sum(
            1 for k in keyword_scanned_keys if _safe_int(source_by_key[k].get("candidate_posts_found")) > 0
        ),
        "keyword_sources_with_confirmed_ko_posts_total": sum(
            1 for k in keyword_scanned_keys if _safe_int(source_by_key[k].get("ko_posts_found")) > 0
        ),
        "keyword_external_sources_with_confirmed_ko_posts_total": sum(
            1 for k in keyword_scanned_keys
            if _safe_int(source_by_key[k].get("ko_posts_found")) > 0
            and str(source_by_key[k].get("source_queue_status") or "") == "processed_found_ko_candidate"
        ),
        "keyword_sources_rejected_local_total": sum(
            1 for k in keyword_scanned_keys if str(source_by_key[k].get("source_queue_status") or "") == "rejected_local_region_source"
        ),
        "keyword_sources_rejected_spam_total": sum(
            1 for k in keyword_scanned_keys if str(source_by_key[k].get("source_queue_status") or "") == "rejected_spam_source"
        ),
    }


def _latest_by_post_url(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    ordered = sorted(
        rows,
        key=lambda row: str(row.get("updated_at") or row.get("_ydb_updated_at") or row.get("last_seen_run_id") or ""),
    )
    for row in ordered:
        url = _canonical_post_url(row)
        if not url:
            continue
        # Several historical PKs can represent one URL. Newer compact rows are
        # sometimes sparse, so replacing the entire record loses the earlier
        # explicit freshness/scope/rejection evidence. Merge only present
        # values in timestamp order and keep one unique product row.
        current = dict(out.get(url) or {})
        for key, value in row.items():
            if value not in (None, ""):
                current[key] = value
        out[url] = current
    return out


def _is_video_manual_review_row(row: dict[str, Any]) -> bool:
    if str(row.get("media_kind") or "").lower() == "video":
        return True
    evidence = " ".join(str(row.get(key) or "") for key in [
        "media_fetch_error", "primary_media_path", "image_url_or_local_path", "unsupported_media_path",
    ]).lower()
    return any(suffix in evidence for suffix in (".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"))


def _fast_check_exact_post_metrics(
    source_rows: list[dict[str, Any]],
    processed_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    image_rows: list[dict[str, Any]],
    publication_rows: list[dict[str, Any]],
    vector_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    hit_rows = [row for row in source_rows if str(row.get("fast_check_status") or "") == "ko_hit"]
    urls = {
        _canonical_post_url(str(row.get("fast_check_hit_post_url") or row.get("keyword_hit_post_url") or ""))
        for row in hit_rows
    } - {""}
    processed = _latest_by_post_url(processed_rows)
    candidates = _latest_by_post_url(candidate_rows)
    images = _latest_by_post_url(image_rows)
    publications = _latest_by_post_url(publication_rows)
    vector_models: dict[str, set[str]] = {}
    processed_scope_values: dict[str, list[bool]] = {}
    processed_fresh_values: dict[str, list[bool]] = {}
    for row in processed_rows:
        url = _canonical_post_url(row)
        if not url:
            continue
        if row.get("kaliningrad_oblast_only_scope") not in (None, ""):
            processed_scope_values.setdefault(url, []).append(
                str(row.get("kaliningrad_oblast_only_scope") or "").lower() in {"1", "true", "yes"}
            )
        if row.get("fresh_enough") not in (None, ""):
            processed_fresh_values.setdefault(url, []).append(
                str(row.get("fresh_enough") or "").lower() not in {"0", "false", "no"}
            )
    for row in vector_rows:
        url = _canonical_post_url(row)
        if url:
            vector_models.setdefault(url, set()).add(str(row.get("model_short") or row.get("model_id") or ""))

    fetched_urls = {url for url in urls if url in processed}
    paired_urls = {
        url for url in urls
        if any("e5" in model for model in vector_models.get(url, set()))
        and any("bge" in model for model in vector_models.get(url, set()))
    }
    accepted_urls: set[str] = set()
    semantic_accept_urls: set[str] = set()
    rejection_counts: dict[str, int] = {}
    pending_counts: dict[str, int] = {}
    for url in fetched_urls:
        processed_row = processed.get(url) or {}
        candidate_row = candidates.get(url) or {}
        # The processed-post projection is the current scoring verdict. A
        # candidate-memory row can intentionally outlive that verdict and may
        # still contain an older `vector_accept_candidate`. Let current
        # processed fields override the historical candidate memory; otherwise
        # a row deferred for fresh BGE enrichment is falsely reported as fully
        # accepted.
        row = {**candidate_row, **processed_row}
        source = next((item for item in hit_rows if _canonical_post_url(str(item.get("fast_check_hit_post_url") or item.get("keyword_hit_post_url") or "")) == url), {})
        source_status = str(source.get("source_queue_status") or "")
        vector_status = str(row.get("vector_gate_status") or "")
        fused = str(row.get("text_vector_fusion_status") or "") == "fused_e5_bge_m3" or url in paired_urls
        # The scored post projection owns region/freshness. Candidate-memory
        # duplicates may share one timestamp and retain an older scope value;
        # never let that broaden the strict product denominator.
        scope_values = processed_scope_values.get(url) or []
        fresh_values = processed_fresh_values.get(url) or []
        ko_only = bool(scope_values) and all(scope_values)
        fresh = not fresh_values or all(fresh_values)
        source_ok = source_status not in {"rejected_local_region_source", "rejected_spam_source"}
        semantic_ok = source_ok and vector_status == "vector_accept_candidate" and fused and ko_only
        if semantic_ok:
            semantic_accept_urls.add(url)
        current_stage = str(row.get("current_stage") or "").lower()
        drop_gate = str(row.get("drop_gate") or "").lower()
        rejection = str(row.get("rejection_reason") or row.get("rejection_reason_primary") or "").lower()
        # `drop_gate` is an unfortunately historical field name: it also
        # records downstream media outcomes such as `image_fetch_gate` and
        # `image_postcardness_gate`. Those values prove that the text gate was
        # passed; treating every non-empty value as a text rejection made the
        # product metric report zero while rows waited for image processing.
        terminal_text_drop_gates = {
            "freshness_gate",
            "semantic_vector_gate",
            "region_evidence_safety_gate",
            "llm_semantic_gate",
            "pre_llm_cost_guard",
            "final_llm_verifier",
        }
        downstream_text_pass_stages = {
            "image_fetch_retry_needed",
            "needs_image_review",
            "good_text_weak_media",
            "low_substance_but_region_relevant",
            "semantic_candidate",
            "favorite",
            "image_reviewable",
            "publication_candidate",
            "publication_confirmed",
            "publication_sent",
        }
        terminal_text_drop = (
            current_stage.startswith("dropped_")
            or current_stage in {"debug_reject", "pre_candidate_needs_llm", "needs_llm_retry"}
            or drop_gate in terminal_text_drop_gates
            or rejection.startswith("reject_")
            or vector_status.startswith("vector_reject")
        )
        text_passed_downstream = current_stage in downstream_text_pass_stages
        if semantic_ok and fresh and not terminal_text_drop and text_passed_downstream:
            accepted_urls.add(url)
            continue
        if source_status == "rejected_local_region_source":
            reason = "source_local"
        elif source_status == "rejected_spam_source":
            reason = "source_spam"
        elif not fresh:
            reason = "stale"
        elif vector_status.startswith("vector_reject"):
            reason = vector_status
        elif vector_status.startswith("vector_defer"):
            reason = "dual_vector_pending"
        elif not ko_only:
            reason = "not_confirmed_ko_only"
        elif not fused:
            reason = "dual_vector_not_complete"
        else:
            reason = "other_text_gate"
        target_counts = pending_counts if reason in {"dual_vector_pending", "other_text_gate"} else rejection_counts
        target_counts[reason] = target_counts.get(reason, 0) + 1

    video_urls = {url for url in accepted_urls if _is_video_manual_review_row(images.get(url) or {})}
    return {
        "fast_check_keyword_match_sources_total": len(hit_rows),
        "fast_check_exact_hit_post_urls_total": len(urls),
        "fast_check_exact_posts_processed_unique_total": len(fetched_urls),
        "fast_check_exact_posts_dual_vectorized_total": len(paired_urls),
        "fast_check_exact_posts_dual_semantic_accept_total": len(semantic_accept_urls),
        "fast_check_exact_posts_dual_semantic_accept_urls": sorted(semantic_accept_urls),
        # Compatibility name: this now means the complete publication text
        # gate (fresh + external source + KO-only + dual semantic accept + no
        # terminal text rejection), not merely semantic similarity.
        "fast_check_exact_posts_strict_text_accepted_total": len(accepted_urls),
        "fast_check_exact_posts_strict_text_accepted_urls": sorted(accepted_urls),
        "fast_check_exact_posts_text_rejected_total": sum(rejection_counts.values()),
        "fast_check_exact_posts_text_rejection_reasons": rejection_counts,
        "fast_check_exact_posts_text_pending_total": sum(pending_counts.values()),
        "fast_check_exact_posts_text_pending_reasons": pending_counts,
        "fast_check_exact_posts_image_queue_total": len(urls & set(images)),
        "fast_check_exact_posts_video_manual_review_total": len(video_urls),
        "fast_check_exact_posts_publication_queue_total": len(urls & set(publications)),
    }


def _similar_source_metrics(
    source_rows: list[dict[str, Any]],
    source_candidates: list[dict[str, Any]] | None = None,
    source_edges: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    source_by_key = {_source_merge_key(r): r for r in source_rows if _source_merge_key(r)}
    similar_row_keys = {_source_merge_key(r) for r in source_rows if _is_similar_discovered_source(r) and _source_merge_key(r)}
    candidate_key_by_id = {
        str(r.get("source_candidate_id") or ""): _source_merge_key(r)
        for r in (source_candidates or [])
        if str(r.get("source_candidate_id") or "") and _source_merge_key(r)
    }
    similar_edge_target_keys = {
        candidate_key_by_id.get(str(r.get("to_source_candidate_id") or ""))
        for r in (source_edges or [])
        if _is_similar_discovered_source(r)
    }
    similar_edge_target_keys = {k for k in similar_edge_target_keys if k}
    similar_evidence_keys = similar_row_keys | similar_edge_target_keys
    similar_scanned_keys = {k for k in similar_evidence_keys if k in source_by_key and _source_has_scan_evidence(source_by_key[k])}
    similar_ko_keys = {k for k in similar_evidence_keys if k in source_by_key and _source_has_ko_candidate(source_by_key[k])}
    return {
        "publics_similar_discovered_total": len(similar_evidence_keys),
        "publics_similar_queue_rows_total": len(similar_row_keys),
        "publics_similar_edge_targets_total": len(similar_edge_target_keys),
        "publics_similar_queue_missing_total": len(similar_evidence_keys - set(source_by_key)),
        "publics_similar_scanned_with_posts_total": len(similar_scanned_keys),
        "publics_similar_with_ko_candidates_total": len(similar_ko_keys),
        "publics_similar_ko_yield_percent": int(round((len(similar_ko_keys) / len(similar_scanned_keys)) * 100)) if similar_scanned_keys else 0,
    }


def _keyword_source_post_regex_metrics(
    source_rows: list[dict[str, Any]],
    diagnostic_post_rows: list[dict[str, Any]],
    source_candidates: list[dict[str, Any]] | None = None,
    source_edges: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    source_by_key = {_source_merge_key(r): r for r in source_rows if _source_merge_key(r)}
    keyword_row_keys = {_source_merge_key(r) for r in source_rows if _is_keyword_discovered_source(r) and _source_merge_key(r)}
    candidate_key_by_id = {
        str(r.get("source_candidate_id") or ""): _source_merge_key(r)
        for r in (source_candidates or [])
        if str(r.get("source_candidate_id") or "") and _source_merge_key(r)
    }
    keyword_edge_target_keys = {
        candidate_key_by_id.get(str(r.get("to_source_candidate_id") or ""))
        for r in (source_edges or [])
        if _is_keyword_discovered_source(r)
    }
    keyword_keys = {k for k in (keyword_row_keys | keyword_edge_target_keys) if k}
    keyword_aliases: set[str] = set()
    for key in keyword_keys:
        keyword_aliases.add(key.lower().rstrip("/"))
        row = source_by_key.get(key)
        if row:
            keyword_aliases.update(_source_alias_keys(row))

    keyword_post_rows: list[dict[str, Any]] = []
    keyword_post_keys: set[str] = set()
    source_keys_with_posts: set[str] = set()
    for row in diagnostic_post_rows:
        row_aliases = _source_alias_keys(row)
        matched_aliases = keyword_aliases & row_aliases
        if not matched_aliases:
            continue
        key = _post_merge_key(row)
        if key and key in keyword_post_keys:
            continue
        if key:
            keyword_post_keys.add(key)
        keyword_post_rows.append(row)
        source_keys_with_posts.update(a for a in matched_aliases if a.startswith("telegram:"))

    rows_with_text: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in keyword_post_rows:
        text = _row_text_for_regex(row)
        if not text.strip():
            continue
        rows_with_text.append((row, _regex_ko_diagnostic(text)))
    regex_raw_rows = [r for r, d in rows_with_text if d["regex_ko_raw"]]
    regex_filtered_rows = [r for r, d in rows_with_text if d["regex_ko_filtered"]]
    vector_rows = [r for r in keyword_post_rows if _is_vector_ko_candidate(r)]
    regex_filtered_keys = {_post_merge_key(r) for r in regex_filtered_rows if _post_merge_key(r)}
    vector_keys = {_post_merge_key(r) for r in vector_rows if _post_merge_key(r)}

    def source_alias_set(rows: list[dict[str, Any]]) -> set[str]:
        out: set[str] = set()
        for row in rows:
            out.update(a for a in _source_alias_keys(row) if a.startswith("telegram:") and a in keyword_aliases)
        return out

    return {
        "publics_keyword_post_rows_total": len(keyword_post_rows),
        "publics_keyword_post_rows_with_text_total": len(rows_with_text),
        "publics_keyword_sources_with_post_rows_total": len(source_keys_with_posts),
        "publics_keyword_regex_ko_raw_posts_total": len(regex_raw_rows),
        "publics_keyword_regex_ko_filtered_posts_total": len(regex_filtered_rows),
        "publics_keyword_vector_ko_candidate_posts_total": len(vector_rows),
        "publics_keyword_regex_sources_with_ko_raw_total": len(source_alias_set(regex_raw_rows)),
        "publics_keyword_regex_sources_with_ko_filtered_total": len(source_alias_set(regex_filtered_rows)),
        "publics_keyword_regex_filtered_without_vector_posts_total": len(regex_filtered_keys - vector_keys),
        "publics_keyword_vector_without_regex_filtered_posts_total": len(vector_keys - regex_filtered_keys),
    }


def _script_name(cmd: list[str]) -> str:
    exe = Path(str(cmd[0] if cmd else "")).name
    return str(cmd[1] if len(cmd) > 1 and exe.startswith("python") else cmd[0])


def _supports_arg(cmd: list[str], arg: str) -> bool:
    script = _script_name(cmd)
    if arg == "--env-file":
        return script in {
            "kaggle/execute_region_talk_bge_m3_enrichment.py",
            "kaggle/execute_region_talk_image_diagnostic.py",
            "kaggle/execute_region_talk_candidate_report.py",
            "scripts/region_talk_publication_finalizer.py",
            "scripts/region_talk_goal_notify.py",
            "scripts/region_talk_article_browser_materialize.py",
        }
    if arg == "--run-id":
        return script in {
            "kaggle/execute_region_talk_bge_m3_enrichment.py",
            "kaggle/execute_region_talk_image_diagnostic.py",
            "kaggle/execute_region_talk_candidate_report.py",
            "scripts/region_talk_publication_finalizer.py",
            "scripts/region_talk_article_browser_materialize.py",
        }
    return False


def _insert_arg_if_missing(cmd: list[str], arg: str, value: str) -> list[str]:
    if arg in cmd or not value or not _supports_arg(cmd, arg):
        return list(cmd)
    return [*cmd, arg, value]


def _action_run_id(action: dict[str, Any]) -> str:
    action_name = str(action.get("action") or "region-talk-action").replace("launch_", "")
    safe = "".join(ch if ch.isalnum() or ch in "-" else "-" for ch in action_name).strip("-") or "action"
    return f"region-talk-orchestrator-{safe}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"


def prepare_action_command(action: dict[str, Any], *, env_file: str) -> tuple[list[str], str]:
    run_id = str(action.get("run_id") or _action_run_id(action))
    cmd = list(action.get("cmd") or [])
    python_bin = (os.getenv("REGION_TALK_ORCHESTRATOR_PYTHON") or sys.executable or "python3").strip()
    if cmd and Path(str(cmd[0])).name.startswith("python") and python_bin:
        cmd[0] = python_bin
    cmd = _insert_arg_if_missing(cmd, "--env-file", env_file)
    cmd = _insert_arg_if_missing(cmd, "--run-id", run_id)
    return cmd, run_id


def _registry_call(coro: Any) -> None:
    try:
        asyncio.run(coro)
    except RuntimeError:
        # If called from an existing loop in a future server integration, skip
        # sync registry mutation rather than blocking the orchestrator. Server
        # code should call kaggle_registry directly from its own async context.
        return
    except Exception:
        return


def _kernel_ref_for_action(action: dict[str, Any]) -> str:
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    slug = ACTION_KERNEL_SLUGS.get(str(action.get("action") or ""), "")
    return f"{username}/{slug}" if username and slug else ""


def _is_active_kernel_launch_refusal(output: str) -> bool:
    text = str(output or "").lower()
    return (
        "region talk kaggle launch refused" in text
        and "active kernel" in text
    )


def _run_cmd(cmd: list[str], *, dry_run: bool, timeout_seconds: int = 300, action: dict[str, Any] | None = None, run_id: str = "") -> dict[str, Any]:
    if dry_run:
        return {"cmd": cmd, "status": "dry_run", "run_id": run_id}
    action = action or {}
    kernel_ref = _kernel_ref_for_action(action)
    job_type = "region_talk" if kernel_ref else "region_talk_local"
    try:
        from kaggle_registry import register_job, update_job_meta  # type: ignore
    except Exception:
        register_job = update_job_meta = None  # type: ignore
    if register_job and kernel_ref:
        _registry_call(register_job(job_type, kernel_ref, meta={
            "run_id": run_id,
            "action": action.get("action"),
            "resource": action.get("resource"),
            "status": "launching",
            "cmd": cmd,
            "env_overrides": action.get("env") or {},
            "started_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }))
    try:
        child_env = os.environ.copy()
        child_env.update({str(k): str(v) for k, v in dict(action.get("env") or {}).items()})
        proc = subprocess.run(cmd, cwd=str(ROOT), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=max(30, int(timeout_seconds)), env=child_env)
        active_kernel_refusal = proc.returncode != 0 and _is_active_kernel_launch_refusal(proc.stdout)
        status = "ok" if proc.returncode == 0 else ("skipped_active_kernel_race" if active_kernel_refusal else "failed")
        if update_job_meta and kernel_ref:
            _registry_call(update_job_meta(job_type, kernel_ref, meta_updates={
                "status": "launched" if status == "ok" else ("active_kernel_race_skipped" if active_kernel_refusal else "launch_failed"),
                "returncode": proc.returncode,
                "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                "output_tail": proc.stdout[-1000:],
            }))
        payload = {"cmd": cmd, "status": status, "returncode": proc.returncode, "run_id": run_id, "kernel_ref": kernel_ref, "output_tail": proc.stdout[-4000:]}
        if active_kernel_refusal:
            payload["reason"] = "launcher_detected_active_kernel_after_status_snapshot"
        return payload
    except Exception as exc:
        if update_job_meta and kernel_ref:
            _registry_call(update_job_meta(job_type, kernel_ref, meta_updates={
                "status": "launch_exception",
                "error": f"{type(exc).__name__}: {str(exc)[:500]}",
                "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            }))
        raise


def _action(
    action: str,
    cmd: list[str],
    reason: str,
    *,
    resource: str = "",
    parallel_safe: bool = False,
    timeout_seconds: int = 300,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload = {
        "action": action,
        "cmd": cmd,
        "reason": reason,
        "resource": resource,
        "parallel_safe": parallel_safe,
        "timeout_seconds": timeout_seconds,
    }
    if env:
        payload["env"] = dict(env)
    return payload


def select_actions_for_execution(actions: list[dict[str, Any]], *, execute_ready: bool, max_actions: int) -> list[dict[str, Any]]:
    if not actions:
        return []
    if not execute_ready:
        return [next((action for action in actions if str(action.get("action") or "") == "launch_candidate_report"), actions[0])]
    max_count = max(1, int(max_actions or 1))
    if str(actions[0].get("action") or "") == "stop":
        return [actions[0]]
    selected: list[dict[str, Any]] = []
    used_resources: set[str] = set()
    # Reserve the first execution slot for continuous exact/manual/discovery
    # intake. Maintenance and enrichment actions must not starve DISCOVERY1.
    for action in actions:
        if str(action.get("action") or "") != "launch_candidate_report":
            continue
        selected.append(action)
        resource = str(action.get("resource") or "")
        if resource:
            used_resources.add(resource)
        break
    if len(selected) >= max_count:
        return selected
    for action in actions:
        if action.get("parallel_safe"):
            continue
        if str(action.get("action") or "") != "notify_confirmed":
            continue
        resource = str(action.get("resource") or "")
        if resource and resource in used_resources:
            continue
        selected.append(action)
        if resource:
            used_resources.add(resource)
        if len(selected) >= max_count:
            return selected
    for action in actions:
        if not action.get("parallel_safe"):
            continue
        if str(action.get("action") or "") == "launch_candidate_report":
            continue
        resource = str(action.get("resource") or "")
        if resource and resource in used_resources:
            continue
        selected.append(action)
        if resource:
            used_resources.add(resource)
        if len(selected) >= max_count:
            return selected
    for action in actions:
        if action.get("parallel_safe"):
            continue
        if str(action.get("action") or "") == "notify_confirmed":
            continue
        resource = str(action.get("resource") or "")
        if resource and resource in used_resources:
            continue
        if not (resource.startswith("local:") or str(action.get("action") or "") == "run_finalizer"):
            continue
        selected.append(action)
        if resource:
            used_resources.add(resource)
        if len(selected) >= max_count:
            break
    if selected:
        return selected
    return [actions[0]]


PRODUCT_PROGRESS_METRIC_KEYS = (
    # Source/post acquisition that widens the actually inspected population.
    "publics_scanned_with_posts_total",
    "publics_with_ko_candidates_total",
    "processed_posts_unique_total",
    "ko_scope_detected_posts_unique_total",
    # Exact/high-probability durable stage advancement, including known posts.
    "fast_check_exact_posts_processed_unique_total",
    "fast_check_exact_posts_dual_vectorized_total",
    "fast_check_exact_posts_strict_text_accepted_total",
    "confirmed_external_blogger_scanned_total",
    "confirmed_external_blogger_with_ko_total",
    "confirmed_external_blogger_vector_accepted_posts_total",
    # Downstream product milestones.
    "image_actual_scored_total",
    "publication_candidate_total",
    "publication_confirmed_total",
    "publication_draft_ready_confirmed_total",
    "publication_delivery_completed_total",
)


def _progress_signature(metrics: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    """Return product milestones only; retries/status churn is not progress.

    All metrics remain visible in the operator scorecard.  This narrower view
    is used only by the loop feedback controller, so a retry counter, policy
    refresh or arbitrary future scalar cannot keep a zero-output loop alive.
    """
    out: list[tuple[str, int]] = []
    for key in PRODUCT_PROGRESS_METRIC_KEYS:
        value = metrics.get(key)
        if isinstance(value, bool):
            out.append((key, int(value)))
            continue
        if isinstance(value, int):
            out.append((key, value))
            continue
        if isinstance(value, float):
            out.append((key, int(value)))
            continue
        if isinstance(value, str):
            raw = value.strip()
            if re.fullmatch(r"[-+]?\d+(?:\.0+)?", raw):
                out.append((key, int(float(raw))))
    return tuple(out)


def _product_progress_increased(
    previous: tuple[tuple[str, int], ...] | None,
    current: tuple[tuple[str, int], ...],
) -> bool:
    """Count only a monotonic durable milestone increase as new progress."""
    if previous is None:
        return True
    before = dict(previous)
    return any(value > int(before.get(key, 0)) for key, value in current)


def _latest_fast_check_rows(source_rows: list[dict[str, Any]], candidate_run_id: str) -> list[dict[str, Any]]:
    """Return sources actually queried by fast-check in the named run.

    ``run_id``/``last_seen_run_id`` are generic source-overlay fields and are
    refreshed by history, keyword and queue handoff paths too. Using them here
    made the product readout claim e.g. 4 fast-checks/2 hits when the notebook's
    durable fast-check event said 1/0. CandidateReport owns the dedicated
    ``last_fast_check_run_id`` field, so no heuristic fallback is honest.
    """
    run_id = str(candidate_run_id or "").strip()
    if not run_id:
        return []
    return [
        row for row in source_rows
        if str(row.get("fast_check_status") or "").strip()
        and str(row.get("last_fast_check_run_id") or "").strip() == run_id
    ]


CANONICAL_METRIC_ALIASES = {
    "pending_scan": "publics_primary_unscanned_pending_total",
    "touched_or_left_pending": "publics_touched_or_not_pending_total",
    "processed_terminal_total": "publics_terminal_processed_total",
    "needs_rescan_or_retry": "publics_needs_rescan_or_retry_total",
    "publics_scanned_with_posts": "publics_scanned_with_posts_total",
    "source_posts_scanned_sum": "source_queue_posts_scanned_total",
    "processed_post_rows": "processed_posts_unique_total",
    "candidate_memory_rows": "candidate_memory_total",
    "image_actual_scored": "image_actual_scored_total",
    "image_strong_visual_score_ge_0_70": "image_strong_actual_ge_0_70_total",
    "publication_queue_total": "publication_candidate_total",
    "publication_ready": "publication_ready_total",
}


def with_canonical_metric_aliases(metrics: dict[str, Any]) -> dict[str, Any]:
    out = dict(metrics)
    for alias, canonical in CANONICAL_METRIC_ALIASES.items():
        if alias not in out and canonical in out:
            out[alias] = out.get(canonical)
    return out


def _heartbeat_metric_fields(prefix: str, row: dict[str, Any] | None) -> dict[str, Any]:
    payload = row or {}
    return {
        f"{prefix}_heartbeat_run_id": str(payload.get("run_id") or ""),
        f"{prefix}_heartbeat_event_name": str(payload.get("event_name") or ""),
        f"{prefix}_heartbeat_phase": str(payload.get("phase") or ""),
        f"{prefix}_heartbeat_status": str(payload.get("status") or ""),
        f"{prefix}_heartbeat_created_at": str(payload.get("created_at") or payload.get("updated_at") or ""),
        f"{prefix}_heartbeat_event_seq": _safe_int(payload.get("event_seq")),
        f"{prefix}_heartbeat_runtime_elapsed_seconds": _safe_float(payload.get("runtime_elapsed_seconds") or payload.get("elapsed_seconds")) or 0,
        f"{prefix}_heartbeat_posts_fetched": _safe_int(payload.get("posts_fetched")),
        f"{prefix}_heartbeat_posts_scored": _safe_int(payload.get("posts_scored") or payload.get("texts_done")),
        f"{prefix}_heartbeat_sources_history_fetched_ok": _safe_int(payload.get("sources_history_fetched_ok")),
        f"{prefix}_heartbeat_history_fetch_runtime_seconds": _safe_float(payload.get("history_fetch_runtime_seconds")) or 0,
        f"{prefix}_heartbeat_reviewable_candidates": _safe_int(payload.get("current_run_reviewable_candidates")),
    }


GOAL_DELTA_METRICS = {
    "new_publics": "publics_total",
    "processed_posts": "processed_posts_unique_total",
    "ko_sources": "publics_with_ko_candidates_total",
    # Raw image_queue_total includes retained rejected/audit rows and therefore
    # can stay flat while the product obtains newly eligible work.
    "image_queue": "image_product_eligible_total",
    # Raw publication rows include historical sent/rejected/tombstone audit
    # records. Product progress is the currently actionable/unsent set.
    "publication_candidates": "publication_active_candidate_total",
    "confirmed": "publication_confirmed_total",
}


def loop_goal_progress(metrics: dict[str, Any], baseline: dict[str, Any], targets: dict[str, int]) -> dict[str, Any]:
    progress: dict[str, Any] = {}
    reached = True
    active = False
    for name, target in targets.items():
        target = int(target or 0)
        if target <= 0:
            continue
        active = True
        metric = GOAL_DELTA_METRICS[name]
        current = _safe_int(metrics.get(metric))
        base = _safe_int(baseline.get(metric))
        delta = current - base
        ok = delta >= target
        reached = reached and ok
        progress[name] = {
            "metric": metric,
            "baseline": base,
            "current": current,
            "delta": delta,
            "target_delta": target,
            "reached": ok,
        }
    return {"active": active, "reached": bool(active and reached), "items": progress}


def _has_active_region_talk_kernel(kaggle_statuses: dict[str, str]) -> bool:
    return any(str(status or "").upper() in ACTIVE_KERNEL_STATUSES for status in kaggle_statuses.values())


def candidate_adaptive_budget(metrics: dict[str, Any]) -> dict[str, int]:
    """Use measured runtime headroom without extending the 20-minute guardrail."""
    runtime_seconds = _safe_float(metrics.get("candidate_heartbeat_runtime_elapsed_seconds")) or 0.0
    # Only missing current BGE pairs block the live funnel.  The worker sample
    # may also contain already-paired rows selected for semantic-bank
    # maintenance; treating those rows as product debt used to suppress the
    # confirmed-blogger breadth budget even at 100% actionable dual coverage.
    bge_backlog = (
        _safe_int(metrics.get("bge_missing_current_sample_total"))
        if "bge_missing_current_sample_total" in metrics
        else _safe_int(metrics.get("bge_pending_sample_total"))
    )
    bge_capacity = max(1, _safe_int(metrics.get("bge_capacity_rows")) or _env_int("REGION_TALK_EXTERNAL_CPU_BGE_CAPACITY_ROWS", 48))
    confirmed_blogger_pending = _safe_int(metrics.get("confirmed_external_blogger_pending_total"))
    heartbeat_event = str(metrics.get("candidate_heartbeat_event_name") or "").strip().lower()
    heartbeat_phase = str(metrics.get("candidate_heartbeat_phase") or "").strip().lower()
    heartbeat_status = str(metrics.get("candidate_heartbeat_status") or "").strip().lower()
    incomplete_late_tail = runtime_seconds <= 0 and (
        heartbeat_status in {"error", "failed"}
        or heartbeat_event in {"state_write_started", "report_write_started"}
        or heartbeat_phase in {"state_write", "report_write"}
    )
    # Generic history is currently the lowest-yield Telegram lane. Keep its
    # default at four sources while exact/fast-check/keyword work is abundant.
    # Confirmed external bloggers are different: two measured runs completed in
    # 401-561 seconds, scanned three new evidence sources each and left more
    # than half of the 54-source supported cohort unseen. When that backlog is
    # still material and both CandidateReport and the one-run BGE capacity have
    # headroom, spend the unused 20-minute budget on breadth (six sources), not
    # on deeper history per source.
    history_sources = max(2, min(6, _env_int("REGION_TALK_ORCHESTRATOR_HISTORY_SOURCES", 4)))
    confirmed_blogger_slots = min(4, history_sources)
    confirmed_blogger_headroom = (
        confirmed_blogger_pending >= 12
        and 0 < runtime_seconds <= 750
        and bge_backlog <= bge_capacity
        and not incomplete_late_tail
    )
    if confirmed_blogger_headroom:
        history_sources = max(history_sources, 6)
        # Preserve one slot for a publication-source attestation or the normal
        # queue while accelerating first scans from the finite evidence cohort.
        confirmed_blogger_slots = min(5, history_sources)
    fast_check_sources = max(
        1,
        min(12, _env_int("REGION_TALK_ORCHESTRATOR_FAST_CHECK_SOURCES", 10)),
    )
    return {
        "history_sources": history_sources,
        "confirmed_blogger_slots": confirmed_blogger_slots,
        "fast_check_sources": fast_check_sources,
        "bge_capacity_rows": bge_capacity,
        "bge_backlog_capacity_percent": int(round((bge_backlog / bge_capacity) * 100)),
        "runtime_seconds_observed": int(round(runtime_seconds)),
        "incomplete_late_tail_observed": int(incomplete_late_tail),
        "confirmed_blogger_headroom_used": int(confirmed_blogger_headroom),
    }


def _open_ydb_driver(
    ydb: Any,
    *,
    endpoint: str,
    database: str,
    credentials: Any,
) -> Any:
    attempts = max(1, _env_int("REGION_TALK_ORCHESTRATOR_YDB_CONNECT_ATTEMPTS", 3))
    backoff = max(0, _env_int("REGION_TALK_ORCHESTRATOR_YDB_CONNECT_BACKOFF_SECONDS", 5))
    timeout = max(1, _env_int("REGION_TALK_ORCHESTRATOR_YDB_CONNECT_TIMEOUT_SECONDS", 20))
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        driver = ydb.Driver(endpoint=endpoint, database=database, credentials=credentials)
        try:
            driver.wait(timeout=timeout, fail_fast=True)
            return driver
        except Exception as exc:
            last_error = exc
            try:
                driver.stop()
            except Exception:
                pass
            if attempt >= attempts:
                raise
            time.sleep(backoff * attempt)
    assert last_error is not None
    raise last_error


def read_region_talk_queue_metrics(limit: int, *, bge_sample_limit: int, allow_yc_fallback: bool = False) -> dict[str, Any]:
    ydb = ensure_ydb_module()

    endpoint, database = ydb_endpoint_database(allow_yc_fallback=allow_yc_fallback)
    driver = _open_ydb_driver(
        ydb,
        endpoint=endpoint,
        database=database,
        credentials=ydb_credentials(ydb, allow_yc_fallback=allow_yc_fallback),
    )
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
    external_blogger_evidence_rows: list[dict[str, Any]] = []
    external_blogger_evidence_read_ok = False
    try:
        kinds = [
            "source_queue_item",
            "source_status_item",
            "online_source_item",
            "source_candidate_item",
            "source_edge_item",
            "comment_link_item",
            "telegram_entity_cache_item",
            "candidate_memory_item",
            "image_queue_item",
            "image_frame_score_item",
            "publication_candidate_item",
            "external_publication_source_item",
            "external_publication_intake_item",
            "region_talk_llm_budget_item",
            "publication_delivery_item",
            "source_onboarding_evidence_item",
            "source_onboarding_profile_item",
            "post_link_queue_item",
            "text_vector_enrichment_item",
            "processed_post_item",
            "post_live_item",
            "queue_cursor",
        ]
        rows_by_kind: dict[str, list[dict[str, Any]]] = {}
        truncated_kinds: list[str] = []
        for kind in kinds:
            kind_limit = _orchestrator_kind_limit(kind, limit)
            loaded = (
                read_text_vector_metric_rows(pool, ydb, table, kind_limit + 1)
                if kind == "text_vector_enrichment_item"
                else read_kind_rows(pool, ydb, table, kind, kind_limit + 1)
            )
            if len(loaded) > kind_limit:
                truncated_kinds.append(kind)
                loaded = loaded[:kind_limit]
            rows_by_kind[kind] = loaded
        ensure_decision_metric_reads_complete(truncated_kinds)
        latest_query = f"SELECT pk, payload_json, updated_at FROM `{table}` WHERE pk IN ('latest_state', 'latest_business_heartbeat', 'latest_business_heartbeat:bge_m3_enrichment', 'latest_business_heartbeat:image_diagnostic');"
        def read_latest_rows(session: Any) -> dict[str, dict[str, Any]]:
            result_sets = session.transaction(ydb.SnapshotReadOnly()).execute(latest_query, commit_tx=True)
            rows = result_sets[0].rows if result_sets else []
            out: dict[str, dict[str, Any]] = {}
            for row in rows:
                payload = row.payload_json
                value = json.loads(payload) if isinstance(payload, str) else dict(payload or {})
                if isinstance(value, dict):
                    value.setdefault("updated_at", str(getattr(row, "updated_at", "") or ""))
                    out[str(row.pk)] = value
            return out
        latest_rows = pool.retry_operation_sync(read_latest_rows)
        latest_state = latest_rows.get("latest_state") or {}
        evidence_table_name = (os.getenv("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_TABLE") or "region_talk_external_blogger_evidence").strip()
        evidence_table = evidence_table_name if evidence_table_name.startswith("/") else database.rstrip("/") + "/" + evidence_table_name.lstrip("/")
        evidence_limit = max(1, _env_int("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_MAX_ROWS", 2000))
        def read_external_evidence(session: Any) -> list[dict[str, Any]]:
            query = (
                f"SELECT record_id, confirmation_status, region_relation_status, telegram_url, vk_public_url, "
                f"vk_video_url, rutube_url, pipeline_status FROM `{evidence_table}` "
                f"ORDER BY record_id LIMIT {evidence_limit};"
            )
            result_sets = session.transaction(ydb.SnapshotReadOnly()).execute(query, commit_tx=True)
            rows = result_sets[0].rows if result_sets else []
            return [{
                "record_id": getattr(row, "record_id", None),
                "confirmation_status": getattr(row, "confirmation_status", None),
                "region_relation_status": getattr(row, "region_relation_status", None),
                "telegram_url": getattr(row, "telegram_url", None),
                "vk_public_url": getattr(row, "vk_public_url", None),
                "vk_video_url": getattr(row, "vk_video_url", None),
                "rutube_url": getattr(row, "rutube_url", None),
                "pipeline_status": getattr(row, "pipeline_status", None),
            } for row in rows]
        try:
            external_blogger_evidence_rows = pool.retry_operation_sync(read_external_evidence)
            external_blogger_evidence_read_ok = True
        except Exception:
            # Registry observability must not make the whole orchestrator blind
            # if the separately managed stable table is temporarily missing.
            external_blogger_evidence_rows = []
    finally:
        driver.stop()

    latest_run_funnel = latest_state.get("run_funnel_metrics") if isinstance(latest_state.get("run_funnel_metrics"), dict) else {}
    candidates = rows_by_kind["candidate_memory_item"]
    images = rows_by_kind["image_queue_item"]
    image_frame_scores = rows_by_kind["image_frame_score_item"]
    publications = rows_by_kind["publication_candidate_item"]
    external_intakes = rows_by_kind["external_publication_intake_item"]
    external_intake_ids = sorted({
        str(row.get("external_publication_id") or "")
        for row in external_intakes
        if str(row.get("external_publication_id") or "")
    })
    llm_budgets = rows_by_kind["region_talk_llm_budget_item"]
    deliveries = rows_by_kind["publication_delivery_item"]
    onboarding_evidence_rows = rows_by_kind["source_onboarding_evidence_item"]
    onboarding_profile_rows = rows_by_kind["source_onboarding_profile_item"]
    post_links = rows_by_kind["post_link_queue_item"]
    vectors = rows_by_kind["text_vector_enrichment_item"]
    source_candidates = rows_by_kind["source_candidate_item"]
    source_edges = rows_by_kind["source_edge_item"]
    comment_links = rows_by_kind["comment_link_item"]
    row_level_entity_cache_rows = rows_by_kind["telegram_entity_cache_item"]
    legacy_entity_cache = latest_state.get("telegram_entity_cache") if isinstance(latest_state, dict) else {}
    entity_cache_by_key: dict[str, dict[str, Any]] = {
        str(row.get("entity_cache_key") or row.get("username") or row.get("canonical_url") or index): row
        for index, row in enumerate(row_level_entity_cache_rows)
    }
    for key, row in (legacy_entity_cache or {}).items():
        if isinstance(row, dict):
            entity_cache_by_key.setdefault(str(key), row)
    entity_cache_rows = list(entity_cache_by_key.values())
    cursors = rows_by_kind["queue_cursor"]
    source_rows = _merge_source_rows(
        rows_by_kind["source_queue_item"],
        rows_by_kind["source_status_item"],
        rows_by_kind["online_source_item"],
        rows_by_kind["external_publication_source_item"],
    )
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
        # Payloads, not only PKs, are required here: an existing BGE PK may
        # carry a stale semantic-bank contract and must still count as
        # actionable work. Passing a set would make the dry-run planner hide
        # that backlog and suppress the worker launch.
        existing_pks=_rows_by_pk(vectors),
        limit=bge_sample_limit,
    )
    bge_collect_stats = dict(getattr(bge_mod, "LAST_COLLECT_STATS", {}) or {})
    bge_capacity_rows = max(1, _env_int("REGION_TALK_EXTERNAL_CPU_BGE_CAPACITY_ROWS", 48))
    candidate_memory_terminal_local = [
        row for row in candidates
        if str(row.get("source_queue_status") or row.get("fetch_status") or "") == "rejected_local_region_source"
        or str(row.get("source_scope") or "") == "local_region"
        or str(row.get("source_geo_class") or "") == "kaliningrad_local"
        or str(row.get("current_lifecycle_status") or "") == "source_terminal_local_audit_only"
    ]
    candidate_memory_terminal_spam = [
        row for row in candidates
        if str(row.get("source_queue_status") or row.get("fetch_status") or "") == "rejected_spam_source"
        or str(row.get("source_scope") or "") == "spam"
        or str(row.get("source_quick_class") or "") == "spam_source_reject"
        or str(row.get("current_lifecycle_status") or "") == "source_terminal_spam_audit_only"
    ]
    candidate_memory_dual_pending = [
        row for row in candidates
        if str(row.get("vector_gate_status") or "") == "vector_defer_wait_bge_m3"
        or str(row.get("current_stage") or "") == "dual_model_vector_enrichment_pending"
        or str(row.get("text_vector_fusion_status") or "") == "missing_bge_m3_enrichment"
    ]
    candidate_memory_image_wait = [
        row for row in candidates
        if str(row.get("current_lifecycle_status") or "") in {"text_candidate_pending_image", "image_fetch_retry_needed", "image_reviewable"}
    ]

    image_pending = [r for r in images if str(r.get("image_queue_status") or "") in {"", "needs_actual_image_fetch", "selected_for_next_image_batch"}]
    image_pending_vk_without_url = [
        row for row in image_pending
        if re.search(r"vk\.com/wall-?\d+_\d+", str(row.get("post_url") or ""), re.I)
        and not str(row.get("image_url_or_local_path") or row.get("primary_media_path") or "").startswith(("http://", "https://"))
    ]
    image_in_progress = [r for r in images if str(r.get("image_queue_status") or "") == "image_analysis_in_progress"]
    image_actual = [r for r in images if str(r.get("image_queue_status") or "") == "actual_scored" and str(r.get("image_model_input_type") or "") == "actual_image"]
    image_visual_review = [r for r in images if str(r.get("image_quality_decision") or "") == "needs_visual_review"]
    image_scoring_retry = [r for r in images if str(r.get("image_quality_decision") or "") == "scoring_retry"]
    image_legacy_accept = [r for r in images if str(r.get("image_quality_decision") or "") == "legacy_auto_accept"]
    image_vlm_accept = [r for r in images if str(r.get("image_quality_decision") or "") == "vlm_visual_accept"]
    image_vlm_backlog = [r for r in images if _image_vlm_backlog_candidate(r)]
    image_vlm_completed = [r for r in images if _image_vlm_verdict_is_current(r)]
    image_vlm_reject = [r for r in image_vlm_completed if str(r.get("image_vlm_decision") or "").lower() == "reject"]
    image_vlm_review = [r for r in image_vlm_completed if str(r.get("image_vlm_decision") or "").lower() in {"review", "needs_review"}]
    image_vlm_error = [r for r in images if str(r.get("image_vlm_status") or "").lower() in {"error", "rate_limited", "budget_busy", "budget_exhausted"}]
    image_partial_acquisition = [r for r in images if str(r.get("image_acquisition_status") or "") == "partial"]
    image_contract_rescore = [row for row in images if _image_contract_rescore_candidate(row)]
    image_browser_materialization_metrics = _image_browser_materialization_metrics(images)
    image_product_eligible = [
        r for r in images
        if str(r.get("publication_eligibility_decision") or "") == "accept"
        and str(r.get("publication_eligibility_gate_version") or "")
    ]
    image_terminal_metrics = _image_queue_status_metrics(images)
    publication_metrics = _publication_handoff_metrics(images, publications, source_rows, candidates)
    image_review_lifecycle_metrics = _image_review_lifecycle_metrics(images, publications)
    e5_vectors = [r for r in vectors if str(r.get("model_short") or "") == "e5" or str(r.get("model_id") or "").startswith("intfloat/multilingual-e5")]
    bge_vectors = [r for r in vectors if str(r.get("model_short") or "") == "bge_m3" or str(r.get("model_id") or "") == "BAAI/bge-m3"]
    vector_pair_metrics = _text_vector_pair_metrics(e5_vectors, bge_vectors)
    diagnostic_post_rows = _merge_post_rows_for_diagnostics(
        rows_by_kind["processed_post_item"],
        rows_by_kind["post_live_item"],
        candidates,
        images,
        publications,
    )
    regex_vector_metrics = _regex_vector_comparison_metrics(diagnostic_post_rows)
    latest_candidate_run_id = str((latest_rows.get("latest_business_heartbeat") or {}).get("run_id") or "")
    processed_post_rows = rows_by_kind["processed_post_item"]
    processed_post_unique_keys = {_post_merge_key(row) for row in processed_post_rows if _post_merge_key(row)}
    ko_scope_conversion_metrics = _ko_scope_conversion_metrics(processed_post_rows)
    processed_post_latest_run_metrics, processed_post_latest_run_rows, processed_post_latest_run_unique_keys = (
        _latest_processed_post_metrics(processed_post_rows, latest_candidate_run_id)
    )
    heuristic_ko_funnel_metrics = _heuristic_ko_funnel_metrics(
        diagnostic_post_rows,
        source_rows,
        latest_candidate_run_id=latest_candidate_run_id,
        latest_processed_post_keys=processed_post_latest_run_unique_keys,
    )
    source_statuses = [str(r.get("source_queue_status") or r.get("queue_status") or r.get("fetch_status") or "") for r in source_rows]
    source_terminal = [
        r for r, status in zip(source_rows, source_statuses)
        if status.startswith("processed_")
    ]
    source_fake_processed_without_scan = [
        r for r, status in zip(source_rows, source_statuses)
        if status.startswith("processed_") and not _source_has_scan_evidence(r)
    ]
    source_retry = [
        r for r, status in zip(source_rows, source_statuses)
        if status in {"needs_rescan_or_retry", "retry", "error"} or status.startswith(("error", "skipped_telegram_unresolved"))
    ]
    source_touched = [
        r for r, status in zip(source_rows, source_statuses)
        if status and status != "pending_scan"
    ]
    processed_post_source_keys = {k for k in (_post_source_merge_key(r) for r in processed_post_rows) if k}
    source_with_posts = [
        r for r in source_rows
        if max(
            _safe_int(r.get("posts_scanned")),
            _safe_int(r.get("source_history_posts_scanned_max")),
        ) > 0 and _source_has_scan_evidence(r)
    ]
    source_with_posts_count = len(source_with_posts)
    source_with_any_processed_post_count = len(processed_post_source_keys)
    cursor_by_name: dict[str, dict[str, Any]] = {}
    for row in cursors:
        name = _queue_cursor_short_name(str(row.get("queue_name") or row.get("_ydb_pk") or "").replace("queue_cursor:", ""))
        if name and ":" not in name and _cursor_row_is_better(cursor_by_name.get(name), row, name):
            cursor_by_name[name] = row
    source_cursor_position = _safe_int((cursor_by_name.get("unified_source_queue") or cursor_by_name.get("source_scan") or {}).get("cursor_position") or 0)
    source_queue_integrity_metrics = _source_queue_integrity_metrics(source_rows, source_cursor_position)
    source_primary_unscanned_pending = [
        r for r, status in zip(source_rows, source_statuses)
        if status in {"", "pending_scan"} and not _source_has_scan_evidence(r)
    ]
    source_unscanned_after_cursor = [
        r for r, status in zip(source_rows, source_statuses)
        if _safe_int(r.get("queue_order")) > source_cursor_position
        and status in {"", "pending_scan", "needs_rescan_or_retry", "retry", "error"}
        and not _source_has_scan_evidence(r)
    ]
    source_backlog_after_cursor = [
        r for r, status in zip(source_rows, source_statuses)
        if _safe_int(r.get("queue_order")) > source_cursor_position
        and status in {"", "pending_scan", "needs_rescan_or_retry", "retry", "error"}
    ]
    source_pending_with_scan_evidence = [
        r for r, status in zip(source_rows, source_statuses)
        if status == "pending_scan" and _source_has_scan_evidence(r)
    ]
    source_pending_post_probe_only = [
        r for r, status in zip(source_rows, source_statuses)
        if status in {"", "pending_scan"}
        and _source_is_post_probe_only(r)
        and not _source_has_scan_evidence(r)
    ]
    source_processed_no_ko = [r for r, status in zip(source_rows, source_statuses) if status == "processed_no_ko"]
    source_processed_ko_candidate = [r for r, status in zip(source_rows, source_statuses) if status == "processed_found_ko_candidate"]
    source_processed_ko_low_image = [r for r, status in zip(source_rows, source_statuses) if status == "processed_found_ko_low_image_quality"]
    source_ko_candidates = [
        r for r in source_rows
        if str(r.get("source_queue_status") or "") in {"processed_found_ko_candidate", "processed_found_ko_low_image_quality"}
        or _safe_int(r.get("ko_posts_found")) > 0
        or _safe_int(r.get("candidate_posts_found")) > 0
    ]
    ko_candidate_source_keys = _ko_candidate_source_keys([*candidates, *images, *publications])
    source_ko_candidates_count = max(len(source_ko_candidates), len(ko_candidate_source_keys))
    rejected_status_prefixes = ("skipped", "error", "reject", "rejected", "debug_self_loop_rejected")
    rejected_sources = [
        r for r in source_rows
        if str(r.get("fetch_status") or r.get("source_queue_status") or r.get("queue_status") or r.get("frontier_action") or "").startswith(rejected_status_prefixes)
        or bool(str(r.get("monitoring_exclusion_reason") or "").strip())
    ]
    keyword_source_metrics = _keyword_source_metrics(source_rows, source_cursor_position, source_candidates, source_edges)
    fast_check_exact_metrics = _fast_check_exact_post_metrics(
        source_rows,
        processed_post_rows,
        candidates,
        images,
        publications,
        vectors,
    )
    keyword_post_regex_metrics = _keyword_source_post_regex_metrics(source_rows, diagnostic_post_rows, source_candidates, source_edges)
    similar_source_metrics = _similar_source_metrics(source_rows, source_candidates, source_edges)
    keyword_latest_run_metrics = _latest_discovery_run_metrics(
        source_candidates,
        source_edges,
        prefix="publics_keyword",
        predicate=_is_keyword_discovered_source,
    )
    similar_latest_run_metrics = _latest_discovery_run_metrics(
        source_candidates,
        source_edges,
        prefix="publics_similar",
        predicate=_is_similar_discovered_source,
    )
    inflow_metrics = _discovery_inflow_metrics([*source_rows, *source_candidates, *source_edges, *post_links])
    strong_images_ge_066 = [
        r for r in images
        if str(r.get("image_queue_status") or "") == "actual_scored"
        and str(r.get("image_model_input_type") or "") == "actual_image"
        and float(r.get("overall_media_score") or r.get("final_visual_score") or 0) >= 0.66
    ]
    strong_images = [
        r for r in images
        if str(r.get("image_queue_status") or "") == "actual_scored"
        and str(r.get("image_model_input_type") or "") == "actual_image"
        and float(r.get("overall_media_score") or r.get("final_visual_score") or 0) >= 0.70
    ]
    post_link_metrics = _post_link_queue_metrics(post_links, entity_cache_rows, source_rows)
    post_link_rescore_metrics = _bge_ready_exact_rescore_metrics(
        post_links,
        processed_post_rows,
        vectors,
        publications,
        source_rows,
    )
    post_link_source_keys = {str(r.get("canonical_source_key") or r.get("source_key") or "") for r in post_links if str(r.get("canonical_source_key") or r.get("source_key") or "").strip()}
    fast_check_rows = [r for r in source_rows if str(r.get("fast_check_status") or "").strip()]
    fast_check_hit_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "ko_hit"]
    fast_check_no_hit_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "no_hit"]
    fast_check_local_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "local_region_source"]
    fast_check_spam_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "spam_source_reject"]
    confirmed_external_blogger_metrics = _confirmed_external_blogger_funnel_metrics(
        # Candidate memory owns the durable/latest fused E5+BGE verdict; its
        # rows intentionally follow processed rows so they win identity dedup
        # instead of a transient pre-BGE defer or later text compaction.
        source_rows, [*processed_post_rows, *candidates], images, publications, deliveries,
    )
    external_blogger_registry_metrics = _external_blogger_registry_metrics(
        external_blogger_evidence_rows,
        source_rows,
    )
    external_blogger_registry_metrics["external_blogger_registry_read_ok"] = int(external_blogger_evidence_read_ok)
    source_posts_scanned_raw_total = sum(_safe_int(r.get("posts_scanned")) for r in source_rows)
    processed_posts_unique_total = len(processed_post_unique_keys)
    source_posts_scanned_effective_total = max(source_posts_scanned_raw_total, processed_posts_unique_total)
    latest_source_scan_run = max([str(r.get("last_scan_run_id") or "") for r in source_rows if str(r.get("last_scan_run_id") or "").strip()] or [""])
    latest_source_scan_rows = [
        r for r in source_rows
        if latest_source_scan_run and str(r.get("last_scan_run_id") or "") == latest_source_scan_run and _source_has_scan_evidence(r)
    ]
    latest_source_scan_posts = sum(_safe_int(r.get("posts_scanned")) for r in latest_source_scan_rows)
    latest_source_scan_ko_rows = [row for row in latest_source_scan_rows if _source_has_ko_candidate(row)]
    latest_fast_check_rows = _latest_fast_check_rows(source_rows, latest_candidate_run_id)
    latest_fast_check_hit_rows = [row for row in latest_fast_check_rows if str(row.get("fast_check_status") or "") == "ko_hit"]
    history_depth_rows = [r for r in source_rows if _safe_float(r.get("history_avg_post_age_days")) is not None]
    latest_history_run = max([str(r.get("last_scan_run_id") or r.get("run_id") or "") for r in history_depth_rows] or [""])
    latest_history_depth_rows = [
        r for r in history_depth_rows
        if latest_history_run and str(r.get("last_scan_run_id") or r.get("run_id") or "") == latest_history_run
    ]
    latest_llm_budget = _latest_llm_budget_row(llm_budgets)

    metrics = {
        "metric_read_any_truncated": int(bool(truncated_kinds)),
        "metric_read_truncated_kinds": ",".join(sorted(truncated_kinds)),
        "publics_total": len(source_rows),
        "publics_touched_or_not_pending_total": len(source_touched),
        "publics_terminal_processed_total": len(source_terminal),
        "publics_fake_processed_without_scan_evidence_total": len(source_fake_processed_without_scan),
        "publics_needs_rescan_or_retry_total": len(source_retry),
        "publics_scanned_with_posts_total": source_with_posts_count,
        "publics_scanned_with_posts_source_rows_total": len(source_with_posts),
        "publics_scanned_with_posts_repair_delta_total": 0,
        "publics_with_any_processed_post_total": source_with_any_processed_post_count,
        "publics_primary_unscanned_pending_total": len(source_primary_unscanned_pending),
        "publics_unscanned_after_cursor_total": len(source_unscanned_after_cursor),
        "publics_backlog_after_cursor_total": len(source_backlog_after_cursor),
        "publics_scanned_or_rejected_before_cursor_total": sum(
            1 for r, status in zip(source_rows, source_statuses)
            if _safe_int(r.get("queue_order")) <= source_cursor_position and (_source_has_scan_evidence(r) or status.startswith("rejected_") or status.startswith("skipped") or status.startswith("error"))
        ),
        "publics_pending_with_scan_evidence_waiting_rescan_total": len(source_pending_with_scan_evidence),
        "publics_pending_post_probe_only_total": len(source_pending_post_probe_only),
        "publics_processed_no_ko_total": len(source_processed_no_ko),
        "publics_processed_found_ko_candidate_total": len(source_processed_ko_candidate),
        "publics_processed_found_ko_low_image_quality_total": len(source_processed_ko_low_image),
        "publics_rejected_spam_source_total": sum(1 for status in source_statuses if status == "rejected_spam_source"),
        "publics_rejected_local_region_source_total": sum(1 for status in source_statuses if status == "rejected_local_region_source"),
        "publics_with_ko_candidates_total": source_ko_candidates_count,
        "publics_with_ko_candidates_source_rows_total": len(source_ko_candidates),
        "publics_with_ko_candidates_repair_delta_total": max(0, source_ko_candidates_count - len(source_ko_candidates)),
        **keyword_source_metrics,
        **fast_check_exact_metrics,
        **keyword_post_regex_metrics,
        **similar_source_metrics,
        **keyword_latest_run_metrics,
        **similar_latest_run_metrics,
        **inflow_metrics,
        **source_queue_integrity_metrics,
        "source_candidates_total": len(source_candidates),
        "source_edges_total": len(source_edges),
        "comment_link_rows_total": len(comment_links),
        **post_link_metrics,
        **post_link_rescore_metrics,
        "telegram_entity_cache_row_level_rows_total": len(row_level_entity_cache_rows),
        "telegram_entity_cache_legacy_latest_state_rows_total": len(legacy_entity_cache or {}),
        "post_link_queue_unique_sources_total": len(post_link_source_keys),
        "post_link_queue_manual_total": sum(1 for r in post_links if _inflow_kind(r) == "manual"),
        "post_link_queue_keyword_total": sum(1 for r in post_links if _inflow_kind(r) == "keyword"),
        "post_link_queue_hashtag_total": sum(1 for r in post_links if _inflow_kind(r) == "hashtag"),
        "post_link_queue_similar_total": sum(1 for r in post_links if _inflow_kind(r) == "similar"),
        "fast_check_ko_sources_total": len(fast_check_rows),
        "fast_check_ko_hit_sources_total": len(fast_check_hit_rows),
        "fast_check_ko_no_hit_sources_total": len(fast_check_no_hit_rows),
        "fast_check_ko_local_rejected_sources_total": len(fast_check_local_rows),
        "fast_check_ko_spam_rejected_sources_total": len(fast_check_spam_rows),
        "fast_check_ko_hit_post_links_total": sum(1 for r in fast_check_hit_rows if str(r.get("fast_check_hit_post_url") or r.get("keyword_hit_post_url") or "").strip()),
        **confirmed_external_blogger_metrics,
        **external_blogger_registry_metrics,
        "rejected_sources_total": len(rejected_sources),
        "source_queue_posts_scanned_total": source_posts_scanned_effective_total,
        "source_queue_posts_scanned_source_rows_total": source_posts_scanned_raw_total,
        "source_queue_posts_scanned_repair_delta_total": max(0, source_posts_scanned_effective_total - source_posts_scanned_raw_total),
        "source_scan_posts_per_scanned_public_avg": round(source_posts_scanned_effective_total / source_with_posts_count, 2) if source_with_posts_count else 0,
        "source_latest_scan_run_sources_total": len(latest_source_scan_rows),
        "source_latest_scan_run_posts_total": latest_source_scan_posts,
        "source_latest_scan_run_posts_per_source_avg": round(latest_source_scan_posts / len(latest_source_scan_rows), 2) if latest_source_scan_rows else 0,
        "source_latest_scan_run_ko_sources_total": len(latest_source_scan_ko_rows),
        "source_latest_scan_run_ko_source_yield_percent": int(round((len(latest_source_scan_ko_rows) / len(latest_source_scan_rows)) * 100)) if latest_source_scan_rows else 0,
        "fast_check_latest_run_sources_total": len(latest_fast_check_rows),
        "fast_check_latest_run_hit_sources_total": len(latest_fast_check_hit_rows),
        "fast_check_latest_run_hit_rate_percent": int(round((len(latest_fast_check_hit_rows) / len(latest_fast_check_rows)) * 100)) if latest_fast_check_rows else 0,
        "history_depth_sources_total": len(history_depth_rows),
        "history_depth_latest_run_sources_total": len(latest_history_depth_rows),
        "history_avg_post_age_days_avg": _avg_numeric(history_depth_rows, "history_avg_post_age_days"),
        "history_newest_post_age_days_min": _min_numeric(history_depth_rows, "history_newest_post_age_days"),
        "history_oldest_post_age_days_max": _max_numeric(history_depth_rows, "history_oldest_post_age_days"),
        "history_latest_run_avg_post_age_days_avg": _avg_numeric(latest_history_depth_rows, "history_avg_post_age_days"),
        "history_latest_run_newest_post_age_days_min": _min_numeric(latest_history_depth_rows, "history_newest_post_age_days"),
        "history_latest_run_oldest_post_age_days_max": _max_numeric(latest_history_depth_rows, "history_oldest_post_age_days"),
        "processed_posts_unique_total": processed_posts_unique_total,
        "processed_post_rows_total": len(processed_post_rows),
        "processed_post_duplicate_identity_rows_total": max(0, len(processed_post_rows) - processed_posts_unique_total),
        **ko_scope_conversion_metrics,
        "processed_posts_latest_candidate_run_id": latest_candidate_run_id,
        **processed_post_latest_run_metrics,
        "candidate_run_posts_actionable_after_idempotency": _safe_int(latest_run_funnel.get("posts_actionable_after_idempotency")),
        "candidate_run_posts_needing_new_e5": _safe_int(latest_run_funnel.get("posts_needing_new_e5")),
        "candidate_run_posts_reusing_e5_for_bge_fusion": _safe_int(latest_run_funnel.get("posts_reusing_e5_for_bge_fusion")),
        "candidate_run_posts_reusing_e5_for_policy_refresh": _safe_int(latest_run_funnel.get("posts_reusing_e5_for_policy_refresh")),
        "candidate_run_posts_waiting_for_bge_without_e5_recompute": _safe_int(latest_run_funnel.get("posts_waiting_for_bge_without_e5_recompute")),
        "candidate_run_posts_skipped_unchanged_current": _safe_int(latest_run_funnel.get("posts_skipped_unchanged_current")),
        "candidate_run_posts_skipped_legacy_current_dual": _safe_int(latest_run_funnel.get("posts_skipped_legacy_current_dual")),
        "fast_check_latest_run_queries_total": _safe_int(latest_run_funnel.get("fast_check_queries_processed")),
        "fast_check_latest_run_query_elapsed_seconds": _safe_float(latest_run_funnel.get("fast_check_query_elapsed_seconds")),
        "fast_check_latest_run_stage_elapsed_seconds": _safe_float(latest_run_funnel.get("fast_check_stage_elapsed_seconds")),
        "fast_check_latest_run_stage_max_seconds": _safe_float(latest_run_funnel.get("fast_check_stage_max_seconds")),
        "fast_check_latest_run_stage_budget_exhausted": _safe_int(latest_run_funnel.get("fast_check_stage_budget_exhausted")),
        "candidate_memory_total": len(candidates),
        "candidate_memory_terminal_local_audit_total": len(candidate_memory_terminal_local),
        "candidate_memory_terminal_spam_audit_total": len(candidate_memory_terminal_spam),
        "candidate_memory_dual_pending_total": len(candidate_memory_dual_pending),
        "candidate_memory_image_wait_total": len(candidate_memory_image_wait),
        "candidate_memory_operational_total": max(0, len(candidates) - len({str(row.get("_ydb_pk") or id(row)) for row in candidate_memory_terminal_local + candidate_memory_terminal_spam})),
        "image_queue_total": len(images),  # compatibility; historical ledger size
        "image_ledger_rows_total": len(images),
        "image_product_eligible_total": len(image_product_eligible),
        "image_pending_total": len(image_pending),
        "image_pending_vk_without_url_total": len(image_pending_vk_without_url),
        "image_in_progress_total": len(image_in_progress),
        "image_actual_scored_total": len(image_actual),
        "image_frame_scores_total": len(image_frame_scores),
        "image_actual_frames_scored_total": sum(_safe_int(row.get("images_scored_actual_count")) for row in image_actual),
        # Keep the historical raw row count for compatibility, but expose the
        # current actionable review population separately.  Old image rows can
        # remain in the immutable ledger after the publication row is rejected
        # or sent and must not be reported as live backlog.
        "image_visual_review_pending_total": len(image_visual_review),
        "image_scoring_retry_total": len(image_scoring_retry),
        "image_legacy_auto_accept_total": len(image_legacy_accept),
        "image_vlm_backlog_total": len(image_vlm_backlog),
        "image_vlm_completed_total": len(image_vlm_completed),
        "image_vlm_accept_total": len(image_vlm_accept),
        "image_vlm_reject_nonterminal_total": len(image_vlm_reject),
        "image_vlm_review_total": len(image_vlm_review),
        "image_vlm_error_or_budget_deferred_total": len(image_vlm_error),
        "image_partial_album_acquisition_total": len(image_partial_acquisition),
        "image_contract_rescore_backlog_total": len(image_contract_rescore),
        # Browser-wait rows are a separate local-Chromium queue.  They become
        # ordinary ImageDiagnostic work only after materialization changes the
        # queue status to ``needs_actual_image_fetch``.
        "image_actionable_work_total": len(image_pending) + len(image_scoring_retry) + len(image_contract_rescore) + len(image_vlm_backlog),
        **image_browser_materialization_metrics,
        **image_terminal_metrics,
        "image_legacy_diagnostic_ge_0_66_total": len(strong_images_ge_066),
        "image_legacy_diagnostic_ge_0_70_total": len(strong_images),
        "image_strong_actual_ge_0_66_total": len(strong_images_ge_066),  # compatibility only
        "image_strong_actual_ge_0_70_total": len(strong_images),  # compatibility only
        **publication_metrics,
        **image_review_lifecycle_metrics,
        "publication_delivery_rows_total": len(deliveries),
        "external_publication_intake_total": len(external_intakes),
        "external_publication_intake_ids": external_intake_ids,
        "external_publication_intake_ready_for_normal_scoring_total": sum(
            1 for row in external_intakes
            if str((row.get("decision") if isinstance(row.get("decision"), dict) else {}).get("import_status") or "")
            == "ready_for_region_talk_scoring"
            and str(row.get("intake_status") or "") not in {
                "manual_review_required", "needs_manual_review", "blocked", "rejected"
            }
        ),
        "external_publication_intake_manual_review_total": sum(
            1 for row in external_intakes
            if str((row.get("decision") if isinstance(row.get("decision"), dict) else {}).get("import_status") or "")
            == "manual_review_required"
            or str(row.get("intake_status") or "") in {
                "manual_review_required", "needs_manual_review", "blocked", "rejected"
            }
        ),
        "external_publication_intake_permission_not_granted_total": sum(
            1 for row in external_intakes
            if str(row.get("publication_permission") or "not_granted") == "not_granted"
        ),
        "publication_delivery_completed_total": sum(1 for row in deliveries if str(row.get("status") or "") == "delivered"),
        "source_onboarding_evidence_total": len(onboarding_evidence_rows),
        "source_onboarding_profile_total": len(onboarding_profile_rows),
        "source_onboarding_profile_ready_total": sum(1 for row in onboarding_profile_rows if str(row.get("profile_status") or "") == "ready"),
        "publication_onboarding_ready_total": sum(1 for row in publications if str(row.get("source_onboarding_status") or "") == "ready" and bool(row.get("source_onboarding_paragraph"))),
        "publication_onboarding_needs_review_total": sum(1 for row in publications if str(row.get("source_onboarding_status") or "") == "needs_review"),
        "publication_onboarding_pending_unsent_total": sum(
            1 for row in publications
            if str(row.get("publication_status") or "") == "gemini_accept"
            and str(row.get("sent_to_chat") or "").lower() != "true"
            and not (str(row.get("source_onboarding_status") or "") == "ready" and bool(row.get("source_onboarding_paragraph")))
        ),
        "publication_llm_budget_id": str(latest_llm_budget.get("budget_id") or ""),
        "publication_llm_budget_reserved_total": _safe_int(latest_llm_budget.get("reserved_total")),
        "publication_llm_budget_remaining_total": _safe_int(latest_llm_budget.get("remaining")),
        "publication_llm_budget_historical_reserved_total": sum(_safe_int(row.get("reserved_total")) for row in llm_budgets),
        "publication_llm_budget_rows_total": len(llm_budgets),
        "text_vector_enrichment_total": len(vectors),
        "text_vector_e5_total": len(e5_vectors),
        "text_vector_bge_m3_total": len(bge_vectors),
        **vector_pair_metrics,
        **regex_vector_metrics,
        **heuristic_ko_funnel_metrics,
        "latest_candidate_heuristic_ko_hit_rate_percent": int(round((heuristic_ko_funnel_metrics.get("heuristic_ko_latest_run_raw_posts_total", 0) / len(processed_post_latest_run_unique_keys)) * 100)) if processed_post_latest_run_unique_keys else 0,
        "latest_candidate_heuristic_to_text_accept_rate_percent": int(round((heuristic_ko_funnel_metrics.get("heuristic_ko_latest_run_text_accepted_total", 0) / heuristic_ko_funnel_metrics.get("heuristic_ko_latest_run_raw_posts_total", 0)) * 100)) if heuristic_ko_funnel_metrics.get("heuristic_ko_latest_run_raw_posts_total", 0) else 0,
        "latest_candidate_heuristic_to_publication_rate_percent": int(round((heuristic_ko_funnel_metrics.get("heuristic_ko_latest_run_publication_total", 0) / heuristic_ko_funnel_metrics.get("heuristic_ko_latest_run_raw_posts_total", 0)) * 100)) if heuristic_ko_funnel_metrics.get("heuristic_ko_latest_run_raw_posts_total", 0) else 0,
        "bge_pending_sample_total": len(bge_pending_rows),
        "bge_pending_sample_limit": bge_sample_limit,
        "bge_capacity_rows": bge_capacity_rows,
        "bge_backlog_capacity_percent": int(round((len(bge_pending_rows) / bge_capacity_rows) * 100)),
        "bge_backlog_within_next_run_capacity": int(len(bge_pending_rows) <= bge_capacity_rows),
        "bge_source_terminal_skipped_sample_total": _safe_int(bge_collect_stats.get("source_terminal_skipped")),
        "bge_missing_current_sample_total": _safe_int(bge_collect_stats.get("missing_current_bge")),
        "bge_existing_stale_rescore_sample_total": _safe_int(bge_collect_stats.get("existing_stale_rescore")),
        "bge_selected_missing_current_sample_total": _safe_int(bge_collect_stats.get("selected_missing_current_bge")),
        "bge_selected_stale_rescore_sample_total": _safe_int(bge_collect_stats.get("selected_stale_rescore")),
        # Product-facing BGE debt is deliberately separated from optional
        # stale semantic-bank maintenance.  Compatibility fields above retain
        # the raw worker sample for diagnostics.
        "bge_immediate_pair_backlog_total": _safe_int(bge_collect_stats.get("missing_current_bge")),
        "bge_immediate_pair_backlog_capacity_percent": int(round((
            _safe_int(bge_collect_stats.get("missing_current_bge")) / bge_capacity_rows
        ) * 100)),
        "bge_stale_maintenance_backlog_total": _safe_int(bge_collect_stats.get("existing_stale_rescore")),
        "bge_stale_maintenance_selected_sample_total": _safe_int(bge_collect_stats.get("selected_stale_rescore")),
        **_heartbeat_metric_fields("candidate", latest_rows.get("latest_business_heartbeat")),
        **_heartbeat_metric_fields("bge", latest_rows.get("latest_business_heartbeat:bge_m3_enrichment")),
        **_heartbeat_metric_fields("image", latest_rows.get("latest_business_heartbeat:image_diagnostic")),
        **{
            f"cursor_{name}_position": _safe_int(row.get("cursor_position") or row.get("done") or 0)
            for name, row in cursor_by_name.items()
        },
        **{
            f"cursor_{name}_total": _safe_int(row.get("total") or 0)
            for name, row in cursor_by_name.items()
            if row.get("total") not in (None, "")
        },
    }
    return with_canonical_metric_aliases(metrics)


def build_decision_plan(
    metrics: dict[str, Any],
    *,
    target_confirmed: int,
    bge_threshold: int,
    image_threshold: int,
    include_main: bool = True,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if int(metrics.get("publication_draft_backfill_actionable_article_total") or 0) > 0:
        actions.append(_action(
            "backfill_publication_drafts_article",
            [
                "python3", "scripts/region_talk_publication_draft_backfill.py",
                "--limit", str(max(1, min(5, _env_int("REGION_TALK_DRAFT_BACKFILL_ARTICLE_BATCH_SIZE", 2)))),
                "--surface", "article",
            ],
            f"{int(metrics.get('publication_draft_backfill_actionable_article_total') or 0)} confirmed articles need v8 editorial copy",
            resource="local:google-ai-region-talk-writer",
            parallel_safe=True,
            timeout_seconds=420,
        ))
    if int(metrics.get("publication_draft_backfill_actionable_telegram_total") or 0) > 0:
        draft_transport = str(
            os.getenv("REGION_TALK_DRAFT_BACKFILL_TRANSPORT") or "telethon_discovery2"
        ).strip()
        draft_resource = NOTIFY_TRANSPORT_RESOURCES.get(draft_transport)
        if draft_transport == "bot_api" or not draft_resource:
            draft_transport = "telethon_discovery2"
            draft_resource = NOTIFY_TRANSPORT_RESOURCES[draft_transport]
        actions.append(_action(
            "backfill_publication_drafts",
            [
                "python3",
                "scripts/region_talk_publication_draft_backfill.py",
                "--limit",
                str(max(1, min(5, _env_int("REGION_TALK_DRAFT_BACKFILL_BATCH_SIZE", 2)))),
                "--transport",
                draft_transport,
                "--surface",
                "telegram",
            ],
            f"{int(metrics.get('publication_draft_backfill_actionable_telegram_total') or 0)} confirmed Telegram candidates need grounded publication copy",
            resource=draft_resource,
            parallel_safe=True,
            timeout_seconds=300,
        ))
    if int(metrics.get("publication_draft_backfill_actionable_vk_total") or 0) > 0:
        actions.append(_action(
            "backfill_publication_drafts_vk",
            [
                "python3",
                "scripts/region_talk_publication_draft_backfill.py",
                "--limit",
                str(max(1, min(5, _env_int("REGION_TALK_DRAFT_BACKFILL_VK_BATCH_SIZE", 2)))),
                "--surface",
                "vk",
            ],
            f"{int(metrics.get('publication_draft_backfill_actionable_vk_total') or 0)} confirmed VK candidates need grounded publication copy",
            resource="local:vk-api",
            parallel_safe=True,
            timeout_seconds=300,
        ))
    if int(metrics.get("publication_unsent_confirmed_total") or 0) > 0:
        notify_transport = str(os.getenv("REGION_TALK_NOTIFY_TRANSPORT") or "telethon_discovery2").strip()
        notify_resource = NOTIFY_TRANSPORT_RESOURCES.get(notify_transport)
        if not notify_resource:
            notify_transport = "telethon_discovery2"
            notify_resource = NOTIFY_TRANSPORT_RESOURCES[notify_transport]
        actions.append(_action(
            "notify_confirmed",
            ["python3", "scripts/region_talk_goal_notify.py", "--limit", "20", "--transport", notify_transport],
            "confirmed rows not sent to operator chat",
            resource=notify_resource,
            timeout_seconds=180,
        ))

    # Discovery/manual intake is continuous product work, not a recommendation
    # that stops when the publication goal is reached. ``include_main`` remains
    # accepted for CLI/API compatibility, but can no longer disable this lane.
    goal_reached = int(metrics.get("publication_sent_total") or 0) >= target_confirmed or int(metrics.get("publication_confirmed_total") or 0) >= target_confirmed
    exact_ready = (
        int(metrics.get("post_link_queue_exact_ready_total") or 0)
        + int(metrics.get("post_link_queue_bge_ready_rescore_total") or 0)
    )
    exact_source_terminal_cleanup = (
        int(metrics.get("post_link_queue_source_terminal_cleanup_total") or 0)
        + int(metrics.get("post_link_queue_bge_ready_rescore_source_terminal_cleanup_total") or 0)
    )
    exact_cache_hits = int(metrics.get("post_link_queue_entity_cache_hit_total") or 0)
    # Five is still a small human-like exact batch and lets one run consume
    # fresh pending links plus a few `bge_ready_rescore` links. Username resolve
    # remains capped at one; cached rows add only paced get_messages calls.
    exact_fetch_limit = 5
    if exact_ready > 5 and exact_cache_hits > 5:
        exact_fetch_limit = min(8, exact_ready, exact_cache_hits)
    elif exact_source_terminal_cleanup > 5:
        # These rows require no Telegram request or BGE inference: the main
        # worker only persists its already-known authoritative source terminal
        # outcome. Give that bounded cleanup enough room to finish in one run.
        exact_fetch_limit = min(8, exact_source_terminal_cleanup)
    candidate_budget = candidate_adaptive_budget(metrics)
    candidate_env = {
        **MAIN_DISCOVERY_YDB_BUDGET_ENV,
        "REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT": str(max(1, exact_fetch_limit)),
        "REGION_TALK_HISTORY_SOURCES_TARGET": str(candidate_budget["history_sources"]),
        "REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN": str(candidate_budget["history_sources"]),
        "REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN": str(candidate_budget["fast_check_sources"]),
        "REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE": str(max(
            1,
            _env_int(
                "REGION_TALK_ORCHESTRATOR_FAST_CHECK_QUERIES_PER_SOURCE",
                _env_int("REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE", 2),
            ),
        )),
        "REGION_TALK_FAST_CHECK_QUERY_STRATEGY": (
            os.getenv("REGION_TALK_FAST_CHECK_QUERY_STRATEGY") or "adaptive_cursor_v1"
        ).strip().lower(),
        "REGION_TALK_FAST_CHECK_ADAPTIVE_PREFER_CONTINUATIONS": (
            "1" if (os.getenv("REGION_TALK_FAST_CHECK_ADAPTIVE_PREFER_CONTINUATIONS") or "0").strip().lower()
            in {"1", "true", "yes", "on"} else "0"
        ),
        "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_QUERIES_PER_SOURCE": str(max(
            1,
            _env_int("REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_QUERIES_PER_SOURCE", 8),
        )),
        "REGION_TALK_FAST_CHECK_STAGE_MAX_SECONDS": str(max(
            1,
            _env_int("REGION_TALK_FAST_CHECK_STAGE_MAX_SECONDS", 180),
        )),
        "REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_RESULTS_PER_QUERY": str(max(
            2,
            _env_int("REGION_TALK_CONFIRMED_BLOGGER_FAST_CHECK_RESULTS_PER_QUERY", 20),
        )),
        "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_ENABLED": (
            "1" if (os.getenv("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_ENABLED") or "1").strip().lower()
            in {"1", "true", "yes", "on"} else "0"
        ),
        "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_TABLE": (
            os.getenv("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_TABLE") or "region_talk_external_blogger_evidence"
        ),
        "REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_MAX_ROWS": (
            os.getenv("REGION_TALK_EXTERNAL_BLOGGER_EVIDENCE_MAX_ROWS") or "2000"
        ),
        "REGION_TALK_CONFIRMED_BLOGGER_HISTORY_SLOTS_PER_RUN": (
            os.getenv("REGION_TALK_CONFIRMED_BLOGGER_HISTORY_SLOTS_PER_RUN")
            or str(candidate_budget["confirmed_blogger_slots"])
        ),
        "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_ENABLED": (
            "1" if (os.getenv("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_ENABLED") or "1").strip().lower()
            in {"1", "true", "yes", "on"} else "0"
        ),
        "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_QUERIES_PER_SOURCE": (
            os.getenv("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_QUERIES_PER_SOURCE") or "8"
        ),
        "REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_RESULTS_PER_QUERY": (
            os.getenv("REGION_TALK_VK_CONFIRMED_BLOGGER_SEARCH_RESULTS_PER_QUERY") or "20"
        ),
        "REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MIN_SECONDS": str(max(
            0,
            _env_float("REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MIN_SECONDS", 5.0),
        )),
        "REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MAX_SECONDS": str(max(
            _env_float("REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MIN_SECONDS", 5.0),
            _env_float("REGION_TALK_TG_FAST_CHECK_QUERY_DELAY_MAX_SECONDS", 9.0),
        )),
        "REGION_TALK_SOURCE_QUEUE_UNCACHED_RESOLVE_LANE_PER_RUN": str(max(
            0,
            _env_int("REGION_TALK_ORCHESTRATOR_UNCACHED_RESOLVE_LANE_PER_RUN", 1),
        )),
        "REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN": str(max(
            0,
            _env_int("REGION_TALK_ORCHESTRATOR_MAX_NETWORK_RESOLVES_PER_RUN", 1),
        )),
        "REGION_TALK_TG_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN": str(max(
            0,
            _env_int("REGION_TALK_ORCHESTRATOR_EXACT_POST_NETWORK_RESOLVE_BUDGET_PER_RUN", 1),
        )),
    }
    actions.append(_action(
        "launch_candidate_report",
        ["python3", "kaggle/execute_region_talk_candidate_report.py", "--max-sources", str(candidate_budget["history_sources"]), "--no-wait"],
        (
            f"drain up to {exact_fetch_limit} "
            + ("source-terminal exact cleanup rows" if exact_source_terminal_cleanup else "exact KO links")
            + f" first; then history={candidate_budget['history_sources']} "
            f"and fast-check={candidate_budget['fast_check_sources']} from measured runtime={candidate_budget['runtime_seconds_observed']}s; "
            "continue keyword/similar discovery"
            + (" after publication goal" if goal_reached else "")
        ),
        resource="telegram:DISCOVERY1",
        parallel_safe=True,
        timeout_seconds=300,
        env=candidate_env,
    ))

    if int(metrics.get("image_pending_vk_without_url_total") or 0) > 0:
        actions.append(_action(
            "prefetch_vk_media",
            [
                "python3",
                "scripts/region_talk_vk_media_prefetch.py",
                "--max-items",
                "10",
                "--allow-fly-fallback",
            ],
            f"{int(metrics.get('image_pending_vk_without_url_total') or 0)} VK image rows need a server-side public CDN URL before Kaggle scoring",
            resource="local:vk-media-prefetch",
            parallel_safe=True,
            timeout_seconds=180,
        ))

    if int(metrics.get("image_browser_materialization_due_total") or 0) > 0:
        actions.append(_action(
            "materialize_article_browser",
            [
                "python3",
                "scripts/region_talk_article_browser_materialize.py",
                "--execute",
                "--limit",
                "3",
            ],
            (
                f"{int(metrics.get('image_browser_materialization_due_total') or 0)} "
                "JS-only article pages are due for bounded source-image materialization"
            ),
            resource="local:region-talk-chromium",
            parallel_safe=True,
            timeout_seconds=180,
        ))

    # Pipeline invariant: after the main notebook writes fresh E5 rows, BGE is
    # the next required consumer. Do not let finalizer/image actions hide this
    # backlog; BGE has no Telegram session and can run in parallel with both
    # CandidateReport(DISCOVERY1) and ImageDiagnostic(DISCOVERY2).
    current_backlog_key = "text_vector_current_version_e5_without_bge_total"
    pair_backlog = (
        int(metrics.get(current_backlog_key) or 0)
        if current_backlog_key in metrics
        else int(metrics.get("text_vector_e5_without_bge_exact_text_total") or 0)
    )
    # Missing current BGE pairs are the only immediate dual-vector dependency.
    # `bge_pending_sample_total` also contains optional stale semantic-bank
    # rescoring, which must not occupy the production launch slot or suppress
    # source breadth.  Keep the old sample as a compatibility fallback for
    # metric snapshots produced before the split.
    bge_backlog = (
        int(metrics.get("bge_missing_current_sample_total") or 0)
        if "bge_missing_current_sample_total" in metrics
        else int(metrics.get("bge_immediate_pair_backlog_total") or 0)
        if "bge_immediate_pair_backlog_total" in metrics
        else int(metrics.get("bge_pending_sample_total") or 0)
        if "bge_pending_sample_total" in metrics
        else pair_backlog
    )
    if bge_backlog >= bge_threshold:
        external_cpu_capacity = max(1, _env_int("REGION_TALK_EXTERNAL_CPU_BGE_CAPACITY_ROWS", 48))
        requested_limit = max(1, _env_int("REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT", external_cpu_capacity))
        bge_batch_limit = min(requested_limit, external_cpu_capacity)
        # The external BGE runtime has a measured batch-size contract of four.
        # CandidateReport remains E5-only, while this process loads BGE only.
        bge_batch_size = 4
        actions.append(_action(
            "launch_bge_m3",
            [
                "python3", "kaggle/execute_region_talk_bge_m3_enrichment.py",
                "--batch-limit", str(max(1, bge_batch_limit)),
                "--batch-size", str(max(1, bge_batch_size)),
                "--no-wait",
            ],
            f"current E5 rows missing their paired BGE vector need immediate enrichment ({bge_backlog}/{external_cpu_capacity} CPU-row capacity)",
            resource="kaggle:bge_m3",
            parallel_safe=True,
            timeout_seconds=300,
            env={
                "REGION_TALK_BGE_E5_ONLY": "1",
                "REGION_TALK_BGE_INPUT_KINDS": "text_vector_enrichment_item",
                # The shared kind already exceeds 6k rows; a prefix-limited
                # scan can permanently starve newer E5 rows whose PK sorts
                # after that window.
                "REGION_TALK_BGE_YDB_SCAN_LIMIT": "20000",
                "REGION_TALK_BGE_BATCH_SIZE": "4",
            },
        ))
    image_actionable_work = int(metrics.get("image_actionable_work_total") or metrics.get("image_pending_total") or 0)
    if image_actionable_work >= image_threshold:
        # Keep live debugging observable: the album-safe scorer can inspect up
        # to 20 frames per post and loads several CPU models.  A fixed 30-post
        # launch made the Kaggle UI look hung even when business heartbeats
        # were healthy.  Ten posts is the bounded default while the explicit
        # env overrides retain a throughput tuning knob for scheduled runs.
        image_max_items = max(1, _env_int("REGION_TALK_ORCHESTRATOR_IMAGE_MAX_ITEMS_PER_RUN", 10))
        image_batch_size = max(
            1,
            min(
                image_max_items,
                _env_int("REGION_TALK_ORCHESTRATOR_IMAGE_BATCH_SIZE", 5),
            ),
        )
        actions.append(_action(
            "launch_image_diagnostic",
            [
                "python3", "kaggle/execute_region_talk_image_diagnostic.py",
                "--source", "ydb", "--max-items-per-run", str(image_max_items), "--batch-size", str(image_batch_size),
                "--wait-initial-seconds", "120", "--wait-after-drain-seconds", "0",
                "--image-poll-interval-seconds", "30", "--no-wait",
            ],
            (
                "ожидает проверка изображений после полного текстового фильтра (новые/повтор/переоценка альбома/визуальная проверка Gemini="
                f"{int(metrics.get('image_pending_total') or 0)}/"
                f"{int(metrics.get('image_scoring_retry_total') or 0)}/"
                f"{int(metrics.get('image_contract_rescore_backlog_total') or 0)}/"
                f"{int(metrics.get('image_vlm_backlog_total') or 0)}); uses DISCOVERY2"
            ),
            resource="telegram:DISCOVERY2",
            parallel_safe=True,
            timeout_seconds=300,
            env={
                "REGION_TALK_IMAGE_VLM_ENABLED": "1",
                "REGION_TALK_IMAGE_VLM_MAX_CALLS_PER_RUN": str(max(0, _env_int("REGION_TALK_ORCHESTRATOR_IMAGE_VLM_MAX_CALLS_PER_RUN", 2))),
                "REGION_TALK_LLM_BUDGET_MAX": str(min(100, max(0, _env_int("REGION_TALK_LLM_BUDGET_MAX", 100)))),
            },
        ))
    if int(metrics.get("publication_source_evidence_backlog_total") or 0) > 0:
        actions.append(_action(
            "prioritize_source_evidence",
            [
                "python3",
                "scripts/region_talk_publication_finalizer.py",
                "--max-llm",
                "0",
                "--prioritize-source-evidence-only",
            ],
            f"{int(metrics.get('publication_source_evidence_backlog_total') or 0)} strong finalist sources need bounded source attestation",
            resource="local:ydb-source-evidence",
            timeout_seconds=300,
        ))
    finalizer_pending = int(metrics.get("finalizer_pending_url_total") or 0)
    onboarding_pending = int(metrics.get("publication_onboarding_pending_unsent_total") or 0)
    if finalizer_pending > 0 or onboarding_pending > 0:
        actions.append(_action(
            "run_finalizer",
            ["python3", "scripts/region_talk_publication_finalizer.py", "--max-llm", "3"],
            f"{finalizer_pending} post URLs require finalization/eligibility refresh; {onboarding_pending} accepted unsent rows need source onboarding",
            resource="local:gemini",
            timeout_seconds=900,
        ))
    return actions


def run_orchestrator_cycle(args: argparse.Namespace, *, allow_yc_fallback: bool, cycle_index: int) -> dict[str, Any]:
    will_execute = bool(args.execute or args.execute_ready)
    metrics = read_region_talk_queue_metrics(args.limit, bge_sample_limit=args.bge_sample_limit, allow_yc_fallback=allow_yc_fallback)
    kaggle_statuses: dict[str, str] = {}
    kaggle_status_error = ""
    if not args.skip_kaggle_status:
        try:
            kaggle_statuses = read_kaggle_kernel_statuses((os.getenv("KAGGLE_USERNAME") or "").strip())
        except Exception as exc:
            kaggle_status_error = f"{type(exc).__name__}: {str(exc)[:300]}"
    if kaggle_statuses:
        metrics["kaggle_kernel_statuses"] = kaggle_statuses
    if kaggle_status_error:
        metrics["kaggle_status_error"] = kaggle_status_error
    actions = build_decision_plan(
        metrics,
        target_confirmed=args.target_confirmed,
        bge_threshold=args.bge_threshold,
        image_threshold=args.image_threshold,
        include_main=not args.no_main,
    )
    actions, active_kernel_skips = filter_actions_for_active_kernels(
        actions,
        kaggle_statuses,
        block_unverified=not bool(args.allow_unverified_kaggle_status),
    )
    result: dict[str, Any] = {
        "ok": True,
        "cycle": cycle_index,
        "dry_run": not will_execute,
        "metrics": metrics,
        "actions": actions,
    }
    if active_kernel_skips:
        result["active_kernel_skips"] = active_kernel_skips
    if args.stats_message:
        result["stats_message"] = build_orchestrator_stats_message(metrics)
    if will_execute:
        # Every action selection gets its own current YDB snapshot.  Remote and
        # local actions can mutate queues while the cycle is still running, so
        # selecting four actions from the first snapshot is not safe.
        selected_names: list[str] = []
        executions: list[dict[str, Any]] = []
        selection_snapshots: list[dict[str, Any]] = []
        selected_action_keys: set[str] = set()
        previous_intake_ids = {
            str(value) for value in (metrics.get("external_publication_intake_ids") or [])
            if str(value)
        }
        max_selections = max(1, int(args.max_actions_per_cycle or 1))
        for selection_index in range(1, max_selections + 1):
            current_metrics = read_region_talk_queue_metrics(
                args.limit,
                bge_sample_limit=args.bge_sample_limit,
                allow_yc_fallback=allow_yc_fallback,
            )
            if kaggle_statuses:
                current_metrics["kaggle_kernel_statuses"] = kaggle_statuses
            current_actions = build_decision_plan(
                current_metrics,
                target_confirmed=args.target_confirmed,
                bge_threshold=args.bge_threshold,
                image_threshold=args.image_threshold,
                include_main=not args.no_main,
            )
            current_actions, current_skips = filter_actions_for_active_kernels(
                current_actions,
                kaggle_statuses,
                block_unverified=not bool(args.allow_unverified_kaggle_status),
            )
            current_actions = [
                action for action in current_actions
                if str(action.get("action") or "") not in selected_action_keys
            ]
            selected = select_actions_for_execution(
                current_actions,
                execute_ready=bool(args.execute_ready),
                max_actions=1,
            )
            current_intake_ids = {
                str(value) for value in (current_metrics.get("external_publication_intake_ids") or [])
                if str(value)
            }
            new_intake_ids = sorted(current_intake_ids - previous_intake_ids)
            selection_snapshots.append({
                "selection_index": selection_index,
                "read_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "metrics": current_metrics,
                "candidate_actions": [str(item.get("action") or "") for item in current_actions],
                "active_kernel_skips": current_skips,
                "new_external_publication_intake_ids": new_intake_ids,
                "new_external_publication_intake_count": len(new_intake_ids),
            })
            previous_intake_ids = current_intake_ids
            if not selected:
                break
            action = selected[0]
            action_name = str(action.get("action") or "")
            selected_action_keys.add(action_name)
            selected_names.append(action_name)
            if not action.get("cmd"):
                continue
            if action_name == "stop":
                continue
            # In production the scheduler supplies credentials through the
            # process environment and there is intentionally no /app/.env.
            # Forward --env-file only when the parent actually loaded one;
            # Path("") would otherwise turn into "." and make every child
            # launcher fail its explicit env-file preflight.
            cmd, run_id = prepare_action_command(action, env_file=str(args.env_file or ""))
            executions.append(_run_cmd(
                cmd,
                dry_run=False,
                timeout_seconds=int(action.get("timeout_seconds") or 300),
                action=action,
                run_id=run_id,
            ))
        result["metrics"] = dict(selection_snapshots[-1]["metrics"]) if selection_snapshots else metrics
        result["selection_metric_snapshots"] = selection_snapshots
        result["selected_actions"] = selected_names
        result["execution"] = executions
    return result


def orchestrator_poll_sleep_seconds(metrics: dict[str, Any], *, normal_seconds: int, downstream_seconds: int) -> int:
    normal = max(5, int(normal_seconds or 180))
    bge_immediate = (
        int(metrics.get("bge_missing_current_sample_total") or 0)
        if "bge_missing_current_sample_total" in metrics
        else int(metrics.get("bge_pending_sample_total") or 0)
    )
    if bge_immediate > 0 or any(int(metrics.get(key) or 0) > 0 for key in (
        "image_pending_total", "finalizer_pending_url_total", "publication_unsent_confirmed_total",
    )):
        return min(normal, max(5, int(downstream_seconds or 60)))
    return normal


_TRANSIENT_CYCLE_ERROR_MARKERS = (
    "connectionfailure",
    "deadline exceeded",
    "failed to resolve endpoints",
    "connection reset",
    "temporarily unavailable",
    "resource exhausted",
    "overloaded",
)


def _is_transient_cycle_error(exc: BaseException) -> bool:
    """Return True only for bounded infrastructure/read failures.

    Authentication, configuration and code errors must still fail the loop
    immediately.  The retry contour exists for short YDB endpoint/session
    outages observed between otherwise successful metric snapshots.
    """

    text = f"{type(exc).__name__}: {exc}".lower()
    return any(marker in text for marker in _TRANSIENT_CYCLE_ERROR_MARKERS)


def run_orchestrator_cycle_with_retries(
    args: argparse.Namespace,
    *,
    allow_yc_fallback: bool,
    cycle_index: int,
    retry_limit: int,
    backoff_seconds: float,
    sleep_fn: Any = time.sleep,
) -> dict[str, Any]:
    """Run one loop cycle and survive a bounded transient YDB read outage."""

    retry_events: list[dict[str, Any]] = []
    retries = max(0, int(retry_limit or 0))
    base_backoff = max(0.0, float(backoff_seconds or 0.0))
    for attempt in range(retries + 1):
        try:
            result = run_orchestrator_cycle(
                args,
                allow_yc_fallback=allow_yc_fallback,
                cycle_index=cycle_index,
            )
        except Exception as exc:
            if attempt >= retries or not _is_transient_cycle_error(exc):
                raise
            wait_seconds = min(60.0, base_backoff * (2**attempt))
            retry_events.append({
                "attempt": attempt + 1,
                "error": f"{type(exc).__name__}: {str(exc)[:500]}",
                "wait_seconds": wait_seconds,
            })
            sleep_fn(wait_seconds)
            continue
        if retry_events:
            result["cycle_transient_retries"] = retry_events
        return result
    raise AssertionError("unreachable orchestrator retry loop")


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
    parser.add_argument("--no-main", action="store_true", help="deprecated compatibility flag; CandidateReport discovery/manual intake remains enabled")
    parser.add_argument("--execute", action="store_true", help="execute the first planned action (default: dry-run only)")
    parser.add_argument("--execute-ready", action="store_true", help="execute all non-conflicting parallel-safe launch actions in this cycle")
    parser.add_argument("--max-actions-per-cycle", type=int, default=4)
    parser.add_argument("--skip-kaggle-status", action="store_true", help="do not query Kaggle active kernel statuses before planning")
    parser.add_argument("--allow-unverified-kaggle-status", action="store_true", help="allow Kaggle launches even when a Region Talk kernel status cannot be verified")
    parser.add_argument("--loop", action="store_true", help="keep polling YDB/Kaggle and launching ready work until target/limits")
    parser.add_argument("--cycle-sleep-seconds", type=int, default=180, help="sleep between loop cycles")
    parser.add_argument("--downstream-backlog-poll-seconds", type=int, default=60, help="poll faster while actionable BGE/image/finalizer work remains")
    parser.add_argument("--max-cycles", type=int, default=0, help="maximum loop cycles; 0 means unlimited until other limits")
    parser.add_argument("--max-runtime-minutes", type=int, default=0, help="maximum wall-clock loop runtime; 0 disables")
    parser.add_argument("--no-progress-cycles", type=int, default=8, help="stop loop after this many idle no-progress cycles with no active kernels")
    parser.add_argument("--cycle-error-retries", type=int, default=3, help="bounded retries for transient YDB endpoint/session failures inside --loop")
    parser.add_argument("--cycle-error-backoff-seconds", type=float, default=15.0, help="initial exponential backoff for transient loop-cycle failures")
    parser.add_argument("--target-new-publics", type=int, default=0, help="loop goal: stop after this many new source/public rows versus loop baseline")
    parser.add_argument("--target-processed-posts", type=int, default=0, help="loop goal: stop after this many newly processed unique posts versus loop baseline")
    parser.add_argument("--target-ko-sources", type=int, default=0, help="loop goal: stop after this many additional KO candidate sources versus loop baseline")
    parser.add_argument("--target-image-queue", type=int, default=0, help="loop goal: stop after this many additional image queue rows versus loop baseline")
    parser.add_argument("--target-publication-candidates", type=int, default=0, help="loop goal: stop after this many additional publication candidate rows versus loop baseline")
    args = parser.parse_args()
    env_path = Path(args.env_file).expanduser()
    explicit_env_file = "--env-file" in sys.argv[1:]
    if explicit_env_file and not env_path.is_file():
        print(json.dumps({
            "ok": False,
            "dry_run": not bool(args.execute or args.execute_ready),
            "error": "missing_env_file",
            "env_file": str(env_path),
            "next_action": "pass an existing absolute --env-file path; do not launch a partially configured CandidateReport from a linked worktree",
        }, ensure_ascii=False, indent=2))
        return 2
    if env_path.is_file():
        env_path = env_path.resolve()
        args.env_file = str(env_path)
        load_env(env_path)
    else:
        # A missing default .env is valid for a deployed process whose secrets
        # are injected by the runtime.  Explicit missing paths still fail above.
        args.env_file = ""
    ensure_kaggle_username_env()
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
            "dry_run": not bool(args.execute or args.execute_ready),
            "error": "missing_ydb_config",
            "missing": missing,
            "next_action": "run from the configured server, export live YDB endpoint/database plus service-account/token credentials, or pass --allow-yc-fallback for local debug",
        }, ensure_ascii=False, indent=2))
        return 2
    if allow_yc_fallback:
        try:
            ensure_child_ydb_env(allow_yc_fallback=True)
        except Exception:
            # The metrics reader will return the detailed YDB/Yandex Cloud error
            # below. Do not print credentials or partial tokens here.
            pass
    if not args.loop:
        try:
            result = run_orchestrator_cycle(args, allow_yc_fallback=allow_yc_fallback, cycle_index=1)
        except Exception as exc:
            print(json.dumps({
                "ok": False,
                "dry_run": not bool(args.execute or args.execute_ready),
                "error": f"{type(exc).__name__}: {str(exc)[:500]}",
                "next_action": "provide live YDB/Kaggle credentials, or retry local debug with --allow-yc-fallback; do not launch when Kaggle status is unverified unless manually audited",
            }, ensure_ascii=False, indent=2))
            return 2
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    started = time.monotonic()
    max_runtime_seconds = max(0, int(args.max_runtime_minutes or 0)) * 60
    last_signature: tuple[int, ...] | None = None
    idle_no_progress = 0
    cycle = 0
    baseline_metrics: dict[str, Any] | None = None
    loop_targets = {
        "new_publics": int(args.target_new_publics or 0),
        "processed_posts": int(args.target_processed_posts or 0),
        "ko_sources": int(args.target_ko_sources or 0),
        "image_queue": int(args.target_image_queue or 0),
        "publication_candidates": int(args.target_publication_candidates or 0),
    }
    # --target-confirmed is an absolute product goal handled by build_decision_plan.
    # Delta loop targets above are optional debug/product sprint goals.
    while True:
        cycle += 1
        try:
            result = run_orchestrator_cycle_with_retries(
                args,
                allow_yc_fallback=allow_yc_fallback,
                cycle_index=cycle,
                retry_limit=int(args.cycle_error_retries or 0),
                backoff_seconds=float(args.cycle_error_backoff_seconds or 0.0),
            )
        except Exception as exc:
            result = {
                "ok": False,
                "cycle": cycle,
                "dry_run": not bool(args.execute or args.execute_ready),
                "error": f"{type(exc).__name__}: {str(exc)[:500]}",
            }
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return 2
        metrics = dict(result.get("metrics") or {})
        if baseline_metrics is None:
            baseline_metrics = dict(metrics)
        goal_progress = loop_goal_progress(metrics, baseline_metrics, loop_targets)
        if goal_progress.get("active"):
            result["loop_goal_progress"] = goal_progress
        print(json.dumps(result, ensure_ascii=False), flush=True)
        actions = list(result.get("actions") or [])
        if actions and actions[0].get("action") == "stop":
            return 0
        if goal_progress.get("reached"):
            return 0
        signature = _progress_signature(metrics)
        active = _has_active_region_talk_kernel(dict(metrics.get("kaggle_kernel_statuses") or {}))
        executed = bool(result.get("execution"))
        if _product_progress_increased(last_signature, signature):
            idle_no_progress = 0
        elif active:
            # A running remote worker has not yet had a chance to publish its
            # durable outcome. Do not penalize it, but do not claim progress.
            pass
        else:
            # Launching another action is activity, not progress. Repeated
            # zero-yield launches must eventually hit the configured stop.
            idle_no_progress += 1
        last_signature = signature
        if int(args.max_cycles or 0) and cycle >= int(args.max_cycles):
            return 0
        if max_runtime_seconds and (time.monotonic() - started) >= max_runtime_seconds:
            return 0
        if int(args.no_progress_cycles or 0) and idle_no_progress >= int(args.no_progress_cycles or 0):
            return 0
        sleep_seconds = orchestrator_poll_sleep_seconds(
            metrics,
            normal_seconds=int(args.cycle_sleep_seconds or 180),
            downstream_seconds=int(args.downstream_backlog_poll_seconds or 60),
        )
        time.sleep(sleep_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
