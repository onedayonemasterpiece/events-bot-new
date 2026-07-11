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
    is_unsent_confirmed_publication,
    ensure_ydb_module,
    load_env,
    read_kind_rows,
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

CURRENT_E5_ENCODER_CONTRACT = "e5_semantic_bank_scores_v1"
CURRENT_BGE_M3_ENCODER_CONTRACT = "bge_m3_flagembedding_dense_v1"
CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION = "region_talk_publication_eligibility_v2"
POST_LINK_READY_STATUSES = {"", "pending_fetch", "retry_fetch", "fetch_error"}
POST_LINK_TERMINAL_STATUSES = {"fetched", "scored"}

MAIN_DISCOVERY_YDB_BUDGET_ENV = {
    "REGION_TALK_STATE_BACKEND": "ydb",
    "REGION_TALK_REQUIRE_YDB_STATE": "1",
    "REGION_TALK_TEXT_EMBEDDING_MODEL_IDS": "intfloat/multilingual-e5-base",
    "REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS": "0",
    "REGION_TALK_EXTERNAL_BGE_M3_FUSION_ENABLED": "1",
    "REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE": "1",
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
    "REGION_TALK_BUILD_IMAGE_QUEUE_BEFORE_SOURCE_QUEUE": "1",
    # Full queue state is already durable in YDB. Keep per-run artifacts to the
    # lightweight product/debug shortlist instead of serializing 7k source rows
    # into a 40-sheet workbook and duplicate JSON/HTML copies every cycle.
    "REGION_TALK_LIGHTWEIGHT_REPORT": "1",
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
    "REGION_TALK_YDB_MAX_POST_ROWS": "1500",
    "REGION_TALK_YDB_MAX_SOURCE_ROWS": "20000",
    "REGION_TALK_YDB_SOURCE_QUEUE_FULL_READ_LIMIT": "20000",
    "REGION_TALK_YDB_MAX_CANDIDATE_ROWS": "1000",
    "REGION_TALK_YDB_MAX_TEXT_VECTOR_ROWS": "6000",
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
    "REGION_TALK_HISTORY_SOURCES_TARGET": "6",
    "REGION_TALK_SOURCE_SELECTION_YDB_QUEUE_ONLY": "1",
    "REGION_TALK_MAX_POSTS_PER_SOURCE": "20",
    "REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN": "6",
    "REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE": "20",
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
    "REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES": "4",
    "REGION_TALK_MAX_TELEGRAM_KEYWORD_PHRASE_QUERIES": "2",
    "REGION_TALK_MAX_TELEGRAM_HASHTAG_QUERIES_PER_RUN": "2",
    "REGION_TALK_TELEGRAM_QUERY_ROTATE": "1",
    "REGION_TALK_TELEGRAM_KEYWORD_RESULTS_PER_QUERY": "5",
    "REGION_TALK_MAX_KEYWORD_DISCOVERED_SOURCES_PER_RUN": "10",
    # Manual and discovered exact URLs are the first bounded intake lane.  They
    # do not replace source/history/similar/keyword/hashtag discovery below.
    "REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST": "1",
    "REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT": "3",
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
    "REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN": "5",
    "REGION_TALK_FAST_CHECK_KO_QUERIES_PER_SOURCE": "2",
    "REGION_TALK_FAST_CHECK_KO_RESULTS_PER_QUERY": "2",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_FAST_CHECK_KO_SECONDS": "330",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_DISCOVERY_TAIL_SECONDS": "300",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_KEYWORD_QUERY_SECONDS": "240",
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
}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


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


def build_orchestrator_stats_message(metrics: dict[str, Any]) -> str:
    """Render one coherent stats surface from the already-read metric snapshot."""
    value = lambda key: _safe_int(metrics.get(key))
    latest_outcomes = metrics.get("heuristic_ko_latest_run_outcome_counts") or {}
    if not isinstance(latest_outcomes, dict):
        latest_outcomes = {}
    top_latest_outcomes = ", ".join(
        f"{reason}={count}"
        for reason, count in sorted(latest_outcomes.items(), key=lambda item: (-_safe_int(item[1]), str(item[0])))[:8]
    ) or "none"
    return "\n".join([
        "📊 Region Talk live YDB stats",
        f"Источников в canonical population: {value('publics_total')}",
        f"Pending primary scan: {value('publics_primary_unscanned_pending_total')}",
        f"Terminal processed: {value('publics_terminal_processed_total')}",
        f"Sources with broad KO/candidate evidence (legacy): {value('publics_with_ko_candidates_total')}",
        (
            "Keyword sources discovered/scanned/preliminary/confirmed-KO/external-confirmed-KO: "
            f"{value('publics_keyword_discovered_total')}/"
            f"{value('publics_keyword_scanned_with_posts_total')}/"
            f"{value('keyword_sources_with_preliminary_candidates_total')}/"
            f"{value('keyword_sources_with_confirmed_ko_posts_total')}/"
            f"{value('keyword_external_sources_with_confirmed_ko_posts_total')}"
        ),
        (
            "Fast-check keyword matches/exact processed/dual/text accepted/image/video/publication: "
            f"{value('fast_check_keyword_match_sources_total')}/"
            f"{value('fast_check_exact_posts_processed_unique_total')}/"
            f"{value('fast_check_exact_posts_dual_vectorized_total')}/"
            f"{value('fast_check_exact_posts_strict_text_accepted_total')}/"
            f"{value('fast_check_exact_posts_image_queue_total')}/"
            f"{value('fast_check_exact_posts_video_manual_review_total')}/"
            f"{value('fast_check_exact_posts_publication_queue_total')}"
        ),
        (
            "Inflow manual/keyword/hashtag/similar: "
            f"{value('discovery_inflow_manual_total')}/"
            f"{value('discovery_inflow_keyword_total')}/"
            f"{value('discovery_inflow_hashtag_total')}/"
            f"{value('discovery_inflow_similar_total')}"
        ),
        (
            "Queue unordered/duplicate rows: "
            f"{value('source_queue_integrity_unordered_total')}/"
            f"{value('source_queue_integrity_duplicate_order_rows_total')}"
        ),
        (
            "Exact ready/cooldown/entity-wait/fetched: "
            f"{value('post_link_queue_exact_ready_total')}/"
            f"{value('post_link_queue_cooldown_total')}/"
            f"{value('post_link_queue_entity_wait_total')}/"
            f"{value('post_link_queue_fetched_total')}"
        ),
        (
            "Processed posts unique/raw/duplicate rows: "
            f"{value('processed_posts_unique_total')}/"
            f"{value('processed_post_rows_total')}/"
            f"{value('processed_post_duplicate_identity_rows_total')}"
        ),
        (
            "Latest Candidate posts unique/raw/duplicate rows: "
            f"{value('processed_posts_unique_latest_candidate_run_total')}/"
            f"{value('processed_post_rows_latest_candidate_run_total')}/"
            f"{value('processed_post_duplicate_identity_rows_latest_candidate_run_total')}"
        ),
        (
            "Heuristic KO latest raw/vector/text/image/publication/sent: "
            f"{value('heuristic_ko_latest_run_raw_posts_total')}/"
            f"{value('heuristic_ko_latest_run_vector_evaluated_total')}/"
            f"{value('heuristic_ko_latest_run_text_accepted_total')}/"
            f"{value('heuristic_ko_latest_run_image_queue_total')}/"
            f"{value('heuristic_ko_latest_run_publication_total')}/"
            f"{value('heuristic_ko_latest_run_sent_total')}"
        ),
        (
            "Latest yields heuristic-per-post/text-per-heuristic/publication-per-heuristic: "
            f"{value('latest_candidate_heuristic_ko_hit_rate_percent')}%/"
            f"{value('latest_candidate_heuristic_to_text_accept_rate_percent')}%/"
            f"{value('latest_candidate_heuristic_to_publication_rate_percent')}%"
        ),
        (
            "Latest source scan sources/KO/yield; fast-check sources/hits/yield: "
            f"{value('source_latest_scan_run_sources_total')}/"
            f"{value('source_latest_scan_run_ko_sources_total')}/"
            f"{value('source_latest_scan_run_ko_source_yield_percent')}%; "
            f"{value('fast_check_latest_run_sources_total')}/"
            f"{value('fast_check_latest_run_hit_sources_total')}/"
            f"{value('fast_check_latest_run_hit_rate_percent')}%"
        ),
        f"Heuristic KO latest outcomes: {top_latest_outcomes}",
        (
            "Candidate memory total/operational/local-audit/spam-audit/dual-pending/image-wait: "
            f"{value('candidate_memory_total')}/"
            f"{value('candidate_memory_operational_total')}/"
            f"{value('candidate_memory_terminal_local_audit_total')}/"
            f"{value('candidate_memory_terminal_spam_audit_total')}/"
            f"{value('candidate_memory_dual_pending_total')}/"
            f"{value('candidate_memory_image_wait_total')}"
        ),
        (
            "Current E5/BGE raw/actionable coverage: "
            f"{value('text_vector_current_version_dual_coverage_percent')}%/"
            f"{value('text_vector_current_version_dual_actionable_coverage_percent')}% "
            f"(raw pending {value('text_vector_current_version_e5_without_bge_total')}; "
            f"actionable {value('text_vector_current_version_e5_without_bge_actionable_total')}; "
            f"short excluded {value('text_vector_current_version_e5_below_bge_min_text_total')}; "
            f"terminal-source excluded {value('text_vector_current_version_e5_without_bge_source_terminal_total')})"
        ),
        (
            "BGE actionable/capacity/load/terminal-skipped: "
            f"{value('bge_pending_sample_total')}/"
            f"{value('bge_capacity_rows')}/"
            f"{value('bge_backlog_capacity_percent')}%/"
            f"{value('bge_source_terminal_skipped_sample_total')}"
        ),
        (
            "Images pending/actual/strong>=0.70: "
            f"{value('image_pending_total')}/"
            f"{value('image_actual_scored_total')}/"
            f"{value('image_strong_actual_ge_0_70_total')}"
        ),
        (
            "Publication total/confirmed/sent/ready: "
            f"{value('publication_candidate_total')}/"
            f"{value('publication_confirmed_total')}/"
            f"{value('publication_sent_total')}/"
            f"{value('publication_ready_total')}"
        ),
        (
            "Finalizer actual-image/video inputs/pending: "
            f"{value('image_actual_scored_urls_total')}/"
            f"{value('video_manual_review_candidate_urls_total')}/"
            f"{value('finalizer_pending_url_total')}"
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
    for action in actions:
        slug = ACTION_KERNEL_SLUGS.get(str(action.get("action") or ""))
        status = str(kaggle_statuses.get(slug or "") or "").upper()
        if slug and status in ACTIVE_KERNEL_STATUSES:
            skipped.append({"action": action.get("action"), "kernel_slug": slug, "status": status, "reason": "kernel_already_active"})
            continue
        if slug and block_unverified and status.startswith(UNVERIFIED_KERNEL_STATUS_PREFIXES):
            skipped.append({"action": action.get("action"), "kernel_slug": slug, "status": status, "reason": "kernel_status_unverified"})
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
        if handle:
            aliases.add(f"telegram:{handle}")
            aliases.add(f"https://t.me/{handle}")
    post_url_handle = _telegram_handle_from_url(str(row.get("post_url") or ""))
    if post_url_handle:
        aliases.add(f"telegram:{post_url_handle}")
        aliases.add(f"https://t.me/{post_url_handle}")
    return {a.lower().rstrip("/") for a in aliases if a}


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
            for k, v in row.items():
                if v in (None, ""):
                    continue
                if k in numeric_max_fields:
                    current[k] = max(_safe_int(current.get(k)), _safe_int(v))
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


def _post_link_queue_metrics(
    post_links: list[dict[str, Any]],
    entity_cache_rows: list[dict[str, Any]] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    states = [_post_link_state(row, now=current) for row in post_links]
    urls = [_canonical_post_url(row) for row in post_links]
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
) -> dict[str, Any]:
    source_by_key = {_source_merge_key(row): row for row in source_rows if _source_merge_key(row)}
    heuristic_rows: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    for row in rows:
        text = _row_text_for_regex(row)
        if not text:
            continue
        diagnostic = _regex_ko_diagnostic(text)
        if not diagnostic.get("regex_ko_raw"):
            continue
        source = source_by_key.get(_post_source_merge_key(row), {})
        heuristic_rows.append((row, diagnostic, _heuristic_ko_outcome(row, diagnostic, source)))

    def belongs_to_run(row: dict[str, Any]) -> bool:
        if not latest_candidate_run_id:
            return False
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
    return {
        "image_not_reviewable_no_media_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "not_reviewable_no_media"),
        "image_not_reviewable_unsupported_media_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "not_reviewable_unsupported_media"),
        "image_rejected_text_gate_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "rejected_text_gate"),
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

    def needs_finalizer(row: dict[str, Any] | None) -> bool:
        if not row:
            return True
        live_fingerprint = str(row.get("_live_authoritative_source_fingerprint") or "")
        persisted_fingerprint = str(row.get("authoritative_source_fingerprint") or "")
        if (
            str(row.get("_live_authoritative_source_found") or "").lower() == "true"
            and live_fingerprint
            and live_fingerprint != persisted_fingerprint
        ):
            return True
        eligibility_verdict = str(row.get("publication_eligibility_verdict") or "").lower()
        gate_version = str(row.get("publication_eligibility_gate_version") or "")
        if not eligibility_verdict or gate_version != CURRENT_PUBLICATION_ELIGIBILITY_GATE_VERSION:
            return True
        publication_status = str(row.get("publication_status") or "").lower()
        candidate_status = str(row.get("publication_candidate_status") or "").lower()
        retryable = publication_status in {"gemini_rate_limited", "gemini_error", "gemini_unknown"} or candidate_status in {
            "llm_budget_deferred", "llm_error", "retry_due",
        }
        if not retryable:
            return False
        retry_at = _parse_iso_datetime(row.get("next_attempt_after"))
        return retry_at is None or retry_at <= now

    finalizer_pending_urls = sorted(
        url for url in finalizer_input_by_url
        if needs_finalizer(publication_by_url.get(url))
    )

    confirmed_urls = {url for url, row in publication_by_url.items() if is_confirmed_publication(row)}
    unsent_confirmed_urls = {url for url in confirmed_urls if is_unsent_confirmed_publication(publication_by_url[url])}
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
        if candidate_status in {"ready_for_llm", "llm_needs_review", "llm_budget_deferred", "llm_error", "retry_due"}:
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
        "publication_sent_total": len(sent_urls),
        "publication_unsent_confirmed_total": len(unsent_confirmed_urls),
        "publication_verifier_pending_total": sum(1 for status in status_by_url.values() if status == "ready_for_llm"),
        "publication_review_or_retry_total": sum(1 for status in status_by_url.values() if status in {"llm_needs_review", "llm_budget_deferred", "llm_error"}),
        "publication_rejected_total": sum(1 for status in status_by_url.values() if status in {"filtered_before_llm", "llm_rejected"}),
        "publication_source_evidence_backlog_total": len(source_evidence_urls),
        "publication_source_evidence_backlog_urls": source_evidence_urls,
        "finalizer_pending_url_total": len(finalizer_pending_urls),
        "finalizer_pending_urls": finalizer_pending_urls,
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
    return bool(
        _safe_int(row.get("posts_scanned")) > 0
        or str(row.get("last_history_fetch_at") or "").strip()
        or str(row.get("primary_scan_completed_at") or "").strip()
        or str(row.get("last_successful_delta_scan_at") or "").strip()
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
    rejection_counts: dict[str, int] = {}
    for url in fetched_urls:
        processed_row = processed.get(url) or {}
        candidate_row = candidates.get(url) or {}
        row = {**processed_row, **candidate_row}
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
        if source_ok and vector_status == "vector_accept_candidate" and fused and ko_only and fresh:
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
        elif not ko_only:
            reason = "not_confirmed_ko_only"
        elif not fused:
            reason = "dual_vector_not_complete"
        else:
            reason = "other_text_gate"
        rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

    video_urls = {url for url in accepted_urls if _is_video_manual_review_row(images.get(url) or {})}
    return {
        "fast_check_keyword_match_sources_total": len(hit_rows),
        "fast_check_exact_hit_post_urls_total": len(urls),
        "fast_check_exact_posts_processed_unique_total": len(fetched_urls),
        "fast_check_exact_posts_dual_vectorized_total": len(paired_urls),
        "fast_check_exact_posts_strict_text_accepted_total": len(accepted_urls),
        "fast_check_exact_posts_strict_text_accepted_urls": sorted(accepted_urls),
        "fast_check_exact_posts_text_rejected_total": max(0, len(fetched_urls) - len(accepted_urls)),
        "fast_check_exact_posts_text_rejection_reasons": rejection_counts,
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
        }
    if arg == "--run-id":
        return script in {
            "kaggle/execute_region_talk_bge_m3_enrichment.py",
            "kaggle/execute_region_talk_image_diagnostic.py",
            "kaggle/execute_region_talk_candidate_report.py",
            "scripts/region_talk_publication_finalizer.py",
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
        selected.append(action)
        resource = str(action.get("resource") or "")
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


def _progress_signature(metrics: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    # Monitor the complete numeric funnel. If a metric is emitted as a scalar
    # number, it participates in progress/no-progress decisions without manual
    # allow-lists or omissions.
    out: list[tuple[str, int]] = []
    for key in sorted(metrics):
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
    bge_backlog = _safe_int(metrics.get("bge_pending_sample_total"))
    bge_capacity = max(1, _safe_int(metrics.get("bge_capacity_rows")) or _env_int("REGION_TALK_EXTERNAL_CPU_BGE_CAPACITY_ROWS", 48))
    heartbeat_event = str(metrics.get("candidate_heartbeat_event_name") or "").strip().lower()
    heartbeat_phase = str(metrics.get("candidate_heartbeat_phase") or "").strip().lower()
    heartbeat_status = str(metrics.get("candidate_heartbeat_status") or "").strip().lower()
    incomplete_late_tail = runtime_seconds <= 0 and (
        heartbeat_status in {"error", "failed"}
        or heartbeat_event in {"state_write_started", "report_write_started"}
        or heartbeat_phase in {"state_write", "report_write"}
    )
    if runtime_seconds > 1050 or incomplete_late_tail or bge_backlog > bge_capacity:
        history_sources = 5
    elif runtime_seconds > 900 or bge_backlog >= int(round(bge_capacity * 0.75)):
        history_sources = 6
    else:
        # Two profiled production runs completed in about 13-14 minutes with
        # six sources. Use part of the measured headroom for breadth, not depth.
        history_sources = 8
    history_sources = max(4, min(10, _env_int("REGION_TALK_ORCHESTRATOR_HISTORY_SOURCES", history_sources)))
    fast_check_sources = max(
        5,
        min(10, _env_int("REGION_TALK_ORCHESTRATOR_FAST_CHECK_SOURCES", history_sources)),
    )
    return {
        "history_sources": history_sources,
        "fast_check_sources": fast_check_sources,
        "bge_capacity_rows": bge_capacity,
        "bge_backlog_capacity_percent": int(round((bge_backlog / bge_capacity) * 100)),
        "runtime_seconds_observed": int(round(runtime_seconds)),
        "incomplete_late_tail_observed": int(incomplete_late_tail),
    }


def read_region_talk_queue_metrics(limit: int, *, bge_sample_limit: int, allow_yc_fallback: bool = False) -> dict[str, Any]:
    ydb = ensure_ydb_module()

    endpoint, database = ydb_endpoint_database(allow_yc_fallback=allow_yc_fallback)
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb, allow_yc_fallback=allow_yc_fallback))
    driver.wait(timeout=20, fail_fast=True)
    pool = ydb.SessionPool(driver)
    table = ydb_table_path(database)
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
            "publication_candidate_item",
            "region_talk_llm_budget_item",
            "publication_delivery_item",
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
            loaded = read_kind_rows(pool, ydb, table, kind, kind_limit + 1)
            if len(loaded) > kind_limit:
                truncated_kinds.append(kind)
                loaded = loaded[:kind_limit]
            rows_by_kind[kind] = loaded
        latest_query = f"SELECT pk, payload_json, updated_at FROM `{table}` WHERE pk IN ('latest_state', 'latest_business_heartbeat', 'latest_business_heartbeat:bge_m3_enrichment', 'latest_business_heartbeat:image_diagnostic');"
        def read_latest_rows(session: Any) -> dict[str, dict[str, Any]]:
            result_sets = session.transaction(ydb.StaleReadOnly()).execute(latest_query, commit_tx=True)
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
    finally:
        driver.stop()

    candidates = rows_by_kind["candidate_memory_item"]
    images = rows_by_kind["image_queue_item"]
    publications = rows_by_kind["publication_candidate_item"]
    llm_budgets = rows_by_kind["region_talk_llm_budget_item"]
    deliveries = rows_by_kind["publication_delivery_item"]
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
        existing_pks={str(v.get("_ydb_pk") or "") for v in vectors},
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
    image_product_eligible = [
        r for r in images
        if str(r.get("publication_eligibility_decision") or "") == "accept"
        and str(r.get("publication_eligibility_gate_version") or "")
    ]
    image_terminal_metrics = _image_queue_status_metrics(images)
    publication_metrics = _publication_handoff_metrics(images, publications, source_rows)
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
    heuristic_ko_funnel_metrics = _heuristic_ko_funnel_metrics(
        diagnostic_post_rows,
        source_rows,
        latest_candidate_run_id=latest_candidate_run_id,
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
    processed_post_rows = rows_by_kind["processed_post_item"]
    processed_post_unique_keys = {_post_merge_key(row) for row in processed_post_rows if _post_merge_key(row)}
    processed_post_latest_run_rows = [
        row for row in processed_post_rows
        if latest_candidate_run_id and latest_candidate_run_id in {
            str(row.get("run_id") or ""), str(row.get("last_seen_run_id") or ""), str(row.get("current_run_id") or "")
        }
    ]
    processed_post_latest_run_unique_keys = {
        _post_merge_key(row) for row in processed_post_latest_run_rows if _post_merge_key(row)
    }
    processed_post_source_keys = {k for k in (_post_source_merge_key(r) for r in processed_post_rows) if k}
    source_with_posts = [r for r in source_rows if _safe_int(r.get("posts_scanned")) > 0]
    source_with_posts_count = max(len(source_with_posts), len(processed_post_source_keys))
    cursor_by_name: dict[str, dict[str, Any]] = {}
    for row in cursors:
        name = _queue_cursor_short_name(str(row.get("queue_name") or row.get("_ydb_pk") or "").replace("queue_cursor:", ""))
        if name and ":" not in name and _cursor_row_is_better(cursor_by_name.get(name), row, name):
            cursor_by_name[name] = row
    source_cursor_position = _safe_int((cursor_by_name.get("unified_source_queue") or cursor_by_name.get("source_scan") or {}).get("cursor_position") or 0)
    source_queue_integrity_metrics = _source_queue_integrity_metrics(source_rows, source_cursor_position)
    source_primary_unscanned_pending = [
        r for r, status in zip(source_rows, source_statuses)
        if status in {"", "pending_scan"} and _safe_int(r.get("posts_scanned")) <= 0 and not str(r.get("last_scan_run_id") or r.get("last_history_fetch_at") or "").strip()
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
        if status == "pending_scan" and (_safe_int(r.get("posts_scanned")) > 0 or str(r.get("last_scan_run_id") or r.get("last_history_fetch_at") or "").strip())
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
    post_link_metrics = _post_link_queue_metrics(post_links, entity_cache_rows)
    post_link_source_keys = {str(r.get("canonical_source_key") or r.get("source_key") or "") for r in post_links if str(r.get("canonical_source_key") or r.get("source_key") or "").strip()}
    fast_check_rows = [r for r in source_rows if str(r.get("fast_check_status") or "").strip()]
    fast_check_hit_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "ko_hit"]
    fast_check_no_hit_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "no_hit"]
    fast_check_local_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "local_region_source"]
    fast_check_spam_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "spam_source_reject"]
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
    latest_fast_check_rows = [
        row for row in fast_check_rows
        if latest_candidate_run_id and str(row.get("run_id") or row.get("last_seen_run_id") or "") == latest_candidate_run_id
    ]
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
        "publics_scanned_with_posts_repair_delta_total": max(0, source_with_posts_count - len(source_with_posts)),
        "publics_primary_unscanned_pending_total": len(source_primary_unscanned_pending),
        "publics_unscanned_after_cursor_total": len(source_unscanned_after_cursor),
        "publics_backlog_after_cursor_total": len(source_backlog_after_cursor),
        "publics_scanned_or_rejected_before_cursor_total": sum(
            1 for r, status in zip(source_rows, source_statuses)
            if _safe_int(r.get("queue_order")) <= source_cursor_position and (_source_has_scan_evidence(r) or status.startswith("rejected_") or status.startswith("skipped") or status.startswith("error"))
        ),
        "publics_pending_with_scan_evidence_waiting_rescan_total": len(source_pending_with_scan_evidence),
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
        "processed_posts_latest_candidate_run_id": latest_candidate_run_id,
        "processed_post_rows_latest_candidate_run_total": len(processed_post_latest_run_rows),
        "processed_posts_unique_latest_candidate_run_total": len(processed_post_latest_run_unique_keys),
        "processed_post_duplicate_identity_rows_latest_candidate_run_total": max(0, len(processed_post_latest_run_rows) - len(processed_post_latest_run_unique_keys)),
        "candidate_memory_total": len(candidates),
        "candidate_memory_terminal_local_audit_total": len(candidate_memory_terminal_local),
        "candidate_memory_terminal_spam_audit_total": len(candidate_memory_terminal_spam),
        "candidate_memory_dual_pending_total": len(candidate_memory_dual_pending),
        "candidate_memory_image_wait_total": len(candidate_memory_image_wait),
        "candidate_memory_operational_total": max(0, len(candidates) - len({str(row.get("_ydb_pk") or id(row)) for row in candidate_memory_terminal_local + candidate_memory_terminal_spam})),
        "image_queue_total": len(images),
        "image_product_eligible_total": len(image_product_eligible),
        "image_pending_total": len(image_pending),
        "image_pending_vk_without_url_total": len(image_pending_vk_without_url),
        "image_in_progress_total": len(image_in_progress),
        "image_actual_scored_total": len(image_actual),
        **image_terminal_metrics,
        "image_strong_actual_ge_0_66_total": len(strong_images_ge_066),
        "image_strong_actual_ge_0_70_total": len(strong_images),
        **publication_metrics,
        "publication_delivery_rows_total": len(deliveries),
        "publication_delivery_completed_total": sum(1 for row in deliveries if str(row.get("status") or "") == "delivered"),
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
    if int(metrics.get("publication_unsent_confirmed_total") or 0) > 0:
        actions.append(_action("notify_confirmed", ["python3", "scripts/region_talk_goal_notify.py", "--limit", "20"], "confirmed rows not sent to operator chat", resource="telegram:e2e", timeout_seconds=180))

    # Discovery/manual intake is continuous product work, not a recommendation
    # that stops when the publication goal is reached. ``include_main`` remains
    # accepted for CLI/API compatibility, but can no longer disable this lane.
    goal_reached = int(metrics.get("publication_sent_total") or 0) >= target_confirmed or int(metrics.get("publication_confirmed_total") or 0) >= target_confirmed
    exact_ready = int(metrics.get("post_link_queue_exact_ready_total") or 0)
    exact_cache_hits = int(metrics.get("post_link_queue_entity_cache_hit_total") or 0)
    # Three is the conservative baseline. When the queue already has private
    # entities, a larger exact batch only adds paced get_messages calls and does
    # not increase the dangerous username-resolve budget (which remains one).
    exact_fetch_limit = 3
    if exact_ready > 3 and exact_cache_hits > 3:
        exact_fetch_limit = min(8, exact_ready, exact_cache_hits)
    candidate_budget = candidate_adaptive_budget(metrics)
    candidate_env = {
        **MAIN_DISCOVERY_YDB_BUDGET_ENV,
        "REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT": str(max(1, exact_fetch_limit)),
        "REGION_TALK_HISTORY_SOURCES_TARGET": str(candidate_budget["history_sources"]),
        "REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN": str(candidate_budget["history_sources"]),
        "REGION_TALK_FAST_CHECK_KO_SOURCES_PER_RUN": str(candidate_budget["fast_check_sources"]),
    }
    actions.append(_action(
        "launch_candidate_report",
        ["python3", "kaggle/execute_region_talk_candidate_report.py", "--max-sources", str(candidate_budget["history_sources"]), "--no-wait"],
        (
            f"drain up to {exact_fetch_limit} exact KO links first; then history={candidate_budget['history_sources']} "
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
    # `bge_pending_sample_total` is produced by the same collect_text_rows()
    # contract as the worker. Pair-gap metrics may include legacy/version rows
    # whose BGE PK already exists and cannot be repaired by launching the worker
    # again. Prefer the actionable sample whenever it is present; use the pair
    # gap only for older metric snapshots that predate this field.
    bge_backlog = (
        int(metrics.get("bge_pending_sample_total") or 0)
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
            f"pending E5 text-vector rows need paired BGE enrichment immediately after main E5 ({bge_backlog}/{external_cpu_capacity} CPU-row capacity)",
            resource="kaggle:bge_m3",
            parallel_safe=True,
            timeout_seconds=300,
            env={
                "REGION_TALK_BGE_E5_ONLY": "1",
                "REGION_TALK_BGE_INPUT_KINDS": "text_vector_enrichment_item",
                "REGION_TALK_BGE_YDB_SCAN_LIMIT": "6000",
                "REGION_TALK_BGE_BATCH_SIZE": "4",
            },
        ))
    if int(metrics.get("image_pending_total") or 0) >= image_threshold:
        actions.append(_action(
            "launch_image_diagnostic",
            [
                "python3", "kaggle/execute_region_talk_image_diagnostic.py",
                "--source", "ydb", "--max-items-per-run", "30", "--batch-size", "10",
                "--wait-initial-seconds", "120", "--wait-after-drain-seconds", "0",
                "--image-poll-interval-seconds", "30", "--no-wait",
            ],
            "text-confirmed image queue has pending rows; uses DISCOVERY2",
            resource="telegram:DISCOVERY2",
            parallel_safe=True,
            timeout_seconds=300,
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
    if int(metrics.get("finalizer_pending_url_total") or 0) > 0:
        actions.append(_action(
            "run_finalizer",
            ["python3", "scripts/region_talk_publication_finalizer.py", "--max-llm", "3"],
            f"{int(metrics.get('finalizer_pending_url_total') or 0)} actual-image post URLs have no publication row",
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
        selected = select_actions_for_execution(
            actions,
            execute_ready=bool(args.execute_ready),
            max_actions=args.max_actions_per_cycle,
        )
        result["selected_actions"] = [a.get("action") for a in selected]
        executions = []
        for action in selected:
            if not action.get("cmd"):
                continue
            if str(action.get("action") or "") == "stop":
                continue
            cmd, run_id = prepare_action_command(action, env_file=str(Path(args.env_file)))
            executions.append(_run_cmd(
                cmd,
                dry_run=False,
                timeout_seconds=int(action.get("timeout_seconds") or 300),
                action=action,
                run_id=run_id,
            ))
        result["execution"] = executions
    return result


def orchestrator_poll_sleep_seconds(metrics: dict[str, Any], *, normal_seconds: int, downstream_seconds: int) -> int:
    normal = max(5, int(normal_seconds or 180))
    if any(int(metrics.get(key) or 0) > 0 for key in (
        "bge_pending_sample_total",
        "image_pending_total",
        "finalizer_pending_url_total",
        "publication_unsent_confirmed_total",
    )):
        return min(normal, max(5, int(downstream_seconds or 60)))
    return normal


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
    parser.add_argument("--target-new-publics", type=int, default=0, help="loop goal: stop after this many new source/public rows versus loop baseline")
    parser.add_argument("--target-processed-posts", type=int, default=0, help="loop goal: stop after this many newly processed unique posts versus loop baseline")
    parser.add_argument("--target-ko-sources", type=int, default=0, help="loop goal: stop after this many additional KO candidate sources versus loop baseline")
    parser.add_argument("--target-image-queue", type=int, default=0, help="loop goal: stop after this many additional image queue rows versus loop baseline")
    parser.add_argument("--target-publication-candidates", type=int, default=0, help="loop goal: stop after this many additional publication candidate rows versus loop baseline")
    args = parser.parse_args()
    load_env(Path(args.env_file))
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
            result = run_orchestrator_cycle(args, allow_yc_fallback=allow_yc_fallback, cycle_index=cycle)
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
        if signature != last_signature or active or executed:
            idle_no_progress = 0
        else:
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
