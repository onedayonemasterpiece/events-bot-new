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
    ydb_token,
)


ACTIVE_KERNEL_STATUSES = {"RUNNING", "PENDING", "QUEUED", "INITIALIZING"}
UNVERIFIED_KERNEL_STATUS_PREFIXES = ("UNVERIFIED",)
ACTION_KERNEL_SLUGS = {
    "launch_candidate_report": "region-talk-candidate-report",
    "launch_bge_m3": "region-talk-bge-m3-enrichment",
    "launch_image_diagnostic": "region-talk-image-diagnostic",
}

MAIN_DISCOVERY_YDB_BUDGET_ENV = {
    "REGION_TALK_STATE_BACKEND": "ydb",
    "REGION_TALK_REQUIRE_YDB_STATE": "1",
    "REGION_TALK_TEXT_EMBEDDING_MODEL_IDS": "intfloat/multilingual-e5-base",
    "REGION_TALK_REQUIRE_DUAL_TEXT_EMBEDDINGS": "0",
    "REGION_TALK_EXTERNAL_BGE_M3_FUSION_ENABLED": "1",
    "REGION_TALK_REQUIRE_EXTERNAL_BGE_M3_FOR_IMAGE_QUEUE": "1",
    # Debug/product runs must finish quickly enough to be observable.  The main
    # notebook still reaches the source-queue/discovery tail, but with smaller
    # batches and bounded YDB writes; otherwise Kaggle spends >30 minutes in
    # tail assembly and YDB row upserts before the next iteration can start.
    "REGION_TALK_SKIP_REPORT_TAIL_AFTER_IMAGE_QUEUE_HANDOFF": "0",
    "REGION_TALK_SKIP_REPORT_TAIL_AFTER_SOURCE_QUEUE_HANDOFF": "1",
    "REGION_TALK_NOTEBOOK_MAX_RUNTIME_SECONDS": "720",
    "REGION_TALK_RUNTIME_RESERVE_BEFORE_REPORT_SECONDS": "210",
    "REGION_TALK_RUNTIME_LOW_BUDGET_MAX_POSTS_TO_SCORE": "25",
    "REGION_TALK_MAX_POSTS_TO_SCORE_PER_RUN": "90",
    "REGION_TALK_YDB_MAX_POST_ROWS": "1500",
    "REGION_TALK_YDB_MAX_SOURCE_ROWS": "6000",
    "REGION_TALK_YDB_MAX_CANDIDATE_ROWS": "1000",
    "REGION_TALK_YDB_MAX_TEXT_VECTOR_ROWS": "6000",
    "REGION_TALK_YDB_SELECT_PAGE_SIZE": "100",
    "REGION_TALK_YDB_REQUEST_TIMEOUT_SECONDS": "6",
    "REGION_TALK_YDB_QUEUE_REQUEST_TIMEOUT_SECONDS": "6",
    "REGION_TALK_YDB_QUEUE_MAX_RETRIES": "0",
    "REGION_TALK_YDB_ROW_UPSERT_CHUNK_SIZE": "25",
    "REGION_TALK_YDB_STATE_LOAD_ATTEMPTS": "4",
    "REGION_TALK_YDB_STATE_LOAD_BACKOFF_SECONDS": "20",
    "REGION_TALK_YDB_STATE_LOAD_REQUEST_TIMEOUT_SECONDS": "12",
    "REGION_TALK_YDB_STATE_LOAD_MAX_RETRIES": "1",
    "REGION_TALK_YDB_ONLINE_QUEUE_WRITE_MAX_ROWS": "80",
    "REGION_TALK_YDB_CANDIDATE_MEMORY_WRITE_CHANGED_ONLY": "1",
    "REGION_TALK_YDB_ONLINE_CANDIDATE_WRITE_MAX_ROWS": "80",
    "REGION_TALK_SOURCE_QUEUE_HANDOFF_MAX_ROWS": "80",
    "REGION_TALK_SOURCE_QUEUE_HANDOFF_PERSIST_REORDERED_TAIL": "0",
    "REGION_TALK_WRITE_SOURCE_STATUS_QUEUE_MIRROR": "0",
    "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_CANDIDATES": "120",
    "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_SOURCE_EDGES": "120",
    "REGION_TALK_YDB_ONLINE_DISCOVERY_MAX_COMMENT_LINKS": "40",
    # Keep main runs short enough to reach the discovery tail, but still gentle
    # toward Telegram: a few similar-channel seeds and a few travel-intent
    # keyword queries per run are enough to make the public/source frontier grow
    # without filling the queue with local Kaliningrad-only publics.
    "REGION_TALK_HISTORY_SOURCES_TARGET": "6",
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
    "REGION_TALK_FETCH_POST_LINK_QUEUE_FIRST": "1",
    "REGION_TALK_POST_LINK_QUEUE_FETCH_LIMIT": "3",
    "REGION_TALK_TG_CACHED_ENTITY_ONLY": "1",
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
    "text_vector_enrichment_item": 6000,
    "processed_post_item": 20000,
    "post_live_item": 20000,
    "source_queue_item": 6000,
    "source_status_item": 6000,
    "online_source_item": 6000,
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
    if kind in {"processed_post_item", "post_live_item"}:
        # Processed/live post totals are goal metrics. They must not be silently
        # flattened by a low source/frontier debug --limit, otherwise the loop
        # can report zero processed-post progress while CandidateReport is
        # actually fetching/writing posts.
        return max(1, cap)
    return max(1, min(max(1, int(requested_limit)), cap))


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
        row.get("source_id")
        or row.get("canonical_source_key")
        or row.get("source_url")
        or row.get("canonical_url")
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


def _text_vector_pair_metrics(e5_vectors: list[dict[str, Any]], bge_vectors: list[dict[str, Any]]) -> dict[str, int]:
    e5_post = {k for k in (_vector_post_key(r) for r in e5_vectors) if k}
    bge_post = {k for k in (_vector_post_key(r) for r in bge_vectors) if k}
    e5_exact = {k for k in (_vector_exact_text_key(r) for r in e5_vectors) if k}
    bge_exact = {k for k in (_vector_exact_text_key(r) for r in bge_vectors) if k}
    post_paired = e5_post & bge_post
    exact_paired = e5_exact & bge_exact
    return {
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


def _image_queue_status_metrics(images: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "image_not_reviewable_no_media_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "not_reviewable_no_media"),
        "image_not_reviewable_unsupported_media_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "not_reviewable_unsupported_media"),
        "image_rejected_text_gate_total": sum(1 for r in images if str(r.get("image_queue_status") or "") == "rejected_text_gate"),
    }


def _is_keyword_discovered_source(row: dict[str, Any]) -> bool:
    haystack = " ".join(str(row.get(k) or "") for k in [
        "added_from", "insertion_policy", "discovery_type", "edge_type", "frontier_reason",
        "matched_query", "matched_hashtag", "keyword_hit_post_url", "keyword_evidence_excerpt",
    ]).lower()
    return "keyword" in haystack or "hashtag" in haystack or "telegram_keyword_search" in haystack


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


def _keyword_source_metrics(
    source_rows: list[dict[str, Any]],
    cursor_position: int,
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
    keyword_edge_target_keys = {k for k in keyword_edge_target_keys if k}
    keyword_evidence_keys = keyword_row_keys | keyword_edge_target_keys
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
        "publics_keyword_edge_targets_total": len(keyword_edge_target_keys),
        "publics_keyword_queue_missing_total": len(keyword_evidence_keys - set(source_by_key)),
        "publics_keyword_fake_processed_without_scan_evidence_total": len(keyword_fake_processed_without_scan_keys),
        "publics_keyword_scanned_with_posts_total": len(keyword_scanned_keys),
        "publics_keyword_with_ko_candidates_total": len(keyword_ko_keys),
        "publics_keyword_pending_after_cursor_total": len(keyword_pending_after_cursor_keys),
        "publics_keyword_ko_yield_percent": int(round((len(keyword_ko_keys) / len(keyword_scanned_keys)) * 100)) if keyword_scanned_keys else 0,
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
        return [actions[0]]
    max_count = max(1, int(max_actions or 1))
    if str(actions[0].get("action") or "") == "stop":
        return [actions[0]]
    selected: list[dict[str, Any]] = []
    used_resources: set[str] = set()
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


GOAL_DELTA_METRICS = {
    "new_publics": "publics_total",
    "processed_posts": "processed_posts_unique_total",
    "ko_sources": "publics_with_ko_candidates_total",
    "image_queue": "image_queue_total",
    "publication_candidates": "publication_candidate_total",
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
            "candidate_memory_item",
            "image_queue_item",
            "publication_candidate_item",
            "post_link_queue_item",
            "text_vector_enrichment_item",
            "processed_post_item",
            "post_live_item",
            "queue_cursor",
        ]
        rows_by_kind = {
            kind: read_kind_rows(pool, ydb, table, kind, _orchestrator_kind_limit(kind, limit))
            for kind in kinds
        }
    finally:
        driver.stop()

    candidates = rows_by_kind["candidate_memory_item"]
    images = rows_by_kind["image_queue_item"]
    publications = rows_by_kind["publication_candidate_item"]
    post_links = rows_by_kind["post_link_queue_item"]
    vectors = rows_by_kind["text_vector_enrichment_item"]
    source_candidates = rows_by_kind["source_candidate_item"]
    source_edges = rows_by_kind["source_edge_item"]
    comment_links = rows_by_kind["comment_link_item"]
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

    image_pending = [r for r in images if str(r.get("image_queue_status") or "") in {"", "needs_actual_image_fetch", "selected_for_next_image_batch"}]
    image_in_progress = [r for r in images if str(r.get("image_queue_status") or "") == "image_analysis_in_progress"]
    image_actual = [r for r in images if str(r.get("image_queue_status") or "") == "actual_scored" and str(r.get("image_model_input_type") or "") == "actual_image"]
    image_terminal_metrics = _image_queue_status_metrics(images)
    confirmed = [r for r in publications if is_confirmed_publication(r)]
    unsent_confirmed = [r for r in publications if is_unsent_confirmed_publication(r)]
    sent = [r for r in publications if str(r.get("sent_to_chat") or "").lower() == "true" or str(r.get("publication_candidate_status") or "") == "sent_to_chat"]
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
    processed_post_source_keys = {k for k in (_post_source_merge_key(r) for r in processed_post_rows) if k}
    source_with_posts = [r for r in source_rows if _safe_int(r.get("posts_scanned")) > 0]
    source_with_posts_count = max(len(source_with_posts), len(processed_post_source_keys))
    cursor_by_name: dict[str, dict[str, Any]] = {}
    for row in cursors:
        name = str(row.get("queue_name") or row.get("_ydb_pk") or "").replace("queue_cursor:", "")
        if name and ":" not in name:
            cursor_by_name[name] = row
    source_cursor_position = _safe_int((cursor_by_name.get("unified_source_queue") or cursor_by_name.get("source_scan") or {}).get("cursor_position") or 0)
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
    keyword_post_regex_metrics = _keyword_source_post_regex_metrics(source_rows, diagnostic_post_rows, source_candidates, source_edges)
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
    publication_ready = [
        r for r in publications
        if str(r.get("publication_candidate_status") or "") in {"publication_ready", "accepted_for_publication"}
        or str(r.get("publication_status") or "") == "publication_ready"
    ]
    post_link_statuses = [str(r.get("post_link_status") or "") for r in post_links]
    post_link_pending = [r for r, status in zip(post_links, post_link_statuses) if status in {"", "pending_fetch", "retry_fetch", "fetch_error"}]
    post_link_fetched = [r for r, status in zip(post_links, post_link_statuses) if status == "fetched"]
    post_link_terminal = [r for r, status in zip(post_links, post_link_statuses) if status.startswith("terminal_")]
    post_link_source_keys = {str(r.get("canonical_source_key") or r.get("source_key") or "") for r in post_links if str(r.get("canonical_source_key") or r.get("source_key") or "").strip()}
    fast_check_rows = [r for r in source_rows if str(r.get("fast_check_status") or "").strip()]
    fast_check_hit_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "ko_hit"]
    fast_check_no_hit_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "no_hit"]
    fast_check_local_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "local_region_source"]
    fast_check_spam_rows = [r for r in fast_check_rows if str(r.get("fast_check_status") or "") == "spam_source_reject"]
    source_posts_scanned_raw_total = sum(_safe_int(r.get("posts_scanned")) for r in source_rows)
    processed_posts_unique_total = len(processed_post_rows)
    source_posts_scanned_effective_total = max(source_posts_scanned_raw_total, processed_posts_unique_total)
    latest_source_scan_run = max([str(r.get("last_scan_run_id") or "") for r in source_rows if str(r.get("last_scan_run_id") or "").strip()] or [""])
    latest_source_scan_rows = [
        r for r in source_rows
        if latest_source_scan_run and str(r.get("last_scan_run_id") or "") == latest_source_scan_run and _source_has_scan_evidence(r)
    ]
    latest_source_scan_posts = sum(_safe_int(r.get("posts_scanned")) for r in latest_source_scan_rows)
    history_depth_rows = [r for r in source_rows if _safe_float(r.get("history_avg_post_age_days")) is not None]
    latest_history_run = max([str(r.get("last_scan_run_id") or r.get("run_id") or "") for r in history_depth_rows] or [""])
    latest_history_depth_rows = [
        r for r in history_depth_rows
        if latest_history_run and str(r.get("last_scan_run_id") or r.get("run_id") or "") == latest_history_run
    ]

    metrics = {
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
        **keyword_post_regex_metrics,
        "source_candidates_total": len(source_candidates),
        "source_edges_total": len(source_edges),
        "comment_link_rows_total": len(comment_links),
        "post_link_queue_total": len(post_links),
        "post_link_queue_pending_total": len(post_link_pending),
        "post_link_queue_fetched_total": len(post_link_fetched),
        "post_link_queue_terminal_total": len(post_link_terminal),
        "post_link_queue_unique_sources_total": len(post_link_source_keys),
        "post_link_queue_keyword_total": sum(1 for r in post_links if "keyword" in str(r.get("priority_reason") or r.get("discovery_type") or "")),
        "post_link_queue_hashtag_total": sum(1 for r in post_links if "hashtag" in str(r.get("priority_reason") or r.get("discovery_type") or "")),
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
        "history_depth_sources_total": len(history_depth_rows),
        "history_depth_latest_run_sources_total": len(latest_history_depth_rows),
        "history_avg_post_age_days_avg": _avg_numeric(history_depth_rows, "history_avg_post_age_days"),
        "history_newest_post_age_days_min": _min_numeric(history_depth_rows, "history_newest_post_age_days"),
        "history_oldest_post_age_days_max": _max_numeric(history_depth_rows, "history_oldest_post_age_days"),
        "history_latest_run_avg_post_age_days_avg": _avg_numeric(latest_history_depth_rows, "history_avg_post_age_days"),
        "history_latest_run_newest_post_age_days_min": _min_numeric(latest_history_depth_rows, "history_newest_post_age_days"),
        "history_latest_run_oldest_post_age_days_max": _max_numeric(latest_history_depth_rows, "history_oldest_post_age_days"),
        "processed_posts_unique_total": processed_posts_unique_total,
        "candidate_memory_total": len(candidates),
        "image_queue_total": len(images),
        "image_pending_total": len(image_pending),
        "image_in_progress_total": len(image_in_progress),
        "image_actual_scored_total": len(image_actual),
        **image_terminal_metrics,
        "image_strong_actual_ge_0_66_total": len(strong_images_ge_066),
        "image_strong_actual_ge_0_70_total": len(strong_images),
        "publication_candidate_total": len(publications),
        "publication_ready_total": len(publication_ready),
        "publication_confirmed_total": len(confirmed),
        "publication_sent_total": len(sent),
        "publication_unsent_confirmed_total": len(unsent_confirmed),
        "text_vector_enrichment_total": len(vectors),
        "text_vector_e5_total": len(e5_vectors),
        "text_vector_bge_m3_total": len(bge_vectors),
        **vector_pair_metrics,
        **regex_vector_metrics,
        "bge_pending_sample_total": len(bge_pending_rows),
        "bge_pending_sample_limit": bge_sample_limit,
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
    if int(metrics.get("publication_sent_total") or 0) >= target_confirmed or int(metrics.get("publication_confirmed_total") or 0) >= target_confirmed:
        return [{"action": "stop", "reason": "target_confirmed_reached"}]
    actions: list[dict[str, Any]] = []
    if int(metrics.get("publication_unsent_confirmed_total") or 0) > 0:
        actions.append(_action("notify_confirmed", ["python3", "scripts/region_talk_goal_notify.py", "--limit", "20"], "confirmed rows not sent to operator chat", resource="telegram:e2e", timeout_seconds=180))

    # Pipeline invariant: after the main notebook writes fresh E5 rows, BGE is
    # the next required consumer. Do not let finalizer/image actions hide this
    # backlog; BGE has no Telegram session and can run in parallel with both
    # CandidateReport(DISCOVERY1) and ImageDiagnostic(DISCOVERY2).
    bge_backlog = max(
        int(metrics.get("text_vector_e5_without_bge_exact_text_total") or 0),
        int(metrics.get("bge_pending_sample_total") or 0),
    )
    if bge_backlog >= bge_threshold:
        bge_batch_limit = _env_int("REGION_TALK_ORCHESTRATOR_BGE_BATCH_LIMIT", 24)
        bge_batch_size = _env_int("REGION_TALK_ORCHESTRATOR_BGE_BATCH_SIZE", 4)
        actions.append(_action(
            "launch_bge_m3",
            [
                "python3", "kaggle/execute_region_talk_bge_m3_enrichment.py",
                "--batch-limit", str(max(1, bge_batch_limit)),
                "--batch-size", str(max(1, bge_batch_size)),
                "--no-wait",
            ],
            "pending E5 text-vector rows need paired BGE enrichment immediately after main E5",
            resource="kaggle:bge_m3",
            parallel_safe=True,
            timeout_seconds=300,
            env={
                "REGION_TALK_BGE_E5_ONLY": "1",
                "REGION_TALK_BGE_INPUT_KINDS": "text_vector_enrichment_item",
                "REGION_TALK_BGE_YDB_SCAN_LIMIT": "6000",
            },
        ))
    if int(metrics.get("image_pending_total") or 0) >= image_threshold:
        actions.append(_action(
            "launch_image_diagnostic",
            ["python3", "kaggle/execute_region_talk_image_diagnostic.py", "--source", "ydb", "--max-items-per-run", "30", "--batch-size", "10", "--no-wait"],
            "text-confirmed image queue has pending rows; uses DISCOVERY2",
            resource="telegram:DISCOVERY2",
            parallel_safe=True,
            timeout_seconds=300,
        ))
    if include_main:
        actions.append(_action(
            "launch_candidate_report",
            ["python3", "kaggle/execute_region_talk_candidate_report.py", "--max-sources", "6", "--no-wait"],
            "continue main discovery/E5 producer in parallel when DISCOVERY1 is free",
            resource="telegram:DISCOVERY1",
            parallel_safe=True,
            timeout_seconds=300,
            env=MAIN_DISCOVERY_YDB_BUDGET_ENV,
        ))
    if int(metrics.get("image_actual_scored_total") or 0) > int(metrics.get("publication_candidate_total") or 0):
        actions.append(_action("run_finalizer", ["python3", "scripts/region_talk_publication_finalizer.py", "--max-llm", "3"], "actual images exist beyond publication rows", resource="local:gemini", timeout_seconds=900))
    if not actions:
        actions.append(_action("launch_candidate_report", ["python3", "kaggle/execute_region_talk_candidate_report.py", "--max-sources", "6", "--no-wait"], "produce new E5/discovery rows", resource="telegram:DISCOVERY1", parallel_safe=True, timeout_seconds=300, env=MAIN_DISCOVERY_YDB_BUDGET_ENV))
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
        result["stats_message"] = build_stats_message(limit=args.limit)
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
    parser.add_argument("--no-main", action="store_true", help="do not include CandidateReport producer in the plan")
    parser.add_argument("--execute", action="store_true", help="execute the first planned action (default: dry-run only)")
    parser.add_argument("--execute-ready", action="store_true", help="execute all non-conflicting parallel-safe launch actions in this cycle")
    parser.add_argument("--max-actions-per-cycle", type=int, default=4)
    parser.add_argument("--skip-kaggle-status", action="store_true", help="do not query Kaggle active kernel statuses before planning")
    parser.add_argument("--allow-unverified-kaggle-status", action="store_true", help="allow Kaggle launches even when a Region Talk kernel status cannot be verified")
    parser.add_argument("--loop", action="store_true", help="keep polling YDB/Kaggle and launching ready work until target/limits")
    parser.add_argument("--cycle-sleep-seconds", type=int, default=180, help="sleep between loop cycles")
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
        time.sleep(max(5, int(args.cycle_sleep_seconds or 180)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
