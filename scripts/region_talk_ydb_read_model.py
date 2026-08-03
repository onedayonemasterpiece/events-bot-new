"""Versioned, bounded Region Talk YDB work queue and counter read model.

The historical compact KV remains the product ledger.  This module adds three
small projections whose primary keys match the normal access patterns:

* ``*_work_queue_v2`` stores only actionable work.  Its key starts with the
  immutable generation, queue name and due time, so a due-page is a bounded
  key-prefix/range read rather than a population scan.
* ``*_work_cursor_v2`` stores the committed keyset cursor and one renewable
  claim lease per immutable generation/queue.  A claim never advances the
  committed cursor; only a token-checked ACK does.
* ``*_read_model_v1`` stores one validated counter/observability row.

The projection is replace-by-generation.  Writers publish all rows for a new
generation before changing the singleton ``current`` pointer.  Readers never
mix generations.  Old generations are maintenance data and are not read by the
normal path.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from scripts.region_talk_ydb_cost import YdbCostBudget, payload_size_bytes


READ_MODEL_SCHEMA_VERSION = "region-talk-ydb-read-model-v1"
WORK_QUEUE_SCHEMA_VERSION = "region-talk-ydb-work-queue-v2"
WORK_CURSOR_SCHEMA_VERSION = "region-talk-ydb-work-cursor-v2"
READ_MODEL_NAME = "current"
CUTOVER_MODES = frozenset({"required", "shadow", "legacy"})

# A missing key must not be interpreted as zero by the autonomous decision
# planner.  Every ready read model explicitly owns this bounded decision set.
REQUIRED_DECISION_METRICS = (
    "post_link_queue_exact_ready_total",
    "post_link_queue_bge_ready_rescore_total",
    "post_link_queue_source_terminal_cleanup_total",
    "image_pending_total",
    "image_scoring_retry_total",
    "image_contract_rescore_backlog_total",
    "image_vlm_backlog_total",
    "image_actionable_work_total",
    "bge_missing_current_sample_total",
    "bge_immediate_pair_backlog_total",
    "finalizer_pending_url_total",
    "publication_unsent_confirmed_total",
    "publication_onboarding_pending_unsent_total",
    "publication_source_evidence_backlog_total",
)


class YdbReadModelUnavailable(RuntimeError):
    """The bounded projection is absent, stale, invalid, or not cut over."""


def _safe_name(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_]+", "_", str(value or "").strip()).strip("_")
    if not name:
        raise ValueError("region_talk_ydb_read_model:namespace_missing")
    return name


def table_paths(database: str, namespace: str) -> tuple[str, str]:
    root = str(database or "").strip().rstrip("/")
    if not root:
        raise ValueError("region_talk_ydb_read_model:database_missing")
    prefix = _safe_name(namespace)
    return (
        f"{root}/{prefix}_work_queue_v2",
        f"{root}/{prefix}_read_model_v1",
    )


def cursor_table_path(work_table_path: str) -> str:
    if not str(work_table_path).endswith("_work_queue_v2"):
        raise ValueError("region_talk_ydb_read_model:work_table_version_mismatch")
    return str(work_table_path)[:-len("_work_queue_v2")] + "_work_cursor_v2"


def cutover_mode(env: Mapping[str, str] | None = None) -> str:
    values = env if env is not None else os.environ
    mode = str(values.get("REGION_TALK_YDB_READ_MODEL_MODE") or "required").strip().lower()
    if mode not in CUTOVER_MODES:
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:invalid_cutover_mode")
    return mode


def legacy_fallback_allowed(env: Mapping[str, str] | None = None) -> bool:
    """Broad compatibility reads require two explicit choices.

    ``shadow`` alone is not permission to scan.  An operator/test must also set
    ``REGION_TALK_YDB_ALLOW_LEGACY_BROAD_READ_FALLBACK=1``; L1's budget still
    guards every request and result.
    """

    values = env if env is not None else os.environ
    flag = str(values.get("REGION_TALK_YDB_ALLOW_LEGACY_BROAD_READ_FALLBACK") or "0").lower()
    return cutover_mode(values) in {"shadow", "legacy"} and flag in {"1", "true", "yes", "on"}


def stable_generation(run_id: Any, updated_at: Any) -> str:
    raw = f"{str(run_id or '').strip()}\n{str(updated_at or '').strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class WorkItem:
    generation: str
    queue_name: str
    status: str
    due_at: str
    priority: int
    item_key: str
    state_pk: str
    payload: dict[str, Any]

    def as_row(self, *, updated_at: str) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "queue_name": self.queue_name,
            "status": self.status,
            "due_at": self.due_at,
            "priority": max(0, int(self.priority)),
            "item_key": self.item_key,
            "state_pk": self.state_pk,
            "payload_json": json.dumps(self.payload, ensure_ascii=False, sort_keys=True),
            "updated_at": updated_at,
        }


def _rows(collection: Any) -> list[dict[str, Any]]:
    if isinstance(collection, dict):
        return [row for row in collection.values() if isinstance(row, dict)]
    if isinstance(collection, list):
        return [row for row in collection if isinstance(row, dict)]
    return []


def _first(row: Mapping[str, Any], fields: Iterable[str]) -> str:
    for field in fields:
        value = str(row.get(field) or "").strip()
        if value:
            return value
    return ""


def _priority(row: Mapping[str, Any]) -> int:
    for field in ("priority", "queue_order", "queue_seq", "image_queue_order", "list_order"):
        try:
            return max(0, int(float(row.get(field) or 0)))
        except (TypeError, ValueError):
            continue
    return 0


def _work_spec(collection_name: str, row: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    """Return queue/status/item-key/state-PK for actionable product rows."""

    if collection_name == "unified_source_queue":
        status = _first(row, ("source_queue_status", "queue_status", "fetch_status")) or "pending_scan"
        if status not in {"pending_scan", "needs_rescan_or_retry", "retry", "error"} and not status.startswith("skipped_telegram_unresolved"):
            return None
        key = _first(row, ("canonical_source_key", "source_queue_id", "source_id"))
        return ("source", status, key, "source_queue_item:" + key) if key else None
    if collection_name == "post_link_queue":
        status = _first(row, ("post_link_status", "fetch_status")) or "pending_fetch"
        if status not in {"pending_fetch", "retry_fetch", "fetch_error", "retry_wait_entity_cache", "bge_ready_rescore"}:
            return None
        key = _first(row, ("post_link_queue_id", "post_url", "keyword_hit_post_url"))
        return ("post_link", status, key, "post_link_queue_item:" + key) if key else None
    if collection_name == "image_candidate_queue":
        status = _first(row, ("image_queue_status", "image_quality_decision")) or "needs_actual_image_fetch"
        if status not in {"needs_actual_image_fetch", "selected_for_next_image_batch", "scoring_retry", "image_analysis_in_progress", "needs_visual_review"}:
            return None
        key = _first(row, ("image_queue_id", "post_url", "post_id"))
        return ("image", status, key, "image_queue_item:" + key) if key else None
    if collection_name == "candidate_memory":
        vector_status = _first(row, ("vector_gate_status", "text_vector_fusion_status", "current_stage"))
        if vector_status not in {"vector_defer_wait_bge_m3", "missing_bge_m3_enrichment", "dual_model_vector_enrichment_pending"}:
            return None
        key = _first(row, ("candidate_memory_id", "post_id", "post_url"))
        return ("bge", vector_status, key, "candidate_memory_item:" + key) if key else None
    if collection_name == "publication_candidate_queue":
        status = _first(row, ("publication_status", "publication_candidate_status")) or "pending"
        sent = str(row.get("sent_to_chat") or "").lower() == "true"
        if sent or status in {"rejected", "published", "sent_to_chat", "expired"} or status.startswith("operator_rejected"):
            return None
        key = _first(row, ("publication_candidate_id", "post_url", "post_id"))
        return ("publication", status, key, "publication_candidate_item:" + key) if key else None
    return None


def build_work_items(
    state: Mapping[str, Any],
    *,
    max_per_queue: int = 500,
) -> list[WorkItem]:
    """Materialize bounded active work from an already in-memory full state."""

    generation = stable_generation(state.get("run_id"), state.get("updated_at"))
    maximum = max(1, int(max_per_queue))
    selected: list[WorkItem] = []
    per_queue: Counter[str] = Counter()
    collections = (
        "unified_source_queue",
        "post_link_queue",
        "image_candidate_queue",
        "candidate_memory",
        "publication_candidate_queue",
    )
    for collection in collections:
        candidates: list[tuple[tuple[int, str, str], WorkItem]] = []
        for row in _rows(state.get(collection)):
            spec = _work_spec(collection, row)
            if spec is None:
                continue
            queue_name, status, item_key, state_pk = spec
            due_at = _first(row, ("next_fetch_after", "retry_after", "next_attempt_after", "updated_at"))
            payload = {
                key: row.get(key)
                for key in (
                    "canonical_source_key", "post_url", "post_id", "source_id",
                    "candidate_memory_id", "publication_candidate_id", "image_queue_id",
                )
                if row.get(key) not in (None, "")
            }
            work = WorkItem(
                generation=generation,
                queue_name=queue_name,
                status=status,
                due_at=due_at,
                priority=_priority(row),
                item_key=item_key,
                state_pk=state_pk,
                payload=payload,
            )
            candidates.append(((work.due_at, work.priority, work.status, work.item_key), work))
        for _sort, work in sorted(candidates, key=lambda item: item[0])[:maximum]:
            selected.append(work)
            per_queue[work.queue_name] += 1
    return selected


def _decision_metrics(state: Mapping[str, Any], work_items: Iterable[WorkItem]) -> dict[str, Any]:
    # Work rows are intentionally capped.  Decision counters are not: the
    # producer already owns the full state in memory, so aggregate the complete
    # collections once and never publish page-size values as population truth.
    post_links = _rows(state.get("post_link_queue"))
    images = _rows(state.get("image_candidate_queue"))
    candidates = _rows(state.get("candidate_memory"))
    publications = _rows(state.get("publication_candidate_queue"))
    post_statuses = Counter(_first(row, ("post_link_status", "fetch_status")) or "pending_fetch" for row in post_links)
    image_statuses = Counter(_first(row, ("image_queue_status", "image_quality_decision")) or "needs_actual_image_fetch" for row in images)
    vector_statuses = Counter(_first(row, ("vector_gate_status", "text_vector_fusion_status", "current_stage")) for row in candidates)
    active_publications = [
        row for row in publications
        if str(row.get("sent_to_chat") or "").lower() != "true"
        and not (_first(row, ("publication_status", "publication_candidate_status"))).startswith("operator_rejected")
        and _first(row, ("publication_status", "publication_candidate_status")) not in {"rejected", "published", "sent_to_chat", "expired"}
    ]
    publication_statuses = Counter(
        _first(row, ("publication_status", "publication_candidate_status")) or "pending"
        for row in active_publications
    )
    metrics: dict[str, Any] = {}
    for source in (state.get("all_time_metrics"), state.get("run_funnel_metrics")):
        if isinstance(source, dict):
            metrics.update(source)
    metrics.update({
        "post_link_queue_exact_ready_total": sum(
            post_statuses[status]
            for status in {"pending_fetch", "retry_fetch", "fetch_error", "retry_wait_entity_cache"}
        ),
        "post_link_queue_bge_ready_rescore_total": post_statuses["bge_ready_rescore"],
        "post_link_queue_source_terminal_cleanup_total": 0,
        "image_pending_total": sum(
            image_statuses[status]
            for status in {"needs_actual_image_fetch", "selected_for_next_image_batch"}
        ),
        "image_scoring_retry_total": image_statuses["scoring_retry"],
        "image_contract_rescore_backlog_total": 0,
        "image_vlm_backlog_total": image_statuses["needs_visual_review"],
        "bge_missing_current_sample_total": sum(
            vector_statuses[status]
            for status in {"vector_defer_wait_bge_m3", "missing_bge_m3_enrichment", "dual_model_vector_enrichment_pending"}
        ),
        "bge_immediate_pair_backlog_total": sum(
            vector_statuses[status]
            for status in {"vector_defer_wait_bge_m3", "missing_bge_m3_enrichment", "dual_model_vector_enrichment_pending"}
        ),
        "finalizer_pending_url_total": len(active_publications),
        "publication_unsent_confirmed_total": sum(
            publication_statuses[status] for status in {"gemini_accept", "llm_confirmed"}
        ),
        "publication_onboarding_pending_unsent_total": sum(
            publication_statuses[status] for status in {"gemini_accept", "needs_source_profile", "needs_review"}
        ),
        "publication_source_evidence_backlog_total": sum(
            publication_statuses[status] for status in {"needs_source_evidence", "needs_source_profile"}
        ),
    })
    metrics["image_actionable_work_total"] = (
        int(metrics["image_pending_total"])
        + int(metrics["image_scoring_retry_total"])
        + int(metrics["image_contract_rescore_backlog_total"])
        + int(metrics["image_vlm_backlog_total"])
    )
    for key in REQUIRED_DECISION_METRICS:
        metrics.setdefault(key, 0)
    return metrics


def build_read_model(
    state: Mapping[str, Any],
    work_items: Iterable[WorkItem],
    *,
    cutover_state: str = "ready",
) -> dict[str, Any]:
    work = list(work_items)
    generation = stable_generation(state.get("run_id"), state.get("updated_at"))
    counts = Counter(item.queue_name for item in work)
    collection_queue = {
        "unified_source_queue": "source",
        "post_link_queue": "post_link",
        "image_candidate_queue": "image",
        "candidate_memory": "bge",
        "publication_candidate_queue": "publication",
    }
    expected_counts: Counter[str] = Counter()
    for collection, queue_name in collection_queue.items():
        expected_counts[queue_name] = sum(
            1 for row in _rows(state.get(collection))
            if _work_spec(collection, row) is not None
        )
    counts_complete = all(counts[name] == expected_counts[name] for name in collection_queue.values())
    input_complete = state.get("_ydb_materialized_projection_input_complete", True) is True
    complete = counts_complete and input_complete
    previous_population = state.get("ydb_read_model_population_totals")
    if not isinstance(previous_population, dict):
        previous_population = {}
    current_population = {
        "sources": len(_rows(state.get("unified_source_queue"))),
        "processed_posts": len(_rows(state.get("processed_posts") or state.get("posts"))),
        "candidate_memory": len(_rows(state.get("candidate_memory"))),
        "images": len(_rows(state.get("image_candidate_queue"))),
        "publications": len(_rows(state.get("publication_candidate_queue"))),
        "post_links": len(_rows(state.get("post_link_queue"))),
    }
    population_totals = {
        key: max(int(previous_population.get(key) or 0), value)
        for key, value in current_population.items()
    }
    def positive_int(value: Any) -> int:
        try:
            return max(0, int(float(value or 0)))
        except (TypeError, ValueError):
            return 0
    source_rows = _rows(state.get("unified_source_queue"))
    source_queue_max_seq = max(
        [positive_int(row.get("queue_seq") or row.get("queue_order")) for row in source_rows]
        + [positive_int(state.get("ydb_read_model_source_queue_max_seq"))]
    )
    source_queue_max_order = max(
        [positive_int(row.get("queue_order")) for row in source_rows]
        + [positive_int(state.get("ydb_read_model_source_queue_max_order"))]
    )
    return {
        "schema_version": READ_MODEL_SCHEMA_VERSION,
        "work_queue_schema_version": WORK_QUEUE_SCHEMA_VERSION,
        "model_name": READ_MODEL_NAME,
        "cutover_state": cutover_state if complete else (
            "blocked_overflow" if not counts_complete else "blocked_incomplete_input"
        ),
        "generation": generation,
        "source_run_id": str(state.get("run_id") or ""),
        "updated_at": str(state.get("updated_at") or ""),
        "work_counts": dict(sorted(counts.items())),
        "expected_work_counts": dict(sorted(expected_counts.items())),
        "work_queue_complete": complete,
        "work_queue_input_complete": input_complete,
        "work_total": len(work),
        "source_queue_max_seq": source_queue_max_seq,
        "source_queue_max_order": source_queue_max_order,
        "population_totals": population_totals,
        "metrics": _decision_metrics(state, work),
    }


def validate_read_model(payload: Any, *, require_ready: bool = True) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:payload_missing")
    if payload.get("schema_version") != READ_MODEL_SCHEMA_VERSION:
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:schema_mismatch")
    if payload.get("work_queue_schema_version") != WORK_QUEUE_SCHEMA_VERSION:
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:work_schema_mismatch")
    if require_ready and payload.get("cutover_state") != "ready":
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:not_ready")
    if payload.get("work_queue_complete") is not True:
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:work_queue_incomplete")
    if not str(payload.get("generation") or ""):
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:generation_missing")
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:metrics_missing")
    missing = [key for key in REQUIRED_DECISION_METRICS if key not in metrics]
    if missing:
        raise YdbReadModelUnavailable("region_talk_ydb_read_model:decision_metrics_incomplete")
    return payload


def work_table_ddl(table_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{table_path}` (
  generation Utf8 NOT NULL,
  queue_name Utf8 NOT NULL,
  status Utf8 NOT NULL,
  due_at Utf8 NOT NULL,
  priority Uint64 NOT NULL,
  item_key Utf8 NOT NULL,
  state_pk Utf8,
  payload_json Json,
  updated_at Utf8,
  PRIMARY KEY (generation, queue_name, due_at, priority, status, item_key)
);"""


def work_cursor_table_ddl(table_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{table_path}` (
  generation Utf8 NOT NULL,
  queue_name Utf8 NOT NULL,
  expected_count Uint64 NOT NULL,
  consumed_count Uint64 NOT NULL,
  cursor_due_at Utf8 NOT NULL,
  cursor_priority Uint64 NOT NULL,
  cursor_status Utf8 NOT NULL,
  cursor_item_key Utf8 NOT NULL,
  claim_due_at Utf8,
  claim_priority Uint64,
  claim_status Utf8,
  claim_item_key Utf8,
  claim_count Uint64,
  lease_owner Utf8,
  lease_token Utf8,
  lease_expires_at Utf8,
  updated_at Utf8,
  PRIMARY KEY (generation, queue_name)
);"""


def read_model_table_ddl(table_path: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{table_path}` (
  model_name Utf8 NOT NULL,
  schema_version Utf8,
  generation Utf8,
  cutover_state Utf8,
  payload_json Json,
  updated_at Utf8,
  PRIMARY KEY (model_name)
);"""


def read_model_query(table_path: str) -> str:
    return f"SELECT payload_json FROM `{table_path}` WHERE model_name = '{READ_MODEL_NAME}';"


def work_page_query(table_path: str, *, limit: int) -> str:
    return f"""DECLARE $generation AS Utf8;
DECLARE $queue_name AS Utf8;
DECLARE $due_cutoff AS Utf8;
DECLARE $cursor_due_at AS Utf8;
DECLARE $cursor_priority AS Uint64;
DECLARE $cursor_status AS Utf8;
DECLARE $cursor_item_key AS Utf8;
SELECT generation, queue_name, status, due_at, priority, item_key, state_pk, payload_json, updated_at
FROM `{table_path}`
WHERE generation = $generation AND queue_name = $queue_name
  AND due_at <= $due_cutoff
  AND (
    due_at > $cursor_due_at
    OR (due_at = $cursor_due_at AND priority > $cursor_priority)
    OR (due_at = $cursor_due_at AND priority = $cursor_priority AND status > $cursor_status)
    OR (due_at = $cursor_due_at AND priority = $cursor_priority AND status = $cursor_status AND item_key > $cursor_item_key)
  )
ORDER BY generation, queue_name, due_at, priority, status, item_key
LIMIT {max(1, int(limit))};"""


def read_current_model(
    pool: Any,
    ydb: Any,
    table_path: str,
    *,
    budget: YdbCostBudget,
) -> dict[str, Any]:
    def op(session: Any) -> dict[str, Any]:
        budget.before_query("read_model.current")
        result_sets = session.transaction(ydb.SnapshotReadOnly()).execute(
            read_model_query(table_path), commit_tx=True,
        )
        rows = result_sets[0].rows if result_sets else []
        budget.record_read(
            "read_model.current", len(rows),
            sum(payload_size_bytes(getattr(row, "payload_json", "")) for row in rows),
        )
        if not rows:
            raise YdbReadModelUnavailable("region_talk_ydb_read_model:current_missing")
        raw = rows[0].payload_json
        payload = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
        return validate_read_model(payload)

    return pool.retry_operation_sync(op)


def read_work_page(
    pool: Any,
    ydb: Any,
    table_path: str,
    *,
    generation: str,
    queue_name: str,
    limit: int,
    budget: YdbCostBudget,
    due_cutoff: str = "9999-12-31T23:59:59Z",
    cursor: tuple[str, int, str, str] = ("", 0, "", ""),
) -> list[dict[str, Any]]:
    maximum = max(1, int(limit))

    def op(session: Any) -> list[dict[str, Any]]:
        budget.before_query(f"read_model.work:{queue_name}")
        query = session.prepare(work_page_query(table_path, limit=maximum))
        result_sets = session.transaction(ydb.SnapshotReadOnly()).execute(
            query,
            {
                "$generation": generation,
                "$queue_name": queue_name,
                "$due_cutoff": str(due_cutoff),
                "$cursor_due_at": str(cursor[0]),
                "$cursor_priority": max(0, int(cursor[1])),
                "$cursor_status": str(cursor[2]),
                "$cursor_item_key": str(cursor[3]),
            },
            commit_tx=True,
        )
        rows = result_sets[0].rows if result_sets else []
        out = [
            {
                "generation": str(getattr(row, "generation", "") or ""),
                "queue_name": str(getattr(row, "queue_name", "") or ""),
                "status": str(getattr(row, "status", "") or ""),
                "due_at": str(getattr(row, "due_at", "") or ""),
                "priority": int(getattr(row, "priority", 0) or 0),
                "item_key": str(getattr(row, "item_key", "") or ""),
                "state_pk": str(getattr(row, "state_pk", "") or ""),
                "payload_json": getattr(row, "payload_json", "") or "",
                "updated_at": str(getattr(row, "updated_at", "") or ""),
            }
            for row in rows
        ]
        budget.record_read("read_model.work:" + queue_name, len(out), sum(payload_size_bytes(row) for row in out))
        return out

    return list(pool.retry_operation_sync(op) or [])
