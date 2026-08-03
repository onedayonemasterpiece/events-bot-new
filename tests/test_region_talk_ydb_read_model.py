from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from scripts.region_talk_ydb_cost import YdbCostBudget
from scripts.region_talk_ydb_read_model import (
    READ_MODEL_SCHEMA_VERSION,
    WORK_QUEUE_SCHEMA_VERSION,
    YdbReadModelUnavailable,
    build_read_model,
    build_work_items,
    cursor_table_path,
    legacy_fallback_allowed,
    read_current_model,
    validate_read_model,
    work_page_query,
    work_cursor_table_ddl,
    work_table_ddl,
)


ROOT = Path(__file__).parents[1]


def _budget() -> YdbCostBudget:
    return YdbCostBudget(
        max_queries=64,
        max_rows_read=5000,
        max_bytes_read=32 * 1024 * 1024,
        max_rows_written=1000,
        max_bytes_written=16 * 1024 * 1024,
        max_estimated_io_ru=8000,
    )


def _state_20k() -> dict:
    return {
        "run_id": "run-20k",
        "updated_at": "2026-08-03T18:00:00Z",
        "post_link_queue": {
            str(index): {
                "post_link_queue_id": str(index),
                "post_url": f"https://t.me/example/{index}",
                "post_link_status": "pending_fetch" if index % 200 == 0 else "fetched",
                "priority": index,
            }
            for index in range(20000)
        },
    }


def test_typed_work_schema_is_keyed_by_generation_and_queue_prefix() -> None:
    ddl = work_table_ddl("/db/work")
    assert "PRIMARY KEY (generation, queue_name, due_at, priority, status, item_key)" in ddl
    assert "PRIMARY KEY (generation, queue_name)" in work_cursor_table_ddl("/db/cursor")
    assert cursor_table_path("/db/region_work_queue_v2") == "/db/region_work_cursor_v2"
    query = work_page_query("/db/work", limit=25)
    assert "WHERE generation = $generation AND queue_name = $queue_name" in query
    assert "due_at <= $due_cutoff" in query
    assert "due_at > $cursor_due_at" in query
    assert "ORDER BY generation, queue_name, due_at, priority, status, item_key" in query
    assert "LIMIT 25" in query
    assert "payload_json" not in query.split("WHERE", 1)[1].split("ORDER BY", 1)[0]


def test_20k_population_materializes_only_actionable_work_and_keeps_exact_counters() -> None:
    state = _state_20k()
    work = build_work_items(state, max_per_queue=200)
    assert len(work) == 100
    assert {item.status for item in work} == {"pending_fetch"}
    assert all(item.state_pk.startswith("post_link_queue_item:") for item in work)
    model = build_read_model(state, work)
    assert model["population_totals"]["post_links"] == 20000
    assert model["work_total"] == 100
    assert model["metrics"]["post_link_queue_exact_ready_total"] == 100


def test_read_model_contract_rejects_missing_decision_counter() -> None:
    model = build_read_model(_state_20k(), build_work_items(_state_20k(), max_per_queue=200))
    assert validate_read_model(model)["schema_version"] == READ_MODEL_SCHEMA_VERSION
    assert model["work_queue_schema_version"] == WORK_QUEUE_SCHEMA_VERSION
    del model["metrics"]["image_pending_total"]
    with pytest.raises(YdbReadModelUnavailable, match="decision_metrics_incomplete"):
        validate_read_model(model)


def test_truncated_materialization_keeps_exact_counter_but_cannot_cut_over() -> None:
    state = _state_20k()
    work = build_work_items(state, max_per_queue=25)
    model = build_read_model(state, work)
    assert len(work) == 25
    assert model["metrics"]["post_link_queue_exact_ready_total"] == 100
    assert model["expected_work_counts"]["post_link"] == 100
    assert model["work_queue_complete"] is False
    with pytest.raises(YdbReadModelUnavailable, match="not_ready|work_queue_incomplete"):
        validate_read_model(model)


def test_legacy_population_scan_requires_shadow_or_legacy_and_explicit_flag() -> None:
    assert not legacy_fallback_allowed({})
    assert not legacy_fallback_allowed({"REGION_TALK_YDB_READ_MODEL_MODE": "shadow"})
    assert not legacy_fallback_allowed({
        "REGION_TALK_YDB_READ_MODEL_MODE": "required",
        "REGION_TALK_YDB_ALLOW_LEGACY_BROAD_READ_FALLBACK": "1",
    })
    assert legacy_fallback_allowed({
        "REGION_TALK_YDB_READ_MODEL_MODE": "shadow",
        "REGION_TALK_YDB_ALLOW_LEGACY_BROAD_READ_FALLBACK": "1",
    })


def test_observability_reads_one_counter_row() -> None:
    model = build_read_model(_state_20k(), build_work_items(_state_20k(), max_per_queue=200))
    row = SimpleNamespace(payload_json=json.dumps(model))

    class Tx:
        def execute(self, query, **kwargs):
            assert "model_name = 'current'" in query
            assert kwargs["commit_tx"] is True
            return [SimpleNamespace(rows=[row])]

    class Session:
        def transaction(self, _mode):
            return Tx()

    class Pool:
        def retry_operation_sync(self, op):
            return op(Session())

    loaded = read_current_model(
        Pool(), SimpleNamespace(SnapshotReadOnly=lambda: object()), "/db/read_model",
        budget=_budget(),
    )
    assert loaded["population_totals"]["post_links"] == 20000


def _load_candidate_module(name: str):
    path = ROOT / "kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_standalone_candidate_projection_matches_bounded_20k_contract() -> None:
    candidate = _load_candidate_module("region_talk_candidate_read_model_test")
    rows, model = candidate.build_region_talk_ydb_materialized_projection(
        _state_20k(), max_per_queue=200,
    )
    assert len(rows) == 100
    assert {row["status"] for row in rows} == {"pending_fetch"}
    assert model["population_totals"]["post_links"] == 20000
    assert model["metrics"]["post_link_queue_exact_ready_total"] == 100


def test_candidate_projection_uses_exact_shared_alias_semantics_and_fails_closed() -> None:
    candidate = _load_candidate_module("region_talk_candidate_alias_parity_test")
    synthetic = {
        "run_id": "aliases", "updated_at": "2026-08-03T18:00:00Z",
        "unified_source_queue": {
            "s1": {"source_queue_id": "s1", "queue_status": "retry"},
            "s2": {"source_id": "s2", "fetch_status": "pending_scan"},
        },
        "post_link_queue": {
            "p1": {"post_link_queue_id": "p1", "fetch_status": "retry_fetch"},
        },
        "image_candidate_queue": {
            "i1": {"image_queue_id": "i1", "image_quality_decision": "needs_visual_review"},
        },
        "candidate_memory": {
            "b1": {"candidate_memory_id": "b1", "text_vector_fusion_status": "missing_bge_m3_enrichment"},
        },
        "publication_candidate_queue": {
            "u1": {"publication_candidate_id": "u1", "publication_candidate_status": "custom_pending_review"},
            "u2": {"publication_candidate_id": "u2", "publication_candidate_status": "published"},
        },
    }
    shared = build_work_items(synthetic, max_per_queue=20)
    local_rows, local_model = candidate.build_region_talk_ydb_materialized_projection(
        synthetic, max_per_queue=20,
    )
    shared_contract = sorted((row.queue_name, row.status, row.item_key, row.state_pk) for row in shared)
    local_contract = sorted((row["queue_name"], row["status"], row["item_key"], row["state_pk"]) for row in local_rows)
    assert local_contract == shared_contract
    assert local_model["expected_work_counts"] == {
        "bge": 1, "image": 1, "post_link": 1, "publication": 1, "source": 2,
    }

    with pytest.raises(candidate.RegionTalkYdbReadModelUnavailable, match="not_ready|incomplete"):
        candidate.build_region_talk_ydb_materialized_projection(synthetic, max_per_queue=1)


def test_v2_due_cursor_has_no_starvation_and_expired_claim_replays_page() -> None:
    candidate = _load_candidate_module("region_talk_candidate_due_cursor_test")
    rows = [
        {"generation": "g", "queue_name": "source", "due_at": "", "priority": 0, "status": "pending_scan", "item_key": "a"},
        {"generation": "g", "queue_name": "source", "due_at": "2026-08-03T10:00:00Z", "priority": 5, "status": "retry", "item_key": "b"},
        {"generation": "g", "queue_name": "source", "due_at": "2026-08-03T10:00:00Z", "priority": 5, "status": "retry", "item_key": "c"},
        {"generation": "g", "queue_name": "source", "due_at": "2026-08-04T10:00:00Z", "priority": 0, "status": "error", "item_key": "future"},
    ]
    page1 = candidate.region_talk_ydb_due_page_from_rows(
        rows, generation="g", queue_name="source", due_cutoff="2026-08-03T23:59:59Z", limit=2,
    )
    # Until ACK the committed cursor is unchanged, so an expired lease replays
    # the exact same deterministic page instead of skipping it.
    replay = candidate.region_talk_ydb_due_page_from_rows(
        rows, generation="g", queue_name="source", due_cutoff="2026-08-03T23:59:59Z", limit=2,
    )
    assert [row["item_key"] for row in page1] == ["a", "b"]
    assert replay == page1
    cursor = candidate.region_talk_ydb_work_key(page1[-1])
    page2 = candidate.region_talk_ydb_due_page_from_rows(
        rows, generation="g", queue_name="source", due_cutoff="2026-08-03T23:59:59Z", cursor=cursor, limit=2,
    )
    assert [row["item_key"] for row in page2] == ["c"]
    cursor = candidate.region_talk_ydb_work_key(page2[-1])
    page3 = candidate.region_talk_ydb_due_page_from_rows(
        rows, generation="g", queue_name="source", due_cutoff="2026-08-05T00:00:00Z", cursor=cursor, limit=2,
    )
    assert [row["item_key"] for row in page3] == ["future"]

    claim_query = candidate.region_talk_ydb_work_claim_query("/db/work", "/db/cursor", limit=2)
    assert "Serializable" not in claim_query  # transaction mode is SDK-owned
    assert "due_at <= $due_cutoff" in claim_query
    assert "COALESCE(lease_expires_at, '') <= $now" in claim_query
    ack_query = candidate.region_talk_ydb_work_ack_query("/db/cursor")
    assert "lease_owner" in ack_query and "lease_token" in ack_query
    assert "lease_expires_at" in ack_query and "consumed_count +" in ack_query


def test_live_claim_uses_serializable_lease_and_ack_rejects_stale_owner() -> None:
    candidate = _load_candidate_module("region_talk_candidate_atomic_claim_test")
    state = {
        "run_id": "atomic", "updated_at": "2026-08-03T18:00:00Z",
        "unified_source_queue": {
            "a": {"source_queue_id": "a", "queue_status": "pending_scan"},
            "b": {"source_queue_id": "b", "queue_status": "retry"},
        },
    }
    _rows, model = candidate.build_region_talk_ydb_materialized_projection(state, max_per_queue=10)
    generation = model["generation"]
    cursor_row = SimpleNamespace(
        generation=generation, queue_name="source", expected_count=2, consumed_count=0,
        cursor_due_at="", cursor_priority=0, cursor_status="", cursor_item_key="",
        lease_owner="", lease_token="", lease_expires_at="",
    )
    page_rows = [
        SimpleNamespace(
            generation=generation, queue_name="source", status="pending_scan", due_at="",
            priority=0, item_key="a", state_pk="source_queue_item:a", payload_json="{}",
            updated_at=state["updated_at"],
        ),
    ]
    modes: list[str] = []

    class ClaimTx:
        def execute(self, _query, params, **kwargs):
            assert params["$due_cutoff"] == "2026-08-03T23:59:59Z"
            assert params["$expected_count"] == 2
            assert kwargs["commit_tx"] is True
            return [SimpleNamespace(rows=[cursor_row]), SimpleNamespace(rows=page_rows)]

    class ClaimSession:
        def prepare(self, query, **_kwargs):
            return query

        def transaction(self, mode):
            modes.append(mode)
            return ClaimTx()

    class Settings:
        def with_timeout(self, _value):
            return self

        def with_operation_timeout(self, _value):
            return self

    ydb = SimpleNamespace(
        SerializableReadWrite=lambda: "serializable", BaseRequestSettings=Settings,
    )
    claimed, claims = candidate.ydb_select_materialized_work(
        ClaimSession(), ydb, "/db/region_work_queue_v2",
        generation=generation, queue_names=("source",), limit_per_queue=1,
        read_model=model, due_cutoff="2026-08-03T23:59:59Z", lease_owner="run-1",
    )
    assert modes == ["serializable"]
    assert [row["item_key"] for row in claimed] == ["a"]
    assert claims[0]["lease_owner"] == "run-1"

    ack_claim_row = SimpleNamespace(
        lease_owner="run-1", lease_token=claims[0]["lease_token"], claim_count=1,
    )

    class AckTx:
        def __init__(self, rows):
            self.rows = rows

        def execute(self, _query, _params, **_kwargs):
            return [SimpleNamespace(rows=self.rows)]

    class AckSession:
        def __init__(self, rows):
            self.rows = rows

        def prepare(self, query, **_kwargs):
            return query

        def transaction(self, mode):
            assert mode == "serializable"
            return AckTx(self.rows)

    assert candidate.ydb_ack_materialized_work_claims(
        AckSession([ack_claim_row]), ydb, "/db/region_work_queue_v2", claims,
    ) == 1
    with pytest.raises(candidate.RegionTalkYdbReadModelUnavailable, match="stale_work_claim"):
        candidate.ydb_ack_materialized_work_claims(
            AckSession([]), ydb, "/db/region_work_queue_v2", claims,
        )

    busy_cursor = SimpleNamespace(**{
        **cursor_row.__dict__, "lease_owner": "other", "lease_token": "live",
        "lease_expires_at": "9999-12-31T23:59:59Z",
    })

    class BusyTx(ClaimTx):
        def execute(self, _query, _params, **_kwargs):
            return [SimpleNamespace(rows=[busy_cursor]), SimpleNamespace(rows=[])]

    class BusySession(ClaimSession):
        def transaction(self, _mode):
            return BusyTx()

    with pytest.raises(candidate.RegionTalkYdbReadModelUnavailable, match="work_lease_busy"):
        candidate.ydb_select_materialized_work(
            BusySession(), ydb, "/db/region_work_queue_v2",
            generation=generation, queue_names=("source",), limit_per_queue=1,
            read_model=model, due_cutoff="2026-08-03T23:59:59Z", lease_owner="run-2",
        )
    bad_model = json.loads(json.dumps(model))
    bad_model["work_counts"]["source"] = 1
    with pytest.raises(candidate.RegionTalkYdbReadModelUnavailable, match="queue_count_mismatch"):
        candidate.ydb_select_materialized_work(
            ClaimSession(), ydb, "/db/region_work_queue_v2",
            generation=generation, queue_names=("source",), limit_per_queue=1,
            read_model=bad_model, due_cutoff="2026-08-03T23:59:59Z", lease_owner="run-3",
        )
    full_cursor = SimpleNamespace(**{**cursor_row.__dict__, "consumed_count": 2})

    class OverflowTx(ClaimTx):
        def execute(self, _query, _params, **_kwargs):
            return [SimpleNamespace(rows=[full_cursor]), SimpleNamespace(rows=page_rows)]

    class OverflowSession(ClaimSession):
        def transaction(self, _mode):
            return OverflowTx()

    with pytest.raises(candidate.RegionTalkYdbReadModelUnavailable, match="work_queue_overflow"):
        candidate.ydb_select_materialized_work(
            OverflowSession(), ydb, "/db/region_work_queue_v2",
            generation=generation, queue_names=("source",), limit_per_queue=1,
            read_model=model, due_cutoff="2026-08-03T23:59:59Z", lease_owner="run-4",
        )


def test_materialized_partial_input_cannot_publish_ready_even_when_local_counts_fit() -> None:
    candidate = _load_candidate_module("region_talk_candidate_partial_ready_test")
    state = {
        "run_id": "partial", "updated_at": "2026-08-03T18:00:00Z",
        "_ydb_materialized_projection_input_complete": False,
        "post_link_queue": {
            "a": {"post_link_queue_id": "a", "fetch_status": "pending_fetch"},
        },
    }
    with pytest.raises(candidate.RegionTalkYdbReadModelUnavailable, match="not_ready|incomplete"):
        candidate.build_region_talk_ydb_materialized_projection(state, max_per_queue=10, cutover_state="ready")
    rows, model = candidate.build_region_talk_ydb_materialized_projection(
        state, max_per_queue=10, cutover_state="shadow",
    )
    assert len(rows) == 1
    assert model["cutover_state"] == "blocked_incomplete_input"
    assert model["work_queue_complete"] is False


def test_offline_cutover_plan_publishes_pointer_last_and_never_executes_live() -> None:
    from scripts.region_talk_ydb_read_model_cutover import build_plan

    plan = build_plan(
        _state_20k(), database="/ru-central1/cloud/database",
        namespace="region_talk_compact", max_per_queue=200, cutover_state="shadow",
    )
    assert plan["live_execution_performed"] is False
    assert plan["cutover_order"][-1] == "publish_current_pointer_last"
    assert len(plan["work_rows"]) == 100
    assert "PRIMARY KEY (generation, queue_name" in plan["ddl"]["work_queue"]
    assert "PRIMARY KEY (generation, queue_name)" in plan["ddl"]["work_cursor"]
    assert len(plan["cursor_rows"]) == 5


def test_orchestrator_returns_counters_without_kind_population_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts import region_talk_orchestrator as orchestrator

    model = build_read_model(_state_20k(), build_work_items(_state_20k(), max_per_queue=200))
    driver = SimpleNamespace(stop=mock.Mock())
    monkeypatch.setenv("REGION_TALK_YDB_READ_MODEL_MODE", "required")
    monkeypatch.setattr(orchestrator, "ensure_ydb_module", lambda: SimpleNamespace(SessionPool=lambda _driver: object()))
    monkeypatch.setattr(orchestrator, "ydb_endpoint_database", lambda **_kwargs: ("grpcs://example", "/db"))
    monkeypatch.setattr(orchestrator, "validate_expected_database", lambda database, **_kwargs: database)
    monkeypatch.setattr(orchestrator, "ydb_credentials", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(orchestrator, "_open_ydb_driver", lambda *_args, **_kwargs: driver)
    monkeypatch.setattr(orchestrator, "read_current_model", lambda *_args, **_kwargs: model)
    population_read = mock.Mock(side_effect=AssertionError("broad kind read must not run"))
    monkeypatch.setattr(orchestrator, "read_kind_rows", population_read)

    metrics = orchestrator.read_region_talk_queue_metrics(20000, bge_sample_limit=48)
    assert metrics["ydb_read_path"] == "materialized_read_model"
    assert metrics["ydb_read_model_population_totals"]["post_links"] == 20000
    population_read.assert_not_called()
    driver.stop.assert_called_once()


def test_candidate_required_start_state_uses_work_pages_and_point_joins_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _load_candidate_module("region_talk_candidate_start_read_model_test")
    state = _state_20k()
    work_items = build_work_items(state, max_per_queue=200)
    model = build_read_model(state, work_items)
    work_rows = [item.as_row(updated_at=state["updated_at"]) for item in work_items[:5]]
    point_rows = {
        row["state_pk"]: {
            "post_link_queue_id": row["item_key"],
            "post_url": f"https://t.me/example/{row['item_key']}",
            "post_link_status": "pending_fetch",
        }
        for row in work_rows
    }

    class Pool:
        def __init__(self, _driver):
            pass

        def retry_operation_sync(self, op, **_kwargs):
            return op(SimpleNamespace())

    driver = SimpleNamespace(stop=mock.Mock())
    ydb = SimpleNamespace(SessionPool=Pool, RetrySettings=lambda **kwargs: kwargs)
    cfg = {"database": "/db", "namespace": "region_talk_compact", "endpoint": "grpcs://example", "missing": ""}
    monkeypatch.setenv("REGION_TALK_YDB_READ_MODEL_MODE", "required")
    monkeypatch.setenv("REGION_TALK_YDB_ENDPOINT", "grpcs://example")
    monkeypatch.setenv("REGION_TALK_YDB_DATABASE", "/db")
    monkeypatch.setattr(candidate, "ydb_connect", lambda: (ydb, driver, cfg))
    monkeypatch.setattr(candidate, "ensure_ydb_kv_table", lambda *_args: None)
    monkeypatch.setattr(candidate, "ydb_select_latest_state", lambda *_args: {
        "run_id": state["run_id"], "updated_at": state["updated_at"],
    })
    monkeypatch.setattr(candidate, "ydb_select_current_read_model", lambda *_args: model)
    monkeypatch.setattr(candidate, "ydb_select_materialized_work", lambda *_args, **_kwargs: (work_rows, []))
    monkeypatch.setattr(candidate, "ydb_select_pk_items", lambda _s, _y, _t, pks, **_kwargs: {
        pk: point_rows[pk] for pk in pks if pk in point_rows
    })
    broad_read = mock.Mock(side_effect=AssertionError("kind population read must not run"))
    monkeypatch.setattr(candidate, "ydb_select_kind_items", broad_read)

    loaded, meta = candidate.load_region_talk_ydb_state()
    assert meta["ydb_read_status"] == "ok", meta.get("ydb_error")
    assert loaded["ydb_read_path"] == "typed_work_queue_plus_point_join"
    assert len(loaded["post_link_queue"]) == 5
    broad_read.assert_not_called()
