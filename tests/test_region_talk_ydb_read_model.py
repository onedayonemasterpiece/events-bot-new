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
    legacy_fallback_allowed,
    read_current_model,
    validate_read_model,
    work_page_query,
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
    assert "PRIMARY KEY (generation, queue_name, status, due_at, priority, item_key)" in ddl
    query = work_page_query("/db/work", limit=25)
    assert "WHERE generation = $generation AND queue_name = $queue_name" in query
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
    monkeypatch.setattr(candidate, "ydb_select_materialized_work", lambda *_args, **_kwargs: work_rows)
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
