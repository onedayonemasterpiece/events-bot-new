from __future__ import annotations

import json
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.region_talk_ydb_cost import (
    YdbCostBudget,
    YdbCostBudgetExceeded,
    YdbIdentityError,
    estimated_yql_io_ru_floor,
    validate_expected_database,
)


FIXTURES = Path(__file__).parent / "fixtures"


def test_exact_database_guard_is_mandatory_and_redacted() -> None:
    expected = "/ru-central1/correct-cloud/current-db"
    with pytest.raises(YdbIdentityError, match="expected_database_missing") as missing:
        validate_expected_database(expected, {})
    assert expected not in str(missing.value)

    with pytest.raises(YdbIdentityError, match="expected_database_mismatch") as mismatch:
        validate_expected_database(
            "/ru-central1/wrong-cloud/stale-db",
            {"REGION_TALK_YDB_EXPECTED_DATABASE": expected},
        )
    assert "wrong-cloud" not in str(mismatch.value)
    assert "correct-cloud" not in str(mismatch.value)
    assert validate_expected_database(
        expected, {"REGION_TALK_YDB_EXPECTED_DATABASE": expected}
    ) == expected


def test_yql_io_ru_floor_uses_documented_row_and_block_maximums() -> None:
    assert estimated_yql_io_ru_floor(read_rows=2, read_bytes=16) == 2
    assert estimated_yql_io_ru_floor(written_rows=2, written_bytes=2456) == 6
    assert estimated_yql_io_ru_floor(
        read_rows=2, read_bytes=16, written_rows=2, written_bytes=2456
    ) == 8


def test_budget_auto_aborts_before_second_query() -> None:
    budget = YdbCostBudget(
        max_queries=1,
        max_rows_read=100,
        max_bytes_read=10000,
        max_rows_written=100,
        max_bytes_written=10000,
        max_estimated_io_ru=100,
    )
    budget.before_query("first")
    with pytest.raises(YdbCostBudgetExceeded, match="queries"):
        budget.before_query("second")


def test_budget_auto_aborts_on_20k_full_materialization_fixture() -> None:
    fixture = json.loads((FIXTURES / "region_talk_ydb_cost_20k.json").read_text())
    budget = YdbCostBudget(
        max_queries=100,
        max_rows_read=fixture["max_scan_rows"],
        max_bytes_read=64 * 1024 * 1024,
        max_rows_written=100,
        max_bytes_written=1024 * 1024,
        max_estimated_io_ru=fixture["max_scan_rows"],
    )
    rows = [
        {
            "pk": f"post_link_queue_item:{index:05d}",
            "post_link_status": (
                fixture["due_status"] if index % fixture["due_every"] == 0
                else fixture["terminal_status"]
            ),
        }
        for index in range(fixture["rows_total"])
    ]
    budget.before_query("legacy_full_scan")
    with pytest.raises(YdbCostBudgetExceeded, match="rows_read"):
        budget.record_read("legacy_full_scan", len(rows), sum(len(json.dumps(row)) for row in rows))


def test_incident_migration_fixture_keeps_exact_owner_counts_and_disabled_state() -> None:
    fixture = json.loads((FIXTURES / "region_talk_ydb_incident_migration.json").read_text())
    assert fixture["expected_database"] != fixture["source_database"]
    assert fixture["table_row_counts"] == [2, 9, 231, 266, 58046]
    assert fixture["ordered_export_hash_required"] is True
    assert fixture["scheduler_enabled"] is False
    assert fixture["throttling_ru_per_second"] == 0


def test_scheduled_preflight_requires_expected_database_even_when_actual_exists(tmp_path: Path) -> None:
    from scripts import region_talk_scheduled_runner as runner

    env = {
        "REGION_TALK_YDB_ENDPOINT": "grpcs://example",
        "REGION_TALK_YDB_DATABASE": "/ru-central1/cloud/db",
    }
    assert "REGION_TALK_YDB_EXPECTED_DATABASE" in runner.missing_autonomy_config(env)


def test_candidate_bulk_change_filter_ignores_only_volatile_run_fields() -> None:
    module_path = Path(__file__).parents[1] / "kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py"
    spec = importlib.util.spec_from_file_location("region_talk_candidate_report_write_test", module_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)

    initial = [("source_queue_item:a", "source_queue_item", {
        "source_queue_status": "pending_scan", "run_id": "r1", "updated_at": "t1",
    })]
    changed, fingerprints = mod.ydb_changed_rows(initial)
    assert len(changed) == 1
    mod.remember_ydb_written_fingerprints(fingerprints)

    volatile_only = [("source_queue_item:a", "source_queue_item", {
        "source_queue_status": "pending_scan", "run_id": "r2", "updated_at": "t2",
    })]
    assert mod.ydb_changed_rows(volatile_only)[0] == []

    product_change = [("source_queue_item:a", "source_queue_item", {
        "source_queue_status": "processed_no_ko", "run_id": "r2", "updated_at": "t2",
    })]
    assert len(mod.ydb_changed_rows(product_change)[0]) == 1


def test_due_page_over_20k_fixture_stops_at_scan_ceiling() -> None:
    """Exercise CandidateReport's compatibility page without a live YDB."""

    fixture = json.loads((FIXTURES / "region_talk_ydb_cost_20k.json").read_text())
    module_path = Path(__file__).parents[1] / "kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py"
    spec = importlib.util.spec_from_file_location("region_talk_candidate_report_cost_test", module_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    all_rows = [
        SimpleNamespace(
            pk=f"post_link_queue_item:{index:05d}",
            payload_json=json.dumps({
                "post_link_status": (
                    fixture["due_status"] if index % fixture["due_every"] == 0
                    else fixture["terminal_status"]
                ),
                "post_url": f"https://t.me/example/{index}",
            }),
            updated_at="2026-08-03T00:00:00Z",
        )
        for index in range(fixture["rows_total"])
    ]
    scanned = 0

    class Tx:
        def execute(self, _query, params, **_kwargs):
            nonlocal scanned
            after = params["$after"]
            start = 0 if after.endswith(":") else int(after.rsplit(":", 1)[-1]) + 1
            page = all_rows[start:start + 25]
            scanned += len(page)
            return [SimpleNamespace(rows=page)]

    class Session:
        def prepare(self, query, **_kwargs):
            return query

        def transaction(self, _mode):
            return Tx()

    ydb = SimpleNamespace(
        SnapshotReadOnly=lambda: object(),
        BaseRequestSettings=lambda: SimpleNamespace(
            with_timeout=lambda _value: SimpleNamespace(
                with_operation_timeout=lambda _other: object()
            )
        ),
    )
    items = mod.ydb_select_due_kind_items(
        Session(), ydb, "/db/state", "post_link_queue_item",
        status_fields=("post_link_status",),
        due_statuses={fixture["due_status"]},
        limit=fixture["page_limit"],
        max_scan_rows=fixture["max_scan_rows"],
    )
    assert len(items) == fixture["expected_due_rows"]
    assert scanned <= fixture["max_scan_rows"]
    assert scanned < fixture["rows_total"]
