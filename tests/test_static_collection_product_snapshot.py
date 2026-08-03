from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SITE_SCRIPTS = ROOT / "site" / "scripts"
if str(SITE_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SITE_SCRIPTS))

import static_collection_product_snapshot as product
from scripts.check_static_collections_product_quality import evaluate_snapshot


def _event(event_id: int, **values):
    row = {
        "id": event_id,
        "title": f"Событие {event_id}",
        "start_date": "2026-08-10",
        "end_date": None,
        "start_time": "18:00",
        "venue_name": "Зал",
        "organizer_names": ["Организатор"],
        "event_type": "занятие",
        "lifecycle_status": "active",
        "other_date_ids": [],
    }
    row.update(values)
    return row


def _source(source_id: int, event_id: int, text="Приглашаем родителей с детьми"):
    return {
        "id": source_id,
        "event_id": event_id,
        "source_type": "telegram",
        "source_url": f"https://t.me/example/{source_id}",
        "source_text": text,
        "trust_level": "official",
    }


def _fact(source_id: int, quote="родителей с детьми", **values):
    row = {
        "value": "confirmed",
        "confidence": 0.95,
        "evidence_quote": quote,
        "reason_code": "explicit_family_invitation",
        "source_id": source_id,
        "input_hash": "a" * 64,
        "policy_version": product.FACTS_POLICY_VERSION,
        "decided_at": "2026-08-02T10:00:00Z",
    }
    row.update(values)
    return row


def _build(events, decisions, sources, *, generated_at="2026-08-02T12:00:00Z"):
    return product.build_product_snapshot(
        events,
        collection_decisions_by_id=decisions,
        source_records_by_event=sources,
        current_date="2026-08-02",
        generated_at=generated_at,
        source_scope="unit-test-fixture",
        evidence_trust_scope="all",
    )


def test_snapshot_uses_only_direct_facts_v3_and_kids_is_child_or_family():
    events = [_event(1), _event(2), _event(3)]
    decisions = {
        1: {"child_directed_decision": _fact(11, reason_code="explicit_child_audience")},
        2: {"family_suitable_decision": _fact(22)},
        3: {
            # Legacy truth is deliberately not facts-v3 publication/product truth.
            "audience_decision": {
                "value": "family",
                "policy_version": product.FACTS_POLICY_VERSION,
            }
        },
    }
    sources = {1: [_source(11, 1)], 2: [_source(22, 2)], 3: [_source(33, 3)]}

    snapshot = _build(events, decisions, sources)

    assert [row["event_id"] for row in snapshot["collections"]["child_directed"]["items"]] == [1]
    assert [row["event_id"] for row in snapshot["collections"]["family_suitable"]["items"]] == [2]
    assert [row["event_id"] for row in snapshot["collections"]["kids"]["items"]] == [1, 2]
    assert all(value["mode"] in {"shadow", "experimental"} for value in snapshot["collections"].values())
    assert snapshot["publication"]["status"] == "blocked"
    assert snapshot["facts_policy_version"] == product.FACTS_POLICY_VERSION
    assert snapshot["source_scope"] == "unit-test-fixture"
    assert snapshot["evidence_trust_scope"] == "all"
    assert snapshot["provider_calls"] == 0
    assert snapshot["coverage"] == {
        "status": "unknown",
        "catalog_event_count": 3,
        "candidate_event_count": None,
        "evaluated_event_count": None,
        "deferred_event_count": None,
        "unprocessed_event_count": None,
        "candidate_event_ids_sha256": None,
        "evaluated_event_ids_sha256": None,
        "deferred_event_ids_sha256": None,
        "unprocessed_event_ids_sha256": None,
        "generator_command": None,
    }
    assert product.validate_product_snapshot(snapshot) == {"valid": True, "errors": []}


def test_explicit_coverage_is_hash_bound_and_complete_requires_no_unprocessed():
    coverage = {
        "status": "complete",
        "candidate_event_count": 3,
        "evaluated_event_count": 2,
        "deferred_event_count": 1,
        "unprocessed_event_count": 0,
        "candidate_event_ids_sha256": "a" * 64,
        "evaluated_event_ids_sha256": "b" * 64,
        "deferred_event_ids_sha256": "c" * 64,
        "unprocessed_event_ids_sha256": "d" * 64,
        "generator_command": "python scripts/backfill_static_collection_facts.py --plan",
    }
    snapshot = product.build_product_snapshot(
        [_event(1), _event(2), _event(3)],
        collection_decisions_by_id={},
        source_records_by_event={},
        current_date="2026-08-02",
        generated_at="2026-08-02T12:00:00Z",
        source_scope="full-current-future-shadow",
        coverage=coverage,
    )

    assert snapshot["coverage"]["status"] == "complete"
    assert snapshot["coverage"]["catalog_event_count"] == 3
    assert product.validate_product_snapshot(snapshot)["valid"] is True
    with pytest.raises(ValueError, match="zero unprocessed"):
        product.normalize_coverage(
            {**coverage, "unprocessed_event_count": 1, "deferred_event_count": 0},
            catalog_event_count=3,
        )


def test_mutual_link_family_dedupes_deterministically_without_fact_transfer():
    events = [
        _event(10, start_date="2026-08-12", other_date_ids=[11]),
        _event(11, start_date="2026-08-09", other_date_ids=[10]),
        _event(12, start_date="2026-08-08", other_date_ids=[10]),  # one-way: separate
    ]
    decisions = {
        10: {"family_suitable_decision": _fact(100)},
        11: {"family_suitable_decision": _fact(110)},
        # 12 has no direct fact and must not inherit from its one-way link.
    }
    sources = {10: [_source(100, 10)], 11: [_source(110, 11)], 12: [_source(120, 12)]}

    snapshot = _build(events, decisions, sources)
    rows = snapshot["collections"]["family_suitable"]["items"]

    assert len(rows) == 1
    assert rows[0]["event_id"] == 11
    assert rows[0]["family_id"] == "linked:10"
    assert rows[0]["member_provenance"] == {
        "family_member_event_ids": [10, 11],
        "representative_event_id": 11,
        "representative_rule": "earliest_start_date_then_event_id_among_direct_fact_members",
        "fact_transfer_across_siblings": False,
        "direct_fact_event_id": 11,
    }


def test_active_date_range_is_current_but_ended_event_is_excluded():
    events = [
        _event(1, start_date="2026-07-01", end_date="2026-08-20"),
        _event(2, start_date="2026-07-01", end_date="2026-08-01"),
    ]
    decisions = {
        1: {"family_suitable_decision": _fact(11)},
        2: {"family_suitable_decision": _fact(22)},
    }
    snapshot = _build(events, decisions, {1: [_source(11, 1)], 2: [_source(22, 2)]})
    assert [row["event_id"] for row in snapshot["collections"]["family_suitable"]["items"]] == [1]


def test_malformed_claimed_fact_stays_visible_and_product_monitor_fails():
    snapshot = _build(
        [_event(1)],
        {1: {"family_suitable_decision": _fact(11, quote="перефразированная цитата")}},
        {1: [_source(11, 1)]},
    )
    item = snapshot["collections"]["family_suitable"]["items"][0]
    assert item["source_status"] == "blocked"
    assert item["review_status"] == "needs_source_review"
    assert item["fact_provenance"][0]["evidence_quote"] == "перефразированная цитата"

    report = evaluate_snapshot(snapshot, today=date.fromisoformat("2026-08-02"))
    assert report["status"] == "FAIL"
    assert {issue["code"] for issue in report["issues"]} >= {
        "review_blocked_results",
        "source_grounding_missing",
    }


def test_warm_snapshot_fingerprints_and_normalized_output_ignore_generated_at():
    events = [_event(1)]
    decisions = {1: {"family_suitable_decision": _fact(11)}}
    sources = {1: [_source(11, 1)]}
    first = _build(events, decisions, sources, generated_at="2026-08-02T12:00:00Z")
    second = _build(events, decisions, sources, generated_at="2026-08-02T13:00:00Z")
    assert first["input_fingerprint"] == second["input_fingerprint"]
    assert first["normalized_output_sha256"] == second["normalized_output_sha256"]
    first_report = evaluate_snapshot(first, today=date.fromisoformat("2026-08-02"))
    second_report = evaluate_snapshot(second, today=date.fromisoformat("2026-08-02"))
    assert first["normalized_output_sha256"] == first_report[
        "normalized_output_sha256"
    ]
    assert first_report["normalized_output_sha256"] == second_report[
        "normalized_output_sha256"
    ]
    assert first["snapshot_sha256"] != second["snapshot_sha256"]


def test_validator_rejects_wrong_top_level_facts_policy_version():
    snapshot = _build([], {}, {})
    snapshot["facts_policy_version"] = "wrong-policy"
    snapshot["normalized_output_sha256"] = product.stable_hash(
        product.normalized_visible_output(snapshot)
    )
    snapshot["snapshot_sha256"] = product.stable_hash(
        {key: value for key, value in snapshot.items() if key != "snapshot_sha256"}
    )
    assert "facts_policy_version_mismatch" in product.validate_product_snapshot(snapshot)[
        "errors"
    ]


def test_stage_source_scope_is_independent_from_evidence_trust_filter():
    snapshot = product.build_product_snapshot(
        [_event(1)],
        collection_decisions_by_id={1: {"family_suitable_decision": _fact(11)}},
        source_records_by_event={1: [_source(11, 1) | {"trust_level": "community"}]},
        current_date="2026-08-02",
        generated_at="2026-08-02T12:00:00Z",
        source_scope="production-copy-after-apply",
        evidence_trust_scope="trusted",
    )
    item = snapshot["collections"]["family_suitable"]["items"][0]
    assert snapshot["source_scope"] == "production-copy-after-apply"
    assert item["source_status"] == "blocked"


def test_cli_loader_binds_event_source_and_writes_valid_snapshot(tmp_path, monkeypatch):
    db_path = tmp_path / "events.sqlite"
    con = sqlite3.connect(db_path)
    con.execute(
        """
        create table event(
          id integer primary key, title text, date text, end_date text, time text,
          location_name text, organizer_names text, event_type text,
          lifecycle_status text, identity_status text, merged_into_event_id integer,
          silent integer, linked_event_ids text, collection_decisions text
        )
        """
    )
    con.execute(
        """
        create table event_source(
          id integer primary key, event_id integer, source_type text,
          source_url text, source_text text, trust_level text
        )
        """
    )
    decisions = {"family_suitable_decision": _fact(9)}
    con.execute(
        "insert into event values(1,'Семейное событие','2026-08-10',null,'18:00','Зал',?,"
        "'занятие','active','canonical',null,0,'[]',?)",
        (json.dumps(["Организатор"]), json.dumps(decisions, ensure_ascii=False)),
    )
    con.execute(
        "insert into event_source values(9,1,'telegram','https://t.me/example/9',?, 'official')",
        ("Приглашаем родителей с детьми",),
    )
    con.commit()
    con.close()
    output = tmp_path / "snapshot.json"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "static_collection_product_snapshot.py",
            "--db",
            str(db_path),
            "--current-date",
            "2026-08-02",
            "--source-scope",
            "production-copy-after-apply",
            "--evidence-trust-scope",
            "all",
            "--output",
            str(output),
        ],
    )
    assert product.main() == 0
    assert output.is_file()
    written = json.loads(output.read_text())
    assert product.validate_product_snapshot(written)["valid"] is True
    assert written["source_scope"] == "production-copy-after-apply"
    item = written["collections"]["family_suitable"]["items"][0]
    assert item["organizer"] == "Организатор"
