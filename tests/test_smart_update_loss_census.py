from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "smart_update_loss_census.py"
SPEC = importlib.util.spec_from_file_location("smart_update_loss_census", SCRIPT)
assert SPEC and SPEC.loader
census = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = census
SPEC.loader.exec_module(census)


def _db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE vk_inbox(
          id INTEGER PRIMARY KEY, group_id INTEGER, post_id INTEGER, status TEXT,
          created_at TEXT, text TEXT, imported_event_id INTEGER, last_error TEXT,
          last_result_json TEXT
        );
        INSERT INTO vk_inbox VALUES
          (1,10,100,'failed','2026-08-05','raw',NULL,'identity gate',NULL),
          (2,10,101,'imported','2026-08-05','raw',77,NULL,NULL);
        """
    )
    con.commit()
    con.close()


def test_classifier_earliest_loss_and_definitive_success_override() -> None:
    assert census.classify_carrier({"no_date": True, "llm_provider_failure": True}) == "B"
    assert census.classify_carrier({"history_prefilter": True, "partial_child_loss": True}) == "G"
    assert census.classify_carrier({"partial_child_loss": True}) == "O"
    assert census.classify_carrier({"loss_class": "DISCOVERY_NO_KEYWORDS"}) == "A"
    assert census.classify_carrier({"payload_unavailable": True}) == "T"
    assert census.classify_carrier(
        {"no_keywords": True, "terminal_outcome": "CREATED"}
    ) == "S"


def test_census_deduplicates_carrier_revision_and_separates_counts() -> None:
    rows = [
        {
            "source_type": "vk", "carrier_id": "1:1", "source_revision_hash": "r1",
            "no_keywords": True, "raw_payload_available": True,
        },
        {
            "source_type": "vk", "carrier_id": "1:1", "source_revision_hash": "r1",
            "no_keywords": True, "llm_started": True, "extracted_event_occurrences": 2,
            "lifecycle_actions": 1, "would_create": 2, "raw_payload_available": True,
        },
        {
            "source_type": "telegram", "carrier_id": "2:2", "source_revision_hash": "r2",
            "partial_child_loss": True, "llm_started": True, "llm_completed": True,
            "incomplete_evidence": True, "extracted_event_occurrences": 3,
            "would_retry": 1, "raw_payload_available": True,
        },
    ]
    report = census.build_census(rows)
    classes = {item["code"]: item for item in report["classes"]}
    assert report["totals"]["carrier_count"] == 2
    assert report["totals"]["extracted_event_occurrences"] == 5
    assert report["totals"]["lifecycle_actions"] == 1
    assert classes["A"]["carrier_count"] == 1
    assert classes["A"]["extracted_event_occurrences"] == 2
    assert classes["O"]["carrier_count"] == 1
    assert classes["O"]["incomplete_evidence_count"] == 1


def test_read_only_run_includes_offline_discovery_misses_and_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "db.sqlite"
    _db(db)
    evidence = tmp_path / "misses.json"
    evidence.write_text(
        json.dumps(
            {
                "vk_misses_sample": [
                    {
                        "evidence_row_id": "sample-1", "source_type": "vk",
                        "carrier_id": "10:102", "source_revision_hash": "r3",
                        "discovery_no_keywords": True, "raw_payload_available": False,
                        "observed_at": "2026-08-05T03:00:00Z",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    before = db.read_bytes()
    first = census.run(
        db, since="2026-08-01", until="2026-08-06",
        supabase_evidence_paths=[evidence],
    )
    second = census.run(
        db, since="2026-08-01", until="2026-08-06",
        supabase_evidence_paths=[evidence],
    )
    assert db.read_bytes() == before
    assert first["query_only"] is True
    assert first["changed_rows"] == 0
    assert first["before_db_hash"] == first["after_db_hash"]
    assert first["inventory_hash"] == second["inventory_hash"]
    assert first["totals"]["carrier_count"] == 3
    assert {item["loss_class"] for item in first["inventory"]} >= {"A", "P", "S"}
    assert first["extrapolation"] == {
        "vk_misses_sample_multiplier": None, "permitted": False
    }


def test_planned_raw_packet_schema_is_feature_detected(tmp_path: Path) -> None:
    db = tmp_path / "db.sqlite"
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE vk_source_packet(
          id INTEGER PRIMARY KEY, source_type TEXT, owner_id INTEGER,
          post_id INTEGER, published_at INTEGER, raw_text TEXT, raw_payload_json TEXT,
          attachment_metadata_json TEXT, payload_hash TEXT, source_revision_hash TEXT,
          discovery_hints_json TEXT,
          evidence_manifest_json TEXT, parse_result_json TEXT, ocr_status TEXT,
          llm_status TEXT, last_typed_reason TEXT, terminal_carrier_outcome TEXT
        );
        CREATE TABLE vk_source_packet_attempt(
          id INTEGER PRIMARY KEY, source_packet_id INTEGER,
          evidence_manifest_json TEXT, llm_started INTEGER, llm_completed INTEGER,
          structured_response_valid INTEGER, event_child_count INTEGER,
          lifecycle_action_count INTEGER, typed_error_reason TEXT,
          terminal_carrier_outcome TEXT
        );
        CREATE TABLE vk_crawl_continuation(id INTEGER PRIMARY KEY, owner_id INTEGER);
        INSERT INTO vk_source_packet VALUES
          (1,'vk',10,20,1785888000,'','{"post":20}','[{"type":"photo"}]','p','r','{}','{}','{}','ok',
           'completed','POST_LLM_REJECT_REASON_PARTIAL',NULL);
        INSERT INTO vk_source_packet_attempt VALUES
          (1,1,'{"partial_child_loss":true}',1,1,1,2,1,NULL,NULL);
        """
    )
    con.commit()
    con.close()
    report = census.run(db, since="2026-08-01", until="2026-08-06")
    assert report["features"]["vk_source_packet"]["available"] is True
    assert report["features"]["vk_source_packet"]["attempt_ledger"] is True
    assert report["features"]["vk_crawl_continuation"]["available"] is True
    assert report["inventory"][0]["loss_class"] == "O"
    assert report["totals"]["extracted_event_occurrences"] == 2
    assert report["totals"]["lifecycle_actions"] == 1
    assert report["totals"]["llm_completed_count"] == 1
    assert report["inventory"][0]["payload_available"] is True
    assert report["metrics"]["vk_source_packets_total"] == 1
    assert report["metrics"]["vk_llm_parse_total"] == 1


def test_no_event_requires_completed_valid_complete_evidence() -> None:
    invalid = census.build_census(
        [
            {
                "source_type": "vk",
                "carrier_id": "1:1",
                "source_revision_hash": "r1",
                "raw_payload_available": True,
                "confirmed_no_event": True,
                "llm_started": True,
                "llm_completed": True,
                "structured_response_valid": True,
                "evidence_complete": False,
            }
        ]
    )
    valid = census.build_census(
        [
            {
                "source_type": "vk",
                "carrier_id": "1:2",
                "source_revision_hash": "r2",
                "raw_payload_available": True,
                "confirmed_no_event": True,
                "llm_started": True,
                "llm_completed": True,
                "structured_response_valid": True,
                "evidence_complete": True,
            }
        ]
    )
    assert invalid["inventory"][0]["loss_class"] == "M"
    assert valid["inventory"][0]["loss_class"] == "R"


def test_generic_ledger_fallback_respects_half_open_window(tmp_path: Path) -> None:
    db = tmp_path / "db.sqlite"
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE ingestion_funnel_ledger(
          source_type TEXT, carrier_id TEXT, source_revision_hash TEXT,
          evidence_json TEXT, observed_at TEXT
        );
        INSERT INTO ingestion_funnel_ledger VALUES
          ('vk','before','r1','{"no_date":true}','2026-07-31T23:59:59Z'),
          ('vk','inside','r2','{"no_date":true}','2026-08-01T00:00:00Z'),
          ('vk','until','r3','{"no_date":true}','2026-08-06T00:00:00Z');
        """
    )
    con.commit()
    con.close()
    report = census.run(db, since="2026-08-01", until="2026-08-06")
    assert report["features"]["ingestion_funnel_ledger"] == {
        "available": True, "rows": 1
    }
    assert report["totals"]["carrier_count"] == 1
    assert report["inventory"][0]["loss_class"] == "B"


def test_feb_jul_sampling_has_explicit_denominators_and_never_multiplies() -> None:
    rows = []
    for index in range(5):
        rows.append(
            {
                "source_type": "vk", "carrier_id": str(index), "source_revision_hash": "r",
                "observed_at": "2026-02-10", "no_date": True,
            }
        )
    rows.append(
        {
            "source_type": "telegram", "carrier_id": "x", "source_revision_hash": "r",
            "observed_at": "2026-07-10", "ocr_failure": True,
        }
    )
    sample = census.stratified_sample(rows, per_stratum=2)
    assert sample["population_denominator"] == 6
    assert sample["sample_count"] == 3
    assert sample["extrapolation_permitted"] is False
    assert sample["vk_misses_sample_multiplier"] is None
    assert sample["strata"][0]["population_denominator"] in {1, 5}
