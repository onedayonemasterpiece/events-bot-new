from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "site" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import static_collection_export as collection_export
import static_collection_gastronomy as mod


def test_runtime_policy_and_prototypes_register_gastronomy_recall_head():
    policy = collection_export.load_object(collection_export.DEFAULT_POLICY_PATH)
    config = policy["labels"]["gastronomy"]
    assert config["strategy"] == "semantic_bge_recall"
    assert config["publication"] == "blocked"
    assert config["publication_truth"] == "source_grounded_gastronomy_v1"

    prototypes = collection_export.load_object(collection_export.DEFAULT_PROTOTYPES_PATH)
    ids = {row["id"] for row in prototypes["prototypes"]}
    assert {
        "gastronomy.positive.food_central",
        "gastronomy.positive.food_culture",
        "gastronomy.positive.food_co_core",
        "gastronomy.hard_negative.venue_only",
        "gastronomy.hard_negative.service_food",
        "gastronomy.hard_negative.metaphor_or_art",
        "gastronomy.hard_negative.generic_market",
    } <= ids


def test_gastronomy_head_is_high_recall_only_and_never_publication_truth():
    policy = {
        "labels": {
            "gastronomy": {
                "strategy": "semantic_bge_recall",
                "positive_prefix": "gastronomy.positive.",
                "negative_prefix": "gastronomy.hard_negative.",
                "minimum_positive_similarity": 0.38,
                "minimum_margin": -0.01,
                "publication": "blocked",
            }
        }
    }
    artifact = {
        "event_vectors": {
            "1": {"vector": [1.0, 0.0]},
            "2": {"vector": [0.0, 1.0]},
        },
        "prototype_vectors": {
            "gastronomy.positive.food": {"vector": [1.0, 0.0]},
            "gastronomy.hard_negative.venue": {"vector": [0.0, 1.0]},
        },
    }
    result = collection_export.score_semantic_candidates(artifact, policy)
    assert result["gastronomy"]["item_ids"] == [1]
    assert result["gastronomy"]["failure_codes"] == []


def batch(ids):
    value = {
        "schema_version": "collection-batch-v1",
        "labels": {
            "gastronomy": {
                "strategy": "semantic_bge_recall",
                "compute_status": "pass",
                "quality_status": "not_evaluated",
                "publication_status": "blocked",
                "item_ids": sorted(ids),
                "item_count": len(ids),
                "failure_codes": ["owner_gold_missing"],
                "last_good": {"status": "absent"},
                "hashes": {},
            }
        },
    }
    value["batch_sha256"] = mod.stable_hash(value)
    return value


def events():
    return [
        {
            "id": 1,
            "title": "Праздник еды, первая дата",
            "start_date": "2026-08-10",
            "end_date": "2026-08-10",
            "event_type": "фестиваль",
            "venue_name": "Площадь",
            "linked_event_ids": [2],
        },
        {
            "id": 2,
            "title": "Праздник еды, вторая дата",
            "start_date": "2026-08-11",
            "end_date": "2026-08-11",
            "event_type": "фестиваль",
            "venue_name": "Площадь",
            "linked_event_ids": [1],
        },
        {
            "id": 3,
            "title": "Чайная встреча",
            "start_date": "2026-08-12",
            "end_date": "2026-08-12",
            "event_type": "лекция",
            "venue_name": "Библиотека",
            "linked_event_ids": [],
        },
        {
            "id": 4,
            "title": "Концерт в ресторане",
            "start_date": "2026-08-13",
            "end_date": "2026-08-13",
            "event_type": "концерт",
            "venue_name": "Ресторан",
            "linked_event_ids": [],
        },
    ]


def sources():
    return {
        2: [
            {
                "id": 20,
                "event_id": 2,
                "source_type": "telegram",
                "source_url": "https://t.me/food/20",
                "source_text": "Гастрономический фестиваль с дегустацией локальных сыров.",
                "trust_level": "high",
            }
        ],
        3: [
            {
                "id": 30,
                "event_id": 3,
                "source_type": "vk",
                "source_url": "https://vk.com/wall-1_30",
                "source_text": "Встреча о чайной культуре с дегустацией редких сортов.",
                "trust_level": "medium",
            }
        ],
        4: [
            {
                "id": 40,
                "event_id": 4,
                "source_type": "telegram",
                "source_url": "https://t.me/music/40",
                "source_text": "Концерт проходит в ресторане. В программе только музыка.",
                "trust_level": "medium",
            }
        ],
    }


def build_queue():
    return mod.build_review_queue(
        events(),
        source_records_by_event=sources(),
        candidate_ids=[1, 2, 3, 4],
        current_date="2026-08-03",
        generated_at="2026-08-03T12:00:00Z",
        source_scope="test",
        batch_sha256="a" * 64,
    )


def decision_for(candidate, *, role, reason, quote):
    source = candidate["source"]
    return {
        "event_id": candidate["event_id"],
        "family_id": candidate["family_id"],
        "input_hash": candidate["input_hash"],
        "source_id": source["source_id"],
        "source_url": source["source_url"],
        "role": role,
        "confidence": 1.0,
        "evidence_quote": quote,
        "reason_code": reason,
        "review_status": "approved",
        "reviewed_at": "2026-08-03T12:10:00Z",
        "reviewer": "owner",
        "manual_lock": True,
    }


def test_candidates_and_reciprocal_family_use_source_bearing_occurrence():
    queue = build_queue()
    assert queue["coverage"] == {
        "candidate_event_count": 4,
        "candidate_family_count": 3,
        "source_bound_family_count": 3,
        "blocked_family_count": 0,
    }
    first = next(row for row in queue["candidates"] if row["family_id"] == "linked:1")
    assert first["candidate_member_event_ids"] == [1, 2]
    assert first["family_member_event_ids"] == [1, 2]
    assert first["event_id"] == 2
    assert first["source"]["source_id"] == 20
    assert first["input_hash"] and len(first["input_hash"]) == 64


def test_starter_store_is_review_required_and_never_looks_like_empty_product():
    queue = build_queue()
    manifest = mod.build_manifest(
        queue,
        mod.starter_decision_store(),
        generated_at="2026-08-03T12:20:00Z",
    )
    assert manifest["extraction_status"] == "review_required"
    assert manifest["catalog_state"] == "unknown"
    assert manifest["publication_status"] == "blocked"
    assert manifest["item_ids"] == []

    overlaid = mod.overlay_collection_batch(batch([1, 2, 3, 4]), manifest)
    label = overlaid["labels"]["gastronomy"]
    assert label["candidate_item_ids"] == [1, 2, 3, 4]
    assert label["item_ids"] == []
    assert label["publication_status"] == "blocked"
    assert "gastronomy_source_review_incomplete" in label["failure_codes"]


def test_owner_review_accepts_only_core_and_co_core_and_deduplicates_family():
    queue = build_queue()
    candidates = {row["family_id"]: row for row in queue["candidates"]}
    store = {
        "schema_version": mod.DECISION_STORE_SCHEMA_VERSION,
        "policy_version": mod.POLICY_VERSION,
        "status": "owner_approved",
        "reviewed_at": "2026-08-03T12:30:00Z",
        "reviewer": "owner",
        "decisions": [
            decision_for(
                candidates["linked:1"],
                role="core",
                reason="food_is_primary_program",
                quote="Гастрономический фестиваль с дегустацией локальных сыров.",
            ),
            decision_for(
                candidates["event:3"],
                role="co_core",
                reason="food_is_equal_program",
                quote="Встреча о чайной культуре с дегустацией редких сортов.",
            ),
            decision_for(
                candidates["event:4"],
                role="incidental",
                reason="venue_or_service_only",
                quote="Концерт проходит в ресторане.",
            ),
        ],
    }
    validation = mod.validate_decision_store(store, queue=queue)
    assert validation["valid"], validation
    manifest = mod.build_manifest(queue, store, generated_at="2026-08-03T12:40:00Z")
    assert manifest["extraction_status"] == "pass"
    assert manifest["catalog_state"] == "low_supply"
    assert manifest["accepted_future_family_count"] == 2
    assert manifest["item_ids"] == [2, 3]
    assert mod.validate_manifest(manifest) == {"valid": True, "errors": []}

    overlaid = mod.overlay_collection_batch(batch([1, 2, 3, 4]), manifest)
    label = overlaid["labels"]["gastronomy"]
    assert label["candidate_item_ids"] == [1, 2, 3, 4]
    assert label["item_ids"] == [2, 3]
    assert label["quality_status"] == "pass"
    assert label["publication_status"] == "shadow"
    assert label["family_counts"] == {"future": 2, "recent": 0}
    assert overlaid["batch_sha256"] == mod.stable_hash(
        {key: value for key, value in overlaid.items() if key != "batch_sha256"}
    )

    product = mod.build_product_quality_snapshot(manifest)
    assert list(product["collections"]) == ["gastronomy"]
    assert len(product["collections"]["gastronomy"]["items"]) == 2


def test_stale_or_non_exact_decision_fails_closed():
    queue = build_queue()
    candidate = queue["candidates"][0]
    bad = {
        "schema_version": mod.DECISION_STORE_SCHEMA_VERSION,
        "policy_version": mod.POLICY_VERSION,
        "status": "owner_approved",
        "reviewed_at": "2026-08-03T12:30:00Z",
        "reviewer": "owner",
        "decisions": [
            {
                **decision_for(
                    candidate,
                    role="core",
                    reason="food_is_primary_program",
                    quote="invented quote",
                ),
                "input_hash": "f" * 64,
            }
        ],
    }
    validation = mod.validate_decision_store(bad, queue=queue)
    assert not validation["valid"]
    assert any("input_hash_stale" in value for value in validation["errors"])
    assert any("evidence_quote_not_exact" in value for value in validation["errors"])


def test_candidate_head_rejects_unsorted_or_duplicate_ids():
    value = batch([1, 2])
    value["labels"]["gastronomy"]["item_ids"] = [2, 1, 2]
    with pytest.raises(ValueError, match="sorted unique"):
        mod.candidate_event_ids(value)


def test_cli_builds_review_queue_manifest_and_fail_closed_batch_overlay(tmp_path):
    import argparse
    import importlib.util
    import sqlite3
    import static_collection_batch as batch_module

    db_path = tmp_path / "snapshot.sqlite"
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            create table event(
              id integer primary key, title text, date text, end_date text,
              time text, location_name text, organizer_names text,
              event_type text, lifecycle_status text, identity_status text,
              merged_into_event_id integer, silent integer,
              linked_event_ids text, collection_decisions text
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
        con.execute(
            "insert into event values(1,'Фестиваль еды','2026-08-10',null,'12:00','Площадь','[]','фестиваль','active','canonical',null,0,'[]',null)"
        )
        con.execute(
            "insert into event_source values(10,1,'telegram','https://t.me/food/10','Дегустация локальных продуктов.','high')"
        )
        con.commit()

    label = batch_module.build_collection_label(
        strategy="semantic_bge_recall",
        compute_status="pass",
        quality_status="not_evaluated",
        publication_status="blocked",
        item_ids=[1],
        failure_codes=["owner_gold_missing"],
    )
    batch_value = batch_module.build_collection_batch(
        catalog_hash="a" * 64,
        labels={"gastronomy": label},
        generated_at="2026-08-03T12:00:00Z",
    )
    batch_path = tmp_path / "collection-batch-v1.json"
    batch_path.write_text(json.dumps(batch_value), encoding="utf-8")
    store_path = tmp_path / "decisions.json"
    store_path.write_text(json.dumps(mod.starter_decision_store()), encoding="utf-8")

    script_path = ROOT / "scripts" / "build_static_collection_gastronomy.py"
    spec = importlib.util.spec_from_file_location("gastronomy_cli_test", script_path)
    assert spec and spec.loader
    cli = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = cli
    spec.loader.exec_module(cli)

    review_path = tmp_path / "review.json"
    manifest_path = tmp_path / "manifest.json"
    product_path = tmp_path / "product.json"
    overlaid_path = tmp_path / "overlaid.json"
    result = cli.run(
        argparse.Namespace(
            db=str(db_path),
            collection_batch=str(batch_path),
            decision_store=str(store_path),
            current_date="2026-08-03",
            source_scope="test-copy",
            evidence_trust_scope="all",
            review_queue_output=str(review_path),
            manifest_output=str(manifest_path),
            product_snapshot_output=str(product_path),
            batch_output=str(overlaid_path),
        )
    )

    assert result["status"] == "review_required"
    assert result["provider_calls"] == 0
    assert review_path.is_file() and manifest_path.is_file() and product_path.is_file()
    overlaid = json.loads(overlaid_path.read_text(encoding="utf-8"))
    assert overlaid["labels"]["gastronomy"]["candidate_item_ids"] == [1]
    assert overlaid["labels"]["gastronomy"]["item_ids"] == []
    assert overlaid["labels"]["gastronomy"]["publication_status"] == "blocked"
