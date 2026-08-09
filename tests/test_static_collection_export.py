from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "site" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import static_collection_batch as batch_module
import static_collection_export as collections
import static_event_bge as bge
from static_place_org_registry import load_registry
import static_collection_product_snapshot as product_snapshot_module


def _load_exporter():
    path = SCRIPTS / "export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("collection_exporter_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ticket_status_does_not_reinfer_free_admission():
    exporter = _load_exporter()
    row = {
        "id": 999,
        "ticket_status": "Бесплатно по регистрации",
        "ticket_link": "https://example.test/ticket",
        "is_free": 0,
        "ticket_price_min": None,
        "ticket_price_max": None,
    }

    ticket = exporter.ticket_info(row)

    assert ticket["is_free"] is False
    assert ticket["kind"] == "registration"


def test_collect_source_records_preserves_type_username_and_trust():
    exporter = _load_exporter()
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table event_source(
          id integer primary key, event_id integer, source_type text,
          source_url text, source_chat_username text, source_chat_id integer,
          source_message_id integer, trust_level text
        )
        """
    )
    con.execute(
        "insert into event_source values(1,7,'telegram','https://t.me/dramteatr39/8','dramteatr39',11,8,'high')"
    )

    assert exporter.collect_source_records(con, 7) == [
        {
            "source_type": "telegram",
            "source_url": "https://t.me/dramteatr39/8",
            "trust_level": "high",
            "source_chat_id": 11,
            "source_message_id": 8,
            "telegram_username": "dramteatr39",
        }
    ]


def test_exporter_product_boundary_emits_source_bound_facts_v3_without_provider(tmp_path):
    exporter = _load_exporter()
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table event_source(
          id integer primary key, event_id integer, source_type text,
          source_url text, source_text text, trust_level text
        )
        """
    )
    con.execute(
        "insert into event_source values(7,1,'telegram','https://t.me/example/7',?, 'official')",
        ("Приглашаем родителей с детьми",),
    )
    output = tmp_path / "product.json"
    result = exporter.build_collection_product_output(
        con,
        events=[
            {
                "id": 1,
                "title": "Семейное занятие",
                "start_date": "2026-08-10",
                "lifecycle_status": "active",
                "other_date_ids": [],
                "organizer_names": ["Семейный центр"],
            }
        ],
        collection_decisions_by_id={
            1: {
                "family_suitable_decision": {
                    "value": "confirmed",
                    "confidence": 0.9,
                    "evidence_quote": "родителей с детьми",
                    "reason_code": "explicit_family_invitation",
                    "source_id": 7,
                    "input_hash": "a" * 64,
                    "policy_version": product_snapshot_module.FACTS_POLICY_VERSION,
                }
            }
        },
        current_date="2026-08-02",
        generated_at="2026-08-02T12:00:00Z",
        source_scope="production-copy-after-apply",
        evidence_trust_scope="all",
        output_path=output,
    )

    snapshot = json.loads(output.read_text(encoding="utf-8"))
    assert result["collection_product_provider_calls"] == 0
    assert result["collection_product_input_fingerprint"] == snapshot["input_fingerprint"]
    assert snapshot["collections"]["family_suitable"]["items"][0]["source_status"] == "grounded"
    assert snapshot["collections"]["family_suitable"]["items"][0]["organizer"] == "Семейный центр"


def test_registry_projection_keeps_official_theatre_and_exact_venue_roles_separate():
    registry = load_registry()
    events = [
        {
            "id": 1,
            "title": "Спектакль",
            "venue_name": "Драматический театр",
            "address": None,
            "city": None,
            "start_date": "2026-08-01",
            "start_time": "19:00",
            "organizer_names": [],
        },
        {
            "id": 2,
            "title": "Гастроли",
            "venue_name": "Другая сцена",
            "address": None,
            "city": None,
            "start_date": "2026-08-02",
            "start_time": "18:00",
            "organizer_names": [],
        },
    ]
    manifest, theatre_ids = collections.build_registry_projection(
        events,
        source_records_by_id={
            1: [{"source_type": "parser:dramteatr", "source_url": "https://dramteatr39.ru/a"}],
            2: [{"source_type": "parser:dramteatr", "source_url": "https://dramteatr39.ru/b"}],
        },
        registry=registry,
        generated_at="2026-08-01T12:00:00Z",
        catalog_hash="a" * 64,
    )

    assert theatre_ids == {1, 2}
    assert len(manifest["theatre_organizations"]) == 8
    assert len(manifest["venues"]) == 6
    drama = next(row for row in manifest["venues"] if row["entity_id"] == "dramteatr39")
    assert [row["event_id"] for row in drama["occurrences"]] == [1]


def test_exact_labels_use_grounded_decisions_not_bge_candidates():
    events = [
        {
            "id": 1,
            "event_type": "лекция",
            "topics": ["FAMILY", "PERSONALITIES"],
            "ticket": {"is_free": True},
            "popularity_signal_score": 2,
        },
        {
            "id": 2,
            "event_type": "выставка",
            "topics": ["EXHIBITIONS"],
            "ticket": {"is_free": False},
            "popularity_signal_score": 0,
        },
    ]
    decisions = {
        1: {
            "audience_decision": {
                "value": "family",
                "policy_version": "static-collection-facts-v2",
            },
            "people_appearances": [
                {
                    "appearance": "confirmed",
                    "origin_scope": "foreign",
                    "policy_version": "static-collection-facts-v2",
                },
                {
                    "appearance": "mentioned",
                    "origin_scope": "russia_nonlocal",
                    "policy_version": "static-collection-facts-v2",
                },
            ],
        }
    }

    labels = collections.build_exact_label_ids(
        events,
        collection_decisions_by_id=decisions,
        theatre_event_ids=[],
        facts_policy_version="static-collection-facts-v2",
    )

    assert labels["free"] == [1]
    assert labels["kids"] == [1]
    assert labels["guests_foreign"] == [1]
    assert labels["guests_russia"] == []
    assert labels["exhibitions"] == [2]


def test_exact_audience_and_people_ignore_stale_fact_policy():
    events = [{"id": 1, "event_type": "лекция", "topics": [], "ticket": {}}]
    decisions = {
        1: {
            "audience_decision": {
                "value": "family",
                "policy_version": "static-collection-facts-v1",
            },
            "people_appearances": [
                {
                    "appearance": "confirmed",
                    "origin_scope": "foreign",
                    "policy_version": "static-collection-facts-v1",
                }
            ],
        }
    }
    labels = collections.build_exact_label_ids(
        events,
        collection_decisions_by_id=decisions,
        theatre_event_ids=[],
        facts_policy_version="static-collection-facts-v2",
    )
    assert labels["kids"] == []
    assert labels["guests_foreign"] == []


def test_semantic_candidates_remain_blocked_in_valid_batch():
    policy = collections.load_object(collections.DEFAULT_POLICY_PATH)
    artifact = {
        "metadata": {
            "model_id": "BAAI/bge-m3",
            "model_revision": "1" * 40,
            "encoder_contract": "fp32",
            "document_kind": "collection_semantics_v1",
            "document_version": "v1",
            "prototype_bank_sha256": "b" * 64,
            "artifact_sha256": "c" * 64,
            "event_cache_identity_sha256": "d" * 64,
        }
    }
    batch = collections.build_collection_batch_payload(
        events=[{"id": 1, "event_type": "лекция", "topics": [], "ticket": {"is_free": False}}],
        collection_decisions_by_id={},
        theatre_event_ids=[],
        semantic_candidates={
            "science": {"item_ids": [1], "failure_codes": []},
        },
        artifact=artifact,
        policy=policy,
        catalog_hash="a" * 64,
        generated_at="2026-08-01T12:00:00Z",
        snapshot={"snapshot_id": "s"},
        registry_sha256="e" * 64,
    )

    validation = batch_module.validate_collection_batch(
        batch, catalog_item_ids=[1], require_compute=True
    )
    assert validation == {"valid": True, "errors": []}
    assert batch["labels"]["science"]["compute_status"] == "pass"
    assert batch["labels"]["science"]["quality_status"] == "not_evaluated"
    assert batch["labels"]["science"]["publication_status"] == "blocked"
    assert batch["labels"]["gastronomy"]["compute_status"] == "blocked"
    assert batch["labels"]["gastronomy"]["item_ids"] == []
    assert batch["labels"]["gastronomy"]["failure_codes"] == [
        "checked_exact_id_manifest_missing"
    ]


def _encoder(texts, **_kwargs):
    rows = []
    for index, _text in enumerate(texts):
        row = [0.0] * bge.EMBEDDING_DIM
        row[index % bge.EMBEDDING_DIM] = 1.0
        rows.append(row)
    return rows


def test_collection_cache_loader_allows_event_reuse_after_prototype_change(tmp_path):
    events = [{"id": 1, "title": "Событие", "description": "Источник"}]
    first_bank = {"schema_version": "v1", "prototypes": [{"id": "x", "text": "one"}]}
    first = bge.build_collection_bge_vector_artifact(
        events,
        first_bank,
        model_revision=bge.MODEL_REVISION,
        classifier={"head": "one"},
        encoder=_encoder,
    )
    npz = tmp_path / "vectors.npz"
    receipt = tmp_path / "receipt.json"
    bge.write_collection_bge_cache(first, npz_path=npz, receipt_path=receipt)
    loaded = bge.load_collection_bge_cache(npz_path=npz, receipt_path=receipt)
    assert loaded is not None

    second = bge.build_collection_bge_vector_artifact(
        events,
        {"schema_version": "v1", "prototypes": [{"id": "x", "text": "two"}]},
        model_revision=bge.MODEL_REVISION,
        classifier={"head": "two"},
        encoder=_encoder,
        previous_artifact=loaded,
    )
    assert second["metadata"]["encoded_event_count"] == 0
    assert second["metadata"]["encoded_prototype_count"] == 1


def test_unusual_collection_head_emits_family_and_neutral_evidence_without_encoder():
    policy = collections.load_object(collections.DEFAULT_POLICY_PATH)
    policy["_prototype_rows"] = [
        {"id": "positive.open.1", "kind": "positive", "family": "open_dialogue"},
        {"id": "positive.rare.1", "kind": "positive", "family": "rare_practice"},
        {"id": "hard_negative.open.1", "kind": "hard_negative", "family": "open_dialogue"},
        {"id": "neutral.1", "kind": "neutral", "family": None},
    ]
    artifact = {
        "event_vectors": {"7": {"text_hash": "a" * 64, "vector": [1.0, 0.0]}},
        "prototype_vectors": {
            "unusual.positive.open.1": {"vector": [0.9, 0.1]},
            "unusual.positive.rare.1": {"vector": [0.8, 0.2]},
            "unusual.hard_negative.open.1": {"vector": [0.2, 0.8]},
            "unusual.neutral.1": {"vector": [0.1, 0.9]},
        },
    }

    result = collections.score_semantic_candidates(artifact, policy)["unusual"]

    assert result["item_ids"] == [7]
    assert result["scores"][7]["family"] == "open_dialogue"
    assert result["scores"][7]["family_top3"] == [
        {"id": "open_dialogue", "score": 0.9},
        {"id": "rare_practice", "score": 0.8},
    ]
    assert result["scores"][7]["positive_neutral_margin"] == 0.8
    assert result["scores"][7]["top_hard_negative_prototype_id"] == (
        "unusual.hard_negative.open.1"
    )


def test_unusual_shadow_is_valid_blocked_evidence_and_excludes_incident_hash():
    events = [
        {
            "id": 1,
            "slug": "odd-one",
            "title": "Ночное участие",
            "event_type": "экскурсия",
            "venue_name": "Башня",
            "city": "Калининград",
            "start_date": "2026-08-10",
            "end_date": None,
            "lifecycle_status": "active",
            "ticket": {},
            "image_assets": [],
            "topics": [],
        },
        {
            "id": 2,
            "slug": "ordinary",
            "title": "Обычная выставка",
            "event_type": "выставка",
            "venue_name": "Архив",
            "city": "Калининград",
            "start_date": "2026-08-11",
            "end_date": None,
            "lifecycle_status": "active",
            "ticket": {},
            "image_assets": [],
            "topics": [],
        },
    ]
    artifact = {
        "metadata": {
            "model_id": "BAAI/bge-m3",
            "model_revision": "1" * 40,
            "embedding_dim": 1024,
            "document_kind": "collection_semantics_v1",
            "document_version": "collection-semantics-doc-v1",
            "prototype_bank_sha256": "2" * 64,
            "classifier_sha256": "3" * 64,
        },
        "event_vectors": {
            "1": {"text_hash": "a" * 64, "vector": []},
            "2": {"text_hash": "b" * 64, "vector": []},
        },
    }
    manifest = collections.unusual_shadow_manifest(
        events=events,
        candidate_ids=[1, 2],
        candidate_scores={
            1: {"positive": 0.8, "negative": 0.2, "margin": 0.6, "family": "after_hours"},
            2: {"positive": 0.9, "negative": 0.1, "margin": 0.8, "family": "restricted_access"},
        },
        incident_regressions={
            "taxonomy_version": "unusual-event-taxonomy-v1",
            "cases": [
                {"document_text_sha256": "b" * 64, "reason_code": "incident_hard_negative"}
            ],
        },
        selection_policy={"target_count": 20, "minimum_publish_count": 12},
        generated_at="2026-08-09T20:00:00Z",
        build_metadata={
            "build_id": "production-test",
            "run_id": "run-test",
            "repo_sha": "4" * 40,
            "source_snapshot_id": "snapshot-test",
            "source_snapshot_hash": "5" * 64,
        },
        artifact=artifact,
    )

    assert manifest["quality_gate"]["status"] == "blocked"
    assert manifest["quality_gate"]["reason"] == "independent_acceptance_holdout_missing"
    assert manifest["selected_event_ids"] == []
    assert manifest["selected_count"] == 0
    assert manifest["review_shortlist_event_ids"] == [1]
    assert manifest["review_shortlist_count"] == 1
    assert manifest["quality_gate"]["metrics"]["incident_regression_count"] == 1
    assert manifest["decisions"][0]["event_id"] == 2
    assert manifest["decisions"][0]["include"] is False
    assert manifest["decisions"][0]["reason_codes"] == ["incident_hard_negative"]
    assert manifest["doc_kind"] == "collection_semantics_v1"
    assert manifest["classifier_hash"] == "3" * 64
