from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "site" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from static_event_bge import (  # noqa: E402
    DOCUMENT_VERSION,
    EMBEDDING_DIM,
    MODEL_REVISION,
    build_related_v1_document,
    build_shared_bge_vector_artifact,
    validate_shared_bge_vector_artifact,
)
from unusual_event_semantics import (  # noqa: E402
    FAMILY_IDS,
    evaluate_unusual_quality_fixture,
    load_unusual_classifier,
    load_unusual_prototype_bank,
    score_unusual_manifest,
)


def event(event_id: int, title: str, **overrides):
    row = {
        "id": event_id,
        "title": title,
        "event_type": "встреча",
        "summary": "Фактическое описание формата",
        "description_html": "<p>Подробности события.</p>",
        "city": "Калининград",
        "venue_name": "Площадка",
        "topics": [],
        "lifecycle_status": "active",
        "start_date": "2026-08-10",
        "end_date": "2026-08-10",
    }
    row.update(overrides)
    return row


def _unit(index: int) -> list[float]:
    vector = [0.0] * EMBEDDING_DIM
    vector[index] = 1.0
    return vector


def _artifact(events):
    bank = load_unusual_prototype_bank()
    classifier = load_unusual_classifier()
    positive_text = next(
        row["text"] for row in bank["prototypes"] if row["kind"] == "positive"
    )
    neutral_text = next(
        row["text"] for row in bank["prototypes"] if row["kind"] == "neutral"
    )
    prototype_coordinates = {}
    coordinate = 1
    for row in bank["prototypes"]:
        if row["text"] == positive_text:
            prototype_coordinates[row["text"]] = 0
        elif row["text"] == neutral_text:
            prototype_coordinates[row["text"]] = 100
        else:
            while coordinate in {100}:
                coordinate += 1
            prototype_coordinates[row["text"]] = coordinate
            coordinate += 1

    def fake_encoder(texts, *, model_revision, batch_size):
        assert model_revision == MODEL_REVISION
        assert batch_size > 0
        output = []
        for text in texts:
            if text in prototype_coordinates:
                output.append(_unit(prototype_coordinates[text]))
            elif "Ordinary" in text:
                output.append(_unit(100))
            else:
                output.append(_unit(0))
        return output

    artifact = build_shared_bge_vector_artifact(
        events,
        bank,
        model_revision=MODEL_REVISION,
        classifier=classifier,
        encoder=fake_encoder,
        build_metadata={"run_id": "unit"},
    )
    return artifact, bank, classifier


def test_related_document_delegates_to_existing_related_v1_contract():
    row = event(41, "Открытая встреча")
    actual = build_related_v1_document(row)
    from scripts import sync_event_search_vectors_to_supabase as canonical
    category = canonical.ru_event_category(row)
    tags = canonical.event_tags(row, category)
    digest = canonical.build_related_digest(row, category, tags)
    expected = (
        f"related-event: title: {canonical.clean_text(row['title'])} | text: {digest}"
    )
    assert actual["document_version"] == DOCUMENT_VERSION
    assert actual["text"] == expected
    assert len(actual["text_hash"]) == 64


def test_shared_artifact_is_one_batch_hash_bound_and_normalised():
    rows = [event(1, "Core")]
    artifact, bank, classifier = _artifact(rows)
    metadata = artifact["metadata"]
    assert metadata["provider_calls"] == 0
    assert metadata["event_count"] == 1
    assert metadata["prototype_count"] == len(bank["prototypes"])
    assert metadata["classifier_sha256"]
    assert validate_shared_bge_vector_artifact(
        artifact,
        prototype_bank=bank,
        expected_classifier_sha256=metadata["classifier_sha256"],
    ) == {"valid": True, "errors": []}
    assert sum(value * value for value in artifact["event_vectors"]["1"]["vector"]) == pytest.approx(1.0)


def test_scorer_is_shadow_only_deduplicates_series_and_reuses_exact_cache():
    rows = [
        event(1, "Core one", other_date_ids=[2]),
        event(2, "Core two", other_date_ids=[1], start_date="2026-08-11", end_date="2026-08-11"),
        event(3, "Ordinary programme"),
        event(4, "Past core", start_date="2026-07-01", end_date="2026-07-01"),
    ]
    artifact, bank, classifier = _artifact(rows)
    first = score_unusual_manifest(
        rows,
        artifact["event_vectors"],
        artifact["prototype_vectors"],
        artifact["metadata"],
        build_metadata={"as_of_date": "2026-07-27", "run_id": "shadow-unit"},
        prototype_bank=bank,
        classifier=classifier,
    )
    assert tuple(row["id"] for row in first["manifest"]["families"]) == FAMILY_IDS
    assert first["manifest"]["status"] == "shadow"
    assert first["manifest"]["evaluation_approval_status"] == "not_approved"
    assert first["manifest"]["items"] == []
    assert first["manifest"]["migration"]["notify"] is False
    assert first["metrics"]["provider_calls"] == 0
    assert first["metrics"]["decision_counts"] == {
        "core_unusual": 2,
        "adjacent": 0,
        "ordinary": 1,
        "abstain": 1,
    }
    assert first["metrics"]["concept_candidates"] == 1
    assert first["metrics"]["concept_duplicates_removed"] == 1
    core = next(row for row in first["manifest"]["shadow_items"] if row["event_id"] == 1)
    assert core["concept_id"] == "occurrence:1"
    assert core["tier"] == "core_unusual"
    assert core["eligibility"]["eligible"] is True
    assert core["unusual_score"] == core["calibrated_confidence"]
    assert len(core["family_scores"]) == 15
    assert core["input_hash"] and core["content_hash"]
    assert core["model_revision"] == MODEL_REVISION
    assert core["prototype_evidence"][0]["prototype_kind"] == "positive"
    past = next(row for row in first["manifest"]["shadow_items"] if row["event_id"] == 4)
    assert past["decision"] == "abstain"
    assert "past" in past["eligibility_failures"]

    second = score_unusual_manifest(
        rows,
        artifact["event_vectors"],
        artifact["prototype_vectors"],
        artifact["metadata"],
        first["cache"],
        {"as_of_date": "2026-07-27", "run_id": "shadow-unit-2"},
        prototype_bank=bank,
        classifier=classifier,
    )
    assert second["metrics"]["cache_hits"] == len(rows)
    assert second["manifest"]["shadow_items"] == first["manifest"]["shadow_items"]


def test_scorer_fails_closed_on_document_or_classifier_hash_mismatch():
    rows = [event(1, "Core")]
    artifact, bank, classifier = _artifact(rows)
    tampered_metadata = dict(artifact["metadata"])
    tampered_metadata["classifier_sha256"] = "0" * 64
    result = score_unusual_manifest(
        rows,
        artifact["event_vectors"],
        artifact["prototype_vectors"],
        tampered_metadata,
        build_metadata={"as_of_date": "2026-07-27"},
        prototype_bank=bank,
        classifier=classifier,
    )
    assert result["manifest"]["status"] == "blocked"
    assert result["manifest"]["items"] == []
    assert "classifier_sha256 mismatch" in result["metrics"]["boundary_errors"]

    changed = [dict(rows[0], title="Changed after vector build")]
    stale = score_unusual_manifest(
        changed,
        artifact["event_vectors"],
        artifact["prototype_vectors"],
        artifact["metadata"],
        build_metadata={"as_of_date": "2026-07-27"},
        prototype_bank=bank,
        classifier=classifier,
    )
    assert stale["manifest"]["status"] == "blocked"
    assert "event 1 text_hash mismatch" in stale["metrics"]["boundary_errors"]


def test_concept_identity_requires_mutual_links_and_ignores_presentation_fields():
    rows = [
        event(1, "Unilateral A", other_date_ids=[2]),
        event(2, "Unilateral B", other_date_ids=[]),
        event(3, "Repeated concept", description_html="<p>First text</p>"),
        event(
            4,
            "Repeated concept",
            description_html="<p>Changed presentation text and price</p>",
            start_date="2026-09-20",
            end_date="2026-09-20",
            ticket={"price_label": "900 ₽"},
        ),
    ]
    artifact, bank, classifier = _artifact(rows)
    result = score_unusual_manifest(
        rows,
        artifact["event_vectors"],
        artifact["prototype_vectors"],
        artifact["metadata"],
        build_metadata={"as_of_date": "2026-07-27"},
        prototype_bank=bank,
        classifier=classifier,
    )
    by_id = {row["event_id"]: row for row in result["manifest"]["shadow_items"]}
    assert by_id[1]["concept_id"] != by_id[2]["concept_id"]
    assert by_id[1]["concept_id_source"] == "stable_semantic_identity"
    assert by_id[3]["concept_id"] == by_id[4]["concept_id"]
    assert by_id[3]["concept_id"].startswith("concept:")


def test_quality_fixture_is_hash_bound_and_real_canary_evidence_is_explicit():
    rows = [event(1, "Core")]
    artifact, bank, classifier = _artifact(rows)
    artifact["metadata"]["build"]["evidence_kind"] = "real_bge_canary"
    from static_event_bge import stable_hash

    unhashed = dict(artifact["metadata"])
    unhashed.pop("artifact_sha256")
    artifact["metadata"]["artifact_sha256"] = stable_hash(
        {
            "metadata": unhashed,
            "event_vectors": artifact["event_vectors"],
            "prototype_vectors": artifact["prototype_vectors"],
        }
    )
    fixture = {
        "schema_version": "unusual-events-golden-v1",
        "cases": [
            {
                "event_id": 1,
                "label": "positive",
                "concept_id": "gold.core",
                "eligible": True,
                "expected_family": "open_dialogue",
                "frozen_tier": "core_unusual",
            }
        ],
    }
    evaluation = evaluate_unusual_quality_fixture(
        fixture, artifact, bank, classifier
    )
    assert evaluation["evidence_kind"] == "real_bge_canary"
    assert evaluation["deterministic_repeat_exact"] is True
    assert evaluation["identical_rebuild_flip_rate"] == 0.0
    assert evaluation["confirmed_unusual_recall"] == 1.0
    assert evaluation["hard_negative_false_positive_rate"] is None
