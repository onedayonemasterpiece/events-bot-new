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
        "semantic_record_version": "canonical-event-semantic-v1",
        "record_kind": "event",
        "eventness_status": "event",
        "identity_status": "canonical",
        "merged_into_event_id": None,
        "silent": False,
        "is_public": True,
        "is_searchable": True,
        "publication_status": "published",
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
    assert {
        row["prototype_kind"] for row in core["prototype_evidence"]
    } == {"positive", "hard_negative", "neutral"}
    assert core["features"]["ordinary_corpus_distance"] == 1.0
    assert core["ordinary_corpus_evidence"]["nearest_event_id"] == 3
    receipt = first["metrics"]["ordinary_corpus_receipt"]
    assert receipt["member_event_ids"] == [3]
    assert receipt["provider_calls"] == 0
    assert receipt["policy_sha256"]
    assert receipt["corpus_sha256"]
    assert receipt["members"][0]["text_hash"]
    assert receipt["members"][0]["vector_sha256"]
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

    # A new artifact receipt/build hash must not invalidate unchanged event
    # decisions; per-event content/vector hashes are the cache boundary.
    refreshed_metadata = dict(artifact["metadata"])
    refreshed_metadata["build"] = {
        **dict(refreshed_metadata.get("build") or {}),
        "run_id": "new-artifact-receipt",
    }
    unhashed = dict(refreshed_metadata)
    unhashed.pop("artifact_sha256")
    from static_event_bge import stable_hash
    refreshed_metadata["artifact_sha256"] = stable_hash({
        "metadata": unhashed,
        "event_vectors": artifact["event_vectors"],
        "prototype_vectors": artifact["prototype_vectors"],
    })
    third = score_unusual_manifest(
        rows,
        artifact["event_vectors"],
        artifact["prototype_vectors"],
        refreshed_metadata,
        first["cache"],
        {"as_of_date": "2026-07-27", "run_id": "shadow-unit-3"},
        prototype_bank=bank,
        classifier=classifier,
    )
    assert third["metrics"]["cache_hits"] == len(rows)


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


@pytest.mark.parametrize(
    ("overrides", "failure"),
    [
        ({"publication_status": "postponed"}, "publication_unavailable"),
        ({"is_searchable": False}, "not_searchable"),
        ({"record_kind": "work_hours"}, "service_or_work_hours"),
        ({"identity_status": None}, "identity_not_canonical"),
        ({"publication_status": None}, "publication_not_confirmed"),
        ({"summary": "", "description_html": ""}, "insufficient_semantic_text"),
    ],
)
def test_structured_eligibility_gates_fail_closed(overrides, failure):
    rows = [event(1, "Core", **overrides)]
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
    row = result["manifest"]["shadow_items"][0]
    assert row["decision"] == "abstain"
    assert failure in row["eligibility_failures"]


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
    assert by_id[3]["concept_id"] == "presentation:3"
    assert by_id[3]["concept_id_source"] == "bge_presentation_cluster"


def test_concept_identity_accepts_mutual_linked_ids_and_canonical_roots():
    rows = [
        event(11, "Linked A", linked_event_ids=[12]),
        event(12, "Linked B", linked_event_ids=[11]),
        event(13, "Root A", canonical_root_event_id=9001),
        event(14, "Root B", canonical_root_event_id=9001),
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
    assert by_id[11]["concept_id"] == by_id[12]["concept_id"] == "occurrence:11"
    assert by_id[13]["concept_id"] == by_id[14]["concept_id"]
    assert by_id[13]["concept_id_source"] == "canonical_root_event_id"


def test_diversity_deferred_rows_never_bypass_family_venue_or_type_caps():
    rows = [
        event(
            event_id,
            f"Core {event_id}",
            venue_name="Одна площадка",
            event_type="встреча",
        )
        for event_id in range(1, 11)
    ]
    rows.append(event(99, "Ordinary baseline", venue_name="Другая площадка"))
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
    selected = result["manifest"]["candidate_items"]
    assert len(selected) == 4
    assert {row["family"] for row in selected} == {"open_dialogue"}
    assert result["metrics"]["diversity_deferred_count"] == 6
    assert result["metrics"]["diversity_caps"]["venue"] == 4


def test_shared_artifact_reencodes_only_a_changed_event():
    rows = [event(1, "First"), event(2, "Second")]
    artifact, bank, classifier = _artifact(rows)
    calls = []

    prototype_texts = {row["text"] for row in bank["prototypes"]}

    def incremental_encoder(texts, *, model_revision, batch_size):
        calls.append(list(texts))
        assert model_revision == MODEL_REVISION
        return [
            _unit(10 if text in prototype_texts else 11)
            for text in texts
        ]

    changed = [dict(rows[0], summary="Changed semantic description"), rows[1]]
    rebuilt = build_shared_bge_vector_artifact(
        changed,
        bank,
        model_revision=MODEL_REVISION,
        classifier=classifier,
        encoder=incremental_encoder,
        previous_artifact=artifact,
    )
    assert len(calls) == 1
    assert len(calls[0]) == 1
    assert rebuilt["metadata"]["encoded_event_count"] == 1
    assert rebuilt["metadata"]["reused_event_count"] == 1
    assert rebuilt["metadata"]["encoded_prototype_count"] == 0


def test_shared_artifact_reuses_vectors_when_only_classifier_changes():
    rows = [event(1, "First"), event(2, "Second")]
    artifact, bank, classifier = _artifact(rows)
    changed_classifier = {
        **classifier,
        "bias": float(classifier["bias"]) + 0.125,
    }

    def must_not_encode(*_args, **_kwargs):
        raise AssertionError("classifier-only calibration must not encode vectors")

    rebuilt = build_shared_bge_vector_artifact(
        rows,
        bank,
        model_revision=MODEL_REVISION,
        classifier=changed_classifier,
        encoder=must_not_encode,
        previous_artifact=artifact,
    )
    assert rebuilt["metadata"]["encoded_event_count"] == 0
    assert rebuilt["metadata"]["reused_event_count"] == len(rows)
    assert rebuilt["metadata"]["encoded_prototype_count"] == 0
    assert rebuilt["metadata"]["reused_prototype_count"] == len(bank["prototypes"])
    assert rebuilt["metadata"]["classifier_sha256"] != artifact["metadata"]["classifier_sha256"]
    assert rebuilt["metadata"]["artifact_sha256"] != artifact["metadata"]["artifact_sha256"]


def test_quality_fixture_is_hash_bound_and_real_canary_evidence_is_explicit():
    rows = [event(1, "Core"), event(2, "Ordinary baseline")]
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
            },
            {
                "event_id": 2,
                "label": "hard_negative",
                "concept_id": None,
                "eligible": True,
                "expected_family": None,
                "frozen_tier": "ordinary",
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
    assert evaluation["hard_negative_false_positive_rate"] == 0.0
    assert evaluation["ordinary_corpus_policy_sha256"]
    assert evaluation["ordinary_corpus_receipt"]["member_event_ids"] == [2]


def test_quality_fixture_deduplicates_publication_and_abstains_ineligible_rows():
    rows = [
        event(1, "Core one"),
        event(2, "Core two"),
        event(3, "Core three"),
        event(4, "Ordinary baseline"),
    ]
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
                "concept_id": "gold.same",
                "eligible": True,
            },
            {
                "event_id": 2,
                "label": "positive",
                "concept_id": "gold.same",
                "eligible": True,
            },
            {
                "event_id": 3,
                "label": "positive",
                "concept_id": "gold.non-event",
                "eligible": False,
            },
            {
                "event_id": 4,
                "label": "hard_negative",
                "concept_id": None,
                "eligible": True,
                "frozen_tier": "ordinary",
            },
        ],
    }
    evaluation = evaluate_unusual_quality_fixture(fixture, artifact, bank, classifier)
    by_id = {row["event_id"]: row for row in evaluation["predictions"]}
    assert by_id[3]["decision"] == "abstain"
    assert by_id[3]["reason_codes"] == ["eligibility_gate_failed"]
    assert evaluation["ineligible_publication_count"] == 0
    assert evaluation["duplicate_concepts_top20"] == 0
    assert evaluation["duplicate_candidates_removed_before_top20"] == 1
    assert evaluation["editorial_ranked_count"] == 1
    assert evaluation["confirmed_unusual_sample_size"] == 2
    assert evaluation["confirmed_unusual_recall"] == 1.0
