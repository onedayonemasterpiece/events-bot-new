from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "site" / "scripts"
FIXTURE = (
    ROOT / "tests" / "fixtures" / "unusual_events_semantic_regressions_v1.json"
)
SOURCE_EVENTS = ROOT / "site" / "src" / "data" / "preview-events.json"

if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from static_event_bge import (  # noqa: E402
    EMBEDDING_DIM,
    MODEL_REVISION,
    build_related_v1_document,
    build_shared_bge_vector_artifact,
)
from unusual_event_semantics import (  # noqa: E402
    evaluate_unusual_quality_fixture,
    load_unusual_classifier,
    load_unusual_prototype_bank,
)


EXPECTED_CLASSES = {
    5376: "ordinary_civic_commemorative_action",
    4327: "ordinary_exhibition",
}
SOURCE_BOUND_FIELDS = (
    "id",
    "title",
    "event_type",
    "summary",
    "description_html",
    "city",
    "venue_name",
    "topics",
    "lifecycle_status",
    "start_date",
    "end_date",
    "display_date",
    "display_time",
)


def load_fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _unit(index: int) -> list[float]:
    vector = [0.0] * EMBEDDING_DIM
    vector[index] = 1.0
    return vector


def test_hard_negative_fixture_freezes_the_two_reported_semantic_boundaries() -> None:
    fixture = load_fixture()
    assert fixture["schema_version"] == "unusual-events-semantic-regressions-v1"
    assert "not classifier calibration" in fixture["purpose"]

    cases = {int(case["event_id"]): case for case in fixture["cases"]}
    assert {event_id: case["regression_class"] for event_id, case in cases.items()} == (
        EXPECTED_CLASSES
    )

    source_payload = json.loads(SOURCE_EVENTS.read_text(encoding="utf-8"))
    source = {
        int(event["id"]): event
        for event in source_payload["events"]
        if int(event["id"]) in EXPECTED_CLASSES
    }
    assert set(source) == set(EXPECTED_CLASSES)

    as_of = date.fromisoformat(fixture["as_of_date"])
    for event_id, case in cases.items():
        assert case["case_id"] == f"hard_negative:{event_id}"
        assert case["label"] == "hard_negative"
        assert case["expected_family"] is None
        assert case["concept_id"] is None
        assert case["eligible"] is True
        assert case["frozen_tier"] == "ordinary"
        assert case["expected_unusual_publication"] is False
        assert len(case["confusable_families"]) >= 2
        assert len(case["semantic_boundary"]["insufficient_signals"]) >= 3
        assert case["semantic_boundary"]["ordinary_reason"].strip()
        assert case["semantic_boundary"]["required_unusual_mechanism"].strip()

        frozen = case["event_input"]
        current = source[event_id]
        assert {field: frozen.get(field) for field in SOURCE_BOUND_FIELDS} == {
            field: current.get(field) for field in SOURCE_BOUND_FIELDS
        }
        active_through = frozen["end_date"] or frozen["start_date"]
        assert date.fromisoformat(active_through) >= as_of


def test_regression_cases_use_full_shared_semantic_documents_not_keyword_rules() -> None:
    fixture = load_fixture()
    documents = {
        int(case["event_id"]): build_related_v1_document(case["event_input"])
        for case in fixture["cases"]
    }

    assert "архивные документы и фотографии" in documents[4327]["text"]
    assert "юбилеям Калининградской области" in documents[5376]["text"]
    assert documents[4327]["document_version"] == "event-related-doc-v1"
    assert documents[5376]["document_version"] == "event-related-doc-v1"
    assert documents[4327]["text_hash"] != documents[5376]["text_hash"]


def test_quality_evaluator_counts_both_semantic_regressions_as_hard_negatives() -> None:
    fixture = load_fixture()
    bank = load_unusual_prototype_bank()
    classifier = load_unusual_classifier()
    prototypes = {row["id"]: row for row in bank["prototypes"]}
    cases = {int(case["event_id"]): case for case in fixture["cases"]}
    event_documents = {
        build_related_v1_document(case["event_input"])["text"]: case
        for case in fixture["cases"]
    }
    prototype_coordinates = {
        row["text"]: index for index, row in enumerate(bank["prototypes"])
    }

    def anchored_semantic_probe(texts, *, model_revision, batch_size):
        assert model_revision == MODEL_REVISION
        assert batch_size > 0
        vectors = []
        for text in texts:
            if text in event_documents:
                anchor_id = event_documents[text]["prompt_anchor"]
                vectors.append(_unit(prototype_coordinates[prototypes[anchor_id]["text"]]))
            else:
                vectors.append(_unit(prototype_coordinates[text]))
        return vectors

    artifact = build_shared_bge_vector_artifact(
        [case["event_input"] for case in cases.values()],
        bank,
        model_revision=MODEL_REVISION,
        classifier=classifier,
        encoder=anchored_semantic_probe,
        build_metadata={
            "run_id": "unit:unusual-semantic-regressions",
            "evidence_kind": "non_production_probe",
        },
    )
    evaluation = evaluate_unusual_quality_fixture(
        fixture,
        artifact,
        bank,
        classifier,
    )

    predictions = {row["event_id"]: row for row in evaluation["predictions"]}
    assert evaluation["evidence_kind"] == "non_production_probe"
    assert evaluation["hard_negative_sample_size"] == 2
    assert evaluation["hard_negative_false_positive_rate"] == 0.0
    assert evaluation["editorial_ranked_count"] == 0
    assert evaluation["deterministic_repeat_exact"] is True
    assert set(predictions) == set(EXPECTED_CLASSES)
    assert all(row["label"] == "hard_negative" for row in predictions.values())
    assert all(row["decision"] == "ordinary" for row in predictions.values())
    assert all(row["tier"] == "ordinary" for row in predictions.values())
    assert all(
        row["decision"] not in {"core", "adjacent"} for row in predictions.values()
    )
