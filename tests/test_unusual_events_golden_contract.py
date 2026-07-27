from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


FIXTURE = Path(__file__).parent / "fixtures" / "unusual_events_golden_v1.json"
EXPECTED_FAMILIES = {
    "open_dialogue",
    "participatory",
    "co_creation",
    "behind_scenes",
    "restricted_access",
    "site_specific",
    "after_hours",
    "hybrid_format",
    "living_history",
    "field_science",
    "rare_practice",
    "gastro_experience",
    "sensory_wellbeing",
    "community_exchange",
    "quirky_ritual",
}
EXPECTED_FROZEN_TIERS = {
    "positive": "core_unusual",
    "hard_negative": "ordinary",
    "non_event": "abstain",
}


def load_fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_unusual_golden_fixture_has_frozen_source_and_all_label_classes() -> None:
    fixture = load_fixture()

    assert fixture["schema_version"] == "unusual-events-golden-v1"
    assert fixture["as_of_date"] == "2026-07-27"
    assert fixture["source"] == {
        "artifact_directory": "artifacts/codex/unusual-events-20260727",
        "snapshot_file": "prod-events.json",
        "snapshot_utc": "2026-07-27",
        "snapshot_row_count": 6587,
        "manual_review_file": "manual_future_taxonomy.csv",
        "fact_policy": (
            "Frozen title/date/type/location/summary/digest values are copied only from "
            "the read-only production snapshot; candidate concept evidence uses the "
            "manual taxonomy when present. No live-canary outcome is encoded."
        ),
        "snapshot_sha256": "9971db619edc59c347e4c9d35b473287d338bc59bb9b152bf284185787193b2d",
        "manual_review_sha256": "769e67b3222dcf9713124dfa7845d01b8f542ebc925cdeb32dc051ca48365592",
    }
    labels = Counter(case["label"] for case in fixture["cases"])
    assert labels["positive"] >= 15
    assert labels["hard_negative"] >= 20
    assert labels["non_event"] >= 5
    assert fixture["canary_evidence"] == "pending"


def test_unusual_golden_fixture_covers_exact_taxonomy_and_eventness_contract() -> None:
    fixture = load_fixture()
    assert fixture["taxonomy"]["schema_version"] == "unusual-family-taxonomy-v1"
    assert set(fixture["taxonomy"]["families"]) == EXPECTED_FAMILIES
    assert len(fixture["taxonomy"]["families"]) == 15

    cases = fixture["cases"]
    assert len({case["case_id"] for case in cases}) == len(cases)
    assert len({case["event_id"] for case in cases}) == len(cases)
    covered_families = {
        case["expected_family"] for case in cases if case["label"] == "positive"
    }
    assert covered_families == EXPECTED_FAMILIES

    for case in cases:
        assert case["source_review"]["snapshot_row_checked"] is True
        assert case["facts"]["title"].strip()
        assert case["facts"]["date"]
        assert case["checked_reason"].strip()
        assert case["eligible"] is case["expected_publication_eligible_as_of"]
        assert case["frozen_tier"] == EXPECTED_FROZEN_TIERS[case["label"]]
        assert case["expected_publication_eligible_as_of"] is (
            case["expected_eventness_eligible"]
            and case["facts"]["date"].split("..", 1)[0] >= fixture["as_of_date"]
        )
        if case["label"] == "positive":
            assert case["expected_eventness_eligible"] is True
            assert case["expected_family"] in EXPECTED_FAMILIES
            assert case["concept_id"]
        elif case["label"] == "hard_negative":
            assert case["expected_eventness_eligible"] is True
            assert case["expected_family"] is None
            assert case["concept_id"] is None
        else:
            assert case["label"] == "non_event"
            assert case["expected_eventness_eligible"] is False
            assert case["expected_family"] is None
            assert case["concept_id"] is None


def test_unusual_golden_series_groups_are_complete_and_collapse_to_one_concept() -> None:
    fixture = load_fixture()
    cases = {case["event_id"]: case for case in fixture["cases"]}
    assert len(fixture["series_groups"]) >= 5

    for group in fixture["series_groups"]:
        members = group["member_event_ids"]
        assert len(members) >= 2
        assert len(set(members)) == len(members)
        assert group["expected_max_visible_in_top_20"] == 1
        for event_id in members:
            case = cases[event_id]
            assert case["label"] == "positive"
            assert case["concept_id"] == group["concept_id"]


def test_unusual_golden_evaluation_thresholds_fail_closed() -> None:
    gate = load_fixture()["evaluation_contract"]
    assert gate == {
        "precision_at_20_min": 0.85,
        "reference_precision_at_20": 0.88,
        "reference_tolerance": 0.05,
        "hard_negative_false_positive_rate_max": 0.05,
        "confirmed_positive_recall_min": 0.8,
        "duplicate_concepts_in_top_20_max": 1,
        "identical_input_flip_rate_max": 0.02,
        "minimum_distinct_families": 5,
        "deterministic_exact_output_required": True,
        "ineligible_published_max": 0,
        "shared_vector_contract_required": True,
    }
