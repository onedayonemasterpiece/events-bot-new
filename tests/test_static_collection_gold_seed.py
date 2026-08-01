import json
from pathlib import Path


FIXTURE = Path(__file__).parent / "fixtures" / "static_collections_gold_v1.json"
EXPECTED_LABELS = {
    "science",
    "strong_impressions",
    "medieval",
    "audience_kids_candidate",
    "audience_family_candidate",
}


def load_seed():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_provisional_seed_cannot_authorize_publication():
    seed = load_seed()
    assert seed["status"] == "provisional_agent_seed_not_owner_approved"
    assert seed["publication_eligible"] is False
    assert set(seed["labels"]) == EXPECTED_LABELS


def test_review_rows_are_complete_unique_and_disjoint_per_label():
    seed = load_seed()
    for label, payload in seed["labels"].items():
        positives = payload["positives"]
        negatives = payload["hard_negatives"]
        positive_ids = [row["event_id"] for row in positives]
        negative_ids = [row["event_id"] for row in negatives]
        assert len(positive_ids) == len(set(positive_ids)), label
        assert len(negative_ids) == len(set(negative_ids)), label
        assert set(positive_ids).isdisjoint(negative_ids), label
        assert len(negatives) >= seed["acceptance"]["semantic_minimum_hard_negatives"]
        for row in [*positives, *negatives]:
            assert row["title"]
            assert row["date"]
            assert row["reason_code"]
            assert row["evidence_excerpt"]
            assert isinstance(row["bge_selected"], bool)


def test_medieval_shortfall_remains_visible_instead_of_gaming_the_gate():
    seed = load_seed()
    minimum = seed["acceptance"]["semantic_minimum_positives"]
    assert len(seed["labels"]["medieval"]["positives"]) < minimum
    for label in EXPECTED_LABELS - {"medieval"}:
        assert len(seed["labels"][label]["positives"]) >= minimum
