from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "docs" / "review-data" / "static_collections_review_seed_v1.json"
POLICY = ROOT / "site" / "scripts" / "static_collection_policy.v2.json"
EXPECTED_LABELS = {
    "child_directed", "family_suitable", "joint_family_activity",
    "science_pop", "research_in_action", "strong_impressions", "medieval",
}
LEGACY_LABELS = {"science", "audience_kids_candidate", "audience_family_candidate"}


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class StaticCollectionReviewSeedTests(unittest.TestCase):
    def test_review_seed_is_provisional_non_publishable_and_uses_ontology_v2(self) -> None:
        seed = load(SEED)
        policy = load(POLICY)
        self.assertEqual(seed["schema_version"], "static-collections-review-seed-v1")
        self.assertEqual(seed["status"], "provisional_agent_seed_not_owner_approved")
        self.assertIs(seed["publication_eligible"], False)
        self.assertEqual(seed["ontology_version"], "static-collection-ontology-v2")
        self.assertEqual(set(seed["labels"]), EXPECTED_LABELS)
        self.assertFalse(set(seed["labels"]) & LEGACY_LABELS)
        self.assertEqual(
            seed["source"]["policy_sha256"], hashlib.sha256(POLICY.read_bytes()).hexdigest()
        )
        self.assertEqual(
            policy["public_projections"]["kids"]["members"],
            ["child_directed", "family_suitable"],
        )
        self.assertEqual(policy["public_projections"]["kids"]["publication"], "blocked")

    def test_review_rows_have_source_provenance_and_unique_families(self) -> None:
        seed = load(SEED)
        for label, payload in seed["labels"].items():
            positives = payload["positives"]
            negatives = payload["hard_negatives"]
            self.assertGreaterEqual(
                len(negatives), seed["acceptance"]["semantic_minimum_hard_negatives"]
            )
            for side in (positives, negatives):
                ids = [row["event_id"] for row in side]
                families = [row["family_id"] for row in side]
                self.assertEqual(len(ids), len(set(ids)), label)
                self.assertEqual(len(families), len(set(families)), label)
            self.assertTrue(
                {row["family_id"] for row in positives}.isdisjoint(
                    row["family_id"] for row in negatives
                ),
                label,
            )
            for row in [*positives, *negatives]:
                self.assertTrue(row["occurrence_date"])
                self.assertEqual(row["review_decision"], "keep")
                self.assertTrue(row["source_quote"])
                self.assertEqual(
                    row["source_quote_sha256"],
                    hashlib.sha256(row["source_quote"].encode("utf-8")).hexdigest(),
                )
                self.assertEqual(len(row["model_document_hash"]), 64)
                self.assertEqual(row["score_status"], "pending_pr_b")
                self.assertTrue(row["source_refs"])
                self.assertEqual(len(row["source_refs"][0]["source_text_sha256"]), 64)

    def test_short_supply_is_visible_and_not_padded_with_duplicate_occurrences(self) -> None:
        seed = load(SEED)
        minimum = seed["acceptance"]["semantic_minimum_positives"]
        expected_short = {
            "science_pop", "research_in_action", "strong_impressions",
            "medieval", "joint_family_activity",
        }
        actual = {
            label
            for label, payload in seed["labels"].items()
            if payload["counts"]["positive_families"] < minimum
        }
        self.assertEqual(actual, expected_short)


if __name__ == "__main__":
    unittest.main()
