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
                self.assertIn(row["source_quote_kind"], {"full", "excerpt"})
                self.assertEqual(
                    row["source_quote_truncated"], row["source_quote_kind"] == "excerpt"
                )
                self.assertEqual(row["source_quote_char_count"], len(row["source_quote"]))

    def test_short_supply_is_visible_and_not_padded_with_duplicate_occurrences(self) -> None:
        seed = load(SEED)
        minimum = seed["acceptance"]["semantic_minimum_positives"]
        expected_short = {
            "science_pop", "research_in_action", "strong_impressions",
            "medieval", "family_suitable", "joint_family_activity",
        }
        actual = {
            label
            for label, payload in seed["labels"].items()
            if payload["counts"]["positive_families"] < minimum
        }
        self.assertEqual(actual, expected_short)

    def test_known_unsupported_semantic_rows_are_absent(self) -> None:
        seed = load(SEED)
        self.assertNotIn(
            4648,
            {row["event_id"] for row in seed["labels"]["science_pop"]["positives"]},
        )
        self.assertNotIn(
            4648,
            {row["event_id"] for row in seed["labels"]["family_suitable"]["positives"]},
        )
        self.assertNotIn(
            6871,
            {
                row["event_id"]
                for payload in seed["labels"].values()
                for side in ("positives", "hard_negatives")
                for row in payload[side]
            },
        )

    def test_festival_scope_distinguishes_parents_children_and_pages(self) -> None:
        seed = load(SEED)
        self.assertEqual(
            seed["review_scope"],
            {
                "cinema_sources": "excluded",
                "cinema_events": "excluded_from_review_rows",
                "festival_parent_rows": "excluded_from_review_rows",
                "festival_child_events": "allowed_with_occurrence_specific_source",
                "festival_extraction_pages": "out_of_scope",
            },
        )
        rows = [
            row
            for payload in seed["labels"].values()
            for side in ("positives", "hard_negatives")
            for row in payload[side]
        ]
        self.assertTrue({4211, 5376, 7185, 7281}.isdisjoint(row["event_id"] for row in rows))
        festival_children = [row for row in rows if "festival_scope_kind" in row]
        self.assertTrue(festival_children)
        self.assertTrue(
            all(
                row["festival_scope_kind"] == "festival_child_event"
                and row["occurrence_specific_source"] is True
                for row in festival_children
            )
        )
        self.assertNotIn(7103, {row["event_id"] for row in rows})

    def test_generator_provenance_is_role_specific_and_reproducible(self) -> None:
        source = load(SEED)["source"]
        self.assertNotIn("generator_repo_sha", source)
        for field in (
            "extraction_repo_sha",
            "seed_builder_repo_sha",
            "integration_repo_sha",
        ):
            self.assertRegex(source[field], r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
        self.assertTrue(source["generator_command"].startswith(
            "python3 scripts/build_static_collections_review_seed.py "
        ))
        self.assertIn("--snapshot", source["generator_command"])
        self.assertIn("--output", source["generator_command"])


if __name__ == "__main__":
    unittest.main()
