from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REVIEW_DIR = ROOT / "docs" / "review-data" / "static-collections-source-reviews-v1"
REQUIRED_IDS = {
    5757, 5781, 6562, 6696, 6766, 6871, 6878, 6898, 7054, 7102,
    7113, 7114, 7172, 7176, 7237, 7238, 7258, 7290, 7307, 7326,
    7333, 7344, 7373, 7374,
}


def stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class StaticCollectionDataQualityReviewTests(unittest.TestCase):
    def test_source_review_receipts_cover_known_defects_and_bind_raw_quotes(self) -> None:
        index = json.loads((REVIEW_DIR / "index.json").read_text(encoding="utf-8"))
        covered: set[int] = set()
        for entry in index["receipts"]:
            receipt = json.loads((REVIEW_DIR / entry["path"]).read_text(encoding="utf-8"))
            self.assertEqual(entry["receipt_id"], receipt["receipt_id"])
            self.assertEqual(entry["status"], receipt["status"])
            self.assertEqual(entry["event_ids"], receipt["event_ids"])
            covered.update(receipt["event_ids"])
            unhashed = dict(receipt)
            declared = unhashed.pop("receipt_sha256")
            self.assertEqual(declared, stable_hash(unhashed))
            self.assertEqual(declared, entry["receipt_sha256"])
            self.assertIn(receipt["status"], {"pass", "needs_source_review", "corrected"})
            self.assertTrue(receipt["source_evidence"])
            for evidence in receipt["source_evidence"]:
                quote = evidence["raw_source_quote"]
                self.assertTrue(quote)
                self.assertEqual(
                    evidence["raw_source_quote_sha256"],
                    hashlib.sha256(quote.encode("utf-8")).hexdigest(),
                )
                self.assertTrue(evidence["source_ref"]["source_url"])
                self.assertEqual(len(evidence["source_ref"]["source_text_sha256"]), 64)
                kind = evidence["raw_source_quote_kind"]
                self.assertIn(kind, {"full", "excerpt"})
                self.assertEqual(
                    evidence["raw_source_quote_truncated"], kind == "excerpt"
                )
                self.assertEqual(
                    evidence["raw_source_quote_char_count"], len(quote)
                )
        self.assertEqual(REQUIRED_IDS, covered)
        self.assertEqual(sorted(REQUIRED_IDS), index["required_event_ids"])

    def test_unresolved_rows_do_not_enter_review_supply(self) -> None:
        seed = json.loads(
            (ROOT / "docs" / "review-data" / "static_collections_review_seed_v1.json").read_text(
                encoding="utf-8"
            )
        )
        selected = {
            row["event_id"]
            for payload in seed["labels"].values()
            for side in ("positives", "hard_negatives")
            for row in payload[side]
        }
        self.assertTrue(
            {
                5757, 6766, 6871, 6878, 7307, 7333, 7344,
                6818, 6865, 7131, 5344, 4211, 5376, 7185, 7281,
                7244, 7247, 7293,
            }.isdisjoint(selected)
        )
        self.assertIn(
            7326,
            {row["event_id"] for row in seed["labels"]["family_suitable"]["positives"]},
        )

    def test_review_family_overrides_match_source_findings(self) -> None:
        seed = json.loads(
            (ROOT / "docs" / "review-data" / "static_collections_review_seed_v1.json").read_text(
                encoding="utf-8"
            )
        )
        rows = {
            row["event_id"]: row
            for payload in seed["labels"].values()
            for side in ("positives", "hard_negatives")
            for row in payload[side]
        }
        self.assertEqual(
            rows[6696]["family_id"], "reviewed-duplicate:pinhole-gusev-2026-07-07"
        )
        self.assertTrue(rows[5781]["family_id"].startswith("linked:"))
        self.assertEqual(rows[7373]["family_id"], rows[7374]["family_id"])
        self.assertEqual(rows[7054]["family_id"], "event:7054")

    def test_6871_receipt_is_occurrence_specific_and_fail_closed(self) -> None:
        receipt = json.loads((REVIEW_DIR / "6871.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["status"], "needs_source_review")
        evidence = receipt["source_evidence"][0]
        self.assertEqual(evidence["source_ref"]["source_id"], 9603588)
        self.assertIn("8 августа, 21:00", evidence["raw_source_quote"])
        self.assertIn("Вечерний кинопоказ", evidence["raw_source_quote"])

    def test_corrected_audience_receipts_bind_exact_review_sources(self) -> None:
        promoted = json.loads((REVIEW_DIR / "6562.json").read_text(encoding="utf-8"))
        self.assertEqual(promoted["status"], "corrected")
        self.assertEqual(promoted["source_evidence"][0]["source_ref"]["source_id"], 7734705)
        self.assertEqual(
            promoted["semantic_review_decision"]["family_suitable"],
            "move_hard_negative_to_positive",
        )

        removed = json.loads(
            (REVIEW_DIR / "6898-7102-7172-7176-7258-7290.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            [item["source_ref"]["source_id"] for item in removed["source_evidence"]],
            [8817752, 9470752, 9573634, 9573638, 9603530, 9745554],
        )


if __name__ == "__main__":
    unittest.main()
