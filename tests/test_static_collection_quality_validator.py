from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate_static_collections_quality.py"
SPEC = importlib.util.spec_from_file_location("static_collections_quality", SCRIPT)
assert SPEC and SPEC.loader
quality = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = quality
SPEC.loader.exec_module(quality)


def _policy(*, publication: str = "blocked") -> dict:
    return {
        "schema_version": "static-collection-policy-v1",
        "semantic_quality_gate": {
            "minimum_positives": 2,
            "minimum_hard_negatives": 2,
            "minimum_recall": 0.8,
            "maximum_hard_negative_fpr": 0.05,
        },
        "labels": {
            "science": {
                "strategy": "semantic_bge",
                "publication": publication,
                "positive_prefix": "science.positive.",
                "negative_prefix": "science.hard_negative.",
                "minimum_positive_similarity": 0.42,
                "minimum_margin": 0.02,
            }
        },
    }


def _legacy_row(event_id: int, expected: str) -> dict:
    return {
        "event_id": event_id,
        "title": f"Event {event_id}",
        "date": "2026-08-02",
        "expected": expected,
        "confidence": "high",
        "reason_code": "fixture",
        "bge_selected": False,
        "evidence_excerpt": "Source-bound evidence for the fixture.",
    }


def _legacy_seed(*, publication_eligible: bool = False) -> dict:
    positives = [
        _legacy_row(1, "positive_candidate"),
        _legacy_row(2, "positive_candidate"),
    ]
    negatives = [
        _legacy_row(10, "hard_negative"),
        _legacy_row(11, "hard_negative"),
    ]
    return {
        "schema_version": "static-collections-gold-v1",
        "status": "provisional_agent_seed_not_owner_approved",
        "publication_eligible": publication_eligible,
        "acceptance": {
            "semantic_minimum_positives": 2,
            "semantic_minimum_hard_negatives": 2,
        },
        "source": {"catalog_hash": "a" * 64},
        "labels": {
            "science": {
                "definition": "Research method and primary evidence.",
                "positives": positives,
                "hard_negatives": negatives,
                "counts": {
                    "positive_candidates": 2,
                    "high_confidence_positives": 2,
                    "hard_negatives": 2,
                },
            }
        },
    }


class StaticCollectionsQualityValidatorTests(unittest.TestCase):
    def _run(
        self,
        *,
        policy: dict,
        seed: dict,
        mode: str = "baseline",
    ) -> tuple[int, dict]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy_path = root / "policy.json"
            seed_path = root / "legacy_gold.json"
            report_path = root / "report.json"
            policy_path.write_text(json.dumps(policy), encoding="utf-8")
            seed_path.write_text(json.dumps(seed), encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                result = quality.main(
                    [
                        "--mode",
                        mode,
                        "--policy",
                        str(policy_path),
                        "--seed",
                        str(seed_path),
                        "--json-report",
                        str(report_path),
                    ]
                )
            report = json.loads(report_path.read_text(encoding="utf-8"))
            return result, report

    def test_baseline_accepts_legacy_seed_only_as_fail_closed_review_data(self) -> None:
        result, report = self._run(policy=_policy(), seed=_legacy_seed())
        self.assertEqual(result, 0)
        self.assertTrue(report["ok"])
        codes = {issue["code"] for issue in report["issues"]}
        self.assertIn("legacy_review_seed_named_gold", codes)
        self.assertIn("legacy_rows_not_reproducible", codes)

    def test_provisional_seed_cannot_be_publication_eligible(self) -> None:
        result, report = self._run(
            policy=_policy(),
            seed=_legacy_seed(publication_eligible=True),
        )
        self.assertEqual(result, 1)
        self.assertIn(
            "provisional_seed_publishable",
            {issue["code"] for issue in report["issues"]},
        )

    def test_semantic_policy_cannot_become_public_without_quality_receipt(self) -> None:
        result, report = self._run(
            policy=_policy(publication="public"),
            seed=_legacy_seed(),
        )
        self.assertEqual(result, 1)
        self.assertIn(
            "policy_publication_leak",
            {issue["code"] for issue in report["issues"]},
        )

    def test_strict_mode_rejects_legacy_gold_and_missing_provenance(self) -> None:
        result, report = self._run(
            policy=_policy(),
            seed=_legacy_seed(),
            mode="strict",
        )
        self.assertEqual(result, 1)
        codes = {issue["code"] for issue in report["issues"]}
        self.assertIn("review_seed_schema_legacy", codes)
        self.assertIn("review_seed_named_gold", codes)
        self.assertIn("family_id_missing", codes)
        self.assertIn("source_quote_missing", codes)
        self.assertIn("owner_gold_missing", codes)

    def test_repository_migrated_review_contract_passes_without_owner_gold(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "review-report.json"
            with contextlib.redirect_stdout(io.StringIO()):
                result = quality.main(
                    [
                        "--mode",
                        "review",
                        "--json-report",
                        str(report_path),
                    ]
                )
            report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(result, 0)
        self.assertTrue(report["ok"])
        self.assertEqual(report["mode"], "review")
        self.assertNotIn(
            "owner_gold_missing", {issue["code"] for issue in report["issues"]}
        )
        self.assertFalse(
            (ROOT / "tests" / "fixtures" / "static_collections_gold_v1.json").exists()
        )


if __name__ == "__main__":
    unittest.main()
