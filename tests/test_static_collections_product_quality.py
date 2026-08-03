from __future__ import annotations

import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_static_collections_product_quality.py"
SPEC = importlib.util.spec_from_file_location("collections_product_quality", SCRIPT)
assert SPEC and SPEC.loader
quality = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = quality
SPEC.loader.exec_module(quality)

FIXTURE_DIR = ROOT / "tests" / "fixtures" / "static_collections_product_quality"
LIVE_REGRESSION = (
    ROOT / "docs" / "review-data" / "static_collections_audience_live_regression_v1.json"
)


def load(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def evaluate(current: dict, *, baseline: dict | None = None, regression: dict | None = None):
    return quality.evaluate_snapshot(
        current,
        baseline=baseline or {},
        regression=regression or {},
        today=date(2026, 8, 2),
        now=datetime(2026, 8, 2, tzinfo=timezone.utc),
    )


class StaticCollectionsProductQualityTests(unittest.TestCase):
    def test_partial_or_unknown_coverage_is_watch(self) -> None:
        current = load("current.json")
        current["coverage"] = {
            "status": "partial",
            "candidate_event_count": 10,
            "evaluated_event_count": 7,
            "deferred_event_count": 1,
            "unprocessed_event_count": 2,
        }
        report = evaluate(current, baseline=load("baseline.json"))
        self.assertEqual(report["status"], "WATCH")
        self.assertIn("coverage_partial", {item["code"] for item in report["issues"]})
        current.pop("coverage")
        report = evaluate(current, baseline=load("baseline.json"))
        self.assertIn("coverage_unknown", {item["code"] for item in report["issues"]})

    def test_live_regression_pack_uses_real_disjoint_audience_examples(self) -> None:
        pack = json.loads(LIVE_REGRESSION.read_text(encoding="utf-8"))
        self.assertFalse(pack["publication_eligible"])
        expected_ids = {6737, 6797, 6562, 7326, 7372, 7307, 7102, 7258, 7290}
        seen: set[int] = set()
        for config in pack["collections"].values():
            positives = set(config.get("must_include_event_ids") or [])
            negatives = set(config.get("must_exclude_event_ids") or [])
            self.assertTrue(positives.isdisjoint(negatives))
            seen.update(positives | negatives)
        self.assertEqual(seen, expected_ids)

    def test_example_fixture_reports_watch_not_failure(self) -> None:
        report = evaluate(
            load("current.json"),
            baseline=load("baseline.json"),
            regression=load("regression.json"),
        )
        self.assertEqual(report["status"], "WATCH")
        codes = {item["code"] for item in report["issues"]}
        self.assertIn("low_supply", codes)
        self.assertNotIn("known_false_positive_returned", codes)

    def test_duplicate_family_is_a_product_failure(self) -> None:
        current = load("current.json")
        duplicate = copy.deepcopy(current["collections"]["kids"]["items"][0])
        duplicate["event_id"] = 777
        current["collections"]["kids"]["items"].append(duplicate)
        report = evaluate(current, baseline=load("baseline.json"))
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("duplicate_families", {item["code"] for item in report["issues"]})

    def test_public_empty_is_failure_but_shadow_empty_is_watch(self) -> None:
        current = load("current.json")
        current["collections"]["kids"]["items"] = []
        current["collections"]["science_pop"]["items"] = []
        report = evaluate(current, baseline=load("baseline.json"))
        self.assertEqual(report["status"], "FAIL")
        by_code = {item["code"] for item in report["issues"]}
        self.assertIn("public_collection_empty", by_code)
        self.assertIn("nonpublic_collection_empty", by_code)

    def test_source_grounding_required_fails_even_for_shadow_snapshot(self) -> None:
        snapshot = {
            "generated_at": "2026-08-02T10:00:00Z",
            "collections": {
                "family_suitable": {
                    "mode": "shadow",
                    "source_grounding_required": True,
                    "items": [
                        {
                            "event_id": 7103,
                            "family_id": "event:7103",
                            "start_date": "2026-08-10",
                            "source_status": "blocked",
                            "review_status": "needs_source_review",
                        }
                    ],
                }
            },
        }
        report = evaluate(snapshot)
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(
            {"review_blocked_results", "source_grounding_missing"}
            <= {issue["code"] for issue in report["issues"]}
        )

    def test_known_false_positive_is_failure(self) -> None:
        current = load("current.json")
        current["collections"]["kids"]["items"].append(
            {
                "event_id": 999001,
                "family_id": "family:known-bad",
                "start_date": "2026-08-09",
                "source_status": "grounded",
                "review_status": "accepted",
            }
        )
        report = evaluate(
            current,
            baseline=load("baseline.json"),
            regression=load("regression.json"),
        )
        self.assertEqual(report["status"], "FAIL")
        self.assertIn(
            "known_false_positive_returned",
            {item["code"] for item in report["issues"]},
        )

    def test_known_positive_missing_is_watch(self) -> None:
        current = load("current.json")
        current["collections"]["kids"]["items"] = [
            row
            for row in current["collections"]["kids"]["items"]
            if row["family_id"] != "family:kids-1"
        ]
        report = evaluate(
            current,
            baseline=load("baseline.json"),
            regression=load("regression.json"),
        )
        self.assertEqual(report["status"], "WATCH")
        self.assertIn("known_positive_missing", {item["code"] for item in report["issues"]})

    def test_same_input_changed_output_is_failure(self) -> None:
        baseline = load("baseline.json")
        current = copy.deepcopy(baseline)
        current["collections"]["kids"]["items"].pop()
        report = evaluate(current, baseline=baseline)
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("same_input_changed_output", {item["code"] for item in report["issues"]})

    def test_failed_rebuild_must_keep_last_good(self) -> None:
        current = load("current.json")
        current["collections"]["kids"].update(
            {"state": "failed", "using_last_good": False, "items": []}
        )
        report = evaluate(current, baseline=load("baseline.json"))
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("last_good_lost", {item["code"] for item in report["issues"]})

    def test_extra_internal_fields_do_not_break_product_monitor(self) -> None:
        current = load("current.json")
        current["new_internal_schema_version"] = "v999"
        current["collections"]["kids"]["new_engine_payload"] = {"anything": [1, 2, 3]}
        report = evaluate(
            current,
            baseline=load("baseline.json"),
            regression=load("regression.json"),
        )
        self.assertIn(report["status"], {"HEALTHY", "WATCH"})

    def test_markdown_and_json_are_actionable(self) -> None:
        report = evaluate(
            load("current.json"),
            baseline=load("baseline.json"),
            regression=load("regression.json"),
        )
        markdown = quality.render_markdown(report)
        self.assertIn("Static collections product quality", markdown)
        self.assertIn("science_pop", markdown)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.json"
            path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
            self.assertGreater(path.stat().st_size, 100)


if __name__ == "__main__":
    unittest.main()
