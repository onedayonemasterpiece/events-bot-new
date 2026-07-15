#!/usr/bin/env python3
"""Score versioned event-age predictions without calling an LLM."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


ORDER = {"0+": 0, "6+": 1, "12+": 2, "16+": 3, "18+": 4}


def division(num: int, den: int) -> float | None:
    return round(num / den, 6) if den else None


def structured_baseline(cases: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(case["id"]): {
            "status": "declared" if case.get("kind") == "structured" else "unknown",
            "value": case.get("input") if case.get("kind") == "structured" else None,
        }
        for case in cases
    }


def evaluate(cases: list[dict[str, Any]], predictions: dict[str, Any]) -> dict[str, Any]:
    tp = fp = fn = negative_total = false_positive = 0
    conflicts = unknown = 0
    assessed_total = assessed_correct = critical_over_permissive = 0
    status_counts: Counter[str] = Counter()
    for case in cases:
        pred = predictions.get(str(case["id"])) or {}
        status = str(pred.get("status") or "unknown")
        value = pred.get("value") or pred.get("age_assessment")
        status_counts[status] += 1
        conflicts += status == "conflict"
        unknown += status in {"unknown", "insufficient_evidence", "budget_deferred"}
        expected = case.get("expected")
        declared_truth = expected in ORDER and case.get("kind") in {"structured", "semantic"}
        declared_prediction = status == "declared" and value in ORDER
        if declared_truth and declared_prediction and value == expected:
            tp += 1
        elif declared_prediction and (not declared_truth or value != expected):
            fp += 1
        elif declared_truth:
            fn += 1
        if expected is None and case.get("kind") == "negative":
            negative_total += 1
            false_positive += bool(value in ORDER)
        if case.get("kind") == "assessment" and expected in ORDER:
            assessed_total += 1
            assessed_correct += status == "assessed" and value == expected
            if status == "assessed" and value in ORDER and ORDER[value] < ORDER[expected]:
                critical_over_permissive += 1
    return {
        "schema_version": "event-age-golden-eval-v1",
        "case_count": len(cases),
        "declared_precision": division(tp, tp + fp),
        "declared_recall": division(tp, tp + fn),
        "false_positive_rate": division(false_positive, negative_total),
        "conflict_share": division(conflicts, len(cases)),
        "unknown_share": division(unknown, len(cases)),
        "assessment_agreement": division(assessed_correct, assessed_total),
        "critical_over_permissive_rate": division(critical_over_permissive, assessed_total),
        "status_counts": dict(status_counts),
        "limitations": [
            "Null assessment metrics mean that no approved human-labeled assessment set was supplied.",
            "The built-in baseline measures structured-field preservation only, not LLM/BGE semantic quality.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--predictions", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = json.loads(args.fixture.read_text(encoding="utf-8"))
    predictions = (
        json.loads(args.predictions.read_text(encoding="utf-8"))
        if args.predictions
        else structured_baseline(cases)
    )
    report = evaluate(cases, predictions)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
