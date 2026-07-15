#!/usr/bin/env python3
"""Fit and automatically gate the CPU-BGE event-age dual head.

The label file is machine-produced.  Production eligibility is evaluated only
on ``official_source_declared`` rows (an explicit age mark from the event
source, removed from the encoded text before embedding).  Codex+Gemini exact
agreement may be supplied as ``ai_consensus_silver`` for training, but never
counts as the official holdout or as approval by a person.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np


CLASSES = ["0+", "6+", "12+", "16+", "18+"]
CLASS_INDEX = {value: index for index, value in enumerate(CLASSES)}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_bucket(value: str, modulo: int = 10) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:8], 16) % modulo


def load_labels(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("label") not in CLASS_INDEX:
            raise ValueError(f"invalid age label: {row.get('label')!r}")
        origin = str(row.get("label_origin") or "")
        if origin not in {"official_source_declared", "ai_consensus_silver"}:
            raise ValueError(f"unsupported label_origin: {origin!r}")
        if not row.get("input_hash"):
            raise ValueError("each label requires input_hash")
        rows.append(row)
    return rows


def softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max(axis=1, keepdims=True)
    exp = np.exp(shifted)
    return exp / np.maximum(exp.sum(axis=1, keepdims=True), 1e-12)


def fit_ridge_head(
    x: np.ndarray,
    y: np.ndarray,
    *,
    ridge: float,
    sample_weight: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    target = np.eye(len(CLASSES), dtype="float64")[y]
    design = np.concatenate([x.astype("float64"), np.ones((len(x), 1))], axis=1)
    root_weight = np.sqrt(sample_weight.astype("float64"))[:, None]
    weighted = design * root_weight
    penalty = np.eye(design.shape[1], dtype="float64") * ridge
    penalty[-1, -1] = 0.0
    params = np.linalg.solve(weighted.T @ weighted + penalty, weighted.T @ (target * root_weight))
    return params[:-1].astype("float32"), params[-1].astype("float32")


def derived_cutoff(probabilities: np.ndarray, predicted: np.ndarray, truth: np.ndarray) -> float:
    correct = predicted == truth
    if not np.any(correct):
        return 1.0
    winning = probabilities[np.arange(len(predicted)), predicted][correct]
    # Five-way linear heads often produce conservative softmax values even on
    # cleanly separated classes.  Keep the floor just above chance (0.20) and
    # let official-holdout accuracy/under-rate/coverage gates decide safety.
    return round(float(np.clip(np.quantile(winning, 0.10), 0.21, 0.90)), 6)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vectors", type=Path, required=True)
    parser.add_argument("--labels", type=Path, required=True)
    parser.add_argument("--prototype-bank", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-class-support", type=int, default=5)
    parser.add_argument("--min-exact-accuracy", type=float, default=0.72)
    parser.add_argument("--min-within-one-accuracy", type=float, default=0.95)
    parser.add_argument("--min-accepted-coverage", type=float, default=0.50)
    parser.add_argument("--max-under-rate", type=float, default=0.10)
    args = parser.parse_args()

    payload = np.load(args.vectors, allow_pickle=False)
    vectors = np.asarray(payload["vectors"], dtype="float32")
    hashes = [str(value) for value in payload["input_hashes"].tolist()]
    if len(vectors) != len(hashes):
        raise ValueError("vector/input_hash count mismatch")
    revision = str(payload["model_revision"].item())
    contract = str(payload["encoder_contract"].item())
    bank_hash = str(payload["prototype_bank_hash"].item())
    if file_sha256(args.prototype_bank) == "":  # pragma: no cover - defensive
        raise ValueError("prototype bank is unreadable")
    bank_payload = json.loads(args.prototype_bank.read_text(encoding="utf-8"))
    computed_bank_hash = hashlib.sha256(
        json.dumps(bank_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if computed_bank_hash != bank_hash:
        raise ValueError("prototype bank hash does not match encoded vectors")

    labels = {str(row["input_hash"]): row for row in load_labels(args.labels)}
    matched = [(index, labels[value]) for index, value in enumerate(hashes) if value in labels]
    if not matched:
        raise ValueError("no labels match encoded input hashes")
    x = np.asarray([vectors[index] for index, _ in matched], dtype="float32")
    y = np.asarray([CLASS_INDEX[str(row["label"])] for _, row in matched], dtype="int64")
    origins = [str(row["label_origin"]) for _, row in matched]
    groups = [str(row.get("group_id") or row.get("event_id") or row["input_hash"]) for _, row in matched]
    buckets = np.asarray([stable_bucket(group) for group in groups])
    official = np.asarray([origin == "official_source_declared" for origin in origins])
    holdout = official & (buckets >= 8)
    calibration = official & (buckets == 7)
    train = buckets < 7
    if not np.any(train) or not np.any(calibration) or not np.any(holdout):
        raise ValueError("grouped split needs train, calibration, and official holdout rows")

    rng = np.random.default_rng(20260715)
    train_indices = np.flatnonzero(train)
    # Two deterministic bootstrap fits make the agreement check independent of
    # prototype retrieval and materially reduce single-head overconfidence.
    weight_a = rng.poisson(1.0, len(train_indices)).astype("float64") + 0.25
    weight_b = rng.poisson(1.0, len(train_indices)).astype("float64") + 0.25
    weights_a, bias_a = fit_ridge_head(x[train], y[train], ridge=0.75, sample_weight=weight_a)
    weights_b, bias_b = fit_ridge_head(x[train], y[train], ridge=1.25, sample_weight=weight_b)

    pa_cal = softmax(x[calibration] @ weights_a + bias_a)
    pb_cal = softmax(x[calibration] @ weights_b + bias_b)
    ya_cal = y[calibration]
    pred_a_cal = pa_cal.argmax(axis=1)
    pred_b_cal = pb_cal.argmax(axis=1)
    cutoff_a = derived_cutoff(pa_cal, pred_a_cal, ya_cal)
    cutoff_b = derived_cutoff(pb_cal, pred_b_cal, ya_cal)

    x_eval, y_eval = x[holdout], y[holdout]
    pa = softmax(x_eval @ weights_a + bias_a)
    pb = softmax(x_eval @ weights_b + bias_b)
    pred_a, pred_b = pa.argmax(axis=1), pb.argmax(axis=1)
    accepted = (
        (pred_a == pred_b)
        & (pa[np.arange(len(pa)), pred_a] >= cutoff_a)
        & (pb[np.arange(len(pb)), pred_b] >= cutoff_b)
    )
    accepted_count = int(accepted.sum())
    accepted_pred = pred_a[accepted]
    accepted_truth = y_eval[accepted]
    exact = float(np.mean(accepted_pred == accepted_truth)) if accepted_count else 0.0
    within_one = float(np.mean(np.abs(accepted_pred - accepted_truth) <= 1)) if accepted_count else 0.0
    under = float(np.mean(accepted_pred < accepted_truth)) if accepted_count else 1.0
    severe_under = float(np.mean(accepted_pred + 1 < accepted_truth)) if accepted_count else 1.0
    agreement = float(np.mean(pred_a == pred_b))
    coverage = float(np.mean(accepted))
    support = {label: int(np.sum(y_eval == index)) for index, label in enumerate(CLASSES)}
    gates = {
        "minimum_class_support": all(value >= args.min_class_support for value in support.values()),
        "zero_severe_under_rate": severe_under == 0.0,
        "under_rate": under <= args.max_under_rate,
        "exact_accuracy": exact >= args.min_exact_accuracy,
        "within_one_accuracy": within_one >= args.min_within_one_accuracy,
        "accepted_coverage": coverage >= args.min_accepted_coverage,
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    classifier_path = args.output_dir / "event_age_bge_classifier.npz"
    np.savez_compressed(
        classifier_path,
        classes=np.asarray(CLASSES),
        weights_a=weights_a,
        bias_a=bias_a,
        weights_b=weights_b,
        bias_b=bias_b,
    )
    manifest = {
        "schema_version": "event-age-bge-evaluation-v2",
        "approval_status": "approved" if all(gates.values()) else "shadow",
        "gate_authority": "automatic_quality_gate_v1",
        "quality_gates_passed": bool(all(gates.values())),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_revision": revision,
        "encoder_contract": contract,
        "prototype_bank_hash": bank_hash,
        "classifier_sha256": file_sha256(classifier_path),
        "evaluation_dataset_hash": file_sha256(args.labels),
        "labeled_case_count": int(len(y_eval)),
        "class_support": support,
        "assessment_agreement": round(agreement, 6),
        "accepted_coverage": round(coverage, 6),
        "exact_accuracy": round(exact, 6),
        "within_one_accuracy": round(within_one, 6),
        "under_rate": round(under, 6),
        "critical_over_permissive_rate": round(severe_under, 6),
        "head_a_min_probability": cutoff_a,
        "head_b_min_probability": cutoff_b,
        "gates": gates,
        "label_policy": {
            "production_holdout": "official_source_declared_only",
            "explicit_age_tokens_masked_before_embedding": True,
            "ai_consensus_silver": "training_only",
            "human_approval_required": False,
        },
    }
    (args.output_dir / "event_age_bge_evaluation.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False))
    return 0 if manifest["quality_gates_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
