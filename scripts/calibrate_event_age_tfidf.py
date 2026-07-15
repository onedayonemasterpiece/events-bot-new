#!/usr/bin/env python3
"""Fit and automatically gate the lexical safety cascade for event ages.

Training uses sklearn, but the exported artifact contains only raw matrices,
IDF and vocabulary. Kaggle inference is implemented without sklearn and is
verified by deterministic logit/decision self-tests stored in the gate JSON.
"""

from __future__ import annotations

import argparse
import collections
import gc
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.svm import LinearSVC

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_WORKER_PATH = ROOT / "kaggle" / "EventAgeBgeAssessment" / "event_age_bge_assessment.py"
_WORKER_SPEC = importlib.util.spec_from_file_location("event_age_bge_assessment_runtime", _WORKER_PATH)
if not _WORKER_SPEC or not _WORKER_SPEC.loader:  # pragma: no cover - repository invariant
    raise RuntimeError(f"cannot import event age worker from {_WORKER_PATH}")
_WORKER = importlib.util.module_from_spec(_WORKER_SPEC)
_WORKER_SPEC.loader.exec_module(_WORKER)
classify_with_safety_cascade = _WORKER.classify_with_safety_cascade
safety_cascade_logits = _WORKER.safety_cascade_logits
stable_hash = _WORKER.stable_hash
validate_safety_cascade_self_tests = _WORKER.validate_safety_cascade_self_tests


CLASSES = ["0+", "6+", "12+", "16+", "18+"]
CLASS_INDEX = {value: index for index, value in enumerate(CLASSES)}
CLASSIFIER_KIND = "char_tfidf_safety_cascade_v1"
DEV_SEEDS = [20260715, 20260716]
ACCEPTANCE_SEED = 20260717


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def combined_sha256(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode("utf-8"))
        digest.update(bytes.fromhex(file_sha256(path)))
    return digest.hexdigest()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def normalized_group(row: dict[str, Any]) -> str:
    value = str(row.get("group_id") or row.get("event_id") or row["input_hash"])
    # The source host must not split repeated productions with the same title.
    title = value.split(":", 1)[-1]
    return " ".join(re.findall(r"[a-zа-яё0-9]+", title.casefold())) or value


def load_training_rows(
    inputs_path: Path, labels_path: Path, scope_review_path: Path | None
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    inputs = {str(row["input_hash"]): row for row in load_jsonl(inputs_path)}
    accepted_ids: set[int] | None = None
    if scope_review_path:
        review = json.loads(scope_review_path.read_text(encoding="utf-8"))
        accepted_ids = {int(value) for value in review.get("accept_ids") or []}
        if not accepted_ids:
            raise ValueError("scope review contains no accepted event ids")
    rows: list[dict[str, Any]] = []
    stats = collections.Counter()
    for label in load_jsonl(labels_path):
        value = str(label.get("label") or "")
        origin = str(label.get("label_origin") or "")
        if value not in CLASS_INDEX:
            raise ValueError(f"invalid label: {value!r}")
        if origin not in {
            "official_source_declared",
            "source_declared_candidate",
            "ai_consensus_silver",
        }:
            raise ValueError(f"unsupported label origin: {origin!r}")
        if origin == "ai_consensus_silver":
            stats["silver_excluded_from_evaluation"] += 1
            continue
        event_id = int(label.get("event_id") or 0)
        if accepted_ids is not None and event_id not in accepted_ids:
            stats["scope_excluded"] += 1
            continue
        input_row = inputs.get(str(label.get("input_hash") or ""))
        if input_row is None:
            stats["missing_input"] += 1
            continue
        rows.append(
            {
                **label,
                "event_id": event_id,
                "text": str(input_row.get("text") or ""),
                "class_index": CLASS_INDEX[value],
                "normalized_group": normalized_group(label),
            }
        )
    group_labels: dict[str, set[int]] = collections.defaultdict(set)
    for row in rows:
        group_labels[row["normalized_group"]].add(int(row["class_index"]))
    clean = [row for row in rows if len(group_labels[row["normalized_group"]]) == 1]
    stats["mixed_group_excluded"] = len(rows) - len(clean)
    stats["selected"] = len(clean)
    return clean, dict(stats)


def make_vectorizer() -> TfidfVectorizer:
    return TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        min_df=2,
        max_features=60000,
        sublinear_tf=True,
    )


def softmax(values: np.ndarray) -> np.ndarray:
    values = values - values.max(axis=1, keepdims=True)
    exp = np.exp(values)
    return exp / exp.sum(axis=1, keepdims=True)


def sigmoid(values: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(values, -30, 30)))


def fit_heads(x_train: Any, y_train: np.ndarray, *, seed: int):
    rng = np.random.default_rng(seed)
    multi: list[LinearSVC] = []
    shared_weight = None
    for index in range(4):
        sample_weight = rng.poisson(1, len(y_train)) + 0.25
        if shared_weight is None:
            shared_weight = sample_weight
        model = LinearSVC(
            C=3 * (1 + 0.1 * index),
            class_weight="balanced",
            random_state=seed + index,
        )
        model.fit(x_train, y_train, sample_weight=sample_weight)
        multi.append(model)
    boundaries: list[LinearSVC] = []
    for boundary in [2, 3, 4]:
        model = LinearSVC(C=1.5, class_weight="balanced", random_state=seed + 20 + boundary)
        model.fit(x_train, y_train >= boundary, sample_weight=shared_weight)
        boundaries.append(model)
    return multi, boundaries


def raw_records(
    test_indices: np.ndarray,
    y: np.ndarray,
    multi: list[LinearSVC],
    boundaries: list[LinearSVC],
    x_test: Any,
    event_ids: np.ndarray,
) -> list[dict[str, Any]]:
    multi_probs = np.stack([softmax(model.decision_function(x_test)) for model in multi])
    head_predictions = multi_probs.argmax(axis=2)
    predictions = np.asarray(
        [np.bincount(column, minlength=5).argmax() for column in head_predictions.T], dtype=int
    )
    consensus = np.asarray(
        [np.bincount(column, minlength=5).max() for column in head_predictions.T], dtype=int
    )
    confidence = np.mean(
        np.stack([head[np.arange(len(test_indices)), predictions] for head in multi_probs]), axis=0
    )
    risks = np.stack([sigmoid(model.decision_function(x_test)) for model in boundaries], axis=1)
    records: list[dict[str, Any]] = []
    for position, row_index in enumerate(test_indices):
        record = {
            "event_id": int(event_ids[row_index]),
            "truth": int(y[row_index]),
            "prediction": int(predictions[position]),
            "consensus": int(consensus[position]),
            "confidence": float(confidence[position]),
        }
        for offset, boundary in enumerate([2, 3, 4]):
            record[f"b{boundary}_mean"] = float(risks[position, offset])
            record[f"b{boundary}_votes"] = int(risks[position, offset] >= 0.5)
        records.append(record)
    return records


def compute_oof(rows: list[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    texts = np.asarray([row["text"] for row in rows])
    y = np.asarray([row["class_index"] for row in rows], dtype="int64")
    groups = np.asarray([row["normalized_group"] for row in rows])
    event_ids = np.asarray([row["event_id"] for row in rows], dtype="int64")
    output: list[dict[str, Any]] = []
    splitter = StratifiedGroupKFold(5, shuffle=True, random_state=seed)
    for fold, (train, test) in enumerate(splitter.split(texts, y, groups)):
        vectorizer = make_vectorizer()
        x_train = vectorizer.fit_transform(texts[train])
        x_test = vectorizer.transform(texts[test])
        multi, boundaries = fit_heads(x_train, y[train], seed=seed + fold * 100)
        output.extend(raw_records(test, y, multi, boundaries, x_test, event_ids))
        del vectorizer, x_train, x_test, multi, boundaries
        gc.collect()
    return output


def metrics(records: list[dict[str, Any]], accepted: np.ndarray) -> dict[str, float | int]:
    truth = np.asarray([row["truth"] for row in records])[accepted]
    prediction = np.asarray([row["prediction"] for row in records])[accepted]
    child = truth <= 1
    return {
        "n": int(len(truth)),
        "coverage": float(np.mean(accepted)),
        "exact": float(np.mean(prediction == truth)) if len(truth) else 0.0,
        "within": float(np.mean(abs(prediction - truth) <= 1)) if len(truth) else 0.0,
        "under": float(np.mean(prediction < truth)) if len(truth) else 1.0,
        "severe_under": float(np.mean(prediction + 1 < truth)) if len(truth) else 1.0,
        "child_severe_over": (
            float(np.mean(prediction[child] >= truth[child] + 2)) if child.any() else 0.0
        ),
    }


def apply_thresholds(records: list[dict[str, Any]], params: dict[str, Any]) -> np.ndarray:
    prediction = np.asarray([row["prediction"] for row in records])
    consensus = np.asarray([row["consensus"] for row in records])
    confidence = np.asarray([row["confidence"] for row in records])
    accepted = (confidence >= params["confidence"]) & (
        ((prediction <= 2) & (consensus >= params["low_consensus"]))
        | ((prediction >= 3) & (consensus >= params["high_consensus"]))
    )
    for predicted_class, boundary in [(0, 2), (1, 3), (2, 4)]:
        mask = prediction == predicted_class
        votes = np.asarray([row[f"b{boundary}_votes"] for row in records])
        risk = np.asarray([row[f"b{boundary}_mean"] for row in records])
        accepted[mask] &= (votes[mask] <= params["max_severe_votes"]) & (
            risk[mask] <= params["max_severe_risk"]
        )
    for predicted_class, boundary in [(2, 2), (3, 3), (4, 4)]:
        mask = prediction == predicted_class
        votes = np.asarray([row[f"b{boundary}_votes"] for row in records])
        risk = np.asarray([row[f"b{boundary}_mean"] for row in records])
        accepted[mask] &= (votes[mask] >= params["min_high_votes"]) & (
            risk[mask] >= params["min_high_risk"]
        )
    return accepted


def tune_thresholds(records: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    confidence = np.asarray([row["confidence"] for row in records])
    risks = np.concatenate(
        [np.asarray([row[f"b{boundary}_mean"] for row in records]) for boundary in [2, 3, 4]]
    )
    confidence_grid = sorted(set([0.0, *np.quantile(confidence, np.arange(0, 0.51, 0.05)).tolist()]))
    max_risk_grid = sorted(set(np.quantile(risks, [0.25, 0.4, 0.5, 0.6, 0.7, 0.8]).tolist()))
    min_risk_grid = sorted(set(np.quantile(risks, [0.2, 0.3, 0.4, 0.5]).tolist()))
    best = None
    for confidence_cutoff in confidence_grid:
        for low_consensus in [3, 4]:
            for high_consensus in [3, 4]:
                for max_severe_votes in [0, 1]:
                    for max_severe_risk in max_risk_grid:
                        for min_high_votes in [0, 1]:
                            for min_high_risk in min_risk_grid:
                                params = {
                                    "confidence": float(confidence_cutoff),
                                    "low_consensus": low_consensus,
                                    "high_consensus": high_consensus,
                                    "max_severe_votes": max_severe_votes,
                                    "max_severe_risk": float(max_severe_risk),
                                    "min_high_votes": min_high_votes,
                                    "min_high_risk": float(min_high_risk),
                                }
                                result = metrics(records, apply_thresholds(records, params))
                                passed = (
                                    result["coverage"] >= 0.5
                                    and result["exact"] >= 0.72
                                    and result["within"] >= 0.95
                                    and result["under"] <= 0.1
                                    and result["severe_under"] == 0
                                )
                                score = (
                                    int(passed),
                                    result["coverage"],
                                    result["exact"],
                                    result["within"],
                                    -result["under"],
                                    -result["child_severe_over"],
                                )
                                if best is None or score > best[0]:
                                    best = (score, params, result)
    assert best is not None
    return best[1], best[2]


def subprocess_seed(args: argparse.Namespace, seed: int, cache: Path) -> None:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--vectors",
        str(args.vectors),
        "--inputs",
        str(args.inputs),
        "--labels",
        str(args.labels),
        "--prototype-bank",
        str(args.prototype_bank),
        "--output-dir",
        str(args.output_dir),
        "--compute-oof-seed",
        str(seed),
    ]
    if args.scope_review:
        command += ["--scope-review", str(args.scope_review)]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr + completed.stdout)
    if not cache.exists():
        raise RuntimeError(f"OOF subprocess did not produce {cache}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vectors", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--labels", type=Path, required=True)
    parser.add_argument("--scope-review", type=Path)
    parser.add_argument("--prototype-bank", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--compute-oof-seed", type=int)
    parser.add_argument("--min-class-support", type=int, default=5)
    parser.add_argument("--min-exact-accuracy", type=float, default=0.72)
    parser.add_argument("--min-within-one-accuracy", type=float, default=0.95)
    parser.add_argument("--min-accepted-coverage", type=float, default=0.50)
    parser.add_argument("--max-under-rate", type=float, default=0.10)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows, selection_stats = load_training_rows(args.inputs, args.labels, args.scope_review)
    if len(rows) < 25:
        raise ValueError("at least 25 scope-clean official labels are required")
    support_all = collections.Counter(row["label"] for row in rows)
    if any(support_all[label] < args.min_class_support for label in CLASSES):
        raise ValueError("insufficient class support before grouped OOF")
    cache_dir = args.output_dir / ".oof"
    cache_dir.mkdir(parents=True, exist_ok=True)
    if args.compute_oof_seed is not None:
        records = compute_oof(rows, args.compute_oof_seed)
        path = cache_dir / f"seed-{args.compute_oof_seed}.json"
        path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
        print(json.dumps({"seed": args.compute_oof_seed, "rows": len(records), "path": str(path)}))
        return 0

    for seed in [*DEV_SEEDS, ACCEPTANCE_SEED]:
        cache = cache_dir / f"seed-{seed}.json"
        if not cache.exists():
            subprocess_seed(args, seed, cache)
    development = []
    for seed in DEV_SEEDS:
        development += json.loads((cache_dir / f"seed-{seed}.json").read_text(encoding="utf-8"))
    params, development_metrics = tune_thresholds(development)
    acceptance_records = json.loads(
        (cache_dir / f"seed-{ACCEPTANCE_SEED}.json").read_text(encoding="utf-8")
    )
    accepted = apply_thresholds(acceptance_records, params)
    acceptance_metrics = metrics(acceptance_records, accepted)
    support = {
        label: int(sum(row["truth"] == index for row in acceptance_records))
        for index, label in enumerate(CLASSES)
    }
    gates = {
        "minimum_class_support": all(value >= args.min_class_support for value in support.values()),
        "zero_severe_under_rate": acceptance_metrics["severe_under"] == 0.0,
        "under_rate": acceptance_metrics["under"] <= args.max_under_rate,
        "exact_accuracy": acceptance_metrics["exact"] >= args.min_exact_accuracy,
        "within_one_accuracy": acceptance_metrics["within"] >= args.min_within_one_accuracy,
        "accepted_coverage": acceptance_metrics["coverage"] >= args.min_accepted_coverage,
    }

    texts = np.asarray([row["text"] for row in rows])
    y = np.asarray([row["class_index"] for row in rows], dtype="int64")
    vectorizer = make_vectorizer()
    x = vectorizer.fit_transform(texts)
    multi, boundaries = fit_heads(x, y, seed=20260718)
    terms = vectorizer.get_feature_names_out()
    classifier_path = args.output_dir / "event_age_bge_classifier.npz"
    np.savez_compressed(
        classifier_path,
        classifier_kind=np.asarray(CLASSIFIER_KIND),
        classes=np.asarray(CLASSES),
        tfidf_terms=np.asarray(terms, dtype="str"),
        tfidf_idf=np.asarray(vectorizer.idf_, dtype="float64"),
        head_weights=np.stack([model.coef_ for model in multi]).astype("float64"),
        head_bias=np.stack([model.intercept_ for model in multi]).astype("float64"),
        boundary_weights=np.stack([model.coef_[0] for model in boundaries]).astype("float64"),
        boundary_bias=np.asarray([model.intercept_[0] for model in boundaries], dtype="float64"),
    )

    vector_payload = np.load(args.vectors, allow_pickle=False)
    revision = str(vector_payload["model_revision"].item())
    contract = str(vector_payload["encoder_contract"].item())
    bank_payload = json.loads(args.prototype_bank.read_text(encoding="utf-8"))
    bank_hash = stable_hash(bank_payload)
    if str(vector_payload["prototype_bank_hash"].item()) != bank_hash:
        raise ValueError("prototype bank hash does not match encoded vectors")

    probes = [
        "семейный творческий мастер-класс для детей и родителей",
        "детский спектакль по мотивам классической сказки",
        "лекция и дискуссия об истории города",
        "вечерний рок-концерт в клубе",
        "психологический триллер о преступлении и зависимости",
    ]
    bundle = np.load(classifier_path, allow_pickle=False)
    self_tests: list[dict[str, Any]] = []
    gate_for_decisions = {"cascade_thresholds": params}
    decisions = classify_with_safety_cascade(probes, bundle, gate_for_decisions)
    probe_matrix = vectorizer.transform(probes)
    expected_head_logits = np.stack([model.decision_function(probe_matrix) for model in multi], axis=1)
    expected_boundary_logits = np.stack(
        [model.decision_function(probe_matrix) for model in boundaries], axis=1
    )
    for index, (text, decision) in enumerate(zip(probes, decisions)):
        head_logits, boundary_logits = safety_cascade_logits(text, bundle)
        if not np.allclose(head_logits, expected_head_logits[index], rtol=0.0, atol=1e-10):
            raise ValueError("pure TF-IDF multiclass inference differs from sklearn calibration")
        if not np.allclose(boundary_logits, expected_boundary_logits[index], rtol=0.0, atol=1e-10):
            raise ValueError("pure TF-IDF ordinal inference differs from sklearn calibration")
        self_tests.append(
            {
                "text": text,
                "head_logits": expected_head_logits[index].tolist(),
                "boundary_logits": expected_boundary_logits[index].tolist(),
                "status": decision["status"],
                "age_assessment": decision["age_assessment"],
            }
        )
    hash_paths = [args.inputs, args.labels]
    if args.scope_review:
        hash_paths.append(args.scope_review)
    manifest = {
        "schema_version": "event-age-bge-evaluation-v3",
        "classifier_kind": CLASSIFIER_KIND,
        "approval_status": "approved" if all(gates.values()) else "shadow",
        "gate_authority": "automatic_quality_gate_v2",
        "quality_gates_passed": bool(all(gates.values())),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_revision": revision,
        "encoder_contract": contract,
        "prototype_bank_hash": bank_hash,
        "classifier_sha256": file_sha256(classifier_path),
        "evaluation_dataset_hash": combined_sha256(hash_paths),
        "scope_review_sha256": file_sha256(args.scope_review) if args.scope_review else None,
        "labeled_case_count": len(rows),
        "selection_stats": selection_stats,
        "class_support": support,
        "accepted_count": int(acceptance_metrics["n"]),
        "accepted_coverage": round(float(acceptance_metrics["coverage"]), 6),
        "exact_accuracy": round(float(acceptance_metrics["exact"]), 6),
        "within_one_accuracy": round(float(acceptance_metrics["within"]), 6),
        "under_rate": round(float(acceptance_metrics["under"]), 6),
        "critical_over_permissive_rate": round(float(acceptance_metrics["severe_under"]), 6),
        "child_severe_over_diagnostic": round(float(acceptance_metrics["child_severe_over"]), 6),
        "development_seeds": DEV_SEEDS,
        "acceptance_seed": ACCEPTANCE_SEED,
        "development_metrics": development_metrics,
        "cascade_thresholds": params,
        "deterministic_self_tests": self_tests,
        "gates": gates,
        "label_policy": {
            "evaluation": "source_declared_scope_clean_grouped_oof",
            "explicit_age_tokens_masked_before_embedding_and_tfidf": True,
            "ambiguous_or_rejected_scope": "excluded",
            "human_approval_required": False,
        },
    }
    validate_safety_cascade_self_tests(bundle, manifest)
    (args.output_dir / "event_age_bge_evaluation.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False))
    return 0 if manifest["quality_gates_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
