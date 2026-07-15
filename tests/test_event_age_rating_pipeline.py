from __future__ import annotations

import importlib.util
import hashlib
import json
import sqlite3
import subprocess
import sys
import types
from pathlib import Path

import numpy as np
import pytest
from sklearn.feature_extraction.text import TfidfVectorizer

from scripts.backfill_event_age_ratings import apply_reviewed_plan, build_plan
from scripts.evaluate_event_age_golden import evaluate, structured_baseline


ROOT = Path(__file__).resolve().parents[1]


def load_bge_worker():
    path = ROOT / "kaggle" / "EventAgeBgeAssessment" / "event_age_bge_assessment.py"
    spec = importlib.util.spec_from_file_location("event_age_bge_assessment", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_bge_launcher():
    path = ROOT / "kaggle" / "execute_event_age_bge_assessment.py"
    spec = importlib.util.spec_from_file_location("execute_event_age_bge_assessment", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_static_exporter():
    path = ROOT / "site" / "scripts" / "export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("event_preview_export", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_backfill_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        create table event(
          id integer primary key, title text, description text, source_text text,
          age_restriction text, age_restriction_status text default 'unknown',
          age_restriction_provenance text, age_restriction_confidence real,
          age_restriction_evidence json, age_restriction_decision_version text,
          age_restriction_input_hash text, age_restriction_updated_at timestamp,
          age_assessment text, age_assessment_provenance text,
          age_assessment_confidence real, age_assessment_evidence json,
          age_assessment_decision_version text, age_assessment_input_hash text,
          age_assessment_engine text
        );
        create table event_source(
          id integer primary key, event_id integer, source_type text,
          source_url text, source_text text
        );
        create table eventposter(
          id integer primary key, event_id integer, poster_hash text,
          ocr_text text, ocr_title text
        );
        insert into event(id,title,description,source_text)
          values(1,'Спектакль','Полное описание события','Источник');
        insert into event_source(event_id,source_type,source_url,source_text)
          values(1,'parser:qtickets','https://tickets.test/1','Возрастное ограничение: 12+.');
        insert into event(id,title,description,source_text)
          values(2,'Концерт','Краткое описание','Источник');
        """
    )
    con.commit()
    return con


def reviewed_declared_payload() -> dict:
    return {
        "status": "declared",
        "value": "12+",
        "provenance": "ticketing_text",
        "confidence": 1.0,
        "evidence_quote": "Возрастное ограничение: 12+",
        "evidence_kind": "source_text",
        "source_document_id": "ticket:1",
        "rubric_codes": [],
        "reason_code": "explicit_event_rating",
    }


def test_backfill_dry_run_and_replay_are_bounded_and_idempotent(tmp_path):
    con = make_backfill_db(tmp_path / "age.sqlite")
    plan = build_plan(con, after_id=0, batch_size=1)
    assert plan["counts"] == {"source_declared_consistent": 1}
    assert plan["rows"][0]["proposed_decision"] is None
    resumed = build_plan(con, after_id=plan["next_after_id"], batch_size=1)
    assert [row["event_id"] for row in resumed["rows"]] == [2]
    assert resumed["counts"] == {"source_missing_insufficient": 1}
    plan["rows"][0]["proposed_decision"] = reviewed_declared_payload()
    first = apply_reviewed_plan(con, plan)
    assert first["counts"] == {"applied_declared": 1}
    first_updated_at = con.execute(
        "select age_restriction_updated_at from event where id=1"
    ).fetchone()[0]
    second = apply_reviewed_plan(con, plan)
    assert second["counts"] == {"unchanged_declared": 1}
    assert con.execute("select age_restriction from event where id=1").fetchone()[0] == "12+"
    assert con.execute(
        "select age_restriction_updated_at from event where id=1"
    ).fetchone()[0] == first_updated_at
    con.close()


def test_static_export_uses_structured_fields_and_labels_assessment(monkeypatch):
    exporter = load_static_exporter()
    row = {
        "age_restriction": None,
        "age_restriction_status": "assessed",
        "age_restriction_provenance": None,
        "age_restriction_decision_version": "v1",
        "age_assessment": "16+",
        "description": "В описании случайно встречается 6+, но оно не читается экспортом.",
    }
    monkeypatch.delenv("STATIC_EVENT_AGE_POLICY", raising=False)
    safe = exporter.event_age_projection(row)
    assert safe["age_restriction"] is None
    assert safe["age_recommendation"] is None
    monkeypatch.setenv("STATIC_EVENT_AGE_POLICY", "declared_or_assessed_labeled")
    labeled = exporter.event_age_projection(row)
    assert labeled["age_restriction"] is None
    assert labeled["age_recommendation"] == "16+"
    assert labeled["age_recommendation_label"] == "Рекомендуемый возраст: 16+ — оценка сервиса"


def test_bge_dual_heads_require_agreement_and_artifact_cutoffs():
    classify_with_dual_heads = load_bge_worker().classify_with_dual_heads
    bundle = {
        "classes": np.asarray(["0+", "6+", "12+", "16+", "18+"]),
        "weights_a": np.asarray([[0, 0, 6, 0, 0], [0, 0, 0, 6, 0]], dtype="float32"),
        "bias_a": np.zeros(5, dtype="float32"),
        "weights_b": np.asarray([[0, 0, 6, 0, 0], [0, 0, 0, 6, 0]], dtype="float32"),
        "bias_b": np.zeros(5, dtype="float32"),
    }
    gate = {"head_a_min_probability": 0.8, "head_b_min_probability": 0.8}
    accepted = classify_with_dual_heads(np.asarray([[1, 0]], dtype="float32"), bundle, gate)[0]
    assert accepted["status"] == "assessed"
    assert accepted["age_assessment"] == "12+"
    bundle["weights_b"] = np.asarray([[0, 0, 0, 6, 0], [0, 0, 0, 6, 0]], dtype="float32")
    rejected = classify_with_dual_heads(np.asarray([[1, 0]], dtype="float32"), bundle, gate)[0]
    assert rejected["status"] == "insufficient_evidence"


def test_bge_dependency_probe_repairs_incompatible_preinstalled_package(monkeypatch):
    worker = load_bge_worker()
    incompatible = types.ModuleType("FlagEmbedding")
    monkeypatch.setitem(sys.modules, "FlagEmbedding", incompatible)
    commands: list[list[str]] = []
    monkeypatch.setattr(worker.subprocess, "check_call", lambda command: commands.append(command))

    worker.ensure_dependencies()

    assert len(commands) == 1
    assert "--upgrade" in commands[0]
    assert "FlagEmbedding==1.4.0" in commands[0]


def test_manual_launcher_exposes_partial_worker_report(tmp_path):
    worker_report_summary = load_bge_launcher().worker_report_summary
    report = tmp_path / "event_age_bge_result.json"
    report.write_text(
        json.dumps({"status": "partial", "events_total": 718, "events_done": 704}),
        encoding="utf-8",
    )

    assert worker_report_summary([str(report)]) == {
        "status": "partial",
        "events_total": 718,
        "events_done": 704,
    }


def test_bge_classifier_gate_is_automatic_and_hash_bound(tmp_path, monkeypatch):
    worker = load_bge_worker()
    bank = {"version": "test", "prototypes": [{"id": "p", "text": "пример"}]}
    bank_hash = worker.stable_hash(bank)
    vector_path = tmp_path / "event_age_bge_prototype_vectors.npz"
    np.savez_compressed(
        vector_path,
        vectors=np.asarray([[1.0, 0.0]], dtype="float32"),
        model_revision=np.asarray("revision"),
        encoder_contract=np.asarray(worker.ENCODER_CONTRACT),
        prototype_bank_hash=np.asarray(bank_hash),
    )
    classifier_path = tmp_path / "event_age_bge_classifier.npz"
    np.savez_compressed(
        classifier_path,
        classes=np.asarray(["0+", "6+", "12+", "16+", "18+"]),
        weights_a=np.zeros((2, 5), dtype="float32"),
        weights_b=np.zeros((2, 5), dtype="float32"),
        bias_a=np.zeros(5, dtype="float32"),
        bias_b=np.zeros(5, dtype="float32"),
    )
    classifier_hash = hashlib.sha256(classifier_path.read_bytes()).hexdigest()
    gate_path = tmp_path / "event_age_bge_evaluation.json"
    gate_path.write_text(
        json.dumps(
            {
                "approval_status": "approved",
                "gate_authority": "automatic_quality_gate_v1",
                "quality_gates_passed": True,
                "model_revision": "revision",
                "encoder_contract": worker.ENCODER_CONTRACT,
                "prototype_bank_hash": bank_hash,
                "classifier_sha256": classifier_hash,
                "evaluation_dataset_hash": "dataset-hash",
                "labeled_case_count": 100,
                "assessment_agreement": 0.9,
                "critical_over_permissive_rate": 0.0,
                "head_a_min_probability": 0.7,
                "head_b_min_probability": 0.7,
                "generated_at": "2026-07-15T00:00:00Z",
                "class_support": {label: 20 for label in ["0+", "6+", "12+", "16+", "18+"]},
                "exact_accuracy": 0.8,
                "within_one_accuracy": 0.98,
            }
        ),
        encoding="utf-8",
    )
    paths = {
        "event_age_bge_prototype_vectors.npz": vector_path,
        "event_age_bge_classifier.npz": classifier_path,
        "event_age_bge_evaluation.json": gate_path,
    }
    monkeypatch.setattr(worker, "find_input", lambda name: paths[name])
    _, classifier, gate, actual_hash = worker.load_prepared_artifacts(
        bank_payload=bank,
        model_revision="revision",
        model=None,
        prototype_texts=["пример"],
    )
    assert classifier is not None
    assert gate["gate_authority"] == "automatic_quality_gate_v1"
    assert "approved_by" not in gate
    assert actual_hash == classifier_hash


def _make_safety_cascade_artifacts(tmp_path, worker):
    samples = [
        "семейный мастер-класс для детей",
        "детский спектакль по сказке",
        "лекция об истории города",
        "вечерний рок концерт",
        "психологический триллер",
    ]
    vectorizer = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        min_df=1,
        sublinear_tf=True,
    ).fit(samples)
    terms = np.asarray(vectorizer.get_feature_names_out(), dtype="str")
    feature_count = len(terms)
    classifier_path = tmp_path / "event_age_bge_classifier.npz"
    head_bias = np.zeros((4, 5), dtype="float64")
    head_bias[:, 2] = 6.0
    np.savez_compressed(
        classifier_path,
        classifier_kind=np.asarray("char_tfidf_safety_cascade_v1"),
        classes=np.asarray(["0+", "6+", "12+", "16+", "18+"]),
        tfidf_terms=terms,
        tfidf_idf=np.asarray(vectorizer.idf_, dtype="float64"),
        head_weights=np.zeros((4, 5, feature_count), dtype="float64"),
        head_bias=head_bias,
        boundary_weights=np.zeros((3, feature_count), dtype="float64"),
        boundary_bias=np.full(3, -2.0, dtype="float64"),
    )
    bundle = np.load(classifier_path, allow_pickle=False)
    thresholds = {
        "confidence": 0.8,
        "low_consensus": 3,
        "high_consensus": 3,
        "max_severe_votes": 0,
        "max_severe_risk": 0.5,
        "min_high_votes": 0,
        "min_high_risk": 0.0,
    }
    gate = {"cascade_thresholds": thresholds}
    decisions = worker.classify_with_safety_cascade(samples, bundle, gate)
    self_tests = []
    for text, decision in zip(samples, decisions):
        head_logits, boundary_logits = worker.safety_cascade_logits(text, bundle)
        self_tests.append(
            {
                "text": text,
                "head_logits": head_logits.tolist(),
                "boundary_logits": boundary_logits.tolist(),
                "status": decision["status"],
                "age_assessment": decision["age_assessment"],
            }
        )
    gate["deterministic_self_tests"] = self_tests
    return samples, vectorizer, classifier_path, bundle, gate


def test_tfidf_safety_cascade_has_sklearn_parity_and_fail_closed_selftests(tmp_path):
    worker = load_bge_worker()
    samples, vectorizer, _, bundle, gate = _make_safety_cascade_artifacts(tmp_path, worker)

    expected = vectorizer.transform([samples[0]]).toarray()[0]
    terms = [str(value) for value in bundle["tfidf_terms"].tolist()]
    actual_sparse = worker._tfidf_features(
        samples[0], {term: index for index, term in enumerate(terms)}, bundle["tfidf_idf"]
    )
    actual = np.zeros_like(expected)
    for index, value in actual_sparse.items():
        actual[index] = value
    np.testing.assert_allclose(actual, expected, rtol=0.0, atol=1e-12)

    decision = worker.classify_with_safety_cascade([samples[0]], bundle, gate)[0]
    assert decision["status"] == "assessed"
    assert decision["age_assessment"] == "12+"
    worker.validate_safety_cascade_self_tests(bundle, gate)

    gate["deterministic_self_tests"][0]["head_logits"][0][0] += 0.1
    with pytest.raises(ValueError, match="multiclass self-test mismatch"):
        worker.validate_safety_cascade_self_tests(bundle, gate)


def test_v2_safety_cascade_gate_requires_valid_startup_selftests(tmp_path, monkeypatch):
    worker = load_bge_worker()
    _, _, classifier_path, _, gate = _make_safety_cascade_artifacts(tmp_path, worker)
    bank = {"version": "test", "prototypes": [{"id": "p", "text": "пример"}]}
    bank_hash = worker.stable_hash(bank)
    vector_path = tmp_path / "event_age_bge_prototype_vectors.npz"
    np.savez_compressed(
        vector_path,
        vectors=np.asarray([[1.0, 0.0]], dtype="float32"),
        model_revision=np.asarray("revision"),
        encoder_contract=np.asarray(worker.ENCODER_CONTRACT),
        prototype_bank_hash=np.asarray(bank_hash),
    )
    classifier_hash = hashlib.sha256(classifier_path.read_bytes()).hexdigest()
    gate.update(
        {
            "approval_status": "approved",
            "gate_authority": "automatic_quality_gate_v2",
            "quality_gates_passed": True,
            "model_revision": "revision",
            "encoder_contract": worker.ENCODER_CONTRACT,
            "prototype_bank_hash": bank_hash,
            "classifier_sha256": classifier_hash,
            "evaluation_dataset_hash": "dataset-hash",
            "labeled_case_count": 531,
            "critical_over_permissive_rate": 0.0,
            "generated_at": "2026-07-15T00:00:00Z",
            "class_support": {label: 20 for label in ["0+", "6+", "12+", "16+", "18+"]},
            "exact_accuracy": 0.95,
            "within_one_accuracy": 0.99,
        }
    )
    gate_path = tmp_path / "event_age_bge_evaluation.json"
    gate_path.write_text(json.dumps(gate, ensure_ascii=False), encoding="utf-8")
    paths = {
        "event_age_bge_prototype_vectors.npz": vector_path,
        "event_age_bge_classifier.npz": classifier_path,
        "event_age_bge_evaluation.json": gate_path,
    }
    monkeypatch.setattr(worker, "find_input", lambda name: paths[name])

    _, classifier, _, _ = worker.load_prepared_artifacts(
        bank_payload=bank,
        model_revision="revision",
        model=None,
        prototype_texts=["пример"],
    )
    assert classifier is not None

    gate["deterministic_self_tests"][0]["boundary_logits"][0] += 0.1
    gate_path.write_text(json.dumps(gate, ensure_ascii=False), encoding="utf-8")
    _, classifier, _, _ = worker.load_prepared_artifacts(
        bank_payload=bank,
        model_revision="revision",
        model=None,
        prototype_texts=["пример"],
    )
    assert classifier is None


def test_automatic_calibrator_uses_official_grouped_holdout_without_human(tmp_path):
    worker = load_bge_worker()
    bank = {"version": "test", "prototypes": [{"id": "p", "text": "пример"}]}
    bank_path = tmp_path / "bank.json"
    bank_path.write_text(json.dumps(bank, ensure_ascii=False), encoding="utf-8")
    bank_hash = worker.stable_hash(bank)
    vectors: list[np.ndarray] = []
    hashes: list[str] = []
    labels: list[dict] = []
    classes = ["0+", "6+", "12+", "16+", "18+"]
    # Find deterministic group ids for each split bucket. Every class gets
    # independent train/calibration/official-holdout support.
    bucket_groups: dict[int, list[str]] = {bucket: [] for bucket in range(10)}
    candidate = 0
    while any(len(values) < 8 for values in bucket_groups.values()):
        value = f"group-{candidate}"
        bucket = int(hashlib.sha256(value.encode()).hexdigest()[:8], 16) % 10
        if len(bucket_groups[bucket]) < 8:
            bucket_groups[bucket].append(value)
        candidate += 1
    for class_index, label in enumerate(classes):
        groups = [
            *bucket_groups[0][class_index : class_index + 3],
            *bucket_groups[1][class_index : class_index + 3],
            *bucket_groups[2][class_index : class_index + 3],
            *bucket_groups[7][class_index : class_index + 6],
            *bucket_groups[8][class_index : class_index + 6],
            *bucket_groups[9][class_index : class_index + 6],
        ]
        # The slices above can overlap across classes, but grouping remains
        # deterministic and never crosses split buckets.
        for row_index, group in enumerate(groups):
            vector = np.zeros(5, dtype="float32")
            vector[class_index] = 1.0
            vectors.append(vector)
            row_hash = hashlib.sha256(f"{label}:{row_index}:{group}".encode()).hexdigest()
            hashes.append(row_hash)
            labels.append(
                {
                    "event_id": len(labels) + 1,
                    "input_hash": row_hash,
                    "label": label,
                    "label_origin": "official_source_declared",
                    "group_id": group,
                }
            )
    vectors_path = tmp_path / "vectors.npz"
    np.savez_compressed(
        vectors_path,
        vectors=np.asarray(vectors),
        input_hashes=np.asarray(hashes),
        model_revision=np.asarray("revision"),
        encoder_contract=np.asarray(worker.ENCODER_CONTRACT),
        prototype_bank_hash=np.asarray(bank_hash),
    )
    labels_path = tmp_path / "labels.jsonl"
    labels_path.write_text(
        "".join(json.dumps(row) + "\n" for row in labels), encoding="utf-8"
    )
    output = tmp_path / "output"
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "calibrate_event_age_bge.py"),
            "--vectors",
            str(vectors_path),
            "--labels",
            str(labels_path),
            "--prototype-bank",
            str(bank_path),
            "--output-dir",
            str(output),
            "--min-class-support",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr + completed.stdout
    manifest = json.loads((output / "event_age_bge_evaluation.json").read_text())
    assert manifest["gate_authority"] == "automatic_quality_gate_v1"
    assert manifest["quality_gates_passed"] is True
    assert manifest["label_policy"]["human_approval_required"] is False


def test_ui_contract_contains_age_fields_without_description_parsing():
    types_source = (ROOT / "site" / "src" / "lib" / "types.ts").read_text(encoding="utf-8")
    exporter_source = (ROOT / "site" / "scripts" / "export-production-preview-data.py").read_text(
        encoding="utf-8"
    )
    assert "age_restriction: AgeRestriction | null" in types_source
    age_projection_body = exporter_source.split("def event_age_projection", 1)[1].split(
        "def strip_emoji_prefix", 1
    )[0]
    assert "description" not in age_projection_body


def test_golden_baseline_reports_null_unvalidated_assessment_metrics():
    cases = json.loads((ROOT / "tests" / "fixtures" / "event_age_rating_golden.json").read_text())
    report = evaluate(cases, structured_baseline(cases))
    assert report["declared_precision"] == 1.0
    assert report["false_positive_rate"] == 0.0
    assert report["assessment_agreement"] is None
    assert report["critical_over_permissive_rate"] is None
