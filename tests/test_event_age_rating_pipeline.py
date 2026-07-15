from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import numpy as np

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
