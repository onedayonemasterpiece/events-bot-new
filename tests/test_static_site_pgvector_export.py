from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_exporter_module():
    path = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("export_production_preview_data", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_gemma_json_object_accepts_duplicated_structured_parts() -> None:
    exporter = _load_exporter_module()
    parsed = exporter.parse_gemma_json_object(
        '{"ranked":[{"event_id":1,"llm_semantic_score":0.9,"similarity_class":"same","confidence":0.8,"reject":false}]}\n'
        '{"ranked":[{"event_id":2,"llm_semantic_score":0.1,"similarity_class":"no","confidence":0.7,"reject":true}]}'
    )
    assert parsed["ranked"][0]["event_id"] == 1


def test_parse_gemma_json_object_rescues_truncated_complete_items_only() -> None:
    exporter = _load_exporter_module()
    parsed = exporter.parse_gemma_json_object(
        '{"ranked":['
        '{"event_id":1,"llm_semantic_score":0.91,"reject":false},'
        '{"event_id":2,"llm_semantic_score":0.42,"reject":true},'
        '{"event_id":3,"llm_semantic_score":'
    )
    assert parsed == {
        "ranked": [
            {"event_id": 1, "llm_semantic_score": 0.91, "reject": False},
            {"event_id": 2, "llm_semantic_score": 0.42, "reject": True},
        ]
    }


def test_build_gemma_related_audit_prompt_is_compact_and_xml_escaped() -> None:
    exporter = _load_exporter_module()
    anchor = {
        "id": 10,
        "title": "A < B",
        "event_type": "лекция",
        "summary": "Про город & людей",
        "venue_name": "Сигнал",
        "city": "Калининград",
        "start_date": "2026-07-01",
    }
    candidate = {
        "id": 11,
        "title": "Похожее",
        "event_type": "лекция",
        "summary": "Другая лекция",
        "venue_name": "Сигнал",
        "city": "Калининград",
        "start_date": "2026-07-02",
    }
    prompt = exporter.build_gemma_related_audit_prompt(
        anchor=anchor,
        candidates=[candidate],
        fact_max_chars=80,
    )
    assert '<candidate id="11">' in prompt
    assert "A &lt; B" in prompt
    assert "Про город &amp; людей" in prompt
    assert "reason_codes" not in prompt
    assert len(prompt) < 1800
