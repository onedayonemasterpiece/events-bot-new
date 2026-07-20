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
    assert "similarity_class" in prompt
    assert "confidence" in prompt
    assert "reason_codes" not in prompt
    assert len(prompt) < 1800


def test_city_level_fallback_is_not_exported_as_a_venue() -> None:
    exporter = _load_exporter_module()

    assert exporter.drop_city_only_venue("Янтарный", "Янтарный") is None
    assert exporter.drop_city_only_venue("  янтарный  ", "Янтарный") is None
    assert exporter.drop_city_only_venue("Площадь Мастеров", "Янтарный") == "Площадь Мастеров"


def _related_event(event_id: int, title: str, *, day: int) -> dict:
    return {
        "id": event_id,
        "title": title,
        "start_date": f"2026-08-{day:02d}",
        "lifecycle_status": "active",
        "ticket": {},
        "other_date_ids": [],
    }


def test_pgvector_graph_reciprocity_links_equal_titles_without_merging() -> None:
    exporter = _load_exporter_module()
    events = [
        _related_event(4327, "Отдыха не знали, из руин подняли", day=2),
        _related_event(5382, "«Отдыха не знали, из руин подняли»", day=4),
        _related_event(7000, "Другая выставка", day=8),
    ]
    chains = {
        "4327": [{
            "event_id": 7000,
            "related_score": 0.7,
            "vector_similarity": 0.76,
            "slot_type": "adjacent_discovery",
            "reason_codes": [],
            "retrieval_sources": ["supabase_pgvector"],
        }],
        "5382": [],
        "7000": [],
    }

    meta = exporter.apply_pgvector_graph_reciprocity(events, chains)

    assert any(item["event_id"] == 5382 for item in chains["4327"])
    assert any(item["event_id"] == 4327 for item in chains["5382"])
    assert meta["exact_title_pairs_missing"] == []
    assert meta["exact_title_links_added"] == 2


def test_pgvector_graph_reciprocity_restores_only_strong_reverse_edges() -> None:
    exporter = _load_exporter_module()
    events = [
        _related_event(1, "Первое", day=1),
        _related_event(2, "Второе", day=2),
        _related_event(3, "Третье", day=3),
    ]
    chains = {
        "1": [
            {
                "event_id": 2,
                "related_score": 0.9,
                "vector_similarity": 0.91,
                "slot_type": "pure_related",
                "reason_codes": [],
                "retrieval_sources": ["supabase_pgvector"],
            },
            {
                "event_id": 3,
                "related_score": 0.7,
                "vector_similarity": 0.79,
                "slot_type": "adjacent_discovery",
                "reason_codes": [],
                "retrieval_sources": ["supabase_pgvector"],
            },
        ],
        "2": [],
        "3": [],
    }

    exporter.apply_pgvector_graph_reciprocity(events, chains)

    assert any(item["event_id"] == 1 for item in chains["2"])
    # The weak pair is not mislabeled as a semantic reciprocal match. A dark
    # node may still be rescued as adjacent discovery, but never pure_related.
    weak_reverse = next((item for item in chains["3"] if item["event_id"] == 1), None)
    assert weak_reverse is None or weak_reverse["slot_type"] == "adjacent_discovery"
