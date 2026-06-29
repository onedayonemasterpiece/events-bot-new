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

