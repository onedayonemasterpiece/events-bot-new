from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_external_research_registry.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_external_research_registry", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_registry_is_schema_valid_and_points_to_stable_contract() -> None:
    mod = load_module()
    payload = mod.build_registry(
        [{
            "canonical_url": "https://archi.ru/russia/101203/vsya-mudrost-okeana",
            "doi": None,
            "title": "Вся мудрость океана",
            "authors": ["И. Автор"],
            "normalized_title": "вся мудрость океана",
            "normalized_authors": ["и. автор"],
            "external_publication_id": "extpub_1234567890abcdef12345678",
            "source_name": "Архи.ру",
            "disposition": "candidate",
        }],
        generated_at="2026-07-20T15:00:00+00:00",
    )

    assert payload["schema_version"] == "region_talk_external_research_registry.v1"
    assert payload["public_registry_url"].endswith("/research-registry.json")
    assert payload["result_contract"]["schema_url"].endswith("/result.schema.json")
    assert len(payload["result_contract"]["sha256"]) == 64
    assert payload["duplicate_guard"]["seen_publication_count"] == 1
    seen = payload["duplicate_guard"]["seen_publications"][0]
    assert seen["authors"] == ["И. Автор"]
    assert seen["external_publication_id"] == "extpub_1234567890abcdef12345678"
    assert json.loads(json.dumps(payload, ensure_ascii=False)) == payload


def test_saved_prompt_requires_exact_registry_url_without_cache_busting() -> None:
    mod = load_module()
    prompt = mod.PROMPT_PATH.read_text(encoding="utf-8")

    assert mod._url(mod.REGISTRY_OBJECT_PATH) in prompt
    assert mod._url(mod.RESULT_SCHEMA_OBJECT_PATH) in prompt
    assert "Open both exact URLs without adding query parameters" in prompt
    assert "Append a cache-busting query parameter" not in prompt


def test_publish_uploads_registry_schema_result_schema_and_prompt(monkeypatch) -> None:
    mod = load_module()
    payload = mod.build_registry([], generated_at="2026-07-20T15:00:00+00:00")
    uploaded: list[tuple[str, str, str]] = []
    sentinel_client = object()
    monkeypatch.setattr(mod, "get_yandex_storage_client", lambda: sentinel_client)

    def fake_upload(data, *, object_path, content_type, bucket, cache_control, client):
        assert data
        assert client is sentinel_client
        assert "no-cache" in cache_control
        uploaded.append((object_path, content_type, bucket))
        return f"https://static.kenigevents.ru/{object_path}"

    monkeypatch.setattr(mod, "upload_yandex_public_bytes", fake_upload)
    report = mod.publish_registry(payload)

    assert report["seen_publication_count"] == 0
    assert {item[0] for item in uploaded} == {
        mod.REGISTRY_OBJECT_PATH,
        mod.REGISTRY_SCHEMA_OBJECT_PATH,
        mod.RESULT_SCHEMA_OBJECT_PATH,
        mod.PROMPT_OBJECT_PATH,
    }
