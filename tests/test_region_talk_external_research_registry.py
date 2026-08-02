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


def test_publish_uploads_registry_schema_result_schema_and_prompt_with_static_key(monkeypatch) -> None:
    mod = load_module()
    payload = mod.build_registry([], generated_at="2026-07-20T15:00:00+00:00")
    uploaded: list[tuple[str, str, str]] = []
    sentinel_client = object()
    monkeypatch.setattr(mod, "_iam_token", lambda: "")
    monkeypatch.setattr(mod, "get_yandex_storage_client", lambda: sentinel_client)

    def fake_upload(data, *, object_path, content_type, bucket, cache_control, client):
        assert data
        assert client is sentinel_client
        assert "no-cache" in cache_control
        uploaded.append((object_path, content_type, bucket))
        return f"https://static.kenigevents.ru/{object_path}"

    monkeypatch.setattr(mod, "upload_yandex_public_bytes", fake_upload)
    report = mod.publish_registry(payload)

    assert report["auth_mode"] == "static_access_key"
    assert report["seen_publication_count"] == 0
    assert {item[0] for item in uploaded} == {
        mod.REGISTRY_OBJECT_PATH,
        mod.REGISTRY_SCHEMA_OBJECT_PATH,
        mod.RESULT_SCHEMA_OBJECT_PATH,
        mod.PROMPT_OBJECT_PATH,
    }


def test_publish_prefers_short_lived_iam_token_and_never_loads_static_key(monkeypatch) -> None:
    mod = load_module()
    payload = mod.build_registry([], generated_at="2026-07-20T15:00:00+00:00")
    uploaded: list[tuple[str, str, str, str]] = []
    monkeypatch.setattr(mod, "_iam_token", lambda: "short-lived-token")

    def forbidden_static_client():
        raise AssertionError("static storage credentials must not be loaded when IAM is available")

    def fake_iam_upload(data, *, object_path, content_type, bucket, cache_control, iam_token):
        assert data
        assert iam_token == "short-lived-token"
        assert "no-cache" in cache_control
        uploaded.append((object_path, content_type, bucket, iam_token))
        return f"https://static.kenigevents.ru/{object_path}"

    monkeypatch.setattr(mod, "get_yandex_storage_client", forbidden_static_client)
    monkeypatch.setattr(mod, "_upload_yandex_public_bytes_with_iam", fake_iam_upload)
    report = mod.publish_registry(payload)

    assert report["auth_mode"] == "iam_token"
    assert report["seen_publication_count"] == 0
    assert {item[0] for item in uploaded} == {
        mod.REGISTRY_OBJECT_PATH,
        mod.REGISTRY_SCHEMA_OBJECT_PATH,
        mod.RESULT_SCHEMA_OBJECT_PATH,
        mod.PROMPT_OBJECT_PATH,
    }


def test_iam_put_uses_bearer_auth_and_path_style_bucket_url(monkeypatch) -> None:
    mod = load_module()
    captured = {}

    class FakeResponse:
        status = 200

        def getcode(self):
            return 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(mod, "urlopen", fake_urlopen)
    result = mod._upload_yandex_public_bytes_with_iam(
        b"{}\n",
        object_path="region-talk/external-publications/research-registry.json",
        content_type="application/json; charset=utf-8",
        bucket="kenigevents.ru",
        cache_control="public, no-cache, no-store",
        iam_token="short-lived-token",
    )

    request = captured["request"]
    assert request.full_url == (
        "https://storage.yandexcloud.net/kenigevents.ru/"
        "region-talk/external-publications/research-registry.json"
    )
    assert request.get_method() == "PUT"
    assert request.get_header("Authorization") == "Bearer short-lived-token"
    assert request.get_header("Cache-control") == "public, no-cache, no-store"
    assert captured["timeout"] == 30
    assert result.endswith("/region-talk/external-publications/research-registry.json")
