from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FESTIVAL_SRC = PROJECT_ROOT / "kaggle" / "UniversalFestivalParser" / "src"
AFISHA_SRC = PROJECT_ROOT / "kaggle" / "AfishaThumb" / "scripts"
RUNTIME_FILES = (
    FESTIVAL_SRC / "reason.py",
    FESTIVAL_SRC / "enrich.py",
    FESTIVAL_SRC / "rate_limit.py",
    AFISHA_SRC / "camera_llm.py",
    AFISHA_SRC / "poster_llm.py",
    AFISHA_SRC / "scene_llm.py",
    AFISHA_SRC / "tour_llm.py",
)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _FakeSecretsProvider:
    def get_secret(self, _name: str) -> None:
        return None


class _FakeGoogleAIClient:
    instances: list["_FakeGoogleAIClient"] = []

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.allow_reserve_fallback = True
        self.allow_local_limiter_fallback = True
        self.allow_local_limiter_on_reserve_error = True
        self.max_retries = 99
        self.fallback_models = ["unexpected-fallback"]
        self.calls: list[dict[str, Any]] = []
        self.__class__.instances.append(self)

    async def generate_content_async(self, **kwargs: Any):
        self.calls.append(kwargs)
        return '{"ok": true}', object()


@pytest.fixture
def fake_gateway(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    import google_ai

    _FakeGoogleAIClient.instances.clear()
    helper_calls: list[dict[str, Any]] = []

    helper = types.ModuleType("google_ai.limiter_supabase")

    def build_google_ai_limiter_supabase_client(**kwargs: Any) -> object:
        helper_calls.append(kwargs)
        return object()

    helper.build_google_ai_limiter_supabase_client = (
        build_google_ai_limiter_supabase_client
    )
    monkeypatch.setitem(sys.modules, "google_ai.limiter_supabase", helper)
    monkeypatch.setattr(google_ai, "GoogleAIClient", _FakeGoogleAIClient)
    monkeypatch.setattr(google_ai, "SecretsProvider", _FakeSecretsProvider)
    return helper_calls


def test_owned_runtime_has_no_direct_google_provider_bypass() -> None:
    forbidden = (
        "google." + "generativeai",
        "from google import " + "genai",
        "from google.genai",
        "generativelanguage." + "googleapis.com",
        "genai." + "Client(",
        "Generative" + "Model(",
    )

    for path in RUNTIME_FILES:
        source = path.read_text(encoding="utf-8")
        matches = [token for token in forbidden if token in source]
        assert not matches, f"{path.relative_to(PROJECT_ROOT)} bypasses gateway: {matches}"


def test_festival_gateway_is_dedicated_fail_closed_and_single_attempt(
    fake_gateway: list[dict[str, Any]],
) -> None:
    module = _load_module(
        "legacy_festival_rate_limit_gateway_test",
        FESTIVAL_SRC / "rate_limit.py",
    )
    client = module.build_festival_google_ai_client(
        api_key="explicit-test-key",
        consumer="test-consumer",
    )

    assert fake_gateway == [{"require_configured": True}]
    assert client.kwargs["consumer"] == "test-consumer"
    assert client.kwargs["default_env_var_name"] == "GOOGLE_API_KEY"
    assert client.allow_reserve_fallback is False
    assert client.allow_local_limiter_fallback is False
    assert client.allow_local_limiter_on_reserve_error is False
    assert client.max_retries == 1
    assert client.fallback_models == []

    secrets = client.kwargs["secrets_provider"]
    assert secrets.get_secret("GOOGLE_API_KEY") == "explicit-test-key"
    assert secrets.get_secret("GOOGLE_API_KEY2") is None


@pytest.mark.asyncio
async def test_festival_reason_routes_through_shared_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    class FakeClient:
        async def generate_content_async(self, **kwargs: Any):
            calls.append(kwargs)
            return '{"festival": {"title_full": "Тест"}}', object()

    shim = types.ModuleType("rate_limit")
    shim.build_festival_google_ai_client = lambda **_kwargs: FakeClient()
    monkeypatch.setitem(sys.modules, "rate_limit", shim)
    module = _load_module("legacy_festival_reason_gateway_test", FESTIVAL_SRC / "reason.py")

    data, error = await module.reason_with_gemma("festival source", "test-key")

    assert error is None
    assert data == {"festival": {"title_full": "Тест"}}
    assert len(calls) == 1
    assert calls[0]["model"] == "gemma-3-27b-it"
    assert calls[0]["max_output_tokens"] == 8192


def test_afishathumb_calls_share_gateway_and_preserve_attempt_caps(
    fake_gateway: list[dict[str, Any]],
) -> None:
    camera = _load_module("legacy_camera_llm_gateway_test", AFISHA_SRC / "camera_llm.py")
    poster = _load_module("legacy_poster_llm_gateway_test", AFISHA_SRC / "poster_llm.py")
    scene = _load_module("legacy_scene_llm_gateway_test", AFISHA_SRC / "scene_llm.py")
    tour = _load_module("legacy_tour_llm_gateway_test", AFISHA_SRC / "tour_llm.py")

    assert camera._call_gemini("camera", "camera-model") == '{"ok": true}'
    assert poster._call_gemini(b"png", "poster-model") == '{"ok": true}'
    assert scene._call_gemini("scene", "scene-model") == '{"ok": true}'
    assert tour._call("tour", "tour-model") == '{"ok": true}'

    assert fake_gateway == [{"require_configured": True}] * 4
    assert len(_FakeGoogleAIClient.instances) == 4
    assert [client.kwargs["consumer"] for client in _FakeGoogleAIClient.instances] == [
        "afishathumb.camera",
        "afishathumb.poster",
        "afishathumb.scene",
        "afishathumb.tour",
    ]
    for module, client in zip(
        (camera, poster, scene, tour),
        _FakeGoogleAIClient.instances,
        strict=True,
    ):
        assert module.MAX_RETRIES == 3
        assert client.max_retries == 1
        assert client.fallback_models == []
        assert client.allow_reserve_fallback is False
        assert client.allow_local_limiter_fallback is False
        assert client.allow_local_limiter_on_reserve_error is False
        assert len(client.calls) == 1

    poster_prompt = _FakeGoogleAIClient.instances[1].calls[0]["prompt"]
    assert poster_prompt[0]["inline_data"] == {
        "mime_type": "image/png",
        "data": b"png",
    }
