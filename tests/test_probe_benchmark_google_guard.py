from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import time
import traceback
from pathlib import Path

import pytest

from event_identity import embed_identity_document_with_gemini


ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_event_identity_embedding_uses_injected_google_ai_client() -> None:
    class FakeGoogleAIClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def embed_content_async(self, **kwargs):
            self.calls.append(kwargs)
            return (0.25, 0.5, 0.75), None

    client = FakeGoogleAIClient()
    result = embed_identity_document_with_gemini(
        "candidate text",
        google_ai_client=client,
        model="gemini-embedding-2",
        dim=3,
    )

    assert result.ok
    assert result.embedding == (0.25, 0.5, 0.75)
    assert client.calls == [
        {
            "model": "gemini-embedding-2",
            "text": "candidate text",
            "output_dimensionality": 3,
        }
    ]


def test_benchmark_google_calls_use_shared_client_and_restore_timeout(monkeypatch) -> None:
    benchmark = _load_module(
        "benchmark_lollipop_g4_guard_test",
        "scripts/inspect/benchmark_lollipop_g4.py",
    )

    class FakeGoogleAIClient:
        def __init__(self) -> None:
            self.provider_timeout_seconds = 7.0
            self.calls: list[dict[str, object]] = []

        async def generate_content_async(self, **kwargs):
            self.calls.append(kwargs)
            return '{"answer": "ok"}', None

    client = FakeGoogleAIClient()
    monkeypatch.setattr(benchmark, "_GOOGLE_AI_CLIENT", client)
    result = asyncio.run(
        benchmark._ask_gemma_json_direct(
            model="models/gemma-4-31b-it",
            system_prompt="Return JSON.",
            user_payload={"question": "test"},
            max_tokens=100,
            response_schema={"type": "object"},
            timeout_sec=12.0,
        )
    )

    assert result == {"answer": "ok"}
    assert len(client.calls) == 1
    assert client.calls[0]["model"] == "models/gemma-4-31b-it"
    assert "SYSTEM POLICY:\nReturn JSON." in str(client.calls[0]["prompt"])
    assert client.provider_timeout_seconds == 7.0


def test_benchmark_fails_closed_without_shared_limiter_credentials(monkeypatch) -> None:
    benchmark = _load_module(
        "benchmark_lollipop_g4_fail_closed_test",
        "scripts/inspect/benchmark_lollipop_g4.py",
    )
    monkeypatch.setattr(benchmark, "_GOOGLE_AI_CLIENT", None)
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_KEY", raising=False)

    with pytest.raises(RuntimeError, match="shared limiter accounting"):
        benchmark._require_shared_google_ai_client()


def test_authorized_search_embedding_uses_shared_google_ai_client() -> None:
    smoke = _load_module(
        "authorized_event_search_smoke_guard_test",
        "scripts/smoke_authorized_event_search_rpc.py",
    )

    class FakeGoogleAIClient:
        async def embed_content_async(self, **kwargs):
            assert kwargs["model"] == "gemini-embedding-2"
            assert kwargs["text"].startswith("task: search result | query:")
            assert kwargs["output_dimensionality"] == smoke.EMBEDDING_DIM
            return tuple(float(i) for i in range(smoke.EMBEDDING_DIM)), None

    values = asyncio.run(
        smoke.embed_query(
            "город",
            "gemini-embedding-2",
            google_ai_client=FakeGoogleAIClient(),
        )
    )
    assert len(values) == smoke.EMBEDDING_DIM
    assert values[:3] == [0.0, 1.0, 2.0]


def _probe_cell_source() -> str:
    notebook = json.loads(
        (ROOT / "kaggle/GemmaKey2Probe/gemma_key2_probe.ipynb").read_text(
            encoding="utf-8"
        )
    )
    return "".join(notebook["cells"][1]["source"])


def _probe_namespace(tmp_path: Path, config: dict[str, object], requests_stub) -> dict:
    return {
        "json": json,
        "time": time,
        "traceback": traceback,
        "WORK_DIR": tmp_path,
        "requests": requests_stub,
        "load_config": lambda: dict(config),
        "load_secrets": lambda: {"GOOGLE_API_KEY2": "fake-key"},
        "pick_secret": lambda _config, _secrets: ("GOOGLE_API_KEY2", "fake-key"),
        "extract_response_text": lambda payload: str(payload.get("text") or ""),
    }


def test_gemma_key2_probe_default_execution_fails_before_provider_send(tmp_path) -> None:
    class RequestsStub:
        calls = 0

        @classmethod
        def post(cls, *_args, **_kwargs):
            cls.calls += 1
            raise AssertionError("provider transport must not run")

    namespace = _probe_namespace(
        tmp_path,
        {"secret_env_var": "GOOGLE_API_KEY2"},
        RequestsStub,
    )
    with pytest.raises(RuntimeError, match="direct Google probe disabled"):
        exec(compile(_probe_cell_source(), "gemma_key2_probe_cell", "exec"), namespace)
    assert RequestsStub.calls == 0


def test_gemma_key2_probe_manual_override_consumes_single_attempt(tmp_path) -> None:
    class Response:
        status_code = 200
        ok = True
        text = ""

        @staticmethod
        def json():
            return {"text": "OK"}

    class RequestsStub:
        calls = 0

        @classmethod
        def post(cls, *_args, **_kwargs):
            cls.calls += 1
            return Response()

    namespace = _probe_namespace(
        tmp_path,
        {
            "secret_env_var": "GOOGLE_API_KEY2",
            "dangerously_allow_unaccounted_google_provider_call": True,
            "dangerously_max_unaccounted_provider_send_attempts": 1,
        },
        RequestsStub,
    )
    exec(compile(_probe_cell_source(), "gemma_key2_probe_cell", "exec"), namespace)

    assert RequestsStub.calls == 1
    assert namespace["provider_send_attempts"] == 1
    output = json.loads((tmp_path / "output.json").read_text(encoding="utf-8"))
    assert output["provider_send_attempt_budget"] == 1
    assert output["provider_send_attempts"] == 1


def test_routine_python_surfaces_have_no_raw_google_transport() -> None:
    paths = (
        ROOT / "event_identity.py",
        ROOT / "scripts/inspect/benchmark_lollipop_g4.py",
        ROOT / "scripts/smoke_authorized_event_search_rpc.py",
    )
    forbidden = (
        "google.generativeai",
        "generativelanguage.googleapis.com",
        "x-goog-api-key",
        "GenerativeModel(",
    )
    for path in paths:
        source = path.read_text(encoding="utf-8")
        for marker in forbidden:
            assert marker not in source, f"{path.relative_to(ROOT)} contains {marker}"


def test_notebook_direct_transport_is_explicitly_guarded_and_bounded() -> None:
    source = _probe_cell_source()
    assert "dangerously_allow_unaccounted_google_provider_call" in source
    assert "dangerously_max_unaccounted_provider_send_attempts" in source
    assert source.count("requests.post(") == 1
    assert source.index("if not dangerous_override:") < source.index("requests.post(")
    assert source.index("provider_send_attempts += 1") < source.index("requests.post(")
