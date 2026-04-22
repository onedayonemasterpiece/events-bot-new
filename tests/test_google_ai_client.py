from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from google_ai.client import (
    _DEFAULT_ENV_CANDIDATE_CACHE,
    GoogleAIClient,
    RequestContext,
)


class _FakeModel:
    def __init__(self, owner: "_FakeGenAI", model_name: str):
        self.owner = owner
        self.model_name = model_name

    async def generate_content_async(self, prompt, generation_config=None, safety_settings=None):
        self.owner.calls.append(
            {
                "model_name": self.model_name,
                "prompt": prompt,
                "generation_config": dict(generation_config or {}),
                "safety_settings": safety_settings,
            }
        )
        return self.owner.response


class _FakeGenAI:
    def __init__(self, response):
        self.response = response
        self.calls: list[dict] = []
        self.configured_key: str | None = None

    def configure(self, api_key: str) -> None:
        self.configured_key = api_key

    def GenerativeModel(self, model_name: str):
        return _FakeModel(self, model_name)


class _FakeSupabaseQuery:
    def __init__(self, data=None):
        self.data = data or []

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self.data)


class _FakeSupabaseClient:
    def __init__(self, data=None, reserve_data=None):
        self.data = data or []
        self.reserve_data = reserve_data
        self.rpc_calls: list[dict] = []

    def table(self, _name: str):
        return _FakeSupabaseQuery(self.data)

    def rpc(self, _name: str, payload: dict):
        self.rpc_calls.append(dict(payload))
        data = self.reserve_data or {
            "ok": True,
            "env_var_name": "GOOGLE_API_KEY2",
            "key_alias": "unexpected-unscoped-key",
        }
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=data))


@pytest.mark.asyncio
async def test_gemma4_keeps_native_json_config_and_filters_thought_parts():
    response = SimpleNamespace(
        candidates=[
            SimpleNamespace(
                content=SimpleNamespace(
                    parts=[
                        {"text": '{"hidden":"thought"}', "thought": True},
                        {"text": '{"ok":true}'},
                    ]
                )
            )
        ],
        usage_metadata={},
    )
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient()
    client._genai = fake_genai

    text, _usage = await client._call_provider(
        api_key="test-key",
        model="gemma-4-31b",
        prompt="hello",
        generation_config={
            "temperature": 0,
            "response_mime_type": "application/json",
            "response_schema": {"type": "object"},
            "response_schema_name": "ignored_name",
        },
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.configured_key == "test-key"
    assert fake_genai.calls[0]["model_name"] == "models/gemma-4-31b-it"
    assert fake_genai.calls[0]["generation_config"]["response_mime_type"] == "application/json"
    assert fake_genai.calls[0]["generation_config"]["response_schema"] == {"type": "object"}
    assert "response_schema_name" not in fake_genai.calls[0]["generation_config"]


@pytest.mark.asyncio
async def test_gemma3_still_strips_native_json_config():
    response = SimpleNamespace(text='{"ok":true}', usage_metadata={})
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient()
    client._genai = fake_genai

    text, _usage = await client._call_provider(
        api_key="test-key",
        model="gemma-3-27b",
        prompt="hello",
        generation_config={
            "temperature": 0,
            "response_mime_type": "application/json",
            "response_schema": {"type": "object"},
            "response_schema_name": "legacy_name",
        },
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.calls[0]["model_name"] == "models/gemma-3-27b-it"
    assert fake_genai.calls[0]["generation_config"] == {"temperature": 0}


def test_requested_gemma_model_stays_first_in_model_chain():
    client = GoogleAIClient()
    client.fallback_models = ["gemma-3-27b", "gemma-4-26b-a4b"]

    assert client._build_model_chain("gemma-4-31b") == [
        "gemma-4-31b",
        "gemma-3-27b",
        "gemma-4-26b-a4b",
    ]


@pytest.mark.asyncio
async def test_multimodal_prompt_passthrough_and_key3_alias(monkeypatch: pytest.MonkeyPatch):
    response = SimpleNamespace(text='{"ok":true}', usage_metadata={})
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient(default_env_var_name="GOOGLE_API_KEY3")
    client._genai = fake_genai
    monkeypatch.setenv("GOOGLE_API_KEY_3", "aliased-key")

    prompt_parts = ["hello", {"image": "placeholder"}]
    text, _usage = await client._call_provider(
        api_key=client._get_api_key(None) or "",
        model="models/gemma-4-31b-it",
        prompt=prompt_parts,
        generation_config={"temperature": 0},
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.configured_key == "aliased-key"
    assert fake_genai.calls[0]["prompt"] == prompt_parts


@pytest.mark.asyncio
async def test_multimodal_prompt_parts_are_forwarded_to_provider() -> None:
    response = SimpleNamespace(text='{"ok":true}', usage_metadata={})
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient()
    client._genai = fake_genai
    prompt = [
        {"text": "Extract poster facts"},
        {"inline_data": {"mime_type": "image/jpeg", "data": b"\xff\xd8\xfftest"}},
    ]

    text, _usage = await client._call_provider(
        api_key="test-key",
        model="gemma-4-31b",
        prompt=prompt,
        generation_config={"temperature": 0},
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.calls[0]["prompt"] == prompt


def test_multimodal_prompt_estimate_ignores_raw_blob_bytes_and_counts_image_overhead() -> None:
    client = GoogleAIClient()
    prompt = [
        {"text": "Extract poster facts"},
        {"inline_data": {"mime_type": "image/jpeg", "data": b"\xff\xd8\xfftest"}},
    ]

    prompt_text, blob_count = client._prompt_estimate_components(prompt)

    assert prompt_text == "Extract poster facts"
    assert blob_count == 1
    assert client._estimate_prompt_tokens(prompt) >= client.DEFAULT_MULTIMODAL_IMAGE_TOKENS


@pytest.mark.asyncio
async def test_missing_scoped_env_key_uses_local_default_env_limiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    monkeypatch.setenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "1")
    client = GoogleAIClient(
        supabase_client=_FakeSupabaseClient(data=[]),
        consumer="kaggle",
        default_env_var_name="GOOGLE_API_KEY3",
    )
    ctx = RequestContext(
        request_uid="req-1",
        consumer="kaggle",
        account_name=None,
        model="gemma-4-31b",
        requested_model="models/gemma-4-31b-it",
        reserved_tpm=123,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is True
    assert reserve.env_var_name == "GOOGLE_API_KEY3"
    assert reserve.key_alias == "local-fallback-default-env-missing"
    assert reserve.blocked_reason == "default_env_candidates_missing"


@pytest.mark.asyncio
async def test_missing_scoped_env_key_cache_stays_local_not_unscoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    monkeypatch.setenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "1")
    supabase = _FakeSupabaseClient(data=[])
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="kaggle",
        default_env_var_name="GOOGLE_API_KEY3",
    )
    ctx = RequestContext(
        request_uid="req-cache",
        consumer="kaggle",
        account_name=None,
        model="gemma-4-31b",
        requested_model="models/gemma-4-31b-it",
        reserved_tpm=123,
    )

    first = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)
    second = await client._reserve(ctx, attempt_no=2, candidate_key_ids=None)

    assert first.env_var_name == "GOOGLE_API_KEY3"
    assert second.env_var_name == "GOOGLE_API_KEY3"
    assert first.blocked_reason == "default_env_candidates_missing"
    assert second.blocked_reason == "default_env_candidates_missing"
    assert supabase.rpc_calls == []


@pytest.mark.asyncio
async def test_provider_call_timeout_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    class _SlowModel:
        async def generate_content_async(self, *_args, **_kwargs):
            await asyncio.sleep(1)
            return SimpleNamespace(text='{"late":true}', usage_metadata={})

    class _SlowGenAI:
        def configure(self, api_key: str) -> None:
            pass

        def GenerativeModel(self, _model_name: str):
            return _SlowModel()

    monkeypatch.setenv("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", "0.01")
    client = GoogleAIClient()
    client._genai = _SlowGenAI()

    with pytest.raises(TimeoutError, match="timed out"):
        await client._call_provider(
            api_key="test-key",
            model="models/gemma-4-31b-it",
            prompt="hello",
            generation_config={"temperature": 0},
            safety_settings=None,
            max_output_tokens=None,
        )
