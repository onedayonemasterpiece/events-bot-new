from __future__ import annotations

from types import SimpleNamespace

import pytest

from google_ai.client import GoogleAIClient


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
