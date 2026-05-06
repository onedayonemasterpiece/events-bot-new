from __future__ import annotations

import pytest

import smart_event_update as su


class _FakeGemmaClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def generate_content_async(self, **kwargs):
        self.calls.append(kwargs)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item, {}


@pytest.mark.asyncio
async def test_ask_gemma_json_uses_native_schema_when_enabled(monkeypatch):
    client = _FakeGemmaClient(['{"facts":["Факт"]}'])
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"facts_extract"})

    data = await su._ask_gemma_json(
        "Верни факты.",
        {
            "type": "object",
            "properties": {
                "facts": {
                    "type": "array",
                    "items": {"type": "string"},
                    "uniqueItems": True,
                }
            },
            "required": ["facts"],
            "additionalProperties": False,
        },
        max_tokens=100,
        label="facts_extract",
    )

    assert data == {"facts": ["Факт"]}
    assert len(client.calls) == 1
    call = client.calls[0]
    assert "JSON schema:" not in call["prompt"]
    assert call["generation_config"]["response_mime_type"] == "application/json"
    schema = call["generation_config"]["response_schema"]
    assert schema["type"] == "OBJECT"
    assert schema["properties"]["facts"]["type"] == "ARRAY"
    assert "uniqueItems" not in schema["properties"]["facts"]
    assert "additionalProperties" not in schema


@pytest.mark.asyncio
async def test_ask_gemma_json_falls_back_to_prompt_schema_after_native_error(monkeypatch):
    client = _FakeGemmaClient(
        [
            RuntimeError("500 INTERNAL"),
            '{"facts":["Факт"]}',
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"facts_extract"})

    data = await su._ask_gemma_json(
        "Верни факты.",
        {
            "type": "object",
            "properties": {"facts": {"type": "array", "items": {"type": "string"}}},
            "required": ["facts"],
        },
        max_tokens=100,
        label="facts_extract",
    )

    assert data == {"facts": ["Факт"]}
    assert len(client.calls) == 2
    assert "response_schema" in client.calls[0]["generation_config"]
    assert client.calls[1]["generation_config"] == {"temperature": 0}
    assert "JSON schema:" in client.calls[1]["prompt"]
