from __future__ import annotations

import sys
import types

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


@pytest.mark.asyncio
async def test_smart_update_llm_trace_records_json_and_text_calls(monkeypatch):
    client = _FakeGemmaClient(
        [
            '{"facts":["Факт"]}',
            "Готовый текст",
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", False)

    su.reset_smart_update_llm_trace()
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
    text = await su._ask_gemma_text("Верни текст.", max_tokens=50, label="fact_first_desc")

    assert data == {"facts": ["Факт"]}
    assert text == "Готовый текст"
    trace = su.get_smart_update_llm_trace()
    assert [item["label"] for item in trace] == ["facts_extract", "fact_first_desc"]
    assert [item["kind"] for item in trace] == ["json", "text"]
    assert all(item["status"] == "ok" for item in trace)
    assert all(item["duration_sec"] >= 0 for item in trace)


@pytest.mark.asyncio
async def test_fact_first_description_bounded_timeout(monkeypatch):
    async def slow_fact_first(**_kwargs):
        import asyncio

        await asyncio.sleep(10)
        return "too late"

    monkeypatch.setattr(su, "_llm_fact_first_description_md", slow_fact_first)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST_TIMEOUT_SEC", 1)

    result = await su._llm_fact_first_description_md_bounded(label="create")

    assert result is None


@pytest.mark.asyncio
async def test_fact_first_description_timeout_is_opt_in(monkeypatch):
    async def fast_fact_first(**_kwargs):
        return "ready"

    monkeypatch.setattr(su, "_llm_fact_first_description_md", fast_fact_first)
    monkeypatch.setattr(su, "SMART_UPDATE_FACT_FIRST_TIMEOUT_SEC", 0)

    result = await su._llm_fact_first_description_md_bounded(label="create")

    assert result == "ready"


@pytest.mark.asyncio
async def test_smart_update_gemma_outer_retry_defaults_to_one(monkeypatch):
    client = _FakeGemmaClient(
        [
            RuntimeError("provider failed"),
            '{"facts":["late"]}',
        ]
    )
    monkeypatch.delenv("SMART_UPDATE_GEMMA_RETRIES", raising=False)
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", False)
    fake_main = types.ModuleType("main")
    fake_main.ask_4o = None
    fake_main.notify_llm_incident = None
    monkeypatch.setitem(sys.modules, "main", fake_main)

    data = await su._ask_gemma_json(
        "Верни факты.",
        {
            "type": "object",
            "properties": {"facts": {"type": "array", "items": {"type": "string"}}},
            "required": ["facts"],
        },
        max_tokens=100,
        label="create_bundle",
    )

    assert data is None
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_g4_split_create_writer_uses_fact_payload(monkeypatch):
    client = _FakeGemmaClient(
        [
            (
                '{"description":"Лид про камерный концерт и сочетание органа с голосом.\\n\\n'
                '### Музыкальная линия\\nВ программе звучат Ave Maria и духовная музыка. '
                'Факты собраны в цельный анонс без служебной информации.\\n\\n'
                '### Акценты программы\\n- Орган\\n- Голос",'
                '"short_description":"Камерный концерт соединяет органное звучание, вокал, духовную традицию и редкую акустику программы.",'
                '"search_digest":"Камерный концерт с органом, голосом и Ave Maria",'
                '"warnings":[]}'
            )
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", False)
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_1",
        source_text="Концерт Ave Maria: орган и голос.",
        title="Аве Мария",
        date="2026-05-08",
        time="18:00",
        location_name="Филармония",
        city="Калининград",
        event_type="concert",
    )

    result = await su._llm_g4_split_create_writer(
        candidate=candidate,
        title="Аве Мария",
        event_type="concert",
        facts_text_clean=[
            "В программе звучат Ave Maria и духовная музыка.",
            "Формат: камерный концерт органа и голоса.",
        ],
    )

    assert result is not None
    assert "### Музыкальная линия" in result["description"]
    assert result["short_description"]
    assert result["search_digest"] == "Камерный концерт с органом, голосом и Ave Maria"
    assert client.calls[0]["max_output_tokens"] >= 1500
    assert "facts_text_clean" in client.calls[0]["prompt"]
