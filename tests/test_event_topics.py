import json
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import main
import models


class _FakeGemmaTopicsClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def generate_content_async(self, **kwargs):
        self.calls.append(kwargs)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item, {}


def test_event_topic_prompt_mentions_topics():
    prompt = main.EVENT_TOPIC_SYSTEM_PROMPT
    for key, label in main.TOPIC_LABELS.items():
        assert key in prompt
        assert label in prompt
    assert "Бесплатно" in prompt
    assert "Фестивали" in prompt
    assert "ярмарк" in prompt.casefold()
    assert "пьесы классических авторов" in prompt
    assert any(name in prompt for name in ("Шекспир", "Мольер", "Пушкин", "Гоголь"))
    assert "исторические или мифологические сюжеты" in prompt
    assert "новой драме" in prompt
    assert "экспериментальным, иммерсивным" in prompt
    assert "ставь обе темы" in prompt
    assert "не должен использоваться сам по себе" in prompt


@pytest.mark.asyncio
async def test_classify_event_topics_gemma_uses_native_schema(monkeypatch):
    client = _FakeGemmaTopicsClient(['{"topics":["CONCERTS"]}'])
    monkeypatch.setattr(main, "_get_event_topics_gemma_client", lambda: client)
    monkeypatch.setattr(main, "EVENT_TOPICS_GEMMA_NATIVE_SCHEMA", True)

    topics = await main._classify_event_topics_gemma("Название: Концерт\nОписание: Орган и голос")

    assert topics == ["CONCERTS"]
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["generation_config"]["response_mime_type"] == "application/json"
    schema = call["generation_config"]["response_schema"]
    assert schema["type"] == "OBJECT"
    assert schema["properties"]["topics"]["type"] == "ARRAY"
    assert "uniqueItems" not in schema["properties"]["topics"]
    assert "additionalProperties" not in schema
    assert "JSON schema:" not in call["prompt"]


@pytest.mark.asyncio
async def test_classify_event_topics_gemma_falls_back_after_native_error(monkeypatch):
    client = _FakeGemmaTopicsClient(
        [
            RuntimeError("500 INTERNAL"),
            '{"topics":["LECTURES"]}',
        ]
    )
    monkeypatch.setattr(main, "_get_event_topics_gemma_client", lambda: client)
    monkeypatch.setattr(main, "EVENT_TOPICS_GEMMA_NATIVE_SCHEMA", True)

    topics = await main._classify_event_topics_gemma("Название: Лекция")

    assert topics == ["LECTURES"]
    assert len(client.calls) == 2
    assert "response_schema" in client.calls[0]["generation_config"]
    assert client.calls[1]["generation_config"] == {"temperature": 0}
    assert "JSON schema:" in client.calls[1]["prompt"]


def test_topic_labels_include_theatre_subtypes():
    assert main.TOPIC_LABELS["THEATRE_CLASSIC"] == "Классический театр и драма"
    assert (
        main.TOPIC_LABELS["THEATRE_MODERN"]
        == "Современный и экспериментальный театр"
    )


def test_topic_labels_include_fashion():
    assert main.TOPIC_LABELS["FASHION"] == "Мода и стиль"


def test_topic_labels_include_kaliningrad_topic():
    assert (
        main.TOPIC_LABELS["KRAEVEDENIE_KALININGRAD_OBLAST"]
        == "Краеведение Калининградской области"
    )


@pytest.mark.asyncio
async def test_classify_event_topics_filters_and_limits(monkeypatch):
    monkeypatch.setenv("FOUR_O_MINI", "1")
    monkeypatch.setattr(main, "EVENT_TOPICS_LLM", "4o")
    captured: dict[str, object] = {}

    async def fake_ask(text, **kwargs):
        captured["text"] = text
        captured["kwargs"] = kwargs
        return json.dumps(
            {
                "topics": [
                    "HISTORICAL_IMMERSION",
                    "неизвестная",
                    "art",
                    "CINEMA",
                    "MUSIC",
                    "URBANISM",
                ]
            }
        )

    monkeypatch.setattr(main, "ask_4o", fake_ask)

    event = SimpleNamespace(
        title="Большой концерт",
        description="Подробное описание мероприятия.",
        source_text="Анонс события #музыка #концерт",
        location_name="Главная сцена",
        location_address="Невский проспект, 1",
        city="Санкт-Петербург",
    )

    result = await main.classify_event_topics(event)

    assert result == [
        "HISTORICAL_IMMERSION",
        "EXHIBITIONS",
        "MOVIES",
        "CONCERTS",
        "URBANISM",
    ]
    assert "#музыка" in captured["text"]
    kwargs = captured["kwargs"]
    assert kwargs["model"] == "gpt-4o-mini"
    assert kwargs["system_prompt"] == main.EVENT_TOPIC_SYSTEM_PROMPT
    topics_schema = kwargs["response_format"]["json_schema"]["schema"]["properties"]["topics"]
    enum_values = topics_schema["items"]["enum"]
    assert enum_values == list(main.TOPIC_LABELS.keys())
    assert "HISTORICAL_IMMERSION" in enum_values
    assert topics_schema["maxItems"] == 5


@pytest.mark.asyncio
async def test_classify_event_topics_handles_invalid_json(monkeypatch):
    monkeypatch.setattr(main, "EVENT_TOPICS_LLM", "4o")

    async def fake_ask(*args, **kwargs):
        return "{invalid_json}"

    monkeypatch.setattr(main, "ask_4o", fake_ask)

    event = SimpleNamespace(
        title="Лекция",
        description="Интересное событие",
        source_text="",
        location_name="",
        location_address="",
        city="",
    )

    result = await main.classify_event_topics(event)

    assert result == []


def test_normalize_topic_identifier_legacy_aliases():
    cases = {
        "handmade": "HANDMADE",
        "Нетворкинг": "NETWORKING",
        "спорт": "ACTIVE",
        "Personalities": "PERSONALITIES",
        "дети": "KIDS_SCHOOL",
        "семейные": "FAMILY",
        "психология": "PSYCHOLOGY",
        "Psychology": "PSYCHOLOGY",
        "mental health": "PSYCHOLOGY",
        "классический спектакль": "THEATRE_CLASSIC",
        "Драма": "THEATRE_CLASSIC",
        "современный театр": "THEATRE_MODERN",
        "experimental theatre": "THEATRE_MODERN",
        "средневековье": "HISTORICAL_IMMERSION",
        "исторические костюмы": "HISTORICAL_IMMERSION",
        "мода": "FASHION",
        "fashion": "FASHION",
        "Fashion Week": "FASHION",
        "показ мод": "FASHION",
        "fashion show": "FASHION",
        "styling": "FASHION",
        "стиль": "FASHION",
        "урбанистика": "URBANISM",
        "URBANISM": "URBANISM",
        "урбанистический": "URBANISM",
        "краеведение": "KRAEVEDENIE_KALININGRAD_OBLAST",
        "Kaliningrad": "KRAEVEDENIE_KALININGRAD_OBLAST",
    }
    for raw, expected in cases.items():
        assert models.normalize_topic_identifier(raw) == expected


@pytest.mark.asyncio
async def test_classify_event_topics_handles_exception(monkeypatch):
    monkeypatch.setattr(main, "EVENT_TOPICS_LLM", "4o")

    async def fake_ask(*args, **kwargs):
        raise RuntimeError("network error")

    monkeypatch.setattr(main, "ask_4o", fake_ask)

    event = SimpleNamespace(
        title="Мастер-класс",
        description="",
        source_text="",
        location_name="",
        location_address="",
        city="",
    )

    result = await main.classify_event_topics(event)

    assert result == []


@pytest.mark.asyncio
async def test_assign_event_topics_adds_kaliningrad_for_city(monkeypatch):
    async def fake_classify(event):
        return ["KRAEVEDENIE_KALININGRAD_OBLAST"]

    monkeypatch.setattr(main, "classify_event_topics", fake_classify)

    event = SimpleNamespace(
        title="Прогулка",
        description="",
        source_text="",
        location_name="",
        location_address="",
        city="Калининград",
        topics_manual=False,
        topics=[],
    )

    topics, length, error, manual = await main.assign_event_topics(event)

    assert topics == ["KRAEVEDENIE_KALININGRAD_OBLAST"]
    assert length == 8
    assert error is None
    assert manual is False


@pytest.mark.asyncio
async def test_assign_event_topics_kaliningrad_uses_hashtags(monkeypatch):
    async def fake_classify(event):
        return ["CONCERTS", "KRAEVEDENIE_KALININGRAD_OBLAST"]

    monkeypatch.setattr(main, "classify_event_topics", fake_classify)

    event = SimpleNamespace(
        title="Концерт",
        description="Праздник",
        source_text="Будем гулять #калининград #праздник",
        location_name="",
        location_address="",
        city="Москва",
        topics_manual=False,
        topics=[],
    )

    topics, length, error, manual = await main.assign_event_topics(event)

    assert topics == ["CONCERTS", "KRAEVEDENIE_KALININGRAD_OBLAST"]
    assert error is None
    assert manual is False


@pytest.mark.asyncio
async def test_assign_event_topics_skips_manual_and_duplicates(monkeypatch):
    async def fake_classify(event):
        return ["KRAEVEDENIE_KALININGRAD_OBLAST"]

    monkeypatch.setattr(main, "classify_event_topics", fake_classify)

    manual_event = SimpleNamespace(
        title="Лекция",
        description="",
        source_text="",
        location_name="",
        location_address="",
        city="Светлогорск",
        topics_manual=True,
        topics=["LECTURES"],
    )

    topics_manual, *_ = await main.assign_event_topics(manual_event)
    assert topics_manual == ["LECTURES"]

    auto_event = SimpleNamespace(
        title="Экскурсия",
        description="",
        source_text="",
        location_name="",
        location_address="",
        city="Зеленоградск",
        topics_manual=False,
        topics=[],
    )

    topics_auto, *_ = await main.assign_event_topics(auto_event)
    assert topics_auto == ["KRAEVEDENIE_KALININGRAD_OBLAST"]


@pytest.mark.asyncio
async def test_assign_event_topics_preserves_urbanism(monkeypatch):
    async def fake_classify(event):
        return ["URBANISM"]

    monkeypatch.setattr(main, "classify_event_topics", fake_classify)

    event = SimpleNamespace(
        title="Город и люди",
        description="Обсуждаем развитие городской среды",
        source_text="",
        location_name="",
        location_address="",
        city="Москва",
        topics_manual=False,
        topics=[],
    )

    topics, length, error, manual = await main.assign_event_topics(event)

    assert topics == ["URBANISM"]
    assert error is None
    assert manual is False


@pytest.mark.asyncio
async def test_assign_event_topics_adds_kaliningrad_to_urbanism(monkeypatch):
    async def fake_classify(event):
        return ["URBANISM", "KRAEVEDENIE_KALININGRAD_OBLAST"]

    monkeypatch.setattr(main, "classify_event_topics", fake_classify)

    event = SimpleNamespace(
        title="Городские практики",
        description="",
        source_text="",
        location_name="",
        location_address="",
        city="Калининград",
        topics_manual=False,
        topics=[],
    )

    topics, length, error, manual = await main.assign_event_topics(event)

    assert topics == ["URBANISM", "KRAEVEDENIE_KALININGRAD_OBLAST"]
    assert error is None
    assert manual is False
