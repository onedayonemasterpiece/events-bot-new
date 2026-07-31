from __future__ import annotations

import sys
import types

import pytest

import smart_event_update as su
from event_age_rating import AGE_DECISION_JSON_SCHEMA


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


def test_native_age_schema_is_google_sdk_compatible_and_nullable():
    from google.genai import types

    converted = su._gemma_native_response_schema(AGE_DECISION_JSON_SCHEMA)
    validated = types.Schema.model_validate(converted)

    assert validated.properties["value"].nullable is True
    assert validated.properties["provenance"].nullable is True
    assert validated.properties["confidence"].nullable is True
    assert validated.properties["evidence_kind"].nullable is True
    assert validated.properties["source_document_id"].nullable is True
    assert None not in converted["properties"]["value"]["enum"]


@pytest.mark.asyncio
async def test_rich_facts_age_evidence_includes_ocr_title_body_beyond_first_three(
    monkeypatch,
):
    client = _FakeGemmaClient(
        [
            (
                '{"public_core_facts":[],"program_or_examples":[],'
                '"context_methodology_facts":[],"people_org_facts":[],'
                '"logistics_facts":[],"uncertain_or_drop":[]}'
            )
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/example/1",
        source_text="Анонс события",
        title="Событие",
        posters=[
            su.PosterCandidate(ocr_text=f"poster {index}") for index in range(3)
        ]
        + [
            su.PosterCandidate(
                ocr_title="ТОЛЬКО ДЛЯ ВЗРОСЛЫХ",
                ocr_text="Возрастное ограничение 18+",
            )
        ],
    )
    await su._llm_extract_candidate_facts(candidate)
    prompt = client.calls[0]["prompt"]
    assert "ТОЛЬКО ДЛЯ ВЗРОСЛЫХ" in prompt
    assert "Возрастное ограничение 18+" in prompt


def test_g4_split_create_cleanup_keeps_route_words_but_strips_hard_logistics():
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_6",
        source_text="Экскурсия по Центральному району.",
        title="Хаусмарки Центрального района",
        date="2026-05-06",
        time="17:30",
        location_name="Центральный район",
        city="Калининград",
        event_type="lecture",
        ticket_price_min=1000,
        ticket_price_max=1000,
    )

    cleaned = su._cleanup_g4_split_create_description(
        (
            "Маршрут пройдет по главным улицам, переулкам и задворкам Центрального района.\n\n"
            "### Детали\n"
            "Участники увидят барельефы, маскароны и медальоны на фасадах зданий.\n"
            "Стоимость участия составит 1000 рублей."
        ),
        candidate=candidate,
    )

    assert cleaned is not None
    assert "улицам" in cleaned
    assert "переулкам" in cleaned
    assert "1000" not in cleaned
    assert "Стоимость участия" not in cleaned


def test_g4_lollipop_light_prompts_keep_interest_lists_out_of_literal_items():
    bucket_prompt = su._g4_lollipop_light_bucket_prompt()
    writer_prompt = su._g4_lollipop_light_writer_system_prompt()

    assert "Do NOT use literal_items for interest lists" in bucket_prompt
    assert "idea examples" in bucket_prompt
    assert "one-word bullet lists" in writer_prompt
    assert "group them in compact natural prose" in writer_prompt
    assert "ONLY for sections whose writer_pack section has a non-empty literal_items" in writer_prompt
    assert "merge them into one compact sentence" in writer_prompt
    assert "final_writer.v3" in writer_prompt


def test_g4_lollipop_light_normalize_bucket_suppresses_short_venue_name_from_infoblock():
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_7",
        source_text="Питчинг в Сигнале.",
        title="Питчинг в Сигнале",
        date="2026-05-14",
        time="18:00",
        location_name="Сигнал",
        location_address="Леонова 22",
        city="Калининград",
        event_type="meetup",
    )

    pack = su._g4_lollipop_light_normalize_bucket_payload(
        ["Организатор — Институт прикладной урбанистики."],
        {"assignments": [{"fact_index": 0, "bucket": "people_and_roles", "literal_items": []}]},
        candidate=candidate,
    )

    logistics = pack["logistics_infoblock"]
    by_label = {item["record_ids"][0]: item for item in logistics}
    assert by_label["location"]["narrative_policy"] == "suppress"
    # Address remains in infoblock — it has digits/comma signals.
    assert by_label["address"].get("narrative_policy") != "suppress"
    assert by_label["date"].get("narrative_policy") != "suppress"


def test_g4_lollipop_light_normalize_bucket_keeps_address_like_location_in_infoblock():
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_8",
        source_text="Лекция.",
        title="Лекция",
        date="2026-05-06",
        time="17:30",
        location_name="ул. Леонова 22, Калининград",
        city="Калининград",
        event_type="lecture",
    )

    pack = su._g4_lollipop_light_normalize_bucket_payload(
        ["Лектор — Игорь Ляшук."],
        {"assignments": [{"fact_index": 0, "bucket": "people_and_roles", "literal_items": []}]},
        candidate=candidate,
    )

    logistics = pack["logistics_infoblock"]
    by_label = {item["record_ids"][0]: item for item in logistics}
    # Location text contains street word + digits + commas — keep it in infoblock.
    assert by_label["location"].get("narrative_policy") != "suppress"


def test_g4_split_create_disables_4o_fallback_for_experimental_stages(monkeypatch):
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)

    assert su._smart_update_4o_fallback_enabled("rich_facts_extract") is False
    assert su._smart_update_4o_fallback_enabled("split_description_writer") is False
    assert su._smart_update_4o_fallback_enabled("split_derived_fields") is False
    assert su._smart_update_4o_fallback_enabled("create_bundle") is False


def test_smart_update_4o_fallback_env_can_disable_all_stages(monkeypatch):
    monkeypatch.setenv("SMART_UPDATE_4O_FALLBACK", "0")
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", False)

    assert su._smart_update_4o_fallback_enabled("create_bundle") is False


def test_smart_update_4o_fallback_budget_limits_mass_fallback(monkeypatch):
    monkeypatch.setenv("SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR", "2")
    su._SMART_UPDATE_4O_FALLBACK_BUDGET["window_start"] = 0.0
    su._SMART_UPDATE_4O_FALLBACK_BUDGET["count"] = 0

    assert su._smart_update_4o_fallback_budget_allows("create_bundle") is True
    assert su._smart_update_4o_fallback_budget_allows("create_bundle") is True
    assert su._smart_update_4o_fallback_budget_allows("create_bundle") is False


def test_smart_update_gemma_client_uses_gateway_pool_and_mass_task_retry_cap(monkeypatch):
    class FakeGoogleAIClient:
        def __init__(self, **kwargs):
            self.max_retries = 3
            self.init_kwargs = kwargs

    fake_google_ai = types.ModuleType("google_ai")
    fake_google_ai.GoogleAIClient = FakeGoogleAIClient
    fake_google_ai.SecretsProvider = lambda: object()
    fake_main = types.ModuleType("main")
    fake_main.get_supabase_client = lambda: None
    fake_main.notify_llm_incident = None
    monkeypatch.setitem(sys.modules, "google_ai", fake_google_ai)
    monkeypatch.setitem(sys.modules, "main", fake_main)
    monkeypatch.setenv("SMART_UPDATE_GOOGLE_AI_MAX_RETRIES", "1")
    monkeypatch.setenv(
        "GOOGLE_AI_NORMAL_KEY_ENVS",
        "GOOGLE_API_KEY6,GOOGLE_API_KEY",
    )

    su._get_gemma_client.cache_clear()
    try:
        client = su._get_gemma_client()
    finally:
        su._get_gemma_client.cache_clear()

    assert client.max_retries == 1
    assert "reserve_key_envs" not in client.init_kwargs


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
    assert client.calls[1]["generation_config"]["temperature"] == 0
    assert client.calls[1]["generation_config"] == {"temperature": 0.0}
    assert "JSON schema:" in client.calls[1]["prompt"]


@pytest.mark.asyncio
async def test_g4_split_create_rich_facts_extracts_sectioned_payload(monkeypatch):
    client = _FakeGemmaClient(
        [
            (
                '{"public_core_facts":["Формат события: открытая городская лаборатория."],'
                '"program_or_examples":["Примеры тем: маршруты, дворы и городские привычки."],'
                '"context_methodology_facts":["Методология основана на интервью с 42 участниками."],'
                '"people_org_facts":["Организатор — Музей города."],'
                '"logistics_facts":["Возрастное ограничение 12+."],'
                '"uncertain_or_drop":["Приходите всей семьёй."]}'
            )
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"rich_facts_extract"})
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_2",
        source_text=(
            "Музей города проводит открытую городскую лабораторию. "
            "Методология основана на интервью с 42 участниками. "
            "Примеры тем: маршруты, дворы и городские привычки. 12+"
        ),
        title="Городская лаборатория",
        date="2026-05-08",
        time="18:00",
        location_name="Музей города",
        city="Калининград",
        event_type="lecture",
    )

    su.reset_smart_update_llm_trace()
    facts = await su._llm_extract_candidate_facts(candidate)

    assert "Формат события: открытая городская лаборатория." in facts
    assert "Методология основана на интервью с 42 участниками." in facts
    assert "Организатор — Музей города." in facts
    assert not any("Приходите" in item for item in facts)
    trace = su.get_smart_update_llm_trace()
    assert trace[0]["label"] == "rich_facts_extract"
    call = client.calls[0]
    assert call["max_output_tokens"] == 1400
    assert call["generation_config"]["response_mime_type"] == "application/json"
    assert "до 40 фактов" in call["prompt"]


@pytest.mark.asyncio
async def test_g4_split_create_rich_facts_prompt_requires_named_speaker_with_title(monkeypatch):
    """Regression for INC kraftmarket39/219 (event 4759): the source post had
    explicit "О спикере: Андрей Анисимов — главный архитектор Калининграда",
    but rich_facts_extract collapsed it to an impersonal "профессиональная
    позиция спикера..." sentence, deleting both name and job title. The
    people_org_facts rule must explicitly forbid that collapse.
    """
    client = _FakeGemmaClient(
        [
            (
                '{"public_core_facts":["Лекция о градостроительных решениях."],'
                '"program_or_examples":[],'
                '"context_methodology_facts":[],'
                '"people_org_facts":[],'
                '"logistics_facts":[],'
                '"uncertain_or_drop":[]}'
            )
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"rich_facts_extract"})
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/kraftmarket39/219",
        source_text=(
            "9.07 Лекция главного архитектора Калининграда:\n"
            "«Влияние планировочных решений на качество жизни на примере старого и нового Калининграда»\n\n"
            "О спикере\n"
            "Андрей Анисимов — главный архитектор Калининграда, работающий с вопросами "
            "архитектурного регулирования, городской среды и визуального облика города."
        ),
        title="Влияние планировочных решений на качество жизни на примере старого и нового Калининграда",
        date="2026-07-09",
        time="18:30",
        location_name="Историко-художественный музей",
        location_address="Клиническая 21",
        city="Калининград",
        event_type="lecture",
    )

    su.reset_smart_update_llm_trace()
    await su._llm_extract_candidate_facts(candidate)
    prompt = client.calls[0]["prompt"]

    # Named-speaker rule must be present and must explicitly forbid the
    # impersonal "профессиональная позиция спикера" collapse that broke 4759.
    assert "ИМЯ" in prompt and "ДОЛЖНОСТЬ" in prompt
    assert "главный архитектор" in prompt
    assert "О спикере" in prompt
    assert "профессиональная позиция спикера" in prompt
    assert "одном именованном факте" in prompt.casefold() or "одном именованном" in prompt
    assert "ОТДЕЛЬНЫЙ именованный факт для КАЖДОГО блока" in prompt
    assert "не сокращай состав до категорий" in prompt
    assert "named roster" in prompt
    assert "Главный архитектор Калининграда" in prompt


@pytest.mark.asyncio
async def test_g4_split_create_writer_repairs_logistics_instead_of_dropping_speaker_roster(
    monkeypatch,
):
    """Regression for INC-2026-06-20-tg-speaker-roster-dropped.

    Event 6244 had a rich speaker roster in the source, but the split writer's
    first draft was rejected for logistics and the create path fell back to a
    generic one-sentence description. The writer should ask the LLM to remove
    logistics and keep the speaker roster before giving up.
    """
    client = _FakeGemmaClient(
        [
            (
                '{"description":"23 июня в 18:30 на Клинической 21 пройдёт паблик-ток.\\n\\n'
                '### Спикеры\\n'
                '- Артур Сарниц, архитектор.\\n'
                '- Андрей Анисимов, главный архитектор Калининграда.",'
                '"warnings":[]}'
            ),
            (
                '{"short_description":"Паблик-ток о городской среде с архитекторами и краеведами.",'
                '"search_digest":"Паблик-ток о городской среде с Артуром Сарницем и Андреем Анисимовым.",'
                '"warnings":[]}'
            ),
        ]
    )
    text_calls: list[dict] = []

    async def fake_ask_gemma_text(prompt, **kwargs):
        text_calls.append({"prompt": prompt, **kwargs})
        return (
            "Паблик-ток о качестве городской среды и будущем районов Калининграда.\n\n"
            "### Спикеры\n"
            "- Артур Сарниц, архитектор\n"
            "- Андрей Анисимов, главный архитектор Калининграда"
        )

    original_logistics_reject = su._description_needs_g4_split_create_logistics_reject

    def fake_logistics_reject(text, *, candidate):
        if "Клинической 21" in (text or ""):
            return True
        return original_logistics_reject(text, candidate=candidate)

    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "_ask_gemma_text", fake_ask_gemma_text)
    monkeypatch.setattr(
        su,
        "_description_needs_g4_split_create_logistics_reject",
        fake_logistics_reject,
    )
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/kenigevents/4104",
        source_text="АРТУР САРНИЦ\nАрхитектор\n\nАНДРЕЙ АНИСИМОВ\nГлавный архитектор Калининграда",
        title="Лекция «Калининград: город-сад или микрорайон для жизни у моря!»",
        date="2026-06-23",
        time="18:30",
        location_name="Историко-художественный музей",
        location_address="Клиническая 21",
        city="Калининград",
        event_type="лекция",
    )

    result = await su._llm_g4_split_create_writer(
        candidate=candidate,
        title=candidate.title,
        event_type=candidate.event_type,
        facts_text_clean=[
            "Формат: паблик-ток с участием экспертов",
            "Спикер: Артур Сарниц, архитектор",
            "Спикер: Андрей Анисимов, главный архитектор Калининграда",
        ],
    )

    assert result is not None
    assert "Артур Сарниц" in result["description"]
    assert "Андрей Анисимов" in result["description"]
    assert "18:30" not in result["description"]
    assert "Клинической 21" not in result["description"]
    assert text_calls and text_calls[0]["label"] == "split_description_writer_remove_logistics"
    writer_prompt = client.calls[0]["prompt"]
    assert "named roster" in writer_prompt
    assert "не сворачивай имена в категории" in writer_prompt


@pytest.mark.asyncio
async def test_g4_split_create_rich_facts_prompt_preserves_organizer_and_inspiration_identity(
    monkeypatch,
):
    client = _FakeGemmaClient(
        [
            (
                '{"public_core_facts":["Своп-мероприятие с книжным обменом."],'
                '"program_or_examples":[],'
                '"context_methodology_facts":["Событие вдохновлено Плоским миром Терри Пратчетта."],'
                '"people_org_facts":["Организатор — сообщество вокруг ОКЦ на Горького 116."],'
                '"logistics_facts":[],'
                '"uncertain_or_drop":[]}'
            )
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"rich_facts_extract"})
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/example/2705",
        source_text=(
            "Живой сундук\n"
            "Своп-мероприятие от ОКЦ на Горького 116.\n"
            "Событие организовано сообществом вокруг ОКЦ на Горького 116 "
            "и вдохновлено Плоским миром Терри Пратчетта."
        ),
        title="Живой сундук",
        date="2026-06-06",
        time="12:00",
        location_name="ОКЦ на Горького",
        location_address="Горького 116",
        city="Калининград",
        event_type="ярмарка",
    )

    await su._llm_extract_candidate_facts(candidate)
    prompt = client.calls[0]["prompt"]

    assert "identity facts" in prompt
    assert "вдохновлено" in prompt
    assert "организовано" in prompt
    assert "вдохновлено" in prompt
    assert "не заменяй" in prompt.casefold()


@pytest.mark.asyncio
async def test_infoblock_logistics_cleanup_prompt_keeps_identity_location_clauses(monkeypatch):
    prompts: list[str] = []

    async def fake_ask_gemma_text(prompt, **_kwargs):
        prompts.append(prompt)
        return "Событие «Живой сундук» организовано сообществом вокруг ОКЦ на Горького 116."

    monkeypatch.setattr(su, "_ask_gemma_text", fake_ask_gemma_text)
    candidate = su.EventCandidate(
        source_type="telegraph_render",
        source_url="https://t.me/okcng/376",
        source_text="Живой сундук. Своп-мероприятие от ОКЦ на Горького 116.",
        title="Живой сундук",
        date="2026-06-06",
        time="12:00-18:00",
        location_name="ОКЦ на Горького",
        location_address="Горького 116",
        city="Калининград",
        event_type="своп",
    )

    await su._llm_remove_infoblock_logistics(
        description=(
            "Событие «Живой сундук» организовано сообществом вокруг ОКЦ на Горького 116, "
            "вдохновлённым Плоским миром Терри Пратчетта."
        ),
        candidate=candidate,
        label="test",
    )

    prompt = prompts[0]
    assert "identity-факты" in prompt
    assert "организовано" in prompt
    assert "вокруг" in prompt
    assert "вдохновлено" in prompt
    assert "это не логистический повтор" in prompt


@pytest.mark.asyncio
async def test_g4_split_create_rich_facts_prompt_requires_bullet_preservation(monkeypatch):
    """Regression for INC-2026-05-11-zoo-lecture (event 4798): the source post
    had two distinct bullet blocks (`О чём поговорим` with 3 items and
    `Правда ли, что` with 2 items), but `rich_facts_extract` collapsed each
    block to one bullet, losing 3/5 facts and producing a meaningless short
    description. The program_or_examples rule must explicitly forbid that
    collapse.
    """
    client = _FakeGemmaClient(
        [
            (
                '{"public_core_facts":["Лекция о зоопарке."],'
                '"program_or_examples":[],'
                '"context_methodology_facts":[],'
                '"people_org_facts":[],'
                '"logistics_facts":[],'
                '"uncertain_or_drop":[]}'
            )
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"rich_facts_extract"})
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/kraftmarket39/example",
        source_text=(
            "26.06 Зоопарку — быть!\n\n"
            "О чём поговорим\n"
            " Когда и почему возник зоопарк.\n"
            " Животные – первые обитатели Калининградского зоопарка.\n"
            " Калининградский зоопарк – место просвещения и культуры.\n\n"
            "Правда ли, что\n"
            " Выживший при штурме Кёнигсберга бегемот Ганс.\n"
            " Животные ручные и их можно гладить и кормить.\n"
        ),
        title="Зоопарку — быть!",
        date="2026-06-26",
        time="18:30",
        location_name="Историко-художественный музей",
        city="Калининград",
        event_type="lecture",
    )

    su.reset_smart_update_llm_trace()
    await su._llm_extract_candidate_facts(candidate)
    prompt = client.calls[0]["prompt"]

    assert "О чём поговорим" in prompt
    assert "Правда ли, что" in prompt
    assert "КАЖДЫЙ bullet" in prompt
    assert "ОТДЕЛЬНЫМ фактом" in prompt
    assert "не сворачивай" in prompt.casefold() or "не сворачивай" in prompt


def test_g4_rich_facts_flatten_preserves_named_speaker_fact():
    """Named-speaker fact returned in people_org_facts must survive the flat
    facts pipeline without being dropped or normalised away. Companion to the
    prompt-level rule above: if Gemma produces the right fact, it must reach
    facts_text_clean intact.
    """
    payload = {
        "public_core_facts": ["Лекция о градостроительных решениях."],
        "program_or_examples": [],
        "context_methodology_facts": [],
        "people_org_facts": ["Лектор: Андрей Анисимов, главный архитектор Калининграда."],
        "logistics_facts": [],
        "uncertain_or_drop": [],
    }

    flat = su._flatten_g4_rich_facts_payload(payload)

    assert any(
        "Андрей Анисимов" in item and "главный архитектор" in item for item in flat
    ), flat


def test_g4_rich_facts_schema_requires_exact_evidence_quote() -> None:
    schema = su._g4_rich_facts_schema()
    item = schema["properties"]["public_core_facts"]["items"]

    assert item["type"] == "object"
    assert item["required"] == ["fact", "evidence_quote"]


def test_merge_facts_require_exact_evidence_quote() -> None:
    item = su.MERGE_SCHEMA["properties"]["added_facts"]["items"]

    assert item["type"] == "object"
    assert item["required"] == ["fact", "evidence_quote"]


def test_merge_fact_contract_rejects_topic_adjacent_hallucination() -> None:
    source = "На Экодворе собирают чистые соусники для повторного использования."
    facts = su._flatten_source_grounded_fact_items(
        [
            {
                "fact": "Можно сдать до 4 шин на переработку.",
                "evidence_quote": "собирают чистые соусники для повторного использования",
            }
        ],
        source_text=source,
        log_context="test",
    )

    assert facts == []


def test_g4_rich_facts_rejects_synthetic_ecodvor_purpose() -> None:
    source = (
        "8 августа Летний Экодвор вернётся в Железнодорожные ворота. "
        "Мы уже намечаем программу с новыми лекциями, мастер-классами и другими активностями."
    )
    payload = {
        "public_core_facts": [
            {
                "fact": "Цель: продвижение экологических инициатив, обмен опытом и активный досуг.",
                "evidence_quote": "Летний Экодвор вернётся в Железнодорожные ворота",
            }
        ],
        "program_or_examples": [],
        "context_methodology_facts": [],
        "people_org_facts": [],
        "logistics_facts": [],
        "uncertain_or_drop": [],
    }

    assert su._flatten_g4_rich_facts_payload(payload, source_corpus=source) == []


def test_managed_vk_publication_is_not_legacy_evidence(monkeypatch) -> None:
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")

    assert su._is_managed_vk_publication_url("https://vk.com/wall-231920894_7008") is True
    assert su._is_managed_vk_publication_url("https://vk.com/wall-132625599_17342") is False


def test_fact_first_sparse_source_prompt_does_not_force_headings() -> None:
    prompt = su._fact_first_description_prompt(
        title="Летний Экодвор",
        event_type="встреча",
        facts_text_clean=[
            "Летний Экодвор вернётся в Железнодорожные ворота.",
            "Организаторы намечают новые лекции и мастер-классы.",
        ],
        epigraph_fact=None,
    )

    assert "SPARSE SOURCE MODE" in prompt
    assert "1–2 коротких абзаца без `###`" in prompt


@pytest.mark.asyncio
async def test_g4_split_create_rich_facts_keeps_prompt_schema_fallback(monkeypatch):
    client = _FakeGemmaClient(
        [
            RuntimeError("provider 500"),
            (
                '{"public_core_facts":["Формат события: городская лаборатория."],'
                '"program_or_examples":["Темы: дворы и маршруты."],'
                '"context_methodology_facts":["Методология основана на интервью."],'
                '"people_org_facts":["Организатор — Музей города."],'
                '"logistics_facts":[],'
                '"uncertain_or_drop":[]}'
            ),
        ]
    )
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"rich_facts_extract"})
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_3",
        source_text="Музей города проводит городскую лабораторию про дворы и маршруты.",
        title="Городская лаборатория",
        date="2026-05-08",
        time="18:00",
        location_name="Музей города",
        city="Калининград",
        event_type="lecture",
    )

    su.reset_smart_update_llm_trace()
    facts = await su._llm_extract_candidate_facts(candidate)

    assert "Формат события: городская лаборатория." in facts
    assert len(client.calls) == 2
    assert "response_schema" in client.calls[0]["generation_config"]
    assert client.calls[1]["generation_config"]["temperature"] == 0
    assert client.calls[1]["generation_config"] == {"temperature": 0.0}
    trace = su.get_smart_update_llm_trace()
    assert trace[0]["status"] == "ok_prompt_after_native"
    assert trace[0]["prompt_schema_fallback_enabled"] is True


@pytest.mark.asyncio
async def test_g4_split_create_description_native_timeout_does_not_double_call_prompt_fallback(monkeypatch):
    client = _FakeGemmaClient([RuntimeError("provider timeout")])
    monkeypatch.delenv("SMART_UPDATE_G4_SPLIT_CREATE_PROMPT_FALLBACK", raising=False)
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"split_description_writer"})

    su.reset_smart_update_llm_trace()
    data = await su._ask_gemma_json(
        "Верни текст.",
        su._g4_split_description_writer_schema(),
        max_tokens=100,
        label="split_description_writer",
    )

    assert data is None
    assert len(client.calls) == 1
    trace = su.get_smart_update_llm_trace()
    assert trace[0]["status"] == "failed_native"
    assert trace[0]["prompt_schema_fallback_enabled"] is False


@pytest.mark.asyncio
async def test_g4_split_create_derived_fields_keep_prompt_schema_fallback(monkeypatch):
    client = _FakeGemmaClient(
        [
            RuntimeError("provider 500"),
            '{"short_description":"Короткое описание события без логистики и лишней рекламы.","search_digest":"Короткое резюме события","warnings":[]}',
        ]
    )
    monkeypatch.delenv("SMART_UPDATE_G4_SPLIT_CREATE_PROMPT_FALLBACK", raising=False)
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES", {"split_derived_fields"})

    su.reset_smart_update_llm_trace()
    data = await su._ask_gemma_json(
        "Верни производные поля.",
        su._g4_split_derived_fields_schema(),
        max_tokens=100,
        label="split_derived_fields",
    )

    assert data is not None
    assert data["search_digest"] == "Короткое резюме события"
    assert len(client.calls) == 2
    assert "response_schema" in client.calls[0]["generation_config"]
    assert client.calls[1]["generation_config"]["temperature"] == 0
    assert client.calls[1]["generation_config"] == {"temperature": 0.0}
    trace = su.get_smart_update_llm_trace()
    assert trace[0]["status"] == "ok_prompt_after_native"
    assert trace[0]["prompt_schema_fallback_enabled"] is True


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
                '"warnings":[]}'
            ),
            (
                '{"short_description":"Камерный концерт соединяет органное звучание, вокал, духовную традицию и редкую акустику программы.",'
                '"search_digest":"Камерный концерт с органом, голосом и Ave Maria",'
                '"warnings":[]}'
            ),
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
    assert client.calls[0]["max_output_tokens"] >= 1700
    assert "facts_text_clean" in client.calls[0]["prompt"]
    assert "производные поля" in client.calls[1]["prompt"]


@pytest.mark.asyncio
async def test_g4_split_create_description_writer_timeout_is_bounded(monkeypatch):
    async def slow_json(*_args, **_kwargs):
        import asyncio

        await asyncio.sleep(10)
        return {
            "description": "too late",
            "short_description": "",
            "search_digest": "",
            "warnings": [],
        }

    monkeypatch.setattr(su, "_ask_gemma_json", slow_json)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_DESCRIPTION_WRITER_TIMEOUT_SEC", 1)
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_5",
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
        facts_text_clean=["Формат: камерный концерт органа и голоса."],
    )

    assert result is None


@pytest.mark.asyncio
async def test_g4_split_create_bundle_uses_fact_ledger_fallback_when_writer_fails(monkeypatch):
    client = _FakeGemmaClient(
        [
            (
                '{"public_core_facts":["Формат события: пешеходная экскурсия по архитектурным деталям."],'
                '"program_or_examples":["Участники увидят барельефы, маскароны и медальоны."],'
                '"context_methodology_facts":["Хаусмарки помогают понять назначение построек."],'
                '"people_org_facts":["Ведущий прогулки: Игорь Ляшук."],'
                '"logistics_facts":["Дата: 2026-05-06","Время: 17:30","Цена: 1000 ₽"],'
                '"uncertain_or_drop":[]}'
            ),
            RuntimeError("writer failed"),
        ]
    )
    monkeypatch.delenv("SMART_UPDATE_G4_SPLIT_CREATE_PROMPT_FALLBACK", raising=False)
    monkeypatch.setattr(su, "_get_gemma_client", lambda: client)
    monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", True)
    monkeypatch.setattr(su, "SMART_UPDATE_GEMMA_NATIVE_SCHEMA", True)
    monkeypatch.setattr(
        su,
        "SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES",
        {"rich_facts_extract", "split_description_writer"},
    )
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_4",
        source_text=(
            "Пешеходная экскурсия по архитектурным деталям. "
            "Участники увидят барельефы, маскароны и медальоны. "
            "Ведущий прогулки: Игорь Ляшук."
        ),
        title="Хаусмарки Центрального района",
        date="2026-05-06",
        time="17:30",
        location_name="Центральный район",
        city="Калининград",
        event_type="lecture",
    )

    bundle = await su._llm_g4_split_create_bundle(
        candidate,
        clean_title="Хаусмарки Центрального района",
        normalized_event_type="lecture",
    )

    assert bundle is not None
    assert bundle["_split_create"] is True
    assert bundle["description"]
    assert "###" in bundle["description"]
    assert "барельефы" in bundle["description"]
    assert "Дата:" not in bundle["description"]
    assert bundle["facts"]
    assert bundle["_split_create_warnings"] == ["writer_unavailable_fact_ledger_fallback"]
    assert "Цена: 1000" not in client.calls[1]["prompt"]
    assert "барельефы" in client.calls[1]["prompt"]
