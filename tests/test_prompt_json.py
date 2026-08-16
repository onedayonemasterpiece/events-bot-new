import json

import pytest

import main


def _extract_prompt_json(value: str) -> dict:
    json_text = value.rsplit("\n", 1)[-1]
    return json.loads(json_text)


def test_build_prompt_includes_aliases():
    main._prompt_cache.cache_clear()
    prompt = main._build_prompt([
        "Fest B",
        "Fest A",
    ], [
        ("alias", 0),
    ])
    assert "Use the JSON below" in prompt
    data = _extract_prompt_json(prompt)
    assert data == {
        "festival_names": ["Fest A", "Fest B"],
        "festival_alias_pairs": [["alias", 0]],
    }


def test_build_prompt_omits_alias_section_when_empty():
    main._prompt_cache.cache_clear()
    prompt = main._build_prompt(["Fest"], [])
    data = _extract_prompt_json(prompt)
    assert data == {"festival_names": ["Fest"]}


def test_aliases_bypass_cache_layer():
    main._prompt_cache.cache_clear()
    prompt_initial = main._build_prompt([
        "Fest",
    ], [
        ("old-alias", 0),
    ])
    data_initial = _extract_prompt_json(prompt_initial)
    assert data_initial["festival_alias_pairs"] == [["old-alias", 0]]

    prompt_updated = main._build_prompt([
        "Fest",
    ], [
        ("new-alias", 0),
    ])
    data_updated = _extract_prompt_json(prompt_updated)
    assert data_updated["festival_alias_pairs"] == [["new-alias", 0]]


def test_base_prompt_includes_known_holidays():
    main._read_base_prompt.cache_clear()
    main._read_holidays.cache_clear()
    prompt = main._read_base_prompt()
    assert "Known holidays:" in prompt
    assert (
        "- Хеллоуин (aliases: хэллоуин, halloween) — Костюмированное празднование с тыквами и сладостями."
        in prompt
    )


def test_base_prompt_rejects_historical_exhibition_dates():
    main._read_base_prompt.cache_clear()
    prompt = main._read_base_prompt()

    assert "Do NOT use historical/background dates from a story" in prompt
    assert "9 октября 1947 года" in prompt
    assert "already opened" in prompt


def test_event_parse_defender_flags_bare_type_dash_venue_title():
    """Defender must flag titles shaped exactly like the master-prompt-forbidden
    `<event_type> — <venue>` template (INC-2026-05-11-bar-bastion). It must NOT
    flag a valid quoted-programme title at the same shape.
    """
    bad_titles = [
        "Концерт — Бар Бастион",
        "🎸 Концерт — Бар Бастион",
        "🎭 Спектакль — Музыкальный театр",
        "Лекция — Музей янтаря",
        "Мастер-класс — Студия Каравелла",
        "Экскурсия — Кафедральный собор",
    ]
    for title in bad_titles:
        assert main._event_parse_title_looks_bare(title), title

    good_titles = [
        "Концерт «Скитальцы»: Артур Беркут и Сергей Маврин",
        "🎸 Концерт «Скитальцы» — Артур Беркут и Сергей Маврин",
        "Спектакль «Жили они долго и счастливо»",
        "Стендап-Экскурсия по Калининграду",  # no dash, no venue fallback
        "Лекция «Виктор Васнецов: богатырь, написавший русскую сказку»",
        "Влияние планировочных решений на качество жизни",  # no event_type prefix
        "",
        None,
    ]
    for title in good_titles:
        assert not main._event_parse_title_looks_bare(title), title


@pytest.mark.asyncio
async def test_parse_event_via_llm_escalates_on_defender_flag(monkeypatch):
    """When Gemma 4 returns a bare `<event_type> — <venue>` title, parse_event_via_llm
    must re-call _parse_event_via_gemma with extra['gemma_model']=escalation_model
    and return the second result. Regression contract for the POC defender +
    escalation pattern (INC-2026-05-11-bar-bastion sister contract).
    """
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_DEFENDER_ESCALATION_MODEL", "gemini-3.1-flash-lite")
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")

    calls: list[dict] = []

    async def fake_gemma(text, source_channel=None, **kwargs):
        calls.append({"model": kwargs.get("gemma_model"), "text_len": len(text)})
        if kwargs.get("gemma_model") == "gemini-3.1-flash-lite":
            return main.ParsedEvents([
                {"title": "🎸 Концерт «Скитальцы»: Артур Беркут и Сергей Маврин"},
            ])
        return main.ParsedEvents([
            {"title": "🎸 Концерт — Бар Бастион"},
        ])

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)

    result = await main.parse_event_via_llm("any text", source_channel="vk_test")

    assert len(calls) == 2, calls
    assert calls[0]["model"] is None, "first call must be the default Gemma route"
    assert calls[1]["model"] == "gemini-3.1-flash-lite", "second call must use the escalation model"
    assert len(result) == 1
    assert "Скитальцы" in result[0]["title"]


@pytest.mark.asyncio
async def test_parse_event_via_llm_no_escalation_when_output_is_clean(monkeypatch):
    """No defender flag, no escalation: only one Gemma call must happen."""
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_DEFENDER_ESCALATION_MODEL", "gemini-3.1-flash-lite")
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")

    calls: list[dict] = []

    async def fake_gemma(text, source_channel=None, **kwargs):
        calls.append({"model": kwargs.get("gemma_model")})
        return main.ParsedEvents([
            {"title": "Лекция «Виктор Васнецов: богатырь, написавший русскую сказку»"},
        ])

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)

    result = await main.parse_event_via_llm("any text", source_channel="vk_test")

    assert len(calls) == 1, calls
    assert result[0]["title"].startswith("Лекция")


@pytest.mark.asyncio
async def test_explicit_default_model_does_not_disable_large_post_route(monkeypatch):
    """VK's receipt model spelling must not pin an oversized carrier to Gemma.

    Regression for INC-2026-08-15 carrier 19444: four complete OCR pages made
    the exact Gemma reservation larger than the model's whole TPM bucket.
    """

    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.delenv("EVENT_PARSE_GEMMA_MODEL", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "2500")
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_MODEL", "gemini-3.1-flash-lite")
    calls: list[str | None] = []

    async def fake_gemma(_text, source_channel=None, **kwargs):
        calls.append(kwargs.get("gemma_model"))
        return main.ParsedEvents(
            [{"title": "Лекция «Без потери события»", "date": "2026-08-20"}]
        )

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)

    result = await main.parse_event_via_llm(
        "Короткий основной текст",
        poster_texts=["Полный OCR " + ("данные " * 500)],
        gemma_model="models/gemma-4-31b-it",
    )

    assert len(result) == 1
    assert calls == ["gemini-3.1-flash-lite"]


@pytest.mark.asyncio
async def test_explicit_nondefault_model_remains_operator_pin_for_large_post(monkeypatch):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.delenv("EVENT_PARSE_GEMMA_MODEL", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "2500")
    calls: list[str | None] = []

    async def fake_gemma(_text, source_channel=None, **kwargs):
        calls.append(kwargs.get("gemma_model"))
        return main.ParsedEvents(
            [{"title": "Лекция «Явный маршрут»", "date": "2026-08-20"}]
        )

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    await main.parse_event_via_llm(
        "Короткий основной текст",
        poster_texts=["Полный OCR " + ("данные " * 500)],
        gemma_model="gemini-3.5-flash-lite",
    )

    assert calls == ["gemini-3.5-flash-lite"]


def test_event_parse_defender_check_returns_reasons_for_each_flagged_event():
    events = [
        {"title": "Концерт «Скитальцы»: Артур Беркут и Сергей Маврин"},
        {"title": "🎭 Спектакль — Музыкальный театр"},
        {"title": "Стендап-Экскурсия по Калининграду"},
        {"title": "Концерт — Бар Бастион"},
    ]
    reasons = main._event_parse_defender_check(events)
    assert len(reasons) == 2
    assert "events[1].title_bare" in reasons[0]
    assert "events[3].title_bare" in reasons[1]


def test_base_prompt_includes_meeting_point_override_rule():
    """Regression for INC-2026-05-11 standup-excursion meeting-point bug
    (production event 4687): the master prompt must explicitly forbid snapping
    a meeting-point landmark to a nearby "Known venues" entry by geographic
    proximity (e.g. `Скульптура «Борющиеся зубры», просп. Мира 2` must NOT
    become `Калининградский зоопарк`, which sits at пр-т Мира 26).
    """
    main._read_base_prompt.cache_clear()
    prompt = main._read_base_prompt()
    assert "Meeting-point override" in prompt
    assert "Встреча:" in prompt and "Место встречи:" in prompt
    assert "Скульптура «Борющиеся зубры»" in prompt
    assert "Калининградский зоопарк" in prompt
    assert "address is geographically close" in prompt
