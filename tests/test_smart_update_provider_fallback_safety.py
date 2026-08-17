from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

import smart_event_update as su


def _poster(text: str) -> su.PosterCandidate:
    return su.PosterCandidate(ocr_text=text)


_VK_ROUNDUP_REPLAY = (
    Path(__file__).resolve().parent
    / "replays"
    / "INC-2026-07-31-poster-candidate-url"
    / "vk_wall_39437155_17212.json"
)


def test_dynamic_openai_schema_name_is_provider_safe() -> None:
    assert su._safe_openai_schema_name("create:fact_first_cov") == "SmartUpdate_create_fact_first_cov"
    assert len(su._safe_openai_schema_name("x:" * 100)) <= 64


def test_legacy_create_bundle_has_no_unreviewed_4o_fallback() -> None:
    assert su._smart_update_4o_fallback_enabled("create_bundle") is False
    assert su._smart_update_4o_fallback_enabled("match_create_bundle") is False
    assert su._smart_update_4o_fallback_enabled("create_bundle_grounding") is False


def test_facts_stages_use_bounded_lite_then_gemma_model_fallback() -> None:
    assert su._smart_update_fallback_models(
        "create_bundle_grounding", su.SMART_UPDATE_FACTS_MODEL
    ) == ["gemini-3.5-flash-lite", "gemma-4-31b-it"]
    assert su._smart_update_fallback_models(
        "split_description_writer", su.SMART_UPDATE_WRITER_MODEL
    ) is None


def test_create_prompts_have_no_incident_specific_proper_nouns() -> None:
    source = inspect.getsource(su)
    assert "Плоский мир" not in source
    assert "Пратчет" not in source


def test_vk_visible_link_label_is_verbatim_grounding_text() -> None:
    source = json.loads(_VK_ROUNDUP_REPLAY.read_text(encoding="utf-8"))["text"]
    visible_quote = "06 августа в 10:00 разберём на вебинаре"

    assert su._norm_text_for_grounding(visible_quote) in su._norm_text_for_grounding(source)
    assert "выдуманный спикер" not in su._norm_text_for_grounding(source)


@pytest.mark.asyncio
async def test_vk_roundup_is_scoped_before_anchor_role_routing(monkeypatch) -> None:
    source = json.loads(_VK_ROUNDUP_REPLAY.read_text(encoding="utf-8"))["text"]
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-39437155_17212",
        source_text=source,
        title="Вебинар по оборудованию для маркировки",
        date="2026-08-06",
        time="10:00",
        location_name="Онлайн",
        city="Калининград",
    )
    assert su._candidate_needs_llm_occurrence_scope_review(candidate) is True
    assert su._candidate_needs_llm_anchor_role_review(candidate) == (True, "explicit_range")

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "scoped",
            "confidence": 0.99,
            "selected_excerpts": [
                "06 августа в 10:00 разберём на вебинаре, как подружиться с новым "
                "оборудованием для маркировки, чтобы торговать без штрафов и сбоев."
            ],
            "reason_short": "target occurrence",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_scope_candidate_occurrence(candidate) == (True, "llm_scoped")
    assert su._candidate_needs_llm_anchor_role_review(candidate) == (False, "no_role_ambiguity")


@pytest.mark.asyncio
async def test_doors_vs_start_is_llm_repaired_from_verbatim_evidence(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="tg",
        source_url="https://t.me/example/1",
        source_text="17 августа. Сбор гостей в 18:30.",
        title="Шоу «ЖЕНЩИНА»",
        date="2026-08-17",
        time="18:30",
        location_name="Театр",
        city="Калининград",
        posters=[_poster("17 АВГУСТА 19:00\nСБОР ГОСТЕЙ В 18:30")],
    )
    needed, trigger = su._candidate_needs_llm_anchor_role_review(candidate)
    assert (needed, trigger) == (True, "multiple_role_times")

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "repair",
            "confidence": 0.99,
            "date": "2026-08-17",
            "end_date": None,
            "time": "19:00",
            "evidence_quotes": ["17 АВГУСТА 19:00", "СБОР ГОСТЕЙ В 18:30"],
            "reason_short": "19:00 is the show start; 18:30 is guest gathering.",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    ok, reason = await su._llm_review_candidate_anchor_roles(candidate, trigger_reason=trigger)
    assert (ok, reason) == (True, "llm_repair")
    assert candidate.time == "19:00"


@pytest.mark.asyncio
async def test_collapsed_exhibition_range_is_llm_repaired(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="tg",
        source_url="https://t.me/example/2",
        source_text="Выставка «Легенды» работает с 4 по 31 июля.",
        title="Легенды",
        date="2026-07-31",
        end_date="2026-07-31",
        time="16:00",
        location_name="Телеграф",
        city="Светлогорск",
        event_type="выставка",
        posters=[_poster("ОТКРЫТИЕ 4 ИЮЛЯ В 16:00\nВЫСТАВКА 4–31 ИЮЛЯ")],
    )
    needed, trigger = su._candidate_needs_llm_anchor_role_review(candidate)
    assert (needed, trigger) == (True, "explicit_range")

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "repair",
            "confidence": 0.98,
            "date": "2026-07-04",
            "end_date": "2026-07-31",
            "time": None,
            "evidence_quotes": ["Выставка «Легенды» работает с 4 по 31 июля"],
            "reason_short": "Explicit public range; 16:00 is opening-only.",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    ok, reason = await su._llm_review_candidate_anchor_roles(candidate, trigger_reason=trigger)
    assert (ok, reason) == (True, "llm_repair")
    assert (candidate.date, candidate.end_date, candidate.time) == (
        "2026-07-04",
        "2026-07-31",
        None,
    )
    assert candidate.end_date_is_inferred is False


@pytest.mark.asyncio
async def test_daily_activity_without_grounded_start_date_is_product_decision(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-9118984_24806",
        source_text=(
            "Выставка работает до 30 августа. Ежедневные экскурсии проходят "
            "в 12:00 и 17:00."
        ),
        title="Экскурсия по выставке",
        date="2026-08-16",
        end_date="2026-08-30",
        time="12:00",
        location_name="Музей Изобразительных искусств",
        city="Калининград",
    )

    async def fake_ask(prompt, schema, *_args, **_kwargs):
        assert "reject_missing_date" in prompt
        assert "reject_missing_date" in schema["properties"]["decision"]["enum"]
        return {
            "decision": "reject_missing_date",
            "confidence": 0.99,
            "date": None,
            "end_date": "2026-08-30",
            "time": "12:00",
            "evidence_quotes": ["Выставка работает до 30 августа"],
            "reason_short": "The source gives an end date but no start date.",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    needed, trigger = su._candidate_needs_llm_anchor_role_review(candidate)
    assert (needed, trigger) == (True, "explicit_range")
    assert await su._llm_review_candidate_anchor_roles(
        candidate, trigger_reason=trigger
    ) == (False, "llm_reject_missing_date")


@pytest.mark.asyncio
async def test_explicit_unknown_activity_start_is_llm_repaired_to_null(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/ecodvor39/931",
        source_text=(
            'Приглашаем на мастер-класс "Джанкбук: блокнот из случайных сокровищ".\n'
            "Время начала уточняется. Программа Экодвора пока формируется.\n"
            "Летний Экодвор пройдёт 8 августа с 14:00 до 17:00."
        ),
        title="Джанкбук: блокнот из случайных сокровищ",
        date="2026-08-08",
        time="14:00",
        location_name="Железнодорожные ворота",
        city="Калининград",
        event_type="мастер-класс",
    )
    needed, trigger = su._candidate_needs_llm_anchor_role_review(candidate)
    assert (needed, trigger) == (True, "explicit_unknown_start_time")

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "repair",
            "confidence": 0.99,
            "date": "2026-08-08",
            "end_date": None,
            "time": None,
            "evidence_quotes": [
                "Время начала уточняется",
                "Летний Экодвор пройдёт 8 августа с 14:00 до 17:00",
            ],
            "reason_short": "14:00–17:00 is the parent event window, not the workshop start.",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    ok, reason = await su._llm_review_candidate_anchor_roles(candidate, trigger_reason=trigger)
    assert (ok, reason) == (True, "llm_repair")
    assert candidate.time is None


def test_llm_confirmed_unknown_start_clears_persisted_merge_time() -> None:
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/ecodvor39/931",
        source_text="Время начала уточняется.",
        title="Джанкбук",
        date="2026-08-08",
        time="",
        metrics={su._EXPLICIT_UNKNOWN_START_LLM_CONFIRMED_METRIC: True},
    )
    event = type("EventStub", (), {"time": "14:00", "time_is_default": False})()
    updated_keys: list[str] = []

    assert su._apply_llm_confirmed_unknown_start_time(
        event,
        candidate,
        updated_keys=updated_keys,
    ) is True
    assert event.time == ""
    assert event.time_is_default is False
    assert updated_keys == ["time", "time_is_default"]


def test_unreviewed_unknown_start_does_not_clear_persisted_merge_time() -> None:
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/example/3",
        source_text="Время начала уточняется.",
        title="Лекция",
        date="2026-08-08",
        time="",
    )
    event = type("EventStub", (), {"time": "15:00", "time_is_default": False})()

    assert su._apply_llm_confirmed_unknown_start_time(
        event,
        candidate,
        updated_keys=[],
    ) is False
    assert event.time == "15:00"


@pytest.mark.asyncio
async def test_explicit_unknown_activity_start_rejects_llm_parent_window(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/ecodvor39/931",
        source_text=(
            "Время начала уточняется. Программа Экодвора пока формируется. "
            "Летний Экодвор пройдёт 8 августа с 14:00 до 17:00."
        ),
        title="Джанкбук: блокнот из случайных сокровищ",
        date="2026-08-08",
        time="14:00",
        location_name="Железнодорожные ворота",
        city="Калининград",
    )

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "repair",
            "confidence": 0.99,
            "date": "2026-08-08",
            "end_date": None,
            "time": "14:00",
            "evidence_quotes": ["Время начала уточняется", "с 14:00 до 17:00"],
            "reason_short": "incorrectly copied the enclosing window",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_review_candidate_anchor_roles(
        candidate,
        trigger_reason="explicit_unknown_start_time",
    ) == (False, "llm_time_conflicts_explicit_unknown")
    assert candidate.time == "14:00"


@pytest.mark.asyncio
async def test_bundle_grounding_rejects_unrelated_fallback_fields(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_2",
        source_text="Шоу «ЖЕНЩИНА»: пластический спектакль о взрослении.",
        title="Шоу «ЖЕНЩИНА»",
        date="2026-08-17",
        location_name="Театр",
    )
    bundle = {
        "title": "Фестиваль чужого книжного мира",
        "description": "Программа посвящена героям посторонней франшизы.",
        "facts": ["Организатор — несуществующее сообщество."],
        "search_digest": "Посторонняя фестивальная программа.",
        "short_description": "Посторонняя фестивальная программа объединит поклонников вымышленной вселенной.",
    }

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "ungrounded",
            "confidence": 0.99,
            "unsupported_fields": ["title", "description", "facts", "search_digest", "short_description"],
            "evidence_quotes": ["Шоу «ЖЕНЩИНА»"],
            "reason_short": "The generated bundle describes another event.",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_review_create_bundle_grounding(bundle, candidate) == (
        False,
        "llm_ungrounded",
        ["title", "description", "facts", "search_digest", "short_description"],
    )
    assert su._remove_llm_rejected_bundle_fields(
        bundle,
        ["title", "description", "facts", "search_digest", "short_description"],
    ) == {}


@pytest.mark.asyncio
async def test_uncertain_bundle_grounding_strips_optional_generated_prose(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_4",
        source_text="7 августа в 11:00 — семинар о продвижении в соцсетях.",
        title="Семинар о продвижении",
        date="2026-08-07",
        time="11:00",
        location_name="Онлайн",
    )

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "uncertain",
            "confidence": 0.70,
            "unsupported_fields": ["description"],
            "evidence_quotes": ["семинар о продвижении в соцсетях"],
            "reason_short": "insufficient confidence",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_review_create_bundle_grounding(
        {"description": "Текст"}, candidate
    ) == (False, "llm_uncertain", ["description"])


@pytest.mark.asyncio
async def test_ungrounded_bundle_without_field_diagnosis_drops_all_generated_public_fields(
    monkeypatch,
) -> None:
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_5",
        source_text="7 августа в 11:00 — семинар о продвижении в соцсетях.",
        title="Семинар о продвижении",
        date="2026-08-07",
        time="11:00",
        location_name="Онлайн",
    )
    bundle = {
        "title": "Неподтверждённый заголовок",
        "description": "Неподтверждённое описание",
        "facts": [],
        "search_digest": None,
        "short_description": "Неподтверждённый анонс",
        "age_decision": {"value": "0+"},
    }

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "ungrounded",
            "confidence": 0.62,
            "unsupported_fields": [],
            "evidence_quotes": ["семинар о продвижении в соцсетях"],
            "reason_short": "generated prose is not fully supported",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_review_create_bundle_grounding(bundle, candidate) == (
        False,
        "llm_ungrounded",
        ["title", "description", "short_description"],
    )
    assert su._remove_llm_rejected_bundle_fields(
        bundle,
        ["title", "description", "short_description"],
    ) == {
        "facts": [],
        "search_digest": None,
        "age_decision": {"value": "0+"},
    }


@pytest.mark.asyncio
async def test_multicity_occurrence_cannot_mix_target_date_and_sibling_city(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_3",
        source_text="12 июля — Калининград, Театр.\n18 июля — Москва, Концертный зал.",
        title="Концерт",
        date="2026-07-18",
        location_name="Театр",
        city="Калининград",
    )

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "scoped",
            "confidence": 0.99,
            "selected_excerpts": ["18 июля — Москва, Концертный зал."],
            "reason_short": "date block",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_scope_candidate_occurrence(candidate) == (
        False,
        "llm_scope_missing_target_city",
    )


@pytest.mark.asyncio
async def test_region_adjective_does_not_fake_explicit_target_city(monkeypatch) -> None:
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-29891284_14297",
        source_text=(
            "15 августа 13:00 | Кураторская экскурсия по выставке "
            "\u00abМатериальные свидетельства контактов пруссов с Русью\u00bb.\n"
            "Выставка \u00abКалининградская область в эфире\u00bb."
        ),
        raw_excerpt=(
            "15 августа 13:00 | Кураторская экскурсия по выставке "
            "\u00abМатериальные свидетельства контактов пруссов с Русью\u00bb."
        ),
        title=(
            "Кураторская экскурсия по выставке "
            "\u00abМатериальные свидетельства контактов пруссов с Русью\u00bb"
        ),
        date="2026-08-15",
        time="13:00",
        location_name="Историко-художественный музей",
        location_address="Клиническая 21",
        city="Калининград",
    )

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "scoped",
            "confidence": 0.99,
            "selected_excerpts": [candidate.raw_excerpt],
            "reason_short": "exact target block",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    assert await su._llm_scope_candidate_occurrence(candidate) == (
        True,
        "llm_scoped",
    )


def test_smart_update_stage_does_not_send_unsupported_thinking_config() -> None:
    config = su._smart_update_gemma_generation_config()
    assert config == {"temperature": 0.0}


def test_force_staged_gemini_routes_semantic_contracts(monkeypatch) -> None:
    monkeypatch.setattr(su, "SMART_UPDATE_FORCE_STAGED_GEMINI", True)
    monkeypatch.setattr(su, "SMART_UPDATE_FACTS_MODEL", "facts-model")
    monkeypatch.setattr(su, "SMART_UPDATE_WRITER_MODEL", "writer-model")
    assert su._resolve_smart_update_model("occurrence_scope_review") == "facts-model"
    assert su._resolve_smart_update_model("create:fact_first_cov") == "facts-model"
    assert su._resolve_smart_update_model("split_description_writer") == "writer-model"


def test_default_staged_routing_keeps_lite_calls_bounded(monkeypatch) -> None:
    monkeypatch.setattr(su, "SMART_UPDATE_FORCE_STAGED_GEMINI", False)
    monkeypatch.setattr(su, "SMART_UPDATE_FACTS_MODEL", "facts-model")
    monkeypatch.setattr(su, "SMART_UPDATE_WRITER_MODEL", "writer-model")
    monkeypatch.setattr(su, "SMART_UPDATE_MODEL", "gemma-model")

    assert su._resolve_smart_update_model("occurrence_scope_review") == "facts-model"
    assert su._resolve_smart_update_model("location_grounding_review") == "facts-model"
    assert su._resolve_smart_update_model("merge:42:fact_first_desc") == "writer-model"
    assert su._resolve_smart_update_model("merge") == "gemma-model"
    assert su._resolve_smart_update_model("merge:42:fact_first_revise") == "gemma-model"
    assert su._resolve_smart_update_model("short_description") == "gemma-model"
