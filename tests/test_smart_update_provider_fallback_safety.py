from __future__ import annotations

import inspect

import pytest

import smart_event_update as su


def _poster(text: str) -> su.PosterCandidate:
    return su.PosterCandidate(ocr_text=text)


def test_dynamic_openai_schema_name_is_provider_safe() -> None:
    assert su._safe_openai_schema_name("create:fact_first_cov") == "SmartUpdate_create_fact_first_cov"
    assert len(su._safe_openai_schema_name("x:" * 100)) <= 64


def test_legacy_create_bundle_has_no_unreviewed_4o_fallback() -> None:
    assert su._smart_update_4o_fallback_enabled("create_bundle") is False
    assert su._smart_update_4o_fallback_enabled("match_create_bundle") is False
    assert su._smart_update_4o_fallback_enabled("create_bundle_grounding") is False


def test_create_prompts_have_no_incident_specific_proper_nouns() -> None:
    source = inspect.getsource(su)
    assert "Плоский мир" not in source
    assert "Пратчет" not in source


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
    )


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
