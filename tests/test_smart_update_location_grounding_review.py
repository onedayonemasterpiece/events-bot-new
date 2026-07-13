from __future__ import annotations

import asyncio

import smart_event_update as seu


def _candidate(location_name: str, source_text: str) -> seu.EventCandidate:
    return seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_2",
        source_text=source_text,
        title="Тестовое событие",
        date="2026-07-15",
        time="19:00",
        location_name=location_name,
        city="Калининград",
    )


def test_unsupported_named_island_routes_to_llm_review() -> None:
    candidate = _candidate(
        "Остров Канта",
        "15 июля, 19:00. Верхнее озеро, остров Шайба (смотровая площадка)",
    )
    needed, reason = seu._candidate_needs_llm_location_grounding_review(candidate)
    assert needed is True
    assert reason == "canonical_location_not_in_source"


def test_festival_phrase_in_location_routes_to_llm_review_without_venue_label() -> None:
    candidate = seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-127107743_14707",
        source_text=(
            "11 июля клуб участвовал в празднике в Светлом. "
            "Увидимся 18 июля на Дне города в Янтарном!"
        ),
        title="Выставка ретроавтомобилей на Дне города в Янтарном",
        date="2026-07-18",
        location_name="День города в Янтарном",
        city="Янтарный",
        festival="День города",
    )

    needed, reason = seu._candidate_needs_llm_location_grounding_review(candidate)

    assert needed is True
    assert reason == "location_overlaps_event_context"


def test_unrelated_source_grounded_venue_is_not_routed_by_festival_context() -> None:
    candidate = seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/example",
        source_text="Выставка пройдёт в музее Янтарный замок на Дне города.",
        title="Выставка на Дне города",
        date="2026-07-18",
        location_name="Музей Янтарный замок",
        city="Янтарный",
        festival="День города",
    )

    needed, reason = seu._candidate_needs_llm_location_grounding_review(candidate)

    assert needed is False
    assert reason == "no_explicit_location_role"


def test_llm_location_review_fails_closed_when_only_event_context_is_grounded(monkeypatch) -> None:
    candidate = seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-127107743_14707",
        source_text="Увидимся 18 июля на Дне города в Янтарном!",
        title="Выставка ретроавтомобилей на Дне города в Янтарном",
        date="2026-07-18",
        location_name="День города в Янтарном",
        city="Янтарный",
        festival="День города",
    )

    async def fake_ask(prompt, *_args, **_kwargs):
        assert "не является названием venue" in prompt
        return {
            "decision": "uncertain",
            "confidence": 0.99,
            "location_name": None,
            "location_address": None,
            "city": "Янтарный",
            "evidence_quote": "на Дне города в Янтарном",
            "reason_short": "The source names the occasion and settlement, not the venue.",
        }

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)
    ok, reason = asyncio.run(
        seu._llm_review_candidate_location_grounding(
            candidate,
            trigger_reason="location_overlaps_event_context",
        )
    )

    assert (ok, reason) == (False, "llm_uncertain")
    assert candidate.location_name == "День города в Янтарном"


def test_llm_location_review_applies_only_source_grounded_repair(monkeypatch) -> None:
    candidate = _candidate(
        "Остров Канта",
        "15 июля, 19:00. Верхнее озеро, остров Шайба (смотровая площадка)",
    )

    async def fake_ask(*args, **kwargs):
        return {
            "decision": "repair",
            "confidence": 0.99,
            "location_name": "Верхнее озеро, остров Шайба",
            "location_address": None,
            "city": "Калининград",
            "evidence_quote": "Верхнее озеро, остров Шайба",
            "reason_short": "Explicit event-local venue.",
        }

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)
    ok, reason = asyncio.run(
        seu._llm_review_candidate_location_grounding(
            candidate,
            trigger_reason="canonical_location_not_in_source",
        )
    )
    assert (ok, reason) == (True, "llm_repair")
    assert candidate.location_name == "Верхнее озеро, остров Шайба"
    assert candidate.location_address is None


def test_llm_location_review_rejects_ungrounded_repair(monkeypatch) -> None:
    candidate = _candidate("Остров Канта", "15 июля, 19:00. Верхнее озеро")

    async def fake_ask(*args, **kwargs):
        return {
            "decision": "repair",
            "confidence": 0.99,
            "location_name": "Несуществующий дворец",
            "location_address": None,
            "city": "Калининград",
            "evidence_quote": "Верхнее озеро",
            "reason_short": "Invented venue.",
        }

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)
    ok, reason = asyncio.run(
        seu._llm_review_candidate_location_grounding(
            candidate,
            trigger_reason="canonical_location_not_in_source",
        )
    )
    assert (ok, reason) == (False, "llm_repair_value_not_grounded")
    assert candidate.location_name == "Остров Канта"
