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
