from __future__ import annotations

import asyncio
import json
from pathlib import Path

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


def test_inflected_museum_quote_allows_terminal_keep(monkeypatch) -> None:
    """Regression for the lost KOIHM exhibition from t.me/koihm/6041."""

    candidate = seu.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/koihm/6041",
        source_text=(
            "13 августа в Калининградском областном "
            "историко-художественном музее открылась выставка."
        ),
        title="Цветные сны немолодого романтика",
        date="2026-08-13",
        end_date="2026-08-31",
        location_name="Историко-художественный музей",
        location_address="Клиническая 21",
        city="Калининград",
    )

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "keep",
            "confidence": 0.99,
            "location_name": None,
            "location_address": None,
            "city": "Калининград",
            "evidence_quote": (
                "в Калининградском областном "
                "историко-художественном музее"
            ),
            "reason_short": "venue is explicit in an inflected form",
        }

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)
    assert asyncio.run(
        seu._llm_review_candidate_location_grounding(
            candidate,
            trigger_reason="explicit_location_role_conflicts_candidate",
        )
    ) == (True, "llm_keep")


def test_supported_address_does_not_ground_unmentioned_venue_name() -> None:
    """Regression for linked source sofit_models/145 / event 7124."""

    candidate = seu.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/sofit_models/145",
        source_text=(
            "КАСТИНГ на показ мод\n"
            "📍 Адрес: Советский проспект, 12 (8 этаж, студия 809)"
        ),
        title="КАСТИНГ на показ мод",
        date="2026-07-26",
        time="19:00",
        location_name="ИЦАЭ (в КГТУ)",
        location_address="Советский проспект, 12",
        city="Калининград",
    )

    needed, reason = seu._candidate_needs_llm_location_grounding_review(candidate)

    assert needed is True
    assert reason == "canonical_location_name_not_in_source"


def test_programme_label_does_not_beat_explicit_attendee_location_marker() -> None:
    candidate = seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-30777579_15738",
        source_text=(
            "Читаем «Гарри Поттер» в Детском книжном клубе\n"
            "📅 6 августа 2026 года, 12:00\n"
            "📍Летний читальный зал\n"
            "При дожде — лекционный зал, 4 этаж\n"
            "#детскийкнижныйклуб"
        ),
        title="Гарри Поттер club",
        date="2026-08-06",
        time="12:00",
        location_name="ДЕТСКИЙ КНИЖНЫЙ КЛУБ",
        location_address="Мира 9",
        city="Калининград",
    )

    needed, reason = seu._candidate_needs_llm_location_grounding_review(candidate)
    assert (needed, reason) == (True, "explicit_location_role_conflicts_candidate")


def test_programme_label_location_review_repairs_to_marked_room(monkeypatch) -> None:
    candidate = seu.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-30777579_15738",
        source_text="Детский книжный клуб\n📍Летний читальный зал\nМира 9, Калининград",
        title="Гарри Поттер club",
        date="2026-08-06",
        location_name="ДЕТСКИЙ КНИЖНЫЙ КЛУБ",
        location_address="Мира 9",
        city="Калининград",
    )

    async def fake_ask(prompt, *_args, **_kwargs):
        assert "Название программы или сообщества" in prompt
        return {
            "decision": "repair",
            "confidence": 0.99,
            "location_name": "Летний читальный зал",
            "location_address": "Мира 9",
            "city": "Калининград",
            "evidence_quote": "Летний читальный зал",
            "reason_short": "explicit attendee location",
        }

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)
    ok, reason = asyncio.run(
        seu._llm_review_candidate_location_grounding(
            candidate,
            trigger_reason="explicit_location_role_conflicts_candidate",
        )
    )
    assert (ok, reason) == (True, "llm_repair")
    assert candidate.location_name == "Летний читальный зал"


def test_post_review_reference_cannot_undo_llm_repair(monkeypatch) -> None:
    candidate = seu.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/sofit_models/145",
        source_text="📍 Адрес: Советский проспект, 12 (8 этаж, студия 809)",
        title="КАСТИНГ на показ мод",
        date="2026-07-26",
        time="19:00",
        location_name="студия 809",
        location_address="Советский проспект, 12",
        city="Калининград",
    )

    monkeypatch.setattr(
        seu,
        "_canonicalize_location_fields",
        lambda **_kwargs: (
            "ИЦАЭ (в КГТУ)",
            "Советский проспект, 12",
            "Калининград",
        ),
    )

    assert seu._canonicalize_location_after_grounding_review(
        candidate,
        review_result="llm_repair",
    ) == (
        "студия 809",
        "Советский проспект, 12",
        "Калининград",
    )


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


def test_llm_location_review_terminally_rejects_only_event_context(monkeypatch) -> None:
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
            "decision": "reject_missing_location",
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

    assert (ok, reason) == (False, "llm_reject_missing_location")
    assert candidate.location_name == "День города в Янтарном"


def test_wall_32547811_11187_low_confidence_keep_is_terminal_positive(monkeypatch) -> None:
    fixture = json.loads(
        (
            Path(__file__).parent
            / "replays"
            / "INC-2026-08-15-ingestion-retry-stall-and-wal-growth"
            / "vk_location_grounding.json"
        ).read_text(encoding="utf-8")
    )["positive"]
    provider_result = fixture.pop("provider_result")
    candidate = seu.EventCandidate(**fixture)

    async def fake_ask(*_args, **_kwargs):
        return provider_result

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)
    assert asyncio.run(
        seu._llm_review_candidate_location_grounding(
            candidate,
            trigger_reason="canonical_location_name_not_in_source",
        )
    ) == (True, "llm_keep")


def test_wall_32547811_11187_keep_survives_nonverbatim_quote_when_address_is_grounded(
    monkeypatch,
) -> None:
    """The review verdict is semantic; a bad citation must not lose a grounded event."""

    fixture = json.loads(
        (
            Path(__file__).parent
            / "replays"
            / "INC-2026-08-15-ingestion-retry-stall-and-wal-growth"
            / "vk_location_grounding.json"
        ).read_text(encoding="utf-8")
    )["positive"]
    fixture.pop("provider_result")
    candidate = seu.EventCandidate(**fixture)

    async def fake_ask(*_args, **_kwargs):
        return {
            "decision": "keep",
            "confidence": 0.99,
            "location_name": "Библиотека Чехова",
            "location_address": "Московский проспект, 39",
            "city": "Калининград",
            "evidence_quote": "Библиотека Чехова, Московский проспект, 39",
            "reason_short": "The exact address supports the venue.",
        }

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)

    assert asyncio.run(
        seu._llm_review_candidate_location_grounding(
            candidate,
            trigger_reason="canonical_location_name_not_in_source",
        )
    ) == (True, "llm_keep")


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
