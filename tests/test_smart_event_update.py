from __future__ import annotations

import pytest

import smart_event_update as sut
from models import Event
from smart_event_update import (
    STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
    EventCandidate,
    adjudicate_collection_candidate,
    build_collection_adjudication_request,
    route_collection_adjudication_reasons,
    validate_collection_adjudication_output,
)


def _payload(
    *,
    admission_value: str = "unknown",
    admission_quote: str = "",
    admission_reason: str = "insufficient_evidence",
    audience_value: str = "unknown",
    audience_quote: str = "",
    audience_reason: str = "insufficient_evidence",
    people: list[dict] | None = None,
) -> dict:
    return {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "admission_decision": {
            "value": admission_value,
            "evidence_quote": admission_quote,
            "reason_code": admission_reason,
        },
        "audience_decision": {
            "value": audience_value,
            "confidence": 0.9 if audience_value != "unknown" else 0.0,
            "evidence_quote": audience_quote,
            "reason_code": audience_reason,
        },
        "people_appearances": people or [],
    }


def _candidate(**kwargs) -> EventCandidate:
    values = {
        "source_type": "telegram",
        "source_url": "https://t.me/example/10",
        "source_text": "Вход бесплатный. Событие для всей семьи.",
        "title": "Событие",
        "date": "2026-08-10",
        "time": "18:00",
        "location_name": "Зал",
    }
    values.update(kwargs)
    return EventCandidate(**values)


def test_candidate_only_router_is_explicit_hash_bound_and_keeps_signals_out_of_evidence():
    candidate = _candidate(
        source_text="Описание без указания аудитории.",
        age_restriction="6+",
        collection_adjudication_reasons=["audience"],
    )
    request = build_collection_adjudication_request(candidate)
    assert request is not None
    assert request["input_hash"]
    assert request["candidate_signals_not_proof"]["age_restriction"] == "6+"
    assert "6+" not in request["source_corpus"]
    assert build_collection_adjudication_request(
        _candidate(collection_adjudication_reasons=[])
    ) is None


def test_production_router_covers_corrections_and_signal_candidates_not_ticket_status_alone():
    assert route_collection_adjudication_reasons(
        _candidate(source_text="Билеты уже в продаже.", ticket_status="available")
    ) == []
    assert route_collection_adjudication_reasons(_candidate(is_free=True)) == ["admission"]
    assert route_collection_adjudication_reasons(_candidate(age_restriction="6+")) == []
    assert route_collection_adjudication_reasons(_candidate(topics=["KIDS_SCHOOL"])) == [
        "audience"
    ]
    assert route_collection_adjudication_reasons(
        _candidate(topics=["PERSONALITIES"])
    ) == ["people"]

    paid_correction_target = Event(
        id=7,
        title="T",
        description="D",
        date="2026-08-10",
        time="18:00",
        location_name="L",
        source_text="S",
        is_free=True,
    )
    assert route_collection_adjudication_reasons(
        _candidate(is_free=False, ticket_price_min=500), paid_correction_target
    ) == ["admission"]


@pytest.mark.parametrize("signal_reason", ["age_rating_signal", "topic_signal", "bge_signal"])
def test_audience_hard_negatives_fail_closed(signal_reason):
    payload = _payload(
        audience_value="kids",
        audience_quote="6+",
        audience_reason=signal_reason,
    )
    assert validate_collection_adjudication_output(
        payload,
        source_corpus="Возрастное ограничение: 6+.",
    ) is None


def test_grounded_audience_requires_exact_source_quote():
    payload = _payload(
        audience_value="family",
        audience_quote="для всей семьи",
        audience_reason="explicit_family_format",
    )
    assert validate_collection_adjudication_output(
        payload, source_corpus="Событие для всей семьи."
    ) is not None
    payload["audience_decision"]["evidence_quote"] = "семейное событие"
    assert validate_collection_adjudication_output(
        payload, source_corpus="Событие для всей семьи."
    ) is None


def test_ticket_sale_alone_is_not_confirmed_paid_but_optional_donation_can_be_free():
    sale = _payload(
        admission_value="confirmed_paid",
        admission_quote="Билеты уже в продаже",
        admission_reason="ticket_sale_status",
    )
    assert validate_collection_adjudication_output(
        sale, source_corpus="Билеты уже в продаже."
    ) is None

    donation = _payload(
        admission_value="confirmed_free",
        admission_quote="Вход свободный, донат по желанию",
        admission_reason="optional_donation",
    )
    assert validate_collection_adjudication_output(
        donation, source_corpus="Вход свободный, донат по желанию."
    ) is not None


def test_people_mention_and_confirmed_appearance_remain_distinct_and_origin_is_grounded():
    corpus = (
        "В обзоре упомянут Иван Петров. "
        "На встрече выступит Анна Смирнова. Анна Смирнова приезжает из Москвы."
    )
    people = [
        {
            "name": "Иван Петров",
            "role": "speaker",
            "appearance": "mentioned",
            "origin_scope": "unknown",
            "evidence_quote": "В обзоре упомянут Иван Петров",
            "origin_evidence_quote": "",
            "reason_code": "report_only",
        },
        {
            "name": "Анна Смирнова",
            "role": "speaker",
            "appearance": "confirmed",
            "origin_scope": "russia_nonlocal",
            "evidence_quote": "На встрече выступит Анна Смирнова",
            "origin_evidence_quote": "Анна Смирнова приезжает из Москвы",
            "reason_code": "explicit_future_participation",
        },
    ]
    result = validate_collection_adjudication_output(_payload(people=people), source_corpus=corpus)
    assert result is not None
    assert [item["appearance"] for item in result["people_appearances"]] == [
        "mentioned",
        "confirmed",
    ]

    guessed_origin = _payload(people=[{**people[0], "origin_scope": "foreign"}])
    assert validate_collection_adjudication_output(guessed_origin, source_corpus=corpus) is None


@pytest.mark.asyncio
async def test_candidate_adjudication_seam_fails_closed_without_external_calls(monkeypatch):
    candidate = _candidate(collection_adjudication_reasons=["changed"])

    async def provider_failure(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sut, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(sut, "_ask_gemma_json", provider_failure)
    assert await adjudicate_collection_candidate(candidate) is None
    assert candidate.collection_semantic_decisions is None


@pytest.mark.asyncio
async def test_irrelevant_candidate_never_calls_provider(monkeypatch):
    candidate = _candidate(
        age_restriction="6+",
        collection_adjudication_reasons=[],
    )
    candidate.collection_adjudication_reasons = route_collection_adjudication_reasons(
        candidate
    )

    async def forbidden_provider(*_args, **_kwargs):
        raise AssertionError("provider must not be called for an unrouted event")

    monkeypatch.setattr(sut, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(sut, "_ask_gemma_json", forbidden_provider)
    assert await adjudicate_collection_candidate(candidate) is None
