from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

import smart_event_update as sut
from models import Event
from smart_event_update import (
    STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
    STATIC_COLLECTION_FACTS_POLICY_VERSION,
    EventCandidate,
    adjudicate_collection_candidate,
    build_collection_adjudication_request,
    collection_adjudication_input_hash,
    get_smart_update_llm_trace,
    reset_smart_update_llm_trace,
    route_collection_adjudication_reasons,
    validate_collection_adjudication_output,
)


def _payload(
    *,
    admission_value: str = "unknown",
    admission_quote: str = "",
    admission_reason: str = "insufficient_evidence",
    child_value: str = "unknown",
    child_quote: str = "",
    child_reason: str = "insufficient_evidence",
    family_value: str = "unknown",
    family_quote: str = "",
    family_reason: str = "insufficient_evidence",
    joint_value: str = "unknown",
    joint_quote: str = "",
    joint_reason: str = "insufficient_evidence",
    people: list[dict] | None = None,
) -> dict:
    return {
        "schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "admission_decision": {
            "value": admission_value,
            "evidence_quote": admission_quote,
            "reason_code": admission_reason,
        },
        "child_directed_decision": {
            "value": child_value,
            "confidence": 0.9 if child_value != "unknown" else 0.0,
            "evidence_quote": child_quote,
            "reason_code": child_reason,
        },
        "family_suitable_decision": {
            "value": family_value,
            "confidence": 0.9 if family_value != "unknown" else 0.0,
            "evidence_quote": family_quote,
            "reason_code": family_reason,
        },
        "joint_family_activity_decision": {
            "value": joint_value,
            "confidence": 0.9 if joint_value != "unknown" else 0.0,
            "evidence_quote": joint_quote,
            "reason_code": joint_reason,
        },
        "people_appearances": people or [],
    }


def _candidate(**kwargs) -> EventCandidate:
    values = {
        "source_type": "telegram",
        "source_url": "https://t.me/example/10",
        "source_text": "Описание события.",
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
    assert request["policy_version"] == STATIC_COLLECTION_FACTS_POLICY_VERSION
    assert request["candidate_signals_not_proof"]["age_restriction"] == "6+"
    assert "6+" not in request["source_corpus"]
    assert build_collection_adjudication_request(
        _candidate(collection_adjudication_reasons=[])
    ) is None


def test_collection_input_hash_is_bound_to_semantic_policy(monkeypatch):
    candidate = _candidate(collection_adjudication_reasons=["audience"])
    current = collection_adjudication_input_hash(candidate)
    monkeypatch.setattr(sut, "STATIC_COLLECTION_FACTS_POLICY_VERSION", "test-next-policy")
    assert collection_adjudication_input_hash(candidate) != current


def test_collection_model_route_stays_on_gemma_even_during_staged_gemini(monkeypatch):
    monkeypatch.setattr(sut, "SMART_UPDATE_FORCE_STAGED_GEMINI", True)
    monkeypatch.setattr(sut, "SMART_UPDATE_WRITER_MODEL", "gemini-3.1-flash-lite")
    monkeypatch.setattr(sut, "SMART_UPDATE_MODEL", "gemma-4-31b-it")
    assert sut._resolve_smart_update_model("collection_candidate_adjudication") == (
        "gemma-4-31b-it"
    )


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
    assert route_collection_adjudication_reasons(
        _candidate(source_text="Родители и дети вместе создадут общую работу.")
    ) == ["audience"]

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

    for existing_key in (
        "audience_decision",
        "child_directed_decision",
        "family_suitable_decision",
        "joint_family_activity_decision",
    ):
        existing = Event(
            id=8,
            title="T",
            description="D",
            date="2026-08-10",
            time="18:00",
            location_name="L",
            source_text="S",
            collection_decisions={existing_key: {"value": "unknown"}},
        )
        assert route_collection_adjudication_reasons(_candidate(), existing) == ["audience"]


def test_age_only_claim_fails_closed():
    payload = _payload(
        child_value="confirmed",
        child_quote="6+",
        child_reason="explicit_child_audience",
    )
    assert validate_collection_adjudication_output(
        payload,
        source_corpus="Возрастное ограничение: 6+.",
    ) is None


def test_grounded_family_requires_exact_source_quote_and_rejects_whole_payload():
    payload = _payload(
        family_value="confirmed",
        family_quote="для всей семьи",
        family_reason="explicit_family_invitation",
    )
    assert validate_collection_adjudication_output(
        payload, source_corpus="Событие для всей семьи."
    ) is not None
    payload["family_suitable_decision"]["evidence_quote"] = "семейное событие"
    assert validate_collection_adjudication_output(
        payload, source_corpus="Событие для всей семьи."
    ) is None


def test_audience_quote_role_guard_rejects_child_authors_and_vague_family_copy():
    child_author = _payload(
        child_value="confirmed",
        child_quote="глазами юных художников",
        child_reason="explicit_child_audience",
    )
    assert validate_collection_adjudication_output(
        child_author,
        source_corpus="Выставка глазами юных художников.",
    ) is None

    vague_family = _payload(
        family_value="confirmed",
        family_quote="семейная атмосфера",
        family_reason="explicit_family_format",
    )
    assert validate_collection_adjudication_output(
        vague_family,
        source_corpus="Вас ждёт семейная атмосфера.",
    ) is None

    explicit_family = _payload(
        family_value="confirmed",
        family_quote="интересно и детям, и взрослым",
        family_reason="explicit_children_and_adults",
    )
    assert validate_collection_adjudication_output(
        explicit_family,
        source_corpus="Будет интересно и детям, и взрослым.",
    ) is not None

    for quote in (
        "#ТеатрДляДетей",
        "https://example.test/spektakli/dlya-detej/show/",
        "Детский кукольный спектакль",
        "в Детском книжном клубе",
    ):
        direct_kids = _payload(
            child_value="confirmed",
            child_quote=quote,
            child_reason="explicit_child_audience",
        )
        assert validate_collection_adjudication_output(
            direct_kids,
            source_corpus=f"Приглашаем {quote}",
        ) is not None

    family_title = "Столярный мастер-класс «Человек – пиктограмма (семейный)»"
    direct_family = _payload(
        family_value="confirmed",
        family_quote=family_title,
        family_reason="explicit_family_format",
    )
    assert validate_collection_adjudication_output(
        direct_family,
        source_corpus=family_title,
    ) is not None


def test_child_theatre_confirms_child_without_inventing_joint_activity():
    corpus = "Приглашаем на детский кукольный спектакль."
    result = validate_collection_adjudication_output(
        _payload(
            child_value="confirmed",
            child_quote="детский кукольный спектакль",
            child_reason="explicit_child_spectators",
        ),
        source_corpus=corpus,
    )
    assert result is not None
    assert result["child_directed_decision"]["value"] == "confirmed"
    assert result["joint_family_activity_decision"]["value"] == "unknown"


def test_explicit_family_invitation_confirms_family_independently():
    corpus = "Приходите всей семьёй — будет интересно детям и взрослым."
    result = validate_collection_adjudication_output(
        _payload(
            family_value="confirmed",
            family_quote="Приходите всей семьёй",
            family_reason="explicit_family_invitation",
        ),
        source_corpus=corpus,
    )
    assert result is not None
    assert result["family_suitable_decision"]["value"] == "confirmed"
    assert result["child_directed_decision"]["value"] == "unknown"


def test_joint_parent_child_practice_requires_three_independent_grounded_facts():
    corpus = (
        "Мастер-класс для детей и родителей. "
        "Родитель и ребёнок вместе создадут одну общую работу."
    )
    result = validate_collection_adjudication_output(
        _payload(
            child_value="confirmed",
            child_quote="для детей",
            child_reason="explicit_child_participants",
            family_value="confirmed",
            family_quote="для детей и родителей",
            family_reason="explicit_children_and_adults",
            joint_value="confirmed",
            joint_quote="Родитель и ребёнок вместе создадут одну общую работу",
            joint_reason="explicit_joint_task",
        ),
        source_corpus=corpus,
    )
    assert result is not None
    assert {
        result[key]["value"]
        for key in (
            "child_directed_decision",
            "family_suitable_decision",
            "joint_family_activity_decision",
        )
    } == {"confirmed"}


def test_parents_only_can_be_denied_only_from_explicit_negative_quote():
    corpus = "Встреча только для родителей, без детей."
    result = validate_collection_adjudication_output(
        _payload(
            child_value="denied",
            child_quote="только для родителей",
            child_reason="explicit_parents_only",
            family_value="denied",
            family_quote="только для родителей",
            family_reason="explicit_parents_only",
            joint_value="denied",
            joint_quote="только для родителей",
            joint_reason="explicit_parents_only",
        ),
        source_corpus=corpus,
    )
    assert result is not None

    missing_proof = _payload(
        child_value="denied",
        child_quote="обычная встреча",
        child_reason="explicit_parents_only",
    )
    assert validate_collection_adjudication_output(
        missing_proof, source_corpus="Это обычная встреча."
    ) is None


def test_family_tournament_does_not_prove_joint_activity():
    payload = _payload(
        child_value="confirmed",
        child_quote="для детей",
        child_reason="explicit_child_participants",
        family_value="confirmed",
        family_quote="Семейный турнир",
        family_reason="explicit_family_format",
        joint_value="confirmed",
        joint_quote="Семейный турнир",
        joint_reason="explicit_parent_child_team",
    )
    assert validate_collection_adjudication_output(
        payload,
        source_corpus="Семейный турнир для детей.",
    ) is None


def test_impossible_joint_combination_rejects_entire_payload():
    corpus = "Родитель и ребёнок вместе создадут общую работу."
    payload = _payload(
        joint_value="confirmed",
        joint_quote="Родитель и ребёнок вместе создадут общую работу",
        joint_reason="explicit_joint_task",
    )
    assert validate_collection_adjudication_output(payload, source_corpus=corpus) is None


def test_one_valid_fact_and_two_unknown_is_accepted_independently():
    corpus = "Приглашаем детей на спектакль."
    result = validate_collection_adjudication_output(
        _payload(
            child_value="confirmed",
            child_quote="Приглашаем детей",
            child_reason="explicit_child_audience",
        ),
        source_corpus=corpus,
    )
    assert result is not None
    assert result["child_directed_decision"]["value"] == "confirmed"
    assert result["family_suitable_decision"]["value"] == "unknown"


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
async def test_audience_prompt_rejects_family_theme_and_child_popularity_as_proof(monkeypatch):
    candidate = _candidate(collection_adjudication_reasons=["audience"])
    captured = {}

    async def provider(prompt, *_args, **_kwargs):
        captured["prompt"] = prompt
        return _payload()

    monkeypatch.setattr(sut, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(sut, "_ask_gemma_json", provider)
    assert await adjudicate_collection_candidate(candidate) is not None
    assert "дети и взрослые/родители прямо приглашены вместе" in captured["prompt"]
    assert "популярность артиста у детей" in captured["prompt"]
    assert "отсутствие положительного доказательства всегда unknown, не denied" in captured["prompt"]


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


@pytest.mark.asyncio
async def test_collection_stage_failed_primary_counts_send_and_uses_exactly_one_4o_fallback(
    monkeypatch,
):
    class FailingPrimary:
        def __init__(self):
            self.calls = []

        async def generate_content_async(self, **kwargs):
            self.calls.append(kwargs)
            kwargs["attempt_observer"](
                {
                    "attempt_no": 1,
                    "requested_model": kwargs["model"],
                    "provider_model_name": "models/gemma-4-31b-it",
                }
            )
            raise RuntimeError("forced primary failure")

    primary = FailingPrimary()
    fallback_calls = []

    async def ask_4o(*args, **kwargs):
        fallback_calls.append((args, kwargs))
        return json.dumps(_payload(), ensure_ascii=False)

    async def notify(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sut, "_get_gemma_client", lambda: primary)
    monkeypatch.setenv("SMART_UPDATE_4O_FALLBACK", "1")
    monkeypatch.delenv("SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR", raising=False)
    monkeypatch.setitem(
        sys.modules,
        "main",
        SimpleNamespace(ask_4o=ask_4o, notify_llm_incident=notify),
    )
    reset_smart_update_llm_trace()
    result = await sut._ask_gemma_json_unbounded(
        "prompt",
        sut.COLLECTION_ADJUDICATION_JSON_SCHEMA,
        max_tokens=1000,
        label="collection_candidate_adjudication",
    )
    assert result == _payload()
    assert len(primary.calls) == 1
    assert primary.calls[0]["allow_model_fallback"] is False
    assert primary.calls[0]["max_provider_attempts"] == 1
    assert primary.calls[0]["generation_config"]["response_schema"]
    assert len(fallback_calls) == 1
    trace = get_smart_update_llm_trace()[-1]
    assert trace["physical_sends"] == 2
    assert trace["actual_models"] == ["models/gemma-4-31b-it", "gpt-4o"]
    assert trace["status"] == "ok_4o_fallback"


@pytest.mark.asyncio
async def test_collection_stage_rejects_paraphrased_whole_provider_payload(monkeypatch):
    candidate = _candidate(
        source_text="Приходите всей семьёй.",
        collection_adjudication_reasons=["audience"],
    )
    paraphrased = _payload(
        family_value="confirmed",
        family_quote="Приглашаем всю семью",
        family_reason="explicit_family_invitation",
    )

    async def provider(*_args, **_kwargs):
        return paraphrased

    monkeypatch.setattr(sut, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(sut, "_ask_gemma_json", provider)
    assert await adjudicate_collection_candidate(candidate) is None
    assert candidate.collection_semantic_decisions is None
