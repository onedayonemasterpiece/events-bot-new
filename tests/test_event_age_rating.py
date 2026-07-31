from __future__ import annotations

from datetime import datetime, timezone

import pytest

from event_age_rating import (
    AGE_DECISION_JSON_SCHEMA,
    AgeRatingDecision,
    apply_age_decision,
    decision_from_semantic_payload,
    declared_structured_decision,
    normalize_age_restriction,
    public_age_projection,
    reconcile_age_decision,
)
from models import Event
from smart_event_update import (
    CREATE_BUNDLE_SCHEMA,
    MERGE_SCHEMA,
    SMART_UPDATE_EVENT_AGE_LLM_MODE,
    EventCandidate,
    _candidate_age_decision,
)


@pytest.mark.parametrize("value", ["0+", "6+", "12+", "16+", "18+"])
def test_allowed_age_values(value):
    assert normalize_age_restriction(value) == value


@pytest.mark.parametrize("value", [None, "", "5+", "21+", "до 18:00", "18 лет", True])
def test_invalid_age_values_never_default_to_zero(value):
    assert normalize_age_restriction(value) is None


def test_structured_candidate_requires_explicit_structured_flag():
    candidate = EventCandidate(
        source_type="parser:qtickets",
        source_url="https://tickets.example/event/1",
        source_text="Описание",
        age_restriction="12+",
        age_restriction_is_structured=True,
    )
    decision = _candidate_age_decision(candidate)
    assert decision is not None
    assert decision.status == "declared"
    assert decision.value == "12+"
    assert decision.provenance == "official_structured"


def test_semantic_declared_requires_exact_grounded_quote():
    payload = {
        "status": "declared",
        "value": "16+",
        "provenance": "organizer_text",
        "confidence": 0.99,
        "evidence_quote": "Возрастное ограничение 16+",
        "evidence_kind": "source_text",
        "source_document_id": "post:1",
        "rubric_codes": [],
        "reason_code": "explicit_event_rating",
    }
    assert decision_from_semantic_payload(
        payload,
        source_url="https://example.test/1",
        source_corpora=["Для события указано: Возрастное ограничение 16+."],
        input_hash="a" * 64,
    ) is not None
    payload["evidence_quote"] = "Возрастное ограничение 18+"
    assert decision_from_semantic_payload(
        payload,
        source_url="https://example.test/1",
        source_corpora=["Для события указано: Возрастное ограничение 16+."],
        input_hash="a" * 64,
    ) is None
    payload["unexpected"] = True
    assert decision_from_semantic_payload(
        payload,
        source_url="https://example.test/1",
        source_corpora=["Возрастное ограничение 18+"],
        input_hash="a" * 64,
    ) is None


def make_event(**kwargs) -> Event:
    base = dict(
        title="Тест",
        description="Описание",
        date="2026-08-01",
        time="18:00",
        location_name="Площадка",
        source_text="Источник",
    )
    base.update(kwargs)
    return Event(**base)


def test_different_declared_values_fail_closed_as_conflict():
    event = make_event(
        age_restriction="6+",
        age_restriction_status="declared",
        age_restriction_provenance="official_structured",
        age_restriction_evidence={"kind": "structured"},
    )
    incoming = declared_structured_decision(
        "12+", source_url="https://other.test", source_type="parser:other", input_hash="b" * 64
    )
    assert incoming is not None
    conflict = reconcile_age_decision(event, incoming)
    assert conflict.status == "conflict"
    assert apply_age_decision(event, conflict, now=datetime(2026, 7, 15, tzinfo=timezone.utc))
    assert event.age_restriction is None
    assert event.age_restriction_status == "conflict"
    assert len(event.age_restriction_evidence["values"]) == 2
    # Replaying either source must not make "last refresh wins".
    replay = reconcile_age_decision(event, incoming)
    assert replay.status == "conflict"
    assert not apply_age_decision(event, replay)
    assert event.age_restriction is None


def test_assessment_is_stored_separately_and_public_default_is_declared_only():
    event = make_event()
    decision = AgeRatingDecision(
        status="assessed",
        value="12+",
        provenance="llm_assessed",
        confidence=0.8,
        evidence={"kind": "content_assessment"},
        input_hash="c" * 64,
        assessment_engine="smart_update_llm",
    )
    assert apply_age_decision(event, decision)
    assert event.age_restriction is None
    assert event.age_assessment == "12+"
    assert event.age_assessment_status == "assessed"
    assert public_age_projection(event)["age_restriction"] is None
    labeled = public_age_projection(event, policy="declared_or_assessed_labeled")
    assert labeled["age_recommendation"] == "12+"

    # A later source with no usable evidence must not silently discard the
    # previously accepted internal assessment.
    missing = AgeRatingDecision(status="insufficient_evidence", input_hash="e" * 64)
    assert not apply_age_decision(event, missing)
    assert event.age_restriction_status == "assessed"
    assert event.age_assessment == "12+"


def test_replay_same_decision_has_no_churn():
    event = make_event()
    decision = declared_structured_decision(
        "6+", source_url="https://tickets.test/1", source_type="parser:qtickets", input_hash="d" * 64
    )
    assert decision is not None
    assert apply_age_decision(event, decision)
    updated_at = event.age_restriction_updated_at
    assert not apply_age_decision(event, decision)
    assert event.age_restriction_updated_at == updated_at


def test_smart_update_age_schema_is_piggyback_only():
    assert "age_decision" in CREATE_BUNDLE_SCHEMA["properties"]
    assert "age_decision" in MERGE_SCHEMA["properties"]
    assert SMART_UPDATE_EVENT_AGE_LLM_MODE in {"off", "piggyback_only"}


def test_age_schema_uses_nullable_type_without_null_enum_members():
    """google-genai Schema.enum accepts strings only; null comes from type."""

    for name in ("value", "provenance", "evidence_kind"):
        property_schema = AGE_DECISION_JSON_SCHEMA["properties"][name]
        assert "null" in property_schema["type"]
        assert None not in property_schema["enum"]
        assert all(isinstance(value, str) for value in property_schema["enum"])
