import pytest

from source_parse_contract import (
    ContradictionFact,
    EvidenceManifest,
    LifecycleActionType,
    ParsedEvents,
    SourceDisposition,
    SourceParseDecision,
    SourceParseRetryReason,
    VerificationReason,
    conditionally_verify_source_decision,
    decision_from_provider_payload,
)


def _manifest(**overrides):
    payload = EvidenceManifest.complete_source("Полный текст", ["OCR 1", "OCR 2"]).to_payload()
    payload.pop("evidence_complete")
    payload.update(overrides)
    return EvidenceManifest.from_mapping(payload)


def _typed(disposition="EVENTS_FOUND", events=None, actions=None, complete=True):
    return {
        "disposition": disposition,
        "events": events or [],
        "lifecycle_actions": actions or [],
        "evidence_complete": complete,
        "parse_version": "test-v1",
    }


def test_t21_valid_confirmed_no_event_is_typed_product_outcome():
    result = decision_from_provider_payload(
        _typed("CONFIRMED_NO_EVENT"), evidence_manifest=_manifest()
    )
    assert result.disposition is SourceDisposition.CONFIRMED_NO_EVENT
    assert result.evidence_complete is True
    assert list(result) == []


def test_t22_empty_provider_body_is_retry_not_no_event_contract():
    result = SourceParseDecision.retry(
        SourceParseRetryReason.EMPTY_PROVIDER_RESPONSE,
        evidence_manifest=_manifest(),
    )
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.EMPTY_PROVIDER_RESPONSE


def test_t22_legacy_empty_array_cannot_claim_confirmed_no_event():
    result = decision_from_provider_payload([], evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


@pytest.mark.parametrize(
    ("reason", "provider_metadata"),
    [
        (SourceParseRetryReason.MALFORMED_JSON, None),
        (SourceParseRetryReason.OUTPUT_TRUNCATED, {"finish_reason": "length"}),
    ],
)
def test_t23_t24_malformed_or_truncated_never_becomes_no_event(reason, provider_metadata):
    if provider_metadata:
        result = decision_from_provider_payload(
            _typed("CONFIRMED_NO_EVENT"),
            evidence_manifest=_manifest(),
            provider_metadata=provider_metadata,
        )
    else:
        result = SourceParseDecision.retry(reason, evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is reason


def test_t25_schema_mismatch_is_retry():
    result = decision_from_provider_payload(
        {"disposition": "EVENTS_FOUND", "events": "not-a-list"},
        evidence_manifest=_manifest(),
    )
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


@pytest.mark.parametrize("technical", ["timeout", "429", "rpd"])
def test_t26_technical_errors_have_typed_retry(technical):
    result = SourceParseDecision.retry(
        SourceParseRetryReason.TECHNICAL_ERROR,
        evidence_manifest=_manifest(),
        verification_reasons=[],
    )
    assert technical
    assert result.is_retry
    assert result.retry_reason is SourceParseRetryReason.TECHNICAL_ERROR


def test_t27_multi_event_source_preserves_every_child_and_list_compatibility():
    events = [{"title": "Лекция 1"}, {"title": "Концерт 2"}, {"title": "Выставка 3"}]
    result = decision_from_provider_payload(
        _typed(events=events), evidence_manifest=_manifest()
    )
    assert result.events == events
    assert list(result) == events
    assert result[1]["title"] == "Концерт 2"
    assert isinstance(result, ParsedEvents)


def test_t28_separate_sessions_remain_occurrence_children():
    events = [
        {"title": "Спектакль", "date": "2026-08-20", "time": "12:00"},
        {"title": "Спектакль", "date": "2026-08-20", "time": "17:00"},
    ]
    result = decision_from_provider_payload(
        _typed(events=events), evidence_manifest=_manifest()
    )
    assert [event["time"] for event in result] == ["12:00", "17:00"]


@pytest.mark.parametrize(
    "fixture_events",
    [
        [{"title": "Будущая лекция", "date": "2026-08-29", "source_role": "recap_plus_future"}],
        [{"title": "Фестиваль", "date": "2026-08-30", "source_role": "giveaway_plus_event"}],
    ],
)
def test_t29_t30_recap_or_giveaway_does_not_delete_future_event(fixture_events):
    result = decision_from_provider_payload(
        _typed(events=fixture_events), evidence_manifest=_manifest()
    )
    assert result.disposition is SourceDisposition.EVENTS_FOUND
    assert result == fixture_events


def test_t31_mixed_lifecycle_and_new_event_are_both_preserved():
    payload = _typed(
        "MIXED",
        events=[{"title": "Новая встреча", "date": "2026-09-01"}],
        actions=[
            {
                "action": "CANCEL",
                "target_title": "Старая встреча",
                "target_date": "2026-08-20",
                "evidence": "встреча отменена",
            }
        ],
    )
    result = decision_from_provider_payload(payload, evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.MIXED
    assert result[0]["title"] == "Новая встреча"
    assert result.lifecycle_actions[0].action is LifecycleActionType.CANCEL


def test_incomplete_evidence_forbids_confirmed_no_event():
    result = decision_from_provider_payload(
        _typed("CONFIRMED_NO_EVENT"),
        evidence_manifest=_manifest(ocr_blocks_included=1, omitted_blocks=["poster:2"]),
    )
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.EVIDENCE_INCOMPLETE


def test_positive_events_survive_partial_evidence_and_require_enrichment():
    result = decision_from_provider_payload(
        _typed(events=[{"title": "Лекция"}], complete=False),
        evidence_manifest=_manifest(ocr_blocks_included=1, omitted_blocks=["poster:2"]),
    )
    assert result.disposition is SourceDisposition.EVENTS_FOUND
    assert result == [{"title": "Лекция"}]
    assert result.enrichment_required is True


@pytest.mark.asyncio
async def test_t36_verifier_technical_error_is_retry_and_keeps_positive_children():
    primary = SourceParseDecision(
        [{"title": "Лекция"}], evidence_manifest=_manifest()
    )

    async def fail(_facts):
        raise TimeoutError("provider timeout")

    result = await conditionally_verify_source_decision(
        primary,
        contradiction_facts=[VerificationReason.EVENT_DATE_CONFLICT],
        invoke=fail,
    )
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.VERIFICATION_TECHNICAL_ERROR
    assert result == [{"title": "Лекция"}]


@pytest.mark.asyncio
async def test_t37_verifier_uncertainty_is_retry_not_reject():
    primary = SourceParseDecision([], evidence_manifest=_manifest())

    async def uncertain(_facts):
        return SourceParseDecision.retry(
            SourceParseRetryReason.VERIFICATION_UNCERTAIN,
            evidence_manifest=_manifest(),
        )

    result = await conditionally_verify_source_decision(
        primary,
        contradiction_facts=[
            ContradictionFact(
                VerificationReason.NO_EVENT_WITH_STRONG_SIGNALS,
                "ticket URL and future date",
            )
        ],
        invoke=uncertain,
    )
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.VERIFICATION_UNCERTAIN


def test_festival_metadata_legacy_adapter_is_preserved():
    result = decision_from_provider_payload(
        {
            **_typed(events=[{"title": "Событие фестиваля"}]),
            "festival": {"name": "Фестиваль"},
        },
        evidence_manifest=_manifest(),
    )
    assert result.festival == {"name": "Фестиваль"}
