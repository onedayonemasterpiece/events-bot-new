import json

import pytest

from source_parse_contract import (
    ContradictionFact,
    EvidenceManifest,
    LifecycleActionType,
    ParsedEvents,
    SourceDisposition,
    SourceNoEventReason,
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


def test_confirmed_giveaway_only_reason_is_closed_and_preserved():
    result = decision_from_provider_payload(
        {
            **_typed("CONFIRMED_NO_EVENT"),
            "no_event_reason": "GIVEAWAY_ONLY",
        },
        evidence_manifest=_manifest(),
    )
    assert result.disposition is SourceDisposition.CONFIRMED_NO_EVENT
    assert result.no_event_reason is SourceNoEventReason.GIVEAWAY_ONLY
    assert result.to_payload()["no_event_reason"] == "GIVEAWAY_ONLY"


@pytest.mark.parametrize(
    "payload",
    [
        {**_typed("CONFIRMED_NO_EVENT"), "no_event_reason": "UNKNOWN"},
        {
            **_typed(events=[{"title": "Event"}]),
            "no_event_reason": "GIVEAWAY_ONLY",
        },
    ],
)
def test_unknown_or_misplaced_no_event_reason_is_schema_retry(payload):
    result = decision_from_provider_payload(payload, evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


def test_t22_empty_provider_body_is_retry_not_no_event_contract():
    result = SourceParseDecision.retry(
        SourceParseRetryReason.EMPTY_PROVIDER_RESPONSE,
        evidence_manifest=_manifest(),
    )
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.EMPTY_PROVIDER_RESPONSE


def test_provider_attempt_receipts_are_secret_free_and_round_trip():
    result = SourceParseDecision.retry(
        SourceParseRetryReason.TECHNICAL_ERROR,
        evidence_manifest=_manifest(),
        provider_attempts=[
            {
                "attempt_kind": "primary",
                "model": "gemma-4-31b-it",
                "quota_scope": "google:project-a",
                "request_id": "request-1",
                "input_tokens": 600,
                "output_tokens": 20,
                "thought_tokens": 10,
                "reserved_tokens": 900,
                "provider_retry_after_ms": 3000,
            }
        ],
    )
    payload = result.to_payload()
    assert payload["provider_attempts"] == list(result.provider_attempts)
    assert "api_key" not in json.dumps(payload)


def test_t22_legacy_empty_array_cannot_claim_confirmed_no_event():
    result = decision_from_provider_payload([], evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


def test_verdict_a_untyped_none_is_schema_retry():
    result = decision_from_provider_payload(None, evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


def test_verdict_b_empty_inference_is_schema_retry_not_no_event():
    result = SourceParseDecision([], evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


@pytest.mark.parametrize("payload", [[{}], {"date": "2026-09-01"}])
def test_verdict_c_legacy_positive_adapter_requires_minimum_event_schema(payload):
    result = decision_from_provider_payload(payload, evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


def test_verdict_d_schema_valid_legacy_positive_remains_supported():
    result = decision_from_provider_payload(
        [{"title": "Лекция", "date": "2026-09-01"}],
        evidence_manifest=_manifest(),
    )
    assert result.disposition is SourceDisposition.EVENTS_FOUND
    assert result.parse_version == "legacy-array-adapter-v1"


@pytest.mark.parametrize(
    "payload",
    [
        _typed("NOT_A_DISPOSITION"),
        {**_typed("RETRY_REQUIRED"), "retry_reason": "NOT_A_REASON"},
        _typed("RETRY_REQUIRED"),
    ],
)
def test_verdict_e_unknown_disposition_or_retry_reason_is_schema_retry(payload):
    result = decision_from_provider_payload(payload, evidence_manifest=_manifest())
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


def test_verdict_f_missing_manifest_is_schema_retry():
    result = decision_from_provider_payload(_typed("CONFIRMED_NO_EVENT"), evidence_manifest=None)
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


def test_explicit_typed_disposition_must_match_children():
    result = SourceParseDecision(
        [{"title": "Лекция"}],
        disposition=SourceDisposition.CONFIRMED_NO_EVENT,
        evidence_manifest=_manifest(),
        evidence_complete=True,
    )
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
    assert result.to_payload()["enrichment_required"] is True


def test_evidence_a_text_only_explicit_zero_attachments_is_complete():
    manifest = EvidenceManifest.complete_source("text", [], attachment_count=0)
    assert manifest.evidence_complete is True


def test_evidence_b_attachment_without_ocr_is_counted_unavailable():
    manifest = EvidenceManifest.complete_source("caption", [], attachment_count=1)
    assert manifest.attachment_count == 1
    assert manifest.unavailable_attachment_count == 1
    assert manifest.ocr_complete is False
    assert manifest.evidence_complete is False


def test_evidence_c_available_but_omitted_ocr_is_incomplete():
    manifest = EvidenceManifest(
        raw_text_chars=7,
        raw_text_hash="a" * 64,
        attachment_count=2,
        ocr_blocks_available=2,
        ocr_blocks_included=1,
        included_chars=10,
    )
    assert manifest.omitted_blocks
    assert manifest.ocr_complete is False
    assert manifest.evidence_complete is False


def test_evidence_d_missing_or_inconsistent_mapping_fails_closed():
    missing = EvidenceManifest.from_mapping({})
    inconsistent = EvidenceManifest.from_mapping(
        {
            **_manifest().to_payload(),
            "attachment_count": 1,
            "ocr_blocks_available": 2,
        }
    )
    assert missing.evidence_complete is False
    assert inconsistent.evidence_complete is False


def test_evidence_e_incomplete_positive_survives_but_no_event_retries():
    manifest = EvidenceManifest.complete_source("caption", [], attachment_count=1)
    positive = decision_from_provider_payload(
        _typed(events=[{"title": "Лекция"}], complete=True),
        evidence_manifest=manifest,
    )
    no_event = decision_from_provider_payload(
        _typed("CONFIRMED_NO_EVENT"), evidence_manifest=manifest
    )
    assert positive.disposition is SourceDisposition.EVENTS_FOUND
    assert positive.enrichment_required is True
    assert no_event.disposition is SourceDisposition.RETRY_REQUIRED
    assert no_event.retry_reason is SourceParseRetryReason.EVIDENCE_INCOMPLETE


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
