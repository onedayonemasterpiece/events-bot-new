import json
from types import SimpleNamespace

import pytest

import main
from google_ai.exceptions import RateLimitError
from source_parse_contract import (
    EvidenceManifest,
    SourceDisposition,
    SourceParseDecision,
    SourceParseRetryReason,
    VerificationReason,
)


def _decision(events=None, disposition=None):
    return SourceParseDecision(
        events or [],
        disposition=disposition,
        evidence_manifest=EvidenceManifest.complete_source("source", ["ocr"]),
    )


@pytest.mark.parametrize(
    "reason",
    [
        VerificationReason.NO_EVENT_WITH_STRONG_SIGNALS,
        VerificationReason.EVENT_DATE_CONFLICT,
        VerificationReason.MULTIPLE_OCCURRENCES_COLLAPSED,
        VerificationReason.GENERIC_UNGROUNDED_TITLE,
        VerificationReason.LIFECYCLE_MIXED_CONTENT_CONFLICT,
        VerificationReason.IMPOSSIBLE_SCHEMA_VALUE,
        VerificationReason.INCOMPLETE_EVIDENCE,
    ],
)
@pytest.mark.asyncio
async def test_t32_t35_only_closed_contradiction_classes_trigger_verifier(monkeypatch, reason):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    monkeypatch.setenv("EVENT_PARSE_DEFENDER_ESCALATION_MODEL", "configured-escalation")
    calls = []

    async def fake_gemma(text, source_channel=None, **kwargs):
        calls.append(kwargs)
        if kwargs.get("verification_request"):
            request = kwargs["verification_request"]
            assert request["source_text"] == text
            assert request["ocr_blocks"] == ["ocr"]
            assert request["primary_result"]
            assert request["evidence_manifest"]
            assert request["contradiction_facts"][0]["reason"] == reason.value
            return _decision([{"title": "Исправленное событие", "date": "2026-09-01"}])
        return _decision([], SourceDisposition.CONFIRMED_NO_EVENT)

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm(
        "source",
        poster_texts=["ocr"],
        contradiction_facts=[reason],
        published_at="2026-08-11T10:00:00+02:00",
        source_context={"url": "https://example.test/post"},
    )
    assert len(calls) == 2
    assert calls[1]["gemma_model"] == "configured-escalation"
    assert result.disposition is SourceDisposition.EVENTS_FOUND
    assert result.verification == {"performed": True, "reasons": [reason.value]}


@pytest.mark.asyncio
async def test_t38_normal_carrier_has_exactly_one_primary_call(monkeypatch):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    calls = []

    async def fake_gemma(*args, **kwargs):
        calls.append(kwargs)
        return _decision([{"title": "Лекция «Архитектура города»", "date": "2026-09-01"}])

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm("source", poster_texts=["ocr"])
    assert len(calls) == 1
    assert result.disposition is SourceDisposition.EVENTS_FOUND


@pytest.mark.asyncio
async def test_generic_title_uses_conditional_verifier_not_deterministic_veto(monkeypatch):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    monkeypatch.setenv("EVENT_PARSE_DEFENDER_ESCALATION_MODEL", "configured-escalation")
    calls = []

    async def fake_gemma(*args, **kwargs):
        calls.append(kwargs)
        if kwargs.get("verification_request"):
            return _decision([{"title": "Концерт «Скитальцы»"}])
        return _decision([{"title": "Концерт — Бар Бастион"}])

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm("source")
    assert len(calls) == 2
    assert "Скитальцы" in result[0]["title"]


@pytest.mark.asyncio
async def test_verifier_technical_failure_returns_typed_retry(monkeypatch):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    calls = 0

    async def fake_gemma(*args, **kwargs):
        nonlocal calls
        calls += 1
        if kwargs.get("verification_request"):
            raise TimeoutError("verification timeout")
        return _decision([{"title": "Лекция", "date": "2026-09-01"}])

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm(
        "source", contradiction_facts=[VerificationReason.EVENT_DATE_CONFLICT]
    )
    assert calls == 2
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.VERIFICATION_TECHNICAL_ERROR


def test_maximum_recall_prompt_contains_complete_semantic_contract():
    main._read_base_prompt.cache_clear()
    prompt = main._read_base_prompt()
    required = [
        "all supplied source text",
        "all poster/card OCR",
        "Do not stop after the first apparent date",
        "Separate every independently attendable sibling",
        "past recap/history section plus a concrete future announcement",
        "Giveaway mechanics do not erase the real event",
        "Mixed content may contain cancellations",
        "Return lifecycle actions separately",
        "Regex/keyword/date hints are neutral evidence only",
    ]
    for clause in required:
        assert clause in prompt


def test_typed_provider_payload_preserves_multiple_actions_and_sessions():
    payload = {
        "disposition": "MIXED",
        "events": [
            {"title": "Сеанс", "date": "2026-09-01", "time": "12:00"},
            {"title": "Сеанс", "date": "2026-09-01", "time": "18:00"},
        ],
        "lifecycle_actions": [
            {"action": "CANCEL", "target_title": "Старый сеанс", "evidence": "отмена"},
            {"action": "RESCHEDULE_TIME", "target_title": "Другой сеанс", "new_time": "20:00", "evidence": "перенос"},
        ],
        "evidence_complete": True,
        "parse_version": "source-parse-v1",
    }
    result = main._event_parse_normalize_parsed_events(
        json.loads(json.dumps(payload)),
        evidence_manifest=EvidenceManifest.complete_source("source", ["ocr"]),
    )
    assert len(result) == 2
    assert len(result.lifecycle_actions) == 2
    assert result.disposition is SourceDisposition.MIXED


def test_production_normalizer_missing_manifest_is_typed_schema_retry():
    result = main._event_parse_normalize_parsed_events(
        {
            "disposition": "CONFIRMED_NO_EVENT",
            "events": [],
            "lifecycle_actions": [],
            "evidence_complete": True,
            "parse_version": "source-parse-v1",
        },
        evidence_manifest=None,
    )
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


@pytest.mark.asyncio
async def test_mocked_empty_provider_response_is_typed_retry(monkeypatch):
    class FakeClient:
        async def generate_content_async(self, **_kwargs):
            return "", SimpleNamespace(input_tokens=1, output_tokens=0, total_tokens=1)

    async def noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())
    monkeypatch.setattr(main, "log_token_usage", noop)
    result = await main._parse_event_via_gemma("source", poster_texts=["ocr"])
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.EMPTY_PROVIDER_RESPONSE


@pytest.mark.asyncio
async def test_mocked_malformed_provider_response_repairs_once_then_retries(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.calls = 0

        async def generate_content_async(self, **_kwargs):
            self.calls += 1
            return "not-json", SimpleNamespace(input_tokens=1, output_tokens=1, total_tokens=2)

    async def noop(*_args, **_kwargs):
        return None

    client = FakeClient()
    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: client)
    monkeypatch.setattr(main, "log_token_usage", noop)
    monkeypatch.delenv("EVENT_PARSE_ENABLE_4O_FALLBACK", raising=False)
    result = await main._parse_event_via_gemma("source", poster_texts=["ocr"])
    assert client.calls == 2
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.MALFORMED_JSON


@pytest.mark.asyncio
async def test_mocked_finish_length_signal_is_retry_without_accepting_partial_json(monkeypatch):
    class FakeClient:
        async def generate_content_async(self, **_kwargs):
            return "[]", SimpleNamespace(
                input_tokens=1,
                output_tokens=1,
                total_tokens=2,
                finish_reason="length",
            )

    async def noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())
    monkeypatch.setattr(main, "log_token_usage", noop)
    result = await main._parse_event_via_gemma("source", poster_texts=["ocr"])
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.OUTPUT_TRUNCATED


@pytest.mark.asyncio
async def test_provider_rate_limit_metadata_survives_typed_parse_boundary(monkeypatch):
    class FakeClient:
        async def generate_content_async(self, **_kwargs):
            raise RateLimitError(
                blocked_reason="rpd",
                retry_after_ms=3_600_000,
                model="gemma-4-31b-it",
                quota_scope="google:shared-project",
                quota_reason="RPD_EXHAUSTED",
            )

    monkeypatch.setattr(main, "_get_event_parse_gemma_client", lambda: FakeClient())
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    result = await main.parse_event_via_llm("source", poster_texts=["ocr"])

    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.TECHNICAL_ERROR
    assert result.provider_attempts[-1] == {
        "attempt_kind": "primary",
        "model": "gemma-4-31b-it",
        "quota_scope": "google:shared-project",
        "quota_reason": "RPD_EXHAUSTED",
        "provider_retry_after_ms": 3_600_000,
    }
