import json
from types import SimpleNamespace

import pytest

import main
from google_ai.exceptions import RateLimitError
from source_parse_contract import (
    EvidenceManifest,
    SourceDisposition,
    SourceNoEventReason,
    SourceParseDecision,
    SourceParseRetryReason,
    VerificationReason,
)


def _decision(events=None, disposition=None):
    return SourceParseDecision(
        events or [],
        disposition=disposition,
        no_event_reason=(
            SourceNoEventReason.NO_ATTENDABLE_EVENT
            if disposition is SourceDisposition.CONFIRMED_NO_EVENT
            else None
        ),
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


@pytest.mark.parametrize(
    ("source_text", "ocr", "primary", "expected_reason"),
    [
        (
            "Приглашаем на концерт 15.09 в 18:00, билеты доступны по ссылке",
            [],
            _decision([], SourceDisposition.CONFIRMED_NO_EVENT),
            VerificationReason.NO_EVENT_WITH_STRONG_SIGNALS,
        ),
        (
            "Подробности на афише",
            ["Лекция 12.09.2026 в 18:00"],
            _decision([{"title": "Лекция", "date": "2026-09-13"}]),
            VerificationReason.EVENT_DATE_CONFLICT,
        ),
        (
            "Сеансы 12.09: начала в 12:00, 15:00 и 18:00",
            [],
            _decision([{"title": "Один сеанс", "date": "2026-09-12", "time": "12:00"}]),
            VerificationReason.MULTIPLE_OCCURRENCES_COLLAPSED,
        ),
        (
            "Лекция 31.02.2026 в 28:90",
            [],
            _decision([{"title": "Лекция", "date": "2026-02-31", "time": "28:90"}]),
            VerificationReason.IMPOSSIBLE_SCHEMA_VALUE,
        ),
    ],
)
@pytest.mark.asyncio
async def test_v1_v2_v3_v6_production_collector_automatically_invokes_one_verifier(
    monkeypatch, source_text, ocr, primary, expected_reason
):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    calls = []

    async def fake_gemma(*args, **kwargs):
        calls.append(kwargs)
        if kwargs.get("verification_request"):
            reasons = {
                item["reason"] for item in kwargs["verification_request"]["contradiction_facts"]
            }
            assert expected_reason.value in reasons
            return _decision([{"title": "Исправленное событие", "date": "2026-09-12"}])
        return primary

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm(
        source_text,
        poster_texts=ocr,
        published_at="2026-08-12T10:00:00+02:00",
    )
    assert len(calls) == 2
    assert result.disposition is SourceDisposition.EVENTS_FOUND


@pytest.mark.asyncio
async def test_v5_lifecycle_plus_new_event_automatically_invokes_verifier(monkeypatch):
    from source_parse_contract import LifecycleAction, LifecycleActionType

    primary = SourceParseDecision(
        [],
        disposition=SourceDisposition.LIFECYCLE_ONLY,
        lifecycle_actions=[
            LifecycleAction(LifecycleActionType.CANCEL, target_title="Старый концерт")
        ],
        evidence_manifest=EvidenceManifest.complete_source("source"),
    )
    calls = []

    async def fake_gemma(*args, **kwargs):
        calls.append(kwargs)
        if kwargs.get("verification_request"):
            reasons = {
                item["reason"] for item in kwargs["verification_request"]["contradiction_facts"]
            }
            assert VerificationReason.LIFECYCLE_MIXED_CONTENT_CONFLICT.value in reasons
            return _decision([{"title": "Новый концерт", "date": "2026-09-20"}])
        return primary

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm(
        "Старый концерт отменён. Приглашаем на концерт 20.09 в 19:00, билеты доступны."
    )
    assert len(calls) == 2
    assert result[0]["title"] == "Новый концерт"


@pytest.mark.asyncio
async def test_v10_auto_verifier_timeout_preserves_positive_sibling(monkeypatch):
    primary = _decision([{"title": "Лекция", "date": "2026-09-13"}])
    calls = 0

    async def fake_gemma(*args, **kwargs):
        nonlocal calls
        calls += 1
        if kwargs.get("verification_request"):
            raise TimeoutError("verification timed out")
        return primary

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm("Лекция 12.09.2026 в 18:00")
    assert calls == 2
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.VERIFICATION_TECHNICAL_ERROR
    assert list(result) == [{"title": "Лекция", "date": "2026-09-13"}]


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


@pytest.mark.asyncio
async def test_vk_linear_primary_schema_failure_gets_one_terminal_adjudication(monkeypatch):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    calls = []

    async def fake_gemma(*args, **kwargs):
        calls.append(kwargs)
        request = kwargs.get("verification_request")
        if request:
            assert request["task"] == "terminal_source_parse_adjudication"
            assert "RETRY_REQUIRED is forbidden" in " ".join(request["rules"])
            return _decision([], SourceDisposition.CONFIRMED_NO_EVENT)
        return SourceParseDecision.retry(
            SourceParseRetryReason.MALFORMED_JSON,
            evidence_manifest=EvidenceManifest.complete_source("source", ["ocr"]),
        )

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm(
        "source",
        poster_texts=["ocr"],
        require_terminal_decision=True,
    )

    assert len(calls) == 2
    assert calls[1]["gemma_model"] == "gemini-3.5-flash-lite"
    assert result.disposition is SourceDisposition.CONFIRMED_NO_EVENT
    assert result.verification["terminal_adjudication"] is True
    assert result.verification["previous_retry_reason"] == "MALFORMED_JSON"


@pytest.mark.asyncio
async def test_vk_linear_verifier_failure_gets_third_and_final_call(monkeypatch):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    calls = []

    async def fake_gemma(*args, **kwargs):
        calls.append(kwargs)
        request = kwargs.get("verification_request")
        if request and request.get("task") == "terminal_source_parse_adjudication":
            return _decision([{"title": "Лекция", "date": "2026-09-12"}])
        if request:
            raise TimeoutError("verification unavailable")
        return _decision([{"title": "Лекция", "date": "2026-09-13"}])

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm(
        "Лекция 12.09.2026 в 18:00",
        require_terminal_decision=True,
    )

    assert len(calls) == 3
    assert calls[2]["verification_request"]["task"] == "terminal_source_parse_adjudication"
    assert result.disposition is SourceDisposition.EVENTS_FOUND
    assert result[0]["date"] == "2026-09-12"


@pytest.mark.asyncio
async def test_vk_linear_terminal_adjudication_is_bounded_when_provider_still_invalid(
    monkeypatch,
):
    calls = 0

    async def fake_gemma(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return SourceParseDecision.retry(
            SourceParseRetryReason.MALFORMED_JSON,
            evidence_manifest=EvidenceManifest.complete_source("source"),
        )

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    result = await main.parse_event_via_llm(
        "source", require_terminal_decision=True
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


@pytest.mark.asyncio
async def test_vk_linear_parse_waits_once_for_bounded_provider_retry(monkeypatch):
    monkeypatch.delenv("EVENT_PARSE_LLM", raising=False)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    calls = []
    sleeps = []

    async def fake_gemma(*_args, **_kwargs):
        calls.append(dict(_kwargs))
        if len(calls) == 1:
            raise RateLimitError(
                blocked_reason="tpm",
                retry_after_ms=3_000,
                model="gemma-4-31b-it",
                quota_scope="google:test-project",
            )
        return _decision(
            [{"title": "Лекция", "date": "2026-09-01"}],
            SourceDisposition.EVENTS_FOUND,
        )

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)

    result = await main.parse_event_via_llm(
        "Лекция 1 сентября",
        poster_texts=["Лекция 1 сентября"],
        rate_limit_max_wait_sec="5",
        require_terminal_decision=True,
    )

    # Two primary invocations (throttle + bounded retry) and the normal
    # contradiction verifier for this deliberately terse fixture.
    assert len(calls) == 3
    assert sleeps == [3.0]
    assert result.disposition is SourceDisposition.EVENTS_FOUND
    assert result.provider_attempts[0]["attempt_kind"] == "primary_rate_limit_wait"
    assert result.provider_attempts[0]["provider_retry_after_ms"] == 3_000
