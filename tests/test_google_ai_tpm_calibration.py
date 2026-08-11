from __future__ import annotations

from types import SimpleNamespace

import pytest

from google_ai.client import (
    GoogleAIClient,
    InputTokenCount,
    RequestContext,
    ReserveResult,
    TokenReservationCalibration,
    UsageInfo,
)
from google_ai.exceptions import ProviderError, RateLimitError


class _CountModel:
    def __init__(self, owner, model_name: str):
        self.owner = owner
        self.model_name = model_name

    async def count_tokens_async(self, prompt):
        self.owner.calls.append((self.model_name, prompt))
        if self.owner.error is not None:
            raise self.owner.error
        return SimpleNamespace(total_tokens=self.owner.total_tokens)


class _CountGenAI:
    def __init__(self, *, total_tokens: int = 0, error: Exception | None = None):
        self.total_tokens = total_tokens
        self.error = error
        self.calls: list[tuple[str, object]] = []

    def configure(self, api_key: str) -> None:
        self.api_key = api_key

    def GenerativeModel(self, model_name: str):
        return _CountModel(self, model_name)


class _GenerateModel:
    def __init__(self, owner, model_name: str):
        self.owner = owner
        self.model_name = model_name

    async def generate_content_async(self, prompt, generation_config=None, safety_settings=None):
        self.owner.config = dict(generation_config or {})
        return self.owner.response


class _GenerateGenAI:
    def __init__(self, response):
        self.response = response
        self.config: dict = {}

    def configure(self, api_key: str) -> None:
        self.api_key = api_key

    def GenerativeModel(self, model_name: str):
        return _GenerateModel(self, model_name)


@pytest.mark.asyncio
async def test_count_tokens_exact_and_safe_fallback_do_not_omit_prompt() -> None:
    prompt = [{"text": "Все исходные данные"}, {"text": "и весь OCR"}]
    client = GoogleAIClient()
    exact_provider = _CountGenAI(total_tokens=731)
    client._genai = exact_provider

    exact = await client.count_input_tokens_async(
        model="gemma-4-31b",
        prompt=prompt,
        api_key="test-key",
    )

    assert exact == InputTokenCount(
        tokens=731,
        source="provider_count_tokens",
        provider_model_name="models/gemma-4-31b-it",
    )
    assert exact_provider.calls == [("models/gemma-4-31b-it", prompt)]

    fallback_provider = _CountGenAI(error=TimeoutError("offline fixture"))
    client._genai = fallback_provider
    fallback = await client.count_input_tokens_async(
        model="gemma-4-31b",
        prompt=prompt,
        api_key="test-key",
    )

    assert fallback.source == "heuristic_fallback"
    assert fallback.fallback_error_type == "TimeoutError"
    assert fallback.tokens == client._estimate_prompt_tokens(prompt)
    assert fallback_provider.calls[0][1] is prompt


def test_calibration_is_scoped_persistable_and_decouples_output_ceiling() -> None:
    observations = [100] * 98 + [600, 700]
    calibration = TokenReservationCalibration.from_observations(
        model="gemma-4-31b",
        consumer="smart_update",
        prompt_version="source-parse-v7",
        output_thought_observations=observations,
        safety_margin_tokens=200,
    )
    restored = TokenReservationCalibration.from_dict(calibration.to_dict())
    client = GoogleAIClient(consumer="smart_update")
    exact_input = InputTokenCount(tokens=2_000, source="provider_count_tokens")

    assert restored == calibration
    assert calibration.observed_p99_output_thought_tokens == 600
    assert client._calculate_reserved_tpm(
        prompt="not substituted for evidence",
        max_output_tokens=8_192,
        input_token_count=exact_input,
        calibration=calibration,
        model="gemma-4-31b",
        prompt_version="source-parse-v7",
    ) == 2_800
    # Calibration changes admission only. It does not lower semantic capacity.
    assert calibration.completion_reservation(output_ceiling_tokens=8_192) == 800

    # Wrong prompt family is a conservative cold start, never a borrowed budget.
    assert client._calculate_reserved_tpm(
        prompt="full evidence",
        max_output_tokens=8_192,
        input_token_count=exact_input,
        calibration=calibration,
        model="gemma-4-31b",
        prompt_version="different-prompt",
    ) == 11_192


def test_warm_calibration_retains_tail_margin_above_output_ceiling() -> None:
    calibration = TokenReservationCalibration(
        model="gemma-4-31b",
        consumer="event_parse",
        prompt_version="source-parse-v1",
        observed_p99_output_thought_tokens=3_891,
        safety_margin_tokens=2_798,
        sample_count=502,
    )
    client = GoogleAIClient(consumer="event_parse")

    reserved = client._calculate_reserved_tpm(
        prompt="ignored when exact input is supplied",
        max_output_tokens=6_000,
        input_token_count=InputTokenCount(6_741, "provider_count_tokens"),
        calibration=calibration,
        model="gemma-4-31b",
        prompt_version="source-parse-v1",
    )

    # Read-only acute telemetry: p99 completion=3891, observed max=5689;
    # max-p99 + 1000 safety gives 2798 and a 13,430-token reservation.
    assert reserved == 13_430


def test_cold_start_reservation_remains_conservative() -> None:
    calibration = TokenReservationCalibration.from_observations(
        model="gemma-4-31b",
        consumer="smart_update",
        prompt_version="new",
        output_thought_observations=[],
        safety_margin_tokens=200,
    )
    client = GoogleAIClient(consumer="smart_update")

    assert client._calculate_reserved_tpm(
        prompt="full source",
        max_output_tokens=4_096,
        input_token_count=InputTokenCount(1_000, "provider_count_tokens"),
        calibration=calibration,
        model="gemma-4-31b",
        prompt_version="new",
    ) == 6_096


def test_usage_includes_thought_reserved_metadata_and_ratio() -> None:
    usage = UsageInfo(
        input_tokens=600,
        output_tokens=200,
        thought_tokens=200,
        total_tokens=1_000,
        reserved_tokens=1_250,
        finish_reason="STOP",
        provider_response_id="response-1",
        provider_request_id="request-1",
        provider_model_version="gemma-4-31b-it@20260801",
    )

    assert usage.actual_total_tokens == 1_000
    assert usage.reservation_actual_ratio == pytest.approx(1.25)
    assert usage.thought_tokens == 200
    assert usage.reserved_tokens == 1_250


@pytest.mark.asyncio
async def test_attempt_attaches_reserved_and_input_count_source(monkeypatch) -> None:
    response = SimpleNamespace(
        candidates=[
            SimpleNamespace(
                finish_reason="STOP",
                content=SimpleNamespace(parts=[{"text": "ok"}]),
            )
        ],
        usage_metadata=SimpleNamespace(
            prompt_token_count=400,
            candidates_token_count=80,
            thoughts_token_count=20,
            total_token_count=500,
        ),
    )
    client = GoogleAIClient()
    client._genai = _GenerateGenAI(response)
    finalized: list[UsageInfo] = []

    async def reserve(*_args, **_kwargs):
        return ReserveResult(
            ok=True,
            api_key_id="key-id",
            env_var_name="GOOGLE_API_KEY",
            quota_scope="google:project-a",
        )

    async def no_op(*_args, **_kwargs):
        return None

    async def finalize(*, usage, **_kwargs):
        finalized.append(usage)

    monkeypatch.setattr(client, "_reserve", reserve)
    monkeypatch.setattr(client, "_get_api_key", lambda _name: "test-key")
    monkeypatch.setattr(client, "_mark_sent", no_op)
    monkeypatch.setattr(client, "_finalize", finalize)
    ctx = RequestContext(
        request_uid="request-1",
        consumer="smart_update",
        account_name=None,
        model="gemma-4-31b",
        reserved_tpm=900,
        requested_model="gemma-4-31b",
        input_count_source="provider_count_tokens",
    )

    _text, usage = await client._attempt_generate(
        ctx=ctx,
        attempt_no=1,
        prompt="full evidence",
        generation_config={},
        safety_settings=None,
        max_output_tokens=8_192,
        candidate_key_ids=None,
    )

    assert usage.reserved_tokens == 900
    assert usage.input_count_source == "provider_count_tokens"
    assert usage.thought_tokens == 20
    assert finalized == [usage]


@pytest.mark.asyncio
async def test_provider_metadata_is_exposed_and_max_tokens_is_typed_non_success() -> None:
    response = SimpleNamespace(
        response_id="response-7",
        request_id="request-9",
        model_version="gemma-4-31b-it@stable",
        candidates=[
            SimpleNamespace(
                finish_reason="MAX_TOKENS",
                content=SimpleNamespace(parts=[{"text": '{"events":[1]}'}]),
            )
        ],
        usage_metadata=SimpleNamespace(
            prompt_token_count=1_000,
            candidates_token_count=300,
            thoughts_token_count=200,
            total_token_count=1_500,
        ),
    )
    client = GoogleAIClient()
    client._genai = _GenerateGenAI(response)

    with pytest.raises(ProviderError) as exc_info:
        await client._call_provider(
            api_key="test-key",
            model="gemma-4-31b",
            prompt="complete evidence",
            generation_config={},
            safety_settings=None,
            max_output_tokens=8_192,
        )

    error = exc_info.value
    assert error.error_type == "output_truncated"
    assert error.finish_reason == "MAX_TOKENS"
    assert error.provider_response_id == "response-7"
    assert error.provider_request_id == "request-9"
    assert error.provider_model_version == "gemma-4-31b-it@stable"
    assert error.usage.thought_tokens == 200
    # The semantic output ceiling sent to the provider remains untouched.
    assert client._genai.config["max_output_tokens"] == 8_192


@pytest.mark.asyncio
async def test_success_exposes_finish_reason_response_request_and_model_version() -> None:
    response = SimpleNamespace(
        response_id="response-ok",
        sdk_http_response=SimpleNamespace(
            headers={"x-goog-request-id": "request-ok"}
        ),
        model_version="gemma-4-31b-it@stable",
        candidates=[
            SimpleNamespace(
                finish_reason="STOP",
                content=SimpleNamespace(parts=[{"text": '{"events":[]}'}]),
            )
        ],
        usage_metadata={
            "prompt_token_count": 400,
            "candidates_token_count": 80,
            "thoughts_token_count": 20,
            "total_token_count": 500,
        },
    )
    client = GoogleAIClient()
    client._genai = _GenerateGenAI(response)

    text, usage = await client._call_provider(
        api_key="test-key",
        model="gemma-4-31b",
        prompt="complete evidence",
        generation_config={},
        safety_settings=None,
        max_output_tokens=4_096,
    )

    assert text == '{"events":[]}'
    assert usage.finish_reason == "STOP"
    assert usage.provider_response_id == "response-ok"
    assert usage.provider_request_id == "request-ok"
    assert usage.provider_model_version == "gemma-4-31b-it@stable"
    assert usage.thought_tokens == 20


@pytest.mark.asyncio
async def test_unknown_finish_reason_is_typed_invalid_response() -> None:
    response = SimpleNamespace(
        candidates=[
            SimpleNamespace(
                finish_reason="BROKEN_ENUM_VALUE",
                content=SimpleNamespace(parts=[{"text": "partial"}]),
            )
        ],
        usage_metadata={},
    )
    client = GoogleAIClient()
    client._genai = _GenerateGenAI(response)

    with pytest.raises(ProviderError) as exc_info:
        await client._call_provider(
            api_key="test-key",
            model="gemma-4-31b",
            prompt="complete evidence",
            generation_config={},
            safety_settings=None,
            max_output_tokens=1_024,
        )

    assert exc_info.value.error_type == "invalid_finish_reason"
    assert exc_info.value.finish_reason == "BROKEN_ENUM_VALUE"


def test_rate_limit_quota_bucket_is_project_model_not_key_and_keeps_reset() -> None:
    first_key = RateLimitError(
        blocked_reason="rpd",
        retry_after_ms=3_600_000,
        model="gemma-4-31b",
        api_key_id="key-a",
        quota_scope="google:shared-project",
        quota_reason="RPD_EXHAUSTED",
    )
    second_key = RateLimitError(
        blocked_reason="rpd",
        retry_after_ms=3_600_000,
        model="gemma-4-31b",
        api_key_id="key-b",
        quota_scope="google:shared-project",
        quota_reason="RPD_EXHAUSTED",
    )

    assert first_key.quota_bucket == second_key.quota_bucket
    assert first_key.quota_bucket == "google:shared-project:gemma-4-31b"
    assert first_key.retry_after_ms == 3_600_000
    assert first_key.quota_reason == "RPD_EXHAUSTED"


def test_provider_rate_limit_classification_keeps_reason_and_retry_after() -> None:
    client = GoogleAIClient()

    error = client._classify_error(
        RuntimeError("429 RESOURCE_EXHAUSTED: Please retry in 12.5s")
    )

    assert error.status_code == 429
    assert error.retryable is True
    assert error.retry_after_ms == 12_500
    assert error.quota_reason == "RESOURCE_EXHAUSTED"
