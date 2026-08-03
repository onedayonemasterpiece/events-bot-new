from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from google_ai.client import (
    _DEFAULT_ENV_CANDIDATE_CACHE,
    _NORMAL_POOL_CURSOR,
    _NORMAL_POOL_ENV_CANDIDATE_CACHE,
    _OVERFLOW_ENV_CANDIDATE_CACHE,
    GoogleAIClient,
    ExternalCallLease,
    RequestContext,
    ReserveResult,
    UsageInfo,
)
from google_ai.exceptions import ProviderError, RateLimitError, ReservationError

_ATOMIC_LIMITER_CONTRACT = GoogleAIClient.REQUIRED_LIMITER_CONTRACT
_ROLLING_BUCKET_STRATEGY = GoogleAIClient.REQUIRED_BUCKET_STRATEGY


class _FakeModel:
    def __init__(self, owner: "_FakeGenAI", model_name: str):
        self.owner = owner
        self.model_name = model_name

    async def generate_content_async(self, prompt, generation_config=None, safety_settings=None):
        self.owner.calls.append(
            {
                "model_name": self.model_name,
                "prompt": prompt,
                "generation_config": dict(generation_config or {}),
                "safety_settings": safety_settings,
            }
        )
        return self.owner.response


class _FakeGenAI:
    def __init__(self, response):
        self.response = response
        self.calls: list[dict] = []
        self.configured_key: str | None = None

    def configure(self, api_key: str) -> None:
        self.configured_key = api_key

    def GenerativeModel(self, model_name: str):
        return _FakeModel(self, model_name)


class _FakeSupabaseQuery:
    def __init__(self, data=None):
        self.data = data or []

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self.data)


class _FakeSupabaseClient:
    def __init__(self, data=None, reserve_data=None):
        self.data = data or []
        self.reserve_data = reserve_data
        self.rpc_calls: list[dict] = []

    def table(self, _name: str):
        return _FakeSupabaseQuery(self.data)

    def rpc(self, _name: str, payload: dict):
        self.rpc_calls.append(dict(payload))
        data = self.reserve_data or {
            "ok": True,
            "env_var_name": "GOOGLE_API_KEY2",
            "key_alias": "unexpected-unscoped-key",
            "quota_scope": "google:test-project",
            "limiter_contract": _ATOMIC_LIMITER_CONTRACT,
            "bucket_strategy": _ROLLING_BUCKET_STRATEGY,
        }
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=data))


class _StrictExternalSupabase:
    def __init__(
        self,
        rows,
        *,
        limiter_contract=_ATOMIC_LIMITER_CONTRACT,
        bucket_strategy=_ROLLING_BUCKET_STRATEGY,
    ):
        self.rows = rows
        self.limiter_contract = limiter_contract
        self.bucket_strategy = bucket_strategy
        self.rpc_calls: list[tuple[str, dict]] = []

    def table(self, _name: str):
        return _FakeSupabaseQuery(self.rows)

    def rpc(self, name: str, payload: dict):
        self.rpc_calls.append((name, dict(payload)))
        if name == "google_ai_reserve":
            selected = payload["p_candidate_key_ids"][0]
            row = next(row for row in self.rows if row["id"] == selected)
            data = {
                "ok": True,
                "api_key_id": selected,
                "env_var_name": row["env_var_name"],
                "key_alias": row.get("key_alias", "fixture"),
                "minute_bucket": "2026-07-31T00:00:00Z",
                "day_bucket": "2026-07-31",
                "quota_scope": "google:test-project",
                "limiter_contract": self.limiter_contract,
                "bucket_strategy": self.bucket_strategy,
            }
        else:
            data = None
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=data))


@pytest.mark.asyncio
async def test_gemma4_keeps_native_json_config_and_filters_thought_parts():
    response = SimpleNamespace(
        candidates=[
            SimpleNamespace(
                content=SimpleNamespace(
                    parts=[
                        {"text": '{"hidden":"thought"}', "thought": True},
                        {"text": '{"ok":true}'},
                    ]
                )
            )
        ],
        usage_metadata={},
    )
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient()
    client._genai = fake_genai

    text, _usage = await client._call_provider(
        api_key="test-key",
        model="gemma-4-31b",
        prompt="hello",
        generation_config={
            "temperature": 0,
            "response_mime_type": "application/json",
            "response_schema": {"type": "object"},
            "response_schema_name": "ignored_name",
        },
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.configured_key == "test-key"
    assert fake_genai.calls[0]["model_name"] == "models/gemma-4-31b-it"
    assert fake_genai.calls[0]["generation_config"]["response_mime_type"] == "application/json"
    assert fake_genai.calls[0]["generation_config"]["response_schema"] == {"type": "object"}
    assert fake_genai.calls[0]["generation_config"]["thinking_config"] == {
        "thinking_level": "minimal"
    }
    assert "response_schema_name" not in fake_genai.calls[0]["generation_config"]


@pytest.mark.asyncio
async def test_gemma4_preserves_explicit_thinking_choice():
    response = SimpleNamespace(text='{"ok":true}', usage_metadata={})
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient()
    client._genai = fake_genai

    await client._call_provider(
        api_key="test-key",
        model="gemma-4-31b",
        prompt="hello",
        generation_config={"thinking_config": {"thinking_level": "high"}},
        safety_settings=None,
        max_output_tokens=None,
    )

    assert fake_genai.calls[0]["generation_config"]["thinking_config"] == {
        "thinking_level": "high"
    }


@pytest.mark.asyncio
async def test_thought_only_response_raises_instead_of_leaking_sdk_repr():
    # Regression for INC-2026-05-17: the model emitted only a thought-channel part
    # (and ran out of the output-token budget before producing an answer). The
    # extractor must NOT stringify the raw SDK response as a last resort — that is
    # exactly how the GenerateContentResponse repr leaked into public VK/Telegraph
    # posts. It must raise empty_response so the caller falls back / retries.
    class _Resp:
        candidates = [
            SimpleNamespace(
                content=SimpleNamespace(
                    parts=[{"text": "* Task: Edit/Rewrite ...", "thought": True}]
                ),
                finish_reason="MAX_TOKENS",
            )
        ]
        usage_metadata = SimpleNamespace(
            prompt_token_count=2562,
            candidates_token_count=0,
            total_token_count=4459,
            thoughts_token_count=1897,
        )

        def __repr__(self) -> str:
            return (
                "sdk_http_response=HttpResponse(headers=) candidates=[Candidate("
                "content=Content(parts=[Part(text=\"...\", thought=True)]))]"
            )

    fake_genai = _FakeGenAI(_Resp())
    client = GoogleAIClient()
    client._genai = fake_genai

    with pytest.raises(ProviderError) as exc_info:
        await client._call_provider(
            api_key="test-key",
            model="gemma-4-31b",
            prompt="hello",
            generation_config={"temperature": 0},
            safety_settings=None,
            max_output_tokens=64,
        )

    err = exc_info.value
    assert getattr(err, "error_type", "") == "empty_response"
    # The raw SDK repr must never be surfaced as model output or in the error.
    assert "sdk_http_response" not in str(err)
    assert "HttpResponse" not in str(err)


@pytest.mark.asyncio
async def test_gemma3_still_strips_native_json_config():
    response = SimpleNamespace(text='{"ok":true}', usage_metadata={})
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient()
    client._genai = fake_genai

    text, _usage = await client._call_provider(
        api_key="test-key",
        model="gemma-3-27b",
        prompt="hello",
        generation_config={
            "temperature": 0,
            "response_mime_type": "application/json",
            "response_schema": {"type": "object"},
            "response_schema_name": "legacy_name",
        },
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.calls[0]["model_name"] == "models/gemma-3-27b-it"
    assert fake_genai.calls[0]["generation_config"] == {"temperature": 0}


def test_requested_gemma_model_stays_first_in_model_chain():
    client = GoogleAIClient()
    client.fallback_models = ["gemma-3-27b", "gemma-4-26b-a4b"]

    assert client._build_model_chain("gemma-4-31b") == [
        "gemma-4-31b",
        "gemma-3-27b",
        "gemma-4-26b-a4b",
    ]


@pytest.mark.asyncio
async def test_per_call_single_send_disables_retry_and_model_fallback(monkeypatch):
    client = GoogleAIClient()
    client.max_retries = 3
    client.fallback_models = ["gemini-3.1-flash-lite"]
    calls: list[str] = []
    observed: list[dict] = []

    async def fake_attempt_generate(*, ctx, attempt_no, attempt_observer, **_kwargs):
        calls.append(ctx.model)
        attempt_observer(
            {
                "attempt_no": attempt_no,
                "requested_model": ctx.requested_model,
                "provider_model_name": ctx.provider_model_name,
            }
        )
        raise ProviderError(
            error_type="ServerError",
            error_message="forced failure",
            retryable=True,
            status_code=503,
        )

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)
    with pytest.raises(ProviderError):
        await client.generate_content_async(
            model="gemma-4-31b-it",
            prompt="bounded",
            max_output_tokens=1000,
            allow_model_fallback=False,
            max_provider_attempts=1,
            attempt_observer=observed.append,
        )

    assert calls == ["gemma-4-31b"]
    assert len(observed) == 1
    assert observed[0]["provider_model_name"] == "models/gemma-4-31b-it"


@pytest.mark.asyncio
async def test_quota_block_falls_through_to_next_model(monkeypatch):
    client = GoogleAIClient()
    client.fallback_models = ["gemini-3.1-flash-lite"]
    calls: list[str] = []
    events: list[tuple[str, dict]] = []

    async def fake_attempt_generate(*, ctx, **_kwargs):
        calls.append(ctx.model)
        if ctx.model == "gemini-3.5-flash-lite":
            raise RateLimitError(blocked_reason="rpd", model=ctx.model)
        return "fallback-ok", SimpleNamespace(total_tokens=1)

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)
    monkeypatch.setattr(
        client,
        "_log_event",
        lambda event, _ctx, **payload: events.append((event, payload)),
    )

    text, usage = await client.generate_content_async(
        model="gemini-3.5-flash-lite",
        prompt="bounded",
        max_output_tokens=16,
    )

    assert text == "fallback-ok"
    assert usage.model == "gemini-3.1-flash-lite"
    assert calls == ["gemini-3.5-flash-lite", "gemini-3.1-flash-lite"]
    fallback = next(payload for event, payload in events if event == "google_ai.model_quota_fallback")
    assert fallback["blocked_reason"] == "rpd"
    assert fallback["next_model"] == "gemini-3.1-flash-lite"


@pytest.mark.asyncio
async def test_non_quota_reservation_block_does_not_change_model(monkeypatch):
    client = GoogleAIClient()
    client.fallback_models = ["gemini-3.1-flash-lite"]
    calls: list[str] = []

    async def fake_attempt_generate(*, ctx, **_kwargs):
        calls.append(ctx.model)
        raise RateLimitError(blocked_reason="model_not_found", model=ctx.model)

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)

    with pytest.raises(RateLimitError):
        await client.generate_content_async(
            model="gemini-3.5-flash-lite",
            prompt="bounded",
            max_output_tokens=16,
        )

    assert calls == ["gemini-3.5-flash-lite"]


@pytest.mark.asyncio
async def test_no_supabase_fails_closed_by_default(monkeypatch):
    monkeypatch.delenv("GOOGLE_AI_ALLOW_RESERVE_FALLBACK", raising=False)
    monkeypatch.delenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", raising=False)
    client = GoogleAIClient(supabase_client=None, consumer="video_partner_filter")
    ctx = RequestContext(
        request_uid="req-shared-limiter-required",
        consumer="video_partner_filter",
        account_name=None,
        model="gemma-4-31b",
        requested_model="gemma-4-31b-it",
        reserved_tpm=100,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.env_var_name is None
    assert reserve.key_alias is None
    assert reserve.blocked_reason == "shared_limiter_unavailable"


@pytest.mark.asyncio
async def test_no_supabase_never_becomes_direct_key_fallback(monkeypatch):
    monkeypatch.setenv("GOOGLE_AI_ALLOW_RESERVE_FALLBACK", "1")
    monkeypatch.setenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "0")
    client = GoogleAIClient(supabase_client=None, consumer="parallel_agent")
    ctx = RequestContext(
        request_uid="req-no-direct-key",
        consumer="parallel_agent",
        account_name=None,
        model="gemini-3.1-flash-lite",
        requested_model="gemini-3.1-flash-lite",
        reserved_tpm=100,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.env_var_name is None
    assert reserve.blocked_reason == "shared_limiter_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limiter_contract", "blocked_reason"),
    [
        (None, "limiter_contract_missing"),
        ("google_ai_key_model_atomic_v0", "limiter_contract_incompatible"),
    ],
)
async def test_reserve_success_fails_closed_without_required_atomic_contract(
    monkeypatch: pytest.MonkeyPatch,
    limiter_contract: str | None,
    blocked_reason: str,
) -> None:
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    monkeypatch.setenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "0")
    supabase = _FakeSupabaseClient(
        data=[
            {
                "id": "key-a",
                "env_var_name": "GOOGLE_API_KEY",
                "priority": 1,
            }
        ],
        reserve_data={
            "ok": True,
            "api_key_id": "key-a",
            "env_var_name": "GOOGLE_API_KEY",
            "quota_scope": "google:test-project",
            "limiter_contract": limiter_contract,
            "bucket_strategy": _ROLLING_BUCKET_STRATEGY,
        },
    )
    client = GoogleAIClient(supabase_client=supabase, consumer="parallel_agent")
    ctx = RequestContext(
        request_uid="req-contract-gate",
        consumer="parallel_agent",
        account_name=None,
        model="gemma-4-31b",
        reserved_tpm=100,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.blocked_reason == blocked_reason
    assert reserve.api_key_id is None
    assert reserve.env_var_name is None
    assert reserve.limiter_contract == limiter_contract


@pytest.mark.asyncio
async def test_reserve_accepts_versioned_project_scope_atomic_contract() -> None:
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    supabase = _FakeSupabaseClient(
        data=[
            {
                "id": "key-a",
                "env_var_name": "GOOGLE_API_KEY",
                "priority": 1,
            }
        ],
        reserve_data={
            "ok": True,
            "api_key_id": "key-a",
            "env_var_name": "GOOGLE_API_KEY",
            "quota_scope": "google:cloud-project-a",
            "limiter_contract": _ATOMIC_LIMITER_CONTRACT,
            "bucket_strategy": _ROLLING_BUCKET_STRATEGY,
        },
    )
    client = GoogleAIClient(supabase_client=supabase, consumer="parallel_agent")
    ctx = RequestContext(
        request_uid="req-contract-ok",
        consumer="parallel_agent",
        account_name=None,
        model="gemma-4-31b",
        reserved_tpm=100,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is True
    assert reserve.api_key_id == "key-a"
    assert reserve.quota_scope == "google:cloud-project-a"
    assert reserve.limiter_contract == _ATOMIC_LIMITER_CONTRACT
    assert reserve.bucket_strategy == _ROLLING_BUCKET_STRATEGY


@pytest.mark.parametrize(
    ("bucket_strategy", "blocked_reason"),
    [
        (None, "limiter_bucket_strategy_missing"),
        ("fixed_minute_utc_day_v1", "limiter_bucket_strategy_incompatible"),
    ],
)
def test_reserve_success_fails_closed_without_required_bucket_strategy(
    bucket_strategy: str | None,
    blocked_reason: str,
) -> None:
    reserve = GoogleAIClient._reserve_result_from_data(
        {
            "ok": True,
            "api_key_id": "key-a",
            "env_var_name": "GOOGLE_API_KEY",
            "quota_scope": "google:test-project",
            "limiter_contract": _ATOMIC_LIMITER_CONTRACT,
            "bucket_strategy": bucket_strategy,
        }
    )

    assert reserve.ok is False
    assert reserve.blocked_reason == blocked_reason
    assert reserve.api_key_id is None
    assert reserve.env_var_name is None
    assert reserve.bucket_strategy == bucket_strategy


def test_reserve_rejects_contract_response_without_quota_scope() -> None:
    reserve = GoogleAIClient._reserve_result_from_data(
        {
            "ok": True,
            "api_key_id": "key-a",
            "env_var_name": "GOOGLE_API_KEY",
            "limiter_contract": _ATOMIC_LIMITER_CONTRACT,
            "bucket_strategy": _ROLLING_BUCKET_STRATEGY,
        }
    )

    assert reserve.ok is False
    assert reserve.blocked_reason == "limiter_quota_scope_missing"
    assert reserve.api_key_id is None
    assert reserve.env_var_name is None


@pytest.mark.asyncio
async def test_no_supabase_local_limiter_requires_explicit_opt_in(monkeypatch):
    monkeypatch.delenv("GOOGLE_AI_LOCAL_RPM", raising=False)
    monkeypatch.setenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "1")
    GoogleAIClient._local_limiter_minute_bucket = None
    GoogleAIClient._local_limiter_used_rpm = 0
    GoogleAIClient._local_limiter_used_tpm = 0
    GoogleAIClient._local_limiter_day_bucket = None
    GoogleAIClient._local_limiter_used_rpd = 0
    client = GoogleAIClient(supabase_client=None, consumer="video_partner_filter")
    ctx = RequestContext(
        request_uid="req-local-rpm",
        consumer="video_partner_filter",
        account_name=None,
        model="gemma-4-31b",
        requested_model="gemma-4-31b-it",
        reserved_tpm=100,
    )

    ok_reserves = [
        await client._reserve(ctx, attempt_no=i + 1, candidate_key_ids=None)
        for i in range(15)
    ]
    blocked = await client._reserve(ctx, attempt_no=16, candidate_key_ids=None)

    assert all(reserve.ok for reserve in ok_reserves)
    assert ok_reserves[-1].key_alias == "local-fallback-no-supabase"
    assert ok_reserves[-1].used_after["rpm"] == 15
    assert blocked.ok is False
    assert blocked.blocked_reason == "rpm"


def test_fly_runtime_disables_cross_process_limiter_fallbacks() -> None:
    fly_config = (Path(__file__).resolve().parents[1] / "fly.toml").read_text(
        encoding="utf-8"
    )

    assert 'GOOGLE_AI_ALLOW_RESERVE_FALLBACK = "0"' in fly_config
    assert 'GOOGLE_AI_LOCAL_LIMITER_FALLBACK = "0"' in fly_config
    assert 'GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR = "0"' in fly_config
    assert 'ENABLE_EVENT_VECTOR_SYNC = "1"' in fly_config
    assert 'STATIC_SITE_REQUIRE_VECTOR_BARRIER = "1"' in fly_config


@pytest.mark.asyncio
async def test_multimodal_prompt_passthrough_and_key3_alias(monkeypatch: pytest.MonkeyPatch):
    response = SimpleNamespace(text='{"ok":true}', usage_metadata={})
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient(default_env_var_name="GOOGLE_API_KEY3")
    client._genai = fake_genai
    monkeypatch.setenv("GOOGLE_API_KEY_3", "aliased-key")

    prompt_parts = ["hello", {"image": "placeholder"}]
    text, _usage = await client._call_provider(
        api_key=client._get_api_key(None) or "",
        model="models/gemma-4-31b-it",
        prompt=prompt_parts,
        generation_config={"temperature": 0},
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.configured_key == "aliased-key"
    assert fake_genai.calls[0]["prompt"] == prompt_parts


@pytest.mark.asyncio
async def test_multimodal_prompt_parts_are_forwarded_to_provider() -> None:
    response = SimpleNamespace(text='{"ok":true}', usage_metadata={})
    fake_genai = _FakeGenAI(response)
    client = GoogleAIClient()
    client._genai = fake_genai
    prompt = [
        {"text": "Extract poster facts"},
        {"inline_data": {"mime_type": "image/jpeg", "data": b"\xff\xd8\xfftest"}},
    ]

    text, _usage = await client._call_provider(
        api_key="test-key",
        model="gemma-4-31b",
        prompt=prompt,
        generation_config={"temperature": 0},
        safety_settings=None,
        max_output_tokens=None,
    )

    assert text == '{"ok":true}'
    assert fake_genai.calls[0]["prompt"] == prompt


def test_multimodal_prompt_estimate_ignores_raw_blob_bytes_and_counts_image_overhead() -> None:
    client = GoogleAIClient()
    prompt = [
        {"text": "Extract poster facts"},
        {"inline_data": {"mime_type": "image/jpeg", "data": b"\xff\xd8\xfftest"}},
    ]

    prompt_text, blob_count = client._prompt_estimate_components(prompt)

    assert prompt_text == "Extract poster facts"
    assert blob_count == 1
    assert client._estimate_prompt_tokens(prompt) >= client.DEFAULT_MULTIMODAL_IMAGE_TOKENS


@pytest.mark.asyncio
async def test_missing_scoped_env_key_uses_local_default_env_limiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    monkeypatch.setenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "1")
    client = GoogleAIClient(
        supabase_client=_FakeSupabaseClient(data=[]),
        consumer="kaggle",
        default_env_var_name="GOOGLE_API_KEY3",
    )
    ctx = RequestContext(
        request_uid="req-1",
        consumer="kaggle",
        account_name=None,
        model="gemma-4-31b",
        requested_model="models/gemma-4-31b-it",
        reserved_tpm=123,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is True
    assert reserve.env_var_name == "GOOGLE_API_KEY3"
    assert reserve.key_alias == "local-fallback-default-env-missing"
    assert reserve.blocked_reason == "default_env_candidates_missing"


@pytest.mark.asyncio
async def test_missing_scoped_env_key_cache_stays_local_not_unscoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    monkeypatch.setenv("GOOGLE_AI_LOCAL_LIMITER_FALLBACK", "1")
    supabase = _FakeSupabaseClient(data=[])
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="kaggle",
        default_env_var_name="GOOGLE_API_KEY3",
    )
    ctx = RequestContext(
        request_uid="req-cache",
        consumer="kaggle",
        account_name=None,
        model="gemma-4-31b",
        requested_model="models/gemma-4-31b-it",
        reserved_tpm=123,
    )

    first = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)
    second = await client._reserve(ctx, attempt_no=2, candidate_key_ids=None)

    assert first.env_var_name == "GOOGLE_API_KEY3"
    assert second.env_var_name == "GOOGLE_API_KEY3"
    assert first.blocked_reason == "default_env_candidates_missing"
    assert second.blocked_reason == "default_env_candidates_missing"
    assert supabase.rpc_calls == []


@pytest.mark.asyncio
async def test_explicit_no_fallback_fails_closed_without_supabase() -> None:
    client = GoogleAIClient(
        supabase_client=None,
        consumer="region_talk_candidate_report",
        default_env_var_name="GOOGLE_API_KEY3",
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    ctx = RequestContext(
        request_uid="region-talk-no-supabase",
        consumer="region_talk_candidate_report",
        account_name=None,
        model="gemini-3.1-flash-lite",
        requested_model="gemini-3.1-flash-lite",
        reserved_tpm=123,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "shared_limiter_unavailable"
    assert reserve.env_var_name is None


@pytest.mark.asyncio
async def test_explicit_no_fallback_fails_closed_without_scoped_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    supabase = _FakeSupabaseClient(data=[])
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="region_talk_image_visual_adjudicator",
        default_env_var_name="GOOGLE_API_KEY3",
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    ctx = RequestContext(
        request_uid="region-talk-no-scoped-key",
        consumer="region_talk_image_visual_adjudicator",
        account_name=None,
        model="gemini-3.1-flash-lite",
        requested_model="gemini-3.1-flash-lite",
        reserved_tpm=123,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "default_env_candidates_missing"
    assert reserve.env_var_name is None
    assert supabase.rpc_calls == []


@pytest.mark.asyncio
async def test_explicit_no_fallback_fails_closed_for_cached_missing_rpc() -> None:
    client = GoogleAIClient(
        supabase_client=_FakeSupabaseClient(),
        consumer="region_talk_external_research",
        default_env_var_name="GOOGLE_API_KEY3",
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client._reserve_rpc_missing = True
    client._reserve_rpc_missing_since = float("inf")
    ctx = RequestContext(
        request_uid="region-talk-no-rpc",
        consumer="region_talk_external_research",
        account_name=None,
        model="gemini-3.1-flash-lite",
        requested_model="gemini-3.1-flash-lite",
        reserved_tpm=123,
    )

    reserve = await client._reserve(ctx, attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "reserve_rpc_missing"
    assert reserve.env_var_name is None


@pytest.mark.asyncio
async def test_provider_call_timeout_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    class _SlowModel:
        async def generate_content_async(self, *_args, **_kwargs):
            await asyncio.sleep(1)
            return SimpleNamespace(text='{"late":true}', usage_metadata={})

    class _SlowGenAI:
        def configure(self, api_key: str) -> None:
            pass

        def GenerativeModel(self, _model_name: str):
            return _SlowModel()

    monkeypatch.setenv("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", "0.01")
    client = GoogleAIClient()
    client._genai = _SlowGenAI()

    with pytest.raises(TimeoutError, match="timed out"):
        await client._call_provider(
            api_key="test-key",
            model="models/gemma-4-31b-it",
            prompt="hello",
            generation_config={"temperature": 0},
            safety_settings=None,
            max_output_tokens=None,
        )


@pytest.mark.asyncio
async def test_hard_single_attempt_configures_new_sdk_without_internal_retries() -> None:
    class _Models:
        async def generate_content(self, **_kwargs):
            return SimpleNamespace(text='{"ok":true}', usage_metadata={})

    class _NewSdk:
        def __init__(self):
            self.client_kwargs = None

        def Client(self, **kwargs):
            self.client_kwargs = kwargs
            return SimpleNamespace(aio=SimpleNamespace(models=_Models()))

    new_sdk = _NewSdk()
    client = GoogleAIClient()
    client._genai = None
    client._genai_new = new_sdk
    client.hard_single_provider_attempt = True

    text, _usage = await client._call_provider(
        api_key="test-key",
        model="gemini-3.1-flash-lite",
        prompt="one HTTP attempt",
        generation_config={"temperature": 0},
        safety_settings=None,
        max_output_tokens=16,
    )

    assert text == '{"ok":true}'
    assert new_sdk.client_kwargs == {
        "api_key": "test-key",
        "http_options": {"retry_options": {"attempts": 1}},
    }


@pytest.mark.asyncio
async def test_hard_single_attempt_forbids_legacy_sdk_fallback() -> None:
    class _LegacySdk:
        def __init__(self):
            self.configure_calls = 0

        def configure(self, **_kwargs):
            self.configure_calls += 1

    legacy = _LegacySdk()
    client = GoogleAIClient()
    client._genai = legacy
    client.hard_single_provider_attempt = True

    with pytest.raises(ProviderError, match="legacy SDK fallback is disabled"):
        await client._call_provider(
            api_key="test-key",
            model="gemini-3.1-flash-lite",
            prompt="no legacy retries",
            generation_config={"temperature": 0},
            safety_settings=None,
            max_output_tokens=16,
        )

    assert legacy.configure_calls == 0


# --- Emergency reserve overflow (INC-2026-06-03) -------------------------------

_OVERFLOW_KEY_ROWS = [
    {"id": "id-key1", "env_var_name": "GOOGLE_API_KEY", "priority": 10},
    {"id": "id-key3", "env_var_name": "GOOGLE_API_KEY3", "priority": 3},
    {"id": "id-key2", "env_var_name": "GOOGLE_API_KEY2", "priority": 5},
]
_SPARE_KEY_IDS = {"id-key3", "id-key2"}


class _OverflowFakeSupabase:
    """Reserve RPC that blocks the scoped lane and frees up once a spare joins."""

    def __init__(self, *, scoped_block: str = "rpd", spare_ok: bool = True):
        self.scoped_block = scoped_block
        self.spare_ok = spare_ok
        self.rpc_calls: list[dict] = []

    def table(self, _name: str):
        return _FakeSupabaseQuery(_OVERFLOW_KEY_ROWS)

    def rpc(self, _name: str, payload: dict):
        self.rpc_calls.append(dict(payload))
        candidates = payload.get("p_candidate_key_ids") or []
        has_spare = any(c in _SPARE_KEY_IDS for c in candidates)
        if has_spare and self.spare_ok:
            data = {
                "ok": True,
                "env_var_name": "GOOGLE_API_KEY3",
                "key_alias": "k3",
                "api_key_id": "id-key3",
                "quota_scope": "google:test-project",
                "limiter_contract": _ATOMIC_LIMITER_CONTRACT,
                "bucket_strategy": _ROLLING_BUCKET_STRATEGY,
            }
        elif has_spare:  # spare present but still exhausted
            data = {"ok": False, "blocked_reason": "rpd", "api_key_id": None}
        else:
            data = {
                "ok": False,
                "blocked_reason": self.scoped_block,
                "api_key_id": None,
                "retry_after_ms": 1000 if self.scoped_block in {"rpm", "tpm"} else None,
            }
        return SimpleNamespace(execute=lambda d=data: SimpleNamespace(data=d))


def _overflow_client(supabase, monkeypatch, **kwargs):
    _DEFAULT_ENV_CANDIDATE_CACHE.clear()
    _OVERFLOW_ENV_CANDIDATE_CACHE.clear()
    monkeypatch.setenv("GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV", "1")
    return GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update",
        default_env_var_name="GOOGLE_API_KEY",
        **kwargs,
    )


def _overflow_ctx(uid: str = "req-of") -> RequestContext:
    return RequestContext(
        request_uid=uid,
        consumer="smart_update",
        account_name=None,
        model="gemini-3.1-flash-lite",
        requested_model="gemini-3.1-flash-lite",
        reserved_tpm=100,
    )


@pytest.mark.asyncio
async def test_reserve_overflow_borrows_spare_key_on_rpd(monkeypatch: pytest.MonkeyPatch) -> None:
    supabase = _OverflowFakeSupabase(scoped_block="rpd", spare_ok=True)
    client = _overflow_client(
        supabase, monkeypatch, reserve_overflow_key_envs="GOOGLE_API_KEY3,GOOGLE_API_KEY2"
    )

    reserve = await client._reserve(_overflow_ctx(), attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is True
    assert reserve.env_var_name == "GOOGLE_API_KEY3"
    # Phase 1 = scoped lane only; phase 2 = scoped + spares (scoped key first).
    assert len(supabase.rpc_calls) == 2
    assert supabase.rpc_calls[0]["p_candidate_key_ids"] == ["id-key1"]
    assert supabase.rpc_calls[1]["p_candidate_key_ids"] == ["id-key1", "id-key3", "id-key2"]
    # Same request/attempt reused across phases (no idempotency conflict).
    assert supabase.rpc_calls[0]["p_request_uid"] == supabase.rpc_calls[1]["p_request_uid"]
    assert supabase.rpc_calls[0]["p_attempt_no"] == supabase.rpc_calls[1]["p_attempt_no"]


@pytest.mark.asyncio
async def test_reserve_overflow_success_does_not_notify_incident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incidents: list[tuple[str, dict]] = []
    supabase = _OverflowFakeSupabase(scoped_block="rpd", spare_ok=True)
    client = _overflow_client(
        supabase,
        monkeypatch,
        reserve_overflow_key_envs="GOOGLE_API_KEY3,GOOGLE_API_KEY2",
        incident_notifier=lambda kind, payload: incidents.append((kind, payload)),
    )

    reserve = await client._reserve(
        _overflow_ctx("req-overflow-no-alert"),
        attempt_no=1,
        candidate_key_ids=None,
    )

    assert reserve.ok is True
    assert reserve.env_var_name == "GOOGLE_API_KEY3"
    assert incidents == []


@pytest.mark.asyncio
async def test_reserve_no_overflow_when_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    supabase = _OverflowFakeSupabase(scoped_block="rpd", spare_ok=True)
    client = _overflow_client(supabase, monkeypatch)  # no overflow envs

    reserve = await client._reserve(_overflow_ctx("req-noof"), attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "rpd"
    assert len(supabase.rpc_calls) == 1  # never expands


@pytest.mark.asyncio
async def test_reserve_does_not_expand_explicit_candidate_scope_to_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supabase = _OverflowFakeSupabase(scoped_block="rpd", spare_ok=True)
    client = _overflow_client(
        supabase,
        monkeypatch,
        reserve_overflow_key_envs="GOOGLE_API_KEY3,GOOGLE_API_KEY2",
    )

    reserve = await client._reserve(
        _overflow_ctx("req-explicit-no-widen"),
        attempt_no=1,
        candidate_key_ids=["id-key1"],
    )

    assert reserve.ok is False
    assert reserve.blocked_reason == "rpd"
    assert [call["p_candidate_key_ids"] for call in supabase.rpc_calls] == [["id-key1"]]


@pytest.mark.asyncio
async def test_reserve_overflow_not_triggered_on_per_minute_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supabase = _OverflowFakeSupabase(scoped_block="rpm", spare_ok=True)
    client = _overflow_client(
        supabase, monkeypatch, reserve_overflow_key_envs="GOOGLE_API_KEY3,GOOGLE_API_KEY2"
    )

    reserve = await client._reserve(_overflow_ctx("req-rpm"), attempt_no=1, candidate_key_ids=None)

    # rpm is a per-minute spike: stay on the scoped key, do not borrow spares.
    assert reserve.ok is False
    assert reserve.blocked_reason == "rpm"
    assert len(supabase.rpc_calls) == 1


@pytest.mark.asyncio
async def test_reserve_overflow_returns_block_when_spares_also_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supabase = _OverflowFakeSupabase(scoped_block="rpd", spare_ok=False)
    client = _overflow_client(
        supabase, monkeypatch, reserve_overflow_key_envs="GOOGLE_API_KEY3,GOOGLE_API_KEY2"
    )

    reserve = await client._reserve(_overflow_ctx("req-allfull"), attempt_no=1, candidate_key_ids=None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "rpd"
    assert len(supabase.rpc_calls) == 2  # tried overflow, still blocked


def test_normalize_overflow_envs_parses_csv_and_list() -> None:
    assert GoogleAIClient._normalize_overflow_envs(None) == []
    assert GoogleAIClient._normalize_overflow_envs("") == []
    assert GoogleAIClient._normalize_overflow_envs(" A , B ,A, ") == ["A", "B"]
    assert GoogleAIClient._normalize_overflow_envs(["A", "B", "A"]) == ["A", "B"]


def test_normal_pool_defaults_to_gateway_owned_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        GoogleAIClient.NORMAL_KEY_ENVS_ENV,
        "GOOGLE_API_KEY6, GOOGLE_API_KEY,GOOGLE_API_KEY6",
    )

    client = GoogleAIClient(supabase_client=_FakeSupabaseClient(), consumer="smart_update")

    assert client.reserve_key_envs == ["GOOGLE_API_KEY6", "GOOGLE_API_KEY"]


def test_explicit_normal_pool_overrides_gateway_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        GoogleAIClient.NORMAL_KEY_ENVS_ENV,
        "GOOGLE_API_KEY6,GOOGLE_API_KEY",
    )

    client = GoogleAIClient(
        supabase_client=_FakeSupabaseClient(),
        consumer="scoped_consumer",
        reserve_key_envs=["GOOGLE_API_KEY3", "GOOGLE_API_KEY5"],
    )

    assert client.reserve_key_envs == ["GOOGLE_API_KEY3", "GOOGLE_API_KEY5"]


def test_explicit_default_lane_does_not_inherit_gateway_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        GoogleAIClient.NORMAL_KEY_ENVS_ENV,
        "GOOGLE_API_KEY6,GOOGLE_API_KEY",
    )

    client = GoogleAIClient(
        supabase_client=_FakeSupabaseClient(),
        consumer="scoped_consumer",
        default_env_var_name="GOOGLE_API_KEY4",
    )

    assert client.default_env_var_name == "GOOGLE_API_KEY4"
    assert client.reserve_key_envs == []


class _NormalPoolSupabase:
    rows = [
        {
            "id": "id-key4",
            "env_var_name": "GOOGLE_API_KEY4",
            "priority": 2,
            "quota_scope": "google:project-a",
        },
        {
            "id": "id-key5",
            "env_var_name": "GOOGLE_API_KEY5",
            "priority": 4,
            "quota_scope": "google:project-b",
        },
    ]

    def __init__(self, blocked: set[str] | None = None):
        self.blocked = blocked or set()
        self.rpc_calls: list[dict] = []

    def table(self, _name: str):
        return _FakeSupabaseQuery(self.rows)

    def rpc(self, name: str, payload: dict):
        self.rpc_calls.append({"name": name, **dict(payload)})
        if name == "google_ai_report_provider_429":
            return SimpleNamespace(execute=lambda: SimpleNamespace(data=None))
        key_id = list(payload.get("p_candidate_key_ids") or [None])[0]
        if key_id in self.blocked:
            data = {"ok": False, "blocked_reason": "rpm", "retry_after_ms": 1000}
        else:
            env = "GOOGLE_API_KEY4" if key_id == "id-key4" else "GOOGLE_API_KEY5"
            row = next(row for row in self.rows if row["id"] == key_id)
            data = {
                "ok": True,
                "api_key_id": key_id,
                "env_var_name": env,
                "quota_scope": row["quota_scope"],
                "limiter_contract": _ATOMIC_LIMITER_CONTRACT,
                "bucket_strategy": _ROLLING_BUCKET_STRATEGY,
            }
        return SimpleNamespace(execute=lambda d=data: SimpleNamespace(data=d))


def _normal_pool_ctx(uid: str) -> RequestContext:
    return RequestContext(
        request_uid=uid,
        consumer="smart_update_image_geometry",
        account_name=None,
        model="gemma-4-31b",
        requested_model="gemma-4-31b-it",
        reserved_tpm=100,
    )


@pytest.mark.asyncio
async def test_normal_pool_rotates_from_first_reservation_without_overflow() -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    _NORMAL_POOL_CURSOR.clear()
    supabase = _NormalPoolSupabase()
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update_image_geometry",
        reserve_key_envs="GOOGLE_API_KEY4,GOOGLE_API_KEY5",
        reserve_overflow_key_envs=[],
    )

    first = await client._reserve(_normal_pool_ctx("pool-1"), 1, None)
    second = await client._reserve(_normal_pool_ctx("pool-2"), 1, None)

    assert first.env_var_name == "GOOGLE_API_KEY4"
    assert second.env_var_name == "GOOGLE_API_KEY5"
    assert [call["p_candidate_key_ids"] for call in supabase.rpc_calls] == [
        ["id-key4"],
        ["id-key5"],
    ]


@pytest.mark.asyncio
async def test_normal_pool_skips_minute_block_inside_same_allocation() -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    _NORMAL_POOL_CURSOR.clear()
    supabase = _NormalPoolSupabase(blocked={"id-key4"})
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update_image_geometry",
        reserve_key_envs=["GOOGLE_API_KEY4", "GOOGLE_API_KEY5"],
        reserve_overflow_key_envs=[],
    )

    reserve = await client._reserve(_normal_pool_ctx("pool-block"), 1, None)

    assert reserve.ok is True
    assert reserve.env_var_name == "GOOGLE_API_KEY5"
    assert [call["p_candidate_key_ids"] for call in supabase.rpc_calls] == [
        ["id-key4"],
        ["id-key5"],
    ]


@pytest.mark.asyncio
async def test_normal_pool_missing_registry_fails_closed() -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    client = GoogleAIClient(
        supabase_client=_FakeSupabaseClient(data=[]),
        consumer="smart_update_image_geometry",
        reserve_key_envs="GOOGLE_API_KEY4,GOOGLE_API_KEY5",
    )

    reserve = await client._reserve(_normal_pool_ctx("pool-missing"), 1, None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "normal_pool_candidates_missing"


@pytest.mark.asyncio
async def test_normal_pool_partial_registry_does_not_silently_shrink() -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    supabase = _NormalPoolSupabase()
    supabase.rows = [
        {"id": "id-key4", "env_var_name": "GOOGLE_API_KEY4", "priority": 2}
    ]
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update_image_geometry",
        reserve_key_envs="GOOGLE_API_KEY4,GOOGLE_API_KEY5",
    )

    reserve = await client._reserve(_normal_pool_ctx("pool-partial"), 1, None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "normal_pool_candidates_missing"
    assert supabase.rpc_calls == []


@pytest.mark.asyncio
async def test_normal_pool_without_shared_limiter_fails_closed() -> None:
    client = GoogleAIClient(
        supabase_client=None,
        consumer="smart_update_image_geometry",
        reserve_key_envs="GOOGLE_API_KEY4,GOOGLE_API_KEY5",
    )

    reserve = await client._reserve(_normal_pool_ctx("pool-no-limiter"), 1, None)

    assert reserve.ok is False
    assert reserve.blocked_reason == "normal_pool_limiter_unavailable"


@pytest.mark.asyncio
async def test_normal_pool_rotates_to_another_quota_scope_on_provider_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    _NORMAL_POOL_CURSOR.clear()
    client = GoogleAIClient(
        supabase_client=_NormalPoolSupabase(),
        consumer="smart_update",
        reserve_key_envs=["GOOGLE_API_KEY4", "GOOGLE_API_KEY5"],
        reserve_overflow_key_envs=[],
    )
    # Smart Update deliberately caps ordinary same-key retries at one. A
    # declared normal pool must still be able to move to its next member.
    client.max_retries = 1
    calls: list[list[str] | None] = []
    events: list[tuple[str, dict]] = []

    async def fake_attempt_generate(*, ctx, attempt_no, candidate_key_ids, **_kwargs):
        calls.append(candidate_key_ids)
        if len(calls) == 1:
            ctx.api_key_id = "id-key4"
            ctx.quota_scope = "google:project-a"
            raise ProviderError(
                error_type="ClientError",
                error_message="429 RESOURCE_EXHAUSTED",
                retryable=True,
                status_code=429,
                retry_after_ms=45000,
            )
        ctx.api_key_id = "id-key5"
        ctx.quota_scope = "google:project-b"
        return "ok", SimpleNamespace(total_tokens=1)

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)
    monkeypatch.setattr(
        client,
        "_log_event",
        lambda event, _ctx, **payload: events.append((event, payload)),
    )

    text, _usage = await client.generate_content_async(
        model="gemini-3.1-flash-lite",
        prompt="bounded",
        max_output_tokens=16,
    )

    assert text == "ok"
    assert calls == [None, ["id-key5"]]
    rotation = next(payload for event, payload in events if event == "google_ai.provider_key_rotation")
    assert rotation["exhausted_api_key_id"] == "id-key4"
    assert rotation["remaining_pool_members"] == 1
    assert rotation["retry_after_ms"] == 45000


@pytest.mark.asyncio
async def test_normal_pool_does_not_rotate_inside_same_quota_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    _NORMAL_POOL_CURSOR.clear()
    supabase = _NormalPoolSupabase()
    supabase.rows = [dict(row, quota_scope="google:shared-project") for row in supabase.rows]
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update",
        reserve_key_envs=["GOOGLE_API_KEY4", "GOOGLE_API_KEY5"],
        reserve_overflow_key_envs=[],
    )
    client.max_retries = 1
    calls = 0

    async def fake_attempt_generate(*, ctx, **_kwargs):
        nonlocal calls
        calls += 1
        ctx.api_key_id = "id-key4"
        ctx.quota_scope = "google:shared-project"
        raise ProviderError(
            error_type="ClientError",
            error_message="429 RESOURCE_EXHAUSTED",
            retryable=True,
            status_code=429,
            retry_after_ms=45000,
        )

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)

    with pytest.raises(ProviderError):
        await client.generate_content_async(
            model="gemini-3.1-flash-lite",
            prompt="same provider project",
            max_output_tokens=16,
            max_provider_attempts=3,
        )

    assert calls == 1
    assert [call["name"] for call in supabase.rpc_calls] == [
        "google_ai_report_provider_429"
    ]


@pytest.mark.asyncio
async def test_single_send_cap_reports_provider_429_without_rotation_or_model_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    _NORMAL_POOL_CURSOR.clear()
    supabase = _NormalPoolSupabase()
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update",
        reserve_key_envs=["GOOGLE_API_KEY4", "GOOGLE_API_KEY5"],
        reserve_overflow_key_envs=[],
    )
    calls: list[str] = []

    async def fake_attempt_generate(*, ctx, **_kwargs):
        calls.append(ctx.requested_model)
        ctx.api_key_id = "id-key4"
        ctx.quota_scope = "google:project-a"
        raise ProviderError(
            error_type="ClientError",
            error_message="429 RESOURCE_EXHAUSTED",
            retryable=True,
            status_code=429,
        )

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)
    with pytest.raises(ProviderError):
        await client.generate_content_async(
            model="gemini-3.1-flash-lite",
            fallback_models=["gemini-3.5-flash-lite"],
            prompt="one physical send",
            max_output_tokens=16,
            max_provider_attempts=1,
        )

    assert calls == ["gemini-3.1-flash-lite"]
    assert [call["name"] for call in supabase.rpc_calls] == [
        "google_ai_report_provider_429"
    ]


@pytest.mark.asyncio
async def test_per_call_model_fallback_opt_out_wins_over_explicit_chain(monkeypatch):
    client = GoogleAIClient()
    calls: list[str] = []

    async def fake_attempt_generate(*, ctx, **_kwargs):
        calls.append(ctx.requested_model)
        raise RateLimitError(blocked_reason="rpd", model=ctx.model)

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)
    with pytest.raises(RateLimitError):
        await client.generate_content_async(
            model="gemini-3.1-flash-lite",
            fallback_models=["gemini-3.5-flash-lite"],
            allow_model_fallback=False,
            prompt="stay on primary",
            max_output_tokens=16,
        )

    assert calls == ["gemini-3.1-flash-lite"]


@pytest.mark.asyncio
async def test_provider_429_can_fall_back_to_next_explicit_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    _NORMAL_POOL_CURSOR.clear()
    supabase = _NormalPoolSupabase()
    supabase.rows = [dict(row, quota_scope="google:shared-project") for row in supabase.rows]
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update",
        reserve_key_envs=["GOOGLE_API_KEY4", "GOOGLE_API_KEY5"],
        reserve_overflow_key_envs=[],
    )
    calls: list[str] = []

    async def fake_attempt_generate(*, ctx, **_kwargs):
        calls.append(ctx.requested_model)
        ctx.api_key_id = "id-key4"
        ctx.quota_scope = "google:shared-project"
        if ctx.requested_model == "gemini-3.1-flash-lite":
            raise ProviderError(
                error_type="ClientError",
                error_message="429 RESOURCE_EXHAUSTED",
                retryable=True,
                status_code=429,
            )
        return "ok-3.5", SimpleNamespace(total_tokens=1)

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)

    text, _usage = await client.generate_content_async(
        model="gemini-3.1-flash-lite",
        fallback_models=["gemini-3.5-flash-lite"],
        prompt="facts extraction",
        max_output_tokens=16,
    )

    assert text == "ok-3.5"
    assert calls == ["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"]


@pytest.mark.asyncio
async def test_normal_pool_can_disable_provider_429_rotation_for_hard_send_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    _NORMAL_POOL_CURSOR.clear()
    client = GoogleAIClient(
        supabase_client=_NormalPoolSupabase(),
        consumer="tg_monitor_video_quality",
        reserve_key_envs=["GOOGLE_API_KEY3", "GOOGLE_API_KEY5"],
        reserve_overflow_key_envs=[],
    )
    client.max_retries = 1
    client.allow_provider_429_rotation = False
    calls = 0

    async def fake_attempt_generate(*, ctx, **_kwargs):
        nonlocal calls
        calls += 1
        ctx.api_key_id = "id-key4"
        raise ProviderError(
            error_type="ClientError",
            error_message="429 RESOURCE_EXHAUSTED",
            retryable=True,
            status_code=429,
            retry_after_ms=45000,
        )

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)

    with pytest.raises(ProviderError):
        await client.generate_content_async(
            model="gemini-3.1-flash-lite",
            prompt="one physical send only",
            max_output_tokens=16,
        )

    assert calls == 1


@pytest.mark.asyncio
async def test_provider_429_without_normal_pool_remains_fail_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GoogleAIClient(
        supabase_client=_FakeSupabaseClient(),
        consumer="smart_update",
        reserve_overflow_key_envs="GOOGLE_API_KEY4,GOOGLE_API_KEY5",
    )
    client.max_retries = 3
    calls = 0

    async def fake_attempt_generate(*, ctx, **_kwargs):
        nonlocal calls
        calls += 1
        ctx.api_key_id = "id-key4"
        raise ProviderError(
            error_type="ClientError",
            error_message="429 RESOURCE_EXHAUSTED",
            retryable=True,
            status_code=429,
            retry_after_ms=45000,
        )

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)

    with pytest.raises(ProviderError) as exc_info:
        await client.generate_content_async(
            model="gemini-3.1-flash-lite",
            prompt="bounded",
            max_output_tokens=16,
        )

    assert exc_info.value.status_code == 429
    assert calls == 1


@pytest.mark.asyncio
async def test_provider_429_does_not_widen_explicit_candidate_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    client = GoogleAIClient(
        supabase_client=_NormalPoolSupabase(),
        consumer="smart_update",
        reserve_key_envs=["GOOGLE_API_KEY4", "GOOGLE_API_KEY5"],
    )
    calls = 0

    async def fake_attempt_generate(*, ctx, candidate_key_ids, **_kwargs):
        nonlocal calls
        calls += 1
        assert candidate_key_ids == ["id-key4"]
        ctx.api_key_id = "id-key4"
        raise ProviderError(
            error_type="ClientError",
            error_message="429 RESOURCE_EXHAUSTED",
            retryable=True,
            status_code=429,
        )

    monkeypatch.setattr(client, "_attempt_generate", fake_attempt_generate)

    with pytest.raises(ProviderError):
        await client.generate_content_async(
            model="gemini-3.1-flash-lite",
            prompt="bounded",
            max_output_tokens=16,
            candidate_key_ids=["id-key4"],
        )

    assert calls == 1


@pytest.mark.asyncio
async def test_embed_content_async_reserves_before_provider_call(monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY_EMBED", "test-embedding-key")
    monkeypatch.setenv("GOOGLE_AI_PROVIDER_TIMEOUT_SEC", "1")
    supabase = _FakeSupabaseClient(
        data=[{"id": "id-embed", "env_var_name": "GOOGLE_API_KEY_EMBED", "priority": 1}],
        reserve_data={
            "ok": True,
            "api_key_id": "id-embed",
            "env_var_name": "GOOGLE_API_KEY_EMBED",
            "key_alias": "embedding-key",
            "quota_scope": "google:test-project",
            "limiter_contract": _ATOMIC_LIMITER_CONTRACT,
            "bucket_strategy": _ROLLING_BUCKET_STRATEGY,
        },
    )
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="smart_update_identity_embedding",
        default_env_var_name="GOOGLE_API_KEY_EMBED",
    )

    provider_calls: list[dict] = []

    class _EmbeddingResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self):
            return b'{"embedding":{"values":[0.1,0.2,0.3]}}'

    def fake_urlopen(request, timeout=None):
        provider_calls.append(
            {
                "url": request.full_url,
                "headers": dict(request.header_items()),
                "body": request.data.decode("utf-8"),
                "timeout": timeout,
            }
        )
        return _EmbeddingResponse()

    monkeypatch.setattr("google_ai.client.urllib.request.urlopen", fake_urlopen)

    values, usage = await client.embed_content_async(
        model="gemini-embedding-2",
        text="identity document",
        output_dimensionality=3,
    )

    assert values == (0.1, 0.2, 0.3)
    assert usage.total_tokens > 0
    assert provider_calls, "provider must be called after reserve"
    assert provider_calls[0]["url"].endswith("/models/gemini-embedding-2:embedContent")
    assert "test-embedding-key" in provider_calls[0]["headers"].values()
    assert supabase.rpc_calls[0]["p_model"] == "gemini-embedding-2"
    assert supabase.rpc_calls[0]["p_consumer"] == "smart_update_identity_embedding"
    assert supabase.rpc_calls[0]["p_reserved_tpm"] > 0
    assert any("p_provider_status" in call for call in supabase.rpc_calls), "finalize RPC must be called"


@pytest.mark.asyncio
async def test_external_call_reservation_is_strict_and_uses_only_declared_pool(monkeypatch):
    rows = [
        {
            "id": "key-a",
            "env_var_name": "GOOGLE_ANTIGRAVITY_KEY_A",
            "priority": 1,
            "key_alias": "ag-a",
        },
        {
            "id": "key-b",
            "env_var_name": "GOOGLE_ANTIGRAVITY_KEY_B",
            "priority": 2,
            "key_alias": "ag-b",
        },
        {
            "id": "unrelated",
            "env_var_name": "GOOGLE_API_KEY",
            "priority": 0,
        },
    ]
    supabase = _StrictExternalSupabase(rows)
    client = GoogleAIClient(supabase_client=supabase, consumer="festival_antigravity")
    _NORMAL_POOL_CURSOR.clear()

    first = await client.reserve_external_call(
        model="antigravity-preview-05-2026",
        reserved_tpm=50_000,
        key_envs=["GOOGLE_ANTIGRAVITY_KEY_A", "GOOGLE_ANTIGRAVITY_KEY_B"],
    )
    second = await client.reserve_external_call(
        model="antigravity-preview-05-2026",
        reserved_tpm=50_000,
        key_envs=["GOOGLE_ANTIGRAVITY_KEY_A", "GOOGLE_ANTIGRAVITY_KEY_B"],
    )

    assert first.request_uid != second.request_uid
    assert [first.api_key_id, second.api_key_id] == ["key-a", "key-b"]
    reserve_calls = [payload for name, payload in supabase.rpc_calls if name == "google_ai_reserve"]
    assert [call["p_candidate_key_ids"] for call in reserve_calls] == [["key-a"], ["key-b"]]
    assert all("unrelated" not in call["p_candidate_key_ids"] for call in reserve_calls)


@pytest.mark.asyncio
async def test_external_call_reservation_fails_closed_without_ledger_or_complete_pool():
    no_ledger = GoogleAIClient(supabase_client=None)
    with pytest.raises(ReservationError, match="shared Supabase limiter"):
        await no_ledger.reserve_external_call(
            model="antigravity-preview-05-2026",
            reserved_tpm=10,
            key_envs=["GOOGLE_ANTIGRAVITY_KEY_A"],
        )

    supabase = _StrictExternalSupabase(
        [{"id": "key-a", "env_var_name": "GOOGLE_ANTIGRAVITY_KEY_A", "priority": 1}]
    )
    incomplete = GoogleAIClient(supabase_client=supabase)
    with pytest.raises(ReservationError, match="pool is incomplete"):
        await incomplete.reserve_external_call(
            model="antigravity-preview-05-2026",
            reserved_tpm=10,
            key_envs=["GOOGLE_ANTIGRAVITY_KEY_A", "GOOGLE_ANTIGRAVITY_KEY_B"],
        )
    assert not supabase.rpc_calls


@pytest.mark.asyncio
async def test_external_call_reservation_rejects_unversioned_limiter_contract():
    supabase = _StrictExternalSupabase(
        [
            {
                "id": "key-a",
                "env_var_name": "GOOGLE_ANTIGRAVITY_KEY_A",
                "priority": 1,
            }
        ],
        limiter_contract=None,
    )
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="festival_antigravity",
    )

    with pytest.raises(ReservationError, match="requires limiter contract"):
        await client.reserve_external_call(
            model="antigravity-preview-05-2026",
            reserved_tpm=10,
            key_envs=["GOOGLE_ANTIGRAVITY_KEY_A"],
        )


@pytest.mark.asyncio
async def test_external_call_reservation_rejects_old_bucket_strategy():
    supabase = _StrictExternalSupabase(
        [
            {
                "id": "key-a",
                "env_var_name": "GOOGLE_ANTIGRAVITY_KEY_A",
                "priority": 1,
            }
        ],
        bucket_strategy="fixed_minute_utc_day_v1",
    )
    client = GoogleAIClient(
        supabase_client=supabase,
        consumer="festival_antigravity",
    )

    with pytest.raises(ReservationError, match="requires limiter contract/strategy"):
        await client.reserve_external_call(
            model="antigravity-preview-05-2026",
            reserved_tpm=10,
            key_envs=["GOOGLE_ANTIGRAVITY_KEY_A"],
        )


@pytest.mark.asyncio
async def test_external_finalize_keeps_provider_and_semantic_status_separate():
    supabase = _StrictExternalSupabase([])
    client = GoogleAIClient(supabase_client=supabase)
    lease = ExternalCallLease(
        request_uid="00000000-0000-4000-8000-000000000001",
        attempt_no=1,
        consumer="festival_antigravity",
        account_name=None,
        model="antigravity-preview-05-2026",
        reserved_tpm=50_000,
        api_key_id="key-a",
        env_var_name="GOOGLE_ANTIGRAVITY_KEY_A",
        key_alias="ag-a",
        minute_bucket=None,
        day_bucket=None,
        started_at=datetime.now(timezone.utc),
    )

    await client.finalize_external_call(
        lease,
        provider_interaction_id="interaction-1",
        provider_terminal_status="completed",
        semantic_status="not_evaluated",
        usage=SimpleNamespace(input_tokens=10, output_tokens=20, total_tokens=30),
        duration_ms=123,
    )
    await client.record_external_call_semantic_result(
        lease,
        semantic_status="failed",
        semantic_error="missing evidence",
    )

    finalize = next(payload for name, payload in supabase.rpc_calls if name == "google_ai_finalize_interaction")
    semantic = next(payload for name, payload in supabase.rpc_calls if name == "google_ai_record_interaction_semantic")
    assert finalize["p_provider_terminal_status"] == "completed"
    assert finalize["p_semantic_status"] == "not_evaluated"
    assert semantic["p_semantic_status"] == "failed"
    assert semantic["p_semantic_error"] == "missing evidence"


def test_external_call_api_key_is_resolved_only_from_leased_env(monkeypatch):
    monkeypatch.setenv("GOOGLE_ANTIGRAVITY_KEY_A", "never-log-this-secret")
    client = GoogleAIClient()
    lease = ExternalCallLease(
        request_uid="00000000-0000-4000-8000-000000000002",
        attempt_no=1,
        consumer="festival_antigravity",
        account_name=None,
        model="antigravity-preview-05-2026",
        reserved_tpm=1,
        api_key_id="key-a",
        env_var_name="GOOGLE_ANTIGRAVITY_KEY_A",
        key_alias="ag-a",
        minute_bucket=None,
        day_bucket=None,
        started_at=datetime.now(timezone.utc),
    )

    assert client.get_external_call_api_key(lease) == "never-log-this-secret"
    assert "never-log-this-secret" not in repr(lease)
    checkpoint = lease.to_dict()
    assert "never-log-this-secret" not in json.dumps(checkpoint)
    assert ExternalCallLease.from_dict(checkpoint) == lease


def test_project_scope_atomic_migration_exposes_verifiable_contract() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "009_google_ai_project_scope_atomic_reserve.sql"
    ).read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS quota_scope TEXT NOT NULL" in migration
    assert "google:default-project" in migration
    assert "CREATE OR REPLACE FUNCTION google_ai_limiter_capabilities()" in migration
    assert _ATOMIC_LIMITER_CONTRACT in migration
    assert "'limiter_contract', v_contract" in migration
    assert "('gemma-4-31b', 15, 15000, 14000, 1000)" in migration
    assert "('gemma-4-26b-a4b', 15, 15000, 14000, 1000)" in migration


def test_project_scope_atomic_migration_locks_and_aggregates_by_scope_model() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "009_google_ai_project_scope_atomic_reserve.sql"
    ).read_text(encoding="utf-8")

    assert "pg_advisory_xact_lock" in migration
    assert "v_quota_scope || ':' || p_model" in migration
    assert migration.count("JOIN google_ai_api_keys scope_key") >= 2
    assert migration.count("scope_key.quota_scope = v_quota_scope") >= 2
    assert "COALESCE(SUM(c.rpm_used), 0)" in migration
    assert "COALESCE(SUM(c.tpm_used), 0)" in migration
    assert "COALESCE(SUM(c.rpd_used), 0)" in migration


def test_personalization_limiter_bootstrap_is_self_contained_and_secret_free() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "supabase"
        / "migrations"
        / "20260731170000_google_ai_canonical_limiter_bootstrap.sql"
    ).read_text(encoding="utf-8")

    for table in (
        "google_ai_model_limits",
        "google_ai_api_keys",
        "google_ai_usage_counters",
        "google_ai_requests",
        "google_ai_request_attempts",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in migration
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY" in migration
    for rpc in (
        "google_ai_limiter_capabilities",
        "google_ai_reserve",
        "google_ai_mark_sent",
        "google_ai_finalize",
        "google_ai_sweep_stale",
        "google_ai_finalize_interaction",
        "google_ai_record_interaction_semantic",
    ):
        assert f"CREATE OR REPLACE FUNCTION {rpc}(" in migration
        assert f"REVOKE ALL ON FUNCTION {rpc}(" in migration

    assert _ATOMIC_LIMITER_CONTRACT in migration
    assert "pg_advisory_xact_lock" in migration
    assert "scope_key.quota_scope = v_quota_scope" in migration
    assert "INSERT INTO google_ai_api_keys" not in migration
    assert "Split-ledger" in migration
    assert "old Antigravity lease must finalize where" in migration
    assert "('gemma-4-31b', 15, 15000, 14000, 1000)" in migration
    assert "('gemma-4-26b-a4b', 15, 15000, 14000, 1000)" in migration
    assert "2147483647" not in migration


def test_google_ai_retry_attempt_accounting_uses_attempt_bucket_and_terminality() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "supabase"
        / "migrations"
        / "20260731183500_google_ai_retry_attempt_accounting.sql"
    ).read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS minute_bucket" in migration
    assert "ADD COLUMN IF NOT EXISTS day_bucket" in migration
    assert "trg_google_ai_sync_attempt_context" in migration
    assert "attempts = GREATEST(attempts, NEW.attempt_no)" in migration
    assert "v_attempt.completed_at IS NOT NULL" in migration
    assert "v_delta := p_usage_total_tokens - v_attempt.reserved_tpm" in migration
    assert "api_key_id = v_attempt.api_key_id" in migration
    assert "minute_bucket = v_attempt.minute_bucket" in migration
    assert "AND attempts <= p_attempt_no" in migration
    assert "IF v_request.finalized_at IS NOT NULL" not in migration


@pytest.mark.asyncio
async def test_direct_reserve_retry_never_uses_general_supabase_env(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_URL", "https://general.example")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "general-secret")
    monkeypatch.delenv("GOOGLE_AI_LIMITER_SUPABASE_URL", raising=False)
    monkeypatch.delenv("GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", raising=False)
    client = GoogleAIClient()
    context = RequestContext(
        request_uid="00000000-0000-4000-8000-000000000003",
        consumer="test",
        account_name=None,
        model="gemini-3.1-flash-lite",
        reserved_tpm=100,
    )

    result = await client._reserve_via_direct_rest(
        context,
        attempt_no=1,
        payload={},
    )

    assert result is None


@pytest.mark.asyncio
async def test_cancelled_sent_attempt_is_finalized_before_cancellation_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GoogleAIClient(supabase_client=_FakeSupabaseClient())
    ctx = RequestContext(
        request_uid="00000000-0000-4000-8000-000000000004",
        consumer="event_parse",
        account_name=None,
        model="gemma-4-31b",
        requested_model="models/gemma-4-31b-it",
        reserved_tpm=14000,
    )
    finalized: list[dict] = []

    async def fake_reserve(*_args, **_kwargs):
        return ReserveResult(
            ok=True,
            api_key_id="key-id",
            env_var_name="GOOGLE_API_KEY",
            quota_scope="google:test-project",
        )

    async def fake_mark_sent(*_args, **_kwargs):
        return None

    async def fake_provider(**_kwargs):
        raise asyncio.CancelledError

    async def fake_finalize(**kwargs):
        finalized.append(kwargs)

    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")
    monkeypatch.setattr(client, "_reserve", fake_reserve)
    monkeypatch.setattr(client, "_mark_sent", fake_mark_sent)
    monkeypatch.setattr(client, "_call_provider", fake_provider)
    monkeypatch.setattr(client, "_finalize", fake_finalize)

    with pytest.raises(asyncio.CancelledError):
        await client._attempt_generate(
            ctx=ctx,
            attempt_no=2,
            prompt="bounded",
            generation_config={"temperature": 0},
            safety_settings=None,
            max_output_tokens=6000,
            candidate_key_ids=None,
        )

    assert len(finalized) == 1
    assert finalized[0]["attempt_no"] == 2
    assert finalized[0]["error"].error_type == "cancelled"
    assert finalized[0]["error"].retryable is False


@pytest.mark.asyncio
async def test_real_attempt_observer_runs_after_mark_sent_before_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GoogleAIClient(supabase_client=_FakeSupabaseClient())
    ctx = RequestContext(
        request_uid="00000000-0000-4000-8000-000000000005",
        consumer="collection_candidate_adjudication",
        account_name=None,
        model="gemma-4-31b",
        requested_model="gemma-4-31b-it",
        provider_model="gemma-4-31b",
        provider_model_name="models/gemma-4-31b-it",
        reserved_tpm=1000,
    )
    order: list[str] = []
    observed: list[dict] = []

    async def fake_reserve(*_args, **_kwargs):
        return ReserveResult(
            ok=True,
            api_key_id="key-id",
            env_var_name="GOOGLE_API_KEY",
            quota_scope="google:test-project",
        )

    async def fake_mark_sent(*_args, **_kwargs):
        order.append("sent")

    async def fake_provider(**_kwargs):
        order.append("provider")
        return "{}", UsageInfo(input_tokens=10, output_tokens=1, total_tokens=11)

    async def fake_finalize(**_kwargs):
        order.append("finalized")

    def observe(payload):
        order.append("observed")
        observed.append(payload)

    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")
    monkeypatch.setattr(client, "_reserve", fake_reserve)
    monkeypatch.setattr(client, "_mark_sent", fake_mark_sent)
    monkeypatch.setattr(client, "_call_provider", fake_provider)
    monkeypatch.setattr(client, "_finalize", fake_finalize)

    text, usage = await client._attempt_generate(
        ctx=ctx,
        attempt_no=1,
        prompt="bounded",
        generation_config={"temperature": 0},
        safety_settings=None,
        max_output_tokens=1000,
        candidate_key_ids=None,
        attempt_observer=observe,
    )

    assert text == "{}"
    assert usage.total_tokens == 11
    assert order == ["sent", "observed", "provider", "finalized"]
    assert observed == [
        {
            "attempt_no": 1,
            "requested_model": "gemma-4-31b-it",
            "provider_model": "gemma-4-31b",
            "provider_model_name": "models/gemma-4-31b-it",
        }
    ]
