from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from google_ai.client import (
    _DEFAULT_ENV_CANDIDATE_CACHE,
    _NORMAL_POOL_CURSOR,
    _NORMAL_POOL_ENV_CANDIDATE_CACHE,
    _OVERFLOW_ENV_CANDIDATE_CACHE,
    GoogleAIClient,
    RequestContext,
)
from google_ai.exceptions import ProviderError


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
        }
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
    assert "response_schema_name" not in fake_genai.calls[0]["generation_config"]


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
async def test_no_supabase_uses_local_rpm_limiter_by_default(monkeypatch):
    monkeypatch.delenv("GOOGLE_AI_LOCAL_RPM", raising=False)
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
    assert reserve.blocked_reason == "supabase_unavailable"
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


class _NormalPoolSupabase:
    rows = [
        {"id": "id-key4", "env_var_name": "GOOGLE_API_KEY4", "priority": 2},
        {"id": "id-key5", "env_var_name": "GOOGLE_API_KEY5", "priority": 4},
    ]

    def __init__(self, blocked: set[str] | None = None):
        self.blocked = blocked or set()
        self.rpc_calls: list[dict] = []

    def table(self, _name: str):
        return _FakeSupabaseQuery(self.rows)

    def rpc(self, _name: str, payload: dict):
        self.rpc_calls.append(dict(payload))
        key_id = list(payload.get("p_candidate_key_ids") or [None])[0]
        if key_id in self.blocked:
            data = {"ok": False, "blocked_reason": "rpm", "retry_after_ms": 1000}
        else:
            env = "GOOGLE_API_KEY4" if key_id == "id-key4" else "GOOGLE_API_KEY5"
            data = {"ok": True, "api_key_id": key_id, "env_var_name": env}
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
async def test_normal_pool_rotates_to_another_key_on_provider_429(
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
            raise ProviderError(
                error_type="ClientError",
                error_message="429 RESOURCE_EXHAUSTED",
                retryable=True,
                status_code=429,
                retry_after_ms=45000,
            )
        ctx.api_key_id = "id-key5"
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
