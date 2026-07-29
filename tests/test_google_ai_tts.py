from __future__ import annotations

import wave
from pathlib import Path
from types import SimpleNamespace

import pytest

from google_ai.client import (
    _NORMAL_POOL_ENV_CANDIDATE_CACHE,
    GoogleAIClient,
    RequestContext,
    ReserveResult,
    UsageInfo,
)
from google_ai.exceptions import ProviderError, RateLimitError, ReservationError
from google_ai.tts import (
    DEFAULT_TTS_MODEL,
    GoogleTTSClient,
    TTS_QUOTA_SCOPE,
    build_tts_prompt,
    write_wav,
)


class _Secrets:
    def get_secret(self, name: str):
        return f"secret-for-{name}"


class _Query:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def is_(self, *_args, **_kwargs):
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self.rows)


class _PreflightSupabase:
    def table(self, name: str):
        if name == "google_ai_model_limits":
            return _Query(
                [
                    {
                        "model": DEFAULT_TTS_MODEL,
                        "quota_scope": TTS_QUOTA_SCOPE,
                        "rpm": 1,
                        "tpm": 2147483647,
                        "rpd": 10,
                    },
                    {
                        "model": "gemini-3.1-flash-tts-preview",
                        "quota_scope": TTS_QUOTA_SCOPE,
                        "rpm": 1,
                        "tpm": 2147483647,
                        "rpd": 10,
                    }
                ]
            )
        if name == "google_ai_usage_counters":
            return _Query([{"api_key_id": "key-1", "rpd_used": 1}])
        if name == "google_ai_api_keys":
            return _Query(
                [
                    {
                        "id": "key-1",
                        "env_var_name": "GOOGLE_API_KEY",
                        "priority": 1,
                    },
                    {
                        "id": "key-2",
                        "env_var_name": "GOOGLE_API_KEY2",
                        "priority": 2,
                    },
                ]
            )
        raise AssertionError(name)


class _FakeModels:
    def __init__(self, owner):
        self.owner = owner

    async def generate_content(self, **kwargs):
        self.owner.calls.append(kwargs)
        if self.owner.error:
            raise self.owner.error
        return SimpleNamespace(
            candidates=[
                SimpleNamespace(
                    content=SimpleNamespace(
                        parts=[
                            SimpleNamespace(
                                inline_data=SimpleNamespace(
                                    data=b"\x01\x00" * 2400,
                                    mime_type="audio/L16;codec=pcm;rate=24000",
                                )
                            )
                        ]
                    )
                )
            ],
            usage_metadata=SimpleNamespace(
                prompt_token_count=12,
                candidates_token_count=25,
                total_token_count=37,
            ),
        )


class _FakeGenAI:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def Client(self, *, api_key: str):
        assert api_key == "secret-for-GOOGLE_API_KEY"
        return SimpleNamespace(aio=SimpleNamespace(models=_FakeModels(self)))


def _client() -> GoogleTTSClient:
    return GoogleTTSClient(
        supabase_client=_PreflightSupabase(),
        secrets_provider=_Secrets(),
        key_envs=["GOOGLE_API_KEY", "GOOGLE_API_KEY2"],
    )


def test_preflight_reports_one_used_request_without_reserving(monkeypatch):
    client = _client()
    reserve_called = False

    async def forbidden_reserve(*_args, **_kwargs):
        nonlocal reserve_called
        reserve_called = True
        raise AssertionError("preflight must not reserve")

    monkeypatch.setattr(client.gateway, "_reserve", forbidden_reserve)
    result = client.preflight()

    assert result["provider_attempts_per_generation"] == 1
    assert result["quota_scope"] == "google-tts"
    assert result["keys"][0]["used"] == 1
    assert result["keys"][0]["remaining"] == 9
    assert result["keys"][1]["used"] == 0
    assert reserve_called is False


@pytest.mark.asyncio
async def test_generation_reserves_marks_sends_once_and_finalizes(monkeypatch, tmp_path):
    client = _client()
    fake_sdk = _FakeGenAI()
    client.gateway._genai_new = fake_sdk
    lifecycle = []

    async def reserve(*_args, **_kwargs):
        lifecycle.append("reserve")
        return ReserveResult(
            ok=True,
            api_key_id="key-1",
            env_var_name="GOOGLE_API_KEY",
            key_alias="primary",
            quota_scope="google-tts",
        )

    async def mark(*_args, **_kwargs):
        lifecycle.append("mark_sent")

    async def finalize(*_args, **kwargs):
        lifecycle.append(("finalize", kwargs.get("error")))

    monkeypatch.setattr(client.gateway, "_reserve", reserve)
    monkeypatch.setattr(client.gateway, "_mark_sent", mark)
    monkeypatch.setattr(client.gateway, "_finalize", finalize)

    speech = await client.generate_speech_async(text="Привет")

    assert lifecycle == ["reserve", "mark_sent", ("finalize", None)]
    assert len(fake_sdk.calls) == 1
    assert fake_sdk.calls[0]["model"] == DEFAULT_TTS_MODEL
    assert fake_sdk.calls[0]["config"]["response_modalities"] == ["AUDIO"]
    assert (
        fake_sdk.calls[0]["config"]["speech_config"]["voice_config"][
            "prebuilt_voice_config"
        ]["voice_name"]
        == "Aoede"
    )
    assert "### TRANSCRIPT\nПривет" in fake_sdk.calls[0]["contents"]
    assert speech.usage == UsageInfo(12, 25, 37)
    path = write_wav(tmp_path / "speech.wav", speech)
    with wave.open(str(path), "rb") as wav:
        assert wav.getframerate() == 24000
        assert wav.getnchannels() == 1
        assert wav.getsampwidth() == 2


@pytest.mark.asyncio
async def test_provider_failure_is_not_retried_or_rotated(monkeypatch):
    client = _client()
    fake_sdk = _FakeGenAI(RuntimeError("503 UNAVAILABLE"))
    client.gateway._genai_new = fake_sdk
    finalized = []

    async def reserve(*_args, **_kwargs):
        return ReserveResult(
            ok=True,
            api_key_id="key-1",
            env_var_name="GOOGLE_API_KEY",
            quota_scope="google-tts",
        )

    async def mark(*_args, **_kwargs):
        return None

    async def finalize(*_args, **kwargs):
        finalized.append(kwargs.get("error"))

    monkeypatch.setattr(client.gateway, "_reserve", reserve)
    monkeypatch.setattr(client.gateway, "_mark_sent", mark)
    monkeypatch.setattr(client.gateway, "_finalize", finalize)

    with pytest.raises(ProviderError):
        await client.generate_speech_async(text="Привет")

    assert len(fake_sdk.calls) == 1
    assert len(finalized) == 1
    assert finalized[0].status_code == 503


@pytest.mark.asyncio
async def test_quota_block_makes_zero_provider_calls(monkeypatch):
    client = _client()
    fake_sdk = _FakeGenAI()
    client.gateway._genai_new = fake_sdk

    async def blocked(*_args, **_kwargs):
        return ReserveResult(ok=False, blocked_reason="rpd")

    monkeypatch.setattr(client.gateway, "_reserve", blocked)

    with pytest.raises(RateLimitError):
        await client.generate_speech_async(text="Привет")
    assert fake_sdk.calls == []


@pytest.mark.asyncio
async def test_mark_sent_failure_makes_zero_provider_calls(monkeypatch):
    client = _client()
    fake_sdk = _FakeGenAI()
    client.gateway._genai_new = fake_sdk

    async def reserve(*_args, **_kwargs):
        return ReserveResult(
            ok=True,
            api_key_id="key-1",
            env_var_name="GOOGLE_API_KEY",
            quota_scope="google-tts",
        )

    async def mark(*_args, **_kwargs):
        raise ReservationError("mark_sent unavailable")

    monkeypatch.setattr(client.gateway, "_reserve", reserve)
    monkeypatch.setattr(client.gateway, "_mark_sent", mark)

    with pytest.raises(ReservationError):
        await client.generate_speech_async(text="Привет")
    assert fake_sdk.calls == []


@pytest.mark.asyncio
async def test_missing_shared_quota_scope_makes_zero_provider_calls(monkeypatch):
    client = _client()
    fake_sdk = _FakeGenAI()
    client.gateway._genai_new = fake_sdk

    async def legacy_reserve(*_args, **_kwargs):
        return ReserveResult(
            ok=True,
            api_key_id="key-1",
            env_var_name="GOOGLE_API_KEY",
            quota_scope=None,
        )

    monkeypatch.setattr(client.gateway, "_reserve", legacy_reserve)

    with pytest.raises(ReservationError, match="Unexpected TTS quota scope"):
        await client.generate_speech_async(text="Привет")
    assert fake_sdk.calls == []


def test_duplicate_active_registry_rows_fail_closed():
    class _DuplicateSupabase(_PreflightSupabase):
        def table(self, name: str):
            if name == "google_ai_api_keys":
                return _Query(
                    [
                        {
                            "id": "key-1a",
                            "env_var_name": "GOOGLE_API_KEY",
                            "priority": 1,
                        },
                        {
                            "id": "key-1b",
                            "env_var_name": "GOOGLE_API_KEY",
                            "priority": 2,
                        },
                    ]
                )
            return super().table(name)

    _NORMAL_POOL_ENV_CANDIDATE_CACHE.clear()
    client = GoogleTTSClient(
        supabase_client=_DuplicateSupabase(),
        secrets_provider=_Secrets(),
        key_envs=["GOOGLE_API_KEY"],
    )
    with pytest.raises(ReservationError):
        client.preflight()


@pytest.mark.asyncio
async def test_strict_gateway_rejects_missing_shared_limiter():
    gateway = GoogleAIClient(
        supabase_client=None,
        require_shared_limiter=True,
        reserve_key_envs=["GOOGLE_API_KEY"],
    )
    ctx = RequestContext(
        request_uid="strict-test",
        consumer="tts",
        account_name=None,
        model=DEFAULT_TTS_MODEL,
        requested_model=DEFAULT_TTS_MODEL,
        reserved_tpm=1,
    )
    with pytest.raises(ReservationError):
        await gateway._reserve(ctx, 1, None)


def test_prompt_is_verbatim_and_validates_empty_text():
    prompt = build_tts_prompt("Мой электронный мозг")
    assert prompt.endswith("### TRANSCRIPT\nМой электронный мозг")
    with pytest.raises(ValueError):
        build_tts_prompt(" ")


def test_migration_shares_models_and_backfills_once():
    sql = Path("migrations/006_google_ai_tts_limits.sql").read_text()
    assert sql.count("'google-tts', 1, 2147483647, 10, 0") == 4
    assert "pg_advisory_xact_lock" in sql
    assert "ON CONFLICT (api_key_id, quota_scope, day_bucket)" in sql
    assert "idx_google_ai_api_keys_active_provider_env" in sql
    assert "CREATE OR REPLACE FUNCTION google_ai_sweep_stale" in sql
    assert "AND quota_scope = v_req.quota_scope" in sql
    assert "09115e9c-ad36-5ca1-af27-da29aad439c7" in sql
    assert "GET DIAGNOSTICS v_inserted = ROW_COUNT" in sql
