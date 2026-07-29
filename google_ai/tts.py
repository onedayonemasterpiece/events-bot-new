"""Fail-closed Gemini text-to-speech through the shared Google AI limiter."""

from __future__ import annotations

import base64
import re
import uuid
import wave
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Iterable

from google_ai.client import GoogleAIClient, RequestContext, UsageInfo
from google_ai.exceptions import ProviderError, RateLimitError, ReservationError

DEFAULT_TTS_MODEL = "gemini-2.5-flash-preview-tts"
SUPPORTED_TTS_MODELS = frozenset(
    {
        DEFAULT_TTS_MODEL,
        "gemini-3.1-flash-tts-preview",
    }
)
TTS_QUOTA_SCOPE = "google-tts"
DEFAULT_TTS_VOICE = "Aoede"
SUPPORTED_TTS_VOICES = frozenset(
    {
        "Achernar",
        "Achird",
        "Algenib",
        "Algieba",
        "Alnilam",
        "Aoede",
        "Autonoe",
        "Callirrhoe",
        "Charon",
        "Despina",
        "Enceladus",
        "Erinome",
        "Fenrir",
        "Gacrux",
        "Iapetus",
        "Kore",
        "Laomedeia",
        "Leda",
        "Orus",
        "Puck",
        "Pulcherrima",
        "Rasalgethi",
        "Sadachbia",
        "Sadaltager",
        "Schedar",
        "Sulafat",
        "Umbriel",
        "Vindemiatrix",
        "Zephyr",
        "Zubenelgenubi",
    }
)
MAX_TTS_TEXT_CHARS = 12_000


@dataclass(frozen=True)
class SpeechResult:
    pcm: bytes
    model: str
    voice: str
    mime_type: str
    sample_rate: int
    channels: int
    sample_width: int
    request_uid: str
    api_key_id: str | None
    key_alias: str | None
    quota_scope: str
    usage: UsageInfo

    @property
    def duration_seconds(self) -> float:
        frame_width = max(1, self.channels * self.sample_width)
        return len(self.pcm) / frame_width / max(1, self.sample_rate)


def build_tts_prompt(
    text: str,
    *,
    language: str = "Russian",
    style: str = "Warm, friendly and kind, with a gentle smile.",
) -> str:
    """Keep performance guidance separate from the verbatim transcript."""

    transcript = str(text or "").strip()
    if not transcript:
        raise ValueError("TTS transcript must not be empty")
    if len(transcript) > MAX_TTS_TEXT_CHARS:
        raise ValueError(
            f"TTS transcript exceeds the {MAX_TTS_TEXT_CHARS}-character safety cap"
        )
    return (
        "### DIRECTOR'S NOTES\n"
        f"Language: {language}. Adult female voice. {style.strip()} "
        "Natural pace and clear pronunciation. Read the transcript verbatim; "
        "do not add, omit, or change any words.\n\n"
        "### TRANSCRIPT\n"
        f"{transcript}"
    )


def write_wav(path: str | Path, speech: SpeechResult) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(destination), "wb") as stream:
        stream.setnchannels(speech.channels)
        stream.setsampwidth(speech.sample_width)
        stream.setframerate(speech.sample_rate)
        stream.writeframes(speech.pcm)
    return destination


class GoogleTTSClient:
    """One-attempt TTS facade that cannot run without shared quota control."""

    def __init__(
        self,
        *,
        supabase_client: Any,
        secrets_provider: Any,
        key_envs: Iterable[str],
        consumer: str = "google_tts",
        account_name: str | None = None,
    ) -> None:
        envs = GoogleAIClient._normalize_overflow_envs(list(key_envs))
        if not envs:
            raise ReservationError("GOOGLE_TTS_KEY_ENVS must name at least one key")
        self.key_envs = tuple(envs)
        self.gateway = GoogleAIClient(
            supabase_client=supabase_client,
            secrets_provider=secrets_provider,
            consumer=consumer,
            account_name=account_name,
            default_env_var_name=envs[0],
            reserve_key_envs=envs,
            reserve_overflow_key_envs=[],
            require_shared_limiter=True,
        )
        # TTS never retries or falls back after a sent provider request.  A new
        # attempt requires a fresh explicit user action.
        self.gateway.max_retries = 1
        self.gateway.fallback_models = []
        self.gateway.allow_reserve_fallback = False
        self.gateway.allow_local_limiter_fallback = False
        self.gateway.allow_local_limiter_on_reserve_error = False

    def preflight(self, *, model: str = DEFAULT_TTS_MODEL) -> dict[str, Any]:
        """Inspect registry/limits/counters without reserving or calling Google."""

        selected_model = self._validate_model(model)
        if self.gateway.supabase is None:
            raise ReservationError("Shared Google AI limiter is unavailable")

        candidate_ids = self.gateway._resolve_normal_pool_candidate_key_ids()
        if not candidate_ids or len(candidate_ids) != len(self.key_envs):
            raise ReservationError(
                "Every GOOGLE_TTS_KEY_ENVS member must have one active registry row"
            )
        missing_secrets = [
            env_name
            for env_name in self.key_envs
            if not self.gateway._get_api_key(env_name)
        ]
        if missing_secrets:
            raise ReservationError(
                "Configured TTS key secrets are missing: " + ",".join(missing_secrets)
            )

        try:
            limits_response = (
                self.gateway.supabase.table("google_ai_model_limits")
                .select("model, quota_scope, rpm, tpm, rpd")
                .in_("model", sorted(SUPPORTED_TTS_MODELS))
                .execute()
            )
            limit_rows = list(limits_response.data or [])
        except Exception as exc:
            raise ReservationError(
                f"TTS model limits are unavailable in the shared limiter: {exc}"
            ) from exc
        by_model = {
            str(row.get("model") or ""): row
            for row in limit_rows
            if row.get("model")
        }
        if set(by_model) != set(SUPPORTED_TTS_MODELS):
            raise ReservationError(
                "Both supported TTS models must be registered exactly once"
            )
        if any(
            str(row.get("quota_scope") or "") != TTS_QUOTA_SCOPE
            for row in by_model.values()
        ):
            raise ReservationError(
                f"Every TTS model must use shared quota scope {TTS_QUOTA_SCOPE}"
            )
        if any(int(row.get("rpd") or 0) != 10 for row in by_model.values()):
            raise ReservationError("TTS RPD must be exactly 10")

        day_bucket = datetime.now(timezone.utc).date().isoformat()
        try:
            usage_response = (
                self.gateway.supabase.table("google_ai_usage_counters")
                .select("api_key_id, rpd_used")
                .eq("quota_scope", TTS_QUOTA_SCOPE)
                .eq("day_bucket", day_bucket)
                .is_("minute_bucket", "null")
                .in_("api_key_id", list(candidate_ids))
                .execute()
            )
            usage_rows = list(usage_response.data or [])
        except Exception as exc:
            raise ReservationError(
                f"TTS daily counters are unavailable in the shared limiter: {exc}"
            ) from exc
        used_by_id = {
            str(row.get("api_key_id")): int(row.get("rpd_used") or 0)
            for row in usage_rows
        }
        keys = [
            {
                "api_key_id": key_id,
                "used": used_by_id.get(key_id, 0),
                "limit": 10,
                "remaining": max(0, 10 - used_by_id.get(key_id, 0)),
            }
            for key_id in candidate_ids
        ]
        return {
            "ok": True,
            "model": selected_model,
            "quota_scope": TTS_QUOTA_SCOPE,
            "day_bucket": day_bucket,
            "provider_attempts_per_generation": 1,
            "keys": keys,
            "total_remaining": sum(item["remaining"] for item in keys),
        }

    async def generate_speech_async(
        self,
        *,
        text: str,
        model: str = DEFAULT_TTS_MODEL,
        voice: str = DEFAULT_TTS_VOICE,
        language: str = "Russian",
        style: str = "Warm, friendly and kind, with a gentle smile.",
    ) -> SpeechResult:
        selected_model = self._validate_model(model)
        selected_voice = self._validate_voice(voice)
        prompt = build_tts_prompt(text, language=language, style=style)
        request_uid = str(uuid.uuid4())
        provider_model, provider_model_name = self.gateway._resolve_provider_model(
            selected_model
        )
        ctx = RequestContext(
            request_uid=request_uid,
            consumer=self.gateway.consumer,
            account_name=self.gateway.account_name,
            model=selected_model,
            requested_model=selected_model,
            provider_model=provider_model,
            provider_model_name=provider_model_name,
            reserved_tpm=self.gateway._calculate_reserved_tpm(
                prompt=prompt,
                max_output_tokens=1,
            ),
        )

        reserve = await self.gateway._reserve(ctx, 1, None)
        if not reserve.ok:
            raise RateLimitError(
                blocked_reason=reserve.blocked_reason or "unknown",
                retry_after_ms=reserve.retry_after_ms,
                model=ctx.model,
                api_key_id=reserve.api_key_id,
                minute_bucket=reserve.minute_bucket,
                day_bucket=reserve.day_bucket,
            )
        if reserve.quota_scope != TTS_QUOTA_SCOPE:
            raise ReservationError(
                f"Unexpected TTS quota scope: {reserve.quota_scope}"
            )

        ctx.api_key_id = reserve.api_key_id
        api_key = self.gateway._get_api_key(reserve.env_var_name)
        if not api_key:
            raise ReservationError(
                f"Reserved Google key secret is unavailable: {reserve.env_var_name}"
            )
        await self.gateway._mark_sent(ctx, 1)

        started = monotonic()
        try:
            pcm, mime_type, usage = await self._call_provider(
                api_key=api_key,
                model=selected_model,
                prompt=prompt,
                voice=selected_voice,
            )
        except Exception as exc:
            duration_ms = int((monotonic() - started) * 1000)
            error = self.gateway._classify_error(exc)
            await self.gateway._finalize(
                ctx=ctx,
                attempt_no=1,
                usage=None,
                duration_ms=duration_ms,
                error=error,
            )
            self.gateway._log_event(
                "google_ai.tts_call_error",
                ctx,
                attempt_no=1,
                duration_ms=duration_ms,
                error=error,
            )
            raise error

        duration_ms = int((monotonic() - started) * 1000)
        await self.gateway._finalize(
            ctx=ctx,
            attempt_no=1,
            usage=usage,
            duration_ms=duration_ms,
        )
        self.gateway._log_event(
            "google_ai.tts_call_ok",
            ctx,
            attempt_no=1,
            duration_ms=duration_ms,
            usage=usage,
        )
        return SpeechResult(
            pcm=pcm,
            model=selected_model,
            voice=selected_voice,
            mime_type=mime_type,
            sample_rate=self._sample_rate(mime_type),
            channels=1,
            sample_width=2,
            request_uid=request_uid,
            api_key_id=reserve.api_key_id,
            key_alias=reserve.key_alias,
            quota_scope=reserve.quota_scope or TTS_QUOTA_SCOPE,
            usage=usage,
        )

    async def _call_provider(
        self,
        *,
        api_key: str,
        model: str,
        prompt: str,
        voice: str,
    ) -> tuple[bytes, str, UsageInfo]:
        sdk = self.gateway.genai_new
        if sdk is None:
            raise RuntimeError("google-genai is required for Gemini TTS")
        client = sdk.Client(api_key=api_key)
        response = await client.aio.models.generate_content(
            model=model,
            contents=prompt,
            config={
                "response_modalities": ["AUDIO"],
                "speech_config": {
                    "voice_config": {
                        "prebuilt_voice_config": {"voice_name": voice}
                    }
                },
            },
        )
        inline = self._first_inline_audio(response)
        data = self._field(inline, "data")
        if isinstance(data, str):
            data = base64.b64decode(data)
        if not isinstance(data, (bytes, bytearray)) or not data:
            raise ProviderError(
                error_type="empty_audio",
                error_message="Provider returned no audio bytes",
                retryable=False,
            )
        mime_type = str(
            self._field(inline, "mime_type")
            or self._field(inline, "mimeType")
            or "audio/L16;codec=pcm;rate=24000"
        )
        usage = self._usage(response)
        return bytes(data), mime_type, usage

    @classmethod
    def _first_inline_audio(cls, response: Any) -> Any:
        candidates = cls._field(response, "candidates") or []
        for candidate in candidates:
            content = cls._field(candidate, "content")
            for part in cls._field(content, "parts") or []:
                inline = cls._field(part, "inline_data") or cls._field(
                    part, "inlineData"
                )
                if inline is not None:
                    return inline
        raise ProviderError(
            error_type="empty_audio",
            error_message="Provider response has no inline audio part",
            retryable=False,
        )

    @staticmethod
    def _field(value: Any, name: str) -> Any:
        if isinstance(value, dict):
            return value.get(name)
        return getattr(value, name, None)

    @classmethod
    def _usage(cls, response: Any) -> UsageInfo:
        meta = cls._field(response, "usage_metadata") or cls._field(
            response, "usageMetadata"
        )
        if not meta:
            return UsageInfo()

        def number(*names: str) -> int:
            for name in names:
                value = cls._field(meta, name)
                if value is not None:
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        return 0
            return 0

        return UsageInfo(
            input_tokens=number("prompt_token_count", "promptTokenCount"),
            output_tokens=number("candidates_token_count", "candidatesTokenCount"),
            total_tokens=number("total_token_count", "totalTokenCount"),
        )

    @staticmethod
    def _sample_rate(mime_type: str) -> int:
        match = re.search(r"(?:^|;)rate=(\d+)", mime_type, flags=re.IGNORECASE)
        return int(match.group(1)) if match else 24_000

    @staticmethod
    def _validate_model(model: str) -> str:
        selected = str(model or "").strip()
        if selected not in SUPPORTED_TTS_MODELS:
            raise ValueError(f"Unsupported Gemini TTS model: {selected}")
        return selected

    @staticmethod
    def _validate_voice(voice: str) -> str:
        selected = str(voice or "").strip()
        if selected not in SUPPORTED_TTS_VOICES:
            raise ValueError(f"Unsupported Gemini TTS voice: {selected}")
        return selected
