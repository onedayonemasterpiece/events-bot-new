from __future__ import annotations

import asyncio
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from .dependencies import optional_import


@lru_cache(maxsize=1)
def _build_supabase_client() -> Any | None:
    from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client

    def legacy_factory() -> Any | None:
        if (os.getenv("SUPABASE_DISABLED") or "").strip() == "1":
            return None
        base_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
        key = (os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
        if not base_url or not key:
            return None
        try:
            from supabase import create_client
            from supabase.client import ClientOptions

            options = ClientOptions()
            options.schema = (os.getenv("SUPABASE_SCHEMA") or "public").strip() or "public"
            return create_client(base_url, key, options=options)
        except Exception:
            return None

    return build_google_ai_limiter_supabase_client(
        fallback_factory=legacy_factory
    )


@lru_cache(maxsize=8)
def get_google_ai_client(default_env_var_name: str = "GOOGLE_API_KEY"):
    google_ai = optional_import("google_ai")
    if google_ai is None:
        return None
    client = google_ai.GoogleAIClient(
        supabase_client=_build_supabase_client(),
        secrets_provider=google_ai.SecretsProvider(),
        consumer="contour_svg",
        account_name=os.getenv("GOOGLE_API_LOCALNAME_CONTOUR") or os.getenv("GOOGLE_API_LOCALNAME"),
        default_env_var_name=default_env_var_name or "GOOGLE_API_KEY",
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    return client


def gateway_status(default_env_var_name: str = "GOOGLE_API_KEY") -> dict[str, Any]:
    client = get_google_ai_client(default_env_var_name)
    provider_sdk = False
    try:
        from google import genai as _genai  # noqa: F401

        provider_sdk = True
    except Exception:
        provider_sdk = False
    registered_default_env_keys: list[str] | None = None
    if client is not None and getattr(client, "supabase", None) is not None:
        try:
            registered_default_env_keys = client._resolve_default_env_candidate_key_ids(  # noqa: SLF001
                consumer="contour_svg"
            )
        except Exception:
            registered_default_env_keys = None
    return {
        "google_ai_client": client is not None,
        "supabase_limiter": bool(getattr(client, "supabase", None)) if client is not None else False,
        "default_env_var_name": default_env_var_name or "GOOGLE_API_KEY",
        "provider_sdk_google_genai": provider_sdk,
        "registered_default_env_key_count": len(registered_default_env_keys or []),
        "reserve_fallback_enabled": bool(getattr(client, "allow_reserve_fallback", True)) if client is not None else True,
        "local_limiter_fallback_enabled": bool(getattr(client, "allow_local_limiter_fallback", True)) if client is not None else True,
        "local_limiter_on_reserve_error_enabled": (
            bool(getattr(client, "allow_local_limiter_on_reserve_error", True)) if client is not None else True
        ),
    }


def assert_gateway_ready(default_env_var_name: str = "GOOGLE_API_KEY") -> dict[str, Any]:
    status = gateway_status(default_env_var_name)
    failures: list[str] = []
    if not status["google_ai_client"]:
        failures.append("google_ai_client")
    if not status["provider_sdk_google_genai"]:
        failures.append("google_genai_sdk")
    if not status["supabase_limiter"]:
        failures.append("supabase_limiter")
    if int(status["registered_default_env_key_count"]) <= 0:
        failures.append("registered_default_env_key")
    for key in [
        "reserve_fallback_enabled",
        "local_limiter_fallback_enabled",
        "local_limiter_on_reserve_error_enabled",
    ]:
        if status[key]:
            failures.append(key)
    if failures:
        raise RuntimeError(f"Contour Gemini gateway preflight failed: {', '.join(failures)}")
    return status


def image_part(path: str | Path, *, mime_type: str = "image/png") -> dict[str, Any]:
    return {
        "inline_data": {
            "mime_type": mime_type,
            "data": Path(path).read_bytes(),
        }
    }


def run_gateway_json_call(
    *,
    model: str,
    prompt: Any,
    generation_config: dict[str, Any],
    default_env_var_name: str,
    max_output_tokens: int,
) -> str:
    client = get_google_ai_client(default_env_var_name)
    if client is None:
        raise RuntimeError("google_ai_gateway_unavailable")
    if getattr(client, "supabase", None) is None:
        raise RuntimeError("google_ai_gateway_requires_supabase_limiter")

    async def _call() -> str:
        text, _usage = await client.generate_content_async(
            model=model,
            prompt=prompt,
            generation_config=generation_config,
            max_output_tokens=max_output_tokens,
        )
        return text

    running_loop = None
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None
    if running_loop is None:
        return asyncio.run(_call())
    raise RuntimeError("contour_svg synchronous pipeline cannot call GoogleAIClient inside an active event loop")
