"""Construction helpers for the dedicated Google AI quota ledger.

The limiter has its own Supabase project so that quota accounting is not tied
to the bot's general-purpose Supabase project.  Callers may supply an explicit
legacy factory for rollout compatibility, but dedicated configuration always
wins and partial dedicated configuration is an error.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit


GOOGLE_AI_LIMITER_SUPABASE_URL_ENV = "GOOGLE_AI_LIMITER_SUPABASE_URL"
GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY_ENV = (
    "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY"
)
GOOGLE_AI_LIMITER_ALLOW_LEGACY_FALLBACK_ENV = (
    "GOOGLE_AI_LIMITER_ALLOW_LEGACY_FALLBACK"
)


class GoogleAILimiterSupabaseConfigurationError(RuntimeError):
    """Raised before provider use when the limiter backend is misconfigured."""


def _validated_url(raw_url: str) -> str:
    url = raw_url.strip().rstrip("/")
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise GoogleAILimiterSupabaseConfigurationError(
            f"{GOOGLE_AI_LIMITER_SUPABASE_URL_ENV} must be an http(s) origin"
        )
    return url


def build_google_ai_limiter_supabase_client(
    *,
    fallback_factory: Callable[[], Any | None] | None = None,
    require_configured: bool = False,
    environ: Mapping[str, str] | None = None,
    client_factory: Callable[[str, str], Any] | None = None,
) -> Any | None:
    """Build the Supabase client used only for Google AI quota accounting.

    ``GOOGLE_AI_LIMITER_SUPABASE_URL`` and
    ``GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY`` are an atomic pair.  A legacy
    factory is accepted only when the caller explicitly opts into the
    local-development compatibility flag.  Production and remote runtimes must
    never silently move quota accounting to another Supabase project.
    """

    source = os.environ if environ is None else environ
    raw_url = str(source.get(GOOGLE_AI_LIMITER_SUPABASE_URL_ENV, "") or "").strip()
    service_key = str(
        source.get(GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY_ENV, "") or ""
    ).strip()

    if bool(raw_url) != bool(service_key):
        missing = (
            GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY_ENV
            if raw_url
            else GOOGLE_AI_LIMITER_SUPABASE_URL_ENV
        )
        raise GoogleAILimiterSupabaseConfigurationError(
            f"dedicated Google AI limiter configuration is incomplete: missing {missing}"
        )

    if not raw_url:
        allow_legacy_fallback = str(
            source.get(GOOGLE_AI_LIMITER_ALLOW_LEGACY_FALLBACK_ENV, "") or ""
        ).strip().lower() in {"1", "true", "yes", "on"}
        if fallback_factory is not None and allow_legacy_fallback and not require_configured:
            client = fallback_factory()
            if client is not None:
                return client
        if require_configured:
            raise GoogleAILimiterSupabaseConfigurationError(
                "Google AI limiter Supabase is not configured"
            )
        return None

    url = _validated_url(raw_url)
    factory = client_factory
    if factory is None:
        try:
            from supabase import create_client
        except Exception as exc:  # pragma: no cover - deployment dependency guard
            raise GoogleAILimiterSupabaseConfigurationError(
                "supabase package is unavailable for the dedicated Google AI limiter"
            ) from exc
        factory = create_client

    try:
        client = factory(url, service_key)
    except Exception as exc:
        raise GoogleAILimiterSupabaseConfigurationError(
            "failed to construct the dedicated Google AI limiter Supabase client"
        ) from exc
    if client is None:
        raise GoogleAILimiterSupabaseConfigurationError(
            "dedicated Google AI limiter Supabase client factory returned no client"
        )
    return client


__all__ = [
    "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY_ENV",
    "GOOGLE_AI_LIMITER_SUPABASE_URL_ENV",
    "GOOGLE_AI_LIMITER_ALLOW_LEGACY_FALLBACK_ENV",
    "GoogleAILimiterSupabaseConfigurationError",
    "build_google_ai_limiter_supabase_client",
]
