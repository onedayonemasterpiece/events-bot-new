from __future__ import annotations

import pytest

from google_ai.limiter_supabase import (
    GoogleAILimiterSupabaseConfigurationError,
    build_google_ai_limiter_supabase_client,
)


URL_ENV = "GOOGLE_AI_LIMITER_SUPABASE_URL"
KEY_ENV = "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY"


def test_dedicated_pair_wins_without_constructing_legacy_client() -> None:
    calls: list[tuple[str, str]] = []

    def legacy_factory():
        raise AssertionError("legacy fallback must not be evaluated")

    dedicated = object()
    result = build_google_ai_limiter_supabase_client(
        environ={URL_ENV: " https://limiter.example.test/ ", KEY_ENV: "service-secret"},
        fallback_factory=legacy_factory,
        client_factory=lambda url, key: calls.append((url, key)) or dedicated,
    )

    assert result is dedicated
    assert calls == [("https://limiter.example.test", "service-secret")]


@pytest.mark.parametrize(
    "environ, missing",
    [
        ({URL_ENV: "https://limiter.example.test"}, KEY_ENV),
        ({KEY_ENV: "service-secret"}, URL_ENV),
    ],
)
def test_partial_dedicated_pair_fails_closed_without_fallback(
    environ: dict[str, str], missing: str
) -> None:
    fallback_called = False

    def legacy_factory():
        nonlocal fallback_called
        fallback_called = True
        return object()

    with pytest.raises(GoogleAILimiterSupabaseConfigurationError, match=missing):
        build_google_ai_limiter_supabase_client(
            environ=environ,
            fallback_factory=legacy_factory,
            client_factory=lambda _url, _key: object(),
        )

    assert fallback_called is False


def test_absent_dedicated_pair_does_not_use_legacy_factory_by_default() -> None:
    legacy = object()
    fallback_called = False

    def legacy_factory():
        nonlocal fallback_called
        fallback_called = True
        return legacy

    assert build_google_ai_limiter_supabase_client(
        environ={}, fallback_factory=legacy_factory
    ) is None
    assert fallback_called is False
    assert build_google_ai_limiter_supabase_client(environ={}) is None


def test_absent_dedicated_pair_allows_explicit_local_legacy_opt_in() -> None:
    legacy = object()

    assert build_google_ai_limiter_supabase_client(
        environ={"GOOGLE_AI_LIMITER_ALLOW_LEGACY_FALLBACK": "1"},
        fallback_factory=lambda: legacy,
    ) is legacy


def test_required_backend_rejects_absent_or_empty_legacy_client() -> None:
    with pytest.raises(
        GoogleAILimiterSupabaseConfigurationError,
        match="not configured",
    ):
        build_google_ai_limiter_supabase_client(
            environ={}, fallback_factory=lambda: None, require_configured=True
        )

    with pytest.raises(
        GoogleAILimiterSupabaseConfigurationError,
        match="not configured",
    ):
        build_google_ai_limiter_supabase_client(
            environ={"GOOGLE_AI_LIMITER_ALLOW_LEGACY_FALLBACK": "1"},
            fallback_factory=lambda: object(),
            require_configured=True,
        )


@pytest.mark.parametrize(
    "url",
    [
        "limiter.example.test",
        "ftp://limiter.example.test",
        "https://user@example.test",
        "https://limiter.example.test/not-an-origin",
        "https://limiter.example.test?project=other",
        "https://limiter.example.test#fragment",
    ],
)
def test_invalid_dedicated_url_fails_before_client_construction(url: str) -> None:
    called = False

    def client_factory(_url: str, _key: str):
        nonlocal called
        called = True
        return object()

    with pytest.raises(
        GoogleAILimiterSupabaseConfigurationError,
        match=r"must be an http\(s\) origin",
    ):
        build_google_ai_limiter_supabase_client(
            environ={URL_ENV: url, KEY_ENV: "service-secret"},
            client_factory=client_factory,
        )

    assert called is False


def test_client_construction_error_is_sanitized() -> None:
    secret = "service-secret-must-not-leak"

    def broken_factory(_url: str, key: str):
        raise ValueError(f"bad key: {key}")

    with pytest.raises(GoogleAILimiterSupabaseConfigurationError) as caught:
        build_google_ai_limiter_supabase_client(
            environ={URL_ENV: "https://limiter.example.test", KEY_ENV: secret},
            client_factory=broken_factory,
        )

    assert secret not in str(caught.value)
