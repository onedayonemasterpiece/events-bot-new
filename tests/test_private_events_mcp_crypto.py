from __future__ import annotations

import pytest

from private_events_mcp.crypto import (
    TokenValidationError,
    mint_access_token,
    pkce_s256,
    validate_access_token,
)


def test_access_token_is_bound_to_issuer_audience_and_client() -> None:
    token, issued = mint_access_token(
        signing_key="k" * 64,
        issuer="https://issuer.example",
        audience="https://resource.example/mcp",
        subject="owner",
        client_id="chatgpt-client",
        scopes={"events:read"},
        lifetime_seconds=900,
        now=1_000,
    )
    identity = validate_access_token(
        token,
        signing_key="k" * 64,
        issuer="https://issuer.example",
        audience="https://resource.example/mcp",
        now=1_001,
    )
    assert identity.subject == "owner"
    assert identity.client_id == "chatgpt-client"
    assert identity.scopes == frozenset({"events:read"})
    assert identity.token_id == issued.token_id

    with pytest.raises(TokenValidationError):
        validate_access_token(
            token,
            signing_key="k" * 64,
            issuer="https://issuer.example",
            audience="https://other.example/mcp",
            now=1_001,
        )


def test_pkce_s256_is_deterministic() -> None:
    verifier = "v" * 64
    assert pkce_s256(verifier) == pkce_s256(verifier)
    assert len(pkce_s256(verifier)) == 43
