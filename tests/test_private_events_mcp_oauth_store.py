from __future__ import annotations

import pytest

from private_events_mcp.auth_store import OAuthStateStore, OAuthStoreError
from private_events_mcp.crypto import pkce_s256


def test_authorization_code_is_pkce_bound_and_single_use(tmp_path) -> None:
    store = OAuthStateStore(str(tmp_path / "oauth.sqlite"))
    verifier = "a" * 64
    store.create_authorization_code(
        code="code-one",
        subject="owner",
        client_id="client",
        redirect_uri="https://chatgpt.com/connector/oauth/callback",
        resource="https://resource.example/mcp",
        scopes={"events:read"},
        code_challenge=pkce_s256(verifier),
        expires_at=2_000,
        now=1_000,
    )
    grant = store.consume_authorization_code(
        code="code-one",
        client_id="client",
        redirect_uri="https://chatgpt.com/connector/oauth/callback",
        resource="https://resource.example/mcp",
        code_verifier=verifier,
        now=1_001,
    )
    assert grant.scopes == frozenset({"events:read"})
    with pytest.raises(OAuthStoreError):
        store.consume_authorization_code(
            code="code-one",
            client_id="client",
            redirect_uri="https://chatgpt.com/connector/oauth/callback",
            resource="https://resource.example/mcp",
            code_verifier=verifier,
            now=1_002,
        )


def test_refresh_token_rotation_rejects_replay(tmp_path) -> None:
    store = OAuthStateStore(str(tmp_path / "oauth.sqlite"))
    store.create_refresh_token(
        token="refresh-one",
        subject="owner",
        client_id="client",
        resource="https://resource.example/mcp",
        scopes={"events:read", "incidents:read"},
        expires_at=5_000,
        now=1_000,
    )
    grant = store.rotate_refresh_token(
        old_token="refresh-one",
        new_token="refresh-two",
        client_id="client",
        resource="https://resource.example/mcp",
        new_expires_at=6_000,
        requested_scopes={"events:read"},
        now=1_001,
    )
    assert grant.scopes == frozenset({"events:read"})
    with pytest.raises(OAuthStoreError):
        store.rotate_refresh_token(
            old_token="refresh-one",
            new_token="refresh-three",
            client_id="client",
            resource="https://resource.example/mcp",
            new_expires_at=7_000,
            now=1_002,
        )
