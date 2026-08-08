from __future__ import annotations

from private_events_mcp.auth_store import OAuthStateStore, OAuthStoreError
from private_events_mcp.crypto import pkce_s256


def test_authorization_code_allowed_scope_gate_is_transactional(tmp_path) -> None:
    store = OAuthStateStore(str(tmp_path / "oauth.sqlite"))
    store.create_authorization_code(
        code="stale-code", subject="alice", client_id="codex",
        redirect_uri="http://localhost/callback", resource="https://codex-mcp",
        scopes={"events:read", "telegram:dm:send"},
        code_challenge=pkce_s256("verifier-allowed-scope-gate-12345678901234567890"),
        expires_at=2_000_000_000, now=1_900_000_000,
    )
    try:
        store.consume_authorization_code(
            code="stale-code", client_id="codex", redirect_uri="http://localhost/callback",
            resource="https://codex-mcp",
            code_verifier="verifier-allowed-scope-gate-12345678901234567890",
            allowed_scopes=frozenset({"events:read"}), now=1_900_000_001,
        )
    except OAuthStoreError as exc:
        assert str(exc) == "invalid_scope"
    else:
        raise AssertionError("over-broad authorization code accepted")
    # Rejection happened before used_at, so a matching policy may still consume it.
    grant = store.consume_authorization_code(
        code="stale-code", client_id="codex", redirect_uri="http://localhost/callback",
        resource="https://codex-mcp",
        code_verifier="verifier-allowed-scope-gate-12345678901234567890",
        allowed_scopes=frozenset({"events:read", "telegram:dm:send"}), now=1_900_000_002,
    )
    assert grant.scopes == {"events:read", "telegram:dm:send"}


def test_refresh_allowed_scope_gate_precedes_revocation(tmp_path) -> None:
    store = OAuthStateStore(str(tmp_path / "oauth.sqlite"))
    store.create_refresh_token(
        token="stale-refresh", subject="alice", client_id="codex",
        resource="https://codex-mcp",
        scopes={"offline_access", "telegram:dm:send"},
        expires_at=2_000_000_000, now=1_900_000_000,
    )
    try:
        store.rotate_refresh_token(
            old_token="stale-refresh", new_token="rejected-new", client_id="codex",
            resource="https://codex-mcp", new_expires_at=2_000_000_100,
            allowed_scopes=frozenset({"offline_access"}), now=1_900_000_001,
        )
    except OAuthStoreError as exc:
        assert str(exc) == "invalid_scope"
    else:
        raise AssertionError("over-broad refresh grant accepted")
    grant = store.rotate_refresh_token(
        old_token="stale-refresh", new_token="accepted-new", client_id="codex",
        resource="https://codex-mcp", new_expires_at=2_000_000_100,
        allowed_scopes=frozenset({"offline_access", "telegram:dm:send"}),
        now=1_900_000_002,
    )
    assert grant.scopes == {"offline_access", "telegram:dm:send"}
