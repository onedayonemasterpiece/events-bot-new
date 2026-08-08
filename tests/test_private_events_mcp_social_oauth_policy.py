from __future__ import annotations

import base64
import re
import sqlite3
import time
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from private_events_mcp.access_policy import (
    APPROVAL_REQUIRED_SOCIAL_SCOPES,
    CHATGPT_DEFAULT_SCOPES,
    CHATGPT_MAX_SCOPES,
    CODEX_DEFAULT_SCOPES,
    CODEX_MAX_SCOPES,
    GRANULAR_SOCIAL_SCOPES,
    LEGACY_PUBLISH_SCOPES,
    LEGACY_SOCIAL_SCOPES,
    READ_SCOPES,
)
from private_events_mcp.config import PrivateEventsMCPConfig
from private_events_mcp.crypto import (
    TokenValidationError,
    mint_access_token,
    pkce_s256,
    secret_hash,
)
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.social_workspace import (
    SOCIAL_WORKSPACE_SCOPES,
    required_scope_for_action,
    required_scope_for_read,
)


_SOCIAL_SWITCH_ENV = {
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_ENABLED": "universal_social_enabled",
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_TELEGRAM_ENABLED": (
        "universal_social_telegram_enabled"
    ),
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_VK_ENABLED": "universal_social_vk_enabled",
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_PRIVATE_READ_ENABLED": (
        "universal_social_private_read_enabled"
    ),
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_DM_ENABLED": "universal_social_dm_enabled",
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_POST_ENABLED": "universal_social_post_enabled",
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_EDIT_DELETE_ENABLED": (
        "universal_social_edit_delete_enabled"
    ),
    "PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_MEDIA_STORY_ENABLED": (
        "universal_social_media_story_enabled"
    ),
}


def _enabled_env(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "PRIVATE_EVENTS_MCP_ENABLED": "1",
        "PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL": "https://events.example",
        "PRIVATE_EVENTS_MCP_PATH_SECRET": "p" * 32,
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_ID": "chatgpt-client",
        "PRIVATE_EVENTS_MCP_OAUTH_CLIENT_SECRET": "s" * 32,
        "PRIVATE_EVENTS_MCP_CODEX_OAUTH_CLIENT_ID": "codex-client",
        "PRIVATE_EVENTS_MCP_OPERATOR_TOKEN": "o" * 32,
        "PRIVATE_EVENTS_MCP_SIGNING_KEY": "k" * 43,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _authorization_query(
    config: PrivateEventsMCPConfig,
    *,
    codex: bool = False,
    scope: str | None = None,
    resource: str | None = None,
    verifier: str = "v" * 64,
    callback: str | None = None,
) -> tuple[dict[str, str], str, str]:
    client_id = config.codex_oauth_client_id if codex else config.oauth_client_id
    resolved_resource = resource or (
        config.codex_resource if codex else config.resource
    )
    resolved_callback = callback or (
        "http://127.0.0.1:8123/callback/social-policy"
        if codex
        else "https://chatgpt.com/connector/oauth/social-policy"
    )
    query = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": resolved_callback,
        "state": "social-policy-state",
        "resource": resolved_resource,
        "code_challenge": pkce_s256(verifier),
        "code_challenge_method": "S256",
    }
    if scope is not None:
        query["scope"] = scope
    return query, resolved_callback, verifier


async def _authorize_code(
    client: TestClient,
    config: PrivateEventsMCPConfig,
    *,
    scope: str,
    verifier: str = "v" * 64,
) -> tuple[str, str]:
    query, callback, _ = _authorization_query(
        config,
        scope=scope,
        verifier=verifier,
    )
    page = await client.get(config.oauth_authorize_path + "?" + urlencode(query))
    assert page.status == 200
    sealed = re.search(
        r'name="authorization_request" value="([^"]+)"', await page.text()
    )
    assert sealed
    granted = await client.post(
        config.oauth_authorize_path,
        data={
            "authorization_request": sealed.group(1),
            "operator_token": config.operator_token,
        },
        allow_redirects=False,
    )
    assert granted.status == 302
    return parse_qs(urlsplit(granted.headers["Location"]).query)["code"][0], callback


def _chatgpt_basic(config: PrivateEventsMCPConfig) -> str:
    encoded = base64.b64encode(
        f"{config.oauth_client_id}:{config.oauth_client_secret}".encode()
    ).decode()
    return f"Basic {encoded}"


def test_scope_registry_preserves_defaults_and_exact_codex_boundary() -> None:
    assert CHATGPT_DEFAULT_SCOPES == READ_SCOPES == frozenset(
        {"events:read", "incidents:read", "operations:read"}
    )
    assert CODEX_DEFAULT_SCOPES == READ_SCOPES
    assert CODEX_MAX_SCOPES == READ_SCOPES | {"offline_access"}
    assert GRANULAR_SOCIAL_SCOPES == SOCIAL_WORKSPACE_SCOPES
    assert GRANULAR_SOCIAL_SCOPES <= CHATGPT_MAX_SCOPES
    assert LEGACY_SOCIAL_SCOPES <= CHATGPT_MAX_SCOPES
    assert "offline_access" in CHATGPT_MAX_SCOPES
    assert CODEX_MAX_SCOPES.isdisjoint(GRANULAR_SOCIAL_SCOPES)
    assert CODEX_MAX_SCOPES.isdisjoint(LEGACY_SOCIAL_SCOPES)
    assert APPROVAL_REQUIRED_SOCIAL_SCOPES.isdisjoint(CHATGPT_DEFAULT_SCOPES)


@pytest.mark.asyncio
async def test_omitted_scope_is_evidence_reads_only_for_both_clients(config) -> None:
    app = web.Application()
    server = attach_private_events_mcp(app, config)
    assert server is not None
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        for codex in (False, True):
            query, _, _ = _authorization_query(config, codex=codex)
            response = await client.get(
                config.oauth_authorize_path + "?" + urlencode(query)
            )
            assert response.status == 200
            body = await response.text()
            sealed = re.search(
                r'name="authorization_request" value="([^"]+)"', body
            )
            assert sealed
            parsed = server.oauth._unseal_authorization_request(sealed.group(1))
            assert parsed.scopes == READ_SCOPES
            assert "offline_access" not in parsed.scopes
            assert "Scopes: <code>events:read, incidents:read, operations:read</code>" in body
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_granular_social_authorization_is_chatgpt_only_and_ui_is_exact(config) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    requested = "vk:story:read telegram:delete"
    try:
        chatgpt_query, _, _ = _authorization_query(config, scope=requested)
        accepted = await client.get(
            config.oauth_authorize_path + "?" + urlencode(chatgpt_query)
        )
        assert accepted.status == 200
        body = await accepted.text()
        assert "Scopes: <code>telegram:delete, vk:story:read</code>" in body
        assert "внешнего подтверждения оператора" in body
        assert "telegram:edit" not in body

        codex_query, _, _ = _authorization_query(
            config,
            codex=True,
            scope=requested,
        )
        denied = await client.get(
            config.oauth_authorize_path + "?" + urlencode(codex_query)
        )
        assert denied.status == 400
        assert "Requested scope is not available" in await denied.text()

        unknown_query, _, _ = _authorization_query(
            config,
            scope="telegram:read:public telegram:provider-native-method",
        )
        unknown = await client.get(
            config.oauth_authorize_path + "?" + urlencode(unknown_query)
        )
        assert unknown.status == 400
        assert "Requested scope is not available" in await unknown.text()

        crossed_query, _, _ = _authorization_query(
            config,
            scope="telegram:read:public",
            resource=config.codex_resource,
        )
        crossed = await client.get(
            config.oauth_authorize_path + "?" + urlencode(crossed_query)
        )
        assert crossed.status == 400
        assert "resource parameter is invalid" in await crossed.text()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_legacy_coarse_scopes_are_not_granular_power_aliases(config) -> None:
    assert LEGACY_SOCIAL_SCOPES.isdisjoint(GRANULAR_SOCIAL_SCOPES)
    legacy_read = frozenset({"telegram:read"})
    legacy_publish = frozenset({"telegram:publish"})
    assert not required_scope_for_read("telegram", "get_item", "private").issubset(
        legacy_read
    )
    for action in ("send_message", "edit", "delete", "story"):
        assert not required_scope_for_action("telegram", action).issubset(
            legacy_publish
        )

    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        query, _, _ = _authorization_query(
            config,
            scope="telegram:read telegram:publish",
        )
        response = await client.get(
            config.oauth_authorize_path + "?" + urlencode(query)
        )
        assert response.status == 200
        body = await response.text()
        assert "Scopes: <code>telegram:publish, telegram:read</code>" in body
        assert "telegram:edit" not in body
        assert "telegram:delete" not in body
        assert "telegram:dm:send" not in body
        assert "telegram:story:write" not in body
        assert "Legacy publish-scopes" in body
        assert "отдельного внешнего подтверждения у них нет" in body
        assert not (LEGACY_PUBLISH_SCOPES & APPROVAL_REQUIRED_SOCIAL_SCOPES)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_code_token_and_refresh_stay_client_resource_and_scope_bound(config) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    verifier = "r" * 64
    granted_scope = "offline_access telegram:read:public"
    try:
        code, callback = await _authorize_code(
            client,
            config,
            scope=granted_scope,
            verifier=verifier,
        )

        crossed_resource = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": callback,
                "resource": config.codex_resource,
                "code_verifier": verifier,
            },
            headers={"Authorization": _chatgpt_basic(config)},
        )
        assert crossed_resource.status == 400
        assert (await crossed_resource.json())["error"] == "invalid_target"

        crossed_client = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "client_id": config.codex_oauth_client_id,
                "code": code,
                "redirect_uri": "http://127.0.0.1:8123/callback/social-policy",
                "resource": config.codex_resource,
                "code_verifier": verifier,
            },
        )
        assert crossed_client.status == 400
        assert (await crossed_client.json())["error"] == "invalid_grant"

        issued = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": callback,
                "resource": config.resource,
                "code_verifier": verifier,
            },
            headers={"Authorization": _chatgpt_basic(config)},
        )
        assert issued.status == 200
        tokens = await issued.json()
        assert tokens["scope"] == "offline_access telegram:read:public"

        cross_client_refresh = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "client_id": config.codex_oauth_client_id,
                "refresh_token": tokens["refresh_token"],
                "resource": config.codex_resource,
            },
        )
        assert cross_client_refresh.status == 400
        assert (await cross_client_refresh.json())["error"] == "invalid_grant"

        broadened = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "resource": config.resource,
                "scope": "offline_access telegram:read:public telegram:delete",
            },
            headers={"Authorization": _chatgpt_basic(config)},
        )
        assert broadened.status == 400
        assert (await broadened.json())["error"] == "invalid_scope"

        refreshed = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "resource": config.resource,
            },
            headers={"Authorization": _chatgpt_basic(config)},
        )
        assert refreshed.status == 200
        refreshed_tokens = await refreshed.json()
        assert refreshed_tokens["scope"] == tokens["scope"]
        assert refreshed_tokens["refresh_token"] != tokens["refresh_token"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_persisted_invalid_code_and_refresh_scopes_fail_before_consumption(config) -> None:
    app = web.Application()
    server = attach_private_events_mcp(app, config)
    assert server is not None
    client = TestClient(TestServer(app))
    await client.start_server()
    now = int(time.time())
    verifier = "z" * 64
    code = "corrupt-code-social-policy"
    refresh = "corrupt-refresh-social-policy"
    corrupt_scopes = frozenset(
        {"offline_access", "telegram:delete", "unknown:scope"}
    )
    try:
        server.oauth.store.create_authorization_code(
            code=code,
            subject="events-bot-owner",
            client_id=config.codex_oauth_client_id,
            redirect_uri="http://127.0.0.1:8123/callback/social-policy",
            resource=config.codex_resource,
            scopes=corrupt_scopes,
            code_challenge=pkce_s256(verifier),
            expires_at=now + 600,
        )
        denied_code = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "client_id": config.codex_oauth_client_id,
                "code": code,
                "redirect_uri": "http://127.0.0.1:8123/callback/social-policy",
                "resource": config.codex_resource,
                "code_verifier": verifier,
            },
        )
        assert denied_code.status == 400
        assert (await denied_code.json())["error"] == "invalid_scope"

        server.oauth.store.create_refresh_token(
            token=refresh,
            subject="events-bot-owner",
            client_id=config.codex_oauth_client_id,
            resource=config.codex_resource,
            scopes=corrupt_scopes,
            expires_at=now + 3600,
        )
        denied_refresh = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "client_id": config.codex_oauth_client_id,
                "refresh_token": refresh,
                "resource": config.codex_resource,
            },
        )
        assert denied_refresh.status == 400
        assert (await denied_refresh.json())["error"] == "invalid_scope"

        with sqlite3.connect(config.auth_database_path) as conn:
            # Inspect by the public store hashing contract without exposing either
            # credential in failure output.
            code_state = conn.execute(
                "SELECT used_at FROM oauth_authorization_code WHERE code_hash=?",
                (secret_hash(code),),
            ).fetchone()
            refresh_state = conn.execute(
                "SELECT revoked_at, rotated_to_hash FROM oauth_refresh_token WHERE token_hash=?",
                (secret_hash(refresh),),
            ).fetchone()
        assert code_state == (None,)
        assert refresh_state == (None, None)
    finally:
        await client.close()


def test_bearer_verification_rejects_codex_social_or_unknown_scopes(config) -> None:
    app = web.Application()
    server = attach_private_events_mcp(app, config)
    assert server is not None
    for client_id, resource, scopes in (
        (
            config.codex_oauth_client_id,
            config.codex_resource,
            {"events:read", "telegram:read:public"},
        ),
        (config.oauth_client_id, config.resource, {"events:read", "unknown:scope"}),
    ):
        token, _ = mint_access_token(
            signing_key=config.signing_key,
            issuer=config.issuer,
            audience=resource,
            subject="events-bot-owner",
            client_id=client_id,
            scopes=scopes,
            lifetime_seconds=300,
        )
        with pytest.raises(TokenValidationError, match="wrong_scope"):
            server.oauth.verify_authorization_header(
                f"Bearer {token}", expected_resource=resource
            )


def test_social_kill_switches_default_off_and_parse_strictly(monkeypatch) -> None:
    _enabled_env(monkeypatch)
    config = PrivateEventsMCPConfig.from_env()
    assert all(not getattr(config, field) for field in _SOCIAL_SWITCH_ENV.values())

    for name in _SOCIAL_SWITCH_ENV:
        monkeypatch.setenv(name, "true")
    configured = PrivateEventsMCPConfig.from_env()
    assert all(getattr(configured, field) for field in _SOCIAL_SWITCH_ENV.values())

    monkeypatch.setenv("PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_DM_ENABLED", "enabled")
    with pytest.raises(
        ValueError,
        match="PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_DM_ENABLED must be an explicit boolean",
    ):
        PrivateEventsMCPConfig.from_env()


def test_disabled_mcp_tolerates_malformed_social_switches(monkeypatch) -> None:
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_ENABLED", "0")
    for name in _SOCIAL_SWITCH_ENV:
        monkeypatch.setenv(name, "not-a-boolean")
    config = PrivateEventsMCPConfig.from_env()
    assert config.enabled is False
    assert all(not getattr(config, field) for field in _SOCIAL_SWITCH_ENV.values())
