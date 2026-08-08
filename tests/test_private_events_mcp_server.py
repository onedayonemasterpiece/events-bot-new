from __future__ import annotations

import base64
from dataclasses import replace
import re
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from private_events_mcp.crypto import AccessIdentity, pkce_s256
from private_events_mcp.integration import attach_private_events_mcp


@pytest.mark.asyncio
async def test_oauth_pkce_and_authenticated_mcp_round_trip(config) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        root_metadata = await client.get("/.well-known/oauth-protected-resource")
        assert root_metadata.status == 404
        metadata = await client.get(config.protected_resource_metadata_path)
        assert metadata.status == 200
        metadata_payload = await metadata.json()
        assert metadata_payload["resource"] == config.resource
        assert metadata_payload["authorization_servers"] == [config.issuer]

        authorization_metadata = await client.get(config.authorization_server_metadata_path)
        assert authorization_metadata.status == 200
        assert (await authorization_metadata.json())["issuer"] == config.issuer

        initialize = await client.post(
            config.mcp_path,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"},
            },
        )
        assert initialize.status == 200

        unauthenticated = await client.post(
            config.mcp_path,
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "search", "arguments": {"query": "архитектура"}},
            },
        )
        challenge = (await unauthenticated.json())["result"]["_meta"]["mcp/www_authenticate"]
        assert challenge

        verifier = "z" * 64
        callback = "https://chatgpt.com/connector/oauth/test-callback-id"
        authorize_query = {
            "response_type": "code",
            "client_id": config.oauth_client_id,
            "redirect_uri": callback,
            "state": "state-test-123",
            "resource": config.resource,
            "scope": "events:read incidents:read operations:read offline_access",
            "code_challenge": pkce_s256(verifier),
            "code_challenge_method": "S256",
        }
        page = await client.get(config.oauth_authorize_path + "?" + urlencode(authorize_query))
        assert page.status == 200
        html = await page.text()
        sealed = re.search(r'name="authorization_request" value="([^"]+)"', html)
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
        location = granted.headers["Location"]
        params = parse_qs(urlsplit(location).query)
        assert params["state"] == ["state-test-123"]
        code = params["code"][0]

        basic = base64.b64encode(
            f"{config.oauth_client_id}:{config.oauth_client_secret}".encode()
        ).decode()
        token_response = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": callback,
                "resource": config.resource,
                "code_verifier": verifier,
            },
            headers={"Authorization": f"Basic {basic}"},
        )
        assert token_response.status == 200
        tokens = await token_response.json()
        assert tokens["token_type"] == "Bearer"
        assert tokens["access_token"]
        assert tokens["refresh_token"]

        mcp = await client.post(
            config.mcp_path,
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "search", "arguments": {"query": "архитектура"}},
            },
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        assert mcp.status == 200
        result = await mcp.json()
        assert result["result"]["structuredContent"]["results"][0]["id"] == "event:42"

        refreshed = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "resource": config.resource,
            },
            headers={"Authorization": f"Basic {basic}"},
        )
        assert refreshed.status == 200
        replacement = await refreshed.json()
        assert replacement["refresh_token"] != tokens["refresh_token"]

        replay = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "resource": config.resource,
            },
            headers={"Authorization": f"Basic {basic}"},
        )
        assert replay.status == 400
        assert (await replay.json())["error"] == "invalid_grant"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_invalid_bearer_is_http_401_with_resource_metadata(config, monkeypatch) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            config.mcp_path,
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            headers={"Authorization": "Bearer invalid"},
        )
        assert response.status == 401
        assert "resource_metadata" in response.headers["WWW-Authenticate"]
    finally:
        await client.close()

    monkeypatch.setenv("PRIVATE_EVENTS_MCP_ENABLED", "0")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL", "not-an-absolute-url")
    disabled_app = web.Application()
    assert attach_private_events_mcp(disabled_app) is None


@pytest.mark.asyncio
async def test_jsonrpc_batch_is_rejected_to_preserve_request_budget(config) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            config.mcp_path,
            json=[
                {"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {}},
                {"jsonrpc": "2.0", "id": 2, "method": "ping", "params": {}},
            ],
        )
        assert response.status == 400
        assert (await response.json())["error"] == "jsonrpc_batch_not_supported"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_unsupported_protocol_header_is_rejected(config, monkeypatch) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            config.mcp_path,
            json={"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {}},
            headers={"MCP-Protocol-Version": "2099-01-01"},
        )
        assert response.status == 400
        assert (await response.json())["error"] == "unsupported_mcp_protocol_version"

        response = await client.post(
            config.mcp_path,
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "initialize",
                "params": {"protocolVersion": "2099-01-01"},
            },
        )
        assert response.status == 400
        assert (await response.json())["error"] == "unsupported_mcp_protocol_version"

        response = await client.post(
            config.oauth_token_path,
            data={"grant_type": "x" * (config.max_request_bytes + 1)},
        )
        assert response.status == 413
        assert (await response.json())["error"] == "request_too_large"
    finally:
        await client.close()

    limited_config = replace(config, authenticated_requests_per_minute=1)
    limited_app = web.Application()
    server = attach_private_events_mcp(limited_app, limited_config)
    assert server is not None
    identities = iter(
        (
            AccessIdentity(
                subject="operator",
                client_id=limited_config.oauth_client_id,
                scopes=frozenset(),
                audience=limited_config.resource,
                token_id="first-token-id",
                expires_at=2**31,
            ),
            AccessIdentity(
                subject="operator",
                client_id=limited_config.oauth_client_id,
                scopes=frozenset(),
                audience=limited_config.resource,
                token_id="refreshed-token-id",
                expires_at=2**31,
            ),
        )
    )
    monkeypatch.setattr(server, "_identity", lambda _request: (next(identities), False))
    limited_client = TestClient(TestServer(limited_app))
    await limited_client.start_server()
    try:
        first = await limited_client.post(
            limited_config.mcp_path,
            json={"jsonrpc": "2.0", "id": 3, "method": "ping", "params": {}},
        )
        assert first.status == 200
        second = await limited_client.post(
            limited_config.mcp_path,
            json={"jsonrpc": "2.0", "id": 4, "method": "ping", "params": {}},
        )
        assert second.status == 429
        assert (await second.json())["error"] == "rate_limited"
    finally:
        await limited_client.close()
