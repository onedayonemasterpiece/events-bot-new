from __future__ import annotations

import base64
from dataclasses import replace
import logging
import re
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from aiohttp import ClientSession, web
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
        authorization_payload = await authorization_metadata.json()
        assert authorization_payload["issuer"] == config.issuer
        assert "none" in authorization_payload["token_endpoint_auth_methods_supported"]
        assert "registration_endpoint" not in authorization_payload

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

        narrowed = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "refresh_token": replacement["refresh_token"],
                "resource": config.resource,
                "scope": "events:read",
            },
            headers={"Authorization": f"Basic {basic}"},
        )
        assert narrowed.status == 200
        narrowed_tokens = await narrowed.json()
        assert narrowed_tokens["scope"] == "events:read"
        assert "refresh_token" not in narrowed_tokens
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_codex_public_client_real_oauth_and_mcp_contract(config) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    verifier = "c" * 64
    callback = "http://127.0.0.1:1455/callback/codex-cli-opaque_123"
    try:
        query = {
            "response_type": "code",
            "client_id": config.codex_oauth_client_id,
            "redirect_uri": callback,
            "state": "codex-state",
            "resource": config.resource,
            "scope": "events:read incidents:read operations:read offline_access",
            "code_challenge": pkce_s256(verifier),
            "code_challenge_method": "S256",
        }
        page = await client.get(config.oauth_authorize_path + "?" + urlencode(query))
        assert page.status == 200
        sealed = re.search(
            r'name="authorization_request" value="([^"]+)",?', await page.text()
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
        redirect = urlsplit(granted.headers["Location"])
        assert f"{redirect.scheme}://{redirect.netloc}{redirect.path}" == callback
        code = parse_qs(redirect.query)["code"][0]

        token_response = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "client_id": config.codex_oauth_client_id,
                "code": code,
                "redirect_uri": callback,
                "resource": config.resource,
                "code_verifier": verifier,
            },
        )
        assert token_response.status == 200
        tokens = await token_response.json()

        listed = await client.post(
            config.mcp_path,
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        assert listed.status == 200
        names = {tool["name"] for tool in (await listed.json())["result"]["tools"]}
        assert names == {
            "search", "fetch", "events_search", "event_get",
            "incidents_search", "incident_get", "operations_snapshot",
        }

        refreshed = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "client_id": config.codex_oauth_client_id,
                "refresh_token": tokens["refresh_token"],
                "resource": config.resource,
            },
        )
        assert refreshed.status == 200
        assert (await refreshed.json())["refresh_token"] != tokens["refresh_token"]
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "redirect_uri",
    [
        "http://localhost:1455/callback/opaque",
        "http://127.0.0.1/callback/opaque",
        "https://127.0.0.1:1455/callback/opaque",
        "http://user@127.0.0.1:1455/callback/opaque",
        "http://127.0.0.1:01455/callback/opaque",
        "http://127.0.0.1:1455/callback",
        "http://127.0.0.1:1455/callback/opaque/extra",
        "http://127.0.0.1:1455/callback/.",
        "http://127.0.0.1:1455/callback/..",
        "http://127.0.0.1:1455/callback/%2e",
        "http://127.0.0.1:1455/callback/%2e%2e",
        "http://127.0.0.1:1455/callback/opaque?query=1",
        "http://127.0.0.1:1455/callback/opaque#fragment",
    ],
)
async def test_codex_redirect_contract_rejects_non_literal_variants(
    config, redirect_uri
) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.get(
            config.oauth_authorize_path
            + "?"
            + urlencode(
                {
                    "response_type": "code",
                    "client_id": config.codex_oauth_client_id,
                    "redirect_uri": redirect_uri,
                    "state": "state",
                    "resource": config.resource,
                    "code_challenge": pkce_s256("v" * 64),
                    "code_challenge_method": "S256",
                }
            )
        )
        assert response.status == 400
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_public_client_rejects_secret_downgrade_and_cross_client_code(config) -> None:
    app = web.Application()
    server = attach_private_events_mcp(app, config)
    assert server is not None
    client = TestClient(TestServer(app))
    await client.start_server()
    callback = "http://127.0.0.1:8123/callback/cross-client"
    verifier = "x" * 64
    code = "cross-client-code"
    server.oauth.store.create_authorization_code(
        code=code,
        subject="events-bot-owner",
        client_id=config.codex_oauth_client_id,
        redirect_uri=callback,
        resource=config.resource,
        scopes={"events:read"},
        code_challenge=pkce_s256(verifier),
        expires_at=2**31,
    )
    try:
        downgraded = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "client_id": config.codex_oauth_client_id,
                "client_secret": config.oauth_client_secret,
                "code": code,
                "redirect_uri": callback,
                "resource": config.resource,
                "code_verifier": verifier,
            },
        )
        assert downgraded.status == 401
        assert (await downgraded.json())["error"] == "invalid_client"

        basic = base64.b64encode(
            f"{config.oauth_client_id}:{config.oauth_client_secret}".encode()
        ).decode()
        crossed = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": "https://chatgpt.com/connector/oauth/cross-client",
                "resource": config.resource,
                "code_verifier": verifier,
            },
            headers={"Authorization": f"Basic {basic}"},
        )
        assert crossed.status == 400
        assert (await crossed.json())["error"] == "invalid_grant"

        valid = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "client_id": config.codex_oauth_client_id,
                "code": code,
                "redirect_uri": callback,
                "resource": config.resource,
                "code_verifier": verifier,
            },
        )
        assert valid.status == 200
        valid_tokens = await valid.json()
        assert valid_tokens["scope"] == "events:read"
        assert "refresh_token" not in valid_tokens
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_omitted_scope_uses_read_only_registered_default(config) -> None:
    app = web.Application()
    server = attach_private_events_mcp(app, config)
    assert server is not None
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.get(
            config.oauth_authorize_path
            + "?"
            + urlencode(
                {
                    "response_type": "code",
                    "client_id": config.codex_oauth_client_id,
                    "redirect_uri": "http://127.0.0.1:9000/callback/default-scope",
                    "state": "state",
                    "resource": config.resource,
                    "code_challenge": pkce_s256("d" * 64),
                    "code_challenge_method": "S256",
                }
            )
        )
        assert response.status == 200
        sealed = re.search(
            r'name="authorization_request" value="([^"]+)"', await response.text()
        )
        assert sealed
        parsed = server.oauth._unseal_authorization_request(sealed.group(1))
        assert parsed.scopes == frozenset(
            {"events:read", "incidents:read", "operations:read"}
        )
        assert "offline_access" not in parsed.scopes
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_invalid_bearer_is_http_401_with_resource_metadata(
    config, monkeypatch, caplog
) -> None:
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

    access_app = web.Application()
    attach_private_events_mcp(access_app, config)
    runner = web.AppRunner(
        access_app,
        access_log_format='AUTH=%{Authorization}i REQUEST="%r"',
    )
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    basic_value = base64.b64encode(
        f"{config.oauth_client_id}:{config.oauth_client_secret}".encode()
    ).decode()
    bearer_value = "access-token-that-must-never-log-1234567890"
    with caplog.at_level(logging.INFO, logger="aiohttp.access"):
        async with ClientSession() as session:
            for authorization in (f"Basic {basic_value}", f"Bearer {bearer_value}"):
                response = await session.post(
                    f"http://127.0.0.1:{port}{config.mcp_path}",
                    json={"jsonrpc": "2.0", "id": 2, "method": "ping", "params": {}},
                    headers={"Authorization": authorization},
                )
                assert response.status == 401
                await response.read()
        logging.getLogger("aiohttp.access").info(
            'request="POST %s" operator=%s signing=%s',
            config.mcp_path,
            config.operator_token,
            config.signing_key,
        )
    await runner.cleanup()
    for forbidden in (
        config.path_secret,
        config.oauth_client_secret,
        config.operator_token,
        config.signing_key,
        basic_value,
        bearer_value,
    ):
        assert forbidden not in caplog.text
    assert "Basic <redacted>" in caplog.text
    assert "Bearer <redacted>" in caplog.text
    assert "/_private/<redacted>/mcp" in caplog.text

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
