from __future__ import annotations

import base64
import json
import logging
import re
from dataclasses import replace
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient, TestServer

from private_events_mcp.crypto import AccessIdentity, pkce_s256
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.social_workspace import validate_prepare_request
from private_events_mcp.social_workspace_runtime import RuntimePrincipal
from private_events_mcp.tool_catalog import ToolCallContext


class _WorkspaceAdapter:
    document_send_supported = True

    async def capabilities(self, target_ref):
        return {}

    async def resolve(self, request):
        return {}

    async def read(self, request):
        return {}

    async def execute(self, intent, *, operation_ref):
        return {
            "target_ref": intent.target_ref,
            "item_ref": "native-sent-message",
            "status": "succeeded",
            "retry_safe": False,
            "read_after_write": {
                "verified": True,
                "observed_item_ref": "native-sent-message",
                "observed_at": "2026-08-08T12:00:00Z",
            },
        }

    async def reconcile(self, operation_ref):
        return {}

    async def stage_asset(self, asset, *, role):
        return "provider-asset"

    async def read_asset(self, asset_ref, *, owner_binding, max_bytes):
        return b"image"


class _DocumentIngestor:
    async def ingest(self, file, **kwargs):
        raise AssertionError("catalogue construction must not ingest")

    def reverify(self, storage_ref, **kwargs):
        raise AssertionError("catalogue construction must not reverify")


def test_universal_social_catalog_is_chatgpt_only_and_adapter_set_is_exact(
    config,
) -> None:
    universal = replace(
        config,
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_vk_enabled=True,
        universal_social_private_read_enabled=True,
        universal_social_dm_enabled=True,
    )
    app = web.Application()
    server = attach_private_events_mcp(
        app,
        universal,
        social_workspace_adapters={
            "telegram": _WorkspaceAdapter(),
            "vk": _WorkspaceAdapter(),
        },
    )
    assert server is not None
    chatgpt_names = {tool.name for tool in server.protocol.tools}
    codex_names = {tool.name for tool in server.codex_protocol.tools}
    assert "social_target_resolve" in chatgpt_names
    assert "social_action_prepare" in chatgpt_names
    assert "social_action_commit" in chatgpt_names
    assert len(codex_names) == 7
    assert not any(name.startswith("social_") for name in codex_names)

    with pytest.raises(ValueError, match="adapter set"):
        attach_private_events_mcp(web.Application(), universal)


def test_file_only_server_advertises_document_ingress_to_chatgpt_not_codex(
    config,
) -> None:
    enabled = replace(
        config,
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_dm_enabled=True,
        universal_social_file_send_enabled=True,
        media_allowed_hosts=("files.example",),
    )
    server = attach_private_events_mcp(
        web.Application(),
        enabled,
        social_workspace_adapters={"telegram": _WorkspaceAdapter()},
        asset_ingestor=_DocumentIngestor(),
    )
    assert server is not None
    stage = next(
        tool for tool in server.protocol.tools if tool.name == "social_asset_stage"
    )
    assert stage.input_schema["properties"]["role"]["enum"] == ["document"]
    assert not any(
        tool.name.startswith("social_") for tool in server.codex_protocol.tools
    )


@pytest.mark.asyncio
async def test_existing_chatgpt_legacy_scopes_see_typed_tools_without_codex_social(
    config,
) -> None:
    universal = replace(
        config,
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_vk_enabled=True,
        universal_social_private_read_enabled=True,
        universal_social_dm_enabled=True,
        universal_social_post_enabled=True,
        universal_social_edit_delete_enabled=True,
        universal_social_media_story_enabled=True,
        media_allowed_hosts=("files.example",),
    )
    server = attach_private_events_mcp(
        web.Application(),
        universal,
        social_workspace_adapters={
            "telegram": _WorkspaceAdapter(),
            "vk": _WorkspaceAdapter(),
        },
        asset_ingestor=object(),
    )
    assert server is not None
    legacy_read = AccessIdentity(
        "operator",
        universal.oauth_client_id,
        frozenset({"telegram:read", "vk:read"}),
        universal.resource,
        "legacy-read-jti",
        2_000_000_000,
    )
    legacy_publish = AccessIdentity(
        "operator",
        universal.oauth_client_id,
        frozenset({"telegram:publish"}),
        universal.resource,
        "legacy-publish-jti",
        2_000_000_000,
    )
    request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    read_list = await server.protocol.dispatch(request, legacy_read)
    publish_list = await server.protocol.dispatch(request, legacy_publish)
    read_names = {tool["name"] for tool in read_list["result"]["tools"]}
    publish_names = {tool["name"] for tool in publish_list["result"]["tools"]}
    assert "social_target_resolve" in read_names
    assert "social_content_feed" in read_names
    assert "social_dialogs_list" in read_names
    assert "social_action_prepare" not in read_names
    assert "social_action_prepare" in publish_names
    assert "social_action_commit" in publish_names
    assert "social_content_feed" not in publish_names

    codex = AccessIdentity(
        "operator",
        universal.codex_oauth_client_id,
        frozenset({"events:read", "incidents:read", "operations:read"}),
        universal.codex_resource,
        "codex-jti",
        2_000_000_000,
    )
    codex_list = await server.codex_protocol.dispatch(request, codex)
    assert len(codex_list["result"]["tools"]) == 7
    assert not any(
        tool["name"].startswith("social_") for tool in codex_list["result"]["tools"]
    )


@pytest.mark.asyncio
async def test_social_approval_page_hides_preview_until_operator_auth_and_commits(
    config,
) -> None:
    universal = replace(
        config,
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_dm_enabled=True,
        social_approval_token="approval_" + "a" * 48,
    )
    app = web.Application()
    server = attach_private_events_mcp(
        app,
        universal,
        social_workspace_adapters={"telegram": _WorkspaceAdapter()},
    )
    assert server is not None and server.social_workspace is not None
    identity = AccessIdentity(
        "operator",
        universal.oauth_client_id,
        frozenset({"telegram:edit"}),
        universal.resource,
        "approval-jti",
        2_000_000_000,
    )
    context = ToolCallContext(identity, universal.resource)
    principal = RuntimePrincipal.from_context(context)
    target = server.social_workspace._mint_ref(
        "target", "native-saved", "telegram", principal
    )
    server.social_workspace._store_target_preview(
        target,
        {
            "platform": "telegram",
            "target_ref": target,
            "kind": "self",
            "display_name": "Saved Messages",
        },
    )
    item = server.social_workspace._mint_ref(
        "item", "native-saved-message", "telegram", principal
    )
    server.social_workspace._store_item_preview(
        item,
        {
            "item_ref": item,
            "target_ref": target,
            "kind": "message",
            "text": "Исходный текст",
        },
    )
    prepared = await server.social_workspace.prepare(
        validate_prepare_request(
            {
                "platform": "telegram",
                "action": "edit",
                "idempotency_key": "approval-flow-123",
                "item_ref": item,
                "content": {
                    "text": "Исправленный текст",
                    "entities": [],
                    "media": [],
                },
            }
        ),
        context,
    )
    assert prepared["approval_url"].startswith(universal.social_approval_url)

    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.get(
            universal.social_approval_path,
            params={
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
        )
        page = await response.text()
        assert response.status == 200 and "Исправленный текст" not in page
        assert "form-action 'self'" in response.headers["Content-Security-Policy"]
        state = re.search(
            r"name=['\"]state['\"] value=['\"]([^'\"]+)['\"]", page
        ).group(1)

        invalid = await client.get(
            universal.social_approval_path,
            params={"preparation_ref": "invalid", "action_digest": "invalid"},
        )
        invalid_page = await invalid.text()
        assert invalid.status == 400 and "Исправленный текст" not in invalid_page
        assert "form-action 'self'" in invalid.headers["Content-Security-Policy"]
        assert invalid.headers["Cache-Control"] == "no-store"

        invalid_state = await client.post(
            universal.social_approval_path,
            data={"phase": "preview", "state": "not-signed", "operator_token": "wrong"},
        )
        assert invalid_state.status == 400
        assert "Исправленный текст" not in await invalid_state.text()
        assert "form-action 'self'" in invalid_state.headers["Content-Security-Policy"]

        denied = await client.post(
            universal.social_approval_path,
            data={"phase": "preview", "state": state, "operator_token": "wrong"},
        )
        assert "Исправленный текст" not in await denied.text()

        preview = await client.post(
            universal.social_approval_path,
            data={
                "phase": "preview",
                "state": state,
                "operator_token": universal.social_approval_token,
            },
        )
        preview_page = await preview.text()
        assert "Исправленный текст" in preview_page and "Saved Messages" in preview_page
        cookie = preview.headers["Set-Cookie"].split(";", 1)[0]

        approved = await client.post(
            universal.social_approval_path,
            data={"phase": "approve", "state": state},
            headers={"Cookie": cookie},
        )
        assert approved.status == 200
        assert "Действие подтверждено" in await approved.text()
    finally:
        await client.close()

    result = await server.social_workspace.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context,
    )
    assert result["status"] == "succeeded"


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

        authorization_metadata = await client.get(
            config.authorization_server_metadata_path
        )
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
        assert initialize.status == 401
        initialize_challenge = initialize.headers["WWW-Authenticate"]
        assert initialize_challenge.startswith("Bearer ")
        assert "resource_metadata" in initialize_challenge

        unauthenticated = await client.post(
            config.mcp_path,
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "search", "arguments": {"query": "архитектура"}},
            },
        )
        assert unauthenticated.status == 401
        challenge = unauthenticated.headers["WWW-Authenticate"]
        assert challenge.startswith("Bearer ")
        assert "resource_metadata" in challenge

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
        page = await client.get(
            config.oauth_authorize_path + "?" + urlencode(authorize_query)
        )
        assert page.status == 200
        csp = page.headers["Content-Security-Policy"]
        assert "form-action 'self' https://chatgpt.com" in csp
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
async def test_chatgpt_redirect_authority_is_canonical_and_not_reflected_in_csp(
    config,
) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    base_query = {
        "response_type": "code",
        "client_id": config.oauth_client_id,
        "state": "state-test-123",
        "resource": config.resource,
        "scope": "events:read",
        "code_challenge": pkce_s256("z" * 64),
        "code_challenge_method": "S256",
    }
    malicious_marker = "img-src evil.example"
    try:
        for redirect_uri in (
            "https://x;img-src evil.example@chatgpt.com/connector/oauth/callback-id",
            "https://chatgpt.com:444/connector/oauth/callback-id",
            "https://chatgpt.com/connector/oauth/callback-id?next=evil",
        ):
            response = await client.get(
                config.oauth_authorize_path
                + "?"
                + urlencode({**base_query, "redirect_uri": redirect_uri})
            )
            assert response.status == 400
            csp = response.headers["Content-Security-Policy"]
            assert malicious_marker not in csp
            assert csp.count("form-action") == 1
            assert "form-action 'self';" in csp
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
            "resource": config.codex_resource,
            "scope": "events:read incidents:read operations:read offline_access",
            "code_challenge": pkce_s256(verifier),
            "code_challenge_method": "S256",
        }
        page = await client.get(config.oauth_authorize_path + "?" + urlencode(query))
        assert page.status == 200
        csp = page.headers["Content-Security-Policy"]
        assert "form-action 'self' http://127.0.0.1:1455" in csp
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
                "resource": config.codex_resource,
                "code_verifier": verifier,
            },
        )
        assert token_response.status == 200
        tokens = await token_response.json()

        listed = await client.post(
            config.codex_mcp_path,
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        assert listed.status == 200
        listed_body = await listed.read()
        assert len(listed_body) < 512 * 1024
        listed_payload = json.loads(listed_body)
        names = {tool["name"] for tool in listed_payload["result"]["tools"]}
        assert names == {
            "search",
            "fetch",
            "events_search",
            "event_get",
            "incidents_search",
            "incident_get",
            "operations_snapshot",
        }

        refreshed = await client.post(
            config.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "client_id": config.codex_oauth_client_id,
                "refresh_token": tokens["refresh_token"],
                "resource": config.codex_resource,
            },
        )
        assert refreshed.status == 200
        assert (await refreshed.json())["refresh_token"] != tokens["refresh_token"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_opencode_public_client_real_oauth_and_full_resource_contract(
    config,
) -> None:
    universal = replace(
        config,
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_vk_enabled=True,
        universal_social_private_read_enabled=True,
        universal_social_dm_enabled=True,
        universal_social_post_enabled=True,
        universal_social_edit_delete_enabled=True,
        universal_social_media_story_enabled=True,
        media_allowed_hosts=("files.example",),
    )
    app = web.Application()
    attach_private_events_mcp(
        app,
        universal,
        social_workspace_adapters={
            "telegram": _WorkspaceAdapter(),
            "vk": _WorkspaceAdapter(),
        },
        asset_ingestor=object(),
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    verifier = "o" * 64
    callback = "http://127.0.0.1:19876/mcp/oauth/callback"
    try:
        discovery = await client.post(
            universal.mcp_path,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"},
            },
        )
        assert discovery.status == 401
        assert "resource_metadata" in discovery.headers["WWW-Authenticate"]

        query = {
            "response_type": "code",
            "client_id": universal.opencode_oauth_client_id,
            "redirect_uri": callback,
            "state": "opencode-state",
            "resource": universal.resource,
            "scope": (
                "events:read incidents:read operations:read offline_access "
                "telegram:read telegram:publish vk:read vk:publish"
            ),
            "code_challenge": pkce_s256(verifier),
            "code_challenge_method": "S256",
        }
        page = await client.get(universal.oauth_authorize_path + "?" + urlencode(query))
        assert page.status == 200
        body = await page.text()
        assert "Events Bot к OpenCode" in body
        assert (
            "form-action 'self' http://127.0.0.1:19876"
            in page.headers["Content-Security-Policy"]
        )
        sealed = re.search(r'name="authorization_request" value="([^"]+)"', body)
        assert sealed
        granted = await client.post(
            universal.oauth_authorize_path,
            data={
                "authorization_request": sealed.group(1),
                "operator_token": universal.operator_token,
            },
            allow_redirects=False,
        )
        assert granted.status == 302
        redirect = urlsplit(granted.headers["Location"])
        assert f"{redirect.scheme}://{redirect.netloc}{redirect.path}" == callback
        code = parse_qs(redirect.query)["code"][0]

        secret_downgrade = await client.post(
            universal.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "client_id": universal.opencode_oauth_client_id,
                "client_secret": universal.oauth_client_secret,
                "code": code,
                "redirect_uri": callback,
                "resource": universal.resource,
                "code_verifier": verifier,
            },
        )
        assert secret_downgrade.status == 401
        assert (await secret_downgrade.json())["error"] == "invalid_client"

        token_response = await client.post(
            universal.oauth_token_path,
            data={
                "grant_type": "authorization_code",
                "client_id": universal.opencode_oauth_client_id,
                "code": code,
                "redirect_uri": callback,
                "resource": universal.resource,
                "code_verifier": verifier,
            },
        )
        assert token_response.status == 200
        tokens = await token_response.json()
        assert "refresh_token" in tokens

        listed = await client.post(
            universal.mcp_path,
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        assert listed.status == 200
        listed_body = await listed.read()
        assert len(listed_body) < 512 * 1024
        listed_payload = json.loads(listed_body)
        names = {tool["name"] for tool in listed_payload["result"]["tools"]}
        assert "social_target_resolve" in names
        assert "social_dialogs_list" in names
        assert "social_action_prepare" in names

        crossed = await client.post(
            universal.codex_mcp_path,
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        assert crossed.status == 401

        refreshed = await client.post(
            universal.oauth_token_path,
            data={
                "grant_type": "refresh_token",
                "client_id": universal.opencode_oauth_client_id,
                "refresh_token": tokens["refresh_token"],
                "resource": universal.resource,
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
        "http://localhost:19876/mcp/oauth/callback",
        "http://127.0.0.1:19876/callback",
        "http://127.0.0.1:19876/mcp/oauth/callback/extra",
        "http://user@127.0.0.1:19876/mcp/oauth/callback",
        "http://127.0.0.1:19876/mcp/oauth/callback?next=bad",
        "http://127.0.0.1:80/mcp/oauth/callback",
        "http://127.0.0.1:019876/mcp/oauth/callback",
        "http://127.0.0.1:65536/mcp/oauth/callback",
        "http://127.0.0.1:19876/mcp/oauth/callback#fragment",
    ],
)
async def test_opencode_redirect_contract_rejects_noncanonical_variants(
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
                    "client_id": config.opencode_oauth_client_id,
                    "redirect_uri": redirect_uri,
                    "state": "state",
                    "resource": config.resource,
                    "code_challenge": pkce_s256("o" * 64),
                    "code_challenge_method": "S256",
                }
            )
        )
        assert response.status == 400
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("port", [19876, 19877, 49152, 65535])
async def test_opencode_redirect_accepts_exact_loopback_path_on_free_port(
    config, port
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
                    "client_id": config.opencode_oauth_client_id,
                    "redirect_uri": f"http://127.0.0.1:{port}/mcp/oauth/callback",
                    "state": "state",
                    "resource": config.resource,
                    "code_challenge": pkce_s256("o" * 64),
                    "code_challenge_method": "S256",
                }
            )
        )
        assert response.status == 200
        assert (
            f"form-action 'self' http://127.0.0.1:{port}"
            in response.headers["Content-Security-Policy"]
        )
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
                    "resource": config.codex_resource,
                    "code_challenge": pkce_s256("v" * 64),
                    "code_challenge_method": "S256",
                }
            )
        )
        assert response.status == 400
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_public_client_rejects_secret_downgrade_and_cross_client_code(
    config,
) -> None:
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
        resource=config.codex_resource,
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
                "resource": config.codex_resource,
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
        social_scope = await client.get(
            config.oauth_authorize_path
            + "?"
            + urlencode(
                {
                    "response_type": "code",
                    "client_id": config.codex_oauth_client_id,
                    "redirect_uri": "http://127.0.0.1:9000/callback/social-denied",
                    "state": "state",
                    "resource": config.codex_resource,
                    "scope": "events:read telegram:read",
                    "code_challenge": pkce_s256("s" * 64),
                    "code_challenge_method": "S256",
                }
            )
        )
        assert social_scope.status == 400
        assert "Requested scope is not available" in await social_scope.text()

        response = await client.get(
            config.oauth_authorize_path
            + "?"
            + urlencode(
                {
                    "response_type": "code",
                    "client_id": config.codex_oauth_client_id,
                    "redirect_uri": "http://127.0.0.1:9000/callback/default-scope",
                    "state": "state",
                    "resource": config.codex_resource,
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
@pytest.mark.parametrize(
    ("endpoint_attr", "metadata_url_attr"),
    (
        ("mcp_path", "resource_metadata_url"),
        ("codex_mcp_path", "codex_resource_metadata_url"),
    ),
)
async def test_missing_bearer_tool_call_is_http_401_with_exact_resource_metadata(
    config, endpoint_attr: str, metadata_url_attr: str
) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            getattr(config, endpoint_attr),
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "operations_snapshot", "arguments": {}},
            },
        )
        assert response.status == 401
        challenge = response.headers["WWW-Authenticate"]
        assert challenge.startswith("Bearer ")
        assert f'resource_metadata="{getattr(config, metadata_url_attr)}"' in challenge
        assert (await response.json())["error"] == "authentication_required"
    finally:
        await client.close()


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
        assert response.status == 401
        assert "resource_metadata" in response.headers["WWW-Authenticate"]

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
