from __future__ import annotations

import asyncio
import base64
import re
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from private_events_mcp.crypto import pkce_s256
from private_events_mcp.integration import attach_private_events_mcp


def _basic_auth(config) -> str:
    encoded = base64.b64encode(
        f"{config.oauth_client_id}:{config.oauth_client_secret}".encode()
    ).decode()
    return f"Basic {encoded}"


async def _authorize(
    client: TestClient,
    config,
    *,
    scope: str,
    verifier: str,
) -> dict:
    callback = "https://chatgpt.com/connector/oauth/persistent-refresh-policy"
    query = {
        "response_type": "code",
        "client_id": config.oauth_client_id,
        "redirect_uri": callback,
        "state": "persistent-refresh-state",
        "resource": config.resource,
        "scope": scope,
        "code_challenge": pkce_s256(verifier),
        "code_challenge_method": "S256",
    }
    page = await client.get(
        config.oauth_authorize_path + "?" + urlencode(query)
    )
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
    code = parse_qs(urlsplit(granted.headers["Location"]).query)["code"][0]

    response = await client.post(
        config.oauth_token_path,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": callback,
            "resource": config.resource,
            "code_verifier": verifier,
        },
        headers={"Authorization": _basic_auth(config)},
    )
    payload = await response.json()
    assert response.status == 200, payload
    return payload


async def _refresh(client: TestClient, config, refresh_token: str) -> tuple[int, dict]:
    response = await client.post(
        config.oauth_token_path,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "resource": config.resource,
        },
        headers={"Authorization": _basic_auth(config)},
    )
    return response.status, await response.json()


@pytest.mark.asyncio
async def test_chatgpt_read_grant_without_offline_scope_survives_concurrent_refresh(
    config,
) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        tokens = await _authorize(
            client,
            config,
            scope="events:read incidents:read operations:read",
            verifier="r" * 64,
        )
        refresh_token = tokens["refresh_token"]
        assert tokens["scope"] == "events:read incidents:read operations:read"

        first, second = await asyncio.gather(
            _refresh(client, config, refresh_token),
            _refresh(client, config, refresh_token),
        )
        for status, payload in (first, second):
            assert status == 200, payload
            assert payload["refresh_token"] == refresh_token
            assert payload["scope"] == tokens["scope"]
            assert payload["access_token"] != tokens["access_token"]

        third_status, third = await _refresh(client, config, refresh_token)
        assert third_status == 200, third
        assert third["refresh_token"] == refresh_token
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_chatgpt_offline_scope_keeps_rotating_spent_token_rejection(config) -> None:
    app = web.Application()
    attach_private_events_mcp(app, config)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        tokens = await _authorize(
            client,
            config,
            scope=(
                "events:read incidents:read operations:read offline_access"
            ),
            verifier="s" * 64,
        )
        original_refresh = tokens["refresh_token"]

        refreshed_status, refreshed = await _refresh(
            client, config, original_refresh
        )
        assert refreshed_status == 200, refreshed
        rotated_refresh = refreshed["refresh_token"]
        assert rotated_refresh != original_refresh

        replay_status, replay = await _refresh(client, config, original_refresh)
        assert replay_status == 400
        assert replay["error"] == "invalid_grant"

        current_status, current = await _refresh(client, config, rotated_refresh)
        assert current_status == 200, current
        assert current["refresh_token"] != rotated_refresh
    finally:
        await client.close()
