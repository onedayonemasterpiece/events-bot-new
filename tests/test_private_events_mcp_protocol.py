from __future__ import annotations

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.protocol import MCPProtocol
from private_events_mcp.repository import EventsEvidenceRepository
from private_events_mcp.tool_catalog import build_tools


@pytest.fixture
def protocol(config) -> MCPProtocol:
    return MCPProtocol(
        build_tools(EventsEvidenceRepository(config)),
        cache_ttl_seconds=0,
        challenge='Bearer resource_metadata="https://example/metadata", error="invalid_token"',
        tool_timeout_seconds=3.0,
    )


def identity(config) -> AccessIdentity:
    return AccessIdentity(
        subject="events-bot-owner",
        client_id=config.oauth_client_id,
        scopes=frozenset({"events:read", "incidents:read", "operations:read"}),
        audience=config.resource,
        token_id="test-token-id",
        expires_at=9_999_999_999,
    )


@pytest.mark.asyncio
async def test_discovery_is_public_but_tool_calls_challenge(protocol) -> None:
    listed = await protocol.dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        None,
    )
    names = {item["name"] for item in listed["result"]["tools"]}
    assert {"search", "fetch", "events_search", "event_get", "incidents_search", "incident_get", "operations_snapshot"} <= names
    assert all(item["annotations"]["readOnlyHint"] for item in listed["result"]["tools"])

    called = await protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "search", "arguments": {"query": "архитектура"}},
        },
        None,
    )
    auth = called["result"]["_meta"]["mcp/www_authenticate"]
    assert auth and "resource_metadata" in auth[0]


@pytest.mark.asyncio
async def test_search_fetch_flow(protocol, config) -> None:
    search = await protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "search", "arguments": {"query": "архитектура"}},
        },
        identity(config),
    )
    structured = search["result"]["structuredContent"]
    assert structured["results"][0]["id"] == "event:42"

    fetched = await protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "fetch", "arguments": {"id": "event:42"}},
        },
        identity(config),
    )
    assert "Лекция об архитектуре" in fetched["result"]["structuredContent"]["text"]
