from __future__ import annotations

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.protocol import MCPProtocol
from private_events_mcp.repository import EventsEvidenceRepository
from private_events_mcp.tool_catalog import ToolSpec, build_tools


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


@pytest.mark.asyncio
async def test_evidence_extensions_preserve_exact_seven_tool_contract(protocol) -> None:
    listed = await protocol.dispatch(
        {"jsonrpc": "2.0", "id": 5, "method": "tools/list", "params": {}}, None
    )
    tools = listed["result"]["tools"]
    assert [item["name"] for item in tools] == [
        "search",
        "fetch",
        "events_search",
        "event_get",
        "incidents_search",
        "incident_get",
        "operations_snapshot",
    ]
    by_name = {item["name"]: item for item in tools}
    assert "post_url" in by_name["events_search"]["inputSchema"]["properties"]
    assert {
        "event_id",
        "source_url",
        "post_url",
        "run_id",
        "job_id",
        "error_class",
        "time_from",
        "time_to",
    } <= set(by_name["incidents_search"]["inputSchema"]["properties"])


@pytest.mark.asyncio
async def test_stable_legacy_social_families_authorize_only_same_provider_and_mode(
    config,
) -> None:
    calls = 0

    async def handler(_arguments, _context):
        nonlocal calls
        calls += 1
        return {"ok": True}

    tool = ToolSpec(
        "typed_send",
        "Typed send",
        "A typed, independently approved social mutation.",
        {"type": "object", "additionalProperties": False, "properties": {}},
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["ok"],
            "properties": {"ok": {"const": True}},
        },
        scopes=frozenset(),
        scope_options=(
            frozenset({"telegram:dm:send"}),
            frozenset({"telegram:publish"}),
        ),
        scope_selector=lambda _arguments: frozenset({"telegram:dm:send"}),
        handler=handler,
        publicly_discoverable=False,
    )
    social_protocol = MCPProtocol(
        (tool,), cache_ttl_seconds=0, challenge='Bearer error="invalid_token"'
    )

    def social_identity(scopes):
        return AccessIdentity(
            "events-bot-owner",
            config.oauth_client_id,
            frozenset(scopes),
            config.resource,
            "social-token",
            9_999_999_999,
        )

    request = {
        "jsonrpc": "2.0",
        "id": 6,
        "method": "tools/call",
        "params": {"name": "typed_send", "arguments": {}},
    }
    allowed = await social_protocol.dispatch(
        request, social_identity({"telegram:publish"})
    )
    assert allowed["result"]["structuredContent"] == {"ok": True}
    assert calls == 1

    for denied_scopes in (
        {"telegram:read"},
        {"vk:publish"},
        {"events:read"},
    ):
        denied = await social_protocol.dispatch(
            request, social_identity(denied_scopes)
        )
        assert denied["result"]["isError"] is True
    assert calls == 1
