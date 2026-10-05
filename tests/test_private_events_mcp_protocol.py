from __future__ import annotations

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.protocol import MCPProtocol
from private_events_mcp.repository import EventsEvidenceRepository
from private_events_mcp.tool_catalog import ToolExecutionError, ToolSpec, build_tools


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


@pytest.mark.asyncio
async def test_owner_can_discover_upgradeable_tools_without_gaining_authority(
    config,
) -> None:
    calls = 0

    async def handler(_arguments, _context):
        nonlocal calls
        calls += 1
        return {"ok": True}

    write_tool = ToolSpec(
        "event_create_prepare",
        "Prepare event create",
        "Owner event mutation that requires an explicit write scope.",
        {"type": "object", "additionalProperties": False, "properties": {}},
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["ok"],
            "properties": {"ok": {"const": True}},
        },
        scopes=frozenset({"events:write"}),
        handler=handler,
        publicly_discoverable=False,
    )
    owner_protocol = MCPProtocol(
        (write_tool,),
        cache_ttl_seconds=0,
        challenge='Bearer resource_metadata="https://example/metadata", error="invalid_token"',
        resource=config.resource,
        allowed_client_ids=frozenset({config.oauth_client_id}),
        discovery_scopes=frozenset({"events:write"}),
    )
    read_only_owner = identity(config)

    listed = await owner_protocol.dispatch(
        {"jsonrpc": "2.0", "id": 7, "method": "tools/list", "params": {}},
        read_only_owner,
    )
    assert [item["name"] for item in listed["result"]["tools"]] == [
        "event_create_prepare"
    ]
    assert listed["result"]["tools"][0]["securitySchemes"] == [
        {"type": "oauth2", "scopes": ["events:write"]}
    ]

    denied = await owner_protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {"name": "event_create_prepare", "arguments": {}},
        },
        read_only_owner,
    )
    assert denied["result"]["isError"] is True
    assert "insufficient_scope" in denied["result"]["_meta"]["mcp/www_authenticate"][0]
    assert calls == 0

    write_owner = AccessIdentity(
        subject=read_only_owner.subject,
        client_id=read_only_owner.client_id,
        scopes=frozenset({"events:read", "events:write"}),
        audience=read_only_owner.audience,
        token_id="write-token-id",
        expires_at=read_only_owner.expires_at,
    )
    allowed = await owner_protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {"name": "event_create_prepare", "arguments": {}},
        },
        write_owner,
    )
    assert allowed["result"]["structuredContent"] == {"ok": True}
    assert calls == 1


@pytest.mark.asyncio
async def test_discovery_scopes_do_not_leak_to_rejected_client(config) -> None:
    async def handler(_arguments, _context):
        return {"ok": True}

    tool = ToolSpec(
        "promo_campaign_create",
        "Promo create",
        "Owner promo mutation.",
        {"type": "object", "additionalProperties": False, "properties": {}},
        {"type": "object"},
        scopes=frozenset({"promo:write"}),
        handler=handler,
        publicly_discoverable=False,
    )
    protocol = MCPProtocol(
        (tool,),
        cache_ttl_seconds=0,
        challenge='Bearer error="invalid_token"',
        resource=config.resource,
        allowed_client_ids=frozenset({config.oauth_client_id}),
        discovery_scopes=frozenset({"promo:write"}),
    )
    foreign = AccessIdentity(
        subject="events-bot-owner",
        client_id="foreign-client",
        scopes=frozenset({"events:read"}),
        audience=config.resource,
        token_id="foreign-token",
        expires_at=9_999_999_999,
    )
    listed = await protocol.dispatch(
        {"jsonrpc": "2.0", "id": 10, "method": "tools/list", "params": {}},
        foreign,
    )
    assert listed["result"]["tools"] == []


@pytest.mark.asyncio
async def test_discovery_scopes_respect_identity_validator(config) -> None:
    async def handler(_arguments, _context):
        return {"ok": True}

    tool = ToolSpec(
        "partner_admin",
        "Partner admin",
        "Owner partner administration.",
        {"type": "object", "additionalProperties": False, "properties": {}},
        {"type": "object"},
        scopes=frozenset({"partners:manage"}),
        handler=handler,
        publicly_discoverable=False,
    )

    def reject(_identity):
        raise ToolExecutionError("PARTNER_ACCESS_REVOKED")

    protocol = MCPProtocol(
        (tool,),
        cache_ttl_seconds=0,
        challenge='Bearer error="invalid_token"',
        resource=config.resource,
        discovery_scopes=frozenset({"partners:manage"}),
        identity_validator=reject,
    )
    listed = await protocol.dispatch(
        {"jsonrpc": "2.0", "id": 11, "method": "tools/list", "params": {}},
        identity(config),
    )
    assert listed["result"]["tools"] == []
