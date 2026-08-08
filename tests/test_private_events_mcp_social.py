from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import replace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from private_events_mcp.access_policy import CHATGPT_MAX_SCOPES, CODEX_MAX_SCOPES
from private_events_mcp.auth_store import OAuthStateStore, SocialTicketError
from private_events_mcp.crypto import AccessIdentity, mint_access_token
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.protocol import MCPProtocol
from private_events_mcp.social import (
    ResolvedTarget,
    SocialAdapterError,
    SocialPost,
    SocialPublishReceipt,
    SocialReadResult,
    TargetAliasPolicy,
)
from private_events_mcp.tool_catalog import ToolSpec


POLICY_JSON = """{
  "telegram": {
    "kenigevents": {
      "provider_target": "-1002331532485",
      "allow_read": true,
      "allow_publish": true
    }
  },
  "vk": {
    "kenigevents": {
      "provider_target": "231920894",
      "allow_read": true,
      "allow_publish": true
    }
  }
}"""


class FakeAdapter:
    def __init__(self, platform: str) -> None:
        self.platform = platform
        self.read_calls = 0
        self.publish_calls = 0
        self.delay = 0.0
        self.error: Exception | None = None
        self.targets: list[ResolvedTarget] = []

    async def read_text(self, *, target: ResolvedTarget, limit: int) -> SocialReadResult:
        self.read_calls += 1
        self.targets.append(target)
        if self.error:
            raise self.error
        return SocialReadResult(
            posts=(
                SocialPost(
                    post_id="post-101-123456789:abcdefghijklmnopqrstuvwxyzABCDE",
                    text=(
                        "Authorization: Bearer provider-secret-abcdefghijklmnop "
                        "123456789:abcdefghijklmnopqrstuvwxyzABCDE"
                    ),
                    published_at="2026-08-08T12:00:00Z",
                ),
            )
        )

    async def publish_text(
        self,
        *,
        target: ResolvedTarget,
        text: str,
        idempotency_key: str,
    ) -> SocialPublishReceipt:
        self.publish_calls += 1
        self.targets.append(target)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return SocialPublishReceipt(reference="provider-receipt-sensitive-987654")


def _token(config, *, codex: bool = False, scopes=frozenset()) -> str:
    token, _identity = mint_access_token(
        signing_key=config.signing_key,
        issuer=config.issuer,
        audience=config.codex_resource if codex else config.resource,
        subject="owner-social-test",
        client_id=config.codex_oauth_client_id if codex else config.oauth_client_id,
        scopes=frozenset(scopes),
        lifetime_seconds=600,
    )
    return token


async def _rpc(client: TestClient, path: str, token: str, method: str, params: dict):
    response = await client.post(
        path,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        headers={"Authorization": f"Bearer {token}"},
    )
    return response, await response.json()


@pytest.mark.parametrize(
    "raw",
    [
        '{"max":{"x":{"provider_target":"1","allow_read":true,"allow_publish":true}}}',
        '{"telegram":{"Bad Alias":{"provider_target":"-10012345","allow_read":true,"allow_publish":true}}}',
        '{"telegram":{"safe":{"provider_target":"https://t.me/raw","allow_read":true,"allow_publish":true}}}',
        '{"vk":{"safe":{"provider_target":"231","allow_read":1,"allow_publish":true}}}',
        '{"vk":{"safe":{"provider_target":"231","allow_read":true,"allow_publish":true,"method":"wall.post"}}}',
    ],
)
def test_target_policy_is_strict_and_has_no_implicit_targets(raw) -> None:
    assert TargetAliasPolicy.from_json("").is_empty
    with pytest.raises(ValueError):
        TargetAliasPolicy.from_json(raw)


@pytest.mark.asyncio
async def test_endpoint_client_scope_and_catalog_isolation(config) -> None:
    telegram = FakeAdapter("telegram")
    vk = FakeAdapter("vk")
    configured = replace(config, social_targets_json=POLICY_JSON)
    app = web.Application()
    attach_private_events_mcp(
        app,
        configured,
        social_adapters={"telegram": telegram, "vk": vk},
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    read_names = {
        "search",
        "fetch",
        "events_search",
        "event_get",
        "incidents_search",
        "incident_get",
        "operations_snapshot",
    }
    social_names = {
        "telegram_read",
        "vk_read",
        "prepare_text_publication",
        "publish_prepared_text",
    }
    try:
        chat_metadata = await client.get(configured.protected_resource_metadata_path)
        codex_metadata = await client.get(configured.codex_protected_resource_metadata_path)
        assert (await chat_metadata.json())["scopes_supported"] == sorted(CHATGPT_MAX_SCOPES)
        codex_metadata_payload = await codex_metadata.json()
        assert codex_metadata_payload["resource"] == configured.codex_resource
        assert codex_metadata_payload["scopes_supported"] == sorted(CODEX_MAX_SCOPES)
        assert not ({"telegram:read", "telegram:publish", "vk:read", "vk:publish"} & set(
            codex_metadata_payload["scopes_supported"]
        ))

        chat_token = _token(configured, scopes=CHATGPT_MAX_SCOPES)
        response, listed = await _rpc(
            client, configured.mcp_path, chat_token, "tools/list", {}
        )
        assert response.status == 200
        tools = {item["name"]: item for item in listed["result"]["tools"]}
        assert set(tools) == read_names | social_names
        assert tools["publish_prepared_text"]["annotations"] == {
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": True,
        }
        telegram_publish_token = _token(configured, scopes={"telegram:publish"})
        _response, telegram_only = await _rpc(
            client, configured.mcp_path, telegram_publish_token, "tools/list", {}
        )
        telegram_prepare = next(
            item
            for item in telegram_only["result"]["tools"]
            if item["name"] == "prepare_text_publication"
        )
        assert telegram_prepare["inputSchema"]["properties"]["platform"]["enum"] == [
            "telegram"
        ]
        assert telegram_prepare["securitySchemes"] == [
            {"type": "oauth2", "scopes": ["telegram:publish"]}
        ]

        minimal_token = _token(
            configured,
            scopes={"events:read", "incidents:read", "operations:read"},
        )
        _response, minimal = await _rpc(
            client, configured.mcp_path, minimal_token, "tools/list", {}
        )
        assert {item["name"] for item in minimal["result"]["tools"]} == read_names
        _response, denied = await _rpc(
            client,
            configured.mcp_path,
            minimal_token,
            "tools/call",
            {"name": "telegram_read", "arguments": {"target_alias": "kenigevents"}},
        )
        assert denied["result"]["isError"] is True
        assert "insufficient_scope" in denied["result"]["_meta"]["mcp/www_authenticate"][0]

        codex_token = _token(configured, codex=True, scopes=CODEX_MAX_SCOPES)
        response, codex = await _rpc(
            client, configured.codex_mcp_path, codex_token, "tools/list", {}
        )
        assert response.status == 200
        assert {item["name"] for item in codex["result"]["tools"]} == read_names
        _response, absent = await _rpc(
            client,
            configured.codex_mcp_path,
            codex_token,
            "tools/call",
            {"name": "telegram_read", "arguments": {"target_alias": "kenigevents"}},
        )
        assert absent["error"]["message"] == "Unknown tool"

        crossed = await client.post(
            configured.codex_mcp_path,
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            headers={"Authorization": f"Bearer {chat_token}"},
        )
        assert crossed.status == 401
        crossed = await client.post(
            configured.mcp_path,
            json={"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}},
            headers={"Authorization": f"Bearer {codex_token}"},
        )
        assert crossed.status == 401

        anonymous = await client.post(
            configured.mcp_path,
            json={"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": {}},
        )
        anonymous_names = {
            item["name"] for item in (await anonymous.json())["result"]["tools"]
        }
        assert anonymous_names == read_names
        assert all("max" not in name.casefold() for name in set(tools))
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_alias_denial_and_read_redaction_and_audit(config) -> None:
    telegram = FakeAdapter("telegram")
    configured = replace(config, social_targets_json=POLICY_JSON)
    app = web.Application()
    attach_private_events_mcp(app, configured, social_adapters={"telegram": telegram})
    client = TestClient(TestServer(app))
    await client.start_server()
    token = _token(configured, scopes=CHATGPT_MAX_SCOPES)
    try:
        _response, denied = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {"name": "telegram_read", "arguments": {"target_alias": "not_allowed"}},
        )
        assert denied["result"]["isError"] is True
        assert telegram.read_calls == 0

        _response, read = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {"name": "telegram_read", "arguments": {"target_alias": "kenigevents"}},
        )
        result = read["result"]["structuredContent"]
        assert result["trust"] == "untrusted_external_data"
        assert result["posts"][0]["trust"] == "untrusted_external_data"
        assert "provider-secret" not in result["posts"][0]["text"]
        assert "123456789:" not in str(result["posts"][0])
        assert "<redacted>" in result["posts"][0]["text"]
        assert telegram.read_calls == 1
    finally:
        await client.close()

    with sqlite3.connect(configured.auth_database_path) as conn:
        outcomes = conn.execute(
            "SELECT outcome FROM social_action_audit WHERE action='read_text' ORDER BY id"
        ).fetchall()
    assert outcomes == [("denied",), ("succeeded",)]


@pytest.mark.asyncio
async def test_prepare_publish_binding_replay_and_redacted_append_only_audit(config) -> None:
    telegram = FakeAdapter("telegram")
    configured = replace(config, social_targets_json=POLICY_JSON)
    app = web.Application()
    attach_private_events_mcp(app, configured, social_adapters={"telegram": telegram})
    client = TestClient(TestServer(app))
    await client.start_server()
    token = _token(configured, scopes=CHATGPT_MAX_SCOPES)
    text = "unique raw publication body 391e8b"
    idempotency = "idem-key-social-391e8b"
    base_arguments = {
        "platform": "telegram",
        "target_alias": "kenigevents",
        "text": text,
        "idempotency_key": idempotency,
    }
    try:
        _response, extra = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {
                "name": "prepare_text_publication",
                "arguments": {**base_arguments, "raw_target_id": "-100999"},
            },
        )
        assert extra["result"]["isError"] is True
        assert "Unsupported argument field" in str(extra)

        _response, prepared = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {"name": "prepare_text_publication", "arguments": base_arguments},
        )
        ticket = prepared["result"]["structuredContent"]["preparation_ticket"]

        _response, mutated = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {
                "name": "publish_prepared_text",
                "arguments": {
                    **base_arguments,
                    "text": text + " changed",
                    "preparation_ticket": ticket,
                },
            },
        )
        assert mutated["result"]["isError"] is True
        assert telegram.publish_calls == 0

        _response, published = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {
                "name": "publish_prepared_text",
                "arguments": {**base_arguments, "preparation_ticket": ticket},
            },
        )
        assert published["result"]["structuredContent"]["published"] is True
        assert telegram.publish_calls == 1
        assert telegram.targets[-1].provider_target == "-1002331532485"

        _response, replay = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {
                "name": "publish_prepared_text",
                "arguments": {**base_arguments, "preparation_ticket": ticket},
            },
        )
        assert replay["result"]["isError"] is True
        assert telegram.publish_calls == 1
    finally:
        await client.close()

    db_bytes = open(configured.auth_database_path, "rb").read()
    for forbidden in (
        text.encode(),
        idempotency.encode(),
        ticket.encode(),
        b"-100999",
        b"-1002331532485",
        b"provider-receipt-sensitive-987654",
    ):
        assert forbidden not in db_bytes
    with sqlite3.connect(configured.auth_database_path) as conn:
        prepare_rows = conn.execute(
            "SELECT outcome, target_alias FROM social_action_audit "
            "WHERE action='prepare_text_publication' ORDER BY id"
        ).fetchall()
        assert prepare_rows == [
            ("denied_invalid_arguments", "kenigevents"),
            ("prepared", "kenigevents"),
        ]
        rows = conn.execute(
            "SELECT outcome, target_alias, length(text_hash) FROM social_action_audit "
            "WHERE action='publish_prepared_text' ORDER BY id"
        ).fetchall()
        assert rows == [
            ("denied_invalid_arguments", "kenigevents", 64),
            ("succeeded", "kenigevents", 64),
            ("denied_invalid_arguments", "kenigevents", 64),
        ]
        with pytest.raises(sqlite3.IntegrityError, match="immutable during retention"):
            conn.execute("DELETE FROM social_action_audit")

    now = int(time.time())
    store = OAuthStateStore(configured.auth_database_path)
    store.audit_social_action(
        action="retention_old",
        outcome="synthetic",
        client_id="client",
        subject="subject",
        resource="resource",
        now=now - 91 * 86400,
    )
    store.audit_social_action(
        action="retention_current",
        outcome="synthetic",
        client_id="client",
        subject="subject",
        resource="resource",
        now=now,
    )
    with sqlite3.connect(configured.auth_database_path) as conn:
        assert conn.execute(
            "SELECT count(*) FROM social_action_audit WHERE action='retention_old'"
        ).fetchone() == (0,)


def test_ticket_expiry_and_every_binding_dimension(tmp_path) -> None:
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    values = {
        "ticket": "ticket-secret-abcdefghijklmnopqrstuvwxyz012345",
        "client_id": "chat-client",
        "subject": "owner",
        "resource": "https://example.test/private/mcp",
        "platform": "telegram",
        "target_alias": "kenigevents",
        "text_hash": "a" * 64,
        "idempotency_key": "idem-key-original",
    }
    store.create_preparation_ticket(**values, expires_at=200, now=100)
    mutations = {
        "client_id": "other-client",
        "subject": "other-owner",
        "resource": "https://example.test/private/codex/mcp",
        "platform": "vk",
        "target_alias": "otheralias",
        "text_hash": "b" * 64,
        "idempotency_key": "idem-key-mutated",
    }
    for key, value in mutations.items():
        attempted = dict(values)
        attempted[key] = value
        with pytest.raises(SocialTicketError, match="binding_mismatch"):
            store.consume_preparation_ticket(**attempted, now=150)
    with pytest.raises(SocialTicketError, match="expired"):
        store.consume_preparation_ticket(**values, now=200)

    # The replay ledger is durable but bounded: the key becomes eligible only
    # once a later write runs cleanup beyond the 90-day retention horizon.
    later = 100 + 91 * 86400
    store.create_preparation_ticket(
        **{**values, "ticket": "replacement-ticket-abcdefghijklmnopqrstuvwxyz"},
        expires_at=later + 60,
        now=later,
    )


@pytest.mark.asyncio
async def test_publish_timeout_consumes_ticket_and_records_unknown_outcome(config) -> None:
    telegram = FakeAdapter("telegram")
    telegram.delay = 0.5
    configured = replace(
        config,
        social_targets_json=POLICY_JSON,
        social_provider_timeout_seconds=0.25,
    )
    app = web.Application()
    attach_private_events_mcp(app, configured, social_adapters={"telegram": telegram})
    client = TestClient(TestServer(app))
    await client.start_server()
    token = _token(configured, scopes=CHATGPT_MAX_SCOPES)
    arguments = {
        "platform": "telegram",
        "target_alias": "kenigevents",
        "text": "timeout body",
        "idempotency_key": "idem-timeout-123",
    }
    try:
        _response, prepared = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {"name": "prepare_text_publication", "arguments": arguments},
        )
        ticket = prepared["result"]["structuredContent"]["preparation_ticket"]
        call = {
            "name": "publish_prepared_text",
            "arguments": {**arguments, "preparation_ticket": ticket},
        }
        _response, timed_out = await _rpc(
            client, configured.mcp_path, token, "tools/call", call
        )
        assert timed_out["result"]["isError"] is True
        assert timed_out["result"]["structuredContent"] == {
            "outcome": "unknown",
            "retry_safe": False,
            "instruction": (
                "The provider may already have accepted the publication. "
                "Do not retry with a new idempotency key."
            ),
        }
        _response, replay = await _rpc(
            client, configured.mcp_path, token, "tools/call", call
        )
        assert replay["result"]["isError"] is True
        _response, reprepared = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {"name": "prepare_text_publication", "arguments": arguments},
        )
        assert reprepared["result"]["isError"] is True
        assert "idempotency_key_already_used" in str(reprepared)
        assert telegram.publish_calls == 1
    finally:
        await client.close()

    with sqlite3.connect(configured.auth_database_path) as conn:
        outcomes = conn.execute(
            "SELECT outcome FROM social_action_audit "
            "WHERE action='publish_prepared_text' ORDER BY id"
        ).fetchall()
    assert outcomes == [("outcome_unknown",), ("denied_invalid_arguments",)]


@pytest.mark.asyncio
async def test_daily_publish_budget_survives_access_token_refresh(config) -> None:
    telegram = FakeAdapter("telegram")
    configured = replace(
        config,
        social_targets_json=POLICY_JSON,
        social_publish_attempts_per_day=1,
    )
    app = web.Application()
    attach_private_events_mcp(app, configured, social_adapters={"telegram": telegram})
    client = TestClient(TestServer(app))
    await client.start_server()
    first_token = _token(configured, scopes=CHATGPT_MAX_SCOPES)
    refreshed_token = _token(configured, scopes=CHATGPT_MAX_SCOPES)

    async def prepare_and_publish(token: str, suffix: str):
        arguments = {
            "platform": "telegram",
            "target_alias": "kenigevents",
            "text": f"budget body {suffix}",
            "idempotency_key": f"idem-budget-{suffix}",
        }
        _response, prepared = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {"name": "prepare_text_publication", "arguments": arguments},
        )
        if prepared["result"].get("isError"):
            return _response, prepared
        ticket = prepared["result"]["structuredContent"]["preparation_ticket"]
        return await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {
                "name": "publish_prepared_text",
                "arguments": {**arguments, "preparation_ticket": ticket},
            },
        )

    try:
        _response, first = await prepare_and_publish(first_token, "first-001")
        assert first["result"]["structuredContent"]["published"] is True
        _response, second = await prepare_and_publish(refreshed_token, "second-002")
        assert second["result"]["isError"] is True
        assert "daily_publish_attempt_limit_reached" in str(second)
        assert telegram.publish_calls == 1
    finally:
        await client.close()

    with sqlite3.connect(configured.auth_database_path) as conn:
        assert conn.execute(
            "SELECT attempts FROM social_publish_daily_budget"
        ).fetchall() == [(1,)]


@pytest.mark.asyncio
async def test_provider_errors_are_redacted_and_empty_policy_denies(config) -> None:
    telegram = FakeAdapter("telegram")
    telegram.error = ValueError("provider_target=-1002331532485 token=raw-secret-value")
    configured = replace(config, social_targets_json=POLICY_JSON)
    app = web.Application()
    attach_private_events_mcp(app, configured, social_adapters={"telegram": telegram})
    client = TestClient(TestServer(app))
    await client.start_server()
    token = _token(configured, scopes=CHATGPT_MAX_SCOPES)
    try:
        _response, failed = await _rpc(
            client,
            configured.mcp_path,
            token,
            "tools/call",
            {"name": "telegram_read", "arguments": {"target_alias": "kenigevents"}},
        )
        assert failed["result"]["isError"] is True
        assert "raw-secret-value" not in str(failed)
        assert "-1002331532485" not in str(failed)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_read_cache_is_partitioned_by_complete_security_context() -> None:
    calls = 0

    async def handler(_arguments, _context):
        nonlocal calls
        calls += 1
        return {"calls": calls}

    tool = ToolSpec(
        name="cached_read",
        title="Cached read",
        description="test",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        scopes=frozenset({"events:read"}),
        handler=handler,
    )
    protocol = MCPProtocol(
        (tool,),
        cache_ttl_seconds=30,
        challenge='Bearer error="invalid_token"',
        resource="https://resource.example/private/mcp",
        allowed_client_ids=frozenset({"client-a", "client-b"}),
        policy_fingerprint="policy-fingerprint-test",
    )
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "cached_read", "arguments": {}},
    }

    def identity(subject, client, scopes):
        return AccessIdentity(
            subject=subject,
            client_id=client,
            scopes=frozenset(scopes),
            audience="https://resource.example/private/mcp",
            token_id="irrelevant-jti",
            expires_at=2**31,
        )

    first = identity("subject-a", "client-a", {"events:read"})
    await protocol.dispatch(request, first)
    await protocol.dispatch(request, first)
    await protocol.dispatch(
        request, identity("subject-b", "client-a", {"events:read"})
    )
    await protocol.dispatch(
        request,
        identity("subject-a", "client-a", {"events:read", "operations:read"}),
    )
    await protocol.dispatch(
        request, identity("subject-a", "client-b", {"events:read"})
    )
    assert calls == 4
    decoded_keys = [json.loads(item) for item in protocol.cache._values]
    assert all(item["resource"] == "https://resource.example/private/mcp" for item in decoded_keys)
    assert all(item["policy"] == "policy-fingerprint-test" for item in decoded_keys)
    assert {item["client"] for item in decoded_keys} == {"client-a", "client-b"}
    assert {item["subject"] for item in decoded_keys} == {"subject-a", "subject-b"}

@pytest.mark.asyncio
async def test_empty_target_policy_denies_every_alias(config) -> None:
    empty = replace(config, auth_database_path=config.auth_database_path + ".empty")
    empty_adapter = FakeAdapter("telegram")
    app = web.Application()
    attach_private_events_mcp(app, empty, social_adapters={"telegram": empty_adapter})
    client = TestClient(TestServer(app))
    await client.start_server()
    token = _token(empty, scopes=CHATGPT_MAX_SCOPES)
    try:
        _response, denied = await _rpc(
            client,
            empty.mcp_path,
            token,
            "tools/call",
            {"name": "telegram_read", "arguments": {"target_alias": "kenigevents"}},
        )
        assert denied["result"]["isError"] is True
        assert empty_adapter.read_calls == 0
        publication = {
            "platform": "telegram",
            "target_alias": "kenigevents",
            "text": "denied body",
            "idempotency_key": "denied-key-001",
        }
        _response, denied_prepare = await _rpc(
            client,
            empty.mcp_path,
            token,
            "tools/call",
            {"name": "prepare_text_publication", "arguments": publication},
        )
        assert denied_prepare["result"]["isError"] is True
        _response, denied_publish = await _rpc(
            client,
            empty.mcp_path,
            token,
            "tools/call",
            {
                "name": "publish_prepared_text",
                "arguments": {
                    **publication,
                    "preparation_ticket": "x" * 48,
                },
            },
        )
        assert denied_publish["result"]["isError"] is True
        _response, malformed = await _rpc(
            client,
            empty.mcp_path,
            token,
            "tools/call",
            {
                "name": "prepare_text_publication",
                "arguments": {
                    **publication,
                    "target_alias": "https://t.me/raw-target",
                    "idempotency_key": "denied-key-malformed",
                },
            },
        )
        assert malformed["result"]["isError"] is True
        for malformed_platform in (["telegram"], {}):
            _response, malformed = await _rpc(
                client,
                empty.mcp_path,
                token,
                "tools/call",
                {
                    "name": "prepare_text_publication",
                    "arguments": {
                        **publication,
                        "platform": malformed_platform,
                        "idempotency_key": "denied-key-bad-platform",
                    },
                },
            )
            assert malformed["result"]["isError"] is True
        read_only_token = _token(empty, scopes={"events:read"})
        _response, insufficient = await _rpc(
            client,
            empty.mcp_path,
            read_only_token,
            "tools/call",
            {"name": "prepare_text_publication", "arguments": publication},
        )
        assert insufficient["result"]["isError"] is True
    finally:
        await client.close()

    with sqlite3.connect(empty.auth_database_path) as conn:
        assert conn.execute(
            "SELECT action, outcome, target_alias FROM social_action_audit "
            "WHERE action LIKE '%publication%' OR action='publish_prepared_text' "
            "ORDER BY id"
        ).fetchall() == [
            ("prepare_text_publication", "denied_invalid_arguments", "kenigevents"),
            ("publish_prepared_text", "denied_invalid_arguments", "kenigevents"),
            ("prepare_text_publication", "denied_invalid_arguments", "invalid"),
            ("prepare_text_publication", "denied_invalid_arguments", "kenigevents"),
            ("prepare_text_publication", "denied_invalid_arguments", "kenigevents"),
            ("prepare_text_publication", "denied_insufficient_scope", "kenigevents"),
        ]
