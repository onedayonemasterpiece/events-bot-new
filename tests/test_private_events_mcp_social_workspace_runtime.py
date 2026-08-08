from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import replace
from pathlib import Path

import pytest

from private_events_mcp.auth_store import OAuthStateStore, OAuthStoreError
from private_events_mcp.crypto import AccessIdentity, pkce_s256
from private_events_mcp.social_workspace import (
    SocialAction,
    SocialReadOperation,
    validate_prepare_request,
    validate_read_request,
)
from private_events_mcp.social_workspace_runtime import (
    RuntimePrincipal,
    SocialBudgetLimits,
    SocialWorkspaceRuntime,
    SocialWorkspaceRuntimeError,
)
from private_events_mcp.social_workspace_tools import build_social_workspace_tools
from private_events_mcp.tool_catalog import ToolCallContext

ALL_SCOPES = frozenset(
    f"{platform}:{suffix}"
    for platform in ("telegram", "vk")
    for suffix in (
        "discover", "read:public", "read:private", "read:dialogs", "dm:send",
        "post:publish", "edit", "delete", "forward", "reaction", "comment",
        "schedule", "story:read", "story:write", "analytics", "audience",
    )
)


def context(*, client: str = "chatgpt", subject: str = "alice", resource: str = "https://mcp") -> ToolCallContext:
    return ToolCallContext(
        AccessIdentity(subject, client, ALL_SCOPES, resource, "jti", int(time.time()) + 3600),
        resource,
    )


class FakeAdapter:
    def __init__(self) -> None:
        self.executions = 0
        self.editorial_pages = 0
        self.editorial_cursors = []
        self.timeout = False

    async def capabilities(self, target_ref):
        return {
            "target_ref": target_ref,
            "target_kinds": ["self", "user", "community"],
            "read_operations": [op.value for op in SocialReadOperation],
            "actions": [action.value for action in SocialAction],
            "content_features": ["rich_text", "image"],
            "max_text_length": 4096,
            "max_media_items": 10,
            "provider_id": "must-not-leak",
        }

    async def resolve(self, request):
        if request.target_locator.kind.value == "self":
            return {"target_ref": "native-self-42", "kind": "self",
                    "display_name": "Saved messages"}
        return {"target_ref": "native-user-123", "kind": "user",
                "display_name": "Exact Person", "canonical_handle": "exact_person"}

    async def read(self, request):
        if request.operation is SocialReadOperation.EDITORIAL_SAMPLE:
            self.editorial_cursors.append(request.cursor)
            self.editorial_pages += 1
            return {
                "target": {"target_ref": request.target_ref, "kind": "community",
                           "title": "Community", "about": "About", "description": "Description",
                           "basic_metrics": {"members": 100},
                           "trust": "untrusted_external_data"},
                "items": [
                    {"item_ref": f"native-post-{self.editorial_pages}-{i}", "kind": "post",
                     "published_at": "2026-08-08T12:00:00Z", "text": f"post {i}",
                     "caption": "", "basic_metrics": {"views": i},
                     "trust": "untrusted_external_data"}
                    for i in range(25)
                ],
                "next_cursor": f"provider-cursor-{self.editorial_pages}",
            }
        return {"results": [], "trust": "untrusted_external_data"}

    async def execute(self, intent):
        self.executions += 1
        if self.timeout:
            await asyncio.sleep(0.1)
        return {
            "target_ref": intent.target_ref,
            "item_ref": "native-sent-99",
            "status": "succeeded",
            "retry_safe": False,
            "read_after_write": {
                "verified": True,
                "observed_item_ref": "native-sent-99",
                "observed_at": "2026-08-08T12:00:00Z",
            },
            "raw_method": "messages.send",
            "access_token": "secret",
        }

    async def reconcile(self, operation_ref):
        return {"status": "failed", "retry_safe": False,
                "error_code": "provider_not_observed"}


@pytest.fixture
def runtime(tmp_path: Path):
    adapter = FakeAdapter()
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    value = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter, "vk": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        provider_timeout_seconds=0.02,
    )
    return value, adapter, store


@pytest.mark.asyncio
async def test_self_resolution_and_opaque_encrypted_binding(runtime) -> None:
    service, _adapter, store = runtime
    request = validate_read_request({
        "platform": "telegram", "operation": "resolve_target",
        "target_locator": {"kind": "self"}, "expected_target_kinds": ["self"],
    })
    result = await service.resolve(request, context())
    assert result["target_ref"].startswith("tgt_")
    assert "native-self-42" not in json.dumps(result)
    assert service._resolve_ref(result["target_ref"], "target", "telegram",
                                RuntimePrincipal.from_context(context())) == "native-self-42"
    with pytest.raises(SocialWorkspaceRuntimeError):
        service._resolve_ref(result["target_ref"], "target", "telegram",
                             RuntimePrincipal.from_context(context(client="other")))
    raw_db = Path(store.path).read_bytes()
    assert b"native-self-42" not in raw_db


@pytest.mark.asyncio
async def test_exact_user_dm_prepare_external_approve_commit_and_replay(runtime) -> None:
    service, adapter, _store = runtime
    resolved = await service.resolve(validate_read_request({
        "platform": "telegram", "operation": "resolve_target",
        "target_locator": {"kind": "username", "value": "@exact_person"},
        "expected_target_kinds": ["user"],
    }), context())
    intent = validate_prepare_request({
        "platform": "telegram", "action": "send_message",
        "idempotency_key": "dm-exact-123", "target_ref": resolved["target_ref"],
        "content": {"text": "Hello", "entities": [], "media": []},
    })
    prepared = await service.prepare(intent, context())
    replay = await service.prepare(intent, context())
    assert replay["preparation_ref"] == prepared["preparation_ref"]
    approval = service.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator@example.test", operator_nonce="nonce-unique-123456789",
    )
    result = await service.commit({
        "preparation_ref": prepared["preparation_ref"], **approval,
        "action_digest": prepared["action_digest"],
    }, context())
    assert result["status"] == "succeeded"
    assert result["target_ref"] == resolved["target_ref"]
    assert result["item_ref"] == result["read_after_write"]["observed_item_ref"]
    assert "raw_method" not in result and "access_token" not in result
    assert adapter.executions == 1
    with pytest.raises(SocialWorkspaceRuntimeError):
        await service.commit({"preparation_ref": prepared["preparation_ref"], **approval,
                              "action_digest": prepared["action_digest"]}, context())
    assert adapter.executions == 1


@pytest.mark.asyncio
async def test_cross_client_resource_and_idempotency_mutation_denied(runtime) -> None:
    service, _adapter, _store = runtime
    target = service._mint_ref("target", "native-user", "telegram",
                               RuntimePrincipal.from_context(context()))
    intent = validate_prepare_request({
        "platform": "telegram", "action": "send_message",
        "idempotency_key": "same-key-123", "target_ref": target,
        "content": {"text": "one", "entities": [], "media": []},
    })
    await service.prepare(intent, context())
    with pytest.raises(SocialWorkspaceRuntimeError):
        await service.prepare(replace(intent, content=replace(intent.content, text="two")), context())
    with pytest.raises(SocialWorkspaceRuntimeError):
        await service.prepare(intent, context(client="other"))
    with pytest.raises(SocialWorkspaceRuntimeError):
        await service.prepare(intent, context(resource="https://other-resource"))


@pytest.mark.asyncio
async def test_editorial_sample_four_pages_is_cumulative_and_cursor_bound(runtime) -> None:
    service, adapter, _store = runtime
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-community", "vk", principal)
    sample_ref = cursor = None
    for page in range(4):
        payload = {
            "platform": "vk", "operation": "editorial_sample",
            "target_ref": target, "expected_target_kinds": ["community"],
            "read_access": "public", "purpose": "editorial_analysis",
            "page_size": 25, "total_limit": 100,
        }
        if sample_ref:
            payload.update(sample_ref=sample_ref, cursor=cursor)
        result = await service.read(validate_read_request(payload), context())
        sample_ref, cursor = result["sample_ref"], result.get("next_cursor")
        assert result["cumulative_count"] == (page + 1) * 25
    with pytest.raises(SocialWorkspaceRuntimeError):
        await service.read(validate_read_request({**payload, "sample_ref": sample_ref,
                                                  "cursor": "forged-continuation"}), context())
    assert adapter.editorial_cursors == [None, "provider-cursor-1", "provider-cursor-2",
                                         "provider-cursor-3"]


@pytest.mark.asyncio
async def test_budgets_and_denials_are_durably_audited(tmp_path: Path) -> None:
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store, adapters={"telegram": FakeAdapter()},
        encryption_key="unit-test-key-that-is-long-enough",
        budget_limits=SocialBudgetLimits(attempts=1, rate=1, egress=100_000, media=10),
    )
    request = validate_read_request({
        "platform": "telegram", "operation": "resolve_target",
        "target_locator": {"kind": "self"}, "expected_target_kinds": ["self"],
    })
    await service.resolve(request, context())
    with pytest.raises(SocialWorkspaceRuntimeError):
        await service.resolve(request, context())
    service.audit_denial(context(), platform="telegram", operation="get_item",
                         reason="cross_target", target_ref="tgt_invalidxxxxxxxx")
    with sqlite3.connect(store.path) as conn:
        rows = conn.execute("SELECT outcome,reason_code FROM social_workspace_audit").fetchall()
    assert ("denied", "cross_target") in rows


@pytest.mark.asyncio
async def test_timeout_is_unknown_not_retry_safe_and_status_reconciles(runtime) -> None:
    service, adapter, _store = runtime
    adapter.timeout = True
    service.provider_timeout_seconds = 0.01
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)
    intent = validate_prepare_request({
        "platform": "telegram", "action": "send_message",
        "idempotency_key": "timeout-123", "target_ref": target,
        "content": {"text": "Hello", "entities": [], "media": []},
    })
    prep = await service.prepare(intent, context())
    approval = service.approve_preparation(
        preparation_ref=prep["preparation_ref"], operator_principal="operator",
        operator_nonce="timeout-nonce-123456",
    )
    result = await service.commit({"preparation_ref": prep["preparation_ref"],
        **approval, "action_digest": prep["action_digest"]}, context())
    assert result["status"] == "outcome_unknown" and result["retry_safe"] is False
    reconciled = await service.reconcile(result["operation_ref"], context())
    assert reconciled["status"] == "failed" and reconciled["retry_safe"] is False


def test_tools_are_private_noncacheable_granular_and_feature_hidden(runtime) -> None:
    service, _adapter, _store = runtime
    tools = build_social_workspace_tools(service,
        feature_policy={"social_content_analytics": False})
    names = {tool.name for tool in tools}
    assert "social_content_analytics" not in names
    assert "social_action_prepare" in names
    assert all(not tool.publicly_discoverable and not tool.cacheable for tool in tools)
    assert all(tool.scope_selector is not None for tool in tools)
    assert all(all(any(scope.startswith(p + ":") for p in ("telegram", "vk"))
                   for scope in option) for tool in tools for option in tool.scope_options)


def test_auth_database_is_separate_and_event_database_is_untouched(tmp_path: Path) -> None:
    event_db = tmp_path / "events.sqlite"
    event_db.write_bytes(b"immutable-event-db-sentinel")
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    SocialWorkspaceRuntime(store=store, adapters={"telegram": FakeAdapter()},
                           encryption_key="unit-test-key-that-is-long-enough")
    assert event_db.read_bytes() == b"immutable-event-db-sentinel"



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
