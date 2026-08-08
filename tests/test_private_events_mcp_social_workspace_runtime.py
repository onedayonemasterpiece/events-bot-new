from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from private_events_mcp.auth_store import OAuthStateStore, OAuthStoreError
from private_events_mcp.crypto import AccessIdentity, pkce_s256
from private_events_mcp.repository import InvalidArgumentsError
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
        self.editorial_sample_refs = []
        self.timeout = False
        self.operation_refs = []
        self.reconcile_refs = []
        self.resolve_calls = 0
        self.capability_calls = 0

    async def capabilities(self, target_ref):
        self.capability_calls += 1
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
        self.resolve_calls += 1
        if request.target_locator.kind.value == "self":
            return {"target_ref": "native-self-42", "kind": "self",
                    "display_name": "Saved messages"}
        return {"target_ref": "native-user-123", "kind": "user",
                "display_name": "Exact Person", "canonical_handle": "exact_person"}

    async def read(self, request):
        if request.operation is SocialReadOperation.EDITORIAL_SAMPLE:
            self.editorial_cursors.append(request.cursor)
            self.editorial_sample_refs.append(request.sample_ref)
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

    async def execute(self, intent, *, operation_ref):
        self.executions += 1
        self.operation_refs.append(operation_ref)
        if self.timeout:
            await asyncio.sleep(0.1)
        return {
            "target_ref": intent.target_ref or intent.destination_target_ref,
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
        self.reconcile_refs.append(operation_ref)
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
            "authorization_basis": "operator_authorized",
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
    assert len(set(adapter.editorial_sample_refs)) == 1
    assert adapter.editorial_sample_refs[0] == sample_ref


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


def test_publish_attempt_budget_uses_utc_day_not_hour(tmp_path: Path) -> None:
    current = [1_787_616_000]  # 2026-08-25T00:00:00Z
    service = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": FakeAdapter()},
        encryption_key="unit-test-key-that-is-long-enough",
        budget_dimension_limits={
            "attempts": {
                "global": 1,
                "principal": 1,
                "target": 1,
                "action": 1,
            }
        },
        clock=lambda: current[0],
    )
    principal = RuntimePrincipal.from_context(context())
    service._consume_budget(
        principal, "telegram", None, "send_message", "attempts", 1
    )
    current[0] += 23 * 3600
    with pytest.raises(SocialWorkspaceRuntimeError, match="attempts budget exceeded"):
        service._consume_budget(
            principal, "telegram", None, "send_message", "attempts", 1
        )
    current[0] += 3600
    service._consume_budget(
        principal, "telegram", None, "send_message", "attempts", 1
    )


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
    assert adapter.operation_refs == [result["operation_ref"]]
    assert adapter.reconcile_refs == [result["operation_ref"]]


@pytest.mark.asyncio
async def test_restart_left_provider_attempted_operation_reconciles_without_retry(
    runtime,
) -> None:
    service, adapter, store = runtime
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)
    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "send_message",
            "idempotency_key": "restart-inflight-123",
            "target_ref": target,
            "content": {"text": "Hello", "entities": [], "media": []},
        }
    )
    prep = await service.prepare(intent, context())
    approval = service.approve_preparation(
        preparation_ref=prep["preparation_ref"],
        operator_principal="operator",
        operator_nonce="restart-inflight-nonce-12345",
    )
    completed = await service.commit(
        {
            "preparation_ref": prep["preparation_ref"],
            **approval,
            "action_digest": prep["action_digest"],
        },
        context(),
    )
    with sqlite3.connect(store.path) as conn:
        conn.execute(
            """UPDATE social_workspace_operation
               SET status='provider_attempted',result_json=NULL,error_code=NULL
               WHERE operation_hash=?""",
            (service._hash(completed["operation_ref"]),),
        )
    executions = adapter.executions
    reconciled = await service.reconcile(completed["operation_ref"], context())
    assert reconciled["status"] == "failed"
    assert reconciled["retry_safe"] is False
    assert adapter.executions == executions
    assert adapter.reconcile_refs == [completed["operation_ref"]]


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


def test_catalog_omits_disabled_action_and_media_surfaces(runtime) -> None:
    service, _adapter, _store = runtime
    disabled = build_social_workspace_tools(
        service,
        feature_policy={
            "private_read": False,
            "dm": False,
            "post": False,
            "edit_delete": False,
            "media_story": False,
        },
    )
    names = {tool.name for tool in disabled}
    assert not {
        "social_action_prepare",
        "social_action_commit",
        "social_action_status",
        "social_content_stories",
        "social_asset_stage",
        "social_asset_status",
    } & names
    assert all(
        not any(
            scope.endswith(
                (
                    ":dm:send",
                    ":post:publish",
                    ":edit",
                    ":delete",
                    ":forward",
                    ":reaction",
                    ":comment",
                    ":schedule",
                    ":story:read",
                    ":story:write",
                )
            )
            for option in tool.scope_options
            for scope in option
        )
        for tool in disabled
    )

    dm_only = build_social_workspace_tools(
        service,
        feature_policy={
            "private_read": False,
            "dm": True,
            "post": False,
            "edit_delete": False,
            "media_story": False,
        },
    )
    prepare = next(tool for tool in dm_only if tool.name == "social_action_prepare")
    assert prepare.input_schema["properties"]["action"]["enum"] == ["send_message"]
    advertised = {
        scope for option in prepare.scope_options for scope in option
    }
    assert "telegram:dm:send" in advertised and "vk:dm:send" in advertised
    assert not any(scope.endswith(":post:publish") for scope in advertised)


@pytest.mark.asyncio
async def test_runtime_feature_policy_is_enforced_inside_handlers(runtime) -> None:
    service, adapter, _store = runtime
    tools = {
        tool.name: tool
        for tool in build_social_workspace_tools(
            service,
            feature_policy={
                "private_read": False,
                "dm": False,
                "post": True,
                "edit_delete": False,
                "media_story": False,
            },
        )
    }
    with pytest.raises(InvalidArgumentsError, match="private social reads are disabled"):
        await tools["social_content_feed"].handler(
            {
                "platform": "telegram",
                "operation": "list_items",
                "target_ref": "tgt_savedmessages0001",
                "read_access": "dialogs",
            },
            context(),
        )
    with pytest.raises(InvalidArgumentsError, match="action class is disabled"):
        await tools["social_action_prepare"].handler(
            {
                "platform": "telegram",
                "action": "send_message",
                "idempotency_key": "disabled-dm-123",
                "target_ref": "tgt_savedmessages0001",
                "content": {"text": "Hello", "entities": [], "media": []},
            },
            context(),
        )
    assert adapter.executions == 0


@pytest.mark.asyncio
async def test_disabled_action_kill_switch_revokes_stale_preparation_commit(
    runtime,
) -> None:
    service, adapter, _store = runtime
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-channel", "telegram", principal)
    prepared = await service.prepare(
        validate_prepare_request(
            {
                "platform": "telegram",
                "action": "publish",
                "idempotency_key": "stale-publish-kill-switch-123",
                "target_ref": target,
                "content": {
                    "text": "Must not publish",
                    "entities": [],
                    "media": [],
                },
            }
        ),
        context(),
    )
    service.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator",
        operator_nonce="stale-publish-kill-switch-nonce",
    )
    tools = {
        tool.name: tool
        for tool in build_social_workspace_tools(
            service,
            feature_policy={
                "private_read": False,
                "dm": True,
                "post": False,
                "edit_delete": False,
                "media_story": False,
            },
        )
    }
    with pytest.raises(InvalidArgumentsError, match="action class is disabled"):
        await tools["social_action_commit"].handler(
            {
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
            context(),
        )
    assert adapter.executions == 0


def test_thread_tool_exposes_comments_and_reactions_contract(runtime) -> None:
    service, _adapter, _store = runtime
    tool = next(
        item
        for item in build_social_workspace_tools(service)
        if item.name == "social_content_thread"
    )
    assert tool.input_schema["properties"]["operation"]["enum"] == [
        "list_comments",
        "list_reactions",
    ]
    assert len(tool.output_schema["oneOf"]) == 2


def test_auth_database_is_separate_and_event_database_is_untouched(tmp_path: Path) -> None:
    event_db = tmp_path / "events.sqlite"
    event_db.write_bytes(b"immutable-event-db-sentinel")
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    SocialWorkspaceRuntime(store=store, adapters={"telegram": FakeAdapter()},
                           encryption_key="unit-test-key-that-is-long-enough")
    assert event_db.read_bytes() == b"immutable-event-db-sentinel"
    assert (tmp_path / "auth.sqlite").stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_normal_read_projects_closed_contract_and_drops_native_identifiers(
    runtime,
) -> None:
    service, adapter, store = runtime

    async def hostile_read(request):
        return {
            "results": [{
                "target_ref": "native-community-777",
                "kind": "community",
                "title": "Named community",
                "about": "About",
                "description": "Description",
                "basic_metrics": {"members": 10},
                "trust": "untrusted_external_data",
                "provider_native_identifier": "native-secret-987654321",
                "innocent_new_provider_field": "must-not-leak",
            }],
            "trust": "untrusted_external_data",
            "provider_debug": "must-not-leak",
        }

    adapter.read = hostile_read
    result = await service.read(validate_read_request({
        "platform": "vk", "operation": "search_targets", "query": "named",
    }), context())
    encoded = json.dumps(result)
    assert "native-secret" not in encoded
    assert "innocent_new_provider_field" not in encoded
    assert "provider_debug" not in encoded
    assert result["results"][0]["target_ref"].startswith("tgt_")

    discovered_target = result["results"][0]["target_ref"]
    prepared = await service.prepare(validate_prepare_request({
        "platform": "vk", "action": "publish",
        "idempotency_key": "searched-target-publication-123",
        "target_ref": discovered_target,
        "content": {"text": "Exact publication", "entities": [], "media": []},
    }), context())
    preview = service.approval_preview(
        preparation_ref=prepared["preparation_ref"],
        action_digest=prepared["action_digest"],
    )
    assert preview["target"]["display_name"] == "Named community"

    source_item = service._mint_ref(
        "item", "native-source-item", "vk", RuntimePrincipal.from_context(context())
    )
    forward = await service.prepare(validate_prepare_request({
        "platform": "vk", "action": "forward",
        "idempotency_key": "searched-target-forward-123",
        "item_ref": source_item,
        "destination_target_ref": discovered_target,
    }), context())
    with pytest.raises(
        SocialWorkspaceRuntimeError, match="human item preview is unavailable"
    ):
        service.approval_preview(
            preparation_ref=forward["preparation_ref"],
            action_digest=forward["action_digest"],
        )

    native_target = service._resolve_ref(
        discovered_target,
        "target",
        "vk",
        RuntimePrincipal.from_context(context()),
    )

    async def item_read(request):
        return {
            "results": [{
                "item_ref": "native-item-123",
                "target_ref": native_target,
                "kind": "post",
                "published_at": "2026-08-08T12:00:00Z",
                "text": "Original exact item",
                "caption": "",
                "basic_metrics": {"views": 1},
                "trust": "untrusted_external_data",
            }],
            "trust": "untrusted_external_data",
        }

    adapter.read = item_read
    feed = await service.read(validate_read_request({
        "platform": "vk", "operation": "list_items",
        "target_ref": discovered_target, "read_access": "public",
    }), context())
    item_ref = feed["results"][0]["item_ref"]
    visible_forward = await service.prepare(validate_prepare_request({
        "platform": "vk", "action": "forward",
        "idempotency_key": "visible-item-forward-123",
        "item_ref": item_ref,
        "destination_target_ref": discovered_target,
    }), context())
    forward_preview = service.approval_preview(
        preparation_ref=visible_forward["preparation_ref"],
        action_digest=visible_forward["action_digest"],
    )
    assert forward_preview["destination_target"]["display_name"] == "Named community"
    assert forward_preview["item"]["text"] == "Original exact item"
    edit = await service.prepare(validate_prepare_request({
        "platform": "vk", "action": "edit",
        "idempotency_key": "exact-item-edit-123",
        "item_ref": item_ref,
        "content": {"text": "Edited exact item", "entities": [], "media": []},
    }), context())
    edit_preview = service.approval_preview(
        preparation_ref=edit["preparation_ref"],
        action_digest=edit["action_digest"],
    )
    assert edit_preview["item"]["text"] == "Original exact item"
    assert edit_preview["source_target"]["display_name"] == "Named community"
    raw_state = Path(store.path).read_bytes()
    assert b"Named community" not in raw_state
    assert b"Original exact item" not in raw_state


@pytest.mark.asyncio
async def test_provider_exception_is_sanitized_in_tool_error_and_audit(
    runtime,
) -> None:
    service, adapter, store = runtime
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)

    async def hostile_execute(intent, *, operation_ref):
        adapter.executions += 1
        raise RuntimeError("Bearer TOPSECRETTOKEN /v1/messages.send")

    adapter.execute = hostile_execute
    intent = validate_prepare_request({
        "platform": "telegram", "action": "send_message",
        "idempotency_key": "hostile-error-123", "target_ref": target,
        "content": {"text": "Hello", "entities": [], "media": []},
    })
    prepared = await service.prepare(intent, context())
    approval = service.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator", operator_nonce="hostile-error-nonce-123",
    )
    commit_tool = next(
        tool for tool in build_social_workspace_tools(service)
        if tool.name == "social_action_commit"
    )
    with pytest.raises(InvalidArgumentsError) as caught:
        await commit_tool.handler({
            "preparation_ref": prepared["preparation_ref"], **approval,
            "action_digest": prepared["action_digest"],
        }, context())
    assert "TOPSECRETTOKEN" not in str(caught.value)
    assert "messages.send" not in str(caught.value)
    with sqlite3.connect(store.path) as conn:
        audit = json.dumps(conn.execute(
            "SELECT platform,operation,outcome,reason_code FROM social_workspace_audit"
        ).fetchall())
    assert "TOPSECRETTOKEN" not in audit and "messages.send" not in audit


@pytest.mark.asyncio
async def test_disabled_provider_is_enforced_by_handler_not_only_descriptor(runtime) -> None:
    service, _telegram, _store = runtime
    vk = FakeAdapter()
    service.adapters["vk"] = vk
    resolve_tool = next(
        tool for tool in build_social_workspace_tools(
            service, capability_policy={"telegram": True, "vk": False}
        ) if tool.name == "social_target_resolve"
    )
    with pytest.raises(InvalidArgumentsError, match="platform is unavailable"):
        await resolve_tool.handler({
            "platform": "vk", "operation": "resolve_target",
            "target_locator": {"kind": "username", "value": "named"},
            "expected_target_kinds": ["user"],
        }, context())
    assert vk.resolve_calls == 0


def test_denial_audit_normalizes_attacker_controlled_dimensions(runtime) -> None:
    service, _adapter, store = runtime
    service.audit_denial(
        context(), platform="Bearer AUDITSECRETTOKEN",
        operation="password=hunter2", reason="Bad Value\nBearer SECRET",
        target_ref="Bearer TARGETSECRET",
    )
    with sqlite3.connect(store.path) as conn:
        row = conn.execute(
            "SELECT platform,operation,reason_code,target_ref_hash "
            "FROM social_workspace_audit ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row == (None, "invalid", "bad_value_bearer_secret", None)


@pytest.mark.asyncio
async def test_approval_capabilities_are_hash_only_at_rest(runtime) -> None:
    service, _adapter, store = runtime
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)
    intent = validate_prepare_request({
        "platform": "telegram", "action": "send_message",
        "idempotency_key": "hash-only-123", "target_ref": target,
        "content": {"text": "Hello", "entities": [], "media": []},
    })
    prepared = await service.prepare(intent, context())
    approval = service.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator", operator_nonce="hash-only-nonce-12345",
    )
    with sqlite3.connect(store.path) as conn:
        stored = conn.execute(
            "SELECT approval_ref,receipt_ref FROM social_workspace_approval"
        ).fetchone()
    assert stored[0] != approval["approval_ref"]
    assert stored[1] != approval["approval_receipt"]
    assert stored == (
        service._hash(approval["approval_ref"]),
        service._hash(approval["approval_receipt"]),
    )
    status = await service.status("preparation", prepared["preparation_ref"], context())
    assert status["status"] == "approved"
    committed = await service.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context(),
    )
    assert committed["status"] == "succeeded"


@pytest.mark.asyncio
async def test_provider_success_followed_by_egress_denial_is_not_reported_failed(
    tmp_path: Path,
) -> None:
    adapter = FakeAdapter()
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store, adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        budget_dimension_limits={
            "egress": {name: 1 for name in ("global", "principal", "target", "action")}
        },
    )
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)
    intent = validate_prepare_request({
        "platform": "telegram", "action": "send_message",
        "idempotency_key": "withheld-123", "target_ref": target,
        "content": {"text": "Hello", "entities": [], "media": []},
    })
    prepared = await service.prepare(intent, context())
    approval = service.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator", operator_nonce="withheld-nonce-12345",
    )
    result = await service.commit({
        "preparation_ref": prepared["preparation_ref"], **approval,
        "action_digest": prepared["action_digest"],
    }, context())
    assert result["status"] == "outcome_unknown"
    assert result["error_code"] == "response_withheld"
    assert result["retry_safe"] is False
    assert adapter.executions == 1
    stored = await service.status("operation", result["operation_ref"], context())
    assert stored == result
    with sqlite3.connect(store.path) as conn:
        outcomes = [row[0] for row in conn.execute(
            "SELECT outcome FROM social_workspace_audit WHERE operation='commit'"
        )]
    assert "failed" not in outcomes
    assert "succeeded_response_withheld" in outcomes


@pytest.mark.asyncio
async def test_reminted_same_native_target_shares_durable_target_budget(
    tmp_path: Path,
) -> None:
    adapter = FakeAdapter()
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store, adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        budget_dimension_limits={
            "rate": {"global": 100, "principal": 100, "target": 1, "action": 100}
        },
    )
    principal = RuntimePrincipal.from_context(context())
    first = service._mint_ref("target", "same-native-target", "telegram", principal)
    second = service._mint_ref("target", "same-native-target", "telegram", principal)
    await service.capabilities(first, context(), platform="telegram")
    with pytest.raises(SocialWorkspaceRuntimeError, match="rate budget exceeded"):
        await service.capabilities(second, context(), platform="telegram")
    assert adapter.capability_calls == 1


@pytest.mark.asyncio
async def test_forward_attempt_budget_is_independent_per_destination(
    tmp_path: Path,
) -> None:
    adapter = FakeAdapter()
    service = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        budget_dimension_limits={
            "attempts": {
                "global": 10,
                "principal": 10,
                "target": 1,
                "action": 10,
            }
        },
    )
    principal = RuntimePrincipal.from_context(context())

    async def forward(index: int, destination: str) -> dict[str, Any]:
        item = service._mint_ref(
            "item", f"native-item-{index}", "telegram", principal
        )
        service._store_item_preview(
            item,
            {
                "item_ref": item,
                "target_ref": destination,
                "kind": "message",
                "text": f"Source {index}",
            },
        )
        prepared = await service.prepare(
            validate_prepare_request(
                {
                    "platform": "telegram",
                    "action": "forward",
                    "idempotency_key": f"forward-target-budget-{index}",
                    "item_ref": item,
                    "destination_target_ref": destination,
                }
            ),
            context(),
        )
        service.approve_preparation(
            preparation_ref=prepared["preparation_ref"],
            operator_principal="operator",
            operator_nonce=f"forward-target-budget-nonce-{index}",
        )
        return await service.commit(
            {
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
            context(),
        )

    first_target = service._mint_ref(
        "target", "native-destination-1", "telegram", principal
    )
    second_target = service._mint_ref(
        "target", "native-destination-2", "telegram", principal
    )
    assert (await forward(1, first_target))["status"] == "succeeded"
    assert (await forward(2, second_target))["status"] == "succeeded"
    with pytest.raises(SocialWorkspaceRuntimeError, match="attempts budget exceeded"):
        await forward(3, first_target)
    assert adapter.executions == 2



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
