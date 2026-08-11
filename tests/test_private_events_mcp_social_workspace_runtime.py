from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass, replace
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

import private_events_mcp.social_workspace_runtime as runtime_module
from private_events_mcp.auth_store import OAuthStateStore, OAuthStoreError
from private_events_mcp.crypto import AccessIdentity, pkce_s256
from private_events_mcp.protocol import MCPProtocol
from private_events_mcp.repository import InvalidArgumentsError
from private_events_mcp.social_workspace import (
    MediaRole,
    SocialAction,
    SocialReadOperation,
    compute_action_digest,
    validate_asset_stage_request,
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
) | frozenset({"vk:notifications:read"})


def context(*, client: str = "chatgpt", subject: str = "alice", resource: str = "https://mcp") -> ToolCallContext:
    return ToolCallContext(
        AccessIdentity(subject, client, ALL_SCOPES, resource, "jti", int(time.time()) + 3600),
        resource,
    )


def scoped_context(*scopes: str) -> ToolCallContext:
    return ToolCallContext(
        AccessIdentity(
            "alice",
            "chatgpt",
            frozenset(scopes),
            "https://mcp",
            "jti-scoped",
            int(time.time()) + 3600,
        ),
        "https://mcp",
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
        self.asset_bytes: bytes | None = None

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
        if request.operation is SocialReadOperation.LIST_DIALOGS:
            return {
                "results": [
                    {
                        "target_ref": "native-dialog-user-123",
                        "kind": "user",
                        "title": "Ticket Winner",
                        "unread_count": 2,
                        "text": "private body must be projected out",
                        "provider_id": "must-not-leak",
                        "trust": "untrusted_external_data",
                    }
                ],
                "trust": "untrusted_external_data",
            }
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

    async def read_asset(self, asset_ref, *, owner_binding, max_bytes):
        assert asset_ref.startswith("provider-asset-")
        assert len(owner_binding) == 64
        assert self.asset_bytes is not None
        assert len(self.asset_bytes) <= max_bytes
        return self.asset_bytes


@dataclass(frozen=True)
class FakeDocumentAsset:
    storage_ref: str
    owner_binding: str
    role: str
    content_digest: str
    mime_type: str
    byte_length: int
    expires_at: int
    width: int | None = None
    height: int | None = None
    display_name: str | None = None
    classification: str | None = None


class FakeDocumentIngestor:
    def __init__(self, now: int) -> None:
        self.now = now
        self.asset: FakeDocumentAsset | None = None
        self.reverify_calls = 0
        self.file_names: list[str | None] = []
        self.reverify_delay = 0.0

    async def ingest(
        self, file, *, owner_binding, max_bytes, expires_at, role
    ):
        assert role == "document"
        assert max_bytes == 48 * 1024 * 1024
        self.file_names.append(file.file_name)
        self.asset = FakeDocumentAsset(
            storage_ref="ing_" + "d" * 24,
            owner_binding=owner_binding,
            role="document",
            content_digest="sha256:" + "e" * 64,
            mime_type="application/vnd.android.package-archive",
            byte_length=128,
            expires_at=expires_at,
            display_name="safe.apk",
            classification="apk",
        )
        return self.asset

    def reverify(self, storage_ref, *, owner_binding, max_bytes, role):
        self.reverify_calls += 1
        if self.reverify_delay:
            time.sleep(self.reverify_delay)
        assert self.asset is not None
        return self.asset


class FakeDocumentAdapter(FakeAdapter):
    async def capabilities(self, target_ref):
        value = dict(await super().capabilities(target_ref))
        value["content_features"] = ["rich_text", "image", "document"]
        return value

    async def stage_asset(self, asset, *, role):
        assert role is MediaRole.DOCUMENT
        return "provider-document-binding"

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
async def test_story_asset_preview_returns_bounded_mcp_image_not_provider_reference(
    runtime,
) -> None:
    service, adapter, _store = runtime
    source = BytesIO()
    Image.new("RGB", (1600, 900), (20, 80, 140)).save(source, format="PNG")
    adapter.asset_bytes = source.getvalue()
    legacy = scoped_context("telegram:read")
    principal = RuntimePrincipal.from_context(legacy)
    asset_ref = service._mint_ref(
        "asset", "provider-asset-story-42", "telegram", principal
    )
    tools = build_social_workspace_tools(
        service,
        feature_policy={"media_story": True},
        capability_policy={"telegram": True, "vk": False},
    )
    protocol = MCPProtocol(
        tools,
        cache_ttl_seconds=60,
        challenge='Bearer error="invalid_token"',
        resource=legacy.resource,
        allowed_client_ids=frozenset({legacy.identity.client_id}),
    )
    response = await protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 91,
            "method": "tools/call",
            "params": {
                "name": "social_asset_preview",
                "arguments": {"platform": "telegram", "asset_ref": asset_ref},
            },
        },
        legacy.identity,
    )
    result = response["result"]
    assert result["isError"] is False
    assert result["structuredContent"] == {
        "platform": "telegram",
        "asset_ref": asset_ref,
        "mime_type": "image/jpeg",
        "byte_length": result["structuredContent"]["byte_length"],
        "width": 768,
        "height": 432,
        "trust": "untrusted_external_data",
    }
    image = result["content"][0]
    assert image["type"] == "image"
    assert image["mimeType"] == "image/jpeg"
    assert len(image["data"]) < 90_000
    encoded = json.dumps(response)
    assert "provider-asset-story-42" not in encoded
    assert "download_url" not in encoded


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
async def test_exact_user_dm_prepare_is_directly_approved_commit_and_replay(runtime) -> None:
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
    assert prepared["status"] == "approved"
    assert replay["status"] == "approved"
    assert "approval_url" not in prepared
    result = await service.commit({
        "preparation_ref": prepared["preparation_ref"],
        "action_digest": prepared["action_digest"],
    }, context())
    assert result["status"] == "succeeded"
    assert result["target_ref"] == resolved["target_ref"]
    assert result["item_ref"] == result["read_after_write"]["observed_item_ref"]
    assert "raw_method" not in result and "access_token" not in result
    assert adapter.executions == 1
    with pytest.raises(SocialWorkspaceRuntimeError):
        await service.commit({"preparation_ref": prepared["preparation_ref"],
                              "action_digest": prepared["action_digest"]}, context())
    assert adapter.executions == 1


@pytest.mark.asyncio
async def test_destructive_edit_still_requires_external_approval(runtime) -> None:
    service, adapter, _store = runtime
    principal = RuntimePrincipal.from_context(context())
    item = service._mint_ref("item", "native-message-42", "telegram", principal)
    intent = validate_prepare_request({
        "platform": "telegram", "action": "edit",
        "idempotency_key": "edit-external-approval-123", "item_ref": item,
        "content": {"text": "Corrected", "entities": [], "media": []},
    })
    prepared = await service.prepare(intent, context())
    assert prepared["status"] == "awaiting_human_approval"
    with pytest.raises(SocialWorkspaceRuntimeError, match="approval"):
        await service.commit({
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        }, context())
    assert adapter.executions == 0


@pytest.mark.asyncio
async def test_existing_awaiting_outbound_preparation_is_not_auto_upgraded(
    runtime,
) -> None:
    service, adapter, _store = runtime
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-old-user", "telegram", principal)
    payload = {
        "platform": "telegram",
        "action": "send_message",
        "idempotency_key": "old-awaiting-send-123",
        "target_ref": target,
        "content": {"text": "Old request", "entities": [], "media": []},
    }
    intent = validate_prepare_request(payload)
    digest = compute_action_digest(intent)
    prep = "prep_" + "o" * 24
    now = service._now()
    client, subject, resource = service._binding(principal)
    with service.store._lock, service.store._connect() as conn:
        conn.execute(
            """INSERT INTO social_workspace_preparation(preparation_hash,preparation_ref,
               client_hash,subject_hash,resource_hash,platform,action,target_ref_hash,
               action_digest,idempotency_hash,intent_ciphertext,status,expires_at,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                service._hash(prep), prep, client, subject, resource, "telegram",
                "send_message", service._hash(target), digest,
                service._hash(intent.idempotency_key),
                service._encrypt(json.dumps(payload, sort_keys=True)),
                "awaiting_human_approval", now + 600, now,
            ),
        )
    replay = await service.prepare(intent, context())
    assert replay["preparation_ref"] == prep
    assert replay["status"] == "awaiting_human_approval"
    with pytest.raises(SocialWorkspaceRuntimeError, match="approval"):
        await service.commit(
            {"preparation_ref": prep, "action_digest": digest}, context()
        )
    assert adapter.executions == 0


@pytest.mark.asyncio
async def test_legacy_publish_scope_runs_typed_prepare_approval_commit_end_to_end(
    runtime,
) -> None:
    service, adapter, _store = runtime
    legacy = scoped_context("telegram:publish")
    principal = RuntimePrincipal.from_context(legacy)
    target = service._mint_ref("target", "native-user-legacy", "telegram", principal)
    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "send_message",
            "idempotency_key": "legacy-typed-dm-123",
            "target_ref": target,
            "content": {"text": "Hello", "entities": [], "media": []},
        }
    )
    prepared = await service.prepare(intent, legacy)
    approval = service.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator@example.test",
        operator_nonce="legacy-typed-dm-nonce-123456",
    )
    result = await service.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            **approval,
            "action_digest": prepared["action_digest"],
        },
        legacy,
    )
    assert result["status"] == "succeeded"
    assert adapter.executions == 1

    cross_provider = scoped_context("vk:publish")
    with pytest.raises(SocialWorkspaceRuntimeError, match="scope is missing"):
        await service.prepare(intent, cross_provider)
    read_only = scoped_context("telegram:read")
    with pytest.raises(SocialWorkspaceRuntimeError, match="scope is missing"):
        await service.prepare(intent, read_only)
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


def test_vk_item_and_notification_tools_are_provider_and_scope_isolated(runtime) -> None:
    service, _adapter, _store = runtime
    telegram_only = {
        tool.name
        for tool in build_social_workspace_tools(
            service, capability_policy={"telegram": True, "vk": False}
        )
    }
    assert "social_item_resolve" not in telegram_only
    assert "social_comment_hints_list" not in telegram_only
    assert "social_dialogs_list" not in telegram_only

    service.adapters["vk"] = FakeAdapter()
    tools = {
        tool.name: tool for tool in build_social_workspace_tools(service)
    }
    assert "social_item_resolve" in tools
    hints = tools["social_comment_hints_list"]
    dialogs = tools["social_dialogs_list"]
    assert frozenset({"vk:notifications:read"}) in hints.scope_options
    assert hints.scope_selector(
        {"platform": "vk", "operation": "list_notifications", "limit": 25}
    ) == {"vk:notifications:read"}
    assert set(dialogs.scope_options) == {
        frozenset({"vk:read"}),
        frozenset({"vk:read:dialogs"}),
    }
    assert dialogs.input_schema["properties"]["platform"] == {"const": "vk"}
    assert dialogs.input_schema["properties"]["read_access"] == {"const": "dialogs"}


@pytest.mark.asyncio
async def test_vk_dialog_tool_returns_metadata_only_for_legacy_read_scope(runtime) -> None:
    service, _adapter, _store = runtime
    tool = next(
        item
        for item in build_social_workspace_tools(service)
        if item.name == "social_dialogs_list"
    )
    output = await tool.handler(
        {
            "platform": "vk",
            "operation": "list_dialogs",
            "read_access": "dialogs",
            "unread_only": True,
            "limit": 20,
        },
        scoped_context("vk:read"),
    )
    assert output["results"][0]["title"] == "Ticket Winner"
    assert output["results"][0]["unread_count"] == 2
    assert output["results"][0]["target_ref"].startswith("tgt_")
    encoded = json.dumps(output)
    assert "private body" not in encoded
    assert "provider_id" not in encoded
    assert "native-dialog-user-123" not in encoded


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
        "social_dialogs_list",
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
async def test_document_runtime_reverifies_digest_and_kill_switch(
    tmp_path: Path, monkeypatch
) -> None:
    now = 1_800_000_000
    ingestor = FakeDocumentIngestor(now)
    adapter = FakeDocumentAdapter()
    monkeypatch.setattr(runtime_module, "VerifiedAsset", FakeDocumentAsset)
    service = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "document.sqlite")),
        adapters={"telegram": adapter, "vk": adapter},
        encryption_key="document-runtime-test-key-long-enough",
        asset_ingestor=ingestor,
        media_story_enabled=False,
        file_send_enabled=True,
        clock=lambda: now,
    )
    tools = build_social_workspace_tools(
        service,
        feature_policy={
            "dm": True,
            "media_story": False,
            "file_send": True,
            "asset_ingress": True,
        },
        capability_policy={"telegram": True, "vk": True},
    )
    stage_tool = next(item for item in tools if item.name == "social_asset_stage")
    assert stage_tool.input_schema["properties"]["role"]["enum"] == ["document"]
    staged = await service.stage_asset(
        validate_asset_stage_request(
            {
                "platform": "telegram",
                "file": {
                    "download_url": "https://files.example.test/document",
                    "file_id": "file-document",
                    "mime_type": "application/vnd.android.package-archive",
                    "file_name": "../unsafe\u202e.apk",
                },
                "role": "document",
            }
        ),
        scoped_context("telegram:dm:send"),
    )
    principal = RuntimePrincipal.from_context(scoped_context("telegram:dm:send"))
    target = service._mint_ref("target", "native-self", "telegram", principal)
    service._store_target_preview(
        target,
        {
            "platform": "telegram",
            "target_ref": target,
            "kind": "self",
            "display_name": "Saved Messages",
        },
    )
    telegram_caps = await service.capabilities(
        target, scoped_context("telegram:dm:send"), platform="telegram"
    )
    assert "document" in telegram_caps["content_features"]
    vk_caps = await service.capabilities(None, context(), platform="vk")
    assert "document" not in vk_caps["content_features"]
    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "send_message",
            "idempotency_key": "document-runtime-123",
            "target_ref": target,
            "content": {
                "text": "caption",
                "entities": [],
                "media": [{"asset_ref": staged["asset_ref"], "role": "document"}],
            },
        }
    )
    ingestor.reverify_delay = 0.05
    service.provider_timeout_seconds = 0.01
    service.asset_ingest_timeout_seconds = 0.01
    with pytest.raises(SocialWorkspaceRuntimeError, match="reverification timed out"):
        await service.prepare(intent, scoped_context("telegram:dm:send"))
    assert adapter.executions == 0

    service.provider_timeout_seconds = 0.2
    service.asset_ingest_timeout_seconds = 0.2
    prepare_task = asyncio.create_task(
        service.prepare(intent, scoped_context("telegram:dm:send"))
    )
    heartbeat_seen = False
    await asyncio.sleep(0.005)
    heartbeat_seen = True
    assert heartbeat_seen is True
    assert prepare_task.done() is False
    prepared = await prepare_task
    assert ingestor.reverify_calls >= 2
    assert ingestor.file_names == ["../unsafe\u202e.apk"]
    status = await service.asset_status(
        staged["asset_ref"], scoped_context("telegram:dm:send")
    )
    assert status["display_name"] == "safe.apk"
    assert status["classification"] == "apk"
    preview = service.approval_preview(
        preparation_ref=prepared["preparation_ref"],
        action_digest=prepared["action_digest"],
    )
    encoded_preview = json.dumps(preview)
    assert "safe.apk" in encoded_preview
    assert "application/vnd.android.package-archive" in encoded_preview
    assert "../unsafe\u202e.apk" not in encoded_preview
    assert "ing_" not in encoded_preview
    assert "provider-document-binding" not in encoded_preview
    assert "../unsafe\u202e.apk".encode("utf-8") not in Path(
        service.store.path
    ).read_bytes()
    assert ingestor.asset is not None
    ingestor.asset = replace(
        ingestor.asset, content_digest="sha256:" + "f" * 64
    )
    with pytest.raises(SocialWorkspaceRuntimeError, match="changed"):
        await service.commit(
            {
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
            scoped_context("telegram:dm:send"),
        )
    assert adapter.executions == 0

    ingestor.asset = replace(
        ingestor.asset, content_digest="sha256:" + "e" * 64
    )
    service.file_send_enabled = False
    with pytest.raises(SocialWorkspaceRuntimeError, match="disabled"):
        await service.commit(
            {
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
            scoped_context("telegram:dm:send"),
        )
    assert adapter.executions == 0

    telegram_caps = await service.capabilities(
        target, scoped_context("telegram:dm:send"), platform="telegram"
    )
    assert "document" not in telegram_caps["content_features"]


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

    global_item = service._mint_ref(
        "item", "native-global-result", "vk", RuntimePrincipal.from_context(context())
    )
    service._store_item_preview(
        global_item,
        {
            "item_ref": global_item,
            "kind": "post",
            "published_at": "2026-08-08T12:00:00Z",
            "text": "Exact global result without source identity",
        },
    )
    destructive = await service.prepare(validate_prepare_request({
        "platform": "vk", "action": "delete",
        "idempotency_key": "global-result-delete-123",
        "item_ref": global_item,
    }), context())
    with pytest.raises(
        SocialWorkspaceRuntimeError,
        match="human source target preview is unavailable",
    ):
        service.approval_preview(
            preparation_ref=destructive["preparation_ref"],
            action_digest=destructive["action_digest"],
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
