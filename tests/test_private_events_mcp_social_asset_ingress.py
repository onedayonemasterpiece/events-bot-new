from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from private_events_mcp.auth_store import OAuthStateStore
from private_events_mcp.config import PrivateEventsMCPConfig, _hosts
from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.media_contract import ChatGPTFile, VerifiedAsset
from private_events_mcp.protocol import MCPProtocol
from private_events_mcp.server import PrivateEventsMCPServer
from private_events_mcp.social_workspace import (
    SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA,
    MediaRole,
    SocialWorkspaceValidationError,
    validate_asset_stage_request,
    validate_prepare_request,
)
from private_events_mcp.social_workspace_runtime import (
    RuntimePrincipal,
    SocialWorkspaceRuntime,
    SocialWorkspaceRuntimeError,
)
from private_events_mcp.social_workspace_tools import build_social_workspace_tools
from private_events_mcp.tool_catalog import ToolCallContext, ToolExecutionError
from private_events_mcp_media import MediaIngressRejected, SecureMediaAssetStore

FILE_VALUE = {
    "download_url": "https://files.example.test/signed/private-image?signature=secret",
    "file_id": "file_private_123",
    "mime_type": "image/png",
    "file_name": "poster.png",
}


def test_media_allowed_hosts_supports_only_safe_leading_wildcards(monkeypatch) -> None:
    monkeypatch.setenv(
        "PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS",
        "files.example.test,*.Uploads.Example.test,files.example.test",
    )
    assert _hosts("PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS") == (
        "files.example.test",
        "*.uploads.example.test",
    )
    for invalid in ("*", "foo.*.example.test", "*.127.0.0.1", "*.2130706433"):
        monkeypatch.setenv("PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS", invalid)
        with pytest.raises(ValueError):
            _hosts("PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS")


def test_disabled_mcp_ignores_malformed_media_host_policy(monkeypatch) -> None:
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_ENABLED", "0")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_MEDIA_ALLOWED_HOSTS", "bad.*.host")
    assert PrivateEventsMCPConfig.from_env().media_allowed_hosts == ()


def _context(
    *, subject: str = "alice", scopes: frozenset[str] | None = None
) -> ToolCallContext:
    resource = "https://mcp.example.test/private"
    return ToolCallContext(
        AccessIdentity(
            subject,
            "chatgpt-client",
            scopes if scopes is not None else frozenset({"telegram:publish"}),
            resource,
            "jti",
            2_000_000_000,
        ),
        resource,
    )


class FakeIngestor:
    def __init__(self) -> None:
        self.calls: list[tuple[ChatGPTFile, str, int, int]] = []
        self.override: dict[str, object] = {}

    async def ingest(self, file, *, owner_binding, max_bytes, expires_at):
        self.calls.append((file, owner_binding, max_bytes, expires_at))
        values = {
            "storage_ref": "ing_" + "a" * 24,
            "owner_binding": owner_binding,
            "content_digest": "sha256:" + "b" * 64,
            "mime_type": "image/png",
            "byte_length": 4096,
            "expires_at": expires_at,
            "width": 1200,
            "height": 1600,
        }
        values.update(self.override)
        return VerifiedAsset(**values)


class FakeAdapter:
    def __init__(self) -> None:
        self.staged: list[tuple[VerifiedAsset, object]] = []
        self.executions = 0
        self.last_intent = None

    async def capabilities(self, target_ref):
        return {
            "target_kinds": ["channel"],
            "read_operations": ["list_items"],
            "actions": ["publish", "story"],
            "content_features": [
                "rich_text",
                "image",
                "video",
                "document",
                "audio",
                "animation",
            ],
            "max_text_length": 4096,
            "max_media_items": 10,
        }

    async def resolve(self, request):
        return {}

    async def read(self, request):
        return {}

    async def execute(self, intent, *, operation_ref):
        self.executions += 1
        self.last_intent = intent
        return {
            "target_ref": intent.target_ref,
            "item_ref": "provider-sent-image-99",
            "status": "succeeded",
            "retry_safe": False,
            "read_after_write": {
                "verified": True,
                "observed_item_ref": "provider-sent-image-99",
                "observed_at": "2027-01-15T08:00:00Z",
            },
        }

    async def reconcile(self, operation_ref):
        return {}

    async def stage_asset(self, asset, *, role):
        self.staged.append((asset, role))
        return "provider-asset-handle"

    async def read_asset(self, asset_ref, *, owner_binding, max_bytes):
        return b"not-used-by-ingress-tests"


@pytest.fixture
def asset_runtime(tmp_path: Path):
    now = [1_800_000_000]
    ingestor = FakeIngestor()
    adapter = FakeAdapter()
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="asset-ingress-test-key-long-enough",
        asset_ingestor=ingestor,
        asset_ttl_seconds=3600,
        clock=lambda: now[0],
    )
    return runtime, ingestor, adapter, now


def _request(**file_updates):
    return validate_asset_stage_request(
        {
            "platform": "telegram",
            "file": {**FILE_VALUE, **file_updates},
            "role": "image",
        }
    )


def _asset_tools(runtime):
    return {
        tool.name: tool
        for tool in build_social_workspace_tools(
            runtime,
            feature_policy={"media_story": True, "post": True},
            capability_policy={"telegram": True, "vk": False},
        )
    }


async def _stage_through_protocol(runtime, file_value):
    context = _context()
    protocol = MCPProtocol(
        tuple(_asset_tools(runtime).values()),
        cache_ttl_seconds=0,
        challenge='Bearer error="invalid_token"',
        resource=context.resource,
        allowed_client_ids=frozenset({context.identity.client_id}),
    )
    return await protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 81,
            "method": "tools/call",
            "params": {
                "name": "social_asset_stage",
                "arguments": {
                    "platform": "telegram",
                    "file": file_value,
                    "role": "image",
                },
            },
        },
        context.identity,
    )


def test_official_file_param_descriptor_and_schema_are_exact(asset_runtime) -> None:
    runtime, _ingestor, _adapter, _now = asset_runtime
    stage = _asset_tools(runtime)["social_asset_stage"]
    preview = _asset_tools(runtime)["social_asset_preview"]
    assert stage.timeout_seconds >= (
        runtime.asset_ingest_timeout_seconds + runtime.provider_timeout_seconds
    )
    assert preview.timeout_seconds >= runtime.provider_timeout_seconds
    descriptor = stage.descriptor(frozenset({"telegram:publish"}))
    assert descriptor["_meta"]["openai/fileParams"] == ["file"]
    assert descriptor["_meta"]["securitySchemes"] == descriptor["securitySchemes"]
    schema = descriptor["inputSchema"]
    assert schema["properties"]["platform"]["enum"] == ["telegram"]
    assert (
        schema["$defs"]["OpenAIFile"]
        == SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA["$defs"]["OpenAIFile"]
    )
    assert schema["required"] == ["platform", "file", "role"]
    assert set(schema["properties"]) == {"platform", "file", "role"}
    assert schema["properties"]["role"]["enum"] == ["image"]
    file_schema = schema["$defs"]["OpenAIFile"]
    assert set(file_schema["properties"]) == {
        "download_url",
        "file_id",
        "mime_type",
        "file_name",
    }
    assert file_schema["required"] == ["download_url", "file_id"]
    assert file_schema["additionalProperties"] is False
    assert not {
        "upload_ref",
        "content_digest",
        "byte_length",
    } & set(schema["properties"])
    with pytest.raises(SocialWorkspaceValidationError, match="only image"):
        validate_asset_stage_request(
            {"platform": "telegram", "file": FILE_VALUE, "role": "video"}
        )


@pytest.mark.asyncio
async def test_stage_returns_safe_diagnostic_code_for_unresolved_string_file(
    asset_runtime,
) -> None:
    runtime, ingestor, adapter, _now = asset_runtime
    response = await _stage_through_protocol(
        runtime, "sandbox:/mnt/data/symphonic concert.png"
    )
    result = response["result"]
    assert result["isError"] is True
    assert result["structuredContent"] == {
        "error_code": "FILE_REF_UNRESOLVED",
        "retry_safe": False,
    }
    assert "sandbox" not in json.dumps(result)
    assert ingestor.calls == []
    assert adapter.staged == []


@pytest.mark.asyncio
async def test_stage_preserves_safe_ingress_code_without_leaking_file_fields(
    asset_runtime,
) -> None:
    runtime, _ingestor, adapter, _now = asset_runtime

    class RejectingIngestor:
        async def ingest(self, file, *, owner_binding, max_bytes, expires_at):
            raise MediaIngressRejected("download host is not allowlisted")

    runtime.asset_ingestor = RejectingIngestor()
    response = await _stage_through_protocol(runtime, FILE_VALUE)
    result = response["result"]
    assert result["isError"] is True
    assert result["structuredContent"] == {
        "error_code": "FILE_HOST_NOT_ALLOWED",
        "retry_safe": False,
    }
    encoded = json.dumps(result)
    assert FILE_VALUE["download_url"] not in encoded
    assert FILE_VALUE["file_id"] not in encoded
    assert adapter.staged == []


@pytest.mark.asyncio
async def test_real_ingestor_host_denial_is_coded_and_audited_without_host(
    tmp_path: Path,
) -> None:
    now = [1_800_000_000]
    adapter = FakeAdapter()
    ingestor = SecureMediaAssetStore(
        tmp_path / "media",
        allowed_hosts=["files.oaiusercontent.com"],
        clock=lambda: now[0],
    )
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="asset-ingress-test-key-long-enough",
        asset_ingestor=ingestor,
        clock=lambda: now[0],
    )
    response = await _stage_through_protocol(runtime, FILE_VALUE)
    assert response["result"]["structuredContent"] == {
        "error_code": "FILE_HOST_NOT_ALLOWED",
        "retry_safe": False,
    }
    with sqlite3.connect(runtime.store.path) as conn:
        reason = conn.execute(
            """SELECT reason_code FROM social_workspace_audit
               WHERE operation='asset_stage' ORDER BY id DESC LIMIT 1"""
        ).fetchone()[0]
    assert reason.startswith("file_host_not_allowed_")
    assert "files.example.test" not in reason
    assert adapter.staged == []


@pytest.mark.asyncio
async def test_runtime_rejects_non_image_before_ingestor(asset_runtime) -> None:
    runtime, ingestor, adapter, _now = asset_runtime
    with pytest.raises(SocialWorkspaceRuntimeError, match="only image"):
        await runtime.stage_asset(
            replace(_request(), role=MediaRole.VIDEO), _context()
        )
    assert ingestor.calls == []
    assert adapter.staged == []


@pytest.mark.asyncio
async def test_runtime_authorizes_before_ingestor(asset_runtime) -> None:
    runtime, ingestor, adapter, _now = asset_runtime
    with pytest.raises(SocialWorkspaceRuntimeError, match="scope is missing"):
        await runtime.stage_asset(
            _request(), _context(scopes=frozenset({"events:read"}))
        )
    assert ingestor.calls == []
    assert adapter.staged == []


@pytest.mark.asyncio
async def test_capabilities_advertise_image_only_for_asset_staging(asset_runtime) -> None:
    runtime, _ingestor, _adapter, _now = asset_runtime
    result = await runtime.capabilities(None, _context(), platform="telegram")
    assert result["content_features"] == ["image", "rich_text"]


@pytest.mark.asyncio
async def test_verified_ingress_is_owner_bound_and_never_leaks_file_fields(
    asset_runtime,
) -> None:
    runtime, ingestor, adapter, _now = asset_runtime
    result = await runtime.stage_asset(_request(), _context())
    assert result["asset_ref"].startswith("ast_")
    assert result["status"] == "ready"
    assert len(ingestor.calls) == 1 and len(adapter.staged) == 1
    file, owner, maximum, requested_expiry = ingestor.calls[0]
    assert file.file_id == FILE_VALUE["file_id"]
    assert maximum == 30 * 1024 * 1024
    assert owner == adapter.staged[0][0].owner_binding
    assert requested_expiry == adapter.staged[0][0].expires_at
    adapter_payload = repr(adapter.staged[0])
    assert FILE_VALUE["download_url"] not in adapter_payload
    assert FILE_VALUE["file_id"] not in adapter_payload
    assert adapter.staged[0][0].storage_ref not in adapter_payload
    assert owner not in adapter_payload

    status = await runtime.asset_status(result["asset_ref"], _context())
    assert status == {
        "asset_ref": result["asset_ref"],
        "status": "ready",
        "mime_type": "image/png",
        "byte_length": 4096,
        "content_digest": "sha256:" + "b" * 64,
        "width": 1200,
        "height": 1600,
        "expires_at": "2027-01-15T09:00:00Z",
        "trust": "untrusted_external_data",
    }
    public_json = json.dumps({"stage": result, "status": status})
    assert FILE_VALUE["download_url"] not in public_json
    assert FILE_VALUE["file_id"] not in public_json
    raw_db = Path(runtime.store.path).read_bytes()
    assert FILE_VALUE["download_url"].encode() not in raw_db
    assert FILE_VALUE["file_id"].encode() not in raw_db
    assert b"provider-asset-handle" not in raw_db

    with pytest.raises(SocialWorkspaceRuntimeError, match="unknown or not bound"):
        await runtime.asset_status(result["asset_ref"], _context(subject="mallory"))

    mallory = _context(subject="mallory")
    mallory_principal = RuntimePrincipal.from_context(mallory)
    mallory_target = runtime._mint_ref(
        "target", "mallory-native-target", "telegram", mallory_principal
    )
    foreign_intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "publish",
            "idempotency_key": "foreign-asset-denial-123",
            "target_ref": mallory_target,
            "content": {
                "text": "Must not send",
                "entities": [],
                "media": [{"asset_ref": result["asset_ref"], "role": "image"}],
            },
        }
    )
    with pytest.raises(SocialWorkspaceRuntimeError, match="expired or not bound"):
        await runtime.prepare(foreign_intent, mallory)
    assert adapter.executions == 0


@pytest.mark.asyncio
async def test_chatgpt_png_to_saved_messages_commits_without_second_approval(
    asset_runtime,
) -> None:
    runtime, _ingestor, adapter, _now = asset_runtime
    staged = await runtime.stage_asset(_request(), _context())
    status = await runtime.asset_status(staged["asset_ref"], _context())
    assert status["status"] == "ready"

    principal = RuntimePrincipal.from_context(_context())
    saved = runtime._mint_ref("target", "native-self", "telegram", principal)
    runtime._store_target_preview(
        saved,
        {
            "platform": "telegram",
            "target_ref": saved,
            "kind": "self",
            "display_name": "Saved Messages",
        },
    )
    prepared = await runtime.prepare(
        validate_prepare_request(
            {
                "platform": "telegram",
                "action": "send_message",
                "idempotency_key": "saved-image-direct-123",
                "target_ref": saved,
                "content": {
                    "text": "Симфонический концерт — дирижёр и оркестр.",
                    "entities": [],
                    "media": [
                        {"asset_ref": staged["asset_ref"], "role": "image"}
                    ],
                },
            }
        ),
        _context(),
    )
    assert prepared["status"] == "approved"
    assert "approval_url" not in prepared
    committed = await runtime.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        _context(),
    )
    assert committed["status"] == "succeeded"
    assert committed["target_ref"] == saved
    assert committed["item_ref"] == committed["read_after_write"][
        "observed_item_ref"
    ]
    assert adapter.executions == 1
    assert adapter.last_intent.content.media[0].asset_ref == "provider-asset-handle"


@pytest.mark.asyncio
async def test_optional_declared_mime_is_only_a_hint(asset_runtime) -> None:
    runtime, _ingestor, adapter, _now = asset_runtime
    result = await runtime.stage_asset(_request(mime_type="image/jpeg"), _context())
    assert result["status"] == "ready"
    assert adapter.staged[0][0].mime_type == "image/png"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "override",
    [
        {"owner_binding": "0" * 64},
        {"content_digest": "sha256:not-a-digest"},
        {"byte_length": 30 * 1024 * 1024 + 1},
        {"expires_at": 1_900_000_000},
        {"width": 9000, "height": 100},
        {"width": 8000, "height": 8000},
        {"mime_type": "text/html"},
    ],
)
async def test_adversarial_ingestor_metadata_fails_before_adapter_or_ref(
    asset_runtime, override
) -> None:
    runtime, ingestor, adapter, _now = asset_runtime
    ingestor.override = override
    with pytest.raises(SocialWorkspaceRuntimeError):
        await runtime.stage_asset(_request(), _context())
    assert adapter.staged == []
    with sqlite3.connect(runtime.store.path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM social_workspace_ref WHERE ref_kind='asset'"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_asset_expiry_is_revalidated_for_status_prepare_and_approval(
    asset_runtime,
) -> None:
    runtime, _ingestor, _adapter, now = asset_runtime
    staged = await runtime.stage_asset(_request(), _context())
    principal = RuntimePrincipal.from_context(_context())
    target = runtime._mint_ref("target", "native-target", "telegram", principal)
    runtime._store_target_preview(
        target,
        {
            "platform": "telegram",
            "target_ref": target,
            "kind": "channel",
            "display_name": "Channel",
        },
    )
    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "publish",
            "idempotency_key": "asset-publish-123",
            "target_ref": target,
            "content": {
                "text": "Poster",
                "entities": [],
                "media": [{"asset_ref": staged["asset_ref"], "role": "image"}],
            },
        }
    )
    prepared = await runtime.prepare(intent, _context())
    preview = runtime.approval_preview(
        preparation_ref=prepared["preparation_ref"],
        action_digest=prepared["action_digest"],
    )
    media = preview["content"]["media"][0]
    assert media["content_digest"] == "sha256:" + "b" * 64
    assert media["mime_type"] == "image/png"
    assert (media["width"], media["height"]) == (1200, 1600)

    now[0] += 3601
    status = await runtime.asset_status(staged["asset_ref"], _context())
    assert status["status"] == "expired"
    with pytest.raises(SocialWorkspaceRuntimeError, match="expired"):
        await runtime.prepare(replace(intent, idempotency_key="after-expiry-123"), _context())
    with pytest.raises(SocialWorkspaceRuntimeError, match="expired"):
        runtime.approval_preview(
            preparation_ref=prepared["preparation_ref"],
            action_digest=prepared["action_digest"],
        )


@pytest.mark.asyncio
async def test_tampered_verified_metadata_breaks_approval_digest(asset_runtime) -> None:
    runtime, _ingestor, adapter, _now = asset_runtime
    staged = await runtime.stage_asset(_request(), _context())
    principal = RuntimePrincipal.from_context(_context())
    target = runtime._mint_ref("target", "native-target", "telegram", principal)
    runtime._store_target_preview(
        target,
        {
            "platform": "telegram",
            "target_ref": target,
            "kind": "channel",
            "display_name": "Channel",
        },
    )
    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "publish",
            "idempotency_key": "tamper-digest-123",
            "target_ref": target,
            "content": {
                "text": "Poster",
                "entities": [],
                "media": [{"asset_ref": staged["asset_ref"], "role": "image"}],
            },
        }
    )
    prepared = await runtime.prepare(intent, _context())
    approval = runtime.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator",
        operator_nonce="tamper-metadata-nonce-12345",
    )
    with runtime.store._lock, runtime.store._connect() as conn:
        row = conn.execute(
            "SELECT preview_json FROM social_workspace_ref_preview WHERE ref_hash=?",
            (runtime._hash(staged["asset_ref"]),),
        ).fetchone()
        metadata = json.loads(runtime._decrypt(row["preview_json"]))
        metadata["content_digest"] = "sha256:" + "c" * 64
        conn.execute(
            "UPDATE social_workspace_ref_preview SET preview_json=? WHERE ref_hash=?",
            (
                runtime._encrypt(json.dumps(metadata, sort_keys=True)),
                runtime._hash(staged["asset_ref"]),
            ),
        )
    with pytest.raises(SocialWorkspaceRuntimeError, match="action digest mismatch"):
        runtime.approval_preview(
            preparation_ref=prepared["preparation_ref"],
            action_digest=prepared["action_digest"],
        )
    with pytest.raises(SocialWorkspaceRuntimeError, match="action digest mismatch"):
        await runtime.commit(
            {
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
                **approval,
            },
            _context(),
        )
    assert adapter.executions == 0


@pytest.mark.asyncio
async def test_disabled_or_missing_ingestor_fails_closed(tmp_path: Path) -> None:
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": FakeAdapter()},
        encryption_key="asset-ingress-test-key-long-enough",
    )
    tools = build_social_workspace_tools(
        runtime,
        feature_policy={"media_story": False},
        capability_policy={"telegram": True, "vk": False},
    )
    assert "social_asset_stage" not in {tool.name for tool in tools}
    enabled = _asset_tools(runtime)["social_asset_stage"]
    with pytest.raises(ToolExecutionError) as caught:
        await enabled.handler(
            {"platform": "telegram", "file": FILE_VALUE, "role": "image"},
            _context(),
        )
    assert caught.value.error_code == "WORKSPACE_NOT_BOUND"


def test_server_media_attach_requires_ingestor_and_keeps_codex_evidence_only(
    config,
) -> None:
    enabled = replace(
        config,
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_post_enabled=True,
        universal_social_media_story_enabled=True,
        media_allowed_hosts=("files.example.test",),
    )
    adapter = FakeAdapter()
    with pytest.raises(ValueError, match="asset ingestor"):
        PrivateEventsMCPServer(
            enabled, social_workspace_adapters={"telegram": adapter}
        )
    with pytest.raises(ValueError, match="storage and host policy"):
        PrivateEventsMCPServer(
            replace(enabled, media_root="relative/path"),
            social_workspace_adapters={"telegram": adapter},
            asset_ingestor=FakeIngestor(),
        )
    missing_preview = FakeAdapter()
    missing_preview.read_asset = None
    with pytest.raises(ValueError, match="staging and preview"):
        PrivateEventsMCPServer(
            enabled,
            social_workspace_adapters={"telegram": missing_preview},
            asset_ingestor=FakeIngestor(),
        )
    server = PrivateEventsMCPServer(
        enabled,
        social_workspace_adapters={"telegram": adapter},
        asset_ingestor=FakeIngestor(),
    )
    assert len(server.codex_protocol.tools) == 7
    assert not any(tool.name.startswith("social_") for tool in server.codex_protocol.tools)
