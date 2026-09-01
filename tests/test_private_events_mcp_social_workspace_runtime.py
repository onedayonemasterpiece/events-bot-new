from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass, replace
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from PIL import Image

import private_events_mcp.social_workspace_runtime as runtime_module
from audio_transcription.contracts import JobState
from audio_transcription.job_store import JobOwnershipError
from private_events_mcp.auth_store import OAuthStateStore, OAuthStoreError
from private_events_mcp.crypto import AccessIdentity, pkce_s256
from private_events_mcp.media_contract import VerifiedAsset
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
    validate_scheduled_items_request,
)
from private_events_mcp.social_workspace_runtime import (
    MAX_TRANSCRIPTION_ATTACHMENTS_PER_READ,
    TRANSCRIPTION_REGISTRATION_CONCURRENCY,
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
        self.forced_result: dict[str, Any] | None = None
        self.scheduled_calls: list[dict[str, Any]] = []
        self.retry_calls: list[tuple[str, int]] = []

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
        if self.forced_result is not None:
            return dict(self.forced_result)
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

    async def scheduled_items(self, **kwargs):
        self.scheduled_calls.append(dict(kwargs))
        return {
            "platform": "telegram",
            "target_ref": kwargs["target_ref"],
            "queue": "scheduled",
            "items": [
                {
                    "item_ref": "native-scheduled-album-42",
                    "target_ref": kwargs["target_ref"],
                    "queue": "scheduled",
                    "scheduled_at": "2026-08-31T12:00:00Z",
                    "text_sha256": "a" * 64,
                    "media_count": 4,
                    "media_roles": ["image"] * 4,
                    "provider_id": 999,
                    "trust": "untrusted_external_data",
                }
            ],
            "exact_match_count": 1,
            "has_more": False,
            "native": {"peer_id": 123},
            "trust": "untrusted_external_data",
        }

    async def retry(self, intent, *, operation_ref, attempt_number):
        self.retry_calls.append((operation_ref, attempt_number))
        await asyncio.sleep(0)
        return {
            "target_ref": intent.target_ref,
            "item_ref": "native-retried-99",
            "status": "succeeded",
            "retry_safe": False,
            "stage": "readback_verified",
            "mutation_boundary_reached": True,
            "read_after_write": {
                "verified": True,
                "observed_item_ref": "native-retried-99",
                "observed_at": "2026-08-31T12:00:00Z",
            },
        }

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


class FakeAlbumIngestor:
    def __init__(self) -> None:
        self.assets: list[VerifiedAsset] = []

    async def ingest(
        self, file, *, owner_binding, max_bytes, expires_at, role="story_media"
    ) -> VerifiedAsset:
        index = len(self.assets) + 1
        asset = VerifiedAsset(
            storage_ref="ing_" + f"{index:024d}",
            owner_binding=owner_binding,
            content_digest="sha256:" + f"{index:064x}",
            mime_type="image/png",
            byte_length=128 + index,
            expires_at=expires_at,
            width=32,
            height=32,
            role="image",
        )
        self.assets.append(asset)
        return asset


class FakeAlbumAdapter(FakeAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.staged_provider_refs: list[str] = []
        self.executed_media_refs: list[str] = []

    async def stage_asset(self, asset, *, role):
        assert role is MediaRole.IMAGE
        provider_ref = f"provider-asset-{len(self.staged_provider_refs) + 1}"
        self.staged_provider_refs.append(provider_ref)
        return provider_ref

    async def execute(self, intent, *, operation_ref):
        self.executed_media_refs = [
            attachment.asset_ref for attachment in intent.content.media
        ]
        return await super().execute(intent, operation_ref=operation_ref)


class FakeReadMediaAdapter(FakeAdapter):
    """Return provider-owned media refs exactly as a social read adapter does."""

    async def read(self, request):
        if request.operation is SocialReadOperation.GET_ITEM:
            return {
                "item": {
                    "item_ref": request.item_ref,
                    "target_ref": "provider-target-media",
                    "kind": "message",
                    "published_at": "2026-08-24T12:00:00Z",
                    "text": "album",
                    "caption": "",
                    "basic_metrics": {"views": 1},
                    "media": [
                        "ast_providerreadmedia000000000001",
                        "ast_providerreadmedia000000000002",
                    ],
                    "trust": "untrusted_external_data",
                },
                "trust": "untrusted_external_data",
            }
        return await super().read(request)

    async def read_asset(self, asset_ref, *, owner_binding, max_bytes):
        assert asset_ref in {
            "ast_providerreadmedia000000000001",
            "ast_providerreadmedia000000000002",
        }
        assert len(owner_binding) == 64
        assert self.asset_bytes is not None
        assert len(self.asset_bytes) <= max_bytes
        return self.asset_bytes


class FakeTelegramAudioReadAdapter(FakeAdapter):
    max_read_asset_bytes = 30 * 1024 * 1024

    def __init__(self) -> None:
        super().__init__()
        self.downloads: list[str] = []
        self.read_bounds: list[int] = []
        self.fail_ref: str | None = None

    async def read(self, request):
        refs = [
            "ast_provider_voice_media_0000000001",
            "ast_provider_voice_media_0000000002",
        ]
        return {
            "item": {
                "item_ref": request.item_ref,
                "target_ref": "provider-target-audio",
                "kind": "message",
                "published_at": "2026-08-25T12:00:00Z",
                "text": "voice batch",
                "caption": "",
                "basic_metrics": {"views": 0},
                "media": refs,
                "attachments": [
                    {
                        "asset_ref": ref,
                        "kind": "voice",
                        "mime_type": "audio/ogg",
                        "byte_length": 16,
                        "duration_seconds": float(index + 1),
                        "binding_fingerprint": str(index + 1) * 64,
                        "trust": "untrusted_external_data",
                    }
                    for index, ref in enumerate(refs)
                ],
                "trust": "untrusted_external_data",
            },
            "trust": "untrusted_external_data",
        }

    async def read_asset(self, asset_ref, *, owner_binding, max_bytes):
        assert len(owner_binding) == 64
        assert max_bytes >= 16
        self.read_bounds.append(max_bytes)
        self.downloads.append(asset_ref)
        if asset_ref == self.fail_ref:
            raise RuntimeError("provider path /secret and access_hash")
        return b"OggS" + b"\0" * 12


class FakeReadAudioService:
    def __init__(self, *, ready: bool = False) -> None:
        self.config = SimpleNamespace(
            max_asset_bytes=1024, poll_interval_seconds=20
        )
        self.ready = ready
        self.jobs: dict[str, dict] = {}
        self.starts = 0
        self.wait_calls: list[tuple[tuple[str, ...], int]] = []

    async def start_provider_transcription(self, **values):
        key = values["idempotency_key"]
        existing = self.jobs.get(key)
        if existing is not None:
            return {**existing, "created": False}
        self.starts += 1
        await values["content_loader"]()
        job = {
            "job_ref": "atr_" + f"{self.starts:024d}",
            "state": "complete" if self.ready else "queued",
        }
        self.jobs[key] = job
        return {**job, "created": True}

    async def status(self, *, job_ref, owner_binding):
        assert len(owner_binding) == 64
        return next(value for value in self.jobs.values() if value["job_ref"] == job_ref)

    async def get_result(self, **values):
        assert values["view"] == "plain"
        return {
            "ready": True,
            "text": "external transcript",
            "next_offset": None,
        }

    async def wait_many(self, *, owner_binding, job_refs, wait_seconds):
        assert len(owner_binding) == 64
        self.wait_calls.append((tuple(job_refs), wait_seconds))
        jobs = tuple(
            SimpleNamespace(
                job_ref=job_ref,
                state=JobState.COMPLETE if self.ready else JobState.QUEUED,
                error_code=None,
                progress={},
            )
            for job_ref in job_refs
        )
        return SimpleNamespace(
            jobs=jobs,
            wait_requested_seconds=wait_seconds,
            wait_actual_seconds=0.0,
            wait_expired=bool(wait_seconds and not self.ready),
            next_poll_after_seconds=0 if self.ready else 20,
        )

    async def get_many(self, *, owner_binding, job_refs, limit):
        assert len(owner_binding) == 64
        assert 1 <= limit <= 60_000
        if not self.ready:
            return {}
        return {
            job_ref: {
                "job_ref": job_ref,
                "state": "complete",
                "ready": True,
                "text": "external transcript"[:limit],
                "next_offset": None,
            }
            for job_ref in job_refs
        }


class OwnerAwareReadAudioService(FakeReadAudioService):
    """Synthetic service enforcing the real durable owner boundary."""

    async def start_provider_transcription(self, **values):
        result = await super().start_provider_transcription(**values)
        self.jobs[values["idempotency_key"]]["owner_binding"] = values[
            "owner_binding"
        ]
        return result

    def _owned(self, job_ref, owner_binding):
        job = next(value for value in self.jobs.values() if value["job_ref"] == job_ref)
        if job["owner_binding"] != owner_binding:
            raise JobOwnershipError("wrong owner")
        return job

    async def status(self, *, job_ref, owner_binding):
        job = self._owned(job_ref, owner_binding)
        return {
            "job_ref": job_ref,
            "state": "complete" if self.ready else job["state"],
        }

    async def get_result(self, *, job_ref, owner_binding, **values):
        self._owned(job_ref, owner_binding)
        assert values["view"] == "plain"
        return {
            "job_ref": job_ref,
            "state": "complete",
            "ready": True,
            "text": "external transcript",
        }

    async def wait_many(self, *, owner_binding, job_refs, wait_seconds):
        for job_ref in job_refs:
            self._owned(job_ref, owner_binding)
        return await super().wait_many(
            owner_binding=owner_binding,
            job_refs=job_refs,
            wait_seconds=wait_seconds,
        )

    async def get_many(self, *, owner_binding, job_refs, limit):
        for job_ref in job_refs:
            self._owned(job_ref, owner_binding)
        return await super().get_many(
            owner_binding=owner_binding,
            job_refs=job_refs,
            limit=limit,
        )


class BatchTelegramAudioReadAdapter(FakeAdapter):
    max_read_asset_bytes = 30 * 1024 * 1024

    def __init__(self, voice_count: int) -> None:
        super().__init__()
        self.voice_count = voice_count
        self.downloads: list[str] = []

    async def read(self, request):
        items = []
        for page_start in range(0, self.voice_count, 10):
            attachments = []
            media = []
            for index in range(page_start, min(page_start + 10, self.voice_count)):
                asset_ref = f"ast_provider_batch_voice_{index:04d}"
                media.append(asset_ref)
                attachments.append(
                    {
                        "asset_ref": asset_ref,
                        "kind": "voice" if index % 2 == 0 else "audio",
                        "mime_type": "audio/ogg",
                        "byte_length": 16,
                        "duration_seconds": float(index + 1),
                        "binding_fingerprint": f"{index + 1:064x}",
                        "trust": "untrusted_external_data",
                    }
                )
            items.append(
                {
                    "item_ref": f"provider-batch-item-{page_start // 10}",
                    "target_ref": request.target_ref or "provider-batch-target",
                    "kind": "message",
                    "published_at": "2026-08-27T08:00:00Z",
                    "text": "batch voice fixture",
                    "caption": "",
                    "basic_metrics": {"views": 0},
                    "media": media,
                    "attachments": attachments,
                    "trust": "untrusted_external_data",
                }
            )
        if request.operation is SocialReadOperation.RESOLVE_ITEM:
            item = items[0]
            return {
                "item": item,
                "source_target": {
                    "target_ref": item["target_ref"],
                    "kind": "group",
                    "title": "Batch voice fixture",
                    "about": "",
                    "description": "",
                    "basic_metrics": {"members": 1},
                    "trust": "untrusted_external_data",
                },
                "trust": "untrusted_external_data",
            }
        if request.operation is SocialReadOperation.GET_ITEM:
            return {"item": items[0], "trust": "untrusted_external_data"}
        if request.operation is SocialReadOperation.LIST_COMMENTS:
            return {
                "root_item_ref": request.item_ref,
                "items": [{**item, "kind": "comment"} for item in items],
                "trust": "untrusted_external_data",
            }
        return {"results": items, "trust": "untrusted_external_data"}

    async def read_asset(self, asset_ref, *, owner_binding, max_bytes):
        assert len(owner_binding) == 64
        assert max_bytes >= 16
        self.downloads.append(asset_ref)
        return b"OggS" + b"\0" * 12


class DeterministicBatchAudioService:
    """Store-shaped fake proving one registration phase and one batch wait."""

    def __init__(
        self,
        states: list[JobState],
        *,
        created_flags: list[bool] | None = None,
        text: str = "external transcript",
        wait_delay: float = 0.0,
    ) -> None:
        self.config = SimpleNamespace(
            max_asset_bytes=1024,
            poll_interval_seconds=20,
        )
        self.initial_states = states
        self.created_flags = created_flags or [True] * len(states)
        self.text = text
        self.wait_delay = wait_delay
        self.jobs: dict[str, dict[str, Any]] = {}
        self.registrations: list[str] = []
        self.wait_calls: list[tuple[tuple[str, ...], int, int]] = []
        self.get_many_calls = 0

    async def start_provider_transcription(self, **values):
        key = values["idempotency_key"]
        existing = self.jobs.get(key)
        if existing is not None:
            self.registrations.append(existing["job_ref"])
            return {**existing, "created": False}
        index = len(self.jobs)
        created = self.created_flags[index]
        if created:
            await values["content_loader"]()
        job = {
            "job_ref": "atr_" + f"{index + 1:024d}",
            "state": self.initial_states[index],
            "error_code": (
                "TRANSCRIPTION_BACKEND_FAILED"
                if self.initial_states[index] in {JobState.FAILED, JobState.CANCELLED}
                else None
            ),
        }
        self.jobs[key] = job
        self.registrations.append(job["job_ref"])
        return {
            "job_ref": job["job_ref"],
            "state": job["state"].value,
            "created": created,
        }

    async def wait_many(self, *, owner_binding, job_refs, wait_seconds):
        assert len(owner_binding) == 64
        self.wait_calls.append(
            (tuple(job_refs), wait_seconds, len(self.registrations))
        )
        if self.wait_delay:
            await asyncio.sleep(self.wait_delay)
        by_ref = {job["job_ref"]: job for job in self.jobs.values()}
        jobs = tuple(
            SimpleNamespace(
                job_ref=job_ref,
                state=by_ref[job_ref]["state"],
                error_code=by_ref[job_ref]["error_code"],
                progress={},
            )
            for job_ref in job_refs
        )
        pending = any(not job.state.terminal for job in jobs)
        return SimpleNamespace(
            jobs=jobs,
            wait_requested_seconds=wait_seconds,
            wait_actual_seconds=self.wait_delay,
            wait_expired=bool(wait_seconds and pending),
            next_poll_after_seconds=20 if pending else 0,
        )

    async def get_many(self, *, owner_binding, job_refs, limit):
        assert len(owner_binding) == 64
        self.get_many_calls += 1
        by_ref = {job["job_ref"]: job for job in self.jobs.values()}
        results = {}
        for job_ref in job_refs:
            if by_ref[job_ref]["state"] is not JobState.COMPLETE:
                continue
            chunk = self.text[:limit]
            results[job_ref] = {
                "job_ref": job_ref,
                "state": "complete",
                "ready": True,
                "text": chunk,
                "next_offset": len(chunk) if len(chunk) < len(self.text) else None,
            }
        return results

    async def status(self, **_values):
        raise AssertionError("high-level batch reads must not poll per-job status")

    async def get_result(self, **_values):
        raise AssertionError("high-level batch reads must use get_many")

    def complete_all(self) -> None:
        for job in self.jobs.values():
            job["state"] = JobState.COMPLETE
            job["error_code"] = None

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


def test_retry_state_schema_is_additive_and_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "auth.sqlite"
    OAuthStateStore(str(path))
    OAuthStateStore(str(path))
    with sqlite3.connect(path) as conn:
        preparation_columns = {
            row[1] for row in conn.execute(
                "PRAGMA table_info(social_workspace_preparation)"
            )
        }
        operation_columns = {
            row[1] for row in conn.execute(
                "PRAGMA table_info(social_workspace_operation)"
            )
        }
    assert "logical_action_ref" in preparation_columns
    assert {"attempt_number", "retry_in_progress", "retry_started_at"} <= operation_columns


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
async def test_read_media_refs_are_outer_bound_and_each_can_be_previewed(
    tmp_path: Path,
) -> None:
    adapter = FakeReadMediaAdapter()
    source = BytesIO()
    Image.new("RGB", (640, 360), (30, 90, 150)).save(source, format="PNG")
    adapter.asset_bytes = source.getvalue()
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        provider_timeout_seconds=0.02,
    )
    call_context = scoped_context("telegram:read", "telegram:story:read")
    principal = RuntimePrincipal.from_context(call_context)
    service._mint_ref(
        "target", "provider-target-media", "telegram", principal
    )
    item_ref = service._mint_ref(
        "item", "itm_providerreadmessage0000000001", "telegram", principal
    )

    item = await service.read(
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "get_item",
                "item_ref": item_ref,
                "read_access": "private",
            }
        ),
        call_context,
    )

    media_refs = item["item"]["media"]
    assert len(media_refs) == 2
    assert all(ref.startswith("ast_") for ref in media_refs)
    assert not any(ref.startswith("ast_providerreadmedia") for ref in media_refs)
    assert len(
        store._connect()
        .execute(
            "SELECT ref_hash FROM social_workspace_ref WHERE ref_kind='asset'"
        )
        .fetchall()
    ) == 2
    for ref in media_refs:
        preview = await service.asset_preview("telegram", ref, call_context)
        assert preview.structured["mime_type"] == "image/jpeg"
        assert preview.content[0]["type"] == "image"


@pytest.mark.asyncio
async def test_telegram_audio_read_ready_cache_dedup_and_safe_projection(tmp_path: Path) -> None:
    adapter = FakeTelegramAudioReadAdapter()
    transcriber = FakeReadAudioService(ready=True)
    # The audio store accepts much larger uploads than Telegram provider media.
    # Reads must use the stricter adapter limit instead of rejecting the bound.
    transcriber.config.max_asset_bytes = 512 * 1024 * 1024
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    runtime = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")
    principal = RuntimePrincipal.from_context(call_context)
    item_ref = runtime._mint_ref(
        "item", "itm_provider_audio_message_000001", "telegram", principal
    )
    request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "get_item",
            "item_ref": item_ref,
            "read_access": "private",
        }
    )

    first = await runtime.read(request, call_context)
    second = await runtime.read(request, call_context)

    assert [value["transcription"]["status"] for value in first["item"]["attachments"]] == [
        "ready",
        "ready",
    ]
    assert all(
        value["transcription"]["text"] == "external transcript"
        for value in first["item"]["attachments"]
    )
    assert first["transcription_summary"] == {
        "total": 2,
        "ready": 2,
        "queued": 0,
        "running": 0,
        "failed": 0,
        "cache_hits": 0,
        "created": 2,
        "wait_expired": False,
        "next_poll_after_seconds": 0,
    }
    assert second["transcription_summary"]["cache_hits"] == 2
    assert second["transcription_summary"]["created"] == 0
    assert first["item"]["media"] == [
        value["asset_ref"] for value in first["item"]["attachments"]
    ]
    assert all(
        value["transcription"]["cache_hit"] is True
        for value in second["item"]["attachments"]
    )
    assert len(adapter.downloads) == 2
    assert adapter.read_bounds == [adapter.max_read_asset_bytes] * 2
    assert transcriber.starts == 2
    encoded = json.dumps([first, second], ensure_ascii=False)
    for forbidden in (
        "provider-target-audio",
        "provider_voice_media",
        "binding_fingerprint",
        "access_hash",
        "/secret",
    ):
        assert forbidden not in encoded
        assert forbidden.encode() not in Path(store.path).read_bytes()
    assert all(
        value["trust"] == "untrusted_external_data"
        and value["transcription"]["trust"] == "untrusted_external_data"
        for value in first["item"]["attachments"]
    )


@pytest.mark.asyncio
async def test_social_voice_batch_repeat_read_returns_ready_text_inline(
    tmp_path: Path,
) -> None:
    """The normal multi-voice path uses one repeat high-level read, not N polls."""

    adapter = FakeTelegramAudioReadAdapter()
    transcriber = OwnerAwareReadAudioService(ready=False)
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    runtime = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read", "telegram:publish")
    principal = RuntimePrincipal.from_context(call_context)
    item_ref = runtime._mint_ref(
        "item", "itm_provider_private_voice_batch", "telegram", principal
    )

    read = await runtime.read(
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "get_item",
                "item_ref": item_ref,
                "read_access": "private",
            }
        ),
        call_context,
    )
    refs = [
        attachment["transcription"]["transcription_ref"]
        for attachment in read["item"]["attachments"]
    ]
    assert len(refs) == 2
    assert all(
        attachment["transcription"]["status"] == "queued"
        for attachment in read["item"]["attachments"]
    )

    transcriber.ready = True
    repeat = await runtime.read(
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "get_item",
                "item_ref": item_ref,
                "read_access": "private",
            }
        ),
        call_context,
    )
    assert [
        attachment["transcription"]["transcription_ref"]
        for attachment in repeat["item"]["attachments"]
    ] == refs
    assert repeat["transcription_summary"]["created"] == 0
    assert repeat["transcription_summary"]["cache_hits"] == 2
    assert repeat["transcription_summary"]["ready"] == 2
    assert [
        attachment["transcription"]["text"]
        for attachment in repeat["item"]["attachments"]
    ] == ["external transcript", "external transcript"]


@pytest.mark.asyncio
async def test_telegram_audio_read_pending_failure_isolation_and_opt_out(tmp_path: Path) -> None:
    adapter = FakeTelegramAudioReadAdapter()
    adapter.fail_ref = "ast_provider_voice_media_0000000001"
    transcriber = FakeReadAudioService(ready=False)
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    runtime = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")
    principal = RuntimePrincipal.from_context(call_context)
    item_ref = runtime._mint_ref(
        "item", "itm_provider_audio_message_000002", "telegram", principal
    )
    common = {
        "platform": "telegram",
        "operation": "get_item",
        "item_ref": item_ref,
        "read_access": "private",
    }

    result = await runtime.read(validate_read_request(common), call_context)
    statuses = [value["transcription"]["status"] for value in result["item"]["attachments"]]
    assert statuses == ["failed", "queued"]
    assert result["item"]["attachments"][0]["transcription"]["error_code"] == (
        "TRANSCRIPTION_MATERIALIZATION_FAILED"
    )
    calls_before = (len(adapter.downloads), transcriber.starts)
    disabled = await runtime.read(
        validate_read_request({**common, "transcribe_audio": False}), call_context
    )
    assert calls_before == (len(adapter.downloads), transcriber.starts)
    assert all("transcription" not in value for value in disabled["item"]["attachments"])


@pytest.mark.asyncio
async def test_telegram_audio_timeout_preserves_base_read_and_isolates_media(tmp_path: Path) -> None:
    class SlowReadAudioService(FakeReadAudioService):
        async def start_provider_transcription(self, **values):
            await asyncio.sleep(0.05)
            return await super().start_provider_transcription(**values)

    adapter = FakeTelegramAudioReadAdapter()
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    runtime = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        provider_timeout_seconds=0.01,
    )
    runtime.enable_audio_transcription(SlowReadAudioService())
    call_context = scoped_context("telegram:read")
    principal = RuntimePrincipal.from_context(call_context)
    item_ref = runtime._mint_ref(
        "item", "itm_provider_audio_timeout_000001", "telegram", principal
    )

    result = await runtime.read(
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "get_item",
                "item_ref": item_ref,
                "read_access": "private",
            }
        ),
        call_context,
    )

    assert result["item"]["text"] == "voice batch"
    assert [
        value["transcription"]["error_code"]
        for value in result["item"]["attachments"]
    ] == [
        "TRANSCRIPTION_MATERIALIZATION_FAILED",
        "TRANSCRIPTION_MATERIALIZATION_FAILED",
    ]
    assert all(
        value["transcription"]["status"] == "failed"
        for value in result["item"]["attachments"]
    )


def _batch_request(runtime, call_context, voice_count, *, wait_seconds=25):
    principal = RuntimePrincipal.from_context(call_context)
    target_ref = runtime._mint_ref(
        "target", f"provider-batch-target-{voice_count}", "telegram", principal
    )
    return validate_read_request(
        {
            "platform": "telegram",
            "operation": "list_items",
            "target_ref": target_ref,
            "read_access": "private",
            "transcribe_audio": True,
            "transcription_wait_seconds": wait_seconds,
        }
    )


def _batch_attachments(payload):
    if isinstance(payload.get("item"), dict):
        items = [payload["item"]]
    else:
        items = payload.get("results", payload.get("items", []))
    return [attachment for item in items for attachment in item["attachments"]]


@pytest.mark.asyncio
async def test_twenty_voice_read_registers_all_jobs_before_one_batch_wait(
    tmp_path: Path, caplog
) -> None:
    adapter = BatchTelegramAudioReadAdapter(20)
    transcriber = DeterministicBatchAudioService([JobState.QUEUED] * 20)
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")

    with caplog.at_level("INFO", logger="private_events_mcp.social_workspace_runtime"):
        result = await asyncio.wait_for(
            runtime.read(
                _batch_request(runtime, call_context, 20, wait_seconds=25),
                call_context,
            ),
            timeout=0.5,
        )

    assert len(adapter.downloads) == 20
    assert len(transcriber.jobs) == 20
    assert transcriber.wait_calls == [
        (
            tuple(f"atr_{index:024d}" for index in range(1, 21)),
            25,
            20,
        )
    ]
    assert result["transcription_summary"] == {
        "total": 20,
        "ready": 0,
        "queued": 20,
        "running": 0,
        "failed": 0,
        "cache_hits": 0,
        "created": 20,
        "wait_expired": True,
        "next_poll_after_seconds": 20,
    }
    attachments = _batch_attachments(result)
    assert len(attachments) == 20
    assert all(
        attachment["transcription"]["status"] == "queued"
        and "error_code" not in attachment["transcription"]
        for attachment in attachments
    )
    assert "TRANSCRIPTION_TIMEOUT" not in json.dumps(result)
    batch_lines = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("social_voice_transcription_batch ")
    ]
    assert len(batch_lines) == 1
    assert all(
        forbidden not in batch_lines[0]
        for forbidden in (
            "atr_",
            "ast_",
            "t.me/",
            "provider-batch",
            "external transcript",
        )
    )


@pytest.mark.asyncio
async def test_exact_item_resolver_returns_inline_batch_transcription(
    tmp_path: Path,
) -> None:
    adapter = BatchTelegramAudioReadAdapter(1)
    transcriber = DeterministicBatchAudioService([JobState.COMPLETE])
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")

    result = await runtime.read(
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "resolve_item",
                "target_locator": {
                    "kind": "profile_link",
                    "value": "https://t.me/c/100/500",
                },
                "read_access": "private",
                "transcribe_audio": True,
                "transcription_wait_seconds": 25,
            }
        ),
        call_context,
    )

    assert result["source_target"]["kind"] == "group"
    assert result["transcription_summary"]["ready"] == 1
    transcription = result["item"]["attachments"][0]["transcription"]
    assert transcription["status"] == "ready"
    assert transcription["text"] == "external transcript"
    assert transcriber.wait_calls[0][1:] == (25, 1)


@pytest.mark.asyncio
async def test_comment_thread_returns_one_batch_summary_without_per_ref_polling(
    tmp_path: Path,
) -> None:
    adapter = BatchTelegramAudioReadAdapter(2)
    transcriber = DeterministicBatchAudioService([JobState.QUEUED] * 2)
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")
    principal = RuntimePrincipal.from_context(call_context)
    root_ref = runtime._mint_ref(
        "item", "provider-thread-root", "telegram", principal
    )

    result = await runtime.read(
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "list_comments",
                "item_ref": root_ref,
                "read_access": "private",
                "transcribe_audio": True,
                "transcription_wait_seconds": 25,
            }
        ),
        call_context,
    )

    assert result["root_item_ref"] == root_ref
    assert result["transcription_summary"]["queued"] == 2
    assert all(
        attachment["transcription"]["status"] == "queued"
        for attachment in _batch_attachments(result)
    )
    assert transcriber.wait_calls == [
        (
            ("atr_000000000000000000000001", "atr_000000000000000000000002"),
            25,
            2,
        )
    ]


@pytest.mark.asyncio
async def test_voice_batch_mixed_durable_states_are_projected_without_poll_burst(
    tmp_path: Path,
) -> None:
    states = [
        JobState.COMPLETE,
        JobState.QUEUED,
        JobState.DISPATCHING,
        JobState.RUNNING,
        JobState.COLLECTING,
        JobState.FAILED,
        JobState.CANCELLED,
    ]
    adapter = BatchTelegramAudioReadAdapter(len(states))
    transcriber = DeterministicBatchAudioService(
        states,
        created_flags=[False, True, False, False, False, False, False],
    )
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")

    result = await runtime.read(
        _batch_request(runtime, call_context, len(states), wait_seconds=25),
        call_context,
    )

    assert result["transcription_summary"] == {
        "total": 7,
        "ready": 1,
        "queued": 2,
        "running": 2,
        "failed": 2,
        "cache_hits": 6,
        "created": 1,
        "wait_expired": True,
        "next_poll_after_seconds": 20,
    }
    transcriptions = [
        attachment["transcription"] for attachment in _batch_attachments(result)
    ]
    assert [value["status"] for value in transcriptions] == [
        "ready",
        "queued",
        "queued",
        "running",
        "running",
        "failed",
        "failed",
    ]
    assert transcriptions[0]["text"] == "external transcript"
    assert all(
        "error_code" not in value for value in transcriptions[1:5]
    )
    assert all(
        value["error_code"] == "TRANSCRIPTION_BACKEND_FAILED"
        for value in transcriptions[5:]
    )
    assert len(adapter.downloads) == 1
    assert transcriber.get_many_calls == 1


@pytest.mark.asyncio
async def test_repeat_high_level_voice_read_reuses_jobs_and_inlines_ready_text(
    tmp_path: Path,
) -> None:
    adapter = BatchTelegramAudioReadAdapter(4)
    transcriber = DeterministicBatchAudioService([JobState.QUEUED] * 4)
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")
    request = _batch_request(runtime, call_context, 4, wait_seconds=0)

    first = await runtime.read(request, call_context)
    first_refs = [
        item["transcription"]["transcription_ref"]
        for item in _batch_attachments(first)
    ]
    transcriber.complete_all()
    second = await runtime.read(request, call_context)

    assert len(adapter.downloads) == 4
    assert len(transcriber.jobs) == 4
    assert first["transcription_summary"]["created"] == 4
    assert first["transcription_summary"]["queued"] == 4
    assert second["transcription_summary"]["created"] == 0
    assert second["transcription_summary"]["cache_hits"] == 4
    assert second["transcription_summary"]["ready"] == 4
    assert [
        item["transcription"]["transcription_ref"]
        for item in _batch_attachments(second)
    ] == first_refs
    assert all(
        item["transcription"]["text"] == "external transcript"
        for item in _batch_attachments(second)
    )


@pytest.mark.asyncio
async def test_voice_batch_response_cap_uses_reproducible_inline_prefixes(
    tmp_path: Path,
) -> None:
    full_text = "длинная расшифровка " * 10_000
    adapter = BatchTelegramAudioReadAdapter(2)
    transcriber = DeterministicBatchAudioService(
        [JobState.COMPLETE, JobState.COMPLETE], text=full_text
    )
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        response_cap_bytes=40 * 1024,
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")

    result = await runtime.read(
        _batch_request(runtime, call_context, 2, wait_seconds=0), call_context
    )
    encoded = json.dumps(result, ensure_ascii=False).encode("utf-8")
    assert len(encoded) <= 40 * 1024
    for attachment in _batch_attachments(result):
        transcription = attachment["transcription"]
        assert transcription["status"] == "ready"
        assert transcription["text_included"] is True
        assert transcription["truncated"] is True
        assert transcription["next_offset"] == len(transcription["text"])
        assert full_text.startswith(transcription["text"])


@pytest.mark.asyncio
async def test_provider_timeout_and_batch_wait_are_independent(tmp_path: Path) -> None:
    class SlowProviderAdapter(BatchTelegramAudioReadAdapter):
        async def read(self, request):
            await asyncio.sleep(0.05)
            return await super().read(request)

    slow_adapter = SlowProviderAdapter(1)
    untouched = DeterministicBatchAudioService([JobState.QUEUED])
    slow_runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "slow.sqlite")),
        adapters={"telegram": slow_adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        provider_timeout_seconds=0.01,
    )
    slow_runtime.enable_audio_transcription(untouched)
    call_context = scoped_context("telegram:read")
    with pytest.raises(SocialWorkspaceRuntimeError, match="provider operation timed out"):
        await slow_runtime.read(
            _batch_request(slow_runtime, call_context, 1, wait_seconds=30),
            call_context,
        )
    assert untouched.registrations == []

    fast_adapter = BatchTelegramAudioReadAdapter(1)
    waiting = DeterministicBatchAudioService(
        [JobState.QUEUED], wait_delay=0.05
    )
    fast_runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "fast.sqlite")),
        adapters={"telegram": fast_adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        provider_timeout_seconds=0.01,
    )
    fast_runtime.enable_audio_transcription(waiting)
    result = await fast_runtime.read(
        _batch_request(fast_runtime, call_context, 1, wait_seconds=30),
        call_context,
    )
    assert result["transcription_summary"]["queued"] == 1
    assert result["transcription_summary"]["wait_expired"] is True


@pytest.mark.asyncio
async def test_protocol_allows_slow_bounded_twenty_voice_registration_stage(
    tmp_path: Path,
) -> None:
    class SlowRegistrationService(DeterministicBatchAudioService):
        async def start_provider_transcription(self, **values):
            await asyncio.sleep(0.02)
            return await super().start_provider_transcription(**values)

    adapter = BatchTelegramAudioReadAdapter(20)
    transcriber = SlowRegistrationService([JobState.QUEUED] * 20)
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        # Seven concurrency waves take longer than this single-item provider
        # budget.  Every item must still be attempted before the one batch wait.
        provider_timeout_seconds=0.05,
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read", "telegram:read:private")
    request = _batch_request(runtime, call_context, 20, wait_seconds=0)
    protocol = MCPProtocol(
        build_social_workspace_tools(
            runtime,
            capability_policy={"telegram": True, "vk": False},
        ),
        cache_ttl_seconds=60,
        challenge='Bearer error="invalid_token"',
        resource=call_context.resource,
        allowed_client_ids=frozenset({call_context.identity.client_id}),
    )

    response = await asyncio.wait_for(
        protocol.dispatch(
            {
                "jsonrpc": "2.0",
                "id": 92,
                "method": "tools/call",
                "params": {
                    "name": "social_content_feed",
                    "arguments": {
                        "platform": request.platform.value,
                        "operation": request.operation.value,
                        "target_ref": request.target_ref,
                        "read_access": request.read_access.value,
                        "transcribe_audio": True,
                        "transcription_wait_seconds": 0,
                    },
                },
            },
            call_context.identity,
        ),
        timeout=0.5,
    )

    assert response["result"]["isError"] is False
    assert response["result"]["structuredContent"]["transcription_summary"][
        "queued"
    ] == 20
    assert len(transcriber.jobs) == 20
    assert transcriber.wait_calls[0][2] == 20


@pytest.mark.asyncio
async def test_oversized_untrusted_voice_batch_fails_before_registration(
    tmp_path: Path,
) -> None:
    adapter = BatchTelegramAudioReadAdapter(
        MAX_TRANSCRIPTION_ATTACHMENTS_PER_READ + 1
    )
    transcriber = DeterministicBatchAudioService(
        [JobState.QUEUED] * (MAX_TRANSCRIPTION_ATTACHMENTS_PER_READ + 1)
    )
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    runtime.enable_audio_transcription(transcriber)
    call_context = scoped_context("telegram:read")

    with pytest.raises(
        SocialWorkspaceRuntimeError,
        match="provider voice batch exceeds response cap",
    ):
        await runtime.read(
            _batch_request(
                runtime,
                call_context,
                MAX_TRANSCRIPTION_ATTACHMENTS_PER_READ + 1,
                wait_seconds=25,
            ),
            call_context,
        )

    assert adapter.downloads == []
    assert transcriber.jobs == {}
    assert transcriber.registrations == []
    assert transcriber.wait_calls == []


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
    assert prepared["commit_required"] is True
    assert prepared["next_action"] == "social_action_commit"
    assert prepared["operation_state"] == "not_started"
    assert prepared["provider_attempted"] is False
    assert prepared["reserved_operation_ref"].startswith("op_")
    assert "approval_url" not in prepared
    before_by_preparation = await service.status(
        "preparation", prepared["preparation_ref"], context()
    )
    before_by_operation = await service.status(
        "operation", prepared["reserved_operation_ref"], context()
    )
    assert before_by_preparation["preparation_status"] == "approved"
    assert before_by_preparation["operation_status"] == "not_started"
    assert before_by_operation["status"] == "not_started"
    assert before_by_operation["provider_attempted"] is False
    assert before_by_operation["mutation_boundary_reached"] is False
    with sqlite3.connect(service.store.path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM social_workspace_operation"
        ).fetchone()[0] == 0
    result = await service.commit({
        "preparation_ref": prepared["preparation_ref"],
        "action_digest": prepared["action_digest"],
    }, context())
    assert result["status"] == "succeeded"
    assert result["target_ref"] == resolved["target_ref"]
    assert result["item_ref"] == result["read_after_write"]["observed_item_ref"]
    assert result["provider_attempted"] is True
    assert result["operation_status"] == "succeeded"
    assert "raw_method" not in result and "access_token" not in result
    assert adapter.executions == 1
    repeated = await service.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context(),
    )
    assert repeated == result
    assert adapter.executions == 1


@pytest.mark.asyncio
async def test_four_image_schedule_runs_one_prepare_commit_operation_in_order(
    tmp_path: Path,
) -> None:
    adapter = FakeAlbumAdapter()
    ingestor = FakeAlbumIngestor()
    store = OAuthStateStore(str(tmp_path / "album.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="album-e2e-test-key-that-is-long-enough",
        asset_ingestor=ingestor,
        budget_dimension_limits={
            "attempts": {
                "global": 100,
                "principal": 100,
                "target": 10,
                "action": 10,
            }
        },
    )
    target = service._mint_ref(
        "target",
        "native-album-channel",
        "telegram",
        RuntimePrincipal.from_context(context()),
    )
    staged_refs: list[str] = []
    for index in range(4):
        staged = await service.stage_asset(
            validate_asset_stage_request(
                {
                    "platform": "telegram",
                    "file": {
                        "download_url": f"https://files.example.test/{index}.png",
                        "file_id": f"file-album-{index}",
                        "mime_type": "image/png",
                        "file_name": f"{index}.png",
                    },
                    "role": "image",
                }
            ),
            context(),
        )
        staged_refs.append(staged["asset_ref"])

    prepare_payload = {
        "platform": "telegram",
        "action": "schedule",
        "idempotency_key": "four-image-album-e2e-0001",
        "target_ref": target,
        "content": {
            "text": "Four image canary",
            "entities": [],
            "media": [
                {"asset_ref": asset_ref, "role": "image"}
                for asset_ref in staged_refs
            ],
        },
        "schedule_at": "2030-08-31T19:15:00Z",
    }
    tools = {
        tool.name: tool for tool in build_social_workspace_tools(service)
    }
    prepared = await tools["social_action_prepare"].handler(
        prepare_payload, context()
    )
    assert prepared["status"] == "approved"
    assert prepared["operation_state"] == "not_started"
    assert adapter.executions == 0
    with sqlite3.connect(store.path) as conn:
        conn.row_factory = sqlite3.Row
        assert conn.execute(
            "SELECT COUNT(*) FROM social_workspace_operation"
        ).fetchone()[0] == 0
        persisted = service._intent_from_row(
            conn.execute("SELECT * FROM social_workspace_preparation").fetchone()
        )
    assert [item.asset_ref for item in persisted.content.media] == staged_refs

    before = await tools["social_action_status"].handler(
        {"operation_ref": prepared["reserved_operation_ref"]}, context()
    )
    assert before["status"] == "not_started"
    assert before["provider_attempted"] is False
    committed = await tools["social_action_commit"].handler(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context(),
    )
    repeated = await tools["social_action_commit"].handler(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context(),
    )
    assert repeated == committed
    assert adapter.executions == 1
    assert adapter.executed_media_refs == adapter.staged_provider_refs
    with sqlite3.connect(store.path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM social_workspace_operation"
        ).fetchone()[0] == 1
        ingress = [
            row[0]
            for row in conn.execute(
                """SELECT reason_code FROM social_workspace_audit
                   WHERE operation='commit_ingress' ORDER BY id"""
            )
        ]
    assert ingress == [
        "tool_received",
        "schema_validated",
        "preparation_resolved",
        "preflight_started",
        "operation_reserved",
        "provider_attempt_started",
        "tool_received",
        "schema_validated",
        "preparation_resolved",
        "operation_already_committed",
    ]


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


@pytest.mark.asyncio
async def test_commit_budget_denial_is_explicit_audited_and_pre_provider(
    tmp_path: Path,
) -> None:
    adapter = FakeAdapter()
    store = OAuthStateStore(str(tmp_path / "commit-budget.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="commit-budget-test-key-that-is-long-enough",
        budget_dimension_limits={
            "attempts": {
                "global": 100,
                "principal": 1,
                "target": 100,
                "action": 100,
            }
        },
    )
    principal = RuntimePrincipal.from_context(context())

    async def prepare_for(native_target: str, key: str):
        target = service._mint_ref("target", native_target, "telegram", principal)
        return await service.prepare(
            validate_prepare_request(
                {
                    "platform": "telegram",
                    "action": "publish",
                    "idempotency_key": key,
                    "target_ref": target,
                    "content": {"text": key, "entities": [], "media": []},
                }
            ),
            context(),
        )

    first = await prepare_for("native-budget-target-1", "budget-first-0001")
    await service.commit(
        {
            "preparation_ref": first["preparation_ref"],
            "action_digest": first["action_digest"],
        },
        context(),
    )
    second = await prepare_for("native-budget-target-2", "budget-second-0002")
    protocol = MCPProtocol(
        build_social_workspace_tools(service),
        cache_ttl_seconds=60,
        challenge='Bearer error="invalid_token"',
        resource=context().resource,
        allowed_client_ids=frozenset({context().identity.client_id}),
    )
    response = await protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 93,
            "method": "tools/call",
            "params": {
                "name": "social_action_commit",
                "arguments": {
                    "preparation_ref": second["preparation_ref"],
                    "action_digest": second["action_digest"],
                },
            },
        },
        context().identity,
    )
    result = response["result"]
    assert result["isError"] is True
    assert result["structuredContent"] == {
        "error_code": "ATTEMPTS_BUDGET_EXCEEDED",
        "retry_safe": False,
    }
    assert "attempts budget" not in json.dumps(result).lower()
    assert adapter.executions == 1
    with sqlite3.connect(store.path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM social_workspace_operation"
        ).fetchone()[0] == 1
        rows = conn.execute(
            """SELECT operation,outcome,reason_code FROM social_workspace_audit
               WHERE operation IN ('commit','commit_ingress') ORDER BY id"""
        ).fetchall()
    assert ("commit_ingress", "received", "tool_received") in rows
    assert ("commit_ingress", "validated", "schema_validated") in rows
    assert ("commit", "denied", "attempts_budget_exceeded") in rows


@pytest.mark.asyncio
async def test_local_media_budget_denial_does_not_open_provider_circuit(
    tmp_path: Path,
) -> None:
    class MediaAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.read_calls = 0

        async def read(self, request):
            self.read_calls += 1
            return {
                "results": [
                    {
                        "item_ref": "native-media-budget-item",
                        "target_ref": request.target_ref,
                        "kind": "message",
                        "published_at": "2026-08-25T12:00:00Z",
                        "text": "bounded item",
                        "caption": "",
                        "basic_metrics": {"views": 0},
                        "media": [
                            "native-media-budget-asset-1",
                            "native-media-budget-asset-2",
                        ],
                        "trust": "untrusted_external_data",
                    }
                ],
                "trust": "untrusted_external_data",
            }

    adapter = MediaAdapter()
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        circuit_failure_threshold=1,
        budget_dimension_limits={
            "media": {
                "global": 100,
                "principal": 100,
                "target": 1,
                "action": 100,
            }
        },
    )
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref(
        "target", "native-media-budget-target", "telegram", principal
    )
    request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "list_items",
            "target_ref": target,
            "read_access": "private",
            "limit": 25,
        }
    )

    for _ in range(2):
        with pytest.raises(SocialWorkspaceRuntimeError, match="media budget exceeded"):
            await service.read(request, context())

    assert adapter.read_calls == 2
    with sqlite3.connect(store.path) as conn:
        row = conn.execute(
            "SELECT consecutive_failures,circuit_open_until "
            "FROM social_workspace_circuit"
        ).fetchone()
    assert row == (0, None)


@pytest.mark.asyncio
async def test_provider_read_failure_still_opens_provider_circuit(
    tmp_path: Path,
) -> None:
    class FailingAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.read_calls = 0

        async def read(self, request):
            self.read_calls += 1
            raise RuntimeError("untrusted provider failure")

    adapter = FailingAdapter()
    service = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
        circuit_failure_threshold=1,
    )
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref(
        "target", "native-provider-failure-target", "telegram", principal
    )
    request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "list_items",
            "target_ref": target,
            "read_access": "private",
            "limit": 25,
        }
    )

    with pytest.raises(SocialWorkspaceRuntimeError, match="provider operation failed"):
        await service.read(request, context())
    with pytest.raises(SocialWorkspaceRuntimeError, match="circuit gate is open"):
        await service.read(request, context())
    assert adapter.read_calls == 1


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
async def test_mutating_adapter_owns_deadline_without_equal_outer_timeout(runtime) -> None:
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
    assert result["status"] == "succeeded" and result["retry_safe"] is False
    assert adapter.operation_refs == [result["operation_ref"]]
    assert adapter.reconcile_refs == []


@pytest.mark.asyncio
async def test_scheduled_items_read_is_scope_bound_logical_and_redacted(runtime) -> None:
    service, adapter, _store = runtime
    call_context = scoped_context("telegram:schedule")
    principal = RuntimePrincipal.from_context(call_context)
    target = service._mint_ref(
        "target", "native-scheduled-target", "telegram", principal
    )
    request = validate_scheduled_items_request(
        {
            "platform": "telegram",
            "target_ref": target,
            "scheduled_from": "2026-08-31T08:00:00Z",
            "scheduled_to": "2026-08-31T14:00:00Z",
            "text_sha256": "a" * 64,
            "media_count": 4,
            "limit": 10,
        }
    )

    result = await service.scheduled_items(request, call_context)

    assert adapter.scheduled_calls == [
        {
            "target_ref": "native-scheduled-target",
            "scheduled_from": "2026-08-31T08:00:00Z",
            "scheduled_to": "2026-08-31T14:00:00Z",
            "text_sha256": "a" * 64,
            "media_count": 4,
            "limit": 10,
        }
    ]
    assert result["target_ref"] == target
    assert len(result["items"]) == 1
    assert result["items"][0]["item_ref"].startswith("itm_")
    assert result["items"][0]["target_ref"] == target
    assert result["items"][0]["media_roles"] == ["image"] * 4
    encoded = json.dumps(result)
    assert "native-scheduled" not in encoded
    assert "provider_id" not in encoded
    assert "peer_id" not in encoded


@pytest.mark.asyncio
async def test_retry_reuses_logical_operation_and_is_single_flight(runtime) -> None:
    service, adapter, store = runtime
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)
    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "publish",
            "idempotency_key": "retry-safe-publish-123",
            "target_ref": target,
            "content": {"text": "retry", "entities": [], "media": []},
        }
    )
    prepared = await service.prepare(intent, context())
    adapter.forced_result = {
        "status": "failed",
        "retry_safe": True,
        "error_code": "media_upload_response_invalid",
        "stage": "wall_photo_multipart",
        "mutation_boundary_reached": False,
    }
    failed = await service.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context(),
    )
    adapter.forced_result = None

    first, second = await asyncio.gather(
        service.retry(failed["operation_ref"], context()),
        service.retry(failed["operation_ref"], context()),
        return_exceptions=True,
    )
    successes = [value for value in (first, second) if isinstance(value, dict)]
    failures = [value for value in (first, second) if isinstance(value, Exception)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert "retry" in str(failures[0]).lower()
    retried = successes[0]
    assert retried["operation_ref"] == failed["operation_ref"]
    assert retried["preparation_ref"] == prepared["preparation_ref"]
    assert retried["logical_action_ref"].startswith("act_")
    assert retried["attempt_number"] == 2
    assert adapter.retry_calls == [(failed["operation_ref"], 2)]
    with sqlite3.connect(store.path) as conn:
        row = conn.execute(
            "SELECT attempt_number,retry_in_progress FROM social_workspace_operation"
        ).fetchone()
    assert row == (2, 0)


@pytest.mark.asyncio
async def test_reconcile_passes_recovered_native_intent_and_evidence_when_supported(
    tmp_path: Path,
) -> None:
    class IntentAwareAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.recovered = None

        async def reconcile(self, operation_ref, *, intent, evidence):
            self.recovered = (operation_ref, intent, evidence)
            return {
                "status": "failed",
                "retry_safe": False,
                "error_code": "provider_not_observed",
            }

    adapter = IntentAwareAdapter()
    store = OAuthStateStore(str(tmp_path / "auth.sqlite"))
    service = SocialWorkspaceRuntime(
        store=store,
        adapters={"telegram": adapter},
        encryption_key="unit-test-key-that-is-long-enough",
    )
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)
    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "schedule",
            "idempotency_key": "reconcile-native-intent-123",
            "target_ref": target,
            "content": {"text": "scheduled", "entities": [], "media": []},
            "schedule_at": "2026-08-31T12:00:00Z",
        }
    )
    prepared = await service.prepare(intent, context())
    adapter.forced_result = {
        "status": "outcome_unknown",
        "retry_safe": False,
        "error_code": "provider_timeout",
    }
    result = await service.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context(),
    )

    # Historical reconciliation is evidence recovery, not a new mutation.
    # It must still recover the exact encrypted provider binding after the
    # public ref TTL has elapsed.
    with store._lock, store._connect() as conn:
        conn.execute(
            "UPDATE social_workspace_ref SET expires_at=? WHERE ref_hash=?",
            (service._now() - 1, service._hash(target)),
        )

    await service.reconcile(result["operation_ref"], context())

    assert adapter.recovered is not None
    operation_ref, recovered_intent, evidence = adapter.recovered
    assert operation_ref == result["operation_ref"]
    assert recovered_intent.target_ref == "native-user"
    assert recovered_intent.schedule_at == "2026-08-31T12:00:00Z"
    assert evidence["attempt_number"] == 1
    assert isinstance(evidence["provider_attempted_at"], int)


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


def test_scheduled_read_and_retry_tools_reuse_schedule_and_legacy_publish_scopes(
    runtime,
) -> None:
    service, _adapter, _store = runtime
    tools = {tool.name: tool for tool in build_social_workspace_tools(service)}
    scheduled = tools["social_scheduled_items_list"]
    assert scheduled.read_only is True
    assert scheduled.scope_selector(
        {"platform": "telegram", "target_ref": "tgt_" + "a" * 16}
    ) == {"telegram:schedule"}
    assert frozenset({"telegram:schedule"}) in scheduled.scope_options
    assert frozenset({"telegram:publish"}) in scheduled.scope_options
    retry = tools["social_action_retry"]
    assert retry.read_only is False
    assert retry.idempotent is True


def test_vk_item_and_notification_tools_are_provider_and_scope_isolated(runtime) -> None:
    service, _adapter, _store = runtime
    telegram_only = {
        tool.name
        for tool in build_social_workspace_tools(
            service, capability_policy={"telegram": True, "vk": False}
        )
    }
    assert "social_item_resolve" in telegram_only
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


def test_chatgpt_legacy_scope_descriptor_publishes_telegram_and_vk_item_resolve(runtime) -> None:
    service, _adapter, _store = runtime
    service.adapters["vk"] = FakeAdapter()
    tool = next(
        item
        for item in build_social_workspace_tools(service)
        if item.name == "social_item_resolve"
    )

    descriptor = tool.descriptor(frozenset({"telegram:read", "vk:read"}))

    assert descriptor["inputSchema"]["properties"]["platform"] == {
        "type": "string",
        "enum": ["telegram", "vk"],
    }
    assert {tuple(item["scopes"]) for item in descriptor["securitySchemes"]} == {
        ("telegram:read",),
        ("vk:read",),
    }


def test_item_resolve_descriptor_publishes_only_the_exact_link_contract(runtime) -> None:
    service, _adapter, _store = runtime
    service.adapters["vk"] = FakeAdapter()
    tool = next(
        item
        for item in build_social_workspace_tools(service)
        if item.name == "social_item_resolve"
    )

    schema = tool.input_schema
    assert set(schema["required"]) == {
        "platform",
        "operation",
        "target_locator",
        "read_access",
    }
    assert set(schema["properties"]) == {
        "platform",
        "operation",
        "target_locator",
        "read_access",
        "transcribe_audio",
        "transcription_wait_seconds",
    }
    assert schema["properties"]["target_locator"] == {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "value"],
        "properties": {
            "kind": {"const": "profile_link"},
            "value": {"type": "string", "minLength": 1, "maxLength": 512},
        },
    }
    assert "expected_target_kinds" not in schema["properties"]
    assert "Use this first" in tool.description
    waves = (
        MAX_TRANSCRIPTION_ATTACHMENTS_PER_READ
        + TRANSCRIPTION_REGISTRATION_CONCURRENCY
        - 1
    ) // TRANSCRIPTION_REGISTRATION_CONCURRENCY
    assert tool.timeout_seconds >= service.provider_timeout_seconds * (1 + waves) + 35


def test_only_four_read_actions_advertise_batch_wait_and_summary(runtime) -> None:
    service, _adapter, _store = runtime

    def contains_property(value: Any, name: str) -> bool:
        if isinstance(value, dict):
            properties = value.get("properties")
            if isinstance(properties, dict) and name in properties:
                return True
            return any(contains_property(child, name) for child in value.values())
        if isinstance(value, list):
            return any(contains_property(child, name) for child in value)
        return False

    tools = build_social_workspace_tools(service)
    expected = {
        "social_item_resolve",
        "social_content_feed",
        "social_content_item",
        "social_content_thread",
    }
    assert {
        tool.name
        for tool in tools
        if contains_property(tool.input_schema, "transcription_wait_seconds")
    } == expected
    assert {
        tool.name
        for tool in tools
        if contains_property(tool.output_schema, "transcription_summary")
    } == expected


def test_legacy_item_resolve_target_kind_hint_is_checked_after_resolution(runtime) -> None:
    service, _adapter, _store = runtime
    request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "resolve_item",
            "target_locator": {
                "kind": "profile_link",
                "value": "https://t.me/c/100/500",
            },
            "expected_target_kinds": ["group"],
        }
    )
    safe = {
        "item": {
            "item_ref": "itm_abcdefghijklmnop",
            "target_ref": "tgt_abcdefghijklmnop",
            "kind": "message",
            "published_at": "2026-08-26T05:00:00Z",
            "text": "",
            "caption": "",
            "basic_metrics": {"views": 0},
            "trust": "untrusted_external_data",
        },
        "source_target": {
            "target_ref": "tgt_abcdefghijklmnop",
            "kind": "channel",
            "title": "Resolved source",
            "about": "",
            "description": "",
            "basic_metrics": {"members": 1},
            "trust": "untrusted_external_data",
        },
        "trust": "untrusted_external_data",
    }

    with pytest.raises(
        SocialWorkspaceRuntimeError,
        match="source target kind mismatch",
    ):
        service._project_read_output(request, safe)


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
    assert status["operation_ref"].startswith("op_")
    assert status["operation_ref"] != "op_" + "0" * 24
    committed = await service.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        context(),
    )
    assert committed["status"] == "succeeded"
    by_preparation = await service.status(
        "preparation", prepared["preparation_ref"], context()
    )
    by_operation = await service.status(
        "operation", committed["operation_ref"], context()
    )
    assert by_preparation == by_operation == committed


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
async def test_provider_returned_unknown_is_not_audited_as_success(runtime) -> None:
    service, adapter, store = runtime
    adapter.forced_result = {
        "status": "outcome_unknown",
        "retry_safe": False,
        "error_code": "provider_timeout",
    }
    principal = RuntimePrincipal.from_context(context())
    target = service._mint_ref("target", "native-user", "telegram", principal)
    intent = validate_prepare_request({
        "platform": "telegram", "action": "send_message",
        "idempotency_key": "returned-unknown-123", "target_ref": target,
        "content": {"text": "Hello", "entities": [], "media": []},
    })
    prepared = await service.prepare(intent, context())
    approval = service.approve_preparation(
        preparation_ref=prepared["preparation_ref"],
        operator_principal="operator", operator_nonce="unknown-nonce-12345",
    )
    result = await service.commit({
        "preparation_ref": prepared["preparation_ref"], **approval,
        "action_digest": prepared["action_digest"],
    }, context())
    assert result["status"] == "outcome_unknown"
    with sqlite3.connect(store.path) as conn:
        audit = conn.execute(
            """SELECT outcome,reason_code FROM social_workspace_audit
               WHERE operation='commit' ORDER BY id DESC LIMIT 1"""
        ).fetchone()
    assert audit == ("outcome_unknown", "provider_timeout")


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
