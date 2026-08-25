import hashlib
import time
from types import SimpleNamespace

import pytest

from audio_transcription.job_store import JobOwnershipError
from audio_transcription.mcp import (
    _merge_audio_tools_for_discovery,
    build_audio_transcription_tools,
)
from private_events_mcp.access_policy import social_scopes_authorized
from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.tool_catalog import ToolCallContext, ToolExecutionError


def test_existing_telegram_publish_scope_authorizes_audio_tools() -> None:
    tools = build_audio_transcription_tools(object(), signing_key="s" * 32)
    legacy = frozenset({"telegram:publish"})
    dedicated = frozenset({"audio:transcribe"})

    assert {tool.name for tool in tools} == {
        "audio_transcription_start",
        "audio_transcription_status",
        "audio_transcription_get",
    }
    for tool in tools:
        assert tool.is_visible(legacy)
        assert tool.is_visible(dedicated)
        assert social_scopes_authorized(tool.required_scopes({}), legacy)
        assert not tool.is_visible(frozenset({"telegram:read"}))


def test_audio_tools_are_discovery_prioritized_without_dropping_existing_tools() -> None:
    audio = build_audio_transcription_tools(object(), signing_key="s" * 32)
    existing = tuple(SimpleNamespace(name=f"existing_{index}") for index in range(27))

    merged = _merge_audio_tools_for_discovery(existing, audio)

    assert [tool.name for tool in merged[:3]] == [
        "audio_transcription_start",
        "audio_transcription_status",
        "audio_transcription_get",
    ]
    assert merged[3:] == existing
    assert len(merged) == 30


@pytest.mark.asyncio
async def test_status_and_get_accept_social_read_principal_binding() -> None:
    """A social-read atr ref remains usable by the public status/get tools."""

    resource = "https://mcp.example.test/mcp"
    identity = AccessIdentity(
        "alice",
        "chatgpt",
        frozenset({"telegram:publish"}),
        resource,
        "jti",
        int(time.time()) + 3600,
    )
    context = ToolCallContext(identity, resource)
    social_owner = hashlib.sha256(
        f"{identity.client_id}\0{identity.subject}\0{resource}".encode("utf-8")
    ).hexdigest()

    class SocialOwnedJobService:
        def __init__(self) -> None:
            self.status_bindings: list[str] = []
            self.get_bindings: list[str] = []

        async def status(self, *, job_ref, owner_binding):
            self.status_bindings.append(owner_binding)
            if owner_binding != social_owner:
                raise JobOwnershipError("wrong owner")
            return {"job_ref": job_ref, "state": "complete"}

        async def get_result(self, *, job_ref, owner_binding, **_values):
            self.get_bindings.append(owner_binding)
            if owner_binding != social_owner:
                raise JobOwnershipError("wrong owner")
            return {"job_ref": job_ref, "state": "complete", "ready": True, "text": "ok"}

    service = SocialOwnedJobService()
    tools = {
        tool.name: tool
        for tool in build_audio_transcription_tools(service, signing_key="s" * 32)
    }

    status = await tools["audio_transcription_status"].handler(
        {"job_ref": "atr_" + "a" * 43}, context
    )
    result = await tools["audio_transcription_get"].handler(
        {"job_ref": "atr_" + "a" * 43, "view": "plain"}, context
    )

    assert status["state"] == "complete"
    assert result["text"] == "ok"
    assert service.status_bindings[-1] == social_owner
    assert service.get_bindings[-1] == social_owner

    other_identity = AccessIdentity(
        "bob",
        "chatgpt",
        frozenset({"telegram:publish"}),
        resource,
        "other-jti",
        int(time.time()) + 3600,
    )
    with pytest.raises(ToolExecutionError) as denied:
        await tools["audio_transcription_status"].handler(
            {"job_ref": "atr_" + "a" * 43},
            ToolCallContext(other_identity, resource),
        )
    assert denied.value.error_code == "TRANSCRIPTION_PRINCIPAL_MISMATCH"
