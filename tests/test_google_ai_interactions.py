from __future__ import annotations

import io
import json
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import pytest

from google_ai.client import ExternalCallLease
from google_ai.exceptions import ProviderError
from google_ai.interactions import (
    ANTIGRAVITY_AGENT,
    INTERACTIONS_API_REVISION,
    AntigravityInteractionsClient,
    HTTPResponse,
    InteractionDeadlineExceeded,
    InteractionsProtocolError,
)


def _json_response(payload: dict[str, Any], status: int = 200) -> HTTPResponse:
    return HTTPResponse(
        status=status,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
    )


def _interaction_payload(
    status: str,
    *,
    interaction_id: str = "interaction-1",
    environment_id: str = "env-1",
    text: Optional[str] = None,
    usage: Optional[dict[str, int]] = None,
) -> dict[str, Any]:
    steps = []
    if text is not None:
        steps.append(
            {
                "type": "model_output",
                "content": [{"type": "text", "text": text}],
            }
        )
    return {
        "id": interaction_id,
        "status": status,
        "environment_id": environment_id,
        "steps": steps,
        "usage": usage or {},
    }


class _FakeTransport:
    def __init__(self, responses: Sequence[HTTPResponse]):
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: Optional[bytes],
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> HTTPResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body,
                "timeout_seconds": timeout_seconds,
                "max_response_bytes": max_response_bytes,
            }
        )
        if not self.responses:
            raise AssertionError("unexpected transport request")
        return self.responses.pop(0)


class _FakeLimiter:
    def __init__(self):
        self.reserve_calls: list[dict[str, Any]] = []
        self.sent: list[ExternalCallLease] = []
        self.finalized: list[dict[str, Any]] = []
        self.semantic: list[dict[str, Any]] = []

    async def reserve_external_call(
        self,
        *,
        model: str,
        reserved_tpm: int,
        key_envs: Sequence[str],
        request_uid: Optional[str] = None,
    ) -> ExternalCallLease:
        call = {
            "model": model,
            "reserved_tpm": reserved_tpm,
            "key_envs": tuple(key_envs),
            "request_uid": request_uid,
        }
        self.reserve_calls.append(call)
        return ExternalCallLease(
            request_uid=str(request_uid),
            attempt_no=1,
            consumer="festival_antigravity",
            account_name=None,
            model=model,
            reserved_tpm=reserved_tpm,
            api_key_id=f"key-{len(self.reserve_calls)}",
            env_var_name=str(key_envs[0]),
            key_alias=f"ag-{len(self.reserve_calls)}",
            minute_bucket=None,
            day_bucket=None,
            started_at=datetime.now(timezone.utc),
        )

    def get_external_call_api_key(self, lease: ExternalCallLease) -> str:
        return "test-secret-key"

    async def mark_external_call_sent(self, lease: ExternalCallLease) -> None:
        self.sent.append(lease)

    async def finalize_external_call(self, lease: ExternalCallLease, **kwargs):
        self.finalized.append({"lease": lease, **kwargs})

    async def record_external_call_semantic_result(self, lease, **kwargs):
        self.semantic.append({"lease": lease, **kwargs})


def _client(
    responses: Sequence[HTTPResponse],
    *,
    limiter: Optional[_FakeLimiter] = None,
    **kwargs,
):
    fake_limiter = limiter or _FakeLimiter()
    transport = _FakeTransport(responses)
    client = AntigravityInteractionsClient(
        fake_limiter,  # type: ignore[arg-type]
        key_envs=["GOOGLE_ANTIGRAVITY_KEY_A", "GOOGLE_ANTIGRAVITY_KEY_B"],
        transport=transport,
        poll_interval_seconds=0,
        **kwargs,
    )
    return client, fake_limiter, transport


@pytest.mark.asyncio
async def test_create_uses_revision_background_store_and_one_distinct_lease_per_post():
    client, limiter, transport = _client(
        [
            _json_response(_interaction_payload("queued", interaction_id="interaction-1")),
            _json_response(_interaction_payload("queued", interaction_id="interaction-2")),
        ]
    )

    first = await client.create("research A", max_total_tokens=50_000)
    second = await client.create("research B", max_total_tokens=60_000)

    assert first.provider_status == second.provider_status == "queued"
    assert len(limiter.reserve_calls) == 2
    assert limiter.reserve_calls[0]["request_uid"] != limiter.reserve_calls[1]["request_uid"]
    assert [call["reserved_tpm"] for call in limiter.reserve_calls] == [50_000, 60_000]
    request = transport.calls[0]
    body = json.loads(request["body"])
    assert request["method"] == "POST"
    assert request["headers"]["Api-Revision"] == INTERACTIONS_API_REVISION
    assert request["headers"]["x-goog-api-key"] == "test-secret-key"
    assert request["headers"]["X-Request-Id"] == limiter.reserve_calls[0]["request_uid"]
    assert body == {
        "agent": ANTIGRAVITY_AGENT,
        "input": "research A",
        "environment": "remote",
        "background": True,
        "store": True,
        "agent_config": {"type": "antigravity", "max_total_tokens": 50_000},
    }
    assert "test-secret-key" not in repr(first)
    restored = type(first).from_checkpoint(first.to_checkpoint())
    assert restored.id == first.id
    assert restored.provider_status == first.provider_status
    assert restored.lease == first.lease
    assert not limiter.finalized, "active create response must remain open for polling"


@pytest.mark.asyncio
async def test_wait_polls_without_extra_rpd_and_finalizes_provider_not_semantics():
    usage = {
        "total_input_tokens": 100,
        "total_output_tokens": 20,
        "total_tokens": 120,
    }
    client, limiter, transport = _client(
        [
            _json_response(_interaction_payload("queued")),
            _json_response(_interaction_payload("in_progress")),
            _json_response(_interaction_payload("completed", text='{"ok":true}', usage=usage)),
        ]
    )

    created = await client.create("research", max_total_tokens=50_000)
    result = await client.wait(created, deadline_seconds=5)

    assert result.provider_status == "completed"
    assert result.output_text == '{"ok":true}'
    assert result.usage.total_tokens == 120
    assert len(limiter.reserve_calls) == 1
    assert [call["method"] for call in transport.calls] == ["POST", "GET", "GET"]
    assert all("X-Request-Id" not in call["headers"] for call in transport.calls[1:])
    assert len(limiter.finalized) == 1
    finalized = limiter.finalized[0]
    assert finalized["provider_terminal_status"] == "completed"
    assert finalized["semantic_status"] == "not_evaluated"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["incomplete", "budget_exceeded", "requires_action"])
async def test_non_success_stops_are_not_collapsed_to_semantic_success(status):
    client, limiter, _transport = _client(
        [_json_response(_interaction_payload(status, text="partial"))]
    )

    result = await client.create("research", max_total_tokens=100_000)

    assert result.provider_status == status
    assert result.output_text == "partial"
    assert limiter.finalized[0]["provider_terminal_status"] == status
    assert limiter.finalized[0]["semantic_status"] == "not_evaluated"
    assert limiter.finalized[0]["error"] is None


@pytest.mark.asyncio
async def test_continuation_sends_previous_id_environment_and_pins_original_key():
    client, limiter, transport = _client(
        [
            _json_response(_interaction_payload("incomplete", interaction_id="first", environment_id="env-a")),
            _json_response(_interaction_payload("queued", interaction_id="second", environment_id="env-a")),
        ]
    )
    previous = await client.create("first pass", max_total_tokens=40_000)

    continued = await client.continue_interaction(
        previous,
        "continue with checkpoints",
        max_total_tokens=30_000,
    )

    body = json.loads(transport.calls[1]["body"])
    assert continued.id == "second"
    assert body["previous_interaction_id"] == "first"
    assert body["environment"] == "env-a"
    assert limiter.reserve_calls[1]["key_envs"] == (
        previous.lease.env_var_name,
    )
    assert limiter.reserve_calls[0]["request_uid"] != limiter.reserve_calls[1]["request_uid"]


@pytest.mark.asyncio
async def test_deadline_cancels_with_current_path_without_consuming_rpd():
    client, limiter, transport = _client(
        [
            _json_response(_interaction_payload("in_progress")),
            _json_response(_interaction_payload("cancelled")),
        ]
    )
    created = await client.create("long task", max_total_tokens=20_000)

    with pytest.raises(InteractionDeadlineExceeded) as exc_info:
        await client.wait(created, deadline_seconds=0, cancel_on_deadline=True)

    assert exc_info.value.cancel_result is not None
    assert exc_info.value.cancel_result.provider_status == "cancelled"
    assert len(limiter.reserve_calls) == 1
    assert transport.calls[1]["url"].endswith("/interactions/interaction-1/cancel")
    assert transport.calls[1]["headers"]["X-Request-Id"]
    assert limiter.finalized[0]["provider_terminal_status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_falls_back_to_preview_colon_spelling_only_on_404():
    client, limiter, transport = _client(
        [
            _json_response(_interaction_payload("in_progress")),
            _json_response({"error": {"message": "not found"}}, status=404),
            _json_response(_interaction_payload("cancelled")),
        ]
    )
    created = await client.create("long task", max_total_tokens=20_000)

    result = await client.cancel(created)

    assert result.provider_status == "cancelled"
    assert transport.calls[1]["url"].endswith("/interaction-1/cancel")
    assert transport.calls[2]["url"].endswith("/interaction-1:cancel")
    assert transport.calls[1]["headers"]["X-Request-Id"] != transport.calls[2]["headers"]["X-Request-Id"]
    assert len(limiter.reserve_calls) == 1


@pytest.mark.asyncio
async def test_unknown_provider_status_is_protocol_failure_and_accounted_as_failed():
    client, limiter, _transport = _client(
        [_json_response(_interaction_payload("mystery"))]
    )

    with pytest.raises(InteractionsProtocolError):
        await client.create("research", max_total_tokens=10_000)

    assert limiter.finalized[0]["provider_terminal_status"] == "failed"
    assert limiter.finalized[0]["semantic_status"] == "not_evaluated"
    assert limiter.finalized[0]["error"].error_type == "unknown_interaction_status"


@pytest.mark.asyncio
async def test_http_error_never_places_key_in_exception():
    client, limiter, _transport = _client(
        [
            _json_response(
                {
                    "error": {
                        "message": "capacity unavailable for test-secret-key"
                    }
                },
                status=503,
            )
        ]
    )

    with pytest.raises(ProviderError) as exc_info:
        await client.create("research", max_total_tokens=10_000)

    assert exc_info.value.status_code == 503
    assert "test-secret-key" not in str(exc_info.value)
    assert "[REDACTED]" in str(exc_info.value)
    assert limiter.finalized[0]["provider_terminal_status"] == "failed"


def _tar_bytes(entries: dict[str, bytes], *, symlink: Optional[tuple[str, str]] = None) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, content in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
        if symlink:
            info = tarfile.TarInfo(symlink[0])
            info.type = tarfile.SYMTYPE
            info.linkname = symlink[1]
            archive.addfile(info)
    return buffer.getvalue()


@pytest.mark.asyncio
async def test_environment_download_is_atomic_safe_and_consumes_no_rpd(tmp_path: Path):
    snapshot = _tar_bytes({"workspace/result.json": b'{"ok":true}'})
    client, limiter, transport = _client(
        [
            _json_response(_interaction_payload("completed")),
            HTTPResponse(status=200, headers={"content-type": "application/x-tar"}, body=snapshot),
        ]
    )
    interaction = await client.create("write result", max_total_tokens=10_000)
    tar_path = tmp_path / "snapshot.tar"
    extracted = tmp_path / "extracted"

    returned = await client.download_environment(
        interaction,
        tar_path,
        extract_to=extracted,
    )

    assert returned == tar_path
    assert tar_path.read_bytes() == snapshot
    assert (extracted / "workspace" / "result.json").read_text() == '{"ok":true}'
    assert len(limiter.reserve_calls) == 1
    assert transport.calls[1]["method"] == "GET"
    assert transport.calls[1]["url"].endswith("/files/environment-env-1:download?alt=media")
    assert "X-Request-Id" not in transport.calls[1]["headers"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot",
    [
        _tar_bytes({"../escape.txt": b"escape"}),
        _tar_bytes({}, symlink=("workspace/link", "/etc/passwd")),
    ],
)
async def test_environment_extraction_rejects_traversal_and_links(tmp_path: Path, snapshot: bytes):
    client, _limiter, _transport = _client(
        [
            _json_response(_interaction_payload("completed")),
            HTTPResponse(status=200, headers={}, body=snapshot),
        ]
    )
    interaction = await client.create("write result", max_total_tokens=10_000)

    with pytest.raises(ValueError, match="unsafe environment snapshot member"):
        await client.download_environment(
            interaction,
            tmp_path / "snapshot.tar",
            extract_to=tmp_path / "extracted",
        )
    assert not (tmp_path / "escape.txt").exists()


@pytest.mark.asyncio
async def test_validation_blocks_unsupported_budget_and_active_continuation():
    client, _limiter, _transport = _client([])
    with pytest.raises(ValueError, match="between 1 and 100000"):
        await client.create("task", max_total_tokens=100_001)

    lease = ExternalCallLease(
        request_uid="00000000-0000-4000-8000-000000000123",
        attempt_no=1,
        consumer="festival_antigravity",
        account_name=None,
        model=ANTIGRAVITY_AGENT,
        reserved_tpm=1,
        api_key_id="key-a",
        env_var_name="GOOGLE_ANTIGRAVITY_KEY_A",
        key_alias="ag-a",
        minute_bucket=None,
        day_bucket=None,
        started_at=datetime.now(timezone.utc),
    )
    active = client._parse_interaction(_interaction_payload("queued"), lease)
    with pytest.raises(ValueError, match="active interaction"):
        await client.continue_interaction(active, max_total_tokens=1)


def test_interaction_accounting_migration_is_additive_and_preserves_semantic_gate():
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "007_google_ai_interaction_accounting.sql"
    ).read_text(encoding="utf-8")

    assert "DROP " not in migration.upper()
    assert "google_ai_finalize_interaction" in migration
    assert "google_ai_record_interaction_semantic" in migration
    assert "provider_completed" in migration
    assert "failed_semantic" in migration
    assert "p_semantic_status = 'passed'" in migration
    assert "provider_terminal_status" in migration
    assert "UPDATE google_ai_model_limits" not in migration
