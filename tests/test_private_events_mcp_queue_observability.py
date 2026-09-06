from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest
from aiohttp import web

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.repository import EventsEvidenceRepository
from private_events_mcp.integration import attach_private_events_mcp


def _owner(config, *, full_read: bool = False) -> AccessIdentity:
    scopes = (
        frozenset({"events:read", "incidents:read", "operations:read"})
        if full_read
        else frozenset({"operations:read"})
    )
    return AccessIdentity(
        "operator",
        config.oauth_client_id,
        scopes,
        config.resource,
        "queue-observability-owner-jti",
        2_000_000_000,
    )


async def _snapshot(server, identity, arguments):
    response = await server.protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "operations_snapshot", "arguments": arguments},
        },
        identity,
    )
    assert response is not None
    return response["result"]


def _insert_job(
    database: Path,
    *,
    job_id: int,
    event_id: int = 42,
    status: str = "pending",
    last_error: str | None = None,
) -> None:
    with sqlite3.connect(database) as conn:
        conn.execute(
            """
            INSERT INTO joboutbox(
                id,event_id,task,status,attempts,last_error,last_result,
                updated_at,next_run_at,payload
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                job_id,
                event_id,
                "telegraph_build",
                status,
                0,
                last_error,
                "must-not-be-listed",
                "2026-08-01T10:04:00Z",
                "2026-08-01T10:05:00Z",
                json.dumps({"secret": "must-not-be-listed"}),
            ),
        )
        conn.commit()


def test_descriptor_changes_owner_only(config) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    owner_tool = next(
        tool for tool in server.protocol.tools if tool.name == "operations_snapshot"
    )
    codex_tool = next(
        tool for tool in server.codex_protocol.tools if tool.name == "operations_snapshot"
    )

    assert set(owner_tool.input_schema["properties"]) == {
        "include_jobs",
        "event_id",
        "status",
        "before_job_id",
        "limit",
    }
    assert owner_tool.input_schema["properties"]["event_id"]["minimum"] == 0
    assert owner_tool.input_schema["properties"]["limit"]["maximum"] == 10
    assert codex_tool.input_schema["properties"] == {}
    assert len(server.protocol.tools) == len(server.codex_protocol.tools) == 7
    assert {tool.name for tool in server.protocol.tools} == {
        tool.name for tool in server.codex_protocol.tools
    }
    assert server.protocol.policy_fingerprint.endswith("+queue-observability-r0")
    assert server.codex_protocol.policy_fingerprint == "codex-read-only-v1"


@pytest.mark.asyncio
async def test_default_call_is_backward_compatible_and_read_only(
    config,
    event_db: Path,
    event_db_digest: str,
) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None

    result = await _snapshot(server, _owner(config), {})

    assert result["isError"] is False
    assert "job_queue" not in result["structuredContent"]
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == event_db_digest


@pytest.mark.asyncio
async def test_missing_queue_table_is_reported_not_crashed(config, tmp_path: Path) -> None:
    legacy_db = tmp_path / "legacy-empty.sqlite"
    sqlite3.connect(legacy_db).close()
    legacy = replace(config, database_path=str(legacy_db))
    server = attach_private_events_mcp(web.Application(), legacy)
    assert server is not None

    result = await _snapshot(server, _owner(legacy), {"include_jobs": True})

    assert result["isError"] is False
    page = result["structuredContent"]["job_queue"]
    assert page["jobs"] == []
    assert page["next_before_job_id"] is None
    assert page["read_contract"]["queue_table_available"] is False
    assert page["read_contract"]["queue_schema_supported"] is False


@pytest.mark.asyncio
async def test_page_is_small_payload_free_redacted_and_read_only(
    config,
    event_db: Path,
    event_db_digest: str,
) -> None:
    with sqlite3.connect(event_db) as conn:
        conn.execute(
            "UPDATE joboutbox SET last_error=? WHERE id=7",
            (
                "Authorization: Bearer abcdefghijklmnopqrstuvwxyz "
                "api_key=visible-key-must-not-survive ordinary diagnostic",
            ),
        )
        conn.commit()
    digest_before_call = hashlib.sha256(event_db.read_bytes()).hexdigest()

    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    result = await _snapshot(server, _owner(config), {"include_jobs": True})

    assert result["isError"] is False
    page = result["structuredContent"]["job_queue"]
    assert page["read_contract"] == {
        "database": "sqlite mode=ro; query_only=ON",
        "provider_network_calls": 0,
        "payload_included": False,
        "ordering": "job_id_desc",
        "queue_table_available": True,
        "queue_schema_supported": True,
        "missing_columns": [],
        "max_page_rows": 10,
    }
    assert len(page["jobs"]) == 1
    job = page["jobs"][0]
    assert job["id"] == 7
    assert job["fetch_id"] == "job:7"
    assert job["event_id"] == 42
    assert job["task"] == "telegraph_build"
    assert job["next_run_at"] == "2026-08-01T10:05:00Z"
    assert "payload" not in job
    assert "last_result" not in job
    serialized = json.dumps(job, ensure_ascii=False)
    assert "visible-key-must-not-survive" not in serialized
    assert "abcdefghijklmnopqrstuvwxyz" not in serialized
    assert "<redacted>" in serialized
    assert "ordinary diagnostic" in serialized
    assert digest_before_call != event_db_digest
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == digest_before_call


@pytest.mark.asyncio
async def test_event_and_status_filters_include_global_jobs(
    config,
    event_db: Path,
) -> None:
    _insert_job(event_db, job_id=8, event_id=0, status="pending")
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    identity = _owner(config)

    matching = await _snapshot(
        server,
        identity,
        {"event_id": 42, "status": "ERROR"},
    )
    page = matching["structuredContent"]["job_queue"]
    assert [job["id"] for job in page["jobs"]] == [7]
    assert page["filters"] == {"event_id": 42, "status": "error"}

    global_jobs = await _snapshot(server, identity, {"event_id": 0})
    global_page = global_jobs["structuredContent"]["job_queue"]
    assert [job["id"] for job in global_page["jobs"]] == [8]
    assert global_page["filters"] == {"event_id": 0}


@pytest.mark.asyncio
async def test_numeric_before_id_pagination_has_exact_end_and_no_duplicates(
    config,
    event_db: Path,
) -> None:
    for job_id in (8, 9, 10):
        _insert_job(event_db, job_id=job_id)

    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    identity = _owner(config)

    first = await _snapshot(server, identity, {"include_jobs": True, "limit": 2})
    first_page = first["structuredContent"]["job_queue"]
    assert [job["id"] for job in first_page["jobs"]] == [10, 9]
    assert first_page["next_before_job_id"] == 9

    second = await _snapshot(
        server,
        identity,
        {"include_jobs": True, "limit": 2, "before_job_id": 9},
    )
    second_page = second["structuredContent"]["job_queue"]
    assert [job["id"] for job in second_page["jobs"]] == [8, 7]
    assert second_page["next_before_job_id"] is None
    assert not (
        {job["id"] for job in first_page["jobs"]}
        & {job["id"] for job in second_page["jobs"]}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"include_jobs": True, "event_id": 0.9}, "event_id must be an integer"),
        ({"include_jobs": "yes"}, "include_jobs must be a boolean"),
        (
            {"include_jobs": False, "status": "pending"},
            "include_jobs=false cannot be combined with queue filters",
        ),
        ({"include_jobs": True, "event_id": -1}, "event_id must be non-negative"),
        ({"include_jobs": True, "before_job_id": 0}, "before_job_id must be positive"),
        (
            {"include_jobs": True, "task": "telegraph_build"},
            "Unsupported argument field(s): task",
        ),
        (
            {"include_jobs": True, "status": "error' OR 1=1--"},
            "status is invalid",
        ),
    ],
)
async def test_arguments_fail_closed(config, arguments, message) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None

    result = await _snapshot(server, _owner(config), arguments)

    assert result["isError"] is True
    assert message in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_existing_fetch_job_detail_path_is_unchanged(config) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    response = await server.protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "fetch", "arguments": {"id": "job:7"}},
        },
        _owner(config, full_read=True),
    )

    assert response is not None
    result = response["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["id"] == "job:7"


@pytest.mark.asyncio
async def test_snapshot_never_starts_full_database_integrity_scan(config, monkeypatch):
    """Production regression: a full quick_check can exhaust the HTTP tool budget."""
    repository = EventsEvidenceRepository(config)
    async def forbidden_integrity_check():
        pytest.fail("Interactive snapshot must not scan the full database")
    monkeypatch.setattr(repository.db, "quick_check", forbidden_integrity_check)
    snapshot = await repository.operations_snapshot()
    assert snapshot["database"]["quick_check"] == "not_run:interactive_budget"
    assert snapshot["network"]["provider_calls"] == 0


@pytest.mark.asyncio
async def test_nested_json_error_is_decoded_before_redaction(config, event_db):
    with sqlite3.connect(event_db) as conn:
        conn.execute("UPDATE joboutbox SET last_error=? WHERE id=7", (json.dumps({
            "nested": {"api_key": "secretcredentialhere", "user_id": 12345,
                       "message": "safe diagnostic", "token": "another-secret"}}),))
    server = attach_private_events_mcp(web.Application(), config)
    result = await _snapshot(server, _owner(config), {"include_jobs": True})
    assert result["isError"] is False
    serialized = json.dumps(result["structuredContent"]["job_queue"])
    assert "secretcredentialhere" not in serialized
    assert "another-secret" not in serialized
    assert "12345" not in serialized
    assert "safe diagnostic" in serialized


@pytest.mark.parametrize("value", [
    '{"nested":{"api_key":"secretcredentialhere","user_id":12345}',
    "{'api_key': 'secretcredentialhere'}",
    '{"api_key":"secretcredentialhere","padding":"' + ('x' * 17000) + '"}',
])
def test_malformed_or_oversized_structured_error_never_falls_back_to_raw_text(value):
    from private_events_mcp.queue_read import _safe_job
    output = json.dumps(_safe_job({"id": 7, "last_error": value}))
    assert "secretcredentialhere" not in output
    assert "12345" not in output
    assert "structured error omitted" in output
