from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from aiohttp import web

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.integration import attach_private_events_mcp


def _owner_identity(config, *, full_read: bool = False) -> AccessIdentity:
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


async def _operations_snapshot(server, identity, arguments):
    response = await server.protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "operations_snapshot",
                "arguments": arguments,
            },
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
    task: str = "telegraph_build",
    status: str = "pending",
    last_error: str | None = None,
    next_run_at: str = "2026-08-01T10:05:00Z",
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
                task,
                status,
                0,
                last_error,
                None,
                "2026-08-01T10:04:00Z",
                next_run_at,
                json.dumps({"event_id": event_id, "secret": "must-not-be-listed"}),
            ),
        )
        conn.commit()


def test_queue_observability_changes_only_owner_descriptor(config) -> None:
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
        "task",
        "status",
        "cursor",
        "limit",
    }
    assert owner_tool.input_schema["properties"]["limit"]["maximum"] == 10
    assert codex_tool.input_schema["properties"] == {}
    assert len(server.protocol.tools) == len(server.codex_protocol.tools) == 7
    assert {tool.name for tool in server.protocol.tools} == {
        tool.name for tool in server.codex_protocol.tools
    }
    assert server.protocol.policy_fingerprint.endswith("+queue-observability-r0")
    assert server.codex_protocol.policy_fingerprint == "codex-read-only-v1"


@pytest.mark.asyncio
async def test_default_snapshot_is_backward_compatible_and_read_only(
    config,
    event_db: Path,
    event_db_digest: str,
) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None

    result = await _operations_snapshot(server, _owner_identity(config), {})

    assert result["isError"] is False
    assert "publication_queue" not in result["structuredContent"]
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == event_db_digest


@pytest.mark.asyncio
async def test_owner_snapshot_lists_payload_free_redacted_jobs(
    config,
    event_db: Path,
    event_db_digest: str,
) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None

    result = await _operations_snapshot(
        server,
        _owner_identity(config),
        {"include_jobs": True},
    )

    assert result["isError"] is False
    page = result["structuredContent"]["publication_queue"]
    assert page["read_contract"] == {
        "database": "sqlite mode=ro; query_only=ON",
        "provider_network_calls": 0,
        "payload_included": False,
        "ordering": "job_id_desc",
        "queue_table_available": True,
        "max_page_rows": 10,
    }
    assert page["page_status_counts"] == {"error": 1}
    assert len(page["jobs"]) == 1
    job = page["jobs"][0]
    assert job["id"] == 7
    assert job["fetch_id"] == "job:7"
    assert job["event_id"] == 42
    assert job["task"] == "telegraph_build"
    assert job["next_run_at"] == "2026-08-01T10:05:00Z"
    assert "payload" not in job
    assert "last_result" not in job
    assert hashlib.sha256(event_db.read_bytes()).hexdigest() == event_db_digest


@pytest.mark.asyncio
async def test_queue_filters_are_parameterized_and_exact(config) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    identity = _owner_identity(config)

    matching = await _operations_snapshot(
        server,
        identity,
        {
            "event_id": 42,
            "task": "TELEGRAPH_BUILD",
            "status": "ERROR",
        },
    )
    assert matching["isError"] is False
    page = matching["structuredContent"]["publication_queue"]
    assert [job["id"] for job in page["jobs"]] == [7]
    assert page["filters"] == {
        "event_id": 42,
        "task": "telegraph_build",
        "status": "error",
    }

    empty = await _operations_snapshot(
        server,
        identity,
        {"event_id": 42, "status": "pending"},
    )
    assert empty["isError"] is False
    assert empty["structuredContent"]["publication_queue"]["jobs"] == []


@pytest.mark.asyncio
async def test_queue_cursor_is_stable_and_does_not_duplicate_rows(
    config,
    event_db: Path,
) -> None:
    for job_id in (8, 9, 10):
        _insert_job(event_db, job_id=job_id)

    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    identity = _owner_identity(config)

    first = await _operations_snapshot(
        server,
        identity,
        {"include_jobs": True, "limit": 2},
    )
    first_page = first["structuredContent"]["publication_queue"]
    assert [job["id"] for job in first_page["jobs"]] == [10, 9]
    assert first_page["next_cursor"]

    second = await _operations_snapshot(
        server,
        identity,
        {
            "include_jobs": True,
            "limit": 2,
            "cursor": first_page["next_cursor"],
        },
    )
    second_page = second["structuredContent"]["publication_queue"]
    assert [job["id"] for job in second_page["jobs"]] == [8, 7]
    assert second_page["next_cursor"] is None
    assert not (
        {job["id"] for job in first_page["jobs"]}
        & {job["id"] for job in second_page["jobs"]}
    )


@pytest.mark.asyncio
async def test_queue_redacts_bearer_and_assignment_credentials(
    config,
    event_db: Path,
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

    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    result = await _operations_snapshot(
        server,
        _owner_identity(config),
        {"include_jobs": True},
    )
    job = result["structuredContent"]["publication_queue"]["jobs"][0]
    serialized = json.dumps(job, ensure_ascii=False)

    assert "visible-key-must-not-survive" not in serialized
    assert "abcdefghijklmnopqrstuvwxyz" not in serialized
    assert "<redacted>" in serialized
    assert "ordinary diagnostic" in serialized


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"include_jobs": "yes"}, "include_jobs must be a boolean"),
        (
            {"include_jobs": False, "status": "pending"},
            "include_jobs=false cannot be combined with queue filters",
        ),
        ({"include_jobs": True, "cursor": "not-a-valid-cursor"}, "cursor is invalid"),
        (
            {"include_jobs": True, "due_after": "2026-08-01T00:00:00Z"},
            "Unsupported argument field(s): due_after",
        ),
        (
            {"include_jobs": True, "status": "error' OR 1=1--"},
            "status is invalid",
        ),
    ],
)
async def test_queue_arguments_fail_closed(config, arguments, message) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None

    result = await _operations_snapshot(server, _owner_identity(config), arguments)

    assert result["isError"] is True
    assert message in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_existing_fetch_job_detail_path_remains_unchanged(config) -> None:
    server = attach_private_events_mcp(web.Application(), config)
    assert server is not None
    identity = _owner_identity(config, full_read=True)

    response = await server.protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "fetch", "arguments": {"id": "job:7"}},
        },
        identity,
    )

    assert response is not None
    result = response["result"]
    assert result["isError"] is False
    assert result["structuredContent"]["id"] == "job:7"
