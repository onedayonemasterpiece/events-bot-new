from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from aiohttp import web
from sqlalchemy import func, select

import main
import smart_event_update as smart_update_module
from db import Database
from models import Event, EventSource, JobOutbox
from private_events_mcp.config import PrivateEventsMCPConfig
from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.event_create import EventCreateRuntime
from private_events_mcp.event_create_adapter import MainEventCreateExecutor
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.server import PrivateEventsMCPServer
from source_parse_contract import (
    EvidenceManifest,
    LifecycleAction,
    LifecycleActionType,
    SourceDisposition,
    SourceParseDecision,
    SourceParseRetryReason,
)


class FakeExecutor:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.calls = 0
        self.requests = []
        self.result = result or {
            "status": "accepted",
            "event_ids": [101],
            "events": [{"event_id": 101, "result": "created"}],
            "jobs": [{"job_id": 501, "task": "telegraph_build", "status": "pending"}],
        }

    async def create(self, request):
        self.calls += 1
        self.requests.append(request)
        await asyncio.sleep(0)
        return dict(self.result)


def _owner(config: PrivateEventsMCPConfig, *, subject: str = "operator") -> AccessIdentity:
    return AccessIdentity(
        subject=subject,
        client_id=config.oauth_client_id,
        scopes=frozenset({"events:write", "operations:read"}),
        audience=config.resource,
        token_id="owner-event-create-jti",
        expires_at=2_000_000_000,
    )


def _codex(config: PrivateEventsMCPConfig) -> AccessIdentity:
    return AccessIdentity(
        subject="codex",
        client_id=config.codex_oauth_client_id,
        scopes=frozenset({"events:read", "operations:read", "events:write"}),
        audience=config.codex_resource,
        token_id="codex-event-create-jti",
        expires_at=2_000_000_000,
    )


async def _call(protocol, identity, name: str, arguments: dict[str, Any]):
    response = await protocol.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
        identity,
    )
    assert response is not None
    return response["result"]


async def _list_tools(protocol, identity) -> set[str]:
    response = await protocol.dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        identity,
    )
    assert response is not None
    return {tool["name"] for tool in response["result"]["tools"]}


async def _database(tmp_path: Path) -> Database:
    database = Database(str(tmp_path / "events-create.sqlite"))
    await database.init()
    return database


async def _insert_event(database: Database, event_id: int = 101) -> None:
    async with database.get_session() as session:
        session.add(
            Event(
                id=event_id,
                title="Fixture event",
                description="Fixture event for operation-ledger tests.",
                short_description="Fixture event.",
                date=(date.today() + timedelta(days=60)).isoformat(),
                time="19:00",
                location_name="Fixture hall",
                city="Калининград",
                source_text="Fixture source text.",
            )
        )
        await session.commit()


def _enabled_config(
    config: PrivateEventsMCPConfig, database: Database
) -> PrivateEventsMCPConfig:
    return replace(
        config,
        database_path=database.path,
        event_create_enabled=True,
        cache_ttl_seconds=0,
    )


def _request(**overrides: Any) -> dict[str, Any]:
    request = {
        "raw_text": (
            "10 октября в 19:00 состоится тестовая лекция о городской архитектуре "
            "в Тестовом зале, Калининград."
        ),
        "source_external_id": "partner-message-1001",
        "idempotency_key": "owner-create-1001",
        "text_policy": "smart_rewrite",
    }
    request.update(overrides)
    return request


@pytest.mark.asyncio
async def test_event_create_is_default_off_and_requires_canonical_database(config) -> None:
    disabled = attach_private_events_mcp(web.Application(), config)
    assert disabled is not None
    assert not any(tool.name.startswith("event_create_") for tool in disabled.protocol.tools)
    assert "event_operation_get" not in {tool.name for tool in disabled.protocol.tools}

    enabled = replace(config, event_create_enabled=True)
    with pytest.raises(ValueError, match="canonical EventsBot Database"):
        attach_private_events_mcp(web.Application(), enabled)


@pytest.mark.asyncio
async def test_owner_tools_are_enabled_only_on_owner_projection(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    fake = FakeExecutor()
    enabled = _enabled_config(config, database)
    runtime = EventCreateRuntime(config=enabled, database=database, executor=fake)
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    try:
        owner_tools = await _list_tools(server.protocol, _owner(enabled))
        codex_tools = await _list_tools(server.codex_protocol, _codex(enabled))
    finally:
        await runtime.shutdown()
        await database.close()

    assert {
        "event_create_prepare",
        "event_create_commit",
        "event_operation_get",
    }.issubset(owner_tools)
    assert not any(name.startswith("event_create_") for name in codex_tools)
    assert "event_operation_get" not in codex_tools


@pytest.mark.asyncio
async def test_prepare_is_stateless_and_mutates_no_canonical_table(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    enabled = _enabled_config(config, database)
    runtime = EventCreateRuntime(
        config=enabled, database=database, executor=FakeExecutor()
    )
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    before: dict[str, int] = {}
    try:
        async with database.raw_conn() as conn:
            for table in ("event", "event_source", "joboutbox", "event_change_log"):
                before[table] = int(
                    (await (await conn.execute(f"SELECT COUNT(*) FROM {table}")).fetchone())[0]
                )
        result = await _call(
            server.protocol,
            _owner(enabled),
            "event_create_prepare",
            _request(),
        )
        async with database.raw_conn() as conn:
            after = {
                table: int(
                    (await (await conn.execute(f"SELECT COUNT(*) FROM {table}")).fetchone())[0]
                )
                for table in before
            }
    finally:
        await runtime.shutdown()
        await database.close()

    assert result["isError"] is False
    prepared = result["structuredContent"]
    assert prepared["committable"] is True
    assert prepared["planned_effects"]["canonical_write"] == "full Smart Update only"
    assert prepared["planned_effects"]["direct_provider_calls"] == 0
    assert after == before


@pytest.mark.asyncio
async def test_commit_is_durable_idempotent_and_pollable(config, tmp_path: Path) -> None:
    database = await _database(tmp_path)
    await _insert_event(database)
    enabled = _enabled_config(config, database)
    fake = FakeExecutor()
    runtime = EventCreateRuntime(config=enabled, database=database, executor=fake)
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    identity = _owner(enabled)
    request = _request()
    try:
        prepared_result = await _call(
            server.protocol, identity, "event_create_prepare", request
        )
        prepared = prepared_result["structuredContent"]
        commit_arguments = {
            **request,
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        }
        first = await _call(
            server.protocol, identity, "event_create_commit", commit_arguments
        )
        operation_ref = first["structuredContent"]["operation_ref"]
        await runtime.wait_for_operation(operation_ref)
        status = await _call(
            server.protocol,
            identity,
            "event_operation_get",
            {"operation_ref": operation_ref},
        )
        second = await _call(
            server.protocol, identity, "event_create_commit", commit_arguments
        )
        async with database.raw_conn() as conn:
            count = int(
                (
                    await (
                        await conn.execute("SELECT COUNT(*) FROM event_change_log")
                    ).fetchone()
                )[0]
            )
    finally:
        await runtime.shutdown()
        await database.close()

    assert status["isError"] is False
    terminal = status["structuredContent"]
    assert terminal["status"] == "accepted"
    assert terminal["terminal"] is True
    assert terminal["event_id"] == 101
    assert terminal["result"]["event_ids"] == [101]
    assert second["structuredContent"]["operation_ref"] == operation_ref
    assert fake.calls == 1
    assert count == 1


@pytest.mark.asyncio
async def test_same_idempotency_key_with_changed_request_fails_closed(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    await _insert_event(database)
    enabled = _enabled_config(config, database)
    fake = FakeExecutor()
    runtime = EventCreateRuntime(config=enabled, database=database, executor=fake)
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    identity = _owner(enabled)
    try:
        first_request = _request()
        first_prepared = (
            await _call(
                server.protocol, identity, "event_create_prepare", first_request
            )
        )["structuredContent"]
        first_commit = await _call(
            server.protocol,
            identity,
            "event_create_commit",
            {
                **first_request,
                "preparation_ref": first_prepared["preparation_ref"],
                "action_digest": first_prepared["action_digest"],
            },
        )
        await runtime.wait_for_operation(
            first_commit["structuredContent"]["operation_ref"]
        )

        changed_request = _request(
            raw_text="11 октября в 20:00 состоится уже другое мероприятие.",
        )
        changed_prepared = (
            await _call(
                server.protocol, identity, "event_create_prepare", changed_request
            )
        )["structuredContent"]
        collision = await _call(
            server.protocol,
            identity,
            "event_create_commit",
            {
                **changed_request,
                "preparation_ref": changed_prepared["preparation_ref"],
                "action_digest": changed_prepared["action_digest"],
            },
        )
    finally:
        await runtime.shutdown()
        await database.close()

    assert collision["isError"] is True
    assert collision["structuredContent"]["error_code"] == (
        "EVENT_CREATE_IDEMPOTENCY_CONFLICT"
    )
    assert fake.calls == 1


@pytest.mark.asyncio
async def test_preparation_is_bound_to_actor_and_exact_request(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    enabled = _enabled_config(config, database)
    runtime = EventCreateRuntime(
        config=enabled, database=database, executor=FakeExecutor()
    )
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    owner = _owner(enabled)
    other = _owner(enabled, subject="another-owner")
    request = _request()
    try:
        prepared = (
            await _call(server.protocol, owner, "event_create_prepare", request)
        )["structuredContent"]
        wrong_actor = await _call(
            server.protocol,
            other,
            "event_create_commit",
            {
                **request,
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
        )
        changed = await _call(
            server.protocol,
            owner,
            "event_create_commit",
            {
                **_request(raw_text=request["raw_text"] + " Изменено."),
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
        )
    finally:
        await runtime.shutdown()
        await database.close()

    assert wrong_actor["isError"] is True
    assert wrong_actor["structuredContent"]["error_code"] == (
        "EVENT_CREATE_PREPARATION_INVALID"
    )
    assert changed["isError"] is True
    assert changed["structuredContent"]["error_code"] == (
        "EVENT_CREATE_DIGEST_MISMATCH"
    )


@pytest.mark.asyncio
async def test_database_init_creates_event_operation_ledger_idempotently(
    tmp_path: Path,
) -> None:
    database = Database(str(tmp_path / "migration.sqlite"))
    try:
        await database.init()
        await database.init()
        async with database.raw_conn() as conn:
            columns = {
                row[1]
                for row in await (
                    await conn.execute("PRAGMA table_info('event_change_log')")
                ).fetchall()
            }
            quick_check = (
                await (await conn.execute("PRAGMA quick_check")).fetchone()
            )[0]
            indexes = {
                row[1]
                for row in await (
                    await conn.execute("PRAGMA index_list('event_change_log')")
                ).fetchall()
            }
    finally:
        await database.close()

    assert {
        "operation_ref",
        "operation_kind",
        "actor_subject",
        "actor_client_id",
        "idempotency_hash",
        "action_digest",
        "request_json",
        "status",
        "event_id",
        "result_json",
    }.issubset(columns)
    assert "ix_event_change_log_status_updated" in indexes
    assert "ix_event_change_log_event" in indexes
    assert quick_check == "ok"


@pytest.mark.asyncio
async def test_main_executor_uses_full_smart_update_and_standard_joboutbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "real-executor.sqlite"))
    await database.init()
    future_date = (date.today() + timedelta(days=30)).isoformat()

    async def fake_parse(_text: str, *_args, **_kwargs):
        return [
            {
                "title": "Тестовая лекция MCP",
                "short_description": "Лекция о безопасной архитектуре.",
                "date": future_date,
                "time": "19:00",
                "location_name": "Тестовый зал",
                "location_address": "ул. Тестовая, 1",
                "city": "Калининград",
                "event_type": "лекция",
            }
        ]

    async def fake_topics(_event: Event):
        return ["LECTURES"]

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "classify_event_topics", fake_topics)
    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)

    request = _request()
    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    runtime_request = EventCreateRuntime.request_from_arguments(request, context)
    try:
        result = await MainEventCreateExecutor(database).create(runtime_request)
        async with database.get_session() as session:
            event_count = int(
                (await session.execute(select(func.count(Event.id)))).scalar_one()
            )
            source_count = int(
                (await session.execute(select(func.count(EventSource.id)))).scalar_one()
            )
            jobs = (
                await session.execute(
                    select(JobOutbox).where(JobOutbox.event_id == result["event_ids"][0])
                )
            ).scalars().all()
    finally:
        await database.close()

    assert result["status"] == "accepted"
    assert event_count == 1
    assert source_count == 1
    assert result["candidate_receipts"][0]["outcome"] == "CREATED"
    tasks = {getattr(job.task, "value", str(job.task)) for job in jobs}
    assert {
        "event_media_review",
        "telegraph_build",
        "ics_publish",
        "tg_ics_post",
        "vk_sync",
        "tg_event_publish",
    }.issubset(tasks)


@pytest.mark.asyncio
async def test_mcp_festival_intake_defers_all_legacy_direct_projection_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "festival-executor.sqlite"))
    await database.init()
    future_date = (date.today() + timedelta(days=40)).isoformat()

    async def fake_parse(_text: str, *_args, **_kwargs):
        return [
            {
                "title": "День тестового фестиваля",
                "short_description": "Программа фестиваля.",
                "date": future_date,
                "time": "18:00",
                "location_name": "Тестовый зал",
                "city": "Калининград",
                "event_type": "концерт",
                "festival": "Тестовый фестиваль MCP",
            }
        ]

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("legacy direct projection was called from MCP commit")

    async def fake_topics(_event: Event):
        return []

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "classify_event_topics", fake_topics)
    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setenv("EVENT_UPDATE_SYNC", "1")
    for name in (
        "rebuild_fest_nav_if_changed",
        "sync_festival_page",
        "sync_festivals_index_page",
        "sync_festival_vk_post",
        "try_set_fest_cover_from_program",
        "sync_month_page",
        "sync_weekend_page",
    ):
        monkeypatch.setattr(main, name, forbidden)

    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    runtime_request = EventCreateRuntime.request_from_arguments(_request(), context)
    try:
        result = await MainEventCreateExecutor(database).create(runtime_request)
    finally:
        await database.close()

    assert result["status"] == "accepted"
    assert result["event_ids"]
    assert any(job["task"] == "festival_pages" for job in result["jobs"])


@pytest.mark.asyncio
async def test_multi_event_source_is_rejected_before_any_canonical_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "multi-event.sqlite"))
    await database.init()
    future_date = (date.today() + timedelta(days=30)).isoformat()

    async def fake_parse(_text: str, *_args, **_kwargs):
        return [
            {
                "title": "Событие один",
                "short_description": "Первое событие.",
                "date": future_date,
                "time": "18:00",
                "location_name": "Зал один",
                "city": "Калининград",
            },
            {
                "title": "Событие два",
                "short_description": "Второе событие.",
                "date": future_date,
                "time": "20:00",
                "location_name": "Зал два",
                "city": "Калининград",
            },
        ]

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    request = EventCreateRuntime.request_from_arguments(_request(), context)
    try:
        result = await MainEventCreateExecutor(database).create(request)
        async with database.get_session() as session:
            event_count = int(
                (await session.execute(select(func.count(Event.id)))).scalar_one()
            )
            source_count = int(
                (await session.execute(select(func.count(EventSource.id)))).scalar_one()
            )
            job_count = int(
                (await session.execute(select(func.count(JobOutbox.id)))).scalar_one()
            )
    finally:
        await database.close()

    assert result["status"] == "rejected"
    assert result["error_code"] == "MULTI_EVENT_SOURCE_REQUIRES_SEPARATE_REQUESTS"
    assert (event_count, source_count, job_count) == (0, 0, 0)


@pytest.mark.asyncio
async def test_festival_level_source_does_not_enter_legacy_queue_from_owner_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "festival-source.sqlite"))
    await database.init()
    future_date = (date.today() + timedelta(days=45)).isoformat()

    async def fake_parse(_text: str, *_args, **_kwargs):
        return [
            {
                "title": "Программа фестиваля",
                "short_description": "Общая программа.",
                "date": future_date,
                "time": "12:00",
                "location_name": "Фестивальная площадка",
                "city": "Калининград",
            }
        ]

    def fake_festival_decision(**_kwargs):
        return SimpleNamespace(
            context="festival_post",
            festival="Тестовый фестиваль",
            festival_full="Тестовый фестиваль",
            dedup_links=[],
            signals=["festival_program"],
        )

    async def forbidden_queue(*_args, **_kwargs):
        raise AssertionError("legacy festival queue must not be mutated by R1")

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "detect_festival_context", fake_festival_decision)
    monkeypatch.setattr(main, "enqueue_festival_source", forbidden_queue)
    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    request = EventCreateRuntime.request_from_arguments(
        _request(source_url="https://example.test/festival/program"), context
    )
    try:
        result = await MainEventCreateExecutor(database).create(request)
        async with database.get_session() as session:
            event_count = int(
                (await session.execute(select(func.count(Event.id)))).scalar_one()
            )
            job_count = int(
                (await session.execute(select(func.count(JobOutbox.id)))).scalar_one()
            )
    finally:
        await database.close()

    assert result["status"] == "rejected"
    assert result["error_code"] == "FESTIVAL_SOURCE_REQUIRES_DEDICATED_INTAKE"
    assert (event_count, job_count) == (0, 0)


@pytest.mark.asyncio
async def test_invalid_accepted_executor_result_fails_closed(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    enabled = _enabled_config(config, database)
    fake = FakeExecutor(
        {
            "status": "accepted",
            "event_ids": [],
            "events": [],
            "jobs": [],
        }
    )
    runtime = EventCreateRuntime(config=enabled, database=database, executor=fake)
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    identity = _owner(enabled)
    request = _request()
    try:
        prepared = (
            await _call(server.protocol, identity, "event_create_prepare", request)
        )["structuredContent"]
        committed = await _call(
            server.protocol,
            identity,
            "event_create_commit",
            {
                **request,
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
        )
        operation_ref = committed["structuredContent"]["operation_ref"]
        await runtime.wait_for_operation(operation_ref)
        status = await _call(
            server.protocol,
            identity,
            "event_operation_get",
            {"operation_ref": operation_ref},
        )
    finally:
        await runtime.shutdown()
        await database.close()

    assert status["structuredContent"]["status"] == "failed"
    assert status["structuredContent"]["error_code"] == "EVENT_CREATE_RESULT_INVALID"


@pytest.mark.asyncio
async def test_stale_processing_operation_becomes_outcome_unknown_without_retry(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    enabled = _enabled_config(config, database)
    runtime = EventCreateRuntime(
        config=enabled, database=database, executor=FakeExecutor()
    )
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    identity = _owner(enabled)
    request = EventCreateRuntime.request_from_arguments(
        _request(), SimpleNamespace(identity=identity)
    )
    operation, _created = await runtime.store.reserve(request)
    operation_ref = operation["operation_ref"]
    await runtime.store.mark_processing(operation_ref)
    async with database.raw_conn() as conn:
        await conn.execute(
            "UPDATE event_change_log SET started_at=datetime('now','-2 hours') "
            "WHERE operation_ref=?",
            (operation_ref,),
        )
        await conn.commit()
    try:
        status = await _call(
            server.protocol,
            identity,
            "event_operation_get",
            {"operation_ref": operation_ref},
        )
        async with database.raw_conn() as conn:
            persisted = await (
                await conn.execute(
                    "SELECT status FROM event_change_log WHERE operation_ref=?",
                    (operation_ref,),
                )
            ).fetchone()
    finally:
        await runtime.shutdown()
        await database.close()

    body = status["structuredContent"]
    assert body["status"] == "outcome_unknown"
    assert body["persisted_status"] == "processing"
    assert body["terminal"] is True
    assert body["error_code"] == "EVENT_CREATE_STALE_PROCESSING"
    assert persisted[0] == "processing"  # status read is genuinely read-only
    assert "idempotency_hash" not in body


@pytest.mark.asyncio
async def test_read_only_owner_can_inspect_status_but_cannot_create(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    enabled = _enabled_config(config, database)
    runtime = EventCreateRuntime(
        config=enabled, database=database, executor=FakeExecutor()
    )
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    identity = AccessIdentity(
        subject="operator",
        client_id=enabled.oauth_client_id,
        scopes=frozenset({"operations:read"}),
        audience=enabled.resource,
        token_id="owner-event-status-only-jti",
        expires_at=2_000_000_000,
    )
    try:
        tools = await _list_tools(server.protocol, identity)
    finally:
        await runtime.shutdown()
        await database.close()

    assert "event_operation_get" in tools
    assert "event_create_prepare" not in tools
    assert "event_create_commit" not in tools


@pytest.mark.asyncio
async def test_default_ingestion_keeps_legacy_schedule_call_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "legacy-shape.sqlite"))
    await database.init()
    future_date = (date.today() + timedelta(days=20)).isoformat()
    schedule_calls: list[int] = []

    async def fake_parse(_text: str, *_args, **_kwargs):
        return [
            {
                "title": "Legacy schedule shape",
                "short_description": "Проверка обратной совместимости.",
                "date": future_date,
                "time": "19:00",
                "location_name": "Тестовый зал",
                "city": "Калининград",
            }
        ]

    async def legacy_schedule(
        _db: Database,
        event: Event,
        drain_nav: bool = False,
        skip_vk_sync: bool = False,
        refresh_existing_vk: bool = False,
    ) -> dict[Any, str]:
        assert drain_nav is False
        assert skip_vk_sync is False
        assert refresh_existing_vk is False
        schedule_calls.append(int(event.id or 0))
        return {}

    async def fake_topics(_event: Event):
        return []

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "schedule_event_update_tasks", legacy_schedule)
    monkeypatch.setattr(main, "classify_event_topics", fake_topics)
    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)
    try:
        result = await main.add_events_from_text(
            database,
            "Обычный ручной путь не должен получать новые ключевые аргументы.",
            "https://example.test/source/legacy-shape",
        )
    finally:
        await database.close()

    assert any(isinstance(item[0], Event) for item in result)
    assert schedule_calls


@pytest.mark.asyncio
async def test_full_mcp_prepare_commit_status_uses_real_smart_update_executor(
    config,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "mcp-e2e.sqlite"))
    await database.init()
    enabled = _enabled_config(config, database)
    future_date = (date.today() + timedelta(days=25)).isoformat()

    async def fake_parse(_text: str, *_args, **_kwargs):
        return [
            {
                "title": "MCP end-to-end lecture",
                "short_description": "Проверка полного owner-контура.",
                "date": future_date,
                "time": "19:00",
                "location_name": "Тестовый зал",
                "location_address": "ул. Тестовая, 1",
                "city": "Калининград",
                "event_type": "лекция",
            }
        ]

    async def fake_topics(_event: Event):
        return ["LECTURES"]

    async def forbidden_projection(*_args, **_kwargs):
        raise AssertionError("MCP commit called an external projection directly")

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "classify_event_topics", fake_topics)
    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setenv("EVENT_UPDATE_SYNC", "1")
    for name in (
        "sync_month_page",
        "sync_weekend_page",
        "sync_festival_page",
        "sync_festivals_index_page",
        "sync_festival_vk_post",
        "try_set_fest_cover_from_program",
    ):
        monkeypatch.setattr(main, name, forbidden_projection)

    app = web.Application()
    server = attach_private_events_mcp(app, enabled, event_database=database)
    assert server is not None
    assert server.event_create_runtime is not None
    identity = _owner(enabled)
    request = _request(
        source_external_id="mcp-e2e-source-1",
        idempotency_key="mcp-e2e-create-1",
    )
    try:
        prepared = (
            await _call(server.protocol, identity, "event_create_prepare", request)
        )["structuredContent"]
        committed = await _call(
            server.protocol,
            identity,
            "event_create_commit",
            {
                **request,
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
        )
        operation_ref = committed["structuredContent"]["operation_ref"]
        await server.event_create_runtime.wait_for_operation(operation_ref)
        status = await _call(
            server.protocol,
            identity,
            "event_operation_get",
            {"operation_ref": operation_ref},
        )
        async with database.get_session() as session:
            events = (await session.execute(select(Event))).scalars().all()
            jobs = (await session.execute(select(JobOutbox))).scalars().all()
    finally:
        await server.event_create_runtime.shutdown()
        await database.close()

    operation = status["structuredContent"]
    assert operation["status"] == "accepted"
    assert operation["event_id"] == events[0].id
    assert operation["result"]["event_ids"] == [events[0].id]
    assert operation["result"]["jobs_scope"].startswith("current event JobOutbox")
    tasks = {getattr(job.task, "value", str(job.task)) for job in jobs}
    assert {"telegraph_build", "vk_sync", "tg_event_publish"}.issubset(tasks)


@pytest.mark.asyncio
async def test_queued_operation_resumes_after_runtime_restart_with_same_key(
    config, tmp_path: Path
) -> None:
    database = await _database(tmp_path)
    await _insert_event(database)
    enabled = _enabled_config(config, database)
    identity = _owner(enabled)
    request_args = _request(
        source_external_id="restart-source-1",
        idempotency_key="restart-create-1",
    )
    first_fake = FakeExecutor()
    first_runtime = EventCreateRuntime(
        config=enabled, database=database, executor=first_fake
    )
    request = first_runtime.request_from_arguments(
        request_args, SimpleNamespace(identity=identity)
    )
    prepared = first_runtime.prepare(request)
    operation, created = await first_runtime.store.reserve(request)
    assert created is True
    assert operation["status"] == "queued"
    await first_runtime.shutdown()

    second_fake = FakeExecutor()
    second_runtime = EventCreateRuntime(
        config=enabled, database=database, executor=second_fake
    )
    try:
        resumed = await second_runtime.commit(
            request,
            preparation_ref=prepared["preparation_ref"],
            action_digest=prepared["action_digest"],
        )
        operation_ref = resumed["operation_ref"]
        await second_runtime.wait_for_operation(operation_ref)
        terminal = await second_runtime.store.get(
            operation_ref,
            actor_subject=identity.subject,
            actor_client_id=identity.client_id,
            actor_audience=identity.audience,
        )
    finally:
        await second_runtime.shutdown()
        await database.close()

    assert first_fake.calls == 0
    assert second_fake.calls == 1
    assert terminal["status"] == "accepted"
    assert terminal["event_id"] == 101


@pytest.mark.asyncio
async def test_main_executor_returns_existing_canonical_event_for_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "merge.sqlite"))
    await database.init()
    future_date = (date.today() + timedelta(days=35)).isoformat()
    async with database.get_session() as session:
        existing = Event(
            title="Merge lecture",
            description="Existing description.",
            short_description="Existing description.",
            date=future_date,
            time="19:00",
            location_name="Merge hall",
            city="Калининград",
            source_text="Existing source.",
        )
        session.add(existing)
        await session.commit()
        await session.refresh(existing)
        existing_id = int(existing.id or 0)

    async def fake_parse(_text: str, *_args, **_kwargs):
        return [
            {
                "title": "Merge lecture",
                "short_description": "Additional source facts.",
                "date": future_date,
                "time": "19:00",
                "location_name": "Merge hall",
                "city": "Калининград",
                "event_type": "лекция",
            }
        ]

    async def fake_topics(_event: Event):
        return []

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "classify_event_topics", fake_topics)
    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)
    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    request = EventCreateRuntime.request_from_arguments(
        _request(
            source_external_id="merge-source-2",
            idempotency_key="merge-create-2",
        ),
        context,
    )
    try:
        result = await MainEventCreateExecutor(database).create(request)
        async with database.get_session() as session:
            event_count = int(
                (await session.execute(select(func.count(Event.id)))).scalar_one()
            )
    finally:
        await database.close()

    assert result["status"] == "accepted"
    assert result["event_ids"] == [existing_id]
    assert result["candidate_receipts"][0]["outcome"] == "MERGED"
    assert event_count == 1


@pytest.mark.asyncio
async def test_parser_retry_creates_no_event_or_downstream_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "retry.sqlite"))
    await database.init()

    async def fake_parse(text: str, *_args, **_kwargs):
        return SourceParseDecision.retry(
            SourceParseRetryReason.TECHNICAL_ERROR,
            evidence_manifest=EvidenceManifest.complete_source(text),
        )

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    request = EventCreateRuntime.request_from_arguments(
        _request(
            source_external_id="retry-source-1",
            idempotency_key="retry-create-1",
        ),
        context,
    )
    try:
        result = await MainEventCreateExecutor(database).create(request)
        async with database.get_session() as session:
            event_count = int(
                (await session.execute(select(func.count(Event.id)))).scalar_one()
            )
            source_count = int(
                (await session.execute(select(func.count(EventSource.id)))).scalar_one()
            )
            job_count = int(
                (await session.execute(select(func.count(JobOutbox.id)))).scalar_one()
            )
    finally:
        await database.close()

    assert result["status"] == "failed"
    assert result["error_code"].startswith("SOURCE_PARSE_RETRY")
    assert (event_count, source_count, job_count) == (0, 0, 0)


@pytest.mark.asyncio
async def test_unclassified_executor_error_becomes_outcome_unknown(
    config, tmp_path: Path
) -> None:
    class RaisingExecutor:
        async def create(self, _request):
            raise RuntimeError("failure after an unknown mutation boundary")

    database = await _database(tmp_path)
    enabled = _enabled_config(config, database)
    runtime = EventCreateRuntime(
        config=enabled, database=database, executor=RaisingExecutor()
    )
    server = PrivateEventsMCPServer(enabled, event_create_runtime=runtime)
    identity = _owner(enabled)
    request = _request(
        source_external_id="unknown-outcome-source-1",
        idempotency_key="unknown-outcome-create-1",
    )
    try:
        prepared = (
            await _call(server.protocol, identity, "event_create_prepare", request)
        )["structuredContent"]
        committed = await _call(
            server.protocol,
            identity,
            "event_create_commit",
            {
                **request,
                "preparation_ref": prepared["preparation_ref"],
                "action_digest": prepared["action_digest"],
            },
        )
        operation_ref = committed["structuredContent"]["operation_ref"]
        await runtime.wait_for_operation(operation_ref)
        status = await _call(
            server.protocol,
            identity,
            "event_operation_get",
            {"operation_ref": operation_ref},
        )
    finally:
        await runtime.shutdown()
        await database.close()

    body = status["structuredContent"]
    assert body["status"] == "outcome_unknown"
    assert body["terminal"] is True
    assert body["error_code"] == "EVENT_CREATE_EXECUTION_OUTCOME_UNKNOWN"
    assert "Do not retry" in body["result"]["instruction"]


def test_r1_release_scenario_map_points_to_real_tests() -> None:
    mapping_path = Path(
        "docs/testing/private-events-mcp-event-create-r1-scenarios.v1.yml"
    )
    mapping = yaml.safe_load(mapping_path.read_text(encoding="utf-8"))
    entries = mapping["scenarios"]
    assert set(entries) == {"CRT-001", "CRT-002", "CRT-003", "CRT-004", "CRT-005"}
    module_globals = globals()
    for node_id in entries.values():
        prefix = "tests/test_private_events_mcp_event_create.py::"
        assert node_id.startswith(prefix)
        assert node_id.removeprefix(prefix) in module_globals


@pytest.mark.asyncio
async def test_zero_event_source_is_rejected_before_festival_or_event_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "zero-event.sqlite"))
    await database.init()

    async def fake_parse(_text: str, *_args, **_kwargs):
        return []

    async def forbidden_festival(*_args, **_kwargs):
        raise AssertionError("zero-event owner source reached festival mutation")

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "ensure_festival", forbidden_festival)
    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    request = EventCreateRuntime.request_from_arguments(
        _request(
            source_external_id="zero-event-source-1",
            idempotency_key="zero-event-create-1",
        ),
        context,
    )
    try:
        result = await MainEventCreateExecutor(database).create(request)
        async with database.get_session() as session:
            event_count = int(
                (await session.execute(select(func.count(Event.id)))).scalar_one()
            )
            job_count = int(
                (await session.execute(select(func.count(JobOutbox.id)))).scalar_one()
            )
    finally:
        await database.close()

    assert result["status"] == "rejected"
    assert result["error_code"] == "EVENT_SOURCE_REQUIRES_EXACTLY_ONE_EVENT"
    assert (event_count, job_count) == (0, 0)


@pytest.mark.asyncio
async def test_mixed_lifecycle_source_is_rejected_before_event_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(str(tmp_path / "mixed-lifecycle.sqlite"))
    await database.init()
    future_date = (date.today() + timedelta(days=28)).isoformat()
    raw_text = "Анонс нового события и отмена другого события организатором."
    manifest = EvidenceManifest.complete_source(raw_text)

    async def fake_parse(_text: str, *_args, **_kwargs):
        return SourceParseDecision(
            [
                {
                    "title": "Новое событие",
                    "short_description": "Новый анонс.",
                    "date": future_date,
                    "time": "19:00",
                    "location_name": "Новый зал",
                    "city": "Калининград",
                }
            ],
            disposition=SourceDisposition.MIXED,
            lifecycle_actions=[
                LifecycleAction(
                    action=LifecycleActionType.CANCEL,
                    target_title="Другое событие",
                    evidence="Организатор сообщил об отмене.",
                )
            ],
            evidence_manifest=manifest,
            evidence_complete=True,
        )

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    context = SimpleNamespace(
        identity=SimpleNamespace(
            subject="operator",
            client_id="owner-client-test",
            audience="https://events.example/mcp",
        )
    )
    request = EventCreateRuntime.request_from_arguments(
        _request(
            raw_text=raw_text,
            source_external_id="mixed-lifecycle-source-1",
            idempotency_key="mixed-lifecycle-create-1",
        ),
        context,
    )
    try:
        result = await MainEventCreateExecutor(database).create(request)
        async with database.get_session() as session:
            event_count = int(
                (await session.execute(select(func.count(Event.id)))).scalar_one()
            )
            job_count = int(
                (await session.execute(select(func.count(JobOutbox.id)))).scalar_one()
            )
    finally:
        await database.close()

    assert result["status"] == "rejected"
    assert result["error_code"] == "LIFECYCLE_SOURCE_REQUIRES_EVENT_CHANGE"
    assert (event_count, job_count) == (0, 0)
