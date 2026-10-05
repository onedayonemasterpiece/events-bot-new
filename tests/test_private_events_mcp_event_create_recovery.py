"""Durable runtime checks with real SQLite, without parser/provider effects."""
import asyncio
import re
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import aiosqlite
import pytest

from private_events_mcp.event_create import EventCreateRequest, EventCreateRuntime


class Database:
    def __init__(self, path):
        self.path = path

    @asynccontextmanager
    async def raw_conn(self):
        async with aiosqlite.connect(self.path) as conn:
            yield conn


class Executor:
    def __init__(self):
        self.requests = []

    async def create(self, request):
        self.requests.append(request)
        return {"status": "accepted", "event_ids": [123]}


async def allowed(request):
    return True


async def setup(tmp_path):
    database = Database(tmp_path / 'ledger.sqlite')
    schema = re.search(
        r'CREATE TABLE IF NOT EXISTS event_change_log\(.*?\n                \)',
        (Path(__file__).parents[1] / 'db.py').read_text(), re.S,
    ).group()
    async with database.raw_conn() as conn:
        await conn.execute(schema)
    executor = Executor()
    runtime = EventCreateRuntime(config=SimpleNamespace(), database=database, executor=executor)
    request = EventCreateRequest(
        raw_text='An exact event text, unchanged on restart.', source_url=None,
        source_external_id='test-42', source_locator='mcp-owner:original-source',
        idempotency_key='original-key-42', text_policy='smart_rewrite',
        actor_subject='partner:actual-principal', actor_client_id='actual-client',
        actor_audience='actual-audience',
    )
    return database, runtime, executor, request


@pytest.mark.asyncio
async def test_recovery_restores_persisted_actor_payload_and_idempotency(tmp_path):
    database, old, executor, request = await setup(tmp_path)
    operation, _ = await old.store.reserve(request)
    runtime = EventCreateRuntime(config=SimpleNamespace(), database=database, executor=executor)
    assert await runtime.recover_queued(authorize=allowed) == 1
    await runtime.wait_for_operation(operation['operation_ref'])
    restored = executor.requests[0]
    assert restored.canonical_action() == request.canonical_action()
    assert (restored.actor_subject, restored.actor_client_id, restored.actor_audience) == (
        request.actor_subject, request.actor_client_id, request.actor_audience)
    assert restored.idempotency_key == ''  # raw key is never persisted or invented
    assert restored.idempotency_hash == request.idempotency_hash
    same, created = await runtime.store.reserve(request)
    assert not created and same['status'] == 'accepted'
    assert await runtime.recover_queued(authorize=allowed) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['processing', 'outcome_unknown', 'accepted', 'failed', 'rejected'])
async def test_never_replay_claimed_or_terminal(tmp_path, status):
    database, runtime, executor, request = await setup(tmp_path)
    operation, _ = await runtime.store.reserve(request)
    async with database.raw_conn() as conn:
        await conn.execute('UPDATE event_change_log SET status=?', (status,))
        await conn.commit()
    assert await runtime.recover_queued(authorize=allowed) == 0
    assert not executor.requests


@pytest.mark.asyncio
async def test_competing_recovery_workers_execute_once(tmp_path):
    database, first, executor, request = await setup(tmp_path)
    operation, _ = await first.store.reserve(request)
    second = EventCreateRuntime(config=SimpleNamespace(), database=database, executor=executor)
    await asyncio.gather(first.recover_queued(authorize=allowed), second.recover_queued(authorize=allowed))
    await asyncio.gather(first.wait_for_operation(operation['operation_ref']), second.wait_for_operation(operation['operation_ref']))
    assert len(executor.requests) == 1


@pytest.mark.asyncio
async def test_policy_rechecked_at_execution_boundary(tmp_path):
    database, runtime, executor, request = await setup(tmp_path)
    operation, _ = await runtime.store.reserve(request)
    policy = {'active': True}

    async def authorize(restored):
        assert restored.actor_subject == request.actor_subject
        return policy['active']

    await runtime.recover_queued(authorize=authorize)
    policy['active'] = False
    await runtime.wait_for_operation(operation['operation_ref'])
    assert not executor.requests
    same, _ = await runtime.store.reserve(request)
    assert same['status'] == 'rejected'
    assert same['error_code'] == 'EVENT_CREATE_ACCESS_REVOKED'


@pytest.mark.asyncio
async def test_corrupt_payload_quarantined_without_execution(tmp_path):
    database, runtime, executor, request = await setup(tmp_path)
    await runtime.store.reserve(request)
    async with database.raw_conn() as conn:
        await conn.execute("UPDATE event_change_log SET request_json='{}'")
        await conn.commit()
    assert await runtime.recover_queued(authorize=allowed) == 0
    same, _ = await runtime.store.reserve(request)
    assert same['error_code'] == 'EVENT_CREATE_RECOVERY_REQUEST_INVALID'
    assert not executor.requests


@pytest.mark.asyncio
async def test_recovery_limit_is_bounded(tmp_path):
    _, runtime, _, _ = await setup(tmp_path)
    for limit in (0, -1, 1001, True, '10'):
        with pytest.raises(ValueError):
            await runtime.recover_queued(authorize=allowed, limit=limit)


@pytest.mark.asyncio
async def test_recovery_requires_callback_and_policy_errors_fail_closed(tmp_path):
    _, runtime, executor, request = await setup(tmp_path)
    operation, _ = await runtime.store.reserve(request)
    with pytest.raises(TypeError):
        await runtime.recover_queued(authorize=None)

    async def unavailable(restored):
        raise RuntimeError('policy unavailable')

    await runtime.recover_queued(authorize=unavailable)
    await runtime.wait_for_operation(operation['operation_ref'])
    same, _ = await runtime.store.reserve(request)
    assert same['status'] == 'outcome_unknown'
    assert not executor.requests
    assert await runtime.recover_queued(authorize=allowed) == 0


@pytest.mark.asyncio
async def test_normal_commit_worker_also_uses_current_runtime_policy(tmp_path):
    database, runtime, executor, request = await setup(tmp_path)
    async def denied(_):
        return False
    runtime.authorize = denied
    operation, _ = await runtime.store.reserve(request)
    runtime._spawn(operation['operation_ref'], request)
    await runtime.wait_for_operation(operation['operation_ref'])
    assert executor.requests == []
    result = await runtime.store.get(operation['operation_ref'], actor_subject=request.actor_subject,
                                    actor_client_id=request.actor_client_id, actor_audience=request.actor_audience)
    assert result['status'] == 'rejected'
    assert result['error_code'] == 'EVENT_CREATE_ACCESS_REVOKED'


@pytest.mark.asyncio
async def test_existing_app_recovery_hook_is_bounded_and_current_owner_only(tmp_path):
    from dataclasses import replace
    from aiohttp import web
    from private_events_mcp.integration import event_create_actor_is_current, recover_private_event_creates
    from private_events_mcp.server import SERVER_APP_KEY

    database, runtime, executor, original = await setup(tmp_path)
    config = SimpleNamespace(enabled=True, event_create_enabled=True, resource='owner-resource',
                             oauth_client_id='owner-client', opencode_oauth_client_id='opencode-client')
    request = replace(original, actor_subject='events-bot-owner', actor_client_id='owner-client',
                      actor_audience='owner-resource')
    assert event_create_actor_is_current(config, request)
    for changed in (replace(request, actor_subject='partner:actual-principal'),
                    replace(request, actor_client_id='codex-client'),
                    replace(request, actor_audience='partner-resource')):
        assert not event_create_actor_is_current(config, changed)
    async def current(req):
        return event_create_actor_is_current(config, req)
    runtime.authorize = current
    app = web.Application()
    assert await recover_private_event_creates(app) == 0
    app[SERVER_APP_KEY] = SimpleNamespace(config=config, event_create_runtime=runtime)
    operation, _ = await runtime.store.reserve(request)
    # A disable leaves an unclaimed operation queued, not executed or destroyed.
    config.event_create_enabled = False
    assert await recover_private_event_creates(app) == 0
    config.event_create_enabled = True
    assert await recover_private_event_creates(app) == 1
    # Owner client removed after scan but before worker mutation boundary.
    config.oauth_client_id = 'rotated-client'
    await runtime.wait_for_operation(operation['operation_ref'])
    assert not executor.requests
