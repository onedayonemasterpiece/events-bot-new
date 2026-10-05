from __future__ import annotations

import ast
import json
import sqlite3
import time
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import select, text

from db import Database
from event_operation_receipts import (
    EventOperationReceiptError, guard_event_operation_context,
    record_event_operation_receipt, validate_event_operation_context,
)
from models import Event, EventSource
from private_events_mcp.partner_access import PartnerAccessStore
from smart_event_update import EventCandidate
from smart_update_identity import input_packet_fingerprint, stable_candidate_identity
from smart_update_state import begin_candidate_attempt


def context():
    return {'operation_ref': 'evt_op_' + 'a' * 24, 'action_digest': 'b' * 64,
            'actor_subject': 'owner', 'actor_client_id': 'owner-client', 'actor_audience': 'owner-resource'}


@pytest_asyncio.fixture
async def db(tmp_path, monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    database = Database(str(tmp_path / 'canonical.sqlite'))
    await database.init()
    yield database
    await database.close()


async def prepare(db, ctx=None):
    ctx = ctx or context()
    candidate = EventCandidate(source_type='manual', source_url='https://events.test/42',
        source_text='Source content', title='Test', date='2026-12-20', time='19:00',
        location_name='Test Hall', event_operation_context=dict(ctx))
    candidate.candidate_key, candidate.occurrence_key = stable_candidate_identity(candidate)
    candidate.source_fingerprint = input_packet_fingerprint(candidate)
    attempt = await begin_candidate_attempt(db, candidate_key=candidate.candidate_key,
        occurrence_key=candidate.occurrence_key, canonical_source_url=candidate.source_url,
        source_type='manual', intent='UPSERT_EVENT', source_fingerprint=candidate.source_fingerprint,
        candidate_payload=asdict(candidate), lease_owner='test-receipt')
    candidate.smart_update_candidate_id = attempt.candidate_state_id
    candidate.smart_update_attempt_no = attempt.attempt_no
    stored_request = {'partner_policy_revision': ctx.get('partner_policy_revision')}
    async with db.raw_conn() as conn:
        await conn.execute('INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,request_json,status,organizer_comment,result_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (ctx['operation_ref'], 'create', ctx['actor_subject'], ctx['actor_client_id'], ctx['actor_audience'],
             'idempotency', ctx['action_digest'], 'manual', candidate.source_url, json.dumps(stored_request),
             'processing', 'owner-review-preserved', '{"status":"processing"}'))
        await conn.commit()
    return candidate


def event(event_id=42):
    return Event(id=event_id, title='Canonical original', description='Original description', source_text='Original source', date='2026-12-20', time='19:00', location_name='Hall')


async def receipt(db):
    async with db.raw_conn() as conn:
        row = await (await conn.execute('SELECT domain_receipt_json FROM event_change_log')).fetchone()
        return json.loads(row[0]) if row and row[0] else None


@pytest.mark.asyncio
async def test_receipt_and_event_commit_together_and_original_audit_untouched(db):
    candidate = await prepare(db)
    async with db.get_session() as session:
        session.add(event())
        proof = await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
        assert await receipt(db) is None  # uncommitted proof is invisible
        await session.commit()
    assert await receipt(db) == proof
    assert proof['effect'] == 'created' and proof['attempt_no'] == 1
    async with db.raw_conn() as conn:
        row = await (await conn.execute('SELECT status,organizer_comment,result_json FROM event_change_log')).fetchone()
        assert row == ('processing', 'owner-review-preserved', '{"status":"processing"}')
    async with db.get_session() as session:
        await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
        await session.commit()
    assert await receipt(db) == proof


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['actor', 'digest', 'state', 'context', 'attempt'])
async def test_bad_correlation_rolls_back_event_even_if_caller_catches(db, change):
    candidate = await prepare(db)
    if change == 'actor':
        candidate.event_operation_context['actor_subject'] = 'foreign'
    elif change == 'digest':
        candidate.event_operation_context['action_digest'] = 'c' * 64
    elif change == 'context':
        candidate.event_operation_context['unexpected'] = 'not allowed'
    elif change == 'attempt':
        candidate.smart_update_attempt_no = 99
    else:
        async with db.raw_conn() as conn:
            await conn.execute("UPDATE event_change_log SET status='outcome_unknown'")
            await conn.commit()
    async with db.get_session() as session:
        session.add(event())
        with pytest.raises(EventOperationReceiptError):
            await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
        await session.commit()
    async with db.get_session() as session:
        assert await session.get(Event, 42) is None
    assert await receipt(db) is None


@pytest.mark.asyncio
async def test_receipt_conflict_rolls_back_merge_event_and_source(db):
    candidate = await prepare(db)
    async with db.get_session() as session:
        session.add(event())
        await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
        await session.commit()
    original = await receipt(db)
    async with db.get_session() as session:
        existing = await session.get(Event, 42)
        existing.title = 'Must roll back'
        session.add(EventSource(event_id=42, source_type='manual', source_url='https://events.test/new'))
        with pytest.raises(EventOperationReceiptError):
            await record_event_operation_receipt(session, candidate, event_id=42, effect='merged')
        await session.commit()
    async with db.get_session() as session:
        assert (await session.get(Event, 42)).title == 'Canonical original'
        assert (await session.execute(select(EventSource))).scalars().all() == []
    assert await receipt(db) == original


@pytest.mark.asyncio
async def test_no_context_is_strict_noop_even_without_session():
    assert await record_event_operation_receipt(None, SimpleNamespace(), event_id=-1, effect='bad') is None
    assert await guard_event_operation_context(None, SimpleNamespace(), event_id=None, effect='bad') is None


@pytest.mark.asyncio
async def test_exact_noop_requires_same_packet_binding_not_url(db):
    candidate = await prepare(db)
    async with db.get_session() as session:
        session.add(event())
        session.add(EventSource(event_id=42, source_type='manual', source_url=candidate.source_url,
            candidate_key=candidate.candidate_key, occurrence_key=candidate.occurrence_key,
            source_fingerprint='0' * 64))
        await session.commit()
    async with db.get_session() as session:
        with pytest.raises(EventOperationReceiptError):
            await record_event_operation_receipt(session, candidate, event_id=42, effect='noop_exact_replay')
    assert await receipt(db) is None
    async with db.get_session() as session:
        source = (await session.execute(select(EventSource))).scalars().one()
        source.source_fingerprint = candidate.source_fingerprint
        await record_event_operation_receipt(session, candidate, event_id=42, effect='noop_exact_replay')
        await session.commit()
    assert (await receipt(db))['effect'] == 'noop_exact_replay'


@pytest.mark.asyncio
async def test_exception_after_receipt_before_commit_rolls_back_both(db):
    candidate = await prepare(db)
    with pytest.raises(RuntimeError):
        async with db.get_session() as session:
            session.add(event())
            await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
            raise RuntimeError('simulated pre-commit crash')
    async with db.get_session() as session:
        assert await session.get(Event, 42) is None
    assert await receipt(db) is None


def test_context_excluded_from_identity_fingerprint_and_candidate_repr():
    candidate = EventCandidate(source_type='manual', source_url='https://events.test/42', source_text='Only source text',
                               title='Title', date='2026-12-20', time='19:00', location_name='Hall')
    original = input_packet_fingerprint(candidate), stable_candidate_identity(candidate)
    candidate.event_operation_context = context()
    assert (input_packet_fingerprint(candidate), stable_candidate_identity(candidate)) == original
    assert context()['operation_ref'] not in repr(candidate)
    assert asdict(candidate)['event_operation_context'] == context()  # private attempt payload only
    tree = ast.parse((Path(__file__).parents[1] / 'smart_event_update.py').read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == 'asdict']
    # Whole candidate serialization is exclusively persisted in begin_candidate_attempt.
    assert len(calls) == 1
    assert calls[0].lineno > 17000


async def partner_context(db):
    policy = PartnerAccessStore(db.path, resource='partner-resource', signing_key='local-key')
    created = policy.create(tenant_id='tenant', organization_id='org', display_name='Partner',
        policy={'scopes': ['partner:events:propose'], 'actions': ['event_create']},
        redirect_uris=['http://127.0.0.1:8421/callback'], expires_at=int(time.time()) + 3600)
    grant = policy.get(created['principal_id'])
    return policy, grant, {**context(), 'actor_subject': grant.subject,
                          'actor_client_id': grant.client_id, 'actor_audience': policy.resource,
                          'partner_policy_revision': grant.policy_revision}


@pytest.mark.asyncio
@pytest.mark.parametrize('effect', ['merged', 'noop_exact_replay'])
async def test_partner_foreign_merge_never_commits_event_or_source(db, effect):
    _, grant, ctx = await partner_context(db)
    candidate = await prepare(db, ctx)
    async with db.get_session() as session:
        session.add(event())
        await session.commit()
    async with db.get_session() as session:
        existing = await session.get(Event, 42)
        existing.title = 'Foreign modification must roll back'
        session.add(EventSource(event_id=42, source_type='manual', source_url=candidate.source_url))
        with pytest.raises(EventOperationReceiptError):
            await record_event_operation_receipt(session, candidate, event_id=42, effect=effect)
        await session.commit()
    async with db.get_session() as session:
        assert (await session.get(Event, 42)).title == 'Canonical original'
        assert (await session.execute(select(EventSource))).scalars().all() == []
    assert await receipt(db) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['suspend', 'rotate', 'policy'])
async def test_partner_current_policy_revision_and_epoch_guard(db, change):
    policy, grant, ctx = await partner_context(db)
    candidate = await prepare(db, ctx)
    kwargs = {'policy': {'scopes': ['partner:events:propose'], 'actions': ['event_create']}} if change == 'policy' else {}
    policy.change(grant.principal_id, action=change, expected_revision=1, **kwargs)
    async with db.get_session() as session:
        session.add(event())
        with pytest.raises(EventOperationReceiptError):
            await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
        await session.commit()
    async with db.get_session() as session:
        assert await session.get(Event, 42) is None
    assert await receipt(db) is None


@pytest.mark.asyncio
async def test_partner_created_and_exact_owned_merge_use_same_connection(db, monkeypatch):
    policy, grant, ctx = await partner_context(db)
    candidate = await prepare(db, ctx)
    original_connect = policy._connect

    def forbidden_connection(*args, **kwargs):
        raise AssertionError('policy must use the canonical mutation connection')

    monkeypatch.setattr(PartnerAccessStore, '_connect', forbidden_connection)
    async with db.get_session() as session:
        session.add(event())
        proof = await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
        await session.commit()
    assert proof['partner_policy_revision'] == 1
    with original_connect() as conn:
        conn.execute('INSERT INTO mcp_partner_event VALUES(?,?,?,?,?)',
                     (grant.principal_id, grant.tenant_id, grant.organization_id, 42, int(time.time())))
    async with db.get_session() as session:
        await guard_event_operation_context(session, candidate, event_id=42, effect='merged', lock=True)
        await session.rollback()


@pytest.mark.asyncio
async def test_database_upgrade_adds_nullable_receipt_and_repeated_init_preserves_old_rows(tmp_path, monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    path = tmp_path / 'old.sqlite'
    # Actual old CREATE statement: the new column is additive via _add_column.
    source = (Path(__file__).parents[1] / 'db.py').read_text()
    start = source.index('CREATE TABLE IF NOT EXISTS event_change_log(')
    end = source.index('\n                )', start) + len('\n                )')
    with sqlite3.connect(path) as conn:
        conn.execute(source[start:end])
        conn.execute("INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,request_json,status) VALUES('old','create','owner','client','resource','key','digest','manual','source','{}','queued')")
    database = Database(str(path))
    try:
        await database.init()
        await database.init()
        async with database.raw_conn() as conn:
            row = await (await conn.execute("SELECT status,domain_receipt_json FROM event_change_log WHERE operation_ref='old'")).fetchone()
            assert row == ('queued', None)
            columns = await (await conn.execute('PRAGMA table_info(event_change_log)')).fetchall()
            assert sum(row[1] == 'domain_receipt_json' for row in columns) == 1
    finally:
        await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['exact_noop', 'attach_context', 'final_probe'])
async def test_alternate_accepted_paths_reject_foreign_partner_before_mutation(db, monkeypatch, path):
    import smart_event_update as su

    _, _, ctx = await partner_context(db)
    candidate = await prepare(db, ctx)
    async with db.get_session() as session:
        session.add(event())
        session.add(EventSource(event_id=42, source_type='manual', source_url=candidate.source_url,
            candidate_key=candidate.candidate_key, occurrence_key=candidate.occurrence_key,
            source_fingerprint=candidate.source_fingerprint, source_text='Original source'))
        await session.commit()
    if path == 'exact_noop':
        with pytest.raises(EventOperationReceiptError):
            await su._smart_event_update_impl(db, candidate, schedule_tasks=False)
    elif path == 'attach_context':
        candidate.target_event_id = 42
        with pytest.raises(EventOperationReceiptError):
            await su._attach_context_source(db, candidate)
    else:
        monkeypatch.setattr(su, '_event_blocked_by_explicit_occurrence', lambda *args: False)
        monkeypatch.setattr(su, '_pre_create_duplicate_probe', lambda *args: event())
        async with db.get_session() as session:
            existing = await session.get(Event, 42)
            with pytest.raises(EventOperationReceiptError):
                await su._accept_final_probe_match(db, session, event=existing, candidate=candidate,
                    schedule_tasks=False, schedule_kwargs=None, enqueue_ticket_sites=None)
    async with db.get_session() as session:
        assert (await session.get(Event, 42)).title == 'Canonical original'
        source = (await session.execute(select(EventSource))).scalars().one()
        assert source.source_text == 'Original source'
    assert await receipt(db) is None


@pytest.mark.asyncio
async def test_owner_exact_noop_real_path_records_without_parser(db):
    import smart_event_update as su

    candidate = await prepare(db)
    async with db.get_session() as session:
        session.add(event())
        session.add(EventSource(event_id=42, source_type='manual', source_url=candidate.source_url,
            candidate_key=candidate.candidate_key, occurrence_key=candidate.occurrence_key,
            source_fingerprint=candidate.source_fingerprint, source_text='Original source'))
        await session.commit()
    result = await su._smart_event_update_impl(db, candidate, schedule_tasks=False)
    assert result.event_id == 42
    assert (await receipt(db))['effect'] == 'noop_exact_replay'


def test_all_accepted_domain_commit_paths_have_receipt_call():
    module = ast.parse((Path(__file__).parents[1] / 'smart_event_update.py').read_text())
    functions = {node.name: node for node in ast.walk(module) if isinstance(node, ast.AsyncFunctionDef)}
    for name in ('_attach_context_source', '_accept_final_probe_match', '_create_from_prepared_candidate', '_smart_event_update_impl'):
        function = functions[name]
        calls = [node for node in ast.walk(function) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == 'record_event_operation_receipt']
        assert calls, name
    source = ast.unparse(functions['_create_from_prepared_candidate'])
    assert source.index("effect='created'") < source.index('await session.commit()')


def disable_models_and_observers(monkeypatch):
    import smart_event_update as su

    monkeypatch.setenv('SMART_UPDATE_SKIP_PAST_EVENTS', '0')
    monkeypatch.setattr(su, 'SMART_UPDATE_LLM_DISABLED', True)
    monkeypatch.setattr(su, 'SMART_UPDATE_IDENTITY_GATE_MODE', su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, 'SMART_UPDATE_MERGE_IDENTITY_GATE_MODE', su.IdentityGateMode.OFF)

    async def no_observer(*args, **kwargs):
        return False

    monkeypatch.setattr(su, '_classify_topics', no_observer)
    monkeypatch.setattr(su, '_apply_holiday_festival_mapping', no_observer)
    monkeypatch.setattr(su, '_ensure_transport_duration_forecast', no_observer)


async def update_test_packet(db, candidate):
    candidate.title = 'Концерт'
    candidate.location_name = 'Дом искусств'
    candidate.city = 'Калининград'
    candidate.event_type = 'концерт'
    candidate.source_text = 'Концерт 20 декабря в 19:00 в Доме искусств. Вечер живой музыки.'
    candidate.source_fingerprint = input_packet_fingerprint(candidate)
    async with db.raw_conn() as conn:
        await conn.execute('UPDATE smart_update_candidate_state SET source_fingerprint=?,candidate_payload=? WHERE id=?',
            (candidate.source_fingerprint, json.dumps(asdict(candidate), default=str), candidate.smart_update_candidate_id))
        await conn.commit()


@pytest.mark.asyncio
async def test_real_normal_create_has_receipt_at_first_domain_commit(db, monkeypatch):
    import smart_event_update as su

    disable_models_and_observers(monkeypatch)
    candidate = await prepare(db)
    await update_test_packet(db, candidate)
    result = await su._smart_event_update_impl(db, candidate, schedule_tasks=False)
    assert result.status == 'created'
    proof = await receipt(db)
    assert proof['effect'] == 'created' and proof['event_id'] == result.event_id
    async with db.get_session() as session:
        source = (await session.execute(select(EventSource))).scalars().one()
        assert source.event_id == proof['event_id']
        assert source.source_fingerprint == proof['source_fingerprint']
        assert context()['operation_ref'] not in source.source_text


@pytest.mark.asyncio
async def test_real_normal_foreign_merge_rolls_back_all_domain_changes(db, monkeypatch):
    import smart_event_update as su

    disable_models_and_observers(monkeypatch)
    _, _, ctx = await partner_context(db)
    candidate = await prepare(db, ctx)
    await update_test_packet(db, candidate)
    # Existing canonical event belongs to no partner portfolio. The same source
    # occurrence anchors a merge, but must not grant rights to this principal.
    existing = event()
    existing.title = candidate.title
    existing.location_name = candidate.location_name
    existing.city = candidate.city
    existing.description = 'Original description'
    async with db.get_session() as session:
        session.add(existing)
        session.add(EventSource(event_id=42, source_type='manual', source_url=candidate.source_url,
            canonical_source_url=candidate.source_url, source_role='identity_bearing',
            candidate_key=candidate.candidate_key, occurrence_key=candidate.occurrence_key,
            source_fingerprint='0' * 64, source_text='Original source'))
        await session.commit()
    with pytest.raises(EventOperationReceiptError):
        await su._smart_event_update_impl(db, candidate, schedule_tasks=False)
    async with db.get_session() as session:
        assert (await session.get(Event, 42)).description == 'Original description'
        source = (await session.execute(select(EventSource))).scalars().one()
        assert source.source_text == 'Original source' and source.source_fingerprint == '0' * 64
    assert await receipt(db) is None


@pytest.mark.asyncio
async def test_parser_rejects_unbounded_context_before_any_work():
    import main

    with pytest.raises(ValueError, match='requires_single_event'):
        await main.add_events_from_text(None, 'source text', None, event_operation_context=context())
    with pytest.raises(EventOperationReceiptError):
        await main.add_events_from_text(None, 'source text', None, require_single_event=True,
                                       event_operation_context={'operation_ref': 'invalid'})


@pytest.mark.asyncio
async def test_partner_cannot_replace_stored_policy_revision(db):
    _, _, ctx = await partner_context(db)
    candidate = await prepare(db, ctx)
    candidate.event_operation_context['partner_policy_revision'] = 2
    async with db.get_session() as session:
        session.add(event())
        with pytest.raises(EventOperationReceiptError):
            await record_event_operation_receipt(session, candidate, event_id=42, effect='created')
        await session.commit()
    async with db.get_session() as session:
        assert await session.get(Event, 42) is None
    assert await receipt(db) is None


@pytest.mark.asyncio
async def test_revoke_after_early_guard_cannot_cross_final_commit(db):
    import asyncio
    from sqlalchemy.exc import OperationalError

    policy, grant, ctx = await partner_context(db)
    candidate = await prepare(db, ctx)
    async with db.get_session() as session:
        session.add(event())
        await session.commit()
    with policy._connect() as conn:
        conn.execute('INSERT INTO mcp_partner_event VALUES(?,?,?,?,?)',
                     (grant.principal_id, grant.tenant_id, grant.organization_id, 42, int(time.time())))
    async with db.get_session() as session:
        await guard_event_operation_context(session, candidate, event_id=42, effect='merged')
        existing = await session.get(Event, 42)
        await asyncio.to_thread(policy.change, grant.principal_id, action='suspend', expected_revision=1)
        existing.title = 'Must not survive concurrent revocation'
        with pytest.raises((EventOperationReceiptError, OperationalError)):
            await record_event_operation_receipt(session, candidate, event_id=42, effect='merged')
        await session.commit()
    async with db.get_session() as session:
        assert (await session.get(Event, 42)).title == 'Canonical original'
    assert await receipt(db) is None
