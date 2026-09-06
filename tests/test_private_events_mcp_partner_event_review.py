import asyncio
from dataclasses import replace

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.partner_event_review import PartnerEventReviewService
from private_events_mcp.tool_catalog import ToolCallContext, ToolExecutionError
from test_private_events_mcp_event_create_recovery import setup, allowed


def owner(subject='owner'):
    identity = AccessIdentity(subject, 'owner-client', frozenset({'events:write'}),
                              'https://events.test/owner', 'token', 5000)
    return ToolCallContext(identity, identity.audience)


async def decision_allowed(context, target, decision):
    return context.identity.subject == 'owner'


def service(runtime, **kwargs):
    return PartnerEventReviewService(store=runtime.store, authorize_submission=allowed,
                                    authorize_decision=kwargs.pop('authorize_decision', decision_allowed), **kwargs)


async def decide(svc, op, req, decision='approve', ctx=None):
    return await svc.decide(op['operation_ref'], expected_action_digest=req.action_digest,
                            decision=decision, owner_context=ctx or owner())


@pytest.mark.asyncio
async def test_review_is_durable_and_never_enters_worker_before_approval(tmp_path):
    db, runtime, executor, req = await setup(tmp_path)
    submitted = await service(runtime).submit(req)
    op = submitted['operation']
    assert op['status'] == 'review_required' and not op['terminal']
    assert await runtime.recover_queued(authorize=allowed) == 0
    assert not executor.requests
    resumed = service(runtime)
    approved = await decide(resumed, op, req)
    assert approved['changed'] and approved['operation']['status'] == 'queued'
    assert not executor.requests  # decision never spawns execution
    assert approved['review']['reviewed_by']['subject'] == 'owner'
    assert approved['operation']['actor_subject'] == req.actor_subject
    await runtime.recover_queued(authorize=allowed)
    await runtime.wait_for_operation(op['operation_ref'])
    replay = await decide(service(runtime), op, req)
    assert not replay['changed'] and replay['operation']['status'] == 'accepted'
    assert replay['review'] == approved['review']  # finish did not erase audit


@pytest.mark.asyncio
async def test_duplicate_submission_and_rejection_are_idempotent(tmp_path):
    _, runtime, executor, req = await setup(tmp_path)
    svc = service(runtime)
    one = await svc.submit(req)
    two = await svc.submit(req)
    assert not two['created'] and one['operation']['operation_ref'] == two['operation']['operation_ref']
    rejected = await decide(svc, one['operation'], req, 'reject')
    assert rejected['operation']['status'] == 'rejected'
    assert rejected['operation']['terminal']
    assert not (await decide(svc, one['operation'], req, 'reject'))['changed']
    with pytest.raises(ToolExecutionError):
        await decide(svc, one['operation'], req)
    assert await runtime.recover_queued(authorize=allowed) == 0
    assert not executor.requests


@pytest.mark.asyncio
async def test_digest_conflict_does_not_queue_or_rewrite(tmp_path):
    _, runtime, _, req = await setup(tmp_path)
    svc = service(runtime)
    op = (await svc.submit(req))['operation']
    with pytest.raises(ToolExecutionError):
        await decide(svc, op, replace(req, raw_text='Changed request content'))
    with pytest.raises(ToolExecutionError):
        await svc.submit(replace(req, raw_text='Changed request content'))
    row, _ = await runtime.store.reserve(req)
    assert row['status'] == 'review_required'


@pytest.mark.asyncio
async def test_competing_owner_decisions_have_single_winner(tmp_path):
    _, runtime, _, req = await setup(tmp_path)
    first, second = service(runtime), service(runtime)
    op = (await first.submit(req))['operation']
    results = await asyncio.gather(decide(first, op, req), decide(second, op, req, 'reject'),
                                   return_exceptions=True)
    assert sum(isinstance(r, ToolExecutionError) for r in results) == 1
    assert sum(isinstance(r, dict) and r['changed'] for r in results) == 1


@pytest.mark.asyncio
async def test_revoked_partner_blocks_approval_other_tenant_continues(tmp_path):
    _, runtime, _, req = await setup(tmp_path)
    revoked = set()

    async def policy(ctx, target, decision):
        return ctx.identity.subject == 'owner' and target.actor_subject not in revoked

    svc = service(runtime, authorize_decision=policy)
    other = replace(req, actor_subject='partner:two', actor_client_id='other-client')
    first = (await svc.submit(req))['operation']
    second = (await svc.submit(other))['operation']
    revoked.add(req.actor_subject)
    with pytest.raises(ToolExecutionError):
        await decide(svc, first, req)
    assert (await decide(svc, second, other))['operation']['status'] == 'queued'
    with pytest.raises(ToolExecutionError):
        await runtime.store.get(first['operation_ref'], actor_subject=other.actor_subject,
                                actor_client_id=other.actor_client_id, actor_audience=other.actor_audience)
    with pytest.raises(ToolExecutionError):
        await decide(svc, first, req, ctx=owner('partner:two'))
    first_row, _ = await runtime.store.reserve(req)
    assert first_row['status'] == 'review_required'


@pytest.mark.asyncio
async def test_failed_policy_rolls_back_and_audit_remains_absent(tmp_path):
    db, runtime, _, req = await setup(tmp_path)

    async def unavailable(*args):
        raise RuntimeError('private detail')

    svc = service(runtime, authorize_decision=unavailable)
    op = (await svc.submit(req))['operation']
    with pytest.raises(ToolExecutionError) as error:
        await decide(svc, op, req)
    assert 'private detail' not in str(error.value)
    async with db.raw_conn() as conn:
        cursor = await conn.execute('SELECT status,organizer_comment FROM event_change_log')
        assert await cursor.fetchone() == ('review_required', None)
    assert (await decide(service(runtime), op, req))['changed']


@pytest.mark.asyncio
async def test_approved_operation_worker_rechecks_revocation(tmp_path):
    _, runtime, executor, req = await setup(tmp_path)
    svc = service(runtime)
    op = (await svc.submit(req))['operation']
    await decide(svc, op, req)

    async def revoked(request):
        return False

    await runtime.recover_queued(authorize=revoked)
    await runtime.wait_for_operation(op['operation_ref'])
    row, _ = await runtime.store.reserve(req)
    assert row['status'] == 'rejected' and not executor.requests
    replay = await decide(svc, op, req)
    assert not replay['changed'] and replay['operation']['status'] == 'rejected'


@pytest.mark.asyncio
@pytest.mark.parametrize('revocation', ['expire', 'suspend', 'scope'])
async def test_real_partner_policy_rechecked_under_review_lock(tmp_path, monkeypatch, revocation):
    import sqlite3
    import private_events_mcp.partner_access as access
    from test_private_events_mcp_partner_access import create, identity, RESOURCE

    db, runtime, _, base = await setup(tmp_path)
    with sqlite3.connect(db.path) as conn:
        conn.executescript(access.SCHEMA)
        conn.execute('CREATE TABLE event(id INTEGER PRIMARY KEY)')
    store = access.PartnerAccessStore(db.path, resource=RESOURCE, signing_key='local-policy-key')
    created = create(store)
    grant = store.get(created['principal_id'])
    who = identity(grant)
    req = replace(base, actor_subject=who.subject, actor_client_id=who.client_id,
                  actor_audience=who.audience)

    async def policy(context, target, decision):
        if context.identity.subject != 'owner':
            return False
        assert target.actor_subject == who.subject
        assert target.actor_client_id == who.client_id
        assert target.actor_audience == who.audience
        store.resolve(who, scope='partner:events:propose', action='event_create')
        return True

    svc = service(runtime, authorize_decision=policy)
    op = (await svc.submit(req))['operation']
    if revocation == 'expire':
        monkeypatch.setattr(access.time, 'time', lambda: grant.expires_at)
    elif revocation == 'suspend':
        store.change(grant.principal_id, action='suspend', expected_revision=1)
    else:
        store.change(grant.principal_id, action='policy', expected_revision=1,
                     policy={'scopes': [], 'actions': []})
    with pytest.raises(ToolExecutionError):
        await decide(svc, op, req)
    current, _ = await runtime.store.reserve(req)
    assert current['status'] == 'review_required'


@pytest.mark.asyncio
async def test_same_decision_race_records_one_owner_attribution(tmp_path):
    _, runtime, _, req = await setup(tmp_path)
    svc = service(runtime)
    op = (await svc.submit(req))['operation']
    first, second = await asyncio.gather(decide(svc, op, req), decide(service(runtime), op, req))
    assert sorted([first['changed'], second['changed']]) == [False, True]
    assert first['review'] == second['review']


@pytest.mark.asyncio
async def test_denied_submission_never_reserves_and_initial_states_are_closed(tmp_path):
    _, runtime, _, req = await setup(tmp_path)

    async def denied(request):
        return False

    svc = PartnerEventReviewService(store=runtime.store, authorize_submission=denied,
                                    authorize_decision=decision_allowed)
    with pytest.raises(ToolExecutionError):
        await svc.submit(req)
    assert await runtime.store.queued() == []
    with pytest.raises(ValueError):
        await runtime.store.reserve(req, initial_status='accepted')
    operation, created = await runtime.store.reserve(req)
    assert created and operation['status'] == 'queued'
    with pytest.raises(ValueError):
        PartnerEventReviewService(store=runtime.store, authorize_submission=None,
                                   authorize_decision=decision_allowed)
