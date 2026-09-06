import json
import sqlite3
from dataclasses import replace

import pytest
import pytest_asyncio
from db import Database
from models import Event
from event_lifecycle_operations import LifecycleAction, LifecycleOperationError, apply_lifecycle_operation, _revision


@pytest_asyncio.fixture
async def prepared(tmp_path, monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    path = tmp_path / 'events.sqlite'
    db = Database(str(path))
    await db.init()
    async with db.get_session() as session:
        for title in ('Target', 'Unrelated same venue'):
            session.add(Event(title=title, description='D', date='2026-10-01', time='19:00', location_name='Same hall', source_text='S'))
        await session.commit()
    await db.close()
    with sqlite3.connect(path) as con:
        con.row_factory = sqlite3.Row
        event = con.execute('SELECT * FROM event ORDER BY id').fetchone()
        action = LifecycleAction(event['id'], 'CANCEL', _revision(event), 'oauth-owner', 'client', 'resource')
        con.execute('INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,request_json,status,event_id,base_event_revision,organizer_comment) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('evt_op_aaaaaaaaaaaaaaaaaaaaaaaa','event_cancel',action.actor_subject,action.actor_client_id,action.actor_audience,'idem',action.digest,'owner','',json.dumps(action.payload()),'processing',action.event_id,action.base_event_revision,'private audit preserved'))
    return path, action


def apply(prepared, **kwargs):
    path, action = prepared
    return apply_lifecycle_operation(path, operation_ref='evt_op_aaaaaaaaaaaaaaaaaaaaaaaa', action=action,
        expected_action_digest=action.digest, authorize=kwargs.pop('authorize', lambda conn, a: True),
        verify_review=kwargs.pop('verify_review', lambda conn, a, op, digest: True), **kwargs)


def row(path, table='event', where='id=1'):
    with sqlite3.connect(path) as con:
        con.row_factory=sqlite3.Row
        return dict(con.execute(f'SELECT * FROM {table} WHERE {where}').fetchone())


@pytest.mark.asyncio
@pytest.mark.parametrize('kind,target', [('CANCEL','cancelled'),('POSTPONE','postponed')])
async def test_atomic_transition_history_and_replay(prepared, kind, target):
    path, action = prepared
    action = replace(action, action=kind)
    with sqlite3.connect(path) as con:
        con.execute('UPDATE event_change_log SET operation_kind=?,action_digest=?,request_json=?', ('event_'+kind.lower(),action.digest,json.dumps(action.payload())))
    prepared = path, action
    calls=[]
    def policy(conn, a):
        assert conn.in_transaction and a == action
        calls.append(id(conn))
        return True
    def review(conn,a,op,digest):
        assert id(conn)==calls[-1] and op=='evt_op_aaaaaaaaaaaaaaaaaaaaaaaa' and digest==action.digest
        return True
    result = apply(prepared, authorize=policy, verify_review=review)
    assert result['downstream']=='reconciliation_required'
    assert row(path)['lifecycle_status']==target
    assert row(path, where='id=2')['lifecycle_status']=='active'
    ledger=row(path,'event_change_log')
    assert ledger['organizer_comment']=='private audit preserved'
    assert json.loads(ledger['before_json'])=={'lifecycle_status':'active'}
    assert json.loads(ledger['after_json'])=={'lifecycle_status':target}
    # Later valid edits do not invalidate historical acceptance or get undone.
    with sqlite3.connect(path) as con:
        con.execute("UPDATE event SET lifecycle_status='active',title='Later edit' WHERE id=1")
    assert apply(prepared)==result
    assert row(path)['title']=='Later edit'
    assert row(path)['lifecycle_status']=='active'


@pytest.mark.asyncio
@pytest.mark.parametrize('callback', ['authorize','verify_review'])
async def test_revoked_or_unreviewed_denies_before_and_after_acceptance(prepared,callback):
    with pytest.raises(LifecycleOperationError):
        apply(prepared, **{callback:lambda *args:False})
    assert row(prepared[0])['lifecycle_status']=='active'
    apply(prepared)
    with pytest.raises(LifecycleOperationError):
        apply(prepared, **{callback:lambda *args:False})


@pytest.mark.asyncio
@pytest.mark.parametrize('sql', [
    "UPDATE event SET title='Changed' WHERE id=1",
    "UPDATE event SET identity_status='merged' WHERE id=1",
    "UPDATE event SET merged_into_event_id=2 WHERE id=1",
    "UPDATE event_change_log SET actor_subject='foreign'",
    "UPDATE event SET identity_status='' WHERE id=1",
    "UPDATE event_change_log SET event_id=2",
    "UPDATE event_change_log SET operation_kind='event_postpone'",
    "UPDATE event_change_log SET action_digest='wrong'",
    "UPDATE event_change_log SET status='rejected'",
    "UPDATE event_change_log SET status='queued'",
    "UPDATE event_change_log SET request_json='{}'",
    "UPDATE event_change_log SET result_json='{}'",
])
async def test_stale_foreign_unclaimed_denied(prepared,sql):
    with sqlite3.connect(prepared[0]) as con: con.execute(sql)
    with pytest.raises(LifecycleOperationError): apply(prepared)
    assert row(prepared[0])['lifecycle_status']=='active'


@pytest.mark.asyncio
async def test_receipt_failure_rolls_back_event(prepared):
    with sqlite3.connect(prepared[0]) as con:
        con.execute("CREATE TRIGGER fail_receipt BEFORE UPDATE ON event_change_log BEGIN SELECT RAISE(ABORT, 'receipt failure'); END")
    with pytest.raises(sqlite3.IntegrityError): apply(prepared)
    assert row(prepared[0])['lifecycle_status']=='active'
    assert row(prepared[0],'event_change_log')['status']=='processing'


@pytest.mark.asyncio
async def test_tampered_receipt_and_digest_fail(prepared):
    with pytest.raises(LifecycleOperationError):
        apply_lifecycle_operation(prepared[0],operation_ref='evt_op_aaaaaaaaaaaaaaaaaaaaaaaa',action=prepared[1],expected_action_digest='0'*64,authorize=lambda *a:True,verify_review=lambda *a:True)
    apply(prepared)
    with sqlite3.connect(prepared[0]) as con: con.execute("UPDATE event_change_log SET after_json='{}'")
    with pytest.raises(LifecycleOperationError): apply(prepared)


@pytest.mark.asyncio
async def test_concurrent_duplicate_never_reapplies(prepared):
    import asyncio
    results = await asyncio.gather(asyncio.to_thread(apply,prepared), asyncio.to_thread(apply,prepared))
    assert results[0] == results[1]
    assert row(prepared[0],'event_change_log')['status']=='accepted'


@pytest.mark.asyncio
async def test_silent_preserved_and_terminal_other_operation_denied(prepared):
    path, action = prepared
    with sqlite3.connect(path) as con:
        con.row_factory=sqlite3.Row
        con.execute('UPDATE event SET silent=1 WHERE id=1')
        action=replace(action,base_event_revision=_revision(con.execute('SELECT * FROM event WHERE id=1').fetchone()))
        con.execute('UPDATE event_change_log SET base_event_revision=?,action_digest=?,request_json=?', (action.base_event_revision,action.digest,json.dumps(action.payload())))
    apply((path,action))
    assert row(path)['silent']==1
    with sqlite3.connect(path) as con:
        con.execute("UPDATE event_change_log SET status='processing',before_json=NULL,after_json=NULL,result_event_revision=NULL")
    with pytest.raises(LifecycleOperationError): apply((path,action))


@pytest.mark.asyncio
async def test_current_policy_reads_exact_locked_connection(prepared):
    path, action=prepared
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE test_policy(subject TEXT PRIMARY KEY, allowed INTEGER)')
        con.execute('INSERT INTO test_policy VALUES(?,0)',(action.actor_subject,))
    def policy(conn, target):
        assert conn.in_transaction
        return conn.execute('SELECT allowed FROM test_policy WHERE subject=?',(target.actor_subject,)).fetchone()[0] == 1
    with pytest.raises(LifecycleOperationError,match='access_denied'): apply(prepared,authorize=policy)
    assert row(path)['lifecycle_status']=='active'


@pytest.mark.parametrize('changes',[{'event_id':True},{'action':[]},{'base_event_revision':None},{'actor_subject':''},{'actor_client_id':'bad\nclient'}])
def test_malformed_action(changes):
    action=LifecycleAction(1,'CANCEL','a'*64,'owner','client','resource')
    with pytest.raises(LifecycleOperationError): replace(action,**changes).payload()


@pytest.mark.asyncio
async def test_callback_exception_rolls_back(prepared):
    def denied(conn, action):
        raise RuntimeError('policy unavailable')
    with pytest.raises(RuntimeError,match='policy unavailable'):
        apply(prepared,authorize=denied)
    assert row(prepared[0])['lifecycle_status']=='active'
    assert row(prepared[0],'event_change_log')['status']=='processing'


@pytest.mark.parametrize('revision', [None, True, False, 0, -1, '1'])
def test_partner_requires_positive_policy_revision(revision):
    action=LifecycleAction(1,'CANCEL','a'*64,'partner:principal:1','client','resource',revision)
    with pytest.raises(LifecycleOperationError,match='partner_policy_revision_invalid'):
        action.payload()


@pytest.mark.asyncio
async def test_partner_policy_revision_frozen_and_current_rechecked(prepared):
    path, owner_action=prepared
    action=replace(owner_action,actor_subject='partner:principal:1',partner_policy_revision=1)
    changed=replace(action,partner_policy_revision=2)
    assert action.digest != changed.digest
    with sqlite3.connect(path) as con:
        con.execute('UPDATE event_change_log SET actor_subject=?,action_digest=?,request_json=?',
                    (action.actor_subject,action.digest,json.dumps(action.payload())))
    # A request with changed revision cannot reuse the frozen ledger operation.
    with pytest.raises(LifecycleOperationError,match='operation_context_conflict'):
        apply((path,changed))
    # Host compares the frozen revision against current durable grant policy.
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE test_grant_policy(subject TEXT PRIMARY KEY, revision INTEGER)')
        con.execute('INSERT INTO test_grant_policy VALUES(?,2)',(action.actor_subject,))
    def current_policy(conn,target):
        current=conn.execute('SELECT revision FROM test_grant_policy WHERE subject=?',(target.actor_subject,)).fetchone()[0]
        return current == target.partner_policy_revision
    with pytest.raises(LifecycleOperationError,match='access_denied'):
        apply((path,action),authorize=current_policy)
    assert row(path)['lifecycle_status']=='active'
    assert row(path,'event_change_log')['status']=='processing'
