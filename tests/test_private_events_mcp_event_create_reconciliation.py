"""Synthetic immutable receipts exercise recovery; core tests prove atomic creation."""
from dataclasses import replace
import json

import pytest

from private_events_mcp.event_create_reconciliation import EventCreateReconciler
from test_private_events_mcp_event_create_recovery import setup, allowed


async def prepared(tmp_path, *, status='outcome_unknown'):
    db,runtime,executor,request=await setup(tmp_path)
    runtime.authorize=allowed
    request=replace(request, actor_subject='events-bot-owner')
    operation,_=await runtime.store.reserve(request)
    receipt={'schema':'event-operation-domain-receipt-v1','operation_ref':operation['operation_ref'],
             'action_digest':request.action_digest,'actor_subject':request.actor_subject,
             'actor_client_id':request.actor_client_id,'actor_audience':request.actor_audience,
             'event_id':123,'effect':'created','candidate_key':'candidate-one','occurrence_key':'occurrence-one',
             'candidate_state_id':1,'attempt_no':1,'source_fingerprint':'a'*64}
    async with db.raw_conn() as conn:
        columns={row[1] for row in await (await conn.execute('PRAGMA table_info(event_change_log)')).fetchall()}
        if 'domain_receipt_json' not in columns:
            await conn.execute('ALTER TABLE event_change_log ADD COLUMN domain_receipt_json JSON')
        await conn.execute('CREATE TABLE event(id INTEGER PRIMARY KEY,identity_status TEXT,merged_into_event_id INTEGER)')
        await conn.execute("INSERT INTO event VALUES(123,'canonical',NULL)")
        await conn.execute("UPDATE event_change_log SET status=?,domain_receipt_json=?,started_at=datetime('now','-31 minutes')", (status,json.dumps(receipt)))
        await conn.commit()
    return db,runtime,executor,request,operation,receipt


@pytest.mark.asyncio
@pytest.mark.parametrize('status',['outcome_unknown','processing'])
async def test_exact_receipt_recovers_domain_only_without_parser(tmp_path,status):
    db,runtime,executor,request,op,_=await prepared(tmp_path,status=status)
    assigned=[]
    async def assign(req,result): assigned.append((req,result))
    runtime.on_accepted=assign
    reconciler=EventCreateReconciler(runtime)
    assert await reconciler.recover()==1
    assert await reconciler.recover()==0
    assert not executor.requests and len(assigned)==1
    result=await runtime.store.get(op['operation_ref'],actor_subject=request.actor_subject,
        actor_client_id=request.actor_client_id,actor_audience=request.actor_audience)
    assert result['status']=='accepted' and result['event_id']==123
    assert result['result']['publication_state']=='reconciliation_required'


@pytest.mark.asyncio
@pytest.mark.parametrize('patch',[
    {'operation_ref':'evt_op_'+'x'*24},{'action_digest':'0'*64},{'actor_subject':'foreign'},
    {'actor_client_id':'foreign'},{'actor_audience':'foreign'},{'event_id':True},{'event_id':999},
    {'effect':'created_or_maybe_merged'},{'candidate_state_id':0},{'attempt_no':1.5},
    {'source_fingerprint':'rawtext'},{'schema':'old-schema'},{'partner_policy_revision':1},
])
async def test_invalid_or_foreign_proof_never_recovers(tmp_path,patch):
    db,runtime,executor,_,_,receipt=await prepared(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute('UPDATE event_change_log SET domain_receipt_json=?',(json.dumps({**receipt,**patch}),)); await conn.commit()
    assert await EventCreateReconciler(runtime).recover()==0
    assert not executor.requests


@pytest.mark.asyncio
async def test_missing_receipt_and_current_source_are_not_proof(tmp_path):
    db,runtime,executor,_,_,_=await prepared(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute('UPDATE event_change_log SET domain_receipt_json=NULL'); await conn.commit()
    assert await EventCreateReconciler(runtime).recover()==0
    assert not executor.requests


@pytest.mark.asyncio
async def test_revocation_and_assignment_error_keep_unknown(tmp_path):
    db,runtime,executor,_,_,_=await prepared(tmp_path)
    async def deny(req): return False
    runtime.authorize=deny
    assert await EventCreateReconciler(runtime).recover()==0
    runtime.authorize=allowed
    async def conflict(req,result): raise ValueError('foreign merged event')
    runtime.on_accepted=conflict
    assert await EventCreateReconciler(runtime).recover()==0
    assert not executor.requests


@pytest.mark.asyncio
async def test_active_processing_and_noncanonical_event_stay_unknown(tmp_path):
    db,runtime,_,_,_,_=await prepared(tmp_path,status='processing')
    async with db.raw_conn() as conn:
        await conn.execute('UPDATE event_change_log SET started_at=CURRENT_TIMESTAMP'); await conn.commit()
    assert await EventCreateReconciler(runtime).recover()==0
    async with db.raw_conn() as conn:
        await conn.execute("UPDATE event_change_log SET status='outcome_unknown'")
        await conn.execute("UPDATE event SET identity_status='merged',merged_into_event_id=999"); await conn.commit()
    assert await EventCreateReconciler(runtime).recover()==0
