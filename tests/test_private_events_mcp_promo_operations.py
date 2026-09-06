import asyncio
from dataclasses import replace
import json
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text
from db import Database
from models import Event
from static_site_release import event_public_revision
from private_events_mcp.oauth import SUBJECT
from private_events_mcp.promo_operations import PromoActor, PromoOperationStore, PromoOperationError
import private_events_mcp.promo_operations as module

ACTOR=PromoActor(SUBJECT,'client','resource')
SOURCE='evt_op_'+'a'*24
NOW=datetime(2026,9,6,tzinfo=timezone.utc).timestamp()


@pytest_asyncio.fixture
async def prepared(tmp_path,monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED','1')
    db=Database(str(tmp_path/'canonical.sqlite'))
    await db.init()
    async with db.get_session() as session:
        event=Event(title='Real event',description='D',date='2026-10-01',time='19:00',location_name='Hall',source_text='S')
        session.add(event)
        await session.commit()
        await session.refresh(event)
        revision=event_public_revision(event)
        event_id=event.id
        await session.execute(text("INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,request_json,status,event_id,result_json) VALUES(:ref,'create',:subject,'client','resource','source-idem',:digest,'manual','','{}','accepted',:event,:result)"),
            {'ref':SOURCE,'subject':SUBJECT,'digest':'a'*64,'event':event_id,'result':json.dumps({'status':'accepted','event_ids':[event_id]})})
        await session.commit()
    request={'accepted_event_operation_ref':SOURCE,'event_id':event_id,'event_revision':revision,
        'surface':'video_general','profile_key':'default','slot_policy':'first_slot','count':3,
        'ends_at':'2026-10-01','is_editorial':False,'sponsorship_disclosure':None,'title_override':None}
    async def auth(session,actor,action,request):
        assert session.in_transaction()
        return True
    yield db,request,auth
    await db.close()


async def counts(db):
    async with db.get_session() as session:
        return [(await session.execute(text('SELECT count(*) FROM '+table))).scalar() for table in
                ('promo_campaign','promo_target','promo_activity','user')]


async def prep(fixture,**kwargs):
    db,request,auth=fixture
    store=PromoOperationStore(db,auth,clock=lambda:NOW)
    result=await store.prepare(request,actor=ACTOR,idempotency_key='logical-key',**kwargs)
    return store,result


@pytest.mark.asyncio
async def test_prepare_commit_restart_replay_and_no_fake_user(prepared):
    db,request,auth=prepared
    store,p=await prep(prepared)
    assert await counts(db)==[0,0,0,0]
    assert p['planned_campaign_status']=='active'
    assert p['business_validation']=='commit_recheck_required'
    assert await store.prepare(request,actor=ACTOR,idempotency_key='logical-key')==p
    store=PromoOperationStore(db,auth,clock=lambda:NOW+1)
    results=await asyncio.gather(*[store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR) for _ in range(2)])
    assert results[0]==results[1]
    assert results[0]['publication_state']=='not_observed'
    assert await counts(db)==[1,1,2,0]
    async with db.get_session() as session:
        assert (await session.execute(text('SELECT created_by FROM promo_campaign'))).scalar() is None
        await session.execute(text("UPDATE event SET title='Later edit'"))
        await session.commit()
    expired=PromoOperationStore(db,auth,clock=lambda:NOW+9999)
    assert await expired.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)==results[0]
    assert await expired.operation_get(p['operation_ref'],actor=ACTOR)==results[0]


@pytest.mark.asyncio
@pytest.mark.parametrize('sql',[
    "UPDATE event_change_log SET actor_client_id='other'",
    "UPDATE event_change_log SET status='processing'",
    "UPDATE event_change_log SET event_id=NULL",
    "UPDATE event_change_log SET result_json='{\"event_ids\":[1,2]}'",
    "UPDATE event SET identity_status='merged'",
    "UPDATE event SET merged_into_event_id=1",
    "UPDATE event SET title='Changed'",
])
async def test_invalid_binding_no_campaign(prepared,sql):
    db,request,auth=prepared
    async with db.get_session() as session:
        await session.execute(text(sql));await session.commit()
    with pytest.raises(PromoOperationError): await prep(prepared)
    assert await counts(db)==[0,0,0,0]


@pytest.mark.asyncio
async def test_stale_expiry_digest_idempotency_and_other_actor(prepared):
    db,request,auth=prepared
    store,p=await prep(prepared)
    with pytest.raises(PromoOperationError,match='IDEMPOTENCY_CONFLICT'):
        await store.prepare({**request,'count':4},actor=ACTOR,idempotency_key='logical-key')
    with pytest.raises(PromoOperationError):
        await store.commit(p['preparation_ref'],action_digest='b'*64,actor=ACTOR)
    with pytest.raises(PromoOperationError):
        await store.operation_get(p['operation_ref'],actor=replace(ACTOR,client_id='other'))
    expired=PromoOperationStore(db,auth,clock=lambda:NOW+600)
    with pytest.raises(PromoOperationError,match='EXPIRED'):
        await expired.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    async with db.get_session() as session:
        await session.execute(text("UPDATE event SET title='Changed'"));await session.commit()
    with pytest.raises(PromoOperationError,match='REVISION_CONFLICT'):
        await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    assert await counts(db)==[0,0,0,0]


@pytest.mark.asyncio
async def test_revocation_on_commit_and_historical_read(prepared):
    db,request,auth=prepared
    store,p=await prep(prepared)
    async def deny(*args): return False
    revoked=PromoOperationStore(db,deny,clock=lambda:NOW)
    with pytest.raises(PromoOperationError,match='ACCESS_DENIED'):
        await revoked.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    with pytest.raises(PromoOperationError,match='ACCESS_DENIED'):
        await revoked.operation_get(p['operation_ref'],actor=ACTOR)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',['service_after_flush','ledger_after_flush','business'])
async def test_failure_rolls_back_shared_transaction(prepared,monkeypatch,failure):
    db,request,auth=prepared
    if failure=='business':
        request['ends_at']='2026-09-01'  # Shared service owns current business eligibility.
    store,p=await prep(prepared)
    if failure=='service_after_flush':
        original=module.create_partner_event_promo_campaign
        async def fail_after(*args,**kwargs):
            await original(*args,**kwargs)
            raise RuntimeError('after flush')
        monkeypatch.setattr(module,'create_partner_event_promo_campaign',fail_after)
    elif failure=='ledger_after_flush':
        async with db.get_session() as session:
            await session.execute(text("CREATE TRIGGER fail_result BEFORE UPDATE ON event_change_log WHEN NEW.operation_kind='promo_campaign_create' BEGIN SELECT RAISE(ABORT,'receipt failure'); END"))
            await session.commit()
    with pytest.raises(Exception):
        await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    assert await counts(db)==[0,0,0,0]
    assert (await store.operation_get(p['operation_ref'],actor=ACTOR))['status']=='prepared'


@pytest.mark.asyncio
async def test_partner_and_extra_keys_rejected(prepared):
    db,request,auth=prepared
    store=PromoOperationStore(db,auth,clock=lambda:NOW)
    with pytest.raises(PromoOperationError):
        await store.prepare(request,actor=replace(ACTOR,subject='partner:any:1'),idempotency_key='logical-key')
    for bad in ({**request,'extra':1},{**request,'count':True},{**request,'ends_at':'20261001'}):
        with pytest.raises(PromoOperationError):
            await store.prepare(bad,actor=ACTOR,idempotency_key='logical-key')


@pytest.mark.asyncio
@pytest.mark.parametrize('surface,profile,valid',[
    ('video_general',None,False),('video_general','unknown',False),
    ('vk_repost','default',False),('vk_repost',None,True),
])
async def test_existing_profile_registry(prepared,surface,profile,valid):
    db,request,auth=prepared
    request.update(surface=surface,profile_key=profile)
    if not valid:
        with pytest.raises(PromoOperationError): await prep(prepared)
    else:
        store,p=await prep(prepared)
        result=await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
        assert result['campaign_status_at_commit']=='active'


@pytest.mark.asyncio
async def test_authorization_and_service_share_locked_session(prepared,monkeypatch):
    db,request,_=prepared
    current=[]
    async def auth(session,actor,action,request):
        assert session.in_transaction()
        if action=='commit': current.append(session)
        return True
    original=module.create_partner_event_promo_campaign
    async def checked(*args,**kwargs):
        assert kwargs['session'] is current[-1]
        assert kwargs['session'].in_transaction()
        return await original(*args,**kwargs)
    monkeypatch.setattr(module,'create_partner_event_promo_campaign',checked)
    store,p=await prep((db,request,auth))
    await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)


@pytest.mark.asyncio
async def test_commit_rechecks_accepted_create_binding(prepared):
    db,request,auth=prepared
    store,p=await prep(prepared)
    async with db.get_session() as session:
        await session.execute(text("UPDATE event_change_log SET result_json='{}' WHERE operation_ref=:ref"),{'ref':SOURCE})
        await session.commit()
    with pytest.raises(PromoOperationError,match='BINDING_DENIED'):
        await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    assert await counts(db)==[0,0,0,0]


@pytest.mark.asyncio
async def test_prepare_replay_keeps_frozen_expiry(prepared):
    db,request,auth=prepared
    store,p=await prep(prepared)
    later=PromoOperationStore(db,auth,clock=lambda:NOW+120)
    assert await later.prepare(request,actor=ACTOR,idempotency_key='logical-key')==p
    expired=PromoOperationStore(db,auth,clock=lambda:NOW+600)
    replay=await expired.prepare(request,actor=ACTOR,idempotency_key='logical-key')
    assert replay=={**p,'status':'expired'}
    assert await expired.operation_get(p['operation_ref'],actor=ACTOR)==replay
    assert await counts(db)==[0,0,0,0]


@pytest.mark.asyncio
@pytest.mark.parametrize('field',['before_json','after_json','changed_fields_json','result_event_revision','domain_receipt_json','result_json'])
async def test_dirty_preparation_history_never_overwritten(prepared,field):
    db,request,auth=prepared
    store,p=await prep(prepared)
    async with db.get_session() as session:
        await session.execute(text('UPDATE event_change_log SET '+field+'=:value WHERE operation_ref=:ref'),{'value':'{}','ref':p['operation_ref']})
        await session.commit()
    with pytest.raises(PromoOperationError,match='OPERATION_CONFLICT'):
        await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    assert await counts(db)==[0,0,0,0]


@pytest.mark.asyncio
async def test_capabilities_current_token_no_reservation(prepared):
    db,request,auth=prepared
    calls=[]
    async def checked(session,actor,action,packet):
        calls.append(action)
        assert session.in_transaction() and set(packet)=={'accepted_event_operation_ref','event_id'}
        return True
    store=PromoOperationStore(db,checked,clock=lambda:NOW)
    result=await store.capabilities(SOURCE,request['event_id'],actor=ACTOR)
    assert result['event_revision']==request['event_revision']
    assert result['business_validation']=='commit_recheck_required'
    assert result['supported_surfaces']==['video_general','vk_repost']
    assert result['video_profiles']==module.PARTNER_PROMO_VIDEO_PROFILES
    assert result['slot_policies']==module.PARTNER_PROMO_SLOT_POLICIES
    async with db.get_session() as session:
        assert (await session.execute(text('SELECT count(*) FROM event_change_log'))).scalar()==1
        await session.execute(text("UPDATE event SET title='Current changed title',silent=1,lifecycle_status='cancelled'"))
        await session.commit()
    current=await store.capabilities(SOURCE,request['event_id'],actor=ACTOR)
    assert current['event_revision']!=result['event_revision']
    assert current['silent'] is True and current['lifecycle_status']=='cancelled'
    assert calls==['capabilities','capabilities']
    assert await counts(db)==[0,0,0,0]


@pytest.mark.asyncio
@pytest.mark.parametrize('change',[
    "UPDATE event_change_log SET actor_subject='foreign'",
    "UPDATE event_change_log SET actor_client_id='other'",
    "UPDATE event_change_log SET status='processing'",
    "UPDATE event SET identity_status='merged'",
    "UPDATE event SET merged_into_event_id=1",
])
async def test_capabilities_exact_actor_accepted_canonical_binding(prepared,change):
    db,request,auth=prepared
    async with db.get_session() as session:
        await session.execute(text(change));await session.commit()
    store=PromoOperationStore(db,auth)
    with pytest.raises(PromoOperationError,match='BINDING_DENIED'):
        await store.capabilities(SOURCE,request['event_id'],actor=ACTOR)


@pytest.mark.asyncio
async def test_capabilities_revoked_wrong_target_and_foreign_actor(prepared):
    db,request,auth=prepared
    async def deny(*args): return False
    with pytest.raises(PromoOperationError,match='ACCESS_DENIED'):
        await PromoOperationStore(db,deny).capabilities(SOURCE,request['event_id'],actor=ACTOR)
    store=PromoOperationStore(db,auth)
    with pytest.raises(PromoOperationError):
        await store.capabilities(SOURCE,999,actor=ACTOR)
    with pytest.raises(PromoOperationError):
        await store.capabilities(SOURCE,request['event_id'],actor=replace(ACTOR,audience='foreign'))
