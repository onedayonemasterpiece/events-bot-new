import asyncio
from dataclasses import replace
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text
from db import Database
from models import PromoCampaign, PromoActivity, PromoTarget
from private_events_mcp.oauth import SUBJECT
from private_events_mcp.promo_operations import PromoActor, PromoOperationStore, PromoOperationError
import private_events_mcp.promo_operations as module

ACTOR=PromoActor(SUBJECT,'owner-client','resource')
NOW=datetime(2026,9,6,tzinfo=timezone.utc).timestamp()


@pytest_asyncio.fixture
async def fixture(tmp_path,monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED','1')
    db=Database(str(tmp_path/'promo.sqlite'))
    await db.init()
    async with db.get_session() as session:
        session.add(PromoCampaign(id=1,title='Existing paused',status='paused',total_exposure_goal=9,daily_exposure_cap=2))
        await session.flush()
        session.add(PromoTarget(campaign_id=1,target_type='query',query_text='existing'))
        session.add(PromoActivity(campaign_id=1,surface='video_general',profile_key='default',enabled=False))
        await session.commit()
    async def auth(session,actor,action,request):
        assert session.in_transaction()
        return True
    store=PromoOperationStore(db,auth,clock=lambda:NOW)
    current=await store.campaign_get(1,actor=ACTOR)
    request={'campaign_id':1,'campaign_revision':current['campaign_revision'],'surface':'video_general',
        'profile_key':'default','slot_policy':'first_slot','count':3}
    yield db,store,request,auth
    await db.close()


async def snapshot(db):
    async with db.get_session() as session:
        return {table:[dict(row) for row in (await session.execute(text('SELECT * FROM '+table+' ORDER BY '+('user_id' if table=='user' else 'id')))).mappings().all()]
            for table in ('promo_campaign','promo_target','promo_activity','promo_exposure','user')}


@pytest.mark.asyncio
async def test_atomic_add_preserves_paused_campaign_and_replay_never_enables(fixture):
    db,store,request,auth=fixture
    before=await snapshot(db)
    p=await store.prepare_activity(request,actor=ACTOR,idempotency_key='activity-key')
    assert p['planned_campaign_status']=='unchanged' and p['planned_activity_enabled'] is True
    assert await snapshot(db)==before
    result1,result2=await asyncio.gather(*[store.commit_activity(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR) for _ in range(2)])
    assert result1==result2 and result1['campaign_status_at_commit']=='paused'
    assert result1['publication_state']=='not_observed'
    after=await snapshot(db)
    assert after['promo_campaign'][0]['status']=='paused'
    assert {k:v for k,v in after['promo_campaign'][0].items() if k!='updated_at'}=={k:v for k,v in before['promo_campaign'][0].items() if k!='updated_at'}
    assert after['promo_target']==before['promo_target']
    assert after['promo_activity'][:1]==before['promo_activity']
    assert len(after['promo_activity'])==2
    assert after['promo_exposure']==before['promo_exposure'] and not after['user']
    async with db.get_session() as session:
        ledger=(await session.execute(text("SELECT * FROM event_change_log WHERE operation_kind='promo_activity_add'"))).mappings().one()
        assert ledger['event_id'] is ledger['base_event_revision'] is None
        await session.execute(text('UPDATE promo_activity SET enabled=0 WHERE id=:id'),{'id':result1['activity_id']})
        await session.commit()
    restarted=PromoOperationStore(db,auth,clock=lambda:NOW+9999)
    assert await restarted.commit_activity(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)==result1
    assert await restarted.operation_get(p['operation_ref'],actor=ACTOR)==result1
    assert not (await snapshot(db))['promo_activity'][-1]['enabled']


@pytest.mark.asyncio
@pytest.mark.parametrize('sql',[
    "UPDATE promo_campaign SET starts_at='2026-09-08'",
    "UPDATE promo_campaign SET ends_at='2026-09-09'",
    "UPDATE promo_campaign SET total_exposure_goal=42",
    "UPDATE promo_campaign SET daily_exposure_cap=4",
    "UPDATE promo_campaign SET status='active'",
    "UPDATE promo_target SET query_text='different'",
    "UPDATE promo_activity SET enabled=1",
])
async def test_current_campaign_cas(fixture,sql):
    db,store,request,_=fixture
    p=await store.prepare_activity(request,actor=ACTOR,idempotency_key='activity-key')
    async with db.get_session() as session:
        await session.execute(text(sql));await session.commit()
    with pytest.raises(PromoOperationError,match='REVISION_CONFLICT'):
        await store.commit_activity(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    assert len((await snapshot(db))['promo_activity'])==1


@pytest.mark.asyncio
async def test_expiry_idempotency_revocation_and_owner_binding(fixture):
    db,store,request,auth=fixture
    p=await store.prepare_activity(request,actor=ACTOR,idempotency_key='activity-key')
    assert await PromoOperationStore(db,auth,clock=lambda:NOW+100).prepare_activity(request,actor=ACTOR,idempotency_key='activity-key')==p
    expired=PromoOperationStore(db,auth,clock=lambda:NOW+600)
    assert (await expired.operation_get(p['operation_ref'],actor=ACTOR))['status']=='expired'
    with pytest.raises(PromoOperationError,match='EXPIRED'):
        await expired.commit_activity(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    with pytest.raises(PromoOperationError,match='IDEMPOTENCY_CONFLICT'):
        await store.prepare_activity({**request,'count':4},actor=ACTOR,idempotency_key='activity-key')
    with pytest.raises(PromoOperationError):
        await store.commit_activity(p['preparation_ref'],action_digest=p['action_digest'],actor=replace(ACTOR,client_id='other'))
    async def denied(*args): return False
    revoked=PromoOperationStore(db,denied,clock=lambda:NOW)
    with pytest.raises(PromoOperationError,match='ACCESS_DENIED'):
        await revoked.commit_activity(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['wrong_kind','dirty','helper_flush_failure','receipt_failure'])
async def test_kind_dispatch_and_atomic_failure(fixture,monkeypatch,mode):
    db,store,request,_=fixture
    p=await store.prepare_activity(request,actor=ACTOR,idempotency_key='activity-key')
    if mode=='wrong_kind':
        with pytest.raises(PromoOperationError):
            await store.commit(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
        async with db.get_session() as session:
            await session.execute(text("UPDATE event_change_log SET operation_kind='promo_campaign_create'"));await session.commit()
    elif mode=='dirty':
        async with db.get_session() as session:
            await session.execute(text("UPDATE event_change_log SET result_json='{}'"));await session.commit()
    elif mode=='helper_flush_failure':
        original=module.add_partner_activity_to_campaign
        async def fail_after(*args,**kwargs):
            assert kwargs['actor_user_id'] is None
            await original(*args,**kwargs)
            raise RuntimeError('after flush')
        monkeypatch.setattr(module,'add_partner_activity_to_campaign',fail_after)
    else:
        async with db.get_session() as session:
            await session.execute(text("CREATE TRIGGER fail_receipt BEFORE UPDATE ON event_change_log BEGIN SELECT RAISE(ABORT,'receipt failure'); END"));await session.commit()
    with pytest.raises(Exception):
        await store.commit_activity(p['preparation_ref'],action_digest=p['action_digest'],actor=ACTOR)
    assert len((await snapshot(db))['promo_activity'])==1


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['archived','capacity'])
async def test_archived_and_capacity_fail_closed(fixture,mode):
    db,store,request,_=fixture
    async with db.get_session() as session:
        if mode=='archived':
            await session.execute(text("UPDATE promo_campaign SET status='archived'"))
        else:
            for _ in range(255): session.add(PromoActivity(campaign_id=1,surface='video_general'))
        await session.commit()
    request['campaign_revision']=(await store.campaign_get(1,actor=ACTOR))['campaign_revision']
    with pytest.raises(PromoOperationError):
        await store.prepare_activity(request,actor=ACTOR,idempotency_key='activity-key')


@pytest.mark.asyncio
@pytest.mark.parametrize('changes',[{'surface':'hero_talk'},{'profile_key':'unknown'},{'slot_policy':'unknown'},{'count':True},{'extra':1}])
async def test_strict_existing_registry_validation(fixture,changes):
    db,store,request,_=fixture
    with pytest.raises(PromoOperationError):
        await store.prepare_activity({**request,**changes},actor=ACTOR,idempotency_key='activity-key')
