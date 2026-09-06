import json
from dataclasses import replace

import pytest
import pytest_asyncio
from sqlalchemy import text
from db import Database
from models import PromoCampaign, PromoActivity, PromoTarget
from private_events_mcp.oauth import SUBJECT
from private_events_mcp.promo_operations import PromoActor, PromoOperationStore, PromoOperationError

ACTOR=PromoActor(SUBJECT,'client','resource')


@pytest_asyncio.fixture
async def fixture(tmp_path,monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED','1')
    db=Database(str(tmp_path/'promo.sqlite'))
    await db.init()
    async with db.get_session() as session:
        for status in ('active','paused','draft','archived','active'):
            session.add(PromoCampaign(title='Campaign '+status,status=status,created_by=987,
                goal_comment='PRIVATE_GOAL_SECRET',total_exposure_goal=4,daily_exposure_cap=2))
        await session.flush()
        session.add(PromoTarget(campaign_id=1,target_type='query',query_text='PRIVATE_TARGET_SECRET'))
        session.add(PromoActivity(campaign_id=1,surface='home_hero',config_json={'token':'CONFIG_SECRET','peer_id':-1234}))
        await session.commit()
    calls=[]
    async def auth(session,actor,action,request):
        assert session.in_transaction()
        calls.append((action,request))
        return True
    store=PromoOperationStore(db,auth)
    yield db,store,calls
    await db.close()


@pytest.mark.asyncio
async def test_current_status_not_historical_and_no_private_config(fixture):
    db,store,calls=fixture
    before=await store.campaign_get(1,actor=ACTOR)
    assert before['campaign']['status']=='active'
    assert before['campaign']['total_exposure_goal']==4
    assert before['campaign']['daily_exposure_cap']==2
    assert before['campaign']['cap_accounting']=='legacy_publication_units_not_browser_visibility'
    assert before['publication_state']=='not_observed'
    assert before['delivery_stats']=='unavailable'
    assert before['activities'][0]['config_state']=='unavailable'
    assert 'SECRET' not in json.dumps(before)
    assert 'peer_id' not in json.dumps(before)
    assert 'created_by' not in before['campaign']
    async with db.get_session() as session:
        await session.execute(text("UPDATE promo_campaign SET status='paused' WHERE id=1"))
        await session.commit()
    after=await store.campaign_get(1,actor=ACTOR)
    assert after['campaign']['status']=='paused'
    assert after['campaign_revision']!=before['campaign_revision']
    assert calls==[('campaign_get',{'campaign_id':1})]*2
    async with db.get_session() as session:
        assert (await session.execute(text('SELECT count(*) FROM user'))).scalar()==0
        assert (await session.execute(text('SELECT count(*) FROM event_change_log'))).scalar()==0


@pytest.mark.asyncio
async def test_keyset_filter_and_limits(fixture):
    db,store,calls=fixture
    first=await store.campaigns_list(actor=ACTOR,limit=2)
    assert [c['campaign_id'] for c in first['campaigns']]==[1,2]
    assert first['has_more'] and first['next_after_id']==2
    second=await store.campaigns_list(actor=ACTOR,limit=2,after_id=2)
    assert [c['campaign_id'] for c in second['campaigns']]==[3,4]
    last=await store.campaigns_list(actor=ACTOR,limit=2,after_id=4)
    assert not last['has_more'] and last['next_after_id'] is None
    active=await store.campaigns_list(actor=ACTOR,status='active')
    assert [c['campaign_id'] for c in active['campaigns']]==[1,5]
    for kwargs in ({'limit':51},{'limit':True},{'after_id':-1},{'status':'invented'}):
        with pytest.raises(PromoOperationError): await store.campaigns_list(actor=ACTOR,**kwargs)


@pytest.mark.asyncio
async def test_full_hidden_rows_affect_revision_but_response_bounded(fixture):
    db,store,_=fixture
    async with db.get_session() as session:
        for _ in range(20):
            session.add(PromoTarget(campaign_id=1,target_type='query',query_text='hidden'))
            session.add(PromoActivity(campaign_id=1,surface='video_general',config_json={}))
        await session.commit()
    first=await store.campaign_get(1,actor=ACTOR)
    assert len(first['targets'])==len(first['activities'])==16
    assert first['targets_count']==first['activities_count']==21
    assert first['targets_truncated'] and first['activities_truncated']
    assert not first['targets_count_is_lower_bound']
    async with db.get_session() as session:
        await session.execute(text("UPDATE promo_activity SET config_json='{\"secret\":\"changed\"}' WHERE id=21"))
        await session.commit()
    second=await store.campaign_get(1,actor=ACTOR)
    assert second['campaign_revision']!=first['campaign_revision']
    assert second['activities']==first['activities']


@pytest.mark.asyncio
async def test_oversized_snapshot_never_partial_revision(fixture):
    db,store,_=fixture
    async with db.get_session() as session:
        for _ in range(257):
            session.add(PromoTarget(campaign_id=1,target_type='query'))
            session.add(PromoActivity(campaign_id=1,surface='video_general'))
        await session.commit()
    response=await store.campaign_get(1,actor=ACTOR)
    assert response['campaign_revision'] is None
    assert response['revision_unavailable_reason']=='snapshot_too_large'
    assert response['targets_count']==response['activities_count']==257
    assert response['targets_count_is_lower_bound'] and response['activities_count_is_lower_bound']
    assert len(response['targets'])==len(response['activities'])==16


@pytest.mark.asyncio
async def test_current_auth_owner_only_missing_and_readonly(fixture):
    db,store,_=fixture
    async def denied(*args): return False
    revoked=PromoOperationStore(db,denied)
    for current in (revoked,):
        with pytest.raises(PromoOperationError): await current.campaign_get(1,actor=ACTOR)
        with pytest.raises(PromoOperationError): await current.campaigns_list(actor=ACTOR)
    with pytest.raises(PromoOperationError):
        await store.campaign_get(1,actor=replace(ACTOR,subject='partner:1:1'))
    with pytest.raises(PromoOperationError,match='NOT_FOUND'):
        await store.campaign_get(999,actor=ACTOR)
    # Reads do not insert or remove campaigns.
    async with db.get_session() as session:
        before=(await session.execute(text('SELECT count(*) FROM promo_campaign'))).scalar()
    await store.campaign_get(1,actor=ACTOR)
    await store.campaigns_list(actor=ACTOR)
    async with db.get_session() as session:
        assert (await session.execute(text('SELECT count(*) FROM promo_campaign'))).scalar()==before
