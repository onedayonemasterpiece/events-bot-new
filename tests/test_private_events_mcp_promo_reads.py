import json
from dataclasses import replace

import pytest
import pytest_asyncio
from sqlalchemy import text
from db import Database
from models import Event, PromoCampaign, PromoActivity, PromoTarget, PromoExposure
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


@pytest.mark.asyncio
async def test_empty_recorded_page_is_not_zero_live_delivery(fixture):
    db,store,_=fixture
    result=await store.campaign_get(1,actor=ACTOR)
    assert result['recorded_exposures']=={'source':'promo_exposure','scope':'recent_recorded_rows_only','rows':[],'has_more':False}
    assert result['publication_state']=='not_observed'
    assert result['delivery_stats']=='unavailable'


@pytest.mark.asyncio
async def test_recorded_page_order_bound_isolation_and_secret_exclusion(fixture):
    from datetime import datetime, timezone
    db,store,_=fixture
    async with db.get_session() as session:
        event=Event(title='Event',description='D',date='2026-10-01',time='19:00',location_name='Hall',source_text='S')
        session.add(event)
        await session.flush()
        for index in range(17):
            session.add(PromoExposure(campaign_id=1,event_id=event.id,activity_id=1,
                surface='vk_repost',placement_kind='recorded_test',publish_status='VK_POSTED',
                public_target_count=index,
                published_at=datetime(2026,9,6 if index<9 else 5,tzinfo=timezone.utc),
                public_targets_json=[{'url':'https://private.example/SECRET','peer_id':-987}],
                details_json={'token':'EXPOSURE_SECRET'}))
        session.add(PromoExposure(campaign_id=2,event_id=event.id,surface='other',
            placement_kind='private',publish_status='OTHER_CAMPAIGN_SECRET',public_target_count=999,
            published_at=datetime(2026,9,7,tzinfo=timezone.utc)))
        await session.commit()
        expected=(await session.execute(text('SELECT id FROM promo_exposure WHERE campaign_id=1 ORDER BY published_at DESC,id DESC LIMIT 16'))).scalars().all()
    result=await store.campaign_get(1,actor=ACTOR)
    page=result['recorded_exposures']
    assert len(page['rows'])==16 and page['has_more'] is True
    assert [row['exposure_id'] for row in page['rows']]==expected
    assert set(page['rows'][0])=={'exposure_id','event_id','activity_id','surface','placement_kind','recorded_publish_status','recorded_public_target_count','recorded_published_at'}
    assert 'SECRET' not in json.dumps(page) and 'peer_id' not in json.dumps(page)
    assert page['rows'][0]['recorded_public_target_count']==8
    async with db.get_session() as session:
        assert (await session.execute(text('SELECT count(*) FROM promo_exposure'))).scalar()==18
    assert result['publication_state']=='not_observed' and result['delivery_stats']=='unavailable'


@pytest.mark.asyncio
async def test_recorded_read_uses_existing_campaign_published_index(fixture):
    db,store,_=fixture
    async with db.get_session() as session:
        plan=(await session.execute(text('EXPLAIN QUERY PLAN SELECT id,event_id,activity_id,surface,placement_kind,publish_status,public_target_count,published_at FROM promo_exposure WHERE campaign_id=:id ORDER BY published_at DESC,id DESC LIMIT 17'),{'id':1})).all()
    details=' '.join(str(row[-1]) for row in plan).upper()
    assert 'SEARCH PROMO_EXPOSURE USING INDEX IX_PROMO_EXPOSURE_CAMPAIGN_PUBLISHED' in details
    assert 'SCAN PROMO_EXPOSURE' not in details


@pytest.mark.asyncio
async def test_recorded_page_remains_under_current_authorization_and_readonly(fixture):
    from sqlalchemy import event as sqlalchemy_event
    db,store,_=fixture
    statements=[]
    engine=db._orm_engine.sync_engine
    def record(conn,cursor,statement,parameters,context,executemany):
        statements.append(statement.strip().split()[0].upper())
    sqlalchemy_event.listen(engine,'before_cursor_execute',record)
    try:
        await store.campaign_get(1,actor=ACTOR)
    finally:
        sqlalchemy_event.remove(engine,'before_cursor_execute',record)
    assert set(statements)<= {'BEGIN','SELECT'}
    async def deny(*args): return False
    with pytest.raises(PromoOperationError,match='ACCESS_DENIED'):
        await PromoOperationStore(db,deny).campaign_get(1,actor=ACTOR)
