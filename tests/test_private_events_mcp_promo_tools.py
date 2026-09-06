"""Local OAuth→MCP→real campaign service; fixtures are never public content."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest
from sqlalchemy import func, select

from db import Database
from models import Event, PromoCampaign, PromoActivity, PromoTarget, User
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.oauth import SUBJECT
from private_events_mcp.server import PrivateEventsMCPServer
from private_events_mcp.tool_catalog import ToolExecutionError
from test_private_events_mcp_event_asset_tools import owner
from test_private_events_mcp_hero_drafts import login
from test_private_events_mcp_partner_protocol import rpc
from test_private_events_mcp_partner_event_operations import content


async def setup(config, tmp_path):
    cfg = replace(config, owner_promo_enabled=True, event_create_enabled=True,
                  database_path=str(tmp_path/'canonical.sqlite'),
                  authenticated_requests_per_minute=1000, anonymous_requests_per_minute=1000)
    db = Database(cfg.database_path)
    await db.init()
    day = (datetime.now(timezone.utc)+timedelta(days=10)).date().isoformat()
    async with db.get_session() as session:
        event = Event(title='Private isolated promo fixture', description='Fixture', date=day,
                      time='19:00', location_name='Fixture venue', source_text='Fixture')
        session.add(event)
        await session.commit()
        event_id = event.id
    accepted_ref = 'evt_op_'+'a'*24
    async with db.raw_conn() as conn:
        await conn.execute('INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,'
            'actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,'
            'request_json,status,event_id,result_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (accepted_ref,'create',SUBJECT,cfg.oauth_client_id,cfg.resource,'fixture-create','a'*64,
             'manual','','{}','accepted',event_id,json.dumps({'status':'accepted','event_ids':[event_id]})))
        await conn.commit()
    request = dict(accepted_event_operation_ref=accepted_ref, event_id=event_id, event_revision='0'*64,
        surface='vk_repost', profile_key=None, slot_policy=None, count=2,
        ends_at=day, is_editorial=True, sponsorship_disclosure=None, title_override='Private fixture campaign')
    app = web.Application()
    server = attach_private_events_mcp(app,cfg,event_database=db)
    return cfg,db,app,server,request


async def counts(db):
    async with db.get_session() as session:
        return tuple([await session.scalar(select(func.count()).select_from(model))
                      for model in (PromoCampaign,PromoTarget,PromoActivity,User)])


def test_default_off_and_explicit_prerequisites(config):
    server = PrivateEventsMCPServer(config)
    assert not any(name.startswith('promo_') for name in server.protocol.by_name)
    assert len(server.codex_protocol.by_name)==7
    with pytest.raises(ValueError,match='canonical Database'):
        PrivateEventsMCPServer(replace(config,owner_promo_enabled=True,event_create_enabled=True))
    with pytest.raises(ValueError,match='event-create'):
        replace(config,owner_promo_enabled=True).validate()


@pytest.mark.asyncio
async def test_oauth_prepare_commit_replay_without_telegram_creator(config,tmp_path):
    cfg,db,app,server,request = await setup(config,tmp_path)
    client = TestClient(TestServer(app)); await client.start_server()
    try:
        before = await counts(db)
        token = await login(client,cfg,'promo:read promo:write')
        async def call(name,args):
            _,body = await rpc(client,cfg.mcp_path,token,name,args)
            return content(body)
        target=await call('promo_capabilities',{'accepted_event_operation_ref':request['accepted_event_operation_ref'],
                                               'event_id':request['event_id']})
        assert len(target['event_revision'])==64 and target['event_revision']!='0'*64
        request['event_revision']=target['event_revision']  # Obtained through MCP, never out-of-band SQLite.
        prepared = await call('promo_campaign_create_prepare',{'request':request,'idempotency_key':'promo-fixture-key'})
        assert await counts(db)==before
        committed = await call('promo_campaign_create_commit',{
            'preparation_ref':prepared['preparation_ref'],'action_digest':prepared['action_digest']})
        assert committed['publication_state']=='not_observed'
        after = await counts(db)
        assert after[:3]==(1,1,2) and after[3]==before[3]
        assert await call('promo_campaign_create_commit',{
            'preparation_ref':prepared['preparation_ref'],'action_digest':prepared['action_digest']})==committed
        assert await counts(db)==after
        async with db.get_session() as session:
            campaign=(await session.execute(select(PromoCampaign))).scalar_one()
            assert campaign.created_by is None and campaign.status=='active'
            campaign.status='paused'
            session.add(campaign)
            await session.commit()
        assert await call('promo_campaign_create_commit',{
            'preparation_ref':prepared['preparation_ref'],'action_digest':prepared['action_digest']})==committed
        async with db.get_session() as session:
            campaign=(await session.execute(select(PromoCampaign))).scalar_one()
            assert campaign.status=='paused'
        paused=await call('promo_campaign_get',{'campaign_id':committed['campaign_id']})
        activity_prepared=await call('promo_activity_add_prepare',{'request':{
            'campaign_id':committed['campaign_id'],'campaign_revision':paused['campaign_revision'],
            'surface':'video_general','profile_key':'default','slot_policy':'first_slot','count':1},
            'idempotency_key':'promo-activity-fixture'})
        assert await counts(db)==after
        activity_result=await call('promo_activity_add_commit',{
            'preparation_ref':activity_prepared['preparation_ref'],'action_digest':activity_prepared['action_digest']})
        assert activity_result['campaign_status_at_commit']=='paused'
        current=await call('promo_campaign_get',{'campaign_id':committed['campaign_id']})
        assert current['campaign']['status']=='paused'
        assert current['activities'][:2]==paused['activities']
        after=await counts(db)
        assert after[:3]==(1,1,3) and after[3]==before[3]
        assert await call('promo_activity_add_commit',{
            'preparation_ref':activity_prepared['preparation_ref'],'action_digest':activity_prepared['action_digest']})==activity_result
        assert await counts(db)==after
        await client.close()
        restarted_app=web.Application()
        attach_private_events_mcp(restarted_app,cfg,event_database=db)
        client=TestClient(TestServer(restarted_app)); await client.start_server()
        token=await login(client,cfg,'promo:read')
        assert await call('promo_operation_get',{'operation_ref':prepared['operation_ref']})==committed
        assert await call('promo_operation_get',{'operation_ref':activity_prepared['operation_ref']})==activity_result
        current=await call('promo_campaign_get',{'campaign_id':committed['campaign_id']})
        assert current['campaign']['status']=='paused'
        assert len(current['campaign_revision'])==64
        assert current['publication_state']=='not_observed'
        assert current['delivery_stats']=='unavailable'
        assert current['recorded_exposures']=={
            'source':'promo_exposure','scope':'recent_recorded_rows_only',
            'rows':[],'has_more':False}
        page=await call('promo_campaigns_list',{'status':'paused','limit':1})
        assert [c['campaign_id'] for c in page['campaigns']]==[committed['campaign_id']]
        assert not page['has_more']
        assert await counts(db)==after
    finally:
        await client.close(); await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('change',[{'subject':'partner:x'},{'scopes':frozenset()},
    {'client_id':'foreign'},{'expires_at':1},{'audience':'foreign'}])
async def test_current_owner_boundary(config,tmp_path,change):
    cfg,db,app,server,request = await setup(config,tmp_path)
    try:
        with pytest.raises(ToolExecutionError):
            await server.owner_promo.prepare({'request':request,'idempotency_key':'promo-fixture-key'},
                owner(cfg,scopes=frozenset({'promo:write'}),**change) if 'scopes' not in change else owner(cfg,**change))
        assert (await counts(db))[:3]==(0,0,0)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_schema_and_flag_fail_closed(config,tmp_path):
    cfg,db,app,server,request = await setup(config,tmp_path)
    ctx=owner(cfg,scopes=frozenset({'promo:write'}))
    try:
        for patch in ({'count':True},{'surface':'hero_talk'},{'creator_user_id':123},{'slot_policy':'first_slot'},
                      {'event_id':'1'},{'title_override':'x'*201}):
            with pytest.raises(ToolExecutionError) as exc:
                await server.owner_promo.prepare({'request':{**request,**patch},'idempotency_key':'promo-fixture-key'},ctx)
            assert exc.value.error_code=='PROMO_INVALID_ARGUMENTS'
        server.config=replace(cfg,owner_promo_enabled=False)
        with pytest.raises(ToolExecutionError):
            await server.owner_promo.prepare({'request':request,'idempotency_key':'promo-fixture-key'},ctx)
        assert (await counts(db))[:3]==(0,0,0)
    finally:
        await db.close()
