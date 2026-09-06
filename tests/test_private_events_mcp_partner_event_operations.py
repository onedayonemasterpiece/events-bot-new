"""Isolated real OAuth/SQLite/protocol with fake canonical executor; no providers."""
from dataclasses import replace
import time

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest

from db import Database
from models import Event
from private_events_mcp.integration import attach_private_events_mcp, recover_private_event_creates
from test_private_events_mcp_partner_protocol import login, rpc, PARTNER_SCOPES
from test_private_events_mcp_event_asset_tools import owner
from test_private_events_mcp_media_store import make_store


async def setup(config, tmp_path, *, automatic=False):
    cfg = replace(config, partner_enabled=True, event_create_enabled=True,
                  partner_event_create_enabled=True, event_assets_enabled=True,
                  database_path=str(tmp_path/'canonical.sqlite'),
                  authenticated_requests_per_minute=1000, anonymous_requests_per_minute=1000)
    db = Database(cfg.database_path)
    await db.init()
    store, fetcher = make_store(tmp_path/'media', clock=time.time)
    app = web.Application()
    server = attach_private_events_mcp(app, cfg, event_database=db, asset_ingestor=store)
    principals = [server.partners.create(tenant_id=f'tenant-{i}', organization_id=f'org-{i}',
                  display_name=f'Partner {i}', policy={'scopes': sorted(PARTNER_SCOPES),
                    'actions':['event_create'], 'auto_approve':['event_create'] if automatic else []},
                  redirect_uris=['http://127.0.0.1:8421/callback'], expires_at=int(time.time())+3600)
                  for i in (1,2)]
    client = TestClient(TestServer(app))
    await client.start_server()
    return cfg, db, app, server, client, principals, fetcher


def args():
    return {'raw_text':'Лекция о городе состоится 12 октября в музейном зале.',
            'source_external_id':'isolated-one','idempotency_key':'isolated-key-one'}


def content(body):
    assert not body.get('result',{}).get('isError'), body
    return body['result']['structuredContent']


@pytest.mark.asyncio
async def test_partner_oauth_image_review_acceptance_portfolio_and_receipts(config, tmp_path):
    cfg, db, app, server, client, partners, fetcher = await setup(config,tmp_path)
    try:
        _, tokens = await login(client,cfg,partners[0]); token=tokens['access_token']
        _, other = await login(client,cfg,partners[1]); foreign=other['access_token']
        _, listed = await rpc(client,cfg.partner_mcp_path,token,'',method='tools/list')
        names={row['name'] for row in listed['result']['tools']}
        assert {'event_create_prepare','event_asset_stage','event_publications_get'} <= names
        assert not names & {'search','fetch','operations_snapshot','partner_event_review_decide','social_read'}
        _, staged = await rpc(client,cfg.partner_mcp_path,token,'event_asset_stage',
                              {'file':{'download_url':'https://media.example.test/image','file_id':'poster'}})
        image=content(staged)
        request={**args(),'media':[{'asset_ref':image['asset_ref'],'content_digest':image['content_digest']}]}
        _, prepared = await rpc(client,cfg.partner_mcp_path,token,'event_create_prepare',request)
        prep=content(prepared); assert prep['owner_review_required']
        _, committed = await rpc(client,cfg.partner_mcp_path,token,'event_create_commit',
            {**request,**{key:prep[key] for key in ('preparation_ref','action_digest','policy_revision')}})
        operation=content(committed); assert operation['status']=='review_required'
        assert await recover_private_event_creates(app)==0
        calls=[]
        resolver=server.event_create_runtime.executor.resolve_media
        async def execute(req):
            media=await resolver(req); calls.append(req)
            assert media[0][0].startswith(b'\x89PNG') and req.actor_subject.startswith('partner:')
            async with db.get_session() as session:
                session.add(Event(id=501,title='Isolated fixture',description='Fixture',source_text='Isolated test source',date='2099-01-01',time='19:00',location_name='Fixture'))
                await session.commit()
            return {'status':'accepted','event_ids':[501], 'events':[{'event_id':501,'result':'created'}], 'jobs':[]}
        server.event_create_runtime.executor.create=execute
        ctx=owner(cfg, scopes=frozenset({'partners:manage'}))
        reviewed=await server.protocol.by_name['partner_event_review_get'].handler({'operation_ref':operation['operation_ref']},ctx)
        assert reviewed['untrusted_source']['media']==request['media']
        preview=await server.protocol.by_name['partner_event_review_image'].handler({'operation_ref':operation['operation_ref']},ctx)
        assert preview.content[0]['mimeType']=='image/jpeg'
        assert preview.structured['content_digest']==image['content_digest']
        decided=await server.protocol.by_name['partner_event_review_decide'].handler(
            {'operation_ref':operation['operation_ref'],'action_digest':prep['action_digest'],'decision':'approve'},ctx)
        assert decided['operation']['status']=='queued' and not calls
        assert await recover_private_event_creates(app)==1
        await server.event_create_runtime.wait_for_operation(operation['operation_ref'])
        _, got=await rpc(client,cfg.partner_mcp_path,token,'event_operation_get',{'operation_ref':operation['operation_ref']})
        assert content(got)['status']=='accepted',got
        _, portfolio=await rpc(client,cfg.partner_mcp_path,token,'partner_events_list',{})
        assert [row['id'] for row in content(portfolio)['events']]==[501]
        _, receipt=await rpc(client,cfg.partner_mcp_path,token,'event_publications_get',{'operation_ref':operation['operation_ref']})
        assert content(receipt)['event_id']==501 and not content(receipt)['live_verified']
        for tool, arguments in [('event_operation_get',{'operation_ref':operation['operation_ref']}),
                                ('event_publications_get',{'operation_ref':operation['operation_ref']}),
                                ('event_asset_get',{'asset_ref':image['asset_ref']})]:
            _, result=await rpc(client,cfg.partner_mcp_path,foreign,tool,arguments)
            assert result['result']['isError']
        assert len(calls)==1 and len(fetcher.calls)==1
    finally:
        await client.close(); await db.close()


@pytest.mark.asyncio
async def test_current_revision_and_revocation_deny_commit(config,tmp_path):
    cfg,db,app,server,client,partners,_=await setup(config,tmp_path,automatic=True)
    try:
        _,tokens=await login(client,cfg,partners[0]); token=tokens['access_token']
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_create_prepare',args()); prep=content(body)
        partner=partners[0]
        server.partners.change(partner['principal_id'], action='policy', expected_revision=1,
            policy={'scopes':sorted(PARTNER_SCOPES),'actions':['event_create'],'auto_approve':[]})
        commit={**args(),**{key:prep[key] for key in ('preparation_ref','action_digest','policy_revision')}}
        _,result=await rpc(client,cfg.partner_mcp_path,token,'event_create_commit',commit)
        assert result['result']['isError'] and result['result']['structuredContent']['error_code']=='PARTNER_POLICY_REVISION_STALE'
        assert await recover_private_event_creates(app)==0
        server.partners.change(partner['principal_id'], action='suspend', expected_revision=2)
        status,_=await rpc(client,cfg.partner_mcp_path,token,'event_create_commit',commit)
        assert status==401
        _,other=await login(client,cfg,partners[1])
        _,valid=await rpc(client,cfg.partner_mcp_path,other['access_token'],'event_create_prepare',args())
        assert not content(valid)['owner_review_required']
    finally:
        await client.close(); await db.close()


@pytest.mark.asyncio
async def test_queued_partner_worker_rechecks_revocation_before_executor(config,tmp_path):
    import asyncio
    cfg,db,app,server,client,partners,_=await setup(config,tmp_path,automatic=True)
    entered, release = asyncio.Event(), asyncio.Event()
    calls=[]
    original=server.event_create_runtime.authorize
    async def delayed_policy(request):
        entered.set(); await release.wait()
        return await original(request)
    async def forbidden_executor(request):
        calls.append(request)
        raise AssertionError('Revoked actor crossed the canonical executor boundary')
    server.event_create_runtime.authorize=delayed_policy
    server.event_create_runtime.executor.create=forbidden_executor
    try:
        _,tokens=await login(client,cfg,partners[0]); token=tokens['access_token']
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_create_prepare',args()); prep=content(body)
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_create_commit',
            {**args(),**{key:prep[key] for key in ('preparation_ref','action_digest','policy_revision')}})
        operation=content(body)
        await entered.wait()
        server.partners.change(partners[0]['principal_id'], action='suspend', expected_revision=1)
        release.set()
        await server.event_create_runtime.wait_for_operation(operation['operation_ref'])
        async with db.raw_conn() as conn:
            row=await (await conn.execute('SELECT status,error_code FROM event_change_log WHERE operation_ref=?', (operation['operation_ref'],))).fetchone()
        assert row[0]=='rejected' and row[1]=='EVENT_CREATE_ACCESS_REVOKED'
        assert not calls
    finally:
        release.set(); await client.close(); await db.close()
