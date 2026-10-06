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
    principals = [
        server.partners.create(
            tenant_id=f'tenant-{i}',
            organization_id=f'org-{i}',
            display_name=f'Partner {i}',
            policy={
                'scopes': sorted(PARTNER_SCOPES),
                'actions':['event_create'],
                'auto_approve':['event_create'] if automatic else [],
            },
            authorities=[{
                'authority_kind': 'venue_operator',
                'subject_type': 'venue',
                'subject_key': 'museum-hall' if i == 1 else 'other-hall',
                'display_name': 'Музейный зал' if i == 1 else 'Другой зал',
                'cities': ['Калининград'],
            }],
            redirect_uris=['http://127.0.0.1:8421/callback'],
            expires_at=int(time.time())+3600,
        )
        for i in (1,2)
    ]
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
            return {'status':'accepted','event_ids':[501], 'events':[{'event_id':501,'result':'created'}], 'jobs':[], 'candidate_receipts':[{'accepted_event_id':999}]}
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
        assert 'candidate_receipts' not in content(got)['result']
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
        server.partners.change(partners[0]['principal_id'], action='portfolio', expected_revision=1, event_ids=[])
        _, removed=await rpc(client,cfg.partner_mcp_path,token,'event_operation_get',{'operation_ref':operation['operation_ref']})
        assert removed['result']['isError']
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


@pytest.mark.asyncio
async def test_real_smart_update_partner_receipt_recovers_lost_completion(config,tmp_path,monkeypatch):
    """Real parser facade/Smart Update/outbox/ledger; only semantic providers are fakes."""
    import asyncio
    import json
    from datetime import date,timedelta
    from sqlalchemy import select,func
    import main
    import smart_event_update as smart_update_module
    cfg,db,app,server,client,partners,_=await setup(config,tmp_path,automatic=True)
    future=(date.today()+timedelta(days=30)).isoformat()
    async def parse(text,*unused,**kwargs):
        return [{'title':'Изолированная лекция о городе','short_description':'Встреча о городской истории.',
                 'date':future,'time':'19:00','location_name':'Музейный зал','location_address':'ул. Тестовая, 1',
                 'city':'Калининград','event_type':'лекция'}]
    async def topics(event): return ['LECTURES']
    monkeypatch.setattr(main,'parse_event_via_llm',parse)
    monkeypatch.setattr(main,'classify_event_topics',topics)
    monkeypatch.setattr(smart_update_module,'SMART_UPDATE_LLM_DISABLED',True)
    original_finish=server.event_create_runtime.store.finish
    crashed=False
    async def lose_completion(ref,**kwargs):
        nonlocal crashed
        if kwargs['status']=='accepted' and not crashed:
            crashed=True
            raise RuntimeError('simulated lost completion after committed domain receipt')
        return await original_finish(ref,**kwargs)
    server.event_create_runtime.store.finish=lose_completion
    try:
        _,tokens=await login(client,cfg,partners[0]); token=tokens['access_token']
        request={**args(),'raw_text':f'Изолированная лекция о городе {future} в 19:00 в музейном зале, Калининград.'}
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_create_prepare',request); prep=content(body)
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_create_commit',
            {**request,**{key:prep[key] for key in ('preparation_ref','action_digest','policy_revision')}})
        operation=content(body)
        await server.event_create_runtime.wait_for_operation(operation['operation_ref'],timeout=30)
        await asyncio.sleep(0)
        async with db.raw_conn() as conn:
            row=await (await conn.execute('SELECT status,domain_receipt_json FROM event_change_log WHERE operation_ref=?',(operation['operation_ref'],))).fetchone()
            before_jobs=(await (await conn.execute('SELECT COUNT(*) FROM joboutbox')).fetchone())[0]
        assert row[0]=='outcome_unknown' and crashed
        receipt=json.loads(row[1]); assert receipt['effect']=='created'
        assert receipt['actor_subject'].startswith('partner:')
        assert await recover_private_event_creates(app)==1
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_operation_get',{'operation_ref':operation['operation_ref']})
        recovered=content(body)
        assert recovered['status']=='accepted' and recovered['event_id']==receipt['event_id']
        assert recovered['result']['publication_state']=='reconciliation_required'
        async with db.raw_conn() as conn:
            assert (await (await conn.execute('SELECT COUNT(*) FROM event')).fetchone())[0]==1
            assert (await (await conn.execute('SELECT creator_id FROM event')).fetchone())[0] is None
            assert (await (await conn.execute('SELECT COUNT(*) FROM joboutbox')).fetchone())[0]==before_jobs
        assert await recover_private_event_creates(app)==0
    finally:
        await client.close(); await db.close()


@pytest.mark.asyncio
async def test_real_http_foreign_same_source_cannot_accept_or_mutate_event(config,tmp_path,monkeypatch):
    """Same public source is not portfolio authority at the actual parser boundary."""
    from datetime import date,timedelta
    import main
    import smart_event_update as smart_update_module
    cfg,db,app,server,client,partners,_=await setup(config,tmp_path,automatic=True)
    future=(date.today()+timedelta(days=30)).isoformat()
    async def parse(text,*unused,**kwargs):
        return [{'title':'Изолированная лекция о городе','short_description':'Встреча о городской истории.',
                 'date':future,'time':'19:00','location_name':'Музейный зал','location_address':'ул. Тестовая, 1',
                 'city':'Калининград','event_type':'лекция'}]
    async def topics(event): return ['LECTURES']
    monkeypatch.setattr(main,'parse_event_via_llm',parse)
    monkeypatch.setattr(main,'classify_event_topics',topics)
    monkeypatch.setattr(smart_update_module,'SMART_UPDATE_LLM_DISABLED',True)
    request={**args(),'source_url':'https://example.test/isolated-shared-source',
             'raw_text':f'Изолированная лекция о городе {future} в 19:00 в музейном зале, Калининград.'}
    async def create_as(principal):
        _,tokens=await login(client,cfg,principal); token=tokens['access_token']
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_create_prepare',request); prep=content(body)
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_create_commit',
            {**request,**{key:prep[key] for key in ('preparation_ref','action_digest','policy_revision')}})
        operation=content(body)
        await server.event_create_runtime.wait_for_operation(operation['operation_ref'],timeout=30)
        _,body=await rpc(client,cfg.partner_mcp_path,token,'event_operation_get',{'operation_ref':operation['operation_ref']})
        return content(body)
    async def state():
        async with db.raw_conn() as conn:
            return tuple([await (await conn.execute(sql)).fetchall() for sql in (
                'SELECT * FROM event ORDER BY id',
                'SELECT * FROM event_source ORDER BY id',
                'SELECT * FROM joboutbox ORDER BY id',
                'SELECT * FROM mcp_partner_event ORDER BY principal_id,event_id',
            )])
    try:
        accepted=await create_as(partners[0])
        assert accepted['status']=='accepted'
        original=await state()
        denied=await create_as(partners[1])
        assert denied['status']=='review_required' and denied['event_id'] is None
        assert (denied.get('result') or {}).get('error_code') == 'PARTNER_AUTHORITY_REVIEW_REQUIRED'
        evaluation = (denied.get('result') or {}).get('authority_evaluation') or {}
        assert evaluation.get('status') == 'review_required'
        assert evaluation.get('matched_authority') is None
        assert 'candidate_receipts' not in (denied.get('result') or {})
        assert await state()==original
        async with db.raw_conn() as conn:
            receipt=await (await conn.execute('SELECT domain_receipt_json FROM event_change_log WHERE operation_ref=?',
                                             (denied['operation_ref'],))).fetchone()
        assert receipt[0] is None
        assert await recover_private_event_creates(app)==0
    finally:
        await client.close(); await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("authorities", [
    [],
    [{
        'authority_kind':'programme_operator',
        'subject_type':'programme',
        'subject_key':'festival-education',
        'display_name':'Festival education programme',
    }],
])
async def test_autoapprove_without_matchable_authority_routes_to_owner_review_before_parser(
    config, tmp_path, authorities
):
    cfg, db, app, server, client, partners, _ = await setup(
        config, tmp_path, automatic=True
    )
    principal = partners[0]['principal_id']
    changed = server.partners.change(
        principal,
        action='authorities',
        expected_revision=1,
        authorities=authorities,
    )
    assert changed['policy_revision'] == 2
    called = False

    async def should_not_execute(_request):
        nonlocal called
        called = True
        raise AssertionError("parser/executor must not run before owner review")

    server.event_create_runtime.executor.create = should_not_execute
    try:
        _, tokens = await login(client, cfg, partners[0])
        token = tokens['access_token']
        _, body = await rpc(
            client, cfg.partner_mcp_path, token, 'event_create_prepare', args()
        )
        prep = content(body)
        assert prep['owner_review_required'] is True
        assert prep['authority_gate_required'] is True
        _, body = await rpc(
            client,
            cfg.partner_mcp_path,
            token,
            'event_create_commit',
            {
                **args(),
                **{
                    key: prep[key]
                    for key in ('preparation_ref','action_digest','policy_revision')
                },
            },
        )
        operation = content(body)
        assert operation['status'] == 'review_required'
        assert called is False
        async with db.raw_conn() as conn:
            assert (await (await conn.execute('SELECT COUNT(*) FROM event')).fetchone())[0] == 0
    finally:
        await client.close()
        await server.event_create_runtime.shutdown()
        await db.close()


@pytest.mark.asyncio
async def test_semantic_authority_review_owner_override_replays_same_intent_and_shares_event(
    config, tmp_path, monkeypatch
):
    from datetime import date, timedelta
    import main
    import smart_event_update as smart_update_module

    cfg, db, app, server, client, partners, _ = await setup(
        config, tmp_path, automatic=True
    )
    future = (date.today() + timedelta(days=30)).isoformat()

    async def parse(text, *unused, **kwargs):
        return [{
            'title':'Изолированная лекция о городе',
            'short_description':'Встреча о городской истории.',
            'date':future,
            'time':'19:00',
            'location_name':'Музейный зал',
            'location_address':'ул. Тестовая, 1',
            'city':'Калининград',
            'event_type':'лекция',
        }]

    async def topics(event):
        return ['LECTURES']

    monkeypatch.setattr(main, 'parse_event_via_llm', parse)
    monkeypatch.setattr(main, 'classify_event_topics', topics)
    monkeypatch.setattr(smart_update_module, 'SMART_UPDATE_LLM_DISABLED', True)
    request = {
        **args(),
        'source_url':'https://example.test/shared-authority-review',
        'raw_text':f'Изолированная лекция о городе {future} в 19:00 в музейном зале, Калининград.',
    }

    async def prepare_commit(principal):
        _, tokens = await login(client, cfg, principal)
        token = tokens['access_token']
        _, body = await rpc(
            client, cfg.partner_mcp_path, token, 'event_create_prepare', request
        )
        prep = content(body)
        _, body = await rpc(
            client, cfg.partner_mcp_path, token, 'event_create_commit',
            {
                **request,
                **{
                    key: prep[key]
                    for key in ('preparation_ref','action_digest','policy_revision')
                },
            },
        )
        operation = content(body)
        await server.event_create_runtime.wait_for_operation(
            operation['operation_ref'], timeout=30
        )
        _, body = await rpc(
            client, cfg.partner_mcp_path, token, 'event_operation_get',
            {'operation_ref':operation['operation_ref']},
        )
        return token, prep, content(body)

    try:
        _token1, _prep1, first = await prepare_commit(partners[0])
        assert first['status'] == 'accepted'
        event_id = first['event_id']

        token2, prep2, second = await prepare_commit(partners[1])
        assert second['status'] == 'review_required'
        assert second['event_id'] is None
        evaluation = (second.get('result') or {}).get('authority_evaluation') or {}
        assert evaluation.get('status') == 'review_required'

        ctx = owner(cfg, scopes=frozenset({'partners:manage'}))
        review = await server.protocol.by_name['partner_event_review_get'].handler(
            {'operation_ref':second['operation_ref']}, ctx
        )
        assert review['authority_evaluation']['status'] == 'review_required'
        decided = await server.protocol.by_name['partner_event_review_decide'].handler(
            {
                'operation_ref':second['operation_ref'],
                'action_digest':prep2['action_digest'],
                'decision':'approve',
            },
            ctx,
        )
        assert decided['operation']['status'] == 'queued'
        assert await recover_private_event_creates(app) == 1
        await server.event_create_runtime.wait_for_operation(
            second['operation_ref'], timeout=30
        )
        _, body = await rpc(
            client, cfg.partner_mcp_path, token2, 'event_operation_get',
            {'operation_ref':second['operation_ref']},
        )
        accepted = content(body)
        assert accepted['status'] == 'accepted'
        assert accepted['event_id'] == event_id
        async with db.raw_conn() as conn:
            rows = await (await conn.execute(
                'SELECT principal_id,event_id FROM mcp_partner_event WHERE event_id=? ORDER BY principal_id',
                (event_id,),
            )).fetchall()
            assert len(rows) == 2
            receipts = await (await conn.execute(
                'SELECT domain_receipt_json FROM event_change_log WHERE operation_ref=?',
                (second['operation_ref'],),
            )).fetchone()
        import json
        proof = json.loads(receipts[0])
        assert proof['partner_authority_status'] == 'owner_override'
    finally:
        await client.close()
        await db.close()


@pytest.mark.asyncio
async def test_matching_festival_authority_reconciles_registry_only_after_event_acceptance(
    config, tmp_path, monkeypatch
):
    from datetime import date, timedelta
    from sqlalchemy import select
    import main
    import smart_event_update as smart_update_module
    from models import Festival

    cfg, db, app, server, client, partners, _ = await setup(
        config, tmp_path, automatic=True
    )
    principal = partners[0]['principal_id']
    server.partners.change(
        principal,
        action='authorities',
        expected_revision=1,
        authorities=[{
            'authority_kind':'festival_operator',
            'subject_type':'festival',
            'subject_key':'kantata',
            'display_name':'Кантата',
        }],
    )
    future = (date.today() + timedelta(days=30)).isoformat()

    async def parse(text, *unused, **kwargs):
        return [{
            'title':'Концерт фестиваля',
            'short_description':'Один концерт фестиваля.',
            'date':future,
            'time':'19:00',
            'location_name':'Чужая площадка',
            'location_address':'ул. Тестовая, 1',
            'city':'Калининград',
            'event_type':'концерт',
            'festival':'Кантата',
            'festival_context':'event_with_festival',
        }]

    async def topics(event):
        return ['CONCERTS']

    original_ensure = main.ensure_festival
    ensure_calls = []

    async def guarded_ensure(*args, **kwargs):
        async with db.raw_conn() as conn:
            event_count = (
                await (await conn.execute('SELECT COUNT(*) FROM event')).fetchone()
            )[0]
        ensure_calls.append(event_count)
        assert event_count == 1, 'Festival registry write crossed authority gate'
        return await original_ensure(*args, **kwargs)

    monkeypatch.setattr(main, 'parse_event_via_llm', parse)
    monkeypatch.setattr(main, 'classify_event_topics', topics)
    monkeypatch.setattr(main, 'ensure_festival', guarded_ensure)
    monkeypatch.setattr(smart_update_module, 'SMART_UPDATE_LLM_DISABLED', True)
    request = {
        **args(),
        'source_external_id':'festival-match',
        'idempotency_key':'festival-match-key',
        'raw_text':f'Концерт фестиваля Кантата {future} в 19:00.',
    }
    try:
        _, tokens = await login(client, cfg, partners[0])
        token = tokens['access_token']
        _, body = await rpc(
            client, cfg.partner_mcp_path, token, 'event_create_prepare', request
        )
        prep = content(body)
        assert prep['owner_review_required'] is False
        _, body = await rpc(
            client, cfg.partner_mcp_path, token, 'event_create_commit',
            {
                **request,
                **{
                    key: prep[key]
                    for key in ('preparation_ref','action_digest','policy_revision')
                },
            },
        )
        operation = content(body)
        await server.event_create_runtime.wait_for_operation(
            operation['operation_ref'], timeout=30
        )
        _, body = await rpc(
            client, cfg.partner_mcp_path, token, 'event_operation_get',
            {'operation_ref':operation['operation_ref']},
        )
        accepted = content(body)
        assert accepted['status'] == 'accepted'
        assert ensure_calls == [1]
        async with db.get_session() as session:
            festivals = (
                await session.execute(select(Festival).where(Festival.name == 'Кантата'))
            ).scalars().all()
        assert len(festivals) == 1
        assert festivals[0].start_date == future
    finally:
        await client.close()
        await db.close()
