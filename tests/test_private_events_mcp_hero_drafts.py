"""Owner draft-only capability: real local OAuth/MCP/SQLite, no publication."""
import base64
from copy import deepcopy
from dataclasses import replace
import re
from urllib.parse import urlencode,parse_qs,urlsplit

from aiohttp import web
from aiohttp.test_utils import TestClient,TestServer
import pytest

from db import Database
from private_events_mcp.crypto import pkce_s256
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.server import PrivateEventsMCPServer
from private_events_mcp.tool_catalog import ToolExecutionError
from test_hero_talk_compiler import fixture
from test_private_events_mcp_event_asset_tools import owner
from test_private_events_mcp_partner_protocol import rpc
from test_private_events_mcp_partner_event_operations import content


def request():
    program,_=fixture()
    return {'action':'upsert_draft','program':program,'expected_revision':0,'idempotency_key':'hero-fixture-key'}


async def setup(config,tmp_path):
    cfg=replace(config,hero_drafts_enabled=True,database_path=str(tmp_path/'canonical.sqlite'),
                authenticated_requests_per_minute=1000,anonymous_requests_per_minute=1000)
    db=Database(cfg.database_path);await db.init()
    app=web.Application();server=attach_private_events_mcp(app,cfg,event_database=db)
    return cfg,db,app,server


async def login(client,cfg,scope='hero:read hero:write'):
    verifier='h'*64;callback='https://chatgpt.com/connector/oauth/test-callback-id'
    page=await client.get(cfg.oauth_authorize_path+'?'+urlencode(dict(response_type='code',client_id=cfg.oauth_client_id,
        redirect_uri=callback,state='hero-fixture',resource=cfg.resource,scope=scope,
        code_challenge=pkce_s256(verifier),code_challenge_method='S256')))
    assert page.status==200
    sealed=re.search(r'name="authorization_request" value="([^"]+)"',await page.text())[1]
    grant=await client.post(cfg.oauth_authorize_path,data={'authorization_request':sealed,'operator_token':cfg.operator_token},allow_redirects=False)
    assert grant.status==302
    code=parse_qs(urlsplit(grant.headers['Location']).query)['code'][0]
    basic=base64.b64encode(f'{cfg.oauth_client_id}:{cfg.oauth_client_secret}'.encode()).decode()
    response=await client.post(cfg.oauth_token_path,data=dict(grant_type='authorization_code',code=code,
        redirect_uri=callback,resource=cfg.resource,code_verifier=verifier),headers={'Authorization':'Basic '+basic})
    assert response.status==200
    return (await response.json())['access_token']


def test_default_off_canonical_db_required_and_codex_unchanged(config):
    server=PrivateEventsMCPServer(config)
    assert not {n for n in server.protocol.by_name if n.startswith('hero_')}
    assert len(server.codex_protocol.by_name)==7
    with pytest.raises(ValueError,match='canonical Database'):
        PrivateEventsMCPServer(replace(config,hero_drafts_enabled=True))


@pytest.mark.asyncio
async def test_real_owner_oauth_prepare_commit_restart_exact_unicode(config,tmp_path):
    cfg,db,app,server=await setup(config,tmp_path)
    client=TestClient(TestServer(app));await client.start_server()
    try:
        token=await login(client,cfg)
        _,body=await rpc(client,cfg.mcp_path,token,'',method='tools/list')
        names={t['name'] for t in body['result']['tools']}
        assert {'hero_talk_prepare','hero_talk_commit','hero_talk_get','hero_talk_operation_get'}<=names
        assert not names & {'hero_talk_preview','hero_talk_asset_stage','hero_talk_stats'}
        _,body=await rpc(client,cfg.mcp_path,token,'hero_talk_prepare',request());prepared=content(body)
        assert prepared['status']=='prepared' and prepared['publication_enabled'] is False
        _,body=await rpc(client,cfg.mcp_path,token,'hero_talk_get',{'program_id':'fixture-program'})
        before=content(body);assert before['desired_revision']==0 and before['active_revision'] is None and before['draft'] is None
        commit={key:prepared[key] for key in ('preparation_ref','action_digest')}
        _,body=await rpc(client,cfg.mcp_path,token,'hero_talk_commit',commit);stored=content(body)
        assert stored['status']=='draft_stored'
        _,body=await rpc(client,cfg.mcp_path,token,'hero_talk_commit',commit);assert content(body)==stored
        await client.close()
        new_app=web.Application();attach_private_events_mcp(new_app,cfg,event_database=db)
        client=TestClient(TestServer(new_app));await client.start_server()
        _,body=await rpc(client,cfg.mcp_path,token,'hero_talk_get',{'program_id':'fixture-program'})
        read=content(body)
        assert read['draft']==request()['program'] and read['desired_revision']==1 and read['active_revision'] is None
        _,body=await rpc(client,cfg.mcp_path,token,'hero_talk_operation_get',{'operation_ref':stored['operation_ref']})
        assert content(body)==stored
        async with db.raw_conn() as conn:
            for table in ('event','promo_campaign','joboutbox'):
                assert (await (await conn.execute(f'SELECT COUNT(*) FROM {table}')).fetchone())[0]==0
    finally:
        await client.close();await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('changes',[
    {'scopes':frozenset({'events:write','telegram:publish'})}, {'client_id':'foreign-client'},
    {'subject':'partner:foreign'}, {'subject':'owner'}, {'expires_at':1}, {'audience':'https://foreign.test/mcp'},
])
async def test_owner_exact_subject_client_resource_scope_and_expiry(config,tmp_path,changes):
    cfg,db,app,server=await setup(config,tmp_path)
    ctx=owner(cfg,scopes=frozenset({'hero:read','hero:write'}))
    ctx=replace(ctx,identity=replace(ctx.identity,**changes))
    try:
        with pytest.raises(ToolExecutionError,match='rejected'):
            await server.hero_drafts.prepare(request(),ctx)
        async with db.raw_conn() as conn:
            assert (await (await conn.execute('SELECT COUNT(*) FROM hero_talk_change_log')).fetchone())[0]==0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_prepared_flag_revocation_foreign_client_and_digest_conflict(config,tmp_path):
    cfg,db,app,server=await setup(config,tmp_path)
    ctx=owner(cfg,scopes=frozenset({'hero:read','hero:write'}))
    try:
        prepared=await server.hero_drafts.prepare(request(),ctx)
        commit={key:prepared[key] for key in ('preparation_ref','action_digest')}
        foreign=owner(cfg,client_id=cfg.opencode_oauth_client_id,scopes=ctx.identity.scopes)
        with pytest.raises(ToolExecutionError): await server.hero_drafts.commit(commit,foreign)
        with pytest.raises(ToolExecutionError): await server.hero_drafts.commit({**commit,'action_digest':'0'*64},ctx)
        server.config=replace(cfg,hero_drafts_enabled=False)
        with pytest.raises(ToolExecutionError): await server.hero_drafts.commit(commit,ctx)
        async with db.raw_conn() as conn:
            assert (await (await conn.execute('SELECT desired_revision FROM hero_talk_program')).fetchone())[0]==0
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('change',[
    lambda r:r.update(action='publish_revision'),
    lambda r:r.update(expected_revision=True),
    lambda r:r['program'].update(origin='promo_campaign',campaign_binding={'campaign_id':1,'activity_id':2}),
    lambda r:r['program'].update(author_mode='automatic'),
    lambda r:r['program']['chains'][0]['nodes'][0]['fragments'][0].update(onclick='secret()'),
    lambda r:r['program'].update(actor_subject='owner'),
])
async def test_strict_typed_draft_has_no_publish_or_hidden_authority(config,tmp_path,change):
    cfg,db,app,server=await setup(config,tmp_path);args=request();change(args)
    try:
        with pytest.raises(ToolExecutionError) as exc:
            await server.hero_drafts.prepare(args,owner(cfg,scopes=frozenset({'hero:write'})))
        assert exc.value.error_code=='HERO_INVALID_ARGUMENTS'
    finally:
        await db.close()
