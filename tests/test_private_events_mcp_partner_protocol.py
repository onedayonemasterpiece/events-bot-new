"""Real HTTP/OAuth/MCP + canonical SQLite. No owner token stands in for a partner."""
import html
import json
import re
import sqlite3
import time
from dataclasses import replace
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from db import Database
from models import Event
from private_events_mcp.crypto import mint_access_token, pkce_s256
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.partner_access import PARTNER_ACTIONS, PARTNER_SCOPES


async def rpc(client, path, token, name, args=None, method='tools/call'):
    response = await client.post(path, json={'jsonrpc':'2.0','id':1,'method':method,
        'params':{'name':name,'arguments':args or {}} if method=='tools/call' else {}},
        headers={'Authorization':'Bearer '+token})
    return response.status, await response.json()


async def login(client, cfg, partner, *, resource=None, verifier='v'*64, password=None, scope=None):
    callback = 'http://127.0.0.1:8421/callback'
    params = dict(response_type='code', client_id=partner['client_id'], redirect_uri=callback,
        state='partner-state', resource=resource or cfg.partner_resource,
        scope=scope or ' '.join(sorted(PARTNER_SCOPES)), code_challenge=pkce_s256(verifier), code_challenge_method='S256')
    page = await client.get(cfg.oauth_authorize_path+'?'+urlencode(params))
    if page.status != 200:
        return page.status, await page.text()
    text = await page.text()
    assert 'Telegram не требуется' in text and 'operator_token' not in text
    sealed = html.unescape(re.search('name="authorization_request" value="([^"]+)"', text)[1])
    result = await client.post(cfg.oauth_authorize_path, data={'authorization_request':sealed,
        'partner_login': password if password is not None else partner['login_secret']}, allow_redirects=False)
    if result.status != 302:
        return result.status, await result.text()
    code = parse_qs(urlsplit(result.headers['Location']).query)['code'][0]
    token = await client.post(cfg.oauth_token_path, data=dict(grant_type='authorization_code',
        client_id=partner['client_id'], redirect_uri=callback, resource=params['resource'], code=code, code_verifier=verifier))
    return token.status, await token.json()


@pytest.mark.asyncio
async def test_partner_actual_oauth_policy_database_and_mcp(config, tmp_path):
    cfg = replace(config, partner_enabled=True, database_path=str(tmp_path/'canonical.sqlite'),
                  authenticated_requests_per_minute=1000, anonymous_requests_per_minute=1000)
    database = Database(cfg.database_path)
    await database.init()
    async with database.get_session() as session:
        for i in (1,2):
            session.add(Event(id=i,title=f'Private event {i}',description=f'Tenant secret {i}',source_text=f'Isolated tenant event {i}',
                              date='2099-12-20',time='18:00',location_name='Hall',city='Kaliningrad'))
        await session.commit()
    app=web.Application(); server=attach_private_events_mcp(app,cfg,event_database=database)
    client=TestClient(TestServer(app));await client.start_server()
    owner_token,_=mint_access_token(signing_key=cfg.signing_key,issuer=cfg.issuer,audience=cfg.resource,
        subject='owner',client_id=cfg.oauth_client_id,scopes=frozenset({'partners:manage','events:read'}),lifetime_seconds=900)
    try:
        # Owner creates partners through actual MCP, not by seeding policy rows.
        partners=[]
        for i in (1,2):
            status,result=await rpc(client,cfg.mcp_path,owner_token,'partner_create',dict(
                tenant_id=f'tenant-{i}',organization_id=f'org-{i}',display_name=f'Partner {i}',
                redirect_uris=['http://127.0.0.1:8421/callback'],event_ids=[i],expires_at=int(time.time())+3600,
                policy=dict(scopes=sorted(PARTNER_SCOPES),actions=sorted(PARTNER_ACTIONS),auto_approve=['event_create'])))
            assert status==200 and not result.get('error'),result
            assert not result['result'].get('isError'),result
            partners.append(result['result']['structuredContent'])
        a,b=partners
        assert (await login(client,cfg,a,password=b['login_secret']))[0]==403
        status,tokens=await login(client,cfg,a);assert status==200,tokens
        status,btokens=await login(client,cfg,b);assert status==200,btokens
        access=tokens['access_token']
        status,tools=await rpc(client,cfg.partner_mcp_path,access,'',method='tools/list')
        names={x['name'] for x in tools['result']['tools']}
        assert status==200 and {'partner_workspace_get','partner_events_list'}<=names
        assert not names & {'search','fetch','operations_snapshot','partner_create','social_read','incident_get'}
        status,result=await rpc(client,cfg.partner_mcp_path,access,'partner_events_list',{})
        assert status==200 and [r['id'] for r in result['result']['structuredContent']['events']]==[1],result
        _,result=await rpc(client,cfg.partner_mcp_path,access,'partner_events_list',{'query':'Private event 2'})
        assert result['result']['structuredContent']['events']==[]
        _,result=await rpc(client,cfg.partner_mcp_path,access,'partner_events_list',{'event_id':2})
        assert result['result']['isError'] and 'Tenant secret 2' not in json.dumps(result)
        assert (await rpc(client,cfg.partner_mcp_path,owner_token,'partner_events_list'))[0]==401
        assert (await rpc(client,cfg.mcp_path,access,'search'))[0]==401
        assert (await rpc(client,cfg.codex_mcp_path,access,'search'))[0]==401
        assert (await login(client,cfg,a,resource=cfg.resource))[0]==400
        # Refresh has the same audience and subject, never owner.
        refreshed=await client.post(cfg.oauth_token_path,data=dict(grant_type='refresh_token',
            client_id=a['client_id'],refresh_token=tokens['refresh_token'],resource=cfg.partner_resource))
        assert refreshed.status==200,await refreshed.text()
        refreshed_tokens=await refreshed.json()
        # A new server reads existing grants; no in-memory identity dependency.
        server2=attach_private_events_mcp(web.Application(),cfg,event_database=database)
        assert server2.oauth.verify_authorization_header('Bearer '+access,expected_resource=cfg.partner_resource).subject.startswith('partner:')
        _,changed=await rpc(client,cfg.mcp_path,owner_token,'partner_access_change',dict(
            principal_id=a['principal_id'],action='suspend',expected_revision=1))
        assert changed['result']['structuredContent']['status']=='suspended',changed
        assert (await rpc(client,cfg.partner_mcp_path,access,'partner_events_list'))[0]==401
        dead=await client.post(cfg.oauth_token_path,data=dict(grant_type='refresh_token',client_id=a['client_id'],
            refresh_token=refreshed_tokens['refresh_token'],resource=cfg.partner_resource))
        assert dead.status==401
        # Other tenant is unaffected.
        _,result=await rpc(client,cfg.partner_mcp_path,btokens['access_token'],'partner_events_list')
        assert [r['id'] for r in result['result']['structuredContent']['events']]==[2]
        with sqlite3.connect(cfg.database_path) as conn:
            assert conn.execute('SELECT COUNT(*) FROM user').fetchone()[0]==0
        assert a['login_secret'] not in json.dumps(server.partners.list())
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_partner_disabled_routes_and_catalog_absent(config):
    app=web.Application();server=attach_private_events_mcp(app,config)
    assert server.partner_protocol is None
    assert 'partner_create' not in {t.name for t in server.protocol.tools}
    client=TestClient(TestServer(app));await client.start_server()
    try:
        assert (await client.get(config.partner_resource_metadata_path)).status==404
        assert (await client.post(config.partner_mcp_path,json={})).status==404
    finally:
        await client.close()
