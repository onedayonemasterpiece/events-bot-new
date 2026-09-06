import sqlite3
import time
from dataclasses import replace

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.partner_access import PartnerAccessStore, PARTNER_SCOPES, PARTNER_ACTIONS, SCHEMA
from private_events_mcp.tool_catalog import ToolExecutionError

RESOURCE='https://test.invalid/private/events-partner/mcp'


def identity(grant, scopes=None):
    return AccessIdentity(subject=grant.subject, client_id=grant.client_id, audience=RESOURCE, scopes=frozenset(scopes or PARTNER_SCOPES), expires_at=int(time.time())+3600, token_id='test-token')


@pytest.fixture
def store(tmp_path):
    path=tmp_path/'canonical.sqlite'
    with sqlite3.connect(path) as c:
        c.executescript(SCHEMA)
        c.execute('CREATE TABLE event(id INTEGER PRIMARY KEY)')
        c.executemany('INSERT INTO event VALUES(?)',[(1,),(2,)])
    return PartnerAccessStore(path,resource=RESOURCE,signing_key='isolated-test-key')


def create(store, org='a', events=None, **kw):
    args=dict(tenant_id='tenant-'+org,organization_id=org,display_name='Partner '+org,
              policy=dict(scopes=sorted(PARTNER_SCOPES),actions=sorted(PARTNER_ACTIONS),auto_approve=['event_create']),
              redirect_uris=['http://127.0.0.1:8421/callback'],expires_at=int(time.time())+3600,event_ids=events or [])
    args.update(kw)
    return store.create(**args)


def test_two_independent_tenants_credentials_and_portfolio(store):
    a=create(store,'a',[1]);b=create(store,'b',[2]); ga=store.get(a['principal_id']);gb=store.get(b['principal_id'])
    assert not a['telegram_required'] and a['login_secret'] not in str(store.list())
    assert store.authenticate(a['client_id'],a['login_secret'])==ga
    assert store.resolve(identity(ga),event_id=1)==ga
    for invalid in (replace(identity(ga),audience='owner'),replace(identity(ga),client_id=gb.client_id),replace(identity(ga),subject=gb.subject)):
        with pytest.raises(ToolExecutionError): store.resolve(invalid,event_id=1)
    with pytest.raises(ToolExecutionError,match='Object not found'): store.resolve(identity(ga),event_id=2)
    with sqlite3.connect(store.path) as c:
        assert a['login_secret'] not in c.execute('SELECT secret_hash FROM mcp_partner_credential WHERE client_id=?',(ga.client_id,)).fetchone()[0]


def test_suspend_resume_rotate_revoke_invalidate_old_tokens(store):
    a=create(store,events=[1]);g=store.get(a['principal_id']);old=identity(g)
    assert store.change(g.principal_id,action='suspend',expected_revision=1)['status']=='suspended'
    with pytest.raises(ToolExecutionError):store.resolve(old)
    store.change(g.principal_id,action='resume',expected_revision=2)
    with pytest.raises(ToolExecutionError):store.resolve(old)
    current=store.get(g.principal_id);store.resolve(identity(current))
    rotated=store.change(g.principal_id,action='rotate',expected_revision=3)
    with pytest.raises(ToolExecutionError):store.authenticate(g.client_id,a['login_secret'])
    store.authenticate(g.client_id,rotated['login_secret'])
    with pytest.raises(ToolExecutionError):store.resolve(identity(current))
    store.change(g.principal_id,action='revoke',expected_revision=4)
    with pytest.raises(ToolExecutionError):store.change(g.principal_id,action='resume',expected_revision=5)


def test_policy_and_portfolio_are_resolved_not_token_claims(store):
    a=create(store,events=[1]);g=store.get(a['principal_id']);who=identity(g)
    store.change(g.principal_id,action='policy',expected_revision=1,policy={'scopes':['partner:events:read'],'actions':[]})
    with pytest.raises(ToolExecutionError):store.resolve(who,action='event_create')
    with pytest.raises(ToolExecutionError):store.resolve(who,scope='partner:events:propose')
    store.change(g.principal_id,action='portfolio',expected_revision=2,event_ids=[2])
    with pytest.raises(ToolExecutionError):store.resolve(who,event_id=1)
    assert store.resolve(who,event_id=2)
    with pytest.raises(ToolExecutionError):store.change(g.principal_id,action='suspend',expected_revision=1)


def test_portfolio_missing_event_rolls_back_entire_create(store):
    with pytest.raises(ToolExecutionError):create(store,events=[999])
    assert store.list()==[]


@pytest.mark.parametrize('action',['event_cancel','event_postpone','event_reschedule'])
def test_partner_cannot_self_approve_lifecycle(store,action):
    with pytest.raises(ToolExecutionError):create(store,policy={'scopes':[],'actions':[action],'auto_approve':[action]})


@pytest.mark.parametrize('uri',['http://evil.invalid/cb','https://evil.invalid/cb?next=x','https://user@evil.invalid/cb','file:///tmp/cb','javascript:alert(1)','https://evil.invalid/cb#x','http://127.0.0.1:08421/callback'])
def test_unsafe_redirects_rejected(store,uri):
    with pytest.raises(ToolExecutionError):create(store,redirect_uris=[uri])


@pytest.mark.parametrize('policy',[{'scopes':['operations:read']},{'actions':['owner']},{'limits':{'activities':0}},{'force':True}])
def test_unknown_or_escalating_policy_rejected(store,policy):
    with pytest.raises(ToolExecutionError):create(store,policy=policy)


@pytest.mark.asyncio
async def test_real_database_init_is_additive_and_repeatable(tmp_path):
    from db import Database
    database=Database(str(tmp_path/'actual.sqlite'))
    await database.init();await database.init()
    with sqlite3.connect(database.path) as conn:
        assert conn.execute('PRAGMA quick_check').fetchone()[0]=='ok'
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'mcp_partner%' AND type='table'").fetchone()[0]==3
