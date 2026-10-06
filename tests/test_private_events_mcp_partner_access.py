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
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'mcp_partner%' AND type='table'").fetchone()[0]==5


def test_durable_actor_policy_does_not_fabricate_token_and_tracks_epoch(store):
    a=create(store, 'a', [1]); b=create(store, 'b', [2])
    ga=store.get(a['principal_id']); gb=store.get(b['principal_id'])
    args=dict(actor_subject=ga.subject, actor_client_id=ga.client_id, actor_audience=RESOURCE,
              scope='partner:events:propose', action='event_create')
    assert store.resolve_durable(**args)==ga
    for patch in ({'actor_subject': gb.subject}, {'actor_client_id': gb.client_id},
                  {'actor_audience': 'owner'}, {'event_id': 2}, {'event_id': 1.9},
                  {'scope': 'events:write'}, {'action': None}):
        with pytest.raises(ToolExecutionError):
            store.resolve_durable(**{**args, **patch})
    store.change(ga.principal_id, action='rotate', expected_revision=ga.policy_revision)
    with pytest.raises(ToolExecutionError):
        store.resolve_durable(**args)
    assert store.resolve(identity(gb), event_id=2)==gb


def test_durable_actor_rechecks_current_scope_and_live_identity_keeps_token_scope(store):
    a=create(store); grant=store.get(a['principal_id'])
    args=dict(actor_subject=grant.subject, actor_client_id=grant.client_id, actor_audience=RESOURCE,
              scope='partner:events:propose', action='event_create')
    with pytest.raises(ToolExecutionError):
        store.resolve(replace(identity(grant), scopes=frozenset()), scope='partner:events:propose')
    assert store.resolve_durable(**args)==grant
    store.change(grant.principal_id, action='policy', expected_revision=grant.policy_revision,
                 policy={'scopes':['partner:events:read'], 'actions':['event_create']})
    with pytest.raises(ToolExecutionError):
        store.resolve_durable(**args)


def authority(kind, subject_type, key, name, *, aliases=None, cities=None):
    value = {
        'authority_kind': kind,
        'subject_type': subject_type,
        'subject_key': key,
        'display_name': name,
    }
    if aliases is not None:
        value['aliases'] = aliases
    if cities is not None:
        value['cities'] = cities
    return value


def test_authority_bindings_are_server_owned_revisioned_and_exact(store):
    created = create(store, authorities=[
        authority('venue_operator', 'venue', 'amber-hall', 'Янтарь-холл',
                  aliases=['Янтарь холл'], cities=['Светлогорск']),
        authority('festival_operator', 'festival', 'kantata', 'Кантата'),
    ])
    grant = store.get(created['principal_id'])
    assert [item['authority_kind'] for item in store.authorities(grant)] == [
        'festival_operator', 'venue_operator'
    ]
    first_revision = grant.policy_revision
    changed = store.change(
        grant.principal_id,
        action='authorities',
        expected_revision=first_revision,
        authorities=[authority('organizer', 'organization', 'dramtheatre', 'Драмтеатр')],
    )
    assert changed['policy_revision'] == first_revision + 1
    assert changed['authorities'][0]['authority_kind'] == 'organizer'
    with pytest.raises(ToolExecutionError):
        store.change(
            grant.principal_id,
            action='authorities',
            expected_revision=changed['policy_revision'],
        )


@pytest.mark.parametrize('bad', [
    [authority('venue_operator', 'festival', 'bad', 'Bad')],
    [authority('unknown', 'venue', 'bad', 'Bad')],
    [{'authority_kind':'venue_operator','subject_type':'venue','subject_key':'bad','display_name':'Bad','unknown':True}],
])
def test_invalid_authority_shape_fails_closed(store, bad):
    with pytest.raises(ToolExecutionError):
        create(store, authorities=bad)


def test_authority_candidate_matching_is_exact_and_role_specific(store):
    from types import SimpleNamespace

    created = create(store, authorities=[
        authority('venue_operator', 'venue', 'amber-hall', 'Янтарь-холл',
                  aliases=['Янтарь холл'], cities=['Светлогорск']),
        authority('organizer', 'organization', 'dramtheatre', 'Драмтеатр'),
        authority('festival_operator', 'festival', 'kantata', 'Кантата'),
        authority('represented_person', 'person', 'ivanov', 'Иван Иванов'),
        authority('represented_collective', 'collective', 'choir', 'Камерный хор'),
        authority('series_operator', 'series', 'city-lectures', 'Городские лекции'),
        authority('programme_operator', 'programme', 'kantata-education', 'Кантата: образование'),
    ])
    grant = store.get(created['principal_id'])

    def candidate(**values):
        base = dict(
            location_name='Чужой зал', city='Калининград',
            organizer_names=[], festival=None, festival_full=None,
            festival_series=None, authority_series_names=[],
            collection_semantic_decisions=None,
        )
        base.update(values)
        return SimpleNamespace(**base)

    assert store.evaluate_create_candidate(
        grant, candidate(location_name='ЯНТАРЬ ХОЛЛ', city='Светлогорск')
    )['matched_authority']['authority_kind'] == 'venue_operator'
    assert store.evaluate_create_candidate(
        grant, candidate(organizer_names=['Драмтеатр'])
    )['matched_authority']['authority_kind'] == 'organizer'
    assert store.evaluate_create_candidate(
        grant, candidate(festival='Кантата')
    )['matched_authority']['authority_kind'] == 'festival_operator'
    assert store.evaluate_create_candidate(
        grant, candidate(authority_series_names=['Городские лекции'])
    )['matched_authority']['authority_kind'] == 'series_operator'
    people = {'people_appearances': [
        {'name':'Иван Иванов','role':'speaker','appearance':'confirmed'},
    ]}
    assert store.evaluate_create_candidate(
        grant, candidate(collection_semantic_decisions=people)
    )['matched_authority']['authority_kind'] == 'represented_person'
    collective = {'people_appearances': [
        {'name':'Камерный хор','role':'performer','appearance':'confirmed'},
    ]}
    assert store.evaluate_create_candidate(
        grant, candidate(collection_semantic_decisions=collective)
    )['matched_authority']['authority_kind'] == 'represented_collective'

    # No substring/fuzzy authority, wrong city, or unconfirmed person matching.
    assert store.evaluate_create_candidate(
        grant, candidate(location_name='Большой зал Янтарь-холла', city='Светлогорск')
    )['status'] == 'review_required'
    assert store.evaluate_create_candidate(
        grant, candidate(location_name='Янтарь-холл', city='Калининград')
    )['status'] == 'review_required'
    assert store.evaluate_create_candidate(
        grant, candidate(collection_semantic_decisions={'people_appearances':[
            {'name':'Иван Иванов','role':'speaker','appearance':'mentioned'},
        ]})
    )['status'] == 'review_required'
    # Programme authority is deliberately review-only until programme evidence is structured.
    only_programme = create(store, org='programme', authorities=[
        authority('programme_operator', 'programme', 'edu', 'Образовательная программа'),
    ])
    programme_grant = store.get(only_programme['principal_id'])
    result = store.evaluate_create_candidate(programme_grant, candidate())
    assert result['status'] == 'review_required'
    assert result['reason'] == 'programme_scope_requires_structured_evidence'
