import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from private_events_mcp.event_create import EventCreateRequest
from private_events_mcp.partner_access import PartnerAccessStore, SCHEMA
from private_events_mcp.partner_accepted_event import assign_accepted_event
from private_events_mcp.tool_catalog import ToolExecutionError


@pytest.fixture
def store(tmp_path):
    path = tmp_path / 'canonical.sqlite'
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA)
        conn.execute('CREATE TABLE event(id INTEGER PRIMARY KEY,title TEXT)')
        conn.execute(
            'CREATE TABLE event_change_log('
            'operation_ref TEXT PRIMARY KEY, action_digest TEXT, actor_subject TEXT, '
            'actor_client_id TEXT, actor_audience TEXT, domain_receipt_json TEXT, organizer_comment TEXT)'
        )
        conn.execute("INSERT INTO event VALUES(42,'Canonical actual event')")
    return PartnerAccessStore(path, resource='https://events.test/partner', signing_key='local-test-key')


def partner(store, *, tenant='tenant-one', org='org-one'):
    data = store.create(tenant_id=tenant, organization_id=org, display_name='Partner',
        policy={'scopes': ['partner:events:propose'], 'actions': ['event_create']},
        redirect_uris=['http://127.0.0.1:8421/callback'], expires_at=int(time.time()) + 3600)
    grant = store.get(data['principal_id'])
    request = EventCreateRequest(raw_text='An actual event text', source_url=None,
        source_external_id='source-42', source_locator='mcp-partner:source-42',
        idempotency_key='idempotency-key-42', text_policy='smart_rewrite',
        actor_subject=grant.subject, actor_client_id=grant.client_id, actor_audience=store.resource)
    return grant, request


def result(provenance='created', event_id=42):
    return {'status': 'accepted', 'event_ids': [event_id],
            'events': [{'event_id': event_id, 'result': provenance}]}


def rows(store):
    with store._connect() as conn:
        return [tuple(row) for row in conn.execute('SELECT principal_id,tenant_id,organization_id,event_id FROM mcp_partner_event')]


def test_created_assignment_and_restart_replay(store):
    grant, request = partner(store)
    first = assign_accepted_event(store, request, result())
    assert first['assigned']
    reopened = PartnerAccessStore(store.path, resource=store.resource, signing_key=store.signing_key)
    replay = assign_accepted_event(reopened, request, result('merged_or_replay'))
    assert not replay['assigned']
    assert rows(store) == [(grant.principal_id, grant.tenant_id, grant.organization_id, 42)]
    with store._connect() as conn:
        assert conn.execute('SELECT title FROM event WHERE id=42').fetchone()[0] == 'Canonical actual event'


@pytest.mark.parametrize('invalid', [
    {'status': 'failed', 'event_ids': [42]}, {'status': 'accepted', 'event_ids': []},
    {'status': 'accepted', 'event_ids': [42, 43]}, result(event_id=True), result(event_id=0),
    result(event_id='42'), result(event_id=2**63), {'status': 'accepted', 'event_ids': [42]},
    {'status': 'accepted', 'event_ids': [42], 'events': [{'event_id': 43, 'result': 'created'}]},
    result('unknown'),
])
def test_invalid_result_never_grants(store, invalid):
    _, request = partner(store)
    with pytest.raises(ToolExecutionError):
        assign_accepted_event(store, request, invalid)
    assert rows(store) == []


def test_absent_canonical_event_never_grants(store):
    _, request = partner(store)
    with pytest.raises(ToolExecutionError):
        assign_accepted_event(store, request, result(event_id=99))
    assert rows(store) == []


@pytest.mark.parametrize('tenant,org', [('foreign', 'org-one'), ('tenant-one', 'foreign')])
def test_foreign_tenant_or_organization_never_grants_even_created(store, tenant, org):
    foreign, foreign_request = partner(store, tenant=tenant, org=org)
    assign_accepted_event(store, foreign_request, result())
    _, request = partner(store)
    with pytest.raises(ToolExecutionError):
        assign_accepted_event(store, request, result())
    assert len(rows(store)) == 1 and rows(store)[0][0] == foreign.principal_id


def test_unowned_or_same_org_merge_requires_owner_assignment(store):
    _, first = partner(store)
    with pytest.raises(ToolExecutionError):
        assign_accepted_event(store, first, result('merged_or_replay'))
    assign_accepted_event(store, first, result())
    _, second = partner(store)
    with pytest.raises(ToolExecutionError):
        assign_accepted_event(store, second, result('merged_or_replay'))
    assert len(rows(store)) == 1


@pytest.mark.parametrize('change', ['suspend', 'revoke', 'rotate', 'scope', 'action', 'expire'])
def test_current_policy_rechecked_in_assignment_transaction(store, monkeypatch, change):
    grant, request = partner(store)
    if change in {'scope', 'action'}:
        store.change(grant.principal_id, action='policy', expected_revision=1,
            policy={'scopes': [] if change == 'scope' else ['partner:events:propose'],
                    'actions': [] if change == 'action' else ['event_create']})
    elif change == 'expire':
        monkeypatch.setattr('private_events_mcp.partner_access.time.time', lambda: grant.expires_at)
    else:
        store.change(grant.principal_id, action=change, expected_revision=1)
    with pytest.raises(ToolExecutionError):
        assign_accepted_event(store, request, result())
    assert rows(store) == []


def test_actor_client_resource_and_epoch_are_exact(store):
    _, request = partner(store)
    for bad in (replace(request, actor_client_id='other'), replace(request, actor_audience='other'),
                replace(request, actor_subject=request.actor_subject.rsplit(':', 1)[0] + ':99')):
        with pytest.raises(ToolExecutionError):
            assign_accepted_event(store, bad, result())
    assert rows(store) == []


def test_concurrent_replay_and_cross_tenant_race(store):
    _, request = partner(store)
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: assign_accepted_event(store, request, result()), range(2)))
    assert sorted(item['assigned'] for item in outcomes) == [False, True]
    with store._connect() as conn:
        conn.execute('DELETE FROM mcp_partner_event')
    _, foreign_request = partner(store, tenant='foreign')

    def assign(req):
        try:
            return assign_accepted_event(store, req, result())
        except ToolExecutionError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(assign, [request, foreign_request]))
    assert sum(isinstance(item, ToolExecutionError) for item in outcomes) == 1
    assert len(rows(store)) == 1


def proofed_request(store, *, tenant, org, operation_char):
    data = store.create(
        tenant_id=tenant,
        organization_id=org,
        display_name='Partner '+org,
        policy={'scopes':['partner:events:propose'], 'actions':['event_create']},
        authorities=[{
            'authority_kind':'venue_operator',
            'subject_type':'venue',
            'subject_key':'shared-hall',
            'display_name':'Shared Hall',
        }],
        redirect_uris=['http://127.0.0.1:8421/callback'],
        expires_at=int(time.time()) + 3600,
    )
    grant = store.get(data['principal_id'])
    operation_ref = 'evt_op_' + operation_char * 24
    request = EventCreateRequest(
        raw_text='An actual event text',
        source_url=None,
        source_external_id='source-'+operation_char,
        source_locator='mcp-partner:source-'+operation_char,
        idempotency_key='idempotency-'+operation_char*8,
        text_policy='smart_rewrite',
        actor_subject=grant.subject,
        actor_client_id=grant.client_id,
        actor_audience=store.resource,
        partner_policy_revision=grant.policy_revision,
        partner_authority_kinds=('venue_operator',),
        _operation_ref=operation_ref,
    )
    proof = json.dumps({
        'partner_authority_status':'matched',
        'partner_authority_kind':'venue_operator',
    })
    with store._connect() as conn:
        conn.execute(
            'INSERT INTO event_change_log VALUES(?,?,?,?,?,?,NULL)',
            (
                operation_ref, request.action_digest, request.actor_subject,
                request.actor_client_id, request.actor_audience, proof,
            ),
        )
    return grant, request


def test_durable_authority_allows_shared_cross_tenant_event_portfolios(store):
    first, first_request = proofed_request(
        store, tenant='tenant-a', org='org-a', operation_char='a'
    )
    second, second_request = proofed_request(
        store, tenant='tenant-b', org='org-b', operation_char='b'
    )
    assert assign_accepted_event(store, first_request, result())['assigned']
    # The second partner discovers the already-canonical event via Smart Update;
    # its own durable authority proof allows a separate portfolio assignment.
    assert assign_accepted_event(
        store, second_request, result('merged_or_replay')
    )['assigned']
    assert sorted(rows(store)) == sorted([
        (first.principal_id, first.tenant_id, first.organization_id, 42),
        (second.principal_id, second.tenant_id, second.organization_id, 42),
    ])


def test_shared_merge_without_durable_authority_proof_still_fails(store):
    first, first_request = proofed_request(
        store, tenant='tenant-a', org='org-a', operation_char='c'
    )
    assert assign_accepted_event(store, first_request, result())['assigned']
    _, legacy = partner(store, tenant='tenant-b', org='org-b')
    with pytest.raises(ToolExecutionError):
        assign_accepted_event(store, legacy, result('merged_or_replay'))
    assert len(rows(store)) == 1
