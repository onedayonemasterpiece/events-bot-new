from __future__ import annotations

import json
import sqlite3

import pytest

from db import Database
from smart_update_state import claim_due_candidates


@pytest.mark.asyncio
async def test_automatic_retry_claims_only_recent_active_identity_with_budget(tmp_path, monkeypatch):
    monkeypatch.setenv('DB_INIT_MINIMAL', '1')
    db = Database(str(tmp_path / 'identity-scope.sqlite'))
    await db.init()
    try:
        base = dict(reason='identity_final_retry', payload={'date': '2099-10-25'},
                    created='2099-10-22', attempts=1, outcome='RETRY_SCHEDULED',
                    claim=None, expiry=None, intent='UPSERT_EVENT')
        cases = {
            'future': {},
            'ongoing': {'payload': {'date': '2099-10-01', 'end_date': '2099-10-28'}},
            'historical_retry': {'created': '2099-08-01'},
            'past_event': {'payload': {'date': '2099-10-20'}},
            'no_date': {'payload': {}},
            'invalid_date': {'payload': {'date': 'not-a-date'}},
            'invalid_json': {'payload': 'not-json'},
            'exhausted': {'attempts': 3},
            'unknown_retry': {'reason': 'attempt_started'},
            'accepted': {'outcome': 'CREATED'},
            'product_reject': {'outcome': 'REJECTED_PRODUCT_POLICY'},
            'active_claim': {'claim': 'other', 'expiry': '2999-01-01'},
            'context': {'intent': 'ATTACH_CONTEXT'},
        }
        with sqlite3.connect(db.path) as conn:
            conn.execute("INSERT INTO event(id,title,description,date,time,location_name,source_text) VALUES(1,'Accepted','Body','2099-10-25','12:00','Museum','Source')")
            for key, overrides in cases.items():
                row = {**base, **overrides}
                payload = row['payload']
                conn.execute('''INSERT INTO smart_update_candidate_state(
                    candidate_key,occurrence_key,source_type,intent,source_fingerprint,
                    candidate_payload,current_outcome,reason,created_at,retry_attempts,
                    attempts,max_attempts,next_retry_at,claimed_by,claim_expires_at,accepted_event_id)
                    VALUES(?,?,'telegram',?,'fp',?,?,?,?,?,?,3,'2000-01-01',?,?,?)''',
                    (key, key, row['intent'], payload if isinstance(payload, str) else json.dumps(payload),
                     row['outcome'], row['reason'], row['created'], row['attempts'], row['attempts'],
                     row['claim'], row['expiry'], 1 if row['outcome'] == 'CREATED' else None))
            conn.commit()
        scope = dict(identity_created_since='2099-10-21', identity_active_on='2099-10-24')
        first = await claim_due_candidates(db, lease_owner='worker-1', limit=1, **scope)
        second = await claim_due_candidates(db, lease_owner='worker-2', limit=10, **scope)
        assert {x.candidate_key for x in first + second} == {'future', 'ongoing'}
        assert len(first) == 1 and len(second) == 1
        assert await claim_due_candidates(db, lease_owner='worker-3', **scope) == []
        with sqlite3.connect(db.path) as conn:
            claimed = {x[0] for x in conn.execute("SELECT candidate_key FROM smart_update_candidate_state WHERE claimed_by LIKE 'worker-%'")}
        assert claimed == {'future', 'ongoing'}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_automatic_retry_requires_complete_scope():
    with pytest.raises(ValueError, match='identity_retry_scope_incomplete'):
        await claim_due_candidates(object(), lease_owner='worker', identity_created_since='2099-10-21')


@pytest.mark.asyncio
async def test_retry_facade_passes_scope_without_changing_semantic_result(monkeypatch):
    import smart_event_update as su
    received = {}
    async def claim(_db, **kwargs):
        received.update(kwargs)
        return []
    monkeypatch.setattr(su, 'claim_due_candidates', claim)
    result = await su.retry_due_smart_update_candidates(
        object(), limit=1, identity_created_since='2099-10-21', identity_active_on='2099-10-24')
    assert received['identity_created_since'] == '2099-10-21'
    assert received['identity_active_on'] == '2099-10-24'
    assert result['claimed'] == 0
