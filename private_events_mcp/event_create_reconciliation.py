"""Recover only operation-specific transactional domain acceptance; never parse again."""
from __future__ import annotations

import json
import logging
import re

import aiosqlite

from .event_create import EventCreateRequest, parse_event_images

logger = logging.getLogger(__name__)

_FIELDS = {'schema','operation_ref','action_digest','actor_subject','actor_client_id','actor_audience',
           'event_id','effect','candidate_key','occurrence_key','candidate_state_id','attempt_no','source_fingerprint'}


def restore_request(row):
    data = json.loads(row['request_json'])
    if data.get('schema') != 'events-mcp-owner-create-r1':
        raise ValueError('unsupported request schema')
    request = EventCreateRequest(raw_text=data['raw_text'],source_url=data['source_url'],
        source_external_id=data['source_external_id'],source_locator=data['source_locator'],
        idempotency_key='',text_policy=data['text_policy'],actor_subject=row['actor_subject'],
        actor_client_id=row['actor_client_id'],actor_audience=row['actor_audience'],
        _persisted_idempotency_hash=row['idempotency_hash'],media=parse_event_images(data.get('media')),
        partner_policy_revision=data.get('partner_policy_revision'))
    if request.action_digest != row['action_digest']:
        raise ValueError('request digest mismatch')
    return request


def verified_receipt(row, request):
    raw = row['domain_receipt_json']
    if not isinstance(raw,str) or len(raw)>8192:
        raise ValueError('missing bounded domain receipt')
    receipt=json.loads(raw)
    if not isinstance(receipt,dict) or set(receipt)-(_FIELDS|{'partner_policy_revision'}) or _FIELDS-set(receipt):
        raise ValueError('invalid domain receipt schema')
    if receipt['schema']!='event-operation-domain-receipt-v1':
        raise ValueError('unsupported domain receipt')
    for key in ('operation_ref','action_digest','actor_subject','actor_client_id','actor_audience'):
        if receipt[key]!=row[key]:
            raise ValueError('receipt identity mismatch')
    if request.actor_subject.startswith('partner:') and (isinstance(request.partner_policy_revision,bool)
            or not isinstance(request.partner_policy_revision,int) or request.partner_policy_revision < 1):
        raise ValueError('missing partner revision')
    if receipt.get('partner_policy_revision') != request.partner_policy_revision:
        raise ValueError('receipt policy mismatch')
    for key in ('event_id','candidate_state_id','attempt_no'):
        value=receipt[key]
        if isinstance(value,bool) or not isinstance(value,int) or not 1<=value<=2**63-1:
            raise ValueError('invalid receipt identifier')
    if receipt['effect'] not in {'created','merged','noop_exact_replay'}:
        raise ValueError('invalid domain effect')
    if not isinstance(receipt['source_fingerprint'],str) or not re.fullmatch('[a-f0-9]{64}',receipt['source_fingerprint']):
        raise ValueError('invalid packet fingerprint')
    for key in ('candidate_key','occurrence_key'):
        if not isinstance(receipt[key],str) or not 1<=len(receipt[key])<=512:
            raise ValueError('invalid candidate identity')
    return receipt


class EventCreateReconciler:
    def __init__(self, runtime):
        if not callable(runtime.authorize):
            raise ValueError('current durable authorization is required')
        self.runtime=runtime
        self.database=runtime.store.database

    async def recover(self, *, limit=25):
        if isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=100:
            raise ValueError('invalid reconciliation limit')
        async with self.database.raw_conn() as conn:
            columns={r[1] for r in await (await conn.execute("PRAGMA table_info(event_change_log)")).fetchall()}
            if 'domain_receipt_json' not in columns:
                return 0  # Previous schema: no receipt can prove an unknown result.
            conn.row_factory=aiosqlite.Row
            cursor=await conn.execute(
                "SELECT * FROM event_change_log WHERE operation_kind='create' AND domain_receipt_json IS NOT NULL "
                "AND (status='outcome_unknown' OR (status='processing' AND started_at<=datetime('now','-30 minutes'))) "
                "ORDER BY updated_at,operation_ref LIMIT ?",(limit,))
            rows=await cursor.fetchall()
        recovered=0
        for row in rows:
            if row['operation_ref'] in self.runtime._tasks:
                continue
            try:
                request=restore_request(row)
                receipt=verified_receipt(row,request)
                if await self.runtime.authorize(request) is not True:
                    continue
                async with self.database.raw_conn() as conn:
                    event=await (await conn.execute('SELECT id,identity_status,merged_into_event_id FROM event WHERE id=?', (receipt['event_id'],))).fetchone()
                if event is None or event[1] not in (None,'canonical') or event[2] is not None:
                    continue
                result={'status':'accepted','event_ids':[receipt['event_id']],
                        'events':[{'event_id':receipt['event_id'], 'result':'created' if receipt['effect']=='created' else 'merged_or_replay'}],
                        'domain_recovery':{'proof':'transactional_operation_receipt','effect':receipt['effect']},
                        'jobs':[], 'jobs_scope':'not_reconstructed_by_domain_recovery',
                        'publication_state':'reconciliation_required'}
                if self.runtime.on_accepted is not None:
                    await self.runtime.on_accepted(request,result)
                if await self.runtime.authorize(request) is not True:
                    continue
                # Compare-and-set: never overwrite another terminal decision or receipt.
                async with self.database.raw_conn() as conn:
                    cursor=await conn.execute(
                        "UPDATE event_change_log SET status='accepted',event_id=?,result_json=?,error_code=NULL,"
                        "updated_at=CURRENT_TIMESTAMP,completed_at=CURRENT_TIMESTAMP WHERE operation_ref=? "
                        "AND action_digest=? AND status=? AND domain_receipt_json=?",
                        (receipt['event_id'],json.dumps(result,sort_keys=True),row['operation_ref'],
                         row['action_digest'],row['status'],row['domain_receipt_json']))
                    await conn.commit()
                    recovered+=int(cursor.rowcount==1)
            except Exception as exc:
                # Absence/conflict/corruption/revocation is not proof of non-acceptance.
                # Preserve unknown; no parser, Event, provider or publication writes.
                logger.warning("event_create domain recovery deferred operation_ref=%s error_type=%s",
                               row["operation_ref"], type(exc).__name__)
                continue
        return recovered
