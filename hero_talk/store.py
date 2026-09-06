"""Draft/version ownership in canonical SQLite, never campaign activation.

This feature-specific history is not a second queue or authorization system.
The host must inject current authorization on the caller's exact connection.
No public content, model/provider work or live permit is produced here.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import secrets
import time
from typing import Any

import aiosqlite

from .compiler import PROGRAM_FIELDS, ORIGINS, _id, _json, _object

SCHEMA = """
CREATE TABLE IF NOT EXISTS hero_talk_program (
    program_id TEXT PRIMARY KEY,
    origin TEXT NOT NULL,
    campaign_id INTEGER,
    activity_id INTEGER,
    desired_revision INTEGER NOT NULL DEFAULT 0,
    active_revision INTEGER,
    status TEXT NOT NULL DEFAULT 'draft',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS hero_talk_change_log (
    operation_ref TEXT PRIMARY KEY,
    program_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    actor_subject TEXT NOT NULL,
    actor_client_id TEXT NOT NULL,
    actor_audience TEXT NOT NULL,
    idempotency_hash TEXT NOT NULL,
    action_digest TEXT NOT NULL,
    request_json JSON NOT NULL,
    base_revision INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'prepared',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(actor_subject, actor_client_id, actor_audience, idempotency_hash),
    FOREIGN KEY(program_id) REFERENCES hero_talk_program(program_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_hero_stored_revision
    ON hero_talk_change_log(program_id, revision) WHERE status='draft_stored';
"""


class HeroStoreError(ValueError):
    pass


@dataclass(frozen=True)
class HeroActor:
    subject: str
    client_id: str
    audience: str

    def checked(self):
        if any(not isinstance(v,str) or not 1<=len(v)<=2048 or any(ord(c)<32 for c in v)
               for v in (self.subject,self.client_id,self.audience)):
            raise HeroStoreError('actor_invalid')
        return self


class HeroProgramStore:
    def __init__(self, database: Any, *, authorize, clock=time.time):
        if not callable(authorize):
            raise ValueError('current authorization callback required')
        self.database=database
        self.authorize=authorize
        self.clock=clock

    async def _allowed(self, conn, actor, action, program):
        actor.checked()
        if await self.authorize(conn,actor,action,program) is not True:
            raise HeroStoreError('access_denied')

    async def put_draft(self, program: dict, *, expected_revision: int,
                        actor: HeroActor, idempotency_key: str) -> dict:
        """Internal convenience; prepare and commit the same exact draft intent."""
        prepared=await self.prepare_draft(program,expected_revision=expected_revision,
                                          actor=actor,idempotency_key=idempotency_key)
        return await self.commit_draft(prepared['operation_ref'],action_digest=prepared['action_digest'],actor=actor)

    async def prepare_draft(self, program: dict, *, expected_revision: int,
                            actor: HeroActor, idempotency_key: str) -> dict:
        """Persist immutable bounded preparation, without changing desired/active revision."""
        actor.checked()
        _object(program,PROGRAM_FIELDS,PROGRAM_FIELDS-{'campaign_binding'})
        program_id=_id(program['program_id'])
        if type(expected_revision) is not int or not 0<=expected_revision<2**63-1:
            raise HeroStoreError('base_revision_invalid')
        if type(program['revision']) is not int or program['revision']!=expected_revision+1:
            raise HeroStoreError('desired_revision_invalid')
        if program['origin'] not in ORIGINS:
            raise HeroStoreError('origin_invalid')
        campaign=program.get('campaign_binding')
        if program['origin']=='promo_campaign':
            _object(campaign,{'campaign_id','activity_id'},{'campaign_id','activity_id'})
            if any(type(v) is not int or not 1<=v<=2**63-1 for v in campaign.values()):
                raise HeroStoreError('campaign_binding_invalid')
        elif campaign is not None:
            raise HeroStoreError('campaign_origin_mismatch')
        campaign=campaign or {}
        request_bytes=_json(program)
        if len(request_bytes)>64*1024:
            raise HeroStoreError('draft_too_large')
        if not isinstance(idempotency_key,str) or not 8<=len(idempotency_key)<=160:
            raise HeroStoreError('idempotency_key_invalid')
        key_hash=hashlib.sha256(idempotency_key.encode()).hexdigest()
        digest=hashlib.sha256(_json({'program':program,'expected_revision':expected_revision})).hexdigest()
        async with self.database.raw_conn() as conn:
            conn.row_factory=aiosqlite.Row
            await conn.execute('BEGIN IMMEDIATE')
            try:
                current=await (await conn.execute('SELECT * FROM hero_talk_program WHERE program_id=?',(program_id,))).fetchone()
                # Current policy is checked even for an idempotent replay.
                await self._allowed(conn,actor,'upsert_draft',dict(current) if current else program)
                prior=await (await conn.execute(
                    'SELECT operation_ref,program_id,revision,action_digest,status,expires_at FROM hero_talk_change_log '
                    'WHERE actor_subject=? AND actor_client_id=? AND actor_audience=? AND idempotency_hash=?',
                    (actor.subject,actor.client_id,actor.audience,key_hash))).fetchone()
                if prior is not None:
                    if prior['action_digest']!=digest:
                        raise HeroStoreError('idempotency_conflict')
                    await conn.commit()
                    return dict(prior)
                if current is None:
                    if expected_revision!=0:
                        raise HeroStoreError('revision_conflict')
                    await conn.execute(
                        'INSERT INTO hero_talk_program(program_id,origin,campaign_id,activity_id) VALUES(?,?,?,?)',
                        (program_id,program['origin'],campaign.get('campaign_id'),campaign.get('activity_id')))
                elif (current['desired_revision']!=expected_revision or current['status']=='archived'):
                    raise HeroStoreError('revision_conflict')
                elif (current['origin']!=program['origin'] or current['campaign_id']!=campaign.get('campaign_id')
                      or current['activity_id']!=campaign.get('activity_id')):
                    raise HeroStoreError('immutable_origin_conflict')
                operation_ref='hero_op_'+secrets.token_urlsafe(24)
                expires_at=int(self.clock())+600
                await conn.execute(
                    'INSERT INTO hero_talk_change_log(operation_ref,program_id,revision,actor_subject,actor_client_id,'
                    'actor_audience,idempotency_hash,action_digest,request_json,base_revision,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (operation_ref,program_id,program['revision'],actor.subject,actor.client_id,actor.audience,
                     key_hash,digest,request_bytes.decode(),expected_revision,expires_at))
                await conn.commit()
                return {'operation_ref':operation_ref,'program_id':program_id,'revision':program['revision'],
                        'action_digest':digest,'status':'prepared','expires_at':expires_at}
            except BaseException:
                await conn.rollback()
                raise

    async def _operation(self, conn, operation_ref, actor):
        if not isinstance(operation_ref,str) or not operation_ref.startswith('hero_op_') or len(operation_ref)>120:
            raise HeroStoreError('operation_reference_invalid')
        actor.checked()
        row=await (await conn.execute(
            'SELECT * FROM hero_talk_change_log WHERE operation_ref=? AND actor_subject=? AND actor_client_id=? AND actor_audience=?',
            (operation_ref,actor.subject,actor.client_id,actor.audience))).fetchone()
        if row is None:
            raise HeroStoreError('operation_not_found')
        return row

    @staticmethod
    def _public_operation(row):
        return {key:row[key] for key in ('operation_ref','program_id','revision','action_digest','status')}

    async def commit_draft(self, operation_ref: str, *, action_digest: str, actor: HeroActor) -> dict:
        """Apply only the persisted preparation under current policy and revision CAS."""
        async with self.database.raw_conn() as conn:
            conn.row_factory=aiosqlite.Row
            await conn.execute('BEGIN IMMEDIATE')
            try:
                row=await self._operation(conn,operation_ref,actor)
                current=await (await conn.execute('SELECT * FROM hero_talk_program WHERE program_id=?',(row['program_id'],))).fetchone()
                await self._allowed(conn,actor,'upsert_draft',dict(current) if current else None)
                if row['action_digest']!=action_digest:
                    raise HeroStoreError('preparation_digest_conflict')
                if row['status']=='draft_stored':
                    await conn.commit()
                    return self._public_operation(row)
                if row['status']!='prepared':
                    raise HeroStoreError('preparation_not_committable')
                if row['expires_at']<=int(self.clock()):
                    raise HeroStoreError('preparation_expired')
                if current is None or current['status']=='archived' or current['desired_revision']!=row['base_revision']:
                    raise HeroStoreError('revision_conflict')
                program=json.loads(row['request_json'])
                digest=hashlib.sha256(_json({'program':program,'expected_revision':row['base_revision']})).hexdigest()
                campaign=program.get('campaign_binding') or {}
                if (digest!=row['action_digest'] or program['program_id']!=current['program_id']
                        or program['revision']!=row['revision'] or program['origin']!=current['origin']
                        or campaign.get('campaign_id')!=current['campaign_id'] or campaign.get('activity_id')!=current['activity_id']):
                    raise HeroStoreError('preparation_corrupt')
                updated=await conn.execute(
                    'UPDATE hero_talk_program SET desired_revision=?,updated_at=CURRENT_TIMESTAMP WHERE program_id=? AND desired_revision=?',
                    (row['revision'],row['program_id'],row['base_revision']))
                if updated.rowcount!=1:
                    raise HeroStoreError('revision_conflict')
                await conn.execute("UPDATE hero_talk_change_log SET status='draft_stored' WHERE operation_ref=? AND status='prepared'",(operation_ref,))
                await conn.commit()
                return {**self._public_operation(row),'status':'draft_stored'}
            except BaseException:
                await conn.rollback()
                raise

    async def operation_get(self, operation_ref: str, *, actor: HeroActor) -> dict:
        async with self.database.raw_conn() as conn:
            conn.row_factory=aiosqlite.Row
            await conn.execute('BEGIN')
            try:
                row=await self._operation(conn,operation_ref,actor)
                current=await (await conn.execute('SELECT * FROM hero_talk_program WHERE program_id=?',(row['program_id'],))).fetchone()
                await self._allowed(conn,actor,'read',dict(current) if current else None)
                result=self._public_operation(row)
                if row['status']=='prepared':
                    if row['expires_at']<=int(self.clock()):
                        result.update(status='blocked',reason='preparation_expired')
                    elif current is None or current['desired_revision']!=row['base_revision'] or current['status']=='archived':
                        result.update(status='superseded',reason='revision_conflict')
                return result
            finally:
                await conn.rollback()

    async def get(self, program_id: str, *, actor: HeroActor) -> dict:
        program_id=_id(program_id)
        async with self.database.raw_conn() as conn:
            conn.row_factory=aiosqlite.Row
            # Policy + program/history share a single read snapshot.
            await conn.execute('BEGIN')
            try:
                current=await (await conn.execute('SELECT * FROM hero_talk_program WHERE program_id=?',(program_id,))).fetchone()
                await self._allowed(conn,actor,'read',dict(current) if current else {'program_id':program_id})
                if current is None:
                    raise HeroStoreError('program_not_found')
                draft=await (await conn.execute(
                    "SELECT request_json FROM hero_talk_change_log WHERE program_id=? AND revision=? AND status='draft_stored'",
                    (program_id,current['desired_revision']))).fetchone()
                result=dict(current)
                result['draft']=json.loads(draft['request_json']) if draft else None
                return result
            finally:
                await conn.rollback()
