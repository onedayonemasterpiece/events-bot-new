"""Canonical draft/CAS tests; no deployed tools, publication or fake OAuth token."""
import asyncio
from copy import deepcopy
import json

import pytest

from db import Database
from hero_talk.store import HeroActor, HeroProgramStore, HeroStoreError
from test_hero_talk_compiler import fixture

ACTOR=HeroActor('events-bot-owner','fixture-client','https://fixture.test/mcp')


async def setup(tmp_path):
    db=Database(str(tmp_path/'hero.sqlite'))
    await db.init()
    async def allowed(conn,actor,action,program):
        assert conn is not None and actor==ACTOR
        return True
    return db,HeroProgramStore(db,authorize=allowed)


@pytest.mark.asyncio
async def test_draft_is_durable_idempotent_and_never_active(tmp_path):
    db,store=await setup(tmp_path);program,_=fixture()
    first=await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
    assert first==await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
    await db.close()
    db=Database(str(tmp_path/'hero.sqlite'));await db.init()
    store=HeroProgramStore(db,authorize=store.authorize)
    try:
        read=await store.get(program['program_id'],actor=ACTOR)
        assert read['desired_revision']==1 and read['active_revision'] is None and read['status']=='draft'
        assert read['draft']==program
        async with db.raw_conn() as conn:
            assert (await (await conn.execute('SELECT COUNT(*) FROM hero_talk_change_log')).fetchone())[0]==1
            assert (await (await conn.execute('SELECT COUNT(*) FROM promo_campaign')).fetchone())[0]==0
            stored=await (await conn.execute('SELECT idempotency_hash,request_json FROM hero_talk_change_log')).fetchone()
        assert stored[0]!='fixture-first' and 'fixture-client' not in stored[1]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_concurrent_desired_revision_cas_has_one_winner(tmp_path):
    db,store=await setup(tmp_path);program,_=fixture()
    try:
        first=await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
        second={**program,'revision':2}
        outcomes=await asyncio.gather(*[store.put_draft(second,expected_revision=1,actor=ACTOR,
                    idempotency_key=f'fixture-concurrent-{i}') for i in range(2)],return_exceptions=True)
        assert sum(isinstance(x,dict) for x in outcomes)==1
        assert sum(isinstance(x,HeroStoreError) and str(x)=='revision_conflict' for x in outcomes)==1
        assert (await store.get(program['program_id'],actor=ACTOR))['desired_revision']==2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_policy_revocation_denies_replay_read_and_new_revision(tmp_path):
    db,store=await setup(tmp_path);program,_=fixture()
    try:
        await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
        async def denied(*args): return False
        store.authorize=denied
        with pytest.raises(HeroStoreError,match='access_denied'):
            await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
        with pytest.raises(HeroStoreError,match='access_denied'):
            await store.get(program['program_id'],actor=ACTOR)
        with pytest.raises(HeroStoreError,match='access_denied'):
            await store.put_draft({**program,'revision':2},expected_revision=1,actor=ACTOR,idempotency_key='fixture-second')
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_failed_authorization_leaves_no_partial_rows(tmp_path):
    db,store=await setup(tmp_path);program,_=fixture()
    async def broken(conn,*args):
        # Same connection is already under write ownership; no provider or model.
        assert conn.in_transaction
        raise RuntimeError('policy unavailable')
    store.authorize=broken
    try:
        with pytest.raises(RuntimeError,match='policy unavailable'):
            await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
        async with db.raw_conn() as conn:
            assert (await (await conn.execute('SELECT COUNT(*) FROM hero_talk_program')).fetchone())[0]==0
            assert (await (await conn.execute('SELECT COUNT(*) FROM hero_talk_change_log')).fetchone())[0]==0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_history_preserved_pause_not_resumed_and_campaign_cannot_be_relabelled(tmp_path):
    db,store=await setup(tmp_path);program,_=fixture()
    program.update(origin='promo_campaign',campaign_binding={'campaign_id':42,'activity_id':43})
    try:
        await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
        async with db.raw_conn() as conn:
            await conn.execute("UPDATE hero_talk_program SET status='paused',active_revision=1");await conn.commit()
        next_program={**program,'revision':2}
        await store.put_draft(next_program,expected_revision=1,actor=ACTOR,idempotency_key='fixture-second')
        read=await store.get(program['program_id'],actor=ACTOR)
        assert read['status']=='paused' and read['active_revision']==1
        rewritten={**program,'revision':3,'origin':'editorial_program'};rewritten.pop('campaign_binding')
        with pytest.raises(HeroStoreError,match='immutable_origin_conflict'):
            await store.put_draft(rewritten,expected_revision=2,actor=ACTOR,idempotency_key='fixture-third')
        async with db.raw_conn() as conn:
            first=json.loads((await (await conn.execute('SELECT request_json FROM hero_talk_change_log WHERE revision=1')).fetchone())[0])
        assert first==program
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_conflicting_idempotency_body_never_creates_revision(tmp_path):
    db,store=await setup(tmp_path);program,_=fixture()
    try:
        await store.put_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-first')
        with pytest.raises(HeroStoreError,match='idempotency_conflict'):
            await store.put_draft({**program,'revision':2},expected_revision=1,actor=ACTOR,idempotency_key='fixture-first')
        assert (await store.get(program['program_id'],actor=ACTOR))['desired_revision']==1
    finally:
        await db.close()


def test_private_draft_history_is_not_in_static_public_projection():
    from static_site_release import STATIC_SITE_PROJECTION_TABLES
    assert 'hero_talk_change_log' not in STATIC_SITE_PROJECTION_TABLES
    assert 'hero_talk_program' not in STATIC_SITE_PROJECTION_TABLES


@pytest.mark.asyncio
async def test_prepare_survives_restart_but_expiry_never_advances_desired_revision(tmp_path):
    db,store=await setup(tmp_path);program,_=fixture();clock=[1000]
    store.clock=lambda:clock[0]
    try:
        prepared=await store.prepare_draft(program,expected_revision=0,actor=ACTOR,idempotency_key='fixture-preparation')
        assert prepared['status']=='prepared'
        assert (await store.get(program['program_id'],actor=ACTOR))['desired_revision']==0
        restarted=HeroProgramStore(db,authorize=store.authorize,clock=lambda:clock[0])
        clock[0]=1600
        with pytest.raises(HeroStoreError,match='preparation_expired'):
            await restarted.commit_draft(prepared['operation_ref'],action_digest=prepared['action_digest'],actor=ACTOR)
        assert (await restarted.get(program['program_id'],actor=ACTOR))['desired_revision']==0
        operation=await restarted.operation_get(prepared['operation_ref'],actor=ACTOR)
        assert operation['status']=='blocked' and operation['reason']=='preparation_expired'
    finally:
        await db.close()
