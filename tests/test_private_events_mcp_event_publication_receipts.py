from dataclasses import replace

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.event_publication_receipts import EventPublicationReceiptService, _public_url
from private_events_mcp.tool_catalog import ToolCallContext, ToolExecutionError
from test_private_events_mcp_event_create_recovery import setup


async def allowed(context, event_id):
    return True


async def fixture(tmp_path, **kwargs):
    db, runtime, executor, req = await setup(tmp_path)
    ctx = ToolCallContext(AccessIdentity(req.actor_subject, req.actor_client_id, frozenset(),
                                         req.actor_audience, 'token', 5000), req.actor_audience)
    operation, _ = await runtime.store.reserve(req)
    async with db.raw_conn() as conn:
        await conn.execute('CREATE TABLE event(id INTEGER PRIMARY KEY,telegraph_url TEXT,tg_event_post_url TEXT,source_vk_post_url TEXT,vk_repost_url TEXT)')
        await conn.execute('INSERT INTO event VALUES(123,NULL,NULL,NULL,NULL)')
        await conn.execute('CREATE TABLE joboutbox(id INTEGER PRIMARY KEY,event_id INTEGER,task TEXT,status TEXT,payload TEXT,last_error TEXT)')
        await conn.commit()
    service = EventPublicationReceiptService(database=db, authorize=kwargs.pop('authorize', allowed), **kwargs)
    return db, runtime, service, ctx, operation['operation_ref']


async def accepted(runtime, ref):
    await runtime.store.finish(ref, status='accepted', result={'event_ids': [123]})


@pytest.mark.asyncio
async def test_dynamic_receipts_are_not_frozen_commit_snapshot(tmp_path):
    db, runtime, service, ctx, ref = await fixture(tmp_path)
    await accepted(runtime, ref)
    initial = await service.read(ref, ctx)
    assert initial['event_id'] == 123 and not initial['live_verified']
    assert initial['publications'][0]['state'] == 'no_public_receipt'
    async with db.raw_conn() as conn:
        await conn.execute("UPDATE event SET tg_event_post_url='https://t.me/channel_test/42', source_vk_post_url='https://vk.com/wall-123_42',telegraph_url='https://telegra.ph/Event-09-06' WHERE id=123")
        await conn.execute("INSERT INTO joboutbox VALUES(1,123,'tg_event_publish','done','private payload','private error')")
        await conn.commit()
    current = await service.read(ref, ctx)
    assert current['publications'][0]['state'] == 'recorded_public_url'
    assert current['jobs'] == [{'job_id': 1, 'task': 'tg_event_publish', 'state': 'done'}]
    assert 'private' not in str(current)
    assert current['publications'][-1]['state'] == 'event_inclusion_unverified'
    assert not current['live_verified']


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['queued', 'processing', 'review_required', 'outcome_unknown', 'failed', 'rejected'])
async def test_only_accepted_operation_exposes_event(tmp_path, status):
    db, runtime, service, ctx, ref = await fixture(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute('UPDATE event_change_log SET status=?,event_id=123', (status,))
        await conn.commit()
    output = await service.read(ref, ctx)
    assert output['event_id'] is None and output['publications'] == [] and output['jobs'] == []


@pytest.mark.asyncio
async def test_exact_actor_and_current_tenant_access_are_required(tmp_path):
    calls = []

    async def policy(ctx, event_id):
        calls.append(event_id)
        return event_id != 123

    db, runtime, service, ctx, ref = await fixture(tmp_path, authorize=policy)
    await accepted(runtime, ref)
    with pytest.raises(ToolExecutionError):
        await service.read(ref, ctx)
    assert calls == [None, 123]
    unrestricted = EventPublicationReceiptService(database=db, authorize=allowed)
    for identity in (replace(ctx.identity, subject='other'), replace(ctx.identity, client_id='other'),
                     replace(ctx.identity, audience='other')):
        with pytest.raises(ToolExecutionError):
            await unrestricted.read(ref, ToolCallContext(identity, identity.audience))


@pytest.mark.asyncio
async def test_revoke_during_read_suppresses_result(tmp_path):
    calls = []

    async def policy(ctx, event_id):
        calls.append(event_id)
        return len(calls) < 3

    _, runtime, service, ctx, ref = await fixture(tmp_path, authorize=policy)
    await accepted(runtime, ref)
    with pytest.raises(ToolExecutionError):
        await service.read(ref, ctx)
    assert calls == [None, 123, 123]


@pytest.mark.asyncio
async def test_done_without_url_is_not_published_and_jobs_are_bounded(tmp_path):
    db, runtime, service, ctx, ref = await fixture(tmp_path, max_jobs=5)
    await accepted(runtime, ref)
    async with db.raw_conn() as conn:
        for index, status in enumerate(['pending', 'running', 'done', 'error', 'paused', 'done'], 1):
            await conn.execute('INSERT INTO joboutbox VALUES(?,123,?,?,NULL,NULL)', (index, 'static_site_build' if index == 6 else 'tg_event_publish', status))
        await conn.execute("INSERT INTO joboutbox VALUES(7,999,'tg_event_publish','done',NULL,NULL)")
        await conn.commit()
    output = await service.read(ref, ctx)
    assert len(output['jobs']) == 5 and output['jobs_truncated']
    assert output['jobs'][0]['task'] == 'static_site_build'
    assert output['publications'][0]['state'] == 'no_public_receipt'
    assert output['publications'][-1]['state'] == 'event_inclusion_unverified'


@pytest.mark.parametrize('url,surface', [
    ('https://t.me/c/123/42', 'telegram'), ('https://t.me/+invite', 'telegram'),
    ('https://t.me/joinchat/123', 'telegram'), ('https://t.me/channel_test/42?token=private', 'telegram'),
    ('https://user:secret@vk.com/wall-123_42', 'vk'), ('https://127.0.0.1/private', 'telegraph'),
    ('https://vk.com.evil.test/wall-123_42', 'vk'), ('javascript:alert(1)', 'telegraph'),
    ('https://private-storage.test/event', 'telegraph'), ('https://vk.com:443/wall-123_42', 'vk'),
])
def test_private_or_untrusted_urls_are_suppressed(url, surface):
    assert _public_url(url, surface) is None


@pytest.mark.asyncio
async def test_missing_canonical_event_and_policy_failure_are_honest(tmp_path):
    db, runtime, service, ctx, ref = await fixture(tmp_path)
    await accepted(runtime, ref)
    async with db.raw_conn() as conn:
        await conn.execute('DELETE FROM event')
        await conn.commit()
    assert (await service.read(ref, ctx))['availability'] == 'canonical_event_missing'

    async def failed(*args):
        raise RuntimeError('private detail')

    with pytest.raises(ToolExecutionError) as error:
        await EventPublicationReceiptService(database=db, authorize=failed).read(ref, ctx)
    assert 'private detail' not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize('event_id,result', [(None, '{"event_ids":[123]}'),
    (0, '{"event_ids":[0]}'), (123, '{}'), (123, '{"event_ids":[999]}'),
    (123, '{"event_ids":[123,999]}'), (123, 'invalid-json'), (123, '{"event_ids":[true]}')])
async def test_inconsistent_accepted_identity_fails_closed(tmp_path, event_id, result):
    db, runtime, service, ctx, ref = await fixture(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute("UPDATE event_change_log SET status='accepted',event_id=?,result_json=?", (event_id, result))
        await conn.commit()
    with pytest.raises(ToolExecutionError):
        await service.read(ref, ctx)


@pytest.mark.asyncio
async def test_pending_job_maps_to_queued_but_does_not_imply_receipt(tmp_path):
    db, runtime, service, ctx, ref = await fixture(tmp_path)
    await accepted(runtime, ref)
    async with db.raw_conn() as conn:
        await conn.execute("INSERT INTO joboutbox VALUES(1,123,'tg_event_publish','pending',NULL,NULL)")
        await conn.commit()
    output = await service.read(ref, ctx)
    assert output['jobs'][0]['state'] == 'queued'
    assert output['publications'][0]['state'] == 'no_public_receipt'
