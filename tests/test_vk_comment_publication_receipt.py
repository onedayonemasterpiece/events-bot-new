"""A frozen announcement keeps its physical public target when canonical facts change."""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

import main
import vk_calendar_comments as calendar
import vk_event_relations as related

OLD = 'https://vk.com/wall-231920894_10'
LIVE = 'https://vk.com/wall-231920894_20'


@pytest.mark.asyncio
@pytest.mark.parametrize('via_worker', [False, True])
@pytest.mark.parametrize('kind', ['calendar', 'related'])
@pytest.mark.parametrize('conflict', [None, 'id_evidence', 'date', 'postponed_id', 'owner', 'deleted', 'missing', 'postpone', 'receipt',
    'target', 'platform', 'status', 'stored_url', 'canonical_url'])
async def test_frozen_comment_uses_verified_public_receipt(tmp_path, monkeypatch, kind, conflict, via_worker):
    db = main.Database(str(tmp_path / 'receipt.sqlite'))
    await db.init()
    published_at = int(datetime.now(timezone.utc).timestamp()) - 3600
    selection = {'dates': [], 'related': [
        {'date': '28.11', 'time': '18:00', 'title': 'Frozen peer', 'url': 'https://vk.com/wall-1_4'}]}
    payload = {'post_url': OLD, 'publish_date': published_at}
    if kind == 'related':
        payload['relations'] = selection
    item = {'id': 20, 'owner_id': -231920894, 'date': published_at,
            'post_type': 'post', 'text': 'Original header\nПОХОЖИЕ СОБЫТИЯ\nFrozen peer'}
    if conflict == 'id_evidence':
        item['date'] += 60
        item['postponed_id'] = 10
    if conflict == 'postpone':
        item['post_type'] = 'postpone'
    if conflict == 'date':
        item['date'] += 60
    if conflict == 'postponed_id':
        item['postponed_id'] = 11
    if conflict == 'owner':
        item['owner_id'] = -2
    if conflict == 'deleted':
        item['is_deleted'] = True
    calls, calendar_events = [], []
    async def api(method, params, *args, **kwargs):
        calls.append((method, params))
        if method == 'wall.get':
            return {'response': {'items': [item]}}
        if method == 'wall.getById':
            return {'response': [item] if params['posts'] == '-231920894_20' and conflict != 'missing' else []}
        if method == 'wall.getComments':
            return {'response': {'items': []}}
        if method == 'docs.getWallUploadServer':
            return {'response': {'upload_url': 'https://fixture.invalid'}}
        if method == 'docs.save':
            return {'response': {'doc': {'id': 30, 'owner_id': 40, 'ext': 'ics'}}}
        if method == 'wall.createComment':
            assert params['post_id'] == 20
            if kind == 'related':
                assert params['message'] == 'ПОХОЖИЕ СОБЫТИЯ\n\n28.11 18:00 [https://vk.com/wall-1_4|Frozen peer]'
            return {'response': {'comment_id': 90}}
        raise AssertionError(method)
    async def calendar_bytes(db, events):
        calendar_events.extend(e.id for e in events)
        return b'fixture calendar'
    class Response:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        def raise_for_status(self): pass
        async def json(self, **kwargs): return {'file': 'fixture'}
    class HTTP:
        def post(self, *args, **kwargs): return Response()
    monkeypatch.setattr(main, '_vk_api', api)
    monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
    monkeypatch.setattr(main, 'VK_SERVICE_TOKEN', 'fixture')
    monkeypatch.setattr(main, '_vk_user_token', lambda: 'fixture')
    monkeypatch.setattr(main, '_recover_managed_vk_live_url', AsyncMock(return_value=False))
    monkeypatch.setattr(main, 'get_bot', lambda: None)
    monkeypatch.setattr(main, 'get_http_session', lambda: HTTP())
    monkeypatch.setattr(calendar, 'calendar_bytes', calendar_bytes)
    monkeypatch.setattr(calendar.asyncio, 'sleep', AsyncMock())
    try:
        async with db.get_session() as session:
            for eid, url, lifecycle in [(1, LIVE + '1' if conflict == 'canonical_url' else OLD, 'active'), (2, OLD if conflict == 'receipt' else LIVE, 'active'),
                    (3, OLD, 'archived'), (4, 'https://vk.com/wall-231920894_99', 'active')]:
                session.add(main.Event(id=eid, title='Current changed title', description='', source_text='',
                    date='2099-11-27', time='19:00', location_name='Hall', lifecycle_status=lifecycle,
                    source_vk_post_url=url))
            await session.commit()
        async with db.raw_conn() as conn:
            for eid, live in [(1, LIVE), *([(2, 'https://vk.com/wall-231920894_21')] if conflict == 'receipt' else [])]:
                await conn.execute('''INSERT INTO event_publication(
                    event_id,platform,target,stored_url,live_url,status)
                    VALUES(?,?,?,?,?,?)''', (eid,
                    'telegram' if conflict == 'platform' else 'vk',
                    'other' if conflict == 'target' else 'klgdevents',
                    OLD + '1' if conflict == 'stored_url' else OLD, live,
                    'missing' if conflict == 'status' else 'published'))
            await conn.commit()
        if via_worker:
            async with db.get_session() as session:
                session.add(main.JobOutbox(event_id=1, task=main.JobTask.vk_calendar_comment,
                    coalesce_key=f'fixture:{kind}', payload=payload,
                    next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
                await session.commit()
            assert await main._run_due_jobs_once(db, None) == 1
            async with db.get_session() as session:
                job = (await session.execute(main.select(main.JobOutbox))).scalars().one()
                definitive = conflict in {'date', 'postponed_id', 'owner', 'receipt', 'canonical_url'}
                assert job.payload == payload
                assert job.terminal_reason == ('failed' if definitive else None)
                assert job.attempts == (1 if conflict and conflict != 'id_evidence' and not definitive else 0)
                assert job.status == (main.JobStatus.done if conflict in (None, 'id_evidence') else main.JobStatus.error)
            assert await main._run_due_jobs_once(db, None) == 0
            assert sum(method == 'wall.createComment' for method, _ in calls) == (conflict in (None, 'id_evidence'))
        elif conflict not in (None, 'id_evidence'):
            with pytest.raises(RuntimeError):
                await main.job_vk_calendar_comment(1, db, None, payload)
            assert not any(method == 'wall.createComment' for method, _ in calls)
        else:
            assert await main.job_vk_calendar_comment(1, db, None, payload)
            assert not await main.job_vk_calendar_comment(1, db, None, payload)
            assert sum(method == 'wall.createComment' for method, _ in calls) == 1
            if kind == 'calendar':
                assert set(calendar_events) == {1, 2}
            async with db.get_session() as session:
                assert (await session.get(main.Event, 1)).source_vk_post_url == OLD
                assert (await session.get(main.Event, 2)).source_vk_post_url == LIVE
            assert payload.get('relations', selection) == selection
        assert not any(method in {'wall.edit', 'wall.editComment', 'wall.deleteComment'} for method, _ in calls)
    finally:
        await db.close()
