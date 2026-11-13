import json
from io import BytesIO
from types import SimpleNamespace

import pytest
from PIL import Image
import main
import vk_event_relations as relations


@pytest.mark.asyncio
async def test_shared_graph_refresh_keeps_original_gallery_and_semantic_only(tmp_path, monkeypatch):
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '1')
    db = main.Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            for i in range(1, 5):
                session.add(main.Event(id=i, title=f'Концерт {i}', description='', source_text='',
                    date='2099-01-01', time='19:00', location_name='Кафедральный собор',
                    source_vk_post_url=f'https://vk.com/wall-1_{i}', is_free=i == 2,
                    linked_event_ids=[3] if i == 1 else [], photo_urls=['poster-a', 'poster-b']))
            await session.commit()
            event = await session.get(main.Event, 1)
        path = relations.graph_path(db)
        path.write_text(json.dumps({'related': {'1': {'similar': [2, 3, 4], 'pure_related': [2, 3]}}}))
        data = await relations.presentation(db, event)
        assert [r['id'] for r in data['related']] == [2]
        assert [r['id'] for r in data['dates']] == [3]
        assert data['related'][0]['medallions'][0].endswith('kant-island.png')
        assert data['related'][0]['is_free']
        cards = await relations.cards(data, 8)
        assert len(cards) == 2
        assert Image.open(BytesIO(cards[1])).size == (1080, 1350)
        async def photo(group, url, *args, **kwargs):
            return url
        async def card(group, payload, *args, **kwargs):
            return f'card-{cards.index(payload)}'
        monkeypatch.setattr(main, 'upload_vk_photo', photo)
        monkeypatch.setattr(main, 'upload_vk_photo_bytes', card)
        attachments, mode = await main._upload_vk_event_gallery('1', event.photo_urls, event.id, db, None, cards)
        assert attachments == ['poster-a', 'poster-b', 'card-0', 'card-1']
        assert mode == 'carousel'  # Also for even event IDs.
        queued = []
        async def enqueue(db, eid, task, **kwargs):
            queued.append((eid, task, kwargs))
        monkeypatch.setattr(main, 'enqueue_job', enqueue)
        async def postponed(*args):
            return [{'id': 1}]
        async def public(*args, **kwargs):
            return {'response': {'items': [{'id': 2}]}}
        monkeypatch.setattr(main, '_fetch_vk_postponed_items_for_owner', postponed)
        monkeypatch.setattr(main, '_vk_api', public)
        await relations.enqueue_refresh(db)
        assert {eid for eid, _, _ in queued} == {1, 2}
        assert any(eid == 1 and task == main.JobTask.vk_sync and kw['requeue_done'] for eid, task, kw in queued)
        old_data = json.dumps(data, sort_keys=True)
        path.write_text(json.dumps({'related': {'1': {'pure_related': [4]}}}))
        assert json.dumps(await relations.presentation(db, event), sort_keys=True) != old_data
        assert 'https://' not in relations.append_list('body', data)
        assert event.photo_urls == ['poster-a', 'poster-b']
        assert await relations.cards(data, 0) == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_comment_replay_edits_one_comment_and_defers_postponed(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    calls, deferred = [], []
    postponed = False
    async def api(method, params, *args, **kwargs):
        calls.append((method, params))
        if method == 'wall.getById':
            return {'response': [{'id': 10, 'date': 4070908800 if postponed else 1, 'text': 'body\nПОХОЖИЕ СОБЫТИЯ\nКонцерт'}]}
        if method == 'wall.getComments':
            return {'response': {'items': []}}
        if method == 'wall.createComment':
            return {'response': {'comment_id': 100}}
        return {'response': 1}
    async def enqueue(*args, **kw):
        deferred.append(kw)
    monkeypatch.setattr(main, '_vk_api', api)
    monkeypatch.setattr(main, '_vk_user_token', lambda: 'test')
    monkeypatch.setattr(main, 'enqueue_job', enqueue)
    event = SimpleNamespace(id=1)
    data = {'related': [{'id': 2, 'title': 'Концерт', 'date': '14.11', 'time': '19:00', 'url': 'https://vk.com/wall-1_2'}], 'dates': []}
    try:
        assert await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data)
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data)
        assert sum(method == 'wall.createComment' for method, _ in calls) == 1
        data['related'][0]['time'] = '20:00'
        assert await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data)
        assert calls[-1][0] == 'wall.editComment'
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', {'related': [], 'dates': []})
        assert calls[-1][0] == 'wall.deleteComment'
        postponed = True
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_11', data)
        assert deferred[-1]['payload']['relations'] is True
        assert deferred[-1]['next_run_at'].timestamp() == 4070908860
    finally:
        await db.close()
