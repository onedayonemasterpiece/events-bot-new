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
                    date='2099-01-01..2099-01-03' if i == 4 else '2099-01-01',
                    end_date='2099-01-03' if i == 4 else None, time='00:00' if i == 4 else '19:00',
                    time_is_default=i == 4, location_name='Кафедральный собор',
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
            async with db.get_session() as session:
                peer = await session.get(main.Event, 2)
            return {'response': {'items': [{'id': 20, 'text': '\n'.join(main.build_vk_source_header(peer, None))}]}}
        monkeypatch.setattr(main, '_fetch_vk_postponed_items_for_owner', postponed)
        monkeypatch.setattr(main, '_vk_api', public)
        await relations.enqueue_refresh(db)
        assert {eid for eid, _, _ in queued} == {1, 2}
        refreshed = await relations.presentation(db, event)
        assert refreshed['related'][0]['url'] == 'https://vk.com/wall-1_20'
        assert any(eid == 1 and task == main.JobTask.vk_sync and kw['requeue_done'] for eid, task, kw in queued)
        old_data = json.dumps(data, sort_keys=True)
        path.write_text(json.dumps({'related': {'1': {'pure_related': [4]}}}))
        updated = await relations.presentation(db, event)
        assert json.dumps(updated, sort_keys=True) != old_data
        assert updated['related'][0]['date'] == '01.01–03.01'
        assert updated['related'][0]['time'] == ''
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
        assert await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data, deferred=True)
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data, deferred=True)
        assert sum(method == 'wall.createComment' for method, _ in calls) == 1
        created = next(params for method, params in calls if method == 'wall.createComment')
        assert created['message'] == 'ПОХОЖИЕ СОБЫТИЯ\n\n14.11 19:00 [https://vk.com/wall-1_2|Концерт]'
        data['related'][0]['time'] = '20:00'
        assert await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data, deferred=True)
        assert calls[-1][0] == 'wall.editComment'
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', {'related': [], 'dates': []}, deferred=True)
        assert calls[-1][0] == 'wall.deleteComment'
        postponed = True
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_11', data)
        assert deferred[-1]['payload']['relations'] is True
        assert 4070908980 <= deferred[-1]['next_run_at'].timestamp() <= 4070910000
    finally:
        await db.close()


def test_related_card_has_no_missing_emoji_glyph_or_title_medallion_overlap(monkeypatch):
    from PIL import ImageDraw
    seen = []
    original = ImageDraw.ImageDraw.text
    def record(self, xy, text, *args, **kwargs):
        seen.append((xy, text, kwargs.get('font')))
        return original(self, xy, text, *args, **kwargs)
    monkeypatch.setattr(ImageDraw.ImageDraw, 'text', record)
    body = relations.render_card([{'date': '20.11', 'date_iso': '2026-11-20', 'time': '19:00', 'title': '🎭 ' + 'Длинное название концерта ' * 12,
        'place': 'Кафедральный собор', 'medallions': ['site/public/assets/organizers/kant-island.png'], 'is_free': True}])
    assert Image.open(BytesIO(body)).size == (1080, 1350)
    assert all('🎭' not in text for _, text, _ in seen)
    assert any(text == 'НОЯБРЯ' for _, text, _ in seen)
    assert any(text == 'пт · 19:00' for _, text, _ in seen)
    title_lines = [(text, font) for xy, text, font in seen if xy[0] == 238 and xy[1] in (24, 58)]
    assert title_lines[-1][0].endswith('…')
    draw = ImageDraw.Draw(Image.new('RGB', (1, 1)))
    assert all(draw.textlength(text, font=font) <= 522 for text, font in title_lines)


@pytest.mark.asyncio
async def test_deferred_comment_uses_persisted_live_url_after_boolean_recovery(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'live-id.sqlite'))
    await db.init()
    async def recover(db, event, **kwargs):
        event.source_vk_post_url = 'https://vk.com/wall-1_20'
        return True
    async def data(*args):
        return {'dates': [], 'related': []}
    async def comment(db, event, url, *args, **kwargs):
        assert url == 'https://vk.com/wall-1_20'
        assert kwargs['deferred'] is True
        return True
    monkeypatch.setattr(main, '_recover_managed_vk_live_url', recover)
    monkeypatch.setattr(relations, 'presentation', data)
    monkeypatch.setattr(relations, 'ensure_comment', comment)
    try:
        async with db.get_session() as session:
            event = main.Event(title='Концерт', description='', source_text='', date='2099-01-01',
                time='19:00', location_name='Зал', source_vk_post_url='https://vk.com/wall-1_10')
            session.add(event)
            await session.commit()
            await session.refresh(event)
        assert await main.job_vk_calendar_comment(event.id, db, None, {'relations': True})
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_post_without_source_image_has_no_relations():
    assert await relations.presentation(None, SimpleNamespace(photo_urls=[])) == {'dates': [], 'related': []}


@pytest.mark.asyncio
async def test_ambiguous_live_identity_cannot_trigger_republication(monkeypatch):
    event = main.Event(id=1, title='Концерт', description='', date='2099-01-01',
        time='19:00', location_name='Зал')
    header = '\n'.join(main.build_vk_source_header(event, None))
    items = [{'id': i, 'text': header} for i in (10, 11)]
    async def api(*args, **kwargs):
        return {'response': {'items': items}}
    monkeypatch.setattr(main, '_vk_api', api)
    monkeypatch.setattr(main, 'VK_USER_TOKEN', 'test')
    monkeypatch.setattr(main, '_vk_wall_get_actors', lambda owner: [SimpleNamespace(kind='user', token='test')])
    with pytest.raises(RuntimeError, match='vk_live_id_ambiguous'):
        await main._find_unique_live_managed_vk_item_for_event(event, owner_id=-1, db=None, bot=None)
    # A bounded refresh omits an ambiguous event and continues with other posts.
    assert await main._find_unique_live_managed_vk_item_for_event(event, owner_id=-1,
        db=None, bot=None, candidate_items=items) is None


@pytest.mark.asyncio
async def test_normal_sync_schedules_one_random_delayed_comment(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    db = main.Database(str(tmp_path / 'delay.sqlite'))
    await db.init()
    calls = []
    published = int(datetime.now(timezone.utc).timestamp())
    async def api(method, params, *args, **kwargs):
        calls.append(method)
        assert method == 'wall.getById'  # Normal publication must never write a comment.
        return {'response': [{'id': 10, 'date': published,
            'text': 'Анонс\nПОХОЖИЕ СОБЫТИЯ\nКонцерт'}]}
    monkeypatch.setattr(main, '_vk_api', api)
    monkeypatch.setattr(main, '_vk_user_token', lambda: 'fixture')
    monkeypatch.setattr(relations.random, 'randint', lambda low, high: high)
    try:
        async with db.get_session() as session:
            event = main.Event(title='Концерт', description='', source_text='',
                date='2099-01-01', time='19:00', location_name='Зал')
            session.add(event)
            await session.commit()
            await session.refresh(event)
        data = {'related': [{'id': 2, 'title': 'Концерт', 'date': '14.11',
            'time': '19:00', 'url': 'https://vk.com/wall-1_2'}], 'dates': []}
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data)
        async with db.get_session() as session:
            job = (await session.execute(relations.select(main.JobOutbox).where(
                main.JobOutbox.coalesce_key == 'vk_related_comment:-1:10'))).scalar_one()
            first_due = job.next_run_at
            assert 1200 <= first_due.replace(tzinfo=timezone.utc).timestamp() - published < 1202
            assert job.task == main.JobTask.vk_calendar_comment
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data)
        async with db.get_session() as session:
            jobs = (await session.execute(relations.select(main.JobOutbox).where(
                main.JobOutbox.coalesce_key == 'vk_related_comment:-1:10'))).scalars().all()
            assert len(jobs) == 1 and jobs[0].next_run_at == first_due
    finally:
        await db.close()
