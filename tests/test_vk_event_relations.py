import json
from datetime import datetime, timedelta, timezone
from io import BytesIO
from types import SimpleNamespace

import pytest
from PIL import Image
import main
import vk_event_relations as relations


@pytest.mark.asyncio
async def test_legacy_related_job_without_carousel_selection_sends_nothing(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    db = main.Database(str(tmp_path / 'legacy-related.sqlite'))
    await db.init()
    recover = AsyncMock(return_value=False)
    graph = AsyncMock(return_value={'dates': [], 'related': [{'id': 2}]})
    comment = AsyncMock(return_value=True)
    monkeypatch.setattr(main, '_recover_managed_vk_live_url', recover)
    monkeypatch.setattr(relations, 'presentation', graph)
    monkeypatch.setattr(relations, 'ensure_comment', comment)
    try:
        async with db.get_session() as session:
            event = main.Event(title='Event', description='', source_text='', date='2099-01-01',
                               time='19:00', location_name='Hall', source_vk_post_url='https://vk.com/wall-1_10')
            session.add(event)
            await session.commit()
            await session.refresh(event)
        assert await main.job_vk_calendar_comment(event.id, db, None, {'relations': True}) is False
        recover.assert_not_called()
        graph.assert_not_called()
        comment.assert_not_called()
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('publication_offset', [3600, -120])
async def test_early_related_job_keeps_same_job_pending_until_publication(tmp_path, monkeypatch, publication_offset):
    db = main.Database(str(tmp_path / 'early-related.sqlite'))
    await db.init()
    published_at = int(datetime.now(timezone.utc).timestamp()) + publication_offset
    monkeypatch.setattr(relations.random, 'randint', lambda a, b: b)
    calls = []
    async def api(method, params, *args, **kwargs):
        calls.append(method)
        if method == 'wall.getById':
            return {'response': [{'id': 10, 'date': published_at,
                                  'text': 'Event\nПОХОЖИЕ СОБЫТИЯ\nPeer'}]}
        if method == 'wall.getComments':
            return {'response': {'items': []}}
        if method == 'wall.createComment':
            return {'response': {'comment_id': 100}}
        raise AssertionError(method)
    async def recovered(*args, **kwargs): return False
    monkeypatch.setattr(main, '_vk_api', api)
    monkeypatch.setattr(main, '_recover_managed_vk_live_url', recovered)
    monkeypatch.setattr(main, '_vk_user_token', lambda: 'fixture')
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    monkeypatch.setattr(main, 'get_bot', lambda: None)
    data = {'dates': [], 'related': [{'id': 2, 'date': '01.01', 'time': '19:00',
                                    'title': 'Peer', 'url': 'https://vk.com/wall-1_2'}]}
    try:
        async with db.get_session() as session:
            session.add(main.Event(id=1, title='Event', description='', source_text='',
                date='2099-01-01', time='19:00', location_name='loc',
                source_vk_post_url='https://vk.com/wall-1_10'))
            session.add(main.JobOutbox(event_id=1, task=main.JobTask.vk_calendar_comment,
                coalesce_key='vk_related_comment:-1:10',
                payload={'post_url': 'https://vk.com/wall-1_10', 'relations': data},
                next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
            await session.commit()
        assert await main._run_due_jobs_once(db, None) == 1
        async with db.get_session() as session:
            job = (await session.execute(main.select(main.JobOutbox))).scalars().one()
        assert job.status == main.JobStatus.pending
        assert job.attempts == 0
        due = main._ensure_utc(job.next_run_at)
        assert 180 <= due.timestamp() - published_at <= 1200
        assert calls == ['wall.getById']
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None): return due + timedelta(seconds=1)
        monkeypatch.setattr(main, 'datetime', Clock)
        monkeypatch.setattr(relations, 'datetime', Clock)
        assert await main._run_due_jobs_once(db, None) == 1
        async with db.get_session() as session:
            assert (await session.get(main.JobOutbox, job.id)).status == main.JobStatus.done
        assert calls.count('wall.createComment') == 1
        assert await main._run_due_jobs_once(db, None) == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_shared_graph_keeps_original_gallery_and_semantic_only(tmp_path, monkeypatch):
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '1')
    db = main.Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            for i in range(1, 5):
                session.add(main.Event(id=i, title=f'Концерт {i}', event_type='концерт', description='', source_text='',
                    date='2099-01-01..2099-01-03' if i == 4 else '2099-01-01',
                    end_date='2099-01-03' if i == 4 else None, time='00:00' if i == 4 else '19:00',
                    time_is_default=i == 4, location_name='Кафедральный собор',
                    source_vk_post_url=f'https://vk.com/wall-1_{i}', is_free=i == 2,
                    linked_event_ids=[3] if i == 1 else [], photo_urls=['poster-a', 'poster-b']))
            await session.commit()
            event = await session.get(main.Event, 1)
        path = relations.graph_path(db)
        path.write_text(json.dumps({'related': {'1': {'similar': [2, 3, 4], 'pure_related': [2, 3]}}}))
        async def visible(method, params, *args, **kwargs):
            assert method == 'wall.getById'
            assert kwargs['token_kind'] == 'service'
            return {'response': [{'owner_id': int(ids.split('_')[0]), 'id': int(ids.split('_')[1]),
                'post_type': 'post'} for ids in params['posts'].split(',')]}
        monkeypatch.setattr(main, '_vk_api', visible)
        data = await relations.presentation(db, event)
        assert [r['id'] for r in data['related']] == [2]
        assert [r['id'] for r in data['dates']] == [3]
        assert data['related'][0]['medallions'][0].endswith('kant-island.png')
        assert data['related'][0]['is_free']
        assert data['related'][0]['event_type'] == 'концерт'
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
        old_data = json.dumps(data, sort_keys=True)
        path.write_text(json.dumps({'related': {'1': {'pure_related': [4]}}}))
        updated = await relations.presentation(db, event)
        assert json.dumps(updated, sort_keys=True) != old_data
        assert updated['related'][0]['daily_hours'] == ''
        async with db.get_session() as session:
            daily = await session.get(main.Event, 4)
            daily.time, daily.time_is_default = '12:00..20:00', False
            await session.commit()
        daily_data = await relations.presentation(db, event)
        assert daily_data['related'][0]['daily_hours'] == '12:00–20:00'
        assert updated['related'][0]['date'] == '01.01–03.01'
        assert updated['related'][0]['time'] == ''
        assert 'https://' not in relations.append_list('body', data)
        assert event.photo_urls == ['poster-a', 'poster-b']
        assert await relations.cards(data, 0) == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_comment_is_created_once_never_edited_and_defers_postponed(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    calls, deferred = [], []
    postponed = False
    async def api(method, params, *args, **kwargs):
        calls.append((method, params))
        if method == 'wall.getById':
            return {'response': [{'id': 10, 'date': 4070908800 if postponed else 1, 'text': 'body\nПОХОЖИЕ СОБЫТИЯ\nКонцерт'}]}
        if method == 'wall.getComments':
            return {'response': {'items': [{'id': 101, 'from_id': -1, 'text': 'ПОХОЖИЕ СОБЫТИЯ\nOld text'}] if params['post_id'] == 12 else []}}
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
        calls_before = len(calls)
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data, deferred=True)
        assert len(calls) == calls_before
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', {'related': [], 'dates': []}, deferred=True)
        assert len(calls) == calls_before
        assert all(method not in {'wall.editComment', 'wall.deleteComment'} for method, _ in calls)
        # Recover a lost receipt without editing or duplicating the existing comment.
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_12', data, deferred=True)
        assert sum(method == 'wall.createComment' for method, _ in calls) == 1
        assert all(method not in {'wall.editComment', 'wall.deleteComment'} for method, _ in calls)
        postponed = True
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_11', data)
        assert deferred[-1]['payload']['relations'] == data
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
        'event_type': 'кинопоказ', 'place': 'Кафедральный собор', 'medallions': ['site/public/assets/organizers/kant-island.png'], 'is_free': True}])
    assert Image.open(BytesIO(body)).size == (1080, 1350)
    assert all('🎭' not in text for _, text, _ in seen)
    assert any('Кинопоказ' in text for _, text, _ in seen)
    assert any(text == 'НОЯБРЯ' for _, text, _ in seen)
    assert any(text == 'пт · 19:00' for _, text, _ in seen)
    title_lines = [(text, font) for xy, text, font in seen if xy[0] == 238 and font.size == 32]
    assert title_lines[-1][0].endswith('…')
    draw = ImageDraw.Draw(Image.new('RGB', (1, 1)))
    assert all(draw.textlength(text, font=font) <= 502 for text, font in title_lines)


@pytest.mark.asyncio
async def test_deferred_comment_uses_persisted_live_url_after_boolean_recovery(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'live-id.sqlite'))
    await db.init()
    async def recover(db, event, **kwargs):
        event.source_vk_post_url = 'https://vk.com/wall-1_20'
        return True
    async def data(*args):
        raise AssertionError('A later graph must not replace the carousel selection')
    selection = {'dates': [], 'related': [{'id': 2, 'title': 'Original carousel event'}]}
    async def comment(db, event, url, *args, **kwargs):
        assert args[0] == selection
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
        assert await main.job_vk_calendar_comment(event.id, db, None, {'relations': selection})
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
            assert job.payload['relations'] == data
        assert not await relations.ensure_comment(db, event, 'https://vk.com/wall-1_10', data)
        async with db.get_session() as session:
            jobs = (await session.execute(relations.select(main.JobOutbox).where(
                main.JobOutbox.coalesce_key == 'vk_related_comment:-1:10'))).scalars().all()
            assert len(jobs) == 1 and jobs[0].next_run_at == first_due
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_related_peers_must_be_public_but_other_dates_need_no_post(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'public-peers.sqlite'))
    await db.init()
    async def public(method, params, *args, **kwargs):
        assert method == 'wall.getById'
        assert kwargs['token_kind'] == 'service' and kwargs['skip_captcha']
        # The first peer is still postponed; the next one is public.
        # An ID from another wall must not make the third peer visible.
        return {'response': [
            {'owner_id': -1, 'id': 2, 'post_type': 'postpone'},
            {'owner_id': -1, 'id': 3, 'post_type': 'post'},
            {'owner_id': -2, 'id': 4, 'post_type': 'post'}]}
    monkeypatch.setattr(main, '_vk_api', public)
    try:
        async with db.get_session() as session:
            for i in range(1, 6):
                session.add(main.Event(id=i, title=f'Концерт {i}', description='', source_text='',
                    date='2099-01-01', time='19:00', location_name='Зал',
                    source_vk_post_url=f'https://vk.com/wall-1_{i}' if i != 5 else None,
                    linked_event_ids=[5] if i == 1 else [], photo_urls=['original']))
            await session.commit()
            event = await session.get(main.Event, 1)
        relations.graph_path(db).write_text(json.dumps({'related': {'1': {'pure_related': [2, 3, 4]}}}))
        data = await relations.presentation(db, event)
        assert [row['id'] for row in data['related']] == [3]
        assert [row['id'] for row in data['dates']] == [5]
        assert event.photo_urls == ['original']
        async def failed(*args, **kwargs):
            raise main.VKAPIError(6, 'Too many requests')
        monkeypatch.setattr(main, '_vk_api', failed)
        with pytest.raises(main.VKAPIError, match='Too many requests'):
            await relations.presentation(db, event)
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('linked', [False, True])
async def test_related_comment_resolves_public_linked_header_once(tmp_path, monkeypatch, linked):
    """The delayed job uses the same real linked-time view as initial publication."""
    from unittest.mock import AsyncMock
    db = main.Database(str(tmp_path / 'linked-public-related.sqlite'))
    await db.init()
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '1')
    monkeypatch.setattr(main, 'VK_TOKEN_AFISHA', 'group-fixture')
    monkeypatch.setattr(main, 'VK_USER_TOKEN', 'user-fixture')
    monkeypatch.setattr(main, '_vk_user_token', lambda: 'user-fixture')
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    monkeypatch.setattr(main, 'get_bot', lambda: None)
    monkeypatch.setattr(relations, 'presentation', AsyncMock(side_effect=AssertionError('frozen selection must not be recomputed')))
    data = {'dates': [], 'related': [{'id': 4, 'date': '15.11', 'time': '19:00',
        'title': 'Сохранённый выбор', 'url': 'https://vk.com/wall-1_4'}]}
    payload = {'post_url': 'https://vk.com/wall-1_10', 'relations': data}
    calls = []
    try:
        async with db.get_session() as session:
            for eid, time in [(1, '11:00'), (2, '12:30')]:
                session.add(main.Event(id=eid, title='Матч за 5 место', description='', source_text='',
                    date='2099-11-14', time=time, location_name='Зал', linked_event_ids=[2] if linked and eid == 1 else [],
                    source_vk_post_url='https://vk.com/wall-1_10'))
            session.add(main.JobOutbox(event_id=1, task=main.JobTask.vk_calendar_comment,
                coalesce_key='vk_related_comment:-1:10', payload=payload,
                next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
            await session.commit()
            event = await session.get(main.Event, 1)
        publish_event, _ = await main._prepare_same_day_linked_publish_event(db, event)
        header = next(line for line in main.build_vk_source_header(publish_event, None) if line.startswith('📅 '))
        assert ('11:00 и 12:30' in header) == linked
        public = {'id': 20, 'date': int(datetime.now(timezone.utc).timestamp()) - 1800,
                  'post_type': 'post', 'text': 'Матч за 5 место\n' + header + '\nПОХОЖИЕ СОБЫТИЯ\nСохранённый выбор'}
        async def api(method, params, *args, **kwargs):
            calls.append((method, params))
            if method == 'wall.getById':
                return {'response': [public] if params['posts'] == '-1_20' else []}
            if method == 'wall.get':
                return {'response': {'items': [] if params['filter'] == 'postponed' else [public]}}
            if method == 'wall.getComments':
                assert params['post_id'] == 20
                return {'response': {'items': []}}
            if method == 'wall.createComment':
                assert params['post_id'] == 20
                assert params['message'] == 'ПОХОЖИЕ СОБЫТИЯ\n\n15.11 19:00 [https://vk.com/wall-1_4|Сохранённый выбор]'
                return {'response': {'comment_id': 100}}
            raise AssertionError(method)
        async def legacy_api(method, **params):
            return await api(method, params)
        monkeypatch.setattr(main, '_vk_api', api)
        monkeypatch.setattr(main, 'vk_api', legacy_api)
        assert await main._run_due_jobs_once(db, None) == 1
        async with db.get_session() as session:
            job = (await session.execute(main.select(main.JobOutbox))).scalars().one()
            assert job.status == main.JobStatus.done
            assert job.payload == payload
            assert (await session.get(main.Event, 1)).source_vk_post_url.endswith('_20')
            assert (await session.get(main.Event, 1)).time == '11:00'
            assert (await session.get(main.Event, 2)).time == '12:30'
        assert sum(method == 'wall.createComment' for method, _ in calls) == 1
        assert await main._run_due_jobs_once(db, None) == 0
        assert not await main.job_vk_calendar_comment(1, db, None, payload)
        assert sum(method == 'wall.createComment' for method, _ in calls) == 1
    finally:
        await db.close()


def test_until_tile_has_no_weekday_or_time_but_normal_tile_keeps_them(monkeypatch):
    from guide_excursions.visual_digest import _draw_date_tile, PALETTES
    from PIL import Image, ImageDraw
    rendered = []
    original = ImageDraw.ImageDraw.text
    def capture(self, xy, text, *args, **kwargs):
        rendered.append(text)
        return original(self, xy, text, *args, **kwargs)
    monkeypatch.setattr(ImageDraw.ImageDraw, 'text', capture)
    draw = ImageDraw.Draw(Image.new('RGB', (250, 200)))
    _draw_date_tile(draw, {'date': '2027-05-30', 'time': '11:00', 'date_prefix': 'до'},
                    174, PALETTES[0].accents[0], PALETTES[0])
    assert rendered == ['МАЯ', 'до', '30']
    rendered.clear()
    _draw_date_tile(draw, {'date': '2027-05-30', 'time': '11:00'},
                    174, PALETTES[0].accents[0], PALETTES[0])
    assert 'вс · 11:00' in rendered
    rendered.clear()
    _draw_date_tile(draw, {'date': '2027-05-30', 'time': '11:00',
                          'date_prefix': 'до', 'daily_hours': '12:00–20:00'},
                    174, PALETTES[0].accents[0], PALETTES[0])
    assert rendered == ['МАЯ', 'до', '30', '12:00–20:00']
