from datetime import datetime as RealDatetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main


def ts(day, hour=0, minute=0):
    return int(RealDatetime(2026, 11, day, hour, minute, tzinfo=main.LOCAL_TZ).timestamp())


def configure(monkeypatch, *, slot, now_day=28):
    class Clock(RealDatetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 11, now_day, 10, tzinfo=main.LOCAL_TZ)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    monkeypatch.setattr(main, 'datetime', Clock)
    monkeypatch.setattr(main, 'VK_POSTPONED_ENABLED', True)
    monkeypatch.setattr(main, 'choose_vk_actor', lambda *args: [
        SimpleNamespace(kind='group', token='fixture', label='fixture')])
    reserve = AsyncMock(return_value=slot)
    api = AsyncMock(return_value={'response': {'post_id': 20}})
    monkeypatch.setattr(main, '_reserve_vk_postponed_publish_date', reserve)
    monkeypatch.setattr(main, '_vk_api', api)
    monkeypatch.setattr(main, '_resolve_vk_postponed_wall_id_any_actor', AsyncMock(return_value=20))
    return reserve, api


def event(**kwargs):
    data = dict(id=1, title='Workshop', description='', source_text='', date='2026-11-28',
                time='', location_name='Hall')
    data.update(kwargs)
    return main.Event(**data)


@pytest.mark.asyncio
@pytest.mark.parametrize('explicit', [False, True])
@pytest.mark.parametrize('fields,day,hour,minute,accepted', [
    ({'date': ''}, 30, 15, 0, True),
    ({'end_date': '2026-11-29', 'end_date_is_inferred': False, 'time': '19:00'}, 29, 23, 59, True),
    ({'end_date': '2026-11-29', 'end_date_is_inferred': False, 'time': '19:00'}, 30, 0, 0, False),
    ({}, 28, 23, 59, True), ({}, 29, 0, 0, False), ({}, 30, 7, 20, False),
    ({'time': '19:00'}, 28, 18, 59, True), ({'time': '19:00'}, 28, 19, 0, False),
    ({'end_date': '2026-11-29', 'end_date_is_inferred': False}, 29, 23, 59, True),
    ({'end_date': '2026-11-29', 'end_date_is_inferred': False}, 30, 0, 0, False),
    ({'date': '2026-11-28..2026-11-29'}, 29, 23, 59, True),
    ({'date': '2026-11-28..2026-11-29'}, 30, 0, 0, False),
    ({'end_date': '2026-11-30', 'end_date_is_inferred': True}, 29, 0, 0, False),
    ({'date': '2026-11-28..2026-11-29', 'end_date': '2026-12-20',
      'end_date_is_inferred': True}, 30, 0, 0, False),
    ({'time': 'по записи'}, 29, 0, 0, False),
    ({'time': '25:70'}, 29, 0, 0, False),
])
async def test_actual_post_respects_canonical_publication_window(
    monkeypatch, explicit, fields, day, hour, minute, accepted
):
    slot = ts(day, hour, minute)
    reserve, api = configure(monkeypatch, slot=slot)
    kwargs = {'publish_date': slot} if explicit else {}
    result = await main.post_to_vk('231920894', 'Workshop', source_event_id=1,
        latest_publish_ts=main._event_publication_start_deadline_ts(event(**fields)), **kwargs)
    if accepted:
        assert result == 'https://vk.com/wall-231920894_20'
        api.assert_awaited_once()
        assert api.call_args.args[0] == 'wall.post'
    else:
        assert result is None
        api.assert_not_awaited()
    assert reserve.await_count == (0 if explicit else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize('now_day,accepted', [(28, True), (29, False)])
async def test_immediate_date_only_post_keeps_event_day(monkeypatch, now_day, accepted):
    _, api = configure(monkeypatch, slot=None, now_day=now_day)
    result = await main.post_to_vk('231920894', 'Workshop', publish_date=0,
        source_event_id=1, latest_publish_ts=main._event_publication_start_deadline_ts(event()))
    assert bool(result) is accepted
    assert api.await_count == int(accepted)


@pytest.mark.asyncio
@pytest.mark.parametrize('slot_day,accepted', [(28, True), (30, False)])
async def test_public_cta_and_plain_fallback_share_publication_window(monkeypatch, slot_day, accepted):
    _, api = configure(monkeypatch, slot=ts(slot_day, 15))
    monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
    monkeypatch.setattr(main, 'VK_PHOTOS_ENABLED', True)
    monkeypatch.setattr(main, 'VK_TOKEN_AFISHA', 'fixture')
    monkeypatch.setattr(main, 'upload_vk_photo', AsyncMock(return_value='photo1'))
    calls = []
    async def public_cta(**kwargs):
        assert kwargs['public_only'] is True
        calls.append(kwargs)
        return await kwargs['post_to_vk_fn'](kwargs['target_group_id'], kwargs['message'],
            attachments=['photo1'], source_event_id=1)
    monkeypatch.setattr('afishaengagement.maybe_publish_shadow_debug_copy', public_cta)
    result = await main.sync_vk_source_post(event(photo_urls=['https://example.test/poster.jpg']),
                                            'Ready text', None, None)
    assert len(calls) == 1
    assert bool(result) is accepted
    assert api.await_count == int(accepted)


@pytest.mark.asyncio
async def test_rejected_date_only_slot_keeps_initial_publication_unwritten(tmp_path, monkeypatch):
    from db import Database
    import vk_event_relations

    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'default')
    _, api = configure(monkeypatch, slot=ts(30, 7, 20))
    monkeypatch.setitem(main.vk_group_blocked, 'wall.post', 0.0)
    monkeypatch.setattr(main, '_should_skip_ticket_giveaway_publication', AsyncMock(return_value=False))
    monkeypatch.setattr(vk_event_relations, 'presentation', AsyncMock(return_value={}))
    comment = AsyncMock()
    site = AsyncMock()
    monkeypatch.setattr(vk_event_relations, 'ensure_comment', comment)
    monkeypatch.setattr(main, '_enqueue_static_site_after_vk_publication', site)

    async def sync(current, *args, **kwargs):
        return await main.post_to_vk('231920894', 'Workshop', source_event_id=current.id,
            latest_publish_ts=main._event_publication_start_deadline_ts(current))

    monkeypatch.setattr(main, 'sync_vk_source_post', sync)
    db = Database(str(tmp_path / 'events.sqlite'))
    try:
        await db.init()
        current = event()
        async with db.get_session() as session:
            session.add(current)
            await session.commit()
        result = await main.job_sync_vk_source_post(current.id, db, None)
        async with db.get_session() as session:
            saved = await session.get(main.Event, current.id)
            assert saved.vk_source_hash is None
            assert saved.source_vk_post_url is None
            assert not list((await session.execute(main.select(main.JobOutbox))).scalars())
        assert result is False
        api.assert_not_awaited()
        comment.assert_not_awaited()
        site.assert_not_awaited()
    finally:
        await db.close()
