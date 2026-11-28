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
