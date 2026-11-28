from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main
import vk_calendar_comments
import vk_event_relations


@pytest.mark.asyncio
@pytest.mark.parametrize('related', [False, True])
@pytest.mark.parametrize('matching', [0, 1, 2])
async def test_sent_header_survives_canonical_title_change(tmp_path, monkeypatch, related, matching):
    db = main.Database(str(tmp_path / 'sent-title.sqlite'))
    await db.init()
    slot = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
    old_url = 'https://vk.com/wall-231920894_10'
    try:
        async with db.get_session() as session:
            session.add(main.Event(id=1, title='Sent title A', description='', source_text='',
                                   date='2099-01-01', time='19:00', location_name='Hall'))
            await session.commit()
        monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'choose_vk_actor', lambda *a: [SimpleNamespace(kind='group', token='fixture', label='fixture')])
        monkeypatch.setattr(main, '_resolve_vk_postponed_wall_id_any_actor', AsyncMock(return_value=10))
        items = [{'id': 30, 'owner_id': -231920894, 'post_type': 'post', 'date': slot,
                  'text': 'Canonical title B\nBody'}]
        items += [{'id': 20+i, 'owner_id': -231920894, 'post_type': 'post', 'date': slot,
                   'text': 'Sent title A\nBody'} for i in range(matching)]
        async def api(method, params, *args, **kw):
            if method == 'wall.post':
                assert params['message'] == '\n Sent title A \nBody'
                return {'response': {'post_id': 10}}
            assert method == 'wall.get'
            return {'response': {'items': items}}
        monkeypatch.setattr(main, '_vk_api', api)
        assert await main.post_to_vk('231920894', '\n Sent title A \nBody', db, None,
                                     publish_date=slot, source_event_id=1) == old_url
        async with db.get_session() as session:
            ev = await session.get(main.Event, 1)
            ev.title = 'Canonical title B'
            ev.source_vk_post_url = old_url
            session.add(ev)
            await session.commit()
            job = (await session.execute(main.select(main.JobOutbox))).scalars().one()
            payload = dict(job.payload)
        if related:
            payload = {'post_url': old_url, 'publish_date': slot, 'relations': {'related': []}}
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.fromtimestamp(slot + 1800, timezone.utc)
        monkeypatch.setattr(main, 'datetime', Clock)
        calendar_send, related_send = AsyncMock(return_value=True), AsyncMock(return_value=True)
        monkeypatch.setattr(vk_calendar_comments, 'ensure_calendar_comment', calendar_send)
        monkeypatch.setattr(vk_event_relations, 'ensure_comment', related_send)
        if matching == 1:
            assert await main.job_vk_calendar_comment(1, db, None, payload)
            sent = related_send if related else calendar_send
            sent.assert_awaited_once()
            assert sent.await_args.args[2] == 'https://vk.com/wall-231920894_20'
        else:
            with pytest.raises(RuntimeError, match='post_missing|public_post_not_found'):
                await main.job_vk_calendar_comment(1, db, None, payload)
            calendar_send.assert_not_awaited()
            related_send.assert_not_awaited()
    finally:
        await db.close()
