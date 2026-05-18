import pytest
from datetime import date
from pathlib import Path

import main
from main import Database, WeekendPage, Event


@pytest.mark.asyncio
async def test_sync_weekend_page_no_longer_publishes_vk(tmp_path: Path, monkeypatch):
    # VK weekend navigation post sync retired 2026-05-17: rebuilding the Telegraph weekend
    # page must no longer trigger any `wall.post` / `wall.edit` calls in `klgdevents`.
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    sat = date(2025, 7, 12)
    main.VK_AFISHA_GROUP_ID = '1'

    class DummyTG:
        def create_page(self, title, content):
            return {'url': 'u1', 'path': 'p1'}

        def edit_page(self, path, title=None, content=None):
            pass

    monkeypatch.setattr('main.get_telegraph_token', lambda: 't')
    monkeypatch.setattr('main.Telegraph', lambda access_token=None, domain=None: DummyTG())

    async def fail_post_to_vk(*args, **kwargs):
        raise AssertionError('VK weekend navigation post must not be published')

    async def fail_edit_vk_post(*args, **kwargs):
        raise AssertionError('VK weekend navigation post must not be edited')

    monkeypatch.setattr(main, 'post_to_vk', fail_post_to_vk)
    monkeypatch.setattr(main, 'edit_vk_post', fail_edit_vk_post)

    async with db.get_session() as session:
        session.add(
            Event(
                title='Party',
                description='d',
                source_text='s',
                source_post_url='https://vk.com/wall-1_1',
                date=sat.isoformat(),
                time='10:00',
                location_name='Club',
                city='Kaliningrad',
            )
        )
        await session.commit()

    await main.sync_weekend_page(db, sat.isoformat())

    async with db.get_session() as session:
        wp = await session.get(WeekendPage, sat.isoformat())
        assert wp and not (wp.vk_post_url or '')


@pytest.mark.asyncio
async def test_sync_vk_weekend_post_is_now_noop(tmp_path: Path, monkeypatch):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    sat = date(2025, 7, 12)
    main.VK_AFISHA_GROUP_ID = '1'

    async with db.get_session() as session:
        session.add(
            WeekendPage(
                start=sat.isoformat(),
                url='u1',
                path='p1',
                vk_post_url='https://vk.com/wall-1_1',
            )
        )
        await session.commit()

    async def fail(*args, **kwargs):
        raise AssertionError('retired sync_vk_weekend_post should not touch VK')

    monkeypatch.setattr(main, 'edit_vk_post', fail)
    monkeypatch.setattr(main, 'post_to_vk', fail)

    await main.sync_vk_weekend_post(db, sat.isoformat())

    async with db.get_session() as session:
        page = await session.get(WeekendPage, sat.isoformat())
        assert page.vk_post_url == 'https://vk.com/wall-1_1'
