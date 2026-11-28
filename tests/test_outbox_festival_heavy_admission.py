import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

import heavy_ops
import main
import vk_calendar_comments


async def prepare(db, monkeypatch, *, path=None):
    monkeypatch.setattr(main, 'HEAVY_SEMAPHORE', asyncio.Semaphore(1))
    monkeypatch.setattr(main, 'get_telegraph_token', lambda: 'fixture')
    create = AsyncMock(return_value={'path': 'fixture-page', 'url': 'https://telegra.ph/fixture-page'})
    edit = AsyncMock(return_value=True)
    monkeypatch.setattr(main, 'telegraph_create_page', create)
    monkeypatch.setattr(main, 'telegraph_edit_page', edit)
    monkeypatch.setattr(main, 'sync_festival_vk_post', AsyncMock(return_value=False))
    monkeypatch.setattr(main, 'rebuild_fest_nav_if_changed', AsyncMock(return_value=False))
    due = datetime.now(timezone.utc) - timedelta(seconds=2)
    async with db.get_session() as session:
        session.add(main.Festival(name='Festival', description='Ready description', telegraph_path=path,
                                  telegraph_url=f'https://telegra.ph/{path}' if path else None))
        session.add(main.Event(id=1, title='Festival event', description='Description', source_text='',
                               date='2099-05-07', time='19:00', location_name='Hall', festival='Festival'))
        session.add(main.JobOutbox(event_id=1, task=main.JobTask.festival_pages,
                                  coalesce_key='festival_pages:1', next_run_at=due, updated_at=due))
        await session.commit()
    return create, edit, due


@pytest.mark.asyncio
async def test_busy_festival_leaves_worker_available_for_later_comment(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'worker.sqlite'))
    await db.init()
    release, held = asyncio.Event(), asyncio.Event()
    owner = cycle = None
    try:
        create, _, due = await prepare(db, monkeypatch)
        async def importing():
            async with heavy_ops.heavy_operation(kind='vk_auto_import', trigger='test'):
                held.set()
                await release.wait()
        owner = asyncio.create_task(importing())
        await held.wait()
        # No comment exists at selection time: priority sorting cannot hide the inversion.
        cycle = asyncio.create_task(main._run_due_jobs_once(db, None, max_jobs=1))
        assert await asyncio.wait_for(cycle, 3) == 0
        async with db.get_session() as session:
            job = (await session.execute(main.select(main.JobOutbox))).scalars().one()
            assert job.status == main.JobStatus.pending and job.attempts == 0
            assert main._ensure_utc(job.updated_at) == due
            assert main._ensure_utc(job.next_run_at) == due
            session.add(main.Event(id=2, title='Announcement', description='', source_text='',
                                   date='2099-05-07', time='19:00', location_name='Hall',
                                   source_vk_post_url='https://vk.com/wall-231920894_20'))
            session.add(main.JobOutbox(event_id=2, task=main.JobTask.vk_calendar_comment,
                                      coalesce_key='calendar:2', next_run_at=due,
                                      payload={'post_url': 'https://vk.com/wall-231920894_20',
                                               'publish_date': int(due.timestamp()) - 3600}))
            await session.commit()
        public = {'id': 20, 'owner_id': -231920894, 'post_type': 'post',
                  'date': int(due.timestamp()) - 3600, 'text': 'Announcement'}
        monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
        monkeypatch.setattr(main, '_vk_api', AsyncMock(return_value={'response': {'items': [public]}}))
        send = AsyncMock(return_value=True)
        monkeypatch.setattr(vk_calendar_comments, 'ensure_calendar_comment', send)
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 1
        send.assert_awaited_once()
        assert send.call_args.args[2] == 'https://vk.com/wall-231920894_20'
        assert not owner.done()
        create.assert_not_awaited()
        release.set()
        await owner
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 1
        create.assert_awaited_once()
        async with db.get_session() as session:
            festival = (await session.execute(main.select(main.Festival))).scalars().one()
            job = (await session.execute(main.select(main.JobOutbox).where(
                main.JobOutbox.task == main.JobTask.festival_pages))).scalars().one()
            assert festival.telegraph_path == 'fixture-page'
            assert job.status == main.JobStatus.done
    finally:
        if cycle and not cycle.done(): cycle.cancel()
        release.set()
        if owner: await owner
        if cycle:
            await asyncio.gather(cycle, return_exceptions=True)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('old_path', [None, 'old-page'])
async def test_direct_sync_keeps_create_and_recreate_serialized_with_worker(tmp_path, monkeypatch, old_path):
    db = main.Database(str(tmp_path / 'concurrent.sqlite'))
    await db.init()
    entered, release = asyncio.Event(), asyncio.Event()
    direct = cycle = None
    try:
        create, edit, _ = await prepare(db, monkeypatch, path=old_path)
        if old_path:
            edit.side_effect = [RuntimeError('PAGE_ACCESS_DENIED'), True]
        async def create_page(*args, **kwargs):
            entered.set()
            await release.wait()
            return {'path': 'fixture-page', 'url': 'https://telegra.ph/fixture-page'}
        create.side_effect = create_page
        direct = asyncio.create_task(main.sync_festival_page(db, 'Festival'))
        await asyncio.wait_for(entered.wait(), 3)
        cycle = asyncio.create_task(main._run_due_jobs_once(db, None, max_jobs=1))
        assert await asyncio.wait_for(cycle, 3) == 0
        create.assert_awaited_once()
        release.set()
        assert await direct == 'https://telegra.ph/fixture-page'
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 1
        create.assert_awaited_once()
        assert edit.call_args.args[1] == 'fixture-page'
        async with db.get_session() as session:
            festival = (await session.execute(main.select(main.Festival))).scalars().one()
            assert festival.telegraph_path == 'fixture-page'
    finally:
        if cycle and not cycle.done(): cycle.cancel()
        release.set()
        if direct: await asyncio.gather(direct, return_exceptions=True)
        if cycle: await asyncio.gather(cycle, return_exceptions=True)
        await db.close()
