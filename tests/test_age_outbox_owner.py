import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from types import SimpleNamespace

import pytest

import main
import scheduling


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_age_owner_keeps_comments_serial_and_available(tmp_path, monkeypatch, fails):
    db = main.Database(str(tmp_path / 'age-owner.sqlite'))
    await db.init()
    held, release, calendar_held, calendar_release = [asyncio.Event() for _ in range(4)]
    owner = calendar = related = None
    calls = []
    updater = AsyncMock()
    monkeypatch.setitem(main._EVENT_PROGRESS, 1, SimpleNamespace(updater=updater))
    due = datetime.now(timezone.utc) - timedelta(seconds=2)

    async def age_handler(*args):
        calls.append('age')
        held.set()
        await release.wait()
        if fails:
            raise RuntimeError("fixture_age_failure")
        return True

    async def calendar_handler(*args):
        if args[-1] == {'relations': {'semantic': [2]}}:
            calls.append('related')
            return True
        calls.append('calendar')
        calendar_held.set()
        await calendar_release.wait()
        return True

    monkeypatch.setitem(main.JOB_HANDLERS, main.JobTask.event_age_bge_assessment.value, age_handler)
    monkeypatch.setitem(main.JOB_HANDLERS, main.JobTask.vk_calendar_comment.value, calendar_handler)
    monkeypatch.setattr(main, '_maybe_auto_recover_vk_captcha_outbox', AsyncMock(return_value=False))
    try:
        async with db.get_session() as session:
            for eid in (1, 2, 3):
                session.add(main.Event(id=eid, title=f'Event {eid}', description='', source_text='',
                                       date='2099-01-01', time='19:00', location_name='Hall'))
            session.add(main.JobOutbox(event_id=1, task=main.JobTask.event_age_bge_assessment,
                                      coalesce_key='age:1', next_run_at=due, updated_at=due))
            await session.commit()
        owner = asyncio.create_task(main.run_event_age_bge_outbox(db, None))
        await asyncio.wait_for(held.wait(), 3)
        async with db.get_session() as session:
            session.add(main.JobOutbox(event_id=2, task=main.JobTask.event_age_bge_assessment,
                                      coalesce_key='age:2', next_run_at=due, updated_at=due))
            session.add(main.JobOutbox(event_id=3, task=main.JobTask.vk_calendar_comment,
                                      coalesce_key='calendar:3', next_run_at=due, updated_at=due))
            await session.commit()
        calendar = asyncio.create_task(main._run_due_jobs_once(db, None, max_jobs=1))
        await asyncio.wait_for(calendar_held.wait(), 3)
        # Same running row and another due age row cannot start a second batch.
        assert await main.run_event_age_bge_outbox(db, None) == 0
        assert await main._run_due_jobs_once(db, None, allowed_tasks={main.JobTask.event_age_bge_assessment}) == 0
        async with db.get_session() as session:
            session.add(main.JobOutbox(event_id=3, task=main.JobTask.vk_calendar_comment,
                                      payload={'relations': {'semantic': [2]}},
                                      coalesce_key='related:3', next_run_at=due, updated_at=due))
            await session.commit()
        related = asyncio.create_task(main._run_due_jobs_once(db, None, max_jobs=1))
        await asyncio.sleep(0)
        assert not related.done()
        assert calls == ['age', 'calendar'] and not owner.done()
        calendar_release.set()
        assert await asyncio.wait_for(calendar, 3) == 1
        assert await asyncio.wait_for(related, 3) == 1
        assert calls == ['age', 'calendar', 'related'] and not owner.done()
        release.set()
        assert await asyncio.wait_for(owner, 3) == 1
        updater.assert_awaited_once()
        expected_status = main.JobStatus.error if fails else main.JobStatus.done
        assert updater.await_args.args[:3] == (main.JobTask.event_age_bge_assessment, 1, expected_status)
        async with db.get_session() as session:
            jobs = {j.coalesce_key: j for j in (await session.execute(main.select(main.JobOutbox))).scalars()}
            assert jobs['age:1'].status == expected_status
            if fails:
                assert jobs['age:1'].attempts == 1
                assert main._ensure_utc(jobs['age:1'].next_run_at) > due
            assert jobs['age:2'].status == main.JobStatus.pending
            assert jobs['calendar:3'].status == jobs['related:3'].status == main.JobStatus.done
    finally:
        release.set()
        calendar_release.set()
        await asyncio.gather(*(t for t in (owner, calendar, related) if t), return_exceptions=True)
        await db.close()


@pytest.mark.asyncio
async def test_age_owner_scheduler_reuses_existing_enable(monkeypatch):
    monkeypatch.setenv('ENABLE_EVENT_AGE_BGE_ASSESSMENT', '1')
    monkeypatch.setattr(scheduling, 'get_running_main', lambda: main)
    runner = AsyncMock(return_value=0)
    monkeypatch.setattr(main, 'run_event_age_bge_outbox', runner)
    scheduler = scheduling.startup(None, None)
    try:
        job = scheduler.get_job('event_age_bge_outbox')
        assert job.max_instances == 1 and job.coalesce
        await job.func(*job.args)
        runner.assert_awaited_once()
        assert runner.await_args.kwargs['run_id']
    finally:
        scheduling.cleanup()
