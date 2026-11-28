from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

import main
from static_site_release import make_request_payload


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['error', 'pending', 'followup'])
async def test_atomic_new_build_request_clears_old_terminal_reason(tmp_path, monkeypatch, state):
    db = main.Database(str(tmp_path / 'static-terminal.sqlite'))
    await db.init()
    now = datetime.now(timezone.utc)
    old_payload = make_request_payload(reason='old', event_ids=[1], effect_at=now, trigger='operator_request')
    running_payload = {**old_payload, 'remote_handoff': {'run_id': 'immutable-owner'}}
    try:
        async with db.get_session() as session:
            session.add(main.Event(id=1, title='Event', description='', source_text='',
                                   date='2099-01-01', time='19:00', location_name='Hall'))
            if state == 'followup':
                session.add(main.JobOutbox(id=1, event_id=1, task=main.JobTask.static_site_build,
                    status=main.JobStatus.running, payload=running_payload, updated_at=now,
                    next_run_at=now, coalesce_key='static_site_build:prod'))
            session.add(main.JobOutbox(id=2, event_id=1, task=main.JobTask.static_site_build,
                status=main.JobStatus.error if state == 'error' else main.JobStatus.pending,
                terminal_reason='failed', attempts=3, payload=old_payload,
                next_run_at=now, updated_at=now, coalesce_key='static_site_build:prod'))
            await session.commit()
        action = await main.enqueue_static_site_build_request(db, reason='vector_projection_ready', event_ids=[1])
        assert action == {'error': 'requeued', 'pending': 'merged-rearmed', 'followup': 'merged'}[state]
        async with db.get_session() as session:
            pending = await session.get(main.JobOutbox, 2)
            assert pending.status == main.JobStatus.pending
            assert pending.terminal_reason is None
            assert pending.attempts == 0
            if state == 'followup':
                owner = await session.get(main.JobOutbox, 1)
                assert owner.status == main.JobStatus.running
                assert owner.payload == running_payload
                # Complete only the fixture owner, then drain its pending successor.
                owner.status = main.JobStatus.done
                session.add(owner)
            await session.commit()
        handler = AsyncMock(return_value=False)
        monkeypatch.setitem(main.JOB_HANDLERS, 'static_site_build', handler)
        monkeypatch.setattr(main, '_maybe_auto_recover_vk_captcha_outbox', AsyncMock(return_value=False))
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None): return now + timedelta(hours=1)
        monkeypatch.setattr(main, 'datetime', Clock)
        assert await main._run_due_jobs_once(db, None, allowed_tasks={main.JobTask.static_site_build}) == 1
        handler.assert_awaited_once()
        assert await main._run_due_jobs_once(db, None, allowed_tasks={main.JobTask.static_site_build}) == 0
    finally:
        await db.close()
