"""Pinned actual worker, real SQLite/recovery; external VK methods are mocked."""
import copy
import pytest
import main

@pytest.mark.asyncio
@pytest.mark.parametrize(
    'starting_attempts,failure_code,restart,reenqueue',
    [(0, 14, False, True), (5, 14, True, False),
     (5, 10, False, False), (0, 14, False, 'none')],
)
async def test_exact_captcha_counter_preserves_other_failure_budget_and_frozen_comment(
    tmp_path, monkeypatch, caplog, starting_attempts, failure_code, restart, reenqueue
):
    monkeypatch.setattr(main, 'CAPTCHA_MAX_ATTEMPTS', 2)
    monkeypatch.setattr(main, 'VK_CAPTCHA_AUTO_RECOVERY_SEC', 0)
    monkeypatch.setattr(main, 'VK_CAPTCHA_RESUME_SPACING_SEC', 0)
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    monkeypatch.setattr(main, '_vk_user_token', lambda: 'offline-only')
    probes = []

    async def read_probe(method, params, *args, **kwargs):
        assert method == 'wall.get' and kwargs['skip_captcha'] is True
        probes.append(method)
        return {'response': {'count': 0, 'items': []}}
    monkeypatch.setattr(main, '_vk_api', read_probe)
    frozen = {'post_url': 'https://vk.com/wall-231920894_10', 'publish_date': 0, 'relations': {'dates': [], 'related': [{'id': 101, 'title': 'Frozen peer', 'url': 'https://vk.com/wall-231920894_9'}]}}
    path = str(tmp_path / 'local.sqlite')
    db = main.Database(path)
    await db.init()
    try:
        async with db.get_session() as session:
            events = [main.Event(title=t, description='facts', date='2099-01-04', time='12:00', location_name='Venue', source_text='facts') for t in ['culprit', 'innocent-comment', 'other-vk-work']]
            session.add_all(events)
            await session.commit()
            for event in events:
                await session.refresh(event)
            jobs = [main.JobOutbox(event_id=events[0].id, task=main.JobTask.vk_calendar_comment, payload=copy.deepcopy(frozen), attempts=starting_attempts), main.JobOutbox(event_id=events[1].id, task=main.JobTask.vk_calendar_comment, payload=copy.deepcopy(frozen)), main.JobOutbox(event_id=events[2].id, task=main.JobTask.weekend_pages, payload={'opaque': 'unchanged'})]
            session.add_all(jobs)
            await session.commit()
            for job in jobs:
                await session.refresh(job)
            job_ids = [job.id for job in jobs]
            event_ids = [event.id for event in events]
        calls = []

        async def handler(event_id, *args):
            calls.append(event_id)
            if event_id == event_ids[0]:
                if failure_code == 14:
                    main._vk_captcha_needed = True
                raise main.VKAPIError(failure_code, 'offline document error', method='docs.save')
            return True
        monkeypatch.setitem(main.JOB_HANDLERS, 'vk_calendar_comment', handler)
        monkeypatch.setitem(main.JOB_HANDLERS, 'weekend_pages', handler)
        for tick in range(10):
            monkeypatch.setattr(main, '_vk_captcha_auto_probe_last_monotonic', 0.0)
            await main._run_due_jobs_once(db, None, allowed_tasks={main.JobTask.vk_calendar_comment, main.JobTask.weekend_pages}, max_jobs=1)
            if tick == 0 and reenqueue:
                if reenqueue == 'none':
                    async with db.get_session() as session:
                        row = await session.get(main.JobOutbox, job_ids[0])
                        row.status = main.JobStatus.error
                        row.last_error = 'offline generic transport failure'
                        await session.commit()
                await main.enqueue_job(db, event_ids[0], main.JobTask.vk_calendar_comment, payload=None if reenqueue == 'none' else copy.deepcopy(frozen))
                if reenqueue == 'none':
                    async with db.get_session() as session:
                        row = await session.get(main.JobOutbox, job_ids[0])
                        assert row.payload == {**frozen, 'captcha_attempts': 1}, row.payload
                    await main.enqueue_job(db, event_ids[0], main.JobTask.vk_calendar_comment, payload=copy.deepcopy(frozen))
            if tick == 0 and restart:
                await db.close()
                db = main.Database(path)
                await db.init()
                monkeypatch.setattr(main, '_vk_captcha_needed', False)
                monkeypatch.setattr(main, '_vk_captcha_resume', None)
        async with db.get_session() as session:
            culprit, innocent, other = [await session.get(main.JobOutbox, i) for i in job_ids]
            if failure_code == 14:
                assert (culprit.payload or {}).get('captcha_attempts') == 2, (culprit.payload, culprit.status, calls)
                assert culprit.attempts == starting_attempts
                assert culprit.status == main.JobStatus.error and culprit.terminal_reason == 'failed'
                assert calls.count(event_ids[0]) == 2
                expected = copy.deepcopy(frozen)
                expected['captcha_attempts'] = 2
                assert culprit.payload == expected
                assert len(probes) == 2
            else:
                assert culprit.attempts == starting_attempts + 1
                assert culprit.status == main.JobStatus.error and culprit.terminal_reason is None
                assert culprit.payload == frozen and calls.count(event_ids[0]) == 1 and (probes == [])
            assert innocent.status == main.JobStatus.done and innocent.attempts == 0 and (innocent.payload == frozen)
            assert other.status == main.JobStatus.done and other.attempts == 0 and (other.payload == {'opaque': 'unchanged'})
        assert calls.count(event_ids[1]) == 1 and calls.count(event_ids[2]) == 1
        terminal_logs = [record.getMessage() for record in caplog.records if record.getMessage().startswith('VK_CAPTCHA_TERMINAL')]
        assert len(terminal_logs) == (1 if failure_code == 14 else 0)
        if terminal_logs:
            assert 'captcha_attempts=2 limit=2' in terminal_logs[0]
        async with db.get_session() as session:
            isolated = main.JobOutbox(event_id=event_ids[2], task=main.JobTask.telegraph_build, payload={'captcha_attempts': 99})
            session.add(isolated)
            await session.commit()
            await session.refresh(isolated)
            isolated_id = isolated.id
        await main.enqueue_job(db, event_ids[2], main.JobTask.telegraph_build, payload={'opaque': 'new'})
        async with db.get_session() as session:
            isolated = await session.get(main.JobOutbox, isolated_id)
            assert isolated.payload == {'opaque': 'new'}
        if failure_code == 14:
            incoming = copy.deepcopy(frozen) if starting_attempts == 0 else None
            result = await main.enqueue_job(db, event_ids[0], main.JobTask.vk_calendar_comment, payload=incoming)
            for _ in range(2):
                monkeypatch.setattr(main, '_vk_captcha_auto_probe_last_monotonic', 0.0)
                await main._run_due_jobs_once(db, None, allowed_tasks={main.JobTask.vk_calendar_comment}, max_jobs=1)
            assert calls.count(event_ids[0]) == 2, ('exhausted job executed again', result, calls)
            assert result == 'skipped'
            async with db.get_session() as session:
                exhausted = await session.get(main.JobOutbox, job_ids[0])
                assert exhausted.status == main.JobStatus.error and exhausted.terminal_reason == 'failed'
                assert exhausted.payload == expected and exhausted.attempts == starting_attempts
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_exhausted_captcha_keeps_restart_pause_with_only_future_work(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    monkeypatch.setattr(main, 'CAPTCHA_MAX_ATTEMPTS', 2)
    monkeypatch.setattr(main, 'VK_CAPTCHA_AUTO_RECOVERY_SEC', 0)
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    monkeypatch.setattr(main, '_vk_captcha_resume', None)
    monkeypatch.setattr(main, '_vk_user_token', lambda: 'offline-only')
    calls, probes = [], []
    blocked = True

    async def handler(event_id, *args):
        calls.append(event_id)
        main._vk_captcha_needed = True
        raise main.VKAPIError(14, 'offline challenge', method='docs.save')

    async def probe(method, params, *args, **kwargs):
        assert method == 'wall.get' and kwargs['skip_captcha'] is True
        probes.append(method)
        if blocked:
            raise main.VKAPIError(14, 'offline challenge', method=method)
        return {'response': {'count': 0, 'items': []}}

    monkeypatch.setitem(main.JOB_HANDLERS, 'vk_calendar_comment', handler)
    monkeypatch.setattr(main, '_vk_api', probe)
    path = str(tmp_path / 'terminal-marker.sqlite')
    db = main.Database(path)
    await db.init()
    future_due = datetime.now(timezone.utc) + timedelta(hours=8)
    try:
        async with db.get_session() as session:
            event = main.Event(title='Future', description='facts', source_text='facts',
                               date='2099-01-04', time='12:00', location_name='Venue')
            session.add(event)
            await session.flush()
            culprit = main.JobOutbox(event_id=event.id, task=main.JobTask.vk_calendar_comment,
                                     payload={'captcha_attempts': 1}, attempts=7)
            future = main.JobOutbox(event_id=event.id, task=main.JobTask.vk_sync,
                                    status=main.JobStatus.error, attempts=4,
                                    next_run_at=future_due, last_error='vk_sync_missing_media')
            session.add_all([culprit, future])
            await session.flush()
            culprit_id, future_id = culprit.id, future.id
            await session.commit()
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 1
        await db.close()
        db = main.Database(path)
        await db.init()
        monkeypatch.setattr(main, '_vk_captcha_needed', False)
        monkeypatch.setattr(main, '_vk_captcha_resume', None)
        monkeypatch.setattr(main, '_vk_captcha_auto_probe_last_monotonic', 0.0)
        assert await main._maybe_auto_recover_vk_captcha_outbox(db, None) is True
        async with db.get_session() as session:
            row = await session.get(main.JobOutbox, culprit_id)
            assert row.status == main.JobStatus.paused and row.terminal_reason == 'failed'
            terminal_snapshot = (row.last_error, row.attempts, row.payload, row.next_run_at)
        blocked = False
        monkeypatch.setattr(main, '_vk_captcha_auto_probe_last_monotonic', 0.0)
        assert await main._maybe_auto_recover_vk_captcha_outbox(db, None) is False
        assert await main._maybe_auto_recover_vk_captcha_outbox(db, None) is False
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 0
        async with db.get_session() as session:
            row = await session.get(main.JobOutbox, culprit_id)
            peer = await session.get(main.JobOutbox, future_id)
            assert row.status == main.JobStatus.error and row.terminal_reason == 'failed'
            assert (row.last_error, row.attempts, row.payload, row.next_run_at) == terminal_snapshot
            assert row.payload == {'captcha_attempts': 2}
            assert peer.status == main.JobStatus.error and peer.attempts == 4
            assert peer.last_error == 'vk_sync_missing_media'
            assert main._ensure_utc(peer.next_run_at) == future_due
        assert len(calls) == 1 and probes == ['wall.get', 'wall.get']
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_consuming_terminal_marker_does_not_revive_due_failed_job(tmp_path, monkeypatch):
    marker = 'captcha_wait:terminal-due'
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    calls = []

    async def handler(event_id, *args):
        calls.append(event_id)
        return True

    monkeypatch.setitem(main.JOB_HANDLERS, 'vk_calendar_comment', handler)
    db = main.Database(str(tmp_path / 'due-terminal.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            row = main.JobOutbox(event_id=0, task=main.JobTask.vk_calendar_comment,
                                 status=main.JobStatus.paused, terminal_reason='failed',
                                 last_error=marker, payload={'captcha_attempts': 2})
            session.add(row)
            await session.flush()
            job_id = row.id
            await session.commit()
        assert await main._resume_vk_captcha_outbox(db, marker) == 0
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 0
        async with db.get_session() as session:
            row = await session.get(main.JobOutbox, job_id)
            assert row.status == main.JobStatus.error and row.terminal_reason == 'failed'
            assert row.last_error == marker and row.payload == {'captcha_attempts': 2}
        assert calls == []
    finally:
        await db.close()
