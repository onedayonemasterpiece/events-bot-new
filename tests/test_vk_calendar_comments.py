from datetime import date

import pytest
from unittest.mock import AsyncMock
from sqlmodel import select
import main
from db import Database
from models import Event, Setting
import vk_calendar_comments as comments


@pytest.fixture(autouse=True)
def no_calendar_pacing_wait(monkeypatch):
    monkeypatch.setattr(comments.asyncio, "sleep", AsyncMock())


def event(**kw):
    values=dict(id=1,title='Лекция',date='2026-11-14',time='18:00',location_name='Музей',description='Программа',source_text='Исходник',source_vk_post_url='https://vk.com/wall-231920894_10')
    values.update(kw)
    return Event(**values)


@pytest.mark.asyncio
async def test_calendar_preserves_time_and_explicit_all_day_span():
    timed=event()
    exhibition=event(id=2,time='',date='2026-11-14',end_date='2026-11-16')
    raw=await comments.calendar_bytes(None,[timed,exhibition])
    assert raw==await comments.calendar_bytes(None,[timed,exhibition])
    parts=main.Calendar.from_ical(raw).walk('VEVENT')
    assert parts[0].decoded('DTSTART').hour==18
    assert parts[1].decoded('DTSTART')==date(2026,11,14)
    assert parts[1].decoded('DTEND')==date(2026,11,17)
    assert str(parts[0]['UID'])!=str(parts[1]['UID'])


@pytest.mark.asyncio
async def test_bad_date_is_not_invented():
    with pytest.raises(ValueError,match='exact event date'):
        await comments.calendar_bytes(None,[event(date='')])


@pytest.mark.asyncio
async def test_new_post_schedules_user_calendar_after_actual_public_time_once(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    from models import JobOutbox, JobStatus, JobTask
    db = Database(str(tmp_path / 'scheduled.sqlite'))
    await db.init()
    try:
        ev = event(source_vk_post_url=None)
        async with db.get_session() as session:
            session.add(ev)
            session.add(event(id=2, source_vk_post_url='https://vk.com/wall-231920894_5'))
            session.add(event(id=3, time='19:00', source_vk_post_url='https://vk.com/wall-231920894_10'))
            await session.commit()
        monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'VK_TOKEN_AFISHA', 'group-fixture')
        monkeypatch.setattr(main, 'VK_USER_TOKEN', 'user-fixture')
        monkeypatch.setattr(main, 'VK_SERVICE_TOKEN', 'service-fixture')
        monkeypatch.setattr(main, '_vk_captcha_needed', False)
        monkeypatch.setattr(main, '_vk_user_token', lambda: 'user-fixture')
        monkeypatch.setattr(main, 'get_bot', lambda: None)
        published_at = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
        calls = []
        async def api(method, params, *args, **kw):
            calls.append((method, kw.get('token_kind')))
            if method == 'wall.post':
                return {'response': {'post_id': 10}}
            if method == 'wall.get':
                if params['filter'] == 'postponed':
                    return {'response': {'items': [{'id': 10, 'date': published_at}]}}
                assert kw['token_kind'] == 'service'
                return {'response': {'items': [
                    {'id': 5, 'date': published_at-86400, 'text': 'Старый пост'},
                    # A prior gallery edit may have made the actual public date earlier.
                    {'id': 20, 'date': published_at+120, 'post_type': 'post',
                     'text': 'Лекция\n' + next(line for line in main.build_vk_source_header(ev, None) if line.startswith('📅 '))},
                ]}}
            if method == 'docs.getWallUploadServer':
                return {'response': {'upload_url': 'https://upload.example/file'}}
            if method == 'docs.save':
                return {'response': {'doc': {'id': 30, 'owner_id': 20, 'ext': 'ics'}}}
            if method == 'wall.createComment':
                assert params['post_id'] == 20
                assert 'from_group' not in params
                assert params['attachments'] == 'doc20_30'
                return {'response': {'comment_id': 90}}
            raise AssertionError(method)
        monkeypatch.setattr(main, '_vk_api', api)
        class Response:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def raise_for_status(self): pass
            async def json(self, **kw): return {'file': 'uploaded'}
        class HTTP:
            def post(self, *args, **kw): return Response()
        monkeypatch.setattr(main, 'get_http_session', lambda: HTTP())
        url = await main.post_to_vk('231920894', 'Анонс', db, None,
                                    publish_date=published_at, source_event_id=1)
        assert url == 'https://vk.com/wall-231920894_10'
        assert all(not method.startswith('docs.') and method != 'wall.createComment' for method, _ in calls)
        async with db.get_session() as session:
            stored = await session.get(Event, 1)
            stored.source_vk_post_url = url
            session.add(stored)
            await session.commit()
            jobs = list((await session.execute(select(JobOutbox))).scalars())
        assert len(jobs) == 1 and jobs[0].task == JobTask.vk_calendar_comment
        due = jobs[0].next_run_at.replace(tzinfo=timezone.utc)
        assert 300 <= due.timestamp() - published_at <= 900
        before = list(calls)
        assert await main._run_due_jobs_once(db, None) == 0
        import asyncio
        def finish_idle_cycle():
            raise asyncio.CancelledError()
        with monkeypatch.context() as idle_patch:
            idle_patch.setattr(main, '_mark_job_outbox_worker_cycle_ok', finish_idle_cycle)
            with pytest.raises(asyncio.CancelledError):
                await main.job_outbox_worker(db, None)
        assert calls == before  # An idle worker must not scan our public wall.
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None): return due + timedelta(seconds=1)
        monkeypatch.setattr(main, 'datetime', Clock)
        assert await main._run_due_jobs_once(db, None) == 1
        assert [method for method, _ in calls[len(before):]] == ['wall.get']
        async with db.get_session() as session:
            deferred = await session.get(JobOutbox, jobs[0].id)
            assert deferred.status == JobStatus.pending
            assert deferred.attempts == 0
            assert deferred.next_run_at.replace(tzinfo=timezone.utc) == due + timedelta(seconds=120)
        class PublicClock(datetime):
            @classmethod
            def now(cls, tz=None): return due + timedelta(seconds=121)
        monkeypatch.setattr(main, 'datetime', PublicClock)
        before = list(calls)
        assert await main._run_due_jobs_once(db, None) == 1
        assert [method for method, _ in calls[len(before):]] == [
            'wall.get', 'docs.getWallUploadServer', 'docs.save', 'wall.createComment']
        async with db.get_session() as session:
            assert (await session.get(Event, 1)).source_vk_post_url == 'https://vk.com/wall-231920894_20'
            assert (await session.get(Event, 2)).source_vk_post_url.endswith('_5')
            assert (await session.get(Event, 3)).source_vk_post_url.endswith('_20')
            assert (await session.get(JobOutbox, jobs[0].id)).status == JobStatus.done
        after = list(calls)
        assert await main._run_due_jobs_once(db, None) == 0
        assert not await comments.ensure_calendar_comment(db, ev, 'https://vk.com/wall-231920894_20')
        assert calls == after
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('author', [-231920894, 20])
async def test_lost_comment_response_reuses_saved_document_without_second_write(tmp_path, monkeypatch, author):
    db = Database(str(tmp_path / 'lost-response.sqlite'))
    await db.init()
    try:
        ev = event()
        async with db.get_session() as session:
            session.add(ev)
            await session.commit()
        monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
        monkeypatch.setattr(main, '_vk_user_token', lambda: 'fixture')
        calls = []
        async def api(method, params, *args, **kw):
            calls.append(method)
            if method == 'docs.getWallUploadServer':
                return {'response': {'upload_url': 'https://upload.example/file'}}
            if method == 'docs.save':
                return {'response': {'doc': {'id': 30, 'owner_id': 20, 'ext': 'ics'}}}
            if method == 'wall.createComment':
                raise RuntimeError('Lost response')
            if method == 'wall.getComments':
                return {'response': {'count': 1, 'items': [{'id': 90, 'from_id': author,
                    'attachments': [{'type': 'doc', 'doc': {'id': 30, 'owner_id': 20, 'ext': 'ics'}}]}]}}
            raise AssertionError(method)
        monkeypatch.setattr(main, '_vk_api', api)
        class Response:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def raise_for_status(self): pass
            async def json(self, **kw): return {'file': 'uploaded'}
        class HTTP:
            def post(self, *args, **kw): return Response()
        monkeypatch.setattr(main, 'get_http_session', lambda: HTTP())
        with pytest.raises(RuntimeError, match='Lost response'):
            await comments.ensure_calendar_comment(db, ev, ev.source_vk_post_url)
        assert not await comments.ensure_calendar_comment(db, ev, ev.source_vk_post_url)
        assert calls == ['docs.getWallUploadServer', 'docs.save', 'wall.createComment', 'wall.getComments']
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_document_save_failure_never_creates_calendar_comment(tmp_path, monkeypatch):
    db = Database(str(tmp_path / 'save-failure.sqlite'))
    await db.init()
    try:
        ev = event()
        async with db.get_session() as session:
            session.add(ev)
            await session.commit()
        monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
        monkeypatch.setattr(main, '_vk_user_token', lambda: 'fixture')
        calls = []
        async def api(method, params, *args, **kw):
            calls.append(method)
            if method == 'docs.getWallUploadServer':
                return {'response': {'upload_url': 'https://upload.example/file'}}
            if method == 'docs.save':
                raise main.VKAPIError(14, 'Fixture CAPTCHA', method=method)
            raise AssertionError(method)
        monkeypatch.setattr(main, '_vk_api', api)
        class Response:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def raise_for_status(self): pass
            async def json(self, **kw): return {'file': 'uploaded'}
        class HTTP:
            def post(self, *args, **kw): return Response()
        monkeypatch.setattr(main, 'get_http_session', lambda: HTTP())
        with pytest.raises(main.VKAPIError):
            await comments.ensure_calendar_comment(db, ev, ev.source_vk_post_url)
        assert calls == ['docs.getWallUploadServer', 'docs.save']
        async with db.get_session() as session:
            assert await session.get(Setting, 'vk_calendar_comment:-231920894:10') is None
        assert comments.asyncio.sleep.await_args_list
        assert all(call.args == (4,) for call in comments.asyncio.sleep.await_args_list)
    finally:
        await db.close()
