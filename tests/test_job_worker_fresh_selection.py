"""A newly due media dependency must not wait for an old whole-batch snapshot."""
import asyncio
import contextlib
from datetime import datetime, timedelta, timezone

import pytest

import main


@pytest.mark.asyncio
@pytest.mark.parametrize("background", [True, False], ids=["background-refresh", "explicit-full-drain"])
async def test_new_media_precedes_remaining_background_batch(tmp_path, monkeypatch, background):
    db = main.Database(str(tmp_path / "fresh-selection.sqlite"))
    await db.init()
    ids = []
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        for title in ("first", "second", "new media"):
            event = main.Event(title=title, description="d", source_text="src", date="2099-01-01", time="12:00", location_name="venue")
            session.add(event)
            await session.flush()
            ids.append(int(event.id))
        for eid in ids[:2]:
            session.add(main.JobOutbox(event_id=eid, task=main.JobTask.festival_pages, next_run_at=now - timedelta(seconds=1)))
        await session.commit()
    calls = []
    two_done = asyncio.Event()

    async def handler(eid, _db, _bot, *args):
        calls.append(eid)
        if eid == ids[0]:
            async with db.get_session() as session:
                session.add(main.JobOutbox(event_id=ids[2], task=main.JobTask.event_media_review, next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
                await session.commit()
        if len(calls) == 2:
            two_done.set()
        return None

    async def noop(*args, **kwargs):
        return None

    monkeypatch.setitem(main.JOB_HANDLERS, "festival_pages", handler)
    monkeypatch.setitem(main.JOB_HANDLERS, "event_media_review", handler)
    monkeypatch.setattr(main, "_watch_nav_jobs", noop)
    monkeypatch.setattr(main, "_log_job_outbox_stats", noop)
    task = None
    try:
        if background:
            task = asyncio.create_task(main.job_outbox_worker(db, None, interval=0.01))
            await asyncio.wait_for(two_done.wait(), timeout=5)
        else:
            assert await main._run_due_jobs_once(db, None) == 2
        assert calls[:2] == [ids[0], ids[2] if background else ids[1]]
    finally:
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await db.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("media_ready", [True, False], ids=["ready-projection", "blocked-projection"])
async def test_projection_waits_for_own_media_only(tmp_path, monkeypatch, media_ready):
    db = main.Database(str(tmp_path / "projection-selection.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        events = []
        for title in ("ready publication", "unrelated image"):
            event = main.Event(title=title, description="d", source_text="src", date="2099-01-01", time="12:00", location_name="venue")
            session.add(event)
            await session.flush()
            events.append(int(event.id))
        publication, other = events
        if media_ready:
            session.add(main.JobOutbox(event_id=publication, task=main.JobTask.event_media_review, status=main.JobStatus.done, next_run_at=now))
            await session.flush()
        session.add(main.JobOutbox(event_id=publication, task=main.JobTask.telegraph_build, depends_on=f"event_media_review:{publication}", next_run_at=now - timedelta(seconds=1)))
        await session.flush()
        media_event = other if media_ready else publication
        session.add(main.JobOutbox(event_id=media_event, task=main.JobTask.event_media_review, next_run_at=now - timedelta(seconds=1)))
        await session.commit()
    calls = []

    async def media_handler(eid, *_args):
        calls.append(("media", eid))

    async def publication_handler(eid, *_args):
        calls.append(("publication", eid))

    async def noop(*_args, **_kwargs):
        return None

    monkeypatch.setitem(main.JOB_HANDLERS, "event_media_review", media_handler)
    monkeypatch.setitem(main.JOB_HANDLERS, "telegraph_build", publication_handler)
    monkeypatch.setattr(main, "_watch_nav_jobs", noop)
    monkeypatch.setattr(main, "_log_job_outbox_stats", noop)
    try:
        await main._run_due_jobs_once(db, None, max_jobs=1)
        expected = ("publication", publication) if media_ready else ("media", publication)
        assert calls == [expected]
    finally:
        await db.engine.dispose()


@pytest.mark.asyncio
async def test_initial_publications_wait_for_complete_gallery_across_media_followups(tmp_path, monkeypatch):
    """One CDN seed must not let VK/TG/Telegraph overtake the remaining source photos."""
    from unittest.mock import AsyncMock
    from sqlmodel import select
    import event_media

    db = main.Database(str(tmp_path / 'complete-gallery.sqlite'))
    await db.init()
    # Gallery readiness is independent of Telegram's real overnight quiet hours.
    clock = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return clock
    monkeypatch.setattr(main, 'datetime', Clock)
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    monkeypatch.setenv('DISABLE_ICS_JOBS', '1')
    monkeypatch.setattr(main, 'next_tg_event_publish_run_at', AsyncMock(return_value=clock))
    monkeypatch.setattr(event_media, 'is_event_media_cdn_url', lambda url: True)
    calls = []
    stage = 0
    try:
        async with db.get_session() as session:
            ev = main.Event(title='Source has two photos', date='2099-01-01', time='18:00',
                            location_name='Venue', source_text='Source', description='Event',
                            photo_urls=['https://cdn.example/first.jpg'])
            session.add(ev)
            await session.flush()
            eid = ev.id
            session.add(main.EventPoster(event_id=eid, poster_hash='first',
                                         review_status='approved', media_semantic_status='classified'))
            session.add(main.EventPoster(event_id=eid, poster_hash='second',
                                         review_status='pending_review', media_semantic_status='pending'))
            await event_media.enqueue_event_media_review_job(session, eid, next_run_at=clock)
            await session.commit()

        async def media_handler(event_id, *_args):
            nonlocal stage
            stage += 1
            calls.append(('media', stage))
            async with db.get_session() as session:
                stored = await session.get(main.Event, event_id)
                if stage == 1:
                    await event_media.enqueue_event_media_review_job(
                        session, event_id, next_run_at=clock, force_followup=True)
                    # An earlier dependency check may already have deferred these jobs.
                    for waiting in (await session.execute(select(main.JobOutbox).where(
                        main.JobOutbox.event_id == event_id,
                        main.JobOutbox.task.in_([main.JobTask.vk_sync, main.JobTask.telegraph_build]),
                    ))).scalars():
                        waiting.next_run_at = clock + timedelta(days=1)
                        session.add(waiting)
                else:
                    stored.photo_urls = ['https://cdn.example/first.jpg', 'https://cdn.example/second.jpg']
                    session.add(stored)
                    poster = (await session.execute(select(main.EventPoster).where(
                        main.EventPoster.event_id == event_id,
                        main.EventPoster.poster_hash == 'second',
                    ))).scalar_one()
                    poster.review_status = 'approved'
                    poster.media_semantic_status = 'classified'
                    session.add(poster)
                    # The gallery is complete; optional geometry can wait until tomorrow.
                    await event_media.enqueue_event_media_review_job(
                        session, event_id, next_run_at=clock + timedelta(days=1), force_followup=True)
                await session.commit()
                await session.refresh(stored)
            await main.schedule_event_update_tasks(db, stored, refresh_existing_vk=True)
            return stage == 2

        def publisher(name):
            async def publish(event_id, *_args):
                async with db.get_session() as session:
                    stored = await session.get(main.Event, event_id)
                calls.append((name, len(stored.photo_urls)))
                return True
            return publish

        monkeypatch.setattr(main, 'JOB_HANDLERS', {
            'event_media_review': media_handler,
            'vk_sync': publisher('vk'),
            'telegraph_build': publisher('telegraph'),
            'tg_event_publish': publisher('tg'),
        })
        await main.schedule_event_update_tasks(db, ev)
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 1
        assert calls == [('media', 1)]
        clock += timedelta(seconds=10)
        assert await main._run_due_jobs_once(db, None, max_jobs=1) == 1
        assert calls == [('media', 1), ('media', 2)]
        clock += timedelta(seconds=10)
        await main._run_due_jobs_once(db, None)
        clock += timedelta(seconds=10)
        await main._run_due_jobs_once(db, None)
        assert sorted(calls[2:]) == [('telegraph', 2), ('tg', 2), ('vk', 2)]
        async with db.get_session() as session:
            jobs = list((await session.execute(select(main.JobOutbox).where(
                main.JobOutbox.task == main.JobTask.vk_sync))).scalars())
            assert len(jobs) == 1 and jobs[0].status == main.JobStatus.done
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('linked', [False, True])
async def test_approved_gallery_keeps_existing_linked_semantic_guard(tmp_path, monkeypatch, linked):
    from unittest.mock import AsyncMock
    import event_media
    db = main.Database(str(tmp_path / 'linked-gallery.sqlite'))
    await db.init()
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    monkeypatch.setattr(main, 'DISABLE_PAGE_JOBS', True)
    monkeypatch.setenv('DISABLE_ICS_JOBS', '1')
    monkeypatch.setattr(event_media, 'is_event_media_cdn_url', lambda url: True)
    calls = []
    async def publish(eid, *_args):
        calls.append(eid)
        return True
    monkeypatch.setattr(main, 'JOB_HANDLERS', {
        'vk_sync': publish, 'telegraph_build': publish,
        'event_media_review': AsyncMock(side_effect=AssertionError('Tomorrow enrichment must not run now')),
    })
    try:
        async with db.get_session() as session:
            ev = main.Event(title='Approved source image', date='2099-01-01', time='18:00',
                            location_name='Venue', source_text='Source', description='Event',
                            linked_event_ids=[123] if linked else [], photo_urls=['https://cdn.example/ready.jpg'])
            session.add(ev)
            await session.flush()
            session.add(main.EventPoster(event_id=ev.id, poster_hash='ready', review_status='approved',
                                         media_semantic_status='pending'))
            await event_media.enqueue_event_media_review_job(session, ev.id, next_run_at=now + timedelta(days=1))
            await session.commit()
        await main.schedule_event_update_tasks(db, ev)
        await main._run_due_jobs_once(db, None)
        assert len(calls) == (0 if linked else 2)
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('waits_for_media', [True, False])
async def test_initial_vk_media_dependency_precedes_unrelated_reviews(tmp_path, monkeypatch, waits_for_media):
    db = main.Database(str(tmp_path / 'initial-vk-priority.sqlite'))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        old = main.Event(title='Existing review', description='d', source_text='s', date='2099-01-01', time='12:00', location_name='Venue')
        fresh = main.Event(title='Initial VK announcement', description='d', source_text='s', date='2099-02-01', time='12:00', location_name='Venue')
        session.add_all([old, fresh])
        await session.flush()
        old_id, fresh_id = old.id, fresh.id
        for eid in (old_id, fresh_id):
            session.add(main.JobOutbox(event_id=eid, task=main.JobTask.event_media_review,
                                      next_run_at=now - timedelta(seconds=1)))
        session.add(main.JobOutbox(event_id=fresh_id, task=main.JobTask.vk_sync,
                                  depends_on=f'event_media_review:{fresh_id}' if waits_for_media else None,
                                  next_run_at=now - timedelta(seconds=1)))
        await session.commit()
    calls = []
    async def media(eid, *_args): calls.append(('media', eid))
    async def publish(eid, *_args): calls.append(('vk', eid))
    async def noop(*_args, **_kwargs): pass
    monkeypatch.setitem(main.JOB_HANDLERS, 'event_media_review', media)
    monkeypatch.setitem(main.JOB_HANDLERS, 'vk_sync', publish)
    monkeypatch.setattr(main, '_watch_nav_jobs', noop)
    monkeypatch.setattr(main, '_log_job_outbox_stats', noop)
    try:
        await main._run_due_jobs_once(db, None, max_jobs=1)
        assert calls == [('media' if waits_for_media else 'vk', fresh_id)]
    finally:
        await db.engine.dispose()


@pytest.mark.asyncio
async def test_related_compute_is_not_starved_by_media_stream(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'related-priority.sqlite'))
    await db.init()
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        event = main.Event(title='Event', description='', source_text='', date='2099-01-01', time='19:00', location_name='Venue')
        session.add(event)
        await session.flush()
        session.add(main.JobOutbox(event_id=event.id, task=main.JobTask.vk_sync,
                                  depends_on=f'event_media_review:{event.id}', next_run_at=now - timedelta(seconds=1)))
        for task in (main.JobTask.event_media_review, main.JobTask.event_vector_sync):
            due = now - timedelta(seconds=60 if task == main.JobTask.event_vector_sync else 1)
            session.add(main.JobOutbox(event_id=event.id, task=task, next_run_at=due))
        await session.commit()
    calls = []
    async def run(eid, *_args):
        calls.append('vector')
    async def media(eid, *_args):
        calls.append('media')
    monkeypatch.setitem(main.JOB_HANDLERS, 'event_vector_sync', run)
    monkeypatch.setitem(main.JOB_HANDLERS, 'event_media_review', media)
    try:
        await main._run_due_jobs_once(db, None, max_jobs=1)
        assert calls == ['vector']
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ready_vk_gets_a_turn_before_new_compute_and_repeated_unrelated_media(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / 'related-vk-fair.sqlite'))
    await db.init()
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        waiting = main.Event(title='Waiting for media', description='', source_text='', date='2099-01-01', time='19:00', location_name='Venue')
        ready = main.Event(title='Ready VK carousel', description='', source_text='', date='2099-01-02', time='19:00', location_name='Venue', photo_urls=['https://cdn.example/poster.jpg'])
        session.add_all([waiting, ready])
        await session.flush()
        wid, rid = waiting.id, ready.id
        session.add(main.JobOutbox(event_id=waiting.id, task=main.JobTask.event_media_review, next_run_at=now-timedelta(seconds=30)))
        session.add(main.JobOutbox(event_id=waiting.id, task=main.JobTask.vk_sync, depends_on=f'event_media_review:{wid}', next_run_at=now-timedelta(seconds=30)))
        session.add(main.JobOutbox(event_id=waiting.id, task=main.JobTask.event_vector_sync, next_run_at=now-timedelta(seconds=10)))
        session.add(main.JobOutbox(event_id=ready.id, task=main.JobTask.vk_sync, next_run_at=now-timedelta(seconds=60)))
        await session.commit()
    calls = []
    async def media(eid, *_args): calls.append(('media', eid))
    async def vector(eid, *_args): calls.append(('vector', eid))
    async def publish(eid, *_args): calls.append(('vk', eid))
    monkeypatch.setitem(main.JOB_HANDLERS, 'event_media_review', media)
    monkeypatch.setitem(main.JOB_HANDLERS, 'event_vector_sync', vector)
    monkeypatch.setitem(main.JOB_HANDLERS, 'vk_sync', publish)
    try:
        await main._run_due_jobs_once(db, None, max_jobs=1)
        assert calls == [('vk', rid)]
    finally:
        await db.close()
