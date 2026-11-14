"""Offline stale-job coverage against the actual canonical SQLite Event read."""
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

import main
from db import Database
from models import Event


@pytest_asyncio.fixture
async def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_SKIP_VK_SOURCES_SEED", "1")
    database = Database(str(tmp_path / "events.sqlite"))
    await database.init()
    yield database
    await database.close()


async def save_event(db, **values):
    event = Event(title="Current event", description="Current description", date="2099-12-20",
                  time="19:00", location_name="Hall", source_text="Original source", **values)
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
    return event


def forbidden_calls(monkeypatch):
    calls = []
    for name in ("_should_skip_ticket_giveaway_publication", "_prepare_same_day_linked_publish_event",
                 "_recover_managed_vk_live_url", "_managed_vk_post_state", "sync_vk_source_post"):
        mock = AsyncMock(side_effect=AssertionError(f"unexpected {name}"))
        monkeypatch.setattr(main, name, mock)
        calls.append(mock)
    token = Mock(side_effect=AssertionError("hidden jobs must not enter token checks"))
    monkeypatch.setattr(main, "_vk_user_token", token)
    monkeypatch.setitem(main.vk_group_blocked, "wall.post", float("inf"))
    return calls + [token]


@pytest.mark.asyncio
@pytest.mark.parametrize("status,silent", [("cancelled", False), ("postponed", False),
    ("active", True), ("unknown", False), ("rescheduled", False), ("merged", False)])
async def test_stale_job_reads_current_hidden_state(db, monkeypatch, status, silent):
    event = await save_event(db)
    # A job was queued while active; execution must not trust that old snapshot.
    async with db.get_session() as session:
        current = await session.get(Event, event.id)
        current.lifecycle_status = status
        current.silent = silent
        session.add(current)
        await session.commit()
    calls = forbidden_calls(monkeypatch)
    assert await main.job_sync_vk_source_post(event.id, db, None) is False
    for call in calls:
        call.assert_not_called()
    async with db.get_session() as session:
        current = await session.get(Event, event.id)
        assert current.source_vk_post_url is None
        assert current.description == "Current description"


@pytest.mark.asyncio
async def test_missing_canonical_row_skips_before_provider_state(db, monkeypatch):
    calls = forbidden_calls(monkeypatch)
    assert await main.job_sync_vk_source_post(987654, db, None) is False
    for call in calls:
        call.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_url", [None, "https://vk.com/wall-1_42"])
async def test_active_current_date_and_managed_repair_still_sync(db, monkeypatch, existing_url):
    event = await save_event(db, source_vk_post_url=existing_url)
    async with db.get_session() as session:
        current = await session.get(Event, event.id)
        current.date = "2099-12-21"  # Rescheduling can retain the canonical active state.
        session.add(current)
        await session.commit()
    monkeypatch.setitem(main.vk_group_blocked, "wall.post", 0.0)
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setattr(main, "_should_skip_ticket_giveaway_publication", AsyncMock(return_value=False))
    async def prepare(database, current):
        return current, [current]
    monkeypatch.setattr(main, "_prepare_same_day_linked_publish_event", prepare)
    monkeypatch.setattr(main, "_recover_managed_vk_live_url", AsyncMock(return_value=None))
    sync = AsyncMock(return_value=None)
    monkeypatch.setattr(main, "sync_vk_source_post", sync)
    persist = AsyncMock(return_value=(None, None))
    monkeypatch.setattr(main, "_persist_vk_source_post_result", persist)
    await main.job_sync_vk_source_post(event.id, db, None)
    sync.assert_awaited_once()
    assert sync.call_args.args[0].date == "2099-12-21"
    assert sync.call_args.args[1] == "Current description"
    assert sync.call_args.kwargs["append_text"] is (not bool(existing_url))
    persist.assert_awaited_once()


@pytest.mark.asyncio
async def test_linked_existing_post_is_remembered_before_edit_failure(db, monkeypatch):
    import vk_event_relations
    from models import JobOutbox, JobTask
    from sqlmodel import select
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setitem(main.vk_group_blocked, 'wall.post', 0.0)
    anchor = await save_event(db, source_vk_post_url='https://vk.com/wall-1_42',
                              vk_source_hash='existing-source-hash')
    member = await save_event(db, linked_event_ids=[anchor.id])
    fresh = await save_event(db)
    monkeypatch.setattr(main, '_should_skip_ticket_giveaway_publication', AsyncMock(return_value=False))
    monkeypatch.setattr(main, '_recover_managed_vk_live_url', AsyncMock(return_value=False))
    monkeypatch.setattr(vk_event_relations, 'presentation', AsyncMock(return_value={'dates': [], 'related': []}))
    sync = AsyncMock(side_effect=RuntimeError('captcha_wait:test'))
    monkeypatch.setattr(main, 'sync_vk_source_post', sync)
    with pytest.raises(RuntimeError, match='captcha_wait'):
        await main.job_sync_vk_source_post(member.id, db, None)
    assert sync.call_args.args[0].id == anchor.id
    async with db.get_session() as session:
        stored = await session.get(Event, member.id)
        assert stored.source_vk_post_url == anchor.source_vk_post_url
        assert stored.vk_source_hash == anchor.vk_source_hash
        jobs = [JobOutbox(event_id=member.id, task=JobTask.vk_sync),
                JobOutbox(event_id=fresh.id, task=JobTask.vk_calendar_comment),
                JobOutbox(event_id=fresh.id, task=JobTask.vk_sync)]
        session.add_all(jobs)
        await session.commit()
    await main.vk_captcha_pause_outbox(db, marker='captcha_wait:linked')
    await main._resume_vk_captcha_outbox(db, 'captcha_wait:linked')
    async with db.get_session() as session:
        rows = (await session.execute(select(JobOutbox).order_by(JobOutbox.next_run_at))).scalars().all()
    assert [(r.event_id, r.task) for r in rows] == [
        (fresh.id, JobTask.vk_calendar_comment), (fresh.id, JobTask.vk_sync), (member.id, JobTask.vk_sync)]
    assert (rows[1].next_run_at - rows[0].next_run_at).total_seconds() == main.VK_CAPTCHA_RESUME_SPACING_SEC


@pytest.mark.asyncio
async def test_initial_image_post_waits_for_shared_relations_before_any_provider_call(db, monkeypatch):
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    event = await save_event(db, photo_urls=['source-poster'])
    calls = forbidden_calls(monkeypatch)
    with pytest.raises(main.StaticSiteSingleFlightDeferred, match='vk_initial_waiting_related'):
        await main.job_sync_vk_source_post(event.id, db, None)
    for call in calls:
        call.assert_not_called()


@pytest.mark.asyncio
async def test_initial_related_wait_keeps_same_outbox_pending_then_completes(db, monkeypatch):
    from datetime import datetime, timezone, timedelta
    from sqlalchemy import select
    event = await save_event(db)
    async with db.get_session() as session:
        job = main.JobOutbox(event_id=event.id, task=main.JobTask.vk_sync,
            next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        session.add(job)
        await session.commit()
        await session.refresh(job)
        jid = job.id
    ready = False
    calls = []
    async def handler(eid, *_args):
        if not ready:
            raise main.StaticSiteSingleFlightDeferred('vk_initial_waiting_related')
        calls.append(eid)
        return True
    monkeypatch.setitem(main.JOB_HANDLERS, 'vk_sync', handler)
    await main._run_due_jobs_once(db, None, max_jobs=1)
    async with db.get_session() as session:
        waiting = await session.get(main.JobOutbox, jid)
        assert waiting.status == main.JobStatus.pending
        assert waiting.last_error == 'vk_initial_waiting_related'
        assert waiting.attempts == 0
        assert waiting.next_run_at.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc) + timedelta(minutes=4)
        waiting.next_run_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        await session.commit()
    ready = True
    await main._run_due_jobs_once(db, None, max_jobs=1)
    async with db.get_session() as session:
        complete = await session.get(main.JobOutbox, jid)
        assert complete.status == main.JobStatus.done
        assert complete.last_error is None
        assert len((await session.execute(select(main.JobOutbox))).scalars().all()) == 1
    assert calls == [event.id]


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [None, "current", "snapshot"])
async def test_graph_changes_do_not_rewrite_initial_complete_post(db, monkeypatch, legacy):
    import json
    import vk_event_relations as relations

    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setitem(main.vk_group_blocked, 'wall.post', 0.0)
    event = await save_event(db, photo_urls=['source-a', 'source-b'])
    relations.graph_path(db).write_text(json.dumps({'related': {str(event.id): {'pure_related': [2]}}}))
    monkeypatch.setattr(main, '_should_skip_ticket_giveaway_publication', AsyncMock(return_value=False))
    async def prepare(database, current):
        return current, [current]
    monkeypatch.setattr(main, '_prepare_same_day_linked_publish_event', prepare)
    monkeypatch.setattr(main, '_recover_managed_vk_live_url', AsyncMock(return_value=None))
    state = AsyncMock(return_value=(True, True))
    monkeypatch.setattr(main, '_managed_vk_post_state', state)
    monkeypatch.setattr(main, '_enqueue_static_site_after_vk_publication', AsyncMock())
    selection = {'dates': [], 'related': [{'id': 2, 'title': 'Original carousel'}]}
    presentation = AsyncMock(return_value=selection)
    comment = AsyncMock(return_value=False)
    monkeypatch.setattr(relations, 'presentation', presentation)
    monkeypatch.setattr(relations, 'ensure_comment', comment)
    sync = AsyncMock(return_value='https://vk.com/wall-1_42')
    monkeypatch.setattr(main, 'sync_vk_source_post', sync)

    real_hash = main.content_hash
    hash_inputs = []
    def capture_hash(value):
        hash_inputs.append(value)
        return real_hash(value)
    monkeypatch.setattr(main, 'content_hash', capture_hash)
    await main.job_sync_vk_source_post(event.id, db, None)
    assert sync.call_args.kwargs['relations'] == selection
    async with db.get_session() as session:
        initial_hash = (await session.get(Event, event.id)).vk_source_hash
    if legacy:
        from models import JobOutbox, JobTask
        async with db.get_session() as session:
            current = await session.get(Event, event.id)
            current.vk_source_hash = real_hash(hash_inputs[0] + "\n" + json.dumps(selection, ensure_ascii=False, sort_keys=True))
            session.add(current)
            if legacy == 'snapshot':
                session.add(JobOutbox(event_id=event.id, task=JobTask.vk_calendar_comment,
                    coalesce_key='vk_related_comment:-1:42', payload={'relations': selection}))
            await session.commit()
    # A new graph may add a second card or change every peer; neither is a source edit.
    relations.graph_path(db).write_text(json.dumps({'related': {str(event.id): {'pure_related': [3, 4]}}}))
    presentation.return_value = selection if legacy == 'current' else {'dates': [{'date': '2099-12-22'}], 'related': [{'id': 3}, {'id': 4}]}
    await main.job_sync_vk_source_post(event.id, db, None)
    assert sync.await_count == 1
    assert comment.await_count == 2
    state.assert_awaited_once()
    assert state.call_args.kwargs == {}
    async with db.get_session() as session:
        current = await session.get(Event, event.id)
        assert current.vk_source_hash == initial_hash
        current.description = 'Corrected source fact'
        session.add(current)
        await session.commit()
    # A real canonical source correction retains the existing update path.
    await main.job_sync_vk_source_post(event.id, db, None)
    assert sync.await_count == 2
    assert sync.call_args.args[1] == 'Corrected source fact'


@pytest.mark.asyncio
@pytest.mark.parametrize('source_url', [None, 'https://vk.com/wall-9_12'])
async def test_captcha_resume_uses_publication_priority_without_shortening_pacing(db, monkeypatch, source_url):
    from models import JobOutbox, JobTask
    from sqlmodel import select
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    existing = await save_event(db, source_vk_post_url='https://vk.com/wall-1_999')
    event = await save_event(db, source_vk_post_url=source_url)
    monkeypatch.setattr(main, 'VK_CAPTCHA_RESUME_SPACING_SEC', 30)
    async with db.get_session() as session:
        session.add(JobOutbox(event_id=existing.id, task=JobTask.vk_sync))
        for task in (JobTask.festival_pages, JobTask.vk_calendar_comment, JobTask.vk_sync):
            session.add(JobOutbox(event_id=event.id, task=task))
        await session.commit()
    await main.vk_captcha_pause_outbox(db, marker='captcha_wait:priority')
    assert await main._resume_vk_captcha_outbox(db, 'captcha_wait:priority') == 4
    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox).order_by(JobOutbox.next_run_at))).scalars().all()
    assert [j.task for j in jobs] == [JobTask.vk_calendar_comment, JobTask.vk_sync, JobTask.vk_sync, JobTask.festival_pages]
    assert [(b.next_run_at - a.next_run_at).total_seconds() for a, b in zip(jobs, jobs[1:])] == [30, 30, 30]
    assert jobs[1].event_id == event.id and jobs[2].event_id == existing.id


@pytest.mark.asyncio
async def test_initial_post_cannot_use_unverified_legacy_graph_when_verifier_enabled(db, monkeypatch):
    import json
    from vk_event_relations import graph_path
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_VERIFY', '1')
    event = await save_event(db, photo_urls=['source-poster'])
    graph_path(db).write_text(json.dumps({'related': {str(event.id): {'pure_related': [2], 'strict_verified': False}}}))
    calls = forbidden_calls(monkeypatch)
    with pytest.raises(main.StaticSiteSingleFlightDeferred, match='vk_initial_waiting_related'):
        await main.job_sync_vk_source_post(event.id, db, None)
    for call in calls:
        call.assert_not_called()


@pytest.mark.asyncio
async def test_verified_empty_anchor_can_continue_initial_publication(db, monkeypatch):
    import json
    from vk_event_relations import graph_path
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_VERIFY', '1')
    event = await save_event(db, photo_urls=['source-poster'])
    graph_path(db).write_text(json.dumps({'related': {str(event.id): {'pure_related': [], 'strict_verified': True}}}))
    monkeypatch.setitem(main.vk_group_blocked, 'wall.post', 0)
    def reached_publication(_event):
        raise AssertionError('verified empty anchor passed the wait')
    monkeypatch.setattr(main, '_event_has_ended_before_today', reached_publication)
    with pytest.raises(AssertionError, match='verified empty anchor passed the wait'):
        await main.job_sync_vk_source_post(event.id, db, None)
