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
async def test_only_unpublished_active_current_event_syncs(db, monkeypatch, existing_url):
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
    if existing_url:
        sync.assert_not_awaited()
        persist.assert_not_awaited()
        return
    sync.assert_awaited_once()
    assert sync.call_args.args[0].date == "2099-12-21"
    assert sync.call_args.args[1] == "Current description"
    assert sync.call_args.kwargs["append_text"] is (not bool(existing_url))
    persist.assert_not_awaited()


@pytest.mark.asyncio
async def test_linked_initial_failure_never_copies_existing_anchor_receipt(db, monkeypatch):
    import vk_event_relations
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setitem(main.vk_group_blocked, 'wall.post', 0.0)
    anchor = await save_event(db, source_vk_post_url='https://vk.com/wall-1_42',
                              vk_source_hash='existing-source-hash')
    member = await save_event(db, linked_event_ids=[anchor.id])
    monkeypatch.setattr(main, '_should_skip_ticket_giveaway_publication', AsyncMock(return_value=False))
    monkeypatch.setattr(vk_event_relations, 'presentation', AsyncMock(return_value={'dates': [], 'related': []}))
    sync = AsyncMock(side_effect=RuntimeError('initial failed'))
    monkeypatch.setattr(main, 'sync_vk_source_post', sync)
    with pytest.raises(RuntimeError, match='initial failed'):
        await main.job_sync_vk_source_post(member.id, db, None)
    assert sync.call_args.args[0].id == member.id
    async with db.get_session() as session:
        stored = await session.get(Event, member.id)
        assert stored.source_vk_post_url is None and stored.vk_source_hash is None
        old = await session.get(Event, anchor.id)
        assert old.source_vk_post_url == anchor.source_vk_post_url
        assert old.vk_source_hash == 'existing-source-hash'


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
@pytest.mark.parametrize('unverified', [False, True])
@pytest.mark.parametrize('date,time,end_date,expected', [
    ('2026-11-28', '13:00', None, 'done'),
    ('2026-11-27', '13:00', '2026-11-27', 'done'),
    ('2026-11-29', '13:00', None, 'pending'),
    ('2026-11-27', '13:00', '2026-11-29', 'pending'),
])
async def test_actual_vk_job_expires_before_related_wait(
    db, monkeypatch, date, time, end_date, expected, unverified
):
    import json
    from datetime import datetime, timezone, timedelta
    from sqlalchemy import select
    from vk_event_relations import graph_path

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 11, 28, 13, 10, tzinfo=main.LOCAL_TZ)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    monkeypatch.setattr(main, 'datetime', Clock)
    monkeypatch.setattr(main, 'LOCAL_TZ', timezone(timedelta(hours=2)))
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_VERIFY', '1')
    event = await save_event(db, photo_urls=['source-poster'])
    async with db.get_session() as session:
        stored = await session.get(Event, event.id)
        stored.date, stored.time, stored.end_date = date, time, end_date
        stored.end_date_is_inferred = False
        session.add(stored)
        job = main.JobOutbox(event_id=event.id, task=main.JobTask.vk_sync,
            next_run_at=Clock.now(timezone.utc) - timedelta(seconds=1))
        session.add(job)
        await session.commit()
        await session.refresh(job)
        jid = job.id
    if unverified:
        path = graph_path(db)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'related': {str(event.id): {'strict_verified': False}}}))
    calls = forbidden_calls(monkeypatch)
    monkeypatch.setitem(main.JOB_HANDLERS, 'vk_sync', main.job_sync_vk_source_post)
    await main._run_due_jobs_once(db, None, max_jobs=1)
    async with db.get_session() as session:
        result = await session.get(main.JobOutbox, jid)
        assert result.status.value == expected
        assert result.attempts == 0
        assert result.last_error == ('vk_initial_waiting_related' if expected == 'pending' else None)
        assert len((await session.execute(select(main.JobOutbox))).scalars().all()) == 1
        stored = await session.get(Event, event.id)
        assert stored.source_vk_post_url is None and stored.vk_source_hash is None
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
async def test_existing_post_does_not_rewrite_or_adopt_hash_for_graph_or_source_changes(db, monkeypatch, legacy):
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
    async with db.get_session() as session:
        applied_hash = (await session.get(Event, event.id)).vk_source_hash
    await main.job_sync_vk_source_post(event.id, db, None)
    assert sync.await_count == 1
    assert comment.await_count == 1
    state.assert_not_awaited()
    async with db.get_session() as session:
        current = await session.get(Event, event.id)
        assert current.vk_source_hash == applied_hash
        current.description = 'Corrected source fact'
        session.add(current)
        await session.commit()
    # Canonical facts may change without automatically rewriting a created announcement.
    await main.job_sync_vk_source_post(event.id, db, None)
    assert sync.await_count == 1
    async with db.get_session() as session:
        assert (await session.get(Event, event.id)).vk_source_hash == applied_hash


@pytest.mark.asyncio
@pytest.mark.parametrize('source_url', [None, 'https://vk.com/wall-9_12'])
@pytest.mark.parametrize('calendar_payload', [{}, {'relations': {}}])
async def test_captcha_resume_uses_publication_priority_without_shortening_pacing(db, monkeypatch, source_url, calendar_payload):
    from models import JobOutbox, JobTask
    from sqlmodel import select
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    existing = await save_event(db, source_vk_post_url='https://vk.com/wall-1_999')
    event = await save_event(db, source_vk_post_url=source_url)
    monkeypatch.setattr(main, 'VK_CAPTCHA_RESUME_SPACING_SEC', 30)
    async with db.get_session() as session:
        session.add(JobOutbox(event_id=existing.id, task=JobTask.vk_sync))
        for task in (JobTask.festival_pages, JobTask.vk_sync):
            session.add(JobOutbox(event_id=event.id, task=task))
        session.add(JobOutbox(event_id=event.id, task=JobTask.vk_calendar_comment, payload=calendar_payload))
        session.add(JobOutbox(event_id=event.id, task=JobTask.vk_calendar_comment,
                             payload={"relations": {"related": [{"id": 42}]}}))
        await session.commit()
    await main.vk_captcha_pause_outbox(db, marker='captcha_wait:priority')
    assert await main._resume_vk_captcha_outbox(db, 'captcha_wait:priority') == 5
    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox).order_by(JobOutbox.next_run_at))).scalars().all()
    assert [j.task for j in jobs] == [JobTask.vk_calendar_comment, JobTask.vk_sync, JobTask.vk_sync,
                                    JobTask.festival_pages, JobTask.vk_calendar_comment]
    assert jobs[0].payload["relations"] and not jobs[-1].payload.get("relations")
    assert [(b.next_run_at - a.next_run_at).total_seconds() for a, b in zip(jobs, jobs[1:])] == [30] * 4
    assert jobs[1].event_id == event.id and jobs[2].event_id == existing.id
    # A slow preceding handler can make all resumed slots overdue.
    from datetime import timedelta
    async with db.get_session() as session:
        for job in jobs:
            job.next_run_at -= timedelta(minutes=3)
            session.add(job)
        await session.commit()
    calendar = AsyncMock(return_value=False)
    sync = AsyncMock(return_value=False)
    monkeypatch.setitem(main.JOB_HANDLERS, "vk_calendar_comment", calendar)
    monkeypatch.setitem(main.JOB_HANDLERS, "vk_sync", sync)
    monkeypatch.setattr(main, "_maybe_auto_recover_vk_captcha_outbox", AsyncMock(return_value=False))
    await main._run_due_jobs_once(db, None, max_jobs=1)
    assert calendar.await_args.args[3].get("relations")
    await main._run_due_jobs_once(db, None, max_jobs=1)
    assert sync.await_args.args[0] == event.id
    # A new ordinary calendar keeps its pre-existing foreground priority.
    async with db.get_session() as session:
        session.add(JobOutbox(event_id=event.id, task=JobTask.vk_calendar_comment,
                              payload={"ordinary": True}))
        await session.commit()
    await main._run_due_jobs_once(db, None, max_jobs=1)
    assert calendar.await_args.args[3] == {"ordinary": True}


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
    async def reached_publication(_db, _event):
        raise AssertionError('verified empty anchor passed the wait')
    monkeypatch.setattr(main, '_should_skip_ticket_giveaway_publication', reached_publication)
    with pytest.raises(AssertionError, match='verified empty anchor passed the wait'):
        await main.job_sync_vk_source_post(event.id, db, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["source", "gallery", "legacy"])
async def test_existing_managed_automatic_job_makes_zero_vk_calls(db, monkeypatch, change):
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    event = await save_event(db, source_vk_post_url="https://vk.com/wall-1_42",
                             vk_source_hash=None if change == "legacy" else "previous-success",
                             photo_urls=["https://cdn.example/a.jpg", "https://cdn.example/b.jpg"])
    async with db.get_session() as session:
        row = await session.get(Event, event.id)
        if change == "source":
            row.description = "A genuinely changed canonical source fact"
        elif change == "gallery":
            row.photo_urls.append("https://cdn.example/c.jpg")
        session.add(row)
        session.add(main.JobOutbox(event_id=event.id, task=main.JobTask.vk_sync))
        await session.commit()
    calls = forbidden_calls(monkeypatch)
    api = AsyncMock(side_effect=AssertionError("existing announcements must not call VK"))
    monkeypatch.setattr(main, "_vk_api", api)
    monkeypatch.setattr(main, "vk_api", api)
    assert await main._run_due_jobs_once(db, None, only_event=event.id,
                                        allowed_tasks={main.JobTask.vk_sync}) == 1
    for call in calls + [api]:
        call.assert_not_called()
    async with db.get_session() as session:
        row = await session.get(Event, event.id)
        assert row.source_vk_post_url == "https://vk.com/wall-1_42"
        assert row.vk_source_hash == event.vk_source_hash


@pytest.mark.asyncio
@pytest.mark.parametrize("managed_peer", [None, "18:00", "20:00"])
async def test_linked_initial_groups_only_when_every_member_is_unpublished(db, monkeypatch, managed_peer):
    import vk_event_relations
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setitem(main.vk_group_blocked, "wall.post", 0.0)
    peer = await save_event(db, source_vk_post_url="https://vk.com/wall-1_100" if managed_peer else None,
                           vk_source_hash="old-success" if managed_peer else None)
    member = await save_event(db, linked_event_ids=[peer.id], photo_urls=["source-a", "source-b"])
    async with db.get_session() as session:
        old = await session.get(Event, peer.id)
        old.time = managed_peer or "18:00"
        old.linked_event_ids = [member.id]
        session.add(old)
        await session.commit()
    selection = {"dates": [], "related": []}
    monkeypatch.setattr(vk_event_relations, "presentation", AsyncMock(return_value=selection))
    monkeypatch.setattr(vk_event_relations, "ensure_comment", AsyncMock())
    monkeypatch.setattr(main, "_enqueue_static_site_after_vk_publication", AsyncMock())
    sync = AsyncMock(return_value="https://vk.com/wall-1_200")
    monkeypatch.setattr(main, "sync_vk_source_post", sync)
    await main.job_sync_vk_source_post(member.id, db, None)
    published = sync.call_args.args[0]
    assert published.id == (member.id if managed_peer else peer.id)
    assert published.time == ("19:00" if managed_peer else "18:00 и 19:00")
    async with db.get_session() as session:
        current = await session.get(Event, member.id)
        old = await session.get(Event, peer.id)
        assert current.source_vk_post_url == "https://vk.com/wall-1_200"
        assert old.source_vk_post_url == ("https://vk.com/wall-1_100" if managed_peer else current.source_vk_post_url)
        if managed_peer:
            assert old.vk_source_hash == "old-success"


@pytest.mark.asyncio
async def test_initial_two_sources_and_both_cards_schedule_delayed_calendar(db, monkeypatch):
    import vk_event_relations
    from datetime import datetime, timedelta, timezone
    monkeypatch.setenv("STATIC_SITE_BUILD_PROFILE", "default")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "fixture")
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setitem(main.vk_group_blocked, "wall.post", 0.0)
    monkeypatch.setattr(main, "_vk_user_token", lambda: "fixture")
    monkeypatch.setattr(main, "choose_vk_actor", lambda *_: [main.VkActor("user", "fixture", "user")])
    photos = ["https://cdn.example/source-a.jpg", "https://cdn.example/source-b.jpg"]
    event = await save_event(db, photo_urls=photos)
    slot = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
    monkeypatch.setattr(main, "_reserve_vk_postponed_publish_date", AsyncMock(return_value=slot))
    monkeypatch.setattr(main, "_resolve_vk_postponed_wall_id_any_actor", AsyncMock(return_value=42))
    selection = {"dates": [{"id": 2, "date": "21.12", "time": "20:00", "url": "https://vk.com/wall-1_2"}],
                 "related": [{"id": 3, "date": "22.12", "time": "19:00", "title": "Another event",
                              "place": "Hall", "url": "https://vk.com/wall-1_3"}]}
    monkeypatch.setattr(vk_event_relations, "presentation", AsyncMock(return_value=selection))
    monkeypatch.setattr(vk_event_relations, "ensure_comment", AsyncMock())
    card_bytes = [b"other dates card", b"similar events card"]
    monkeypatch.setattr(vk_event_relations, "cards", AsyncMock(return_value=card_bytes))
    gallery = AsyncMock(return_value=(["photo-1_1", "photo-1_2", "photo-1_3", "photo-1_4"], "grid"))
    monkeypatch.setattr(main, "_upload_vk_event_gallery", gallery)
    calls = []
    async def api(method, params, *args, **kwargs):
        calls.append(method)
        assert method == "wall.post"
        assert params["attachments"] == "photo-1_1,photo-1_2,photo-1_3,photo-1_4"
        assert params["publish_date"] == slot
        return {"response": {"post_id": 42}}
    monkeypatch.setattr(main, "_vk_api", api)
    monkeypatch.setattr(main, "_enqueue_static_site_after_vk_publication", AsyncMock())
    await main.job_sync_vk_source_post(event.id, db, None)
    assert calls == ["wall.post"]
    main._reserve_vk_postponed_publish_date.assert_awaited_once()
    assert gallery.await_args.args[1] == photos
    assert gallery.await_args.kwargs["relation_cards"] == card_bytes
    async with db.get_session() as session:
        current = await session.get(Event, event.id)
        assert current.source_vk_post_url == "https://vk.com/wall-1_42" and current.vk_source_hash
        jobs = list((await session.execute(main.select(main.JobOutbox).where(
            main.JobOutbox.task == main.JobTask.vk_calendar_comment))).scalars())
        assert len(jobs) == 1 and jobs[0].payload["publish_date"] == slot
        assert 300 <= jobs[0].next_run_at.replace(tzinfo=timezone.utc).timestamp() - slot <= 900


@pytest.mark.asyncio
async def test_normal_scheduler_does_not_probe_or_requeue_managed_post(db, monkeypatch):
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    event = await save_event(db, source_vk_post_url="https://vk.com/wall-1_42",
                             photo_urls=["https://cdn.example/new.jpg"])
    probe = AsyncMock(side_effect=AssertionError("automatic scheduling must not probe existing media"))
    monkeypatch.setattr(main, "_event_has_existing_managed_vk_post", probe)
    enqueue = AsyncMock(return_value="queued")
    monkeypatch.setattr(main, "enqueue_job", enqueue)
    await main.schedule_event_update_tasks(db, event)
    probe.assert_not_awaited()
    assert all(call.args[2] != main.JobTask.vk_sync for call in enqueue.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['frozen', 'document', 'sync', 'missing', 'transport', 'value_error'])
async def test_frozen_ambiguity_finishes_without_retrying_other_failures(db, monkeypatch, case):
    from datetime import datetime, timedelta, timezone

    clock = [datetime.now(timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz) if tz else clock[0].replace(tzinfo=None)

    monkeypatch.setattr(main, 'datetime', Clock)
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    event = await save_event(db, vk_source_hash='known', source_vk_post_url='https://vk.com/wall-231920894_10')
    task = main.JobTask.vk_sync if case == 'sync' else main.JobTask.vk_calendar_comment
    payload = {} if case == 'document' else {'relations': {'related': [{'id': 9}]}}
    reason = {'missing': 'vk_relations_post_missing', 'transport': 'offline_transport'}.get(case, 'vk_live_id_ambiguous')
    exception = ValueError(reason) if case == 'value_error' else RuntimeError(reason)
    calls, healthy = [], []

    async def failed(event_id, *args):
        calls.append(event_id)
        raise exception

    async def succeeded(event_id, *args):
        healthy.append(event_id)
        return True

    async def forbidden_api(*args, **kwargs):
        raise AssertionError('External VK API must not run in this regression')

    monkeypatch.setitem(main.JOB_HANDLERS, task.value, failed)
    monkeypatch.setitem(main.JOB_HANDLERS, 'telegraph_build', succeeded)
    monkeypatch.setattr(main, '_vk_api', forbidden_api)
    async with db.get_session() as session:
        row = main.JobOutbox(event_id=event.id, task=task, payload=payload, attempts=2,
                             next_run_at=clock[0] - timedelta(seconds=1))
        session.add(row)
        session.add(main.JobOutbox(event_id=event.id, task=main.JobTask.telegraph_build,
                                   next_run_at=clock[0] - timedelta(seconds=1)))
        await session.flush()
        job_id = row.id
        await session.commit()
    assert await main._run_due_jobs_once(db, None) == 2
    async with db.get_session() as session:
        row = await session.get(main.JobOutbox, job_id)
        terminal = case == 'frozen'
        assert row.status == main.JobStatus.error
        assert row.terminal_reason == ('failed' if terminal else None)
        assert row.attempts == (2 if terminal else 3)
        assert row.payload == payload and row.last_error == reason
        clock[0] = main._ensure_utc(row.next_run_at) + timedelta(seconds=1)
    assert await main._run_due_jobs_once(db, None) == (0 if terminal else 1)
    assert len(calls) == (1 if terminal else 2)
    assert healthy == [event.id] and main._vk_captcha_needed is False


@pytest.mark.asyncio
async def test_initial_relations_use_reserved_publication_time(db, monkeypatch):
    import json
    from datetime import datetime as RealDatetime, timezone, timedelta
    import vk_event_relations
    class Clock(RealDatetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 11, 27, 4, tzinfo=timezone.utc)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    monkeypatch.setattr(main, 'datetime', Clock)
    monkeypatch.setattr(main, 'LOCAL_TZ', timezone(timedelta(hours=2)))
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'default')
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '1')
    monkeypatch.setattr(main, 'choose_vk_actor', lambda *_: [])
    slot = int(RealDatetime(2026, 11, 28, 18, 40, tzinfo=timezone.utc).timestamp())
    reserve = AsyncMock(return_value=slot)
    monkeypatch.setattr(main, '_reserve_vk_postponed_publish_date', reserve)
    monkeypatch.setattr(main, '_event_primary_publish_source_url', AsyncMock(return_value='https://t.me/source/1'))
    monkeypatch.setattr(main, '_should_skip_ticket_giveaway_publication', AsyncMock(return_value=False))
    monkeypatch.setattr('tg_graphic_medallions.resolve_event_graphic_medallions', lambda *_args, **_kwargs: [])
    async def visible(method, params, *args, **kwargs):
        return {'response': [{'owner_id': -1, 'id': int(value.split('_')[1]), 'post_type': 'post'}
                             for value in params['posts'].split(',')]}
    monkeypatch.setattr(main, '_vk_api', visible)
    anchor = await save_event(db, photo_urls=['source'])
    async with db.get_session() as session:
        peers = []
        for date in ['2026-11-28', '2026-11-29']:
            peer = Event(title='Peer', description='', source_text='', date=date, time='19:00',
                         location_name='Hall', source_vk_post_url=f'https://vk.com/wall-1_{len(peers)+20}')
            session.add(peer)
            await session.flush()
            peers.append(peer.id)
        await session.commit()
    vk_event_relations.graph_path(db).write_text(json.dumps({'related': {str(anchor.id): {'pure_related': peers}}}))
    publish = AsyncMock(return_value=None)
    monkeypatch.setattr(main, 'sync_vk_source_post', publish)
    await main.job_sync_vk_source_post(anchor.id, db, None)
    selected = publish.await_args.kwargs['relations']['related']
    assert [row['id'] for row in selected] == [peers[1]]
    reserve.assert_awaited_once()
    assert publish.await_args.kwargs['publish_date'] == slot


@pytest.mark.asyncio
async def test_missing_source_image_does_not_reserve_before_media(db, monkeypatch):
    import vk_event_relations
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'default')
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    reserve = AsyncMock()
    monkeypatch.setattr(main, '_reserve_vk_postponed_publish_date', reserve)
    monkeypatch.setattr(main, '_should_skip_ticket_giveaway_publication', AsyncMock(return_value=False))
    monkeypatch.setattr(vk_event_relations, 'presentation', AsyncMock(return_value={'dates': [], 'related': []}))
    publish = AsyncMock(side_effect=RuntimeError('vk_sync_missing_media'))
    monkeypatch.setattr(main, 'sync_vk_source_post', publish)
    anchor = await save_event(db, photo_urls=[])
    with pytest.raises(RuntimeError, match='vk_sync_missing_media'):
        await main.job_sync_vk_source_post(anchor.id, db, None)
    reserve.assert_not_awaited()
    assert publish.await_args.kwargs['publish_date'] is None
