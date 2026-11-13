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
