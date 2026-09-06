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
