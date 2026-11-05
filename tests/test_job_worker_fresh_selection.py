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
    monkeypatch.setattr(main, "_reconcile_recent_managed_vk_live_urls", noop)
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
