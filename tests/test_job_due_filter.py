import pytest
from datetime import datetime, timedelta, timezone
from sqlmodel import select

import main
from main import Database, Event, JobOutbox, JobTask, JobStatus


@pytest.mark.asyncio
async def test_future_job_does_not_block_month_pages(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        ev = Event(
            title="t",
            description="d",
            date="2025-09-05",
            time="12:00",
            location_name="loc",
            source_text="src",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.vk_sync,
                status=JobStatus.pending,
                next_run_at=future,
            )
        )
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.month_pages,
                status=JobStatus.pending,
                next_run_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    calls: list[int] = []

    async def fake_month_pages(eid, db_obj, bot_obj):
        calls.append(eid)
        return True

    monkeypatch.setitem(main.JOB_HANDLERS, "month_pages", fake_month_pages)

    await main._run_due_jobs_once(db, None)

    assert calls == [ev.id]

    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox))).scalars().all()
    statuses = {j.task: j.status for j in jobs}
    assert statuses[JobTask.month_pages] == JobStatus.done
    assert statuses[JobTask.vk_sync] == JobStatus.pending


@pytest.mark.asyncio
async def test_vk_sync_is_not_starved_by_unrelated_telegraph_backlog(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)

    async with db.get_session() as session:
        telegraph_event = Event(
            title="Telegraph backlog",
            description="d",
            date="2026-06-06",
            time="12:00",
            location_name="loc",
            source_text="src",
        )
        vk_event = Event(
            title="Ready for VK",
            description="d",
            date="2026-06-07",
            time="13:00",
            location_name="loc",
            source_text="src",
        )
        session.add(telegraph_event)
        session.add(vk_event)
        await session.commit()
        await session.refresh(telegraph_event)
        await session.refresh(vk_event)
        session.add(
            JobOutbox(
                event_id=telegraph_event.id,
                task=JobTask.telegraph_build,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        session.add(
            JobOutbox(
                event_id=vk_event.id,
                task=JobTask.vk_sync,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        vk_event_id = int(vk_event.id)

    calls: list[tuple[str, int]] = []

    async def fake_telegraph(eid, db_obj, bot_obj):
        calls.append(("telegraph_build", eid))
        return True

    async def fake_vk(eid, db_obj, bot_obj):
        calls.append(("vk_sync", eid))
        async with db_obj.get_session() as session:
            ev = await session.get(Event, eid)
            ev.source_vk_post_url = "https://vk.com/wall-231920894_1"
            session.add(ev)
            await session.commit()
        return True

    monkeypatch.setattr(
        main,
        "JOB_HANDLERS",
        {
            "telegraph_build": fake_telegraph,
            "vk_sync": fake_vk,
        },
    )

    await main._run_due_jobs_once(db, None)

    assert calls[0] == ("vk_sync", vk_event_id)
    async with db.get_session() as session:
        vk_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == vk_event_id,
                    JobOutbox.task == JobTask.vk_sync,
                )
            )
        ).scalar_one()
    assert vk_job.status == JobStatus.done


@pytest.mark.asyncio
async def test_vk_sync_is_not_blocked_by_same_event_calendar_backlog(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)

    async with db.get_session() as session:
        ev = Event(
            title="Ready for managed VK",
            description="d",
            date="2026-06-12",
            time="20:30",
            location_name="loc",
            source_text="src",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.tg_ics_post,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.vk_sync,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        event_id = int(ev.id)

    calls: list[tuple[str, int]] = []

    async def fake_vk(eid, db_obj, bot_obj):
        calls.append(("vk_sync", eid))
        return True

    monkeypatch.setattr(main, "JOB_HANDLERS", {"vk_sync": fake_vk})

    await main._run_due_jobs_once(db, None)

    assert calls and calls[0] == ("vk_sync", event_id)
    async with db.get_session() as session:
        vk_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event_id,
                    JobOutbox.task == JobTask.vk_sync,
                )
            )
        ).scalar_one()
    assert vk_job.status == JobStatus.done


@pytest.mark.asyncio
async def test_dependency_wait_does_not_expire_while_dependency_is_retrying(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=2)
    dep_retry_at = now + timedelta(minutes=10)

    async with db.get_session() as session:
        ev = Event(
            title="Calendar retry",
            description="d",
            date="2026-06-12",
            time="20:30",
            location_name="loc",
            source_text="src",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.ics_publish,
                status=JobStatus.error,
                last_error="Server disconnected without sending a response.",
                next_run_at=dep_retry_at,
                updated_at=now,
            )
        )
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.tg_ics_post,
                status=JobStatus.pending,
                depends_on=f"ics_publish:{ev.id}",
                next_run_at=old,
                updated_at=old,
            )
        )
        await session.commit()
        event_id = int(ev.id)

    monkeypatch.setattr(main, "JOB_HANDLERS", {})

    processed = await main._run_due_jobs_once(db, None)

    assert processed == 0
    async with db.get_session() as session:
        tg_ics_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event_id,
                    JobOutbox.task == JobTask.tg_ics_post,
                )
            )
        ).scalar_one()
    assert tg_ics_job.status == JobStatus.pending
    assert tg_ics_job.last_error is None
    assert main._ensure_utc(tg_ics_job.next_run_at) > now
