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
async def test_due_tg_event_publish_backlog_is_spaced_at_execution(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    now = datetime.now(timezone.utc) - timedelta(minutes=5)

    async with db.get_session() as session:
        first = Event(
            title="First",
            description="d",
            date="2026-06-10",
            time="12:00",
            location_name="loc",
            source_text="src",
        )
        second = Event(
            title="Second",
            description="d",
            date="2026-06-10",
            time="13:00",
            location_name="loc",
            source_text="src",
        )
        session.add(first)
        session.add(second)
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
        session.add(
            JobOutbox(
                event_id=first.id,
                task=JobTask.tg_event_publish,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        session.add(
            JobOutbox(
                event_id=second.id,
                task=JobTask.tg_event_publish,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        first_id = int(first.id)
        second_id = int(second.id)

    calls: list[int] = []

    async def fake_tg_publish(eid, db_obj, bot_obj):
        calls.append(eid)
        return True

    monkeypatch.setattr(main, "JOB_HANDLERS", {"tg_event_publish": fake_tg_publish})

    processed = await main._run_due_jobs_once(db, None)

    assert processed == 1
    assert calls == [first_id]
    async with db.get_session() as session:
        first_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == first_id,
                    JobOutbox.task == JobTask.tg_event_publish,
                )
            )
        ).scalar_one()
        second_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == second_id,
                    JobOutbox.task == JobTask.tg_event_publish,
                )
            )
        ).scalar_one()

    assert first_job.status == JobStatus.done
    assert second_job.status == JobStatus.pending
    assert main._ensure_utc(second_job.next_run_at) >= (
        main._ensure_utc(first_job.updated_at) + timedelta(minutes=10)
    )
