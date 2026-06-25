import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, text

import main
from main import Database, Event, JobOutbox, JobTask, JobStatus


@pytest.mark.asyncio
async def test_due_jobs_ignore_unknown_task_values_without_crashing(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev = Event(
            title="a",
            description="d",
            date="2026-06-25",
            time="10:00",
            location_name="x",
            source_text="s",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        now = datetime.now(timezone.utc)
        await session.execute(
            text(
                "INSERT INTO joboutbox "
                "(event_id, task, payload, status, attempts, updated_at, next_run_at) "
                "VALUES (:event_id, 'telegraph_nav_month', NULL, 'pending', 0, :now, :now)"
            ),
            {"event_id": ev.id, "now": now},
        )
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.month_pages,
                status=JobStatus.pending,
                updated_at=now,
                next_run_at=now,
            )
        )
        await session.commit()

    calls: list[int] = []

    async def fake_month_pages(event_id: int, db_obj: Database, bot_obj):
        calls.append(event_id)
        return True

    monkeypatch.setitem(main.JOB_HANDLERS, "month_pages", fake_month_pages)

    processed = await main._run_due_jobs_once(db, bot=None)

    assert processed == 1
    assert calls == [ev.id]
    async with db.get_session() as session:
        invalid = (
            await session.execute(
                text("SELECT status FROM joboutbox WHERE task='telegraph_nav_month'")
            )
        ).scalar_one()
    assert invalid == "pending"


@pytest.mark.asyncio
async def test_running_stale_marked_and_replaced(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev1 = Event(title="a", description="d", date="2025-09-05", time="10:00", location_name="x", source_text="s")
        ev2 = Event(title="b", description="d", date="2025-09-06", time="10:00", location_name="x", source_text="s")
        session.add_all([ev1, ev2])
        await session.commit()
        await session.refresh(ev1)
        await session.refresh(ev2)
        session.add(
            JobOutbox(
                event_id=ev1.id,
                task=JobTask.month_pages,
                status=JobStatus.running,
                coalesce_key="month_pages:2025-09",
                updated_at=datetime.now(timezone.utc) - timedelta(minutes=15),
                next_run_at=datetime.now(timezone.utc) - timedelta(minutes=15),
            )
        )
        await session.commit()
    action = await main.enqueue_job(db, ev2.id, JobTask.month_pages)
    assert action == "new"
    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox))).scalars().all()
        assert len(jobs) == 2
        statuses = {job.status for job in jobs}
        assert JobStatus.error in statuses
        assert JobStatus.pending in statuses
        pend = next(j for j in jobs if j.status == JobStatus.pending)
        assert pend.event_id == ev2.id


@pytest.mark.asyncio
async def test_running_telegraph_build_has_llm_runtime_budget(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev = Event(
            title="Telegraph render",
            description="d",
            date="2026-06-13",
            time="",
            location_name="x",
            source_text="s",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.telegraph_build,
                status=JobStatus.running,
                updated_at=datetime.now(timezone.utc) - timedelta(minutes=4),
                next_run_at=datetime.now(timezone.utc) - timedelta(minutes=4),
            )
        )
        await session.commit()

    await main._run_due_jobs_once(db, bot=None)

    async with db.get_session() as session:
        job = (await session.execute(select(JobOutbox))).scalar_one()

    assert job.status == JobStatus.running
    assert job.last_error is None


@pytest.mark.asyncio
async def test_running_telegraph_build_with_result_is_marked_done(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev = Event(
            title="Telegraph render",
            description="d",
            date="2026-06-13",
            time="",
            location_name="x",
            source_text="s",
            telegraph_url="https://telegra.ph/event-ready",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.telegraph_build,
                status=JobStatus.running,
                updated_at=datetime.now(timezone.utc) - timedelta(minutes=4),
                next_run_at=datetime.now(timezone.utc) - timedelta(minutes=4),
            )
        )
        await session.commit()

    await main._run_due_jobs_once(db, bot=None)

    async with db.get_session() as session:
        job = (await session.execute(select(JobOutbox))).scalar_one()

    assert job.status == JobStatus.done
    assert job.last_error is None
    assert job.last_result == "https://telegra.ph/event-ready"


@pytest.mark.asyncio
async def test_running_vk_sync_stale_retries_instead_of_terminal_dependency_block(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    started_at = datetime.now(timezone.utc) - timedelta(minutes=20)
    async with db.get_session() as session:
        ev = Event(
            title="VK pipeline event",
            description="d",
            date="2026-06-14",
            time="12:00",
            location_name="x",
            source_text="s",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=ev.id,
                task=JobTask.vk_sync,
                status=JobStatus.running,
                updated_at=started_at,
                next_run_at=started_at,
            )
        )
        await session.commit()

    before = datetime.now(timezone.utc)
    await main._run_due_jobs_once(db, bot=None)

    async with db.get_session() as session:
        job = (await session.execute(select(JobOutbox))).scalar_one()

    assert job.status == JobStatus.error
    assert job.last_error == "stale"
    assert job.attempts == 1
    assert before < job.next_run_at < before + timedelta(minutes=2)
