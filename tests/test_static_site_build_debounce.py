from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

import main
from main import Database, Event, JobOutbox, JobStatus, JobTask


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_static_build_running_owner_gets_one_deferred_followup(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        event = Event(
            title="Future event",
            description="Description",
            date="2026-08-01",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)
        session.add(
            JobOutbox(
                event_id=event.id,
                task=JobTask.static_site_build,
                status=JobStatus.running,
                coalesce_key="static_site_build:prod",
                updated_at=now - timedelta(minutes=20),
                next_run_at=now - timedelta(minutes=20),
            )
        )
        await session.commit()

    first_run_at = now + timedelta(minutes=15)
    action = await main.enqueue_job(
        db,
        event.id,
        JobTask.static_site_build,
        payload={"reason": "smart_update", "event_id": event.id},
        coalesce_key="static_site_build:prod",
        next_run_at=first_run_at,
    )
    assert action == "merged"

    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox).order_by(JobOutbox.id))).scalars().all()

    assert [job.status for job in jobs] == [JobStatus.running, JobStatus.pending]
    assert jobs[1].coalesce_key == "static_site_build:prod"
    assert _utc(jobs[1].next_run_at) >= first_run_at


@pytest.mark.asyncio
async def test_static_build_pending_debounce_moves_to_fifteen_minutes_after_latest_update(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            title="Future event",
            description="Description",
            date="2026-08-01",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    first_run_at = datetime.now(timezone.utc) + timedelta(minutes=15)
    await main.enqueue_job(
        db,
        event.id,
        JobTask.static_site_build,
        coalesce_key="static_site_build:prod",
        next_run_at=first_run_at,
    )
    latest_run_at = first_run_at + timedelta(minutes=5)
    action = await main.enqueue_job(
        db,
        event.id,
        JobTask.static_site_build,
        coalesce_key="static_site_build:prod",
        next_run_at=latest_run_at,
    )
    assert action == "merged-rearmed"

    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox))).scalars().all()

    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.pending
    assert _utc(jobs[0].next_run_at) >= latest_run_at
