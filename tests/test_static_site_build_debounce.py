import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

import main
from main import Database, Event, JobOutbox, JobStatus, JobTask
from static_site_release import (
    StaticSiteBuildClaim,
    StaticSiteSingleFlightDeferred,
    make_request_payload,
)


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


@pytest.mark.asyncio
async def test_static_build_concurrent_enqueues_create_one_pending_row(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    run_at = datetime.now(timezone.utc) + timedelta(minutes=15)
    actions = await asyncio.gather(
        *(
            main.enqueue_job(
                db,
                event_id,
                JobTask.static_site_build,
                payload={"trigger": "smart_update", "event_ids": [event_id]},
                coalesce_key="static_site_build:prod",
                next_run_at=run_at + timedelta(seconds=event_id),
            )
            for event_id in range(1, 17)
        )
    )
    assert actions.count("new") == 1
    async with db.get_session() as session:
        jobs = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.coalesce_key == "static_site_build:prod")
            )
        ).scalars().all()
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.pending
    assert jobs[0].payload["event_ids"] == list(range(1, 17))


@pytest.mark.asyncio
async def test_static_build_noop_gate_performs_zero_kaggle_push(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    payload = make_request_payload(reason="default manual", trigger="operator_request")
    async with db.get_session() as session:
        session.add(
            JobOutbox(
                event_id=0,
                task=JobTask.static_site_build,
                payload=payload,
                status=JobStatus.running,
                coalesce_key="static_site_build:prod",
                updated_at=datetime.now(timezone.utc),
                next_run_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()
    monkeypatch.setenv("ENABLE_STATIC_SITE_KAGGLE_BUILDER", "1")
    monkeypatch.setenv("STATIC_SITE_REPO_SHA", "a" * 40)
    monkeypatch.setenv("STATIC_SITE_SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    monkeypatch.delenv("STATIC_SITE_REQUIRE_VECTOR_BARRIER", raising=False)
    monkeypatch.setattr(
        main,
        "claim_static_site_build",
        lambda *_args, **kwargs: StaticSiteBuildClaim(
            action="noop",
            input_fingerprint=kwargs["input_fingerprint"],
            previous_run_id="run-previous",
        ),
    )

    async def forbidden_push(*_args, **_kwargs):
        raise AssertionError("no-op crossed the Kaggle push boundary")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden_push)
    result = await main.job_static_site_build_kaggle(0, db, None)
    assert result is False
    async with db.get_session() as session:
        row = (
            await session.execute(select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build))
        ).scalar_one()
    assert row.payload["build_receipt"]["status"] == "noop"
    assert row.payload["build_receipt"]["kaggle_push_count"] == 0


@pytest.mark.asyncio
async def test_changed_followup_blocked_by_single_flight_is_retained_then_runs(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    payload = make_request_payload(reason="changed public input", event_ids=[77])
    async with db.get_session() as session:
        session.add(
            JobOutbox(
                event_id=77,
                task=JobTask.static_site_build,
                payload=payload,
                status=JobStatus.pending,
                coalesce_key="static_site_build:prod",
                updated_at=datetime.now(timezone.utc),
                next_run_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()
    calls = 0

    async def fake_handler(_event_id, _db, _bot):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise StaticSiteSingleFlightDeferred("active remote run")
        return False

    monkeypatch.setitem(main.JOB_HANDLERS, "static_site_build", fake_handler)
    assert await main._run_due_jobs_once(
        db, None, allowed_tasks={JobTask.static_site_build}
    ) == 1
    async with db.get_session() as session:
        row = (
            await session.execute(select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build))
        ).scalar_one()
        assert row.status == JobStatus.pending
        assert row.attempts == 0
        assert row.payload == payload
        row.next_run_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.add(row)
        await session.commit()
    assert await main._run_due_jobs_once(
        db, None, allowed_tasks={JobTask.static_site_build}
    ) == 1
    async with db.get_session() as session:
        row = (
            await session.execute(select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build))
        ).scalar_one()
    assert row.status == JobStatus.done
    assert calls == 2


@pytest.mark.asyncio
async def test_startup_calendar_catchup_enqueues_without_smart_update(monkeypatch):
    import scheduling

    calls = []

    async def enqueue(db, **kwargs):
        calls.append((db, kwargs))
        return "new"

    monkeypatch.setenv("ENABLE_STATIC_SITE_KAGGLE_BUILDER", "1")
    monkeypatch.setattr(
        scheduling,
        "get_running_main",
        lambda: type("MainStub", (), {"enqueue_static_site_build_request": staticmethod(enqueue)})(),
    )
    marker = object()
    assert await scheduling._enqueue_static_site_calendar_refresh(
        marker, trigger="startup_catchup"
    ) is True
    assert calls[0][0] is marker
    assert calls[0][1]["trigger"] == "startup_catchup"
    assert calls[0][1]["delay_seconds"] == 0
