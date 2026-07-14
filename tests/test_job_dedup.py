import logging
import pytest
import main
from main import Database, Event, JobTask, JobOutbox, JobStatus
from sqlalchemy import select
from datetime import datetime, timedelta, timezone


@pytest.mark.asyncio
async def test_enqueue_job_dedup(tmp_path, caplog):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    caplog.set_level(logging.INFO)
    await main.enqueue_job(db, 1, JobTask.week_pages)
    await main.enqueue_job(db, 1, JobTask.week_pages)
    await main.enqueue_job(db, 1, JobTask.month_pages)
    await main.enqueue_job(db, 1, JobTask.month_pages)
    async with db.get_session() as session:
        res = await session.execute(select(JobOutbox).where(JobOutbox.event_id == 1))
        jobs = res.scalars().all()
    kinds = [j.task for j in jobs]
    assert kinds.count(JobTask.week_pages) == 1
    assert kinds.count(JobTask.month_pages) == 1
    assert any(
        r.message.startswith("ENQ") and "job_key=week_pages:1" in r.message and "merged" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_enqueue_job_requeue_and_skip(tmp_path, caplog):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(
            JobOutbox(
                event_id=1,
                task=JobTask.month_pages,
                status=JobStatus.done,
                attempts=1,
                last_error="err",
                updated_at=datetime.now(timezone.utc) - timedelta(days=1),
                next_run_at=datetime.now(timezone.utc) - timedelta(days=1),
            )
        )
        session.add(
            JobOutbox(
                event_id=1,
                task=JobTask.vk_sync,
                status=JobStatus.done,
            )
        )
        await session.commit()
    caplog.set_level(logging.INFO)
    action1 = await main.enqueue_job(db, 1, JobTask.month_pages)
    action2 = await main.enqueue_job(db, 1, JobTask.vk_sync)
    assert action1 == "requeued"
    assert action2 == "skipped"
    async with db.get_session() as session:
        res = await session.execute(
            select(JobOutbox).where(JobOutbox.event_id == 1)
        )
        jobs = {j.task: j for j in res.scalars().all()}
    assert jobs[JobTask.month_pages].status == JobStatus.pending
    assert jobs[JobTask.month_pages].attempts == 0
    assert jobs[JobTask.month_pages].last_error is None
    assert jobs[JobTask.vk_sync].status == JobStatus.done
    assert any(
        r.message.startswith("ENQ") and "job_key=month_pages:1" in r.message and "requeued" in r.message
        for r in caplog.records
    )
    assert any(
        r.message.startswith("ENQ") and "job_key=vk_sync:1" in r.message and "skipped" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_enqueue_job_requeues_done_vk_sync_without_managed_vk_post(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev = Event(
            title="Telegram event",
            description="d",
            date="2026-06-06",
            time="12:00",
            location_name="loc",
            source_text="src",
            source_post_url="https://t.me/example/1",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=int(ev.id),
                task=JobTask.vk_sync,
                status=JobStatus.done,
            )
        )
        await session.commit()
        event_id = int(ev.id)

    action = await main.enqueue_job(db, event_id, JobTask.vk_sync)

    assert action == "requeued"
    async with db.get_session() as session:
        job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event_id,
                    JobOutbox.task == JobTask.vk_sync,
                )
            )
        ).scalar_one()
    assert job.status == JobStatus.pending
    await db.close()


@pytest.mark.asyncio
async def test_enqueue_job_skips_done_vk_sync_for_existing_postponed_managed_post(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")

    async def fake_vk_api(method, **kwargs):
        assert method == "wall.getById"
        assert kwargs["posts"] == "-231920894_2841"
        return {"response": {"items": []}}

    async def fake_find_postponed(**kwargs):
        assert kwargs["owner_id"] == -231920894
        assert kwargs["post_id"] == 2841
        return {"id": 2841, "attachments": []}

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(
        main,
        "_find_vk_postponed_wall_item_any_actor",
        fake_find_postponed,
    )

    async with db.get_session() as session:
        ev = Event(
            title="Postponed VK event",
            description="d",
            date="2026-06-17",
            time="19:00",
            location_name="loc",
            source_text="src",
            source_vk_post_url="https://vk.com/wall-231920894_2841",
            vk_source_hash="hash",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=int(ev.id),
                task=JobTask.vk_sync,
                status=JobStatus.done,
            )
        )
        await session.commit()
        event_id = int(ev.id)

    action = await main.enqueue_job(db, event_id, JobTask.vk_sync)

    assert action == "skipped"
    async with db.get_session() as session:
        job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event_id,
                    JobOutbox.task == JobTask.vk_sync,
                )
            )
        ).scalar_one()
    assert job.status == JobStatus.done
    await db.close()


@pytest.mark.asyncio
async def test_enqueue_job_requeues_done_vk_sync_for_canonical_event_refresh(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_has_managed_vk_post(event):
        return True

    monkeypatch.setattr(main, "_event_has_existing_managed_vk_post", fake_has_managed_vk_post)

    async with db.get_session() as session:
        ev = Event(
            title="Updated Telegram event",
            description="d",
            date="2026-08-08",
            time="",
            location_name="loc",
            source_text="src",
            source_post_url="https://t.me/example/1",
            source_vk_post_url="https://vk.com/wall-231920894_2841",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            JobOutbox(
                event_id=int(ev.id),
                task=JobTask.vk_sync,
                status=JobStatus.done,
            )
        )
        await session.commit()
        event_id = int(ev.id)

    action = await main.enqueue_job(
        db,
        event_id,
        JobTask.vk_sync,
        requeue_done=True,
    )

    assert action == "requeued"
    async with db.get_session() as session:
        job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event_id,
                    JobOutbox.task == JobTask.vk_sync,
                )
            )
        ).scalar_one()
    assert job.status == JobStatus.pending
    await db.close()


@pytest.mark.asyncio
async def test_schedule_event_refresh_rearms_existing_managed_vk_projection(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls = []

    async def fake_enqueue_job(db_obj, event_id, task, *args, **kwargs):
        calls.append((event_id, task, kwargs))
        return "requeued"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    async with db.get_session() as session:
        ev = Event(
            title="Updated Telegram event",
            description="d",
            date="2026-08-08",
            time="",
            location_name="loc",
            source_text="src",
            source_post_url="https://t.me/example/1",
            source_vk_post_url="https://vk.com/wall-231920894_2841",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        event_id = int(ev.id)

    await main.schedule_event_update_tasks(
        db,
        ev,
        refresh_existing_vk=True,
    )

    vk_calls = [item for item in calls if item[1] == JobTask.vk_sync]
    assert vk_calls == [
        (event_id, JobTask.vk_sync, {"requeue_done": True})
    ]
    await db.close()


@pytest.mark.asyncio
async def test_schedule_event_refresh_orders_stale_calendar_cleanup_before_public_fanout(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls = []

    async def fake_enqueue_job(db_obj, event_id, task, *args, **kwargs):
        calls.append((task, kwargs))
        return "requeued"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    async with db.get_session() as session:
        ev = Event(
            title="Updated Telegram event",
            description="d",
            date="2026-08-08",
            time="",
            location_name="loc",
            source_text="src",
            source_post_url="https://t.me/example/1",
            source_vk_post_url="https://vk.com/wall-231920894_2841",
            ics_url=(
                "https://example.supabase.co/storage/v1/object/public/"
                "events-ics/event-1-2026-08-08.ics"
            ),
            ics_hash="old-hash",
            ics_file_id="old-file",
            ics_post_url="https://t.me/kenigeventscalendar/77",
            ics_post_id=77,
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        event_id = int(ev.id)

    await main.schedule_event_update_tasks(
        db,
        ev,
        refresh_existing_vk=True,
    )

    by_task = {task: kwargs for task, kwargs in calls}
    ics_key = f"ics_publish:{event_id}"
    telegraph_key = f"telegraph_build:{event_id}"
    tg_ics_key = f"tg_ics_post:{event_id}"
    assert JobTask.ics_publish in by_task
    assert by_task[JobTask.telegraph_build]["depends_on"] == [ics_key]
    assert by_task[JobTask.tg_ics_post]["depends_on"] == [telegraph_key, ics_key]
    assert by_task[JobTask.vk_sync]["depends_on"] == [ics_key]
    assert by_task[JobTask.vk_sync]["requeue_done"] is True
    assert by_task[JobTask.tg_event_publish]["depends_on"] == [
        telegraph_key,
        tg_ics_key,
    ]
    await db.close()
