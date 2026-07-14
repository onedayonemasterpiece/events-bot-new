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


@pytest.mark.asyncio
async def test_durable_tg_premium_emoji_edit_job_is_enqueued(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("ENABLE_TG_PREMIUM_EMOJI_EDITOR", "1")
    monkeypatch.setenv("TG_PREMIUM_EMOJI_EDIT_DELAY_SECONDS", "0")
    monkeypatch.setenv("TG_PREMIUM_EMOJI_EDIT_JITTER_SECONDS", "0")
    now = datetime.now(timezone.utc)

    async with db.get_session() as session:
        ev = Event(
            title="Premium recovery",
            description="d",
            date="2026-07-10",
            time="12:00",
            location_name="loc",
            source_text="src",
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        event_id = int(ev.id)

    result = await main.enqueue_tg_event_premium_emoji_edit_job(
        db,
        event_id,
        1941,
        next_run_at=now,
    )

    assert result == "new"
    async with db.get_session() as session:
        job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event_id,
                    JobOutbox.task == JobTask.tg_premium_emoji_edit,
                )
            )
        ).scalar_one()
    assert job.status == JobStatus.pending
    assert job.payload == {"message_id": 1941}
    assert job.coalesce_key == f"{JobTask.tg_premium_emoji_edit.value}:{event_id}"


@pytest.mark.asyncio
async def test_due_tg_premium_emoji_edit_jobs_are_spaced(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_PREMIUM_EMOJI_JOB_INTERVAL_SECONDS", "90")
    now = datetime.now(timezone.utc) - timedelta(minutes=5)

    async with db.get_session() as session:
        first = Event(
            title="First premium",
            description="d",
            date="2026-07-10",
            time="12:00",
            location_name="loc",
            source_text="src",
        )
        second = Event(
            title="Second premium",
            description="d",
            date="2026-07-11",
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
                task=JobTask.tg_premium_emoji_edit,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        session.add(
            JobOutbox(
                event_id=second.id,
                task=JobTask.tg_premium_emoji_edit,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        first_id = int(first.id)
        second_id = int(second.id)

    calls: list[int] = []

    async def fake_premium_edit(eid, db_obj, bot_obj):
        calls.append(eid)
        return True

    monkeypatch.setattr(main, "JOB_HANDLERS", {"tg_premium_emoji_edit": fake_premium_edit})

    processed = await main._run_due_jobs_once(db, None)

    assert processed == 1
    assert calls == [first_id]
    async with db.get_session() as session:
        first_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == first_id,
                    JobOutbox.task == JobTask.tg_premium_emoji_edit,
                )
            )
        ).scalar_one()
        second_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == second_id,
                    JobOutbox.task == JobTask.tg_premium_emoji_edit,
                )
            )
        ).scalar_one()

    assert first_job.status == JobStatus.done
    assert second_job.status == JobStatus.pending
    assert main._ensure_utc(second_job.next_run_at) >= (
        main._ensure_utc(first_job.updated_at) + timedelta(seconds=90)
    )


@pytest.mark.asyncio
async def test_tg_event_publish_new_posts_outrank_existing_post_edits(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    now = datetime.now(timezone.utc) - timedelta(minutes=5)

    async with db.get_session() as session:
        existing = Event(
            title="Existing edit",
            description="d",
            date="2026-07-10",
            time="12:00",
            location_name="loc",
            source_text="src",
            added_at=now - timedelta(days=2),
            tg_event_post_id=100,
            tg_event_post_url="https://t.me/c/1/100",
        )
        new_first = Event(
            title="New announcement",
            description="d",
            date="2026-07-11",
            time="13:00",
            location_name="loc",
            source_text="src",
            added_at=now - timedelta(hours=12),
        )
        new_second = Event(
            title="Second new announcement",
            description="d",
            date="2026-07-12",
            time="14:00",
            location_name="loc",
            source_text="src",
            added_at=now - timedelta(hours=13),
        )
        session.add(existing)
        session.add(new_first)
        session.add(new_second)
        await session.commit()
        await session.refresh(existing)
        await session.refresh(new_first)
        await session.refresh(new_second)
        for ev in (existing, new_first, new_second):
            session.add(
                JobOutbox(
                    event_id=ev.id,
                    task=JobTask.tg_event_publish,
                    status=JobStatus.pending,
                    next_run_at=now,
                    updated_at=now,
                )
            )
        await session.commit()
        existing_id = int(existing.id)
        new_first_id = int(new_first.id)
        new_second_id = int(new_second.id)

    calls: list[int] = []

    async def fake_tg_publish(eid, db_obj, bot_obj):
        calls.append(eid)
        async with db_obj.get_session() as session:
            event = await session.get(Event, int(eid))
            if event and not getattr(event, "tg_event_post_id", None):
                event.tg_event_post_id = 1000 + len(calls)
                event.tg_event_post_url = f"https://t.me/c/1/{event.tg_event_post_id}"
                session.add(event)
                await session.commit()
        return True

    monkeypatch.setattr(main, "JOB_HANDLERS", {"tg_event_publish": fake_tg_publish})

    processed = await main._run_due_jobs_once(db, None)

    assert processed == 1
    assert calls == [new_first_id]
    async with db.get_session() as session:
        second_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == new_second_id,
                    JobOutbox.task == JobTask.tg_event_publish,
                )
            )
        ).scalar_one()
        existing_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == existing_id,
                    JobOutbox.task == JobTask.tg_event_publish,
                )
            )
        ).scalar_one()
    assert second_job.status == JobStatus.pending
    assert existing_job.status == JobStatus.pending
    assert main._ensure_utc(second_job.next_run_at) > datetime.now(timezone.utc)
    assert main._ensure_utc(existing_job.next_run_at) > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_confirmed_public_repair_outranks_normal_tg_lanes_without_bypassing_spacing(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    now = datetime.now(timezone.utc) - timedelta(minutes=5)

    async with db.get_session() as session:
        repair = Event(
            title="Confirmed duplicate repair",
            description="d",
            date="2026-07-10",
            time="12:00",
            location_name="loc",
            source_text="src",
            added_at=now - timedelta(days=2),
            tg_event_post_id=100,
            tg_event_post_url="https://t.me/c/1/100",
        )
        fresh = Event(
            title="Fresh announcement",
            description="d",
            date="2026-07-11",
            time="13:00",
            location_name="loc",
            source_text="src",
            added_at=now,
        )
        session.add(repair)
        session.add(fresh)
        await session.commit()
        await session.refresh(repair)
        await session.refresh(fresh)
        session.add(
            JobOutbox(
                event_id=repair.id,
                task=JobTask.tg_event_publish,
                payload={"public_repair_priority": True},
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        session.add(
            JobOutbox(
                event_id=fresh.id,
                task=JobTask.tg_event_publish,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        repair_id = int(repair.id)
        fresh_id = int(fresh.id)

    calls: list[int] = []

    async def fake_tg_publish(eid, db_obj, bot_obj):
        calls.append(eid)
        return True

    monkeypatch.setattr(main, "JOB_HANDLERS", {"tg_event_publish": fake_tg_publish})

    processed = await main._run_due_jobs_once(db, None)

    assert processed == 1
    assert calls == [repair_id]
    async with db.get_session() as session:
        repair_job = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.event_id == repair_id)
            )
        ).scalar_one()
        fresh_job = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.event_id == fresh_id)
            )
        ).scalar_one()
    assert repair_job.status == JobStatus.done
    assert fresh_job.status == JobStatus.pending
    assert main._ensure_utc(fresh_job.next_run_at) >= (
        main._ensure_utc(repair_job.updated_at) + timedelta(minutes=10)
    )


@pytest.mark.asyncio
async def test_fresh_tg_event_publish_is_not_starved_by_old_backlog(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    monkeypatch.setenv("TG_EVENT_PUBLISH_FRESH_QUEUE_HOURS", "12")
    now = datetime.now(timezone.utc) - timedelta(minutes=5)

    async with db.get_session() as session:
        old_event = Event(
            title="Old catch-up",
            description="d",
            date="2026-07-10",
            time="12:00",
            location_name="loc",
            source_text="src",
            added_at=now - timedelta(days=2),
        )
        fresh_event = Event(
            title="Fresh Smart Update",
            description="d",
            date="2026-07-11",
            time="13:00",
            location_name="loc",
            source_text="src",
            added_at=now,
        )
        session.add(old_event)
        session.add(fresh_event)
        await session.commit()
        await session.refresh(old_event)
        await session.refresh(fresh_event)
        session.add(
            JobOutbox(
                event_id=old_event.id,
                task=JobTask.tg_event_publish,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        session.add(
            JobOutbox(
                event_id=fresh_event.id,
                task=JobTask.tg_event_publish,
                status=JobStatus.pending,
                next_run_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        old_id = int(old_event.id)
        fresh_id = int(fresh_event.id)

    calls: list[int] = []

    async def fake_tg_publish(eid, db_obj, bot_obj):
        calls.append(eid)
        return True

    monkeypatch.setattr(main, "JOB_HANDLERS", {"tg_event_publish": fake_tg_publish})

    processed = await main._run_due_jobs_once(db, None)

    assert processed == 1
    assert calls == [fresh_id]
    async with db.get_session() as session:
        old_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == old_id,
                    JobOutbox.task == JobTask.tg_event_publish,
                )
            )
        ).scalar_one()
        fresh_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == fresh_id,
                    JobOutbox.task == JobTask.tg_event_publish,
                )
            )
        ).scalar_one()

    assert fresh_job.status == JobStatus.done
    assert old_job.status == JobStatus.pending
    assert main._ensure_utc(old_job.next_run_at) >= (
        main._ensure_utc(fresh_job.updated_at) + timedelta(minutes=10)
    )
