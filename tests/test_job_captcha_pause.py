import pytest
from sqlmodel import select

import main
from main import Database, Event, JobOutbox, JobTask, JobStatus


@pytest.mark.asyncio
async def test_vk_jobs_paused_and_resumed(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    ev = Event(
        title="t",
        description="d",
        date="2099-01-04",
        time="12:00",
        location_name="loc",
        source_text="src",
    )
    async with db.get_session() as session:
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(JobOutbox(event_id=ev.id, task=JobTask.vk_sync))
        session.add(JobOutbox(event_id=ev.id, task=JobTask.week_pages))
        await session.commit()

    calls: list[str] = []

    async def fake_vk_job(event_id, db, bot):
        calls.append("call")
        if len(calls) == 1:
            raise main.VKAPIError(14, "Captcha needed")
        return True

    monkeypatch.setitem(main.JOB_HANDLERS, "vk_sync", fake_vk_job)
    monkeypatch.setitem(main.JOB_HANDLERS, "week_pages", fake_vk_job)
    monkeypatch.setattr(main, "VK_CAPTCHA_RESUME_SPACING_SEC", 0)

    await main._run_due_jobs_once(db, None)

    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox))).scalars().all()
    assert all(j.status == JobStatus.paused for j in jobs)
    assert all(j.attempts == 0 for j in jobs)
    assert len({j.last_error for j in jobs}) == 1
    assert jobs[0].last_error.startswith("captcha_wait:")
    assert len(calls) == 1

    resume = main._vk_captcha_resume
    assert resume is not None
    await resume()
    main._vk_captcha_resume = None

    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox))).scalars().all()
        assert all(j.status == JobStatus.pending for j in jobs)

    await main._run_due_jobs_once(db, None)

    async with db.get_session() as session:
        jobs = (await session.execute(select(JobOutbox))).scalars().all()
    assert all(j.status == JobStatus.done for j in jobs)
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_vk_captcha_cohort_auto_recovers_after_harmless_probe(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    ev = Event(
        title="future",
        description="d",
        date="2099-01-04",
        time="12:00",
        location_name="loc",
        source_text="src",
    )
    async with db.get_session() as session:
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(JobOutbox(event_id=ev.id, task=JobTask.vk_sync))
        await session.commit()

    marker = "captcha_wait:123"
    monkeypatch.setattr(main, "VK_CAPTCHA_AUTO_RECOVERY_SEC", 0)
    monkeypatch.setattr(main, "VK_CAPTCHA_RESUME_SPACING_SEC", 0)
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "_vk_captcha_auto_probe_last_monotonic", 0.0)
    monkeypatch.setattr(main, "_vk_captcha_needed", True)
    monkeypatch.setattr(main, "_vk_user_token", lambda: "token")
    calls = []

    async def fake_vk_api(method, params, *args, **kwargs):
        calls.append((method, params, kwargs))
        return {"response": {"count": 0, "items": []}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    await main.vk_captcha_pause_outbox(db, marker=marker)

    blocked = await main._maybe_auto_recover_vk_captcha_outbox(db, None)

    assert blocked is False
    assert calls and calls[0][0] == "wall.get"
    assert calls[0][1]["filter"] == "postponed"
    assert calls[0][2]["skip_captcha"] is True
    async with db.get_session() as session:
        job = (await session.execute(select(JobOutbox))).scalars().one()
    assert job.status == JobStatus.pending
    assert main._vk_captcha_needed is False


@pytest.mark.asyncio
async def test_permanent_vk_edit_error_does_not_pause_whole_outbox(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    ev = Event(
        title="future",
        description="d",
        date="2099-01-04",
        time="12:00",
        location_name="loc",
        source_text="src",
    )
    async with db.get_session() as session:
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(JobOutbox(event_id=ev.id, task=JobTask.vk_sync))
        await session.commit()

    async def fake_vk_job(event_id, db, bot):
        raise main.VKAPIError(
            15,
            "Access denied: edit time expired",
            method="wall.edit",
        )

    monkeypatch.setitem(main.JOB_HANDLERS, "vk_sync", fake_vk_job)
    monkeypatch.setattr(main, "_vk_captcha_needed", False)
    monkeypatch.setattr(main, "_vk_captcha_resume", None)

    await main._run_due_jobs_once(db, None)

    async with db.get_session() as session:
        job = (await session.execute(select(JobOutbox))).scalars().one()
    assert job.status == JobStatus.paused
    assert job.attempts == 0
    assert job.last_error.startswith("permanent VK publication error:")
    assert main._vk_captcha_resume is None
