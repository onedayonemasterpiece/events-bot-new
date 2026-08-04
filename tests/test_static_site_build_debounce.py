import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text as sql_text

import main
from main import Database, Event, JobOutbox, JobStatus, JobTask
from static_site_release import (
    StaticSiteBuildClaim,
    StaticSiteSingleFlightDeferred,
    make_request_payload,
)


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def test_static_site_repo_sha_prefers_immutable_image_revision(tmp_path, monkeypatch, caplog):
    image_sha = "b" * 40
    revision_file = tmp_path / ".static-site-repo-sha"
    revision_file.write_text(image_sha + "\n", encoding="utf-8")
    monkeypatch.setenv("STATIC_SITE_IMAGE_REPO_SHA_FILE", str(revision_file))
    monkeypatch.setenv("STATIC_SITE_REPO_SHA", "a" * 40)

    assert main._resolve_static_site_repo_sha() == image_sha
    assert "drift ignored" in caplog.text


def test_static_site_repo_sha_rejects_invalid_image_revision(tmp_path, monkeypatch):
    revision_file = tmp_path / ".static-site-repo-sha"
    revision_file.write_text("not-a-revision\n", encoding="utf-8")
    monkeypatch.setenv("STATIC_SITE_IMAGE_REPO_SHA_FILE", str(revision_file))
    monkeypatch.setenv("STATIC_SITE_REPO_SHA", "a" * 40)

    with pytest.raises(main.StaticSitePermanentError, match="image revision"):
        main._resolve_static_site_repo_sha()


def test_static_site_repo_sha_keeps_legacy_fallback_for_local_tests(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "STATIC_SITE_IMAGE_REPO_SHA_FILE", str(tmp_path / "missing-revision")
    )
    monkeypatch.setenv("STATIC_SITE_REPO_SHA", "c" * 40)

    assert main._resolve_static_site_repo_sha() == "c" * 40


async def _seed_failed_static_build(
    db: Database,
    *,
    payload: dict,
    active_claim_matches: bool,
) -> int:
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        job = JobOutbox(
            event_id=0,
            task=JobTask.static_site_build,
            payload=payload,
            status=JobStatus.error,
            coalesce_key="static_site_build:prod",
            updated_at=now,
            next_run_at=now,
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)
        job_id = int(job.id)
    with sqlite3.connect(db.path) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO static_site_build_state(
                release_channel, schema_version, active_job_id, active_run_id,
                active_claim_token, updated_at
            ) VALUES('secret_preview', 'static_site_build_state_v1', ?, ?, ?, ?)
            """,
            (
                job_id if active_claim_matches else job_id + 1,
                "static-site:recoverable-run",
                "recoverable-claim",
                now.isoformat(),
            ),
        )
    return job_id


@pytest.mark.asyncio
async def test_requeue_preserves_remote_handoff_for_exact_active_claim(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    failed_payload = make_request_payload(
        reason="remote push", event_ids=[11], correlation_id="remote-run"
    )
    failed_payload["remote_handoff"] = {
        "run_id": "static-site:recoverable-run",
        "kernel_ref": "owner/static-site-builder",
        "dataset_ref": "owner/static-site-input",
    }
    failed_payload["snapshot"] = {
        "sqlite_path": "/immutable/input.sqlite",
        "manifest_path": "/immutable/input.manifest.json",
    }
    job_id = await _seed_failed_static_build(
        db, payload=failed_payload, active_claim_matches=True
    )
    incoming = make_request_payload(
        reason="startup catchup", event_ids=[12], correlation_id="startup"
    )

    before = datetime.now(timezone.utc)
    action = await main.enqueue_job(
        db,
        12,
        JobTask.static_site_build,
        payload=incoming,
        coalesce_key="static_site_build:prod",
        next_run_at=datetime.now(timezone.utc),
    )

    assert action == "requeued"
    async with db.get_session() as session:
        row = await session.get(JobOutbox, job_id)
    assert row is not None
    assert row.status == JobStatus.pending
    assert row.payload["remote_handoff"] == failed_payload["remote_handoff"]
    assert row.payload["snapshot"] == failed_payload["snapshot"]
    assert row.payload["event_ids"] == [11, 12]
    assert row.payload["reasons"] == ["remote push", "startup catchup"]
    assert _utc(row.next_run_at) >= before
    assert _utc(row.next_run_at) <= datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_smart_update_does_not_debounce_exact_pending_remote_recovery(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    recovery_payload = make_request_payload(
        reason="recover terminal output", event_ids=[31], correlation_id="remote-run"
    )
    recovery_payload["remote_handoff"] = {
        "run_id": "static-site:recoverable-run",
        "kernel_ref": "owner/static-site-builder",
    }
    async with db.get_session() as session:
        row = JobOutbox(
            event_id=31,
            task=JobTask.static_site_build,
            payload=recovery_payload,
            status=JobStatus.pending,
            coalesce_key="static_site_build:prod",
            updated_at=now,
            next_run_at=now + timedelta(minutes=15),
        )
        session.add(row)
        await session.commit()
        await session.refresh(row)
        job_id = int(row.id)
    with sqlite3.connect(db.path) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO static_site_build_state(
                release_channel, schema_version, active_job_id, active_run_id,
                active_claim_token, updated_at
            ) VALUES('secret_preview', 'static_site_build_state_v1', ?, ?, ?, ?)
            """,
            (job_id, "static-site:recoverable-run", "recoverable-claim", now.isoformat()),
        )

    effect_at = now + timedelta(seconds=5)
    action = await main.enqueue_job(
        db,
        32,
        JobTask.static_site_build,
        payload=make_request_payload(
            reason="new Smart Update effect",
            event_ids=[32],
            effect_at=effect_at,
            trigger="smart_update",
        ),
        coalesce_key="static_site_build:prod",
        next_run_at=effect_at + timedelta(minutes=15),
    )

    assert action == "merged-rearmed"
    async with db.get_session() as session:
        recovered = await session.get(JobOutbox, job_id)
    assert recovered is not None
    assert recovered.payload["remote_handoff"] == recovery_payload["remote_handoff"]
    assert recovered.payload["event_ids"] == [31, 32]
    assert _utc(recovered.next_run_at) <= datetime.now(timezone.utc)
    await db.close()


@pytest.mark.asyncio
async def test_requeue_drops_stale_handoff_without_exact_active_claim(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    failed_payload = make_request_payload(
        reason="old failed build", event_ids=[21], correlation_id="old-run"
    )
    failed_payload["remote_handoff"] = {
        "run_id": "static-site:stale-run",
        "kernel_ref": "owner/stale-kernel",
    }
    failed_payload["snapshot"] = {"sqlite_path": "/stale/input.sqlite"}
    job_id = await _seed_failed_static_build(
        db, payload=failed_payload, active_claim_matches=False
    )
    incoming = make_request_payload(
        reason="operator replacement", event_ids=[22], correlation_id="operator"
    )

    action = await main.enqueue_job(
        db,
        22,
        JobTask.static_site_build,
        payload=incoming,
        coalesce_key="static_site_build:prod",
        next_run_at=datetime.now(timezone.utc),
    )

    assert action == "requeued"
    async with db.get_session() as session:
        row = await session.get(JobOutbox, job_id)
    assert row is not None
    assert row.status == JobStatus.pending
    assert "remote_handoff" not in row.payload
    assert "snapshot" not in row.payload
    assert row.payload["event_ids"] == [22]
    assert row.payload["reasons"] == ["operator replacement"]


@pytest.mark.asyncio
async def test_active_error_recovery_runs_before_newer_pending_followup(
    tmp_path, monkeypatch
):
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
        active = JobOutbox(
            event_id=event.id,
            task=JobTask.static_site_build,
            payload=make_request_payload(reason="recover active run"),
            status=JobStatus.error,
            coalesce_key="static_site_build:prod",
            updated_at=now,
            next_run_at=now - timedelta(seconds=1),
        )
        session.add(active)
        await session.commit()
        await session.refresh(active)
        followup = JobOutbox(
            event_id=event.id,
            task=JobTask.static_site_build,
            payload=make_request_payload(reason="new Smart Update effect"),
            status=JobStatus.pending,
            coalesce_key="static_site_build:prod",
            updated_at=now,
            next_run_at=now + timedelta(minutes=10),
        )
        session.add(followup)
        await session.commit()
        active_id = int(active.id)
        followup_id = int(followup.id)
    with sqlite3.connect(db.path) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO static_site_build_state(
                release_channel, schema_version, active_job_id, active_run_id,
                active_claim_token, updated_at
            ) VALUES('secret_preview', 'static_site_build_state_v1', ?, ?, ?, ?)
            """,
            (active_id, "static-site:active-recovery", "claim", now.isoformat()),
        )

    calls: list[int] = []

    async def recover(event_id, _db, _bot):
        calls.append(event_id)
        return False

    monkeypatch.setitem(main.JOB_HANDLERS, "static_site_build", recover)
    processed = await main._run_due_jobs_once(
        db, None, allowed_tasks={JobTask.static_site_build}
    )

    assert processed == 1
    assert calls == [event.id]
    async with db.get_session() as session:
        recovered = await session.get(JobOutbox, active_id)
        pending = await session.get(JobOutbox, followup_id)
    assert recovered is not None and recovered.status == JobStatus.done
    assert recovered.last_error is None
    assert pending is not None and pending.status == JobStatus.pending


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
async def test_static_claim_lost_defers_followup_instead_of_hot_spinning(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "claim-lost.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        owner_event = Event(
            title="Owner",
            description="Description",
            date="2026-08-01",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        followup_event = Event(
            title="Follow-up",
            description="Description",
            date="2026-08-02",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        session.add_all([owner_event, followup_event])
        await session.commit()
        await session.refresh(owner_event)
        await session.refresh(followup_event)
        session.add_all(
            [
                JobOutbox(
                    event_id=owner_event.id,
                    task=JobTask.static_site_build,
                    payload=make_request_payload(reason="active owner"),
                    status=JobStatus.running,
                    coalesce_key="static_site_build:owner",
                    updated_at=now,
                    next_run_at=now,
                ),
                JobOutbox(
                    event_id=followup_event.id,
                    task=JobTask.static_site_build,
                    payload=make_request_payload(reason="new Smart Update"),
                    status=JobStatus.pending,
                    coalesce_key="static_site_build:prod",
                    updated_at=now,
                    next_run_at=now - timedelta(seconds=1),
                ),
            ]
        )
        await session.commit()

    calls = 0

    async def forbidden_handler(*_args):
        nonlocal calls
        calls += 1
        return False

    monkeypatch.setenv("STATIC_SITE_CLAIM_RETRY_SECONDS", "30")
    monkeypatch.setitem(main.JOB_HANDLERS, "static_site_build", forbidden_handler)
    before = datetime.now(timezone.utc)
    assert await main._run_due_jobs_once(
        db, None, allowed_tasks={JobTask.static_site_build}
    ) == 0
    assert await main._run_due_jobs_once(
        db, None, allowed_tasks={JobTask.static_site_build}
    ) == 0
    async with db.get_session() as session:
        followup = (
            await session.execute(
                select(JobOutbox)
                .where(JobOutbox.coalesce_key == "static_site_build:prod")
            )
        ).scalar_one()
    assert followup.status == JobStatus.pending
    assert followup.attempts == 0
    assert followup.last_error == "waiting_for_static_site_owner"
    assert _utc(followup.next_run_at) >= before + timedelta(seconds=29)
    assert calls == 0
    await db.close()


@pytest.mark.asyncio
async def test_static_pre_handoff_orphan_recovers_before_full_runtime_budget(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "pre-handoff-orphan.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        owner_event = Event(
            title="Orphan owner",
            description="Description",
            date="2026-08-01",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        followup_event = Event(
            title="Catch-up",
            description="Description",
            date="2026-08-02",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        session.add_all([owner_event, followup_event])
        await session.commit()
        await session.refresh(owner_event)
        await session.refresh(followup_event)
        owner = JobOutbox(
            event_id=owner_event.id,
            task=JobTask.static_site_build,
            payload=make_request_payload(reason="interrupted before handoff"),
            status=JobStatus.running,
            coalesce_key="static_site_build:prod",
            updated_at=now - timedelta(minutes=11),
            next_run_at=now - timedelta(minutes=11),
        )
        session.add(owner)
        await session.commit()
        await session.refresh(owner)
        owner_id = int(owner.id)
        followup = JobOutbox(
            event_id=followup_event.id,
            task=JobTask.static_site_build,
            payload=make_request_payload(reason="current Smart Update"),
            status=JobStatus.pending,
            coalesce_key="static_site_build:prod",
            updated_at=now,
            next_run_at=now - timedelta(seconds=1),
        )
        session.add(followup)
        await session.commit()

    calls: list[int] = []

    async def handler(event_id, _db, _bot):
        calls.append(event_id)
        return False

    monkeypatch.setenv("STATIC_SITE_PRE_HANDOFF_STALE_SECONDS", "600")
    monkeypatch.setitem(main.JOB_HANDLERS, "static_site_build", handler)
    assert await main._run_due_jobs_once(
        db, None, allowed_tasks={JobTask.static_site_build}
    ) == 1
    async with db.get_session() as session:
        rows = (
            await session.execute(select(JobOutbox).order_by(JobOutbox.id))
        ).scalars().all()
    assert rows[0].id == owner_id
    assert rows[0].status == JobStatus.error
    assert rows[0].last_error == "stale"
    assert rows[1].status == JobStatus.done
    assert calls == [followup_event.id]
    await db.close()


@pytest.mark.asyncio
async def test_terminal_remote_owner_is_rearmed_before_followup_claim(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "terminal-owner.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        owner_event = Event(
            title="Terminal owner",
            description="Description",
            date="2026-08-01",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        followup_event = Event(
            title="Follow-up",
            description="Description",
            date="2026-08-02",
            time="18:00",
            location_name="Venue",
            source_text="source",
        )
        session.add_all([owner_event, followup_event])
        await session.commit()
        await session.refresh(owner_event)
        await session.refresh(followup_event)
        owner = JobOutbox(
            event_id=owner_event.id,
            task=JobTask.static_site_build,
            payload=make_request_payload(reason="remote owner"),
            status=JobStatus.running,
            coalesce_key="static_site_build:prod",
            updated_at=now,
            next_run_at=now,
        )
        session.add(owner)
        await session.commit()
        await session.refresh(owner)
        owner_id = int(owner.id)
        followup = JobOutbox(
            event_id=followup_event.id,
            task=JobTask.static_site_build,
            payload=make_request_payload(reason="new Smart Update"),
            status=JobStatus.pending,
            coalesce_key="static_site_build:prod",
            updated_at=now,
            next_run_at=now - timedelta(seconds=1),
        )
        session.add(followup)
        await session.commit()

    fingerprint = "a" * 64
    claim = main.claim_static_site_build(
        db.path,
        job_id=owner_id,
        run_id="static-site:terminal-owner",
        input_fingerprint=fingerprint,
        effective_date="2026-08-01",
        request_watermark="watermark",
        now=now,
    )
    assert claim.action == "claimed"
    async with db.get_session() as session:
        owner = await session.get(JobOutbox, owner_id)
        owner.payload = {
            **dict(owner.payload or {}),
            "remote_handoff": {
                "build_id": "production-secret-terminal-owner",
                "run_id": "static-site:terminal-owner",
                "repo_sha": "b" * 40,
                "candidate_token": "candidate-token",
                "snapshot_path": str(tmp_path / "snapshot.sqlite"),
                "manifest_path": str(tmp_path / "snapshot.manifest.json"),
                "input_fingerprint": fingerprint,
                "current_datetime": "2026-08-01T02:00:00+02:00",
            },
        }
        await session.execute(
            sql_text(
                """
                INSERT INTO kaggle_run_ledger(
                    run_id, kind, status, token_hash, progress_json,
                    created_at, updated_at, terminal_at
                ) VALUES(
                    'static-site:terminal-owner', 'static_site_builder', 'failed',
                    'token-hash', '{}', :created_at, :updated_at, :terminal_at
                )
                """
            ),
            {
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "terminal_at": now.isoformat(),
            },
        )
        await session.commit()

    calls: list[int] = []

    async def handler(event_id, _db, _bot):
        calls.append(event_id)
        return False

    monkeypatch.setitem(main.JOB_HANDLERS, "static_site_build", handler)
    assert await main._run_due_jobs_once(
        db, None, allowed_tasks={JobTask.static_site_build}
    ) == 2
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build).order_by(JobOutbox.id)
            )
        ).scalars().all()
    assert rows[0].status == JobStatus.done
    assert rows[1].status == JobStatus.done
    assert calls == [owner_event.id, followup_event.id]
    await db.close()


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

@pytest.mark.asyncio
async def test_smart_update_rearms_exactly_latest_effect_plus_fifteen_after_old_immediate(tmp_path):
    db = Database(str(tmp_path / "mixed-trigger.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    operator = make_request_payload(
        reason="operator", event_ids=[1], effect_at=now, trigger="operator_request"
    )
    await main.enqueue_job(
        db,
        1,
        JobTask.static_site_build,
        payload=operator,
        coalesce_key="static_site_build:prod",
        next_run_at=now,
    )
    latest = now + timedelta(seconds=3)
    smart = make_request_payload(
        reason="smart", event_ids=[2], effect_at=latest, trigger="smart_update"
    )
    action = await main.enqueue_job(
        db,
        2,
        JobTask.static_site_build,
        payload=smart,
        coalesce_key="static_site_build:prod",
        next_run_at=latest + timedelta(minutes=15),
    )

    assert action == "merged-rearmed"
    async with db.get_session() as session:
        row = (await session.execute(select(JobOutbox))).scalar_one()
    assert _utc(row.next_run_at) == latest + timedelta(minutes=15)
    # Evidence remains merged, but it no longer drives the scheduling decision.
    assert row.payload["trigger"] == "operator_request"
    assert row.payload["latest_effect_at"] == latest.isoformat().replace("+00:00", "Z")
    await db.close()


@pytest.mark.asyncio
async def test_concurrent_smart_updates_keep_one_row_at_latest_effect_plus_fifteen(tmp_path):
    db = Database(str(tmp_path / "concurrent-trailing.sqlite"))
    await db.init()
    base = datetime.now(timezone.utc)

    async def enqueue(offset: int):
        effect = base + timedelta(seconds=offset)
        return await main.enqueue_job(
            db,
            offset + 1,
            JobTask.static_site_build,
            payload=make_request_payload(
                reason=f"smart-{offset}",
                event_ids=[offset + 1],
                effect_at=effect,
                trigger="smart_update",
            ),
            coalesce_key="static_site_build:prod",
            next_run_at=effect + timedelta(minutes=15),
        )

    await asyncio.gather(*(enqueue(offset) for offset in range(8)))
    async with db.get_session() as session:
        rows = (await session.execute(select(JobOutbox))).scalars().all()
    assert len(rows) == 1
    assert rows[0].status == JobStatus.pending
    assert _utc(rows[0].next_run_at) == base + timedelta(seconds=7, minutes=15)
    assert rows[0].payload["event_ids"] == list(range(1, 9))
    await db.close()


@pytest.mark.asyncio
async def test_immediate_override_pulls_running_followup_forward_then_smart_rearms(tmp_path):
    db = Database(str(tmp_path / "running-mixed.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        session.add(
            JobOutbox(
                event_id=1,
                task=JobTask.static_site_build,
                payload=make_request_payload(reason="owner"),
                status=JobStatus.running,
                coalesce_key="static_site_build:prod",
                updated_at=now,
                next_run_at=now,
            )
        )
        await session.commit()

    effect = now + timedelta(seconds=2)
    smart = make_request_payload(
        reason="smart", event_ids=[2], effect_at=effect, trigger="smart_update"
    )
    await main.enqueue_job(
        db, 2, JobTask.static_site_build, payload=smart,
        coalesce_key="static_site_build:prod",
        next_run_at=effect + timedelta(minutes=15),
    )
    immediate_at = now + timedelta(seconds=4)
    operator = make_request_payload(
        reason="operator", event_ids=[3], effect_at=immediate_at, trigger="operator_request"
    )
    await main.enqueue_job(
        db, 3, JobTask.static_site_build, payload=operator,
        coalesce_key="static_site_build:prod", next_run_at=immediate_at,
    )
    latest = now + timedelta(seconds=6)
    final_smart = make_request_payload(
        reason="smart-latest", event_ids=[4], effect_at=latest, trigger="smart_update"
    )
    await main.enqueue_job(
        db, 4, JobTask.static_site_build, payload=final_smart,
        coalesce_key="static_site_build:prod",
        next_run_at=latest + timedelta(minutes=15),
    )

    async with db.get_session() as session:
        rows = (await session.execute(select(JobOutbox).order_by(JobOutbox.id))).scalars().all()
    assert [row.status for row in rows] == [JobStatus.running, JobStatus.pending]
    assert _utc(rows[1].next_run_at) == latest + timedelta(minutes=15)
    assert rows[1].payload["event_ids"] == [2, 3, 4]
    await db.close()
