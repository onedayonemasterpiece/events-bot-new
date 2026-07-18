from __future__ import annotations

import json
import hashlib
import io
import sqlite3
import tarfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from static_site_release import (
    MAX_CORRELATIONS,
    MAX_EVENT_IDS,
    StaticSitePermanentError,
    StaticSiteRetryableError,
    active_static_site_remote_run,
    classify_failure,
    claim_static_site_build,
    compute_static_site_input_fingerprint,
    create_immutable_snapshot,
    finish_static_site_build_claim,
    freshness_state,
    make_request_payload,
    merge_request_payload,
    publish_secret_candidate_archive,
    resolve_build_clock,
    validate_snapshot,
    validate_vector_barrier,
)


def test_build_clock_uses_kaliningrad_boundary_and_rejects_mismatch() -> None:
    before = resolve_build_clock(now=datetime(2026, 7, 18, 21, 59, tzinfo=timezone.utc))
    after = resolve_build_clock(now=datetime(2026, 7, 18, 22, 0, tzinfo=timezone.utc))
    assert before.time_zone == after.time_zone == "Europe/Kaliningrad"
    assert before.effective_date == "2026-07-18"
    assert after.effective_date == "2026-07-19"
    assert after.current_datetime.startswith("2026-07-19T00:00:00+02:00")
    normalized = resolve_build_clock(current_date="2026-07-18")
    assert normalized.current_datetime == "2026-07-18T00:00:00+02:00"
    with pytest.raises(StaticSitePermanentError, match="date_mismatch"):
        resolve_build_clock(
            current_date="2026-07-18",
            current_datetime="2026-07-19T00:00:00+02:00",
        )


def _fingerprint_db(path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE event(id INTEGER PRIMARY KEY, title TEXT, date TEXT, description TEXT)"
        )
        connection.execute(
            "INSERT INTO event VALUES(1, 'Public title', '2026-08-01', 'Public description')"
        )
        connection.execute(
            "CREATE TABLE joboutbox(id INTEGER PRIMARY KEY, attempts INTEGER, updated_at TEXT)"
        )
        connection.execute("INSERT INTO joboutbox VALUES(1, 0, '2026-07-18T00:00:00Z')")


def test_fingerprint_ignores_operational_churn_but_changes_for_public_date_and_config(tmp_path) -> None:
    database = tmp_path / "fingerprint.sqlite"
    _fingerprint_db(database)
    base, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full", "policy": "v1"},
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE joboutbox SET attempts=99, updated_at='2026-07-18T12:00:00Z' WHERE id=1"
        )
        connection.execute(
            "INSERT INTO event VALUES(2, 'Past-only churn', '2026-07-01', 'Already absent from output')"
        )
    unchanged, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full", "policy": "v1"},
    )
    assert unchanged == base
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE event SET title='Changed public title' WHERE id=1")
    public_changed, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full", "policy": "v1"},
    )
    rollover, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-19",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full", "policy": "v1"},
    )
    config_changed, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full", "policy": "v2"},
    )
    publish_disabled, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={
            "catalog_mode": "full",
            "policy": "v1",
            "secret_publish_enabled": False,
        },
    )
    publish_enabled, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={
            "catalog_mode": "full",
            "policy": "v1",
            "secret_publish_enabled": True,
        },
    )
    assert public_changed != base
    assert rollover != public_changed
    assert config_changed != public_changed
    assert publish_enabled != publish_disabled


def test_durable_noop_force_and_concurrent_single_flight(tmp_path) -> None:
    database = tmp_path / "claims.sqlite"
    _fingerprint_db(database)
    fingerprint, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full"},
    )

    def attempt(number: int):
        return claim_static_site_build(
            database,
            job_id=number,
            run_id=f"run-{number}",
            input_fingerprint=fingerprint,
            effective_date="2026-07-18",
            request_watermark=f"watermark-{number}",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(attempt, (1, 2)))
    assert sorted(claim.action for claim in claims) == ["busy", "claimed"]
    owner = next(claim for claim in claims if claim.action == "claimed")
    finish_static_site_build_claim(
        database,
        claim_token=owner.claim_token or "",
        run_id="run-1" if claims[0] is owner else "run-2",
        input_fingerprint=fingerprint,
        effective_date="2026-07-18",
        success=True,
        receipt={"kaggle_push_count": 1},
    )
    noop = claim_static_site_build(
        database,
        job_id=3,
        run_id="run-3",
        input_fingerprint=fingerprint,
        effective_date="2026-07-18",
        request_watermark="watermark-3",
    )
    assert noop.action == "noop"
    forced = claim_static_site_build(
        database,
        job_id=4,
        run_id="run-4",
        input_fingerprint=fingerprint,
        effective_date="2026-07-18",
        request_watermark="watermark-4",
        force_rebuild=True,
    )
    assert forced.action == "claimed"
    finish_static_site_build_claim(
        database,
        claim_token=forced.claim_token or "",
        run_id="run-4",
        input_fingerprint=fingerprint,
        effective_date="2026-07-18",
        success=False,
    )
    with sqlite3.connect(database) as connection:
        outcomes = [row[0] for row in connection.execute(
            "SELECT outcome FROM static_site_build_history ORDER BY id"
        )]
    assert "busy" in outcomes and "noop" in outcomes and "success" in outcomes


def test_live_kaggle_ledger_prevents_stale_remote_reset(tmp_path) -> None:
    database = tmp_path / "remote.sqlite"
    _fingerprint_db(database)
    fingerprint, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full"},
    )
    old = datetime(2026, 7, 18, 0, tzinfo=timezone.utc)
    claim = claim_static_site_build(
        database,
        job_id=1,
        run_id="remote-live",
        input_fingerprint=fingerprint,
        effective_date="2026-07-18",
        request_watermark="watermark",
        now=old,
    )
    assert claim.action == "claimed"
    fresh = datetime(2026, 7, 18, 3, tzinfo=timezone.utc)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE kaggle_run_ledger(
                run_id TEXT PRIMARY KEY, status TEXT, updated_at TEXT,
                last_heartbeat_at TEXT, terminal_at TEXT
            )
            """
        )
        connection.execute(
            "INSERT INTO kaggle_run_ledger VALUES(?, 'alive', ?, ?, NULL)",
            ("remote-live", fresh.isoformat(), fresh.isoformat()),
        )
    assert active_static_site_remote_run(
        database, stale_seconds=60, now=fresh + timedelta(seconds=30)
    ) == "remote-live"
    blocked = claim_static_site_build(
        database,
        job_id=2,
        run_id="would-duplicate",
        input_fingerprint="b" * 64,
        effective_date="2026-07-18",
        request_watermark="watermark-2",
        stale_seconds=60,
        now=fresh + timedelta(seconds=30),
    )
    assert blocked.action == "busy"


class _MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def put_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key in self.objects or kwargs.get("IfNoneMatch") != "*":
            raise AssertionError("publisher attempted overwrite or non-conditional write")
        body = kwargs["Body"].read()
        self.objects[key] = {
            "Body": body,
            "ContentType": kwargs["ContentType"],
            "CacheControl": kwargs["CacheControl"],
        }
        return {"ETag": hashlib.md5(body).hexdigest()}  # nosec - in-memory S3 test token only

    def get_object(self, **kwargs):
        stored = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {**stored, "Body": io.BytesIO(stored["Body"])}


def test_add_build_01_payload_union_is_bounded_and_keeps_latest_effect() -> None:
    old = make_request_payload(
        reason="smart_update",
        event_ids=range(1, MAX_EVENT_IDS + 100),
        event_revisions={1: "r1"},
        correlation_id="c-old",
        effect_at="2026-07-17T10:00:00Z",
    )
    new = make_request_payload(
        reason="operator",
        event_ids=[2, 999],
        event_revisions={2: "r2"},
        correlation_id="c-new",
        effect_at="2026-07-17T10:05:00Z",
    )
    merged = merge_request_payload(old, new)
    assert merged["release_channel"] == "secret_preview"
    assert merged["latest_effect_at"] == "2026-07-17T10:05:00Z"
    assert merged["reasons"] == ["smart_update", "operator"]
    assert merged["event_revisions"] == {"1": "r1", "2": "r2"}
    assert len(merged["event_ids"]) == MAX_EVENT_IDS
    assert len(merged["correlation_ids"]) <= MAX_CORRELATIONS
    assert len(merged["target_watermark"]) == 64


def test_add_build_08_online_backup_is_immutable_and_hash_bound(tmp_path) -> None:
    source = tmp_path / "live.sqlite"
    writer = sqlite3.connect(source)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("CREATE TABLE event(id INTEGER PRIMARY KEY, added_at TEXT, title TEXT)")
    writer.execute("INSERT INTO event VALUES(1, '2026-07-17T10:00:00Z', 'one')")
    writer.commit()
    request = make_request_payload(reason="test", event_ids=[1], correlation_id="snapshot")

    snapshot, manifest, metadata = create_immutable_snapshot(
        source, tmp_path / "snapshots", request_payload=request, snapshot_id="add-build-08"
    )
    validated = validate_snapshot(snapshot, manifest)
    assert validated.sha256 == metadata.sha256
    assert validated.quick_check == "ok"
    assert validated.max_event_id == 1

    writer.execute("INSERT INTO event VALUES(2, '2026-07-17T10:01:00Z', 'two')")
    writer.commit()
    with sqlite3.connect(snapshot) as frozen:
        assert frozen.execute("SELECT count(*) FROM event").fetchone()[0] == 1
    writer.close()

    snapshot.write_bytes(snapshot.read_bytes() + b"tamper")
    with pytest.raises(StaticSitePermanentError, match="hash_or_size"):
        validate_snapshot(snapshot, manifest)


def test_add_related_01_02_03_04_vector_barrier_and_optional_base_contract(tmp_path) -> None:
    """Acceptance metadata: ADD-RELATED-01, -02, -03 and -04."""
    disabled = make_request_payload(reason="base", event_ids=[1], require_vector_barrier=False)
    assert validate_vector_barrier(disabled, None)["status"] == "disabled"

    required = make_request_payload(
        reason="related",
        event_ids=[1],
        event_revisions={1: "revision-1"},
        require_vector_barrier=True,
        expected_related_v1_hash="related-hash",
    )
    with pytest.raises(StaticSiteRetryableError, match="pending"):
        validate_vector_barrier(required, None)
    receipt = tmp_path / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "status": "complete",
                "related_v1_hash": "related-hash",
                "event_revisions": {"1": "revision-1"},
                "run_id": "projection-1",
            }
        ),
        encoding="utf-8",
    )
    assert validate_vector_barrier(required, receipt)["projection_run_id"] == "projection-1"


def test_add_build_13_and_add_obs_failure_and_freshness_contract() -> None:
    assert classify_failure(StaticSitePermanentError("bad")).retryable is False
    assert classify_failure(StaticSiteRetryableError("later")).retryable is True
    now = datetime(2026, 7, 17, 12, tzinfo=timezone.utc)
    stale = freshness_state(
        latest_effect_at="2026-07-17T10:00:00Z",
        latest_success_at="2026-07-17T09:00:00Z",
        has_active_request=False,
        now=now,
        max_staleness_seconds=3600,
    )
    assert stale == {"status": "stale", "stale": True, "age_seconds": 7200, "active": False}


def test_add_build_10_secret_candidate_publish_is_create_only_and_root_isolated(tmp_path) -> None:
    token = "A" * 43
    candidate = tmp_path / "candidate" / "_review" / token
    candidate.mkdir(parents=True)
    index = b"<!doctype html><meta name=robots content=noindex>"
    (candidate / "index.html").write_bytes(index)
    manifest = {
        "schema_version": "static_secret_candidate_manifest_v1",
        "site_mode": "secret_candidate",
        "publication_mode": "secret_link",
        "build_id": "production-test",
        "run_id": "static-site:test:run",
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "checks": {
            "candidate_contract": "ok", "catalog_parity": "ok", "noindex": "ok",
            "no_referrer": "ok", "prefix_containment": "ok", "root_isolation": "ok",
        },
        "files": [{
            "key": "index.html", "sha256": hashlib.sha256(index).hexdigest(),
            "size": len(index), "content_type": "text/html; charset=utf-8",
            "cache_control": "private, no-store, max-age=0",
        }],
    }
    (candidate / "secret-candidate-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    archive = tmp_path / "candidate.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(candidate, arcname=f"_review/{token}")
    client = _MemoryS3()
    receipt = publish_secret_candidate_archive(
        archive,
        build_result={
            "build_id": "production-test", "run_id": "static-site:test:run",
            "snapshot": {"snapshot_id": "snapshot-test"}, "candidate": {"token": token},
        },
        extraction_root=tmp_path / "extract", bucket="bucket", endpoint="https://storage.invalid",
        region="ru-central1", access_key_id="test", secret_access_key="test",
        s3_client=client, list_preflight=lambda endpoint, bucket: True,
        public_probe=lambda url: url.endswith(f"/_review/{token}/"),
    )
    assert receipt.root_mutation is False and receipt.stable_ics_mutation is False
    assert receipt.object_count == 2
    assert all(key.startswith(f"_review/{token}/") for _bucket, key in client.objects)


@pytest.mark.asyncio
async def test_add_build_01_running_request_gets_exactly_one_merged_followup(tmp_path) -> None:
    import main
    from db import Database
    from models import JobOutbox, JobStatus, JobTask
    from sqlalchemy import select

    db = Database(str(tmp_path / "outbox.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    running_payload = make_request_payload(reason="first", event_ids=[1], correlation_id="first")
    async with db.get_session() as session:
        session.add(
            JobOutbox(
                event_id=1,
                task=JobTask.static_site_build,
                status=JobStatus.running,
                payload=running_payload,
                coalesce_key="static_site_build:prod",
                next_run_at=now,
                updated_at=now,
            )
        )
        await session.commit()

    for event_id in (2, 3):
        await main.enqueue_job(
            db,
            event_id,
            JobTask.static_site_build,
            payload=make_request_payload(
                reason="smart_update", event_ids=[event_id], correlation_id=f"event-{event_id}"
            ),
            coalesce_key="static_site_build:prod",
            next_run_at=now + timedelta(minutes=15 + event_id),
        )

    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(JobOutbox)
                .where(JobOutbox.coalesce_key == "static_site_build:prod")
                .order_by(JobOutbox.id)
            )
        ).scalars().all()
    assert len(rows) == 2
    assert rows[0].status == JobStatus.running
    assert rows[0].payload == running_payload
    assert rows[1].status == JobStatus.pending
    assert rows[1].payload["event_ids"] == [2, 3]
    await db.close()


@pytest.mark.asyncio
async def test_add_build_01_on_demand_uses_same_secret_outbox(tmp_path) -> None:
    import main
    from db import Database
    from models import JobOutbox, JobTask
    from sqlalchemy import select

    db = Database(str(tmp_path / "request.sqlite"))
    await db.init()
    result = await main.enqueue_static_site_build_request(
        db, reason="operator smoke", event_ids=[], correlation_id="manual-1"
    )
    assert result in {"new", "requeued", "merged-rearmed"}
    async with db.get_session() as session:
        row = (
            await session.execute(select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build))
        ).scalar_one()
    assert row.coalesce_key == "static_site_build:prod"
    assert row.payload["release_channel"] == "secret_preview"
    assert row.payload["trigger"] == "operator_request"
    await db.close()


@pytest.mark.asyncio
async def test_add_build_13_retryable_failure_is_bounded(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import main
    from db import Database
    from models import JobOutbox, JobStatus, JobTask
    from sqlalchemy import select

    db = Database(str(tmp_path / "retry.sqlite"))
    await db.init()
    await main.enqueue_static_site_build_request(
        db, reason="retry test", correlation_id="retry-1"
    )

    async def fail_retryably(event_id, db, bot):
        raise StaticSiteRetryableError("provider temporarily unavailable")

    monkeypatch.setitem(main.JOB_HANDLERS, JobTask.static_site_build.value, fail_retryably)
    monkeypatch.setenv("STATIC_SITE_MAX_ATTEMPTS", "4")
    for _ in range(4):
        async with db.get_session() as session:
            row = (
                await session.execute(
                    select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build)
                )
            ).scalar_one()
            row.next_run_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            session.add(row)
            await session.commit()
        assert await main._run_due_jobs_once(db, bot=None) == 1

    async with db.get_session() as session:
        row = (
            await session.execute(select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build))
        ).scalar_one()
    assert row.status == JobStatus.error
    assert row.attempts == 4
    assert row.payload["failure"]["class"] == "retryable_dependency"
    assert row.payload["failure"]["attempt"] == 4
    assert row.next_run_at > datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=3000)
    await db.close()
