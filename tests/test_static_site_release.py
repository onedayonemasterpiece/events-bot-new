from __future__ import annotations

import json
import hashlib
import io
import sqlite3
import tarfile
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

from static_site_release import (
    CURRENT_SECRET_CANDIDATE_RECEIPT_SCHEMA,
    MAX_CORRELATIONS,
    MAX_EVENT_IDS,
    StaticSitePermanentError,
    StaticSiteRetryableError,
    active_static_site_remote_run,
    recoverable_static_site_build,
    classify_failure,
    claim_static_site_build,
    compute_static_site_input_fingerprint,
    create_immutable_snapshot,
    delete_immutable_snapshot,
    delete_static_site_output,
    finish_static_site_build_claim,
    freshness_state,
    make_request_payload,
    merge_request_payload,
    publish_secret_candidate_archive,
    prune_immutable_snapshots,
    prune_static_site_outputs,
    resolve_build_clock,
    resolve_current_secret_candidate,
    static_site_result_counts,
    validate_production_candidate_result,
    validate_snapshot,
    validate_vector_barrier,
)


def _current_candidate_receipt(
    token: str,
    *,
    build_id: str,
    run_id: str,
    fingerprint: str,
) -> dict[str, object]:
    return {
        "schema_version": CURRENT_SECRET_CANDIDATE_RECEIPT_SCHEMA,
        "release_channel": "secret_preview",
        "build_id": build_id,
        "run_id": run_id,
        "repo_sha": "a" * 40,
        "snapshot_id": f"snapshot-{build_id}",
        "input_fingerprint": fingerprint,
        "effective_date": "2026-07-18",
        "result_sha256": "b" * 64,
        "manifest_sha256": "c" * 64,
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "object_count": 42,
        "public_url": f"https://kenigevents.ru/_review/{token}/",
        "verified_at": "2026-07-18T12:00:00Z",
        "root_mutation": False,
        "stable_ics_mutation": False,
    }


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


@pytest.mark.asyncio
async def test_current_review_resolver_tracks_latest_checked_candidate_and_preserves_previous(
    tmp_path,
) -> None:
    import main

    database = tmp_path / "current-review.sqlite"
    _fingerprint_db(database)
    first_fingerprint, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full", "revision": 1},
    )
    first_token = "A" * 43
    first = claim_static_site_build(
        database,
        job_id=1,
        run_id="static-site:first",
        input_fingerprint=first_fingerprint,
        effective_date="2026-07-18",
        request_watermark="first",
    )
    finish_static_site_build_claim(
        database,
        claim_token=first.claim_token or "",
        run_id="static-site:first",
        input_fingerprint=first_fingerprint,
        effective_date="2026-07-18",
        success=True,
        receipt={
            "current_secret_candidate": _current_candidate_receipt(
                first_token,
                build_id="production-first",
                run_id="static-site:first",
                fingerprint=first_fingerprint,
            )
        },
    )
    current = resolve_current_secret_candidate(database)
    assert current is not None
    assert current.public_url.endswith(f"/_review/{first_token}/")

    # An unchanged request does not publish and must not erase the last review.
    noop = claim_static_site_build(
        database,
        job_id=2,
        run_id="static-site:noop",
        input_fingerprint=first_fingerprint,
        effective_date="2026-07-18",
        request_watermark="noop",
    )
    assert noop.action == "noop"
    assert resolve_current_secret_candidate(database) == current

    second_fingerprint, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-07-18",
        repo_sha="a" * 40,
        build_config={"catalog_mode": "full", "revision": 2},
    )
    failed = claim_static_site_build(
        database,
        job_id=3,
        run_id="static-site:failed",
        input_fingerprint=second_fingerprint,
        effective_date="2026-07-18",
        request_watermark="failed",
    )
    finish_static_site_build_claim(
        database,
        claim_token=failed.claim_token or "",
        run_id="static-site:failed",
        input_fingerprint=second_fingerprint,
        effective_date="2026-07-18",
        success=False,
        receipt={"status": "failed"},
    )
    assert resolve_current_secret_candidate(database) == current

    # The outbox/bot link producer consumes the same resolver rather than a
    # hard-coded historical preview URL.
    database_ref = type("DatabaseRef", (), {"path": str(database)})()
    assert (
        await main._job_result_link(main.JobTask.static_site_build, 0, database_ref)
        == current.public_url
    )

    second_token = "B" * 43
    second = claim_static_site_build(
        database,
        job_id=4,
        run_id="static-site:second",
        input_fingerprint=second_fingerprint,
        effective_date="2026-07-18",
        request_watermark="second",
    )
    finish_static_site_build_claim(
        database,
        claim_token=second.claim_token or "",
        run_id="static-site:second",
        input_fingerprint=second_fingerprint,
        effective_date="2026-07-18",
        success=True,
        receipt={
            "current_secret_candidate": _current_candidate_receipt(
                second_token,
                build_id="production-second",
                run_id="static-site:second",
                fingerprint=second_fingerprint,
            )
        },
    )
    latest = resolve_current_secret_candidate(database)
    assert latest is not None and latest.run_id == "static-site:second"
    assert await main._job_result_link(main.JobTask.static_site_build, 0, database_ref) == latest.public_url


def test_current_review_resolver_rejects_incomplete_receipt_without_replacing_previous(tmp_path) -> None:
    database = tmp_path / "invalid-current-review.sqlite"
    _fingerprint_db(database)
    first_fingerprint = "1" * 64
    first_token = "C" * 43
    first = claim_static_site_build(
        database,
        job_id=1,
        run_id="static-site:first",
        input_fingerprint=first_fingerprint,
        effective_date="2026-07-18",
        request_watermark="first",
    )
    finish_static_site_build_claim(
        database,
        claim_token=first.claim_token or "",
        run_id="static-site:first",
        input_fingerprint=first_fingerprint,
        effective_date="2026-07-18",
        success=True,
        receipt={
            "current_secret_candidate": _current_candidate_receipt(
                first_token,
                build_id="production-first",
                run_id="static-site:first",
                fingerprint=first_fingerprint,
            )
        },
    )
    previous = resolve_current_secret_candidate(database)
    second_fingerprint = "2" * 64
    second = claim_static_site_build(
        database,
        job_id=2,
        run_id="static-site:second",
        input_fingerprint=second_fingerprint,
        effective_date="2026-07-18",
        request_watermark="second",
    )
    incomplete = _current_candidate_receipt(
        "D" * 43,
        build_id="production-second",
        run_id="static-site:second",
        fingerprint=second_fingerprint,
    )
    incomplete.pop("manifest_sha256")
    with pytest.raises(StaticSitePermanentError, match="manifest_sha256"):
        finish_static_site_build_claim(
            database,
            claim_token=second.claim_token or "",
            run_id="static-site:second",
            input_fingerprint=second_fingerprint,
            effective_date="2026-07-18",
            success=True,
            receipt={"current_secret_candidate": incomplete},
        )
    assert resolve_current_secret_candidate(database) == previous
    finish_static_site_build_claim(
        database,
        claim_token=second.claim_token or "",
        run_id="static-site:second",
        input_fingerprint=second_fingerprint,
        effective_date="2026-07-18",
        success=False,
        receipt={"status": "rejected_incomplete_receipt"},
    )


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


def test_terminal_orphan_remains_recoverable_by_exact_job_and_dataset(tmp_path) -> None:
    database = tmp_path / "terminal.sqlite"
    _fingerprint_db(database)
    claim = claim_static_site_build(
        database,
        job_id=41,
        run_id="remote-terminal",
        input_fingerprint="c" * 64,
        effective_date="2026-07-18",
        request_watermark="watermark",
    )
    assert claim.action == "claimed"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE kaggle_run_ledger(
                run_id TEXT PRIMARY KEY, kernel_ref TEXT, dataset_ref TEXT,
                status TEXT, terminal_at TEXT
            )
            """
        )
        connection.execute(
            "INSERT INTO kaggle_run_ledger VALUES(?, ?, ?, 'done', ?)",
            ("remote-terminal", "owner/kernel", "owner/exact-input", "2026-07-18T12:00:00Z"),
        )
    recovered = recoverable_static_site_build(database, job_id=41)
    assert recovered is not None
    assert recovered.claim_token == claim.claim_token
    assert recovered.run_id == "remote-terminal"
    assert recovered.dataset_ref == "owner/exact-input"
    assert recovered.remote_status == "done"
    assert recoverable_static_site_build(database, job_id=42) is None


def test_terminal_remote_claim_does_not_block_new_coalesced_job_for_claim_ttl(tmp_path) -> None:
    database = tmp_path / "terminal-supersede.sqlite"
    _fingerprint_db(database)
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    first = claim_static_site_build(
        database,
        job_id=41,
        run_id="remote-terminal-error",
        input_fingerprint="c" * 64,
        effective_date="2026-07-20",
        request_watermark="old-watermark",
        now=now,
    )
    assert first.action == "claimed"
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
            "INSERT INTO kaggle_run_ledger VALUES(?, 'failed', ?, ?, ?)",
            (
                "remote-terminal-error",
                now.isoformat(),
                now.isoformat(),
                (now + timedelta(seconds=10)).isoformat(),
            ),
        )

    replacement = claim_static_site_build(
        database,
        job_id=42,
        run_id="replacement-run",
        input_fingerprint="d" * 64,
        effective_date="2026-07-20",
        request_watermark="new-watermark",
        stale_seconds=7200,
        now=now + timedelta(seconds=20),
    )
    assert replacement.action == "claimed"
    with sqlite3.connect(database) as connection:
        active = connection.execute(
            "SELECT active_job_id, active_run_id FROM static_site_build_state"
        ).fetchone()
        superseded = connection.execute(
            "SELECT evidence_json FROM static_site_build_history "
            "WHERE run_id='remote-terminal-error' AND outcome='failed' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert active == (42, "replacement-run")
    assert json.loads(superseded[0])["reason"] == "terminal_remote_claim_superseded"


class _MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def put_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") != "*":
            raise AssertionError("publisher attempted overwrite or non-conditional write")
        if key in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed", "Message": "already exists"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        body = kwargs["Body"].read()
        self.objects[key] = {
            "Body": body,
            "ContentType": kwargs["ContentType"],
            "CacheControl": kwargs["CacheControl"],
            "Metadata": dict(kwargs.get("Metadata") or {}),
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


def test_snapshot_retention_preserves_active_and_bounds_terminal_leaks(tmp_path) -> None:
    source = tmp_path / "live.sqlite"
    _fingerprint_db(source)
    root = tmp_path / "snapshots"
    snapshots = []
    for index in range(4):
        snapshot, manifest, _metadata = create_immutable_snapshot(
            source,
            root,
            request_payload=make_request_payload(reason=f"retention-{index}"),
            snapshot_id=f"snapshot-retention-{index}",
        )
        snapshot.touch()
        snapshots.append((snapshot, manifest))

    report = prune_immutable_snapshots(
        root,
        preserve_paths=snapshots[0],
        keep_latest_terminal=1,
    )
    assert snapshots[0][0].is_file() and snapshots[0][1].is_file()
    assert snapshots[3][0].is_file() and snapshots[3][1].is_file()
    assert not snapshots[1][0].exists() and not snapshots[2][0].exists()
    assert len(report["removed_snapshot_ids"]) == 2
    assert report["removed_bytes"] > 0

    removed = delete_immutable_snapshot(*snapshots[3])
    assert removed > 0
    assert not snapshots[3][0].exists() and not snapshots[3][1].exists()


def test_snapshot_retention_removes_only_stale_recognized_incomplete_files(tmp_path) -> None:
    root = tmp_path / "snapshots"
    root.mkdir()
    old_tmp = root / ".snapshot-old.deadbeef.tmp"
    orphan_sqlite = root / "snapshot-orphan.sqlite"
    recent_tmp = root / ".snapshot-recent.deadbeef.tmp"
    unknown = root / "operator-note.txt"
    for path in (old_tmp, orphan_sqlite, recent_tmp, unknown):
        path.write_bytes(b"evidence")
    old = datetime.now(timezone.utc).timestamp() - 3600
    os.utime(old_tmp, (old, old))
    os.utime(orphan_sqlite, (old, old))
    os.utime(unknown, (old, old))

    report = prune_immutable_snapshots(root, stale_incomplete_seconds=900)
    assert sorted(report["removed_incomplete_files"]) == sorted(
        [old_tmp.name, orphan_sqlite.name]
    )
    assert not old_tmp.exists() and not orphan_sqlite.exists()
    assert recent_tmp.exists(), "a possibly active backup temp must be preserved"
    assert unknown.exists(), "unknown operator evidence must never be pruned"


def test_output_retention_preserves_exact_handoff_and_unknown_paths(tmp_path) -> None:
    root = tmp_path / "builder"
    root.mkdir()
    builds = [f"production-retention-{index}" for index in range(4)]
    for index, build_id in enumerate(builds):
        output = root / f"output-{build_id}"
        output.mkdir()
        (output / "payload.bin").write_bytes(b"x" * (index + 1))
        os.utime(output, (100 + index, 100 + index))
    unknown_dir = root / "operator-evidence"
    unknown_dir.mkdir()
    (unknown_dir / "keep.txt").write_text("keep", encoding="utf-8")
    preview = root / "output-preview-manual"
    preview.mkdir()
    symlink = root / "output-production-symlink"
    symlink.symlink_to(unknown_dir, target_is_directory=True)

    report = prune_static_site_outputs(
        root,
        preserve_build_ids=[builds[0]],
        keep_latest_terminal=1,
    )

    assert (root / f"output-{builds[0]}").is_dir()
    assert (root / f"output-{builds[3]}").is_dir()
    assert not (root / f"output-{builds[1]}").exists()
    assert not (root / f"output-{builds[2]}").exists()
    assert unknown_dir.is_dir() and preview.is_dir() and symlink.is_symlink()
    assert sorted(report["removed_build_ids"]) == sorted(builds[1:3])
    assert report["removed_bytes"] == 5
    assert report["skipped_symlink_build_ids"] == ["production-symlink"]

    assert delete_static_site_output(root, builds[3]) == 4
    with pytest.raises(StaticSitePermanentError, match="identity_invalid"):
        delete_static_site_output(root, "preview-manual")


def test_static_site_result_counts_retains_only_redacted_numeric_diagnostics() -> None:
    counts = static_site_result_counts(
        {
            "event_count": 10,
            "counts": {
                "event_page_count": "10",
                "page_count": 20,
                "file_count": 30,
                "bytes": 1000,
                "candidate_token": "never-store",
                "public_url": "https://example.test/_review/secret/",
            },
        },
        object_count=42,
    )

    assert counts == {
        "event_count": 10,
        "event_page_count": 10,
        "page_count": 20,
        "file_count": 30,
        "object_count": 42,
        "bytes": 1000,
    }


def test_production_candidate_result_requires_exact_template_and_noindex_checks(tmp_path) -> None:
    source = tmp_path / "source.sqlite"
    _fingerprint_db(source)
    snapshot, _manifest, metadata = create_immutable_snapshot(
        source,
        tmp_path / "snapshots",
        request_payload=make_request_payload(reason="checks"),
        snapshot_id="candidate-checks",
    )
    assert snapshot.is_file()
    output = tmp_path / "output"
    output.mkdir()
    artifacts = []
    for kind, filename in (
        ("production_root", "production-root.tar.gz"),
        ("secret_candidate", "secret-candidate.tar.gz"),
        ("browser_evidence", "browser-evidence.tar.gz"),
    ):
        path = output / filename
        with tarfile.open(path, "w:gz") as bundle:
            payload = b"checked"
            info = tarfile.TarInfo(name=f"{kind}.txt")
            info.size = len(payload)
            bundle.addfile(info, io.BytesIO(payload))
        artifacts.append(
            {
                "kind": kind,
                "filename": filename,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size": path.stat().st_size,
            }
        )
    token = "E" * 43
    clock = resolve_build_clock(current_date="2026-07-18")
    result = {
        "schema_version": "static_site_build_result_v2",
        "ok": True,
        "profile": "production-candidate",
        "build_id": "production-checks",
        "run_id": "static-site:checks",
        "repo_sha": "a" * 40,
        "input_fingerprint": "f" * 64,
        "build_clock": {
            "time_zone": clock.time_zone,
            "effective_date": clock.effective_date,
            "current_datetime": clock.current_datetime,
        },
        "snapshot": {
            "snapshot_id": metadata.snapshot_id,
            "snapshot_sha256": metadata.sha256,
            "size": metadata.size_bytes,
        },
        "candidate": {"token": token},
        "checks": {
            "preview_contract": {
                "status": "ok",
                "build_id": "preview-gate-production-checks",
                "archived": False,
                "published": False,
            },
            "production": {
                "astro_build": "ok",
                "template_matrix": "ok",
                "production_contract": "ok",
                "catalog_parity": "ok",
                "fixture_isolation": "ok",
                "canonical_and_indexing": "ok",
                "tree_hashes": "ok",
                "browser_visual": "ok",
            },
            "secret_candidate": {
                "astro_build": "ok",
                "candidate_contract": "ok",
                "catalog_parity": "ok",
                "noindex": "ok",
                "no_referrer": "ok",
                "prefix_containment": "ok",
                "root_isolation": "ok",
                "browser_visual": "ok",
            },
        },
        "artifacts": artifacts,
    }
    result_path = output / "static_site_build_result.json"
    result_path.write_text(json.dumps(result), encoding="utf-8")
    validated, candidate_archive = validate_production_candidate_result(
        result_path,
        output_dir=output,
        build_id="production-checks",
        run_id="static-site:checks",
        repo_sha="a" * 40,
        snapshot=metadata,
        candidate_token=token,
        input_fingerprint="f" * 64,
        build_clock=clock,
    )
    assert validated["checks"]["production"]["template_matrix"] == "ok"
    assert validated["checks"]["preview_contract"]["status"] == "ok"
    assert candidate_archive.name == "secret-candidate.tar.gz"

    result["checks"]["preview_contract"]["status"] = "pending"
    result_path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(StaticSitePermanentError, match="preview_contract"):
        validate_production_candidate_result(
            result_path,
            output_dir=output,
            build_id="production-checks",
            run_id="static-site:checks",
            repo_sha="a" * 40,
            snapshot=metadata,
            candidate_token=token,
            input_fingerprint="f" * 64,
            build_clock=clock,
        )
    result["checks"]["preview_contract"]["status"] = "ok"
    result["checks"]["preview_contract"]["archived"] = True
    result_path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(StaticSitePermanentError, match="release_leak"):
        validate_production_candidate_result(
            result_path,
            output_dir=output,
            build_id="production-checks",
            run_id="static-site:checks",
            repo_sha="a" * 40,
            snapshot=metadata,
            candidate_token=token,
            input_fingerprint="f" * 64,
            build_clock=clock,
        )
    result["checks"]["preview_contract"]["archived"] = False
    result["checks"]["production"]["template_matrix"] = "pending"
    result_path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(StaticSitePermanentError, match="template_matrix"):
        validate_production_candidate_result(
            result_path,
            output_dir=output,
            build_id="production-checks",
            run_id="static-site:checks",
            repo_sha="a" * 40,
            snapshot=metadata,
            candidate_token=token,
            input_fingerprint="f" * 64,
            build_clock=clock,
        )

    result["checks"]["production"]["template_matrix"] = "ok"
    result["checks"]["secret_candidate"]["browser_visual"] = "pending"
    result_path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(StaticSitePermanentError, match="browser_visual"):
        validate_production_candidate_result(
            result_path,
            output_dir=output,
            build_id="production-checks",
            run_id="static-site:checks",
            repo_sha="a" * 40,
            snapshot=metadata,
            candidate_token=token,
            input_fingerprint="f" * 64,
            build_clock=clock,
        )

    result["checks"]["secret_candidate"]["browser_visual"] = "ok"
    result["artifacts"] = [
        artifact for artifact in artifacts if artifact["kind"] != "browser_evidence"
    ]
    result_path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(StaticSitePermanentError, match="artifact_set_mismatch"):
        validate_production_candidate_result(
            result_path,
            output_dir=output,
            build_id="production-checks",
            run_id="static-site:checks",
            repo_sha="a" * 40,
            snapshot=metadata,
            candidate_token=token,
            input_fingerprint="f" * 64,
            build_clock=clock,
        )


def test_add_related_01_02_03_04_vector_barrier_and_optional_base_contract(tmp_path) -> None:
    """Acceptance metadata: ADD-RELATED-01, -02, -03 and -04."""
    disabled = make_request_payload(reason="base", event_ids=[1], require_vector_barrier=False)
    assert validate_vector_barrier(disabled, None)["status"] == "disabled"

    related_hash = "b" * 64
    required = make_request_payload(
        reason="related",
        event_ids=[1],
        event_revisions={1: "revision-1"},
        require_vector_barrier=True,
        expected_related_v1_hash=related_hash,
    )
    with pytest.raises(StaticSiteRetryableError, match="pending"):
        validate_vector_barrier(required, None)
    receipt = tmp_path / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": "event_vector_sync_receipt_v1",
                "status": "complete",
                "complete": True,
                "search_v3_hash": "a" * 64,
                "related_v1_hash": related_hash,
                "event_revisions": {"1": "revision-1"},
                "run_id": "projection-1",
            }
        ),
        encoding="utf-8",
    )
    assert validate_vector_barrier(required, receipt)["projection_run_id"] == "projection-1"

    malformed = tmp_path / "malformed-receipt.json"
    malformed.write_text(
        json.dumps(
            {
                "schema_version": "event_vector_sync_receipt_v1",
                "status": "complete",
                "complete": True,
                "search_v3_hash": "not-a-corpus-hash",
                "related_v1_hash": related_hash,
                "event_revisions": {"1": "revision-1"},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(StaticSitePermanentError, match="hash_invalid:search_v3_hash"):
        validate_vector_barrier(required, malformed)


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
            "no_referrer": "ok", "prefix_containment": "ok", "root_isolation": "ok", "browser_visual": "ok",
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

    # Recovery after a receipt-write lock adopts only byte-identical objects
    # under the same immutable token; it never performs an overwrite.
    adopted = publish_secret_candidate_archive(
        archive,
        build_result={
            "build_id": "production-test", "run_id": "static-site:test:run",
            "snapshot": {"snapshot_id": "snapshot-test"}, "candidate": {"token": token},
        },
        extraction_root=tmp_path / "extract-retry", bucket="bucket",
        endpoint="https://storage.invalid", region="ru-central1",
        access_key_id="test", secret_access_key="test", s3_client=client,
        list_preflight=lambda endpoint, bucket: True,
        public_probe=lambda url: url.endswith(f"/_review/{token}/"),
    )
    assert adopted.object_count == receipt.object_count
    assert adopted.manifest_sha256 == receipt.manifest_sha256
    assert adopted.token_sha256 == receipt.token_sha256
    assert adopted.public_url == receipt.public_url


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
