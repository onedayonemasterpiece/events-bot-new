from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from static_site_diagnostics import (
    collect_static_site_diagnostics,
    format_static_site_diagnostics,
    redact,
)
from static_site_release import delete_static_site_output


NOW = datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc)
TOKEN = "A" * 43


def _schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE static_site_build_state(
            release_channel TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            last_success_fingerprint TEXT,
            last_success_run_id TEXT,
            last_success_at TEXT,
            last_success_receipt_json TEXT NOT NULL DEFAULT '{}',
            current_secret_candidate_receipt_json TEXT,
            active_claim_token TEXT,
            active_job_id INTEGER,
            active_run_id TEXT,
            active_fingerprint TEXT,
            active_effective_date TEXT,
            active_claimed_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE static_site_build_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            release_channel TEXT NOT NULL,
            job_id INTEGER,
            request_watermark TEXT,
            input_fingerprint TEXT NOT NULL,
            effective_date TEXT NOT NULL,
            force_rebuild INTEGER NOT NULL DEFAULT 0,
            outcome TEXT NOT NULL,
            run_id TEXT,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE TABLE kaggle_run_ledger(
            run_id TEXT PRIMARY KEY,
            kind TEXT,
            status TEXT NOT NULL,
            phase TEXT,
            progress_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT,
            updated_at TEXT,
            last_heartbeat_at TEXT,
            terminal_at TEXT
        );
        """
    )


def _insert_history(
    connection: sqlite3.Connection,
    *,
    job_id: int | None,
    outcome: str,
    run_id: str,
    evidence: dict | str = "{}",
) -> None:
    payload = evidence if isinstance(evidence, str) else json.dumps(evidence)
    connection.execute(
        """
        INSERT INTO static_site_build_history(
            release_channel, job_id, input_fingerprint, effective_date,
            outcome, run_id, evidence_json, created_at
        ) VALUES('secret_preview', ?, ?, '2026-07-18', ?, ?, ?, '2026-07-18T11:00:00Z')
        """,
        (job_id, hashlib.sha256(run_id.encode()).hexdigest(), outcome, run_id, payload),
    )


def _insert_ledger(connection: sqlite3.Connection, run_id: str, status: str) -> None:
    connection.execute(
        """
        INSERT INTO kaggle_run_ledger(
            run_id, kind, status, phase, progress_json, created_at, updated_at,
            last_heartbeat_at, terminal_at
        ) VALUES(?, 'static_site_builder', ?, 'report', '{}',
                 '2026-07-18T10:00:00Z', '2026-07-18T11:00:00Z',
                 '2026-07-18T10:30:00Z', '2026-07-18T11:00:00Z')
        """,
        (run_id, status),
    )


def _current_receipt(token_sha256: str) -> dict:
    return {
        "schema_version": "static_site_current_secret_candidate_v1",
        "release_channel": "secret_preview",
        "build_id": "production-safe-build",
        "run_id": "run-success",
        "repo_sha": "a" * 40,
        "snapshot_id": "snapshot-safe",
        "input_fingerprint": "b" * 64,
        "effective_date": "2026-07-18",
        "result_sha256": "c" * 64,
        "manifest_sha256": "d" * 64,
        "token_sha256": token_sha256,
        "object_count": 42,
        "public_url": f"https://kenigevents.ru/_review/{TOKEN}/?access_token=never-print",
        "verified_at": "2026-07-18T11:00:00Z",
        "root_mutation": False,
        "stable_ics_mutation": False,
    }


def test_collects_last_24h_counts_channels_and_redacts_bearer_inputs(tmp_path: Path) -> None:
    database = tmp_path / "db.sqlite"
    token_hash = hashlib.sha256(TOKEN.encode()).hexdigest()
    receipt = _current_receipt(token_hash)
    with sqlite3.connect(database) as connection:
        _schema(connection)
        _insert_history(connection, job_id=1, outcome="claimed", run_id="run-success")
        _insert_history(
            connection,
            job_id=None,
            outcome="success",
            run_id="run-success",
            evidence={"event_count": 10, "current_secret_candidate": receipt},
        )
        _insert_history(connection, job_id=2, outcome="claimed", run_id="run-failed")
        _insert_history(connection, job_id=None, outcome="failed", run_id="run-failed")
        _insert_history(connection, job_id=3, outcome="noop", run_id="run-noop")
        _insert_ledger(connection, "run-success", "done")
        _insert_ledger(connection, "run-failed", "failed")
        connection.execute(
            """
            INSERT INTO static_site_build_state(
                release_channel, schema_version, last_success_fingerprint,
                last_success_run_id, last_success_at, last_success_receipt_json,
                current_secret_candidate_receipt_json, updated_at
            ) VALUES('secret_preview', 'static_site_build_state_v1', ?, 'run-success',
                     '2026-07-18T11:00:00Z', ?, ?, '2026-07-18T11:00:00Z')
            """,
            ("b" * 64, json.dumps({"run_id": "run-success", "object_count": 42}), json.dumps(receipt)),
        )

    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "static_secret_candidate_manifest_v1",
                "site_mode": "secret_candidate",
                "build_id": "production-safe-build",
                "run_id": "run-success",
                "repo_sha": "a" * 40,
                "token_sha256": token_hash,
                "tree_sha256": "e" * 64,
                "counts": {
                    "event_count": 10,
                    "event_page_count": 10,
                    "page_count": 20,
                    "file_count": 30,
                    "bytes": 1000,
                },
            }
        ),
        encoding="utf-8",
    )
    bucket = tmp_path / "bucket.json"
    secret_objects = [
        {"Key": f"_review/{TOKEN}/object-{index}.html", "Size": 20}
        for index in range(41)
    ]
    secret_objects.append(
        {"Key": f"_review/{TOKEN}/secret-candidate-manifest.json", "Size": 180}
    )
    bucket.write_text(
        json.dumps(
            {
                "objects": [
                    *secret_objects,
                    {"Key": "_static/releases/release-safe/index.html", "Size": 30},
                    {"Key": "current.json", "Size": 40},
                    {"Key": "ics/123.ics", "Size": 50},
                ],
                "current_pointer": {
                    "release_id": "release-safe",
                    "build_id": "stable-safe",
                    "manifest_sha256": "f" * 64,
                },
            }
        ),
        encoding="utf-8",
    )

    report = collect_static_site_diagnostics(
        database,
        now=NOW,
        manifest_paths=[manifest],
        bucket_inventory_paths=[bucket],
    )

    summary = report["last_24h"]
    assert summary["requests"] == 3
    assert summary["succeeded"] == 1
    assert summary["failed"] == 1
    assert summary["noop"] == 1
    assert summary["generated_totals"] == {
        "event_count": 10,
        "event_page_count": 10,
        "page_count": 20,
        "file_count": 30,
        "object_count": 42,
        "bytes": 1000,
    }
    assert report["channels"]["secret_preview"]["current_pointer"]["status"] == "available"
    assert report["channels"]["secret_preview"]["current_pointer"]["location"] == "/_review/<redacted>/"
    assert report["channels"]["stable"]["current_pointer"]["status"] == "observed_from_input"
    assert report["channels"]["stable"]["lifecycle"] == "diagnostics_only_no_activation"
    assert report["consistency"]["status"] == "ok"

    serialized = json.dumps(report, ensure_ascii=False)
    rendered = format_static_site_diagnostics(report)
    assert TOKEN not in serialized
    assert TOKEN not in rendered
    assert "never-print" not in serialized
    assert "https://" not in serialized


def test_reports_history_ledger_state_and_bucket_orphans(tmp_path: Path) -> None:
    database = tmp_path / "db.sqlite"
    current_hash = hashlib.sha256(TOKEN.encode()).hexdigest()
    other_token = "B" * 43
    with sqlite3.connect(database) as connection:
        _schema(connection)
        _insert_history(connection, job_id=1, outcome="claimed", run_id="history-only")
        _insert_history(connection, job_id=None, outcome="success", run_id="history-only")
        _insert_ledger(connection, "ledger-only", "done")
        connection.execute(
            """
            INSERT INTO static_site_build_state(
                release_channel, schema_version, last_success_receipt_json,
                current_secret_candidate_receipt_json, active_claim_token,
                active_job_id, active_run_id, updated_at
            ) VALUES('secret_preview', 'static_site_build_state_v1', '{}', ?,
                     'claim-secret-never-output', 9, 'active-missing', '2026-07-18T11:00:00Z')
            """,
            (json.dumps(_current_receipt(current_hash)),),
        )
    bucket = tmp_path / "bucket.json"
    bucket.write_text(
        json.dumps({"objects": [{"key": f"_review/{other_token}/index.html", "size": 1}]}),
        encoding="utf-8",
    )

    report = collect_static_site_diagnostics(database, now=NOW, bucket_inventory_paths=[bucket])
    issue_types = report["consistency"]["issue_types"]
    assert issue_types["history_run_missing_ledger"] == 1
    assert issue_types["ledger_run_missing_history"] == 1
    assert issue_types["ledger_success_missing_history"] == 1
    assert issue_types["active_state_run_missing_ledger"] == 1
    assert issue_types["current_secret_pointer_prefix_missing"] == 1
    assert issue_types["unreferenced_secret_prefix"] == 1
    serialized = json.dumps(report)
    assert other_token not in serialized
    assert "claim-secret-never-output" not in serialized


def test_missing_tables_are_reported_without_mutating_database(tmp_path: Path) -> None:
    database = tmp_path / "minimal.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE sentinel(value TEXT)")
        connection.execute("INSERT INTO sentinel VALUES('kept')")
    before = hashlib.sha256(database.read_bytes()).hexdigest()

    report = collect_static_site_diagnostics(database, now=NOW)

    after = hashlib.sha256(database.read_bytes()).hexdigest()
    assert before == after
    assert report["sources"]["database"]["status"] == "read_only"
    assert report["sources"]["database"]["tables"] == {
        "static_site_build_history": False,
        "static_site_build_state": False,
        "kaggle_run_ledger": False,
    }
    assert report["last_24h"]["requests"] == 0


def test_durable_counts_survive_terminal_full_output_removal(tmp_path: Path) -> None:
    database = tmp_path / "db.sqlite"
    run_id = "run-durable-counts"
    counts = {
        "event_count": 10,
        "event_page_count": 10,
        "page_count": 20,
        "file_count": 30,
        "object_count": 42,
        "bytes": 1000,
    }
    with sqlite3.connect(database) as connection:
        _schema(connection)
        _insert_history(connection, job_id=1, outcome="success", run_id=run_id)
        _insert_ledger(connection, run_id, "done")
        current = _current_receipt(hashlib.sha256(TOKEN.encode()).hexdigest())
        current["run_id"] = run_id
        connection.execute(
            """
            INSERT INTO static_site_build_state(
                release_channel, schema_version, last_success_run_id,
                last_success_at, last_success_receipt_json,
                current_secret_candidate_receipt_json, updated_at
            ) VALUES('secret_preview', 'static_site_build_state_v1', ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                "2026-07-18T11:00:00Z",
                json.dumps({"run_id": run_id, "counts": counts}),
                json.dumps(current),
                "2026-07-18T11:00:00Z",
            ),
        )

    artifact_root = tmp_path / "builder"
    output = artifact_root / "output-production-durable-counts"
    output.mkdir(parents=True)
    (output / "full-output.bin").write_bytes(b"full-output")
    assert delete_static_site_output(
        artifact_root, "production-durable-counts"
    ) == len(b"full-output")
    assert not output.exists()

    report = collect_static_site_diagnostics(database, now=NOW)

    assert report["last_24h"]["generated_totals"] == counts
    serialized = json.dumps(report)
    assert TOKEN not in serialized
    assert "https://" not in serialized


def test_cli_json_is_redacted_and_read_only(tmp_path: Path) -> None:
    database = tmp_path / "db.sqlite"
    with sqlite3.connect(database) as connection:
        _schema(connection)
        _insert_history(
            connection,
            job_id=1,
            outcome="noop",
            run_id="run-noop",
            evidence={"public_url": f"https://example.test/_review/{TOKEN}/"},
        )
    script = Path(__file__).parents[1] / "scripts" / "static_site_build_diagnostics.py"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--db",
            str(database),
            "--format",
            "json",
            "--now",
            "2026-07-18T12:00:00Z",
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    payload = json.loads(completed.stdout)
    assert payload["last_24h"]["noop"] == 1
    assert TOKEN not in completed.stdout
    assert "https://" not in completed.stdout


def test_redact_handles_unexpected_nested_credentials() -> None:
    value = redact(
        {
            "nested": [
                {"authorization": f"Bearer {TOKEN}"},
                {"callback_url": f"https://host/_review/{TOKEN}/?token=x"},
                {"candidate_token": TOKEN},
                {"token_sha256": "a" * 64},
            ]
        }
    )
    serialized = json.dumps(value)
    assert TOKEN not in serialized
    assert "https://" not in serialized
    assert value["nested"][3]["token_sha256"] == "a" * 64
