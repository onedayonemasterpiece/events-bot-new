from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "ops" / "smart_update_prod_audit.py"
SPEC = importlib.util.spec_from_file_location("smart_update_prod_audit", MODULE_PATH)
assert SPEC and SPEC.loader
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


def _window():
    return (
        datetime(2026, 8, 4, 5, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 4, 6, 0, tzinfo=timezone.utc),
    )


def test_database_uses_schema_adaptive_read_only_metrics(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    con = sqlite3.connect(db_path)
    con.executescript(
        """
        CREATE TABLE event_source(id INTEGER PRIMARY KEY,event_id INTEGER,source_type TEXT,source_url TEXT,imported_at TEXT);
        CREATE TABLE event_source_fact(id INTEGER PRIMARY KEY,event_id INTEGER,source_id INTEGER,status TEXT);
        CREATE TABLE event_identity_decision_log(id INTEGER PRIMARY KEY,event_id INTEGER,source_type TEXT,source_url TEXT,decision TEXT,decision_reason TEXT,confidence REAL,decision_payload TEXT,created_at TEXT);
        CREATE TABLE event(id INTEGER PRIMARY KEY,age_assessment_status TEXT,collection_decisions TEXT);
        CREATE TABLE joboutbox(event_id INTEGER,task TEXT,status TEXT,attempts INTEGER,last_error TEXT,updated_at TEXT,next_run_at TEXT);
        CREATE TABLE static_site_build_state(release_channel TEXT,schema_version TEXT,last_success_at TEXT,active_job_id INTEGER,active_run_id TEXT,active_claimed_at TEXT,updated_at TEXT);
        CREATE TABLE kaggle_run_ledger(status TEXT,created_at TEXT);
        """
    )
    con.executemany(
        "INSERT INTO event_source VALUES(?,?,?,?,?)",
        [
            (1, 10, "telegram", "https://example.invalid/private-a", "2026-08-04 05:10:00"),
            (2, 11, "vk", "https://example.invalid/private-b", "2026-08-04T05:20:00Z"),
            (3, 11, "parser", "https://example.invalid/old", "2026-08-03 01:00:00"),
        ],
    )
    con.execute("INSERT INTO event_source_fact VALUES(1,10,1,'added')")
    con.execute(
        "INSERT INTO event_identity_decision_log VALUES(1,11,'vk','https://example.invalid/private-b','merge','ambiguous candidate',0.5,?,?)",
        (json.dumps({"changed_fields": ["title", "creator_id"]}), "2026-08-04 05:25:00"),
    )
    con.executemany("INSERT INTO event VALUES(?,?,?)", [(10, "pending", "{}"), (11, "done", "{}")])
    con.execute("INSERT INTO joboutbox VALUES(10,'telegraph_build','pending',2,'token=very-secret-value','2026-08-04 05:00:00','2026-08-04 05:01:00')")
    con.execute("INSERT INTO static_site_build_state VALUES('secret','v1','2026-08-04 04:00:00',NULL,NULL,NULL,'2026-08-04 05:00:00')")
    con.execute("INSERT INTO kaggle_run_ledger VALUES('done','2026-08-04 05:30:00')")
    con.commit()
    con.close()

    gaps: list[dict[str, str]] = []
    start, end = _window()
    metrics, samples, manifest, queries, available = AUDIT.collect_database(
        start, end, gaps, db_uri=f"file:{db_path}?mode=ro"
    )

    assert available is True
    assert metrics["quick_check"] == ["ok"]
    assert metrics["event_source"]["imports_by_source_type"] == {"telegram": 1, "vk": 1}
    assert metrics["event_source"]["unique_touched_event_id"] == 2
    assert metrics["event_source"]["create_vs_merge_existing_by_first_imported_at"] == {
        "create": 1,
        "merge_existing": 1,
    }
    assert metrics["identity"]["ambiguous_auto_merge_evidence"] == 1
    assert samples[11]["changed_fields"] == {"title"}
    assert "very-secret-value" not in json.dumps(metrics)
    assert manifest["uri"] == AUDIT.DB_URI
    assert any("PRAGMA quick_check" in item["statement"] for item in queries)


def test_runtime_log_window_is_exact_and_excerpts_are_synthesized(tmp_path: Path, monkeypatch) -> None:
    log = tmp_path / "events-bot.log"
    log.write_text(
        "2026-08-04 04:59:59 INFO smart_update.created event_id=1 source_url=https://t.me/private/1\n"
        "2026-08-04 05:10:00 INFO smart_update.start event_id=10 source_type=telegram source_url=https://t.me/private/2 title=Private Person\n"
        "2026-08-04 05:20:00 INFO smart_update.merge event_id=10 status=skipped_nochange updated_keys=title run_id=private-run-id\n"
        "2026-08-04 06:00:00 INFO smart_update.created event_id=12\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("RUNTIME_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("RUNTIME_LOG_BASENAME", "events-bot.log")
    gaps: list[dict[str, str]] = []
    start, end = _window()
    metrics, excerpts, available = AUDIT.collect_runtime_logs(start, end, gaps)
    assert available is True
    assert metrics["smart_update"]["starts"] == 1
    assert metrics["smart_update"]["terminal_outcomes"]["no_op"] == 1
    assert metrics["smart_update"]["terminal_outcomes"]["created"] == 0
    assert "https://" not in excerpts
    assert "Private Person" not in excerpts
    assert "private-run-id" not in excerpts
    assert "corr_" in excerpts


def test_bundle_contract_and_manifest_hashes() -> None:
    sha = "a" * 40
    args = argparse.Namespace(hours=1, end_utc="2026-08-04T06:00:00Z", expected_repo_sha=sha, public_health_status="ok")
    db_manifest = {"schema_hash": "b" * 64, "table_inventory": [], "uri": AUDIT.DB_URI, "query_only": True, "consistent_read_transaction": True, "quick_check": ["ok"]}
    with (
        mock.patch.object(AUDIT, "deployed_identity", return_value=({"matches_expected": True}, sha, True)),
        mock.patch.object(AUDIT, "internal_health", return_value=({"http_status": 200, "ready": True}, True)),
        mock.patch.object(AUDIT, "command_capacity", return_value={}),
        mock.patch.object(AUDIT, "collect_database", return_value=({"quick_check": ["ok"], "identity": {}, "joboutbox": {"stale": []}}, {}, db_manifest, [], True)),
        mock.patch.object(AUDIT, "collect_runtime_logs", return_value=({"smart_update": {"timeouts": 0}, "warm_replay_event_ids": []}, "", True)),
        mock.patch.object(AUDIT, "collect_limiter", return_value=({"available": True, "unfinished_reservations": 0, "active_cooldowns": []}, True, [])),
    ):
        envelope = AUDIT.build_bundle(args)

    assert set(envelope) == {"classification", "exit_code", "files"}
    assert set(envelope["files"]) == set(AUDIT.EVIDENCE_FILES)
    assert envelope["classification"] == "PASS"
    manifest = json.loads(envelope["files"]["manifest.json"])
    assert manifest["repo_sha"] == sha
    assert manifest["in_container_sha"] == sha
    assert manifest["evidence_policy"] == "restricted"
    assert set(manifest["artifact_sha256"]) == set(AUDIT.EVIDENCE_FILES) - {"manifest.json"}
    for name, expected in manifest["artifact_sha256"].items():
        assert hashlib.sha256(envelope["files"][name].encode()).hexdigest() == expected
    qa = json.loads(envelope["files"]["qa-summary.json"])
    assert qa["observer_access"] == {
        "runtime_logs": True,
        "database": True,
        "limiter_ledger": True,
        "exact_deployed_sha": True,
    }
    assert json.loads(envelope["files"]["redaction-audit.json"])["passed"] is True


def test_any_missing_observer_forces_blocked() -> None:
    sha = "a" * 40
    args = argparse.Namespace(hours=1, end_utc="2026-08-04T06:00:00Z", expected_repo_sha=sha, public_health_status="ok")
    with (
        mock.patch.object(AUDIT, "deployed_identity", return_value=({}, sha, True)),
        mock.patch.object(AUDIT, "internal_health", return_value=({"http_status": 200}, True)),
        mock.patch.object(AUDIT, "command_capacity", return_value={}),
        mock.patch.object(AUDIT, "collect_database", return_value=({}, {}, {}, [], False)),
        mock.patch.object(AUDIT, "collect_runtime_logs", return_value=({}, "", True)),
        mock.patch.object(AUDIT, "collect_limiter", return_value=({}, True, [])),
    ):
        envelope = AUDIT.build_bundle(args)
    assert envelope["classification"] == "BLOCKED_OBSERVER_ACCESS"
    assert envelope["exit_code"] == 3
