from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
import sqlite3
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "ops" / "smart_update_prod_audit.py"
WORKFLOW_PATH = Path(__file__).parents[1] / ".github" / "workflows" / "smart-update-prod-audit.yml"
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
    con.execute(
        "INSERT INTO event_identity_decision_log VALUES(2,10,'telegram','https://example.invalid/private-a','allow_merge','guarded merge',0.9,?,?)",
        (json.dumps({"blocking_conflicts": ["private-detail"]}), "2026-08-04 05:26:00"),
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
    assert metrics["identity"]["critical_false_merge_evidence"] == 1
    assert samples[11]["changed_fields"] == {"title"}
    assert "very-secret-value" not in json.dumps(metrics)
    assert manifest["uri"] == AUDIT.DB_URI
    assert manifest["temp_store"] == "MEMORY"
    assert any("PRAGMA quick_check" in item["statement"] for item in queries)


def test_runtime_log_window_is_exact_and_excerpts_are_synthesized(tmp_path: Path, monkeypatch) -> None:
    log = tmp_path / "events-bot.log"
    log.write_text(
        "2026-08-04 04:59:59 INFO smart_update.created event_id=1 source_url=https://t.me/private/1\n"
        "2026-08-04 05:10:00 INFO smart_update.start event_id=10 source_type=telegram source_url=https://t.me/private/2 title=Private Person\n"
        "2026-08-04 05:20:00 INFO smart_update.merge event_id=10 status=skipped_nochange updated_keys=title run_id=private-run-id\n"
        "2026-08-04 05:21:00 INFO root: ENQ [E10] new | job_key=telegraph_build:10\n"
        "2026-08-04 05:22:00 INFO root: RUN [E10] start | job_id=50 task=telegraph_build key=telegraph_build:10\n"
        "2026-08-04 05:23:00 INFO root: RUN [E10] done | job_id=50 task=telegraph_build result=changed\n"
        "2026-08-04 05:24:00 INFO source_parsing.handlers: source_parsing: exact ticket slot lookup event_id=10 url=https://example.invalid/private date=2026-08-04 time=19:00\n"
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
    assert metrics["downstream_for_observed_event_ids"] == [
        {"task": "telegraph_build", "state": "enqueue", "count": 1},
        {"task": "telegraph_build", "state": "running", "count": 1},
        {"task": "telegraph_build", "state": "terminal_done", "count": 1},
    ]
    assert metrics["warm_replay_event_ids"] == []
    assert metrics["warm_replay_candidate_event_ids"] == [10]


def test_bundle_contract_and_manifest_hashes() -> None:
    sha = "a" * 40
    args = argparse.Namespace(hours=1, end_utc="2026-08-04T06:00:00Z", expected_repo_sha=sha, public_health_status="ok")
    db_manifest = {"schema_hash": "b" * 64, "table_inventory": [], "uri": AUDIT.DB_URI, "query_only": True, "consistent_read_transaction": True, "quick_check": ["ok"]}
    with (
        mock.patch.object(AUDIT, "deployed_identity", return_value=({"matches_expected": True}, sha, True)),
        mock.patch.object(AUDIT, "internal_health", return_value=({"http_status": 200, "ready": True}, True)),
        mock.patch.object(AUDIT, "command_capacity", return_value={}),
        mock.patch.object(AUDIT, "collect_database", return_value=({"quick_check": ["ok"], "identity": {}, "joboutbox": {"stale": []}}, {}, db_manifest, [], True)),
        mock.patch.object(AUDIT, "collect_runtime_logs", return_value=({"smart_update": {"timeouts": 0, "exact_packet_replay": 1}, "warm_replay_event_ids": []}, "", True)),
        mock.patch.object(AUDIT, "collect_limiter", return_value=({"available": True, "unfinished_reservations": 0, "active_cooldowns": []}, True, [])),
    ):
        envelope = AUDIT.build_bundle(args)

    assert set(envelope) == {"classification", "exit_code", "files"}
    assert set(envelope["files"]) == set(AUDIT.EVIDENCE_FILES)
    assert envelope["classification"] == "PASS"
    manifest = json.loads(envelope["files"]["manifest.json"])
    assert manifest["repo_sha"] == sha
    assert manifest["in_container_sha"] == sha
    assert manifest["window"] == {
        "start_utc": "2026-08-04T05:00:00Z",
        "end_utc": "2026-08-04T06:00:00Z",
        "hours": 1,
    }
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
    assert "http://" not in "\n".join(envelope["files"].values())
    sample_rows = [json.loads(line) for line in envelope["files"]["samples.jsonl"].splitlines()]
    assert any(row["sample_kind"] == "exact_warm_replay" for row in sample_rows)


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


def test_unverified_exact_replay_forces_watch_not_pass() -> None:
    sha = "a" * 40
    args = argparse.Namespace(hours=1, end_utc="2026-08-04T06:00:00Z", expected_repo_sha=sha, public_health_status="ok")
    db_manifest = {"schema_hash": "b" * 64, "table_inventory": [{"name": "event"}], "uri": AUDIT.DB_URI, "query_only": True, "temp_store": "MEMORY", "consistent_read_transaction": True, "quick_check": ["ok"]}
    with (
        mock.patch.object(AUDIT, "deployed_identity", return_value=({"matches_expected": True}, sha, True)),
        mock.patch.object(AUDIT, "internal_health", return_value=({"http_status": 200, "ready": True}, True)),
        mock.patch.object(AUDIT, "command_capacity", return_value={}),
        mock.patch.object(AUDIT, "collect_database", return_value=({"quick_check": ["ok"], "identity": {}, "joboutbox": {"stale": []}}, {}, db_manifest, [{"engine": "sqlite", "statement": "SELECT 1"}], True)),
        mock.patch.object(AUDIT, "collect_runtime_logs", return_value=({"smart_update": {"timeouts": 0, "exact_packet_replay": 0, "exact_packet_replay_candidates": 1}, "warm_replay_event_ids": []}, "", True)),
        mock.patch.object(AUDIT, "collect_limiter", return_value=({"available": True, "unfinished_reservations": 0, "active_cooldowns": []}, True, [{"engine": "postgrest_select"}])),
    ):
        envelope = AUDIT.build_bundle(args)
    assert envelope["classification"] == "WATCH"
    findings = json.loads(envelope["files"]["findings.json"])["findings"]
    assert any(item["code"] == "exact_packet_replay_unverified" for item in findings)


def test_product_sample_without_observed_source_type_uses_unknown() -> None:
    samples = AUDIT.make_samples(
        {42: {"source_types": set(), "source_url_hashes": set(), "changed_fields": {"title"}, "decision": "observed"}},
        {"warm_replay_event_ids": []},
    )
    rows = [json.loads(line) for line in samples.splitlines()]
    assert rows[0]["event_id"] == 42
    assert rows[0]["source_type"] == "unknown"
    assert rows[0]["changed_field_names"] == ["title"]


def test_query_recorder_rejects_mutating_sql() -> None:
    connection = sqlite3.connect(":memory:")
    recorder = AUDIT.QueryRecorder()
    try:
        recorder.execute(connection, "safe", "SELECT 1").fetchall()
        try:
            recorder.execute(connection, "unsafe", "INSERT INTO missing VALUES (1)")
        except ValueError as exc:
            assert str(exc) == "non_read_only_sql_rejected"
        else:  # pragma: no cover - safety invariant
            raise AssertionError("mutating SQL was accepted")
    finally:
        connection.close()


def test_limiter_uses_select_get_shapes_and_reports_attempt_finalization(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_AI_LIMITER_SUPABASE_URL", "https://example.invalid")
    monkeypatch.setenv("GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", "not-emitted-service-key")
    calls = []

    def fake_get(_base_url, _service_key, table, params, *, max_rows=50000):
        calls.append((table, tuple(params), max_rows))
        filters = dict(params)
        if table == "google_ai_requests" and "created_at" in filters:
            return ([{
                "request_uid": "11111111-1111-1111-1111-111111111111",
                "consumer": "smart_update", "model": "gemini", "status": "done",
                "attempts": 2, "finalized_at": "2026-08-04T05:20:00Z",
                "quota_scope": "private-scope", "meta": {}, "created_at": "2026-08-04T05:00:00Z",
            }], False)
        if table == "google_ai_request_attempts" and "started_at" in filters:
            return ([
                {"request_uid": "11111111-1111-1111-1111-111111111111", "attempt_no": 1, "status": "error", "quota_scope": "private-scope", "provider_error_code": "429", "completed_at": "2026-08-04T05:01:00Z"},
                {"request_uid": "11111111-1111-1111-1111-111111111111", "attempt_no": 2, "status": "done", "quota_scope": "private-scope", "completed_at": None},
            ], False)
        if table == "google_ai_provider_cooldowns":
            return ([], False)
        return ([], False)

    monkeypatch.setattr(AUDIT, "postgrest_get", fake_get)
    gaps = []
    start, end = _window()
    metrics, available, queries = AUDIT.collect_limiter(start, end, gaps)
    assert available is True
    assert metrics["attempt_finalization"] == {"finalized": 1, "unfinalized": 1}
    assert metrics["failure_classes"]["provider_429"] == 1
    assert metrics["logical_requests_with_multiple_physical_attempts_in_one_scope"] == 1
    assert metrics["distinct_quota_scopes"] == 1
    assert all(item[0] in {"google_ai_requests", "google_ai_request_attempts", "google_ai_provider_cooldowns"} for item in calls)
    assert all(item["engine"] == "postgrest_select" for item in queries)
    as_of_request = next(dict(params) for table, params, _ in calls if table == "google_ai_requests" and "or" in dict(params))
    as_of_attempt = next(dict(params) for table, params, _ in calls if table == "google_ai_request_attempts" and "or" in dict(params))
    as_of_cooldown = next(dict(params) for table, params, _ in calls if table == "google_ai_provider_cooldowns" and "blocked_until" in dict(params))
    assert as_of_request["created_at"].startswith("lt.") and "finalized_at.gt." in as_of_request["or"]
    assert as_of_attempt["started_at"].startswith("lt.") and "completed_at.gt." in as_of_attempt["or"]
    assert as_of_cooldown["updated_at"].startswith("lt.") and as_of_cooldown["blocked_until"].startswith("gt.")
    assert "private-scope" not in json.dumps(metrics)


def test_workflow_and_auditor_manifest_window_contract_are_locked() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    source = MODULE_PATH.read_text(encoding="utf-8")
    assert 'window = manifest["window"]' in workflow
    assert 'window.get("end_utc") != os.environ.get("REQUEST_END_UTC")' in workflow
    assert 'window.get("hours") != int(os.environ["REQUEST_HOURS"])' in workflow
    assert 'sha_missing = not isinstance(deployed_sha, str) or not re.fullmatch' in workflow
    assert 'deployed_sha != tested_sha and classification != "FAIL"' in workflow
    assert '"window": {"start_utc": utc_iso(start), "end_utc": utc_iso(end), "hours": args.hours}' in source
    assert '"utc_window"' not in source


def test_workflow_security_contract_is_fail_closed() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "on:\n  workflow_dispatch:" in workflow
    assert "\n  push:" not in workflow and "\n  pull_request:" not in workflow and "schedule:" not in workflow
    assert "permissions:\n  contents: read" in workflow
    assert "group: smart-update-prod-audit" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "environment: production-readonly" in workflow
    assert workflow.count("secrets.FLY_API_TOKEN_SMART_UPDATE_AUDIT") == 1
    assert '${GITHUB_REF}" != "refs/heads/main"' in workflow
    assert '"${REQUEST_SHA}" != "${GITHUB_SHA}"' in workflow
    assert "persist-credentials: false" in workflow
    assert "flyctl ssh console" in workflow and "< \"${transport}\"" in workflow
    assert "flyctl deploy" not in workflow and "machine restart" not in workflow.lower() and "sftp" not in workflow.lower()
    assert workflow.index("Check public health without credentials") < workflow.index("secrets.FLY_API_TOKEN_SMART_UPDATE_AUDIT")
    assert workflow.index("Upload restricted sanitized evidence") < workflow.index("Enforce terminal classification")
    assert "if: always() && steps.evidence.outcome == 'success'" in workflow


def test_adversarial_redaction_and_error_signatures_do_not_echo_values() -> None:
    canaries = {
        "secret": "token=very-secret-value-1234567890",
        "jwt": "eyJabcdefghijk.abcdefghijklmnop.abcdefghijk",
        "telegram": "https://t.me/private/123456",
        "email": "private.person@example.invalid",
        "phone": "+7 (999) 111-22-33",
        "payload": '"source_text":"private prose"',
    }
    scan = AUDIT.redaction_scan({"canaries.txt": "\n".join(canaries.values())})
    assert scan["passed"] is False
    assert all(value not in json.dumps(scan) for value in canaries.values())
    signature = AUDIT.safe_error_signature("TimeoutError token=very-secret-value-123 https://t.me/private/1 id=123456")
    assert signature.startswith("TimeoutError:err_")
    assert "secret" not in signature and "https" not in signature and "123456" not in signature


def test_actual_workflow_extractor_accepts_a_real_auditor_envelope(tmp_path: Path, monkeypatch) -> None:
    sha = "a" * 40
    args = argparse.Namespace(hours=1, end_utc="2026-08-04T06:00:00Z", expected_repo_sha=sha, public_health_status="200")
    db_manifest = {
        "schema_hash": "b" * 64,
        "table_inventory": [{"name": "event", "columns": []}],
        "uri": AUDIT.DB_URI,
        "query_only": True,
        "temp_store": "MEMORY",
        "consistent_read_transaction": True,
        "quick_check": ["ok"],
    }
    with (
        mock.patch.object(AUDIT, "deployed_identity", return_value=({"machine_id": "machine", "machine_version": "1", "image_identity": "image", "matches_expected": True}, sha, True)),
        mock.patch.object(AUDIT, "internal_health", return_value=({"http_status": 200, "ready": True}, True)),
        mock.patch.object(AUDIT, "command_capacity", return_value={"df_data": {"available_1k": 1}}),
        mock.patch.object(AUDIT, "collect_database", return_value=({"quick_check": ["ok"], "identity": {}, "joboutbox": {"stale": []}}, {}, db_manifest, [{"engine": "sqlite", "statement": "SELECT 1"}], True)),
        mock.patch.object(AUDIT, "collect_runtime_logs", return_value=({"smart_update": {"timeouts": 0, "exact_packet_replay": 1}, "warm_replay_event_ids": []}, "", True)),
        mock.patch.object(AUDIT, "collect_limiter", return_value=({"available": True, "unfinished_reservations": 0, "active_cooldowns": []}, True, [{"engine": "postgrest_select", "table": "google_ai_requests"}])),
    ):
        envelope = AUDIT.build_bundle(args)
    assert envelope["classification"] == "PASS"

    raw = "SMART_UPDATE_AUDIT_BUNDLE_V1:" + base64.b64encode(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    ).decode() + "\n"
    (tmp_path / "smart-update-audit-ssh.stdout").write_text(raw, encoding="utf-8")
    (tmp_path / "smart-update-audit-ssh.stderr").write_text("", encoding="utf-8")
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir()
    output_path = tmp_path / "github-output"
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setenv("EVIDENCE_DIR", str(evidence_dir))
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    monkeypatch.setenv("PREFLIGHT_ALLOWED", "true")
    monkeypatch.setenv("PREFLIGHT_REASON", "ok")
    monkeypatch.setenv("REQUEST_HOURS", "1")
    monkeypatch.setenv("REQUEST_END_UTC", "2026-08-04T06:00:00Z")
    monkeypatch.setenv("REQUEST_SHA", sha)
    monkeypatch.setenv("GITHUB_SHA", sha)
    monkeypatch.setenv("PUBLIC_HEALTH_STATUS", "200")
    monkeypatch.setenv("SSH_RC", "0")

    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    section = workflow.split("- name: Extract, sanitize, and verify exact evidence bundle", 1)[1]
    embedded = section.split("python3 - <<'PY'\n", 1)[1].split("\n          PY", 1)[0]
    exec(compile(textwrap.dedent(embedded), "<workflow-evidence-validator>", "exec"), {"__name__": "__main__"})
    assert "classification=PASS" in output_path.read_text(encoding="utf-8")
    assert {path.name for path in evidence_dir.iterdir()} == set(AUDIT.EVIDENCE_FILES)
