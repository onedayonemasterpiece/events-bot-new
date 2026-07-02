from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import date
from pathlib import Path

import importlib.util

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "inspect" / "audit_identity_gate_rollout.py"
_SPEC = importlib.util.spec_from_file_location("audit_identity_gate_rollout", _SCRIPT)
assert _SPEC and _SPEC.loader
_rollout = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _rollout
_SPEC.loader.exec_module(_rollout)
build_rollout_payload = _rollout.build_rollout_payload


def _init(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        create table event_identity_decision_log(
            id integer primary key autoincrement,
            event_id integer,
            candidate_event_id integer,
            source_type text,
            source_url text,
            decision text not null,
            decision_reason text,
            confidence real,
            decided_by text,
            decision_payload json,
            created_at text not null
        );
        """
    )


def _insert(
    conn: sqlite3.Connection,
    *,
    decision: str,
    reason: str,
    payload: dict,
    created_at: str,
    event_id: int | None = None,
) -> None:
    conn.execute(
        """
        insert into event_identity_decision_log(
            event_id, source_type, source_url, decision, decision_reason,
            confidence, decided_by, decision_payload, created_at
        )
        values(?, 'telegram', 'https://t.me/source/1', ?, ?, 0.95,
               'smart_update.identity_gate', ?, ?)
        """,
        (event_id, decision, reason, json.dumps(payload), created_at),
    )


def test_rollout_audit_counts_veto_failsafe_and_vector_errors(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _insert(
        conn,
        decision="veto_create",
        reason="vector_identity_match",
        event_id=10,
        payload={"mode": "enforce", "fail_safe": False, "vector": {"available": True, "score": 0.91}},
        created_at="2026-07-01 10:00:00",
    )
    _insert(
        conn,
        decision="veto_create",
        reason="identity_gate_error",
        payload={"mode": "enforce", "fail_safe": True, "vector": {"available": False, "error": "timeout"}},
        created_at="2026-07-02 10:00:00",
    )
    _insert(
        conn,
        decision="allow_create",
        reason="old_shadow",
        payload={"mode": "shadow", "fail_safe": False},
        created_at="2026-06-01 10:00:00",
    )
    conn.commit()
    conn.close()

    payload = build_rollout_payload(db_path, current=date(2026, 7, 2), since_days=14)

    assert payload["identity_gate_decision_count"] == 2
    assert payload["identity_gate_veto_create_count"] == 2
    assert payload["identity_gate_fail_safe_count"] == 1
    assert payload["identity_gate_vector_error_count"] == 1
    assert payload["identity_gate_vector_available_count"] == 1
    assert payload["identity_gate_matched_event_count"] == 1
    assert payload["identity_gate_modes"] == {"enforce": 2}
    assert payload["identity_gate_reasons"]["vector_identity_match"] == 1


def test_rollout_audit_cli_prometheus(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _insert(
        conn,
        decision="veto_create",
        reason="final_transaction_duplicate_probe",
        event_id=11,
        payload={"mode": "enforce", "fail_safe": False},
        created_at="2026-07-02 10:00:00",
    )
    conn.commit()
    conn.close()

    result = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--db",
            str(db_path),
            "--current-date",
            "2026-07-02",
            "--since-days",
            "14",
            "--format",
            "prometheus",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert 'events_identity_gate_veto_create_count_since_total{window_days="14"} 1' in result.stdout
    assert 'events_identity_gate_final_probe_veto_count_since_total{window_days="14"} 1' in result.stdout


def test_rollout_audit_reports_env_readiness_without_secret_values(tmp_path, monkeypatch):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    conn.commit()
    conn.close()
    monkeypatch.setenv("SMART_UPDATE_IDENTITY_GATE", "enforce")
    monkeypatch.setenv("SMART_UPDATE_IDENTITY_VECTOR_RECALL", "1")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY", "secret-service-role")
    monkeypatch.setenv("SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV", "GOOGLE_API_KEY_TEST")
    monkeypatch.setenv("GOOGLE_API_KEY_TEST", "secret-google")
    monkeypatch.setenv("ENABLE_EXHIBITION_DUPLICATE_AUDIT", "1")
    monkeypatch.setenv("EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS", "14")

    payload = build_rollout_payload(db_path, current=date(2026, 7, 2), since_days=14)
    rendered = _rollout.prometheus(payload)

    assert payload["env_readiness"]["ready"] is True
    assert payload["env_readiness"]["smart_update_identity_google_key_env"] == "GOOGLE_API_KEY_TEST"
    assert "secret-service-role" not in json.dumps(payload)
    assert "secret-google" not in json.dumps(payload)
    assert 'events_identity_gate_env_ready{check="ready"} 1' in rendered


def test_rollout_env_readiness_accepts_existing_supabase_env_fallback(tmp_path, monkeypatch):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    conn.commit()
    conn.close()
    monkeypatch.delenv("PERSONALIZATION_SUPABASE_URL", raising=False)
    monkeypatch.delenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "secret-existing")
    monkeypatch.setenv("SMART_UPDATE_IDENTITY_GATE", "enforce")
    monkeypatch.setenv("GOOGLE_API_KEY4", "secret-google")
    monkeypatch.setenv("ENABLE_EXHIBITION_DUPLICATE_AUDIT", "1")

    payload = build_rollout_payload(db_path, current=date(2026, 7, 2), since_days=14)

    assert payload["env_readiness"]["personalization_supabase_url_present"] is True
    assert payload["env_readiness"]["personalization_supabase_service_role_present"] is True
    assert payload["env_readiness"]["ready"] is True
    assert "secret-existing" not in json.dumps(payload)


def test_rollout_audit_since_date_overrides_since_days(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _insert(
        conn,
        decision="veto_create",
        reason="old_before_enforce",
        payload={"mode": "enforce"},
        created_at="2026-07-01 10:00:00",
    )
    conn.commit()
    conn.close()

    payload = build_rollout_payload(
        db_path,
        current=date(2026, 7, 2),
        since_days=14,
        since_date=date(2026, 7, 2),
    )

    assert payload["identity_gate_decision_count"] == 0
