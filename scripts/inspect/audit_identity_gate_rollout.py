#!/usr/bin/env python3
"""Summarize Smart Update identity-gate decision-log metrics for rollout windows.

This read-only helper complements the public `/vystavki/` duplicate monitor: the
public monitor proves whether new high-confidence exhibition duplicates reached
public inventory, while this helper shows whether the identity gate actually ran,
vetoed, failed safe, or experienced vector-recall errors during the same window.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class IdentityGateDecision:
    id: int
    event_id: int | None
    candidate_event_id: int | None
    source_type: str | None
    source_url: str | None
    decision: str
    decision_reason: str | None
    confidence: float | None
    decided_by: str | None
    decision_payload: dict[str, Any]
    created_at: str | None


def _env_enabled(raw: str | None, *, default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def identity_gate_env_readiness() -> dict[str, Any]:
    google_key_env = (os.getenv("SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV") or "GOOGLE_API_KEY4").strip() or "GOOGLE_API_KEY4"
    service_role_present = bool(
        (os.getenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY") or "").strip()
        or (os.getenv("PERSONALIZATION_SUPABASE_SECRET_KEY") or "").strip()
    )
    since_days_raw = (os.getenv("EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS") or "14").strip()
    try:
        since_days_value = int(since_days_raw)
    except ValueError:
        since_days_value = 0
    readiness = {
        "smart_update_identity_gate_enforce": (os.getenv("SMART_UPDATE_IDENTITY_GATE") or "").strip().lower() == "enforce",
        "smart_update_identity_vector_recall_enabled": _env_enabled(os.getenv("SMART_UPDATE_IDENTITY_VECTOR_RECALL"), default=True),
        "personalization_supabase_url_present": bool((os.getenv("PERSONALIZATION_SUPABASE_URL") or "").strip()),
        "personalization_supabase_service_role_present": service_role_present,
        "smart_update_identity_google_key_env": google_key_env,
        "smart_update_identity_google_key_present": bool((os.getenv(google_key_env) or "").strip()),
        "exhibition_duplicate_audit_enabled": _env_enabled(os.getenv("ENABLE_EXHIBITION_DUPLICATE_AUDIT"), default=False),
        "exhibition_duplicate_audit_since_days_14": since_days_value == 14,
    }
    readiness["ready"] = all(
        bool(readiness[key])
        for key in (
            "smart_update_identity_gate_enforce",
            "smart_update_identity_vector_recall_enabled",
            "personalization_supabase_url_present",
            "personalization_supabase_service_role_present",
            "smart_update_identity_google_key_present",
            "exhibition_duplicate_audit_enabled",
            "exhibition_duplicate_audit_since_days_14",
        )
    )
    return readiness


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except Exception:
        return set()


def _json_loads(raw: str | bytes | None) -> dict[str, Any]:
    if raw in {None, "", b""}:
        return {}
    try:
        value = json.loads(raw)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _created_at_since_sql(since: date) -> str:
    # SQLite CURRENT_TIMESTAMP is UTC `YYYY-MM-DD HH:MM:SS`; comparing first ten
    # chars keeps this schema-adaptive for both SQLite timestamps and ISO strings.
    return since.isoformat()


def load_identity_gate_decisions(
    conn: sqlite3.Connection,
    *,
    since: date,
) -> list[IdentityGateDecision]:
    cols = _columns(conn, "event_identity_decision_log")
    required = {"id", "decision", "created_at"}
    if not required <= cols:
        return []
    optional = {
        name: (name if name in cols else f"NULL AS {name}")
        for name in (
            "event_id",
            "candidate_event_id",
            "source_type",
            "source_url",
            "decision_reason",
            "confidence",
            "decided_by",
            "decision_payload",
            "created_at",
        )
    }
    sql = f"""
        SELECT id,
               {optional['event_id']}, {optional['candidate_event_id']},
               {optional['source_type']}, {optional['source_url']},
               decision, {optional['decision_reason']}, {optional['confidence']},
               {optional['decided_by']}, {optional['decision_payload']},
               {optional['created_at']}
        FROM event_identity_decision_log
        WHERE substr(COALESCE(created_at, ''), 1, 10) >= ?
        ORDER BY id ASC
    """
    out: list[IdentityGateDecision] = []
    for row in conn.execute(sql, (_created_at_since_sql(since),)):
        out.append(
            IdentityGateDecision(
                id=int(row[0]),
                event_id=int(row[1]) if row[1] is not None else None,
                candidate_event_id=int(row[2]) if row[2] is not None else None,
                source_type=row[3],
                source_url=row[4],
                decision=str(row[5] or ""),
                decision_reason=row[6],
                confidence=float(row[7]) if row[7] is not None else None,
                decided_by=row[8],
                decision_payload=_json_loads(row[9]),
                created_at=row[10],
            )
        )
    return out


def _payload_mode(row: IdentityGateDecision) -> str:
    return str(row.decision_payload.get("mode") or "unknown").strip().lower() or "unknown"


def _payload_vector(row: IdentityGateDecision) -> dict[str, Any]:
    vector = row.decision_payload.get("vector")
    return vector if isinstance(vector, dict) else {}


def _is_fail_safe(row: IdentityGateDecision) -> bool:
    return bool(row.decision_payload.get("fail_safe")) or row.decision_reason == "identity_gate_error"


def _has_vector_error(row: IdentityGateDecision) -> bool:
    vector = _payload_vector(row)
    err = vector.get("error")
    if str(err or "").strip():
        return True
    suppressed = row.decision_payload.get("suppressed_vector_error")
    if isinstance(suppressed, dict):
        return bool(str(suppressed.get("error") or "").strip())
    return False


def _vector_available(row: IdentityGateDecision) -> bool:
    vector = _payload_vector(row)
    return bool(vector.get("available"))


def _reason_counts(rows: Iterable[IdentityGateDecision]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        reason = str(row.decision_reason or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _mode_counts(rows: Iterable[IdentityGateDecision]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        mode = _payload_mode(row)
        counts[mode] = counts.get(mode, 0) + 1
    return dict(sorted(counts.items()))


def build_rollout_payload(
    db_path: Path | str,
    *,
    current: date | None = None,
    since_days: int = 14,
) -> dict[str, Any]:
    current = current or date.today()
    since_days = max(1, int(since_days or 14))
    since = current - timedelta(days=since_days)
    conn = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        rows = load_identity_gate_decisions(conn, since=since)
    finally:
        conn.close()

    veto_rows = [row for row in rows if row.decision == "veto_create"]
    fail_safe_rows = [row for row in rows if _is_fail_safe(row)]
    vector_error_rows = [row for row in rows if _has_vector_error(row)]
    vector_available_rows = [row for row in rows if _vector_available(row)]
    final_probe_rows = [row for row in rows if row.decision_reason == "final_transaction_duplicate_probe"]
    matched_rows = [row for row in rows if row.event_id is not None]

    return {
        "current_date": current.isoformat(),
        "since_days": since_days,
        "since_date": since.isoformat(),
        "identity_gate_decision_count": len(rows),
        "identity_gate_veto_create_count": len(veto_rows),
        "identity_gate_allow_create_count": sum(1 for row in rows if row.decision == "allow_create"),
        "identity_gate_fail_safe_count": len(fail_safe_rows),
        "identity_gate_vector_error_count": len(vector_error_rows),
        "identity_gate_vector_available_count": len(vector_available_rows),
        "identity_gate_final_probe_veto_count": len(final_probe_rows),
        "identity_gate_matched_event_count": len(matched_rows),
        "identity_gate_modes": _mode_counts(rows),
        "identity_gate_reasons": _reason_counts(rows),
        "recent_vetoes": [asdict(row) for row in veto_rows[-20:]],
        "recent_fail_safes": [asdict(row) for row in fail_safe_rows[-20:]],
        "recent_vector_errors": [asdict(row) for row in vector_error_rows[-20:]],
        "env_readiness": identity_gate_env_readiness(),
    }


def prometheus(payload: dict[str, Any]) -> str:
    since_days = payload["since_days"]
    metric_keys = [
        "identity_gate_decision_count",
        "identity_gate_veto_create_count",
        "identity_gate_allow_create_count",
        "identity_gate_fail_safe_count",
        "identity_gate_vector_error_count",
        "identity_gate_vector_available_count",
        "identity_gate_final_probe_veto_count",
        "identity_gate_matched_event_count",
    ]
    lines = [
        f'events_identity_gate_{key.removeprefix("identity_gate_")}_since_total{{window_days="{since_days}"}} {int(payload[key])}'
        for key in metric_keys
    ]
    env_readiness = payload.get("env_readiness") or {}
    for key, value in sorted(env_readiness.items()):
        if key == "smart_update_identity_google_key_env":
            continue
        lines.append(f'events_identity_gate_env_ready{{check="{key}"}} {1 if bool(value) else 0}')
    for mode, count in sorted((payload.get("identity_gate_modes") or {}).items()):
        lines.append(f'events_identity_gate_decisions_by_mode_since_total{{mode="{mode}",window_days="{since_days}"}} {int(count)}')
    for reason, count in sorted((payload.get("identity_gate_reasons") or {}).items()):
        safe_reason = str(reason).replace('"', "'")
        lines.append(f'events_identity_gate_decisions_by_reason_since_total{{reason="{safe_reason}",window_days="{since_days}"}} {int(count)}')
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--current-date", default=date.today().isoformat())
    parser.add_argument("--since-days", type=int, default=14)
    parser.add_argument("--format", choices=("text", "json", "prometheus", "both"), default="text")
    args = parser.parse_args()

    try:
        payload = build_rollout_payload(
            args.db,
            current=date.fromisoformat(args.current_date),
            since_days=args.since_days,
        )
    except Exception as exc:
        print(f"audit_identity_gate_rollout failed: {exc}", file=sys.stderr)
        return 3

    if args.format in {"json", "both"}:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.format in {"prometheus", "both"}:
        print(prometheus(payload), end="")
    if args.format == "text":
        print(
            " ".join(
                [
                    f"identity_gate_decisions={payload['identity_gate_decision_count']}",
                    f"veto_create={payload['identity_gate_veto_create_count']}",
                    f"fail_safe={payload['identity_gate_fail_safe_count']}",
                    f"vector_errors={payload['identity_gate_vector_error_count']}",
                    f"window_days={payload['since_days']}",
                ]
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
