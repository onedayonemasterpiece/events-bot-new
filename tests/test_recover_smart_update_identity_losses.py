from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "recover_smart_update_identity_losses.py"
SPEC = importlib.util.spec_from_file_location("recover_smart_update_identity_losses", SCRIPT)
assert SPEC and SPEC.loader
recovery = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = recovery
SPEC.loader.exec_module(recovery)


NOW = "2026-08-10T12:00:00+00:00"


def _make_legacy_db(path: Path, *, with_durable: bool = False) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE vk_inbox (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            created_at TEXT,
            locked_by INTEGER,
            locked_at TEXT,
            review_batch TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            imported_event_id INTEGER,
            last_error TEXT,
            last_result_json TEXT
        );
        """
    )
    con.executemany(
        "INSERT INTO vk_inbox VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            (1, "failed", "2026-08-05 01:00:00", None, None, None, 3, None, "source_binding_conflict", None),
            (2, "deferred", "2026-08-06 01:00:00", None, "2026-08-10 11:00:00", "auto:old", 2, 7001, None, None),
            (3, "deferred", "2026-08-06 01:00:00", None, "2026-08-10 13:00:00", "auto:new", 1, None, None, None),
            (4, "rejected", "2026-08-06 01:00:00", None, None, None, 0, None, None, '{"terminal":"REJECTED_PRODUCT_POLICY"}'),
            (5, "imported", "2026-08-06 01:00:00", None, None, None, 0, 7002, None, None),
            (6, "failed", "2026-08-06 01:00:00", None, None, None, 1, None, "REJECTED_PRODUCT_POLICY", None),
            (7, "failed", "2026-08-03 23:59:59", None, None, None, 1, None, "source_binding_conflict", None),
        ],
    )
    if with_durable:
        con.executescript(
            """
            CREATE TABLE smart_update_candidate_state (
                id INTEGER PRIMARY KEY,
                current_outcome TEXT NOT NULL,
                created_at TEXT NOT NULL,
                next_retry_at TEXT,
                claimed_by TEXT,
                claim_expires_at TEXT,
                updated_at TEXT
            );
            """
        )
        con.executemany(
            "INSERT INTO smart_update_candidate_state VALUES (?,?,?,?,?,?,?)",
            [
                (10, "RETRY_SCHEDULED", "2026-08-05 00:00:00", "2026-08-10 10:00:00", "dead-worker", "2026-08-10 11:00:00", None),
                (11, "RETRY_SCHEDULED", "2026-08-05 00:00:00", "2026-08-10 10:00:00", None, None, None),
                (12, "RETRY_SCHEDULED", "2026-08-05 00:00:00", "2026-08-10 13:00:00", None, None, None),
                (13, "REJECTED_PRODUCT_POLICY", "2026-08-05 00:00:00", None, None, None, None),
                (14, "RETRY_SCHEDULED", "2026-08-03 00:00:00", "2026-08-10 10:00:00", "old", "2026-08-10 11:00:00", None),
                (15, "RETRY_SCHEDULED", "2026-08-05 00:00:00", "2026-08-10 10:00:00", "active", "2026-08-10 13:00:00", None),
            ],
        )
    con.commit()
    con.close()


def _status_map(path: Path) -> dict[int, tuple[object, ...]]:
    con = sqlite3.connect(path)
    rows = con.execute(
        "SELECT id,status,locked_at,review_batch,attempts,imported_event_id FROM vk_inbox ORDER BY id"
    ).fetchall()
    con.close()
    return {int(row[0]): tuple(row[1:]) for row in rows}


def _add_all_source_surfaces(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE telegram_scanned_message (
            source_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            processed_at TEXT NOT NULL,
            status TEXT NOT NULL,
            events_extracted INTEGER NOT NULL DEFAULT 0,
            events_imported INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            PRIMARY KEY(source_id,message_id)
        );
        CREATE TABLE telegram_source_force_message (
            source_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            created_at TEXT,
            PRIMARY KEY(source_id,message_id)
        );
        CREATE TABLE ticket_site_queue (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_result_json TEXT,
            created_at TEXT,
            updated_at TEXT,
            next_run_at TEXT
        );
        CREATE TABLE festival_queue (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            result_json TEXT,
            created_at TEXT,
            updated_at TEXT,
            next_run_at TEXT
        );
        CREATE TABLE ops_run (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT,
            started_at TEXT NOT NULL,
            details_json TEXT
        );
        CREATE TABLE source_parser_recovery_request (
            source_type TEXT PRIMARY KEY,
            requested_since TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_run_at TEXT NOT NULL,
            last_error TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """
    )
    con.executemany(
        "INSERT INTO telegram_scanned_message VALUES (?,?,?,?,?,?,?)",
        [
            (1, 10, "2026-08-05 01:00:00", "skipped", 3, 0, '{"skip_breakdown":{"skipped_identity_gate:location_conflict":1}}'),
            (1, 11, "2026-08-05 01:05:00", "partial", 5, 1, '{"skip_breakdown":{"skipped_identity_gate:merge_identity_blocking_conflict":1}}'),
            (1, 12, "2026-08-05 01:10:00", "error", 1, 0, "REJECTED_PRODUCT_POLICY:past_event"),
            (1, 13, "2026-08-05 01:15:00", "done", 1, 1, None),
            (1, 14, "2026-08-05 01:20:00", "retry_scheduled", 2, 0, None),
            (1, 15, "2026-08-05 01:25:00", "partial", 2, 0, '{"skip_breakdown":{"past_event":2}}'),
        ],
    )
    con.execute(
        "INSERT INTO telegram_source_force_message VALUES (?,?,?)",
        (1, 14, "2026-08-06 00:00:00"),
    )
    con.executemany(
        "INSERT INTO ticket_site_queue VALUES (?,?,?,?,?,?,?,?)",
        [
            (20, "error", 3, "database locked", '{}', "2026-08-05", "2026-08-06", "2026-08-12"),
            (21, "error", 1, "REJECTED_PRODUCT_POLICY", '{}', "2026-08-05", "2026-08-06", "2026-08-12"),
            (22, "active", 0, None, '{}', "2026-08-05", "2026-08-06", "2026-08-12"),
        ],
    )
    con.executemany(
        "INSERT INTO festival_queue VALUES (?,?,?,?,?,?,?,?)",
        [
            (30, "error", 2, "provider timeout", '{}', "2026-08-05", "2026-08-06", "2026-08-12"),
            (31, "review", 1, None, '{"reason":"policy"}', "2026-08-05", "2026-08-06", "2026-08-12"),
            (32, "pending", 0, None, '{}', "2026-08-05", "2026-08-06", "2026-08-12"),
        ],
    )
    con.execute(
        "INSERT INTO ops_run VALUES (?,?,?,?,?)",
        (
            40,
            "parse",
            "partial",
            "2026-08-06 12:00:00",
            json.dumps(
                {
                    "sources": {
                        "dramteatr": {"failed": 4, "retry_scheduled": 2},
                        "muzteatr": {"failed": 3, "retry_scheduled": 0},
                        "philharmonia": {"failed": 0, "retry_scheduled": 0},
                    }
                }
            ),
        ),
    )
    con.commit()
    con.close()


def test_dry_run_is_read_only_and_excludes_product_rejects(tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite"
    _make_legacy_db(db)
    before = db.read_bytes()

    result = recovery.run(
        db,
        since="2026-08-04",
        dry_run=True,
        batch_size=100,
        now=NOW,
    )

    assert db.read_bytes() == before
    assert result["mode"] == "dry-run"
    assert result["changed"] is False
    assert result["legacy_vk"] == {
        "eligible": 2,
        "selected": 2,
        "would_requeue": 2,
        "requeued": 0,
        "selected_with_existing_imports": 1,
        "excluded_product_policy": 1,
        "not_due": 1,
        "schema_supported": True,
    }
    assert result["aggregate"]["would_change"] == 2


def test_apply_is_idempotent_and_never_mutates_imported_or_rejected(tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite"
    _make_legacy_db(db)

    first = recovery.run(db, since="2026-08-04", dry_run=False, batch_size=100, now=NOW)
    first_rows = _status_map(db)
    second = recovery.run(db, since="2026-08-04", dry_run=False, batch_size=100, now=NOW)

    assert first["status"] == "applied"
    assert first["aggregate"]["changed"] == 2
    assert first_rows[1] == ("pending", None, None, 0, None)
    assert first_rows[2] == ("pending", None, None, 0, 7001)
    assert first_rows[4][0] == "rejected"
    assert first_rows[5][0] == "imported"
    assert first_rows[6][0] == "failed"
    assert second["status"] == "noop"
    assert second["aggregate"]["changed"] == 0
    assert _status_map(db) == first_rows


def test_durable_due_retry_claim_recovery_is_feature_detected_and_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite"
    _make_legacy_db(db, with_durable=True)

    first = recovery.run(db, since="2026-08-04", dry_run=False, batch_size=100, now=NOW)
    con = sqlite3.connect(db)
    claims = dict(con.execute("SELECT id,claimed_by FROM smart_update_candidate_state"))
    con.close()
    second = recovery.run(db, since="2026-08-04", dry_run=False, batch_size=100, now=NOW)

    assert first["durable_candidates"] == {
        "eligible_due": 3,
        "selected": 3,
        "would_requeue": 1,
        "requeued": 1,
        "already_available": 1,
        "active_claims_skipped": 1,
        "schema_supported": True,
    }
    assert claims[10] is None
    assert claims[15] == "active"
    assert second["durable_candidates"]["requeued"] == 0
    assert second["durable_candidates"]["already_available"] == 2


def test_all_source_recovery_rearms_telegram_and_technical_queues_idempotently(
    tmp_path: Path,
) -> None:
    db = tmp_path / "events.sqlite"
    _make_legacy_db(db)
    _add_all_source_surfaces(db)

    dry = recovery.run(db, since="2026-08-04", dry_run=True, batch_size=100, now=NOW)
    assert dry["legacy_telegram"] == {
        "eligible": 3,
        "selected": 3,
        "would_requeue": 2,
        "requeued": 0,
        "already_available": 1,
        "selected_with_existing_imports": 1,
        "excluded_product_policy": 1,
        "schema_supported": True,
    }
    assert dry["legacy_ticket_queue"]["would_requeue"] == 1
    assert dry["legacy_ticket_queue"]["already_available"] == 1
    assert dry["legacy_ticket_queue"]["excluded_product_policy"] == 1
    assert dry["legacy_festival_queue"]["would_requeue"] == 1
    assert dry["legacy_festival_queue"]["already_available"] == 1
    assert dry["source_parser"] == {
        "runs_seen": 1,
        "failed_items_observed": 7,
        "retry_items_observed": 2,
        "affected_sources": ["dramteatr", "muzteatr"],
        "eligible": 2,
        "selected": 2,
        "would_requeue": 2,
        "requeued": 0,
        "already_available": 0,
        "recovery_mode": "queued_full_catalog_refresh",
        "queue_schema_supported": True,
        "deployment_required": False,
        "schema_supported": True,
    }

    first = recovery.run(db, since="2026-08-04", dry_run=False, batch_size=100, now=NOW)
    second = recovery.run(db, since="2026-08-04", dry_run=False, batch_size=100, now=NOW)
    assert first["aggregate"]["changed"] == 8  # 2 Telegram + 2 parser + 2 queues + 2 VK
    assert second["aggregate"]["changed"] == 0
    assert second["legacy_telegram"]["already_available"] == 3
    assert second["source_parser"]["already_available"] == 2

    con = sqlite3.connect(db)
    force_rows = con.execute(
        "SELECT source_id,message_id FROM telegram_source_force_message ORDER BY message_id"
    ).fetchall()
    ticket = dict(con.execute("SELECT id,status FROM ticket_site_queue"))
    festival = dict(con.execute("SELECT id,status FROM festival_queue"))
    parser_requests = dict(
        con.execute("SELECT source_type,status FROM source_parser_recovery_request")
    )
    rejected_tg = con.execute(
        "SELECT status FROM telegram_scanned_message WHERE message_id=12"
    ).fetchone()[0]
    con.close()
    assert force_rows == [(1, 10), (1, 11), (1, 14)]
    assert ticket == {20: "active", 21: "error", 22: "active"}
    assert festival == {30: "pending", 31: "review", 32: "pending"}
    assert parser_requests == {"dramteatr": "pending", "muzteatr": "pending"}
    assert rejected_tg == "error"


def test_batch_size_is_shared_between_durable_and_legacy_sources(tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite"
    _make_legacy_db(db, with_durable=True)

    result = recovery.run(db, since="2026-08-04", dry_run=True, batch_size=2, now=NOW)

    assert result["durable_candidates"]["selected"] == 2
    assert result["legacy_vk"]["selected"] == 0
    assert result["aggregate"]["selected"] == 2


@pytest.mark.parametrize("value", ["", "not-a-date", "2026-99-88"])
def test_since_validation(value: str, tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite"
    _make_legacy_db(db)
    with pytest.raises(recovery.RecoveryError, match="invalid_since"):
        recovery.run(db, since=value, dry_run=True, batch_size=10, now=NOW)


def test_main_defaults_to_dry_run_and_emits_only_aggregate_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "events.sqlite"
    _make_legacy_db(db)

    exit_code = recovery.main(["--db", str(db), "--since", "2026-08-04", "--batch-size", "1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "dry-run"
    assert payload["aggregate"]["selected"] == 1
    assert "rows" not in payload
    assert _status_map(db)[1][0] == "failed"
