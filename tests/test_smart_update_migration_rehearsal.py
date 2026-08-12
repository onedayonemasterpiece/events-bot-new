from __future__ import annotations

import importlib.util
from pathlib import Path
import sqlite3
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ops" / "rehearse_smart_update_migration.py"
SPEC = importlib.util.spec_from_file_location("rehearse_smart_update_migration", SCRIPT)
assert SPEC and SPEC.loader
rehearsal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = rehearsal
SPEC.loader.exec_module(rehearsal)


def _snapshot(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE legacy(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO legacy VALUES(1,'preserve-me');
        """
    )
    con.commit()
    con.close()


def test_rehearsal_runs_init_twice_and_never_changes_original(tmp_path: Path) -> None:
    original, clone = tmp_path / "prod.sqlite", tmp_path / "clone.sqlite"
    _snapshot(original)
    before = original.read_bytes()
    calls: list[Path] = []

    def initialize(path: Path) -> None:
        calls.append(path)
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE IF NOT EXISTS migration_added(id INTEGER PRIMARY KEY)")
        con.commit()
        con.close()

    report = rehearsal.run(
        original, clone, since="2026-08-01", until="2026-08-11",
        initializer=initialize,
    )

    assert len(calls) == 2
    assert calls == [clone.resolve(), clone.resolve()]
    assert original.read_bytes() == before
    assert report["status"] == "passed"
    assert report["original"]["byte_unchanged"] is True
    assert report["original"]["query_only_write_rejected"] is True
    assert report["clone"]["database_init_runs"] == 2
    assert report["rollback_rehearsal"] == {
        "legacy_schema_compatible": True,
        "transaction_rollback_probe_absent": True,
        "schema_hash_unchanged": True,
    }
    assert report["census"]["changed_rows"] == 0
    assert report["recovery"]["changed"] is False
    assert report["mutations_confined_to_clone"] is True
    con = sqlite3.connect(original.resolve().as_uri() + "?mode=ro", uri=True)
    con.execute("PRAGMA query_only=ON")
    with pytest.raises(sqlite3.DatabaseError):
        con.execute("INSERT INTO legacy VALUES(2,'forbidden')")
    assert con.execute("SELECT * FROM legacy").fetchall() == [(1, "preserve-me")]
    con.close()


def test_rehearsal_fails_closed_on_identity_conflicts(tmp_path: Path) -> None:
    original, clone = tmp_path / "prod.sqlite", tmp_path / "clone.sqlite"
    con = sqlite3.connect(original)
    con.executescript(
        """
        CREATE TABLE event_source(
          id INTEGER PRIMARY KEY, source_type TEXT, source_url TEXT,
          occurrence_key TEXT
        );
        INSERT INTO event_source VALUES
          (1,'vk','u','same'), (2,'vk','u','same');
        """
    )
    con.commit()
    con.close()
    before = original.read_bytes()

    with pytest.raises(
        rehearsal.RehearsalError, match="migration_conflict_requires_repair_plan"
    ):
        rehearsal.run(
            original, clone, since="2026-08-01", until="2026-08-11",
            initializer=lambda _path: None,
        )

    assert original.read_bytes() == before


def test_rehearsal_reports_unchanged_legacy_fk_orphans_without_calling_them_new(
    tmp_path: Path,
) -> None:
    original, clone = tmp_path / "prod.sqlite", tmp_path / "clone.sqlite"
    con = sqlite3.connect(original)
    con.executescript(
        """
        CREATE TABLE parent(id INTEGER PRIMARY KEY);
        CREATE TABLE child(
          id INTEGER PRIMARY KEY,
          parent_id INTEGER REFERENCES parent(id)
        );
        INSERT INTO child VALUES(1,999);
        """
    )
    con.commit()
    con.close()

    report = rehearsal.run(
        original,
        clone,
        since="2026-08-01",
        until="2026-08-11",
        initializer=lambda _path: None,
    )

    conflicts = report["migration"]["conflicts"]
    assert conflicts["preexisting_foreign_key_violations"] == 1
    assert conflicts["new_foreign_key_violations"] == 0
    plan = report["migration"]["preexisting_foreign_key_repair_plan"]
    assert plan["required"] is True
    assert plan["execution"] == "not_executed;production_writes_forbidden"
    assert plan["by_relation"] == [
        {"table": "child", "parent": "parent", "fkid": 0, "count": 1}
    ]
