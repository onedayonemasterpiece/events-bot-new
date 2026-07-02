from __future__ import annotations

import sqlite3

import importlib.util
import subprocess
import sys
from datetime import date
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "inspect" / "audit_public_exhibition_duplicates.py"
_SPEC = importlib.util.spec_from_file_location("audit_public_exhibition_duplicates", _SCRIPT)
assert _SPEC and _SPEC.loader
_monitor = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _monitor
_SPEC.loader.exec_module(_monitor)
find_high_confidence_duplicates = _monitor.find_high_confidence_duplicates
load_public_exhibitions = _monitor.load_public_exhibitions


def _init(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        create table event(
            id integer primary key,
            title text not null,
            date text not null,
            end_date text,
            location_name text,
            city text,
            event_type text,
            source_post_url text,
            source_vk_post_url text,
            ticket_link text,
            lifecycle_status text default 'active',
            identity_status text default 'canonical',
            merged_into_event_id integer
        );
        """
    )


def _insert(conn: sqlite3.Connection, *values) -> None:
    conn.execute(
        """
        insert into event(id,title,date,end_date,location_name,city,event_type,lifecycle_status,identity_status,merged_into_event_id)
        values(?,?,?,?,?,?,?,?,?,?)
        """,
        values,
    )


def test_monitor_finds_overlapping_canonical_exhibition_duplicates(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _insert(conn, 1, "Розовый натюрморт", "2026-07-01", "2026-08-01", "Музей", "Калининград", "выставка", "active", "canonical", None)
    _insert(conn, 2, "Розовый натюрморт", "2026-07-02", "2026-08-02", "Музей", "Калининград", "выставка", "active", "canonical", None)
    _insert(conn, 3, "Другая выставка", "2026-07-02", "2026-08-02", "Другая галерея", "Калининград", "выставка", "active", "canonical", None)
    conn.commit()

    rows = load_public_exhibitions(conn, date(2026, 7, 2))
    pairs = find_high_confidence_duplicates(rows)

    assert [(p.left_id, p.right_id) for p in pairs] == [(1, 2)]
    assert pairs[0].confidence >= 0.9


def test_monitor_ignores_merged_review_and_distinct_recurring_rows(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _insert(conn, 1, "Точка и линия", "2026-07-01", "2026-08-01", "Музей", "Калининград", "выставка", "active", "canonical", None)
    _insert(conn, 2, "Точка и линия", "2026-07-02", "2026-08-02", "Музей", "Калининград", "выставка", "active", "merged", 1)
    _insert(conn, 3, "Стендап: Гассан Джабер", "2026-07-10", None, "Клуб", "Калининград", "стендап", "active", "canonical", None)
    _insert(conn, 4, "Стендап: Гассан Джабер", "2026-07-20", None, "Клуб", "Калининград", "стендап", "active", "canonical", None)
    conn.commit()

    rows = load_public_exhibitions(conn, date(2026, 7, 2))
    pairs = find_high_confidence_duplicates(rows)

    assert pairs == []


def test_monitor_cli_outputs_prometheus_and_exit_code(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _insert(conn, 1, "Розовый натюрморт", "2026-07-01", "2026-08-01", "Музей", "Калининград", "выставка", "active", "canonical", None)
    _insert(conn, 2, "Розовый натюрморт", "2026-07-02", "2026-08-02", "Музей", "Калининград", "выставка", "active", "canonical", None)
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
            "--fail-on-high-confidence",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert 'events_public_exhibition_duplicate_pairs_since_total{confidence="high",window_days="14"} 1' in result.stdout


def test_monitor_since_window_uses_added_at_when_available(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        create table event(
            id integer primary key,
            title text not null,
            date text not null,
            end_date text,
            location_name text,
            city text,
            event_type text,
            source_post_url text,
            source_vk_post_url text,
            ticket_link text,
            lifecycle_status text default 'active',
            identity_status text default 'canonical',
            merged_into_event_id integer,
            added_at text
        );
        """
    )
    conn.execute(
        """insert into event(id,title,date,end_date,location_name,city,event_type,lifecycle_status,identity_status,merged_into_event_id,added_at)
           values(1,'Старая выставка','2026-07-01','2026-08-01','Музей','Калининград','выставка','active','canonical',NULL,'2026-06-01 10:00:00')"""
    )
    conn.execute(
        """insert into event(id,title,date,end_date,location_name,city,event_type,lifecycle_status,identity_status,merged_into_event_id,added_at)
           values(2,'Старая выставка','2026-07-02','2026-08-02','Музей','Калининград','выставка','active','canonical',NULL,'2026-06-02 10:00:00')"""
    )
    conn.execute(
        """insert into event(id,title,date,end_date,location_name,city,event_type,lifecycle_status,identity_status,merged_into_event_id,added_at)
           values(3,'Новая выставка','2026-07-02','2026-08-02','Галерея','Калининград','выставка','active','canonical',NULL,'2026-07-01 10:00:00')"""
    )
    conn.execute(
        """insert into event(id,title,date,end_date,location_name,city,event_type,lifecycle_status,identity_status,merged_into_event_id,added_at)
           values(4,'Новая выставка','2026-07-03','2026-08-03','Галерея','Калининград','выставка','active','canonical',NULL,'2026-07-02 10:00:00')"""
    )
    conn.commit()
    conn.close()

    payload = _monitor.build_audit_payload(db_path, current=date(2026, 7, 2), since_days=14)

    assert payload["high_confidence_duplicate_total_count"] == 2
    assert payload["high_confidence_duplicate_count"] == 1
    assert [(p["left_id"], p["right_id"]) for p in payload["duplicates"]] == [(3, 4)]


def test_monitor_since_date_overrides_since_days(tmp_path):
    db_path = tmp_path / "events.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        create table event(
            id integer primary key,
            title text not null,
            date text not null,
            end_date text,
            location_name text,
            city text,
            event_type text,
            lifecycle_status text default 'active',
            identity_status text default 'canonical',
            merged_into_event_id integer,
            added_at text
        );
        """
    )
    conn.execute("insert into event values(1,'Розовый натюрморт','2026-07-01','2026-08-01','Музей','Калининград','выставка','active','canonical',NULL,'2026-07-01 10:00:00')")
    conn.execute("insert into event values(2,'Розовый натюрморт','2026-07-02','2026-08-02','Музей','Калининград','выставка','active','canonical',NULL,'2026-07-01 11:00:00')")
    conn.commit()
    conn.close()

    payload = _monitor.build_audit_payload(
        db_path,
        current=date(2026, 7, 2),
        since_days=14,
        since_date=date(2026, 7, 2),
    )

    assert payload["high_confidence_duplicate_total_count"] == 1
    assert payload["high_confidence_duplicate_count"] == 0
