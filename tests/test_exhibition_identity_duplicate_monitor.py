from __future__ import annotations

import sqlite3

import importlib.util
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "inspect" / "audit_public_exhibition_duplicates.py"
_SPEC = importlib.util.spec_from_file_location("audit_public_exhibition_duplicates", _SCRIPT)
assert _SPEC and _SPEC.loader
_monitor = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _monitor
_SPEC.loader.exec_module(_monitor)
find_high_confidence_duplicates = _monitor.find_high_confidence_duplicates
load_public_exhibitions = _monitor.load_public_exhibitions


def _init_review_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        create table event_identity_decision_log(
            id integer primary key,
            event_id integer,
            candidate_event_id integer,
            decision text,
            decision_reason text,
            confidence real,
            decided_by text,
            decision_payload text,
            created_at text
        );
        create table smart_update_candidate_state(
            id integer primary key,
            accepted_event_id integer,
            diagnostic_event_id integer
        );
        """
    )


def _review(
    conn: sqlite3.Connection,
    review_id: int,
    left_id: int,
    right_id: int,
    *,
    decision: str,
    relation: str,
    evidence: list[str],
    conflicts: list[str],
) -> None:
    conn.execute(
        """
        insert into event_identity_decision_log(
            id,event_id,candidate_event_id,decision,decision_reason,confidence,
            decided_by,decision_payload,created_at
        ) values(?,?,?,?,?,0.99,'terra.manual-review',?,'2026-08-23 08:00:00')
        """,
        (
            review_id,
            left_id,
            right_id,
            decision,
            "reviewed_pair",
            __import__("json").dumps(
                {
                    "stage": "manual_pair_review_v1",
                    "action": decision,
                    "relation": relation,
                    "evidence": evidence,
                    "blocking_conflicts": conflicts,
                },
                ensure_ascii=False,
            ),
        ),
    )


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
    assert payload["candidate_pair_count"] == 1
    assert payload["unresolved_count"] == 1
    assert payload["candidate_pair_window_count"] == 0
    assert payload["unresolved_window_count"] == 0


def test_audit_partitions_recall_candidates_by_authoritative_evidence(tmp_path):
    db_path = tmp_path / "reviewed.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _init_review_tables(conn)
    for event_id, title, venue in (
        (10, "Выставка Цветущая форма", "Музей 1"),
        (11, "Выставка Цветущая форма", "Музей 1"),
        (20, "Выставка Точка и линия", "Музей 2"),
        (21, "Экскурсия по выставке Точка и линия", "Музей 2"),
        (30, "Выставка Мой город", "Музей 3"),
        (31, "Выставка Мой город", "Музей 3"),
    ):
        _insert(conn, event_id, title, "2026-08-20", "2026-09-20", venue, "Калининград", "выставка", "active", "canonical", None)
    _review(
        conn, 1, 10, 11,
        decision="CONFIRMED_DUPLICATE", relation="same_event",
        evidence=["same source project, title, venue and date range"], conflicts=[],
    )
    _review(
        conn, 2, 20, 21,
        decision="FINAL_DISTINCT", relation="distinct_event",
        evidence=["source identifies a separately bookable guided tour"],
        conflicts=["event_type: exhibition vs excursion"],
    )
    conn.commit(); conn.close()

    payload = _monitor.build_audit_payload(db_path, current=date(2026, 8, 23), since_days=14)

    assert payload["candidate_pair_count"] == 3
    assert payload["confirmed_duplicate_count"] == 1
    assert payload["keep_distinct_count"] == 1
    assert payload["unresolved_count"] == 1
    assert payload["candidate_pair_count"] == sum(
        payload[key]
        for key in ("confirmed_duplicate_count", "keep_distinct_count", "unresolved_count")
    )


@pytest.mark.parametrize("relation", ["related_but_distinct", "parent_child"])
def test_mixed_component_hard_negative_relations_are_authoritative(
    tmp_path, relation
):
    db_path = tmp_path / f"{relation}.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _init_review_tables(conn)
    _insert(
        conn,
        1,
        "Выставка Приёмная кампания",
        "2026-08-20",
        "2026-09-20",
        "Школа искусств",
        "Калининград",
        "выставка",
        "active",
        "canonical",
        None,
    )
    _insert(
        conn,
        2,
        "Выставка Приёмная кампания отделения",
        "2026-08-20",
        "2026-09-20",
        "Школа искусств",
        "Калининград",
        "выставка",
        "active",
        "canonical",
        None,
    )
    _review(
        conn,
        1,
        1,
        2,
        decision="FINAL_DISTINCT",
        relation=relation,
        evidence=["source-grounded campaign and department roles"],
        conflicts=["scope and separately maintained identity"],
    )
    conn.commit()
    conn.close()

    payload = _monitor.build_audit_payload(
        db_path, current=date(2026, 8, 23), since_days=14
    )

    assert payload["candidate_pair_count"] == 1
    assert payload["keep_distinct_count"] == 1
    assert payload["unresolved_count"] == 0


def test_keep_distinct_requires_grounding_and_linked_ids_are_not_identity_proof(tmp_path):
    db_path = tmp_path / "linked.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        create table event(
            id integer primary key,title text,date text,end_date text,location_name text,
            city text,event_type text,source_post_url text,source_vk_post_url text,
            ticket_link text,lifecycle_status text,identity_status text,
            merged_into_event_id integer,linked_event_ids text
        );
        """
    )
    _init_review_tables(conn)
    for values in (
        (8207, "Выставка Сага о первых", "2026-08-23", "2026-09-01", "Музей", "Калининград", "выставка", "[8208]"),
        (8208, "Выставка Сага о первых", "2026-08-23", "2026-09-01", "Музей", "Калининград", "выставка", "[8207]"),
    ):
        conn.execute(
            "insert into event(id,title,date,end_date,location_name,city,event_type,source_post_url,source_vk_post_url,ticket_link,lifecycle_status,identity_status,merged_into_event_id,linked_event_ids) values(?,?,?,?,?,?,?,NULL,NULL,NULL,'active','canonical',NULL,?)",
            values,
        )
    _review(
        conn, 1, 8207, 8208,
        decision="FINAL_DISTINCT", relation="distinct_occurrence",
        evidence=[], conflicts=["time: 13:00 vs 13:30"],
    )
    conn.commit(); conn.close()

    unresolved = _monitor.build_audit_payload(db_path, current=date(2026, 8, 23))
    assert unresolved["keep_distinct_count"] == 0
    assert unresolved["unresolved_count"] == 1

    conn = sqlite3.connect(db_path)
    conn.execute(
        "update event_identity_decision_log set decision_payload=? where id=1",
        (__import__("json").dumps({
            "stage": "manual_pair_review_v1", "action": "FINAL_DISTINCT",
            "relation": "distinct_occurrence",
            "evidence": ["source states two separately timed admission sessions"],
            "blocking_conflicts": ["time: 13:00 vs 13:30"],
        }),),
    )
    conn.commit(); conn.close()
    reviewed = _monitor.build_audit_payload(db_path, current=date(2026, 8, 23))
    assert reviewed["keep_distinct_count"] == 1
    assert reviewed["unresolved_count"] == 0


def test_final_distinct_correlates_owner_log_to_accepted_candidate_state(tmp_path):
    db_path = tmp_path / "state-correlated.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn)
    _init_review_tables(conn)
    for values in (
        (1, "Выставка одной серии", "2026-08-23", "2026-09-01", "Музей", "Калининград", "выставка", "active", "canonical", None),
        (2, "Выставка одной серии", "2026-08-24", "2026-09-02", "Музей", "Калининград", "выставка", "active", "canonical", None),
    ):
        _insert(conn, *values)
    conn.execute(
        "insert into smart_update_candidate_state(id,accepted_event_id,diagnostic_event_id) values(7,2,NULL)"
    )
    conn.execute(
        "insert into event_identity_decision_log(id,event_id,candidate_event_id,decision,decision_reason,confidence,decided_by,decision_payload,created_at) "
        "values(1,1,NULL,'FINAL_DISTINCT','different dated occurrence',0.99,'smart_update.identity_gate',?,'2026-08-23 08:00:00')",
        (
            __import__("json").dumps(
                {
                    "stage": "final_identity_adjudicator",
                    "action": "FINAL_DISTINCT",
                    "relation": "distinct_occurrence",
                    "evidence": ["source states the separately dated occurrence"],
                    "blocking_conflicts": ["date: 2026-08-23 vs 2026-08-24"],
                    "candidate_state_id": 7,
                }
            ),
        ),
    )
    conn.commit()
    conn.close()

    payload = _monitor.build_audit_payload(
        db_path, current=date(2026, 8, 23), since_days=14
    )
    assert payload["candidate_pair_count"] == 1
    assert payload["keep_distinct_count"] == 1
    assert payload["unresolved_count"] == 0


def test_production_shaped_august_duplicate_reviews_are_confirmed(tmp_path):
    db_path = tmp_path / "august-confirmed.sqlite"
    conn = sqlite3.connect(db_path)
    _init(conn); _init_review_tables(conn)
    for values in (
        (7614, "Выставка Цветущая форма", "2026-08-18", "2026-09-13", "Музей ИЗО", "Калининград", "выставка", "active", "canonical", None),
        (8273, "Выставка Цветущая форма", "2026-08-22", "2026-09-13", "Музей ИЗО", "Калининград", "выставка", "active", "canonical", None),
        (8156, "Выставка Мой город", "2026-08-21", "2026-09-30", "Дом-музей Брахерта", "Светлогорск", "выставка", "active", "canonical", None),
        (8187, "Выставка Мой город", "2026-08-21", "2026-09-29", "Дом-музей Брахерта", "Светлогорск", "выставка", "active", "canonical", None),
    ):
        _insert(conn, *values)
    _review(conn, 1, 7614, 8273, decision="CONFIRMED_DUPLICATE", relation="same_event", evidence=["same Красный круг project, title, venue and 13 September closing"], conflicts=[])
    _review(conn, 2, 8156, 8187, decision="CONFIRMED_DUPLICATE", relation="same_event", evidence=["same Море внутри project, opening and venue; only closing-date drift"], conflicts=[])
    conn.commit(); conn.close()

    payload = _monitor.build_audit_payload(db_path, current=date(2026, 8, 23))
    assert payload["candidate_pair_count"] == 2
    assert payload["confirmed_duplicate_count"] == 2
    assert payload["unresolved_count"] == 0
