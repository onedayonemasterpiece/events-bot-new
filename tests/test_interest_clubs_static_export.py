from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[1]
EXPORTER_PATH = ROOT / "site" / "scripts" / "export-production-preview-data.py"


def _load_exporter():
    spec = importlib.util.spec_from_file_location("static_preview_exporter", EXPORTER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _club_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        create table interest_club (
          id integer primary key,
          slug text not null,
          canonical_name text not null,
          topic text not null,
          description text,
          city text,
          typical_place text,
          public_status text not null,
          identity_version integer,
          policy_version text,
          aliases_json text,
          source_anchors_json text,
          provenance_json text,
          created_at text,
          updated_at text
        );
        create table interest_club_event (
          id integer primary key,
          club_id integer not null,
          event_id integer not null,
          status text not null,
          decision_lane text,
          evidence_quote text,
          evidence_json text,
          model text,
          policy_version text,
          input_hash text,
          evaluated_at text,
          updated_at text,
          unique(club_id, event_id)
        );
        create table interest_club_evaluation (
          id integer primary key,
          club_id integer not null,
          event_id integer not null,
          status text not null,
          verdict text not null,
          decision_lane text,
          evidence_quote text,
          evidence_json text,
          model text,
          policy_version text not null,
          input_hash text not null,
          error_code text,
          attempts integer,
          created_at text,
          updated_at text,
          unique(club_id,event_id,policy_version,input_hash)
        );
        create table event (
          id integer primary key,
          title text not null,
          date text not null,
          time text,
          city text,
          location_name text,
          lifecycle_status text default 'active',
          identity_status text default 'canonical',
          merged_into_event_id integer,
          silent integer default 0,
          festival text
        );
        create table event_source (id integer primary key, event_id integer, source_url text);
        """
    )
    con.executemany(
        "insert into interest_club(id,slug,canonical_name,topic,description,city,typical_place,public_status,updated_at) values(?,?,?,?,?,?,?,?,?)",
        [
            (1, "chess-club", "Шахматный клуб", "Шахматы", "Открытые встречи.", "Калининград", "Библиотека", "approved", "2026-07-17T08:00:00Z"),
            (2, "shadow-club", "Теневой кандидат", "Настольные игры", None, "Калининград", None, "shadow", None),
            (3, "bad slug", "Некорректный URL", "Игры", None, None, None, "approved", None),
            (4, "one-date-club", "Одна дата", "Игры", None, None, None, "approved", None),
            (5, "stale-club", "Неактивный клуб", "Игры", None, None, None, "approved", None),
        ],
    )
    con.executemany(
        "insert into event(id,title,date,time,city,location_name,lifecycle_status,identity_status,silent,festival) values(?,?,?,?,?,?,?,?,?,?)",
        [
            (10, "Прошлая встреча", "2026-07-01", "18:00", "Калининград", "Библиотека", "active", "canonical", 0, None),
            (11, "Ближайшая встреча", "2026-07-20", "19:00", "Калининград", "Библиотека", "active", "canonical", 0, None),
            (12, "Отложенная связь", "2026-07-21", None, "Калининград", "Библиотека", "active", "canonical", 0, None),
            (13, "Фестиваль", "2026-07-22", None, "Калининград", "Парк", "active", "canonical", 0, "Летний фестиваль"),
            (14, "Архивное событие", "2026-07-23", None, "Калининград", "Библиотека", "archived", "canonical", 0, None),
            (15, "Теневой клуб", "2026-07-24", None, "Калининград", "Библиотека", "active", "canonical", 0, None),
            (16, "Единственная дата", "2026-07-25", None, "Калининград", "Библиотека", "active", "canonical", 0, None),
            (17, "Старая встреча", "2026-01-01", None, "Калининград", "Библиотека", "active", "canonical", 0, None),
            (18, "Ещё одна старая встреча", "2026-02-01", None, "Калининград", "Библиотека", "active", "canonical", 0, None),
        ],
    )
    con.executemany(
        "insert into interest_club_event(id,club_id,event_id,status,policy_version,input_hash,updated_at) values(?,?,?,?,?,?,?)",
        [
            (1, 1, 10, "active", "p1", "h1", "2026-07-17T08:00:00Z"),
            (2, 1, 11, "active", "p1", "h2", "2026-07-17T08:00:00Z"),
            (3, 1, 12, "deferred", "p1", "h3", "2026-07-17T08:00:00Z"),
            (4, 1, 13, "active", "p1", "h4", "2026-07-17T08:00:00Z"),
            (5, 1, 14, "active", "p1", "h5", "2026-07-17T08:00:00Z"),
            (6, 2, 15, "active", "p1", "h6", "2026-07-17T08:00:00Z"),
            (7, 4, 16, "active", "p1", "h7", "2026-07-17T08:00:00Z"),
            (8, 5, 17, "active", "p1", "h8", "2026-07-17T08:00:00Z"),
            (9, 5, 18, "active", "p1", "h9", "2026-07-17T08:00:00Z"),
        ],
    )
    con.executemany(
        "insert into interest_club_evaluation(id,club_id,event_id,status,verdict,policy_version,input_hash,updated_at) values(?,?,?,?,?,?,?,?)",
        [
            (idx, club_id, event_id, "accepted", "yes", "p1", f"h{idx}", "2026-07-17T08:00:00Z")
            for idx, club_id, event_id in [
                (1, 1, 10), (2, 1, 11), (4, 1, 13), (5, 1, 14),
                (6, 2, 15), (7, 4, 16), (8, 5, 17), (9, 5, 18),
            ]
        ],
    )
    con.execute("insert into event_source(event_id,source_url) values(11,'https://example.test/meeting-11')")
    return con


def test_projection_fail_closes_when_club_tables_are_missing() -> None:
    exporter = _load_exporter()
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    result = exporter.build_interest_clubs_projection(
        con,
        current_date="2026-07-17",
        generated_at="2026-07-17T10:00:00Z",
        exported_events=[],
        enabled=True,
    )

    assert result["schema_version"] == "interest-clubs-static-v1"
    assert result["source"] == "empty-contract-fallback"
    assert result["clubs"] == []


def test_projection_publishes_only_approved_active_grounded_relations() -> None:
    exporter = _load_exporter()
    con = _club_db()

    result = exporter.build_interest_clubs_projection(
        con,
        current_date="2026-07-17",
        generated_at="2026-07-17T10:00:00Z",
        exported_events=[{"id": 11, "slug": "blizhayshaya-vstrecha-kaliningrad-11"}],
        enabled=True,
    )

    assert result["source"] == "sqlite-interest-clubs-v1"
    assert [club["slug"] for club in result["clubs"]] == ["chess-club"]
    club = result["clubs"][0]
    assert club["topic"] == "Шахматы"
    assert club["city"] == "Калининград"
    assert club["typical_venue"] == "Библиотека"
    assert club["activity"] == {
        "meeting_count": 2,
        "distinct_date_count": 2,
        "first_observed_date": "2026-07-01",
        "last_observed_date": "2026-07-20",
        "future_meeting_count": 1,
    }
    assert club["future_meetings"] == [
        {
            "event_id": 11,
            "title": "Ближайшая встреча",
            "start_date": "2026-07-20",
            "start_time": "19:00",
            "display_time": "19:00",
            "city": "Калининград",
            "venue_name": "Библиотека",
            "event_path": "/sobytiya/blizhayshaya-vstrecha-kaliningrad-11/",
            "source_url": "https://example.test/meeting-11",
        }
    ]


def test_projection_is_empty_by_default_until_release_gate_is_enabled() -> None:
    exporter = _load_exporter()
    result = exporter.build_interest_clubs_projection(
        _club_db(),
        current_date="2026-07-17",
        generated_at="2026-07-17T10:00:00Z",
        exported_events=[],
    )

    assert result["source"] == "disabled-by-build-gate"
    assert result["clubs"] == []


def _production_control_db() -> tuple[sqlite3.Connection, dict]:
    fixture = json.loads(
        (ROOT / "tests" / "fixtures" / "interest_clubs_production_control_20260801.json").read_text(
            encoding="utf-8"
        )
    )
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(
        """
        create table interest_club (
          id integer primary key, slug text, canonical_name text, topic text,
          description text, city text, typical_place text, public_status text,
          identity_version integer default 1, policy_version text,
          aliases_json text, source_anchors_json text, provenance_json text,
          created_at text, updated_at text
        );
        create table interest_club_event (
          id integer primary key, club_id integer, event_id integer, status text,
          decision_lane text, evidence_quote text, evidence_json text, model text,
          policy_version text, input_hash text, evaluated_at text, updated_at text
        );
        create table interest_club_evaluation (
          id integer primary key, club_id integer, event_id integer, status text,
          verdict text, decision_lane text, evidence_quote text, evidence_json text,
          model text, policy_version text, input_hash text, error_code text,
          attempts integer, created_at text, updated_at text
        );
        create table event (
          id integer primary key, title text, date text, end_date text, time text,
          city text, location_name text, lifecycle_status text,
          identity_status text, merged_into_event_id integer, silent integer,
          festival text
        );
        create table event_source (id integer primary key,event_id integer,source_url text);
        """
    )
    for club in fixture["clubs"]:
        con.execute(
            "insert into interest_club(id,slug,canonical_name,topic,public_status,updated_at) values(?,?,?,?,?,?)",
            (*[club[key] for key in ("id", "slug", "canonical_name", "topic", "public_status")], "2026-07-22T23:31:20Z"),
        )
    for event in fixture["events"]:
        columns = list(event)
        con.execute(
            f"insert into event({','.join(columns)}) values({','.join('?' for _ in columns)})",
            [event[column] for column in columns],
        )
    for index, relation in enumerate(fixture["relations"], start=1):
        con.execute(
            "insert into interest_club_event(id,club_id,event_id,status,decision_lane,policy_version,input_hash,updated_at) values(?,?,?,?,?,?,?,?)",
            (index, relation["club_id"], relation["event_id"], relation["status"], "source", relation["policy_version"], relation["input_hash"], "2026-07-22T23:31:20Z"),
        )
        con.execute(
            "insert into interest_club_evaluation(id,club_id,event_id,status,verdict,decision_lane,policy_version,input_hash,updated_at) values(?,?,?,?,?,?,?,?,?)",
            (index, relation["club_id"], relation["event_id"], "accepted", "yes", "source", relation["policy_version"], relation["input_hash"], "2026-07-22T23:31:20Z"),
        )
    return con, fixture


def test_v2_exact_production_control_keeps_six_approved_clubs_without_upcoming_events() -> None:
    exporter = _load_exporter()
    con, fixture = _production_control_db()
    result = exporter.build_interest_clubs_projection_v2(
        con,
        current_date="2026-08-01",
        generated_at="2026-08-01T10:00:00Z",
        exported_events=[],
        enabled=True,
    )
    assert len(fixture["clubs"]) == 6
    assert len(fixture["relations"]) == 13
    assert {relation["event_id"] for relation in fixture["relations"]} == {
        2897, 6929, 6990, 2533, 6662, 3032, 3516, 5806,
        3488, 3923, 3265, 6853, 3393,
    }
    assert {club["slug"] for club in result["clubs"]} == {
        club["slug"] for club in fixture["clubs"]
    }
    assert all(club["status"] == "active" for club in result["clubs"])
    assert all(club["activity"]["future_meeting_count"] == 0 for club in result["clubs"])
    assert all(club["current_catalog_event_ids"] == [] for club in result["clubs"])
    assert result["window"]["six_months_start_inclusive"] == "2026-02-01"
    assert result["exclusion_receipts"]["festival_relation_allowed_count"] == 1
    assert result["exclusion_receipts"]["catalog_event_id_omitted_count"] == 13
    assert next(club for club in result["clubs"] if club["slug"] == "cinemango")["activity"]["meeting_count_6m"] == 1


def test_v2_hash_gate_boundary_dormancy_and_reactivation() -> None:
    exporter = _load_exporter()
    con, _fixture = _production_control_db()
    # Exact hash mismatch makes the one-relation CINEMANGO identity dormant.
    con.execute("update interest_club_evaluation set input_hash='mismatch' where club_id=8")
    first = exporter.build_interest_clubs_projection_v2(
        con, current_date="2026-08-01", generated_at="2026-08-01T10:00:00Z",
        exported_events=[], enabled=True,
    )
    assert "cinemango" not in {club["slug"] for club in first["clubs"]}
    assert first["exclusion_receipts"]["unaccepted_relation_count"] == 1

    con.execute("update interest_club_evaluation set input_hash=(select input_hash from interest_club_event where club_id=8) where club_id=8")
    con.execute("update event set date='2026-01-31' where id=3393")
    dormant = exporter.build_interest_clubs_projection_v2(
        con, current_date="2026-08-01", generated_at="2026-08-01T10:00:00Z",
        exported_events=[], enabled=True,
    )
    assert "cinemango" not in {club["slug"] for club in dormant["clubs"]}
    con.execute("update event set date='2026-02-01' where id=3393")
    boundary = exporter.build_interest_clubs_projection_v2(
        con, current_date="2026-08-01", generated_at="2026-08-01T10:00:00Z",
        exported_events=[], enabled=True,
    )
    assert "cinemango" in {club["slug"] for club in boundary["clubs"]}
    con.execute("update event set silent=1 where id=3393")
    silent = exporter.build_interest_clubs_projection_v2(
        con, current_date="2026-08-01", generated_at="2026-08-01T10:00:00Z",
        exported_events=[], enabled=True,
    )
    assert "cinemango" not in {club["slug"] for club in silent["clubs"]}
    con.execute("update event set silent=0,date='2026-08-10' where id=3393")
    reactivated = exporter.build_interest_clubs_projection_v2(
        con, current_date="2026-08-01", generated_at="2026-08-01T10:00:00Z",
        exported_events=[{"id": 3393, "slug": "cinemango-3393"}], enabled=True,
    )
    club = next(club for club in reactivated["clubs"] if club["slug"] == "cinemango")
    assert club["activity"]["next_activity_date"] == "2026-08-10"
    assert club["current_catalog_event_ids"] == [3393]
