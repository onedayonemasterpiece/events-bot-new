from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


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
        "insert into interest_club_event(id,club_id,event_id,status) values(?,?,?,?)",
        [
            (1, 1, 10, "active"),
            (2, 1, 11, "active"),
            (3, 1, 12, "deferred"),
            (4, 1, 13, "active"),
            (5, 1, 14, "active"),
            (6, 2, 15, "active"),
            (7, 4, 16, "active"),
            (8, 5, 17, "active"),
            (9, 5, 18, "active"),
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
