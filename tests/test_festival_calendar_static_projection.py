from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

from scripts.backfill_festival_calendar_2026 import backfill


ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "site/src/data/festivalTimelineSeed.json"


CALENDAR_DDL = """
CREATE TABLE festival_calendar_item(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    calendar_year INTEGER NOT NULL,
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    date_precision TEXT NOT NULL DEFAULT 'exact',
    date_label TEXT NOT NULL,
    sort_date TEXT NOT NULL,
    month_key TEXT NOT NULL,
    display_order INTEGER NOT NULL,
    place_label TEXT NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    status_label TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_label TEXT NOT NULL,
    internal_event_id INTEGER,
    festival_id INTEGER,
    cover_key TEXT NOT NULL,
    image_width INTEGER NOT NULL,
    image_height INTEGER NOT NULL,
    media_mode TEXT NOT NULL DEFAULT 'visual',
    object_position TEXT,
    catalog_version TEXT NOT NULL,
    is_public BOOLEAN NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(calendar_year,slug),
    UNIQUE(calendar_year,display_order)
)
"""


def make_db(path: Path) -> None:
    seed = json.loads(SEED.read_text(encoding="utf-8"))
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE event(id INTEGER PRIMARY KEY)")
    con.execute(
        "CREATE TABLE festival("
        "id INTEGER PRIMARY KEY,name TEXT,full_name TEXT,aliases JSON,"
        "start_date TEXT,end_date TEXT)"
    )
    con.execute(CALENDAR_DDL)
    for item in seed["items"]:
        if item["festivalId"] is not None:
            name = (
                "Калининград Сити Джаз"
                if item["slug"] == "city-jazz"
                else item["title"]
            )
            con.execute(
                "INSERT INTO festival(id,name,full_name,aliases,start_date,end_date) "
                "VALUES(?,?,?,?,?,?)",
                (
                    item["festivalId"],
                    name,
                    item["title"],
                    json.dumps(item["aliases"], ensure_ascii=False),
                    "2025-01-01",
                    "2025-12-31",
                ),
            )
        if item["internalEventId"] is not None:
            con.execute(
                "INSERT OR IGNORE INTO event(id) VALUES(?)",
                (item["internalEventId"],),
            )
    con.commit()
    con.close()


def load_exporter():
    path = ROOT / "site/scripts/export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("festival_static_exporter", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_backfill_is_idempotent_and_never_rewrites_legacy_editions(tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite"
    make_db(db)

    dry = backfill(db, apply=False)
    assert dry["status"] == "dry-run"
    assert dry["inserts"] == 21

    applied = backfill(db, apply=True)
    assert applied["public_count_after"] == 21
    assert applied["public_distinct_slugs_after"] == 21
    assert applied["public_distinct_orders_after"] == 21

    again = backfill(db, apply=False)
    assert again["inserts"] == 0
    assert again["updates"] == 0
    assert again["unchanged"] == 21

    con = sqlite3.connect(db)
    assert con.execute(
        "SELECT start_date,end_date FROM festival WHERE id=5"
    ).fetchone() == ("2025-01-01", "2025-12-31")
    assert con.execute(
        "SELECT start_date,end_date,date_label,date_precision "
        "FROM festival_calendar_item WHERE slug='dni-literatury'"
    ).fetchone() == (None, None, "Октябрь", "month")
    con.close()


def test_backfill_refuses_display_order_conflicts(tmp_path: Path) -> None:
    db = tmp_path / "events.sqlite"
    make_db(db)
    con = sqlite3.connect(db)
    con.execute(
        """
        INSERT INTO festival_calendar_item(
          calendar_year,slug,title,description,date_label,sort_date,month_key,
          display_order,place_label,category,status,status_label,source_url,
          source_label,cover_key,image_width,image_height,catalog_version
        ) VALUES(2026,'wrong','Wrong','Wrong','Wrong','2026-01-01','july',1,
          'Wrong','Wrong','announced','Wrong','https://example.test/',
          'Wrong','/assets/festivals/timeline/wrong.webp',1,1,'test')
        """
    )
    con.commit()
    con.close()
    with pytest.raises(ValueError, match="display-order conflict"):
        backfill(db, apply=True)


def test_export_projection_is_db_backed_and_preserves_broad_date_labels(
    tmp_path: Path,
) -> None:
    db = tmp_path / "events.sqlite"
    make_db(db)
    backfill(db, apply=True)
    exporter = load_exporter()
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    projection = exporter.build_festival_timeline_projection(
        con,
        current_date="2026-07-26",
        generated_at="2026-07-26T18:00:00+00:00",
        require_complete=True,
    )
    con.close()

    assert projection["source"] == "sqlite-festival-calendar-v1"
    assert projection["database_row_count"] == 21
    assert len(projection["festivals"]) == 21
    assert len({item["slug"] for item in projection["festivals"]}) == 21
    status_counts = {
        status: sum(item["status"] == status for item in projection["festivals"])
        for status in {"announced", "program-pending", "date-pending"}
    }
    assert status_counts == {
        "announced": 9,
        "program-pending": 8,
        "date-pending": 4,
    }
    broad = next(
        item for item in projection["festivals"] if item["slug"] == "klub-puteshestvennikov"
    )
    assert broad["dateLabel"] == "Октябрь — декабрь"
    assert broad["startDate"] is None
    assert broad["endDate"] is None


def test_full_export_fails_closed_without_calendar_schema(tmp_path: Path) -> None:
    db = tmp_path / "empty.sqlite"
    sqlite3.connect(db).close()
    exporter = load_exporter()
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    with pytest.raises(ValueError, match="requires festival_calendar_item"):
        exporter.build_festival_timeline_projection(
            con,
            current_date="2026-07-26",
            generated_at="2026-07-26T18:00:00+00:00",
            require_complete=True,
        )
    con.close()
