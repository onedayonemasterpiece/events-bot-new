from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


def _load_exporter_module():
    path = Path(__file__).resolve().parents[1] / "site" / "scripts" / "export-production-preview-data.py"
    spec = importlib.util.spec_from_file_location("export_production_preview_data", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _connect_with_public_gate_columns() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table event(
            id integer primary key,
            date text,
            end_date text,
            time text,
            title text,
            location_name text,
            location_address text,
            city text,
            lifecycle_status text,
            status text,
            moderation_status text,
            identity_status text,
            merged_into_event_id integer
        )
        """
    )
    return con


def _insert_event(con: sqlite3.Connection, event_id: int, **overrides: object) -> None:
    row = {
        "id": event_id,
        "date": "2026-07-10",
        "end_date": None,
        "time": "19:00",
        "title": f"Valid event {event_id}",
        "location_name": "Кафедральный собор",
        "location_address": "Канта 1",
        "city": "Калининград",
        "lifecycle_status": "active",
        "status": "active",
        "moderation_status": "accepted",
        "identity_status": "canonical",
        "merged_into_event_id": None,
    }
    row.update(overrides)
    columns = ", ".join(row)
    placeholders = ", ".join("?" for _ in row)
    con.execute(f"insert into event({columns}) values({placeholders})", tuple(row.values()))


def test_public_projection_gate_applies_to_include_ids() -> None:
    exporter = _load_exporter_module()
    con = _connect_with_public_gate_columns()
    _insert_event(con, 1)
    _insert_event(con, 2, identity_status="alias")
    _insert_event(con, 3, merged_into_event_id=1)
    _insert_event(con, 4, lifecycle_status="review")
    _insert_event(con, 5, status="quarantine")
    _insert_event(con, 6, moderation_status="rejected")

    rows = exporter.fetch_rows(con, limit=20, current_date="2026-07-01", include_ids=[1, 2, 3, 4, 5, 6])

    assert [row["id"] for row in rows] == [1]


def test_public_projection_gate_rejects_invalid_iso_dates_and_leakage() -> None:
    exporter = _load_exporter_module()
    con = _connect_with_public_gate_columns()
    _insert_event(con, 1)
    _insert_event(con, 2, date="2026-99-99")
    _insert_event(con, 3, end_date="2026-02-31")
    _insert_event(con, 4, title="Вот обновленный текст: концерт")
    _insert_event(con, 5, location_name="В программе — лекция и обсуждение")
    _insert_event(con, 6, location_address="```json {\"venue\": \"Дом\"}")

    rows = exporter.fetch_rows(con, limit=20, current_date="2026-07-01", include_ids=[1, 2, 3, 4, 5, 6])

    assert [row["id"] for row in rows] == [1]


def test_public_projection_gate_is_safe_for_old_schema_rows() -> None:
    exporter = _load_exporter_module()
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table event(
            id integer primary key,
            date text,
            time text,
            title text,
            location_name text
        )
        """
    )
    con.execute(
        "insert into event(id, date, time, title, location_name) values(?, ?, ?, ?, ?)",
        (10, "2026-07-10", "18:00", "Старый формат", "Музей"),
    )

    rows = exporter.fetch_rows(con, limit=5, current_date="2026-07-01", include_ids=[10])

    assert [row["id"] for row in rows] == [10]


def test_source_grounded_ocr_override_keeps_known_text_poster_uncropped(monkeypatch) -> None:
    exporter = _load_exporter_module()
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "create table eventposter(id integer primary key, event_id integer, supabase_url text, catbox_url text, ocr_text text)"
    )
    con.execute(
        "insert into eventposter(event_id, supabase_url, catbox_url, ocr_text) values(?, ?, ?, ?)",
        (6510, "https://example.test/text-poster.webp", None, None),
    )
    monkeypatch.setattr(exporter, "probe_image_dimensions", lambda _url: (1080, 1080))

    primary, mode, assets = exporter.collect_images(con, 6510, "[]", "Хиты любимых артистов")

    assert primary == "https://example.test/text-poster.webp"
    assert mode == "ocr_text"
    assert assets[0]["recommended_hero_fit"] == "contain"
    assert assets[0]["safe_crop"] is False
