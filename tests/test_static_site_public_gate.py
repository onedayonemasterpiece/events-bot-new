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
            silent integer default 0,
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
        "silent": 0,
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
    _insert_event(con, 7, silent=1)

    rows = exporter.fetch_rows(con, limit=20, current_date="2026-07-01", include_ids=[1, 2, 3, 4, 5, 6, 7])

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


def test_add_build_07_full_catalog_mode_is_unbounded_and_fail_closed() -> None:
    exporter = _load_exporter_module()
    con = _connect_with_public_gate_columns()
    for event_id in range(1, 121):
        _insert_event(con, event_id, date=f"2026-07-{10 + (event_id % 20):02d}")
    _insert_event(con, 1001, silent=1)
    _insert_event(con, 1002, lifecycle_status="cancelled")

    rows = exporter.fetch_rows(
        con,
        limit=None,
        current_date="2026-07-01",
        include_ids=[],
    )

    assert len(rows) == 120
    assert {row["id"] for row in rows} == set(range(1, 121))
    try:
        exporter.fetch_rows(con, limit=0, current_date="2026-07-01", include_ids=[])
    except ValueError as exc:
        assert "catalog_mode=full" in str(exc)
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("limit=0 must not mean full catalog")


def test_full_catalog_keeps_elapsed_same_day_events_for_today_muting() -> None:
    exporter = _load_exporter_module()
    con = _connect_with_public_gate_columns()
    _insert_event(con, 1, date="2026-07-27", time="11:00")
    _insert_event(con, 2, date="2026-07-27", time="19:00")
    _insert_event(con, 3, date="2026-07-26", time="23:00")
    _insert_event(con, 4, date="2026-07-28", time="09:00")

    rows = exporter.fetch_rows(
        con,
        limit=None,
        current_date="2026-07-27",
        current_time="21:50",
        include_ids=[],
    )
    ledger = exporter.build_catalog_ledger(
        con,
        rows,
        current_date="2026-07-27",
        current_time="21:50",
        generated_at="2026-07-27T19:50:00+00:00",
        repo_sha="a" * 40,
        run_id="static-site:test:today-elapsed",
        build_id="production-test-today-elapsed",
        snapshot_id="snapshot-test-today-elapsed",
        snapshot_sha256="b" * 64,
        snapshot_size=123,
    )

    assert [row["id"] for row in rows] == [1, 2, 4]
    assert [item["event_id"] for item in ledger["eligible"]] == [1, 2, 4]


def test_recent_detail_archive_retains_links_without_reentering_public_catalog() -> None:
    exporter = _load_exporter_module()
    con = _connect_with_public_gate_columns()
    _insert_event(con, 1, date="2026-07-26")
    _insert_event(con, 2, date="2026-07-13")
    _insert_event(con, 3, date="2026-07-27")
    _insert_event(con, 4, date="2026-07-25", silent=1)
    _insert_event(con, 5, date="2026-07-24", identity_status="alias")
    _insert_event(con, 6, date="2026-07-01", end_date="2026-07-20")

    current = exporter.fetch_rows(
        con,
        limit=None,
        current_date="2026-07-28",
        include_ids=[],
    )
    archived = exporter.fetch_recent_event_detail_archive_rows(
        con,
        current_date="2026-07-28",
    )

    assert current == []
    assert [row["id"] for row in archived] == [3, 1, 6, 2]
    assert exporter.EVENT_DETAIL_ARCHIVE_DAYS == 30


def test_add_build_09_catalog_ledger_proves_eligible_and_excluded_parity() -> None:
    exporter = _load_exporter_module()
    con = _connect_with_public_gate_columns()
    con.execute("alter table event add column age_restriction text")
    con.execute("alter table event add column age_restriction_status text")
    _insert_event(con, 1, age_restriction="16+", age_restriction_status="declared")
    _insert_event(con, 2, silent=1)
    _insert_event(con, 3, identity_status="alias")
    _insert_event(con, 4, age_restriction="18+", age_restriction_status="unknown")
    rows = exporter.fetch_rows(con, limit=None, current_date="2026-07-01", include_ids=[])

    ledger = exporter.build_catalog_ledger(
        con,
        rows,
        current_date="2026-07-01",
        current_time=None,
        generated_at="2026-07-17T00:00:00+00:00",
        repo_sha="a" * 40,
        run_id="static-site:test:1",
        build_id="production-test-1",
        snapshot_id="snapshot-test-1",
        snapshot_sha256="b" * 64,
        snapshot_size=123,
    )

    assert ledger["eligible_count"] == 2
    assert [item["event_id"] for item in ledger["eligible"]] == [1, 4]
    assert [item["age_restriction"] for item in ledger["eligible"]] == ["16+", None]
    assert {item["event_id"]: item["reason"] for item in ledger["excluded"]} == {
        2: "silent",
        3: "identity_status:not_canonical",
    }
    assert ledger["snapshot"]["sha256"] == "b" * 64


def test_add_build_09_linked_occurrences_are_mutual_and_catalog_bounded() -> None:
    exporter = _load_exporter_module()
    events = [
        {"id": 1, "other_date_ids": [1, 2, 3, 999]},
        {"id": 2, "other_date_ids": [1]},
        {"id": 3, "other_date_ids": []},
    ]

    exporter.normalize_linked_occurrences(events)

    assert events == [
        {"id": 1, "other_date_ids": [2]},
        {"id": 2, "other_date_ids": [1]},
        {"id": 3, "other_date_ids": []},
    ]


def test_collect_images_uses_one_url_per_approved_logical_poster() -> None:
    exporter = _load_exporter_module()
    exporter.SKIP_IMAGE_PROBES = True
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table eventposter(
            id integer primary key,
            event_id integer not null,
            supabase_url text,
            catbox_url text,
            ocr_text text,
            review_status text,
            display_order integer default 0
        )
        """
    )
    con.executemany(
        "insert into eventposter values(?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 7, "https://static.kenigevents.ru/a.webp", "https://source.example/a.jpg", "A", "approved", 0),
            (2, 7, "https://static.kenigevents.ru/b.webp", None, "B", "pending_review", 1),
            (3, 7, None, "https://source.example/c.jpg", "C", "duplicate", 2),
            (4, 7, None, "https://source.example/d.jpg", "D", "approved", 3),
        ],
    )

    _primary, _mode, _role, assets = exporter.collect_images(
        con,
        7,
        '["https://legacy.example/leak.jpg"]',
        "Событие",
    )

    assert [asset["src"] for asset in assets] == [
        "https://static.kenigevents.ru/a.webp",
    ]
    assert assets[0]["media_role"] == "unknown_document"
    assert assets[0]["image_text_mode"] == "unknown"
    assert assets[0]["recommended_hero_fit"] == "contain"
    assert _mode == "unknown"
    assert _role == "unknown_document"


def test_collect_images_canonicalizes_current_bucket_and_rejects_other_hosts() -> None:
    exporter = _load_exporter_module()
    exporter.SKIP_IMAGE_PROBES = True
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "create table eventposter(id integer primary key, event_id integer, supabase_url text, catbox_url text, ocr_text text, review_status text, display_order integer)"
    )
    con.executemany(
        "insert into eventposter values(?, ?, ?, ?, ?, ?, ?)",
        [
            (
                1,
                9,
                "https://storage.yandexcloud.net/kenigevents.ru/p/a.webp",
                None,
                None,
                "approved",
                0,
            ),
            (2, 9, "https://legacy.example/b.webp", None, None, "approved", 1),
        ],
    )

    primary, _mode, _role, assets = exporter.collect_images(con, 9, "[]", "Событие")

    assert primary == "https://static.kenigevents.ru/p/a.webp"
    assert [asset["src"] for asset in assets] == [
        "https://static.kenigevents.ru/p/a.webp"
    ]
    assert _mode == "unknown"
    assert assets[0]["image_text_mode"] == "unknown"


def test_collect_images_does_not_fallback_when_only_quarantined_rows_exist() -> None:
    exporter = _load_exporter_module()
    exporter.SKIP_IMAGE_PROBES = True
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "create table eventposter(id integer primary key, event_id integer, supabase_url text, catbox_url text, ocr_text text, review_status text, display_order integer)"
    )
    con.execute(
        "insert into eventposter values(1, 8, 'https://static.example/pending.webp', null, null, 'pending_review', 0)"
    )
    _primary, _mode, _role, assets = exporter.collect_images(
        con, 8, '["https://legacy.example/leak.jpg"]', "Событие"
    )
    assert assets == []


def _geometry_export_connection(*, poster_pixel: str = "pixel-current") -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table eventposter(
            id integer primary key,
            event_id integer,
            supabase_url text,
            catbox_url text,
            ocr_text text,
            review_status text,
            display_order integer,
            width integer,
            height integer,
            image_text_mode text,
            media_role text,
            media_role_confidence real,
            media_semantic_status text,
            safe_crop integer,
            raw_sha256 text,
            pixel_sha256 text,
            image_geometry_id integer
        )
        """
    )
    con.execute(
        """
        create table event_image_geometry(
            id integer primary key,
            pixel_sha256 text,
            model text,
            prompt_version text,
            status text,
            source_width integer,
            source_height integer,
            face_boxes_yxyx_json text,
            valuable_region_yxyx_json text,
            valuable_region_confidence real,
            reason_code text
        )
        """
    )
    con.execute(
        """
        insert into eventposter(
            id,event_id,supabase_url,review_status,display_order,width,height,
            image_text_mode,media_role,media_role_confidence,media_semantic_status,
            safe_crop,raw_sha256,pixel_sha256,image_geometry_id
        ) values(1,42,'https://static.kenigevents.ru/p/current.webp','approved',0,
                 1600,1000,'visual_only','event_photo',.97,'classified',1,
                 'raw-current',?,7)
        """,
        (poster_pixel,),
    )
    con.execute(
        """
        insert into event_image_geometry values(
            7,'pixel-current','gemma-4-31b-it','event-image-geometry-v1','classified',1600,1000,
            '[[0.2,0.4,0.4,0.6],[2,0,3,1]]','[0.1,0.25,0.8,0.75]',.92,
            'viewer_value_region'
        )
        """
    )
    return con


def test_collect_images_exports_only_current_classified_normalized_geometry() -> None:
    exporter = _load_exporter_module()
    exporter.SKIP_IMAGE_PROBES = True
    con = _geometry_export_connection()

    _primary, _mode, _role, assets = exporter.collect_images(con, 42, "[]", "Событие")

    assert assets[0]["geometry_coordinate_space"] == "normalized_0_1"
    assert assets[0]["current_pixel_sha256"] == assets[0]["geometry_pixel_sha256"] == "pixel-current"
    assert assets[0]["face_boxes"] == [{"x": 0.4, "y": 0.2, "w": 0.2, "h": 0.2}]
    assert assets[0]["valuable_region"] == {
        "x": 0.25,
        "y": 0.1,
        "w": 0.5,
        "h": 0.7,
        "confidence": 0.92,
    }
    assert assets[0]["safe_crop"] is True
    assert assets[0]["recommended_hero_fit"] == "cover"


def test_collect_images_drops_stale_geometry_and_closes_crop_gate() -> None:
    exporter = _load_exporter_module()
    exporter.SKIP_IMAGE_PROBES = True
    con = _geometry_export_connection(poster_pixel="pixel-replaced")

    _primary, _mode, _role, assets = exporter.collect_images(con, 42, "[]", "Событие")

    assert "geometry_id" not in assets[0]
    assert "face_boxes" not in assets[0]
    assert "valuable_region" not in assets[0]
    assert assets[0]["safe_crop"] is False
    assert assets[0]["recommended_hero_fit"] == "contain"


def test_collect_images_drops_same_pixel_geometry_from_obsolete_contract() -> None:
    exporter = _load_exporter_module()
    exporter.SKIP_IMAGE_PROBES = True
    con = _geometry_export_connection()
    con.execute(
        "update event_image_geometry set model='gemma-3-27b-it', prompt_version='event-image-geometry-v0' where id=7"
    )

    _primary, _mode, _role, assets = exporter.collect_images(con, 42, "[]", "Событие")

    assert "geometry_id" not in assets[0]
    assert assets[0]["safe_crop"] is False
    assert assets[0]["recommended_hero_fit"] == "contain"
