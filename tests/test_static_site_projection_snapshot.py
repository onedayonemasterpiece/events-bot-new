from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

from static_site_release import (
    PROJECTION_CONTENT_SCHEMA,
    PROJECTION_SNAPSHOT_SCHEMA,
    StaticSitePermanentError,
    StaticSiteRetryableError,
    create_immutable_projection_snapshot,
    validate_snapshot,
)


def _load_exporter():
    path = (
        Path(__file__).resolve().parents[1]
        / "site"
        / "scripts"
        / "export-production-preview-data.py"
    )
    spec = importlib.util.spec_from_file_location(
        "static_projection_exporter_test", path
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _source_database(path: Path, *, large_operational_payload: int = 0) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        pragma journal_mode=wal;
        create table event(
            id integer primary key,
            title text not null,
            description text,
            date text not null,
            end_date text,
            time text,
            silent integer default 0,
            lifecycle_status text default 'active',
            identity_status text default 'resolved',
            merged_into_event_id integer,
            source_post_url text,
            source_vk_post_url text,
            tg_event_post_url text,
            vk_repost_url text,
            photo_urls text
        );
        create table eventposter(
            id integer primary key,
            event_id integer not null,
            supabase_url text,
            catbox_url text,
            ocr_text text,
            review_status text,
            display_order integer,
            media_role text,
            recommended_hero_fit text,
            width integer,
            height integer
        );
        create table event_source(
            id integer primary key,
            event_id integer not null,
            source_type text,
            source_url text,
            source_chat_username text,
            source_chat_id integer,
            source_message_id integer,
            trust_level text,
            imported_at text
        );
        create table vk_source_packet(id integer primary key, payload blob);
        create table vk_inbox(id integer primary key, payload text);
        create table joboutbox(id integer primary key, payload text);
        create table ops_run(id integer primary key, details_json text);
        create table kaggle_run_ledger(run_id text primary key, token_hash text);
        """
    )
    con.executemany(
        """
        insert into event(
            id,title,description,date,end_date,time,silent,lifecycle_status,
            identity_status,source_post_url,photo_urls
        ) values(?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                1,
                "Текущий концерт",
                "Описание",
                "2026-08-16",
                None,
                "19:00",
                0,
                "active",
                "canonical",
                "https://t.me/source/1",
                "[]",
            ),
            (
                2,
                "Прошедшее событие",
                "Архив",
                "2026-08-01",
                None,
                "18:00",
                0,
                "active",
                "canonical",
                "https://vk.com/wall-1_2",
                "[]",
            ),
        ],
    )
    con.execute(
        "insert into eventposter values(1,1,?,?,?,?,?,?,?,?,?)",
        (
            "https://cdn.example/one.jpg",
            None,
            "poster",
            "approved",
            1,
            "poster",
            "contain",
            1200,
            1600,
        ),
    )
    con.execute(
        "insert into event_source values(1,1,'telegram',?,'source',1,1,'trusted',?)",
        ("https://t.me/source/1", "2026-08-15T00:00:00Z"),
    )
    con.execute(
        "insert into vk_source_packet(payload) values(?)",
        (b"x" * large_operational_payload,),
    )
    con.execute("insert into vk_inbox(payload) values('private')")
    con.execute("insert into joboutbox(payload) values('private')")
    con.execute("insert into ops_run(details_json) values('private')")
    con.execute("insert into kaggle_run_ledger values('run','secret')")
    con.commit()
    con.close()


def _build_projection(source: Path, target: Path):
    return create_immutable_projection_snapshot(
        source,
        target,
        request_payload={
            "target_watermark": "watermark",
            "event_revisions": {"1": "a" * 64},
        },
        snapshot_id="snapshot-projection-test",
    )


def test_projection_is_compact_and_excludes_operational_tables(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source, large_operational_payload=8 * 1024 * 1024)

    snapshot, manifest, metadata = _build_projection(source, tmp_path / "projection")

    assert metadata.schema_version == PROJECTION_SNAPSHOT_SCHEMA
    assert metadata.projection_schema_version == PROJECTION_CONTENT_SCHEMA
    assert metadata.quick_check == "ok"
    assert metadata.table_row_counts == {
        "event": 2,
        "event_source": 1,
        "eventposter": 1,
    }
    assert metadata.source_table_row_counts == metadata.table_row_counts
    assert snapshot.stat().st_size < source.stat().st_size // 4
    assert validate_snapshot(snapshot, manifest) == metadata
    with sqlite3.connect(snapshot) as con:
        tables = {
            str(row[0])
            for row in con.execute(
                "select name from sqlite_master "
                "where type='table' and name not like 'sqlite_%'"
            )
        }
    assert tables == {"event", "eventposter", "event_source"}
    assert not tables & {
        "vk_source_packet",
        "vk_inbox",
        "joboutbox",
        "ops_run",
        "kaggle_run_ledger",
    }


def test_projection_preserves_exporter_visible_rows_and_media(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    snapshot, _manifest, _metadata = _build_projection(
        source, tmp_path / "projection"
    )
    exporter = _load_exporter()

    with sqlite3.connect(source) as live, sqlite3.connect(snapshot) as projected:
        live.row_factory = sqlite3.Row
        projected.row_factory = sqlite3.Row
        live_rows = exporter.fetch_rows(
            live,
            limit=None,
            current_date="2026-08-15",
            include_ids=[],
        )
        projected_rows = exporter.fetch_rows(
            projected,
            limit=None,
            current_date="2026-08-15",
            include_ids=[],
        )
        assert [dict(row) for row in projected_rows] == [
            dict(row) for row in live_rows
        ]
        live_event = live.execute("select * from event where id=1").fetchone()
        projected_event = projected.execute(
            "select * from event where id=1"
        ).fetchone()
        assert exporter.collect_source_urls(projected, 1, projected_event) == (
            exporter.collect_source_urls(live, 1, live_event)
        )
        assert exporter.collect_images(
            projected, 1, "[]", "Текущий концерт"
        ) == exporter.collect_images(live, 1, "[]", "Текущий концерт")


def test_projection_manifest_row_count_tamper_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    snapshot, manifest, _metadata = _build_projection(
        source, tmp_path / "projection"
    )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["table_row_counts"]["event"] = 999
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        StaticSitePermanentError,
        match="static_site_projection_row_count_mismatch:event",
    ):
        validate_snapshot(snapshot, manifest)


def test_projection_size_cap_fails_before_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    with sqlite3.connect(source) as con:
        con.execute("alter table event add column large_public_payload blob")
        con.execute(
            "update event set large_public_payload=? where id=1",
            (b"y" * (2 * 1024 * 1024),),
        )
        con.commit()
    monkeypatch.setenv("STATIC_SITE_PROJECTION_MAX_BYTES", str(1024 * 1024))

    with pytest.raises(
        StaticSiteRetryableError,
        match="static_site_projection_size_out_of_bounds",
    ):
        _build_projection(source, tmp_path / "projection")


def test_projection_closes_source_reader_before_return(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    _build_projection(source, tmp_path / "projection")

    with sqlite3.connect(source, timeout=0.2) as writer:
        writer.execute(
            "insert into event(id,title,date) values(3,'После снимка','2026-08-17')"
        )
        writer.commit()
        busy, _log_pages, _checkpointed = writer.execute(
            "pragma wal_checkpoint(truncate)"
        ).fetchone()
    assert busy == 0
