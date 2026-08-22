from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest
import static_site_release

from static_site_release import (
    PROJECTION_CONTENT_SCHEMA,
    PROJECTION_SNAPSHOT_SCHEMA,
    STATIC_SITE_PROJECTION_COLUMNS,
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


def _load_collection_product_snapshot():
    path = (
        Path(__file__).resolve().parents[1]
        / "site"
        / "scripts"
        / "static_collection_product_snapshot.py"
    )
    spec = importlib.util.spec_from_file_location(
        "static_collection_product_snapshot_projection_test", path
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
            imported_at text,
            source_text text,
            source_fingerprint text,
            candidate_key text,
            occurrence_key text,
            smart_update_candidate_id integer,
            canonical_source_url text,
            source_role text
        );
        create table user(
            user_id integer primary key,
            username text,
            is_superadmin integer,
            is_partner integer,
            organization text,
            location text,
            blocked integer,
            last_partner_reminder text
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
        """insert into event_source(
            id,event_id,source_type,source_url,source_chat_username,
            source_chat_id,source_message_id,trust_level,imported_at,
            source_text,source_fingerprint,candidate_key,occurrence_key,
            canonical_source_url,source_role
        ) values(1,1,'telegram',?,'source',1,1,'trusted',?,?,?,?,?,?,?)""",
        (
            "https://t.me/source/1", "2026-08-15T00:00:00Z", "exact source evidence",
            "private fingerprint", "private candidate", "private occurrence",
            "https://t.me/source/1", "identity",
        ),
    )
    con.execute(
        "insert into user values(1,'private-user',1,1,'Partner','private-place',1,'private-time')"
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


def _add_full_exporter_products(path: Path) -> None:
    """Add every optional SQLite product consumed by the static exporter."""

    con = sqlite3.connect(path)
    for definition in (
        "short_description text", "festival text", "location_name text",
        "location_address text", "city text", "ticket_price_min integer",
        "ticket_price_max integer", "ticket_link text", "event_type text",
        "duration_forecast_minutes integer", "is_free integer default 0",
        "pushkin_card integer default 0", "source_text text",
        "collection_decisions text", "organizer_names text", "telegraph_url text",
        "creator_id integer", "topics text", "added_at text", "ticket_status text",
        "age_restriction text", "age_restriction_status text",
        "age_restriction_provenance text", "age_restriction_decision_version text",
        "age_assessment text", "linked_event_ids text", "revision integer",
        "updated_at text",
    ):
        con.execute(f"alter table event add column {definition}")
    for definition in (
        "image_text_mode text", "media_role_confidence real",
        "media_semantic_status text", "focal_x real", "focal_y real",
        "safe_crop integer", "image_geometry_id integer", "thumbnail_256_url text",
        "thumbnail_256_width integer", "thumbnail_256_height integer",
        "thumbnail_512_url text", "thumbnail_512_width integer",
        "thumbnail_512_height integer", "raw_sha256 text", "pixel_sha256 text",
        "canonical_object_path text",
    ):
        con.execute(f"alter table eventposter add column {definition}")
    con.executescript(
        """
        create table organization(name text primary key, vk_source_group_ids text, private_notes text);
        create table event_publication(
            event_id integer, platform text, target text, stored_url text,
            live_url text, status text, resolved_at text, stored_post_id integer
        );
        create table promo_exposure(
            event_id integer, surface text, publish_status text,
            details_json text, public_targets_json text, private_campaign_payload text
        );
        create table poll_repost_run(
            chosen_event_id integer, status text, poll_chat_id text,
            forwarded_message_id integer, error_json text
        );
        create table telegram_post_metric(
            source_url text, collected_ts integer, views integer, likes integer,
            forwards integer, reactions_json text
        );
        create table vk_post_metric(
            source_url text, collected_ts integer, views integer, likes integer,
            reposts integer, comments integer
        );
        create table social_metric_snapshot(
            platform text, publisher_id text, source_url text, age_bucket text,
            views integer, likes integer, comments integer, shares integer,
            collected_ts integer, status text, reactions_json text
        );
        create table event_image_geometry(
            id integer, pixel_sha256 text, model text, prompt_version text,
            status text, source_width integer, source_height integer,
            face_boxes_yxyx_json text, valuable_region_yxyx_json text,
            valuable_region_confidence real, reason_code text, private_trace text
        );
        create table video_asset(
            id integer, sha256 text, analysis_status text, cdn_url text,
            cdn_path text, mime_type text, width integer, height integer,
            duration_seconds real, aesthetic_score real, technical_score real,
            showcase_score real, description text, search_text text, analysis_json text
        );
        create table event_video_link(
            event_id integer, video_asset_id integer, event_relevance_score real,
            ranking_score real, source_url text, match_reason text
        );
        create table artist_registry_entity(
            artist_id text, entity_type text, display_name text,
            verification_status text, photo_url text, photo_rights_status text,
            photo_rights_evidence_json text, private_research_json text
        );
        create table event_artist_appearance(
            event_id integer, artist_id text, role text, status text,
            physical_visit_status text, participant_evidence_json text,
            eligibility_status text, cancelled_at text,
            media_identity_status text, private_review_json text
        );
        create table interest_club(
            id integer, slug text, canonical_name text, topic text,
            description text, city text, typical_place text, public_status text,
            updated_at text, provenance_json text
        );
        create table interest_club_event(
            club_id integer, event_id integer, status text, policy_version text,
            input_hash text, updated_at text, evidence_json text
        );
        create table interest_club_evaluation(
            id integer, club_id integer, event_id integer, status text, verdict text,
            policy_version text, input_hash text, updated_at text, evidence_json text
        );
        create table festival_calendar_item(
            id integer, calendar_year integer, slug text, title text, description text,
            start_date text, end_date text, date_precision text, date_label text,
            sort_date text, month_key text, display_order integer, place_label text,
            category text, status text, status_label text, source_url text,
            source_label text, internal_event_id integer, festival_id integer,
            cover_key text, image_width integer, image_height integer, media_mode text,
            object_position text, catalog_version text, is_public integer,
            internal_editor_notes text
        );
        """
    )
    con.execute(
        """update event set short_description='Коротко', location_name='Зал',
        location_address='Улица 1', city='Калининград', ticket_price_min=500,
        ticket_price_max=700, ticket_link='https://tickets.example/1',
        event_type='концерт', duration_forecast_minutes=90, is_free=0,
        pushkin_card=1, source_text=description, collection_decisions='{}',
        organizer_names='["Организатор"]', telegraph_url='https://telegra.ph/e',
        creator_id=1, topics='["музыка"]', added_at='2026-08-01T00:00:00Z',
        ticket_status='available', age_restriction='12+',
        age_restriction_status='declared', age_restriction_provenance='source',
        age_restriction_decision_version='v1', linked_event_ids='[]', revision=id,
        updated_at='2026-08-15T00:00:00Z'"""
    )
    con.execute("update user set is_partner=1, organization='Partner' where user_id=1")
    con.execute("insert into organization values('Partner','[123]','private')")
    con.execute(
        "insert into event_publication values(1,'vk','klgdevents',null,?,'published',?,999)",
        ("https://vk.com/wall-231920894_1", "2026-08-15T00:00:00Z"),
    )
    con.execute(
        "insert into promo_exposure values(1,'tg_repost','TG_FORWARDED',?,?,'private')",
        ('{"target_url":"https://t.me/kenigevents/1"}', "[]"),
    )
    con.execute("insert into poll_repost_run values(1,'forwarded','kenigevents',2,'private')")
    con.execute("insert into telegram_post_metric values(?,100,20,3,2,'private')", ("https://t.me/source/1",))
    con.execute("insert into vk_post_metric values(?,100,30,4,1,2)", ("https://vk.com/wall-1_2",))
    con.execute("insert into social_metric_snapshot values('telegram','source',?,'1h',20,3,1,2,100,'collected','private')", ("https://t.me/source/1",))
    pixel = "a" * 64
    con.execute("insert into event_image_geometry values(1,?,'model','v1','classified',1200,1600,'[]','[0,0,1,1]',1.0,'ok','private')", (pixel,))
    con.execute("update eventposter set image_geometry_id=1,pixel_sha256=?,media_semantic_status='accepted',image_text_mode='poster'", (pixel,))
    con.execute("insert into video_asset values(1,?,'accepted','https://cdn.example/video.mp4','video.mp4','video/mp4',720,1280,10,80,90,85,'Видео','поиск','private')", ("b" * 64,))
    con.execute("insert into event_video_link values(1,1,90,95,'https://source/video','private')")
    con.execute("insert into artist_registry_entity values('artist-1','person','Артист','verified','https://cdn.example/artist.jpg','press_kit_verified',?,'private')", ('[{"source_url":"https://artist.example","credit_text":"Автор"}]',))
    con.execute("insert into event_artist_appearance values(1,'artist-1','headliner','confirmed','confirmed',?,'eligible',null,'verified','private')", ('[{"source_url":"https://event.example"}]',))
    con.execute("insert into interest_club values(1,'club','Клуб','книги','Описание','Калининград','Зал','approved','2026-08-15','private')")
    for event_id in (1, 2):
        con.execute("insert into interest_club_event values(1,?,'active','v1',?,?,'private')", (event_id, f'h{event_id}', '2026-08-15'))
        con.execute("insert into interest_club_evaluation values(?,1,?,'accepted','yes','v1',?,?,'private')", (event_id, event_id, f'h{event_id}', '2026-08-15'))
    con.execute(
        """insert into festival_calendar_item values(
        1,2026,'fest','Фестиваль','Описание','2026-08-20','2026-08-21','exact',
        '20–21 августа','2026-08-20','2026-08',1,'Площадь','music','confirmed',
        'Подтверждено','https://festival.example','Официальный сайт',1,null,
        'fest.webp',1200,800,'visual','50% 50%','v1',1,'private')"""
    )
    con.commit()
    con.close()


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
        "user": 1,
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
    assert tables == {"event", "eventposter", "event_source", "user"}
    assert not tables & {
        "vk_source_packet",
        "vk_inbox",
        "joboutbox",
        "ops_run",
        "kaggle_run_ledger",
    }
    with sqlite3.connect(snapshot) as con:
        projected_columns = {
            table: [str(row[1]) for row in con.execute(f'pragma table_info("{table}")')]
            for table in ("user", "event_source")
        }
    assert projected_columns["user"] == ["user_id", "is_partner", "organization"]
    assert projected_columns["event_source"] == [
        "id", "event_id", "source_type", "source_url", "source_chat_username",
        "source_chat_id", "source_message_id", "imported_at", "trust_level",
        "source_text",
    ]
    assert metadata.table_columns["user"] == projected_columns["user"]
    assert "source_fingerprint" not in metadata.table_columns["event_source"]
    with sqlite3.connect(snapshot) as con:
        source_text = con.execute(
            "select source_text from event_source where id=1"
        ).fetchone()[0]
        source_records = _load_collection_product_snapshot().load_source_records(
            con, event_ids=[1]
        )
    assert source_text == "exact source evidence"
    assert source_records[1][0]["source_text"] == "exact source evidence"


def test_projection_preserves_exporter_visible_rows_and_media(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    snapshot, _manifest, metadata = _build_projection(
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


def test_projection_preserves_source_order_across_different_query_plans(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    with sqlite3.connect(source) as con:
        con.executemany(
            """
            insert into event_source(
                id,event_id,source_type,source_url,source_chat_username,
                source_chat_id,source_message_id,trust_level,imported_at
            ) values(?,1,'telegram',?,'source',1,?,'trusted','2026-08-15T00:00:00Z')
            """,
            [
                (2, "https://t.me/source/z-last-by-url", 2),
                (3, "https://t.me/source/a-first-by-url", 3),
            ],
        )
        # The production database and compact projection intentionally do not
        # share indexes. The public source order must therefore be explicit,
        # not an accidental consequence of either query plan.
        con.execute(
            "create index event_source_event_url_idx "
            "on event_source(event_id, source_url)"
        )
    snapshot, _manifest, _metadata = _build_projection(
        source, tmp_path / "projection"
    )
    exporter = _load_exporter()

    with sqlite3.connect(source) as live, sqlite3.connect(snapshot) as projected:
        live.row_factory = sqlite3.Row
        projected.row_factory = sqlite3.Row
        live_event = live.execute("select * from event where id=1").fetchone()
        projected_event = projected.execute(
            "select * from event where id=1"
        ).fetchone()
        expected = [
            "https://t.me/source/1",
            "https://t.me/source/z-last-by-url",
            "https://t.me/source/a-first-by-url",
        ]
        assert exporter.collect_source_urls(live, 1, live_event) == expected
        assert exporter.collect_source_urls(projected, 1, projected_event) == expected


def test_projection_parity_covers_full_exporter_products_and_optional_relations(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    _add_full_exporter_products(source)
    snapshot, _manifest, metadata = _build_projection(
        source, tmp_path / "projection"
    )
    exporter = _load_exporter()

    def products(con: sqlite3.Connection) -> dict[str, object]:
        con.row_factory = sqlite3.Row
        rows = exporter.fetch_rows(con, None, "2026-08-15", [])
        archive = exporter.fetch_recent_event_detail_archive_rows(
            con, "2026-08-15"
        )
        all_rows = [*rows, *archive]
        ids = [int(row["id"]) for row in all_rows]
        participants = exporter.event_participants_for_events(con, ids)
        videos = exporter.event_video_assets_for_events(con, ids)
        events = [
            exporter.build_event(
                con,
                row,
                "2026-08-15",
                participants=participants[int(row["id"])],
                video_assets=videos[int(row["id"])],
            )
            for row in rows
        ]
        archived_events = [
            exporter.build_event(
                con,
                row,
                "2026-08-15",
                participants=participants[int(row["id"])],
                video_assets=videos[int(row["id"])],
            )
            for row in archive
        ]
        ledger = exporter.build_catalog_ledger(
            con,
            rows,
            exported_events=events,
            current_date="2026-08-15",
            current_time="12:00",
            generated_at="2026-08-15T10:00:00Z",
            repo_sha="a" * 40,
            run_id="run",
            build_id="production-test",
            snapshot_id="snapshot",
            snapshot_sha256="b" * 64,
            snapshot_size=123,
        )
        return {
            "events": events,
            "archive": archived_events,
            "ledger": ledger,
            "sources": {
                int(row["id"]): exporter.collect_source_records(con, int(row["id"]))
                for row in all_rows
            },
            "clubs_v1": exporter.build_interest_clubs_projection(
                con,
                current_date="2026-08-15",
                generated_at="2026-08-15T10:00:00Z",
                exported_events=events,
                enabled=True,
            ),
            "clubs_v2": exporter.build_interest_clubs_projection_v2(
                con,
                current_date="2026-08-15",
                generated_at="2026-08-15T10:00:00Z",
                exported_events=events,
                enabled=True,
            ),
            "festival": exporter.build_festival_timeline_projection(
                con,
                current_date="2026-08-15",
                generated_at="2026-08-15T10:00:00Z",
                require_complete=False,
            ),
        }

    with sqlite3.connect(source) as live, sqlite3.connect(snapshot) as projected:
        assert products(projected) == products(live)
        for table, columns in metadata.table_columns.items():
            assert set(columns) <= set(STATIC_SITE_PROJECTION_COLUMNS[table])
            assert not any(
                column.startswith("private_") or column in {"evidence_json", "provenance_json"}
                for column in columns
            )


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


def test_projection_manifest_column_tamper_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    snapshot, manifest, _metadata = _build_projection(
        source, tmp_path / "projection"
    )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["table_columns"]["user"].append("username")
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        StaticSitePermanentError,
        match="static_site_projection_column_mismatch:user",
    ):
        validate_snapshot(snapshot, manifest)


def test_projection_size_cap_fails_before_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    with sqlite3.connect(source) as con:
        con.execute(
            "update event set description=? where id=1",
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


def test_projection_progress_handler_interrupts_long_source_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.sqlite"
    _source_database(source)
    with sqlite3.connect(source) as con:
        con.executemany(
            "insert into event(id,title,date) values(?,?,?)",
            ((event_id, f"Event {event_id}", "2026-08-17") for event_id in range(3, 20003)),
        )
        con.commit()

    clock = iter((0.0, 10.0, 10.0, 10.0))
    monkeypatch.setattr(
        static_site_release.unix_time,
        "monotonic",
        lambda: next(clock, 10.0),
    )
    monkeypatch.setenv("STATIC_SITE_PROJECTION_READ_MAX_SECONDS", "5")
    with pytest.raises(
        StaticSiteRetryableError,
        match="sqlite_projection_failed:OperationalError:interrupted",
    ):
        _build_projection(source, tmp_path / "projection")

    with sqlite3.connect(source, timeout=0.2) as writer:
        busy, _log_pages, _checkpointed = writer.execute(
            "pragma wal_checkpoint(truncate)"
        ).fetchone()
    assert busy == 0
