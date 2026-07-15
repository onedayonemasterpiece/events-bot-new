from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image

from db import Database
from service_share_card import (
    CANONICAL_URL, MANIFEST_SCHEMA_VERSION, build_catalog_snapshot,
    export_asset_bundle, load_active_promo_candidates, load_catalog_snapshot,
)
from service_share_scheduler import register_service_share_daily_job
from scripts.research.select_service_share_events import select
from scripts.run_service_share_still_kaggle import create_status_input_dataset


def event(event_id: int, **overrides):
    row = {
        "id": event_id, "title": f"Событие {event_id}", "date": "2026-07-20", "end_date": None,
        "time": "19:00", "city": "Калининград", "festival": None,
        "poster_url": f"https://cdn.test/{event_id}.webp", "safe_crop": True,
        "image_has_ocr_text": False,
        "added_at": "2026-07-14T12:00:00+00:00", "identity_status": "canonical",
        "merged_into_event_id": None, "silent": False, "lifecycle_status": "active",
    }
    row.update(overrides)
    return row


def test_snapshot_is_public_normalized_recent_and_order_stable():
    measured = datetime(2026, 7, 15, 8, tzinfo=timezone.utc)
    rows = [
        event(1, date="2026-07-01", end_date="2026-07-18", city="пос. Романово"),
        event(2, city=" РОМАНОВО ", added_at="2026-07-01T00:00:00Z"),
        event(3, lifecycle_status="cancelled"), event(4, silent=True),
        event(5, date="2026-07-01", end_date=None), event(6, identity_status="review"),
    ]
    first = build_catalog_snapshot(rows, measured_at=measured)
    second = build_catalog_snapshot(list(reversed(rows)), measured_at=measured)
    assert first["eligible_event_count"] == 2
    assert first["city_count"] == 1
    assert first["city_names"] == ["Романово"]
    assert first["recent_added_count"] == 1
    assert first["catalog_hash"] == second["catalog_hash"]


def test_selection_reserves_promo_and_never_mislabels_underfill():
    events = []
    for event_id in range(1, 13):
        row = event(event_id)
        if event_id == 1:
            row["image_has_ocr_text"] = True
        row.update({"start_date": row["date"], "image_url": row["poster_url"],
                    "source_likes_count": 100 - event_id, "source_views_count": event_id * 10,
                    "source_engagement_sources_count": 1})
        events.append(row)
    result = select(events, local_date="2026-07-15", promo_candidates=[{"event_id": 1, "campaign_id": 7, "activity_id": 0,
                                                                         "target_id": 9, "provenance": "explicit_target_preview_fallback_no_surface_activity"}])
    assert result["events"][1]["event_id"] == 1
    assert result["events"][1]["selection_group"] == "promoted"
    assert result["events"][1]["image_has_ocr_text"] is True
    assert result["promo_status"] == {"requested": 2, "selected": 1, "underfilled": True, "missing": 1,
                                      "fallback_mislabeled_as_promo": False,
                                      "reason": "active_explicit_promo_targets_with_approved_posters_exhausted"}
    assert result["actual_mix"]["promoted"] == 1
    assert result["promo_shortfall"] == {"requested": 2, "selected": 1, "missing": 1,
                                           "reason": "active_explicit_promo_targets_with_approved_posters_exhausted"}
    assert len(result["events"]) == 8 == len({row["event_id"] for row in result["events"]})
    assert [row["slot_index"] for row in result["events"]] == list(range(8))
    tomorrow = select(events, local_date="2026-07-16", promo_candidates=[{"event_id": 1}])
    assert [row["event_id"] for row in result["events"]] != [row["event_id"] for row in tomorrow["events"]]


def test_manifest_exact_ui_contract_and_true_assets(tmp_path: Path):
    master = tmp_path / "master.png"; Image.new("RGB", (64, 64), "#f1ece5").save(master)
    snapshot = {"local_date": "2026-07-15", "measured_at": "2026-07-15T08:00:00Z", "timezone": "Europe/Kaliningrad",
                "eligible_event_count": 284, "city_count": 15, "recent_added_count": 85, "catalog_hash": "a" * 64}
    selection = {"events": [{"event_id": 1, "selection_group": "popular"}], "actual_mix": {"popular": 1},
                 "promo_status": {"underfilled": True}}
    path = export_asset_bundle(master_png=master, output_dir=tmp_path / "out", visual_payload={"date": "2026-07-15"},
                               selection=selection, snapshot=snapshot, composition={"family": "soft_s_curve"},
                               bundle_sha256="b" * 64, result_sha256="c" * 64)
    data = json.loads(path.read_text())
    assert path.as_posix().endswith("current/manifest.json")
    assert data["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert data["canonical_url"] == CANONICAL_URL
    assert data["asset_version"] and data["visual_payload_hash"]
    for kind, signature, mime in (("png", b"\x89PNG\r\n\x1a\n", "image/png"), ("webp", b"RIFF", "image/webp")):
        record = data["assets"][kind]
        asset = path.parents[1] / "versions" / data["asset_version"] / record["filename"]
        assert asset.read_bytes().startswith(signature)
        assert record["mime_type"] == mime and record["byte_size"] == asset.stat().st_size
        assert record["sha256"] == hashlib.sha256(asset.read_bytes()).hexdigest()
        assert record["url"].startswith("../versions/")


def test_status_input_is_absent_when_callback_contract_incomplete(tmp_path: Path):
    class Client:
        pass
    assert create_status_input_dataset(Client(), username="u", run_id="r", kernel_ref="u/k", dataset_ref="u/d",
                                       status_db=None, callback_url=None) is None
    assert not list(tmp_path.glob("**/kaggle_run.json"))


def test_scheduler_registration_is_off_by_default_and_local_tz(monkeypatch):
    class Scheduler:
        def __init__(self): self.calls = []
        def add_job(self, *args, **kwargs): self.calls.append((args, kwargs)); return "job"
    scheduler = Scheduler()
    monkeypatch.delenv("ENABLE_SERVICE_SHARE_CARD_DAILY", raising=False)
    assert register_service_share_daily_job(scheduler) is None and scheduler.calls == []
    monkeypatch.setenv("ENABLE_SERVICE_SHARE_CARD_DAILY", "1")
    monkeypatch.setenv("SERVICE_SHARE_CARD_TIME_LOCAL", "08:45")
    assert register_service_share_daily_job(scheduler) == "job"
    kwargs = scheduler.calls[0][1]
    assert (kwargs["hour"], kwargs["minute"], str(kwargs["timezone"])) == (8, 45, "Europe/Kaliningrad")


def test_promo_resolver_uses_explicit_targets_and_ignores_all(tmp_path: Path):
    async def run():
        db = Database(str(tmp_path / "promo.sqlite"))
        async with db.raw_conn() as conn:
            await conn.executescript("""
            CREATE TABLE promo_campaign(id INTEGER PRIMARY KEY, status TEXT, priority INT, starts_at TEXT, ends_at TEXT);
            CREATE TABLE promo_activity(id INTEGER PRIMARY KEY, campaign_id INT, surface TEXT, enabled INT, max_per_publish INT);
            CREATE TABLE promo_target(id INTEGER PRIMARY KEY, campaign_id INT, target_type TEXT, event_id INT, festival_name TEXT, query_text TEXT);
            INSERT INTO promo_campaign VALUES(1,'active',5,'2026-07-01T00:00:00Z','2026-08-01T00:00:00Z');
            INSERT INTO promo_activity VALUES(1,1,'service_share_card',1,5);
            INSERT INTO promo_target VALUES(1,1,'all',NULL,NULL,NULL);
            INSERT INTO promo_target VALUES(2,1,'festival',NULL,'Фест',NULL);
            """); await conn.commit()
        snapshot = {"events": [{"id": 1, "festival": "Фест", "image_url": "https://x/1"},
                                {"id": 2, "festival": None, "image_url": "https://x/2"}]}
        rows = await load_active_promo_candidates(db, snapshot=snapshot, measured_at=datetime(2026, 7, 15, tzinfo=timezone.utc))
        await db.close(); return rows
    rows = asyncio.run(run())
    assert [row["event_id"] for row in rows] == [1]
    assert rows[0]["provenance"] == "exact_surface_activity"


def test_snapshot_accepts_approved_managed_poster_without_semantic_classifier_gate(tmp_path: Path):
    async def run():
        db = Database(str(tmp_path / "catalog.sqlite"))
        async with db.raw_conn() as conn:
            await conn.executescript("""
            CREATE TABLE event(
              id INTEGER PRIMARY KEY, title TEXT, date TEXT, end_date TEXT, time TEXT,
              city TEXT, festival TEXT, photo_urls TEXT, added_at TEXT,
              identity_status TEXT, merged_into_event_id INT, silent INT, lifecycle_status TEXT
            );
            CREATE TABLE eventposter(
              id INTEGER PRIMARY KEY, event_id INT, thumbnail_512_url TEXT,
              supabase_url TEXT, thumbnail_256_url TEXT, duplicate_of_id INT,
              review_status TEXT, media_semantic_status TEXT, display_order INT,
              safe_crop INT, image_text_mode TEXT, ocr_text TEXT
            );
            INSERT INTO event VALUES(1,'Будущее событие','2026-07-20',NULL,'19:00',
              'Калининград',NULL,'[]','2026-07-14T12:00:00Z','canonical',NULL,0,'active');
            INSERT INTO event VALUES(2,'Событие с фото','2026-07-21',NULL,'20:00',
              'Калининград',NULL,'[]','2026-07-14T12:00:00Z','canonical',NULL,0,'active');
            INSERT INTO eventposter VALUES(1,1,'https://static.test/512.webp',
              'https://static.test/full.webp',NULL,NULL,'approved','error',0,0,NULL,'АФИША 20 ИЮЛЯ');
            INSERT INTO eventposter VALUES(2,2,'https://static.test/photo-512.webp',
              'https://static.test/photo.webp',NULL,NULL,'approved','error',0,0,NULL,'');
            """)
            await conn.commit()
        snapshot = await load_catalog_snapshot(db, measured_at=datetime(2026, 7, 15, tzinfo=timezone.utc))
        await db.close()
        return snapshot
    snapshot = asyncio.run(run())
    assert snapshot["eligible_event_count"] == 2
    assert snapshot["events"][0]["image_url"] == "https://static.test/full.webp"
    assert snapshot["events"][0]["image_has_ocr_text"] is True
    assert snapshot["events"][1]["image_url"] == "https://static.test/photo.webp"
    assert snapshot["events"][1]["image_has_ocr_text"] is False
