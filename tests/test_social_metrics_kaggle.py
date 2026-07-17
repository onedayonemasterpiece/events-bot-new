from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import social_metrics_batch as smb
import social_metrics_kaggle as smk
from kaggle.SocialMetricsCollector import social_metrics_collector as collector
from db import Database
from models import Event

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def test_kaggle_vk_matcher_keeps_strict_title_date_anchor_contract():
    candidate = {
        "title": "Событие 1",
        "date": "2026-07-20",
        "time": "19:00",
        "location_name": "Площадка",
    }
    assert collector._match_post(candidate, {
        "text": "Событие 1\n20 июля 19:00\nПлощадка",
    }) == (4, 1.0)
    assert collector._match_post(candidate, {
        "text": "Событие 1\n19:00\nПлощадка",
    }) == (0, 0.0)
    assert collector._match_post(candidate, {
        "text": "Другое событие\n20 июля 19:00\nПлощадка",
    }) == (0, 0.0)


def _event(event_id: int, message_id: int) -> Event:
    return Event(
        id=event_id,
        title=f"Событие {event_id}",
        description="Описание",
        date="2026-07-20",
        time="19:00",
        location_name="Площадка",
        source_text="Источник",
        tg_event_post_id=message_id,
        tg_event_post_url=f"https://t.me/kldevents/{message_id}",
        lifecycle_status="active",
        identity_status="canonical",
        silent=False,
    )


@pytest.mark.asyncio
async def test_manifest_is_exact_id_only_and_import_recomputes_buckets(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(_event(1, 701))
        await session.commit()

    manifest = await smb.build_social_metrics_manifest(db, run_id="social-metrics:test", now_utc=NOW)
    assert [(row["publisher_id"], row["post_id"], row["publication_kind"]) for row in manifest["targets"]] == [
        ("kldevents", 701, "event_announcement")
    ]
    observed = int(datetime.now(timezone.utc).timestamp())
    post_ts = observed - 8 * 60 * 60
    result = {
        "schema_version": 2,
        "run_id": manifest["run_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "observations": [{
            "target_id": "telegram:kldevents:701",
            "observed_ts": observed,
            "status": "collected",
            "post_ts": post_ts,
            "views": 0,
            "likes": None,
            "comments": 2,
            "shares": 3,
            "reactions": {},
        }],
    }
    imported = await smb.import_social_metrics_result(db, manifest=manifest, result=result)
    assert imported == {"collected": 1, "not_found": 0, "error": 0, "skipped_late": 1}
    async with db.raw_conn() as conn:
        rows = await (await conn.execute(
            "SELECT age_bucket,status,views,likes,comments,shares FROM social_metric_snapshot ORDER BY age_bucket"
        )).fetchall()
    assert [tuple(row) for row in rows] == [
        ("1h", "skipped_late", None, None, None, None),
        ("6h", "collected", 0, None, 2, 3),
    ]

    # Re-importing a downloaded artifact performs no second bucket write.
    repeated = await smb.import_social_metrics_result(db, manifest=manifest, result=result)
    assert repeated == {"collected": 0, "not_found": 0, "error": 0, "skipped_late": 0}
    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["run", "sha", "unknown", "duplicate", "missing", "negative", "future"])
async def test_import_rejects_tampered_result_before_writes(tmp_path, mutation):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(_event(1, 702))
        await session.commit()
    manifest = await smb.build_social_metrics_manifest(db, run_id="social-metrics:test2", now_utc=NOW)
    observation = {
        "target_id": "telegram:kldevents:702", "observed_ts": int(datetime.now(timezone.utc).timestamp()),
        "status": "collected", "post_ts": int(NOW.timestamp()) - 7200,
        "views": 1, "likes": 2, "comments": None, "shares": 0, "reactions": {},
    }
    result = {"schema_version": 2, "run_id": manifest["run_id"], "manifest_sha256": manifest["manifest_sha256"], "observations": [observation]}
    if mutation == "run": result["run_id"] = "other"
    elif mutation == "sha": result["manifest_sha256"] = "0" * 64
    elif mutation == "unknown": observation["target_id"] = "telegram:kldevents:999"
    elif mutation == "duplicate": result["observations"].append(dict(observation))
    elif mutation == "missing": result["observations"] = []
    elif mutation == "negative": observation["views"] = -1
    elif mutation == "future": observation["observed_ts"] = int(datetime.now(timezone.utc).timestamp()) + 3600
    with pytest.raises(ValueError):
        await smb.import_social_metrics_result(db, manifest=manifest, result=result)
    async with db.raw_conn() as conn:
        count = (await (await conn.execute("SELECT COUNT(*) FROM social_metric_snapshot")).fetchone())[0]
    assert count == 0
    await db.close()


def test_kaggle_secret_payload_never_borrows_other_telegram_sessions(monkeypatch):
    monkeypatch.delenv("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR", raising=False)
    monkeypatch.setenv("TELEGRAM_AUTH_BUNDLE_E2E", "forbidden")
    monkeypatch.setenv("TELEGRAM_AUTH_BUNDLE_S22", "forbidden")
    monkeypatch.setenv("TELEGRAM_SESSION", "forbidden")
    monkeypatch.setenv("TG_API_ID", "1")
    monkeypatch.setenv("TG_API_HASH", "hash")
    with pytest.raises(RuntimeError, match="dedicated"):
        smk._secret_payload(True, False)


@pytest.mark.asyncio
async def test_kaggle_vk_resolution_is_revalidated_and_imported_with_metrics(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    event = _event(1, 703)
    event.source_vk_post_url = "https://vk.com/wall-231920894_1001"
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    manifest = await smb.build_social_metrics_manifest(db, run_id="social-metrics:resolve", now_utc=NOW)
    assert [row["candidate_id"] for row in manifest["vk_resolve_candidates"]] == [
        "vkresolve:klgdevents:1"
    ]
    observed = int(datetime.now(timezone.utc).timestamp())
    observation = {
        "target_id": "telegram:kldevents:703",
        "observed_ts": observed,
        "status": "error",
        "error_code": "test",
    }
    base = {
        "schema_version": 2,
        "run_id": manifest["run_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "observations": [observation],
        "vk_resolutions": [],
    }
    with pytest.raises(ValueError, match="incomplete VK resolution coverage"):
        await smb.import_social_metrics_result(db, manifest=manifest, result=base)

    post_ts = observed - 90 * 60
    result = dict(base)
    result["vk_resolutions"] = [{
        "candidate_id": "vkresolve:klgdevents:1",
        "status": "published",
        "observed_ts": observed,
        "live_post_id": 2001,
        "post_ts": post_ts,
        "match_method": "wall_scan",
        "match_confidence": 1.0,
        "evidence_text": "Событие 1\n20 июля 19:00\nПлощадка",
        "views": 500,
        "likes": 20,
        "comments": 3,
        "shares": 4,
    }]
    imported = await smb.import_social_metrics_result(db, manifest=manifest, result=result)
    assert imported["resolved_published"] == 1
    async with db.raw_conn() as conn:
        publication = await (await conn.execute(
            "SELECT live_post_id,status,match_method FROM event_publication WHERE event_id=1"
        )).fetchone()
        snapshot = await (await conn.execute(
            "SELECT age_bucket,views,likes,comments,shares FROM social_metric_snapshot WHERE platform='vk'"
        )).fetchone()
    assert tuple(publication) == (2001, "published", "wall_scan")
    assert tuple(snapshot) == ("1h", 500, 20, 3, 4)
    await db.close()


@pytest.mark.asyncio
async def test_failed_launch_cleans_created_datasets_and_marks_ledger_terminal(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    manifest = {
        "schema_version": 2,
        "run_id": "placeholder",
        "generated_at": NOW.isoformat(),
        "targets": [{
            "target_id": "vk:231920894:1",
            "platform": "vk",
            "publisher_id": "231920894",
            "post_id": 1,
        }],
        "vk_resolve_candidates": [],
        "vk_wall_scan_limit": 100,
    }

    async def fake_manifest(_db, *, run_id):
        value = dict(manifest)
        value["run_id"] = run_id
        value["manifest_sha256"] = smb._manifest_digest(value)
        return value

    cleaned: list[str] = []

    async def fake_to_thread(func, *args, **kwargs):
        if func is smk._launch_sync:
            kwargs["refs"].append("user/private-input")
            raise RuntimeError("launch failed")
        if func is smk._cleanup_sync:
            cleaned.extend(args[0])
            return None
        raise AssertionError(func)

    monkeypatch.setenv("ENABLE_SOCIAL_METRICS_KAGGLE", "1")
    monkeypatch.setenv("KAGGLE_USERNAME", "user")
    monkeypatch.setenv("VK_TOKEN", "test")
    monkeypatch.setenv("KAGGLE_STATUS_CALLBACK_URL", "https://example.test/internal/kaggle/run-event")
    monkeypatch.setattr(smk, "build_social_metrics_manifest", fake_manifest)
    monkeypatch.setattr(smk.asyncio, "to_thread", fake_to_thread)
    with pytest.raises(RuntimeError, match="launch failed"):
        await smk.run_social_metrics_kaggle_batch(db)
    assert cleaned == ["user/private-input"]
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT status,phase,error FROM kaggle_run_ledger WHERE kind='social_metrics_collector'"
        )).fetchone()
    assert row[0:2] == ("error", "server_launch_failed")
    assert "launch failed" in row[2]
    await db.close()
