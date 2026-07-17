from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import social_metrics_batch as smb
import social_metrics_kaggle as smk
from db import Database
from models import Event

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


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
        "schema_version": 1,
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
@pytest.mark.parametrize("mutation", ["run", "sha", "unknown", "duplicate", "negative", "future"])
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
    result = {"schema_version": 1, "run_id": manifest["run_id"], "manifest_sha256": manifest["manifest_sha256"], "observations": [observation]}
    if mutation == "run": result["run_id"] = "other"
    elif mutation == "sha": result["manifest_sha256"] = "0" * 64
    elif mutation == "unknown": observation["target_id"] = "telegram:kldevents:999"
    elif mutation == "duplicate": result["observations"].append(dict(observation))
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
