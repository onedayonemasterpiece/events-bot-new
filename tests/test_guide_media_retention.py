from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from db import Database
from guide_excursions.media_retention import (
    GuideMediaRetentionPolicy,
    prune_guide_media_store,
)


async def _create_retention_schema(db: Database) -> None:
    async with db.raw_conn() as conn:
        await conn.executescript(
            """
            CREATE TABLE guide_monitor_post(
                id INTEGER PRIMARY KEY,
                post_date TEXT,
                media_refs_json TEXT,
                media_assets_json TEXT
            );
            CREATE TABLE guide_occurrence(
                id INTEGER PRIMARY KEY,
                date TEXT
            );
            CREATE TABLE guide_occurrence_source(
                occurrence_id INTEGER,
                post_id INTEGER
            );
            CREATE TABLE guide_digest_issue(
                id INTEGER PRIMARY KEY,
                family TEXT,
                status TEXT,
                media_items_json TEXT,
                created_at TEXT,
                published_at TEXT
            );
            """
        )
        await conn.commit()


def _asset(path) -> str:
    return json.dumps([{"kind": "photo", "path": str(path)}])


def _refs(label: str) -> str:
    return json.dumps([{"kind": "photo", "label": label}])


def _old(path, now: datetime, *, days: int = 30, hours: int = 0) -> None:
    timestamp = (now - timedelta(days=days, hours=hours)).timestamp()
    os.utime(path, (timestamp, timestamp), follow_symlinks=False)


@pytest.mark.asyncio
async def test_prune_protects_live_refs_and_repairs_only_deleted_or_missing_assets(tmp_path):
    db = Database(str(tmp_path / "guide.sqlite"))
    await _create_retention_schema(db)
    root = tmp_path / "guide_media"
    root.mkdir()
    now = datetime(2026, 7, 13, 8, tzinfo=timezone.utc)

    future = root / "future.jpg"
    recent = root / "recent.jpg"
    digest = root / "digest.jpg"
    historical = root / "historical.jpg"
    orphan = root / "orphan.jpg"
    young = root / "young.jpg"
    missing = root / "already-missing.jpg"
    outside = tmp_path / "outside.jpg"
    for path in (future, recent, digest, historical, orphan, young):
        path.write_bytes(path.name.encode())
    outside.write_bytes(b"outside")
    for path in (future, recent, digest, historical, orphan):
        _old(path, now)
    _old(young, now, days=1)
    symlink = root / "unknown-link"
    symlink.symlink_to(orphan)

    async with db.raw_conn() as conn:
        await conn.executemany(
            "INSERT INTO guide_monitor_post(id, post_date, media_refs_json, media_assets_json) VALUES(?,?,?,?)",
            [
                (1, "2026-06-01 10:00:00", _refs("future"), _asset(future)),
                (2, "2026-07-10 10:00:00", _refs("recent"), _asset(recent)),
                (3, "2026-06-01 10:00:00", _refs("historical"), _asset(historical)),
                (4, "2026-06-01 10:00:00", _refs("missing"), _asset(missing)),
                (5, "2026-06-01 10:00:00", _refs("outside"), _asset(outside)),
            ],
        )
        await conn.execute("INSERT INTO guide_occurrence(id, date) VALUES(1, '2026-07-13')")
        await conn.execute("INSERT INTO guide_occurrence_source(occurrence_id, post_id) VALUES(1,1)")
        media_items = json.dumps([{"media_asset": {"kind": "photo", "path": str(digest)}}])
        await conn.execute(
            """
            INSERT INTO guide_digest_issue(
                id, family, status, media_items_json, created_at, published_at
            ) VALUES(1, 'new', 'published', ?, '2026-07-12', '2026-07-12')
            """,
            (media_items,),
        )
        await conn.commit()

    policy = GuideMediaRetentionPolicy(
        retention_days=14,
        recent_post_grace_days=14,
        max_total_bytes=1024 * 1024,
        min_free_bytes=0,
        max_delete_files=100,
        max_delete_bytes=1024 * 1024,
    )
    preview = await prune_guide_media_store(
        db,
        root=root,
        reason="test_preview",
        dry_run=True,
        policy=policy,
        now=now,
    )
    assert preview.planned_delete_files == 2
    assert preview.planned_db_rows_repaired == 2
    assert preview.ignored_reference_paths == 1
    assert historical.exists() and orphan.exists() and missing.exists() is False
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT media_assets_json FROM guide_monitor_post WHERE id=4")
        assert "already-missing.jpg" in (await cur.fetchone())[0]

    applied = await prune_guide_media_store(
        db,
        root=root,
        reason="test_apply",
        policy=policy,
        now=now,
    )
    assert applied.deleted_files == 2
    assert applied.db_rows_repaired == 2
    assert applied.db_assets_removed == 2
    assert applied.db_refs_removed == 2
    assert not historical.exists()
    assert not orphan.exists()
    assert future.exists()
    assert recent.exists()
    assert digest.exists()
    assert young.exists()
    assert outside.exists()
    assert symlink.is_symlink()
    assert applied.skipped_non_regular == 1
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT id, media_refs_json, media_assets_json FROM guide_monitor_post ORDER BY id"
        )
        rows = await cur.fetchall()
    assert json.loads(rows[0][2])[0]["path"] == str(future)
    assert json.loads(rows[1][2])[0]["path"] == str(recent)
    assert json.loads(rows[2][1]) == [] and json.loads(rows[2][2]) == []
    assert json.loads(rows[3][1]) == [] and json.loads(rows[3][2]) == []
    assert json.loads(rows[4][2])[0]["path"] == str(outside)
    await db.close()


@pytest.mark.asyncio
async def test_prune_uses_short_carousel_retention_and_bounds_oldest_deletes(tmp_path):
    db = Database(str(tmp_path / "guide.sqlite"))
    await _create_retention_schema(db)
    root = tmp_path / "guide_media"
    old_preview = root / "_digest_carousel" / "10" / "slide_0.jpg"
    recent_preview = root / "_digest_carousel" / "11" / "slide_0.jpg"
    old_published = root / "_digest_carousel" / "12" / "slide_0.jpg"
    old_preview.parent.mkdir(parents=True)
    recent_preview.parent.mkdir(parents=True)
    old_published.parent.mkdir(parents=True)
    for path in (old_preview, recent_preview, old_published):
        path.write_bytes(b"card")
    now = datetime(2026, 7, 13, 8, tzinfo=timezone.utc)
    _old(old_preview, now, days=1, hours=2)
    _old(recent_preview, now, days=0, hours=12)
    _old(old_published, now, days=8)
    async with db.raw_conn() as conn:
        await conn.executemany(
            """
            INSERT INTO guide_digest_issue(
                id, family, status, media_items_json, created_at, published_at
            ) VALUES(?,?,?,?,?,?)
            """,
            [
                (10, "a", "preview", "[]", "2026-07-12", None),
                (11, "b", "partial", "[]", "2026-07-13", None),
                (12, "c", "published", "[]", "2026-07-05", "2026-07-05"),
            ],
        )
        await conn.commit()
    policy = GuideMediaRetentionPolicy(
        retention_days=14,
        max_total_bytes=1024 * 1024,
        min_free_bytes=0,
        max_delete_files=1,
        max_delete_bytes=1024 * 1024,
        preview_carousel_retention_hours=24,
        published_carousel_retention_hours=7 * 24,
    )
    result = await prune_guide_media_store(
        db,
        root=root,
        reason="carousel",
        policy=policy,
        now=now,
    )
    assert result.candidate_files == 2
    assert result.deleted_files == 1
    assert result.bounded is True
    assert not old_published.exists()  # oldest candidate wins
    assert old_preview.exists()
    assert recent_preview.exists()
    await db.close()


@pytest.mark.asyncio
async def test_results_import_runs_retention_before_and_after_even_on_failure(monkeypatch, tmp_path):
    from guide_excursions import service

    calls: list[str] = []

    class FakeResult:
        def __init__(self, reason: str):
            self.reason = reason

        def as_dict(self):
            return {"reason": self.reason}

    async def fake_prune(db, *, root, reason):
        calls.append(reason)
        return FakeResult(reason)

    async def failing_inner(db, *, results_path):
        raise RuntimeError("broken import")

    monkeypatch.setattr(service, "prune_guide_media_store", fake_prune)
    monkeypatch.setattr(service, "_import_results_file_inner", failing_inner)
    with pytest.raises(RuntimeError, match="broken import"):
        await service._import_results_file(
            object(),
            results_path=str(tmp_path / "result.json"),
        )
    assert calls == ["before_results_import", "after_results_import"]
