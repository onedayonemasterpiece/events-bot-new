from __future__ import annotations

import math

import pytest
from sqlalchemy import select
import yandex_storage

from db import Database
from models import Event, EventVideoLink, VideoAsset
from source_parsing.telegram.handlers import (
    _extract_message_videos_payload,
    _persist_event_video_assets,
)
from supabase_storage import enqueue_orphan_video_assets, flush_supabase_delete_queue


def _event(title: str, date: str) -> Event:
    return Event(
        title=title,
        description="description",
        date=date,
        time="18:00",
        location_name="venue",
        source_text="source",
    )


@pytest.mark.asyncio
async def test_video_schema_bootstraps_and_init_is_idempotent(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await db.init()

    async with db.raw_conn() as conn:
        video_columns = {
            row[1] for row in await (await conn.execute("PRAGMA table_info(video_asset)")).fetchall()
        }
        link_columns = {
            row[1]
            for row in await (await conn.execute("PRAGMA table_info(event_video_link)")).fetchall()
        }
        indexes = {
            row[1]
            for row in await (await conn.execute("PRAGMA index_list(event_video_link)")).fetchall()
        }

    assert {"sha256", "analysis_status", "cdn_url", "showcase_score", "analysis_json"} <= video_columns
    assert {
        "event_id",
        "video_asset_id",
        "event_relevance_score",
        "ranking_score",
        "match_reason",
        "relation_confidence",
    } <= link_columns
    assert "ix_event_video_link_event_rank" in indexes
    await db.close()


def test_video_payload_accepts_cache_hit_and_keeps_event_specific_relations():
    sha = "a" * 64
    videos, status = _extract_message_videos_payload(
        {
            "video_status": "cache_hit",
            "videos": [
                {
                    "status": "cache_hit",
                    "sha256": sha,
                    "cdn_url": "https://storage.yandexcloud.net/kenigevents/videos/a.mp4",
                    "showcase_score": math.inf,
                    "event_indexes": [0, 1, 2],
                    "event_relevance_score": 99,
                    "relation_confidence": 999,
                    "event_relevance_scores": [
                        {
                            "event_index": 0,
                            "relevance_score": 20,
                            "reason": "background venue",
                            "confidence": 75,
                        },
                        {
                            "event_index": 1,
                            "relevance_score": 80,
                            "reason": "exact performance",
                            "confidence": 90,
                        },
                        {
                            "event_index": 2,
                            "relevance_score": 900,
                            "reason": "invalid score must not become 100",
                        },
                    ],
                }
            ]
        }
    )

    assert status == "accepted"
    assert len(videos) == 1
    assert videos[0]["analysis_status"] == "accepted"
    assert videos[0]["showcase_score"] is None
    assert videos[0]["relation_confidence"] is None
    assert videos[0]["event_relevance_scores"] == [
        {
            "event_index": 0,
            "event_relevance_score": 20.0,
            "match_reason": "background venue",
            "relation_confidence": 75.0,
        },
        {
            "event_index": 1,
            "event_relevance_score": 80.0,
            "match_reason": "exact performance",
            "relation_confidence": 90.0,
        },
    ]


@pytest.mark.asyncio
async def test_same_sha_links_many_events_with_distinct_rank_and_is_idempotent(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        first = _event("first", "2030-01-01")
        second = _event("second", "2030-01-02")
        session.add(first)
        session.add(second)
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
        first_id = int(first.id)
        second_id = int(second.id)

    videos, _ = _extract_message_videos_payload(
        {
            "videos": [
                {
                    "analysis_status": "accepted",
                    "sha256": "b" * 64,
                    "cdn_url": "https://storage.yandexcloud.net/kenigevents/videos/b.mp4",
                    "showcase_score": 80,
                    "event_indexes": [0, 1],
                    "event_relevance_scores": [
                        {"event_index": 0, "relevance_score": 20, "reason": "weak"},
                        {"event_index": 1, "relevance_score": 80, "reason": "exact"},
                    ],
                }
            ]
        }
    )

    result = await _persist_event_video_assets(
        db,
        event_ids_by_index={0: first_id, 1: second_id},
        videos=videos,
        source_url="https://t.me/meowafisha/123",
    )
    repeat = await _persist_event_video_assets(
        db,
        event_ids_by_index={0: first_id, 1: second_id},
        videos=videos,
        source_url="https://t.me/meowafisha/123",
    )

    assert result == (2, 1, {first_id: 1, second_id: 1})
    assert repeat == (0, 1, {})
    async with db.get_session() as session:
        assets = (await session.execute(select(VideoAsset))).scalars().all()
        links = (
            await session.execute(
                select(EventVideoLink).order_by(EventVideoLink.event_id)
            )
        ).scalars().all()
    assert len(assets) == 1
    assert len(links) == 2
    assert [(link.event_relevance_score, link.ranking_score, link.match_reason) for link in links] == [
        (20.0, 65.0, "weak"),
        (80.0, 80.0, "exact"),
    ]
    assert all(link.source_url == "https://t.me/meowafisha/123" for link in links)
    await db.close()


@pytest.mark.asyncio
async def test_orphan_cleanup_waits_for_last_link_and_retains_analysis(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        first = _event("first", "2030-01-01")
        second = _event("second", "2030-01-02")
        asset = VideoAsset(
            sha256="c" * 64,
            analysis_status="accepted",
            cdn_url="https://storage.yandexcloud.net/kenigevents/videos/c.mp4",
            cdn_bucket="kenigevents",
            cdn_path="videos/c.mp4",
            showcase_score=91,
            description="retained exact-SHA analysis",
        )
        session.add(first)
        session.add(second)
        session.add(asset)
        await session.flush()
        session.add(EventVideoLink(event_id=int(first.id), video_asset_id=int(asset.id)))
        session.add(EventVideoLink(event_id=int(second.id), video_asset_id=int(asset.id)))
        await session.commit()
        first_id, second_id, asset_id = int(first.id), int(second.id), int(asset.id)

    async with db.get_session() as session:
        first_link = (
            await session.execute(
                select(EventVideoLink).where(EventVideoLink.event_id == first_id)
            )
        ).scalar_one()
        await session.delete(first_link)
        await session.commit()
    assert await enqueue_orphan_video_assets(db) == 0

    async with db.get_session() as session:
        second_link = (
            await session.execute(
                select(EventVideoLink).where(EventVideoLink.event_id == second_id)
            )
        ).scalar_one()
        await session.delete(second_link)
        await session.commit()
    assert await enqueue_orphan_video_assets(db) == 1
    assert await enqueue_orphan_video_assets(db) == 0

    async with db.get_session() as session:
        retained = await session.get(VideoAsset, asset_id)
    assert retained is not None
    assert retained.analysis_status == "accepted"
    assert retained.showcase_score == 91
    assert retained.description == "retained exact-SHA analysis"
    assert retained.cdn_url is None
    assert retained.cdn_path is None
    assert retained.cdn_bucket is None
    async with db.raw_conn() as conn:
        count = (
            await (
                await conn.execute(
                    "SELECT COUNT(*) FROM supabase_delete_queue "
                    "WHERE bucket='kenigevents' AND path='videos/c.mp4'"
                )
            ).fetchone()
        )[0]
    assert int(count) == 1
    await db.close()


@pytest.mark.asyncio
async def test_same_sha_relink_restores_cdn_and_cancels_delete_intent(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event("relinked", "2030-01-01")
        asset = VideoAsset(
            sha256="f" * 64,
            analysis_status="accepted",
            showcase_score=88,
            description="cached analysis",
        )
        session.add(event)
        session.add(asset)
        await session.commit()
        await session.refresh(event)
        await session.refresh(asset)
        event_id, asset_id = int(event.id), int(asset.id)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO supabase_delete_queue(bucket, path) VALUES(?, ?)",
            ("kenigevents", "videos/f.mp4"),
        )
        await conn.commit()

    inserted, total, by_event = await _persist_event_video_assets(
        db,
        event_ids_by_index={0: event_id},
        videos=[
            {
                "analysis_status": "accepted",
                "sha256": "f" * 64,
                "cdn_url": "https://storage.yandexcloud.net/kenigevents/videos/f.mp4",
                "cdn_bucket": "kenigevents",
                "cdn_path": "videos/f.mp4",
                "showcase_score": 1,  # terminal analysis must not be overwritten
                "event_indexes": [0],
                "event_relevance_score": 80,
            }
        ],
        source_url="https://t.me/meowafisha/999",
    )

    assert (inserted, total, by_event) == (1, 1, {event_id: 1})
    async with db.get_session() as session:
        retained = await session.get(VideoAsset, asset_id)
    assert retained is not None
    assert retained.showcase_score == 88
    assert retained.description == "cached analysis"
    assert retained.cdn_path == "videos/f.mp4"
    async with db.raw_conn() as conn:
        count = (await (await conn.execute("SELECT COUNT(*) FROM supabase_delete_queue")).fetchone())[0]
    assert int(count) == 0
    await db.close()


class _RecordingBucket:
    def __init__(self, calls: list[list[str]]):
        self.calls = calls

    def remove(self, paths: list[str]):
        self.calls.append(list(paths))


class _RecordingStorage:
    def __init__(self, calls: list[list[str]]):
        self.calls = calls

    def from_(self, _bucket: str):
        return _RecordingBucket(self.calls)


class _RecordingClient:
    def __init__(self, calls: list[list[str]]):
        self.storage = _RecordingStorage(calls)


@pytest.mark.asyncio
async def test_queue_flush_cancels_stale_delete_for_relinked_video(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event("event", "2030-01-01")
        asset = VideoAsset(
            sha256="d" * 64,
            analysis_status="accepted",
            cdn_url="https://example.supabase.co/storage/v1/object/public/media/videos/d.mp4",
            cdn_bucket="media",
            cdn_path="videos/d.mp4",
        )
        session.add(event)
        session.add(asset)
        await session.flush()
        session.add(EventVideoLink(event_id=int(event.id), video_asset_id=int(asset.id)))
        await session.commit()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO supabase_delete_queue(bucket, path) VALUES(?, ?)",
            ("media", "videos/d.mp4"),
        )
        await conn.commit()

    calls: list[list[str]] = []
    removed = await flush_supabase_delete_queue(
        db, supabase_client=_RecordingClient(calls), limit=100
    )

    assert removed == 0
    assert calls == []
    async with db.raw_conn() as conn:
        count = (await (await conn.execute("SELECT COUNT(*) FROM supabase_delete_queue")).fetchone())[0]
    assert int(count) == 0
    await db.close()


@pytest.mark.asyncio
async def test_queue_flush_deletes_orphan_binary_but_retains_sha_row(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        asset = VideoAsset(
            sha256="e" * 64,
            analysis_status="accepted",
            cdn_url="https://example.supabase.co/storage/v1/object/public/media/videos/e.mp4",
            cdn_bucket="media",
            cdn_path="videos/e.mp4",
            showcase_score=84,
        )
        session.add(asset)
        await session.commit()
        await session.refresh(asset)
        asset_id = int(asset.id)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO supabase_delete_queue(bucket, path) VALUES(?, ?)",
            ("media", "videos/e.mp4"),
        )
        await conn.commit()

    calls: list[list[str]] = []
    removed = await flush_supabase_delete_queue(
        db, supabase_client=_RecordingClient(calls), limit=100
    )

    assert removed == 1
    assert calls == [["videos/e.mp4"]]
    async with db.get_session() as session:
        retained = await session.get(VideoAsset, asset_id)
    assert retained is not None
    assert retained.sha256 == "e" * 64
    assert retained.showcase_score == 84
    assert retained.cdn_url is None
    assert retained.cdn_path is None
    assert retained.cdn_bucket is None
    await db.close()


@pytest.mark.asyncio
async def test_yandex_rows_flush_without_supabase_even_after_older_supabase_row(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setenv("YC_STORAGE_BUCKET", "kenigevents")
    monkeypatch.setenv("YC_SA_BOT_STORAGE", "access")
    monkeypatch.setenv("YC_SA_BOT_STORAGE_KEY", "secret")
    monkeypatch.setattr(yandex_storage, "get_yandex_storage_client", lambda: object())
    monkeypatch.setattr(
        yandex_storage,
        "delete_yandex_objects",
        lambda *, bucket, object_paths, client=None: calls.append(
            (bucket, list(object_paths))
        ),
    )
    async with db.raw_conn() as conn:
        await conn.executemany(
            "INSERT INTO supabase_delete_queue(bucket, path) VALUES(?, ?)",
            [
                ("legacy-supabase", "old/blocked.mp4"),
                ("kenigevents", "videos/yandex.mp4"),
            ],
        )
        await conn.commit()

    removed = await flush_supabase_delete_queue(
        db, supabase_client=None, limit=100
    )

    assert removed == 1
    assert calls == [("kenigevents", ["videos/yandex.mp4"])]
    async with db.raw_conn() as conn:
        rows = await (
            await conn.execute(
                "SELECT bucket, path, attempts FROM supabase_delete_queue ORDER BY id"
            )
        ).fetchall()
    assert rows == [("legacy-supabase", "old/blocked.mp4", 1)]
    await db.close()
