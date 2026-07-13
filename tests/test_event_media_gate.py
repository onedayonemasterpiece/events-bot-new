from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sqlite3
from pathlib import Path

import pytest
from PIL import Image, PngImagePlugin
from sqlmodel import select

from db import Database
import event_media
from event_media import (
    APPROVED,
    DUPLICATE,
    PENDING_REVIEW,
    DownloadedPoster,
    get_event_gallery_urls,
    review_next_event_media_pair,
)
from media_dedup import compute_image_fingerprints
from models import Event, EventMediaPairReview, EventPoster, JobOutbox, JobTask
from smart_event_update import PosterCandidate, _apply_posters


def _png_bytes(*, metadata: str) -> bytes:
    image = Image.new("RGB", (64, 48), (20, 100, 180))
    info = PngImagePlugin.PngInfo()
    info.add_text("note", metadata)
    out = io.BytesIO()
    image.save(out, format="PNG", pnginfo=info)
    return out.getvalue()


def _pattern_png_bytes(*, invert: bool = False) -> bytes:
    image = Image.new("RGB", (64, 48), "white" if invert else "black")
    for x in range(32):
        for y in range(48):
            image.putpixel((x, y), (0, 0, 0) if invert else (255, 255, 255))
    out = io.BytesIO()
    image.save(out, format="PNG")
    return out.getvalue()


def _event() -> Event:
    return Event(
        title="Тестовое событие",
        description="Описание",
        date="2026-08-01",
        time="18:00",
        location_name="Площадка",
        source_text="Источник",
        photo_urls=[],
        photo_count=0,
    )


def test_fingerprints_distinguish_raw_container_but_match_pixels() -> None:
    left = _png_bytes(metadata="left")
    right = _png_bytes(metadata="right")
    left_fp = compute_image_fingerprints(left)
    right_fp = compute_image_fingerprints(right)

    assert left_fp is not None and right_fp is not None
    assert left_fp.raw_sha256 != right_fp.raw_sha256
    assert left_fp.pixel_sha256 == right_fp.pixel_sha256
    assert left_fp.dhash_hex == right_fp.dhash_hex
    assert len(left_fp.phash_hex) == 64


@pytest.mark.asyncio
async def test_smart_update_quarantines_second_image_and_projects_only_approved(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)

        added, _urls, _preview, _rejected, changed = await _apply_posters(
            session,
            event.id,
            [
                PosterCandidate(supabase_url="https://static.example/one.webp", sha256="1" * 64),
                PosterCandidate(supabase_url="https://static.example/two.webp", sha256="2" * 64),
            ],
        )
        await session.commit()
        await session.refresh(event)
        rows = list(
            (
                await session.execute(
                    select(EventPoster).where(EventPoster.event_id == event.id).order_by(EventPoster.id)
                )
            ).scalars().all()
        )
        reviews = list(
            (
                await session.execute(
                    select(EventMediaPairReview).where(EventMediaPairReview.event_id == event.id)
                )
            ).scalars().all()
        )
        jobs = list(
            (
                await session.execute(
                    select(JobOutbox).where(
                        JobOutbox.event_id == event.id,
                        JobOutbox.task == JobTask.event_media_review,
                    )
                )
            ).scalars().all()
        )

    assert added == 2
    assert changed is True
    assert [row.review_status for row in rows] == [APPROVED, PENDING_REVIEW]
    assert event.photo_urls == ["https://static.example/one.webp"]
    assert event.photo_count == 1
    assert len(reviews) == 1
    assert len(jobs) == 1


@pytest.mark.asyncio
async def test_review_frontier_does_not_eagerly_create_pending_pair_matrix(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        await _apply_posters(
            session,
            event.id,
            [
                PosterCandidate(supabase_url=f"https://static.example/{idx}.webp", sha256=str(idx) * 64)
                for idx in (1, 2, 3)
            ],
        )
        await session.commit()
        reviews = list(
            (
                await session.execute(
                    select(EventMediaPairReview).where(EventMediaPairReview.event_id == event.id)
                )
            ).scalars().all()
        )
    assert len(reviews) == 1


@pytest.mark.asyncio
async def test_exact_content_reimport_merges_without_new_review(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    digest = hashlib.sha256(b"same").hexdigest()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        await _apply_posters(
            session,
            event.id,
            [PosterCandidate(catbox_url="https://cdn.example/source.jpg", sha256=digest)],
        )
        await _apply_posters(
            session,
            event.id,
            [PosterCandidate(supabase_url="https://static.example/managed.webp", sha256=digest)],
        )
        await session.commit()
        rows = list(
            (
                await session.execute(select(EventPoster).where(EventPoster.event_id == event.id))
            ).scalars().all()
        )
        reviews = list(
            (
                await session.execute(
                    select(EventMediaPairReview).where(EventMediaPairReview.event_id == event.id)
                )
            ).scalars().all()
        )

    assert len(rows) == 1
    assert rows[0].review_status == APPROVED
    assert rows[0].raw_sha256 == digest
    assert rows[0].supabase_url == "https://static.example/managed.webp"
    assert reviews == []


@pytest.mark.asyncio
async def test_pending_and_duplicate_rows_never_leak_through_gallery_resolver(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        session.add_all(
            [
                EventPoster(
                    event_id=event.id,
                    poster_hash="a",
                    supabase_url="https://static.example/a.webp",
                    review_status=APPROVED,
                ),
                EventPoster(
                    event_id=event.id,
                    poster_hash="b",
                    supabase_url="https://static.example/b.webp",
                    review_status=PENDING_REVIEW,
                ),
                EventPoster(
                    event_id=event.id,
                    poster_hash="c",
                    supabase_url="https://static.example/c.webp",
                    review_status=DUPLICATE,
                ),
            ]
        )
        await session.commit()
        urls = await get_event_gallery_urls(session, event.id, legacy_fallback=False)

    assert urls == ["https://static.example/a.webp"]


@pytest.mark.asyncio
async def test_legacy_fallback_cannot_republish_quarantined_ledger(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        event.photo_urls = ["https://legacy.example/must-not-leak.jpg"]
        event.photo_count = 1
        session.add(event)
        await session.commit()
        await session.refresh(event)
        session.add(
            EventPoster(
                event_id=event.id,
                poster_hash="pending-only",
                supabase_url="https://static.example/pending.webp",
                review_status=PENDING_REVIEW,
            )
        )
        await session.commit()
        urls = await get_event_gallery_urls(session, event.id, legacy_fallback=True)
    assert urls == []


@pytest.mark.asyncio
async def test_exact_pixel_duplicate_is_resolved_without_provider_call(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    payloads = {
        "https://static.example/a.png": _png_bytes(metadata="left"),
        "https://static.example/b.png": _png_bytes(metadata="right"),
    }

    async def fake_download(poster: EventPoster) -> DownloadedPoster:
        url = str(poster.supabase_url)
        return DownloadedPoster(payloads[url], "image/png", url)

    async def forbidden_provider(**_kwargs):
        raise AssertionError("pixel-exact duplicates must not spend an LLM call")

    monkeypatch.setattr(event_media, "_download_poster", fake_download)
    monkeypatch.setattr(event_media, "_call_reviewer", forbidden_provider)
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)
        await _apply_posters(
            session,
            event_id,
            [
                PosterCandidate(supabase_url="https://static.example/a.png", sha256="a" * 64),
                PosterCandidate(supabase_url="https://static.example/b.png", sha256="b" * 64),
            ],
        )
        await session.commit()

    assert await review_next_event_media_pair(event_id, db) is False
    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(EventPoster).where(EventPoster.event_id == event_id).order_by(EventPoster.id)
                )
            ).scalars().all()
        )
        review = (
            await session.execute(
                select(EventMediaPairReview).where(EventMediaPairReview.event_id == event_id)
            )
        ).scalar_one()
        event = await session.get(Event, event_id)

    assert [row.review_status for row in rows] == [APPROVED, DUPLICATE]
    assert review.status == "resolved"
    assert review.decision == "duplicate"
    assert review.reason_code == "pixel_sha256_equal"
    assert review.provider_calls == 0
    assert event.photo_urls == ["https://static.example/a.png"]


@pytest.mark.asyncio
async def test_semantic_conflict_rejects_candidate_without_manual_queue(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    payloads = {
        "https://static.example/right.png": _pattern_png_bytes(invert=False),
        "https://static.example/wrong.png": _pattern_png_bytes(invert=True),
    }

    async def fake_download(poster: EventPoster) -> DownloadedPoster:
        url = str(poster.supabase_url)
        return DownloadedPoster(payloads[url], "image/png", url)

    async def semantic_reviewer(**_kwargs):
        return (
            {
                "decision": "distinct",
                "duplicate_kind": "none",
                "confidence": 0.99,
                "semantic_conflict": True,
                "canonical_side": "left",
                "reason_code": "right_unrelated_to_event",
            },
            1,
        )

    monkeypatch.setattr(event_media, "_download_poster", fake_download)
    monkeypatch.setattr(event_media, "_call_reviewer", semantic_reviewer)
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)
        await _apply_posters(
            session,
            event_id,
            [
                PosterCandidate(supabase_url="https://static.example/right.png", sha256="a" * 64),
                PosterCandidate(supabase_url="https://static.example/wrong.png", sha256="b" * 64),
            ],
        )
        await session.commit()

    await review_next_event_media_pair(event_id, db)
    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(EventPoster).where(EventPoster.event_id == event_id).order_by(EventPoster.id)
                )
            ).scalars().all()
        )
        event = await session.get(Event, event_id)
    assert [row.review_status for row in rows] == [APPROVED, "rejected"]
    assert event.photo_urls == ["https://static.example/right.png"]


@pytest.mark.asyncio
async def test_review_status_migration_is_one_time_and_restart_safe(tmp_path) -> None:
    path = tmp_path / "legacy.sqlite"
    con = sqlite3.connect(path)
    con.execute(
        """
        create table eventposter(
            id integer primary key,
            event_id integer not null,
            catbox_url text,
            poster_hash text not null,
            updated_at timestamp
        )
        """
    )
    con.execute(
        "insert into eventposter(id, event_id, catbox_url, poster_hash, updated_at) values(1, 1, 'u', 'h', CURRENT_TIMESTAMP)"
    )
    con.commit()
    con.close()

    db = Database(str(path))
    await db.init()
    con = sqlite3.connect(path)
    assert con.execute("select review_status from eventposter where id=1").fetchone()[0] == APPROVED
    con.execute("update eventposter set review_status=? where id=1", (PENDING_REVIEW,))
    con.commit()
    con.close()

    await db.init()
    con = sqlite3.connect(path)
    assert con.execute("select review_status from eventposter where id=1").fetchone()[0] == PENDING_REVIEW
    con.close()


def test_production_media_writers_are_restricted_to_the_gate() -> None:
    root = Path(__file__).resolve().parents[1]
    allowed = {
        root / "smart_event_update.py",
        root / "event_media.py",
        root / "scripts" / "apply_event_media_audit_cleanup.py",
        root / "scripts" / "backfill_catbox_posters_to_yandex.py",
        root / "scripts" / "stage_event_media_review_backfill.py",
    }
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if any(part in {".git", ".venv", "tests", "alembic"} for part in path.parts):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "EventPoster(" in text and path not in allowed and path.name != "models.py":
            offenders.append(str(path.relative_to(root)))
    assert offenders == []


@pytest.mark.asyncio
async def test_backfill_stages_legacy_multi_image_event_without_deleting_evidence(tmp_path) -> None:
    db_path = tmp_path / "db.sqlite"
    db = Database(str(db_path))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        event.date = "2026-08-01"
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)
        session.add_all(
            [
                EventPoster(
                    event_id=event_id,
                    poster_hash="legacy-a",
                    supabase_url="https://static.example/a.webp",
                    review_status=APPROVED,
                ),
                EventPoster(
                    event_id=event_id,
                    poster_hash="legacy-b",
                    supabase_url="https://static.example/b.webp",
                    review_status=APPROVED,
                ),
            ]
        )
        event.photo_urls = ["https://static.example/a.webp", "https://static.example/b.webp"]
        event.photo_count = 2
        session.add(event)
        await session.commit()

    script = Path(__file__).resolve().parents[1] / "scripts" / "stage_event_media_review_backfill.py"
    spec = importlib.util.spec_from_file_location("stage_event_media_review_backfill", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    result = module.stage(con, current_date="2026-07-13", apply=True)
    con.commit()
    statuses = con.execute(
        "select review_status from eventposter where event_id=? order by id", (event_id,)
    ).fetchall()
    projection = con.execute(
        "select photo_urls, photo_count from event where id=?", (event_id,)
    ).fetchone()
    jobs = con.execute(
        "select task, next_run_at from joboutbox where event_id=? and status='pending'", (event_id,)
    ).fetchall()
    backup_count = con.execute(
        f"select count(*) from {module.BACKUP_PREFIX}_eventposter where event_id=?",
        (event_id,),
    ).fetchone()[0]
    con.close()

    assert result["staged_events"] == 1
    assert [row[0] for row in statuses] == [APPROVED, PENDING_REVIEW]
    assert json.loads(projection[0]) == ["https://static.example/a.webp"]
    assert projection[1] == 1
    job_tasks = {row[0] for row in jobs}
    assert all("T" not in str(row[1]) for row in jobs)
    assert "event_media_review" in job_tasks
    assert "static_site_build" in job_tasks
    assert not {"telegraph_build", "vk_sync", "tg_event_publish"} & job_tasks
    assert result["public_projection_changed_events"] == 1
    assert result["public_rebuild_jobs"] == {
        "telegraph_build": 0,
        "vk_sync": 0,
        "tg_event_publish": 0,
        "static_site_build": 1,
    }
    assert backup_count == 2

    # Once the automatic reviewer has accepted the pending row, a recurring
    # backfill pass must not quarantine that decision again.
    con = sqlite3.connect(db_path)
    con.execute(
        "update eventposter set review_status='approved', review_reason='automated_pair_distinct', reviewed_at=CURRENT_TIMESTAMP where event_id=? and review_status='pending_review'",
        (event_id,),
    )
    con.commit()
    con.row_factory = sqlite3.Row
    rerun = module.stage(con, current_date="2026-07-13", apply=False)
    con.close()
    assert rerun["staged_events"] == 0
