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
async def test_current_yandex_bucket_url_is_canonicalized_to_cdn(monkeypatch) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    candidate = PosterCandidate(
        supabase_url=(
            "https://storage.yandexcloud.net/kenigevents.ru/"
            "p/dh16/aa/abcdef.webp"
        )
    )

    assert await event_media.materialize_event_media_candidate_to_cdn(candidate)
    assert candidate.supabase_url == (
        "https://static.kenigevents.ru/p/dh16/aa/abcdef.webp"
    )
    assert candidate.supabase_path == "p/dh16/aa/abcdef.webp"


@pytest.mark.asyncio
async def test_cdn_retry_preserves_unique_raw_sha_survivor(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    source_bytes = _pattern_png_bytes()
    digest = hashlib.sha256(source_bytes).hexdigest()

    def download(_url: str, *, max_bytes: int, timeout: float) -> DownloadedPoster:
        del max_bytes, timeout
        return DownloadedPoster(
            data=source_bytes,
            mime_type="image/png",
            source_url="https://source.example/duplicate.png",
        )

    monkeypatch.setattr(event_media, "_download_url", download)
    monkeypatch.setattr(
        "yandex_storage.upload_yandex_public_bytes",
        lambda _data, *, object_path, content_type, **_kwargs: (
            f"https://static.kenigevents.ru/{object_path}"
        ),
    )

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        survivor = EventPoster(
            event_id=int(event.id),
            poster_hash="survivor",
            supabase_url="https://static.kenigevents.ru/p/dh16/aa/survivor.webp",
            raw_sha256=digest,
            review_status=APPROVED,
            display_order=0,
        )
        retry = EventPoster(
            event_id=int(event.id),
            poster_hash="retry",
            catbox_url="https://source.example/duplicate.png",
            review_status=PENDING_REVIEW,
            display_order=1,
        )
        session.add(survivor)
        session.add(retry)
        await session.commit()
        await session.refresh(retry)

        updated, failed = await event_media.materialize_event_posters_to_cdn(
            session, int(event.id)
        )
        await session.commit()
        await session.refresh(retry)

    await db.engine.dispose()
    assert updated == 2
    assert failed == 0
    assert retry.supabase_url.startswith("https://static.kenigevents.ru/p/dh16/")
    assert retry.raw_sha256 is None


@pytest.mark.asyncio
async def test_strict_cdn_gate_does_not_project_unmaterialized_source_url(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")

    async def fail_materialize(_candidate) -> bool:
        return False

    monkeypatch.setattr(
        event_media, "materialize_event_media_candidate_to_cdn", fail_materialize
    )
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
            [PosterCandidate(catbox_url="https://source.example/poster.jpg")],
        )
        await session.commit()
        await session.refresh(event)
        row = (
            await session.execute(
                select(EventPoster).where(EventPoster.event_id == event.id)
            )
        ).scalar_one()

    assert row.review_status == PENDING_REVIEW
    assert row.review_reason == "cdn_mirror_pending"
    assert event.photo_urls == []
    assert event.photo_count == 0


@pytest.mark.asyncio
async def test_recovered_unavailable_poster_is_reopened_and_projected(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    source_url = "https://source.example/recovered.jpg"
    cdn_url = "https://static.kenigevents.ru/p/dh16/aa/recovered.webp"

    async def recover(candidate) -> bool:
        candidate.supabase_url = cdn_url
        candidate.supabase_path = "p/dh16/aa/recovered.webp"
        return True

    monkeypatch.setattr(
        event_media, "materialize_event_media_candidate_to_cdn", recover
    )
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        session.add(
            EventPoster(
                event_id=int(event.id),
                catbox_url=source_url,
                poster_hash=hashlib.sha256(f"url:{source_url}".encode()).hexdigest(),
                review_status="unavailable",
                review_reason="download_unavailable",
            )
        )
        await session.commit()
        await _apply_posters(
            session,
            event.id,
            [PosterCandidate(catbox_url=source_url)],
        )
        await session.commit()
        await session.refresh(event)
        row = (
            await session.execute(
                select(EventPoster).where(EventPoster.event_id == event.id)
            )
        ).scalar_one()

    assert row.review_status == APPROVED
    assert row.supabase_url == cdn_url
    assert event.photo_urls == [cdn_url]


@pytest.mark.asyncio
async def test_pair_reviewer_uses_standard_json_schema_transport(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeSession:
        async def commit(self) -> None:
            return None

    class FakeGoogleAIClient:
        def __init__(self, **_kwargs):
            pass

        async def generate_content_async(self, **kwargs):
            captured.update(kwargs)
            return (
                json.dumps(
                    {
                        "decision": "distinct",
                        "duplicate_kind": "none",
                        "confidence": 0.99,
                        "semantic_conflict": False,
                        "canonical_side": "either",
                        "reason_code": "different_photos",
                    }
                ),
                None,
            )

    async def fake_claim(*_args, **_kwargs) -> bool:
        return True

    import google_ai
    import main

    monkeypatch.setattr(event_media, "_claim_feature_budget", fake_claim)
    monkeypatch.setattr(event_media, "compute_global_ssim", lambda *_args: 0.5)
    monkeypatch.setattr(google_ai, "GoogleAIClient", FakeGoogleAIClient)
    monkeypatch.setattr(google_ai, "SecretsProvider", lambda: object())
    monkeypatch.setattr(main, "get_supabase_client", lambda: None)
    result, calls = await event_media._call_reviewer(
        event=_event(),
        left=EventPoster(event_id=1, poster_hash="left", review_status=APPROVED),
        right=EventPoster(event_id=1, poster_hash="right", review_status=PENDING_REVIEW),
        left_media=DownloadedPoster(b"left", "image/png", "https://example/left.png"),
        right_media=DownloadedPoster(b"right", "image/png", "https://example/right.png"),
        model="gemini-3.1-flash-lite-preview",
        stage="primary",
        session=FakeSession(),
    )

    assert calls == 1
    assert result and result["decision"] == "distinct"
    config = captured["generation_config"]
    assert isinstance(config, dict)
    assert config["response_json_schema"] == event_media._REVIEW_SCHEMA
    assert "response_schema" not in config


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


@pytest.mark.asyncio
async def test_stale_running_pair_review_is_returned_to_automatic_retry(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = event_media.datetime.now(event_media.timezone.utc)
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        left = EventPoster(
            event_id=int(event.id),
            poster_hash="left-stale",
            supabase_url="https://static.example/left.webp",
            review_status=APPROVED,
        )
        right = EventPoster(
            event_id=int(event.id),
            poster_hash="right-stale",
            supabase_url="https://static.example/right.webp",
            review_status=PENDING_REVIEW,
        )
        session.add(left)
        session.add(right)
        await session.commit()
        await session.refresh(left)
        await session.refresh(right)
        review = EventMediaPairReview(
            event_id=int(event.id),
            left_poster_id=int(left.id),
            right_poster_id=int(right.id),
            context_hash="ctx-stale",
            pair_input_hash="pair-stale",
            status="running",
            attempts=1,
            updated_at=now - event_media.timedelta(minutes=20),
            next_run_at=now - event_media.timedelta(minutes=20),
        )
        session.add(review)
        await session.commit()

        recovered = await event_media._recover_stale_running_reviews(
            session, int(event.id), now=now
        )
        await session.commit()
        await session.refresh(review)

    assert recovered == 1
    assert review.status == "deferred"
    assert review.decision == "uncertain"
    assert review.reason_code == "automatic_running_recovered"
    assert review.last_error == "interrupted_running_review"
    assert review.attempts == 1
    recovered_run_at = review.next_run_at
    if recovered_run_at.tzinfo is None:
        recovered_run_at = recovered_run_at.replace(tzinfo=event_media.timezone.utc)
    assert recovered_run_at == now


@pytest.mark.asyncio
async def test_exact_display_url_is_resolved_before_recovered_pair_download(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = event_media.datetime.now(event_media.timezone.utc)
    shared_url = "https://static.example/shared.webp"
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        left = EventPoster(
            event_id=int(event.id),
            poster_hash="left-same-url",
            supabase_url=shared_url,
            review_status=APPROVED,
            display_order=0,
        )
        right = EventPoster(
            event_id=int(event.id),
            poster_hash="right-same-url",
            supabase_url=shared_url,
            review_status=PENDING_REVIEW,
            display_order=1,
        )
        session.add(left)
        session.add(right)
        await session.commit()
        await session.refresh(left)
        await session.refresh(right)
        review = EventMediaPairReview(
            event_id=int(event.id),
            left_poster_id=int(left.id),
            right_poster_id=int(right.id),
            context_hash="ctx-same-url",
            pair_input_hash="pair-same-url",
            status="deferred",
            attempts=1,
            updated_at=now,
            next_run_at=now,
        )
        session.add(review)
        await session.commit()

        created = await event_media.ensure_event_media_reviews(session, int(event.id))
        await event_media.sync_event_gallery_projection(session, int(event.id))
        await session.commit()
        await session.refresh(left)
        await session.refresh(right)
        await session.refresh(review)
        await session.refresh(event)

    assert created == 0
    assert left.review_status == APPROVED
    assert right.review_status == DUPLICATE
    assert right.duplicate_of_id == left.id
    assert right.review_reason == "exact_display_url_duplicate"
    assert review.status == "cancelled"
    assert review.decision == "duplicate"
    assert review.reason_code == "exact_display_url_duplicate"
    assert event.photo_urls == [shared_url]
    assert event.photo_count == 1


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


def test_audit_cleanup_marks_only_confirmed_telegram_duplicate_for_priority(
    tmp_path,
) -> None:
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    (audit_dir / "inventory.jsonl").write_text(
        json.dumps(
            {
                "event": {"id": 1, "title": "t", "date": "2026-08-01"},
                "static_gallery": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (audit_dir / "visual-review.csv").write_text(
        "event_id,confirmed_duplicate_groups,classification,visual_review_status\n"
        '1,"[]",legitimate_distinct,reviewed_complete\n',
        encoding="utf-8",
    )
    (audit_dir / "downloaded-media-manifest.json").write_text(
        json.dumps({"items": []}), encoding="utf-8"
    )
    (audit_dir / "public-telegram-surfaces.json").write_text(
        json.dumps([{"event_id": 1, "duplicate_visible": True}]), encoding="utf-8"
    )

    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "apply_event_media_audit_cleanup.py"
    )
    spec = importlib.util.spec_from_file_location("apply_event_media_audit_cleanup", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    plan = module.build_plan(audit_dir)

    assert plan["events"][0]["telegram_duplicate_visible"] is True


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
