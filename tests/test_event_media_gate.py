from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

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
    _insert_pair_review_if_absent,
    get_event_gallery_urls,
    review_next_event_media_pair,
)
from media_dedup import compute_image_fingerprints
from media_dedup import prepare_image_for_supabase
from models import (
    Event,
    EventImageGeometry,
    EventMediaPairReview,
    EventPoster,
    JobOutbox,
    JobTask,
)
from smart_event_update import (
    PosterCandidate,
    _apply_posters,
    _poster_source_exact_variant_hash,
)


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


@pytest.mark.asyncio
async def test_pair_review_insert_is_idempotent_on_unique_input_hash(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.commit()
        await session.refresh(event)
        left = EventPoster(
            event_id=event.id,
            poster_hash="left",
            supabase_url="https://static.example/left.webp",
            review_status=APPROVED,
        )
        right = EventPoster(
            event_id=event.id,
            poster_hash="right",
            supabase_url="https://static.example/right.webp",
            review_status=PENDING_REVIEW,
        )
        session.add(left)
        session.add(right)
        await session.commit()
        await session.refresh(left)
        await session.refresh(right)

        first = await _insert_pair_review_if_absent(
            session,
            event_id=event.id,
            left_poster_id=left.id,
            right_poster_id=right.id,
            context_hash="context",
            pair_input_hash="same-input",
        )
        second = await _insert_pair_review_if_absent(
            session,
            event_id=event.id,
            left_poster_id=left.id,
            right_poster_id=right.id,
            context_hash="context",
            pair_input_hash="same-input",
        )
        await session.commit()
        rows = list(
            (
                await session.execute(
                    select(EventMediaPairReview).where(
                        EventMediaPairReview.pair_input_hash == "same-input"
                    )
                )
            ).scalars().all()
        )

    assert first is True
    assert second is False
    assert len(rows) == 1


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
            f"p/image/v2/aa/{'a' * 64}.webp"
        )
    )

    assert await event_media.materialize_event_media_candidate_to_cdn(candidate)
    assert candidate.supabase_url == (
        f"https://static.kenigevents.ru/p/image/v2/aa/{'a' * 64}.webp"
    )
    assert candidate.supabase_path == f"p/image/v2/aa/{'a' * 64}.webp"
    assert candidate.raw_sha256 == "a" * 64


@pytest.mark.asyncio
async def test_legacy_managed_poster_is_rematerialized_to_exact_v2(monkeypatch) -> None:
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    source_bytes = _pattern_png_bytes()

    def download(url: str, *, max_bytes: int, timeout: float):
        del max_bytes, timeout
        return DownloadedPoster(source_bytes, "image/webp", url)

    monkeypatch.setattr(event_media, "_download_url", download)
    monkeypatch.setattr(
        "yandex_storage.upload_yandex_public_bytes",
        lambda _data, *, object_path, content_type, **_kwargs: (
            f"https://static.kenigevents.ru/{object_path}"
        ),
    )
    candidate = PosterCandidate(
        supabase_url=(
            "https://static.kenigevents.ru/"
            f"p/dh16/aa/{'a' * 64}.webp"
        ),
        supabase_path=f"p/dh16/aa/{'a' * 64}.webp",
    )

    assert await event_media.materialize_event_media_candidate_to_cdn(candidate)
    assert candidate.supabase_path.startswith("p/image/v2/")
    assert candidate.supabase_url.endswith(candidate.supabase_path)
    assert candidate.raw_sha256 == candidate.supabase_path.split("/")[-1][:-5]


@pytest.mark.asyncio
async def test_cdn_display_url_change_invalidates_visual_evidence(monkeypatch) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    candidate = SimpleNamespace(
        supabase_url=(
            "https://storage.yandexcloud.net/kenigevents.ru/"
            f"p/image/v2/aa/{'a' * 64}.webp"
        ),
        catbox_url=None,
        image_geometry_id=42,
        pixel_sha256="1" * 64,
        media_semantic_status="classified",
        media_semantic_reason_code=None,
        media_role="event_photo",
        focal_x=0.4,
        focal_y=0.5,
        safe_crop=True,
    )

    assert await event_media.materialize_event_media_candidate_to_cdn(candidate)

    assert candidate.supabase_url == (
        f"https://static.kenigevents.ru/p/image/v2/aa/{'a' * 64}.webp"
    )
    assert candidate.image_geometry_id is None
    assert candidate.pixel_sha256 is None
    assert candidate.media_semantic_status == "pending"
    assert candidate.media_semantic_reason_code == "display_identity_changed"
    assert candidate.media_role is None
    assert candidate.focal_x is None and candidate.focal_y is None
    assert candidate.safe_crop is False


@pytest.mark.asyncio
async def test_geometry_download_never_falls_back_from_display_to_source(
    monkeypatch,
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    calls: list[str] = []

    def download(url: str, *, max_bytes: int, timeout: float):
        del max_bytes, timeout
        calls.append(url)
        if url == "https://static.kenigevents.ru/current.webp":
            raise OSError("managed object unavailable")
        return DownloadedPoster(b"different source", "image/jpeg", url)

    monkeypatch.setattr(event_media, "_download_url", download)
    poster = EventPoster(
        event_id=1,
        poster_hash="display-only-download",
        supabase_url="https://static.kenigevents.ru/current.webp",
        catbox_url="https://source.example/different.jpg",
        review_status=APPROVED,
    )

    with pytest.raises(RuntimeError, match="managed object unavailable"):
        await event_media._download_poster(poster)

    assert calls == ["https://static.kenigevents.ru/current.webp"]


@pytest.mark.asyncio
async def test_cdn_retry_preserves_unique_raw_sha_survivor(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    source_bytes = _pattern_png_bytes()
    prepared = prepare_image_for_supabase(source_bytes)
    assert prepared is not None
    digest = prepared.encoded_sha256

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
    assert retry.supabase_url.startswith("https://static.kenigevents.ru/p/image/v2/")
    assert retry.raw_sha256 is None


@pytest.mark.asyncio
async def test_exact_v2_early_return_preserves_unique_raw_sha_survivor(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    digest = "a" * 64
    exact_path = f"p/image/v2/aa/{digest}.webp"
    exact_url = f"https://static.kenigevents.ru/{exact_path}"

    db = Database(str(tmp_path / "exact-early-return.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.flush()
        survivor = EventPoster(
            event_id=int(event.id),
            poster_hash="survivor",
            supabase_url=exact_url,
            supabase_path=exact_path,
            raw_sha256=digest,
            thumbnail_256_url="https://static.kenigevents.ru/t/256/a.webp",
            thumbnail_512_url="https://static.kenigevents.ru/t/512/a.webp",
            review_status=APPROVED,
            display_order=0,
        )
        duplicate = EventPoster(
            event_id=int(event.id),
            poster_hash="duplicate",
            supabase_url=exact_url,
            supabase_path=exact_path,
            thumbnail_256_url="https://static.kenigevents.ru/t/256/b.webp",
            thumbnail_512_url="https://static.kenigevents.ru/t/512/b.webp",
            review_status=APPROVED,
            display_order=1,
        )
        session.add(survivor)
        session.add(duplicate)
        await session.commit()

        updated, failed = await event_media.materialize_event_posters_to_cdn(
            session, int(event.id)
        )
        await session.commit()
        await session.refresh(duplicate)

    await db.engine.dispose()
    assert updated == 0
    assert failed == 0
    assert duplicate.raw_sha256 is None


@pytest.mark.asyncio
async def test_smart_update_merge_does_not_claim_another_rows_raw_sha(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "0")
    digest = "b" * 64
    exact_path = f"p/image/v2/bb/{digest}.webp"
    exact_url = f"https://static.kenigevents.ru/{exact_path}"
    db = Database(str(tmp_path / "smart-merge-raw-conflict.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.flush()
        matched_by_source = EventPoster(
            event_id=int(event.id),
            poster_hash="source-candidate",
            review_status=APPROVED,
            display_order=0,
        )
        raw_owner = EventPoster(
            event_id=int(event.id),
            poster_hash="other-source",
            supabase_url=exact_url,
            supabase_path=exact_path,
            raw_sha256=digest,
            review_status=APPROVED,
            display_order=1,
        )
        session.add(matched_by_source)
        session.add(raw_owner)
        await session.commit()

        await _apply_posters(
            session,
            int(event.id),
            [
                PosterCandidate(
                    sha256="source-candidate",
                    raw_sha256=digest,
                    supabase_url=exact_url,
                    supabase_path=exact_path,
                )
            ],
        )
        await session.commit()
        await session.refresh(matched_by_source)
        await session.refresh(raw_owner)

    await db.engine.dispose()
    assert matched_by_source.raw_sha256 is None
    assert matched_by_source.supabase_url is None
    assert matched_by_source.supabase_path is None
    assert raw_owner.raw_sha256 == digest
    assert raw_owner.supabase_url == exact_url
    assert raw_owner.supabase_path == exact_path


@pytest.mark.asyncio
async def test_repeated_source_reconcile_does_not_replace_classified_exact_v2_row(
    tmp_path, monkeypatch
) -> None:
    """A mutable provenance URL must not reset exact visual evidence forever."""

    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "0")
    source_url = "https://source.example/mutable-poster.jpg"
    old_raw = "a" * 64
    new_raw = "b" * 64
    old_path = f"p/image/v2/aa/{old_raw}.webp"
    new_path = f"p/image/v2/bb/{new_raw}.webp"
    old_url = f"https://static.kenigevents.ru/{old_path}"
    new_url = f"https://static.kenigevents.ru/{new_path}"
    db = Database(str(tmp_path / "exact-source-convergence.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.flush()
        geometry = EventImageGeometry(
            pixel_sha256="1" * 64,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        session.add(geometry)
        await session.flush()
        canonical = EventPoster(
            event_id=int(event.id),
            poster_hash="old-source-identity",
            catbox_url=source_url,
            supabase_url=old_url,
            supabase_path=old_path,
            raw_sha256=old_raw,
            pixel_sha256="1" * 64,
            image_geometry_id=int(geometry.id),
            review_status=APPROVED,
            display_order=0,
            media_semantic_status="classified",
            media_semantic_prompt_version=event_media.MEDIA_ROLE_PROMPT_VERSION,
            media_semantic_context_hash=event_media._context_hash(event),
            media_role="event_photo",
            safe_crop=True,
        )
        session.add(canonical)
        await session.commit()

        def candidate() -> PosterCandidate:
            return PosterCandidate(
                sha256="new-source-identity",
                catbox_url=source_url,
                supabase_url=new_url,
                supabase_path=new_path,
                raw_sha256=new_raw,
            )

        first_added, *_ = await _apply_posters(
            session, int(event.id), [candidate()]
        )
        await session.commit()
        second_added, *_ = await _apply_posters(
            session, int(event.id), [candidate()]
        )
        await session.commit()
        await session.refresh(canonical)
        rows = list(
            (
                await session.execute(
                    select(EventPoster)
                    .where(EventPoster.event_id == int(event.id))
                    .order_by(EventPoster.id.asc())
                )
            ).scalars()
        )

    await db.engine.dispose()
    assert first_added == 1
    assert second_added == 0
    assert len(rows) == 2
    assert canonical.supabase_url == old_url
    assert canonical.supabase_path == old_path
    assert canonical.raw_sha256 == old_raw
    assert canonical.pixel_sha256 == "1" * 64
    assert canonical.image_geometry_id == geometry.id
    assert canonical.media_semantic_status == "classified"
    assert canonical.media_role == "event_photo"
    assert canonical.safe_crop is True
    assert rows[1].supabase_url == new_url
    assert rows[1].raw_sha256 == new_raw


@pytest.mark.asyncio
async def test_stable_source_hash_with_new_exact_rendition_uses_stable_variant(
    tmp_path, monkeypatch
) -> None:
    """A mutable source hash may not violate the per-event unique constraint."""

    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "0")
    source_hash = "source-hash-stays-stable"
    old_raw = "a" * 64
    new_raw = "b" * 64
    canonical_raw = "c" * 64

    def exact_path(digest: str) -> str:
        return f"p/image/v2/{digest[:2]}/{digest}.webp"

    def exact_url(digest: str) -> str:
        return f"https://static.kenigevents.ru/{exact_path(digest)}"

    db = Database(str(tmp_path / "stable-source-new-exact.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.flush()
        geometry = EventImageGeometry(
            pixel_sha256="1" * 64,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        session.add(geometry)
        await session.flush()
        canonical = EventPoster(
            event_id=int(event.id),
            poster_hash="canonical",
            supabase_url=exact_url(canonical_raw),
            supabase_path=exact_path(canonical_raw),
            raw_sha256=canonical_raw,
            pixel_sha256="1" * 64,
            image_geometry_id=int(geometry.id),
            review_status=APPROVED,
            display_order=0,
        )
        old_source_alias = EventPoster(
            event_id=int(event.id),
            poster_hash=source_hash,
            catbox_url="https://source.example/mutable.jpg",
            supabase_url=exact_url(old_raw),
            supabase_path=exact_path(old_raw),
            raw_sha256=old_raw,
            pixel_sha256="1" * 64,
            image_geometry_id=int(geometry.id),
            review_status=DUPLICATE,
            duplicate_of_id=None,
            display_order=1,
            media_semantic_status="classified",
            media_role="event_photo",
            safe_crop=True,
        )
        session.add(canonical)
        session.add(old_source_alias)
        await session.commit()

        def candidate() -> PosterCandidate:
            return PosterCandidate(
                sha256=source_hash,
                catbox_url="https://source.example/mutable.jpg",
                supabase_url=exact_url(new_raw),
                supabase_path=exact_path(new_raw),
                raw_sha256=new_raw,
            )

        first_added, *_ = await _apply_posters(
            session, int(event.id), [candidate()]
        )
        await session.commit()
        second_added, *_ = await _apply_posters(
            session, int(event.id), [candidate()]
        )
        await session.commit()
        await session.refresh(old_source_alias)
        rows = list(
            (
                await session.execute(
                    select(EventPoster)
                    .where(EventPoster.event_id == int(event.id))
                    .order_by(EventPoster.id.asc())
                )
            ).scalars()
        )

    await db.engine.dispose()
    assert first_added == 1
    assert second_added == 0
    assert len(rows) == 3
    assert old_source_alias.poster_hash == source_hash
    assert old_source_alias.supabase_path == exact_path(old_raw)
    assert old_source_alias.raw_sha256 == old_raw
    assert old_source_alias.image_geometry_id == geometry.id
    assert old_source_alias.media_semantic_status == "classified"
    assert old_source_alias.safe_crop is True
    variant = rows[2]
    assert variant.poster_hash == _poster_source_exact_variant_hash(
        source_hash, new_raw
    )
    assert variant.poster_hash != source_hash
    assert variant.supabase_path == exact_path(new_raw)
    assert variant.raw_sha256 == new_raw


@pytest.mark.asyncio
async def test_cdn_object_path_uses_exact_encoded_hash_not_shared_dhash(monkeypatch) -> None:
    monkeypatch.setenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru")
    source_payloads = {
        "https://source.example/one.png": b"source-one",
        "https://source.example/two.png": b"source-two",
    }
    prepared_payloads = {
        b"source-one": b"encoded-one",
        b"source-two": b"encoded-two",
    }

    def download(url: str, *, max_bytes: int, timeout: float) -> DownloadedPoster:
        del max_bytes, timeout
        return DownloadedPoster(source_payloads[url], "image/png", url)

    def prepare(payload: bytes, **_kwargs):
        encoded = prepared_payloads[payload]
        return SimpleNamespace(
            dhash_hex="ab" * 32,
            webp_bytes=encoded,
            encoded_sha256=hashlib.sha256(encoded).hexdigest(),
            width=64,
            height=48,
            thumbnails=(),
        )

    uploaded_paths: list[str] = []

    def upload(_data, *, object_path, content_type, **_kwargs):
        del content_type
        uploaded_paths.append(object_path)
        return f"https://static.kenigevents.ru/{object_path}"

    monkeypatch.setattr(event_media, "_download_url", download)
    monkeypatch.setattr("media_dedup.prepare_image_for_supabase", prepare)
    monkeypatch.setattr("yandex_storage.upload_yandex_public_bytes", upload)
    first = PosterCandidate(
        catbox_url="https://source.example/one.png", sha256="f" * 64
    )
    second = PosterCandidate(catbox_url="https://source.example/two.png")

    assert await event_media.materialize_event_media_candidate_to_cdn(first)
    assert await event_media.materialize_event_media_candidate_to_cdn(second)

    assert first.phash == second.phash == "ab" * 32
    assert first.sha256 == "f" * 64
    assert first.raw_sha256 == hashlib.sha256(b"encoded-one").hexdigest()
    assert second.raw_sha256 == hashlib.sha256(b"encoded-two").hexdigest()
    assert first.supabase_path != second.supabase_path
    assert uploaded_paths == [first.supabase_path, second.supabase_path]
    assert all(path.startswith("p/image/v2/") for path in uploaded_paths)


@pytest.mark.asyncio
async def test_smart_update_display_identity_change_invalidates_visual_evidence(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "0")
    db = Database(str(tmp_path / "identity-change.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.flush()
        geometry = EventImageGeometry(
            pixel_sha256="1" * 64,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        session.add(geometry)
        await session.flush()
        poster = EventPoster(
            event_id=int(event.id),
            poster_hash="a" * 64,
            raw_sha256="a" * 64,
            pixel_sha256="1" * 64,
            perceptual_hash="2" * 64,
            phash="3" * 64,
            supabase_url="https://static.example/old.webp",
            supabase_path="p/image/v2/old.webp",
            review_status=APPROVED,
            width=1200,
            height=800,
            mime_type="image/webp",
            image_geometry_id=int(geometry.id),
            media_semantic_status="classified",
            media_semantic_prompt_version=event_media.MEDIA_ROLE_PROMPT_VERSION,
            media_semantic_context_hash=event_media._context_hash(event),
            media_role="event_photo",
            focal_x=0.4,
            focal_y=0.5,
            safe_crop=True,
            thumbnail_256_url="https://static.example/old-256.webp",
            thumbnail_512_url="https://static.example/old-512.webp",
        )
        session.add(poster)
        await session.commit()

        await _apply_posters(
            session,
            int(event.id),
            [
                PosterCandidate(
                    supabase_url="https://static.example/new.webp",
                    supabase_path="p/image/v2/new.webp",
                    sha256="a" * 64,
                )
            ],
        )
        await session.commit()
        await session.refresh(poster)
        job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == int(event.id),
                    JobOutbox.task == JobTask.event_media_review,
                )
            )
        ).scalar_one_or_none()

    await db.engine.dispose()
    assert poster.supabase_url == "https://static.example/new.webp"
    assert poster.pixel_sha256 is None
    assert poster.image_geometry_id is None
    assert poster.media_semantic_status == "pending"
    assert poster.media_semantic_reason_code == "display_identity_changed"
    assert poster.focal_x is None and poster.focal_y is None
    assert poster.safe_crop is False
    assert poster.thumbnail_256_url is None and poster.thumbnail_512_url is None
    assert job is not None


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
        geometry_job = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event.id,
                    JobOutbox.task == JobTask.event_media_review,
                )
            )
        ).scalar_one_or_none()

    assert row.review_status == APPROVED
    assert row.supabase_url == cdn_url
    assert event.photo_urls == [cdn_url]
    assert geometry_job is not None


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
    # Candidate/source identity stays in poster_hash. raw_sha256 is reserved
    # for exact bytes actually served by the managed display URL.
    assert rows[0].poster_hash == digest
    assert rows[0].raw_sha256 is None
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
async def test_final_distinct_pair_promotes_and_enqueues_geometry_accumulation(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "final-pair-geometry.sqlite"))
    await db.init()
    payloads = {
        "https://static.example/a.png": _pattern_png_bytes(invert=False),
        "https://static.example/b.png": _pattern_png_bytes(invert=True),
    }

    async def fake_download(poster: EventPoster) -> DownloadedPoster:
        url = str(poster.supabase_url)
        return DownloadedPoster(payloads[url], "image/png", url)

    async def distinct_reviewer(**_kwargs):
        return (
            {
                "decision": "distinct",
                "duplicate_kind": "none",
                "confidence": 0.99,
                "semantic_conflict": False,
                "canonical_side": "either",
                "reason_code": "different_photos",
            },
            1,
        )

    monkeypatch.setattr(event_media, "_download_poster", fake_download)
    monkeypatch.setattr(event_media, "_call_reviewer", distinct_reviewer)
    async with db.get_session() as session:
        event = _event()
        session.add(event)
        await session.flush()
        event_id = int(event.id)
        await _apply_posters(
            session,
            event_id,
            [
                PosterCandidate(
                    supabase_url="https://static.example/a.png", sha256="a" * 64
                ),
                PosterCandidate(
                    supabase_url="https://static.example/b.png", sha256="b" * 64
                ),
            ],
        )
        await session.flush()
        rows = (
            await session.execute(
                select(EventPoster)
                .where(EventPoster.event_id == event_id)
                .order_by(EventPoster.id)
            )
        ).scalars().all()
        first_fp = compute_image_fingerprints(payloads[str(rows[0].supabase_url)])
        assert first_fp is not None
        geometry = EventImageGeometry(
            pixel_sha256=first_fp.pixel_sha256,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        session.add(geometry)
        await session.flush()
        rows[0].pixel_sha256 = first_fp.pixel_sha256
        rows[0].image_geometry_id = int(geometry.id)
        for job in (
            await session.execute(
                select(JobOutbox).where(JobOutbox.event_id == event_id)
            )
        ).scalars().all():
            await session.delete(job)
        await session.commit()
        second_id = int(rows[1].id)

    await review_next_event_media_pair(event_id, db)
    async with db.get_session() as session:
        second = await session.get(EventPoster, second_id)
        jobs = (
            await session.execute(
                select(JobOutbox).where(
                    JobOutbox.event_id == event_id,
                    JobOutbox.task == JobTask.event_media_review,
                )
            )
        ).scalars().all()
        candidate_id = await event_media._next_geometry_candidate_id(
            session, event_id
        )

    await db.engine.dispose()
    assert second.review_status == APPROVED
    assert candidate_id == second_id
    assert len(jobs) == 1


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
