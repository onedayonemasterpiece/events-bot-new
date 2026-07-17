from __future__ import annotations

import hashlib
import io
from types import SimpleNamespace

import pytest
from PIL import Image

import event_media
from db import Database
from media_dedup import (
    build_event_thumbnail_object_path,
    prepare_image_for_supabase,
)
from models import Event, EventImageGeometry, EventPoster


def _jpeg_bytes(width: int = 1600, height: int = 1000) -> bytes:
    image = Image.new("RGB", (width, height), (34, 91, 146))
    out = io.BytesIO()
    image.save(out, format="JPEG", quality=90)
    return out.getvalue()


def _role_decision(*, role: str, confidence: float = 0.97, contract: bool = True):
    return {
        "media_role": role,
        "image_text_mode": "ocr_text",
        "primary_purpose": "identifies the event",
        "confidence": confidence,
        "reason_code": "event_identity_matches",
        "poster_contract": {
            "primary_event_promotion": contract,
            "event_identity_grounded": contract,
            "not_utility_document": contract,
        },
        "focal_point": {"x": 0.45, "y": 0.4},
        "safe_crop": False,
        "evidence": ["title and date match"],
    }


def test_prepared_event_media_has_small_immutable_derivatives() -> None:
    prepared = prepare_image_for_supabase(_jpeg_bytes())

    assert prepared is not None
    assert (prepared.width, prepared.height) == (1600, 1000)
    assert prepared.encoded_sha256 == hashlib.sha256(prepared.webp_bytes).hexdigest()
    assert [item.longest_edge for item in prepared.thumbnails] == [256, 512]
    assert [(item.width, item.height) for item in prepared.thumbnails] == [
        (256, 160),
        (512, 320),
    ]
    assert all(item.webp_bytes for item in prepared.thumbnails)


def test_thumbnail_object_path_is_recipe_and_content_addressed() -> None:
    digest = "ab" * 32

    assert build_event_thumbnail_object_path(digest, longest_edge=256) == (
        f"p/thumb/v1/ab/{digest}/256.webp"
    )
    with pytest.raises(ValueError, match="unsupported event thumbnail edge"):
        build_event_thumbnail_object_path(digest, longest_edge=320)


def test_identity_poster_contract_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("EVENT_MEDIA_ROLE_POSTER_CONFIDENCE", "0.88")

    accepted = event_media._validated_media_role(
        _role_decision(role="event_identity_poster")
    )
    assert accepted is not None
    assert accepted["media_role"] == "event_identity_poster"

    assert event_media._validated_media_role(
        _role_decision(role="event_identity_poster", confidence=0.87)
    ) is None
    assert event_media._validated_media_role(
        _role_decision(role="event_identity_poster", contract=False)
    ) is None


def test_utility_document_is_never_promoted_by_deterministic_validation() -> None:
    value = _role_decision(role="attendee_information", contract=False)
    value["primary_purpose"] = "services and visitor rules"
    value["reason_code"] = "utility_services_card"

    accepted = event_media._validated_media_role(value)

    assert accepted is not None
    assert accepted["media_role"] == "attendee_information"
    assert accepted["poster_contract"]["primary_event_promotion"] is False


def test_image_geometry_validation_normalizes_compact_yxyx() -> None:
    value = event_media._validated_image_geometry(
        {
            "face_boxes_yxyx": [[100, 200, 300, 450], [20, 30, 80, 90]],
            "valuable_region_yxyx": [50, 100, 900, 950],
            "valuable_region_confidence": 0.93,
            "reason_code": "main_people_and_title",
        }
    )

    assert value == {
        "face_boxes_yxyx": [[0.1, 0.2, 0.3, 0.45], [0.02, 0.03, 0.08, 0.09]],
        "valuable_region_yxyx": [0.05, 0.1, 0.9, 0.95],
        "valuable_region_confidence": 0.93,
        "reason_code": "main_people_and_title",
    }
    no_faces = event_media._validated_image_geometry(
        {
            "face_boxes_yxyx": [],
            "valuable_region_yxyx": [0, 0, 1000, 1000],
            "valuable_region_confidence": 0.7,
            "reason_code": "full_scene",
        }
    )
    assert no_faces is not None and no_faces["face_boxes_yxyx"] == []


@pytest.mark.parametrize(
    "box",
    ([100, 100, 100, 300], [-1, 0, 10, 10], [0, 0, 1001, 1000], [1, 2, 3]),
)
def test_image_geometry_validation_rejects_invalid_boxes(box) -> None:
    assert event_media._validated_image_geometry(
        {
            "face_boxes_yxyx": [box],
            "valuable_region_yxyx": [0, 0, 1000, 1000],
            "valuable_region_confidence": 0.8,
            "reason_code": "test",
        }
    ) is None


def test_image_geometry_crowd_contract_is_bounded_to_crop_relevant_faces() -> None:
    face = [10, 10, 20, 20]
    assert "up to 25 largest/clearest" in event_media._image_geometry_prompt()
    assert event_media._validated_image_geometry(
        {
            "face_boxes_yxyx": [face] * event_media.IMAGE_GEOMETRY_MAX_FACE_BOXES,
            "valuable_region_yxyx": [0, 0, 1000, 1000],
            "valuable_region_confidence": 0.8,
            "reason_code": "crowd",
        }
    ) is not None
    assert event_media._validated_image_geometry(
        {
            "face_boxes_yxyx": [face]
            * (event_media.IMAGE_GEOMETRY_MAX_FACE_BOXES + 1),
            "valuable_region_yxyx": [0, 0, 1000, 1000],
            "valuable_region_confidence": 0.8,
            "reason_code": "crowd",
        }
    ) is None


@pytest.mark.asyncio
async def test_geometry_cache_reuses_pixel_identity_across_events(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "geometry.sqlite"))
    await db.init()
    image_bytes = _jpeg_bytes(320, 200)
    provider_calls = 0

    async def fake_download(_poster):
        return event_media.DownloadedPoster(
            data=image_bytes,
            mime_type="image/jpeg",
            source_url="https://static.kenigevents.ru/test.webp",
        )

    async def fake_budget(_session, _stage, _limit):
        return True

    async def fake_provider(_media):
        nonlocal provider_calls
        provider_calls += 1
        return (
            {
                "face_boxes_yxyx": [[0.1, 0.2, 0.3, 0.4]],
                "valuable_region_yxyx": [0.05, 0.1, 0.9, 0.95],
                "valuable_region_confidence": 0.91,
                "reason_code": "main_subject",
            },
            SimpleNamespace(input_tokens=10, output_tokens=5, total_tokens=15),
        )

    monkeypatch.setattr(event_media, "_download_poster", fake_download)
    monkeypatch.setattr(event_media, "_claim_feature_budget", fake_budget)
    monkeypatch.setattr(event_media, "_call_image_geometry_provider", fake_provider)

    async with db.get_session() as session:
        first_event = Event(
            title="Первое",
            description="Описание",
            date="2026-08-01",
            time="18:00",
            location_name="Площадка",
            source_text="Источник",
        )
        second_event = Event(
            title="Второе",
            description="Описание",
            date="2026-08-02",
            time="18:00",
            location_name="Площадка",
            source_text="Источник",
        )
        session.add(first_event)
        session.add(second_event)
        await session.commit()
        await session.refresh(first_event)
        await session.refresh(second_event)
        first = EventPoster(
            event_id=int(first_event.id),
            poster_hash="a" * 64,
            supabase_url="https://static.kenigevents.ru/a.webp",
            review_status="approved",
        )
        second = EventPoster(
            event_id=int(second_event.id),
            poster_hash="b" * 64,
            supabase_url="https://static.kenigevents.ru/b.webp",
            review_status="approved",
        )
        session.add(first)
        session.add(second)
        await session.commit()
        await session.refresh(first)
        await session.refresh(second)
        first_ids = (int(first_event.id), int(first.id))
        second_ids = (int(second_event.id), int(second.id))

    first_outcome = await event_media.analyze_event_poster_geometry(*first_ids, db)
    second_outcome = await event_media.analyze_event_poster_geometry(*second_ids, db)

    async with db.get_session() as session:
        rows = (await session.execute(event_media.select(EventImageGeometry))).scalars().all()
        first_row = await session.get(EventPoster, first_ids[1])
        second_row = await session.get(EventPoster, second_ids[1])

    await db.engine.dispose()
    assert first_outcome.provider_called is True
    assert second_outcome.cache_hit is True
    assert provider_calls == 1
    assert len(rows) == 1
    assert first_row is not None and second_row is not None
    assert first_row.image_geometry_id == second_row.image_geometry_id == rows[0].id


@pytest.mark.asyncio
async def test_geometry_download_failure_is_item_level(tmp_path, monkeypatch) -> None:
    db = Database(str(tmp_path / "geometry-download.sqlite"))
    await db.init()

    async def broken_download(_poster):
        raise RuntimeError("HTTP 404")

    monkeypatch.setattr(event_media, "_download_poster", broken_download)
    async with db.get_session() as session:
        event = Event(
            title="Missing image",
            description="Описание",
            date="2026-08-01",
            time="18:00",
            location_name="Площадка",
            source_text="Источник",
        )
        session.add(event)
        await session.flush()
        poster = EventPoster(
            event_id=int(event.id),
            poster_hash="c" * 64,
            supabase_url="https://static.kenigevents.ru/missing.webp",
            review_status="approved",
        )
        session.add(poster)
        await session.commit()
        ids = (int(event.id), int(poster.id))

    outcome = await event_media.analyze_event_poster_geometry(*ids, db)

    await db.engine.dispose()
    assert outcome.status == "error"
    assert outcome.provider_called is False
    assert outcome.error == "image_download_failed:RuntimeError:HTTP 404"


@pytest.mark.asyncio
async def test_geometry_error_followup_waits_until_retry_window(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "geometry-retry.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            title="Retry image",
            description="Описание",
            date="2026-08-01",
            time="18:00",
            location_name="Площадка",
            source_text="Источник",
        )
        session.add(event)
        await session.flush()
        poster = EventPoster(
            event_id=int(event.id),
            poster_hash="d" * 64,
            supabase_url="https://static.kenigevents.ru/retry.webp",
            review_status="approved",
            media_semantic_status="classified",
            media_semantic_prompt_version=event_media.MEDIA_ROLE_PROMPT_VERSION,
            media_semantic_context_hash=event_media._context_hash(event),
        )
        session.add(poster)
        await session.commit()
        ids = (int(event.id), int(poster.id))

    async def fail_geometry(event_id, poster_id, _db):
        return event_media.ImageGeometryOutcome(
            "error", poster_id, error="provider_failed", provider_called=True
        )

    monkeypatch.setattr(event_media, "analyze_event_poster_geometry", fail_geometry)
    before = event_media.datetime.now(event_media.timezone.utc)
    await event_media.review_next_event_media_pair(ids[0], db)

    async with db.get_session() as session:
        job = (
            await session.execute(
                event_media.select(event_media.JobOutbox).where(
                    event_media.JobOutbox.event_id == ids[0],
                    event_media.JobOutbox.task == event_media.JobTask.event_media_review,
                    event_media.JobOutbox.status == event_media.JobStatus.pending,
                )
            )
        ).scalar_one()
    await db.engine.dispose()
    assert job.next_run_at >= before.replace(tzinfo=None) + event_media.timedelta(hours=20)
