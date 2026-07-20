from __future__ import annotations

import hashlib
import io
from types import SimpleNamespace

import pytest
from PIL import Image

import event_media
from db import Database
from media_dedup import (
    ImageFingerprints,
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
async def test_geometry_pixel_mismatch_is_not_current_and_is_selected(tmp_path) -> None:
    db = Database(str(tmp_path / "geometry-stale-pixel.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            title="Changed pixels",
            description="Описание",
            date="2026-08-01",
            time="18:00",
            location_name="Площадка",
            source_text="Источник",
        )
        session.add(event)
        await session.flush()
        geometry = EventImageGeometry(
            pixel_sha256="a" * 64,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        session.add(geometry)
        await session.flush()
        poster = EventPoster(
            event_id=int(event.id),
            poster_hash="stale-link",
            supabase_url="https://static.kenigevents.ru/changed.webp",
            review_status="approved",
            pixel_sha256="b" * 64,
            image_geometry_id=int(geometry.id),
        )
        session.add(poster)
        await session.commit()
        await session.refresh(poster)

        assert await event_media._current_geometry_for_poster(session, poster) is None
        assert await event_media._next_geometry_candidate_id(
            session, int(event.id)
        ) == int(poster.id)
    await db.engine.dispose()


def test_pixel_fingerprint_change_invalidates_geometry_and_semantic_crop() -> None:
    poster = EventPoster(
        event_id=1,
        poster_hash="fingerprint-change",
        pixel_sha256="a" * 64,
        image_geometry_id=9,
        media_semantic_status="classified",
        media_role="event_photo",
        focal_x=0.4,
        focal_y=0.6,
        safe_crop=True,
    )
    fp = ImageFingerprints(
        raw_sha256="c" * 64,
        pixel_sha256="b" * 64,
        dhash_hex="d" * 64,
        phash_hex="e" * 64,
        width=100,
        height=200,
        mime_type="image/webp",
    )

    event_media._apply_fingerprints(poster, fp)

    assert poster.raw_sha256 == "c" * 64
    assert poster.pixel_sha256 == "b" * 64
    assert poster.image_geometry_id is None
    assert poster.media_semantic_status == "pending"
    assert poster.media_semantic_reason_code == "pixel_identity_changed"
    assert poster.media_role is None
    assert poster.focal_x is None and poster.focal_y is None
    assert poster.safe_crop is False


@pytest.mark.asyncio
async def test_coalesced_pending_media_job_is_moved_earlier_for_ready_work(
    tmp_path,
) -> None:
    db = Database(str(tmp_path / "media-job-coalesce.sqlite"))
    await db.init()
    now = event_media.datetime.now(event_media.timezone.utc)
    async with db.get_session() as session:
        assert await event_media.enqueue_event_media_review_job(
            session,
            123,
            next_run_at=now + event_media.timedelta(hours=20),
            force_followup=True,
        )
        assert not await event_media.enqueue_event_media_review_job(
            session,
            123,
            next_run_at=now + event_media.timedelta(seconds=7),
            force_followup=True,
        )
        await session.commit()
        job = (
            await session.execute(
                event_media.select(event_media.JobOutbox).where(
                    event_media.JobOutbox.event_id == 123
                )
            )
        ).scalar_one()
    await db.engine.dispose()
    next_run_at = job.next_run_at
    if next_run_at.tzinfo is None:
        next_run_at = next_run_at.replace(tzinfo=event_media.timezone.utc)
    assert next_run_at == now + event_media.timedelta(seconds=7)


@pytest.mark.asyncio
async def test_future_semantic_retry_remains_armed_after_geometry_is_current(
    tmp_path,
) -> None:
    db = Database(str(tmp_path / "semantic-retry-armed.sqlite"))
    await db.init()
    now = event_media.datetime.now(event_media.timezone.utc)
    retry_at = now + event_media.timedelta(minutes=10)
    async with db.get_session() as session:
        event = Event(
            title="Retry remains armed",
            description="Описание",
            date="2026-08-01",
            time="18:00",
            location_name="Площадка",
            source_text="Источник",
        )
        session.add(event)
        await session.flush()
        geometry = EventImageGeometry(
            pixel_sha256="a" * 64,
            model=event_media.image_geometry_model(),
            prompt_version=event_media.IMAGE_GEOMETRY_PROMPT_VERSION,
            status="classified",
        )
        session.add(geometry)
        await session.flush()
        poster = EventPoster(
            event_id=int(event.id),
            poster_hash="future-semantic-retry",
            supabase_url="https://static.kenigevents.ru/retry.webp",
            review_status="approved",
            pixel_sha256="a" * 64,
            image_geometry_id=int(geometry.id),
            media_semantic_status="pending",
            media_semantic_prompt_version=event_media.MEDIA_ROLE_PROMPT_VERSION,
            media_semantic_context_hash=event_media._context_hash(event),
            media_semantic_classified_at=retry_at,
        )
        session.add(poster)
        await session.commit()
        event_id = int(event.id)

    assert await event_media.review_next_event_media_pair(event_id, db) is False
    async with db.get_session() as session:
        job = (
            await session.execute(
                event_media.select(event_media.JobOutbox).where(
                    event_media.JobOutbox.event_id == event_id
                )
            )
        ).scalar_one()
    await db.engine.dispose()
    job_at = job.next_run_at
    if job_at.tzinfo is None:
        job_at = job_at.replace(tzinfo=event_media.timezone.utc)
    assert job_at == retry_at


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


@pytest.mark.asyncio
async def test_pending_semantic_role_does_not_starve_geometry(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "geometry-semantic-budget.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            title="Independent geometry budget",
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
            poster_hash="e" * 64,
            supabase_url="https://static.kenigevents.ru/geometry.webp",
            review_status="approved",
            media_semantic_status="pending",
        )
        session.add(poster)
        await session.commit()
        ids = (int(event.id), int(poster.id))

    calls: list[tuple[str, int]] = []

    async def exhausted_semantic(event_id, poster_id, _db):
        calls.append(("semantic", int(poster_id)))
        return False

    async def classify_geometry(event_id, poster_id, _db):
        calls.append(("geometry", int(poster_id)))
        return event_media.ImageGeometryOutcome(
            "classified", poster_id, provider_called=True
        )

    monkeypatch.setattr(
        event_media, "_classify_event_poster_role", exhausted_semantic
    )
    monkeypatch.setattr(
        event_media, "analyze_event_poster_geometry", classify_geometry
    )

    changed = await event_media.review_next_event_media_pair(ids[0], db)

    await db.engine.dispose()
    assert changed is True
    assert calls == [("semantic", ids[1]), ("geometry", ids[1])]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_error", "minimum_delay_seconds"),
    [
        ("429 RESOURCE_EXHAUSTED rpm", 590),
        ("RateLimitError:rpd", 3600),
        ("TimeoutError:timed out", 890),
    ],
)
async def test_transient_media_role_failure_is_delayed_and_uses_normal_key_pool(
    tmp_path, monkeypatch, provider_error, minimum_delay_seconds
) -> None:
    db = Database(str(tmp_path / f"semantic-retry-{minimum_delay_seconds}.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            title="Semantic retry",
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
            poster_hash="semantic-retry",
            supabase_url="https://static.kenigevents.ru/retry.webp",
            review_status="approved",
            media_semantic_status="pending",
        )
        session.add(poster)
        await session.commit()
        ids = int(event.id), int(poster.id)

    async def fake_download(_poster):
        return event_media.DownloadedPoster(
            data=_jpeg_bytes(64, 48),
            mime_type="image/jpeg",
            source_url="https://static.kenigevents.ru/retry.webp",
        )

    async def fake_budget(*_args):
        return True

    captured_init: dict[str, object] = {}

    class FakeGoogleAIClient:
        def __init__(self, **kwargs):
            captured_init.update(kwargs)

        async def generate_content_async(self, **_kwargs):
            raise RuntimeError(provider_error)

    import google_ai
    import main

    monkeypatch.setattr(event_media, "_download_poster", fake_download)
    monkeypatch.setattr(event_media, "_claim_feature_budget", fake_budget)
    monkeypatch.setattr(google_ai, "GoogleAIClient", FakeGoogleAIClient)
    monkeypatch.setattr(google_ai, "SecretsProvider", lambda: object())
    monkeypatch.setattr(main, "get_supabase_client", lambda: object())
    monkeypatch.setenv(
        "EVENT_MEDIA_ROLE_GOOGLE_KEY_ENVS", "GOOGLE_API_KEY4,GOOGLE_API_KEY5"
    )

    before = event_media.datetime.now(event_media.timezone.utc)
    assert await event_media._classify_event_poster_role(*ids, db) is False

    async with db.get_session() as session:
        poster = await session.get(EventPoster, ids[1])
        job = (
            await session.execute(
                event_media.select(event_media.JobOutbox).where(
                    event_media.JobOutbox.event_id == ids[0],
                    event_media.JobOutbox.status == event_media.JobStatus.pending,
                )
            )
        ).scalar_one()
    await db.engine.dispose()
    retry_at = poster.media_semantic_classified_at
    job_at = job.next_run_at
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=event_media.timezone.utc)
    if job_at.tzinfo is None:
        job_at = job_at.replace(tzinfo=event_media.timezone.utc)
    assert poster.media_semantic_status == "pending"
    assert poster.media_semantic_reason_code.startswith("transient_provider_error:")
    assert retry_at >= before + event_media.timedelta(seconds=minimum_delay_seconds)
    assert job_at == retry_at
    assert captured_init["default_env_var_name"] == "GOOGLE_API_KEY4"
    assert captured_init["reserve_key_envs"] == [
        "GOOGLE_API_KEY4",
        "GOOGLE_API_KEY5",
    ]
    assert captured_init["reserve_overflow_key_envs"] == []


@pytest.mark.asyncio
async def test_invalid_media_role_response_remains_permanent_error(
    tmp_path, monkeypatch
) -> None:
    db = Database(str(tmp_path / "semantic-permanent.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = Event(
            title="Semantic invalid",
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
            poster_hash="semantic-invalid",
            supabase_url="https://static.kenigevents.ru/invalid.webp",
            review_status="approved",
            media_semantic_status="pending",
        )
        session.add(poster)
        await session.commit()
        ids = int(event.id), int(poster.id)

    async def fake_download(_poster):
        return event_media.DownloadedPoster(
            data=_jpeg_bytes(64, 48),
            mime_type="image/jpeg",
            source_url="https://static.kenigevents.ru/invalid.webp",
        )

    async def fake_budget(*_args):
        return True

    class FakeGoogleAIClient:
        def __init__(self, **_kwargs):
            pass

        async def generate_content_async(self, **_kwargs):
            return "{}", None

    import google_ai
    import main

    monkeypatch.setattr(event_media, "_download_poster", fake_download)
    monkeypatch.setattr(event_media, "_claim_feature_budget", fake_budget)
    monkeypatch.setattr(google_ai, "GoogleAIClient", FakeGoogleAIClient)
    monkeypatch.setattr(google_ai, "SecretsProvider", lambda: object())
    monkeypatch.setattr(main, "get_supabase_client", lambda: object())

    assert await event_media._classify_event_poster_role(*ids, db) is False
    async with db.get_session() as session:
        poster = await session.get(EventPoster, ids[1])
        jobs = (
            await session.execute(
                event_media.select(event_media.JobOutbox).where(
                    event_media.JobOutbox.event_id == ids[0]
                )
            )
        ).scalars().all()
    await db.engine.dispose()
    assert poster.media_semantic_status == "error"
    assert poster.media_semantic_reason_code == "invalid_or_low_confidence_media_role"
    assert jobs == []
