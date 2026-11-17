from datetime import datetime, timedelta, timezone
import io

import pytest
from PIL import Image
from sqlmodel import select

import event_media
from db import Database
from models import Event, EventMediaPairReview, EventPoster, JobOutbox, JobStatus, JobTask


@pytest.mark.asyncio
@pytest.mark.parametrize("ready_extra", [False, True])
async def test_unavailable_mirror_does_not_repeat_gallery_or_hold_ready_media(
    tmp_path, monkeypatch, ready_extra,
):
    db = Database(str(tmp_path / "gallery.sqlite"))
    await db.init()
    downloads, reviews, enrichments = [], [], []
    try:
        async with db.get_session() as session:
            event = Event(title="Gallery", description="Source", source_text="Source",
                          date="2099-01-01", time="18:00", location_name="Venue")
            session.add(event)
            await session.flush()
            eid = int(event.id)
            approved = EventPoster(event_id=eid, poster_hash="ready-cover",
                supabase_url="https://static.example/cover.webp", review_status="approved",
                display_order=0)
            unavailable = EventPoster(event_id=eid, poster_hash="missing-image",
                catbox_url="https://source.example/missing.jpg", review_status="pending_review",
                review_reason="cdn_mirror_pending", display_order=1)
            session.add_all([approved, unavailable])
            if ready_extra:
                session.add(EventPoster(event_id=eid, poster_hash="new-ready-image",
                    supabase_url="https://static.example/new.webp", review_status="pending_review",
                    display_order=2))
            session.add(JobOutbox(event_id=eid, task=JobTask.event_media_review,
                status=JobStatus.error, next_run_at=datetime.now(timezone.utc) + timedelta(days=1),
                last_error="event_media_cdn_materialization_pending"))
            await session.commit()
            blocked_id = int(unavailable.id)

        async def download(poster):
            downloads.append(int(poster.id))
            if int(poster.id) == blocked_id:
                raise RuntimeError("missing_fixture_image")
            image = Image.new("RGB", (80, 60), "white")
            for x in range(80):
                for y in range(60):
                    image.putpixel((x, y), ((x * poster.id) % 256, (y * 7) % 256, (x+y) % 256))
            out = io.BytesIO()
            image.save(out, format="JPEG")
            return event_media.DownloadedPoster(out.getvalue(), "image/jpeg", poster.supabase_url)

        async def reviewer(**kwargs):
            reviews.append((int(kwargs["left"].id), int(kwargs["right"].id)))
            if "gallery" in kwargs:
                return {"images": [dict(poster_id=int(p.id), decision="distinct",
                    duplicate_kind="none", confidence=0.99, semantic_conflict=False,
                    canonical_side="either", reason_code="different_images", duplicate_of_id=None)
                    for p, _ in kwargs["gallery"]]}
            return dict(decision="distinct", duplicate_kind="none", confidence=0.99,
                semantic_conflict=False, canonical_side="either", reason_code="different_images")

        async def enrichment(event_id, database):
            enrichments.append(event_id)
            return False

        async def no_geometry(*args, **kwargs):
            return False

        monkeypatch.setattr(event_media, "_download_poster", download)
        monkeypatch.setattr(event_media, "_call_reviewer", reviewer)
        monkeypatch.setattr(event_media, "_review_event_media_enrichment", enrichment)
        monkeypatch.setattr(event_media, "_enqueue_geometry_followup_if_needed", no_geometry)
        await event_media.review_next_event_media_pair(eid, db)
        assert blocked_id not in downloads
        if ready_extra:
            assert len(downloads) == 2 and len(reviews) == 1
        else:
            assert not downloads and enrichments == [eid]
        async with db.get_session() as session:
            blocked = await session.get(EventPoster, blocked_id)
            assert blocked.review_status == "pending_review"
            assert blocked.review_reason == "cdn_mirror_pending"
            pairs = (await session.execute(select(EventMediaPairReview).where(
                EventMediaPairReview.event_id == eid))).scalars().all()
            assert all(blocked_id not in (p.left_poster_id, p.right_poster_id) for p in pairs)
            # Recovery clears the existing marker; the same image is eligible again.
            blocked.review_reason = "awaiting_automated_pair_review"
            blocked.supabase_url = "https://static.example/recovered.webp"
            session.add(blocked)
            await session.commit()
        downloads.clear()
        # This fixture image is now readable, with the same canonical ID.
        blocked_id_before = blocked_id
        blocked_id = -1
        await event_media.review_next_event_media_pair(eid, db)
        assert blocked_id_before in downloads
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_unmaterialized_gallery_does_not_choose_an_unavailable_seed(tmp_path):
    db = Database(str(tmp_path / "unavailable-seed.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            event = Event(title="Gallery", description="Source", source_text="Source",
                          date="2099-01-01", time="18:00", location_name="Venue")
            session.add(event)
            await session.flush()
            poster = EventPoster(event_id=int(event.id), poster_hash="missing-only",
                catbox_url="https://source.example/missing.jpg", review_status="pending_review",
                review_reason="cdn_mirror_pending")
            session.add(poster)
            await session.flush()
            assert await event_media.ensure_event_media_reviews(session, int(event.id)) == 0
            await session.commit()
            await session.refresh(poster)
            assert poster.review_status == "pending_review"
            assert poster.review_reason == "cdn_mirror_pending"
    finally:
        await db.close()
