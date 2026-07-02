from __future__ import annotations

import pytest
from sqlmodel import select

import smart_event_update as su
from db import Database
from models import Event, EventPoster
from smart_event_update import EventCandidate, PosterCandidate, _apply_posters


def test_date_provenance_ladder_prefers_grounded_channels() -> None:
    source_candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/source/1",
        source_text="Концерт состоится 12 июля в 19:00.",
        title="Концерт",
        date="2026-07-12",
    )
    poster_candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_2",
        source_text="Анонс концерта.",
        title="Концерт",
        date="2026-07-12",
        posters=[PosterCandidate(ocr_text="КОНЦЕРТ 12.07")],
    )
    parser_candidate = EventCandidate(
        source_type="parser:venue",
        source_url="https://venue.example/event",
        source_text="Анонс без даты в тексте карточки.",
        title="Концерт",
        date="2026-07-12",
    )

    assert su._candidate_date_provenance_level(source_candidate) == su.DATE_PROVENANCE_SOURCE_TEXT
    assert su._candidate_date_provenance_level(poster_candidate) == su.DATE_PROVENANCE_POSTER_OCR
    assert (
        su._candidate_date_provenance_level(parser_candidate, is_canonical_site=True)
        == su.DATE_PROVENANCE_CANONICAL_SOURCE
    )
    assert su._date_provenance_trust_rank(su.DATE_PROVENANCE_CANONICAL_SOURCE) > su._date_provenance_trust_rank(
        su.DATE_PROVENANCE_POSTER_OCR
    )


def test_conservative_date_update_allows_only_safe_merge_cases() -> None:
    event = Event(
        title="Выставка",
        description="D",
        date="2026-06-01",
        end_date="2026-07-01",
        end_date_is_inferred=True,
        location_name="Музей",
        event_type="выставка",
        source_text="old",
    )
    grounded = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/source/1",
        source_text="Выставка откроется 12 июня.",
        title="Выставка",
        date="2026-06-12",
        location_name="Музей",
        event_type="выставка",
    )
    ungrounded = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/source/2",
        source_text="Выставка скоро откроется.",
        title="Выставка",
        date="2026-06-20",
        location_name="Музей",
        event_type="выставка",
    )

    assert su._can_apply_conservative_date_update(event, grounded, is_canonical_site=False) == (
        True,
        "inferred_long_event_grounded",
    )
    assert su._can_apply_conservative_date_update(event, ungrounded, is_canonical_site=False) == (
        False,
        "no_update",
    )
    assert su._can_apply_conservative_date_update(event, ungrounded, is_canonical_site=True) == (
        True,
        "canonical_source",
    )


def test_poster_identity_dedup_collapses_candidate_batch_by_supabase_path() -> None:
    weak = PosterCandidate(
        supabase_url="https://storage.example/old.webp",
        supabase_path="posters/event.webp",
        sha256="sha-old",
    )
    rich = PosterCandidate(
        supabase_url="https://storage.example/new.webp",
        supabase_path="posters/event.webp",
        sha256="sha-new",
        ocr_title="Event poster title",
    )

    deduped = su._dedup_poster_candidates_by_identity([weak, rich])

    assert len(deduped) == 1
    assert deduped[0].supabase_url == "https://storage.example/new.webp"


def test_poster_identity_index_uses_strong_keys_before_url_fallback() -> None:
    row = EventPoster(
        event_id=1,
        poster_hash="old-sha",
        supabase_path="posters/event.webp",
        supabase_url="https://storage.example/event.webp",
        phash="ab" * 32,
    )
    index = su._build_eventposter_identity_index([row])

    by_path = PosterCandidate(supabase_path="posters/event.webp", sha256="new-sha")
    by_url = PosterCandidate(catbox_url="https://storage.example/event.webp")

    assert su._find_duplicate_eventposter_by_identity(by_path, index) == (row, "supabase_path")
    assert su._find_duplicate_eventposter_by_identity(by_url, index) == (row, "url")


@pytest.mark.asyncio
async def test_apply_posters_merges_existing_row_by_supabase_path(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        event = Event(
            title="T",
            description="D",
            date="2026-06-01",
            time="20:00",
            location_name="Place",
            source_text="T",
            photo_urls=["https://storage.example/old.webp"],
            photo_count=1,
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)
        session.add(
            EventPoster(
                event_id=event.id,
                poster_hash="old-sha",
                supabase_path="posters/event.webp",
                supabase_url="https://storage.example/old.webp",
            )
        )
        await session.commit()

        added, added_urls, _preview, pruned, changed = await _apply_posters(
            session,
            event.id,
            [
                PosterCandidate(
                    supabase_path="posters/event.webp",
                    supabase_url="https://storage.example/new.webp",
                    sha256="new-sha",
                    ocr_title="T",
                )
            ],
            event_title="T",
        )
        await session.commit()
        await session.refresh(event)
        rows = (
            await session.execute(select(EventPoster).where(EventPoster.event_id == event.id))
        ).scalars().all()

    assert added == 0
    assert added_urls == ["https://storage.example/new.webp"]
    assert pruned == 0
    assert changed is True
    assert len(rows) == 1
    assert rows[0].poster_hash == "old-sha"
    assert rows[0].supabase_url == "https://storage.example/new.webp"
    assert event.photo_urls == ["https://storage.example/new.webp"]
