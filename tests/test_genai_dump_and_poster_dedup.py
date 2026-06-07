from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlmodel import select

import markup
import media_dedup
import smart_event_update as su
from db import Database
from models import Event, EventPoster
from smart_event_update import (
    PosterCandidate,
    _apply_posters,
    _dedup_near_duplicate_posters,
    _is_generic_title_event_type_venue,
    _llm_recover_event_title,
    _sanitize_description_output,
)

# A realistic stringified google-genai GenerateContentResponse (INC-2026-05-17):
# the model returned only a thought part, was truncated, and str(resp) leaked the
# whole object into the public post body.
LEAK = (
    "sdk_http_response=HttpResponse(headers=) candidates=[Candidate(content=Content("
    "parts=[Part(text=\"\"\"* Task: Edit/Rewrite a Markdown event announcement.\"\"\", "
    "thought=True )], role='model'), finish_reason=, index=0 )] "
    "model_version='gemma-4-31b-it' usage_metadata=GenerateContentResponseUsageMetadata("
    "prompt_token_count=2562, thoughts_token_count=1897 ) parsed=None"
)


def test_detector_flags_sdk_dump_and_spares_prose():
    assert markup.looks_like_genai_response_dump(LEAK) is True
    assert (
        markup.looks_like_genai_response_dump(
            "Концерт органной музыки в Кафедральном соборе. Билеты по регистрации."
        )
        is False
    )
    assert markup.looks_like_genai_response_dump("") is False
    assert markup.looks_like_genai_response_dump(None) is False
    # A single incidental marker must not trigger (>=2 required).
    assert markup.looks_like_genai_response_dump("see parts=[Part( in the docs") is False


def test_sanitize_for_vk_drops_dump_but_keeps_prose():
    assert markup.sanitize_for_vk(LEAK) == ""
    assert "Концерт" in markup.sanitize_for_vk("Концерт в соборе")


def test_sanitize_description_output_rejects_dump():
    assert _sanitize_description_output(LEAK, source_text="src") is None
    assert _sanitize_description_output("Обычное описание события.", source_text="src")


def test_hamming_distance_hex():
    a = "ff" * 32  # 64 hex chars == 256 bits
    b = a[:-1] + "e"  # flip one low bit of the last nibble
    assert media_dedup.hamming_distance_hex(a, a) == 0
    assert 0 < media_dedup.hamming_distance_hex(a, b) <= 4
    assert media_dedup.hamming_distance_hex(a, None) > 1000
    assert media_dedup.hamming_distance_hex(a, "ab") > 1000  # length mismatch sentinel


def _poster(phash=None, ocr_title=None, supabase_url=None, catbox_url=None, sha256=None):
    return PosterCandidate(
        catbox_url=catbox_url,
        supabase_url=supabase_url,
        sha256=sha256,
        phash=phash,
        ocr_title=ocr_title,
    )


def test_near_duplicate_posters_collapsed_keeping_best():
    ph = "ab" * 32
    ph_near = ph[:-1] + "a"  # 1-bit difference from ph -> within threshold
    p1 = _poster(phash=ph, ocr_title="", supabase_url="u1", sha256="s1")
    p2 = _poster(phash=ph_near, ocr_title="Concert poster text", supabase_url="u2", sha256="s2")
    out = _dedup_near_duplicate_posters([p1, p2])
    assert len(out) == 1
    # Higher-quality survivor (the one carrying OCR title) is kept.
    assert out[0].ocr_title == "Concert poster text"


def test_distinct_posters_preserved():
    p1 = _poster(phash="00" * 32, sha256="s1")
    p2 = _poster(phash="ff" * 32, sha256="s2")
    out = _dedup_near_duplicate_posters([p1, p2])
    assert len(out) == 2


def test_posters_without_phash_are_kept():
    p1 = _poster(phash=None, sha256="s1", catbox_url="c1")
    p2 = _poster(phash=None, sha256="s2", catbox_url="c2")
    out = _dedup_near_duplicate_posters([p1, p2])
    assert len(out) == 2


@pytest.mark.asyncio
async def test_apply_posters_dedupes_legacy_photo_urls_by_phash(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    legacy_url = "https://cdn.example/poster.jpg"
    managed_url = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/ab/"
        + ("ab" * 32)
        + ".webp"
    )

    async def fake_photo_hash(url):
        if url == legacy_url:
            return "ab" * 32
        return su._extract_dhash_from_managed_photo_url(url)

    monkeypatch.setattr(su, "_photo_url_dhash", fake_photo_hash)

    async with db.get_session() as session:
        event = Event(
            title="T",
            description="D",
            date="2026-06-01",
            time="20:00",
            location_name="Place",
            source_text="T",
            photo_urls=[legacy_url],
            photo_count=1,
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

        added, added_urls, _preview, pruned, changed = await _apply_posters(
            session,
            event.id,
            [
                PosterCandidate(
                    supabase_url=managed_url,
                    sha256="sha-new",
                    phash="ab" * 32,
                    ocr_title="T",
                )
            ],
            event_title="T",
        )
        await session.commit()
        await session.refresh(event)

    assert added == 1
    assert added_urls == [managed_url]
    assert pruned == 0
    assert changed is True
    assert event.photo_urls == [managed_url]
    assert event.photo_count == 1


@pytest.mark.asyncio
async def test_apply_posters_backfills_eventposter_phash_and_prunes_duplicate_rows(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    managed_url = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/ab/"
        + ("ab" * 32)
        + ".webp"
    )
    raw_url = "https://vk.example/raw.jpg"

    async def fake_photo_hash(url):
        if url == raw_url:
            return "ab" * 32
        return su._extract_dhash_from_managed_photo_url(url)

    monkeypatch.setattr(su, "_photo_url_dhash", fake_photo_hash)

    async with db.get_session() as session:
        event = Event(
            title="T",
            description="D",
            date="2026-06-01",
            time="20:00",
            location_name="Place",
            source_text="T",
            photo_urls=[managed_url, raw_url],
            photo_count=2,
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)
        session.add(
            EventPoster(
                event_id=event.id,
                supabase_url=managed_url,
                poster_hash="managed-sha",
                phash="ab" * 32,
            )
        )
        session.add(
            EventPoster(
                event_id=event.id,
                catbox_url=raw_url,
                poster_hash="raw-sha",
                phash=None,
            )
        )
        await session.commit()

        _added, _added_urls, _preview, pruned, changed = await _apply_posters(
            session,
            event.id,
            [],
            event_title="T",
        )
        await session.commit()
        await session.refresh(event)
        rows = (
            await session.execute(
                select(EventPoster).where(EventPoster.event_id == event.id)
            )
        ).scalars().all()

    assert pruned == 1
    assert changed is True
    assert event.photo_urls == [managed_url]
    assert event.photo_count == 1
    assert len(rows) == 1
    assert rows[0].supabase_url == managed_url


# --- Grounded title recovery (INC-2026-05-29 location-as-title) -----------------

def test_generic_title_detector():
    assert _is_generic_title_event_type_venue(
        "Концерт — Филармония им. Светланова",
        event_type="концерт",
        location_name="Филармония им. Светланова",
        city="Калининград",
    ) is True
    # A real title is not generic.
    assert _is_generic_title_event_type_venue(
        "Саундтреки на органе",
        event_type="концерт",
        location_name="Филармония им. Светланова",
        city="Калининград",
    ) is False


def _candidate(source_text, *, event_type="концерт", venue="Филармония им. Светланова"):
    return SimpleNamespace(
        source_text=source_text,
        raw_excerpt=None,
        location_name=venue,
        event_type=event_type,
        city="Калининград",
        source_type="vk",
        source_url="https://vk.com/wall-1_1",
        posters=[],
    )


def _patch_llm(monkeypatch, returned_title):
    async def _fake(prompt, *, max_tokens, label, temperature=0.0):
        return returned_title
    monkeypatch.setattr(su, "_ask_gemma_text", _fake)


def _patch_llm_sequence(monkeypatch, returned_titles):
    calls = iter(returned_titles)

    async def _fake(prompt, *, max_tokens, label, temperature=0.0):
        return next(calls)

    monkeypatch.setattr(su, "_ask_gemma_text", _fake)


@pytest.mark.asyncio
async def test_title_recovery_accepts_grounded(monkeypatch):
    cand = _candidate("⭐ Мелодии кино в живом звучании. «Саундтреки на органе». Мария Гаврилюк исполнит музыку из фильмов.")
    _patch_llm(monkeypatch, "Саундтреки на органе")
    out = await _llm_recover_event_title(cand, normalized_event_type="концерт", facts=[])
    assert out == "Саундтреки на органе"


@pytest.mark.asyncio
async def test_title_recovery_rejects_ungrounded(monkeypatch):
    cand = _candidate("Концерт органной музыки в соборе.")
    _patch_llm(monkeypatch, "Полёт валькирий и драконы")  # not in source
    out = await _llm_recover_event_title(cand, normalized_event_type="концерт", facts=[])
    assert out is None


@pytest.mark.asyncio
async def test_title_recovery_rejects_generic_result(monkeypatch):
    cand = _candidate("Какой-то текст про филармонию и концерт.")
    _patch_llm(monkeypatch, "Концерт — Филармония им. Светланова")  # still a placeholder
    out = await _llm_recover_event_title(cand, normalized_event_type="концерт", facts=[])
    assert out is None


@pytest.mark.asyncio
async def test_title_recovery_empty_result(monkeypatch):
    cand = _candidate("Текст без явного названия.")
    _patch_llm(monkeypatch, "")
    out = await _llm_recover_event_title(cand, normalized_event_type="концерт", facts=[])
    assert out is None


@pytest.mark.asyncio
async def test_title_recovery_falls_back_to_public_grounded_heading(monkeypatch):
    cand = _candidate(
        "Приглашаем на летний фестиваль фортепианной музыки Pianissimo. "
        "26 июня выступит Илья Папоян.",
        venue="Филиал Третьяковской галереи",
    )
    _patch_llm_sequence(monkeypatch, ["НЕТ", "Pianissimo: Илья Папоян"])
    out = await _llm_recover_event_title(
        cand,
        normalized_event_type="концерт",
        facts=["Фестиваль: Pianissimo", "Исполнитель: Илья Папоян"],
    )
    assert out == "Pianissimo: Илья Папоян"


@pytest.mark.asyncio
async def test_title_recovery_public_heading_requires_all_meaningful_tokens_grounded(monkeypatch):
    cand = _candidate("В центре внимания — Розовый натюрморт К. Петрова-Водкина.")
    _patch_llm_sequence(monkeypatch, ["НЕТ", "Розовый космос"])
    out = await _llm_recover_event_title(cand, normalized_event_type="концерт", facts=[])
    assert out is None
