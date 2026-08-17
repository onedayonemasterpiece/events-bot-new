from __future__ import annotations

import pytest
from sqlalchemy import select

import smart_event_update as su
from db import Database
from models import Event, EventSource
from smart_event_update import EventCandidate, PosterCandidate, smart_event_update


async def _no_topics(*_args, **_kwargs):  # noqa: ANN001 - test helper
    return None


@pytest.mark.asyncio
async def test_smart_update_recovers_source_own_name_for_generic_category_title(tmp_path, monkeypatch):
    """Replay INC-2026-06-30 title-loss shape through Smart Update create boundary."""

    # Keep the fixed 2026 fixture focused on title recovery as wall time moves.
    monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "SMART_UPDATE_G4_SPLIT_CREATE", False)
        monkeypatch.setattr(su, "SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE", False)
        monkeypatch.setattr(su, "SMART_UPDATE_DEDUP_ADJUDICATOR", False)

        async def _fake_bundle(*_args, **_kwargs):  # noqa: ANN001 - test helper
            return {
                "title": None,
                "description": (
                    "Городской фестиваль «ВЕЛОДЕНЬ» — праздник велокультуры "
                    "с детскими заездами, мастер-классами и велопарадом."
                ),
                "short_description": "Фестиваль «ВЕЛОДЕНЬ» объединяет детские заезды, мастер-классы и городской велопарад.",
                "search_digest": "Городской велофестиваль «ВЕЛОДЕНЬ» с заездами, мастер-классами и велопарадом.",
                "facts": [
                    "Название: Городской фестиваль «ВЕЛОДЕНЬ».",
                    "Дата: 12 июля 2026.",
                    "Время: 10:00.",
                    "Формат: детские заезды, мастер-классы, конкурсы и велопарад.",
                ],
            }

        async def _fake_recover(*_args, **_kwargs):  # noqa: ANN001 - test helper
            return "Городской фестиваль «ВЕЛОДЕНЬ»"

        monkeypatch.setattr(su, "_llm_create_description_facts_and_digest", _fake_bundle)
        monkeypatch.setattr(su, "_call_title_recovery_prompt", _fake_recover)

        async def _grounded_bundle(*_args, **_kwargs):  # noqa: ANN001 - test helper
            return True, "llm_grounded", []

        monkeypatch.setattr(su, "_llm_review_create_bundle_grounding", _grounded_bundle)

        source_text = (
            "Городской фестиваль «ВЕЛОДЕНЬ»\n"
            "12 июля в 10:00 на парковке у Правительства Калининградской области.\n"
            "В программе детские заезды, мастер-классы, конкурсы и велопарад 13 км."
        )
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/kulturnaya_chaika/7913",
            source_text=source_text,
            title="Городской фестиваль",
            date="2026-07-12",
            time="10:00",
            location_name="Парковка у Правительства Калининградской области",
            location_address="Дм.Донского 1",
            city="Калининград",
            ticket_link="https://example.test/register",
            ticket_status="registration",
            event_type="фестиваль",
            posters=[
                PosterCandidate(
                    ocr_title="ВЕЛОДЕНЬ / 12 ИЮЛЯ",
                    ocr_text="ВЕЛОДЕНЬ 12 ИЮЛЯ 10:00 Дм.Донского 1",
                )
            ],
        )

        result = await smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )

        assert result.status == "created"
        assert result.event_id
        async with db.get_session() as session:
            saved = await session.get(Event, int(result.event_id))
            assert saved is not None
            assert saved.title == "Городской фестиваль «ВЕЛОДЕНЬ»"
            assert "ВЕЛОДЕНЬ" in (saved.description or "")
            source_rows = (
                await session.execute(
                    select(EventSource).where(EventSource.event_id == int(result.event_id))
                )
            ).scalars().all()
            assert len(source_rows) == 1
            assert "Городской фестиваль «ВЕЛОДЕНЬ»" in (source_rows[0].source_text or "")
    finally:
        await db.close()
