import pytest
from sqlalchemy import select

from db import Database
from models import Event, EventSource, EventSourceFact
import smart_event_update as su
from smart_event_update import EventCandidate, smart_event_update


async def _no_topics(*_args, **_kwargs):  # noqa: ANN001 - test helper
    return None


@pytest.mark.asyncio
async def test_merge_filters_ungrounded_sensitive_facts_from_linked_source(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)

        async with db.get_session() as session:
            ev = Event(
                title="Весенний Экодвор",
                description="Старое описание.",
                date="2026-04-19",
                time="12:00-15:00",
                location_name="Железнодорожные ворота",
                city="Калининград",
                event_type="экология",
                source_text="ЭкоКёниг приглашает на Весенний Экодвор.",
                source_texts=["ЭкоКёниг приглашает на Весенний Экодвор."],
            )
            session.add(ev)
            await session.flush()

            src = EventSource(
                event_id=int(ev.id or 0),
                source_type="telegram",
                source_url="https://t.me/signalkld/10431",
                source_text="ЭкоКёниг приглашает на Весенний Экодвор.",
            )
            session.add(src)
            await session.flush()
            session.add(
                EventSourceFact(
                    event_id=int(ev.id or 0),
                    source_id=int(src.id or 0),
                    fact="Принимается органика и вторсырье к переработке.",
                    status="added",
                )
            )
            await session.commit()
            eid = int(ev.id or 0)

        async def _fake_merge(*_args, **_kwargs):  # noqa: ANN001 - test helper
            return {
                "title": "Весенний Экодвор",
                "description": "Нерелевантное merge-описание.",
                "search_digest": "Нерелевантный дайджест.",
                "ticket_link": None,
                "ticket_price_min": None,
                "ticket_price_max": None,
                "ticket_status": None,
                "added_facts": [
                    "Веломастерская будет проходить в формате ремонт-кафе.",
                    "Возрастное ограничение: 12+.",
                    "Максимальный размер группы — 4 человека.",
                    "Концерт продлится около двух часов с одним антрактом.",
                ],
                "duplicate_facts": [
                    "И. С. Бах — Токката и фуга ре минор",
                ],
                "conflict_facts": [],
                "skipped_conflicts": [],
            }

        async def _fake_ff_desc(*, facts_text_clean, **_kwargs):  # noqa: ANN001 - test helper
            return "\n".join(facts_text_clean)

        async def _fake_digest(**_kwargs):  # noqa: ANN001 - test helper
            return "Веломастерская и экологические практики на одном событии."

        async def _fake_short(**_kwargs):  # noqa: ANN001 - test helper
            return "Экодвор объединяет экологические практики, обмен и веломастерскую для всех гостей."

        monkeypatch.setattr(su, "_llm_merge_event", _fake_merge)
        monkeypatch.setattr(su, "_llm_fact_first_description_md", _fake_ff_desc)
        monkeypatch.setattr(su, "_llm_build_search_digest", _fake_digest)
        monkeypatch.setattr(su, "_llm_build_short_description", _fake_short)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/ecodvor39/735",
            source_text=(
                "Какая органика принимается на Весеннем Экодворе?\n"
                "Веломастерская будет проходить в формате ремонт-кафе.\n"
                "Весенний Экодвор пройдёт 19 апреля в Железнодорожных воротах."
            ),
            title="Весенний Экодвор",
            date="2026-04-19",
            time="12:00-15:00",
            location_name="Железнодорожные ворота",
            city="Калининград",
            event_type="экология",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)
        assert result.status == "merged"
        assert int(result.event_id or 0) == eid

        async with db.get_session() as session:
            updated = await session.get(Event, eid)
            assert updated is not None
            assert "ремонт-кафе" in (updated.description or "")
            assert "12+" not in (updated.description or "")
            assert "4 человека" not in (updated.description or "")
            assert "антракт" not in (updated.description or "")
            assert "Бах" not in (updated.description or "")

            rows = (
                await session.execute(
                    select(EventSourceFact.fact).where(EventSourceFact.event_id == eid)
                )
            ).all()
            facts = [str(row[0] or "") for row in rows]
            assert any("ремонт-кафе" in fact for fact in facts)
            assert all("12+" not in fact for fact in facts)
            assert all("4 человека" not in fact for fact in facts)
            assert all("антракт" not in fact for fact in facts)
            assert all("Бах" not in fact for fact in facts)
    finally:
        await db.close()
