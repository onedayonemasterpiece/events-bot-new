import pytest

from db import Database
from models import Event
import smart_event_update as su
from smart_event_update import EventCandidate, smart_event_update


async def _no_topics(*_args, **_kwargs):  # noqa: ANN001 - test helper
    return None


async def _seed_club_znakomstv_event(db: Database) -> int:
    async with db.get_session() as session:
        ev = Event(
            title="ШОУ «КЛУБ ЗНАКОМСТВ»",
            description="Существующая карточка шоу.",
            date="2026-04-22",
            time="20:00",
            location_name="Форма пицца-бар, Гаражная 2б, Калининград",
            location_address="Гаражная 2",
            city="Калининград",
            event_type="шоу",
            ticket_link="https://clck.ru/3SZt9j",
            source_text=(
                "22.04 КОМЕДИЙНОЕ ШОУ «КЛУБ ЗНАКОМСТВ»\n"
                "Навсегда забудьте о скучных приложениях.\n"
                "📍 «Винный факультет», Гаражная 2\n"
                "🕖 Сбор гостей 19:00, начало 20:00\n"
                "Билеты по ссылке: https://clck.ru/3SZt9j"
            ),
            source_post_url="https://t.me/locostandup/3321",
            telegraph_url="https://telegra.ph/SHOU-KLUB-ZNAKOMSTV-04-16",
            telegraph_path="SHOU-KLUB-ZNAKOMSTV-04-16",
        )
        session.add(ev)
        await session.commit()
        return int(ev.id or 0)


@pytest.mark.asyncio
async def test_smart_update_merges_copy_post_same_day_text_when_ticket_link_differs(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        eid = await _seed_club_znakomstv_event(db)

        candidate = EventCandidate(
            source_type="vk",
            source_url="https://vk.com/wall-219175543_156",
            source_chat_id=219175543,
            source_message_id=156,
            source_text=(
                "22.04 КОМЕДИЙНОЕ ШОУ «КЛУБ ЗНАКОМСТВ»\n"
                "Навсегда забудьте о скучных приложениях.\n"
                "📍 «Винный факультет», Гаражная 2\n"
                "🕖 Сбор гостей 19:00, начало 20:00\n"
                "🎟 Билеты\n"
                "18+"
            ),
            title="Клуб знакомств: комедийное шоу",
            date="2026-04-22",
            time="20:00",
            location_name="Форма пицца-бар",
            location_address="Гаражная 2",
            city="Калининград",
            ticket_link="https://locostandup.ru/",
            event_type="концерт",
            emoji="🎤",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid

        async with db.get_session() as session:
            rows = (
                await session.execute(
                    su.select(Event).where(Event.date == "2026-04-22").order_by(Event.id)
                )
            ).scalars().all()
            assert [int(ev.id or 0) for ev in rows] == [eid]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_merges_doors_vs_start_duplicate_without_ticket_anchor(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        eid = await _seed_club_znakomstv_event(db)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/locostandup/3334",
            source_chat_id=1544118629,
            source_message_id=3334,
            source_text=(
                "22.04 КОМЕДИЙНОЕ ШОУ «КЛУБ ЗНАКОМСТВ»\n"
                "Навсегда забудьте о скучных приложениях.\n"
                "📍 «Винный факультет», Гаражная 2\n"
                "🕖 Сбор гостей 19:00, начало 20:00\n"
                "🎟 Билеты\n"
                "18+"
            ),
            title="Клуб Знакомств",
            date="2026-04-22",
            time="19:00",
            location_name="Форма пицца-бар, Гаражная 2б, Калининград",
            location_address="Гаражная 2",
            city="Калининград",
            ticket_link="https://locostandup.ru",
            event_type="шоу",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid

        async with db.get_session() as session:
            rows = (
                await session.execute(
                    su.select(Event).where(Event.date == "2026-04-22").order_by(Event.id)
                )
            ).scalars().all()
            assert [int(ev.id or 0) for ev in rows] == [eid]
            merged = rows[0]
            assert str(merged.time or "") == "20:00"
    finally:
        await db.close()
