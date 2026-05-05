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


async def _seed_dramteatr_zhenitba_event(db: Database) -> int:
    async with db.get_session() as session:
        ev = Event(
            title="Женитьба",
            description="Существующая карточка спектакля.",
            date="2026-05-01",
            time="19:00",
            location_name="Драматический театр",
            location_address="Мира 4",
            city="Калининград",
            event_type="спектакль",
            ticket_link="https://dramteatr39.ru/spektakli/jenitba",
            source_text="О спектакле «Женитьба». Ближайшие спектакли: 1 мая, 19:00.",
            source_post_url="https://dramteatr39.ru/spektakli/jenitba",
            telegraph_url="https://telegra.ph/ZHenitba-04-06",
            telegraph_path="ZHenitba-04-06",
        )
        session.add(ev)
        await session.commit()
        return int(ev.id or 0)


async def _seed_yantar_trofimov_event(db: Database) -> int:
    async with db.get_session() as session:
        ev = Event(
            title="Сергей Трофимов",
            description="Большой сольный концерт.",
            date="2026-05-17",
            time="",
            location_name="Янтарь холл, Ленина 11, Светлогорск",
            location_address="Ленина 11",
            city="Светлогорск",
            event_type="концерт",
            ticket_link="https://янтарьхолл.рф/afisha/sergey-trofimov%202026/?utm_source=tg",
            source_text="17 мая в Янтарь-холл большой сольный концерт Сергея Трофимова.",
            source_post_url="https://t.me/yantarholl/4304",
            telegraph_url="https://telegra.ph/Sergej-Trofimov-04-16",
            telegraph_path="Sergej-Trofimov-04-16",
        )
        session.add(ev)
        await session.commit()
        return int(ev.id or 0)


async def _seed_tretyakov_art_breakfast_event(db: Database) -> int:
    async with db.get_session() as session:
        ev = Event(
            title="Великие учителя. Арт-завтрак",
            description="Существующая карточка арт-завтрака.",
            date="2026-05-10",
            time="11:00",
            location_name="Филиал Третьяковской галереи, Парадная наб. 3, Калининград",
            location_address="Парадная наб. 3",
            city="Калининград",
            event_type="лекция",
            ticket_link="https://kaliningrad.tretyakovgallery.ru/tickets/#buy/event/46075",
            source_text=(
                "В филиале Третьяковской галереи в Калининграде стартует цикл арт-завтраков. "
                "10 мая пройдет первый арт-завтрак, посвященный выставке Великие учителя. "
                "Участники познакомятся с творчеством признанных русских мастеров."
            ),
            source_post_url="https://vk.com/wall-151577515_25061",
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
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
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
async def test_smart_update_merges_unsupported_default_time_duplicate(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        eid = await _seed_dramteatr_zhenitba_event(db)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/dramteatr39/4126",
            source_chat_id=1371643671,
            source_message_id=4126,
            source_text="01.05 | Женитьба",
            title="Женитьба",
            date="2026-05-01",
            time="18:00",
            time_is_default=True,
            location_name="Драматический театр",
            location_address="Мира 4",
            city="Калининград",
            ticket_link="https://dramteatr39.ru/spektakli/jenitba",
            event_type="спектакль",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_merges_same_specific_ticket_same_place_without_time(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        eid = await _seed_yantar_trofimov_event(db)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/yantarholl/4408",
            source_chat_id=1491770994,
            source_message_id=4408,
            source_text="17 мая в Янтарь-холл большой сольный концерт Сергея ТРОФИМОВА.",
            title="Сольный концерт Сергея ТРОФИМОВА",
            date="2026-05-17",
            time="",
            location_name="Янтарь холл, Ленина 11, Светлогорск",
            location_address="Ленина 11",
            city="Светлогорск",
            ticket_link="https://янтарьхолл.рф/afisha/sergey-trofimov%202026/",
            event_type="концерт",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_merges_near_identical_same_slot_copy_with_rewritten_title(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        eid = await _seed_tretyakov_art_breakfast_event(db)

        candidate = EventCandidate(
            source_type="vk",
            source_url="https://vk.com/wall-212760444_4883",
            source_chat_id=212760444,
            source_message_id=4883,
            source_text=(
                "Арт-завтрак В кругу великих. "
                "В филиале Третьяковской галереи в Калининграде стартует цикл арт-завтраков. "
                "10 мая пройдет первый арт-завтрак, посвященный выставке Великие учителя. "
                "Участники познакомятся с творчеством признанных русских мастеров."
            ),
            title="В кругу великих: арт-завтрак в Третьяковской галерее",
            date="2026-05-10",
            time="11:00",
            location_name="Филиал Третьяковской галереи, Парадная наб. 3, Калининград",
            location_address="Парадная наб. 3",
            city="Калининград",
            ticket_link="https://vk.cc/cX4omB",
            event_type="лекция",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid
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
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
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
