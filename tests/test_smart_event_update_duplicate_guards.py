import inspect

import pytest

from db import Database
from models import Event
import smart_event_update as su
from smart_event_update import EventCandidate, smart_event_update


async def _no_topics(*_args, **_kwargs):  # noqa: ANN001 - test helper
    return None


def test_match_create_prompt_distinguishes_time_conflict_from_multi_session():
    """INC-2026-06-25: same real event reposts with conflicting source times
    must be matched for LLM merge, while explicit multi-session schedules stay
    separate occurrences."""

    src = inspect.getsource(su._llm_match_or_create_bundle)
    assert "НЕ создавай отдельное событие только из-за этого" in src
    assert "одно событие с конфликтом/правкой времени" in src
    assert "один и тот же источник явно перечисляет несколько самостоятельных сеансов" in src
    assert "для новой самостоятельной occurrence" in src
    assert "Для регулярных/сезонных событий НЕ склеивай точную occurrence" in src
    assert "Выбирай `action=create` для новой occurrence" in src


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


async def _seed_yantar_valeria_event(db: Database) -> int:
    async with db.get_session() as session:
        ev = Event(
            title="Концерт Валерии",
            description="Существующая карточка концерта Валерии.",
            date="2026-07-01",
            time="",
            location_name="Янтарь холл",
            location_address="Ленина 11",
            city="Светлогорск",
            event_type="концерт",
            ticket_link="https://янтарьхолл.рф",
            source_text=(
                "Концерт Народной артистки России ВАЛЕРИИ, запланированный на 29 мая, "
                "переносится на 1 июля. Все купленные билеты действительны."
            ),
            source_post_url="https://t.me/yantarholl/4584",
            telegraph_url="https://telegra.ph/Koncert-Valerii-05-20",
            telegraph_path="Koncert-Valerii-05-20",
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


async def _seed_dachniki_event(db: Database) -> int:
    async with db.get_session() as session:
        ev = Event(
            title="Дачники",
            description="Существующая карточка спектакля.",
            date="2026-06-02",
            time="19:00",
            location_name="Драматический театр, Мира 4, Калининград",
            location_address="Мира 4",
            city="Калининград",
            event_type="спектакль",
            ticket_link="https://dramteatr39.ru/spektakli/dachniki",
            source_text=(
                "БАШНЯ-2026\n\n"
                "- 02.06 в 19:00 | Дачники\n"
                "- Школа-студия МХАТ (Москва)\n"
                "- Билеты и подробная информация https://dramteatr39.ru/spektakli/dachniki\n\n"
                "Дипломный спектакль IV курса Актерского факультета.\n"
                "В сегодняшней постановке именно эта несостоявшаяся любовь становится основной темой спектакля. "
                "Ее необходимо найти в любых ее проявлениях - абсурдных, нелепых, подчас жестоких, "
                "но столь необходимых театру для того, чтобы он сумел раскрыть человека."
            ),
            source_post_url="https://dramteatr39.ru/spektakli/dachniki",
            source_vk_post_url="https://vk.com/wall-132625599_17342",
            telegraph_url="https://telegra.ph/Dachniki-04-08",
            telegraph_path="Dachniki-04-08",
        )
        session.add(ev)
        await session.commit()
        return int(ev.id or 0)


async def _seed_westside_men_event(db: Database) -> int:
    async with db.get_session() as session:
        ev = Event(
            title="Род мужской",
            description="Кинопоказ киноклуба Westside Movieclub.",
            date="2026-06-12",
            time="20:30",
            location_name="ОКЦ на Горького",
            location_address="Горького 116",
            city="Калининград",
            event_type="кинопоказ",
            ticket_link="https://okts-na-gorkogo.timepad.ru/event/4024691",
            source_text=(
                "12 июня 20:30 - «Род мужской» (2026)\n"
                "📍Новый ОКЦ, Горького 116\n"
                "Билеты: https://okts-na-gorkogo.timepad.ru/event/4024691"
            ),
            source_post_url="https://t.me/westside_movieclub/6092",
        )
        session.add(ev)
        await session.commit()
        return int(ev.id or 0)


@pytest.mark.asyncio
async def test_citywide_music_night_location_drift_reaches_llm_match(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    llm_seen_ids: list[int] = []
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        async with db.get_session() as session:
            ev = Event(
                title="Калининградская музыкальная ночь",
                description="Городской музыкальный фестиваль на нескольких площадках.",
                date="2026-06-20",
                time="19:00",
                location_name="Дом китобоя",
                location_address="Мира 9",
                city="Калининград",
                event_type="фестиваль",
                source_text="Калининградская музыкальная ночь — городской фестиваль с разными площадками.",
                source_post_url="https://t.me/domkitoboya/3300",
                telegraph_url="https://telegra.ph/Kaliningradskaya-muzykalnaya-noch-06-15",
            )
            session.add(ev)
            await session.commit()
            await session.refresh(ev)
            eid = int(ev.id or 0)

        async def fake_match_or_create_bundle(candidate, events, **kwargs):
            llm_seen_ids.extend(int(ev.id or 0) for ev in events)
            return {
                "action": "match",
                "match_event_id": eid,
                "confidence": 0.92,
                "reason_short": "same citywide festival title/date/time; venue text drift",
            }

        async def fake_merge_event(*args, **kwargs):
            return {
                "description": "Городской музыкальный фестиваль.",
                "added_facts": [],
                "duplicate_facts": [],
                "conflict_facts": [],
                "skipped_conflicts": [],
            }

        monkeypatch.setattr(su, "_llm_match_or_create_bundle", fake_match_or_create_bundle)
        monkeypatch.setattr(su, "_llm_merge_event", fake_merge_event)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/meowafisha/7661",
            source_text=(
                "Калининградская музыкальная ночь 20 июня с 19:00 до 23:00. "
                "Это городской музыкальный фестиваль, вдохновлённый форматом Ural Music Night."
            ),
            title="Калининградская музыкальная ночь",
            date="2026-06-20",
            time="19:00",
            location_name="Ural Music Night",
            city="Калининград",
            event_type="фестиваль",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid
        assert eid in llm_seen_ids
        async with db.get_session() as session:
            rows = (
                await session.execute(
                    su.select(Event)
                    .where(Event.title == "Калининградская музыкальная ночь")
                    .order_by(Event.id)
                )
            ).scalars().all()
            assert [int(row.id or 0) for row in rows] == [eid]
    finally:
        await db.close()


def test_title_related_allows_russian_inflection_artist_title() -> None:
    assert su._titles_look_related("Валерия", "Концерт Валерии") is True
    assert su._titles_look_related("🎤 Концерт Валерии", "Валерия") is True


@pytest.mark.asyncio
async def test_high_confidence_llm_match_is_not_vetoed_by_title_wrapper_drift(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        eid = await _seed_yantar_valeria_event(db)

        async def fake_match_or_create_bundle(candidate, events, **kwargs):
            assert [int(ev.id or 0) for ev in events] == [eid]
            return {
                "action": "match",
                "match_event_id": eid,
                "confidence": 1.0,
                "reason_short": (
                    "Полное совпадение артиста, даты и площадки; новый источник "
                    "добавляет время концерта."
                ),
            }

        async def fake_merge_event(*args, **kwargs):
            return {
                "description": "Народная артистка России Валерия выступит в Янтарь-холле.",
                "added_facts": ["Время концерта: 19:00"],
                "duplicate_facts": [],
                "conflict_facts": [],
                "skipped_conflicts": [],
            }

        monkeypatch.setattr(su, "_llm_match_or_create_bundle", fake_match_or_create_bundle)
        monkeypatch.setattr(su, "_llm_merge_event", fake_merge_event)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/yantarholl/4727",
            source_chat_username="yantarholl",
            source_message_id=4727,
            source_text=(
                "1 июля в Янтарь-холле с сольным концертом выступит Народная артистка "
                "России — Валерия. Среди главных хитов артистки — «Часики», "
                "«Нежность моя», «Таю». Билеты на сайте Янтарь-холла."
            ),
            title="Большой сольный вечер",
            date="2026-07-01",
            time="19:00",
            location_name="Янтарь холл",
            location_address="Ленина 11",
            city="Светлогорск",
            event_type="концерт",
            ticket_link="https://янтарьхолл.рф/afisha/valeriya-solnyy-kontsert%2001%2007/",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid
        async with db.get_session() as session:
            rows = (
                await session.execute(
                    su.select(Event).where(Event.date == "2026-07-01").order_by(Event.id)
                )
            ).scalars().all()
            assert [int(ev.id or 0) for ev in rows] == [eid]
    finally:
        await db.close()


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
async def test_smart_update_merges_dachniki_when_location_is_prose_leak(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        eid = await _seed_dachniki_event(db)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/dramteatr39/4179",
            source_chat_id=1371643671,
            source_message_id=4179,
            source_text=(
                "БАШНЯ-2026\n\n"
                "02.06 в 19:00 | Дачники\n"
                "Школа-студия МХАТ (Москва)\n\n"
                "Дипломный спектакль IV курса Актерского факультета.\n"
                "В сегодняшней постановке именно эта несостоявшаяся любовь становится основной темой спектакля. "
                "Ее необходимо найти в любых ее проявлениях - абсурдных, нелепых, подчас жестоких, "
                "но столь необходимых театру для того, чтобы он сумел раскрыть человека."
            ),
            title="Дачники",
            date="2026-06-02",
            time="19:00",
            location_name=(
                "нелепых, подчас жестоких, но столь необходимых театру для того, "
                "чтобы он сумел раскрыть человека, стоящего на сцене во всем его многообразии и красоте."
            ),
            city="Калининград",
            event_type="спектакль",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid

        async with db.get_session() as session:
            rows = (
                await session.execute(
                    su.select(Event).where(Event.title == "Дачники").order_by(Event.id)
                )
            ).scalars().all()
            assert [int(ev.id or 0) for ev in rows] == [eid]
            assert rows[0].location_name == "Драматический театр, Мира 4, Калининград"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_rejects_unmatched_prose_location_candidate(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/example/1",
            source_text=(
                "02.06 в 19:00 | Новый спектакль\n"
                "Описание постановки без явной площадки."
            ),
            title="Новый спектакль",
            date="2026-06-02",
            time="19:00",
            location_name=(
                "нелепых, подчас жестоких, но столь необходимых театру для того, "
                "чтобы он сумел раскрыть человека"
            ),
            city="Калининград",
            event_type="спектакль",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.retry_reason is su.RetryReason.SOURCE_VERIFICATION_REQUIRED
        assert result.reason == "prose_location"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_rejects_temporal_location_candidate(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/barn_kaliningrad/1058",
            source_text=(
                "Завтра, 14 июня, в 12:00 в рамках ОП!ФЕСТА "
                "состоится экспериментальный пленэр."
            ),
            title="Экспериментальный пленэр",
            date="2026-06-14",
            time="12:00",
            location_name="🤗Завтра",
            location_address="Каштановая аллея 1а",
            city="Калининград",
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.retry_reason is su.RetryReason.SOURCE_VERIFICATION_REQUIRED
        assert result.reason == "prose_location"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smart_update_rejects_reaction_text_location_candidate(
    tmp_path,
    monkeypatch,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/molod_kld/3709",
            source_text="🏠 Дайджест, мы его очень ждали",
            title="Дайджест",
            date="2026-06-07",
            time="",
            location_name="мы его очень ждали",
            city="Калининград",
            posters=[
                su.PosterCandidate(
                    sha256="poster-digest",
                    ocr_text="1-7 июня",
                    ocr_title="Дайджест мероприятий",
                )
            ],
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.retry_reason is su.RetryReason.SOURCE_VERIFICATION_REQUIRED
        assert result.reason == "weak_eventness_review_uncertain"
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


@pytest.mark.asyncio
async def test_smart_update_recalls_same_ticket_duplicate_despite_wrong_default_location(
    tmp_path,
    monkeypatch,
):
    """INC-2026-06-12: wrong channel defaults must not remove a same-ticket
    duplicate from the Smart Update recall set before LLM/dedup matching."""

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")
        eid = await _seed_westside_men_event(db)

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/terkatalk/4990",
            source_chat_id=123,
            source_message_id=4990,
            source_text=(
                "12.06/13.06 | Women Power в киноклубе westside movieclub\n"
                "12 июня в 20:30 — «Род мужской» (2026)\n"
                "📍Новый ОКЦ, ул. Горького, 116\n"
                "Билеты: https://okts-na-gorkogo.timepad.ru/event/4024691/"
            ),
            title="Род мужской» (2026)",
            date="2026-06-12",
            time="20:30",
            location_name="Пространство Тёрка",
            location_address="Пл. Победы 4 (1 под. 2 этаж)",
            city="Калининград",
            ticket_link="https://okts-na-gorkogo.timepad.ru/event/4024691/",
            event_type="кинопоказ",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        assert int(result.event_id or 0) == eid
        async with db.get_session() as session:
            rows = (
                await session.execute(
                    su.select(Event).where(Event.date == "2026-06-12").order_by(Event.id)
                )
            ).scalars().all()
            assert [int(ev.id or 0) for ev in rows] == [eid]
            assert rows[0].location_name == "ОКЦ на Горького"
    finally:
        await db.close()
