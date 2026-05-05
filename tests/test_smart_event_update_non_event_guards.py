import smart_event_update as su
import pytest
from sqlalchemy import select

from db import Database
from models import Event
from smart_event_update import EventCandidate, smart_event_update


async def _no_topics(*_args, **_kwargs):  # noqa: ANN001 - test helper
    return None


def test_online_registration_does_not_make_offline_event_online_only() -> None:
    text = (
        "2 мая в округе пройдет большой велопробег. "
        "Маршрут около 30 км, старт от городского стадиона. "
        "Предварительная онлайн-регистрация: https://example.test/form"
    )

    assert su._looks_like_online_event("Велопробег в Гусеве", text) is False


def test_online_only_webinar_still_skips_as_online_event() -> None:
    text = "Вебинар пройдет в Zoom, ссылка на подключение придет после регистрации."

    assert su._looks_like_online_event("Онлайн-вебинар", text) is True


def test_festival_program_at_muzeynaya_alley_is_not_work_schedule() -> None:
    text = (
        "Зеленоградск, море, рыба — и наука? Именно так, 1 мая ИЦАЭ Калининграда "
        "везёт «Научную лужайку» на выставку морской гастрономии «ФИШтиваль».\n\n"
        "Что вас ждёт с 11:00 до 18:00:\n\n"
        "11:30 — Химическое шоу «Сумасшедшая наука».\n\n"
        "13:00 — Лекция «Наука морских путешествий».\n\n"
        "14:00 — Ток-шоу «Научный холодильник: рыба».\n\n"
        "Зеленоградск, Музейная аллея. Вход свободный, запись не нужна."
    )

    assert su._looks_like_work_schedule_notice("Химическое шоу «Сумасшедшая наука»", text) is False


def test_library_lecture_with_weekday_and_time_is_not_work_schedule() -> None:
    text = (
        "Открыли регистрацию на лекцию\n"
        "«Калининградский морской торговый порт: яркие страницы советской истории и современность»\n"
        "в рамках фестиваля «80 историй о главном».\n\n"
        "Порт - это место встречи моряков, портовиков, железнодорожников, автомобилистов.\n\n"
        "спикер: Евгения Нижегородцева\n"
        "аттестованный гид, экскурсовод.\n\n"
        "по регистрации\n\n"
        "вторник\n"
        "7 июля 18:30\n"
        "Библиотека А.П. Чехова, Московский проспект 36, Калининград"
    )

    assert (
        su._looks_like_work_schedule_notice(
            "Калининградский морской торговый порт: яркие страницы советской истории и современность",
            text,
        )
        is False
    )


def test_museum_work_hours_still_skip_as_work_schedule() -> None:
    text = (
        "График работы музея в праздничные дни:\n"
        "понедельник — выходной\n"
        "вторник с 10:00 до 18:00\n"
        "среда 10:00-19:00"
    )

    assert su._looks_like_work_schedule_notice("График работы музея", text) is True


def test_weekend_wording_without_work_hours_headline_stays_llm_owned() -> None:
    text = (
        "В выходные дни в библиотеке пройдут лекции и мастер-классы.\n"
        "Суббота, 18:30 — лекция о море.\n"
        "Воскресенье, 12:00 — семейный мастер-класс."
    )

    assert su._looks_like_work_schedule_notice("Лекции в библиотеке", text) is False


def test_rental_booking_availability_is_not_event() -> None:
    text = (
        "11 мая в АгроПарке «Некрасово поле» свободны купола. "
        "Можно забронировать купол для отдыха с семьей или компанией. "
        "Доступны три варианта, стоимость 1500 ₽ / 2500 ₽."
    )

    assert su._looks_like_rental_booking_not_event("Аренда куполов", text) is True


@pytest.mark.asyncio
async def test_zero_ticket_price_without_explicit_free_evidence_stays_not_free(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/kraftmarket39/196",
            source_text="Лекция по регистрации. Стоимость в посте не указана.",
            title="История парусного спорта",
            date="2026-05-19",
            time="16:00",
            location_name="Лекторий ОКЕАНиЯ",
            city="Калининград",
            ticket_price_min=0,
            ticket_price_max=0,
            is_free=None,
            event_type="лекция",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
        async with db.get_session() as session:
            saved = await session.get(Event, result.event_id)
            assert saved is not None
            assert saved.is_free is False
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_giveaway_does_not_mark_event_free(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/meowafisha/7288",
            source_text=(
                "БИЗОН МЕТАЛ ФЕСТ пройдет 17 мая в 18:00 в Yalta Club. "
                "Разыгрываем два билета, победитель получит билеты."
            ),
            title="БИЗОН МЕТАЛ ФЕСТ",
            date="2026-05-17",
            time="18:00",
            location_name="Yalta Club",
            city="Калининград",
            is_free=True,
            event_type="концерт",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
        async with db.get_session() as session:
            saved = await session.get(Event, result.event_id)
            assert saved is not None
            assert saved.is_free is False
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_exhibition_teaser_without_exact_date_is_skipped(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/domkitoboya/3191",
            source_text=(
                "Май, труд\n\n"
                "Выставка «Куплю гараж. Калининград», которую мы готовим совместно "
                "с Музеем Транспорта Москвы.\n\n"
                "Анонс через пару дней"
            ),
            title="Выставка «Куплю гараж. Калининград»",
            date="2026-05-02",
            location_name="Дом китобоя",
            location_address="Мира 9",
            city="Калининград",
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "skipped_non_event"
        assert result.reason == "unsupported_exhibition_teaser_date"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_dated_exhibition_with_curator_excursions_is_not_course_promo(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        source_text = (
            "13 мая в музее «Дом китобоя» откроется выставка "
            "«Куплю гараж. Калининград».\n\n"
            "Выставка будет работать ежедневно с 12 до 20 часов.\n"
            "Стоимость билета - 300 р.\n"
            "13 и 14 мая пройдут кураторские экскурсии. Начало в 15.00 и 19.00."
        )
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/domkitoboya/3193",
            source_text=source_text,
            title="Выставка «Куплю гараж. Калининград»",
            date="2026-05-13",
            location_name="Дом китобоя",
            location_address="Мира 9",
            city="Калининград",
            ticket_price_min=300,
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
        async with db.get_session() as session:
            saved = await session.get(Event, result.event_id)
            assert saved is not None
            assert saved.date == "2026-05-13"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_grounded_exhibition_date_corrects_inferred_legacy_range(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        async with db.get_session() as session:
            session.add(
                Event(
                    title="Выставка «Куплю гараж. Калининград»",
                    description="Анонс выставки.",
                    date="2026-05-02",
                    end_date="2026-06-02",
                    end_date_is_inferred=True,
                    time="",
                    location_name="Дом китобоя",
                    location_address="Мира 9",
                    city="Калининград",
                    source_text="Май, труд. Анонс через пару дней.",
                    source_post_url="https://t.me/domkitoboya/3191",
                    event_type="выставка",
                )
            )
            await session.commit()

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/domkitoboya/3193",
            source_text=(
                "13 мая в музее «Дом китобоя» откроется выставка "
                "«Куплю гараж. Калининград»."
            ),
            title="Выставка «Куплю гараж. Калининград»",
            date="2026-05-13",
            location_name="Дом китобоя",
            location_address="Мира 9",
            city="Калининград",
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "merged"
        async with db.get_session() as session:
            rows = (await session.execute(select(Event))).scalars().all()
            assert len(rows) == 1
            assert rows[0].date == "2026-05-13"
            assert rows[0].end_date == "2026-06-13"
            assert rows[0].end_date_is_inferred is True
    finally:
        await db.close()
