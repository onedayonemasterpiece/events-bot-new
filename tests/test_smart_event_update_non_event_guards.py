import smart_event_update as su
import pytest
from sqlalchemy import select

from db import Database
from models import Event
from smart_event_update import EventCandidate, PosterCandidate, smart_event_update


async def _no_topics(*_args, **_kwargs):  # noqa: ANN001 - test helper
    return None


async def _grounded_bundle_review():
    return True, "llm_grounded", []


def _historical_museum_interview_source() -> str:
    return (
        "ИСТОРИЯ ИЗ ПЕРВЫХ УСТ: к 80-летию КОИХМ. "
        "Она работает в музее с 1978 года. В июне 1979 года привезли останки "
        "Кристионаса Донелайтиса. 11 октября 1979 года состоялось открытие музея. "
        "Из интервью журналу «Музеи 39», апрель 2026."
    )


def test_historical_museum_interview_routes_to_llm_eventness() -> None:
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/koihm/5936",
        source_text=_historical_museum_interview_source(),
        title="Литературный музей в Чистых Прудах",
        date="2026-10-11",
        location_name="Историко-художественный музей",
        city="Калининград",
    )

    assert su._has_historical_anniversary_interview_risk(candidate, candidate.source_text) is True
    assert su._candidate_needs_llm_eventness_review(candidate, candidate.source_text) is True


@pytest.mark.asyncio
async def test_historical_museum_interview_llm_rejects_without_future_announcement(monkeypatch) -> None:
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/koihm/5936",
        source_text=_historical_museum_interview_source(),
        title="Литературный музей в Чистых Прудах",
        date="2026-10-11",
        location_name="Историко-художественный музей",
        city="Калининград",
    )

    async def _ask(prompt, _schema, **kwargs):
        assert kwargs["label"] == "eventness_review"
        assert "музейная летопись" in prompt
        return {"decision": "non_event", "confidence": 0.99, "reason_short": "historical interview"}

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "_ask_gemma_json", _ask)
    decision, confidence, _ = await su._llm_review_candidate_eventness(
        candidate,
        clean_title=candidate.title,
        clean_source_text=candidate.source_text,
        clean_raw_excerpt=None,
    )
    assert (decision, confidence) == ("non_event", 0.99)


@pytest.mark.asyncio
async def test_real_future_museum_lecture_survives_historical_context_review(monkeypatch) -> None:
    source = (
        "Музей открыт в 1979 году. 11 октября 2026 года в 15:00 приглашаем на лекцию "
        "«История музея». Место: Литературный музей в Чистых Прудах."
    )
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/koihm/control",
        source_text=source,
        title="Лекция «История музея»",
        date="2026-10-11",
        time="15:00",
        location_name="Литературный музей в Чистых Прудах",
        city="Чистые Пруды",
    )

    async def _ask(*_args, **_kwargs):
        return {"decision": "event", "confidence": 0.98, "reason_short": "explicit future invite"}

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(su, "_ask_gemma_json", _ask)
    decision, confidence, _ = await su._llm_review_candidate_eventness(
        candidate,
        clean_title=candidate.title,
        clean_source_text=source,
        clean_raw_excerpt=None,
    )
    assert (decision, confidence) == ("event", 0.98)


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


def test_zoo_excursion_with_discount_ticket_wording_is_not_non_event_notice() -> None:
    text = (
        "10 мая 11:00\n"
        "Авторская экскурсия «Я работаю в зоопарке!»\n"
        "Приходите, чтобы взглянуть на зоопарк под другим углом. "
        "Стоимость участия: 500 руб./чел. + входной билет "
        "(взрослый - 600 руб., льготный - 300 руб., детский - 300 руб.)."
    )

    assert su._looks_like_non_event_notice("Авторская экскурсия «Я работаю в зоопарке!»", text) is False


def test_source_grounded_known_spelling_guard_restores_performer_name() -> None:
    source = 'Премьера оратории [id210697448|Евы Симуран] "Возрождение".'
    generated = "Произведение Евы Симюран посвящено Дню Победы."

    assert (
        su._restore_source_grounded_known_spellings(generated, source_text=source)
        == "Произведение Евы Симуран посвящено Дню Победы."
    )


@pytest.mark.asyncio
async def test_physical_art_market_with_stream_word_in_program_is_not_online_skip(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        source_text = (
            "10 мая с 12:00 до 19:00 в Культурном месте на Острове Канта "
            "пройдет арт маркет. В программе: 12:00 - трансляция концерта, "
            "14:00 - джаз-хоп концерт. Приходите за авторскими работами."
        )
        candidate = EventCandidate(
            source_type="vk",
            source_url="https://vk.com/wall-138053522_2532",
            source_text=source_text,
            title="Арт-маркет в Культурном месте",
            date="2026-05-10",
            time="12:00",
            location_name="Остров Канта",
            city="Калининград",
            event_type="ярмарка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
    finally:
        await db.close()


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


def test_80_stories_architecture_lecture_is_not_work_schedule() -> None:
    text = (
        "Архитектура советского Калининграда (1946 - 1960 годы)\n\n"
        "Александр Николаевич Попадин\n"
        "писатель, культуролог, краевед\n\n"
        "О чём поговорим\n"
        "Какие первоочередные задачи стояли перед архитектурным цехом в новом советском городе?\n"
        "Дизайн-код «сталинского Калининграда» на главной улице города.\n\n"
        "🆓🆓🆓🆓, по регистрации\n\n"
        "суббота\n"
        "30 мая 15:00\n"
        "Историко-художественный музей, Клиническая 21, Калининград\n\n"
        "#80_историй_о_главном"
    )

    assert (
        su._looks_like_work_schedule_notice(
            "Архитектура советского Калининграда",
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


def test_completed_festival_recap_with_unconfirmed_next_location_is_non_event() -> None:
    source_text = (
        "Калининград, спасибо!\n"
        "Было классно\n\n"
        "Стояли солнечные выходные, но все кто пришёл на Гаражку — лучшие.\n\n"
        "Спасибо мастерам — что не уехали на море, а радовали нас своим творчеством.\n"
        "Спасибо гостям — что вы с нами!\n\n"
        "Следующий фестиваль:\n"
        "5–6 сентября\n"
        "Локация уточняется!\n"
        "До сентября."
    )
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/garazhka_kld/1505",
        source_text=source_text,
        title="Гаражка",
        date="2026-09-05",
        end_date="2026-09-06",
        location_name="спасибо!",
        location_address="Мира 9",
        city="Калининград",
        festival="Гаражка",
        festival_source=True,
        festival_series="Гаражка",
    )

    assert (
        su._looks_like_completed_event_report_not_event(
            "Гаражка",
            source_text,
            candidate=candidate,
        )
        is True
    )


def _e6691_retrocar_recap_source() -> str:
    return (
        "Прошедшая в День города Калининграда выставка ретроавтомобилей стала машиной времени.\n"
        "Несмотря на пугающий прогноз дождя, атмосфера на мероприятии была невероятно тëплой "
        "и ностальгической.\n"
        "На празднике были вручены заслуженные кубки в номинациях «Автомобили-юбиляры», "
        "«Вклад в советский кинематограф», «Автомобиль детской мечты» и «Приз зрительских симпатий».\n"
        "Огромную благодарность за помощь в организации праздника выражаем Калининградскому "
        "областному историко-художественному музею за предоставленную отличную площадку.\n"
        "Если вы не успели рассмотреть автомобили, не огорчайтесь. Мы ждём вас на нашей следующей "
        "выставке 11 июля в городе Светлом на праздновании Дня города и Дня рыбака!\n"
        "Спасибо всем участникам выставки!"
    )


def _e6853_retrocar_recap_source() -> str:
    return (
        "11 июля наш клуб провел в потрясающей атмосфере Дня города Светлого и Дня рыбака.\n"
        "Хотим выразить искреннюю благодарность администрации города. Мероприятие было "
        "организовано на высочайшем уровне!\n"
        "АвтоРетроКлуб тоже подготовил сюрприз для горожан. Помимо традиционной выставки "
        "редких машин, где блистал Mercedes Benz 230 Cabriolet W 143 1938 года, мы стали "
        "частью сценки «Свадебный выезд» с ГАЗ-24-10 «Волга».\n"
        "Огромное спасибо каждому участнику выездной выставки. Светлый, спасибо за "
        "гостеприимство!\n"
        "А мы не сбавляем обороты. Впереди новые дороги и новые встречи.\n"
        "Увидимся уже в следующую субботу, 18 июля, на Дне города в Янтарном!"
    )


def test_mixed_occurrence_router_covers_e6853_without_deciding_eventness() -> None:
    source_text = _e6853_retrocar_recap_source()
    assert su._has_retrospective_future_teaser_shape("День города в Янтарном", source_text) is False
    assert su._has_mixed_occurrence_role_risk("День города в Янтарном", source_text) is True


@pytest.mark.asyncio
async def test_e6853_replay_llm_fails_closed_before_duration_or_create(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        async def _review(*_args, **_kwargs):
            return "non_event", 0.97, "past details do not ground the future occurrence"

        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _review)

        def _must_not_infer(_candidate):
            raise AssertionError("duration fallback ran before mixed-occurrence review")

        monkeypatch.setattr(su, "_maybe_apply_default_end_date_for_long_event", _must_not_infer)
        candidate = EventCandidate(
            source_type="vk",
            source_url="https://vk.com/wall-127107743_14707",
            source_text=_e6853_retrocar_recap_source(),
            title="День города в Янтарном",
            date="2026-07-18",
            end_date="2026-08-18",
            end_date_is_inferred=True,
            location_name="Калининград Сити Джаз Клуб",
            location_address="Мира 33-35",
            city="Калининград",
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.retry_reason is su.RetryReason.SOURCE_VERIFICATION_REQUIRED
        assert result.reason == "mixed_occurrence_role_review_non_event"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_mixed_recap_grounded_future_event_keeps_one_day_shape(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "_classify_topics", _no_topics)

        async def _review(*_args, **_kwargs):
            return "event", 0.96, "future date and venue are explicit"

        monkeypatch.setattr(su, "_llm_review_candidate_eventness", _review)
        source_text = (
            "Спасибо гостям прошлой выставки 11 июля. Впереди новая встреча. "
            "Следующая выставка ретроавтомобилей состоится 18 июля в Янтарном, "
            "место: площадь Мастеров. Ждём вас!"
        )
        candidate = EventCandidate(
            source_type="vk",
            source_url="https://vk.com/wall-127107743_14708",
            source_text=source_text,
            title="Выставка ретроавтомобилей",
            date="2026-07-18",
            location_name="площадь Мастеров",
            city="Янтарный",
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status in {"created", "merged"}
        assert candidate.end_date is None
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_roundup_scope_review_selects_only_target_block(monkeypatch) -> None:
    source_text = (
        "15 июля в 18.00 — концерт «Ритмы огня»\n"
        "Фламенко, гитара и кастаньеты.\n"
        "16 июля в 18.00 — концерт «Песни от сердца»\n"
        "Песни Эдит Пиаф и Уитни Хьюстон.\n"
        "19 июля в 18.00 — концерт «Мелодии любви»\n"
        "Олег Яковенко исполнит песни Азнавура и Талькова.\n"
        "После каждого представления — авторский ужин."
    )
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-194927034_4750",
        source_text=source_text,
        title="Концерт «Ритмы огня»",
        date="2026-07-15",
        time="18:00",
        location_name="Солёная ворона",
        city="Зеленоградск",
    )
    assert su._candidate_needs_llm_occurrence_scope_review(candidate) is True

    async def _ask(_prompt, _schema, **_kwargs):
        return {
            "decision": "scoped",
            "confidence": 0.98,
            "selected_excerpts": [
                "15 июля в 18.00 — концерт «Ритмы огня»\nФламенко, гитара и кастаньеты.",
                "После каждого представления — авторский ужин.",
            ],
            "reason_short": "target block plus shared logistics",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", _ask)
    ok, reason = await su._llm_scope_candidate_occurrence(candidate)
    assert (ok, reason) == (True, "llm_scoped")
    assert "Фламенко" in (candidate.occurrence_scope_text or "")
    assert "Олег Яковенко" not in (candidate.occurrence_scope_text or "")


@pytest.mark.asyncio
async def test_roundup_scope_review_rejects_nonverbatim_model_output(monkeypatch) -> None:
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-194927034_4750",
        source_text="15 июля — Ритмы огня\n16 июля — Песни от сердца",
        title="Ритмы огня",
        date="2026-07-15",
        location_name="Солёная ворона",
    )

    async def _ask(_prompt, _schema, **_kwargs):
        return {
            "decision": "scoped",
            "confidence": 0.99,
            "selected_excerpts": ["15 июля — Ритмы огня с артистом, которого источник не называл"],
            "reason_short": "invented",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", _ask)
    ok, reason = await su._llm_scope_candidate_occurrence(candidate)
    assert ok is False
    assert reason == "llm_scope_not_verbatim"


def test_retrocar_recap_with_ungrounded_next_venue_is_non_event() -> None:
    source_text = _e6691_retrocar_recap_source()
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-127107743_14691",
        source_text=source_text,
        title="Выставка ретроавтомобилей",
        date="2026-07-11",
        end_date="2026-08-11",
        end_date_is_inferred=True,
        location_name="Калининград Сити Джаз Клуб",
        location_address="Мира 33-35",
        city="Калининград",
        event_type="выставка",
    )

    assert (
        su._looks_like_retrospective_future_teaser_not_event(
            "Выставка ретроавтомобилей",
            source_text,
            candidate=candidate,
        )
        is True
    )
    assert su._candidate_needs_llm_eventness_review(candidate, source_text) is True


@pytest.mark.asyncio
async def test_retrocar_recap_replay_skips_before_create(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        source_text = _e6691_retrocar_recap_source()
        candidate = EventCandidate(
            source_type="vk",
            source_url="https://vk.com/wall-127107743_14691",
            source_text=source_text,
            title="Выставка ретроавтомобилей",
            date="2026-07-11",
            end_date="2026-08-11",
            end_date_is_inferred=True,
            location_name="Калининград Сити Джаз Клуб",
            location_address="Мира 33-35",
            city="Калининград",
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.retry_reason is su.RetryReason.SOURCE_VERIFICATION_REQUIRED
    finally:
        await db.close()


def test_grounded_future_exhibition_announcement_is_not_retrocar_recap_skip() -> None:
    source_text = (
        "Следующая выставка ретроавтомобилей пройдёт 11 июля в Светлом на площади у Дома культуры.\n"
        "Ждём гостей на праздновании Дня города и Дня рыбака. Адрес: ул. Советская 9."
    )
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-127107743_14692",
        source_text=source_text,
        title="Выставка ретроавтомобилей",
        date="2026-07-11",
        location_name="Дом культуры",
        location_address="Советская 9",
        city="Светлый",
        event_type="выставка",
    )

    assert (
        su._looks_like_retrospective_future_teaser_not_event(
            "Выставка ретроавтомобилей",
            source_text,
            candidate=candidate,
        )
        is False
    )


@pytest.mark.asyncio
async def test_garazhka_recap_replay_skips_before_create(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        source_text = (
            "Калининград, спасибо!\n"
            "Было классно\n\n"
            "Стояли солнечные выходные — половина города уехала на море, "
            "но все кто пришёл на Гаражку в любимую локацию ПОНАРТ — лучшие.\n\n"
            "Спасибо мастерам — что не уехали на море, а радовали нас своим творчеством.\n"
            "Спасибо гостям — что вы с нами!\n\n"
            "Следующий фестиваль:\n"
            "5–6 сентября\n"
            "Локация уточняется!\n"
            "До сентября."
        )
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/garazhka_kld/1505",
            source_text=source_text,
            title="Гаражка",
            date="2026-09-05",
            end_date="2026-09-06",
            location_name="спасибо!",
            location_address="Мира 9",
            city="Калининград",
            festival="Гаражка",
            festival_source=True,
            festival_series="Гаражка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.retry_reason is su.RetryReason.SOURCE_VERIFICATION_REQUIRED
    finally:
        await db.close()


def test_future_festival_announcement_with_grounded_location_is_not_recap_skip() -> None:
    source_text = (
        "Следующая Гаражка пройдет 5–6 сентября в пространстве Понарт.\n"
        "Ждём гостей и участников на маркете, мастер-классах и кинопоказах.\n"
        "Адрес: Судостроительная 6, Калининград."
    )
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/garazhka_kld/1510",
        source_text=source_text,
        title="Гаражка",
        date="2026-09-05",
        end_date="2026-09-06",
        location_name="Понарт",
        location_address="Судостроительная 6",
        city="Калининград",
        festival="Гаражка",
        festival_source=True,
        festival_series="Гаражка",
    )

    assert (
        su._looks_like_completed_event_report_not_event(
            "Гаражка",
            source_text,
            candidate=candidate,
        )
        is False
    )


@pytest.mark.asyncio
async def test_festdir_entry_notice_replay_skips_before_create(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        source_text = (
            "Важная информация для гостей концерта 14 июня в арт-пространстве «Понарт».\n\n"
            "Обращаем внимание, что вход на концертную площадку будет организован "
            "не через привычный вход на территорию арт-пространства.\n\n"
            "В связи с особенностями размещения сцены проход для зрителей будет "
            "осуществляться через другой вход. Пожалуйста, ориентируйтесь на навигацию на месте.\n\n"
            "До встречи!\n\n"
            "Проект реализуется при поддержке Президентского фонда культурных инициатив.\n"
            "АНО «Фестивальная дирекция» Кантата"
        )
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/festdir/4673",
            source_text=source_text,
            title="Концерт",
            date="2026-09-04",
            time="14:00",
            location_name="Понарт",
            location_address="Судостроительная 6",
            city="Калининград",
            festival="Кантата",
            event_type="концерт",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.retry_reason is su.RetryReason.SOURCE_VERIFICATION_REQUIRED
    finally:
        await db.close()


def test_event_logistics_notice_for_existing_concert_is_non_event() -> None:
    source_text = (
        "Важная информация для гостей концерта 14 июня в арт-пространстве «Понарт».\n\n"
        "Обращаем внимание, что вход на концертную площадку будет организован "
        "не через привычный вход на территорию арт-пространства.\n\n"
        "В связи с особенностями размещения сцены проход для зрителей будет "
        "осуществляться через другой вход. Пожалуйста, ориентируйтесь на навигацию на месте.\n\n"
        "До встречи!\n\n"
        "Проект реализуется при поддержке Президентского фонда культурных инициатив.\n"
        "АНО «Фестивальная дирекция» Кантата"
    )

    assert (
        su._looks_like_event_logistics_notice_not_event(
            "Концерт",
            source_text,
        )
        is True
    )


def test_new_concert_invite_with_entry_details_is_not_logistics_skip() -> None:
    source_text = (
        "Приглашаем на концерт 4 сентября в 19:00 в арт-пространстве «Понарт».\n"
        "Билеты по ссылке. Вход на площадку будет организован через центральные ворота."
    )

    assert (
        su._looks_like_event_logistics_notice_not_event(
            "Концерт в Понарте",
            source_text,
        )
        is False
    )


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
            source_disposition="EVENTS_FOUND",
            source_evidence_complete=True,
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
            source_disposition="EVENTS_FOUND",
            source_evidence_complete=True,
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
        async with db.get_session() as session:
            saved = await session.get(Event, result.event_id)
            assert saved is not None
            assert saved.is_free is True
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
            source_disposition="EVENTS_FOUND",
            source_evidence_complete=True,
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_digest_stub_is_routed_to_llm_eventness_and_skipped(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls: list[tuple[str, dict]] = []

    async def fake_ask(prompt, schema, *, max_tokens, label):
        calls.append((label, schema))
        assert label == "eventness_review"
        assert "Дайджест - посмотри, приходи" in prompt
        return {
            "decision": "non_event",
            "confidence": 0.91,
            "reason_short": "source is a digest/rubric stub, not a concrete event",
            "grounded_title": None,
            "has_single_concrete_event": False,
            "missing_anchors": ["event title", "venue"],
        }

    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/molod_kld/3750",
            source_text="🏠 Дайджест - посмотри, приходи",
            title="Дайджест",
            date="2026-06-21",
            time="18:00",
            location_name="приходи",
            city="Калининград",
            event_type="встреча",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.reason == "weak_eventness_review_non_event"
        assert calls and calls[0][0] == "eventness_review"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_eventness_review_includes_poster_ocr_as_source_evidence(monkeypatch):
    captured: dict[str, str] = {}

    async def fake_ask(prompt, schema, *, max_tokens, label):
        captured["prompt"] = prompt
        return {
            "decision": "event",
            "confidence": 0.93,
            "reason_short": "poster names a dated exhibition and venue",
            "grounded_title": "Выставка к Дню памяти",
            "has_single_concrete_event": True,
            "missing_anchors": [],
        }

    monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_1",
        source_text="Завтра пройдут памятные мероприятия. Подробности на афише.",
        title="Выставка к Дню памяти",
        date="2026-08-03",
        end_date="2026-08-14",
        location_name="Центральная библиотека",
        city="Гусев",
        event_type="выставка",
        posters=[
            PosterCandidate(
                ocr_title="Мероприятия ко Дню памяти",
                ocr_text="03-14 августа — выставка, Центральная библиотека, Гусев",
            )
        ],
    )

    decision, confidence, _reason = await su._llm_review_candidate_eventness(
        candidate,
        clean_title=candidate.title or "",
        clean_source_text=candidate.source_text,
        clean_raw_excerpt=None,
    )

    assert decision == "event"
    assert confidence == 0.93
    assert '"poster_ocr"' in captured["prompt"]
    assert "03-14 августа" in captured["prompt"]
    assert "полноценным source evidence" in captured["prompt"]


@pytest.mark.asyncio
async def test_concise_real_invite_survives_eventness_review(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_ask(prompt, schema, *, max_tokens, label):
        if label == "eventness_review":
            return {
                "decision": "event",
                "confidence": 0.86,
                "reason_short": "source names one concrete lecture with time and venue",
                "grounded_title": "Лекция о городе",
                "has_single_concrete_event": True,
                "missing_anchors": [],
            }
        raise AssertionError(f"unexpected LLM label after eventness review: {label}")

    async def fake_create_bundle(*args, **kwargs):
        return {
            "description": "Короткая лекция о городе с конкретным временем и местом.",
            "facts": ["Лекция пройдёт 21 июня в 18:00"],
            "search_digest": "лекция о городе",
            "short_description": "Лекция о городе для тех, кто любит локальную историю.",
        }

    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
        monkeypatch.setattr(
            su,
            "_llm_create_description_facts_and_digest",
            fake_create_bundle,
        )
        monkeypatch.setattr(
            su,
            "_llm_review_create_bundle_grounding",
            lambda *_args, **_kwargs: _grounded_bundle_review(),
        )
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/example/42",
            source_text="Афиша: 21 июня в 18:00 ждём на лекции «Лекция о городе» в Доме китобоя.",
            title="Афиша",
            date="2026-06-21",
            time="18:00",
            location_name="Дом китобоя",
            city="Калининград",
            event_type="лекция",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
        async with db.get_session() as session:
            saved = await session.get(Event, result.event_id)
            assert saved is not None
            assert saved.title == "Афиша"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ungrounded_social_date_routes_to_llm_eventness_and_skips(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls: list[str] = []

    async def fake_ask(prompt, schema, *, max_tokens, label):
        calls.append(label)
        assert label == "eventness_review"
        assert "Кофе и Музыка" in prompt
        assert "2027-03-01" in prompt
        return {
            "decision": "non_event",
            "confidence": 0.94,
            "reason_short": "coffee/music promo has no source-grounded event date",
            "grounded_title": None,
            "has_single_concrete_event": False,
            "missing_anchors": ["source-grounded date"],
        }

    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        source_text = (
            "Кофе и Музыка: Эспрессо из зёрен Кооператива Чёрный и вдохновение Charlie Mingus\n\n"
            "Друзья, сегодня мы приготовили для вас яркий дуэт — насыщенный эспрессо "
            "из отборных зёрен Кооператива Чёрный, из Эфиопии.\n\n"
            "Приходите в Сигнал за вдохновением и отличным настроением!"
        )
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/signalkld/11052",
            source_text=source_text,
            title="Кофе и Музыка: Эспрессо из зёрен Кооператива Чёрный и вдохновение Charlie Mingus",
            date="2027-03-01",
            time="",
            location_name="Сигнал",
            location_address="Леонова 22",
            city="Калининград",
            event_type="встреча",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.reason == "weak_eventness_review_non_event"
        assert calls == ["eventness_review"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ungrounded_relative_date_can_survive_llm_eventness_review(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls: list[str] = []

    async def fake_ask(prompt, schema, *, max_tokens, label):
        calls.append(label)
        assert label == "eventness_review"
        assert "Сегодня" in prompt
        return {
            "decision": "event",
            "confidence": 0.88,
            "reason_short": "relative date anchor plus concrete venue/program",
            "grounded_title": "Джазовый вечер",
            "has_single_concrete_event": True,
            "missing_anchors": [],
        }

    async def fake_create_bundle(*args, **kwargs):
        return {
            "description": "Джазовый вечер в Сигнале с конкретным приглашением на сегодня.",
            "facts": ["Источник содержит относительный якорь: сегодня"],
            "search_digest": "джазовый вечер в Сигнале",
            "short_description": "Джазовый вечер в Сигнале для любителей живой музыки.",
        }

    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "SMART_UPDATE_IDENTITY_GATE_MODE", su.IdentityGateMode.OFF)
        monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
        monkeypatch.setattr(su, "_llm_create_description_facts_and_digest", fake_create_bundle)
        monkeypatch.setattr(
            su,
            "_llm_review_create_bundle_grounding",
            lambda *_args, **_kwargs: _grounded_bundle_review(),
        )
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/signalkld/relative",
            source_text="Сегодня в 19:00 в Сигнале пройдёт джазовый вечер. Приходите!",
            title="Джазовый вечер",
            date="2026-07-03",
            time="19:00",
            location_name="Сигнал",
            location_address="Леонова 22",
            city="Калининград",
            event_type="концерт",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
        assert calls == ["eventness_review"]
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
async def test_opening_only_exhibition_title_does_not_get_default_month_range(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        source_text = (
            "Завтра, 5 июня, в 19:00 состоится открытие выставки-экзамена "
            "четвертого семестра первого в Калининграде курса художников-сценографов "
            "«Обход 2.0».\n\n"
            "Вход: 350 руб.\n"
            "Билеты: на нашем сайте"
        )
        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/barn_kaliningrad/1033",
            source_text=source_text,
            title="Открытие выставки-экзамена «Обход 2.0»",
            date="2026-06-05",
            time="19:00",
            location_name="Барн",
            location_address="Каштановая аллея 1а",
            city="Калининград",
            ticket_price_min=350,
            ticket_price_max=350,
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "created"
        async with db.get_session() as session:
            saved = await session.get(Event, result.event_id)
            assert saved is not None
            assert saved.date == "2026-06-05"
            assert saved.time == "19:00"
            assert saved.end_date is None
            assert saved.end_date_is_inferred is False
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
            assert rows[0].end_date == "2026-06-02"
            assert rows[0].end_date_is_inferred is True
    finally:
        await db.close()


def test_short_non_location_fragments_are_unsupported_prose_locations() -> None:
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/agropark39/1885",
        source_text="И не забывайте , что парк работает для прогулок каждый день",
        title="Самосбор клубники",
        date="2026-07-05",
        location_name="И не забывайте",
        city="Калининград",
    )

    assert su._candidate_location_looks_unsupported_prose(candidate) is True


@pytest.mark.asyncio
async def test_campaign_discount_action_routes_to_llm_eventness_and_skips(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls: list[str] = []

    async def fake_ask(prompt, schema, *, max_tokens, label):
        calls.append(label)
        assert label == "eventness_review"
        assert "Веди родителей в музей" in prompt
        return {
            "decision": "non_event",
            "confidence": 0.9,
            "reason_short": "discount campaign, not one concrete event",
            "grounded_title": None,
            "has_single_concrete_event": False,
            "missing_anchors": ["concrete event program"],
        }

    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
        monkeypatch.setattr(
            su,
            "_candidate_needs_llm_anchor_role_review",
            lambda _candidate: (False, "test_eventness_only"),
        )
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="vk",
            source_url="https://vk.com/wall-29891284_13962",
            source_text=(
                "Акция «Веди родителей в музей». С 1 июля по 15 августа "
                "обладатели Пушкинской карты смогут получить скидку 10%. "
                "Как принять участие? Приобретите билет и предъявите документы."
            ),
            title="Акция «Веди родителей в музей»",
            date="2026-07-01",
            end_date="2026-08-15",
            location_name="Историко-художественный музей",
            location_address="Клиническая 21",
            city="Калининград",
            event_type="акция",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.reason == "weak_eventness_review_non_event"
        assert calls == ["eventness_review"]
    finally:
        await db.close()


def test_operational_ticket_validity_shape_routes_to_llm_not_deterministic_skip() -> None:
    source_text = (
        "Калининградский зоопарк открыт и работает в обычном режиме. "
        "Зоопарк 9:00 - 21:00, кассы 9:00 - 19:30. "
        "Входной билет действителен до 31 декабря 2026 года."
    )
    candidate = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/kldzoo/7641",
        source_text=source_text,
        title="Калининградский зоопарк",
        date="2026-12-31",
        time="09:00",
        location_name="Калининградский зоопарк",
        city="Калининград",
        event_type="выставка",
    )

    assert su._looks_like_work_schedule_notice(candidate.title, source_text) is False
    assert su._candidate_needs_llm_eventness_review(candidate, source_text) is True

    real_excursion = EventCandidate(
        source_type="telegram",
        source_url="https://t.me/kldzoo/positive-control",
        source_text=(
            "12 июля в 14:00 состоится экскурсия «Слон по городу идёт». "
            "Сбор у входа в Калининградский зоопарк, билеты на сайте."
        ),
        title="Экскурсия «Слон по городу идёт»",
        date="2026-07-12",
        time="14:00",
        location_name="Калининградский зоопарк",
        city="Калининград",
        event_type="экскурсия",
    )
    assert su._candidate_needs_llm_eventness_review(real_excursion, real_excursion.source_text) is False


@pytest.mark.asyncio
async def test_zoo_ticket_validity_notice_is_llm_reviewed_and_skipped(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    calls: list[str] = []

    async def fake_ask(prompt, schema, *, max_tokens, label):
        calls.append(label)
        assert label == "eventness_review"
        assert "Входной билет действителен до 31 декабря 2026 года" in prompt
        assert "срок действия, не дата события" in prompt
        return {
            "decision": "non_event",
            "confidence": 0.99,
            "reason_short": "work hours and ticket validity only",
            "grounded_title": None,
            "has_single_concrete_event": False,
            "missing_anchors": ["attendee-facing program"],
        }

    try:
        monkeypatch.setattr(su, "_classify_topics", _no_topics)
        monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
        monkeypatch.setattr(su, "_ask_gemma_json", fake_ask)
        monkeypatch.setattr(
            su,
            "_candidate_needs_llm_anchor_role_review",
            lambda _candidate: (False, "test_eventness_only"),
        )
        monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

        candidate = EventCandidate(
            source_type="telegram",
            source_url="https://t.me/kldzoo/7641",
            source_text=(
                "Калининградский зоопарк открыт и работает в обычном режиме:\n"
                "зоопарк 9:00 - 21:00\nкассы 9:00 - 19:30\n"
                "Билеты можно купить в кассах и на сайте.\n"
                "Входной билет действителен до 31 декабря 2026 года."
            ),
            raw_excerpt="Зоопарк открыт; билет действителен до 31 декабря.",
            title="Калининградский зоопарк",
            date="2026-12-31",
            time="09:00",
            location_name="Калининградский зоопарк",
            location_address="Мира 26",
            city="Калининград",
            event_type="выставка",
        )

        result = await smart_event_update(db, candidate, check_source_url=False, schedule_tasks=False)

        assert result.status == "retry_scheduled"
        assert result.reason == "weak_eventness_review_non_event"
        assert calls == ["eventness_review"]
        async with db.get_session() as session:
            rows = (await session.execute(select(Event))).scalars().all()
            assert rows == []
    finally:
        await db.close()
