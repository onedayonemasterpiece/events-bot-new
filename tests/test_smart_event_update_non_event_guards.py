import smart_event_update as su


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
