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
