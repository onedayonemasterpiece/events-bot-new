import pytest

from main import normalize_event_type


@pytest.mark.parametrize("event_type", [None, "", "кинопоказ"])
def test_normalize_event_type_film(event_type):
    title = "\ud83c\udfa5 Очень страшное кино"
    desc = "Показ культового фильма 'Очень страшное кино' в Заре."
    assert normalize_event_type(title, desc, event_type) == "кинопоказ"


def test_explicit_stage_type_survives_actor_cinema_credits():
    assert normalize_event_type(
        "Любовь и голуби",
        "Спектакль с участием звёзд российского театра и кино.",
        "спектакль",
    ) == "спектакль"


def test_normalize_event_type_no_change():
    title = "\ud83c\udfb5 Concert"
    desc = "Музыкальный концерт"
    assert normalize_event_type(title, desc, "концерт") == "концерт"


def test_normalize_event_type_masterclass():
    title = "\ud83d\udcda Workshop"
    desc = "Практический мастер-класс"
    assert normalize_event_type(title, desc, "мастер-класс") == "мастер-класс"
