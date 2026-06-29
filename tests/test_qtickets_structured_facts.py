from pathlib import Path

from source_parsing.handlers import _build_parser_source_text
from source_parsing.qtickets import parse_qtickets_output


def _fixture_path() -> str:
    return str(
        Path(__file__).parent
        / "replays"
        / "INC-2026-06-29-qtickets-structured-facts-lost"
        / "qtickets_events.json"
    )


def test_qtickets_replay_preserves_structured_address_and_end_date():
    events = parse_qtickets_output([_fixture_path()])

    flava = events[0]
    assert flava.title == "FLAVA INTENSIVE (VALERA & LERA VOYNITS)"
    assert flava.parsed_date == "2026-07-04"
    assert flava.parsed_time == "14:10"
    assert flava.end_date == "2026-07-07"
    assert flava.location == "КОНЦЕПТ"
    assert flava.location_address == "ул. Ленинский проспект, 42Б, Калининград, Россия"
    assert flava.ticket_price_min == 1800


def test_qtickets_llm_source_text_keeps_page_facts_ahead_of_ocr_fragments():
    event = parse_qtickets_output([_fixture_path()])[0]

    text = _build_parser_source_text(
        event,
        full_description="Возраст: 0+",
        location_name=event.location,
    )

    assert "Название: FLAVA INTENSIVE (VALERA & LERA VOYNITS)" in text
    assert "Дата: 2026-07-04" in text
    assert "Дата окончания: 2026-07-07" in text
    assert "Время: 14:10" in text
    assert "Площадка: КОНЦЕПТ" in text
    assert "Адрес: ул. Ленинский проспект, 42Б, Калининград, Россия" in text
    assert "Контракт источника:" in text
    assert "не заменяй название страницы отдельными словами с афиши" in text


def test_qtickets_negative_control_does_not_require_end_date_or_address():
    event = parse_qtickets_output([_fixture_path()])[1]

    text = _build_parser_source_text(
        event,
        full_description=event.description,
        location_name=event.location,
    )

    assert "Название: Контрольное событие без афишного конфликта" in text
    assert "Дата окончания:" not in text
    assert "Адрес:" not in text
