from festival_queue import detect_festival_context


def test_single_event_in_festival_source_is_not_queued_as_whole_festival() -> None:
    decision = detect_festival_context(
        parsed_events=[
            {
                "title": "Мастер-класс «Индустриальный пейзаж»",
                "date": "2026-05-02",
                "time": "12:00",
                "location_name": "Историко-художественный музей",
                "event_type": "мастер-класс",
                "festival_context": "festival_post",
            }
        ],
        festival_payload={"name": "Калининград художественный", "festival_context": "festival_post"},
        source_text=(
            "Продолжаем цикл художественных мастер-классов «Калининград художественный: "
            "80 лет в красках и образах» и приглашаем вас создать индустриальный пейзаж. "
            "2 мая | 12:00. Стоимость билета: 500 рублей. "
            "- Техника: скетч смешанными материалами.\n"
            "- Все материалы предоставляются.\n"
            "- Занятие подходит для любого уровня подготовки.\n"
            "Программа проходит в рамках празднования 80-летия Калининградской области."
        ),
        source_is_festival=True,
        source_series="Калининград художественный",
    )

    assert decision.context == "event_with_festival"
    assert decision.festival == "Калининград художественный"


def test_source_festival_program_without_single_event_still_goes_to_queue() -> None:
    decision = detect_festival_context(
        parsed_events=[],
        festival_payload={"name": "Калининград художественный", "festival_context": "festival_post"},
        source_text=(
            "Полная афиша цикла «Калининград художественный»: программа выставок, "
            "мастер-классов и встреч на май."
        ),
        source_is_festival=True,
        source_series="Калининград художественный",
    )

    assert decision.context == "festival_post"
