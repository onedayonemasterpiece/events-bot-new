from datetime import date

from source_parsing.venue_catalogs import (
    ESTRADA_LOCATION,
    YANTARHALL_LOCATION,
    estrada_month_urls,
    parse_estrada_widget_html,
    parse_yantarhall_html,
    yantarhall_next_page,
)


def test_parse_estrada_widget_month_card():
    html = """
    <a class="calendar__tag"
       href="/widget/events?date_from=2026-08-01&amp;date_until=2026-08-31">Август</a>
    <div class="events">
      <p class="events__title"><span>14 августа / ПЯТНИЦА</span></p>
      <div class="events__list">
        <div class="event">
          <span class="event__price">от 1 200 ₽</span>
          <p class="event__title">Школа счастья</p>
          <p class="event__time">19:00
            <span class="event__duration">2 часа</span>
            <span class="event__age">12+</span>
          </p>
          <p class="event__place">Большой зал</p>
          <a class="event__button" href="/widget/events/937/event_seats">Купить</a>
        </div>
      </div>
    </div>
    """
    page_url = (
        "https://domiskusstv.edinoepole.ru/widget/events"
        "?date_from=2026-08-01&date_until=2026-08-31"
    )
    events = parse_estrada_widget_html(
        html,
        page_url=page_url,
        today=date(2026, 7, 27),
    )

    assert estrada_month_urls(html) == [
        "https://domiskusstv.edinoepole.ru/widget/events"
        "?date_from=2026-08-01&date_until=2026-08-31"
    ]
    assert events == [
        {
            "title": "Школа счастья",
            "date_raw": "2026-08-14 19:00",
            "parsed_date": "2026-08-14",
            "parsed_time": "19:00",
            "ticket_status": "available",
            "url": "https://domiskusstv.edinoepole.ru/widget/events/937/event_seats",
            "photos": [],
            "description": "Большой зал. 2 часа",
            "location": ESTRADA_LOCATION,
            "location_address": "Ленинский проспект 155, Калининград",
            "age_restriction": "12+",
            "scene": "Большой зал",
            "ticket_price_min": 1200,
        }
    ]


def test_parse_yantarhall_tile_and_table_layouts():
    html = """
    <li data-event-link="/afisha/pelageya 2026/">
      <div class="event-image"
           style="background-image:url('/upload/pelageya.jpg')"></div>
      <a class="event-title-text" href="/afisha/pelageya 2026/">Пелагея</a>
      <p class="event-description">Концерт</p>
      <p class="event-prices">Билеты от 3000руб.<br><span>Воз. ограничение 6+</span></p>
      <div class="event-date">
        <p class="event-date-month"><span>28</span> Июля</p>
        <p class="event-time">20:00</p>
      </div>
    </li>
    <li data-event-link="/afisha/pupo/">
      <div class="event-date">
        <p class="event-day">29</p>
        <p class="event-month">Июля</p>
        <p class="event-time"><span>Ср</span><span>19:00</span></p>
      </div>
      <div class="image-mobile"
           style="background:url(/upload/pupo.jpg);background-size:cover"></div>
      <a class="event-title-text" href="/afisha/pupo/">ПУПО (PUPO)</a>
      <p class="event-description">Концерт</p>
    </li>
    <a data-ajax-id="b46cae13ece978d7b2f4bf4c0f7608ee"
       data-next-page="2"></a>
    """
    events = parse_yantarhall_html(html, today=date(2026, 7, 27))

    assert yantarhall_next_page(html) == 2
    assert [(item["parsed_date"], item["parsed_time"], item["title"]) for item in events] == [
        ("2026-07-28", "20:00", "Пелагея"),
        ("2026-07-29", "19:00", "ПУПО (PUPO)"),
    ]
    assert events[0]["url"] == "https://янтарьхолл.рф/afisha/pelageya%202026/"
    assert events[0]["photos"] == ["https://янтарьхолл.рф/upload/pelageya.jpg"]
    assert events[0]["ticket_price_min"] == 3000
    assert events[0]["age_restriction"] == "6+"
    assert events[0]["location"] == YANTARHALL_LOCATION


def test_yantarhall_year_rollover_and_terminal_page():
    html = """
    <li data-event-link="/afisha/new-year/">
      <a class="event-title-text" href="/afisha/new-year/">Новогодний концерт</a>
      <div class="event-date">
        <p class="event-date-month"><span>03</span> Января</p>
        <p class="event-time">18:00</p>
      </div>
    </li>
    """
    events = parse_yantarhall_html(html, today=date(2026, 12, 20))
    assert events[0]["parsed_date"] == "2027-01-03"
    assert yantarhall_next_page(html) is None

