"""Direct official-catalog parsers for local venues.

These sources are small HTML catalogs and do not need a Kaggle browser kernel:

* Kaliningrad Theatre of Variety (former Dom Iskusstv) — Edinoe Pole widget;
* Yantar Hall — Bitrix catalog with AJAX pagination.

The functions return the same JSON-shaped records that
``source_parsing.parser.parse_theatre_json`` already consumes.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Iterable
from urllib.parse import parse_qs, quote, unquote, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

from net import http_call

ESTRADA_WIDGET_URL = "https://domiskusstv.edinoepole.ru/widget/events"
YANTARHALL_URL = "https://янтарьхолл.рф/"
YANTARHALL_AJAX_ID = "b46cae13ece978d7b2f4bf4c0f7608ee"

ESTRADA_LOCATION = (
    "Калининградский театр эстрады (Дом искусств), "
    "Ленинский проспект 155, Калининград"
)
YANTARHALL_LOCATION = "Янтарь холл, Ленина 11, Светлогорск"

_MONTHS_RU = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}
_DATE_RE = re.compile(
    r"(?iu)\b(\d{1,2})\s+("
    + "|".join(re.escape(name) for name in _MONTHS_RU)
    + r")\b"
)
_TIME_RE = re.compile(r"\b([01]?\d|2[0-3]):([0-5]\d)\b")
_PRICE_RE = re.compile(r"(?iu)(?:от\s*)?(\d[\d\s]*)\s*(?:₽|руб)")
_AGE_RE = re.compile(r"\b(\d{1,2})\s*\+")
_CSS_URL_RE = re.compile(r"url\((['\"]?)(.*?)\1\)", re.IGNORECASE)


def _text(node: Tag | None) -> str:
    return node.get_text(" ", strip=True) if node is not None else ""


def _decode_html(content: bytes) -> str:
    for encoding in ("utf-8", "cp1251"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _future_year(day: int, month: int, *, today: date) -> int:
    """Infer the catalog year without turning a just-passed day into next year."""

    candidate = date(today.year, month, day)
    if candidate < today:
        return today.year + 1
    return today.year


def _parse_day_month(raw: str, *, today: date, year_hint: int | None = None) -> str | None:
    match = _DATE_RE.search(str(raw or "").replace("\xa0", " "))
    if not match:
        return None
    day = int(match.group(1))
    month = _MONTHS_RU[match.group(2).casefold()]
    year = int(year_hint) if year_hint else _future_year(day, month, today=today)
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _parse_time(raw: str) -> str | None:
    match = _TIME_RE.search(str(raw or "").replace(".", ":"))
    if not match:
        return None
    return f"{int(match.group(1)):02d}:{int(match.group(2)):02d}"


def _parse_price(raw: str) -> int | None:
    match = _PRICE_RE.search(str(raw or "").replace("\xa0", " "))
    if not match:
        return None
    try:
        return int(re.sub(r"\s+", "", match.group(1)))
    except ValueError:
        return None


def _parse_age(raw: str) -> str:
    match = _AGE_RE.search(str(raw or ""))
    return f"{int(match.group(1))}+" if match else ""


def _canonical_http_url(base_url: str, href: str | None) -> str:
    absolute = urljoin(base_url, str(href or "").strip())
    if not absolute:
        return ""
    parts = urlsplit(absolute)
    path = quote(unquote(parts.path), safe="/:@")
    query = quote(unquote(parts.query), safe="=&?/:@,+")
    return urlunsplit((parts.scheme, parts.netloc, path, query, ""))


def _dedupe_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for event in events:
        key = (
            str(event.get("parsed_date") or ""),
            str(event.get("parsed_time") or ""),
            str(event.get("title") or "").strip().casefold(),
            str(event.get("url") or "").rstrip("/"),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(event)
    result.sort(
        key=lambda item: (
            str(item.get("parsed_date") or ""),
            str(item.get("parsed_time") or ""),
            str(item.get("title") or ""),
        )
    )
    return result


def parse_estrada_widget_html(
    html: str,
    *,
    page_url: str = ESTRADA_WIDGET_URL,
    today: date | None = None,
) -> list[dict[str, Any]]:
    """Parse one Edinoe Pole month page."""

    today = today or date.today()
    soup = BeautifulSoup(html or "", "html.parser")
    query = parse_qs(urlsplit(page_url).query)
    year_hint: int | None = None
    for key in ("date_from", "date_until"):
        raw = (query.get(key) or [""])[0]
        match = re.match(r"^(\d{4})-", raw)
        if match:
            year_hint = int(match.group(1))
            break

    output: list[dict[str, Any]] = []
    for card in soup.select(".event"):
        title = _text(card.select_one(".event__title"))
        if not title:
            continue
        group = card.find_parent(class_="events__list")
        heading = group.find_previous_sibling(class_="events__title") if group else None
        parsed_date = _parse_day_month(
            _text(heading),
            today=today,
            year_hint=year_hint,
        )
        parsed_time = _parse_time(_text(card.select_one(".event__time")))
        if not parsed_date:
            continue
        button = card.select_one("a.event__button[href]")
        price_text = _text(card.select_one(".event__price"))
        age = _parse_age(_text(card.select_one(".event__age")))
        place = _text(card.select_one(".event__place"))
        duration = _text(card.select_one(".event__duration"))
        description = ". ".join(part for part in (place, duration) if part)
        output.append(
            {
                "title": title,
                "date_raw": f"{parsed_date} {parsed_time or ''}".strip(),
                "parsed_date": parsed_date,
                "parsed_time": parsed_time,
                "ticket_status": "available" if button else "unknown",
                "url": _canonical_http_url(page_url, button.get("href") if button else page_url),
                "photos": [],
                "description": description,
                "location": ESTRADA_LOCATION,
                "location_address": "Ленинский проспект 155, Калининград",
                "age_restriction": age,
                "scene": place,
                "ticket_price_min": _parse_price(price_text),
            }
        )
    return _dedupe_events(output)


def estrada_month_urls(html: str) -> list[str]:
    """Return the distinct current/future month pages linked by the widget."""

    soup = BeautifulSoup(html or "", "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a.calendar__tag[href]"):
        href = str(anchor.get("href") or "").strip()
        if "date_from=" not in href or "date_until=" not in href:
            continue
        url = _canonical_http_url(ESTRADA_WIDGET_URL, href)
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def _closest_event_card(title_node: Tag) -> Tag | None:
    for parent in title_node.parents:
        if not isinstance(parent, Tag):
            continue
        if parent.get("data-event-link") and parent.select_one(".event-date"):
            return parent
    return None


def _yantar_date_text(card: Tag) -> str:
    combined = _text(card.select_one(".event-date-month"))
    if combined:
        return combined
    date_box = card.select_one(".event-date")
    if not date_box:
        return ""
    day_node = date_box.select_one(".event-day")
    month_nodes = date_box.select(".event-month")
    month_text = next(
        (
            _text(node)
            for node in month_nodes
            if _text(node).casefold() in _MONTHS_RU
        ),
        "",
    )
    return f"{_text(day_node)} {month_text}".strip()


def _yantar_time_text(card: Tag) -> str:
    date_box = card.select_one(".event-date")
    if not date_box:
        return ""
    return _text(date_box.select_one(".event-time"))


def _yantar_photo(card: Tag) -> str:
    image_node = card.select_one(".event-image, .image-mobile")
    style = str(image_node.get("style") or "") if image_node else ""
    match = _CSS_URL_RE.search(style)
    return _canonical_http_url(YANTARHALL_URL, match.group(2)) if match else ""


def parse_yantarhall_html(
    html: str,
    *,
    today: date | None = None,
) -> list[dict[str, Any]]:
    """Parse either the initial Yantar Hall page or one AJAX page."""

    today = today or date.today()
    soup = BeautifulSoup(html or "", "html.parser")
    output: list[dict[str, Any]] = []
    seen_cards: set[int] = set()
    for title_node in soup.select(".event-title-text"):
        card = _closest_event_card(title_node)
        if card is None or id(card) in seen_cards:
            continue
        seen_cards.add(id(card))
        title = _text(title_node)
        parsed_date = _parse_day_month(_yantar_date_text(card), today=today)
        parsed_time = _parse_time(_yantar_time_text(card))
        if not title or not parsed_date:
            continue
        href = title_node.get("href") or card.get("data-event-link")
        event_url = _canonical_http_url(YANTARHALL_URL, str(href or ""))
        description = _text(card.select_one(".event-description"))
        price_text = _text(card.select_one(".event-prices"))
        if price_text:
            description = ". ".join(part for part in (description, price_text) if part)
        photo = _yantar_photo(card)
        output.append(
            {
                "title": title,
                "date_raw": f"{parsed_date} {parsed_time or ''}".strip(),
                "parsed_date": parsed_date,
                "parsed_time": parsed_time,
                "ticket_status": "available",
                "url": event_url or YANTARHALL_URL,
                "photos": [photo] if photo else [],
                "description": description,
                "location": YANTARHALL_LOCATION,
                "location_address": "Ленина 11, Светлогорск",
                "age_restriction": _parse_age(price_text),
                "ticket_price_min": _parse_price(price_text),
                "is_free": bool(re.search(r"(?iu)\bвход\s+свободн", price_text)),
            }
        )
    return _dedupe_events(output)


def yantarhall_next_page(html: str) -> int | None:
    soup = BeautifulSoup(html or "", "html.parser")
    anchor = soup.select_one(f'[data-ajax-id="{YANTARHALL_AJAX_ID}"][data-next-page]')
    if anchor is None:
        return None
    try:
        value = int(str(anchor.get("data-next-page") or ""))
    except ValueError:
        return None
    return value if 2 <= value <= 20 else None


async def _fetch_html(name: str, url: str, **kwargs: Any) -> str:
    response = await http_call(
        name,
        "GET",
        url,
        timeout=30,
        retries=3,
        backoff=1.0,
        headers={"User-Agent": "events-bot/1.0 (+official catalog parser)"},
        **kwargs,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"{name} returned HTTP {response.status_code}")
    return _decode_html(response.content)


async def fetch_estrada_catalog(*, today: date | None = None) -> list[dict[str, Any]]:
    """Fetch every month exposed by the official theatre ticket widget."""

    today = today or date.today()
    first_html = await _fetch_html("source_catalog_estrada", ESTRADA_WIDGET_URL)
    page_urls = estrada_month_urls(first_html)
    if not page_urls:
        page_urls = [ESTRADA_WIDGET_URL]
    events: list[dict[str, Any]] = []
    for index, page_url in enumerate(page_urls[:12], start=1):
        html = (
            first_html
            if page_url.rstrip("/") == ESTRADA_WIDGET_URL.rstrip("/")
            else await _fetch_html(f"source_catalog_estrada_month_{index}", page_url)
        )
        events.extend(parse_estrada_widget_html(html, page_url=page_url, today=today))
    result = [
        event
        for event in _dedupe_events(events)
        if str(event.get("parsed_date") or "") >= today.isoformat()
    ]
    if not result:
        raise RuntimeError("official Estrada catalog returned zero events")
    return result


async def fetch_yantarhall_catalog(*, today: date | None = None) -> list[dict[str, Any]]:
    """Fetch the complete bounded Bitrix event catalog, following AJAX pages."""

    today = today or date.today()
    html = await _fetch_html("source_catalog_yantarhall", YANTARHALL_URL)
    events = parse_yantarhall_html(html, today=today)
    next_page = yantarhall_next_page(html)
    seen_pages: set[int] = set()
    while next_page is not None and next_page not in seen_pages and len(seen_pages) < 19:
        seen_pages.add(next_page)
        html = await _fetch_html(
            f"source_catalog_yantarhall_page_{next_page}",
            YANTARHALL_URL,
            params={
                "bxajaxid": YANTARHALL_AJAX_ID,
                "PAGEN_2": next_page,
            },
        )
        events.extend(parse_yantarhall_html(html, today=today))
        candidate = yantarhall_next_page(html)
        next_page = candidate if candidate and candidate > next_page else None
    result = [
        event
        for event in _dedupe_events(events)
        if str(event.get("parsed_date") or "") >= today.isoformat()
    ]
    if not result:
        raise RuntimeError("official Yantar Hall catalog returned zero events")
    return result
