from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/replays/INC-2026-07-27-future-event-source-coverage-drop"
PARSER_PATH = ROOT / "kaggle/ParsePhilharmonia/philharmonia_parser.py"


def _load_parser():
    spec = importlib.util.spec_from_file_location("philharmonia_parser_test", PARSER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_current_listing_dom_extracts_only_future_events():
    parser = _load_parser()
    html = (FIXTURES / "philharmonia-listing.html").read_text(encoding="utf-8")

    events = parser.parse_listing_html(html, today=date(2026, 7, 27))

    assert [event["title"] for event in events] == [
        "Волшебный мир Хаяо Миядзаки",
        "Органные миры",
    ]
    assert events[0] == {
        "title": "Волшебный мир Хаяо Миядзаки",
        "url": "https://filarmonia39.ru/afisha/miyazaki/",
        "ticket_url": "https://filarmonia39.ru/afisha/miyazaki/buy/",
        "date_text": "30 июля 2026",
        "normalized_date": "2026-07-30",
        "time": "19:00",
        "age_restriction": "6+",
        "image_url": "https://filarmonia39.ru/upload/miyazaki.jpg",
        "description": "",
        "listing_text": "Волшебный мир Хаяо Миядзаки",
        "price_min": None,
        "price_max": None,
        "ticket_status": "available",
        "pushkin_card": True,
        "scene": "Концертный зал",
    }
    assert events[1]["ticket_status"] == "unavailable"


def test_current_detail_dom_enriches_description_price_and_ticket_url():
    parser = _load_parser()
    listing = parser.parse_listing_html(
        (FIXTURES / "philharmonia-listing.html").read_text(encoding="utf-8"),
        today=date(2026, 7, 27),
    )[0]
    detail = (FIXTURES / "philharmonia-detail.html").read_text(encoding="utf-8")

    event = parser.enrich_event_from_detail_html(listing, detail)

    assert event["description"].startswith("Камерный масштаб")
    assert event["price_min"] == 800
    assert event["price_max"] == 1200
    assert event["ticket_status"] == "available"
    assert event["ticket_url"] == "https://filarmonia39.ru/afisha/miyazaki/buy/"


def test_kernel_notebook_uses_current_http_parser_without_stale_selectors():
    source = (ROOT / "kaggle/ParsePhilharmonia/parse_philharmonia.ipynb").read_text(encoding="utf-8")

    assert "philharmonia_parser.py" in source
    assert "fetch_philharmonia_events" in source
    assert "afisha_list_item" not in source
    assert "?event&m=" not in source
