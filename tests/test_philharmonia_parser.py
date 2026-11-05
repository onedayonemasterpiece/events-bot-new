from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path

import pytest

from source_parsing.philharmonia import parse_philharmonia_output


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


def test_kernel_is_self_contained_current_http_parser():
    metadata = json.loads(
        (ROOT / "kaggle/ParsePhilharmonia/kernel-metadata.json").read_text(
            encoding="utf-8"
        )
    )
    source = PARSER_PATH.read_text(encoding="utf-8")

    assert metadata["code_file"] == "philharmonia_parser.py"
    assert metadata["kernel_type"] == "script"
    assert metadata["id"] == "zigomaro/parse-philharmonia-script"
    assert "fetch_philharmonia_events" in source
    assert 'if __name__ == "__main__":' in source
    assert "philharmonia_results.json" in source
    assert "afisha_list_item" not in source
    assert "?event&m=" not in source


def test_parser_main_writes_validated_results_without_status_dataset(
    tmp_path, monkeypatch
):
    parser = _load_parser()
    expected = [{"title": "Органные миры", "normalized_date": "2026-08-01"}]
    output = tmp_path / "philharmonia_results.json"

    monkeypatch.setattr(parser, "OUTPUT_PATH", output)
    monkeypatch.setattr(parser, "_load_status_client", lambda: None)
    monkeypatch.setattr(parser, "fetch_philharmonia_events", lambda: expected)

    parser.main()

    assert json.loads(output.read_text(encoding="utf-8")) == expected


def test_primary_parser_runners_do_not_depend_on_per_run_status_dataset():
    for relative_path in (
        "source_parsing/kaggle_runner.py",
        "source_parsing/philharmonia.py",
        "source_parsing/qtickets.py",
    ):
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "create_kaggle_status_dataset" not in source, relative_path
        assert "per-run status dataset disabled" in source, relative_path


def test_production_output_keeps_normalized_date_as_iso_text(tmp_path):
    output = tmp_path / "philharmonia_results.json"
    output.write_text(
        json.dumps(
            [
                {
                    "title": "Органные миры",
                    "url": "https://filarmonia39.ru/afisha/organnye-miry/",
                    "date_text": "2 августа 2026",
                    "normalized_date": "2026-08-02",
                    "time": "18:00",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    events = parse_philharmonia_output([str(output)])

    assert len(events) == 1
    assert events[0].parsed_date == "2026-08-02"
    assert isinstance(events[0].parsed_date, str)


def test_programme_only_detail_preserves_official_source_text():
    parser = _load_parser()
    detail = (FIXTURES / "philharmonia-program-only.html").read_text(encoding="utf-8")
    event = parser.enrich_event_from_detail_html({"url": "https://filarmonia39.ru/afisha/velikie-shedevry-muzyki-bakh-rekviem-motsarta/"}, detail)
    assert "Токката и фуга ре минор" in event["description"]
    assert "Моцарт «Реквием»" in event["description"]
    assert "Мария Гаврилюк" in event["description"]
    assert "Купить" not in event["description"]


def test_detail_without_prose_or_programme_still_rejected():
    parser = _load_parser()
    with pytest.raises(ValueError, match="detail description missing"):
        parser.enrich_event_from_detail_html({"url": "https://filarmonia39.ru/empty/"}, '<footer>Филармония</footer><section class="production_program"><h1>Программа</h1></section><a class="buy_button" href="/buy/">Купить</a>')


def test_prose_description_stays_primary_over_programme():
    parser = _load_parser()
    event = parser.enrich_event_from_detail_html({"url": "https://filarmonia39.ru/control/"}, '<section class="production_description"><div class="text_container">Полное описание концерта.</div></section><section class="production_program">Дополнительная программа</section>')
    assert event["description"] == "Полное описание концерта."
