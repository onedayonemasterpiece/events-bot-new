from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from volunteer_monitor.dobro_adapter import (
    DobroParseError,
    canonicalize_event_url,
    extract_event_urls,
    is_in_target_region,
    parse_event_page,
    parse_russian_date_range,
)
from volunteer_monitor.service import MonitorTransportError, read_event_map, run_fixture_monitor
from volunteer_monitor.source_config import DobroSourceConfig
from volunteer_monitor.types import AvailabilityStatus, MonitorRunStatus


CHECKED_AT = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
TODAY = date(2026, 8, 4)


@pytest.fixture
def fixture_dir() -> Path:
    """Keep the volunteer-monitor tests independent from the bot-wide conftest."""
    return Path(__file__).parent / "fixtures" / "volunteer_monitor"


def test_canonicalizes_only_dobro_event_urls() -> None:
    assert canonicalize_event_url("https://www.dobro.ru/event/123/?utm=x") == "https://dobro.ru/event/123"
    assert canonicalize_event_url("/event/456") == "https://dobro.ru/event/456"
    with pytest.raises(DobroParseError):
        canonicalize_event_url("https://example.com/event/123")
    with pytest.raises(DobroParseError):
        canonicalize_event_url("https://dobro.ru/news/123")


def test_search_extracts_and_deduplicates_urls(fixture_dir) -> None:
    html = (fixture_dir / "search.html").read_text(encoding="utf-8")
    assert extract_event_urls(html) == [
        "https://dobro.ru/event/11719663",
        "https://dobro.ru/event/10176380",
    ]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("20 – 23 августа 2026, 10:00", (date(2026, 8, 20), date(2026, 8, 23))),
        ("21 января – 25 декабря 2026", (date(2026, 1, 21), date(2026, 12, 25))),
        ("21 января 2026 — 25 декабря 2026", (date(2026, 1, 21), date(2026, 12, 25))),
        ("18 июня 2026", (date(2026, 6, 18), date(2026, 6, 18))),
    ],
)
def test_russian_date_ranges(raw: str, expected: tuple[date, date]) -> None:
    parsed = parse_russian_date_range(raw)
    assert parsed is not None
    assert (parsed.start, parsed.end) == expected


def test_open_application_extracts_source_grounded_fields_and_redacts_pii(fixture_dir) -> None:
    item = parse_event_page(
        (fixture_dir / "event_open.html").read_text(encoding="utf-8"),
        source_url="https://dobro.ru/event/11719663",
        checked_at=CHECKED_AT,
        today=TODAY,
    )
    assert item.availability_status is AvailabilityStatus.OPEN
    assert item.availability_reason == "enabled_application_cta"
    assert item.title == "Российско-Кыргызский форум"
    assert item.region == "Калининградская область"
    assert item.city == "Калининград"
    assert item.venue == "Форум-холл"
    assert item.organizer_name == "Ассоциация Добро.рф"
    assert item.roles == ("Администратор офиса",)
    assert item.external_links == ("https://forum-example.ru/program",)
    assert "unrelated.example.org" not in " ".join(item.external_links)
    assert "+7" not in item.source_excerpt
    assert "volunteer@example.org" not in item.source_excerpt
    assert "[телефон удалён]" in item.source_excerpt
    assert "[email удалён]" in item.source_excerpt
    assert is_in_target_region(item, "Калининградская область")


def test_visible_text_fallback_matches_current_dobro_page_shape(fixture_dir) -> None:
    item = parse_event_page(
        (fixture_dir / "event_visible_open.html").read_text(encoding="utf-8"),
        source_url="https://dobro.ru/event/12000001",
        checked_at=CHECKED_AT,
        today=TODAY,
    )
    assert item.title == "Фестиваль света"
    assert item.organizer_name == "Калининградский добровольческий центр"
    assert item.region == "Калининградская область"
    assert item.city == "Калининград"
    assert item.event_start_at == date(2026, 8, 20)
    assert item.event_end_at == date(2026, 8, 23)
    assert item.availability_status is AvailabilityStatus.OPEN
    assert item.external_links == ("https://light-fest.example/program",)
    assert "+7" not in item.source_excerpt
    assert "team@example.ru" not in item.source_excerpt


def test_explicit_closed_marker_wins_over_stale_cta(fixture_dir) -> None:
    item = parse_event_page(
        (fixture_dir / "event_closed.html").read_text(encoding="utf-8"),
        source_url="https://dobro.ru/event/10176380",
        checked_at=CHECKED_AT,
        today=TODAY,
    )
    assert item.availability_status is AvailabilityStatus.CLOSED
    assert item.availability_reason == "explicit_closed_marker"


def test_passed_deadline_is_expired_even_if_cta_exists(fixture_dir) -> None:
    item = parse_event_page(
        (fixture_dir / "event_expired.html").read_text(encoding="utf-8"),
        source_url="https://dobro.ru/event/12",
        checked_at=CHECKED_AT,
        today=TODAY,
    )
    assert item.availability_status is AvailabilityStatus.EXPIRED
    assert item.availability_reason == "application_deadline_passed"


def test_ambiguous_source_is_unknown_not_false_open(fixture_dir) -> None:
    item = parse_event_page(
        (fixture_dir / "event_unknown.html").read_text(encoding="utf-8"),
        source_url="https://dobro.ru/event/99",
        checked_at=CHECKED_AT,
        today=TODAY,
    )
    assert item.availability_status is AvailabilityStatus.UNKNOWN


def test_availability_and_semantic_hashes_are_separate(fixture_dir) -> None:
    open_html = (fixture_dir / "event_open.html").read_text(encoding="utf-8")
    closed_html = open_html.replace(
        '<a href="/apply/11719663">Подать заявку</a>',
        "<p>Набор закрыт</p><button disabled>Подать заявку</button>",
    )
    before = parse_event_page(
        open_html,
        source_url="https://dobro.ru/event/11719663",
        checked_at=CHECKED_AT,
        today=TODAY,
    )
    after = parse_event_page(
        closed_html,
        source_url="https://dobro.ru/event/11719663",
        checked_at=CHECKED_AT,
        today=TODAY,
    )
    assert before.semantic_hash == after.semantic_hash
    assert before.availability_hash != after.availability_hash


def test_fixture_monitor_is_idempotent_and_bounded(fixture_dir) -> None:
    kwargs = {
        "search_html": (fixture_dir / "search.html").read_text(encoding="utf-8"),
        "event_html_by_url": read_event_map(fixture_dir / "event-map.json"),
        "config": DobroSourceConfig(max_items=20),
        "checked_at": CHECKED_AT,
        "today": TODAY,
    }
    first = run_fixture_monitor(**kwargs).to_dict()
    second = run_fixture_monitor(**kwargs).to_dict()
    assert first == second
    assert first["run_status"] == MonitorRunStatus.PASS.value
    assert first["opportunity_count"] == 2
    assert first["status_counts"]["OPEN"] == 1
    assert first["status_counts"]["CLOSED"] == 1
    assert len(first["result_sha256"]) == 64


def test_outside_region_rows_cannot_create_a_false_regional_success(fixture_dir) -> None:
    with pytest.raises(MonitorTransportError, match="outside the configured region"):
        run_fixture_monitor(
            search_html='<a href="/event/12000004">Москва</a>',
            event_html_by_url={
                "https://dobro.ru/event/12000004": (
                    fixture_dir / "event_outside.html"
                ).read_text(encoding="utf-8")
            },
            config=DobroSourceConfig(max_items=20),
            checked_at=CHECKED_AT,
            today=TODAY,
        )


def test_source_config_rejects_unbounded_values() -> None:
    with pytest.raises(ValueError, match="max_items"):
        DobroSourceConfig(max_items=251).validate()
    with pytest.raises(ValueError, match="search_url"):
        DobroSourceConfig(search_url="https://example.org/search").validate()
