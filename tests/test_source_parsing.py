"""Tests for source_parsing module."""

import pytest
from datetime import date, timedelta

from source_parsing.parser import (
    TheatreEvent,
    parse_date_raw,
    parse_theatre_json,
    normalize_location_name,
    fuzzy_title_match,
    limit_photos_for_source,
)


class TestParseDateRaw:
    """Tests for Russian date parsing."""

    def test_full_date_with_time(self):
        """Parse complete date with time."""
        date_str, time_str = parse_date_raw("28 декабря 18:00")
        assert time_str == "18:00"
        assert date_str is not None
        assert date_str.endswith("-12-28")

    def test_uppercase_date(self):
        """Parse uppercase date."""
        date_str, time_str = parse_date_raw("02 ЯНВАРЯ 13:00")
        assert time_str == "13:00"
        assert date_str is not None
        assert "-01-02" in date_str

    def test_date_without_time(self):
        """Parse date without time."""
        date_str, time_str = parse_date_raw("15 марта")
        assert time_str is None
        assert date_str is not None
        assert "-03-15" in date_str

    def test_truncated_month(self):
        """Parse truncated month name."""
        date_str, time_str = parse_date_raw("28 ДЕКАБР")
        assert date_str is not None
        assert "-12-28" in date_str

    def test_empty_string(self):
        """Handle empty input."""
        date_str, time_str = parse_date_raw("")
        assert date_str is None
        assert time_str is None

    def test_numeric_date_format(self):
        """Parse DD.MM.YYYY format from Pyramida."""
        date_str, time_str = parse_date_raw("21.03.2026 18:00")
        assert date_str == "2026-03-21"
        assert time_str == "18:00"

    def test_numeric_date_without_time(self):
        """Parse DD.MM.YYYY format without time."""
        date_str, time_str = parse_date_raw("15.01.2025")
        assert date_str == "2025-01-15"
        assert time_str is None


class TestParseTheatreJson:
    """Tests for JSON parsing."""

    def test_parse_single_event(self):
        """Parse a single event from JSON."""
        json_data = {
            "title": "Чайка",
            "date_raw": "28 декабря 18:00",
            "ticket_status": "available",
            "url": "https://example.com/ticket",
            "photos": ["https://example.com/photo1.jpg"],
            "description": "Спектакль по пьесе Чехова",
            "pushkin_card": True,
            "location": "Драматический театр",
        }
        events = parse_theatre_json(json_data, "dramteatr")
        
        assert len(events) == 1
        event = events[0]
        assert event.title == "Чайка"
        assert event.ticket_status == "available"
        assert event.pushkin_card is True
        assert event.parsed_time == "18:00"

    def test_parse_list_of_events(self):
        """Parse multiple events from JSON list."""
        json_data = [
            {"title": "Event 1", "date_raw": "1 января 12:00", "ticket_status": "available", "url": ""},
            {"title": "Event 2", "date_raw": "2 января 14:00", "ticket_status": "sold_out", "url": ""},
        ]
        events = parse_theatre_json(json_data, "muzteatr")
        
        assert len(events) == 2
        assert events[0].title == "Event 1"
        assert events[0].ticket_status == "available"
        assert events[1].title == "Event 2"
        assert events[1].ticket_status == "sold_out"

    def test_parse_invalid_json_string(self):
        """Handle invalid JSON gracefully."""
        events = parse_theatre_json("not valid json", "test")
        assert events == []

    def test_skip_empty_title(self):
        """Skip events without title."""
        json_data = [
            {"title": "", "date_raw": "1 января", "ticket_status": "available", "url": ""},
            {"title": "Valid Event", "date_raw": "2 января", "ticket_status": "available", "url": ""},
        ]
        events = parse_theatre_json(json_data, "test")
        
        assert len(events) == 1
        assert events[0].title == "Valid Event"

    def test_tretyakov_preserves_explicit_no_dates_disposition(self):
        events = parse_theatre_json(
            {
                "title": "Россия — пути времени",
                "date_raw": "",
                "ticket_status": "available",
                "url": "https://ticketstret.gallery/event/example",
                "source_type": "no_dates",
            },
            "tretyakov",
        )

        assert len(events) == 1
        assert events[0].source_type == "tretyakov"
        assert events[0].source_disposition == "no_dates"


class TestNormalizeLocationName:
    """Tests for location name normalization."""

    def test_normalize_dramteatr(self):
        """Normalize Драматический театр variants."""
        assert normalize_location_name("Драматический театр") == "Драматический театр"
        assert normalize_location_name("драматический театр") == "Драматический театр"
        assert normalize_location_name("Калининградский драматический театр") == "Драматический театр"

    def test_normalize_muzteatr(self):
        """Normalize Музыкальный театр variants."""
        assert normalize_location_name("Музыкальный театр") == "Музыкальный театр"
        assert normalize_location_name("музыкальный театр") == "Музыкальный театр"

    def test_normalize_sobor(self):
        """Normalize Кафедральный собор variants."""
        assert normalize_location_name("Кафедральный собор") == "Кафедральный собор"
        assert normalize_location_name("кафедральный собор") == "Кафедральный собор"

    def test_unknown_location(self):
        """Return original for unknown locations."""
        assert normalize_location_name("Другое место") == "Другое место"

    def test_empty_location(self):
        """Handle empty input."""
        assert normalize_location_name("") == ""

    def test_normalize_tretyakov(self):
        """Normalize Tretyakov variants."""
        expected = "Филиал Третьяковской галереи"
        assert normalize_location_name("Третьяков") == expected
        assert normalize_location_name("Третьяковка Калининград") == expected
        assert normalize_location_name("Атриум") == expected


class TestFuzzyTitleMatch:
    """Tests for fuzzy title matching."""

    def test_exact_match(self):
        """Exact matches should return True."""
        assert fuzzy_title_match("Чайка", "Чайка") is True

    def test_case_insensitive(self):
        """Case differences should match."""
        assert fuzzy_title_match("Чайка", "чайка") is True
        assert fuzzy_title_match("ЧАЙКА", "Чайка") is True

    def test_similar_titles(self):
        """Similar titles should match."""
        # "Чайка Спектакль" vs "Чайка спектакль" - very similar
        assert fuzzy_title_match("Чайка спектакль", "Чайка. спектакль") is True

    def test_different_titles(self):
        """Different titles should not match."""
        assert fuzzy_title_match("Чайка", "Три сестры") is False

    def test_empty_titles(self):
        """Empty titles should not match."""
        assert fuzzy_title_match("", "Чайка") is False
        assert fuzzy_title_match("Чайка", "") is False


class TestLimitPhotosForSource:
    """Tests for photo limiting."""

    def test_muzteatr_limits_to_5(self):
        """Muzteater photos should be limited to 5."""
        photos = [f"photo{i}.jpg" for i in range(10)]
        result = limit_photos_for_source(photos, "muzteatr")
        assert len(result) == 5

    def test_muzteatr_under_limit(self):
        """Muzteater with fewer photos keeps all."""
        photos = ["photo1.jpg", "photo2.jpg"]
        result = limit_photos_for_source(photos, "muzteatr")
        assert len(result) == 2

    def test_other_source_no_limit(self):
        """Other sources don't have photo limits."""
        photos = [f"photo{i}.jpg" for i in range(10)]
        result = limit_photos_for_source(photos, "dramteatr")
        assert len(result) == 10

    def test_empty_photos(self):
        """Handle empty photo list."""
        result = limit_photos_for_source([], "muzteatr")
        assert result == []


class TestShortDescriptionFallback:
    """Tests for short_description fallback logic."""

    def test_first_sentence_extraction(self):
        """First sentence should be extracted when splitting by period."""
        description = "Это первое предложение. Это второе предложение. И третье."
        first_sentence = description.split('.')[0].strip()
        assert first_sentence == "Это первое предложение"
        
    def test_first_sentence_with_empty_result(self):
        """Empty description should not cause errors."""
        description = ""
        first_sentence = description.split('.')[0].strip()
        assert first_sentence == ""
        
    def test_first_sentence_no_period(self):
        """Description without period returns full text."""
        description = "Текст без точки"
        first_sentence = description.split('.')[0].strip()
        assert first_sentence == "Текст без точки"

    def test_fallback_to_title_when_empty(self):
        """When description is empty, title should be used."""
        description = ""
        title = "Название события"
        first_sentence = description.split('.')[0].strip() if description else ""
        result = first_sentence + '.' if first_sentence else title
        assert result == title


@pytest.mark.asyncio
async def test_inc_20260713_existing_source_media_replay_uses_smart_update_cdn_gate(
    tmp_path, monkeypatch
):
    """Replay the two parser shapes that produced text-only announcements."""

    import json
    import sys
    from pathlib import Path
    from types import SimpleNamespace

    from sqlmodel import select

    import event_media
    import source_parsing.handlers as handlers
    from db import Database
    from models import Event, EventPoster

    replay_path = (
        Path(__file__).parent
        / "replays"
        / "INC-2026-07-13-tg-media-downgrade-non-cdn-posters"
        / "source-media.json"
    )
    replay = json.loads(replay_path.read_text(encoding="utf-8"))
    dram_raw = next(item for item in replay["events"] if item["source_type"] == "dramteatr")

    class FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def text(self):
            return dram_raw["page_html"]

    class FakeSession:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def get(self, _url):
            return FakeResponse()

    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientTimeout=lambda **_kwargs: object(), ClientSession=FakeSession),
    )
    cover = await handlers._fetch_og_image_for_dramteatr(dram_raw["url"])
    assert cover == dram_raw["expected_cover"]

    raw_events = []
    for item in replay["events"]:
        payload = dict(item)
        payload.pop("event_id", None)
        payload.pop("page_html", None)
        payload.pop("expected_cover", None)
        if payload["source_type"] == "dramteatr":
            payload["photos"] = [cover]
        raw_events.append(payload)
    parsed = []
    for payload in raw_events:
        parsed.extend(parse_theatre_json(payload, payload["source_type"]))
    assert len(parsed) == 2
    # This fixture verifies media replay, not historical date filtering. Keep
    # it perennial as the fixed 2026-07 occurrence ages past wall-clock today.
    for source_event in parsed:
        parsed_day = source_event.parsed_date
        if isinstance(parsed_day, str):
            parsed_day = date.fromisoformat(parsed_day)
        if parsed_day and parsed_day < date.today():
            replacement = date.today() + timedelta(days=30)
            source_event.parsed_date = replacement.isoformat()

    db = Database(str(tmp_path / "replay.sqlite"))
    await db.init()
    event_ids = {}
    async with db.get_session() as session:
        for source_event in parsed:
            stored = Event(
                title=source_event.title,
                description="Replay",
                date=str(source_event.parsed_date),
                time=source_event.parsed_time,
                location_name=source_event.location,
                source_text="Replay source",
                photo_urls=[],
                photo_count=0,
            )
            session.add(stored)
            await session.flush()
            event_ids[source_event.title] = int(stored.id)
        await session.commit()

    async def find_existing(_db, _location, _date, _time, title):
        return event_ids[title], False

    async def true_result(*_args, **_kwargs):
        return True

    async def no_result(*_args, **_kwargs):
        return None

    async def materialize(candidate):
        digest = __import__("hashlib").sha256(candidate.catbox_url.encode()).hexdigest()
        candidate.supabase_url = f"https://static.kenigevents.ru/p/replay/{digest}.webp"
        candidate.supabase_path = f"p/replay/{digest}.webp"
        candidate.phash = digest[:64]
        return True

    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "1")
    monkeypatch.setattr(handlers, "find_existing_event", find_existing)
    monkeypatch.setattr(handlers, "event_has_parser_source", true_result)
    monkeypatch.setattr(handlers, "update_event_ticket_status", true_result)
    monkeypatch.setattr(handlers, "update_linked_events", no_result)
    monkeypatch.setattr(handlers, "schedule_existing_event_update", no_result)
    monkeypatch.setattr(event_media, "materialize_event_media_candidate_to_cdn", materialize)

    stats, _ = await handlers.process_source_events(
        db,
        None,
        parsed,
        source="replay",
        start_index=0,
        total_count=len(parsed),
    )
    assert stats.failed == 0
    assert stats.ticket_updated == 2

    async with db.get_session() as session:
        rows = list((await session.execute(select(EventPoster).order_by(EventPoster.id))).scalars())
        stored_events = list((await session.execute(select(Event).order_by(Event.id))).scalars())
    await db.engine.dispose()

    assert len(rows) == 2
    assert all(row.review_status == "approved" for row in rows)
    assert all(row.supabase_url.startswith("https://static.kenigevents.ru/") for row in rows)
    assert {row.catbox_url for row in rows} == {
        raw_events[0]["photos"][0],
        dram_raw["expected_cover"],
    }
    assert all(event.photo_count == 1 for event in stored_events)
    assert all(
        event.photo_urls[0].startswith("https://static.kenigevents.ru/")
        for event in stored_events
    )
