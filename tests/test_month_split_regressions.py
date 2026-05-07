import shutil
import sqlite3
from pathlib import Path
from datetime import date, timedelta

import pytest
from unittest.mock import AsyncMock
from telegraph.utils import nodes_to_html

import main
from contextlib import asynccontextmanager

from models import MonthPage, MonthPagePart, Festival, Event


SNAPSHOT_DB = Path("db_prod_snapshot.sqlite")


async def _load_month_data(tmp_path, month: str):
    """Return prod snapshot month data and festival map."""
    if not SNAPSHOT_DB.exists():
        pytest.skip("db_prod_snapshot.sqlite is not available in this checkout")
    db_path = tmp_path / "db.sqlite"
    shutil.copyfile(SNAPSHOT_DB, db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    start = date.fromisoformat(month + "-01")
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end_str = end.isoformat()

    rows = conn.execute(
        "select * from event where date >= ? and date < ? order by date, time",
        (start.isoformat(), end_str),
    ).fetchall()
    events = []
    exhibitions = []
    for r in rows:
        data = dict(r)
        data["added_at"] = None  # avoid naive datetime parsing in tests
        obj = Event(**data)
        if r["event_type"] == "выставка":
            exhibitions.append(obj)
        else:
            events.append(obj)

    fest_rows = conn.execute("select * from festival").fetchall()
    fest_map = {
        r["name"].casefold(): Festival(**dict(r))
        for r in fest_rows
        if r["name"]
    }
    conn.close()
    return events, exhibitions, fest_map


class FakeResult:
    def __init__(self, obj=None):
        self.obj = obj

    def scalar_one_or_none(self):
        return self.obj

    def scalars(self):
        return self

    def first(self):
        return self.obj
        
    def all(self):
        return [self.obj] if self.obj else []


class FakeDB:
    """Lightweight stub to satisfy split_month_until_ok DB interactions."""

    def __init__(self, page: MonthPage):
        self.page = page
        self.db_page = MonthPage(
            month=page.month,
            url=page.url,
            path=page.path,
            url2=page.url2,
            path2=page.path2,
            content_hash=page.content_hash,
            content_hash2=page.content_hash2,
        )
        self.parts: dict[int, MonthPagePart] = {}

    @asynccontextmanager
    async def get_session(self):
        yield self

    async def execute(self, stmt):
        # Very crude mock for select(MonthPagePart).where(...)
        # We assume strict structure matching what main.py does
        s = str(stmt)
        if "month_page_part" in s.lower():
            # Extract part_number? Or just return p1 if asked
            # main.py does: .where(MonthPagePart.month == month, MonthPagePart.part_number == 1)
            # We can just return part 1 if it exists
            p1 = self.parts.get(1)
            return FakeResult(p1)
        return FakeResult(None)

    def add(self, obj):
        if isinstance(obj, MonthPagePart):
            self.parts[obj.part_number] = obj

    async def commit(self):
        return None

    async def get(self, model, key):
        if model is MonthPage:
            return self.db_page
        if model is MonthPagePart:
            return self.parts.get(key)
        return None


@pytest.mark.asyncio
async def test_optimize_month_chunks_preserves_exhibitions(tmp_path, monkeypatch):
    """If события влезают в лимит, выставки не должны пропадать при split."""
    month = "2025-07"  # smaller slice keeps test fast
    monkeypatch.setattr(main, "ensure_event_telegraph_link", AsyncMock())
    events, exhibitions, fest_map = await _load_month_data(tmp_path, month)

    async def build_stub(
        _db,
        _month,
        evs,
        exhs,
        continuation_url=None,
        size_limit=None,
        *,
        include_ics=True,
        include_details=True,
        page_number=1,
        first_date=None,
        last_date=None,
    ):
        return main._build_month_page_content_sync(
            _month,
            evs,
            exhs,
            fest_map,
            continuation_url,
            size_limit,
            None,
            include_ics,
            include_details,
            page_number,
            first_date,
            last_date,
        )

    monkeypatch.setattr(main, "build_month_page_content", build_stub)

    assert events, "Snapshot must contain events for the month"
    assert exhibitions, "Snapshot must contain exhibitions to reproduce the bug"

    async def size(ev, ex):
        title, content, _ = await main.build_month_page_content(
        None,
        month,
        ev,
        ex,
            include_ics=True,
            include_details=True,
        )
        html = main.unescape_html_comments(nodes_to_html(content))
        html = main.ensure_footer_nav_with_hr(html, "<nav/>", month=month, page=1)
        return len(html.encode())

    events_only_size = await size(events, [])
    with_exhibitions_size = await size(events, exhibitions)
    assert with_exhibitions_size > events_only_size

    # Set TELEGRAPH_LIMIT so события влезают, а события+выставки — нет.
    limit = (events_only_size + with_exhibitions_size) // 2
    if limit <= events_only_size:
        limit = events_only_size + 1
    monkeypatch.setattr(main, "TELEGRAPH_LIMIT", limit)

    chunks, *_ = await main.optimize_month_chunks(
        None,
        month,
        events,
        exhibitions,
        "<nav/>",
    )

    assert any(exhs for _, exhs in chunks), (
        "Exhibitions must be preserved when events fit alone but exhibitions overflow"
    )


@pytest.mark.asyncio
async def test_optimize_month_chunks_splits_exhibition_tail(monkeypatch):
    """Exhibition-only tail pages must be split before Telegraph writes."""

    month = "2026-05"
    exhibitions = [
        Event(
            title=f"Exhibition {idx}",
            description="d",
            source_text="s",
            date=f"{month}-01",
            end_date=f"{month}-31",
            time="",
            location_name="Museum",
            event_type="выставка",
        )
        for idx in range(5)
    ]

    async def build_stub(
        _db,
        _month,
        evs,
        exhs,
        continuation_url=None,
        size_limit=None,
        *,
        include_ics=True,
        include_details=True,
        page_number=1,
        first_date=None,
        last_date=None,
    ):
        units = len(evs) + len(exhs)
        size = units * 100 + (30 if continuation_url else 0)
        return "title", [{"tag": "p", "children": ["x" * size]}], size

    monkeypatch.setattr(main, "build_month_page_content", build_stub)
    monkeypatch.setattr(main, "TELEGRAPH_LIMIT", 250)

    chunks, include_ics, include_details = await main.optimize_month_chunks(
        None,
        month,
        [],
        exhibitions,
        "<nav/>",
    )

    assert chunks
    assert include_ics is False
    assert include_details is True
    assert sum(len(exhs) for _, exhs in chunks) == len(exhibitions)
    assert all(len(exhs) <= 2 for _, exhs in chunks)


@pytest.mark.asyncio
async def test_optimize_month_chunks_falls_back_without_details_for_oversized_tail(monkeypatch):
    """If even one exhibition is too large with details, use minimal mode."""

    month = "2026-05"
    exhibitions = [
        Event(
            title=f"Exhibition {idx}",
            description="d",
            source_text="s",
            date=f"{month}-01",
            end_date=f"{month}-31",
            time="",
            location_name="Museum",
            event_type="выставка",
        )
        for idx in range(3)
    ]

    async def build_stub(
        _db,
        _month,
        evs,
        exhs,
        continuation_url=None,
        size_limit=None,
        *,
        include_ics=True,
        include_details=True,
        page_number=1,
        first_date=None,
        last_date=None,
    ):
        per_item = 500 if include_details else 100
        size = (len(evs) + len(exhs)) * per_item + (30 if continuation_url else 0)
        return "title", [{"tag": "p", "children": ["x" * size]}], size

    monkeypatch.setattr(main, "build_month_page_content", build_stub)
    monkeypatch.setattr(main, "TELEGRAPH_LIMIT", 250)

    chunks, include_ics, include_details = await main.optimize_month_chunks(
        None,
        month,
        [],
        exhibitions,
        "<nav/>",
    )

    assert chunks
    assert include_ics is False
    assert include_details is False
    assert sum(len(exhs) for _, exhs in chunks) == len(exhibitions)
    assert all(len(exhs) <= 2 for _, exhs in chunks)


def test_month_page_limits_and_compacts_permanent_exhibitions(monkeypatch):
    """Permanent exhibitions on aggregate pages stay bounded and digest-sized."""

    month = "2026-05"

    class FakeDatetime(main.datetime):
        @classmethod
        def now(cls, tz=None):
            return main.datetime(2026, 5, 10, 12, 0, tzinfo=tz)

    monkeypatch.setattr(main, "datetime", FakeDatetime)

    long_phrase = "ЭТОТ ДЛИННЫЙ ТЕКСТ НЕ ДОЛЖЕН ПОПАСТЬ В МЕСЯЧНУЮ СТРАНИЦУ"
    short = "Короткое описание постоянной выставки с главным сюжетом и редким предметом"
    exhibitions = [
        Event(
            title=f"Expo {idx:02d}",
            description=(long_phrase + " ") * 20,
            short_description=short,
            source_text="s",
            date=f"{month}-01",
            end_date=f"{month}-31",
            time="",
            location_name="Museum",
            event_type="выставка",
        )
        for idx in range(main.MONTH_PERMANENT_EXHIBITIONS_LIMIT + 5)
    ]

    _title, content, _size = main._build_month_page_content_sync(
        month,
        [],
        exhibitions,
        {},
        None,
        None,
        None,
        None,
        True,
        True,
    )
    html = nodes_to_html(content)

    assert html.count("<h4>") == main.MONTH_PERMANENT_EXHIBITIONS_LIMIT
    assert "Expo 00" in html
    assert f"Expo {main.MONTH_PERMANENT_EXHIBITIONS_LIMIT:02d}" not in html
    assert short in html
    assert long_phrase not in html


@pytest.mark.asyncio
async def test_sync_month_page_update_links_fallback_does_not_recurse_nav(monkeypatch):
    """A nav-refresh fallback rebuild must not trigger another nav refresh loop."""

    calls = []

    async def inner_stub(db, month, update_links=False, force=False, progress=None):
        calls.append(("inner", month, update_links, force))
        return True

    async def refresh_stub(db):
        calls.append(("refresh",))

    monkeypatch.setattr(main, "_sync_month_page_inner", inner_stub)
    monkeypatch.setattr(main, "refresh_month_nav", refresh_stub)

    await main.sync_month_page(object(), "2026-05", update_links=True, force=True)
    assert calls == [("inner", "2026-05", True, True)]

    await main.sync_month_page(object(), "2026-06", update_links=False, force=True)
    assert calls[-2:] == [
        ("inner", "2026-06", False, True),
        ("refresh",),
    ]


@pytest.mark.asyncio
async def test_split_month_until_ok_updates_page_object(tmp_path, monkeypatch):
    """split_month_until_ok should mutate переданный MonthPage (path/url)."""
    month = "2025-07"  # smaller slice keeps test fast
    monkeypatch.setattr(main, "ensure_event_telegraph_link", AsyncMock())
    events, exhibitions, fest_map = await _load_month_data(tmp_path, month)

    async def build_stub(
        _db,
        _month,
        evs,
        exhs,
        continuation_url=None,
        size_limit=None,
        *,
        include_ics=True,
        include_details=True,
        page_number=1,
        first_date=None,
        last_date=None,
    ):
        return main._build_month_page_content_sync(
            _month,
            evs,
            exhs,
            fest_map,
            continuation_url,
            size_limit,
            None,
            include_ics,
            include_details,
            page_number,
            first_date,
            last_date,
        )

    monkeypatch.setattr(main, "build_month_page_content", build_stub)

    # Force multiple pages based on actual content size.
    title, content, _ = await main.build_month_page_content(
        None, month, events, exhibitions
    )
    html = main.unescape_html_comments(nodes_to_html(content))
    total_size = len(html.encode())
    monkeypatch.setattr(main, "TELEGRAPH_LIMIT", max(512, total_size // 2))

    page = MonthPage(month=month, url="", path="")
    fake_db = FakeDB(page)

    class DummyTG:
        def __init__(self):
            self._counter = 0

        def create_page(self, **kwargs):
            self._counter += 1
            return {
                "path": f"p{self._counter}",
                "url": f"https://telegra.ph/p{self._counter}",
            }

        def edit_page(self, path, **kwargs):
            return {"path": path, "url": f"https://telegra.ph/{path}"}

    tg = DummyTG()

    await main.split_month_until_ok(
        fake_db,
        tg,
        page,
        month,
        events,
        exhibitions,
        "<nav/>",
    )

    assert page.path, "MonthPage.path should be updated so caller can see the new URL"
    assert page.url, "MonthPage.url should be updated so caller can see the new URL"
