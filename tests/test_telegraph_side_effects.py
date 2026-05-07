import importlib
from datetime import date
from types import SimpleNamespace

import pytest

import main as orig_main


class FakeTG:
    def __init__(self, access_token=None):
        pass
    def get_page(self, *a, **k):
        return {}


async def fake_call(func, *a, **k):
    return {}


@pytest.mark.asyncio
async def test_ensure_event_telegraph_link_pure(tmp_path, monkeypatch):
    m = importlib.reload(orig_main)
    db = m.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev = m.Event(
            title="T",
            description="D",
            date="2025-09-01",
            time="12:00",
            location_name="Loc",
            source_text="SRC",
            telegraph_path="abc",
            source_post_url="https://example.com",
        )
        session.add(ev)
        await session.commit()
        eid = ev.id
    async with db.get_session() as session:
        ev = await session.get(m.Event, eid)
    called = False
    async def fake_update(*a, **k):
        nonlocal called
        called = True
    monkeypatch.setattr(m, "update_telegraph_event_page", fake_update)
    await m.ensure_event_telegraph_link(ev, None, db)
    assert not called
    assert ev.telegraph_url == "https://telegra.ph/abc"
    async with db.get_session() as session:
        refreshed = await session.get(m.Event, eid)
        assert refreshed.telegraph_url == "https://telegra.ph/abc"


@pytest.mark.asyncio
async def test_update_event_page_edits_without_create(tmp_path, monkeypatch):
    m = importlib.reload(orig_main)
    db = m.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev = m.Event(
            title="T",
            description="D",
            date="2025-09-01",
            time="12:00",
            location_name="Loc",
            source_text="SRC",
            telegraph_path="abc",
            telegraph_url="https://telegra.ph/abc",
        )
        session.add(ev)
        await session.commit()
        eid = ev.id
    async def fake_bspc(*a, **k):
        return "<p>x</p>", "", ""
    monkeypatch.setattr(m, "build_source_page_content", fake_bspc)
    monkeypatch.setattr(m, "get_telegraph_token", lambda: "t")
    monkeypatch.setattr(m, "Telegraph", FakeTG)
    monkeypatch.setattr(m, "telegraph_call", fake_call)
    create_calls = []
    edit_calls = []
    async def fake_create(*a, **k):
        create_calls.append(1)
        return {"url": "https://telegra.ph/new", "path": "new"}
    async def fake_edit(tg, path, **k):
        edit_calls.append(path)
        return {}
    monkeypatch.setattr(m, "telegraph_create_page", fake_create)
    monkeypatch.setattr(m, "telegraph_edit_page", fake_edit)
    await m.update_telegraph_event_page(eid, db, None)
    assert create_calls == []
    assert edit_calls == ["abc"]


@pytest.mark.asyncio
async def test_navigation_builds_do_not_touch_events(tmp_path, monkeypatch):
    m = importlib.reload(orig_main)
    db = m.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        ev = m.Event(
            title="E",
            description="D",
            date="2025-09-05",
            time="12:00",
            location_name="Loc",
            source_text="TXT",
        )
        session.add(ev)
        await session.commit()
        eid = ev.id
    async def fake_bspc2(*a, **k):
        return "<p>x</p>", "", ""
    monkeypatch.setattr(m, "build_source_page_content", fake_bspc2)
    monkeypatch.setattr(m, "get_telegraph_token", lambda: "t")
    monkeypatch.setattr(m, "Telegraph", FakeTG)
    async def fake_month(*a, **k):
        return "T", [], 0
    async def fake_weekend(*a, **k):
        return "W", [], 0
    monkeypatch.setattr(m, "build_month_page_content", fake_month)
    monkeypatch.setattr(m, "build_weekend_page_content", fake_weekend)
    monkeypatch.setattr(m, "telegraph_call", fake_call)
    m.DISABLE_EVENT_PAGE_UPDATES = False
    create_calls = []
    edit_calls = []
    async def fake_create(tg, *a, caller="event_pipeline", eid=None, **k):
        create_calls.append((caller, eid))
        return {"url": "https://tg/x", "path": "x"}
    async def fake_edit(tg, path, *, caller="event_pipeline", eid=None, **k):
        edit_calls.append((caller, eid, path))
        return {}
    monkeypatch.setattr(m, "telegraph_create_page", fake_create)
    monkeypatch.setattr(m, "telegraph_edit_page", fake_edit)
    await m.update_telegraph_event_page(eid, db, None)
    await m.sync_month_page(db, "2025-09", update_links=False)
    await m.sync_weekend_page(db, "2025-09-06", update_links=False, post_vk=False)
    async with db.get_session() as session:
        ev = await session.get(m.Event, eid)
        ev.title = "New"
        session.add(ev)
        await session.commit()
    await m.update_telegraph_event_page(eid, db, None)
    assert all(c[0] == "event_pipeline" for c in create_calls if c[1] == eid)
    assert all(c[0] == "event_pipeline" for c in edit_calls if c[1] == eid)
    await m.rebuild_pages(db, ["2025-09"], ["2025-09-06"])
    assert all(c[0] == "event_pipeline" for c in create_calls if c[1] == eid)
    assert all(c[0] == "event_pipeline" for c in edit_calls if c[1] == eid)


@pytest.mark.asyncio
async def test_telegraph_rehydrates_missing_media_from_event_sources(tmp_path, monkeypatch):
    m = importlib.reload(orig_main)
    db = m.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        ev = m.Event(
            title="Выставка «Куплю гараж. Калининград»",
            description="D",
            date="2026-05-13",
            time="12:00",
            location_name="Дом китобоя",
            source_text="SRC",
            photo_urls=["https://cdn.example/existing.jpg"],
            photo_count=1,
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        eid = int(ev.id)
        session.add(
            m.EventPoster(
                event_id=eid,
                catbox_url="https://cdn.example/existing.jpg",
                poster_hash="existing",
            )
        )
        session.add_all(
            [
                m.EventSource(
                    event_id=eid,
                    source_type="vk",
                    source_url="https://vk.com/wall-1_10",
                    source_text="vk",
                ),
                m.EventSource(
                    event_id=eid,
                    source_type="telegram",
                    source_url="https://t.me/domkitoboya/3191",
                    source_text="tg1",
                ),
                m.EventSource(
                    event_id=eid,
                    source_type="telegram",
                    source_url="https://t.me/domkitoboya/3193",
                    source_text="tg2",
                ),
            ]
        )
        await session.commit()

    async def fake_fetch(source_type, source_url, *, limit):
        if source_url.endswith("1_10"):
            return [
                SimpleNamespace(catbox_url="https://cdn.example/existing.jpg", sha256="existing"),
                SimpleNamespace(catbox_url="https://cdn.example/vk.jpg", sha256="vk"),
            ]
        if source_url.endswith("/3191"):
            return [SimpleNamespace(catbox_url="https://cdn.example/tg3191.jpg", sha256="tg3191")]
        if source_url.endswith("/3193"):
            return [SimpleNamespace(catbox_url="https://cdn.example/tg3193.jpg", sha256="tg3193")]
        return []

    monkeypatch.setattr(m, "_fetch_event_source_poster_candidates", fake_fetch)
    monkeypatch.setenv("EVENT_SOURCE_MEDIA_REHYDRATE_ON_TELEGRAPH", "1")

    async with db.get_session() as session:
        ev = await session.get(m.Event, eid)
        added = await m._rehydrate_missing_event_source_posters_for_telegraph(
            session,
            ev,
            event_id=eid,
        )
        await session.commit()

    assert added == 3
    async with db.get_session() as session:
        ev = await session.get(m.Event, eid)
        assert ev.photo_count == 4
        assert ev.photo_urls == [
            "https://cdn.example/existing.jpg",
            "https://cdn.example/vk.jpg",
            "https://cdn.example/tg3191.jpg",
            "https://cdn.example/tg3193.jpg",
        ]
        rows = (
            await session.execute(
                m.select(m.EventPoster.poster_hash).where(m.EventPoster.event_id == eid)
            )
        ).all()
        assert {r[0] for r in rows} == {"existing", "vk", "tg3191", "tg3193"}
