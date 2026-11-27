import importlib
import json
from datetime import date
from pathlib import Path
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


def _node_text(nodes):
    return " ".join(
        node if isinstance(node, str) else _node_text(node.get("children", []))
        for node in nodes
    )


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
@pytest.mark.parametrize("time_is_default", [False, True])
async def test_update_event_page_edits_without_create(tmp_path, monkeypatch, time_is_default):
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
            time_is_default=time_is_default,
        )
        peer = m.Event(
            title="T", description="D", date="2025-09-02", time="13:00",
            location_name="Loc", source_text="SRC", time_is_default=time_is_default,
            telegraph_url="https://telegra.ph/peer", lifecycle_status="active",
        )
        session.add(peer)
        await session.flush()
        ev.linked_event_ids = [peer.id]
        session.add(ev)
        await session.commit()
        eid = ev.id
        caption, _ = m.format_event_caption(ev)
        assert ("12:00" in caption) is (not time_is_default)
        assert ("12:00" in m._format_daily_recommend_today_line(ev)) is (not time_is_default)
    async def fake_bspc(*a, **k):
        summary = k["event_summary"]
        assert summary.time == (None if time_is_default else "12:00")
        assert len(summary.other_dates) == 1
        assert summary.other_dates[0].time == (None if time_is_default else "13:00")
        rendered = await m._build_source_summary_block(summary)
        assert ("12:00" in rendered) is (not time_is_default)
        assert ("13:00" in rendered) is (not time_is_default)
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
    async def forbidden_rehydrate(*_args, **_kwargs):
        raise AssertionError("Telegraph renderer must remain a read-only media consumer")
    monkeypatch.setattr(
        m,
        "_rehydrate_missing_event_source_posters_for_telegraph",
        forbidden_rehydrate,
    )
    await m.update_telegraph_event_page(eid, db, None)
    assert create_calls == []
    assert edit_calls == ["abc"]


@pytest.mark.asyncio
async def test_update_event_page_does_not_create_replacement_on_flood(tmp_path, monkeypatch):
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

    async def fake_create(*a, **k):
        create_calls.append(1)
        return {"url": "https://telegra.ph/new", "path": "new"}

    async def fake_edit(tg, path, **k):
        raise RuntimeError("Flood control exceeded. Retry in 1592 seconds")

    monkeypatch.setattr(m, "telegraph_create_page", fake_create)
    monkeypatch.setattr(m, "telegraph_edit_page", fake_edit)

    with pytest.raises(RuntimeError, match="Flood control exceeded"):
        await m.update_telegraph_event_page(eid, db, None)
    assert create_calls == []
    async with db.get_session() as session:
        refreshed = await session.get(m.Event, eid)
        assert refreshed.telegraph_path == "abc"
        assert refreshed.telegraph_url == "https://telegra.ph/abc"


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
async def test_media_job_rehydrates_multiple_sources_but_quarantines_new_images(tmp_path, monkeypatch):
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
        assert ev.photo_count == 1
        assert ev.photo_urls == [
            "https://cdn.example/existing.jpg",
        ]
        rows = (
            await session.execute(
                m.select(m.EventPoster.poster_hash, m.EventPoster.review_status)
                .where(m.EventPoster.event_id == eid)
                .order_by(m.EventPoster.id.asc())
            )
        ).all()
        assert {r[0] for r in rows} == {"existing", "vk", "tg3191", "tg3193"}
        assert [r[1] for r in rows] == [
            "approved",
            "pending_review",
            "pending_review",
            "pending_review",
        ]


@pytest.mark.asyncio
async def test_telegraph_rehydrate_skips_shared_multi_event_source(tmp_path, monkeypatch):
    m = importlib.reload(orig_main)
    db = m.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        ev1 = m.Event(
            title="Лекция Летова",
            description="D",
            date="2026-06-19",
            time="20:00",
            location_name="Дом китобоя",
            source_text="SRC",
            photo_urls=[],
            photo_count=0,
        )
        ev2 = m.Event(
            title="Другое событие из той же подборки",
            description="D",
            date="2026-06-19",
            time="18:00",
            location_name="Дом китобоя",
            source_text="SRC",
            photo_urls=[],
            photo_count=0,
        )
        session.add_all([ev1, ev2])
        await session.commit()
        await session.refresh(ev1)
        await session.refresh(ev2)
        eid = int(ev1.id)
        shared_url = "https://t.me/kulturnaya_chaika/7860"
        session.add_all(
            [
                m.EventSource(event_id=eid, source_type="telegram", source_url=shared_url, source_text="tg"),
                m.EventSource(event_id=int(ev2.id), source_type="telegram", source_url=shared_url, source_text="tg"),
            ]
        )
        await session.commit()

    calls = []

    async def fake_fetch(source_type, source_url, *, limit):
        calls.append(source_url)
        return [SimpleNamespace(catbox_url="https://cdn.example/wrong.jpg", sha256="wrong")]

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

    assert added == 0
    assert calls == []


@pytest.mark.asyncio
async def test_source_builder_preserves_11245_ticket_conditions():
    # A factual projection reproduces the route; full source is private audit evidence.
    case = json.loads(
        (Path(__file__).parent / "fixtures/telegraph_11245_ticket_conditions.json").read_text()
    )
    summary = orig_main.SourcePageEventSummary(**case["canonical_corrected"])
    rendered, _, _ = await orig_main.build_source_page_content(
        "Конное шоу «Приключение Вани»", case["accepted_description"], case["source_url"],
        event_summary=summary, catbox_urls=[],
    )
    from telegraph.utils import html_to_nodes

    visible = _node_text(html_to_nodes(rendered))
    for fragment in (
        "Семьям от четырёх человек — 800 ₽ за каждого",
        "До трёх лет вход бесплатный, катание не включено",
        "«Балтийское долголетие» участники проходят бесплатно",
        "сопровождающие платят 800 ₽",
        "+7 (950) 673-02-13",
    ):
        assert fragment in visible
    assert "от 800 до 1000 руб." in visible


@pytest.mark.asyncio
@pytest.mark.parametrize("case_name", ["horse11245", "short_paid", "short_free", "merged_programme"])
async def test_event_renderer_preserves_accepted_description_and_summary(
    tmp_path, monkeypatch, case_name,
):
    # Unlike the global conftest mock, this is the real Event -> HTML -> nodes path.
    m = importlib.reload(orig_main)
    import smart_event_update as su

    case = json.loads(
        (Path(__file__).parent / "fixtures/telegraph_11245_ticket_conditions.json").read_text()
    )
    anchors = dict(case["canonical_corrected"])
    description = case["accepted_description"]
    source = description
    expected = (
        "Детям и взрослым — 1000 ₽",
        "Семьям от четырёх человек — 800 ₽ за каждого",
        "До трёх лет вход бесплатный, катание не включено",
        "«Балтийское долголетие» участники проходят бесплатно",
        "сопровождающие платят 800 ₽",
        "Обязательна предварительная бронь", "150 мест", "+7 (950) 673-02-13",
        "автобус № 111", "тренер по джигитовке",
    )
    is_free = case_name in {"short_free", "merged_programme"}
    if case_name == "short_paid":
        description = source = "Квартет исполнит произведения Баха."
        expected = (description,)
    elif case_name == "short_free":
        description = source = "Встреча с художниками и обсуждение их новых работ."
        expected = (description,)
    elif case_name == "merged_programme":
        description = (
            "Программа включает оранжерею Ботанического сада, пробы воды и почвы, "
            "лаборатории КГТУ и итоговую проверочную работу.\n\n"
            "Вход свободный. Дополнительный практикум — 700 ₽, по предварительной записи."
        )
        # A later source need not repeat the complete already accepted programme.
        source = "Дополнительный практикум — 700 ₽, по предварительной записи."
        expected = ("оранжерею Ботанического сада", "пробы воды и почвы", "лаборатории КГТУ",
                    "итоговую проверочную работу", "Дополнительный практикум — 700 ₽",
                    "по предварительной записи")
    if is_free:
        anchors.update(ticket_price_min=None, ticket_price_max=None)

    editor_calls = []

    async def lossy_editor(**kwargs):
        editor_calls.append(kwargs["label"])
        # Fixed destructive editor output tests routing, not model compliance.
        return "Общий анонс без условий участия."

    monkeypatch.setattr(su, "_llm_remove_infoblock_logistics", lossy_editor)
    monkeypatch.setattr(m, "get_telegraph_token", lambda: "test-token")
    monkeypatch.setattr(m, "Telegraph", FakeTG)
    monkeypatch.setenv("TELEGRAPH_VERIFY_EDITABLE_ON_NOCHANGE", "0")
    published = []

    async def capture_page(*args, **kwargs):
        published.append(kwargs["content"])
        return {"url": "https://telegra.ph/accepted-description", "path": "accepted-description"}

    monkeypatch.setattr(m, "telegraph_create_page", capture_page)
    monkeypatch.setattr(m, "telegraph_edit_page", capture_page)
    db = m.Database(str(tmp_path / "accepted-render.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            ev = m.Event(
                title="Конное шоу «Приключение Вани»" if case_name == "horse11245" else "Событие",
                description=description, source_text=source,
                source_post_url=case["source_url"] if case_name == "horse11245" else None,
                is_free=is_free, ticket_link="https://example.org/register",
                **anchors,
            )
            session.add(ev)
            await session.commit()
            event_id = ev.id
        await m.update_telegraph_event_page(event_id, db, None)
        visible = " ".join(_node_text(published[0]).split())
        for fragment in expected:
            assert fragment in visible
        assert "Комсомольск" in visible
        assert "15:00" in visible
        assert ("Бесплатно, по регистрации" if is_free else "от 800 до 1000 руб.") in visible
        assert not editor_calls
        async with db.get_session() as session:
            assert (await session.get(m.Event, event_id)).description == description
        # Rebuilding the same accepted event is deterministic and does not republish.
        await m.update_telegraph_event_page(event_id, db, None)
        assert len(published) == 1
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("parsed_children", [1, 3])
async def test_rehydrate_respects_multi_child_source_when_only_one_child_is_accepted(
    tmp_path, monkeypatch, parsed_children
):
    from models import VKSourcePacket
    import smart_event_update

    m = importlib.reload(orig_main)
    db = m.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    source_url = "https://vk.com/wall-149955604_25765"
    async with db.get_session() as session:
        event = m.Event(
            title="Дискотека", description="D", date="2026-11-27", time="23:00",
            location_name="Бар Бастион", source_text="SRC", photo_urls=[], photo_count=0,
        )
        session.add(event)
        await session.flush()
        event_id = int(event.id)
        session.add(m.EventSource(
            event_id=event_id, source_type="vk", source_url=source_url, source_text="SRC",
        ))
        session.add(VKSourcePacket(
            owner_id=149955604, post_id=25765, revision=1, source_url=source_url,
            published_at=1795770000, raw_text="SRC", raw_payload_json="{}",
            payload_hash="payload", source_revision_hash="revision", status="imported",
            parse_result_json=json.dumps({"drafts": [
                {"title": str(index), "poster_media": [], "allow_raw_photo_fallback": False}
                for index in range(parsed_children)
            ]}),
        ))
        await session.commit()

    fetched = []
    async def fake_fetch(source_type, source_url, *, limit):
        fetched.append(source_url)
        return [SimpleNamespace(catbox_url="https://cdn.example/other-programme.jpg", sha256="other")]

    async def fake_apply(session, event_id, candidates):
        return len(candidates), [], False, 0, False

    monkeypatch.setattr(m, "_fetch_event_source_poster_candidates", fake_fetch)
    monkeypatch.setattr(smart_event_update, "_apply_posters", fake_apply)
    monkeypatch.setenv("EVENT_SOURCE_MEDIA_REHYDRATE_ON_TELEGRAPH", "1")
    async with db.get_session() as session:
        event = await session.get(m.Event, event_id)
        added = await m._rehydrate_missing_event_source_posters_for_telegraph(
            session, event, event_id=event_id,
        )
    await db.close()
    assert added == (1 if parsed_children == 1 else 0)
    assert fetched == ([source_url] if parsed_children == 1 else [])
