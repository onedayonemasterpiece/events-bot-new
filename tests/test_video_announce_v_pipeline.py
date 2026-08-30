from __future__ import annotations

import asyncio
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from db import Database
from main import LOCAL_TZ
from models import (
    Channel,
    Event,
    EventPoster,
    User,
    VideoAnnounceItem,
    VideoAnnounceItemStatus,
    VideoAnnounceSession,
    VideoAnnounceSessionStatus,
)
from video_announce.custom_types import RankedEvent
from video_announce.handlers import handle_video_callback
from video_announce.popular_review import PopularReviewPick, PopularReviewSelection
import video_announce.scenario as scenario_module
import video_announce.poller as poller_module
from video_announce.scenario import TOMORROW_TEST_MIN_POSTERS, VideoAnnounceScenario


class _DummyBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str, dict]] = []

    async def send_message(self, chat_id: int, text: str, **kwargs) -> None:  # noqa: ARG002
        self.messages.append((chat_id, text, kwargs))

    async def send_document(self, chat_id: int, document, **kwargs) -> None:  # noqa: ANN001,ARG002
        self.messages.append((chat_id, "document", kwargs))


class _DummyCallback:
    def __init__(self, data: str, *, chat_id: int = 123, user_id: int = 1) -> None:
        self.data = data
        self.message = SimpleNamespace(chat=SimpleNamespace(id=chat_id), message_id=1)
        self.from_user = SimpleNamespace(id=user_id)
        self.answers: list[tuple[str, bool]] = []

    async def answer(self, text: str, show_alert: bool = False) -> None:
        self.answers.append((text, show_alert))


@pytest.mark.asyncio
async def test_rendering_unique_index_is_scoped_by_profile(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(
            VideoAnnounceSession(
                status=VideoAnnounceSessionStatus.RENDERING,
                profile_key="default",
            )
        )
        session.add(
            VideoAnnounceSession(
                status=VideoAnnounceSessionStatus.RENDERING,
                profile_key="kenigsberg_story",
            )
        )
        await session.commit()

    async with db.get_session() as session:
        session.add(
            VideoAnnounceSession(
                status=VideoAnnounceSessionStatus.RENDERING,
                profile_key="kenigsberg_story",
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.asyncio
async def test_partner_render_lock_is_scoped_by_profile(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(
            VideoAnnounceSession(
                status=VideoAnnounceSessionStatus.RENDERING,
                profile_key="popular_review_eco",
            )
        )
        await session.commit()

    scenario = VideoAnnounceScenario(db, _DummyBot(), chat_id=123, user_id=1)

    assert (await scenario.has_rendering()) is not None
    assert (await scenario.has_rendering(profile_key="popular_review_eco")) is not None
    assert await scenario.has_rendering(profile_key="popular_review_konb") is None


@pytest.mark.asyncio
async def test_popular_review_deduplicates_repeated_render_lock_notifications(
    monkeypatch,
) -> None:
    bot = _DummyBot()
    scenario = VideoAnnounceScenario(object(), bot, chat_id=123, user_id=1)
    rendering = VideoAnnounceSession(
        id=1083,
        status=VideoAnnounceSessionStatus.RENDERING,
        profile_key="popular_review",
    )

    async def _allowed() -> bool:
        return True

    async def _rendering(**_kwargs):
        return rendering

    monkeypatch.setattr(scenario, "ensure_access", _allowed)
    monkeypatch.setattr(scenario, "has_rendering", _rendering)
    scenario_module._rendering_notice_cache.clear()

    assert await scenario.run_popular_review_pipeline() is None
    assert await scenario.run_popular_review_pipeline() is None

    assert [(chat_id, text) for chat_id, text, _kwargs in bot.messages] == [
        (123, "Сессия #1083 уже рендерится, дождитесь завершения")
    ]


@pytest.mark.asyncio
async def test_run_tomorrow_pipeline_creates_session_and_starts(monkeypatch, tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now_local = datetime.now(LOCAL_TZ)
    tomorrow = (now_local + timedelta(days=1)).date()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        ev = Event(
            title="Event",
            description="d",
            source_text="s",
            date=tomorrow.isoformat(),
            time="19:00",
            location_name="Loc",
            city="City",
            photo_urls=["https://example.com/1.jpg"],
            photo_count=1,
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            EventPoster(
                event_id=ev.id,
                poster_hash="h1",
                ocr_text="TEXT",
                ocr_title="TITLE",
                updated_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    started: dict[str, int] = {}

    async def _fake_start_render(  # noqa: ANN001,ARG002
        self,
        session_id: int,
        message=None,
        *,
        limit_scenes=None,
        background: bool = True,
    ) -> str:
        started["session_id"] = session_id
        started["background"] = background
        return "Рендеринг запущен"

    monkeypatch.setattr(VideoAnnounceScenario, "start_render", _fake_start_render)

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)
    await scenario.run_tomorrow_pipeline()

    assert "session_id" in started

    async with db.get_session() as session:
        sess = await session.get(VideoAnnounceSession, started["session_id"])
        assert sess is not None
        assert isinstance(sess.selection_params, dict)
        assert sess.selection_params.get("random_order") is True
        assert sess.selection_params.get("target_date") == tomorrow.isoformat()
        assert sess.kaggle_kernel_ref

        res = await session.execute(
            select(VideoAnnounceItem).where(VideoAnnounceItem.session_id == sess.id)
        )
        items = list(res.scalars().all())
        assert any(it.status == VideoAnnounceItemStatus.READY for it in items)

    assert any(
        text == f"Сессия #{started['session_id']}: выбрано 1 событий, готовлю рендер…"
        for _, text, _ in bot.messages
    )
    assert all("запускаю Kaggle" not in text for _, text, _ in bot.messages)


@pytest.mark.asyncio
async def test_run_tomorrow_pipeline_test_mode_limits_scenes(monkeypatch, tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now_local = datetime.now(LOCAL_TZ)
    tomorrow = (now_local + timedelta(days=1)).date()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        ev = Event(
            title="Event",
            description="d",
            source_text="s",
            date=tomorrow.isoformat(),
            time="19:00",
            location_name="Loc",
            city="City",
            photo_urls=["https://example.com/1.jpg"],
            photo_count=1,
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            EventPoster(
                event_id=ev.id,
                poster_hash="h1",
                ocr_text="TEXT",
                ocr_title="TITLE",
                updated_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    started: dict[str, int] = {}

    async def _fake_start_render(  # noqa: ANN001,ARG002
        self,
        session_id: int,
        message=None,
        *,
        limit_scenes=None,
        background: bool = True,
    ) -> str:
        started["session_id"] = session_id
        started["limit_scenes"] = limit_scenes
        started["background"] = background
        return "Рендеринг запущен"

    monkeypatch.setattr(VideoAnnounceScenario, "start_render", _fake_start_render)

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)
    await scenario.run_tomorrow_pipeline(test_mode=True)

    assert started["limit_scenes"] == TOMORROW_TEST_MIN_POSTERS


@pytest.mark.asyncio
async def test_run_tomorrow_pipeline_lane_busy_does_not_create_selected_session(
    monkeypatch, tmp_path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("VIDEO_ANNOUNCE_STORY_ENABLED", "1")
    monkeypatch.setenv("VIDEO_ANNOUNCE_VIDEO_LANE_AUTH_ENVS", "LANE1,LANE2")

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        session.add(
            VideoAnnounceSession(
                status=VideoAnnounceSessionStatus.RENDERING,
                profile_key="popular_review_eco",
                selection_params={"_video_lane_auth_env": "LANE1"},
                kaggle_kernel_ref="zigomaro/cherryflash",
            )
        )
        session.add(
            VideoAnnounceSession(
                status=VideoAnnounceSessionStatus.RENDERING,
                profile_key="popular_review_konb",
                selection_params={"_video_lane_auth_env": "LANE2"},
                kaggle_kernel_ref="zigomaro/cherryflash-video-lane-1",
            )
        )
        await session.commit()

    async def _start_render_must_not_run(*_args, **_kwargs) -> str:
        raise AssertionError("lane-busy preflight must stop before SELECTED/start_render")

    monkeypatch.setattr(VideoAnnounceScenario, "start_render", _start_render_must_not_run)

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)
    result = await scenario.run_tomorrow_pipeline(wait_for_handoff=True)

    assert result is None
    assert scenario.last_tomorrow_skip_reason == "video_lanes_busy"
    assert any("video-lanes заняты" in text for _, text, _ in bot.messages)

    async with db.get_session() as session:
        res = await session.execute(select(VideoAnnounceSession))
        sessions = list(res.scalars().all())
    assert len(sessions) == 2
    assert all(sess.status == VideoAnnounceSessionStatus.RENDERING for sess in sessions)


@pytest.mark.asyncio
async def test_start_render_persists_notify_chat_id_for_recovery(monkeypatch, tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    tomorrow = (datetime.now(LOCAL_TZ) + timedelta(days=1)).date()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        ev = Event(
            title="Event",
            description="d",
            source_text="s",
            date=tomorrow.isoformat(),
            time="19:00",
            location_name="Loc",
            city="City",
            photo_urls=["https://example.com/1.jpg"],
            photo_count=1,
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)

        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.SELECTED,
            profile_key="default",
            selection_params={"mode": "default"},
            kaggle_kernel_ref="zigomaro/crumple-video",
            test_chat_id=-1002210431821,
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

        session.add(
            VideoAnnounceItem(
                session_id=session_id,
                event_id=ev.id,
                position=1,
                score=1.0,
                status=VideoAnnounceItemStatus.READY,
            )
        )
        await session.commit()

    async def _fake_fill_missing_about(*args, **kwargs):  # noqa: ANN002,ANN003
        return {}

    async def _fake_build_render_payload(self, session_obj, ranked):  # noqa: ANN001,ARG002
        return {"session_id": session_obj.id}

    async def _fake_enrich_payload_with_poster_overlays(db_obj, payload_json):  # noqa: ANN001
        del db_obj
        return payload_json

    async def _fake_update_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def _fake_send_payload_file(self, session_obj, payload_json, *, caption=None):  # noqa: ANN001,ARG002
        return None

    async def _fake_render_and_notify(self, session_obj, ranked, **kwargs):  # noqa: ANN001,ARG002
        return None

    async def _fake_kernel_available(self, ref):  # noqa: ANN001,ARG002
        return True, None

    monkeypatch.setattr(scenario_module, "fill_missing_about", _fake_fill_missing_about)
    monkeypatch.setattr(VideoAnnounceScenario, "_build_render_payload", _fake_build_render_payload)
    monkeypatch.setattr(
        scenario_module,
        "enrich_payload_with_poster_overlays",
        _fake_enrich_payload_with_poster_overlays,
    )
    monkeypatch.setattr(scenario_module, "update_status_message", _fake_update_status_message)
    monkeypatch.setattr(VideoAnnounceScenario, "_send_payload_file", _fake_send_payload_file)
    monkeypatch.setattr(VideoAnnounceScenario, "_render_and_notify", _fake_render_and_notify)
    monkeypatch.setattr(
        VideoAnnounceScenario,
        "_kaggle_kernel_target_available",
        _fake_kernel_available,
    )
    monkeypatch.setattr(scenario_module, "payload_as_json", lambda payload, tz: '{"ok": true}')

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=555, user_id=1)

    result = await scenario.start_render(session_id)
    await asyncio.sleep(0)

    assert result == "Рендеринг запущен"
    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.RENDERING
        assert isinstance(refreshed.selection_params, dict)
        assert refreshed.selection_params["notify_chat_id"] == 555


@pytest.mark.asyncio
async def test_run_popular_review_pipeline_uses_cherryflash_kernel_and_keniggpt(
    monkeypatch, tmp_path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        session.add(
            Channel(
                channel_id=-1002210431821,
                title="Кёнигсберг GPT",
                username="keniggpt",
                is_admin=True,
                is_registered=True,
            )
        )
        events = []
        for idx in range(2):
            ev = Event(
                title=f"Popular {idx + 1}",
                description="Long description",
                short_description="Short description",
                search_digest="Digest text for scene expansion",
                source_text="s",
                date="2026-04-12",
                time="19:00",
                location_name=f"Loc {idx + 1}",
                city="Калининград",
                photo_urls=[
                    f"https://example.com/{idx + 1}.jpg",
                    f"https://example.com/{idx + 1}_b.jpg",
                ],
                photo_count=2,
            )
            session.add(ev)
            events.append(ev)
        await session.commit()
        for ev in events:
            await session.refresh(ev)
            session.add(
                EventPoster(
                    event_id=ev.id,
                    poster_hash=f"h{ev.id}",
                    ocr_text="TEXT",
                    ocr_title="TITLE",
                    updated_at=datetime.now(timezone.utc),
                )
            )
        await session.commit()

    async with db.get_session() as session:
        result = await session.execute(select(Event).order_by(Event.id))
        stored_events = list(result.scalars().all())

    fake_selection = PopularReviewSelection(
        picks=[
            PopularReviewPick(
                event=stored_events[0],
                score=9.5,
                source_window="24h",
                source_post_url="https://t.me/example/1",
                source_label="tg",
                anti_repeat_status="fresh",
                description="Digest one",
            ),
            PopularReviewPick(
                event=stored_events[1],
                score=8.7,
                source_window="3d",
                source_post_url="https://t.me/example/2",
                source_label="tg",
                anti_repeat_status="fresh",
                description="Digest two",
            ),
        ],
        ranked=[
            RankedEvent(event=stored_events[0], score=9.5, position=1, description="Digest one"),
            RankedEvent(event=stored_events[1], score=8.7, position=2, description="Digest two"),
        ],
        trace={
            stored_events[0].id: {
                "score": 9.5,
                "source_window": "24h",
                "source_post_url": "https://t.me/example/1",
                "source_label": "tg",
                "anti_repeat_status": "fresh",
            },
            stored_events[1].id: {
                "score": 8.7,
                "source_window": "3d",
                "source_post_url": "https://t.me/example/2",
                "source_label": "tg",
                "anti_repeat_status": "fresh",
            },
        },
    )

    async def _fake_build_popular_review_selection(*args, **kwargs):
        return fake_selection

    started: dict[str, int] = {}

    async def _fake_start_render(  # noqa: ANN001,ARG002
        self,
        session_id: int,
        message=None,
        *,
        limit_scenes=None,
        background: bool = True,
    ) -> str:
        started["session_id"] = session_id
        started["limit_scenes"] = limit_scenes
        started["background"] = background
        return "Рендеринг запущен"

    monkeypatch.setattr(
        "video_announce.scenario.build_popular_review_selection",
        _fake_build_popular_review_selection,
    )
    monkeypatch.setattr(VideoAnnounceScenario, "start_render", _fake_start_render)

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)
    await scenario.run_popular_review_pipeline()

    assert started["limit_scenes"] == 2
    assert started["background"] is True

    async with db.get_session() as session:
        result = await session.execute(select(VideoAnnounceSession))
        sessions = list(result.scalars().all())
        assert len(sessions) == 1
        sess = sessions[0]
        assert sess.profile_key == "popular_review"
        assert sess.test_chat_id == -1002210431821
        assert sess.main_chat_id is None
        assert sess.kaggle_kernel_ref == "local:CherryFlash"
        assert sess.selection_params["mode"] == "popular_review"
        assert sess.selection_params["render_scene_limit"] == 2
        assert "popular_review_trace" in sess.selection_params

        items = list(
            (
                await session.execute(
                    select(VideoAnnounceItem).where(VideoAnnounceItem.session_id == sess.id)
                )
            ).scalars()
        )
        assert len(items) == 2
        assert all(item.status == VideoAnnounceItemStatus.READY for item in items)
        assert all(item.final_description for item in items)


@pytest.mark.asyncio
async def test_render_and_notify_cherryflash_fails_when_bind_wait_does_not_confirm_dataset(
    monkeypatch, tmp_path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="popular_review",
            selection_params={"mode": "popular_review"},
            test_chat_id=-1002210431821,
            main_chat_id=None,
            kaggle_kernel_ref="local:CherryFlash",
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    class _DummyClient:
        def get_kernel_status(self, kernel_ref: str) -> dict[str, str]:
            assert kernel_ref == "zigomaro/cherryflash"
            return {"status": "RUNNING"}

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)
    call_order: list[tuple[str, str]] = []

    async def _fake_create_dataset(self, session_obj, json_text, finalized, *, client):  # noqa: ANN001,ARG002
        assert session_obj.id == session_id
        return "zigomaro/cherryflash-session-161", ["zigomaro/story-cipher"]

    async def _fake_push_kernel(self, client, dataset_sources, kernel_ref, **kwargs):  # noqa: ANN001,ARG001
        assert kernel_ref == "local:CherryFlash"
        assert dataset_sources == [
            "zigomaro/cherryflash-session-161",
            "zigomaro/story-cipher",
        ]
        call_order.append(("push_kernel", kernel_ref))
        return "zigomaro/cherryflash"

    async def _fake_update_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return (123, 1)

    async def _fake_await_dataset_ready(client, dataset_ref, **kwargs):  # noqa: ANN001
        call_order.append(("dataset_ready", dataset_ref))
        assert kwargs["expected_files"] == ["payload.json"]
        return {"status": "ready", "files": ["payload.json"]}

    async def _fake_await_kernel_dataset_sources(client, kernel_ref, expected_sources, **kwargs):  # noqa: ANN001
        call_order.append(("bind_wait", kernel_ref))
        assert kernel_ref == "zigomaro/cherryflash"
        assert expected_sources == [
            "zigomaro/cherryflash-session-161",
            "zigomaro/story-cipher",
        ]
        raise RuntimeError("bind failed")

    monkeypatch.setattr(scenario_module, "KaggleClient", _DummyClient)
    monkeypatch.setattr(VideoAnnounceScenario, "_create_dataset", _fake_create_dataset)
    monkeypatch.setattr(VideoAnnounceScenario, "_push_kernel", _fake_push_kernel)
    monkeypatch.setattr(scenario_module, "update_status_message", _fake_update_status_message)
    monkeypatch.setattr(scenario_module, "await_dataset_ready", _fake_await_dataset_ready)
    monkeypatch.setattr(
        scenario_module,
        "await_kernel_dataset_sources",
        _fake_await_kernel_dataset_sources,
    )

    session_obj = SimpleNamespace(
        id=session_id,
        profile_key="popular_review",
        kaggle_kernel_ref="local:CherryFlash",
        kaggle_dataset=None,
        test_chat_id=-1002210431821,
        main_chat_id=None,
    )
    await scenario._render_and_notify(  # noqa: SLF001
        session_obj,
        [],
        status_message=(123, 1),
        payload_json="{}",
    )

    assert call_order == [
        ("dataset_ready", "zigomaro/cherryflash-session-161"),
        ("push_kernel", "local:CherryFlash"),
        ("bind_wait", "zigomaro/cherryflash"),
    ]

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.FAILED
        assert refreshed.error == "kaggle push failed"
        assert refreshed.kaggle_dataset is None
        assert refreshed.kaggle_kernel_ref == "local:CherryFlash"


@pytest.mark.asyncio
async def test_render_and_notify_non_cherryflash_still_fails_on_bind_wait_error(
    monkeypatch, tmp_path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.CREATED,
            profile_key="default",
            selection_params={"mode": "default"},
            test_chat_id=-1002210431821,
            main_chat_id=None,
            kaggle_kernel_ref="owner/video-announce-renderer",
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    class _DummyClient:
        def get_kernel_status(self, kernel_ref: str) -> dict[str, str]:
            assert kernel_ref == "owner/video-announce-renderer"
            return {"status": "RUNNING"}

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)

    async def _fake_create_dataset(self, session_obj, json_text, finalized, *, client):  # noqa: ANN001,ARG002
        assert session_obj.id == session_id
        return "zigomaro/video-afisha-session-161", []

    async def _fake_push_kernel(self, client, dataset_sources, kernel_ref):  # noqa: ANN001
        assert kernel_ref == "owner/video-announce-renderer"
        assert dataset_sources == ["zigomaro/video-afisha-session-161"]
        return "owner/video-announce-renderer"

    async def _fake_update_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return (123, 1)

    async def _fake_await_dataset_ready(client, dataset_ref, **kwargs):  # noqa: ANN001
        assert dataset_ref == "zigomaro/video-afisha-session-161"
        return {"status": "ready", "files": ["payload.json"]}

    async def _fake_await_kernel_dataset_sources(client, kernel_ref, expected_sources, **kwargs):  # noqa: ANN001
        assert kernel_ref == "owner/video-announce-renderer"
        assert expected_sources == ["zigomaro/video-afisha-session-161"]
        raise RuntimeError("bind failed")

    monkeypatch.setattr(scenario_module, "KaggleClient", _DummyClient)
    monkeypatch.setattr(VideoAnnounceScenario, "_create_dataset", _fake_create_dataset)
    monkeypatch.setattr(VideoAnnounceScenario, "_push_kernel", _fake_push_kernel)
    monkeypatch.setattr(scenario_module, "update_status_message", _fake_update_status_message)
    monkeypatch.setattr(scenario_module, "await_dataset_ready", _fake_await_dataset_ready)
    monkeypatch.setattr(
        scenario_module,
        "await_kernel_dataset_sources",
        _fake_await_kernel_dataset_sources,
    )

    session_obj = SimpleNamespace(
        id=session_id,
        profile_key="default",
        kaggle_kernel_ref="owner/video-announce-renderer",
        kaggle_dataset=None,
        main_chat_id=None,
    )
    await scenario._render_and_notify(  # noqa: SLF001
        session_obj,
        [],
        status_message=(123, 1),
        payload_json="{}",
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.FAILED
        assert refreshed.error == "kaggle push failed"


@pytest.mark.asyncio
async def test_start_session_popular_review_dispatches_direct_pipeline(
    monkeypatch, tmp_path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    captured: dict[str, int] = {}

    async def _fake_run_popular_review_pipeline(self):
        captured["chat_id"] = self.chat_id
        captured["user_id"] = self.user_id

    monkeypatch.setattr(
        VideoAnnounceScenario,
        "run_popular_review_pipeline",
        _fake_run_popular_review_pipeline,
    )
    async def _fake_ensure_access(self):
        return True

    monkeypatch.setattr(VideoAnnounceScenario, "ensure_access", _fake_ensure_access)

    scenario = VideoAnnounceScenario(db, _DummyBot(), chat_id=321, user_id=7)
    await scenario.start_session("popular_review")

    assert captured == {"chat_id": 321, "user_id": 7}


@pytest.mark.asyncio
async def test_handle_video_callback_vidauto_cherryflash_runs_direct_pipeline(
    monkeypatch, tmp_path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    captured: dict[str, int] = {}

    async def _fake_run_popular_review_pipeline(self):
        captured["chat_id"] = self.chat_id
        captured["user_id"] = self.user_id

    monkeypatch.setattr(
        VideoAnnounceScenario,
        "run_popular_review_pipeline",
        _fake_run_popular_review_pipeline,
    )

    callback = _DummyCallback("vidauto:cherryflash", chat_id=456, user_id=9)
    await handle_video_callback(
        callback,
        db,
        _DummyBot(),
        build_events_message=lambda *args, **kwargs: None,
        get_tz_offset=lambda *args, **kwargs: None,
        offset_to_timezone=lambda *args, **kwargs: None,
    )

    assert captured == {"chat_id": 456, "user_id": 9}
    assert callback.answers == [("Запускаю CherryFlash…", False)]


def test_build_cherryflash_selection_manifest_tolerates_missing_trace_entry():
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)

    manifest = scenario._build_cherryflash_selection_manifest(
        {
            "scenes": [
                {
                    "event_id": 101,
                    "title": "Event One",
                    "festival": "80 историй о главном",
                    "date": "12 апреля",
                    "date_iso": "2026-04-12",
                    "time": "19:00",
                    "city": "Калининград",
                    "location_name": "Hall One",
                    "location": "Калининград",
                    "images": ["poster_1.jpg"],
                    "description": "desc one",
                    "scene_variant": "primary",
                },
                {
                    "event_id": 202,
                    "title": "Event Two",
                    "date": "13 апреля",
                    "date_iso": "2026-04-13",
                    "time": "20:00",
                    "city": "Светлогорск",
                    "location_name": "Hall Two",
                    "location": "Светлогорск",
                    "images": ["poster_2.jpg"],
                    "description": "desc two",
                    "scene_variant": "primary",
                },
            ]
        },
        selection_params={
            "popular_review_trace": {
                "101": {"score": 4.2, "source_window": "24h"},
            },
            "story_publish_mode": "video",
        },
        story_publish_enabled=False,
    )

    assert manifest["selected_event_ids"] == [101, 202]
    assert manifest["ribbon_order"] == [202, 101]
    assert manifest["story_publish_enabled"] is False
    assert manifest["story_publish_mode"] == "video"
    assert manifest["events"][0]["festival"] == "80 историй о главном"
    assert manifest["events"][0]["date"] == "2026-04-12"
    assert manifest["events"][0]["date_display"] == "12 апреля"
    assert manifest["events"][0]["time"] == "19:00"
    assert manifest["events"][0]["city"] == "Калининград"
    assert manifest["events"][0]["location_name"] == "Hall One"
    assert manifest["events"][0]["poster_file"] == "poster_1.jpg"
    assert manifest["events"][0]["poster_candidates"] == ["poster_1.jpg"]
    assert manifest["events"][0]["score"] == 4.2
    assert "score" not in manifest["events"][1]


def test_build_cherryflash_selection_manifest_keeps_remote_poster_candidates():
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)

    manifest = scenario._build_cherryflash_selection_manifest(
        {
            "scenes": [
                {
                    "event_id": 101,
                    "title": "Remote Poster Event",
                    "date": "13 апреля",
                    "date_iso": "2026-04-13",
                    "time": "19:00",
                    "city": "Калининград",
                    "location_name": "Hall One",
                    "location": "Калининград",
                    "images": [
                        "https://files.catbox.moe/7fp9nh.jpg",
                        "https://example.com/poster.webp",
                    ],
                    "description": "desc one",
                    "scene_variant": "primary",
                },
            ]
        },
        selection_params={"story_publish_mode": "video"},
        story_publish_enabled=False,
    )

    assert manifest["events"][0]["poster_file"] == "7fp9nh.jpg"
    assert manifest["events"][0]["poster_candidates"] == [
        "https://files.catbox.moe/7fp9nh.jpg",
        "https://example.com/poster.webp",
    ]


def test_cherryflash_bundle_files_include_final_png():
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)

    bundle_files = dict(scenario._iter_cherryflash_bundle_files())

    assert "Final.png" in bundle_files.values()
    assert "video_announce/video_afisha_2d.py" in bundle_files.values()
    assert "video_announce/cherryflash_text.py" in bundle_files.values()
    assert "kaggle_common/story_publish.py" in bundle_files.values()
    assert "scripts/render_cherryflash_guide_true3d_v4.py" in bundle_files.values()


@pytest.mark.asyncio
async def test_create_cherryflash_dataset_writes_story_publish_config_when_enabled(
    monkeypatch,
    tmp_path,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)

    async def _fake_prefetch(payload_obj, tmp_dir, *, max_images_per_scene=3):  # noqa: ARG001
        return None

    async def _fake_selected_event_dates(session_id):  # noqa: ARG001
        return ["2026-04-10"]

    async def _fake_build_story_publish_config(*args, **kwargs):  # noqa: ANN002,ANN003
        return {
            "version": 1,
            "mode": "video",
            "smoke_only": False,
            "period_seconds": 43200,
            "pinned": False,
            "caption": None,
            "targets": [{"peer": "@keniggpt", "label": "@keniggpt", "delay_seconds": 0}],
        }

    def _fake_write_story_secret_files(path, **kwargs):  # noqa: ANN001, ARG001
        cipher_path = Path(path) / "story_publish.enc"
        key_path = Path(path) / "story_publish.key"
        cipher_path.write_bytes(b"cipher")
        key_path.write_bytes(b"key")
        return cipher_path, key_path

    monkeypatch.setattr(scenario, "_prefetch_scene_images", _fake_prefetch)
    monkeypatch.setattr(scenario, "_selected_event_dates", _fake_selected_event_dates)
    monkeypatch.setattr(
        "video_announce.scenario.build_story_publish_config",
        _fake_build_story_publish_config,
    )
    monkeypatch.setattr(
        "video_announce.scenario.write_story_secret_files",
        _fake_write_story_secret_files,
    )
    monkeypatch.setenv("KAGGLE_USERNAME", "zigomaro")

    snapshot_dir = tmp_path / "snapshot"
    calls: list[tuple[str, Path]] = []

    class _DummyClient:
        def create_dataset(self, path):
            calls.append(("create_dataset", Path(path)))
            shutil.copytree(path, snapshot_dir, dirs_exist_ok=True)

        def dataset_status(self, dataset):  # noqa: ANN001
            raise AssertionError(f"dataset_status should not be called: {dataset}")

        def delete_dataset(self, *args, **kwargs):  # noqa: ANN002,ANN003
            raise AssertionError("delete_dataset should not be called")

    payload = {
        "scenes": [
            {
                "event_id": 101,
                "title": "Event One",
                "date": "12 апреля",
                "location": "Калининград",
                "images": ["poster_1.jpg"],
                "description": "desc one",
                "scene_variant": "primary",
            }
        ]
    }

    dataset_id, story_sources = await scenario._create_cherryflash_dataset(
        SimpleNamespace(id=42, main_chat_id=-100123),
        json.dumps(payload, ensure_ascii=False),
        client=_DummyClient(),
        selection_params={"story_publish_enabled": True, "story_publish_mode": "video"},
    )

    assert dataset_id.startswith("zigomaro/cherryflash-session-42-")
    assert story_sources == []
    assert len(calls) == 1
    assert calls[0][0] == "create_dataset"
    assert (snapshot_dir / "story_publish.json").exists()
    assert (snapshot_dir / "story_publish.enc").exists()
    assert (snapshot_dir / "story_publish.key").exists()
    assert (snapshot_dir / "kaggle_common" / "story_publish.py").exists()

    selection_manifest = json.loads(
        (snapshot_dir / "assets" / "cherryflash_selection.json").read_text(encoding="utf-8")
    )
    root_selection_manifest = json.loads(
        (snapshot_dir / "cherryflash_selection.json").read_text(encoding="utf-8")
    )
    assert selection_manifest["story_publish_enabled"] is True
    assert selection_manifest["story_publish_mode"] == "video"
    assert root_selection_manifest == selection_manifest


@pytest.mark.asyncio
async def test_create_cherryflash_dataset_fails_when_story_requested_but_config_missing(
    monkeypatch,
    tmp_path,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)

    async def _fake_prefetch(payload_obj, tmp_dir, *, max_images_per_scene=3):  # noqa: ARG001
        return None

    monkeypatch.setattr(scenario, "_prefetch_scene_images", _fake_prefetch)
    monkeypatch.setattr(
        "video_announce.scenario.build_story_publish_config",
        AsyncMock(return_value=None),
    )
    monkeypatch.setenv("KAGGLE_USERNAME", "zigomaro")

    class _DummyClient:
        def create_dataset(self, path):  # noqa: ANN001
            raise AssertionError("dataset create must not happen when story config is missing")

    payload = {
        "scenes": [
            {
                "event_id": 101,
                "title": "Event One",
                "date": "12 апреля",
                "location": "Калининград",
                "images": ["poster_1.jpg"],
                "description": "desc one",
                "scene_variant": "primary",
            }
        ]
    }

    with pytest.raises(RuntimeError, match="story_publish.json was not generated"):
        await scenario._create_cherryflash_dataset(
            SimpleNamespace(id=42, main_chat_id=-100123),
            json.dumps(payload, ensure_ascii=False),
            client=_DummyClient(),
            selection_params={"story_publish_enabled": True, "story_publish_mode": "video"},
        )


@pytest.mark.asyncio
async def test_create_dataset_preserves_story_flags_when_payload_selection_meta_is_stripped(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("KAGGLE_USERNAME", "zigomaro")
    monkeypatch.setenv("VK_ACCESS_TOKEN5", "vk-token")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)
    captured: dict[str, object] = {}

    async def _fake_create_cherryflash_dataset(session_obj, json_text, *, client, selection_params):  # noqa: ANN001
        captured["selection_params"] = dict(selection_params)
        return "zigomaro/cherryflash-session-test", []

    monkeypatch.setattr(scenario, "_create_cherryflash_dataset", _fake_create_cherryflash_dataset)

    payload = {
        "selection_params": {
            "mode": "popular_review",
        },
        "scenes": [],
    }
    session_obj = SimpleNamespace(
        id=158,
        kaggle_kernel_ref="local:CherryFlash",
        selection_params={
            "mode": "popular_review",
            "story_publish_enabled": True,
            "story_publish_mode": "video",
            "story_targets_override": [
                {"peer": "@kenigevents", "delay_seconds": 0, "mode": "upload"},
                {"peer": "@lovekenig", "delay_seconds": 600, "mode": "repost_previous"},
                {"peer": "@loving_guide39", "delay_seconds": 600, "mode": "repost_previous"},
            ],
        },
    )

    dataset_id, story_sources = await scenario._create_dataset(
        session_obj,
        json.dumps(payload, ensure_ascii=False),
        [],
        client=SimpleNamespace(),
    )

    assert dataset_id == "zigomaro/cherryflash-session-test"
    assert story_sources == []
    assert captured["selection_params"] == {
        "mode": "popular_review",
        "story_publish_enabled": True,
        "story_publish_mode": "video",
        "story_targets_override": [
            {"peer": "@kenigevents", "delay_seconds": 0, "mode": "upload"},
            {"peer": "@lovekenig", "delay_seconds": 600, "mode": "repost_previous"},
            {"peer": "@loving_guide39", "delay_seconds": 600, "mode": "repost_previous"},
        ],
    }


@pytest.mark.asyncio
async def test_create_crumple_dataset_bundles_current_story_helper_when_story_enabled(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("KAGGLE_USERNAME", "zigomaro")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)
    snapshot_dir = tmp_path / "snapshot"
    calls: list[tuple[str, Path]] = []

    async def _fake_selected_event_dates(session_id):  # noqa: ARG001
        return ["2026-06-15"]

    async def _fake_selected_event_cities(session_id):  # noqa: ARG001
        return ["Калининград"]

    async def _fake_build_story_publish_config(*args, **kwargs):  # noqa: ANN002,ANN003
        return {
            "version": 1,
            "mode": "video",
            "targets": [
                {
                    "peer": "kenigeventsofficial",
                    "label": "vk:kenigeventsofficial:wall",
                    "transport": "vk_wall",
                    "mode": "upload",
                    "required": True,
                }
            ],
        }

    class _DummyClient:
        def create_dataset(self, path):  # noqa: ANN001
            calls.append(("create_dataset", Path(path)))
            shutil.copytree(path, snapshot_dir, dirs_exist_ok=True)

        def delete_dataset(self, *args, **kwargs):  # noqa: ANN002,ANN003
            raise AssertionError("delete_dataset should not be called")

    monkeypatch.setattr(scenario, "_selected_event_dates", _fake_selected_event_dates)
    monkeypatch.setattr(scenario, "_selected_event_cities", _fake_selected_event_cities)
    monkeypatch.setattr(scenario, "_copy_assets", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "video_announce.scenario.build_story_publish_config",
        _fake_build_story_publish_config,
    )
    def _fake_write_story_secret_files(path, **kwargs):  # noqa: ANN001, ARG001
        cipher_path = Path(path) / "story_publish.enc"
        key_path = Path(path) / "story_publish.key"
        cipher_path.write_bytes(b"cipher")
        key_path.write_bytes(b"key")
        return cipher_path, key_path

    monkeypatch.setattr(
        "video_announce.scenario.write_story_secret_files",
        _fake_write_story_secret_files,
    )

    session_obj = SimpleNamespace(
        id=676,
        kaggle_kernel_ref="zigomaro/crumple-video",
        selection_params={"story_publish_enabled": True, "story_publish_mode": "video"},
        main_chat_id=-100123,
    )

    dataset_id, story_sources = await scenario._create_dataset(
        session_obj,
        json.dumps({"scenes": []}, ensure_ascii=False),
        [],
        client=_DummyClient(),
    )

    assert dataset_id == "zigomaro/video-afisha-session-676"
    assert story_sources == []
    assert len(calls) == 1
    assert calls[0][0] == "create_dataset"
    bundled_helper = snapshot_dir / "kaggle_common" / "story_publish.py"
    assert bundled_helper.exists()
    assert bundled_helper.read_text(encoding="utf-8") == (
        Path("kaggle/CrumpleVideo/story_publish.py").read_text(encoding="utf-8")
    )
    assert (snapshot_dir / "story_publish.json").exists()
    assert (snapshot_dir / "story_publish.enc").exists()
    assert (snapshot_dir / "story_publish.key").exists()


@pytest.mark.asyncio
async def test_video_lane_assignment_skips_busy_lane_and_sets_resource_and_target(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_VIDEO_LANE_AUTH_ENVS",
        "TELEGRAM_AUTH_BUNDLE_STORY,TELEGRAM_AUTH_BUNDLE_S22_VIDEO1",
    )
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_CHERRYFLASH_KERNEL_REFS",
        "zigomaro/cherryflash,zigomaro/cherryflash-video1",
    )
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)

    async with db.get_session() as session:
        busy = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="popular_review",
            selection_params={
                "story_publish_enabled": True,
                "_video_lane_auth_env": "TELEGRAM_AUTH_BUNDLE_STORY",
            },
            kaggle_kernel_ref="zigomaro/cherryflash",
        )
        fresh = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.SELECTED,
            profile_key="popular_review",
            selection_params={"story_publish_enabled": True},
            kaggle_kernel_ref="local:CherryFlash",
        )
        session.add_all([busy, fresh])
        await session.commit()
        await session.refresh(fresh)

        params, kernel_ref, error = await scenario._assign_video_lane_for_render(
            session,
            fresh,
            dict(fresh.selection_params),
        )

    assert error is None
    assert params["_video_lane_auth_env"] == "TELEGRAM_AUTH_BUNDLE_S22_VIDEO1"
    assert params["_video_lane_resource_key"] == "telegram_session:env:TELEGRAM_AUTH_BUNDLE_S22_VIDEO1"
    assert params["_kaggle_source_kernel_ref"] == "local:CherryFlash"
    assert params["_kaggle_target_kernel_ref"] == "zigomaro/cherryflash-video1"
    assert kernel_ref == "zigomaro/cherryflash-video1"


@pytest.mark.asyncio
async def test_video_lane_assignment_skips_active_resource_lease(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_VIDEO_LANE_AUTH_ENVS",
        "TELEGRAM_AUTH_BUNDLE_STORY,TELEGRAM_AUTH_BUNDLE_S22_VIDEO1",
    )
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_CHERRYFLASH_KERNEL_REFS",
        "zigomaro/cherryflash,zigomaro/cherryflash-video1",
    )
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO kaggle_resource_lease(
                resource_key, run_id, holder_kind, status, acquired_at, expires_at, updated_at
            )
            VALUES (?, 'videoannounce:703', 'kaggle', 'active', ?, ?, ?)
            """,
            (
                "telegram_session:env:TELEGRAM_AUTH_BUNDLE_STORY",
                (now - timedelta(minutes=5)).isoformat(),
                (now + timedelta(hours=1)).isoformat(),
                now.isoformat(),
            ),
        )
        await conn.commit()

    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)
    async with db.get_session() as session:
        fresh = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.SELECTED,
            profile_key="popular_review",
            selection_params={"story_publish_enabled": True},
            kaggle_kernel_ref="local:CherryFlash",
        )
        session.add(fresh)
        await session.commit()
        await session.refresh(fresh)

        params, kernel_ref, error = await scenario._assign_video_lane_for_render(
            session,
            fresh,
            dict(fresh.selection_params),
        )

    assert error is None
    assert params["_video_lane_auth_env"] == "TELEGRAM_AUTH_BUNDLE_S22_VIDEO1"
    assert params["_video_lane_resource_key"] == "telegram_session:env:TELEGRAM_AUTH_BUNDLE_S22_VIDEO1"
    assert params["_kaggle_source_kernel_ref"] == "local:CherryFlash"
    assert params["_kaggle_target_kernel_ref"] == "zigomaro/cherryflash-video1"
    assert kernel_ref == "zigomaro/cherryflash-video1"


@pytest.mark.asyncio
async def test_video_lane_assignment_fails_when_all_resource_leases_busy(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv(
        "VIDEO_ANNOUNCE_VIDEO_LANE_AUTH_ENVS",
        "TELEGRAM_AUTH_BUNDLE_STORY,TELEGRAM_AUTH_BUNDLE_S22_VIDEO1",
    )
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.raw_conn() as conn:
        for env_name in ["TELEGRAM_AUTH_BUNDLE_STORY", "TELEGRAM_AUTH_BUNDLE_S22_VIDEO1"]:
            await conn.execute(
                """
                INSERT INTO kaggle_resource_lease(
                    resource_key, run_id, holder_kind, status, acquired_at, expires_at, updated_at
                )
                VALUES (?, ?, 'kaggle', 'active', ?, ?, ?)
                """,
                (
                    f"telegram_session:env:{env_name}",
                    f"videoannounce:{env_name}",
                    (now - timedelta(minutes=5)).isoformat(),
                    (now + timedelta(hours=1)).isoformat(),
                    now.isoformat(),
                ),
            )
        await conn.commit()

    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)
    async with db.get_session() as session:
        fresh = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.SELECTED,
            profile_key="popular_review",
            selection_params={"story_publish_enabled": True},
            kaggle_kernel_ref="local:CherryFlash",
        )
        session.add(fresh)
        await session.commit()
        await session.refresh(fresh)

        params, kernel_ref, error = await scenario._assign_video_lane_for_render(
            session,
            fresh,
            dict(fresh.selection_params),
        )

    assert params == {"story_publish_enabled": True}
    assert kernel_ref == "local:CherryFlash"
    assert error and "video-lanes заняты" in error


@pytest.mark.asyncio
async def test_video_lane_notice_is_deduplicated_per_scheduled_slot(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db=db, bot=bot, chat_id=123, user_id=123)
    scenario_module._video_lane_notice_cache.clear()

    await scenario._notify_video_lane_once(slot_key="tomorrow:2026-08-31", text="busy")
    await scenario._notify_video_lane_once(
        slot_key="tomorrow:2026-08-31", text="busy with changed detail"
    )
    await scenario._notify_video_lane_once(slot_key="tomorrow:2026-09-01", text="busy")

    assert [(chat_id, text) for chat_id, text, _kwargs in bot.messages] == [
        (123, "busy"),
        (123, "busy"),
    ]
    scenario_module._video_lane_notice_cache.clear()


@pytest.mark.asyncio
async def test_fresh_kaggle_ledger_heartbeat_prevents_fixed_timeout_failure(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime.now(timezone.utc)
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO kaggle_run_ledger(
                run_id, session_id, kind, notebook, status, phase, progress_json,
                token_hash, last_heartbeat_at, created_at, updated_at
            )
            VALUES (?, ?, 'cherryflash', 'CherryFlash', 'alive', 'render', ?, 'token-hash', ?, ?, ?)
            """,
            (
                "videoannounce:701",
                701,
                json.dumps({"progress_label": "рендер 80%"}, ensure_ascii=False),
                (now - timedelta(minutes=5)).isoformat(),
                (now - timedelta(hours=4)).isoformat(),
                now.isoformat(),
            ),
        )
        await conn.commit()

    heartbeat = await poller_module._fresh_kaggle_ledger_heartbeat(
        db,
        "videoannounce:701",
        now=now,
        grace_minutes=20,
    )

    assert heartbeat is not None
    assert heartbeat["phase"] == "render"
    assert heartbeat["progress"]["progress_label"] == "рендер 80%"


@pytest.mark.asyncio
async def test_create_story_publish_only_dataset_filters_failed_vk_target(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("KAGGLE_USERNAME", "zigomaro")
    monkeypatch.setenv("VK_ACCESS_TOKEN5", "vk-token")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    video_path = tmp_path / "crumple_video_final.mp4"
    video_path.write_bytes(b"video")
    snapshot_dir = tmp_path / "publish_only_snapshot"

    async def _fake_build_story_publish_config(*args, **kwargs):  # noqa: ANN002,ANN003
        return {
            "version": 1,
            "mode": "video",
            "targets": [
                {
                    "peer": "me",
                    "label": "me",
                    "mode": "upload",
                },
                {
                    "peer": "kenigeventsofficial",
                    "label": "vk:kenigeventsofficial:wall",
                    "transport": "vk_wall",
                    "delay_seconds": 300,
                    "mode": "upload",
                    "caption": "События на завтра",
                },
                {
                    "peer": "@kenigevents",
                    "label": "@kenigevents",
                    "mode": "repost_previous",
                    "required": True,
                },
            ],
        }

    class _DummyClient:
        def create_dataset(self, path):  # noqa: ANN001
            shutil.copytree(path, snapshot_dir, dirs_exist_ok=True)

    monkeypatch.setattr(
        poller_module,
        "build_story_publish_config",
        _fake_build_story_publish_config,
    )
    monkeypatch.setattr(
        poller_module,
        "create_kaggle_run_config",
        AsyncMock(return_value=None),
    )

    dataset_id, story_sources = await poller_module._create_story_publish_only_dataset(
        db,
        _DummyClient(),
        SimpleNamespace(
            id=676,
            main_chat_id=None,
            selection_params={"story_publish_enabled": True, "story_publish_mode": "video"},
        ),
        video_path=video_path,
        story_report={
            "targets": [
                {
                    "label": "vk:kenigeventsofficial:wall",
                    "transport": "vk_wall",
                    "ok": False,
                    "error": 'ValueError: No user has "kenigeventsofficial" as username',
                }
            ]
        },
    )

    assert dataset_id.startswith("zigomaro/crumple-story-publish-session-676-")
    assert story_sources == []
    assert (snapshot_dir / "crumple_video_final.mp4").read_bytes() == b"video"
    assert (snapshot_dir / "story_publish.enc").exists()
    assert (snapshot_dir / "story_publish.key").exists()
    assert (snapshot_dir / "kaggle_common" / "story_publish.py").exists()
    story_config = json.loads((snapshot_dir / "story_publish.json").read_text(encoding="utf-8"))
    assert story_config["publish_only_recovery"] is True
    assert story_config["targets"] == [
        {
            "peer": "kenigeventsofficial",
            "label": "vk:kenigeventsofficial:wall",
            "transport": "vk_wall",
            "delay_seconds": 0,
            "mode": "upload",
            "caption": "События на завтра",
            "required": True,
            "blocking": False,
        }
    ]


@pytest.mark.asyncio
async def test_create_cherryflash_dataset_retries_dataset_create_after_invalid_token(
    monkeypatch,
    tmp_path,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    scenario = VideoAnnounceScenario(db=db, bot=_DummyBot(), chat_id=0, user_id=0)

    async def _fake_prefetch(payload_obj, tmp_dir, *, max_images_per_scene=3):  # noqa: ARG001
        return None

    async def _fake_selected_event_dates(session_id):  # noqa: ARG001
        return []

    async def _fake_sleep(seconds):  # noqa: ANN001
        sleep_calls.append(seconds)

    monkeypatch.setattr(scenario, "_prefetch_scene_images", _fake_prefetch)
    monkeypatch.setattr(scenario, "_selected_event_dates", _fake_selected_event_dates)
    monkeypatch.setattr("video_announce.scenario.asyncio.sleep", _fake_sleep)
    monkeypatch.setenv("KAGGLE_USERNAME", "zigomaro")

    sleep_calls: list[int] = []
    create_calls = 0

    class _DummyClient:
        def create_dataset(self, path):  # noqa: ANN001
            nonlocal create_calls
            create_calls += 1
            if create_calls == 1:
                raise RuntimeError(
                    'Kaggle dataset_create_new failed (status=400; '
                    '{"error":{"code":400,"message":"Invalid token","status":"INVALID_ARGUMENT"}})'
                )
            return None

        def dataset_status(self, dataset):  # noqa: ANN001
            raise AssertionError(f"dataset_status should not be called: {dataset}")

        def delete_dataset(self, *args, **kwargs):  # noqa: ANN002,ANN003
            raise AssertionError("delete_dataset should not be called")

    payload = {
        "scenes": [
            {
                "event_id": 101,
                "title": "Event One",
                "date": "12 апреля",
                "location": "Калининград",
                "images": ["poster_1.jpg"],
                "description": "desc one",
                "scene_variant": "primary",
            }
        ]
    }

    dataset_id, story_sources = await scenario._create_cherryflash_dataset(
        SimpleNamespace(id=42, main_chat_id=-100123),
        json.dumps(payload, ensure_ascii=False),
        client=_DummyClient(),
        selection_params={"story_publish_enabled": False, "story_publish_mode": "video"},
    )

    assert dataset_id.startswith("zigomaro/cherryflash-session-42-")
    assert story_sources == []
    assert create_calls == 2
    assert sleep_calls == [10]


@pytest.mark.asyncio
async def test_prefetch_scene_images_writes_into_assets_posters(monkeypatch, tmp_path):
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)
    payload = {
        "scenes": [
            {
                "images": ["https://example.com/poster.jpg"],
            }
        ]
    }

    class _Resp:
        status_code = 200
        content = b"fake-image"

    async def _fake_http_call(*args, **kwargs):
        return _Resp()

    monkeypatch.setattr("video_announce.scenario.http_call", _fake_http_call)

    await scenario._prefetch_scene_images(payload, tmp_path, max_images_per_scene=3)

    assert (tmp_path / "assets" / "posters" / "scene_1_1.jpg").exists()
    assert payload["scenes"][0]["images"] == ["scene_1_1.jpg"]
    assert payload["scenes"][0]["image"] == "scene_1_1.jpg"


@pytest.mark.asyncio
async def test_prefetch_scene_images_uses_normal_policy_for_yandex_bucket(monkeypatch, tmp_path):
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)
    payload = {
        "scenes": [
            {
                "images": [
                    "https://storage.yandexcloud.net/cherryflash-backfill/posters/abc123.webp?X-Amz-Algorithm=AWS4-HMAC-SHA256"
                ],
            }
        ]
    }
    calls: list[dict] = []

    class _Resp:
        status_code = 200
        content = b"fake-image"

    async def _fake_http_call(*args, **kwargs):
        calls.append(kwargs)
        return _Resp()

    monkeypatch.setattr("video_announce.scenario.http_call", _fake_http_call)

    await scenario._prefetch_scene_images(payload, tmp_path, max_images_per_scene=3)

    assert calls
    assert calls[0]["timeout"] == 20
    assert calls[0]["retries"] == 3
    assert calls[0]["backoff"] == 1.0
    assert (tmp_path / "assets" / "posters" / "scene_1_1.webp").exists()
    assert payload["scenes"][0]["images"] == ["scene_1_1.webp"]
    assert payload["scenes"][0]["image"] == "scene_1_1.webp"


@pytest.mark.asyncio
async def test_prefetch_scene_images_falls_back_to_yandex_origin_on_cdn_tls_failure(
    monkeypatch, tmp_path
):
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)
    cdn_url = "https://static.kenigevents.ru/p/image/v2/aa/poster.webp"
    origin_url = (
        "https://storage.yandexcloud.net/kenigevents.ru/"
        "p/image/v2/aa/poster.webp"
    )
    payload = {"scenes": [{"images": [cdn_url]}]}
    calls: list[str] = []

    class _Resp:
        status_code = 200
        content = b"origin-image"

    async def _fake_http_call(_label, _method, url, **_kwargs):
        calls.append(url)
        if url == cdn_url:
            raise RuntimeError("certificate verify failed: hostname mismatch")
        assert url == origin_url
        return _Resp()

    monkeypatch.setattr("video_announce.scenario.http_call", _fake_http_call)

    await scenario._prefetch_scene_images(payload, tmp_path, max_images_per_scene=3)

    assert calls == [cdn_url, origin_url]
    assert (tmp_path / "assets" / "posters" / "scene_1_1.webp").read_bytes() == b"origin-image"
    assert payload["scenes"][0]["images"] == ["scene_1_1.webp"]


@pytest.mark.asyncio
async def test_prefetch_scene_images_uses_fast_policy_for_catbox(monkeypatch, tmp_path):
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)
    payload = {
        "scenes": [
            {
                "images": ["https://files.catbox.moe/poster.jpg"],
            }
        ]
    }
    calls: list[dict] = []

    class _Resp:
        status_code = 200
        content = b"fake-image"

    async def _fake_http_call(*args, **kwargs):
        calls.append(kwargs)
        return _Resp()

    monkeypatch.setattr("video_announce.scenario.http_call", _fake_http_call)

    await scenario._prefetch_scene_images(payload, tmp_path, max_images_per_scene=3)

    assert calls
    assert calls[0]["timeout"] == 6
    assert calls[0]["retries"] == 1
    assert calls[0]["backoff"] == 0.25


@pytest.mark.asyncio
async def test_prefetch_test_posters_uses_fast_policy_for_catbox(monkeypatch, tmp_path):
    scenario = VideoAnnounceScenario(db=None, bot=_DummyBot(), chat_id=0, user_id=0)
    payload = {
        "scenes": [
            {
                "images": ["https://files.catbox.moe/poster.jpg"],
            }
        ]
    }
    calls: list[dict] = []

    class _Resp:
        status_code = 200
        content = b"fake-image"

    async def _fake_http_call(*args, **kwargs):
        calls.append(kwargs)
        return _Resp()

    monkeypatch.setattr("video_announce.scenario.http_call", _fake_http_call)

    await scenario._prefetch_test_posters(payload, tmp_path)

    assert calls
    assert calls[0]["timeout"] == 6
    assert calls[0]["retries"] == 1
    assert calls[0]["backoff"] == 0.25


@pytest.mark.asyncio
async def test_prepare_tomorrow_session_builds_manual_preflight_without_render(
    monkeypatch, tmp_path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now_local = datetime.now(LOCAL_TZ)
    tomorrow = (now_local + timedelta(days=1)).date()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        ev = Event(
            title="Manual Event",
            description="d",
            source_text="s",
            date=tomorrow.isoformat(),
            time="19:00",
            location_name="Loc",
            city="City",
            photo_urls=["https://example.com/1.jpg"],
            photo_count=1,
        )
        session.add(ev)
        await session.commit()
        await session.refresh(ev)
        session.add(
            EventPoster(
                event_id=ev.id,
                poster_hash="h1",
                ocr_text="TEXT",
                ocr_title="TITLE",
                updated_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    async def _should_not_render(*args, **kwargs):  # noqa: ANN002,ANN003
        raise AssertionError("prepare_tomorrow_session must not auto-start render")

    monkeypatch.setattr(VideoAnnounceScenario, "start_render", _should_not_render)

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)
    await scenario.prepare_tomorrow_session()

    async with db.get_session() as session:
        res = await session.execute(select(VideoAnnounceSession))
        sessions = list(res.scalars().all())
        assert len(sessions) == 1
        sess = sessions[0]
        assert sess.status == VideoAnnounceSessionStatus.SELECTED
        assert isinstance(sess.selection_params, dict)
        assert sess.selection_params.get("target_date") == tomorrow.isoformat()
        assert sess.selection_params.get("render_scene_limit") == 12
        assert sess.kaggle_kernel_ref is None

        res_items = await session.execute(
            select(VideoAnnounceItem).where(VideoAnnounceItem.session_id == sess.id)
        )
        items = list(res_items.scalars().all())
        assert any(it.status == VideoAnnounceItemStatus.READY for it in items)

    texts = [text for _, text, _ in bot.messages]
    assert any("подготовлена" in text for text in texts)
    assert any("SELECTED" in text for text in texts)


@pytest.mark.asyncio
async def test_show_kernel_selection_blocks_when_ready_items_exceed_limit(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.SELECTED,
            profile_key="default",
            selection_params={"render_scene_limit": 12},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)

        for idx in range(13):
            ev = Event(
                title=f"Event {idx}",
                description="d",
                source_text="s",
                date="2026-03-14",
                time="19:00",
                location_name=f"Loc {idx}",
                city="City",
                photo_urls=[f"https://example.com/{idx}.jpg"],
                photo_count=1,
            )
            session.add(ev)
            await session.flush()
            session.add(
                VideoAnnounceItem(
                    session_id=sess.id,
                    event_id=ev.id,
                    position=idx + 1,
                    status=VideoAnnounceItemStatus.READY,
                )
            )
        await session.commit()

    bot = _DummyBot()
    scenario = VideoAnnounceScenario(db, bot, chat_id=123, user_id=1)

    msg = await scenario.show_kernel_selection(sess.id)

    assert msg == (
        "Выбрано 13 событий, а текущий рендер поддерживает максимум 12. "
        "Снимите лишние в SELECTED перед запуском."
    )


def test_story_publish_only_lock_prevents_concurrent_session(monkeypatch, tmp_path):
    monkeypatch.setattr(poller_module, "STORY_PUBLISH_ONLY_LOCK_DIR", tmp_path)

    first = poller_module._acquire_story_publish_only_lock(676)
    assert first is not None
    try:
        assert poller_module._acquire_story_publish_only_lock(676) is None
    finally:
        poller_module._release_story_publish_only_lock(first)

    second = poller_module._acquire_story_publish_only_lock(676)
    assert second is not None
    poller_module._release_story_publish_only_lock(second)


@pytest.mark.asyncio
async def test_story_publish_only_recovery_skips_already_published_session(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(
            VideoAnnounceSession(
                id=676,
                status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
            )
        )
        session.add(
            VideoAnnounceSession(
                id=677,
                status=VideoAnnounceSessionStatus.FAILED,
            )
        )
        await session.commit()

    assert await poller_module._load_session_if_publish_only_allowed(db, 676) is None

    recoverable = await poller_module._load_session_if_publish_only_allowed(db, 677)
    assert recoverable is not None
    assert recoverable.id == 677
