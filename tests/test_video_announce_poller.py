from __future__ import annotations

from pathlib import Path

import pytest

from db import Database
from models import User, VideoAnnounceSession, VideoAnnounceSessionStatus
import video_announce.poller as poller_module


class _DummyBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str, **kwargs) -> None:  # noqa: ARG002
        self.messages.append((chat_id, text))


@pytest.mark.asyncio
async def test_resume_rendering_sessions_fails_local_kernel_refs(monkeypatch, tmp_path: Path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            kaggle_kernel_ref="local:CrumpleVideo",
            test_chat_id=123,
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    def _should_not_poll(*args, **kwargs):  # noqa: ANN002,ANN003
        raise AssertionError("local kernel ref must not start Kaggle poller on resume")

    monkeypatch.setattr(poller_module, "start_kernel_poller_task", _should_not_poll)

    bot = _DummyBot()
    recovered = await poller_module.resume_rendering_sessions(db, bot, chat_id=123)

    assert recovered == 0
    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.FAILED
        assert refreshed.error == "runtime restart before Kaggle handoff; rerun required"
    assert bot.messages == [
        (
            123,
            "⚠️ Сессия #1: рантайм перезапустился до подтверждённого запуска Kaggle.\n"
            "Сессия переведена в FAILED; нужен повторный запуск.",
        )
    ]


@pytest.mark.asyncio
async def test_resume_rendering_sessions_restarts_remote_kernel_pollers(monkeypatch, tmp_path: Path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            kaggle_kernel_ref="zigomaro/crumple-video",
            kaggle_dataset="zigomaro/video-announce-session-1",
            test_chat_id=123,
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    started: list[tuple[int, str | None]] = []

    def _fake_start_poller(db, client, session_obj, **kwargs):  # noqa: ANN001
        del db, client, kwargs
        started.append((session_obj.id, session_obj.kaggle_dataset))
        return None

    monkeypatch.setattr(poller_module, "start_kernel_poller_task", _fake_start_poller)

    bot = _DummyBot()
    recovered = await poller_module.resume_rendering_sessions(db, bot, chat_id=123)

    assert recovered == 1
    assert started == [(session_id, "zigomaro/video-announce-session-1")]
    assert bot.messages == []
