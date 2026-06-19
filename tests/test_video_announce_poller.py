from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from db import Database, close_known_databases
from models import User, VideoAnnounceSession, VideoAnnounceSessionStatus
import video_announce.poller as poller_module


class _DummyBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str, **kwargs) -> None:  # noqa: ARG002
        self.messages.append((chat_id, text))


class _CompleteKernelClient:
    def get_kernel_status(self, kernel_ref: str) -> dict:  # noqa: ARG002
        return {"status": "complete"}

    def download_kernel_output(self, kernel_ref: str, *, path: Path, **kwargs) -> list[str]:  # noqa: ARG002
        video = path / "cherryflash_full_final.mp4"
        video.write_bytes(b"fake-video")
        return [video.name]


@pytest_asyncio.fixture(autouse=True)
async def _close_databases_after_test():
    yield
    await close_known_databases()


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
            started_at=datetime.now(timezone.utc) - timedelta(minutes=30),
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
async def test_resume_rendering_sessions_keeps_fresh_local_kernel_refs_during_handoff_grace(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            kaggle_kernel_ref="local:CherryFlash",
            test_chat_id=123,
            started_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    def _should_not_poll(*args, **kwargs):  # noqa: ANN002,ANN003
        raise AssertionError("fresh local handoff session must not start Kaggle poller yet")

    monkeypatch.setattr(poller_module, "start_kernel_poller_task", _should_not_poll)

    bot = _DummyBot()
    recovered = await poller_module.resume_rendering_sessions(db, bot, chat_id=123)

    assert recovered == 0
    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.RENDERING
        assert refreshed.error in (None, "")
    assert bot.messages == []


@pytest.mark.asyncio
async def test_resume_rendering_sessions_skips_fresh_remote_pre_handoff_without_dataset(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            kaggle_kernel_ref="zigomaro/cherryflash-video1",
            kaggle_dataset=None,
            test_chat_id=123,
            started_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    def _should_not_poll(*args, **kwargs):  # noqa: ANN002,ANN003
        raise AssertionError("pre-handoff session without dataset must not poll stale Kaggle kernel")

    monkeypatch.setattr(poller_module, "start_kernel_poller_task", _should_not_poll)

    bot = _DummyBot()
    recovered = await poller_module.resume_rendering_sessions(db, bot, chat_id=123)

    assert recovered == 0
    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.RENDERING
        assert refreshed.error in (None, "")
    assert bot.messages == []


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


@pytest.mark.asyncio
async def test_resume_rendering_sessions_revives_false_failed_live_ledger(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.FAILED,
            kaggle_kernel_ref="zigomaro/cherryflash",
            kaggle_dataset="zigomaro/cherryflash-session-714",
            test_chat_id=123,
            error="missing video output",
            finished_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    heartbeat_at = datetime.now(timezone.utc).isoformat()
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO kaggle_run_ledger(
                run_id, session_id, kind, notebook, kernel_ref, dataset_ref,
                status, phase, token_hash, progress_json, created_at, updated_at,
                last_heartbeat_at
            )
            VALUES(?, ?, 'cherryflash', 'CherryFlash', ?, ?, 'alive', 'render',
                   'test', '{}', ?, ?, ?)
            """,
            (
                f"videoannounce:{session_id}",
                session_id,
                "zigomaro/cherryflash",
                "zigomaro/cherryflash-session-714",
                heartbeat_at,
                heartbeat_at,
                heartbeat_at,
            ),
        )
        await conn.commit()

    started: list[tuple[int, str | None]] = []

    def _fake_start_poller(db, client, session_obj, **kwargs):  # noqa: ANN001
        del db, client, kwargs
        started.append((session_obj.id, session_obj.kaggle_dataset))
        return None

    monkeypatch.setattr(poller_module, "start_kernel_poller_task", _fake_start_poller)

    bot = _DummyBot()
    recovered = await poller_module.resume_rendering_sessions(db, bot, chat_id=123)

    assert recovered == 1
    assert started == [(session_id, "zigomaro/cherryflash-session-714")]
    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.RENDERING
        assert refreshed.finished_at is None
        assert refreshed.error in (None, "")


@pytest.mark.asyncio
async def test_kenigsberg_poller_clears_remote_telegram_registry(monkeypatch):
    removed: list[tuple[str, str]] = []

    async def fake_run_kernel_poller(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    async def fake_remove_job(job_type: str, kernel_ref: str) -> None:
        removed.append((job_type, kernel_ref))

    monkeypatch.setattr(poller_module, "run_kernel_poller", fake_run_kernel_poller)
    monkeypatch.setattr(poller_module, "remove_job", fake_remove_job)

    session_obj = VideoAnnounceSession(
        id=777,
        status=VideoAnnounceSessionStatus.RENDERING,
        profile_key="kenigsberg_story",
        kaggle_kernel_ref="zigomaro/koenigsberg-stories",
    )

    task = poller_module.start_kernel_poller_task(
        object(),
        object(),
        session_obj,
        bot=object(),
        notify_chat_id=1,
        test_chat_id=None,
        main_chat_id=None,
    )

    await task
    await asyncio.sleep(0)

    assert removed == [("kenigsberg_story", "zigomaro/koenigsberg-stories")]


@pytest.mark.asyncio
async def test_resume_rendering_sessions_uses_superadmin_dm_not_channel_fallback(tmp_path: Path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(User(user_id=777, is_superadmin=True))
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            kaggle_kernel_ref="local:CherryFlash",
            test_chat_id=-1002210431821,
            started_at=datetime.now(timezone.utc) - timedelta(minutes=30),
        )
        session.add(sess)
        await session.commit()

    bot = _DummyBot()
    recovered = await poller_module.resume_rendering_sessions(db, bot)

    assert recovered == 0
    assert bot.messages == [
        (
            777,
            "⚠️ Сессия #1: рантайм перезапустился до подтверждённого запуска Kaggle.\n"
            "Сессия переведена в FAILED; нужен повторный запуск.",
        )
    ]


@pytest.mark.asyncio
async def test_update_status_sets_published_at_for_published_test(tmp_path: Path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.DONE,
            profile_key="popular_review",
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    updated = await poller_module._update_status(
        db,
        session_id,
        status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
    )

    assert updated is not None
    assert updated.status == VideoAnnounceSessionStatus.PUBLISHED_TEST
    assert updated.published_at is not None


@pytest.mark.asyncio
async def test_completed_kernel_bot_delivery_failure_becomes_publish_blocked(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="popular_review",
            kaggle_kernel_ref="zigomaro/cherryflash",
            test_chat_id=-1002210431821,
            selection_params={"target_date": "2026-06-16", "mode": "popular_review"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    async def noop_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def fake_caption(*args, **kwargs):  # noqa: ANN002,ANN003
        return "Видеоанонс #682 · 16 июня"

    async def raise_bad_gateway(*args, **kwargs):  # noqa: ANN002,ANN003
        raise RuntimeError("Telegram server says - Bad Gateway")

    async def noop_send_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", noop_status_message)
    monkeypatch.setattr(poller_module, "_build_video_caption", fake_caption)
    monkeypatch.setattr(poller_module, "_send_video_with_preview", raise_bad_gateway)
    monkeypatch.setattr(poller_module, "_send_logs", noop_send_logs)

    bot = _DummyBot()
    await poller_module.run_kernel_poller(
        db,
        _CompleteKernelClient(),
        VideoAnnounceSession(id=session_id, status=VideoAnnounceSessionStatus.RENDERING, kaggle_kernel_ref="zigomaro/cherryflash"),
        bot=bot,
        notify_chat_id=777,
        test_chat_id=-1002210431821,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=1,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.PUBLISH_BLOCKED
        assert refreshed.video_url == "cherryflash_full_final.mp4"
        assert refreshed.error == "post-render bot delivery failed"
        assert refreshed.finished_at is not None
    assert bot.messages == [
        (
            777,
            "⚠️ Сессия #1: видео готово, но бот не смог доставить mp4 в тест/notify чат. Полный Kaggle rerender не требуется.",
        )
    ]


@pytest.mark.asyncio
async def test_story_failure_after_render_becomes_publish_blocked(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="default",
            kaggle_kernel_ref="zigomaro/crumple-video",
            test_chat_id=123,
            selection_params={"target_date": "2026-06-17", "mode": "tomorrow"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    class StoryFailureClient(_CompleteKernelClient):
        def download_kernel_output(self, kernel_ref: str, *, path: Path, **kwargs) -> list[str]:  # noqa: ARG002
            video = path / "crumple_video_final.mp4"
            video.write_bytes(b"fake-video")
            report = path / "story_publish_report.json"
            report.write_text(
                '{"ok": false, "targets": [{"label": "@kenigevents", "ok": false, "error": "BOOSTS_REQUIRED"}]}',
                encoding="utf-8",
            )
            return [video.name, report.name]

    async def noop_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def fake_caption(*args, **kwargs):  # noqa: ANN002,ANN003
        return "Видеоанонс #682 · 17 июня"

    async def no_recovery(*args, **kwargs):  # noqa: ANN002,ANN003
        return False

    async def noop_send_video(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def noop_send_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", noop_status_message)
    monkeypatch.setattr(poller_module, "_build_video_caption", fake_caption)
    monkeypatch.setattr(poller_module, "run_story_publish_only_recovery", no_recovery)
    monkeypatch.setattr(poller_module, "_send_video_with_preview", noop_send_video)
    monkeypatch.setattr(poller_module, "_send_logs", noop_send_logs)

    bot = _DummyBot()
    await poller_module.run_kernel_poller(
        db,
        StoryFailureClient(),
        VideoAnnounceSession(id=session_id, status=VideoAnnounceSessionStatus.RENDERING, kaggle_kernel_ref="zigomaro/crumple-video"),
        bot=bot,
        notify_chat_id=777,
        test_chat_id=123,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=1,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.PUBLISH_BLOCKED
        assert refreshed.video_url == "crumple_video_final.mp4"
        assert refreshed.error == "story publish failed: @kenigevents (BOOSTS_REQUIRED)"
        assert refreshed.finished_at is not None


@pytest.mark.asyncio
async def test_error_kernel_with_rendered_story_failure_becomes_publish_blocked(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="kenigsberg_story",
            kaggle_kernel_ref="zigomaro/koenigsberg-stories",
            test_chat_id=123,
            selection_params={"target_date": "2026-06-12", "mode": "kenigsberg_story"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    class ErrorWithOutputClient(_CompleteKernelClient):
        def get_kernel_status(self, kernel_ref: str) -> dict:  # noqa: ARG002
            return {"status": "error", "failureMessage": "MEDIA_FILE_INVALID"}

        def download_kernel_output(self, kernel_ref: str, *, path: Path, **kwargs) -> list[str]:  # noqa: ARG002
            video = path / "koenigsberg_story_final.mp4"
            video.write_bytes(b"fake-video")
            report = path / "story_publish_report.json"
            report.write_text(
                '{"ok": false, "targets": [{"label": "@mostvkenig", "ok": false, "error": "MEDIA_FILE_INVALID"}]}',
                encoding="utf-8",
            )
            return [video.name, report.name]

    async def noop_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def fake_caption(*args, **kwargs):  # noqa: ANN002,ANN003
        return "Кёнигсберг #661"

    async def no_recovery(*args, **kwargs):  # noqa: ANN002,ANN003
        return False

    async def noop_send_video(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def noop_send_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", noop_status_message)
    monkeypatch.setattr(poller_module, "_build_video_caption", fake_caption)
    monkeypatch.setattr(poller_module, "run_story_publish_only_recovery", no_recovery)
    monkeypatch.setattr(poller_module, "_send_video_with_preview", noop_send_video)
    monkeypatch.setattr(poller_module, "_send_logs", noop_send_logs)

    bot = _DummyBot()
    await poller_module.run_kernel_poller(
        db,
        ErrorWithOutputClient(),
        VideoAnnounceSession(
            id=session_id,
            status=VideoAnnounceSessionStatus.RENDERING,
            kaggle_kernel_ref="zigomaro/koenigsberg-stories",
        ),
        bot=bot,
        notify_chat_id=777,
        test_chat_id=123,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=1,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.PUBLISH_BLOCKED
        assert refreshed.video_url == "koenigsberg_story_final.mp4"
        assert refreshed.error == "story publish failed: @mostvkenig (MEDIA_FILE_INVALID)"
        assert refreshed.finished_at is not None


@pytest.mark.asyncio
async def test_cancelled_kernel_does_not_probe_or_publish_output(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="popular_review",
            kaggle_kernel_ref="zigomaro/cherryflash",
            test_chat_id=123,
            selection_params={"target_date": "2026-06-18", "mode": "popular_review"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    class CancelledClient(_CompleteKernelClient):
        def get_kernel_status(self, kernel_ref: str) -> dict:  # noqa: ARG002
            return {"status": "CANCEL_ACKNOWLEDGED"}

        def download_kernel_output(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("cancelled operator run must not be output-recovered")

    async def noop_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def noop_download_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", noop_status_message)
    monkeypatch.setattr(poller_module, "_download_and_send_logs", noop_download_logs)

    bot = _DummyBot()
    await poller_module.run_kernel_poller(
        db,
        CancelledClient(),
        VideoAnnounceSession(id=session_id, status=VideoAnnounceSessionStatus.RENDERING, kaggle_kernel_ref="zigomaro/cherryflash"),
        bot=bot,
        notify_chat_id=777,
        test_chat_id=123,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=1,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.FAILED
        assert "CANCEL_ACKNOWLEDGED" in (refreshed.error or "")
    assert bot.messages == [
        (777, "❌ Сессия #1 отменена в Kaggle: cancel_acknowledged")
    ]


@pytest.mark.asyncio
async def test_error_status_with_fresh_notebook_heartbeat_keeps_polling(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="popular_review",
            kaggle_kernel_ref="zigomaro/cherryflash",
            test_chat_id=123,
            selection_params={"target_date": "2026-06-18", "mode": "popular_review"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

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
                f"videoannounce:{session_id}",
                session_id,
                json.dumps({"progress_label": "рендер 42%"}, ensure_ascii=False),
                (now - timedelta(minutes=2)).isoformat(),
                (now - timedelta(hours=1)).isoformat(),
                now.isoformat(),
            ),
        )
        await conn.commit()

    class ErrorThenCompleteClient(_CompleteKernelClient):
        def __init__(self) -> None:
            self.status_calls = 0

        def get_kernel_status(self, kernel_ref: str) -> dict:  # noqa: ARG002
            self.status_calls += 1
            if self.status_calls == 1:
                return {"status": "error", "failureMessage": "transient provider state"}
            return {"status": "complete"}

    status_notes: list[str | None] = []

    async def capture_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        status_notes.append(kwargs.get("note"))
        return None

    async def fake_caption(*args, **kwargs):  # noqa: ANN002,ANN003
        return "Видеоанонс #1 · 18 июня"

    async def noop_send_video(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def noop_send_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", capture_status_message)
    monkeypatch.setattr(poller_module, "_build_video_caption", fake_caption)
    monkeypatch.setattr(poller_module, "_send_video_with_preview", noop_send_video)
    monkeypatch.setattr(poller_module, "_send_logs", noop_send_logs)

    client = ErrorThenCompleteClient()
    await poller_module.run_kernel_poller(
        db,
        client,
        VideoAnnounceSession(id=session_id, status=VideoAnnounceSessionStatus.RENDERING, kaggle_kernel_ref="zigomaro/cherryflash"),
        bot=_DummyBot(),
        notify_chat_id=777,
        test_chat_id=123,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=1,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.PUBLISHED_TEST
        assert refreshed.video_url == "cherryflash_full_final.mp4"
        assert not refreshed.error
    assert client.status_calls == 2
    assert any(
        note and "notebook heartbeat свежий" in note
        for note in status_notes
    )


@pytest.mark.asyncio
async def test_complete_kernel_missing_video_with_story_preflight_failure_is_publish_blocked(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="default",
            kaggle_kernel_ref="zigomaro/crumple-video",
            test_chat_id=123,
            selection_params={"target_date": "2026-04-25", "mode": "tomorrow"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    class PreflightFailureNoVideoClient(_CompleteKernelClient):
        def download_kernel_output(self, kernel_ref: str, *, path: Path, **kwargs) -> list[str]:  # noqa: ARG002
            report = path / "story_publish_report.json"
            report.write_text(
                '{"ok": false, "targets": [{"label": "@kenigevents", "ok": false, "error": "BOOSTS_REQUIRED"}]}',
                encoding="utf-8",
            )
            log = path / "crumple.log"
            log.write_text("Story preflight failed: BOOSTS_REQUIRED", encoding="utf-8")
            return [report.name, log.name]

    async def noop_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def noop_send_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", noop_status_message)
    monkeypatch.setattr(poller_module, "_send_logs", noop_send_logs)

    bot = _DummyBot()
    await poller_module.run_kernel_poller(
        db,
        PreflightFailureNoVideoClient(),
        VideoAnnounceSession(id=session_id, status=VideoAnnounceSessionStatus.RENDERING, kaggle_kernel_ref="zigomaro/crumple-video"),
        bot=bot,
        notify_chat_id=777,
        test_chat_id=123,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=1,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.PUBLISH_BLOCKED
        assert refreshed.video_url is None
        assert refreshed.error == "story publish failed: @kenigevents (BOOSTS_REQUIRED)"
        assert refreshed.finished_at is not None


@pytest.mark.asyncio
async def test_unknown_status_probes_output_before_failed(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="popular_review",
            kaggle_kernel_ref="zigomaro/cherryflash",
            test_chat_id=123,
            selection_params={"target_date": "2026-06-08", "mode": "popular_review"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    class UnknownThenOutputClient(_CompleteKernelClient):
        def __init__(self) -> None:
            self.status_calls = 0

        def get_kernel_status(self, kernel_ref: str) -> dict:  # noqa: ARG002
            self.status_calls += 1
            return {"status": "UNKNOWN"}

    async def noop_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def fake_caption(*args, **kwargs):  # noqa: ANN002,ANN003
        return "Видеоанонс #1 · 8 июня"

    async def noop_send_video(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def noop_send_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", noop_status_message)
    monkeypatch.setattr(poller_module, "_build_video_caption", fake_caption)
    monkeypatch.setattr(poller_module, "_send_video_with_preview", noop_send_video)
    monkeypatch.setattr(poller_module, "_send_logs", noop_send_logs)

    client = UnknownThenOutputClient()
    await poller_module.run_kernel_poller(
        db,
        client,
        VideoAnnounceSession(id=session_id, status=VideoAnnounceSessionStatus.RENDERING, kaggle_kernel_ref="zigomaro/cherryflash"),
        bot=_DummyBot(),
        notify_chat_id=777,
        test_chat_id=123,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=1,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.PUBLISHED_TEST
        assert refreshed.video_url == "cherryflash_full_final.mp4"
    assert client.status_calls == 30


@pytest.mark.asyncio
async def test_timeout_probes_output_before_failed(
    monkeypatch, tmp_path: Path
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        sess = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.RENDERING,
            profile_key="popular_review",
            kaggle_kernel_ref="zigomaro/cherryflash",
            test_chat_id=123,
            selection_params={"target_date": "2026-06-18", "mode": "popular_review"},
        )
        session.add(sess)
        await session.commit()
        await session.refresh(sess)
        session_id = int(sess.id)

    async def noop_status_message(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def fake_caption(*args, **kwargs):  # noqa: ANN002,ANN003
        return "Видеоанонс #1 · 18 июня"

    async def noop_send_video(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    async def noop_send_logs(*args, **kwargs):  # noqa: ANN002,ANN003
        return None

    monkeypatch.setattr(poller_module, "update_status_message", noop_status_message)
    monkeypatch.setattr(poller_module, "_build_video_caption", fake_caption)
    monkeypatch.setattr(poller_module, "_send_video_with_preview", noop_send_video)
    monkeypatch.setattr(poller_module, "_send_logs", noop_send_logs)

    await poller_module.run_kernel_poller(
        db,
        _CompleteKernelClient(),
        VideoAnnounceSession(id=session_id, status=VideoAnnounceSessionStatus.RENDERING, kaggle_kernel_ref="zigomaro/cherryflash"),
        bot=_DummyBot(),
        notify_chat_id=777,
        test_chat_id=123,
        main_chat_id=None,
        poll_interval=0,
        timeout_minutes=0,
        download_dir=tmp_path,
    )

    async with db.get_session() as session:
        refreshed = await session.get(VideoAnnounceSession, session_id)
        assert refreshed is not None
        assert refreshed.status == VideoAnnounceSessionStatus.PUBLISHED_TEST
        assert refreshed.video_url == "cherryflash_full_final.mp4"
