import os
import sys
from types import SimpleNamespace

import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from db import Database
from google_ai.exceptions import RateLimitError

import vk_auto_queue


class DummyBot:
    async def send_message(self, *args, **kwargs):  # pragma: no cover - progress disabled in tests
        return None

    async def get_me(self):
        class Me:
            username = "eventsbotTestBot"

        return Me()


@pytest.mark.asyncio
async def test_vk_auto_queue_rate_limit_marks_row_deferred_for_next_batch(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, status, locked_by, locked_at, review_batch, attempts)
            VALUES(?, ?, ?, ?, ?, NULL, 1, 'locked', 777, CURRENT_TIMESTAMP, 'batch-x', 0)
            """,
            (1, 123, 456, 0, "cached text"),
        )
        await conn.commit()

    async def fake_fetch_vk_post_text_and_photos(_group_id, _post_id, *, db, bot, limit):  # noqa: ARG001
        return "text", [], None, {"views": 10, "likes": 1}, vk_auto_queue.VkFetchStatus(True, "ok")

    async def fake_build_event_drafts(*_args, **_kwargs):
        raise RateLimitError(blocked_reason="tpm", retry_after_ms=3000)

    async def noop_sleep(_sec):  # noqa: ARG001
        return None

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch_vk_post_text_and_photos)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_auto_queue.asyncio, "sleep", noop_sleep)
    monkeypatch.setenv("VK_AUTO_IMPORT_RATE_LIMIT_MAX_WAIT_SEC", "5")
    monkeypatch.setenv("VK_AUTO_IMPORT_RATE_LIMIT_MAX_DEFERS", "3")

    report = vk_auto_queue.VkAutoImportReport(batch_id="batch-x")
    post = SimpleNamespace(
        id=1,
        group_id=123,
        post_id=456,
        date=0,
        text="cached text",
        event_ts_hint=None,
    )

    await vk_auto_queue._process_vk_inbox_row(  # type: ignore[attr-defined]
        db,
        DummyBot(),
        chat_id=1,
        operator_id=1,
        batch_id="batch-x",
        post=post,
        source_url="https://vk.com/wall-123_456",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )

    assert report.inbox_deferred == 1
    assert report.inbox_failed == 0

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, locked_by, review_batch, locked_at, attempts FROM vk_inbox WHERE id=?",
            (1,),
        )
        status, locked_by, review_batch, locked_at, attempts = await cur.fetchone()
    assert status == "deferred"
    assert locked_by is None
    assert review_batch == "batch-x"
    assert locked_at is not None
    assert attempts == 1


@pytest.mark.asyncio
async def test_vk_auto_queue_rate_limit_marks_row_failed_after_max_defers(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, status, locked_by, locked_at, review_batch, attempts)
            VALUES(?, ?, ?, ?, ?, NULL, 1, 'locked', 777, CURRENT_TIMESTAMP, 'batch-x', 2)
            """,
            (1, 123, 456, 0, "cached text"),
        )
        await conn.commit()

    async def fake_fetch_vk_post_text_and_photos(_group_id, _post_id, *, db, bot, limit):  # noqa: ARG001
        return "text", [], None, {"views": 10, "likes": 1}, vk_auto_queue.VkFetchStatus(True, "ok")

    async def fake_build_event_drafts(*_args, **_kwargs):
        raise RateLimitError(blocked_reason="tpm", retry_after_ms=3000)

    async def noop_sleep(_sec):  # noqa: ARG001
        return None

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch_vk_post_text_and_photos)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_auto_queue.asyncio, "sleep", noop_sleep)
    monkeypatch.setenv("VK_AUTO_IMPORT_RATE_LIMIT_MAX_WAIT_SEC", "5")
    monkeypatch.setenv("VK_AUTO_IMPORT_RATE_LIMIT_MAX_DEFERS", "3")

    report = vk_auto_queue.VkAutoImportReport(batch_id="batch-x")
    post = SimpleNamespace(
        id=1,
        group_id=123,
        post_id=456,
        date=0,
        text="cached text",
        event_ts_hint=None,
    )

    await vk_auto_queue._process_vk_inbox_row(  # type: ignore[attr-defined]
        db,
        DummyBot(),
        chat_id=1,
        operator_id=1,
        batch_id="batch-x",
        post=post,
        source_url="https://vk.com/wall-123_456",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )

    assert report.inbox_deferred == 0
    assert report.inbox_failed == 1
    assert any("drafts_rate_limited_terminal" in err for err in report.errors)

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, locked_by, locked_at, review_batch, attempts FROM vk_inbox WHERE id=?",
            (1,),
        )
        status, locked_by, locked_at, review_batch, attempts = await cur.fetchone()

    assert status == "failed"
    assert locked_by is None
    assert locked_at is None
    assert review_batch is None
    assert attempts == 3
