from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

import guide_excursions.service as guide_service


def test_covered_occurrence_ids_only_include_published_clusters():
    rows = [
        {"id": 101, "canonical_title": "Опубликованная карточка"},
        {"id": 303, "canonical_title": "Вторая опубликованная карточка"},
    ]
    coverage = {
        101: [101, 202],
        303: [303],
        404: [404, 405],
    }

    covered = guide_service._covered_occurrence_ids_for_published_rows(
        rows,
        coverage_by_display_id=coverage,
    )

    assert covered == [101, 202, 303]


@pytest.mark.asyncio
async def test_publish_guide_digest_uses_single_album_caption_when_text_fits(tmp_path, monkeypatch):
    media_path = tmp_path / "guide-photo.jpg"
    media_path.write_bytes(b"fake-image")

    preview = {
        "issue_id": 77,
        "texts": [
            "Новые экскурсии гидов: 1 находка на ближайшие дни\n\n"
            '1. <a href="https://t.me/source/1">Каштаны в Калининграде</a>'
        ],
        "items": [{"id": 101, "date": "2026-04-19"}],
        "media_items": [
            {
                "occurrence_id": 101,
                "media_asset": {"path": str(media_path), "kind": "photo"},
                "media_ref": {"kind": "photo"},
            }
        ],
        "covered_occurrence_ids": [101],
    }

    async def fake_build_preview(db, *, family, limit=24, run_id=None):
        del db, family, limit, run_id
        return preview

    monkeypatch.setattr(guide_service, "build_guide_digest_preview", fake_build_preview)

    class _FakeConn:
        def __init__(self) -> None:
            self.row_factory = None
            self.executed: list[tuple[str, tuple[object, ...]]] = []

        async def execute(self, sql, params=()):
            self.executed.append((sql, tuple(params)))
            return None

        async def commit(self):
            return None

    class _FakeDB:
        def __init__(self) -> None:
            self.conn = _FakeConn()

        @asynccontextmanager
        async def raw_conn(self):
            yield self.conn

    class _Message:
        def __init__(self, message_id: int) -> None:
            self.message_id = message_id

    class _DummyBot:
        def __init__(self) -> None:
            self.media_calls: list[tuple[str, list[object]]] = []
            self.message_calls: list[tuple[str, str]] = []
            self.caption_edits: list[tuple[str, int, str]] = []

        async def send_media_group(self, chat_id, media):
            self.media_calls.append((chat_id, list(media)))
            return [_Message(501 + idx) for idx, _item in enumerate(media)]

        async def send_message(self, chat_id, text, **kwargs):
            del kwargs
            self.message_calls.append((chat_id, text))
            return _Message(900 + len(self.message_calls))

        async def edit_message_caption(self, chat_id, message_id, caption, **kwargs):
            del kwargs
            self.caption_edits.append((chat_id, message_id, caption))
            return None

    db = _FakeDB()
    bot = _DummyBot()

    result = await guide_service.publish_guide_digest(
        db,
        bot,
        family="new_occurrences",
        chat_id=None,
        target_chat="@digest_target",
    )

    assert len(bot.media_calls) == 1
    sent_media = bot.media_calls[0][1]
    assert len(sent_media) == 1
    assert sent_media[0].caption == preview["texts"][0]
    assert getattr(sent_media[0], "parse_mode", None) == "HTML"
    assert bot.message_calls == []
    assert bot.caption_edits == []
    assert result["media_message_ids"] == [501]
    assert result["text_message_ids"] == []
    assert result["message_ids"] == [501]

    occurrence_updates = [
        params
        for sql, params in db.conn.executed
        if "UPDATE guide_occurrence SET published_new_digest_issue_id" in sql
    ]
    assert occurrence_updates == [(77, 101)]
