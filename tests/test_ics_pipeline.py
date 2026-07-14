import asyncio
import time
import os
import pytest
from aiogram import Bot, types
import main
from main import Database, Event, Channel, JobOutbox, JobTask
from sqlalchemy import select


class DummyBot(Bot):
    def __init__(self, token: str):
        super().__init__(token)
        self.docs = []
        self.edits = []
        self.deleted = []

    async def send_document(self, chat_id, document, caption=None, **kwargs):
        self.docs.append((chat_id, caption, kwargs.get("parse_mode")))
        from types import SimpleNamespace
        chat = SimpleNamespace(id=chat_id)
        return SimpleNamespace(
            document=SimpleNamespace(file_id="file" + str(len(self.docs))),
            message_id=len(self.docs),
            chat=chat,
        )

    async def delete_message(self, chat_id, message_id, **kwargs):
        self.deleted.append((chat_id, message_id))
        return True

    async def edit_message_media(self, chat_id, message_id, media, **kwargs):
        self.edits.append((chat_id, message_id, media))
        from types import SimpleNamespace
        return SimpleNamespace(
            document=SimpleNamespace(file_id="edited" + str(len(self.edits))),
            message_id=message_id,
            chat=SimpleNamespace(id=chat_id),
        )


class FakeClient:
    def __init__(self):
        self.storage = self
        self.uploaded = []

    def from_(self, bucket):
        self.bucket = bucket
        return self

    def upload(self, path, content, options):
        self.uploaded.append((path, content, options))

    def get_public_url(self, path):
        return f"https://supabase/{path}"


class Progress:
    def __init__(self):
        self.marks = []

    def mark(self, key, status, detail):
        self.marks.append((key, status, detail))


@pytest.mark.asyncio
async def test_publish_ics_both_channels_success(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="Concert",
                description="desc",
                source_text="s",
                date="2025-07-18",
                time="19:00",
                location_name="Hall",
                city="Town",
            )
        )
        await session.commit()
    fake = FakeClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)
    called = {}
    async def fake_update(*a, **k):
        called["v"] = True
    monkeypatch.setattr(main, "update_source_page_ics", fake_update)
    monkeypatch.setattr(main, "update_source_post_keyboard", lambda *a, **k: None)
    await main.ics_publish(1, db, bot)
    await main.tg_ics_post(1, db, bot)
    assert fake.uploaded
    assert bot.docs
    assert called.get("v")
    async with db.get_session() as session:
        ev = await session.get(Event, 1)
        assert ev.ics_hash and ev.ics_url and ev.ics_file_id
        assert ev.ics_post_url and ev.ics_post_id


@pytest.mark.asyncio
async def test_tg_ics_post_stores_public_channel_url_when_asset_has_username(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(
            Channel(
                channel_id=-1002807919036,
                title="Calendar",
                username="kenigeventscalendar",
                is_admin=True,
                is_asset=True,
            )
        )
        session.add(
            Event(
                id=1,
                title="Concert",
                description="desc",
                source_text="s",
                date="2025-07-18",
                time="19:00",
                location_name="Hall",
                city="Town",
            )
        )
        await session.commit()
    monkeypatch.setattr(main, "update_source_post_keyboard", lambda *a, **k: None)

    await main.tg_ics_post(1, db, bot)

    async with db.get_session() as session:
        ev = await session.get(Event, 1)
        assert ev.ics_post_url == "https://t.me/kenigeventscalendar/1"


@pytest.mark.asyncio
async def test_ics_skips_when_no_change(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="A",
                description="d",
                source_text="s",
                date="2025-07-18",
                time="19:00",
                location_name="Hall",
                city="Town",
            )
        )
        await session.commit()
    fake = FakeClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)
    async def fake_update(*a, **k):
        pass
    monkeypatch.setattr(main, "update_source_page_ics", fake_update)
    monkeypatch.setattr(main, "update_source_post_keyboard", lambda *a, **k: None)
    await main.ics_publish(1, db, bot)
    await main.tg_ics_post(1, db, bot)
    fake.uploaded.clear()
    bot.docs.clear()
    pr = Progress()
    await main.ics_publish(1, db, bot, pr)
    assert not fake.uploaded
    assert ("ics_supabase", "skipped_nochange", "no change") in pr.marks
    pr2 = Progress()
    await main.tg_ics_post(1, db, bot, pr2)
    assert not bot.docs
    assert ("ics_telegram", "skipped_nochange", "no change") in pr2.marks


@pytest.mark.asyncio
async def test_tg_ics_post_updates_source_keyboard_on_skip(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="A",
                description="d",
                source_text="s",
                date="2025-07-18",
                time="19:00",
                location_name="Hall",
                city="Town",
                source_chat_id=1,
                source_message_id=2,
                ics_post_url="https://t.me/c/1/2",
            )
        )
        await session.commit()
    fake = FakeClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)
    async def fake_update_page(*a, **k):
        pass
    monkeypatch.setattr(main, "update_source_page_ics", fake_update_page)
    # Initial run to populate ics_hash and file id
    monkeypatch.setattr(main, "update_source_post_keyboard", lambda *a, **k: None)
    await main.ics_publish(1, db, bot)
    await main.tg_ics_post(1, db, bot)
    # second run should skip but still trigger keyboard update
    called = {}
    async def fake_update(*a, **k):
        called["v"] = True
    monkeypatch.setattr(main, "update_source_post_keyboard", fake_update)
    pr = Progress()
    await main.tg_ics_post(1, db, bot, pr)
    assert ("ics_telegram", "skipped_nochange", "no change") in pr.marks
    assert called.get("v")


@pytest.mark.asyncio
async def test_ics_updates_on_change(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="A",
                description="d",
                source_text="s",
                date="2025-07-18",
                time="19:00",
                location_name="Hall",
                city="Town",
            )
        )
        await session.commit()
    fake = FakeClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)
    async def fake_update(*a, **k):
        pass
    monkeypatch.setattr(main, "update_source_page_ics", fake_update)
    monkeypatch.setattr(main, "update_source_post_keyboard", lambda *a, **k: None)
    await main.ics_publish(1, db, bot)
    await main.tg_ics_post(1, db, bot)
    async with db.get_session() as session:
        ev = await session.get(Event, 1)
        h1, f1 = ev.ics_hash, ev.ics_file_id
        ev.date = "2025-07-19"
        await session.commit()
    await main.ics_publish(1, db, bot)
    await main.tg_ics_post(1, db, bot)
    async with db.get_session() as session:
        ev = await session.get(Event, 1)
        assert ev.ics_hash != h1
        assert ev.ics_file_id != f1
        assert ev.ics_post_hash == ev.ics_hash


@pytest.mark.asyncio
async def test_supabase_error_does_not_block_telegram(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="A",
                description="d",
                source_text="s",
                date="2025-07-18",
                time="19:00",
                location_name="Hall",
                city="Town",
            )
        )
        await session.commit()
    class BadClient(FakeClient):
        def upload(self, *a, **k):
            raise RuntimeError("fail")
    fake = BadClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)
    with pytest.raises(RuntimeError):
        await main.ics_publish(1, db, bot)
    await main.tg_ics_post(1, db, bot)
    assert bot.docs
    async with db.get_session() as session:
        ev = await session.get(Event, 1)
        assert ev.ics_file_id
        assert ev.ics_url is None


@pytest.mark.asyncio
async def test_telegram_error_does_not_block_supabase(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    class BadBot(DummyBot):
        async def send_document(self, *a, **k):
            raise RuntimeError("tg fail")
    bot = BadBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="A",
                description="d",
                source_text="s",
                date="2025-07-18",
                time="19:00",
                location_name="Hall",
                city="Town",
            )
        )
        await session.commit()
    fake = FakeClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)
    await main.ics_publish(1, db, bot)
    with pytest.raises(RuntimeError):
        await main.tg_ics_post(1, db, bot)
    assert fake.uploaded
    async with db.get_session() as session:
        ev = await session.get(Event, 1)
        assert ev.ics_url
        assert ev.ics_file_id is None


@pytest.mark.asyncio
async def test_ics_jobs_skip_invalid_schedule_without_retry(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="A",
                description="d",
                source_text="s",
                date="2025-07-18",
                time="по расписанию",
                location_name="Hall",
                city="Town",
            )
        )
        await session.commit()

    fake = FakeClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)

    pr = Progress()
    assert await main.ics_publish(1, db, bot, pr) is False
    assert not fake.uploaded
    assert len(pr.marks) == 1
    assert pr.marks[0][0] == "ics_supabase"
    assert pr.marks[0][1] == "skipped_invalid_schedule"
    assert "bad time" in pr.marks[0][2]
    assert "по расписанию" in pr.marks[0][2]

    pr2 = Progress()
    assert await main.tg_ics_post(1, db, bot, pr2) is False
    assert not bot.docs
    assert len(pr2.marks) == 1
    assert pr2.marks[0][0] == "ics_telegram"
    assert pr2.marks[0][1] == "skipped_invalid_schedule"
    assert "bad time" in pr2.marks[0][2]


@pytest.mark.asyncio
async def test_ics_jobs_remove_stale_projections_when_time_becomes_unknown(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add(
            Event(
                id=1,
                title="A",
                description="d",
                source_text="s",
                date="2026-08-08",
                time="",
                location_name="Hall",
                city="Town",
                ics_url=(
                    "https://example.supabase.co/storage/v1/object/public/"
                    "events-ics/event-1-2026-08-08.ics"
                ),
                ics_hash="old-hash",
                ics_file_id="old-file",
                ics_post_hash="old-hash",
                ics_post_url="https://t.me/kenigeventscalendar/77",
                ics_post_id=77,
                vk_ics_short_url="https://vk.cc/old",
                vk_ics_short_key="old",
            )
        )
        await session.commit()

    async def fake_update_keyboard(*args, **kwargs):
        return None

    monkeypatch.setattr(main, "update_source_post_keyboard", fake_update_keyboard)
    supabase_progress = Progress()
    assert await main.ics_publish(1, db, bot, supabase_progress) is True
    assert supabase_progress.marks == [
        ("ics_supabase", "removed_invalid_schedule", "bad time")
    ]

    telegram_progress = Progress()
    assert await main.tg_ics_post(1, db, bot, telegram_progress) is True
    assert telegram_progress.marks == [
        ("ics_telegram", "removed_invalid_schedule", "bad time")
    ]
    assert bot.deleted == [(-100, 77)]

    async with db.get_session() as session:
        ev = await session.get(Event, 1)
        assert ev.ics_url is None
        assert ev.ics_hash is None
        assert ev.ics_file_id is None
        assert ev.ics_post_hash is None
        assert ev.ics_post_url is None
        assert ev.ics_post_id is None
        assert ev.vk_ics_short_url is None
        assert ev.vk_ics_short_key is None
        queued = (
            await session.execute(
                main.text(
                    "SELECT bucket, path FROM supabase_delete_queue "
                    "WHERE path='event-1-2026-08-08.ics'"
                )
            )
        ).first()
    assert queued == ("events-ics", "event-1-2026-08-08.ics")
    await db.close()


@pytest.mark.asyncio
async def test_ics_coalesced_jobs_and_semaphore(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = DummyBot("123:abc")
    async with db.get_session() as session:
        session.add(Channel(channel_id=-100, title="Asset", is_admin=True, is_asset=True))
        session.add_all(
            [
                Event(
                    id=1,
                    title="A",
                    description="d",
                    source_text="s",
                    date="2025-07-18",
                    time="19:00",
                    location_name="Hall",
                    city="Town",
                ),
                Event(
                    id=2,
                    title="B",
                    description="d",
                    source_text="s",
                    date="2025-07-19",
                    time="19:00",
                    location_name="Hall",
                    city="Town",
                ),
            ]
        )
        await session.commit()
    fake = FakeClient()
    monkeypatch.setattr(main, "get_supabase_client", lambda: fake)
    order = []
    orig = main.build_ics_content
    async def fake_build(db_arg, ev_arg):
        order.append((ev_arg.id, time.perf_counter()))
        await asyncio.sleep(0.1)
        return await orig(db_arg, ev_arg)
    monkeypatch.setattr(main, "build_ics_content", fake_build)
    await asyncio.gather(
        main.ics_publish(1, db, bot),
        main.ics_publish(2, db, bot),
    )
    assert order[0][0] == 1 and order[1][0] == 2
    assert order[1][1] >= order[0][1]


@pytest.mark.asyncio
async def test_ics_upload_uses_direct_storage_endpoint_when_supabase_env_configured(monkeypatch):
    calls = []

    class Resp:
        status_code = 201
        text = '{"ok":true}'

    def fake_post(url, *, headers, data, timeout):
        calls.append((url, headers, data, timeout))
        return Resp()

    monkeypatch.setattr(main, "SUPABASE_URL", "https://example.supabase.co/rest/v1")
    monkeypatch.setattr(main, "SUPABASE_KEY", "service-key")
    monkeypatch.setattr(main, "SUPABASE_BUCKET", "events-ics")
    monkeypatch.setattr(main, "_normalized_supabase_url_source", None)
    monkeypatch.setattr(main, "_normalized_supabase_url", None)

    import requests

    monkeypatch.setattr(requests, "post", fake_post)

    url = await main._upload_ics_to_supabase_storage("event-1-2026-06-25.ics", b"BEGIN:VCALENDAR")

    assert url == "https://example.supabase.co/storage/v1/object/public/events-ics/event-1-2026-06-25.ics"
    assert calls
    upload_url, headers, body, timeout = calls[0]
    assert upload_url == "https://example.supabase.co/storage/v1/object/events-ics/event-1-2026-06-25.ics"
    assert headers["x-upsert"] == "true"
    assert headers["Content-Type"] == main.ICS_CONTENT_TYPE
    assert body == b"BEGIN:VCALENDAR"
    assert timeout == 45
