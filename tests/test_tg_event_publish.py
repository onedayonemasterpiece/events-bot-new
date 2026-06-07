from types import SimpleNamespace

import pytest

import main


class DummyTgBot:
    def __init__(self):
        self.messages = []
        self.photos = []
        self.media_groups = []
        self._next_id = 100

    async def send_message(self, chat_id, text, **kwargs):
        self.messages.append((chat_id, text, kwargs))
        self._next_id += 1
        return SimpleNamespace(
            message_id=self._next_id,
            chat=SimpleNamespace(id=-1001234567890),
        )

    async def send_photo(self, chat_id, photo, **kwargs):
        self.photos.append((chat_id, photo, kwargs))
        self._next_id += 1
        return SimpleNamespace(
            message_id=self._next_id,
            chat=SimpleNamespace(id=-1001234567890),
        )

    async def send_media_group(self, chat_id, media):
        self.media_groups.append((chat_id, media))
        sent = []
        for _item in media:
            self._next_id += 1
            sent.append(
                SimpleNamespace(
                    message_id=self._next_id,
                    chat=SimpleNamespace(id=-1001234567890),
                )
            )
        return sent

    async def edit_message_text(self, **_kwargs):
        raise AssertionError("edit should not be called in first publish")

    async def edit_message_caption(self, **_kwargs):
        raise AssertionError("edit should not be called in first publish")


def _event(**kwargs) -> main.Event:
    data = {
        "id": 42,
        "title": "Камерный концерт",
        "description": "",
        "date": "2026-06-20",
        "time": "19:00",
        "location_name": "Дом искусств",
        "location_address": "Ленинский проспект 155",
        "city": "Калининград",
        "source_text": "source",
        "ticket_link": "https://example.com/tickets",
        "ticket_price_min": 500,
        "telegraph_url": "https://telegra.ph/event",
        "photo_urls": [],
    }
    data.update(kwargs)
    return main.Event(**data)


def test_build_tg_event_announcement_formats_links_hashtags_and_footer():
    event = _event(festival="Фестиваль света")

    text = main.build_tg_event_announcement(
        event,
        "Небольшое описание с **важной деталью**.",
    )

    assert text.startswith("<b>Камерный концерт</b>")
    assert "КАМЕРНЫЙ КОНЦЕРТ" not in text
    assert '<a href="https://example.com/tickets">Билеты</a>' in text
    assert "Калининград" in text
    assert "#Калининград" not in text
    assert "#20июня" in text
    assert "#Фестивальсвета" in text
    assert "#анонс" not in text
    assert '<a href="https://telegra.ph/event">Подробнее</a>' in text
    assert '<a href="https://t.me/kldevents">Подписаться</a>' in text
    assert '<a href="https://vk.com/klgdevents">Вконтакте</a>' in text


def test_tg_event_source_hash_includes_prompt_version(monkeypatch):
    event = _event()
    base_hash = main.build_tg_event_source_hash(event, "source text")

    monkeypatch.setattr(main, "TG_EVENT_REWRITE_PROMPT_VERSION", "tg-event-hook-test")

    assert main.build_tg_event_source_hash(event, "source text") != base_hash


@pytest.mark.asyncio
async def test_tg_event_publish_sends_single_photo_caption_with_calendar_button(monkeypatch):
    event = _event(
        photo_urls=["https://img.example/0.jpg"],
        ics_url="https://example.com/event.ics",
        ics_post_url="https://t.me/c/asset/42",
    )
    long_text = " ".join(["Очень длинное описание события."] * 120)

    async def fake_hook_text(event_arg, source_text):
        assert event_arg is event
        assert source_text == long_text
        return "Что делает этот вечер особенным? Музыка прозвучит в камерном формате."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        long_text,
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "photo_caption"
    assert source_hash
    assert not bot.messages
    assert len(bot.photos) == 1
    assert bot.photos[0][1] == "https://img.example/0.jpg"
    message_text = bot.photos[0][2]["caption"]
    message_kwargs = bot.photos[0][2]
    assert "Что делает этот вечер особенным?" in message_text
    assert main._tg_html_visible_len(message_text) <= 1000
    assert "Подробнее" in message_text
    assert "Подписаться" in message_text
    assert "#20июня" in message_text
    assert "#анонс" not in message_text
    button = message_kwargs["reply_markup"].inline_keyboard[0][0]
    assert button.text == "📅 20 июня 19:00 · Добавить в календарь"
    assert button.url == "https://t.me/c/asset/42"
    assert not bot.media_groups


@pytest.mark.asyncio
async def test_tg_event_publish_sends_album_caption_for_multiple_media(monkeypatch):
    event = _event(
        photo_urls=[f"https://img.example/{idx}.jpg" for idx in range(12)],
        ics_post_url="https://t.me/c/asset/42",
    )
    long_text = " ".join(["Очень длинное описание события."] * 120)

    async def fake_hook_text(event_arg, source_text):
        assert event_arg is event
        assert source_text == long_text
        return "Что делает этот вечер особенным? Музыка прозвучит в камерном формате."

    monkeypatch.setattr(main, "build_tg_event_hook_text", fake_hook_text)
    bot = DummyTgBot()

    url, post_id, mode, source_hash = await main.publish_tg_event_announcement(
        event,
        long_text,
        None,
        bot,
    )

    assert url == "https://t.me/c/1234567890/101"
    assert post_id == 101
    assert mode == "album_caption"
    assert source_hash
    assert not bot.messages
    assert not bot.photos
    assert len(bot.media_groups) == 2
    assert len(bot.media_groups[0][1]) == 10
    assert len(bot.media_groups[1][1]) == 2
    assert [item.media for item in bot.media_groups[0][1]][0] == "https://img.example/0.jpg"
    assert "Что делает этот вечер особенным?" in bot.media_groups[0][1][0].caption
    assert "Добавить в календарь" in bot.media_groups[0][1][0].caption


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_enqueues_tg_publish(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append((task, kwargs.get("depends_on")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(id=None)
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert (
        main.JobTask.tg_event_publish,
        ["telegraph_build:job", "tg_ics_post:job"],
    ) in tasks


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_tg_publish_for_past(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(
        id=None,
        date=(main.datetime.now(main.LOCAL_TZ).date() - main.timedelta(days=1)).isoformat(),
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert main.JobTask.tg_event_publish not in tasks
