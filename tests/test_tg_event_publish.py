from types import SimpleNamespace

import pytest

import main


class DummyTgBot:
    def __init__(self):
        self.messages = []
        self.photos = []
        self.media_groups = []
        self.deleted = []
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

    async def delete_message(self, **kwargs):
        self.deleted.append(kwargs)


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
    assert "#Калининград" in text
    assert "#афишакалининград" in text
    assert "#концерт" in text
    assert "#20июня" in text
    assert "#Фестивальсвета" in text
    assert "#анонс" not in text
    assert '<a href="https://telegra.ph/event">Подробнее</a>' in text
    assert '<a href="https://t.me/+MrSeuZSHv3VjMThi">Подписаться</a>' in text
    assert '<a href="https://vk.com/klgdevents">Вконтакте</a>' in text


def test_tg_event_source_hash_includes_prompt_version(monkeypatch):
    event = _event()
    base_hash = main.build_tg_event_source_hash(event, "source text")

    monkeypatch.setattr(main, "TG_EVENT_REWRITE_PROMPT_VERSION", "tg-event-hook-test")

    assert main.build_tg_event_source_hash(event, "source text") != base_hash


def test_tg_event_source_hash_includes_media_signature():
    event = _event()
    base_hash = main.build_tg_event_source_hash(event, "source text")

    event.photo_urls = ["https://img.example/poster.webp"]

    assert main.build_tg_event_source_hash(event, "source text") != base_hash


def test_unique_tg_media_urls_dedupes_supabase_dhash_near_duplicates():
    base = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/0c/"
        "0c9a2cd38cb24672c27010e8004d884b84434062a0d264522cc29896985624c0.webp"
    )
    near_duplicate = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/0c/"
        "0c9a2cd38cb24672c27010f8004d884b84234062a0d264522cc298b6985624c0.webp"
    )
    distinct = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/89/"
        "892b6145103c3c3e2c0aae2a2b0b608921893489108c18281d241c380c5000d0.webp"
    )

    assert main._unique_tg_media_urls([base, near_duplicate, distinct, base]) == [
        base,
        distinct,
    ]


def test_unique_tg_media_urls_dedupes_borderline_supabase_dhash_duplicates():
    base = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/8c/"
        "8c22d06b5b245a25046594720972196274e373193230d963c70b520848490504.webp"
    )
    near_duplicate = (
        "https://storage.yandexcloud.net/kenigevents/p/dh16/89/"
        "8922d26b59245825046514720972196274e373193230d963c70b180048490504.webp"
    )

    assert main._unique_tg_media_urls([base, near_duplicate]) == [base]


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
    assert "#Калининград" in message_text
    assert "#афишакалининград" in message_text
    assert "#анонс" not in message_text
    button = message_kwargs["reply_markup"].inline_keyboard[0][0]
    assert button.text == "📅 20 июня 19:00 · Добавить в календарь"
    assert button.url == "https://t.me/c/asset/42"
    assert not bot.media_groups


@pytest.mark.asyncio
async def test_tg_event_publish_replaces_old_text_when_media_appears(monkeypatch):
    event = _event(
        photo_urls=["https://img.example/0.jpg"],
        tg_event_post_id=77,
        tg_event_post_mode="text",
        tg_event_post_url="https://t.me/c/1234567890/77",
        tg_event_source_hash="old",
    )
    long_text = " ".join(["Описание события."] * 20)

    async def fake_hook_text(event_arg, source_text):
        return "Что делает этот вечер особенным? Камерный формат."

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
    assert source_hash and source_hash != "old"
    assert len(bot.photos) == 1
    assert bot.deleted == [{"chat_id": "@kldevents", "message_id": 77}]


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
        tasks.append((task, kwargs.get("depends_on"), kwargs.get("next_run_at")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    deferred_at = main.datetime(2026, 6, 20, 5, 0, tzinfo=main.timezone.utc)
    monkeypatch.setattr(
        main,
        "next_tg_event_publish_run_at",
        lambda db_obj: main.asyncio.sleep(0, result=deferred_at),
    )
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(id=None)
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert (
        main.JobTask.tg_event_publish,
        [
            f"telegraph_build:{event.id}",
            f"tg_ics_post:{event.id}",
            f"vk_sync:{event.id}",
        ],
        deferred_at,
    ) in tasks
    assert (main.JobTask.vk_sync, None, None) in tasks


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_skips_calendar_dependency_without_time(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append((task, kwargs.get("depends_on"), kwargs.get("next_run_at")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    deferred_at = main.datetime(2026, 6, 20, 5, 0, tzinfo=main.timezone.utc)
    monkeypatch.setattr(
        main,
        "next_tg_event_publish_run_at",
        lambda db_obj: main.asyncio.sleep(0, result=deferred_at),
    )
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(id=None, time="")
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert not any(task == main.JobTask.tg_ics_post for task, _, _ in tasks)
    assert (
        main.JobTask.tg_event_publish,
        [
            f"telegraph_build:{event.id}",
            f"vk_sync:{event.id}",
        ],
        deferred_at,
    ) in tasks


@pytest.mark.asyncio
async def test_enqueue_tg_publish_replaces_stale_calendar_dependency(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    event = _event(id=None, time="")
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)
        session.add(
            main.JobOutbox(
                event_id=event_id,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                depends_on=f"telegraph_build:{event_id},tg_ics_post:{event_id},vk_sync:{event_id}",
                next_run_at=main.datetime(2026, 6, 20, 5, 0, tzinfo=main.timezone.utc),
                updated_at=main.datetime(2026, 6, 19, 5, 0, tzinfo=main.timezone.utc),
            )
        )
        await session.commit()

    result = await main.enqueue_job(
        db,
        event_id,
        main.JobTask.tg_event_publish,
        depends_on=[f"telegraph_build:{event_id}", f"vk_sync:{event_id}"],
        replace_depends_on=True,
        next_run_at=main.datetime(2026, 6, 20, 5, 10, tzinfo=main.timezone.utc),
    )

    async with db.get_session() as session:
        job = (
            await session.execute(
                main.select(main.JobOutbox).where(
                    main.JobOutbox.event_id == event_id,
                    main.JobOutbox.task == main.JobTask.tg_event_publish,
                )
            )
        ).scalar_one()

    assert result == "merged-rearmed"
    assert job.depends_on == f"telegraph_build:{event_id},vk_sync:{event_id}"


@pytest.mark.asyncio
async def test_schedule_event_update_tasks_requeues_deleted_managed_vk_post(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")

    async def fake_vk_api(method, **kwargs):
        assert method == "wall.getById"
        assert kwargs["posts"] == "-231920894_2432"
        return {"response": {"items": []}}

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append((task, kwargs.get("depends_on"), kwargs.get("next_run_at")))
        return f"{task.value}:job"

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)

    event = _event(
        id=None,
        source_vk_post_url="https://vk.com/wall-231920894_2432",
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event, skip_vk_sync=False)

    assert any(task == main.JobTask.vk_sync for task, _, _ in tasks)
    tg_publish = [item for item in tasks if item[0] == main.JobTask.tg_event_publish][0]
    assert f"vk_sync:{event.id}" in tg_publish[1]


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_defers_night_and_spaces_jobs(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")

    night_utc = main.datetime(2026, 6, 7, 0, 30, tzinfo=main.timezone.utc)
    first = await main.next_tg_event_publish_run_at(db, now=night_utc)
    assert first.astimezone(main.LOCAL_TZ).hour == 7
    assert first.astimezone(main.LOCAL_TZ).minute == 0

    async with db.get_session() as session:
        session.add(
            main.JobOutbox(
                event_id=42,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                updated_at=night_utc,
                next_run_at=first,
            )
        )
        await session.commit()

    second = await main.next_tg_event_publish_run_at(db, now=night_utc)
    assert second == first + main.timedelta(minutes=10)


@pytest.mark.asyncio
async def test_next_tg_event_publish_run_at_ignores_far_future_cancelled_backlog(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("TG_EVENT_PUBLISH_START_HOUR", "7")
    monkeypatch.setenv("TG_EVENT_PUBLISH_END_HOUR", "23")
    monkeypatch.setenv("TG_EVENT_PUBLISH_INTERVAL_MINUTES", "10")
    monkeypatch.setenv("TG_EVENT_PUBLISH_SPACING_HORIZON_HOURS", "24")

    now = main.datetime(2026, 6, 8, 10, 45, tzinfo=main.timezone.utc)
    far_future = now + main.timedelta(days=3650)
    async with db.get_session() as session:
        session.add(
            main.JobOutbox(
                event_id=42,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.error,
                last_error="cancelled_bad_dependency_key_after_fanout_fix",
                updated_at=now,
                next_run_at=far_future,
            )
        )
        session.add(
            main.JobOutbox(
                event_id=43,
                task=main.JobTask.tg_event_publish,
                status=main.JobStatus.pending,
                updated_at=now,
                next_run_at=far_future,
            )
        )
        await session.commit()

    scheduled = await main.next_tg_event_publish_run_at(db, now=now)
    assert scheduled == main._normalize_tg_event_publish_run_at(now)


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
