import pytest
from pathlib import Path
import json
import logging
import sqlite3
from datetime import datetime, timezone
import main


@pytest.mark.asyncio
async def test_edit_vk_post_treats_expired_edit_window_as_terminal(monkeypatch):
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "_vk_user_token", lambda: "token")
    calls = []

    async def fake_vk_api(method, params, *args, **kwargs):
        calls.append(method)
        if method == "wall.getById":
            return {
                "response": [
                    {
                        "text": "old",
                        "date": int(datetime.now(timezone.utc).timestamp()),
                        "can_edit": 1,
                        "attachments": [],
                    }
                ]
            }
        raise main.VKAPIError(
            15,
            "Access denied: edit time expired",
            method="wall.edit",
        )

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    result = await main.edit_vk_post(
        "https://vk.com/wall-231920894_6648",
        "new",
    )

    assert result is None
    assert calls == ["wall.getById", "wall.edit"]


def _expected_vk_source_hash(event, text):
    return main.content_hash(
        "\n".join(
            [
                main.VK_SOURCE_POST_FORMAT_VERSION,
                str(event.title or ""),
                str(event.date or ""),
                str(event.time or ""),
                str(event.location_name or ""),
                str(event.location_address or ""),
                str(event.city or ""),
                str(event.ticket_link or ""),
                f"ics:{str(event.ics_url or '<none>')}",
                json.dumps(list(event.photo_urls or []), ensure_ascii=False),
                text,
            ]
        )
    )


@pytest.fixture(autouse=True)
def _sync_event_updates(monkeypatch):
    monkeypatch.setenv("EVENT_UPDATE_SYNC", "1")
    async def fake_month(db_obj, month):
        return None
    async def fake_weekend(db_obj, start):
        return None
    monkeypatch.setattr(main, "sync_month_page", fake_month)
    monkeypatch.setattr(main, "sync_weekend_page", fake_weekend)


@pytest.mark.asyncio
async def test_vk_photos_enabled_defaults_on_until_explicitly_disabled(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED_DEFAULT", True)

    assert await main.get_vk_photos_enabled(db) is True

    await main.set_vk_photos_enabled(db, False)

    assert await main.get_vk_photos_enabled(db) is False


@pytest.mark.asyncio
async def test_vk_wall_source_still_gets_event_vk_sync(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    future_date = (main.datetime.now(main.LOCAL_TZ).date() + main.timedelta(days=10)).isoformat()

    async with db.get_session() as session:
        event = main.Event(
            title="T",
            description="",
            date=future_date,
            time="10:00",
            location_name="Place",
            source_text="T",
            source_post_url="https://vk.com/wall-1_2",
            # Imported VK events also persist the source URL in source_vk_post_url —
            # vk_sync must still be scheduled to publish a managed klgdevents post.
            source_vk_post_url="https://vk.com/wall-1_2",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event)

    assert main.JobTask.vk_sync in tasks


@pytest.mark.asyncio
async def test_past_vk_wall_source_skips_event_vk_sync(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    past_date = (main.datetime.now(main.LOCAL_TZ).date() - main.timedelta(days=1)).isoformat()

    async with db.get_session() as session:
        event = main.Event(
            title="Past",
            description="",
            date=past_date,
            time="10:00",
            location_name="Place",
            source_text="Past",
            source_post_url="https://vk.com/wall-1_2",
            source_vk_post_url="https://vk.com/wall-1_2",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event)

    assert main.JobTask.vk_sync not in tasks


@pytest.mark.asyncio
async def test_ongoing_vk_wall_source_still_gets_event_vk_sync(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    today = main.datetime.now(main.LOCAL_TZ).date()

    async with db.get_session() as session:
        event = main.Event(
            title="Ongoing",
            description="",
            date=(today - main.timedelta(days=5)).isoformat(),
            end_date=(today + main.timedelta(days=1)).isoformat(),
            time="10:00",
            location_name="Place",
            source_text="Ongoing",
            source_post_url="https://vk.com/wall-1_2",
            source_vk_post_url="https://vk.com/wall-1_2",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event)

    assert main.JobTask.vk_sync in tasks


@pytest.mark.asyncio
async def test_managed_klgdevents_event_skips_vk_sync(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    tasks = []

    async def fake_enqueue_job(db_obj, eid, task, **kwargs):
        tasks.append(task)
        return "job"

    monkeypatch.setattr(main, "enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")

    async with db.get_session() as session:
        event = main.Event(
            title="T",
            description="",
            date="2026-06-01",
            time="10:00",
            location_name="Place",
            source_text="T",
            source_vk_post_url="https://vk.com/wall-231920894_981",
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    await main.schedule_event_update_tasks(db, event)

    assert main.JobTask.vk_sync not in tasks


@pytest.mark.asyncio
async def test_sync_vk_source_post_includes_calendar_link(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"

    event = main.Event(
        title="Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
    )

    captured_message = {}

    async def fake_post_to_vk(
        group_id, message, db=None, bot=None, attachments=None, **_kwargs
    ):
        captured_message["text"] = message
        return "https://vk.com/wall-1_2"

    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)

    calls = []

    async def fake_vk_api(method, params, db=None, bot=None, token=None):
        calls.append((method, params))
        if method == "utils.getShortLink":
            return {"response": {"short_url": "https://vk.cc/abcd", "key": "abcd"}}
        return {"response": {}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)

    url = await main.sync_vk_source_post(
        event,
        "Title\nDescription",
        None,
        None,
        ics_url="http://ics",
    )

    assert url == "https://vk.com/wall-1_2"
    assert not any(method == "wall.createComment" for method, _ in calls)
    lines = captured_message["text"].splitlines()
    assert lines[0] == "Title"
    assert lines[1] == main.VK_BLANK_LINE
    assert "Добавить в календарь vk.cc/abcd" in captured_message["text"]
    assert captured_message["text"].endswith(main.VK_SOURCE_FOOTER)


@pytest.mark.asyncio
async def test_sync_vk_source_post_uses_original_calendar_link_on_short_fail(
    monkeypatch,
):
    main.VK_AFISHA_GROUP_ID = "1"

    event = main.Event(
        title="Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
    )

    captured_message: dict[str, str] = {}

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        captured_message["text"] = message
        return "https://vk.com/wall-1_2"

    async def fake_vk_api(method, params, db=None, bot=None, token=None):
        if method == "utils.getShortLink":
            raise RuntimeError("fail")
        return {"response": {}}

    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)

    url = await main.sync_vk_source_post(
        event,
        "Title\nDescription",
        None,
        None,
        ics_url="http://ics",
    )

    assert url == "https://vk.com/wall-1_2"
    assert "Добавить в календарь http://ics" in captured_message["text"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_refreshes_ics_short_link(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"

    event = main.Event(
        title="Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
        ics_url="http://old",
        vk_ics_short_url="https://vk.cc/old",
        vk_ics_short_key="old",
    )

    captured: dict[str, str] = {}

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        captured["text"] = message
        return "https://vk.com/wall-1_2"

    calls: list[tuple[str, dict[str, str]]] = []

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        calls.append((method, params))
        if method == "utils.getShortLink":
            return {"response": {"short_url": "https://vk.cc/newkey", "key": "newkey"}}
        return {"response": {}}

    monkeypatch.setattr(main, "post_to_vk", fake_post)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)

    url = await main.sync_vk_source_post(
        event,
        "Title\nDescription",
        None,
        None,
        ics_url="http://new",
    )

    assert url == "https://vk.com/wall-1_2"
    assert any(method == "utils.getShortLink" for method, _ in calls)
    assert event.ics_url == "http://new"
    assert event.vk_ics_short_url == "https://vk.cc/newkey"
    assert event.vk_ics_short_key == "newkey"
    assert "Добавить в календарь vk.cc/newkey" in captured["text"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_attaches_photos(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_TOKEN_AFISHA = "ga"
    main.VK_MAX_ATTACHMENTS = 1

    event = main.Event(
        title="Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
        photo_urls=["http://img1", "http://img2"],
    )

    uploaded: list[tuple[str, str]] = []

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        uploaded.append((url, token))
        return f"ph{url[-1]}"

    posted: dict[str, list[str] | None] = {}

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        posted["vals"] = attachments
        return "https://vk.com/wall-1_2"

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post)

    url = await main.sync_vk_source_post(event, "Text", None, None)

    assert url == "https://vk.com/wall-1_2"
    assert uploaded == [("http://img1", None)]
    assert posted["vals"] == ["ph1"]


@pytest.mark.asyncio
async def test_upload_vk_photo_retries_upload_server_request(monkeypatch):
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")
    monkeypatch.setattr(
        main,
        "choose_vk_actor",
        lambda owner_id, intent: [main.VkActor("user", "user-token", "user:test")],
    )
    monkeypatch.setattr(main, "detect_image_type", lambda data: "png")
    monkeypatch.setattr(main, "ensure_jpeg", lambda data, name: (data, name))

    async def fake_sleep(_delay):
        return None

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)

    class FakeContent:
        async def iter_chunked(self, _size):
            yield b"image-bytes"

    class FakeDownloadResponse:
        headers = {"Content-Length": "11"}
        content_length = 11
        content = FakeContent()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

    class FakeUploadResponse:
        def __init__(self, attempt):
            self.attempt = attempt

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            if self.attempt == 1:
                raise ValueError("unexpected mimetype text/html")
            return {"photo": "photo-json", "server": 7, "hash": "hash-json"}

    upload_urls: list[str] = []

    class FakeSession:
        def get(self, _url):
            return FakeDownloadResponse()

        def post(self, upload_url, data=None):
            upload_urls.append(upload_url)
            return FakeUploadResponse(len(upload_urls))

    monkeypatch.setattr(main, "get_http_session", lambda: FakeSession())

    upload_server_calls = 0
    save_params: dict[str, object] = {}

    async def fake_vk_api(method, params=None, *args, **kwargs):
        nonlocal upload_server_calls
        if method == "photos.getWallUploadServer":
            upload_server_calls += 1
            return {"response": {"upload_url": f"https://upload/{upload_server_calls}"}}
        if method == "photos.saveWallPhoto":
            save_params.update(params or {})
            return {"response": [{"owner_id": -1, "id": 42}]}
        raise AssertionError(method)

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    result = await main.upload_vk_photo("1", "https://storage.example/photo.webp")

    assert result == "photo-1_42"
    assert upload_urls == ["https://upload/1", "https://upload/2"]
    assert save_params["photo"] == "photo-json"


@pytest.mark.asyncio
async def test_sync_vk_source_post_passes_vk_coauthor_candidate(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = False

    event = main.Event(
        title="Лекция Музея Советского детства БФУ",
        description="",
        date="2026-06-03",
        time="18:00",
        location_name="Музей Советского детства БФУ",
        city="Калининград",
        source_text="https://vk.com/prodetstvosu",
    )

    posted = {}

    async def fake_post(
        group_id,
        message,
        db=None,
        bot=None,
        attachments=None,
        **kwargs,
    ):
        posted.update(kwargs)
        return "https://vk.com/wall-1_2"

    monkeypatch.setattr(main, "post_to_vk", fake_post)

    url = await main.sync_vk_source_post(event, "Text", None, None)

    assert url == "https://vk.com/wall-1_2"
    assert posted["vk_coauthor_url"] == "https://vk.com/prodetstvosu"
    assert posted["vk_coauthor_screen_name"] == "prodetstvosu"


@pytest.mark.asyncio
async def test_post_to_vk_sends_coauthor_params_and_retries_without_rejected_params(monkeypatch):
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "group-token")
    monkeypatch.setattr(main, "VK_USER_TOKEN", "")

    async def fake_reserve(*_args, **_kwargs):
        return None

    monkeypatch.setattr(main, "_reserve_vk_postponed_publish_date", fake_reserve)

    calls = []

    async def fake_vk_api(method, params=None, *_, **__):
        calls.append((method, dict(params or {})))
        if method == "groups.getById":
            return {"response": [{"id": 30777579}]}
        if len([m for m, _params in calls if m == "wall.post"]) == 1:
            raise main.VKAPIError(100, "One of the parameters specified was missing or invalid")
        return {"response": {"post_id": 2395}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.post_to_vk(
        "1",
        "Message",
        vk_coauthor_url="https://vk.com/konb39",
        vk_coauthor_screen_name="konb39",
    )

    wall_calls = [params for method, params in calls if method == "wall.post"]
    assert url == "https://vk.com/wall-1_2395"
    assert wall_calls[0]["copyright"] == "https://vk.com/konb39"
    assert wall_calls[0]["coauthors"] == "-30777579"
    assert wall_calls[0]["coauthor_ids"] == "-30777579"
    assert "copyright" not in wall_calls[1]
    assert "coauthors" not in wall_calls[1]
    assert "coauthor_ids" not in wall_calls[1]


@pytest.mark.asyncio
async def test_vk_postponed_reservation_spreads_same_source_afisha(monkeypatch):
    monkeypatch.setattr(main, "VK_POSTPONED_ENABLED", True)
    monkeypatch.setattr(main, "VK_POSTPONED_TZ", "UTC")
    monkeypatch.setattr(main, "VK_POSTPONED_START_HOUR", 0)
    monkeypatch.setattr(main, "VK_POSTPONED_MIN_INTERVAL_SECONDS", 600)
    monkeypatch.setenv("SAME_SOURCE_EVENT_PUBLISH_INTERVAL_HOURS", "12")
    main._vk_postponed_reserved_until_by_owner.clear()
    main._vk_postponed_reserved_until_by_source.clear()

    async def fake_fetch_anchors(*_args, **_kwargs):
        return []

    monkeypatch.setattr(main, "_fetch_vk_postponed_anchor_timestamps", fake_fetch_anchors)
    actors = [main.VkActor("group", "token", "group:test")]
    now = main.datetime(2026, 6, 14, 8, 0, tzinfo=main.timezone.utc)
    source_url = "https://vk.com/wall-194927034_4698"

    first = await main._reserve_vk_postponed_publish_date(
        -231920894,
        actors,
        None,
        None,
        now=now,
        source_url=source_url,
        event_id=1,
    )
    second = await main._reserve_vk_postponed_publish_date(
        -231920894,
        actors,
        None,
        None,
        now=now,
        source_url=source_url,
        event_id=2,
    )

    assert first == int((now + main.timedelta(minutes=10)).timestamp())
    assert second == first + 12 * 3600
    main._vk_postponed_reserved_until_by_owner.clear()
    main._vk_postponed_reserved_until_by_source.clear()


@pytest.mark.asyncio
async def test_sync_vk_source_post_captcha_pauses_before_text_only_post(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_MAX_ATTACHMENTS = 1

    event = main.Event(
        title="Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
        photo_urls=["http://img1"],
    )

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        raise main.VKAPIError(14, "Captcha needed", method="photos.getWallUploadServer")

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        raise AssertionError("wall.post must wait for captcha instead of publishing text-only")

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post)

    with pytest.raises(main.VKAPIError) as exc:
        await main.sync_vk_source_post(event, "Text", None, None)

    assert exc.value.code == 14


@pytest.mark.asyncio
async def test_sync_vk_source_post_blocks_text_only_telegram_event(monkeypatch):
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "")
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.delenv("VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS", raising=False)

    event = main.Event(
        id=5640,
        title="Title",
        description="",
        date="2026-06-06",
        time="18:00",
        location_name="Place",
        source_text="Text",
        source_post_url="https://t.me/k_mira101/424",
        photo_urls=[],
    )

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        raise AssertionError("telegram-origin vk_sync must not publish without media")

    monkeypatch.setattr(main, "post_to_vk", fake_post)

    with pytest.raises(RuntimeError, match="vk_sync_missing_media_for_telegram_event"):
        await main.sync_vk_source_post(event, "Text", None, None)


@pytest.mark.asyncio
async def test_sync_vk_source_post_allows_vk_origin_with_source_ids(monkeypatch):
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "")
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.delenv("VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS", raising=False)

    event = main.Event(
        id=5779,
        title="Title",
        description="",
        date="2026-06-08",
        time="18:30",
        location_name="Place",
        source_text="Text",
        source_post_url="https://vk.com/wall-30777579_15383",
        source_chat_id=30777579,
        source_message_id=15383,
        photo_urls=[],
    )

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        return "https://vk.com/wall-1_2"

    monkeypatch.setattr(main, "post_to_vk", fake_post)

    assert await main.sync_vk_source_post(event, "Text", None, None) == "https://vk.com/wall-1_2"


@pytest.mark.asyncio
async def test_sync_vk_source_post_blocks_vk_origin_when_available_media_uploads_empty(monkeypatch):
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "")
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setattr(main, "VK_MAX_ATTACHMENTS", 10)
    monkeypatch.setattr(main, "VK_USER_TOKEN", "user-token")

    event = main.Event(
        id=5282,
        title="Благотворительный концерт",
        description="",
        date="2026-06-11",
        time="20:00",
        location_name="Стендап клуб Локация",
        source_text="Text",
        source_post_url="https://vk.com/wall-214027639_11341",
        photo_urls=["https://storage.example/poster.webp"],
        photo_count=1,
    )

    async def fake_upload(group_id, photo_url, db=None, bot=None):
        return None

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        raise AssertionError("vk_sync must not publish text-only when event media was available")

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post)

    with pytest.raises(RuntimeError, match="vk_sync_missing_media_for_telegram_event"):
        await main.sync_vk_source_post(event, "Text", None, None)


@pytest.mark.asyncio
async def test_sync_vk_source_post_blocks_new_post_on_partial_media_upload(monkeypatch):
    sync_globals = main.sync_vk_source_post.__globals__
    monkeypatch.setitem(sync_globals, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setitem(sync_globals, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setitem(sync_globals, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setitem(sync_globals, "VK_USER_TOKEN", "u")
    monkeypatch.setitem(sync_globals, "VK_MAX_ATTACHMENTS", 10)

    event = main.Event(
        id=5951,
        title="Путешествие в сказку в деревне Холмогорье",
        description="",
        date="2026-06-13",
        time="11:00",
        location_name="Холмогорье",
        source_text="Text",
        source_post_url="https://vk.com/wall-146688375_7432",
        photo_urls=["https://storage.example/1.webp", "https://storage.example/2.webp"],
        photo_count=2,
    )

    async def fake_upload(group_id, photo_url, db=None, bot=None):
        return "photo-1_1" if photo_url.endswith("1.webp") else None

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        raise AssertionError("vk_sync must not create a source post with partial media")

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post)

    with pytest.raises(RuntimeError, match="vk_sync_partial_media_upload"):
        await main.sync_vk_source_post(event, "Text", None, None)


@pytest.mark.asyncio
async def test_sync_vk_source_post_does_not_override_canonical_media_gate(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_TOKEN_AFISHA = "ga"
    main.VK_MAX_ATTACHMENTS = 10

    event = main.Event(
        title="Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
        photo_urls=["http://img1", "http://img2", "http://img3"],
    )

    uploaded: list[str] = []

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        uploaded.append(url)
        return f"ph{len(uploaded)}"

    posted: dict[str, list[str] | None] = {}

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        posted["vals"] = attachments
        return "https://vk.com/wall-1_2"

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post)

    url = await main.sync_vk_source_post(event, "Text", None, None)

    assert url == "https://vk.com/wall-1_2"
    assert uploaded == ["http://img1", "http://img2", "http://img3"]
    assert posted["vals"] == ["ph1", "ph2", "ph3"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_skips_group_only_photo_upload(monkeypatch, caplog):
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", "ga")
    monkeypatch.setattr(main, "VK_TOKEN", None)
    monkeypatch.setattr(main, "VK_USER_TOKEN", None)

    event = main.Event(
        title="Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
        photo_urls=["http://img1"],
    )

    posted: dict[str, list[str] | None] = {}
    caplog.set_level(logging.INFO)
    calls: list[str] = []

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        posted["attachments"] = attachments
        return "https://vk.com/wall-1_2"

    async def fake_vk_api(method, params, db=None, bot=None, token=None, **kwargs):
        calls.append(method)
        return {"response": {}}

    monkeypatch.setattr(main, "post_to_vk", fake_post)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    url = await main.sync_vk_source_post(event, "Text", None, None)

    assert url == "https://vk.com/wall-1_2"
    assert posted["attachments"] is None
    assert "photos.getWallUploadServer" not in calls
    assert not any("photos.getWallUploadServer" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_sync_vk_source_post_updates_attachments(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_TOKEN_AFISHA = "ga"

    event = main.Event(
        title="T",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
        photo_urls=["http://img1"],
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"

    async def fake_vk_api(method, params, db=None, bot=None, token=None, **kwargs):
        if method == "wall.getById":
            msg = main.build_vk_source_message(event, "old")
            return {"response": [{"text": msg}]}
        return {"response": {}}

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        return "ph1"

    edited: dict[str, list[str] | None] = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["attachments"] = attachments

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    url = await main.sync_vk_source_post(event, "new", None, None)

    assert url == "https://vk.com/wall-1_1"
    assert edited["attachments"] == ["ph1"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_does_not_run_afishaengagement_shadow_on_update(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_EVENTS_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_TOKEN_AFISHA = "ga"

    event = main.Event(
        id=42,
        title="Lecture",
        description="",
        date="2026-06-20",
        time="18:00",
        location_name="Place",
        photo_urls=["http://img1"],
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"

    async def fake_vk_api(method, params=None, db=None, bot=None, token=None, **kwargs):
        if method == "wall.getById":
            msg = main.build_vk_source_message(event, "old")
            return {"response": [{"text": msg}]}
        return {"response": {}}

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        return "ph1"

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        return None

    calls: list[dict[str, object]] = []

    async def fake_shadow(**kwargs):
        calls.append(kwargs)
        raise AssertionError("existing post update must not create a shadow CTA copy")

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)
    monkeypatch.setattr(
        "afishaengagement.maybe_publish_shadow_debug_copy",
        fake_shadow,
    )

    url = await main.sync_vk_source_post(event, "new text", None, None)

    assert url == "https://vk.com/wall-1_1"
    assert calls == []


@pytest.mark.asyncio
async def test_sync_vk_source_post_uses_afishaengagement_preflight_for_new_public_post(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_EVENTS_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_TOKEN_AFISHA = "ga"

    event = main.Event(
        id=44,
        title="Lecture",
        description="",
        date="2026-06-20",
        time="18:00",
        location_name="Place",
        photo_urls=["http://img1"],
    )

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        return "ph1"

    async def fail_post_to_vk(*args, **kwargs):
        raise AssertionError("plain VK post must not be created when public CTA preflight succeeds")

    captured: dict[str, object] = {}

    async def fake_engagement(**kwargs):
        captured.update(kwargs)
        return "https://vk.com/wall-1_777"

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fail_post_to_vk)
    monkeypatch.setattr(
        "afishaengagement.maybe_publish_shadow_debug_copy",
        fake_engagement,
    )

    url = await main.sync_vk_source_post(event, "new text", None, None)

    assert url == "https://vk.com/wall-1_777"
    assert captured["event"] is event
    assert captured["target_group_id"] == "1"
    assert captured["photo_urls"] == ["http://img1"]
    assert "new text" in str(captured["message"])
    assert captured["public_only"] is True
    assert captured["existing_vk_post_url"] is None


@pytest.mark.asyncio
async def test_sync_vk_source_post_keeps_plain_post_after_public_cta_miss(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_EVENTS_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_TOKEN_AFISHA = "ga"

    event = main.Event(
        id=45,
        title="Lecture",
        description="",
        date="2026-06-20",
        time="18:00",
        location_name="Place",
        photo_urls=["http://img1"],
    )

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        return "ph1"

    calls: list[tuple[str, object, object, object]] = []

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None, **kwargs):
        calls.append(("plain", attachments, None, None))
        return "https://vk.com/wall-1_123"

    async def fake_engagement(**kwargs):
        calls.append(
            (
                "engagement",
                kwargs.get("public_only"),
                kwargs.get("shadow_only"),
                kwargs.get("existing_vk_post_url"),
            )
        )
        if kwargs.get("public_only"):
            return None
        return "https://vk.com/wall-1_999"

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr(
        "afishaengagement.maybe_publish_shadow_debug_copy",
        fake_engagement,
    )

    url = await main.sync_vk_source_post(event, "new text", None, None)

    assert url == "https://vk.com/wall-1_123"
    assert calls == [
        ("engagement", True, False, None),
        ("plain", ["ph1"], None, None),
    ]


@pytest.mark.asyncio
async def test_sync_vk_source_post_dedupes_photo_list_on_update_without_shadow(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_EVENTS_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = True
    main.VK_TOKEN_AFISHA = "ga"
    main.VK_MAX_ATTACHMENTS = 10

    event = main.Event(
        id=43,
        title="Women Power",
        description="",
        date="2026-06-20",
        time="18:00",
        location_name="Place",
        photo_urls=["http://stale-or-duplicate", "http://actual-poster"],
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"

    async def fake_vk_api(method, params=None, db=None, bot=None, token=None, **kwargs):
        if method == "wall.getById":
            msg = main.build_vk_source_message(event, "old")
            return {"response": [{"text": msg}]}
        return {"response": {}}

    async def fake_dedupe(photo_urls):
        assert list(photo_urls) == ["http://stale-or-duplicate", "http://actual-poster"]
        return ["http://actual-poster"]

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        assert url == "http://actual-poster"
        return "photo-1_actual"

    edited: dict[str, object] = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["attachments"] = attachments

    calls: list[dict[str, object]] = []

    async def fake_shadow(**kwargs):
        calls.append(kwargs)
        raise AssertionError("existing post update must not create a shadow CTA copy")

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "_dedupe_event_photo_urls_for_publish", fake_dedupe)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)
    monkeypatch.setattr(
        "afishaengagement.maybe_publish_shadow_debug_copy",
        fake_shadow,
    )

    url = await main.sync_vk_source_post(event, "new text", None, None)

    assert url == "https://vk.com/wall-1_1"
    assert edited["attachments"] == ["photo-1_actual"]
    assert calls == []


@pytest.mark.asyncio
async def test_fetch_vk_latest_postponed_ignores_afishaengagement_debug(monkeypatch):
    now_ts = 1_781_100_000
    regular_ts = now_ts + 600
    debug_ts = now_ts + 3 * 24 * 3600

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime.fromtimestamp(now_ts, tz=timezone.utc)
            if tz is not None:
                return value.astimezone(tz)
            return value.replace(tzinfo=None)

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        assert method == "wall.get"
        return {
            "response": {
                "items": [
                    {
                        "id": 2,
                        "date": debug_ts,
                        "text": "event\n\n[AFISHAENGAGEMENT DEBUG COPY — DELETE BEFORE PUBLISH]\n#afishaengagement_shadow",
                    },
                    {"id": 1, "date": regular_ts, "text": "regular postponed"},
                ]
            }
        }

    actor = main.VkActor(kind="user", token=None, label="user")
    monkeypatch.setattr(main, "datetime", FixedDateTime)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)

    latest = await main._fetch_vk_latest_postponed_ts(-231920894, [actor], None, None)

    assert latest == regular_ts


@pytest.mark.asyncio
async def test_fetch_vk_latest_postponed_ignores_far_future_regular_anchor(monkeypatch):
    now_ts = 1_781_100_000
    near_ts = now_ts + 600
    far_ts = now_ts + 3 * 24 * 3600

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime.fromtimestamp(now_ts, tz=timezone.utc)
            if tz is not None:
                return value.astimezone(tz)
            return value.replace(tzinfo=None)

    async def fake_vk_api(method, params, db=None, bot=None, **kwargs):
        assert method == "wall.get"
        return {
            "response": {
                "items": [
                    {"id": 2, "date": far_ts, "text": "regular but stale future anchor"},
                    {"id": 1, "date": near_ts, "text": "regular postponed"},
                ]
            }
        }

    actor = main.VkActor(kind="user", token=None, label="user")
    monkeypatch.setattr(main, "datetime", FixedDateTime)
    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "VK_POSTPONED_MAX_ANCHOR_AHEAD_SECONDS", 18 * 3600)

    latest = await main._fetch_vk_latest_postponed_ts(-231920894, [actor], None, None)

    assert latest == near_ts


@pytest.mark.asyncio
async def test_sync_vk_source_post_resolves_stale_postponed_id(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_EVENTS_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = False

    event = main.Event(
        title="T",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
    )
    event.source_vk_post_url = "https://vk.com/wall-1_10"

    async def fake_vk_api(method, **kwargs):
        posts = kwargs.get("posts", "")
        if method == "wall.getById" and posts == "-1_10":
            return {"response": []}
        if method == "wall.getById" and posts == "-1_11":
            msg = main.build_vk_source_message(event, "old")
            return {"response": [{"text": msg}]}
        return {"response": []}

    async def fake_resolve(**kwargs):
        assert kwargs["post_id"] == 10
        return 11

    edited: dict[str, str] = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["url"] = url

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "_resolve_vk_postponed_wall_id", fake_resolve)
    monkeypatch.setattr(main, "choose_vk_actor", lambda owner_id, intent: [main.VkActor("user", "u", "user")])
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    url = await main.sync_vk_source_post(event, "new", None, None)

    assert url == "https://vk.com/wall-1_11"
    assert event.source_vk_post_url == "https://vk.com/wall-1_11"
    assert edited["url"] == "https://vk.com/wall-1_11"


@pytest.mark.asyncio
async def test_sync_vk_source_post_keeps_existing_same_id_postponed_post(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_EVENTS_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = False

    event = main.Event(
        title="T",
        description="",
        date="2026-06-17",
        time="19:00",
        location_name="Place",
    )
    event.source_vk_post_url = "https://vk.com/wall-1_10"

    async def fake_vk_api(method, **kwargs):
        posts = kwargs.get("posts", "")
        if method == "wall.getById" and posts == "-1_10":
            return {"response": {"items": []}}
        return {"response": {"items": []}}

    async def fake_resolve(**kwargs):
        assert kwargs["post_id"] == 10
        return 10

    edited: dict[str, str] = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["url"] = url

    async def fail_post(*args, **kwargs):
        raise AssertionError("existing postponed managed post must be edited, not recreated")

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "_resolve_vk_postponed_wall_id", fake_resolve)
    monkeypatch.setattr(main, "choose_vk_actor", lambda owner_id, intent: [main.VkActor("user", "u", "user")])
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)
    monkeypatch.setattr(main, "post_to_vk", fail_post)

    url = await main.sync_vk_source_post(event, "new", None, None)

    assert url == "https://vk.com/wall-1_10"
    assert event.source_vk_post_url == "https://vk.com/wall-1_10"
    assert edited["url"] == "https://vk.com/wall-1_10"


@pytest.mark.asyncio
async def test_postponed_lookup_uses_postponed_filter_before_all(monkeypatch):
    calls: list[str] = []

    async def fake_vk_api(method, params, *_args, **_kwargs):
        assert method == "wall.get"
        calls.append(params["filter"])
        if params["filter"] == "postponed":
            return {"response": {"items": [{"id": 7250, "date": 1783968000}]}}
        return {"response": {"items": []}}

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    actor = main.VkActor(kind="user", token="u", label="user")

    resolved = await main._resolve_vk_postponed_wall_id(
        owner_id=-231920894,
        post_id=7250,
        actor=actor,
        token="u",
        db=None,
        bot=None,
    )

    assert resolved == 7250
    assert calls == ["postponed"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_recreates_deleted_managed_post(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    main.VK_EVENTS_GROUP_ID = "1"
    main.VK_PHOTOS_ENABLED = False

    event = main.Event(
        title="T",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
    )
    event.source_vk_post_url = "https://vk.com/wall-1_10"

    async def fake_vk_api(method, **kwargs):
        assert method == "wall.getById"
        assert kwargs["posts"] == "-1_10"
        return {"response": {"items": []}}

    async def fake_resolve(**kwargs):
        return None

    async def fake_post(group_id, message, db=None, bot=None, attachments=None, **kwargs):
        return "https://vk.com/wall-1_12"

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "_resolve_vk_postponed_wall_id", fake_resolve)
    monkeypatch.setattr(main, "choose_vk_actor", lambda owner_id, intent: [main.VkActor("user", "u", "user")])
    monkeypatch.setattr(main, "post_to_vk", fake_post)

    url = await main.sync_vk_source_post(event, "new", None, None)

    assert url == "https://vk.com/wall-1_12"
    assert event.source_vk_post_url == ""


@pytest.mark.asyncio
async def test_sync_vk_source_post_attaches_partial_reupload_to_text_only_post(
    monkeypatch,
):
    sync_globals = main.sync_vk_source_post.__globals__
    monkeypatch.setitem(sync_globals, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setitem(sync_globals, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setitem(sync_globals, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setitem(sync_globals, "VK_USER_TOKEN", "u")
    monkeypatch.setitem(sync_globals, "VK_MAX_ATTACHMENTS", 10)

    event = main.Event(
        title="T",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
        photo_urls=["http://img1", "http://img2"],
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"

    async def fake_vk_api(method, params=None, db=None, bot=None, **kwargs):
        if method == "wall.getById":
            msg = main.build_vk_source_message(event, "old")
            return {"response": [{"text": msg}]}
        return {"response": {}}

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        return "ph1" if url.endswith("img1") else None

    edited: dict[str, list[str] | None] = {}

    async def fake_edit(url, message, db=None, bot=None, attachments=None, **kwargs):
        edited["attachments"] = attachments

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    url = await main.sync_vk_source_post(event, "new", None, None)

    assert url == "https://vk.com/wall-1_1"
    assert edited["attachments"] == ["ph1"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_preserves_existing_photos_on_partial_reupload(
    monkeypatch,
):
    sync_globals = main.sync_vk_source_post.__globals__
    monkeypatch.setitem(sync_globals, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setitem(sync_globals, "VK_AFISHA_GROUP_ID", "1")
    monkeypatch.setitem(sync_globals, "VK_PHOTOS_ENABLED", True)
    monkeypatch.setitem(sync_globals, "VK_USER_TOKEN", "u")
    monkeypatch.setitem(sync_globals, "VK_MAX_ATTACHMENTS", 10)

    event = main.Event(
        title="T",
        description="",
        date="2026-08-01",
        time="12:00",
        location_name="Place",
        photo_urls=["http://img1", "http://img2"],
        source_vk_post_url="https://vk.com/wall-1_1",
    )

    async def fake_vk_api(method, params=None, db=None, bot=None, **kwargs):
        if method == "wall.getById":
            msg = main.build_vk_source_message(event, "old")
            return {
                "response": {
                    "items": [
                        {
                            "text": msg,
                            "attachments": [{"type": "photo"}],
                        }
                    ]
                }
            }
        return {"response": {}}

    async def fake_upload(group_id, url, db=None, bot=None, *, token=None, token_kind="group"):
        return "ph1" if url.endswith("img1") else None

    edited = {}

    async def fake_edit(url, message, db=None, bot=None, attachments=None, **kwargs):
        edited["attachments"] = attachments

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    await main.sync_vk_source_post(event, "new", None, None)

    assert edited["attachments"] is None


@pytest.mark.asyncio
async def test_sync_vk_source_post_creates_when_existing_vk_url_is_external(
    monkeypatch,
):
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")

    event = main.Event(
        title="T",
        description="",
        date="2026-06-01",
        time="10:00",
        location_name="Place",
        source_text="T",
    )
    event.source_vk_post_url = "https://vk.com/wall-48383763_40520"

    posted = {}

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None, **_kwargs):
        posted["group_id"] = group_id
        posted["message"] = message
        return "https://vk.com/wall-231920894_1"

    async def fail_edit(*args, **kwargs):
        raise AssertionError("external source VK post must not be edited")

    monkeypatch.setattr(main, "post_to_vk", fake_post_to_vk)
    monkeypatch.setattr(main, "edit_vk_post", fail_edit)

    url = await main.sync_vk_source_post(event, "Text", None, None)

    assert url == "https://vk.com/wall-231920894_1"
    assert posted["group_id"] == "231920894"


def test_build_vk_source_message_converts_links():
    text = "Регистрация [здесь](http://reg) и <a href=\"http://pay\">билеты</a>"
    event = main.Event(
        title="T",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
    )
    msg = main.build_vk_source_message(event, text)
    assert "здесь (http://reg)" in msg
    assert "билеты (http://pay)" in msg


def test_build_vk_source_message_appends_announce_hashtags():
    event = main.Event(
        title="T",
        description="",
        date="2026-06-03",
        time="18:30",
        location_name="Place",
        city="Калининград",
    )

    msg = main.build_vk_source_message(event, "Описание")

    assert (
            "#анонс #анонс39 #кудапойтиКалининград #афишакалининград "
            "#Калининград #3июня #3_июня"
        ) in msg
    assert msg.endswith(main.VK_SOURCE_FOOTER)


def test_build_vk_source_message_appends_type_hashtag_from_text():
    event = main.Event(
        title="Открытая лекция",
        description="",
        date="2026-06-03",
        time="18:30",
        location_name="Place",
        city="Калининград",
    )

    msg = main.build_vk_source_message(event, "Описание")

    assert "#лекция" in msg


def test_build_vk_source_message_uses_canonical_festival_hashtag():
    event = main.Event(
        title="T",
        description="",
        date="2026-06-03",
        time="18:30",
        location_name="Place",
        city="Калининград",
        festival="Кантаты",
    )
    festival = main.Festival(name="Кантата")

    msg = main.build_vk_source_message(event, "Описание", festival=festival)

    assert "#Кантата" in msg
    assert "#Кантаты" not in msg


@pytest.mark.asyncio
async def test_resolve_event_festival_matches_inflected_event_label(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(main.Festival(name="Кантата"))
        await session.commit()

    event = main.Event(
        title="T",
        description="",
        date="2026-06-03",
        time="18:30",
        location_name="Place",
        city="Калининград",
        festival="Кантаты",
    )

    festival = await main._resolve_event_festival(db, event)

    assert festival is not None
    assert festival.name == "Кантата"


@pytest.mark.asyncio
async def test_sync_vk_source_post_does_not_preserve_old_hashtag_tail(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    event = main.Event(
        title="Old",
        description="",
        date="2026-06-03",
        time="18:30",
        location_name="Place",
        city="Калининград",
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"

    existing = main.build_vk_source_message(event, "old text")

    async def fake_vk_api(method, *_, **__):
        return {"response": [{"text": existing}]}

    edited = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["text"] = message

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    event.title = "New"
    await main.sync_vk_source_post(event, "new text", None, None, append_text=True)

    msg = edited["text"]
    assert msg.split().count("#анонс") == 1
    assert "old text" in msg
    assert "new text" in msg


@pytest.mark.asyncio
async def test_job_sync_vk_source_post_resyncs_title_only_change(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    future_date = (main.datetime.now(main.LOCAL_TZ).date() + main.timedelta(days=10)).isoformat()

    event = main.Event(
        title="New title",
        description="Same body",
        source_text="Same body",
        date=future_date,
        time="18:30",
        location_name="Place",
        city="Калининград",
        source_vk_post_url="https://vk.com/wall-1_1",
        vk_source_hash=main.content_hash("Same body"),
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = event.id

    calls = []

    async def fake_sync_vk_source_post(ev, text_for_vk, db_arg, bot, ics_url=None, **kwargs):
        calls.append((ev.title, text_for_vk, kwargs.get("append_text")))
        return "https://vk.com/wall-1_1"

    monkeypatch.setattr(main, "sync_vk_source_post", fake_sync_vk_source_post)

    await main.job_sync_vk_source_post(event_id, db, None)

    assert calls == [("New title", "Same body", False)]
    async with db.get_session() as session:
        updated = await session.get(main.Event, event_id)
    assert updated.vk_source_hash == _expected_vk_source_hash(updated, "Same body")


@pytest.mark.asyncio
async def test_job_sync_vk_source_post_resyncs_when_calendar_projection_is_removed(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    future_date = (main.datetime.now(main.LOCAL_TZ).date() + main.timedelta(days=10)).isoformat()

    event = main.Event(
        title="Calendar cleanup",
        description="Same body",
        source_text="Same body",
        date=future_date,
        time="",
        location_name="Place",
        city="Калининград",
        source_vk_post_url="https://vk.com/wall-1_1",
        ics_url="https://example.test/old.ics",
    )
    event.vk_source_hash = _expected_vk_source_hash(event, "Same body")
    event.ics_url = None
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)

    calls = []

    async def fake_sync_vk_source_post(ev, text_for_vk, db_arg, bot, ics_url=None, **kwargs):
        calls.append((ev.id, text_for_vk, ics_url))
        return "https://vk.com/wall-1_1"

    monkeypatch.setattr(main, "sync_vk_source_post", fake_sync_vk_source_post)
    monkeypatch.setattr(
        main,
        "_recover_managed_vk_live_url",
        lambda *args, **kwargs: main.asyncio.sleep(0, result=False),
    )

    await main.job_sync_vk_source_post(event_id, db, None)

    assert calls == [(event_id, "Same body", None)]
    async with db.get_session() as session:
        updated = await session.get(main.Event, event_id)
    assert updated.vk_source_hash == _expected_vk_source_hash(updated, "Same body")
    await db.close()


@pytest.mark.asyncio
async def test_job_sync_vk_source_post_republishes_missing_managed_post(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "231920894")
    future_date = (main.datetime.now(main.LOCAL_TZ).date() + main.timedelta(days=10)).isoformat()

    event = main.Event(
        title="Concert",
        description="Same body",
        source_text="Same body",
        date=future_date,
        time="18:30",
        location_name="Place",
        city="Калининград",
        source_vk_post_url="https://vk.com/wall-231920894_2375",
    )
    event.vk_source_hash = _expected_vk_source_hash(event, "Same body")
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = event.id

    async def fake_vk_api(method, **kwargs):
        assert method == "wall.getById"
        assert kwargs["posts"] == "-231920894_2375"
        return {"response": {"items": []}}

    calls = []

    async def fake_sync_vk_source_post(ev, text_for_vk, db_arg, bot, ics_url=None, **kwargs):
        calls.append((ev.source_vk_post_url, text_for_vk))
        return "https://vk.com/wall-231920894_2389"

    async def fake_no_postponed(**kwargs):
        return None

    async def fake_no_live(*args, **kwargs):
        return None

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(
        main, "_find_vk_postponed_wall_item_any_actor", fake_no_postponed
    )
    monkeypatch.setattr(
        main, "_find_unique_live_managed_vk_item_for_event", fake_no_live
    )
    monkeypatch.setattr(main, "sync_vk_source_post", fake_sync_vk_source_post)

    await main.job_sync_vk_source_post(event_id, db, None)

    assert calls == [(None, "Same body")]
    async with db.get_session() as session:
        updated = await session.get(main.Event, event_id)
    assert updated.source_vk_post_url == "https://vk.com/wall-231920894_2389"
    assert updated.vk_source_hash == _expected_vk_source_hash(updated, "Same body")


@pytest.mark.asyncio
async def test_job_sync_vk_source_post_skips_past_event(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setattr(main, "VK_EVENTS_GROUP_ID", "1")
    monkeypatch.setattr(main, "VK_AFISHA_GROUP_ID", "1")
    past_date = (main.datetime.now(main.LOCAL_TZ).date() - main.timedelta(days=1)).isoformat()

    event = main.Event(
        title="Past",
        description="Same body",
        source_text="Same body",
        date=past_date,
        time="18:30",
        location_name="Place",
        city="Калининград",
        source_vk_post_url="https://vk.com/wall-2_1",
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = event.id

    calls = []

    async def fake_sync_vk_source_post(ev, text_for_vk, db_arg, bot, ics_url=None, **kwargs):
        calls.append((ev.id, text_for_vk))
        return "https://vk.com/wall-1_1"

    monkeypatch.setattr(main, "sync_vk_source_post", fake_sync_vk_source_post)

    result = await main.job_sync_vk_source_post(event_id, db, None)

    assert result is False
    assert calls == []


@pytest.mark.asyncio
async def test_persist_vk_source_post_result_retries_sqlite_lock(monkeypatch):
    event = main.Event(
        id=123,
        title="T",
        description="d",
        date="2026-06-03",
        time="18:30",
        location_name="Place",
        city="Калининград",
    )
    sleep_delays = []

    async def fake_sleep(delay):
        sleep_delays.append(delay)

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)

    class FakeDB:
        def __init__(self):
            self.commit_calls = 0

        def get_session(self):
            db = self

            class FakeSession:
                async def get(self, model, obj_id):
                    if model is main.Event and obj_id == event.id:
                        return event
                    return None

                def add(self, obj):
                    return None

                async def commit(self):
                    db.commit_calls += 1
                    if db.commit_calls == 1:
                        raise sqlite3.OperationalError("database is locked")

            class Ctx:
                async def __aenter__(self):
                    return FakeSession()

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            return Ctx()

    db = FakeDB()
    vk_url = "https://vk.com/wall-231920894_2002"
    new_hash = main.content_hash("T\nd")

    saved, user = await main._persist_vk_source_post_result(
        event.id,
        db,
        vk_url,
        new_hash,
        bot=None,
    )

    assert saved is event
    assert user is None
    assert db.commit_calls == 2
    assert sleep_delays
    assert event.source_vk_post_url == vk_url
    assert event.vk_source_hash == new_hash


@pytest.mark.asyncio
async def test_add_events_from_text_preserves_links(tmp_path: Path, monkeypatch):
    main.VK_AFISHA_GROUP_ID = ""
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_parse(text: str, source_channel: str | None = None, **kwargs):
        return [
            {
                "title": "T",
                "short_description": "d",
                "date": "2099-01-01",
                "time": "18:00",
                "location_name": "Hall",
            }
        ]

    async def fake_create(title, text, source, html_text=None, media=None, ics_url=None, db=None, **kwargs):
        return "url", "p"

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(main, "create_source_page", fake_create)

    html = "<a href='http://reg'>Регистрация</a>"
    res = await main.add_events_from_text(db, "Регистрация", None, html, None)
    ev = res[0][0]
    assert "http://reg" in ev.source_text
    assert "(http://reg)" not in ev.source_text


@pytest.mark.asyncio
async def test_sync_vk_source_post_appends_only_text(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    event = main.Event(
        title="Old",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Old Place",
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"

    existing = main.build_vk_source_message(event, "old text")

    async def fake_vk_api(method, *_, **__):
        return {"response": [{"text": existing}]}

    edited = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["text"] = message

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    event.title = "New"
    event.location_name = "New Place"

    url = await main.sync_vk_source_post(
        event, "new text", None, None, append_text=True
    )

    assert url == "https://vk.com/wall-1_1"
    msg = edited["text"]
    lines = msg.splitlines()
    assert lines[0] == "New"
    assert "Old Place" not in msg
    assert "old text" in msg
    assert "new text" in msg
    assert msg.count(main.CONTENT_SEPARATOR) == 1
    assert lines.count("New") == 1


@pytest.mark.asyncio
async def test_sync_vk_source_post_does_not_append_identical_text(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    event = main.Event(
        title="Выставка",
        description="",
        date="2026-07-18",
        time="",
        location_name="Янтарный",
        city="Янтарный",
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"
    existing = main.build_vk_source_message(event, "Однодневная уличная выставка.")

    async def fake_vk_api(method, *_, **__):
        return {"response": [{"text": existing}]}

    edited = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["text"] = message

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    await main.sync_vk_source_post(
        event,
        "Однодневная уличная выставка.",
        None,
        None,
        append_text=True,
    )

    assert edited["text"].count("Однодневная уличная выставка.") == 1
    assert main.CONTENT_SEPARATOR not in edited["text"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_updates_without_append(monkeypatch):
    main.VK_AFISHA_GROUP_ID = "1"
    event = main.Event(
        title="Old Title",
        description="",
        date="2024-01-01",
        time="00:00",
        location_name="Place",
    )
    event.source_vk_post_url = "https://vk.com/wall-1_1"

    existing = main.build_vk_source_message(event, "old text")

    async def fake_vk_api(method, *_, **__):
        return {"response": [{"text": existing}]}

    edited = {}

    async def fake_edit(
        url, message, db=None, bot=None, attachments=None, **kwargs
    ):
        edited["text"] = message

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    event.title = "Updated Title"

    url = await main.sync_vk_source_post(
        event, "updated text", None, None, append_text=False
    )

    assert url == "https://vk.com/wall-1_1"
    msg = edited["text"]
    lines = msg.splitlines()
    assert lines[0] == "Updated Title"
    assert "old text" not in msg
    assert "updated text" in msg
    assert main.CONTENT_SEPARATOR not in msg
