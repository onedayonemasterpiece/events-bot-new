import pytest
from pathlib import Path
import logging
import main


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

    async with db.get_session() as session:
        event = main.Event(
            title="T",
            description="",
            date="2026-06-01",
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
        group_id, message, db=None, bot=None, attachments=None
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

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None):
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

    async def fake_post(group_id, message, db=None, bot=None, attachments=None):
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

    async def fake_post(group_id, message, db=None, bot=None, attachments=None):
        posted["vals"] = attachments
        return "https://vk.com/wall-1_2"

    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "post_to_vk", fake_post)

    url = await main.sync_vk_source_post(event, "Text", None, None)

    assert url == "https://vk.com/wall-1_2"
    assert uploaded == [("http://img1", None)]
    assert posted["vals"] == ["ph1"]


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

    async def fake_post(group_id, message, db=None, bot=None, attachments=None):
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

    async def fake_edit(url, message, db=None, bot=None, attachments=None):
        edited["attachments"] = attachments

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    url = await main.sync_vk_source_post(event, "new", None, None)

    assert url == "https://vk.com/wall-1_1"
    assert edited["attachments"] == ["ph1"]


@pytest.mark.asyncio
async def test_sync_vk_source_post_preserves_attachments_on_partial_reupload(
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

    async def fake_edit(url, message, db=None, bot=None, attachments=None):
        edited["attachments"] = attachments

    monkeypatch.setattr(main, "_vk_api", fake_vk_api)
    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    monkeypatch.setattr(main, "upload_vk_photo", fake_upload)
    monkeypatch.setattr(main, "edit_vk_post", fake_edit)

    url = await main.sync_vk_source_post(event, "new", None, None)

    assert url == "https://vk.com/wall-1_1"
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

    async def fake_post_to_vk(group_id, message, db=None, bot=None, attachments=None):
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
        "#анонс #анонс39 #кудапойтиКалининград #афишаКалининград "
        "#Калининград #3июня #3_июня"
    ) in msg
    assert msg.endswith(main.VK_SOURCE_FOOTER)


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

    async def fake_edit(url, message, db=None, bot=None, attachments=None):
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
async def test_add_events_from_text_preserves_links(tmp_path: Path, monkeypatch):
    main.VK_AFISHA_GROUP_ID = ""
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_parse(text: str, source_channel: str | None = None, festival_names=None):
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

    monkeypatch.setattr(main, "parse_event_via_4o", fake_parse)
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

    async def fake_edit(url, message, db=None, bot=None, attachments=None):
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

    async def fake_edit(url, message, db=None, bot=None, attachments=None):
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
