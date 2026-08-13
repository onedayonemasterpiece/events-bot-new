from datetime import timezone
from types import SimpleNamespace
from shutil import _ntuple_diskusage

import pytest

import main
import vk_intake
from db import Database


def test_vk_crawl_storage_guard_blocks_before_volume_is_critical(monkeypatch):
    monkeypatch.setattr(
        vk_intake.shutil,
        "disk_usage",
        lambda _path: _ntuple_diskusage(3 * 1024**3, 3 * 1024**3 - 280 * 1024**2, 280 * 1024**2),
    )
    monkeypatch.setenv("VK_CRAWL_MIN_FREE_MB", "512")

    with pytest.raises(RuntimeError, match="vk_crawl_storage_admission_blocked"):
        vk_intake._require_vk_crawl_storage_headroom(
            SimpleNamespace(path="/data/db.sqlite")
        )


def test_vk_crawl_storage_guard_does_not_apply_to_local_test_db(monkeypatch):
    monkeypatch.setattr(
        vk_intake.shutil,
        "disk_usage",
        lambda _path: (_ for _ in ()).throw(AssertionError("unexpected disk probe")),
    )

    vk_intake._require_vk_crawl_storage_headroom(
        SimpleNamespace(path="/tmp/test-db.sqlite")
    )


@pytest.mark.asyncio
async def test_vk_crawl_rechecks_capacity_between_sources(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNTIME_DISK_PATH", str(tmp_path))
    monkeypatch.setenv("VK_CRAWL_MIN_FREE_MB", "512")
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.executemany(
            """
            INSERT INTO vk_source(group_id,screen_name,name,location,default_time,default_ticket_link)
            VALUES(?,?,?,?,?,?)
            """,
            [(1, "one", "One", "", None, None), (2, "two", "Two", "", None, None)],
        )
        await conn.commit()

    probes = 0

    def shrinking_usage(_path):
        nonlocal probes
        probes += 1
        free_mb = 900 if probes <= 3 else 500
        return _ntuple_diskusage(3 * 1024**3, 3 * 1024**3 - free_mb * 1024**2, free_mb * 1024**2)

    seen_groups = []

    async def fake_wall(group_id, since, count, offset=0, owner_type="group"):
        seen_groups.append(group_id)
        return []

    monkeypatch.setattr(vk_intake.shutil, "disk_usage", shrinking_usage)
    monkeypatch.setattr(main, "vk_wall_since", fake_wall)

    with pytest.raises(RuntimeError, match="vk_crawl_storage_admission_blocked"):
        await vk_intake.crawl_once(db)

    assert len(seen_groups) == 1
    assert probes >= 4
    await db.close()


def test_vk_llm_text_field_cleaner_drops_location_placeholders():
    assert vk_intake._clean_llm_text_field("location_address", field_name="location_address") is None
    assert vk_intake._clean_llm_text_field(" address ", field_name="location_address") is None
    assert vk_intake._clean_llm_text_field("адрес", field_name="location_address") is None
    assert vk_intake._clean_llm_text_field("location_name", field_name="location_name") is None
    assert vk_intake._clean_llm_text_field("Черняховского 26", field_name="location_address") == "Черняховского 26"


class DummyMessage:
    def __init__(self, chat_id, text, reply_markup):
        self.chat = SimpleNamespace(id=chat_id)
        self.text = text
        self.reply_markup = reply_markup
        self.extra_kwargs: dict[str, object] = {}

    async def edit_text(self, text, **kwargs):
        self.text = text
        self.reply_markup = kwargs.get("reply_markup")
        return self


class DummyBot:
    def __init__(self):
        self.messages = []

    async def send_message(self, chat_id, text, **kwargs):
        msg = DummyMessage(chat_id, text, kwargs.get("reply_markup"))
        msg.extra_kwargs = kwargs
        self.messages.append(msg)
        return msg


class DummyCallback:
    def __init__(self, data, message):
        self.data = data
        self.message = message
        self.answers: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def answer(self, *args, **kwargs):
        self.answers.append((args, kwargs))


@pytest.mark.asyncio
async def test_vk_list_shows_numbers_and_default_time(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        for idx in range(1, 13):
            await conn.execute(
                "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
                (
                    idx,
                    f"club{idx}",
                    f"Group {idx}",
                    None,
                    "19:00" if idx == 1 else None,
                    "https://tickets.example/club1" if idx == 1 else None,
                ),
            )
        await conn.execute(
            "INSERT INTO vk_crawl_cursor(group_id, updated_at, checked_at) VALUES(?, ?, ?)",
            (1, "2024-05-31 12:34:56", 1717245296),
        )
        await conn.execute(
            "INSERT INTO vk_crawl_cursor(group_id, updated_at) VALUES(?, ?)",
            (2, 1717156496),
        )
        # inbox stats for first two groups
        for post_id in range(2):
            await conn.execute(
                "INSERT INTO vk_inbox(group_id, post_id, date, text, matched_kw, has_date, status) VALUES(?,?,?,?,?,?,?)",
                (1, post_id, 0, "text", None, 1, "pending"),
            )
        await conn.execute(
            "INSERT INTO vk_inbox(group_id, post_id, date, text, matched_kw, has_date, status) VALUES(?,?,?,?,?,?,?)",
            (1, 100, 0, "text", None, 1, "skipped"),
        )
        for post_id in range(12):
            await conn.execute(
                "INSERT INTO vk_inbox(group_id, post_id, date, text, matched_kw, has_date, status) VALUES(?,?,?,?,?,?,?)",
                (2, 200 + post_id, 0, "text", None, 1, "imported"),
            )
        await conn.execute(
            "INSERT INTO vk_inbox(group_id, post_id, date, text, matched_kw, has_date, status) VALUES(?,?,?,?,?,?,?)",
            (2, 400, 0, "text", None, 1, "rejected"),
        )
        await conn.commit()

    bot = DummyBot()
    msg = SimpleNamespace(chat=SimpleNamespace(id=1))
    await main.handle_vk_list(msg, db, bot)

    assert bot.messages, "no message sent"
    lines = bot.messages[0].text.splitlines()
    assert lines[0].startswith("1.")
    assert "типовое время: 19:00" in lines[0]
    assert "билеты: https://tickets.example/club1" in lines[0]
    assert "последнее сканирование: 2024-06-01 12:34" in lines[0]
    assert "последний найденный пост: 2024-05-31 12:34" in lines[0]
    assert lines[1] == " Pending | Skipped | Imported | Rejected "
    assert (
        lines[2]
        == "       2        |       1        |        0        |        0        "
    )
    assert lines[3].startswith("2.")
    assert "типовое время: -" in lines[3]
    assert ", последнее сканирование: -" in lines[3]
    assert "последний найденный пост: 2024-05-31 11:54" in lines[3]
    assert lines[4] == " Pending | Skipped | Imported | Rejected "
    assert (
        lines[5]
        == "       0        |       0        |       12        |        1        "
    )
    buttons = bot.messages[0].reply_markup.inline_keyboard
    assert buttons[0][0].text == "❌ 1"
    assert buttons[0][0].callback_data.endswith(":1")
    assert buttons[0][1].text == "⚙️ 1"
    assert buttons[1][0].text == "🕒 1"
    assert buttons[1][1].text == "🎟 1"
    assert buttons[1][2].text == "📍 1"
    assert buttons[2][0].text == "❌ 2"
    assert buttons[2][0].callback_data.endswith(":1")
    assert buttons[2][1].text == "⚙️ 2"
    assert buttons[3][0].text == "🕒 2"
    assert buttons[3][1].text == "🎟 2"
    assert buttons[3][2].text == "📍 2"
    rejected_buttons = [btn for row in buttons for btn in row if btn.text.startswith("🚫 Rejected")]
    assert rejected_buttons, "expected rejected button"
    assert rejected_buttons[0].text == "🚫 Rejected: 1"
    assert rejected_buttons[0].callback_data == "vkrejected:2:1"
    assert buttons[-1][-1].callback_data == "vksrcpage:2"

    callback = DummyCallback("vksrcpage:2", bot.messages[0])
    await main.handle_vk_list_page_callback(callback, db, bot)
    assert callback.answers
    page2_lines = bot.messages[0].text.splitlines()
    assert page2_lines[0].startswith("11.")
    assert page2_lines[1] == " Pending | Skipped | Imported | Rejected "
    assert (
        page2_lines[2]
        == "       0        |       0        |        0        |        0        "
    )
    nav_row = bot.messages[0].reply_markup.inline_keyboard[-1]
    assert nav_row[0].callback_data == "vksrcpage:1"


@pytest.mark.asyncio
async def test_vk_rejected_callback_sends_links(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Group 1", None, None, None),
        )
        for idx in range(2):
            await conn.execute(
                "INSERT INTO vk_inbox(group_id, post_id, date, text, matched_kw, has_date, status) VALUES(?,?,?,?,?,?,?)",
                (1, 100 + idx, 0, "text", None, 1, "rejected"),
            )
        await conn.commit()

    bot = DummyBot()
    list_message = DummyMessage(1, "", None)
    callback = DummyCallback("vkrejected:1:1", list_message)

    await main.handle_vk_rejected_callback(callback, db, bot)

    assert callback.answers
    assert len(bot.messages) == 1
    sent = bot.messages[0]
    assert sent.extra_kwargs.get("disable_web_page_preview") is True
    lines = sent.text.splitlines()
    assert lines[0] == "🚫 Отклонённые посты — Group 1 (vk.com/club1)"
    assert set(lines[1:]) == {
        "https://vk.com/wall-1_100",
        "https://vk.com/wall-1_101",
    }


@pytest.mark.asyncio
async def test_vk_rejected_callback_alerts_when_empty(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Group 1", None, None, None),
        )
        await conn.commit()

    bot = DummyBot()
    list_message = DummyMessage(1, "", None)
    callback = DummyCallback("vkrejected:1:1", list_message)

    await main.handle_vk_rejected_callback(callback, db, bot)

    assert not bot.messages
    assert callback.answers
    assert callback.answers[0][0] == ("Нет отклонённых постов",)


@pytest.mark.asyncio
async def test_vk_crawl_empty_group_updates_checked_at(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Group 1", None, None, None),
        )
        await conn.commit()

    fixed_now = 1717245296

    async def fake_wall_since(gid, since, count, offset=0, owner_type="group"):
        assert gid == 1
        assert offset == 0
        return []

    async def fake_get_tz_offset(db_obj):
        assert db_obj is db
        main.LOCAL_TZ = timezone.utc
        return "+00:00"

    async def no_sleep(_):
        pass

    monkeypatch.setattr(main, "vk_wall_since", fake_wall_since)
    monkeypatch.setattr(main, "get_supabase_client", lambda: None)
    monkeypatch.setattr(main, "get_tz_offset", fake_get_tz_offset)
    monkeypatch.setattr(main, "mark_vk_import_result", lambda **_: None)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(vk_intake.time, "time", lambda: fixed_now)

    stats = await vk_intake.crawl_once(db)
    assert stats["groups_checked"] == 1

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT last_seen_ts, last_post_id, updated_at, checked_at FROM vk_crawl_cursor WHERE group_id=?",
            (1,),
        )
        row = await cur.fetchone()

    assert row == (0, 0, None, fixed_now)

    bot = DummyBot()
    msg = SimpleNamespace(chat=SimpleNamespace(id=1))
    await main.handle_vk_list(msg, db, bot)

    assert bot.messages, "no message sent"
    lines = bot.messages[0].text.splitlines()
    assert "последнее сканирование: 2024-06-01 12:34" in lines[0]


@pytest.mark.asyncio
async def test_vk_quiet_source_uses_checked_at_to_avoid_repeated_backfill(
    tmp_path, monkeypatch
):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    fixed_now = 1_717_245_296
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name) VALUES(1,'club1','Group 1')"
        )
        await conn.execute(
            """
            INSERT INTO vk_crawl_cursor(
                group_id,last_seen_ts,last_post_id,updated_at,checked_at
            ) VALUES(1,?,?,?,?)
            """,
            (fixed_now - 10 * 86400, 10, fixed_now - 10 * 86400, fixed_now - 60),
        )
        await conn.commit()

    calls = []

    async def fake_wall_since(gid, since, count, offset=0, owner_type="group"):
        calls.append((gid, since, count, offset))
        return []

    async def no_sleep(_):
        pass

    monkeypatch.setattr(main, "vk_wall_since", fake_wall_since)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(vk_intake.time, "time", lambda: fixed_now)

    stats = await vk_intake.crawl_once(db)

    assert stats["backfill_days_used"] is None
    assert calls[0][1] == fixed_now - 10 * 86400 - vk_intake.VK_CRAWL_OVERLAP_SEC


@pytest.mark.asyncio
async def test_vk_default_time_message_updates_db(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "One", None, None, None),
        )
        await conn.commit()
        cur = await conn.execute("SELECT id FROM vk_source")
        (vid,) = await cur.fetchone()

    bot = DummyBot()
    main.vk_default_time_sessions[1] = main.VkDefaultTimeSession(
        source_id=vid,
        page=1,
    )
    message = SimpleNamespace(
        chat=SimpleNamespace(id=1),
        from_user=SimpleNamespace(id=1),
        text="20:30",
    )
    await main.handle_vk_dtime_message(message, db, bot)
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT default_time FROM vk_source WHERE id=?", (vid,))
        (val,) = await cur.fetchone()
    assert val == "20:30"

    main.vk_default_time_sessions[1] = main.VkDefaultTimeSession(
        source_id=vid,
        page=1,
    )
    message.text = "-"
    await main.handle_vk_dtime_message(message, db, bot)
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT default_time FROM vk_source WHERE id=?", (vid,))
        (val,) = await cur.fetchone()
    assert val is None


@pytest.mark.asyncio
async def test_vk_default_ticket_link_message_updates_db(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "One", None, None, None),
        )
        await conn.commit()
        cur = await conn.execute("SELECT id FROM vk_source")
        (vid,) = await cur.fetchone()

    bot = DummyBot()
    list_message = DummyMessage(1, "", None)
    main.vk_default_ticket_link_sessions.clear()
    main.vk_default_ticket_link_sessions[1] = main.VkDefaultTicketLinkSession(
        source_id=vid,
        page=1,
        message=list_message,
    )
    message = SimpleNamespace(
        chat=SimpleNamespace(id=1),
        from_user=SimpleNamespace(id=1),
        text="https://tickets.new",
    )
    await main.handle_vk_ticket_link_message(message, db, bot)
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT default_ticket_link FROM vk_source WHERE id=?",
            (vid,),
        )
        (link_val,) = await cur.fetchone()
    assert link_val == "https://tickets.new"
    assert "https://tickets.new" in bot.messages[-1].text
    assert "https://tickets.new" in list_message.text

    main.vk_default_ticket_link_sessions[1] = main.VkDefaultTicketLinkSession(
        source_id=vid,
        page=1,
        message=list_message,
    )
    message.text = "-"
    await main.handle_vk_ticket_link_message(message, db, bot)
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT default_ticket_link FROM vk_source WHERE id=?",
            (vid,),
        )
        (link_val,) = await cur.fetchone()
    assert link_val is None

    main.vk_default_ticket_link_sessions[1] = main.VkDefaultTicketLinkSession(
        source_id=vid,
        page=1,
        message=list_message,
    )
    message.text = "ftp://invalid"
    await main.handle_vk_ticket_link_message(message, db, bot)
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT default_ticket_link FROM vk_source WHERE id=?",
            (vid,),
        )
        (link_val,) = await cur.fetchone()
    assert link_val is None
    assert "Неверный формат" in bot.messages[-1].text
    assert 1 in main.vk_default_ticket_link_sessions


@pytest.mark.asyncio
async def test_vk_default_location_message_updates_db(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "One", None, None, None),
        )
        await conn.commit()
        cur = await conn.execute("SELECT id FROM vk_source")
        (vid,) = await cur.fetchone()

    bot = DummyBot()
    list_message = DummyMessage(1, "", None)
    main.vk_default_location_sessions.clear()
    main.vk_default_location_sessions[1] = main.VkDefaultLocationSession(
        source_id=vid,
        page=1,
        message=list_message,
    )
    message = SimpleNamespace(
        chat=SimpleNamespace(id=1),
        from_user=SimpleNamespace(id=1),
        text="Калининград",
    )
    await main.handle_vk_location_message(message, db, bot)
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT location FROM vk_source WHERE id=?", (vid,))
        (location_val,) = await cur.fetchone()
    assert location_val == "Калининград"
    assert "Калининград" in bot.messages[-1].text
    assert "Калининград" in list_message.text

    main.vk_default_location_sessions[1] = main.VkDefaultLocationSession(
        source_id=vid,
        page=1,
        message=list_message,
    )
    message.text = "-"
    await main.handle_vk_location_message(message, db, bot)
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT location FROM vk_source WHERE id=?", (vid,))
        (location_val,) = await cur.fetchone()
    assert location_val is None


@pytest.mark.asyncio
async def test_db_init_repairs_known_vk_source_location_defaults(tmp_path):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location) VALUES(?,?,?,?)",
            (214027639, "locostandup", "Loco Standup", "Калининград Сити Джаз Клуб, Мира 33-35, Калининград"),
        )
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location) VALUES(?,?,?,?)",
            (149955604, "bar_bastion", "БАСТИОН. Калининград", "Калининград Сити Джаз Клуб"),
        )
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location) VALUES(?,?,?,?)",
            (30777579, "konb39", "Калининградская областная научная библиотека", None),
        )
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location) VALUES(?,?,?,?)",
            (179910542, "amberarena39", "Дворец спорта «Янтарный»", None),
        )
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location) VALUES(?,?,?,?)",
            (39437155, "mbkaliningrad39", "Мой бизнес 39 | Калининград", "Калининград Сити Джаз Клуб"),
        )
        await conn.commit()

    await db.init()

    async with db.raw_conn() as conn:
        rows = await conn.execute_fetchall(
            "SELECT group_id, location FROM vk_source WHERE group_id IN (?, ?, ?, ?, ?) ORDER BY group_id",
            (149955604, 179910542, 214027639, 30777579, 39437155),
        )
    locations = {int(row[0]): row[1] for row in rows}
    assert locations[149955604] == "Бар Бастион, Судостроительная 6/1, Калининград"
    assert locations[179910542] == "Дворец спорта «Янтарный», Согласия 39, Калининград"
    assert locations[214027639] == "Стендап клуб Локация, Юбилейная 18, Калининград"
    assert locations[30777579] == "Научная библиотека, Мира 9, Калининград"
    assert locations[39437155] == "Центр «Мой бизнес», Уральская 18, Калининград"


@pytest.mark.asyncio
async def test_build_event_payload_includes_default_time(monkeypatch):
    captured = {}

    async def fake_parse(text, **kwargs):
        captured["text"] = text
        captured["festival_names"] = kwargs.get("festival_names")
        return [{"title": "T", "date": "2099-01-01"}]

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)

    draft, festival_payload = await vk_intake.build_event_payload_from_vk(
        "text", default_time="19:00"
    )

    assert captured["text"].startswith("text\n")
    assert "Правила извлечения локации" in captured["text"]
    assert captured["festival_names"] is None
    assert draft.time == "19:00"
    assert festival_payload is None


@pytest.mark.asyncio
async def test_build_event_payload_drops_ddmm_date_marker_time_even_with_other_times(monkeypatch):
    """INC-2026-06-12: date markers like 12.06 must not survive as 12:06
    when the same source contains real times elsewhere."""

    async def fake_parse(text, **kwargs):
        return [
            {
                "title": "Род мужской",
                "date": "2026-06-12",
                "time": "12:06",
                "location_name": "ОКЦ на Горького",
                "location_address": "Горького 116",
                "city": "Калининград",
            }
        ]

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)

    draft, festival_payload = await vk_intake.build_event_payload_from_vk(
        "12.06 — «Род мужской» в 20:30\n13.06 — «Солнцестояние» в 20:00"
    )

    assert draft.time is None
    assert draft.time_is_default is False
    assert festival_payload is None


@pytest.mark.asyncio
async def test_build_event_payload_uses_default_ticket_link(monkeypatch):
    captured = {}

    async def fake_parse(text, **kwargs):
        captured["text"] = text
        return [{"title": "T", "date": "2099-01-01"}]

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)

    draft, festival_payload = await vk_intake.build_event_payload_from_vk(
        "text", default_ticket_link="https://tickets.example"
    )

    assert "https://tickets.example" in captured["text"]
    assert draft.links == ["https://tickets.example"]
    assert festival_payload is None


@pytest.mark.asyncio
async def test_build_event_payload_preserves_llm_ticket_link(monkeypatch):
    captured = {}

    async def fake_parse(text, **kwargs):
        captured["text"] = text
        return [
            {
                "title": "T",
                "date": "2099-01-01",
                "ticket_link": "https://real.example",
            }
        ]

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)

    draft, festival_payload = await vk_intake.build_event_payload_from_vk(
        "text", default_ticket_link="https://tickets.example"
    )

    assert "https://tickets.example" in captured["text"]
    assert draft.links == ["https://real.example"]
    assert festival_payload is None
