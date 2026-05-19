"""Interface dry-run for the partner promo flow as @Ekaterina_Tikun.

Stands up a fresh SQLite DB, seeds Tikun (real user_id from prod) + one
future event of hers, then walks the FSM and management menu and prints
each rendered text + button keyboard. No live Telegram calls, no
production mutations.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from aiogram import types  # noqa: E402

from db import Database  # noqa: E402
from models import Event, User  # noqa: E402


TIKUN_USER_ID = 1614660478
TIKUN_USERNAME = "Ekaterina_Tikun"


# --- recording fake Bot --------------------------------------------------


@dataclass
class RecordedMessage:
    chat_id: int
    text: str
    reply_markup: types.InlineKeyboardMarkup | None
    parse_mode: str | None


class FakeBot:
    def __init__(self) -> None:
        self.messages: list[RecordedMessage] = []
        self.id = 999

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_markup: types.InlineKeyboardMarkup | None = None,
        parse_mode: str | None = None,
        **_: Any,
    ) -> None:
        self.messages.append(
            RecordedMessage(
                chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode
            )
        )


class FakeChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class FakeUser:
    def __init__(self, user_id: int, username: str) -> None:
        self.id = user_id
        self.username = username


class FakeMessage:
    def __init__(self, *, chat_id: int, from_user: FakeUser, text: str | None = None) -> None:
        self.chat = FakeChat(chat_id)
        self.from_user = from_user
        self.text = text
        self.message_id = 1
        self.last_edit: tuple[str, types.InlineKeyboardMarkup | None, str | None] | None = None

    async def edit_text(
        self,
        text: str,
        *,
        reply_markup: types.InlineKeyboardMarkup | None = None,
        parse_mode: str | None = None,
        **_: Any,
    ) -> None:
        self.last_edit = (text, reply_markup, parse_mode)

    async def delete(self) -> None:
        return None


class FakeCallback:
    def __init__(
        self,
        *,
        data: str,
        message: FakeMessage,
        from_user: FakeUser,
    ) -> None:
        self.data = data
        self.message = message
        self.from_user = from_user
        self.answered: list[tuple[str, bool]] = []

    async def answer(self, text: str = "", *, show_alert: bool = False, **_: Any) -> None:
        self.answered.append((text, show_alert))


# --- helpers -------------------------------------------------------------


def fmt_keyboard(markup: types.InlineKeyboardMarkup | None) -> str:
    if markup is None:
        return "(no buttons)"
    lines = []
    for row in markup.inline_keyboard:
        cells = []
        for b in row:
            cells.append(f"[{b.text}]")
        lines.append(" ".join(cells))
    return "\n".join(lines)


def print_frame(title: str, *, text: str, markup: types.InlineKeyboardMarkup | None) -> None:
    bar = "─" * 78
    print(bar)
    print(f"┃ {title}")
    print(bar)
    print(text)
    print()
    print("Buttons:")
    print(fmt_keyboard(markup))
    print()


# --- the dry-run ---------------------------------------------------------


async def run() -> None:
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    db = Database(path)
    await db.init()

    today = date(2026, 5, 18)
    async with db.get_session() as session:
        session.add(
            User(
                user_id=TIKUN_USER_ID,
                username=TIKUN_USERNAME,
                is_partner=True,
                organization="Научная библиотека",
            )
        )
        # one future event matching the real partner workflow
        ev = Event(
            title="Лекция о Канте",
            description="Лекция о философии Канта в библиотеке.",
            short_description="Лекция о Канте",
            search_digest="Кант лекция",
            source_text="seed",
            date="2026-05-25",
            time="18:00",
            location_name="Калининградская областная научная библиотека",
            location_address="ул. Тельмана 6",
            city="Калининград",
            photo_urls=["https://example.com/poster.jpg"],
            photo_count=1,
            creator_id=TIKUN_USER_ID,
            lifecycle_status="active",
            silent=False,
        )
        session.add(ev)
        await session.commit()
        ev_id = ev.id

    # also a second event with a multi-day end_date — exercise the clamp
    async with db.get_session() as session:
        ev2 = Event(
            title="Выставка \"Кёнигсберг 1900\"",
            description="Многодневная выставка.",
            short_description="Выставка",
            search_digest="выставка",
            source_text="seed",
            date="2026-05-30",
            end_date="2026-06-05",
            time="10:00",
            location_name="КОНБ",
            city="Калининград",
            photo_urls=["https://example.com/poster2.jpg"],
            photo_count=1,
            creator_id=TIKUN_USER_ID,
            lifecycle_status="active",
            silent=False,
        )
        session.add(ev2)
        await session.commit()
        ev2_id = ev2.id

    bot = FakeBot()

    from handlers.partner_promo_cmd import (
        handle_partner_promo_callback,
        handle_promo_menu_command,
    )

    tg_user = FakeUser(TIKUN_USER_ID, TIKUN_USERNAME)

    # --- frame 1: /promo menu, partner, no campaigns yet ----------------
    msg = FakeMessage(chat_id=TIKUN_USER_ID, from_user=tg_user, text="/promo")
    await handle_promo_menu_command(msg, db, bot)  # type: ignore[arg-type]
    rec = bot.messages[-1]
    print_frame(
        "1. /promo — партнёрский пустой экран",
        text=rec.text,
        markup=rec.reply_markup,
    )

    # --- frame 2: 🎬 on event 1 — step 0 (no campaigns yet) ------------
    cb_msg = FakeMessage(chat_id=TIKUN_USER_ID, from_user=tg_user)
    cb = FakeCallback(
        data=f"ppromo:start:{ev_id}", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)  # type: ignore[arg-type]
    text, markup, _ = cb_msg.last_edit or (
        bot.messages[-1].text,
        bot.messages[-1].reply_markup,
        None,
    )
    print_frame("2. 🎬 → шаг 0 (список кампаний по событию)", text=text, markup=markup)

    # --- frame 3: «Новая промо-кампания» = step 1 (surface) -----------
    cb = FakeCallback(
        data=f"ppromo:new:{ev_id}", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame("3. Шаг 1/6 — выбор поверхности", text=text, markup=markup)

    # --- frame 4: pick popular_review -----------------------------------
    cb = FakeCallback(
        data=f"ppromo:surface:{ev_id}:video_general:popular_review",
        message=cb_msg,
        from_user=tg_user,
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame("4. Шаг 2/6 — выбор слота", text=text, markup=markup)

    # --- frame 5: choose first_two_slots --------------------------------
    cb = FakeCallback(
        data=f"ppromo:slot:{ev_id}:first_two_slots",
        message=cb_msg,
        from_user=tg_user,
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame("5. Шаг 3/6 — количество показов", text=text, markup=markup)

    # --- frame 6: count=3 -----------------------------------------------
    cb = FakeCallback(
        data=f"ppromo:count:{ev_id}:3", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame("6. Шаг 4/6 — дата окончания", text=text, markup=markup)

    # --- frame 7: end=event ---------------------------------------------
    cb = FakeCallback(
        data=f"ppromo:end:{ev_id}:event", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame("7. Шаг 5/6 — режим", text=text, markup=markup)

    # --- frame 8: mode=partner ------------------------------------------
    cb = FakeCallback(
        data=f"ppromo:mode:{ev_id}:partner", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame("8. Шаг 6/6 — подтверждение", text=text, markup=markup)

    # --- frame 9: confirm ------------------------------------------------
    cb = FakeCallback(
        data=f"ppromo:confirm:{ev_id}", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame("9. Кампания создана — итоговое сообщение", text=text, markup=markup)

    # --- frame 10: /promo now non-empty ---------------------------------
    bot.messages.clear()
    msg = FakeMessage(chat_id=TIKUN_USER_ID, from_user=tg_user, text="/promo")
    await handle_promo_menu_command(msg, db, bot)
    rec = bot.messages[-1]
    print_frame(
        "10. /promo — кампания появилась в меню партнёра",
        text=rec.text,
        markup=rec.reply_markup,
    )

    # Extract first campaign button (callback data ppromo:view:N)
    btn_cid = None
    for row in rec.reply_markup.inline_keyboard:
        for b in row:
            if b.callback_data and b.callback_data.startswith("ppromo:view:"):
                btn_cid = int(b.callback_data.split(":")[2])
                break
        if btn_cid:
            break
    assert btn_cid is not None, "expected a campaign-view button"

    # --- frame 11: campaign card ----------------------------------------
    cb_msg = FakeMessage(chat_id=TIKUN_USER_ID, from_user=tg_user)
    cb = FakeCallback(
        data=f"ppromo:view:{btn_cid}", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame(f"11. Карточка кампании #{btn_cid}", text=text, markup=markup)

    # --- frame 12: pause -----------------------------------------------
    cb = FakeCallback(
        data=f"ppromo:pause:{btn_cid}", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame(
        f"12. После «Пауза» — карточка с кнопкой «Запустить»",
        text=text,
        markup=markup,
    )

    # --- frame 13: stats ------------------------------------------------
    cb = FakeCallback(
        data=f"ppromo:stats:{btn_cid}", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame(f"13. Экран статистики", text=text, markup=markup)

    # --- frame 14: archived menu ----------------------------------------
    cb_msg = FakeMessage(chat_id=TIKUN_USER_ID, from_user=tg_user)
    cb = FakeCallback(
        data="ppromo:menu:archived", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame(
        "14. Архив (по умолчанию пусто, активная не в архиве)",
        text=text,
        markup=markup,
    )

    # --- frame 15: cancel mid-flow on second event ----------------------
    cb_msg = FakeMessage(chat_id=TIKUN_USER_ID, from_user=tg_user)
    cb = FakeCallback(
        data=f"ppromo:start:{ev2_id}", message=cb_msg, from_user=tg_user
    )
    await handle_partner_promo_callback(cb, db, bot)
    text, markup, _ = cb_msg.last_edit
    print_frame(
        f"15. 🎬 на втором событии (выставка с end_date — кламп срабатывает в шаге 4)",
        text=text,
        markup=markup,
    )

    os.unlink(path)


if __name__ == "__main__":
    asyncio.run(run())
