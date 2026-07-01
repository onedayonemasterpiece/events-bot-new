from __future__ import annotations

import logging
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

from runtime import require_main_attr
from .config import load_config
from .review import build_surface_keyboard, find_opportunity_by_review_message, format_surface_card, record_feedback, record_surface_feedback
from .service import add_surface_seed, export_feedback_jsonl, latest_report_url, list_surfaces, queue_counts, run_acq_discovery_shadow, surface_counts

logger = logging.getLogger(__name__)
acq_router = Router(name="subscriber_acquisition")


def _menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Запуск", callback_data="acqmenu:run"), InlineKeyboardButton(text="📥 Очередь", callback_data="acqmenu:queue")],
        [InlineKeyboardButton(text="🧭 Места", callback_data="acqmenu:surfaces"), InlineKeyboardButton(text="📄 Отчёт", callback_data="acqmenu:report")],
        [InlineKeyboardButton(text="⬇️ Экспорт", callback_data="acqmenu:export")],
    ])


async def _db_and_auth(message_or_callback) -> tuple[object | None, bool]:
    get_db = require_main_attr("get_db")
    has_admin_access = require_main_attr("has_admin_access")
    db = get_db()
    if db is None:
        return None, False
    user_id = getattr(getattr(message_or_callback, "from_user", None), "id", None)
    if user_id is None:
        return db, False
    from models import User
    async with db.get_session() as session:
        user = await session.get(User, int(user_id))
    return db, bool(has_admin_access(user))


@acq_router.message(Command("acq"))
async def cmd_acq(message: Message) -> None:
    db, ok = await _db_and_auth(message)
    if not db:
        await message.answer("❌ База данных ещё не готова")
        return
    if not ok:
        await message.answer("❌ Команда доступна только администраторам")
        return
    q = await queue_counts(db)
    s = await surface_counts(db)
    text = (
        "🧭 <b>Subscriber Acquisition Discovery</b>\n"
        f"Очередь: pending={q.get('pending', 0)} approved={q.get('approved', 0)} rejected={q.get('rejected', 0)} keep={q.get('keep', 0)}\n"
        f"Места: candidate={s.get('candidate', 0)} approved={s.get('approved', 0)} rejected={s.get('rejected', 0)} paused={s.get('paused', 0)}"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=_menu())


@acq_router.message(Command("acq_run"))
async def cmd_acq_run(message: Message) -> None:
    db, ok = await _db_and_auth(message)
    if not db or not ok:
        await message.answer("❌ Команда доступна только администраторам")
        return
    cfg = load_config()
    bot = require_main_attr("get_bot")() or message.bot
    await message.answer("▶️ Запускаю acquisition discovery в shadow mode…")
    try:
        result = await run_acq_discovery_shadow(db, bot=bot, config=cfg)
    except Exception as exc:
        logger.exception("acq_run failed")
        await message.answer(f"❌ Ошибка acq_run: {exc}")
        return
    report = result.run.telegraph_url or "—"
    await message.answer(
        f"✅ Готово: run #{result.run.id}\n"
        f"Места: {len(result.surfaces)}\n"
        f"Кандидаты: {len(result.opportunities)}\n"
        f"Отчёт: {report}",
        disable_web_page_preview=True,
    )


@acq_router.message(Command("acq_queue"))
async def cmd_acq_queue(message: Message) -> None:
    db, ok = await _db_and_auth(message)
    if not db or not ok:
        await message.answer("❌ Команда доступна только администраторам")
        return
    q = await queue_counts(db)
    await message.answer("📥 Очередь: " + ", ".join(f"{k}={v}" for k, v in sorted(q.items())) or "пусто")


@acq_router.message(Command("acq_surfaces"))
async def cmd_acq_surfaces(message: Message) -> None:
    db, ok = await _db_and_auth(message)
    if not db or not ok:
        await message.answer("❌ Команда доступна только администраторам")
        return
    counts = await surface_counts(db)
    text = ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "пусто"
    await message.answer("🧭 Места: " + text)
    surfaces = await list_surfaces(db, limit=10)
    for surface in surfaces:
        await message.answer(format_surface_card(surface), parse_mode="HTML", reply_markup=build_surface_keyboard(surface), disable_web_page_preview=True)


@acq_router.message(Command("acq_surface_add"))
async def cmd_acq_surface_add(message: Message) -> None:
    db, ok = await _db_and_auth(message)
    if not db or not ok:
        await message.answer("❌ Команда доступна только администраторам")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /acq_surface_add https://t.me/... или https://vk.com/...")
        return
    try:
        surface = await add_surface_seed(db, parts[1], reviewer_id=int(message.from_user.id) if message.from_user else None)
    except Exception as exc:
        await message.answer(f"❌ Не удалось добавить surface: {exc}")
        return
    await message.answer(format_surface_card(surface), parse_mode="HTML", reply_markup=build_surface_keyboard(surface), disable_web_page_preview=True)


@acq_router.message(Command("acq_report"))
async def cmd_acq_report(message: Message) -> None:
    db, ok = await _db_and_auth(message)
    if not db or not ok:
        await message.answer("❌ Команда доступна только администраторам")
        return
    url = await latest_report_url(db)
    await message.answer(f"📄 Последний отчёт: {url or '—'}", disable_web_page_preview=True)


@acq_router.message(Command("acq_export"))
async def cmd_acq_export(message: Message) -> None:
    db, ok = await _db_and_auth(message)
    if not db or not ok:
        await message.answer("❌ Команда доступна только администраторам")
        return
    data = await export_feedback_jsonl(db)
    if not data:
        await message.answer("⬇️ Feedback пока пуст")
        return
    import tempfile
    from pathlib import Path
    path = Path(tempfile.gettempdir()) / "acq_feedback.jsonl"
    path.write_text(data, encoding="utf-8")
    await message.answer_document(FSInputFile(path), caption="⬇️ acq feedback JSONL")


@acq_router.callback_query(lambda c: bool(c.data) and c.data.startswith("acq:"))
async def acq_review_callback(callback: CallbackQuery) -> None:
    db, ok = await _db_and_auth(callback)
    if not db or not ok:
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        _, action, raw_id = str(callback.data).split(":", 2)
        opp_id = int(raw_id)
    except Exception:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return
    if action == "comment":
        await callback.answer(
            "Чтобы сохранить причину, ответьте обычным сообщением-реплаем на эту карточку. Например: «рекламный пост, нужен комментарий».",
            show_alert=True,
        )
        return
    await record_feedback(
        db,
        opportunity_id=opp_id,
        reviewer_id=int(callback.from_user.id) if callback.from_user else None,
        action=action,
        review_message_chat_id=int(callback.message.chat.id) if callback.message else None,
        review_message_id=int(callback.message.message_id) if callback.message else None,
    )
    label = {"approve": "✅ Да", "reject": "❌ Нет", "keep": "🕒 Потом"}.get(action, action)
    if action == "reject":
        await callback.answer("Сохранено: ❌ Нет. Причину можно оставить реплаем на эту карточку.", show_alert=True)
    else:
        await callback.answer(f"Сохранено: {label}")


@acq_router.callback_query(lambda c: bool(c.data) and c.data.startswith("acqsurf:"))
async def acq_surface_callback(callback: CallbackQuery) -> None:
    db, ok = await _db_and_auth(callback)
    if not db or not ok:
        await callback.answer("Нет доступа", show_alert=True)
        return
    try:
        _, action, raw_id = str(callback.data).split(":", 2)
        surface_id = int(raw_id)
    except Exception:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return
    if action == "comment":
        await callback.answer(
            "Для surface-причины ответьте реплаем на карточку или используйте экспорт feedback; отдельное поле причины будет добавлено в следующий UI-pass.",
            show_alert=True,
        )
        return
    try:
        await record_surface_feedback(
            db,
            surface_id=surface_id,
            reviewer_id=int(callback.from_user.id) if callback.from_user else None,
            action=action,
        )
    except Exception as exc:
        await callback.answer(f"Ошибка: {exc}", show_alert=True)
        return
    label = {"approve": "✅ Да", "reject": "❌ Нет", "pause": "🕒 Потом"}.get(action, action)
    if action == "reject":
        await callback.answer("Surface сохранён: ❌ Нет. Причину можно оставить реплаем на карточку.", show_alert=True)
    else:
        await callback.answer(f"Surface сохранён: {label}")


@acq_router.callback_query(lambda c: bool(c.data) and c.data.startswith("acqmenu:"))
async def acq_menu_callback(callback: CallbackQuery) -> None:
    action = str(callback.data).split(":", 1)[1]
    if not callback.message:
        await callback.answer()
        return
    db, ok = await _db_and_auth(callback)
    if not db or not ok:
        await callback.answer("Нет доступа", show_alert=True)
        return
    if action == "run":
        cfg = load_config()
        bot = require_main_attr("get_bot")() or callback.message.bot
        await callback.message.answer("▶️ Запускаю acquisition discovery в shadow mode…")
        try:
            result = await run_acq_discovery_shadow(db, bot=bot, config=cfg)
        except Exception as exc:
            logger.exception("acq menu run failed")
            await callback.message.answer(f"❌ Ошибка acq_run: {exc}")
            await callback.answer()
            return
        await callback.message.answer(
            f"✅ Готово: run #{result.run.id}\n"
            f"Места: {len(result.surfaces)}\n"
            f"Кандидаты: {len(result.opportunities)}\n"
            f"Отчёт: {result.run.telegraph_url or '—'}",
            disable_web_page_preview=True,
        )
    elif action == "queue":
        q = await queue_counts(db)
        text = ", ".join(f"{k}={v}" for k, v in sorted(q.items())) or "пусто"
        await callback.message.answer("📥 Очередь: " + text)
    elif action == "surfaces":
        s = await surface_counts(db)
        text = ", ".join(f"{k}={v}" for k, v in sorted(s.items())) or "пусто"
        await callback.message.answer("🧭 Места: " + text)
        for surface in await list_surfaces(db, limit=10):
            await callback.message.answer(format_surface_card(surface), parse_mode="HTML", reply_markup=build_surface_keyboard(surface), disable_web_page_preview=True)
    elif action == "report":
        url = await latest_report_url(db)
        await callback.message.answer(f"📄 Последний отчёт: {url or '—'}", disable_web_page_preview=True)
    elif action == "export":
        data = await export_feedback_jsonl(db)
        if not data:
            await callback.message.answer("⬇️ Feedback пока пуст")
        else:
            import tempfile
            from pathlib import Path
            path = Path(tempfile.gettempdir()) / "acq_feedback.jsonl"
            path.write_text(data, encoding="utf-8")
            await callback.message.answer_document(FSInputFile(path), caption="⬇️ acq feedback JSONL")
    await callback.answer()


@acq_router.message(F.reply_to_message)
async def acq_reply_comment(message: Message) -> None:
    cfg = load_config()
    if not cfg.review_chat_id or int(message.chat.id) != int(cfg.review_chat_id):
        return
    reply = message.reply_to_message
    if not reply:
        return
    get_db = require_main_attr("get_db")
    db = get_db()
    if db is None:
        return
    opp = await find_opportunity_by_review_message(db, chat_id=int(reply.chat.id), message_id=int(reply.message_id))
    if opp is None:
        return
    await record_feedback(
        db,
        opportunity_id=opp.id,
        reviewer_id=int(message.from_user.id) if message.from_user else None,
        action="comment",
        note=message.text or message.caption or "",
        review_message_chat_id=int(reply.chat.id),
        review_message_id=int(reply.message_id),
    )
    await message.reply("💬 Комментарий сохранён")
