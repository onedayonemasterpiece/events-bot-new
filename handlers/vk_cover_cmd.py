from __future__ import annotations

import html
import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message

from runtime import require_main_attr
from vk_dynamic_cover import (
    SETTINGS_ACTIVE_UNTIL,
    build_cover_pack,
    is_dynamic_cover_enabled,
    load_default_cover_state,
    load_cover_history,
    restore_default_cover_if_expired,
    save_current_cover_as_default,
    set_dynamic_cover_enabled,
)

logger = logging.getLogger(__name__)

vk_cover_router = Router(name="vk_cover")


async def _require_superadmin(db, user_id: int) -> bool:  # noqa: ANN001 - runtime db
    from models import User

    async with db.get_session() as session:
        user = await session.get(User, int(user_id))
        return bool(user and not user.blocked and user.is_superadmin)


def _args(message: Message) -> list[str]:
    return (message.text or "").strip().split()[1:]


def _cover_keyboard(*, enabled: bool | None = None, has_default: bool | None = None) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="👁 Preview", callback_data="cover:preview"),
            InlineKeyboardButton(text="📨 На согласование", callback_data="cover:request"),
        ],
        [
            InlineKeyboardButton(text="💾 Сохранить дефолт", callback_data="cover:save_default"),
        ],
        [
            InlineKeyboardButton(text="↩️ Восстановить дефолт", callback_data="cover:restore"),
            InlineKeyboardButton(
                text="⏸ Off" if enabled else "▶️ On",
                callback_data="cover:off" if enabled else "cover:on",
            ),
        ],
        [
            InlineKeyboardButton(text="🕘 История", callback_data="cover:history"),
            InlineKeyboardButton(text="🔄 Статус", callback_data="cover:status"),
        ],
    ]
    if has_default is False:
        rows[2][0] = InlineKeyboardButton(text="↩️ Нет дефолта", callback_data="cover:restore")
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _send_pack_preview(message: Message, pack, *, caption_prefix: str) -> None:  # noqa: ANN001
    item_lines = "\n".join(
        f"• {html.escape(item.title)} — {html.escape(item.period)}"
        for item in pack.items
    )
    await message.answer_document(
        FSInputFile(str(pack.wide_path), filename="vk-cover-wide-1920x768.png"),
        caption=f"{caption_prefix}\nWide 1920×768\n{item_lines}",
        parse_mode="HTML",
    )
    for idx, path in enumerate(pack.mobile_paths[:2], start=1):
        await message.answer_document(
            FSInputFile(str(path), filename=f"vk-cover-mobile-{idx}-1080x1920.png"),
            caption=f"Mobile live cover preview {idx}/1080×1920",
        )
    if len(pack.mobile_paths) > 2:
        await message.answer(f"Ещё mobile slides: {len(pack.mobile_paths) - 2} сгенерированы локально рядом с preview.")


def _usage() -> str:
    return (
        "/cover status\n"
        "/cover preview\n"
        "/cover request\n"
        "/cover apply  # alias: генерирует предложение без публикации\n"
        "/cover save_default\n"
        "/cover restore\n"
        "/cover history\n"
        "/cover on | off"
    )


async def _cover_action(message: Message, action: str, *, db, bot, user_id: int) -> None:  # noqa: ANN001
    get_setting_value = require_main_attr("get_setting_value")
    action = action.strip().lower().replace("-", "_")

    if action in {"help", "h", "?"}:
        enabled = await is_dynamic_cover_enabled(db)
        default_state = await load_default_cover_state(db)
        await message.answer(_usage(), reply_markup=_cover_keyboard(enabled=enabled, has_default=bool(default_state)))
        return

    if action == "status":
        enabled = await is_dynamic_cover_enabled(db)
        active_until = await get_setting_value(db, SETTINGS_ACTIVE_UNTIL)
        history = await load_cover_history(db)
        default_state = await load_default_cover_state(db)
        default_line = "не сохранён"
        if default_state:
            default_line = f"{default_state.get('saved_at', '—')} · {default_state.get('path', '—')}"
        lines = [
            f"VK dynamic cover: {'ON' if enabled else 'OFF'}",
            f"Активна до: {active_until or 'не установлено'}",
            f"Дефолт: {default_line}",
            f"История: {len(history)} записей",
            "",
            _usage(),
        ]
        await message.answer(
            "\n".join(lines),
            reply_markup=_cover_keyboard(enabled=enabled, has_default=bool(default_state)),
        )
        return

    if action == "on":
        await set_dynamic_cover_enabled(db, True)
        default_state = await load_default_cover_state(db)
        await message.answer(
            "✅ VK dynamic cover включена.",
            reply_markup=_cover_keyboard(enabled=True, has_default=bool(default_state)),
        )
        return

    if action == "off":
        await set_dynamic_cover_enabled(db, False)
        default_state = await load_default_cover_state(db)
        await message.answer(
            "⏸ VK dynamic cover выключена. Промо не будет менять обложку, пока не включишь обратно.",
            reply_markup=_cover_keyboard(enabled=False, has_default=bool(default_state)),
        )
        return

    if action == "history":
        history = await load_cover_history(db)
        default_state = await load_default_cover_state(db)
        if not history:
            await message.answer(
                "История смен пока пустая.",
                reply_markup=_cover_keyboard(
                    enabled=await is_dynamic_cover_enabled(db),
                    has_default=bool(default_state),
                ),
            )
            return
        lines = ["История VK cover:"]
        for row in history[:10]:
            items = row.get("items") or []
            names = ", ".join(str(i.get("title", "")) for i in items[:3] if isinstance(i, dict))
            lines.append(
                f"• {row.get('applied_at') or row.get('saved_at') or row.get('restored_at') or '—'}"
                f" · {row.get('kind', '—')} · {names or row.get('path') or 'без названий'}"
            )
        await message.answer(
            "\n".join(lines),
            reply_markup=_cover_keyboard(
                enabled=await is_dynamic_cover_enabled(db),
                has_default=bool(default_state),
            ),
        )
        return

    if action == "preview":
        pack = await build_cover_pack(db)
        await _send_pack_preview(message, pack, caption_prefix="Preview VK dynamic cover")
        return

    if action in {"request", "proposal", "propose", "apply"}:
        if action == "apply":
            status = await message.answer(
                "Генерирую предложение VK cover без публикации. "
                "Прямой apply отключён до отдельного approval-flow."
            )
        else:
            status = await message.answer("Генерирую предложение VK cover на согласование…")
        pack = await build_cover_pack(db)
        await _send_pack_preview(
            message,
            pack,
            caption_prefix="🖼 VK cover proposal · без публикации",
        )
        try:
            await status.delete()
        except Exception:
            pass
        return

    if action in {"save_default", "default", "save"}:
        status = await message.answer("Сохраняю текущую VK-обложку как дефолт на сервере…")
        state = await save_current_cover_as_default(db, bot=bot, operator_id=user_id)
        try:
            await status.delete()
        except Exception:
            pass
        await message.answer(
            f"✅ Текущая VK-обложка сохранена как дефолт.\n{state.get('path')}",
            reply_markup=_cover_keyboard(
                enabled=await is_dynamic_cover_enabled(db),
                has_default=True,
            ),
        )
        return

    if action == "restore":
        changed = await restore_default_cover_if_expired(db, bot=bot, force=True)
        default_state = await load_default_cover_state(db)
        if changed:
            text = "✅ Сохранённая дефолтная VK-обложка восстановлена."
        elif default_state:
            text = "❌ Не удалось восстановить сохранённый дефолт: файл не найден или VK вернул ошибку."
        else:
            text = "Сначала сохрани текущую VK-обложку кнопкой «Сохранить дефолт»."
        await message.answer(
            text,
            reply_markup=_cover_keyboard(
                enabled=await is_dynamic_cover_enabled(db),
                has_default=bool(default_state),
            ),
        )
        return

    await message.answer("Неизвестное действие.\n" + _usage())


@vk_cover_router.message(Command("cover"))
async def cmd_cover(message: Message) -> None:
    get_db = require_main_attr("get_db")
    get_bot = require_main_attr("get_bot")
    db = get_db()
    bot = get_bot()
    if db is None:
        await message.answer("❌ База данных ещё не инициализирована.")
        return
    try:
        user_id = int(message.from_user.id)
    except Exception:
        await message.answer("❌ Не удалось определить пользователя.")
        return
    if not await _require_superadmin(db, user_id):
        await message.answer("❌ Команда доступна только суперадминам.")
        return

    args = _args(message)
    action = (args[0].lower() if args else "status").strip()

    try:
        await _cover_action(message, action, db=db, bot=bot, user_id=user_id)
    except Exception as exc:
        logger.exception("cover command failed action=%s", action)
        await message.answer(f"❌ /cover завершилась с ошибкой: {exc}")


@vk_cover_router.callback_query(F.data.startswith("cover:"))
async def cb_cover(callback: CallbackQuery) -> None:
    get_db = require_main_attr("get_db")
    get_bot = require_main_attr("get_bot")
    db = get_db()
    bot = get_bot()
    if db is None or callback.message is None:
        await callback.answer("База ещё не готова", show_alert=True)
        return
    try:
        user_id = int(callback.from_user.id)
    except Exception:
        await callback.answer("Не удалось определить пользователя", show_alert=True)
        return
    if not await _require_superadmin(db, user_id):
        await callback.answer("Только для суперадминов", show_alert=True)
        return
    action = str(callback.data or "").split(":", 1)[1] or "status"
    await callback.answer()
    try:
        await _cover_action(callback.message, action, db=db, bot=bot, user_id=user_id)
    except Exception as exc:
        logger.exception("cover callback failed action=%s", action)
        await callback.message.answer(f"❌ Cover action завершилась с ошибкой: {exc}")
