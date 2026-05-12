from __future__ import annotations

import os
import re

from aiogram import Router, types
from aiogram.filters import Command, CommandObject

from kenigsberg_stories.state import (
    apply_generated_timeline_bans,
    format_bans_report,
    load_state,
    load_thoughts,
    parse_second_ranges,
    reset_bans,
)
from runtime import require_main_attr

kenigsberg_stories_router = Router(name="kenigsberg_stories")


async def _require_superadmin(message: types.Message) -> bool:
    get_db = require_main_attr("get_db")
    db = get_db()
    if db is None:
        await message.answer("Бот ещё не инициализирован. Попробуйте позже.")
        return False
    from models import User

    async with db.get_session() as session:
        user = await session.get(User, message.from_user.id)
    if not require_main_attr("has_admin_access")(user):
        await message.answer("Not authorized")
        return False
    return True


def _launch_enabled() -> bool:
    raw = (os.getenv("KENIGSBERG_STORIES_KAGGLE_ENABLED") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _extract_ban_args(args: str) -> tuple[int | None, str]:
    text = (args or "").strip()
    issue_match = re.search(r"#?\s*(\d+)", text)
    if not issue_match:
        return None, ""
    issue_number = int(issue_match.group(1))
    tail = text[issue_match.end() :].strip()
    tail = re.sub(r"^(?:бан|bans?|отрезк(?:и|ов)?|секунд(?:ы)?)\s*", "", tail, flags=re.I).strip()
    return issue_number, tail


async def _handle_ban_command(message: types.Message, args: str) -> None:
    db = require_main_attr("get_db")()
    issue_number, ranges_text = _extract_ban_args(args)
    if issue_number is None:
        await message.answer("Формат: /kenigsberg ban #15 1-3, 7, 16-17")
        return
    ranges = parse_second_ranges(ranges_text)
    if not ranges:
        await message.answer("Не нашёл диапазоны секунд. Пример: 1-3, 7, 16-17")
        return
    mapped, message_text = await apply_generated_timeline_bans(
        db,
        issue_number=issue_number,
        ranges=ranges,
    )
    if not mapped:
        await message.answer(message_text)
        return
    lines = [message_text]
    for item in mapped[:10]:
        lines.append(
            "{dataset} {source_file}: {source_start:.2f}-{source_end:.2f}s".format(
                **item
            )
        )
    if len(mapped) > 10:
        lines.append(f"... и ещё {len(mapped) - 10}")
    await message.answer("\n".join(lines))


async def _handle_launch(message: types.Message) -> None:
    thoughts = load_thoughts()
    if not _launch_enabled():
        await message.answer(
            "Kenigsberg Stories MVP подготовлен, но Kaggle запуск сейчас выключен.\n"
            "Флаг: KENIGSBERG_STORIES_KAGGLE_ENABLED=1.\n"
            f"Мыслей в пуле: {len(thoughts)}.\n\n"
            "Доступно:\n"
            "/kenigsberg status\n"
            "/kenigsberg bans\n"
            "/kenigsberg ban #15 1-3, 7, 16-17\n"
            "/kenigsberg bans reset"
        )
        return
    await message.answer(
        "Kaggle launch для Kenigsberg Stories ещё не включён в этом safety-слое. "
        "Следующий шаг: подключить per-run dataset + kernel handoff после отдельного подтверждения."
    )


@kenigsberg_stories_router.message(Command("kenigsberg"))
async def cmd_kenigsberg(message: types.Message, command: CommandObject) -> None:
    if not await _require_superadmin(message):
        return
    args = (command.args or "").strip()
    lowered = args.casefold()
    db = require_main_attr("get_db")()

    if not args or lowered in {"start", "run", "generate", "сгенерируй", "запуск"}:
        await _handle_launch(message)
        return
    if lowered in {"status", "статус"}:
        state = await load_state(db)
        thoughts = load_thoughts()
        await message.answer(
            "Kenigsberg Stories\n"
            f"launch_enabled={_launch_enabled()}\n"
            f"next_issue=#{state.get('next_issue', 1)}\n"
            f"thoughts={len(thoughts)} used={len(state.get('used_thought_ids') or [])}\n"
            f"issues={len(state.get('issues') or {})} bans={len(state.get('source_bans') or [])}"
        )
        return
    if lowered in {"bans", "bans list", "покажи баны", "список банов"}:
        await message.answer(format_bans_report(await load_state(db)))
        return
    if lowered in {"bans reset", "ban reset", "сбрось баны", "reset bans"}:
        await reset_bans(db)
        await message.answer("Баны Kenigsberg Stories сброшены.")
        return
    if lowered.startswith("ban") or lowered.startswith("бан"):
        tail = re.sub(r"^(?:ban|бан)\s*", "", args, flags=re.I).strip()
        await _handle_ban_command(message, tail)
        return

    await message.answer(
        "Формат:\n"
        "/kenigsberg\n"
        "/kenigsberg status\n"
        "/kenigsberg bans\n"
        "/kenigsberg ban #15 1-3, 7, 16-17\n"
        "/kenigsberg bans reset"
    )
