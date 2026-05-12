from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from aiogram import Router, types
from aiogram.filters import Command, CommandObject
from sqlalchemy import select

from kenigsberg_stories.state import (
    KENIGSBERG_PROFILE_KEY,
    apply_generated_timeline_bans,
    choose_next_thought,
    format_bans_report,
    load_state,
    load_thoughts,
    parse_second_ranges,
    recent_source_exclusions,
    reserve_issue_number,
    reset_bans,
)
from models import Channel, VideoAnnounceSession, VideoAnnounceSessionStatus
from runtime import require_main_attr
from video_announce.kaggle_client import (
    KaggleClient,
    await_dataset_ready,
    await_kernel_dataset_sources,
)
from video_announce.poller import (
    VIDEO_KAGGLE_TIMEOUT_MINUTES,
    remember_status_message,
    start_kernel_poller_task,
    update_status_message,
)

kenigsberg_stories_router = Router(name="kenigsberg_stories")
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
KAGGLE_READY_WAIT_SECONDS = max(
    30,
    int(os.getenv("KENIGSBERG_KAGGLE_READY_WAIT_SECONDS", "180")),
)
KAGGLE_BIND_WAIT_SECONDS = max(
    10,
    int(os.getenv("KENIGSBERG_KAGGLE_BIND_WAIT_SECONDS", "120")),
)

class StoryTextPreparationError(RuntimeError):
    pass


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


def _canonicalize_ban_args(args: str) -> str:
    text = " ".join(str(args or "").split())
    if not text:
        return ""
    lowered = text.casefold()
    if any(word in lowered for word in ("покажи", "список", "вывед", "list")) and (
        "бан" in lowered or "ban" in lowered
    ):
        return "bans"
    if lowered in {"бан", "ban"}:
        return "ban"
    if re.match(r"^(?:ban|бан)\b", text, flags=re.IGNORECASE):
        return text
    if "бан" not in lowered and "ban" not in lowered:
        return text
    issue_match = re.search(r"#?\s*(\d+)", text)
    range_match = re.search(r"(?:бан|ban)\s+(.+)$", text, flags=re.IGNORECASE)
    if issue_match and range_match:
        issue = int(issue_match.group(1))
        ranges_text = range_match.group(1).strip()
        if ranges_text:
            return f"ban #{issue} {ranges_text}"
    return text


def _split_final_thought_text(thought_text: str) -> list[str]:
    """Split the curated thought into readable screens without rewriting it."""
    text = " ".join(str(thought_text or "").split())
    if not text:
        return []

    sentence_parts = [
        item.strip()
        for item in re.split(r"(?<=[.!?])\s+", text)
        if item.strip()
    ]
    if len(sentence_parts) >= 2:
        return sentence_parts[:5]

    # Long one-sentence thoughts still need breathing room on screen. Split on
    # strong punctuation, preserving the punctuation on the preceding part.
    parts: list[str] = []
    current = ""
    for token in re.split(r"([:;—])", text):
        if not token:
            continue
        if token in {":", ";", "—"}:
            current = f"{current}{token}".strip()
            continue
        candidate = f"{current} {token}".strip() if current else token.strip()
        if len(candidate) > 90 and current:
            parts.append(current)
            current = token.strip()
        else:
            current = candidate
    if current:
        parts.append(current)
    return [part for part in parts if part][:5] or [text]


def _prepare_story_text_from_thought(thought_text: str) -> dict:
    lines = _split_final_thought_text(thought_text)
    if not lines:
        raise StoryTextPreparationError("empty thought text")
    return {
        "hook": lines[0][:96],
        "scene_lines": lines,
        "caption": thought_text[:240],
        "source": "thoughts_md",
    }


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
    db = require_main_attr("get_db")()
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
    await _launch_kaggle_generation(message, db, thoughts_count=len(thoughts))


async def _resolve_channel_id(db, username: str) -> int | None:
    raw_env = (os.getenv("KENIGSBERG_STORIES_TEST_CHAT_ID") or "").strip()
    if raw_env:
        try:
            return int(raw_env)
        except ValueError:
            pass
    normalized = username.strip().lstrip("@").casefold()
    async with db.get_session() as session:
        result = await session.execute(select(Channel))
        for channel in result.scalars().all():
            if str(channel.username or "").strip().lstrip("@").casefold() == normalized:
                return int(channel.channel_id)
    return None


def _copy_required_assets(tmp_path: Path) -> None:
    files = [
        (PROJECT_ROOT / "scripts" / "render_kenigsberg_story.py", "scripts/render_kenigsberg_story.py"),
        (
            PROJECT_ROOT / "docs" / "features" / "kenigsberg-stories" / "thoughts.md",
            "docs/features/kenigsberg-stories/thoughts.md",
        ),
        (
            PROJECT_ROOT / "kaggle" / "CherryFlash" / "video_announce" / "assets" / "BebasNeue-Bold.ttf",
            "assets/BebasNeue-Bold.ttf",
        ),
    ]
    for name in ("Cygre-Regular.ttf", "Cygre-Medium.ttf", "Cygre-SemiBold.ttf", "Cygre-Bold.ttf"):
        files.append(
            (
                PROJECT_ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts" / name,
                f"assets/ro_znanie_fonts/{name}",
            )
        )
    for src, rel in files:
        if not src.exists():
            raise RuntimeError(f"Missing Kenigsberg runtime asset: {src}")
        dest = tmp_path / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)


async def _create_kenigsberg_dataset(db, *, session_id: int, payload: dict) -> str:
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME not set")
    run_suffix = f"{session_id}-{int(time.time())}"
    dataset_id = f"{username}/kenigsberg-session-{run_suffix}"
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "dataset-metadata.json").write_text(
            json.dumps(
                {
                    "title": f"Kenigsberg Story Session {session_id}",
                    "id": dataset_id,
                    "licenses": [{"name": "CC0-1.0"}],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (tmp_path / "payload.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _copy_required_assets(tmp_path)
        (tmp_path / "bundle_manifest.json").write_text(
            json.dumps(
                {
                    "session_id": session_id,
                    "mode": KENIGSBERG_PROFILE_KEY,
                    "files": sorted(
                        str(path.relative_to(tmp_path))
                        for path in tmp_path.rglob("*")
                        if path.is_file()
                    ),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        await asyncio.to_thread(KaggleClient().create_dataset, tmp_path)
    return dataset_id


async def _launch_kaggle_generation(message: types.Message, db, *, thoughts_count: int) -> None:
    existing = None
    async with db.get_session() as session:
        result = await session.execute(
            select(VideoAnnounceSession)
            .where(
                VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING,
                VideoAnnounceSession.profile_key == KENIGSBERG_PROFILE_KEY,
            )
            .order_by(VideoAnnounceSession.id.desc())
            .limit(1)
        )
        existing = result.scalar_one_or_none()
    if existing:
        kaggle_state = ""
        kernel_ref = str(existing.kaggle_kernel_ref or "").strip()
        if kernel_ref and not kernel_ref.startswith("local:"):
            try:
                status = await asyncio.to_thread(KaggleClient().get_kernel_status, kernel_ref)
                raw_status = str(status.get("status") or "").strip()
                if raw_status:
                    kaggle_state = f" Kaggle={raw_status}."
            except Exception:
                logger.warning(
                    "kenigsberg: failed to fetch active session kaggle status session=%s",
                    existing.id,
                    exc_info=True,
                )
        if "complete" in kaggle_state.casefold():
            await message.answer(
                f"Сессия #{existing.id} уже завершилась на Kaggle, бот забирает и публикует результат."
                f"{kaggle_state} Дождитесь сообщения с видео/логами."
            )
        else:
            await message.answer(
                f"Сессия #{existing.id} ещё обрабатывается.{kaggle_state} "
                "Новый Kenigsberg запуск начнётся после финализации текущего."
            )
        return

    test_chat_id = await _resolve_channel_id(db, "keniggpt")
    if not test_chat_id:
        await message.answer("Не найден test target @keniggpt в таблице channel и KENIGSBERG_STORIES_TEST_CHAT_ID.")
        return

    issue_number = await reserve_issue_number(db)
    thought = await choose_next_thought(db)
    seed = (secrets.randbits(63) ^ time.time_ns() ^ issue_number) & ((1 << 63) - 1)
    state = await load_state(db)
    thought_text = str(thought.get("text") or "")
    await message.answer(
        f"Kenigsberg #{issue_number}: принял команду, готовлю Kaggle, "
        f"thought={thought.get('id') or '-'} / {thoughts_count}."
    )
    logger.info(
        "kenigsberg: launch accepted issue=%s thought=%s chat_id=%s user_id=%s",
        issue_number,
        thought.get("id") or "",
        message.chat.id if message.chat else None,
        message.from_user.id if message.from_user else None,
    )
    try:
        story_text = _prepare_story_text_from_thought(thought_text)
    except StoryTextPreparationError as exc:
        logger.warning(
            "kenigsberg: launch aborted issue=%s thought=%s reason=%s",
            issue_number,
            thought.get("id") or "",
            exc,
        )
        await message.answer(
            f"Kenigsberg #{issue_number}: не нашёл текст мысли в thoughts.md, Kaggle не запускаю."
        )
        return
    payload = {
        "issue_number": issue_number,
        "seed": seed,
        "profile_key": KENIGSBERG_PROFILE_KEY,
        "thought_id": thought.get("id") or "",
        "thought_text": thought_text,
        "hook": story_text.get("hook") or "",
        "scene_lines": story_text.get("scene_lines") or [],
        "caption": story_text.get("caption") or "",
        "text_source": story_text.get("source") or "",
        "crop_bottom_px": int(os.getenv("KENIGSBERG_STORIES_CROP_BOTTOM_PX", "96")),
        "source_bans": [
            *(state.get("source_bans") or []),
            *recent_source_exclusions(state),
        ],
        "target": "https://t.me/keniggpt",
        "strategy": "heuristic_v1",
    }
    async with db.get_session() as session:
        obj = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.SELECTED,
            profile_key=KENIGSBERG_PROFILE_KEY,
            selection_params=payload,
            test_chat_id=test_chat_id,
            main_chat_id=None,
            kaggle_kernel_ref="local:KoenigsbergStories",
        )
        session.add(obj)
        await session.commit()
        await session.refresh(obj)

    status_message = await update_status_message(
        message.bot,
        obj,
        {},
        chat_id=message.chat.id,
        allow_send=True,
        note="Kenigsberg: готовим Kaggle",
    )
    status_chat_id = status_message[0] if status_message else message.chat.id
    status_message_id = status_message[1] if status_message else None
    if status_message:
        remember_status_message(obj.id, status_chat_id, status_message_id)

    await message.answer(
        f"Kenigsberg #{issue_number}: запускаю Kaggle MVP, period=auto, "
        f"thought={thought.get('id') or '-'} / {thoughts_count}, "
        f"text={story_text.get('source') or 'unknown'}."
    )
    client = KaggleClient()
    try:
        async with db.get_session() as session:
            fresh = await session.get(VideoAnnounceSession, obj.id)
            fresh.status = VideoAnnounceSessionStatus.RENDERING
            fresh.started_at = datetime.now(timezone.utc)
            session.add(fresh)
            await session.commit()
            await session.refresh(fresh)
            obj = fresh
        dataset_id = await _create_kenigsberg_dataset(db, session_id=obj.id, payload=payload)
        await await_dataset_ready(
            client,
            dataset_id,
            timeout_seconds=KAGGLE_READY_WAIT_SECONDS,
            poll_interval_seconds=5,
            expected_files=["payload.json", "scripts/render_kenigsberg_story.py"],
        )
        kernel_ref = await asyncio.to_thread(
            client.deploy_kernel_update,
            "local:KoenigsbergStories",
            [dataset_id],
        )
        await await_kernel_dataset_sources(
            client,
            kernel_ref,
            [dataset_id],
            timeout_seconds=KAGGLE_BIND_WAIT_SECONDS,
            poll_interval_seconds=10,
        )
        async with db.get_session() as session:
            fresh = await session.get(VideoAnnounceSession, obj.id)
            fresh.kaggle_dataset = dataset_id
            fresh.kaggle_kernel_ref = kernel_ref
            session.add(fresh)
            await session.commit()
            await session.refresh(fresh)
            obj = fresh
    except Exception as exc:
        async with db.get_session() as session:
            fresh = await session.get(VideoAnnounceSession, obj.id)
            if fresh:
                fresh.status = VideoAnnounceSessionStatus.FAILED
                fresh.error = f"kaggle launch failed: {type(exc).__name__}: {exc}"
                session.add(fresh)
                await session.commit()
        await message.answer(f"Kenigsberg #{issue_number}: не удалось запустить Kaggle: {type(exc).__name__}: {exc}")
        return

    start_kernel_poller_task(
        db,
        client,
        obj,
        bot=message.bot,
        notify_chat_id=message.chat.id,
        test_chat_id=test_chat_id,
        main_chat_id=None,
        status_chat_id=status_chat_id,
        status_message_id=status_message_id,
        poll_interval=45,
        timeout_minutes=VIDEO_KAGGLE_TIMEOUT_MINUTES,
        dataset_slug=obj.kaggle_dataset,
    )


@kenigsberg_stories_router.message(Command("kenigsberg"))
async def cmd_kenigsberg(message: types.Message, command: CommandObject) -> None:
    if not await _require_superadmin(message):
        return
    args = (command.args or "").strip()
    args = _canonicalize_ban_args(args)
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
            f"issues={len(state.get('issues') or {})} bans={len(state.get('source_bans') or [])} "
            f"recent_exclusions={len(recent_source_exclusions(state))}"
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
