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
from datetime import datetime, timedelta, timezone
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
    VIDEO_KAGGLE_HANDOFF_GRACE_MINUTES,
    VIDEO_KAGGLE_TIMEOUT_MINUTES,
    remember_status_message,
    start_kernel_poller_task,
    update_status_message,
)
from video_announce.story_publish import (
    STORY_PUBLISH_CIPHER_FILENAME,
    STORY_PUBLISH_CONFIG_FILENAME,
    STORY_PUBLISH_KEY_FILENAME,
    build_story_publish_config,
    write_story_secret_files,
)

kenigsberg_stories_router = Router(name="kenigsberg_stories")
logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
_KENIGSBERG_LAUNCH_LOCK = asyncio.Lock()
_KENIGSBERG_LAUNCH_TASKS: set[asyncio.Task] = set()
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


def _is_sqlite_locked(exc: Exception) -> bool:
    return "database is locked" in str(exc).casefold()


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_local_kernel_ref(kernel_ref: str | None) -> bool:
    return str(kernel_ref or "").strip().startswith("local:")


def _is_stale_local_handoff(
    session_obj: VideoAnnounceSession,
    *,
    now: datetime | None = None,
) -> bool:
    if not _is_local_kernel_ref(session_obj.kaggle_kernel_ref):
        return False
    if session_obj.kaggle_dataset:
        return False
    reference = _as_utc(session_obj.started_at) or _as_utc(session_obj.created_at)
    if reference is None:
        return False
    now_utc = _as_utc(now) or datetime.now(timezone.utc)
    deadline = reference + timedelta(minutes=VIDEO_KAGGLE_HANDOFF_GRACE_MINUTES)
    return now_utc >= deadline


async def _update_video_session_with_retry(db, session_id: int, mutate) -> VideoAnnounceSession | None:
    last_exc: Exception | None = None
    for attempt in range(1, 8):
        try:
            async with db.get_session() as session:
                fresh = await session.get(VideoAnnounceSession, session_id)
                if not fresh:
                    return None
                mutate(fresh)
                session.add(fresh)
                await session.commit()
                await session.refresh(fresh)
                return fresh
        except Exception as exc:
            last_exc = exc
            if not _is_sqlite_locked(exc) or attempt >= 7:
                raise
            logger.warning(
                "kenigsberg: retrying video session update after sqlite lock session=%s attempt=%s",
                session_id,
                attempt,
            )
            await asyncio.sleep(0.35 * attempt)
    if last_exc:
        raise last_exc
    return None


async def _mark_session_failed_with_retry(
    db,
    session_id: int,
    *,
    error: str,
) -> VideoAnnounceSession | None:
    def mutate(obj: VideoAnnounceSession) -> None:
        obj.status = VideoAnnounceSessionStatus.FAILED
        obj.error = error
        obj.finished_at = datetime.now(timezone.utc)

    return await _update_video_session_with_retry(db, session_id, mutate)


async def _mark_session_rendering_with_retry(db, session_id: int) -> VideoAnnounceSession | None:
    def mutate(obj: VideoAnnounceSession) -> None:
        obj.status = VideoAnnounceSessionStatus.RENDERING
        obj.started_at = datetime.now(timezone.utc)

    return await _update_video_session_with_retry(db, session_id, mutate)


async def _persist_kaggle_handoff_with_retry(
    db,
    session_id: int,
    *,
    dataset_id: str,
    kernel_ref: str,
) -> VideoAnnounceSession | None:
    def mutate(obj: VideoAnnounceSession) -> None:
        obj.kaggle_dataset = dataset_id
        obj.kaggle_kernel_ref = kernel_ref

    return await _update_video_session_with_retry(db, session_id, mutate)


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


def _production_story_enabled() -> bool:
    raw = (os.getenv("KENIGSBERG_STORIES_PRODUCTION_ENABLED") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _kenigsberg_story_targets_override() -> list[dict[str, object]]:
    raw = (os.getenv("KENIGSBERG_STORIES_STORY_TARGETS_JSON") or "").strip()
    if raw:
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            raise RuntimeError("KENIGSBERG_STORIES_STORY_TARGETS_JSON must be a JSON list")
        return parsed
    return [
        {
            "peer": "@mostvkenig",
            "delay_seconds": 0,
            "mode": "upload",
            "required": True,
            "label": "@mostvkenig",
        }
    ]


async def _build_production_story_config(db) -> dict | None:
    story_selection_params = {
        "mode": KENIGSBERG_PROFILE_KEY,
        "story_publish_enabled": True,
        "story_publish_mode": "video",
        "story_upload_profile": "telegram_story_native_hevc_720p_v1",
        "story_targets_override": _kenigsberg_story_targets_override(),
        "story_caption": "",
    }
    business_targets_override = (os.getenv("KENIGSBERG_STORIES_STORY_BUSINESS_TARGETS") or "").strip()
    if business_targets_override:
        story_selection_params["story_business_targets"] = business_targets_override
    return await build_story_publish_config(
        db,
        main_chat_id=None,
        selection_params=story_selection_params,
        selected_event_dates=None,
    )


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


def _normalize_story_copy(text: str) -> str:
    return " ".join(str(text or "").split())


def _extract_json_object(text: str) -> dict | None:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z0-9_-]*\n", "", raw).replace("```", "").strip()
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        pass
    match = re.search(r"\{.*\}", raw, flags=re.S)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _validate_llm_scene_lines(thought_text: str, lines: list[str]) -> list[str]:
    thought_normalized = _normalize_story_copy(thought_text)
    clean = [_normalize_story_copy(line) for line in lines if _normalize_story_copy(line)]
    if not thought_normalized:
        raise StoryTextPreparationError("empty thought text")
    if not clean:
        raise StoryTextPreparationError("LLM returned no scene lines")
    if _normalize_story_copy(" ".join(clean)) != thought_normalized:
        raise StoryTextPreparationError("LLM scene split changed the thought text")
    too_long = [line for line in clean if len(line) > 118 or len(line.split()) > 19]
    if too_long:
        raise StoryTextPreparationError("LLM scene split returned an overlong screen")
    if len(clean) > 8:
        raise StoryTextPreparationError("LLM scene split returned too many screens")
    return clean


async def _ask_story_text_split_llm(thought_text: str) -> dict:
    model = (os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_MODEL") or "gemini-3.1-flash-lite").strip()
    if not model:
        raise StoryTextPreparationError("KENIGSBERG_STORIES_TEXT_SPLIT_MODEL is empty")
    timeout = max(8.0, float(os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_TIMEOUT_SEC", "40") or "40"))
    attempts = max(1, min(4, int(os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_ATTEMPTS", "3") or "3")))
    provider_timeout = max(
        5.0,
        min(
            timeout - 2.0,
            float(os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_PROVIDER_TIMEOUT_SEC", "30") or "30"),
        ),
    )
    try:
        from google_ai import GoogleAIClient, SecretsProvider

        supabase = require_main_attr("get_supabase_client")()
        incident_notifier = require_main_attr("notify_llm_incident")
        client = GoogleAIClient(
            supabase_client=supabase,
            secrets_provider=SecretsProvider(),
            consumer="kenigsberg_stories",
            incident_notifier=incident_notifier,
        )
        client.max_retries = max(
            1,
            min(2, int(os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_PROVIDER_RETRIES", "1") or "1")),
        )
        client.provider_timeout_seconds = provider_timeout
    except Exception as exc:
        raise StoryTextPreparationError(f"LLM client unavailable: {exc}") from exc
    prompt = (
        "Раздели готовый русский текст для Telegram Story на смысловые экраны.\n"
        "Не переписывай: нельзя менять, удалять или добавлять слова, даты, имена, кавычки и пунктуацию.\n"
        "Верни JSON: {\"scene_lines\":[...],\"hook\":\"...\"}. hook ровно равен первой scene_line.\n"
        "Склейка scene_lines через один пробел должна дословно дать исходный текст.\n"
        "Нужно 1-6 экранов, каждый желательно до 15 слов.\n\n"
        f"Текст: {thought_text}"
    )
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            raw, _usage = await asyncio.wait_for(
                client.generate_content_async(
                    model=model,
                    prompt=prompt,
                    generation_config={"temperature": 0, "response_mime_type": "application/json"},
                    max_output_tokens=500,
                ),
                timeout=timeout,
            )
            data = _extract_json_object(raw or "")
            if data is not None:
                return data
            last_error = StoryTextPreparationError("LLM scene split returned invalid JSON")
        except Exception as exc:
            last_error = exc
        if attempt < attempts:
            logger.warning(
                "kenigsberg: text split LLM attempt failed attempt=%s/%s err=%r",
                attempt,
                attempts,
                last_error,
            )
            await asyncio.sleep(0.8 * attempt)
    raise StoryTextPreparationError(f"LLM scene split failed: {last_error!r}") from last_error


async def _prepare_story_text_from_thought(thought_text: str) -> dict:
    if not _normalize_story_copy(thought_text):
        raise StoryTextPreparationError("empty thought text")
    data = await _ask_story_text_split_llm(thought_text)
    raw_lines = data.get("scene_lines") if isinstance(data, dict) else None
    if not isinstance(raw_lines, list):
        raise StoryTextPreparationError("LLM scene split returned no scene_lines list")
    lines = _validate_llm_scene_lines(thought_text, [str(item or "") for item in raw_lines])
    hook = _normalize_story_copy(data.get("hook") or lines[0])
    if hook != lines[0]:
        raise StoryTextPreparationError("LLM hook must equal first scene line")
    return {
        "hook": hook,
        "scene_lines": lines,
        "caption": thought_text[:240],
        "source": "thoughts_md_llm_split",
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


async def _handle_unlock_command(message: types.Message) -> None:
    db = require_main_attr("get_db")()
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
    if not existing:
        await message.answer("Активной Kenigsberg-сессии в RENDERING нет.")
        return
    if not _is_local_kernel_ref(existing.kaggle_kernel_ref) or existing.kaggle_dataset:
        await message.answer(
            f"Сессия #{existing.id} уже передана в Kaggle ({existing.kaggle_kernel_ref or 'kernel неизвестен'}). "
            "Такую сессию вручную не снимаю, дождитесь poller/status."
        )
        return
    failed = await _mark_session_failed_with_retry(
        db,
        existing.id,
        error="manual unlock: stale Kenigsberg local handoff; rerun allowed",
    )
    await message.answer(
        f"Снял зависшую pre-handoff Kenigsberg-сессию #{existing.id}: "
        f"{failed.status if failed else 'FAILED'}. Можно запускать /kenigsberg заново."
    )


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


async def _run_launch_in_background(message: types.Message) -> None:
    if _KENIGSBERG_LAUNCH_LOCK.locked():
        await message.answer(
            "Kenigsberg: предыдущий запуск ещё проходит preflight/Kaggle handoff. "
            "Если он зависнет до создания сессии, команда вернёт ошибку отдельным сообщением."
        )
        return
    async with _KENIGSBERG_LAUNCH_LOCK:
        try:
            await _handle_launch(message)
        except Exception as exc:
            logger.exception("kenigsberg: background launch crashed")
            try:
                await message.answer(f"Kenigsberg: запуск сорвался до Kaggle: {type(exc).__name__}: {exc}")
            except Exception:
                logger.exception("kenigsberg: failed to notify operator about launch crash")


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
        (
            PROJECT_ROOT / "kaggle" / "CrumpleVideo" / "story_publish.py",
            "kaggle_common/story_publish.py",
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


async def _create_kenigsberg_dataset(
    db,
    *,
    session_id: int,
    payload: dict,
    story_config: dict | None = None,
) -> str:
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
        if story_config is not None:
            (tmp_path / STORY_PUBLISH_CONFIG_FILENAME).write_text(
                json.dumps(story_config, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            write_story_secret_files(tmp_path)
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
        if _is_stale_local_handoff(existing):
            failed = await _mark_session_failed_with_retry(
                db,
                existing.id,
                error="stale Kenigsberg local handoff; auto-failed before rerun",
            )
            logger.warning(
                "kenigsberg: auto-failed stale local handoff session=%s before rerun",
                existing.id,
            )
            await message.answer(
                f"Снял зависшую pre-handoff сессию #{existing.id}"
                + (f" ({failed.status})." if failed else ".")
                + " Запускаю новый Kenigsberg render."
            )
            existing = None
        if existing is None:
            pass
        else:
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
                local_hint = (
                    " Это pre-handoff local-сессия; если она зависнет, используйте /kenigsberg unlock."
                    if _is_local_kernel_ref(existing.kaggle_kernel_ref)
                    else ""
                )
                await message.answer(
                    f"Сессия #{existing.id} ещё обрабатывается.{kaggle_state} "
                    f"Новый Kenigsberg запуск начнётся после финализации текущего.{local_hint}"
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
        story_text = await _prepare_story_text_from_thought(thought_text)
    except StoryTextPreparationError as exc:
        logger.warning(
            "kenigsberg: launch aborted issue=%s thought=%s reason=%s",
            issue_number,
            thought.get("id") or "",
            exc,
        )
        await message.answer(
            f"Kenigsberg #{issue_number}: LLM не подготовила безопасную смысловую нарезку текста, "
            "Kaggle не запускаю."
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
        "bottom_mask_px": int(os.getenv("KENIGSBERG_STORIES_BOTTOM_MASK_PX", "34")),
        "source_bans": [
            *(state.get("source_bans") or []),
            *recent_source_exclusions(state),
        ],
        "target": "https://t.me/keniggpt",
        "strategy": "heuristic_v1",
        "story_publish_requested": False,
    }
    story_config: dict | None = None
    if _production_story_enabled():
        try:
            story_config = await _build_production_story_config(db)
        except Exception as exc:
            logger.exception("kenigsberg: failed to build production story config")
            await message.answer(
                f"Kenigsberg #{issue_number}: production story publishing включён, "
                f"но preflight config не собрался: {type(exc).__name__}: {exc}"
            )
            return
        if not story_config:
            await message.answer(
                f"Kenigsberg #{issue_number}: production story publishing включён, "
                "но story_publish.json не удалось собрать. Kaggle не запускаю."
            )
            return
        payload["story_publish_requested"] = True
        payload["production_target"] = "https://t.me/mostvkenig"
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
        obj = await _mark_session_rendering_with_retry(db, obj.id) or obj
        dataset_id = await _create_kenigsberg_dataset(
            db,
            session_id=obj.id,
            payload=payload,
            story_config=story_config,
        )
        expected_files = ["payload.json", "scripts/render_kenigsberg_story.py"]
        if story_config is not None:
            expected_files.extend(
                [
                    STORY_PUBLISH_CONFIG_FILENAME,
                    STORY_PUBLISH_CIPHER_FILENAME,
                    STORY_PUBLISH_KEY_FILENAME,
                    "kaggle_common/story_publish.py",
                ]
            )
        await await_dataset_ready(
            client,
            dataset_id,
            timeout_seconds=KAGGLE_READY_WAIT_SECONDS,
            poll_interval_seconds=5,
            expected_files=expected_files,
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
        obj = await _persist_kaggle_handoff_with_retry(
            db,
            obj.id,
            dataset_id=dataset_id,
            kernel_ref=kernel_ref,
        ) or obj
    except Exception as exc:
        await _mark_session_failed_with_retry(
            db,
            obj.id,
            error=f"kaggle launch failed: {type(exc).__name__}: {exc}",
        )
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
    args = (command.args or "").strip()
    args = _canonicalize_ban_args(args)
    lowered = args.casefold()
    is_launch = not args or lowered in {"start", "run", "generate", "сгенерируй", "запуск"}

    if is_launch:
        await message.answer(
            "Kenigsberg: команду получил. Проверяю доступ и состояние запуска; "
            "следующие статусы придут отдельными сообщениями."
        )
    if not await _require_superadmin(message):
        return

    if is_launch:
        task = asyncio.create_task(_run_launch_in_background(message))
        _KENIGSBERG_LAUNCH_TASKS.add(task)
        task.add_done_callback(_KENIGSBERG_LAUNCH_TASKS.discard)
        return
    db = require_main_attr("get_db")()
    if lowered in {"status", "статус"}:
        state = await load_state(db)
        thoughts = load_thoughts()
        await message.answer(
            "Kenigsberg Stories\n"
            f"launch_enabled={_launch_enabled()}\n"
            f"production_story_enabled={_production_story_enabled()}\n"
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
    if lowered in {"unlock", "cancel", "снять", "сбросить сессию", "cancel stuck"}:
        await _handle_unlock_command(message)
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
        "/kenigsberg unlock\n"
        "/kenigsberg ban #15 1-3, 7, 16-17\n"
        "/kenigsberg bans reset"
    )
