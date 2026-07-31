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

from kaggle_registry import register_job
from kaggle_status import (
    KAGGLE_RUN_FILENAME,
    create_kaggle_run_config,
    write_kaggle_status_files,
)
from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client
from kenigsberg_stories.state import (
    KENIGSBERG_PROFILE_KEY,
    apply_generated_timeline_bans,
    choose_next_poem,
    choose_next_thought,
    format_bans_report,
    load_state,
    load_poems,
    load_thoughts,
    parse_second_ranges,
    poetry_due,
    recent_source_exclusions,
    recent_music_exclusions,
    reserve_issue_number,
    reset_bans,
)
from models import Channel, VideoAnnounceSession, VideoAnnounceSessionStatus
from remote_telegram_session import (
    RemoteTelegramSessionBusyError,
    format_remote_telegram_session_busy_lines,
    raise_if_remote_telegram_session_busy,
)
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
    story_remote_auth_scope,
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


def _telegram_session_resource_key(auth_scope: str | None) -> str:
    raw = str(auth_scope or "unknown").strip().casefold() or "unknown"
    safe = re.sub(r"[^a-z0-9_.:-]+", "-", raw).strip("-") or "unknown"
    return f"telegram_session:{safe}"


class StoryTextPreparationError(RuntimeError):
    pass


POETRY_INTERVAL_DAYS = max(1, int(os.getenv("KENIGSBERG_POETRY_INTERVAL_DAYS", "3") or "3"))
KENIGSBERG_POETRY_TEST_TARGET = os.getenv("KENIGSBERG_POETRY_TEST_TARGET", "@keniggpt").strip() or "@keniggpt"
KENIGSBERG_POETRY_HASHTAGS = (
    "#Kenigsberg #Königsberg #NeuroKönigsberg #МоствКёнигсберг #Кёнигсберг"
)


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


def _kenigsberg_story_targets(
    *,
    poetry_test: bool = False,
    poetry_vk_caption: str = "",
) -> list[dict[str, object]]:
    if poetry_test:
        return [
            {
                "peer": KENIGSBERG_POETRY_TEST_TARGET,
                "transport": "telegram_chat",
                "delay_seconds": 0,
                "mode": "upload",
                "blocking": True,
                "required": True,
                "label": KENIGSBERG_POETRY_TEST_TARGET,
            }
        ]
    targets: list[dict[str, object]] = [
        {
            "peer": "@mostvkenig",
            "delay_seconds": 0,
            "mode": "upload",
            "blocking": True,
            "required": True,
            "label": "@mostvkenig",
            "fallback_peer": "me",
        },
        {
            "peer": "@loving_guide39",
            "delay_seconds": 600,
            "mode": "repost_previous",
            "blocking": False,
            "required": False,
            "label": "@loving_guide39",
        },
        {
            "peer": "@jane_tour39",
            "delay_seconds": 600,
            "mode": "repost_previous",
            "blocking": False,
            "required": False,
            "label": "@jane_tour39",
        },
        {
            "peer": "mostvkenig",
            "transport": "vk_story",
            "delay_seconds": 120,
            "mode": "upload",
            "blocking": False,
            "required": False,
            "label": "vk:mostvkenig:story",
        },
    ]
    if poetry_vk_caption.strip():
        targets.append(
            {
                "peer": "mostvkenig",
                "transport": "vk_wall",
                "delay_seconds": 180,
                "mode": "upload",
                "blocking": False,
                "required": False,
                "label": "vk:mostvkenig:wall",
                "caption": poetry_vk_caption.strip(),
            }
        )
    return targets


async def _build_production_story_config(
    db,
    *,
    poetry_test: bool = False,
    poetry_vk_caption: str = "",
) -> dict | None:
    story_selection_params = {
        "mode": KENIGSBERG_PROFILE_KEY,
        "story_publish_enabled": True,
        "story_publish_mode": "video",
        "story_upload_profile": "telegram_story_native_hevc_720p_v1",
        "story_targets_override": _kenigsberg_story_targets(
            poetry_test=poetry_test,
            poetry_vk_caption=poetry_vk_caption,
        ),
        "story_business_targets": [],
        "story_caption": "",
    }
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


def _text_split_retry_delays(attempts: int) -> list[float]:
    raw = (os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_RETRY_DELAYS_SEC") or "2,6,12").strip()
    delays: list[float] = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        try:
            delays.append(max(0.2, min(30.0, float(value))))
        except Exception:
            continue
    while len(delays) < max(0, attempts - 1):
        delays.append(float(2 * (len(delays) + 1)))
    return delays


def _validate_llm_scene_lines(thought_text: str, lines: list[str]) -> list[str]:
    thought_normalized = _normalize_story_copy(thought_text)
    clean = [_normalize_story_copy(line) for line in lines if _normalize_story_copy(line)]
    if not thought_normalized:
        raise StoryTextPreparationError("empty thought text")
    if not clean:
        raise StoryTextPreparationError("LLM returned no scene lines")
    if _normalize_story_copy(" ".join(clean)) != thought_normalized:
        raise StoryTextPreparationError("LLM scene split changed the thought text")
    too_long = [line for line in clean if len(line) > 160 or len(line.split()) > 34]
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

        supabase = build_google_ai_limiter_supabase_client(
            fallback_factory=require_main_attr("get_supabase_client")
        )
        client = GoogleAIClient(
            supabase_client=supabase,
            secrets_provider=SecretsProvider(),
            consumer="kenigsberg_stories",
            incident_notifier=None,
        )
        # Kenigsberg text slicing is quality-sensitive: retry the selected
        # Gemini-lite model, but do not transparently fall back to Gemma.
        client.fallback_models = []
        client.incident_notifications_enabled = False
        client.max_retries = max(
            1,
            min(2, int(os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_PROVIDER_RETRIES", "1") or "1")),
        )
        client.provider_timeout_seconds = provider_timeout
    except Exception as exc:
        raise StoryTextPreparationError(f"LLM client unavailable: {exc}") from exc
    prompt = _story_text_split_prompt(thought_text)
    last_error: Exception | None = None
    retry_delays = _text_split_retry_delays(attempts)
    for attempt in range(1, attempts + 1):
        try:
            logger.info(
                "kenigsberg: text split LLM attempt issue_text_len=%s attempt=%s/%s model=%s",
                len(thought_text),
                attempt,
                attempts,
                model,
            )
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
            await asyncio.sleep(retry_delays[attempt - 1])
    raise StoryTextPreparationError(f"LLM scene split failed: {last_error!r}") from last_error


def _story_text_split_prompt(thought_text: str) -> str:
    return (
        "Раздели готовый русский текст для Telegram Story на смысловые экраны.\n"
        "Не переписывай: нельзя менять, удалять или добавлять слова, даты, имена, кавычки и пунктуацию.\n"
        "Верни JSON: {\"scene_lines\":[...],\"hook\":\"...\"}. hook ровно равен первой scene_line.\n"
        "Склейка scene_lines через один пробел должна дословно дать исходный текст.\n"
        "Нужно 1-6 экранов, каждый желательно до 15 слов.\n\n"
        f"Текст: {thought_text}"
    )


def _text_split_fallback_4o_model() -> str:
    return (os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_FALLBACK_4O_MODEL") or "gpt-4o").strip()


async def _ask_story_text_split_4o(thought_text: str) -> dict:
    model = _text_split_fallback_4o_model()
    if not model:
        raise StoryTextPreparationError("4o text split fallback is disabled")
    timeout = max(8.0, float(os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_4O_TIMEOUT_SEC", "45") or "45"))
    ask_4o = require_main_attr("ask_4o")
    raw = await asyncio.wait_for(
        ask_4o(
            _story_text_split_prompt(thought_text),
            system_prompt=(
                "Ты аккуратный редактор титров. Возвращай только JSON. "
                "Нельзя переписывать исходный текст, только расставлять границы экранов."
            ),
            response_format={"type": "json_object"},
            max_tokens=500,
            model=model,
            meta={"consumer": "kenigsberg_stories", "stage": "text_split_4o_fallback"},
            temperature=0.0,
        ),
        timeout=timeout,
    )
    data = _extract_json_object(raw or "")
    if data is None:
        raise StoryTextPreparationError("4o scene split returned invalid JSON")
    return data


def _build_story_text_payload(thought_text: str, data: dict, *, text_model: str, fallback_from: str = "") -> dict:
    raw_lines = data.get("scene_lines") if isinstance(data, dict) else None
    if not isinstance(raw_lines, list):
        raise StoryTextPreparationError("LLM scene split returned no scene_lines list")
    lines = _validate_llm_scene_lines(thought_text, [str(item or "") for item in raw_lines])
    hook = _normalize_story_copy(data.get("hook") or lines[0])
    if hook != lines[0]:
        raise StoryTextPreparationError("LLM hook must equal first scene line")
    payload = {
        "hook": hook,
        "scene_lines": lines,
        "caption": thought_text[:240],
        "source": "thoughts_md_llm_split",
        "text_model": text_model,
    }
    if fallback_from:
        payload["text_fallback_from"] = fallback_from
    return payload


async def _prepare_story_text_from_thought(thought_text: str) -> dict:
    if not _normalize_story_copy(thought_text):
        raise StoryTextPreparationError("empty thought text")
    primary_model = (os.getenv("KENIGSBERG_STORIES_TEXT_SPLIT_MODEL") or "gemini-3.1-flash-lite").strip()
    try:
        data = await _ask_story_text_split_llm(thought_text)
        return _build_story_text_payload(thought_text, data, text_model=primary_model)
    except Exception as primary_exc:
        fallback_model = _text_split_fallback_4o_model()
        if not fallback_model:
            raise StoryTextPreparationError(f"LLM scene split failed: {primary_exc!r}") from primary_exc
        logger.warning(
            "kenigsberg: primary text split failed; trying 4o fallback primary_model=%s fallback_model=%s err=%r",
            primary_model,
            fallback_model,
            primary_exc,
        )
        try:
            data = await _ask_story_text_split_4o(thought_text)
            return _build_story_text_payload(
                thought_text,
                data,
                text_model=fallback_model,
                fallback_from=primary_model,
            )
        except Exception as fallback_exc:
            raise StoryTextPreparationError(
                f"LLM scene split failed: primary={primary_exc!r}; fallback={fallback_exc!r}"
            ) from fallback_exc


def _poem_blocks_for_video(poem: dict) -> list[list[str]]:
    blocks: list[list[str]] = []
    for raw_block in poem.get("blocks") or []:
        if not isinstance(raw_block, list):
            continue
        block = []
        for raw_line in raw_block:
            line = " ".join(str(raw_line or "").split())
            if not line or line.startswith("@"):
                continue
            block.append(line)
        if block:
            blocks.append(block)
    return blocks


def _poem_author_lines(poem: dict) -> list[str]:
    return [
        str(poem.get("author") or "").strip(),
        str(poem.get("author_note") or "").strip(),
        str(poem.get("handle") or "").strip(),
    ]


def _poem_plain_text_for_hook(poem: dict) -> str:
    title = str(poem.get("title") or "").strip()
    block_text = "\n\n".join("\n".join(block) for block in _poem_blocks_for_video(poem))
    return "\n\n".join(part for part in [title, block_text] if part)


async def _build_poetry_vk_caption(poem: dict) -> str:
    author = str(poem.get("author") or "").strip()
    poem_text = _poem_plain_text_for_hook(poem)
    fallback_hook = (
        f"Кёнигсберг звучит голосом {author}: любовь к городу в строках и памяти."
        if author
        else "Кёнигсберг звучит в стихах: город, память и любовь в одном коротком видео."
    )
    hook = fallback_hook
    try:
        ask_4o = require_main_attr("ask_4o")
        raw = await asyncio.wait_for(
            ask_4o(
                (
                    "Напиши один короткий цепляющий VK-хук на русском для видео со стихотворением "
                    "о Кёнигсберге. 10-16 слов. Без хештегов, кавычек и эмодзи. "
                    f"Автор: {author or '-'}\n\nСтих:\n{poem_text[:2500]}"
                ),
                system_prompt="Ты редактор культурного VK-сообщества. Верни только одну строку хука.",
                max_tokens=80,
                model=os.getenv("KENIGSBERG_POETRY_HOOK_MODEL", "gpt-4o"),
                meta={"consumer": "kenigsberg_stories", "stage": "poetry_vk_hook"},
                temperature=0.4,
            ),
            timeout=max(8.0, float(os.getenv("KENIGSBERG_POETRY_HOOK_TIMEOUT_SEC", "25") or "25")),
        )
        candidate = " ".join(str(raw or "").strip().strip('"').split())
        if 6 <= len(candidate.split()) <= 20:
            hook = candidate
    except Exception:
        logger.warning("kenigsberg: poetry hook generation failed; using fallback", exc_info=True)
    author_lines = [line for line in _poem_author_lines(poem) if line]
    return "\n".join([hook, "", *author_lines, "", KENIGSBERG_POETRY_HASHTAGS]).strip()


def _build_poetry_text_payload(poem: dict, *, vk_caption: str) -> dict:
    blocks = _poem_blocks_for_video(poem)
    if not blocks:
        raise StoryTextPreparationError("poem has no text blocks")
    title = str(poem.get("title") or "").strip()
    first_line = blocks[0][0] if blocks and blocks[0] else title
    signature_lines = [line for line in _poem_author_lines(poem) if line and not line.startswith("@")]
    signature_keyset = {line.casefold() for line in signature_lines}
    stanza_blocks = list(blocks)
    if signature_keyset and stanza_blocks:
        tail_keys = {line.casefold() for line in stanza_blocks[-1]}
        if tail_keys and tail_keys.issubset(signature_keyset):
            stanza_blocks = stanza_blocks[:-1]
    if not stanza_blocks:
        stanza_blocks = list(blocks)
        signature_lines = []
    return {
        "hook": title or first_line,
        "scene_lines": ["\n".join(block) for block in stanza_blocks],
        "caption": vk_caption,
        "source": "poems_md",
        "text_model": "",
        "poem_blocks": stanza_blocks,
        "poem_signature_lines": signature_lines,
        "poem_author_lines": _poem_author_lines(poem),
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
    await _launch_kaggle_generation(
        message,
        db,
        thoughts_count=len(thoughts),
        bot=message.bot,
        notify_chat_id=message.chat.id,
        operator_user_id=message.from_user.id if message.from_user else None,
        trigger="manual",
    )


async def _handle_launch_with_mode(message: types.Message, *, poetry_mode: str = "auto") -> None:
    db = require_main_attr("get_db")()
    thoughts = load_thoughts()
    await _launch_kaggle_generation(
        message,
        db,
        thoughts_count=len(thoughts),
        bot=message.bot,
        notify_chat_id=message.chat.id,
        operator_user_id=message.from_user.id if message.from_user else None,
        trigger="manual",
        poetry_mode=poetry_mode,
    )


async def _run_launch_in_background(message: types.Message, *, poetry_mode: str = "auto") -> None:
    if _KENIGSBERG_LAUNCH_LOCK.locked():
        await message.answer(
            "Kenigsberg: предыдущий запуск ещё проходит preflight/Kaggle handoff. "
            "Если он зависнет до создания сессии, команда вернёт ошибку отдельным сообщением."
        )
        return
    async with _KENIGSBERG_LAUNCH_LOCK:
        try:
            await _handle_launch_with_mode(message, poetry_mode=poetry_mode)
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
            PROJECT_ROOT / "docs" / "features" / "kenigsberg-stories" / "poems.md",
            "docs/features/kenigsberg-stories/poems.md",
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
    kaggle_run_config: dict | None = None,
    dataset_id: str | None = None,
) -> str:
    username = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME not set")
    dataset_id = dataset_id or f"{username}/kenigsberg-session-{session_id}-{int(time.time())}"
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
        write_kaggle_status_files(tmp_path, kaggle_run_config)
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


async def _send_launch_message(bot, chat_id: int | None, text: str) -> None:
    if chat_id is None:
        logger.info("kenigsberg: %s", text)
        return
    await bot.send_message(int(chat_id), text, disable_web_page_preview=True)


async def _launch_kaggle_generation(
    message: types.Message | None,
    db,
    *,
    thoughts_count: int,
    bot=None,
    notify_chat_id: int | None = None,
    operator_user_id: int | None = None,
    trigger: str = "manual",
    poetry_mode: str = "auto",
) -> int | None:
    if message is not None:
        bot = bot or message.bot
        notify_chat_id = notify_chat_id if notify_chat_id is not None else message.chat.id
        operator_user_id = (
            operator_user_id
            if operator_user_id is not None
            else (message.from_user.id if message.from_user else None)
        )
    if bot is None:
        raise RuntimeError("Kenigsberg launch requires bot instance")

    async def notify(text: str) -> None:
        await _send_launch_message(bot, notify_chat_id, text)

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
            await notify(
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
                await notify(
                    f"Сессия #{existing.id} уже завершилась на Kaggle, бот забирает и публикует результат."
                    f"{kaggle_state} Дождитесь сообщения с видео/логами."
                )
            else:
                local_hint = (
                    " Это pre-handoff local-сессия; если она зависнет, используйте /kenigsberg unlock."
                    if _is_local_kernel_ref(existing.kaggle_kernel_ref)
                    else ""
                )
                await notify(
                    f"Сессия #{existing.id} ещё обрабатывается.{kaggle_state} "
                    f"Новый Kenigsberg запуск начнётся после финализации текущего.{local_hint}"
                )
            return None

    test_chat_id = None

    story_auth_scope = story_remote_auth_scope()
    try:
        await raise_if_remote_telegram_session_busy(
            current_job_type=KENIGSBERG_PROFILE_KEY,
            current_auth_scope=story_auth_scope,
        )
    except RemoteTelegramSessionBusyError as exc:
        logger.warning(
            "kenigsberg.remote_telegram_session_busy conflicts=%s",
            [conflict.kernel_ref for conflict in exc.conflicts],
        )
        for line in format_remote_telegram_session_busy_lines(
            exc.conflicts,
            actor_label="Kenigsberg Stories",
        ):
            await notify(line)
        return None

    issue_number = await reserve_issue_number(db)
    seed = (secrets.randbits(63) ^ time.time_ns() ^ issue_number) & ((1 << 63) - 1)
    state = await load_state(db)
    poems = load_poems()
    mode_key = str(poetry_mode or "auto").strip().casefold()
    force_poetry = mode_key in {"today", "test", "force"}
    poetry_test = mode_key == "test"
    use_poetry = bool(
        poems
        and (
            force_poetry
            or (
                mode_key == "auto"
                and poetry_due(state, interval_days=POETRY_INTERVAL_DAYS)
            )
        )
    )
    thought: dict[str, str] = {"id": "", "text": ""}
    poem: dict | None = None
    story_text: dict
    poetry_vk_caption = ""
    if use_poetry:
        poem = await choose_next_poem(db)
        if poem is None:
            use_poetry = False
    if use_poetry and poem is not None:
        poetry_vk_caption = await _build_poetry_vk_caption(poem)
        story_text = _build_poetry_text_payload(poem, vk_caption=poetry_vk_caption)
        thought_text = str(poem.get("body") or "")
    else:
        thought = await choose_next_thought(db)
        thought_text = str(thought.get("text") or "")
    await notify(
        f"Kenigsberg #{issue_number}: принял команду, готовлю Kaggle, "
        + (
            f"poetry={poem.get('id') if poem else '-'} / {len(poems)}"
            if use_poetry and poem is not None
            else f"thought={thought.get('id') or '-'} / {thoughts_count}"
        )
        + "."
    )
    logger.info(
        "kenigsberg: launch accepted issue=%s thought=%s chat_id=%s user_id=%s",
        issue_number,
        poem.get("id") if poem else thought.get("id") or "",
        notify_chat_id,
        operator_user_id,
    )
    if not use_poetry:
        try:
            story_text = await _prepare_story_text_from_thought(thought_text)
        except StoryTextPreparationError as exc:
            logger.warning(
                "kenigsberg: launch aborted issue=%s thought=%s reason=%s",
                issue_number,
                thought.get("id") or "",
                exc,
            )
            await notify(
                f"Kenigsberg #{issue_number}: LLM не подготовила безопасную смысловую нарезку текста, "
                "Kaggle не запускаю."
            )
            return None
    payload = {
        "issue_number": issue_number,
        "seed": seed,
        "profile_key": KENIGSBERG_PROFILE_KEY,
        "content_mode": "poetry" if use_poetry else "thought",
        "thought_id": thought.get("id") or "",
        "thought_text": thought_text,
        "poem_id": poem.get("id") if poem else "",
        "poem_title": poem.get("title") if poem else "",
        "poem_author": poem.get("author") if poem else "",
        "poem_author_note": poem.get("author_note") if poem else "",
        "poem_handle": poem.get("handle") if poem else "",
        "poem_audio": poem.get("audio") if poem else "",
        "poem_blocks": story_text.get("poem_blocks") or [],
        "poem_signature_lines": story_text.get("poem_signature_lines") or [],
        "poem_author_lines": story_text.get("poem_author_lines") or [],
        "forced_video_dataset": (poem.get("video_dataset") or "") if poem else "",
        "hook": story_text.get("hook") or "",
        "scene_lines": story_text.get("scene_lines") or [],
        "caption": story_text.get("caption") or "",
        "text_source": story_text.get("source") or "",
        "text_model": story_text.get("text_model") or "",
        "text_fallback_from": story_text.get("text_fallback_from") or "",
        "crop_bottom_px": int(os.getenv("KENIGSBERG_STORIES_CROP_BOTTOM_PX", "96")),
        "bottom_mask_px": int(os.getenv("KENIGSBERG_STORIES_BOTTOM_MASK_PX", "34")),
        "source_bans": [
            *(state.get("source_bans") or []),
            *recent_source_exclusions(state),
        ],
        "recent_music": recent_music_exclusions(state),
        "target": "https://t.me/mostvkenig",
        "strategy": "heuristic_v1",
        "trigger": trigger,
        "poetry_mode": mode_key if use_poetry else "",
        "story_publish_requested": True,
        "production_target": KENIGSBERG_POETRY_TEST_TARGET if poetry_test else "https://t.me/mostvkenig",
    }
    story_config: dict | None = None
    try:
        story_config = await _build_production_story_config(
            db,
            poetry_test=poetry_test,
            poetry_vk_caption=poetry_vk_caption if use_poetry and not poetry_test else "",
        )
    except Exception as exc:
        logger.exception("kenigsberg: failed to build production story config")
        await notify(
            f"Kenigsberg #{issue_number}: production story publishing включён, "
            f"но preflight config не собрался: {type(exc).__name__}: {exc}"
        )
        return None
    if not story_config:
        await notify(
            f"Kenigsberg #{issue_number}: production story publishing включён, "
            "но story_publish.json не удалось собрать. Kaggle не запускаю."
        )
        return None
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
        bot,
        obj,
        {},
        chat_id=notify_chat_id,
        allow_send=True,
        note="Kenigsberg: готовим Kaggle",
    )
    status_chat_id = status_message[0] if status_message else notify_chat_id
    status_message_id = status_message[1] if status_message else None
    if status_message:
        remember_status_message(obj.id, status_chat_id, status_message_id)

    await notify(
        f"Kenigsberg #{issue_number}: запускаю Kaggle MVP, period=auto, "
        + (
            f"poetry={poem.get('id') if poem else '-'} / {len(poems)}, "
            if use_poetry and poem is not None
            else f"thought={thought.get('id') or '-'} / {thoughts_count}, "
        )
        + f"text={story_text.get('source') or 'unknown'}"
        f"{'/' + str(story_text.get('text_model')) if story_text.get('text_model') else ''}."
    )
    client = KaggleClient()
    try:
        obj = await _mark_session_rendering_with_retry(db, obj.id) or obj
        kaggle_username = (os.getenv("KAGGLE_USERNAME") or "").strip()
        if not kaggle_username:
            raise RuntimeError("KAGGLE_USERNAME not set")
        run_dataset_ref = f"{kaggle_username}/kenigsberg-session-{obj.id}-{int(time.time())}"
        kaggle_run_config = await create_kaggle_run_config(
            db,
            run_id=f"kenigsberg:{obj.id}",
            session_id=obj.id,
            kind=KENIGSBERG_PROFILE_KEY,
            notebook="KoenigsbergStories",
            kernel_ref=obj.kaggle_kernel_ref,
            dataset_ref=run_dataset_ref,
            resource_leases=[_telegram_session_resource_key(story_auth_scope)],
        )
        dataset_id = await _create_kenigsberg_dataset(
            db,
            session_id=obj.id,
            payload=payload,
            story_config=story_config,
            kaggle_run_config=kaggle_run_config,
            dataset_id=run_dataset_ref,
        )
        expected_files = ["payload.json", "scripts/render_kenigsberg_story.py"]
        if kaggle_run_config:
            expected_files.append(KAGGLE_RUN_FILENAME)
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
        try:
            await register_job(
                KENIGSBERG_PROFILE_KEY,
                kernel_ref,
                meta={
                    "session_id": obj.id,
                    "issue_number": issue_number,
                    "trigger": trigger,
                    "chat_id": notify_chat_id,
                    "operator_user_id": operator_user_id,
                    "dataset_slug": dataset_id,
                    "remote_telegram_auth_scope": story_auth_scope,
                    "pid": os.getpid(),
                },
            )
        except Exception:
            logger.warning(
                "kenigsberg: failed to register remote telegram session job session=%s kernel=%s",
                obj.id,
                kernel_ref,
                exc_info=True,
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
        await notify(f"Kenigsberg #{issue_number}: не удалось запустить Kaggle: {type(exc).__name__}: {exc}")
        return None

    start_kernel_poller_task(
        db,
        client,
        obj,
        bot=bot,
        notify_chat_id=notify_chat_id,
        test_chat_id=test_chat_id,
        main_chat_id=None,
        status_chat_id=status_chat_id,
        status_message_id=status_message_id,
        poll_interval=45,
        timeout_minutes=VIDEO_KAGGLE_TIMEOUT_MINUTES,
        dataset_slug=obj.kaggle_dataset,
    )
    return int(obj.id)


async def launch_scheduled_kenigsberg_story(
    db,
    bot,
    *,
    notify_chat_id: int | None,
    trigger: str = "scheduled",
    poetry_mode: str = "auto",
) -> int | None:
    if _KENIGSBERG_LAUNCH_LOCK.locked():
        raise RuntimeError("Kenigsberg launch preflight is already running")
    async with _KENIGSBERG_LAUNCH_LOCK:
        return await _launch_kaggle_generation(
            None,
            db,
            thoughts_count=len(load_thoughts()),
            bot=bot,
            notify_chat_id=notify_chat_id,
            operator_user_id=0,
            trigger=trigger,
            poetry_mode=poetry_mode,
        )


@kenigsberg_stories_router.message(Command("kenigsberg"))
async def cmd_kenigsberg(message: types.Message, command: CommandObject) -> None:
    args = (command.args or "").strip()
    args = _canonicalize_ban_args(args)
    lowered = args.casefold()
    poetry_mode = "auto"
    launch_args = {"--poetry-test", "--poetry-today", "poetry-test", "poetry-today"}
    if lowered in {"--poetry-test", "poetry-test"}:
        poetry_mode = "test"
    elif lowered in {"--poetry-today", "poetry-today"}:
        poetry_mode = "today"
    is_launch = (
        not args
        or lowered in {"start", "run", "generate", "сгенерируй", "запуск"}
        or lowered in launch_args
    )

    if is_launch:
        await message.answer(
            "Kenigsberg: команду получил. Проверяю доступ и состояние запуска; "
            "следующие статусы придут отдельными сообщениями."
        )
    if not await _require_superadmin(message):
        return

    if is_launch:
        task = asyncio.create_task(_run_launch_in_background(message, poetry_mode=poetry_mode))
        _KENIGSBERG_LAUNCH_TASKS.add(task)
        task.add_done_callback(_KENIGSBERG_LAUNCH_TASKS.discard)
        return
    db = require_main_attr("get_db")()
    if lowered in {"status", "статус"}:
        state = await load_state(db)
        thoughts = load_thoughts()
        await message.answer(
            "Kenigsberg Stories\n"
            "launch=enabled_in_code\n"
            "production_story=enabled_in_code\n"
            f"next_issue=#{state.get('next_issue', 1)}\n"
            f"thoughts={len(thoughts)} used={len(state.get('used_thought_ids') or [])}\n"
            f"poems={len(load_poems())} used={len(state.get('used_poem_ids') or [])} "
            f"pending={state.get('pending_poem_id') or '-'} "
            f"last_poetry={state.get('last_poetry_success_at') or '-'}\n"
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
        "/kenigsberg --poetry-test\n"
        "/kenigsberg --poetry-today\n"
        "/kenigsberg status\n"
        "/kenigsberg bans\n"
        "/kenigsberg unlock\n"
        "/kenigsberg ban #15 1-3, 7, 16-17\n"
        "/kenigsberg bans reset"
    )
