from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

try:
    import cv2
except Exception:  # pragma: no cover - optional runtime dependency in tests/dev
    cv2 = None
from aiogram import types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import FSInputFile
from sqlalchemy import select

from admin_chat import resolve_superadmin_chat_id
from db import Database
from kaggle_status import (
    KAGGLE_RUN_FILENAME,
    create_kaggle_run_config,
    enrich_kaggle_status_from_ledger,
    format_kaggle_status_label,
    write_kaggle_status_files,
)
from kaggle_registry import remove_job
from main import format_day_pretty
from models import (
    Event,
    VideoAnnounceEventHit,
    VideoAnnounceItem,
    VideoAnnounceItemStatus,
    VideoAnnounceSession,
    VideoAnnounceSessionStatus,
)
from promo import record_video_promo_exposures
from .kaggle_client import (
    LOCAL_KERNEL_PREFIX,
    KaggleClient,
    await_dataset_ready,
    await_kernel_dataset_sources,
    resolve_kaggle_slug as _accel_pref_resolve_slug,
)
from .story_publish import (
    STORY_PUBLISH_CIPHER_FILENAME,
    STORY_PUBLISH_CONFIG_FILENAME,
    STORY_PUBLISH_KEY_FILENAME,
    build_story_publish_config,
    encrypt_secret,
)

logger = logging.getLogger(__name__)
KENIGSBERG_REMOTE_TELEGRAM_JOB_TYPE = "kenigsberg_story"

_status_messages: dict[int, tuple[int, int]] = {}
_status_locks: dict[int, asyncio.Lock] = {}
_poller_tasks: dict[int, asyncio.Task] = {}


def _is_viewer_facing_cherryflash_publish(session_obj: VideoAnnounceSession | None) -> bool:
    profile_key = str(getattr(session_obj, "profile_key", "") or "")
    return profile_key == "popular_review" or profile_key.startswith("popular_review_")


def _read_positive_int(env_key: str, default: int) -> int:
    raw_value = os.getenv(env_key)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
        if value <= 0:
            raise ValueError
        return value
    except ValueError:
        logger.warning(
            "video_announce: invalid %s=%r, falling back to default %s",
            env_key,
            raw_value,
            default,
        )
        return default


VIDEO_MAX_MB = _read_positive_int("VIDEO_MAX_MB", 50)
VIDEO_KAGGLE_TIMEOUT_MINUTES = _read_positive_int("VIDEO_KAGGLE_TIMEOUT_MINUTES", 225)
VIDEO_KAGGLE_HANDOFF_GRACE_MINUTES = _read_positive_int(
    "VIDEO_KAGGLE_HANDOFF_GRACE_MINUTES",
    10,
)
VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES = _read_positive_int(
    "VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES",
    15,
)
VIDEO_KAGGLE_REMOTE_ALIVE_EXTENSION_MINUTES = _read_positive_int(
    "VIDEO_KAGGLE_REMOTE_ALIVE_EXTENSION_MINUTES",
    30,
)
VIDEO_KAGGLE_ABSOLUTE_TIMEOUT_MINUTES = _read_positive_int(
    "VIDEO_KAGGLE_ABSOLUTE_TIMEOUT_MINUTES",
    12 * 60,
)
STORY_PUBLISH_ONLY_TIMEOUT_MINUTES = _read_positive_int(
    "VIDEO_ANNOUNCE_STORY_PUBLISH_ONLY_TIMEOUT_MINUTES",
    45,
)
STORY_PUBLISH_ONLY_KERNEL_REF = (
    os.getenv("VIDEO_ANNOUNCE_STORY_PUBLISH_ONLY_KERNEL_REF")
    or "local:CrumpleStoryPublishOnly"
).strip()
STORY_PUBLISH_ONLY_RECOVERY_ENABLED = (
    os.getenv("VIDEO_ANNOUNCE_STORY_PUBLISH_ONLY_RECOVERY", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
STORY_PUBLISH_ONLY_LOCK_DIR = Path(
    os.getenv("VIDEO_ANNOUNCE_STORY_PUBLISH_ONLY_LOCK_DIR")
    or "/tmp/events-bot-locks"
)

logger.info(
    "video_announce: limits configured max_video_mb=%s kaggle_timeout_min=%s handoff_grace_min=%s remote_alive_grace_min=%s",
    VIDEO_MAX_MB,
    VIDEO_KAGGLE_TIMEOUT_MINUTES,
    VIDEO_KAGGLE_HANDOFF_GRACE_MINUTES,
    VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES,
)


def _is_local_kernel_ref(kernel_ref: str | None) -> bool:
    return str(kernel_ref or "").strip().startswith(LOCAL_KERNEL_PREFIX)


def _parse_utc_iso(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _fresh_kaggle_ledger_heartbeat(
    db: Database,
    run_id: str,
    *,
    now: datetime | None = None,
    grace_minutes: int = VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES,
) -> dict | None:
    now = now or datetime.now(timezone.utc)
    try:
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                """
                SELECT status, phase, last_heartbeat_at, updated_at, terminal_at, progress_json
                FROM kaggle_run_ledger
                WHERE run_id=?
                """,
                (run_id,),
            )
            row = await cur.fetchone()
            await cur.close()
    except Exception:
        logger.exception("video_announce: failed to read Kaggle ledger heartbeat run_id=%s", run_id)
        return None
    if not row:
        return None
    status = str(row[0] or "").strip().casefold()
    if status in {"failed", "error", "cancelled", "canceled", "complete", "done"}:
        return None
    if row[4]:
        return None
    heartbeat_at = _parse_utc_iso(row[2]) or _parse_utc_iso(row[3])
    if heartbeat_at is None:
        return None
    age_seconds = (now - heartbeat_at).total_seconds()
    if age_seconds < 0 or age_seconds > max(60, int(grace_minutes) * 60):
        return None
    progress: dict = {}
    try:
        parsed = json.loads(row[5] or "{}")
        if isinstance(parsed, dict):
            progress = parsed
    except Exception:
        progress = {}
    return {
        "status": row[0],
        "phase": row[1],
        "last_heartbeat_at": row[2],
        "updated_at": row[3],
        "age_seconds": age_seconds,
        "progress": progress,
    }


def _local_handoff_grace_deadline(session_obj: VideoAnnounceSession) -> datetime | None:
    reference = (
        session_obj.started_at
        or session_obj.created_at
    )
    if not isinstance(reference, datetime):
        return None
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return reference + timedelta(minutes=VIDEO_KAGGLE_HANDOFF_GRACE_MINUTES)


async def _live_video_ledger_session_ids(
    db: Database,
    *,
    now: datetime | None = None,
    grace_minutes: int = VIDEO_KAGGLE_REMOTE_ALIVE_GRACE_MINUTES,
) -> set[int]:
    """Return video session ids whose Kaggle status ledger is freshly alive.

    This is used by recovery after a bot restart to rescue sessions that were
    falsely marked FAILED by stale Kaggle status/output while the newly pushed
    notebook had already started and was still sending heartbeats.
    """

    if db is None or not hasattr(db, "raw_conn"):
        return set()
    now = now or datetime.now(timezone.utc)
    try:
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                """
                SELECT run_id, session_id, kind, notebook, status,
                       last_heartbeat_at, updated_at, terminal_at
                FROM kaggle_run_ledger
                WHERE run_id LIKE 'videoannounce:%'
                  AND session_id IS NOT NULL
                """
            )
            rows = await cur.fetchall()
            await cur.close()
    except Exception:
        logger.exception("video_announce: failed to read live Kaggle video ledgers")
        return set()

    live: set[int] = set()
    for (
        run_id_raw,
        session_id,
        kind_raw,
        notebook_raw,
        status_raw,
        heartbeat_raw,
        updated_raw,
        terminal_raw,
    ) in rows:
        run_id = str(run_id_raw or "").strip()
        kind = str(kind_raw or "").strip().casefold()
        notebook = str(notebook_raw or "").strip().casefold()
        if ":publish-only:" in run_id or kind.endswith("publish_only") or "publishonly" in notebook:
            continue
        status = str(status_raw or "").strip().casefold()
        if status in {"failed", "error", "cancelled", "canceled", "complete", "done"}:
            continue
        if terminal_raw:
            continue
        heartbeat_at = _parse_utc_iso(heartbeat_raw) or _parse_utc_iso(updated_raw)
        if heartbeat_at is None:
            continue
        age_seconds = (now - heartbeat_at).total_seconds()
        if 0 <= age_seconds <= max(60, int(grace_minutes) * 60):
            try:
                live.add(int(session_id))
            except (TypeError, ValueError):
                continue
    return live


def _terminal_reconcile_grace_minutes() -> int:
    """Bound the window in which the original poller may finish post-processing.

    A Kaggle notebook marks its durable ledger terminal before the bot has
    downloaded and delivered the render output.  A short grace therefore avoids
    racing a healthy poller.  After the grace, a session that is *still*
    ``RENDERING`` is a stale projection and must not hold the global/profile
    render lock forever.
    """

    raw = (os.getenv("VIDEO_TERMINAL_RECONCILE_GRACE_MINUTES") or "60").strip()
    try:
        return max(10, min(24 * 60, int(raw)))
    except ValueError:
        return 60


async def _terminal_video_ledger_rows(db: Database) -> dict[int, dict[str, object]]:
    """Return terminal source-render ledgers keyed by session id.

    Publish-only ledgers are deliberately excluded: their terminal state does
    not authorize changing the source render session.
    """

    try:
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                """
                SELECT run_id, session_id, kind, notebook, status,
                       terminal_at, updated_at
                FROM kaggle_run_ledger
                WHERE run_id LIKE 'videoannounce:%'
                  AND session_id IS NOT NULL
                  AND (
                    terminal_at IS NOT NULL
                    OR lower(COALESCE(status, '')) IN (
                        'done', 'complete', 'failed', 'error', 'cancelled', 'canceled'
                    )
                  )
                ORDER BY updated_at DESC
                """
            )
            rows = await cur.fetchall()
            await cur.close()
    except Exception:
        logger.exception("video_announce: failed to read terminal Kaggle video ledgers")
        return {}

    terminal: dict[int, dict[str, object]] = {}
    for run_id_raw, session_id_raw, kind_raw, notebook_raw, status_raw, terminal_raw, updated_raw in rows:
        run_id = str(run_id_raw or "").strip()
        kind = str(kind_raw or "").strip().casefold()
        notebook = str(notebook_raw or "").strip().casefold()
        if ":publish-only:" in run_id or kind.endswith("publish_only") or "publishonly" in notebook:
            continue
        try:
            session_id = int(session_id_raw)
        except (TypeError, ValueError):
            continue
        terminal.setdefault(
            session_id,
            {
                "status": str(status_raw or "").strip().casefold(),
                "terminal_at": terminal_raw,
                "updated_at": updated_raw,
            },
        )
    return terminal


async def reconcile_terminal_rendering_sessions(
    db: Database,
    bot=None,
    *,
    chat_id: int | None = None,
    now: datetime | None = None,
    grace_minutes: int | None = None,
) -> int:
    """Release stale ``RENDERING`` locks whose Kaggle ledger is terminal.

    The notebook ledger proves that remote execution has stopped, but it does
    not prove that the bot downloaded and delivered the final video.  Successful
    remote runs without a durable local delivery receipt are therefore marked
    ``PUBLISH_BLOCKED`` (not ``DONE``); failed/cancelled remote runs become
    ``FAILED``.  The transition is idempotent and emits at most one operator
    notification because only ``RENDERING`` rows are eligible.
    """

    terminal_rows = await _terminal_video_ledger_rows(db)
    if not terminal_rows:
        return 0
    now_utc = now or datetime.now(timezone.utc)
    grace = _terminal_reconcile_grace_minutes() if grace_minutes is None else max(0, int(grace_minutes))
    async with db.get_session() as session:
        result = await session.execute(
            select(VideoAnnounceSession).where(
                VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING,
                VideoAnnounceSession.id.in_(sorted(terminal_rows)),
            )
        )
        candidates = list(result.scalars().all())

    reconciled = 0
    for sess in candidates:
        ledger = terminal_rows.get(int(sess.id or 0)) or {}
        terminal_at = _parse_utc_iso(ledger.get("terminal_at")) or _parse_utc_iso(
            ledger.get("updated_at")
        )
        if terminal_at is None:
            continue
        age_seconds = (now_utc - terminal_at).total_seconds()
        if age_seconds < max(0, grace) * 60:
            continue

        ledger_status = str(ledger.get("status") or "").casefold()
        remote_failed = ledger_status in {"failed", "error", "cancelled", "canceled"}
        target_status = (
            VideoAnnounceSessionStatus.FAILED
            if remote_failed
            else VideoAnnounceSessionStatus.PUBLISH_BLOCKED
        )
        safe_error = (
            "terminal Kaggle run failed; stale rendering lock reconciled"
            if remote_failed
            else "terminal Kaggle run was not projected to a verified delivery; publish recovery required"
        )
        updated = await _update_status(
            db,
            int(sess.id),
            status=target_status,
            error=safe_error,
            expected_status=VideoAnnounceSessionStatus.RENDERING,
        )
        if updated is None:
            continue
        task = _poller_tasks.get(int(sess.id or 0))
        if task is not None and not task.done():
            task.cancel()
            # Let cancellation reach the first cooperative boundary without
            # waiting on provider/network cleanup indefinitely.
            await asyncio.sleep(0)
        reconciled += 1
        logger.error(
            "video_announce: reconciled stale terminal render session=%s ledger_status=%s target_status=%s terminal_age_sec=%.0f",
            updated.id,
            ledger_status or "terminal",
            target_status.value,
            age_seconds,
        )
        if bot is not None:
            notify_chat_id = await _resolve_recovery_notify_chat_id(
                db,
                updated,
                chat_id=chat_id,
            )
            if notify_chat_id:
                try:
                    await asyncio.wait_for(
                        bot.send_message(
                            notify_chat_id,
                            (
                                f"⚠️ Сессия #{updated.id}: Kaggle уже завершился, но итоговая "
                                "доставка видео не была подтверждена. Зависший render-lock снят; "
                                "слепой повтор публикации не выполнялся."
                            ),
                        ),
                        timeout=15,
                    )
                except Exception:
                    logger.warning(
                        "video_announce: failed to notify terminal reconciliation session=%s",
                        updated.id,
                        exc_info=True,
                    )
    return reconciled


def _video_thumbnail_input(video_path: str | Path) -> types.InputFile | None:
    preview_path = Path(video_path).with_name("telegram_preview.jpg")
    if preview_path.exists():
        return FSInputFile(preview_path)
    encoded_bytes: bytes | None = None
    if cv2 is not None:
        cap = cv2.VideoCapture(str(video_path))
        if cap.isOpened():
            try:
                ok, frame = cap.read()
            finally:
                cap.release()
            if ok and frame is not None:
                ok, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
                if ok:
                    encoded_bytes = encoded.tobytes()
        else:
            logger.warning("video_announce: failed to open video for thumbnail %s", video_path)
    if not encoded_bytes and shutil.which("ffmpeg"):
        result = subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-i",
                str(video_path),
                "-frames:v",
                "1",
                "-q:v",
                "2",
                "-f",
                "image2pipe",
                "-vcodec",
                "mjpeg",
                "-",
            ],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout:
            encoded_bytes = result.stdout
    if not encoded_bytes:
        logger.info("video_announce: explicit video thumbnail unavailable for %s", video_path)
        return None
    stem = Path(video_path).stem
    return types.BufferedInputFile(encoded_bytes, filename=f"{stem}_thumb.jpg")


async def _send_video_with_preview(
    bot,
    chat_id: int,
    video_path: str | Path,
    *,
    caption: str,
) -> None:
    await bot.send_video(
        chat_id,
        FSInputFile(video_path),
        caption=caption,
        supports_streaming=True,
    )


def _status_keyboard(session_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=
        [[types.InlineKeyboardButton(text="🔄 Обновить статус", callback_data=f"vidkstat:{session_id}")]]
    )


def remember_status_message(session_id: int, chat_id: int, message_id: int) -> None:
    _status_messages[session_id] = (chat_id, message_id)


def get_status_message(session_id: int) -> tuple[int, int] | None:
    return _status_messages.get(session_id)


def _get_status_lock(session_id: int) -> asyncio.Lock:
    lock = _status_locks.get(session_id)
    if not lock:
        lock = asyncio.Lock()
        _status_locks[session_id] = lock
    return lock


def _track_poller_task(session_id: int, task: asyncio.Task) -> None:
    _poller_tasks[session_id] = task

    def _cleanup(_task: asyncio.Task) -> None:
        _poller_tasks.pop(session_id, None)

    task.add_done_callback(_cleanup)


def _poller_active(session_id: int) -> bool:
    task = _poller_tasks.get(session_id)
    return bool(task and not task.done())


def start_kernel_poller_task(
    db: Database,
    client: KaggleClient,
    session_obj: VideoAnnounceSession,
    *,
    bot,
    notify_chat_id: int,
    test_chat_id: int | None,
    main_chat_id: int | None,
    status_chat_id: int | None = None,
    status_message_id: int | None = None,
    poll_interval: int = 60,
    timeout_minutes: int = VIDEO_KAGGLE_TIMEOUT_MINUTES,
    download_dir: Path | None = None,
    dataset_slug: str | None = None,
) -> asyncio.Task:
    if _poller_active(session_obj.id):
        return _poller_tasks[session_obj.id]
    task = asyncio.create_task(
        run_kernel_poller(
            db,
            client,
            session_obj,
            bot=bot,
            notify_chat_id=notify_chat_id,
            test_chat_id=test_chat_id,
            main_chat_id=main_chat_id,
            status_chat_id=status_chat_id,
            status_message_id=status_message_id,
            poll_interval=poll_interval,
            timeout_minutes=timeout_minutes,
            download_dir=download_dir,
            dataset_slug=dataset_slug,
        )
    )
    _track_poller_task(session_obj.id, task)
    _track_remote_telegram_session_cleanup(session_obj, task)
    return task


def _track_remote_telegram_session_cleanup(
    session_obj: VideoAnnounceSession,
    task: asyncio.Task,
) -> None:
    if str(getattr(session_obj, "profile_key", "") or "") != KENIGSBERG_REMOTE_TELEGRAM_JOB_TYPE:
        return
    kernel_ref = str(getattr(session_obj, "kaggle_kernel_ref", "") or "").strip()
    if not kernel_ref or kernel_ref.startswith("local:"):
        return

    def _cleanup(_task: asyncio.Task) -> None:
        async def _remove() -> None:
            try:
                await remove_job(KENIGSBERG_REMOTE_TELEGRAM_JOB_TYPE, kernel_ref)
            except Exception:
                logger.warning(
                    "video_announce: failed to clear kenigsberg remote telegram job kernel=%s",
                    kernel_ref,
                    exc_info=True,
                )

        try:
            asyncio.create_task(_remove())
        except RuntimeError:
            logger.warning(
                "video_announce: no running loop to clear kenigsberg remote telegram job kernel=%s",
                kernel_ref,
            )

    task.add_done_callback(_cleanup)


def _parse_positive_int(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _format_date_range(dates: list[date]) -> str | None:
    if not dates:
        return None
    min_date = min(dates)
    max_date = max(dates)
    if min_date == max_date:
        return format_day_pretty(min_date)
    return f"{format_day_pretty(min_date)} - {format_day_pretty(max_date)}"


def _selection_render_limit(session_obj: VideoAnnounceSession) -> int | None:
    params = (
        session_obj.selection_params
        if isinstance(session_obj.selection_params, dict)
        else {}
    )
    return _parse_positive_int(params.get("render_scene_limit"))


def _resolve_notify_chat_id(session_obj: VideoAnnounceSession) -> int | None:
    params = (
        session_obj.selection_params
        if isinstance(session_obj.selection_params, dict)
        else {}
    )
    raw = params.get("notify_chat_id")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


async def _resolve_recovery_notify_chat_id(
    db: Database,
    session_obj: VideoAnnounceSession,
    *,
    chat_id: int | None = None,
) -> int | None:
    if chat_id is not None:
        return int(chat_id)
    explicit = _resolve_notify_chat_id(session_obj)
    if explicit is not None:
        return explicit
    return await resolve_superadmin_chat_id(db)


def _fallback_target_date_label(session_obj: VideoAnnounceSession) -> str | None:
    params = (
        session_obj.selection_params
        if isinstance(session_obj.selection_params, dict)
        else {}
    )
    raw = params.get("target_date")
    if not raw:
        return None
    try:
        parsed = date.fromisoformat(str(raw))
    except ValueError:
        return None
    return format_day_pretty(parsed)


async def _load_session_date_range(
    db: Database, session_obj: VideoAnnounceSession
) -> str | None:
    limit = _selection_render_limit(session_obj)
    async with db.get_session() as session:
        res = await session.execute(
            select(VideoAnnounceItem)
            .where(VideoAnnounceItem.session_id == session_obj.id)
            .where(VideoAnnounceItem.status == VideoAnnounceItemStatus.READY)
            .order_by(VideoAnnounceItem.position)
        )
        items = res.scalars().all()
        if limit:
            items = items[:limit]
        event_ids = [item.event_id for item in items]
        if not event_ids:
            return None
        ev_res = await session.execute(select(Event).where(Event.id.in_(event_ids)))
        events = ev_res.scalars().all()
    dates: list[date] = []
    for ev in events:
        try:
            raw_date = (ev.date or "").split("..", 1)[0]
            dates.append(date.fromisoformat(raw_date))
        except Exception:
            continue
    return _format_date_range(dates)


async def _build_video_caption(
    db: Database, session_obj: VideoAnnounceSession
) -> str:
    label = await _load_session_date_range(db, session_obj)
    if not label:
        label = _fallback_target_date_label(session_obj)
    if not label:
        label = format_day_pretty((datetime.now(timezone.utc) + timedelta(days=1)).date())
    return f"Видео-анонс #{session_obj.id} на завтра {label}"


def _format_kaggle_status(status: dict | None) -> str:
    return format_kaggle_status_label(status)


def _status_text(
    session_obj: VideoAnnounceSession,
    kaggle_status: dict | None,
    *,
    note: str | None = None,
) -> str:
    lines = [
        f"Сессия #{session_obj.id}: {session_obj.status}",
        f"Kernel: {session_obj.kaggle_kernel_ref or '—'}",
        f"Dataset: {session_obj.kaggle_dataset or '—'}",
        f"Статус Kaggle: {_format_kaggle_status(kaggle_status)}",
    ]
    if session_obj.video_url:
        lines.append(f"Видео: {session_obj.video_url}")
    if session_obj.error:
        lines.append(f"Ошибка: {session_obj.error}")
    if note:
        lines.append(note)
    return "\n".join(lines)


async def update_status_message(
    bot,
    session_obj: VideoAnnounceSession,
    kaggle_status: dict | None,
    *,
    db: Database | None = None,
    chat_id: int | None = None,
    message_id: int | None = None,
    allow_send: bool = False,
    note: str | None = None,
) -> tuple[int, int] | None:
    kaggle_status = await enrich_kaggle_status_from_ledger(
        db,
        f"videoannounce:{session_obj.id}",
        kaggle_status,
    )
    text = _status_text(session_obj, kaggle_status, note=note)
    markup = _status_keyboard(session_obj.id)
    lock = _get_status_lock(session_obj.id)
    async with lock:
        stored = get_status_message(session_obj.id)
        if stored and (chat_id is None or message_id is None):
            chat_id, message_id = stored
        if message_id is None and not allow_send:
            return stored
        try:
            if message_id is None and chat_id is not None:
                sent = await bot.send_message(chat_id, text, reply_markup=markup)
                remember_status_message(session_obj.id, sent.chat.id, sent.message_id)
                return (sent.chat.id, sent.message_id)
            if chat_id is not None and message_id is not None:
                await bot.edit_message_text(
                    text=text, chat_id=chat_id, message_id=message_id, reply_markup=markup
                )
                remember_status_message(session_obj.id, chat_id, message_id)
                return (chat_id, message_id)
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                remember_status_message(session_obj.id, chat_id, message_id)
                return (chat_id, message_id) if chat_id is not None and message_id is not None else stored
            logger.exception(
                "video_announce: failed to update status message session_id=%s",
                session_obj.id,
            )
        except Exception:
            logger.exception(
                "video_announce: failed to update status message session_id=%s",
                session_obj.id,
            )
        return stored


def _find_video(files: Iterable[Path]) -> Path | None:
    candidates = [
        file
        for file in files
        if file.exists() and file.suffix.lower() in {".mp4", ".mov", ".mkv", ".webm"}
    ]
    if not candidates:
        return None
    preferred = [f for f in candidates if "final" in f.name.lower()]
    if preferred:
        return max(preferred, key=lambda f: f.stat().st_size if f.exists() else 0)
    return max(candidates, key=lambda f: f.stat().st_size if f.exists() else 0)


def _expected_video_name(session_obj: VideoAnnounceSession) -> str | None:
    """Return the product-final artifact required for a successful session.

    CherryFlash also writes an intro-only approval mp4 whose filename contains
    ``final``.  It is useful diagnostic output, but it must never be accepted
    as the completed daily product after the notebook exits early.
    """

    profile_key = str(session_obj.profile_key or "").strip().lower()
    kernel_ref = str(session_obj.kaggle_kernel_ref or "").strip().lower()
    if (
        profile_key.startswith("popular_review")
        or profile_key.startswith("cherryflash")
        or "cherryflash" in kernel_ref
    ):
        return "cherryflash_full_final.mp4"
    return None


def _find_session_video(
    session_obj: VideoAnnounceSession,
    files: Iterable[Path],
) -> Path | None:
    materialized = list(files)
    expected_name = _expected_video_name(session_obj)
    if expected_name:
        candidates = [
            path
            for path in materialized
            if path.exists() and path.name == expected_name
        ]
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda path: path.stat().st_size if path.exists() else 0,
        )
    return _find_video(materialized)


def _find_logs(files: Iterable[Path]) -> list[Path]:
    return [
        f
        for f in files
        if f.exists() and f.suffix.lower() in {".txt", ".log", ".json"}
    ]


def _find_story_report(files: Iterable[Path]) -> Path | None:
    for file in files:
        if file.exists() and file.name == "story_publish_report.json":
            return file
    return None


def _load_story_report(path: Path | None) -> dict | None:
    if not path or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("video_announce: failed to parse story report %s", path)
        return None
    return payload if isinstance(payload, dict) else None


async def _load_session_if_publish_only_allowed(
    db: Database,
    session_id: int,
) -> VideoAnnounceSession | None:
    """Return a fresh session unless publish-only recovery is already moot.

    Publish-only recovery can be invoked both by the standard poller and by the
    operator catch-up script.  The filesystem lock prevents concurrent work in a
    single machine lifetime, but Fly deploy/restart can drop /tmp locks while a
    second recovery is being scheduled.  Re-check the durable DB state before
    creating a new Kaggle publish-only dataset so a session already recovered by
    another invocation is not published to VK twice.
    """

    async with db.get_session() as session:
        obj = await session.get(VideoAnnounceSession, session_id)
        if not obj:
            return None
        if obj.status in {
            VideoAnnounceSessionStatus.PUBLISHED_TEST,
            VideoAnnounceSessionStatus.PUBLISHED_MAIN,
        }:
            logger.info(
                "video_announce: publish-only recovery skipped; session already published session=%s status=%s",
                session_id,
                obj.status,
            )
            return None
        return obj


async def _maybe_register_kenigsberg_manifest(
    db: Database,
    session_obj: VideoAnnounceSession,
    output_files: list[Path],
) -> None:
    if str(session_obj.profile_key or "") != "kenigsberg_story":
        return
    manifest_path = next(
        (path for path in output_files if path.name == "kenigsberg_issue_manifest.json"),
        None,
    )
    if manifest_path is None or not manifest_path.exists():
        logger.warning(
            "video_announce: kenigsberg manifest missing session=%s",
            session_obj.id,
        )
        return
    try:
        from kenigsberg_stories.state import register_issue_manifest

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        await register_issue_manifest(db, manifest)
        logger.info(
            "video_announce: registered kenigsberg issue manifest session=%s issue=%s",
            session_obj.id,
            manifest.get("issue_number"),
        )
    except Exception:
        logger.exception(
            "video_announce: failed to register kenigsberg manifest session=%s",
            session_obj.id,
        )


def _extract_partner_story_metadata(
    report: dict | None,
) -> tuple[str | None, str | None]:
    """Return ``(story_id, business_connection_hash)`` from a story-publish report.

    Partner-track sessions publish through a single Telegram Business target,
    so we look for the first Business target with a non-empty ``story_id``.
    Returns ``(None, None)`` if nothing usable is present.
    """
    if not isinstance(report, dict):
        return None, None
    for item in report.get("targets") or []:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("ok")):
            continue
        transport = str(item.get("transport") or "").strip().lower()
        if transport not in {"telegram_business", "business"}:
            continue
        story_id = item.get("story_id")
        if story_id in (None, "", 0):
            continue
        connection_hash = (
            item.get("business_connection_hash")
            or item.get("connection_hash")
            or ""
        )
        return str(story_id).strip(), str(connection_hash).strip() or None
    return None, None


async def _persist_partner_story_metadata(
    db: Database,
    session_obj: VideoAnnounceSession,
    report: dict | None,
) -> None:
    if not str(session_obj.partner_track_id or "").strip():
        return
    story_id, connection_hash = _extract_partner_story_metadata(report)
    if not story_id:
        return
    async with db.get_session() as session:
        fresh = await session.get(VideoAnnounceSession, session_obj.id)
        if fresh is None:
            return
        fresh.partner_story_id = story_id
        if connection_hash:
            fresh.partner_story_connection_hash = connection_hash
        session.add(fresh)
        await session.commit()
        await session.refresh(fresh)
        session_obj.partner_story_id = fresh.partner_story_id
        session_obj.partner_story_connection_hash = fresh.partner_story_connection_hash
    logger.info(
        "video_announce: persisted partner story metadata session=%s track=%s story_id=%s",
        session_obj.id,
        session_obj.partner_track_id,
        story_id,
    )


def _story_failure_message(report: dict | None) -> str | None:
    if not report or report.get("ok") is True:
        return None
    failed_targets: list[str] = []
    for item in report.get("targets") or []:
        if bool(item.get("ok")):
            continue
        label = str(item.get("label") or item.get("peer") or "target")
        error = str(item.get("error") or "").strip()
        failed_targets.append(f"{label} ({error})" if error else label)
    if failed_targets:
        return "story publish failed: " + "; ".join(failed_targets)
    error = str(report.get("error") or "").strip()
    if error:
        return f"story publish failed: {error}"
    return "story publish failed"


def _vk_failed_target_labels(report: dict | None) -> set[str]:
    labels: set[str] = set()
    if not isinstance(report, dict):
        return labels
    for item in report.get("targets") or []:
        if not isinstance(item, dict) or bool(item.get("ok")):
            continue
        transport = str(item.get("transport") or "").strip().lower()
        if transport not in {"vk_story", "vk_wall", "vk_wall_story"}:
            continue
        label = str(item.get("label") or item.get("peer") or "").strip()
        if label:
            labels.add(label)
    return labels


def _acquire_story_publish_only_lock(session_id: int):
    import fcntl

    STORY_PUBLISH_ONLY_LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = STORY_PUBLISH_ONLY_LOCK_DIR / f"crumple-story-publish-only-{session_id}.lock"
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(
        json.dumps(
            {
                "session_id": session_id,
                "pid": os.getpid(),
                "acquired_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        )
    )
    handle.flush()
    return handle


def _release_story_publish_only_lock(handle) -> None:
    if handle is None:
        return
    try:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except Exception:
        logger.debug("video_announce: failed to unlock publish-only lock", exc_info=True)
    try:
        handle.close()
    except Exception:
        logger.debug("video_announce: failed to close publish-only lock", exc_info=True)


def _filter_story_config_for_publish_only_recovery(
    config: dict,
    *,
    story_report: dict | None,
) -> dict | None:
    failed_vk_labels = _vk_failed_target_labels(story_report)
    targets: list[dict] = []
    for raw in config.get("targets") or []:
        if not isinstance(raw, dict):
            continue
        transport = str(raw.get("transport") or "").strip().lower()
        if transport not in {"vk_story", "vk_wall", "vk_wall_story"}:
            continue
        label = str(raw.get("label") or raw.get("peer") or "").strip()
        if failed_vk_labels and label not in failed_vk_labels:
            continue
        target = dict(raw)
        target["delay_seconds"] = 0
        target["required"] = True
        target.setdefault("blocking", False)
        targets.append(target)
    if not targets:
        return None
    filtered = dict(config)
    filtered["targets"] = targets
    filtered["publish_only_recovery"] = True
    return filtered


async def _selected_event_dates_for_session(
    db: Database,
    session_obj: VideoAnnounceSession,
) -> list[str]:
    limit = _selection_render_limit(session_obj)
    async with db.get_session() as session:
        res = await session.execute(
            select(VideoAnnounceItem)
            .where(VideoAnnounceItem.session_id == session_obj.id)
            .where(VideoAnnounceItem.status == VideoAnnounceItemStatus.READY)
            .order_by(VideoAnnounceItem.position)
        )
        items = res.scalars().all()
        if limit:
            items = items[:limit]
        event_ids = [item.event_id for item in items]
        if not event_ids:
            return []
        ev_res = await session.execute(select(Event).where(Event.id.in_(event_ids)))
        events = ev_res.scalars().all()
    by_id = {ev.id: ev for ev in events}
    dates: list[str] = []
    for item in items:
        ev = by_id.get(item.event_id)
        if ev and ev.date:
            dates.append(str(ev.date).split("..", 1)[0])
    return dates


async def _selected_event_cities_for_session(
    db: Database,
    session_obj: VideoAnnounceSession,
) -> list[str]:
    limit = _selection_render_limit(session_obj)
    async with db.get_session() as session:
        res = await session.execute(
            select(VideoAnnounceItem)
            .where(VideoAnnounceItem.session_id == session_obj.id)
            .where(VideoAnnounceItem.status == VideoAnnounceItemStatus.READY)
            .order_by(VideoAnnounceItem.position)
        )
        items = res.scalars().all()
        if limit:
            items = items[:limit]
        event_ids = [item.event_id for item in items]
        if not event_ids:
            return []
        ev_res = await session.execute(select(Event).where(Event.id.in_(event_ids)))
        events = ev_res.scalars().all()
    by_id = {ev.id: ev for ev in events}
    cities: list[str] = []
    for item in items:
        ev = by_id.get(item.event_id)
        city = str(getattr(ev, "city", "") or "").strip() if ev else ""
        if city and city not in cities:
            cities.append(city)
    return cities


async def _create_story_publish_only_dataset(
    db: Database,
    client: KaggleClient,
    session_obj: VideoAnnounceSession,
    *,
    video_path: Path,
    story_report: dict | None,
) -> tuple[str, list[str]]:
    username = os.getenv("KAGGLE_USERNAME", "").strip()
    if not username:
        raise RuntimeError("KAGGLE_USERNAME not set")
    if not video_path.exists():
        raise RuntimeError(f"publish-only source video missing: {video_path}")

    run_suffix = f"{session_obj.id}-{int(time.time())}"
    dataset_id = f"{username}/crumple-story-publish-session-{run_suffix}"
    meta = {
        "title": f"Crumple Story Publish Session {session_obj.id} {run_suffix}",
        "id": dataset_id,
        "licenses": [{"name": "CC0-1.0"}],
    }

    selection_params = (
        dict(session_obj.selection_params)
        if isinstance(session_obj.selection_params, dict)
        else {}
    )
    story_config = await build_story_publish_config(
        db,
        main_chat_id=session_obj.main_chat_id,
        selection_params=selection_params,
        selected_event_dates=await _selected_event_dates_for_session(db, session_obj),
        selected_event_cities=await _selected_event_cities_for_session(db, session_obj),
    )
    if not story_config:
        raise RuntimeError("publish-only recovery could not build story_publish.json")
    story_config = _filter_story_config_for_publish_only_recovery(
        story_config,
        story_report=story_report,
    )
    if not story_config:
        raise RuntimeError("publish-only recovery has no failed VK targets")

    vk_access_token = str(os.getenv("VK_ACCESS_TOKEN5") or "").strip()
    if not vk_access_token:
        raise RuntimeError("publish-only recovery requires VK_ACCESS_TOKEN5")
    auth_payload = json.dumps({"vk_access_token": vk_access_token}, ensure_ascii=False)
    encrypted_auth, auth_key = encrypt_secret(auth_payload)
    if not encrypted_auth or not auth_key:
        raise RuntimeError("publish-only recovery could not encrypt VK auth")

    story_dataset_sources: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "dataset-metadata.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        shutil.copy2(video_path, tmp_path / "crumple_video_final.mp4")
        (tmp_path / STORY_PUBLISH_CONFIG_FILENAME).write_text(
            json.dumps(story_config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (tmp_path / STORY_PUBLISH_CIPHER_FILENAME).write_bytes(encrypted_auth)
        (tmp_path / STORY_PUBLISH_KEY_FILENAME).write_bytes(auth_key)
        project_root = Path(__file__).resolve().parent.parent
        helper_src = project_root / "kaggle" / "CrumpleVideo" / "story_publish.py"
        helper_dest = tmp_path / "kaggle_common" / "story_publish.py"
        if not helper_src.exists():
            raise RuntimeError(f"Missing CrumpleVideo story helper: {helper_src}")
        helper_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(helper_src, helper_dest)
        kaggle_run_config = await create_kaggle_run_config(
            db,
            run_id=f"videoannounce:{session_obj.id}:publish-only:{int(time.time())}",
            session_id=session_obj.id,
            kind="crumple_story_publish_only",
            notebook="CrumpleStoryPublishOnly",
            kernel_ref=STORY_PUBLISH_ONLY_KERNEL_REF,
            dataset_ref=dataset_id,
            resource_leases=[],
        )
        if kaggle_run_config:
            write_kaggle_status_files(tmp_path, kaggle_run_config)
        (tmp_path / "publish_only_manifest.json").write_text(
            json.dumps(
                {
                    "session_id": session_obj.id,
                    "source_session_id": session_obj.id,
                    "source_video": video_path.name,
                    "targets": story_config.get("targets") or [],
                    "files": [
                        "crumple_video_final.mp4",
                        STORY_PUBLISH_CONFIG_FILENAME,
                        STORY_PUBLISH_CIPHER_FILENAME,
                        STORY_PUBLISH_KEY_FILENAME,
                        "kaggle_common/story_publish.py",
                        KAGGLE_RUN_FILENAME,
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        await asyncio.to_thread(client.create_dataset, tmp_path)
    logger.warning(
        "video_announce: publish-only dataset created session=%s dataset=%s sources=%s",
        session_obj.id,
        dataset_id,
        story_dataset_sources,
    )
    return dataset_id, story_dataset_sources


async def run_story_publish_only_recovery(
    db: Database,
    client: KaggleClient,
    session_obj: VideoAnnounceSession,
    *,
    bot,
    notify_chat_id: int,
    video_path: Path | None = None,
    story_report: dict | None = None,
    download_dir: Path | None = None,
) -> bool:
    """Republish failed CrumpleVideo VK story/wall targets without rerendering."""

    if not STORY_PUBLISH_ONLY_RECOVERY_ENABLED:
        return False
    lock_handle = _acquire_story_publish_only_lock(session_obj.id)
    if lock_handle is None:
        logger.warning(
            "video_announce: publish-only recovery already running session=%s",
            session_obj.id,
        )
        if bot is not None:
            await bot.send_message(
                notify_chat_id,
                f"⚠️ Сессия #{session_obj.id}: publish-only компенсация уже выполняется",
            )
        return False
    fresh_session = await _load_session_if_publish_only_allowed(db, session_obj.id)
    if fresh_session is None:
        _release_story_publish_only_lock(lock_handle)
        return False
    session_obj = fresh_session

    dataset_slug: str | None = None
    kernel_ref = STORY_PUBLISH_ONLY_KERNEL_REF
    source_output_dir: Path | None = None
    recovery_output_dir: Path | None = None
    try:
        source_video = video_path
        if source_video is None:
            source_kernel_ref = str(session_obj.kaggle_kernel_ref or "").strip()
            if not source_kernel_ref:
                return False
            tmp_dir = download_dir or Path(os.getenv("TMPDIR", "/tmp"))
            source_output_dir = tmp_dir / f"videoannounce-publish-only-source-{session_obj.id}"
            source_output_dir.mkdir(parents=True, exist_ok=True)
            files = await asyncio.to_thread(
                client.download_kernel_output,
                source_kernel_ref,
                path=source_output_dir,
                force=True,
                quiet=True,
            )
            output_files = _expand_output_paths([source_output_dir / item for item in files])
            source_video = _find_video(output_files)
            if story_report is None:
                story_report = _load_story_report(_find_story_report(output_files))
        if not _vk_failed_target_labels(story_report):
            return False
        if source_video is None or not source_video.exists():
            return False

        dataset_slug, extra_sources = await _create_story_publish_only_dataset(
            db,
            client,
            session_obj,
            video_path=source_video,
            story_report=story_report,
        )
        await await_dataset_ready(
            client,
            dataset_slug,
            timeout_seconds=180,
            poll_interval_seconds=5,
            expected_files=[
                "crumple_video_final.mp4",
                STORY_PUBLISH_CONFIG_FILENAME,
                "kaggle_common/story_publish.py",
                STORY_PUBLISH_CIPHER_FILENAME,
                STORY_PUBLISH_KEY_FILENAME,
            ],
        )
        dataset_sources = [dataset_slug, *extra_sources]
        kernel_ref = await asyncio.to_thread(
            client.deploy_kernel_update,
            kernel_ref,
            dataset_sources,
        )
        await await_kernel_dataset_sources(
            client,
            kernel_ref,
            dataset_sources,
            timeout_seconds=120,
            poll_interval_seconds=10,
        )
        deadline = datetime.now(timezone.utc) + timedelta(
            minutes=STORY_PUBLISH_ONLY_TIMEOUT_MINUTES
        )
        while datetime.now(timezone.utc) < deadline:
            status = await asyncio.to_thread(client.get_kernel_status, kernel_ref)
            state = str(status.get("status") or "").lower()
            if state == "complete":
                break
            if state in {"error", "failed", "cancelled", "canceled"}:
                raise RuntimeError(f"publish-only Kaggle failed: {status}")
            await asyncio.sleep(30)
        else:
            raise RuntimeError("publish-only Kaggle timeout")

        tmp_dir = download_dir or Path(os.getenv("TMPDIR", "/tmp"))
        recovery_output_dir = tmp_dir / f"videoannounce-publish-only-{session_obj.id}"
        recovery_output_dir.mkdir(parents=True, exist_ok=True)
        files = await asyncio.to_thread(
            client.download_kernel_output,
            kernel_ref,
            path=recovery_output_dir,
            force=True,
            quiet=True,
        )
        output_files = _expand_output_paths([recovery_output_dir / item for item in files])
        recovery_report = _load_story_report(_find_story_report(output_files))
        recovery_failure = _story_failure_message(recovery_report)
        if recovery_failure:
            raise RuntimeError(recovery_failure)
        session_obj = await _update_status(
            db,
            session_obj.id,
            status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
            error=None,
            video_url=session_obj.video_url or source_video.name,
        )
        if bot is not None:
            await bot.send_message(
                notify_chat_id,
                (
                    f"✅ Сессия #{session_obj.id if session_obj else '?'}: "
                    "publish-only компенсация без ререндера завершена"
                ),
            )
        return True
    except Exception as exc:
        logger.exception(
            "video_announce: publish-only recovery failed session=%s",
            session_obj.id,
        )
        if bot is not None:
            await bot.send_message(
                notify_chat_id,
                f"⚠️ Сессия #{session_obj.id}: publish-only компенсация не удалась: {exc}",
            )
        return False
    finally:
        if dataset_slug:
            await _cleanup_dataset(client, dataset_slug)
        temp_root = download_dir or Path(os.getenv("TMPDIR", "/tmp"))
        for family, path in (
            ("publish-only-source", source_output_dir),
            ("publish-only", recovery_output_dir),
        ):
            if path is not None:
                await asyncio.to_thread(
                    _cleanup_ephemeral_local_output,
                    session_obj.id,
                    path,
                    temp_root,
                    family,
                )
        _release_story_publish_only_lock(lock_handle)


def _expand_output_paths(paths: Iterable[Path]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for p in paths:
        if p.is_dir():
            for child in p.rglob("*"):
                if child.is_file() and child not in seen:
                    files.append(child)
                    seen.add(child)
        elif p.is_file() and p not in seen:
            files.append(p)
            seen.add(p)
    return files


async def _update_status(
    db: Database,
    session_id: int,
    *,
    status: VideoAnnounceSessionStatus,
    error: str | None = None,
    video_url: str | None = None,
    expected_status: VideoAnnounceSessionStatus | None = None,
) -> VideoAnnounceSession | None:
    last_exc: Exception | None = None
    for attempt in range(1, 6):
        try:
            async with db.get_session() as session:
                obj = await session.get(VideoAnnounceSession, session_id)
                if not obj:
                    return None
                if expected_status is not None and obj.status != expected_status:
                    return None
                obj.status = status
                if status in {
                    VideoAnnounceSessionStatus.DONE,
                    VideoAnnounceSessionStatus.FAILED,
                    VideoAnnounceSessionStatus.PUBLISH_BLOCKED,
                }:
                    obj.finished_at = datetime.now(timezone.utc)
                if status == VideoAnnounceSessionStatus.PUBLISHED_TEST and obj.published_at is None:
                    obj.published_at = datetime.now(timezone.utc)
                if video_url:
                    obj.video_url = video_url
                obj.error = error
                await session.commit()
                await session.refresh(obj)
                return obj
        except Exception as exc:
            last_exc = exc
            if "database is locked" not in str(exc).casefold() or attempt >= 5:
                raise
            logger.warning(
                "video_announce: retrying status update after sqlite lock session=%s status=%s attempt=%s",
                session_id,
                status,
                attempt,
            )
            await asyncio.sleep(0.3 * attempt)
    if last_exc:
        raise last_exc
    return None


async def _load_session_for_status(
    db: Database,
    session_id: int,
) -> VideoAnnounceSession | None:
    async with db.get_session() as session:
        return await session.get(VideoAnnounceSession, session_id)


async def _mark_published_main(db: Database, session_obj: VideoAnnounceSession) -> None:
    published_at = datetime.now(timezone.utc)
    async with db.get_session() as session:
        fresh = await session.get(VideoAnnounceSession, session_obj.id)
        if not fresh:
            return
        fresh.status = VideoAnnounceSessionStatus.PUBLISHED_MAIN
        fresh.published_at = published_at
        res = await session.execute(
            select(VideoAnnounceItem).where(VideoAnnounceItem.session_id == session_obj.id)
        )
        items = res.scalars().all()
        event_ids = [it.event_id for it in items]
        if event_ids:
            ev_res = await session.execute(select(Event).where(Event.id.in_(event_ids)))
            events = ev_res.scalars().all()
        else:
            events = []
        existing_hits: set[int] = set()
        if event_ids:
            hit_res = await session.execute(
                select(VideoAnnounceEventHit.event_id).where(
                    VideoAnnounceEventHit.session_id == session_obj.id,
                    VideoAnnounceEventHit.event_id.in_(event_ids),
                )
            )
            existing_hits = set(hit_res.scalars().all())
        for ev in events:
            ev.video_include_count = max(0, (ev.video_include_count or 0) - 1)
            if ev.id not in existing_hits:
                session.add(
                    VideoAnnounceEventHit(session_id=session_obj.id, event_id=ev.id)
                )
        session.add(fresh)
        await session.commit()
    try:
        count = await record_video_promo_exposures(
            db,
            session_id=int(session_obj.id),
            publish_status=VideoAnnounceSessionStatus.PUBLISHED_MAIN.value,
            published_at=published_at,
            public_target_count=1,
            public_targets=[{"kind": "main_channel"}],
        )
        if count:
            logger.info(
                "video_announce: recorded promo exposures session_id=%s count=%s",
                session_obj.id,
                count,
            )
    except Exception:
        logger.exception(
            "video_announce: failed to record promo exposures session_id=%s",
            session_obj.id,
        )


async def _record_viewer_facing_test_promo_exposures(
    db: Database,
    session_obj: VideoAnnounceSession | None,
    *,
    target_chat_id: int | None,
) -> None:
    if not session_obj or not target_chat_id:
        return
    if not _is_viewer_facing_cherryflash_publish(session_obj):
        return
    published_at = session_obj.published_at
    if not isinstance(published_at, datetime):
        published_at = datetime.now(timezone.utc)
    try:
        count = await record_video_promo_exposures(
            db,
            session_id=int(session_obj.id),
            publish_status=VideoAnnounceSessionStatus.PUBLISHED_TEST.value,
            published_at=published_at,
            public_target_count=1,
            public_targets=[{"kind": "viewer_facing_cherryflash_target", "chat_id": int(target_chat_id)}],
        )
        if count:
            logger.info(
                "video_announce: recorded viewer-facing CherryFlash promo exposures session_id=%s count=%s",
                session_obj.id,
                count,
            )
    except Exception:
        logger.exception(
            "video_announce: failed to record viewer-facing CherryFlash promo exposures session_id=%s",
            getattr(session_obj, "id", None),
        )


async def _send_logs(bot, chat_id: int, files: list[Path], *, caption: str | None = None) -> None:
    for file in files:
        try:
            input_file = FSInputFile(file)
            await bot.send_document(
                chat_id, input_file, caption=caption, disable_notification=True
            )
        except Exception:
            logger.exception("video_announce: failed to send log %s", file)


async def _download_and_send_logs(
    client: KaggleClient,
    kernel_ref: str,
    bot,
    chat_id: int,
    session_id: int,
    *,
    download_dir: Path | None = None,
    caption_prefix: str = "Логи Kaggle",
) -> None:
    """Download kernel output and send any log files to the chat."""
    tmp_dir = download_dir or Path(os.getenv("TMPDIR", "/tmp"))
    output_dir = tmp_dir / f"videoannounce-logs-{session_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        logger.info(
            "video_announce: downloading kernel output for logs kernel=%s session=%s",
            kernel_ref,
            session_id,
        )
        files = await asyncio.to_thread(
            client.download_kernel_output,
            kernel_ref,
            path=output_dir,
            force=True,
            quiet=True,
        )
        # files is a list of relative paths as strings
        # Create full paths without flattening directories
        paths = [output_dir / f for f in files]
        
        # Recursively find all log files in the output directory
        log_candidates = []
        for p in paths:
             if p.is_dir():
                 log_candidates.extend(list(p.rglob("*")))
             else:
                 log_candidates.append(p)

        log_files = _find_logs(log_candidates)
        # Deduplicate paths just in case
        log_files = sorted(list(set(log_files)))

        logger.info(
            "video_announce: found %s log files in output: %s",
            len(log_files),
            [f.name for f in log_files],
        )

        MAX_FILES_TO_SEND = 10
        if log_files:
            if len(log_files) <= MAX_FILES_TO_SEND:
                await _send_logs(
                    bot, chat_id, log_files, caption=f"{caption_prefix} сессии #{session_id}"
                )
            else:
                # Too many files, zip them
                zip_path = output_dir / f"logs-{session_id}.zip"
                import zipfile
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for lf in log_files:
                        zipf.write(lf, lf.relative_to(output_dir))
                
                await _send_logs(
                    bot, chat_id, [zip_path], caption=f"{caption_prefix} сессии #{session_id} (архив)"
                )

        else:
            # Send all files if no .log/.txt/.json found
            all_files = []
            for p in output_dir.rglob("*"):
                if p.is_file():
                    all_files.append(p)
            
            logger.info(
                "video_announce: no log files found, sending all %s files",
                len(all_files),
            )
            if all_files:
                if len(all_files) <= MAX_FILES_TO_SEND:
                    await _send_logs(
                        bot, chat_id, all_files, caption=f"{caption_prefix} сессии #{session_id}"
                    )
                else:
                    zip_path = output_dir / f"output-{session_id}.zip"
                    import zipfile
                    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        for f in all_files:
                            zipf.write(f, f.relative_to(output_dir))
                    await _send_logs(
                        bot, chat_id, [zip_path], caption=f"{caption_prefix} сессии #{session_id} (полный архив)"
                    )
            else:
                await bot.send_message(
                    chat_id, f"⚠️ Логи Kaggle для сессии #{session_id} не найдены"
                )
    except Exception:
        logger.exception(
            "video_announce: failed to download kernel output for logs session=%s",
            session_id,
        )
        await bot.send_message(
            chat_id, f"⚠️ Не удалось скачать логи Kaggle для сессии #{session_id}"
        )
    finally:
        await asyncio.to_thread(
            _cleanup_ephemeral_local_output,
            session_id,
            output_dir,
            tmp_dir,
            "logs",
        )


async def _cleanup_dataset(client: KaggleClient, dataset_slug: str | None) -> None:
    """Delete the temporary Kaggle dataset after kernel completion."""
    if not dataset_slug:
        return
    try:
        logger.info("video_announce: deleting dataset %s", dataset_slug)
        await asyncio.to_thread(client.delete_dataset, dataset_slug)
        logger.info("video_announce: dataset %s deleted successfully", dataset_slug)
    except Exception:
        logger.exception("video_announce: failed to delete dataset %s", dataset_slug)


_LOCAL_OUTPUT_CLEANUP_STATUSES = frozenset(
    {
        VideoAnnounceSessionStatus.PUBLISHED_TEST,
        VideoAnnounceSessionStatus.PUBLISHED_MAIN,
    }
)


def _recognized_local_output_dir(
    *,
    session_id: int | None,
    output_dir: Path,
    temp_root: Path,
    family: str = "render",
) -> Path | None:
    """Return an assertion-checked local output tree, or refuse the path.

    Cleanup is intentionally limited to the exact directory created by
    ``run_kernel_poller``.  In particular, a caller cannot use a different
    basename, a different session id, a sibling tree, or a symlink as a bulk
    deletion target.
    """

    if not isinstance(session_id, int) or session_id <= 0:
        return None
    names = {
        "render": f"videoannounce-{session_id}",
        "publish-only-source": f"videoannounce-publish-only-source-{session_id}",
        "publish-only": f"videoannounce-publish-only-{session_id}",
        "logs": f"videoannounce-logs-{session_id}",
    }
    expected_name = names.get(family)
    if expected_name is None:
        return None
    output_dir = Path(output_dir)
    temp_root = Path(temp_root)
    if output_dir.name != expected_name or output_dir.is_symlink():
        return None
    try:
        resolved_root = temp_root.resolve(strict=True)
        resolved_output = output_dir.resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    if not resolved_root.is_dir() or not resolved_output.is_dir():
        return None
    if resolved_output.parent != resolved_root or resolved_output.name != expected_name:
        return None
    return resolved_output


def _cleanup_ephemeral_local_output(
    session_id: int,
    output_dir: Path,
    temp_root: Path,
    family: str,
) -> bool:
    recognized = _recognized_local_output_dir(
        session_id=session_id,
        output_dir=output_dir,
        temp_root=temp_root,
        family=family,
    )
    if recognized is None:
        logger.warning(
            "video_announce: refusing unrecognized ephemeral output cleanup session=%s family=%s path=%s",
            session_id,
            family,
            output_dir,
        )
        return False
    try:
        shutil.rmtree(recognized)
        logger.info(
            "video_announce: deleted ephemeral local output session=%s family=%s path=%s",
            session_id,
            family,
            recognized,
        )
        return True
    except Exception:
        logger.exception(
            "video_announce: failed to delete ephemeral local output session=%s family=%s path=%s",
            session_id,
            family,
            recognized,
        )
        return False


def _acquire_local_output_lock(session_id: int, temp_root: Path):
    if not isinstance(session_id, int) or session_id <= 0:
        return None
    temp_root.mkdir(parents=True, exist_ok=True)
    handle = (temp_root / f".videoannounce-{session_id}.output.lock").open("a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def _release_local_output_lock(handle) -> None:
    if handle is None:
        return
    try:
        fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


async def _cleanup_terminal_local_output(
    db: Database,
    session_id: int | None,
    output_dir: Path,
    *,
    temp_root: Path,
    output_lock=None,
) -> bool:
    """Remove a published session's recognized local Kaggle output tree.

    The durable DB row is reloaded immediately before deletion.  ``DONE`` is
    deliberately not eligible: it is a transitional, publish-recoverable state.
    A test publication with a configured main target is also non-terminal until
    main publication succeeds.  Failed and publish-blocked output is preserved
    for narrow recovery.
    """

    owned_lock = None
    if output_lock is None:
        owned_lock = _acquire_local_output_lock(int(session_id or 0), Path(temp_root))
        if owned_lock is None:
            logger.info(
                "video_announce: preserving local output owned by another poller session=%s path=%s",
                session_id,
                output_dir,
            )
            return False
        output_lock = owned_lock
    recognized = _recognized_local_output_dir(
        session_id=session_id,
        output_dir=output_dir,
        temp_root=temp_root,
    )
    if recognized is None:
        logger.warning(
            "video_announce: refusing unrecognized local output cleanup session=%s path=%s root=%s",
            session_id,
            output_dir,
            temp_root,
        )
        _release_local_output_lock(owned_lock)
        return False
    try:
        async with db.get_session() as session:
            fresh = await session.get(VideoAnnounceSession, session_id)
        published_test_still_needs_main = (
            fresh is not None
            and fresh.status == VideoAnnounceSessionStatus.PUBLISHED_TEST
            and fresh.main_chat_id is not None
        )
        if (
            fresh is None
            or fresh.status not in _LOCAL_OUTPUT_CLEANUP_STATUSES
            or published_test_still_needs_main
            or fresh.finished_at is None
            or fresh.published_at is None
            or not fresh.video_url
        ):
            logger.info(
                "video_announce: preserving local output session=%s status=%s path=%s",
                session_id,
                getattr(fresh, "status", None),
                recognized,
            )
            return False
        await asyncio.to_thread(shutil.rmtree, recognized)
        logger.info(
            "video_announce: deleted terminal published local output session=%s status=%s path=%s",
            session_id,
            fresh.status,
            recognized,
        )
        return True
    except Exception:
        logger.exception(
            "video_announce: failed to clean terminal local output session=%s path=%s",
            session_id,
            recognized,
        )
        return False
    finally:
        _release_local_output_lock(owned_lock)


async def reconcile_terminal_local_outputs(
    db: Database,
    *,
    temp_root: Path | None = None,
    live_session_ids: set[int] | None = None,
) -> int:
    """Retry bounded cleanup of published render trees after a restart."""

    root = Path(temp_root or os.getenv("TMPDIR", "/tmp"))
    if not root.is_dir() or root.is_symlink():
        return 0
    live = live_session_ids if live_session_ids is not None else await _live_video_ledger_session_ids(db)
    removed = 0
    for path in sorted(root.iterdir()):
        match = re.fullmatch(r"videoannounce-([1-9][0-9]*)", path.name)
        if not match or path.is_symlink() or not path.is_dir():
            continue
        session_id = int(match.group(1))
        if session_id in live or _poller_active(session_id):
            continue
        if await _cleanup_terminal_local_output(
            db,
            session_id,
            path,
            temp_root=root,
        ):
            removed += 1
    return removed

async def run_kernel_poller(
    db: Database,
    client: KaggleClient,
    session_obj: VideoAnnounceSession,
    *,
    bot,
    notify_chat_id: int,
    test_chat_id: int | None,
    main_chat_id: int | None,
    status_chat_id: int | None = None,
    status_message_id: int | None = None,
    poll_interval: int = 60,
    timeout_minutes: int = VIDEO_KAGGLE_TIMEOUT_MINUTES,
    download_dir: Path | None = None,
    dataset_slug: str | None = None,
) -> None:
    started_at = datetime.now(timezone.utc)
    deadline = started_at + timedelta(minutes=timeout_minutes)
    absolute_timeout_minutes = max(timeout_minutes, VIDEO_KAGGLE_ABSOLUTE_TIMEOUT_MINUTES)
    absolute_deadline = started_at + timedelta(minutes=absolute_timeout_minutes)
    kernel_ref = session_obj.kaggle_kernel_ref
    if not kernel_ref:
        await _update_status(
            db,
            session_obj.id,
            status=VideoAnnounceSessionStatus.FAILED,
            error="kernel reference missing",
        )
        await bot.send_message(notify_chat_id, "Не указан kernel для сессии")
        return
    status_message = await update_status_message(
        bot,
        session_obj,
        {},
        db=db,
        chat_id=status_chat_id,
        message_id=status_message_id,
        allow_send=True,
        note="Старт отслеживания Kaggle",
    )
    if status_message:
        status_chat_id, status_message_id = status_message
    
    # Track consecutive unknown statuses
    unknown_status_count = 0
    # Kaggle kernel can take a while to start, API returns None during startup
    # At ~1 min poll interval, 30 attempts = ~30 minutes before failing
    MAX_UNKNOWN_STATUS_COUNT = 30

    # INC-2026-05-26 round 3: track when the kernel first went QUEUED to drive
    # an auto-demote to a lower accelerator tier when P100 is congested.
    from . import accel_pref as _accel_pref

    queued_since: datetime | None = None

    timed_out = True
    status: dict = {}
    timeout_status: dict | None = None
    timeout_note_sent_at: datetime | None = None
    output_probe_failure_error: str | None = None
    output_probe_failure_message: str | None = None
    output_probe_status: dict | None = None
    while True:
        now = datetime.now(timezone.utc)
        if now >= deadline:
            heartbeat = await _fresh_kaggle_ledger_heartbeat(
                db,
                f"videoannounce:{session_obj.id}",
                now=now,
            )
            if heartbeat and now < absolute_deadline:
                extension_deadline = now + timedelta(
                    minutes=VIDEO_KAGGLE_REMOTE_ALIVE_EXTENSION_MINUTES
                )
                deadline = min(extension_deadline, absolute_deadline)
                progress_label = ""
                progress = heartbeat.get("progress") or {}
                if isinstance(progress, dict) and progress.get("progress_label"):
                    progress_label = f" · {progress.get('progress_label')}"
                logger.warning(
                    "video_announce: fixed timeout reached but Kaggle ledger is alive; extending poller "
                    "session=%s kernel=%s heartbeat_age=%.1fs phase=%s new_deadline=%s absolute_deadline=%s",
                    session_obj.id,
                    kernel_ref,
                    float(heartbeat.get("age_seconds") or 0),
                    heartbeat.get("phase"),
                    deadline.isoformat(),
                    absolute_deadline.isoformat(),
                )
                if (
                    timeout_note_sent_at is None
                    or (now - timeout_note_sent_at).total_seconds() >= 60 * 60
                ):
                    await bot.send_message(
                        notify_chat_id,
                        (
                            f"⏳ Сессия #{session_obj.id}: лимит {timeout_minutes} мин достигнут, "
                            f"но Kaggle живой ({heartbeat.get('phase') or heartbeat.get('status')}"
                            f"{progress_label}); продолжаю ждать до {deadline:%H:%M UTC}."
                        ),
                    )
                    timeout_note_sent_at = now
                continue
            break
        try:
            status = await asyncio.to_thread(client.get_kernel_status, kernel_ref)
            logger.info(
                "video_announce: kernel status poll session=%s kernel=%s status=%s",
                session_obj.id,
                kernel_ref,
                status.get("status"),
            )
        except Exception:
            logger.exception("video_announce: kernel status failed session=%s", session_obj.id)
            status = {}
        await update_status_message(
            bot,
            session_obj,
            status,
            db=db,
            chat_id=status_chat_id,
            message_id=status_message_id,
            allow_send=True,
        )
        state = str(status.get("status") or "").lower()
        
        # Handle unknown/empty status
        if not state or state in {"none", "unknown"}:
            unknown_status_count += 1
            logger.warning(
                "video_announce: unknown kernel status session=%s count=%s/%s full_response=%s",
                session_obj.id,
                unknown_status_count,
                MAX_UNKNOWN_STATUS_COUNT,
                status,
            )
            if unknown_status_count >= MAX_UNKNOWN_STATUS_COUNT:
                error_msg = f"Kaggle API returns unknown status after {MAX_UNKNOWN_STATUS_COUNT} attempts"
                heartbeat = await _fresh_kaggle_ledger_heartbeat(
                    db,
                    f"videoannounce:{session_obj.id}",
                    now=datetime.now(timezone.utc),
                )
                if heartbeat:
                    logger.warning(
                        "video_announce: Kaggle status is unknown but ledger heartbeat is fresh; "
                        "continuing poll session=%s kernel=%s heartbeat_age=%.1fs phase=%s",
                        session_obj.id,
                        kernel_ref,
                        float(heartbeat.get("age_seconds") or 0),
                        heartbeat.get("phase"),
                    )
                    unknown_status_count = 0
                    await asyncio.sleep(poll_interval)
                    continue
                logger.warning(
                    "video_announce: Kaggle status remained unknown; probing output before failing "
                    "session=%s kernel=%s",
                    session_obj.id,
                    kernel_ref,
                )
                output_probe_failure_error = error_msg
                output_probe_failure_message = (
                    f"⚠️ Сессия #{session_obj.id}: Kaggle API не возвращает статус, "
                    "и готовый output не подтвердился.\n"
                    "Проверьте ноутбук вручную на kaggle.com"
                )
                output_probe_status = dict(status)
                timed_out = False
                break
            await asyncio.sleep(poll_interval)
            continue
        else:
            # Reset counter if we get a valid status
            unknown_status_count = 0
        
        # INC-2026-05-26 round 3: stuck-in-queue auto-demote.
        # If the kernel sits in QUEUED beyond the threshold (default 5 min),
        # write an accel-pref demotion for this slug so the *next* push uses
        # T4 instead, fail this session in our DB, and exit. The current
        # zombie Kaggle session can't be cancelled programmatically (Kaggle
        # public API rejects it), so it stays QUEUED on Kaggle's side and
        # will eventually run uselessly — that's acceptable; the kernel-slug
        # lock prevents us from piling up more versions on the same slug.
        if state == "queued":
            if queued_since is None:
                queued_since = datetime.now(timezone.utc)
            else:
                stuck_for = (datetime.now(timezone.utc) - queued_since).total_seconds()
                if stuck_for >= _accel_pref.queue_demote_threshold_sec():
                    slug = _accel_pref_resolve_slug(kernel_ref)
                    # Skip auto-demote when this slug has no configured ladder
                    # (e.g. CPU-only kernels like crumple-video). Without this
                    # check the demote call returns None ("ladder exhausted")
                    # and we wrongly hard-fail a CPU run that would have
                    # finished naturally. INC-2026-05-26 round 4 regression.
                    if slug and _accel_pref.ladder_for(slug):
                        current_pref = await _accel_pref.read_active_pref(db, slug)
                        current_tier = (
                            current_pref.tier if current_pref else _accel_pref.TIER_DEFAULT
                        )
                        reason = (
                            f"queue >{int(stuck_for)}s on {current_tier} "
                            f"(session #{session_obj.id})"
                        )
                        new_pref = await _accel_pref.demote(
                            db, slug, current_tier=current_tier, reason=reason
                        )
                        if new_pref is None:
                            err_msg = (
                                f"kaggle_queue: ladder exhausted on {current_tier} "
                                f"after {int(stuck_for)}s"
                            )
                            session_obj = await _update_status(
                                db,
                                session_obj.id,
                                status=VideoAnnounceSessionStatus.FAILED,
                                error=err_msg,
                            )
                            await bot.send_message(
                                notify_chat_id,
                                f"🛑 Сессия #{session_obj.id if session_obj else '?'}: все Kaggle тиеры исчерпаны для {slug} ({int(stuck_for)}с в очереди). Рендер не пойдёт; нужно ручное вмешательство.",
                            )
                            return
                        err_msg = (
                            f"queue_demote: {current_tier}->{new_pref.tier} "
                            f"after {int(stuck_for)}s queue"
                        )
                        session_obj = await _update_status(
                            db,
                            session_obj.id,
                            status=VideoAnnounceSessionStatus.FAILED,
                            error=err_msg,
                        )
                        expires_local = new_pref.expires_at.astimezone(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M UTC"
                        )
                        # INC-2026-05-26 round 5: delete the session-specific
                        # dataset so the zombie Kaggle run (which we cannot
                        # cancel via API) fails at the mount step instead of
                        # mounting our payload + Telegram bundle and
                        # conflicting with the next push from a different IP
                        # (previous incident: AuthKeyDuplicatedError when
                        # v128 zombie + v129 fresh both opened the same
                        # Telegram session simultaneously).
                        await _cleanup_dataset(client, dataset_slug)
                        await bot.send_message(
                            notify_chat_id,
                            (
                                f"⚠️ Сессия #{session_obj.id if session_obj else '?'}: "
                                f"Kaggle очередь {int(stuck_for)}s на {current_tier} → "
                                f"следующие пуши {slug} идут на {new_pref.tier} "
                                f"до {expires_local}. Dataset зомби-сессии удалён, "
                                f"чтобы избежать конфликта Telegram-сессии. "
                                f"Watchdog подберёт пропущенный слот."
                            ),
                        )
                        return
        else:
            queued_since = None

        if state == "complete":
            timed_out = False
            break
        if state in {
            "cancel_acknowledged",
            "cancel_requested",
            "cancelled",
            "canceled",
        }:
            failure_msg = status.get("failureMessage") or status.get("failure_message") or ""
            error_detail = f"{state}: {failure_msg}" if failure_msg else str(status)
            logger.warning(
                "video_announce: kernel cancelled session=%s kernel=%s error=%s",
                session_obj.id,
                kernel_ref,
                error_detail,
            )
            session_obj = await _update_status(
                db,
                session_obj.id,
                status=VideoAnnounceSessionStatus.FAILED,
                error=error_detail,
            )
            if not session_obj:
                return
            await update_status_message(
                bot,
                session_obj,
                status,
                db=db,
                chat_id=status_chat_id,
                message_id=status_message_id,
                allow_send=True,
            )
            await bot.send_message(
                notify_chat_id, f"❌ Сессия #{session_obj.id} отменена в Kaggle: {state}"
            )
            await _download_and_send_logs(
                client,
                kernel_ref,
                bot,
                notify_chat_id,
                session_obj.id,
                download_dir=download_dir,
                caption_prefix="❌ Логи отменённого Kaggle run",
            )
            await _cleanup_dataset(client, dataset_slug)
            return
        if state in {"error", "failed"}:
            failure_msg = status.get("failureMessage") or status.get("failure_message") or ""
            error_detail = f"{state}: {failure_msg}" if failure_msg else str(status)
            logger.warning(
                "video_announce: kernel failed session=%s kernel=%s error=%s",
                session_obj.id,
                kernel_ref,
                error_detail,
            )
            heartbeat = await _fresh_kaggle_ledger_heartbeat(
                db,
                f"videoannounce:{session_obj.id}",
                now=datetime.now(timezone.utc),
            )
            if heartbeat:
                display_status = dict(status)
                display_status["status"] = heartbeat.get("status") or "alive"
                display_status["phase"] = heartbeat.get("phase")
                display_status.pop("failureMessage", None)
                display_status.pop("failure_message", None)
                display_status.pop("error", None)
                logger.warning(
                    "video_announce: Kaggle terminal status conflicts with fresh notebook heartbeat; "
                    "continuing poll session=%s kernel=%s provider_state=%s heartbeat_age=%.1fs phase=%s",
                    session_obj.id,
                    kernel_ref,
                    state,
                    float(heartbeat.get("age_seconds") or 0),
                    heartbeat.get("phase"),
                )
                await update_status_message(
                    bot,
                    session_obj,
                    display_status,
                    db=db,
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    allow_send=True,
                    note=(
                        f"Kaggle API сообщил {state}, но notebook heartbeat свежий "
                        f"({int(float(heartbeat.get('age_seconds') or 0))}с); продолжаю ждать."
                    ),
                )
                await asyncio.sleep(poll_interval)
                continue
            logger.warning(
                "video_announce: probing Kaggle output before accepting terminal failure "
                "session=%s kernel=%s state=%s",
                session_obj.id,
                kernel_ref,
                state,
            )
            output_probe_failure_error = error_detail
            output_probe_failure_message = (
                f"❌ Сессия #{session_obj.id} завершилась ошибкой Kaggle: {state}; "
                "готовый output не подтвердился."
            )
            output_probe_status = dict(status)
            timed_out = False
            break
        await asyncio.sleep(poll_interval)
    if timed_out:
        timeout_status = status if "status" in locals() else {}
        logger.warning(
            "video_announce: kernel timeout session=%s kernel=%s timeout_min=%s",
            session_obj.id,
            kernel_ref,
            timeout_minutes,
        )
        logger.warning(
            "video_announce: probing Kaggle output before timeout failure session=%s kernel=%s",
            session_obj.id,
            kernel_ref,
        )
        output_probe_failure_error = f"timeout after {timeout_minutes}min"
        output_probe_failure_message = (
            f"⏱️ Сессия #{session_obj.id} не завершилась за {timeout_minutes} минут; "
            "готовый output не подтвердился."
        )
        output_probe_status = dict(timeout_status)

    tmp_dir = download_dir or Path(os.getenv("TMPDIR", "/tmp"))
    output_dir = tmp_dir / f"videoannounce-{session_obj.id}"
    output_lock = _acquire_local_output_lock(session_obj.id, tmp_dir)
    if output_lock is None:
        logger.warning(
            "video_announce: output phase already owned by another poller session=%s",
            session_obj.id,
        )
        await _cleanup_dataset(client, dataset_slug)
        return
    render_output_ready = False
    ready_video_name: str | None = None
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        max_attempts = 3
        files: list[str] = []
        for attempt in range(1, max_attempts + 1):
            try:
                files = await asyncio.to_thread(
                    client.download_kernel_output,
                    kernel_ref,
                    path=output_dir,
                    force=True,
                    quiet=True,
                )
                break
            except Exception:
                logger.exception(
                    "video_announce: kernel output download failed attempt=%s/%s session=%s",
                    attempt,
                    max_attempts,
                    session_obj.id,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(5 * attempt)
                else:
                    raise
        paths = [output_dir / f for f in files]
        output_files = _expand_output_paths(paths)
        video_path = _find_session_video(session_obj, output_files)
        log_files = _find_logs(output_files)
        story_report_path = _find_story_report(output_files)
        story_report = _load_story_report(story_report_path)
        await _persist_partner_story_metadata(db, session_obj, story_report)
        if not video_path:
            logger.warning(
                "video_announce: no video in output session=%s files=%s",
                session_obj.id,
                files or [p.name for p in output_files],
            )
            story_failure = _story_failure_message(story_report)
            if story_failure:
                session_obj = await _update_status(
                    db,
                    session_obj.id,
                    status=VideoAnnounceSessionStatus.PUBLISH_BLOCKED,
                    error=story_failure,
                )
                if not session_obj:
                    return
                await update_status_message(
                    bot,
                    session_obj,
                    status,
                    db=db,
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    allow_send=True,
                    note="Story publish/preflight заблокировал render",
                )
                await bot.send_message(
                    notify_chat_id,
                    f"⚠️ Сессия #{session_obj.id}: {story_failure}. Полный Kaggle rerender не требуется без изменения target/access.",
                )
                if log_files:
                    await _send_logs(
                        bot,
                        notify_chat_id,
                        log_files,
                        caption=f"⚠️ Логи preflight/publish blocker сессии #{session_obj.id}",
                    )
                return
            expected_video_name = _expected_video_name(session_obj)
            missing_video_error = output_probe_failure_error or (
                f"missing expected video output: {expected_video_name}"
                if expected_video_name
                else "missing video output"
            )
            session_obj = await _update_status(
                db,
                session_obj.id,
                status=VideoAnnounceSessionStatus.FAILED,
                error=missing_video_error,
            )
            if not session_obj:
                return
            await update_status_message(
                bot,
                session_obj,
                status,
                db=db,
                chat_id=status_chat_id,
                message_id=status_message_id,
                allow_send=True,
            )
            await bot.send_message(notify_chat_id, "❌ Видео не найдено в выводе kernel")
            # Send logs even when video is missing
            if log_files:
                await _send_logs(
                    bot,
                    notify_chat_id,
                    log_files,
                    caption=f"❌ Логи (нет видео) сессии #{session_obj.id}",
                )
            return
        render_output_ready = True
        ready_video_name = video_path.name
        if video_path.stat().st_size > VIDEO_MAX_MB * 1024 * 1024:
            session_obj = await _update_status(
                db,
                session_obj.id,
                status=VideoAnnounceSessionStatus.PUBLISH_BLOCKED,
                error=f"video exceeds {VIDEO_MAX_MB}MB",
                video_url=video_path.name,
            )
            if not session_obj:
                return
            await update_status_message(
                bot,
                session_obj,
                status,
                db=db,
                chat_id=status_chat_id,
                message_id=status_message_id,
                allow_send=True,
            )
            await bot.send_message(
                notify_chat_id,
                f"⚠️ Видео из сессии #{session_obj.id} превышает {VIDEO_MAX_MB} MB. "
                "Артефакт есть; нужен узкий publish/encode fix, не blind rerender.",
            )
            return
        session_obj = await _update_status(
            db,
            session_obj.id,
            status=VideoAnnounceSessionStatus.DONE,
            video_url=video_path.name,
        )
        if not session_obj:
            return
        await update_status_message(
            bot,
            session_obj,
            status,
            db=db,
            chat_id=status_chat_id,
            message_id=status_message_id,
            allow_send=True,
        )
        caption = await _build_video_caption(db, session_obj)
        story_failure = _story_failure_message(story_report)
        if story_failure:
            recovered = await run_story_publish_only_recovery(
                db,
                client,
                session_obj,
                bot=bot,
                notify_chat_id=notify_chat_id,
                video_path=video_path,
                story_report=story_report,
                download_dir=download_dir,
            )
            if recovered:
                refreshed = await _load_session_for_status(db, session_obj.id)
                if refreshed:
                    await update_status_message(
                        bot,
                        refreshed,
                        status,
                        db=db,
                        chat_id=status_chat_id,
                        message_id=status_message_id,
                        allow_send=True,
                        note="Story publish восстановлен без ререндера",
                    )
            else:
                session_obj = await _update_status(
                    db,
                    session_obj.id,
                    status=VideoAnnounceSessionStatus.PUBLISH_BLOCKED,
                    error=story_failure,
                )
                if not session_obj:
                    return
                await update_status_message(
                    bot,
                    session_obj,
                    status,
                    db=db,
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    allow_send=True,
                    note="Story publish завершился ошибкой",
                )
                await bot.send_message(
                    notify_chat_id,
                    f"⚠️ Сессия #{session_obj.id}: {story_failure}",
                )
                try:
                    await _send_video_with_preview(
                        bot,
                        notify_chat_id,
                        video_path,
                        caption=f"{caption}\n\n⚠️ Story publish failed",
                    )
                except Exception:
                    logger.warning(
                        "video_announce: failed to send failed-story video to notify chat %s",
                        notify_chat_id,
                        exc_info=True,
                    )
                report_and_logs = []
                if story_report_path:
                    report_and_logs.append(story_report_path)
                report_and_logs.extend(
                    file for file in log_files if story_report_path is None or file != story_report_path
                )
                if report_and_logs:
                    await _send_logs(
                        bot,
                        notify_chat_id,
                        report_and_logs,
                        caption=f"⚠️ Story publish report сессии #{session_obj.id}",
                    )
                return
        await _maybe_register_kenigsberg_manifest(db, session_obj, output_files)
        target_test = test_chat_id or notify_chat_id
        test_delivery_ok = False
        try:
            await _send_video_with_preview(bot, target_test, video_path, caption=caption)
            test_delivery_ok = True
        except Exception as e:
            logger.warning("video_announce: failed to send video to test chat %s: %s", target_test, e)
            # Fallback to notify_chat_id if test_chat_id fails.  A fallback
            # failure is a post-render delivery blocker, not a render/output
            # failure, so keep it inside this phase instead of letting the
            # broad download handler turn it into a full-rerender signal.
            if target_test != notify_chat_id:
                try:
                    await _send_video_with_preview(bot, notify_chat_id, video_path, caption=caption)
                    test_delivery_ok = True
                except Exception as fallback_exc:
                    logger.warning(
                        "video_announce: failed to send video fallback to notify chat %s: %s",
                        notify_chat_id,
                        fallback_exc,
                    )
        try:
            await _send_logs(bot, notify_chat_id, log_files, caption=f"✅ Логи сессии #{session_obj.id}")
        except Exception as logs_exc:
            logger.warning(
                "video_announce: failed to send post-render logs session=%s chat=%s: %s",
                session_obj.id,
                notify_chat_id,
                logs_exc,
            )
        if not test_delivery_ok:
            session_obj = await _update_status(
                db,
                session_obj.id,
                status=VideoAnnounceSessionStatus.PUBLISH_BLOCKED,
                error="post-render bot delivery failed",
            )
            if session_obj:
                await update_status_message(
                    bot,
                    session_obj,
                    status,
                    db=db,
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    allow_send=True,
                    note="Видео готово; доставка ботом заблокирована",
                )
                try:
                    await bot.send_message(
                        notify_chat_id,
                        f"⚠️ Сессия #{session_obj.id}: видео готово, но бот не смог доставить mp4 в тест/notify чат. Полный Kaggle rerender не требуется.",
                    )
                except Exception:
                    logger.warning(
                        "video_announce: failed to send post-render blocked notification session=%s",
                        session_obj.id,
                        exc_info=True,
                    )
            return
        session_obj = await _update_status(
            db,
            session_obj.id,
            status=VideoAnnounceSessionStatus.PUBLISHED_TEST,
        )
        await _record_viewer_facing_test_promo_exposures(
            db,
            session_obj,
            target_chat_id=target_test,
        )
        if session_obj:
            await update_status_message(
                bot,
                session_obj,
                status,
                db=db,
                chat_id=status_chat_id,
                message_id=status_message_id,
                allow_send=True,
                note="Отправлено в тестовый канал",
            )
        if main_chat_id:
            try:
                await _send_video_with_preview(bot, main_chat_id, video_path, caption=caption)
                await _mark_published_main(db, session_obj)
                async with db.get_session() as session:
                    refreshed = await session.get(VideoAnnounceSession, session_obj.id)
                if refreshed:
                    await update_status_message(
                        bot,
                        refreshed,
                        status,
                        db=db,
                        chat_id=status_chat_id,
                        message_id=status_message_id,
                        allow_send=True,
                        note="Опубликовано в основном канале",
                    )
            except Exception as e:
                logger.warning("video_announce: failed to send video to main chat %s: %s", main_chat_id, e)
    except Exception as exc:
        if render_output_ready:
            logger.exception(
                "video_announce: post-render handling failed session=%s kernel=%s",
                session_obj.id,
                kernel_ref,
            )
            session_obj = await _update_status(
                db,
                session_obj.id,
                status=VideoAnnounceSessionStatus.PUBLISH_BLOCKED,
                error=f"post-render handling failed: {exc}",
                video_url=ready_video_name,
            )
            if session_obj:
                await update_status_message(
                    bot,
                    session_obj,
                    status,
                    db=db,
                    chat_id=status_chat_id,
                    message_id=status_message_id,
                    allow_send=True,
                    note="Видео готово; post-render доставка/фанаут заблокированы",
                )
                try:
                    await bot.send_message(
                        notify_chat_id,
                        f"⚠️ Сессия #{session_obj.id}: видео уже готово, но post-render обработка упала. Полный Kaggle rerender запрещён; нужен узкий retry/reconcile.",
                    )
                except Exception:
                    logger.warning(
                        "video_announce: failed to send post-render exception notification session=%s",
                        session_obj.id,
                        exc_info=True,
                    )
            return
        logger.exception(
            "video_announce: failed to download kernel output session=%s kernel=%s",
            session_obj.id,
            kernel_ref,
        )
        final_error = output_probe_failure_error or "kernel output download failed"
        session_obj = await _update_status(
            db,
            session_obj.id,
            status=VideoAnnounceSessionStatus.FAILED,
            error=final_error,
        )
        if session_obj:
            await update_status_message(
                bot,
                session_obj,
                output_probe_status or status,
                db=db,
                chat_id=status_chat_id,
                message_id=status_message_id,
                allow_send=True,
            )
        await bot.send_message(
            notify_chat_id,
            output_probe_failure_message
            or f"⚠️ Сессия #{session_obj.id}: не удалось скачать вывод kernel",
        )
    finally:
        await _cleanup_dataset(client, dataset_slug)
        await _cleanup_terminal_local_output(
            db,
            session_obj.id,
            output_dir,
            temp_root=tmp_dir,
            output_lock=output_lock,
        )
        _release_local_output_lock(output_lock)


async def resume_rendering_sessions(db: Database, bot, *, chat_id: int | None = None) -> int:
    await reconcile_terminal_rendering_sessions(db, bot, chat_id=chat_id)
    live_ledger_session_ids = await _live_video_ledger_session_ids(db)
    await reconcile_terminal_local_outputs(
        db,
        live_session_ids=live_ledger_session_ids,
    )
    resumable_live_statuses = {
        VideoAnnounceSessionStatus.RENDERING,
        VideoAnnounceSessionStatus.FAILED,
    }
    async with db.get_session() as session:
        query = select(VideoAnnounceSession).where(
            VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING
        )
        if live_ledger_session_ids:
            query = select(VideoAnnounceSession).where(
                (VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING)
                | (
                    VideoAnnounceSession.id.in_(live_ledger_session_ids)
                    & VideoAnnounceSession.status.in_(resumable_live_statuses)
                )
            )
        res = await session.execute(query)
        sessions = res.scalars().all()
    if not sessions:
        return 0
    recovered = 0
    client = KaggleClient()
    for sess in sessions:
        kernel_ref = str(sess.kaggle_kernel_ref or "").strip()
        if not kernel_ref:
            continue
        notify_chat_id = await _resolve_recovery_notify_chat_id(
            db,
            sess,
            chat_id=chat_id,
        )
        if not notify_chat_id:
            continue
        dataset_slug = str(sess.kaggle_dataset or "").strip()
        if not dataset_slug:
            grace_deadline = _local_handoff_grace_deadline(sess)
            now_utc = datetime.now(timezone.utc)
            if grace_deadline is not None and now_utc < grace_deadline:
                logger.warning(
                    "video_announce: skipping recovery for pre-handoff session without dataset "
                    "session_id=%s kernel_ref=%s grace_until=%s",
                    sess.id,
                    kernel_ref,
                    grace_deadline.isoformat(),
                )
                continue
            logger.error(
                "video_announce: refusing to resume session_id=%s without Kaggle dataset kernel_ref=%s",
                sess.id,
                kernel_ref,
            )
            failed = await _update_status(
                db,
                sess.id,
                status=VideoAnnounceSessionStatus.FAILED,
                error="runtime restart before Kaggle handoff; rerun required",
            )
            if failed:
                await bot.send_message(
                    notify_chat_id,
                    (
                        f"⚠️ Сессия #{failed.id}: рантайм перезапустился до подтверждённого запуска Kaggle.\n"
                        "Сессия переведена в FAILED; нужен повторный запуск."
                    ),
                )
            continue
        if sess.id in live_ledger_session_ids and sess.status == VideoAnnounceSessionStatus.FAILED:
            async with db.get_session() as session:
                obj = await session.get(VideoAnnounceSession, sess.id)
                if obj and obj.status == VideoAnnounceSessionStatus.FAILED:
                    logger.warning(
                        "video_announce: reviving false-failed session from fresh Kaggle heartbeat "
                        "session_id=%s kernel_ref=%s dataset=%s",
                        sess.id,
                        kernel_ref,
                        dataset_slug,
                    )
                    obj.status = VideoAnnounceSessionStatus.RENDERING
                    obj.finished_at = None
                    obj.error = None
                    session.add(obj)
                    await session.commit()
                    await session.refresh(obj)
                    sess = obj
        if _is_local_kernel_ref(kernel_ref):
            grace_deadline = _local_handoff_grace_deadline(sess)
            now_utc = datetime.now(timezone.utc)
            if grace_deadline is not None and now_utc < grace_deadline:
                logger.warning(
                    "video_announce: skipping immediate fail for fresh local handoff session_id=%s kernel_ref=%s "
                    "grace_until=%s",
                    sess.id,
                    kernel_ref,
                    grace_deadline.isoformat(),
                )
                continue
            logger.error(
                "video_announce: refusing to resume session_id=%s with local kernel ref=%s",
                sess.id,
                kernel_ref,
            )
            failed = await _update_status(
                db,
                sess.id,
                status=VideoAnnounceSessionStatus.FAILED,
                error="runtime restart before Kaggle handoff; rerun required",
            )
            if failed:
                await bot.send_message(
                    notify_chat_id,
                    (
                        f"⚠️ Сессия #{failed.id}: рантайм перезапустился до подтверждённого запуска Kaggle.\n"
                        "Сессия переведена в FAILED; нужен повторный запуск."
                    ),
                )
            continue
        if _poller_active(sess.id):
            continue
        start_kernel_poller_task(
            db,
            client,
            sess,
            bot=bot,
            notify_chat_id=notify_chat_id,
            test_chat_id=sess.test_chat_id,
            main_chat_id=sess.main_chat_id,
            poll_interval=60,
            timeout_minutes=VIDEO_KAGGLE_TIMEOUT_MINUTES,
            dataset_slug=dataset_slug,
        )
        recovered += 1
    return recovered


async def reset_stuck_sessions(db: Database, *, max_age_minutes: int = 30) -> int:
    """Move long-running RENDERING sessions into FAILED state."""

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    updated = 0
    async with db.get_session() as session:
        res = await session.execute(
            select(VideoAnnounceSession).where(
                VideoAnnounceSession.status == VideoAnnounceSessionStatus.RENDERING,
                VideoAnnounceSession.started_at < cutoff,
            )
        )
        for sess in res.scalars():
            sess.status = VideoAnnounceSessionStatus.FAILED
            sess.finished_at = datetime.now(timezone.utc)
            sess.error = "stuck rendering watchdog"
            updated += 1
            logger.warning("video_announce reset stuck session_id=%s", sess.id)
        if updated:
            await session.commit()
    return updated
