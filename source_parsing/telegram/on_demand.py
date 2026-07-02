from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from admin_chat import resolve_superadmin_chat_id
from db import Database
from telegram_sources import normalize_tg_username

from .service import run_telegram_monitor

logger = logging.getLogger(__name__)

DEFAULT_ON_DEMAND_SOURCE = "kraftmarket39"
BUSY_ERRORS = {
    "already_running",
    "already_running_global_lock",
    "remote_telegram_session_busy",
}

_DISPATCH_LOCK = asyncio.Lock()


def _env_enabled(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def is_on_demand_enabled() -> bool:
    return _env_enabled("ENABLE_TG_MONITORING_ON_DEMAND", default=True)


def debounce_seconds() -> int:
    raw = (os.getenv("TG_MONITORING_ON_DEMAND_DEBOUNCE_SECONDS") or "").strip()
    try:
        return max(0, min(int(raw or "600"), 24 * 3600))
    except Exception:
        return 600


def retry_seconds() -> int:
    raw = (os.getenv("TG_MONITORING_ON_DEMAND_RETRY_SECONDS") or "").strip()
    try:
        return max(60, min(int(raw or "600"), 24 * 3600))
    except Exception:
        return 600


def scheduler_poll_seconds() -> int:
    raw = (os.getenv("TG_MONITORING_ON_DEMAND_POLL_SECONDS") or "").strip()
    try:
        return max(30, min(int(raw or "60"), 3600))
    except Exception:
        return 60


def max_runs_per_tick() -> int:
    raw = (os.getenv("TG_MONITORING_ON_DEMAND_MAX_RUNS_PER_TICK") or "").strip()
    try:
        return max(1, min(int(raw or "1"), 10))
    except Exception:
        return 1


def allowed_source_usernames() -> set[str]:
    raw = (os.getenv("TG_MONITORING_ON_DEMAND_SOURCES") or DEFAULT_ON_DEMAND_SOURCE).strip()
    usernames = {
        normalized
        for part in raw.replace(";", ",").split(",")
        if (normalized := normalize_tg_username(part))
    }
    return usernames or {DEFAULT_ON_DEMAND_SOURCE}




def is_private_forward_message(message: Any) -> bool:
    """Return True only for forwarded messages sent directly to the bot.

    Reposts in channels/groups are signals for channel-specific automation and
    must not fall through into the manual add-event flow, because that flow sends
    operator service messages to ``message.chat.id``.
    """

    chat = getattr(message, "chat", None)
    chat_type = str(getattr(chat, "type", "") or "").strip().lower()
    if chat_type != "private":
        return False
    model_extra = getattr(message, "model_extra", None) or {}
    return bool(
        getattr(message, "forward_date", None)
        or getattr(message, "forward_from_chat", None)
        or getattr(message, "forward_origin", None)
        or (isinstance(model_extra, dict) and "forward_origin" in model_extra)
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sql_dt(value: datetime | None = None) -> str:
    dt = value or _utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _message_dt(message: Any) -> datetime | None:
    value = getattr(message, "date", None)
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def enqueue_on_demand_channel_post(
    db: Database,
    message: Any,
    *,
    now: datetime | None = None,
) -> bool:
    """Persist a source-specific Telegram Monitoring request for a channel post.

    Bot API updates are only a fast signal: the actual extraction/import remains
    the existing source-scoped Telegram Monitoring + Smart Update pipeline.
    """

    if not is_on_demand_enabled():
        return False

    chat = getattr(message, "chat", None)
    username = normalize_tg_username(getattr(chat, "username", None))
    if not username or username not in allowed_source_usernames():
        return False

    try:
        message_id = int(getattr(message, "message_id", 0) or 0)
    except Exception:
        message_id = 0
    if message_id <= 0:
        return False

    now = now or _utc_now()
    due_at = now + timedelta(seconds=debounce_seconds())
    message_date = _message_dt(message)
    chat_id = getattr(chat, "id", None)
    try:
        chat_id_int = int(chat_id) if chat_id is not None else None
    except Exception:
        chat_id_int = None

    async with db.raw_conn() as conn:
        cursor = await conn.execute(
            """
            SELECT id, enabled
            FROM telegram_source
            WHERE username = ?
            """,
            (username,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if not row or not bool(row[1]):
            logger.info(
                "tg_on_demand.skip source=%s message_id=%s reason=source_missing_or_disabled",
                username,
                message_id,
            )
            return False
        source_id = int(row[0])

        await conn.execute(
            """
            INSERT OR IGNORE INTO telegram_source_force_message(source_id, message_id, created_at)
            VALUES(?, ?, ?)
            """,
            (source_id, message_id, _sql_dt(now)),
        )
        await conn.execute(
            """
            INSERT INTO telegram_monitoring_on_demand_queue(
                source_username,
                source_id,
                chat_id,
                latest_message_id,
                latest_message_date,
                first_seen_at,
                updated_at,
                next_run_at,
                attempts,
                status,
                last_error
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, 'pending', NULL)
            ON CONFLICT(source_username) DO UPDATE SET
                source_id = excluded.source_id,
                chat_id = excluded.chat_id,
                latest_message_id = MAX(
                    COALESCE(telegram_monitoring_on_demand_queue.latest_message_id, 0),
                    excluded.latest_message_id
                ),
                latest_message_date = excluded.latest_message_date,
                updated_at = excluded.updated_at,
                next_run_at = excluded.next_run_at,
                status = 'pending',
                last_error = NULL
            """,
            (
                username,
                source_id,
                chat_id_int,
                message_id,
                _sql_dt(message_date) if message_date else None,
                _sql_dt(now),
                _sql_dt(now),
                _sql_dt(due_at),
            ),
        )
        await conn.commit()

    logger.info(
        "tg_on_demand.queued source=%s message_id=%s due_at=%s debounce_sec=%s",
        username,
        message_id,
        due_at.isoformat(),
        debounce_seconds(),
    )
    return True


async def _reset_stale_running(db: Database, *, now: datetime) -> None:
    stale_before = now - timedelta(seconds=retry_seconds())
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            UPDATE telegram_monitoring_on_demand_queue
            SET status='pending',
                next_run_at=?,
                updated_at=?,
                last_error='stale_running_reset'
            WHERE status='running'
              AND (last_run_at IS NULL OR last_run_at <= ?)
            """,
            (_sql_dt(now), _sql_dt(now), _sql_dt(stale_before)),
        )
        await conn.commit()


async def _fetch_due_rows(db: Database, *, now: datetime) -> list[tuple[Any, ...]]:
    async with db.raw_conn() as conn:
        cursor = await conn.execute(
            """
            SELECT
                q.source_username,
                q.source_id,
                q.latest_message_id,
                q.attempts,
                s.enabled
            FROM telegram_monitoring_on_demand_queue q
            JOIN telegram_source s ON s.id = q.source_id
            WHERE q.status = 'pending'
              AND q.next_run_at <= ?
            ORDER BY q.next_run_at ASC
            LIMIT ?
            """,
            (_sql_dt(now), max_runs_per_tick()),
        )
        rows = await cursor.fetchall()
        await cursor.close()
    return list(rows or [])


async def _mark_running(
    db: Database,
    *,
    username: str,
    now: datetime,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            UPDATE telegram_monitoring_on_demand_queue
            SET status='running',
                attempts=attempts + 1,
                last_run_at=?,
                updated_at=?,
                last_error=NULL
            WHERE source_username=?
            """,
            (_sql_dt(now), _sql_dt(now), username),
        )
        await conn.commit()


async def _mark_retry(
    db: Database,
    *,
    username: str,
    now: datetime,
    reason: str,
) -> None:
    next_run = now + timedelta(seconds=retry_seconds())
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            UPDATE telegram_monitoring_on_demand_queue
            SET status='pending',
                next_run_at=?,
                updated_at=?,
                last_error=?
            WHERE source_username=?
            """,
            (_sql_dt(next_run), _sql_dt(now), reason[:500], username),
        )
        await conn.commit()
    logger.info(
        "tg_on_demand.retry source=%s reason=%s next_run_at=%s",
        username,
        reason,
        next_run.isoformat(),
    )


async def _mark_terminal(
    db: Database,
    *,
    username: str,
    now: datetime,
    status: str,
    error: str | None = None,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            UPDATE telegram_monitoring_on_demand_queue
            SET status=?,
                updated_at=?,
                last_error=?
            WHERE source_username=?
            """,
            (status, _sql_dt(now), (error or "")[:500] or None, username),
        )
        await conn.commit()


async def dispatch_due_on_demand_monitoring(
    db: Database,
    bot: Any | None,
    *,
    run_id: str | None = None,
) -> int:
    """Run due source-specific on-demand Telegram Monitoring requests.

    Returns the number of due rows that were attempted. Busy resource outcomes
    are not terminal: they are requeued for the next retry interval.
    """

    if not is_on_demand_enabled():
        return 0
    if _DISPATCH_LOCK.locked():
        logger.info("tg_on_demand.skip reason=dispatcher_already_running run_id=%s", run_id)
        return 0

    async with _DISPATCH_LOCK:
        now = _utc_now()
        await _reset_stale_running(db, now=now)
        logger.info("tg_on_demand.dispatcher_tick run_id=%s", run_id)
        rows = await _fetch_due_rows(db, now=now)
        if not rows:
            return 0

        chat_id = await resolve_superadmin_chat_id(db)
        attempted = 0
        for row in rows:
            username = normalize_tg_username(str(row[0] or ""))
            latest_message_id = int(row[2] or 0)
            source_enabled = bool(row[4])
            if not username:
                continue
            if not source_enabled:
                await _mark_terminal(
                    db,
                    username=username,
                    now=_utc_now(),
                    status="skipped",
                    error="source_disabled",
                )
                continue

            run_started = _utc_now()
            await _mark_running(db, username=username, now=run_started)
            attempted += 1
            logger.info(
                "tg_on_demand.dispatch source=%s latest_message_id=%s run_id=%s",
                username,
                latest_message_id,
                run_id,
            )
            try:
                report = await run_telegram_monitor(
                    db,
                    bot=bot,
                    chat_id=chat_id,
                    trigger="on_demand",
                    operator_id=0,
                    source_usernames=[username],
                    send_progress=_env_enabled(
                        "TG_MONITORING_ON_DEMAND_SEND_PROGRESS",
                        default=False,
                    ),
                )
            except Exception as exc:
                logger.exception("tg_on_demand.dispatch_failed source=%s", username)
                await _mark_terminal(
                    db,
                    username=username,
                    now=_utc_now(),
                    status="error",
                    error=f"{type(exc).__name__}: {exc}",
                )
                continue

            errors = [str(item) for item in (getattr(report, "errors", None) or []) if str(item)]
            busy = [err for err in errors if err in BUSY_ERRORS]
            if busy:
                await _mark_retry(
                    db,
                    username=username,
                    now=_utc_now(),
                    reason=",".join(sorted(set(busy))),
                )
            elif errors:
                await _mark_terminal(
                    db,
                    username=username,
                    now=_utc_now(),
                    status="error",
                    error=",".join(errors),
                )
            else:
                await _mark_terminal(
                    db,
                    username=username,
                    now=_utc_now(),
                    status="done",
                    error=None,
                )

        return attempted
