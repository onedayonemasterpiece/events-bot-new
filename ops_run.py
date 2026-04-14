from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Mapping

from db import Database

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency for typing only
    from aiosqlite import Error as AioSqliteError
except ImportError:  # pragma: no cover - optional dependency for typing only
    _LOCK_ERROR_CLASSES: tuple[type[Exception], ...] = (sqlite3.OperationalError,)
else:
    _LOCK_ERROR_CLASSES = (sqlite3.OperationalError, AioSqliteError)

_LOCK_RETRY_ATTEMPTS = 5
_LOCK_RETRY_BASE_DELAY_SEC = 0.1


def _utc_sql(dt: datetime | None = None) -> str:
    value = dt or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _json_dumps(payload: Mapping[str, Any] | None) -> str:
    if not payload:
        return "{}"
    try:
        return json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        logger.warning("ops_run: failed to serialize payload", exc_info=True)
        return "{}"


async def _retry_locked_write(
    conn: Any,
    operation,
    *,
    description: str,
):
    last_exc: Exception | None = None
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        try:
            return await operation()
        except _LOCK_ERROR_CLASSES as exc:
            message = str(exc).lower()
            if "database is locked" not in message:
                raise
            last_exc = exc
            if attempt == _LOCK_RETRY_ATTEMPTS - 1:
                break
            logger.warning(
                "ops_run: locked_retry %s attempt=%s/%s",
                description,
                attempt + 1,
                _LOCK_RETRY_ATTEMPTS,
            )
            rollback = getattr(conn, "rollback", None)
            if callable(rollback):
                try:
                    await rollback()
                except Exception:  # pragma: no cover - best effort cleanup
                    logger.debug(
                        "ops_run: locked_retry rollback_failed %s",
                        description,
                        exc_info=True,
                    )
            await asyncio.sleep(_LOCK_RETRY_BASE_DELAY_SEC * (2**attempt))
    assert last_exc is not None  # pragma: no cover - for type checkers
    raise last_exc


async def start_ops_run(
    db: Database,
    *,
    kind: str,
    trigger: str,
    chat_id: int | None = None,
    operator_id: int | None = None,
    started_at: datetime | None = None,
    metrics: Mapping[str, Any] | None = None,
    details: Mapping[str, Any] | None = None,
) -> int | None:
    try:
        async with db.raw_conn() as conn:
            async def _insert() -> int | None:
                cursor = await conn.execute(
                    """
                    INSERT INTO ops_run(
                        kind,
                        trigger,
                        chat_id,
                        operator_id,
                        started_at,
                        status,
                        metrics_json,
                        details_json
                    )
                    VALUES(?, ?, ?, ?, ?, 'running', ?, ?)
                    """,
                    (
                        str(kind or "").strip(),
                        str(trigger or "").strip() or "manual",
                        int(chat_id) if chat_id is not None else None,
                        int(operator_id) if operator_id is not None else None,
                        _utc_sql(started_at),
                        _json_dumps(metrics),
                        _json_dumps(details),
                    ),
                )
                await conn.commit()
                return int(cursor.lastrowid or 0) or None

            return await _retry_locked_write(
                conn,
                _insert,
                description=f"start kind={str(kind or '').strip() or 'unknown'} trigger={str(trigger or '').strip() or 'manual'}",
            )
    except Exception:
        logger.warning("ops_run: failed to start run kind=%s trigger=%s", kind, trigger, exc_info=True)
        return None


async def finish_ops_run(
    db: Database,
    *,
    run_id: int | None,
    status: str,
    finished_at: datetime | None = None,
    metrics: Mapping[str, Any] | None = None,
    details: Mapping[str, Any] | None = None,
) -> None:
    if not run_id:
        return
    try:
        async with db.raw_conn() as conn:
            async def _update() -> None:
                await conn.execute(
                    """
                    UPDATE ops_run
                    SET
                        finished_at = ?,
                        status = ?,
                        metrics_json = ?,
                        details_json = ?
                    WHERE id = ?
                    """,
                    (
                        _utc_sql(finished_at),
                        str(status or "").strip() or "success",
                        _json_dumps(metrics),
                        _json_dumps(details),
                        int(run_id),
                    ),
                )
                await conn.commit()

            await _retry_locked_write(
                conn,
                _update,
                description=f"finish id={int(run_id)} status={str(status or '').strip() or 'success'}",
            )
    except Exception:
        logger.warning("ops_run: failed to finish run id=%s status=%s", run_id, status, exc_info=True)


async def cleanup_running_ops_runs_on_startup(
    db: Database,
    *,
    status: str = "crashed",
    finished_at: datetime | None = None,
) -> int:
    """Mark unfinished runs as crashed after an unexpected restart.

    Fly restarts/OOM kills drop in-memory tasks. Any rows left in
    ``ops_run.status='running'`` become orphaned and confuse operational
    dashboards. This helper is intended to be called once on app startup
    *before* schedulers start new runs.
    """

    try:
        async with db.raw_conn() as conn:
            async def _update() -> int:
                cursor = await conn.execute(
                    """
                    UPDATE ops_run
                    SET
                        finished_at = ?,
                        status = ?
                    WHERE status = 'running'
                      AND finished_at IS NULL
                    """,
                    (
                        _utc_sql(finished_at),
                        str(status or "").strip() or "crashed",
                    ),
                )
                await conn.commit()
                return int(cursor.rowcount or 0)

            count = await _retry_locked_write(
                conn,
                _update,
                description=f"startup_cleanup status={str(status or '').strip() or 'crashed'}",
            )
    except Exception:
        logger.warning("ops_run: startup cleanup failed", exc_info=True)
        return 0

    if count:
        logger.info("ops_run: startup cleanup marked=%s status=%s", count, status)
    return count
