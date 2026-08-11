from __future__ import annotations

import asyncio
import hashlib
import json
from collections import deque
from dataclasses import dataclass
from typing import Optional, Any, Awaitable, Callable

import logging
import math
import os
import random
import sqlite3
import time as _time

from db import Database
from runtime import require_main_attr
from vk_intake import OCR_PENDING_SENTINEL, extract_event_ts_hint


LOCK_TIMEOUT_SECONDS = 10 * 60
"""Maximum time a row may remain locked before being returned to the queue."""

try:  # pragma: no cover - optional dependency for typing only
    from aiosqlite import Error as AioSqliteError
except ImportError:  # pragma: no cover - optional dependency for typing only
    AIOSQLITE_ERRORS: tuple[type[Exception], ...] = ()
else:
    AIOSQLITE_ERRORS = (AioSqliteError,)

_LOCK_RETRY_ATTEMPTS = 5
_LOCK_RETRY_BASE_DELAY = 0.1
_LOCK_ERROR_CLASSES = (sqlite3.OperationalError,) + AIOSQLITE_ERRORS


async def _retry_locked_write(
    conn,
    operation: Callable[[], Awaitable[Any]],
    *,
    attempts: int = _LOCK_RETRY_ATTEMPTS,
    base_delay: float = _LOCK_RETRY_BASE_DELAY,
    description: str = "operation",
) -> Any:
    """Retry ``operation`` when SQLite reports a locked database."""

    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return await operation()
        except _LOCK_ERROR_CLASSES as exc:
            message = str(exc).lower()
            if "database is locked" not in message:
                raise
            last_exc = exc
            if attempt == attempts - 1:
                break
            logging.warning(
                "vk_review locked_retry %s attempt=%s/%s", description, attempt + 1, attempts
            )
            if hasattr(conn, "rollback"):
                try:
                    await conn.rollback()
                except Exception:  # pragma: no cover - best effort cleanup
                    logging.debug(
                        "vk_review locked_retry rollback_failed %s", description, exc_info=True
                    )
            delay = base_delay * (2**attempt)
            await asyncio.sleep(delay)
    assert last_exc is not None  # for type checkers
    raise last_exc


async def _run_locked_write(
    conn,
    operation: Callable[[], Awaitable[Any]],
    *,
    description: str,
) -> Any:
    """Run a write transaction with commit retry on transient SQLite locks."""

    async def _wrapped() -> Any:
        result = await operation()
        await conn.commit()
        return result

    return await _retry_locked_write(conn, _wrapped, description=description)


async def _unlock_stale(conn) -> int:
    """Return stale locks back to the queue.

    Rows older than :data:`LOCK_TIMEOUT_SECONDS` are switched back to ``pending``
    state with ``review_batch`` cleared so they can be picked again by any
    operator. Legacy ``importing`` rows are treated the same way: the current
    auto-import flow only uses ``locked`` while processing, so an old
    ``importing`` status would otherwise remain invisible to the queue forever.
    Returns number of rows that were unlocked.
    """

    cursor = await conn.execute(
        """
        UPDATE vk_inbox
        SET status='pending', locked_by=NULL, locked_at=NULL, review_batch=NULL
        WHERE status IN ('locked', 'importing')
          AND (locked_at IS NULL OR locked_at < datetime('now', ?))
        """,
        (f"-{LOCK_TIMEOUT_SECONDS} seconds",),
    )
    await conn.execute(
        """
        UPDATE vk_source_packet
        SET status='pending', lease_owner=NULL, lease_expires_at=NULL,
            next_attempt_at=CURRENT_TIMESTAMP,
            last_typed_reason='ORPHANED_LEASE', updated_at=CURRENT_TIMESTAMP
        WHERE status='processing'
          AND (lease_expires_at IS NULL OR lease_expires_at<CURRENT_TIMESTAMP)
        """
    )
    return cursor.rowcount


async def release_stale_locks(db: Database) -> int:
    """Public helper to unlock stale rows outside of review flow."""

    async with db.raw_conn() as conn:
        count = await _run_locked_write(
            conn,
            lambda: _unlock_stale(conn),
            description="release_stale_locks",
        )
    if count:
        logging.info("vk_review release_stale_locks count=%s", count)
    return count


async def release_due_deferred(db: Database, *, batch_id: str | None = None) -> int:
    """Move due deferred rows back to ``pending`` for a new batch.

    Rate-limited rows are persisted as ``status='deferred'`` with ``locked_at``
    storing the earliest retry time. They must not be resumed inside the same
    batch that deferred them, otherwise a long unbounded run can re-pick the
    same post in a tight loop once the retry window expires.
    """

    async with db.raw_conn() as conn:
        async def _update() -> int:
            if batch_id:
                cursor = await conn.execute(
                    """
                    UPDATE vk_inbox
                    SET status='pending', locked_by=NULL, locked_at=NULL,
                        review_batch=NULL, next_attempt_at=NULL
                    WHERE status='deferred'
                      AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
                      AND (review_batch IS NULL OR review_batch <> ?)
                    """,
                    (batch_id,),
                )
            else:
                cursor = await conn.execute(
                    """
                    UPDATE vk_inbox
                    SET status='pending', locked_by=NULL, locked_at=NULL,
                        review_batch=NULL, next_attempt_at=NULL
                    WHERE status='deferred'
                      AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
                    """
                )
            return int(cursor.rowcount or 0)

        count = await _run_locked_write(
            conn,
            _update,
            description=f"release_due_deferred batch={batch_id or ''}",
        )
    if count:
        logging.info("vk_review release_due_deferred count=%s batch=%s", count, batch_id or "")
    return int(count or 0)


@dataclass(frozen=True)
class LockRecoveryCounts:
    unlocked: int = 0


async def release_all_locks(db: Database) -> LockRecoveryCounts:
    """Unlock *all* locked inbox rows.

    Intended for app startup recovery after unexpected restarts/OOM, when the
    previous in-flight review/auto-import task is gone and any locks would be
    orphaned.
    """

    async with db.raw_conn() as conn:
        async def _unlock_all() -> LockRecoveryCounts:
            cursor = await conn.execute(
                """
                UPDATE vk_inbox
                SET status='pending',
                    locked_by=NULL,
                    locked_at=NULL,
                    review_batch=NULL,
                    attempts=COALESCE(attempts, 0) + 1
                WHERE status='locked'
                  AND review_batch LIKE 'auto:%'
                """
            )
            unlocked_auto = int(cursor.rowcount or 0)

            cursor = await conn.execute(
                """
                UPDATE vk_inbox
                SET status='pending', locked_by=NULL, locked_at=NULL, review_batch=NULL
                WHERE status='locked'
                  AND (review_batch IS NULL OR review_batch NOT LIKE 'auto:%')
                """
            )
            unlocked_other = int(cursor.rowcount or 0)
            await conn.execute(
                """
                UPDATE vk_source_packet
                SET status='pending', lease_owner=NULL, lease_expires_at=NULL,
                    next_attempt_at=CURRENT_TIMESTAMP,
                    last_typed_reason='ORPHANED_LEASE', updated_at=CURRENT_TIMESTAMP
                WHERE status='processing'
                """
            )
            return LockRecoveryCounts(unlocked=unlocked_auto + unlocked_other)

        counts = await _run_locked_write(
            conn,
            _unlock_all,
            description="release_all_locks",
        )
    if counts.unlocked:
        logging.info("vk_review release_all_locks unlocked=%s", counts.unlocked)
    return counts


async def refresh_vk_event_ts_hints(db: Database) -> int:
    """Recompute :mod:`vk_inbox` timestamp hints for queued rows."""

    updates: list[tuple[int | None, int]] = []
    get_tz_offset = require_main_attr("get_tz_offset")
    await get_tz_offset(db)
    async with db.raw_conn() as conn:
        original_row_factory = conn.row_factory
        conn.row_factory = __import__("sqlite3").Row
        try:
            cursor = await conn.execute(
                """
                SELECT id, text, date, event_ts_hint
                FROM vk_inbox
                WHERE status IN ('pending', 'locked', 'skipped', 'failed', 'deferred')
                """
            )
            rows = await cursor.fetchall()
            await cursor.close()

            for row in rows:
                inbox_id = row["id"]
                text = row["text"] or ""
                publish_ts = row["date"]
                try:
                    hint = extract_event_ts_hint(
                        text, publish_ts=publish_ts, allow_past=True
                    )
                except Exception:  # pragma: no cover - defensive
                    logging.exception(
                        "vk_review refresh_hint_failed id=%s", inbox_id
                    )
                    hint = None
                if hint != row["event_ts_hint"]:
                    updates.append((hint, inbox_id))

            for hint, inbox_id in updates:
                await conn.execute(
                    "UPDATE vk_inbox SET event_ts_hint=? WHERE id=?",
                    (hint, inbox_id),
                )

            if updates:
                await conn.commit()
        finally:
            conn.row_factory = original_row_factory

    if updates:
        logging.info(
            "vk_review refresh_vk_event_ts_hints updated=%s", len(updates)
        )
    return len(updates)


_FAR_BUCKET_HISTORY: dict[int, deque[str]] = {}


@dataclass
class InboxPost:
    id: int
    group_id: int
    post_id: int
    date: int
    text: str
    matched_kw: Optional[str]
    has_date: int
    status: str
    review_batch: Optional[str]
    imported_event_id: Optional[int]
    event_ts_hint: Optional[int]
    # ``owner_type`` ('group' or 'user') distinguishes community posts
    # (owner_id = -group_id) from personal-page posts (owner_id = +user_id).
    # Default 'group' preserves the legacy contract for pre-migration rows.
    owner_type: str = "group"
    source_packet_id: Optional[int] = None


def _hours_from_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logging.warning("vk_review invalid env %s=%s, using default %s", name, value, default)
        return default


def _float_from_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logging.warning("vk_review invalid env %s=%s, using default %s", name, value, default)
        return default


def _int_from_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logging.warning("vk_review invalid env %s=%s, using default %s", name, value, default)
        return default


def _get_far_history(operator_id: int, limit: int) -> Optional[deque[str]]:
    if limit <= 0:
        _FAR_BUCKET_HISTORY.pop(operator_id, None)
        return None
    history = _FAR_BUCKET_HISTORY.get(operator_id)
    if history is None or history.maxlen != limit:
        history = deque(maxlen=limit)
        _FAR_BUCKET_HISTORY[operator_id] = history
    return history


async def pick_next(
    db: Database,
    operator_id: int,
    batch_id: str,
    *,
    requeue_skipped: bool = True,
    prefer_oldest: bool = False,
    strict_chronological: bool = False,
    resume_locked: bool = True,
) -> Optional[InboxPost]:
    """Atomically claim the oldest due carrier without semantic eligibility gates.

    ``event_ts_hint`` is a priority hint only. Publication age is the primary
    order so unknown-date and far-hint carriers cannot starve. Legacy selection
    switches remain accepted for caller compatibility but never exclude a row.
    """

    del strict_chronological
    date_order = "ASC" if prefer_oldest else "ASC"
    columns = (
        "id, group_id, post_id, date, text, matched_kw, has_date, status, "
        "review_batch, imported_event_id, event_ts_hint, "
        "COALESCE(owner_type, 'group'), source_packet_id"
    )
    async with db.raw_conn() as conn:
        await _unlock_stale(conn)
        if resume_locked:
            cur = await conn.execute(
                f"""
                SELECT {columns}
                FROM vk_inbox
                WHERE status='locked' AND locked_by=?
                ORDER BY locked_at ASC, id ASC
                LIMIT 1
                """,
                (operator_id,),
            )
            row = await cur.fetchone()
            if row:
                await conn.execute(
                    "UPDATE vk_inbox SET review_batch=?, locked_at=CURRENT_TIMESTAMP WHERE id=?",
                    (batch_id, int(row[0])),
                )
                packet_id = row[12]
                if packet_id is not None:
                    await conn.execute(
                        """
                        UPDATE vk_source_packet
                        SET status='processing', lease_owner=?,
                            lease_expires_at=datetime('now','+15 minutes'),
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id=?
                        """,
                        (str(operator_id), int(packet_id)),
                    )
                await conn.commit()
                values = list(row)
                values[8] = batch_id
                return InboxPost(*values)

        if requeue_skipped:
            await conn.execute(
                """
                UPDATE vk_inbox
                SET status='pending', locked_by=NULL, locked_at=NULL, review_batch=NULL
                WHERE status='skipped'
                """
            )

        cursor = await conn.execute(
            f"""
            WITH next AS (
                SELECT id FROM vk_inbox
                WHERE status='pending'
                  AND (next_attempt_at IS NULL OR next_attempt_at<=CURRENT_TIMESTAMP)
                ORDER BY date {date_order},
                         CASE WHEN event_ts_hint IS NULL THEN 1 ELSE 0 END,
                         event_ts_hint ASC,
                         id ASC
                LIMIT 1
            )
            UPDATE vk_inbox
            SET status='locked', locked_by=?, locked_at=CURRENT_TIMESTAMP, review_batch=?
            WHERE id=(SELECT id FROM next)
            RETURNING {columns}
            """,
            (operator_id, batch_id),
        )
        row = await cursor.fetchone()
        if not row:
            await conn.commit()
            return None
        packet_id = row[12]
        if packet_id is not None:
            await conn.execute(
                """
                UPDATE vk_source_packet
                SET status='processing', lease_owner=?,
                    lease_expires_at=datetime('now','+15 minutes'),
                    attempts=attempts+1, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (str(operator_id), int(packet_id)),
            )
        await conn.commit()
    post = InboxPost(*row)
    logging.info(
        "vk_review pick_next id=%s packet=%s group=%s post=%s hint=%s",
        post.id, post.source_packet_id, post.group_id, post.post_id, post.event_ts_hint,
    )
    return post

async def mark_skipped(db: Database, inbox_id: int) -> None:
    async with db.raw_conn() as conn:
        async def _update() -> None:
            await conn.execute(
                "UPDATE vk_inbox SET status='skipped', locked_by=NULL, locked_at=NULL WHERE id=?",
                (inbox_id,),
            )

        await _run_locked_write(
            conn,
            _update,
            description=f"mark_skipped inbox_id={inbox_id}",
        )


async def schedule_retry(
    db: Database,
    inbox_id: int,
    *,
    typed_reason: str,
    batch_id: str | None = None,
    retry_after_sec: float | int | None = None,
    quota_scope: str | None = None,
    provider_retry_after: int | None = None,
) -> tuple[str, int]:
    """Release the lease and persist capped backoff forever.

    Technical/provider/OCR/schema/persist/restart failures never become a
    terminal ``failed``/``rejected`` state. After quick retries the same row
    simply moves to a wider capped backoff and remains automatically due.
    """

    try:
        requested = max(0, int(math.ceil(float(retry_after_sec or 0))))
    except Exception:
        requested = 0
    reason = str(typed_reason or "TECHNICAL_ERROR").strip().upper() or "TECHNICAL_ERROR"
    async with db.raw_conn() as conn:
        async def _update() -> tuple[str, int]:
            cur = await conn.execute(
                "SELECT COALESCE(attempts,0), source_packet_id FROM vk_inbox WHERE id=?",
                (inbox_id,),
            )
            row = await cur.fetchone()
            attempts = int((row[0] if row else 0) or 0) + 1
            packet_id = row[1] if row else None
            # Fast retries widen into a bounded one-day interval; there is no
            # terminal attempt ceiling.
            backoff = min(86400, max(requested, min(3600, 15 * (2 ** min(attempts, 8)))))
            modifier = f"+{backoff} seconds"
            await conn.execute(
                """
                UPDATE vk_inbox
                SET status='deferred', locked_by=NULL, locked_at=NULL,
                    review_batch=?, attempts=?, next_attempt_at=datetime('now',?),
                    last_typed_reason=?, quota_scope=COALESCE(?,quota_scope),
                    provider_retry_after=?
                WHERE id=?
                """,
                (
                    batch_id, attempts, modifier, reason, quota_scope,
                    provider_retry_after, inbox_id,
                ),
            )
            if packet_id is not None:
                await conn.execute(
                    """
                    UPDATE vk_source_packet
                    SET status='retry_scheduled', llm_status=CASE
                            WHEN ? LIKE 'OCR_%' THEN llm_status ELSE 'retry_scheduled' END,
                        next_attempt_at=datetime('now',?), attempts=?,
                        lease_owner=NULL, lease_expires_at=NULL,
                        last_typed_reason=?, quota_scope=COALESCE(?,quota_scope),
                        provider_retry_after=?, terminal_carrier_outcome=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (
                        reason, modifier, attempts, reason, quota_scope,
                        provider_retry_after, int(packet_id),
                    ),
                )
            return "deferred", attempts
        return await _run_locked_write(
            conn, _update, description=f"schedule_retry inbox_id={inbox_id} reason={reason}"
        )


async def load_successful_parse_receipt(
    db: Database,
    *,
    source_packet_id: int | None,
    prompt_version: str,
    model: str,
) -> dict[str, Any] | None:
    """Return an immutable successful parse for exact packet replay."""

    if source_packet_id is None:
        return None
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT parse_result_json
            FROM vk_source_packet
            WHERE id=? AND llm_status='completed'
              AND prompt_version=? AND model=?
              AND successful_parse_key IS NOT NULL
              AND parse_result_json IS NOT NULL
            """,
            (int(source_packet_id), str(prompt_version), str(model)),
        )
        row = await cur.fetchone()
    if not row or not row[0]:
        return None
    try:
        payload = json.loads(str(row[0]))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


async def record_source_parse_attempt(
    db: Database,
    *,
    source_packet_id: int | None,
    prompt_version: str,
    model: str,
    evidence_manifest: dict[str, Any],
    parse_result: dict[str, Any] | None,
    disposition: str,
    retry_reason: str | None,
    event_child_count: int,
    lifecycle_action_count: int,
    quota_scope: str | None = None,
    request_id: str | None = None,
    response_id: str | None = None,
    finish_reason: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    thought_tokens: int | None = None,
    reserved_tokens: int | None = None,
    provider_retry_after: int | None = None,
    attempt_kind: str = "primary",
    llm_started: bool = True,
    llm_completed: bool | None = None,
    structured_response_valid: bool | None = None,
    verification_triggered: bool = False,
    verification_reason: str | None = None,
    verification_disposition: str | None = None,
) -> str | None:
    """Append a funnel attempt and store a replayable successful receipt."""

    if source_packet_id is None:
        return None
    manifest_json = json.dumps(
        evidence_manifest or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    result_json = (
        json.dumps(parse_result, ensure_ascii=False, sort_keys=True, default=str)
        if parse_result is not None else None
    )
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT source_url,source_revision_hash,payload_hash,
                   discovery_keyword_hints_json,discovered_date_hints_json
            FROM vk_source_packet WHERE id=?
            """,
            (int(source_packet_id),),
        )
        packet = await cur.fetchone()
        if not packet:
            return None
        source_url, revision_hash, payload_hash, keyword_json, date_json = packet
        parse_key = hashlib.sha256(
            "\x1f".join(
                (str(payload_hash), str(revision_hash), manifest_json, str(prompt_version), str(model))
            ).encode("utf-8")
        ).hexdigest()
        cur = await conn.execute(
            "SELECT COALESCE(MAX(attempt_no),0)+1 FROM vk_source_packet_attempt WHERE source_packet_id=?",
            (int(source_packet_id),),
        )
        attempt_row = await cur.fetchone()
        attempt_no = int((attempt_row[0] if attempt_row else 1) or 1)
        completed = bool(parse_result is not None) if llm_completed is None else bool(llm_completed)
        valid = (
            bool(parse_result is not None and not retry_reason)
            if structured_response_valid is None
            else bool(structured_response_valid)
        )
        hints_json = json.dumps(
            {"keywords": json.loads(keyword_json or "[]"), "dates": json.loads(date_json or "[]")},
            ensure_ascii=False,
            sort_keys=True,
        )
        await conn.execute(
            """
            INSERT INTO vk_source_packet_attempt(
                source_packet_id,attempt_no,attempt_kind,parse_key,payload_hash,
                source_type,source_url,source_revision_hash,discovery_hints_json,
                evidence_manifest_json,llm_started,llm_completed,
                structured_response_valid,model,quota_scope,request_id,response_id,
                finish_reason,provider_retry_after,input_tokens,output_tokens,
                thought_tokens,reserved_tokens,primary_disposition,
                verification_triggered,verification_reason,verification_disposition,
                event_child_count,lifecycle_action_count,typed_error_reason,completed_at
            ) VALUES(
                :source_packet_id,:attempt_no,:attempt_kind,:parse_key,:payload_hash,
                'vk',:source_url,:source_revision_hash,:discovery_hints_json,
                :evidence_manifest_json,:llm_started,:llm_completed,
                :structured_response_valid,:model,:quota_scope,:request_id,:response_id,
                :finish_reason,:provider_retry_after,:input_tokens,:output_tokens,
                :thought_tokens,:reserved_tokens,:primary_disposition,
                :verification_triggered,:verification_reason,:verification_disposition,
                :event_child_count,:lifecycle_action_count,:typed_error_reason,
                CURRENT_TIMESTAMP
            )
            """,
            {
                "source_packet_id": int(source_packet_id),
                "attempt_no": attempt_no,
                "attempt_kind": str(attempt_kind or "primary"),
                "parse_key": parse_key,
                "payload_hash": str(payload_hash),
                "source_url": str(source_url),
                "source_revision_hash": str(revision_hash),
                "discovery_hints_json": hints_json,
                "evidence_manifest_json": manifest_json,
                "llm_started": 1 if llm_started else 0,
                "llm_completed": 1 if completed else 0,
                "structured_response_valid": 1 if valid else 0,
                "model": str(model),
                "quota_scope": quota_scope,
                "request_id": request_id,
                "response_id": response_id,
                "finish_reason": finish_reason,
                "provider_retry_after": provider_retry_after,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "thought_tokens": thought_tokens,
                "reserved_tokens": reserved_tokens,
                "primary_disposition": str(disposition),
                "verification_triggered": 1 if verification_triggered else 0,
                "verification_reason": verification_reason,
                "verification_disposition": verification_disposition,
                "event_child_count": int(event_child_count),
                "lifecycle_action_count": int(lifecycle_action_count),
                "typed_error_reason": retry_reason,
            },
        )
        if valid:
            await conn.execute(
                """
                UPDATE vk_source_packet
                SET llm_status='completed', ocr_status=CASE
                        WHEN json_extract(?,'$.evidence_complete') THEN 'completed'
                        ELSE 'incomplete' END,
                    evidence_manifest_json=?, parse_result_json=?,
                    successful_parse_key=?, prompt_version=?, model=?,
                    quota_scope=?, last_typed_reason=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    manifest_json, manifest_json, result_json, parse_key,
                    str(prompt_version), str(model), quota_scope, str(disposition),
                    int(source_packet_id),
                ),
            )
        else:
            await conn.execute(
                """
                UPDATE vk_source_packet
                SET llm_status='retry_scheduled', evidence_manifest_json=?,
                    prompt_version=?, model=?, quota_scope=?,
                    last_typed_reason=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    manifest_json, str(prompt_version), str(model), quota_scope,
                    retry_reason or str(disposition), int(source_packet_id),
                ),
            )
        await conn.commit()
    return parse_key


async def record_exact_parse_replay(
    db: Database,
    *,
    source_packet_id: int | None,
    prompt_version: str,
    model: str,
) -> None:
    if source_packet_id is None:
        return
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT source_url,source_revision_hash,payload_hash,
                   discovery_keyword_hints_json,discovered_date_hints_json,
                   evidence_manifest_json,successful_parse_key
            FROM vk_source_packet WHERE id=?
            """,
            (int(source_packet_id),),
        )
        row = await cur.fetchone()
        if not row:
            return
        cur = await conn.execute(
            "SELECT COALESCE(MAX(attempt_no),0)+1 FROM vk_source_packet_attempt WHERE source_packet_id=?",
            (int(source_packet_id),),
        )
        attempt_row = await cur.fetchone()
        hints = json.dumps(
            {"keywords": json.loads(row[3] or "[]"), "dates": json.loads(row[4] or "[]")},
            ensure_ascii=False,
            sort_keys=True,
        )
        await conn.execute(
            """
            INSERT INTO vk_source_packet_attempt(
                source_packet_id,attempt_no,attempt_kind,parse_key,payload_hash,
                source_type,source_url,source_revision_hash,discovery_hints_json,
                evidence_manifest_json,llm_started,llm_completed,
                structured_response_valid,model,primary_disposition,
                terminal_carrier_outcome,completed_at
            ) VALUES(?,?,'exact_replay',?,?,?,?,?,?,?,0,0,1,?,'EXACT_REPLAY','EXACT_REPLAY',CURRENT_TIMESTAMP)
            """,
            (
                int(source_packet_id), int((attempt_row[0] if attempt_row else 1) or 1),
                row[6], row[2], "vk", row[0], row[1], hints, row[5], str(model),
            ),
        )
        await conn.commit()


async def record_carrier_resolution(
    db: Database,
    *,
    source_packet_id: int | None,
    child_outcomes: list[str],
    terminal_carrier_outcome: str | None,
    next_attempt_at: str | None = None,
    typed_error_reason: str | None = None,
) -> None:
    if source_packet_id is None:
        return
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            UPDATE vk_source_packet_attempt
            SET smart_update_child_outcomes_json=?,terminal_carrier_outcome=?,
                next_attempt_at=?,typed_error_reason=COALESCE(?,typed_error_reason)
            WHERE id=(
                SELECT id FROM vk_source_packet_attempt
                WHERE source_packet_id=? ORDER BY attempt_no DESC,id DESC LIMIT 1
            )
            """,
            (
                json.dumps(child_outcomes, ensure_ascii=False), terminal_carrier_outcome,
                next_attempt_at, typed_error_reason, int(source_packet_id),
            ),
        )
        await conn.commit()


async def mark_rejected(db: Database, inbox_id: int) -> None:
    """Record a validated, complete-evidence LLM no-event outcome only."""

    async with db.raw_conn() as conn:
        async def _update() -> None:
            cur = await conn.execute("SELECT source_packet_id FROM vk_inbox WHERE id=?", (inbox_id,))
            row = await cur.fetchone()
            await conn.execute(
                """
                UPDATE vk_inbox SET status='confirmed_no_event', locked_by=NULL,
                    locked_at=NULL, next_attempt_at=NULL,
                    last_typed_reason='CONFIRMED_NO_EVENT'
                WHERE id=?
                """,
                (inbox_id,),
            )
            if row and row[0] is not None:
                await conn.execute(
                    """
                    UPDATE vk_source_packet
                    SET status='confirmed_no_event', llm_status='completed',
                        terminal_carrier_outcome='CONFIRMED_NO_EVENT',
                        lease_owner=NULL, lease_expires_at=NULL,
                        last_typed_reason='CONFIRMED_NO_EVENT', updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (int(row[0]),),
                )
        await _run_locked_write(conn, _update, description=f"mark_confirmed_no_event inbox_id={inbox_id}")


async def mark_pending(db: Database, inbox_id: int) -> None:
    """Return an inbox row back to the due queue."""
    async with db.raw_conn() as conn:
        async def _update() -> None:
            await conn.execute(
                """
                UPDATE vk_inbox SET status='pending', locked_by=NULL, locked_at=NULL,
                    review_batch=NULL, next_attempt_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (inbox_id,),
            )
        await _run_locked_write(conn, _update, description=f"mark_pending inbox_id={inbox_id}")


async def mark_deferred(
    db: Database,
    inbox_id: int,
    *,
    batch_id: str | None,
    retry_after_sec: float | int | None = None,
    typed_reason: str = "RETRY_REQUIRED",
) -> None:
    await schedule_retry(
        db,
        inbox_id,
        typed_reason=typed_reason,
        batch_id=batch_id,
        retry_after_sec=retry_after_sec,
    )


async def mark_rate_limited(
    db: Database,
    inbox_id: int,
    *,
    batch_id: str | None,
    retry_after_sec: float | int | None = None,
    max_attempts: int,
) -> tuple[str, int]:
    # ``max_attempts`` controls alerting cadence only; it is never terminal.
    del max_attempts
    return await schedule_retry(
        db,
        inbox_id,
        typed_reason="RATE_LIMITED",
        batch_id=batch_id,
        retry_after_sec=retry_after_sec,
        provider_retry_after=(int(retry_after_sec) if retry_after_sec else None),
    )

async def mark_imported(
    db: Database,
    inbox_id: int,
    batch_id: str,
    operator_id: int,
    event_id: int | None,
    event_date: str | None,
) -> None:
    """Backward-compatible wrapper for :func:`mark_imported_events`."""
    event_ids = [int(event_id)] if event_id else []
    event_dates = [event_date] if event_date else []
    await mark_imported_events(
        db,
        inbox_id=inbox_id,
        batch_id=batch_id,
        operator_id=operator_id,
        event_ids=event_ids,
        event_dates=event_dates,
    )


async def mark_carrier_outcome(
    db: Database,
    *,
    inbox_id: int,
    outcome: str,
    keep_due: bool = False,
    typed_reason: str | None = None,
) -> None:
    normalized = str(outcome or "").strip().upper()
    status_map = {
        "EVENTS_RESOLVED": "imported",
        "LIFECYCLE_RESOLVED": "imported",
        "MIXED_RESOLVED": "imported",
        "CONFIRMED_PRODUCT_EXCLUSION": "confirmed_product_exclusion",
        "EXACT_REPLAY": "imported",
    }
    status = "deferred" if keep_due else status_map.get(normalized, "pending")
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT source_packet_id FROM vk_inbox WHERE id=?", (inbox_id,))
        row = await cur.fetchone()
        await conn.execute(
            """
            UPDATE vk_inbox
            SET status=?,locked_by=NULL,locked_at=NULL,
                next_attempt_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END,
                last_typed_reason=?
            WHERE id=?
            """,
            (status, 1 if keep_due else 0, typed_reason or normalized, inbox_id),
        )
        if row and row[0] is not None:
            await conn.execute(
                """
                UPDATE vk_source_packet
                SET status=?,terminal_carrier_outcome=?,lease_owner=NULL,
                    lease_expires_at=NULL,last_typed_reason=?,updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (status, normalized, typed_reason or normalized, int(row[0])),
            )
        await conn.commit()


async def mark_imported_events(
    db: Database,
    *,
    inbox_id: int,
    batch_id: str,
    operator_id: int,
    event_ids: list[int] | None = None,
    event_dates: list[str | None] | None = None,
) -> None:
    """Mark inbox row as imported and link it with one or more events.

    VK posts may yield multiple events. We keep ``vk_inbox.imported_event_id`` as
    a convenience pointer to the first imported event (if any) and store the full
    mapping in ``vk_inbox_import_event``.

    ``event_dates`` may contain either ``YYYY-MM-DD`` or ``YYYY-MM`` strings; we
    extract month parts and accumulate them in ``vk_review_batch.months_csv``.
    """

    ids = [int(v) for v in (event_ids or []) if v]
    primary_event_id = ids[0] if ids else None

    months: set[str] = set()
    for raw in (event_dates or []):
        month = (raw or "")[:7]
        if month:
            months.add(month)

    async with db.raw_conn() as conn:
        async def _update() -> None:
            nonlocal batch_id
            if not batch_id:
                cur = await conn.execute(
                    "SELECT review_batch FROM vk_inbox WHERE id=?",
                    (inbox_id,),
                )
                row = await cur.fetchone()
                if row and row[0]:
                    batch_id = row[0]

            await conn.execute(
                """
                UPDATE vk_inbox
                SET status='imported', locked_by=NULL, locked_at=NULL,
                    imported_event_id=?, review_batch=?
                WHERE id=?
                """,
                (primary_event_id, batch_id, inbox_id),
            )
            cur_packet = await conn.execute(
                "SELECT source_packet_id FROM vk_inbox WHERE id=?", (inbox_id,)
            )
            packet_row = await cur_packet.fetchone()
            if packet_row and packet_row[0] is not None:
                await conn.execute(
                    """
                    UPDATE vk_source_packet
                    SET status='events_resolved',
                        terminal_carrier_outcome='EVENTS_RESOLVED',
                        lease_owner=NULL,lease_expires_at=NULL,
                        last_typed_reason='EVENTS_RESOLVED',updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (int(packet_row[0]),),
                )

            if ids:
                for eid in ids:
                    await conn.execute(
                        "INSERT OR IGNORE INTO vk_inbox_import_event(inbox_id, event_id) VALUES(?,?)",
                        (inbox_id, eid),
                    )

            if batch_id:
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO vk_review_batch(batch_id, operator_id, months_csv)
                    VALUES(?,?,?)
                    """,
                    (batch_id, operator_id, ""),
                )
                cur = await conn.execute(
                    "SELECT months_csv FROM vk_review_batch WHERE batch_id=?",
                    (batch_id,),
                )
                row = await cur.fetchone()
                if row and row[0]:
                    months.update(set(filter(None, str(row[0]).split(","))))
                months_csv = ",".join(sorted(months))
                await conn.execute(
                    "UPDATE vk_review_batch SET months_csv=?, finished_at=NULL WHERE batch_id=?",
                    (months_csv, batch_id),
                )
            else:
                logging.warning(
                    "vk_review mark_imported missing_batch",
                    extra={
                        "inbox_id": inbox_id,
                        "event_ids": ids,
                        "event_dates": event_dates,
                    },
                )

        await _run_locked_write(
            conn,
            _update,
            description=f"mark_imported_events inbox_id={inbox_id}",
        )

    logging.info(
        "vk_review mark_imported_events inbox_id=%s primary_event_id=%s events=%s months=%s",
        inbox_id,
        primary_event_id,
        len(ids),
        ",".join(sorted(months)),
    )


async def save_repost_url(db: Database, event_id: int, url: str) -> None:
    """Persist ``vk_repost_url`` for the event."""

    async with db.raw_conn() as conn:
        async def _update() -> None:
            await conn.execute(
                "UPDATE event SET vk_repost_url=? WHERE id=?",
                (url, event_id),
            )
            await conn.commit()

        await _retry_locked_write(
            conn,
            _update,
            description=f"save_repost_url event_id={event_id}",
        )


async def finish_batch(
    db: Database,
    batch_id: str,
    rebuild_cb: Callable[[Database, str], Awaitable[Any]],
) -> list[str]:
    """Finish review batch and rebuild affected months sequentially.

    ``rebuild_cb`` is awaited for every month individually to guarantee
    sequential rebuilds.  The function clears ``months_csv`` and sets
    ``finished_at`` timestamp.  Returns the list of months that were rebuilt.
    """

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT months_csv FROM vk_review_batch WHERE batch_id=?", (batch_id,)
        )
        row = await cur.fetchone()
        months = [m for m in (row[0].split(',') if row and row[0] else []) if m]
    for month in months:
        start = _time.perf_counter() if "_time" in globals() else None
        await rebuild_cb(db, month)
        if start is not None:
            took = int((_time.perf_counter() - start) * 1000)
            logging.info("vk_review rebuild month=%s took_ms=%d", month, took)
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE vk_review_batch SET months_csv='', finished_at=CURRENT_TIMESTAMP WHERE batch_id=?",
            (batch_id,),
        )
        await conn.commit()
    return months
