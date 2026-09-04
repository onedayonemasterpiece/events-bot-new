from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import shutil
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from db import Database

logger = logging.getLogger(__name__)

KAGGLE_RUN_FILENAME = "kaggle_run.json"
KAGGLE_STATUS_CLIENT_FILENAME = "kaggle_status_client.py"
DEFAULT_CALLBACK_PATH = "/internal/kaggle/run-event"
TERMINAL_EVENTS = {"render_done", "report_written"}
TERMINAL_STATUSES = {"complete", "done", "failed", "error", "cancelled", "canceled"}
PHASE_PROGRESS_PERCENT = {
    "bootstrap": 0,
    "created": 0,
    "prepare": 5,
    "preflight": 5,
    "pushed": 10,
    "kernel_shape_wait": 15,
    "poll": 20,
    "run": 50,
    "parse": 55,
    "download": 45,
    "distill": 65,
    "reason": 80,
    "render": 60,
    "publish": 85,
    "fresh_output_wait": 95,
    "report": 95,
    "write_report": 95,
    "cleanup": 98,
}


def _read_positive_int_env(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError
        return value
    except ValueError:
        logger.warning("kaggle_status: invalid %s=%r; using default %s", key, raw, default)
        return default


KAGGLE_RESOURCE_LEASE_RENEW_TTL_SECONDS = _read_positive_int_env(
    "KAGGLE_RESOURCE_LEASE_RENEW_TTL_SECONDS",
    3 * 60 * 60,
)
KAGGLE_STATUS_ALIVE_EVENT_MIN_INTERVAL_SECONDS = _read_positive_int_env(
    "KAGGLE_STATUS_ALIVE_EVENT_MIN_INTERVAL_SECONDS",
    5 * 60,
)


def _is_sqlite_lock_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database table is locked" in message


async def _begin_immediate_with_retry(
    conn: Any,
    *,
    run_id: str,
    operation: str,
    max_attempts: int = 5,
) -> None:
    """Acquire SQLite's single writer slot with bounded lock-only retries."""

    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            await conn.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_lock_error(exc) or attempt >= attempts:
                raise
            delay = min(2 ** (attempt - 1), 8)
            logger.warning(
                "kaggle_status: SQLite writer lock operation=%s run_id=%s "
                "attempt=%s/%s retry_in=%ss",
                operation,
                _clean_text(run_id, limit=300),
                attempt,
                attempts,
                delay,
            )
            await asyncio.sleep(delay)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_token(token: str) -> str:
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def generate_callback_token() -> str:
    return secrets.token_urlsafe(32)


def resolve_callback_url() -> str | None:
    explicit = (os.getenv("KAGGLE_STATUS_CALLBACK_URL") or "").strip()
    if explicit:
        return explicit
    base = (os.getenv("WEBHOOK_URL") or "").strip()
    if not base:
        return None
    return base.rstrip("/") + DEFAULT_CALLBACK_PATH


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def _clean_text(value: Any, *, limit: int = 2000) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:limit]


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _bounded_percent(value: Any) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    return max(0, min(100, int(round(number))))


def estimate_progress_percent(
    progress: dict[str, Any] | None,
    *,
    event: str | None = None,
    status: str | None = None,
    phase: str | None = None,
) -> int | None:
    data = progress if isinstance(progress, dict) else {}
    for key in ("progress_percent", "percent", "completion_percent", "pct"):
        percent = _bounded_percent(data.get(key))
        if percent is not None:
            return percent

    status_l = str(status or "").strip().casefold()
    event_l = str(event or "").strip().casefold()
    phase_l = str(phase or data.get("phase") or "").strip().casefold()
    if event_l in TERMINAL_EVENTS and status_l in TERMINAL_STATUSES:
        return 100
    if status_l in {"complete", "done"}:
        return 100

    pairs = (
        ("cell_index", "cell_total"),
        ("url_index", "urls_total"),
        ("source_index", "sources_total"),
        ("sources_done", "sources_total"),
        ("post_index", "posts_total"),
        ("posts_done", "posts_total"),
        ("event_index", "events_total"),
        ("events_done", "events_total"),
        ("scene_index", "scenes_total"),
        ("scenes_done", "scenes_total"),
        ("frame_index", "frames_total"),
        ("frames_done", "frames_total"),
        ("month_index", "months_total"),
        ("item_index", "items_total"),
        ("items_done", "items_total"),
        ("processed", "total"),
    )
    for done_key, total_key in pairs:
        done = _as_float(data.get(done_key))
        total = _as_float(data.get(total_key))
        if done is None or total is None or total <= 0:
            continue
        percent = _bounded_percent((done / total) * 100.0)
        if percent is None:
            continue
        if status_l in {"running", "alive", "queued"} and percent >= 100:
            return 95
        return percent

    if phase_l in PHASE_PROGRESS_PERCENT:
        return PHASE_PROGRESS_PERCENT[phase_l]
    return None


def with_progress_percent(
    progress: dict[str, Any] | None,
    *,
    event: str | None = None,
    status: str | None = None,
    phase: str | None = None,
) -> dict[str, Any]:
    data = dict(progress or {})
    percent = estimate_progress_percent(data, event=event, status=status, phase=phase)
    if percent is not None:
        data["progress_percent"] = percent
    return data


def format_kaggle_status_label(status: dict | None) -> str:
    if not status:
        return "неизвестен"
    state = status.get("status")
    if not state:
        return "неизвестен"
    failure_msg = (
        status.get("failureMessage")
        or status.get("failure_message")
        or status.get("errorMessage")
        or status.get("error_message")
        or status.get("error")
    )
    progress = status.get("progress") if isinstance(status.get("progress"), dict) else {}
    percent = _bounded_percent(status.get("progress_percent"))
    if percent is None:
        percent = estimate_progress_percent(
            progress,
            status=str(state),
            phase=str(status.get("phase") or status.get("_ledger_phase") or ""),
        )
    result = str(state)
    if percent is not None:
        result += f" {percent}%"
    progress_label = _clean_text(progress.get("progress_label") if isinstance(progress, dict) else None, limit=120)
    if not progress_label and isinstance(progress, dict):
        for label, done_key, total_key in (
            ("ячейки", "cell_index", "cell_total"),
            ("url", "url_index", "urls_total"),
            ("источники", "source_index", "sources_total"),
            ("источники", "sources_done", "sources_total"),
            ("посты", "post_index", "posts_total"),
            ("посты", "posts_done", "posts_total"),
            ("события", "event_index", "events_total"),
            ("события", "events_done", "events_total"),
            ("сцены", "scene_index", "scenes_total"),
            ("сцены", "scenes_done", "scenes_total"),
            ("кадры", "frame_index", "frames_total"),
            ("кадры", "frames_done", "frames_total"),
            ("месяцы", "month_index", "months_total"),
            ("операции", "item_index", "items_total"),
            ("операции", "items_done", "items_total"),
        ):
            done = progress.get(done_key)
            total = progress.get(total_key)
            if done is not None and total:
                progress_label = f"{label} {done}/{total}"
                break
    if progress_label:
        result += f" · {progress_label}"
    if failure_msg:
        result += f" ({failure_msg})"
    return result


async def enrich_kaggle_status_from_ledger(
    db: Database | None,
    run_id: str | None,
    status: dict | None,
) -> dict | None:
    if db is None or not run_id:
        return status
    try:
        async with db.raw_conn() as conn:
            cur = await conn.execute(
                """
                SELECT status, phase, progress_json, updated_at, last_heartbeat_at, terminal_at, error
                FROM kaggle_run_ledger
                WHERE run_id=?
                """,
                (run_id,),
            )
            row = await cur.fetchone()
            await cur.close()
    except Exception:
        logger.exception("kaggle_status: failed to read ledger progress run_id=%s", run_id)
        return status
    if not row:
        return status
    payload = dict(status or {})
    progress: dict[str, Any] = {}
    try:
        parsed = json.loads(row[2] or "{}")
        if isinstance(parsed, dict):
            progress = parsed
    except Exception:
        progress = {}
    ledger_status = str(row[0] or "")
    ledger_phase = str(row[1] or "")
    progress = with_progress_percent(
        progress,
        status=ledger_status,
        phase=ledger_phase,
    )
    payload["progress"] = progress
    payload["progress_percent"] = progress.get("progress_percent")
    payload["_ledger_status"] = ledger_status
    payload["_ledger_phase"] = ledger_phase
    payload["_ledger_updated_at"] = row[3]
    payload["_ledger_last_heartbeat_at"] = row[4]
    payload["_ledger_terminal_at"] = row[5]
    if row[6] and not (
        payload.get("failureMessage") or payload.get("failure_message") or payload.get("error")
    ):
        payload["failureMessage"] = row[6]
    if not payload.get("status") and ledger_status:
        payload["status"] = ledger_status
    return payload


def write_kaggle_status_files(folder: str | Path, config: dict[str, Any] | None) -> None:
    if not config:
        return
    folder_path = Path(folder)
    folder_path.mkdir(parents=True, exist_ok=True)
    (folder_path / KAGGLE_RUN_FILENAME).write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    client_src = Path(__file__).resolve().parent / "kaggle" / KAGGLE_STATUS_CLIENT_FILENAME
    if client_src.exists():
        shutil.copy2(client_src, folder_path / KAGGLE_STATUS_CLIENT_FILENAME)


def _slugify(value: str, *, max_len: int = 60) -> str:
    raw = (value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = raw.strip("-") or secrets.token_hex(4)
    return raw[:max_len].rstrip("-") or secrets.token_hex(4)


def _status_dataset_title(safe_prefix: str, safe_run: str) -> str:
    title = f"Status {safe_prefix} {safe_run}".strip()
    if len(title) <= 50:
        return title
    suffix = f" {safe_run}" if safe_run else ""
    prefix_limit = max(1, 50 - len("Status ") - len(suffix))
    title = f"Status {safe_prefix[:prefix_limit].rstrip('-')}{suffix}".strip()
    return title[:50].rstrip("- ")


def _status_run_slug(run_id: str, *, max_len: int = 18) -> str:
    """Keep status dataset slugs readable without losing run uniqueness."""
    digest = hashlib.sha256((run_id or "").encode("utf-8")).hexdigest()[:8]
    head_len = max(1, max_len - len(digest) - 1)
    head = _slugify(run_id, max_len=head_len)
    return f"{head}-{digest}"[:max_len].rstrip("-")


def _is_dataset_exists_error(exc: Exception) -> bool:
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    if status_code == 409:
        return True
    message = str(exc).casefold()
    return any(
        marker in message
        for marker in (
            "already exists",
            "already been used",
            "already in use",
            "duplicate",
            "conflict",
        )
    )


def create_kaggle_status_dataset(
    client: Any,
    *,
    username: str,
    slug_prefix: str,
    run_id: str,
    config: dict[str, Any] | None,
) -> str | None:
    if not config:
        return None
    safe_prefix = _slugify(slug_prefix, max_len=36)
    safe_run = _status_run_slug(run_id, max_len=18)
    slug = f"{username}/{safe_prefix}-{safe_run}"[:80].rstrip("-")
    title = _status_dataset_title(safe_prefix, safe_run)
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_kaggle_status_files(tmp_path, config)
        (tmp_path / "dataset-metadata.json").write_text(
            json.dumps(
                {
                    "title": title,
                    "id": slug,
                    "licenses": [{"name": "CC0-1.0"}],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        try:
            client.create_dataset(tmp_path)
        except Exception as exc:
            if not _is_dataset_exists_error(exc):
                logger.exception("kaggle_status: status dataset create failed dataset=%s", slug)
                raise
            logger.info("kaggle_status: dataset exists; trying version dataset=%s", slug)
            try:
                client.create_dataset_version(
                    tmp_path,
                    version_notes=f"refresh {safe_run}",
                    quiet=True,
                    convert_to_csv=False,
                    dir_mode="zip",
                )
            except Exception:
                logger.exception("kaggle_status: status dataset version failed dataset=%s", slug)
                raise
    deadline = time.monotonic() + 180
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            status = str(client.dataset_status(slug))
            files = client.dataset_list_files(slug, page_size=50)
            names = {
                str(item.get("name") if isinstance(item, dict) else getattr(item, "name", item))
                for item in (files or [])
            }
            if status.casefold() == "ready" and KAGGLE_RUN_FILENAME in names and KAGGLE_STATUS_CLIENT_FILENAME in names:
                logger.info("kaggle_status: dataset ready dataset=%s files=%s", slug, sorted(names))
                break
            last_error = f"status={status} files={sorted(names)}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(5)
    else:
        raise RuntimeError(f"kaggle_status dataset not ready dataset={slug}: {last_error}")
    return slug


class KaggleResourceLeaseBusy(RuntimeError):
    """A server-side resource lease prevents a second remote launch."""


async def create_kaggle_run_config(
    db: Database,
    *,
    run_id: str,
    session_id: int | None,
    kind: str,
    notebook: str,
    kernel_ref: str | None = None,
    dataset_ref: str | None = None,
    callback_url: str | None = None,
    resource_leases: list[str] | None = None,
    replace_existing: bool = True,
) -> dict[str, Any] | None:
    callback = callback_url or resolve_callback_url()
    if not callback:
        logger.info("kaggle_status: callback_url unavailable; skipping run config run_id=%s", run_id)
        return None
    token = generate_callback_token()
    now = utc_now_iso()
    # Parser orchestration intentionally starts several Kaggle kernels in
    # parallel.  Database.raw_conn() is a shared cached connection, so concurrent
    # BEGIN IMMEDIATE calls on it can fail with "cannot start a transaction
    # within a transaction".  Give every run-config writer its own connection
    # and let SQLite serialize the short writes.
    async with _fresh_status_write_transaction(
        db,
        run_id=run_id,
        operation="create_run_config",
    ) as conn:
        await _expire_resource_leases(conn, now=now)
        if not replace_existing:
            cur = await conn.execute(
                "SELECT 1 FROM kaggle_run_ledger WHERE run_id=?",
                (run_id,),
            )
            if await cur.fetchone():
                logger.info("kaggle_status: run already claimed run_id=%s", run_id)
                return None
        await conn.execute(
            """
            INSERT INTO kaggle_run_ledger(
                run_id, session_id, kind, notebook, kernel_ref, dataset_ref,
                status, phase, token_hash, progress_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'created', 'created', ?, '{}', ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                session_id=excluded.session_id,
                kind=excluded.kind,
                notebook=excluded.notebook,
                kernel_ref=COALESCE(excluded.kernel_ref, kaggle_run_ledger.kernel_ref),
                dataset_ref=COALESCE(excluded.dataset_ref, kaggle_run_ledger.dataset_ref),
                token_hash=excluded.token_hash,
                status='created',
                phase='created',
                progress_json='{}',
                updated_at=excluded.updated_at,
                terminal_at=NULL,
                error=NULL
            """,
            (
                run_id,
                session_id,
                kind,
                notebook,
                kernel_ref,
                dataset_ref,
                hash_token(token),
                now,
                now,
            ),
        )
        # Reserve before Kaggle metadata can be pushed.  A kernel-side acquire is
        # still retained for heartbeat/release, but it is too late to prevent two
        # callers from replacing the same slot between dataset upload and start.
        for resource_key in dict.fromkeys(str(item).strip() for item in (resource_leases or []) if str(item).strip()):
            reservation = await _record_resource(
                conn,
                run_id=run_id,
                resource={"key": resource_key, "action": "acquire", "holder_kind": "kaggle_preflight"},
            )
            if reservation.get("resource_action") == "blocked":
                raise KaggleResourceLeaseBusy(
                    f"kaggle_resource_lease_busy:{resource_key}:{reservation.get('holder_run_id') or 'unknown'}"
                )
    return {
        "run_id": run_id,
        "session_id": session_id,
        "kind": kind,
        "notebook": notebook,
        "callback_url": callback,
        "token": token,
        "resource_leases": list(resource_leases or []),
    }


async def validate_run_token(db: Database, run_id: str, token: str) -> bool:
    # Run configs are written by short-lived runner processes.  The web app's
    # shared Database.raw_conn may still own a read snapshot (or a failed write
    # transaction) from an unrelated callback and therefore miss the new token.
    # Authentication must observe the current durable ledger, not that cached
    # connection state.
    conn = await aiosqlite.connect(db.path, timeout=30)
    try:
        await conn.execute("PRAGMA busy_timeout=30000")
        cur = await conn.execute(
            "SELECT token_hash FROM kaggle_run_ledger WHERE run_id = ?",
            (run_id,),
        )
        row = await cur.fetchone()
        await cur.close()
    finally:
        await conn.close()
    return bool(row and row[0] and secrets.compare_digest(str(row[0]), hash_token(token)))


@asynccontextmanager
async def _fresh_status_write_transaction(
    db: Database,
    *,
    run_id: str,
    operation: str = "record_run_event",
):
    """Isolate one status-ledger write from shared SQLite transactions."""

    conn = await aiosqlite.connect(db.path, timeout=30)
    try:
        await conn.execute("PRAGMA busy_timeout=30000")
        await _begin_immediate_with_retry(
            conn,
            run_id=run_id,
            operation=operation,
        )
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    finally:
        await conn.close()


async def _expire_resource_leases(conn, *, now: str | None = None, resource_key: str | None = None) -> None:
    now_value = now or utc_now_iso()
    if resource_key:
        await conn.execute(
            """
            UPDATE kaggle_resource_lease
            SET status='expired', released_at=?, updated_at=?
            WHERE resource_key=? AND status='active' AND expires_at < ?
            """,
            (now_value, now_value, resource_key, now_value),
        )
        return
    await conn.execute(
        """
        UPDATE kaggle_resource_lease
        SET status='expired', released_at=?, updated_at=?
        WHERE status='active' AND expires_at < ?
        """,
        (now_value, now_value, now_value),
    )


async def _record_resource(conn, *, run_id: str, resource: dict[str, Any]) -> dict[str, Any]:
    key = _clean_text(resource.get("key") or resource.get("resource_key"), limit=300)
    if not key:
        return {"resource_action": "none"}
    action = str(resource.get("action") or "acquire").strip().lower()
    ttl_seconds = int(resource.get("ttl_seconds") or 7200)
    ttl_seconds = max(60, min(ttl_seconds, 24 * 3600))
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    expires = datetime.fromtimestamp(now_dt.timestamp() + ttl_seconds, timezone.utc).isoformat()
    await _expire_resource_leases(conn, now=now, resource_key=key)
    cur = await conn.execute(
        "SELECT run_id, status, expires_at FROM kaggle_resource_lease WHERE resource_key=?",
        (key,),
    )
    row = await cur.fetchone()
    await cur.close()
    if action == "release":
        if row and row[0] == run_id:
            await conn.execute(
                "UPDATE kaggle_resource_lease SET status='released', released_at=?, updated_at=? WHERE resource_key=?",
                (now, now, key),
            )
            return {"resource_action": "released", "resource_key": key}
        return {"resource_action": "ignored_release", "resource_key": key}
    if row and row[0] != run_id and str(row[1]) == "active":
        return {
            "resource_action": "blocked",
            "resource_key": key,
            "holder_run_id": row[0],
            "expires_at": row[2],
        }
    await conn.execute(
        """
        INSERT INTO kaggle_resource_lease(resource_key, run_id, holder_kind, status, acquired_at, expires_at, updated_at)
        VALUES (?, ?, ?, 'active', ?, ?, ?)
        ON CONFLICT(resource_key) DO UPDATE SET
            run_id=excluded.run_id,
            holder_kind=excluded.holder_kind,
            status='active',
            acquired_at=COALESCE(kaggle_resource_lease.acquired_at, excluded.acquired_at),
            expires_at=excluded.expires_at,
            updated_at=excluded.updated_at,
            released_at=NULL
        """,
        (key, run_id, _clean_text(resource.get("holder_kind"), limit=100) or "kaggle", now, expires, now),
    )
    return {"resource_action": "acquired", "resource_key": key, "expires_at": expires}


def _parse_utc_iso(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _renew_run_resource_leases(
    conn,
    *,
    run_id: str,
    now: str,
    ttl_seconds: int = KAGGLE_RESOURCE_LEASE_RENEW_TTL_SECONDS,
) -> int:
    now_dt = _parse_utc_iso(now) or datetime.now(timezone.utc)
    ttl = max(60, min(int(ttl_seconds), 24 * 3600))
    expires = datetime.fromtimestamp(now_dt.timestamp() + ttl, timezone.utc).isoformat()
    cur = await conn.execute(
        """
        UPDATE kaggle_resource_lease
        SET expires_at=?, updated_at=?
        WHERE run_id=? AND status='active' AND expires_at < ?
        """,
        (expires, now, run_id, expires),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


async def _coalesced_alive_seq(
    conn,
    *,
    run_id: str,
    phase: str,
    now: str,
    min_interval_seconds: int = KAGGLE_STATUS_ALIVE_EVENT_MIN_INTERVAL_SECONDS,
) -> int | None:
    interval = max(0, int(min_interval_seconds))
    if interval <= 0:
        return None
    cur = await conn.execute(
        """
        SELECT seq, phase, created_at
        FROM kaggle_run_event
        WHERE run_id=? AND event_name='alive'
        ORDER BY seq DESC
        LIMIT 1
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    if not row:
        return None
    created = _parse_utc_iso(row[2])
    current = _parse_utc_iso(now)
    if created is None or current is None:
        return None
    if str(row[1] or "") != str(phase or ""):
        return None
    age = (current - created).total_seconds()
    if 0 <= age < interval:
        return int(row[0])
    return None


async def record_kaggle_run_event(db: Database, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    run_id = _clean_text(payload.get("run_id"), limit=300)
    token = str(payload.get("token") or "")
    if not run_id or not token:
        return 400, {"ok": False, "error": "run_id and token are required"}
    if not await validate_run_token(db, run_id, token):
        return 403, {"ok": False, "error": "invalid token"}
    event = _clean_text(payload.get("event") or payload.get("event_name"), limit=100)
    if not event:
        return 400, {"ok": False, "error": "event is required"}
    event_uid = _clean_text(payload.get("event_uid") or payload.get("event_id"), limit=200)
    phase = _clean_text(payload.get("phase"), limit=100) or event
    status = _clean_text(payload.get("status"), limit=100) or ("alive" if event == "alive" else "running")
    progress = payload.get("progress") if isinstance(payload.get("progress"), dict) else {}
    message = _clean_text(payload.get("message"), limit=2000)
    now = utc_now_iso()
    resource_result: dict[str, Any] = {}
    progress = with_progress_percent(progress, event=event, status=status, phase=phase)
    async with _fresh_status_write_transaction(db, run_id=run_id) as conn:
        await _expire_resource_leases(conn, now=now)
        if isinstance(payload.get("resource"), dict):
            resource_result = await _record_resource(conn, run_id=run_id, resource=payload["resource"])
        elif event == "alive":
            renewed = await _renew_run_resource_leases(conn, run_id=run_id, now=now)
            if renewed:
                resource_result = {
                    "resource_action": "renewed",
                    "resource_lease_count": renewed,
                }
        if event_uid:
            cur = await conn.execute(
                "SELECT seq FROM kaggle_run_event WHERE run_id=? AND event_uid=?",
                (run_id, event_uid),
            )
            row = await cur.fetchone()
            await cur.close()
            if row:
                await conn.commit()
                body = {"ok": True, "run_id": run_id, "seq": int(row[0]), "duplicate": True}
                body.update(resource_result)
                return 200, body
        coalesced = False
        coalesced_seq = None
        if event == "alive":
            coalesced_seq = await _coalesced_alive_seq(
                conn,
                run_id=run_id,
                phase=phase,
                now=now,
            )
        if coalesced_seq is not None:
            seq = coalesced_seq
            coalesced = True
        else:
            cur = await conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM kaggle_run_event WHERE run_id=?",
                (run_id,),
            )
            row = await cur.fetchone()
            await cur.close()
            seq = int(row[0] or 0) + 1
            await conn.execute(
                """
                INSERT INTO kaggle_run_event(
                    run_id, seq, event_name, phase, status, event_uid, progress_json, message, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, seq, event, phase, status, event_uid, _json_dumps(progress), message, now),
            )
        terminal_at = now if event in TERMINAL_EVENTS and status in {"done", "partial", "failed", "error"} else None
        error = message if status in {"failed", "error"} else None
        await conn.execute(
            """
            UPDATE kaggle_run_ledger
            SET phase=?, status=?, progress_json=?, updated_at=?,
                last_heartbeat_at=CASE WHEN ?='alive' THEN ? ELSE last_heartbeat_at END,
                terminal_at=COALESCE(?, terminal_at),
                error=COALESCE(?, error)
            WHERE run_id=?
            """,
            (phase, status, _json_dumps(progress), now, event, now, terminal_at, error, run_id),
        )
        await conn.commit()
    logger.info(
        "kaggle_status.event run_id=%s event=%s phase=%s status=%s progress_keys=%s resource=%s",
        run_id,
        event,
        phase,
        status,
        sorted(progress.keys()),
        resource_result.get("resource_action"),
    )
    body = {"ok": True, "run_id": run_id, "seq": seq, "duplicate": False}
    if coalesced:
        body["coalesced"] = True
    body.update(resource_result)
    return 200, body


async def _reconcile_kaggle_run_terminal_from_host_once(
    db: Database,
    *,
    run_id: str,
    message: str,
) -> dict[str, Any]:
    clean_run_id = _clean_text(run_id, limit=300)
    if not clean_run_id:
        raise ValueError("run_id is required")
    now = utc_now_iso()
    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                "SELECT status, terminal_at FROM kaggle_run_ledger WHERE run_id=?",
                (clean_run_id,),
            )
            ledger = await cur.fetchone()
            await cur.close()
            if not ledger:
                await conn.rollback()
                return {"status": "missing", "run_id": clean_run_id}
            cur = await conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM kaggle_run_event WHERE run_id=?",
                (clean_run_id,),
            )
            callback_event_count, max_seq = await cur.fetchone()
            await cur.close()
            release_cur = await conn.execute(
                """
                UPDATE kaggle_resource_lease
                SET status='released', released_at=?, updated_at=?
                WHERE run_id=? AND status='active'
                """,
                (now, now, clean_run_id),
            )
            released_resource_count = int(getattr(release_cur, "rowcount", 0) or 0)
            if ledger[1] and str(ledger[0] or "").lower() in TERMINAL_STATUSES:
                await conn.commit()
                return {
                    "status": "already_terminal",
                    "run_id": clean_run_id,
                    "callback_event_count": int(callback_event_count or 0),
                    "released_resource_count": released_resource_count,
                }
            progress = {
                "phase": "report",
                "progress_percent": 100,
                "progress_label": "результат проверен принимающим хостом",
                "host_reconciled": True,
                "callback_event_count": int(callback_event_count or 0),
            }
            event_uid = "host_result_validated"
            await conn.execute(
                """
                INSERT OR IGNORE INTO kaggle_run_event(
                    run_id, seq, event_name, phase, status, event_uid,
                    progress_json, message, created_at
                ) VALUES (?, ?, 'host_result_validated', 'report', 'done', ?, ?, ?, ?)
                """,
                (
                    clean_run_id,
                    int(max_seq or 0) + 1,
                    event_uid,
                    _json_dumps(progress),
                    _clean_text(message, limit=2000),
                    now,
                ),
            )
            await conn.execute(
                """
                UPDATE kaggle_run_ledger
                SET phase='report', status='done', progress_json=?, updated_at=?,
                    terminal_at=COALESCE(terminal_at, ?), error=NULL
                WHERE run_id=?
                """,
                (_json_dumps(progress), now, now, clean_run_id),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    logger.info(
        "kaggle_status: host reconciled terminal run_id=%s callback_event_count=%s",
        clean_run_id,
        int(callback_event_count or 0),
    )
    return {
        "status": "reconciled",
        "run_id": clean_run_id,
        "callback_event_count": int(callback_event_count or 0),
        "released_resource_count": released_resource_count,
    }


async def reconcile_kaggle_run_terminal_from_host(
    db: Database,
    *,
    run_id: str,
    message: str = "host validated downloaded Kaggle result",
) -> dict[str, Any]:
    """Close a run ledger after the host validates the immutable result.

    Kernel callbacks remain the primary status signal.  This reconciliation is
    a fail-safe for the narrow case where Kaggle completed and the host
    cryptographically validated the downloaded result, but callback delivery
    was unavailable.  It records that distinction instead of leaving a
    successful run permanently in ``created`` and releases only resource
    leases still owned by this exact run.  The ownership predicate is critical:
    a late host receipt must never release a successor run's lease.

    Publication may finish while Smart Update owns a short SQLite writer lock.
    A one-shot best-effort call can therefore strand a three-hour resource
    lease after a successful build. Retry only SQLite lock contention; all
    other failures remain visible immediately.
    """

    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            return await _reconcile_kaggle_run_terminal_from_host_once(
                db,
                run_id=run_id,
                message=message,
            )
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_lock_error(exc) or attempt >= max_attempts:
                raise
            delay = min(2 ** (attempt - 1), 8)
            logger.warning(
                "kaggle_status: host reconciliation SQLite lock run_id=%s attempt=%s/%s retry_in=%ss",
                _clean_text(run_id, limit=300),
                attempt,
                max_attempts,
                delay,
            )
            await asyncio.sleep(delay)


async def _reconcile_kaggle_run_failure_from_host_once(
    db: Database,
    *,
    run_id: str,
    message: str,
) -> dict[str, Any]:
    """Persist a host-observed terminal failure and release its exact leases."""

    clean_run_id = _clean_text(run_id, limit=300)
    if not clean_run_id:
        raise ValueError("run_id is required")
    clean_message = _clean_text(message, limit=2000) or "host observed terminal Kaggle failure"
    now = utc_now_iso()
    async with db.raw_conn() as conn:
        await _begin_immediate_with_retry(
            conn,
            run_id=clean_run_id,
            operation="reconcile_failure",
        )
        try:
            cur = await conn.execute(
                "SELECT status, terminal_at FROM kaggle_run_ledger WHERE run_id=?",
                (clean_run_id,),
            )
            ledger = await cur.fetchone()
            await cur.close()
            if not ledger:
                await conn.rollback()
                return {"status": "missing", "run_id": clean_run_id}
            cur = await conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM kaggle_run_event WHERE run_id=?",
                (clean_run_id,),
            )
            callback_event_count, max_seq = await cur.fetchone()
            await cur.close()
            release_cur = await conn.execute(
                """
                UPDATE kaggle_resource_lease
                SET status='released', released_at=?, updated_at=?
                WHERE run_id=? AND status='active'
                """,
                (now, now, clean_run_id),
            )
            released_resource_count = int(getattr(release_cur, "rowcount", 0) or 0)
            if ledger[1] and str(ledger[0] or "").lower() in TERMINAL_STATUSES:
                await conn.commit()
                return {
                    "status": "already_terminal",
                    "run_id": clean_run_id,
                    "callback_event_count": int(callback_event_count or 0),
                    "released_resource_count": released_resource_count,
                }
            progress = {
                "phase": "terminal",
                "progress_percent": 100,
                "progress_label": "ошибка подтверждена принимающим хостом",
                "host_reconciled": True,
                "callback_event_count": int(callback_event_count or 0),
            }
            event_uid = "host_failure_observed"
            await conn.execute(
                """
                INSERT OR IGNORE INTO kaggle_run_event(
                    run_id, seq, event_name, phase, status, event_uid,
                    progress_json, message, created_at
                ) VALUES (?, ?, 'host_failure_observed', 'terminal', 'failed', ?, ?, ?, ?)
                """,
                (
                    clean_run_id,
                    int(max_seq or 0) + 1,
                    event_uid,
                    _json_dumps(progress),
                    clean_message,
                    now,
                ),
            )
            await conn.execute(
                """
                UPDATE kaggle_run_ledger
                SET phase='terminal', status='failed', progress_json=?, updated_at=?,
                    terminal_at=COALESCE(terminal_at, ?), error=?
                WHERE run_id=?
                """,
                (_json_dumps(progress), now, now, clean_message, clean_run_id),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    logger.info(
        "kaggle_status: host reconciled failure run_id=%s callback_event_count=%s released_resources=%s",
        clean_run_id,
        int(callback_event_count or 0),
        released_resource_count,
    )
    return {
        "status": "failed_reconciled",
        "run_id": clean_run_id,
        "callback_event_count": int(callback_event_count or 0),
        "released_resource_count": released_resource_count,
    }


async def reconcile_kaggle_run_failure_from_host(
    db: Database,
    *,
    run_id: str,
    message: str = "host observed terminal Kaggle failure",
) -> dict[str, Any]:
    """Close a failed Kaggle ledger and release only resources owned by it."""

    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            return await _reconcile_kaggle_run_failure_from_host_once(
                db,
                run_id=run_id,
                message=message,
            )
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_lock_error(exc) or attempt >= max_attempts:
                raise
            delay = min(2 ** (attempt - 1), 8)
            logger.warning(
                "kaggle_status: host failure reconciliation SQLite lock run_id=%s "
                "attempt=%s/%s retry_in=%ss",
                _clean_text(run_id, limit=300),
                attempt,
                max_attempts,
                delay,
            )
            await asyncio.sleep(delay)


def make_kaggle_run_event_handler(db: Database):
    from aiohttp import web

    async def kaggle_run_event_handler(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        if not isinstance(payload, dict):
            return web.json_response({"ok": False, "error": "json object required"}, status=400)
        status, response = await record_kaggle_run_event(db, payload)
        return web.json_response(response, status=status)

    return kaggle_run_event_handler
