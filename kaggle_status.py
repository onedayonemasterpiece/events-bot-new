from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db import Database

logger = logging.getLogger(__name__)

KAGGLE_RUN_FILENAME = "kaggle_run.json"
KAGGLE_STATUS_CLIENT_FILENAME = "kaggle_status_client.py"
DEFAULT_CALLBACK_PATH = "/internal/kaggle/run-event"
TERMINAL_EVENTS = {"render_done", "report_written"}


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
    safe_run = _slugify(run_id, max_len=18)
    slug = f"{username}/{safe_prefix}-{safe_run}"[:80].rstrip("-")
    title = f"Kaggle Status {safe_prefix} {safe_run}"[:100]
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
        except Exception:
            logger.info("kaggle_status: dataset exists or create failed; trying version dataset=%s", slug)
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
    return slug


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
) -> dict[str, Any] | None:
    callback = callback_url or resolve_callback_url()
    if not callback:
        logger.info("kaggle_status: callback_url unavailable; skipping run config run_id=%s", run_id)
        return None
    token = generate_callback_token()
    now = utc_now_iso()
    async with db.raw_conn() as conn:
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
        await conn.commit()
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
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT token_hash FROM kaggle_run_ledger WHERE run_id = ?",
            (run_id,),
        )
        row = await cur.fetchone()
        await cur.close()
    return bool(row and row[0] and secrets.compare_digest(str(row[0]), hash_token(token)))


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
    await conn.execute(
        "UPDATE kaggle_resource_lease SET status='expired', released_at=? WHERE resource_key=? AND status='active' AND expires_at < ?",
        (now, key, now),
    )
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
    async with db.raw_conn() as conn:
        if isinstance(payload.get("resource"), dict):
            resource_result = await _record_resource(conn, run_id=run_id, resource=payload["resource"])
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
    body.update(resource_result)
    return 200, body


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
