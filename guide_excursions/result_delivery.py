"""Run-bound result delivery into the existing guide importer and callback ledger."""
from __future__ import annotations

import asyncio
import errno
import fcntl
import functools
import json
import logging
import os
import re
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

from aiohttp import web

from kaggle_status import _fresh_status_write_transaction, utc_now_iso, validate_run_token
from . import kaggle_service

logger = logging.getLogger(__name__)
_PROCESS_LOCK_OWNER = None


def serialized_guide_process(function):
    """Share one OS lock between web recovery and standalone normal launchers."""
    @functools.wraps(function)
    async def wrapped(db, *args, **kwargs):
        global _PROCESS_LOCK_OWNER
        task = asyncio.current_task()
        if _PROCESS_LOCK_OWNER is task:
            return await function(db, *args, **kwargs)
        with open(str(Path(db.path).resolve()) + ".guide-import.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _PROCESS_LOCK_OWNER = task
            try:
                return await function(db, *args, **kwargs)
            finally:
                _PROCESS_LOCK_OWNER = None
    return wrapped


def acknowledge_import(run_id: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        (kaggle_service.RESULTS_STORE_ROOT / f"guide-excursions-push-{run_id}" / ".delivery_pending").unlink(missing_ok=True)


def _unpack(archive: Path, staging: Path, target: Path, run_id: str) -> tuple[Path, str]:
    with zipfile.ZipFile(archive) as bundle:
        limit = kaggle_service.RESULTS_STORE_MAX_MB * 1024 * 1024
        if limit and sum(item.file_size for item in bundle.infolist()) > limit:
            raise ValueError("Result exceeds the existing results-store budget")
        for item in bundle.infolist():
            path = PurePosixPath(item.filename)
            if (path.is_absolute() or ".." in path.parts or "\\" in item.filename
                    or (item.filename != "guide_excursions_results.json"
                        and path.parts[:1] != ("guide_media",))):
                raise ValueError("Unexpected result archive member")
        bundle.extractall(staging)
    result = staging / "guide_excursions_results.json"
    payload = json.loads(result.read_text())
    if not isinstance(payload, dict) or payload.get("run_id") != run_id or not isinstance(payload.get("sources"), list):
        raise ValueError("Result run_id or sources mismatch")
    # A receipt must outlive an application/host restart, not just its waiter.
    for path in [*staging.rglob("*"), staging]:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    try:
        staging.rename(target)
    except OSError as exc:
        if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
            raise
        # Re-delivery of the same immutable run package.
    parent_fd = os.open(target.parent, os.O_DIRECTORY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    return target / result.name, payload.get("scan_mode") or "full"


async def receive_guide_results(request, db):
    full_run_id = request.headers.get("X-Kaggle-Run-ID", "")
    token = request.headers.get("Authorization", "").removeprefix("Bearer ")
    if not re.fullmatch(r"guide_monitor:[A-Za-z0-9_-]+", full_run_id):
        raise web.HTTPForbidden(text="Invalid guide run identity")
    if not await validate_run_token(db, full_run_id, token):
        raise web.HTTPForbidden(text="Invalid run token")
    run_id = full_run_id.split(":", 1)[1]
    root = kaggle_service.RESULTS_STORE_ROOT
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"guide-excursions-push-{run_id}"
    with tempfile.TemporaryDirectory(prefix=".guide-push-", dir=root) as work:
        work = Path(work)
        archive = work / "result.zip"
        total = 0
        limit = kaggle_service.RESULTS_STORE_MAX_MB * 1024 * 1024
        with archive.open("wb") as output:
            async for chunk in request.content.iter_chunked(64 * 1024):
                total += len(chunk)
                if limit and total > limit:
                    raise web.HTTPRequestEntityTooLarge(max_size=limit, actual_size=total)
                output.write(chunk)
        staging = work / "files"
        staging.mkdir()
        (staging / ".delivery_pending").touch()
        try:
            result, mode = await asyncio.to_thread(_unpack, archive, staging, target, run_id)
        except (ValueError, KeyError, OSError, zipfile.BadZipFile) as exc:
            logger.warning("guide_result.rejected run_id=%s error=%s", run_id, exc)
            raise web.HTTPBadRequest(text=str(exc)) from exc
    receipt = {"results_path": str(result), "mode": mode}
    # Keep this event even when a previous host decision marked the run failed.
    # It also survives removal/replacement of the kernel-keyed poller registry.
    async with _fresh_status_write_transaction(db, run_id=full_run_id) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO kaggle_run_event "
            "(run_id,seq,event_name,phase,status,event_uid,progress_json,created_at) "
            "SELECT ?,COALESCE(MAX(seq),0)+1,'guide_result_saved','delivery','done',"
            "'guide_result_saved',?,? FROM kaggle_run_event WHERE run_id=?",
            (full_run_id, json.dumps(receipt), utc_now_iso(), full_run_id),
        )
        cur = await conn.execute(
            "SELECT 1 FROM ops_run WHERE kind='guide_monitoring' "
            "AND status IN ('success','partial','error') "
            "AND json_extract(details_json,'$.run_id')=? "
            "AND json_type(details_json,'$.import_summary')='object' LIMIT 1", (run_id,),
        )
        accepted = await cur.fetchone()
    if accepted:
        acknowledge_import(run_id)
    from scheduling import request_guide_result_import
    request_guide_result_import()
    logger.info("guide_result.saved run_id=%s bytes=%d path=%s", run_id, total, result)
    return web.json_response({"ok": True, "saved": True, "run_id": full_run_id})


async def pending_result_jobs(db):
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT e.run_id,l.kernel_ref,e.progress_json FROM kaggle_run_event e "
            "JOIN kaggle_run_ledger l ON l.run_id=e.run_id "
            "WHERE e.event_name='guide_result_saved' AND NOT EXISTS ("
            "SELECT 1 FROM ops_run o WHERE o.kind='guide_monitoring' "
            "AND o.status IN ('success','partial','error') "
            "AND e.run_id='guide_monitor:' || json_extract(o.details_json,'$.run_id') "
            "AND json_type(o.details_json,'$.import_summary')='object')"
        )
        rows = await cur.fetchall()
    return [{"kernel_ref": row[1] or row[0], "meta": {
        **json.loads(row[2]), "run_id": row[0].split(":", 1)[1], "push_delivery": True,
    }} for row in rows]
