"""Durable projection of canonical current events into the vector sidecar.

Semantic event fields are produced by the LLM-first Smart Update pipeline.  This
module is intentionally only a transport/reconciliation lane: export canonical
facts, build the two vector documents, and idempotently project their hashes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlmodel import Session, create_engine, select

from models import Event
from ops_run import finish_ops_run, start_ops_run
from static_site_release import event_public_revision

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent
LOCAL_TZ = ZoneInfo("Europe/Kaliningrad")
_SYNC_LOCK = asyncio.Lock()
DEFAULT_RECEIPT_PATH = Path("/data/event_vector_sync_receipt.json")
RECEIPT_SCHEMA_VERSION = "event_vector_sync_receipt_v2"


class EventVectorSyncDeferred(RuntimeError):
    """Keep an outbox projection pending while an exact static release is open."""

    def __init__(self, reason: str, retry_at: datetime) -> None:
        super().__init__(f"event_vector_sync_release_guard:{reason}")
        self.reason = reason
        self.retry_at = retry_at


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int((os.getenv(name) or str(default)).strip() or default))
    except ValueError:
        return max(minimum, default)


def enabled() -> bool:
    return (os.getenv("ENABLE_EVENT_VECTOR_SYNC") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def static_release_guard(db_path: str, *, now: datetime | None = None) -> dict[str, Any] | None:
    """Hold vector projection stable during a build and its exact-test window.

    The Search API exposes one current projection.  Updating it while an
    immutable candidate is being built (or immediately after publication)
    makes an otherwise valid release impossible to accept exactly.  This
    read-only guard coordinates the two existing durable lanes; it never drops
    the pending vector request.
    """

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    grace_seconds = min(
        1800,
        max(120, _env_int("EVENT_VECTOR_SYNC_POST_STATIC_GRACE_SECONDS", 900)),
    )
    try:
        with sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True, timeout=30) as con:
            exists = con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='static_site_build_state'"
            ).fetchone()
            if not exists:
                return None
            row = con.execute(
                "SELECT active_job_id, last_success_at FROM static_site_build_state "
                "WHERE release_channel='secret_preview'"
            ).fetchone()
    except sqlite3.Error:
        # Fail open for deployments that do not have the static-site release
        # schema. The vector sync's own complete-receipt gates still apply.
        return None
    if not row:
        return None
    if row[0] is not None:
        return {
            "reason": "static_build_active",
            "retry_at": current + timedelta(minutes=5),
        }
    raw_success = str(row[1] or "").strip()
    if not raw_success:
        return None
    try:
        success_at = datetime.fromisoformat(raw_success.replace("Z", "+00:00"))
        if success_at.tzinfo is None:
            success_at = success_at.replace(tzinfo=timezone.utc)
        success_at = success_at.astimezone(timezone.utc)
    except ValueError:
        return None
    retry_at = success_at + timedelta(seconds=grace_seconds)
    if current < retry_at:
        return {"reason": "post_static_exact_window", "retry_at": retry_at}
    return None


def _create_sqlite_snapshot(source_path: str, target_path: Path) -> None:
    """Take a transactionally consistent SQLite backup for export + revisions."""

    source = Path(source_path).resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=60.0) as source_db:
        with sqlite3.connect(target_path, timeout=60.0) as target_db:
            source_db.backup(target_db)


def _snapshot_event_revisions(snapshot_path: Path) -> dict[str, str]:
    engine = create_engine(f"sqlite:///{snapshot_path}")
    try:
        with Session(engine) as session:
            events = session.exec(select(Event)).all()
            return {
                str(int(event.id)): event_public_revision(event)
                for event in events
                if event.id is not None and int(event.id) > 0
            }
    finally:
        engine.dispose()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Replace a durable receipt without exposing partial JSON to readers."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


async def _run_process(cmd: list[str], *, stage: str, run_id: int | None) -> list[str]:
    logger.info(
        "event_vector_sync stage_start run_id=%s stage=%s command=%s",
        run_id,
        stage,
        Path(cmd[1]).name if len(cmd) > 1 else cmd[0],
    )
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(ROOT),
        env=os.environ.copy(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert proc.stdout is not None
    lines: list[str] = []
    while True:
        raw = await proc.stdout.readline()
        if not raw:
            break
        line = raw.decode("utf-8", errors="replace").rstrip()
        if line:
            lines.append(line)
            logger.info("event_vector_sync run_id=%s stage=%s output=%s", run_id, stage, line)
    code = await proc.wait()
    if code:
        raise RuntimeError(
            f"event vector {stage} failed code={code}: " + " | ".join(lines[-12:])
        )
    logger.info("event_vector_sync stage_done run_id=%s stage=%s", run_id, stage)
    return lines


async def run_event_vector_sync(
    db: Any,
    *,
    trigger: str,
    owner_event_id: int | None = None,
    scheduler_run_id: str | None = None,
) -> dict[str, Any]:
    """Reconcile the full actionable catalog and persist an auditable ops_run."""

    if not enabled():
        return {"status": "skipped", "reason": "ENABLE_EVENT_VECTOR_SYNC is off"}
    db_path = str(getattr(db, "path", "") or "")
    if not db_path or db_path == ":memory:":
        raise RuntimeError("event vector sync requires a file-backed SQLite DB")

    release_guard = await asyncio.to_thread(static_release_guard, db_path)
    if release_guard is not None:
        logger.info(
            "event_vector_sync deferred reason=%s retry_at=%s",
            release_guard["reason"],
            release_guard["retry_at"].isoformat(),
        )
        return {
            "status": "deferred",
            "reason": release_guard["reason"],
            "retry_at": release_guard["retry_at"].isoformat(),
        }

    details: dict[str, Any] = {
        "owner_event_id": owner_event_id,
        "scheduler_run_id": scheduler_run_id,
        "db_path": db_path,
        "document_kinds": ["search_v3", "related_v1"],
    }
    ops_run_id = await start_ops_run(
        db,
        kind="event_vector_sync",
        trigger=trigger,
        operator_id=0 if trigger == "scheduled" else None,
        details=details,
    )
    started = asyncio.get_running_loop().time()
    try:
        async with _SYNC_LOCK:
            catalog_limit = _env_int("EVENT_VECTOR_SYNC_CATALOG_LIMIT", 5000, minimum=1)
            provider_cap = _env_int("EVENT_VECTOR_SYNC_MAX_PROVIDER_CALLS", 500, minimum=1)
            current_date = datetime.now(LOCAL_TZ).date().isoformat()
            with tempfile.TemporaryDirectory(prefix="event-vector-sync-") as tmp:
                tmp_path = Path(tmp)
                report_path = tmp_path / "sync-report.json"
                snapshot_path = tmp_path / "events.sqlite"
                await asyncio.to_thread(_create_sqlite_snapshot, db_path, snapshot_path)
                event_revisions = await asyncio.to_thread(
                    _snapshot_event_revisions, snapshot_path
                )
                await _run_process(
                    [
                        sys.executable,
                        str(ROOT / "site" / "scripts" / "export-production-preview-data.py"),
                        "--db",
                        str(snapshot_path),
                        "--output-dir",
                        str(tmp_path),
                        "--limit",
                        str(catalog_limit),
                        "--current-date",
                        current_date,
                        "--include-ids",
                        "",
                        "--skip-related",
                        "--skip-image-probes",
                    ],
                    stage="export",
                    run_id=ops_run_id,
                )
                await _run_process(
                    [
                        sys.executable,
                        str(ROOT / "scripts" / "sync_event_search_vectors_to_supabase.py"),
                        "--apply",
                        "--preview-events-json",
                        str(tmp_path / "preview-events.json"),
                        "--site-origin",
                        (os.getenv("PUBLIC_SITE_ORIGIN") or "https://kenigevents.ru").strip(),
                        "--ics-base-url",
                        (os.getenv("PUBLIC_ICS_BASE_URL") or "https://static.kenigevents.ru/ics").strip(),
                        "--embedding-model",
                        (os.getenv("EVENT_SEARCH_EMBEDDING_MODEL") or "gemini-embedding-2").strip(),
                        "--embedding-dim",
                        str(_env_int("EVENT_SEARCH_EMBEDDING_DIM", 768, minimum=1)),
                        "--google-key-env",
                        (os.getenv("EVENT_VECTOR_SYNC_GOOGLE_KEY_ENV") or "GOOGLE_API_KEY4").strip(),
                        "--max-provider-calls",
                        str(provider_cap),
                        "--sleep-seconds",
                        (os.getenv("EVENT_VECTOR_SYNC_SLEEP_SECONDS") or "0.2").strip(),
                        "--prune-missing",
                        "--require-complete",
                        "--report-json",
                        str(report_path),
                    ],
                    stage="project",
                    run_id=ops_run_id,
                )
                report = json.loads(report_path.read_text(encoding="utf-8"))
                for hash_key in (
                    "catalog_revision",
                    "corpus_revision",
                    "search_document_revision",
                    "search_v3_hash",
                    "related_v1_hash",
                ):
                    if not re.fullmatch(r"[0-9a-f]{64}", str(report.get(hash_key) or "")):
                        raise RuntimeError(f"event vector report missing valid {hash_key}")
                if report["corpus_revision"] != report["search_v3_hash"]:
                    raise RuntimeError("event vector report corpus/search_v3 revision mismatch")
                if report["search_document_revision"] != report["corpus_revision"]:
                    raise RuntimeError("event vector report search-document revision mismatch")
                coverage = report.get("coverage")
                if not isinstance(coverage, dict) or coverage.get("status") != "complete":
                    raise RuntimeError("event vector report coverage is not complete")

            receipt_path = Path(
                (os.getenv("EVENT_VECTOR_SYNC_RECEIPT_PATH") or "").strip()
                or DEFAULT_RECEIPT_PATH
            )
            receipt = {
                "schema_version": RECEIPT_SCHEMA_VERSION,
                "status": "complete",
                "complete": True,
                "run_id": ops_run_id,
                "projection_run_id": ops_run_id,
                "projected_at": datetime.now(LOCAL_TZ).isoformat(),
                "embedding_model": report.get("embedding_model"),
                "embedding_dim": report.get("embedding_dim"),
                "document_kinds": report.get("document_kinds") or ["search_v3", "related_v1"],
                "events": int(report.get("events") or 0),
                "catalog_revision": report.get("catalog_revision"),
                "corpus_revision": report.get("corpus_revision"),
                "search_document_revision": report.get("search_document_revision"),
                "search_v3_hash": report.get("search_v3_hash"),
                "related_v1_hash": report.get("related_v1_hash"),
                "coverage": report.get("coverage"),
                "event_revisions": event_revisions,
            }
            await asyncio.to_thread(_atomic_write_json, receipt_path, receipt)

        metrics = {
            "events": int(report.get("events") or 0),
            "documents_upserted": int(report.get("documents_upserted") or 0),
            "embeddings_upserted": int(report.get("embeddings_upserted") or 0),
            "embeddings_skipped_unchanged": int(report.get("embeddings_skipped_unchanged") or 0),
            "provider_calls": int(report.get("provider_calls") or 0),
            "stale_events_deleted": int(report.get("stale_events_deleted") or 0),
            "not_embedded_due_call_cap": int(report.get("not_embedded_due_call_cap") or 0),
            "duration_seconds": round(asyncio.get_running_loop().time() - started, 3),
        }
        details.update(
            {
                "complete": bool(report.get("complete")),
                "embedding_model": report.get("embedding_model"),
                "embedding_dim": report.get("embedding_dim"),
                "embeddings_skipped_by_kind": report.get("embeddings_skipped_by_kind") or {},
                "stale_event_ids": report.get("stale_event_ids") or [],
                "search_v3_hash": report.get("search_v3_hash"),
                "related_v1_hash": report.get("related_v1_hash"),
                "catalog_revision": report.get("catalog_revision"),
                "corpus_revision": report.get("corpus_revision"),
                "search_document_revision": report.get("search_document_revision"),
                "coverage_status": (report.get("coverage") or {}).get("status"),
                "receipt_path": str(receipt_path),
                "event_revisions_count": len(event_revisions),
            }
        )
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="success",
            metrics=metrics,
            details=details,
        )
        logger.info(
            "event_vector_sync complete run_id=%s trigger=%s owner_event_id=%s metrics=%s",
            ops_run_id,
            trigger,
            owner_event_id,
            json.dumps(metrics, ensure_ascii=False, sort_keys=True),
        )
        return {"status": "success", "ops_run_id": ops_run_id, **metrics, **details}
    except Exception as exc:
        details["error"] = str(exc)[:4000]
        details["failure_stage"] = "event_vector_sync"
        await finish_ops_run(
            db,
            run_id=ops_run_id,
            status="failed",
            metrics={"duration_seconds": round(asyncio.get_running_loop().time() - started, 3)},
            details=details,
        )
        logger.exception(
            "event_vector_sync failed run_id=%s trigger=%s owner_event_id=%s",
            ops_run_id,
            trigger,
            owner_event_id,
        )
        raise


async def job_event_vector_sync(event_id: int, db: Any, bot: Any) -> bool:
    result = await run_event_vector_sync(db, trigger="outbox", owner_event_id=event_id)
    if result.get("status") == "deferred":
        raise EventVectorSyncDeferred(
            str(result.get("reason") or "release_guard"),
            datetime.fromisoformat(str(result["retry_at"])),
        )
    return result.get("status") == "success"
