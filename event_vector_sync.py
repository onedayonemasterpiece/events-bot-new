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
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ops_run import finish_ops_run, start_ops_run

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent
LOCAL_TZ = ZoneInfo("Europe/Kaliningrad")
_SYNC_LOCK = asyncio.Lock()


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
                await _run_process(
                    [
                        sys.executable,
                        str(ROOT / "site" / "scripts" / "export-production-preview-data.py"),
                        "--db",
                        db_path,
                        "--output-dir",
                        str(tmp_path),
                        "--limit",
                        str(catalog_limit),
                        "--current-date",
                        current_date,
                        "--include-ids",
                        "",
                        "--skip-related",
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
    return result.get("status") == "success"

