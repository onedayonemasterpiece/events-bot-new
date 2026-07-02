"""Scheduled acceptance audit for public exhibition duplicate regressions."""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

from admin_chat import resolve_superadmin_chat_id
from db import Database
from ops_run import finish_ops_run, start_ops_run
from scripts.inspect.audit_public_exhibition_duplicates import build_audit_payload

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _env_enabled(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _status_from_payload(payload: dict[str, Any]) -> str:
    return "failed" if int(payload.get("high_confidence_duplicate_count") or 0) > 0 else "success"


def _metrics_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "public_exhibition_count": int(payload.get("public_exhibition_count") or 0),
        "high_confidence_duplicate_count": int(payload.get("high_confidence_duplicate_count") or 0),
        "high_confidence_duplicate_cluster_count": int(
            payload.get("high_confidence_duplicate_cluster_count") or 0
        ),
        "high_confidence_duplicate_total_count": int(
            payload.get("high_confidence_duplicate_total_count") or 0
        ),
        "high_confidence_duplicate_total_cluster_count": int(
            payload.get("high_confidence_duplicate_total_cluster_count") or 0
        ),
        "since_days": int(payload.get("since_days") or 14),
    }


def _details_from_payload(payload: dict[str, Any], *, run_id: str | None) -> dict[str, Any]:
    max_pairs = _int_env("EXHIBITION_DUPLICATE_AUDIT_MAX_DETAILS_PAIRS", 20, minimum=1)
    duplicates = list(payload.get("duplicates") or [])
    all_duplicates = list(payload.get("all_duplicates") or [])
    return {
        "scheduler_run_id": run_id,
        "current_date": payload.get("current_date"),
        "since_date": payload.get("since_date"),
        "since_days": payload.get("since_days"),
        "duplicates_truncated": len(duplicates) > max_pairs,
        "duplicates": duplicates[:max_pairs],
        "all_duplicate_count": len(all_duplicates),
        "all_duplicates_truncated": len(all_duplicates) > max_pairs,
        "all_duplicates": all_duplicates[:max_pairs],
    }


async def _notify_admin(
    db: Database,
    bot: Any,
    *,
    payload: dict[str, Any],
    status: str,
) -> None:
    duplicate_count = int(payload.get("high_confidence_duplicate_count") or 0)
    notify_success = _env_enabled("EXHIBITION_DUPLICATE_AUDIT_NOTIFY_ON_SUCCESS", default=False)
    if duplicate_count <= 0 and not notify_success:
        return
    if bot is None or not hasattr(bot, "send_message"):
        return
    chat_id = await resolve_superadmin_chat_id(db)
    if not chat_id:
        return

    max_pairs = _int_env("EXHIBITION_DUPLICATE_AUDIT_MAX_ALERT_PAIRS", 8, minimum=1)
    lines = [
        (
            "🚨 Exhibition duplicate audit: найдены high-confidence дубли"
            if duplicate_count > 0
            else "✅ Exhibition duplicate audit: дублей не найдено"
        ),
        f"status={status}",
        f"current_date={payload.get('current_date')} window_days={payload.get('since_days')}",
        f"public_exhibitions={payload.get('public_exhibition_count')} duplicate_pairs_since={duplicate_count} clusters_since={payload.get('high_confidence_duplicate_cluster_count')}",
        f"duplicate_pairs_total={payload.get('high_confidence_duplicate_total_count')} clusters_total={payload.get('high_confidence_duplicate_total_cluster_count')}",
    ]
    for pair in list(payload.get("duplicates") or [])[:max_pairs]:
        lines.append(
            f"• #{pair.get('left_id')} ↔ #{pair.get('right_id')} "
            f"{float(pair.get('confidence') or 0):.3f}: "
            f"{pair.get('left_title')} / {pair.get('right_title')}"
        )
    if duplicate_count > max_pairs:
        lines.append(f"… ещё {duplicate_count - max_pairs} пар(ы) в ops_run.details_json")
    try:
        await bot.send_message(chat_id, "\n".join(lines), disable_web_page_preview=True)
    except Exception:
        logger.warning("exhibition_duplicate_audit: admin notification failed", exc_info=True)


async def run_exhibition_duplicate_audit_scheduler(
    db: Database,
    bot: Any | None = None,
    *,
    trigger: str = "scheduled",
    run_id: str | None = None,
    current_date: date | None = None,
    raise_on_duplicates: bool | None = None,
) -> dict[str, Any]:
    """Run the read-only /vystavki/ duplicate acceptance audit and record ops_run.

    The audit is intentionally non-mutating.  High-confidence duplicates are a
    rollout/quality regression: the run is stored as ``failed`` and, by default,
    the scheduler raises after recording and notifying so APScheduler also emits
    a job error.
    """

    db_path = Path(getattr(db, "path", "") or "")
    since_days = _int_env("EXHIBITION_DUPLICATE_AUDIT_SINCE_DAYS", 14, minimum=1)
    should_raise = _env_enabled("EXHIBITION_DUPLICATE_AUDIT_RAISE_ON_DUPLICATES", default=True)
    if raise_on_duplicates is not None:
        should_raise = bool(raise_on_duplicates)

    ops_details_start = {
        "scheduler_run_id": run_id,
        "db_path": str(db_path),
        "since_days": since_days,
    }
    ops_run_id = await start_ops_run(
        db,
        kind="exhibition_duplicate_audit",
        trigger=trigger,
        operator_id=0 if trigger == "scheduled" else None,
        details=ops_details_start,
    )

    try:
        if not db_path or str(db_path) in {":memory:", ""}:
            raise RuntimeError("exhibition duplicate audit requires a file-backed SQLite DB")
        payload = build_audit_payload(db_path, current=current_date, since_days=since_days)
        status = _status_from_payload(payload)
        metrics = _metrics_from_payload(payload)
        details = _details_from_payload(payload, run_id=run_id)
        await finish_ops_run(db, run_id=ops_run_id, status=status, metrics=metrics, details=details)
        logger.info(
            "exhibition_duplicate_audit status=%s public_exhibitions=%s duplicates=%s clusters=%s",
            status,
            metrics["public_exhibition_count"],
            metrics["high_confidence_duplicate_count"],
            metrics["high_confidence_duplicate_cluster_count"],
        )
        await _notify_admin(db, bot, payload=payload, status=status)
        if status == "failed" and should_raise:
            raise RuntimeError(
                "high-confidence public exhibition duplicates found: "
                f"{metrics['high_confidence_duplicate_count']}"
            )
        return payload
    except Exception as exc:
        if "payload" not in locals():
            await finish_ops_run(
                db,
                run_id=ops_run_id,
                status="error",
                metrics={"since_days": since_days},
                details={**ops_details_start, "error": str(exc)},
            )
        raise
