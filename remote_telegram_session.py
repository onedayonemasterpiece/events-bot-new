from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from kaggle_registry import list_jobs
from video_announce.kaggle_client import KaggleClient


REMOTE_TELEGRAM_KAGGLE_JOB_TYPES = frozenset(
    {
        "guide_monitoring",
        "tg_monitoring",
        "telegraph_cache_probe",
    }
)
TERMINAL_KAGGLE_STATES = frozenset(
    {
        "CANCEL_ACKNOWLEDGED",
        "CANCELED",
        "CANCELLED",
        "COMPLETE",
        "ERROR",
        "FAILED",
    }
)


@dataclass(slots=True, frozen=True)
class RemoteTelegramSessionConflict:
    job_type: str
    kernel_ref: str
    run_id: str | None
    status: str
    created_at: str | None
    failure_message: str | None
    meta: dict[str, Any]


class RemoteTelegramSessionBusyError(RuntimeError):
    def __init__(self, conflicts: list[RemoteTelegramSessionConflict]):
        self.conflicts = list(conflicts)
        super().__init__(describe_remote_telegram_session_conflicts(conflicts))


def _extract_failure_message(status: dict[str, Any] | None) -> str:
    if not isinstance(status, dict):
        return ""
    for key in ("failureMessage", "failure_message", "errorMessage", "error_message", "error"):
        value = status.get(key)
        if value:
            return str(value).strip()
    return ""


def describe_remote_telegram_session_conflicts(
    conflicts: list[RemoteTelegramSessionConflict],
) -> str:
    if not conflicts:
        return "remote Telegram session is busy"
    parts: list[str] = []
    for conflict in conflicts[:3]:
        part = f"{conflict.job_type} status={conflict.status or 'UNKNOWN'}"
        if conflict.run_id:
            part += f" run_id={conflict.run_id}"
        if conflict.kernel_ref:
            part += f" kernel={conflict.kernel_ref}"
        if conflict.failure_message:
            part += f" failure={conflict.failure_message}"
        parts.append(part)
    return "remote Telegram session is busy: " + " | ".join(parts)


def format_remote_telegram_session_busy_lines(
    conflicts: list[RemoteTelegramSessionConflict],
    *,
    actor_label: str,
) -> list[str]:
    lines = [
        f"⏳ {actor_label}: удалённая Telegram session уже занята другим Kaggle run.",
    ]
    for conflict in conflicts[:3]:
        line = f"• {conflict.job_type}"
        if conflict.status:
            line += f" status={conflict.status}"
        if conflict.run_id:
            line += f" run_id={conflict.run_id}"
        if conflict.kernel_ref:
            line += f" kernel={conflict.kernel_ref}"
        lines.append(line)
        if conflict.failure_message:
            lines.append(f"  причина: {conflict.failure_message}")
    return lines


async def find_remote_telegram_session_conflicts(
    *,
    current_job_type: str | None = None,
    current_kernel_ref: str | None = None,
) -> list[RemoteTelegramSessionConflict]:
    jobs = await list_jobs()
    candidates = [
        job
        for job in jobs
        if isinstance(job, dict)
        and str(job.get("type") or "").strip() in REMOTE_TELEGRAM_KAGGLE_JOB_TYPES
    ]
    if not candidates:
        return []

    client = KaggleClient()
    conflicts: list[RemoteTelegramSessionConflict] = []
    for job in candidates:
        job_type = str(job.get("type") or "").strip()
        kernel_ref = str(job.get("kernel_ref") or "").strip()
        if not job_type or not kernel_ref:
            continue
        if current_job_type and current_kernel_ref:
            if job_type == current_job_type and kernel_ref == current_kernel_ref:
                continue

        meta = job.get("meta") if isinstance(job.get("meta"), dict) else {}
        try:
            status_payload = await asyncio.to_thread(client.get_kernel_status, kernel_ref)
        except Exception as exc:
            conflicts.append(
                RemoteTelegramSessionConflict(
                    job_type=job_type,
                    kernel_ref=kernel_ref,
                    run_id=str(meta.get("run_id") or "").strip() or None,
                    status="UNKNOWN",
                    created_at=str(job.get("created_at") or "").strip() or None,
                    failure_message=f"Kaggle status lookup failed: {type(exc).__name__}: {exc}",
                    meta=dict(meta),
                )
            )
            continue

        state = str((status_payload or {}).get("status") or "").strip().upper()
        if state in TERMINAL_KAGGLE_STATES:
            continue
        conflicts.append(
            RemoteTelegramSessionConflict(
                job_type=job_type,
                kernel_ref=kernel_ref,
                run_id=str(meta.get("run_id") or "").strip() or None,
                status=state or "UNKNOWN",
                created_at=str(job.get("created_at") or "").strip() or None,
                failure_message=_extract_failure_message(status_payload) or None,
                meta=dict(meta),
            )
        )
    return conflicts


async def raise_if_remote_telegram_session_busy(
    *,
    current_job_type: str,
    current_kernel_ref: str | None = None,
) -> None:
    conflicts = await find_remote_telegram_session_conflicts(
        current_job_type=current_job_type,
        current_kernel_ref=current_kernel_ref,
    )
    if conflicts:
        raise RemoteTelegramSessionBusyError(conflicts)
