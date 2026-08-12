from __future__ import annotations

import asyncio
import logging
import os
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from kaggle_registry import list_jobs, update_job_meta
from video_announce.kaggle_client import KaggleClient

logger = logging.getLogger(__name__)


REMOTE_TELEGRAM_KAGGLE_JOB_TYPES = frozenset(
    {
        "audio_transcription",
        "guide_monitoring",
        "kenigsberg_story",
        "tg_monitoring",
        "telegraph_cache_probe",
    }
)
UNKNOWN_STATUS_STALE_MINUTES_ENV = "REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES"
DEFAULT_UNKNOWN_STATUS_STALE_MINUTES = 390
AUTH_SCOPE_META_KEYS = (
    "remote_telegram_auth_scope",
    "auth_scope",
    "auth_source",
    "auth_bundle_env",
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
    auth_scope: str | None
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


def normalize_remote_telegram_auth_scope(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw or None


def remote_telegram_auth_scope_from_meta(meta: dict[str, Any] | None) -> str | None:
    if not isinstance(meta, dict):
        return None
    for key in AUTH_SCOPE_META_KEYS:
        scope = normalize_remote_telegram_auth_scope(meta.get(key))
        if scope:
            return scope
    return None


def remote_telegram_auth_scopes_conflict(
    current_auth_scope: str | None,
    existing_auth_scope: str | None,
) -> bool:
    current = normalize_remote_telegram_auth_scope(current_auth_scope)
    existing = normalize_remote_telegram_auth_scope(existing_auth_scope)
    if current is None or existing is None:
        return True
    return current == existing


def _unknown_status_stale_minutes() -> int:
    raw = (os.getenv(UNKNOWN_STATUS_STALE_MINUTES_ENV) or "").strip()
    if not raw:
        return DEFAULT_UNKNOWN_STATUS_STALE_MINUTES
    try:
        return max(60, int(raw))
    except Exception:
        logger.warning(
            "remote_telegram_session.invalid_unknown_stale_minutes env=%s value=%r default=%s",
            UNKNOWN_STATUS_STALE_MINUTES_ENV,
            raw,
            DEFAULT_UNKNOWN_STATUS_STALE_MINUTES,
        )
        return DEFAULT_UNKNOWN_STATUS_STALE_MINUTES


def _parse_created_at(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _job_age_minutes(job: dict[str, Any]) -> float | None:
    created_at = _parse_created_at(job.get("created_at"))
    if created_at is None:
        return None
    return (datetime.now(timezone.utc) - created_at).total_seconds() / 60.0


def _is_transient_kaggle_status_error(exc: Exception) -> bool:
    if isinstance(exc, ssl.SSLError) or exc.__class__.__name__.endswith("SSLError"):
        return True
    if isinstance(exc, ConnectionError) or exc.__class__.__name__.endswith("ConnectionError"):
        return True
    if exc.__class__.__name__.endswith("Timeout"):
        return True
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    try:
        status_int = int(status_code)
    except Exception:
        status_int = 0
    return 500 <= status_int <= 599


def _status_lookup_error_message(exc: Exception) -> str:
    return f"Kaggle status lookup failed: {type(exc).__name__}: {exc}"


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
        if conflict.auth_scope:
            part += f" auth={conflict.auth_scope}"
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
        if conflict.auth_scope:
            line += f" auth={conflict.auth_scope}"
        lines.append(line)
        if conflict.failure_message:
            lines.append(f"  причина: {conflict.failure_message}")
    return lines


async def find_remote_telegram_session_conflicts(
    *,
    current_job_type: str | None = None,
    current_kernel_ref: str | None = None,
    current_auth_scope: str | None = None,
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
        auth_scope = remote_telegram_auth_scope_from_meta(meta)
        if not remote_telegram_auth_scopes_conflict(current_auth_scope, auth_scope):
            continue
        try:
            status_payload = await asyncio.to_thread(client.get_kernel_status, kernel_ref)
        except Exception as exc:
            failure_message = _status_lookup_error_message(exc)
            age_minutes = _job_age_minutes(job)
            stale_after = _unknown_status_stale_minutes()
            if (
                _is_transient_kaggle_status_error(exc)
                and age_minutes is not None
                and age_minutes >= stale_after
            ):
                logger.warning(
                    "remote_telegram_session.ignore_stale_unknown job_type=%s kernel=%s run_id=%s age_minutes=%.1f stale_after=%s failure=%s",
                    job_type,
                    kernel_ref,
                    str(meta.get("run_id") or "").strip() or None,
                    age_minutes,
                    stale_after,
                    failure_message,
                )
                try:
                    await update_job_meta(
                        job_type,
                        kernel_ref,
                        meta_updates={
                            "remote_session_guard_ignored_at": datetime.now(timezone.utc).isoformat(),
                            "remote_session_guard_ignore_reason": "stale_transient_status_lookup_failure",
                            "remote_session_guard_failure": failure_message,
                            "remote_session_guard_age_minutes": round(age_minutes, 1),
                        },
                    )
                except Exception:
                    logger.warning(
                        "remote_telegram_session.mark_stale_unknown_failed job_type=%s kernel=%s",
                        job_type,
                        kernel_ref,
                        exc_info=True,
                    )
                continue
            conflicts.append(
                RemoteTelegramSessionConflict(
                    job_type=job_type,
                    kernel_ref=kernel_ref,
                    run_id=str(meta.get("run_id") or "").strip() or None,
                    status="UNKNOWN",
                    created_at=str(job.get("created_at") or "").strip() or None,
                    failure_message=failure_message,
                    auth_scope=auth_scope,
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
                auth_scope=auth_scope,
                meta=dict(meta),
            )
        )
    return conflicts


async def raise_if_remote_telegram_session_busy(
    *,
    current_job_type: str,
    current_kernel_ref: str | None = None,
    current_auth_scope: str | None = None,
) -> None:
    conflicts = await find_remote_telegram_session_conflicts(
        current_job_type=current_job_type,
        current_kernel_ref=current_kernel_ref,
        current_auth_scope=current_auth_scope,
    )
    if conflicts:
        raise RemoteTelegramSessionBusyError(conflicts)
