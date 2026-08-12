from __future__ import annotations

import asyncio
from typing import Any

from kaggle_registry import list_jobs, remove_job
from remote_telegram_session import (
    RemoteTelegramSessionBusyError,
    RemoteTelegramSessionConflict,
    raise_if_remote_telegram_session_busy,
    remote_telegram_auth_scope_from_meta,
    remote_telegram_auth_scopes_conflict,
)
from video_announce.kaggle_client import KaggleClient

_AUDIO_JOB_TYPE = "audio_transcription"
_TERMINAL = {
    "CANCEL_ACKNOWLEDGED",
    "CANCELED",
    "CANCELLED",
    "COMPLETE",
    "ERROR",
    "FAILED",
}


async def raise_if_audio_transcription_session_busy(
    *,
    current_auth_scope: str,
    client: KaggleClient | None = None,
) -> None:
    """Apply the shared guard plus a compatibility check for this new job kind.

    The central ``REMOTE_TELEGRAM_KAGGLE_JOB_TYPES`` set is also updated by the
    integration patch. Keeping this local check makes the new worker fail closed
    during a rolling deploy where the package and central guard may briefly be
    on adjacent revisions.
    """

    await raise_if_remote_telegram_session_busy(
        current_job_type=_AUDIO_JOB_TYPE,
        current_kernel_ref=None,
        current_auth_scope=current_auth_scope,
    )
    jobs = await list_jobs()
    candidates = [
        job
        for job in jobs
        if isinstance(job, dict)
        and str(job.get("type") or "").strip() == _AUDIO_JOB_TYPE
    ]
    if not candidates:
        return
    kaggle = client or KaggleClient()
    conflicts: list[RemoteTelegramSessionConflict] = []
    for job in candidates:
        kernel_ref = str(job.get("kernel_ref") or "").strip()
        if not kernel_ref:
            continue
        meta = job.get("meta") if isinstance(job.get("meta"), dict) else {}
        auth_scope = remote_telegram_auth_scope_from_meta(meta)
        if not remote_telegram_auth_scopes_conflict(current_auth_scope, auth_scope):
            continue
        try:
            status_payload = await asyncio.to_thread(kaggle.get_kernel_status, kernel_ref)
            state = str((status_payload or {}).get("status") or "").strip().upper()
        except Exception as exc:
            conflicts.append(
                RemoteTelegramSessionConflict(
                    job_type=_AUDIO_JOB_TYPE,
                    kernel_ref=kernel_ref,
                    run_id=str(meta.get("run_id") or "").strip() or None,
                    status="UNKNOWN",
                    created_at=str(job.get("created_at") or "").strip() or None,
                    failure_message=f"Kaggle status lookup failed: {type(exc).__name__}",
                    auth_scope=auth_scope,
                    meta=dict(meta),
                )
            )
            continue
        if state in _TERMINAL:
            try:
                await remove_job(_AUDIO_JOB_TYPE, kernel_ref)
            except Exception:
                pass
            continue
        failure = None
        for key in (
            "failureMessage",
            "failure_message",
            "errorMessage",
            "error_message",
            "error",
        ):
            if status_payload.get(key):
                failure = str(status_payload[key])[:500]
                break
        conflicts.append(
            RemoteTelegramSessionConflict(
                job_type=_AUDIO_JOB_TYPE,
                kernel_ref=kernel_ref,
                run_id=str(meta.get("run_id") or "").strip() or None,
                status=state or "UNKNOWN",
                created_at=str(job.get("created_at") or "").strip() or None,
                failure_message=failure,
                auth_scope=auth_scope,
                meta=dict(meta),
            )
        )
    if conflicts:
        raise RemoteTelegramSessionBusyError(conflicts)
