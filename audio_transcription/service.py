from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import shutil
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .asset_store import AudioAssetStore, AudioFileParam
from .config import AudioTranscriptionConfig
from .contracts import JobState, Precision
from .job_store import AudioJobStore, JobNotFound, TranscriptionJob
from .kaggle_backend import KaggleAudioBackend
from .time_anchor import parse_aware_datetime

logger = logging.getLogger(__name__)


class AudioTranscriptionService:
    """Durable host-side orchestration around the Kaggle/Telegram worker."""

    def __init__(
        self,
        config: AudioTranscriptionConfig,
        *,
        asset_store: AudioAssetStore | None = None,
        job_store: AudioJobStore | None = None,
        backend: KaggleAudioBackend | None = None,
    ) -> None:
        self.config = config
        config.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        config.result_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        config.root.chmod(0o700)
        config.result_root.chmod(0o700)
        self.asset_store = asset_store or AudioAssetStore(
            config.asset_root,
            allowed_hosts=config.allowed_hosts,
            max_asset_bytes=config.max_asset_bytes,
            max_store_bytes=config.max_store_bytes,
            ttl_seconds=config.asset_ttl_seconds,
            timeout_seconds=config.download_timeout_seconds,
        )
        self.job_store = job_store or AudioJobStore(config.job_db_path)
        self.backend = backend or KaggleAudioBackend(config, self.asset_store)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._monitor_task: asyncio.Task[None] | None = None
        self._dispatch_lock = asyncio.Semaphore(1)
        self._reconcile_locks: dict[str, asyncio.Lock] = {}
        self._last_retention_cleanup = 0.0
        self._closed = False

    async def start_runtime(self) -> None:
        self._cleanup_expired_results()
        if self._monitor_task is None or self._monitor_task.done():
            self._closed = False
            self._monitor_task = asyncio.create_task(
                self._monitor_loop(), name="audio-transcription-monitor"
            )
        for job in self.job_store.active_jobs():
            if job.state in {JobState.QUEUED, JobState.DISPATCHING}:
                self._schedule_dispatch(job)

    async def close(self) -> None:
        self._closed = True
        tasks = [task for task in self._tasks.values() if not task.done()]
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
            tasks.append(self._monitor_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._monitor_task = None

    @staticmethod
    def _fingerprint_request(request: dict[str, Any]) -> str:
        canonical = json.dumps(
            request,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _validate_timezone(name: str) -> None:
        try:
            ZoneInfo(name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {name}") from exc

    async def start_transcription(
        self,
        *,
        owner_binding: str,
        file: AudioFileParam,
        idempotency_key: str,
        precision: Precision,
        timezone_name: str,
        recording_started_at: str | None,
    ) -> dict[str, Any]:
        self._validate_timezone(timezone_name)
        if recording_started_at:
            parse_aware_datetime(recording_started_at)
        asset = await self.asset_store.ingest(file, owner_binding=owner_binding)
        request_core = {
            "precision": Precision(precision).value,
            "timezone": timezone_name,
            "recording_started_at": recording_started_at,
            "language": "ru",
            "source_sha256": asset.content_digest,
            "source_name": asset.display_name,
            "source_suffix": asset.suffix,
        }
        request = {
            **request_core,
            "request_fingerprint": self._fingerprint_request(request_core),
        }
        try:
            job, created = self.job_store.create(
                owner_binding=owner_binding,
                idempotency_key=idempotency_key,
                asset_ref=asset.storage_ref,
                request=request,
            )
        except Exception:
            try:
                self.asset_store.delete(
                    asset.storage_ref, owner_binding=owner_binding
                )
            except Exception:
                pass
            raise
        if not created:
            if job.request.get("request_fingerprint") != request["request_fingerprint"]:
                try:
                    self.asset_store.delete(asset.storage_ref, owner_binding=owner_binding)
                except Exception:
                    pass
                raise ValueError("idempotency key is already bound to a different audio request")
            if job.asset_ref != asset.storage_ref:
                try:
                    self.asset_store.delete(asset.storage_ref, owner_binding=owner_binding)
                except Exception:
                    pass
        elif job.state is JobState.QUEUED:
            self._schedule_dispatch(job)
        return {
            **job.public_dict(),
            "created": created,
            "source": {
                "sha256": asset.content_digest,
                "mime_type": asset.mime_type,
                "byte_length": asset.byte_length,
                "display_name": asset.display_name,
            },
        }

    def _schedule_dispatch(self, job: TranscriptionJob) -> None:
        existing = self._tasks.get(job.job_ref)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._dispatch(job.job_ref, job.owner_binding),
            name=f"audio-transcription-dispatch-{job.job_ref[-8:]}",
        )
        self._tasks[job.job_ref] = task
        task.add_done_callback(lambda _task, ref=job.job_ref: self._tasks.pop(ref, None))

    async def _dispatch(self, job_ref: str, owner_binding: str) -> None:
        async with self._dispatch_lock:
            try:
                job = self.job_store.get(job_ref, owner_binding=owner_binding)
            except JobNotFound:
                return
            if job.state not in {JobState.QUEUED, JobState.DISPATCHING}:
                return
            attempt = int((job.progress or {}).get("dispatch_attempt", 0)) + 1
            job = self.job_store.update(
                job_ref,
                state=JobState.DISPATCHING,
                progress={
                    "phase": "dispatching",
                    "progress_percent": 5,
                    "dispatch_attempt": attempt,
                },
            )
            try:
                receipt = await self.backend.dispatch(job)
            except Exception as exc:
                retry_safe = bool(getattr(exc, "retry_safe", False))
                is_session_busy = type(exc).__name__ == "RemoteTelegramSessionBusyError"
                if is_session_busy or (retry_safe and attempt < 4):
                    self.job_store.update(
                        job_ref,
                        state=JobState.QUEUED,
                        progress={
                            "phase": (
                                "waiting_for_remote_session"
                                if is_session_busy
                                else "dispatch_retry"
                            ),
                            "progress_percent": 0,
                            # A legitimate shared-session wait must not consume the
                            # bounded transport retry budget.
                            "dispatch_attempt": max(0, attempt - 1) if is_session_busy else attempt,
                            "retry_safe": True,
                        },
                    )
                    return
                code = getattr(exc, "code", None) or (
                    "REMOTE_TELEGRAM_SESSION_BUSY"
                    if is_session_busy
                    else "AUDIO_DISPATCH_FAILED"
                )
                self.job_store.update(
                    job_ref,
                    state=JobState.FAILED,
                    error_code=str(code)[:64],
                    error_detail=type(exc).__name__,
                    progress={"phase": "failed", "progress_percent": 100},
                )
                try:
                    self.asset_store.delete(
                        job.asset_ref, owner_binding=job.owner_binding
                    )
                except Exception:
                    logger.warning(
                        "audio_transcription.asset_cleanup_failed job_ref=%s",
                        job_ref,
                        exc_info=True,
                    )
                logger.exception(
                    "audio_transcription.dispatch_failed job_ref=%s", job_ref
                )
                return
            self.job_store.update(
                job_ref,
                state=JobState.RUNNING,
                kernel_ref=receipt.kernel_ref,
                input_dataset_ref=receipt.input_dataset_ref,
                key_dataset_ref=receipt.key_dataset_ref,
                progress={"phase": "kaggle_running", "progress_percent": 20},
            )

    def _cleanup_expired_results(self) -> int:
        cutoff = time.time() - self.config.result_retention_days * 24 * 3600
        removed = 0
        root = self.config.result_root.resolve()
        if not root.exists():
            return 0
        for candidate in root.iterdir():
            try:
                info = candidate.lstat()
            except FileNotFoundError:
                continue
            if candidate.is_symlink() or not candidate.is_dir() or info.st_mtime > cutoff:
                continue
            resolved = candidate.resolve()
            if resolved.parent != root:
                continue
            shutil.rmtree(resolved)
            removed += 1
        self._last_retention_cleanup = time.monotonic()
        return removed

    def _maybe_cleanup_expired_results(self) -> None:
        if time.monotonic() - self._last_retention_cleanup >= 3600:
            removed = self._cleanup_expired_results()
            if removed:
                logger.info("audio_transcription.results_expired count=%s", removed)

    async def _monitor_loop(self) -> None:
        while not self._closed:
            try:
                self._maybe_cleanup_expired_results()
                active = self.job_store.active_jobs()
                for job in active:
                    if job.state is JobState.QUEUED:
                        self._schedule_dispatch(job)
                    elif job.state is JobState.RUNNING:
                        await self._reconcile_job(job)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("audio_transcription.monitor_failed")
            await asyncio.sleep(self.config.poll_interval_seconds)

    async def _reconcile_job(self, job: TranscriptionJob) -> TranscriptionJob:
        lock = self._reconcile_locks.setdefault(job.job_ref, asyncio.Lock())
        async with lock:
            try:
                current = self.job_store.get(
                    job.job_ref, owner_binding=job.owner_binding
                )
            except JobNotFound:
                return job
            if current.state is not JobState.RUNNING:
                return current
            result = await self.backend.reconcile(current)
            if result.state is JobState.RUNNING:
                return self.job_store.update(current.job_ref, progress=result.progress)
            updated = self.job_store.update(
                current.job_ref,
                state=result.state,
                result_dir=result.result_dir,
                error_code=result.error_code,
                error_detail=result.error_detail,
                progress=result.progress,
            )
            if result.state.terminal:
                try:
                    self.asset_store.delete(
                        current.asset_ref, owner_binding=current.owner_binding
                    )
                except Exception:
                    logger.warning(
                        "audio_transcription.asset_cleanup_failed job_ref=%s",
                        current.job_ref,
                        exc_info=True,
                    )
                self._reconcile_locks.pop(current.job_ref, None)
            return updated

    async def status(self, *, job_ref: str, owner_binding: str) -> dict[str, Any]:
        job = self.job_store.get(job_ref, owner_binding=owner_binding)
        if job.state is JobState.RUNNING:
            job = await self._reconcile_job(job)
        payload = job.public_dict()
        if job.state is JobState.COMPLETE:
            try:
                transcript = self._result_file(job, "transcript.json")
            except ValueError:
                payload["result_available"] = False
                payload["result_expired"] = True
            else:
                payload["result_available"] = True
                payload["available_views"] = [
                    "segments",
                    "plain",
                    "timeline",
                    "json",
                    "srt",
                    "vtt",
                ]
                try:
                    result = json.loads(transcript.read_text(encoding="utf-8"))
                    payload["segment_count"] = len(result.get("segments") or [])
                    payload["recording_anchor"] = result.get("recording_anchor")
                    payload["source"] = result.get("source")
                    manifest = json.loads(
                        self._result_file(job, "manifest.json").read_text(
                            encoding="utf-8"
                        )
                    )
                    telegram = manifest.get("telegram")
                    if isinstance(telegram, dict):
                        payload["telegram"] = telegram
                except Exception:
                    payload["result_metadata_unavailable"] = True
        return payload

    def _result_file(self, job: TranscriptionJob, file_name: str) -> Path:
        if not job.result_dir:
            raise ValueError("transcription result is not available")
        root = Path(job.result_dir).resolve()
        configured = self.config.result_root.resolve()
        if root != configured and configured not in root.parents:
            raise ValueError("invalid transcription result path")
        path = (root / file_name).resolve()
        if path.parent != root or not path.is_file():
            raise ValueError("requested transcription result is unavailable")
        return path

    async def get_result(
        self,
        *,
        job_ref: str,
        owner_binding: str,
        view: str,
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        job = self.job_store.get(job_ref, owner_binding=owner_binding)
        if job.state is JobState.RUNNING:
            job = await self._reconcile_job(job)
        if job.state is not JobState.COMPLETE:
            return {
                **job.public_dict(),
                "ready": False,
            }
        if not job.result_dir or not Path(job.result_dir).is_dir():
            raise ValueError("transcription result expired")
        if view == "segments":
            payload = json.loads(
                self._result_file(job, "transcript.json").read_text(encoding="utf-8")
            )
            segments = tuple(payload.get("segments") or [])
            if offset < 0 or not 1 <= limit <= 100:
                raise ValueError("segment offset/limit is invalid")
            page = segments[offset : offset + limit]
            next_offset = offset + len(page)
            return {
                "job_ref": job.job_ref,
                "state": job.state.value,
                "ready": True,
                "view": "segments",
                "segments": page,
                "next_offset": next_offset if next_offset < len(segments) else None,
                "total": len(segments),
                "recording_anchor": payload.get("recording_anchor"),
                "source": payload.get("source"),
            }
        file_names = {
            "plain": "transcript.txt",
            "timeline": "transcript.timeline.txt",
            "json": "transcript.json",
            "srt": "transcript.srt",
            "vtt": "transcript.vtt",
        }
        file_name = file_names.get(view)
        if file_name is None:
            raise ValueError("unsupported transcription result view")
        text = self._result_file(job, file_name).read_text(encoding="utf-8")
        if offset < 0 or not 1 <= limit <= 60_000:
            raise ValueError("text offset/limit is invalid")
        chunk = text[offset : offset + limit]
        next_offset = offset + len(chunk)
        return {
            "job_ref": job.job_ref,
            "state": job.state.value,
            "ready": True,
            "view": view,
            "text": chunk,
            "next_offset": next_offset if next_offset < len(text) else None,
            "total_chars": len(text),
        }
