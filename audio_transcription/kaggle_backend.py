from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kaggle_registry import register_job, remove_job
from source_parsing.telegram.split_secrets import encrypt_secret
from video_announce.kaggle_client import KaggleClient, await_dataset_ready

from .asset_store import AudioAssetStore
from .config import AudioTranscriptionConfig
from .contracts import JobState, SCHEMA_VERSION
from .job_store import TranscriptionJob
from .session_guard import raise_if_audio_transcription_session_busy

_JOB_TYPE = "audio_transcription"
_TERMINAL_COMPLETE = {"COMPLETE"}
_TERMINAL_FAILED = {
    "CANCEL_ACKNOWLEDGED",
    "CANCELED",
    "CANCELLED",
    "ERROR",
    "FAILED",
}
_SAFE_DETAIL_RE = re.compile(r"[^A-Za-z0-9_.:/ -]+")


class KaggleAudioError(RuntimeError):
    def __init__(self, code: str, message: str, *, retry_safe: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retry_safe = retry_safe


@dataclass(frozen=True, slots=True)
class DispatchReceipt:
    kernel_ref: str
    input_dataset_ref: str
    key_dataset_ref: str


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    state: JobState
    progress: dict[str, Any]
    result_dir: str | None = None
    error_code: str | None = None
    error_detail: str | None = None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_failure(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return _SAFE_DETAIL_RE.sub("_", text)[:500]


def _job_age_hours(job: TranscriptionJob) -> float | None:
    raw = str(job.created_at or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        created = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    elapsed = datetime.now(timezone.utc) - created.astimezone(timezone.utc)
    return max(0.0, elapsed.total_seconds() / 3600)


class KaggleAudioBackend:
    def __init__(
        self,
        config: AudioTranscriptionConfig,
        asset_store: AudioAssetStore,
        *,
        client: KaggleClient | None = None,
    ) -> None:
        self.config = config
        self.asset_store = asset_store
        self.client = client or KaggleClient()

    @staticmethod
    def _runtime_bundle(target: Path) -> None:
        package_root = Path(__file__).resolve().parent
        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for source in sorted(package_root.rglob("*.py")):
                if "__pycache__" in source.parts:
                    continue
                relative = source.relative_to(package_root)
                archive.write(source, Path("audio_transcription") / relative)

    @staticmethod
    def _dataset_metadata(folder: Path, *, dataset_ref: str, title: str) -> None:
        (folder / "dataset-metadata.json").write_text(
            json.dumps(
                {
                    "title": title[:50],
                    "id": dataset_ref,
                    "licenses": [{"name": "other"}],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _dataset_refs(self, job: TranscriptionJob) -> tuple[str, str]:
        username = (os.getenv("KAGGLE_USERNAME") or "").strip()
        if not username:
            raise KaggleAudioError("KAGGLE_CREDENTIALS_MISSING", "Kaggle username is missing")
        token = hashlib.sha256(job.job_ref.encode("utf-8")).hexdigest()[:16]
        return (
            f"{username}/audio-transcription-{token}",
            f"{username}/audio-transcription-key-{token}",
        )

    def _secret_payload(self) -> dict[str, str]:
        names = (
            "TG_API_ID",
            "TG_API_HASH",
            self.config.auth_bundle_env,
        )
        payload = {name: (os.getenv(name) or "").strip() for name in names}
        missing = [name for name, value in payload.items() if not value]
        if missing:
            raise KaggleAudioError(
                "TELEGRAM_AUTH_MISSING",
                "missing Telegram transcription credentials: " + ", ".join(missing),
            )
        payload["AUDIO_TRANSCRIPTION_AUTH_BUNDLE_ENV"] = self.config.auth_bundle_env
        return payload

    async def dispatch(self, job: TranscriptionJob) -> DispatchReceipt:
        await raise_if_audio_transcription_session_busy(
            current_auth_scope=self.config.auth_bundle_env,
            client=self.client,
        )
        input_ref, key_ref = self._dataset_refs(job)
        request = dict(job.request)
        request.update(
            {
                "schema_version": SCHEMA_VERSION,
                "job_ref": job.job_ref,
                "telegram_peer": self.config.telegram_peer,
                "cleanup_messages": self.config.cleanup_messages,
            }
        )
        registry_meta = {
            "run_id": job.job_ref,
            "job_ref": job.job_ref,
            "remote_telegram_auth_scope": self.config.auth_bundle_env,
            "input_dataset_ref": input_ref,
            "key_dataset_ref": key_ref,
            "source_sha256": request.get("source_sha256"),
            "phase": "dispatching",
        }
        # Register the known target before any kernel push. If the push outcome
        # becomes unknown, the session lane remains fail-closed instead of
        # immediately reusing the same Telegram auth key from another IP.
        await register_job(_JOB_TYPE, self.config.kernel_ref, meta=registry_meta)
        deploy_attempted = False
        kernel_ref = self.config.kernel_ref
        try:
            with tempfile.TemporaryDirectory(prefix="audio-transcription-handoff-") as temporary:
                root = Path(temporary)
                input_dir = root / "input"
                key_dir = root / "key"
                input_dir.mkdir()
                key_dir.mkdir()
                verified = self.asset_store.reverify(
                    job.asset_ref, owner_binding=job.owner_binding
                )
                asset = self.asset_store.copy_verified_to(
                    job.asset_ref,
                    owner_binding=job.owner_binding,
                    destination=input_dir / ("source" + verified.suffix),
                )
                request.update(
                    {
                        "source_file": "source" + asset.suffix,
                        "source_sha256": asset.content_digest,
                        "source_name": asset.display_name,
                        "source_mime": asset.mime_type,
                        "source_bytes": asset.byte_length,
                    }
                )
                (input_dir / "request.json").write_text(
                    json.dumps(request, ensure_ascii=False, indent=2, sort_keys=True),
                    encoding="utf-8",
                )
                secret_blob = json.dumps(
                    self._secret_payload(), ensure_ascii=False, sort_keys=True
                )
                encrypted, key = encrypt_secret(secret_blob)
                (input_dir / "secrets.enc").write_bytes(encrypted)
                (key_dir / "fernet.key").write_bytes(key)
                self._runtime_bundle(input_dir / "audio-transcription-runtime.bundle")
                self._dataset_metadata(
                    input_dir,
                    dataset_ref=input_ref,
                    title=f"Audio transcription {job.job_ref[-12:]}",
                )
                self._dataset_metadata(
                    key_dir,
                    dataset_ref=key_ref,
                    title=f"Audio transcription key {job.job_ref[-12:]}",
                )
                await asyncio.to_thread(self.client.create_dataset, input_dir)
                await await_dataset_ready(
                    self.client,
                    input_ref,
                    expected_files=[
                        request["source_file"],
                        "request.json",
                        "secrets.enc",
                        "audio-transcription-runtime.bundle",
                    ],
                )
                await asyncio.to_thread(self.client.create_dataset, key_dir)
                await await_dataset_ready(
                    self.client,
                    key_ref,
                    expected_files=["fernet.key"],
                )
                deploy_attempted = True
                kernel_ref = await asyncio.to_thread(
                    self.client.deploy_kernel_update,
                    self.config.kernel_source,
                    [input_ref, key_ref],
                    target_kernel_ref=self.config.kernel_ref,
                )
        except Exception as exc:
            await self._delete_dataset_quietly(input_ref)
            await self._delete_dataset_quietly(key_ref)
            if not deploy_attempted:
                try:
                    await remove_job(_JOB_TYPE, self.config.kernel_ref)
                except Exception:
                    pass
            raise KaggleAudioError(
                (
                    "KAGGLE_DISPATCH_OUTCOME_UNKNOWN"
                    if deploy_attempted
                    else "KAGGLE_DISPATCH_FAILED"
                ),
                f"Kaggle audio dispatch failed: {type(exc).__name__}",
                retry_safe=not deploy_attempted,
            ) from exc
        if kernel_ref != self.config.kernel_ref:
            await register_job(
                _JOB_TYPE,
                kernel_ref,
                meta={**registry_meta, "phase": "running"},
            )
            await remove_job(_JOB_TYPE, self.config.kernel_ref)
        else:
            await register_job(
                _JOB_TYPE,
                kernel_ref,
                meta={**registry_meta, "phase": "running"},
            )
        return DispatchReceipt(
            kernel_ref=kernel_ref,
            input_dataset_ref=input_ref,
            key_dataset_ref=key_ref,
        )

    async def reconcile(self, job: TranscriptionJob) -> ReconcileResult:
        if not job.kernel_ref:
            return ReconcileResult(
                state=JobState.QUEUED,
                progress={"phase": "queued", "progress_percent": 0},
            )
        try:
            status = await asyncio.to_thread(
                self.client.get_kernel_status, job.kernel_ref
            )
        except Exception as exc:
            return ReconcileResult(
                state=JobState.RUNNING,
                progress={
                    "phase": "status_unavailable",
                    "progress_percent": 20,
                    "retry_safe": True,
                    "status_error": type(exc).__name__,
                },
            )
        state = str(status.get("status") or "").strip().upper()
        if state in _TERMINAL_FAILED:
            failure = _safe_failure(
                status.get("failureMessage")
                or status.get("failure_message")
                or status.get("error")
            )
            await self._cleanup_remote(job)
            return ReconcileResult(
                state=JobState.FAILED,
                progress={"phase": "failed", "progress_percent": 100},
                error_code="KAGGLE_RUN_FAILED",
                error_detail=failure,
            )
        if state not in _TERMINAL_COMPLETE:
            phase = state.casefold() or "running"
            age_hours = _job_age_hours(job)
            overdue = age_hours is not None and age_hours >= self.config.max_run_hours
            return ReconcileResult(
                state=JobState.RUNNING,
                progress={
                    "phase": "overdue" if overdue else phase,
                    "provider_phase": phase,
                    "progress_percent": 50,
                    "operator_attention": overdue,
                    "run_age_hours": round(age_hours, 2) if age_hours is not None else None,
                },
            )
        result_dir = self.config.result_root / job.job_ref
        if result_dir.exists():
            shutil.rmtree(result_dir)
        result_dir.mkdir(parents=True, exist_ok=False)
        try:
            await asyncio.to_thread(
                self.client.download_kernel_output,
                job.kernel_ref,
                path=result_dir,
                force=True,
                quiet=True,
            )
            transcript_matches = list(result_dir.rglob("transcript.json"))
            manifest_matches = list(result_dir.rglob("manifest.json"))
            if len(transcript_matches) != 1 or len(manifest_matches) != 1:
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_MISSING",
                    "Kaggle output must contain exactly one transcript and manifest",
                )
            transcript_path = transcript_matches[0]
            manifest_path = manifest_matches[0]
            if transcript_path.parent != manifest_path.parent:
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "transcript and manifest directories differ"
                )
            payload = json.loads(transcript_path.read_text(encoding="utf-8"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if payload.get("schema_version") != SCHEMA_VERSION:
                raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "unexpected transcript schema")
            if payload.get("job_ref") != job.job_ref:
                raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "transcript job binding mismatch")
            expected_source = str(job.request.get("source_sha256") or "")
            observed_source = str((payload.get("source") or {}).get("sha256") or "")
            if expected_source and expected_source != observed_source:
                raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "source digest mismatch")
            files = manifest.get("files") if isinstance(manifest, dict) else None
            if not isinstance(files, dict) or set(files) != {
                "plain",
                "timeline",
                "json",
                "srt",
                "vtt",
            }:
                raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "invalid output manifest")
            if manifest.get("job_ref") != job.job_ref:
                raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "manifest job binding mismatch")
            if manifest.get("source_sha256") != observed_source:
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "manifest source binding mismatch"
                )
            telegram = manifest.get("telegram")
            if not isinstance(telegram, dict):
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "Telegram receipt summary is missing"
                )
            temporary = telegram.get("temporary_messages")
            if not isinstance(temporary, dict):
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "temporary-message summary is missing"
                )
            try:
                native_count = int(telegram.get("native_transcriptions"))
                cleanup_attempts = int(temporary.get("cleanup_attempts"))
                cleanup_succeeded = int(temporary.get("cleanup_succeeded"))
                cleanup_failed = int(temporary.get("cleanup_failed"))
            except (TypeError, ValueError) as exc:
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "invalid Telegram receipt counts"
                ) from exc
            if native_count != len(payload.get("segments") or []):
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "Telegram transcript count mismatch"
                )
            cleanup_enabled = bool(temporary.get("cleanup_enabled"))
            if cleanup_attempts != cleanup_succeeded + cleanup_failed:
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "temporary-message counts mismatch"
                )
            if cleanup_enabled and cleanup_attempts < native_count:
                raise KaggleAudioError(
                    "KAGGLE_OUTPUT_INVALID", "temporary-message cleanup evidence incomplete"
                )
            output_root = transcript_path.parent
            for entry in files.values():
                if not isinstance(entry, dict):
                    raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "invalid manifest entry")
                file_name = str(entry.get("file_name") or "")
                if not file_name or Path(file_name).name != file_name:
                    raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "unsafe manifest filename")
                candidate = output_root / file_name
                if candidate.is_symlink() or not candidate.is_file():
                    raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "manifest file missing")
                if candidate.stat().st_size != int(entry.get("byte_length") or -1):
                    raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "manifest size mismatch")
                if _sha256_file(candidate) != str(entry.get("sha256") or ""):
                    raise KaggleAudioError("KAGGLE_OUTPUT_INVALID", "manifest digest mismatch")
            await self._cleanup_remote(job)
            return ReconcileResult(
                state=JobState.COMPLETE,
                progress={"phase": "complete", "progress_percent": 100},
                result_dir=str(output_root),
            )
        except KaggleAudioError as exc:
            shutil.rmtree(result_dir, ignore_errors=True)
            await self._cleanup_remote(job)
            return ReconcileResult(
                state=JobState.FAILED,
                progress={"phase": "collect_failed", "progress_percent": 100},
                error_code=exc.code,
                error_detail=_safe_failure(exc),
            )
        except Exception as exc:
            shutil.rmtree(result_dir, ignore_errors=True)
            await self._cleanup_remote(job)
            return ReconcileResult(
                state=JobState.FAILED,
                progress={"phase": "collect_failed", "progress_percent": 100},
                error_code="KAGGLE_OUTPUT_FAILED",
                error_detail=type(exc).__name__,
            )

    async def _cleanup_remote(self, job: TranscriptionJob) -> None:
        if job.kernel_ref:
            try:
                await remove_job(_JOB_TYPE, job.kernel_ref)
            except Exception:
                pass
        if not self.config.keep_kaggle_datasets:
            for dataset in (job.input_dataset_ref, job.key_dataset_ref):
                if dataset:
                    await self._delete_dataset_quietly(dataset)

    async def _delete_dataset_quietly(self, dataset_ref: str) -> None:
        try:
            await asyncio.to_thread(self.client.delete_dataset, dataset_ref)
        except Exception:
            pass
