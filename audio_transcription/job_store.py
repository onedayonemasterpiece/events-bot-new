from __future__ import annotations

import json
import re
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contracts import JobState

_JOB_REF_RE = re.compile(r"^atr_[A-Za-z0-9_-]{24,160}$")
_OWNER_RE = re.compile(r"^[0-9a-f]{64}$")
_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


class JobStoreError(RuntimeError):
    pass


class JobNotFound(JobStoreError):
    pass


class JobOwnershipError(JobStoreError):
    pass


@dataclass(frozen=True, slots=True)
class TranscriptionJob:
    job_ref: str
    owner_binding: str
    idempotency_key: str
    asset_ref: str
    state: JobState
    request: dict[str, Any]
    created_at: str
    updated_at: str
    kernel_ref: str | None = None
    input_dataset_ref: str | None = None
    key_dataset_ref: str | None = None
    result_dir: str | None = None
    error_code: str | None = None
    error_detail: str | None = None
    progress: dict[str, Any] | None = None

    def public_dict(self) -> dict[str, Any]:
        return {
            "job_ref": self.job_ref,
            "state": self.state.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "progress": dict(self.progress or {}),
            "error_code": self.error_code,
            "complete": self.state is JobState.COMPLETE,
            "terminal": self.state.terminal,
        }


class AudioJobStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("audio job store path must be absolute")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialise()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def _initialise(self) -> None:
        with self._lock, self._db() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS audio_transcription_job (
                    job_ref TEXT PRIMARY KEY,
                    owner_binding TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    asset_ref TEXT NOT NULL,
                    state TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    kernel_ref TEXT,
                    input_dataset_ref TEXT,
                    key_dataset_ref TEXT,
                    result_dir TEXT,
                    error_code TEXT,
                    error_detail TEXT,
                    progress_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(owner_binding, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS audio_transcription_job_state
                ON audio_transcription_job(state, updated_at);
                """
            )
            db.commit()

    def create(
        self,
        *,
        owner_binding: str,
        idempotency_key: str,
        asset_ref: str,
        request: dict[str, Any],
    ) -> tuple[TranscriptionJob, bool]:
        if not _OWNER_RE.fullmatch(owner_binding):
            raise ValueError("invalid owner binding")
        if not _IDEMPOTENCY_RE.fullmatch(idempotency_key):
            raise ValueError("invalid idempotency key")
        now = self._now()
        with self._lock, self._db() as db:
            existing = db.execute(
                """
                SELECT * FROM audio_transcription_job
                WHERE owner_binding=? AND idempotency_key=?
                """,
                (owner_binding, idempotency_key),
            ).fetchone()
            if existing is not None:
                return self._from_row(existing), False
            job_ref = "atr_" + secrets.token_urlsafe(32)
            db.execute(
                """
                INSERT INTO audio_transcription_job(
                    job_ref, owner_binding, idempotency_key, asset_ref, state,
                    request_json, progress_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, '{}', ?, ?)
                """,
                (
                    job_ref,
                    owner_binding,
                    idempotency_key,
                    asset_ref,
                    JobState.QUEUED.value,
                    json.dumps(request, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            db.commit()
            row = db.execute(
                "SELECT * FROM audio_transcription_job WHERE job_ref=?", (job_ref,)
            ).fetchone()
            assert row is not None
            return self._from_row(row), True

    def get(self, job_ref: str, *, owner_binding: str) -> TranscriptionJob:
        if not _JOB_REF_RE.fullmatch(str(job_ref or "")):
            raise JobNotFound("unknown transcription job")
        with self._lock, self._db() as db:
            row = db.execute(
                "SELECT * FROM audio_transcription_job WHERE job_ref=?", (job_ref,)
            ).fetchone()
        if row is None:
            raise JobNotFound("unknown transcription job")
        job = self._from_row(row)
        if job.owner_binding != owner_binding:
            raise JobOwnershipError("transcription job belongs to another principal")
        return job

    def find_by_idempotency(
        self, *, owner_binding: str, idempotency_key: str
    ) -> TranscriptionJob | None:
        if not _OWNER_RE.fullmatch(owner_binding) or not _IDEMPOTENCY_RE.fullmatch(
            idempotency_key
        ):
            raise ValueError("invalid transcription lookup binding")
        with self._lock, self._db() as db:
            row = db.execute(
                """SELECT * FROM audio_transcription_job
                   WHERE owner_binding=? AND idempotency_key=?""",
                (owner_binding, idempotency_key),
            ).fetchone()
        return self._from_row(row) if row is not None else None

    def update(
        self,
        job_ref: str,
        *,
        state: JobState | None = None,
        kernel_ref: str | None = None,
        input_dataset_ref: str | None = None,
        key_dataset_ref: str | None = None,
        result_dir: str | None = None,
        error_code: str | None = None,
        error_detail: str | None = None,
        progress: dict[str, Any] | None = None,
    ) -> TranscriptionJob:
        fields: dict[str, Any] = {"updated_at": self._now()}
        if state is not None:
            fields["state"] = JobState(state).value
        for key, value in (
            ("kernel_ref", kernel_ref),
            ("input_dataset_ref", input_dataset_ref),
            ("key_dataset_ref", key_dataset_ref),
            ("result_dir", result_dir),
            ("error_code", error_code),
            ("error_detail", error_detail),
        ):
            if value is not None:
                fields[key] = value
        if progress is not None:
            fields["progress_json"] = json.dumps(progress, ensure_ascii=False, sort_keys=True)
        assignments = ", ".join(f"{key}=?" for key in fields)
        values = [*fields.values(), job_ref]
        with self._lock, self._db() as db:
            cursor = db.execute(
                f"UPDATE audio_transcription_job SET {assignments} WHERE job_ref=?",
                values,
            )
            if cursor.rowcount != 1:
                raise JobNotFound("unknown transcription job")
            db.commit()
            row = db.execute(
                "SELECT * FROM audio_transcription_job WHERE job_ref=?", (job_ref,)
            ).fetchone()
            assert row is not None
            return self._from_row(row)

    def active_jobs(self) -> tuple[TranscriptionJob, ...]:
        terminal = tuple(state.value for state in JobState if state.terminal)
        placeholders = ",".join("?" for _ in terminal)
        with self._lock, self._db() as db:
            rows = db.execute(
                "SELECT * FROM audio_transcription_job "
                f"WHERE state NOT IN ({placeholders}) ORDER BY created_at",
                terminal,
            ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    @staticmethod
    def _from_row(row: sqlite3.Row) -> TranscriptionJob:
        try:
            request = json.loads(row["request_json"] or "{}")
        except json.JSONDecodeError:
            request = {}
        try:
            progress = json.loads(row["progress_json"] or "{}")
        except json.JSONDecodeError:
            progress = {}
        return TranscriptionJob(
            job_ref=str(row["job_ref"]),
            owner_binding=str(row["owner_binding"]),
            idempotency_key=str(row["idempotency_key"]),
            asset_ref=str(row["asset_ref"]),
            state=JobState(str(row["state"])),
            request=request if isinstance(request, dict) else {},
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            kernel_ref=(str(row["kernel_ref"]) if row["kernel_ref"] else None),
            input_dataset_ref=(
                str(row["input_dataset_ref"]) if row["input_dataset_ref"] else None
            ),
            key_dataset_ref=(str(row["key_dataset_ref"]) if row["key_dataset_ref"] else None),
            result_dir=(str(row["result_dir"]) if row["result_dir"] else None),
            error_code=(str(row["error_code"]) if row["error_code"] else None),
            error_detail=(str(row["error_detail"]) if row["error_detail"] else None),
            progress=progress if isinstance(progress, dict) else {},
        )
