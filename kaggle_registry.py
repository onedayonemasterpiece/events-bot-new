from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def _resolve_registry_path() -> Path:
    env = (os.getenv("KAGGLE_JOBS_PATH") or "").strip()
    if env:
        return Path(env)
    # In Fly production /data is a writable volume; in local/dev it may be missing
    # or protected. Fall back to artifacts to avoid PermissionError.
    if os.path.isdir("/data") and os.access("/data", os.W_OK):
        return Path("/data/kaggle_jobs.json")
    return Path("artifacts/run/kaggle_jobs.json")


_REGISTRY_PATH = _resolve_registry_path()
_LOCK = asyncio.Lock()


class KaggleRegistryError(RuntimeError):
    """The durable Kaggle handoff registry is unreadable or malformed."""


def _load_registry() -> dict[str, Any]:
    if not _REGISTRY_PATH.exists():
        return {"jobs": [], "launch_intents": []}
    try:
        raw = _REGISTRY_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception as exc:
        raise KaggleRegistryError(
            f"Kaggle registry cannot be read: {type(exc).__name__}"
        ) from exc
    if not isinstance(data, dict):
        raise KaggleRegistryError("Kaggle registry root is not an object")
    jobs = data.get("jobs")
    if jobs is None:
        data["jobs"] = []
    elif not isinstance(jobs, list):
        raise KaggleRegistryError("Kaggle registry jobs is not a list")
    intents = data.get("launch_intents")
    if intents is None:
        data["launch_intents"] = []
    elif not isinstance(intents, list):
        raise KaggleRegistryError("Kaggle registry launch_intents is not a list")
    return data


def _save_registry(data: dict[str, Any]) -> None:
    _REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _REGISTRY_PATH.with_suffix(".tmp")
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    with tmp_path.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    tmp_path.replace(_REGISTRY_PATH)
    directory_fd = os.open(_REGISTRY_PATH.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


async def register_launch_intent(
    job_type: str,
    run_id: str,
    *,
    meta: dict[str, Any] | None = None,
) -> None:
    """Persist a unique pre-push handoff barrier.

    A remote push must not happen unless this write and read-back succeed.  If
    the process dies after the push but before ``register_job``, the intent
    remains and watchdogs fail closed instead of launching a duplicate.
    """

    intent_id = f"{job_type}:{run_id}"
    intent = {
        "id": intent_id,
        "type": job_type,
        "run_id": run_id,
        "state": "prepared",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "meta": meta or {},
    }
    async with _LOCK:
        data = _load_registry()
        intents = [item for item in data["launch_intents"] if isinstance(item, dict)]
        intents = [item for item in intents if item.get("id") != intent_id]
        intents.append(intent)
        data["launch_intents"] = intents
        _save_registry(data)
        persisted = _load_registry()
        if not any(
            isinstance(item, dict) and item.get("id") == intent_id
            for item in persisted["launch_intents"]
        ):
            raise KaggleRegistryError("Kaggle launch intent read-back failed")


async def remove_launch_intent(job_type: str, run_id: str) -> None:
    intent_id = f"{job_type}:{run_id}"
    async with _LOCK:
        data = _load_registry()
        data["launch_intents"] = [
            item
            for item in data["launch_intents"]
            if not isinstance(item, dict) or item.get("id") != intent_id
        ]
        _save_registry(data)


async def list_launch_intents(job_type: str | None = None) -> list[dict[str, Any]]:
    async with _LOCK:
        data = _load_registry()
        intents = [item for item in data["launch_intents"] if isinstance(item, dict)]
    if job_type:
        return [item for item in intents if item.get("type") == job_type]
    return intents


def _job_id(job_type: str, kernel_ref: str) -> str:
    return f"{job_type}:{kernel_ref}" if kernel_ref else job_type


async def register_job(
    job_type: str,
    kernel_ref: str,
    *,
    meta: dict[str, Any] | None = None,
) -> None:
    job = {
        "id": _job_id(job_type, kernel_ref),
        "type": job_type,
        "kernel_ref": kernel_ref,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "meta": meta or {},
    }
    async with _LOCK:
        data = _load_registry()
        jobs = [j for j in data.get("jobs", []) if isinstance(j, dict)]
        job_ids = {j.get("id") for j in jobs}
        if job["id"] in job_ids:
            jobs = [j for j in jobs if j.get("id") != job["id"]]
        jobs.append(job)
        data["jobs"] = jobs
        _save_registry(data)


async def remove_job(job_type: str, kernel_ref: str) -> None:
    job_id = _job_id(job_type, kernel_ref)
    async with _LOCK:
        data = _load_registry()
        jobs = [j for j in data.get("jobs", []) if isinstance(j, dict)]
        jobs = [j for j in jobs if j.get("id") != job_id]
        data["jobs"] = jobs
        _save_registry(data)


async def update_job_meta(
    job_type: str,
    kernel_ref: str,
    *,
    meta_updates: dict[str, Any] | None = None,
    delete_keys: list[str] | tuple[str, ...] | set[str] | None = None,
) -> dict[str, Any] | None:
    job_id = _job_id(job_type, kernel_ref)
    async with _LOCK:
        data = _load_registry()
        jobs = [j for j in data.get("jobs", []) if isinstance(j, dict)]
        updated_job: dict[str, Any] | None = None
        for job in jobs:
            if job.get("id") != job_id:
                continue
            meta = job.get("meta")
            merged_meta = dict(meta) if isinstance(meta, dict) else {}
            if meta_updates:
                merged_meta.update(meta_updates)
            for key in delete_keys or ():
                merged_meta.pop(str(key), None)
            job["meta"] = merged_meta
            updated_job = dict(job)
            break
        if updated_job is None:
            return None
        data["jobs"] = jobs
        _save_registry(data)
        return updated_job


async def list_jobs(job_type: str | None = None) -> list[dict[str, Any]]:
    async with _LOCK:
        data = _load_registry()
        jobs = [j for j in data.get("jobs", []) if isinstance(j, dict)]
    if job_type:
        return [j for j in jobs if j.get("type") == job_type]
    return jobs
