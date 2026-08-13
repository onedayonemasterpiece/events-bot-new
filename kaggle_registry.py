from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
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


_INTENT_STATES = {"prepared", "indeterminate", "reconciling"}


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_records(data: dict[str, Any]) -> None:
    seen_jobs: set[str] = set()
    for index, job in enumerate(data["jobs"]):
        if not isinstance(job, dict):
            raise KaggleRegistryError(f"Kaggle registry jobs[{index}] is not an object")
        meta = job.get("meta")
        if not all(_nonempty(job.get(key)) for key in ("id", "type", "kernel_ref")):
            raise KaggleRegistryError(f"Kaggle registry jobs[{index}] identity is malformed")
        if not isinstance(meta, dict) or not _nonempty(meta.get("run_id")):
            raise KaggleRegistryError(f"Kaggle registry jobs[{index}] run_id is malformed")
        job_id = str(job["id"])
        if job_id in seen_jobs:
            raise KaggleRegistryError(f"Kaggle registry duplicate job id: {job_id}")
        seen_jobs.add(job_id)

    seen_intents: set[str] = set()
    for index, intent in enumerate(data["launch_intents"]):
        if not isinstance(intent, dict):
            raise KaggleRegistryError(
                f"Kaggle registry launch_intents[{index}] is not an object"
            )
        meta = intent.get("meta")
        if not all(_nonempty(intent.get(key)) for key in ("id", "type", "run_id")):
            raise KaggleRegistryError(
                f"Kaggle registry launch_intents[{index}] identity is malformed"
            )
        if intent.get("state") not in _INTENT_STATES:
            raise KaggleRegistryError(
                f"Kaggle registry launch_intents[{index}] state is malformed"
            )
        if not isinstance(meta, dict) or not _nonempty(meta.get("kernel_ref_hint")):
            raise KaggleRegistryError(
                f"Kaggle registry launch_intents[{index}] kernel_ref is malformed"
            )
        intent_id = str(intent["id"])
        if intent_id in seen_intents:
            raise KaggleRegistryError(f"Kaggle registry duplicate launch intent id: {intent_id}")
        seen_intents.add(intent_id)


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
    _validate_records(data)
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
    clean_meta = dict(meta or {})
    if not _nonempty(clean_meta.get("kernel_ref_hint")):
        raise KaggleRegistryError("Kaggle launch intent requires kernel_ref_hint")
    intent = {
        "id": intent_id,
        "type": job_type,
        "run_id": run_id,
        "state": "prepared",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "meta": clean_meta,
    }
    async with _LOCK:
        data = _load_registry()
        intents = list(data["launch_intents"])
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


async def mark_launch_intent_indeterminate(
    job_type: str,
    run_id: str,
    *,
    error: BaseException,
) -> None:
    """Keep an ambiguous post-submit handoff durable for exact reconciliation."""

    intent_id = f"{job_type}:{run_id}"
    async with _LOCK:
        data = _load_registry()
        found = False
        for intent in data["launch_intents"]:
            if intent.get("id") != intent_id:
                continue
            intent["state"] = "indeterminate"
            intent["last_error"] = f"{type(error).__name__}: {error}"[:500]
            intent["last_checked_at"] = datetime.now(timezone.utc).isoformat()
            found = True
            break
        if not found:
            raise KaggleRegistryError(f"Kaggle launch intent missing: {intent_id}")
        _save_registry(data)


async def list_launch_intents(job_type: str | None = None) -> list[dict[str, Any]]:
    async with _LOCK:
        data = _load_registry()
        intents = [item for item in data["launch_intents"] if isinstance(item, dict)]
    if job_type:
        return [item for item in intents if item.get("type") == job_type]
    return intents


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _is_not_found(exc: BaseException) -> bool:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None) == 404 or "404" in str(exc)


async def _discard_proven_unsubmitted_intent(
    *,
    client: Any,
    job_type: str,
    run_id: str,
    dataset_slugs: list[str],
) -> None:
    # Config datasets are unique per attempt. Delete them before the barrier;
    # if cleanup fails the intent remains and the watchdog stays fail-closed.
    for slug in dataset_slugs:
        try:
            await asyncio.to_thread(client.delete_dataset, slug)
        except Exception as exc:
            if not _is_not_found(exc):
                raise
    await remove_launch_intent(job_type, run_id)


async def reconcile_launch_intents(
    job_type: str,
    *,
    client: Any | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Resolve crash/timeout windows by exact remote dataset identity.

    An accepted push is promoted only when the current remote kernel metadata
    contains exactly the two unique config datasets from the durable intent.
    An old intent is discarded only when Kaggle proves the kernel did not
    advance after the intent was created (or the exact ref is absent).
    Transport/ambiguous evidence always leaves the barrier in place.
    """

    intents = await list_launch_intents(job_type)
    if not intents:
        return []
    if client is None:
        from video_announce.kaggle_client import KaggleClient

        client = KaggleClient()
    observed_now = now or datetime.now(timezone.utc)
    grace = max(
        60,
        int(os.getenv("KAGGLE_LAUNCH_INTENT_RECONCILE_GRACE_SECONDS", "900")),
    )
    outcomes: list[dict[str, Any]] = []
    for intent in intents:
        run_id = str(intent["run_id"])
        meta = dict(intent.get("meta") or {})
        kernel_ref = str(meta["kernel_ref_hint"]).strip()
        datasets = [
            str(item).strip()
            for item in (meta.get("dataset_slugs") or [])
            if str(item).strip()
        ]
        created_at = _parse_datetime(intent.get("created_at"))
        old_enough = bool(
            created_at and observed_now >= created_at + timedelta(seconds=grace)
        )
        try:
            _matched, remote_meta = await asyncio.to_thread(
                client.kernel_has_dataset_sources, kernel_ref, datasets
            )
        except Exception as exc:
            if old_enough and _is_not_found(exc):
                await _discard_proven_unsubmitted_intent(
                    client=client,
                    job_type=job_type,
                    run_id=run_id,
                    dataset_slugs=datasets,
                )
                outcomes.append({"run_id": run_id, "status": "not_submitted_404"})
            else:
                outcomes.append(
                    {
                        "run_id": run_id,
                        "status": "indeterminate",
                        "error": type(exc).__name__,
                    }
                )
            continue

        actual = {
            str(item).strip()
            for item in (remote_meta.get("dataset_sources") or [])
            if str(item).strip()
        }
        if datasets and actual == set(datasets):
            await promote_launch_intent(
                job_type,
                run_id,
                kernel_ref,
                meta={
                    **meta,
                    "intent_reconciled_at": observed_now.isoformat(),
                    "intent_reconciliation": "exact_dataset_sources",
                },
            )
            outcomes.append({"run_id": run_id, "status": "promoted"})
            continue

        if not old_enough:
            outcomes.append({"run_id": run_id, "status": "propagating"})
            continue

        # A successful list response can prove that the constant kernel never
        # advanced after this intent. A later run with different datasets is
        # ambiguous (for example an operator push) and must remain blocked.
        owner = kernel_ref.split("/", 1)[0] if "/" in kernel_ref else ""
        try:
            listed = await asyncio.to_thread(client.kernels_list, owner, 100)
        except Exception as exc:
            outcomes.append(
                {"run_id": run_id, "status": "indeterminate", "error": type(exc).__name__}
            )
            continue
        remote_row = next(
            (row for row in listed if str(row.get("ref") or "") == kernel_ref),
            None,
        )
        remote_started = _parse_datetime(
            remote_row.get("lastRunTime") if remote_row is not None else None
        )
        if remote_row is None or (
            created_at is not None
            and remote_started is not None
            and remote_started <= created_at
        ):
            await _discard_proven_unsubmitted_intent(
                client=client,
                job_type=job_type,
                run_id=run_id,
                dataset_slugs=datasets,
            )
            outcomes.append({"run_id": run_id, "status": "not_submitted"})
        else:
            outcomes.append({"run_id": run_id, "status": "indeterminate_remote_advanced"})
    return outcomes


def _job_id(job_type: str, kernel_ref: str) -> str:
    return f"{job_type}:{kernel_ref}" if kernel_ref else job_type


async def register_job(
    job_type: str,
    kernel_ref: str,
    *,
    meta: dict[str, Any] | None = None,
) -> None:
    clean_meta = dict(meta or {})
    if not _nonempty(clean_meta.get("run_id")):
        raise KaggleRegistryError("Kaggle recovery job requires meta.run_id")
    job = {
        "id": _job_id(job_type, kernel_ref),
        "type": job_type,
        "kernel_ref": kernel_ref,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "meta": clean_meta,
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


async def promote_launch_intent(
    job_type: str,
    run_id: str,
    kernel_ref: str,
    *,
    meta: dict[str, Any] | None = None,
) -> None:
    """Atomically convert one pre-push barrier into its recovery job."""

    intent_id = f"{job_type}:{run_id}"
    clean_kernel_ref = str(kernel_ref or "").strip()
    if not clean_kernel_ref:
        raise KaggleRegistryError("Kaggle launch intent promotion requires kernel_ref")
    async with _LOCK:
        data = _load_registry()
        intent = next(
            (item for item in data["launch_intents"] if item.get("id") == intent_id),
            None,
        )
        if intent is None:
            raise KaggleRegistryError(f"Kaggle launch intent missing: {intent_id}")
        merged_meta = dict(intent.get("meta") or {})
        merged_meta.update(dict(meta or {}))
        merged_meta["run_id"] = run_id
        job = {
            "id": _job_id(job_type, clean_kernel_ref),
            "type": job_type,
            "kernel_ref": clean_kernel_ref,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "meta": merged_meta,
        }
        data["jobs"] = [
            item for item in data["jobs"] if item.get("id") != job["id"]
        ] + [job]
        data["launch_intents"] = [
            item for item in data["launch_intents"] if item.get("id") != intent_id
        ]
        _save_registry(data)
        persisted = _load_registry()
        if any(item.get("id") == intent_id for item in persisted["launch_intents"]):
            raise KaggleRegistryError("Kaggle launch intent promotion did not clear intent")
        if not any(item.get("id") == job["id"] for item in persisted["jobs"]):
            raise KaggleRegistryError("Kaggle launch intent promotion did not persist job")


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
