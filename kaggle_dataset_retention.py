"""Bound private, disposable Kaggle inputs without touching backups or live work.

The provider catalog is the retry queue: failed deletions are rediscovered on
the next pass. No new database or notebook execution is needed for retention.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)
PREFIXES = ("static-site-builder-input-", "cherryflash-session-")
TERMINAL = {"done", "complete", "failed", "error", "cancelled", "canceled"}


def dataset_ref(value: str) -> str:
    """Normalize an optional pinned dataset version, never a foreign URL."""
    match = re.fullmatch(r"([A-Za-z0-9_-]+/[A-Za-z0-9_-]+)(?:/[0-9]+)?", str(value))
    return match[1] if match else ""


def is_disposable(ref: str, owner: str) -> bool:
    normalized = dataset_ref(ref)
    return normalized == ref and normalized.startswith(owner + "/") and normalized.split("/", 1)[1].startswith(PREFIXES)


def _dict(value) -> dict:
    return value if isinstance(value, dict) else value.to_dict()


class KaggleRetentionStore:
    def __init__(self, api):
        self.api = api
        self.deadline = time.monotonic() + 720
        self.redacted_notebook_rows = 0

    def _call(self, function, *args, **kwargs):
        for attempt in range(4):
            # Keep maintenance below the provider's API request budget. A 429
            # proves rejection; network/unknown mutation outcomes are not retried.
            if time.monotonic() >= self.deadline:
                raise TimeoutError("Kaggle retention pass exhausted its API time budget")
            time.sleep(1)
            try:
                return function(*args, **kwargs)
            except Exception as exc:
                response = getattr(exc, "response", None)
                if getattr(response, "status_code", None) != 429 or attempt == 3:
                    raise
                time.sleep(20 * (attempt + 1))

    def list_inputs(self) -> list[dict]:
        records = {}
        for prefix in PREFIXES:
            seen = set()
            for page in range(1, 1001):
                items = self._call(self.api.dataset_list, mine=True, search=prefix, page=page) or []
                if not items:
                    break
                refs = set()
                for item in items:
                    row = _dict(item)
                    ref = row.get("ref", "")
                    refs.add(ref)
                    records[ref] = row
                if not refs - seen:
                    raise RuntimeError("Kaggle dataset pagination did not advance")
                seen.update(refs)
            else:
                raise RuntimeError("Kaggle dataset pagination was truncated")
        return list(records.values())

    def protected_notebook_inputs(self) -> set[str]:
        """Read metadata only; never download notebook code or credential files."""
        from kaggle.api.kaggle_api_extended import ApiGetKernelRequest

        refs = set()
        for page in range(1, 1001):
            items = self._call(self.api.kernels_list, mine=True, page=page, page_size=100) or []
            if not items:
                break
            next_refs = set()
            for item in items:
                metadata = _dict(item)
                if metadata.get("ref"):
                    next_refs.add(metadata["ref"])
                elif metadata == {"title": "[Private Notebook]", "lastRunTime": "2010-04-01T00:00:00.000Z"}:
                    # Kaggle emits opaque placeholders with no addressable
                    # identity. Never invent a ref or treat them as an idle run.
                    # Applying such a scan additionally requires the canonical
                    # project ledger below, including unknown/recovery owners.
                    self.redacted_notebook_rows += 1
                else:
                    raise RuntimeError("Kaggle notebook identity is missing")
            if not next_refs - refs:
                raise RuntimeError("Kaggle notebook pagination did not advance")
            refs.update(next_refs)
        else:
            raise RuntimeError("Kaggle notebook pagination was truncated")
        protected = set()
        for ref in sorted(refs):
            request = ApiGetKernelRequest()
            request.user_name, request.kernel_slug = ref.split("/", 1)
            with self.api.build_kaggle_client() as client:
                response = self._call(client.kernels.kernels_api_client.get_kernel, request)
            metadata = _dict(response.metadata)
            if metadata.get("ref") != ref:
                raise RuntimeError("Kaggle notebook metadata identity mismatch")
            protected.update(
                dataset_ref(item) for item in metadata.get("datasetDataSources") or []
                if dataset_ref(item)
            )
        return protected

    def delete(self, ref: str) -> None:
        owner, slug = ref.split("/", 1)
        # The SDK returns False if the requested mutation was not performed.
        if self._call(self.api.dataset_delete, owner, slug, no_confirm=True) is False:
            raise RuntimeError("Kaggle did not acknowledge dataset deletion")


def protected_local_inputs(database_path: str | Path | None) -> set[str]:
    if database_path is None:
        return set()
    # Never create an empty database and mistake it for an idle production DB.
    uri = Path(database_path).resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=10) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "kaggle_run_ledger" not in tables:
            raise RuntimeError("Kaggle ledger unavailable; retention fails closed")
        protected = {
            dataset_ref(row[0]) for row in conn.execute(
                "SELECT dataset_ref FROM kaggle_run_ledger WHERE terminal_at IS NULL "
                "OR lower(status) NOT IN ('done','complete','failed','error','cancelled','canceled')"
            ) if dataset_ref(row[0])
        }
        if "kaggle_resource_lease" in tables:
            protected.update(dataset_ref(row[0]) for row in conn.execute(
                "SELECT r.dataset_ref FROM kaggle_run_ledger r JOIN kaggle_resource_lease l "
                "ON l.run_id=r.run_id WHERE l.status='active'"
            ) if dataset_ref(row[0]))
        if "static_site_build_state" in tables:
            for run_id, job_id in conn.execute(
                "SELECT active_run_id, active_job_id FROM static_site_build_state "
                "WHERE active_run_id IS NOT NULL OR active_job_id IS NOT NULL"
            ):
                run_refs = {dataset_ref(row[0]) for row in conn.execute(
                    "SELECT dataset_ref FROM kaggle_run_ledger WHERE run_id=?", (run_id,)
                ) if dataset_ref(row[0])}
                protected.update(run_refs)
                if job_id:
                    row = conn.execute("SELECT payload FROM joboutbox WHERE id=?", (job_id,)).fetchone()
                    if not row or not row[0]:
                        raise RuntimeError("Active static-site handoff is unreadable")
                    payload = json.loads(row[0])
                    if not isinstance(payload, dict):
                        raise RuntimeError("Active static-site handoff is malformed")
                    # Include the exact recoverable handoff even if a ledger
                    # projection is temporarily incomplete after a restart.
                    payload_refs = set()
                    def collect(value):
                        if isinstance(value, dict):
                            for item in value.values():
                                collect(item)
                        elif isinstance(value, list):
                            for item in value:
                                collect(item)
                        elif isinstance(value, str) and dataset_ref(value):
                            payload_refs.add(dataset_ref(value))
                    collect(payload)
                    protected.update(payload_refs)
                    if not run_refs and not any(ref.split('/')[1].startswith("static-site-builder-input-") for ref in payload_refs):
                        raise RuntimeError("Active static-site dataset identity is unknown")
                elif not run_refs:
                    raise RuntimeError("Active static-site dataset identity is unknown")
        if "videoannounce_session" in tables:
            protected.update(dataset_ref(row[0]) for row in conn.execute(
                "SELECT kaggle_dataset FROM videoannounce_session WHERE status IN "
                "('CREATED','SELECTED','RENDERING','PUBLISH_BLOCKED')"
            ) if dataset_ref(row[0]))
        return protected


def plan_cleanup(records: list[dict], *, owner: str, protected: set[str],
                 min_age_hours: int = 24, now: datetime | None = None) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", owner):
        raise ValueError("An exact Kaggle owner is required")
    if min_age_hours < 24:
        raise ValueError("Disposable input retention cannot be shorter than 24 hours")
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(hours=min_age_hours)
    candidates, kept = [], []
    for row in records:
        ref = row.get("ref", "")
        if not is_disposable(ref, owner) or row.get("isPrivate") is not True:
            continue
        reason = None
        if ref in protected:
            reason = "current_notebook_or_recoverable_run"
        elif row.get("currentVersionNumber") != 1:
            reason = "versioned_or_unknown_identity"
        else:
            try:
                updated = datetime.fromisoformat(row["lastUpdated"].replace("Z", "+00:00"))
                if updated.tzinfo is None:
                    raise ValueError("missing timezone")
                if updated > cutoff:
                    reason = "within_retention"
            except (KeyError, TypeError, ValueError):
                reason = "unknown_age"
        item = {"ref": ref, "last_updated": row.get("lastUpdated"),
                "catalog_bytes": row.get("totalBytes", 0)}
        if reason:
            kept.append({**item, "reason": reason})
        else:
            candidates.append(item)
    return {"owner": owner, "min_age_hours": min_age_hours,
            "candidates": sorted(candidates, key=lambda item: item["ref"]), "kept": kept}


def cleanup_inputs(store, *, owner: str, database_path=None, apply: bool = False,
                   min_age_hours: int = 24, limit: int = 100, record=None,
                   approved_refs: set[str] | None = None) -> dict:
    protected = store.protected_notebook_inputs() | protected_local_inputs(database_path)
    redacted_rows = getattr(store, "redacted_notebook_rows", 0)
    if redacted_rows and database_path is None:
        raise RuntimeError("Opaque Kaggle notebook rows require canonical project ownership state")
    records = store.list_inputs()
    plan = plan_cleanup(records, owner=owner, protected=protected, min_age_hours=min_age_hours)
    if approved_refs is not None:
        plan["candidates"] = [row for row in plan["candidates"] if row["ref"] in approved_refs]
    result = {**plan, "apply": apply, "redacted_notebook_rows": redacted_rows,
              "deleted": [], "failed": [], "remaining": []}
    if record:
        record({"event": "plan", **plan})
    if not apply:
        return result
    for row in plan["candidates"][:max(0, limit)]:
        ref = row["ref"]
        # Recheck local ownership before every mutation, not just at scan time.
        if ref in protected_local_inputs(database_path):
            result["kept"].append({**row, "reason": "new_recoverable_run"})
            continue
        if record:
            record({"event": "delete_requested", "ref": ref})
        try:
            store.delete(ref)
        except Exception as exc:
            result["failed"].append(ref)
            logger.warning("kaggle_retention delete_failed dataset=%s error_type=%s", ref, type(exc).__name__)
            if record:
                record({"event": "delete_failed_or_unknown", "ref": ref, "error_type": type(exc).__name__})
        else:
            result["deleted"].append(ref)
            logger.info("kaggle_retention delete_acknowledged dataset=%s", ref)
            if record:
                record({"event": "delete_acknowledged", "ref": ref})
    # A successful provider response alone is insufficient for the report.
    # Rediscover before retrying an unknown outcome or declaring space freed.
    remaining = {row.get("ref") for row in store.list_inputs()}
    result["remaining"] = sorted(remaining & {row["ref"] for row in plan["candidates"]})
    result["verified_absent"] = sorted({row["ref"] for row in plan["candidates"]} - remaining)
    if record:
        record({"event": "readback", "verified_absent": result["verified_absent"], "remaining": result["remaining"]})
    logger.info("kaggle_retention complete candidates=%s acknowledged=%s absent=%s remaining=%s failed=%s",
                len(plan["candidates"]), len(result["deleted"]), len(result["verified_absent"]),
                len(result["remaining"]), len(result["failed"]))
    return result


async def cleanup_kaggle_inputs(db) -> dict:
    """Existing scheduler owns this bounded retry pass; never block its loop."""
    from video_announce.kaggle_client import KaggleClient

    owner = (os.getenv("KAGGLE_USERNAME") or "").strip()
    if not owner:
        return {"status": "disabled", "reason": "no_kaggle_owner"}
    client = KaggleClient()
    def run():
        return cleanup_inputs(KaggleRetentionStore(client._get_api()), owner=owner,
                              database_path=db.path, apply=True)
    return await asyncio.to_thread(run)
