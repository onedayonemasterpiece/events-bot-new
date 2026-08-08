"""Bind Search corpus evidence to an immutable static-site snapshot."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

from static_site_release import (
    StaticSitePermanentError,
    StaticSiteRetryableError,
    validate_vector_barrier,
)

MAX_RECEIPT_BYTES = 2_097_152


@lru_cache(maxsize=1)
def _load_exporter():
    path = (
        Path(__file__).resolve().parent
        / "site"
        / "scripts"
        / "export-production-preview-data.py"
    )
    spec = importlib.util.spec_from_file_location("static_site_exporter_runtime", path)
    if not spec or not spec.loader:
        raise StaticSitePermanentError("static_site_exporter_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(spec.name, module)
    spec.loader.exec_module(module)
    return module


def snapshot_search_catalog_revision(
    snapshot_path: str | os.PathLike[str],
    *,
    current_date: str,
    current_datetime: str,
) -> str:
    """Hash the exact full Search payload exported from one snapshot."""

    exporter = _load_exporter()
    connection = sqlite3.connect(
        f"file:{Path(snapshot_path).resolve()}?mode=ro", uri=True
    )
    connection.row_factory = sqlite3.Row
    try:
        effective_date, effective_time = exporter.split_current_datetime(
            current_datetime, current_date
        )
        rows = exporter.fetch_rows(
            connection,
            None,
            effective_date,
            [],
            current_time=effective_time,
            focus_date_from="",
            focus_date_to="",
        )
        event_ids = [int(row["id"]) for row in rows]
        participants = exporter.event_participants_for_events(connection, event_ids)
        videos = exporter.event_video_assets_for_events(connection, event_ids)
        events = [
            exporter.build_event(
                connection,
                row,
                effective_date,
                participants=participants.get(int(row["id"]), []),
                video_assets=videos.get(int(row["id"]), []),
            )
            for row in rows
        ]
        exporter.normalize_linked_occurrences(events)
        return str(exporter.exported_search_catalog_revision(events))
    finally:
        connection.close()


def freeze_search_receipt(
    source_path: str | os.PathLike[str],
    snapshot_path: str | os.PathLike[str],
) -> Path:
    """Copy one bounded receipt read to a snapshot-scoped immutable path."""

    source = Path(source_path)
    target = Path(snapshot_path).with_suffix(".search-receipt.json")
    if target.exists():
        raise StaticSitePermanentError("static_site_search_receipt_snapshot_exists")
    try:
        with source.open("rb") as handle:
            raw = handle.read(MAX_RECEIPT_BYTES + 1)
    except OSError as exc:
        raise StaticSiteRetryableError("vector_barrier_receipt_pending") from exc
    if not raw or len(raw) > MAX_RECEIPT_BYTES:
        raise StaticSitePermanentError("static_site_search_receipt_size_invalid")
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise StaticSitePermanentError("static_site_search_receipt_invalid") from exc
    if not isinstance(payload, Mapping):
        raise StaticSitePermanentError("static_site_search_receipt_invalid")
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        target.unlink(missing_ok=True)
        raise
    return target


def bind_snapshot_search_receipt(
    *,
    request_payload: Mapping[str, Any],
    source_receipt_path: str | os.PathLike[str],
    snapshot_path: str | os.PathLike[str],
    current_date: str,
    current_datetime: str,
) -> tuple[Path, dict[str, Any]]:
    """Freeze and verify one receipt before spending a remote Kaggle run."""

    frozen_path = freeze_search_receipt(source_receipt_path, snapshot_path)
    try:
        evidence = validate_vector_barrier(request_payload, frozen_path)
        snapshot_revision = snapshot_search_catalog_revision(
            snapshot_path,
            current_date=current_date,
            current_datetime=current_datetime,
        )
        if evidence.get("catalog_revision") != snapshot_revision:
            raise StaticSiteRetryableError("vector_barrier_catalog_revision_pending")
        evidence = dict(evidence)
        evidence["snapshot_catalog_revision"] = snapshot_revision
        evidence["receipt_sha256"] = hashlib.sha256(frozen_path.read_bytes()).hexdigest()
        return frozen_path, evidence
    except Exception:
        frozen_path.unlink(missing_ok=True)
        raise
