from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from db import Database

logger = logging.getLogger(__name__)

_MIB = 1024 * 1024


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, min(value, maximum))


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class GuideMediaRetentionPolicy:
    enabled: bool = True
    retention_days: int = 14
    recent_post_grace_days: int = 14
    max_total_bytes: int = 384 * _MIB
    min_free_bytes: int = 256 * _MIB
    max_delete_files: int = 500
    max_delete_bytes: int = 512 * _MIB
    preview_carousel_retention_hours: int = 24
    published_carousel_retention_hours: int = 7 * 24
    local_timezone: str = "Europe/Kaliningrad"

    @classmethod
    def from_env(cls) -> "GuideMediaRetentionPolicy":
        return cls(
            enabled=_env_bool("GUIDE_MEDIA_RETENTION_ENABLED", True),
            retention_days=_env_int(
                "GUIDE_MEDIA_RETENTION_DAYS", 14, minimum=1, maximum=365
            ),
            recent_post_grace_days=_env_int(
                "GUIDE_MEDIA_RECENT_POST_GRACE_DAYS", 14, minimum=1, maximum=90
            ),
            max_total_bytes=_env_int(
                "GUIDE_MEDIA_RETENTION_MAX_TOTAL_BYTES",
                384 * _MIB,
                minimum=32 * _MIB,
                maximum=16 * 1024 * _MIB,
            ),
            min_free_bytes=_env_int(
                "GUIDE_MEDIA_RETENTION_MIN_FREE_BYTES",
                256 * _MIB,
                minimum=16 * _MIB,
                maximum=16 * 1024 * _MIB,
            ),
            max_delete_files=_env_int(
                "GUIDE_MEDIA_RETENTION_MAX_DELETE_FILES", 500, minimum=1, maximum=10_000
            ),
            max_delete_bytes=_env_int(
                "GUIDE_MEDIA_RETENTION_MAX_DELETE_BYTES",
                512 * _MIB,
                minimum=16 * _MIB,
                maximum=16 * 1024 * _MIB,
            ),
            preview_carousel_retention_hours=_env_int(
                "GUIDE_MEDIA_CAROUSEL_PREVIEW_RETENTION_HOURS",
                24,
                minimum=1,
                maximum=30 * 24,
            ),
            published_carousel_retention_hours=_env_int(
                "GUIDE_MEDIA_CAROUSEL_PUBLISHED_RETENTION_HOURS",
                7 * 24,
                minimum=1,
                maximum=90 * 24,
            ),
            local_timezone=(
                (os.getenv("GUIDE_MEDIA_RETENTION_TZ") or "Europe/Kaliningrad").strip()
                or "Europe/Kaliningrad"
            ),
        )


@dataclass(slots=True)
class GuideMediaRetentionResult:
    reason: str
    mode: str
    root: str
    enabled: bool
    scanned_files: int = 0
    scanned_bytes: int = 0
    protected_paths: int = 0
    protected_existing_files: int = 0
    recent_post_paths: int = 0
    current_occurrence_paths: int = 0
    current_digest_paths: int = 0
    candidate_files: int = 0
    candidate_bytes: int = 0
    planned_delete_files: int = 0
    planned_delete_bytes: int = 0
    deleted_files: int = 0
    deleted_bytes: int = 0
    stale_db_paths: int = 0
    planned_db_rows_repaired: int = 0
    db_rows_repaired: int = 0
    db_assets_removed: int = 0
    db_refs_removed: int = 0
    empty_dirs_removed: int = 0
    skipped_non_regular: int = 0
    skipped_too_recent: int = 0
    ignored_reference_paths: int = 0
    free_bytes_before: int = 0
    free_bytes_after: int = 0
    total_bytes_after: int = 0
    max_total_bytes: int = 0
    min_free_bytes: int = 0
    policy_satisfied: bool = True
    bounded: bool = False
    sample_planned_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _StoredFile:
    path: Path
    key: str
    size: int
    mtime: float


@dataclass(slots=True)
class _DbMediaState:
    post_rows: list[dict[str, Any]]
    protected_current_occurrence: set[str]
    protected_recent_post: set[str]
    protected_current_digest: set[str]
    issue_rows: list[dict[str, Any]]
    ignored_reference_paths: int


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _path_key(raw_path: Any, root: Path) -> str | None:
    text = str(raw_path or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = root / path
    # Deliberately do not resolve symlinks. The inventory never follows or
    # removes them, and lexical containment keeps an outside target harmless.
    normalized = Path(os.path.abspath(os.path.normpath(str(path))))
    try:
        normalized.relative_to(root)
    except ValueError:
        return None
    return str(normalized)


def _asset_path_keys(value: Any, root: Path) -> tuple[set[str], int]:
    keys: set[str] = set()
    ignored = 0
    for item in _json_list(value):
        if not isinstance(item, Mapping):
            continue
        asset: Any = item.get("media_asset") if isinstance(item.get("media_asset"), Mapping) else item
        if not isinstance(asset, Mapping) or not str(asset.get("path") or "").strip():
            continue
        key = _path_key(asset.get("path"), root)
        if key is None:
            ignored += 1
        else:
            keys.add(key)
    return keys, ignored


def _local_today(now: datetime, timezone_name: str) -> str:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("UTC")
    aware = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    return aware.astimezone(tz).date().isoformat()


async def _table_columns(conn: Any, table: str) -> set[str]:
    cur = await conn.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    return {str(row[1]) for row in rows}


def _current_digest_issue_ids(rows: Sequence[Mapping[str, Any]]) -> set[int]:
    current: set[int] = set()
    latest_published: dict[str, tuple[str, int]] = {}
    for row in rows:
        issue_id = int(row.get("id") or 0)
        if issue_id <= 0:
            continue
        status = str(row.get("status") or "").strip().lower()
        if status in {"preview", "partial"}:
            current.add(issue_id)
        if status != "published":
            continue
        family = str(row.get("family") or "").strip() or "unknown"
        order_key = str(row.get("published_at") or row.get("created_at") or "")
        previous = latest_published.get(family)
        if previous is None or (order_key, issue_id) > previous:
            latest_published[family] = (order_key, issue_id)
    current.update(issue_id for _order_key, issue_id in latest_published.values())
    return current


async def _load_db_media_state(
    db: Database,
    *,
    root: Path,
    today_iso: str,
    recent_cutoff: datetime,
) -> _DbMediaState:
    async with db.raw_conn() as conn:
        conn.row_factory = sqlite3.Row
        post_columns = await _table_columns(conn, "guide_monitor_post")
        occurrence_columns = await _table_columns(conn, "guide_occurrence")
        source_columns = await _table_columns(conn, "guide_occurrence_source")
        issue_columns = await _table_columns(conn, "guide_digest_issue")

        post_rows: list[dict[str, Any]] = []
        if {"id", "media_assets_json", "media_refs_json"}.issubset(post_columns):
            select_post_date = "post_date" if "post_date" in post_columns else "NULL AS post_date"
            cur = await conn.execute(
                f"SELECT id, media_assets_json, media_refs_json, {select_post_date} FROM guide_monitor_post"
            )
            post_rows = [dict(row) for row in await cur.fetchall()]

        current_post_ids: set[int] = set()
        if (
            {"id", "date"}.issubset(occurrence_columns)
            and {"occurrence_id", "post_id"}.issubset(source_columns)
        ):
            cur = await conn.execute(
                """
                SELECT DISTINCT gos.post_id
                FROM guide_occurrence_source gos
                JOIN guide_occurrence go ON go.id=gos.occurrence_id
                WHERE go.date IS NOT NULL AND date(go.date) >= date(?)
                """,
                (today_iso,),
            )
            current_post_ids = {int(row[0]) for row in await cur.fetchall()}

        issue_rows: list[dict[str, Any]] = []
        if {"id", "family", "status", "media_items_json"}.issubset(issue_columns):
            optional = [
                name if name in issue_columns else f"NULL AS {name}"
                for name in ("created_at", "published_at")
            ]
            cur = await conn.execute(
                "SELECT id, family, status, media_items_json, " + ", ".join(optional) + " FROM guide_digest_issue"
            )
            issue_rows = [dict(row) for row in await cur.fetchall()]

    current_issue_ids = _current_digest_issue_ids(issue_rows)
    current_occurrence_paths: set[str] = set()
    recent_post_paths: set[str] = set()
    current_digest_paths: set[str] = set()
    ignored = 0
    recent_cutoff_text = recent_cutoff.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for row in post_rows:
        keys, ignored_count = _asset_path_keys(row.get("media_assets_json"), root)
        ignored += ignored_count
        if int(row.get("id") or 0) in current_post_ids:
            current_occurrence_paths.update(keys)
        post_date = str(row.get("post_date") or "").strip().replace("T", " ")[:19]
        if post_date and post_date >= recent_cutoff_text:
            recent_post_paths.update(keys)
    for row in issue_rows:
        if int(row.get("id") or 0) not in current_issue_ids:
            continue
        keys, ignored_count = _asset_path_keys(row.get("media_items_json"), root)
        ignored += ignored_count
        current_digest_paths.update(keys)
    return _DbMediaState(
        post_rows=post_rows,
        protected_current_occurrence=current_occurrence_paths,
        protected_recent_post=recent_post_paths,
        protected_current_digest=current_digest_paths,
        issue_rows=issue_rows,
        ignored_reference_paths=ignored,
    )


def _inventory(root: Path, result: GuideMediaRetentionResult) -> list[_StoredFile]:
    files: list[_StoredFile] = []
    if not root.exists():
        return files
    for current_root, dir_names, file_names in os.walk(root, topdown=True, followlinks=False):
        safe_dirs: list[str] = []
        for name in dir_names:
            path = Path(current_root) / name
            try:
                if path.is_symlink():
                    result.skipped_non_regular += 1
                else:
                    safe_dirs.append(name)
            except OSError as exc:
                result.errors.append(f"inventory_dir:{path}:{type(exc).__name__}")
        dir_names[:] = safe_dirs
        for name in file_names:
            path = Path(current_root) / name
            try:
                if path.is_symlink() or not path.is_file():
                    result.skipped_non_regular += 1
                    continue
                stat = path.stat(follow_symlinks=False)
            except OSError as exc:
                result.errors.append(f"inventory_file:{path}:{type(exc).__name__}")
                continue
            files.append(
                _StoredFile(
                    path=path,
                    key=str(Path(os.path.abspath(str(path)))),
                    size=int(stat.st_size),
                    mtime=float(stat.st_mtime),
                )
            )
    return files


def _issue_by_id(rows: Sequence[Mapping[str, Any]]) -> dict[int, Mapping[str, Any]]:
    return {int(row.get("id") or 0): row for row in rows if int(row.get("id") or 0) > 0}


def _carousel_expiry_seconds(
    stored: _StoredFile,
    *,
    root: Path,
    issues: Mapping[int, Mapping[str, Any]],
    policy: GuideMediaRetentionPolicy,
) -> int | None:
    try:
        relative = stored.path.relative_to(root)
    except ValueError:
        return None
    parts = relative.parts
    if len(parts) < 3 or parts[0] != "_digest_carousel":
        return None
    try:
        issue_id = int(parts[1])
    except ValueError:
        return policy.retention_days * 86400
    issue = issues.get(issue_id)
    status = str((issue or {}).get("status") or "").strip().lower()
    if status in {"preview", "partial"}:
        return policy.preview_carousel_retention_hours * 3600
    if status == "published":
        return policy.published_carousel_retention_hours * 3600
    return policy.retention_days * 86400


def _stale_db_path_keys(post_rows: Sequence[Mapping[str, Any]], root: Path) -> set[str]:
    stale: set[str] = set()
    for row in post_rows:
        keys, _ignored = _asset_path_keys(row.get("media_assets_json"), root)
        for key in keys:
            # lexists preserves unknown/broken symlink paths: retention must not
            # delete or rewrite non-regular filesystem entries.
            if not os.path.lexists(key):
                stale.add(key)
    return stale


def _healed_post_payloads(
    row: Mapping[str, Any],
    *,
    root: Path,
    stale_keys: set[str],
) -> tuple[list[Any], list[Any], int, int] | None:
    assets = _json_list(row.get("media_assets_json"))
    refs = _json_list(row.get("media_refs_json"))
    if not assets:
        return None
    new_assets: list[Any] = []
    removed_asset_indexes: set[int] = set()
    for idx, asset in enumerate(assets):
        raw_path = asset.get("path") if isinstance(asset, Mapping) else None
        key = _path_key(raw_path, root) if raw_path else None
        if key is not None and key in stale_keys:
            removed_asset_indexes.add(idx)
            continue
        new_assets.append(asset)
    if not removed_asset_indexes:
        return None
    # media_refs and media_assets are position-aligned by the import path. Drop
    # only the corresponding refs; preserve any extra/non-materialized refs.
    new_refs = [item for idx, item in enumerate(refs) if idx not in removed_asset_indexes]
    refs_removed = len(refs) - len(new_refs)
    return new_assets, new_refs, len(removed_asset_indexes), refs_removed


async def _repair_stale_db_paths(
    db: Database,
    *,
    root: Path,
    post_rows: Sequence[Mapping[str, Any]],
    stale_keys: set[str],
    apply: bool,
) -> tuple[int, int, int]:
    repairs: list[tuple[str, str, int, int, int]] = []
    for row in post_rows:
        healed = _healed_post_payloads(row, root=root, stale_keys=stale_keys)
        if healed is None:
            continue
        assets, refs, assets_removed, refs_removed = healed
        repairs.append(
            (
                json.dumps(refs, ensure_ascii=False, separators=(",", ":")),
                json.dumps(assets, ensure_ascii=False, separators=(",", ":")),
                int(row.get("id") or 0),
                assets_removed,
                refs_removed,
            )
        )
    if not apply or not repairs:
        return len(repairs), 0, 0
    async with db.raw_conn() as conn:
        try:
            for refs_json, assets_json, post_id, _asset_count, _ref_count in repairs:
                await conn.execute(
                    "UPDATE guide_monitor_post SET media_refs_json=?, media_assets_json=? WHERE id=?",
                    (refs_json, assets_json, post_id),
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    return (
        len(repairs),
        sum(item[3] for item in repairs),
        sum(item[4] for item in repairs),
    )


def _remove_empty_dirs(root: Path, result: GuideMediaRetentionResult) -> None:
    if not root.is_dir():
        return
    directories: list[Path] = []
    for current_root, dir_names, _file_names in os.walk(root, topdown=True, followlinks=False):
        for name in dir_names:
            path = Path(current_root) / name
            if not path.is_symlink():
                directories.append(path)
    for path in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        try:
            path.rmdir()
            result.empty_dirs_removed += 1
        except OSError:
            continue


async def prune_guide_media_store(
    db: Database,
    *,
    root: str | Path,
    reason: str,
    dry_run: bool = False,
    policy: GuideMediaRetentionPolicy | None = None,
    now: datetime | None = None,
) -> GuideMediaRetentionResult:
    """Inventory and safely prune the persistent guide media store.

    Files referenced by current/future occurrences, recent source posts, or a
    current digest issue are immutable. Only old, unprotected regular files are
    candidates. Applying a prune also removes stale, position-aligned DB asset
    references for files deleted by this run or already proven absent.
    """

    policy = policy or GuideMediaRetentionPolicy.from_env()
    root_path = Path(root).expanduser()
    root_path = Path(os.path.abspath(os.path.normpath(str(root_path))))
    mode = "dry_run" if dry_run else "apply"
    result = GuideMediaRetentionResult(
        reason=str(reason or "manual"),
        mode=mode,
        root=str(root_path),
        enabled=policy.enabled,
        max_total_bytes=policy.max_total_bytes,
        min_free_bytes=policy.min_free_bytes,
    )
    if not policy.enabled:
        logger.info("guide_media_retention skipped reason=%s disabled=1", result.reason)
        return result

    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    recent_cutoff = current_time - timedelta(days=policy.recent_post_grace_days)
    state = await _load_db_media_state(
        db,
        root=root_path,
        today_iso=_local_today(current_time, policy.local_timezone),
        recent_cutoff=recent_cutoff,
    )
    protected = {
        *state.protected_current_occurrence,
        *state.protected_recent_post,
        *state.protected_current_digest,
    }
    result.current_occurrence_paths = len(state.protected_current_occurrence)
    result.recent_post_paths = len(state.protected_recent_post)
    result.current_digest_paths = len(state.protected_current_digest)
    result.protected_paths = len(protected)
    result.ignored_reference_paths = state.ignored_reference_paths

    files = _inventory(root_path, result)
    result.scanned_files = len(files)
    result.scanned_bytes = sum(item.size for item in files)
    existing_keys = {item.key for item in files}
    result.protected_existing_files = len(protected.intersection(existing_keys))
    try:
        result.free_bytes_before = shutil.disk_usage(root_path if root_path.exists() else root_path.parent).free
    except OSError as exc:
        result.errors.append(f"disk_usage:{type(exc).__name__}:{exc}")

    issue_map = _issue_by_id(state.issue_rows)
    candidates: list[_StoredFile] = []
    now_ts = current_time.timestamp()
    for stored in files:
        if stored.key in protected:
            continue
        expiry_seconds = _carousel_expiry_seconds(
            stored,
            root=root_path,
            issues=issue_map,
            policy=policy,
        )
        if expiry_seconds is None:
            expiry_seconds = policy.retention_days * 86400
        if now_ts - stored.mtime < expiry_seconds:
            result.skipped_too_recent += 1
            continue
        candidates.append(stored)
    candidates.sort(key=lambda item: (item.mtime, item.key))
    result.candidate_files = len(candidates)
    result.candidate_bytes = sum(item.size for item in candidates)

    planned: list[_StoredFile] = []
    planned_bytes = 0
    for stored in candidates:
        if len(planned) >= policy.max_delete_files:
            result.bounded = True
            break
        if planned and planned_bytes + stored.size > policy.max_delete_bytes:
            result.bounded = True
            break
        planned.append(stored)
        planned_bytes += stored.size
    result.planned_delete_files = len(planned)
    result.planned_delete_bytes = planned_bytes
    result.sample_planned_paths = [item.key for item in planned[:10]]

    stale_before = _stale_db_path_keys(state.post_rows, root_path)
    deleted_keys: set[str] = set()
    if not dry_run:
        for stored in planned:
            try:
                if stored.path.is_symlink() or not stored.path.is_file():
                    result.skipped_non_regular += 1
                    continue
                stored.path.unlink()
            except OSError as exc:
                result.errors.append(f"delete:{stored.key}:{type(exc).__name__}:{exc}")
                continue
            deleted_keys.add(stored.key)
            result.deleted_files += 1
            result.deleted_bytes += stored.size

    stale_for_repair = stale_before | (deleted_keys if not dry_run else {item.key for item in planned})
    result.stale_db_paths = len(stale_for_repair)
    try:
        planned_rows, assets_removed, refs_removed = await _repair_stale_db_paths(
            db,
            root=root_path,
            post_rows=state.post_rows,
            stale_keys=stale_for_repair,
            apply=not dry_run,
        )
        result.planned_db_rows_repaired = planned_rows
        if not dry_run:
            result.db_rows_repaired = planned_rows
            result.db_assets_removed = assets_removed
            result.db_refs_removed = refs_removed
    except Exception as exc:
        result.errors.append(f"db_repair:{type(exc).__name__}:{exc}")
        logger.exception("guide_media_retention db_repair_failed reason=%s", result.reason)

    if not dry_run:
        _remove_empty_dirs(root_path, result)
    effective_deleted = result.planned_delete_bytes if dry_run else result.deleted_bytes
    result.total_bytes_after = max(0, result.scanned_bytes - effective_deleted)
    result.free_bytes_after = result.free_bytes_before + effective_deleted
    result.policy_satisfied = (
        result.total_bytes_after <= policy.max_total_bytes
        and result.free_bytes_after >= policy.min_free_bytes
    )
    if (result.candidate_files > result.planned_delete_files) or (
        not result.policy_satisfied and bool(candidates)
    ):
        result.bounded = True

    logger.info(
        "guide_media_retention reason=%s mode=%s root=%s scanned_files=%s scanned_bytes=%s "
        "protected=%s candidates=%s candidate_bytes=%s planned=%s planned_bytes=%s "
        "deleted=%s deleted_bytes=%s stale_db_paths=%s db_rows_repaired=%s "
        "free_before=%s free_after=%s total_after=%s policy_satisfied=%s bounded=%s errors=%s",
        result.reason,
        result.mode,
        result.root,
        result.scanned_files,
        result.scanned_bytes,
        result.protected_paths,
        result.candidate_files,
        result.candidate_bytes,
        result.planned_delete_files,
        result.planned_delete_bytes,
        result.deleted_files,
        result.deleted_bytes,
        result.stale_db_paths,
        result.db_rows_repaired,
        result.free_bytes_before,
        result.free_bytes_after,
        result.total_bytes_after,
        result.policy_satisfied,
        result.bounded,
        len(result.errors),
    )
    return result
