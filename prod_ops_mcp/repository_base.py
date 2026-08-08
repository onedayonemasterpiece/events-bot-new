from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

EVENT_FIELDS = (
    "id", "title", "short_description", "date", "time", "end_date",
    "location_name", "location_address", "city", "festival", "event_type",
    "lifecycle_status", "identity_status", "merged_into_event_id", "is_free",
    "ticket_status", "telegraph_url", "source_post_url", "source_vk_post_url",
    "vk_repost_url", "tg_event_post_url", "ics_url", "added_at",
)
SOURCE_FIELDS = (
    "id", "event_id", "source_type", "source_url", "canonical_source_url",
    "source_role", "trust_level", "imported_at",
)
DECISION_FIELDS = (
    "id", "event_id", "candidate_event_id", "source_type", "source_url",
    "decision", "decision_reason", "confidence", "decided_by", "created_at",
)
SOURCE_FACT_FIELDS = ("id", "event_id", "source_id", "fact", "status", "created_at")
JOB_FIELDS = (
    "id", "event_id", "task", "status", "attempts", "last_error",
    "last_result", "updated_at", "next_run_at", "coalesce_key", "depends_on",
)
OPS_RUN_FIELDS = (
    "id", "kind", "trigger", "started_at", "finished_at", "status", "metrics_json",
)


class RepositoryError(RuntimeError):
    pass


class ReadOnlySQLiteBase:
    def __init__(self, database_path: str, *, query_timeout_ms: int = 300) -> None:
        self._path = Path(database_path).expanduser()
        self._timeout_seconds = max(0.05, query_timeout_ms / 1000.0)

    def _connect(self) -> sqlite3.Connection:
        path = self._path.resolve(strict=True)
        uri = f"file:{quote(str(path), safe='/')}?mode=ro"
        connection = sqlite3.connect(
            uri, uri=True, timeout=min(0.1, self._timeout_seconds), check_same_thread=False
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=50")
        deadline = time.monotonic() + self._timeout_seconds
        connection.set_progress_handler(
            lambda: 1 if time.monotonic() >= deadline else 0, 1000
        )
        return connection

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None

    @classmethod
    def _first_table(cls, connection: sqlite3.Connection, names: Iterable[str]) -> str | None:
        return next((name for name in names if cls._table_exists(connection, name)), None)

    @classmethod
    def _columns(cls, connection: sqlite3.Connection, table: str) -> set[str]:
        if not cls._table_exists(connection, table):
            return set()
        return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}

    @staticmethod
    def _selected(existing: set[str], desired: Iterable[str], alias: str = "") -> list[str]:
        prefix = f"{alias}." if alias else ""
        return [f'{prefix}"{name}" AS "{name}"' for name in desired if name in existing]

    @staticmethod
    def _rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
        return [dict(row) for row in cursor.fetchall()]

    async def _run(self, function, *args):
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(function, *args), timeout=self._timeout_seconds + 0.25
            )
        except (sqlite3.Error, OSError, asyncio.TimeoutError) as exc:
            raise RepositoryError(f"bounded read failed: {type(exc).__name__}") from exc
