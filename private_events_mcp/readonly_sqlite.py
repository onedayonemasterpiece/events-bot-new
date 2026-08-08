from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import quote


_READ_ONLY_SQL_RE = re.compile(r"^\s*(?:SELECT|WITH|PRAGMA)\b", re.IGNORECASE)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ReadOnlySQLiteError(RuntimeError):
    """Base error for the bounded read-only SQLite adapter."""


class DatabaseUnavailableError(ReadOnlySQLiteError):
    pass


class QueryBudgetExceeded(ReadOnlySQLiteError):
    pass


@dataclass(frozen=True, slots=True)
class SchemaSnapshot:
    tables: Mapping[str, frozenset[str]]
    generated_at: float

    def has_table(self, table: str) -> bool:
        return table in self.tables

    def columns(self, table: str) -> frozenset[str]:
        return self.tables.get(table, frozenset())


class ReadOnlySQLite:
    """Bounded SQLite reader that can never open the event database for writes.

    Each operation gets its own `mode=ro` connection and a VM progress handler.
    The adapter intentionally exposes no user-selectable SQL surface; callers pass
    only internal, parameterized statements.
    """

    def __init__(
        self,
        path: str,
        *,
        query_timeout_ms: int = 350,
        busy_timeout_ms: int = 250,
        max_rows: int = 25,
        schema_ttl_seconds: int = 30,
    ) -> None:
        self.path = path
        self.query_timeout_ms = max(50, int(query_timeout_ms))
        self.busy_timeout_ms = max(0, int(busy_timeout_ms))
        self.max_rows = max(1, int(max_rows))
        self.schema_ttl_seconds = max(1, int(schema_ttl_seconds))
        self._schema: SchemaSnapshot | None = None
        self._schema_lock = Lock()

    @staticmethod
    def quote_identifier(value: str) -> str:
        if not _IDENTIFIER_RE.fullmatch(value):
            raise ValueError(f"Unsafe SQLite identifier: {value!r}")
        return f'"{value}"'

    def _database_uri(self) -> str:
        if not self.path:
            raise DatabaseUnavailableError("database_path_missing")
        if self.path.startswith("file:"):
            separator = "&" if "?" in self.path else "?"
            return f"{self.path}{separator}mode=ro"
        absolute = os.path.abspath(self.path)
        if not Path(absolute).is_file():
            raise DatabaseUnavailableError("database_file_missing")
        return f"file:{quote(absolute)}?mode=ro"

    def _connect(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(
                self._database_uri(),
                uri=True,
                timeout=max(self.busy_timeout_ms / 1000.0, 0.001),
                isolation_level=None,
                check_same_thread=False,
            )
        except sqlite3.Error as exc:
            raise DatabaseUnavailableError("database_open_failed") from exc
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-2000")
        return conn

    @staticmethod
    def _validate_internal_sql(sql: str) -> None:
        if not isinstance(sql, str) or not _READ_ONLY_SQL_RE.match(sql):
            raise ValueError("Only internal read-only statements are allowed")
        # Multiple statements and write-oriented pragmas are not accepted.
        stripped = sql.strip().rstrip(";")
        if ";" in stripped:
            raise ValueError("Multiple SQLite statements are not allowed")
        lowered = stripped.casefold()
        forbidden = (
            "pragma writable_schema",
            "pragma journal_mode",
            "pragma wal_checkpoint",
            "pragma optimize",
            "pragma vacuum",
            "attach ",
            "detach ",
        )
        if any(item in lowered for item in forbidden):
            raise ValueError("Statement is outside the read-only contract")

    def _query_sync(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
        *,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        self._validate_internal_sql(sql)
        row_limit = min(max(1, int(max_rows or self.max_rows)), self.max_rows)
        deadline = time.monotonic() + self.query_timeout_ms / 1000.0
        conn = self._connect()
        timed_out = False

        def _progress() -> int:
            nonlocal timed_out
            if time.monotonic() >= deadline:
                timed_out = True
                return 1
            return 0

        conn.set_progress_handler(_progress, 500)
        try:
            cursor = conn.execute(sql, params)
            names = [item[0] for item in cursor.description or ()]
            rows = cursor.fetchmany(row_limit + 1)
            if len(rows) > row_limit:
                rows = rows[:row_limit]
            return [
                {name: row[index] for index, name in enumerate(names)}
                for row in rows
            ]
        except sqlite3.OperationalError as exc:
            message = str(exc).casefold()
            if timed_out or "interrupted" in message:
                raise QueryBudgetExceeded("sqlite_query_budget_exceeded") from exc
            if "locked" in message or "busy" in message:
                raise QueryBudgetExceeded("sqlite_busy") from exc
            raise ReadOnlySQLiteError("sqlite_query_failed") from exc
        except sqlite3.Error as exc:
            raise ReadOnlySQLiteError("sqlite_query_failed") from exc
        finally:
            conn.set_progress_handler(None, 0)
            conn.close()

    async def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
        *,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self._query_sync,
            sql,
            params,
            max_rows=max_rows,
        )

    def _schema_sync(self) -> SchemaSnapshot:
        now = time.monotonic()
        with self._schema_lock:
            if self._schema and now - self._schema.generated_at < self.schema_ttl_seconds:
                return self._schema
            deadline = time.monotonic() + self.query_timeout_ms / 1000.0
            conn = self._connect()
            timed_out = False

            def _progress() -> int:
                nonlocal timed_out
                if time.monotonic() >= deadline:
                    timed_out = True
                    return 1
                return 0

            conn.set_progress_handler(_progress, 500)
            try:
                table_rows = conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
                tables: dict[str, frozenset[str]] = {}
                for table_row in table_rows:
                    table = str(table_row[0])
                    if not _IDENTIFIER_RE.fullmatch(table):
                        continue
                    columns = conn.execute(
                        f"PRAGMA table_info({self.quote_identifier(table)})"
                    ).fetchall()
                    tables[table] = frozenset(str(column[1]) for column in columns)
                snapshot = SchemaSnapshot(tables=tables, generated_at=now)
                self._schema = snapshot
                return snapshot
            except sqlite3.OperationalError as exc:
                message = str(exc).casefold()
                if timed_out or "interrupted" in message:
                    raise QueryBudgetExceeded("sqlite_schema_budget_exceeded") from exc
                if "locked" in message or "busy" in message:
                    raise QueryBudgetExceeded("sqlite_busy") from exc
                raise ReadOnlySQLiteError("sqlite_schema_failed") from exc
            except sqlite3.Error as exc:
                raise ReadOnlySQLiteError("sqlite_schema_failed") from exc
            finally:
                conn.set_progress_handler(None, 0)
                conn.close()

    async def schema(self) -> SchemaSnapshot:
        return await asyncio.to_thread(self._schema_sync)

    async def existing_columns(
        self,
        table: str,
        candidates: Iterable[str],
    ) -> tuple[str, ...]:
        snapshot = await self.schema()
        available = snapshot.columns(table)
        return tuple(item for item in candidates if item in available)

    async def table_exists(self, table: str) -> bool:
        return (await self.schema()).has_table(table)

    async def quick_check(self) -> str:
        rows = await self.query("PRAGMA quick_check(1)", max_rows=1)
        if not rows:
            return "unknown"
        return str(next(iter(rows[0].values()), "unknown"))
