"""Fail-closed identity and application-side cost guards for Region Talk YDB.

The ledger intentionally records an *I/O RU floor*, not an exact billed RU
value.  YQL billing is the greater of CPU and I/O cost; the client can account
for returned/written rows and bytes, while exact CPU cost is available only in
server-side query statistics.  Query and row ceilings therefore remain hard
independent guards even when the I/O estimate is below the eventual bill.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


class YdbIdentityError(RuntimeError):
    """The configured database does not match the explicit production owner."""


class YdbCostBudgetExceeded(RuntimeError):
    """A Region Talk YDB operation crossed an application-side cost ceiling."""


def _clean_path(value: Any) -> str:
    return str(value or "").strip().rstrip("/")


def validate_expected_database(
    database: str,
    env: Mapping[str, str] | None = None,
    *,
    require_expected: bool = True,
) -> str:
    """Validate the complete YDB database path without exposing it in errors.

    A complete YDB path contains the cloud/folder owner component, so exact
    equality is also the account-placement guard.  Error markers are stable and
    deliberately contain neither the configured nor expected path.
    """

    values = env if env is not None else os.environ
    actual = _clean_path(database)
    expected = _clean_path(values.get("REGION_TALK_YDB_EXPECTED_DATABASE"))
    if not actual:
        raise YdbIdentityError("region_talk_ydb_identity:database_missing")
    if not expected:
        if require_expected:
            raise YdbIdentityError("region_talk_ydb_identity:expected_database_missing")
        return actual
    if actual != expected:
        raise YdbIdentityError("region_talk_ydb_identity:expected_database_mismatch")
    return actual


def payload_size_bytes(value: Any) -> int:
    """Return deterministic UTF-8 JSON size for a response/write payload."""

    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    )


def rows_size_bytes(rows: Iterable[Any]) -> int:
    return sum(payload_size_bytes(row) for row in rows)


def estimated_yql_io_ru_floor(
    *,
    read_rows: int = 0,
    read_bytes: int = 0,
    written_rows: int = 0,
    written_bytes: int = 0,
) -> int:
    """Estimate the documented YQL I/O RU component.

    Reads cost ``max(rows, ceil(bytes/4KiB))`` and writes cost twice
    ``max(rows, ceil(bytes/1KiB))``.  CPU can make the billed result higher.
    """

    read_ops = max(max(0, int(read_rows)), math.ceil(max(0, int(read_bytes)) / 4096))
    write_ops = max(max(0, int(written_rows)), math.ceil(max(0, int(written_bytes)) / 1024))
    return read_ops + (2 * write_ops)


def _env_limit(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        parsed = int(str(values.get(name) or default).strip())
    except (TypeError, ValueError):
        parsed = default
    return max(0, parsed)


@dataclass
class YdbCostBudget:
    """Per-process/cycle hard budget with a redacted audit snapshot."""

    max_queries: int
    max_rows_read: int
    max_bytes_read: int
    max_rows_written: int
    max_bytes_written: int
    max_estimated_io_ru: int
    label: str = "region_talk"
    queries: int = 0
    rows_read: int = 0
    bytes_read: int = 0
    rows_written: int = 0
    bytes_written: int = 0
    operations: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        label: str = "region_talk",
    ) -> "YdbCostBudget":
        values = env if env is not None else os.environ
        return cls(
            max_queries=_env_limit(values, "REGION_TALK_YDB_BUDGET_MAX_QUERIES", 64),
            max_rows_read=_env_limit(values, "REGION_TALK_YDB_BUDGET_MAX_ROWS_READ", 5000),
            max_bytes_read=_env_limit(values, "REGION_TALK_YDB_BUDGET_MAX_BYTES_READ", 32 * 1024 * 1024),
            max_rows_written=_env_limit(values, "REGION_TALK_YDB_BUDGET_MAX_ROWS_WRITTEN", 1000),
            max_bytes_written=_env_limit(values, "REGION_TALK_YDB_BUDGET_MAX_BYTES_WRITTEN", 16 * 1024 * 1024),
            max_estimated_io_ru=_env_limit(values, "REGION_TALK_YDB_BUDGET_MAX_ESTIMATED_IO_RU", 8000),
            label=label,
        )

    @property
    def estimated_io_ru(self) -> int:
        return estimated_yql_io_ru_floor(
            read_rows=self.rows_read,
            read_bytes=self.bytes_read,
            written_rows=self.rows_written,
            written_bytes=self.bytes_written,
        )

    def _check(self, operation: str) -> None:
        checks = (
            ("queries", self.queries, self.max_queries),
            ("rows_read", self.rows_read, self.max_rows_read),
            ("bytes_read", self.bytes_read, self.max_bytes_read),
            ("rows_written", self.rows_written, self.max_rows_written),
            ("bytes_written", self.bytes_written, self.max_bytes_written),
            ("estimated_io_ru", self.estimated_io_ru, self.max_estimated_io_ru),
        )
        for metric, value, ceiling in checks:
            if ceiling > 0 and value > ceiling:
                raise YdbCostBudgetExceeded(
                    f"region_talk_ydb_budget_exceeded:{metric}:operation={operation}:value={value}:limit={ceiling}"
                )

    def before_query(self, operation: str) -> None:
        self.queries += 1
        self.operations.append({"operation": str(operation), "queries": 1})
        self._check(operation)

    def record_read(self, operation: str, rows: int, payload_bytes: int) -> None:
        rows = max(0, int(rows))
        payload_bytes = max(0, int(payload_bytes))
        self.rows_read += rows
        self.bytes_read += payload_bytes
        self.operations.append({"operation": str(operation), "rows_read": rows, "bytes_read": payload_bytes})
        self._check(operation)

    def record_write(self, operation: str, rows: int, payload_bytes: int) -> None:
        rows = max(0, int(rows))
        payload_bytes = max(0, int(payload_bytes))
        self.rows_written += rows
        self.bytes_written += payload_bytes
        self.operations.append({"operation": str(operation), "rows_written": rows, "bytes_written": payload_bytes})
        self._check(operation)

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": "region-talk-ydb-cost-ledger-v1",
            "label": self.label,
            "queries": self.queries,
            "rows_read": self.rows_read,
            "bytes_read": self.bytes_read,
            "rows_written": self.rows_written,
            "bytes_written": self.bytes_written,
            "estimated_io_ru_floor": self.estimated_io_ru,
            "limits": {
                "queries": self.max_queries,
                "rows_read": self.max_rows_read,
                "bytes_read": self.max_bytes_read,
                "rows_written": self.max_rows_written,
                "bytes_written": self.max_bytes_written,
                "estimated_io_ru": self.max_estimated_io_ru,
            },
            "exact_billed_ru_available": False,
            "exact_billed_ru_note": "CPU/query-statistics may make billed RU higher than the I/O floor",
        }
