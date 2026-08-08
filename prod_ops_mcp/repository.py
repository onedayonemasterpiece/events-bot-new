from __future__ import annotations

from typing import Any, Mapping

from .repository_base import JOB_FIELDS, OPS_RUN_FIELDS, RepositoryError
from .repository_source import SourceTraceRepository


class ReadOnlyOperationsRepository(SourceTraceRepository):
    async def health_snapshot(self) -> dict[str, Any]:
        return await self._run(self._health_snapshot_sync)

    def _health_snapshot_sync(self) -> dict[str, Any]:
        if not self._path.exists():
            return {"database": {"status": "missing"}}
        stat = self._path.stat()
        result: dict[str, Any] = {"database": {
            "status": "available", "size_bytes": stat.st_size,
            "mtime_epoch": int(stat.st_mtime),
        }}
        with self._connect() as connection:
            result["database"]["data_version"] = int(connection.execute("PRAGMA data_version").fetchone()[0])
            if self._table_exists(connection, "event"):
                row = connection.execute("SELECT MAX(id) AS max_event_id FROM event").fetchone()
                result["database"]["max_event_id"] = row["max_event_id"] if row else None
            table = self._first_table(connection, ("joboutbox", "job_outbox"))
            if table:
                selected = self._selected(self._columns(connection, table), ("id", "status", "task", "updated_at"))
                if selected:
                    result["recent_jobs"] = self._rows(connection.execute(
                        f'SELECT {", ".join(selected)} FROM "{table}" ORDER BY id DESC LIMIT 5'
                    ))
        return result

    async def ops_runs_inspect(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        return await self._run(self._ops_runs_sync, dict(arguments))

    def _ops_runs_sync(self, arguments: dict[str, Any]) -> dict[str, Any]:
        limit = min(20, max(1, int(arguments.get("limit") or 10)))
        kind, status = str(arguments.get("kind") or "").strip(), str(arguments.get("status") or "").strip()
        if len(kind) > 80 or len(status) > 32:
            raise RepositoryError("run filter is too long")
        with self._connect() as connection:
            table = self._first_table(connection, ("ops_run", "opsrun"))
            if not table:
                return {"items": [], "count": 0, "evidence_gaps": ["ops_run_table_missing"]}
            columns = self._columns(connection, table)
            selected = self._selected(columns, OPS_RUN_FIELDS)
            where, params = [], []
            if kind and "kind" in columns:
                where.append('"kind" = ?'); params.append(kind)
            if status and "status" in columns:
                where.append('"status" = ?'); params.append(status)
            clause = f'WHERE {" AND ".join(where)}' if where else ""
            params.append(limit)
            items = self._rows(connection.execute(
                f'SELECT {", ".join(selected)} FROM "{table}" {clause} ORDER BY id DESC LIMIT ?', params,
            ))
            return {"items": items, "count": len(items), "limit": limit}

    async def jobs_inspect(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        return await self._run(self._jobs_sync, dict(arguments))

    def _jobs_sync(self, arguments: dict[str, Any]) -> dict[str, Any]:
        limit = min(20, max(1, int(arguments.get("limit") or 10)))
        status, event_id = str(arguments.get("status") or "").strip(), arguments.get("event_id")
        if len(status) > 32:
            raise RepositoryError("status is too long")
        with self._connect() as connection:
            table = self._first_table(connection, ("joboutbox", "job_outbox"))
            if not table:
                return {"items": [], "count": 0, "evidence_gaps": ["job_outbox_table_missing"]}
            columns = self._columns(connection, table)
            selected = self._selected(columns, JOB_FIELDS)
            where, params = [], []
            if event_id is not None and "event_id" in columns:
                try:
                    event_id = int(event_id)
                except (TypeError, ValueError) as exc:
                    raise RepositoryError("event_id must be an integer") from exc
                where.append('"event_id" = ?'); params.append(event_id)
            if status and "status" in columns:
                where.append('"status" = ?'); params.append(status)
            clause = f'WHERE {" AND ".join(where)}' if where else ""
            params.append(limit)
            items = self._rows(connection.execute(
                f'SELECT {", ".join(selected)} FROM "{table}" {clause} ORDER BY id DESC LIMIT ?', params,
            ))
            return {"items": items, "count": len(items), "limit": limit}


__all__ = ["ReadOnlyOperationsRepository", "RepositoryError"]
