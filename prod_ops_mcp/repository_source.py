from __future__ import annotations

from typing import Any, Mapping

from .repository_base import SOURCE_FIELDS, RepositoryError
from .repository_explain import EventExplainRepository


class SourceTraceRepository(EventExplainRepository):
    async def source_trace(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        event_id = arguments.get("event_id")
        source_url = str(arguments.get("source_url") or "").strip()
        if event_id is None and not source_url:
            raise RepositoryError("event_id or exact source_url is required")
        if len(source_url) > 500:
            raise RepositoryError("source_url is too long")
        return await self._run(self._source_trace_sync, event_id, source_url)

    def _source_trace_sync(self, event_id: Any, source_url: str) -> dict[str, Any]:
        with self._connect() as connection:
            table = self._first_table(connection, ("event_source", "eventsource"))
            if not table:
                return {"items": [], "count": 0, "evidence_gaps": ["event_source_table_missing"]}
            columns = self._columns(connection, table)
            selected = self._selected(columns, SOURCE_FIELDS)
            where, params = [], []
            if event_id is not None:
                try:
                    event_id = int(event_id)
                except (TypeError, ValueError) as exc:
                    raise RepositoryError("event_id must be an integer") from exc
                where.append('"event_id" = ?')
                params.append(event_id)
            if source_url:
                predicates = []
                for name in ("source_url", "canonical_source_url"):
                    if name in columns:
                        predicates.append(f'"{name}" = ?')
                        params.append(source_url)
                if not predicates:
                    return {"items": [], "count": 0, "evidence_gaps": ["source_url_columns_missing"]}
                where.append("(" + " OR ".join(predicates) + ")")
            items = self._rows(connection.execute(
                f'SELECT {", ".join(selected)} FROM "{table}" '
                f'WHERE {" AND ".join(where)} ORDER BY id DESC LIMIT 20', params,
            ))
            return {"items": items, "count": len(items)}
