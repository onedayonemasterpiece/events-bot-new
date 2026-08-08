from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Mapping

from .repository_base import EVENT_FIELDS, ReadOnlySQLiteBase, RepositoryError

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class EventSearchRepository(ReadOnlySQLiteBase):
    async def events_find(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        return await self._run(self._events_find_sync, dict(arguments))

    def _events_find_sync(self, arguments: dict[str, Any]) -> dict[str, Any]:
        event_id = arguments.get("event_id")
        query = str(arguments.get("query") or "").strip()
        city = str(arguments.get("city") or "").strip()
        source_url = str(arguments.get("source_url") or "").strip()
        date_from = str(arguments.get("date_from") or "").strip()
        date_to = str(arguments.get("date_to") or "").strip()
        limit = min(20, max(1, int(arguments.get("limit") or 10)))
        if event_id is not None:
            try:
                event_id = int(event_id)
            except (TypeError, ValueError) as exc:
                raise RepositoryError("event_id must be an integer") from exc
            if event_id <= 0:
                raise RepositoryError("event_id must be positive")
        if len(query) > 120 or len(city) > 80 or len(source_url) > 500:
            raise RepositoryError("search argument is too long")
        for label, value in (("date_from", date_from), ("date_to", date_to)):
            if value and not _DATE_RE.fullmatch(value):
                raise RepositoryError(f"{label} must be YYYY-MM-DD")
        if not any((event_id, query, city, source_url, date_from, date_to)):
            raise RepositoryError("at least one bounded search filter is required")

        original_dates = bool(arguments.get("date_from") or arguments.get("date_to"))
        with self._connect() as connection:
            columns = self._columns(connection, "event")
            if not columns:
                return {"items": [], "count": 0, "evidence_gaps": ["event_table_missing"]}
            selected = self._selected(columns, EVENT_FIELDS, "e")
            where: list[str] = []
            params: list[Any] = []
            joins = ""
            if event_id:
                where.append('e."id" = ?')
                params.append(event_id)
            if query:
                searchable = [name for name in ("title", "short_description", "search_digest") if name in columns]
                if not searchable:
                    raise RepositoryError("searchable event columns are missing")
                escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                pattern = f"%{escaped}%"
                where.append("(" + " OR ".join(
                    f'e."{name}" LIKE ? ESCAPE \'\\\' COLLATE NOCASE' for name in searchable
                ) + ")")
                params.extend([pattern] * len(searchable))
                if "date" in columns and not date_from and not date_to:
                    today = date.today()
                    date_from = (today - timedelta(days=30)).isoformat()
                    date_to = (today + timedelta(days=730)).isoformat()
            if city and "city" in columns:
                where.append('e."city" = ? COLLATE NOCASE')
                params.append(city)
            if date_from and "date" in columns:
                where.append('e."date" >= ?')
                params.append(date_from)
            if date_to and "date" in columns:
                where.append('e."date" <= ?')
                params.append(date_to)
            if source_url:
                source_table = self._first_table(connection, ("event_source", "eventsource"))
                if source_table:
                    source_columns = self._columns(connection, source_table)
                    joins = f' JOIN "{source_table}" s ON s."event_id" = e."id" '
                    predicates = []
                    for name in ("source_url", "canonical_source_url"):
                        if name in source_columns:
                            predicates.append(f's."{name}" = ?')
                            params.append(source_url)
                    if not predicates:
                        return {"items": [], "count": 0, "evidence_gaps": ["source_url_columns_missing"]}
                    where.append("(" + " OR ".join(predicates) + ")")
                else:
                    predicates = []
                    for name in ("source_post_url", "source_vk_post_url"):
                        if name in columns:
                            predicates.append(f'e."{name}" = ?')
                            params.append(source_url)
                    if not predicates:
                        return {"items": [], "count": 0, "evidence_gaps": ["event_source_table_missing"]}
                    where.append("(" + " OR ".join(predicates) + ")")
            order = (["e.\"date\" ASC"] if "date" in columns else [])
            if "time" in columns:
                order.append('e."time" ASC')
            order.append('e."id" ASC')
            params.append(limit)
            sql = (
                f'SELECT DISTINCT {", ".join(selected)} FROM event e {joins} '
                f'WHERE {" AND ".join(where)} ORDER BY {", ".join(order)} LIMIT ?'
            )
            items = self._rows(connection.execute(sql, params))
            return {
                "items": items, "count": len(items), "limit": limit,
                "default_date_window_applied": bool(query and not original_dates),
            }
