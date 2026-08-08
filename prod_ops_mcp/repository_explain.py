from __future__ import annotations

from typing import Any

from .repository_base import (
    DECISION_FIELDS, EVENT_FIELDS, JOB_FIELDS, SOURCE_FACT_FIELDS, SOURCE_FIELDS,
    RepositoryError,
)
from .repository_search import EventSearchRepository


class EventExplainRepository(EventSearchRepository):
    async def event_explain(self, event_id: int) -> dict[str, Any]:
        return await self._run(self._event_explain_sync, int(event_id))

    def _event_explain_sync(self, event_id: int) -> dict[str, Any]:
        if event_id <= 0:
            raise RepositoryError("event_id must be positive")
        with self._connect() as connection:
            columns = self._columns(connection, "event")
            selected = self._selected(columns, EVENT_FIELDS)
            if not selected:
                return {"status": "not_found", "event_id": event_id}
            row = connection.execute(
                f'SELECT {", ".join(selected)} FROM event WHERE id=? LIMIT 1', (event_id,)
            ).fetchone()
            if row is None:
                return {"status": "not_found", "event_id": event_id}
            event = dict(row)
            result: dict[str, Any] = {
                "status": "found", "event": event, "sources": [], "source_facts": [],
                "identity_decisions": [], "jobs": [], "evidence_gaps": [],
                "public_surfaces": {
                    name: event.get(name) for name in (
                        "telegraph_url", "source_post_url", "source_vk_post_url",
                        "vk_repost_url", "tg_event_post_url", "ics_url",
                    ) if event.get(name)
                },
            }
            self._load_sources(connection, event_id, result)
            self._load_facts(connection, event_id, result)
            self._load_decisions(connection, event_id, result)
            self._load_jobs(connection, event_id, result)
            return result

    def _load_sources(self, connection, event_id: int, result: dict[str, Any]) -> None:
        table = self._first_table(connection, ("event_source", "eventsource"))
        if not table:
            result["evidence_gaps"].append("event_source_table_missing")
            return
        selected = self._selected(self._columns(connection, table), SOURCE_FIELDS)
        if selected:
            result["sources"] = self._rows(connection.execute(
                f'SELECT {", ".join(selected)} FROM "{table}" WHERE event_id=? ORDER BY id LIMIT 20',
                (event_id,),
            ))

    def _load_facts(self, connection, event_id: int, result: dict[str, Any]) -> None:
        table = self._first_table(connection, ("event_source_fact", "eventsourcefact"))
        if not table:
            result["evidence_gaps"].append("event_source_fact_table_missing")
            return
        columns = self._columns(connection, table)
        selected = self._selected(columns, SOURCE_FACT_FIELDS)
        if selected and "event_id" in columns:
            result["source_facts"] = self._rows(connection.execute(
                f'SELECT {", ".join(selected)} FROM "{table}" WHERE event_id=? ORDER BY id DESC LIMIT 30',
                (event_id,),
            ))

    def _load_decisions(self, connection, event_id: int, result: dict[str, Any]) -> None:
        table = self._first_table(connection, ("event_identity_decision_log", "eventidentitydecisionlog"))
        if not table:
            result["evidence_gaps"].append("identity_decision_table_missing")
            return
        columns = self._columns(connection, table)
        selected = self._selected(columns, DECISION_FIELDS)
        if not selected:
            return
        predicates, params = ['"event_id" = ?'], [event_id]
        if "candidate_event_id" in columns:
            predicates.append('"candidate_event_id" = ?')
            params.append(event_id)
        result["identity_decisions"] = self._rows(connection.execute(
            f'SELECT {", ".join(selected)} FROM "{table}" '
            f'WHERE ({" OR ".join(predicates)}) ORDER BY id DESC LIMIT 20', params,
        ))

    def _load_jobs(self, connection, event_id: int, result: dict[str, Any]) -> None:
        table = self._first_table(connection, ("joboutbox", "job_outbox"))
        if not table:
            result["evidence_gaps"].append("job_outbox_table_missing")
            return
        columns = self._columns(connection, table)
        selected = self._selected(columns, JOB_FIELDS)
        if selected and "event_id" in columns:
            result["jobs"] = self._rows(connection.execute(
                f'SELECT {", ".join(selected)} FROM "{table}" WHERE event_id=? ORDER BY id DESC LIMIT 20',
                (event_id,),
            ))
