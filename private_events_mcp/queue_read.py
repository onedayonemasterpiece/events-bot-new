from __future__ import annotations

import re
import json
from dataclasses import replace
from typing import Any, Mapping

from .repository import (
    EventsEvidenceRepository,
    InvalidArgumentsError,
    redact_and_clip_untrusted,
)


_CORE_COLUMNS = (
    "id",
    "event_id",
    "task",
    "status",
    "attempts",
    "last_error",
    "updated_at",
    "next_run_at",
)
_STATUS_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")
_QUEUE_ARGUMENTS = ("event_id", "status", "before_job_id", "limit")
_MAX_PAGE_ROWS = 10


def _integer(value: Any, *, name: str, minimum: int) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise InvalidArgumentsError(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidArgumentsError(f"{name} must be an integer") from exc
    if parsed < minimum:
        qualifier = "non-negative" if minimum == 0 else "positive"
        raise InvalidArgumentsError(f"{name} must be {qualifier}")
    return parsed


def _status(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise InvalidArgumentsError("status must be a string")
    clean = value.strip().casefold()
    if not _STATUS_RE.fullmatch(clean):
        raise InvalidArgumentsError("status is invalid")
    return clean


def _page_limit(repository: EventsEvidenceRepository, value: Any) -> int:
    maximum = max(1, min(_MAX_PAGE_ROWS, repository.config.max_rows))
    if value is None:
        return maximum
    parsed = _integer(value, name="limit", minimum=1)
    assert parsed is not None
    return min(parsed, maximum)


def queue_requested(arguments: Mapping[str, Any]) -> bool:
    include_jobs = arguments.get("include_jobs")
    if include_jobs is not None and not isinstance(include_jobs, bool):
        raise InvalidArgumentsError("include_jobs must be a boolean")
    filters_requested = any(
        arguments.get(name) not in (None, "") for name in _QUEUE_ARGUMENTS
    )
    if include_jobs is False and filters_requested:
        raise InvalidArgumentsError(
            "include_jobs=false cannot be combined with queue filters"
        )
    return include_jobs is True or filters_requested


def _input_properties(repository: EventsEvidenceRepository) -> dict[str, Any]:
    maximum = max(1, min(_MAX_PAGE_ROWS, repository.config.max_rows))
    return {
        "include_jobs": {
            "type": "boolean",
            "default": False,
            "description": "Include one bounded, payload-free JobOutbox page.",
        },
        "event_id": {
            "type": "integer",
            "minimum": 0,
            "description": "Use 0 for global jobs not bound to one event.",
        },
        "status": {
            "type": "string",
            "minLength": 1,
            "maxLength": 80,
            "pattern": _STATUS_RE.pattern,
        },
        "before_job_id": {
            "type": "integer",
            "minimum": 1,
            "description": "Return jobs with a smaller numeric id.",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": maximum,
            "default": maximum,
        },
    }



def _safe_error(value: Any) -> Any:
    if isinstance(value, str) and value.lstrip().startswith(("{", "[")):
        # Never fall back to raw malformed JSON/Python repr: quoted credential
        # keys may evade free-text assignment redaction. Bound parser input too.
        if len(value) > 16_384:
            return "<structured error omitted: size budget>"
        try:
            value = json.loads(value)
            return redact_and_clip_untrusted(value, limit=1000)
        except (ValueError, TypeError, RecursionError):
            return "<structured error omitted: invalid encoding>"
    return redact_and_clip_untrusted(value, limit=1000)


def _safe_job(row: Mapping[str, Any]) -> dict[str, Any]:
    job = {
        str(key): redact_and_clip_untrusted(
            _safe_error(value) if key == "last_error" else value,
            limit=1000 if key == "last_error" else 300,
        )
        for key, value in row.items()
    }
    job["fetch_id"] = f"job:{job['id']}"
    return job


async def publication_queue_page(
    repository: EventsEvidenceRepository,
    arguments: Mapping[str, Any],
) -> dict[str, Any]:
    """Read a small, deterministic JobOutbox page without exposing payloads."""

    event_id = _integer(arguments.get("event_id"), name="event_id", minimum=0)
    status = _status(arguments.get("status"))
    before_job_id = _integer(
        arguments.get("before_job_id"), name="before_job_id", minimum=1
    )
    limit = _page_limit(repository, arguments.get("limit"))

    columns = (await repository.db.schema()).columns("joboutbox")
    missing = [column for column in _CORE_COLUMNS if column not in columns]
    if missing:
        return {
            "jobs": [],
            "next_before_job_id": None,
            "filters": {},
            "read_contract": {
                "database": "sqlite mode=ro; query_only=ON",
                "provider_network_calls": 0,
                "payload_included": False,
                "ordering": "job_id_desc",
                "queue_table_available": bool(columns),
                "queue_schema_supported": False,
                "missing_columns": missing,
                "max_page_rows": max(
                    1, min(_MAX_PAGE_ROWS, repository.config.max_rows)
                ),
            },
        }

    where: list[str] = []
    params: list[Any] = []
    if event_id is not None:
        where.append(f"{repository.db.quote_identifier('event_id')}=?")
        params.append(event_id)
    if status is not None:
        where.append(f"{repository.db.quote_identifier('status')}=?")
        params.append(status)
    if before_job_id is not None:
        where.append(f"{repository.db.quote_identifier('id')}<?")
        params.append(before_job_id)

    selected = ", ".join(
        repository.db.quote_identifier(column) for column in _CORE_COLUMNS
    )
    table = repository.db.quote_identifier("joboutbox")
    where_sql = " WHERE " + " AND ".join(where) if where else ""
    sql = (
        f"SELECT {selected} FROM {table}{where_sql} "
        f"ORDER BY {repository.db.quote_identifier('id')} DESC LIMIT ?"
    )
    rows = await repository.db.query(sql, (*params, limit), max_rows=limit)
    jobs = [_safe_job(row) for row in rows]

    next_before_job_id = None
    if len(jobs) == limit and jobs:
        last_id = int(jobs[-1]["id"])
        probe_where = [*where, f"{repository.db.quote_identifier('id')}<?"]
        probe = await repository.db.query(
            f"SELECT {repository.db.quote_identifier('id')} FROM {table} "
            f"WHERE {' AND '.join(probe_where)} LIMIT 1",
            (*params, last_id),
            max_rows=1,
        )
        if probe:
            next_before_job_id = last_id

    filters = {
        key: value
        for key, value in {
            "event_id": event_id,
            "status": status,
            "before_job_id": before_job_id,
        }.items()
        if value is not None
    }
    return {
        "jobs": jobs,
        "next_before_job_id": next_before_job_id,
        "filters": filters,
        "read_contract": {
            "database": "sqlite mode=ro; query_only=ON",
            "provider_network_calls": 0,
            "payload_included": False,
            "ordering": "job_id_desc",
            "queue_table_available": True,
            "queue_schema_supported": True,
            "missing_columns": [],
            "max_page_rows": max(1, min(_MAX_PAGE_ROWS, repository.config.max_rows)),
        },
    }


def attach_owner_queue_observability(server: Any) -> None:
    """Extend only the owner snapshot; keep the Codex catalogue unchanged."""

    owner_tools = tuple(server.protocol.tools)
    matches = [tool for tool in owner_tools if tool.name == "operations_snapshot"]
    if len(matches) != 1:
        raise ValueError("owner operations_snapshot tool is missing or duplicated")
    original = matches[0]
    if "include_jobs" in (original.input_schema.get("properties") or {}):
        return

    input_schema = dict(original.input_schema)
    input_schema["type"] = "object"
    input_schema["additionalProperties"] = False
    properties = dict(input_schema.get("properties") or {})
    properties.update(_input_properties(server.repository))
    input_schema["properties"] = properties

    async def operations_snapshot_with_queue(
        arguments: Mapping[str, Any], context: Any
    ) -> dict[str, Any]:
        requested = queue_requested(arguments)
        baseline = await original.handler({}, context)
        if not requested:
            return baseline
        if not isinstance(baseline, dict):
            raise RuntimeError("operations_snapshot returned a non-object result")
        return {
            **baseline,
            "job_queue": await publication_queue_page(server.repository, arguments),
        }

    enhanced = replace(
        original,
        description=(
            original.description
            + " Owner ChatGPT/OpenCode may request one small payload-free JobOutbox "
            "page; fetch job:<id> for existing detail evidence."
        ),
        input_schema=input_schema,
        handler=operations_snapshot_with_queue,
    )
    server.protocol.tools = tuple(
        enhanced if tool.name == "operations_snapshot" else tool
        for tool in owner_tools
    )
    server.protocol.by_name = {tool.name: tool for tool in server.protocol.tools}
    server.protocol.policy_fingerprint += "+queue-observability-r0"
    server.protocol.instructions += (
        " For owner JobOutbox inspection, call operations_snapshot with "
        "include_jobs=true or a bounded event/status filter, then fetch job:<id> "
        "for detail."
    )
