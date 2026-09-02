from __future__ import annotations

import base64
import copy
import re
from collections import Counter
from dataclasses import replace
from typing import Any, Mapping

from .repository import (
    EventsEvidenceRepository,
    InvalidArgumentsError,
    redact_and_clip_untrusted,
)


_LIST_COLUMNS = (
    "id",
    "event_id",
    "task",
    "status",
    "attempts",
    "last_error",
    "coalesce_key",
    "depends_on",
    "updated_at",
    "next_run_at",
    "error_class",
)
_FILTER_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")
_CURSOR_RE = re.compile(r"^[A-Za-z0-9_-]{4,96}$")
_QUEUE_ARGUMENTS = ("event_id", "task", "status", "cursor", "limit")
_MAX_PAGE_ROWS = 10


def _positive_int(value: Any, *, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise InvalidArgumentsError(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidArgumentsError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise InvalidArgumentsError(f"{name} must be positive")
    return parsed


def _filter_token(value: Any, *, name: str) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise InvalidArgumentsError(f"{name} must be a string")
    clean = value.strip()
    if not _FILTER_TOKEN_RE.fullmatch(clean):
        raise InvalidArgumentsError(f"{name} is invalid")
    return clean.casefold()


def _page_limit(repository: EventsEvidenceRepository) -> int:
    return max(1, min(_MAX_PAGE_ROWS, repository.config.max_rows))


def _limit(value: Any, *, maximum: int) -> int:
    if value is None:
        return maximum
    if isinstance(value, bool):
        raise InvalidArgumentsError("limit must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidArgumentsError("limit must be an integer") from exc
    return max(1, min(parsed, maximum))


def _encode_cursor(job_id: int) -> str:
    raw = f"job-v1:{int(job_id)}".encode("ascii")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decode_cursor(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise InvalidArgumentsError("cursor must be a string")
    clean = value.strip()
    if not _CURSOR_RE.fullmatch(clean):
        raise InvalidArgumentsError("cursor is invalid")
    try:
        raw = base64.urlsafe_b64decode(clean + "=" * (-len(clean) % 4)).decode(
            "ascii"
        )
        prefix, identifier = raw.split(":", 1)
        job_id = int(identifier)
    except (ValueError, UnicodeDecodeError) as exc:
        raise InvalidArgumentsError("cursor is invalid") from exc
    if prefix != "job-v1" or job_id <= 0:
        raise InvalidArgumentsError("cursor is invalid")
    return job_id


def _safe_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in row.items():
        result[str(key)] = redact_and_clip_untrusted(
            value,
            limit=1000 if key == "last_error" else 300,
        )
    if result.get("id") is not None:
        result["fetch_id"] = f"job:{result['id']}"
    return result


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


def _queue_input_properties(
    repository: EventsEvidenceRepository,
) -> dict[str, Any]:
    maximum = _page_limit(repository)
    return {
        "include_jobs": {
            "type": "boolean",
            "default": False,
            "description": (
                "Include one bounded, payload-free JobOutbox page. "
                "Queue filters also imply true."
            ),
        },
        "event_id": {"type": "integer", "minimum": 1},
        "task": {
            "type": "string",
            "minLength": 1,
            "maxLength": 120,
            "pattern": _FILTER_TOKEN_RE.pattern,
        },
        "status": {
            "type": "string",
            "minLength": 1,
            "maxLength": 120,
            "pattern": _FILTER_TOKEN_RE.pattern,
        },
        "cursor": {"type": "string", "minLength": 4, "maxLength": 96},
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": maximum,
            "default": maximum,
        },
    }


async def publication_queue_page(
    repository: EventsEvidenceRepository,
    arguments: Mapping[str, Any],
) -> dict[str, Any]:
    """Return one stable, read-only JobOutbox page without exposing payloads.

    Pagination uses the immutable numeric job id rather than mutable queue
    timestamps. Filters are parameterized and schema-adaptive. Requesting a
    filter whose legacy table lacks the necessary column fails explicitly
    instead of returning a misleading empty page.
    """

    event_id = _positive_int(arguments.get("event_id"), name="event_id")
    task = _filter_token(arguments.get("task"), name="task")
    status = _filter_token(arguments.get("status"), name="status")
    cursor_id = _decode_cursor(arguments.get("cursor"))
    limit = _limit(arguments.get("limit"), maximum=_page_limit(repository))

    snapshot = await repository.db.schema()
    columns = snapshot.columns("joboutbox")
    if not columns or "id" not in columns:
        return {
            "jobs": [],
            "next_cursor": None,
            "page_status_counts": {},
            "filters": {},
            "read_contract": {
                "database": "sqlite mode=ro; query_only=ON",
                "provider_network_calls": 0,
                "payload_included": False,
                "ordering": "job_id_desc",
                "queue_table_available": False,
                "max_page_rows": _page_limit(repository),
            },
        }

    required_by_filter = {
        "event_id": event_id,
        "task": task,
        "status": status,
    }
    missing = sorted(
        column
        for column, requested in required_by_filter.items()
        if requested is not None and column not in columns
    )
    if missing:
        raise InvalidArgumentsError(
            "queue filter unavailable for current schema: " + ", ".join(missing)
        )

    selected = tuple(column for column in _LIST_COLUMNS if column in columns)
    select_sql = ", ".join(
        repository.db.quote_identifier(column) for column in selected
    )
    where: list[str] = []
    params: list[Any] = []

    if event_id is not None:
        where.append(f"{repository.db.quote_identifier('event_id')}=?")
        params.append(event_id)
    if task is not None:
        where.append(f"{repository.db.quote_identifier('task')}=?")
        params.append(task)
    if status is not None:
        where.append(f"{repository.db.quote_identifier('status')}=?")
        params.append(status)
    if cursor_id is not None:
        where.append(f"{repository.db.quote_identifier('id')}<?")
        params.append(cursor_id)

    base_where = " WHERE " + " AND ".join(where) if where else ""
    sql = (
        f"SELECT {select_sql} FROM {repository.db.quote_identifier('joboutbox')}"
        f"{base_where} ORDER BY {repository.db.quote_identifier('id')} DESC LIMIT ?"
    )
    raw_rows = await repository.db.query(
        sql,
        (*params, limit),
        max_rows=limit,
    )
    jobs = [_safe_row(row) for row in raw_rows]

    next_cursor = None
    if len(jobs) == limit and jobs:
        last_id = int(jobs[-1]["id"])
        more_where = [*where, f"{repository.db.quote_identifier('id')}<?"]
        more_sql = (
            f"SELECT {repository.db.quote_identifier('id')} "
            f"FROM {repository.db.quote_identifier('joboutbox')} "
            f"WHERE {' AND '.join(more_where)} LIMIT 1"
        )
        more_rows = await repository.db.query(
            more_sql,
            (*params, last_id),
            max_rows=1,
        )
        if more_rows:
            next_cursor = _encode_cursor(last_id)

    page_counts = Counter(str(job.get("status") or "unknown") for job in jobs)
    filters = {
        key: value
        for key, value in {
            "event_id": event_id,
            "task": task,
            "status": status,
        }.items()
        if value is not None
    }
    return {
        "jobs": jobs,
        "next_cursor": next_cursor,
        "page_status_counts": dict(sorted(page_counts.items())),
        "filters": filters,
        "read_contract": {
            "database": "sqlite mode=ro; query_only=ON",
            "provider_network_calls": 0,
            "payload_included": False,
            "ordering": "job_id_desc",
            "queue_table_available": True,
            "max_page_rows": _page_limit(repository),
        },
    }


def attach_owner_queue_observability(server: Any) -> None:
    """Extend the existing owner snapshot without changing the Codex catalogue."""

    owner_tools = tuple(server.protocol.tools)
    matches = [tool for tool in owner_tools if tool.name == "operations_snapshot"]
    if len(matches) != 1:
        raise ValueError("owner operations_snapshot tool is missing or duplicated")
    original = matches[0]
    existing_properties = original.input_schema.get("properties", {})
    if (
        isinstance(existing_properties, Mapping)
        and "include_jobs" in existing_properties
    ):
        return

    input_schema = copy.deepcopy(dict(original.input_schema))
    input_schema["type"] = "object"
    input_schema["additionalProperties"] = False
    properties = dict(input_schema.get("properties") or {})
    properties.update(_queue_input_properties(server.repository))
    input_schema["properties"] = properties

    async def operations_snapshot_with_queue(
        arguments: Mapping[str, Any], context: Any
    ) -> dict[str, Any]:
        requested = queue_requested(arguments)
        baseline = await original.handler({}, context)
        if not isinstance(baseline, dict):
            raise RuntimeError("operations_snapshot returned a non-object result")
        if not requested:
            return baseline
        result = dict(baseline)
        result["publication_queue"] = await publication_queue_page(
            server.repository,
            arguments,
        )
        return result

    enhanced = replace(
        original,
        description=(
            original.description
            + " Owner ChatGPT/OpenCode may additionally request one bounded, "
            "payload-free publication job page; fetch job:<id> for the existing "
            "detailed evidence view."
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
        " For owner queue inspection, call operations_snapshot with "
        "include_jobs=true or bounded job filters, then fetch job:<id> for "
        "detail. Job payloads are not listed."
    )
