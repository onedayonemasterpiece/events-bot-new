from __future__ import annotations

import asyncio
import aiosqlite
import hashlib
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urlsplit

from .crypto import (
    TokenValidationError,
    constant_time_equal,
    random_token,
    secret_hash,
    sign_compact_token,
    verify_compact_token,
)
from .repository import InvalidArgumentsError, redact_and_clip_untrusted
from .tool_catalog import ToolCallContext, ToolExecutionError, ToolSpec

logger = logging.getLogger(__name__)

_PREPARATION_TOKEN_TYPE = "event-create-prep+jwt"
_PREPARATION_PREFIX = "eventprep_"
_PREPARATION_TTL_SECONDS = 10 * 60
_RAW_TEXT_MAX_CHARS = 20_000
_PROCESSING_STALE_SECONDS = 30 * 60
_EXTERNAL_ID_RE = re.compile(r"^[A-Za-z0-9._~:@/-]{1,160}$")
_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9._~:@/-]{8,160}$")
_OPERATION_REF_RE = re.compile(r"^evt_op_[A-Za-z0-9_-]{20,120}$")
_TERMINAL_STATES = frozenset(
    {"accepted", "rejected", "failed", "outcome_unknown"}
)


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _required_text(
    value: Any,
    *,
    name: str,
    minimum: int = 1,
    maximum: int,
) -> str:
    if not isinstance(value, str):
        raise InvalidArgumentsError(f"{name} must be a string")
    clean = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(clean) < minimum:
        raise InvalidArgumentsError(f"{name} is required")
    if len(clean) > maximum:
        raise InvalidArgumentsError(f"{name} is too long")
    if any(ord(character) == 0 for character in clean):
        raise InvalidArgumentsError(f"{name} contains a forbidden character")
    return clean


def _optional_source_url(value: Any) -> str | None:
    if value in (None, ""):
        return None
    clean = _required_text(value, name="source_url", maximum=1000)
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in clean):
        raise InvalidArgumentsError("source_url contains a forbidden character")
    try:
        parsed = urlsplit(clean)
        _ = parsed.port
    except ValueError as exc:
        raise InvalidArgumentsError("source_url is invalid") from exc
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
        raise InvalidArgumentsError("source_url must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password:
        raise InvalidArgumentsError("source_url must not contain credentials")
    return clean


def _optional_external_id(value: Any) -> str | None:
    if value in (None, ""):
        return None
    clean = _required_text(value, name="source_external_id", maximum=160)
    if not _EXTERNAL_ID_RE.fullmatch(clean):
        raise InvalidArgumentsError("source_external_id is invalid")
    return clean


def _idempotency_key(value: Any) -> str:
    clean = _required_text(
        value,
        name="idempotency_key",
        minimum=8,
        maximum=160,
    )
    if not _IDEMPOTENCY_RE.fullmatch(clean):
        raise InvalidArgumentsError("idempotency_key is invalid")
    return clean


@dataclass(frozen=True, slots=True)
class EventCreateRequest:
    raw_text: str
    source_url: str | None
    source_external_id: str | None
    source_locator: str
    idempotency_key: str
    text_policy: str
    actor_subject: str
    actor_client_id: str
    actor_audience: str
    _persisted_idempotency_hash: str | None = None

    def canonical_action(self) -> dict[str, Any]:
        return {
            "schema": "events-mcp-owner-create-r1",
            "raw_text": self.raw_text,
            "source_url": self.source_url,
            "source_external_id": self.source_external_id,
            "source_locator": self.source_locator,
            "text_policy": self.text_policy,
        }

    @property
    def action_digest(self) -> str:
        return hashlib.sha256(
            _canonical_json(self.canonical_action()).encode("utf-8")
        ).hexdigest()

    @property
    def idempotency_hash(self) -> str:
        return self._persisted_idempotency_hash or secret_hash(self.idempotency_key)

    def stored_request(self) -> dict[str, Any]:
        return {
            **self.canonical_action(),
            "source_external_id": self.source_external_id,
        }


class EventCreateExecutor(Protocol):
    async def create(self, request: EventCreateRequest) -> Mapping[str, Any]: ...


class EventCreateOperationStore:
    """Minimal canonical-DB operation ledger for owner event creation."""

    _PUBLIC_COLUMNS = (
        "operation_ref",
        "operation_kind",
        "actor_subject",
        "actor_client_id",
        "actor_audience",
        "action_digest",
        "source_type",
        "source_url",
        "status",
        "event_id",
        "result_json",
        "error_code",
        "created_at",
        "started_at",
        "updated_at",
        "completed_at",
    )

    def __init__(self, database: Any) -> None:
        self.database = database

    @staticmethod
    def _row_to_public(row: Any) -> dict[str, Any]:
        if row is None:
            raise ToolExecutionError(
                "EVENT_OPERATION_NOT_FOUND",
                "Event operation was not found.",
                retry_safe=False,
            )
        data = dict(row)
        # Internal lookup material must never cross the MCP output boundary.
        data.pop("idempotency_hash", None)
        raw_result = data.pop("result_json", None)
        result: Any = None
        if isinstance(raw_result, str) and raw_result.strip():
            try:
                result = json.loads(raw_result)
            except json.JSONDecodeError:
                result = {"status": "unreadable_result"}
        elif isinstance(raw_result, Mapping):
            result = dict(raw_result)

        persisted_status = str(data.get("status") or "")
        if persisted_status == "processing":
            started_at = data.get("started_at")
            try:
                started = datetime.fromisoformat(str(started_at))
                started = (
                    started.replace(tzinfo=timezone.utc)
                    if started.tzinfo is None
                    else started.astimezone(timezone.utc)
                )
            except (TypeError, ValueError):
                started = None
            if started is not None and (
                datetime.now(timezone.utc) - started
            ).total_seconds() >= _PROCESSING_STALE_SECONDS:
                # Keep this read tool genuinely read-only. The projection becomes
                # conservative, while the durable row remains available for an
                # explicit recovery decision rather than being mutated by a GET.
                data["persisted_status"] = persisted_status
                data["status"] = "outcome_unknown"
                data["error_code"] = "EVENT_CREATE_STALE_PROCESSING"
                result = {
                    "status": "outcome_unknown",
                    "instruction": (
                        "Do not retry with another idempotency key; inspect "
                        "canonical event/source state before any operator decision."
                    ),
                }

        data["result"] = redact_and_clip_untrusted(result, limit=4000)
        data["terminal"] = str(data.get("status") or "") in _TERMINAL_STATES
        return redact_and_clip_untrusted(data, limit=4000)

    async def _select_by_idempotency(
        self,
        conn: Any,
        *,
        actor_subject: str,
        actor_client_id: str,
        actor_audience: str,
        idempotency_hash: str,
    ) -> Any:
        columns = ",".join(self._PUBLIC_COLUMNS) + ",idempotency_hash"
        cursor = await conn.execute(
            f"SELECT {columns} FROM event_change_log "
            "WHERE actor_subject=? AND actor_client_id=? AND actor_audience=? "
            "AND operation_kind='create' AND idempotency_hash=? LIMIT 1",
            (actor_subject, actor_client_id, actor_audience, idempotency_hash),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def reserve(self, request: EventCreateRequest) -> tuple[dict[str, Any], bool]:
        async with self.database.raw_conn() as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("BEGIN IMMEDIATE")
            try:
                existing = await self._select_by_idempotency(
                    conn,
                    actor_subject=request.actor_subject,
                    actor_client_id=request.actor_client_id,
                    actor_audience=request.actor_audience,
                    idempotency_hash=request.idempotency_hash,
                )
                if existing is not None:
                    if not constant_time_equal(
                        str(existing["action_digest"]), request.action_digest
                    ):
                        raise ToolExecutionError(
                            "EVENT_CREATE_IDEMPOTENCY_CONFLICT",
                            "The idempotency key is already bound to another event request.",
                            retry_safe=False,
                        )
                    await conn.commit()
                    return self._row_to_public(existing), False

                operation_ref = "evt_op_" + random_token(24)
                await conn.execute(
                    """
                    INSERT INTO event_change_log(
                        operation_ref,operation_kind,actor_subject,actor_client_id,
                        actor_audience,idempotency_hash,action_digest,source_type,
                        source_url,request_json,status,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?, 'queued',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """,
                    (
                        operation_ref,
                        "create",
                        request.actor_subject,
                        request.actor_client_id,
                        request.actor_audience,
                        request.idempotency_hash,
                        request.action_digest,
                        "manual",
                        request.source_locator,
                        _canonical_json(request.stored_request()),
                    ),
                )
                await conn.commit()
                row = await self._select_by_idempotency(
                    conn,
                    actor_subject=request.actor_subject,
                    actor_client_id=request.actor_client_id,
                    actor_audience=request.actor_audience,
                    idempotency_hash=request.idempotency_hash,
                )
                return self._row_to_public(row), True
            except BaseException:
                await conn.rollback()
                raise

    async def queued(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Internal recovery input; never expose request_json through MCP."""
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("recovery limit must be between 1 and 1000")
        async with self.database.raw_conn() as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                "SELECT operation_ref,actor_subject,actor_client_id,actor_audience,"
                "action_digest,idempotency_hash,request_json FROM event_change_log "
                "WHERE operation_kind='create' AND status='queued' "
                "ORDER BY created_at,operation_ref LIMIT ?", (limit,),
            )
            rows = await cursor.fetchall()
            await cursor.close()
        return [dict(row) for row in rows]

    async def mark_processing(self, operation_ref: str) -> bool:
        async with self.database.raw_conn() as conn:
            cursor = await conn.execute(
                """
                UPDATE event_change_log
                SET status='processing',started_at=COALESCE(started_at,CURRENT_TIMESTAMP),
                    updated_at=CURRENT_TIMESTAMP,error_code=NULL
                WHERE operation_ref=? AND status='queued'
                """,
                (operation_ref,),
            )
            await conn.commit()
            return int(cursor.rowcount or 0) == 1

    async def finish(
        self,
        operation_ref: str,
        *,
        status: str,
        result: Mapping[str, Any] | None,
        error_code: str | None = None,
    ) -> None:
        if status not in _TERMINAL_STATES:
            raise ValueError("invalid terminal event operation status")
        safe_result = redact_and_clip_untrusted(dict(result or {}), limit=12_000)
        event_ids = safe_result.get("event_ids") if isinstance(safe_result, Mapping) else None
        event_id = None
        if isinstance(event_ids, list) and event_ids:
            try:
                event_id = int(event_ids[0])
            except (TypeError, ValueError):
                event_id = None
        async with self.database.raw_conn() as conn:
            await conn.execute(
                """
                UPDATE event_change_log
                SET status=?,event_id=?,result_json=?,error_code=?,
                    updated_at=CURRENT_TIMESTAMP,completed_at=CURRENT_TIMESTAMP
                WHERE operation_ref=? AND status IN ('queued','processing')
                """,
                (
                    status,
                    event_id,
                    _canonical_json(safe_result),
                    error_code,
                    operation_ref,
                ),
            )
            await conn.commit()

    async def get(
        self,
        operation_ref: str,
        *,
        actor_subject: str,
        actor_client_id: str,
        actor_audience: str,
    ) -> dict[str, Any]:
        if not isinstance(operation_ref, str) or not _OPERATION_REF_RE.fullmatch(
            operation_ref
        ):
            raise InvalidArgumentsError("operation_ref is invalid")
        columns = ",".join(self._PUBLIC_COLUMNS)
        async with self.database.raw_conn() as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                f"SELECT {columns} FROM event_change_log "
                "WHERE operation_ref=? AND actor_subject=? AND actor_client_id=? "
                "AND actor_audience=? LIMIT 1",
                (operation_ref, actor_subject, actor_client_id, actor_audience),
            )
            row = await cursor.fetchone()
            await cursor.close()
        return self._row_to_public(row)


class EventCreateRuntime:
    def __init__(
        self,
        *,
        config: Any,
        database: Any,
        executor: EventCreateExecutor,
        authorize: Callable[[EventCreateRequest], Awaitable[bool]] | None = None,
    ) -> None:
        self.config = config
        self.store = EventCreateOperationStore(database)
        self.executor = executor
        self.authorize = authorize
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @staticmethod
    def request_from_arguments(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> EventCreateRequest:
        raw_text = _required_text(
            arguments.get("raw_text"),
            name="raw_text",
            minimum=10,
            maximum=_RAW_TEXT_MAX_CHARS,
        )
        source_url = _optional_source_url(arguments.get("source_url"))
        source_external_id = _optional_external_id(
            arguments.get("source_external_id")
        )
        if source_url is None and source_external_id is None:
            raise InvalidArgumentsError(
                "source_url or source_external_id is required"
            )
        key = _idempotency_key(arguments.get("idempotency_key"))
        policy = str(arguments.get("text_policy") or "smart_rewrite").strip()
        if policy != "smart_rewrite":
            raise InvalidArgumentsError(
                "R1 supports only text_policy=smart_rewrite"
            )
        source_locator = source_url
        if source_locator is None:
            source_locator = "mcp-owner:" + hashlib.sha256(
                f"{context.identity.client_id}:{source_external_id}".encode("utf-8")
            ).hexdigest()
        return EventCreateRequest(
            raw_text=raw_text,
            source_url=source_url,
            source_external_id=source_external_id,
            source_locator=source_locator,
            idempotency_key=key,
            text_policy=policy,
            actor_subject=context.identity.subject,
            actor_client_id=context.identity.client_id,
            actor_audience=context.identity.audience,
        )

    def prepare(
        self, request: EventCreateRequest, *, now: int | None = None
    ) -> dict[str, Any]:
        issued = int(time.time()) if now is None else int(now)
        expires = issued + _PREPARATION_TTL_SECONDS
        payload = {
            "sub": request.actor_subject,
            "client_id": request.actor_client_id,
            "aud": request.actor_audience,
            "digest": request.action_digest,
            "idem": request.idempotency_hash,
            "source": secret_hash(request.source_locator),
            "iat": issued,
            "nbf": issued - 5,
            "exp": expires,
            "nonce": random_token(12),
        }
        token = sign_compact_token(
            payload,
            self.config.signing_key,
            token_type=_PREPARATION_TOKEN_TYPE,
        )
        return {
            "preparation_ref": _PREPARATION_PREFIX + token,
            "action_digest": request.action_digest,
            "expires_at": expires,
            "committable": True,
            "preview": {
                "text_policy": request.text_policy,
                "raw_text_chars": len(request.raw_text),
                "raw_text_preview": request.raw_text[:600],
                "source_url": request.source_url,
                "source_external_id": request.source_external_id,
                "source_locator_alias": secret_hash(request.source_locator)[:16],
            },
            "planned_effects": {
                "parser": "existing add_events_from_text parser",
                "canonical_write": "full Smart Update only",
                "publication": "existing schedule_event_update_tasks -> JobOutbox",
                "direct_provider_calls": 0,
                "promo": False,
                "text_policy": "smart_rewrite",
            },
        }

    def verify_preparation(
        self,
        request: EventCreateRequest,
        *,
        preparation_ref: Any,
        action_digest: Any,
        now: int | None = None,
    ) -> None:
        if not isinstance(preparation_ref, str) or not preparation_ref.startswith(
            _PREPARATION_PREFIX
        ):
            raise ToolExecutionError(
                "EVENT_CREATE_PREPARATION_INVALID",
                "Event preparation is invalid or expired.",
                retry_safe=True,
            )
        if not isinstance(action_digest, str) or not re.fullmatch(
            r"[a-f0-9]{64}", action_digest
        ):
            raise InvalidArgumentsError("action_digest is invalid")
        if not constant_time_equal(action_digest, request.action_digest):
            raise ToolExecutionError(
                "EVENT_CREATE_DIGEST_MISMATCH",
                "The event request changed after preparation.",
                retry_safe=True,
            )
        try:
            payload = verify_compact_token(
                preparation_ref[len(_PREPARATION_PREFIX) :],
                self.config.signing_key,
                expected_type=_PREPARATION_TOKEN_TYPE,
                now=now,
            )
        except TokenValidationError as exc:
            raise ToolExecutionError(
                "EVENT_CREATE_PREPARATION_INVALID",
                "Event preparation is invalid or expired.",
                retry_safe=True,
            ) from exc
        bindings = {
            "sub": request.actor_subject,
            "client_id": request.actor_client_id,
            "aud": request.actor_audience,
            "digest": request.action_digest,
            "idem": request.idempotency_hash,
            "source": secret_hash(request.source_locator),
        }
        for key, expected in bindings.items():
            actual = payload.get(key)
            if not isinstance(actual, str) or not constant_time_equal(actual, expected):
                raise ToolExecutionError(
                    "EVENT_CREATE_PREPARATION_INVALID",
                    "Event preparation is invalid or expired.",
                    retry_safe=True,
                )

    def _spawn(
        self, operation_ref: str, request: EventCreateRequest,
        *, authorize: Callable[[EventCreateRequest], Awaitable[bool]] | None = None,
    ) -> None:
        if operation_ref in self._tasks:
            return
        task = asyncio.create_task(
            self._execute(operation_ref, request, authorize=authorize),
            name=f"events-mcp-create:{operation_ref}",
        )
        self._tasks[operation_ref] = task

        def _done(done: asyncio.Task[None]) -> None:
            self._tasks.pop(operation_ref, None)
            if done.cancelled():
                return
            try:
                error = done.exception()
            except Exception:
                logger.exception(
                    "event_create background result inspection failed operation_ref=%s",
                    operation_ref,
                )
                return
            if error is not None:
                logger.error(
                    "event_create background task failed operation_ref=%s error_type=%s",
                    operation_ref,
                    type(error).__name__,
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(_done)

    async def _execute(
        self, operation_ref: str, request: EventCreateRequest,
        *, authorize: Callable[[EventCreateRequest], Awaitable[bool]] | None = None,
    ) -> None:
        if not await self.store.mark_processing(operation_ref):
            return
        try:
            current_authorize = authorize or self.authorize
            if current_authorize is not None and await current_authorize(request) is not True:
                await self.store.finish(
                    operation_ref, status="rejected", result={"status": "rejected"},
                    error_code="EVENT_CREATE_ACCESS_REVOKED",
                )
                return
            raw_result = await self.executor.create(request)
            result = redact_and_clip_untrusted(dict(raw_result), limit=12_000)
            raw_status = str(result.get("status") or "failed")
            status = (
                raw_status
                if raw_status in {"accepted", "rejected", "failed"}
                else "failed"
            )
            raw_event_ids = result.get("event_ids")
            valid_event_ids = (
                isinstance(raw_event_ids, list)
                and bool(raw_event_ids)
                and all(
                    isinstance(value, int) and not isinstance(value, bool) and value > 0
                    for value in raw_event_ids
                )
            )
            if status == "accepted" and not valid_event_ids:
                status = "failed"
                result = {
                    "status": "failed",
                    "error_code": "EVENT_CREATE_RESULT_INVALID",
                }
            error_code = None if status == "accepted" else str(
                result.get("error_code") or "EVENT_CREATE_NOT_ACCEPTED"
            )[:120]
            await self.store.finish(
                operation_ref,
                status=status,
                result=result,
                error_code=error_code,
            )
        except asyncio.CancelledError:
            await self.store.finish(
                operation_ref,
                status="outcome_unknown",
                result={
                    "status": "outcome_unknown",
                    "instruction": "Do not retry with another idempotency key; inspect canonical event/source state.",
                },
                error_code="EVENT_CREATE_CANCELLED_AFTER_START",
            )
            raise
        except Exception:
            logger.exception(
                "event_create execution outcome is unknown operation_ref=%s",
                operation_ref,
            )
            # The executor owns parser + Smart Update + queue scheduling. Once it
            # starts, an arbitrary exception cannot prove that no canonical write
            # crossed the mutation boundary. Fail closed and require readback.
            await self.store.finish(
                operation_ref,
                status="outcome_unknown",
                result={
                    "status": "outcome_unknown",
                    "instruction": (
                        "Do not retry with another idempotency key; inspect "
                        "canonical event/source state before any operator decision."
                    ),
                },
                error_code="EVENT_CREATE_EXECUTION_OUTCOME_UNKNOWN",
            )

    async def commit(
        self,
        request: EventCreateRequest,
        *,
        preparation_ref: Any,
        action_digest: Any,
    ) -> dict[str, Any]:
        self.verify_preparation(
            request,
            preparation_ref=preparation_ref,
            action_digest=action_digest,
        )
        operation, created = await self.store.reserve(request)
        if created or operation.get("status") == "queued":
            self._spawn(str(operation["operation_ref"]), request)
            await asyncio.sleep(0)
            operation = await self.store.get(
                str(operation["operation_ref"]),
                actor_subject=request.actor_subject,
                actor_client_id=request.actor_client_id,
                actor_audience=request.actor_audience,
            )
        return operation

    async def recover_queued(
        self, *, authorize: Callable[[EventCreateRequest], Awaitable[bool]],
        limit: int = 100,
    ) -> int:
        """Resume only unclaimed intents with current mutation-boundary policy.

        Processing/unknown operations require explicit canonical reconciliation.
        Multiple runtimes compete through the atomic mark_processing claim.
        """
        if not callable(authorize):
            raise TypeError("recovery requires a current authorization callback")
        scheduled = 0
        for row in await self.store.queued(limit=limit):
            operation_ref = row["operation_ref"]
            if operation_ref in self._tasks:
                continue
            try:
                stored = json.loads(row["request_json"])
                if stored.get("schema") != "events-mcp-owner-create-r1":
                    raise ValueError("unsupported stored request schema")
                request = EventCreateRequest(
                    raw_text=stored["raw_text"], source_url=stored["source_url"],
                    source_external_id=stored["source_external_id"],
                    source_locator=stored["source_locator"], text_policy=stored["text_policy"],
                    actor_subject=row["actor_subject"], actor_client_id=row["actor_client_id"],
                    actor_audience=row["actor_audience"], idempotency_key="",
                    _persisted_idempotency_hash=row["idempotency_hash"],
                )
                if not constant_time_equal(request.action_digest, row["action_digest"]):
                    raise ValueError("stored request digest mismatch")
            except (ValueError, KeyError, TypeError, AttributeError):
                if await self.store.mark_processing(operation_ref):
                    await self.store.finish(
                        operation_ref, status="failed", result={"status": "failed"},
                        error_code="EVENT_CREATE_RECOVERY_REQUEST_INVALID",
                    )
                continue
            self._spawn(operation_ref, request, authorize=authorize)
            scheduled += 1
        return scheduled

    async def wait_for_operation(
        self, operation_ref: str, *, timeout: float = 10.0
    ) -> None:
        task = self._tasks.get(operation_ref)
        if task is not None:
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)

    async def shutdown(self) -> None:
        tasks = tuple(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


_INPUT_PROPERTIES = {
    "raw_text": {
        "type": "string",
        "minLength": 10,
        "maxLength": _RAW_TEXT_MAX_CHARS,
    },
    "source_url": {"type": "string", "maxLength": 1000},
    "source_external_id": {"type": "string", "maxLength": 160},
    "idempotency_key": {
        "type": "string",
        "minLength": 8,
        "maxLength": 160,
        "pattern": _IDEMPOTENCY_RE.pattern,
    },
    "text_policy": {
        "type": "string",
        "enum": ["smart_rewrite"],
        "default": "smart_rewrite",
    },
}
_GENERIC_OUTPUT = {"type": "object", "additionalProperties": True}


def build_event_create_tools(
    runtime: EventCreateRuntime,
) -> tuple[ToolSpec, ...]:
    async def prepare(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        request = runtime.request_from_arguments(arguments, context)
        return runtime.prepare(request)

    async def commit(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        request = runtime.request_from_arguments(arguments, context)
        return await runtime.commit(
            request,
            preparation_ref=arguments.get("preparation_ref"),
            action_digest=arguments.get("action_digest"),
        )

    async def operation_get(
        arguments: Mapping[str, Any], context: ToolCallContext
    ) -> dict[str, Any]:
        return await runtime.store.get(
            str(arguments.get("operation_ref") or ""),
            actor_subject=context.identity.subject,
            actor_client_id=context.identity.client_id,
            actor_audience=context.identity.audience,
        )

    write_scope = frozenset({"events:write"})
    return (
        ToolSpec(
            name="event_create_prepare",
            title="Prepare canonical event creation",
            description=(
                "Validate and freeze one owner event-create request. R1 supports raw text only, "
                "text_policy=smart_rewrite, and no media or promo. This tool does not mutate "
                "Event, EventSource, promo or JobOutbox."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["raw_text", "idempotency_key"],
                "properties": dict(_INPUT_PROPERTIES),
            },
            output_schema=_GENERIC_OUTPUT,
            scopes=write_scope,
            handler=prepare,
            read_only=True,
            destructive=False,
            idempotent=True,
            cacheable=False,
            publicly_discoverable=False,
        ),
        ToolSpec(
            name="event_create_commit",
            title="Commit canonical event creation",
            description=(
                "Start the prepared owner event request through the existing parser and full "
                "Smart Update. Accepted changes use the existing standard JobOutbox fan-out; "
                "the MCP handler never calls Telegram, VK or Telegraph providers directly. "
                "Poll event_operation_get until terminal."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "raw_text",
                    "idempotency_key",
                    "preparation_ref",
                    "action_digest",
                ],
                "properties": {
                    **_INPUT_PROPERTIES,
                    "preparation_ref": {"type": "string", "maxLength": 4096},
                    "action_digest": {
                        "type": "string",
                        "pattern": "^[a-f0-9]{64}$",
                    },
                },
            },
            output_schema=_GENERIC_OUTPUT,
            scopes=write_scope,
            handler=commit,
            read_only=False,
            destructive=True,
            idempotent=True,
            cacheable=False,
            publicly_discoverable=False,
            timeout_seconds=5.0,
        ),
        ToolSpec(
            name="event_operation_get",
            title="Get owner event operation",
            description=(
                "Read the durable owner event-create operation status and accepted event/job "
                "references. The operation is bound to the calling owner OAuth client."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "required": ["operation_ref"],
                "properties": {
                    "operation_ref": {
                        "type": "string",
                        "pattern": _OPERATION_REF_RE.pattern,
                    }
                },
            },
            output_schema=_GENERIC_OUTPUT,
            scopes=frozenset({"operations:read"}),
            handler=operation_get,
            read_only=True,
            destructive=False,
            idempotent=True,
            cacheable=False,
            publicly_discoverable=False,
        ),
    )
