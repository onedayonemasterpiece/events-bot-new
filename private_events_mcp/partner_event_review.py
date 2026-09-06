"""Human review transitions on the canonical create ledger, without event writes."""
from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from .crypto import constant_time_equal
from .event_create import EventCreateOperationStore, EventCreateRequest, _OPERATION_REF_RE
from .tool_catalog import ToolCallContext, ToolExecutionError


@dataclass(frozen=True, slots=True)
class ReviewTarget:
    operation_ref: str
    actor_subject: str
    actor_client_id: str
    actor_audience: str
    action_digest: str


def _error(code: str) -> ToolExecutionError:
    return ToolExecutionError(code, "Event review is unavailable or conflicts with current state.", retry_safe=False)


class PartnerEventReviewService:
    def __init__(
        self, *, store: EventCreateOperationStore,
        authorize_submission: Callable[[EventCreateRequest], Awaitable[bool]],
        authorize_decision: Callable[[ToolCallContext, ReviewTarget, str], Awaitable[bool]],
    ) -> None:
        if not callable(authorize_submission) or not callable(authorize_decision):
            raise ValueError("current submission and owner/partner decision policy are required")
        self.store = store
        self.authorize_submission = authorize_submission
        self.authorize_decision = authorize_decision

    async def submit(self, request: EventCreateRequest) -> dict[str, Any]:
        try:
            allowed = await self.authorize_submission(request)
        except Exception:
            raise _error("EVENT_REVIEW_ACCESS_DENIED") from None
        if allowed is not True:
            raise _error("EVENT_REVIEW_ACCESS_DENIED")
        operation, created = await self.store.reserve(request, initial_status="review_required")
        return {"operation": operation, "created": created}

    async def decide(
        self, operation_ref: str, *, expected_action_digest: str,
        decision: str, owner_context: ToolCallContext,
    ) -> dict[str, Any]:
        if (not isinstance(operation_ref, str) or not _OPERATION_REF_RE.fullmatch(operation_ref)
                or not isinstance(expected_action_digest, str)
                or not re.fullmatch(r"[a-f0-9]{64}", expected_action_digest)
                or decision not in {"approve", "reject"}):
            raise _error("EVENT_REVIEW_INVALID_ARGUMENTS")
        if owner_context.resource != owner_context.identity.audience:
            raise _error("EVENT_REVIEW_ACCESS_DENIED")
        columns = ','.join(self.store._PUBLIC_COLUMNS) + ',organizer_comment'
        async with self.store.database.raw_conn() as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await conn.execute(
                    f"SELECT {columns} FROM event_change_log WHERE operation_ref=? AND operation_kind='create'",
                    (operation_ref,),
                )
                row = await cursor.fetchone()
                await cursor.close()
                if row is None:
                    raise _error("EVENT_REVIEW_NOT_FOUND")
                target = ReviewTarget(operation_ref, row['actor_subject'], row['actor_client_id'],
                                      row['actor_audience'], row['action_digest'])
                # Must be bounded/read-only: the same canonical SQLite write lock
                # serializes policy revocation with this decision. No network/LLM.
                try:
                    allowed = await self.authorize_decision(owner_context, target, decision)
                except Exception:
                    raise _error("EVENT_REVIEW_ACCESS_DENIED") from None
                if allowed is not True:
                    raise _error("EVENT_REVIEW_ACCESS_DENIED")
                if not constant_time_equal(target.action_digest, expected_action_digest):
                    raise _error("EVENT_REVIEW_DIGEST_CONFLICT")
                raw_audit = row['organizer_comment']
                if raw_audit:
                    try:
                        audit = json.loads(raw_audit)
                    except (ValueError, TypeError):
                        raise _error("EVENT_REVIEW_STATE_CONFLICT") from None
                    if (not isinstance(audit, dict) or audit.get('schema') != 'partner-event-review-v1'
                            or audit.get('decision') != decision
                            or audit.get('action_digest') != expected_action_digest):
                        raise _error("EVENT_REVIEW_STATE_CONFLICT")
                    await conn.commit()
                    public = {key: row[key] for key in self.store._PUBLIC_COLUMNS}
                    return {"operation": self.store._row_to_public(public), "review": audit, "changed": False}
                if row['status'] != 'review_required':
                    raise _error("EVENT_REVIEW_STATE_CONFLICT")
                identity = owner_context.identity
                audit = {"schema": "partner-event-review-v1", "decision": decision,
                         "action_digest": expected_action_digest,
                         "reviewed_by": {"subject": identity.subject, "client_id": identity.client_id,
                                         "audience": identity.audience},
                         "reviewed_at": datetime.now(timezone.utc).isoformat()}
                status = 'queued' if decision == 'approve' else 'rejected'
                error_code = None if decision == 'approve' else 'EVENT_CREATE_OWNER_REJECTED'
                await conn.execute(
                    "UPDATE event_change_log SET status=?,organizer_comment=?,error_code=?,"
                    "updated_at=CURRENT_TIMESTAMP,completed_at=CASE WHEN ?='rejected' THEN CURRENT_TIMESTAMP ELSE NULL END "
                    "WHERE operation_ref=? AND status='review_required' AND action_digest=?",
                    (status, json.dumps(audit, ensure_ascii=False, sort_keys=True), error_code,
                     status, operation_ref, expected_action_digest),
                )
                cursor = await conn.execute(
                    f"SELECT {','.join(self.store._PUBLIC_COLUMNS)} FROM event_change_log WHERE operation_ref=?",
                    (operation_ref,),
                )
                updated = await cursor.fetchone()
                await cursor.close()
                await conn.commit()
                return {"operation": self.store._row_to_public(updated), "review": audit, "changed": True}
            except BaseException:
                await conn.rollback()
                raise
