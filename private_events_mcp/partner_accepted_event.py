"""Assign a newly accepted create result through existing partner portfolio rows.

Internal post-executor boundary only: never accepts an arbitrary caller event ID.
Failures must retain the operation's unknown outcome for canonical reconciliation.
"""
from __future__ import annotations

import json
import time
from collections.abc import Mapping
from typing import Any

from .event_create import EventCreateRequest
from .partner_access import PartnerAccessStore
from .tool_catalog import ToolExecutionError


def _error(code: str) -> ToolExecutionError:
    return ToolExecutionError(code, 'Accepted event assignment requires canonical reconciliation.', retry_safe=False)


def assign_accepted_event(
    partner_store: PartnerAccessStore, request: EventCreateRequest, result: Mapping[str, Any],
) -> dict[str, Any]:
    """Synchronous, atomic current-policy/authority check and idempotent assignment.

    Call only with the actual executor result and its original durable request.
    Existing events may be shared across independent partner portfolios only
    when the same operation carries durable authority proof or explicit owner
    approval; mere merge/replay is never enough.
    """
    if not isinstance(result, Mapping) or result.get('status') != 'accepted':
        raise _error('PARTNER_ACCEPTED_EVENT_RESULT_INVALID')
    ids = result.get('event_ids')
    if (not isinstance(ids, list) or len(ids) != 1 or isinstance(ids[0], bool)
            or not isinstance(ids[0], int) or not 1 <= ids[0] <= 2**63 - 1):
        raise _error('PARTNER_ACCEPTED_EVENT_RESULT_INVALID')
    event_id = ids[0]
    events = result.get('events')
    if (not isinstance(events, list) or len(events) != 1 or not isinstance(events[0], Mapping)
            or isinstance(events[0].get('event_id'), bool)
            or not isinstance(events[0].get('event_id'), int)
            or events[0]['event_id'] != event_id
            or not isinstance(events[0].get('result'), str)
            or events[0]['result'] not in {'created', 'merged_or_replay'}):
        raise _error('PARTNER_ACCEPTED_EVENT_RESULT_INVALID')

    with partner_store._connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        grant = partner_store.resolve_durable(
            actor_subject=request.actor_subject, actor_client_id=request.actor_client_id,
            actor_audience=request.actor_audience, scope='partner:events:propose',
            action='event_create', conn=conn,
        )
        if (getattr(request, 'partner_policy_revision', None) is not None
                and request.partner_policy_revision != grant.policy_revision):
            raise _error('PARTNER_POLICY_REVISION_STALE')
        if conn.execute('SELECT 1 FROM event WHERE id=?', (event_id,)).fetchone() is None:
            raise _error('PARTNER_ACCEPTED_EVENT_NOT_FOUND')
        existing = conn.execute(
            'SELECT 1 FROM mcp_partner_event WHERE principal_id=? AND event_id=? '
            'AND tenant_id=? AND organization_id=?',
            (grant.principal_id, event_id, grant.tenant_id, grant.organization_id),
        ).fetchone()
        if request._operation_ref is None and existing is None:
            foreign = conn.execute(
                'SELECT 1 FROM mcp_partner_event WHERE event_id=? AND '
                '(tenant_id<>? OR organization_id<>?) LIMIT 1',
                (event_id, grant.tenant_id, grant.organization_id),
            ).fetchone()
            if foreign is not None:
                raise _error('PARTNER_ACCEPTED_EVENT_OWNERSHIP_CONFLICT')

        authority_status = None
        if request._operation_ref is not None:
            row = conn.execute(
                'SELECT domain_receipt_json,organizer_comment FROM event_change_log '
                'WHERE operation_ref=? AND action_digest=? AND actor_subject=? '
                'AND actor_client_id=? AND actor_audience=?',
                (
                    request._operation_ref, request.action_digest, request.actor_subject,
                    request.actor_client_id, request.actor_audience,
                ),
            ).fetchone()
            if row is None:
                raise _error('PARTNER_ACCEPTED_EVENT_RECEIPT_MISSING')
            try:
                domain_receipt = json.loads(row[0]) if row[0] else {}
            except (TypeError, ValueError):
                domain_receipt = {}
            authority_status = (
                domain_receipt.get('partner_authority_status')
                if isinstance(domain_receipt, Mapping)
                else None
            )
            if authority_status is None and row[1]:
                try:
                    audit = json.loads(row[1])
                except (TypeError, ValueError):
                    audit = None
                if (
                    isinstance(audit, Mapping)
                    and audit.get('schema') == 'partner-event-review-v1'
                    and audit.get('decision') == 'approve'
                    and audit.get('action_digest') == request.action_digest
                ):
                    authority_status = 'owner_override'
            if authority_status not in {'matched', 'owner_override', 'portfolio_owned', None}:
                raise _error('PARTNER_ACCEPTED_EVENT_AUTHORITY_INVALID')

        if existing is None:
            if events[0]['result'] == 'merged_or_replay':
                if authority_status not in {'matched', 'owner_override'}:
                    raise _error('PARTNER_ACCEPTED_EVENT_MERGE_REQUIRES_AUTHORITY')
            elif request.partner_authority_kinds and authority_status not in {'matched', 'owner_override'}:
                # Legacy pre-authority created receipts remain recoverable when
                # partner_authority_kinds is absent; new gated requests require proof.
                raise _error('PARTNER_ACCEPTED_EVENT_AUTHORITY_REQUIRED')
        cursor = conn.execute(
            'INSERT OR IGNORE INTO mcp_partner_event '
            '(principal_id,tenant_id,organization_id,event_id,created_at) VALUES(?,?,?,?,?)',
            (grant.principal_id, grant.tenant_id, grant.organization_id, event_id, int(time.time())),
        )
        return {'event_id': event_id, 'principal_id': grant.principal_id,
                'tenant_id': grant.tenant_id, 'organization_id': grant.organization_id,
                'assigned': cursor.rowcount == 1}
