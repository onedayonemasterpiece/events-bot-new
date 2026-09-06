"""Assign a newly accepted create result through existing partner portfolio rows.

Internal post-executor boundary only: never accepts an arbitrary caller event ID.
Failures must retain the operation's unknown outcome for canonical reconciliation.
"""
from __future__ import annotations

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
    """Synchronous, atomic current-policy/ownership check and idempotent assignment.

    Call only with the actual executor result and its original durable request.
    A first grant requires explicit created provenance. Existing/unknown merges
    are never a reason to acquire a new portfolio item, even within the same org.
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
        if conn.execute('SELECT 1 FROM event WHERE id=?', (event_id,)).fetchone() is None:
            raise _error('PARTNER_ACCEPTED_EVENT_NOT_FOUND')
        foreign = conn.execute(
            'SELECT 1 FROM mcp_partner_event WHERE event_id=? AND '
            '(tenant_id<>? OR organization_id<>?) LIMIT 1',
            (event_id, grant.tenant_id, grant.organization_id),
        ).fetchone()
        if foreign is not None:
            raise _error('PARTNER_ACCEPTED_EVENT_OWNERSHIP_CONFLICT')
        existing = conn.execute(
            'SELECT 1 FROM mcp_partner_event WHERE principal_id=? AND event_id=? '
            'AND tenant_id=? AND organization_id=?',
            (grant.principal_id, event_id, grant.tenant_id, grant.organization_id),
        ).fetchone()
        if existing is None and events[0]['result'] != 'created':
            raise _error('PARTNER_ACCEPTED_EVENT_MERGE_REQUIRES_OWNER')
        cursor = conn.execute(
            'INSERT OR IGNORE INTO mcp_partner_event '
            '(principal_id,tenant_id,organization_id,event_id,created_at) VALUES(?,?,?,?,?)',
            (grant.principal_id, grant.tenant_id, grant.organization_id, event_id, int(time.time())),
        )
        return {'event_id': event_id, 'principal_id': grant.principal_id,
                'tenant_id': grant.tenant_id, 'organization_id': grant.organization_id,
                'assigned': cursor.rowcount == 1}
