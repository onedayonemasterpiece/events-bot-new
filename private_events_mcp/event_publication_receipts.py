"""Current canonical publication evidence for an actor's accepted create operation.

Read-only database projection, not a provider/live-page verification claim.
"""
from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import aiosqlite

from .event_create import _OPERATION_REF_RE
from .tool_catalog import ToolCallContext, ToolExecutionError

_TASKS = ('telegraph_build', 'vk_sync', 'tg_event_publish', 'tg_premium_emoji_edit',
          'event_media_review', 'static_site_build')
_STATUSES = {'pending': 'queued', 'running': 'running', 'done': 'done',
             'error': 'error', 'paused': 'paused'}
_OPERATION_STATES = frozenset({'queued', 'review_required', 'processing', 'accepted',
                              'rejected', 'failed', 'outcome_unknown'})


def _denied() -> ToolExecutionError:
    return ToolExecutionError('EVENT_PUBLICATIONS_UNAVAILABLE',
                              'Event publication evidence is unavailable or not authorized.', retry_safe=False)


def _public_url(value: Any, surface: str) -> str | None:
    if (not isinstance(value, str) or not 1 <= len(value) <= 1000
            or value != value.strip() or any(ord(c) < 33 or ord(c) == 127 for c in value)):
        return None
    try:
        url = urlsplit(value)
        if (url.scheme != 'https' or url.username or url.password or url.port is not None
                or url.query or url.fragment):
            return None
    except ValueError:
        return None
    if surface == 'telegram':
        if url.netloc != 't.me' or not re.fullmatch(r'/[A-Za-z][A-Za-z0-9_]{4,31}/[1-9][0-9]*', url.path):
            return None
        if url.path.split('/')[1].lower() in {'joinchat', 'share', 'proxy', 'login'}:
            return None
    elif surface in {'vk', 'vk_repost'}:
        if url.netloc != 'vk.com' or not re.fullmatch(r'/wall-?[1-9][0-9]*_[1-9][0-9]*', url.path):
            return None
    elif surface == 'telegraph':
        if url.netloc != 'telegra.ph' or not re.fullmatch(r'/[A-Za-z0-9_-]{1,240}', url.path):
            return None
    else:
        return None
    return value


class EventPublicationReceiptService:
    def __init__(
        self, *, database: Any,
        authorize: Callable[[ToolCallContext, int | None], Awaitable[bool]],
        max_jobs: int = 50,
    ) -> None:
        if not callable(authorize):
            raise ValueError('current publication-read policy is required')
        if isinstance(max_jobs, bool) or not isinstance(max_jobs, int) or not 1 <= max_jobs <= 100:
            raise ValueError('max_jobs must be between 1 and 100')
        self.database = database
        self.authorize = authorize
        self.max_jobs = max_jobs

    async def _check(self, context: ToolCallContext, event_id: int | None) -> None:
        if context.resource != context.identity.audience:
            raise _denied()
        try:
            allowed = await self.authorize(context, event_id)
        except Exception:
            raise _denied() from None
        if allowed is not True:
            raise _denied()

    async def read(self, operation_ref: str, context: ToolCallContext) -> dict[str, Any]:
        """No event-ID parameter: only the accepted ID in this actor's ledger."""
        await self._check(context, None)
        if not isinstance(operation_ref, str) or not _OPERATION_REF_RE.fullmatch(operation_ref):
            raise _denied()
        identity = context.identity
        async with self.database.raw_conn() as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                "SELECT status,event_id,result_json FROM event_change_log WHERE operation_ref=? "
                "AND operation_kind='create' AND actor_subject=? AND actor_client_id=? AND actor_audience=?",
                (operation_ref, identity.subject, identity.client_id, identity.audience),
            )
            operation = await cursor.fetchone()
            await cursor.close()
        if operation is None:
            raise _denied()
        state = operation['status'] if operation['status'] in _OPERATION_STATES else 'unknown'
        base = {'operation_ref': operation_ref, 'operation_status': state,
                'observed_at': datetime.now(timezone.utc).isoformat(),
                'live_verified': False, 'evidence_source': 'canonical_database'}
        if state != 'accepted':
            await self._check(context, None)
            return {**base, 'availability': 'operation_not_accepted', 'event_id': None,
                    'publications': [], 'jobs': [], 'jobs_truncated': False}
        event_id = operation['event_id']
        if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0:
            raise _denied()
        try:
            result = json.loads(operation['result_json'])
            ids = result.get('event_ids') if isinstance(result, dict) else None
            if (not isinstance(ids, list) or len(ids) != 1
                    or isinstance(ids[0], bool) or not isinstance(ids[0], int)
                    or ids[0] != event_id):
                raise ValueError('accepted identity mismatch')
        except (TypeError, ValueError):
            raise _denied() from None
        await self._check(context, event_id)
        async with self.database.raw_conn() as conn:
            conn.row_factory = aiosqlite.Row
            # Both projections use one consistent read snapshot; policy after the
            # snapshot is checked outside it so a revoke cannot hide in that view.
            await conn.execute('BEGIN')
            try:
                cursor = await conn.execute(
                    'SELECT telegraph_url,tg_event_post_url,source_vk_post_url,vk_repost_url FROM event WHERE id=?',
                    (event_id,),
                )
                event = await cursor.fetchone()
                await cursor.close()
                cursor = await conn.execute(
                    "SELECT id,task,status FROM joboutbox WHERE event_id=? AND task IN ("
                    + ','.join('?' for _ in _TASKS) + ') ORDER BY id DESC LIMIT ?',
                    (event_id, *_TASKS, self.max_jobs + 1),
                )
                rows = await cursor.fetchall()
                await cursor.close()
            finally:
                await conn.rollback()
        await self._check(context, event_id)
        if event is None:
            return {**base, 'availability': 'canonical_event_missing', 'event_id': event_id,
                    'publications': [], 'jobs': [], 'jobs_truncated': False}
        publications = []
        for surface, column in (('telegram', 'tg_event_post_url'), ('vk', 'source_vk_post_url'),
                                ('vk_repost', 'vk_repost_url'), ('telegraph', 'telegraph_url')):
            url = _public_url(event[column], surface)
            publications.append({'surface': surface, 'url': url,
                                 'state': 'recorded_public_url' if url else 'no_public_receipt',
                                 'evidence_column': column})
        publications.append({'surface': 'static_site', 'url': None,
                             'state': 'event_inclusion_unverified', 'evidence_column': None})
        jobs = [{'job_id': row['id'], 'task': row['task'],
                 'state': _STATUSES.get(row['status'], 'unknown')}
                for row in rows[:self.max_jobs]]
        return {**base, 'availability': 'available', 'event_id': event_id,
                'publications': publications, 'jobs': jobs,
                'jobs_truncated': len(rows) > self.max_jobs}
