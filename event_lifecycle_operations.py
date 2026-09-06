"""Exact-ID reviewed CANCEL/POSTPONE canonical transactions, without fan-out.

The host owns durable reservation/review and current authorization. Authorization
runs on this exact locked SQLite connection and must be read-only, bounded and
local: no commit, second connection, providers, models or Telegram impersonation.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from types import SimpleNamespace
from typing import Callable

from static_site_release import event_public_revision


class LifecycleOperationError(ValueError):
    pass


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


@dataclass(frozen=True)
class LifecycleAction:
    event_id: int
    action: str
    base_event_revision: str
    actor_subject: str
    actor_client_id: str
    actor_audience: str

    def payload(self) -> dict:
        if (type(self.event_id) is not int or not 1 <= self.event_id <= 2**63-1
                or not isinstance(self.action, str) or self.action not in {'CANCEL', 'POSTPONE'}):
            raise LifecycleOperationError('action_invalid')
        if not isinstance(self.base_event_revision, str) or not re.fullmatch(r'[a-f0-9]{64}', self.base_event_revision):
            raise LifecycleOperationError('revision_invalid')
        if any(not isinstance(value, str) or not value.strip() or len(value) > 2048 for value in
               (self.actor_subject, self.actor_client_id, self.actor_audience)):
            raise LifecycleOperationError('actor_invalid')
        return {'schema': 'event-lifecycle-action-v1', **asdict(self)}

    @property
    def digest(self) -> str:
        return hashlib.sha256(_json(self.payload()).encode()).hexdigest()


def _revision(row):
    values = dict(row)
    for field in ('linked_event_ids', 'photo_urls', 'topics'):
        if isinstance(values.get(field), str):
            values[field] = json.loads(values[field])
    for field in ('silent', 'time_is_default', 'is_free', 'pushkin_card'):
        if values.get(field) is not None:
            values[field] = bool(values[field])
    return event_public_revision(SimpleNamespace(**values))


def apply_lifecycle_operation(database_path: str | Path, *, operation_ref: str,
                              action: LifecycleAction, expected_action_digest: str,
                              authorize: Callable[[sqlite3.Connection, LifecycleAction], bool],
                              verify_review: Callable[[sqlite3.Connection, LifecycleAction, str, str], bool]) -> dict:
    """Consume an existing reviewed processing ledger row, never reserve/review it.

    verify_review independently verifies frozen approval for exact op/digest/actor/target
    on the same connection; it cannot substitute for current-policy authorize.
    No existing create-only review service is assumed to support these actions.
    Accepted replay returns historical receipt without reapplying after later edits.
    """
    payload = action.payload()
    digest = action.digest
    if expected_action_digest != digest or not isinstance(operation_ref, str) or not operation_ref:
        raise LifecycleOperationError('action_digest_conflict')
    if not callable(authorize) or not callable(verify_review):
        raise LifecycleOperationError('authorization_required')
    connection = sqlite3.connect(Path(database_path).resolve().as_uri() + '?mode=rw', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('BEGIN IMMEDIATE')
        ledger = connection.execute('SELECT * FROM event_change_log WHERE operation_ref=?', (operation_ref,)).fetchone()
        if ledger is None:
            raise LifecycleOperationError('operation_not_found')
        if (ledger['operation_kind'] != 'event_' + action.action.lower()
                or ledger['action_digest'] != digest or ledger['event_id'] != action.event_id
                or ledger['base_event_revision'] != action.base_event_revision
                or any(ledger[field] != getattr(action, field) for field in
                       ('actor_subject', 'actor_client_id', 'actor_audience'))
                or json.loads(ledger['request_json']) != payload):
            raise LifecycleOperationError('operation_context_conflict')
        if authorize(connection, action) is not True:
            raise LifecycleOperationError('access_denied')
        if not connection.in_transaction:
            raise LifecycleOperationError('authorization_transaction_lost')
        if verify_review(connection, action, operation_ref, digest) is not True:
            raise LifecycleOperationError('review_required')
        if not connection.in_transaction:
            raise LifecycleOperationError('review_transaction_lost')
        event = connection.execute('SELECT * FROM event WHERE id=?', (action.event_id,)).fetchone()
        target_status = {'CANCEL': 'cancelled', 'POSTPONE': 'postponed'}[action.action]
        if ledger['status'] == 'accepted':
            receipt = json.loads(ledger['result_json'] or '{}')
            expected = {'schema': 'event-lifecycle-result-v1', 'status': 'accepted',
                'operation_ref': operation_ref, 'action_digest': digest, 'event_id': action.event_id,
                'action': action.action, 'result_event_revision': ledger['result_event_revision'],
                'downstream': 'reconciliation_required'}
            if (receipt != expected or not re.fullmatch(r'[a-f0-9]{64}', ledger['result_event_revision'] or '')
                    or json.loads(ledger['before_json'] or '{}') != {'lifecycle_status': 'active'}
                    or json.loads(ledger['after_json'] or '{}') != {'lifecycle_status': target_status}
                    or json.loads(ledger['changed_fields_json'] or '[]') != ['lifecycle_status']):
                raise LifecycleOperationError('replay_receipt_conflict')
            connection.commit()
            return receipt
        if event is None or event['identity_status'] != 'canonical' or event['merged_into_event_id'] is not None:
            raise LifecycleOperationError('event_not_canonical')
        current_revision = _revision(event)
        if ledger['status'] != 'processing':
            raise LifecycleOperationError('operation_state_conflict')
        if current_revision != action.base_event_revision or event['lifecycle_status'] != 'active':
            raise LifecycleOperationError('event_revision_conflict')
        if (ledger['before_json'] or ledger['after_json'] or ledger['result_event_revision']
                or ledger['changed_fields_json'] or ledger['domain_receipt_json']):
            raise LifecycleOperationError('operation_history_conflict')
        before = {'lifecycle_status': event['lifecycle_status']}
        after = {'lifecycle_status': target_status}
        connection.execute('UPDATE event SET lifecycle_status=? WHERE id=?', (target_status, action.event_id))
        updated = connection.execute('SELECT * FROM event WHERE id=?', (action.event_id,)).fetchone()
        revision = _revision(updated)
        receipt = {'schema': 'event-lifecycle-result-v1', 'status': 'accepted',
                   'operation_ref': operation_ref, 'action_digest': digest, 'event_id': action.event_id,
                   'action': action.action, 'result_event_revision': revision,
                   'downstream': 'reconciliation_required'}
        changed = connection.execute("UPDATE event_change_log SET status='accepted',before_json=?,after_json=?,"
            'changed_fields_json=?,result_event_revision=?,result_json=?,updated_at=CURRENT_TIMESTAMP,'
            "completed_at=CURRENT_TIMESTAMP WHERE operation_ref=? AND status='processing' AND action_digest=?",
            (_json(before), _json(after), _json(['lifecycle_status']), revision, _json(receipt), operation_ref, digest))
        if changed.rowcount != 1:
            raise LifecycleOperationError('operation_state_conflict')
        connection.commit()
        return receipt
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
