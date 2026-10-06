"""Transactional, operation-specific domain acceptance receipts.

No parser, model, provider or alternate ledger. This module never commits its
caller's transaction and does nothing for legacy intake without a context.
"""
from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text

_CONTEXT_KEYS = frozenset({'operation_ref', 'action_digest', 'actor_subject',
                           'actor_client_id', 'actor_audience'})
_CONTEXT_OPTIONAL_KEYS = frozenset({'partner_policy_revision', 'partner_authority_kinds'})
_PARTNER_AUTHORITY_KINDS = frozenset({
    'venue_operator', 'organizer', 'festival_operator', 'represented_person',
    'represented_collective', 'series_operator', 'programme_operator',
})
_SCHEMA = 'event-operation-domain-receipt-v1'


class EventOperationReceiptError(RuntimeError):
    """Fail closed and roll back the containing domain mutation."""


class PartnerAuthorityReviewRequired(EventOperationReceiptError):
    """Parsed candidate is safe to review but cannot cross the canonical write gate."""

    def __init__(self, evaluation: Mapping[str, Any]) -> None:
        super().__init__('partner_authority_review_required')
        self.evaluation = dict(evaluation)


def validate_event_operation_context(context: Any) -> dict[str, Any] | None:
    if context is None:
        return None
    if (not isinstance(context, Mapping) or not _CONTEXT_KEYS <= set(context)
            or set(context) - _CONTEXT_KEYS - _CONTEXT_OPTIONAL_KEYS
            or any(not isinstance(context[key], str) or not 1 <= len(context[key]) <= 2048
                   for key in _CONTEXT_KEYS)
            or not re.fullmatch(r'evt_op_[A-Za-z0-9_-]{20,120}', context['operation_ref'])
            or not re.fullmatch(r'[a-f0-9]{64}', context['action_digest'])):
        raise EventOperationReceiptError('event_operation_context_invalid')
    revision = context.get('partner_policy_revision')
    if ((revision is not None and not _positive(revision))
            or (context['actor_subject'].startswith('partner:') and not _positive(revision))):
        raise EventOperationReceiptError('event_operation_policy_revision_invalid')
    kinds = context.get('partner_authority_kinds')
    if kinds is not None:
        if (not isinstance(kinds, list) or len(kinds) > len(_PARTNER_AUTHORITY_KINDS)
                or any(not isinstance(value, str) or value not in _PARTNER_AUTHORITY_KINDS for value in kinds)
                or kinds != sorted(set(kinds))):
            raise EventOperationReceiptError('event_operation_authority_context_invalid')
    return dict(context)


def _positive(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 2**63 - 1


class _PolicyRows:
    def __init__(self, result: Any) -> None:
        self.result = result

    def fetchone(self) -> Any:
        row = self.result.fetchone()
        return row._mapping if row is not None else None

    def fetchall(self) -> list[Any]:
        return [row._mapping for row in self.result.fetchall()]


class _PolicyConnection:
    """Adapt the existing policy's read-only DBAPI calls to this exact session."""
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def execute(self, sql: str, params: tuple = ()) -> _PolicyRows:
        return _PolicyRows(self.connection.exec_driver_sql(sql, params))


def _owner_review_approved(raw: Any, context: Mapping[str, Any]) -> bool:
    if not isinstance(raw, str) or not raw:
        return False
    try:
        audit = json.loads(raw)
    except (TypeError, ValueError):
        return False
    return bool(
        isinstance(audit, dict)
        and audit.get('schema') == 'partner-event-review-v1'
        and audit.get('decision') == 'approve'
        and audit.get('action_digest') == context.get('action_digest')
    )


async def guard_event_operation_context(
    session: Any, candidate: Any, *, event_id: int | None, effect: str, lock: bool = False,
) -> dict[str, Any] | None:
    context = getattr(candidate, 'event_operation_context', None)
    if context is None:
        return None
    try:
        context = validate_event_operation_context(context)
        if (effect not in {'created', 'merged', 'noop_exact_replay'}
                or (effect != 'created' and not _positive(event_id))):
            raise EventOperationReceiptError('event_operation_guard_target_invalid')
        where = ("operation_ref=:operation_ref AND operation_kind='create' AND status='processing' "
                 'AND action_digest=:action_digest AND actor_subject=:actor_subject '
                 'AND actor_client_id=:actor_client_id AND actor_audience=:actor_audience')
        if lock:
            # Acquire SQLite write ownership only at final commit boundary, not
            # across model calls. A stale read snapshot cannot upgrade past revoke.
            updated = await session.execute(text(
                'UPDATE event_change_log SET domain_receipt_json=domain_receipt_json WHERE ' + where
            ), context)
            if updated.rowcount != 1:
                raise EventOperationReceiptError('event_operation_binding_or_state_conflict')
        ledger = await session.execute(
            text('SELECT request_json,organizer_comment FROM event_change_log WHERE ' + where),
            context,
        )
        row = ledger.first()
        if row is None:
            raise EventOperationReceiptError('event_operation_binding_or_state_conflict')
        if context['actor_subject'].startswith('partner:'):
            try:
                stored = json.loads(row[0])
            except (TypeError, ValueError):
                stored = None
            stored_kinds = (
                stored.get('partner_authority_kinds', [])
                if isinstance(stored, dict)
                else None
            )
            if (not isinstance(stored, dict) or not _positive(stored.get('partner_policy_revision'))
                    or stored.get('partner_policy_revision') != context['partner_policy_revision']
                    or stored_kinds != context.get('partner_authority_kinds', [])):
                raise EventOperationReceiptError('event_operation_policy_revision_conflict')
            owner_override = _owner_review_approved(row[1], context)

            def resolve(sync_session: Any) -> dict[str, Any]:
                # Reuse current epoch/scopes/actions/portfolio policy on the exact
                # canonical transaction. Authority aliases are server-owned rows;
                # candidate fields are already parsed/grounded by Smart Update.
                from private_events_mcp.partner_access import PartnerAccessStore

                connection = _PolicyConnection(sync_session.connection())
                policy = PartnerAccessStore(
                    sync_session.bind.url.database, resource=context['actor_audience'], signing_key='',
                )
                grant = policy.resolve_durable(
                    actor_subject=context['actor_subject'], actor_client_id=context['actor_client_id'],
                    actor_audience=context['actor_audience'], scope='partner:events:propose',
                    action='event_create', conn=connection,
                )
                if grant.policy_revision != context['partner_policy_revision']:
                    raise EventOperationReceiptError('event_operation_policy_revision_conflict')
                current_kinds = list(policy.authority_kinds(grant, conn=connection))
                if current_kinds != context.get('partner_authority_kinds', []):
                    raise EventOperationReceiptError('event_operation_authority_revision_conflict')
                if owner_override:
                    return {
                        'status': 'owner_override',
                        'reason': 'owner_review_approved',
                        'matched_authority': None,
                    }
                if effect != 'created' and policy.owns(grant, int(event_id), conn=connection):
                    return {
                        'status': 'portfolio_owned',
                        'reason': 'existing_portfolio_assignment',
                        'matched_authority': None,
                    }
                evaluation = policy.evaluate_create_candidate(grant, candidate, conn=connection)
                if evaluation.get('status') != 'matched':
                    raise PartnerAuthorityReviewRequired(evaluation)
                return evaluation
            try:
                evaluation = await session.run_sync(resolve)
                setattr(candidate, 'partner_authority_evaluation', evaluation)
            except PartnerAuthorityReviewRequired:
                raise
            except EventOperationReceiptError:
                raise
            except Exception:
                raise EventOperationReceiptError('event_operation_partner_access_denied') from None
        return context
    except BaseException:
        await session.rollback()
        raise


async def record_event_operation_receipt(
    session: Any, candidate: Any, *, event_id: int, effect: str,
) -> dict[str, Any] | None:
    """Write immutable receipt before the caller's Event/EventSource commit.

    All exceptions roll back the caller's transaction, including pending Event
    changes. Receipt existence proves domain acceptance, NOT publication/fanout.
    """
    context = getattr(candidate, 'event_operation_context', None)
    if context is None:
        return None
    try:
        context = validate_event_operation_context(context)
        if not _positive(event_id) or effect not in {'created', 'merged', 'noop_exact_replay'}:
            raise EventOperationReceiptError('event_operation_receipt_invalid')
        candidate_key = getattr(candidate, 'candidate_key', None)
        occurrence_key = getattr(candidate, 'occurrence_key', None)
        fingerprint = getattr(candidate, 'source_fingerprint', None)
        candidate_id = getattr(candidate, 'smart_update_candidate_id', None)
        attempt_no = getattr(candidate, 'smart_update_attempt_no', None)
        if (not isinstance(candidate_key, str) or not 1 <= len(candidate_key) <= 512
                or not isinstance(occurrence_key, str) or not 1 <= len(occurrence_key) <= 512
                or not isinstance(fingerprint, str) or not re.fullmatch(r'[a-f0-9]{64}', fingerprint)
                or not _positive(candidate_id) or not _positive(attempt_no)):
            raise EventOperationReceiptError('event_operation_candidate_proof_missing')
        receipt = {**context, 'schema': _SCHEMA, 'event_id': event_id, 'effect': effect,
                   'candidate_key': candidate_key, 'occurrence_key': occurrence_key,
                   'candidate_state_id': candidate_id, 'attempt_no': attempt_no,
                   'source_fingerprint': fingerprint}
        await session.flush()
        await guard_event_operation_context(session, candidate, event_id=event_id, effect=effect, lock=True)
        evaluation = getattr(candidate, 'partner_authority_evaluation', None)
        if context['actor_subject'].startswith('partner:') and isinstance(evaluation, Mapping):
            receipt['partner_authority_status'] = str(evaluation.get('status') or '')[:40]
            matched = evaluation.get('matched_authority')
            if isinstance(matched, Mapping):
                authority_id = matched.get('authority_id')
                authority_kind = matched.get('authority_kind')
                if isinstance(authority_id, str) and re.fullmatch(r'auth_[a-f0-9]{24}', authority_id):
                    receipt['partner_authority_id'] = authority_id
                if isinstance(authority_kind, str) and authority_kind in _PARTNER_AUTHORITY_KINDS:
                    receipt['partner_authority_kind'] = authority_kind
        exists = await session.execute(text('SELECT 1 FROM event WHERE id=:event_id'), {'event_id': event_id})
        if exists.first() is None:
            raise EventOperationReceiptError('event_operation_event_missing')
        # The exact attempt, rather than mutable latest source state alone, must
        # exist and carry the original input correlation supplied by the facade.
        proof = await session.execute(text(
            'SELECT s.candidate_payload FROM smart_update_candidate_state s '
            'JOIN smart_update_attempt a ON a.candidate_state_id=s.id '
            'WHERE s.id=:candidate_id AND s.candidate_key=:candidate_key '
            'AND s.occurrence_key=:occurrence_key AND s.source_fingerprint=:fingerprint '
            'AND a.attempt_no=:attempt_no AND a.finished_at IS NULL'
        ), {'candidate_id': candidate_id, 'candidate_key': candidate_key,
            'occurrence_key': occurrence_key, 'fingerprint': fingerprint, 'attempt_no': attempt_no})
        proof_row = proof.first()
        try:
            payload = json.loads(proof_row[0]) if proof_row is not None else None
        except (TypeError, ValueError):
            payload = None
        if not isinstance(payload, dict) or payload.get('event_operation_context') != context:
            raise EventOperationReceiptError('event_operation_attempt_context_mismatch')
        if effect == 'noop_exact_replay':
            source = await session.execute(text(
                'SELECT 1 FROM event_source WHERE event_id=:event_id AND candidate_key=:candidate_key '
                'AND occurrence_key=:occurrence_key AND source_fingerprint=:fingerprint LIMIT 1'
            ), {'event_id': event_id, 'candidate_key': candidate_key,
                'occurrence_key': occurrence_key, 'fingerprint': fingerprint})
            if source.first() is None:
                raise EventOperationReceiptError('event_operation_exact_replay_proof_missing')
        params = dict(context)
        ledger = await session.execute(text(
            "SELECT domain_receipt_json FROM event_change_log WHERE operation_ref=:operation_ref "
            "AND operation_kind='create' AND status='processing' AND action_digest=:action_digest "
            'AND actor_subject=:actor_subject AND actor_client_id=:actor_client_id AND actor_audience=:actor_audience'
        ), params)
        row = ledger.first()
        if row is None:
            raise EventOperationReceiptError('event_operation_binding_or_state_conflict')
        previous = row[0]
        if previous is not None:
            try:
                same = json.loads(previous) == receipt
            except (TypeError, ValueError):
                same = False
            if not same:
                raise EventOperationReceiptError('event_operation_receipt_conflict')
            return receipt
        params['receipt'] = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        updated = await session.execute(text(
            'UPDATE event_change_log SET domain_receipt_json=:receipt '
            "WHERE operation_ref=:operation_ref AND operation_kind='create' AND status='processing' "
            'AND action_digest=:action_digest AND actor_subject=:actor_subject '
            'AND actor_client_id=:actor_client_id AND actor_audience=:actor_audience '
            'AND domain_receipt_json IS NULL'
        ), params)
        if updated.rowcount != 1:
            raise EventOperationReceiptError('event_operation_receipt_write_conflict')
        return receipt
    except BaseException:
        await session.rollback()
        raise
