"""Durable, closed Smart Update candidate/attempt state machine.

The state ledger is deliberately independent from the event transaction.  A candidate
is first persisted as RETRY_SCHEDULED and therefore remains recoverable if processing
is interrupted before a domain write can be acknowledged.  Each completed attempt has
exactly one of the five public terminal outcomes.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
from typing import Any, Mapping


class SmartUpdateTerminalOutcome(str, Enum):
    CREATED = "CREATED"
    MERGED = "MERGED"
    NOOP_EXACT_REPLAY = "NOOP_EXACT_REPLAY"
    REJECTED_PRODUCT_POLICY = "REJECTED_PRODUCT_POLICY"
    FAILED_TECHNICAL = "FAILED_TECHNICAL"
    # Legacy/transitional value only. New Smart Update invocations must close
    # in the same call and ``finish_candidate_attempt`` converts this value to
    # FAILED_TECHNICAL rather than scheduling background work.
    RETRY_SCHEDULED = "RETRY_SCHEDULED"


TERMINAL_OUTCOMES = tuple(item.value for item in SmartUpdateTerminalOutcome)


class ProductExclusionReason(str, Enum):
    """Closed product-policy reasons allowed to terminate without an Event."""

    MISSING_DATE = "missing_date"
    MISSING_TITLE = "missing_title"
    MISSING_LOCATION = "missing_location"
    EMPTY_TITLE_AFTER_CLEAN = "empty_title_after_clean"
    INVALID_DATE = "invalid_date"
    PAST_EVENT = "past_event"
    FESTIVAL_POST = "festival_post"
    SCHEDULE_DIGEST = "schedule_digest"
    PROMO_OR_CONGRATS = "promo_or_congrats"
    PROMO_ONLY = "promo_only"
    GIVEAWAY_NO_EVENT = "giveaway_no_event"
    NON_EVENT = "non_event"
    OPEN_CALL = "open_call"
    WORK_SCHEDULE = "work_schedule"
    NON_EVENT_NOTICE = "non_event_notice"
    VENUE_STATUS_UPDATE = "venue_status_update"
    CONGRATS_NOTICE = "congrats_notice"
    UNSUPPORTED_EXHIBITION_TEASER_DATE = "unsupported_exhibition_teaser_date"
    COURSE_PROMO = "course_promo"
    SERVICE_PROMO = "service_promo"
    RENTAL_BOOKING = "rental_booking"
    TOO_SOON = "too_soon"
    EVENT_LOGISTICS_NOTICE = "event_logistics_notice"
    ONLINE_EVENT = "online_event"
    BOOK_REVIEW = "book_review"
    PHOTO_DAY = "photo_day"
    RETROSPECTIVE_FUTURE_TEASER = "retrospective_future_teaser"
    COMPLETED_EVENT_REPORT = "completed_event_report"
    OUT_OF_REGION = "out_of_region"
    PROSE_LOCATION = "prose_location"


class RetryReason(str, Enum):
    """Closed retry classes. Technical classes are never identity fallbacks."""

    UNKNOWN = "unknown_typed_reason"
    INVALID_INTENT = "invalid_smart_update_intent"
    CANDIDATE_ATTEMPT_IN_PROGRESS = "candidate_attempt_in_progress"
    CANDIDATE_STATE_UNAVAILABLE = "candidate_state_unavailable"
    CANDIDATE_STATE_ACK_FAILED = "candidate_state_ack_failed"
    SOURCE_BINDING_CONFLICT = "source_binding_conflict"
    SMART_UPDATE_INTEGRITY_ERROR = "smart_update_integrity_error"
    SMART_UPDATE_PROCESSING_ERROR = "smart_update_processing_error"
    ATTACH_CONTEXT_TARGET_REQUIRED = "attach_context_target_required"
    ATTACH_CONTEXT_SOURCE_URL_REQUIRED = "attach_context_source_url_required"
    ATTACH_CONTEXT_TARGET_MISSING = "attach_context_target_missing"
    FINAL_PROBE_EVENT_MISSING = "final_probe_event_missing"
    EVENT_MISSING = "event_missing"
    IDENTITY_MATCH_DISAPPEARED = "identity_gate_match_disappeared"
    IDENTITY_TECHNICAL_FAILURE = "identity_technical_failure"
    IDENTITY_SEMANTIC_UNKNOWN = "identity_semantic_unknown"
    DEDUP_ADJUDICATOR_TECHNICAL_FAILURE = "dedup_adjudicator_technical_failure"
    PRODUCT_REASON_UNTYPED = "product_reason_untyped"
    SOURCE_DECISION_INVALID = "source_decision_invalid"
    SOURCE_VERIFICATION_REQUIRED = "source_verification_required"
    SOURCE_VERIFICATION_TECHNICAL_FAILURE = "source_verification_technical_failure"
    SOURCE_EVIDENCE_INCOMPLETE = "source_evidence_incomplete"


class IdentityDistinctReason(str, Enum):
    """Closed positive evidence authorising a separate Event."""

    RELATED_BUT_DISTINCT = "related_but_distinct"
    FESTIVAL_CONTEXT_SIBLING = "festival_context_sibling"
    UNSAFE_TO_MERGE = "unsafe_to_merge"
    SPECIFIC_TICKET_OCCURRENCE_CONFLICT = "specific_ticket_occurrence_conflict"
    EXPLICIT_OCCURRENCE_ID_CONFLICT = "explicit_occurrence_id_conflict"
    UNKNOWN_AFTER_BOUNDED_ADJUDICATION = "unknown_after_bounded_adjudication"
    INCOHERENT_MERGE = "incoherent_merge"


class LifecycleReason(str, Enum):
    """Closed lifecycle/provenance resolutions produced by Smart Update."""

    CONTEXT_PROVENANCE_ATTACHED = "context_provenance_attached"
    CONTEXT_PROVENANCE_REPLAY = "context_provenance_replay"


class CandidateAttemptInProgress(RuntimeError):
    """The same candidate identity is already leased by another execution."""


@dataclass(frozen=True, slots=True)
class CandidateAttemptReceipt:
    candidate_state_id: int
    candidate_key: str
    # Consecutive retry number for the current packet.
    attempt: int
    # Monotonic append-only ledger number for this candidate identity.
    attempt_no: int
    max_attempts: int
    previous_reason: str | None = None
    previous_diagnostic_event_id: int | None = None
    previous_retry_reason: RetryReason | None = None


@dataclass(frozen=True, slots=True)
class ClaimedCandidate:
    candidate_state_id: int
    candidate_key: str
    candidate_payload: dict[str, Any]
    attempts: int
    max_attempts: int
    previous_reason: str | None
    previous_retry_reason: RetryReason | None = None


def parse_retry_reason(value: object) -> RetryReason | None:
    """Parse only exact closed values; prose and substrings have no authority."""

    if isinstance(value, RetryReason):
        return value
    try:
        return RetryReason(str(value))
    except (TypeError, ValueError):
        return None


def _json_payload(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), ensure_ascii=False, sort_keys=True, default=str)


async def begin_candidate_attempt(
    db: Any,
    *,
    candidate_key: str,
    occurrence_key: str,
    canonical_source_url: str | None,
    source_type: str,
    intent: str,
    source_fingerprint: str,
    candidate_payload: Mapping[str, Any],
    max_attempts: int = 3,
    lease_owner: str,
    lease_seconds: int = 3600,
) -> CandidateAttemptReceipt:
    """Register/claim one attempt while keeping the candidate recoverable."""

    max_attempts = max(1, int(max_attempts))
    owner = str(lease_owner or "").strip()
    if not owner:
        raise ValueError("lease_owner_required")
    lease_seconds = max(30, min(int(lease_seconds), 7200))
    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            active_cursor = await conn.execute(
                "SELECT claimed_by FROM smart_update_candidate_state "
                "WHERE candidate_key=? AND claimed_by IS NOT NULL "
                "AND claim_expires_at>CURRENT_TIMESTAMP",
                (candidate_key,),
            )
            active_row = await active_cursor.fetchone()
            await active_cursor.close()
            if active_row is not None and str(active_row[0]) != owner:
                raise CandidateAttemptInProgress("candidate_attempt_in_progress")
            await conn.execute(
                f"""
                INSERT INTO smart_update_candidate_state(
                    candidate_key, occurrence_key, canonical_source_url, source_type,
                    intent, source_fingerprint, candidate_payload, current_outcome,
                    attempts, retry_attempts, max_attempts, next_retry_at,
                    claimed_by, claim_expires_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,'RETRY_SCHEDULED',0,0,?,CURRENT_TIMESTAMP,?,
                    datetime(CURRENT_TIMESTAMP, '+{lease_seconds} seconds'),CURRENT_TIMESTAMP)
                ON CONFLICT(candidate_key) DO UPDATE SET
                    retry_attempts=CASE
                        WHEN smart_update_candidate_state.current_outcome='RETRY_SCHEDULED'
                         AND smart_update_candidate_state.source_fingerprint=excluded.source_fingerprint
                        THEN smart_update_candidate_state.retry_attempts ELSE 0 END,
                    reason=CASE
                        WHEN smart_update_candidate_state.current_outcome='RETRY_SCHEDULED'
                         AND smart_update_candidate_state.source_fingerprint=excluded.source_fingerprint
                        THEN smart_update_candidate_state.reason ELSE NULL END,
                    source_fingerprint=excluded.source_fingerprint,
                    candidate_payload=excluded.candidate_payload,
                    max_attempts=MAX(smart_update_candidate_state.max_attempts, excluded.max_attempts),
                    claimed_by=excluded.claimed_by,
                    claim_expires_at=excluded.claim_expires_at,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    candidate_key,
                    occurrence_key,
                    canonical_source_url,
                    source_type,
                    intent,
                    source_fingerprint,
                    _json_payload(candidate_payload),
                    max_attempts,
                    owner,
                ),
            )
            cursor = await conn.execute(
                "SELECT id, attempts, retry_attempts, max_attempts, reason, "
                "occurrence_key, canonical_source_url, source_type, intent, diagnostic_event_id "
                "FROM smart_update_candidate_state WHERE candidate_key=?",
                (candidate_key,),
            )
            row = await cursor.fetchone()
            await cursor.close()
            if row is None:
                raise RuntimeError("candidate_state_registration_failed")
            state_id = int(row[0])
            previous_attempts = int(row[1])
            previous_retry_attempts = int(row[2])
            stored_max = int(row[3])
            previous_reason = str(row[4]) if row[4] is not None else None
            previous_diagnostic_event_id = int(row[9]) if row[9] is not None else None
            stored_identity = (
                str(row[5]),
                str(row[6] or ""),
                str(row[7]),
                str(row[8]),
            )
            requested_identity = (
                str(occurrence_key),
                str(canonical_source_url or ""),
                str(source_type),
                str(intent),
            )
            if stored_identity != requested_identity:
                raise RuntimeError("candidate_key_identity_collision")
            # A prior process may have committed the Event/EventSource write
            # and died before acknowledging its attempt. Once its lease is no
            # longer authoritative (or the same owner explicitly replays),
            # close the abandoned ledger row visibly before opening one
            # recovery attempt. Exact packet replay can still recover an
            # accepted domain write without leaving an automatic retry behind.
            await conn.execute(
                "UPDATE smart_update_attempt SET finished_at=CURRENT_TIMESTAMP, "
                "terminal_outcome='FAILED_TECHNICAL', reason='interrupted_before_ack' "
                "WHERE candidate_state_id=? AND finished_at IS NULL",
                (state_id,),
            )
            attempt_no = previous_attempts + 1
            attempt = previous_retry_attempts + 1
            await conn.execute(
                "UPDATE smart_update_candidate_state SET attempts=?, retry_attempts=?, current_outcome='RETRY_SCHEDULED', "
                "accepted_event_id=NULL, diagnostic_event_id=NULL, reason='attempt_started', "
                "next_retry_at=CURRENT_TIMESTAMP, retry_exhausted=0, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (attempt_no, attempt, state_id),
            )
            await conn.execute(
                """
                INSERT INTO smart_update_attempt(
                    candidate_state_id, attempt_no, started_at, terminal_outcome, reason
                ) VALUES(?,?,CURRENT_TIMESTAMP,'RETRY_SCHEDULED','attempt_started')
                """,
                (state_id, attempt_no),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    return CandidateAttemptReceipt(
        candidate_state_id=state_id,
        candidate_key=candidate_key,
        attempt=attempt,
        attempt_no=attempt_no,
        max_attempts=stored_max,
        previous_reason=previous_reason,
        previous_diagnostic_event_id=previous_diagnostic_event_id,
        previous_retry_reason=parse_retry_reason(previous_reason),
    )


async def finish_candidate_attempt(
    db: Any,
    receipt: CandidateAttemptReceipt,
    *,
    outcome: SmartUpdateTerminalOutcome,
    event_id: int | None = None,
    diagnostic_event_id: int | None = None,
    reason: str | None = None,
    retry_reason: RetryReason | None = None,
    product_exclusion_reason: ProductExclusionReason | None = None,
    identity_distinct_reason: IdentityDistinctReason | None = None,
    lifecycle_reason: LifecycleReason | None = None,
    retry_delay_seconds: int = 300,
) -> None:
    """Atomically close one attempt; never create background retry work.

    ``RETRY_SCHEDULED`` remains readable for legacy rows and old callers, but a
    completed invocation is projected as the visible ``FAILED_TECHNICAL``
    terminal. The delay argument is retained for caller compatibility only.
    """

    if not isinstance(outcome, SmartUpdateTerminalOutcome):
        outcome = SmartUpdateTerminalOutcome(str(outcome))
    if outcome is SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY:
        if not isinstance(product_exclusion_reason, ProductExclusionReason):
            outcome = SmartUpdateTerminalOutcome.FAILED_TECHNICAL
            retry_reason = RetryReason.PRODUCT_REASON_UNTYPED
            reason = retry_reason.value
        else:
            reason = product_exclusion_reason.value
    elif outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED:
        outcome = SmartUpdateTerminalOutcome.FAILED_TECHNICAL
        if isinstance(retry_reason, RetryReason):
            reason = retry_reason.value
    elif isinstance(identity_distinct_reason, IdentityDistinctReason):
        reason = identity_distinct_reason.value
    elif isinstance(lifecycle_reason, LifecycleReason):
        reason = lifecycle_reason.value

    accepted = outcome in {
        SmartUpdateTerminalOutcome.CREATED,
        SmartUpdateTerminalOutcome.MERGED,
        SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY,
    }
    accepted_event_id = int(event_id) if accepted and event_id is not None else None
    diagnostic_id = int(diagnostic_event_id) if diagnostic_event_id is not None else None
    del retry_delay_seconds
    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await conn.execute(
                """
                UPDATE smart_update_attempt
                SET finished_at=CURRENT_TIMESTAMP, terminal_outcome=?, accepted_event_id=?,
                    diagnostic_event_id=?, reason=?
                WHERE candidate_state_id=? AND attempt_no=?
                """,
                (
                    outcome.value,
                    accepted_event_id,
                    diagnostic_id,
                    reason,
                    receipt.candidate_state_id,
                    receipt.attempt_no,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("candidate_attempt_not_open")
            await cursor.close()
            failed_technical = outcome is SmartUpdateTerminalOutcome.FAILED_TECHNICAL
            retry_exhausted = 1 if failed_technical else 0
            retry_counter = receipt.attempt if failed_technical else 0
            await conn.execute(
                """
                UPDATE smart_update_candidate_state
                SET current_outcome=?, accepted_event_id=?, diagnostic_event_id=?, reason=?,
                    next_retry_at=NULL, updated_at=CURRENT_TIMESTAMP,
                    retry_attempts=?,
                    retry_exhausted=?, claimed_by=NULL, claim_expires_at=NULL,
                    completed_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    outcome.value,
                    accepted_event_id,
                    diagnostic_id,
                    reason,
                    retry_counter,
                    retry_exhausted,
                    receipt.candidate_state_id,
                ),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def terminalize_candidate_ack_failure(
    db: Any,
    receipt: CandidateAttemptReceipt,
    *,
    diagnostic_event_id: int | None,
    reason: str = "candidate_state_ack_failed",
) -> None:
    """Fail closed after an accepted domain write cannot be acknowledged.

    This is deliberately a separate minimal transaction from the normal
    accepted acknowledgement.  It prevents the provisional state/attempt from
    remaining an ownerless ``RETRY_SCHEDULED`` row when the product retry worker
    is disabled.  The already-written Event is diagnostic evidence only until
    an operator reconciles the explicit technical terminal.
    """

    diagnostic_id = (
        int(diagnostic_event_id) if diagnostic_event_id is not None else None
    )
    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await conn.execute(
                """
                UPDATE smart_update_attempt
                SET finished_at=COALESCE(finished_at, CURRENT_TIMESTAMP),
                    terminal_outcome='FAILED_TECHNICAL', accepted_event_id=NULL,
                    diagnostic_event_id=?, reason=?
                WHERE candidate_state_id=? AND attempt_no=?
                """,
                (
                    diagnostic_id,
                    reason,
                    receipt.candidate_state_id,
                    receipt.attempt_no,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("candidate_ack_failure_attempt_missing")
            await cursor.close()
            cursor = await conn.execute(
                """
                UPDATE smart_update_candidate_state
                SET current_outcome='FAILED_TECHNICAL', accepted_event_id=NULL,
                    diagnostic_event_id=?, reason=?, next_retry_at=NULL,
                    retry_attempts=?, retry_exhausted=1,
                    claimed_by=NULL, claim_expires_at=NULL,
                    completed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    diagnostic_id,
                    reason,
                    receipt.attempt,
                    receipt.candidate_state_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("candidate_ack_failure_state_missing")
            await cursor.close()
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def smart_update_funnel_counts(db: Any) -> dict[str, int]:
    """Return a balance-checkable snapshot of current candidate terminals."""

    counts = {name: 0 for name in TERMINAL_OUTCOMES}
    async with db.raw_conn() as conn:
        cursor = await conn.execute(
            "SELECT current_outcome, COUNT(*) FROM smart_update_candidate_state GROUP BY current_outcome"
        )
        rows = await cursor.fetchall()
        await cursor.close()
        exhausted_cursor = await conn.execute(
            "SELECT COUNT(*) FROM smart_update_candidate_state WHERE retry_exhausted=1"
        )
        exhausted_row = await exhausted_cursor.fetchone()
        await exhausted_cursor.close()
        attempts_cursor = await conn.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN finished_at IS NOT NULL THEN 1 ELSE 0 END) "
            "FROM smart_update_attempt"
        )
        attempts_row = await attempts_cursor.fetchone()
        await attempts_cursor.close()
    unresolved = 0
    for raw, count in rows:
        name = str(raw or "")
        if name in counts:
            counts[name] += int(count)
        else:
            unresolved += int(count)
    total = sum(counts.values()) + unresolved
    counts["candidates_total"] = total
    counts["terminal_unresolved"] = unresolved
    counts["terminal_balance"] = sum(counts[name] for name in TERMINAL_OUTCOMES)
    counts["retry_exhausted"] = int(exhausted_row[0]) if exhausted_row else 0
    attempt_starts = int(attempts_row[0] or 0) if attempts_row else 0
    attempt_terminals = int(attempts_row[1] or 0) if attempts_row else 0
    counts["attempt_starts"] = attempt_starts
    counts["attempt_terminals"] = attempt_terminals
    counts["attempt_unresolved"] = attempt_starts - attempt_terminals
    return counts


async def claim_due_candidates(
    db: Any,
    *,
    lease_owner: str,
    limit: int = 25,
    lease_seconds: int = 300,
) -> list[ClaimedCandidate]:
    """Claim due retry candidates once across concurrent processes."""

    owner = str(lease_owner or "").strip()
    if not owner:
        raise ValueError("lease_owner_required")
    limit = max(1, min(int(limit), 200))
    lease_seconds = max(30, min(int(lease_seconds), 3600))
    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await conn.execute(
                """
                SELECT id, candidate_key, candidate_payload, retry_attempts, max_attempts, reason
                FROM smart_update_candidate_state
                WHERE current_outcome='RETRY_SCHEDULED'
                  AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
                  AND (claim_expires_at IS NULL OR claim_expires_at <= CURRENT_TIMESTAMP)
                ORDER BY COALESCE(next_retry_at, created_at), id
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            claimed: list[ClaimedCandidate] = []
            for row in rows:
                state_id = int(row[0])
                update = await conn.execute(
                    f"UPDATE smart_update_candidate_state SET claimed_by=?, "
                    f"claim_expires_at=datetime(CURRENT_TIMESTAMP, '+{lease_seconds} seconds'), "
                    "updated_at=CURRENT_TIMESTAMP WHERE id=? "
                    "AND (claim_expires_at IS NULL OR claim_expires_at <= CURRENT_TIMESTAMP)",
                    (owner, state_id),
                )
                if update.rowcount != 1:
                    await update.close()
                    continue
                await update.close()
                try:
                    payload = json.loads(str(row[2] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    payload = {}
                claimed.append(
                    ClaimedCandidate(
                        candidate_state_id=state_id,
                        candidate_key=str(row[1]),
                        candidate_payload=payload if isinstance(payload, dict) else {},
                        attempts=int(row[3]),
                        max_attempts=int(row[4]),
                        previous_reason=str(row[5]) if row[5] is not None else None,
                        previous_retry_reason=parse_retry_reason(row[5]),
                    )
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    return claimed


async def terminalize_claimed_candidate_technical(
    db: Any,
    *,
    candidate_state_id: int,
    lease_owner: str,
    reason: str,
) -> None:
    """Close a claimed legacy retry that cannot even be rehydrated."""

    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            await conn.execute(
                "UPDATE smart_update_attempt SET finished_at=CURRENT_TIMESTAMP, "
                "terminal_outcome='FAILED_TECHNICAL', reason=? "
                "WHERE candidate_state_id=? AND finished_at IS NULL",
                (str(reason), int(candidate_state_id)),
            )
            cursor = await conn.execute(
                "UPDATE smart_update_candidate_state SET "
                "current_outcome='FAILED_TECHNICAL', reason=?, retry_exhausted=1, "
                "next_retry_at=NULL, claimed_by=NULL, claim_expires_at=NULL, "
                "completed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=? AND current_outcome='RETRY_SCHEDULED' AND claimed_by=?",
                (str(reason), int(candidate_state_id), str(lease_owner)),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("claimed_candidate_terminalization_lost_lease")
            await cursor.close()
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
