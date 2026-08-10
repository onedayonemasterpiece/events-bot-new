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
    RETRY_SCHEDULED = "RETRY_SCHEDULED"


TERMINAL_OUTCOMES = tuple(item.value for item in SmartUpdateTerminalOutcome)


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


@dataclass(frozen=True, slots=True)
class ClaimedCandidate:
    candidate_state_id: int
    candidate_key: str
    candidate_payload: dict[str, Any]
    attempts: int
    max_attempts: int
    previous_reason: str | None


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
            # close that append-only row as the durable retry it already
            # projected before opening the next attempt. Exact packet replay
            # will then recover the accepted terminal without losing balance.
            await conn.execute(
                "UPDATE smart_update_attempt SET finished_at=CURRENT_TIMESTAMP, "
                "terminal_outcome='RETRY_SCHEDULED', reason='superseded_by_replay' "
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
        state_id,
        candidate_key,
        attempt,
        attempt_no,
        stored_max,
        previous_reason,
        previous_diagnostic_event_id,
    )


async def finish_candidate_attempt(
    db: Any,
    receipt: CandidateAttemptReceipt,
    *,
    outcome: SmartUpdateTerminalOutcome,
    event_id: int | None = None,
    diagnostic_event_id: int | None = None,
    reason: str | None = None,
    retry_delay_seconds: int = 300,
) -> None:
    """Atomically close an attempt and project its terminal onto candidate state."""

    if not isinstance(outcome, SmartUpdateTerminalOutcome):
        outcome = SmartUpdateTerminalOutcome(str(outcome))
    accepted = outcome in {
        SmartUpdateTerminalOutcome.CREATED,
        SmartUpdateTerminalOutcome.MERGED,
        SmartUpdateTerminalOutcome.NOOP_EXACT_REPLAY,
    }
    accepted_event_id = int(event_id) if accepted and event_id is not None else None
    diagnostic_id = int(diagnostic_event_id) if diagnostic_event_id is not None else None
    delay = max(1, int(retry_delay_seconds))
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
            # ``max_attempts`` bounds identity uncertainty: the facade forces a
            # distinct create on that final identity attempt. Pure technical
            # failures have no safe fallback, so they remain durably due rather
            # than becoming an unreachable pseudo-terminal. Clamp their retry
            # counter at the last slot while the append-only attempt number
            # continues to grow.
            retry_exhausted = 0
            retry_counter = (
                min(receipt.attempt, max(0, receipt.max_attempts - 1))
                if outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
                and receipt.attempt >= receipt.max_attempts
                else (
                    receipt.attempt
                    if outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
                    else 0
                )
            )
            retry_expr = (
                f"datetime(CURRENT_TIMESTAMP, '+{delay} seconds')"
                if outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
                else "NULL"
            )
            await conn.execute(
                f"""
                UPDATE smart_update_candidate_state
                SET current_outcome=?, accepted_event_id=?, diagnostic_event_id=?, reason=?,
                    next_retry_at={retry_expr}, updated_at=CURRENT_TIMESTAMP,
                    retry_attempts=?,
                    retry_exhausted=?, claimed_by=NULL, claim_expires_at=NULL,
                    completed_at=CASE WHEN ?='RETRY_SCHEDULED' THEN NULL ELSE CURRENT_TIMESTAMP END
                WHERE id=?
                """,
                (
                    outcome.value,
                    accepted_event_id,
                    diagnostic_id,
                    reason,
                    retry_counter,
                    retry_exhausted,
                    outcome.value,
                    receipt.candidate_state_id,
                ),
            )
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
                WHERE current_outcome='RETRY_SCHEDULED' AND retry_exhausted=0
                  AND retry_attempts < max_attempts
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
                    )
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    return claimed
