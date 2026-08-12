import sqlite3

import pytest

from db import Database
from smart_update_state import (
    CandidateAttemptInProgress,
    SmartUpdateTerminalOutcome,
    begin_candidate_attempt,
    finish_candidate_attempt,
    claim_due_candidates,
    smart_update_funnel_counts,
)


@pytest.mark.asyncio
async def test_candidate_state_schema_and_balanced_funnel(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "events.sqlite"))
    await db.init()

    receipt = await begin_candidate_attempt(
        db,
        candidate_key="candidate-1",
        occurrence_key="ordinal:0",
        canonical_source_url="https://vk.com/wall-1_2",
        source_type="vk",
        intent="UPSERT_EVENT",
        source_fingerprint="fingerprint-1",
        candidate_payload={"source_type": "vk"},
        max_attempts=3,
        lease_owner="test-candidate-1",
    )
    assert receipt.attempt == 1
    await finish_candidate_attempt(
        db,
        receipt,
        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
        reason="transient",
    )
    counts = await smart_update_funnel_counts(db)
    assert counts["candidates_total"] == 1
    assert counts["RETRY_SCHEDULED"] == 1
    assert counts["terminal_unresolved"] == 0
    assert counts["attempt_starts"] == counts["attempt_terminals"] == 1
    assert counts["attempt_unresolved"] == 0
    assert sum(counts[item.value] for item in SmartUpdateTerminalOutcome) == 1

    with sqlite3.connect(db.path) as conn:
        event_source_columns = {row[1] for row in conn.execute("PRAGMA table_info(event_source)")}
        assert {"candidate_key", "occurrence_key", "smart_update_candidate_id"} <= event_source_columns
        attempt_rows = conn.execute(
            "SELECT attempt_no, terminal_outcome FROM smart_update_attempt"
        ).fetchall()
        assert attempt_rows == [(1, "RETRY_SCHEDULED")]
    await db.close()


@pytest.mark.asyncio
async def test_due_claim_is_leased_once_and_retry_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "claims.sqlite"))
    await db.init()
    receipt = await begin_candidate_attempt(
        db,
        candidate_key="candidate-due",
        occurrence_key="ordinal:1",
        canonical_source_url="https://vk.com/wall-1_3",
        source_type="vk",
        intent="UPSERT_EVENT",
        source_fingerprint="fp",
        candidate_payload={"source_type": "vk"},
        max_attempts=2,
        lease_owner="worker-a",
    )
    await finish_candidate_attempt(
        db,
        receipt,
        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
        reason="merge_identity_llm_unavailable",
        retry_delay_seconds=1,
    )
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE smart_update_candidate_state SET next_retry_at=CURRENT_TIMESTAMP"
        )
        await conn.commit()
    first = await claim_due_candidates(db, lease_owner="worker-a")
    second = await claim_due_candidates(db, lease_owner="worker-b")
    assert len(first) == 1
    assert first[0].previous_reason == "merge_identity_llm_unavailable"
    assert second == []
    await db.close()


@pytest.mark.asyncio
async def test_attempt_ledger_is_monotonic_and_active_claim_blocks_duplicate_execution(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "attempts.sqlite"))
    await db.init()
    first = await begin_candidate_attempt(
        db,
        candidate_key="candidate-ledger",
        occurrence_key="ordinal:4",
        canonical_source_url="https://vk.com/wall-1_4",
        source_type="vk",
        intent="UPSERT_EVENT",
        source_fingerprint="fp-1",
        candidate_payload={"source_type": "vk"},
        max_attempts=2,
        lease_owner="worker-a",
    )
    with pytest.raises(CandidateAttemptInProgress):
        await begin_candidate_attempt(
            db,
            candidate_key="candidate-ledger",
            occurrence_key="ordinal:4",
            canonical_source_url="https://vk.com/wall-1_4",
            source_type="vk",
            intent="UPSERT_EVENT",
            source_fingerprint="fp-1",
            candidate_payload={"source_type": "vk"},
            max_attempts=2,
            lease_owner="worker-b",
        )
    await finish_candidate_attempt(
        db,
        first,
        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
        reason="technical_db_error",
        retry_delay_seconds=1,
    )
    second = await begin_candidate_attempt(
        db,
        candidate_key="candidate-ledger",
        occurrence_key="ordinal:4",
        canonical_source_url="https://vk.com/wall-1_4",
        source_type="vk",
        intent="UPSERT_EVENT",
        source_fingerprint="fp-1",
        candidate_payload={"source_type": "vk"},
        max_attempts=2,
        lease_owner="worker-b",
    )
    assert (first.attempt_no, second.attempt_no) == (1, 2)
    assert (first.attempt, second.attempt) == (1, 2)
    await finish_candidate_attempt(
        db,
        second,
        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
        reason="technical_db_error",
        retry_delay_seconds=1,
    )
    async with db.raw_conn() as conn:
        await conn.execute(
            "UPDATE smart_update_candidate_state SET next_retry_at=CURRENT_TIMESTAMP"
        )
        await conn.commit()
    claimed = await claim_due_candidates(db, lease_owner="worker-c")
    assert len(claimed) == 1
    assert claimed[0].attempts == 1
    counts = await smart_update_funnel_counts(db)
    assert counts["attempt_starts"] == counts["attempt_terminals"] == 2
    assert counts["attempt_unresolved"] == 0
    assert counts["retry_exhausted"] == 0
    await db.close()


@pytest.mark.asyncio
async def test_same_owner_replay_closes_interrupted_attempt_before_opening_next(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "interrupted-attempt.sqlite"))
    await db.init()
    first = await begin_candidate_attempt(
        db,
        candidate_key="candidate-interrupted",
        occurrence_key="ordinal:8",
        canonical_source_url="https://vk.com/wall-1_8",
        source_type="vk",
        intent="UPSERT_EVENT",
        source_fingerprint="fp-interrupted",
        candidate_payload={"source_type": "vk"},
        max_attempts=3,
        lease_owner="worker-a",
    )
    second = await begin_candidate_attempt(
        db,
        candidate_key="candidate-interrupted",
        occurrence_key="ordinal:8",
        canonical_source_url="https://vk.com/wall-1_8",
        source_type="vk",
        intent="UPSERT_EVENT",
        source_fingerprint="fp-interrupted",
        candidate_payload={"source_type": "vk"},
        max_attempts=3,
        lease_owner="worker-a",
    )
    assert (first.attempt_no, second.attempt_no) == (1, 2)
    await finish_candidate_attempt(
        db,
        second,
        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
        reason="replayed",
    )
    with sqlite3.connect(db.path) as conn:
        rows = conn.execute(
            "SELECT attempt_no,finished_at,terminal_outcome,reason "
            "FROM smart_update_attempt ORDER BY attempt_no"
        ).fetchall()
    assert rows[0][0] == 1
    assert rows[0][1] is not None
    assert rows[0][2:] == ("RETRY_SCHEDULED", "superseded_by_replay")
    assert rows[1][0] == 2
    assert rows[1][1] is not None
    counts = await smart_update_funnel_counts(db)
    assert counts["attempt_starts"] == counts["attempt_terminals"] == 2
    assert counts["attempt_unresolved"] == 0
    await db.close()
