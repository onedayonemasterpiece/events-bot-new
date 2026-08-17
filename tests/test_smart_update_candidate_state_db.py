import asyncio
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
    terminalize_claimed_candidate_technical,
)


@pytest.mark.asyncio
async def test_candidate_terminal_ack_waits_for_unrelated_raw_writer(
    tmp_path,
    monkeypatch,
):
    """An unrelated raw transaction must not own Smart Update's connection."""

    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    monkeypatch.setenv("DB_TIMEOUT_SEC", "2")
    db = Database(str(tmp_path / "candidate-ack-contention.sqlite"))
    await db.init()
    receipt = await begin_candidate_attempt(
        db,
        candidate_key="candidate-ack-contention",
        occurrence_key="ordinal:17",
        canonical_source_url="https://tickets.example/event/17",
        source_type="parser:test",
        intent="UPSERT_EVENT",
        source_fingerprint="fp-contention",
        candidate_payload={"source_type": "parser:test"},
        lease_owner="source-parser",
    )

    blocker_entered = asyncio.Event()
    release_blocker = asyncio.Event()

    async def unrelated_writer() -> None:
        async with db.raw_conn() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            await conn.execute(
                "INSERT INTO setting(key, value) VALUES('ack-blocker', 'held')"
            )
            blocker_entered.set()
            await release_blocker.wait()
            await conn.commit()

    blocker = asyncio.create_task(unrelated_writer())
    await asyncio.wait_for(blocker_entered.wait(), timeout=1)
    acknowledgement = asyncio.create_task(
        finish_candidate_attempt(
            db,
            receipt,
            outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
            reason="explicit_terminal",
        )
    )
    await asyncio.sleep(0.05)
    assert not acknowledgement.done()

    release_blocker.set()
    await asyncio.wait_for(asyncio.gather(blocker, acknowledgement), timeout=2)
    with sqlite3.connect(db.path) as conn:
        assert conn.execute(
            "SELECT current_outcome,reason,claimed_by FROM smart_update_candidate_state "
            "WHERE candidate_key='candidate-ack-contention'"
        ).fetchone() == ("FAILED_TECHNICAL", "explicit_terminal", None)
        assert conn.execute(
            "SELECT terminal_outcome,reason,finished_at IS NOT NULL "
            "FROM smart_update_attempt WHERE candidate_state_id=? AND attempt_no=?",
            (receipt.candidate_state_id, receipt.attempt_no),
        ).fetchone() == ("FAILED_TECHNICAL", "explicit_terminal", 1)
    await db.close()


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
    assert counts["FAILED_TECHNICAL"] == 1
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
        assert attempt_rows == [(1, "FAILED_TECHNICAL")]
    await db.close()


@pytest.mark.asyncio
async def test_completed_technical_failure_is_not_claimable(tmp_path, monkeypatch):
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
    assert await claim_due_candidates(db, lease_owner="worker-a") == []
    with sqlite3.connect(db.path) as conn:
        assert conn.execute(
            "SELECT current_outcome,retry_exhausted,next_retry_at FROM smart_update_candidate_state"
        ).fetchone() == ("FAILED_TECHNICAL", 1, None)
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
    assert (first.attempt, second.attempt) == (1, 1)
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
    assert claimed == []
    counts = await smart_update_funnel_counts(db)
    assert counts["attempt_starts"] == counts["attempt_terminals"] == 2
    assert counts["attempt_unresolved"] == 0
    assert counts["retry_exhausted"] == 1
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
    assert rows[0][2:] == ("FAILED_TECHNICAL", "interrupted_before_ack")
    assert rows[1][0] == 2
    assert rows[1][1] is not None
    counts = await smart_update_funnel_counts(db)
    assert counts["attempt_starts"] == counts["attempt_terminals"] == 2
    assert counts["attempt_unresolved"] == 0
    await db.close()


@pytest.mark.asyncio
async def test_expired_max_attempt_legacy_claim_is_recovered_once(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    db = Database(str(tmp_path / "legacy-max.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO smart_update_candidate_state("
            "candidate_key,occurrence_key,source_type,intent,source_fingerprint,candidate_payload,"
            "current_outcome,attempts,retry_attempts,max_attempts,next_retry_at,claimed_by,claim_expires_at"
            ") VALUES('legacy-max','ordinal:9','vk','UPSERT_EVENT','fp','{}',"
            "'RETRY_SCHEDULED',3,3,3,CURRENT_TIMESTAMP,'dead-worker',datetime(CURRENT_TIMESTAMP,'-1 minute'))"
        )
        state_id = (await (await conn.execute(
            "SELECT id FROM smart_update_candidate_state WHERE candidate_key='legacy-max'"
        )).fetchone())[0]
        await conn.execute(
            "INSERT INTO smart_update_attempt(candidate_state_id,attempt_no,terminal_outcome,reason) "
            "VALUES(?,3,'RETRY_SCHEDULED','attempt_started')",
            (state_id,),
        )
        await conn.commit()

    claimed = await claim_due_candidates(db, lease_owner="recovery", limit=1)
    assert len(claimed) == 1
    assert claimed[0].attempts == 3
    await terminalize_claimed_candidate_technical(
        db,
        candidate_state_id=state_id,
        lease_owner="recovery",
        reason="legacy_recovery_payload_invalid",
    )
    assert await claim_due_candidates(db, lease_owner="other", limit=1) == []
    with sqlite3.connect(db.path) as conn:
        assert conn.execute(
            "SELECT current_outcome,retry_exhausted,claimed_by FROM smart_update_candidate_state WHERE id=?",
            (state_id,),
        ).fetchone() == ("FAILED_TECHNICAL", 1, None)
        assert conn.execute(
            "SELECT terminal_outcome,finished_at IS NOT NULL FROM smart_update_attempt WHERE candidate_state_id=?",
            (state_id,),
        ).fetchone() == ("FAILED_TECHNICAL", 1)
    await db.close()
