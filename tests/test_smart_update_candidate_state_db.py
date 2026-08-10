import sqlite3

import pytest

from db import Database
from smart_update_state import (
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
