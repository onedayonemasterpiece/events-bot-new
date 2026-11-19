"""A source occurrence can be retried through a different authorized intake."""

import json
import sqlite3
from dataclasses import asdict

import pytest
import pytest_asyncio

from db import Database
from models import Event, EventSource
from smart_event_update import EventCandidate, _exact_input_noop_event_id
from smart_update_identity import input_packet_fingerprint, stable_candidate_identity
from smart_update_state import (
    CandidateAttemptInProgress,
    RetryReason,
    SmartUpdateTerminalOutcome,
    begin_candidate_attempt,
    finish_candidate_attempt,
)


@pytest_asyncio.fixture
async def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_INIT_MINIMAL", "1")
    database = Database(str(tmp_path / "provenance.sqlite"))
    await database.init()
    try:
        yield database
    finally:
        await database.close()


def packet(source_type):
    candidate = EventCandidate(
        source_type=source_type,
        source_url="https://vk.com/wall-18582168_5882",
        source_role="identity_bearing",
        source_text="Конное шоу «Приключение Вани» 29 ноября в 15:00.",
        title="Конное шоу «Приключение Вани»",
        date="2026-11-29",
        time="15:00",
        location_name="Конный центр Rauschen",
        producer_ordinal=0,
    )
    candidate.candidate_key, candidate.occurrence_key = stable_candidate_identity(candidate)
    candidate.source_fingerprint = input_packet_fingerprint(candidate)
    return candidate


def attempt_arguments(candidate, owner):
    return dict(
        candidate_key=candidate.candidate_key,
        occurrence_key=candidate.occurrence_key,
        canonical_source_url=candidate.source_url,
        source_type=candidate.source_type,
        intent=candidate.intent.value,
        source_fingerprint=candidate.source_fingerprint,
        candidate_payload=asdict(candidate),
        lease_owner=owner,
    )


def ledger_snapshot(db):
    with sqlite3.connect(db.path) as conn:
        return (
            conn.execute("SELECT * FROM smart_update_candidate_state ORDER BY id").fetchall(),
            conn.execute("SELECT * FROM smart_update_attempt ORDER BY id").fetchall(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("old_type", "new_type", "prior_outcome"),
    [
        ("vk", "manual", SmartUpdateTerminalOutcome.FAILED_TECHNICAL),
        ("vk", "manual", SmartUpdateTerminalOutcome.RETRY_SCHEDULED),
        ("manual", "manual", SmartUpdateTerminalOutcome.FAILED_TECHNICAL),
        ("manual", "vk", SmartUpdateTerminalOutcome.FAILED_TECHNICAL),
    ],
)
async def test_same_url_occurrence_registers_next_attempt_with_current_provenance(
    db, old_type, new_type, prior_outcome,
):
    old, new = packet(old_type), packet(new_type)
    assert (old.candidate_key, old.occurrence_key) == (new.candidate_key, new.occurrence_key)
    first = await begin_candidate_attempt(db, **attempt_arguments(old, "first"))
    await finish_candidate_attempt(
        db, first, outcome=prior_outcome, reason="original_failure",
        retry_reason=RetryReason.IDENTITY_FINAL_RETRY,
    )
    with sqlite3.connect(db.path) as conn:
        assert conn.execute(
            "SELECT current_outcome FROM smart_update_candidate_state"
        ).fetchone() == (prior_outcome.value,)

    second = await begin_candidate_attempt(db, **attempt_arguments(new, "second"))

    assert second.candidate_state_id == first.candidate_state_id
    assert (first.attempt_no, second.attempt_no) == (1, 2)
    with sqlite3.connect(db.path) as conn:
        row = conn.execute(
            "SELECT source_type,source_fingerprint,candidate_payload,attempts "
            "FROM smart_update_candidate_state"
        ).fetchone()
        assert (row[0], row[1], row[3]) == (new_type, new.source_fingerprint, 2)
        assert json.loads(row[2])["source_type"] == new_type
        assert conn.execute(
            "SELECT attempt_no,terminal_outcome FROM smart_update_attempt ORDER BY attempt_no"
        ).fetchall() == [(1, prior_outcome.value), (2, "RETRY_SCHEDULED")]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["url", "occurrence", "intent", "live_lease", "null_url", "empty_url"])
async def test_provenance_change_cannot_bypass_identity_or_live_lease(db, change):
    original = attempt_arguments(packet("vk"), "first")
    if change in {"null_url", "empty_url"}:
        original["canonical_source_url"] = None if change == "null_url" else ""
    first = await begin_candidate_attempt(db, **original)
    if change != "live_lease":
        await finish_candidate_attempt(
            db, first, outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
            reason="original_failure",
        )
    before = ledger_snapshot(db)
    requested = {
        **original,
        "source_type": "manual",
        "source_fingerprint": "changed-packet",
        "candidate_payload": {"source_type": "manual", "source_text": "Changed input"},
        "lease_owner": "other-owner",
    }
    if change == "url":
        requested["canonical_source_url"] = "https://vk.com/wall-18582168_9999"
    elif change == "occurrence":
        requested["occurrence_key"] = "source-native:different-session"
    elif change == "intent":
        requested["intent"] = "ATTACH_CONTEXT"
    error = CandidateAttemptInProgress if change == "live_lease" else RuntimeError
    reason = "candidate_attempt_in_progress" if change == "live_lease" else "candidate_key_identity_collision"
    with pytest.raises(error, match=reason):
        await begin_candidate_attempt(db, **requested)
    assert ledger_snapshot(db) == before


@pytest.mark.asyncio
async def test_accepted_vk_packet_does_not_become_exact_noop_for_manual_intake(db):
    old, new = packet("vk"), packet("manual")
    assert old.candidate_key == new.candidate_key
    assert old.source_fingerprint != new.source_fingerprint
    first = await begin_candidate_attempt(db, **attempt_arguments(old, "first"))
    async with db.get_session() as session:
        session.add(Event(
            id=42, title=old.title, description="Original description", date=old.date,
            time=old.time, location_name=old.location_name, source_text=old.source_text,
        ))
        session.add(EventSource(
            event_id=42, source_type="vk", source_url=old.source_url,
            canonical_source_url=old.source_url, source_role="identity_bearing",
            candidate_key=old.candidate_key, occurrence_key=old.occurrence_key,
            source_fingerprint=old.source_fingerprint,
        ))
        await session.commit()
    await finish_candidate_attempt(
        db, first, outcome=SmartUpdateTerminalOutcome.CREATED, event_id=42,
    )
    lookup = dict(
        canonical_source_url=old.source_url, source_role="identity_bearing",
        occurrence_key=old.occurrence_key, candidate_key=old.candidate_key,
    )
    assert await _exact_input_noop_event_id(
        db, source_fingerprint=old.source_fingerprint, **lookup,
    ) == (42, False)
    with sqlite3.connect(db.path) as conn:
        original_source = conn.execute("SELECT * FROM event_source").fetchall()

    second = await begin_candidate_attempt(db, **attempt_arguments(new, "second"))

    assert second.attempt_no == 2
    assert new.source_type == "manual"
    assert await _exact_input_noop_event_id(
        db, source_fingerprint=new.source_fingerprint, **lookup,
    ) == (None, False)
    with sqlite3.connect(db.path) as conn:
        assert conn.execute("SELECT * FROM event_source").fetchall() == original_source
        assert conn.execute(
            "SELECT source_type,source_fingerprint FROM smart_update_candidate_state"
        ).fetchone() == ("manual", new.source_fingerprint)
