from smart_event_update import (
    EventCandidate,
    SmartUpdateIntent,
    SmartUpdateResult,
    SmartUpdateTerminalOutcome,
)


def test_terminal_outcome_contract_is_closed():
    assert {item.name for item in SmartUpdateTerminalOutcome} == {
        "CREATED",
        "MERGED",
        "NOOP_EXACT_REPLAY",
        "REJECTED_PRODUCT_POLICY",
        "RETRY_SCHEDULED",
    }
    assert {item.value for item in SmartUpdateTerminalOutcome} == {
        "CREATED",
        "MERGED",
        "NOOP_EXACT_REPLAY",
        "REJECTED_PRODUCT_POLICY",
        "RETRY_SCHEDULED",
    }


def test_result_only_exposes_accepted_event_id_for_accepted_terminals():
    accepted = SmartUpdateResult(
        outcome=SmartUpdateTerminalOutcome.CREATED,
        event_id=42,
        attempt=1,
    )
    assert accepted.event_id == 42
    assert accepted.diagnostic_event_id is None
    assert accepted.is_accepted and accepted.is_changed and not accepted.is_retry

    retry = SmartUpdateResult(
        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
        event_id=42,
        diagnostic_event_id=7,
        reason="source_binding_conflict",
        attempt=2,
    )
    assert retry.event_id is None
    assert retry.diagnostic_event_id == 7
    assert retry.is_retry and not retry.is_accepted


def test_legacy_internal_status_is_normalized_at_result_boundary():
    rejected = SmartUpdateResult(status="skipped_non_event", event_id=9, reason="non_event")
    assert rejected.outcome is SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY
    assert rejected.event_id is None
    assert rejected.diagnostic_event_id == 9

    retry = SmartUpdateResult(status="review_required", event_id=11)
    assert retry.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
    assert retry.event_id is None
    assert retry.diagnostic_event_id == 11


def test_event_candidate_default_intent_is_upsert():
    candidate = EventCandidate(source_type="vk", source_url="https://vk.com/wall-1_2", source_text="x")
    assert candidate.intent is SmartUpdateIntent.UPSERT_EVENT
