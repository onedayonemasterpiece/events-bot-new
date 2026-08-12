from smart_event_update import SmartUpdateResult
from smart_update_state import (
    IdentityDistinctReason,
    LifecycleReason,
    ProductExclusionReason,
    RetryReason,
    SmartUpdateTerminalOutcome,
)


def test_free_form_or_substring_reason_cannot_authorize_product_terminal() -> None:
    result = SmartUpdateResult(
        status="skipped_non_event",
        reason="definitely_non_event_without_provider_timeout_words",
    )

    assert result.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
    assert result.retry_reason is RetryReason.UNKNOWN
    assert result.product_exclusion_reason is None


def test_closed_product_reason_authorizes_product_terminal() -> None:
    result = SmartUpdateResult(
        status="skipped_non_event",
        reason="arbitrary diagnostic prose",
        product_exclusion_reason=ProductExclusionReason.COMPLETED_EVENT_REPORT,
    )

    assert result.outcome is SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY
    assert result.product_exclusion_reason is ProductExclusionReason.COMPLETED_EVENT_REPORT


def test_explicit_untyped_product_outcome_fails_closed_to_retry() -> None:
    result = SmartUpdateResult(
        outcome=SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY,
        reason="unknown_new_reason",
    )

    assert result.outcome is SmartUpdateTerminalOutcome.RETRY_SCHEDULED
    assert result.retry_reason is RetryReason.PRODUCT_REASON_UNTYPED


def test_diagnostic_event_id_never_becomes_accepted_event_id() -> None:
    result = SmartUpdateResult(
        outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
        event_id=77,
        retry_reason=RetryReason.IDENTITY_TECHNICAL_FAILURE,
    )

    assert result.event_id is None
    assert result.diagnostic_event_id == 77
    assert not result.is_accepted
    assert not result.is_changed


def test_closed_identity_and_lifecycle_fields_are_structural() -> None:
    created = SmartUpdateResult(
        outcome=SmartUpdateTerminalOutcome.CREATED,
        event_id=12,
        identity_distinct_reason=IdentityDistinctReason.RELATED_BUT_DISTINCT,
    )
    attached = SmartUpdateResult(
        outcome=SmartUpdateTerminalOutcome.MERGED,
        event_id=12,
        lifecycle_reason=LifecycleReason.CONTEXT_PROVENANCE_ATTACHED,
    )

    assert created.identity_distinct_reason is IdentityDistinctReason.RELATED_BUT_DISTINCT
    assert attached.lifecycle_reason is LifecycleReason.CONTEXT_PROVENANCE_ATTACHED


def test_far_future_ocr_conflict_cannot_silence_positive_child() -> None:
    import inspect
    import smart_event_update as module

    source = inspect.getsource(module._smart_event_update_impl)
    assert "force_silent_due_to_date_risk" not in source
    assert "silent=False" in source
    note_source = inspect.getsource(module._far_future_poster_date_mismatch_note)
    assert "event.silent=1" not in note_source
