from __future__ import annotations

import copy

import pytest

from scripts.region_talk_external_publication_review import (
    REVIEW_SCHEMA_VERSION,
    ReviewError,
    apply_review,
)


def _row() -> dict:
    return {
        "external_publication_id": "extpub_test",
        "canonical_url": "https://example.org/article",
        "research_window_start": "2025-07-01",
        "research_window_end": "2026-07-01",
        "publication": {
            "published_at": None,
            "date_precision": "year",
            "date_basis": "issue_metadata",
            "access_status": "full_text",
        },
        "source_assessment": {"scope": "external"},
        "policy_classification": {
            "product_policy_match": True,
            "language_policy_match": True,
            "hard_exclusion_codes": [],
        },
        "quality_assessment": {
            "track": "scholarly",
            "quality_tier": "credible",
            "kaliningrad_centrality": {"score": 4},
            "public_interest": {"score": 3},
            "accessibility": {"score": 3},
            "scholarly_details": {
                "publication_status": "peer_reviewed",
                "correction_status": "not_checked",
                "funding_disclosed": "yes",
                "conflicts_disclosed": "yes",
                "limitations_visible": False,
            },
        },
        "decision": {
            "research_decision": "needs_review",
            "downstream_readiness": "manual_review_required",
            "reason_codes": ["exact_publication_day_unverified", "scholarly_postpublication_check_pending"],
            "import_status": "manual_review_required",
        },
    }


def _review() -> dict:
    return {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "external_publication_id": "extpub_test",
        "decision": "approve",
        "reviewer": "test-reviewer",
        "reviewed_at": "2026-07-31T10:30:00+00:00",
        "reason": "Primary metadata resolves the two blocking checks.",
        "resolved_reason_codes": [
            "exact_publication_day_unverified",
            "scholarly_postpublication_check_pending",
        ],
        "evidence": [{
            "url": "https://api.crossref.org/works/example",
            "supports": ["publication.published_at", "correction_status"],
            "note": "DOI metadata gives the date and no update relation.",
        }],
        "updates": {
            "publication.published_at": "2026-03-06",
            "publication.date_precision": "day",
            "publication.date_basis": "doi_metadata",
            "quality_assessment.scholarly_details.correction_status": "none_found",
        },
    }


def test_apply_review_promotes_only_after_all_checks() -> None:
    original = _row()
    updated, attestation = apply_review(original, _review())
    assert original["publication"]["published_at"] is None
    assert updated["decision"]["import_status"] == "ready_for_region_talk_scoring"
    assert updated["publication"]["published_at"] == "2026-03-06"
    assert updated["operator_policy_override"]["decision"] == "approved"
    assert attestation["review_id"].startswith("extpubreview_")


def test_apply_review_rejects_unresolved_reason() -> None:
    review = _review()
    review["resolved_reason_codes"] = ["exact_publication_day_unverified"]
    with pytest.raises(ReviewError, match="missing: scholarly_postpublication_check_pending"):
        apply_review(_row(), review)


def test_apply_review_rejects_hard_exclusion() -> None:
    row = copy.deepcopy(_row())
    row["policy_classification"]["hard_exclusion_codes"] = ["sharp_negative_region_image"]
    with pytest.raises(ReviewError, match="hard exclusions"):
        apply_review(row, _review())


def test_apply_review_block_is_auditable_without_field_updates() -> None:
    review = _review()
    review["decision"] = "block"
    review["updates"] = {}
    updated, attestation = apply_review(_row(), review)
    assert updated["decision"]["import_status"] == "research_only_blocked"
    assert updated["operator_policy_override"]["decision"] == "blocked"
    assert attestation["evidence"]
