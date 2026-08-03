from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scripts.run_static_collection_facts_v3_fallback_drill import (
    REPORT_SCHEMA_VERSION,
    run_drill,
    run_scenario,
)


@pytest.mark.asyncio
async def test_valid_fallback_uses_one_send_and_production_validator_apply() -> None:
    result = await run_scenario("valid_fallback")

    assert result["status"] == "pass"
    assert result["provider"]["primary"]["forced_failure"] is True
    assert result["provider"]["primary"]["physical_sends"] == 1
    assert result["provider"]["fallback"]["physical_sends"] == 1
    assert result["provider"]["trace"]["physical_sends"] == 2
    assert result["provider"]["trace"]["actual_models"] == [
        "models/gemma-4-31b-it",
        "gpt-4o",
    ]
    assert result["validator_result"] == "accepted"
    assert result["apply_attempted"] is True
    assert result["applied"] is True
    assert result["existing_truth_preserved"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["malformed_fallback", "evidence_mismatch"])
async def test_invalid_fallback_fails_closed_without_apply(scenario: str) -> None:
    result = await run_scenario(scenario)

    assert result["status"] == "pass"
    assert result["provider"]["primary"]["physical_sends"] == 1
    assert result["provider"]["fallback"]["physical_sends"] == 1
    assert result["validator_result"] == "rejected"
    assert result["apply_attempted"] is False
    assert result["applied"] is False
    assert result["existing_truth_preserved"] is True
    assert result["whole_collection_unchanged"] is True
    assert result["before_sha256"] == result["after_sha256"]


@pytest.mark.asyncio
async def test_total_unavailable_abstains_and_preserves_truth() -> None:
    result = await run_scenario("total_unavailable")

    assert result["status"] == "pass"
    assert result["provider"]["primary"]["physical_sends"] == 1
    assert result["provider"]["fallback"]["physical_sends"] == 1
    assert result["provider"]["trace"]["status"] == "failed"
    assert result["validator_result"] == "abstained"
    assert result["apply_attempted"] is False
    assert result["existing_truth_preserved"] is True
    assert result["before_sha256"] == result["after_sha256"]


@pytest.mark.asyncio
async def test_report_is_explicitly_offline_bounded_and_not_gate_c_claim() -> None:
    command = "python3 scripts/run_static_collection_facts_v3_fallback_drill.py --output out.json"
    report = await run_drill(
        generated_at=datetime(2026, 8, 2, tzinfo=timezone.utc),
        generator_command=command,
    )

    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["generated_at"] == "2026-08-02T00:00:00Z"
    assert report["transport_mode"] == "offline_injected_no_network"
    assert report["status"] == "pass"
    assert report["summary"] == {
        "cases": 4,
        "passed": 4,
        "failed": 0,
        "real_provider_calls": 0,
        "maximum_primary_sends_per_case": 1,
        "maximum_fallback_sends_per_case": 1,
    }
    assert report["gate_claim"] == "offline_harness_only_real_gate_c_not_claimed"
    assert report["publication_status"] == "blocked"
    assert report == await run_drill(
        generated_at=datetime(2026, 8, 2, tzinfo=timezone.utc),
        generator_command=command,
    )
