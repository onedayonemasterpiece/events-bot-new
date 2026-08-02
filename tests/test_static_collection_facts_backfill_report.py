from __future__ import annotations

import copy
import json

import jsonschema
import pytest

from scripts import backfill_static_collection_facts as backfill


def _valid_report():
    return {
        "schema_version": backfill.REPORT_SCHEMA_VERSION,
        "repo_sha": "a" * 40,
        "generator_command": (
            "python3 scripts/backfill_static_collection_facts.py --evaluate "
            "--event-id 7326 --primary-only"
        ),
        "mode": "evaluate",
        "primary_only": True,
        "facts_policy_version": "static-collection-facts-v3",
        "adjudication_schema_version": "static-collection-adjudication-v2",
        "started_at": "2026-08-02T12:00:00Z",
        "finished_at": "2026-08-02T12:00:01Z",
        "db_snapshot": {
            "path": "/redacted/fresh-production-copy.sqlite",
            "sha256_before": "b" * 64,
            "sha256_after": "b" * 64,
            "quick_check_before": "ok",
            "quick_check_after": "ok",
        },
        "selection": {
            "current_date": "2026-08-02",
            "reasons": ["audience"],
            "requested_event_ids": [7326],
            "resolved_event_ids": [7326],
            "unresolved_event_ids": [],
            "eligible_event_count": 1,
            "selected_event_count": 1,
            "selection_truncated": False,
            "omitted_event_ids": [],
            "max_sources_per_event": 2,
            "requested_source_ids": [9001],
            "requested_source_bindings": [{"event_id": 7326, "source_id": 9001}],
        },
        "plan": [
            {
                "event_id": 7326,
                "reasons": ["audience"],
                "source_ids": [9001],
                "unselected_source_ids": [],
            }
        ],
        "execution": {
            "attempted_sources": 1,
            "provider_calls": 1,
            "physical_sends": 1,
            "physical_sends_complete": True,
            "writes": 0,
            "cached_sources": 0,
            "deferred_sources": 0,
            "evaluated_sources": 1,
            "applied_sources": 0,
            "unchanged_sources": 0,
            "events": [
                {
                    "event_id": 7326,
                    "reasons": ["audience"],
                    "selected_source_ids": [9001],
                    "unselected_source_ids": [],
                    "sources": [
                        {
                            "source_id": 9001,
                            "input_hash": "c" * 64,
                            "provider_called": True,
                            "write_status": "not_requested",
                            "changed_keys": [],
                            "status": "evaluated",
                            "trace": {
                                "logical_calls": 1,
                                "physical_sends": 1,
                                "requested_model": "gemma-4-31b-it",
                                "actual_model_path": "gemma-4-31b-it",
                                "fallback_used": False,
                                "attempts": 1,
                                "rate_limit_waits": 0,
                                "input_tokens": 500,
                                "output_tokens": 180,
                                "latency_sec": 0.5,
                                "statuses": ["ok"],
                            },
                            "validated_outcomes": {
                                "child_directed_decision": {"value": "unknown"},
                                "family_suitable_decision": {"value": "confirmed"},
                                "joint_family_activity_decision": {"value": "unknown"},
                            },
                            "legacy_projection": {
                                "value": "family",
                                "derived_from_facts_v3": True,
                            },
                        }
                    ],
                }
            ],
        },
        "logical_diff": {
            "sha256_before": "d" * 64,
            "sha256_after": "d" * 64,
            "changed_event_ids": [],
            "changed_event_source_ids": [],
            "selected_event_allowlist_ok": True,
        },
    }


def test_versioned_report_schema_is_valid_and_accepts_canonical_report():
    schema = json.loads(backfill.REPORT_SCHEMA_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    backfill.validate_report_schema(_valid_report())


@pytest.mark.parametrize(
    ("path", "bad_value"),
    [
        (("schema_version",), "static-collection-facts-backfill-v1"),
        (("db_snapshot", "sha256_before"), "not-a-hash"),
        (("selection", "requested_event_ids"), [7326, 7326]),
        (("execution", "provider_calls"), -1),
    ],
)
def test_report_contract_rejects_version_hash_id_and_count_mismatches(path, bad_value):
    report = copy.deepcopy(_valid_report())
    target = report
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = bad_value
    with pytest.raises(jsonschema.ValidationError):
        backfill.validate_report_schema(report)


def test_report_contract_rejects_unredacted_or_unknown_extra_trace_fields():
    report = _valid_report()
    report["execution"]["events"][0]["sources"][0]["trace"]["api_key"] = "secret"
    with pytest.raises(jsonschema.ValidationError):
        backfill.validate_report_schema(report)
