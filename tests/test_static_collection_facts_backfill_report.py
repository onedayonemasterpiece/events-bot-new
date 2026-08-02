from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from pathlib import Path

import jsonschema
import pytest

from scripts import backfill_static_collection_facts as backfill
from scripts import evaluate_static_collection_facts_v3_gate_b as gate_b


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


def _canonical_hash(value):
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, value) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _gate_fixture(tmp_path: Path, *, misses: int = 0):
    """Build a self-contained source-bound 5-family-per-label Gate-B replay."""

    db_path = tmp_path / "snapshot.sqlite"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE event (id INTEGER PRIMARY KEY);
        CREATE TABLE event_source (
            id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            source_type TEXT,
            source_url TEXT,
            trust_level TEXT,
            source_chat_username TEXT,
            source_message_id INTEGER,
            source_text TEXT
        );
        """
    )
    rows_by_label = {label: [] for label in gate_b.TARGET_LABELS}
    receipt_evidence = []
    runtime_events = []
    event_ids = []
    source_ids = []
    counter = 0
    for label, decision_key in gate_b.TARGET_LABELS.items():
        for family_offset in range(5):
            counter += 1
            event_id = 1000 + counter
            source_id = 9000 + counter
            text = f"Источник {label} семейство {family_offset}: точная проверяемая цитата."
            source_url = f"https://example.test/{event_id}"
            connection.execute("INSERT INTO event(id) VALUES (?)", (event_id,))
            connection.execute(
                """INSERT INTO event_source(
                    id,event_id,source_type,source_url,trust_level,
                    source_chat_username,source_message_id,source_text
                ) VALUES (?,?,?,?,?,?,?,?)""",
                (source_id, event_id, "fixture", source_url, "high", None, None, text),
            )
            source_ref = {
                "event_id": event_id,
                "source_id": source_id,
                "source_type": "fixture",
                "source_url": source_url,
                "trust_level": "high",
                "source_chat_username": None,
                "source_message_id": None,
                "source_text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                "source_text_char_count": len(text),
                "source_record_sha256": "f" * 64,
            }
            source_ref["source_ref_sha256"] = gate_b._source_ref_hash(source_ref)
            quote_contract = {
                "source_quote": text,
                "source_quote_sha256": hashlib.sha256(text.encode()).hexdigest(),
                "source_quote_kind": "full",
                "source_quote_truncated": False,
                "source_quote_start_char": 0,
                "source_quote_end_char": len(text),
                "source_quote_char_count": len(text),
                "source_quote_omitted_prefix_chars": 0,
                "source_quote_omitted_suffix_chars": 0,
            }
            rows_by_label[label].append(
                {
                    "event_id": event_id,
                    "family_id": f"family:{event_id}",
                    "occurrence_member_ids": [event_id],
                    "expected": "positive_candidate",
                    "confidence": "high",
                    "review_decision": "keep",
                    "source_status": "sufficient",
                    "source_refs": [source_ref],
                    **quote_contract,
                }
            )
            receipt_evidence.append(
                {
                    "event_id": event_id,
                    "source_ref": source_ref,
                    **{
                        key.replace("source_quote", "raw_source_quote", 1): value
                        for key, value in quote_contract.items()
                    },
                }
            )
            outcome = "unknown" if family_offset < misses else "confirmed"
            decisions = {
                key: {"value": "denied", "evidence_quote": text}
                for key in gate_b.TARGET_LABELS.values()
            }
            decisions[decision_key] = {"value": outcome}
            if outcome != "unknown":
                decisions[decision_key]["evidence_quote"] = text
            if label == "joint_family_activity" and outcome == "confirmed":
                for required in ("child_directed_decision", "family_suitable_decision"):
                    decisions[required] = {"value": "confirmed", "evidence_quote": text}
            runtime_events.append(
                {
                    "event_id": event_id,
                    "reasons": ["audience"],
                    "selected_source_ids": [source_id],
                    "unselected_source_ids": [],
                    "sources": [
                        {
                            "source_id": source_id,
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
                                "input_tokens": 20,
                                "output_tokens": 10,
                                "latency_sec": 0.1,
                                "statuses": ["ok"],
                            },
                            "validated_outcomes": decisions,
                            "legacy_projection": {},
                        }
                    ],
                }
            )
            event_ids.append(event_id)
            source_ids.append(source_id)
    connection.commit()
    connection.close()

    review_dir = tmp_path / "source-reviews"
    review_dir.mkdir()
    receipt = {
        "schema_version": "static-collection-source-review-v1",
        "receipt_id": "gate-fixture",
        "reviewed_at": "2026-08-02T12:00:00Z",
        "status": "pass",
        "event_ids": event_ids,
        "source_evidence": receipt_evidence,
    }
    receipt["receipt_sha256"] = _canonical_hash(receipt)
    receipt_path = review_dir / "gate-fixture.json"
    _write_json(receipt_path, receipt)
    snapshot_hash = "e" * 64
    index = {
        "schema_version": "static-collection-source-review-index-v1",
        "reviewed_at": "2026-08-02T12:00:00Z",
        "source_snapshot_sha256": snapshot_hash,
        "required_event_ids": event_ids,
        "receipts": [
            {
                "receipt_id": receipt["receipt_id"],
                "path": receipt_path.name,
                "status": receipt["status"],
                "receipt_sha256": receipt["receipt_sha256"],
                "event_ids": event_ids,
            }
        ],
    }
    index["index_sha256"] = _canonical_hash(index)
    index_path = review_dir / "index.json"
    _write_json(index_path, index)

    seed = {
        "schema_version": "static-collections-review-seed-v1",
        "publication_eligible": False,
        "evidence_snapshot_sha256": snapshot_hash,
        "source": {
            "source_review_index_sha256": index["index_sha256"],
            "generator_command": (
                "python3 scripts/build_static_collections_review_seed.py "
                "--seed docs/review-data/static_collections_review_seed_v1.json "
                "--snapshot artifacts/codex/static-collections-pr-a/"
                "static-collections-evidence-snapshot-v1.json --output /tmp/seed.json"
            ),
            "extracted_at": "2026-08-02T11:00:00Z",
            "reviewed_at": "2026-08-02T12:00:00Z",
            "extraction_repo_sha": "a" * 40,
            "seed_builder_repo_sha": "b" * 40,
            "integration_repo_sha": "c" * 40,
        },
        "snapshot_contract": {
            **gate_b.SNAPSHOT_CONTRACT,
            "json_options": gate_b.SNAPSHOT_JSON_OPTIONS,
            "event_count": len(event_ids),
            "event_source_count": len(source_ids),
            "db_file_mtime": "2026-08-02T10:00:00Z",
        },
        "labels": {
            label: {"positives": rows, "hard_negatives": []}
            for label, rows in rows_by_label.items()
        },
    }
    seed_path = tmp_path / "seed.json"
    _write_json(seed_path, seed)

    repo_sha = gate_b._repo_sha()
    db_hash = gate_b._sha256_file(db_path)
    report = _valid_report()
    report.update(repo_sha=repo_sha)
    report["db_snapshot"].update(
        path=str(db_path), sha256_before=db_hash, sha256_after=db_hash
    )
    report["selection"].update(
        requested_event_ids=event_ids,
        resolved_event_ids=event_ids,
        eligible_event_count=len(event_ids),
        selected_event_count=len(event_ids),
        requested_source_ids=source_ids,
        requested_source_bindings=[
            {"event_id": event_id, "source_id": source_id}
            for event_id, source_id in zip(event_ids, source_ids)
        ],
    )
    report["plan"] = [
        {
            "event_id": event_id,
            "reasons": ["audience"],
            "source_ids": [source_id],
            "unselected_source_ids": [],
        }
        for event_id, source_id in zip(event_ids, source_ids)
    ]
    report["execution"].update(
        attempted_sources=len(source_ids),
        provider_calls=len(source_ids),
        physical_sends=len(source_ids),
        evaluated_sources=len(source_ids),
        events=runtime_events,
    )
    report_path = tmp_path / "report.json"
    _write_json(report_path, report)
    return {
        "db": db_path,
        "seed": seed_path,
        "index": index_path,
        "report": report_path,
        "repo_sha": repo_sha,
    }


def _evaluate(paths):
    return gate_b.evaluate_gate_b(
        report_path=paths["report"],
        seed_path=paths["seed"],
        source_review_index_path=paths["index"],
        db_path=paths["db"],
        boundary_manifest_path=paths.get("boundary_manifest"),
        expected_repo_sha=paths["repo_sha"],
    )


def _add_boundary(
    paths,
    *,
    expectation="not_confirmed",
    runtime_outcome="denied",
    bind=True,
):
    event_id = 2000
    source_id = 9900
    text = "Граничный реальный источник с точной цитатой."
    connection = sqlite3.connect(paths["db"])
    connection.execute("INSERT INTO event(id) VALUES (?)", (event_id,))
    connection.execute(
        """INSERT INTO event_source(
            id,event_id,source_type,source_url,trust_level,
            source_chat_username,source_message_id,source_text
        ) VALUES (?,?,?,?,?,?,?,?)""",
        (
            source_id,
            event_id,
            "fixture",
            "https://example.test/boundary",
            "high",
            None,
            None,
            text,
        ),
    )
    connection.commit()
    connection.close()
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    db_hash = gate_b._sha256_file(paths["db"])
    report["db_snapshot"].update(sha256_before=db_hash, sha256_after=db_hash)
    report["selection"]["requested_event_ids"].append(event_id)
    report["selection"]["resolved_event_ids"].append(event_id)
    report["selection"]["requested_source_ids"].append(source_id)
    report["selection"]["requested_source_bindings"].append(
        {"event_id": event_id, "source_id": source_id}
    )
    report["selection"]["eligible_event_count"] += 1
    report["selection"]["selected_event_count"] += 1
    report["plan"].append(
        {
            "event_id": event_id,
            "reasons": ["audience"],
            "source_ids": [source_id],
            "unselected_source_ids": [],
        }
    )
    decisions = {
        key: {"value": "denied", "evidence_quote": text}
        for key in gate_b.TARGET_LABELS.values()
    }
    decisions["child_directed_decision"] = {"value": runtime_outcome}
    if runtime_outcome in {"confirmed", "denied"}:
        decisions["child_directed_decision"]["evidence_quote"] = text
    report["execution"]["events"].append(
        {
            "event_id": event_id,
            "reasons": ["audience"],
            "selected_source_ids": [source_id],
            "unselected_source_ids": [],
            "sources": [
                {
                    "source_id": source_id,
                    "input_hash": "d" * 64,
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
                        "input_tokens": 20,
                        "output_tokens": 10,
                        "latency_sec": 0.1,
                        "statuses": ["ok"],
                    },
                    "validated_outcomes": decisions,
                    "legacy_projection": {},
                }
            ],
        }
    )
    for key in ("attempted_sources", "provider_calls", "physical_sends", "evaluated_sources"):
        report["execution"][key] += 1
    _write_json(paths["report"], report)
    if bind:
        manifest = {
            "schema_version": "static-collection-facts-v3-boundary-manifest-v1",
            "seed_sha256": gate_b._sha256_file(paths["seed"]),
            "boundaries": [
                {
                    "boundary_id": "named-2000-child",
                    "event_id": event_id,
                    "source_id": source_id,
                    "source_text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                    "label": "child_directed",
                    "expectation": expectation,
                }
            ],
        }
        manifest_path = paths["report"].parent / "boundary-manifest.json"
        _write_json(manifest_path, manifest)
        paths["boundary_manifest"] = manifest_path
    return paths


def _mutate_json(path: Path, mutate) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    mutate(value)
    _write_json(path, value)


def _family(result, label, offset):
    return result["labels"][label]["families"][offset]


def test_gate_b_four_of_five_eligible_families_pass_and_hashes_are_bound(tmp_path):
    paths = _gate_fixture(tmp_path, misses=1)
    result = _evaluate(paths)
    assert result["status"] == "pass"
    assert result["copy_gates_allowed"] is True
    assert result["publication_status"] == "blocked"
    assert all(payload["recall"] == 0.8 for payload in result["labels"].values())
    assert result["hashes"]["report_file_sha256"] == gate_b._sha256_file(paths["report"])
    assert result["hashes"]["seed_file_sha256"] == gate_b._sha256_file(paths["seed"])
    assert result["hashes"]["source_review_index_file_sha256"] == gate_b._sha256_file(paths["index"])
    assert result["hashes"]["db_file_sha256"] == gate_b._sha256_file(paths["db"])
    assert result["hashes"]["expected_repo_sha"] == paths["repo_sha"]


def test_gate_b_three_of_five_blocks_copy_gates(tmp_path):
    result = _evaluate(_gate_fixture(tmp_path, misses=2))
    assert result["status"] == "blocked"
    assert result["copy_gates_allowed"] is False
    assert all(payload["recall"] == 0.6 for payload in result["labels"].values())
    assert {error["code"] for error in result["errors"]} == {"recall_below_minimum"}


def test_gate_b_borderline_and_source_insufficient_are_watch_not_denominator(tmp_path):
    paths = _gate_fixture(tmp_path)

    def mutate(seed):
        rows = seed["labels"]["child_directed"]["positives"]
        rows[0]["confidence"] = "borderline"
        rows[1]["source_status"] = "insufficient"

    _mutate_json(paths["seed"], mutate)
    result = _evaluate(paths)
    assert result["status"] == "pass"
    assert result["labels"]["child_directed"]["eligible_positive_families"] == 3
    assert _family(result, "child_directed", 0)["review_classification"] == "borderline_watch"
    assert _family(result, "child_directed", 1)["review_classification"] == "source_insufficient"
    assert {warning["code"] for warning in result["warnings"]} == {
        "borderline_watch",
        "source_insufficient",
    }


def test_gate_b_classifies_seed_conflict_and_model_miss(tmp_path):
    paths = _gate_fixture(tmp_path)

    def mutate(seed):
        payload = seed["labels"]["family_suitable"]
        payload["hard_negatives"].append(copy.deepcopy(payload["positives"][0]))

    _mutate_json(paths["seed"], mutate)
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    report["execution"]["events"][6]["sources"][0]["validated_outcomes"][
        "family_suitable_decision"
    ] = {"value": "unknown"}
    _write_json(paths["report"], report)
    result = _evaluate(paths)
    assert result["status"] == "blocked"
    assert _family(result, "family_suitable", 0)["review_classification"] == "seed_conflict"
    assert _family(result, "family_suitable", 1)["review_classification"] == "model_miss"
    assert "seed_family_conflict" in {error["code"] for error in result["errors"]}


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [(["http_503"], "provider_deferred"), (["ok"], "validator_reject")],
)
def test_gate_b_distinguishes_provider_deferred_and_validator_reject(
    tmp_path, statuses, expected
):
    paths = _gate_fixture(tmp_path)
    report = json.loads(paths["report"].read_text(encoding="utf-8"))
    source = report["execution"]["events"][0]["sources"][0]
    source["status"] = "deferred"
    source["deferred_reason"] = "provider_or_validation_failure"
    source.pop("validated_outcomes")
    source["trace"]["statuses"] = statuses
    report["execution"]["deferred_sources"] = 1
    report["execution"]["evaluated_sources"] -= 1
    _write_json(paths["report"], report)
    result = _evaluate(paths)
    assert _family(result, "child_directed", 0)["runtime_outcome"] == expected
    assert _family(result, "child_directed", 0)["review_classification"] == "model_miss"
    assert result["status"] == "pass"  # 4/5 hard-positive recall remains 0.80.


def test_gate_b_confirmed_hard_negative_is_blocking(tmp_path):
    paths = _gate_fixture(tmp_path)

    def mutate(seed):
        payload = seed["labels"]["child_directed"]
        row = payload["positives"].pop(0)
        row["expected"] = "hard_negative"
        payload["hard_negatives"].append(row)

    _mutate_json(paths["seed"], mutate)
    result = _evaluate(paths)
    assert result["status"] == "blocked"
    assert result["labels"]["child_directed"]["confirmed_hard_negative_families"] == 1
    assert "hard_negative_confirmed" in {error["code"] for error in result["errors"]}


@pytest.mark.parametrize("mismatch", ["quote", "source"])
def test_gate_b_exact_quote_or_source_mismatch_blocks(tmp_path, mismatch):
    paths = _gate_fixture(tmp_path)
    if mismatch == "quote":
        _mutate_json(
            paths["seed"],
            lambda seed: seed["labels"]["child_directed"]["positives"][0].update(
                source_quote="not in source"
            ),
        )
        expected = "quote_source_mismatch"
    else:
        connection = sqlite3.connect(paths["db"])
        connection.execute(
            "UPDATE event_source SET source_url='https://changed.test' WHERE id=9001"
        )
        connection.commit()
        connection.close()
        db_hash = gate_b._sha256_file(paths["db"])
        _mutate_json(
            paths["report"],
            lambda report: report["db_snapshot"].update(
                sha256_before=db_hash, sha256_after=db_hash
            ),
        )
        expected = "source_metadata_mismatch"
    result = _evaluate(paths)
    assert result["status"] == "blocked"
    assert expected in {error["code"] for error in result["errors"]}


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("db", "db_hash_mismatch"),
        ("repo", "repo_sha_mismatch"),
        ("snapshot", "seed_index_snapshot_mismatch"),
        ("index", "seed_index_hash_mismatch"),
        ("cohort", "stale_seed_cohort"),
        ("generator", "generator_provenance_invalid"),
    ],
)
def test_gate_b_rejects_independent_hash_and_cohort_mismatches(tmp_path, target, expected):
    paths = _gate_fixture(tmp_path)
    if target == "db":
        _mutate_json(
            paths["report"],
            lambda report: report["db_snapshot"].update(sha256_after="a" * 64),
        )
    elif target == "repo":
        paths["repo_sha"] = "a" * 40
    elif target == "snapshot":
        _mutate_json(
            paths["seed"], lambda seed: seed.update(evidence_snapshot_sha256="a" * 64)
        )
    elif target == "index":
        _mutate_json(
            paths["seed"],
            lambda seed: seed["source"].update(source_review_index_sha256="a" * 64),
        )
    elif target == "cohort":
        _mutate_json(
            paths["report"],
            lambda report: report["selection"]["requested_source_bindings"].pop(),
        )
    else:
        _mutate_json(
            paths["seed"],
            lambda seed: seed["source"].pop("seed_builder_repo_sha"),
        )
    result = _evaluate(paths)
    assert result["status"] == "blocked"
    assert expected in {error["code"] for error in result["errors"]}


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("index_internal", "index_hash_mismatch"),
        ("receipt", "receipt_hash_mismatch"),
        ("source_ref", "source_ref_hash_mismatch"),
        ("source_text", "source_text_hash_mismatch"),
    ],
)
def test_gate_b_rejects_index_receipt_source_ref_and_text_hashes(
    tmp_path, target, expected
):
    paths = _gate_fixture(tmp_path)
    if target == "index_internal":
        _mutate_json(
            paths["index"], lambda index: index.update(reviewed_at="2026-08-02T13:00:00Z")
        )
    elif target == "receipt":
        receipt_path = paths["index"].parent / "gate-fixture.json"
        _mutate_json(
            receipt_path,
            lambda receipt: receipt.update(reviewed_at="2026-08-02T13:00:00Z"),
        )
    elif target == "source_ref":
        _mutate_json(
            paths["seed"],
            lambda seed: seed["labels"]["child_directed"]["positives"][0][
                "source_refs"
            ][0].update(source_ref_sha256="a" * 64),
        )
    else:
        def mutate(seed):
            ref = seed["labels"]["child_directed"]["positives"][0]["source_refs"][0]
            ref["source_text_sha256"] = "a" * 64
            ref["source_ref_sha256"] = gate_b._source_ref_hash(ref)

        _mutate_json(paths["seed"], mutate)
    result = _evaluate(paths)
    assert result["status"] == "blocked"
    assert expected in {error["code"] for error in result["errors"]}


def test_gate_b_accepts_exact_seed_plus_bound_boundary_cohort(tmp_path):
    paths = _add_boundary(_gate_fixture(tmp_path), runtime_outcome="denied")
    result = _evaluate(paths)
    assert result["status"] == "pass"
    assert result["boundaries"] == {
        "manifest_supplied": True,
        "total": 1,
        "hard_failures": 0,
        "watch_disagreements": 0,
        "classifications": {"match": 1},
        "rows": [
            {
                "boundary_id": "named-2000-child",
                "event_id": 2000,
                "source_id": 9900,
                "label": "child_directed",
                "expectation": "not_confirmed",
                "runtime_outcome": "denied",
                "classification": "match",
            }
        ],
    }
    assert result["hashes"]["boundary_manifest_file_sha256"] == gate_b._sha256_file(
        paths["boundary_manifest"]
    )
    assert result["labels"]["child_directed"]["eligible_positive_families"] == 5


def test_gate_b_rejects_unbound_extra_runtime_row(tmp_path):
    result = _evaluate(_add_boundary(_gate_fixture(tmp_path), bind=False))
    assert result["status"] == "blocked"
    codes = {error["code"] for error in result["errors"]}
    assert "stale_seed_cohort" in codes
    assert "execution_cohort_mismatch" in codes


def test_gate_b_blocks_confirmed_hard_boundary(tmp_path):
    paths = _add_boundary(
        _gate_fixture(tmp_path), expectation="not_confirmed", runtime_outcome="confirmed"
    )
    result = _evaluate(paths)
    assert result["status"] == "blocked"
    assert result["boundaries"]["hard_failures"] == 1
    assert result["boundaries"]["rows"][0]["classification"] == (
        "hard_negative_confirmed"
    )
    assert "boundary_hard_negative_confirmed" in {
        error["code"] for error in result["errors"]
    }


def test_gate_b_watch_disagreement_is_warning_not_recall_or_copy_block(tmp_path):
    paths = _add_boundary(
        _gate_fixture(tmp_path), expectation="confirmed_watch", runtime_outcome="unknown"
    )
    result = _evaluate(paths)
    assert result["status"] == "pass"
    assert result["copy_gates_allowed"] is True
    assert result["boundaries"]["watch_disagreements"] == 1
    assert result["boundaries"]["rows"][0]["classification"] == "watch_disagreement"
    assert "boundary_watch_disagreement" in {
        warning["code"] for warning in result["warnings"]
    }
    assert result["labels"]["child_directed"]["eligible_positive_families"] == 5
