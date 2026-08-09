from __future__ import annotations

import importlib.util
import hashlib
import json
from pathlib import Path

import pytest

from scripts import unusual_events_health as health


ROOT = Path(__file__).resolve().parents[1]
ISSUE_HELPER = ROOT / ".github/scripts/unusual-events-health-issue.py"


def _sha(character: str) -> str:
    return character * 64


def _selected(count: int) -> list[dict[str, object]]:
    return [
        {
            "event_id": index,
            "concept_id": f"concept:{index}",
            "title": f"Event {index}",
            "path": f"/sobytiya/event-{index}/",
            "date": "2026-08-20",
            "end_date": "2026-08-20",
            "family": f"family-{index % 5}",
            "unusual_score": 0.9 - index / 1000,
            "reason_codes": ["semantic_evidence"],
            "image_policy": {"status": "accepted"},
            "has_image": True,
        }
        for index in range(1, count + 1)
    ]


def _artifacts(*, count: int = 20, run: str = "run-1", build: str = "build-1"):
    fingerprint = _sha("f")
    snapshot = _sha("e")
    artifact = _sha("a")
    manifest_sha = _sha("b")
    cache_sha = _sha("c")
    receipt_sha = _sha("d")
    bge = {
        "schema_version": health.BGE_RECEIPT_SCHEMA,
        "artifact_sha256": artifact,
        "event_cache_identity_sha256": _sha("8"),
        "model_id": health.MODEL_ID,
        "model_revision": health.MODEL_REVISION,
        "encoder_contract": health.ENCODER_CONTRACT,
        "document_kind": health.DOCUMENT_KIND,
        "document_version": health.DOCUMENT_VERSION,
        "embedding_dim": health.EMBEDDING_DIM,
        "dtype": "float32",
        "prototype_bank_sha256": _sha("1"),
        "classifier_sha256": _sha("2"),
        "npz_sha256": _sha("3"),
        "event_text_hashes": {str(index): _sha("4") for index in range(1, 21)},
        "prototype_text_hashes": {"one": _sha("5")},
        "metadata": {
            "model_id": health.MODEL_ID,
            "model_revision": health.MODEL_REVISION,
            "encoder_contract": health.ENCODER_CONTRACT,
            "document_kind": health.DOCUMENT_KIND,
            "document_version": health.DOCUMENT_VERSION,
            "embedding_dim": health.EMBEDDING_DIM,
            "artifact_sha256": artifact,
            "event_cache_identity_sha256": _sha("8"),
            "prototype_bank_sha256": _sha("1"),
            "classifier_sha256": _sha("2"),
            "provider_calls": 0,
            "event_count": 20,
            "encoded_event_count": 2,
            "reused_event_count": 18,
            "prototype_count": 1,
            "build": {
                "build_id": build,
                "source_snapshot_hash": snapshot,
                "input_fingerprint": fingerprint,
            },
        },
    }
    manifest = {
        "schema_version": "static_unusual_events_v1",
        "build_id": build,
        "generated_at": "2026-08-09T12:00:00Z",
        "as_of_date": "2026-08-09",
        "source_snapshot_id": "snapshot-1",
        "source_snapshot_hash": snapshot,
        "input_fingerprint": fingerprint,
        "delivery_status": "approved",
        "quality_gate": {
            "status": "approved",
            "metrics": {"precision_at_20": 0.9, "provider_calls": 0},
        },
        "provider_calls": 0,
        "prototype_bank_hash": _sha("6"),
        "classifier_hash": _sha("7"),
        "migration": {"enabled": False, "notify": False},
        "items": _selected(count),
        "candidate_items": _selected(24),
        "near_threshold": [{"event_id": 25, "score": 0.71, "reason": "near"}],
        "exclusions": [{"event_id": 26, "reason": "hard_negative"}],
        "duplicates": [{"event_id": 27, "concept_id": "concept:1", "reason": "duplicate"}],
        "expired": [],
    }
    cache = {
        "schema_version": health.CACHE_SCHEMA,
        "status": "ready",
        "provider_calls": 0,
        "input_fingerprint": fingerprint,
        "records": {},
    }
    builder = {
        "schema_version": "static_site_build_result_v2",
        "ok": True,
        "profile": "production-candidate",
        "build_id": build,
        "run_id": run,
        "repo_sha": "d" * 40,
        "input_fingerprint": fingerprint,
        "snapshot": {"snapshot_id": "snapshot-1", "snapshot_sha256": snapshot},
        "finished_at": "2026-08-09T12:10:00Z",
        "semantic": {
            "status": "validated",
            "provider_calls": 0,
            "artifact_sha256": artifact,
            "artifact_event_count": 20,
            "manifest_sha256": manifest_sha,
            "unusual_cache_sha256": cache_sha,
            "vector_receipt_sha256": receipt_sha,
            "input_fingerprint": fingerprint,
        },
    }
    return bge, manifest, cache, builder, {
        "bge_receipt": receipt_sha,
        "unusual_manifest": manifest_sha,
        "unusual_cache": cache_sha,
    }


def _evaluate(*, count: int = 20, previous=None, run: str = "run-1", build: str = "build-1"):
    bge, manifest, cache, builder, hashes = _artifacts(count=count, run=run, build=build)
    return health.evaluate_health(
        bge_receipt=bge,
        unusual_manifest=manifest,
        unusual_cache=cache,
        builder_receipt=builder,
        artifact_sha256s=hashes,
        previous_health=previous,
        generated_at="2026-08-09T13:00:00Z",
    )


def test_healthy_ready_contract_is_bounded_and_contains_no_persisted_urls():
    result = _evaluate()

    assert result["health_status"] == "HEALTHY"
    assert result["content_readiness"] == "READY"
    assert result["feed"]["selected_count"] == 20
    assert result["feed"]["target_count"] == 20
    assert result["feed"]["minimum_publish_count"] == 12
    assert result["repo_sha"] == "d" * 40
    assert result["run_id"] == "run-1"
    assert result["as_of_date"] == "2026-08-09"
    assert result["publication"]["expected"] is True
    assert result["publication"]["canonical_path"] == "/neobychnoe/"
    assert result["feed"]["visible_event_ids"] == result["feed"]["ordered_visible_event_ids"]
    assert result["feed"]["selected"][0]["start_date"] == "2026-08-20"
    assert result["feed"]["selected"][0]["image_required"] is True
    assert len(result["feed"]["near_threshold"]) == 1
    assert result["publication"]["manifest_sha256"] == _sha("b")
    assert result["contracts"]["visible_output_sha256"] == result["feed"]["visible_output_sha256"]
    assert "http://" not in json.dumps(result)
    assert "https://" not in json.dumps(result)


def test_watch_is_ready_between_minimum_and_target():
    result = _evaluate(count=12)

    assert result["health_status"] == "WATCH"
    assert result["content_readiness"] == "READY"
    assert [finding["code"] for finding in result["findings"]["warnings"]] == ["feed.under_target"]


def test_below_minimum_is_incident_not_ready():
    result = _evaluate(count=11)

    assert result["health_status"] == "INCIDENT"
    assert result["content_readiness"] == "NOT_READY"
    assert "feed.below_minimum" in {finding["code"] for finding in result["findings"]["errors"]}


def test_provider_call_or_hash_drift_is_incident_blocked():
    bge, manifest, cache, builder, hashes = _artifacts()
    bge["metadata"]["provider_calls"] = 1
    builder["semantic"]["manifest_sha256"] = _sha("9")

    result = health.evaluate_health(
        bge_receipt=bge,
        unusual_manifest=manifest,
        unusual_cache=cache,
        builder_receipt=builder,
        artifact_sha256s=hashes,
        generated_at="2026-08-09T13:00:00Z",
    )

    assert result["health_status"] == "INCIDENT"
    assert result["content_readiness"] == "BLOCKED"
    codes = {finding["code"] for finding in result["findings"]["errors"]}
    assert {"bge.provider_calls", "identity.manifest_sha256"} <= codes


def test_selected_evidence_is_capped_and_absolute_urls_fail_closed():
    bge, manifest, cache, builder, hashes = _artifacts()
    manifest["near_threshold"] = [{"event_id": index} for index in range(100, 150)]
    manifest["items"][0]["path"] = "https://example.invalid/private"

    result = health.evaluate_health(
        bge_receipt=bge,
        unusual_manifest=manifest,
        unusual_cache=cache,
        builder_receipt=builder,
        artifact_sha256s=hashes,
        generated_at="2026-08-09T13:00:00Z",
    )

    assert result["health_status"] == "INCIDENT"
    assert result["feed"]["selected"][0]["path"] is None
    assert len(result["feed"]["near_threshold"]) == health.MAX_SUPPORT_ROWS
    assert "example.invalid" not in json.dumps(result)


def test_closure_requires_two_distinct_consecutive_healthy_ready_runs():
    first = _evaluate(run="run-1", build="build-1")
    duplicate = _evaluate(previous=first, run="run-1", build="build-1")
    second = _evaluate(previous=first, run="run-2", build="build-2")

    assert first["closure"] == {
        "required_consecutive_runs": 2,
        "consecutive_healthy_ready_runs": 1,
        "eligible_to_close": False,
    }
    assert duplicate["health_status"] == "WATCH"
    assert duplicate["closure"]["eligible_to_close"] is False
    assert second["health_status"] == "HEALTHY"
    assert second["closure"]["consecutive_healthy_ready_runs"] == 2
    assert second["closure"]["eligible_to_close"] is True


def test_resolver_bundle_requires_same_pipeline_run_identity():
    bge, manifest, cache, builder, hashes = _artifacts()
    bundle = {
        "schema_version": health.INPUT_SCHEMA,
        "request_id": "another-run",
        "run_mode": "warm",
        "artifacts": {
            "bge_receipt": {"sha256": hashes["bge_receipt"], "payload": bge},
            "unusual_manifest": {"sha256": hashes["unusual_manifest"], "payload": manifest},
            "unusual_cache": {"sha256": hashes["unusual_cache"], "payload": cache},
            "builder_receipt": {"sha256": _sha("b"), "payload": builder},
        },
    }

    with pytest.raises(health.ContractError, match="resolver_builder_run_id_mismatch"):
        health.evaluate_bundle(
            bundle,
            target_count=20,
            minimum_count=12,
            previous_health=None,
            generated_at="2026-08-09T13:00:00Z",
        )


def test_request_acknowledgement_is_strict():
    assert health.validate_request(
        {
            "schema_version": health.REQUEST_SCHEMA,
            "accepted": True,
            "request_id": "static-site:health-123",
            "run_mode": "cold",
        },
        expected_mode="cold",
    ) == "static-site:health-123"
    with pytest.raises(health.ContractError, match="request_mode_mismatch"):
        health.validate_request(
            {
                "schema_version": health.REQUEST_SCHEMA,
                "accepted": True,
                "request_id": "static-site:health-123",
                "run_mode": "warm",
            },
            expected_mode="cold",
        )


def _load_issue_helper():
    spec = importlib.util.spec_from_file_location("unusual_events_health_issue", ISSUE_HELPER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_issue_plan_opens_on_watch_and_closes_only_after_closure_gate():
    issue = _load_issue_helper()
    watch = _evaluate(count=12)
    first = _evaluate()
    second = _evaluate(previous=first, run="run-2", build="build-2")

    assert issue.build_issue_plan(watch)["action"] == "OPEN_OR_UPDATE"
    assert issue.build_issue_plan(first)["action"] == "HOLD"
    close_plan = issue.build_issue_plan(second)
    assert close_plan["action"] == "CLOSE"
    assert "http" not in close_plan["body"]


def test_blocked_cli_is_incident_and_returns_two(tmp_path: Path):
    output = tmp_path / "health.json"
    markdown = tmp_path / "health.md"
    rc = health.main(
        [
            "blocked",
            "--code",
            "same_pipeline_cold_unsupported",
            "--message",
            "The existing builder cannot accept a cacheless request.",
            "--output",
            str(output),
            "--markdown-output",
            str(markdown),
            "--generated-at",
            "2026-08-09T13:00:00Z",
        ]
    )

    assert rc == 2
    payload = json.loads(output.read_text())
    assert payload["health_status"] == "INCIDENT"
    assert payload["content_readiness"] == "BLOCKED"
    assert "same_pipeline_cold_unsupported" in markdown.read_text()


@pytest.mark.parametrize(("count", "expected_status", "expected_rc"), [(12, "WATCH", 0), (11, "INCIDENT", 2)])
def test_evaluate_cli_exit_contract(tmp_path: Path, count: int, expected_status: str, expected_rc: int):
    bge, manifest, cache, builder, _ = _artifacts(count=count)
    paths = {
        "bge": tmp_path / "bge.json",
        "manifest": tmp_path / "manifest.json",
        "cache": tmp_path / "cache.json",
        "builder": tmp_path / "builder.json",
    }
    for key, payload in (("bge", bge), ("manifest", manifest), ("cache", cache)):
        paths[key].write_text(json.dumps(payload), encoding="utf-8")
    builder["semantic"].update(
        {
            "vector_receipt_sha256": hashlib.sha256(paths["bge"].read_bytes()).hexdigest(),
            "manifest_sha256": hashlib.sha256(paths["manifest"].read_bytes()).hexdigest(),
            "unusual_cache_sha256": hashlib.sha256(paths["cache"].read_bytes()).hexdigest(),
        }
    )
    paths["builder"].write_text(json.dumps(builder), encoding="utf-8")
    output = tmp_path / "health.json"
    rc = health.main(
        [
            "evaluate",
            "--bge-receipt", str(paths["bge"]),
            "--unusual-manifest", str(paths["manifest"]),
            "--unusual-cache", str(paths["cache"]),
            "--builder-receipt", str(paths["builder"]),
            "--output", str(output),
            "--markdown-output", str(tmp_path / "health.md"),
            "--generated-at", "2026-08-09T13:00:00Z",
        ]
    )
    assert rc == expected_rc
    assert json.loads(output.read_text())["health_status"] == expected_status


def test_machine_contract_files_are_json_objects():
    for name in (
        "unusual-events-health-policy-v1.json",
        "unusual-events-health-resolver-v1.schema.json",
        "unusual-events-production-health-v1.schema.json",
    ):
        value = json.loads((ROOT / "docs/features/unusual-events" / name).read_text())
        assert isinstance(value, dict)
