from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from scripts.resolve_unusual_events_health import ResolverError, resolve_bundle


def _write_json(path: Path, value: dict) -> str:
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, Path, dict]:
    artifact_root = tmp_path / "builder"
    artifact_root.mkdir()
    bge = {"schema_version": "static-collection-bge-cache-receipt-v1"}
    manifest = {"schema_version": "static_unusual_events_v1"}
    cache = {"schema_version": "unusual-event-score-cache-v1"}
    bge_sha = _write_json(
        artifact_root / "static_event_bge_vectors.receipt.json", bge
    )
    manifest_sha = _write_json(
        artifact_root / "unusual-events-manifest.json", manifest
    )
    cache_sha = _write_json(artifact_root / "unusual_events_cache.json", cache)
    receipt = {
        "schema_version": "static_site_success_receipt_v2",
        "release_channel": "secret_preview",
        "build_id": "production-test",
        "run_id": "static-site:production-test:run",
        "repo_sha": "a" * 40,
        "snapshot_id": "snapshot-test",
        "snapshot_sha256": "b" * 64,
        "input_fingerprint": "c" * 64,
        "effective_date": "2026-08-09",
        "semantic_cache_mode": "warm",
        "semantic": {
            "input_fingerprint": "c" * 64,
            "vector_receipt_sha256": bge_sha,
            "manifest_sha256": manifest_sha,
            "unusual_events_manifest_sha256": manifest_sha,
            "unusual_cache_sha256": cache_sha,
        },
    }
    database = tmp_path / "db.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE static_site_build_state(
                release_channel TEXT PRIMARY KEY,
                last_success_receipt_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO static_site_build_state VALUES('secret_preview', ?)",
            (json.dumps(receipt),),
        )
    return database, artifact_root, receipt


def test_resolver_emits_only_exact_same_pipeline_artifacts(tmp_path: Path):
    database, artifact_root, receipt = _fixture(tmp_path)

    result = resolve_bundle(
        database=database,
        artifact_root=artifact_root,
        expect_mode="warm",
        expect_effective_date="2026-08-09",
        expect_repo_sha="a" * 40,
    )

    assert result["schema_version"] == "unusual-events-health-resolver-v1"
    assert result["request_id"] == receipt["run_id"]
    assert result["run_mode"] == "warm"
    assert set(result["artifacts"]) == {
        "bge_receipt",
        "unusual_manifest",
        "unusual_cache",
        "builder_receipt",
    }
    assert "public_url" not in json.dumps(result)


def test_resolver_waits_for_exact_deployed_repo_sha(tmp_path: Path):
    database, artifact_root, _ = _fixture(tmp_path)

    with pytest.raises(ResolverError, match="builder_repo_sha_pending"):
        resolve_bundle(
            database=database,
            artifact_root=artifact_root,
            expect_repo_sha="d" * 40,
        )


def test_resolver_waits_for_distinct_cold_run(tmp_path: Path):
    database, artifact_root, receipt = _fixture(tmp_path)

    with pytest.raises(ResolverError, match="builder_run_not_advanced"):
        resolve_bundle(
            database=database,
            artifact_root=artifact_root,
            expect_mode="cold",
            after_run_id=receipt["run_id"],
        )


def test_resolver_fails_closed_on_hash_drift(tmp_path: Path):
    database, artifact_root, _ = _fixture(tmp_path)
    (artifact_root / "unusual_events_cache.json").write_text(
        '{"schema_version":"tampered"}\n', encoding="utf-8"
    )

    with pytest.raises(ResolverError, match="artifact_hash_mismatch:unusual_cache"):
        resolve_bundle(database=database, artifact_root=artifact_root)
