from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "features" / "region-talk-channel"
MODULE_PATH = ROOT / "scripts" / "region_talk_publisher_profile_correction_review.py"
IMPORTER_PATH = ROOT / "scripts" / "region_talk_publisher_profile_import.py"


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def prepared_correction(name: str):
    importer = load(IMPORTER_PATH, "region_talk_publisher_profile_import_for_review")
    path = DOCS / f"region-talk-publisher-profile-enrichment-{name}-2026-08-02.json"
    raw = path.read_bytes()
    payload = json.loads(raw)
    return importer.prepare_import(payload, input_bytes=raw)["corrections"][0]


def review_for(correction: dict, intake_hash: str, decision: str) -> dict:
    return {
        "schema_version": "region_talk_publisher_profile_candidate_correction_review.v1",
        "publisher_profile_correction_id": correction["publisher_profile_correction_id"],
        "expected_correction_hash": correction["correction_hash"],
        "expected_live_intake_payload_sha256": intake_hash,
        "canonical_url": correction["canonical_url"],
        "decision": decision,
        "reviewer": "region-talk-operator",
        "reviewed_at": "2026-08-02T18:00:00+00:00",
        "reason": "Проверены актуальная запись intake и публичные доказательства источника.",
        "evidence": [{
            "url": correction["canonical_url"],
            "supports": ["regional_locality" if decision == "block_regional" else "external_scope"],
            "note": "Страница и подпись автора проверены вручную перед решением.",
        }],
    }


def test_rg_review_stays_blocked_and_never_grants_publication() -> None:
    mod = load(MODULE_PATH, "region_talk_publisher_profile_correction_review_rg")
    correction = prepared_correction("rg-ru")
    review = mod.validate_review(review_for(correction, "a" * 64, "block_regional"))

    updated, attestation = mod.reviewed_correction(correction, review, live_intake_hash="a" * 64)

    assert updated["review_status"] == "reviewed_regional"
    assert updated["live_revalidation_status"] == "blocked_regional"
    assert updated["regeneration_allowed"] is False
    assert updated["candidate_mutation_allowed"] is False
    assert updated["publication_permission"] == "not_granted"
    assert attestation["candidate_mutated"] is False
    assert attestation["publication_effect"] == "none"


def test_hard_locality_cannot_be_retained_without_explicit_fresh_override() -> None:
    mod = load(MODULE_PATH, "region_talk_publisher_profile_correction_review_override")
    correction = prepared_correction("rg-ru")
    review = mod.validate_review(review_for(correction, "b" * 64, "retain_external"))

    with pytest.raises(mod.CorrectionReviewError, match="fresh evidence"):
        mod.reviewed_correction(correction, review, live_intake_hash="b" * 64)


def _install_fake_ydb(mod, monkeypatch, durable: dict[str, str]):
    reads: list[str] = []

    class Transaction:
        def __init__(self) -> None:
            self.staged: dict[str, str] = {}

        def execute(self, query, params, commit_tx=False):
            if "SELECT payload_json" in query:
                reads.append(params["$pk"])
                value = self.staged.get(params["$pk"], durable.get(params["$pk"]))
                rows = [] if value is None else [types.SimpleNamespace(payload_json=value)]
                return [types.SimpleNamespace(rows=rows)]
            assert "UPSERT INTO" in query
            self.staged[params["$pk"]] = params["$payload_json"]
            if commit_tx:
                durable.update(self.staged)
            return []

        def rollback(self):
            self.staged.clear()

    class Session:
        @staticmethod
        def prepare(query):
            return query

        @staticmethod
        def transaction(_mode):
            return Transaction()

    class Pool:
        def __init__(self, _driver) -> None:
            pass

        @staticmethod
        def retry_operation_sync(operation):
            return operation(Session())

    class Driver:
        def __init__(self, **_kwargs) -> None:
            pass

        @staticmethod
        def wait(**_kwargs):
            pass

        @staticmethod
        def stop(**_kwargs):
            pass

    fake_ydb = types.SimpleNamespace(
        Driver=Driver,
        SessionPool=Pool,
        SerializableReadWrite=lambda: object(),
    )
    monkeypatch.setattr(mod, "ensure_ydb_module", lambda: fake_ydb)
    monkeypatch.setattr(mod, "ydb_endpoint_database", lambda: ("grpc://example", "/db"))
    monkeypatch.setattr(mod, "ydb_credentials", lambda _ydb: None)
    monkeypatch.setattr(mod, "ydb_table_path", lambda _database: "/db/state")
    return reads


def test_review_strongly_rereads_and_writes_only_correction_and_audit(monkeypatch) -> None:
    mod = load(MODULE_PATH, "region_talk_publisher_profile_correction_review_tx")
    correction = prepared_correction("archi-ru")
    external_id = "extpub_archi"
    intake = json.dumps({
        "external_publication_id": external_id,
        "canonical_url": correction["canonical_url"],
        "publication_permission": "not_granted",
    }, ensure_ascii=False, separators=(",", ":"))
    intake_hash = hashlib.sha256(intake.encode()).hexdigest()
    durable = {
        correction["pk"]: json.dumps(correction, ensure_ascii=False, separators=(",", ":")),
        correction["live_identity_pk"]: json.dumps({"external_publication_id": external_id}),
        "external_publication_intake_item:" + external_id: intake,
    }
    reads = _install_fake_ydb(mod, monkeypatch, durable)
    before_intake = durable["external_publication_intake_item:" + external_id]
    review = review_for(correction, intake_hash, "retain_external")

    dry = mod.execute_review(review, execute=False)
    assert dry["execution_status"] == "validated_live_no_write"
    assert dry["written_ydb_rows"] == 0
    committed = mod.execute_review(review, execute=True)
    assert committed["execution_status"] == "committed"
    assert committed["written_ydb_rows"] == 2
    assert durable["external_publication_intake_item:" + external_id] == before_intake
    stored = json.loads(durable[correction["pk"]])
    assert stored["review_status"] == "retained_external"
    assert stored["regeneration_allowed"] is True
    assert stored["candidate_mutated_by_review"] is False
    assert correction["pk"] in reads
    assert correction["live_identity_pk"] in reads
    assert "external_publication_intake_item:" + external_id in reads
    assert any(pk.startswith("publisher_profile_candidate_correction_review_item:") for pk in durable)


def test_changed_live_intake_fails_closed_with_zero_writes(monkeypatch) -> None:
    mod = load(MODULE_PATH, "region_talk_publisher_profile_correction_review_changed")
    correction = prepared_correction("rg-ru")
    external_id = "extpub_rg"
    intake = json.dumps({
        "external_publication_id": external_id,
        "canonical_url": correction["canonical_url"],
        "changed": True,
    }, separators=(",", ":"))
    durable = {
        correction["pk"]: json.dumps(correction, ensure_ascii=False, separators=(",", ":")),
        correction["live_identity_pk"]: json.dumps({"external_publication_id": external_id}),
        "external_publication_intake_item:" + external_id: intake,
    }
    _install_fake_ydb(mod, monkeypatch, durable)
    before = dict(durable)

    with pytest.raises(mod.CorrectionReviewError, match="live intake changed"):
        mod.execute_review(review_for(correction, "0" * 64, "block_regional"), execute=True)
    assert durable == before


def test_missing_current_identity_cannot_fall_back_to_import_snapshot(monkeypatch) -> None:
    mod = load(MODULE_PATH, "region_talk_publisher_profile_correction_review_identity_missing")
    correction = prepared_correction("rg-ru")
    external_id = "extpub_snapshot_only"
    correction["live_external_publication_id"] = external_id
    intake = json.dumps({
        "external_publication_id": external_id,
        "canonical_url": correction["canonical_url"],
    }, separators=(",", ":"))
    intake_hash = hashlib.sha256(intake.encode()).hexdigest()
    durable = {
        correction["pk"]: json.dumps(correction, ensure_ascii=False, separators=(",", ":")),
        # The current identity row is intentionally absent. A stale
        # import-time external id must never authorize explicit review.
        "external_publication_intake_item:" + external_id: intake,
    }
    _install_fake_ydb(mod, monkeypatch, durable)
    before = dict(durable)

    with pytest.raises(mod.CorrectionReviewError, match="strong identity read"):
        mod.execute_review(review_for(correction, intake_hash, "block_regional"), execute=True)
    assert durable == before
