from __future__ import annotations

import importlib.util
import hashlib
import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_external_publication_provenance_backfill.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_external_publication_provenance_backfill", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def legacy_row():
    return {
        "external_publication_id": "extpub_legacy",
        "research_request_id": "region-talk-legacy-run",
        "canonical_url": "https://publisher.example/article?utm_source=old",
        "imported_at": "2026-07-19T22:31:25+00:00",
        "publication": {"title": "Article title", "authors": ["A. Author"]},
        "evidence": [{"url": "https://publisher.example/about", "paraphrase": "Publisher profile"}],
        "decision": {
            "import_status": "ready_for_region_talk_scoring",
            "downstream_readiness": "candidate_report",
        },
    }


def test_legacy_attestation_preserves_semantics_and_defaults_to_unreviewed() -> None:
    mod = load_module()
    prepared = mod.build_backfill(
        [legacy_row()], selected_ids={"extpub_legacy"}, attested_at="2026-08-02T13:00:00+00:00"
    )

    assert prepared["execution_blocked"] is False
    assert prepared["update_ids"] == ["extpub_legacy"]
    updated = prepared["updates"][0]
    assert updated["decision"] == legacy_row()["decision"]
    assert updated["review_status"] == "unreviewed"
    assert updated["publication_permission"] == "not_granted"
    assert updated["request_id"] == "region-talk-legacy-run"
    assert updated["identity_keys"]
    assert len(updated["legacy_provenance_attestation"]["legacy_row_sha256"]) == 64
    assert updated["legacy_provenance_attestation"]["input_json_sha256_available"] is False


def test_legacy_attestation_is_idempotently_skipped() -> None:
    mod = load_module()
    first = mod.build_backfill([legacy_row()], attested_at="2026-08-02T13:00:00+00:00")
    second = mod.build_backfill(first["updates"], attested_at="2026-08-02T14:00:00+00:00")

    assert second["updates"] == []
    assert second["skipped_attested_ids"] == ["extpub_legacy"]


def test_missing_request_fails_closed() -> None:
    mod = load_module()
    row = legacy_row()
    row.pop("research_request_id")

    prepared = mod.build_backfill([row], attested_at="2026-08-02T13:00:00+00:00")

    assert prepared["execution_blocked"] is True
    assert prepared["blocked"][0]["external_publication_id"] == "extpub_legacy"


def test_missing_historical_intake_time_fails_closed() -> None:
    mod = load_module()
    row = legacy_row()
    row.pop("imported_at")

    prepared = mod.build_backfill([row], attested_at="2026-08-02T13:00:00+00:00")

    assert prepared["execution_blocked"] is True
    assert "intake time" in prepared["blocked"][0]["reason"]


def fake_pool_with_row(row):
    pk = "external_publication_intake_item:" + row["external_publication_id"]
    durable = {pk: json.dumps(row, ensure_ascii=False)}

    class Transaction:
        def __init__(self):
            self.staged = {}

        def execute(self, query, params, commit_tx=False):
            if "SELECT payload_json" in query:
                value = self.staged.get(params["$pk"], durable.get(params["$pk"]))
                rows = [] if value is None else [types.SimpleNamespace(payload_json=value)]
                return [types.SimpleNamespace(rows=rows)]
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
        @staticmethod
        def retry_operation_sync(operation):
            return operation(Session())

    return Pool(), types.SimpleNamespace(SerializableReadWrite=lambda: object()), durable


def test_execute_reserves_all_identities_in_same_transaction() -> None:
    mod = load_module()
    row = legacy_row()
    row["doi"] = "10.1234/legacy"
    prepared = mod.build_backfill([row], attested_at="2026-08-02T13:00:00+00:00")
    pool, ydb, durable = fake_pool_with_row(row)

    result = mod.execute_backfill(pool, ydb, "table", prepared)

    assert result["intake_updates"] == 1
    assert result["identity_reservations_written"] == len(prepared["updates"][0]["identity_keys"])
    assert result["written_ydb_rows"] == 1 + result["identity_reservations_written"]
    for identity_key in prepared["updates"][0]["identity_keys"]:
        identity_sha = hashlib.sha256(identity_key.encode()).hexdigest()
        identity = json.loads(durable["external_publication_identity_item:" + identity_sha])
        assert identity["external_publication_id"] == "extpub_legacy"
        assert identity["reservation_mode"] == "legacy_provenance_backfill"


def test_execute_detects_concurrent_operator_review_and_writes_nothing() -> None:
    mod = load_module()
    row = legacy_row()
    prepared = mod.build_backfill([row], attested_at="2026-08-02T13:00:00+00:00")
    pool, ydb, durable = fake_pool_with_row(row)
    pk = "external_publication_intake_item:extpub_legacy"
    changed = dict(row)
    changed.update({"review_status": "reviewed", "publication_permission": "blocked"})
    durable[pk] = json.dumps(changed, ensure_ascii=False)
    before = dict(durable)

    with pytest.raises(mod.BackfillError, match="intake changed before write"):
        mod.execute_backfill(pool, ydb, "table", prepared)

    assert durable == before
