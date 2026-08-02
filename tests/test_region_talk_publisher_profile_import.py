from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "features" / "region-talk-channel"
MODULE_PATH = ROOT / "scripts" / "region_talk_publisher_profile_import.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_publisher_profile_import", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def package(name: str = "archi-ru") -> tuple[Path, bytes, dict]:
    path = DOCS / f"region-talk-publisher-profile-enrichment-{name}-2026-08-02.json"
    raw = path.read_bytes()
    return path, raw, json.loads(raw.decode("utf-8"))


def test_prepare_validates_exact_bytes_and_builds_separate_guarded_rows() -> None:
    mod = load_module()
    _path, raw, payload = package()
    prepared = mod.prepare_import(
        payload,
        input_bytes=raw,
        expected_input_sha256=hashlib.sha256(raw).hexdigest(),
        imported_at="2026-08-02T12:00:00+00:00",
    )

    assert prepared["batch"]["input_json_sha256"] == hashlib.sha256(raw).hexdigest()
    assert prepared["batch"]["profile_count"] == 1
    profile = prepared["profiles"][0]
    assert profile["canonical_source_key"] == "web:archi.ru"
    assert profile["input_canonical_source_key"] == "domain:archi.ru"
    assert profile["source_domain"] == "archi.ru"
    assert profile["profile_hash"] == mod.canonical_json_sha256(payload["profiles"][0])
    assert profile["evidence_fingerprint"] == mod.publisher_evidence_fingerprint(
        payload["profiles"][0]["evidence"]
    )
    assert profile["profile_status"] == "ready"
    assert profile["usable_without_profile_llm"] is True
    assert profile["publication_permission"] == "not_granted"
    assert profile["profile_dimensions"]["outlet_identity"]
    assert profile["profile_dimensions"]["intended_audience"]
    assert profile["profile_dimensions"]["distinctive_value"]

    kinds = [kind for _pk, kind, _row in prepared["ydb_rows"]]
    assert kinds.count("publisher_profile_item") == 1
    assert kinds.count("publisher_profile_candidate_correction_item") == 1
    assert kinds.count("publisher_profile_import_batch") == 1
    assert kinds.count("publisher_profile_import_receipt_item") == 1
    assert "external_publication_intake_item" not in kinds
    assert "publication_candidate_item" not in kinds

    correction = prepared["corrections"][0]
    assert correction["review_status"] == "unreviewed"
    assert correction["live_revalidation_status"] == "pending_live_revalidation"
    assert correction["publication_permission"] == "not_granted"
    assert correction["candidate_mutation_allowed"] is False
    assert correction["regeneration_allowed"] is False


def test_wrong_exact_sha_or_noncanonical_domain_fails_before_rows() -> None:
    mod = load_module()
    _path, raw, payload = package()
    with pytest.raises(mod.ContractError, match="exact input SHA-256"):
        mod.prepare_import(payload, input_bytes=raw, expected_input_sha256="0" * 64)

    changed = copy.deepcopy(payload)
    changed["profiles"][0]["source_domain"] = "www.archi.ru"
    with pytest.raises(mod.ContractError, match="source_domain must already be canonical"):
        mod.prepare_import(changed, input_bytes=json.dumps(changed).encode())


def test_rg_exact_article_is_queued_fail_closed_without_regeneration() -> None:
    mod = load_module()
    _path, raw, payload = package("rg-ru")
    prepared = mod.prepare_import(payload, input_bytes=raw)

    profile = prepared["profiles"][0]
    correction = prepared["corrections"][0]
    assert profile["canonical_source_key"] == "web:rg.ru"
    assert profile["scope"] == "mixed"
    assert profile["profile_status"] == "needs_review"
    assert profile["usable_without_profile_llm"] is False
    assert correction["canonical_url"].startswith("https://rg.ru/2025/09/16/reg-szfo/")
    assert correction["recommended_action"] == "re_adjudicate_externality"
    assert {"regional_local_edition", "local_correspondent", "federal_brand_not_sufficient"} <= set(
        correction["reason_codes"]
    )
    assert correction["next_action"] == "operator_re_adjudicate_externality"
    assert correction["regeneration_allowed"] is False


def _install_fake_ydb(mod, monkeypatch):
    durable: dict[str, str] = {}
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
    return durable, reads


def test_atomic_execute_strongly_rereads_and_replay_or_sha_conflict_writes_zero(monkeypatch) -> None:
    mod = load_module()
    durable, reads = _install_fake_ydb(mod, monkeypatch)
    _path, raw, payload = package()
    prepared = mod.prepare_import(payload, input_bytes=raw)

    committed = mod.execute_import(prepared)
    after_first = dict(durable)
    replay = mod.execute_import(prepared)

    assert committed["status"] == "committed"
    assert replay["status"] == "identical_replay"
    assert replay["written_ydb_rows"] == 0
    assert durable == after_first
    for prefix in (
        "publisher_profile_import_batch:",
        "publisher_profile_import_receipt_item:",
        "publisher_profile_item:",
        "publisher_profile_candidate_correction_item:",
        "external_publication_identity_item:",
        "external_publication_intake_item:",
    ):
        assert any(pk.startswith(prefix) for pk in reads), prefix

    changed = copy.deepcopy(payload)
    changed["run"]["purpose"] += " changed"
    conflict = mod.prepare_import(changed, input_bytes=json.dumps(changed).encode("utf-8"))
    with pytest.raises(mod.ContractError, match="request_id conflict"):
        mod.execute_import(conflict)
    assert durable == after_first


def test_existing_intake_is_only_snapshotted_into_correction_not_mutated(monkeypatch) -> None:
    mod = load_module()
    durable, _reads = _install_fake_ydb(mod, monkeypatch)
    _path, raw, payload = package("rg-ru")
    prepared = mod.prepare_import(payload, input_bytes=raw)
    correction = prepared["corrections"][0]
    identity_pk = correction["live_identity_pk"]
    external_id = "extpub_existing"
    intake_pk = "external_publication_intake_item:" + external_id
    durable[identity_pk] = json.dumps({"external_publication_id": external_id})
    durable[intake_pk] = json.dumps({
        "external_publication_id": external_id,
        "canonical_url": correction["canonical_url"],
        "review_status": "unreviewed",
        "publication_permission": "not_granted",
        "decision": {"import_status": "ready_for_region_talk_scoring"},
    })
    before = durable[intake_pk]

    mod.execute_import(prepared)

    assert durable[intake_pk] == before
    stored = json.loads(durable[correction["pk"]])
    assert stored["live_intake_found"] is True
    assert stored["live_external_publication_id"] == external_id
    assert stored["live_intake_snapshot_sha256"] == hashlib.sha256(before.encode()).hexdigest()
    assert stored["review_status"] == "unreviewed"
    assert stored["live_revalidation_status"] == "pending_live_revalidation"
    assert stored["publication_permission"] == "not_granted"


def test_exact_replay_fails_closed_when_atomic_record_is_incomplete(monkeypatch) -> None:
    mod = load_module()
    durable, _reads = _install_fake_ydb(mod, monkeypatch)
    _path, raw, payload = package()
    prepared = mod.prepare_import(payload, input_bytes=raw)
    mod.execute_import(prepared)
    durable.pop(prepared["corrections"][0]["pk"])
    before = dict(durable)

    with pytest.raises(mod.ContractError, match="incomplete exact replay"):
        mod.execute_import(prepared)

    assert durable == before


def test_default_cli_is_dry_run_and_never_opens_ydb(tmp_path: Path, monkeypatch) -> None:
    mod = load_module()
    _path, raw, _payload = package()
    input_path = tmp_path / "profile.json"
    report_path = tmp_path / "report.json"
    input_path.write_bytes(raw)
    monkeypatch.setattr(mod, "execute_import", lambda _prepared: pytest.fail("dry-run opened YDB"))
    monkeypatch.setattr(sys, "argv", [str(MODULE_PATH), str(input_path), "--report", str(report_path)])

    assert mod.main() == 0
    report = json.loads(report_path.read_text())
    assert report["executed"] is False
    assert report["execution_status"] == "validated"
    assert report["written_ydb_rows"] == 0
    assert report["publication_effect"] == "none"


def test_dedicated_workflow_is_trusted_main_oidc_sequential_and_nonpublishing() -> None:
    workflow = (
        ROOT / ".github" / "workflows" / "region-talk-publisher-profile-import.yml"
    ).read_text(encoding="utf-8")

    assert "ref: main" in workflow
    assert "origin/main" in workflow
    assert "region-talk-publisher-profile-enrichment-" in workflow
    assert "region-talk-external-research-result-" not in workflow
    assert workflow.index("Validate every selected profile before Yandex authentication") < workflow.index(
        "Exchange GitHub OIDC token"
    )
    assert "id-token: write" in workflow
    assert "while IFS= read -r input_path" in workflow
    assert workflow.count("--expected-input-sha256") == 2
    assert "--execute" in workflow
    assert "actions/upload-artifact@v4" in workflow
    assert "retention-days: 7" in workflow
    for forbidden in (
        "region_talk_publication_finalizer", "region_talk_goal_notify", "publish_current_registry",
        "TELEGRAM_BOT_TOKEN", "publication_permission=granted",
    ):
        assert forbidden not in workflow
