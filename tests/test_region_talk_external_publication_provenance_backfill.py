from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

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
