from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPORTER = ROOT / "site" / "scripts" / "export-production-preview-data.py"


def canonical_hash(value) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def load_exporter():
    spec = importlib.util.spec_from_file_location("static_unusual_adapter_test", EXPORTER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def install_fake_semantic_modules(monkeypatch):
    calls = {"encode": 0, "score": 0, "quality": 0}
    bank = {
        "schema_version": "unusual-prototypes-v1",
        "taxonomy_version": "unusual-taxonomy-v1",
        "prototypes": [{"id": "odd", "text": "unusual"}],
    }
    classifier = {
        "schema_version": "unusual-classifier-v1",
        "policy_version": "unusual-policy-v1",
    }
    bge = types.ModuleType("static_event_bge")
    bge.MODEL_ID = "BAAI/bge-m3"
    bge.MODEL_REVISION = "1" * 40
    bge.EMBEDDING_DIM = 4
    bge.DOCUMENT_VERSION = "event-related-doc-v1"
    bge.ENCODER_CONTRACT = "bge_m3_cpu_dense_fp32_l2_v1"
    bge.stable_hash = canonical_hash
    bge.build_related_v1_documents = lambda events: [
        {
            "event_id": int(event["id"]),
            "text_hash": hashlib.sha256(str(event["title"]).encode()).hexdigest(),
        }
        for event in sorted(events, key=lambda row: int(row["id"]))
    ]

    def build(events, prototype_bank, *, model_revision, classifier, **kwargs):
        calls["encode"] += 1
        event_vectors = {
            str(event["id"]): {
                "text_hash": hashlib.sha256(str(event["title"]).encode()).hexdigest(),
                "vector": [1.0, 0.0, 0.0, 0.0],
            }
            for event in events
        }
        prototype_vectors = {
            "odd": {
                "text_hash": hashlib.sha256(b"unusual").hexdigest(),
                "vector": [1.0, 0.0, 0.0, 0.0],
            }
        }
        metadata = {
            "encoder_contract": bge.ENCODER_CONTRACT,
            "model_id": bge.MODEL_ID,
            "model_revision": model_revision,
            "embedding_dim": bge.EMBEDDING_DIM,
            "document_version": bge.DOCUMENT_VERSION,
            "prototype_bank_sha256": canonical_hash(prototype_bank),
            "classifier_sha256": canonical_hash(classifier),
            "event_count": len(event_vectors),
            "prototype_count": len(prototype_vectors),
            "provider_calls": 0,
            "build": dict(kwargs.get("build_metadata") or {}),
        }
        metadata["artifact_sha256"] = canonical_hash(
            {
                "metadata": metadata,
                "event_vectors": event_vectors,
                "prototype_vectors": prototype_vectors,
            }
        )
        return {
            "schema_version": "static-event-bge-v1",
            "metadata": metadata,
            "event_vectors": event_vectors,
            "prototype_vectors": prototype_vectors,
        }

    bge.build_shared_bge_vector_artifact = build
    bge.validate_shared_bge_vector_artifact = lambda *_args, **_kwargs: {
        "valid": True,
        "errors": [],
    }
    unusual = types.ModuleType("unusual_event_semantics")
    unusual.load_unusual_prototype_bank = lambda: bank
    unusual.load_unusual_classifier = lambda: classifier

    def evaluate(fixture, artifact, prototype_bank, classifier):
        calls["quality"] += 1
        assert fixture["cases"][0]["label"] == "positive"
        assert artifact["metadata"]["build"]["evidence_kind"] == "real_bge_canary"
        assert "99" in artifact["event_vectors"]
        return {
            "evidence_kind": "real_bge_canary",
            "artifact_sha256": artifact["metadata"]["artifact_sha256"],
            "prototype_bank_sha256": canonical_hash(prototype_bank),
            "classifier_sha256": canonical_hash(classifier),
            "editorial_sample_size": 1,
        }

    unusual.evaluate_unusual_quality_fixture = evaluate

    def score(events, event_vectors, prototype_vectors, metadata, **kwargs):
        calls["score"] += 1
        assert event_vectors is kwargs.pop("_expected_vectors", event_vectors)
        assert metadata["provider_calls"] == 0
        assert kwargs["build_metadata"]["as_of_date"] == "2026-07-27"
        assert kwargs["build_metadata"]["quality_evaluation"]["evidence_kind"] == "real_bge_canary"
        return {
            "manifest": {
                "status": "approved",
                "quality_gate": {"status": "approved", "metrics": {"precision": 1.0}},
                "items": [
                    {
                        "event_id": int(events[0]["id"]),
                        "concept_id": "concept:test",
                        "representative_event_id": int(events[0]["id"]),
                        "tier": "confident",
                        "unusual_score": 0.95,
                        "confidence": 0.9,
                        "families": ["format"],
                        "reason_codes": ["prototype:odd"],
                        "prototype_evidence": [{"prototype_id": "odd", "score": 0.95}],
                        "first_published_at": "2026-07-27T00:00:00Z",
                        "notify_eligible": True,
                        "content_hash": "c" * 64,
                    }
                ],
                "shadow_items": [],
            },
            "cache": {"records": {}},
            "metrics": {"provider_calls": 0, "concept_duplicates_removed": 0},
        }

    unusual.score_unusual_manifest = score
    monkeypatch.setitem(sys.modules, "static_event_bge", bge)
    monkeypatch.setitem(sys.modules, "unusual_event_semantics", unusual)
    return calls


def test_adapter_reuses_one_vector_artifact_and_migration_never_notifies(
    tmp_path: Path, monkeypatch, capsys
):
    module = load_exporter()
    calls = install_fake_semantic_modules(monkeypatch)
    events = [
        {
            "id": 7,
            "title": "Иммерсивная прогулка",
            "start_date": "2026-07-28",
            "lifecycle_status": "active",
            "slug": "immersivnaya-progulka",
        }
    ]
    paths = {
        "out_dir": tmp_path / "data",
        "vector_cache_path": tmp_path / "static_event_bge_vectors.npz",
        "vector_receipt_path": tmp_path / "static_event_bge_vectors.receipt.json",
        "unusual_cache_path": tmp_path / "unusual_events_cache.json",
        "unusual_last_good_path": tmp_path / "unusual_events_last_good.json",
        "quality_fixture_path": tmp_path / "unusual_events_golden_v1.json",
    }
    paths["quality_fixture_path"].write_text(
        json.dumps(
            {
                "schema_version": "unusual-events-golden-v1",
                "cases": [
                    {
                        "event_id": 99,
                        "label": "positive",
                        "concept_id": "concept:test",
                        "eligible": True,
                        "frozen_tier": "core_unusual",
                        "facts": {
                            "title": "Театр в заброшенном ангаре",
                            "short_description": "Иммерсивный формат",
                            "date": "2026-07-28",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    build = {
        "build_id": "production-test",
        "generated_at": "2026-07-27T00:00:00Z",
        "as_of_date": "2026-07-27",
        "source_snapshot_id": "snapshot-test",
        "source_snapshot_hash": "a" * 64,
        "input_fingerprint": "b" * 64,
    }
    first_artifact, first_result = module.build_shared_bge_and_unusual(
        events,
        build_metadata=build,
        model_revision="1" * 40,
        batch_size=8,
        migration=True,
        **paths,
    )
    second_artifact, second_result = module.build_shared_bge_and_unusual(
        events,
        build_metadata=build,
        model_revision="1" * 40,
        batch_size=8,
        migration=True,
        **paths,
    )

    assert calls == {"encode": 1, "score": 2, "quality": 2}
    assert first_artifact["metadata"]["event_count"] == 2
    assert first_result["artifact_event_count"] == 2
    assert first_artifact["metadata"]["artifact_sha256"] == second_artifact["metadata"]["artifact_sha256"]
    assert first_result["provider_calls"] == second_result["provider_calls"] == 0
    assert second_result["cache_state"] == "hit"
    manifest = json.loads((paths["out_dir"] / "unusual-events.json").read_text())
    assert manifest["source_snapshot_hash"] == "a" * 64
    assert manifest["doc_kind"] == "related_v1"
    assert manifest["migration"] == {"enabled": True, "notify": False}
    assert manifest["items"][0]["notify_eligible"] is False
    assert manifest["items"][0]["event_snapshot"]["id"] == 7
    assert manifest["items"][0]["path"] == "/sobytiya/immersivnaya-progulka/"
    logs = capsys.readouterr().err
    for stage in (
        "unusual_vector_reuse_start",
        "unusual_prototype_load",
        "unusual_score_complete",
        "unusual_quality_gate",
        "unusual_concept_dedup",
        "unusual_manifest_written",
        "unusual_cache_written",
    ):
        assert f'"stage": "{stage}"' in logs
