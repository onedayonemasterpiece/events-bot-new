from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "site" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import static_collection_batch as batch_module
import static_event_bge as bge


def _encoder_calls(calls):
    def encode(texts, **_kwargs):
        calls.append(list(texts))
        rows = []
        for index, _text in enumerate(texts):
            row = [0.0] * bge.EMBEDDING_DIM
            row[index % bge.EMBEDDING_DIM] = 1.0
            rows.append(row)
        return rows

    return encode


def _bank(text="odd"):
    return {
        "schema_version": "collection-prototypes-v1",
        "prototypes": [{"id": "unusual:positive:1", "text": text}],
    }


def test_collection_document_is_evidence_only_and_ignores_generated_hints():
    base = {
        "id": 7,
        "title": "Ночная экскурсия",
        "description": "Вход в башню с экскурсоводом",
        "event_type": "экскурсия",
        "venue_name": "Башня",
        "city": "Калининград",
        "topics": ["Детям", "Наука"],
        "tags": ["generated"],
        "audience_properties": "regex: kids",
    }
    changed = {
        **base,
        "topics": ["Другое"],
        "tags": ["another"],
        "audience_properties": "regex: adults",
    }

    left = bge.build_collection_semantics_v1_document(base)
    right = bge.build_collection_semantics_v1_document(changed)

    assert left == right
    assert left["document_kind"] == "collection_semantics_v1"
    assert "Детям" not in left["text"]
    assert "regex" not in left["text"]


def test_collection_event_cache_survives_prototype_and_head_only_changes():
    events = [{"id": 1, "title": "Event", "description": "Evidence"}]
    first_calls = []
    first = bge.build_collection_bge_vector_artifact(
        events,
        _bank("first prototype"),
        model_revision=bge.MODEL_REVISION,
        classifier={"head": "v1"},
        encoder=_encoder_calls(first_calls),
    )
    second_calls = []
    second = bge.build_collection_bge_vector_artifact(
        events,
        _bank("changed prototype"),
        model_revision=bge.MODEL_REVISION,
        classifier={"head": "v2"},
        encoder=_encoder_calls(second_calls),
        previous_artifact=first,
    )

    assert first["metadata"]["encoded_event_count"] == 1
    assert second["metadata"]["encoded_event_count"] == 0
    assert second["metadata"]["reused_event_count"] == 1
    assert second["metadata"]["encoded_prototype_count"] == 1
    assert len(second_calls) == 1
    assert second_calls[0] == ["changed prototype"]
    assert second["metadata"]["event_cache_identity_sha256"] == first["metadata"]["event_cache_identity_sha256"]


def test_collection_vectors_and_npz_are_float32(tmp_path):
    artifact = bge.build_collection_bge_vector_artifact(
        [{"id": 1, "title": "Event", "description": "Evidence"}],
        _bank(),
        model_revision=bge.MODEL_REVISION,
        classifier={"head": "v1"},
        encoder=_encoder_calls([]),
    )
    validation = bge.validate_collection_bge_vector_artifact(
        artifact,
        prototype_bank=_bank(),
        expected_classifier_sha256=bge.stable_hash({"head": "v1"}),
    )
    assert validation == {"valid": True, "errors": []}

    npz_path = tmp_path / "vectors.npz"
    receipt_path = tmp_path / "receipt.json"
    receipt = bge.write_collection_bge_cache(
        artifact, npz_path=npz_path, receipt_path=receipt_path
    )
    with np.load(npz_path, allow_pickle=False) as stored:
        assert stored["event_vectors"].dtype == np.dtype("float32")
        assert stored["prototype_vectors"].dtype == np.dtype("float32")
    assert bge.validate_collection_bge_cache(
        npz_path=npz_path, receipt=receipt
    ) == {"valid": True, "errors": []}


def _sha(char):
    return char * 64


def test_collection_batch_separates_gates_and_validates_last_good_and_empty():
    batch = batch_module.build_collection_batch(
        catalog_hash=_sha("a"),
        policy_hash=_sha("b"),
        generated_at="2026-08-01T12:00:00Z",
        labels={
            "unusual": {
                "strategy": "semantic_bge",
                "compute_status": "pass",
                "quality_status": "pass",
                "publication_status": "shadow",
                "item_ids": [2],
                "hashes": {
                    "catalog_input_sha256": _sha("a"),
                    "model_sha256": _sha("c"),
                    "document_contract_sha256": _sha("d"),
                    "prototype_bank_sha256": _sha("e"),
                    "head_sha256": _sha("f"),
                },
            },
            "science": {
                "strategy": "semantic_bge",
                "compute_status": "pass",
                "quality_status": "pass",
                "publication_status": "ready",
                "item_ids": [],
                "approved_empty": True,
                "approved_empty_reason": "eligible supply was checked and empty",
                "verified_supply_count": 0,
            },
        },
        egress_receipt={"source_bytes": 123, "provider_calls": 0},
    )

    assert batch_module.validate_collection_batch(
        batch, catalog_item_ids=[1, 2], require_compute=True
    ) == {"valid": True, "errors": []}
    assert batch["labels"]["unusual"]["publication_status"] == "shadow"
    assert batch["labels"]["unusual"]["compute_status"] == "pass"

    failed = json.loads(json.dumps(batch))
    failed["labels"]["science"]["compute_status"] = "failed"
    failed["labels"]["science"]["failure_codes"] = ["encoder_failed"]
    failed["labels"]["science"]["last_good"] = {
        "status": "used",
        "batch_sha256": _sha("9"),
        "item_ids": [],
    }
    failed["batch_sha256"] = batch_module.stable_hash(
        {key: value for key, value in failed.items() if key != "batch_sha256"}
    )
    result = batch_module.validate_collection_batch(
        failed, catalog_item_ids=[1, 2], require_compute=True
    )
    assert result == {"valid": True, "errors": []}

    failed["labels"]["science"]["last_good"] = {"status": "absent"}
    failed["batch_sha256"] = batch_module.stable_hash(
        {key: value for key, value in failed.items() if key != "batch_sha256"}
    )
    result = batch_module.validate_collection_batch(
        failed, catalog_item_ids=[1, 2], require_compute=True
    )
    assert not result["valid"]
    assert any("compute pass or used last-good" in error for error in result["errors"])


def test_kernel_requires_and_validates_collection_receipt_for_pgvector_without_unusual(
    tmp_path, monkeypatch
):
    builder_path = ROOT / "kaggle" / "StaticSiteBuilder" / "static_site_builder.py"
    spec = importlib.util.spec_from_file_location("collection_kernel_test", builder_path)
    assert spec and spec.loader
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)

    site_dir = tmp_path / "site"
    data_dir = site_dir / "src" / "data"
    scripts_dir = site_dir / "scripts"
    data_dir.mkdir(parents=True)
    scripts_dir.mkdir(parents=True)
    (scripts_dir / "static_collection_batch.py").write_text(
        (SCRIPTS / "static_collection_batch.py").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    batch = batch_module.build_collection_batch(
        catalog_hash=_sha("a"),
        labels={
            "unusual": {
                "strategy": "semantic_bge",
                "compute_status": "pass",
                "quality_status": "pass",
                "publication_status": "shadow",
                "item_ids": [1],
            }
        },
    )
    batch_module.write_collection_batch(data_dir / "collection-batch-v1.json", batch)
    (data_dir / "production-catalog.json").write_text(
        json.dumps({"eligible": [{"id": 1}]}), encoding="utf-8"
    )
    working = tmp_path / "working"
    working.mkdir()
    monkeypatch.setattr(builder, "SITE_DIR", site_dir)
    monkeypatch.setattr(builder, "WORKING", working)
    sys.modules.pop("static_collection_batch", None)

    receipt = builder.read_collection_semantic_receipt(
        {
            "profile": "production-candidate",
            "related_mode": "pgvector",
            "unusual_enabled": False,
            "collection_semantic_compute": True,
        }
    )

    assert receipt["status"] == "validated"
    assert receipt["collection_batch_sha256"] == builder.sha256_file(
        working / "collection-batch-v1.json"
    )
    assert receipt["batch_contract_sha256"] == batch["batch_sha256"]


def test_runner_persists_collection_receipt_without_requiring_unusual_outputs(
    tmp_path,
):
    from types import SimpleNamespace
    from scripts import run_static_site_builder_kaggle as runner

    out_dir = tmp_path / "output"
    out_dir.mkdir()
    for name, content in {
        "static_event_bge_vectors.npz": b"npz",
        "static_event_bge_vectors.receipt.json": b"receipt",
        "collection-batch-v1.json": b"batch",
    }.items():
        (out_dir / name).write_bytes(content)
    sha = runner.sha256_file
    result = {
        "semantic": {
            "vector_cache_sha256": sha(out_dir / "static_event_bge_vectors.npz"),
            "vector_receipt_sha256": sha(
                out_dir / "static_event_bge_vectors.receipt.json"
            ),
            "collection_batch_sha256": sha(out_dir / "collection-batch-v1.json"),
        }
    }
    args = SimpleNamespace(
        bge_vector_cache=str(tmp_path / "durable" / "vectors.npz"),
        bge_vector_receipt=str(tmp_path / "durable" / "receipt.json"),
        unusual_cache=str(tmp_path / "durable" / "unusual.json"),
        unusual_last_good=str(tmp_path / "durable" / "unusual-last-good.json"),
        collection_batch=str(tmp_path / "durable" / "collection-batch-v1.json"),
        collection_batch_last_good=str(
            tmp_path / "durable" / "collection-batch-last-good.json"
        ),
        collection_semantic_compute=True,
        unusual_enabled=False,
    )

    runner.persist_semantic_outputs(out_dir, args, result)

    assert Path(args.collection_batch).read_bytes() == b"batch"
    assert not Path(args.unusual_cache).exists()
