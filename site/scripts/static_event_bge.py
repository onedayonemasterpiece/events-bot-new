#!/usr/bin/env python3
"""Shared, hash-bound BGE-M3 vectors for static event consumers.

This module is the *only* BGE encoding boundary for the static-site consumers
introduced with the unusual-events feed.  It deliberately builds the existing
``related_v1`` document contract from the canonical vector-sync implementation
and encodes event documents and consumer prototype documents in one model
session.  Scorers consume the resulting artifact and must never instantiate an
encoder themselves.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

MODEL_ID = "BAAI/bge-m3"
MODEL_REVISION = "5617a9f61b028005a4858fdac845db406aefb181"
EMBEDDING_DIM = 1024
ENCODER_CONTRACT = "bge_m3_cpu_dense_fp32_l2_v1"
DOCUMENT_VERSION = "event-related-doc-v1"
ARTIFACT_SCHEMA_VERSION = "static-event-bge-v1"
VECTOR_NORMALIZATION = "l2"
_ROOT = Path(__file__).resolve().parents[2]


def stable_hash(value: Any) -> str:
    """Return a canonical SHA-256 for JSON-compatible data."""

    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _related_contract():
    """Import the canonical document functions without copying their policy."""

    # ``scripts`` is a namespace package when the repository root is on
    # sys.path.  A normal import keeps document construction bound to the
    # already shipped search/vector contract and avoids a second drifting copy.
    import sys

    root = str(_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    from scripts.sync_event_search_vectors_to_supabase import (  # type: ignore
        build_related_digest,
        clean_text,
        event_tags,
        ru_event_category,
    )

    return clean_text, ru_event_category, event_tags, build_related_digest


def build_related_v1_document(event: Mapping[str, Any]) -> dict[str, Any]:
    """Build the exact ``related_v1`` document and its immutable text hash."""

    clean_text, ru_event_category, event_tags, build_related_digest = _related_contract()
    payload = dict(event)
    category = ru_event_category(payload)
    tags = event_tags(payload, category)
    digest = build_related_digest(payload, category, tags)
    text = f"related-event: title: {clean_text(payload.get('title'))} | text: {digest}"
    event_id = int(payload.get("id") or payload.get("event_id") or 0)
    if event_id <= 0:
        raise ValueError("event id must be a positive integer")
    return {
        "event_id": event_id,
        "document_version": DOCUMENT_VERSION,
        "text": text,
        "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def build_related_v1_documents(
    events: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build documents in stable event-id order and reject duplicate ids."""

    rows = [build_related_v1_document(event) for event in events]
    rows.sort(key=lambda row: int(row["event_id"]))
    ids = [int(row["event_id"]) for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("events contain duplicate ids")
    return rows


def _prototype_rows(prototype_bank: Mapping[str, Any]) -> list[dict[str, str]]:
    prototypes = prototype_bank.get("prototypes")
    if not isinstance(prototypes, list) or not prototypes:
        raise ValueError("prototype bank must contain a non-empty prototypes list")
    rows: list[dict[str, str]] = []
    for raw in prototypes:
        if not isinstance(raw, Mapping):
            raise ValueError("prototype entries must be objects")
        prototype_id = str(raw.get("id") or "").strip()
        text = str(raw.get("text") or "").strip()
        if not prototype_id or not text:
            raise ValueError("every prototype requires id and text")
        rows.append(
            {
                "prototype_id": prototype_id,
                "text": text,
                "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            }
        )
    rows.sort(key=lambda row: row["prototype_id"])
    ids = [row["prototype_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("prototype ids must be unique")
    return rows


def _normalise(vector: Sequence[float], *, expected_dim: int) -> list[float]:
    if len(vector) != expected_dim:
        raise ValueError(
            f"encoder returned dimension {len(vector)} instead of {expected_dim}"
        )
    values = [float(value) for value in vector]
    if not all(math.isfinite(value) for value in values):
        raise ValueError("encoder returned a non-finite vector")
    norm = math.sqrt(sum(value * value for value in values))
    if norm <= 1e-12:
        raise ValueError("encoder returned a zero vector")
    return [round(value / norm, 9) for value in values]


def _default_encoder(
    texts: Sequence[str], *, model_revision: str, batch_size: int
) -> Sequence[Sequence[float]]:
    """Load pinned BGE-M3 locally and return dense FP32 vectors.

    Imports stay inside this boundary so importing either static consumer does
    not download a model or require FlagEmbedding.
    """

    if model_revision != MODEL_REVISION:
        raise ValueError("unpinned BGE-M3 model revision is forbidden")
    from huggingface_hub import snapshot_download  # type: ignore
    from FlagEmbedding import BGEM3FlagModel  # type: ignore

    local_model = snapshot_download(repo_id=MODEL_ID, revision=model_revision)
    model = BGEM3FlagModel(local_model, use_fp16=False)
    encoded = model.encode(
        list(texts),
        batch_size=max(1, int(batch_size)),
        max_length=2048,
        return_dense=True,
        return_sparse=False,
        return_colbert_vecs=False,
    )
    return encoded["dense_vecs"]


def build_shared_bge_vector_artifact(
    events: Iterable[Mapping[str, Any]],
    prototype_bank: Mapping[str, Any],
    *,
    model_revision: str,
    classifier: Mapping[str, Any],
    encoder: Callable[..., Sequence[Sequence[float]]] | None = None,
    batch_size: int = 8,
    build_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Encode related documents and prototypes once into a shared artifact."""

    if model_revision != MODEL_REVISION:
        raise ValueError(
            f"model_revision must equal the pinned revision {MODEL_REVISION}"
        )
    documents = build_related_v1_documents(events)
    prototypes = _prototype_rows(prototype_bank)
    texts = [row["text"] for row in documents] + [row["text"] for row in prototypes]
    encode = encoder or _default_encoder
    raw_vectors = encode(
        texts, model_revision=model_revision, batch_size=max(1, int(batch_size))
    )
    if len(raw_vectors) != len(texts):
        raise ValueError("encoder result count does not match input count")
    vectors = [_normalise(vector, expected_dim=EMBEDDING_DIM) for vector in raw_vectors]
    event_vectors = {
        str(row["event_id"]): {
            "text_hash": row["text_hash"],
            "vector": vectors[index],
        }
        for index, row in enumerate(documents)
    }
    offset = len(documents)
    prototype_vectors = {
        row["prototype_id"]: {
            "text_hash": row["text_hash"],
            "vector": vectors[offset + index],
        }
        for index, row in enumerate(prototypes)
    }
    metadata = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "encoder_contract": ENCODER_CONTRACT,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_version": DOCUMENT_VERSION,
        "vector_normalization": VECTOR_NORMALIZATION,
        "prototype_bank_schema_version": prototype_bank.get("schema_version"),
        "prototype_bank_sha256": stable_hash(prototype_bank),
        "classifier_sha256": stable_hash(classifier),
        "event_count": len(event_vectors),
        "prototype_count": len(prototype_vectors),
        "provider_calls": 0,
        "build": dict(build_metadata or {}),
    }
    payload_hash = stable_hash(
        {
            "metadata": metadata,
            "event_vectors": event_vectors,
            "prototype_vectors": prototype_vectors,
        }
    )
    metadata["artifact_sha256"] = payload_hash
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "metadata": metadata,
        "event_vectors": event_vectors,
        "prototype_vectors": prototype_vectors,
    }


def validate_shared_bge_vector_artifact(
    artifact: Mapping[str, Any],
    *,
    prototype_bank: Mapping[str, Any],
    expected_classifier_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate exact vector-space and payload hashes at a consumer boundary."""

    errors: list[str] = []
    metadata = artifact.get("metadata")
    if not isinstance(metadata, Mapping):
        return {"valid": False, "errors": ["missing vector metadata"]}
    expected = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "encoder_contract": ENCODER_CONTRACT,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_version": DOCUMENT_VERSION,
        "vector_normalization": VECTOR_NORMALIZATION,
        "prototype_bank_sha256": stable_hash(prototype_bank),
    }
    for key, value in expected.items():
        actual = artifact.get("schema_version") if key == "schema_version" else metadata.get(key)
        if actual != value:
            errors.append(f"{key} mismatch")
    if expected_classifier_sha256 is not None and metadata.get(
        "classifier_sha256"
    ) != expected_classifier_sha256:
        errors.append("classifier_sha256 mismatch")
    event_vectors = artifact.get("event_vectors")
    prototype_vectors = artifact.get("prototype_vectors")
    if not isinstance(event_vectors, Mapping):
        errors.append("event_vectors must be an object")
        event_vectors = {}
    if not isinstance(prototype_vectors, Mapping):
        errors.append("prototype_vectors must be an object")
        prototype_vectors = {}
    for collection_name, collection in (
        ("event", event_vectors),
        ("prototype", prototype_vectors),
    ):
        for key, row in collection.items():
            if not isinstance(row, Mapping) or not isinstance(row.get("vector"), list):
                errors.append(f"{collection_name} vector {key} has invalid shape")
                continue
            try:
                _normalise(row["vector"], expected_dim=EMBEDDING_DIM)
            except (TypeError, ValueError) as exc:
                errors.append(f"{collection_name} vector {key}: {exc}")
    declared_hash = metadata.get("artifact_sha256")
    unhashed_metadata = dict(metadata)
    unhashed_metadata.pop("artifact_sha256", None)
    actual_hash = stable_hash(
        {
            "metadata": unhashed_metadata,
            "event_vectors": event_vectors,
            "prototype_vectors": prototype_vectors,
        }
    )
    if declared_hash != actual_hash:
        errors.append("artifact_sha256 mismatch")
    return {"valid": not errors, "errors": errors}


def _load_json(path: Path) -> Any:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, (dict, list)):
        raise ValueError(f"{path} must contain a JSON object or list")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--prototype-bank", type=Path, required=True)
    parser.add_argument("--classifier", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model-revision", required=True)
    parser.add_argument("--batch-size", type=int, default=8)
    args = parser.parse_args(argv)
    events_payload = _load_json(args.events)
    events = events_payload.get("events", events_payload) if isinstance(events_payload, dict) else events_payload
    if not isinstance(events, list):
        raise ValueError("--events must contain a list or {events: [...]}")
    bank = _load_json(args.prototype_bank)
    if not isinstance(bank, dict):
        raise ValueError("--prototype-bank must contain an object")
    classifier = _load_json(args.classifier)
    if not isinstance(classifier, dict):
        raise ValueError("--classifier must contain an object")
    artifact = build_shared_bge_vector_artifact(
        events,
        bank,
        model_revision=args.model_revision,
        classifier=classifier,
        batch_size=args.batch_size,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
