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
import html
import json
import math
import os
import re
import struct
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

MODEL_ID = "BAAI/bge-m3"
MODEL_REVISION = "5617a9f61b028005a4858fdac845db406aefb181"
EMBEDDING_DIM = 1024
ENCODER_CONTRACT = "bge_m3_cpu_dense_fp32_l2_v1"
DOCUMENT_VERSION = "event-related-doc-v1"
ARTIFACT_SCHEMA_VERSION = "static-event-bge-v1"
COLLECTION_DOCUMENT_KIND = "collection_semantics_v1"
COLLECTION_DOCUMENT_VERSION = "collection-semantics-doc-v1"
COLLECTION_ARTIFACT_SCHEMA_VERSION = "static-collection-bge-v1"
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


def _evidence_text(value: Any) -> str:
    """Compact source-facing text without generated tags or regex properties."""

    if isinstance(value, (list, tuple)):
        value = ", ".join(str(item) for item in value if str(item).strip())
    clean = html.unescape(re.sub(r"<[^>]+>", " ", str(value or "")))
    return " ".join(clean.split())


def build_collection_semantics_v1_document(
    event: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the evidence-only document shared by static collection heads.

    Deliberately excluded fields include ``topics``, ``tags``, audience regex
    properties and any precomputed related digest.  A classifier must therefore
    evaluate event evidence rather than embedding its own generated hint.
    """

    event_id = int(event.get("id") or event.get("event_id") or 0)
    if event_id <= 0:
        raise ValueError("event id must be a positive integer")
    sections = (
        ("title", event.get("title")),
        (
            "description",
            event.get("description")
            or event.get("short_description")
            or event.get("summary")
            or event.get("description_html"),
        ),
        ("event_type", event.get("event_type")),
        ("venue", event.get("venue_name") or event.get("location_name")),
        ("city", event.get("city")),
        ("organizers", event.get("organizer_names")),
        ("participants", event.get("participants")),
    )
    body = " | ".join(
        f"{name}: {clean}" for name, raw in sections if (clean := _evidence_text(raw))
    )
    text = f"collection-event: {body}"
    return {
        "event_id": event_id,
        "document_kind": COLLECTION_DOCUMENT_KIND,
        "document_version": COLLECTION_DOCUMENT_VERSION,
        "text": text,
        "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def build_collection_semantics_v1_documents(
    events: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = [build_collection_semantics_v1_document(event) for event in events]
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


def _normalise(
    vector: Sequence[float], *, expected_dim: int, float32: bool = False
) -> list[float]:
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
    normalised = [value / norm for value in values]
    if float32:
        # Keep the serialized cache contract honest even before the NPZ writer:
        # every scalar is rounded by IEEE-754 binary32, not Python's float64.
        return [struct.unpack("<f", struct.pack("<f", value))[0] for value in normalised]
    return [round(value, 9) for value in normalised]


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
    previous_artifact: Mapping[str, Any] | None = None,
    _documents: Sequence[Mapping[str, Any]] | None = None,
    _document_kind: str = "related_v1",
    _document_version: str = DOCUMENT_VERSION,
    _artifact_schema_version: str = ARTIFACT_SCHEMA_VERSION,
    _float32: bool = False,
) -> dict[str, Any]:
    """Encode only changed related documents/prototypes into one artifact.

    A compatible previous artifact is an input cache, not a second semantic
    source. Rows whose text hash is unchanged are copied verbatim; all changed
    rows are encoded together in one model call. Removed events disappear from
    the new artifact.
    """

    if model_revision != MODEL_REVISION:
        raise ValueError(
            f"model_revision must equal the pinned revision {MODEL_REVISION}"
        )
    documents = list(_documents) if _documents is not None else build_related_v1_documents(events)
    prototypes = _prototype_rows(prototype_bank)
    previous_metadata = (
        previous_artifact.get("metadata", {})
        if isinstance(previous_artifact, Mapping)
        else {}
    )
    previous_event_compatible = bool(
        isinstance(previous_artifact, Mapping)
        and previous_artifact.get("schema_version") == _artifact_schema_version
        and isinstance(previous_metadata, Mapping)
        and previous_metadata.get("encoder_contract") == ENCODER_CONTRACT
        and previous_metadata.get("model_id") == MODEL_ID
        and previous_metadata.get("model_revision") == MODEL_REVISION
        and previous_metadata.get("embedding_dim") == EMBEDDING_DIM
        and previous_metadata.get("document_kind", "related_v1") == _document_kind
        and previous_metadata.get("document_version") == _document_version
        and previous_metadata.get("vector_normalization") == VECTOR_NORMALIZATION
    )
    # Prototype-bank/head churn must never invalidate unchanged event rows.
    # Prototype reuse remains bound to the exact bank independently.
    previous_prototype_compatible = bool(
        previous_event_compatible
        and previous_metadata.get("prototype_bank_sha256") == stable_hash(prototype_bank)
    )
    previous_events = (
        previous_artifact.get("event_vectors", {})
        if previous_event_compatible
        else {}
    )
    previous_prototypes = (
        previous_artifact.get("prototype_vectors", {})
        if previous_prototype_compatible
        else {}
    )
    reusable_events: dict[str, dict[str, Any]] = {}
    reusable_prototypes: dict[str, dict[str, Any]] = {}
    pending: list[tuple[str, str, str, str]] = []
    for row in documents:
        key = str(row["event_id"])
        cached = previous_events.get(key) if isinstance(previous_events, Mapping) else None
        if isinstance(cached, Mapping) and cached.get("text_hash") == row["text_hash"]:
            reusable_events[key] = {
                "text_hash": row["text_hash"],
                "vector": _normalise(
                    cached.get("vector", []),
                    expected_dim=EMBEDDING_DIM,
                    float32=_float32,
                ),
            }
        else:
            pending.append(("event", key, row["text"], row["text_hash"]))
    for row in prototypes:
        key = row["prototype_id"]
        cached = previous_prototypes.get(key) if isinstance(previous_prototypes, Mapping) else None
        if isinstance(cached, Mapping) and cached.get("text_hash") == row["text_hash"]:
            reusable_prototypes[key] = {
                "text_hash": row["text_hash"],
                "vector": _normalise(
                    cached.get("vector", []),
                    expected_dim=EMBEDDING_DIM,
                    float32=_float32,
                ),
            }
        else:
            pending.append(("prototype", key, row["text"], row["text_hash"]))

    encoded_rows: list[list[float]] = []
    if pending:
        encode = encoder or _default_encoder
        raw_vectors = encode(
            [row[2] for row in pending],
            model_revision=model_revision,
            batch_size=max(1, int(batch_size)),
        )
        if len(raw_vectors) != len(pending):
            raise ValueError("encoder result count does not match changed input count")
        encoded_rows = [
            _normalise(vector, expected_dim=EMBEDDING_DIM, float32=_float32)
            for vector in raw_vectors
        ]
    event_vectors = dict(reusable_events)
    prototype_vectors = dict(reusable_prototypes)
    for (kind, key, _text, text_hash), vector in zip(pending, encoded_rows):
        target = event_vectors if kind == "event" else prototype_vectors
        target[key] = {"text_hash": text_hash, "vector": vector}
    metadata = {
        "schema_version": _artifact_schema_version,
        "encoder_contract": ENCODER_CONTRACT,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_kind": _document_kind,
        "document_version": _document_version,
        "vector_normalization": VECTOR_NORMALIZATION,
        "event_cache_identity_sha256": stable_hash(
            {
                "encoder_contract": ENCODER_CONTRACT,
                "model_id": MODEL_ID,
                "model_revision": MODEL_REVISION,
                "embedding_dim": EMBEDDING_DIM,
                "document_kind": _document_kind,
                "document_version": _document_version,
                "vector_normalization": VECTOR_NORMALIZATION,
                "dtype": "float32" if _float32 else "json-number",
            }
        ),
        "prototype_bank_schema_version": prototype_bank.get("schema_version"),
        "prototype_bank_sha256": stable_hash(prototype_bank),
        "classifier_sha256": stable_hash(classifier),
        "event_count": len(event_vectors),
        "prototype_count": len(prototype_vectors),
        "provider_calls": 0,
        "encoded_event_count": sum(row[0] == "event" for row in pending),
        "reused_event_count": len(reusable_events),
        "encoded_prototype_count": sum(row[0] == "prototype" for row in pending),
        "reused_prototype_count": len(reusable_prototypes),
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
        "schema_version": _artifact_schema_version,
        "metadata": metadata,
        "event_vectors": event_vectors,
        "prototype_vectors": prototype_vectors,
    }


def build_collection_bge_vector_artifact(
    events: Iterable[Mapping[str, Any]],
    prototype_bank: Mapping[str, Any],
    *,
    model_revision: str,
    classifier: Mapping[str, Any],
    encoder: Callable[..., Sequence[Sequence[float]]] | None = None,
    batch_size: int = 8,
    build_metadata: Mapping[str, Any] | None = None,
    previous_artifact: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the collection semantic matrix with float32-bound values."""

    event_rows = list(events)
    documents = build_collection_semantics_v1_documents(event_rows)
    return build_shared_bge_vector_artifact(
        event_rows,
        prototype_bank,
        model_revision=model_revision,
        classifier=classifier,
        encoder=encoder,
        batch_size=batch_size,
        build_metadata=build_metadata,
        previous_artifact=previous_artifact,
        _documents=documents,
        _document_kind=COLLECTION_DOCUMENT_KIND,
        _document_version=COLLECTION_DOCUMENT_VERSION,
        _artifact_schema_version=COLLECTION_ARTIFACT_SCHEMA_VERSION,
        _float32=True,
    )


def validate_shared_bge_vector_artifact(
    artifact: Mapping[str, Any],
    *,
    prototype_bank: Mapping[str, Any],
    expected_classifier_sha256: str | None = None,
    _document_kind: str = "related_v1",
    _document_version: str = DOCUMENT_VERSION,
    _artifact_schema_version: str = ARTIFACT_SCHEMA_VERSION,
    _require_float32: bool = False,
) -> dict[str, Any]:
    """Validate exact vector-space and payload hashes at a consumer boundary."""

    errors: list[str] = []
    metadata = artifact.get("metadata")
    if not isinstance(metadata, Mapping):
        return {"valid": False, "errors": ["missing vector metadata"]}
    expected = {
        "schema_version": _artifact_schema_version,
        "encoder_contract": ENCODER_CONTRACT,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "embedding_dim": EMBEDDING_DIM,
        "document_kind": _document_kind,
        "document_version": _document_version,
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
                _normalise(
                    row["vector"],
                    expected_dim=EMBEDDING_DIM,
                    float32=_require_float32,
                )
                if _require_float32 and any(
                    float(value)
                    != struct.unpack("<f", struct.pack("<f", float(value)))[0]
                    for value in row["vector"]
                ):
                    errors.append(f"{collection_name} vector {key} is not float32-bound")
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


def validate_collection_bge_vector_artifact(
    artifact: Mapping[str, Any],
    *,
    prototype_bank: Mapping[str, Any],
    expected_classifier_sha256: str | None = None,
) -> dict[str, Any]:
    return validate_shared_bge_vector_artifact(
        artifact,
        prototype_bank=prototype_bank,
        expected_classifier_sha256=expected_classifier_sha256,
        _document_kind=COLLECTION_DOCUMENT_KIND,
        _document_version=COLLECTION_DOCUMENT_VERSION,
        _artifact_schema_version=COLLECTION_ARTIFACT_SCHEMA_VERSION,
        _require_float32=True,
    )


def write_collection_bge_cache(
    artifact: Mapping[str, Any], *, npz_path: str | Path, receipt_path: str | Path
) -> dict[str, Any]:
    """Write the collection cache as float32 and return its hash receipt."""

    import numpy as np  # type: ignore

    metadata = artifact.get("metadata")
    if not isinstance(metadata, Mapping):
        raise ValueError("collection artifact metadata missing")
    events = artifact.get("event_vectors")
    prototypes = artifact.get("prototype_vectors")
    if not isinstance(events, Mapping) or not isinstance(prototypes, Mapping):
        raise ValueError("collection artifact vectors missing")
    event_ids = sorted(events, key=int)
    prototype_ids = sorted(prototypes)
    target = Path(npz_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with temporary.open("wb") as handle:
        np.savez_compressed(
            handle,
            event_ids=np.asarray(event_ids),
            event_vectors=np.asarray(
                [events[event_id]["vector"] for event_id in event_ids], dtype=np.float32
            ),
            prototype_ids=np.asarray(prototype_ids),
            prototype_vectors=np.asarray(
                [prototypes[prototype_id]["vector"] for prototype_id in prototype_ids],
                dtype=np.float32,
            ),
        )
    os.replace(temporary, target)
    receipt = {
        "schema_version": "static-collection-bge-cache-receipt-v1",
        "artifact_sha256": metadata.get("artifact_sha256"),
        "event_cache_identity_sha256": metadata.get("event_cache_identity_sha256"),
        "model_id": metadata.get("model_id"),
        "model_revision": metadata.get("model_revision"),
        "encoder_contract": metadata.get("encoder_contract"),
        "document_kind": metadata.get("document_kind"),
        "document_version": metadata.get("document_version"),
        "embedding_dim": metadata.get("embedding_dim"),
        "dtype": "float32",
        "event_text_hashes": {
            event_id: events[event_id]["text_hash"] for event_id in event_ids
        },
        "prototype_text_hashes": {
            prototype_id: prototypes[prototype_id]["text_hash"]
            for prototype_id in prototype_ids
        },
        "npz_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
    }
    receipt_target = Path(receipt_path)
    receipt_target.parent.mkdir(parents=True, exist_ok=True)
    temporary_receipt = receipt_target.with_name(
        f".{receipt_target.name}.{os.getpid()}.tmp"
    )
    temporary_receipt.write_text(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temporary_receipt, receipt_target)
    return receipt


def validate_collection_bge_cache(
    *, npz_path: str | Path, receipt: Mapping[str, Any]
) -> dict[str, Any]:
    """Validate dimensions and the physical float32 dtype of a persisted cache."""

    import numpy as np  # type: ignore

    errors: list[str] = []
    path = Path(npz_path)
    if receipt.get("schema_version") != "static-collection-bge-cache-receipt-v1":
        errors.append("cache receipt schema mismatch")
    if receipt.get("dtype") != "float32":
        errors.append("cache receipt dtype mismatch")
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != receipt.get(
        "npz_sha256"
    ):
        errors.append("cache file hash mismatch")
        return {"valid": False, "errors": errors}
    with np.load(path, allow_pickle=False) as stored:
        for key in ("event_vectors", "prototype_vectors"):
            matrix = stored[key]
            if matrix.dtype != np.dtype("float32"):
                errors.append(f"{key} dtype must be float32")
            if matrix.ndim != 2 or matrix.shape[1] != EMBEDDING_DIM:
                errors.append(f"{key} dimension mismatch")
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
