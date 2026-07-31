from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from festival_web_research.contracts import CheckpointKind, CheckpointRecord
from festival_web_research.evidence import (
    EvidenceValidationError, canonical_json_bytes, canonical_json_sha256,
    normalize_snapshot_text,
)
from festival_web_research.validators import (
    ContractViolation, validate_checkpoint_chain, validate_inventory_conservation,
)


def test_normalizer_is_mechanical_and_hash_bound() -> None:
    normalized = normalize_snapshot_text("Cafe\u0301\r\n next ")
    assert normalized.text == "Café\n next "
    assert normalized.content_sha256 == hashlib.sha256(normalized.text.encode()).hexdigest()
    with pytest.raises(EvidenceValidationError, match="unsupported normalizer"):
        normalize_snapshot_text("x", version="future")


def test_canonical_json_hash_is_order_independent_and_bounded() -> None:
    left = {"б": [1, 2], "a": {"z": True}}
    right = {"a": {"z": True}, "б": [1, 2]}
    assert canonical_json_bytes(left) == canonical_json_bytes(right)
    assert canonical_json_sha256(left) == canonical_json_sha256(right)
    with pytest.raises(EvidenceValidationError, match="byte count"):
        canonical_json_bytes({"x": "large"}, max_bytes=2)
    with pytest.raises(EvidenceValidationError, match="maximum depth"):
        canonical_json_bytes({"x": {"y": 1}}, max_depth=1)
    with pytest.raises(EvidenceValidationError, match="non-finite"):
        canonical_json_bytes({"x": float("nan")})


def test_inventory_union_must_be_conserved() -> None:
    validate_inventory_conservation(
        a_item_ids=["1", "2"], b_item_ids=["x"],
        resolutions={"A:1": "canonical:i1", "A:2": "rejected:D2", "B:x": "unresolved"},
    )
    with pytest.raises(ContractViolation, match="inventory_not_conserved"):
        validate_inventory_conservation(a_item_ids=["1"], b_item_ids=["x"], resolutions={"A:1": "canonical:i"})
    with pytest.raises(ContractViolation, match="invalid_inventory_resolution"):
        validate_inventory_conservation(a_item_ids=["1"], b_item_ids=[], resolutions={"A:1": "dropped"})


def _checkpoint(kind: CheckpointKind, sequence: int, content: bytes, parent: str | None) -> CheckpointRecord:
    return CheckpointRecord(
        checkpoint_id=f"cp{sequence}", kind=kind, sequence=sequence,
        relative_path=f"{sequence}-{kind.value}.json", content_sha256=hashlib.sha256(content).hexdigest(),
        byte_count=len(content), created_at_utc=datetime(2026, 7, 31, tzinfo=UTC),
        parent_sha256=parent,
    )


def test_checkpoint_chain_order_hash_and_mandatory_set() -> None:
    kinds = list(CheckpointKind)
    records = []
    artifacts = {}
    parent = None
    for sequence, kind in enumerate(kinds):
        content = f"{kind.value}\n".encode()
        record = _checkpoint(kind, sequence, content, parent)
        records.append(record)
        artifacts[record.relative_path] = content
        parent = record.content_sha256
    validate_checkpoint_chain(records, artifact_bytes_by_path=artifacts)
    damaged = dict(artifacts)
    damaged[records[2].relative_path] = b"different"
    with pytest.raises(ContractViolation, match="checkpoint_artifact_mismatch"):
        validate_checkpoint_chain(records, artifact_bytes_by_path=damaged)
    with pytest.raises(ContractViolation, match="mandatory_checkpoint_missing"):
        validate_checkpoint_chain(records[:-1], artifact_bytes_by_path={k: v for k, v in artifacts.items() if k != records[-1].relative_path})
    swapped = records.copy()
    swapped[2], swapped[3] = swapped[3].model_copy(update={"sequence": 2}), swapped[2].model_copy(update={"sequence": 3})
    with pytest.raises(ContractViolation, match="checkpoint_stage_regression"):
        validate_checkpoint_chain(swapped, artifact_bytes_by_path=artifacts)


def test_candidate_hash_ignores_workflow_state_but_binds_semantics() -> None:
    from festival_web_research.evidence import candidate_projection_sha256
    base = {"revision": {"candidate_sha256": "old", "status": "shadow", "effective_at": None}, "identity": {"title": "A"}}
    first = candidate_projection_sha256(base)
    assert candidate_projection_sha256({"revision": {"candidate_sha256": "new", "status": "approved", "effective_at": "later"}, "identity": {"title": "A"}}) == first
    assert candidate_projection_sha256({"revision": {}, "identity": {"title": "B"}}) != first
    assert base["revision"]["candidate_sha256"] == "old"


def test_claim_normalization_is_mechanical_only() -> None:
    from festival_web_research.contracts import Claim, QuoteSpan
    from festival_web_research.evidence import validate_claim_normalization
    base = dict(
        claim_id="C", source_id="S", local_subject_id="festival", subject_kind="festival",
        field="title", raw_value="  Name  ", normalized_value="Name", normalization="trim",
        evidence=QuoteSpan(quote="  Name  ", quote_start=0, quote_end=8),
        content_sha256="a" * 64, normalizer_version="festival-text-normalizer-v1",
    )
    validate_claim_normalization(Claim(**base))
    with pytest.raises(EvidenceValidationError, match="invalid trim"):
        validate_claim_normalization(Claim(**{**base, "normalized_value": "Editorial Name"}))
    with pytest.raises(EvidenceValidationError, match="raw scalar"):
        validate_claim_normalization(Claim(**{**base, "evidence": QuoteSpan(quote="Name", quote_start=0, quote_end=4)}))


def test_checkpoint_chain_allows_per_source_review_claim_subject_triplets() -> None:
    kinds = [
        CheckpointKind.STATE, CheckpointKind.SOURCE_LEDGER,
        CheckpointKind.SOURCE_REVIEW, CheckpointKind.CLAIMS, CheckpointKind.SUBJECTS,
        CheckpointKind.SOURCE_REVIEW, CheckpointKind.CLAIMS, CheckpointKind.SUBJECTS,
        CheckpointKind.TOPOLOGY, CheckpointKind.PROGRAMME_INVENTORY,
        CheckpointKind.CANDIDATE, CheckpointKind.RUN_SUMMARY,
    ]
    records, artifacts, parent = [], {}, None
    for sequence, kind in enumerate(kinds):
        content = f"{sequence}:{kind.value}".encode()
        record = _checkpoint(kind, sequence, content, parent).model_copy(update={"checkpoint_id": f"multi{sequence}"})
        records.append(record)
        artifacts[record.relative_path] = content
        parent = record.content_sha256
    validate_checkpoint_chain(records, artifact_bytes_by_path=artifacts)
