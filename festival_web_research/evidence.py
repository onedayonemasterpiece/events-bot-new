"""Hash-bound text normalization, exact quote checks and canonical hashing."""
from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from copy import deepcopy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from datetime import date, time

from .contracts import Claim, SourceSnapshot
from .sources import UnsafeSourceURL, canonicalize_public_url

NORMALIZER_VERSION = "festival-text-normalizer-v1"


class EvidenceValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class NormalizedSnapshot:
    text: str
    content_sha256: str
    normalizer_version: str = NORMALIZER_VERSION


def normalize_snapshot_text(raw: str, *, version: str = NORMALIZER_VERSION) -> NormalizedSnapshot:
    if version != NORMALIZER_VERSION:
        raise EvidenceValidationError(f"unsupported normalizer version: {version}")
    if not isinstance(raw, str):
        raise EvidenceValidationError("snapshot text must be str")
    # Mechanical only: Unicode composition and newline normalization.  Do not
    # trim/collapse whitespace because offsets must remain reproducible.
    text = unicodedata.normalize("NFC", raw.replace("\r\n", "\n").replace("\r", "\n"))
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return NormalizedSnapshot(text=text, content_sha256=digest, normalizer_version=version)


def validate_claim_normalization(claim: Claim) -> None:
    raw = claim.raw_value
    normalized = claim.normalized_value
    if str(raw) not in claim.evidence.quote:
        raise EvidenceValidationError("quote does not contain the raw scalar")
    if claim.normalization == "none":
        if normalized != raw:
            raise EvidenceValidationError("none normalization changed the value")
    elif claim.normalization == "trim":
        if not isinstance(raw, str) or normalized != raw.strip():
            raise EvidenceValidationError("invalid trim normalization")
    elif claim.normalization == "iso_date":
        if not isinstance(normalized, str):
            raise EvidenceValidationError("ISO date normalization must be a string")
        try:
            date.fromisoformat(normalized)
        except ValueError as exc:
            raise EvidenceValidationError("invalid ISO date normalization") from exc
    elif claim.normalization == "iso_time":
        if not isinstance(normalized, str):
            raise EvidenceValidationError("ISO time normalization must be a string")
        try:
            time.fromisoformat(normalized)
        except ValueError as exc:
            raise EvidenceValidationError("invalid ISO time normalization") from exc
    elif claim.normalization == "canonical_url":
        if not isinstance(raw, str) or not isinstance(normalized, str):
            raise EvidenceValidationError("URL normalization requires string values")
        try:
            expected = canonicalize_public_url(raw)
        except UnsafeSourceURL as exc:
            raise EvidenceValidationError("raw URL is unsafe") from exc
        if normalized != expected:
            raise EvidenceValidationError("invalid canonical URL normalization")


def validate_exact_quote(
    claim: Claim,
    source: SourceSnapshot,
    normalized_text: str,
) -> None:
    snapshot = normalize_snapshot_text(normalized_text, version=source.normalizer_version)
    if snapshot.content_sha256 != source.content_sha256:
        raise EvidenceValidationError("snapshot hash does not match source ledger")
    if claim.content_sha256 != source.content_sha256:
        raise EvidenceValidationError("claim is bound to a different snapshot hash")
    if claim.normalizer_version != source.normalizer_version:
        raise EvidenceValidationError("claim normalizer version mismatch")
    if claim.source_id != source.source_id:
        raise EvidenceValidationError("claim source reference mismatch")
    validate_claim_normalization(claim)
    span = claim.evidence
    if span.quote_end > len(snapshot.text):
        raise EvidenceValidationError("quote offsets exceed snapshot")
    if snapshot.text[span.quote_start:span.quote_end] != span.quote:
        raise EvidenceValidationError("quote offsets do not reproduce the exact quote")


def _bounded_walk(value: Any, *, depth: int, max_depth: int, counter: list[int], max_items: int) -> Any:
    if depth > max_depth:
        raise EvidenceValidationError("canonical JSON exceeds maximum depth")
    counter[0] += 1
    if counter[0] > max_items:
        raise EvidenceValidationError("canonical JSON exceeds maximum item count")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EvidenceValidationError("non-finite JSON number")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise EvidenceValidationError("canonical JSON object keys must be strings")
            if key in result:
                raise EvidenceValidationError("duplicate canonical JSON key")
            result[key] = _bounded_walk(child, depth=depth + 1, max_depth=max_depth, counter=counter, max_items=max_items)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_bounded_walk(child, depth=depth + 1, max_depth=max_depth, counter=counter, max_items=max_items) for child in value]
    raise EvidenceValidationError(f"not JSON-compatible: {type(value).__name__}")


def canonical_json_bytes(value: Any, *, max_bytes: int = 4 * 1024 * 1024, max_depth: int = 64, max_items: int = 200_000) -> bytes:
    checked = _bounded_walk(value, depth=0, max_depth=max_depth, counter=[0], max_items=max_items)
    encoded = json.dumps(checked, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(encoded) > max_bytes:
        raise EvidenceValidationError("canonical JSON exceeds maximum byte count")
    return encoded


def canonical_json_sha256(value: Any, **bounds: int) -> str:
    return hashlib.sha256(canonical_json_bytes(value, **bounds)).hexdigest()


def candidate_projection_sha256(candidate: Mapping[str, Any]) -> str:
    """Hash semantic candidate content while excluding mutable workflow state.

    The input is not modified.  The required revision envelope is kept, its
    self-referential hash is nulled, and approval/status timestamps are omitted.
    """
    if not isinstance(candidate, Mapping):
        raise EvidenceValidationError("candidate must be an object")
    projection = deepcopy(dict(candidate))
    revision = projection.get("revision")
    if not isinstance(revision, dict):
        raise EvidenceValidationError("candidate revision must be an object")
    revision["candidate_sha256"] = None
    revision.pop("status", None)
    revision.pop("effective_at", None)
    return canonical_json_sha256(projection)
