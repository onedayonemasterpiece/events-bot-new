"""Canonical event age-rating decisions and fail-closed validation.

The module deliberately separates an organiser/source-declared restriction from
an internal product assessment.  It contains no broad keyword classifier: text
and OCR decisions must arrive from a versioned semantic stage and are accepted
only after schema and evidence grounding checks.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable


VALID_AGE_RESTRICTIONS = frozenset({"0+", "6+", "12+", "16+", "18+"})
AGE_RATING_STATUSES = frozenset(
    {
        "declared",
        "assessed",
        "conflict",
        "insufficient_evidence",
        "unknown",
        "budget_deferred",
    }
)
DECLARED_PROVENANCE = frozenset(
    {
        "official_structured",
        "organizer_text",
        "ticketing_text",
        "venue_text",
        "poster_ocr",
        "manual_override",
    }
)
ASSESSED_PROVENANCE = frozenset({"llm_assessed", "bge_assessed", "model_assessed"})
DEFAULT_DECISION_VERSION = "event-age-decision-v1"
DEFAULT_RUBRIC_VERSION = "ru-436fz-engineering-rubric-v1"


AGE_DECISION_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": [
                "declared",
                "assessed",
                "conflict",
                "insufficient_evidence",
                "unknown",
            ],
        },
        # Gemini's native Schema contract accepts only strings in ``enum``.
        # Nullability is expressed by the union type, not a null enum member.
        "value": {"type": ["string", "null"], "enum": ["0+", "6+", "12+", "16+", "18+"]},
        "provenance": {
            "type": ["string", "null"],
            "enum": [
                "official_structured",
                "organizer_text",
                "ticketing_text",
                "venue_text",
                "poster_ocr",
                "llm_assessed",
            ],
        },
        "confidence": {"type": ["number", "null"]},
        "evidence_quote": {"type": "string"},
        "evidence_kind": {
            "type": ["string", "null"],
            "enum": ["structured", "source_text", "raw_excerpt", "poster_ocr", "content_assessment"],
        },
        "source_document_id": {"type": ["string", "null"]},
        "rubric_codes": {"type": "array", "items": {"type": "string"}},
        "reason_code": {"type": "string"},
    },
    "required": [
        "status",
        "value",
        "provenance",
        "confidence",
        "evidence_quote",
        "evidence_kind",
        "source_document_id",
        "rubric_codes",
        "reason_code",
    ],
    "additionalProperties": False,
}


@dataclass(frozen=True, slots=True)
class AgeRatingDecision:
    status: str
    value: str | None = None
    provenance: str | None = None
    confidence: float | None = None
    source_url: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    decision_version: str = DEFAULT_DECISION_VERSION
    rubric_version: str = DEFAULT_RUBRIC_VERSION
    input_hash: str | None = None
    assessment_engine: str | None = None
    run_id: str | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_age_restriction(value: Any) -> str | None:
    """Narrow format normalizer; never infers a rating from surrounding prose."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        raw = f"{value}+"
    else:
        raw = str(value).strip()
    raw = raw.replace("＋", "+")
    match = re.fullmatch(r"(?:возраст(?:ное ограничение)?\s*[:\-]?\s*)?(0|6|12|16|18)\s*\+", raw, re.I)
    if not match:
        return None
    normalized = f"{match.group(1)}+"
    return normalized if normalized in VALID_AGE_RESTRICTIONS else None


def compact_evidence(
    *,
    kind: str,
    quote: str | None = None,
    source_hash: str | None = None,
    document_id: str | None = None,
    rubric_codes: Iterable[str] = (),
    reason_code: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {"kind": str(kind or "unknown")[:48]}
    if quote:
        out["quote"] = re.sub(r"\s+", " ", str(quote)).strip()[:320]
    if source_hash:
        out["source_hash"] = str(source_hash)[:64]
    if document_id:
        out["document_id"] = str(document_id)[:160]
    codes = [str(code).strip()[:64] for code in rubric_codes if str(code).strip()][:12]
    if codes:
        out["rubric_codes"] = codes
    if reason_code:
        out["reason_code"] = str(reason_code)[:96]
    return out


def age_input_hash(
    *,
    source_type: str | None,
    source_url: str | None,
    source_text: str | None,
    raw_excerpt: str | None = None,
    poster_ocr: Iterable[str] = (),
    rubric_version: str = DEFAULT_RUBRIC_VERSION,
    engine_version: str = DEFAULT_DECISION_VERSION,
) -> str:
    payload = {
        "source_type": str(source_type or "").strip().casefold(),
        "source_url": str(source_url or "").strip(),
        "source_text": re.sub(r"\s+", " ", str(source_text or "")).strip(),
        "raw_excerpt": re.sub(r"\s+", " ", str(raw_excerpt or "")).strip(),
        "poster_ocr": [re.sub(r"\s+", " ", str(x)).strip() for x in poster_ocr if str(x).strip()],
        "rubric_version": rubric_version,
        "engine_version": engine_version,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def declared_structured_decision(
    value: Any,
    *,
    source_url: str | None,
    source_type: str | None,
    input_hash: str | None = None,
) -> AgeRatingDecision | None:
    normalized = normalize_age_restriction(value)
    if normalized is None:
        return None
    raw = str(value).strip()
    source_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return AgeRatingDecision(
        status="declared",
        value=normalized,
        provenance="official_structured",
        confidence=1.0,
        source_url=source_url,
        evidence=compact_evidence(
            kind="structured",
            quote=raw,
            source_hash=source_hash,
            document_id=source_type,
            reason_code="structured_source_field",
        ),
        input_hash=input_hash,
    )


def _quote_is_grounded(quote: str, corpora: Iterable[str]) -> bool:
    needle = re.sub(r"\s+", " ", quote or "").strip().casefold()
    if not needle:
        return False
    return any(
        needle in re.sub(r"\s+", " ", str(text or "")).strip().casefold()
        for text in corpora
        if str(text or "").strip()
    )


def decision_from_semantic_payload(
    payload: Any,
    *,
    source_url: str | None,
    source_corpora: Iterable[str],
    input_hash: str | None,
    decision_version: str = DEFAULT_DECISION_VERSION,
    rubric_version: str = DEFAULT_RUBRIC_VERSION,
) -> AgeRatingDecision | None:
    """Validate a strict semantic decision; malformed/ungrounded data fail closed."""

    if not isinstance(payload, dict):
        return None
    required_keys = set(AGE_DECISION_JSON_SCHEMA["required"])
    allowed_keys = set(AGE_DECISION_JSON_SCHEMA["properties"])
    if required_keys - payload.keys() or payload.keys() - allowed_keys:
        return None
    evidence_kind = payload.get("evidence_kind")
    if evidence_kind not in {None, "structured", "source_text", "raw_excerpt", "poster_ocr", "content_assessment"}:
        return None
    rubric_codes = payload.get("rubric_codes")
    if not isinstance(rubric_codes, list) or any(not isinstance(code, str) for code in rubric_codes):
        return None
    if not isinstance(payload.get("evidence_quote"), str) or not isinstance(
        payload.get("reason_code"), str
    ):
        return None
    if payload.get("source_document_id") is not None and not isinstance(
        payload.get("source_document_id"), str
    ):
        return None
    status = str(payload.get("status") or "").strip()
    if status not in AGE_RATING_STATUSES - {"budget_deferred"}:
        return None
    value = normalize_age_restriction(payload.get("value"))
    provenance = str(payload.get("provenance") or "").strip() or None
    quote = str(payload.get("evidence_quote") or "").strip()
    confidence_raw = payload.get("confidence")
    try:
        confidence = float(confidence_raw) if confidence_raw is not None else None
    except (TypeError, ValueError):
        return None
    if confidence is not None and not 0.0 <= confidence <= 1.0:
        return None

    if status in {"conflict", "insufficient_evidence", "unknown"}:
        if value is not None:
            return None
        provenance = None
    elif status == "declared":
        if value is None or provenance not in DECLARED_PROVENANCE - {"manual_override"}:
            return None
        if not _quote_is_grounded(quote, source_corpora):
            return None
    elif status == "assessed":
        if value is None or provenance != "llm_assessed":
            return None
        if not _quote_is_grounded(quote, source_corpora):
            return None

    evidence = compact_evidence(
        kind=str(evidence_kind or status),
        quote=quote,
        source_hash=input_hash,
        document_id=str(payload.get("source_document_id") or "") or None,
        rubric_codes=rubric_codes,
        reason_code=str(payload.get("reason_code") or "") or None,
    )
    return AgeRatingDecision(
        status=status,
        value=value,
        provenance=provenance,
        confidence=confidence,
        source_url=source_url,
        evidence=evidence,
        decision_version=decision_version,
        rubric_version=rubric_version,
        input_hash=input_hash,
        assessment_engine="smart_update_llm" if status == "assessed" else None,
    )


def apply_age_decision(event: Any, decision: AgeRatingDecision, *, now: datetime | None = None) -> bool:
    """Apply a validated decision and return whether canonical/internal state changed."""

    before = tuple(
        getattr(event, name, None)
        for name in (
            "age_restriction",
            "age_restriction_status",
            "age_restriction_provenance",
            "age_restriction_source_url",
            "age_restriction_confidence",
            "age_restriction_evidence",
            "age_restriction_decision_version",
            "age_restriction_input_hash",
            "age_assessment",
            "age_assessment_status",
            "age_assessment_provenance",
            "age_assessment_confidence",
            "age_assessment_evidence",
            "age_assessment_decision_version",
            "age_assessment_input_hash",
            "age_assessment_engine",
            "age_assessment_run_id",
        )
    )

    # A manual declared value is never displaced automatically.
    manual = getattr(event, "age_restriction_provenance", None) == "manual_override"
    if decision.status == "declared" and not manual:
        event.age_restriction = decision.value
        event.age_restriction_status = "declared"
        event.age_restriction_provenance = decision.provenance
        event.age_restriction_source_url = decision.source_url
        event.age_restriction_confidence = decision.confidence
        event.age_restriction_evidence = decision.evidence
        event.age_restriction_decision_version = decision.decision_version
        event.age_restriction_input_hash = decision.input_hash
    elif decision.status == "assessed":
        event.age_assessment = decision.value
        event.age_assessment_status = "assessed"
        event.age_assessment_provenance = decision.provenance
        event.age_assessment_confidence = decision.confidence
        event.age_assessment_evidence = decision.evidence
        event.age_assessment_decision_version = decision.decision_version
        event.age_assessment_input_hash = decision.input_hash
        event.age_assessment_engine = decision.assessment_engine
        event.age_assessment_run_id = decision.run_id
        if not getattr(event, "age_restriction", None) and not manual:
            event.age_restriction_status = "assessed"
    elif decision.status == "conflict" and not manual:
        event.age_restriction = None
        event.age_restriction_status = "conflict"
        event.age_restriction_provenance = None
        event.age_restriction_source_url = decision.source_url
        event.age_restriction_confidence = decision.confidence
        event.age_restriction_evidence = decision.evidence
        event.age_restriction_decision_version = decision.decision_version
        event.age_restriction_input_hash = decision.input_hash
    elif decision.status in {"insufficient_evidence", "unknown", "budget_deferred"}:
        # Missing evidence from a new source must not erase an existing
        # declaration or a previously accepted internal assessment.
        if (
            not getattr(event, "age_restriction", None)
            and not getattr(event, "age_assessment", None)
            and not manual
        ):
            event.age_restriction_status = decision.status
            event.age_restriction_decision_version = decision.decision_version
            event.age_restriction_input_hash = decision.input_hash
        if not getattr(event, "age_assessment", None):
            event.age_assessment_status = decision.status
            event.age_assessment_input_hash = decision.input_hash

    after = tuple(
        getattr(event, name, None)
        for name in (
            "age_restriction",
            "age_restriction_status",
            "age_restriction_provenance",
            "age_restriction_source_url",
            "age_restriction_confidence",
            "age_restriction_evidence",
            "age_restriction_decision_version",
            "age_restriction_input_hash",
            "age_assessment",
            "age_assessment_status",
            "age_assessment_provenance",
            "age_assessment_confidence",
            "age_assessment_evidence",
            "age_assessment_decision_version",
            "age_assessment_input_hash",
            "age_assessment_engine",
            "age_assessment_run_id",
        )
    )
    changed = before != after
    if changed:
        changed_at = now or utc_now()
        event.age_restriction_updated_at = changed_at
        if decision.status in {
            "assessed",
            "insufficient_evidence",
            "unknown",
            "budget_deferred",
        }:
            event.age_assessment_updated_at = changed_at
    return changed


def reconcile_age_decision(event: Any, decision: AgeRatingDecision) -> AgeRatingDecision:
    """Turn disagreeing declarations into an explicit unresolved conflict.

    Source priority is intentionally *not* encoded here: deciding that two
    values refer to the same occurrence/program is semantic work. A manual
    override remains authoritative; otherwise different declared values fail
    closed until an upstream semantic adjudicator supplies a resolved decision.
    """

    current = normalize_age_restriction(getattr(event, "age_restriction", None))
    current_provenance = getattr(event, "age_restriction_provenance", None)
    if current_provenance == "manual_override":
        return decision
    if (
        getattr(event, "age_restriction_status", None) == "conflict"
        and decision.status == "declared"
    ):
        # Replaying one side of an unresolved conflict is not adjudication.
        # Keep the conflict stable until a manual/explicit resolution workflow
        # is introduced; otherwise the last source to refresh would win.
        return AgeRatingDecision(
            status="conflict",
            value=None,
            provenance=None,
            confidence=getattr(event, "age_restriction_confidence", None),
            source_url=getattr(event, "age_restriction_source_url", None),
            evidence=getattr(event, "age_restriction_evidence", None) or {},
            decision_version=(
                getattr(event, "age_restriction_decision_version", None)
                or decision.decision_version
            ),
            input_hash=(
                getattr(event, "age_restriction_input_hash", None)
                or decision.input_hash
            ),
        )
    if decision.status != "declared" or not current or current == decision.value:
        return decision
    old_evidence = getattr(event, "age_restriction_evidence", None) or {}
    evidence = compact_evidence(
        kind="source_conflict",
        quote=f"{current} <> {decision.value}",
        source_hash=decision.input_hash,
        document_id=decision.source_url,
        reason_code="unresolved_declared_conflict",
    )
    evidence["values"] = [
        {"value": current, "provenance": current_provenance, "evidence": old_evidence},
        {"value": decision.value, "provenance": decision.provenance, "evidence": decision.evidence},
    ]
    return AgeRatingDecision(
        status="conflict",
        value=None,
        provenance=None,
        confidence=None,
        source_url=decision.source_url,
        evidence=evidence,
        decision_version=decision.decision_version,
        rubric_version=decision.rubric_version,
        input_hash=decision.input_hash,
    )


def public_age_projection(event: Any, *, policy: str = "declared_only") -> dict[str, Any]:
    """Return the renderer projection without reparsing descriptions."""

    declared = normalize_age_restriction(getattr(event, "age_restriction", None))
    status = str(getattr(event, "age_restriction_status", None) or "unknown")
    if declared and status == "declared":
        return {"age_restriction": declared, "age_restriction_status": "declared", "age_restriction_provenance": getattr(event, "age_restriction_provenance", None)}
    if policy == "declared_or_assessed_labeled":
        assessed = normalize_age_restriction(getattr(event, "age_assessment", None))
        if assessed:
            return {"age_restriction": None, "age_restriction_status": "assessed", "age_recommendation": assessed, "age_restriction_provenance": getattr(event, "age_assessment_provenance", None)}
    return {"age_restriction": None, "age_restriction_status": status, "age_restriction_provenance": None}
