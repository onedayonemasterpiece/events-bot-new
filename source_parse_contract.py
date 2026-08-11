"""Typed, dependency-light contract for one source-level semantic parse.

The contract deliberately contains no provider, ORM, queue, or bot imports.  A
source adapter may therefore use it before importing the large application
module, and every provider path can express technical uncertainty without
overloading an empty event list.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence


PARSE_VERSION = "source-parse-v1"


class SourceDisposition(str, Enum):
    EVENTS_FOUND = "EVENTS_FOUND"
    CONFIRMED_NO_EVENT = "CONFIRMED_NO_EVENT"
    LIFECYCLE_ONLY = "LIFECYCLE_ONLY"
    MIXED = "MIXED"
    RETRY_REQUIRED = "RETRY_REQUIRED"


class LifecycleActionType(str, Enum):
    CANCEL = "CANCEL"
    POSTPONE = "POSTPONE"
    RESCHEDULE_DATE = "RESCHEDULE_DATE"
    RESCHEDULE_TIME = "RESCHEDULE_TIME"
    UPDATE_DETAILS = "UPDATE_DETAILS"


class SourceParseRetryReason(str, Enum):
    EMPTY_PROVIDER_RESPONSE = "EMPTY_PROVIDER_RESPONSE"
    MALFORMED_JSON = "MALFORMED_JSON"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    OUTPUT_TRUNCATED = "OUTPUT_TRUNCATED"
    TECHNICAL_ERROR = "TECHNICAL_ERROR"
    EVIDENCE_INCOMPLETE = "EVIDENCE_INCOMPLETE"
    VERIFICATION_TECHNICAL_ERROR = "VERIFICATION_TECHNICAL_ERROR"
    VERIFICATION_UNCERTAIN = "VERIFICATION_UNCERTAIN"


class SourceNoEventReason(str, Enum):
    NO_ATTENDABLE_EVENT = "NO_ATTENDABLE_EVENT"
    GIVEAWAY_ONLY = "GIVEAWAY_ONLY"
    VAGUE_TEASER = "VAGUE_TEASER"
    REFERRAL_ONLY = "REFERRAL_ONLY"
    SERVICE_OR_RENTAL = "SERVICE_OR_RENTAL"
    RECAP_ONLY = "RECAP_ONLY"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"


class VerificationReason(str, Enum):
    """The seven and only seven semantic contradiction classes."""

    NO_EVENT_WITH_STRONG_SIGNALS = "NO_EVENT_WITH_STRONG_SIGNALS"
    EVENT_DATE_CONFLICT = "EVENT_DATE_CONFLICT"
    MULTIPLE_OCCURRENCES_COLLAPSED = "MULTIPLE_OCCURRENCES_COLLAPSED"
    GENERIC_UNGROUNDED_TITLE = "GENERIC_UNGROUNDED_TITLE"
    LIFECYCLE_MIXED_CONTENT_CONFLICT = "LIFECYCLE_MIXED_CONTENT_CONFLICT"
    IMPOSSIBLE_SCHEMA_VALUE = "IMPOSSIBLE_SCHEMA_VALUE"
    INCOMPLETE_EVIDENCE = "INCOMPLETE_EVIDENCE"


@dataclass(frozen=True, slots=True)
class EvidenceManifest:
    raw_text_chars: int
    raw_text_hash: str
    attachment_count: int = 0
    ocr_blocks_available: int = 0
    ocr_blocks_included: int = 0
    included_chars: int = 0
    omitted_blocks: tuple[str, ...] = ()
    unavailable_attachment_count: int = 0
    ocr_complete: bool = True
    source_text_truncated: bool = False
    provider_output_truncated: bool = False

    def __post_init__(self) -> None:
        """Canonicalise cardinality so a manifest cannot overstate evidence.

        One OCR block is the source-level evidence unit for one attachment.  A
        producer may report fewer available blocks (OCR unavailable) or fewer
        included blocks (available evidence omitted), but neither state can be
        represented as complete.  Applying this invariant here also protects
        direct dataclass construction, not only the mapping adapter.
        """

        attachment_count = max(0, int(self.attachment_count or 0))
        available = max(0, int(self.ocr_blocks_available or 0))
        included = max(0, int(self.ocr_blocks_included or 0))
        unavailable = max(0, int(self.unavailable_attachment_count or 0))
        omitted = tuple(str(item) for item in self.omitted_blocks if str(item))
        cardinality_valid = included <= available <= attachment_count

        # Be conservative when a caller supplied contradictory counts.  Keep
        # every reported block visible while making the manifest incomplete.
        attachment_count = max(attachment_count, available, included)
        available = max(available, included)
        unavailable = max(unavailable, attachment_count - available)
        omitted_gap = max(0, available - included)
        if omitted_gap > len(omitted):
            omitted = (
                *omitted,
                *tuple(
                    f"ocr_block:{included + idx + 1}:omitted"
                    for idx in range(omitted_gap - len(omitted))
                ),
            )

        complete_cardinality = (
            cardinality_valid
            and attachment_count == available == included
            and unavailable == 0
            and not omitted
        )
        object.__setattr__(self, "attachment_count", attachment_count)
        object.__setattr__(self, "ocr_blocks_available", available)
        object.__setattr__(self, "ocr_blocks_included", included)
        object.__setattr__(self, "unavailable_attachment_count", unavailable)
        object.__setattr__(self, "omitted_blocks", omitted)
        object.__setattr__(self, "ocr_complete", bool(self.ocr_complete and complete_cardinality))

    @property
    def evidence_complete(self) -> bool:
        return (
            not self.source_text_truncated
            and not self.provider_output_truncated
            and self.ocr_complete
            and self.unavailable_attachment_count == 0
            and self.ocr_blocks_included == self.ocr_blocks_available
            and not self.omitted_blocks
        )

    @classmethod
    def complete_source(
        cls,
        source_text: str,
        ocr_blocks: Sequence[str] | None = None,
        *,
        attachment_count: int | None = None,
    ) -> "EvidenceManifest":
        text = source_text or ""
        blocks = tuple(str(block or "") for block in (ocr_blocks or ()))
        attachments = max(
            len(blocks), int(attachment_count if attachment_count is not None else len(blocks))
        )
        return cls(
            raw_text_chars=len(text),
            raw_text_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
            attachment_count=attachments,
            ocr_blocks_available=len(blocks),
            ocr_blocks_included=len(blocks),
            included_chars=len(text) + sum(len(block) for block in blocks),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "EvidenceManifest":
        required = {
            "raw_text_chars",
            "raw_text_hash",
            "attachment_count",
            "ocr_blocks_available",
            "ocr_blocks_included",
            "included_chars",
            "omitted_blocks",
            "unavailable_attachment_count",
            "ocr_complete",
            "source_text_truncated",
            "provider_output_truncated",
        }
        structurally_valid = required.issubset(value)

        def nonnegative_int(key: str) -> int:
            nonlocal structurally_valid
            raw = value.get(key)
            if isinstance(raw, bool):
                structurally_valid = False
                return 0
            try:
                parsed = int(raw)
            except (TypeError, ValueError):
                structurally_valid = False
                return 0
            if parsed < 0:
                structurally_valid = False
                return 0
            return parsed

        omitted_raw = value.get("omitted_blocks")
        if not isinstance(omitted_raw, (list, tuple)):
            structurally_valid = False
            omitted_raw = ()
        for key in ("ocr_complete", "source_text_truncated", "provider_output_truncated"):
            if not isinstance(value.get(key), bool):
                structurally_valid = False
        raw_hash = value.get("raw_text_hash")
        if (
            not isinstance(raw_hash, str)
            or len(raw_hash) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in raw_hash)
        ):
            structurally_valid = False
        raw_text_chars = nonnegative_int("raw_text_chars")
        included_chars = nonnegative_int("included_chars")
        if included_chars < raw_text_chars:
            structurally_valid = False
        manifest = cls(
            raw_text_chars=raw_text_chars,
            raw_text_hash=str(raw_hash or ""),
            attachment_count=nonnegative_int("attachment_count"),
            ocr_blocks_available=nonnegative_int("ocr_blocks_available"),
            ocr_blocks_included=nonnegative_int("ocr_blocks_included"),
            included_chars=included_chars,
            omitted_blocks=tuple(str(v) for v in omitted_raw),
            unavailable_attachment_count=nonnegative_int("unavailable_attachment_count"),
            ocr_complete=bool(value.get("ocr_complete")) and structurally_valid,
            source_text_truncated=bool(value.get("source_text_truncated", True)),
            provider_output_truncated=bool(value.get("provider_output_truncated", True)),
        )
        if structurally_valid:
            return manifest
        # Missing/invalid receipt fields are technical uncertainty.  Preserve
        # the parseable diagnostics but force the derived completeness false.
        payload = asdict(manifest)
        payload["ocr_complete"] = False
        return cls(**payload)

    def with_provider_truncation(self) -> "EvidenceManifest":
        payload = asdict(self)
        payload["provider_output_truncated"] = True
        return EvidenceManifest(**payload)

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["omitted_blocks"] = list(self.omitted_blocks)
        payload["evidence_complete"] = self.evidence_complete
        return payload


@dataclass(frozen=True, slots=True)
class LifecycleAction:
    action: LifecycleActionType
    target_title: str | None = None
    target_date: str | None = None
    target_time: str | None = None
    target_location: str | None = None
    new_date: str | None = None
    new_time: str | None = None
    evidence: str = ""

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "LifecycleAction":
        try:
            action = LifecycleActionType(str(value.get("action") or "").strip().upper())
        except ValueError as exc:
            raise ValueError("unknown lifecycle action") from exc
        return cls(
            action=action,
            target_title=_optional_text(value.get("target_title")),
            target_date=_optional_text(value.get("target_date")),
            target_time=_optional_text(value.get("target_time")),
            target_location=_optional_text(value.get("target_location")),
            new_date=_optional_text(value.get("new_date")),
            new_time=_optional_text(value.get("new_time")),
            evidence=str(value.get("evidence") or "").strip(),
        )

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["action"] = self.action.value
        return payload


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


@dataclass(frozen=True, slots=True)
class ContradictionFact:
    reason: VerificationReason
    details: str
    evidence: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "reason": self.reason.value,
            "details": self.details,
            "evidence": list(self.evidence),
        }


class SourceParseDecision(list[dict[str, Any]]):
    """Typed source verdict that remains a drop-in list for legacy callers.

    Iteration, indexing, length checks and ``festival`` access intentionally
    match the old ``ParsedEvents`` container.  New callers must inspect
    ``disposition`` rather than interpreting an empty list.
    """

    def __init__(
        self,
        events: Sequence[dict[str, Any]] | None = None,
        *,
        disposition: SourceDisposition | str | None = None,
        lifecycle_actions: Sequence[LifecycleAction] | None = None,
        evidence_manifest: EvidenceManifest | None = None,
        evidence_complete: bool | None = None,
        parse_version: str = PARSE_VERSION,
        festival: dict[str, Any] | None = None,
        retry_reason: SourceParseRetryReason | str | None = None,
        no_event_reason: SourceNoEventReason | str | None = None,
        verification_reasons: Sequence[VerificationReason] | None = None,
        verification: Mapping[str, Any] | None = None,
        enrichment_required: bool | None = None,
        provider_attempts: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        event_items = list(events or ())
        super().__init__(event_items)
        actions = tuple(lifecycle_actions or ())
        inferred_empty_retry = disposition is None and not event_items and not actions
        if disposition is None:
            disposition = (
                SourceDisposition.RETRY_REQUIRED
                if inferred_empty_retry
                else _infer_disposition(event_items, actions)
            )
        self.disposition = SourceDisposition(disposition)
        self.lifecycle_actions = actions
        self.evidence_manifest = evidence_manifest
        manifest_complete = evidence_manifest.evidence_complete if evidence_manifest else False
        self.evidence_complete = manifest_complete if evidence_complete is None else bool(evidence_complete and manifest_complete)
        self.parse_version = str(parse_version or PARSE_VERSION)
        self.festival = festival
        self.retry_reason = (
            SourceParseRetryReason(retry_reason) if retry_reason is not None else None
        )
        self.no_event_reason = (
            SourceNoEventReason(no_event_reason) if no_event_reason is not None else None
        )
        if inferred_empty_retry and self.retry_reason is None:
            self.retry_reason = SourceParseRetryReason.SCHEMA_MISMATCH
        self.verification_reasons = tuple(verification_reasons or ())
        self.verification = dict(verification) if verification is not None else None
        self.enrichment_required = (
            bool(enrichment_required)
            if enrichment_required is not None
            else bool(event_items and not self.evidence_complete)
        )
        # Sanitized transport/accounting metadata only.  It intentionally
        # excludes API-key identifiers and request payloads, but survives the
        # typed adapter so the owning durable queue can append one ledger row
        # per physical provider attempt.
        self.provider_attempts = tuple(
            dict(item) for item in (provider_attempts or ()) if isinstance(item, Mapping)
        )

        expected_disposition = _infer_disposition(event_items, actions)
        invalid_events = any(
            not isinstance(item, Mapping)
            or not isinstance(item.get("title"), str)
            or not item.get("title", "").strip()
            for item in event_items
        )
        if self.disposition is not SourceDisposition.RETRY_REQUIRED and (
            invalid_events
            or self.disposition is not expected_disposition
            or (
                self.no_event_reason is not None
                and self.disposition is not SourceDisposition.CONFIRMED_NO_EVENT
            )
        ):
            self.disposition = SourceDisposition.RETRY_REQUIRED
            self.retry_reason = SourceParseRetryReason.SCHEMA_MISMATCH
            self.no_event_reason = None
            self.evidence_complete = False
            self.enrichment_required = bool(event_items)

        if self.disposition is SourceDisposition.CONFIRMED_NO_EVENT and not self.evidence_complete:
            self.disposition = SourceDisposition.RETRY_REQUIRED
            self.retry_reason = (
                SourceParseRetryReason.EVIDENCE_INCOMPLETE
                if evidence_manifest is not None
                else SourceParseRetryReason.SCHEMA_MISMATCH
            )
            self.no_event_reason = None
            self.enrichment_required = False

    @property
    def events(self) -> list[dict[str, Any]]:
        return list(self)

    @property
    def is_retry(self) -> bool:
        return self.disposition is SourceDisposition.RETRY_REQUIRED

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "disposition": self.disposition.value,
            "events": list(self),
            "lifecycle_actions": [action.to_payload() for action in self.lifecycle_actions],
            "evidence_complete": self.evidence_complete,
            "parse_version": self.parse_version,
            "enrichment_required": self.enrichment_required,
        }
        if self.festival is not None:
            payload["festival"] = self.festival
        if self.retry_reason is not None:
            payload["retry_reason"] = self.retry_reason.value
        if self.no_event_reason is not None:
            payload["no_event_reason"] = self.no_event_reason.value
        if self.evidence_manifest is not None:
            payload["evidence_manifest"] = self.evidence_manifest.to_payload()
        if self.verification is not None:
            payload["verification"] = dict(self.verification)
        if self.provider_attempts:
            payload["provider_attempts"] = [dict(item) for item in self.provider_attempts]
        return payload

    @classmethod
    def retry(
        cls,
        reason: SourceParseRetryReason,
        *,
        evidence_manifest: EvidenceManifest | None = None,
        events: Sequence[dict[str, Any]] | None = None,
        lifecycle_actions: Sequence[LifecycleAction] | None = None,
        verification_reasons: Sequence[VerificationReason] | None = None,
        festival: dict[str, Any] | None = None,
        provider_attempts: Sequence[Mapping[str, Any]] | None = None,
    ) -> "SourceParseDecision":
        return cls(
            events,
            disposition=SourceDisposition.RETRY_REQUIRED,
            lifecycle_actions=lifecycle_actions,
            evidence_manifest=evidence_manifest,
            evidence_complete=False,
            festival=festival,
            retry_reason=reason,
            verification_reasons=verification_reasons,
            enrichment_required=bool(events),
            provider_attempts=provider_attempts,
        )

    def with_provider_attempts(
        self, attempts: Sequence[Mapping[str, Any]] | None
    ) -> "SourceParseDecision":
        """Attach redacted provider receipts while preserving list compatibility."""

        self.provider_attempts = tuple(
            dict(item) for item in (attempts or ()) if isinstance(item, Mapping)
        )
        return self


# Backward-compatible public spelling used throughout the application.
ParsedEvents = SourceParseDecision


def _infer_disposition(
    events: Sequence[Mapping[str, Any]], actions: Sequence[LifecycleAction]
) -> SourceDisposition:
    if events and actions:
        return SourceDisposition.MIXED
    if events:
        return SourceDisposition.EVENTS_FOUND
    if actions:
        return SourceDisposition.LIFECYCLE_ONLY
    return SourceDisposition.CONFIRMED_NO_EVENT


def provider_response_is_truncated(metadata: Any) -> bool:
    """Recognise common provider finish signals without importing an SDK."""

    if metadata is None:
        return False
    if isinstance(metadata, Mapping):
        values: Iterable[Any] = (
            metadata.get("finish_reason"),
            metadata.get("finishReason"),
            metadata.get("stop_reason"),
        )
    else:
        values = (
            getattr(metadata, "finish_reason", None),
            getattr(metadata, "finishReason", None),
            getattr(metadata, "stop_reason", None),
        )
    truncated = {"length", "max_tokens", "max_output_tokens", "token_limit", "truncated"}
    return any(str(value or "").strip().casefold() in truncated for value in values)


def provider_attempt_metadata(metadata: Any, *, attempt_kind: str) -> dict[str, Any]:
    """Return a secret-free durable receipt for one physical provider call."""

    def read(*names: str) -> Any:
        if isinstance(metadata, Mapping):
            for name in names:
                if metadata.get(name) is not None:
                    return metadata.get(name)
            return None
        for name in names:
            value = getattr(metadata, name, None)
            if value is not None:
                return value
        return None

    usage = read("usage")
    usage_source = usage if usage is not None else metadata

    def usage_read(*names: str) -> Any:
        if isinstance(usage_source, Mapping):
            for name in names:
                if usage_source.get(name) is not None:
                    return usage_source.get(name)
            return None
        for name in names:
            value = getattr(usage_source, name, None)
            if value is not None:
                return value
        return None

    payload: dict[str, Any] = {"attempt_kind": str(attempt_kind or "primary")}
    text_fields = {
        "model": read("model", "provider_model_version"),
        "quota_scope": read("quota_scope"),
        "quota_reason": read("quota_reason", "blocked_reason"),
        "request_id": read("provider_request_id", "request_id"),
        "response_id": read("provider_response_id", "response_id"),
        "finish_reason": read("finish_reason") or usage_read("finish_reason"),
        "provider_model_version": read("provider_model_version")
        or usage_read("provider_model_version"),
        "input_count_source": usage_read("input_count_source"),
        "error_type": read("error_type"),
    }
    for key, value in text_fields.items():
        if value not in (None, ""):
            payload[key] = str(value)
    int_fields = {
        "input_tokens": usage_read("input_tokens"),
        "output_tokens": usage_read("output_tokens"),
        "thought_tokens": usage_read("thought_tokens"),
        "reserved_tokens": usage_read("reserved_tokens"),
        "actual_total_tokens": usage_read("actual_total_tokens", "total_tokens"),
        "provider_retry_after_ms": read("retry_after_ms"),
        "status_code": read("status_code"),
    }
    for key, value in int_fields.items():
        if value is None:
            continue
        try:
            payload[key] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    return payload


def decision_from_provider_payload(
    payload: Any,
    *,
    evidence_manifest: EvidenceManifest | None,
    provider_metadata: Any = None,
) -> SourceParseDecision:
    """Validate provider JSON and convert it to the closed source verdict.

    Non-empty legacy arrays and single-event objects are accepted only as a
    temporary positive-result adapter.  A legacy ``[]`` is intrinsically
    ambiguous and can never prove ``CONFIRMED_NO_EVENT``; only the closed typed
    object can carry that terminal semantic verdict.
    """

    if evidence_manifest is None:
        return SourceParseDecision.retry(SourceParseRetryReason.SCHEMA_MISMATCH)

    if provider_response_is_truncated(provider_metadata):
        return SourceParseDecision.retry(
            SourceParseRetryReason.OUTPUT_TRUNCATED,
            evidence_manifest=evidence_manifest.with_provider_truncation(),
        )

    festival = _festival_payload(payload)
    if isinstance(payload, list):
        if not all(_legacy_event_is_valid(item) for item in payload):
            return SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                evidence_manifest=evidence_manifest,
            )
        if not payload:
            return SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                evidence_manifest=evidence_manifest,
            )
        return SourceParseDecision(
            payload,
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=evidence_manifest,
            evidence_complete=evidence_manifest.evidence_complete,
            festival=festival,
            parse_version="legacy-array-adapter-v1",
        )

    if not isinstance(payload, dict):
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=evidence_manifest,
        )

    typed_shape = any(
        key in payload for key in ("disposition", "events", "lifecycle_actions")
    )
    if not typed_shape:
        # Temporary compatibility for the legacy single-event object.
        if not _legacy_event_is_valid(payload):
            return SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                evidence_manifest=evidence_manifest,
                festival=festival,
            )
        return SourceParseDecision(
            [payload],
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=evidence_manifest,
            evidence_complete=evidence_manifest.evidence_complete,
            festival=festival,
            parse_version="legacy-object-adapter-v1",
        )

    try:
        disposition = SourceDisposition(str(payload.get("disposition") or ""))
    except ValueError:
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=evidence_manifest,
            festival=festival,
        )
    raw_no_event_reason = payload.get("no_event_reason")
    no_event_reason: SourceNoEventReason | None = None
    if raw_no_event_reason is not None:
        try:
            no_event_reason = SourceNoEventReason(str(raw_no_event_reason))
        except (TypeError, ValueError):
            return SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                evidence_manifest=evidence_manifest,
                festival=festival,
            )
        if disposition is not SourceDisposition.CONFIRMED_NO_EVENT:
            return SourceParseDecision.retry(
                SourceParseRetryReason.SCHEMA_MISMATCH,
                evidence_manifest=evidence_manifest,
                festival=festival,
            )
    events = payload.get("events")
    actions_raw = payload.get("lifecycle_actions")
    if not isinstance(events, list) or not all(_event_mapping_is_valid(item) for item in events):
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=evidence_manifest,
            festival=festival,
        )
    if not isinstance(actions_raw, list) or not all(isinstance(item, dict) for item in actions_raw):
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=evidence_manifest,
            festival=festival,
        )
    try:
        actions = tuple(LifecycleAction.from_mapping(item) for item in actions_raw)
    except ValueError:
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=evidence_manifest,
            festival=festival,
        )

    if disposition is SourceDisposition.RETRY_REQUIRED:
        raw_reason = payload.get("retry_reason")
        try:
            reason = SourceParseRetryReason(str(raw_reason))
        except (TypeError, ValueError):
            reason = SourceParseRetryReason.SCHEMA_MISMATCH
        return SourceParseDecision.retry(
            reason,
            evidence_manifest=evidence_manifest,
            events=events,
            lifecycle_actions=actions,
            festival=festival,
        )

    expected = _infer_disposition(events, actions)
    if disposition is not expected:
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=evidence_manifest,
            events=events,
            lifecycle_actions=actions,
            festival=festival,
        )

    declared_complete = payload.get("evidence_complete")
    if not isinstance(declared_complete, bool):
        return SourceParseDecision.retry(
            SourceParseRetryReason.SCHEMA_MISMATCH,
            evidence_manifest=evidence_manifest,
            events=events,
            lifecycle_actions=actions,
            festival=festival,
        )
    effective_complete = bool(declared_complete and evidence_manifest.evidence_complete)
    if disposition is SourceDisposition.CONFIRMED_NO_EVENT and not effective_complete:
        return SourceParseDecision.retry(
            SourceParseRetryReason.EVIDENCE_INCOMPLETE,
            evidence_manifest=evidence_manifest,
            festival=festival,
        )
    return SourceParseDecision(
        events,
        disposition=disposition,
        lifecycle_actions=actions,
        evidence_manifest=evidence_manifest,
        evidence_complete=effective_complete,
        parse_version=str(payload.get("parse_version") or PARSE_VERSION),
        festival=festival,
        no_event_reason=no_event_reason,
        enrichment_required=bool(events and not effective_complete),
    )


def _event_mapping_is_valid(value: Any) -> bool:
    """Validate the minimum shared event envelope without semantic guessing."""

    return isinstance(value, Mapping) and isinstance(value.get("title"), str) and bool(
        value.get("title", "").strip()
    )


def _legacy_event_is_valid(value: Any) -> bool:
    # Legacy positives remain a narrow rolling adapter.  Empty objects and
    # arbitrary receipt mappings must never acquire EVENTS_FOUND authority.
    return _event_mapping_is_valid(value)


def _festival_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("festival")
    if isinstance(value, str):
        return {"name": value}
    if isinstance(value, Mapping):
        return dict(value)
    return None


def normalise_contradiction_facts(
    values: Sequence[ContradictionFact | VerificationReason | str | Mapping[str, Any]] | None,
) -> tuple[ContradictionFact, ...]:
    facts: list[ContradictionFact] = []
    for value in values or ():
        if isinstance(value, ContradictionFact):
            facts.append(value)
            continue
        if isinstance(value, Mapping):
            raw_reason = value.get("reason")
            details = str(value.get("details") or "").strip()
            evidence = tuple(str(item) for item in (value.get("evidence") or ()))
        else:
            raw_reason = value
            details = ""
            evidence = ()
        try:
            reason = raw_reason if isinstance(raw_reason, VerificationReason) else VerificationReason(str(raw_reason))
        except ValueError:
            # Unknown/free-form facts are diagnostics only; they cannot trigger
            # a semantic veto or an unbounded verifier stage.
            continue
        facts.append(ContradictionFact(reason, details or reason.value, evidence))
    return tuple(facts)


def collect_verification_facts(
    decision: SourceParseDecision,
    supplied: Sequence[ContradictionFact | VerificationReason | str | Mapping[str, Any]] | None = None,
) -> tuple[ContradictionFact, ...]:
    facts = list(normalise_contradiction_facts(supplied))
    manifest = decision.evidence_manifest
    if manifest is not None and not manifest.evidence_complete:
        facts.append(
            ContradictionFact(
                VerificationReason.INCOMPLETE_EVIDENCE,
                "Evidence manifest is incomplete or carries a truncation signal.",
            )
        )
    unique: dict[VerificationReason, ContradictionFact] = {}
    for fact in facts:
        unique.setdefault(fact.reason, fact)
    return tuple(unique.values())


def build_verification_request(
    *,
    source_text: str,
    ocr_blocks: Sequence[str] | None,
    evidence_manifest: EvidenceManifest,
    primary_decision: SourceParseDecision,
    contradiction_facts: Sequence[ContradictionFact],
    today: str,
    published_at: str | None,
    source_context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "task": "conditionally_verify_source_parse",
        "rules": [
            "Use all source text and every OCR block; deterministic hints are evidence, never a verdict.",
            "Confirm or correct events and lifecycle actions, split missing siblings/sessions, or return RETRY_REQUIRED.",
            "Return the same typed SourceParseDecision JSON schema and no prose.",
        ],
        "today": today,
        "published_at": published_at,
        "source_context": dict(source_context or {}),
        "source_text": source_text or "",
        "ocr_blocks": list(ocr_blocks or ()),
        "evidence_manifest": evidence_manifest.to_payload(),
        "primary_result": primary_decision.to_payload(),
        "contradiction_facts": [fact.to_payload() for fact in contradiction_facts],
    }


async def conditionally_verify_source_decision(
    primary_decision: SourceParseDecision,
    *,
    contradiction_facts: Sequence[ContradictionFact | VerificationReason | str | Mapping[str, Any]] | None,
    invoke: Callable[[tuple[ContradictionFact, ...]], Awaitable[SourceParseDecision]],
) -> SourceParseDecision:
    """Run exactly zero or one verifier call for the closed contradiction set."""

    facts = collect_verification_facts(primary_decision, contradiction_facts)
    if not facts:
        return primary_decision
    try:
        corrected = await invoke(facts)
    except Exception:
        return SourceParseDecision.retry(
            SourceParseRetryReason.VERIFICATION_TECHNICAL_ERROR,
            evidence_manifest=primary_decision.evidence_manifest,
            events=list(primary_decision),
            lifecycle_actions=primary_decision.lifecycle_actions,
            verification_reasons=[fact.reason for fact in facts],
            festival=primary_decision.festival,
            provider_attempts=primary_decision.provider_attempts,
        )
    if not isinstance(corrected, SourceParseDecision) or corrected.is_retry:
        corrected_attempts = (
            corrected.provider_attempts
            if isinstance(corrected, SourceParseDecision)
            else ()
        )
        retry_reason = (
            SourceParseRetryReason.VERIFICATION_UNCERTAIN
            if isinstance(corrected, SourceParseDecision)
            and corrected.retry_reason is SourceParseRetryReason.VERIFICATION_UNCERTAIN
            else SourceParseRetryReason.VERIFICATION_TECHNICAL_ERROR
        )
        return SourceParseDecision.retry(
            retry_reason,
            evidence_manifest=primary_decision.evidence_manifest,
            events=list(primary_decision),
            lifecycle_actions=primary_decision.lifecycle_actions,
            verification_reasons=[fact.reason for fact in facts],
            festival=primary_decision.festival,
            provider_attempts=[
                *primary_decision.provider_attempts,
                *corrected_attempts,
            ],
        )
    corrected.with_provider_attempts(
        [*primary_decision.provider_attempts, *corrected.provider_attempts]
    )
    corrected.verification_reasons = tuple(fact.reason for fact in facts)
    corrected.verification = {
        "performed": True,
        "reasons": [fact.reason.value for fact in facts],
    }
    return corrected
