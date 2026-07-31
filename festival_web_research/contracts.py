"""Closed, provider-neutral contracts for festival web research.

The models in this module validate structure only.  They deliberately do not
classify festival meaning; topology, role and disposition values must arrive
from an evidence-backed semantic collector or an operator.
"""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class PrimaryTopology(StrEnum):
    SERIES_SEASON = "series_season"
    LINEUP = "lineup"
    GRID_SHOWCASE = "grid_showcase"
    TERRITORY = "territory"
    MARKET = "market"
    ROUTE_PROMENADE = "route_promenade"
    NETWORK_PASS = "network_pass"


class ProgrammeStructure(StrEnum):
    IDENTITY_ONLY = "identity_only"
    SINGLE_COMPOUND_EVENT = "single_compound_event"
    STANDALONE_EVENTS = "standalone_events"
    SCHEDULE_ONLY = "schedule_only"
    HYBRID = "hybrid"
    CONTINUOUS_EXPERIENCE = "continuous_experience"
    DISTRIBUTED_CYCLE = "distributed_cycle"
    UNKNOWN = "unknown"


class EntityRole(StrEnum):
    CHILD_EVENT = "child_event"
    PROGRAMME_BLOCK = "programme_block"
    TEMPORAL_ANCHOR = "temporal_anchor"
    ACTIVITY_OR_ZONE = "activity_or_zone"
    PARTICIPANT = "participant"
    WORK = "work"
    ROUTE_POINT = "route_point"
    PRODUCT_OR_OFFER = "product_or_offer"
    SERVICE_INFORMATION = "service_information"


class ItemDisposition(StrEnum):
    LINK_EXISTING_EVENT = "link_existing_event"
    CREATE_EVENT_CANDIDATE = "create_event_candidate"
    SCHEDULE_SLOT = "schedule_slot"
    PROGRAMME_ONLY = "programme_only"
    CONTINUOUS_ACTIVITY = "continuous_activity"
    SERVICE_INFORMATION = "service_information"
    REJECT = "reject"


class SourceRole(StrEnum):
    OFFICIAL_HOME = "official_home"
    OFFICIAL_EDITION = "official_edition"
    OFFICIAL_PROGRAM = "official_program"
    OFFICIAL_DOCUMENT = "official_document"
    OFFICIAL_ORGANIZER = "official_organizer"
    OFFICIAL_VENUE = "official_venue"
    OFFICIAL_EVENT = "official_event"
    TICKET_SINGLE_ITEM = "ticket_single_item"
    TICKET_SINGLE_EVENT = "ticket_single_event"
    TICKET_DAY = "ticket_day"
    TICKET_FESTIVAL = "ticket_festival"
    TICKET_SUBSCRIPTION = "ticket_subscription"
    TICKET_PASS_OR_SUBSCRIPTION = "ticket_pass_or_subscription"
    FESTIVAL_PASS = "festival_pass"
    REGISTRATION = "registration"
    REGIONAL_OFFICIAL = "regional_official"
    REGIONAL_TOURISM = "regional_tourism"
    MEDIA = "media"
    AGGREGATOR = "aggregator"
    MEDIA_OR_AGGREGATOR = "media_or_aggregator"
    DOCUMENT_PDF = "document_pdf"
    DOCUMENT_IMAGE = "document_image"
    MACHINE_FEED = "machine_feed"
    SOCIAL = "social"
    OTHER = "other"
    AMBIGUOUS = "ambiguous"
    REJECTED = "rejected"


class EditionStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"


class SubjectKind(StrEnum):
    FESTIVAL = "festival"
    PROGRAMME_ITEM = "programme_item"
    PARTICIPANT = "participant"
    WORK = "work"
    ROUTE_POINT = "route_point"
    PRODUCT_OR_OFFER = "product_or_offer"
    VENUE = "venue"
    ORGANIZER = "organizer"


class ResearchSubject(ClosedModel):
    source_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    local_subject_id: str = Field(min_length=1, max_length=256)
    subject_kind: SubjectKind


class ClaimField(StrEnum):
    TITLE = "title"
    EDITION_LABEL = "edition_label"
    DESCRIPTION_FACT = "description_fact"
    START_DATE = "start_date"
    END_DATE = "end_date"
    DATE = "date"
    TIME_START = "time_start"
    TIMEZONE = "timezone"
    VENUE_NAME = "venue_name"
    VENUE_ADDRESS = "venue_address"
    CITY = "city"
    ORGANIZER_NAME = "organizer_name"
    ORGANIZER_ROLE = "organizer_role"
    PARTICIPANT_NAME = "participant_name"
    PARTICIPANT_ROLE = "participant_role"
    TICKET_URL = "ticket_url"
    PRICE_TEXT = "price_text"
    REGISTRATION_URL = "registration_url"
    CANONICAL_URL = "canonical_url"


class ClaimStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"


class DecisionStatus(StrEnum):
    SUPPORTED = "supported"
    CONFLICT = "conflict"
    UNKNOWN = "unknown"
    REJECTED = "rejected"


class DecisionActor(StrEnum):
    LANE_MODEL = "lane_model"
    HOST_RECONCILER = "host_reconciler"
    OPERATOR = "operator"


class DecisionKind(StrEnum):
    EDITION_CLASSIFICATION_BUNDLE = "edition_classification_bundle"
    DISCOVERY_TOPOLOGY = "discovery_topology"
    PROGRAMME_STRUCTURE = "programme_structure"
    PROGRAMME_ITEM_ENTITY_ROLE = "programme_item_entity_role"
    PROGRAMME_ITEM_DISPOSITION = "programme_item_disposition"
    EVENT_GATE_BUNDLE = "event_gate_bundle"
    ENTITY_MATCH = "entity_match"
    SOURCE_EDITION_STATUS = "source_edition_status"


class GateStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


class HostGateStatus(StrEnum):
    PENDING = "pending"
    PASS = "pass"
    FAIL = "fail"


class CheckpointKind(StrEnum):
    STATE = "state"
    SOURCE_LEDGER = "source_ledger"
    SOURCE_REVIEW = "source_review"
    CLAIMS = "claims"
    SUBJECTS = "subjects"
    TOPOLOGY = "topology"
    PROGRAMME_INVENTORY = "programme_inventory"
    CANDIDATE = "candidate"
    RUN_SUMMARY = "run_summary"


class QuoteSpan(ClosedModel):
    quote: str = Field(min_length=1, max_length=16_384)
    quote_start: int = Field(ge=0)
    quote_end: int = Field(gt=0)

    @model_validator(mode="after")
    def ordered(self) -> "QuoteSpan":
        if self.quote_end <= self.quote_start:
            raise ValueError("quote_end must be greater than quote_start")
        if self.quote_end - self.quote_start != len(self.quote):
            raise ValueError("quote offsets must have the same length as quote")
        return self


class SourceSnapshot(ClosedModel):
    source_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    requested_url: str = Field(min_length=1, max_length=4096)
    resolved_url: str = Field(min_length=1, max_length=4096)
    canonical_url: str = Field(min_length=1, max_length=4096)
    source_role: SourceRole
    edition_status: EditionStatus
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalizer_version: str = Field(min_length=1, max_length=128)
    snapshot_ref: str = Field(min_length=1, max_length=1024)
    retrieved_at_utc: datetime
    content_type: str | None = Field(default=None, max_length=255)

    @field_validator("retrieved_at_utc")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("retrieved_at_utc must be timezone-aware")
        return value


JsonScalar = str | int | float | bool | None


class Claim(ClosedModel):
    claim_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    source_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    local_subject_id: str = Field(min_length=1, max_length=256)
    subject_kind: SubjectKind
    field: ClaimField
    raw_value: JsonScalar
    normalized_value: JsonScalar = None
    normalization: Literal["none", "trim", "iso_date", "iso_time", "canonical_url"]
    evidence: QuoteSpan
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalizer_version: str = Field(min_length=1, max_length=128)
    status: ClaimStatus = ClaimStatus.ACCEPTED


class Decision(ClosedModel):
    decision_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    decision_kind: DecisionKind
    subject_ref: str = Field(min_length=1, max_length=256)
    selected_value: Any
    alternatives_rejected: list[Any] = Field(default_factory=list, max_length=64)
    evidence_claim_ids: list[str] = Field(default_factory=list, max_length=512)
    reason_codes: list[str] = Field(default_factory=list, max_length=64)
    status: DecisionStatus
    actor_kind: DecisionActor

    @field_validator("evidence_claim_ids")
    @classmethod
    def unique_claim_refs(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate evidence claim reference")
        return value


class SemanticEventGate(ClosedModel):
    current_edition: GateStatus
    independent_choice: GateStatus
    event_grade_occurrence: GateStatus
    meaningful_identity: GateStatus
    access_compatibility: GateStatus
    topology_guardrail: GateStatus
    evidence_validation: GateStatus

    def all_pass(self) -> bool:
        return all(value == GateStatus.PASS for value in (
            self.current_edition,
            self.independent_choice,
            self.event_grade_occurrence,
            self.meaningful_identity,
            self.access_compatibility,
            self.topology_guardrail,
            self.evidence_validation,
        ))


class HostApplyGate(ClosedModel):
    operator_approval: HostGateStatus = HostGateStatus.PENDING
    smart_update: HostGateStatus = HostGateStatus.PENDING


class ProgrammeItem(ClosedModel):
    item_id: str = Field(min_length=1, max_length=256)
    entity_role: EntityRole
    disposition: ItemDisposition
    identity_claim_ids: list[str] = Field(default_factory=list, max_length=256)
    logistics_claim_ids: list[str] = Field(default_factory=list, max_length=256)
    decision_ids: list[str] = Field(min_length=1, max_length=64)
    event_gate: SemanticEventGate

    @field_validator("identity_claim_ids", "logistics_claim_ids", "decision_ids")
    @classmethod
    def unique_refs(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate reference")
        return value


class FestivalClassification(ClosedModel):
    """Evidence references for the semantic classification selected upstream."""
    primary_topology: PrimaryTopology | None
    secondary_topologies: list[PrimaryTopology] = Field(default_factory=list, max_length=6)
    programme_structure: ProgrammeStructure
    claim_ids: list[str] = Field(default_factory=list, max_length=512)
    decision_ids: list[str] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def topology_set_is_consistent(self) -> "FestivalClassification":
        if len(self.secondary_topologies) != len(set(self.secondary_topologies)):
            raise ValueError("duplicate secondary topology")
        if self.primary_topology in self.secondary_topologies:
            raise ValueError("primary topology cannot also be secondary")
        return self


class CheckpointRecord(ClosedModel):
    checkpoint_id: str = Field(min_length=1, max_length=128)
    kind: CheckpointKind
    sequence: int = Field(ge=0)
    relative_path: str = Field(min_length=1, max_length=1024)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_count: int = Field(ge=0, le=64 * 1024 * 1024)
    created_at_utc: datetime
    parent_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("created_at_utc")
    @classmethod
    def checkpoint_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at_utc must be timezone-aware")
        return value
