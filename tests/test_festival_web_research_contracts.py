from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from festival_web_research.contracts import (
    Claim, Decision, DecisionActor, DecisionKind, DecisionStatus, EditionStatus,
    EntityRole, GateStatus, ItemDisposition, PrimaryTopology, ProgrammeItem,
    ProgrammeStructure, QuoteSpan, SemanticEventGate, SourceRole, SourceSnapshot,
)
from festival_web_research.evidence import normalize_snapshot_text
from festival_web_research.validators import (
    ContractViolation, assert_no_agent_apply_authority, load_taxonomy_registry,
    taxonomy_registry_hash, validate_programme_item, validate_reference_graph,
)

REGISTRY = Path("festival_web_research/schemas/festival-taxonomy-registry-v2.json")


def passing_gate() -> SemanticEventGate:
    return SemanticEventGate(**{name: GateStatus.PASS for name in SemanticEventGate.model_fields})


def test_registry_has_exactly_seven_primary_topologies() -> None:
    registry = load_taxonomy_registry(REGISTRY.read_bytes())
    assert registry["primary_topologies"] == [value.value for value in PrimaryTopology]
    assert len(PrimaryTopology) == 7
    assert "unknown" not in registry["primary_topologies"]
    assert len(taxonomy_registry_hash(registry)) == 64
    assert [v.value for v in ProgrammeStructure] == registry["programme_structures"]


def test_registry_rejects_eighth_or_reordered_topology() -> None:
    registry = json.loads(REGISTRY.read_text())
    registry["primary_topologies"].append("unknown")
    with pytest.raises(ContractViolation, match="primary_topology_registry_mismatch"):
        load_taxonomy_registry(json.dumps(registry))
    registry = json.loads(REGISTRY.read_text())
    registry["primary_topologies"].reverse()
    with pytest.raises(ContractViolation, match="primary_topology_registry_mismatch"):
        load_taxonomy_registry(json.dumps(registry))


def test_agent_apply_authority_is_rejected_recursively() -> None:
    assert_no_agent_apply_authority({"event_gate": {"current_edition": "pass"}})
    with pytest.raises(ContractViolation, match="agent_supplied_host_authority"):
        assert_no_agent_apply_authority({"items": [{"event_gate": {"operator_approval": "pass"}}]})
    with pytest.raises(ContractViolation, match="agent_supplied_host_authority"):
        assert_no_agent_apply_authority({"smart_update": "pass"})


def test_closed_contract_rejects_extra_and_invalid_enums() -> None:
    with pytest.raises(ValidationError):
        SemanticEventGate(**{**{name: "pass" for name in SemanticEventGate.model_fields}, "operator_approval": "pass"})
    with pytest.raises(ValidationError):
        ProgrammeItem(
            item_id="i", entity_role="artist", disposition="schedule_slot",
            decision_ids=["D1"], event_gate=passing_gate(),
        )


def test_event_candidate_requires_all_gates_and_event_role() -> None:
    valid = ProgrammeItem(
        item_id="i1", entity_role=EntityRole.CHILD_EVENT,
        disposition=ItemDisposition.CREATE_EVENT_CANDIDATE,
        identity_claim_ids=["C1"], logistics_claim_ids=["C2"],
        decision_ids=["D1"], event_gate=passing_gate(),
    )
    validate_programme_item(valid)
    failed_gate = passing_gate().model_copy(update={"independent_choice": GateStatus.UNKNOWN})
    with pytest.raises(ContractViolation, match="event_disposition_without_all_gates"):
        validate_programme_item(valid.model_copy(update={"event_gate": failed_gate}))
    with pytest.raises(ContractViolation, match="event_disposition_for_non_event_role"):
        validate_programme_item(valid.model_copy(update={"entity_role": EntityRole.PARTICIPANT}))


def test_service_role_and_disposition_are_bijective() -> None:
    item = ProgrammeItem(
        item_id="svc", entity_role=EntityRole.SERVICE_INFORMATION,
        disposition=ItemDisposition.SERVICE_INFORMATION, decision_ids=["D1"],
        event_gate=SemanticEventGate(**{name: GateStatus.NOT_APPLICABLE for name in SemanticEventGate.model_fields}),
    )
    validate_programme_item(item)
    with pytest.raises(ContractViolation, match="service_role_disposition_mismatch"):
        validate_programme_item(item.model_copy(update={"disposition": ItemDisposition.PROGRAMME_ONLY}))


def _graph():
    normalized = normalize_snapshot_text("Фестиваль 2026\nКонцерт 18:00")
    source = SourceSnapshot(
        source_id="S1", requested_url="https://example.org/", resolved_url="https://example.org/",
        canonical_url="https://example.org/", source_role=SourceRole.OFFICIAL_PROGRAM,
        edition_status=EditionStatus.ACCEPTED, content_sha256=normalized.content_sha256,
        normalizer_version=normalized.normalizer_version, snapshot_ref="snapshot:S1",
        retrieved_at_utc=datetime(2026, 7, 31, tzinfo=UTC),
    )
    claim = Claim(
        claim_id="C1", source_id="S1", local_subject_id="programme:1",
        subject_kind="programme_item", field="title", raw_value="Концерт",
        normalized_value="Концерт", normalization="none",
        evidence=QuoteSpan(quote="Концерт", quote_start=15, quote_end=22),
        content_sha256=normalized.content_sha256, normalizer_version=normalized.normalizer_version,
    )
    decision = Decision(
        decision_id="D1", decision_kind=DecisionKind.PROGRAMME_ITEM_DISPOSITION,
        subject_ref="programme_item:i1", selected_value="programme_only",
        evidence_claim_ids=["C1"], reason_codes=["shared_container"],
        status=DecisionStatus.SUPPORTED, actor_kind=DecisionActor.LANE_MODEL,
    )
    item = ProgrammeItem(
        item_id="i1", entity_role=EntityRole.PROGRAMME_BLOCK,
        disposition=ItemDisposition.PROGRAMME_ONLY, identity_claim_ids=["C1"],
        decision_ids=["D1"], event_gate=SemanticEventGate(**{name: GateStatus.UNKNOWN for name in SemanticEventGate.model_fields}),
    )
    return normalized.text, source, claim, decision, item


def test_reference_graph_positive_and_negative() -> None:
    text, source, claim, decision, item = _graph()
    validate_reference_graph(sources=[source], snapshot_text_by_source={"S1": text}, claims=[claim], decisions=[decision], programme_items=[item])
    with pytest.raises(ContractViolation, match="unresolved_item_claim"):
        validate_reference_graph(sources=[source], snapshot_text_by_source={"S1": text}, claims=[claim], decisions=[decision], programme_items=[item.model_copy(update={"identity_claim_ids": ["missing"]})])
    wrong = claim.model_copy(update={"evidence": QuoteSpan(quote="онцерт", quote_start=15, quote_end=21)})
    with pytest.raises(ContractViolation, match="invalid_exact_quote"):
        validate_reference_graph(sources=[source], snapshot_text_by_source={"S1": text}, claims=[wrong], decisions=[decision], programme_items=[item])
    mismatch = decision.model_copy(update={"selected_value": "schedule_slot"})
    with pytest.raises(ContractViolation, match="item_disposition_decision_mismatch"):
        validate_reference_graph(sources=[source], snapshot_text_by_source={"S1": text}, claims=[claim], decisions=[mismatch], programme_items=[item])
    wrong_subject = decision.model_copy(update={"subject_ref": "programme_item:other"})
    with pytest.raises(ContractViolation, match="item_disposition_decision_mismatch"):
        validate_reference_graph(sources=[source], snapshot_text_by_source={"S1": text}, claims=[claim], decisions=[wrong_subject], programme_items=[item])


def test_event_evidence_must_belong_to_the_same_programme_subject() -> None:
    text, source, claim, decision, _ = _graph()
    claims = [
        claim.model_copy(update={"claim_id": claim_id, "field": field})
        for claim_id, field in (
            ("CT", "title"), ("CD", "date"), ("CM", "time_start"), ("CV", "venue_name")
        )
    ]
    event_decision = decision.model_copy(update={
        "selected_value": "create_event_candidate",
        "evidence_claim_ids": [value.claim_id for value in claims],
    })
    event = ProgrammeItem(
        item_id="i1", entity_role=EntityRole.CHILD_EVENT,
        disposition=ItemDisposition.CREATE_EVENT_CANDIDATE,
        identity_claim_ids=["CT"], logistics_claim_ids=["CD", "CM", "CV"],
        decision_ids=["D1"], event_gate=passing_gate(),
    )
    with pytest.raises(ContractViolation, match="event_claim_subject_mismatch"):
        validate_reference_graph(
            sources=[source], snapshot_text_by_source={"S1": text}, claims=claims,
            decisions=[event_decision], programme_items=[event],
        )
    bound_claims = [value.model_copy(update={"local_subject_id": "i1"}) for value in claims]
    validate_reference_graph(
        sources=[source], snapshot_text_by_source={"S1": text}, claims=bound_claims,
        decisions=[event_decision], programme_items=[event],
    )


def test_classification_unknown_is_none_not_eighth_topology() -> None:
    from festival_web_research.contracts import FestivalClassification
    value = FestivalClassification(
        primary_topology=None, secondary_topologies=[], programme_structure="unknown",
        decision_ids=["D1"],
    )
    assert value.primary_topology is None
    with pytest.raises(ValidationError):
        FestivalClassification(primary_topology="unknown", programme_structure="unknown", decision_ids=["D1"])
    with pytest.raises(ValidationError):
        FestivalClassification(primary_topology="lineup", secondary_topologies=["lineup"], programme_structure="hybrid", decision_ids=["D1"])


def test_ticket_claim_requires_single_item_ticket_source() -> None:
    text, source, claim, decision, item = _graph()
    ticket_claim = claim.model_copy(update={"field": "ticket_url", "raw_value": "Концерт", "normalized_value": "Концерт"})
    with pytest.raises(ContractViolation, match="ticket_claim_wrong_source_scope"):
        validate_reference_graph(sources=[source], snapshot_text_by_source={"S1": text}, claims=[ticket_claim], decisions=[decision], programme_items=[item])
