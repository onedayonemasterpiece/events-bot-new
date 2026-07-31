"""Fail-closed structural validators for untrusted agent output."""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import PurePosixPath
from typing import Any

from .contracts import (
    CheckpointKind,
    CheckpointRecord,
    Claim,
    ClaimField,
    Decision,
    DecisionKind,
    EditionStatus,
    EntityRole,
    ItemDisposition,
    PrimaryTopology,
    ProgrammeItem, ResearchSubject,
    ProgrammeStructure,
    SourceSnapshot,
)
from .evidence import EvidenceValidationError, canonical_json_bytes, canonical_json_sha256, validate_exact_quote
from .sources import UnsafeSourceURL, canonicalize_public_url


class ContractViolation(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


def _unique_by(items: Iterable[Any], field: str, code: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in items:
        key = getattr(item, field)
        if key in result:
            raise ContractViolation(code, key)
        result[key] = item
    return result


def assert_no_agent_apply_authority(payload: Any) -> None:
    """Reject host-owned authority fields anywhere in untrusted agent JSON."""
    if isinstance(payload, Mapping):
        forbidden = {"operator_approval", "smart_update"}.intersection(payload)
        if forbidden:
            raise ContractViolation("agent_supplied_host_authority", ",".join(sorted(forbidden)))
        for value in payload.values():
            assert_no_agent_apply_authority(value)
    elif isinstance(payload, list):
        for value in payload:
            assert_no_agent_apply_authority(value)


def validate_taxonomy_registry(registry: Mapping[str, Any]) -> None:
    expected_topologies = [item.value for item in PrimaryTopology]
    if registry.get("primary_topologies") != expected_topologies:
        raise ContractViolation("primary_topology_registry_mismatch")
    if len(expected_topologies) != 7 or len(set(expected_topologies)) != 7:
        raise ContractViolation("primary_topology_count_not_seven")
    if registry.get("unknown_topology_state", "missing") is not None:
        raise ContractViolation("unknown_is_not_primary_topology")
    if registry.get("programme_structures") != [item.value for item in ProgrammeStructure]:
        raise ContractViolation("programme_structure_registry_mismatch")
    if registry.get("entity_roles") != [item.value for item in EntityRole]:
        raise ContractViolation("entity_role_registry_mismatch")
    if registry.get("item_dispositions") != [item.value for item in ItemDisposition]:
        raise ContractViolation("item_disposition_registry_mismatch")
    if registry.get("host_only_apply_fields") != ["operator_approval", "smart_update"]:
        raise ContractViolation("host_authority_registry_mismatch")


def taxonomy_registry_hash(registry: Mapping[str, Any]) -> str:
    validate_taxonomy_registry(registry)
    return canonical_json_sha256(registry)


def load_taxonomy_registry(data: bytes | str) -> dict[str, Any]:
    try:
        result = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ContractViolation("invalid_taxonomy_registry_json", str(exc)) from exc
    if not isinstance(result, dict):
        raise ContractViolation("taxonomy_registry_not_object")
    validate_taxonomy_registry(result)
    return result


def validate_programme_item(item: ProgrammeItem) -> None:
    event_dispositions = {ItemDisposition.CREATE_EVENT_CANDIDATE, ItemDisposition.LINK_EXISTING_EVENT}
    event_roles = {EntityRole.CHILD_EVENT, EntityRole.PROGRAMME_BLOCK}
    if item.disposition in event_dispositions:
        if item.entity_role not in event_roles:
            raise ContractViolation("event_disposition_for_non_event_role", item.item_id)
        if not item.event_gate.all_pass():
            raise ContractViolation("event_disposition_without_all_gates", item.item_id)
        if not item.identity_claim_ids or not item.logistics_claim_ids:
            raise ContractViolation("event_disposition_without_identity_or_logistics_evidence", item.item_id)
    if item.entity_role in {
        EntityRole.PARTICIPANT, EntityRole.WORK, EntityRole.ROUTE_POINT,
        EntityRole.PRODUCT_OR_OFFER, EntityRole.ACTIVITY_OR_ZONE,
        EntityRole.SERVICE_INFORMATION,
    } and item.disposition in event_dispositions:
        raise ContractViolation("non_event_entity_materialized", item.item_id)
    if (item.entity_role == EntityRole.SERVICE_INFORMATION) != (item.disposition == ItemDisposition.SERVICE_INFORMATION):
        raise ContractViolation("service_role_disposition_mismatch", item.item_id)


def validate_reference_graph(
    *,
    sources: Iterable[SourceSnapshot],
    snapshot_text_by_source: Mapping[str, str],
    claims: Iterable[Claim],
    decisions: Iterable[Decision],
    programme_items: Iterable[ProgrammeItem],
    subjects: Iterable[ResearchSubject] | None = None,
) -> None:
    source_map = _unique_by(sources, "source_id", "duplicate_source_id")
    claim_map = _unique_by(claims, "claim_id", "duplicate_claim_id")
    decision_map = _unique_by(decisions, "decision_id", "duplicate_decision_id")
    item_map = _unique_by(programme_items, "item_id", "duplicate_programme_item_id")
    subject_map: dict[tuple[str, str], ResearchSubject] | None = None
    if subjects is not None:
        subject_map = {}
        for subject in subjects:
            key = (subject.source_id, subject.local_subject_id)
            if key in subject_map:
                raise ContractViolation("duplicate_local_subject", f"{key[0]}:{key[1]}")
            if subject.source_id not in source_map:
                raise ContractViolation("unresolved_subject_source", f"{key[0]}:{key[1]}")
            subject_map[key] = subject

    if set(snapshot_text_by_source) != set(source_map):
        missing = set(source_map).symmetric_difference(snapshot_text_by_source)
        raise ContractViolation("snapshot_source_set_mismatch", ",".join(sorted(missing)))
    for source in source_map.values():
        try:
            canonical = canonicalize_public_url(source.canonical_url)
            canonicalize_public_url(source.requested_url)
            canonicalize_public_url(source.resolved_url)
        except UnsafeSourceURL as exc:
            raise ContractViolation("unsafe_source_url", f"{source.source_id}: {exc}") from exc
        if canonical != source.canonical_url:
            raise ContractViolation("noncanonical_source_url", source.source_id)
    for claim in claim_map.values():
        source = source_map.get(claim.source_id)
        if source is None:
            raise ContractViolation("unresolved_claim_source", claim.claim_id)
        if subject_map is not None:
            subject = subject_map.get((claim.source_id, claim.local_subject_id))
            if subject is None:
                raise ContractViolation("unresolved_claim_subject", claim.claim_id)
            if subject.subject_kind != claim.subject_kind:
                raise ContractViolation("claim_subject_kind_mismatch", claim.claim_id)
        if claim.status.value == "accepted" and source.edition_status != EditionStatus.ACCEPTED:
            raise ContractViolation("accepted_claim_from_nonaccepted_source", claim.claim_id)
        if claim.field == ClaimField.TICKET_URL and source.source_role.value not in {"ticket_single_item", "ticket_single_event"}:
            raise ContractViolation("ticket_claim_wrong_source_scope", claim.claim_id)
        try:
            validate_exact_quote(claim, source, snapshot_text_by_source[claim.source_id])
        except EvidenceValidationError as exc:
            raise ContractViolation("invalid_exact_quote", f"{claim.claim_id}: {exc}") from exc

    for decision in decision_map.values():
        try:
            canonical_json_bytes(decision.selected_value, max_bytes=256 * 1024, max_depth=32, max_items=20_000)
            canonical_json_bytes(decision.alternatives_rejected, max_bytes=256 * 1024, max_depth=32, max_items=20_000)
        except EvidenceValidationError as exc:
            raise ContractViolation("unbounded_decision_value", decision.decision_id) from exc
        missing = set(decision.evidence_claim_ids) - set(claim_map)
        if missing:
            raise ContractViolation("unresolved_decision_claim", f"{decision.decision_id}:{sorted(missing)}")
        if decision.actor_kind.value == "lane_model" and not decision.evidence_claim_ids:
            raise ContractViolation("model_decision_without_evidence", decision.decision_id)

    for item in item_map.values():
        missing_claims = (set(item.identity_claim_ids) | set(item.logistics_claim_ids)) - set(claim_map)
        if missing_claims:
            raise ContractViolation("unresolved_item_claim", f"{item.item_id}:{sorted(missing_claims)}")
        missing_decisions = set(item.decision_ids) - set(decision_map)
        if missing_decisions:
            raise ContractViolation("unresolved_item_decision", f"{item.item_id}:{sorted(missing_decisions)}")
        if item.disposition in {
            ItemDisposition.CREATE_EVENT_CANDIDATE,
            ItemDisposition.LINK_EXISTING_EVENT,
        }:
            identity_fields = {claim_map[ref].field for ref in item.identity_claim_ids}
            logistics_fields = {claim_map[ref].field for ref in item.logistics_claim_ids}
            if ClaimField.TITLE not in identity_fields:
                raise ContractViolation("event_identity_without_title_claim", item.item_id)
            if not logistics_fields.intersection({ClaimField.DATE, ClaimField.START_DATE}):
                raise ContractViolation("event_logistics_without_date_claim", item.item_id)
            if ClaimField.TIME_START not in logistics_fields:
                raise ContractViolation("event_logistics_without_time_claim", item.item_id)
            if not logistics_fields.intersection(
                {ClaimField.VENUE_NAME, ClaimField.VENUE_ADDRESS, ClaimField.CITY}
            ):
                raise ContractViolation("event_logistics_without_place_claim", item.item_id)
        disposition_decisions = [decision_map[ref] for ref in item.decision_ids if decision_map[ref].decision_kind == DecisionKind.PROGRAMME_ITEM_DISPOSITION]
        if len(disposition_decisions) != 1 or disposition_decisions[0].selected_value != item.disposition.value:
            raise ContractViolation("item_disposition_decision_mismatch", item.item_id)
        validate_programme_item(item)


def validate_inventory_conservation(
    *,
    a_item_ids: Iterable[str],
    b_item_ids: Iterable[str],
    resolutions: Mapping[str, str],
) -> None:
    """Require every A/B source-local item to have exactly one explicit fate.

    Resolution keys are namespaced as ``A:<id>`` and ``B:<id>``. Values must be
    ``canonical:<id>``, ``rejected:<decision-id>`` or ``unresolved``.
    """
    a_ids = list(a_item_ids)
    b_ids = list(b_item_ids)
    if len(a_ids) != len(set(a_ids)) or len(b_ids) != len(set(b_ids)):
        raise ContractViolation("duplicate_inventory_item_id")
    expected = {f"A:{value}" for value in a_ids} | {f"B:{value}" for value in b_ids}
    actual = set(resolutions)
    if actual != expected:
        missing = expected - actual
        extra = actual - expected
        raise ContractViolation("inventory_not_conserved", f"missing={sorted(missing)} extra={sorted(extra)}")
    for item_ref, resolution in resolutions.items():
        if not isinstance(resolution, str) or not resolution:
            raise ContractViolation("invalid_inventory_resolution", item_ref)
        if resolution == "unresolved":
            continue
        if resolution.startswith("canonical:") and len(resolution) > len("canonical:"):
            continue
        if resolution.startswith("rejected:") and len(resolution) > len("rejected:"):
            continue
        raise ContractViolation("invalid_inventory_resolution", f"{item_ref}:{resolution}")


def validate_checkpoint_chain(
    checkpoints: Iterable[CheckpointRecord],
    *,
    artifact_bytes_by_path: Mapping[str, bytes],
    require_terminal: bool = True,
) -> None:
    records = list(checkpoints)
    if not records:
        raise ContractViolation("empty_checkpoint_chain")
    if [record.sequence for record in records] != list(range(len(records))):
        raise ContractViolation("checkpoint_sequence_gap")
    if len({record.checkpoint_id for record in records}) != len(records):
        raise ContractViolation("duplicate_checkpoint_id")
    if len({record.relative_path for record in records}) != len(records):
        raise ContractViolation("duplicate_checkpoint_path")

    kinds = [record.kind for record in records]
    if len(kinds) < 2 or kinds[:2] != [CheckpointKind.STATE, CheckpointKind.SOURCE_LEDGER]:
        raise ContractViolation("checkpoint_stage_regression", "state/source_ledger must lead")
    cursor = 2
    source_count = 0
    source_triplet = [CheckpointKind.SOURCE_REVIEW, CheckpointKind.CLAIMS, CheckpointKind.SUBJECTS]
    while kinds[cursor:cursor + 3] == source_triplet:
        source_count += 1
        cursor += 3
    if source_count == 0:
        if cursor < len(kinds) and kinds[cursor] in source_triplet:
            raise ContractViolation("checkpoint_stage_regression", "invalid source checkpoint triplet")
        raise ContractViolation("mandatory_checkpoint_missing", "source_review,claims,subjects")
    semantic_tail = [CheckpointKind.TOPOLOGY, CheckpointKind.PROGRAMME_INVENTORY]
    if kinds[cursor:cursor + 2] != semantic_tail:
        raise ContractViolation("checkpoint_stage_regression", "topology/programme_inventory expected")
    cursor += 2
    if require_terminal:
        if kinds[cursor:] != [CheckpointKind.CANDIDATE, CheckpointKind.RUN_SUMMARY]:
            raise ContractViolation("mandatory_checkpoint_missing", "candidate,run_summary")
    elif kinds[cursor:] not in ([], [CheckpointKind.CANDIDATE], [CheckpointKind.CANDIDATE, CheckpointKind.RUN_SUMMARY]):
        raise ContractViolation("checkpoint_stage_regression", "invalid incomplete tail")

    previous_hash: str | None = None
    for record in records:
        path = PurePosixPath(record.relative_path)
        if path.is_absolute() or ".." in path.parts or "\\" in record.relative_path:
            raise ContractViolation("unsafe_checkpoint_path", record.relative_path)
        content = artifact_bytes_by_path.get(record.relative_path)
        if content is None:
            raise ContractViolation("checkpoint_artifact_missing", record.relative_path)
        import hashlib
        digest = hashlib.sha256(content).hexdigest()
        if digest != record.content_sha256 or len(content) != record.byte_count:
            raise ContractViolation("checkpoint_artifact_mismatch", record.relative_path)
        if record.parent_sha256 != previous_hash:
            raise ContractViolation("checkpoint_parent_hash_mismatch", record.checkpoint_id)
        previous_hash = digest
