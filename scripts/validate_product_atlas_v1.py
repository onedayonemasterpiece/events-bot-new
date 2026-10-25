#!/usr/bin/env python3
"""Fail-closed validation for LoveKGD Product Atlas Git SoT v1.

This validator proves structural and semantic traceability. It deliberately does
not infer that delivery, deployment or healthy runtime proves a user/owner
outcome. It has no network, analytics or Penpot access.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
ATLAS = ROOT / "docs" / "product-model" / "atlas" / "v1"

ENTITY_FILES = (
    "product-core.v1.json",
    "journeys.v1.json",
    "capabilities.v1.json",
    "work-items.v1.json",
    "enablers-and-guardrails.v1.json",
    "acceptance.v1.json",
    "measurement-and-decisions.v1.json",
)

ENTITY_STATUSES = {
    "accepted",
    "source_proven",
    "hypothesis",
    "partial",
    "unresolved",
    "not_modeled",
    "superseded",
    "not_applicable",
}
SOURCE_STATUSES = {
    "current",
    "partially_current",
    "historical",
    "research_only",
    "superseded",
    "conflicting",
    "unresolved",
}
LANES = {"user", "owner_operator", "future_partner"}
FACETS = {
    "definition",
    "delivery",
    "verification",
    "deployment",
    "runtime_health",
    "evidence",
    "user_outcome",
    "owner_outcome",
}
COMMON_FIELDS = {
    "id",
    "kind",
    "title",
    "definition",
    "stakeholder_lane",
    "status",
    "source_refs",
    "confidence",
    "relations",
    "facets",
    "unresolved_conflicts",
    "supersession_history",
}
PARTNER_MEANING_KINDS = {
    "user_need",
    "job",
    "job_story",
    "user_outcome",
    "owner_outcome",
    "journey",
    "journey_step",
    "recovery_path",
    "capability",
    "user_story",
}
PRODUCT_ID_PREFIXES = (
    "lane.",
    "need.",
    "job.",
    "job-story.",
    "user-outcome.",
    "owner-outcome.",
    "journey.",
    "step.",
    "recovery.",
    "capability.",
    "story.",
    "operator-job.",
    "enabler.",
    "guardrail.",
    "rule.",
    "acceptance.",
    "event.",
    "mq.",
    "problem.",
    "ui-gap.",
    "finding.",
    "decision.",
)
EXPECTED_ARCHETYPES = {
    "archetype.home",
    "archetype.artifacts",
    "archetype.collections",
    "archetype.event-detail",
    "archetype.exhibitions",
    "archetype.favorites",
    "archetype.festivals",
    "archetype.focus-group",
    "archetype.information-pages",
    "archetype.interest-clubs",
    "archetype.listing.date",
    "archetype.listing.popular",
    "archetype.listing.unusual",
    "archetype.listing.weekend",
    "archetype.personal-feed",
    "archetype.search",
    "archetype.special-state",
}
HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
UUID_LIKE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
)


class ValidationError(RuntimeError):
    pass


def load_json(path: Path) -> Any:
    if not path.is_file():
        raise ValidationError(f"missing required file: {path.relative_to(ROOT)}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"invalid JSON {path.relative_to(ROOT)}: {exc}") from exc


def walk(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def assert_no_done(value: Any, label: str) -> None:
    for child in walk(value):
        if isinstance(child, str) and child.strip().lower() == "done":
            raise ValidationError(f"forbidden one-dimensional status 'done' in {label}")


def collect_entities() -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {}
    entities: list[dict[str, Any]] = []
    for filename in ENTITY_FILES:
        document = load_json(ATLAS / filename)
        assert_no_done(document, filename)
        records = document.get("entities")
        if not isinstance(records, list) or not records:
            raise ValidationError(f"{filename}: non-empty entities[] required")
        for entity in records:
            if not isinstance(entity, dict):
                raise ValidationError(f"{filename}: entity must be an object")
            missing = COMMON_FIELDS - entity.keys()
            if missing:
                raise ValidationError(
                    f"{filename}:{entity.get('id', '<unknown>')}: missing {sorted(missing)}"
                )
            entity_id = entity["id"]
            if not isinstance(entity_id, str) or not entity_id:
                raise ValidationError(f"{filename}: invalid entity ID")
            if entity_id in by_id:
                raise ValidationError(f"duplicate entity ID: {entity_id}")
            if entity["status"] not in ENTITY_STATUSES:
                raise ValidationError(f"{entity_id}: invalid status {entity['status']!r}")
            if entity["stakeholder_lane"] not in LANES:
                raise ValidationError(
                    f"{entity_id}: invalid stakeholder lane {entity['stakeholder_lane']!r}"
                )
            if not isinstance(entity["source_refs"], list) or not entity["source_refs"]:
                raise ValidationError(f"{entity_id}: non-empty source_refs required")
            if not isinstance(entity["relations"], list):
                raise ValidationError(f"{entity_id}: relations[] required")
            confidence = entity["confidence"]
            if not isinstance(confidence, dict) or not confidence.get("level"):
                raise ValidationError(f"{entity_id}: confidence level required")
            facets = entity["facets"]
            if not isinstance(facets, dict) or set(facets) != FACETS:
                raise ValidationError(
                    f"{entity_id}: facets must be exactly {sorted(FACETS)}"
                )
            # A guardrail saying partner meaning must not be invented is an
            # owner-controlled constraint, not invented partner product meaning.
            if (
                entity["stakeholder_lane"] == "future_partner"
                and entity["kind"] in PARTNER_MEANING_KINDS
                and entity["status"] != "not_modeled"
            ):
                raise ValidationError(
                    f"{entity_id}: future partner product meaning must remain not_modeled"
                )
            by_id[entity_id] = entity
            entities.append(entity)
    return by_id, entities


def validate_sources(entities: list[dict[str, Any]]) -> set[str]:
    lock = load_json(ATLAS / "source-lock.v1.json")
    exact = load_json(ATLAS / "source-lock-exact-resolutions.v1.json")
    assert_no_done(lock, "source-lock.v1.json")
    sources = lock.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ValidationError("source-lock.v1.json: non-empty sources[] required")

    source_ids: set[str] = set()
    unresolved_blob_ids: set[str] = set()
    for source in sources:
        required = {
            "id",
            "repository",
            "repository_sha",
            "blob_sha",
            "path",
            "date",
            "authority_type",
            "proves",
            "status",
            "superseded_by",
        }
        missing = required - source.keys()
        if missing:
            raise ValidationError(f"source {source.get('id')}: missing {sorted(missing)}")
        source_id = source["id"]
        if source_id in source_ids:
            raise ValidationError(f"duplicate source ID: {source_id}")
        source_ids.add(source_id)
        if source["status"] not in SOURCE_STATUSES:
            raise ValidationError(f"{source_id}: invalid source status")
        if not HEX40.fullmatch(source["repository_sha"]):
            raise ValidationError(f"{source_id}: exact repository SHA required")
        blob = source["blob_sha"]
        if blob is None:
            unresolved_blob_ids.add(source_id)
        elif not HEX40.fullmatch(blob):
            raise ValidationError(f"{source_id}: invalid Git blob SHA")

    resolved_ids: set[str] = set()
    for resolution in exact.get("resolutions", []):
        source_id = resolution.get("source_id")
        if source_id not in source_ids:
            raise ValidationError(f"source resolution references unknown source {source_id}")
        if source_id in resolved_ids:
            raise ValidationError(f"duplicate source resolution: {source_id}")
        resolved_ids.add(source_id)
        blob = resolution.get("blob_sha")
        aggregate = resolution.get("aggregate_sha256")
        if blob is None and aggregate is None:
            raise ValidationError(f"{source_id}: blob or aggregate hash required")
        if blob is not None and not HEX40.fullmatch(blob):
            raise ValidationError(f"{source_id}: invalid resolved blob SHA")
        if aggregate is not None and not HEX64.fullmatch(aggregate):
            raise ValidationError(f"{source_id}: invalid aggregate SHA-256")

    missing = unresolved_blob_ids - resolved_ids
    if missing:
        raise ValidationError(f"null source blobs lack exact resolution: {sorted(missing)}")

    ui = lock.get("locks", {}).get("corrected_ui_sot", {})
    if ui.get("sha") != "9b8043f3bdb86fab4eee00bf94b0f10d4f029c50":
        raise ValidationError("corrected UI SoT SHA drift")
    if ui.get("manifest_sha256") != (
        "ac2cb64bbccb113dd7c81cdb8caec953d3d5e2f56ea10a1f54914d7a0ed46819"
    ):
        raise ValidationError("corrected UI manifest SHA-256 drift")
    if ui.get("archetype_count") != 17 or ui.get("route_pattern_count") != 29:
        raise ValidationError("corrected UI coverage lock drift")

    for entity in entities:
        for source_ref in entity["source_refs"]:
            if source_ref not in source_ids:
                raise ValidationError(f"{entity['id']}: unknown source ref {source_ref}")
    return source_ids


def validate_relations(by_id: dict[str, dict[str, Any]], entities: list[dict[str, Any]]) -> None:
    for entity in entities:
        for relation in entity["relations"]:
            if not isinstance(relation, dict) or not relation.get("type"):
                raise ValidationError(f"{entity['id']}: malformed relation")
            target = relation.get("target_id")
            if not isinstance(target, str) or not target:
                raise ValidationError(f"{entity['id']}: relation target_id required")
            if target.startswith(PRODUCT_ID_PREFIXES) and target not in by_id:
                raise ValidationError(f"{entity['id']}: orphan relation {target}")


def validate_user_stories(by_id: dict[str, dict[str, Any]], entities: list[dict[str, Any]]) -> None:
    for story in (entity for entity in entities if entity["kind"] == "user_story"):
        contract = story.get("story_contract")
        required = {
            "actor",
            "context",
            "vertical_slice",
            "observable_result",
            "acceptance_rule_ids",
            "acceptance_scenario_ids",
            "measurement_question_id",
            "implementation_evidence",
            "release_evidence",
            "current_status",
        }
        if not isinstance(contract, dict):
            raise ValidationError(f"{story['id']}: story_contract required")
        missing = required - contract.keys()
        if missing:
            raise ValidationError(f"{story['id']}: story_contract missing {sorted(missing)}")
        linked = (
            list(contract["acceptance_rule_ids"])
            + list(contract["acceptance_scenario_ids"])
            + [contract["measurement_question_id"]]
        )
        for target in linked:
            if target not in by_id:
                raise ValidationError(f"{story['id']}: orphan story link {target}")
        relation_types = {relation["type"] for relation in story["relations"]}
        mandatory = {"develops", "supports", "used_in", "verified_by", "measured_by"}
        if not mandatory <= relation_types:
            raise ValidationError(
                f"{story['id']}: missing relation types {sorted(mandatory - relation_types)}"
            )
        if not str(contract["observable_result"]).strip():
            raise ValidationError(f"{story['id']}: observable result required")
        if re.search(r"\b(migration|database table|API endpoint)\b", story["title"], re.I):
            raise ValidationError(f"{story['id']}: technical work mislabeled as User Story")


def validate_ui_linkage(by_id: dict[str, dict[str, Any]], source_ids: set[str]) -> None:
    document = load_json(ATLAS / "ui-linkage.v1.json")
    assert_no_done(document, "ui-linkage.v1.json")
    links = document.get("links")
    if not isinstance(links, list) or len(links) != 17:
        raise ValidationError("ui-linkage.v1.json: exactly 17 links required")
    archetypes = [link.get("archetype_id") for link in links]
    if set(archetypes) != EXPECTED_ARCHETYPES or len(archetypes) != len(set(archetypes)):
        raise ValidationError(
            f"UI archetype mismatch: missing={sorted(EXPECTED_ARCHETYPES - set(archetypes))}, "
            f"extra={sorted(set(archetypes) - EXPECTED_ARCHETYPES)}"
        )
    if document.get("coverage", {}).get("route_registry_mapping_percent") != 100:
        raise ValidationError("route registry mapping must remain 100%")
    if UUID_LIKE.search(json.dumps(document, ensure_ascii=False)):
        raise ValidationError("fabricated or unpublished UUID in UI linkage")

    for link in links:
        link_id = link.get("id", "<unknown>")
        for source_ref in link.get("source_refs", []):
            if source_ref not in source_ids:
                raise ValidationError(f"{link_id}: unknown source ref {source_ref}")
        for product_id in link.get("product_entity_ids", []):
            if product_id not in by_id:
                raise ValidationError(f"{link_id}: orphan product entity {product_id}")
        for acceptance_id in link.get("acceptance_scenario_ids", []):
            if acceptance_id not in by_id:
                raise ValidationError(f"{link_id}: orphan acceptance entity {acceptance_id}")
        for measurement_id in link.get("measurement_question_ids", []):
            if measurement_id not in by_id:
                raise ValidationError(f"{link_id}: orphan measurement question {measurement_id}")
        if not link.get("measurement_question_ids") and link.get("measurement_status") != "not_modeled":
            raise ValidationError(f"{link_id}: measurement question or not_modeled marker required")
        regions = link.get("semantic_regions")
        if not isinstance(regions, list) or not regions:
            raise ValidationError(f"{link_id}: semantic_regions[] required")
        for region in regions:
            if region.get("configured_instance_binding") != "binding_pending":
                raise ValidationError(f"{link_id}: unpublished binding must be binding_pending")
            if not region.get("region_id") or not region.get("product_screen_states"):
                raise ValidationError(f"{link_id}: region and ProductScreenStates required")


def validate_unresolved(by_id: dict[str, dict[str, Any]], source_ids: set[str]) -> None:
    document = load_json(ATLAS / "unresolved-ledger.v1.json")
    assert_no_done(document, "unresolved-ledger.v1.json")
    items = document.get("items")
    if not isinstance(items, list) or not items:
        raise ValidationError("unresolved-ledger.v1.json: non-empty items[] required")
    seen: set[str] = set()
    for item in items:
        item_id = item.get("id")
        if not item_id or item_id in seen:
            raise ValidationError(f"invalid/duplicate unresolved item: {item_id}")
        seen.add(item_id)
        for source_ref in item.get("source_refs", []):
            if source_ref not in source_ids:
                raise ValidationError(f"{item_id}: unknown source ref {source_ref}")
        for affected in item.get("affected_entity_ids", []):
            if affected not in by_id:
                raise ValidationError(f"{item_id}: orphan affected entity {affected}")
        if item.get("type") == "binding_pending" and item.get("binding_status") != "binding_pending":
            raise ValidationError(f"{item_id}: binding_pending marker required")
        if not item.get("resolution_gate") or not item.get("prohibited_shortcut"):
            raise ValidationError(f"{item_id}: resolution gate and prohibited shortcut required")


def validate() -> dict[str, int]:
    by_id, entities = collect_entities()
    source_ids = validate_sources(entities)
    validate_relations(by_id, entities)
    validate_user_stories(by_id, entities)
    validate_ui_linkage(by_id, source_ids)
    validate_unresolved(by_id, source_ids)

    kinds: dict[str, int] = {}
    for entity in entities:
        kinds[entity["kind"]] = kinds.get(entity["kind"], 0) + 1
    return {
        "entities": len(entities),
        "entity_kinds": len(kinds),
        "sources": len(source_ids),
        "archetypes": len(EXPECTED_ARCHETYPES),
        "user_stories": kinds.get("user_story", 0),
    }


def main() -> int:
    try:
        summary = validate()
    except ValidationError as exc:
        print(f"PRODUCT_ATLAS_V1_FAIL: {exc}", file=sys.stderr)
        return 1
    print("PRODUCT_ATLAS_V1_PASS " + json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
