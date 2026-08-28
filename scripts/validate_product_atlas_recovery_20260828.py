#!/usr/bin/env python3
"""Validate the 2026-08-28 Product Atlas recovery and optional exact external checkouts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
ATLAS = ROOT / "docs" / "product-model" / "atlas" / "v1"
LOCK_PATH = ATLAS / "current-source-lock.2026-08-28.v2.json"
DELTA_PATH = ATLAS / "product-delta.2026-08-28.v2.json"
HANDOFF_PATH = ATLAS / "visualization-handoff.2026-08-28.v1.json"
RECOVERY_PATH = ATLAS / "recovery-2026-08-28.md"

BASE_ENTITY_FILES = [
    "product-core.v1.json",
    "journeys.v1.json",
    "capabilities.v1.json",
    "work-items.v1.json",
    "enablers-and-guardrails.v1.json",
    "acceptance.v1.json",
    "measurement-and-decisions.v1.json",
]

ALLOWED_STATUSES = {
    "accepted",
    "source_proven",
    "hypothesis",
    "partial",
    "unresolved",
    "not_modeled",
    "superseded",
    "not_applicable",
}
REQUIRED_ENTITY_FIELDS = {
    "id",
    "kind",
    "title",
    "definition",
    "stakeholder_lane",
    "status",
    "confidence",
    "source_refs",
    "relations",
    "facets",
    "unresolved_conflicts",
    "supersession_history",
}
REQUIRED_FACETS = {
    "definition",
    "delivery",
    "verification",
    "deployment",
    "runtime_health",
    "evidence",
    "user_outcome",
    "owner_outcome",
}
EXPECTED_ARCHETYPES = {
    "archetype.home",
    "archetype.listing.date",
    "archetype.listing.weekend",
    "archetype.listing.popular",
    "archetype.listing.unusual",
    "archetype.search",
    "archetype.event-detail",
    "archetype.collections",
    "archetype.festivals",
    "archetype.exhibitions",
    "archetype.interest-clubs",
    "archetype.favorites",
    "archetype.personal-feed",
    "archetype.focus-group",
    "archetype.artifacts",
    "archetype.information-pages",
    "archetype.special-state",
}
UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)


def fail(message: str) -> None:
    raise AssertionError(message)


def load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AssertionError(f"missing file: {path.relative_to(ROOT)}") from exc
    except json.JSONDecodeError as exc:
        raise AssertionError(f"invalid JSON: {path.relative_to(ROOT)}: {exc}") from exc
    if not isinstance(data, dict):
        fail(f"root must be an object: {path.relative_to(ROOT)}")
    return data


def git_head(repo: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
    ).strip()


def git_blob_sha(path: Path) -> str:
    payload = path.read_bytes()
    return hashlib.sha1(f"blob {len(payload)}\0".encode() + payload).hexdigest()


def iter_ids(value: Any, keys: set[str]) -> Iterable[str]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in keys:
                if isinstance(child, str):
                    yield child
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, str):
                            yield item
            yield from iter_ids(child, keys)
    elif isinstance(value, list):
        for child in value:
            yield from iter_ids(child, keys)


def collect_base_entities() -> dict[str, dict[str, Any]]:
    entities: dict[str, dict[str, Any]] = {}
    for name in BASE_ENTITY_FILES:
        data = load_json(ATLAS / name)
        rows = data.get("entities")
        if not isinstance(rows, list):
            fail(f"{name}: entities must be a list")
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("id"), str):
                fail(f"{name}: malformed entity")
            entity_id = row["id"]
            if entity_id in entities:
                fail(f"duplicate base entity ID: {entity_id}")
            entities[entity_id] = row
    return entities


def collect_unresolved_ids() -> set[str]:
    data = load_json(ATLAS / "unresolved-ledger.v1.json")
    rows = data.get("items")
    if not isinstance(rows, list):
        fail("unresolved-ledger.v1.json: items must be a list")
    result: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            fail("unresolved-ledger.v1.json: malformed item")
        if row["id"] in result:
            fail(f"duplicate unresolved ID: {row['id']}")
        result.add(row["id"])
    return result


def validate_local() -> tuple[int, int, int, int]:
    lock = load_json(LOCK_PATH)
    delta = load_json(DELTA_PATH)
    handoff = load_json(HANDOFF_PATH)
    recovery = RECOVERY_PATH.read_text(encoding="utf-8")

    if lock.get("schema_version") != "lovekgd-product-atlas-current-source-lock.v2":
        fail("unexpected current source-lock schema")
    if delta.get("schema_version") != "lovekgd-product-atlas-delta.v2":
        fail("unexpected delta schema")
    if handoff.get("schema_version") != "lovekgd-product-atlas-visualization-handoff.v1":
        fail("unexpected visualization handoff schema")
    if "Penpot reads/writes in this recovery: `0 / 0`" not in recovery:
        fail("recovery record must state zero Penpot reads/writes")

    sources = lock.get("sources")
    if not isinstance(sources, list):
        fail("current source lock must contain sources")
    source_ids = [row.get("id") for row in sources if isinstance(row, dict)]
    if len(source_ids) != len(set(source_ids)) or not all(isinstance(x, str) for x in source_ids):
        fail("source IDs must be unique strings")
    source_id_set = set(source_ids)

    expected_lock_values = {
        "src.product-main-20260828": "8710e56fa3685f6c30a90cd062d532dce0348cce",
        "src.corrected-semantic-ui-base": "9b8043f3bdb86fab4eee00bf94b0f10d4f029c50",
        "src.as-is-round-trip-baseline-20260825": "b86bab3e91511b3d4bd7d953b22bceb847f02a51",
        "src.owner-review-delta-20260828": "47d0fef53c33200492d92f6a086d9b8813fe187e",
        "src.astro-owner-audit-candidate-20260828": "49c351873d40a2ea55f0a32837c7376e344d9c17",
        "src.agent-assisted-discovery-hypothesis-20260826": "f78e7c5974b4192bddf9eea901ee6d8b57f51560",
        "src.design-system-planning-20260826": "2923eeb474f2c91189be3c3a2ba72f0db9e4ca89",
    }
    sources_by_id = {row["id"]: row for row in sources}
    for source_id, expected_sha in expected_lock_values.items():
        row = sources_by_id.get(source_id)
        if row is None:
            fail(f"missing required current source: {source_id}")
        if row.get("repository_sha") != expected_sha:
            fail(f"{source_id}: expected SHA {expected_sha}, got {row.get('repository_sha')}")

    baseline = sources_by_id["src.as-is-round-trip-baseline-20260825"]
    expected_coverage = {
        "archetypes": 17,
        "boards": 34,
        "regions": 97,
        "patterns": 97,
        "components": 75,
        "states": 180,
        "orphan_design_ids": 0,
    }
    if baseline.get("coverage") != expected_coverage:
        fail("AS-IS baseline coverage drifted")

    penpot_target = lock.get("penpot_target")
    if not isinstance(penpot_target, dict):
        fail("source lock missing penpot_target")
    if penpot_target.get("binding_status") != "binding_pending":
        fail("Penpot target must remain binding_pending")
    for field in ("account", "file_id"):
        if penpot_target.get(field) is not None:
            fail(f"Penpot target {field} must be null before MCP readback")
    if penpot_target.get("page_ids") != []:
        fail("Penpot target page_ids must be empty before MCP readback")
    if penpot_target.get("reuse_existing_design_system_ids") is not False:
        fail("design-system Penpot IDs must not be reusable target IDs")
    if penpot_target.get("read_calls") != 0 or penpot_target.get("write_calls") != 0:
        fail("Git recovery must record zero Penpot calls")

    base_entities = collect_base_entities()
    unresolved_ids = collect_unresolved_ids()
    updates = delta.get("entity_updates")
    new_entities = delta.get("new_entities")
    unresolved_updates = delta.get("unresolved_updates")
    if not isinstance(updates, list) or not isinstance(new_entities, list):
        fail("delta must contain entity_updates and new_entities lists")
    if not isinstance(unresolved_updates, list):
        fail("delta must contain unresolved_updates")

    for update in updates:
        if not isinstance(update, dict):
            fail("malformed entity update")
        entity_id = update.get("entity_id")
        if entity_id not in base_entities:
            fail(f"entity update targets missing base ID: {entity_id}")
        if update.get("status") not in ALLOWED_STATUSES:
            fail(f"entity update has invalid status: {entity_id}")
        refs = update.get("source_refs")
        if not isinstance(refs, list) or not set(refs).issubset(source_id_set):
            fail(f"entity update has unresolved source refs: {entity_id}")
        patch = update.get("facet_patch")
        if not isinstance(patch, dict) or set(patch) != REQUIRED_FACETS:
            fail(f"entity update must patch all independent facets: {entity_id}")

    new_by_id: dict[str, dict[str, Any]] = {}
    for entity in new_entities:
        if not isinstance(entity, dict):
            fail("malformed new entity")
        missing = REQUIRED_ENTITY_FIELDS - set(entity)
        if missing:
            fail(f"{entity.get('id')}: missing fields {sorted(missing)}")
        entity_id = entity["id"]
        if entity_id in base_entities or entity_id in new_by_id:
            fail(f"new entity ID collides with existing ID: {entity_id}")
        if entity["status"] not in ALLOWED_STATUSES or entity["status"] == "done":
            fail(f"{entity_id}: invalid status")
        if entity["stakeholder_lane"] == "future_partner" and entity["status"] != "not_modeled":
            fail(f"{entity_id}: future partner meaning must remain not_modeled")
        if not isinstance(entity["facets"], dict) or set(entity["facets"]) != REQUIRED_FACETS:
            fail(f"{entity_id}: independent facets are incomplete")
        if not isinstance(entity["source_refs"], list) or not set(entity["source_refs"]).issubset(source_id_set):
            fail(f"{entity_id}: unresolved current source refs")
        new_by_id[entity_id] = entity

    effective_ids = set(base_entities) | set(new_by_id)
    for entity in new_entities:
        for relation in entity["relations"]:
            if not isinstance(relation, dict) or relation.get("target_id") not in effective_ids:
                fail(f"{entity['id']}: unresolved relation target {relation}")

    for update in unresolved_updates:
        if not isinstance(update, dict) or update.get("item_id") not in unresolved_ids:
            fail(f"unresolved update targets missing item: {update}")
        if update.get("status") not in ALLOWED_STATUSES:
            fail(f"invalid unresolved update status: {update.get('item_id')}")
        refs = update.get("source_refs")
        if not isinstance(refs, list) or not set(refs).issubset(source_id_set):
            fail(f"unresolved update has unresolved source refs: {update.get('item_id')}")

    target = handoff.get("target_penpot")
    if not isinstance(target, dict):
        fail("visualization handoff missing target_penpot")
    if target.get("binding_status") != "binding_pending" or target.get("transport") != "penpot_mcp":
        fail("visualization target must be MCP binding_pending")
    for field in ("account", "team_id", "file_id"):
        if target.get(field) is not None:
            fail(f"visualization target {field} must be null")
    if target.get("reuse_source_penpot_ids") is not False:
        fail("source Penpot IDs must not be reused")
    if target.get("plugin") != "not_applicable":
        fail("Product Atlas plugin must remain not_applicable")

    views = handoff.get("views")
    if not isinstance(views, list) or len(views) < 5:
        fail("visualization handoff must define the product views")
    view_ids = [view.get("view_id") for view in views if isinstance(view, dict)]
    if len(view_ids) != len(set(view_ids)) or not all(isinstance(x, str) for x in view_ids):
        fail("view IDs must be unique strings")
    for view in views:
        if view.get("binding_status") != "binding_pending" or view.get("page_binding") is not None:
            fail(f"{view.get('view_id')}: page binding must remain pending/null")

    site_view = next((view for view in views if view.get("view_id") == "view.site-as-is-map"), None)
    if site_view is None:
        fail("missing site-as-is map view")
    archetypes = site_view.get("archetypes")
    if not isinstance(archetypes, list):
        fail("site-as-is archetypes must be a list")
    archetype_ids = {row.get("archetype_id") for row in archetypes if isinstance(row, dict)}
    if archetype_ids != EXPECTED_ARCHETYPES or site_view.get("archetype_count") != 17:
        fail(f"site-as-is archetype coverage mismatch: {sorted(archetype_ids)}")

    referenced_entity_ids = set(
        iter_ids(
            handoff,
            {
                "primary_entity_ids",
                "secondary_entity_ids",
                "product_entity_ids",
                "problem_entity_ids",
                "ui_gap_entity_ids",
                "outcome_entity_ids",
                "current_capability_ids",
                "hypothesis_capability_ids",
                "journey_entity_ids",
                "job_entity_ids",
                "active_gap_ids",
            },
        )
    )
    missing_refs = sorted(referenced_entity_ids - effective_ids)
    if missing_refs:
        fail(f"visualization handoff references missing product IDs: {missing_refs}")

    handoff_text = HANDOFF_PATH.read_text(encoding="utf-8")
    if UUID_RE.search(handoff_text):
        fail("visualization handoff contains a fabricated/reused UUID")
    if '"done"' in handoff_text or '"done"' in DELTA_PATH.read_text(encoding="utf-8"):
        fail("generic done status is forbidden")

    return len(base_entities), len(new_entities), len(views), len(archetypes)


def validate_external(
    ui_baseline: Path | None,
    ui_review: Path | None,
    product_main: Path | None,
    astro_delta: Path | None,
    hypothesis: Path | None,
) -> None:
    if product_main is not None:
        expected = "8710e56fa3685f6c30a90cd062d532dce0348cce"
        if git_head(product_main) != expected:
            fail("product-main checkout SHA drift")

    if ui_baseline is not None:
        expected = "b86bab3e91511b3d4bd7d953b22bceb847f02a51"
        if git_head(ui_baseline) != expected:
            fail("UI baseline checkout SHA drift")
        path = ui_baseline / "catalog/product-atlas-linkage-handoff/v1/design-system-linkage.v1.json"
        if git_blob_sha(path) != "6c5fe775e2bcc7c767a9a1c3509b61f1feafce77":
            fail("UI baseline handoff blob drift")
        data = load_json(path)
        if data.get("status") != "READY_FOR_PARALLEL_GIT_ONLY_PRODUCT_ATLAS_SOT":
            fail("UI baseline handoff is not ready for parallel Git-only Product Atlas")
        if data.get("coverage") != {
            "archetypes": 17,
            "regions": 97,
            "patterns": 97,
            "components": 75,
            "states": 180,
            "boards": 34,
            "orphan_design_ids": [],
        }:
            fail("UI baseline handoff coverage drift")

    if ui_review is not None:
        expected = "47d0fef53c33200492d92f6a086d9b8813fe187e"
        if git_head(ui_review) != expected:
            fail("UI owner-review checkout SHA drift")
        path = ui_review / "docs/reviews/penpot-owner-comments-resolution-20260826.md"
        if git_blob_sha(path) != "8157e074a882fdc03a7db2a043078870d75a2a88":
            fail("owner-review ledger blob drift")
        text = path.read_text(encoding="utf-8")
        for marker in (
            "Status: `IN_PROGRESS`",
            "OV-50",
            "OV-52",
            "READY_FOR_OWNER_REREVIEW",
            "processed: NO",
            "ListingDiscoveryRail@6",
        ):
            if marker not in text:
                fail(f"owner-review ledger missing marker: {marker}")

    if astro_delta is not None:
        expected = "49c351873d40a2ea55f0a32837c7376e344d9c17"
        if git_head(astro_delta) != expected:
            fail("Astro owner-audit candidate checkout SHA drift")
        rail = astro_delta / "site/src/components/listings/ListingDiscoveryRail.astro"
        if git_blob_sha(rail) != "beb7f6c650d69f2b9eec245a004ff264d01010e9":
            fail("ListingDiscoveryRail@6 blob drift")
        rail_text = rail.read_text(encoding="utf-8")
        for marker in ("version?: 5 | 6", "'plane' | 'floating-island'"):
            if marker not in rail_text:
                fail(f"ListingDiscoveryRail@6 missing marker: {marker}")
        inventory = astro_delta / "docs/features/static-site-pages/artifacts/collection-1-inventory-2026-08-28.md"
        if git_blob_sha(inventory) != "3df5587c6c5766c5a3e18c1d6202cfb68795d895":
            fail("exact-seven artifact inventory blob drift")
        if "ровно 7" not in inventory.read_text(encoding="utf-8"):
            fail("artifact inventory no longer proves exactly seven items")

    if hypothesis is not None:
        expected = "f78e7c5974b4192bddf9eea901ee6d8b57f51560"
        if git_head(hypothesis) != expected:
            fail("agent-assisted discovery hypothesis checkout SHA drift")
        location = hypothesis / "docs/features/location-directory/README.md"
        assisted = hypothesis / "docs/features/static-site-pages/smart-vector-search/agent-assisted-event-discovery.md"
        if not location.is_file() or not assisted.is_file():
            fail("hypothesis checkout is missing location/agent-assisted documents")
        if "rescue" not in assisted.read_text(encoding="utf-8").lower():
            fail("agent-assisted discovery is no longer explicitly rescue-first")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ui-baseline", type=Path)
    parser.add_argument("--ui-review", type=Path)
    parser.add_argument("--product-main", type=Path)
    parser.add_argument("--astro-delta", type=Path)
    parser.add_argument("--hypothesis", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        base_count, new_count, view_count, archetype_count = validate_local()
        validate_external(
            args.ui_baseline,
            args.ui_review,
            args.product_main,
            args.astro_delta,
            args.hypothesis,
        )
    except (AssertionError, OSError, subprocess.CalledProcessError) as exc:
        print(f"PRODUCT_ATLAS_RECOVERY_20260828_FAIL: {exc}", file=sys.stderr)
        return 1
    print(
        "PRODUCT_ATLAS_RECOVERY_20260828_PASS "
        f"base_entities={base_count} new_entities={new_count} "
        f"views={view_count} archetypes={archetype_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
