#!/usr/bin/env python3
"""Generate Astro's frozen consumer bridge from the exact UI SoT v2 files.

This script is intentionally the only place where cross-repository copying is
allowed. Routes select scenario IDs; they never own payload copies or ID lists.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


SITE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = SITE_ROOT / "src/data/ui-reference-events-v2.json"
DEFAULT_REGISTRY_OUTPUT = SITE_ROOT / "src/data/design-system-reference-fixtures.json"


def raw_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_sha256(value: Any) -> str:
    body = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return hashlib.sha256(body).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def fixture_id_to_event_id(fixture_id: str) -> int:
    prefix = "event.real."
    if not fixture_id.startswith(prefix):
        raise SystemExit(f"unsupported fixture identity: {fixture_id}")
    return int(fixture_id.removeprefix(prefix))


def build(ui_sot_root: Path, output: Path, registry_output: Path) -> None:
    corpus_root = ui_sot_root / "catalog/fixtures/ui-reference-events/v2"
    registry_path = ui_sot_root / "catalog/fixtures/design-system-reference/v2/registry.v2.json"
    scenario_path = ui_sot_root / "catalog/fixtures/design-system-reference/v2/scenarios/archetype.collections.free.september.desktop-ready.v2.json"
    corpus_path = corpus_root / "corpus.json"
    projection_path = corpus_root / "projections/free-collection-september.v1.json"
    assets_path = corpus_root / "assets-manifest.json"

    corpus = read_json(corpus_path)
    projection = read_json(projection_path)
    ui_registry = read_json(registry_path)
    scenario = read_json(scenario_path)
    assets = read_json(assets_path)

    if corpus["corpus_id"] != "ui-reference-events.v2":
        raise SystemExit("expected Golden Event Corpus v2")
    if ui_registry["registry_id"] != "design-system-reference-v2":
        raise SystemExit("expected design-system reference registry v2")
    if scenario["scenario_id"] != "free-collection-september-desktop-v2":
        raise SystemExit("unexpected reference scenario")
    if projection["fixture_input_order"] != scenario["fixture_input_order"]:
        raise SystemExit("projection/scenario fixture input order drift")

    wrappers = []
    for row in corpus["fixtures"]:
        wrapper = read_json(corpus_root / row["payload_path"])
        if canonical_sha256(wrapper["preview_event"]) != row["preview_event_sha256"]:
            raise SystemExit(f"payload hash drift: {row['fixture_id']}")
        wrappers.append(wrapper)

    fixture_ids = [row["fixture_id"] for row in wrappers]
    if fixture_ids != ui_registry["pools"]["events.golden.v2"]:
        raise SystemExit("corpus order drift against UI registry")

    frozen = {
        "schema_version": "astro-ui-reference-events.v2",
        "generated": True,
        "authority": {
            "repository": "onedayonemasterpiece/lovekgd-design-system",
            "registry": "catalog/fixtures/design-system-reference/v2/registry.v2.json",
            "registry_sha256": raw_sha256(registry_path),
            "scenario": "catalog/fixtures/design-system-reference/v2/scenarios/archetype.collections.free.september.desktop-ready.v2.json",
            "scenario_sha256": raw_sha256(scenario_path),
            "corpus": "catalog/fixtures/ui-reference-events/v2/corpus.json",
            "corpus_file_sha256": raw_sha256(corpus_path),
            "corpus_content_sha256": corpus["corpus_sha256"],
            "projection": "catalog/fixtures/ui-reference-events/v2/projections/free-collection-september.v1.json",
            "projection_sha256": raw_sha256(projection_path),
            "assets_manifest_sha256": assets["assets_manifest_sha256"],
            "source_preview_export_sha256": corpus["source"]["preview_export_sha256"],
            "source_repository_sha": corpus["source"]["repository_sha"],
            "source_snapshot_sha256": corpus["source"]["snapshot"]["sha256"],
        },
        "projection": projection,
        "fixtures": wrappers,
    }
    write_json(output, frozen)

    scenario_ids = [fixture_id_to_event_id(value) for value in scenario["fixture_input_order"]]
    render_ids = [fixture_id_to_event_id(value) for value in scenario["expected_render_order"]]
    runtime_registry = {
        "schema_version": "design-system-reference-fixtures.v3",
        "profile_id": "design-system-reference-v3",
        "authority": {
            "ui_sot_registry_id": ui_registry["registry_id"],
            "ui_sot_contract": f"lovekgd-design-system:{frozen['authority']['registry']}",
            "ui_sot_contract_sha256": frozen["authority"]["registry_sha256"],
            "ui_sot_scenario": f"lovekgd-design-system:{frozen['authority']['scenario']}",
            "ui_sot_scenario_sha256": frozen["authority"]["scenario_sha256"],
            "golden_corpus_id": corpus["corpus_id"],
            "golden_corpus_content_sha256": corpus["corpus_sha256"],
            "payload_source": "site/src/data/ui-reference-events-v2.json",
            "rule": "Generated only by site/scripts/build-design-system-reference-fixtures-v3.py from exact UI SoT v2 payloads; routes select scenarios and never own IDs or payload fields.",
        },
        "events": {
            "component_conformance_fixture_ids": [row["event_id"] for row in wrappers],
            "archetype_fixture_ids": scenario_ids,
            "fixtures": [
                {
                    "event_id": row["event_id"],
                    "preview_event_sha256": row["preview_event_sha256"],
                    "coverage": row["coverage_tags"],
                }
                for row in wrappers
            ],
        },
        "festivals": {
            "rows": [
                {"role": "single-wide", "slugs": ["city-jazz"]},
                {"role": "packed-four-varied-framing", "slugs": ["sosedi", "grozd", "more-vnutri", "bolshoy-kaup"]},
                {"role": "paired-status-contrast", "slugs": ["v-edinstve", "jazz-v-filarmonii"]},
            ]
        },
        "clubs": {"fixture_slugs": ["game-vibes", "neural-researchers", "technology-researchers"]},
        "container_families": {
            "event_card_equal_height_grid": {
                "runtime_owner": "site/src/components/OptimizedEventCardGrid.astro",
                "card_family": "EventCard@2",
                "purpose": "Large EventCard rows on related, recommendation and collection surfaces; preserve all source cards and equalize cards within each rendered desktop row.",
            },
            "desktop_listing_rows": {
                "runtime_owners": [
                    "site/src/components/listings/ExactTimeTimeline.astro",
                    "site/src/components/listings/PopularBehaviorRows.astro",
                    "site/src/components/listings/WeekendEditorialTimeline.astro",
                ],
                "card_family": "ListingEventCard@9",
                "purpose": "Date, Popular and Weekend desktop listing rows. This is not an EventCard grid.",
            },
            "festival_timeline_rows": {
                "runtime_owner": "site/src/lib/festivalTimelineLayout.ts#packFestivalTimeline",
                "card_family": "FestivalCard",
                "purpose": "Festival rows with source-derived 1/4/2 packing.",
            },
            "interest_club_grid": {
                "runtime_owner": "site/src/pages/kluby-po-interesam/index.astro#.club-list",
                "card_family": "InterestClubCard@1",
                "purpose": "Interest-club grid; route-owned until its separate centralization task is certified.",
            },
        },
        "scenarios": {
            scenario["scenario_id"]: {
                "route": scenario["route_ref"]["route"],
                "viewport": scenario["viewport"],
                "reference_date": "2026-09-01",
                "updated_date": ui_registry["reference_clock"]["date"],
                "event_ids": scenario_ids,
                "expected_card_count": len(scenario_ids),
                "container_family": "event_card_equal_height_grid",
                "card_family": "EventCard@2",
                "availability": "local-preview-only",
                "expected_render_order": render_ids,
                "expected_groups": projection["expected_groups"],
                "coverage_requirements": projection["coverage_requirements"],
                "explicit_exclusions": projection["explicit_exclusions"],
                "review_states": scenario["acceptance"]["review_states"],
            }
        },
    }
    write_json(registry_output, runtime_registry)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ui-sot-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--registry-output", type=Path, default=DEFAULT_REGISTRY_OUTPUT)
    args = parser.parse_args()
    build(args.ui_sot_root.resolve(), args.output.resolve(), args.registry_output.resolve())


if __name__ == "__main__":
    main()
