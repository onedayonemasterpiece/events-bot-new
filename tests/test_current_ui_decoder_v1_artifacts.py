import json
import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
MODULE = REPO / "scripts/current_ui_resource_graph/v1/artifacts.mjs"
EXPECTED = json.loads(
    (
        REPO
        / "tests/fixtures/current-ui-decoder-v1/artifacts/expected-inventory.json"
    ).read_text(encoding="utf-8")
)


def _node(expression: str):
    script = f"""
      import * as lane from {json.dumps(MODULE.as_uri())};
      const value = {expression};
      process.stdout.write(JSON.stringify(value));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _lane():
    return _node("lane.buildArtifactDecoderLane()")


def test_two_as_is_systems_remain_strictly_separate():
    decoded = _lane()
    assert decoded["schema_version"] == EXPECTED["schema_version"]
    assert [system["id"] for system in decoded["systems"]] == EXPECTED["system_ids"]
    focus, amber = decoded["systems"]
    assert focus["relationship_to_other_artifact_systems"] == "independent-not-a-variant"
    assert amber["relationship_to_other_artifact_systems"] == "independent-not-a-variant"
    assert set(focus["component_paths"]).isdisjoint(amber["component_paths"])
    assert focus["persistence"]["storage_key"] != amber["persistence"]["storage_key"]
    assert focus["reachability"]["status"] == "lab-only"
    assert amber["reachability"]["status"] == "source-only"
    assert not focus["reachability"]["production_observed"]
    assert not amber["reachability"]["production_observed"]


def test_focus_states_catalog_baseline_and_execution_boundary_are_exact():
    decoded = _lane()
    focus = next(item for item in decoded["systems"] if item["id"].endswith("focus-egg-prototype-v1"))
    states = {
        item["value"]
        for item in decoded["state_records"]
        if item["system_id"] == focus["id"] and item["axis"] == "artifact-state"
    }
    assert states == {"locked", "eligible", "found", "unavailable"}
    assert focus["state_resolution_precedence"] == [
        "found",
        "unavailable",
        "eligible",
        "locked",
    ]
    assert focus["catalog_baseline"]["counts"] == EXPECTED["focus_catalog_counts"]
    assert focus["catalog_baseline"]["by_id"]["FG-E06"] == "unavailable"
    assert focus["catalog_baseline"]["by_id"]["FG-E12"] == "locked"
    assert focus["reachability"]["executable_scope"] == "FG-E12-saved-list-demo-only"
    assert focus["gates"][0]["condition"] == "distinct_renderable_event_ids >= 3"
    assert focus["gates"][1]["result"] == "FG-E12 absent"


def test_amber_has_a_fail_closed_production_truth_table():
    decoded = _lane()
    amber = next(item for item in decoded["systems"] if item["id"].endswith("amber-research-collectible-v1"))
    gate = next(item for item in amber["gates"] if item["id"] == "amber.production-hard-block")
    production_rows = [row for row in gate["truth_table"] if row["site_mode"] == "production"]
    assert production_rows
    assert all(row["enabled"] is False for row in production_rows)
    assert next(
        row
        for row in gate["truth_table"]
        if row["site_mode"] == "secret_candidate" and row["flag"] == "tail"
    )["enabled"] is True
    assert amber["collection_baseline"] == {
        "slot_count": 5,
        "active_slot_count": 1,
        "reserved_slot_count": 4,
        "states": ["empty", "found"],
        "initial_progress": "0/5",
        "found_progress": "1/5",
        "proof": amber["collection_baseline"]["proof"],
    }
    assert amber["route_contexts"][0]["public_production"] == "hard-blocked"
    assert amber["route_contexts"][1]["public_production"] == "unavailable-shell-only"


def test_transition_inventory_preserves_observable_and_omitted_updates():
    decoded = _lane()
    assert [item["id"] for item in decoded["transition_records"]] == EXPECTED["transition_ids"]
    focus_found = next(
        item
        for item in decoded["transition_records"]
        if item["id"] == "artifact-transition.focus.eligible-to-found"
    )
    assert focus_found["from"] == "eligible"
    assert focus_found["to"] == "found"
    assert focus_found["emitted_event"] == "focus-egg-found"
    assert focus_found["omitted_update"] == "focus-egg-artifact__glyph text remains eligible glyph"
    amber_collect = next(
        item
        for item in decoded["transition_records"]
        if item["id"] == "artifact-transition.amber.awake-to-collected"
    )
    assert amber_collect["through"] == "collecting"
    assert amber_collect["timer_ms"] == {"regular": 460, "reduced_motion": 0}
    assert "aria-pressed=true" in amber_collect["observable_updates"]
    amber_dialog = next(
        item
        for item in decoded["transition_records"]
        if item["id"] == "artifact-transition.amber.found-to-dialog-open"
    )
    assert "focus-restored-to-last-trigger" in amber_dialog["close_contract"]


def test_pairwise_plan_is_bounded_and_covers_source_breakpoint_sides():
    decoded = _lane()
    plan = decoded["specimen_plan"]
    assert len(plan) <= EXPECTED["maximum_specimen_count"]
    assert all(item["observation_status"] == "not-captured" for item in plan)
    assert all(item["proof_label"] == "controlled-specimen-planned-not-observed" for item in plan)
    widths = {width for item in plan for width in item["viewport_widths"]}
    assert widths == set(EXPECTED["boundary_probe_widths"])
    responsive_widths = {
        width for item in decoded["responsive_contexts"] for width in item["probe_widths"]
    }
    assert widths == responsive_widths
    contexts = {item["context"] for item in plan}
    assert {
        "standalone-artifact",
        "catalog",
        "saved-list-demo",
        "artifact-route",
        "non-production-artifact-route",
        "weekend-mobile-rail",
    } <= contexts
    capture_channels = {
        "element-screenshot",
        "bounded-dom-summary",
        "computed-styles",
        "geometry",
        "css-variables",
        "accessibility-state",
        "focus-state",
        "hidden-open-expanded-disabled-state",
        "breakpoint-context",
        "override-source",
    }
    assert all(capture_channels <= set(item["capture_requirements"]) for item in plan)
    assert any(item["axes"].get("density") == "compact" for item in plan)
    assert any(item["axes"].get("input") == "keyboard" for item in plan)
    assert any(item["axes"].get("motion") == "reduced" for item in plan)


def test_known_mismatches_are_explicit_not_normalization_decisions():
    decoded = _lane()
    mismatches = {item["id"]: item for item in decoded["mismatches"]}
    assert set(mismatches) == set(EXPECTED["required_mismatch_ids"])
    assert mismatches["mismatch.artifacts.amber-false-transport-family"][
        "prohibited_family_claim"
    ] == "family.transport"
    assert "not the pre-rendered mark glyph" in mismatches[
        "mismatch.artifacts.focus-catalog-mark"
    ]["observed_fact"]
    assert "denominator" in mismatches[
        "mismatch.artifacts.focus-unavailable-found-count"
    ]["observed_fact"]
    assert "does not listen" in mismatches[
        "mismatch.artifacts.focus-no-storage-listener"
    ]["observed_fact"]
    assert all(item["decision"] == "NOT_MERGED" for item in mismatches.values())
    assert all(item["normalization_allowed"] is False for item in mismatches.values())


def test_parent_collectibles_remains_unresolved_and_unsynthesized():
    decoded = _lane()
    parent = next(
        item
        for item in decoded["unresolved"]
        if item["id"] == "artifact-parent.collectibles-unresolved"
    )
    assert parent["child_system_ids"] == EXPECTED["system_ids"]
    assert parent["relationship"] == "unresolved-not-a-variant-contract"
    assert parent["decision"] == "NOT_MERGED"
    assert parent["merge_allowed"] is False
    assert parent["synthesis_allowed"] is False
    assert parent["normalization_allowed"] is False


def test_lane_is_deterministic_and_claims_no_capture_or_runtime_run():
    result = _node(
        "({"
        "same: lane.stableSerializeArtifactLane() === lane.stableSerializeArtifactLane(),"
        "first: lane.stableSerializeArtifactLane(),"
        "second: lane.stableSerializeArtifactLane(lane.buildArtifactDecoderLane())"
        "})"
    )
    assert result["same"] is True
    assert result["first"] == result["second"]
    decoded = json.loads(result["first"])
    assert decoded["specimen_observations"] == []
    assert decoded["constraints"]["browser_capture_claimed"] is False
    assert decoded["constraints"]["private_corpus_run_claimed"] is False
    assert decoded["constraints"]["normalization"] is False
    assert decoded["constraints"]["astro_css_mutation"] is False


def test_validator_rejects_production_enablement_and_parent_merge():
    result = _node(
        "(() => {"
        "const first = lane.buildArtifactDecoderLane();"
        "first.systems[1].gates.find(x => x.id === 'amber.production-hard-block').truth_table[1].enabled = true;"
        "let production = null; try { lane.validateArtifactDecoderLane(first); } catch (error) { production = error.message; }"
        "const second = lane.buildArtifactDecoderLane();"
        "second.unresolved.find(x => x.id === lane.ARTIFACT_PARENT_ID).merge_allowed = true;"
        "let parent = null; try { lane.validateArtifactDecoderLane(second); } catch (error) { parent = error.message; }"
        "return { production, parent };"
        "})()"
    )
    assert "production hard block" in result["production"]
    assert "unresolved parent" in result["parent"]
