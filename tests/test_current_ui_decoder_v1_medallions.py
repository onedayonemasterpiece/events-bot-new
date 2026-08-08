import json
import subprocess
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
MODULE = REPO / "scripts/current_ui_resource_graph/v1/medallions.mjs"
FIXTURE = REPO / "tests/fixtures/current-ui-decoder-v1/medallions/cases.json"


def _node(expression: str):
    script = f"""
import * as m from {json.dumps(MODULE.as_uri())};
import {{ readFileSync }} from 'node:fs';
const fixture = JSON.parse(readFileSync({json.dumps(str(FIXTURE))}, 'utf8'));
const value = await (async () => ({expression}))();
process.stdout.write(JSON.stringify(value));
"""
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _node_failure(expression: str) -> str:
    script = f"""
import * as m from {json.dumps(MODULE.as_uri())};
try {{ {expression}; process.exitCode = 2; }} catch (error) {{ process.stdout.write(error.message); }}
"""
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_axes_defaults_and_empty_state_are_exact_and_unmerged():
    value = _node("({lane:m.buildMedallionDecoderLane(), empty:m.projectEventTokenMedallions()})")
    lane = value["lane"]
    assert lane["axes"]["layouts"] == ["inline", "desktop-slots"]
    assert lane["axes"]["roles"] == ["main", "secondary"]
    assert lane["axes"]["kinds"] == ["organizer", "source", "program", "pushkin", "badge", "pill"]
    assert lane["axes"]["identity_categories"] == ["venue_brand", "festival_brand", "festival", "organizer"]
    assert lane["axes"]["identity_resolutions"] == ["resolved", "conflicting_source_identity", "ambiguous_venue_identity"]
    assert lane["defaults"] == {
        "allow_top_slot": True,
        "identity_category": "organizer-when-omitted-by-definition",
        "layout": "inline",
        "token_role": "secondary",
    }
    assert value["empty"]["layout"] == "inline"
    assert value["empty"]["allow_top_slot"] is True
    assert value["empty"]["groups"] == []
    assert value["empty"]["rendered"] is False
    assert value["empty"]["decision"] == "NOT_MERGED"
    assert value["empty"]["recommendation"] == "unresolved"
    assert value["empty"]["normalization_allowed"] is False


def test_desktop_slots_put_at_most_one_main_on_top_and_strip_pills():
    expression = """m.projectEventTokenMedallions({
      layout:'desktop-slots', allow_top_slot:true, tokens:[
        {key:'main',kind:'organizer',role:'main',identity_category:'festival',image_url:'/a.svg'},
        {key:'second-main',kind:'organizer',role:'main',identity_category:'organizer',image_url:'/b.svg'},
        {key:'source',kind:'source',role:'secondary',image_url:'/source.svg'},
        {key:'price',kind:'pill',role:'secondary'}
      ]
    })"""
    projected = _node(expression)
    assert projected["resolved_main_token_key"] == "main"
    assert projected["groups"][0]["slot"] == "top"
    assert projected["groups"][0]["token_keys"] == ["main"]
    assert projected["groups"][1]["token_keys"] == ["second-main", "source"]
    assert projected["removed_for_desktop_keys"] == ["price"]
    assert all(token["kind"] != "pill" for group in projected["groups"] for token in group["tokens"])

    top_off = _node(expression.replace("allow_top_slot:true", "allow_top_slot:false"))
    assert [group["slot"] for group in top_off["groups"]] == ["inline"]
    assert top_off["groups"][0]["token_keys"] == ["main", "second-main", "source"]


def test_identity_and_token_caps_include_fail_closed_and_free_retention():
    value = _node("""({
      resolved:m.selectResolvedMedallionIdentities(fixture.identity_candidates),
      conflict:m.selectResolvedMedallionIdentities(fixture.identity_candidates,'conflicting_source_identity'),
      ambiguous:m.selectResolvedMedallionIdentities(fixture.identity_candidates,'ambiguous_venue_identity'),
      overflow:m.projectEventTokenMedallions({tokens:fixture.overflow_tokens})
    })""")
    assert [item["key"] for item in value["resolved"]] == ["festival-brand", "organizer", "venue-a"]
    assert value["conflict"] == []
    assert [item["key"] for item in value["ambiguous"]] == ["festival-brand", "organizer"]
    # free is seventh in source order: exact AS-IS behavior keeps first five and free.
    assert value["overflow"]["visible_token_keys"] == [
        "organizer:a", "source:a", "program:a", "pushkin-card", "price", "free-admission"
    ]
    assert len(value["overflow"]["visible_token_keys"]) == 6


def test_media_fallback_contract_matches_picture_source_behavior():
    value = _node("""({
      webpFallback:m.medallionPrimaryImage({image_url:'/a.webp',fallback_image_url:'/a.png'}),
      webpOnly:m.medallionPrimaryImage({image_url:'/a.webp'}),
      vector:m.medallionPrimaryImage({image_url:'/a.svg'})
    })""")
    assert value["webpFallback"] == {
        "fallback_used_as_img_src": True,
        "primary_image_src": "/a.png",
        "webp_source_srcset": "/a.webp",
    }
    assert value["webpOnly"]["primary_image_src"] == "/a.webp"
    assert value["webpOnly"]["webp_source_srcset"] is None
    assert value["vector"]["primary_image_src"] == "/a.svg"
    assert value["vector"]["fallback_used_as_img_src"] is False


def test_invalid_values_and_unsafe_assets_fail_closed():
    assert "Invalid medallion layout" in _node_failure("m.projectEventTokenMedallions({layout:'merged'})")
    assert "allow_top_slot must be boolean" in _node_failure("m.projectEventTokenMedallions({allow_top_slot:'yes'})")
    assert "Invalid medallion token kind" in _node_failure("m.projectEventTokenMedallions({tokens:[{key:'x',kind:'new'}]})")
    assert "Invalid medallion identity category" in _node_failure("m.selectResolvedMedallionIdentities([{key:'x',category:'other'}])")
    assert "local root-relative asset" in _node_failure("m.medallionPrimaryImage({image_url:'https://example.invalid/a.webp'})")
    assert "cannot retain organizer" in _node_failure(
        "m.projectEventTokenMedallions({identity_resolution:'conflicting_source_identity',tokens:[{key:'x',kind:'organizer',image_url:'/x.svg'}]})"
    )


def test_breakpoint_and_height_boundaries_are_explicit():
    rows = _node("fixture.boundary_viewports.map(([width,height]) => m.classifyMedallionViewport(width,height))")
    indexed = {(row["width"], row["height"]): row for row in rows}
    assert indexed[(1023, 900)]["event_page_surface"] == "mobile"
    assert indexed[(1023, 900)]["event_component_layout"] == "inline"
    assert indexed[(1024, 900)]["event_page_surface"] == "desktop"
    assert indexed[(1024, 900)]["event_component_layout"] == "desktop-slots"
    assert indexed[(1280, 720)]["desktop_height_treatment"] == "compact-height-72px-image-tokens"
    assert indexed[(1280, 721)]["desktop_height_treatment"] == "regular-height-clamped-image-tokens"
    assert indexed[(1440, 900)]["desktop_width_context"] == "1440-plus"
    assert all(row["decision"] == "NOT_MERGED" and row["normalization_allowed"] is False for row in rows)


def test_plans_are_bounded_deterministic_and_do_not_claim_captures():
    value = _node("""(() => {
      const a=m.buildMedallionDecoderLane(); const b=m.buildMedallionDecoderLane();
      return {a,b, equal:JSON.stringify(a)===JSON.stringify(b)};
    })()""")
    assert value["equal"] is True
    lane = value["a"]
    assert len(lane["production_route_plan"]) == 11
    assert len(lane["controlled_specimen_plan"]) == 14
    assert len(lane["specimen_plan"]) == 25
    assert lane["specimen_observations"] == []
    for row in lane["production_route_plan"] + lane["controlled_specimen_plan"]:
        assert row["observation_status"] == "planned-not-captured"
        assert row["proof_label"].endswith("not-observed")
        assert row["decision"] == "NOT_MERGED"
        assert row["recommendation"] == "unresolved"
        assert row["normalization_allowed"] is False
    assert lane["capture_contract"]["observation_status"] == "not-captured-by-this-lane"
    assert lane["pinned_source_sha"] == "ef7aa62e45c60f7a12da6160f490719c0721ec03"
    assert lane["constraints"]["browser_capture_claimed"] is False
    assert lane["constraints"]["private_corpus_run_claimed"] is False
    assert all(lane["constraints"][key] is False for key in (
        "merge", "split", "normalization", "tokenization",
        "penpot_mutation", "astro_css_mutation",
    ))
    serialized = _node("m.stableSerializeMedallionLane()")
    assert serialized.endswith("\n")


def test_listing_exhibition_and_lab_resources_stay_separate_with_mismatches():
    lane = _node("m.buildMedallionDecoderLane()")
    resources = {item["resource_family"]: item for item in lane["resource_candidates"]}
    assert {
        "event-detail", "listing-card", "mobile-listing-rail", "exhibition-row",
        "medallion-catalog-lab", "design-system-lab-instance",
    } == set(resources)
    assert all(item["equivalence_status"] == "NOT_MERGED" for item in resources.values())
    assert resources["medallion-catalog-lab"]["reachability"] == "lab-only"
    assert "manual catalog markup" in resources["medallion-catalog-lab"]["separation_basis"]
    mismatches = {item["id"]: item for item in lane["mismatches"]}
    assert "mismatch.medallions-organizer-count-28-vs-stale-27" in mismatches
    count_channels = mismatches["mismatch.medallions-organizer-count-28-vs-stale-27"]["channels"]
    assert [channel["value"] for channel in count_channels] == [28, 28, 27]
    assert "mismatch.medallions-detail-geometry-doc-vs-consumer-css" in mismatches
    assert "mismatch.medallions-lab-catalog-not-component-equivalent" in mismatches
    assert all(item["recommendation"] == "unresolved" for item in lane["mismatches"])
    assert any(item["blocks_handoff"] for item in lane["unresolved"])
