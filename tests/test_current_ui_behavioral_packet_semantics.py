import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _registry_summary():
    script = r"""
      import {
        ALL_BREAKPOINT_PROBE_IDS,
        buildBehaviorPacketRegistry,
        expectedRasterCount,
      } from './scripts/current_ui_resource_graph/v1/behavioral/registry.mjs';
      import {assertBehaviorPacketRegistry} from './scripts/current_ui_resource_graph/v1/behavioral/validate.mjs';
      import {DYNAMIC_REGIONS} from './scripts/current_ui_resource_graph/v1/behavioral-requirements.mjs';
      const registry=buildBehaviorPacketRegistry();assertBehaviorPacketRegistry(registry);
      const executable=registry.plans.filter((row)=>row.execution_status!=='explicit-blocker');
      const blockers=registry.plans.filter((row)=>row.execution_status==='explicit-blocker');
      const byId=Object.fromEntries(registry.plans.map((row)=>[row.id,row]));
      console.log(JSON.stringify({
        plans:registry.plans.length,
        executable:executable.length,
        blockers:blockers.length,
        rasters:expectedRasterCount(registry),
        dynamics:[...new Set(registry.plans.flatMap((row)=>row.dynamic_region_ids||[]))].sort(),
        requiredDynamics:DYNAMIC_REGIONS.map((row)=>row.id).sort(),
        breakpointIds:ALL_BREAKPOINT_PROBE_IDS,
        coveredBreakpointIds:[...new Set(registry.plans.flatMap((row)=>row.breakpoint_probe_ids||[]))].sort(),
        allReachable:registry.plans.every((row)=>Boolean(row.reachability)),
        allCoverageReferenced:registry.plans.every((row)=>Array.isArray(row.coverage_refs)&&row.coverage_refs.length>0),
        optionalActions:registry.plans.flatMap((row)=>row.steps).flatMap((row)=>row.actions).filter((row)=>row.optional).length,
        nonPageScrollTargets:registry.plans.flatMap((row)=>row.steps).flatMap((row)=>row.actions).filter((row)=>row.kind==='scroll-to-selector'&&row.scope!=='page').length,
        actionPhasesWithoutDelta:executable.flatMap((row)=>row.steps).filter((step)=>step.actions.length&&!step.expect?.semantic_delta).length,
        mediaWithoutProvenance:executable.filter((row)=>row.family==='media'&&!row.media_provenance).length,
        transport:registry.plans.filter((row)=>row.family==='transport').map((row)=>({id:row.id,minimum:row.fixture_provenance?.minimum_options,open:row.steps.find((step)=>step.phase==='compact-open')?.expect?.details_open,optional:row.steps.flatMap((step)=>step.actions).some((action)=>action.optional)})),
        menuShort:byId['behavior-packet.menu-short-scroll'],
        rail:byId['behavior-packet.rail-keyboard-home-end'],
        sticky:byId['behavior-packet.sticky-weekend-nav'],
        breakpointBlocker:byId['behavior-packet.breakpoint-container-runtime-coverage-gap'],
      }));
    """
    result = subprocess.run(
        ['node', '--input-type=module', '-e', script],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return json.loads(result.stdout)


def test_corrected_packet_registry_has_truthful_semantic_coverage():
    payload = _registry_summary()
    assert (payload['plans'], payload['executable'], payload['blockers'], payload['rasters']) == (67, 57, 10, 124)
    assert payload['dynamics'] == payload['requiredDynamics']
    assert len(payload['breakpointIds']) == 293
    assert payload['coveredBreakpointIds'] == sorted(payload['breakpointIds'])
    assert payload['allReachable'] is True
    assert payload['allCoverageReferenced'] is True
    assert payload['optionalActions'] == 0
    assert payload['nonPageScrollTargets'] == 0
    assert payload['actionPhasesWithoutDelta'] == 0
    assert payload['mediaWithoutProvenance'] == 0

    disclosures = [row for row in payload['transport'] if row['id'].endswith('-disclosure')]
    assert len(disclosures) == 3
    assert all(row['minimum'] >= 4 and row['open'] is True and row['optional'] is False for row in disclosures)

    menu_short = payload['menuShort']
    assert menu_short['root_selector'] == '.mobile-discovery-menu__panel'
    assert menu_short['steps'][0]['actions'] == [{
        'kind': 'click',
        'scope': 'page',
        'selector': '[data-mobile-discovery-menu][data-reference4-fullscreen] > summary',
        'target_requirement': 'required-element',
    }]
    assert menu_short['steps'][1]['actions'][0]['kind'] == 'scroll-element'
    assert 'selector' not in menu_short['steps'][1]['actions'][0]

    assert payload['rail']['execution_status'] == 'explicit-blocker'
    assert payload['rail']['blocks_ready'] is True
    assert payload['rail']['runtime_probe']['focusable'] is True
    assert payload['rail']['runtime_probe']['observed_scroll_left'] == 0

    assert payload['sticky']['visible_root_required'] is True
    assert all(step['expect'].get('root_geometry') == 'nonzero' for step in payload['sticky']['steps'])

    blocker = payload['breakpointBlocker']
    assert blocker['execution_status'] == 'explicit-blocker'
    assert blocker['blocks_ready'] is True
    assert blocker['breakpoint_probe_ids'] == payload['breakpointIds']
    assert blocker['blocked_states'] == ['per-probe-runtime-transition-unobserved']


def test_capture_is_bounded_and_emits_per_plan_progress():
    source = (ROOT / 'scripts/current_ui_resource_graph/v1/behavioral/capture.mjs').read_text()
    assert 'FONT_SETTLE_TIMEOUT_MS=4000' in source
    assert 'SCREENSHOT_TIMEOUT_MS=30000' in source
    assert 'CONTROLLED_ROUTE_TIMEOUT_MS=20000' in source
    assert "process.env.PW_TEST_SCREENSHOT_NO_FONTS_READY='1'" in source
    assert "resourceType:'image'" in source
    assert 'blocks_ready:plan.blocks_ready===true' in source
    assert 'filter({visible:true}).first().click()' in source
    assert 'descendant_states' in source
    assert "'aria-checked'" in source
    assert "'aria-pressed'" in source
    assert '[behavior-capture] plan ${planIndex+1}/${selected.length}' in source
    assert 'start\\n' in source
    assert 'complete elapsed_ms=' in source
