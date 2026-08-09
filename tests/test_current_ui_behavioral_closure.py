import json
import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def node(script, *args, check=True):
    return subprocess.run(
        ['node', '--input-type=module', '-e', script, *map(str, args)],
        cwd=ROOT, text=True, capture_output=True, check=check,
    )


def test_registry_marks_superseded_evidence_gates_nonblocking():
    script = r"""
      import {buildBehaviorPacketRegistry} from './scripts/current_ui_resource_graph/v1/behavioral/registry.mjs';
      import {assertBehaviorPacketRegistry} from './scripts/current_ui_resource_graph/v1/behavioral/validate.mjs';
      const registry=buildBehaviorPacketRegistry();assertBehaviorPacketRegistry(registry);
      const byId=Object.fromEntries(registry.plans.map(row=>[row.id,row]));
      console.log(JSON.stringify({rail:byId['behavior-packet.rail-keyboard-home-end'],probe:byId['behavior-packet.breakpoint-container-runtime-coverage-gap']}));
    """
    payload = json.loads(node(script).stdout)
    assert payload['rail']['blocks_ready'] is False
    assert payload['rail']['home_end_required'] is False
    assert payload['rail']['semantic_contract'] == 'ordinary-focusable-horizontal-content-list-not-composite'
    assert payload['probe']['blocks_ready'] is False
    assert payload['probe']['superseded_by'] == 'breakpoint-probe-observations.jsonl'
    assert len(payload['probe']['breakpoint_probe_ids']) == 293


def test_closure_constants_bind_prior_review_and_design_publication():
    script = r"""
      import {DESIGN_PUBLICATION,PRIOR_REVIEWED_MANIFEST_SHA256,PINNED_SOURCE_SHA} from './scripts/current_ui_resource_graph/v1/behavioral/closure-materialize.mjs';
      console.log(JSON.stringify({DESIGN_PUBLICATION,PRIOR_REVIEWED_MANIFEST_SHA256,PINNED_SOURCE_SHA}));
    """
    payload = json.loads(node(script).stdout)
    assert payload['PINNED_SOURCE_SHA'] == 'ef7aa62e45c60f7a12da6160f490719c0721ec03'
    assert payload['PRIOR_REVIEWED_MANIFEST_SHA256'] == 'c6c62cee8bea4e9440ff85bc75c46bc85cf5abf3e2fdcd4c7357c6ece916436f'
    assert payload['DESIGN_PUBLICATION']['main_commit'] == 'f9cb3c931d6f2200f0a4221f5130b3a6299f7005'
    assert payload['DESIGN_PUBLICATION']['r07_path'] == 'docs/research/ui-normalization-2026-08/07-cross-research-synthesis-and-adoption.md'


def test_breakpoint_rasters_do_not_wait_unbounded_for_optional_remote_fonts():
    script = r"""
      delete process.env.PW_TEST_SCREENSHOT_NO_FONTS_READY;
      await import('./scripts/current_ui_resource_graph/v1/behavioral/breakpoint-runtime.mjs');
      console.log(process.env.PW_TEST_SCREENSHOT_NO_FONTS_READY || 'missing');
    """
    assert node(script).stdout.strip() == '1'


def test_exact_local_closure_fixture_when_supplied():
    fixture = os.environ.get('CURRENT_UI_BEHAVIORAL_CLOSURE_FIXTURE')
    if not fixture:
        return
    result = subprocess.run(
        ['node', 'scripts/current_ui_resource_graph/v1/behavioral/closure-validate.mjs', fixture],
        cwd=ROOT, text=True, capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload == {
        'status': 'valid', 'base': 'valid',
        'final_status': 'READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS',
        'terminal': 293, 'pass': 236, 'mismatch': 39, 'unreachable': 18,
        'observations': 134, 'new_rasters': 10, 'reviews': 134,
    }
