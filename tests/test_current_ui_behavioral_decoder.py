import json
import hashlib
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/current-ui-behavioral-mini'

def test_behavioral_decoder_is_append_only_and_fail_closed(tmp_path):
    out = tmp_path / 'supplement'
    command = [
        'node', 'scripts/current_ui_resource_graph/behavioral-decode.mjs',
        '--source-root', str(FIXTURE / 'site'), '--base-snapshot-root', str(FIXTURE / 'base'),
        '--output', str(out), '--source-sha', 'ef7aa62e45c60f7a12da6160f490719c0721ec03',
    ]
    result = subprocess.run(command, cwd=ROOT, check=True, text=True, capture_output=True)
    payload = json.loads(result.stdout)
    assert payload['status'] == 'EVIDENCE_COLLECTION_INCOMPLETE'
    manifest = json.loads((out / 'manifest.json').read_text())
    assert manifest['immutable_v1_modified'] is False
    assert manifest['constraints']['normalization'] is False
    assert (out / 'behavior-contracts.jsonl').exists()
    assert (out / 'experiment-registry.jsonl').exists()
    assert (out / 'artifact-index.json').exists()
    assert json.loads((out / 'independent-audit.json').read_text())['status'] == 'pending'
    provenance = [json.loads(line) for line in (out / 'requirements-provenance-ledger.jsonl').read_text().splitlines()]
    statuses = {row['status'] for row in provenance}
    assert statuses <= {
        'accepted-current', 'implemented-current', 'accepted-not-implemented',
        'experiment-unresolved', 'historical-replaced', 'proposal-only', 'conflict', 'unresolved',
    }
    assert next(row for row in provenance if row['id'] == 'requirement.curated.media.2x3-status')['status'] == 'unresolved'
    assert next(row for row in provenance if row['id'] == 'requirement.curated.event-detail.cta-binding')['status'] == 'implemented-current'
    media = [json.loads(line) for line in (out / 'media-policy-matrix.jsonl').read_text().splitlines()]
    ratios = {row['ratio'] for row in media}
    assert {'4:5', '5:4', '3:2', '2:3', '1:1', 'intrinsic/source'} <= ratios
    dynamic = [json.loads(line) for line in (out / 'dynamic-region-loading-matrix.jsonl').read_text().splitlines()]
    personal_feed = next(row for row in dynamic if row['id'] == 'dynamic-region.personal-feed')
    assert personal_feed['skeleton']['status'] == 'not-present-current'
    assert personal_feed['offline_disposition'] == 'not-implemented'
    plans = [json.loads(line) for line in (out / 'behavior-specimen-plan.jsonl').read_text().splitlines()]
    treatments = {row['treatment'] for row in plans if row['contract_id'] == 'behavior.transport-experiment'}
    assert treatments == {'departure_board_v1', 'route_strips_v1', 'next_departure_queue_v1'}
    check = subprocess.run(['node', '--input-type=module', '-e', f"import {{ assertBehavioralSupplement }} from './scripts/current_ui_resource_graph/v1/behavioral.mjs'; console.log(assertBehavioralSupplement('{out}', {{allowIncomplete:true}}).status)"], cwd=ROOT, check=True, text=True, capture_output=True)
    assert check.stdout.strip() == 'valid'


def test_behavioral_validator_rejects_old_status(tmp_path):
    out = tmp_path / 'supplement'
    subprocess.run([
        'node', 'scripts/current_ui_resource_graph/behavioral-decode.mjs',
        '--source-root', str(FIXTURE / 'site'), '--base-snapshot-root', str(FIXTURE / 'base'),
        '--output', str(out), '--source-sha', 'ef7aa62e45c60f7a12da6160f490719c0721ec03',
    ], cwd=ROOT, check=True, capture_output=True)
    manifest = json.loads((out / 'manifest.json').read_text())
    manifest['status'] = 'READY_FOR_NORMALIZATION_CHARTER_SYNTHESIS'
    (out / 'manifest.json').write_text(json.dumps(manifest))
    result = subprocess.run([
        'node', '--input-type=module', '-e',
        f"import {{ assertBehavioralSupplement }} from './scripts/current_ui_resource_graph/v1/behavioral.mjs'; assertBehavioralSupplement('{out}', {{allowIncomplete:true}})",
    ], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode != 0
    assert 'Invalid behavioral supplement status' in result.stderr


def test_behavior_packet_registry_is_closed_bounded_and_not_merged():
    script = r"""
      import {buildBehaviorPacketRegistry,expectedRasterCount} from './scripts/current_ui_resource_graph/v1/behavioral/registry.mjs';
      import {assertBehaviorPacketRegistry} from './scripts/current_ui_resource_graph/v1/behavioral/validate.mjs';
      const registry=buildBehaviorPacketRegistry();assertBehaviorPacketRegistry(registry);
      const payload={plans:registry.plans.length,rasters:expectedRasterCount(registry),blockers:registry.plans.filter(x=>x.execution_status==='explicit-blocker').length,
        treatments:registry.plans.filter(x=>x.family==='transport').map(x=>x.id),ratios:[...new Set(registry.plans.flatMap(x=>x.ratios||[]))],
        stop:registry.plans.every(x=>x.decision==='NOT_MERGED'&&x.normalization_allowed===false&&x.production_state_claimed===false)};
      console.log(JSON.stringify(payload));
    """
    result = subprocess.run(['node', '--input-type=module', '-e', script], cwd=ROOT, check=True, text=True, capture_output=True)
    payload = json.loads(result.stdout)
    assert payload == {
        'plans': 67, 'rasters': 124, 'blockers': 10,
        'treatments': [
            'behavior-packet.transport-departure-board-v1-baseline',
            'behavior-packet.transport-departure-board-v1-disclosure',
            'behavior-packet.transport-route-strips-v1-baseline',
            'behavior-packet.transport-route-strips-v1-disclosure',
            'behavior-packet.transport-next-departure-queue-v1-baseline',
            'behavior-packet.transport-next-departure-queue-v1-disclosure',
        ],
        'ratios': ['5:4', '4:5'], 'stop': True,
    }


def test_behavior_harness_never_copies_candidate_node_modules(tmp_path):
    source = tmp_path / 'source-site'
    target = tmp_path / 'target-site'
    (source / 'node_modules' / 'package').mkdir(parents=True)
    (source / 'node_modules' / 'package' / 'sentinel').write_text('must-not-copy')
    (source / 'src').mkdir()
    (source / 'src' / 'page.astro').write_text('<main>exact source</main>')
    script = r"""
      import {copySiteWithoutNodeModules} from './scripts/current_ui_resource_graph/v1/behavioral/harness.mjs';
      copySiteWithoutNodeModules(process.argv[1],process.argv[2]);
    """
    subprocess.run(['node', '--input-type=module', '-e', script, str(source), str(target)], cwd=ROOT, check=True)
    assert (target / 'src' / 'page.astro').read_text() == '<main>exact source</main>'
    assert not (target / 'node_modules').exists()


def test_capture_materializer_stays_incomplete_before_full_resolution_review(tmp_path):
    source = tmp_path / 'source'
    capture = tmp_path / 'capture'
    final = tmp_path / 'final'
    reviewed_final = tmp_path / 'reviewed-final'
    review_ledger_path = tmp_path / 'visual-review-ledger.jsonl'
    subprocess.run([
        'node', 'scripts/current_ui_resource_graph/behavioral-decode.mjs',
        '--source-root', str(FIXTURE / 'site'), '--base-snapshot-root', str(FIXTURE / 'base'),
        '--output', str(source), '--source-sha', 'ef7aa62e45c60f7a12da6160f490719c0721ec03',
    ], cwd=ROOT, check=True, capture_output=True)
    generator = r"""
      import {createHash} from 'node:crypto';import {mkdirSync,writeFileSync} from 'node:fs';import {join} from 'node:path';
      import {buildBehaviorPacketRegistry} from './scripts/current_ui_resource_graph/v1/behavioral/registry.mjs';
      const root=process.argv[1];mkdirSync(join(root,'behavior-rasters'),{recursive:true});const sha=x=>createHash('sha256').update(x).digest('hex');const observations=[];const blockers=[];
      for(const plan of buildBehaviorPacketRegistry().plans){if(plan.execution_status==='explicit-blocker'){blockers.push({schema_version:plan.schema_version,id:`behavior-blocker.${sha(plan.id).slice(0,18)}`,plan_id:plan.id,family:plan.family,reason:plan.blocker_reason,source_path:plan.source_path||null,reachability:plan.reachability,dynamic_region_ids:plan.dynamic_region_ids||[],breakpoint_probe_ids:plan.breakpoint_probe_ids||[],coverage_refs:plan.coverage_refs||[],blocked_states:plan.blocked_states||[],blocks_ready:plan.blocks_ready===true,status:'explicit-blocker',production_state_claimed:false,decision:'NOT_MERGED'});continue;}
        for(let i=0;i<plan.steps.length;i++){const step=plan.steps[i];if(!step.capture)continue;const bytes=Buffer.from(`fake-png:${plan.id}:${step.phase}`);const name=`${sha(plan.id+step.phase).slice(0,18)}.png`;writeFileSync(join(root,'behavior-rasters',name),bytes);observations.push({schema_version:plan.schema_version,id:`behavior-observation.${sha(plan.id+'\\0'+step.phase).slice(0,18)}`,plan_id:plan.id,family:plan.family,phase:step.phase,sequence_index:i,source_sha:plan.source_sha,evidence_plane:plan.evidence_plane,reachability:plan.reachability,dynamic_region_ids:plan.dynamic_region_ids||[],breakpoint_probe_ids:plan.breakpoint_probe_ids||[],coverage_refs:plan.coverage_refs||[],route_hash:sha(plan.route),viewport:plan.viewport,container:{planned_width:plan.container_width||null,actual_width:100,actual_height:100},ratios:plan.ratios||[],media_provenance:plan.media_provenance||null,action_receipts:step.actions.map((action)=>({kind:action.kind,target:{status:'resolved'},result:'applied'})),transition:{assertions_passed:true},font_settle:{status:'ready'},image_settle:plan.screenshot_stabilization?{status:'settled',image_count:0,complete_count:0}:{status:'not-requested'},screenshot:{path:`behavior-rasters/${name}`,bytes:bytes.length,sha256:sha(bytes),dhash:'0000000000000000'},dom:{full_html_retained:false},network:{raw_urls_retained:false},evidence_status:'captured-not-reviewed',review_status:'pending-human-full-resolution-review',production_state_claimed:false,normalization_allowed:false,decision:'NOT_MERGED'});}}
      writeFileSync(join(root,'behavior-specimen-observations.jsonl'),observations.map(JSON.stringify).join('\n')+'\n');writeFileSync(join(root,'behavior-capture-blockers.jsonl'),blockers.map(JSON.stringify).join('\n')+'\n');
    """
    subprocess.run(['node', '--input-type=module', '-e', generator, str(capture)], cwd=ROOT, check=True)
    subprocess.run([
        'node', 'scripts/current_ui_resource_graph/v1/behavioral/materialize.mjs',
        '--source-supplement', str(source), '--capture-root', str(capture), '--output', str(final),
    ], cwd=ROOT, check=True, capture_output=True)
    receipt = json.loads((final / 'receipt.json').read_text())
    assert receipt['final_status'] == 'EVIDENCE_COLLECTION_INCOMPLETE'
    assert receipt['counts'] == {'explicit_blockers': 10, 'observations': 124, 'plans': 67, 'rasters': 124, 'reviews': 0}
    plans = [json.loads(line) for line in (final / 'behavior-specimen-plan.jsonl').read_text().splitlines()]
    assert sum(row['capture_status'] == 'captured-not-reviewed' for row in plans) == 57
    assert sum(row['capture_status'] == 'explicit-blocker' for row in plans) == 10
    assert sum(row.get('blocks_ready') is True for row in plans) == 2
    reviews = [json.loads(line) for line in (final / 'visual-review-ledger.jsonl').read_text().splitlines()]
    assert len(reviews) == 124
    assert all(row['review_status'] == 'pending-human-full-resolution-review' for row in reviews)
    portable_base_path = '../decoder-v1-snapshot-20260808T124842-4786ac53bc'
    assert json.loads((final / 'manifest.json').read_text())['base_snapshot']['path'] == portable_base_path
    assert json.loads((final / 'artifact-index.json').read_text())['base_snapshot']['path'] == portable_base_path
    assert json.loads((final / 'artifact-receipt.json').read_text())['base_snapshot']['path'] == portable_base_path
    dynamic_rows = [
        json.loads(line)
        for line in (final / 'dynamic-region-loading-matrix.jsonl').read_text().splitlines()
    ]
    assert len(dynamic_rows) == 13
    assert all(
        row['runtime_packet_ids'] or row['explicit_blocker_packet_ids']
        for row in dynamic_rows
    )
    assert all(row['runtime_evidence_status'] != 'coverage-missing' for row in dynamic_rows)

    # A complete file-level review is necessary but cannot erase exact
    # blocks_ready findings.  The materializer must remain fail-closed without
    # demanding READY-only Actions/Release/audit metadata.
    observations = [
        json.loads(line)
        for line in (capture / 'behavior-specimen-observations.jsonl').read_text().splitlines()
    ]
    review_rows = [
        {
            'schema_version': 'current_ui_behavioral_visual_review_v1_1',
            'id': f"behavior-review.{hashlib.sha256(row['id'].encode()).hexdigest()[:18]}",
            'observation_id': row['id'],
            'plan_id': row['plan_id'],
            'path': row['screenshot']['path'],
            'sha256': row['screenshot']['sha256'],
            'media_type': 'image/png',
            'review_status': 'reviewed-full-resolution',
            'full_resolution_opened': True,
            'visual_result': 'fixture-raster-opened-for-fail-closed-test',
            'reviewer': 'test-reviewer',
            'reviewed_at': '2026-08-09T00:00:00Z',
            'decision': 'NOT_MERGED',
        }
        for row in observations
    ]
    review_ledger_path.write_text(
        ''.join(json.dumps(row, sort_keys=True) + '\n' for row in review_rows)
    )
    subprocess.run([
        'node', 'scripts/current_ui_resource_graph/v1/behavioral/materialize.mjs',
        '--source-supplement', str(source), '--capture-root', str(capture),
        '--review-ledger', str(review_ledger_path), '--output', str(reviewed_final),
    ], cwd=ROOT, check=True, capture_output=True)
    reviewed_receipt = json.loads((reviewed_final / 'receipt.json').read_text())
    reviewed_manifest = json.loads((reviewed_final / 'manifest.json').read_text())
    assert reviewed_receipt['final_status'] == 'EVIDENCE_COLLECTION_INCOMPLETE'
    assert reviewed_receipt['status'] == 'partial'
    assert reviewed_receipt['counts']['reviews'] == 124
    reviewed_unresolved = [
        json.loads(line)
        for line in (reviewed_final / 'unresolved.jsonl').read_text().splitlines()
    ]
    blocking_plan_ids = {
        row.get('plan_id')
        for row in reviewed_unresolved
        if row.get('blocks_ready') is True
    }
    assert {
        'behavior-packet.rail-keyboard-home-end',
        'behavior-packet.breakpoint-container-runtime-coverage-gap',
    } <= blocking_plan_ids
    assert len(reviewed_receipt['blockers']) >= 2
    assert reviewed_manifest['human_visual_review']['completed'] is True
    assert reviewed_manifest['human_visual_review']['reviewed_raster_count'] == 124
    assert reviewed_manifest['blockers'] == reviewed_receipt['blockers']
    assert json.loads((reviewed_final / 'artifact-receipt.json').read_text())['status'] == 'review-complete-evidence-incomplete'

    # The deep validator must reject a tampered compact entry even when an
    # attacker also refreshes the outer manifest/receipt hash chain.
    artifact_index_path = final / 'artifact-index.json'
    artifact_index = json.loads(artifact_index_path.read_text())
    compact = next(row for row in artifact_index['entries'] if row['storage'] == 'compact-supplement')
    compact['sha256'] = '0' * 64
    artifact_index_path.write_text(json.dumps(artifact_index, sort_keys=True) + '\n')
    manifest_path = final / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    artifact_bytes = artifact_index_path.read_bytes()
    manifest['outputs']['artifact-index.json']['bytes'] = len(artifact_bytes)
    manifest['outputs']['artifact-index.json']['sha256'] = hashlib.sha256(artifact_bytes).hexdigest()
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + '\n')
    receipt_path = final / 'receipt.json'
    receipt = json.loads(receipt_path.read_text())
    receipt['manifest_sha256'] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    receipt_path.write_text(json.dumps(receipt, sort_keys=True) + '\n')
    result = subprocess.run([
        'node', '--input-type=module', '-e',
        f"import {{ assertBehavioralSupplement }} from './scripts/current_ui_resource_graph/v1/behavioral.mjs'; assertBehavioralSupplement('{final}', {{allowIncomplete:true}})",
    ], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode != 0
    assert 'Behavioral compact artifact hash mismatch' in result.stderr


def test_behavior_capture_workflow_is_fixed_identity_and_review_pending():
    import yaml
    workflow_path = ROOT / '.github/workflows/current-ui-behavioral-decoder-v1-1.yml'
    parsed = yaml.safe_load(workflow_path.read_text())
    assert parsed
    source = workflow_path.read_text()
    assert 'ef7aa62e45c60f7a12da6160f490719c0721ec03' in source
    assert 'e77fc2457fadfdffb46ed2d90304ebb91e89a715' in source
    assert 'CAPTURE_COMPLETE_NO_GO_PENDING_REVIEW' in source
    assert 'retention-days: 30' in source
    assert 'READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS' not in source
    assert 'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02' in source
