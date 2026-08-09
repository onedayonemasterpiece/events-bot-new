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
        'plans': 50, 'rasters': 99, 'blockers': 5,
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


def test_capture_materializer_stays_incomplete_before_full_resolution_review(tmp_path):
    source = tmp_path / 'source'
    capture = tmp_path / 'capture'
    final = tmp_path / 'final'
    subprocess.run([
        'node', 'scripts/current_ui_resource_graph/behavioral-decode.mjs',
        '--source-root', str(FIXTURE / 'site'), '--base-snapshot-root', str(FIXTURE / 'base'),
        '--output', str(source), '--source-sha', 'ef7aa62e45c60f7a12da6160f490719c0721ec03',
    ], cwd=ROOT, check=True, capture_output=True)
    generator = r"""
      import {createHash} from 'node:crypto';import {mkdirSync,writeFileSync} from 'node:fs';import {join} from 'node:path';
      import {buildBehaviorPacketRegistry} from './scripts/current_ui_resource_graph/v1/behavioral/registry.mjs';
      const root=process.argv[1];mkdirSync(join(root,'behavior-rasters'),{recursive:true});const sha=x=>createHash('sha256').update(x).digest('hex');const observations=[];const blockers=[];
      for(const plan of buildBehaviorPacketRegistry().plans){if(plan.execution_status==='explicit-blocker'){blockers.push({schema_version:plan.schema_version,id:`behavior-blocker.${sha(plan.id).slice(0,18)}`,plan_id:plan.id,family:plan.family,reason:plan.blocker_reason,source_path:plan.source_path||null,status:'explicit-blocker',production_state_claimed:false,decision:'NOT_MERGED'});continue;}
        for(let i=0;i<plan.steps.length;i++){const step=plan.steps[i];if(!step.capture)continue;const bytes=Buffer.from(`fake-png:${plan.id}:${step.phase}`);const name=`${sha(plan.id+step.phase).slice(0,18)}.png`;writeFileSync(join(root,'behavior-rasters',name),bytes);observations.push({schema_version:plan.schema_version,id:`behavior-observation.${sha(plan.id+'\\0'+step.phase).slice(0,18)}`,plan_id:plan.id,family:plan.family,phase:step.phase,sequence_index:i,source_sha:plan.source_sha,evidence_plane:plan.evidence_plane,route_hash:sha(plan.route),viewport:plan.viewport,ratios:plan.ratios||[],screenshot:{path:`behavior-rasters/${name}`,bytes:bytes.length,sha256:sha(bytes),dhash:'0000000000000000'},dom:{full_html_retained:false},network:{raw_urls_retained:false},evidence_status:'captured-not-reviewed',review_status:'pending-human-full-resolution-review',production_state_claimed:false,normalization_allowed:false,decision:'NOT_MERGED'});}}
      writeFileSync(join(root,'behavior-specimen-observations.jsonl'),observations.map(JSON.stringify).join('\n')+'\n');writeFileSync(join(root,'behavior-capture-blockers.jsonl'),blockers.map(JSON.stringify).join('\n')+'\n');
    """
    subprocess.run(['node', '--input-type=module', '-e', generator, str(capture)], cwd=ROOT, check=True)
    subprocess.run([
        'node', 'scripts/current_ui_resource_graph/v1/behavioral/materialize.mjs',
        '--source-supplement', str(source), '--capture-root', str(capture), '--output', str(final),
    ], cwd=ROOT, check=True, capture_output=True)
    receipt = json.loads((final / 'receipt.json').read_text())
    assert receipt['final_status'] == 'EVIDENCE_COLLECTION_INCOMPLETE'
    assert receipt['counts'] == {'explicit_blockers': 5, 'observations': 99, 'plans': 50, 'rasters': 99, 'reviews': 0}
    plans = [json.loads(line) for line in (final / 'behavior-specimen-plan.jsonl').read_text().splitlines()]
    assert sum(row['capture_status'] == 'captured-not-reviewed' for row in plans) == 45
    assert sum(row['capture_status'] == 'explicit-blocker' for row in plans) == 5
    reviews = [json.loads(line) for line in (final / 'visual-review-ledger.jsonl').read_text().splitlines()]
    assert len(reviews) == 99
    assert all(row['review_status'] == 'pending-human-full-resolution-review' for row in reviews)

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
