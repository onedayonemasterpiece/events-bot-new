import json
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
