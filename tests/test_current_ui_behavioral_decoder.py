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
    plans = [json.loads(line) for line in (out / 'behavior-specimen-plan.jsonl').read_text().splitlines()]
    treatments = {row['treatment'] for row in plans if row['contract_id'] == 'behavior.transport-experiment'}
    assert treatments == {'departure_board_v1', 'route_strips_v1', 'next_departure_queue_v1'}
    check = subprocess.run(['node', '--input-type=module', '-e', f"import {{ assertBehavioralSupplement }} from './scripts/current_ui_resource_graph/v1/behavioral.mjs'; console.log(assertBehavioralSupplement('{out}', {{allowIncomplete:true}}).status)"], cwd=ROOT, check=True, text=True, capture_output=True)
    assert check.stdout.strip() == 'valid'
