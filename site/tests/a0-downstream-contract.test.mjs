import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const contract = JSON.parse(await readFile(
  new URL('../src/data/a0-downstream-contract.v1.json', import.meta.url),
  'utf8',
));

test('A0 downstream contract freezes one selective source transaction', () => {
  assert.equal(contract.schema_version, 'kenigevents.a0-downstream-contract.v1');
  assert.equal(contract.contract_version, '1.9.0');
  assert.equal(contract.role, 'A0');
  assert.equal(contract.mechanical_batch.issue_comment, 5527907602);
  assert.equal(contract.mechanical_batch.status, 'READY_FOR_R0_MECHANICAL');
  assert.equal(contract.mechanical_batch.wholesale_a0_branch_merge_allowed, false);
  assert.deepEqual(contract.mechanical_batch.clusters, [
    'A0-MECH-01',
    'A0-MECH-02',
    'A0-MECH-03',
    'A0-MECH-04',
    'A0-MECH-05',
    'A0-MECH-06',
  ]);
});

test('thin-S, Penpot, V0 and rollback pointers are exact immutable commits', () => {
  assert.equal(contract.thin_s.commit, 'a800f619b66cdf713e94f234382481bb8621dd22');
  assert.equal(contract.penpot_ready_spec.commit, '3d4e148b74594f6d0dda3adda0649441dc17bde7');
  assert.equal(contract.v0_acceptance_matrix.commit, '952315ce0a4a5312e3f34c5afc7e8b05066c2147');
  assert.equal(contract.integration_and_rollback.commit, 'd0509f97e04fda3f24b026dd767262d2098f5463');
  for (const layer of [
    contract.thin_s,
    contract.penpot_ready_spec,
    contract.v0_acceptance_matrix,
    contract.integration_and_rollback,
  ]) {
    assert.equal(layer.repository, 'onedayonemasterpiece/lovekgd-design-system');
    assert.equal(layer.branch, 'integration/launch-normalized-sot-penpot-20260902');
    assert.match(layer.commit, /^[0-9a-f]{40}$/u);
  }
});

test('PM0 item 37 and product-evidence boundaries cannot be inflated', () => {
  assert.equal(contract.v0_acceptance_matrix.pm0_item_37_denominator, 19);
  assert.equal(contract.v0_acceptance_matrix.source_identity_at_v0_recensus, '16/19');
  assert.equal(contract.v0_acceptance_matrix.current_full_kaggle_build_identity, '0/19');
  assert.equal(contract.v0_acceptance_matrix.v0_browser_reviewed, '0/19');
  assert.equal(contract.golden_preflight.classification, 'PIPELINE_PREFLIGHT_ONLY');
  assert.equal(contract.golden_preflight.current_successor_acceptance_credit, false);
  assert.equal(contract.source_checkpoint.tests_executed, false);
  assert.equal(contract.source_checkpoint.candidate_integrated, false);
  assert.equal(contract.source_checkpoint.browser_verdict, 'NOT_CLAIMED');
  assert.equal(contract.penpot_ready_spec.status, 'SPEC_ONLY_NOT_MATERIALIZED');
});
