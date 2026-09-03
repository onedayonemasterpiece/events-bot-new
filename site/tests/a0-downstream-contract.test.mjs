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

test('current F0/M0 dependencies supersede historical checkpoints without changing A0 semantics', () => {
  assert.equal(contract.dependencies.accepted_combined_precursor, '0d73428dfafff2fd5450b74fd68e7bb40e92d2c5');
  assert.equal(contract.dependencies.f0, 'de92dabd4551e117ca1af1be7915ff223321cc32');
  assert.equal(contract.dependencies.m0_previous_checkpoint, '4c83fc7769b1dec2d92469373e3b15154af437f4');
  assert.equal(contract.dependencies.m0, 'c808c75dd975a9851e148ccf993c32787d2b6886');
  assert.equal(contract.current_ref_overlay.commit, 'efad360ed112ed1ef20a8015285a3f804c8bf6d0');
  assert.equal(contract.current_ref_overlay.m0_compatibility_verdict, 'COMPATIBLE_SUCCESSOR_NO_A0_SOURCE_CHANGE_REQUIRED');
});

test('thin-S base and exact focus-route extension are immutable', () => {
  assert.equal(contract.thin_s.repository, 'onedayonemasterpiece/lovekgd-design-system');
  assert.equal(contract.thin_s.branch, 'integration/launch-normalized-sot-penpot-20260902');
  assert.equal(contract.thin_s.base.commit, 'a800f619b66cdf713e94f234382481bb8621dd22');
  assert.equal(contract.thin_s.extensions.length, 1);
  assert.equal(contract.thin_s.extensions[0].commit, '410c643cb24a529f211d7d88e609fb61f830cacf');
  assert.equal(contract.thin_s.extensions[0].scope, 'PM0-37-16 through PM0-37-19');
  assert.equal(contract.thin_s.current_ref_overlay, 'efad360ed112ed1ef20a8015285a3f804c8bf6d0');
});

test('Penpot base and focus-route linked-instance extension are immutable', () => {
  assert.equal(contract.penpot_ready_spec.repository, 'onedayonemasterpiece/lovekgd-design-system');
  assert.equal(contract.penpot_ready_spec.branch, 'integration/launch-normalized-sot-penpot-20260902');
  assert.equal(contract.penpot_ready_spec.base.commit, '3d4e148b74594f6d0dda3adda0649441dc17bde7');
  assert.equal(contract.penpot_ready_spec.extensions.length, 1);
  assert.equal(contract.penpot_ready_spec.extensions[0].commit, '0c9f781a524b05f99d275ae3cd77b06b146f32e5');
  assert.deepEqual(contract.penpot_ready_spec.extensions[0].masters, ['A0-M15', 'A0-M16', 'A0-M17']);
  assert.deepEqual(contract.penpot_ready_spec.extensions[0].route_boards, ['A0-R18', 'A0-R19', 'A0-R20', 'A0-R21']);
  assert.equal(contract.penpot_ready_spec.status, 'SPEC_ONLY_NOT_MATERIALIZED');
});

test('V0 and rollback pointers are exact immutable commits with the current-ref overlay', () => {
  assert.equal(contract.v0_acceptance_matrix.commit, '952315ce0a4a5312e3f34c5afc7e8b05066c2147');
  assert.equal(contract.integration_and_rollback.commit, 'd0509f97e04fda3f24b026dd767262d2098f5463');
  assert.equal(contract.v0_acceptance_matrix.current_ref_overlay, 'efad360ed112ed1ef20a8015285a3f804c8bf6d0');
  assert.equal(contract.integration_and_rollback.current_ref_overlay, 'efad360ed112ed1ef20a8015285a3f804c8bf6d0');
  for (const layer of [contract.v0_acceptance_matrix, contract.integration_and_rollback]) {
    assert.equal(layer.repository, 'onedayonemasterpiece/lovekgd-design-system');
    assert.equal(layer.branch, 'integration/launch-normalized-sot-penpot-20260902');
    assert.match(layer.commit, /^[0-9a-f]{40}$/u);
  }
});

test('PM0 item 37 and product-evidence boundaries cannot be inflated', () => {
  assert.equal(contract.v0_acceptance_matrix.pm0_item_37_denominator, 19);
  assert.equal(contract.v0_acceptance_matrix.source_identity_at_v0_recensus, '16/19');
  assert.equal(contract.v0_acceptance_matrix.source_identity_after_mechanical_target, '19/19');
  assert.equal(contract.v0_acceptance_matrix.current_full_kaggle_build_identity, '0/19');
  assert.equal(contract.v0_acceptance_matrix.v0_browser_reviewed, '0/19');
  assert.equal(contract.golden_preflight.classification, 'PIPELINE_PREFLIGHT_ONLY');
  assert.equal(contract.golden_preflight.current_successor_acceptance_credit, false);
  assert.equal(contract.source_checkpoint.tests_executed, false);
  assert.equal(contract.source_checkpoint.candidate_integrated, false);
  assert.equal(contract.source_checkpoint.browser_verdict, 'NOT_CLAIMED');
});
