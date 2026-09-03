import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const raw = await readFile(new URL('../src/data/a0-route-family-normalization.v1.json', import.meta.url), 'utf8');
const matrix = JSON.parse(raw);

test('A0 grouped route-family matrix separates strict convergence from substantive implementation', () => {
  assert.equal(matrix.schema_version, 'a0-route-family-normalization-v1');
  assert.equal(matrix.matrix_version, '1.2.0');
  assert.equal(matrix.source_contract, 'site/src/data/design-system-production-surface-contract.v1.json');
  assert.equal(matrix.grouped_family_scope.denominator, 9);
  assert.equal(matrix.families.length, matrix.grouped_family_scope.denominator);

  const strict = matrix.families.filter((family) => family.status === 'source_converged_unintegrated');
  assert.equal(strict.length, matrix.grouped_family_scope.strict_source_converged);
  assert.equal(matrix.grouped_family_scope.strict_fraction, `${strict.length}/9`);
  assert.equal(matrix.grouped_family_scope.strict_fraction, '3/9');
  assert.equal(matrix.grouped_family_scope.substantive_source_implemented, 5);
  assert.equal(matrix.grouped_family_scope.substantive_fraction, '5/9');
  assert.match(matrix.grouped_family_scope.correction, /former 5\/9 field mixed substantive implementation with strict convergence/u);
  assert.equal(matrix.browser_verdict_owner, 'V0');
});

test('current dependency refs resolve through the compatible M0 overlay', () => {
  assert.equal(matrix.current_refs.accepted_combined_precursor, '0d73428dfafff2fd5450b74fd68e7bb40e92d2c5');
  assert.equal(matrix.current_refs.f0_dependency, 'de92dabd4551e117ca1af1be7915ff223321cc32');
  assert.equal(matrix.current_refs.m0_previous_checkpoint, '4c83fc7769b1dec2d92469373e3b15154af437f4');
  assert.equal(matrix.current_refs.m0_dependency, 'c808c75dd975a9851e148ccf993c32787d2b6886');
  assert.match(matrix.current_refs.current_ref_overlay, /efad360ed112ed1ef20a8015285a3f804c8bf6d0$/u);
});

test('every grouped family has concrete source evidence and an honest remaining boundary', () => {
  const expectedIds = [
    'collections',
    'festivals',
    'exhibitions',
    'for_me',
    'focus_group',
    'artifacts',
    'event_detail',
    'interest_clubs',
    'information_pages',
  ];
  assert.deepEqual(matrix.families.map((family) => family.id), expectedIds);
  for (const family of matrix.families) {
    assert.ok(typeof family.status === 'string' && family.status.length > 0, `${family.id} lacks status`);
    assert.ok(Array.isArray(family.evidence) && family.evidence.length > 0, `${family.id} lacks evidence`);
    assert.ok(typeof family.remaining === 'string' && family.remaining.length > 0, `${family.id} lacks remaining boundary`);
  }
});

test('strict convergence is limited to collections, artifacts and information pages', () => {
  const strictIds = matrix.families
    .filter((family) => family.status === 'source_converged_unintegrated')
    .map((family) => family.id);
  assert.deepEqual(strictIds, ['collections', 'artifacts', 'information_pages']);
});

test('open families name exact mechanical, foundation and browser boundaries', () => {
  const byId = Object.fromEntries(matrix.families.map((family) => [family.id, family]));
  assert.match(byId.festivals.remaining, /R0 route identity|F0 exact festival-theme|V0/u);
  assert.match(byId.exhibitions.remaining, /inner-family|F0 exact exhibition-theme|V0/u);
  assert.match(byId.for_me.remaining, /consumer-local \.ke-icon-role square|24px/u);
  assert.match(byId.focus_group.remaining, /FocusEggCollectionRouteComposition|ClosedFocusHubRouteComposition/u);
  assert.match(byId.event_detail.remaining, /MediaFrame|EventMediaRail|EventHero exception/u);
  assert.match(byId.interest_clubs.remaining, /F0 exact detail-route foundation binding/u);
});

test('PM0 item 37 keeps the exact 19-case evidence boundary', () => {
  assert.equal(matrix.pm0_item_37.denominator, 19);
  assert.equal(matrix.pm0_item_37.source_identity_at_v0_recensus, '16/19');
  assert.equal(matrix.pm0_item_37.source_identity_after_mechanical_target, '19/19');
  assert.equal(matrix.pm0_item_37.current_full_kaggle_build_identity, '0/19');
  assert.equal(matrix.pm0_item_37.v0_browser_reviewed, '0/19');
});

test('downstream contracts and the one mechanical batch are explicit', () => {
  assert.equal(matrix.mechanical_batch.issue_comment, 5527907602);
  assert.deepEqual(matrix.mechanical_batch.clusters, [
    'A0-MECH-01',
    'A0-MECH-02',
    'A0-MECH-03',
    'A0-MECH-04',
    'A0-MECH-05',
    'A0-MECH-06',
  ]);
  for (const key of [
    'local_pointer',
    'thin_s',
    'thin_s_focus_extension',
    'penpot_ready',
    'penpot_focus_extension',
    'v0_acceptance',
    'integration_rollback',
    'current_ref_overlay',
  ]) {
    assert.equal(typeof matrix.downstream_contracts[key], 'string');
    assert.ok(matrix.downstream_contracts[key].length > 0);
  }
  assert.ok(matrix.claims_not_made.includes('source tests executed'));
  assert.ok(matrix.claims_not_made.includes('V0 browser acceptance'));
});
