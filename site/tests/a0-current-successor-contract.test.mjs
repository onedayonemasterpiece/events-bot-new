import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const contract = JSON.parse(await readFile(
  new URL('../src/data/a0-current-successor-consumer-closure.v1.json', import.meta.url),
  'utf8',
));

test('A0 contract is bound to the current pre-F0 successor without inflating acceptance', () => {
  assert.equal(contract.schema, 'kenigevents.a0.current-successor-consumer-closure.v1');
  assert.equal(contract.contract_version, '1.10.0');
  assert.equal(contract.role, 'A0');
  assert.equal(contract.fresh_census.current_successor.sha, '1bc6d9cb4c122046f4782532381de953727c1da6');
  assert.equal(contract.published_previews.current_fresh_real.source_sha, contract.fresh_census.current_successor.sha);
  assert.equal(contract.published_previews.current_fresh_real.classification, 'VALID_PUBLIC_PRE_F0_BASELINE');
  assert.equal(contract.published_previews.current_fresh_real.independent_v0_pass, false);
  assert.equal(contract.candidate_review.decision, 'ACCEPT_AS_INTEGRATION_BASE_NOT_AS_FINAL_A0_SUCCESSOR');
  assert.equal(contract.candidate_review.whole_a0_branch_merge_allowed, false);
});

test('accepted F0-reviewed direct A0 patches are exact', () => {
  assert.deepEqual(contract.source_outputs.accepted_direct_patches, [
    {
      path: 'site/src/components/InterestClubCard.astro',
      commit: '29538035d94a15adac9fd8ee7463ece87c8d1033',
      f0_acceptance_comment: 5529863067,
    },
    {
      path: 'site/src/pages/kluby-po-interesam/[slug]/index.astro',
      commit: '2aa70f2ff8819d83e58da1473a35435d22783578',
      f0_acceptance_comment: 5529948310,
    },
  ]);
  assert.equal(contract.fresh_census.f0.semantic_source_floor, '0fb2938344cf96b05be0df09dfb9e69525b3717d');
  assert.equal(contract.fresh_census.m0.consumer_conflict_with_this_contract, false);
});

test('one executable transform owns exactly six A0 consumer paths', () => {
  const transform = contract.source_outputs.executable_transform;
  assert.equal(transform.path, 'site/scripts/apply-a0-current-successor-consumer-closure.mjs');
  assert.equal(transform.commit, '5e66444b20a0c4b90004a895345a5c8389ec8530');
  assert.equal(transform.target_paths.length, 6);
  assert.deepEqual(transform.target_paths, [
    'site/src/components/InterestClubCard.astro',
    'site/src/pages/kluby-po-interesam/[slug]/index.astro',
    'site/src/pages/festivali/index.astro',
    'site/src/components/ExhibitionsPersonalSurface.astro',
    'site/src/pages/fokus-gruppa/kollektsiya/index.astro',
    'site/src/pages/zakrytaya-afisha/index.astro',
  ]);
  assert.equal(contract.materialization_transaction.ready_for_r0_comment, 5527907602);
  assert.equal(contract.materialization_transaction.one_batch_only, true);
});

test('A0 executable transaction keeps F0, FR0 and M0 roots read-only', () => {
  const forbidden = contract.source_outputs.executable_transform.forbidden_root_mutations;
  assert.ok(forbidden.includes('site/src/components/design-system/**'));
  assert.ok(forbidden.includes('site/src/components/media-frame.css'));
  assert.ok(forbidden.includes('site/src/components/EventMediaRail.astro'));
  assert.ok(forbidden.includes('site/src/components/EventHero.astro'));
  assert.ok(forbidden.includes('site/src/components/AdaptiveEventCardGrid.astro'));
  assert.match(contract.rollback.policy, /Preserve F0, FR0 and M0 canonical roots/u);
});

test('every A0 source unit has its own rollback boundary', () => {
  const targetPaths = contract.source_outputs.executable_transform.target_paths;
  const rollbackPaths = contract.rollback.units.map((unit) => unit.path);
  assert.deepEqual(rollbackPaths, targetPaths);
  assert.equal(new Set(contract.rollback.units.map((unit) => unit.id)).size, targetPaths.length);
  for (const unit of contract.rollback.units) {
    assert.ok(unit.trigger.length > 0, `${unit.id} lacks trigger`);
  }
});

test('completion truth and next product gate remain evidence-bound', () => {
  assert.equal(contract.pm0_and_product_gate.item_37_denominator, 19);
  assert.equal(contract.pm0_and_product_gate.identity_target_after_materialization, '19/19');
  assert.equal(contract.pm0_and_product_gate.current_post_closure_full_kaggle_identity, '0/19');
  assert.equal(contract.pm0_and_product_gate.current_independent_v0_reviewed, '0/19');
  assert.equal(contract.truth_boundary.source_decisions_complete, true);
  assert.equal(contract.truth_boundary.source_runtime_tests_executed_by_a0, false);
  assert.equal(contract.truth_boundary.current_successor_contains_closure, false);
  assert.equal(contract.truth_boundary.post_closure_kaggle_preview_exists, false);
  assert.equal(contract.truth_boundary.independent_v0_pass_exists, false);
  assert.equal(contract.truth_boundary.penpot_materialized, false);
});
