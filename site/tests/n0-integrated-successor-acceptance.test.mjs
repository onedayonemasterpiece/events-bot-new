import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const receipt = JSON.parse(await readFile(
  new URL('../scripts/n0-integrated-successor-acceptance.v1.json', import.meta.url),
  'utf8',
));

const BASE = 'cebeafeee08251a327145ee973ee035cced65204';
const SUCCESSOR = '3ca6a143e4286c165282c2d8ceef1759a41185b7';
const F0 = 'ea4eda91d03bd15bb99e26f4990fe9818e3d4d8b';
const M0 = '105bac16be6c73916a25f3e78b02116869ed5e1e';
const A0 = '3ef253980bdfe0731158f5b8b4b47965fa153ce9';

const batch = (role) => receipt.integrated_batches.find((item) => item.role === role);

test('successor is one forward-only source-accepted transaction, not a runtime claim', () => {
  assert.equal(receipt.schema, 'kenigevents.n0-integrated-successor-acceptance.v1');
  assert.equal(receipt.contract_version, '1.10.0');
  assert.equal(receipt.base.sha, BASE);
  assert.equal(receipt.successor.sha, SUCCESSOR);
  assert.equal(receipt.successor.branch, 'agent/static-site-single-kaggle-contract');
  assert.equal(receipt.successor.classification, 'N0_ACCEPTED_INTEGRATED_SOURCE_RUNTIME_PENDING');
  assert.equal(receipt.successor.forward_only_from_base, true);
  assert.equal(receipt.successor.ahead_by, 25);
  assert.equal(receipt.successor.behind_by, 0);
  assert.equal(receipt.successor.merge_base, BASE);
  assert.equal(receipt.successor.source_acceptance, true);
  assert.equal(receipt.successor.executable_acceptance, false);
  assert.equal(receipt.successor.kaggle_publication_acceptance, false);
  assert.equal(receipt.successor.v0_acceptance, false);
});

test('exactly one accepted F0, M0 and A0 batch is integrated', () => {
  assert.deepEqual(receipt.integrated_batches.map(({ role, requested_sha }) => [role, requested_sha]), [
    ['F0', F0],
    ['M0', M0],
    ['A0', A0],
  ]);
  assert.equal(new Set(receipt.integrated_batches.map(({ role }) => role)).size, 3);
  assert.equal(batch('F0').integration_commit, 'b90704190314427c81d2d04cc7c51123d53fb68a');
  assert.equal(batch('F0').integration_form, 'PATCH_EQUIVALENT_SINGLE_BATCH');
  assert.equal(batch('M0').integration_commit, 'a06cd102e27af313ce1446fe84777fd6d5cc449d');
  assert.equal(batch('M0').integration_form, 'EXACT_MERGE_PARENT');
  assert.equal(batch('A0').integration_commit, '8e9975b69c6e353944a79586f5dba82faaa887c0');
  assert.equal(batch('A0').integration_form, 'PATCH_EQUIVALENT_SINGLE_FILE');
  assert.ok(receipt.integrated_batches.every(({ decision }) => decision === 'ACCEPTED_EXACT_SOURCE'));
});

test('F0, M0 and A0 exact blob identities are pinned', () => {
  assert.deepEqual(batch('F0').exact_blobs, {
    'site/src/components/design-system/check-f0-route-theme-bindings.mjs': 'ecd6730f595542f2bae1a3be38f2718f0e9490f9',
    'site/src/components/design-system/f0-route-theme-bindings.v1.json': '90abc8034f0254e67fe1f257d5072f1153018e5e',
    'site/src/components/design-system/f0-current-successor-final-source-review.v1.json': '12ac3c3de67ca69ab6128ab86fa6f12b559743ea',
  });
  assert.deepEqual(batch('M0').exact_blobs, {
    'site/src/components/EventCard.astro': '7bc689ac9b6f823955828706ea4f5ecb23acb58a',
    'site/src/components/listings/ListingEventCard.astro': 'b8a218b9a0accb9f64b51be332a4e797b4aac2b7',
    'site/src/lib/relatedCardLayout.mjs': '1088e47f5e82062b4f7140b60ead6eb1b5ee898f',
  });
  assert.deepEqual(batch('A0').exact_blobs, {
    'site/tests/a0-current-successor-fr0-consumer-boundary.test.mjs': 'b8041bd0b80719bd55008923c9d273105462da59',
  });
});

test('reconciliation changes only documented integration gates and no FR0 source', () => {
  const reconciliation = receipt.integration_reconciliation;
  assert.equal(reconciliation.commit, SUCCESSOR);
  assert.equal(reconciliation.subject, 'test(ui): reconcile post-cutover integration gates');
  assert.deepEqual(reconciliation.allowed_scope, [
    'CHANGELOG.md',
    'docs/features/static-site-pages/design-system/launch-normalization-48h.md',
    'site/tests/event-card-flex-placement.test.mjs',
    'site/tests/m0-post-fr0-test-ownership.test.mjs',
  ]);
  assert.equal(reconciliation.new_FR0_source, false);
  assert.equal(reconciliation.overwrites_accepted_FR0_owners, false);
});

test('there is no second role batch outside the successor', () => {
  const queue = receipt.role_queue_invariant;
  assert.equal(queue.maximum_merge_ready_batches_outside_successor_per_role, 1);
  assert.equal(queue.F0_outside_successor, 0);
  assert.equal(queue.M0_outside_successor, 0);
  assert.equal(queue.A0_outside_successor, 0);
  assert.equal(queue.M0_branch_noop_after_requested_sha.sha, 'ef9a514f78eccbd373d7d66c8f103378484cea6d');
  assert.equal(queue.M0_branch_noop_after_requested_sha.product_source_changes, 0);
  assert.equal(queue.M0_branch_noop_after_requested_sha.classification, 'NOT_A_SECOND_BATCH');
  assert.equal(queue.FR0_source_batch, null);
  assert.equal(queue.FR0_admission_rule, 'ONLY_AFTER_FACTUAL_FR0_DRIFT');
});

test('accepted exhibitions/FR0 source remains byte-stable in the successor', () => {
  assert.deepEqual(receipt.preserved_frozen_blobs, {
    'site/src/components/ExhibitionsPersonalSurface.astro': '61d065efbc9b05254601ae807b7fcffec701bd04',
    'site/src/components/ExhibitionPrototypeRow.astro': 'ff94b32a288b079f27ca9e8c33d6975f52012478',
    'site/src/components/exhibitionsMediaFrameBridge.mjs': '1898f0ce973676241d54530d51c17b577e7c6509',
    'site/src/components/media-frame.css': '1231b0665054da3cd9bf936585a7d2e02838b82a',
    'site/tests/fr0-exhibitions-media-frame-contract.test.mjs': 'ff26615718b83134db9bb65e1a10831818381743',
  });
});

test('remaining gate is executable, full-real, immutable and source-bound', () => {
  const gate = receipt.remaining_exact_gate;
  assert.equal(gate.owner, 'R0');
  assert.ok(gate.required.some((item) => item.includes(SUCCESSOR)));
  assert.ok(gate.required.includes('one canonical full-real Kaggle StaticSiteBuilder run'));
  assert.ok(gate.required.includes('HTTP-200 immutable owner URL'));
  assert.ok(gate.required.includes('matching preview-build.json with exact delivery SHA'));
  assert.ok(gate.required.includes('fresh snapshot and Kaggle operation/artifact identity'));
  assert.ok(gate.required.includes('no root or stable-ICS mutation'));
  assert.equal(gate.then, 'N0_RUNTIME_ACCEPTANCE_AND_EXACT_V0_TRIGGER');
});

test('source acceptance does not prematurely claim downstream evidence', () => {
  assert.deepEqual(receipt.non_claims, [
    'tests executed',
    'runtime accepted',
    'successor Kaggle preview published',
    'successor V0 verdict issued',
    'vertical slice browser accepted',
  ]);
});
