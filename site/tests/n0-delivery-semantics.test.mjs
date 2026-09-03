import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const receipt = JSON.parse(await readFile(
  new URL('../scripts/n0-delivery-semantics.v1.json', import.meta.url),
  'utf8',
));

const FROZEN = 'cebeafeee08251a327145ee973ee035cced65204';
const F0 = 'ea4eda91d03bd15bb99e26f4990fe9818e3d4d8b';
const M0 = '8b0c6709beca1f7b0bef81b27464f786df1d7806';
const A0 = '3ef253980bdfe0731158f5b8b4b47965fa153ce9';
const FR0 = '2231e1d668f896d634e5663b59520bc710d5fea6';

test('delivery correction changes no topology, ownership, branch, contract, T0 or generation path', () => {
  assert.equal(receipt.schema, 'kenigevents.n0-delivery-semantics.v1');
  assert.equal(receipt.contract_reference.version, '1.10.0');
  assert.equal(receipt.contract_reference.mutation_by_this_receipt, false);
  assert.equal(receipt.correction_scope.kind, 'DELIVERY_SEMANTICS_ONLY');
  for (const field of [
    'changes_topology',
    'changes_ownership',
    'changes_branches',
    'changes_contract',
    'changes_T0',
    'changes_generation_path',
  ]) assert.equal(receipt.correction_scope[field], false, `${field} must remain false`);
});

test('cebeafee remains the immutable publication transaction despite later role output', () => {
  const frozen = receipt.frozen_transaction;
  assert.equal(frozen.sha, FROZEN);
  assert.equal(frozen.branch, 'agent/static-site-single-kaggle-contract');
  assert.equal(frozen.r0_result_comment, 5531586743);
  assert.equal(frozen.publication_state, 'IN_PROGRESS_AS_OF_LAST_MEANINGFUL_ISSUE_READ');
  assert.equal(frozen.build_id, 'preview-real-cebeafeee-normalized-20260903-v1');
  assert.equal(frozen.data_mode, 'real');
  assert.deepEqual(frozen.page_classes, ['all']);
  assert.equal(frozen.source_and_local_gate_result, 'ACCEPTED_FOR_CANONICAL_PUBLICATION');
  assert.equal(frozen.do_not_reopen_or_delay_for_later_role_outputs, true);
  assert.deepEqual(frozen.later_outputs_are_next_candidate_inputs_only, [F0, M0, A0]);
  assert.equal(frozen.snapshot.id, 'issue621-real-cebeafeee-20260903T201333Z');
  assert.match(frozen.snapshot.sha256, /^[0-9a-f]{64}$/u);
  assert.ok(frozen.required_post_publication_verification.includes(
    'preview-build.json repo SHA equals cebeafeee08251a327145ee973ee035cced65204',
  ));
});

test('F0 rejection is narrowed to vocabulary/checker and preserves accepted source slices', () => {
  const result = receipt.f0_result_classification;
  assert.equal(result.comment, 5531682890);
  assert.equal(result.frozen_transaction_verdict, 'ACCEPTED_WITH_NONBLOCKING_VOCABULARY_CHECKER_DELTA');
  assert.equal(result.festivals, 'ACCEPTED_SOURCE');
  assert.equal(result.exhibitions, 'ACCEPTED_SOURCE');
  assert.equal(result.fr0_batch_4, 'ACCEPTED_SOURCE');
  assert.equal(result.duplicate_f0_token_or_style_owner, 'NOT_FOUND');
  assert.equal(result.vocabulary_checker_correction.sha, F0);
  assert.equal(result.vocabulary_checker_correction.classification, 'NEXT_CANDIDATE_ONLY');
  assert.equal(result.vocabulary_checker_correction.product_consumer_change, false);
  assert.equal(result.vocabulary_checker_correction.blocks_frozen_publication, false);
});

test('exactly one next-successor batch is selected from F0, M0 and A0', () => {
  const intake = receipt.next_successor_intake;
  assert.equal(intake.base, FROZEN);
  assert.deepEqual(intake.ordered_batches.map(({ role, sha }) => [role, sha]), [
    ['F0', F0],
    ['M0', M0],
    ['A0', A0],
  ]);
  assert.equal(new Set(intake.ordered_batches.map(({ role }) => role)).size, 3);
  assert.equal(intake.max_merge_ready_batches_outside_successor_per_role, 1);
  assert.equal(intake.candidate_lag_minutes_max, 30);
  assert.deepEqual(intake.next_full_real_preview_after_compatible_batch_count, [2, 3]);
  assert.equal(intake.next_full_real_preview_max_minutes, 60);
  assert.equal(intake.broken_batch_exclusion_is_individual, true);
  assert.equal(intake.materialization_owner, 'R0');
  assert.equal(intake.candidate_acceptance_owner, 'N0');
  assert.equal(intake.browser_verdict_owner, 'V0');
});

test('the current M0 head is one coherent batch and no speculative FR0 batch is admitted', () => {
  const m0 = receipt.current_pull_integrator_refs.M0;
  assert.equal(m0.result_comment, 5531788718);
  assert.equal(m0.published_merge_ready_head, '7e879210f68f7a7e4a53755f2e9431c61f445569');
  assert.equal(m0.current_head, M0);
  assert.equal(m0.changed_product_source_after_published_merge_ready_head, false);
  assert.deepEqual(m0.compatibility_tail, [
    '1828bde1c869150b25a292a65e8b20a646a5d611',
    M0,
  ]);
  assert.equal(m0.decision, 'CURRENT_HEAD_ACCEPTED_AS_THE_ONE_M0_NEXT_SUCCESSOR_BATCH');

  assert.equal(receipt.current_pull_integrator_refs.FR0.head, FR0);
  assert.equal(receipt.next_successor_intake.FR0_source_batch, null);
  assert.equal(receipt.next_successor_intake.FR0_admission_rule, 'ONLY_AFTER_FACTUAL_FR0_DRIFT');
});

test('publication opens an exact V0 trigger and an independently acceptable exhibitions slice', () => {
  const trigger = receipt.post_publication_v0_trigger;
  assert.equal(trigger.state, 'PENDING_EXACT_PUBLIC_URL_AND_MANIFEST');
  assert.equal(trigger.must_publish_immediately_after_N0_identity_acceptance, true);
  assert.equal(trigger.N0_must_not_issue_browser_PASS, true);
  for (const field of [
    'exact owner URL',
    'exact source SHA',
    'snapshot id and digest',
    'matching preview-build.json',
    'canonical Kaggle operation and artifact identity',
  ]) assert.ok(trigger.must_name.includes(field), `missing V0 trigger field: ${field}`);

  const slice = receipt.vertical_slice;
  assert.equal(slice.id, 'EXHIBITIONS_FR0_MEDIAFRAME');
  assert.deepEqual(slice.chain, [
    'ExhibitionsPersonalSurface',
    'ExhibitionPrototypeRow',
    'FR0 MediaFrame',
  ]);
  assert.equal(slice.source_state, 'ACCEPTED_IN_CEBEAFEE');
  assert.equal(slice.browser_state, 'PENDING_V0_SECTION_VERDICT');
  assert.equal(slice.acceptance_owner, 'N0');
  assert.match(slice.defect_isolation, /unrelated route/u);
});

test('the receipt makes no premature public, V0, slice or successor claim', () => {
  for (const nonClaim of [
    'canonical cebeafee publication completed',
    'public manifest verified',
    'independent V0 verdict received',
    'vertical slice browser-accepted',
    'next successor integrated or tested',
    'next successor full-real preview published',
  ]) assert.ok(receipt.non_claims.includes(nonClaim), `missing non-claim: ${nonClaim}`);
});
