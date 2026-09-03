import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const receipt = JSON.parse(await readFile(
  new URL('../scripts/n0-delivery-semantics.v1.json', import.meta.url),
  'utf8',
));

const FROZEN = 'cebeafeee08251a327145ee973ee035cced65204';
const F0 = 'ea4eda91d03bd15bb99e26f4990fe9818e3d4d8b';
const M0 = '105bac16be6c73916a25f3e78b02116869ed5e1e';
const A0 = '3ef253980bdfe0731158f5b8b4b47965fa153ce9';
const FR0 = '2231e1d668f896d634e5663b59520bc710d5fea6';

test('delivery correction changes no topology, ownership, branch, contract, T0 or generation path', () => {
  assert.equal(receipt.schema, 'kenigevents.n0-delivery-semantics.v1');
  assert.equal(receipt.version, '1.2.0');
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

test('cebeafee immutable publication is exact and later role outputs stay next-only', () => {
  const frozen = receipt.frozen_transaction;
  assert.equal(frozen.sha, FROZEN);
  assert.equal(frozen.branch, 'agent/static-site-single-kaggle-contract');
  assert.equal(frozen.initial_r0_result_comment, 5531586743);
  assert.equal(frozen.publication_result_comment, 5531984205);
  assert.equal(frozen.publication_state, 'N0_ACCEPTED_EXACT_IMMUTABLE_PUBLICATION');
  assert.equal(frozen.build_id, 'preview-real-cebeafeee-normalized-20260903-v1');
  assert.equal(frozen.public_url, `https://kenigevents.ru/${frozen.build_id}/__preview/`);
  assert.equal(frozen.manifest_url, `https://kenigevents.ru/${frozen.build_id}/preview-build.json`);
  assert.equal(frozen.data_mode, 'real');
  assert.deepEqual(frozen.page_classes, ['all']);
  assert.equal(frozen.catalog_mode, 'slice');
  assert.equal(frozen.source_and_local_gate_result, 'ACCEPTED_FOR_CANONICAL_PUBLICATION');
  assert.equal(frozen.N0_identity_acceptance, 'ACCEPTED');
  assert.match(frozen.N0_runtime_scope, /NOT_BROWSER_VERDICT/u);
  assert.equal(frozen.do_not_reopen_or_delay_for_later_role_outputs, true);
  assert.deepEqual(frozen.later_outputs_are_next_candidate_inputs_only, [F0, M0, A0]);
  assert.equal(frozen.snapshot.id, 'issue621-real-cebeafeee-20260903T201333Z');
  assert.match(frozen.snapshot.sha256, /^[0-9a-f]{64}$/u);
});

test('canonical Kaggle and public manifest identity are pinned without root mutation', () => {
  const { kaggle, artifact, publication } = receipt.frozen_transaction;
  assert.equal(kaggle.runner, 'scripts/run_static_site_builder_kaggle.py');
  assert.equal(kaggle.input_dataset, 'zigomaro/static-site-builder-input-20260903202018-f8d2db');
  assert.equal(kaggle.kernel, 'zigomaro/kenigevents-static-site-builder');
  assert.equal(kaggle.kernel_input_match, true);
  assert.equal(kaggle.status, 'COMPLETE');
  assert.equal(kaggle.semantic_provider_calls, 0);
  assert.match(artifact.result_sha256, /^[0-9a-f]{64}$/u);
  assert.match(artifact.archive_sha256, /^[0-9a-f]{64}$/u);
  assert.equal(publication.owner_http, 200);
  assert.equal(publication.manifest_http, 200);
  assert.equal(publication.manifest_repo_sha, FROZEN);
  assert.equal(publication.manifest_build_id, receipt.frozen_transaction.build_id);
  assert.equal(publication.manifest_base_path, `/${receipt.frozen_transaction.build_id}`);
  assert.equal(publication.manifest_data_mode, 'real');
  assert.deepEqual(publication.manifest_page_classes, ['all']);
  assert.equal(publication.root_mutation, false);
  assert.equal(publication.stable_ics_mutation, false);
});

test('all direct-GitHub ancestry anchors are explicit and A0 remains bounded', () => {
  const ancestry = receipt.ancestry_acceptance;
  assert.equal(ancestry.decision, 'ACCEPTED_DIRECT_GITHUB_COMPARE');
  assert.equal(ancestry.anchors.length, 10);
  assert.equal(new Set(ancestry.anchors.map(({ sha }) => sha)).size, 10);
  assert.ok(ancestry.anchors.every(({ sha, ahead_by }) => /^[0-9a-f]{40}$/u.test(sha) && ahead_by > 0));
  assert.deepEqual(ancestry.anchors.map(({ role }) => role), [
    'baseline',
    'programme',
    'N0',
    'F0_source',
    'F0_exhibitions_boundary',
    'M0_cutover',
    'M0_downstream',
    'M0_wave2',
    'FR0',
    'V0_harness',
  ]);
  assert.equal(ancestry.A0_integration_form, 'BOUNDED_NET_CONSUMER_PROJECTION_NOT_WHOLE_BRANCH');
  assert.deepEqual(ancestry.A0_projection_commits, [
    '041a391201bb9527f7e19d89c5f3fdc358358cd8',
    '3ae487cc4',
  ]);
});

test('F0 rejection is narrowed to vocabulary/checker and preserves accepted source slices', () => {
  const result = receipt.f0_result_classification;
  assert.equal(result.comment, 5531682890);
  assert.equal(result.delivery_confirmation_comment, 5531902859);
  assert.equal(result.frozen_transaction_verdict, 'ACCEPTED_WITH_NONBLOCKING_VOCABULARY_CHECKER_DELTA');
  assert.equal(result.festivals, 'ACCEPTED_SOURCE');
  assert.equal(result.exhibitions, 'ACCEPTED_SOURCE');
  assert.equal(result.fr0_batch_4, 'ACCEPTED_SOURCE');
  assert.equal(result.duplicate_f0_token_or_style_owner, 'NOT_FOUND');
  assert.equal(result.vocabulary_checker_correction.sha, F0);
  assert.equal(result.vocabulary_checker_correction.classification, 'THE_ONE_F0_NEXT_CANDIDATE_BATCH');
  assert.equal(result.vocabulary_checker_correction.product_consumer_change, false);
  assert.equal(result.vocabulary_checker_correction.blocks_frozen_publication, false);
  assert.equal(result.vocabulary_checker_correction.whole_branch_merge_allowed, false);
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
  assert.equal(m0.FR0_delivery_review_comment, 5531918584);
  assert.equal(m0.published_merge_ready_head, '7e879210f68f7a7e4a53755f2e9431c61f445569');
  assert.equal(m0.current_head, M0);
  assert.equal(m0.current_is_descendant_of_published_merge_ready_head, true);
  assert.equal(m0.tail_commit_count, 6);
  assert.equal(m0.tail_changed_product_source, false);
  assert.deepEqual(m0.tail_changed_paths, [
    'site/src/data/m0-downstream-bindings.v1.json',
    'site/tests/m0-card-icon-role-contract.test.mjs',
    'site/tests/event-card-control-target.test.mjs',
    'site/tests/m0-post-fr0-test-ownership.test.mjs',
    'site/tests/m0-post-fr0-test-ownership.v1.json',
  ]);
  assert.equal(m0.FR0_card_source_review, 'ACCEPTED_SOURCE');
  assert.equal(m0.decision, 'CURRENT_HEAD_ACCEPTED_AS_THE_ONE_M0_NEXT_SUCCESSOR_BATCH');

  assert.equal(receipt.current_pull_integrator_refs.FR0.head, FR0);
  assert.equal(receipt.next_successor_intake.FR0_source_batch, null);
  assert.equal(receipt.next_successor_intake.FR0_admission_rule, 'ONLY_AFTER_FACTUAL_FR0_DRIFT');
});

test('mobile rail is isolated outside the current three-batch intake', () => {
  const unit = receipt.deferred_independent_unit;
  assert.equal(unit.id, 'MOBILE_LISTING_RAIL_RESOURCE_STATE_BINDING');
  assert.equal(unit.FR0_delivery_review_comment, 5531918584);
  assert.equal(unit.source_writer, 'A0_CONSUMER_LINE');
  assert.deepEqual(unit.affected_paths, [
    'site/src/components/listings/MobileListingRailRow.astro',
    'site/src/components/listings/MobileListingRailSurface.astro',
  ]);
  assert.equal(unit.classification, 'SMALLEST_OWNER_REJECTION_DEFERRED_OUTSIDE_CURRENT_THREE_BATCH_INTAKE');
  assert.equal(unit.blocks_frozen_cebeafee_publication, false);
  assert.equal(unit.blocks_F0_M0_A0_next_candidate_intake, false);
  assert.equal(unit.creates_FR0_source_batch, false);
  assert.equal(unit.may_enter_later_candidate_only_as_separately_accepted_A0_consumer_batch, true);
});

test('N0 trigger is exact and browser acceptance remains V0-owned', () => {
  const trigger = receipt.post_publication_v0_trigger;
  assert.equal(trigger.state, 'N0_READY_TO_PUBLISH_EXACT_TRIGGER');
  assert.equal(trigger.R0_trigger_comment, 5531986889);
  assert.equal(trigger.source_sha, FROZEN);
  assert.equal(trigger.public_url, receipt.frozen_transaction.public_url);
  assert.equal(trigger.manifest_url, receipt.frozen_transaction.manifest_url);
  assert.deepEqual(trigger.FR0_requirement_comments, [5531944339, 5531980502]);
  assert.equal(trigger.late_F0_M0_A0_credit_forbidden, true);
  assert.equal(trigger.R0_smoke_is_independent_V0_verdict, false);
  assert.equal(trigger.N0_must_not_issue_browser_PASS, true);

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

test('the receipt makes no premature V0, slice or successor claim', () => {
  for (const nonClaim of [
    'independent V0 verdict received',
    'vertical slice browser-accepted',
    'next successor integrated or tested',
    'next successor full-real preview published',
  ]) assert.ok(receipt.non_claims.includes(nonClaim), `missing non-claim: ${nonClaim}`);
});
