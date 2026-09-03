import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const testsDir = dirname(fileURLToPath(import.meta.url));
const siteDir = resolve(testsDir, '..');
const manifest = JSON.parse(readFileSync(join(siteDir, 'scripts', 'n0-successor-acceptance.v1.json'), 'utf8'));
const packageJson = JSON.parse(readFileSync(join(siteDir, 'package.json'), 'utf8'));
const goldenContract = readFileSync(
  resolve(siteDir, '..', 'docs', 'features', 'static-site-pages', 'design-system', 'golden-review-preview-v1.md'),
  'utf8',
);

const PRECURSOR = '0d73428dfafff2fd5450b74fd68e7bb40e92d2c5';
const SUCCESSOR_BASE = '9152994b026b34d60d21d68bfa2a4d7d8dc20f3e';
const PIPELINE_PARENT = '0d92654b9637e31753fed5bd4bf6a4a66763c079';
const M0_INTEGRATED_PARENT = '1d145d5efd2a332eff29e69b6afcf43414769906';
const M0_HEAD = '0bf25ff6c17e1b649b1a64919502103ba60b0d43';
const M0_INVALID = 'e620e69fd8fb4641415320beaa3ea9c1003beee8';
const F0_HEAD = '77c0833a7f5d2542bcfb8313a3353f42b75233dd';
const A0_HEAD = '8f96c659cd710091fba61eb65cb4846cccdd2c38';
const A0_REJECTED_REMOVAL = '5e466d65bc2b71a814c26c063f90aa07709de08f';
const A0_RESTORE = '84399b51b77701be714fdc84429318a9a28f93fd';
const GOLDEN_SHA = '84504f30eebc334deba46e94365601c3d572c5c0';

const gate = (id) => manifest.gate_graph.find((item) => item.id === id);

test('N0 authority distinguishes programme, precursor, source successor and exact runtime evidence', () => {
  assert.equal(manifest.schema_version, 'kenigevents.n0-successor-acceptance.v1');
  assert.equal(manifest.contract_version, '1.9.0');
  assert.equal(manifest.pm0_version, '2.2.0');
  assert.equal(manifest.owner, 'N0');

  assert.equal(manifest.authority.legacy_combined_candidate.head, PRECURSOR);
  assert.equal(
    manifest.authority.legacy_combined_candidate.decision,
    'ACCEPTED_PRECURSOR_NOT_CURRENT_SUCCESSOR',
  );

  const successor = manifest.authority.current_integrated_successor_base;
  assert.equal(successor.head, SUCCESSOR_BASE);
  assert.deepEqual(successor.parents, [PIPELINE_PARENT, M0_INTEGRATED_PARENT]);
  assert.equal(successor.decision, 'SOURCE_ACCEPTED_UNBUILT_SUCCESSOR_BASE');
  assert.equal(successor.runtime_acceptance, false);
  assert.equal(successor.public_url, null);
  assert.ok(successor.included_scope.includes('one canonical real/golden Kaggle pipeline'));
  assert.ok(successor.included_scope.includes('coherent AdaptiveEventCardGrid input/source/rendered diagnostics'));

  assert.equal(manifest.authority.n0_branch.resolve_remote_head_at_merge, true);
  assert.equal(
    manifest.authority.n0_branch.decision,
    'SOURCE_ACCEPTED_PENDING_R0_INTEGRATION_AND_EXECUTABLE_TESTS',
  );
});

test('published Golden transaction closes only its exact Golden evidence class', () => {
  const golden = manifest.accepted_golden_transaction;
  assert.equal(golden.decision, 'ACCEPTED_AS_GOLDEN_KAGGLE_PIPELINE_AND_CORPUS_EVIDENCE_ONLY');
  assert.equal(golden.issue_comment, 5527249164);
  assert.equal(golden.repo_sha, GOLDEN_SHA);
  assert.equal(golden.build_id, 'preview-golden-84504f30-20270604-v1');
  assert.equal(golden.url, 'https://kenigevents.ru/preview-golden-84504f30-20270604-v1/__preview/');
  assert.equal(golden.data_mode, 'golden');
  assert.deepEqual(golden.page_classes, ['all']);
  assert.equal(golden.kaggle.status, 'COMPLETE');
  assert.equal(golden.artifact.root_mutation, false);
  assert.equal(golden.artifact.stable_ics_mutation, false);
  assert.equal(golden.verification.document_http_200, 36);
  assert.equal(golden.verification.document_horizontal_overflow_cases, 0);
  assert.equal(golden.verification.v0_independent_verdict, 'PENDING');
  assert.equal(golden.checklist_evidence['4'], 'DONE_FULL_GOLDEN_KAGGLE_REVIEW_PREVIEW');
  assert.equal(golden.checklist_evidence['9'], 'PARTIAL_PUBLISHED_STRESS_MATRIX_V0_VISUAL_VERDICT_PENDING');

  for (const boundary of [
    'PM0 item 3 fresh-real full Kaggle Review Preview',
    'voice-review readiness gate',
    'V0 visual PASS',
    'ASTRO_NORMALIZATION_PASS',
    'thin S or Penpot equality',
  ]) assert.ok(golden.does_not_close.includes(boundary), `missing non-claim: ${boundary}`);
});

test('unpublished exact-SHA Kaggle retry does not inherit the published Golden URL', () => {
  const diagnostic = manifest.pipeline_diagnostics;
  assert.equal(diagnostic.exact_sha, PIPELINE_PARENT);
  assert.equal(diagnostic.issue_comment, 5527345279);
  assert.equal(diagnostic.status, 'COMPLETE');
  assert.equal(diagnostic.build_id, 'preview-golden-0d92654b-20270604-clock-v1');
  assert.equal(diagnostic.publication, 'INTENTIONALLY_NOT_PUBLISHED');
  assert.equal(diagnostic.service_share.status, 'skipped');
  assert.equal(diagnostic.service_share.reason, 'golden_preview_frozen_clock');
  assert.equal(diagnostic.accepted_as, 'PIPELINE_SOURCE_AND_FROZEN_CLOCK_DIAGNOSTIC_ONLY');
  assert.notEqual(diagnostic.exact_sha, manifest.accepted_golden_transaction.repo_sha);
});

test('F0 output is selectively accepted without overstating actual-consumer convergence', () => {
  const f0 = manifest.role_outputs.F0;
  assert.equal(f0.head, F0_HEAD);
  assert.equal(f0.decision, 'SELECTIVE_SOURCE_ACCEPTED_NOT_END_TO_END_COMPLETE');
  assert.deepEqual(f0.accepted_functional_commits, [
    'fd47202484da3abb2eff0fac70c6123a0cebba4b',
    '11de3f042de2bd9205e7a594f78813566d5be5d6',
    '2e95a34abd39317766abfe19dae8c6b9d4676da2',
    F0_HEAD,
  ]);
  assert.equal(f0.remaining_source_evidence.consumer_files_with_raw_lengths, 84);
  assert.ok(f0.remaining_source_evidence.direct_font_family_consumers.includes(
    'site/src/layouts/EventLayout.astro',
  ));
  assert.ok(f0.remaining_source_evidence.transitional_local_icon_alias_consumers.includes(
    'site/src/components/InterestClubCard.astro',
  ));
  assert.equal(f0.runtime_status, 'NOT_RUN_ON_CURRENT_SUCCESSOR');
});

test('M0 cardinality drift is resolved and four safety commits remain explicit integration inputs', () => {
  const m0 = manifest.role_outputs.M0;
  assert.equal(m0.head, M0_HEAD);
  assert.equal(m0.invalid_result_sha_rejected, M0_INVALID);
  assert.equal(m0.integrated_parent, M0_INTEGRATED_PARENT);
  assert.equal(m0.integrated_by, SUCCESSOR_BASE);
  assert.equal(m0.decision, 'SOURCE_ACCEPTED_TIP_PARTIALLY_INTEGRATED');
  assert.equal(m0.resolved_drift.code, 'ADAPTIVE_SOURCE_COUNT_ORDER_CARDINALITY');
  assert.equal(m0.resolved_drift.diagnostics_owner, 'AdaptiveEventCardGrid');
  assert.equal(m0.resolved_drift.diagnostics_contract, 'input-source-rendered-v1');
  assert.deepEqual(m0.accepted_pending_integration_commits, [
    'da089b95817ed3a3a6c0c5b37adfd16a686235fb',
    '07bc9c21fc67e24bd26a9268c0df6e459aff004e',
    '1401b21d9ec68fe38f879de86028f468b515a8d0',
    M0_HEAD,
  ]);
  assert.ok(m0.pending_scope.includes('fail-closed ListingEventCard broken-media fallback state'));
  assert.equal(m0.runtime_status, 'NOT_RUN_ON_FULL_M0_TIP');
});

test('A0 whole-tip merge remains held while accepted user-facing behavior is preserved', () => {
  const a0 = manifest.role_outputs.A0;
  assert.equal(a0.head, A0_HEAD);
  assert.equal(a0.decision, 'HOLD_WHOLESALE_TIP_FOR_A0_COMPLETION_OR_SELECTIVE_PATCH');
  assert.equal(a0.standalone_build_target, false);
  assert.equal(a0.route_family_fraction, '5/9');
  assert.equal(a0.product_removal_rejected_commit, A0_REJECTED_REMOVAL);
  assert.equal(a0.product_behavior_restore_commit, A0_RESTORE);
  assert.equal(a0.restore_decision, 'ACCEPTED_CORRECTION');
  assert.equal(a0.writer_blocked_paths.length, 5);
  assert.ok(a0.open_drift.some((item) => item.includes('hidden donor layouts')));
  for (const behavior of [
    'visible Large/Compact choice',
    'localStorage restoration',
    'keyboard radio behavior',
    'pinch behavior',
    'visible-event anchor preservation',
  ]) assert.ok(a0.must_preserve_without_owner_decision.includes(behavior));
});

test('nearest real candidate is exact, bounded and does not wait for unrelated A0 completion', () => {
  const candidate = manifest.candidate_plan.nearest_full_real_candidate;
  assert.equal(candidate.base, SUCCESSOR_BASE);
  assert.equal(candidate.status, 'PARTIAL_INTEGRATION_REQUIRED');
  assert.equal(candidate.include.length, 3);
  assert.equal(candidate.include[0].source, 'work/ui-normalization-n0-checklist-20260903');
  assert.equal(candidate.include[1].selection, 'POST_bbbc9b_SOURCE_PATHS_THROUGH_77c0833');
  assert.equal(candidate.include[2].selection, 'FOUR_COMMITS_AFTER_1d145d5_THROUGH_0bf25ff');
  assert.equal(candidate.hold_for_next_successor[0].source, 'work/ui-normalization-a0-wave-3-20260903');
  assert.ok(candidate.preserve.includes('Popular Large/Compact user-facing behavior'));
  assert.ok(candidate.reject.some((item) => item.includes(M0_INVALID)));
  assert.ok(candidate.reject.includes('whole-current-A0-tip merge'));
});

test('end-to-end gate graph keeps executable, browser, thin-S, Penpot and release boundaries ordered', () => {
  const ids = manifest.gate_graph.map((item) => item.id);
  assert.deepEqual(ids, [
    'CURRENT_SOURCE_CANDIDATE',
    'FULL_REAL_KAGGLE_REVIEW_PREVIEW',
    'V0_REAL_DOM_COMPUTED_STYLE_AUDIT',
    'ASTRO_NORMALIZATION_PASS',
    'THIN_S_BINDING_READY',
    'PENPOT_NATIVE_MATERIALIZATION_READY',
    'RELEASE_CANDIDATE_READY',
  ]);
  assert.equal(gate('CURRENT_SOURCE_CANDIDATE').status, 'PARTIAL');
  assert.equal(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').status, 'BLOCKED_BY_CURRENT_SOURCE_CANDIDATE');
  assert.ok(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').requires.includes('--preview-data-mode real'));
  assert.ok(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').requires.includes('--page-class all'));
  assert.equal(gate('V0_REAL_DOM_COMPUTED_STYLE_AUDIT').owner, 'V0');
  assert.equal(gate('THIN_S_BINDING_READY').status, 'BLOCKED_BY_ASTRO_NORMALIZATION_PASS');
  assert.equal(gate('PENPOT_NATIVE_MATERIALIZATION_READY').owner, 'R0.PENPOT');
  assert.equal(gate('RELEASE_CANDIDATE_READY').status, 'BLOCKED_BY_GOLDEN_A_S_P_PASS');
});

test('V0 trigger is published without an N0 browser claim', () => {
  const golden = manifest.v0_triggers.golden;
  assert.equal(golden.status, 'READY_REQUIRES_PROPER_BROWSER_ACTIVATION');
  assert.equal(golden.url, 'https://kenigevents.ru/preview-golden-84504f30-20270604-v1/__preview/');
  assert.equal(golden.latest_v0_comment, 5527356024);
  assert.equal(golden.v0_observations, 0);
  assert.equal(golden.v0_pass, false);
  assert.equal(manifest.v0_triggers.fresh_real.status, 'PENDING');
});

test('N0 source exposes no local full-preview publication path', () => {
  assert.equal(packageJson.scripts['deploy:preview'], undefined);
  assert.equal(packageJson.scripts['deploy:golden-preview'], undefined);
  assert.equal(packageJson.scripts['test:golden-deploy-contract'], undefined);
  assert.equal(existsSync(join(siteDir, 'scripts', 'deploy-golden-preview.mjs')), false);
  assert.equal(existsSync(join(siteDir, 'tests', 'golden-review-deploy-contract.test.mjs')), false);
  assert.match(goldenContract, /Contract: `kenigevents\.launch-normalized-ui\.v1@1\.9\.0`/u);
  assert.match(goldenContract, /same Kaggle runner/u);
  assert.match(goldenContract, /Local diagnostic/u);
  assert.doesNotMatch(goldenContract, /npm run deploy:golden-preview/u);
  assert.ok(manifest.prohibitions.includes('do not use deploy:preview or deploy:golden-preview as a launch path'));
  assert.ok(manifest.prohibitions.includes(
    'do not open thin-S or Penpot materialization before accepted executable Astro and required V0 evidence',
  ));
});
