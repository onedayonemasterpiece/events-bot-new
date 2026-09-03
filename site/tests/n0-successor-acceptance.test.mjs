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
const F0_FROZEN = 'de92dabd4551e117ca1af1be7915ff223321cc32';
const M0_FROZEN = '4c83fc7769b1dec2d92469373e3b15154af437f4';
const A0_FROZEN = 'ec926580fa2cc003318006f4c1d671fc459ea26c';
const M0_INVALID = 'e620e69fd8fb4641415320beaa3ea9c1003beee8';
const A0_REJECTED_REMOVAL = '5e466d65bc2b71a814c26c063f90aa07709de08f';
const A0_RESTORE = '84399b51b77701be714fdc84429318a9a28f93fd';
const GOLDEN_SHA = '84504f30eebc334deba46e94365601c3d572c5c0';
const V0_COMMENT = 5527892153;
const A0_BATCH_COMMENT = 5527907602;

const gate = (id) => manifest.gate_graph.find((item) => item.id === id);

test('N0 authority separates programme, precursor, source successor and runtime evidence', () => {
  assert.equal(manifest.schema_version, 'kenigevents.n0-successor-acceptance.v1');
  assert.equal(manifest.contract_version, '1.9.0');
  assert.equal(manifest.pm0_version, '2.3.0');
  assert.equal(manifest.owner, 'N0');
  assert.equal(manifest.evidence_cutoff.issue_comment, A0_BATCH_COMMENT);

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

test('published Golden transaction remains exact Golden evidence and now carries independent DRIFT', () => {
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
  assert.equal(golden.verification.v0_independent_verdict, 'DRIFT');
  assert.equal(golden.verification.v0_issue_comment, V0_COMMENT);
  assert.equal(golden.checklist_evidence['4'], 'DONE_FULL_GOLDEN_KAGGLE_REVIEW_PREVIEW');
  assert.equal(golden.checklist_evidence['9'], 'PARTIAL_PUBLISHED_STRESS_MATRIX_V0_DRIFT');

  for (const boundary of [
    'PM0 item 3 fresh-real full Kaggle Review Preview',
    'voice-review readiness gate',
    'V0 visual PASS',
    'ASTRO_NORMALIZATION_PASS',
    'thin S or Penpot equality',
  ]) assert.ok(golden.does_not_close.includes(boundary), `missing non-claim: ${boundary}`);
});

test('unpublished exact-SHA Kaggle retry remains diagnostic-only', () => {
  const diagnostic = manifest.pipeline_diagnostics;
  assert.equal(diagnostic.exact_sha, PIPELINE_PARENT);
  assert.equal(diagnostic.issue_comment, 5527345279);
  assert.equal(diagnostic.status, 'COMPLETE');
  assert.equal(diagnostic.publication, 'INTENTIONALLY_NOT_PUBLISHED');
  assert.equal(diagnostic.service_share.status, 'skipped');
  assert.equal(diagnostic.service_share.reason, 'golden_preview_frozen_clock');
  assert.equal(diagnostic.accepted_as, 'PIPELINE_SOURCE_AND_FROZEN_CLOCK_DIAGNOSTIC_ONLY');
  assert.notEqual(diagnostic.exact_sha, manifest.accepted_golden_transaction.repo_sha);
});

test('N0 classifies the three Golden V0 findings without manufacturing source work', () => {
  const verdict = manifest.v0_golden_verdict;
  assert.equal(verdict.issue_comment, V0_COMMENT);
  assert.equal(verdict.verdict, 'DRIFT');
  assert.equal(verdict.release_acceptance, false);
  assert.equal(verdict.target.repo_sha, GOLDEN_SHA);
  assert.deepEqual(verdict.matrix.viewports, [375, 620, 1024, 1440]);
  assert.equal(verdict.matrix.document_http_200, 40);
  assert.equal(verdict.matrix.document_total, 40);
  assert.equal(verdict.matrix.free_collection_visible_cards_at_375, 6);
  assert.equal(verdict.matrix.free_collection_horizontal_overflow_observed, false);

  const target = verdict.findings.event_card_auxiliary_target_height;
  assert.equal(target.classification, 'ACCEPTED_PRODUCT_DRIFT');
  assert.equal(target.owner, 'A0');
  assert.equal(target.source_path, 'site/src/layouts/EventLayout.astro');
  assert.equal(target.observed_height_px, 36.28);
  assert.equal(target.minimum_height_px, 44);
  assert.equal(target.blocks_first_real_candidate, true);

  const anchors = verdict.findings.stable_dom_anchors;
  assert.equal(anchors.classification, 'REJECTED_AS_V0_SELECTOR_CONTRACT_DRIFT');
  assert.equal(anchors.source_change_required, false);
  assert.deepEqual(anchors.canonical_identity, [
    'data-ds-family',
    'data-ds-version',
    'data-ds-variant',
    'data-ds-state',
  ]);
  assert.match(anchors.forbidden_resolution, /data-ui/u);

  const blank = verdict.findings.target_blank;
  assert.equal(blank.classification, 'REJECTED_AS_OVERBROAD_V0_NEGATIVE_GATE');
  assert.equal(blank.source_change_required, false);
  assert.deepEqual(blank.required_rel_tokens, ['noopener', 'noreferrer']);
  assert.deepEqual(blank.replacement_negative_selectors, [
    '[target="_blank"]:not([rel~="noopener"])',
    '[target="_blank"]:not([rel~="noreferrer"])',
  ]);
});

test('F0 and M0 frozen inputs are source-accepted without false runtime completion', () => {
  const f0 = manifest.role_outputs.F0;
  assert.equal(f0.latest_reviewed_head, F0_FROZEN);
  assert.equal(f0.decision, 'SOURCE_ACCEPTED_NOT_END_TO_END_COMPLETE');
  assert.equal(f0.accepted_functional_commits.at(-1), F0_FROZEN);
  assert.equal(f0.typography_authority.owner_approval_claimed, false);
  assert.equal(f0.typography_authority.font_binary_authority, false);
  assert.equal(f0.typography_authority.pm0_item_11, 'PARTIAL');
  assert.equal(f0.runtime_status, 'NOT_RUN_ON_CURRENT_SUCCESSOR');

  const m0 = manifest.role_outputs.M0;
  assert.equal(m0.latest_reviewed_head, M0_FROZEN);
  assert.equal(m0.invalid_result_sha_rejected, M0_INVALID);
  assert.equal(m0.integrated_parent, M0_INTEGRATED_PARENT);
  assert.equal(m0.integrated_by, SUCCESSOR_BASE);
  assert.equal(m0.decision, 'SOURCE_ACCEPTED_TIP_PARTIALLY_INTEGRATED');
  assert.equal(m0.resolved_drift.code, 'ADAPTIVE_SOURCE_COUNT_ORDER_CARDINALITY');
  assert.equal(m0.resolved_drift.diagnostics_owner, 'AdaptiveEventCardGrid');
  assert.equal(m0.resolved_drift.diagnostics_contract, 'input-source-rendered-v1');
  assert.equal(m0.accepted_pending_integration_commits.length, 18);
  assert.equal(m0.accepted_pending_integration_commits.at(-1), M0_FROZEN);
  assert.ok(m0.accepted_scope.includes('canonical EventCard, ListingEventCard and MobileListingRailRow icon roles'));
  assert.equal(m0.runtime_status, 'NOT_RUN_ON_FULL_M0_TIP');
});

test('A0 net consumer diff and mechanical batch are accepted while whole-branch replay is forbidden', () => {
  const a0 = manifest.role_outputs.A0;
  assert.equal(a0.latest_reviewed_head, A0_FROZEN);
  assert.equal(a0.decision, 'NET_CONSUMER_DIFF_AND_MECHANICAL_BATCH_ACCEPTED_WHOLE_BRANCH_MERGE_FORBIDDEN');
  assert.equal(a0.standalone_build_target, false);
  assert.equal(a0.product_removal_rejected_commit, A0_REJECTED_REMOVAL);
  assert.equal(a0.product_behavior_restore_commit, A0_RESTORE);
  assert.equal(a0.popular_density_decision, 'RESOLVED_PRESERVED_AND_BOUND_TO_VISIBLE_REPRESENTATIONS');

  const batch = a0.materialization_batch;
  assert.equal(batch.issue_comment, A0_BATCH_COMMENT);
  assert.equal(batch.status, 'N0_ACCEPTED_WITH_44PX_AMENDMENT');
  assert.deepEqual(batch.frozen_dependencies, {
    F0: F0_FROZEN,
    M0: M0_FROZEN,
    A0: A0_FROZEN,
  });
  assert.equal(Object.keys(batch.clusters).length, 5);
  assert.ok(batch.exclude_m0_roots.includes('site/src/components/AdaptiveEventCardGrid.astro'));
  assert.ok(batch.exclude_m0_roots.includes('site/src/components/OptimizedEventCardGrid.astro'));
  assert.ok(batch.n0_amendments.some((item) => item.includes('computed target is at least 44px')));
  assert.ok(batch.n0_amendments.some((item) => item.includes('do not add data-ui aliases')));
  assert.ok(batch.n0_amendments.some((item) => item.includes('rel noopener and noreferrer')));
  assert.equal(batch.semantic_decisions_complete, true);
  assert.equal(batch.source_checkpoint_tests_executed_by_A0, false);
  assert.equal(batch.candidate_integrated, false);
  assert.equal(batch.browser_verdict_claimed, false);
});

test('nearest full-real candidate is exact and blocked by one real source fix plus integration', () => {
  const candidate = manifest.candidate_plan.nearest_full_real_candidate;
  assert.equal(candidate.base, SUCCESSOR_BASE);
  assert.equal(candidate.status, 'BLOCKED_BY_V0_TARGET_HEIGHT_FIX_AND_PARTIAL_INTEGRATION');
  assert.equal(candidate.include.length, 4);
  assert.match(candidate.include[0].selection, /CURRENT_REMOTE_HEAD/u);
  assert.match(candidate.include[1].selection, new RegExp(F0_FROZEN, 'u'));
  assert.match(candidate.include[2].selection, new RegExp(M0_FROZEN, 'u'));
  assert.match(candidate.include[3].selection, new RegExp(A0_FROZEN, 'u'));
  assert.equal(candidate.blocking_source_fixes.length, 1);
  assert.deepEqual(candidate.blocking_source_fixes[0], {
    code: 'EVENT_CARD_AUXILIARY_TARGET_HEIGHT_BELOW_44PX',
    owner: 'A0',
    path: 'site/src/layouts/EventLayout.astro',
    required: 'specific not-interested target min-height absent or >=44px',
    verification: 'N0_REQUIRE_V0_GOLDEN_DRIFT_FIXED=1 npm run test:n0-v0-golden-drift',
  });
  assert.ok(candidate.preserve.includes('Popular Large/Compact user-facing behavior'));
  assert.ok(candidate.reject.includes('whole-current-A0-tip merge'));
  assert.ok(candidate.reject.includes('A0 replay of M0-owned roots'));
  assert.ok(candidate.reject.includes('parallel data-ui identity aliases'));
  assert.ok(candidate.reject.includes('blanket target=_blank ban'));
  assert.ok(candidate.reject.some((item) => item.includes(M0_INVALID)));
});

test('gate graph orders source, real Kaggle, V0, Astro, thin-S, Penpot and release boundaries', () => {
  assert.deepEqual(manifest.gate_graph.map((item) => item.id), [
    'CURRENT_SOURCE_CANDIDATE',
    'FULL_REAL_KAGGLE_REVIEW_PREVIEW',
    'V0_REAL_DOM_COMPUTED_STYLE_AUDIT',
    'ASTRO_NORMALIZATION_PASS',
    'THIN_S_BINDING_READY',
    'PENPOT_NATIVE_MATERIALIZATION_READY',
    'RELEASE_CANDIDATE_READY',
  ]);
  assert.equal(gate('CURRENT_SOURCE_CANDIDATE').status, 'BLOCKED_BY_ONE_A0_SOURCE_FIX_AND_INTEGRATION');
  assert.equal(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').status, 'BLOCKED_BY_CURRENT_SOURCE_CANDIDATE');
  assert.ok(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').requires.includes('--preview-data-mode real'));
  assert.ok(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').requires.includes('--page-class all'));
  assert.equal(gate('V0_REAL_DOM_COMPUTED_STYLE_AUDIT').owner, 'V0');
  assert.equal(gate('THIN_S_BINDING_READY').status, 'BLOCKED_BY_ASTRO_NORMALIZATION_PASS');
  assert.equal(gate('PENPOT_NATIVE_MATERIALIZATION_READY').owner, 'R0.PENPOT');
  assert.equal(gate('RELEASE_CANDIDATE_READY').status, 'BLOCKED_BY_GOLDEN_A_S_P_PASS');
});

test('PM0 delta mode observes live branches without inflating readiness', () => {
  const delta = manifest.pm0_delta_reporting;
  assert.equal(delta.owner_correction_comment, 5527668536);
  assert.equal(delta.effective_immediately, true);
  assert.equal(delta.current_spec_version, '2.3.0');
  assert.deepEqual(delta.required_output_blocks, [
    'checkbox_transitions',
    'progress_inside_partial',
    'not_in_candidate',
    'new_owner_visible_result',
  ]);
  assert.match(delta.zero_changes_allowed_only_when, /all relevant heads/u);
  assert.match(delta.v0_recheck_claim_requires, /new factual V0 browser verdict/u);
  assert.equal(delta.branch_progress_may_change_partial_detail_without_changing_symbol, true);
  assert.equal(delta.branch_progress_may_not_create_done_without_full_item_acceptance, true);
});

test('V0 trigger is DRIFT, not an N0 browser PASS', () => {
  const golden = manifest.v0_triggers.golden;
  assert.equal(golden.status, 'AUDITED_DRIFT');
  assert.equal(golden.url, 'https://kenigevents.ru/preview-golden-84504f30-20270604-v1/__preview/');
  assert.equal(golden.latest_v0_comment, V0_COMMENT);
  assert.equal(golden.v0_document_loads, 40);
  assert.equal(golden.v0_pass, false);
  assert.equal(manifest.v0_triggers.fresh_real.status, 'PENDING');
});

test('N0 source exposes no local full-preview publication path', () => {
  assert.equal(packageJson.scripts['deploy:preview'], undefined);
  assert.equal(packageJson.scripts['deploy:golden-preview'], undefined);
  assert.equal(packageJson.scripts['test:golden-deploy-contract'], undefined);
  assert.equal(packageJson.scripts['test:n0-v0-golden-drift'], 'node --test tests/n0-v0-golden-drift-acceptance.test.mjs');
  assert.equal(existsSync(join(siteDir, 'scripts', 'deploy-golden-preview.mjs')), false);
  assert.equal(existsSync(join(siteDir, 'tests', 'golden-review-deploy-contract.test.mjs')), false);
  assert.match(goldenContract, /Contract: `kenigevents\.launch-normalized-ui\.v1@1\.9\.0`/u);
  assert.match(goldenContract, /same Kaggle runner/u);
  assert.match(goldenContract, /Local diagnostic/u);
  assert.doesNotMatch(goldenContract, /npm run deploy:golden-preview/u);
  assert.ok(manifest.prohibitions.includes('do not use deploy:preview or deploy:golden-preview as a launch path'));
  assert.ok(manifest.prohibitions.includes('do not add a parallel data-ui identity system'));
  assert.ok(manifest.prohibitions.includes('do not treat safe external target=_blank links as product drift'));
});
