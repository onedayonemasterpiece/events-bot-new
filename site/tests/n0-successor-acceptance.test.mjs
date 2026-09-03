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

const PROGRAMME = 'ce59cc2fda4e61c8af37f980a5e440c093bd2da8';
const PRECURSOR = '0d73428dfafff2fd5450b74fd68e7bb40e92d2c5';
const PUBLISHED_REAL = '4536847f9fbdaa27326ebb3ec9ec1c825736e107';
const SOURCE_PARENT = 'd0ad17088e13d5dab3d992326f393e2616246c5d';
const SOURCE_SUCCESSOR = '2e8f4dd2393ce0c5100f8b610aae3f01380aad8c';
const F0_INTEGRATED = 'de92dabd4551e117ca1af1be7915ff223321cc32';
const F0_CURRENT = 'f2b9927e25ac0566d35577f69c574cedadab27d7';
const M0_INTEGRATED = 'c808c75dd975a9851e148ccf993c32787d2b6886';
const M0_SOURCE = 'c71351decdcee02941acb26c5e2fbaf88faf0378';
const M0_CURRENT = 'a6cb9d454cdac3c4165c3715f1e747ef6a59fe3c';
const A0_CURRENT = '61d340a3ca291f074289d2292b33f56b2bad8a22';
const SOT_CURRENT = '77f72cf71ee52c10598c2129dae1d961507825a4';
const GOLDEN_SHA = '84504f30eebc334deba46e94365601c3d572c5c0';

const gate = (id) => manifest.gate_graph.find((item) => item.id === id);
const delta = (id) => manifest.source_successor_review.required_current_deltas.find((item) => item.id === id);

test('N0 authority distinguishes programme, published baseline and current source successor', () => {
  assert.equal(manifest.schema_version, 'kenigevents.n0-successor-acceptance.v1');
  assert.equal(manifest.contract_version, '1.9.0');
  assert.equal(manifest.pm0_version, '2.3.0');
  assert.equal(manifest.owner, 'N0');
  assert.equal(manifest.current_refs.programme.head, PROGRAMME);
  assert.equal(manifest.current_refs.legacy_combined_candidate.head, PRECURSOR);
  assert.equal(manifest.current_refs.published_fresh_real_source.head, PUBLISHED_REAL);
  assert.equal(manifest.current_refs.source_successor.head, SOURCE_SUCCESSOR);
  assert.equal(manifest.current_refs.source_successor.accepted_source_parent, SOURCE_PARENT);
  assert.equal(manifest.current_refs.source_successor.runtime_acceptance, false);
  assert.equal(manifest.current_refs.source_successor.public_url, null);
});

test('fresh-real 453 transaction closes PM0 2 and 3 as pipeline/data baseline only', () => {
  const real = manifest.accepted_fresh_real_transaction;
  assert.equal(real.decision, 'ACCEPTED_AS_PM0_3_FULL_REAL_PIPELINE_AND_FRESH_DATA_BASELINE');
  assert.equal(real.issue_comment, 5528274698);
  assert.equal(real.repo_sha, PUBLISHED_REAL);
  assert.equal(real.url, 'https://kenigevents.ru/preview-real-4536847f-fresh-20260903-v1/__preview/');
  assert.equal(real.manifest_url, 'https://kenigevents.ru/preview-real-4536847f-fresh-20260903-v1/preview-build.json');
  assert.deepEqual(real.explicit_ancestry, [PROGRAMME, '1d145d5efd2a332eff29e69b6afcf43414769906', PUBLISHED_REAL]);
  assert.equal(real.data_mode, 'real');
  assert.deepEqual(real.page_classes, ['all']);
  assert.equal(real.snapshot.id, 'issue621-real-9152994b-20260903T144827Z');
  assert.equal(real.snapshot.sha256, '1a084d7e321771e5acb7681d61589962c8d48cc68f37d6c7799180bf13573b5e');
  assert.equal(real.build.event_count, 300);
  assert.equal(real.kaggle.status, 'COMPLETE');
  assert.equal(real.artifact.root_mutation, false);
  assert.equal(real.artifact.stable_ics_mutation, false);
  assert.equal(real.technical_verification.owner_url_http, 200);
  assert.equal(real.technical_verification.r0_document_http_200, '126/126');
  assert.equal(real.technical_verification.independent_v0_verdict, 'PENDING');
  assert.equal(real.pm0['2'], 'DONE');
  assert.equal(real.pm0['3'], 'DONE');
  assert.ok(real.does_not_close.includes('runtime acceptance of the current source successor'));
  assert.ok(real.does_not_close.includes('voice-review readiness'));
  assert.ok(real.does_not_close.includes('independent V0 PASS'));
});

test('Golden remains deterministic evidence with an independent DRIFT verdict', () => {
  const golden = manifest.accepted_golden_transaction;
  assert.equal(golden.repo_sha, GOLDEN_SHA);
  assert.equal(golden.data_mode, 'golden');
  assert.equal(golden.kaggle.status, 'COMPLETE');
  assert.equal(golden.verification.v0_independent_verdict, 'DRIFT');
  assert.equal(golden.verification.v0_issue_comment, 5527892153);
  assert.equal(golden.pm0['4'], 'DONE');
  assert.equal(golden.pm0['9'], 'PARTIAL_V0_DRIFT');

  const verdict = manifest.v0_golden_verdict;
  assert.equal(verdict.verdict, 'DRIFT');
  assert.equal(verdict.release_acceptance, false);
  assert.equal(verdict.target.repo_sha, GOLDEN_SHA);
  assert.equal(verdict.matrix.document_http_200, 40);
  assert.equal(verdict.matrix.document_total, 40);
  assert.equal(verdict.findings.event_card_auxiliary_target_height.current_source_status,
    'CLOSED_IN_D0AD1708_PENDING_V0_RECHECK');
  assert.equal(verdict.findings.event_card_auxiliary_target_height.current_source_blocker, false);
  assert.equal(verdict.findings.stable_dom_anchors.source_change_required, false);
  assert.equal(verdict.findings.target_blank.source_change_required, false);
});

test('source successor review accepts closures without inheriting runtime evidence', () => {
  const review = manifest.source_successor_review;
  assert.equal(review.candidate.sha, SOURCE_SUCCESSOR);
  assert.equal(review.candidate.accepted_source_parent, SOURCE_PARENT);
  assert.equal(review.candidate.decision, 'SOURCE_ACCEPTED_INTERMEDIATE_CURRENT_SUCCESSOR');
  assert.equal(review.candidate.runtime_tests_accepted, false);
  assert.equal(review.candidate.published_preview_inherited, false);
  assert.equal(review.included_lineage.F0, F0_INTEGRATED);
  assert.equal(review.included_lineage.M0, M0_INTEGRATED);
  assert.ok(review.accepted_source_closures.some((item) => item.includes('44px')));
  assert.ok(review.accepted_source_closures.some((item) => item.includes('overflow-x:auto')));
  assert.ok(review.accepted_source_closures.some((item) => item.includes('overflow-only probe')));
});

test('current F0 route-theme batch is required with exact consumer and strict boundaries', () => {
  assert.equal(manifest.current_refs.F0.integrated_head, F0_INTEGRATED);
  assert.equal(manifest.current_refs.F0.current_head, F0_CURRENT);
  assert.equal(manifest.role_acceptance.F0.current_head, F0_CURRENT);
  assert.equal(manifest.role_acceptance.F0.decision, 'CURRENT_BATCH_SOURCE_ACCEPTED_A0_CONSUMPTION_PENDING');

  const f0 = delta('F0-ROUTE-THEME-BINDINGS');
  assert.equal(f0.head, F0_CURRENT);
  assert.equal(f0.decision, 'ACCEPTED_REQUIRED_BEFORE_NEXT_FULL_BUILD');
  assert.equal(f0.clusters.length, 3);
  assert.ok(f0.clusters.some((item) => item.includes('--ex-*')));
  assert.ok(f0.clusters.some((item) => item.includes('sub-44px')));
  assert.ok(f0.clusters.some((item) => item.includes('text-arrow')));
  assert.equal(f0.strict_command,
    'F0_REQUIRE_ROUTE_THEME_CONSUMED=1 node site/src/components/design-system/check-f0-route-theme-bindings.mjs');
});

test('current M0 delta is accepted through EventHero MediaFrame without claiming execution', () => {
  assert.equal(manifest.current_refs.M0.integrated_head, M0_INTEGRATED);
  assert.equal(manifest.current_refs.M0.current_source_head, M0_SOURCE);
  assert.equal(manifest.current_refs.M0.current_head, M0_CURRENT);
  assert.equal(manifest.current_refs.M0.source_delta_commits.length, 5);
  assert.equal(manifest.current_refs.M0.source_delta_commits.at(-1), M0_SOURCE);
  assert.equal(manifest.current_refs.M0.downstream_contract_commit, M0_CURRENT);

  const m0 = delta('M0-CURRENT-FAMILY-DELTA');
  assert.equal(m0.source_head, M0_SOURCE);
  assert.equal(m0.head, M0_CURRENT);
  assert.equal(m0.commits.length, 6);
  assert.equal(m0.decision, 'SOURCE_AND_DOWNSTREAM_CONTRACTS_ACCEPTED_REQUIRED_BEFORE_NEXT_FULL_BUILD');
  assert.ok(m0.accepted_scope.includes('resolved rail media contradictions fail closed'));
  assert.ok(m0.accepted_scope.includes('responsive stack/progressive seam remains source-addressable'));
  assert.ok(m0.accepted_scope.some((item) => item.includes('non-interactive MediaFrame')));
  assert.equal(manifest.role_acceptance.M0.source_head, M0_SOURCE);
  assert.equal(manifest.role_acceptance.M0.current_head, M0_CURRENT);
  assert.equal(manifest.role_acceptance.M0.runtime_status, 'NOT_RUN_ON_CURRENT_SOURCE_HEAD');
  assert.ok(manifest.role_acceptance.M0.accepted.some((item) => item.includes('machine-readable family')));
});

test('fresh-real V0 blocker is accepted as a tool boundary, not a browser verdict', () => {
  const blocker = manifest.v0_platform_blocker;
  assert.equal(blocker.issue_comment, 5529063082);
  assert.equal(blocker.classification, 'ACTUAL_TOOL_SURFACE_BOUNDARY');
  assert.equal(blocker.target_repo_sha, PUBLISHED_REAL);
  assert.equal(blocker.verdict, 'NOT_EXECUTED');
  assert.equal(blocker.browser_pass_claimed, false);
  assert.equal(blocker.browser_drift_claimed, false);
  assert.equal(blocker.n0_decision, 'ACCEPTED_PLATFORM_BLOCKER_BASELINE_REMAINS_UNAUDITED');
  assert.match(blocker.remaining_trigger, /callable my-browser-bridge/u);

  const trigger = manifest.v0_triggers.published_fresh_real_baseline;
  assert.equal(trigger.status, 'READY_BUT_V0_BLOCKED_BY_TOOL_SURFACE');
  assert.equal(trigger.blocker_comment, 5529063082);
  assert.equal(trigger.browser_verdict, 'NOT_EXECUTED');
  assert.ok(manifest.prohibitions.includes('V0 platform blocker represented as browser PASS or DRIFT'));
});

test('A0 completion remains held at 17/19 until route themes and MECH-06 are materialized', () => {
  const a0 = manifest.role_acceptance.A0;
  assert.equal(manifest.current_refs.A0.current_head, A0_CURRENT);
  assert.equal(manifest.current_refs.A0.whole_branch_merge_allowed, false);
  assert.equal(a0.source_identity_fraction_on_current_successor, '17/19');
  assert.deepEqual(a0.missing_source_identities, [
    'FocusEggCollectionRouteComposition',
    'ClosedFocusHubRouteComposition',
  ]);
  assert.deepEqual(a0.route_theme_consumption_pending, [
    'exhibitions',
    'festivals',
    'interest-club detail',
  ]);
  assert.equal(a0.whole_branch_merge_allowed, false);
  assert.equal(a0.browser_pass, false);

  const focus = delta('A0-MECH-06-FOCUS-ROUTE-IDENTITIES');
  assert.deepEqual(focus.required_families, [
    'FocusEggCollectionRouteComposition',
    'ClosedFocusHubRouteComposition',
  ]);
  assert.deepEqual(focus.required_states, ['found-N-of-M', 'checking', 'locked', 'available']);
});

test('thin-S and Penpot preparation remain non-inflating', () => {
  const downstream = manifest.downstream_bindings;
  assert.equal(downstream.thin_s.head, SOT_CURRENT);
  assert.equal(downstream.thin_s.source_identity, '17/19_CURRENT_SUCCESSOR');
  assert.equal(downstream.thin_s.current_build_verified, '0/19');
  assert.equal(downstream.thin_s.browser_observed, '0/19');
  assert.equal(downstream.penpot.declared_status, 'SPEC_ONLY_NOT_MATERIALIZED');
  assert.equal(downstream.penpot.classification, 'PM0_39_NOT_DONE');
  assert.equal(downstream.v0_matrix.current_successor_build_verified, '0/19');
  assert.equal(downstream.v0_matrix.browser_observed_current_successor, '0/19');
  assert.equal(downstream.rollback.production_mutation, false);
});

test('nearest candidate requires current F0, M0, route identities and exact execution', () => {
  const candidate = manifest.nearest_full_real_candidate;
  assert.equal(candidate.base, SOURCE_SUCCESSOR);
  assert.equal(candidate.status,
    'BLOCKED_BY_F0_CONSUMPTION_M0_DELTA_A0_MECH_06_AND_EXECUTABLE_TESTS');
  assert.ok(candidate.include.some((item) => item.includes(F0_CURRENT)));
  assert.ok(candidate.include.some((item) => item.includes(M0_CURRENT)));
  assert.deepEqual(candidate.blocking_source_fixes.map((item) => item.code), [
    'F0_ROUTE_THEME_CONSUMPTION_PENDING',
    'A0_FOCUS_ROUTE_IDENTITIES_MISSING',
  ]);
  assert.ok(candidate.blocking_integration.some((item) => item.includes(F0_CURRENT)));
  assert.ok(candidate.blocking_integration.some((item) => item.includes(M0_CURRENT)));
  assert.ok(candidate.preserve.includes('Popular Large/Compact behavior'));
  assert.ok(candidate.reject.includes('whole A0 branch merge'));
  assert.ok(candidate.reject.includes('runtime inheritance from an earlier SHA'));
  assert.ok(candidate.reject.includes('treating 4536847f V0 evidence as current-successor PASS'));
});

test('gate graph preserves pipeline, V0, Astro, thin-S, Penpot and release boundaries', () => {
  assert.equal(gate('SOURCE_CANDIDATE').status,
    'BLOCKED_BY_F0_CONSUMPTION_M0_DELTA_A0_MECH_06_AND_TESTS');
  assert.equal(gate('FULL_REAL_KAGGLE_PREVIEW').status,
    'PM0_3_BASELINE_DONE_CURRENT_SUCCESSOR_REBUILD_PENDING');
  assert.equal(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').alias_of, 'FULL_REAL_KAGGLE_PREVIEW');
  assert.equal(gate('V0_REAL_AUDIT').owner, 'V0');
  assert.equal(gate('V0_REAL_AUDIT').status, 'BASELINE_4536847F_READY_CURRENT_SUCCESSOR_PENDING');
  assert.equal(gate('ASTRO_NORMALIZATION_PASS').owner, 'N0');
  assert.equal(gate('THIN_S_BOUND').status, 'BLOCKED_BY_ASTRO_NORMALIZATION_PASS');
  assert.equal(gate('PENPOT_NATIVE').owner, 'R0.PENPOT');
  assert.equal(gate('RELEASE_CANDIDATE').status, 'BLOCKED_BY_GOLDEN_A_S_P_PASS');
});

test('one-pipeline and negative acceptance rules remain executable', () => {
  assert.equal(packageJson.scripts['deploy:preview'], undefined);
  assert.equal(packageJson.scripts['deploy:golden-preview'], undefined);
  assert.equal(packageJson.scripts['test:golden-deploy-contract'], undefined);
  assert.equal(existsSync(join(siteDir, 'scripts', 'deploy-golden-preview.mjs')), false);
  assert.equal(existsSync(join(siteDir, 'tests', 'golden-review-deploy-contract.test.mjs')), false);
  assert.match(goldenContract, /one existing `events-bot-new` Kaggle `StaticSiteBuilder`/u);
  assert.match(goldenContract, /Local diagnostic/u);
  assert.doesNotMatch(goldenContract, /npm run deploy:golden-preview/u);
  assert.ok(manifest.prohibitions.includes('whole A0 branch merge'));
  assert.ok(manifest.prohibitions.includes('parallel data-ui identity system'));
  assert.ok(manifest.prohibitions.includes('treating safe target=_blank links as drift'));
  assert.ok(manifest.prohibitions.includes('4536847f baseline represented as current-successor PASS'));
});
