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
const SOURCE_SUCCESSOR = '1bc6d9cb4c122046f4782532381de953727c1da6';
const F0_SOURCE = '0fb2938344cf96b05be0df09dfb9e69525b3717d';
const F0_REPORT = 'dbb1914f5a5c15f5cb1c9e8464374248a9b2a046';
const M0_SOURCE = 'c71351decdcee02941acb26c5e2fbaf88faf0378';
const M0_DOWNSTREAM = '5eeaba09b5ec432a77ff899ce98fb8b9f492c133';
const A0_CURRENT = '61d340a3ca291f074289d2292b33f56b2bad8a22';
const SOT_CURRENT = 'f6a71df53eafe0763af5a606591372d952e9d371';
const GOLDEN_SHA = '84504f30eebc334deba46e94365601c3d572c5c0';

const gate = (id) => manifest.gate_graph.find((item) => item.id === id);
const delta = (id) => manifest.source_successor_review.required_current_deltas.find((item) => item.id === id);

test('N0 authority separates programme, precursor, published baseline and current source successor', () => {
  assert.equal(manifest.schema_version, 'kenigevents.n0-successor-acceptance.v1');
  assert.equal(manifest.contract_version, '1.9.0');
  assert.equal(manifest.pm0_version, '2.3.0');
  assert.equal(manifest.role, 'N0');
  assert.equal(manifest.current_refs.programme.head, PROGRAMME);
  assert.equal(manifest.current_refs.legacy_combined_candidate.head, PRECURSOR);
  assert.equal(manifest.current_refs.source_successor.head, SOURCE_SUCCESSOR);
  assert.equal(manifest.current_refs.source_successor.runtime_acceptance, false);
  assert.equal(manifest.current_refs.source_successor.public_url, null);
});

test('fresh-real transaction closes PM0 2 and 3 without inheriting later-source credit', () => {
  const real = manifest.accepted_fresh_real_transaction;
  assert.equal(real.decision, 'ACCEPTED_AS_PM0_2_AND_3_PIPELINE_AND_FRESH_DATA_BASELINE');
  assert.equal(real.issue_comment, 5528274698);
  assert.equal(real.repo_sha, PUBLISHED_REAL);
  assert.equal(real.url, 'https://kenigevents.ru/preview-real-4536847f-fresh-20260903-v1/__preview/');
  assert.equal(real.manifest_url, 'https://kenigevents.ru/preview-real-4536847f-fresh-20260903-v1/preview-build.json');
  assert.deepEqual(real.explicit_ancestry, [PROGRAMME, '1d145d5efd2a332eff29e69b6afcf43414769906', PUBLISHED_REAL]);
  assert.equal(real.data_mode, 'real');
  assert.deepEqual(real.page_classes, ['all']);
  assert.equal(real.snapshot.id, 'issue621-real-9152994b-20260903T144827Z');
  assert.equal(real.event_count, 300);
  assert.equal(real.kaggle.status, 'COMPLETE');
  assert.equal(real.artifact.root_mutation, false);
  assert.equal(real.artifact.stable_ics_mutation, false);
  assert.equal(real.technical_verification.r0_document_http_200, '126/126');
  assert.equal(real.technical_verification.independent_v0_verdict, 'NOT_EXECUTED');
  assert.equal(real.pm0['2'], 'DONE');
  assert.equal(real.pm0['3'], 'DONE');
  assert.ok(real.does_not_close.includes('runtime acceptance of the current source successor'));
  assert.ok(real.does_not_close.includes('voice-review readiness'));
});

test('Golden remains accepted deterministic evidence with independent DRIFT', () => {
  const golden = manifest.accepted_golden_transaction;
  assert.equal(golden.repo_sha, GOLDEN_SHA);
  assert.equal(golden.data_mode, 'golden');
  assert.equal(golden.kaggle_status, 'COMPLETE');
  assert.equal(golden.v0_verdict, 'DRIFT');
  assert.equal(golden.v0_issue_comment, 5527892153);
  assert.equal(golden.pm0['4'], 'DONE');
  assert.equal(golden.pm0['9'], 'PARTIAL_V0_DRIFT');

  const verdict = manifest.v0_golden_verdict;
  assert.equal(verdict.verdict, 'DRIFT');
  assert.equal(verdict.release_acceptance, false);
  assert.equal(verdict.target.repo_sha, GOLDEN_SHA);
  assert.equal(verdict.matrix.document_http_200, 40);
  assert.equal(verdict.findings.event_card_auxiliary_target_height.current_source_status,
    'CLOSED_IN_D0AD1708_PENDING_V0_RECHECK');
  assert.equal(verdict.findings.event_card_auxiliary_target_height.current_source_blocker, false);
  assert.equal(verdict.findings.stable_dom_anchors.source_change_required, false);
  assert.equal(verdict.findings.target_blank.source_change_required, false);
});

test('fresh-real V0 tool failure is recorded without manufacturing a browser verdict', () => {
  const blocker = manifest.v0_platform_blocker;
  assert.equal(blocker.issue_comment, 5529063082);
  assert.equal(blocker.classification, 'ACTUAL_TOOL_SURFACE_BOUNDARY');
  assert.equal(blocker.target_repo_sha, PUBLISHED_REAL);
  assert.equal(blocker.verdict, 'NOT_EXECUTED');
  assert.equal(blocker.browser_pass_claimed, false);
  assert.equal(blocker.browser_drift_claimed, false);
  assert.match(blocker.remaining_trigger, /my-browser-bridge/u);

  const trigger = manifest.v0_triggers.published_fresh_real_baseline;
  assert.equal(trigger.status, 'READY_BUT_V0_BLOCKED_BY_TOOL_SURFACE');
  assert.equal(trigger.blocker_comment, 5529063082);
  assert.equal(trigger.browser_verdict, 'NOT_EXECUTED');
});

test('F0 source and reporting refs are separated and both acceptance clusters are explicit', () => {
  const f0 = manifest.current_refs.F0;
  assert.equal(f0.source_head, F0_SOURCE);
  assert.equal(f0.reporting_head, F0_REPORT);
  assert.equal(f0.reporting_head_required_for_runtime, false);

  const route = delta('F0-ROUTE-THEME-BINDINGS');
  assert.equal(route.source_head, 'f2b9927e25ac0566d35577f69c574cedadab27d7');
  assert.equal(route.consumer_owner, 'A0');
  assert.equal(route.paths.length, 3);
  assert.equal(route.strict_command,
    'F0_REQUIRE_ROUTE_THEME_CONSUMED=1 node site/src/components/design-system/check-f0-route-theme-bindings.mjs');

  const club = delta('F0-INTEREST-CLUB-CARD-RESIDUAL');
  assert.equal(club.source_head, F0_SOURCE);
  assert.equal(club.reporting_head, F0_REPORT);
  assert.equal(club.consumer_owner, 'A0');
  assert.match(club.required, /four exact decorative alpha substitutions/u);
  assert.equal(club.strict_command,
    'F0_REQUIRE_CLUB_THEME_CONSUMED=1 node site/src/components/design-system/check-f0-interest-club-theme-decision.mjs');
});

test('M0 source and downstream tip are accepted as distinct current inputs', () => {
  const m0 = manifest.current_refs.M0;
  assert.equal(m0.source_head, M0_SOURCE);
  assert.equal(m0.downstream_head, M0_DOWNSTREAM);

  const accepted = delta('M0-CURRENT-SOURCE-AND-DOWNSTREAM');
  assert.equal(accepted.source_head, M0_SOURCE);
  assert.equal(accepted.downstream_head, M0_DOWNSTREAM);
  assert.equal(accepted.decision, 'SOURCE_AND_DOWNSTREAM_ACCEPTED_REQUIRED_BEFORE_NEXT_FULL_BUILD');
  assert.ok(accepted.accepted_scope.some((item) => /fail closed/u.test(item)));
  assert.ok(accepted.accepted_scope.some((item) => /EventHero/u.test(item)));
  assert.ok(accepted.accepted_scope.some((item) => /canonical SoT paths/u.test(item)));
  assert.equal(manifest.role_acceptance.M0.runtime_status, 'NOT_RUN_ON_CURRENT_SOURCE_HEAD');
});

test('A0 remains partial at 17/19 and whole-branch replay is forbidden', () => {
  const a0 = manifest.role_acceptance.A0;
  assert.equal(manifest.current_refs.A0.current_head, A0_CURRENT);
  assert.equal(manifest.current_refs.A0.whole_branch_merge_allowed, false);
  assert.equal(a0.source_identity_fraction, '17/19');
  assert.deepEqual(a0.missing_source_identities, [
    'FocusEggCollectionRouteComposition',
    'ClosedFocusHubRouteComposition',
  ]);
  assert.ok(a0.route_theme_consumption_pending.includes('InterestClubCard residual palette'));
  assert.equal(a0.whole_branch_merge_allowed, false);
  assert.equal(a0.browser_pass, false);

  const focus = delta('A0-MECH-06-FOCUS-ROUTE-IDENTITIES');
  assert.deepEqual(focus.required_families, [
    'FocusEggCollectionRouteComposition',
    'ClosedFocusHubRouteComposition',
  ]);
  assert.deepEqual(focus.required_states, ['found-N-of-M', 'checking', 'locked', 'available']);
});

test('SoT and Penpot-ready records remain preparation rather than product completion', () => {
  assert.equal(manifest.current_refs.thin_sot.head, SOT_CURRENT);
  assert.equal(manifest.downstream_bindings.N0.transaction,
    'docs/launch-normalization/n0-current-successor-acceptance.v1.json');
  assert.equal(manifest.downstream_bindings.N0.commit, '23b8b70ae0ad5b5cf59c19a7f1e94fec230c6de8');
  assert.equal(manifest.downstream_bindings.F0.classification,
    'SOURCE_SPECS_READY_INTEGRATION_AND_V0_PENDING');
  assert.equal(manifest.downstream_bindings.M0.classification, 'SOURCE_INDEX_READY_SOT_FILES_PENDING');
  assert.equal(manifest.downstream_bindings.A0.source_identity, '17/19');
  assert.equal(manifest.downstream_bindings.A0.current_build_verified, '0/19');
  assert.equal(manifest.downstream_bindings.A0.browser_observed, '0/19');
  assert.equal(manifest.downstream_bindings.A0.penpot, 'SPEC_ONLY_NOT_MATERIALIZED');
});

test('nearest candidate is exact, bounded and rejects runtime inheritance', () => {
  const candidate = manifest.nearest_full_real_candidate;
  assert.equal(candidate.base, SOURCE_SUCCESSOR);
  assert.equal(candidate.status,
    'BLOCKED_BY_F0_CONSUMPTION_M0_CURRENT_A0_MECH_06_AND_EXECUTABLE_TESTS');
  assert.ok(candidate.include.some((item) => item.includes(F0_SOURCE)));
  assert.ok(candidate.include.some((item) => item.includes(M0_SOURCE)));
  assert.ok(candidate.include.some((item) => item.includes(M0_DOWNSTREAM)));
  assert.deepEqual(candidate.blocking_source_fixes, [
    'F0_ROUTE_THEME_CONSUMPTION',
    'F0_INTEREST_CLUB_CARD_RESIDUAL',
    'A0_FOCUS_ROUTE_IDENTITIES',
  ]);
  assert.ok(candidate.preserve.includes('Popular Large/Compact behavior'));
  assert.ok(candidate.reject.includes('whole A0 branch merge'));
  assert.ok(candidate.reject.includes('runtime inheritance from an earlier SHA'));
  assert.ok(candidate.reject.includes('treating 4536847f as current-successor browser acceptance'));
});

test('gate graph preserves build, browser, Astro, thin-S, Penpot and release boundaries', () => {
  assert.equal(gate('SOURCE_CANDIDATE').status,
    'BLOCKED_BY_F0_CONSUMPTION_M0_CURRENT_A0_MECH_06_AND_TESTS');
  assert.equal(gate('FULL_REAL_KAGGLE_PREVIEW').status,
    'PM0_3_BASELINE_DONE_CURRENT_SUCCESSOR_REBUILD_PENDING');
  assert.equal(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').alias_of, 'FULL_REAL_KAGGLE_PREVIEW');
  assert.equal(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').owner, 'R0');
  assert.ok(gate('FULL_REAL_KAGGLE_REVIEW_PREVIEW').requires.includes(
    'canonical events-bot-new Kaggle StaticSiteBuilder'));
  assert.equal(gate('V0_REAL_AUDIT').owner, 'V0');
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
  assert.doesNotMatch(goldenContract, /npm run deploy:golden-preview/u);
  assert.ok(manifest.prohibitions.includes('whole A0 branch merge'));
  assert.ok(manifest.prohibitions.includes('parallel data-ui identity system'));
  assert.ok(manifest.prohibitions.includes('treating safe target=_blank links as drift'));
  assert.ok(manifest.prohibitions.includes('V0 platform blocker represented as browser PASS or DRIFT'));
});
