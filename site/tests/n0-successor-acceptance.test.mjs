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
const SOURCE_SUCCESSOR = '4536847f9fbdaa27326ebb3ec9ec1c825736e107';
const F0 = 'de92dabd4551e117ca1af1be7915ff223321cc32';
const M0 = 'c808c75dd975a9851e148ccf993c32787d2b6886';
const A0_SOURCE = 'ec926580fa2cc003318006f4c1d671fc459ea26c';
const A0_DOWNSTREAM = 'f9217d66711731a911543314d17e35fed9824a2a';
const GOLDEN_SHA = '84504f30eebc334deba46e94365601c3d572c5c0';
const V0_COMMENT = 5527892153;
const A0_COMMENT = 5527907602;

const gate = (id) => manifest.gate_graph.find((item) => item.id === id);

test('current authority separates the precursor, unbuilt source successor and role refs', () => {
  assert.equal(manifest.schema_version, 'kenigevents.n0-successor-acceptance.v1');
  assert.equal(manifest.contract_version, '1.9.0');
  assert.equal(manifest.pm0_version, '2.3.0');
  assert.equal(manifest.owner, 'N0');
  assert.equal(manifest.evidence_cutoff.issue_comment, A0_COMMENT);

  assert.equal(manifest.current_refs.legacy_combined_candidate.head, PRECURSOR);
  assert.equal(
    manifest.current_refs.legacy_combined_candidate.classification,
    'ACCEPTED_PRECURSOR_NOT_CURRENT_SUCCESSOR',
  );
  assert.equal(manifest.current_refs.source_successor.head, SOURCE_SUCCESSOR);
  assert.equal(manifest.current_refs.source_successor.classification, 'SOURCE_ACCEPTED_UNBUILT');
  assert.equal(manifest.current_refs.source_successor.runtime_acceptance, false);
  assert.equal(manifest.current_refs.source_successor.public_url, null);
  assert.equal(manifest.current_refs.F0.reviewed_head, F0);
  assert.equal(manifest.current_refs.M0.reviewed_head, M0);
  assert.equal(manifest.current_refs.A0.source_materialization_head, A0_SOURCE);
  assert.equal(manifest.current_refs.A0.downstream_contract_head, A0_DOWNSTREAM);
  assert.equal(manifest.current_refs.N0.resolve_current_descendant_at_materialization, true);
  assert.equal(manifest.current_refs.thin_sot.resolve_current_descendant_at_materialization, true);
});

test('the published Golden transaction remains exact Golden evidence only', () => {
  const golden = manifest.accepted_golden_transaction;
  assert.equal(golden.decision, 'ACCEPTED_AS_GOLDEN_PIPELINE_AND_CORPUS_EVIDENCE_ONLY');
  assert.equal(golden.issue_comment, 5527249164);
  assert.equal(golden.repo_sha, GOLDEN_SHA);
  assert.equal(golden.url, 'https://kenigevents.ru/preview-golden-84504f30-20270604-v1/__preview/');
  assert.equal(golden.data_mode, 'golden');
  assert.deepEqual(golden.page_classes, ['all']);
  assert.equal(golden.kaggle.status, 'COMPLETE');
  assert.equal(golden.artifact.root_mutation, false);
  assert.equal(golden.artifact.stable_ics_mutation, false);
  assert.equal(golden.technical_verification.owner_url_http, 200);
  assert.equal(golden.technical_verification.document_http_200, '36/36');
  assert.equal(golden.pm0['4'], 'DONE');
  assert.equal(golden.pm0['9'], 'PARTIAL_V0_DRIFT');
  for (const boundary of [
    'fresh-real full Kaggle Review Preview',
    'voice-review readiness',
    'V0 PASS',
    'ASTRO_NORMALIZATION_PASS',
    'native Penpot',
  ]) assert.ok(golden.does_not_close.includes(boundary), `missing Golden non-claim ${boundary}`);
});

test('the unpublished retry cannot inherit the public Golden result', () => {
  const diagnostic = manifest.unpublished_pipeline_diagnostic;
  assert.equal(diagnostic.issue_comment, 5527345279);
  assert.equal(diagnostic.repo_sha, '0d92654b9637e31753fed5bd4bf6a4a66763c079');
  assert.equal(diagnostic.status, 'COMPLETE');
  assert.equal(diagnostic.publication, false);
  assert.equal(diagnostic.service_share, 'SKIPPED_GOLDEN_FROZEN_CLOCK');
  assert.equal(diagnostic.classification, 'SOURCE_AND_CLOCK_DIAGNOSTIC_ONLY');
  assert.notEqual(diagnostic.repo_sha, manifest.accepted_golden_transaction.repo_sha);
});

test('N0 accepts one Golden product drift and rejects two overbroad source gates', () => {
  const verdict = manifest.v0_golden_verdict;
  assert.equal(verdict.issue_comment, V0_COMMENT);
  assert.equal(verdict.verdict, 'DRIFT');
  assert.equal(verdict.release_acceptance, false);
  assert.equal(verdict.target_repo_sha, GOLDEN_SHA);
  assert.equal(verdict.matrix.document_http_200, '40/40');
  assert.equal(verdict.matrix.free_collection_visible_cards_at_375, 6);
  assert.equal(verdict.matrix.free_collection_horizontal_overflow, false);

  const target = verdict.findings.EVENT_CARD_AUXILIARY_TARGET_HEIGHT;
  assert.equal(target.classification, 'ACCEPTED_PRODUCT_DRIFT');
  assert.equal(target.owner, 'A0');
  assert.equal(target.source_path, 'site/src/layouts/EventLayout.astro');
  assert.equal(target.observed_height_px, 36.28);
  assert.equal(target.minimum_height_px, 44);
  assert.equal(target.blocks_first_real_candidate, true);

  const anchors = verdict.findings.DATA_UI_ANCHORS;
  assert.equal(anchors.classification, 'REJECTED_SELECTOR_CONTRACT_DRIFT');
  assert.equal(anchors.source_change_required, false);
  assert.deepEqual(anchors.canonical_identity, [
    'data-ds-family',
    'data-ds-version',
    'data-ds-variant',
    'data-ds-state',
  ]);
  assert.match(anchors.forbidden_resolution, /data-ui/u);

  const blank = verdict.findings.TARGET_BLANK;
  assert.equal(blank.classification, 'REJECTED_OVERBROAD_NEGATIVE_GATE');
  assert.equal(blank.source_change_required, false);
  assert.deepEqual(blank.required_rel_tokens, ['noopener', 'noreferrer']);
  assert.equal(blank.negative_selectors.length, 2);
});

test('F0, M0 and A0 acceptance boundaries remain explicit and non-inflating', () => {
  const { F0:f0, M0:m0, A0:a0 } = manifest.role_acceptance;
  assert.equal(f0.decision, 'SOURCE_ACCEPTED_NOT_END_TO_END_COMPLETE');
  assert.equal(f0.head, F0);
  assert.equal(f0.pm0_item_11, 'PARTIAL');
  assert.ok(f0.remaining.includes('no owner-approved font binary or @font-face authority'));

  assert.equal(m0.decision, 'SOURCE_ACCEPTED_RUNTIME_PENDING');
  assert.equal(m0.head, M0);
  assert.equal(m0.delta_commits, 9);
  assert.ok(m0.selected_paths.includes('site/src/components/event-card.css'));
  assert.ok(m0.accepted.includes('duplicate-safe source identity and named remainder variants'));
  assert.match(m0.boundary, /36px duplicate owner/u);

  assert.equal(a0.decision, 'MATERIALIZATION_BATCH_ACCEPTED_WHOLE_BRANCH_MERGE_FORBIDDEN');
  assert.equal(a0.issue_comment, A0_COMMENT);
  assert.equal(a0.source_head, A0_SOURCE);
  assert.equal(a0.downstream_head, A0_DOWNSTREAM);
  assert.ok(a0.exclude_m0_roots.includes('OptimizedEventCardGrid.astro'));
  assert.ok(a0.N0_amendments.some((item) => item.includes('computed >=44px')));
  assert.ok(a0.product_behavior_to_preserve.includes('Popular Large/Compact choice'));
  assert.equal(a0.source_tests_executed, false);
  assert.equal(a0.candidate_integrated, false);
  assert.equal(a0.browser_pass, false);
});

test('downstream bindings expose useful preparation without claiming integration or Penpot', () => {
  const downstream = manifest.downstream_bindings;
  assert.deepEqual(downstream.thin_s, {
    path: 'catalog/normalization/a0-thin-s-bindings.v1.json',
    commit: 'a800f619b66cdf713e94f234382481bb8621dd22',
    bindings: 19,
    source_implemented: '19/19',
    current_build_verified: '0/19',
    browser_observed: '0/19',
    classification: 'PM0_25_PARTIAL_SOURCE_IMPLEMENTED_UNINTEGRATED',
  });
  assert.equal(downstream.penpot.declared_status, 'SPEC_ONLY_NOT_MATERIALIZED');
  assert.equal(downstream.penpot.classification, 'PM0_39_NOT_DONE');
  assert.equal(downstream.v0_matrix.current_build_verified, '0/19');
  assert.equal(downstream.v0_matrix.browser_observed, '0/19');
  assert.equal(downstream.v0_matrix.classification, 'ACCEPTANCE_MATRIX_READY_NO_PASS');
  assert.deepEqual(downstream.rollback.units, [
    'A0-MECH-01', 'A0-MECH-02', 'A0-MECH-03', 'A0-MECH-04', 'A0-MECH-05',
  ]);
  assert.equal(downstream.rollback.production_mutation, false);
  assert.equal(downstream.rollback.classification, 'RELEASE_MECHANICS_READY_NOT_EXECUTED');
});

test('the nearest full-real candidate has one source defect plus integration and test gates', () => {
  const candidate = manifest.nearest_full_real_candidate;
  assert.equal(candidate.base, SOURCE_SUCCESSOR);
  assert.equal(candidate.status, 'BLOCKED_BY_ONE_A0_SOURCE_FIX_INTEGRATION_AND_TESTS');
  assert.equal(candidate.include.length, 4);
  assert.ok(candidate.include.some((item) => item.includes(F0)));
  assert.ok(candidate.include.some((item) => item.includes(M0)));
  assert.ok(candidate.include.some((item) => item.includes(A0_SOURCE)));
  assert.deepEqual(candidate.blocking_source_fixes, [{
    code: 'EVENT_CARD_AUXILIARY_TARGET_HEIGHT_BELOW_44PX',
    owner: 'A0',
    path: 'site/src/layouts/EventLayout.astro',
    required: 'remove or raise the specific 36px override',
    verification: 'N0_REQUIRE_V0_GOLDEN_DRIFT_FIXED=1 npm run test:n0-v0-golden-drift',
  }]);
  assert.ok(candidate.preserve.includes('Popular Large/Compact behavior'));
  assert.ok(candidate.reject.includes('whole A0 branch merge'));
  assert.ok(candidate.reject.includes('A0 replay of M0 roots'));
  assert.ok(candidate.reject.includes('parallel data-ui identity aliases'));
  assert.ok(candidate.reject.includes('blanket target=_blank ban'));
});

test('gate graph preserves the source→real→V0→Astro→thin-S→Penpot→release order', () => {
  assert.deepEqual(manifest.gate_graph.map((item) => item.id), [
    'SOURCE_CANDIDATE',
    'FULL_REAL_KAGGLE_PREVIEW',
    'V0_REAL_AUDIT',
    'ASTRO_NORMALIZATION_PASS',
    'THIN_S_BOUND',
    'PENPOT_NATIVE',
    'RELEASE_CANDIDATE',
  ]);
  assert.equal(gate('SOURCE_CANDIDATE').status, 'BLOCKED_BY_A0_FIX_INTEGRATION_AND_TESTS');
  assert.equal(gate('FULL_REAL_KAGGLE_PREVIEW').status, 'BLOCKED_BY_SOURCE_CANDIDATE');
  assert.ok(gate('FULL_REAL_KAGGLE_PREVIEW').requires.includes('--preview-data-mode real'));
  assert.ok(gate('FULL_REAL_KAGGLE_PREVIEW').requires.includes('--page-class all'));
  assert.equal(gate('V0_REAL_AUDIT').owner, 'V0');
  assert.equal(gate('THIN_S_BOUND').status, 'BLOCKED_BY_ASTRO_NORMALIZATION_PASS');
  assert.equal(gate('PENPOT_NATIVE').owner, 'R0.PENPOT');
  assert.equal(gate('RELEASE_CANDIDATE').status, 'BLOCKED_BY_GOLDEN_A_S_P_PASS');
});

test('PM0 delta reporting and one-pipeline publication rules remain enforced', () => {
  assert.deepEqual(manifest.pm0_delta_reporting.required_blocks, [
    'checkbox_transitions',
    'progress_inside_partial',
    'not_in_candidate',
    'new_owner_visible_result',
  ]);
  assert.equal(manifest.pm0_delta_reporting.zero_changes_only_when_all_refs_and_evidence_unchanged, true);
  assert.equal(manifest.pm0_delta_reporting.new_v0_claim_requires_new_browser_verdict, true);

  assert.equal(packageJson.scripts['deploy:preview'], undefined);
  assert.equal(packageJson.scripts['deploy:golden-preview'], undefined);
  assert.equal(packageJson.scripts['test:golden-deploy-contract'], undefined);
  assert.equal(existsSync(join(siteDir, 'scripts', 'deploy-golden-preview.mjs')), false);
  assert.equal(existsSync(join(siteDir, 'tests', 'golden-review-deploy-contract.test.mjs')), false);
  assert.match(goldenContract, /one existing `events-bot-new` Kaggle `StaticSiteBuilder`/u);
  assert.match(goldenContract, /Local diagnostic/u);
  assert.doesNotMatch(goldenContract, /npm run deploy:golden-preview/u);
  assert.ok(manifest.prohibitions.includes('full or published preview outside the canonical Kaggle pipeline'));
  assert.ok(manifest.prohibitions.includes('parallel data-ui identity system'));
  assert.ok(manifest.prohibitions.includes('treating safe target=_blank links as drift'));
});
