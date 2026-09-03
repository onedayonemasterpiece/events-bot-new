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

const BASE = '0d73428dfafff2fd5450b74fd68e7bb40e92d2c5';
const GOLDEN_SHA = '84504f30eebc334deba46e94365601c3d572c5c0';
const M0_VALID = 'e620e063472a20f5e703bfb7845cb9ad5e302cfe';
const M0_INVALID = 'e620e69fd8fb4641415320beaa3ea9c1003beee8';
const A0_REJECTED_REMOVAL = '5e466d65bc2b71a814c26c063f90aa07709de08f';
const A0_RESTORE = '84399b51b77701be714fdc84429318a9a28f93fd';

test('N0 acceptance is bound to contract 1.9 and one current candidate precursor', () => {
  assert.equal(manifest.schema_version, 'kenigevents.n0-successor-acceptance.v1');
  assert.equal(manifest.contract_version, '1.9.0');
  assert.equal(manifest.owner, 'N0');
  assert.equal(manifest.authority.combined_candidate.head, BASE);
  assert.equal(manifest.authority.combined_candidate.decision, 'ACCEPTED_PRECURSOR');
  assert.equal(
    manifest.authority.kaggle_pipeline_source.runtime_acceptance_rule,
    'ONLY_EXACT_BUILT_SHA_INHERITS_A_KAGGLE_RESULT',
  );
});

test('repaired Golden Kaggle transaction is accepted without impersonating the real or V0 gates', () => {
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
  for (const boundary of [
    'PM0 item 3 fresh-real full Kaggle Review Preview',
    'voice-review readiness gate',
    'V0 visual PASS',
    'ASTRO_NORMALIZATION_PASS',
  ]) assert.ok(golden.does_not_close.includes(boundary), `missing non-claim: ${boundary}`);
  assert.equal(manifest.next_real_gate.status, 'OPEN');
  assert.equal(manifest.next_real_gate.golden_build_is_not_a_substitute, true);
});

test('N0 rejects invalid refs and keeps current M0/A0 source boundaries explicit', () => {
  const m0 = manifest.role_outputs.M0;
  assert.equal(m0.valid_head, M0_VALID);
  assert.equal(m0.invalid_result_sha_rejected, M0_INVALID);
  assert.equal(m0.decision, 'SOURCE_SUBSTANTIVE_NOT_CANDIDATE_READY');
  assert.equal(m0.blocking_drift.code, 'ADAPTIVE_SOURCE_COUNT_ORDER_CARDINALITY');
  assert.match(m0.blocking_drift.fact, /events\.length/u);
  assert.match(m0.blocking_drift.fact, /events\.slice\(0, limit\)/u);

  const a0 = manifest.role_outputs.A0;
  assert.equal(a0.decision, 'SELECTIVE_PATCH_ONLY_NOT_WHOLESALE');
  assert.equal(a0.standalone_build_target, false);
  assert.equal(a0.product_removal_rejected_commit, A0_REJECTED_REMOVAL);
  assert.equal(a0.product_behavior_restore_commit, A0_RESTORE);
  assert.equal(a0.restore_decision, 'ACCEPTED_CORRECTION');
  assert.ok(a0.open_drift.some((item) => item.includes('hidden donor layouts')));
});

test('only the exact built SHA has runtime acceptance; newer pipeline heads stay unbuilt checkpoints', () => {
  const decisions = manifest.pipeline_source_decisions;
  assert.equal(decisions.built_transaction_commits[GOLDEN_SHA], 'ACCEPT build-prefixed asset origin');
  assert.notEqual(decisions.post_transaction_head, GOLDEN_SHA);
  assert.equal(decisions.post_transaction_decision, 'SOURCE_REVIEW_PENDING_NO_RUNTIME_INHERITANCE');
  assert.equal(manifest.v0_triggers.golden.status, 'READY');
  assert.equal(manifest.v0_triggers.fresh_real.status, 'PENDING');
});

test('N0 source contract exposes no local full-preview publication command', () => {
  assert.equal(packageJson.scripts['deploy:preview'], undefined);
  assert.equal(packageJson.scripts['deploy:golden-preview'], undefined);
  assert.equal(packageJson.scripts['test:golden-deploy-contract'], undefined);
  assert.equal(existsSync(join(siteDir, 'scripts', 'deploy-golden-preview.mjs')), false);
  assert.equal(existsSync(join(siteDir, 'tests', 'golden-review-deploy-contract.test.mjs')), false);
  assert.match(goldenContract, /Contract: `kenigevents\.launch-normalized-ui\.v1@1\.9\.0`/u);
  assert.match(goldenContract, /same Kaggle runner/u);
  assert.match(goldenContract, /Local diagnostic/u);
  assert.doesNotMatch(goldenContract, /npm run deploy:golden-preview/u);
  assert.ok(manifest.next_real_gate.required.includes('the canonical events-bot-new Kaggle StaticSiteBuilder'));
  assert.ok(manifest.prohibitions.includes('do not use deploy:preview or deploy:golden-preview as a launch path'));
});
