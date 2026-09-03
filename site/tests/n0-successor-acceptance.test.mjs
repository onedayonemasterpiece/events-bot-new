import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const testsDir = dirname(fileURLToPath(import.meta.url));
const siteDir = resolve(testsDir, '..');
const manifest = JSON.parse(readFileSync(join(siteDir, 'scripts', 'n0-successor-acceptance.v1.json'), 'utf8'));
const packageJson = JSON.parse(readFileSync(join(siteDir, 'package.json'), 'utf8'));

const BASE = '0d73428dfafff2fd5450b74fd68e7bb40e92d2c5';
const FROZEN = 'd5ff87bcb7a2b2051ad956ef9e7e2733a1ae62c3';
const M0 = 'e5c07e9f00b34f4143d8539c79b6b0f9f4de32a8';
const A0_ACCEPTED = '82691aa174b788687fb8e2f0f94809ef64a36e51';
const A0_REJECTED = '5e466d65bc2b71a814c26c063f90aa07709de08f';

test('N0 manifest freezes the first real preview and keeps successor work separate', () => {
  assert.equal(manifest.schema_version, 'kenigevents.n0-successor-acceptance.v1');
  assert.equal(manifest.contract_version, '1.8.0');
  assert.equal(manifest.owner, 'N0');
  assert.equal(manifest.base_candidate.sha, BASE);
  assert.equal(manifest.base_candidate.decision, 'ACCEPTED_PRECURSOR');
  assert.equal(manifest.frozen_first_real_preview.repo_sha, FROZEN);
  assert.equal(manifest.frozen_first_real_preview.build_id, 'preview-ui-normalized-d5ff87bc-fresh-20260903-v1');
  assert.equal(manifest.frozen_first_real_preview.decision, 'KEEP_FROZEN_DO_NOT_MUTATE');
  assert.ok(manifest.prohibitions.some((item) => item.includes('frozen first real-preview')));
});

test('successor merge order uses accepted M0, bounded A0 and final N0 only', () => {
  assert.deepEqual(manifest.successor_merge_order.map((item) => item.role), ['M0', 'A0', 'N0']);
  assert.equal(manifest.successor_merge_order[0].ref, M0);
  assert.equal(manifest.successor_merge_order[1].ref, A0_ACCEPTED);
  assert.match(manifest.successor_merge_order[2].ref, /^work\/ui-normalization-n0-checklist-20260903@/u);
  assert.equal(manifest.role_outputs.M0.decision, 'ACCEPT_SOURCE_FOR_SUCCESSOR');
  assert.equal(manifest.role_outputs.M0.lineage.merge_base, BASE);
  assert.equal(manifest.role_outputs.M0.lineage.ahead_by, 13);
  assert.equal(manifest.role_outputs.M0.lineage.behind_by, 0);
  assert.equal(manifest.role_outputs.A0.accepted_head, A0_ACCEPTED);
  assert.equal(manifest.role_outputs.A0.rejected_tip, A0_REJECTED);
  assert.equal(manifest.role_outputs.A0.rejection_code, 'PRODUCT_BEHAVIOR_REMOVAL_NOT_AUTHORIZED');
  assert.equal(manifest.successor_merge_order.some((item) => item.ref === A0_REJECTED), false);
  assert.ok(manifest.prohibitions.some((item) => item.includes(A0_REJECTED.slice(0, 8))));
});

test('accepted role deltas have explicit non-overlap and executable gates', () => {
  assert.equal(manifest.path_overlap_review.M0_A0, 'NONE in accepted deltas');
  assert.match(manifest.path_overlap_review.M0_N0, /^NONE/u);
  assert.match(manifest.path_overlap_review.A0_N0, /^NONE/u);
  assert.ok(manifest.required_source_commands.some((command) => command.includes('ui-normalization-m0-contract.test.mjs')));
  assert.ok(manifest.required_source_commands.some((command) => command.includes('focus-normalization-source.test.mjs')));
  assert.ok(manifest.required_source_commands.some((command) => command.includes('test:golden-preview-contract')));
  assert.ok(manifest.required_source_commands.some((command) => command.includes('test:golden-deploy-contract')));
  assert.deepEqual(manifest.required_successor_real_gate.slice(0, 3), [
    'npm run build:preview',
    'npm run check:preview',
    'npm run check:unified-prototype',
  ]);
  assert.deepEqual(manifest.required_golden_gate_after_successor_real.slice(0, 4), [
    'npm run build:golden-preview',
    'npm run check:golden-preview',
    'npm run check:preview',
    'npm run check:unified-prototype',
  ]);
});

test('manifest never turns source review into runtime, browser or voice-review PASS', () => {
  assert.equal(manifest.role_outputs.N0.runtime_status, 'NOT_RUN_GITHUB_ONLY_ROLE');
  assert.equal(manifest.role_outputs.M0.browser_status, 'V0_OWNED_NOT_CLAIMED');
  assert.equal(manifest.checklist_effect.voice_review_gate, 'NOT_READY until current-sha fresh-real public 2xx plus V0 no-critical-drift verdict.');
  assert.ok(manifest.prohibitions.includes('do not treat source tests as runtime PASS'));
  assert.ok(manifest.prohibitions.includes('do not treat R0 local smoke as V0 verdict'));
});

test('package exposes the complete N0 source gate', () => {
  assert.equal(packageJson.scripts['test:n0-successor-acceptance'], 'node --test tests/n0-successor-acceptance.test.mjs');
  assert.match(packageJson.scripts['test:golden-preview-contract'], /golden-review-preview-contract\.test\.mjs/u);
  assert.match(packageJson.scripts['test:golden-preview-contract'], /golden-review-actions\.test\.mjs/u);
  assert.match(packageJson.scripts['check:golden-preview'], /check-golden-preview\.mjs/u);
  assert.match(packageJson.scripts['check:golden-preview'], /check-golden-actions\.mjs/u);
});
