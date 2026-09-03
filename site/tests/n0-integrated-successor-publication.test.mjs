import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const receipt = JSON.parse(await readFile(
  new URL('../scripts/n0-integrated-successor-publication.v1.json', import.meta.url),
  'utf8',
));

const SHA = '3ca6a143e4286c165282c2d8ceef1759a41185b7';
const BASE = 'cebeafeee08251a327145ee973ee035cced65204';
const BUILD = 'preview-real-3ca6a143e-normalized-20260903-v1';

test('publication receipt is exact, immutable and bound to the integrated successor', () => {
  assert.equal(receipt.schema, 'kenigevents.n0-integrated-successor-publication.v1');
  assert.equal(receipt.version, '1.0.0');
  assert.equal(receipt.contract_version, '1.10.0');
  assert.equal(receipt.acceptance_comment, 5532465693);
  assert.equal(receipt.r0_result_comment, 5532453272);
  assert.equal(receipt.transaction.source_sha, SHA);
  assert.equal(receipt.transaction.base_sha, BASE);
  assert.equal(receipt.transaction.build_id, BUILD);
  assert.equal(receipt.transaction.public_url, `https://kenigevents.ru/${BUILD}/__preview/`);
  assert.equal(receipt.transaction.manifest_url, `https://kenigevents.ru/${BUILD}/preview-build.json`);
  assert.equal(receipt.transaction.data_mode, 'real');
  assert.deepEqual(receipt.transaction.page_classes, ['all']);
  assert.equal(receipt.transaction.current_remote_head_at_acceptance, SHA);
  assert.equal(receipt.transaction.no_late_batch_after_freeze, true);
});

test('manifest and snapshot identities are complete', () => {
  assert.deepEqual(receipt.manifest_identity, {
    owner_http: 200,
    manifest_http: 200,
    repo_sha: SHA,
    build_id: BUILD,
    base_path: `/${BUILD}`,
    data_mode: 'real',
    page_classes: ['all'],
    authorized_search_configured: true,
  });
  assert.equal(receipt.snapshot.id, 'issue621-real-3ca6a143e-20260903T211550Z');
  assert.match(receipt.snapshot.sha256, /^[0-9a-f]{64}$/u);
  assert.equal(receipt.snapshot.size_bytes, 80838656);
  assert.equal(receipt.snapshot.event_rows, 8312);
  assert.equal(receipt.snapshot.max_event_id, 8723);
});

test('canonical Kaggle artifact and create-only publication are pinned', () => {
  assert.equal(receipt.kaggle.dataset, 'zigomaro/static-site-builder-input-20260903211913-70b493');
  assert.equal(receipt.kaggle.kernel, 'zigomaro/kenigevents-static-site-builder');
  assert.equal(receipt.kaggle.status, 'COMPLETE');
  for (const digest of [
    receipt.kaggle.artifact_archive_sha256,
    receipt.kaggle.result_sha256,
    receipt.kaggle.kernel_log_sha256,
  ]) assert.match(digest, /^[0-9a-f]{64}$/u);
  assert.equal(receipt.publication.object_count, 1423);
  assert.equal(receipt.publication.total_bytes, 282255137);
  assert.equal(receipt.publication.semantics, 'create-only immutable prefix');
  assert.equal(receipt.publication.root_mutation, false);
  assert.equal(receipt.publication.stable_ics_mutation, false);
});

test('the exact F0, M0 and A0 intake remains frozen', () => {
  assert.deepEqual(receipt.integrated_intake, {
    F0: {
      role_sha: 'ea4eda91d03bd15bb99e26f4990fe9818e3d4d8b',
      integration_commit: 'b90704190314427c81d2d04cc7c51123d53fb68a',
    },
    M0: {
      role_sha: '105bac16be6c73916a25f3e78b02116869ed5e1e',
      integration_commit: 'a06cd102e27af313ce1446fe84777fd6d5cc449d',
    },
    A0: {
      role_sha: '3ef253980bdfe0731158f5b8b4b47965fa153ce9',
      integration_commit: '8e9975b69c6e353944a79586f5dba82faaa887c0',
    },
    FR0_source_batch: null,
    R0_reconciliation: SHA,
  });
});

test('R0 smoke is kept below the independent V0 boundary', () => {
  assert.equal(receipt.R0_smoke_boundary.classification, 'SUPPORTING_TECHNICAL_EVIDENCE_NOT_V0');
  assert.equal(receipt.R0_smoke_boundary.route_contract_matrix, '10/10 PASS');
  assert.equal(receipt.R0_smoke_boundary.focused_contract_matrix, '77/77 PASS');
  assert.equal(receipt.R0_smoke_boundary.isolated_browser_matrix, '176/176 PASS');
  assert.equal(receipt.R0_smoke_boundary.independent_browser_verdict, false);
  assert.equal(receipt.acceptance.independent_V0, 'PENDING');
  assert.equal(receipt.acceptance.exhibitions_vertical_slice, 'SOURCE_ACCEPTED_BROWSER_PENDING');
  assert.equal(receipt.acceptance.runtime_EventCard_MediaFrame, 'KNOWN_A0_DRIFT_NOT_PASS');
});

test('next gate requires a personal exact-source verdict and an independently accepted slice', () => {
  assert.equal(receipt.exact_V0_trigger.comment, 5532465693);
  assert.equal(receipt.exact_V0_trigger.target_source_sha, SHA);
  assert.deepEqual(receipt.exact_V0_trigger.known_runtime_drift_comments, [5532284724, 5532335696]);
  assert.deepEqual(receipt.exact_V0_trigger.FR0_requirement_comments, [5531944339, 5531980502]);
  assert.equal(receipt.exact_V0_trigger.required_exhibitions_slice, true);
  assert.equal(receipt.exact_V0_trigger.result, 'PENDING');
  assert.equal(receipt.next_gate.owner, 'V0_THEN_N0');
  assert.ok(receipt.next_gate.required.some((item) => item.includes('personal my-browser-bridge verdict')));
  assert.ok(receipt.next_gate.required.some((item) => item.includes('exhibitions vertical slice independently')));
});

test('publication acceptance makes no downstream PASS claim', () => {
  assert.deepEqual(receipt.non_claims, [
    'independent V0 verdict received',
    'exhibitions vertical slice browser accepted',
    'runtime EventCard MediaFrame delivery passed',
    'ASTRO_NORMALIZATION_PASS issued',
  ]);
});
