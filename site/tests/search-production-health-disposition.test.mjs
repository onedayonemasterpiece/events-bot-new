import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

import {
  SEARCH_HEALTH_REPORT_ACTIONS,
  buildSearchHealthReportPlan,
} from '../e2e/search/production-health-disposition/report-plan.mjs';
import {
  SEARCH_HEALTH_SUMMARY_FIELDS,
  SEARCH_HEALTH_SUMMARY_SCHEMA,
  normalizeSearchHealthSummary,
} from '../e2e/search/production-health-disposition/summary.mjs';

const fingerprint = (character) => character.repeat(64);

const summary = (overrides = {}) => ({
  schema_version: SEARCH_HEALTH_SUMMARY_SCHEMA,
  platform: 'browser',
  product_health: 'HEALTHY',
  execution_status: 'PASS',
  failure_class: null,
  target_superseded: false,
  target_fingerprint: fingerprint('a'),
  runtime_fingerprint: fingerprint('b'),
  run_id: '31307905426',
  run_url: 'https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31307905426',
  ...overrides,
});

test('summary interface is an exact fixed allowlist and rejects raw evidence fields', () => {
  assert.deepEqual(SEARCH_HEALTH_SUMMARY_FIELDS, [
    'schema_version', 'platform', 'product_health', 'execution_status', 'failure_class',
    'target_superseded', 'target_fingerprint', 'runtime_fingerprint', 'run_id', 'run_url',
  ]);
  assert.deepEqual(normalizeSearchHealthSummary(summary()), summary());
  for (const forbiddenField of [
    'target_url', 'query', 'card', 'session', 'raw_logs', 'email', 'access_token', 'artifact_url',
  ]) {
    assert.throws(
      () => normalizeSearchHealthSummary({ ...summary(), [forbiddenField]: 'must-not-pass' }),
      new RegExp(`field_not_allowlisted:${forbiddenField}`, 'u'),
    );
  }
});

test('summary validation fails closed on inconsistent dimensions and unsafe run URLs', () => {
  assert.throws(
    () => normalizeSearchHealthSummary(summary({ product_health: 'BROKEN' })),
    /broken_outcome_inconsistent/u,
  );
  assert.throws(
    () => normalizeSearchHealthSummary(summary({ failure_class: 'BROKEN_RESULT_ROUTE' })),
    /healthy_outcome_inconsistent/u,
  );
  assert.throws(
    () => normalizeSearchHealthSummary(summary({ run_url: 'https://attacker.invalid/actions/runs/31307905426' })),
    /run_url/u,
  );
  assert.throws(
    () => normalizeSearchHealthSummary(summary({
      platform: 'ios', product_health: 'UNCONFIRMED', execution_status: 'FAILED',
      failure_class: 'UNKNOWN_ANDROID_INFRA',
    })),
    /android_failure_platform_mismatch/u,
  );
});


test('superseded target never opens or closes a product incident', () => {
  const broken = buildSearchHealthReportPlan({ summary: summary({
    target_superseded: true, product_health: 'BROKEN', execution_status: 'FAILED',
    failure_class: 'BROKEN_NO_RESULTS',
  }) });
  const healthy = buildSearchHealthReportPlan({ summary: summary({ target_superseded: true }) });
  assert.deepEqual(
    [broken.operation.action, broken.operation.reason, healthy.operation.action, healthy.operation.reason],
    ['none', 'target_superseded', 'none', 'target_superseded'],
  );
});

test('proven BROKEN result opens an immediate exact platform product incident', () => {
  const plan = buildSearchHealthReportPlan({ summary: summary({
    platform: 'android',
    product_health: 'BROKEN',
    execution_status: 'FAILED',
    failure_class: 'BROKEN_RESULT_RENDER',
  }) });

  assert.equal(plan.operation.action, SEARCH_HEALTH_REPORT_ACTIONS.OPEN_OR_UPDATE);
  assert.equal(plan.operation.issue_kind, 'product');
  assert.equal(plan.operation.fingerprint, 'search-product:android:BROKEN_RESULT_RENDER');
  assert.equal(plan.operation.platform, 'android');
  assert.match(plan.operation.body, /31307905426/u);
});

test('healthy full proof closes product incidents for its exact platform only', () => {
  const android = buildSearchHealthReportPlan({ summary: summary({ platform: 'android' }) });
  const browser = buildSearchHealthReportPlan({ summary: summary({ platform: 'browser' }) });

  assert.deepEqual(
    { action: android.operation.action, kind: android.operation.issue_kind, prefix: android.operation.fingerprint_prefix },
    { action: 'close_matching', kind: 'product', prefix: 'search-product:android:' },
  );
  assert.equal(browser.operation.fingerprint_prefix, 'search-product:browser:');
  assert.doesNotMatch(android.operation.fingerprint_prefix, /browser|ios/u);
  assert.equal(Object.hasOwn(android.operation, 'close_infrastructure'), false);
});

test('UNKNOWN opens infrastructure incident only on third consecutive identical terminal run', () => {
  const unknown = summary({
    platform: 'ios',
    product_health: 'UNCONFIRMED',
    execution_status: 'FAILED',
    failure_class: 'UNKNOWN_IOS_INFRA',
  });
  const first = buildSearchHealthReportPlan({ summary: unknown });
  const second = buildSearchHealthReportPlan({ summary: unknown, history: [unknown] });
  const third = buildSearchHealthReportPlan({ summary: unknown, history: [unknown, unknown] });

  assert.deepEqual(
    [first.operation.action, first.operation.identical_terminal_runs],
    ['none', 1],
  );
  assert.deepEqual(
    [second.operation.action, second.operation.identical_terminal_runs],
    ['none', 2],
  );
  assert.equal(third.operation.action, 'open_or_update');
  assert.equal(third.operation.issue_kind, 'infrastructure');
  assert.equal(third.operation.fingerprint, 'search-infra:ios:UNKNOWN_IOS_INFRA');
  assert.match(third.operation.body, /Consecutive identical terminal runs: `3`/u);
});

test('UNKNOWN streak is platform-local: desktop cannot suppress mobile and same-platform class changes reset it', () => {
  const current = summary({
    platform: 'android', product_health: 'UNCONFIRMED', execution_status: 'FAILED',
    failure_class: 'UNKNOWN_ANDROID_INFRA',
  });
  const olderMatchOne = { ...current, run_id: '31307905419', run_url: 'https://github.com/o/r/actions/runs/31307905419' };
  const olderMatchTwo = { ...current, run_id: '31307905420', run_url: 'https://github.com/o/r/actions/runs/31307905420' };
  const interveningBrowser = summary({
    platform: 'browser', product_health: 'UNCONFIRMED', execution_status: 'FAILED',
    failure_class: 'UNKNOWN_AUTH_BROKER', run_id: '31307905421',
    run_url: 'https://github.com/o/r/actions/runs/31307905421',
  });
  const notSuppressed = buildSearchHealthReportPlan({
    summary: current,
    history: [olderMatchOne, olderMatchTwo, interveningBrowser],
  });

  assert.equal(notSuppressed.operation.action, 'open_or_update');
  assert.equal(notSuppressed.operation.fingerprint, 'search-infra:android:UNKNOWN_ANDROID_INFRA');

  const samePlatformDifferentClass = {
    ...current,
    failure_class: 'UNKNOWN_AUTH_BROKER',
    run_id: '31307905422',
    run_url: 'https://github.com/o/r/actions/runs/31307905422',
  };
  const reset = buildSearchHealthReportPlan({
    summary: current,
    history: [olderMatchOne, olderMatchTwo, samePlatformDifferentClass, interveningBrowser],
  });
  assert.equal(reset.operation.action, 'none');
  assert.equal(reset.operation.identical_terminal_runs, 1);
});

test('cost and evidence failures are immediate separate non-product incidents', () => {
  const cost = buildSearchHealthReportPlan({ summary: summary({
    platform: 'android', product_health: 'UNCONFIRMED', execution_status: 'FAILED',
    failure_class: 'COST_GUARD_FAILED',
  }) });
  const evidence = buildSearchHealthReportPlan({ summary: summary({
    platform: 'ios', product_health: 'UNCONFIRMED', execution_status: 'FAILED',
    failure_class: 'EVIDENCE_REDACTION_FAILED',
  }) });

  assert.equal(cost.operation.issue_kind, 'cost');
  assert.equal(cost.operation.fingerprint, 'search-cost:android');
  assert.equal(evidence.operation.issue_kind, 'security_evidence');
  assert.equal(evidence.operation.fingerprint, 'search-evidence:ios');
  assert.equal(evidence.operation.artifact_policy, 'forbidden');
  assert.doesNotMatch(evidence.operation.fingerprint, /search-product/u);
});

test('blocked release creates no incident', () => {
  const plan = buildSearchHealthReportPlan({ summary: summary({
    product_health: 'UNCONFIRMED', execution_status: 'BLOCKED',
    failure_class: 'BLOCKED_RELEASE_NOT_ACTIVE',
  }) });
  assert.deepEqual(
    { action: plan.operation.action, reason: plan.operation.reason },
    { action: 'none', reason: 'release_not_active' },
  );
});

test('generated bodies contain allowlisted summary only and omit target/query/card/session/raw logs', () => {
  const plan = buildSearchHealthReportPlan({ summary: summary({
    product_health: 'BROKEN', execution_status: 'FAILED', failure_class: 'BROKEN_NO_RESULTS',
  }) });
  const serialized = JSON.stringify(plan.operation);
  for (const forbidden of ['target_url', 'query', 'card', 'session', 'raw_logs', 'access_token', 'email']) {
    assert.doesNotMatch(serialized, new RegExp(forbidden, 'iu'));
  }
  assert.match(plan.operation.body, /Target fingerprint/u);
  assert.match(plan.operation.body, /Runtime fingerprint/u);
});

test('CLI reads a sanitized envelope from stdin and emits the deterministic plan', () => {
  const input = JSON.stringify({ summary: summary({
    product_health: 'UNCONFIRMED', execution_status: 'FAILED', failure_class: 'COST_GUARD_FAILED',
  }) });
  const result = spawnSync(
    process.execPath,
    ['e2e/search/production-health-disposition/cli.mjs'],
    { cwd: new URL('../', import.meta.url), input, encoding: 'utf8' },
  );
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).operation.fingerprint, 'search-cost:browser');
});
