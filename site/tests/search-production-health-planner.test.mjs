import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import test from 'node:test';

import {
  PRODUCTION_HEALTH_MANUAL_PROFILES,
  PRODUCTION_HEALTH_TRIGGERS,
  SEARCH_VALIDATION_PROFILES,
  isProductionHealthContractPath,
  planProductionHealthRun,
} from '../e2e/search/production-health-planner.mjs';

const execFileAsync = promisify(execFile);
const cli = new URL('../e2e/search/production-health-plan-cli.mjs', import.meta.url);

test('relevant pull requests deterministically select contract CI without live work', () => {
  assert.equal(isProductionHealthContractPath('site/e2e/search/production-health-contract.mjs'), true);
  assert.equal(isProductionHealthContractPath('.github/workflows/search-production-health.yml'), true);
  assert.equal(isProductionHealthContractPath('site/e2e/auth-session-fixture/session-fixture.mjs'), true);
  assert.equal(isProductionHealthContractPath('scripts/request_static_site_build.py'), true);
  assert.equal(isProductionHealthContractPath('README.md'), false);
  const relevant = planProductionHealthRun({
    plane: 'contract_ci',
    trigger: 'pull_request',
    changedPaths: ['README.md', 'site/e2e/search/production-health-contract.mjs'],
  });
  assert.equal(relevant.eligible, true);
  assert.equal(relevant.selection, 'deterministic_pr_contract_ci');
  assert.deepEqual(relevant.relevant_paths, ['site/e2e/search/production-health-contract.mjs']);
  assert.equal(relevant.dry_run, true);
  assert.equal(relevant.zero_live, true);
  assert.deepEqual(relevant.selected_platforms, []);
  assert.equal(relevant.release_qualification_requested, false);
  assert.deepEqual(relevant.live_calls, { target_resolver: 0, browser: 0, search_post: 0, supabase: 0 });

  const unrelated = planProductionHealthRun({
    plane: 'contract_ci', trigger: 'pull_request', changedPaths: ['README.md'],
  });
  assert.equal(unrelated.eligible, false);
  assert.equal(unrelated.reason, 'no_relevant_contract_path');
});

test('morning and evening triggers select deterministic browser/mobile pairs', () => {
  const morning = planProductionHealthRun({
    plane: 'production_health', trigger: PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_MORNING,
  });
  assert.equal(morning.eligible, true);
  assert.equal(morning.selection, 'scheduled_morning');
  assert.deepEqual(morning.selected_platforms, ['browser', 'android']);

  const evening = planProductionHealthRun({
    plane: 'production_health', trigger: PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_EVENING,
  });
  assert.equal(evening.eligible, true);
  assert.equal(evening.selection, 'scheduled_evening');
  assert.deepEqual(evening.selected_platforms, ['browser', 'ios']);

  for (const plan of [morning, evening]) {
    assert.equal(plan.dry_run, true);
    assert.equal(plan.zero_live, true);
    assert.equal(plan.live_calls.search_post, 0);
    assert.equal(plan.future_health_contract.query.search_post_count, 1);
    assert.equal(plan.future_health_contract.forbidden_activity.llm_calls, 0);
    assert.equal(plan.future_health_contract.forbidden_activity.pagination_requests, 0);
  }

  const ambiguous = planProductionHealthRun({
    plane: 'production_health', trigger: PRODUCTION_HEALTH_TRIGGERS.TWICE_DAILY,
  });
  assert.equal(ambiguous.eligible, false);
  assert.equal(ambiguous.reason, 'ambiguous_schedule_forbidden');
  assert.deepEqual(ambiguous.selected_platforms, []);
  assert.equal(planProductionHealthRun({ plane: 'production_health', trigger: 'pull_request' }).eligible, false);
});

test('manual profiles select only their exact deterministic platform matrix', () => {
  const expected = {
    browser: ['browser'],
    browser_android: ['browser', 'android'],
    browser_ios: ['browser', 'ios'],
    all: ['browser', 'android', 'ios'],
  };
  assert.deepEqual(PRODUCTION_HEALTH_MANUAL_PROFILES, {
    BROWSER: 'browser',
    BROWSER_ANDROID: 'browser_android',
    BROWSER_IOS: 'browser_ios',
    ALL: 'all',
  });
  for (const [profile, platforms] of Object.entries(expected)) {
    const plan = planProductionHealthRun({
      plane: 'production_health', trigger: 'workflow_dispatch', profile,
    });
    assert.equal(plan.eligible, true, profile);
    assert.deepEqual(plan.selected_platforms, platforms, profile);
    assert.equal(plan.release_qualification_requested, false, profile);
  }
  const defaultManual = planProductionHealthRun({
    plane: 'production_health', trigger: 'workflow_dispatch',
  });
  assert.deepEqual(defaultManual.selected_platforms, ['browser']);

  const invalid = planProductionHealthRun({
    plane: 'production_health', trigger: 'workflow_dispatch', profile: 'android',
  });
  assert.equal(invalid.eligible, false);
  assert.equal(invalid.reason, 'manual_profile_invalid');
  assert.deepEqual(invalid.selected_platforms, []);
});

test('release validation marker alone selects standard/full without diff inference', () => {
  assert.deepEqual(SEARCH_VALIDATION_PROFILES, {
    NONE: 'none', STANDARD: 'standard', FULL: 'full',
  });
  for (const validationProfile of ['standard', 'full']) {
    const plan = planProductionHealthRun({
      plane: 'production_health',
      trigger: 'search_runtime_deploy',
      validationProfile,
      changedPaths: ['supabase/functions/event-search/index.ts'],
    });
    assert.equal(plan.eligible, true, validationProfile);
    assert.deepEqual(plan.selected_platforms, ['browser', 'android', 'ios'], validationProfile);
    assert.equal(plan.release_qualification_requested, validationProfile === 'full', validationProfile);
  }
  for (const validationProfile of [undefined, 'none', 'critical']) {
    const plan = planProductionHealthRun({
      plane: 'production_health',
      trigger: 'search_runtime_deploy',
      validationProfile,
      changedPaths: ['supabase/functions/event-search/index.ts'],
    });
    assert.equal(plan.eligible, false, String(validationProfile));
    assert.deepEqual(plan.selected_platforms, [], String(validationProfile));
    assert.equal(plan.release_qualification_requested, false, String(validationProfile));
  }
});

test('Smart Update, snapshots, data generation and corpus/index movement never trigger health', () => {
  for (const trigger of ['smart_update', 'snapshot_generation', 'data_generation', 'corpus_movement', 'index_movement']) {
    const plan = planProductionHealthRun({ plane: 'production_health', trigger });
    assert.equal(plan.eligible, false, trigger);
    assert.equal(plan.reason, 'data_or_index_movement_never_triggers_health', trigger);
    assert.equal(plan.zero_live, true, trigger);
    assert.deepEqual(plan.selected_platforms, [], trigger);
  }
});

test('release qualification is manual, selective and Stage-1 dry', () => {
  const manual = planProductionHealthRun({ plane: 'release_qualification', trigger: 'workflow_dispatch' });
  assert.equal(manual.eligible, true);
  assert.equal(manual.selection, 'manual_selective');
  assert.deepEqual(manual.selected_platforms, []);
  assert.equal(manual.future_health_contract, null);
  assert.deepEqual(manual.live_calls, { target_resolver: 0, browser: 0, search_post: 0, supabase: 0 });
  assert.equal(planProductionHealthRun({ plane: 'release_qualification', trigger: 'twice_daily' }).eligible, false);
});

test('CLI emits deterministic sanitized JSON and performs no live operation', async () => {
  const args = ['--plane', 'production_health', '--trigger', 'workflow_dispatch'];
  const first = await execFileAsync(process.execPath, [cli.pathname, ...args]);
  const second = await execFileAsync(process.execPath, [cli.pathname, ...args]);
  assert.equal(first.stderr, '');
  assert.equal(second.stderr, '');
  assert.equal(first.stdout, second.stdout);
  const plan = JSON.parse(first.stdout);
  assert.equal(plan.eligible, true);
  assert.equal(plan.dry_run, true);
  assert.equal(plan.zero_live, true);
  assert.deepEqual(plan.live_calls, { target_resolver: 0, browser: 0, search_post: 0, supabase: 0 });
  assert.equal(JSON.stringify(plan).includes('_review/'), false);

  const release = await execFileAsync(process.execPath, [
    cli.pathname, '--plane=release_qualification', '--trigger=workflow_dispatch',
  ]);
  assert.equal(JSON.parse(release.stdout).selection, 'manual_selective');
});
