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
  validateSearchRuntimeDeployPayload,
} from '../e2e/search/production-health-planner.mjs';

const execFileAsync = promisify(execFile);
const cli = new URL('../e2e/search/production-health-plan-cli.mjs', import.meta.url);
const sha = '0123456789abcdef0123456789abcdef01234567';

const marker = (overrides = {}) => ({
  site_runtime_sha: sha,
  search_backend_revision: 'event-search:89abcdef',
  validation_profile: 'standard',
  changed_surfaces: ['site_runtime'],
  deployment_run_id: 'fly-main-123.1',
  ...overrides,
});

test('contract CI remains deterministic and side-effect free', () => {
  assert.equal(isProductionHealthContractPath('scripts/deploy_fly_main.sh'), true);
  assert.equal(isProductionHealthContractPath('README.md'), false);
  const plan = planProductionHealthRun({
    plane: 'contract_ci', trigger: 'pull_request', changedPaths: ['README.md', 'scripts/deploy_fly_main.sh'],
  });
  assert.equal(plan.eligible, true);
  assert.equal(plan.dry_run, true);
  assert.equal(plan.zero_live, true);
  assert.deepEqual(plan.planner_side_effects, { target_resolver: 0, browser: 0, search_post: 0, supabase: 0 });
  assert.deepEqual(plan.expected_live_calls, { platform_sessions: 0, search_post: 0 });
});

test('exact UTC schedules select browser+Android and browser+iOS', () => {
  const morning = planProductionHealthRun({
    plane: 'production_health', trigger: PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_MORNING,
  });
  const evening = planProductionHealthRun({
    plane: 'production_health', trigger: PRODUCTION_HEALTH_TRIGGERS.SCHEDULE_EVENING,
  });
  assert.deepEqual(morning.selected_platforms, ['browser', 'android']);
  assert.deepEqual(evening.selected_platforms, ['browser', 'ios']);
  assert.deepEqual(morning.expected_live_calls, { platform_sessions: 2, search_post: 2 });
  assert.equal(morning.dry_run, false);
  assert.equal(evening.zero_live, false);
  const ambiguous = planProductionHealthRun({ plane: 'production_health', trigger: 'twice_daily' });
  assert.equal(ambiguous.eligible, false);
  assert.equal(ambiguous.reason, 'ambiguous_schedule_forbidden');
});

test('manual profiles select only their exact platform matrices', () => {
  assert.deepEqual(PRODUCTION_HEALTH_MANUAL_PROFILES, {
    BROWSER: 'browser', BROWSER_ANDROID: 'browser_android', BROWSER_IOS: 'browser_ios', ALL: 'all',
  });
  const expected = {
    browser: ['browser'], browser_android: ['browser', 'android'],
    browser_ios: ['browser', 'ios'], all: ['browser', 'android', 'ios'],
  };
  for (const [profile, platforms] of Object.entries(expected)) {
    const plan = planProductionHealthRun({ plane: 'production_health', trigger: 'workflow_dispatch', profile });
    assert.equal(plan.eligible, true, profile);
    assert.deepEqual(plan.selected_platforms, platforms, profile);
  }
  assert.equal(planProductionHealthRun({
    plane: 'production_health', trigger: 'workflow_dispatch', profile: 'android',
  }).eligible, false);
});

test('repository deployment marker is exact, sanitized and explicit standard/full only', () => {
  assert.deepEqual(SEARCH_VALIDATION_PROFILES, { NONE: 'none', STANDARD: 'standard', FULL: 'full' });
  const standard = planProductionHealthRun({
    plane: 'production_health', trigger: 'search_runtime_deploy', deploymentMarker: marker(),
  });
  assert.deepEqual(standard.selected_platforms, ['browser', 'android', 'ios']);
  assert.equal(standard.release_qualification_requested, false);
  assert.equal(standard.deployment_marker.site_runtime_sha, sha);
  const full = planProductionHealthRun({
    plane: 'production_health', trigger: 'search_runtime_deploy',
    deploymentMarker: marker({ validation_profile: 'full', changed_surfaces: ['search_backend'] }),
  });
  assert.equal(full.release_qualification_requested, true);
  assert.deepEqual(full.selected_platforms, standard.selected_platforms,
    'changed_surfaces is telemetry and must not affect platform selection');

  for (const invalid of [
    marker({ validation_profile: 'none' }),
    marker({ site_runtime_sha: 'main' }),
    marker({ changed_surfaces: ['critical'] }),
    { ...marker(), unexpected: 'value' },
    marker({ deployment_run_id: 'https://secret.invalid' }),
  ]) assert.throws(() => validateSearchRuntimeDeployPayload(invalid), /search_runtime_/u);
});

test('data, generation, corpus and index movement never trigger health', () => {
  for (const trigger of ['smart_update', 'snapshot_generation', 'data_generation', 'corpus_movement', 'index_movement']) {
    const plan = planProductionHealthRun({ plane: 'production_health', trigger });
    assert.equal(plan.eligible, false, trigger);
    assert.equal(plan.reason, 'data_or_index_movement_never_triggers_health', trigger);
  }
});

test('release qualification is manual selective and never schedule-eligible', () => {
  const manual = planProductionHealthRun({ plane: 'release_qualification', trigger: 'workflow_dispatch' });
  assert.equal(manual.eligible, true);
  assert.deepEqual(manual.selected_platforms, ['browser']);
  assert.equal(manual.health_contract, null);
  assert.equal(planProductionHealthRun({
    plane: 'release_qualification', trigger: 'schedule_morning',
  }).eligible, false);
});

test('CLI validates marker and emits sanitized deterministic JSON without side effects', async () => {
  const args = [
    '--plane', 'production_health', '--trigger', 'search_runtime_deploy',
    '--site-runtime-sha', sha, '--search-backend-revision', 'event-search:89abcdef',
    '--validation-profile', 'full', '--changed-surface', 'site_runtime',
    '--deployment-run-id', 'fly-main-123.1',
  ];
  const first = await execFileAsync(process.execPath, [cli.pathname, ...args]);
  const second = await execFileAsync(process.execPath, [cli.pathname, ...args]);
  assert.equal(first.stdout, second.stdout);
  const plan = JSON.parse(first.stdout);
  assert.equal(plan.release_qualification_requested, true);
  assert.deepEqual(plan.planner_side_effects, { target_resolver: 0, browser: 0, search_post: 0, supabase: 0 });
  assert.doesNotMatch(JSON.stringify(plan), /https?:\/\//iu);
});
