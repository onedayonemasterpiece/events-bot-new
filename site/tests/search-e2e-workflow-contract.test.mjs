import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import YAML from 'yaml';

const legacyWorkflowUrl = new URL('../../.github/workflows/static-site-search-canary.yml', import.meta.url);
const productionHealthWorkflowUrl = new URL('../../.github/workflows/search-production-health.yml', import.meta.url);
const releaseQualificationWorkflowUrl = new URL('../../.github/workflows/search-release-qualification.yml', import.meta.url);
const ciWorkflowUrl = new URL('../../.github/workflows/ci.yaml', import.meta.url);
const registryUrl = new URL('../../docs/testing/static-site-autotest-scenarios.v1.yml', import.meta.url);
const e2eIndexUrl = new URL('../../docs/operations/e2e-scenarios.md', import.meta.url);
const productionBuildUrl = new URL('../scripts/build-production.mjs', import.meta.url);
const candidateBuildUrl = new URL('../scripts/build-secret-candidate.mjs', import.meta.url);
const exporterUrl = new URL('../scripts/export-production-preview-data.py', import.meta.url);
const browserRunnerUrl = new URL('../../.github/scripts/run-browser-static-search.mjs', import.meta.url);
const targetResolverUrl = new URL('../../.github/scripts/resolve-static-search-target.sh', import.meta.url);

const formerSearchCrons = [
  '17,47 * * * *',
  '23 */3 * * *',
  '41 1,7,13,19 * * *',
  '19 2 * * *',
];

function assertManualOnly(parsed, label) {
  assert.deepEqual(Object.keys(parsed?.on || {}), ['workflow_dispatch'], `${label} must be manual-only`);
}

test('legacy Search canary is manual-only live debug and never reports product incidents', async () => {
  const source = await readFile(legacyWorkflowUrl, 'utf8');
  const parsed = YAML.parse(source);
  assertManualOnly(parsed, 'legacy Search canary');
  assert.deepEqual(parsed?.on?.workflow_dispatch?.inputs?.revision_policy?.options, ['release_exact', 'live_consistent']);
  assert.equal(parsed?.permissions?.['id-token'], 'write');
  assert.equal(parsed?.permissions?.issues, undefined);
  assert.deepEqual(parsed.concurrency, { group: 'search-production-health', 'cancel-in-progress': false });
  for (const cron of formerSearchCrons) assert.equal(source.includes(cron), false, cron);
  for (const job of ['browser', 'android', 'ios', 'terminal']) assert.ok(parsed?.jobs?.[job]);
  assert.match(source, /environment: \{ name: search-e2e \}/u);
  assert.match(source, /E2E_SEARCH_PLATFORM: android/u);
  assert.match(source, /E2E_SEARCH_PLATFORM: ios/u);
  assert.match(source, /reactivecircus\/android-emulator-runner@[0-9a-f]{40}/u);
  assert.match(source, /runs-on: macos-15/u);
  assert.match(source, /AUTH_SESSION_BROKER_OIDC_AUDIENCE: kenigevents-static-search-broker/u);
  assert.match(source, /E2E_SEARCH_REVISION_POLICY/u);
  assert.match(source, /variants="\[\\"\$INPUT_VARIANT\\"\]"/u);
  assert.match(source, /environment: \{ name: search-e2e \}/u);
  assert.match(source, /exit 1/u);
  assert.doesNotMatch(source, /repository_dispatch|\bschedule\s*:|github\.event\.(?:schedule|action)/u);
  assert.doesNotMatch(source, /\bgh issue (?:create|comment|close|list)\b|issues:\s*write|deduplicated repeated-failure incident/iu);
  assert.doesNotMatch(YAML.stringify(parsed.jobs.terminal), /github\.token|\bgh\s|incident/iu);
  assert.match(YAML.stringify(parsed.jobs.terminal), /Aggregate manual debug result/u);
  assert.doesNotMatch(source, /focus-email|E2E_MAIL|IMAP|POSTBOX|real.?mail/iu);
  assert.match(source, /trap 'rm -f "\$\{RUNNER_TEMP\}\/appium-search-android\.log"' EXIT/u);
  assert.match(source, /trap cleanup_appium EXIT/u);
  assert.match(source, /cleanup_appium\(\)[\s\S]*kill "\$appium_pid"[\s\S]*wait "\$appium_pid"[\s\S]*search-ios-prior-startup-receipt\.json/u);
  assert.match(source, /for attempt in 1 2/u);
  assert.match(source, /mobile-startup-retry\.mjs/u);
  assert.match(source, /E2E_APPIUM_LOG_PATH/u);
  assert.match(source, /E2E_APPIUM_PRIOR_RECEIPT_PATH/u);
  assert.match(source, /Retrying one pre-side-effect Appium\/WDA infrastructure startup/u);
  assert.match(source, /kill "\$appium_pid"/u);
  assert.equal((source.match(/issue-static-search-session\.mjs/gu) || []).length, 2,
    'one broker issuance step per Android/iOS job; iOS retry must reuse the unconsumed callback');
  for (const jobName of ['browser', 'android', 'ios']) {
    const upload = parsed.jobs[jobName].steps.find((step) => String(step.name).startsWith('Upload sanitized'));
    assert.equal(upload?.with?.['include-hidden-files'], true, `${jobName} must upload .redaction-ok`);
  }
});

test('production health has only the two bounded schedules, manual profiles and runtime marker', async () => {
  const source = await readFile(productionHealthWorkflowUrl, 'utf8');
  const parsed = YAML.parse(source);
  assert.deepEqual(parsed.on.schedule, [{ cron: '17 6 * * *' }, { cron: '17 18 * * *' }]);
  assert.deepEqual(parsed.on.workflow_dispatch.inputs.profile.options,
    ['browser', 'browser_android', 'browser_ios', 'all']);
  assert.deepEqual(parsed.on.repository_dispatch.types, ['search-runtime-deployed']);
  assert.deepEqual(parsed.concurrency, { group: 'search-production-health', 'cancel-in-progress': false });
  assert.match(String(parsed.jobs.plan.if), /workflow_dispatch.*SEARCH_PRODUCTION_HEALTH_ENABLED/u);
  for (const cron of formerSearchCrons) assert.equal(source.includes(cron), false, cron);
  assert.equal(parsed.on.workflow_run, undefined);
  assert.equal(parsed.on.workflow_call, undefined);
  assert.equal(parsed.on.push, undefined);
  assert.doesNotMatch(source, /smart.update|snapshot.generation|corpus.movement|index.movement/iu);
});

test('platform jobs invoke only the unified one-session runner and qualification is never scheduled', async () => {
  const [healthSource, qualificationSource] = await Promise.all([
    readFile(productionHealthWorkflowUrl, 'utf8'), readFile(releaseQualificationWorkflowUrl, 'utf8'),
  ]);
  const health = YAML.parse(healthSource);
  const qualification = YAML.parse(qualificationSource);
  assert.deepEqual(Object.keys(qualification.on), ['workflow_dispatch']);
  for (const platform of ['browser', 'android', 'ios']) {
    assert.ok(health.jobs[platform], platform);
    assert.match(YAML.stringify(health.jobs[platform]), /production-health-run\.mjs/u);
    assert.doesNotMatch(YAML.stringify(health.jobs[platform]), /issue-static-search-session|production-health-mobile-preflight/iu);
  }
  assert.equal((healthSource.match(/production-health-run\.mjs/gu) || []).length, 3);
  assert.equal((healthSource.match(/gh workflow run search-release-qualification\.yml/gu) || []).length, 1);
  assert.doesNotMatch(healthSource, /\bgh issue (?:create|comment|close|list)\b/iu);
  assert.match(YAML.stringify(health.jobs.terminal), /production-health-report-plan-cli\.mjs/u);
  assert.equal(qualification.on.schedule, undefined);
  assert.match(qualificationSource, /production-health-run\.mjs/u);
  assert.deepEqual(qualification.on.workflow_dispatch.inputs.profile.options, ['browser', 'full']);
  assert.match(qualificationSource, /\["cold_vector_llm","degraded_vector_fallback"\]/u);
  assert.match(qualificationSource, /one no-mail session/u);
  assert.match(qualificationSource, /E2E_SEARCH_REVISION_POLICY: release_exact/u);
  assert.match(healthSource, /-f profile=full/u);
  for (const platform of ['browser', 'android', 'ios']) {
    assert.match(String(health.jobs['request-release-qualification'].if),
      new RegExp(`needs\\.${platform}\\.result == 'success'`, 'u'));
  }
});

test('default pull-request CI runs the deterministic Search production-health suite without secrets', async () => {
  const source = await readFile(ciWorkflowUrl, 'utf8');
  const parsed = YAML.parse(source);
  assert.ok(parsed?.on?.pull_request);
  const steps = parsed.jobs['static-browser-release-gate'].steps;
  const searchStep = steps.find((step) => step.name === 'Run deterministic Search production-health contracts');
  assert.equal(searchStep?.run, 'npm run test:search-production-health');
  assert.equal(searchStep?.env, undefined);
  assert.doesNotMatch(source, /\$\{\{\s*secrets\./u);
});

test('all Search and default CI workflow YAML parses', async () => {
  for (const url of [legacyWorkflowUrl, productionHealthWorkflowUrl, releaseQualificationWorkflowUrl, ciWorkflowUrl]) {
    const source = await readFile(url, 'utf8');
    assert.doesNotThrow(() => YAML.parse(source), url.pathname);
  }
});

test('E2E index preserves current MCP contract and labels old Search journey manual-only', async () => {
  const source = await readFile(e2eIndexUrl, 'utf8');
  assert.equal(source.split('\n').filter((line) => line.includes('scripts/smoke_private_events_mcp.py')).length, 1);
  assert.match(source, /prepare\(approved\) -> commit/u);
  assert.doesNotMatch(source, /prepare -> browser approval -> commit/u);
  assert.match(source, /Manual legacy authenticated semantic Search harness/u);
  assert.match(source, /former cached 30-minute[\s\S]*triggers are disabled/u);
  assert.doesNotMatch(source, /scheduled\/post-deploy `release_exact` still requires/u);
});

test('registry freezes exact Search variants and platform-scoped personas', async () => {
  const source = await readFile(registryUrl, 'utf8');
  const registry = YAML.parse(source);
  assert.deepEqual(registry.scenarios['browser.route_health'].layers, ['L0', 'L1']);
  assert.deepEqual(registry.scenarios['browser.route_health'].platforms, ['browser']);
  assert.deepEqual(registry.scenarios['search.production_health'].layers, ['L0', 'L1', 'L2']);
  assert.deepEqual(registry.scenarios['search.production_health'].platforms, ['browser', 'android', 'ios']);
  assert.match(source, /search\.live_cached_journey:[\s\S]*variants: \[cached_vector\]/u);
  assert.match(source, /search\.live_cold_journey:[\s\S]*variants: \[cold_vector, cold_vector_llm, degraded_vector_fallback\]/u);
  for (const token of [
    'request_policy:', 'expected_cache_state:', 'provider_attempts:', 'auth_persona:',
    'platform_selection:', 'blocking_policy:', 'evidence_requirements:',
  ]) assert.ok(source.includes(token), token);
  assert.match(source, /search-cached-platform-scoped/u);
  assert.match(source, /search-cold-browser/u);
  assert.match(source, /search-degraded-browser/u);
});

test('immutable candidate binds the authoritative complete Search projection revisions', async () => {
  const [production, candidate, exporter] = await Promise.all([
    readFile(productionBuildUrl, 'utf8'), readFile(candidateBuildUrl, 'utf8'), readFile(exporterUrl, 'utf8'),
  ]);
  assert.match(exporter, /--prune-missing/u);
  assert.match(exporter, /--require-complete/u);
  assert.match(exporter, /event-search-corpus-receipt\.json/u);
  assert.match(production, /search_revisions/u);
  assert.match(production, /event_vector_sync_receipt_v2/u);
  assert.match(production, /Search corpus revision identities disagree/u);
  assert.match(production, /Search corpus\/catalog revision mismatch/u);
  assert.match(candidate, /Authorized Search candidate requires complete catalog\/corpus revisions/u);
  assert.match(candidate, /search_revisions: productionManifest\.search_revisions/u);
});

test('browser fixture sends its issued session through the owner RLS probe', async () => {
  const source = await readFile(browserRunnerUrl, 'utf8');
  assert.match(source, /createRequire\(new URL\('\.\.\/\.\.\/site\/package\.json'/u);
  assert.match(source, /siteRequire\('playwright'\)/u);
  assert.doesNotMatch(source, /from 'playwright'/u);
  assert.match(source, /id: 'search-cached-browser'/u);
  assert.match(source, /platform: 'browser'/u);
  assert.doesNotMatch(source, /id: 'search-(?:cold|degraded)-browser'/u);
  assert.match(source, /protectedOwnerProbe\(\{ fetchImpl, userId, supabaseUrl, accessToken, publishableKey \}\)/u);
  assert.match(source, /apikey: publishableKey/u);
  assert.match(source, /authorization: `Bearer \$\{accessToken\}`/u);
});

test('target resolver uses the Python runtime shipped in the Fly image', async () => {
  const source = await readFile(targetResolverUrl, 'utf8');
  assert.match(source, /--command "python3 scripts\/request_static_site_build\.py/u);
  assert.doesNotMatch(source, /\.venv\/bin\/python/u);
  assert.doesNotMatch(source, /\bmapfile\b/u);
  assert.match(source, /IFS= read -r target_url/u);
  assert.match(source, /IFS= read -r repo_sha/u);
});
