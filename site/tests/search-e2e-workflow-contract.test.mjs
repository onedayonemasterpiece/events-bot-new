import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import YAML from 'yaml';

const workflowUrl = new URL('../../.github/workflows/static-site-search-canary.yml', import.meta.url);
const registryUrl = new URL('../../docs/testing/static-site-autotest-scenarios.v1.yml', import.meta.url);
const productionBuildUrl = new URL('../scripts/build-production.mjs', import.meta.url);
const candidateBuildUrl = new URL('../scripts/build-secret-candidate.mjs', import.meta.url);
const exporterUrl = new URL('../scripts/export-production-preview-data.py', import.meta.url);
const browserRunnerUrl = new URL('../../.github/scripts/run-browser-static-search.mjs', import.meta.url);
const targetResolverUrl = new URL('../../.github/scripts/resolve-static-search-target.sh', import.meta.url);

test('Search workflow has independent no-mail schedules, L2 jobs and blocking post-deploy trigger', async () => {
  const source = await readFile(workflowUrl, 'utf8');
  const parsed = YAML.parse(source);
  assert.ok(parsed?.on?.workflow_dispatch);
  assert.deepEqual(parsed?.on?.repository_dispatch?.types, ['static-site-search-post-deploy']);
  assert.deepEqual(parsed?.on?.schedule?.map((item) => item.cron), [
    '17,47 * * * *', '23 */3 * * *', '41 1,7,13,19 * * *', '19 2 * * *',
  ]);
  assert.equal(parsed?.permissions?.['id-token'], 'write');
  for (const job of ['browser', 'android', 'ios', 'terminal']) assert.ok(parsed?.jobs?.[job]);
  assert.match(source, /environment: \{ name: search-e2e \}/u);
  assert.match(source, /E2E_SEARCH_PLATFORM: android/u);
  assert.match(source, /E2E_SEARCH_PLATFORM: ios/u);
  assert.match(source, /reactivecircus\/android-emulator-runner@[0-9a-f]{40}/u);
  assert.match(source, /runs-on: macos-15/u);
  assert.match(source, /static-site-search-post-deploy/u);
  assert.match(source, /AUTH_SESSION_BROKER_OIDC_AUDIENCE: kenigevents-static-search-broker/u);
  assert.match(source, /environment: \{ name: search-e2e \}/u);
  assert.match(source, /exit 1/u);
  assert.doesNotMatch(source, /focus-email|E2E_MAIL|IMAP|POSTBOX|real.?mail/iu);
  for (const jobName of ['browser', 'android', 'ios']) {
    const upload = parsed.jobs[jobName].steps.find((step) => String(step.name).startsWith('Upload sanitized'));
    assert.equal(upload?.with?.['include-hidden-files'], true, `${jobName} must upload .redaction-ok`);
  }
});

test('registry freezes exact Search variants and platform-scoped personas', async () => {
  const source = await readFile(registryUrl, 'utf8');
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
  for (const persona of ['search-cached-browser', 'search-cold-browser', 'search-degraded-browser']) {
    assert.ok(source.includes(persona), persona);
  }
  assert.match(source, /protectedOwnerProbe\(\{ fetchImpl, userId, supabaseUrl, accessToken, publishableKey \}\)/u);
  assert.match(source, /apikey: publishableKey/u);
  assert.match(source, /authorization: `Bearer \$\{accessToken\}`/u);
});

test('target resolver uses the Python runtime shipped in the Fly image', async () => {
  const source = await readFile(targetResolverUrl, 'utf8');
  assert.match(source, /--command "python3 scripts\/request_static_site_build\.py/u);
  assert.doesNotMatch(source, /\.venv\/bin\/python/u);
});
