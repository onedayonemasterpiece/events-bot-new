import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';

import { runProductionHealthReporter } from '../e2e/search/production-health-report-plan-cli.mjs';

const hash = 'a'.repeat(64);
const record = (platform, runId, failureClass = null) => ({
  schema_version: 'search_production_health_evidence_v1', platform,
  product_health: failureClass ? 'UNCONFIRMED' : 'HEALTHY',
  execution_status: failureClass ? 'FAILED' : 'PASS', failure_class: failureClass,
  workflow_run_id: runId,
  target: { target_url_sha256: hash, target_repo_sha: 'b'.repeat(40), target_superseded: false },
  search: { response: { search_contract_version: 'v1' } },
});

async function put(root, name, value) {
  const directory = join(root, name); await mkdir(directory, { recursive: true });
  await writeFile(join(directory, 'result.json'), JSON.stringify(value));
}

const env = (runId, results = {}) => ({
  GITHUB_RUN_ID: runId, GITHUB_REPOSITORY: 'onedayonemasterpiece/events-bot-new',
  GITHUB_SHA: 'c'.repeat(40), BROWSER_RESULT: 'skipped', ANDROID_RESULT: 'skipped',
  IOS_RESULT: 'skipped', ...results,
});

test('terminal reporter plans current healthy platform and ignores skipped cells', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-'));
  await put(root, 'browser', record('browser', '123'));
  const value = await runProductionHealthReporter(['--evidence-root', root], env('123', { BROWSER_RESULT: 'success' }));
  assert.deepEqual(value, [{ platform: 'browser', action: 'close_matching' }]);
});

test('terminal reporter opens infrastructure plan only on third prior/current identical platform result', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-current-'));
  const history = await mkdtemp(join(tmpdir(), 'search-report-history-'));
  await put(root, 'android', record('android', '300', 'UNKNOWN_ANDROID_INFRA'));
  await put(history, 'one', record('android', '100', 'UNKNOWN_ANDROID_INFRA'));
  await put(history, 'two', record('android', '200', 'UNKNOWN_ANDROID_INFRA'));
  const value = await runProductionHealthReporter(
    ['--evidence-root', root, '--history-root', history],
    env('300', { ANDROID_RESULT: 'failure' }),
  );
  assert.deepEqual(value, [{ platform: 'android', action: 'open_or_update' }]);
});

test('missing failed artifact becomes platform-specific UNKNOWN fallback without raw logs', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-empty-'));
  const output = join(root, 'aggregate', 'summary.json');
  const value = await runProductionHealthReporter(
    ['--evidence-root', root, '--aggregate-output', output],
    env('999', { IOS_RESULT: 'failure' }),
  );
  assert.deepEqual(value, [{ platform: 'ios', action: 'none' }]);
  const summary = JSON.parse(await readFile(output, 'utf8')).platforms[0];
  const requiredEvidenceFields = [
    'tested_at', 'target_url_sha256', 'target_superseded', 'site_runtime_sha',
    'search_backend_revision', 'content_generation_id', 'search_index_generation_id',
    'search_contract_version', 'request_id', 'search_post_count', 'result_count',
    'rendered_card_count', 'opened_route_status', 'latency_ms', 'cache_status',
    'provider_attempt_counts', 'client_observed_supabase_bytes',
  ];
  assert.deepEqual(requiredEvidenceFields.filter((key) => !(key in summary)), []);
  assert.equal(summary.evidence_available, false);
  assert.equal(summary.search_post_count, 0);
  assert.deepEqual(summary.provider_attempt_counts, { embedding: 0, vector: 0, llm: 0 });
});

test('fixed redaction output routes security disposition and forbids aggregate artifact', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-redaction-'));
  const aggregate = join(root, 'aggregate', 'summary.json');
  const value = await runProductionHealthReporter(
    ['--evidence-root', root, '--aggregate-output', aggregate],
    env('1000', { BROWSER_RESULT: 'failure', BROWSER_FAILURE_CLASS: 'EVIDENCE_REDACTION_FAILED' }),
  );
  assert.deepEqual(value, [{ platform: 'browser', action: 'open_or_update' }]);
  await assert.rejects(() => readFile(aggregate, 'utf8'), /ENOENT/u);
});

test('workflow aggregate contains only sanitized per-platform summaries and totals', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-aggregate-'));
  const output = join(root, 'aggregate', 'summary.json');
  await put(root, 'browser', {
    ...record('browser', '1200'), tested_at: '2026-08-09T15:00:00.000Z',
    search: {
      response: { search_contract_version: 'v1', catalog_revision: 'cat', corpus_revision: 'corpus', request_id: 'req' },
      physical_post_count: 1, response_id_count: 2, card_count: 2, latency_ms: 450,
      cache_state: 'miss', provider_attempts: { embedding: 1, vector: 1, llm: 0 },
      event_route: { http_status: 200 },
    },
    supabase_observed_bytes: { total_bytes: 12345 },
  });
  await runProductionHealthReporter(
    ['--evidence-root', root, '--aggregate-output', output],
    env('1200', { BROWSER_RESULT: 'success' }),
  );
  const value = JSON.parse(await readFile(output, 'utf8'));
  assert.equal(value.aggregate.search_post_count, 1);
  assert.equal(value.aggregate.llm_attempts, 0);
  assert.equal(value.aggregate.pagination_requests, 0);
  assert.equal(value.aggregate.client_observed_supabase_bytes, 12345);
  assert.equal(value.platforms[0].opened_route_status, 200);
  assert.equal(JSON.stringify(value).includes('https://kenigevents.ru'), false);
});


test('missing artifact never trusts a BROKEN workflow output as product proof', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-unproved-broken-'));
  const value = await runProductionHealthReporter(
    ['--evidence-root', root],
    env('1300', { BROWSER_RESULT: 'failure', BROWSER_FAILURE_CLASS: 'BROKEN_NO_RESULTS' }),
  );
  assert.deepEqual(value, [{ platform: 'browser', action: 'none' }]);
});

test('aggregate fallback history carries pre-runner UNKNOWN streak to the third run', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-current-empty-'));
  const history = await mkdtemp(join(tmpdir(), 'search-report-aggregate-history-'));
  for (const runId of ['100', '200']) {
    const directory = join(history, runId, `search-production-health-aggregate-${runId}-1`);
    await mkdir(directory, { recursive: true });
    const fallback = {
      schema_version: 'search_production_health_summary_v1', platform: 'android',
      product_health: 'UNCONFIRMED', execution_status: 'FAILED', failure_class: 'UNKNOWN_ANDROID_INFRA',
      target_superseded: false, target_fingerprint: hash, runtime_fingerprint: hash,
      run_id: runId, run_url: `https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/${runId}`,
      evidence_available: false,
    };
    await writeFile(join(directory, 'summary.json'), JSON.stringify({
      schema_version: 'search_production_health_workflow_summary_v1', workflow_run_id: runId,
      platforms: [fallback], platform_count: 1, aggregate: {},
    }));
  }
  const value = await runProductionHealthReporter(
    ['--evidence-root', root, '--history-root', history],
    env('300', { ANDROID_RESULT: 'failure', ANDROID_FAILURE_CLASS: 'UNKNOWN_ANDROID_INFRA' }),
  );
  assert.deepEqual(value, [{ platform: 'android', action: 'open_or_update' }]);
});

test('superseded evidence never mutates product issues', async () => {
  const root = await mkdtemp(join(tmpdir(), 'search-report-superseded-'));
  await put(root, 'browser', {
    ...record('browser', '1400'), product_health: 'BROKEN', execution_status: 'FAILED',
    failure_class: 'BROKEN_NO_RESULTS', target: {
      target_url_sha256: hash, target_repo_sha: 'b'.repeat(40), target_superseded: true,
    },
  });
  const value = await runProductionHealthReporter(
    ['--evidence-root', root], env('1400', { BROWSER_RESULT: 'failure' }),
  );
  assert.deepEqual(value, [{ platform: 'browser', action: 'none' }]);
});
