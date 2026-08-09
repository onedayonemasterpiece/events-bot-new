import assert from 'node:assert/strict';
import test from 'node:test';

import { applySearchHealthReportPlan } from '../../.github/scripts/apply-search-health-report-plan.mjs';
import { buildSearchHealthReportPlan } from '../e2e/search/production-health-disposition/report-plan.mjs';
import { SEARCH_HEALTH_SUMMARY_SCHEMA } from '../e2e/search/production-health-disposition/summary.mjs';

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

const brokenPlan = (platform = 'android', failureClass = 'BROKEN_RESULT_RENDER') => (
  buildSearchHealthReportPlan({ summary: summary({
    platform,
    product_health: 'BROKEN',
    execution_status: 'FAILED',
    failure_class: failureClass,
  }) })
);

const responseJson = (value, status = 200) => ({
  ok: status >= 200 && status < 300,
  status,
  async json() { return value; },
});

const marker = (fingerprintValue) => `<!-- search-health-fingerprint:${fingerprintValue} -->`;

test('NONE returns without token/repository and makes zero REST calls', async () => {
  const blocked = buildSearchHealthReportPlan({ summary: summary({
    product_health: 'UNCONFIRMED',
    execution_status: 'BLOCKED',
    failure_class: 'BLOCKED_RELEASE_NOT_ACTIVE',
  }) });
  let calls = 0;
  const result = await applySearchHealthReportPlan(blocked, {
    fetchImpl: async () => { calls += 1; throw new Error('must not call'); },
    token: '',
    repository: '',
  });
  assert.deepEqual(result, {
    action: 'none', reason: 'release_not_active', issue_numbers: [], dry_run: false,
  });
  assert.equal(calls, 0);
});

test('open/update finds only the exact fingerprint and reopens that issue', async () => {
  const plan = brokenPlan('android', 'BROKEN_RESULT_RENDER');
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (options.method === 'GET') {
      return responseJson([
        { number: 41, state: 'closed', body: `${marker(plan.operation.fingerprint)}\nold` },
        { number: 42, state: 'open', body: marker('search-product:browser:BROKEN_RESULT_RENDER') },
      ]);
    }
    return responseJson({ number: 41 });
  };

  const result = await applySearchHealthReportPlan(plan, {
    fetchImpl,
    token: 'token-value-never-logged',
    repository: 'onedayonemasterpiece/events-bot-new',
  });

  assert.deepEqual(result, {
    action: 'update', fingerprint: plan.operation.fingerprint, issue_numbers: [41], dry_run: false,
  });
  assert.equal(calls.length, 2);
  assert.match(calls[0].url, /\/issues\?.*state=all/u);
  assert.equal(calls[1].options.method, 'PATCH');
  assert.match(calls[1].url, /\/issues\/41$/u);
  assert.equal(calls[1].options.headers.authorization, 'Bearer token-value-never-logged');
  assert.deepEqual(JSON.parse(calls[1].options.body), {
    title: plan.operation.title,
    body: plan.operation.body,
    labels: plan.operation.labels,
    state: 'open',
  });
});

test('open plan creates one issue when no exact fingerprint exists', async () => {
  const plan = brokenPlan('ios', 'BROKEN_NO_RESULTS');
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (options.method === 'GET' && url.includes('/issues?')) return responseJson([]);
    if (options.method === 'GET' && url.includes('/labels?')) {
      return responseJson(plan.operation.labels.map((name) => ({ name })));
    }
    return responseJson({ number: 91 }, 201);
  };

  const result = await applySearchHealthReportPlan(plan, {
    fetchImpl, token: 't', repository: 'owner/repo',
  });
  assert.equal(result.action, 'create');
  assert.deepEqual(result.issue_numbers, [91]);
  assert.equal(calls.length, 3);
  assert.equal(calls[2].options.method, 'POST');
  assert.match(calls[2].url, /\/repos\/owner\/repo\/issues$/u);
  assert.deepEqual(JSON.parse(calls[2].options.body).labels, [
    'search-production-health', 'search-health:product', 'search-platform:ios',
  ]);
});

test('first incident creates missing managed labels before creating the issue', async () => {
  const plan = brokenPlan('browser', 'BROKEN_NO_RESULTS');
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (options.method === 'GET' && url.includes('/issues?')) return responseJson([]);
    if (options.method === 'GET' && url.includes('/labels?')) return responseJson([{ name: 'bug' }]);
    if (url.endsWith('/labels')) return responseJson({ name: JSON.parse(options.body).name }, 201);
    return responseJson({ number: 92 }, 201);
  };
  const result = await applySearchHealthReportPlan(plan, {
    fetchImpl, token: 't', repository: 'owner/repo',
  });
  assert.equal(result.action, 'create');
  assert.deepEqual(calls.slice(2, 5).map((call) => JSON.parse(call.options.body).name), plan.operation.labels);
  assert.match(calls.at(-1).url, /\/issues$/u);
});

test('duplicate exact fingerprints fail closed before mutation', async () => {
  const plan = brokenPlan();
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return responseJson([
      { number: 1, body: marker(plan.operation.fingerprint) },
      { number: 2, body: marker(plan.operation.fingerprint) },
    ]);
  };
  await assert.rejects(
    applySearchHealthReportPlan(plan, { fetchImpl, token: 't', repository: 'o/r' }),
    /duplicate_fingerprint/u,
  );
  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.method, 'GET');
});

test('healthy closure mutates only open exact-platform product issues', async () => {
  const plan = buildSearchHealthReportPlan({ summary: summary({ platform: 'android' }) });
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (options.method === 'GET') {
      return responseJson([
        { number: 12, state: 'open', body: marker('search-product:android:BROKEN_RESULT_ROUTE') },
        { number: 13, state: 'open', body: marker('search-product:browser:BROKEN_RESULT_ROUTE') },
        { number: 14, state: 'open', body: marker('search-cost:android') },
        { number: 15, state: 'closed', body: marker('search-product:android:BROKEN_NO_RESULTS') },
        { number: 16, state: 'open', body: marker('search-product:android:BROKEN_RESULT_RENDER'), pull_request: {} },
      ]);
    }
    return responseJson(options.method === 'POST' ? { id: 501 } : { number: 12 });
  };

  const result = await applySearchHealthReportPlan(plan, {
    fetchImpl, token: 't', repository: 'o/r',
  });
  assert.deepEqual(result, {
    action: 'close_matching',
    fingerprint_prefix: 'search-product:android:',
    issue_numbers: [12],
    dry_run: false,
  });
  assert.equal(calls.length, 3);
  assert.match(calls[1].url, /\/issues\/12\/comments$/u);
  assert.deepEqual(JSON.parse(calls[1].options.body), { body: plan.operation.close_comment });
  assert.match(calls[2].url, /\/issues\/12$/u);
  assert.deepEqual(JSON.parse(calls[2].options.body), { state: 'closed', state_reason: 'completed' });
});

test('dry-run resolves exact action but performs no POST or PATCH', async () => {
  const plan = brokenPlan();
  const methods = [];
  const result = await applySearchHealthReportPlan(plan, {
    fetchImpl: async (_url, options) => {
      methods.push(options.method);
      return responseJson([]);
    },
    token: 't',
    repository: 'o/r',
    dryRun: true,
  });
  assert.equal(result.action, 'create');
  assert.equal(result.dry_run, true);
  assert.deepEqual(methods, ['GET']);
});

test('tampered body, title, labels or extra plan fields fail before REST', async () => {
  const canonical = brokenPlan();
  const mutations = [
    { ...canonical, leaked_session: 'secret' },
    { ...canonical, operation: { ...canonical.operation, title: 'tampered' } },
    { ...canonical, operation: { ...canonical.operation, body: 'raw logs' } },
    { ...canonical, operation: { ...canonical.operation, labels: ['incident'] } },
  ];
  for (const mutation of mutations) {
    let calls = 0;
    await assert.rejects(
      applySearchHealthReportPlan(mutation, {
        fetchImpl: async () => { calls += 1; return responseJson([]); },
        token: 't',
        repository: 'o/r',
      }),
      /report_plan_invalid/u,
    );
    assert.equal(calls, 0);
  }
});

test('REST failures expose only method/path/status, never token, plan body or response body', async () => {
  const plan = brokenPlan();
  const token = 'extremely-secret-token';
  let error;
  try {
    await applySearchHealthReportPlan(plan, {
      fetchImpl: async () => responseJson({ message: 'raw-response-secret' }, 500),
      token,
      repository: 'o/r',
    });
  } catch (caught) {
    error = caught;
  }
  assert.ok(error instanceof Error);
  assert.match(error.message, /github_api:GET:\/repos\/o\/r\/issues:status_500/u);
  assert.doesNotMatch(error.message, new RegExp(token, 'u'));
  assert.doesNotMatch(error.message, /raw-response-secret|BROKEN_RESULT_RENDER|Target fingerprint/u);

  let transportError;
  try {
    await applySearchHealthReportPlan(plan, {
      fetchImpl: async () => { throw new Error(`${token}:${plan.operation.body}`); },
      token,
      repository: 'o/r',
    });
  } catch (caught) {
    transportError = caught;
  }
  assert.match(transportError.message, /github_api_transport:GET/u);
  assert.doesNotMatch(transportError.message, new RegExp(token, 'u'));
  assert.doesNotMatch(transportError.message, /BROKEN_RESULT_RENDER|Target fingerprint/u);
});
