import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { summarizeSearchPayload } from '../e2e/search/acceptance.mjs';
import {
  createPlaywrightSearchAdapter,
  snapshotResultsInPage,
} from '../e2e/search/adapters/playwright.mjs';
import {
  PRODUCTION_HEALTH_UI_QUERY,
  runProductionHealthJourney,
} from '../e2e/search/production-health-journey.mjs';
import {
  SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  SUPABASE_CLIENT_BYTE_TARGET,
  SupabaseClientObservedByteMeter,
  mergeSupabaseClientByteSnapshots,
} from '../e2e/search/production-health-meter.mjs';
import {
  createBuiltInBrowserHooks,
  githubMaskCommand,
  hasActiveSearchReleaseReceipt,
  resolveExpectedAcceptedTarget,
  runProductionHealthCell,
} from '../e2e/search/production-health-run.mjs';
import { createBuiltInMobileHooks } from '../e2e/search/production-health-run.mjs';
import {
  installSearchRuntimeProbe,
  snapshotSearchRuntimeProbe,
  verifyAuthenticatedOwnerRuntimeProbe,
} from '../e2e/search/adapters/runtime-probe.mjs';
import {
  createAcceptedTargetRun,
  normalizeAcceptedTargetResolverResult,
} from '../e2e/search/production-health-target.mjs';
import { waitForActiveSearchBackend } from '../e2e/search/search-backend-release-probe.mjs';

const meter = (bytes = 1024) => ({
  schema_version: 'supabase_client_observed_bytes_v1', measurement_basis: 'client_observed_response_bytes',
  total_bytes: bytes, target_bytes: SUPABASE_CLIENT_BYTE_TARGET, hard_limit_bytes: SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  budget_status: bytes <= SUPABASE_CLIENT_BYTE_TARGET ? 'within_target' : bytes <= SUPABASE_CLIENT_BYTE_HARD_LIMIT ? 'above_target' : 'hard_limit_exceeded',
  target_met: bytes <= SUPABASE_CLIENT_BYTE_TARGET, cost_guard_passed: bytes <= SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  hard_limit_exceeded: bytes > SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  categories: { auth: 0, edge: bytes, direct_rest: 0, direct_rpc: 0 },
  sources: { content_length: bytes, received_body: 0 }, excluded_requests: 0,
});
const backendRevision = `sha256:${'d'.repeat(64)}`;

test('GitHub masking command accepts one exact secret and rejects log injection', () => {
  assert.equal(githubMaskCommand('https://kenigevents.ru/_review/private/poisk/'),
    '::add-mask::https://kenigevents.ru/_review/private/poisk/');
  assert.throws(() => githubMaskCommand('https://example.test/\nleak'),
    /search_health_mask_value_invalid/u);
});

function fakeJourneyAdapter(overrides = {}) {
  let typed = '';
  let policy = null;
  let activityCalls = 0;
  let navigationOpened = false;
  let postNavigationSearchPostCount = 0;
  let latePostRecorded = false;
  let terminalMinimumCardCount = null;
  let submitCalls = 0;
  let opened = false;
  let physicalPosts = 0;
  let preNavigationActivity = null;
  let preNavigationResults = null;
  const state = {
    requests: [], responses: [], routes: [],
    network: { storage_requests: 0, receipt_rpc_requests: 0, failed_requests: 0 },
    meter: meter(overrides.bytes ?? 2048),
  };
  const diagnostics = { console_errors: 0, failed_requests: 0, error_responses: 0, storage_requests: 0 };
  const ids = overrides.ids || ['101', '102'];
  const response = {
    schema_version: 'event_search_v2', search_contract_version: 'event_search_v2',
    search_backend_revision: backendRevision,
    request_id: 'request-1', receipt_id: null, response_ids: [...ids],
    response_families: ids.map((id) => `event:${id}`), item_count: ids.length, fallback_count: 0,
    has_more: false, next_offset: 0, result_cache_status: overrides.cache || 'miss', served_from_cache: overrides.cache === 'hit',
    requested_execution_mode: 'cold_vector', actual_execution_mode: overrides.actualMode || 'cold_vector',
    catalog_revision: 'a'.repeat(64), corpus_revision: 'b'.repeat(64), search_document_revision: 'c'.repeat(64),
    policy_versions: {}, provider_attempts: { embedding: 1, vector: 1, llm: overrides.llm ?? 0 },
    provider_attempts_present: true, provider_attempts_source: 'request_counters',
    llm: { requested: false, used: false, status: '' }, http_status: 200, route: 'direct',
    ...(overrides.responseTelemetry || {}),
  };
  const resultSnapshot = () => ({
    terminal: true, error: overrides.resultError === true,
    cards_visible: ids.length > 0, visible_card_count: ids.length,
    rendered_ids: [...ids], rendered_families: ids.map((id) => `event:${id}`),
    card_renderer_unavailable: overrides.rendererUnavailable === true,
    skeleton_count: overrides.skeletons || 0, placeholder_count: overrides.placeholders || 0,
  });
  return {
    get typed() { return typed; }, get policy() { return policy; },
    get activityCalls() { return activityCalls; },
    get terminalMinimumCardCount() { return terminalMinimumCardCount; },
    get submitCalls() { return submitCalls; },
    async configureRequestPolicy(value) { policy = value; },
    async open() { opened = true; },
    async inspectSurface() { return { enabled: true, authorized: true, input_tag: 'textarea', enter_key_hint: 'search' }; },
    async activity() { activityCalls += 1; return structuredClone(state); },
    async awaitPhysicalIdle() {},
    async physicalActivity() {
      const pageInitBytes = opened ? Number(overrides.pageInitBytes || 0) : 0;
      const searchBytes = submitCalls > 0 ? Number(overrides.bytes ?? 2048) : 0;
      const postBytes = navigationOpened ? Number(overrides.postNavigationBytes || 0) : 0;
      const bytes = pageInitBytes + searchBytes + postBytes;
      return {
        search_posts: physicalPosts,
        storage_requests: Number(state.network.storage_requests || 0),
        receipt_rpc_requests: Number(state.network.receipt_rpc_requests || 0),
        meter: meter(bytes),
      };
    },
    async healthDiagnostics() {
      if (navigationOpened && overrides.latePostAfterNavigation && !latePostRecorded) {
        latePostRecorded = true;
        postNavigationSearchPostCount += 1;
        physicalPosts += 1;
      }
      if (navigationOpened && overrides.finalDiagnosticsFailure) {
        throw new Error(overrides.finalDiagnosticsError || 'search_browser_crashed_after_navigation');
      }
      return { ...diagnostics, ...(overrides.diagnostics || {}) };
    },
    async typeQuery(value) { typed = value; },
    async submitWithSearchIntent() {
      submitCalls += 1;
      const body = {
        limit: overrides.limit ?? 5, offset: overrides.offset ?? 0,
        use_llm_verifier: false, allow_llm_fallback: false,
        execution_mode_present: overrides.executionModePresent === true,
      };
      const times = overrides.postCount ?? 1;
      for (let index = 0; index < times; index += 1) {
        physicalPosts += 1;
        state.requests.push({ sequence: index + 1, method: 'POST', path: '/functions/v1/event-search', route: 'direct', body_contract: body });
        state.responses.push({ ...response, sequence: index + 1 });
        state.routes.push({ operation_id: `op-${index}`, policy: 'selected-once', route: 'direct', initial_route: 'direct', kind: 'success', status: 200 });
      }
      state.network.storage_requests = overrides.storageRequests || 0;
      state.network.receipt_rpc_requests = overrides.receiptRpcRequests || 0;
    },
    async waitForTerminal({ minimumCardCount }) { terminalMinimumCardCount = minimumCardCount; },
    async snapshotResults() {
      return resultSnapshot();
    },
    async realScrollResults() { return { performed: true, delta_y: 640, card_visible_after: true, gesture_count: 1 }; },
    async openFirstResult() {
      const searchPageActivity = structuredClone(state);
      preNavigationActivity = searchPageActivity;
      preNavigationResults = resultSnapshot();
      navigationOpened = true;
      if (overrides.postAfterOpen) {
        state.requests.push({ method: 'POST', path: '/functions/v1/event-search', body_contract: {} });
        postNavigationSearchPostCount += 1;
        physicalPosts += 1;
      }
      if (overrides.resetActivityOnOpen) {
        state.requests.length = 0;
        state.responses.length = 0;
        state.routes.length = 0;
      }
      return overrides.eventRoute || {
        same_origin: true, http_status: 200, destination_class: 'event_detail', network_source: 'fake',
        search_page_activity_before_navigation: searchPageActivity,
      };
    },
    async postNavigationSearchPostCount() { return postNavigationSearchPostCount; },
    async postNavigationMeterSnapshot() { return meter(overrides.postNavigationBytes ?? 0); },
    async failedJourneyEvidence() {
      return {
        activity: structuredClone(preNavigationActivity || state),
        results: structuredClone(preNavigationResults || resultSnapshot()),
        post_navigation_search_post_count: postNavigationSearchPostCount,
        post_navigation_meter: meter(overrides.postNavigationBytes ?? 0),
      };
    },
  };
}

const targetRow = (token = 'A'.repeat(43), overrides = {}) => ({
  source: 'current_accepted_pointer', target_url: `https://kenigevents.ru/_review/${token}/poisk/`,
  target_repo_sha: 'a'.repeat(40), build_id: 'build-1', run_id: 'run-1', snapshot_id: 'snapshot-1',
  result_sha256: 'b'.repeat(64), manifest_sha256: 'c'.repeat(64),
  token_sha256: createHash('sha256').update(token).digest('hex'), input_fingerprint: 'd'.repeat(64),
  ...overrides,
});

const authReceipt = {
  get_user_verified: true, protected_probe_verified: true, protected_probe_request_count: 1,
  product_otp_issue_count: 0, external_mail_send_count: 0, external_mail_receipt_count: 0,
  real_mail_fallback: 'forbidden',
};

const preflight = {
  side_effect_free: true, browser_ready: true, transport_ready: true, viewport_ready: true,
  auth_requests: 0, search_posts: 0, otp_requests: 0, supabase_requests: 0,
};

test('production health journey sends exact UI intent and one normal vector-only POST', async () => {
  for (const cache of ['hit', 'miss', 'stored']) {
    const adapter = fakeJourneyAdapter({ cache });
    const result = await runProductionHealthJourney({
      adapter, targetUrl: 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/',
    });
    assert.equal(adapter.typed, PRODUCTION_HEALTH_UI_QUERY);
    assert.equal(adapter.typed, 'куда сходить в Калининграде');
    assert.deepEqual(adapter.policy, { production_health: true, selected_once: true });
    assert.equal(result.search_post_count, 1);
    assert.equal(result.card_count, 2);
    assert.deepEqual(result.response_ids, result.rendered_ids);
    assert.equal(result.provider_attempts.llm, 0);
    assert.equal(result.event_route.http_status, 200);
    assert.equal(JSON.stringify(result).includes(PRODUCTION_HEALTH_UI_QUERY), false);
  }
});

test('production health journey merges post-navigation Supabase bytes and enforces the hard cap', async () => {
  const within = await runProductionHealthJourney({
    adapter: fakeJourneyAdapter({ bytes: 40_000, postNavigationBytes: 20_000 }),
    targetUrl: 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/',
  });
  assert.equal(within.meter.total_bytes, 60_000);
  assert.equal(within.meter.budget_status, 'above_target');

  await assert.rejects(() => runProductionHealthJourney({
    adapter: fakeJourneyAdapter({ bytes: 90_000, postNavigationBytes: 9_000 }),
    targetUrl: 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/',
  }), /search_health_supabase_hard_limit_exceeded/u);
});

test('page-init Auth/RLS hard cap blocks before physical Search dispatch', async () => {
  const adapter = fakeJourneyAdapter({ pageInitBytes: SUPABASE_CLIENT_BYTE_HARD_LIMIT + 1 });
  await assert.rejects(() => runProductionHealthJourney({
    adapter,
    targetUrl: 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/',
  }), /search_health_supabase_hard_limit_exceeded/u);
  assert.equal(adapter.submitCalls, 0);
  assert.equal((await adapter.activity()).requests.length, 0);
});

test('normal vector health accepts a cache hit reported as actual cached_vector', async () => {
  const result = await runProductionHealthJourney({
    adapter: fakeJourneyAdapter({ cache: 'hit', actualMode: 'cached_vector' }),
    targetUrl: 'https://kenigevents.ru/',
  });
  assert.equal(result.cache_state, 'hit');
  assert.equal(result.search_post_count, 1);
});

test('cache write status remains telemetry and never overrides a valid Search result', async () => {
  for (const cache of ['store_failed', 'skipped', 'other_bounded_status']) {
    const result = await runProductionHealthJourney({
      adapter: fakeJourneyAdapter({ cache }), targetUrl: 'https://kenigevents.ru/',
    });
    assert.equal(result.cache_state, cache);
  }
  const adapter = fakeJourneyAdapter({ cache: 'store_failed' });
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const cell = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
  });
  assert.equal(cell.product_health, 'HEALTHY');
  assert.equal(cell.search.cache_state, 'store_failed');
});

test('production health fails closed when any required sanitized response identity is absent', async () => {
  for (const field of [
    'request_id', 'search_contract_version', 'catalog_revision',
    'corpus_revision', 'search_document_revision',
  ]) {
    await assert.rejects(() => runProductionHealthJourney({
      adapter: fakeJourneyAdapter({ responseTelemetry: { [field]: '' } }),
      targetUrl: 'https://kenigevents.ru/',
    }), /response_identity_invalid/u);
  }
});

test('production journey waits for a terminal response rather than requiring a result card', async () => {
  const adapter = fakeJourneyAdapter({ ids: [] });
  await assert.rejects(() => runProductionHealthJourney({
    adapter, targetUrl: 'https://kenigevents.ru/',
  }), /no_results/u);
  assert.equal(adapter.terminalMinimumCardCount, 0);
});

test('cell classifies terminal zero results separately from renderer and placeholder defects', async () => {
  const run = async (overrides) => {
    const adapter = fakeJourneyAdapter(overrides);
    adapter.preflight = async () => preflight;
    adapter.close = async () => {};
    return runProductionHealthCell({
      platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
      createAdapter: async () => adapter,
      issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
    });
  };
  assert.equal((await run({ ids: [] })).failure_class, 'BROKEN_NO_RESULTS');
  assert.equal((await run({ ids: [], rendererUnavailable: true })).failure_class, 'BROKEN_RESULT_RENDER');
  assert.equal((await run({ ids: [], placeholders: 1 })).failure_class, 'BROKEN_RESULT_RENDER');
});

test('event navigation may reset the page probe after preserving the final Search-page activity', async () => {
  const adapter = fakeJourneyAdapter({ resetActivityOnOpen: true });
  const result = await runProductionHealthJourney({
    adapter,
    targetUrl: 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/',
  });
  assert.equal(result.search_post_count, 1);
  assert.equal(result.event_route.http_status, 200);
  assert.equal(adapter.activityCalls, 2);
});

test('transport instrumentation records one physical Search once when its raw fetch was wrapped first', async () => {
  const originalFetch = globalThis.fetch;
  const originalLocation = globalThis.location;
  const originalDocument = globalThis.document;
  const originalClients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  try {
    globalThis.fetch = async () => new Response(JSON.stringify({
      search_contract_version: 'event_search_v2', search_backend_revision: backendRevision,
      requested_execution_mode: 'cold_vector', actual_execution_mode: 'cold_vector',
      result_cache_status: 'miss', items: [{ event_id: 1 }],
      request_counters: { embedding_provider_attempts: 1, vector_rpc_attempts: 1, llm_provider_attempts: 0 },
    }), { status: 200, headers: { 'content-type': 'application/json', 'content-length': '256' } });
    Object.defineProperty(globalThis, 'location', { configurable: true, value: { href: 'https://kenigevents.ru/poisk/' } });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: { querySelector: () => ({
      dataset: { supabaseUrl: 'https://project.supabase.co', supabaseRelayUrl: '' },
    }) } });
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = new Map();
    installSearchRuntimeProbe({ production_health: true });
    const transport = {
      request(input, init) { return globalThis.fetch(input, init); },
      outcomeHistory: () => [],
    };
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__.set('search', { transport });
    installSearchRuntimeProbe({ production_health: true });
    await transport.request('https://project.supabase.co/functions/v1/event-search', {
      method: 'POST', body: JSON.stringify({ query: 'not retained' }),
    });
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const snapshot = snapshotSearchRuntimeProbe();
      if (snapshot.responses.length === 1 && snapshot.meter.pending_measurements === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    const snapshot = snapshotSearchRuntimeProbe();
    assert.equal(snapshot.requests.length, 1);
    assert.equal(snapshot.responses.length, 1);
    assert.equal(snapshot.meter.categories.edge, 256);
  } finally {
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = originalClients;
    globalThis.fetch = originalFetch;
    Object.defineProperty(globalThis, 'location', { configurable: true, value: originalLocation });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: originalDocument });
  }
});

test('cold resilient route selection meters its physical functions probe without double-counting Search', async () => {
  const originalFetch = globalThis.fetch;
  const originalLocation = globalThis.location;
  const originalDocument = globalThis.document;
  const originalClients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  try {
    globalThis.fetch = async (input) => {
      const url = new URL(String(input));
      if (url.pathname === '/auth/v1/health') throw new Error('expected_losing_probe_abort');
      if (url.pathname === '/functions/v1/transport-probe') {
        return new Response(null, { status: 204, headers: { 'content-length': '128' } });
      }
      if (url.pathname === '/rest/v1/safe_read') {
        const bytes = url.origin === 'https://relay.example' ? 96 : 64;
        return new Response('[]', { status: 200, headers: { 'content-length': String(bytes) } });
      }
      return new Response(JSON.stringify({
        search_contract_version: 'event_search_v2', search_backend_revision: backendRevision,
        requested_execution_mode: 'cold_vector', actual_execution_mode: 'cold_vector',
        result_cache_status: 'miss', items: [{ event_id: 1 }],
        request_counters: { embedding_provider_attempts: 1, vector_rpc_attempts: 1, llm_provider_attempts: 0 },
      }), { status: 200, headers: { 'content-type': 'application/json', 'content-length': '256' } });
    };
    Object.defineProperty(globalThis, 'location', { configurable: true, value: { href: 'https://kenigevents.ru/poisk/' } });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: { querySelector: () => ({
      dataset: { supabaseUrl: 'https://project.supabase.co', supabaseRelayUrl: 'https://relay.example' },
    }) } });
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = new Map();
    installSearchRuntimeProbe({ production_health: true });
    const transport = {
      async request(input, init) {
        try { await this.rawFetch('https://project.supabase.co/auth/v1/health'); } catch { /* losing probe */ }
        await this.rawFetch('https://project.supabase.co/functions/v1/transport-probe', { method: 'HEAD' });
        await this.rawFetch('https://project.supabase.co/rest/v1/safe_read');
        await this.rawFetch('https://relay.example/rest/v1/safe_read');
        return this.rawFetch(input, init);
      },
      rawFetch: globalThis.fetch,
      outcomeHistory: () => [],
    };
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__.set('search', { transport });
    installSearchRuntimeProbe({ production_health: true });
    await transport.request('https://project.supabase.co/functions/v1/event-search', {
      method: 'POST', body: JSON.stringify({ query: 'not retained' }),
    });
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const snapshot = snapshotSearchRuntimeProbe();
      if (snapshot.responses.length === 1 && snapshot.meter.pending_measurements === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    const snapshot = snapshotSearchRuntimeProbe();
    assert.equal(snapshot.requests.length, 1);
    assert.equal(snapshot.responses.length, 1);
    assert.equal(snapshot.meter.categories.edge, 384);
    assert.equal(snapshot.meter.categories.direct_rest, 160);
    assert.equal(snapshot.network.failed_requests, 0);
  } finally {
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = originalClients;
    globalThis.fetch = originalFetch;
    Object.defineProperty(globalThis, 'location', { configurable: true, value: originalLocation });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: originalDocument });
  }
});

test('real ResilientSupabaseTransport transformed Response meters probe and Search exactly once', async () => {
  const { createResilientSupabaseTransport } = await import('../src/lib/resilientSupabaseTransport.ts');
  const originalLocation = globalThis.location;
  const originalDocument = globalThis.document;
  const originalClients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  try {
    const rawFetch = async (input, init = {}) => {
      const url = new URL(String(input));
      if (url.pathname === '/functions/v1/transport-probe') {
        const nonce = JSON.parse(String(init.body || '{}')).nonce;
        return new Response(JSON.stringify({ nonce, schema: 1 }), { status: 200,
          headers: { 'content-type': 'application/json', 'content-length': '128' } });
      }
      return new Response(JSON.stringify({
        schema_version: 'event_search_v2', search_contract_version: 'event_search_v2',
        search_backend_revision: backendRevision,
        request_id: 'real-transport-request', requested_execution_mode: 'cold_vector',
        actual_execution_mode: 'cold_vector', result_cache_status: 'miss',
        catalog_revision: 'a'.repeat(64), corpus_revision: 'b'.repeat(64),
        search_document_revision: 'c'.repeat(64), items: [{ event_id: 1 }],
        request_counters: { embedding_provider_attempts: 1, vector_rpc_attempts: 1, llm_provider_attempts: 0 },
      }), { status: 200, headers: { 'content-type': 'application/json', 'content-length': '512' } });
    };
    const transport = createResilientSupabaseTransport({
      directUrl: 'https://project.supabase.co', publishableKey: 'publishable-test-key',
      fetchImpl: rawFetch, sessionStorage: null, probeStaggerMs: 0,
    });
    Object.defineProperty(globalThis, 'location', { configurable: true, value: { href: 'https://kenigevents.ru/poisk/' } });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: { querySelector: () => ({
      dataset: { supabaseUrl: 'https://project.supabase.co', supabaseRelayUrl: '' },
    }) } });
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = new Map([['real', { transport }]]);
    installSearchRuntimeProbe({ production_health: true });
    const response = await transport.request('https://project.supabase.co/functions/v1/event-search', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: 'never retained', client_request_id: crypto.randomUUID() }),
    });
    assert.equal(response.status, 200);
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const snapshot = snapshotSearchRuntimeProbe();
      if (snapshot.responses.length === 1 && snapshot.meter.pending_measurements === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    const snapshot = snapshotSearchRuntimeProbe();
    assert.equal(snapshot.requests.length, 1);
    assert.equal(snapshot.responses.length, 1);
    assert.equal(snapshot.meter.categories.edge, 640);
  } finally {
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = originalClients;
    Object.defineProperty(globalThis, 'location', { configurable: true, value: originalLocation });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: originalDocument });
  }
});

test('real ResilientSupabaseTransport meters discarded safe-read response and alternate exactly once', async () => {
  const { createResilientSupabaseTransport } = await import('../src/lib/resilientSupabaseTransport.ts');
  const originalLocation = globalThis.location;
  const originalDocument = globalThis.document;
  const originalClients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  try {
    const rawFetch = async (input, init = {}) => {
      const url = new URL(String(input));
      if (url.pathname === '/rest/v1/rpc/transport_probe_v1') {
        const nonce = JSON.parse(String(init.body || '{}')).p_nonce;
        return new Response(JSON.stringify({ nonce, schema: 1 }), { status: 200,
          headers: { 'content-type': 'application/json', 'content-length': '128' } });
      }
      if (url.origin === 'https://project.supabase.co') {
        return new Response(JSON.stringify({ message: 'discarded' }), { status: 503,
          headers: { 'content-type': 'application/json', 'content-length': '256' } });
      }
      return new Response('[]', { status: 200,
        headers: { 'content-type': 'application/json', 'content-length': '512' } });
    };
    const transport = createResilientSupabaseTransport({
      directUrl: 'https://project.supabase.co', relayUrl: 'https://relay.example',
      publishableKey: 'publishable-test-key', fetchImpl: rawFetch,
      sessionStorage: null, probeStaggerMs: 1000,
    });
    Object.defineProperty(globalThis, 'location', { configurable: true, value: { href: 'https://kenigevents.ru/poisk/' } });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: { querySelector: () => ({
      dataset: { supabaseUrl: 'https://project.supabase.co', supabaseRelayUrl: 'https://relay.example' },
    }) } });
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = new Map([['real-safe-read', { transport }]]);
    installSearchRuntimeProbe({ production_health: true });
    const response = await transport.request('https://project.supabase.co/rest/v1/user_saved_event?select=user_id', {
      method: 'GET', headers: { accept: 'application/json' },
    });
    assert.equal(response.status, 200);
    for (let attempt = 0; attempt < 20; attempt += 1) {
      if (snapshotSearchRuntimeProbe().meter.pending_measurements === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    const snapshot = snapshotSearchRuntimeProbe();
    assert.equal(snapshot.meter.categories.direct_rpc, 128);
    assert.equal(snapshot.meter.categories.direct_rest, 768);
  } finally {
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = originalClients;
    Object.defineProperty(globalThis, 'location', { configurable: true, value: originalLocation });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: originalDocument });
  }
});

test('relay-origin receipt/storage traffic is forbidden-counted and metered like direct Supabase', async () => {
  const originalFetch = globalThis.fetch;
  const originalLocation = globalThis.location;
  const originalDocument = globalThis.document;
  const originalClients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  try {
    globalThis.fetch = async () => new Response('{}', { status: 200,
      headers: { 'content-type': 'application/json', 'content-length': '512' } });
    Object.defineProperty(globalThis, 'location', { configurable: true, value: { href: 'https://kenigevents.ru/poisk/' } });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: { querySelector: () => ({
      dataset: { supabaseUrl: 'https://project.supabase.co', supabaseRelayUrl: 'https://relay.example' },
    }) } });
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = new Map();
    installSearchRuntimeProbe({ production_health: true });
    await globalThis.fetch('https://relay.example/storage/v1/object/public/poster');
    await globalThis.fetch('https://relay.example/rest/v1/rpc/get_event_search_receipt_v1');
    for (let attempt = 0; attempt < 20; attempt += 1) {
      if (snapshotSearchRuntimeProbe().meter.pending_measurements === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    const snapshot = snapshotSearchRuntimeProbe();
    assert.equal(snapshot.network.storage_requests, 1);
    assert.equal(snapshot.network.receipt_rpc_requests, 1);
    assert.equal(snapshot.meter.categories.direct_rpc, 512);
  } finally {
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = originalClients;
    globalThis.fetch = originalFetch;
    Object.defineProperty(globalThis, 'location', { configurable: true, value: originalLocation });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: originalDocument });
  }
});

test('production health journey fails closed on duplicates, canary body, LLM, extra RPC/storage and route defects', async () => {
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ postCount: 2 }), targetUrl: 'https://kenigevents.ru/' }), /duplicate_post/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ executionModePresent: true }), targetUrl: 'https://kenigevents.ru/' }), /request_contract_invalid/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ llm: 1 }), targetUrl: 'https://kenigevents.ru/' }), /llm_activity_forbidden/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ ids: ['1', '2', '3', '4', '5', '6'] }), targetUrl: 'https://kenigevents.ru/' }), /result_render_invalid/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ receiptRpcRequests: 1 }), targetUrl: 'https://kenigevents.ru/' }), /receipt_rpc_forbidden/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ storageRequests: 1 }), targetUrl: 'https://kenigevents.ru/' }), /storage_forbidden/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ eventRoute: { same_origin: false, http_status: 200 } }), targetUrl: 'https://kenigevents.ru/' }), /event_route_invalid/u);
});

test('actual event-search request_counters are authoritative over legacy aliases', () => {
  const summary = summarizeSearchPayload({
    requested_execution_mode: 'cold_vector', actual_execution_mode: 'cold_vector',
    request_counters: { embedding_provider_attempts: 2, vector_rpc_attempts: 3, llm_provider_attempts: 0 },
    provider_attempt_counters: { embedding: 99, vector: 99, llm: 99 }, items: [{ event_id: 7 }],
  });
  assert.deepEqual(summary.provider_attempts, { embedding: 2, vector: 3, llm: 0 });
  assert.equal(summary.provider_attempts_present, true);
  assert.equal(summary.provider_attempts_source, 'request_counters');
});

test('meter snapshots merge Auth/Edge/REST/RPC and preserve 48/96 KiB gates', () => {
  const origin = 'https://project.supabase.co';
  const auth = new SupabaseClientObservedByteMeter({ supabaseOrigins: [origin] });
  auth.recordResponse({ url: `${origin}/auth/v1/user`, body: new Uint8Array(10_000) });
  auth.recordResponse({ url: `${origin}/rest/v1/user_saved_event`, body: new Uint8Array(2_000) });
  const journey = new SupabaseClientObservedByteMeter({ supabaseOrigins: [origin] });
  journey.recordResponse({ url: `${origin}/functions/v1/event-search`, body: new Uint8Array(40_000) });
  journey.recordResponse({ url: `${origin}/rest/v1/rpc/quota`, body: new Uint8Array(1_000) });
  const combined = mergeSupabaseClientByteSnapshots(auth.snapshot(), journey.snapshot());
  assert.equal(combined.total_bytes, 53_000);
  assert.equal(combined.target_met, false);
  assert.equal(combined.cost_guard_passed, true);
  assert.deepEqual(combined.categories, { auth: 10_000, edge: 40_000, direct_rest: 2_000, direct_rpc: 1_000 });
  const exceeded = mergeSupabaseClientByteSnapshots(combined, meter(50_000));
  assert.equal(exceeded.hard_limit_exceeded, true);
});

test('cell preflights before issuance, uses one adapter, rereads pointer without retry, and writes allowlisted evidence', async () => {
  const order = [];
  const adapter = fakeJourneyAdapter();
  adapter.preflight = async () => { order.push('preflight'); return preflight; };
  adapter.close = async () => { order.push('adapter_close'); };
  const originalOpen = adapter.open;
  adapter.open = async (...args) => { order.push('journey'); return originalOpen(...args); };
  let resolveCount = 0;
  const targetRun = createAcceptedTargetRun(async () => {
    resolveCount += 1;
    return targetRow('A'.repeat(43), resolveCount === 1 ? {} : { manifest_sha256: 'e'.repeat(64) });
  });
  const root = await mkdtemp(join(tmpdir(), 'search-health-evidence-'));
  try {
    const result = await runProductionHealthCell({
      platform: 'browser', targetRun, evidenceDirectory: root,
      createAdapter: async () => { order.push('adapter_create'); return adapter; },
      issueSession: async () => {
        order.push('issue');
        return {
          authReceipt, meter: meter(4096), attach: async () => { order.push('attach'); },
          cleanup: async () => { order.push('cleanup'); },
        };
      },
    });
    assert.deepEqual(order, ['adapter_create', 'preflight', 'issue', 'attach', 'journey', 'adapter_close', 'cleanup']);
    assert.equal(result.product_health, 'HEALTHY');
    assert.equal(result.execution_status, 'PASS');
    assert.equal(result.target.target_superseded, true);
    assert.equal(resolveCount, 2);
    assert.equal(result.search.physical_post_count, 1);
    assert.equal(result.auth.cleanup_status, 'PASS');
    const raw = await readFile(join(root, 'result.json'), 'utf8');
    assert.equal(raw.includes(PRODUCTION_HEALTH_UI_QUERY), false);
    assert.equal(raw.includes('A'.repeat(43)), false);
    assert.equal(raw.includes('https://'), false);
    assert.equal(raw.includes('raw error'), false);
    const written = JSON.parse(raw);
    assert.equal(written.target.immutable_identity.manifest_sha256, 'c'.repeat(64));
    assert.equal(written.search.expected_backend_revision, null);
    assert.equal(written.search.response.search_backend_revision, backendRevision);
    const summary = JSON.parse(await readFile(join(root, 'qa-summary.json'), 'utf8'));
    assert.equal(summary.search_backend_revision, backendRevision);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('cell merges issued verified Auth bytes with the new-page journey delta', async () => {
  const adapter = fakeJourneyAdapter({ bytes: 2048, pageInitBytes: 1024 });
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const result = await runProductionHealthCell({
    platform: 'android', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({
      authReceipt, meter_cumulative_with_journey: true,
      meterSnapshot: () => meter(4096), attach: async () => {}, cleanup: async () => {},
    }),
  });
  assert.equal(result.execution_status, 'PASS');
  assert.equal(result.supabase_observed_bytes.total_bytes, 7168);
});

test('failed side-effect-free preflight never issues Auth or Search and is not a product incident', async () => {
  let issued = 0;
  const adapter = fakeJourneyAdapter();
  adapter.preflight = async () => ({ ...preflight, search_posts: 1 });
  adapter.close = async () => {};
  const targetRun = createAcceptedTargetRun(async () => targetRow());
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun,
    createAdapter: async () => adapter,
    issueSession: async () => { issued += 1; throw new Error('must_not_run'); },
  });
  assert.equal(issued, 0);
  assert.equal(result.product_health, 'UNCONFIRMED');
  assert.equal(result.execution_status, 'FAILED');
  assert.equal(result.failure_class, 'UNKNOWN_RUNNER_BROWSER');
  assert.equal(result.search.physical_post_count, 0);
});

test('release gate blocks before adapter/Auth/Search', async () => {
  let adapters = 0;
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    releaseGate: async () => false,
    createAdapter: async () => { adapters += 1; return fakeJourneyAdapter(); },
    issueSession: async () => { throw new Error('must_not_run'); },
  });
  assert.equal(adapters, 0);
  assert.equal(result.execution_status, 'BLOCKED');
  assert.equal(result.failure_class, 'BLOCKED_RELEASE_NOT_ACTIVE');
});

test('unavailable initial accepted receipt is retried boundedly and blocks before adapter/Auth/Search', async () => {
  let reads = 0;
  let sleeps = 0;
  let adapters = 0;
  let issued = 0;
  const errors = [
    'search_health_current_review_not_ready',
    'search_health_target_source_invalid',
    'search_health_target_url_invalid',
  ];
  const result = await runProductionHealthCell({
    platform: 'browser',
    targetRun: createAcceptedTargetRun(async () => {
      const message = errors[Math.min(reads, errors.length - 1)];
      reads += 1;
      throw new Error(message);
    }),
    initialTargetMaxAttempts: 3,
    initialTargetDelayMs: 1,
    sleep: async () => { sleeps += 1; },
    createAdapter: async () => { adapters += 1; return fakeJourneyAdapter(); },
    issueSession: async () => { issued += 1; throw new Error('must_not_run'); },
  });
  assert.equal(reads, 3);
  assert.equal(sleeps, 2);
  assert.equal(adapters, 0);
  assert.equal(issued, 0);
  assert.equal(result.product_health, 'UNCONFIRMED');
  assert.equal(result.execution_status, 'BLOCKED');
  assert.equal(result.failure_class, 'BLOCKED_RELEASE_NOT_ACTIVE');
  assert.equal(result.search.physical_post_count, 0);
  assert.equal(result.preflight.search_posts, 0);
  assert.equal(result.auth.get_user_verified, false);
});

test('explicit release wait is bounded and performs only target reads before Auth/Search', async () => {
  let reads = 0;
  let sleeps = 0;
  const expected = 'f'.repeat(40);
  const matched = await resolveExpectedAcceptedTarget({
    resolver: async () => {
      reads += 1;
      const sha = reads < 3 ? 'a'.repeat(40) : expected;
      return normalizeAcceptedTargetResolverResult(targetRow('A'.repeat(43), {
        target_repo_sha: sha, repo_sha: sha,
      }));
    },
    expectedSiteSha: expected,
    maxAttempts: 4,
    delayMs: 1,
    sleep: async () => { sleeps += 1; },
  });
  assert.equal(matched.active, true);
  assert.equal(matched.attempts, 3);
  assert.equal(sleeps, 2);

  reads = 0;
  const blocked = await resolveExpectedAcceptedTarget({
    resolver: async () => { reads += 1; return normalizeAcceptedTargetResolverResult(targetRow()); },
    expectedSiteSha: expected,
    maxAttempts: 2,
    delayMs: 0,
    sleep: async () => {},
  });
  assert.equal(blocked.active, false);
  assert.equal(reads, 2);

  reads = 0;
  sleeps = 0;
  const unavailable = await resolveExpectedAcceptedTarget({
    resolver: async () => {
      reads += 1;
      throw new Error(reads === 1
        ? 'search_health_current_review_not_ready'
        : 'search_health_target_source_invalid');
    },
    maxAttempts: 3,
    delayMs: 1,
    sleep: async () => { sleeps += 1; },
  });
  assert.equal(unavailable.active, false);
  assert.equal(unavailable.target, null);
  assert.equal(unavailable.attempts, 3);
  assert.equal(reads, 3);
  assert.equal(sleeps, 2);
});

test('backend deployment marker is a pre-Auth/Search release receipt and missing identity blocks', async () => {
  assert.equal(hasActiveSearchReleaseReceipt({ siteActive: true }), true);
  assert.equal(hasActiveSearchReleaseReceipt({
    siteActive: true, expectedSearchBackendRevision: backendRevision, deploymentRunId: '',
  }), false);
  assert.equal(hasActiveSearchReleaseReceipt({
    siteActive: true, expectedSearchBackendRevision: backendRevision, deploymentRunId: 'deploy-42.1',
    backendActive: true,
  }), true);
  assert.equal(hasActiveSearchReleaseReceipt({
    siteActive: false, expectedSearchBackendRevision: backendRevision, deploymentRunId: 'deploy-42.1',
  }), false);

  let issued = 0;
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    releaseGate: async () => hasActiveSearchReleaseReceipt({
      siteActive: true, expectedSearchBackendRevision: backendRevision, deploymentRunId: '',
    }),
    expectedSearchBackendRevision: backendRevision,
    createAdapter: async () => fakeJourneyAdapter(),
    issueSession: async () => { issued += 1; throw new Error('must_not_run'); },
  });
  assert.equal(issued, 0);
  assert.equal(result.execution_status, 'BLOCKED');
  assert.equal(result.failure_class, 'BLOCKED_RELEASE_NOT_ACTIVE');
  assert.equal(result.search.physical_post_count, 0);
});

test('backend contract probe is a bounded HEAD-only pre-Auth/Search gate', async () => {
  const calls = [];
  let sleeps = 0;
  const receipt = await waitForActiveSearchBackend({
    supabaseUrl: 'https://project.supabase.co', publishableKey: 'publishable-test-key',
    expectedRevision: backendRevision, attempts: 3, delayMs: 1,
    sleep: async () => { sleeps += 1; },
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      return { status: calls.length === 3 ? 200 : 503, headers: new Headers({
        'x-kenigevents-search-revision': calls.length === 3 ? backendRevision : `sha256:${'0'.repeat(64)}`,
        'x-kenigevents-search-contract': 'event-search-contract-v2',
      }) };
    },
  });
  assert.equal(receipt.active, true);
  assert.equal(receipt.observed_revision, backendRevision);
  assert.equal(receipt.observed_contract_version, 'event-search-contract-v2');
  assert.equal(receipt.product_search_posts, 0);
  assert.equal(receipt.auth_requests, 0);
  assert.equal(sleeps, 2);
  assert.equal(calls.length, 3);
  assert.ok(calls.every(({ url, init }) => url.endsWith('/functions/v1/event-search')
    && init.method === 'HEAD' && init.redirect === 'error'));
});

test('backend release probe never retains unclosed response headers', async () => {
  const receipt = await waitForActiveSearchBackend({
    supabaseUrl: 'https://project.supabase.co', publishableKey: 'publishable-test-key',
    expectedRevision: backendRevision, attempts: 1,
    fetchImpl: async () => ({ status: 503, headers: {
      get: (name) => name === 'x-kenigevents-search-revision'
        ? 'unsafe revision value' : 'unsafe contract value',
    } }),
  });
  assert.equal(receipt.active, false);
  assert.equal(receipt.observed_revision, null);
  assert.equal(receipt.observed_contract_version, null);
});

test('HEAD match followed by an Edge revision change is blocked from retained response without retry', async () => {
  const changedRevision = `sha256:${'e'.repeat(64)}`;
  const headCalls = [];
  const adapter = fakeJourneyAdapter({
    rendererUnavailable: true,
    responseTelemetry: { search_backend_revision: changedRevision },
  });
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    expectedSearchBackendRevision: backendRevision,
    releaseGate: async () => (await waitForActiveSearchBackend({
      supabaseUrl: 'https://project.supabase.co', publishableKey: 'publishable-test-key',
      expectedRevision: backendRevision, attempts: 1,
      fetchImpl: async (url, init) => {
        headCalls.push({ url, init });
        return { status: 200, headers: new Headers({
          'x-kenigevents-search-revision': backendRevision,
          'x-kenigevents-search-contract': 'event-search-contract-v2',
        }) };
      },
    })).active,
    createAdapter: async () => adapter,
    issueSession: async () => ({
      authReceipt, meter: meter(4096), attach: async () => {}, cleanup: async () => {},
    }),
  });
  assert.equal(headCalls.length, 1);
  assert.equal(adapter.submitCalls, 1);
  assert.equal(result.search.physical_post_count, 1);
  assert.equal(result.search.response.search_backend_revision, changedRevision);
  assert.equal(result.execution_status, 'BLOCKED');
  assert.equal(result.failure_class, 'BLOCKED_RELEASE_NOT_ACTIVE');
  assert.equal(result.product_health, 'UNCONFIRMED');
});

test('HEAD match followed by an Edge error without a valid revision remains a product request failure', async () => {
  const headCalls = [];
  const adapter = fakeJourneyAdapter({
    responseTelemetry: { http_status: 502, search_backend_revision: '' },
  });
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    expectedSearchBackendRevision: backendRevision,
    releaseGate: async () => (await waitForActiveSearchBackend({
      supabaseUrl: 'https://project.supabase.co', publishableKey: 'publishable-test-key',
      expectedRevision: backendRevision, attempts: 1,
      fetchImpl: async (url, init) => {
        headCalls.push({ url, init });
        return { status: 200, headers: new Headers({
          'x-kenigevents-search-revision': backendRevision,
          'x-kenigevents-search-contract': 'event-search-contract-v2',
        }) };
      },
    })).active,
    createAdapter: async () => adapter,
    issueSession: async () => ({
      authReceipt, meter: meter(4096), attach: async () => {}, cleanup: async () => {},
    }),
  });
  assert.equal(headCalls.length, 1);
  assert.equal(adapter.submitCalls, 1);
  assert.equal(result.search.physical_post_count, 1);
  assert.equal(result.search.response.search_backend_revision, null);
  assert.equal(result.execution_status, 'FAILED');
  assert.equal(result.failure_class, 'BROKEN_SEARCH_REQUEST');
  assert.equal(result.product_health, 'BROKEN');
});

test('one physical Search POST is enforced through scroll and event navigation', async () => {
  await assert.rejects(() => runProductionHealthJourney({
    adapter: fakeJourneyAdapter({ postAfterOpen: true }),
    targetUrl: 'https://kenigevents.ru/',
  }), /post_navigation_search_forbidden/u);
});

test('a late Search POST after event navigation completion still fails the whole-cell proof', async () => {
  await assert.rejects(() => runProductionHealthJourney({
    adapter: fakeJourneyAdapter({ latePostAfterNavigation: true }),
    targetUrl: 'https://kenigevents.ru/',
  }), /post_navigation_search_forbidden/u);
});

test('callback/session attach failure is Auth integration, not broker infrastructure', async () => {
  const adapter = fakeJourneyAdapter();
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => { throw new Error('session_restore_failed'); }, cleanup: async () => {} }),
  });
  assert.equal(result.failure_class, 'BROKEN_AUTH_INTEGRATION');
});

test('mobile preflight failure preserves only a closed Appium diagnostic receipt', async () => {
  const receipt = {
    schema_version: 'mobile-preflight-failure-v1', failure_stage: 'webdriver_session_create',
    error_class: 'webdriver_session_error', appium_server_ready: true, elapsed_ms: 91_000,
    log_inspected: true, chromedriver_discovery_attempted: true, chromedriver_missing: true,
    chromedriver_download_failed: false, chrome_session_failed: false,
    web_context_failed: false, uiautomator_server_failed: false,
  };
  const error = new Error('webdriver_session_error:raw secret');
  error.searchReceipt = receipt;
  const result = await runProductionHealthCell({
    platform: 'android', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => { throw error; },
    issueSession: async () => { throw new Error('issuance_forbidden'); },
  });
  assert.equal(result.failure_class, 'UNKNOWN_ANDROID_INFRA');
  assert.equal(result.preflight.error_class, 'webdriver_session_error');
  assert.equal(result.preflight.appium_server_ready, true);
  assert.equal(result.preflight.chromedriver_missing, true);
  assert.doesNotMatch(JSON.stringify(result), /raw secret/u);
});

test('cleanup failure prevents a healthy PASS and leaves closed failure evidence', async () => {
  const adapter = fakeJourneyAdapter();
  adapter.preflight = async () => preflight;
  adapter.close = async () => { throw new Error('delete_session_failed'); };
  const result = await runProductionHealthCell({
    platform: 'android', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
  });
  assert.equal(result.product_health, 'UNCONFIRMED');
  assert.equal(result.execution_status, 'FAILED');
  assert.equal(result.failure_class, 'UNKNOWN_ANDROID_INFRA');
  assert.equal(result.auth.cleanup_status, 'FAIL');
});

test('failed journey retains physical POST, meter and supersession evidence', async () => {
  const adapter = fakeJourneyAdapter({ skeletons: 1, bytes: 35_000 });
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  let reads = 0;
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => {
      reads += 1;
      return targetRow(reads === 1 ? 'A'.repeat(43) : 'B'.repeat(43), reads === 1 ? {} : {
        manifest_sha256: 'e'.repeat(64), token_sha256: createHash('sha256').update('B'.repeat(43)).digest('hex'),
      });
    }),
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
  });
  assert.equal(result.failure_class, 'BROKEN_RESULT_RENDER');
  assert.equal(result.search.physical_post_count, 1);
  assert.equal(result.supabase_observed_bytes.total_bytes, 35_000);
  assert.equal(result.target.target_superseded, true);
  assert.equal(reads, 2);
});

test('post-preflight Appium log/session loss is platform infrastructure, never product BROKEN', async () => {
  for (const [platform, message, expected] of [
    ['android', 'mobile_health_diagnostics_unavailable', 'UNKNOWN_ANDROID_INFRA'],
    ['ios', 'invalid session id', 'UNKNOWN_IOS_INFRA'],
    ['android', 'mobile_auth_terminal_bytes_timeout', 'UNKNOWN_ANDROID_INFRA'],
    ['android', 'mobile_android_cdp_route_unavailable', 'UNKNOWN_ANDROID_INFRA'],
    ['ios', 'mobile_post_navigation_terminal_bytes_missing', 'UNKNOWN_IOS_INFRA'],
    ['browser', 'search_post_navigation_meter_failed', 'UNKNOWN_RUNNER_BROWSER'],
    ['browser', 'search_post_navigation_meter_origin_missing', 'UNKNOWN_RUNNER_BROWSER'],
    ['browser', 'search_post_navigation_meter_missing', 'UNKNOWN_RUNNER_BROWSER'],
    ['browser', 'search_post_navigation_observation_missing', 'UNKNOWN_RUNNER_BROWSER'],
    ['ios', 'search_physical_observation_missing', 'UNKNOWN_IOS_INFRA'],
  ]) {
    const adapter = fakeJourneyAdapter({
      finalDiagnosticsFailure: true, finalDiagnosticsError: message, resetActivityOnOpen: true,
    });
    adapter.preflight = async () => preflight;
    adapter.close = async () => {};
    const result = await runProductionHealthCell({
      platform, targetRun: createAcceptedTargetRun(async () => targetRow()),
      createAdapter: async () => adapter,
      issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
    });
    assert.equal(result.failure_class, expected);
    assert.equal(result.product_health, 'UNCONFIRMED');
    assert.equal(result.search.physical_post_count, 1);
  }
});

test('post-navigation diagnostic failure retains the pre-navigation Search response and meter', async () => {
  const adapter = fakeJourneyAdapter({ finalDiagnosticsFailure: true, resetActivityOnOpen: true, bytes: 36_000 });
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
  });
  assert.equal(result.failure_class, 'UNKNOWN_RUNNER_BROWSER');
  assert.equal(result.search.physical_post_count, 1);
  assert.equal(result.search.response.request_id, 'request-1');
  assert.equal(result.search.response_id_count, 2);
  assert.equal(result.search.rendered_id_count, 2);
  assert.equal(result.supabase_observed_bytes.total_bytes, 36_000);
});

test('late post-navigation Search failure retains total physical count two and original response', async () => {
  const adapter = fakeJourneyAdapter({ latePostAfterNavigation: true, resetActivityOnOpen: true, bytes: 37_000 });
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
  });
  assert.equal(result.search.physical_post_count, 2);
  assert.equal(result.search.response.request_id, 'request-1');
  assert.equal(result.search.response_id_count, 2);
  assert.equal(result.search.rendered_id_count, 2);
  assert.equal(result.supabase_observed_bytes.total_bytes, 37_000);
});

test('failed cell drains final diagnostics before sampling authoritative mobile physical activity', async () => {
  const adapter = fakeJourneyAdapter({ bytes: 38_000 });
  let diagnosticsDrained = false;
  let diagnosticsCalls = 0;
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  adapter.snapshotResults = async () => { throw new Error('search_health_result_render_failed'); };
  adapter.failedJourneyEvidence = async () => ({
    activity: {
      requests: [{ method: 'POST', path: '/functions/v1/event-search', body_contract: {
        limit: 5, use_llm_verifier: false, allow_llm_fallback: false,
      } }],
      responses: [{ request_id: 'request-1', http_status: 200, route: 'direct',
        response_ids: ['101', '102'], provider_attempts: { embedding: 1, vector: 1, llm: 0 } }],
      meter: meter(38_000),
    },
    results: { terminal: true, rendered_ids: ['101', '102'] },
    post_navigation_search_post_count: 0,
  });
  adapter.healthDiagnostics = async () => {
    diagnosticsCalls += 1;
    if (diagnosticsCalls >= 2) diagnosticsDrained = true;
    return { console_errors: 0, failed_requests: 0, error_responses: 0 };
  };
  adapter.physicalActivity = async () => ({
    search_posts: diagnosticsDrained ? 2 : 0,
    storage_requests: 0, receipt_rpc_requests: 0, meter: meter(38_000),
  });
  const result = await runProductionHealthCell({
    platform: 'android', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
  });
  assert.equal(diagnosticsDrained, true);
  assert.equal(result.search.physical_post_count, 2);
});

test('Auth traffic over the hard cap stops before the single Search and stays unconfirmed', async () => {
  let opened = 0;
  const adapter = fakeJourneyAdapter();
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const originalOpen = adapter.open;
  adapter.open = async (...args) => { opened += 1; return originalOpen(...args); };
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    createAdapter: async () => adapter,
    issueSession: async () => ({
      authReceipt,
      meter: meter(SUPABASE_CLIENT_BYTE_HARD_LIMIT + 1),
      attach: async () => {}, cleanup: async () => {},
    }),
  });
  assert.equal(opened, 0);
  assert.equal(result.product_health, 'UNCONFIRMED');
  assert.equal(result.execution_status, 'FAILED');
  assert.equal(result.failure_class, 'COST_GUARD_FAILED');
  assert.equal(result.search.physical_post_count, 0);
});

test('target normalizer keeps secret navigation private and exposes exact immutable evidence only', () => {
  const target = normalizeAcceptedTargetResolverResult(targetRow());
  assert.equal(target.navigationUrl().includes('A'.repeat(43)), true);
  assert.equal(JSON.stringify(target).includes('A'.repeat(43)), false);
  assert.deepEqual(Object.keys(target.immutable_identity), [
    'build_id', 'run_id', 'repo_sha', 'snapshot_id', 'result_sha256',
    'manifest_sha256', 'token_sha256', 'input_fingerprint',
  ]);
});

test('built-in mobile hook needs no email persona and verifies Auth only after callback attach', async () => {
  const calls = [];
  const adapter = { verifyAuthenticatedOwner: async () => ({ receipt: authReceipt, meter: meter(1234) }) };
  const adapterModule = {
    createAndroidSearchAdapter(options) { calls.push(['adapter', options.path]); return adapter; },
  };
  const fixtureModule = {
    createAuthSessionBrokerIssuer() {
      return { async issue(input) {
        calls.push(['issue', input.purpose, input.personaId, input.platform]);
        return { actionLink: 'https://project.supabase.co/auth/v1/verify?token=opaque', emailOtp: '123456' };
      } };
    },
    createBrowserVerificationCallback() { calls.push(['callback']); return 'https://kenigevents.ru/auth/callback'; },
  };
  const hooks = await createBuiltInMobileHooks({
    AUTH_SESSION_BROKER_URL: 'https://broker.example/issue', GITHUB_RUN_ID: '42',
  }, 'android', { adapterModule, fixtureModule, oidcToken: 'oidc' });
  assert.equal(await hooks.createAdapter(), adapter);
  const issued = await hooks.issueSession({ target: normalizeAcceptedTargetResolverResult(targetRow()) });
  assert.equal(issued.actionLink, 'https://kenigevents.ru/auth/callback');
  const verified = await issued.verifyAuth(adapter);
  assert.equal(verified.receipt.get_user_verified, true);
  assert.equal(issued.meterSnapshot().total_bytes, 1234);
  assert.deepEqual(calls, [
    ['adapter', '/wd/hub'],
    ['issue', 'production_health', 'search-cached-android', 'android'],
    ['callback'],
  ]);
});

test('built-in browser hook binds the real owner probe into fixture issuance', async () => {
  const calls = [];
  const adapter = {};
  const adapterModule = {
    createPlaywrightSearchAdapter(options) { calls.push(['adapter', options.productionHealth]); return adapter; },
  };
  const fixtureModule = {
    createAuthSessionBrokerIssuer(options) {
      calls.push(['issuer', options.oidcToken]);
      return { kind: 'github_oidc_broker', issue: async () => ({}) };
    },
    async createAuthSessionFixture(options) {
      calls.push(['fixture', options.purpose, options.platform, options.personaId,
        typeof options.protectedProbe]);
      return { receipt: authReceipt, storageStatePath: '/tmp/ephemeral-state', cleanup: async () => {} };
    },
  };
  const observed = new SupabaseClientObservedByteMeter({ supabaseOrigins: ['https://project.supabase.co'] });
  const hooks = await createBuiltInBrowserHooks({
    PERSONALIZATION_SUPABASE_URL: 'https://project.supabase.co',
    PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: 'publishable',
    AUTH_SESSION_BROKER_URL: 'https://broker.example/issue',
    SEARCH_E2E_PERSONA_EMAIL_CACHED_BROWSER: 'fixture@example.invalid',
    E2E_SEARCH_HEALTH_MODE: 'production_health',
    GITHUB_RUN_ID: '42',
  }, observed, {
    adapterModule, fixtureModule, oidcToken: 'verified-oidc',
    fetchImpl: async () => new Response('[]', { status: 200 }),
  });
  assert.equal(await hooks.createAdapter(), adapter);
  const issued = await hooks.issueSession({
    platform: 'browser', target: normalizeAcceptedTargetResolverResult(targetRow()),
  });
  assert.equal(issued.authReceipt.get_user_verified, true);
  assert.deepEqual(calls, [
    ['adapter', true], ['issuer', 'verified-oidc'],
    ['fixture', 'production_health', 'browser', 'search-cached-browser', 'function'],
  ]);
});

test('release qualification browser hook uses the distinct cold persona', async () => {
  let fixtureOptions;
  const hooks = await createBuiltInBrowserHooks({
    PERSONALIZATION_SUPABASE_URL: 'https://project.supabase.co',
    PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: 'publishable',
    AUTH_SESSION_BROKER_URL: 'https://broker.example/issue',
    SEARCH_E2E_PERSONA_EMAIL_COLD_BROWSER: 'cold@example.invalid',
    E2E_SEARCH_HEALTH_MODE: 'release_qualification',
    GITHUB_RUN_ID: '42',
  }, new SupabaseClientObservedByteMeter({ supabaseOrigins: ['https://project.supabase.co'] }), {
    adapterModule: { createPlaywrightSearchAdapter: () => ({}) },
    fixtureModule: {
      createAuthSessionBrokerIssuer: () => ({ kind: 'github_oidc_broker', issue: async () => ({}) }),
      async createAuthSessionFixture(options) {
        fixtureOptions = options;
        return { receipt: authReceipt, storageStatePath: '/tmp/ephemeral-state', cleanup: async () => {} };
      },
    },
    oidcToken: 'verified-oidc', fetchImpl: async () => new Response('[]', { status: 200 }),
  });
  await hooks.issueSession({
    platform: 'browser', target: normalizeAcceptedTargetResolverResult(targetRow()),
  });
  assert.equal(fixtureOptions.purpose, 'release_qualification');
  assert.equal(fixtureOptions.personaId, 'search-cold-browser');
  assert.deepEqual(fixtureOptions.personas,
    { 'search-cold-browser': { email: 'cold@example.invalid' } });
});

test('authenticated owner runtime proof uses resilient get-user plus exactly one owner RLS read without returning identity', async () => {
  const originalDocument = globalThis.document;
  const originalStorage = globalThis.localStorage;
  const originalClients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  const requests = [];
  const supabaseUrl = 'https://project.supabase.co';
  const userId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
  try {
    Object.defineProperty(globalThis, 'document', { configurable: true, value: {
      querySelector: () => ({ dataset: { supabaseUrl, supabaseKey: 'publishable' } }),
    } });
    Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: {
      getItem: () => JSON.stringify({ access_token: 'secret-token' }),
    } });
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = new Map([['client', {
      key: `${supabaseUrl}|relay|key`,
      async request(input, init) {
        const parsed = new URL(String(input));
        requests.push({ path: parsed.pathname, method: init.method, owner: parsed.searchParams.get('user_id') });
        return new Response(JSON.stringify(parsed.pathname === '/auth/v1/user'
          ? { id: userId } : [{ user_id: userId }]), { status: 200, headers: { 'content-type': 'application/json' } });
      },
    }]]);
    const receipt = await verifyAuthenticatedOwnerRuntimeProbe();
    assert.equal(receipt.get_user_verified, true);
    assert.equal(receipt.protected_probe_request_count, 1);
    assert.deepEqual(requests, [
      { path: '/auth/v1/user', method: 'GET', owner: null },
      { path: '/rest/v1/user_saved_event', method: 'GET', owner: `eq.${userId}` },
    ]);
    assert.equal(JSON.stringify(receipt).includes(userId), false);
    assert.equal(JSON.stringify(receipt).includes('secret-token'), false);
  } finally {
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = originalClients;
    Object.defineProperty(globalThis, 'document', { configurable: true, value: originalDocument });
    Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: originalStorage });
  }
});


test('Playwright snapshot detects real skeleton markup and id-less placeholders', () => {
  const resultNode = { hidden: false, querySelector: () => null };
  const event = { getAttribute: (name) => name === 'data-event-id' ? '42' : '',
    getBoundingClientRect: () => ({ width: 10, height: 10, top: 0, bottom: 10 }) };
  const placeholder = { getAttribute: () => '',
    getBoundingClientRect: () => ({ width: 10, height: 10, top: 0, bottom: 10 }) };
  const prior = {
    document: Object.getOwnPropertyDescriptor(globalThis, 'document'),
    getComputedStyle: Object.getOwnPropertyDescriptor(globalThis, 'getComputedStyle'),
    innerHeight: Object.getOwnPropertyDescriptor(globalThis, 'innerHeight'),
  };
  Object.defineProperty(globalThis, 'document', { configurable: true, writable: true, value: {
    querySelector(selector) {
      if (selector === '[data-authorized-search]') return { dataset: { searchTerminal: 'true' } };
      if (selector === '[data-search-results]') return resultNode;
      if (selector === '[data-search-status]') return { getAttribute: () => null };
      if (selector === '[data-search-submit]') return { getAttribute: () => 'false' };
      return null;
    },
    querySelectorAll(selector) {
      if (selector.includes('.authorized-search__skeleton-card')) return [{}];
      if (selector.includes('[data-event-card][data-event-id]')) return [event];
      if (selector.includes('[data-event-card],')) return [event, placeholder];
      return [];
    },
  } });
  Object.defineProperty(globalThis, 'getComputedStyle', { configurable: true, writable: true,
    value: () => ({ display: 'block', visibility: 'visible' }) });
  Object.defineProperty(globalThis, 'innerHeight', { configurable: true, writable: true, value: 800 });
  try {
    const snapshot = snapshotResultsInPage();
    assert.equal(snapshot.skeleton_count, 1);
    assert.equal(snapshot.placeholder_count, 1);
  } finally {
    for (const [name, descriptor] of Object.entries(prior)) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor);
      else delete globalThis[name];
    }
  }
});

test('Playwright DOM snapshot treats terminal zero-results and renderer-unavailable states as complete', () => {
  const prior = {
    document: Object.getOwnPropertyDescriptor(globalThis, 'document'),
    getComputedStyle: Object.getOwnPropertyDescriptor(globalThis, 'getComputedStyle'),
    innerHeight: Object.getOwnPropertyDescriptor(globalThis, 'innerHeight'),
  };
  const resultNode = { hidden: false, querySelector: (selector) => (
    selector === '[data-search-card-render-unavailable]' ? {} : null
  ) };
  Object.defineProperty(globalThis, 'document', { configurable: true, value: {
    querySelector(selector) {
      if (selector === '[data-authorized-search]') return { dataset: { searchTerminal: 'true' } };
      if (selector === '[data-search-results]') return resultNode;
      if (selector === '[data-search-status]') return { getAttribute: () => 'alert' };
      if (selector === '[data-search-submit]') return { getAttribute: () => 'false' };
      return null;
    },
    querySelectorAll: () => [],
  } });
  Object.defineProperty(globalThis, 'getComputedStyle', { configurable: true, value: () => ({ display: 'block', visibility: 'visible' }) });
  Object.defineProperty(globalThis, 'innerHeight', { configurable: true, value: 800 });
  try {
    const snapshot = snapshotResultsInPage();
    assert.equal(snapshot.terminal, true);
    assert.equal(snapshot.rendered_ids.length, 0);
    assert.equal(snapshot.card_renderer_unavailable, true);
  } finally {
    for (const [name, descriptor] of Object.entries(prior)) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor);
      else delete globalThis[name];
    }
  }
});

test('Playwright target open rejects an otherwise same-final-URL redirect chain', async () => {
  const page = {
    on() {}, viewportSize: () => ({ width: 1280, height: 720 }),
    url: () => 'https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/',
    async goto() {
      return { status: () => 200, request: () => ({ redirectedFrom: () => ({}) }) };
    },
  };
  const adapter = await createPlaywrightSearchAdapter({ page, productionHealth: true });
  await assert.rejects(
    () => adapter.open('https://kenigevents.ru/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/'),
    /search_target_redirected/u,
  );
});

test('Playwright physical observer waits for delayed page-init bytes before pre-submit gate', async () => {
  const listeners = new Map();
  const page = {
    on(name, listener) { const values = listeners.get(name) || new Set(); values.add(listener); listeners.set(name, values); },
    off(name, listener) { listeners.get(name)?.delete(listener); },
    viewportSize: () => ({ width: 1280, height: 720 }), url: () => 'about:blank',
    async waitForTimeout(ms) { await new Promise((resolve) => setTimeout(resolve, ms)); },
  };
  const adapter = await createPlaywrightSearchAdapter({ page, productionHealth: true,
    supabaseOrigins: ['https://project.supabase.co'], physicalQuietMs: 30 });
  setTimeout(() => {
    const request = { method: () => 'GET', url: () => 'https://project.supabase.co/auth/v1/user' };
    for (const listener of listeners.get('request') || []) listener(request);
    for (const listener of listeners.get('response') || []) listener({
      url: () => 'https://project.supabase.co/auth/v1/user', status: () => 200,
      allHeaders: async () => ({ 'content-length': String(SUPABASE_CLIENT_BYTE_HARD_LIMIT + 1) }),
      body: async () => { throw new Error('body must not be fetched'); },
    });
    for (const listener of listeners.get('requestfinished') || []) listener(request);
  }, 10);
  await adapter.awaitPhysicalIdle();
  const activity = await adapter.physicalActivity();
  assert.equal(activity.search_posts, 0);
  assert.equal(activity.meter.hard_limit_exceeded, true);
});

test('Playwright diagnostics ignore failed decorative subresources but retain document and Supabase failures', async () => {
  const listeners = new Map();
  const page = {
    on(name, listener) { const values = listeners.get(name) || new Set(); values.add(listener); listeners.set(name, values); },
    off(name, listener) { listeners.get(name)?.delete(listener); },
    viewportSize: () => ({ width: 1280, height: 720 }), url: () => 'about:blank',
  };
  const adapter = await createPlaywrightSearchAdapter({
    page, productionHealth: true, supabaseOrigins: ['https://project.supabase.co'],
  });
  const fail = (url, resourceType) => {
    const request = { url: () => url, resourceType: () => resourceType };
    for (const listener of listeners.get('requestfailed') || []) listener(request);
  };
  fail('https://images.example.invalid/card.webp', 'image');
  fail('https://project.supabase.co/functions/v1/transport-probe', 'fetch');
  fail('https://project.supabase.co/rest/v1/rpc/transport_probe_v1', 'fetch');
  assert.equal((await adapter.healthDiagnostics()).failed_requests, 0);
  fail('https://project.supabase.co/functions/v1/event-search', 'fetch');
  fail('https://kenigevents.ru/_review/token/poisk/', 'document');
  const diagnostics = await adapter.healthDiagnostics();
  assert.equal(diagnostics.failed_requests, 2);
  assert.equal(diagnostics.failed_edge_requests, 1);
  assert.equal(diagnostics.failed_document_requests, 1);
  assert.equal(diagnostics.failed_auth_requests, 0);
  assert.equal(diagnostics.failed_rest_requests, 0);
  assert.equal(diagnostics.failed_rpc_requests, 0);
});

test('Playwright card open preserves Search activity and independently counts later Search POSTs', async () => {
  let currentUrl = 'https://kenigevents.ru/poisk/';
  const listeners = new Map();
  const emitRequest = (url, method = 'GET') => {
    const request = { method: () => method, url: () => url };
    for (const listener of listeners.get('request') || []) listener(request);
    for (const listener of listeners.get('requestfinished') || []) listener(request);
  };
  const searchActivity = {
    requests: [{ method: 'POST', path: '/functions/v1/event-search' }],
    responses: [{}], routes: [{}], meter: meter(),
  };
  const link = {
    first() { return this; },
    async count() { return 1; },
    async getAttribute() { return '/sobytiya/42/'; },
    async click() {
      emitRequest('https://project.supabase.co/functions/v1/event-search?not-retained=yes', 'POST');
      currentUrl = 'https://kenigevents.ru/sobytiya/42/';
    },
  };
  const first = {
    first() { return this; }, async waitFor() {},
    locator() { return link; },
  };
  const page = {
    on(name, listener) {
      const values = listeners.get(name) || new Set();
      values.add(listener);
      listeners.set(name, values);
    },
    off(name, listener) { listeners.get(name)?.delete(listener); },
    viewportSize: () => ({ width: 1280, height: 720 }),
    url: () => currentUrl,
    locator: () => first,
    async evaluate(fn) {
      if (String(fn).includes('supabaseRelayUrl')) return ['https://project.supabase.co'];
      return structuredClone(searchActivity);
    },
    async waitForNavigation() {
      return {
        status: () => 200,
        request: () => ({
          url: () => 'https://kenigevents.ru/sobytiya/42/', redirectedFrom: () => null,
        }),
      };
    },
    async waitForTimeout(ms) { await new Promise((resolve) => setTimeout(resolve, ms)); },
  };
  const adapter = await createPlaywrightSearchAdapter({
    page, productionHealth: true, postNavigationQuietMs: 30,
  });
  const receipt = await adapter.openFirstResult();
  assert.deepEqual(receipt.search_page_activity_before_navigation, searchActivity);
  assert.equal(receipt.same_origin, true);
  assert.equal(receipt.http_status, 200);
  assert.equal(listeners.get('request')?.size || 0, 2);
  emitRequest('https://project.supabase.co/functions/v1/event-search?late=yes', 'POST');
  for (const listener of listeners.get('response') || []) {
    listener({
      url: () => 'https://project.supabase.co/rest/v1/user_saved_event',
      status: () => 200,
      allHeaders: async () => ({}),
      body: async () => Buffer.alloc(4096),
    });
  }
  setTimeout(() => {
    emitRequest('https://project.supabase.co/auth/v1/user');
    for (const listener of listeners.get('response') || []) {
      listener({
        url: () => 'https://project.supabase.co/auth/v1/user', status: () => 200,
        allHeaders: async () => ({ 'content-length': '1024' }), body: async () => {
          throw new Error('body must not be fetched with content-length');
        },
      });
    }
  }, 10);
  assert.equal(await adapter.postNavigationSearchPostCount(), 2);
  const postNavigationMeter = await adapter.postNavigationMeterSnapshot();
  assert.equal(postNavigationMeter.total_bytes, 5120);
  assert.equal(postNavigationMeter.categories.direct_rest, 4096);
  assert.equal(postNavigationMeter.categories.auth, 1024);
  assert.equal(listeners.get('request')?.size || 0, 1);
  assert.equal(listeners.get('response')?.size || 0, 1); // one permanent diagnostics listener remains
  assert.doesNotMatch(JSON.stringify(receipt), /not-retained/u);
});

test('Playwright failed evidence preserves the pre-navigation snapshot when meter finalization fails', async () => {
  let currentUrl = 'https://kenigevents.ru/poisk/';
  const listeners = new Map();
  const activity = { requests: [{ method: 'POST', path: '/functions/v1/event-search' }],
    responses: [{ request_id: 'request-before-crash' }], routes: [{}], meter: meter(2048) };
  const link = { first() { return this; }, async count() { return 1; },
    async getAttribute() { return '/sobytiya/42/'; }, async click() { currentUrl = 'https://kenigevents.ru/sobytiya/42/'; } };
  const first = { first() { return this; }, async waitFor() {}, locator() { return link; } };
  const page = {
    on(name, listener) { const values = listeners.get(name) || new Set(); values.add(listener); listeners.set(name, values); },
    off(name, listener) { listeners.get(name)?.delete(listener); },
    viewportSize: () => ({ width: 1280, height: 720 }), url: () => currentUrl,
    locator: () => first,
    async evaluate(fn) {
      if (String(fn).includes('supabaseRelayUrl')) return ['https://project.supabase.co'];
      return structuredClone(activity);
    },
    async waitForNavigation() { return { status: () => 200, request: () => ({
      url: () => 'https://kenigevents.ru/sobytiya/42/', redirectedFrom: () => null,
    }) }; },
    async waitForTimeout() { throw new Error('browser closed'); },
  };
  const adapter = await createPlaywrightSearchAdapter({ page, productionHealth: true, postNavigationQuietMs: 30 });
  await adapter.openFirstResult();
  const retained = await adapter.failedJourneyEvidence();
  assert.equal(retained.activity.responses[0].request_id, 'request-before-crash');
  assert.equal(retained.post_navigation_search_post_count, 0);
  assert.equal(retained.post_navigation_meter, undefined);
});

test('Playwright rejects homepage and non-event same-origin 200 card destinations before click', async () => {
  for (const href of ['/', '/mesta/example/']) {
    let clicked = false;
    const link = {
      first() { return this; }, async count() { return 1; },
      async getAttribute() { return href; },
      async click() { clicked = true; },
    };
    const first = {
      first() { return this; }, async waitFor() {}, locator() { return link; },
    };
    const page = {
      on() {}, off() {}, viewportSize: () => ({ width: 1280, height: 720 }),
      url: () => 'https://kenigevents.ru/poisk/', locator: () => first,
    };
    const adapter = await createPlaywrightSearchAdapter({ page, productionHealth: true });
    await assert.rejects(() => adapter.openFirstResult(), /event_path|candidate_prefix/u);
    assert.equal(clicked, false);
  }
});
