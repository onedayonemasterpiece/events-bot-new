import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { summarizeSearchPayload } from '../e2e/search/acceptance.mjs';
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
  resolveExpectedAcceptedTarget,
  runProductionHealthCell,
} from '../e2e/search/production-health-run.mjs';
import { createBuiltInMobileHooks } from '../e2e/search/production-health-run.mjs';
import { verifyAuthenticatedOwnerRuntimeProbe } from '../e2e/search/adapters/runtime-probe.mjs';
import {
  createAcceptedTargetRun,
  normalizeAcceptedTargetResolverResult,
} from '../e2e/search/production-health-target.mjs';

const meter = (bytes = 1024) => ({
  schema_version: 'supabase_client_observed_bytes_v1', measurement_basis: 'client_observed_response_bytes',
  total_bytes: bytes, target_bytes: SUPABASE_CLIENT_BYTE_TARGET, hard_limit_bytes: SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  budget_status: bytes <= SUPABASE_CLIENT_BYTE_TARGET ? 'within_target' : bytes <= SUPABASE_CLIENT_BYTE_HARD_LIMIT ? 'above_target' : 'hard_limit_exceeded',
  target_met: bytes <= SUPABASE_CLIENT_BYTE_TARGET, cost_guard_passed: bytes <= SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  hard_limit_exceeded: bytes > SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  categories: { auth: 0, edge: bytes, direct_rest: 0, direct_rpc: 0 },
  sources: { content_length: bytes, received_body: 0 }, excluded_requests: 0,
});

function fakeJourneyAdapter(overrides = {}) {
  let typed = '';
  let policy = null;
  const state = {
    requests: [], responses: [], routes: [],
    network: { storage_requests: 0, receipt_rpc_requests: 0, failed_requests: 0 },
    meter: meter(overrides.bytes ?? 2048),
  };
  const diagnostics = { console_errors: 0, failed_requests: 0, error_responses: 0, storage_requests: 0 };
  const ids = overrides.ids || ['101', '102'];
  const response = {
    schema_version: 'event_search_v2', search_contract_version: 'event_search_v2',
    request_id: 'request-1', receipt_id: null, response_ids: [...ids],
    response_families: ids.map((id) => `event:${id}`), item_count: ids.length, fallback_count: 0,
    has_more: false, next_offset: 0, result_cache_status: overrides.cache || 'miss', served_from_cache: overrides.cache === 'hit',
    requested_execution_mode: 'cold_vector', actual_execution_mode: overrides.actualMode || 'cold_vector',
    catalog_revision: 'a'.repeat(64), corpus_revision: 'b'.repeat(64), search_document_revision: 'c'.repeat(64),
    policy_versions: {}, provider_attempts: { embedding: 1, vector: 1, llm: overrides.llm ?? 0 },
    provider_attempts_present: true, provider_attempts_source: 'request_counters',
    llm: { requested: false, used: false, status: '' }, http_status: 200, route: 'direct',
  };
  return {
    get typed() { return typed; }, get policy() { return policy; },
    async configureRequestPolicy(value) { policy = value; },
    async open() {},
    async inspectSurface() { return { enabled: true, authorized: true, input_tag: 'textarea', enter_key_hint: 'search' }; },
    async activity() { return structuredClone(state); },
    async healthDiagnostics() { return { ...diagnostics, ...(overrides.diagnostics || {}) }; },
    async typeQuery(value) { typed = value; },
    async submitWithSearchIntent() {
      const body = {
        limit: overrides.limit ?? 5, offset: overrides.offset ?? 0,
        use_llm_verifier: false, allow_llm_fallback: false,
        execution_mode_present: overrides.executionModePresent === true,
      };
      const times = overrides.postCount ?? 1;
      for (let index = 0; index < times; index += 1) {
        state.requests.push({ sequence: index + 1, method: 'POST', path: '/functions/v1/event-search', route: 'direct', body_contract: body });
        state.responses.push({ ...response, sequence: index + 1 });
        state.routes.push({ operation_id: `op-${index}`, policy: 'selected-once', route: 'direct', initial_route: 'direct', kind: 'success', status: 200 });
      }
      state.network.storage_requests = overrides.storageRequests || 0;
      state.network.receipt_rpc_requests = overrides.receiptRpcRequests || 0;
    },
    async waitForTerminal() {},
    async snapshotResults() {
      return {
        terminal: true, error: false, cards_visible: true, visible_card_count: ids.length,
        rendered_ids: [...ids], rendered_families: ids.map((id) => `event:${id}`),
        card_renderer_unavailable: false, skeleton_count: overrides.skeletons || 0, placeholder_count: 0,
      };
    },
    async realScrollResults() { return { performed: true, delta_y: 640, card_visible_after: true, gesture_count: 1 }; },
    async openFirstResult() { return overrides.eventRoute || { same_origin: true, http_status: 200, destination_class: 'event_detail', network_source: 'fake' }; },
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

test('production health journey fails closed on duplicates, canary body, LLM, extra RPC/storage and route defects', async () => {
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ postCount: 2 }), targetUrl: 'https://kenigevents.ru/' }), /duplicate_post/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ executionModePresent: true }), targetUrl: 'https://kenigevents.ru/' }), /request_contract_invalid/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ llm: 1 }), targetUrl: 'https://kenigevents.ru/' }), /llm_activity_forbidden/u);
  await assert.rejects(() => runProductionHealthJourney({ adapter: fakeJourneyAdapter({ cache: 'bypass' }), targetUrl: 'https://kenigevents.ru/' }), /cache_state/u);
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
    assert.equal(JSON.parse(raw).target.immutable_identity.manifest_sha256, 'c'.repeat(64));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
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
});

test('explicit backend deployment mismatch blocks after the single Search without retry', async () => {
  const adapter = fakeJourneyAdapter();
  adapter.preflight = async () => preflight;
  adapter.close = async () => {};
  const result = await runProductionHealthCell({
    platform: 'browser', targetRun: createAcceptedTargetRun(async () => targetRow()),
    expectedSearchBackendRevision: 'expected-v2',
    createAdapter: async () => adapter,
    issueSession: async () => ({ authReceipt, attach: async () => {}, cleanup: async () => {} }),
  });
  assert.equal(result.execution_status, 'BLOCKED');
  assert.equal(result.failure_class, 'BLOCKED_RELEASE_NOT_ACTIVE');
  assert.equal(result.search.physical_post_count, 1);
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
        calls.push(['issue', input.personaId, input.platform]);
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
    ['adapter', '/wd/hub'], ['issue', 'search-cached-android', 'android'], ['callback'],
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
      calls.push(['fixture', options.platform, typeof options.protectedProbe]);
      return { receipt: authReceipt, storageStatePath: '/tmp/ephemeral-state', cleanup: async () => {} };
    },
  };
  const observed = new SupabaseClientObservedByteMeter({ supabaseOrigins: ['https://project.supabase.co'] });
  const hooks = await createBuiltInBrowserHooks({
    PERSONALIZATION_SUPABASE_URL: 'https://project.supabase.co',
    PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: 'publishable',
    AUTH_SESSION_BROKER_URL: 'https://broker.example/issue',
    SEARCH_E2E_PERSONA_EMAIL_CACHED_BROWSER: 'fixture@example.invalid',
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
    ['adapter', true], ['issuer', 'verified-oidc'], ['fixture', 'browser', 'function'],
  ]);
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
