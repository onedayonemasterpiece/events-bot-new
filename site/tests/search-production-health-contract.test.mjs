import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ACCEPTED_HEALTH_CACHE_STATES,
  FUTURE_PRODUCTION_HEALTH_PLAN,
  PRODUCTION_HEALTH_RESULTS,
  PRODUCTION_HEALTH_RESULT_VALUES,
  classifyProductionHealthResult,
  decideProductionHealthRetry,
  evaluateProductionHealthObservation,
} from '../e2e/search/production-health-contract.mjs';
import {
  SUPABASE_CLIENT_BYTE_HARD_LIMIT,
  SUPABASE_CLIENT_BYTE_TARGET,
  SupabaseClientObservedByteMeter,
  classifySupabaseClientUrl,
  observedResponseByteLength,
} from '../e2e/search/production-health-meter.mjs';
import {
  assessAcceptedTargetSupersession,
  createAcceptedTargetRun,
  normalizeAcceptedTargetResolverResult,
  redactAcceptedTargetUrl,
} from '../e2e/search/production-health-target.mjs';

const exactResults = [
  'HEALTHY',
  'DEGRADED',
  'BROKEN_SEARCH_SURFACE',
  'BROKEN_AUTH_INTEGRATION',
  'BROKEN_SEARCH_REQUEST',
  'BROKEN_NO_RESULTS',
  'BROKEN_RESULT_RENDER',
  'BROKEN_RESULT_ROUTE',
  'UNKNOWN_AUTH_BROKER',
  'UNKNOWN_RUNNER_BROWSER',
  'BLOCKED_RELEASE_NOT_ACTIVE',
  'COST_GUARD_FAILED',
  'EVIDENCE_REDACTION_FAILED',
];

const healthyObservation = (overrides = {}) => ({
  evidence_redaction_passed: true,
  cost_guard_passed: true,
  release_active: true,
  auth_broker_known: true,
  runner_browser_known: true,
  search_surface_ready: true,
  auth_integration_ready: true,
  query_count: 1,
  query_dispatch: 'ui',
  query_execution: 'vector_only',
  search_post_count: 1,
  limit: 5,
  cache_state: 'hit',
  llm_calls: 0,
  pagination_requests: 0,
  receipt_rpc_calls: 0,
  storage_image_requests: 0,
  card_count: 2,
  cards_rendered: true,
  real_scroll_count: 1,
  real_scroll_performed: true,
  card_http_200_count: 1,
  ...overrides,
});

const resolverRow = (overrides = {}) => ({
  source: 'current_accepted_pointer',
  target_url: `https://kenigevents.ru/_review/${'A'.repeat(43)}/poisk/`,
  target_repo_sha: 'a'.repeat(40),
  checkout_repo_sha: 'b'.repeat(40),
  accepted_release_id: 'release-42',
  generation_ids: { catalog: 'catalog-9', corpus: 'corpus-10' },
  ...overrides,
});

test('result enum is exact and only BROKEN results are product failures/incidents', () => {
  assert.deepEqual(PRODUCTION_HEALTH_RESULT_VALUES, exactResults);
  assert.deepEqual(Object.keys(PRODUCTION_HEALTH_RESULTS), exactResults);
  for (const result of exactResults) {
    const disposition = classifyProductionHealthResult(result);
    const expected = result.startsWith('BROKEN_');
    assert.equal(disposition.product_failure, expected, result);
    assert.equal(disposition.product_incident, expected, result);
  }
  assert.throws(() => classifyProductionHealthResult('FAILED'), /result_unknown/u);
});

test('future health contract is one bounded UI vector request and contains no release coupling', () => {
  assert.deepEqual(ACCEPTED_HEALTH_CACHE_STATES, ['hit', 'miss', 'stored', 'bypass']);
  assert.deepEqual(FUTURE_PRODUCTION_HEALTH_PLAN.query, {
    count: 1,
    dispatch: 'ui',
    execution: 'vector_only',
    search_post_count: 1,
    limit: 5,
  });
  assert.deepEqual(FUTURE_PRODUCTION_HEALTH_PLAN.results, {
    card_count: { minimum: 1, maximum: 5 },
    real_scroll_count: 1,
    card_http_200_count: 1,
  });
  assert.deepEqual(FUTURE_PRODUCTION_HEALTH_PLAN.forbidden_activity, {
    llm_calls: 0,
    pagination_requests: 0,
    receipt_rpc_calls: 0,
    storage_image_requests: 0,
  });
  assert.equal(JSON.stringify(FUTURE_PRODUCTION_HEALTH_PLAN).includes('release_exact'), false);
});

test('cache hit/miss/stored/bypass and content or index drift are healthy', () => {
  for (const cache_state of ACCEPTED_HEALTH_CACHE_STATES) {
    const result = evaluateProductionHealthObservation(healthyObservation({
      cache_state,
      catalog_revision_changed: true,
      corpus_revision_changed: true,
      index_generation_changed: true,
    }));
    assert.equal(result.result, 'HEALTHY', cache_state);
  }
});

test('health evaluator enforces one POST, zero LLM, zero pagination and bounded cards', () => {
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ search_post_count: 2 })).result, 'BROKEN_SEARCH_REQUEST');
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ llm_calls: 1 })).result, 'BROKEN_SEARCH_REQUEST');
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ pagination_requests: 1 })).result, 'BROKEN_SEARCH_REQUEST');
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ receipt_rpc_calls: 1 })).result, 'BROKEN_SEARCH_REQUEST');
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ storage_image_requests: 1 })).result, 'BROKEN_SEARCH_REQUEST');
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ card_count: 0 })).result, 'BROKEN_NO_RESULTS');
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ card_count: 6 })).result, 'BROKEN_RESULT_RENDER');
  assert.equal(evaluateProductionHealthObservation(healthyObservation({ card_http_200_count: 0 })).result, 'BROKEN_RESULT_ROUTE');
});

test('retry is denied after dispatch and fail-closed before dispatch', () => {
  assert.deepEqual(decideProductionHealthRetry({ search_dispatched: true, zero_side_effects_proven: true }), {
    retry_allowed: false,
    reason: 'search_already_dispatched',
  });
  assert.equal(decideProductionHealthRetry({ search_dispatched: false }).retry_allowed, false);
  assert.equal(decideProductionHealthRetry({ search_dispatched: false, zero_side_effects_proven: true }).retry_allowed, true);
});

test('accepted target normalizes, redacts for display and does not couple checkout SHA', () => {
  const target = normalizeAcceptedTargetResolverResult(resolverRow());
  assert.equal(target.target_repo_sha, 'a'.repeat(40));
  assert.match(target.navigationUrl(), /_review\/A{43}\/poisk\/$/u);
  assert.equal(target.target_url_redacted, 'https://kenigevents.ru/_review/<redacted>/poisk/');
  assert.equal(JSON.stringify(target).includes('A'.repeat(43)), false);
  assert.deepEqual(target.generation_ids, { catalog: 'catalog-9', corpus: 'corpus-10' });
  assert.equal(redactAcceptedTargetUrl(target.navigationUrl()), target.target_url_redacted);
  assert.throws(() => normalizeAcceptedTargetResolverResult(resolverRow({
    target_url: 'https://kenigevents.ru/poisk/',
  })), /target_url_invalid/u);
  assert.throws(() => normalizeAcceptedTargetResolverResult(resolverRow({
    source: 'latest_kaggle_job',
  })), /target_source_invalid/u);
  assert.throws(() => normalizeAcceptedTargetResolverResult(resolverRow({
    public_poisk_fallback: 'https://kenigevents.ru/poisk/',
  })), /fallback_forbidden/u);
});

test('target pin is stable and pointer changes are telemetry, not retry/product failure', async () => {
  const rows = [resolverRow(), resolverRow({
    target_url: `https://kenigevents.ru/_review/${'B'.repeat(43)}/poisk/`,
    target_repo_sha: 'c'.repeat(40),
    accepted_release_id: 'release-43',
  })];
  const run = createAcceptedTargetRun(async () => rows.shift());
  const first = await run.pin();
  assert.equal(await run.pin(), first);
  assert.equal(run.resolverCallCount(), 1);
  assert.deepEqual(await run.observeSupersession(), {
    target_superseded: true,
    retry_allowed: false,
    product_failure: false,
    product_incident: false,
  });
  assert.equal(run.resolverCallCount(), 2);

  const pinned = normalizeAcceptedTargetResolverResult(resolverRow());
  const generationOnlyChange = normalizeAcceptedTargetResolverResult(resolverRow({
    generation_ids: { catalog: 'new-generation' },
  }));
  assert.equal(assessAcceptedTargetSupersession(pinned, generationOnlyChange).target_superseded, false);
  assert.equal(assessAcceptedTargetSupersession(pinned, pinned).target_superseded, false);
});

test('Supabase meter classifies Auth, Edge, direct REST/RPC and excludes other origins', () => {
  const origin = 'https://project.supabase.co';
  assert.equal(classifySupabaseClientUrl(`${origin}/auth/v1/user`, { supabaseOrigins: [origin] }), 'auth');
  assert.equal(classifySupabaseClientUrl(`${origin}/functions/v1/event-search`, { supabaseOrigins: [origin] }), 'edge');
  assert.equal(classifySupabaseClientUrl(`${origin}/rest/v1/events`, { supabaseOrigins: [origin] }), 'direct_rest');
  assert.equal(classifySupabaseClientUrl(`${origin}/rest/v1/rpc/search_events`, { supabaseOrigins: [origin] }), 'direct_rpc');
  assert.equal(classifySupabaseClientUrl('https://storage.yandexcloud.net/site/image.webp', { supabaseOrigins: [origin] }), 'excluded');
  assert.equal(classifySupabaseClientUrl('https://cdn.kenigevents.ru/image.webp', { supabaseOrigins: [origin] }), 'excluded');
  assert.equal(classifySupabaseClientUrl('https://other.supabase.co/auth/v1/user', { supabaseOrigins: [origin] }), 'excluded');
});

test('meter uses Content-Length or received body and enforces 48/96 KiB boundaries', () => {
  assert.deepEqual(observedResponseByteLength({ headers: { 'Content-Length': '7' }, body: 'ignored' }), {
    bytes: 7,
    source: 'content_length',
  });
  assert.deepEqual(observedResponseByteLength({ body: 'я' }), { bytes: 2, source: 'received_body' });

  const origin = 'https://project.supabase.co';
  const atTarget = new SupabaseClientObservedByteMeter({ supabaseOrigins: [origin] });
  atTarget.recordResponse({ url: `${origin}/auth/v1/user`, headers: { 'content-length': String(SUPABASE_CLIENT_BYTE_TARGET) } });
  assert.equal(atTarget.snapshot().budget_status, 'within_target');

  const atHard = new SupabaseClientObservedByteMeter({ supabaseOrigins: [origin] });
  atHard.recordResponse({ url: `${origin}/functions/v1/event-search`, body: new Uint8Array(SUPABASE_CLIENT_BYTE_HARD_LIMIT) });
  assert.equal(atHard.snapshot().budget_status, 'above_target');
  assert.equal(atHard.snapshot().hard_limit_exceeded, false);
  atHard.recordResponse({ url: `${origin}/rest/v1/rpc/probe`, body: new Uint8Array(1) });
  assert.equal(atHard.snapshot().budget_status, 'hard_limit_exceeded');
  assert.equal(atHard.snapshot().hard_limit_exceeded, true);

  const beforeExcluded = atHard.snapshot().total_bytes;
  atHard.recordResponse({ url: 'https://cdn.kenigevents.ru/image.webp', body: new Uint8Array(999) });
  assert.equal(atHard.snapshot().total_bytes, beforeExcluded);
  assert.equal(atHard.snapshot().excluded_requests, 1);
  assert.equal(JSON.stringify(atHard.snapshot()).includes('billing'), false);
});
