import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  assertExecutionReceipt,
  assertResponseRenderedIds,
  assertUniqueCards,
  summarizeSearchPayload,
} from '../e2e/search/acceptance.mjs';
import { assertSanitizedSearchEvidence, sanitizedTargetPath } from '../e2e/search/evidence.mjs';
import { SEARCH_CANARY_VARIANTS } from '../e2e/search/canary-manifest.mjs';
import { installSearchRuntimeProbe } from '../e2e/search/adapters/runtime-probe.mjs';

test('response summary retains IDs and receipts but drops query and card text', () => {
  const summary = summarizeSearchPayload({
    search_contract_version: 'search-v2', requested_execution_mode: 'cold_vector', actual_execution_mode: 'cold_vector',
    request_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', receipt_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    result_cache_status: 'stored', served_from_cache: false,
    catalog_revision: 'catalog-a', corpus_revision: 'corpus-a', provider_attempt_counters: { embedding: 1, vector: 1, llm: 0 },
    query: 'must not survive', items: [
      { event_id: 11, title: 'must not survive', occurrence_member_ids: [11, 12] },
      { event_id: 12, title: 'same family', occurrence_member_ids: [12, 11] },
    ],
    fallback_items: [{ event_id: 20, title: 'also secret' }], has_more: false,
  });
  assert.deepEqual(summary.response_ids, ['11', '20']);
  assert.deepEqual(summary.response_families, ['family:11,12', 'event:20']);
  assert.equal(summary.receipt_id, 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb');
  assert.equal(JSON.stringify(summary).includes('must not survive'), false);
  assertExecutionReceipt(summary, 'cold_vector');
  assertSanitizedSearchEvidence(summary);
});

test('browser runtime probe retains the owner-scoped receipt identity', async () => {
  const transport = {
    fetch: async () => new Response(JSON.stringify({
      search_contract_version: 'search-v2',
      request_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      receipt_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
      requested_execution_mode: 'cold_vector',
      actual_execution_mode: 'cold_vector',
      items: [{ event_id: 11 }],
      provider_attempt_counters: { embedding: 1, vector: 1, llm: 0 },
    }), { status: 200, headers: { 'content-type': 'application/json' } }),
  };
  const originalLocation = globalThis.location;
  const originalDocument = globalThis.document;
  const originalClients = globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__;
  try {
    Object.defineProperty(globalThis, 'location', { configurable: true, value: { href: 'https://kenigevents.ru/poisk/' } });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: { querySelector: () => null } });
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = new Map([['search', { transport }]]);
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    installSearchRuntimeProbe({ execution_mode: 'cold_vector' });
    await transport.fetch('https://example.supabase.co/functions/v1/event-search', { method: 'POST', body: '{}' });
    assert.equal(
      globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__.responses[0]?.receipt_id,
      'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    );
  } finally {
    delete globalThis.__KENIGEVENTS_SEARCH_HARNESS_V1__;
    globalThis.__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__ = originalClients;
    Object.defineProperty(globalThis, 'location', { configurable: true, value: originalLocation });
    Object.defineProperty(globalThis, 'document', { configurable: true, value: originalDocument });
  }
});

test('rendered IDs and duplicate family assertions are strict', () => {
  assert.doesNotThrow(() => assertResponseRenderedIds({ response_ids: ['2', '3'] }, { rendered_ids: ['1', '2', '3'] }, { prefixIds: ['1'] }));
  assert.throws(() => assertResponseRenderedIds({ response_ids: ['2'] }, { rendered_ids: ['1', '3'] }, { prefixIds: ['1'] }), /ids_mismatch/u);
  assert.throws(() => assertUniqueCards({ rendered_ids: ['1', '2'], rendered_families: ['family:1,2', 'family:1,2'] }), /duplicate_families/u);
});

test('manifest has only the four closed execution modes and exact policies', () => {
  assert.deepEqual(Object.keys(SEARCH_CANARY_VARIANTS), [
    'cached_vector', 'cold_vector', 'cold_vector_llm', 'degraded_vector_fallback',
  ]);
  for (const [name, value] of Object.entries(SEARCH_CANARY_VARIANTS)) {
    assert.equal(value.request_policy.execution_mode, name);
    assert.equal(value.request_policy.selected_once, true);
  }
  assert.equal(SEARCH_CANARY_VARIANTS.cold_vector.allowed_provider_attempts.llm, 0);
  assert.equal(SEARCH_CANARY_VARIANTS.degraded_vector_fallback.allowed_provider_attempts.llm, 0);
});

test('evidence rejects secrets and redacts secret-candidate target path', () => {
  assert.equal(sanitizedTargetPath('/preview-secret-token-123/poisk/'), '/preview-<redacted>/poisk/');
  assert.equal(sanitizedTargetPath('/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/'), '/_review/<redacted>/poisk/');
  assert.throws(() => assertSanitizedSearchEvidence({ session: 'value' }), /forbidden_key/u);
  assert.throws(() => assertSanitizedSearchEvidence({ note: 'user@example.com' }), /forbidden_value/u);
  assert.throws(() => assertSanitizedSearchEvidence({ path: '/preview-secret-token-123/poisk/' }), /forbidden_value/u);
  assert.throws(() => assertSanitizedSearchEvidence({ path: '/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/poisk/' }), /forbidden_value/u);
});
