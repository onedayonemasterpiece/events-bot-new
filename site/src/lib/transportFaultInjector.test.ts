import assert from 'node:assert/strict';
import test from 'node:test';
import { createResilientSupabaseTransport } from './resilientSupabaseTransport.ts';
import { createFaultInjectingFetch } from './transportFaultInjector.e2e.ts';

const direct = 'https://project.supabase.co';
const relay = 'https://relay.example.test';
const profile = {
  id: 'client_supabase_direct_unreachable',
  registry_digest: 'a'.repeat(64),
  rules: [{ host_class: 'supabase_direct' as const, failure: 'network_reject' as const }],
};

async function healthyRoute(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const url = new URL(String(input));
  if (url.pathname === '/auth/v1/health') return Response.json({ version: 'test' });
  if (url.pathname === '/auth/v1/otp') return Response.json({ accepted: true });
  if (url.origin === 'https://kenigevents.ru' && url.pathname === '/healthz') return Response.json({ ok: true });
  throw new Error(`unexpected_test_request:${url.origin}${url.pathname}:${init?.method || 'GET'}`);
}

test('direct-unreachable profile faults only the exact direct origin and emits sanitized evidence', async () => {
  const calls: string[] = [];
  const injected = createFaultInjectingFetch({
    directUrl: direct,
    relayUrl: relay,
    fetchImpl: (async (input, init) => {
      calls.push(String(input));
      return healthyRoute(input, init);
    }) as typeof fetch,
  }, profile);

  await assert.rejects(() => injected(`${direct}/auth/v1/health`), /transport_fault_injected/u);
  assert.equal((await injected(`${relay}/auth/v1/health`)).ok, true);
  assert.equal((await injected('https://kenigevents.ru/healthz')).ok, true);
  assert.deepEqual(calls, [`${relay}/auth/v1/health`, 'https://kenigevents.ru/healthz']);

  const events = (globalThis as typeof globalThis & {
    ['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:events']?: Array<Record<string, unknown>>;
  })['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:events'] || [];
  const event = events.at(-1);
  assert.equal(event?.fault_profile, profile.id);
  assert.equal(event?.host_class, 'supabase_direct');
  assert.equal(event?.failure, 'network_reject');
  assert.equal(event?.method, 'GET');
  assert.ok(!Object.hasOwn(event || {}, 'url'));
  assert.ok(!Object.hasOwn(event || {}, 'body'));
});

test('direct fault reaches probe selection before selected-once OTP and dispatches only relay once', async () => {
  const upstream: string[] = [];
  const injected = createFaultInjectingFetch({
    directUrl: direct,
    relayUrl: relay,
    fetchImpl: (async (input, init) => {
      upstream.push(String(input));
      return healthyRoute(input, init);
    }) as typeof fetch,
  }, profile);
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: 'sb_publishable_test',
    fetchImpl: injected,
    persistentStorage: null,
    probeStaggerMs: 0,
    routeCacheNamespace: profile.id,
  });

  const response = await transport.fetch(`${direct}/auth/v1/otp`, { method: 'POST', body: '{}' });
  assert.equal(response.ok, true);
  assert.equal(response.headers.get('x-ke-transport-route'), 'relay');
  assert.equal(upstream.filter((url) => url === `${relay}/auth/v1/otp`).length, 1);
  assert.equal(upstream.filter((url) => url === `${direct}/auth/v1/otp`).length, 0);
  assert.equal(transport.latestOutcome('auth.otp')?.initialRoute, 'relay');
  assert.equal(transport.latestOutcome('auth.otp')?.finalRoute, 'relay');
});
