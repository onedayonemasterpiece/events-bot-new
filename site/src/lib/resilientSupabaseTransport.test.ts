import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createResilientSupabaseTransport,
  supabaseAuthStorageKey,
} from './resilientSupabaseTransport.ts';

const direct = 'https://project.supabase.co';
const relay = 'https://relay.example.test';
const key = 'sb_publishable_test';

test('keeps the historical Supabase auth storage key while transport origin changes', () => {
  assert.equal(supabaseAuthStorageKey(direct), 'sb-project-auth-token');
});

test('selects the healthy relay with safe parallel health probes', async () => {
  const calls: Array<{ url: string; method: string }> = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: null,
    fetchImpl: (async (input, init) => {
      const url = String(input);
      calls.push({ url, method: String(init?.method || 'GET') });
      if (url.startsWith(direct)) throw new TypeError('blocked');
      return new Response('{}', { status: 200 });
    }) as typeof fetch,
  });
  const result = await transport.selectRoute();
  assert.equal(result.route, 'relay');
  assert.equal(result.probes.length, 2);
  assert.equal(calls.filter((item) => item.url.endsWith('/auth/v1/health')).length, 2);
});

test('does not wait for a hanging direct probe after the relay is healthy', async () => {
  let releaseDirect: (() => void) | null = null;
  const directPending = new Promise<void>((resolve) => { releaseDirect = resolve; });
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: null,
    fetchImpl: (async (input) => {
      const url = String(input);
      if (url.startsWith(direct)) {
        await directPending;
        throw new TypeError('blocked');
      }
      return new Response('{}', { status: 200 });
    }) as typeof fetch,
  });
  const result = await Promise.race([
    transport.selectRoute(),
    new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error('selection_waited_for_direct')), 250)),
  ]);
  assert.equal(result.route, 'relay');
  releaseDirect?.();
});

test('sends a non-idempotent OTP only once through the preselected route', async () => {
  const calls: Array<{ url: string; method: string }> = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: null,
    fetchImpl: (async (input, init) => {
      const url = String(input);
      const method = String(init?.method || 'GET').toUpperCase();
      calls.push({ url, method });
      if (url.endsWith('/auth/v1/health')) {
        if (url.startsWith(direct)) throw new TypeError('blocked');
        return new Response('{}', { status: 200 });
      }
      throw new TypeError('ambiguous otp failure');
    }) as typeof fetch,
  });
  await assert.rejects(() => transport.fetch(`${direct}/auth/v1/otp`, {
    method: 'POST',
    body: '{}',
  }));
  const otpCalls = calls.filter((item) => item.url.includes('/auth/v1/otp'));
  assert.deepEqual(otpCalls, [{ url: `${relay}/auth/v1/otp`, method: 'POST' }]);
});

test('falls back once for a safe read and never duplicates it further', async () => {
  const calls: string[] = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: null,
    fetchImpl: (async (input) => {
      const url = String(input);
      calls.push(url);
      if (url.endsWith('/auth/v1/health')) return new Response('{}', { status: 200 });
      if (url.startsWith(direct)) return new Response('upstream', { status: 503 });
      return new Response('ok', { status: 200 });
    }) as typeof fetch,
  });
  const response = await transport.fetch(`${direct}/rest/v1/example?select=id`, { method: 'GET' });
  assert.equal(response.status, 200);
  assert.deepEqual(calls.filter((url) => url.includes('/rest/v1/example')), [
    `${direct}/rest/v1/example?select=id`,
    `${relay}/rest/v1/example?select=id`,
  ]);
});

test('does not rewrite unrelated requests', async () => {
  let seen = '';
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: null,
    fetchImpl: (async (input) => {
      seen = String(input);
      return new Response('{}', { status: 200 });
    }) as typeof fetch,
  });
  await transport.fetch('https://kenigevents.ru/data/events.json');
  assert.equal(seen, 'https://kenigevents.ru/data/events.json');
});

test('browser-native fetch keeps the global receiver', async () => {
  const originalFetch = globalThis.fetch;
  const calls: string[] = [];
  globalThis.fetch = function (this: typeof globalThis, input: RequestInfo | URL) {
    assert.equal(this, globalThis);
    calls.push(String(input));
    return Promise.resolve(new Response('{}', { status: 200 }));
  } as typeof fetch;
  try {
    const transport = createResilientSupabaseTransport({
      directUrl: direct,
      publishableKey: key,
      sessionStorage: null,
    });
    const probe = await transport.probe('direct');
    assert.equal(probe.ok, true);
    assert.deepEqual(calls, [`${direct}/auth/v1/health`]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
