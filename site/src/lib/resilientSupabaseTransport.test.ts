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

test('reuses the selected route and rechecks both paths only after the cache window', async () => {
  let now = 1_000;
  const calls: string[] = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    cacheTtlMs: 5_000,
    sessionStorage: null,
    now: () => now,
    fetchImpl: (async (input) => {
      calls.push(String(input));
      return new Response('{}', { status: 200 });
    }) as typeof fetch,
  });
  await transport.fetch(`${direct}/rest/v1/first?select=id`);
  await transport.fetch(`${direct}/rest/v1/second?select=id`);
  assert.equal(calls.filter((url) => url.endsWith('/auth/v1/health')).length, 2);
  now += 5_001;
  await transport.fetch(`${direct}/rest/v1/third?select=id`);
  assert.equal(calls.filter((url) => url.endsWith('/auth/v1/health')).length, 4);
});

test('shares a recent diagnostic route choice with the next page in the same session', async () => {
  const values = new Map<string, string>();
  const storage = {
    getItem: (name: string) => values.get(name) || null,
    setItem: (name: string, value: string) => { values.set(name, value); },
    removeItem: (name: string) => { values.delete(name); },
  };
  const diagnostic = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: storage,
    fetchImpl: (async (input) => {
      if (String(input).startsWith(direct)) throw new TypeError('blocked');
      return new Response('{}', { status: 200 });
    }) as typeof fetch,
  });
  assert.equal((await diagnostic.selectRoute(true)).route, 'relay');

  const calls: string[] = [];
  const nextPage = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: storage,
    fetchImpl: (async (input) => {
      calls.push(String(input));
      return new Response('{}', { status: 200 });
    }) as typeof fetch,
  });
  await nextPage.fetch(`${direct}/rest/v1/example?select=id`);
  assert.deepEqual(calls, [`${relay}/rest/v1/example?select=id`]);
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
  const second = await transport.fetch(`${direct}/rest/v1/second?select=id`, { method: 'GET' });
  assert.equal(second.status, 200);
  assert.deepEqual(calls.filter((url) => url.includes('/rest/v1/second')), [
    `${relay}/rest/v1/second?select=id`,
  ]);
});

test('gives a safe fallback its own time budget before the caller deadline', async () => {
  const calls: string[] = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct,
    relayUrl: relay,
    publishableKey: key,
    sessionStorage: null,
    safeRequestTimeoutMs: 1_000,
    fetchImpl: (async (input, init) => {
      const url = String(input);
      calls.push(url);
      if (url.endsWith('/auth/v1/health')) {
        if (url.startsWith(relay)) await new Promise((resolve) => setTimeout(resolve, 25));
        return new Response('{}', { status: 200 });
      }
      if (url.startsWith(relay)) return new Response('recovered', { status: 200 });
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => {
          reject(new DOMException('primary timed out', 'AbortError'));
        }, { once: true });
      });
    }) as typeof fetch,
  });
  const caller = new AbortController();
  const callerTimer = setTimeout(() => caller.abort('caller_deadline'), 2_500);
  try {
    const response = await transport.fetch(`${direct}/rest/v1/flaky?select=id`, {
      method: 'GET',
      signal: caller.signal,
    });
    assert.equal(response.status, 200);
    assert.equal(caller.signal.aborted, false);
    assert.deepEqual(calls.filter((url) => url.includes('/rest/v1/flaky')), [
      `${direct}/rest/v1/flaky?select=id`,
      `${relay}/rest/v1/flaky?select=id`,
    ]);
  } finally {
    clearTimeout(callerTimer);
  }
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
