import assert from 'node:assert/strict';
import http, { type IncomingMessage, type ServerResponse } from 'node:http';
import test from 'node:test';
import {
  createResilientSupabaseTransport,
  parseSupabaseTransportError,
  supabaseAuthStorageKey,
} from './resilientSupabaseTransport.ts';
import { classifyBackendOperation, policyForOperation } from './backendOperationCatalog.ts';
import { getResilientDataClient, resetResilientDataClientRegistryForTests } from './resilientDataClient.ts';

const direct = 'https://project.supabase.co';
const relay = 'https://relay.example.test';
const key = 'sb_publishable_test';

async function probeResponse(input: RequestInfo | URL, init?: RequestInit): Promise<Response | null> {
  const path = new URL(String(input)).pathname;
  if (path === '/auth/v1/health') return Response.json({ version: 'test' });
  if (path.endsWith('/rest/v1/rpc/transport_probe_v1')) {
    const payload = JSON.parse(String(init?.body || '{}'));
    return Response.json({ nonce: payload.p_nonce, schema: 1 });
  }
  if (path.endsWith('/functions/v1/transport-probe')) {
    const payload = JSON.parse(String(init?.body || '{}'));
    return Response.json({ nonce: payload.nonce, schema: 1 });
  }
  return null;
}

async function requestBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString('utf8');
}

async function startServer(handler: (request: IncomingMessage, response: ServerResponse) => void | Promise<void>) {
  const server = http.createServer((request, response) => { void handler(request, response); });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('server_address_missing');
  return {
    origin: `http://127.0.0.1:${address.port}`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

function writeAuthProbe(request: IncomingMessage, response: ServerResponse): boolean {
  if (request.url !== '/auth/v1/health') return false;
  response.writeHead(200, { 'Content-Type': 'application/json' });
  response.end('{"version":"test"}');
  return true;
}

async function writeDataProbe(request: IncomingMessage, response: ServerResponse): Promise<boolean> {
  if (request.url !== '/rest/v1/rpc/transport_probe_v1') return false;
  const payload = JSON.parse(await requestBody(request));
  response.writeHead(200, { 'Content-Type': 'application/json' });
  response.end(JSON.stringify({ nonce: payload.p_nonce, schema: 1 }));
  return true;
}

test('keeps the historical auth storage key', () => {
  assert.equal(supabaseAuthStorageKey(direct), 'sb-project-auth-token');
});

test('operation catalog owns semantics and rejects unknown mutations', () => {
  assert.equal(policyForOperation(classifyBackendOperation(`${direct}/auth/v1/otp`, { method: 'POST' })), 'selected-once');
  assert.equal(policyForOperation(classifyBackendOperation(`${direct}/rest/v1/rpc/get_listing_personal_feed_v1`, { method: 'POST' })), 'safe-read');
  assert.equal(policyForOperation(classifyBackendOperation(`${direct}/rest/v1/rpc/submit_focus_group_feedback_v2`, { method: 'POST' })), 'idempotent-replay');
  assert.equal(classifyBackendOperation(`${direct}/functions/v1/event-search`, {
    method: 'POST', headers: { Accept: 'application/x-ndjson' },
  }).responseMode, 'stream');
  assert.throws(() => classifyBackendOperation(`${direct}/rest/v1/rpc/unknown`, { method: 'POST' }), /unclassified/u);
});

test('staggered probe reaches relay without waiting for hanging direct', async () => {
  const calls: string[] = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null, probeStaggerMs: 25,
    fetchImpl: (async (input, init) => {
      calls.push(String(input));
      if (String(input).startsWith(direct)) return new Promise<Response>(() => {});
      return await probeResponse(input, init) || Response.json({});
    }) as typeof fetch,
  });
  const result = await Promise.race([
    transport.selectRoute(false, 'auth'),
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error('waited_for_direct')), 250)),
  ]);
  assert.equal(result.route, 'relay');
  assert.equal(calls.filter((url) => url.endsWith('/auth/v1/health')).length, 2);
});

test('last-known-good is compact, cross-page and capability-specific', async () => {
  const values = new Map<string, string>();
  const storage = {
    getItem: (name: string) => values.get(name) || null,
    setItem: (name: string, value: string) => { values.set(name, value); },
    removeItem: (name: string) => { values.delete(name); },
  };
  const first = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: storage,
    fetchImpl: (async (input, init) => {
      if (String(input).startsWith(direct)) throw new TypeError('blocked');
      return await probeResponse(input, init) || Response.json([]);
    }) as typeof fetch,
  });
  const firstRead = await first.fetch(`${direct}/rest/v1/personalization_event_reaction_counter?select=id`);
  assert.equal(firstRead.headers.get('x-ke-transport-route'), 'relay');

  const calls: string[] = [];
  const second = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: storage,
    fetchImpl: (async (input, init) => {
      calls.push(String(input));
      return await probeResponse(input, init) || Response.json([]);
    }) as typeof fetch,
  });
  await second.fetch(`${direct}/rest/v1/personalization_event_reaction_counter?select=id`);
  assert.deepEqual(calls, [`${relay}/rest/v1/personalization_event_reaction_counter?select=id`]);
  assert.ok([...values.values()].every((value) => value.length < 100));
});

test('200 headers followed by stalled OTP body is ambiguous and dispatched once', async () => {
  let otpCount = 0;
  const directServer = await startServer((request, response) => {
    if (writeAuthProbe(request, response)) return;
    if (request.url === '/auth/v1/otp') {
      otpCount += 1;
      response.writeHead(200, { 'Content-Type': 'application/json' });
      response.write('{"accepted":');
      return;
    }
    response.writeHead(404).end();
  });
  const relayServer = await startServer((request, response) => {
    if (writeAuthProbe(request, response)) return;
    if (request.url === '/auth/v1/otp') {
      otpCount += 1;
      response.writeHead(200, { 'Content-Type': 'application/json' });
      response.end('{}');
      return;
    }
    response.writeHead(404).end();
  });
  try {
    const transport = createResilientSupabaseTransport({
      directUrl: directServer.origin, relayUrl: relayServer.origin, publishableKey: key,
      sessionStorage: null, probeStaggerMs: 100, selectedRequestTimeoutMs: 2_000,
    });
    await assert.rejects(
      () => transport.fetch(`${directServer.origin}/auth/v1/otp`, { method: 'POST', body: '{}' }),
      (error) => parseSupabaseTransportError(error)?.phase === 'body',
    );
    assert.equal(otpCount, 1);
  } finally {
    await Promise.all([directServer.close(), relayServer.close()]);
  }
});

test('partial body socket close is body failure and not success', async () => {
  let count = 0;
  const server = await startServer((request, response) => {
    if (writeAuthProbe(request, response)) return;
    count += 1;
    response.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': '20' });
    response.write('{"ok":');
    setTimeout(() => response.socket?.destroy(), 20);
  });
  try {
    const transport = createResilientSupabaseTransport({ directUrl: server.origin, publishableKey: key, sessionStorage: null });
    await assert.rejects(
      () => transport.fetch(`${server.origin}/auth/v1/otp`, { method: 'POST', body: '{}' }),
      (error) => parseSupabaseTransportError(error)?.phase === 'body',
    );
    assert.equal(count, 1);
  } finally { await server.close(); }
});

test('safe read falls back after stalled body and caches alternate', async () => {
  let directReads = 0;
  let relayReads = 0;
  const directServer = await startServer(async (request, response) => {
    if (await writeDataProbe(request, response)) return;
    directReads += 1;
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.write('{"rows":');
  });
  const relayServer = await startServer(async (request, response) => {
    if (await writeDataProbe(request, response)) return;
    relayReads += 1;
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end('[]');
  });
  try {
    const transport = createResilientSupabaseTransport({
      directUrl: directServer.origin, relayUrl: relayServer.origin, publishableKey: key,
      sessionStorage: null, safeRequestTimeoutMs: 1_000, probeStaggerMs: 100,
    });
    const first = await transport.fetch(`${directServer.origin}/rest/v1/example?select=id`);
    assert.equal(first.headers.get('x-ke-transport-route'), 'relay');
    const second = await transport.fetch(`${directServer.origin}/rest/v1/second?select=id`);
    assert.equal(second.headers.get('x-ke-transport-route'), 'relay');
    assert.equal(directReads, 1);
    assert.equal(relayReads, 2);
  } finally { await Promise.all([directServer.close(), relayServer.close()]); }
});

test('invalid JSON safe-read recovers but selected-once is ambiguous', async () => {
  const transport = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null,
    fetchImpl: (async (input, init) => {
      const probe = await probeResponse(input, init);
      if (probe) return probe;
      if (String(input).startsWith(direct)) return new Response('{broken', { status: 200 });
      return Response.json({ ok: true });
    }) as typeof fetch,
  });
  const read = await transport.fetch(`${direct}/rest/v1/example?select=id`);
  assert.equal(read.headers.get('x-ke-transport-route'), 'relay');

  const auth = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null,
    fetchImpl: (async (input, init) => await probeResponse(input, init) || new Response('{broken', { status: 200 })) as typeof fetch,
  });
  await assert.rejects(
    () => auth.fetch(`${direct}/auth/v1/otp`, { method: 'POST', body: '{}' }),
    (error) => parseSupabaseTransportError(error)?.phase === 'decode',
  );
});

test('NDJSON stream keeps parent cancellation but is not cut by the JSON request deadline', async () => {
  const transport = createResilientSupabaseTransport({
    directUrl: direct, publishableKey: key, sessionStorage: null, selectedRequestTimeoutMs: 2_000,
    fetchImpl: (async (input, init) => {
      const probe = await probeResponse(input, init);
      if (probe) return probe;
      const signal = init?.signal;
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          const timer = setTimeout(() => {
            controller.enqueue(new TextEncoder().encode('{"type":"result","data":{"items":[]}}\n'));
            controller.close();
          }, 2_100);
          signal?.addEventListener('abort', () => {
            clearTimeout(timer);
            controller.error(new DOMException('aborted', 'AbortError'));
          }, { once: true });
        },
      });
      return new Response(body, { status: 200, headers: { 'Content-Type': 'application/x-ndjson' } });
    }) as typeof fetch,
  });
  const response = await transport.fetch(`${direct}/functions/v1/event-search`, {
    method: 'POST', headers: { Accept: 'application/x-ndjson' }, body: '{}',
  });
  assert.match(await response.text(), /"type":"result"/u);
});

test('NDJSON stream body failure stays attached to its selected-once operation', async () => {
  const transport = createResilientSupabaseTransport({
    directUrl: direct, publishableKey: key, sessionStorage: null,
    fetchImpl: (async (input, init) => {
      const probe = await probeResponse(input, init);
      if (probe) return probe;
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('{"type":"progress"}\n'));
          setTimeout(() => controller.error(new TypeError('stream lost')), 20);
        },
      }), { status: 200, headers: { 'Content-Type': 'application/x-ndjson' } });
    }) as typeof fetch,
  });
  const response = await transport.fetch(`${direct}/functions/v1/event-search`, {
    method: 'POST', headers: { Accept: 'application/x-ndjson' }, body: '{}',
  });
  await assert.rejects(
    () => response.text(),
    (error) => parseSupabaseTransportError(error)?.phase === 'body',
  );
});

test('429 is definitive and is not replayed', async () => {
  let count = 0;
  const transport = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null,
    fetchImpl: (async (input, init) => {
      const probe = await probeResponse(input, init);
      if (probe) return probe;
      count += 1;
      return Response.json({ message: 'rate limit' }, { status: 429, headers: { 'Retry-After': '60' } });
    }) as typeof fetch,
  });
  const response = await transport.fetch(`${direct}/auth/v1/otp`, { method: 'POST', body: '{}' });
  assert.equal(response.status, 429);
  assert.equal(response.headers.get('retry-after'), '60');
  assert.equal(count, 1);
});

test('idempotent command may retry once and selected-once may not', async () => {
  const calls: string[] = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null,
    fetchImpl: (async (input, init) => {
      calls.push(String(input));
      const probe = await probeResponse(input, init);
      if (probe) return probe;
      if (String(input).startsWith(direct)) throw new TypeError('degraded');
      return Response.json({ ok: true });
    }) as typeof fetch,
  });
  assert.equal((await transport.fetch(`${direct}/rest/v1/rpc/register_focus_group_participant_v1`, {
    method: 'POST', body: '{}',
  })).ok, true);
  assert.equal(calls.filter((url) => url.includes('register_focus_group_participant_v1')).length, 2);

  let otpCount = 0;
  const auth = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null,
    fetchImpl: (async (input, init) => {
      const probe = await probeResponse(input, init);
      if (probe) return probe;
      otpCount += 1;
      throw new TypeError('ambiguous');
    }) as typeof fetch,
  });
  await assert.rejects(
    () => auth.fetch(`${direct}/auth/v1/otp`, { method: 'POST', body: '{}' }),
    (error) => parseSupabaseTransportError(error)?.code === 'ambiguous',
  );
  assert.equal(otpCount, 1);
});

test('no route prevents selected-once dispatch', async () => {
  const calls: string[] = [];
  const transport = createResilientSupabaseTransport({
    directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null, probeStaggerMs: 0,
    fetchImpl: (async (input) => { calls.push(String(input)); throw new TypeError('offline'); }) as typeof fetch,
  });
  await assert.rejects(
    () => transport.fetch(`${direct}/auth/v1/otp`, { method: 'POST', body: '{}' }),
    (error) => parseSupabaseTransportError(error)?.code === 'no_route',
  );
  assert.equal(calls.filter((url) => url.endsWith('/auth/v1/otp')).length, 0);
});

test('unrelated fetch bypasses the catalog and native fetch keeps its receiver', async () => {
  let seen = '';
  const transport = createResilientSupabaseTransport({
    directUrl: direct, publishableKey: key, sessionStorage: null,
    fetchImpl: (async (input) => { seen = String(input); return Response.json({}); }) as typeof fetch,
  });
  await transport.fetch('https://kenigevents.ru/data/events.json');
  assert.equal(seen, 'https://kenigevents.ru/data/events.json');

  const original = globalThis.fetch;
  globalThis.fetch = function (this: typeof globalThis) {
    assert.equal(this, globalThis);
    return Promise.resolve(Response.json({ version: 'test' }));
  } as typeof fetch;
  try {
    const native = createResilientSupabaseTransport({ directUrl: direct, publishableKey: key, sessionStorage: null });
    assert.equal((await native.probe('direct', 'auth')).ok, true);
  } finally { globalThis.fetch = original; }
});

test('data client singleton stays configuration-keyed', () => {
  resetResilientDataClientRegistryForTests();
  const config = { directUrl: direct, relayUrl: relay, publishableKey: key, sessionStorage: null };
  assert.equal(getResilientDataClient(config), getResilientDataClient(config));
  assert.notEqual(getResilientDataClient(config), getResilientDataClient({ ...config, relayUrl: 'https://other.test' }));
});
