import assert from 'node:assert/strict';
import test from 'node:test';
import {
  makeConnectivityReceipt,
  runConnectivityProbe,
  summarizeConnectivityAttempts,
} from './connectivityDiagnostic.ts';

test('summarizes bounded timings and preserves the first failure class', () => {
  const summary = summarizeConnectivityAttempts('auth', 'Auth', [
    { state: 'ok', status: 200, elapsedMs: 180, bytes: 107 },
    { state: 'network_error', status: null, elapsedMs: 8000, bytes: null },
    { state: 'ok', status: 200, elapsedMs: 120, bytes: 107 },
  ]);
  assert.equal(summary.state, 'network_error');
  assert.equal(summary.minMs, 120);
  assert.equal(summary.medianMs, 180);
  assert.equal(summary.maxMs, 8000);
});

test('runs sequential no-store probes and measures response bytes', async () => {
  let time = 0;
  const calls: RequestInit[] = [];
  const result = await runConnectivityProbe(
    { id: 'ydb', label: 'YDB', url: 'https://example.test/control' },
    {
      attempts: 3,
      now: () => {
        time += 25;
        return time;
      },
      fetchImpl: (async (_url, init) => {
        calls.push(init || {});
        return new Response('{"status":"ready"}', { status: 200 });
      }) as typeof fetch,
    },
  );
  assert.equal(result.state, 'ok');
  assert.equal(result.medianMs, 25);
  assert.deepEqual(result.attempts.map((attempt) => attempt.bytes), [18, 18, 18]);
  assert.equal(calls.length, 3);
  assert.ok(calls.every((call) => call.cache === 'no-store'));
  assert.ok(calls.every((call) => call.credentials === 'omit'));
});

test('accepts a completed opaque response only for the transport-only probe', async () => {
  const opaqueResponse = {
    ok: false,
    status: 0,
    type: 'opaque',
    arrayBuffer: async () => new ArrayBuffer(0),
  } as Response;
  const result = await runConnectivityProbe(
    {
      id: 'direct',
      label: 'Direct',
      url: 'https://example.test/health?probe=ABCD-1234',
      mode: 'no-cors',
      acceptOpaque: true,
    },
    {
      fetchImpl: (async (_url, init) => {
        assert.equal(init?.mode, 'no-cors');
        return opaqueResponse;
      }) as typeof fetch,
    },
  );
  assert.equal(result.state, 'ok');
  assert.equal(result.status, null);
  assert.equal(result.attempts.length, 1);
});

test('uses one 20-second attempt by default instead of multiplying the wait', async () => {
  const calls: RequestInit[] = [];
  const result = await runConnectivityProbe(
    { id: 'auth', label: 'Auth', url: 'https://example.test/health' },
    {
      fetchImpl: (async (_url, init) => {
        calls.push(init || {});
        return new Response('{}', { status: 200 });
      }) as typeof fetch,
    },
  );
  assert.equal(result.state, 'ok');
  assert.equal(result.attempts.length, 1);
  assert.equal(calls.length, 1);
});

test('receipt is compact and contains no address, token, key or user agent', () => {
  const receipt = makeConnectivityReceipt([
    summarizeConnectivityAttempts('auth', 'Auth', [
      { state: 'ok', status: 200, elapsedMs: 120, bytes: 107 },
    ]),
  ], {
    origin: 'https://kenigevents.ru',
    online: true,
    effectiveType: '4g',
    standalone: true,
    checkedAt: '2026-07-31T09:30:00.000Z',
  });
  const raw = JSON.stringify(receipt);
  assert.doesNotMatch(raw, /email|token|apikey|authorization|user.?agent/iu);
  assert.match(raw, /2026-07-31T09:30:00.000Z/u);
  assert.ok(raw.includes('"median_ms":120'));
});
