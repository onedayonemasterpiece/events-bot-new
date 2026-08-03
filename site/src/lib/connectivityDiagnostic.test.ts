import assert from 'node:assert/strict';
import test from 'node:test';
import {
  diagnoseConnectivity,
  makeCompactConnectivityReceipt,
  makeConnectivityReceipt,
  runConnectivityProbe,
  summarizeConnectivityAttempts,
  type ConnectivityProbeResult,
} from './connectivityDiagnostic.ts';

const probe = (
  id: string,
  state: ConnectivityProbeResult['state'],
  options: { status?: number | null; elapsedMs?: number; route?: 'direct' | 'relay' | null } = {},
): ConnectivityProbeResult => summarizeConnectivityAttempts(id, id, [{
  state,
  status: options.status ?? (state === 'ok' ? 200 : null),
  elapsedMs: options.elapsedMs ?? 120,
  bytes: state === 'ok' ? 2 : null,
  route: options.route ?? null,
}]);

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

test('captures the actual resilient route from the sanitized response header', async () => {
  const result = await runConnectivityProbe(
    { id: 'framework-data', label: 'Data', url: 'https://example.test/data' },
    {
      fetchImpl: (async () => new Response('{}', {
        status: 200,
        headers: { 'x-ke-transport-route': 'relay' },
      })) as typeof fetch,
    },
  );
  assert.equal(result.route, 'relay');
  assert.equal(result.attempts[0]?.route, 'relay');
});

test('accepts a completed opaque response only for the transport-only probe', async () => {
  const opaqueResponse = {
    ok: false,
    status: 0,
    type: 'opaque',
    headers: new Headers(),
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

test('classifies the field screenshot pattern as direct core with Yandex degraded', () => {
  const results = [
    probe('direct-auth', 'ok', { elapsedMs: 1387, route: 'direct' }),
    probe('direct-data', 'ok', { elapsedMs: 1386, route: 'direct' }),
    probe('relay-auth', 'network_error', { elapsedMs: 311, route: 'relay' }),
    probe('relay-data', 'network_error', { elapsedMs: 320, route: 'relay' }),
    probe('framework-auth', 'ok', { elapsedMs: 261, route: 'direct' }),
    probe('framework-data', 'ok', { elapsedMs: 260, route: 'direct' }),
    probe('ydb-control', 'network_error', { elapsedMs: 303 }),
  ];
  const diagnosis = diagnoseConnectivity(results, { online: true });
  assert.equal(diagnosis.code, 'CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED');
  assert.equal(diagnosis.severity, 'degraded');
  assert.equal(diagnosis.canContinue, true);
  assert.equal(diagnosis.authRoute, 'direct');
  assert.equal(diagnosis.dataRoute, 'direct');
  assert.match(diagnosis.headline, /доступны напрямую/iu);
  assert.match(diagnosis.detail, /не является доказательством глобального сбоя/iu);
  assert.equal(diagnosis.confirmedActionsNeedRepeat, false);
});

test('classifies reciprocal direct outage as relay recovery', () => {
  const diagnosis = diagnoseConnectivity([
    probe('direct-auth', 'timeout', { route: 'direct' }),
    probe('direct-data', 'timeout', { route: 'direct' }),
    probe('relay-auth', 'ok', { route: 'relay' }),
    probe('relay-data', 'ok', { route: 'relay' }),
    probe('framework-auth', 'ok', { route: 'relay' }),
    probe('framework-data', 'ok', { route: 'relay' }),
    probe('ydb-control', 'ok'),
  ], { online: true });
  assert.equal(diagnosis.code, 'CORE_AVAILABLE_RELAY_DIRECT_DEGRADED');
  assert.equal(diagnosis.authRoute, 'relay');
  assert.equal(diagnosis.dataRoute, 'relay');
  assert.equal(diagnosis.canContinue, true);
});

test('keeps YDB-only failure separate from core transport availability', () => {
  const diagnosis = diagnoseConnectivity([
    probe('direct-auth', 'ok', { route: 'direct' }),
    probe('direct-data', 'ok', { route: 'direct' }),
    probe('relay-auth', 'ok', { route: 'relay' }),
    probe('relay-data', 'ok', { route: 'relay' }),
    probe('framework-auth', 'ok', { route: 'direct' }),
    probe('framework-data', 'ok', { route: 'direct' }),
    probe('ydb-control', 'timeout'),
  ], { online: true });
  assert.equal(diagnosis.code, 'CORE_AVAILABLE_YDB_DEGRADED');
  assert.equal(diagnosis.core, 'available');
  assert.equal(diagnosis.ydb, 'unavailable');
  assert.equal(diagnosis.canContinue, true);
});

test('blocks when both resilient core operations are unavailable', () => {
  const diagnosis = diagnoseConnectivity([
    probe('direct-auth', 'network_error', { route: 'direct' }),
    probe('direct-data', 'network_error', { route: 'direct' }),
    probe('relay-auth', 'network_error', { route: 'relay' }),
    probe('relay-data', 'network_error', { route: 'relay' }),
    probe('framework-auth', 'network_error'),
    probe('framework-data', 'network_error'),
    probe('ydb-control', 'network_error'),
  ], { online: true });
  assert.equal(diagnosis.code, 'CORE_UNAVAILABLE');
  assert.equal(diagnosis.severity, 'blocked');
  assert.equal(diagnosis.canContinue, false);
  assert.match(diagnosis.guidance, /Не нажимайте отправку многократно/iu);
});

test('compact v2 receipt contains the classification and actual routes without PII', () => {
  const results = [
    probe('direct-auth', 'ok', { route: 'direct' }),
    probe('direct-data', 'ok', { route: 'direct' }),
    probe('relay-auth', 'network_error', { route: 'relay' }),
    probe('relay-data', 'network_error', { route: 'relay' }),
    probe('framework-auth', 'ok', { route: 'direct' }),
    probe('framework-data', 'ok', { route: 'direct' }),
    probe('ydb-control', 'network_error'),
  ];
  const diagnosis = diagnoseConnectivity(results, { online: true });
  const receipt = makeCompactConnectivityReceipt(results, {
    probeId: '0845-0CD4',
    checkedAt: '2026-08-03T08:25:46.066Z',
    diagnosis,
    mode: 'WEB',
    effectiveType: '4g',
    serviceWorkerActive: true,
    online: true,
  });
  assert.match(receipt, /^KE5 /u);
  assert.match(receipt, /CODE=CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED/u);
  assert.match(receipt, /FA=OK\/120@D/u);
  assert.match(receipt, /PATHA=D PATHD=D/u);
  assert.doesNotMatch(receipt, /email|token|apikey|authorization|user.?agent/iu);
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
