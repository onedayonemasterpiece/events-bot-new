import assert from 'node:assert/strict';
import test from 'node:test';

import { sanitizedFailureClass, summarizeRuntimeDiagnostics } from '../../e2e/focus-email/helpers/runtime-diagnostics.mjs';

test('a cancelled losing health probe with a successful peer is expected', () => {
  const summary = summarizeRuntimeDiagnostics([
    { path: '/auth/v1/health', host_class: 'direct', status: 200, failure_class: null },
    { path: '/auth/v1/health', host_class: 'relay', status: null, failure_class: 'request_cancelled' },
  ]);
  assert.equal(summary.expected_cancelled_probe_count, 1);
  assert.equal(summary.unexpected_network_failure_count, 0);
  assert.equal(summary.blocking_failure_count, 0);
});

test('a cancellation without a healthy peer is unexpected and blocking', () => {
  const summary = summarizeRuntimeDiagnostics([
    { path: '/auth/v1/health', host_class: 'relay', status: null, failure_class: 'request_cancelled' },
  ]);
  assert.equal(summary.expected_cancelled_probe_count, 0);
  assert.equal(summary.unexpected_network_failure_count, 1);
  assert.equal(summary.blocking_failure_count, 1);
});

test('the declared best-effort verification telemetry 403 is a structured PASS warning', () => {
  const summary = summarizeRuntimeDiagnostics([
    { path: '/rest/v1/rpc/focus_auth_record_verification_v1', status: 403, failure_class: null },
  ], [{ type: 'error', text: 'Failed to load resource: the server responded with a status of 403 ()' }]);
  assert.equal(summary.unexpected_http_4xx_5xx_count, 0);
  assert.equal(summary.warnings[0].code, 'BEST_EFFORT_AUTH_TELEMETRY_403');
  assert.equal(summary.unexpected_console_error_count, 0);
  assert.equal(summary.blocking_failure_count, 0);
});

test('arbitrary HTTP and console failures remain blocking', () => {
  const summary = summarizeRuntimeDiagnostics([
    { path: '/auth/v1/verify', status: 500, failure_class: null },
  ], [{ type: 'error', text: 'unexpected product error' }]);
  assert.equal(summary.warnings.length, 0);
  assert.equal(summary.blocking_failure_count, 2);
});

test('driver failure text is reduced to an allowlisted class', () => {
  assert.equal(sanitizedFailureClass({ canceled: true, errorText: 'secret' }), 'request_cancelled');
  assert.equal(sanitizedFailureClass({ errorText: 'net::ERR_ABORTED' }), 'request_cancelled');
  assert.equal(sanitizedFailureClass({ errorText: 'address containing details' }), 'network_failure');
});
