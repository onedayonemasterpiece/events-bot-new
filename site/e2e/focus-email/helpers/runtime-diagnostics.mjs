const HEALTH_PATH = '/auth/v1/health';
const TELEMETRY_PATH = '/rest/v1/rpc/focus_auth_record_verification_v1';

function cancellation(entry) {
  return entry?.failure_class === 'request_cancelled';
}

export function summarizeRuntimeDiagnostics(entries = [], consoles = []) {
  const healthSuccesses = entries.filter((entry) => entry.path === HEALTH_PATH
    && Number(entry.status) >= 200 && Number(entry.status) < 300);
  const expectedCancelled = entries.filter((entry) => entry.path === HEALTH_PATH
    && cancellation(entry) && healthSuccesses.some((peer) => peer !== entry
      && peer.host_class && entry.host_class && peer.host_class !== entry.host_class));
  const unexpectedNetwork = entries.filter((entry) => entry.failure_class
    && !expectedCancelled.includes(entry));
  const httpFailures = entries.filter((entry) => Number(entry.status) >= 400);
  const telemetry403 = httpFailures.filter((entry) => entry.path === TELEMETRY_PATH && Number(entry.status) === 403);
  const unexpectedHttp = httpFailures.filter((entry) => !telemetry403.includes(entry));
  const warnings = [];
  if (telemetry403.length) warnings.push({
    code: 'BEST_EFFORT_AUTH_TELEMETRY_403',
    operation: 'focus_auth_record_verification_v1',
    status: 403,
    count: telemetry403.length,
    policy: 'warning',
  });
  const generic403Console = consoles.filter((item) => item?.type === 'error'
    && /Failed to load resource:.*status of 403/iu.test(String(item?.text || ''))).slice(0, telemetry403.length);
  const unexpectedConsole = consoles.filter((item) => item?.type === 'error'
    && !(telemetry403.length && generic403Console.includes(item)));
  return {
    expected_cancelled_probe_count: expectedCancelled.length,
    unexpected_network_failure_count: unexpectedNetwork.length,
    unexpected_http_4xx_5xx_count: unexpectedHttp.length,
    unexpected_console_error_count: unexpectedConsole.length,
    warnings,
    blocking_failure_count: unexpectedNetwork.length + unexpectedHttp.length + unexpectedConsole.length,
  };
}

export function sanitizedFailureClass({ canceled = false, errorText = '' } = {}) {
  return canceled || /abort|cancel/iu.test(String(errorText)) ? 'request_cancelled' : 'network_failure';
}
