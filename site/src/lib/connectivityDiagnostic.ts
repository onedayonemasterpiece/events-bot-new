export type ConnectivityProbeState =
  | 'ok'
  | 'http_error'
  | 'timeout'
  | 'network_error'
  | 'not_configured';

export interface ConnectivityAttempt {
  state: ConnectivityProbeState;
  status: number | null;
  elapsedMs: number;
  bytes: number | null;
}

export interface ConnectivityProbeResult {
  id: string;
  label: string;
  state: ConnectivityProbeState;
  status: number | null;
  attempts: ConnectivityAttempt[];
  minMs: number | null;
  medianMs: number | null;
  maxMs: number | null;
}

interface ProbeTarget {
  id: string;
  label: string;
  url: string;
  headers?: Record<string, string>;
}

interface RunOptions {
  attempts?: number;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
  now?: () => number;
}

const rounded = (value: number): number => Math.round(value * 10) / 10;

export const summarizeConnectivityAttempts = (
  id: string,
  label: string,
  attempts: ConnectivityAttempt[],
): ConnectivityProbeResult => {
  const timings = attempts
    .filter((item) => item.state !== 'not_configured')
    .map((item) => item.elapsedMs)
    .sort((left, right) => left - right);
  const middle = Math.floor(timings.length / 2);
  const median = timings.length === 0
    ? null
    : timings.length % 2 === 1
      ? timings[middle]
      : (timings[middle - 1] + timings[middle]) / 2;
  const failed = attempts.find((item) => item.state !== 'ok');
  const last = attempts.at(-1);
  return {
    id,
    label,
    state: failed?.state || last?.state || 'not_configured',
    status: failed?.status ?? last?.status ?? null,
    attempts,
    minMs: timings.length ? rounded(timings[0]) : null,
    medianMs: median === null ? null : rounded(median),
    maxMs: timings.length ? rounded(timings.at(-1) as number) : null,
  };
};

const runAttempt = async (
  target: ProbeTarget,
  timeoutMs: number,
  fetchImpl: typeof fetch,
  now: () => number,
): Promise<ConnectivityAttempt> => {
  if (!target.url) {
    return { state: 'not_configured', status: null, elapsedMs: 0, bytes: null };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort('connectivity_probe_timeout'), timeoutMs);
  const startedAt = now();
  try {
    const response = await fetchImpl(target.url, {
      method: 'GET',
      headers: target.headers,
      cache: 'no-store',
      credentials: 'omit',
      signal: controller.signal,
    });
    const bytes = (await response.arrayBuffer()).byteLength;
    return {
      state: response.ok ? 'ok' : 'http_error',
      status: response.status,
      elapsedMs: rounded(now() - startedAt),
      bytes,
    };
  } catch (error) {
    const aborted = controller.signal.aborted
      || (error instanceof DOMException && error.name === 'AbortError');
    return {
      state: aborted ? 'timeout' : 'network_error',
      status: null,
      elapsedMs: rounded(now() - startedAt),
      bytes: null,
    };
  } finally {
    clearTimeout(timer);
  }
};

export const runConnectivityProbe = async (
  target: ProbeTarget,
  options: RunOptions = {},
): Promise<ConnectivityProbeResult> => {
  const count = Math.min(5, Math.max(1, options.attempts ?? 3));
  const timeoutMs = Math.min(15_000, Math.max(1_000, options.timeoutMs ?? 8_000));
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => performance.now());
  const attempts: ConnectivityAttempt[] = [];
  for (let index = 0; index < count; index += 1) {
    attempts.push(await runAttempt(target, timeoutMs, fetchImpl, now));
  }
  return summarizeConnectivityAttempts(target.id, target.label, attempts);
};

export const makeConnectivityReceipt = (
  results: ConnectivityProbeResult[],
  details: {
    origin: string;
    online: boolean;
    effectiveType?: string;
    standalone?: boolean;
    checkedAt?: string;
  },
) => ({
  schema: 'kenigevents.focus_connectivity.v1',
  checked_at: details.checkedAt || new Date().toISOString(),
  origin: details.origin,
  online: details.online,
  effective_type: String(details.effectiveType || 'unknown').slice(0, 16),
  standalone: details.standalone === true,
  probes: results.map((result) => ({
    id: result.id,
    state: result.state,
    status: result.status,
    min_ms: result.minMs,
    median_ms: result.medianMs,
    max_ms: result.maxMs,
    attempts: result.attempts.map((attempt) => ({
      state: attempt.state,
      status: attempt.status,
      elapsed_ms: attempt.elapsedMs,
      bytes: attempt.bytes,
    })),
  })),
});

