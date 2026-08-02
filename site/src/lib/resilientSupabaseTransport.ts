import {
  classifyBackendOperation,
  policyForOperation,
  type BackendCapability,
  type BackendOperationDefinition,
} from './backendOperationCatalog.ts';

export type SupabaseTransportRoute = 'direct' | 'relay';
export type ResilientOperationPolicy = 'safe-read' | 'selected-once' | 'idempotent-replay';
export type SupabaseTransportFailurePhase = 'dispatch' | 'headers' | 'body' | 'decode';

export interface SupabaseRouteProbe {
  route: SupabaseTransportRoute;
  capability: BackendCapability;
  ok: boolean;
  status: number | null;
  elapsedMs: number;
  error: 'none' | 'timeout' | 'network' | 'http_error' | 'protocol' | 'not_configured' | 'quarantined';
}

export interface SupabaseRouteSelection {
  capability: BackendCapability;
  route: SupabaseTransportRoute | null;
  selectedAt: number;
  probes: SupabaseRouteProbe[];
}

export interface ResilientSupabaseTransportConfig {
  directUrl: string;
  relayUrl?: string;
  publishableKey: string;
  probeTimeoutMs?: number;
  cacheTtlMs?: number;
  staleCacheTtlMs?: number;
  probeStaggerMs?: number;
  safeRequestTimeoutMs?: number;
  selectedRequestTimeoutMs?: number;
  fetchImpl?: typeof fetch;
  routeCacheNamespace?: string;
  now?: () => number;
  persistentStorage?: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
  /** @deprecated Kept while callers migrate from the v2 config name. */
  sessionStorage?: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
}

export interface ResilientRequestOptions {
  policy?: ResilientOperationPolicy;
  operation?: BackendOperationDefinition;
}

export type SupabaseTransportOutcomeKind =
  | 'definitive'
  | 'recovered'
  | 'ambiguous'
  | 'no_route'
  | 'transport_failure';

export interface SupabaseTransportOutcome {
  operationId: string;
  operation: string;
  capability: BackendCapability;
  policy: ResilientOperationPolicy;
  initialRoute: SupabaseTransportRoute | null;
  finalRoute: SupabaseTransportRoute | null;
  kind: SupabaseTransportOutcomeKind;
  status: number | null;
  phase: SupabaseTransportFailurePhase | 'selection' | null;
  startedAt: number;
  elapsedMs: number;
}

interface CircuitState {
  failures: number;
  openUntil: number;
}

interface InternalPhaseError extends Error {
  transportPhase?: SupabaseTransportFailurePhase;
}

const DEFAULT_PROBE_TIMEOUT_MS = 3_000;
const DEFAULT_CACHE_TTL_MS = 300_000;
const DEFAULT_STALE_CACHE_TTL_MS = 1_800_000;
const DEFAULT_PROBE_STAGGER_MS = 225;
const DEFAULT_SAFE_REQUEST_TIMEOUT_MS = 4_000;
const DEFAULT_SELECTED_REQUEST_TIMEOUT_MS = 12_000;
const ROUTE_CACHE_PREFIX = 'ke_supabase_transport_route_v3:';
const TRANSPORT_ERROR_PREFIX = 'ke_transport_v3';
const CONTRACT_VERSION = 3;
const CIRCUIT_DELAYS_MS = [30_000, 60_000, 120_000, 300_000] as const;
const OUTCOME_HISTORY_LIMIT = 24;

const trimOrigin = (value: string): string => String(value || '').replace(/\/+$/u, '');

function clamp(value: number | undefined, fallback: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, Number(value || fallback)));
}

function compactHash(value: string): string {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function requestUrl(input: RequestInfo | URL): URL {
  if (input instanceof Request) return new URL(input.url);
  return new URL(String(input));
}

function rewriteOrigin(input: RequestInfo | URL, fromOrigin: string, toOrigin: string): RequestInfo | URL {
  const url = requestUrl(input);
  if (url.origin !== new URL(fromOrigin).origin) return input;
  const destination = new URL(toOrigin);
  destination.pathname = `${destination.pathname.replace(/\/$/u, '')}${url.pathname}` || '/';
  destination.search = url.search;
  destination.hash = '';
  if (input instanceof Request) return new Request(destination, input);
  if (input instanceof URL) return destination;
  return destination.toString();
}

function combineAbortSignals(upstream: AbortSignal | null | undefined, timeoutMs: number) {
  const controller = new AbortController();
  const abort = () => controller.abort(upstream?.reason || 'supabase_transport_aborted');
  if (upstream?.aborted) abort();
  else upstream?.addEventListener('abort', abort, { once: true });
  let timer: ReturnType<typeof setTimeout> | null = setTimeout(
    () => controller.abort('supabase_transport_timeout'),
    timeoutMs,
  );
  const clearDeadline = () => {
    if (!timer) return;
    clearTimeout(timer);
    timer = null;
  };
  return {
    controller,
    signal: controller.signal,
    // Streaming operations use the deadline only while opening the response.
    // The caller's AbortSignal remains linked until the stream is completed or
    // cancelled, while the search UI owns its per-chunk idle watchdog.
    clearDeadline,
    dispose() {
      clearDeadline();
      upstream?.removeEventListener('abort', abort);
    },
  };
}

function browserPersistentStorageOrNull(): Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null {
  try {
    return typeof window === 'undefined' ? null : window.localStorage;
  } catch {
    return null;
  }
}

function operationId(): string {
  try {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  } catch {
    // Fall through to a non-secret correlation id.
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
}

function phaseError(phase: SupabaseTransportFailurePhase, cause: unknown): InternalPhaseError {
  const error = cause instanceof Error ? cause as InternalPhaseError : new Error(String(cause || 'transport_failure')) as InternalPhaseError;
  error.transportPhase = phase;
  return error;
}

function isAbortError(error: unknown, signal?: AbortSignal): boolean {
  return Boolean(signal?.aborted || (error instanceof DOMException && error.name === 'AbortError'));
}

function transportErrorMessage(
  code: string,
  id: string,
  route: SupabaseTransportRoute | 'none',
  phase: SupabaseTransportFailurePhase | 'selection',
): string {
  return `${TRANSPORT_ERROR_PREFIX}|${code}|${id}|${route}|${phase}`;
}

export interface SupabaseTransportErrorDetails {
  code: 'ambiguous' | 'no_route';
  operationId: string;
  route: SupabaseTransportRoute | null;
  phase: SupabaseTransportFailurePhase | 'selection';
}

export function parseSupabaseTransportError(error: unknown): SupabaseTransportErrorDetails | null {
  const message = String((error as Error)?.message || error || '');
  const marker = message.match(/ke_transport_v3\|(ambiguous|no_route)\|([^|\s]+)\|(direct|relay|none)\|(dispatch|headers|body|decode|selection)/u);
  if (!marker) return null;
  return {
    code: marker[1] as 'ambiguous' | 'no_route',
    operationId: marker[2],
    route: marker[3] === 'none' ? null : marker[3] as SupabaseTransportRoute,
    phase: marker[4] as SupabaseTransportFailurePhase | 'selection',
  };
}

export class SupabaseNoHealthyRouteError extends Error {
  readonly code = 'supabase_transport_no_healthy_route';
  readonly operationId: string;
  constructor(id = operationId()) {
    super(transportErrorMessage('no_route', id, 'none', 'selection'));
    this.name = 'SupabaseNoHealthyRouteError';
    this.operationId = id;
  }
}

export class SupabaseAmbiguousWriteError extends Error {
  readonly code = 'supabase_transport_ambiguous_result';
  readonly cause: unknown;
  readonly operationId: string;
  readonly route: SupabaseTransportRoute;
  readonly phase: SupabaseTransportFailurePhase;
  readonly possiblyCommitted = true;
  constructor(cause: unknown, id = operationId(), route: SupabaseTransportRoute = 'direct', phase: SupabaseTransportFailurePhase = 'dispatch') {
    super(transportErrorMessage('ambiguous', id, route, phase));
    this.name = 'SupabaseAmbiguousWriteError';
    this.cause = cause;
    this.operationId = id;
    this.route = route;
    this.phase = phase;
  }
}

function responseWithTransportHeaders(response: Response, body: BodyInit | null, route: SupabaseTransportRoute, id: string): Response {
  const headers = new Headers(response.headers);
  headers.set('x-ke-transport-route', route);
  headers.set('x-ke-transport-operation', id);
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

async function readBoundedBody(
  response: Response,
  operation: BackendOperationDefinition,
  signal: AbortSignal,
): Promise<Uint8Array> {
  const contentLength = Number(response.headers.get('content-length') || 0);
  if (Number.isFinite(contentLength) && contentLength > operation.responseLimitBytes) {
    throw phaseError('body', new Error('supabase_transport_response_too_large'));
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      if (signal.aborted) throw new DOMException('transport aborted', 'AbortError');
      const item = await reader.read();
      if (item.done) break;
      if (!item.value) continue;
      total += item.value.byteLength;
      if (total > operation.responseLimitBytes) {
        await reader.cancel('supabase_transport_response_too_large').catch(() => {});
        throw new Error('supabase_transport_response_too_large');
      }
      chunks.push(item.value);
    }
  } catch (error) {
    throw phaseError('body', error);
  } finally {
    try { reader.releaseLock(); } catch { /* best effort */ }
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function validateBufferedProtocol(response: Response, body: Uint8Array, operation: BackendOperationDefinition): void {
  if (operation.responseMode !== 'buffered-json' || response.status === 204 || body.byteLength === 0) return;
  try {
    JSON.parse(new TextDecoder().decode(body));
  } catch (error) {
    throw phaseError('decode', error);
  }
}

function streamResponseWithDeadline(
  response: Response,
  route: SupabaseTransportRoute,
  id: string,
  bounded: ReturnType<typeof combineAbortSignals>,
  onComplete: () => void,
  onFailure: (error: unknown) => Error,
): Response {
  bounded.clearDeadline();
  if (!response.body) {
    bounded.dispose();
    onComplete();
    return responseWithTransportHeaders(response, null, route, id);
  }
  const reader = response.body.getReader();
  let closed = false;
  const finish = () => {
    if (closed) return;
    closed = true;
    bounded.dispose();
    try { reader.releaseLock(); } catch { /* best effort */ }
  };
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const item = await reader.read();
        if (item.done) {
          finish();
          onComplete();
          controller.close();
          return;
        }
        controller.enqueue(item.value);
      } catch (error) {
        finish();
        controller.error(onFailure(error));
      }
    },
    async cancel(reason) {
      try { await reader.cancel(reason); } finally { finish(); }
    },
  });
  return responseWithTransportHeaders(response, body, route, id);
}

export class ResilientSupabaseTransport {
  readonly directUrl: string;
  readonly relayUrl: string;
  readonly publishableKey: string;
  readonly fetch: typeof fetch;
  readonly contractVersion = CONTRACT_VERSION;
  private readonly rawFetch: typeof fetch;
  private readonly now: () => number;
  private readonly storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
  private readonly probeTimeoutMs: number;
  private readonly cacheTtlMs: number;
  private readonly staleCacheTtlMs: number;
  private readonly probeStaggerMs: number;
  private readonly safeRequestTimeoutMs: number;
  private readonly selectedRequestTimeoutMs: number;
  private readonly routeCachePrefix: string;
  private readonly selections = new Map<BackendCapability, SupabaseRouteSelection>();
  private readonly selecting = new Map<BackendCapability, Promise<SupabaseRouteSelection>>();
  private readonly circuits = new Map<string, CircuitState>();
  private readonly outcomes: SupabaseTransportOutcome[] = [];

  constructor(config: ResilientSupabaseTransportConfig) {
    this.directUrl = trimOrigin(config.directUrl);
    this.relayUrl = trimOrigin(config.relayUrl || '');
    this.publishableKey = String(config.publishableKey || '');
    if (!this.directUrl || !this.publishableKey) throw new Error('supabase_transport_public_config_missing');
    this.rawFetch = config.fetchImpl || globalThis.fetch.bind(globalThis);
    this.now = config.now || (() => Date.now());
    this.storage = config.persistentStorage !== undefined
      ? config.persistentStorage
      : config.sessionStorage !== undefined
        ? config.sessionStorage
        : browserPersistentStorageOrNull();
    this.probeTimeoutMs = clamp(config.probeTimeoutMs, DEFAULT_PROBE_TIMEOUT_MS, 500, 10_000);
    this.cacheTtlMs = clamp(config.cacheTtlMs, DEFAULT_CACHE_TTL_MS, 5_000, 600_000);
    this.staleCacheTtlMs = clamp(config.staleCacheTtlMs, DEFAULT_STALE_CACHE_TTL_MS, this.cacheTtlMs, 3_600_000);
    this.probeStaggerMs = clamp(config.probeStaggerMs, DEFAULT_PROBE_STAGGER_MS, 0, 1_000);
    this.safeRequestTimeoutMs = clamp(config.safeRequestTimeoutMs, DEFAULT_SAFE_REQUEST_TIMEOUT_MS, 1_000, 20_000);
    this.selectedRequestTimeoutMs = clamp(config.selectedRequestTimeoutMs, DEFAULT_SELECTED_REQUEST_TIMEOUT_MS, 2_000, 30_000);
    this.routeCachePrefix = `${ROUTE_CACHE_PREFIX}${compactHash(`${this.directUrl}|${this.relayUrl}|${config.routeCacheNamespace || 'normal'}`)}:`;
    this.fetch = this.fetchRequest.bind(this) as typeof fetch;
  }

  private storageKey(capability: BackendCapability): string {
    return `${this.routeCachePrefix}${capability}`;
  }

  private readCachedSelection(capability: BackendCapability): SupabaseRouteSelection | null {
    const memory = this.selections.get(capability);
    if (memory && this.now() - memory.selectedAt < this.staleCacheTtlMs) return memory;
    try {
      const raw = this.storage?.getItem(this.storageKey(capability));
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { v?: number; r?: string; t?: number };
      if (parsed.v !== CONTRACT_VERSION || (parsed.r !== 'direct' && parsed.r !== 'relay')) return null;
      if (parsed.r === 'relay' && !this.relayUrl) return null;
      const selectedAt = Number(parsed.t || 0);
      if (!selectedAt || this.now() - selectedAt >= this.staleCacheTtlMs) return null;
      const selection = { capability, route: parsed.r, selectedAt, probes: [] } satisfies SupabaseRouteSelection;
      this.selections.set(capability, selection);
      return selection;
    } catch {
      return null;
    }
  }

  private cacheSelection(selection: SupabaseRouteSelection): SupabaseRouteSelection {
    if (!selection.route) return selection;
    this.selections.set(selection.capability, selection);
    try {
      this.storage?.setItem(this.storageKey(selection.capability), JSON.stringify({
        v: CONTRACT_VERSION,
        r: selection.route,
        t: selection.selectedAt,
      }));
    } catch {
      // Route state is a compact optimization, never an Auth dependency.
    }
    return selection;
  }

  invalidate(capability?: BackendCapability): void {
    const capabilities: BackendCapability[] = capability
      ? [capability]
      : ['auth', 'data', 'functions', 'storage-small', 'oauth-navigation'];
    for (const item of capabilities) {
      this.selections.delete(item);
      this.selecting.delete(item);
      try { this.storage?.removeItem(this.storageKey(item)); } catch { /* best effort */ }
    }
  }

  latestOutcome(operation?: string, startedAfter = 0): SupabaseTransportOutcome | null {
    for (let index = this.outcomes.length - 1; index >= 0; index -= 1) {
      const outcome = this.outcomes[index];
      if (outcome.startedAt < startedAfter) continue;
      if (!operation || outcome.operation === operation) return { ...outcome };
    }
    return null;
  }

  outcomeHistory(startedAfter = 0): SupabaseTransportOutcome[] {
    return this.outcomes.filter((outcome) => outcome.startedAt >= startedAfter).map((outcome) => ({ ...outcome }));
  }

  private recordOutcome(outcome: SupabaseTransportOutcome): void {
    this.outcomes.push(outcome);
    if (this.outcomes.length > OUTCOME_HISTORY_LIMIT) {
      this.outcomes.splice(0, this.outcomes.length - OUTCOME_HISTORY_LIMIT);
    }
  }

  urlForRoute(route: SupabaseTransportRoute, pathAndQuery: string): string {
    const base = route === 'relay' && this.relayUrl ? this.relayUrl : this.directUrl;
    const normalizedPath = String(pathAndQuery || '').startsWith('/') ? String(pathAndQuery || '') : `/${pathAndQuery}`;
    return `${base}${normalizedPath}`;
  }

  private circuitKey(capability: BackendCapability, route: SupabaseTransportRoute): string {
    return `${capability}:${route}`;
  }

  private routeQuarantined(capability: BackendCapability, route: SupabaseTransportRoute): boolean {
    const state = this.circuits.get(this.circuitKey(capability, route));
    return Boolean(state && state.openUntil > this.now());
  }

  private markRouteFailure(capability: BackendCapability, route: SupabaseTransportRoute): void {
    const key = this.circuitKey(capability, route);
    const previous = this.circuits.get(key);
    const failures = Math.min((previous?.failures || 0) + 1, CIRCUIT_DELAYS_MS.length);
    this.circuits.set(key, {
      failures,
      openUntil: this.now() + CIRCUIT_DELAYS_MS[failures - 1],
    });
    const selected = this.selections.get(capability);
    if (selected?.route === route) this.invalidate(capability);
  }

  private markRouteSuccess(capability: BackendCapability, route: SupabaseTransportRoute): void {
    this.circuits.delete(this.circuitKey(capability, route));
    this.cacheSelection({ capability, route, selectedAt: this.now(), probes: [] });
  }

  private probeRequest(capability: BackendCapability): {
    path: string;
    init: RequestInit;
    validate: (payload: unknown, nonce: string) => boolean;
    nonce: string;
  } {
    const nonce = operationId();
    const commonHeaders = {
      apikey: this.publishableKey,
      Authorization: `Bearer ${this.publishableKey}`,
      'X-Client-Info': `kenigevents-resilient-transport/${CONTRACT_VERSION}`,
    };
    if (capability === 'data') {
      return {
        path: '/rest/v1/rpc/transport_probe_v1',
        init: {
          method: 'POST',
          cache: 'no-store',
          credentials: 'omit',
          headers: { ...commonHeaders, 'Content-Type': 'application/json' },
          body: JSON.stringify({ p_nonce: nonce }),
        },
        validate: (payload, expected) => Boolean(payload && typeof payload === 'object'
          && (payload as { nonce?: unknown }).nonce === expected
          && (payload as { schema?: unknown }).schema === 1),
        nonce,
      };
    }
    if (capability === 'functions') {
      return {
        path: '/functions/v1/transport-probe',
        init: {
          method: 'POST',
          cache: 'no-store',
          credentials: 'omit',
          headers: { ...commonHeaders, 'Content-Type': 'application/json' },
          body: JSON.stringify({ nonce }),
        },
        validate: (payload, expected) => Boolean(payload && typeof payload === 'object'
          && (payload as { nonce?: unknown }).nonce === expected
          && (payload as { schema?: unknown }).schema === 1),
        nonce,
      };
    }
    return {
      path: '/auth/v1/health',
      init: { method: 'GET', cache: 'no-store', credentials: 'omit', headers: commonHeaders },
      validate: (payload) => Boolean(payload && typeof payload === 'object'),
      nonce,
    };
  }

  async probe(
    route: SupabaseTransportRoute,
    capability: BackendCapability = 'auth',
    parentSignal?: AbortSignal,
  ): Promise<SupabaseRouteProbe> {
    if (route === 'relay' && !this.relayUrl) {
      return { route, capability, ok: false, status: null, elapsedMs: 0, error: 'not_configured' };
    }
    if (this.routeQuarantined(capability, route)) {
      return { route, capability, ok: false, status: null, elapsedMs: 0, error: 'quarantined' };
    }
    const started = this.now();
    const probe = this.probeRequest(capability);
    const bounded = combineAbortSignals(parentSignal, this.probeTimeoutMs);
    let status: number | null = null;
    try {
      const response = await this.rawFetch(this.urlForRoute(route, probe.path), {
        ...probe.init,
        signal: bounded.signal,
      });
      status = response.status;
      if (!response.ok) {
        await readBoundedBody(response, {
          name: 'transport.probe', capability, semantics: 'safe-read', responseMode: 'buffered-json',
          responseLimitBytes: 64 * 1024, routeSupport: ['direct', 'relay'],
        }, bounded.signal).catch(() => new Uint8Array());
        return { route, capability, ok: false, status, elapsedMs: Math.max(0, Math.round(this.now() - started)), error: 'http_error' };
      }
      const body = await readBoundedBody(response, {
        name: 'transport.probe', capability, semantics: 'safe-read', responseMode: 'buffered-json',
        responseLimitBytes: 64 * 1024, routeSupport: ['direct', 'relay'],
      }, bounded.signal);
      const payload = JSON.parse(new TextDecoder().decode(body));
      const ok = probe.validate(payload, probe.nonce);
      if (ok) this.circuits.delete(this.circuitKey(capability, route));
      return {
        route,
        capability,
        ok,
        status,
        elapsedMs: Math.max(0, Math.round(this.now() - started)),
        error: ok ? 'none' : 'protocol',
      };
    } catch (error) {
      const timeout = isAbortError(error, bounded.signal);
      return {
        route,
        capability,
        ok: false,
        status,
        elapsedMs: Math.max(0, Math.round(this.now() - started)),
        error: timeout ? 'timeout' : error instanceof SyntaxError ? 'protocol' : 'network',
      };
    } finally {
      bounded.dispose();
    }
  }

  async selectRoute(
    force = false,
    capability: BackendCapability = 'auth',
    routeSupport: readonly SupabaseTransportRoute[] = ['direct', 'relay'],
  ): Promise<SupabaseRouteSelection> {
    const cached = this.readCachedSelection(capability);
    if (!force && cached?.route && routeSupport.includes(cached.route)
      && this.now() - cached.selectedAt < this.cacheTtlMs
      && !this.routeQuarantined(capability, cached.route)) return cached;
    const inFlight = this.selecting.get(capability);
    if (!force && inFlight) return inFlight;
    const work = this.selectRouteOnce(capability, routeSupport, cached?.route || null);
    this.selecting.set(capability, work);
    try {
      return await work;
    } finally {
      if (this.selecting.get(capability) === work) this.selecting.delete(capability);
    }
  }

  private async selectRouteOnce(
    capability: BackendCapability,
    routeSupport: readonly SupabaseTransportRoute[],
    lastKnown: SupabaseTransportRoute | null,
  ): Promise<SupabaseRouteSelection> {
    const configured = routeSupport.filter((route) => route === 'direct' || Boolean(this.relayUrl));
    const available = configured.filter((route) => !this.routeQuarantined(capability, route));
    const routes = [...available].sort((left, right) => {
      if (left === lastKnown) return -1;
      if (right === lastKnown) return 1;
      return left === 'direct' ? -1 : 1;
    });
    if (!routes.length) return { capability, route: null, selectedAt: this.now(), probes: [] };
    if (routes.length === 1) {
      const only = await this.probe(routes[0], capability);
      const selection = { capability, route: only.ok ? only.route : null, selectedAt: this.now(), probes: [only] } satisfies SupabaseRouteSelection;
      return selection.route ? this.cacheSelection(selection) : selection;
    }

    const completed: SupabaseRouteProbe[] = [];
    const controllers = new Map<SupabaseTransportRoute, AbortController>();
    let secondaryStarted = false;
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const result = await new Promise<SupabaseRouteProbe | null>((resolve) => {
      const finish = (probe: SupabaseRouteProbe) => {
        if (settled) return;
        completed.push(probe);
        if (probe.ok) {
          settled = true;
          if (timer) clearTimeout(timer);
          for (const [route, controller] of controllers) {
            if (route !== probe.route) controller.abort('alternate_probe_cancelled');
          }
          resolve(probe);
          return;
        }
        if (probe.route === routes[0] && !secondaryStarted) startSecondary();
        if (secondaryStarted && completed.length >= routes.length) {
          settled = true;
          resolve(null);
        }
      };
      const start = (route: SupabaseTransportRoute) => {
        const controller = new AbortController();
        controllers.set(route, controller);
        void this.probe(route, capability, controller.signal).then(finish).catch(() => finish({
          route, capability, ok: false, status: null, elapsedMs: 0, error: 'network',
        }));
      };
      const startSecondary = () => {
        if (secondaryStarted || settled) return;
        secondaryStarted = true;
        start(routes[1]);
      };
      start(routes[0]);
      timer = setTimeout(startSecondary, this.probeStaggerMs);
    });

    const selection = { capability, route: result?.route || null, selectedAt: this.now(), probes: [...completed] } satisfies SupabaseRouteSelection;
    return selection.route ? this.cacheSelection(selection) : selection;
  }

  async diagnose(capability: BackendCapability = 'auth'): Promise<SupabaseRouteSelection> {
    this.invalidate(capability);
    const probes = await Promise.all([this.probe('direct', capability), this.probe('relay', capability)]);
    const healthy = probes.filter((item) => item.ok).sort((left, right) => left.elapsedMs - right.elapsedMs);
    const selection = { capability, route: healthy[0]?.route || null, selectedAt: this.now(), probes } satisfies SupabaseRouteSelection;
    return selection.route ? this.cacheSelection(selection) : selection;
  }

  private alternate(route: SupabaseTransportRoute, operation: BackendOperationDefinition): SupabaseTransportRoute | null {
    if (!this.relayUrl) return null;
    const alternate = route === 'direct' ? 'relay' : 'direct';
    if (!operation.routeSupport.includes(alternate) || this.routeQuarantined(operation.capability, alternate)) return null;
    return alternate;
  }

  private async executeRoute(
    route: SupabaseTransportRoute,
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    timeoutMs: number,
    operation: BackendOperationDefinition,
    id: string,
  ): Promise<Response> {
    const destination = route === 'relay' ? rewriteOrigin(input, this.directUrl, this.relayUrl) : input;
    const upstreamSignal = init?.signal || (input instanceof Request ? input.signal : null);
    const bounded = combineAbortSignals(upstreamSignal, timeoutMs);
    let response: Response;
    try {
      response = await this.rawFetch(destination, { ...init, signal: bounded.signal });
    } catch (error) {
      bounded.dispose();
      throw phaseError('dispatch', error);
    }

    if (operation.responseMode === 'stream' && response.ok) {
      return streamResponseWithDeadline(
        response,
        route,
        id,
        bounded,
        () => this.markRouteSuccess(operation.capability, route),
        (error) => {
          this.markRouteFailure(operation.capability, route);
          return new SupabaseAmbiguousWriteError(error, id, route, 'body');
        },
      );
    }

    try {
      const bufferedOperation = operation.responseMode === 'stream'
        ? { ...operation, responseMode: 'buffered-json' as const }
        : operation;
      const body = bufferedOperation.responseMode === 'empty'
        ? new Uint8Array()
        : await readBoundedBody(response, bufferedOperation, bounded.signal);
      validateBufferedProtocol(response, body, bufferedOperation);
      const replayBody = body.byteLength ? body.slice().buffer as ArrayBuffer : null;
      return responseWithTransportHeaders(response, replayBody, route, id);
    } finally {
      bounded.dispose();
    }
  }

  async request(
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    options: ResilientRequestOptions = {},
  ): Promise<Response> {
    const url = requestUrl(input);
    if (url.origin !== new URL(this.directUrl).origin) return this.rawFetch(input, init);
    const operation = options.operation || classifyBackendOperation(input, init);
    const expectedPolicy = policyForOperation(operation);
    if (options.policy && options.policy !== expectedPolicy) {
      throw new Error(`backend_operation_policy_mismatch:${operation.name}:${options.policy}:${expectedPolicy}`);
    }
    const id = operationId();
    const startedAt = this.now();
    let initialRoute: SupabaseTransportRoute | null = null;
    const record = (
      kind: SupabaseTransportOutcomeKind,
      finalRoute: SupabaseTransportRoute | null,
      status: number | null,
      phase: SupabaseTransportFailurePhase | 'selection' | null = null,
    ) => this.recordOutcome({
      operationId: id,
      operation: operation.name,
      capability: operation.capability,
      policy: expectedPolicy,
      initialRoute,
      finalRoute,
      kind,
      status,
      phase,
      startedAt,
      elapsedMs: Math.max(0, Math.round(this.now() - startedAt)),
    });
    const selection = await this.selectRoute(false, operation.capability, operation.routeSupport);
    if (!selection.route) {
      record('no_route', null, null, 'selection');
      throw new SupabaseNoHealthyRouteError(id);
    }
    initialRoute = selection.route;
    const replayable = expectedPolicy === 'safe-read' || expectedPolicy === 'idempotent-replay';
    const timeoutMs = replayable ? this.safeRequestTimeoutMs : this.selectedRequestTimeoutMs;

    try {
      const response = await this.executeRoute(selection.route, input, init, timeoutMs, operation, id);
      if (!replayable && response.status >= 500) {
        throw new SupabaseAmbiguousWriteError(new Error(`upstream_${response.status}`), id, selection.route, 'headers');
      }
      if (operation.responseMode === 'stream' && response.ok) return response;
      if (!replayable || response.status < 500) {
        this.markRouteSuccess(operation.capability, selection.route);
        record('definitive', selection.route, response.status);
        return response;
      }
      const alternate = this.alternate(selection.route, operation);
      if (!alternate) {
        record('definitive', selection.route, response.status);
        return response;
      }
      const recovered = await this.executeRoute(alternate, input, init, timeoutMs, operation, id);
      if (recovered.status < 500) this.markRouteSuccess(operation.capability, alternate);
      record('recovered', alternate, recovered.status);
      return recovered;
    } catch (error) {
      if (error instanceof SupabaseAmbiguousWriteError) {
        record('ambiguous', error.route, null, error.phase);
        throw error;
      }
      const phase = (error as InternalPhaseError)?.transportPhase || 'dispatch';
      this.markRouteFailure(operation.capability, selection.route);
      const alternate = replayable ? this.alternate(selection.route, operation) : null;
      if (alternate) {
        try {
          const recovered = await this.executeRoute(alternate, input, init, timeoutMs, operation, id);
          if (recovered.status < 500) this.markRouteSuccess(operation.capability, alternate);
          record('recovered', alternate, recovered.status);
          return recovered;
        } catch (alternateError) {
          const alternatePhase = (alternateError as InternalPhaseError)?.transportPhase || 'dispatch';
          this.markRouteFailure(operation.capability, alternate);
          if (expectedPolicy === 'idempotent-replay') {
            record('transport_failure', alternate, null, alternatePhase);
            throw alternateError;
          }
          record('transport_failure', alternate, null, alternatePhase);
          throw phaseError(alternatePhase, alternateError);
        }
      }
      if (!replayable) {
        record('ambiguous', selection.route, null, phase);
        throw new SupabaseAmbiguousWriteError(error, id, selection.route, phase);
      }
      record('transport_failure', selection.route, null, phase);
      throw error;
    }
  }

  private fetchRequest(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    return this.request(input, init);
  }
}

export function createResilientSupabaseTransport(config: ResilientSupabaseTransportConfig) {
  return new ResilientSupabaseTransport(config);
}

export function supabaseAuthStorageKey(supabaseUrl: string): string {
  const hostname = new URL(supabaseUrl).hostname;
  const projectRef = hostname.split('.')[0];
  return `sb-${projectRef}-auth-token`;
}
