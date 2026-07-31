export type SupabaseTransportRoute = 'direct' | 'relay';

export type ResilientOperationPolicy = 'safe-read' | 'selected-once' | 'idempotent-replay';

export interface SupabaseRouteProbe {
  route: SupabaseTransportRoute;
  ok: boolean;
  status: number | null;
  elapsedMs: number;
  error: 'none' | 'timeout' | 'network' | 'http_error' | 'not_configured';
}

export interface SupabaseRouteSelection {
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
  safeRequestTimeoutMs?: number;
  selectedRequestTimeoutMs?: number;
  fetchImpl?: typeof fetch;
  now?: () => number;
  sessionStorage?: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
}

export interface ResilientRequestOptions {
  policy: ResilientOperationPolicy;
}

const DEFAULT_PROBE_TIMEOUT_MS = 4_500;
const DEFAULT_CACHE_TTL_MS = 120_000;
const DEFAULT_SAFE_REQUEST_TIMEOUT_MS = 4_000;
const DEFAULT_SELECTED_REQUEST_TIMEOUT_MS = 12_000;
const ROUTE_CACHE_PREFIX = 'ke_supabase_transport_route_v2:';

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

function requestMethod(input: RequestInfo | URL, init?: RequestInit): string {
  return String(init?.method || (input instanceof Request ? input.method : 'GET')).toUpperCase();
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

function isSafeMethod(method: string): boolean {
  return method === 'GET' || method === 'HEAD';
}

function combineAbortSignals(upstream: AbortSignal | null | undefined, timeoutMs: number) {
  const controller = new AbortController();
  const abort = () => controller.abort(upstream?.reason || 'supabase_transport_aborted');
  if (upstream?.aborted) abort();
  else upstream?.addEventListener('abort', abort, { once: true });
  const timer = setTimeout(() => controller.abort('supabase_transport_timeout'), timeoutMs);
  return {
    signal: controller.signal,
    dispose() {
      clearTimeout(timer);
      upstream?.removeEventListener('abort', abort);
    },
  };
}

function sessionStorageOrNull(): Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null {
  try {
    return typeof window === 'undefined' ? null : window.sessionStorage;
  } catch {
    return null;
  }
}

export class SupabaseNoHealthyRouteError extends Error {
  readonly code = 'supabase_transport_no_healthy_route';
  constructor() {
    super('supabase_transport_no_healthy_route');
    this.name = 'SupabaseNoHealthyRouteError';
  }
}

export class SupabaseAmbiguousWriteError extends Error {
  readonly code = 'supabase_transport_ambiguous_result';
  readonly cause: unknown;
  constructor(cause: unknown) {
    super('supabase_transport_ambiguous_result');
    this.name = 'SupabaseAmbiguousWriteError';
    this.cause = cause;
  }
}

export class ResilientSupabaseTransport {
  readonly directUrl: string;
  readonly relayUrl: string;
  readonly publishableKey: string;
  readonly fetch: typeof fetch;
  private readonly rawFetch: typeof fetch;
  private readonly now: () => number;
  private readonly storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
  private readonly probeTimeoutMs: number;
  private readonly cacheTtlMs: number;
  private readonly safeRequestTimeoutMs: number;
  private readonly selectedRequestTimeoutMs: number;
  private readonly routeCacheKey: string;
  private selection: SupabaseRouteSelection | null = null;
  private selecting: Promise<SupabaseRouteSelection> | null = null;
  private lastAmbiguousAt = 0;
  private lastNoHealthyAt = 0;

  constructor(config: ResilientSupabaseTransportConfig) {
    this.directUrl = trimOrigin(config.directUrl);
    this.relayUrl = trimOrigin(config.relayUrl || '');
    this.publishableKey = String(config.publishableKey || '');
    if (!this.directUrl || !this.publishableKey) throw new Error('supabase_transport_public_config_missing');
    // Browser-native fetch must keep the Window/global receiver.
    this.rawFetch = config.fetchImpl || globalThis.fetch.bind(globalThis);
    this.now = config.now || (() => Date.now());
    this.storage = config.sessionStorage === undefined ? sessionStorageOrNull() : config.sessionStorage;
    this.probeTimeoutMs = clamp(config.probeTimeoutMs, DEFAULT_PROBE_TIMEOUT_MS, 500, 10_000);
    this.cacheTtlMs = clamp(config.cacheTtlMs, DEFAULT_CACHE_TTL_MS, 5_000, 600_000);
    this.safeRequestTimeoutMs = clamp(config.safeRequestTimeoutMs, DEFAULT_SAFE_REQUEST_TIMEOUT_MS, 1_000, 20_000);
    this.selectedRequestTimeoutMs = clamp(
      config.selectedRequestTimeoutMs,
      DEFAULT_SELECTED_REQUEST_TIMEOUT_MS,
      2_000,
      30_000,
    );
    this.routeCacheKey = `${ROUTE_CACHE_PREFIX}${compactHash(`${this.directUrl}|${this.relayUrl}`)}`;
    this.selection = this.readCachedSelection();
    this.fetch = this.fetchRequest.bind(this) as typeof fetch;
  }

  private readCachedSelection(): SupabaseRouteSelection | null {
    try {
      const raw = this.storage?.getItem(this.routeCacheKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { route?: string; selectedAt?: number };
      if (parsed.route !== 'direct' && parsed.route !== 'relay') return null;
      if (parsed.route === 'relay' && !this.relayUrl) return null;
      const selectedAt = Number(parsed.selectedAt || 0);
      if (!selectedAt || this.now() - selectedAt >= this.cacheTtlMs) return null;
      return { route: parsed.route, selectedAt, probes: [] };
    } catch {
      return null;
    }
  }

  private cacheSelection(selection: SupabaseRouteSelection): SupabaseRouteSelection {
    this.selection = selection.route ? selection : null;
    if (!selection.route) return selection;
    try {
      this.storage?.setItem(this.routeCacheKey, JSON.stringify({
        route: selection.route,
        selectedAt: selection.selectedAt,
      }));
    } catch {
      // A session-only cache is an optimization, never an Auth dependency.
    }
    return selection;
  }

  invalidate(): void {
    this.selection = null;
    this.selecting = null;
    try {
      this.storage?.removeItem(this.routeCacheKey);
    } catch {
      // The next request will still probe again in memory.
    }
  }

  wasAmbiguousSince(startedAt: number): boolean {
    return this.lastAmbiguousAt >= startedAt;
  }

  hadNoHealthyRouteSince(startedAt: number): boolean {
    return this.lastNoHealthyAt >= startedAt;
  }

  urlForRoute(route: SupabaseTransportRoute, pathAndQuery: string): string {
    const base = route === 'relay' && this.relayUrl ? this.relayUrl : this.directUrl;
    const normalizedPath = String(pathAndQuery || '').startsWith('/') ? String(pathAndQuery || '') : `/${pathAndQuery}`;
    return `${base}${normalizedPath}`;
  }

  async probe(route: SupabaseTransportRoute): Promise<SupabaseRouteProbe> {
    if (route === 'relay' && !this.relayUrl) {
      return { route, ok: false, status: null, elapsedMs: 0, error: 'not_configured' };
    }
    const started = this.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort('supabase_transport_probe_timeout'), this.probeTimeoutMs);
    try {
      const response = await this.rawFetch(this.urlForRoute(route, '/auth/v1/health'), {
        method: 'GET',
        cache: 'no-store',
        credentials: 'omit',
        headers: {
          apikey: this.publishableKey,
          Authorization: `Bearer ${this.publishableKey}`,
          'X-Client-Info': 'kenigevents-resilient-transport/2',
        },
        signal: controller.signal,
      });
      return {
        route,
        ok: response.ok,
        status: response.status,
        elapsedMs: Math.max(0, Math.round(this.now() - started)),
        error: response.ok ? 'none' : 'http_error',
      };
    } catch (error) {
      const timeout = controller.signal.aborted || (error instanceof DOMException && error.name === 'AbortError');
      return {
        route,
        ok: false,
        status: null,
        elapsedMs: Math.max(0, Math.round(this.now() - started)),
        error: timeout ? 'timeout' : 'network',
      };
    } finally {
      clearTimeout(timer);
    }
  }

  async selectRoute(force = false): Promise<SupabaseRouteSelection> {
    if (!force && this.selection?.route && this.now() - this.selection.selectedAt < this.cacheTtlMs) return this.selection;
    if (!force && this.selecting) return this.selecting;
    const work = this.selectRouteOnce();
    this.selecting = work;
    try {
      return await work;
    } finally {
      if (this.selecting === work) this.selecting = null;
    }
  }

  private async selectRouteOnce(): Promise<SupabaseRouteSelection> {
    const routes: SupabaseTransportRoute[] = this.relayUrl ? ['direct', 'relay'] : ['direct'];
    const completed: SupabaseRouteProbe[] = [];
    const pending = routes.map((route) => this.probe(route));
    const firstHealthy = await new Promise<SupabaseRouteProbe | null>((resolve) => {
      let settled = false;
      const finish = (result: SupabaseRouteProbe) => {
        completed.push(result);
        if (!settled && result.ok) {
          settled = true;
          resolve(result);
        } else if (!settled && completed.length === pending.length) {
          settled = true;
          resolve(null);
        }
      };
      pending.forEach((probe, index) => probe.then(finish).catch(() => finish({
        route: routes[index], ok: false, status: null, elapsedMs: 0, error: 'network',
      })));
    });
    const selection = { route: firstHealthy?.route || null, selectedAt: this.now(), probes: [...completed] };
    if (!selection.route) this.lastNoHealthyAt = selection.selectedAt;
    return this.cacheSelection(selection);
  }

  async diagnose(): Promise<SupabaseRouteSelection> {
    this.invalidate();
    const probes = await Promise.all([this.probe('direct'), this.probe('relay')]);
    const healthy = probes.filter((item) => item.ok).sort((left, right) => left.elapsedMs - right.elapsedMs);
    const selection = { route: healthy[0]?.route || null, selectedAt: this.now(), probes } satisfies SupabaseRouteSelection;
    if (!selection.route) this.lastNoHealthyAt = selection.selectedAt;
    return this.cacheSelection(selection);
  }

  private alternate(route: SupabaseTransportRoute): SupabaseTransportRoute | null {
    if (!this.relayUrl) return null;
    return route === 'direct' ? 'relay' : 'direct';
  }

  private async rawRequest(
    route: SupabaseTransportRoute,
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    timeoutMs: number | null,
  ): Promise<Response> {
    const destination = route === 'relay' ? rewriteOrigin(input, this.directUrl, this.relayUrl) : input;
    if (!timeoutMs) return this.rawFetch(destination, init);
    const upstreamSignal = init?.signal || (input instanceof Request ? input.signal : null);
    const bounded = combineAbortSignals(upstreamSignal, timeoutMs);
    try {
      return await this.rawFetch(destination, { ...init, signal: bounded.signal });
    } finally {
      bounded.dispose();
    }
  }

  async request(
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    options: ResilientRequestOptions,
  ): Promise<Response> {
    const url = requestUrl(input);
    if (url.origin !== new URL(this.directUrl).origin) return this.rawFetch(input, init);
    const method = requestMethod(input, init);
    if (options.policy !== 'safe-read' && isSafeMethod(method)) throw new Error('write_policy_requires_non_safe_method');
    const selection = await this.selectRoute();
    if (!selection.route) {
      this.lastNoHealthyAt = this.now();
      throw new SupabaseNoHealthyRouteError();
    }
    const safe = options.policy === 'safe-read';
    const replayable = options.policy === 'idempotent-replay';
    try {
      const response = await this.rawRequest(
        selection.route,
        input,
        init,
        safe ? this.safeRequestTimeoutMs : this.selectedRequestTimeoutMs,
      );
      if ((!safe && !replayable) || response.status < 500) return response;
      const alternate = this.alternate(selection.route);
      if (!alternate) return response;
      this.invalidate();
      const recovered = await this.rawRequest(alternate, input, init, safe ? this.safeRequestTimeoutMs : this.selectedRequestTimeoutMs);
      if (recovered.status < 500) this.cacheSelection({ route: alternate, selectedAt: this.now(), probes: [] });
      return recovered;
    } catch (error) {
      this.invalidate();
      const alternate = safe || replayable ? this.alternate(selection.route) : null;
      if (alternate) {
        const recovered = await this.rawRequest(alternate, input, init, safe ? this.safeRequestTimeoutMs : this.selectedRequestTimeoutMs);
        if (recovered.status < 500) this.cacheSelection({ route: alternate, selectedAt: this.now(), probes: [] });
        return recovered;
      }
      if (!safe) {
        this.lastAmbiguousAt = this.now();
        throw new SupabaseAmbiguousWriteError(error);
      }
      throw error;
    }
  }

  private fetchRequest(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const method = requestMethod(input, init);
    return this.request(input, init, { policy: isSafeMethod(method) ? 'safe-read' : 'selected-once' });
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
