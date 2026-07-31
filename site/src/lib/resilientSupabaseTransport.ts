export type SupabaseTransportRoute = 'direct' | 'relay';

export interface SupabaseRouteProbe {
  route: SupabaseTransportRoute;
  ok: boolean;
  status: number | null;
  elapsedMs: number;
  error: 'none' | 'timeout' | 'network' | 'http_error' | 'not_configured';
}

export interface SupabaseRouteSelection {
  route: SupabaseTransportRoute;
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
  fetchImpl?: typeof fetch;
  now?: () => number;
  sessionStorage?: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
}

const DEFAULT_PROBE_TIMEOUT_MS = 4_500;
const DEFAULT_CACHE_TTL_MS = 120_000;
const DEFAULT_SAFE_REQUEST_TIMEOUT_MS = 8_000;
const ROUTE_CACHE_KEY = 'ke_supabase_transport_route_v1';

const trimOrigin = (value: string): string => String(value || '').replace(/\/+$/u, '');

function clamp(value: number | undefined, fallback: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, Number(value || fallback)));
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
  private selection: SupabaseRouteSelection | null = null;
  private selecting: Promise<SupabaseRouteSelection> | null = null;

  constructor(config: ResilientSupabaseTransportConfig) {
    this.directUrl = trimOrigin(config.directUrl);
    this.relayUrl = trimOrigin(config.relayUrl || '');
    this.publishableKey = String(config.publishableKey || '');
    if (!this.directUrl || !this.publishableKey) throw new Error('supabase_transport_public_config_missing');
    // Browser-native fetch must keep the Window/global receiver. Invoking an
    // unbound native fetch through an object field can fail in Chromium with
    // `TypeError: Illegal invocation` before a request leaves the device.
    this.rawFetch = config.fetchImpl || globalThis.fetch.bind(globalThis);
    this.now = config.now || (() => Date.now());
    this.storage = config.sessionStorage === undefined ? sessionStorageOrNull() : config.sessionStorage;
    this.probeTimeoutMs = clamp(config.probeTimeoutMs, DEFAULT_PROBE_TIMEOUT_MS, 500, 10_000);
    this.cacheTtlMs = clamp(config.cacheTtlMs, DEFAULT_CACHE_TTL_MS, 5_000, 600_000);
    this.safeRequestTimeoutMs = clamp(
      config.safeRequestTimeoutMs,
      DEFAULT_SAFE_REQUEST_TIMEOUT_MS,
      1_000,
      20_000,
    );
    this.selection = this.readCachedSelection();
    this.fetch = this.fetchRequest.bind(this) as typeof fetch;
  }

  private readCachedSelection(): SupabaseRouteSelection | null {
    try {
      const raw = this.storage?.getItem(ROUTE_CACHE_KEY);
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
    this.selection = selection;
    try {
      this.storage?.setItem(ROUTE_CACHE_KEY, JSON.stringify({
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
      this.storage?.removeItem(ROUTE_CACHE_KEY);
    } catch {
      // The next request will still probe again in memory.
    }
  }

  urlForRoute(route: SupabaseTransportRoute, pathAndQuery: string): string {
    const base = route === 'relay' && this.relayUrl ? this.relayUrl : this.directUrl;
    const normalizedPath = String(pathAndQuery || '').startsWith('/')
      ? String(pathAndQuery || '')
      : `/${pathAndQuery}`;
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
          'X-Client-Info': 'kenigevents-resilient-transport/1',
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
      const timeout = controller.signal.aborted
        || (error instanceof DOMException && error.name === 'AbortError');
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
    if (!force && this.selection && this.now() - this.selection.selectedAt < this.cacheTtlMs) {
      return this.selection;
    }
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
    if (!this.relayUrl) {
      const direct = await this.probe('direct');
      return this.cacheSelection({ route: 'direct', selectedAt: this.now(), probes: [direct] });
    }
    const completed: SupabaseRouteProbe[] = [];
    const pending = [this.probe('direct'), this.probe('relay')];
    const firstHealthy = await new Promise<SupabaseRouteProbe | null>((resolve) => {
      for (const probe of pending) {
        probe.then((result) => {
          completed.push(result);
          if (result.ok) resolve(result);
          else if (completed.length === pending.length) resolve(null);
        }).catch(() => {
          if (completed.length === pending.length) resolve(null);
        });
      }
    });
    const route = firstHealthy?.route || 'relay';
    return this.cacheSelection({ route, selectedAt: this.now(), probes: [...completed] });
  }

  async diagnose(): Promise<SupabaseRouteSelection> {
    this.invalidate();
    const probes = await Promise.all([this.probe('direct'), this.probe('relay')]);
    const healthy = probes.filter((item) => item.ok).sort((left, right) => left.elapsedMs - right.elapsedMs);
    const selection = {
      route: healthy[0]?.route || (this.relayUrl ? 'relay' : 'direct'),
      selectedAt: this.now(),
      probes,
    } satisfies SupabaseRouteSelection;
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
    const destination = route === 'relay'
      ? rewriteOrigin(input, this.directUrl, this.relayUrl)
      : input;
    if (!timeoutMs) return this.rawFetch(destination, init);
    const upstreamSignal = init?.signal || (input instanceof Request ? input.signal : null);
    const bounded = combineAbortSignals(upstreamSignal, timeoutMs);
    try {
      return await this.rawFetch(destination, { ...init, signal: bounded.signal });
    } finally {
      bounded.dispose();
    }
  }

  private async fetchRequest(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const url = requestUrl(input);
    if (url.origin !== new URL(this.directUrl).origin) return this.rawFetch(input, init);
    const method = requestMethod(input, init);
    const selection = await this.selectRoute();
    const safe = isSafeMethod(method);
    try {
      const response = await this.rawRequest(
        selection.route,
        input,
        init,
        safe ? this.safeRequestTimeoutMs : null,
      );
      if (!safe || response.status < 500) return response;
      const alternate = this.alternate(selection.route);
      if (!alternate) return response;
      this.invalidate();
      return this.rawRequest(alternate, input, init, this.safeRequestTimeoutMs);
    } catch (error) {
      this.invalidate();
      const alternate = safe ? this.alternate(selection.route) : null;
      if (!alternate) throw error;
      return this.rawRequest(alternate, input, init, this.safeRequestTimeoutMs);
    }
  }
}

export function createResilientSupabaseTransport(config: ResilientSupabaseTransportConfig) {
  return new ResilientSupabaseTransport(config);
}

export function supabaseAuthStorageKey(supabaseUrl: string): string {
  const hostname = new URL(supabaseUrl).hostname;
  // Keep byte-for-byte parity with @supabase/supabase-js 2.108.2's default.
  const projectRef = hostname.split('.')[0];
  return `sb-${projectRef}-auth-token`;
}
