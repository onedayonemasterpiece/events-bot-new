import {
  createResilientSupabaseTransport,
  type ResilientSupabaseTransport,
  type ResilientSupabaseTransportConfig,
} from './resilientSupabaseTransport.ts';
import {
  resolveTransportFaultInjection,
  transportFaultProfileIdentity,
} from './transportFaultInjector.ts';

export interface ResilientDataClientConfig extends ResilientSupabaseTransportConfig {}

const GLOBAL_REGISTRY_KEY = '__KENIGEVENTS_RESILIENT_DATA_CLIENTS_V1__';

function normalizedOrigin(value: string | undefined): string {
  return String(value || '').replace(/\/+$/u, '');
}

function compactHash(value: string): string {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function configKey(config: ResilientDataClientConfig): string {
  return [
    normalizedOrigin(config.directUrl),
    normalizedOrigin(config.relayUrl),
    compactHash(String(config.publishableKey || '')),
    transportFaultProfileIdentity(),
  ].join('|');
}

function registry(): Map<string, ResilientDataClient> {
  const owner = globalThis as typeof globalThis & {
    [GLOBAL_REGISTRY_KEY]?: Map<string, ResilientDataClient>;
  };
  if (!owner[GLOBAL_REGISTRY_KEY]) owner[GLOBAL_REGISTRY_KEY] = new Map();
  return owner[GLOBAL_REGISTRY_KEY];
}

export class ResilientDataClient {
  readonly transport: ResilientSupabaseTransport;
  readonly fetch: typeof fetch;
  readonly key: string;

  constructor(config: ResilientDataClientConfig) {
    this.key = configKey(config);
    const fault = resolveTransportFaultInjection(config);
    this.transport = createResilientSupabaseTransport({
      ...config,
      fetchImpl: fault.fetchImpl,
      routeCacheNamespace: fault.cacheNamespace,
    });
    this.fetch = this.fetchDefault.bind(this) as typeof fetch;
  }

  request(input: RequestInfo | URL, init?: RequestInit) {
    // Feature code supplies only the HTTP request. Retry, replay and route
    // semantics are owned exclusively by backendOperationCatalog.
    return this.transport.request(input, init);
  }

  private fetchDefault(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    return this.request(input, init);
  }
}

export function getResilientDataClient(config: ResilientDataClientConfig): ResilientDataClient {
  const key = configKey(config);
  const clients = registry();
  const existing = clients.get(key);
  if (existing) return existing;
  const client = new ResilientDataClient(config);
  clients.set(key, client);
  return client;
}

export function createIsolatedResilientDataClient(config: ResilientDataClientConfig): ResilientDataClient {
  return new ResilientDataClient(config);
}

export function resetResilientDataClientRegistryForTests(): void {
  registry().clear();
}
