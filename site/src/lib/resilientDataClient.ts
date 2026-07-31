import {
  createResilientSupabaseTransport,
  type ResilientOperationPolicy,
  type ResilientSupabaseTransport,
  type ResilientSupabaseTransportConfig,
} from './resilientSupabaseTransport.ts';

export interface ResilientDataClientConfig extends ResilientSupabaseTransportConfig {}

export interface ResilientDataRequestOptions {
  policy: ResilientOperationPolicy;
}

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
  ].join('|');
}

function registry(): Map<string, ResilientDataClient> {
  const owner = globalThis as typeof globalThis & {
    [GLOBAL_REGISTRY_KEY]?: Map<string, ResilientDataClient>;
  };
  if (!owner[GLOBAL_REGISTRY_KEY]) owner[GLOBAL_REGISTRY_KEY] = new Map();
  return owner[GLOBAL_REGISTRY_KEY];
}

function methodOf(input: RequestInfo | URL, init?: RequestInit): string {
  return String(init?.method || (input instanceof Request ? input.method : 'GET')).toUpperCase();
}

export class ResilientDataClient {
  readonly transport: ResilientSupabaseTransport;
  readonly fetch: typeof fetch;
  readonly key: string;

  constructor(config: ResilientDataClientConfig) {
    this.key = configKey(config);
    this.transport = createResilientSupabaseTransport(config);
    this.fetch = this.fetchDefault.bind(this) as typeof fetch;
  }

  request(input: RequestInfo | URL, init: RequestInit | undefined, options: ResilientDataRequestOptions) {
    return this.transport.request(input, init, options);
  }

  safeRead(input: RequestInfo | URL, init: RequestInit = {}) {
    return this.request(input, { ...init, method: init.method || 'GET' }, { policy: 'safe-read' });
  }

  selectedOnce(input: RequestInfo | URL, init: RequestInit) {
    return this.request(input, init, { policy: 'selected-once' });
  }

  idempotentReplay(input: RequestInfo | URL, init: RequestInit) {
    return this.request(input, init, { policy: 'idempotent-replay' });
  }

  private fetchDefault(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const method = methodOf(input, init);
    return this.request(input, init, {
      policy: method === 'GET' || method === 'HEAD' ? 'safe-read' : 'selected-once',
    });
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
