import type { ResilientSupabaseTransportConfig } from './resilientSupabaseTransport.ts';

interface TransportFaultResolution {
  fetchImpl?: typeof fetch;
  profileId: string;
  cacheNamespace: string;
}

declare const __KENIGEVENTS_TRANSPORT_FAULT_PROFILE__: {
  id: string;
  registry_digest: string;
  rules: Array<{
    host_class: 'supabase_direct' | 'yandex_supabase_relay';
    failure: 'network_reject';
  }>;
};

type FaultHostClass = 'supabase_direct' | 'yandex_supabase_relay';

interface SanitizedFaultEvent {
  schema_version: 1;
  fault_profile: string;
  injector_layer: 'resilient_transport_fetch';
  host_class: FaultHostClass;
  failure: 'network_reject';
  method: string;
  hit_count: number;
  activated_at: string;
}

interface SanitizedTransportEvent {
  schema_version: 1;
  fault_profile: string;
  injector_layer: 'resilient_transport_fetch';
  host_class: FaultHostClass;
  route: 'direct' | 'relay';
  operation_path: string;
  method: string;
  result: 'fault_injected' | 'response' | 'network_failure';
  status: number | null;
  sequence: number;
  observed_at: string;
}

const GLOBAL_EVENTS_KEY = 'KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:events';
const GLOBAL_TRANSPORT_EVENTS_KEY = 'KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:transport-events';
const GLOBAL_RECEIPT_KEY = 'KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:receipt';
const MAX_EVENTS = 64;

function requestUrl(input: RequestInfo | URL): URL {
  if (input instanceof Request) return new URL(input.url);
  return new URL(String(input));
}

function eventStore(): SanitizedFaultEvent[] {
  const owner = globalThis as typeof globalThis & { [GLOBAL_EVENTS_KEY]?: SanitizedFaultEvent[] };
  if (!owner[GLOBAL_EVENTS_KEY]) owner[GLOBAL_EVENTS_KEY] = [];
  return owner[GLOBAL_EVENTS_KEY];
}

function emitFaultEvent(event: SanitizedFaultEvent): void {
  const events = eventStore();
  events.push(event);
  if (events.length > MAX_EVENTS) events.splice(0, events.length - MAX_EVENTS);
}

function transportEventStore(): SanitizedTransportEvent[] {
  const owner = globalThis as typeof globalThis & { [GLOBAL_TRANSPORT_EVENTS_KEY]?: SanitizedTransportEvent[] };
  if (!owner[GLOBAL_TRANSPORT_EVENTS_KEY]) owner[GLOBAL_TRANSPORT_EVENTS_KEY] = [];
  return owner[GLOBAL_TRANSPORT_EVENTS_KEY];
}

function operationPath(pathname: string): string {
  const known = [
    '/auth/v1/health',
    '/auth/v1/otp',
    '/auth/v1/verify',
    '/rest/v1/rpc/transport_probe_v1',
    '/rest/v1/rpc/register_focus_group_participant_v1',
    '/functions/v1/transport-probe',
  ];
  return known.find((value) => pathname.endsWith(value)) || '/other';
}

function emitTransportEvent(event: Omit<SanitizedTransportEvent, 'sequence' | 'observed_at'>): void {
  const events = transportEventStore();
  events.push({ ...event, sequence: events.length + 1, observed_at: new Date().toISOString() });
  if (events.length > MAX_EVENTS) events.splice(0, events.length - MAX_EVENTS);
}

function registerReceipt(profile: { id: string; registry_digest: string }): void {
  const owner = globalThis as typeof globalThis & { [GLOBAL_RECEIPT_KEY]?: Record<string, unknown> };
  owner[GLOBAL_RECEIPT_KEY] = Object.freeze({
    schema_version: 1,
    profile_id: profile.id,
    registry_digest: profile.registry_digest,
    injector_layer: 'resilient_transport_fetch',
  });
}

export function transportFaultProfileIdentity(): string {
  return `${__KENIGEVENTS_TRANSPORT_FAULT_PROFILE__.id}:${__KENIGEVENTS_TRANSPORT_FAULT_PROFILE__.registry_digest}`;
}

export function createFaultInjectingFetch(
  config: Pick<ResilientSupabaseTransportConfig, 'directUrl' | 'relayUrl' | 'fetchImpl'>,
  profile = __KENIGEVENTS_TRANSPORT_FAULT_PROFILE__,
): typeof fetch {
  registerReceipt(profile);
  const baseFetch = config.fetchImpl || globalThis.fetch.bind(globalThis);
  const origins = new Map<string, FaultHostClass>();
  origins.set(new URL(config.directUrl).origin, 'supabase_direct');
  if (config.relayUrl) origins.set(new URL(config.relayUrl).origin, 'yandex_supabase_relay');
  let hitCount = 0;

  return (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = requestUrl(input);
    const hostClass = origins.get(url.origin);
    if (!hostClass) return baseFetch(input, init);
    const method = String(init?.method || (input instanceof Request ? input.method : 'GET')).toUpperCase();
    const common = {
      schema_version: 1 as const,
      fault_profile: profile.id,
      injector_layer: 'resilient_transport_fetch' as const,
      host_class: hostClass,
      route: hostClass === 'supabase_direct' ? 'direct' as const : 'relay' as const,
      operation_path: operationPath(url.pathname),
      method,
    };
    const rule = hostClass ? profile.rules.find((item) => item.host_class === hostClass) : undefined;
    if (!rule) {
      try {
        const response = await baseFetch(input, init);
        emitTransportEvent({ ...common, result: 'response', status: response.status });
        return response;
      } catch (error) {
        emitTransportEvent({ ...common, result: 'network_failure', status: null });
        throw error;
      }
    }
    hitCount += 1;
    emitFaultEvent({
      schema_version: 1,
      fault_profile: profile.id,
      injector_layer: 'resilient_transport_fetch',
      host_class: hostClass,
      failure: rule.failure,
      method,
      hit_count: hitCount,
      activated_at: new Date().toISOString(),
    });
    emitTransportEvent({ ...common, result: 'fault_injected', status: null });
    throw new TypeError('transport_fault_injected_network_reject');
  }) as typeof fetch;
}

export function resolveTransportFaultInjection(
  config: ResilientSupabaseTransportConfig,
): TransportFaultResolution {
  return {
    fetchImpl: createFaultInjectingFetch(config),
    profileId: __KENIGEVENTS_TRANSPORT_FAULT_PROFILE__.id,
    cacheNamespace: transportFaultProfileIdentity(),
  };
}
