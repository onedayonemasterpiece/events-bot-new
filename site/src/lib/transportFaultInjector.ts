import type { ResilientSupabaseTransportConfig } from './resilientSupabaseTransport.ts';

export interface TransportFaultResolution {
  fetchImpl?: typeof fetch;
  profileId: string;
  cacheNamespace: string;
}

export function transportFaultProfileIdentity(): string {
  return 'normal';
}

export function resolveTransportFaultInjection(
  config: ResilientSupabaseTransportConfig,
): TransportFaultResolution {
  return {
    fetchImpl: config.fetchImpl,
    profileId: 'normal',
    cacheNamespace: 'normal',
  };
}
