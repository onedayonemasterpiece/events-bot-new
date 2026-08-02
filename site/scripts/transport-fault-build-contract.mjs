import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const TRANSPORT_FAULT_BUILD_FLAG = 'STATIC_SITE_TRANSPORT_FAULT_BUILD';
export const TRANSPORT_FAULT_PROFILE_ENV = 'STATIC_SITE_TRANSPORT_FAULT_PROFILE';
export const TRANSPORT_FAULT_SENTINEL = 'KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1';

const HOST_CLASSES = new Set(['supabase_direct', 'yandex_supabase_relay']);
const FAILURES = new Set(['network_reject']);
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const registryPath = resolve(repoRoot, 'docs/testing/transport-fault-profiles.v1.yml');

export function loadTransportFaultRegistry() {
  const source = readFileSync(registryPath, 'utf8');
  const registry = JSON.parse(source);
  if (registry?.schema !== 'static_site_transport_fault_profiles.v1' || !registry.profiles) {
    throw new Error('Invalid transport fault registry schema');
  }
  for (const [profileId, profile] of Object.entries(registry.profiles)) {
    if (!/^[a-z][a-z0-9_]*$/u.test(profileId) || !Array.isArray(profile?.rules)) {
      throw new Error(`Invalid transport fault profile: ${profileId}`);
    }
    for (const rule of profile.rules) {
      if (!HOST_CLASSES.has(rule?.host_class) || !FAILURES.has(rule?.failure)) {
        throw new Error(`Invalid transport fault rule in ${profileId}`);
      }
    }
  }
  return {
    ...registry,
    digest: createHash('sha256').update(source).digest('hex'),
  };
}

export function selectedTransportFaultProfile(env = process.env) {
  const profileId = String(env[TRANSPORT_FAULT_PROFILE_ENV] || 'normal').trim();
  const enabled = String(env[TRANSPORT_FAULT_BUILD_FLAG] || '') === '1';
  const registry = loadTransportFaultRegistry();
  const profile = registry.profiles[profileId];
  if (!profile) throw new Error(`Unknown transport fault profile: ${profileId}`);
  if (profileId !== 'normal' && !enabled) {
    throw new Error(`${TRANSPORT_FAULT_BUILD_FLAG}=1 is required for fault profile ${profileId}`);
  }
  if (enabled && profileId === 'normal') {
    throw new Error(`${TRANSPORT_FAULT_BUILD_FLAG}=1 requires a non-normal fault profile`);
  }
  return { id: profileId, ...profile, registry_digest: registry.digest, enabled };
}

export function assertTransportFaultBuildDisabled(env = process.env, target = 'release') {
  const enabled = String(env[TRANSPORT_FAULT_BUILD_FLAG] || '').trim();
  const profile = String(env[TRANSPORT_FAULT_PROFILE_ENV] || '').trim();
  if (enabled || profile) throw new Error(`Transport fault injection is forbidden in ${target} builds`);
}

export function removeTransportFaultBuildEnv(env) {
  delete env[TRANSPORT_FAULT_BUILD_FLAG];
  delete env[TRANSPORT_FAULT_PROFILE_ENV];
  return env;
}
