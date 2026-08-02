import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assertTransportFaultBuildDisabled,
  loadTransportFaultRegistry,
  selectedTransportFaultProfile,
} from './transport-fault-build-contract.mjs';

test('fault registry is closed, digest-bound and selects only an enabled non-normal profile', () => {
  const registry = loadTransportFaultRegistry();
  assert.match(registry.digest, /^[0-9a-f]{64}$/u);
  assert.deepEqual(Object.keys(registry.profiles), [
    'normal',
    'client_supabase_direct_unreachable',
    'client_yandex_relay_unreachable',
    'both_client_routes_unreachable',
  ]);
  assert.throws(
    () => selectedTransportFaultProfile({ STATIC_SITE_TRANSPORT_FAULT_PROFILE: 'unknown' }),
    /Unknown transport fault profile/u,
  );
  assert.throws(
    () => selectedTransportFaultProfile({ STATIC_SITE_TRANSPORT_FAULT_PROFILE: 'client_supabase_direct_unreachable' }),
    /STATIC_SITE_TRANSPORT_FAULT_BUILD=1/u,
  );
  const selected = selectedTransportFaultProfile({
    STATIC_SITE_TRANSPORT_FAULT_BUILD: '1',
    STATIC_SITE_TRANSPORT_FAULT_PROFILE: 'client_supabase_direct_unreachable',
  });
  assert.equal(selected.id, 'client_supabase_direct_unreachable');
  assert.equal(selected.rules[0].host_class, 'supabase_direct');
});

test('production and secret-candidate builds reject either fault activation variable', () => {
  assert.doesNotThrow(() => assertTransportFaultBuildDisabled({}, 'production'));
  assert.throws(
    () => assertTransportFaultBuildDisabled({ STATIC_SITE_TRANSPORT_FAULT_BUILD: '1' }, 'production'),
    /forbidden in production/u,
  );
  assert.throws(
    () => assertTransportFaultBuildDisabled({ STATIC_SITE_TRANSPORT_FAULT_PROFILE: 'normal' }, 'secret-candidate'),
    /forbidden in secret-candidate/u,
  );
});
