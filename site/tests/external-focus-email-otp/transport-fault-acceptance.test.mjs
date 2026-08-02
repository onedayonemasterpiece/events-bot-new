import assert from 'node:assert/strict';
import test from 'node:test';

import { assertExpectedTransportRoutes } from '../../e2e/focus-email/helpers/transport-fault-acceptance.mjs';

const mandatoryOutcomes = (route) => [
  { operation: 'auth.otp', finalRoute: route },
  { operation: 'auth.verify', finalRoute: route },
  { operation: 'rpc.register_focus_group_participant_v1', finalRoute: route },
];

test('direct-unreachable acceptance requires one relay outcome per mandatory operation', () => {
  assert.doesNotThrow(() => assertExpectedTransportRoutes(
    'client_supabase_direct_unreachable', mandatoryOutcomes('relay'),
  ));
});

test('relay-unreachable acceptance requires one direct outcome per mandatory operation', () => {
  assert.doesNotThrow(() => assertExpectedTransportRoutes(
    'client_yandex_relay_unreachable', mandatoryOutcomes('direct'),
  ));
});

test('fault acceptance rejects an opposite-route or duplicate mandatory outcome', () => {
  assert.throws(() => assertExpectedTransportRoutes('client_yandex_relay_unreachable', [
    ...mandatoryOutcomes('direct'),
    { operation: 'auth.otp', finalRoute: 'relay' },
  ]), /fault_route_selection:auth\.otp:1:1/u);
});
