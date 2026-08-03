import assert from 'node:assert/strict';
import test from 'node:test';

import {
  NO_MAIL_FAULT_PROFILES,
  NO_MAIL_OPERATION_MATRIX,
  runNoMailFaultMatrix,
} from '../e2e/auth-session-fixture/noMailFaultMatrix.ts';

const SELECTED_ONCE_OPERATIONS = ['auth', 'search', 'personalization'] as const;

test('the zero-mail matrix exposes every required deterministic failure class', () => {
  assert.deepEqual(NO_MAIL_FAULT_PROFILES, [
    'normal',
    'client_supabase_direct_unreachable',
    'client_yandex_relay_unreachable',
    'both_client_routes_unreachable',
    'supabase_upstream_unavailable',
    'selected_once_response_body_ambiguous',
    'recovery_after_reload',
  ]);
});

test('Auth, Search, personalization and focus never issue OTP or touch mail', async () => {
  for (const profile of NO_MAIL_FAULT_PROFILES) {
    const receipt = await runNoMailFaultMatrix(profile);
    assert.equal(receipt.product_otp_issue_count, 0, profile);
    assert.equal(receipt.external_mail_send_count, 0, profile);
    assert.equal(receipt.external_mail_receipt_count, 0, profile);
    assert.equal(receipt.duplicate_dispatch_count, 0, profile);
    assert.equal(receipt.duplicate_effect_count, 0, profile);
    assert.equal(receipt.selected_once_dispatch_violation_count, 0, profile);
    assert.equal(receipt.false_relay_recovery_count, 0, profile);
    assert.deepEqual(Object.keys(receipt.operations), Object.keys(NO_MAIL_OPERATION_MATRIX));

    for (const operation of Object.values(receipt.operations)) {
      if (operation.expected_policy === 'selected-once') {
        assert.ok(operation.dispatch_count <= 1, `${profile}:${operation.operation}:selected-once`);
      }
      assert.equal(operation.duplicate_effect_count, 0, `${profile}:${operation.operation}:effect`);
      assert.equal(
        operation.fault_activation_codes.length > 0,
        profile !== 'normal',
        `${profile}:${operation.operation}:fault activation`,
      );
    }

    const serialized = JSON.stringify(receipt);
    for (const forbidden of [
      'direct.supabase.co',
      'relay.example.invalid',
      '/auth/v1/verify',
      '/functions/v1/event-search',
      'fixture-auth-action',
      'fixture-search-action',
      'fixture-personalization-action',
      'fixture-focus-action',
      'Authorization',
      'Bearer ',
    ]) {
      assert.equal(serialized.includes(forbidden), false, `${profile}:receipt leaked ${forbidden}`);
    }
    assert.equal(receipt.fault_activation.sensitive_fields_omitted, true, profile);
    assert.equal(receipt.fault_activation.activated, receipt.fault_activation.expected, profile);
  }
});

test('normal and one-route-down profiles dispatch once and create one effect', async () => {
  for (const profile of [
    'normal',
    'client_supabase_direct_unreachable',
    'client_yandex_relay_unreachable',
  ] as const) {
    const receipt = await runNoMailFaultMatrix(profile);
    for (const [name, operation] of Object.entries(receipt.operations)) {
      assert.equal(operation.outcome, 'PASS', `${profile}:${name}`);
      assert.equal(operation.dispatch_count, 1, `${profile}:${name}`);
      assert.equal(operation.effect_count, 1, `${profile}:${name}`);
      assert.equal(operation.local_state, 'committed', `${profile}:${name}`);
      const expectedRoute = profile === 'client_supabase_direct_unreachable' ? 'relay' : 'direct';
      assert.equal(operation.selected_route, expectedRoute, `${profile}:${name}`);
    }
  }
});

test('both routes down performs zero product dispatch and retains pending intent', async () => {
  const receipt = await runNoMailFaultMatrix('both_client_routes_unreachable');
  for (const [name, operation] of Object.entries(receipt.operations)) {
    assert.equal(operation.outcome, 'NO_HEALTHY_ROUTE', name);
    assert.equal(operation.selected_route, null, name);
    assert.equal(operation.dispatch_count, 0, name);
    assert.equal(operation.effect_count, 0, name);
    assert.equal(operation.local_state, 'pending', name);
  }
  assert.ok(receipt.fault_activation.codes.includes('direct.probe.client_unreachable'));
  assert.ok(receipt.fault_activation.codes.includes('relay.probe.client_unreachable'));
});

test('shared Supabase upstream failure is never reported as relay recovery', async () => {
  const receipt = await runNoMailFaultMatrix('supabase_upstream_unavailable');
  for (const name of SELECTED_ONCE_OPERATIONS) {
    const operation = receipt.operations[name];
    assert.equal(operation.outcome, 'SHARED_UPSTREAM_UNAVAILABLE', name);
    assert.equal(operation.dispatch_count, 1, name);
    assert.equal(operation.effect_count, 0, name);
    assert.equal(operation.transport_outcome_kind, 'ambiguous', name);
    assert.equal(operation.false_relay_recovery, false, name);
  }
  const focus = receipt.operations.focus;
  assert.equal(focus.outcome, 'SHARED_UPSTREAM_UNAVAILABLE');
  assert.equal(focus.dispatch_count, 2);
  assert.equal(focus.effect_count, 0);
  assert.equal(focus.transport_outcome_kind, 'transport_failure');
  assert.equal(focus.false_relay_recovery, false);
  assert.deepEqual(focus.dispatched_routes, ['direct', 'relay']);
});

test('ambiguous body never replays selected-once but idempotent focus converges to one effect', async () => {
  const receipt = await runNoMailFaultMatrix('selected_once_response_body_ambiguous');
  for (const name of SELECTED_ONCE_OPERATIONS) {
    const operation = receipt.operations[name];
    assert.equal(operation.outcome, 'AMBIGUOUS_SELECTED_ONCE', name);
    assert.equal(operation.dispatch_count, 1, name);
    assert.equal(operation.effect_count, 1, name);
    assert.equal(operation.transport_outcome_kind, 'ambiguous', name);
    assert.deepEqual(operation.dispatched_routes, ['direct'], name);
  }
  const focus = receipt.operations.focus;
  assert.equal(focus.outcome, 'RECOVERED_IDEMPOTENT');
  assert.equal(focus.dispatch_count, 2);
  assert.equal(focus.effect_count, 1);
  assert.equal(focus.duplicate_effect_count, 0);
  assert.equal(focus.transport_outcome_kind, 'recovered');
  assert.deepEqual(focus.dispatched_routes, ['direct', 'relay']);
});

test('stable pending intent survives reload and recovery without a duplicate dispatch/effect', async () => {
  const receipt = await runNoMailFaultMatrix('recovery_after_reload');
  for (const [name, operation] of Object.entries(receipt.operations)) {
    assert.equal(operation.outcome, 'RECOVERED_AFTER_RELOAD', name);
    assert.equal(operation.initial_dispatch_count, 0, name);
    assert.equal(operation.recovery_dispatch_count, 1, name);
    assert.equal(operation.dispatch_count, 1, name);
    assert.equal(operation.effect_count, 1, name);
    assert.equal(operation.reload_survived, true, name);
    assert.equal(operation.stable_action_id_preserved, true, name);
    assert.equal(operation.local_state, 'committed', name);
  }
});

test('selected-once and idempotent policies stay explicit by product operation', () => {
  assert.equal(NO_MAIL_OPERATION_MATRIX.auth.policy, 'selected-once');
  assert.equal(NO_MAIL_OPERATION_MATRIX.search.policy, 'selected-once');
  assert.equal(NO_MAIL_OPERATION_MATRIX.personalization.policy, 'selected-once');
  assert.equal(NO_MAIL_OPERATION_MATRIX.focus.policy, 'idempotent-replay');
});
