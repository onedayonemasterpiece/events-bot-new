import assert from 'node:assert/strict';
import test from 'node:test';

import {
  NO_MAIL_FAULT_PROFILES,
  NO_MAIL_OPERATION_MATRIX,
  runNoMailFaultMatrix,
} from '../e2e/auth-session-fixture/noMailFaultMatrix.ts';

test('Auth, Search, personalization and focus use a zero-mail deterministic route matrix', async () => {
  for (const profile of NO_MAIL_FAULT_PROFILES) {
    const receipt = await runNoMailFaultMatrix(profile);
    assert.equal(receipt.product_otp_issue_count, 0, profile);
    assert.equal(receipt.external_mail_send_count, 0, profile);
    assert.equal(receipt.external_mail_receipt_count, 0, profile);
    assert.equal(receipt.duplicate_dispatch_count, 0, profile);
    assert.deepEqual(Object.keys(receipt.operations), Object.keys(NO_MAIL_OPERATION_MATRIX));

    for (const [name, operation] of Object.entries(receipt.operations)) {
      if (profile === 'both_client_routes_unreachable') {
        assert.equal(operation.outcome, 'NO_HEALTHY_ROUTE', `${profile}:${name}`);
        assert.equal(operation.selected_route, null, `${profile}:${name}`);
        assert.equal(operation.dispatch_count, 0, `${profile}:${name}`);
      } else {
        assert.equal(operation.outcome, 'PASS', `${profile}:${name}`);
        assert.equal(operation.dispatch_count, 1, `${profile}:${name}`);
        const expectedRoute = profile === 'client_supabase_direct_unreachable' ? 'relay' : 'direct';
        assert.equal(operation.selected_route, expectedRoute, `${profile}:${name}`);
      }
    }
  }
});

test('selected-once and idempotent policies stay explicit by product operation', () => {
  assert.equal(NO_MAIL_OPERATION_MATRIX.auth.policy, 'selected-once');
  assert.equal(NO_MAIL_OPERATION_MATRIX.search.policy, 'selected-once');
  assert.equal(NO_MAIL_OPERATION_MATRIX.personalization.policy, 'selected-once');
  assert.equal(NO_MAIL_OPERATION_MATRIX.focus.policy, 'idempotent-replay');
});
