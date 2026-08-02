import assert from 'node:assert/strict';
import test from 'node:test';
import { focusOtpFailureDomain } from '../../e2e/focus-email/helpers/failure-domain.mjs';

test('failure domains preserve Safari startup, simulator control, viewport and keyboard distinctions', () => {
  assert.equal(focusOtpFailureDomain(new Error('simulator_safari_navigation:target_origin_not_reached')), 'BLOCKED_INFRASTRUCTURE');
  assert.equal(focusOtpFailureDomain(new Error('safari_first_run_ui:stuck')), 'BLOCKED_SAFARI_FIRST_RUN_UI');
  assert.equal(focusOtpFailureDomain(new Error('ios_simulator_keyboard:numeric')), 'BLOCKED_IOS_SIMULATOR_KEYBOARD');
  assert.equal(focusOtpFailureDomain(new Error('fail_mobile_viewport:email')), 'FAIL_MOBILE_VIEWPORT');
  assert.equal(focusOtpFailureDomain(new Error('fail_mobile_keyboard:email')), 'FAIL_MOBILE_KEYBOARD');
});
