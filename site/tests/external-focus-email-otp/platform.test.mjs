import assert from 'node:assert/strict';
import test from 'node:test';

import { assertSequentialMailboxPolicy, classifyKeyboardAcceptance, selectedPlatforms, validateMobileConfig, validatePlatform } from '../../e2e/focus-email/helpers/platform.mjs';

test('platform selector is closed and all stays browser then Android then iOS', () => {
  assert.deepEqual(selectedPlatforms('all'), ['browser', 'android', 'ios']);
  assert.equal(validatePlatform('android'), 'android');
  assert.throws(() => validatePlatform('desktop'), /platform_invalid/u);
  assert.equal(assertSequentialMailboxPolicy({ maxParallel: 1 }), true);
  assert.throws(() => assertSequentialMailboxPolicy({ maxParallel: 2 }), /concurrency/u);
});

test('mobile adapter config blocks missing pinned simulator and accepts exact fixtures', () => {
  assert.throws(() => validateMobileConfig('android', {}), /simulator_configuration_missing/u);
  assert.throws(() => validateMobileConfig('ios', { E2E_DEVICE_NAME: 'iPhone 16', E2E_PLATFORM_VERSION: '18.5' }), /runtime_missing/u);
  assert.equal(validateMobileConfig('android', { E2E_DEVICE_NAME: 'Pixel 7', E2E_PLATFORM_VERSION: '35' }).deviceName, 'Pixel 7');
  assert.equal(validateMobileConfig('ios', { E2E_DEVICE_NAME: 'iPhone 16', E2E_PLATFORM_VERSION: '18.5', E2E_DEVICE_UDID: 'safe-udid' }).udid, 'safe-udid');
});

test('keyboard acceptance classifies native presence, focus, input path and viewport', () => {
  const accepted = classifyKeyboardAcceptance({ shown: true, active: true, visible: true, inputMode: 'numeric', viewport: { innerHeight: 400, elementBottom: 350 } });
  assert.equal(accepted.passed, true);
  assert.equal(classifyKeyboardAcceptance({ shown: false, active: true, visible: true, inputMode: 'email', viewport: { innerHeight: 400, elementBottom: 350 } }).passed, false);
  assert.equal(classifyKeyboardAcceptance({ shown: true, active: true, visible: true, inputMode: 'text', viewport: { innerHeight: 400, elementBottom: 350 } }).passed, false);
});
