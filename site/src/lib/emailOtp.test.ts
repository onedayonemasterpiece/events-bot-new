import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  EMAIL_OTP_LENGTH,
  canSubmitEmailOtp,
  isCompleteEmailOtp,
  normalizeEmailOtp,
} from './emailOtp.ts';

test('email OTP keeps six ASCII digits including a leading zero', () => {
  assert.equal(EMAIL_OTP_LENGTH, 6);
  assert.equal(normalizeEmailOtp(' 01 23-45 '), '012345');
  assert.equal(normalizeEmailOtp('12a34b56789'), '123456');
});

test('email OTP is incomplete before the sixth digit', () => {
  assert.equal(isCompleteEmailOtp('12345'), false);
  assert.equal(isCompleteEmailOtp('123456'), true);
});

test('email OTP auto-submit is single-flight and does not repeat the same code', () => {
  assert.equal(canSubmitEmailOtp('123456'), true);
  assert.equal(canSubmitEmailOtp('123456', { inFlight: true }), false);
  assert.equal(canSubmitEmailOtp('123456', { lastSubmitted: '123456' }), false);
  assert.equal(canSubmitEmailOtp('654321', { lastSubmitted: '123456' }), true);
});
