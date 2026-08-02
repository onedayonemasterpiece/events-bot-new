import assert from 'node:assert/strict';
import test from 'node:test';

import { redactText, scanUnsafeText } from '../../e2e/focus-email/helpers/redaction.mjs';

test('redacts email, OTP secret, JWT, bearer and token values', () => {
  const otp = '123456';
  const jwt = 'eyJabcdefghijk.abcdefghijk.abcdefghijk';
  const source = `person@example.ru ${otp} Authorization: Bearer secret ${jwt} access_token=visible`;
  const result = redactText(source, [otp]);
  assert.doesNotMatch(result, /person@example|123456|secret|eyJabcdefgh|visible/u);
  assert.deepEqual(scanUnsafeText(result, [otp]), []);
});

test('unsafe scanner blocks raw secrets', () => {
  assert.ok(scanUnsafeText('focus-e2e@kenigevents.ru token_hash=abc', ['654321']).length >= 2);
});
