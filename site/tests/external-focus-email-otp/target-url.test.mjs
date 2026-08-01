import assert from 'node:assert/strict';
import test from 'node:test';

import { recipientForRun, validateFocusE2eTarget } from '../../e2e/focus-email/helpers/target-url.mjs';

test('accepts exact KenigEvents preview and production onboarding targets', () => {
  assert.equal(validateFocusE2eTarget('https://kenigevents.ru/preview-20260801-focus-onboarding-otp-r5/fokus-gruppa/priglashenie/?install=presentation&focus_test_reset=1#invite=focus-group-2026-announcements').hostname, 'kenigevents.ru');
  assert.equal(validateFocusE2eTarget('https://kenigevents.ru/fokus-gruppa/priglashenie/').pathname, '/fokus-gruppa/priglashenie/');
});

test('rejects unsafe origin, credentials, ports, paths and encoded controls', () => {
  for (const value of [
    'http://kenigevents.ru/fokus-gruppa/priglashenie/',
    'https://evil.example/fokus-gruppa/priglashenie/',
    'https://kenigevents.ru.evil.example/fokus-gruppa/priglashenie/',
    'https://user:pass@kenigevents.ru/fokus-gruppa/priglashenie/',
    'https://kenigevents.ru:8443/fokus-gruppa/priglashenie/',
    'https://127.0.0.1/fokus-gruppa/priglashenie/',
    'https://kenigevents.ru/poisk/',
    'https://kenigevents.ru/fokus-gruppa/priglashenie/%0a',
    'https://kenigevents.ru/fokus-gruppa/priglashenie/?token_hash=secret',
  ]) assert.throws(() => validateFocusE2eTarget(value));
});

test('fixed recipient remains one returning identity and unique mode is explicit', () => {
  assert.equal(recipientForRun('focus-e2e@kenigevents.ru', 'run-123').coverage, 'returning_test_identity');
  assert.deepEqual(recipientForRun('focus-e2e+{run_id}@kenigevents.ru', 'Run 123'), {
    recipient: 'focus-e2e+run-123@kenigevents.ru',
    coverage: 'fresh_unique_identity',
  });
});
