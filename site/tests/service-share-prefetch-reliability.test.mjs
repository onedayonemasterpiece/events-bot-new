import assert from 'node:assert/strict';
import test from 'node:test';

import { settleServiceSharePrefetch } from '../src/lib/service-share/controller.js';

test('service-share background asset prefetch consumes rejection instead of leaking pageerror', async () => {
  const value = await settleServiceSharePrefetch(Promise.reject(new Error('asset_timeout')));
  assert.equal(value, null);
});

test('service-share background asset prefetch preserves successful values', async () => {
  const marker = { ok: true };
  assert.equal(await settleServiceSharePrefetch(Promise.resolve(marker)), marker);
});
