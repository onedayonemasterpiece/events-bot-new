import assert from 'node:assert/strict';
import test from 'node:test';

import { requirePreviewAuthorizedSearch } from './preview-public-env.mjs';

test('required authorized candidate fails closed without the resilient relay', () => {
  assert.throws(
    () => requirePreviewAuthorizedSearch(
      { configured: true, resilientConfigured: false },
      { PREVIEW_REQUIRE_AUTHORIZED_SEARCH: '1' },
    ),
    /resilient relay URL is missing/u,
  );
});

test('required authorized candidate accepts complete resilient public config', () => {
  assert.doesNotThrow(() => requirePreviewAuthorizedSearch(
    { configured: true, resilientConfigured: true },
    { PREVIEW_REQUIRE_AUTHORIZED_SEARCH: '1' },
  ));
});
