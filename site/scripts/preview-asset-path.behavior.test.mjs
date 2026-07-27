import assert from 'node:assert/strict';
import test from 'node:test';

import { localPreviewRuntimePath } from './preview-asset-path.mjs';

test('preview checks map local, prefixed and immutable-CDN Astro runtime URLs to checked bytes', () => {
  assert.equal(localPreviewRuntimePath('_astro/page.ABC123.css'), '_astro/page.ABC123.css');
  assert.equal(
    localPreviewRuntimePath('/preview-gate-build/_astro/page.ABC123.css'),
    '_astro/page.ABC123.css',
  );
  assert.equal(
    localPreviewRuntimePath('https://static.kenigevents.ru/production-build/_astro/page.ABC123.css'),
    '_astro/page.ABC123.css',
  );
});

test('preview runtime mapping rejects non-runtime and traversal-shaped URLs', () => {
  assert.equal(localPreviewRuntimePath('https://static.kenigevents.ru/production-build/p/event.webp'), null);
  assert.equal(localPreviewRuntimePath('/_astro/'), null);
  assert.equal(localPreviewRuntimePath('/_astro/%2e%2e/secret.css'), null);
  assert.equal(localPreviewRuntimePath('not a url with spaces'), null);
});
