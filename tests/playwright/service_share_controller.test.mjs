import assert from 'node:assert/strict';
import test from 'node:test';

import {
  SERVICE_SHARE_CANONICAL_URL,
  coarseServiceSharePlatform,
  createImageClipboardItem,
  createRichClipboardItem,
  escapeServiceShareHtml,
  latencyBand,
  serviceShareHtml,
  serviceSharePlainText,
  validateServiceShareManifest,
} from '../../site/src/lib/service-share/controller.js';

const manifestPayload = {
  schema_version: 'service-share-card-manifest-v1',
  asset_version: '2026-07-15-v1',
  visual_payload_hash: 'c'.repeat(64),
  canonical_url: SERVICE_SHARE_CANONICAL_URL,
  share_text: 'KenigEvents — события Калининграда и области\nНайдите своё событие быстрее',
  assets: {
    webp: { url: '/service-share/assets/card-123.webp', mime_type: 'image/webp', byte_size: 1234, sha256: 'a'.repeat(64) },
    png: { url: '/service-share/assets/card-123.png', mime_type: 'image/png', byte_size: 5678, sha256: 'b'.repeat(64) },
  },
};

test('normalizes one versioned WebP/PNG manifest around the canonical service URL', () => {
  const manifest = validateServiceShareManifest(manifestPayload, 'https://kenigevents.test/preview/service-share/current/manifest.json');
  assert.equal(manifest.canonical_url, SERVICE_SHARE_CANONICAL_URL);
  assert.equal(manifest.assets.webp.url, 'https://kenigevents.test/service-share/assets/card-123.webp');
  assert.equal(manifest.assets.png.mime_type, 'image/png');
  assert.equal(manifest.assets.png.byte_size, 5678);
  assert.equal(serviceSharePlainText(manifest).endsWith(`\n${SERVICE_SHARE_CANONICAL_URL}`), true);
});

test('rejects event/personal URLs and non-WebP/PNG representations', () => {
  assert.throws(
    () => validateServiceShareManifest({ ...manifestPayload, canonical_url: 'https://kenigevents.ru/sobytiya/secret/' }, 'https://kenigevents.ru/manifest.json'),
    /canonical_mismatch/u,
  );
  assert.throws(
    () => validateServiceShareManifest({ ...manifestPayload, share_text: 'Событие https://kenigevents.ru/sobytiya/1/' }, 'https://kenigevents.ru/manifest.json'),
    /copy_invalid/u,
  );
  assert.throws(
    () => validateServiceShareManifest({ ...manifestPayload, assets: { ...manifestPayload.assets, png: { ...manifestPayload.assets.png, mime_type: 'image/webp' } } }, 'https://kenigevents.ru/manifest.json'),
    /asset_mime_invalid/u,
  );
  assert.throws(
    () => validateServiceShareManifest({ ...manifestPayload, assets: { ...manifestPayload.assets, png: { ...manifestPayload.assets.png, url: 'https://evil.example/card.png' } } }, 'https://kenigevents.ru/manifest.json'),
    /asset_origin_unapproved/u,
  );
  assert.throws(
    () => validateServiceShareManifest({ ...manifestPayload, visual_payload_hash: 'not-a-sha' }, 'https://kenigevents.ru/manifest.json'),
    /visual_payload_hash_invalid/u,
  );
  assert.throws(
    () => validateServiceShareManifest({ ...manifestPayload, assets: { ...manifestPayload.assets, webp: { ...manifestPayload.assets.webp, sha256: 'bad' } } }, 'https://kenigevents.ru/manifest.json'),
    /asset_sha_invalid/u,
  );
});

test('builds escaped minimal HTML with no executable/user-controlled markup', () => {
  const unsafe = validateServiceShareManifest({
    ...manifestPayload,
    share_text: '<img src=x onerror=alert(1)> & «Афиша»',
  }, 'https://kenigevents.ru/manifest.json');
  const html = serviceShareHtml(unsafe);
  assert.match(html, /^<article>/u);
  assert.ok(html.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.ok(html.includes(`<a href="${SERVICE_SHARE_CANONICAL_URL}">`));
  assert.equal(/<script|<img src=x|javascript:/iu.test(html), false);
  assert.equal(escapeServiceShareHtml('"<&\''), '&quot;&lt;&amp;&#39;');
});

test('creates exactly one image-only ClipboardItem for the desktop card intent', async () => {
  class FakeClipboardItem {
    constructor(representations) {
      this.representations = representations;
      this.types = Object.keys(representations);
    }
  }
  const png = Promise.resolve(new Blob([new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10])], { type: 'image/png' }));
  const item = createImageClipboardItem(png, FakeClipboardItem);
  assert.deepEqual(item.types, ['image/png']);
  assert.equal((await item.representations['image/png']).type, 'image/png');
  assert.equal('text/plain' in item.representations, false);
  assert.equal('text/html' in item.representations, false);

  const manifest = validateServiceShareManifest(manifestPayload, 'https://kenigevents.ru/manifest.json');
  const compatibilityItem = createRichClipboardItem(manifest, 'd1', png, FakeClipboardItem);
  assert.deepEqual(compatibilityItem.types, ['image/png']);
  assert.throws(() => createImageClipboardItem(png, null), /clipboard_item_unavailable/u);
});

test('bounds platform and latency telemetry values', () => {
  assert.equal(coarseServiceSharePlatform({ platform: 'Win32' }), 'windows');
  assert.equal(coarseServiceSharePlatform({ userAgentData: { platform: 'Android' } }), 'android');
  assert.equal(coarseServiceSharePlatform({ platform: 'mystery' }), 'other');
  assert.equal(latencyBand(10), 'lt_250ms');
  assert.equal(latencyBand(500), '250_999ms');
  assert.equal(latencyBand(1800), '1_3s');
  assert.equal(latencyBand(5000), 'gte_3s');
});
