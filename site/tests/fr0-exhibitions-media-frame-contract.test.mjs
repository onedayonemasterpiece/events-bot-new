import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

const fr0ExhibitionsTokens = [
  '--ke-color-exhibitions-medallion-fallback-surface',
  '--ke-color-exhibitions-medallion-fallback-ring',
  '--ke-color-exhibitions-deck-frame-line',
  '--ke-color-exhibitions-deck-frame-surface',
  '--ke-elevation-exhibitions-deck-frame',
  '--ke-elevation-exhibitions-deck-overlay',
  '--ke-color-exhibitions-deck-stack-line',
  '--ke-elevation-exhibitions-deck-stack',
  '--ke-color-exhibitions-skeleton-start',
  '--ke-color-exhibitions-skeleton-mid',
  '--ke-color-exhibitions-depth-start',
  '--ke-color-exhibitions-depth-mid',
  '--ke-color-exhibitions-depth-end',
  '--ke-color-exhibitions-depth-line',
  '--ke-color-exhibitions-depth-surface',
  '--ke-elevation-exhibitions-depth-overlay',
  '--ke-elevation-exhibitions-medallion',
  '--ke-color-exhibitions-deck-frame-line-hover',
  '--ke-elevation-exhibitions-deck-frame-hover',
  '--ke-elevation-exhibitions-deck-stack-hover',
  '--ke-elevation-exhibitions-deck-filter-hover',
];

test('exhibitions deck, gallery and medallion use the canonical MediaFrame protocol', async () => {
  const [row, bridge, mediaFrame, surface] = await Promise.all([
    read('src/components/ExhibitionPrototypeRow.astro'),
    read('src/components/exhibitionsMediaFrameBridge.mjs'),
    read('src/components/media-frame.css'),
    read('src/components/ExhibitionsPersonalSurface.astro'),
  ]);

  assert.ok(row.includes("import './media-frame.css';"));
  assert.ok(row.includes("import { hydrateExhibitionsMediaFrames } from './exhibitionsMediaFrameBridge.mjs';"));
  assert.ok(row.includes('data-gallery-images={JSON.stringify(galleryImages)}'), 'legacy A0 gallery binding must remain intact');
  assert.ok(row.includes('data-gallery-manifest={JSON.stringify(galleryManifest)}'));
  for (const field of ['srcset: asset.thumbnailSrcset', 'width: asset.width', 'height: asset.height', 'sourceRatio: asset.sourceRatio', 'mediaRole: asset.mediaRole', 'textMode: asset.textMode']) {
    assert.ok(row.includes(field), `gallery manifest must retain ${field}`);
  }
  assert.ok(row.includes("treatment: 'document-natural'"));
  assert.ok(row.includes("cropReason: 'gallery_full_asset_contain'"));
  assert.ok(row.includes('safeCrop: false'));

  for (const invariant of [
    'data-media-frame-surface="exhibitions-deck"',
    'data-media-frame-fit="contain"',
    'data-media-frame-crop-permission="forbidden"',
    'data-media-frame-resource-state="idle"',
    'data-media-frame-interaction-owner="caller"',
    'data-deck-image\n            data-media-frame-image',
    'data-image-skeleton data-media-frame-placeholder',
    'ex-deck__depth-plane" data-media-frame-fallback',
    'data-media-frame-surface="exhibitions-medallion"',
    'data-media-frame-crop-reason="institutional_identity_square"',
    'data-media-frame-interaction-owner="none"',
  ]) assert.ok(row.includes(invariant), `missing SSR exhibitions MediaFrame invariant: ${invariant}`);

  assert.ok(bridge.includes("surface === 'exhibitions-deck'"));
  assert.ok(bridge.includes("asset?.treatment === 'photo-cover'"));
  assert.ok(bridge.includes("asset?.safeCrop === true"));
  assert.ok(bridge.includes("textMode === 'visual_only'"));
  assert.ok(bridge.includes("role === 'event_photo'"));
  assert.ok(bridge.includes("surface === 'exhibitions-gallery'\n        ? 'gallery_full_asset_contain'"));
  assert.ok(bridge.includes("fit: safeCover ? 'cover' : 'contain'"));
  assert.ok(bridge.includes("cropPermission: safeCover ? 'allowed' : 'forbidden'"));
  assert.ok(bridge.includes("state === 'error' ? 'broken'"));
  assert.ok(bridge.includes("state === 'depth' ? 'fallback'"));
  assert.ok(bridge.includes("frame.dataset.mediaFrameKind = 'fallback'"));
  assert.ok(bridge.includes("frame.dataset.mediaFrameCropReason = fallbackReason"));
  assert.ok(bridge.includes("frame.setAttribute('data-media-frame-fallback', '')"));
  assert.ok(bridge.includes("frame.removeAttribute('data-media-frame-fallback')"));

  for (const galleryInvariant of [
    "opener?.dataset?.galleryManifest",
    "image.setAttribute('data-media-frame-image', '')",
    "skeleton?.setAttribute('data-media-frame-placeholder', '')",
    "error?.setAttribute('data-media-frame-fallback', '')",
    "publishFrame(media, 'exhibitions-gallery'",
    "image.setAttribute('srcset', srcset)",
    "image.setAttribute('sizes', GALLERY_SIZES)",
    "attributeFilter: ['data-image-state']",
    "attributeFilter: ['src']",
  ]) assert.ok(bridge.includes(galleryInvariant), `missing gallery framing invariant: ${galleryInvariant}`);

  const containSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-fit="contain"] > [data-media-frame-image]';
  assert.ok(mediaFrame.includes(containSelector));
  for (const surfaceName of ['exhibitions-deck', 'exhibitions-gallery', 'exhibitions-medallion']) {
    assert.ok(mediaFrame.includes(`data-media-frame-surface="${surfaceName}"`), `canonical CSS must own ${surfaceName}`);
  }
  assert.match(mediaFrame, /data-media-frame-surface="exhibitions-gallery"\][^}]+display:\s*grid;[^}]+place-items:\s*center;/su);
  assert.match(mediaFrame, /data-media-frame-resource-state="pending"\][^}]+data-media-frame-placeholder/su);
  assert.match(mediaFrame, /data-media-frame-resource-state="broken"[^}]+data-media-frame-fallback/su);

  for (const token of fr0ExhibitionsTokens) {
    assert.ok(surface.includes(`var(${token}`), `materialized exhibitions consumer must retain FR0 token ${token}`);
  }

  // The compatibility bridge must not require rewriting the protected A0
  // interaction loop or the donor-parity CSS receipt in this FR0 batch.
  assert.ok(surface.includes("const openGallery = (opener,row) =>"));
  assert.ok(surface.includes("galleryMedia.dataset.imageState = 'loading'"));
  assert.ok(surface.includes('.ex-deck__frame img {'));
  assert.ok(surface.includes('object-fit:cover;'));
});
