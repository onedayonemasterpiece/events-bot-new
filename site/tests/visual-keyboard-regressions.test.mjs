import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  packRelatedCardRows,
  resolveRelatedCardMediaTreatment,
} from '../src/lib/relatedCardLayout.mjs';
import {
  footerViewportShortcutOwnership,
  keyboardGalleryDestination,
} from '../src/lib/keyboardEventNavigation.mjs';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

const imageEvent = (id, width, height, asset = {}) => ({
  id,
  image_url:`/${id}.jpg`,
  image_text_mode:asset.image_text_mode || 'visual_only',
  image_assets:[{
    src:`/${id}.jpg`,
    width,
    height,
    image_text_mode:'visual_only',
    ...asset,
  }],
});

const classifiedPhoto = (overrides = {}) => ({
  media_role:'event_photo',
  media_semantic_status:'classified',
  safe_crop:true,
  current_pixel_sha256:'pixel-a',
  geometry_pixel_sha256:'pixel-a',
  geometry_status:'classified',
  geometry_coordinate_space:'normalized_0_1',
  face_boxes:[],
  valuable_region:{ x:.4, y:.4, w:.2, h:.2 },
  ...overrides,
});

test('row packing serializes the same final protected treatment that EventCard resolves', () => {
  const items = [
    imageEvent(1, 1600, 1000, classifiedPhoto()),
    imageEvent(2, 800, 1000, classifiedPhoto({ geometry_pixel_sha256:'older-pixel' })),
    imageEvent(3, 800, 1000, { image_text_mode:'ocr_text' }),
  ];
  const packed = packRelatedCardRows(items, { rowSize:3, mediaTreatment:'hybrid' });

  assert.deepEqual(packed.map(({ layout }) => layout.mediaTreatment), [
    'visual-cover',
    'visual-contain',
    'document-contain',
  ]);
  for (const { item, layout } of packed) {
    const cardDecision = resolveRelatedCardMediaTreatment(item, layout.rowRatio);
    assert.equal(layout.fit, cardDecision.fit);
    assert.equal(layout.mediaTreatment, cardDecision.mediaTreatment);
    assert.equal(layout.cropReason, cardDecision.cropReason);
    assert.equal(layout.objectPosition, cardDecision.objectPosition);
  }
});

test('fail-closed visuals participate in row geometry instead of being packed as cover', () => {
  const stale = classifiedPhoto({ geometry_pixel_sha256:'older-pixel' });
  const packed = packRelatedCardRows([
    imageEvent(1, 500, 1000, stale),
    imageEvent(2, 800, 1000, stale),
    imageEvent(3, 1600, 1000, stale),
  ], { rowSize:3, mediaTreatment:'hybrid' });

  assert.ok(packed.every(({ layout }) => layout.fit === 'contain'));
  assert.ok(packed.every(({ layout }) => layout.mediaTreatment === 'visual-contain'));
  assert.match(packed[0].layout.rowMode, /-authoritative$/u);
  assert.ok(packed[0].layout.rowRatio < 1, 'final contain geometry must replace the old squareish cover assumption');
  const oldSquareWorstBand = Math.max(...packed.map(({ layout }) => Math.max(0, 1 - Math.min(layout.mediaRatio, 1 / layout.mediaRatio))));
  const finalWorstBand = Math.max(...packed.map(({ layout }) => layout.potentialCoverCrop));
  assert.ok(finalWorstBand < oldSquareWorstBand);
});

test('related and personal continuation surfaces cannot override EventCard fit', async () => {
  const [card, desktop, personal, layout] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/DesktopEventPage.astro'),
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);

  assert.match(card, /resolveRelatedCardMediaTreatment\(event, cardTargetAspect\)/u);
  assert.match(card, /data-card-authoritative-fit=\{cardCrop\.fit\}/u);
  assert.doesNotMatch(desktop, /data-lab-media-treatment="[^"]+"[^}]*object-fit:/su);
  assert.doesNotMatch(personal, /data-lab-media-treatment="[^"]+"[^}]*object-fit:/su);
  assert.doesNotMatch(personal, /data-lab-media-kind="visual"[^}]*object-fit:\s*cover/su);
  assert.match(layout, /image\.style\.objectFit = authoritativeFit/u);
  assert.match(layout, /relatedLayout\?\.cropReason \|\| 'responsive_target_unknown'/u);
});

test('gallery handoff accepts only an exact same-origin event destination', () => {
  const current = 'https://example.test/_review/token/sobytiya/source-1/';
  assert.equal(
    keyboardGalleryDestination('../destination-2/?from=gallery', current),
    '/_review/token/sobytiya/destination-2/?from=gallery',
  );
  assert.equal(keyboardGalleryDestination('https://other.test/sobytiya/destination-2/', current), '');
  assert.equal(keyboardGalleryDestination('/afisha/', current), '');
});

test('visible footer owns P/S only from body or a stale offscreen managed card', () => {
  const viewport = { viewportWidth:1200, viewportHeight:800 };
  const footerRect = { left:0, right:1200, top:610, bottom:850, width:1200, height:240 };
  const visibleCard = { left:20, right:360, top:100, bottom:500, width:340, height:400 };
  const offscreenCard = { left:20, right:360, top:-700, bottom:-300, width:340, height:400 };

  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'body', ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'managed-card', targetRect:offscreenCard, ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'managed-card', targetRect:visibleCard, ...viewport }), false);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'other', ...viewport }), false);
  assert.equal(footerViewportShortcutOwnership({
    footerRect:{ ...footerRect, top:900, bottom:1140 },
    targetKind:'body',
    ...viewport,
  }), false);
});

test('router persists one bounded gallery handoff and consumes it for body arrows only', async () => {
  const router = await read('src/lib/keyboardEventNavigation.mjs');
  assert.match(router, /GALLERY_HANDOFF_TTL_MS = 30_000/u);
  assert.match(router, /win\.sessionStorage\.setItem\(GALLERY_HANDOFF_STORAGE_KEY/u);
  assert.match(router, /value\.destination === current/u);
  assert.match(router, /galleryDestinationHandoffExpiresAt >= Date\.now\(\)[\s\S]*event\.code === 'ArrowLeft'[\s\S]*target === doc\.body[\s\S]*selectHero/u);
  assert.match(router, /const targetKind = footerShare\.contains\(target\)[\s\S]*managed-card/u);
  assert.match(router, /footerViewportShortcutOwnership/u);
});
