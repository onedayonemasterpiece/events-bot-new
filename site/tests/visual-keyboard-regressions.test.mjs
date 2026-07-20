import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
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
const cropCanaries = JSON.parse(readFileSync(new URL('./fixtures/related-card-crop-canaries.json', import.meta.url), 'utf8'));

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

test('row packing serializes the same unified treatment that EventCard resolves', () => {
  const items = [
    imageEvent(1, 1600, 1000, classifiedPhoto()),
    imageEvent(2, 800, 1000, classifiedPhoto({ geometry_pixel_sha256:'older-pixel' })),
    imageEvent(3, 800, 1000, { image_text_mode:'ocr_text' }),
  ];
  const packed = packRelatedCardRows(items, { rowSize:3, mediaTreatment:'hybrid' });

  assert.deepEqual(packed.map(({ layout }) => layout.mediaTreatment), [
    'visual-cover',
    'visual-cover',
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

test('visual-only photos retain the accepted compact cover contract when bbox metadata is stale', () => {
  const stale = classifiedPhoto({ geometry_pixel_sha256:'older-pixel' });
  const packed = packRelatedCardRows([
    imageEvent(1, 500, 1000, stale),
    imageEvent(2, 800, 1000, stale),
    imageEvent(3, 1600, 1000, stale),
  ], { rowSize:3, mediaTreatment:'hybrid' });

  assert.ok(packed.every(({ layout }) => layout.fit === 'cover'));
  assert.ok(packed.every(({ layout }) => layout.mediaTreatment === 'visual-cover'));
  assert.ok(packed.every(({ layout }) => layout.cropReason.startsWith('visual_only_focal_fallback:')));
  assert.ok(packed[0].layout.rowRatio >= 1 && packed[0].layout.rowRatio <= 4 / 3);
});

test('the Dog-page photo canaries cannot regress to contain bands or a tall singleton row', () => {
  const packed = packRelatedCardRows([
    imageEvent(5757, 1200, 800, { media_role:'unknown_document', media_semantic_status:'classified', safe_crop:false }),
    imageEvent(6586, 1000, 410, classifiedPhoto({
      valuable_region:{ x:.01, y:.1, w:.98, h:.8 },
    })),
    imageEvent(6318, 1200, 675, classifiedPhoto({ geometry_pixel_sha256:'older-pixel' })),
    imageEvent(6652, 1170, 1755, { media_role:'event_photo', media_semantic_status:'classified', safe_crop:true }),
  ], { rowSize:3, mediaTreatment:'hybrid' });

  assert.deepEqual(packed.map(({ layout }) => layout.mediaTreatment), [
    'visual-cover', 'visual-cover', 'visual-cover', 'visual-cover',
  ]);
  assert.ok(packed.every(({ layout }) => layout.fit === 'cover'));
  assert.equal(packed[3].layout.rowRatio, 1, 'a lone portrait photo keeps the accepted square preview row');
});

test('captured Dog-page production payloads reject the exact 22%/32% photo-band regression', () => {
  const packed = packRelatedCardRows(cropCanaries.visuals, { rowSize:3, mediaTreatment:'hybrid' });
  assert.deepEqual(packed.map(({ item }) => item.id), [5757, 6586, 6318, 5756]);
  assert.ok(packed.every(({ layout }) => layout.mediaKind === 'visual'));
  assert.ok(packed.every(({ layout }) => layout.mediaTreatment === 'visual-cover' && layout.fit === 'cover'));
  assert.equal(packed[0].layout.objectPosition, '50% 25%', 'asset focal metadata must survive the restored cover path');
  assert.equal(packed[3].layout.rowRatio, 1, 'the portrait singleton stays in the compact canonical row');

  const document = resolveRelatedCardMediaTreatment(cropCanaries.document, 1);
  assert.equal(document.mediaKind, 'document');
  assert.equal(document.mediaTreatment, 'document-contain');
  assert.equal(document.fit, 'contain');
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

test('visible footer owns P/S from body or an offscreen managed event owner', () => {
  const viewport = { viewportWidth:1200, viewportHeight:800 };
  const footerRect = { left:0, right:1200, top:610, bottom:850, width:1200, height:240 };
  const visibleCard = { left:20, right:360, top:100, bottom:500, width:340, height:400 };
  const offscreenCard = { left:20, right:360, top:-700, bottom:-300, width:340, height:400 };

  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'body', ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'managed-card', targetRect:offscreenCard, ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'event-surface', targetRect:offscreenCard, ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'event-surface', targetRect:visibleCard, ...viewport }), false);
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
  assert.match(router, /targetKind === 'event-surface' \? surface\.getBoundingClientRect\(\) : null/u);
  assert.match(router, /footerViewportShortcutOwnership/u);
  assert.match(router, /if \(!legacyBase\) return;[\s\S]*anchor\.href = normalize\(anchor\.href\)/u,
    'root continuation cards must retain the same relative canonical links as static cards');
});
