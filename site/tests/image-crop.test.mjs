import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { resolveEventImageCrop, solveProtectedCrop } from '../src/lib/imageCrop.mjs';

const currentPhoto = (overrides = {}) => ({
  width:1600,
  height:1000,
  image_text_mode:'visual_only',
  media_semantic_status:'classified',
  media_role:'event_photo',
  safe_crop:true,
  current_pixel_sha256:'pixel-a',
  geometry_pixel_sha256:'pixel-a',
  geometry_status:'classified',
  geometry_coordinate_space:'normalized_0_1',
  face_boxes:[{ x:.44, y:.24, w:.12, h:.16 }],
  valuable_region:{ x:.32, y:.2, w:.36, h:.5, confidence:.91 },
  ...overrides,
});

test('protected crop returns a cover window containing face and value regions', () => {
  const result = resolveEventImageCrop(currentPhoto(), 4 / 5, { margin:.02 });

  assert.equal(result.fit, 'cover');
  assert.equal(result.reason, 'protected_regions_fit');
  assert.ok(result.cropWindow);
  assert.ok(result.cropWindow.x <= .32 - .02 + 1e-6);
  assert.ok(result.cropWindow.x + result.cropWindow.w >= .68 + .02 - 1e-6);
  assert.match(result.objectPosition, /^\d+% \d+%$/u);
});

test('protected crop fails closed when the protected union cannot fit', () => {
  const result = solveProtectedCrop({
    sourceWidth:1600,
    sourceHeight:900,
    targetAspect:4 / 5,
    boxes:[{ x:.02, y:.2, w:.96, h:.5 }],
    margin:.02,
  });

  assert.deepEqual(result, {
    fit:'contain',
    objectPosition:'50% 50%',
    cropWindow:null,
    reason:'protected_regions_do_not_fit',
  });
});

test('stale pixel provenance, OCR and missing geometry never unlock cover', () => {
  assert.equal(resolveEventImageCrop(currentPhoto({ geometry_pixel_sha256:'older-pixel' }), 16 / 9).reason, 'missing_or_stale_geometry');
  assert.equal(resolveEventImageCrop(currentPhoto({ image_text_mode:'ocr_text' }), 16 / 9).reason, 'document_or_unknown_media');
  assert.equal(resolveEventImageCrop(currentPhoto({ valuable_region:undefined }), 16 / 9).reason, 'missing_valuable_region');
  assert.equal(resolveEventImageCrop(currentPhoto({ safe_crop:false }), 16 / 9).reason, 'semantic_crop_gate_closed');
});

test('responsive surfaces fail closed and only an exact resolved card ratio invokes the solver', async () => {
  const read = (name) => readFile(new URL(`../src/components/${name}`, import.meta.url), 'utf8');
  const [card, hero, listing, desktop] = await Promise.all([
    read('EventCard.astro'),
    read('EventHero.astro'),
    read('EventListItem.astro'),
    read('DesktopEventPage.astro'),
  ]);

  assert.match(card, /desktopRelatedCrop\s*\? resolveEventImageCrop\(primaryImageAsset, cardTargetAspect\)/u);
  assert.match(card, /reason:'responsive_target_unknown'/u);
  assert.doesNotMatch(
    desktop,
    /\[data-lab-media-kind="visual"\] \.event-card__media\) \{[^}]*object-fit:cover !important;/u,
  );
  for (const source of [hero, listing, desktop]) {
    assert.match(source, /reason:'responsive_target_unknown'/u);
    assert.doesNotMatch(source, /resolveEventImageCrop\([^)]*,\s*16\s*\//u);
  }
});
