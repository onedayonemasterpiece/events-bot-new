import assert from 'node:assert/strict';
import test from 'node:test';

import { resolveMobileListingRailMedia } from '../src/lib/mobileListingRailMedia.mjs';

function selected(asset, overrides = {}) {
  return {
    asset,
    ratio: asset.width / asset.height,
    mode: 'natural',
    adaptiveCrop: false,
    ...overrides,
  };
}

function asset(overrides = {}) {
  return {
    src: 'https://static.kenigevents.ru/example.webp',
    width: 900,
    height: 1200,
    image_text_mode: 'visual_only',
    media_semantic_status: 'classified',
    media_role: 'event_photo',
    ...overrides,
  };
}

test('single classified visual-only portrait gets the narrow 140x112 landscape treatment', () => {
  const image = asset();
  const result = resolveMobileListingRailMedia(
    { image_text_mode: 'visual_only', image_assets: [image] },
    selected(image),
  );
  assert.deepEqual(result, {
    fit: 'cover',
    ratio: 5 / 4,
    width: 140,
    reason: 'single_tall_visual_landscape',
  });
});

test('OCR, unknown semantics and document roles fail closed to authored contain geometry', () => {
  for (const image of [
    asset({ image_text_mode: 'ocr_text' }),
    asset({ media_semantic_status: 'unknown' }),
    asset({ media_role: 'program_or_schedule' }),
  ]) {
    const event = { image_text_mode: image.image_text_mode, image_assets: [image] };
    const result = resolveMobileListingRailMedia(event, selected(image));
    assert.equal(result.fit, 'contain');
    assert.equal(result.width, 84);
    assert.equal(result.reason, 'protected_natural_geometry');
  }
});

test('multiple portraits keep the selected authored geometry', () => {
  const first = asset();
  const second = asset({ src: 'https://static.kenigevents.ru/second.webp' });
  const result = resolveMobileListingRailMedia(
    { image_text_mode: 'visual_only', image_assets: [first, second] },
    selected(first),
  );
  assert.equal(result.fit, 'contain');
  assert.equal(result.width, 84);
  assert.equal(result.reason, 'protected_natural_geometry');
});
