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
    media_role_confidence: .95,
    safe_crop: true,
    focal_point: { x: .5, y: .5 },
    ...overrides,
  };
}

test('every single classified crop-safe visual gets donor 140x112 cover, including wide Pianissimo media', () => {
  for (const image of [
    asset(),
    asset({ width: 3072, height: 1307, recommended_object_position: '65% 35%' }),
  ]) {
    const result = resolveMobileListingRailMedia(
      { image_text_mode: 'visual_only', image_assets: [image] },
      selected(image),
    );
    assert.deepEqual(result, {
      fit: 'cover',
      ratio: 5 / 4,
      width: 140,
      reason: 'single_safe_visual_landscape_5x4',
    });
  }
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

test('source-reviewed portrait selected from mixed inventory gets vertical 4:5 cover within 20% crop', () => {
  const poster = asset({
    src: 'https://static.kenigevents.ru/poster.webp',
    width: 1080,
    height: 1080,
    image_text_mode: 'ocr_text',
    media_role: 'event_identity_poster',
    safe_crop: false,
    focal_point: null,
  });
  const reviewedPhoto = asset({
    src: 'https://static.kenigevents.ru/photo.webp',
    width: 828,
    height: 1227,
    listing_crop_evidence: 'source-reviewed',
    listing_no_ocr_review: true,
  });
  const result = resolveMobileListingRailMedia(
    { image_text_mode: 'ocr_text', image_assets: [poster, reviewedPhoto] },
    selected(reviewedPhoto),
  );
  assert.deepEqual(result, {
    fit: 'cover',
    ratio: 4 / 5,
    width: 90,
    reason: 'reviewed_multi_visual_portrait_4x5',
  });
  const retainedArea = Math.min((828 / 1227) / (4 / 5), (4 / 5) / (828 / 1227));
  assert.ok(retainedArea >= 0.8);
});

test('generic crop review cannot override an event-level OCR protection marker', () => {
  const reviewedPhoto = asset({
    width: 828,
    height: 1227,
    listing_crop_evidence: 'source-reviewed',
    listing_no_ocr_review: false,
  });
  const result = resolveMobileListingRailMedia(
    { image_text_mode: 'ocr_text', image_assets: [reviewedPhoto, asset({ src: 'second.webp' })] },
    selected(reviewedPhoto),
  );
  assert.equal(result.fit, 'contain');
  assert.equal(result.reason, 'safe_visual_authored_geometry');
});

test('unreviewed multi-image visual remains fail-closed at authored geometry', () => {
  const first = asset();
  const second = asset({ src: 'https://static.kenigevents.ru/second.webp' });
  const result = resolveMobileListingRailMedia(
    { image_text_mode: 'visual_only', image_assets: [first, second] },
    selected(first),
  );
  assert.equal(result.fit, 'contain');
  assert.equal(result.width, 84);
  assert.equal(result.reason, 'safe_visual_authored_geometry');
});
