import assert from 'node:assert/strict';
import test from 'node:test';

import {
  resolveMobileListingRailMedia,
  resolveMobileListingRailMediaItems,
} from '../src/lib/mobileListingRailMedia.mjs';

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

test('a contradictory event-level OCR or unknown marker blocks the lone 5:4 crop', () => {
  for (const eventMode of ['ocr_text', 'unknown']) {
    const image = asset();
    const result = resolveMobileListingRailMedia(
      { image_text_mode:eventMode, image_assets:[image] },
      selected(image),
    );
    assert.equal(result.fit, 'contain');
    assert.equal(result.reason, 'safe_visual_authored_geometry');
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

test('every safe visual-only asset in a multi-image gallery uses 5:4 cover without fields', () => {
  const first = asset({ width:3072, height:1307 });
  const second = asset({ src: 'https://static.kenigevents.ru/second.webp', width:3072, height:1307 });
  const result = resolveMobileListingRailMedia(
    { image_text_mode: 'visual_only', image_assets: [first, second] },
    selected(first),
  );
  assert.deepEqual(result, {
    fit: 'cover',
    ratio: 5 / 4,
    width: 140,
    reason: 'safe_visual_landscape_5x4',
  });
});

test('mobile rail returns a bounded real gallery in source order and protects every asset independently', () => {
  const first = asset({ src:'https://static.kenigevents.ru/first.webp', asset_key:'first' });
  const poster = asset({
    src:'https://static.kenigevents.ru/poster.webp',
    asset_key:'poster',
    width:1080,
    height:1350,
    image_text_mode:'ocr_text',
    media_role:'event_identity_poster',
    safe_crop:false,
  });
  const reviewedPhoto = asset({
    src:'https://static.kenigevents.ru/reviewed.webp',
    asset_key:'reviewed',
    width:828,
    height:1227,
    listing_crop_evidence:'source-reviewed',
    listing_no_ocr_review:true,
  });
  const overflow = [
    asset({ src:'https://static.kenigevents.ru/four.webp', asset_key:'four' }),
    asset({ src:'https://static.kenigevents.ru/five.webp', asset_key:'five' }),
  ];
  const event = { image_text_mode:'visual_only', image_assets:[first, poster, reviewedPhoto, ...overflow] };
  const items = resolveMobileListingRailMediaItems(event, selected(first), 4);

  assert.equal(items.length, 4);
  assert.deepEqual(items.map((item) => item.src), [
    first.src,
    poster.src,
    reviewedPhoto.src,
    overflow[0].src,
  ]);
  assert.equal(items[0].fit, 'cover');
  assert.equal(items[0].ratio, 4 / 5);
  assert.equal(items[0].reason, 'safe_visual_portrait_4x5');
  assert.equal(items[1].imageTextMode, 'ocr_text');
  assert.equal(items[1].fit, 'contain');
  assert.equal(items[2].fit, 'cover');
  assert.equal(items[2].reason, 'safe_visual_portrait_4x5');
});

test('mobile gallery deduplicates the selected asset and clamps an excessive requested limit', () => {
  const images = Array.from({ length:8 }, (_unused, index) => asset({
    src:`https://static.kenigevents.ru/${index}.webp`,
    asset_key:`asset-${index}`,
  }));
  const event = { image_text_mode:'visual_only', image_assets:images };
  const items = resolveMobileListingRailMediaItems(event, selected(images[0]), 99);
  assert.equal(items.length, 6);
  assert.equal(new Set(items.map((item) => item.src)).size, items.length);
});
