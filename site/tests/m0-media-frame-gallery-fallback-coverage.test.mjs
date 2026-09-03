import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const occurrences = (source, token) => source.split(token).length - 1;

const COMMON_MEDIA_FIELDS = [
  'data-media-frame-role',
  'data-media-frame-kind',
  'data-media-frame-ratio',
  'data-media-frame-fit',
  'data-media-frame-crop-permission',
  'data-media-frame-focal-position',
  'data-media-frame-clip',
  'data-media-frame-radius',
  'data-media-frame-loading',
  'data-media-frame-source-width',
  'data-media-frame-source-height',
];

const ROOTS = [
  ['src/components/EventCard.astro', 2],
  ['src/components/listings/ListingEventCard.astro', 1],
  ['src/components/EventMediaRail.astro', 2],
  ['src/components/listings/MobileListingRailRow.astro', 1],
];

test('every owned card/media root exposes the common MediaFrame geometry protocol', async () => {
  for (const [file, minimumRoots] of ROOTS) {
    const source = await read(file);
    assert.ok(
      occurrences(source, 'data-media-frame-contract="v1"') >= minimumRoots,
      `${file} must expose its canonical MediaFrame roots`,
    );
    for (const field of COMMON_MEDIA_FIELDS) {
      assert.ok(source.includes(field), `${file} misses ${field}`);
    }
    assert.ok(source.includes('data-media-frame-image'), `${file} misses image anatomy`);
  }
});

test('priority and fallback are explicit named variants rather than fake universal fields', async () => {
  const eventCard = await read('src/components/EventCard.astro');
  assert.match(eventCard, /data-media-frame-loading="lazy"/u,
    'EventCard has a fixed lazy policy and does not need a synthetic priority field');

  for (const file of [
    'src/components/listings/ListingEventCard.astro',
    'src/components/EventMediaRail.astro',
    'src/components/listings/MobileListingRailRow.astro',
  ]) {
    const source = await read(file);
    assert.ok(source.includes('data-media-frame-priority'), `${file} misses variable priority diagnostics`);
  }

  for (const file of [
    'src/components/EventCard.astro',
    'src/components/listings/ListingEventCard.astro',
    'src/components/EventMediaRail.astro',
  ]) {
    const source = await read(file);
    assert.ok(source.includes('data-media-frame-fallback'), `${file} misses fallback anatomy`);
  }

  const mobileRow = await read('src/components/listings/MobileListingRailRow.astro');
  assert.match(mobileRow, /return imageUrl \? \([\s\S]*?\) : null;/u,
    'mobile rail intentionally omits an unavailable media segment instead of painting a second fallback card');
  assert.doesNotMatch(mobileRow, /data-media-frame-fallback/u);
});

test('EventMediaRail exposes all three variants and both input contracts', async () => {
  const source = await read('src/components/EventMediaRail.astro');
  for (const variant of ['gallery-thumbnails', 'hero-selector', 'poster-strip']) {
    assert.ok(source.includes(variant), `EventMediaRail misses ${variant}`);
  }
  assert.match(source, /data-event-media-rail-contract=\{usesResolvedItems \? 'resolved-items-v1' : 'asset-input-v1'\}/u);
  assert.match(source, /data-event-media-rail-source-owner=\{usesResolvedItems \? 'caller' : 'EventMediaRail'\}/u);
  assert.match(source, /data-event-media-rail-rejected=\{rejectedCount \|\| undefined\}/u);
  assert.ok(occurrences(source, 'data-media-frame-contract="v1"') >= 2);
  assert.ok(occurrences(source, 'data-media-frame-style-owner="media-frame.css"') >= 2);
  assert.match(source, /data-media-frame-source-ratio=\{sourceRatio\?\.toFixed\(5\)\}/u);
});

test('EventCard and ListingEventCard keep caller interaction outside MediaFrame', async () => {
  for (const file of ['src/components/EventCard.astro', 'src/components/listings/ListingEventCard.astro']) {
    const source = await read(file);
    assert.match(source, /data-media-frame-interaction-owner="caller"/u);
    assert.doesNotMatch(source, /<(?:a|button)\b[^>]*data-media-frame/u,
      `${file} must not merge navigation/action ownership into MediaFrame`);
  }
});
