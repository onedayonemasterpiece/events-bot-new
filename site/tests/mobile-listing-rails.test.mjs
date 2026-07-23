import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), 'utf8');

test('accepted v23 full-viewport 112px rail is tracked on every approved mobile listing surface', async () => {
  const [surface, row, menu, dates, weekend, popular] = await Promise.all([
    read('src/components/listings/MobileListingRailSurface.astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/Reference4MobileMenu.astro'),
    read('src/components/listings/DateListingSurface.astro'),
    read('src/components/listings/WeekendListingSurface.astro'),
    read('src/components/listings/PopularListingSurface.astro'),
  ]);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.event-row\{height:112px/u);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.rail-window\{[\s\S]*width:100vw;height:112px/u);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.track-start\{flex:0 0 5px/u);
  assert.match(surface, /\.ke-mobile-listing-rails--v23 \.event-summary\{[\s\S]*flex:0 0 296px;width:296px;height:112px/u);
  assert.match(surface, /@media \(max-width:720px\)/u);
  for (const donorClass of ['event-row', 'rail-window', 'event-summary', 'event-media', 'event-digest', 'event-medallion-slot', 'event-like-cta']) {
    assert.match(row, new RegExp(`['"]${donorClass}(?:['"]|--)`, 'u'), donorClass);
  }
  assert.match(row, /occurrenceMode === 'per-family' \? getOccurrencePresentation\(event\) : null/u);
  assert.match(row, /image\.mode === 'visual-crop' && image\.adaptiveCrop \? 'cover' : 'contain'/u);
  assert.doesNotMatch(menu, /\.reference4-menu__brand::before/u);
  assert.match(dates, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(weekend, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(popular, /collapseOccurrenceCards\(group\.events, 'per-family'\)/u);
  assert.match(popular, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-family"/u);
});

test('search, personal, exhibitions and event continuation do not inherit the listing rail', async () => {
  const paths = [
    'src/pages/poisk/index.astro',
    'src/pages/dlya-menya/index.astro',
    'src/pages/vystavki/index.astro',
    'src/pages/sobytiya/[slug].astro',
  ];
  for (const path of paths) {
    assert.doesNotMatch(await read(path), /MobileListingRailSurface/u, path);
  }
});
