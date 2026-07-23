import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), 'utf8');

test('accepted 112px rail is tracked on every approved mobile listing surface', async () => {
  const [surface, row, dates, weekend, popular] = await Promise.all([
    read('src/components/listings/MobileListingRailSurface.astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/listings/DateListingSurface.astro'),
    read('src/components/listings/WeekendListingSurface.astro'),
    read('src/components/listings/PopularListingSurface.astro'),
  ]);
  assert.match(surface, /\.ke-mobile-rail-row \{[\s\S]*height:112px/u);
  assert.match(surface, /@media \(max-width:720px\)/u);
  assert.match(surface, /\.ke-listing-desktop-body,\.ke-popular-mobile-existing \{ display:none !important; \}/u);
  assert.match(row, /data-mobile-listing-row/u);
  assert.match(row, /occurrenceMode === 'per-family' \? getOccurrencePresentation\(event\) : null/u);
  assert.match(row, /image\.mode === 'visual-crop' && image\.adaptiveCrop \? 'cover' : 'contain'/u);
  assert.match(dates, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(weekend, /<MobileListingRailSurface[\s\S]*occurrenceMode="per-date"/u);
  assert.match(popular, /collapseOccurrenceCards\(visibleEvents, 'per-family'\)/u);
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
