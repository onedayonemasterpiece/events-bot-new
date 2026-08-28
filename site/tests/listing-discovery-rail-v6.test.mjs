import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('ListingDiscoveryRail v6 owns the plane and Floating Island surfaces', async () => {
  const [component, styles, catalog] = await Promise.all([
    read('src/components/listings/ListingDiscoveryRail.astro'),
    read('src/styles/design-system.css'),
    read('src/pages/lab/design-system/index.astro'),
  ]);
  assert.match(component, /version\?: 5 \| 6/u);
  assert.match(component, /surface\?: 'plane' \| 'floating-island'/u);
  assert.match(component, /version = 6, surface = 'plane'/u);
  assert.match(component, /data-ds-version=\{version\}/u);
  assert.match(component, /data-listing-discovery-surface=\{resolvedSurface\}/u);
  assert.match(styles, /\.ke-listing-discovery-rail--floating-island \{[\s\S]*background: transparent;[\s\S]*pointer-events: none;/u);
  assert.match(styles, /\.ke-listing-discovery-rail--floating-island \.ke-listing-discovery-rail__inner \{[\s\S]*width: fit-content;[\s\S]*border-radius: var\(--ke-radius-pill\);[\s\S]*backdrop-filter: blur\(14px\);/u);
  assert.match(catalog, /data-ds-component="ListingDiscoveryRail" data-ds-version="6"/u);
  assert.match(catalog, /data-ds-version="5" data-ds-replaced-by="ListingDiscoveryRail@6"/u);
});

test('all production consumers use v6 and Weekend selects Floating Island', async () => {
  const [date, popular, weekend] = await Promise.all([
    read('src/components/listings/DateListingSurface.astro'),
    read('src/components/listings/PopularListingSurface.astro'),
    read('src/components/listings/WeekendListingSurface.astro'),
  ]);
  for (const source of [date, popular, weekend]) {
    assert.match(source, /<ListingDiscoveryRail[\s\S]*?version=\{6\}/u);
  }
  assert.match(weekend, /version=\{6\} surface="floating-island"/u);
  assert.doesNotMatch(date, /surface="floating-island"/u);
  assert.doesNotMatch(popular, /surface="floating-island"/u);
});
