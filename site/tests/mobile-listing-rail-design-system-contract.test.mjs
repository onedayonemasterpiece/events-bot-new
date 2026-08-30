import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

test('production surface inventory includes the actual Mobile Rail owners', async () => {
  const contract = JSON.parse(await read('../src/data/design-system-production-surface-contract.v1.json'));
  const family = contract.component_families.find(({ id }) => id === 'event.cards-and-rails');
  assert.ok(family);
  assert.ok(family.source_files.includes('site/src/components/listings/MobileListingRailSurface.astro'));
  assert.ok(family.source_files.includes('site/src/components/listings/MobileListingRailRow.astro'));
  assert.ok(family.source_files.includes('site/src/lib/mobileListingRailMedia.mjs'));
});

test('design-system catalog links a real-data fixture using the production surface', async () => {
  const [catalog, fixture] = await Promise.all([
    read('../src/pages/lab/design-system/index.astro'),
    read('../src/pages/lab/design-system/mobile-listing-rail-media/index.astro'),
  ]);
  assert.match(catalog, /data-ds-component="MobileListingRailSurface"/u);
  assert.match(catalog, /\/lab\/design-system\/mobile-listing-rail-media\//u);
  assert.match(fixture, /<MobileListingRailSurface/u);
  assert.match(fixture, /const fixtureIds = \[5374, 6936, 6652\]/u);
  assert.doesNotMatch(fixture, /EventMediaFrame/u);
});
