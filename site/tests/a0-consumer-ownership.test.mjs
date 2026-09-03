import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('Popular owns one mobile rail and no hidden density-era representations', async () => {
  const source = await read('src/components/listings/PopularListingSurface.astro');

  assert.match(source, /import MobileListingRailSurface from/u);
  assert.equal((source.match(/<MobileListingRailSurface\b/gu) || []).length, 1);
  assert.match(source, /<PopularBehaviorRows[\s\S]*<PopularPersonalizedRow/u);
  for (const obsolete of [
    'PopularMobileBehaviorRows',
    'PopularMobileAdaptiveRows',
    'PopularMobileGroupContext',
    'ListingMobileDensitySwitch',
    'ke-popular-mobile-existing',
    'data-mobile-card-density',
    'ke_popular_mobile_density_v1',
  ]) assert.ok(!source.includes(obsolete), `Popular retains obsolete owner ${obsolete}`);
});

test('collection route delegates item markup and styles to one canonical catalog root', async () => {
  const [route, catalog] = await Promise.all([
    read('src/pages/podborki/index.astro'),
    read('src/components/CollectionCatalog.astro'),
  ]);

  assert.match(route, /import CollectionCatalog from/u);
  assert.match(route, /<CollectionCatalog entries=\{collectionCatalogEntries\} \/>/u);
  assert.match(route, /data-ds-family="CollectionCatalogRouteComposition"/u);
  assert.doesNotMatch(route, /collectionCatalogEntries\.map|<nav\b|collection-card|collection-grid/u);

  assert.match(catalog, /data-ds-family="CollectionCatalog"/u);
  assert.match(catalog, /data-ds-family="CollectionCatalogItem"/u);
  assert.match(catalog, /data-ke-foundation-consumer="collection-catalog"/u);
  assert.match(catalog, /--ke-color-collection-link-border/u);
  assert.match(catalog, /--ke-color-collection-link-surface/u);
  assert.match(catalog, /--ke-color-collection-link-hover-surface/u);
  assert.match(catalog, /--ke-color-collection-link-text/u);
  assert.doesNotMatch(catalog, /var\(--color-border,\s*#ddd\)|var\(--color-surface,\s*#fff\)/u);
});
