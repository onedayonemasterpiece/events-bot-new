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

test('partners route consumes the complete partnership registry instead of owning responsive literals', async () => {
  const source = await read('src/pages/partners/index.astro');

  assert.match(source, /data-ds-family="PartnersRouteComposition"/u);
  assert.match(source, /data-ke-foundation-consumer="partners-route"/u);
  for (const token of [
    '--ke-partners-page-padding-bottom',
    '--ke-partners-container',
    '--ke-partners-heading-size',
    '--ke-partners-grid-columns-mobile',
    '--ke-partners-grid-row-height-wide',
    '--ke-partner-focus-alpha',
    '--ke-partner-motion',
    '--ke-partner-logo-height-tall-desktop',
    '--ke-partner-meta-size',
    '--ke-partners-mobile-column-gap',
    '--ke-partner-mobile-logo-height-tall',
    '--ke-partners-narrow-row-height',
  ]) assert.match(source, new RegExp(token, 'u'));

  assert.doesNotMatch(
    source,
    /padding-bottom:\s*clamp\(2\.25rem|max-width:\s*1100px|--cell-h:\s*76px|column-gap:\s*0\.62rem|row-gap:\s*0\.78rem|max-height:\s*112px|max-height:\s*104px|font-size:\s*0\.58rem/u,
    'partners route no longer owns the partnership geometry cluster',
  );
});
