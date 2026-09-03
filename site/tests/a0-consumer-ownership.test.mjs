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

test('interest-club cards use one canonical collection palette and arrow identity', async () => {
  const source = await read('src/components/InterestClubCard.astro');

  assert.match(source, /import SemanticIcon from '\.\/design-system\/SemanticIcon\.astro'/u);
  assert.match(source, /data-ds-family="InterestClubCard"/u);
  assert.match(source, /data-ke-foundation-consumer="interest-club-card"/u);
  assert.match(source, /<SemanticIcon name="arrow-right" role="inline" \/>/u);
  for (const token of [
    '--ke-color-club-card-surface',
    '--ke-color-club-card-fallback-start',
    '--ke-color-club-card-veil-bottom',
    '--ke-color-club-card-future-desktop-start',
    '--ke-elevation-club-future',
    '--ke-elevation-club-card-focus',
    '--ke-club-card-radius-mobile',
    '--ke-club-card-arrow-icon-size',
  ]) assert.match(source, new RegExp(token, 'u'));
  assert.doesNotMatch(
    source,
    /background:\s*#17343a|color:\s*#e9fffd|color:\s*#a9ddff|outline:\s*3px solid #f4b942|box-shadow:\s*0 22px 48px/u,
    'club card no longer owns its canonical collection palette or elevations',
  );
});

test('artifact collection uses canonical collection state, dialog and close identity', async () => {
  const source = await read('src/components/artifacts/ArtifactCollection.astro');

  assert.match(source, /import SemanticIcon from '\.\.\/design-system\/SemanticIcon\.astro'/u);
  assert.match(source, /data-ds-family="ArtifactCollection"/u);
  assert.match(source, /data-ke-foundation-consumer="artifact-collection"/u);
  assert.match(source, /<SemanticIcon name="close" role="control" \/>/u);
  assert.match(source, /root\.dataset\.dsState = found \? 'found' : 'empty'/u);
  for (const token of [
    '--ke-artifact-container',
    '--ke-color-artifact-device-surface',
    '--ke-color-artifact-slot-border',
    '--ke-elevation-artifact-slot',
    '--ke-artifact-slot-min-height-mobile',
    '--ke-elevation-amber-collection-media',
    '--ke-artifact-dialog-radius',
    '--ke-elevation-artifact-dialog',
    '--ke-artifact-dialog-close-icon-size',
    '--ke-color-artifact-disabled-surface',
  ]) assert.match(source, new RegExp(token, 'u'));
  assert.doesNotMatch(
    source,
    />×<|color:#281d17|background:#fff6df|background:#fffaf2|box-shadow:0 12px 30px|box-shadow:0 24px 80px|background:#eee2d5/u,
    'artifact collection no longer owns the canonical collection palette, elevation or close glyph',
  );
});
