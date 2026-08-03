import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('checked registry exposes the four statuses and blocks blocked surfaces everywhere', async () => {
  const registry = JSON.parse(await read('src/data/static-collection-registry.json'));
  assert.equal(registry.schema_version, 'static-collection-registry-v1');
  assert.deepEqual(new Set(registry.entries.map((entry) => entry.status)), new Set(['public', 'repair', 'blocked', 'deferred']));
  for (const entry of registry.entries.filter((item) => item.status === 'blocked')) {
    assert.equal(entry.catalog, false);
    assert.equal(entry.navigation, false);
    assert.equal(entry.sitemap, false);
  }
});

test('catalog, navigation and sitemap consume the registry instead of prose classification', async () => {
  const [catalog, menu, sitemap, gastronomy, resolver] = await Promise.all([
    read('src/pages/podborki/index.astro'),
    read('src/components/Reference4MobileMenu.astro'),
    read('src/pages/sitemap.xml.ts'),
    read('src/pages/podborki/gastronomiya/index.astro'),
    read('src/lib/staticCollections.ts'),
  ]);
  assert.match(catalog, /getCollectionCatalogEntries/u);
  assert.match(menu, /getCollectionNavigationEntries/u);
  assert.match(sitemap, /getCollectionSitemapEntries/u);
  assert.match(gastronomy, /resolveGastronomyCollection\(getEvents\(\)\)/u);
  assert.doesNotMatch(gastronomy, /title\.match|description\.match|topics\.some/u);
  assert.match(resolver, /manifest\.provider_calls !== 0/u);
  assert.match(resolver, /manifest\.catalog_hash !== currentCatalogHash/u);
  assert.match(resolver, /claimedHash !== actualHash/u);
  assert.match(resolver, /lifecycle === 'dormant'/u);
});
