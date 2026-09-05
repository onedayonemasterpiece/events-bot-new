import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { freeCollectionCountMessage, freeEventCountLabel } from '../src/lib/freeCollection.mjs';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('Free collection retains its medallion identity while using one ordinary listing', async () => {
  const [surface, route] = await Promise.all([
    read('src/components/FreeCollectionSurface.astro'),
    read('src/pages/podborki/[slug]/index.astro'),
  ]);
  assert.match(route, /collection\.slug === 'besplatnye-sobytiya'[\s\S]*<FreeCollectionSurface/u);
  assert.match(surface, /data-free-collection-medallion="large"/u);
  assert.match(surface, /data-free-collection-medallion="compact"/u);
  assert.match(surface, /\.free-collection__medallion--hero \{[^}]*justify-self:end/u);
  assert.match(surface, /\.free-collection__sticky-identity \{[\s\S]*?position:sticky;[\s\S]*?top:var\(--ke-free-sticky-top\)/u);
  assert.match(surface, /@media\(max-width:759px\)[\s\S]*?\.free-collection__sticky-identity \{ top:var\(--ke-free-sticky-top-compact\)/u);
  assert.doesNotMatch(surface, /free-collection__shelf|backdrop-filter/u);
  assert.match(surface, /data-compact-visible/u);
  assert.match(surface, /<h1 id="free-collection-title">Бесплатные события<\/h1>/u);
  assert.match(surface, /<AdaptiveEventCardGrid[\s\S]*?events=\{initialEvents\}[\s\S]*?rowSize=\{3\}[\s\S]*?mobileFlowMedia[\s\S]*?discoveryFeed[\s\S]*?runtimeManaged[\s\S]*?runtimeVisibleOnly[\s\S]*?runtimeSourcePolicy="all-direct"/u);
  assert.match(surface, /data-free-collection-grid/gu);
  assert.match(surface, /data-free-collection-eligibility':'confirmed-free'/u);
  assert.match(surface, /events\.slice\(0, 12\)/u);
  assert.match(surface, /id="free-collection-catalog" type="application\/json"[\s\S]*?set:html=\{freeCatalogJson\}/u);
  assert.match(surface, /page_size: 12, preload_target: 12, eligibility_filter: 'confirmed-free'/u);
  assert.match(surface, /JSON\.stringify\(freeCatalog\)\.replace\(\/</u);
  assert.match(surface, /discoverySrc="#free-collection-catalog"/u);
  assert.match(surface, /data-free-collection-result-count[\s\S]*?data-free-collection-loaded-count[\s\S]*?data-free-collection-total-count/u);
  assert.doesNotMatch(surface, /data-free-collection-event-group="exhibitions"|Бесплатные выставки|regularEvents|exhibitionEvents/u);
  assert.match(surface, /parents=\{\[\s*\{ label:'Афиша', href:siteHomeHref\(\) \},\s*\]\}/u);
  assert.doesNotMatch(surface, /label:'Поиск'|\/poisk\//u, 'Free is a first-class collection, not a saved Search result');
  assert.match(route, /mobileSection=\{collection\.slug === 'besplatnye-sobytiya' \? 'home' : 'search'\}/u);
});

test('Free collection count distinguishes loaded cards from the eligible total with Russian forms', () => {
  assert.deepEqual(
    [0, 1, 2, 5, 11, 21, 22, 25].map(freeEventCountLabel),
    ['0 событий', '1 событие', '2 события', '5 событий', '11 событий', '21 событие', '22 события', '25 событий'],
  );
  assert.equal(freeCollectionCountMessage(2, 5), 'Показано 2 события из 5 событий');
  assert.equal(freeCollectionCountMessage(5, 5), '5 событий');
  assert.equal(freeCollectionCountMessage(0, 0), '0 событий');
});
