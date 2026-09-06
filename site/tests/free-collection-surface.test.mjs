import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { freeCollectionCountMessage, freeEventCountLabel } from '../src/lib/freeCollection.mjs';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('Free collection retains its medallion identity while using one ordinary listing', async () => {
  const [surface, route, layout] = await Promise.all([
    read('src/components/FreeCollectionSurface.astro'),
    read('src/pages/podborki/[slug]/index.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);
  assert.match(route, /collection\.slug === 'besplatnye-sobytiya'[\s\S]*<FreeCollectionSurface/u);
  assert.match(surface, /data-free-collection-medallion="large"/u);
  assert.equal((surface.match(/data-free-collection-medallion=/gu)||[]).length,1);
  assert.match(surface, /\.free-collection__medallion--hero \{[\s\S]*?position:sticky/u);
  assert.match(surface, /grid-area:2 \/ 2 \/ 4 \/ 3/u);
  assert.match(surface, /transform-origin:right top/u);
  assert.match(surface, /to \{transform:scale\(\.84\)/u);
  assert.doesNotMatch(surface, /free-collection__sticky-identity|medallion--compact|data-compact-visible|backdrop-filter/u);
  assert.match(surface, /<h1 id="free-collection-title">Бесплатные события<\/h1>/u);
  assert.match(surface, /<AdaptiveEventCardGrid[\s\S]*?events=\{initialEvents\}[\s\S]*?rowSize=\{3\}[\s\S]*?mobileFlowMedia[\s\S]*?discoveryFeed[\s\S]*?runtimeManaged[\s\S]*?runtimeVisibleOnly[\s\S]*?runtimeSourcePolicy="all-direct"/u);
  assert.match(surface, /data-free-collection-grid/gu);
  assert.match(surface, /data-free-collection-eligibility':'confirmed-free'/u);
  assert.match(surface, /planRelatedCardRows\(events, \{ rowSize:3, presentation:'flow' \}\)/u);
  assert.match(surface, /const initialEvents = plannedEvents\.slice\(0, 12\)/u);
  assert.match(surface, /composition_order: 'global-natural-rows-v1'/u);
  assert.match(surface, /composition_source_event_ids: events\.map/u);
  assert.match(surface, /related_static: plannedEvents\.map/u);
  assert.match(surface, /id="free-collection-catalog" type="application\/json"[\s\S]*?set:html=\{freeCatalogJson\}/u);
  assert.match(surface, /page_size: 12, preload_target: 12, eligibility_filter: 'confirmed-free'/u);
  assert.match(surface, /JSON\.stringify\(freeCatalog\)\.replace\(\/</u);
  assert.match(surface, /discoverySrc="#free-collection-catalog"/u);
  assert.match(surface, /data-free-collection-result-count[\s\S]*?data-free-collection-loaded-count[\s\S]*?data-free-collection-total-count/u);
  assert.match(layout, /function retainPreplannedFreeComposition/u);
  assert.match(layout, /store\.manifest\.composition_order === 'global-natural-rows-v1'/u);
  assert.match(layout, /feed\.dataset\.framingPlan = 'global-natural-rows-v1'/u);
  assert.doesNotMatch(surface, /ke-type-display-collection|Готовая подборка|Как собрана:|Это не личный сохранённый поиск/u);
  assert.match(surface, /font:var\(--ke-type-h1\)/u);
  assert.doesNotMatch(surface, /data-free-collection-event-group="exhibitions"|Бесплатные выставки|regularEvents|exhibitionEvents/u);
  assert.doesNotMatch(surface, /<Breadcrumbs|data-product-parent-link/u, 'Free has no redundant back-to-Afisha breadcrumb');
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
