import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('materialized Free collection owns a large right identity then a transparent compact sticky medallion', async () => {
  const [surface, route] = await Promise.all([
    read('src/components/FreeCollectionSurface.astro'),
    read('src/pages/podborki/[slug]/index.astro'),
  ]);
  assert.match(route, /collection\.slug === 'besplatnye-sobytiya'[\s\S]*<FreeCollectionSurface/u);
  assert.match(surface, /data-free-collection-medallion="large"/u);
  assert.match(surface, /data-free-collection-medallion="compact"/u);
  assert.match(surface, /\.free-collection__medallion--hero \{[^}]*justify-self:end/u);
  assert.match(surface, /\.free-collection__sticky-identity \{[\s\S]*?position:sticky;[\s\S]*?top:57px/u);
  assert.match(surface, /@media\(max-width:759px\)[\s\S]*?\.free-collection__sticky-identity \{ top:64px/u);
  assert.doesNotMatch(surface, /free-collection__shelf|backdrop-filter/u);
  assert.match(surface, /data-compact-visible/u);
  assert.match(surface, /import OptimizedEventCardGrid from '\.\/OptimizedEventCardGrid\.astro'/u);
  assert.match(surface, /events=\{regularEvents\}[\s\S]*?rowSize=\{3\}[\s\S]*?responsiveMobile[\s\S]*?surface="free-collection-events"/u);
  assert.match(surface, /events=\{exhibitionEvents\}[\s\S]*?rowSize=\{3\}[\s\S]*?responsiveMobile[\s\S]*?surface="free-collection-exhibitions"/u);
  assert.doesNotMatch(surface, /<EventCard|mobileFlowMedia/u, 'Free collection must use the certified typical large-card grid rather than a page-local callsite');
  assert.match(surface, /parents=\{\[\s*\{ label:'Афиша', href:siteHomeHref\(\) \},\s*\]\}/u);
  assert.doesNotMatch(surface, /label:'Поиск'|\/poisk\//u, 'Free is a first-class collection, not a saved Search result');
  assert.match(route, /mobileSection=\{collection\.slug === 'besplatnye-sobytiya' \? 'afisha' : 'search'\}/u);
  assert.match(route, /activeFixture\?\.scenario\.updated_date \|\| getPreviewBuild\(\)\.current_date/u);
});

test('free collection separates ongoing exhibitions after timed events', async () => {
  const [surface, optimizedGrid] = await Promise.all([
    read('src/components/FreeCollectionSurface.astro'),
    read('src/components/OptimizedEventCardGrid.astro'),
  ]);
  assert.match(surface, /\(event\.event_type \|\| ''\)\.trim\(\)\.toLocaleLowerCase\('ru-RU'\) === 'выставка'/u);
  assert.match(surface, /event\.topics\.includes\('EXHIBITIONS'\)/u);
  assert.match(surface, /data-free-collection-event-group="events"/u);
  assert.match(surface, /data-free-collection-event-group="exhibitions"/u);
  assert.match(surface, /Бесплатные выставки/u);
  assert.ok(
    surface.indexOf('data-free-collection-event-group="events"')
      < surface.indexOf('data-free-collection-event-group="exhibitions"'),
    'ordinary/timed events must precede the exhibition group',
  );
  assert.match(optimizedGrid, /optimizationBatchSize = Math\.max\(rowSize, rowSize \* 3\)/u);
  assert.match(optimizedGrid, /rowIndex: layout\.rowIndex \+ rowOffset/u);
});
