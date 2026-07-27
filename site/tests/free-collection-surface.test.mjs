import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('materialized Free collection owns a large right identity then a compact sticky shelf', async () => {
  const [surface, route] = await Promise.all([
    read('src/components/FreeCollectionSurface.astro'),
    read('src/pages/podborki/[slug]/index.astro'),
  ]);
  assert.match(route, /collection\.slug === 'besplatnye-sobytiya'[\s\S]*<FreeCollectionSurface/u);
  assert.match(surface, /data-free-collection-medallion="large"/u);
  assert.match(surface, /data-free-collection-medallion="compact"/u);
  assert.match(surface, /\.free-collection__medallion--hero \{[^}]*justify-self:end/u);
  assert.match(surface, /\.free-collection__shelf \{[\s\S]*?position:sticky;[\s\S]*?top:57px/u);
  assert.match(surface, /@media\(max-width:759px\)[\s\S]*?\.free-collection__shelf \{ top:64px/u);
  assert.match(surface, /events\.map\(\(event\) => <EventCard event=\{event\} mobileFlowMedia \/>\)/u);
  assert.match(surface, /parents=\{\[\s*\{ label:'Афиша', href:siteHomeHref\(\) \},\s*\]\}/u);
  assert.doesNotMatch(surface, /label:'Поиск'|\/poisk\//u, 'Free is a first-class collection, not a saved Search result');
  assert.match(route, /mobileSection=\{collection\.slug === 'besplatnye-sobytiya' \? 'home' : 'search'\}/u);
});
