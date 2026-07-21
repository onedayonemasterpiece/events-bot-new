import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const searchPage = read('src/pages/poisk/index.astro');
const learning = read('src/components/SearchCollectionLinks.astro');
const collections = read('src/data/searchCollections.ts');
const collectionPage = read('src/pages/podborki/[slug]/index.astro');
const donor = read('src/components/AuthorizedEventSearch.astro');

test('materialized collections are real static links and examples are fill-only controls', () => {
  assert.match(learning, /href=\{withBase\(`\/podborki\/\$\{item\.slug\}\/`\)\}/u);
  assert.match(learning, /data-search-collection-link/u);
  assert.match(learning, /type="button"[\s\S]*data-search-query-fill=\{item\.phrase\}/u);
  assert.match(learning, /input\.value = query/u);
  assert.doesNotMatch(learning, /form\.requestSubmit|form\.submit|window\.location|fetch\(/u);
  assert.match(learning, /пример/u);
  assert.match(learning, /Личные сохранённые запросы[\s\S]*только после входа/u);
});

test('technical seed-tag copy is removed from the Search page', () => {
  assert.match(searchPage, /<SearchCollectionLinks\s*\/>/u);
  assert.doesNotMatch(searchPage, /Поисковые теги|seed-теги|могут стать страницами/u);
});

test('materialized collection routes use canonical large EventCard without bespoke result rows', () => {
  assert.match(collectionPage, /import EventCard from/u);
  assert.match(collectionPage, /events\.map\(\(event\) => <EventCard event=\{event\} \/>\)/u);
  assert.match(collectionPage, /data-search-collection-results/u);
  assert.match(collectionPage, /noindex/u);
  assert.doesNotMatch(collectionPage, /EventListItem|authorized-search__vector-card|search-result-row/u);
  assert.match(collections, /collapseOccurrenceCards\(matches, 'per-family'\)/u);
});

test('collection claims are explicit and derived from actual event fields', () => {
  assert.match(collections, /event\.ticket\.is_free/u);
  assert.match(collections, /event\.topics\.some/u);
  assert.match(collections, /event\.topics\.includes\('STANDUP'\)/u);
  assert.match(collections, /\/джаз\/iu\.test\(event\.title\)/u);
  assert.doesNotMatch(collections, /similar|embedding|inference/iu);
});

test('v58 donor retains separate submit progress and canonical EventCard rendering', () => {
  assert.match(donor, /data-search-form/u);
  assert.match(donor, /data-search-submit/u);
  assert.match(donor, /--search-progress/u);
  assert.match(donor, /submit\.classList\.toggle\('is-loading', isLoading\)/u);
  assert.match(donor, /window\.KenigEventsRenderEventCard/u);
  assert.match(donor, /renderer\([^\n]+, 'split-actions'\)/u);
  assert.doesNotMatch(searchPage, /email|magic link|otp/iu);
});
