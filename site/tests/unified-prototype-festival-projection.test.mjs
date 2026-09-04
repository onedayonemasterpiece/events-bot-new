import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), 'utf8');

test('unified prototype derives festival completeness from the current exported projection', async () => {
  const checker = await read('scripts/check-unified-prototype.mjs');

  assert.match(checker, /src\/data\/festival-timeline\.json/u);
  assert.match(checker, /expectedFestivalCount = projectedFestivals\.length/u);
  assert.match(checker, /projectedFestivalMonths = new Set/u);
  assert.match(checker, /for \(const month of projectedFestivalMonths\)/u);
  assert.match(checker, /data-festival-count="\$\{expectedFestivalCount\}"/u);
  assert.match(checker, /data-festival-card="\$\{festival\.slug\}"/u);
  assert.doesNotMatch(checker, /data-festival-count="21"/u);
  assert.doesNotMatch(checker, /length < 6/u);
  assert.doesNotMatch(checker, /\['july', 'august'/u);
});

test('unified prototype verifies the current always-visible occurrence selector markup', async () => {
  const checker = await read('scripts/check-unified-prototype.mjs');

  assert.match(checker, /data-occurrence-variant="desktop"/u);
  assert.match(checker, /data-occurrence-variant="mobile"/u);
  assert.match(checker, /data-occurrence-variant="practical"/u);
  assert.match(checker, /event-occurrences__rows/u);
  assert.doesNotMatch(checker, /includes\('event-occurrences__schedule'\)/u);
});

test('full real gate selects current projected event specimens instead of expired ids', async () => {
  const checker = await read('scripts/check-unified-prototype.mjs');

  assert.match(checker, /const compatibilityPage = eventPages\.find/u);
  assert.match(checker, /const semanticErrorEvent = eventsData\.events\.find/u);
  assert.match(checker, /const forecastPage = eventPages\.find/u);
  assert.match(checker, /data-event-end-basis="forecast"/u);
  assert.doesNotMatch(checker, /event\.id === 6686/u);
  assert.doesNotMatch(checker, /event\.id === 6529/u);
});
