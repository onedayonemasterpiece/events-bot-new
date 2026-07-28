import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('recently elapsed events extend detail routes without entering listing catalog', async () => {
  const [events, page, ics, exporter, productionBuild, productionCheck, candidateCheck] = await Promise.all([
    read('src/lib/events.ts'),
    read('src/pages/sobytiya/[slug].astro'),
    read('src/pages/sobytiya/[slug]/event.ics.ts'),
    read('scripts/export-production-preview-data.py'),
    read('scripts/build-production.mjs'),
    read('scripts/check-production.mjs'),
    read('scripts/check-secret-candidate.mjs'),
  ]);
  assert.match(events, /import archivedEventData from '\.\.\/data\/preview-event-archive\.json'/u);
  assert.match(events, /export function getEvents\(\)[\s\S]*return \[\.\.\.data\.events\]/u);
  assert.match(events, /export function getEventDetailEvents\(\)/u);
  assert.match(events, /for \(const event of \[\.\.\.data\.events, \.\.\.archivedDetails\.events\]\)/u);
  assert.match(events, /export function isArchivedEventDetail/u);
  assert.match(page, /getEventDetailEvents\(\)\.map/u);
  assert.match(page, /noindex=\{archivedDetail\}/u);
  assert.match(ics, /getEventDetailEvents\(\)\.map/u);
  assert.match(exporter, /EVENT_DETAIL_ARCHIVE_DAYS = 30/u);
  assert.match(exporter, /detail\/ICS only; excluded from listings, Search, Popular and recommendations/u);
  assert.match(productionBuild, /event_detail_archive/u);
  assert.match(productionCheck, /archived detail leaked into sitemap/u);
  assert.match(candidateCheck, /eventArchiveData\.events/u);
});
