import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('accepted desktop and graphite CTA are registered runtime consumers', async () => {
  const [registrySource, route, desktop, catalog] = await Promise.all([
    read('src/data/design-system-registry.json'),
    read('src/pages/sobytiya/[slug].astro'),
    read('src/components/DesktopEventPage.astro'),
    read('src/pages/lab/design-system/index.astro'),
  ]);
  const registry = JSON.parse(registrySource);
  assert.equal(registry.accepted_runtime_base, 'integration/static-event-v10-system-routing@d5dab75a');
  assert.match(route, /import DesktopEventPage/u);
  assert.match(route, /<DesktopEventPage/u);
  assert.match(desktop, /<DesktopEventActionPanel/u);
  assert.doesNotMatch(route, /EventCtaPanel|EventMediaRail/u);
  for (const state of ['paid-price', 'paid-unknown', 'registration', 'free-calendar', 'free-registration', 'phone-copy', 'source', 'sold-out', 'unavailable']) {
    assert.ok(catalog.includes(`'${state}'`), `missing graphite CTA state ${state}`);
  }
  assert.match(catalog, /editorial-ocr-companion-arrival/u);
  assert.match(catalog, /split-low-resolution/u);
});

test('search and personal feed use shared skeleton without hidden override', async () => {
  const [search, personal, catalog, css] = await Promise.all([
    read('src/components/AuthorizedEventSearch.astro'),
    read('src/components/PersonalFeedSlot.astro'),
    read('src/pages/lab/design-system/index.astro'),
    read('src/styles/design-system.css'),
  ]);
  assert.match(search, /import Skeleton/u);
  assert.match(search, /showSkeleton: !append/u);
  assert.doesNotMatch(search, /showSkeleton: false/u);
  assert.match(personal, /data-personal-feed-fixture/u);
  assert.doesNotMatch(catalog, /personal-feed-section\[hidden\].*display:\s*block/su);
  assert.doesNotMatch(catalog, /tone="loading"/u);
  assert.doesNotMatch(css, /ke-state-panel--loading/u);
  for (const state of ['anonymous', 'ready', 'progress', 'skeleton', 'results', 'empty', 'error', 'quota']) {
    assert.ok(catalog.includes(`previewState="${state}"`), `missing search fixture ${state}`);
  }
});
