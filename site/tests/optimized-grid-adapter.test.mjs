import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('OptimizedEventCardGrid is a thin compatibility adapter over the canonical adaptive grid', async () => {
  const [adapter, adaptive, desktop] = await Promise.all([
    read('src/components/OptimizedEventCardGrid.astro'),
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/DesktopEventPage.astro'),
  ]);

  assert.match(desktop, /import OptimizedEventCardGrid from '\.\/OptimizedEventCardGrid\.astro'/u);
  assert.match(adapter, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro'/u);
  assert.equal((adapter.match(/<AdaptiveEventCardGrid\b/gu) || []).length, 1);
  assert.match(adapter, /mode="packed"/u);
  assert.match(adapter, /responsive=\{responsiveMobile \? 'stack' : 'fixed'\}/u);
  assert.match(adapter, /discoveryFeed=\{!responsiveMobile\}/u);
  assert.match(adapter, /legacyOptimizedContract/u);
  assert.doesNotMatch(adapter, /import EventCard|packRelatedCardRows|<EventCard\b|<style>/u,
    'adapter must not own packing, card markup or grid/media styles');

  assert.match(adaptive, /legacyOptimizedContract\?: boolean/u);
  assert.match(adaptive, /legacyOptimizedContract && 'optimized-event-card-grid'/u);
  assert.match(adaptive, /data-optimized-event-card-grid=\{legacyOptimizedContract \? '' : undefined\}/u);
  assert.match(adaptive, /data-lab-row-normalize=\{mode === 'packed' \? '' : undefined\}/u);
});
