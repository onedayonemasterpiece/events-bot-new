import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('AdaptiveEventCardGrid owns flow and packed layout while the legacy grid is adapter-only', async () => {
  const [adaptive, legacy] = await Promise.all([
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/OptimizedEventCardGrid.astro'),
  ]);

  assert.match(adaptive, /import EventCard from '\.\/EventCard\.astro'/u);
  assert.match(adaptive, /import \{ packRelatedCardRows \} from '\.\.\/lib\/relatedCardLayout\.mjs'/u);
  assert.equal((adaptive.match(/<EventCard\b/gu) || []).length, 1);
  assert.match(adaptive, /mode === 'packed'\s*\? packRelatedCardRows\(events, \{ limit, rowSize, mediaTreatment \}\)/u);
  assert.match(adaptive, /events\.slice\(0, limit\)\.map\(\(item\) => \(\{ item, layout: undefined \}\)\)/u);
  assert.match(adaptive, /mode === 'packed' && 'cards-grid--immersive'/u);
  assert.match(adaptive, /desktopRelatedCrop=\{mode === 'packed'\}/u);
  assert.match(adaptive, /desktopRelatedLayout=\{mode === 'packed' \? layout : undefined\}/u);

  assert.match(legacy, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro'/u);
  assert.match(legacy, /<AdaptiveEventCardGrid/u);
  assert.match(legacy, /mode="packed"/u);
  assert.match(legacy, /legacyOptimizedContract/u);
  assert.doesNotMatch(legacy, /import EventCard/u);
  assert.doesNotMatch(legacy, /<EventCard\b/u);
  assert.doesNotMatch(legacy, /packRelatedCardRows/u);
  assert.doesNotMatch(legacy, /<style>/u);
});

test('AdaptiveEventCardGrid clamps inputs and exposes the exact diagnostics contract', async () => {
  const adaptive = await read('src/components/AdaptiveEventCardGrid.astro');

  assert.match(adaptive, /Math\.max\(1, Math\.min\(6, Math\.floor\(Number\.isFinite\(numericRowSize\) \? numericRowSize : 3\)\)\)/u);
  assert.match(adaptive, /Math\.max\(0, Math\.floor\(Number\.isFinite\(numericLimit\) \? numericLimit : defaultLimit\)\)/u);
  assert.match(adaptive, /const defaultLimit = mode === 'packed' \? 10 : events\.length/u);
  assert.match(adaptive, /const gridVariant = `\$\{mode\}-\$\{rowSize\}-column`/u);
  for (const marker of [
    'data-ds-component="AdaptiveEventCardGrid"',
    'data-ds-family="AdaptiveEventCardGrid"',
    'data-ds-version="1"',
    'data-adaptive-event-card-grid',
    'data-adaptive-grid-card-root="EventCard"',
    'data-adaptive-grid-layout-engine="flex-lines"',
    'data-adaptive-grid-remainder-policy="stretch"',
  ]) assert.ok(adaptive.includes(marker), `missing ${marker}`);
  assert.match(adaptive, /data-optimized-event-card-grid=\{legacyOptimizedContract \? '' : undefined\}/u);
});

test('AdaptiveEventCardGrid flex lines fill complete and final rows without phantom tracks', async () => {
  const adaptive = await read('src/components/AdaptiveEventCardGrid.astro');

  assert.match(adaptive, /\.cards-grid\.adaptive-event-card-grid \{[\s\S]*display: flex;[\s\S]*flex-wrap: wrap;[\s\S]*gap: var\(--adaptive-event-card-gap\);/u);
  assert.match(adaptive, /\.adaptive-event-card-grid > :global\(\.event-card\) \{[\s\S]*flex-grow: 1;[\s\S]*flex-shrink: 1;/u);
  for (let rowSize = 1; rowSize <= 6; rowSize += 1) {
    assert.match(adaptive, new RegExp(`data-adaptive-grid-row-size="${rowSize}"`, 'u'));
  }
  assert.match(adaptive, /data-adaptive-grid-row-size="1"[^\n]*flex-basis: 100%/u);
  assert.match(adaptive, /data-adaptive-grid-row-size="2"[^\n]*flex-basis: calc\(50% - var\(--adaptive-event-card-gap\)\)/u);
  assert.doesNotMatch(adaptive, /grid-template-columns:\s*repeat/u);
});

test('normalized component families and MediaFrame diagnostics retain their exact versions and fits', async () => {
  const [adaptive, card, listing, rail] = await Promise.all([
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/EventCard.astro'),
    read('src/components/listings/ListingEventCard.astro'),
    read('src/components/EventMediaRail.astro'),
  ]);

  for (const [source, family, version] of [
    [adaptive, 'AdaptiveEventCardGrid', '1'],
    [card, 'EventCard', '2'],
    [listing, 'ListingEventCard', '9'],
    [rail, 'EventMediaRail', '1'],
  ]) {
    assert.ok(source.includes(`data-ds-component="${family}"`));
    assert.ok(source.includes(`data-ds-family="${family}"`));
    assert.ok(source.includes(`data-ds-version="${version}"`));
  }

  assert.match(card, /cardCrop\.mediaTreatment === 'visual-cover'[\s\S]*\? 'visual'/u);
  assert.match(card, /const mediaFrameFit = imageUrl \? cardCrop\.fit : 'contain'/u);
  assert.match(listing, /const mediaFrameFit = image\.mode\.endsWith\('-crop'\) \? 'cover' : 'contain'/u);
  assert.match(listing, /class="ke-listing-card__media ke-skeleton"[\s\S]*data-media-frame/u);
  assert.match(rail, /if \(asset\.media_semantic_status === 'error'\) return 'unknown'/u);
  assert.match(rail, /mediaFrameKind\(asset\) === 'visual' \? 'cover' : 'contain'/u);
  assert.match(rail, /style=\{`object-fit:\$\{mediaFrameFit\(asset\)\}`\}/u);
  assert.doesNotMatch(rail, /<a\b[^>]*data-media-frame/u);
});
