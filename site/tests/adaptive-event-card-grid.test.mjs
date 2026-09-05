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

  assert.match(adaptive, /import EventCard(?:, \{ type EventCardRootAttributes \})? from '\.\/EventCard\.astro'/u);
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
    'data-ds-version="2"',
    'data-adaptive-event-card-grid',
    'data-adaptive-grid-card-root="EventCard"',
    'data-adaptive-grid-layout-engine="flex-lines"',
    'data-adaptive-grid-remainder-policy="regular-column"',
  ]) assert.ok(adaptive.includes(marker), `missing ${marker}`);
  assert.match(adaptive, /data-adaptive-grid-remainder-variant=\{initialRemainderVariant\}/u);
  assert.match(adaptive, /data-optimized-event-card-grid=\{legacyOptimizedContract \? '' : undefined\}/u);
});

test('Wave 2 keeps consumer metadata data-only and canonical diagnostics reserved', async () => {
  const [adaptive, card] = await Promise.all([
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/EventCard.astro'),
  ]);

  assert.match(adaptive, /rootAttributes\?: EventCardRootAttributes/u);
  assert.match(adaptive, /itemRoots\?: Record<string, AdaptiveEventCardItemRoot>/u);
  assert.match(adaptive, /\^data-\[a-z0-9_\.:-\]\+\$/u);
  assert.match(adaptive, /!ADAPTIVE_ROOT_RESERVED_ATTRIBUTES\.has\(name\)/u);
  assert.match(adaptive, /'data-adaptive-grid-remainder-variant'/u);
  assert.match(adaptive, /itemRoots\[`\$\{item\.id\}:\$\{sourceIndex\}`\] \|\| itemRoots\[String\(item\.id\)\]/u);
  assert.match(adaptive, /const itemRoot = itemRootFor\(item, resolvedSourceIndexes\[renderedIndex\] \?\? -1\);/u);
  assert.match(adaptive, /rootClassName=\{itemRoot\?\.className\}/u);
  assert.match(adaptive, /rootAttributes=\{itemRoot\?\.attributes\}/u);
  assert.equal((adaptive.match(/<EventCard\b/gu) || []).length, 1, 'metadata bridge must not add a wrapper or second card root');

  assert.match(card, /rootClassName\?: string/u);
  assert.match(card, /rootAttributes\?: EventCardRootAttributes/u);
  assert.match(card, /!EVENT_CARD_RESERVED_ROOT_ATTRIBUTES\.has\(name\)/u);
  assert.match(card, /<article\s+\{\.\.\.safeRootAttributes\}[\s\S]*data-ds-component="EventCard"/u);
});

test('Wave 2 responsive strategies and legacy adapter mapping remain explicit', async () => {
  const [adaptive, legacy] = await Promise.all([
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/OptimizedEventCardGrid.astro'),
  ]);

  assert.match(adaptive, /type ResponsiveStrategy = 'fixed' \| 'progressive' \| 'stack'/u);
  assert.match(adaptive, /requestedResponsive === 'progressive' \|\| requestedResponsive === 'stack' \|\| requestedResponsive === 'fixed'/u);
  assert.match(adaptive, /: responsiveMobile \? 'stack' : 'fixed'/u);
  assert.match(adaptive, /data-adaptive-grid-responsive=\{responsive\}/u);
  for (const diagnostic of ['source-count', 'rendered-count', 'remainder-count', 'remainder-variant', 'source-order', 'rendered-order', 'item-root-contract']) {
    assert.ok(adaptive.includes(`data-adaptive-grid-${diagnostic}`), `missing ${diagnostic} diagnostic`);
  }
  assert.match(adaptive, /@media \(max-width: 1023px\)[\s\S]*adaptive-event-card-grid--responsive-stack/u);
  assert.match(adaptive, /@media \(max-width: 960px\)[\s\S]*adaptive-event-card-grid--responsive-progressive/u);
  assert.match(adaptive, /@media \(max-width: 620px\)[\s\S]*adaptive-event-card-grid--responsive-progressive/u);
  assert.match(adaptive, /adaptive-event-card-grid--responsive-stack\[data-adaptive-grid-row-size\]/u);
  assert.match(adaptive, /adaptive-event-card-grid--responsive-progressive\[data-adaptive-grid-row-size\]/u);
  assert.match(legacy, /responsive=\{responsiveMobile \? 'stack' : 'fixed'\}/u);
  assert.doesNotMatch(legacy, /<style>|packRelatedCardRows|<EventCard\b/u);
});

test('AdaptiveEventCardGrid shared tracks fill full rows while preserving ordinary column widths on remainder rows', async () => {
  const adaptive = await read('src/components/AdaptiveEventCardGrid.astro');

  assert.match(adaptive, /\.cards-grid\.adaptive-event-card-grid \{[\s\S]*display: grid;[\s\S]*grid-template-columns: repeat\(var\(--adaptive-event-card-columns, 3\), minmax\(0, 1fr\)\);[\s\S]*gap: var\(--adaptive-event-card-gap\);/u);
  assert.match(adaptive, /\.adaptive-event-card-grid > :global\(\.event-card\) \{[\s\S]*grid-row: span 4;[\s\S]*grid-template-rows: subgrid;/u);
  for (let rowSize = 1; rowSize <= 6; rowSize += 1) {
    assert.match(adaptive, new RegExp(`data-adaptive-grid-row-size="${rowSize}"`, 'u'));
  }
  assert.match(adaptive, /data-adaptive-grid-row-size="1"[^\n]*--adaptive-event-card-columns: 1/u);
  assert.match(adaptive, /data-adaptive-grid-row-size="2"[^\n]*--adaptive-event-card-columns: 2/u);
  assert.ok(adaptive.includes("type AdaptiveGridRemainderVariant = 'complete' | `regular-${number}-of-${number}`;"));
  assert.match(adaptive, /return remainder === 0 \? 'complete' : `regular-\$\{remainder\}-of-\$\{size\}`/u);
  assert.match(adaptive, /grid\.dataset\.adaptiveGridRemainderVariant = runtimeRemainderVariant/u);
  assert.match(adaptive, /`remainder-\$\{runtimeRemainderVariant\}`/u);
  assert.doesNotMatch(adaptive, /repeat\(\s*auto-fit/u);
});

test('packed grid keeps row geometry and layering but delegates MediaFrame anatomy', async () => {
  const adaptive = await read('src/components/AdaptiveEventCardGrid.astro');
  const shell = /\.adaptive-event-card-grid--packed :global\(\[data-lab-related-card\] \.event-card__media-shell\) \{([\s\S]*?)\n  \}/u.exec(adaptive)?.[1] || '';
  const image = /\.adaptive-event-card-grid--packed :global\(\[data-lab-related-card\] \.event-card__media\) \{([\s\S]*?)\n  \}/u.exec(adaptive)?.[1] || '';

  assert.match(shell, /height: auto !important/u);
  assert.match(shell, /aspect-ratio: var\(--lab-row-media-ratio\) !important/u);
  assert.match(shell, /background: #d2c5b7/u);
  assert.doesNotMatch(shell, /\b(?:position|isolation|width|overflow)\s*:/u);
  assert.match(image, /position: absolute !important/u);
  assert.match(image, /z-index: 2/u);
  assert.match(image, /inset: 0/u);
  assert.doesNotMatch(image, /\b(?:width|height)\s*:/u);
});

test('normalized component families and MediaFrame diagnostics retain exact versions, roles and fits', async () => {
  const [adaptive, card, listing, rail, mediaFrame] = await Promise.all([
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/EventCard.astro'),
    read('src/components/listings/ListingEventCard.astro'),
    read('src/components/EventMediaRail.astro'),
    read('src/components/media-frame.css'),
  ]);

  for (const [source, family, version] of [
    [adaptive, 'AdaptiveEventCardGrid', '2'],
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
  assert.doesNotMatch(rail, /style=\{`object-fit:/u);
  assert.doesNotMatch(rail, /\.event-media-rail__frame\s*\{/u);
  for (const source of [card, listing, rail]) {
    assert.doesNotMatch(source, /<(?:a|button)\b[^>]*data-media-frame/u);
    assert.match(source, /data-media-frame-style-owner="media-frame\.css"/u);
    assert.match(source, /data-media-frame-role=/u);
    assert.match(source, /data-media-frame-crop-permission=/u);
    assert.match(source, /data-media-frame-focal-position=/u);
    assert.match(source, /data-media-frame-clip="frame"/u);
    assert.match(source, /data-media-frame-radius="surface"/u);
    assert.match(source, /data-media-frame-interaction-owner="caller"/u);
    assert.match(source, /data-media-frame-(?:image|fallback)/u);
  }
  assert.match(mediaFrame, /Canonical MediaFrame v1 structural and fit owner/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="cover"\][\s\S]*object-fit: cover/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="contain"\][\s\S]*object-fit: contain/u);
  assert.match(mediaFrame, /object-position: var\(--media-frame-object-position, 50% 50%\)/u);
});
