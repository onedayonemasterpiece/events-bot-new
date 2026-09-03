import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const occurrences = (source, token) => source.split(token).length - 1;
const DIRECT_FRAMING_DECLARATION = /(?:^|[;{])\s*object-(?:fit|position)\s*:/mu;

const M0_SOURCE_FILES = [
  'src/lib/relatedCardLayout.mjs',
  'src/components/OptimizedEventCardGrid.astro',
  'src/components/AdaptiveEventCardGrid.astro',
  'src/components/EventCard.astro',
  'src/components/listings/ListingEventCard.astro',
];

test('post-FR0 M0 contract reads only card, grid and row-layout sources', async () => {
  const sources = await Promise.all(M0_SOURCE_FILES.map(read));
  assert.equal(sources.length, M0_SOURCE_FILES.length);
  assert.deepEqual(M0_SOURCE_FILES, [
    'src/lib/relatedCardLayout.mjs',
    'src/components/OptimizedEventCardGrid.astro',
    'src/components/AdaptiveEventCardGrid.astro',
    'src/components/EventCard.astro',
    'src/components/listings/ListingEventCard.astro',
  ]);
});

test('AdaptiveEventCardGrid is the sole card-grid diagnostics and remainder owner', async () => {
  const source = await read('src/components/AdaptiveEventCardGrid.astro');

  assert.match(source, /data-ds-component="AdaptiveEventCardGrid"/u);
  assert.match(source, /data-ds-family="AdaptiveEventCardGrid"/u);
  assert.match(source, /data-ds-version="1"/u);
  assert.match(source, /data-adaptive-grid-layout-engine="flex-lines"/u);
  assert.equal(occurrences(source, 'data-adaptive-grid-diagnostics-owner='), 1,
    'AdaptiveEventCardGrid must publish one diagnostics writer declaration');
  assert.match(source, /data-adaptive-grid-diagnostics-owner="AdaptiveEventCardGrid"/u);
  assert.match(source, /data-adaptive-grid-diagnostics-contract="input-source-rendered-v1"/u);

  for (const field of [
    'data-adaptive-grid-input-count',
    'data-adaptive-grid-input-order',
    'data-adaptive-grid-source-count',
    'data-adaptive-grid-source-order',
    'data-adaptive-grid-rendered-count',
    'data-adaptive-grid-rendered-order',
  ]) assert.ok(source.includes(field), `missing grid population field: ${field}`);

  assert.match(source, /const sourceIndexesByItem = new Map<PreviewEvent, number\[\]>/u);
  assert.match(source, /const sourceIndexesByEventId = new Map<string, number\[\]>/u);
  assert.match(source, /const claimedSourceIndexes = new Set<number>\(\)/u);
  assert.match(source, /const itemRootFor = \(item: PreviewEvent, sourceIndex: number\)[\s\S]*itemRoots\[`\$\{item\.id\}:\$\{sourceIndex\}`\]/u);
  assert.match(source, /data-adaptive-grid-item-root-contract=\{hasItemRoots \? 'event-id-or-event-id-source-index'/u);

  assert.ok(source.includes("type AdaptiveGridRemainderVariant = 'complete' | `stretch-${number}-of-${number}`;"));
  assert.match(source, /const remainderVariantFor = \(count: number, size: number\): AdaptiveGridRemainderVariant/u);
  assert.match(source, /return remainder === 0 \? 'complete' : `stretch-\$\{remainder\}-of-\$\{size\}`;/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRemainderVariant = runtimeRemainderVariant/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRenderedCount = String\(count\)/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRenderedOrder = order/u);

  assert.match(source, /display: flex;/u);
  assert.match(source, /flex-wrap: wrap;/u);
  assert.doesNotMatch(source, /grid-template-columns\s*:/u,
    'canonical adaptive rows must not reintroduce phantom CSS-grid tracks');
  assert.match(source, /adaptive-event-card-grid--responsive-stack\[data-adaptive-grid-row-size\]/u);
  assert.match(source, /adaptive-event-card-grid--responsive-progressive\[data-adaptive-grid-row-size\]/u);
});

test('OptimizedEventCardGrid remains a compatibility adapter only', async () => {
  const source = await read('src/components/OptimizedEventCardGrid.astro');

  assert.equal(occurrences(source, '<AdaptiveEventCardGrid'), 1);
  assert.match(source, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro';/u);
  assert.match(source, /legacyOptimizedContract/u);
  assert.match(source, /responsive=\{responsiveMobile \? 'stack' : 'fixed'\}/u);
  assert.doesNotMatch(source, /import EventCard/u);
  assert.doesNotMatch(source, /packRelatedCardRows/u);
  assert.doesNotMatch(source, /data-adaptive-grid-(?:input|source|rendered|remainder)/u);
  assert.doesNotMatch(source, /<style\b/u);
});

test('EventCard and ListingEventCard keep one root, canonical actions and metadata', async () => {
  const [eventCard, listingCard, actionCss] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/listings/ListingEventCard.astro'),
    read('src/components/event-card.css'),
  ]);

  assert.equal(occurrences(eventCard, '<article'), 1, 'EventCard must expose one root');
  assert.equal(occurrences(listingCard, '<article'), 1, 'ListingEventCard must expose one root');

  for (const [source, family] of [[eventCard, 'EventCard'], [listingCard, 'ListingEventCard']]) {
    assert.ok(source.includes(`data-ds-family="${family}"`));
    assert.match(source, /data-ds-version=/u);
    assert.match(source, /data-ds-variant=/u);
    assert.match(source, /data-ds-state=/u);
    assert.match(source, /data-media-frame-resource-state=/u);
    assert.doesNotMatch(source, /data-media-frame-state|dataset\.mediaFrameState/u);
    assert.doesNotMatch(source, DIRECT_FRAMING_DECLARATION,
      `${family} must publish framing inputs without owning object-fit/object-position paint`);
  }

  for (const action of ['not_interested', 'like']) {
    assert.ok(eventCard.includes(`data-feedback-action="${action}"`), `missing EventCard action: ${action}`);
  }
  assert.match(eventCard, /data-native-share/u);
  assert.match(eventCard, /data-calendar-action/u);
  assert.match(eventCard, /<SemanticIcon name="dislike" role="action" \/>/u);
  assert.match(eventCard, /<SemanticIcon name="share" role="action" \/>/u);
  assert.match(eventCard, /<SemanticIcon name="heart" role="action" \/>/u);
  assert.match(actionCss, /\[data-ds-component="EventCard"\]\.event-card--split-actions[\s\S]*\.feedback-button--negative[\s\S]*min-height: var\(--ke-control-min, 44px\);/u);

  assert.match(eventCard, /data-card-type/u);
  assert.match(eventCard, /<EventOccurrenceLabel presentation=\{occurrencePresentation\} \/>/u);
  assert.match(eventCard, /data-card-status/u);
  assert.match(listingCard, /data-listing-event-type=/u);
  assert.match(listingCard, /showFree && 'free-admission'/u);
  assert.match(listingCard, /data-listing-proof-placement=\{hasSocialProof \? \(proofInside \? 'inside' : 'rail'\) : 'none'\}/u);
  assert.match(listingCard, /const tailWidth = splitIdentityProofRail \? 96 : hasSideRail \? \(visibleIdentityCount === 0 \? 40 : 64\) : 0;/u);
});

test('relatedCardLayout owns numeric normalization, crop budget and deterministic occupancy', async () => {
  const source = await read('src/lib/relatedCardLayout.mjs');

  assert.match(source, /const MAX_DOCUMENT_CROP = 0\.2;/u);
  assert.match(source, /const normalizedTargetAspect = finiteRatio\(targetAspect, mediaRatio\);/u);
  assert.match(source, /const potentialCoverCrop = cropFraction\(mediaRatio, normalizedTargetAspect\);/u);
  assert.match(source, /if \(potentialCoverCrop > MAX_DOCUMENT_CROP \+ EPSILON\)/u);
  assert.match(source, /cropReason:'document_crop_budget_exceeded'/u);
  assert.match(source, /const limit = Math\.max\(0, Math\.floor\(Number\.isFinite\(requestedLimit\) \? requestedLimit : items\.length\)\);/u);
  assert.match(source, /const rowSize = Math\.max\(1, Math\.min\(6, Math\.floor\(Number\.isFinite\(requestedRowSize\) \? requestedRowSize : 3\)\)\);/u);
  assert.match(source, /return left\.signature\.localeCompare\(right\.signature\);/u);
  assert.match(source, /rowColumn:index/u);
  assert.match(source, /return rows\.flatMap\(\(row, rowIndex\) => materializeRow\(row, rowIndex, presentation\)\);/u);
});
