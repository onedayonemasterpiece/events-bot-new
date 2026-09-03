import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const occurrences = (source, token) => source.split(token).length - 1;

test('EventMediaRail exposes one canonical three-variant family and preserves caller media decisions', async () => {
  const source = await read('src/components/EventMediaRail.astro');

  assert.match(source, /export type EventMediaRailVariant = 'gallery-thumbnails' \| 'hero-selector' \| 'poster-strip';/u);
  assert.match(source, /data-ds-family="EventMediaRail"/u);
  assert.match(source, /data-event-media-rail-contract=\{usesResolvedItems \? 'resolved-items-v1' : 'asset-input-v1'\}/u);
  assert.match(source, /data-event-media-rail-source-owner=\{usesResolvedItems \? 'caller' : 'EventMediaRail'\}/u);
  assert.match(source, /item\.fit === 'cover' && kind === 'visual' \? 'cover' : 'contain'/u);
  assert.match(source, /import '\.\/media-frame\.css';/u);

  for (const hook of [
    'data-responsive-rail',
    'data-responsive-rail-item',
    'data-responsive-rail-more',
    'data-responsive-split-rail',
    'data-responsive-split-item',
    'data-responsive-split-more',
    'data-efficient-viewer-start',
    'data-rail-thumbnail',
    'data-thumbnail-src',
  ]) {
    assert.ok(source.includes(hook), `missing production rail hook: ${hook}`);
  }

  assert.ok(occurrences(source, 'data-media-frame-contract="v1"') >= 2,
    'gallery and resolved rail anatomies must both publish MediaFrame v1');
  assert.ok(occurrences(source, 'data-media-frame-style-owner="media-frame.css"') >= 2,
    'every rail anatomy must name the shared structural owner');
  assert.ok(occurrences(source, 'data-media-frame-interaction-owner="caller"') >= 2,
    'MediaFrame must remain non-interactive in both rail anatomies');
  assert.match(source, /data-media-frame-fit=\{item\.fit\}/u);
  assert.match(source, /data-media-frame-object-position=\{item\.objectPosition\}/u);
  assert.match(source, /data-media-frame-crop-reason=\{item\.cropReason\}/u);
  assert.match(source, /attributeFilter:\['data-rail-visible-count', 'data-rail-hidden-count', 'data-rail-complete'\]/u);
  assert.doesNotMatch(source, /\.event-media-rail__frame\s*\{[\s\S]*?overflow:\s*hidden/u);
  assert.doesNotMatch(source, /style=\{`object-fit:/u);
});

test('AdaptiveEventCardGrid owns compatibility, live-region, runtime and consumer metadata contracts', async () => {
  const source = await read('src/components/AdaptiveEventCardGrid.astro');
  const optimized = await read('src/components/OptimizedEventCardGrid.astro');

  for (const prop of ['runtimeManaged?', 'ariaLive?', 'ariaAtomic?', 'ariaBusy?', 'ariaLabel?', 'ariaLabelledby?']) {
    assert.ok(source.includes(prop), `missing adaptive runtime/live-region API: ${prop}`);
  }
  assert.match(source, /const runtimeManagedGrid = runtimeManaged \?\? personalFeed;/u);
  assert.match(source, /aria-live=\{ariaLive\}/u);
  assert.match(source, /aria-atomic=\{ariaAtomic === undefined \? undefined : String\(ariaAtomic\)\}/u);
  assert.match(source, /aria-busy=\{ariaBusy === undefined \? undefined : String\(ariaBusy\)\}/u);
  assert.match(source, /aria-labelledby=\{ariaLabelledby\}/u);
  assert.match(source, /'data-adaptive-grid-live'/u);
  assert.match(source, /'data-adaptive-grid-runtime-managed'/u);
  assert.match(source, /data-adaptive-grid-runtime-managed=\{runtimeManagedGrid \? 'true' : undefined\}/u);
  assert.match(source, /data-adaptive-grid-item-root-contract=\{hasItemRoots/u);
  assert.match(source, /rootClassName=\{itemRootFor\(item\)\?\.className\}/u);
  assert.match(source, /rootAttributes=\{itemRootFor\(item\)\?\.attributes\}/u);
  assert.match(source, /!ADAPTIVE_ROOT_RESERVED_ATTRIBUTES\.has\(name\)/u);
  assert.match(source, /function bindAdaptiveEventCardGridRuntime\(root = document\)/u);
  assert.match(source, /querySelectorAll\('\[data-adaptive-event-card-grid\]\[data-adaptive-grid-runtime-managed="true"\]'\)/u);
  assert.match(source, /new MutationObserver\(sync\)\.observe\(grid, \{ childList:true \}\)/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRemainderCount = String\(count % runtimeRowSize\)/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRenderedOrder = order/u);

  assert.match(optimized, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro';/u);
  assert.match(optimized, /legacyOptimizedContract/u);
  assert.doesNotMatch(optimized, /import EventCard/u);
  assert.doesNotMatch(optimized, /packRelatedCardRows/u);
});

test('EventCard and ListingEventCard use one guarded root bridge and shared MediaFrame style owner', async () => {
  const eventCard = await read('src/components/EventCard.astro');
  const listingCard = await read('src/components/listings/ListingEventCard.astro');
  const mediaFrame = await read('src/components/media-frame.css');

  for (const source of [eventCard, listingCard]) {
    assert.match(source, /rootClassName\?: string/u);
    assert.match(source, /rootAttributes\?:/u);
    assert.match(source, /\.\.\.safeRootAttributes/u);
    assert.match(source, /data-media-frame-contract="v1"/u);
    assert.match(source, /data-media-frame-style-owner="media-frame\.css"/u);
    assert.match(source, /data-media-frame-fit=/u);
    assert.match(source, /data-media-frame-object-position=/u);
    assert.match(source, /data-media-frame-crop-reason=/u);
    assert.match(source, /data-media-frame-interaction-owner="caller"/u);
  }

  assert.match(eventCard, /import '\.\/media-frame\.css';/u);
  assert.match(eventCard, /!EVENT_CARD_RESERVED_ROOT_ATTRIBUTES\.has\(name\)/u);
  assert.match(eventCard, /const cardClass = \['event-card', `event-card--\$\{variant\}`, rootClassName\]/u);
  assert.doesNotMatch(eventCard, /const cardImageStyle/u);
  assert.doesNotMatch(eventCard, /style=\{cardImageStyle\}/u);

  assert.match(listingCard, /import '\.\.\/media-frame\.css';/u);
  assert.match(listingCard, /hidden\?: boolean/u);
  assert.match(listingCard, /hidden=\{hidden\}/u);
  assert.match(listingCard, /!LISTING_EVENT_CARD_RESERVED_ROOT_ATTRIBUTES\.has\(name\)/u);
  assert.match(listingCard, /const cardClass = \['ke-listing-card', rootClassName\]/u);
  assert.doesNotMatch(listingCard, /style="display:block;width:100%;height:100%;overflow:hidden"/u);

  assert.match(mediaFrame, /Canonical MediaFrame v1 structural and fit owner/u);
  assert.match(mediaFrame, /\[data-media-frame\]\[data-media-frame-contract="v1"\]\[data-media-frame-fit="cover"\]/u);
  assert.match(mediaFrame, /object-position:\s*var\(--media-frame-object-position, 50% 50%\)/u);
  assert.match(mediaFrame, /\[data-media-frame-fill="true"\]/u);
});
