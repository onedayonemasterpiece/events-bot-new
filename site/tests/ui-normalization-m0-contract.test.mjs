import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const occurrences = (source, token) => source.split(token).length - 1;

const MEDIA_FRAME_FIELDS = [
  'data-media-frame-style-owner="media-frame.css"',
  'data-media-frame-role=',
  'data-media-frame-kind=',
  'data-media-frame-fit=',
  'data-media-frame-crop-permission=',
  'data-media-frame-ratio=',
  'data-media-frame-object-position=',
  'data-media-frame-focal-position=',
  'data-media-frame-crop-reason=',
  'data-media-frame-clip="frame"',
  'data-media-frame-radius="surface"',
  'data-media-frame-loading=',
  'data-media-frame-interaction-owner="caller"',
];

test('EventMediaRail exposes one canonical three-variant family and preserves caller media decisions', async () => {
  const source = await read('src/components/EventMediaRail.astro');

  assert.match(source, /import '\.\/media-frame\.css';/u);
  assert.match(source, /export type EventMediaRailVariant = 'gallery-thumbnails' \| 'hero-selector' \| 'poster-strip';/u);
  assert.match(source, /data-ds-family="EventMediaRail"/u);
  assert.match(source, /data-event-media-rail-contract=\{usesResolvedItems \? 'resolved-items-v1' : 'asset-input-v1'\}/u);
  assert.match(source, /data-event-media-rail-source-owner=\{usesResolvedItems \? 'caller' : 'EventMediaRail'\}/u);
  assert.match(source, /item\.fit === 'cover' && kind === 'visual' \? 'cover' : 'contain'/u);
  assert.match(source, /const mediaFrameCropPermission = \(fit: EventMediaRailFrameFit\) => fit === 'cover' \? 'allowed' : 'forbidden';/u);

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

  for (const field of MEDIA_FRAME_FIELDS) {
    assert.ok(source.includes(field), `missing rail MediaFrame field: ${field}`);
  }
  assert.ok(occurrences(source, 'data-media-frame-contract="v1"') >= 2,
    'gallery and resolved rail anatomies must both publish MediaFrame v1');
  assert.ok(occurrences(source, 'data-media-frame-style-owner="media-frame.css"') >= 2,
    'every rail anatomy must name the canonical CSS owner');
  assert.match(source, /data-media-frame-fill="true"/u);
  assert.match(source, /data-media-frame-source-ratio=\{sourceRatio\?\.toFixed\(5\)\}/u);
  assert.match(source, /attributeFilter:\['data-rail-visible-count', 'data-rail-hidden-count', 'data-rail-complete'\]/u);
  assert.doesNotMatch(source, /style=\{`object-fit:/u);
  assert.doesNotMatch(source, /\.event-media-rail__frame\s*\{/u);
});

test('AdaptiveEventCardGrid owns compatibility, live-region, filtered runtime and named remainder contracts', async () => {
  const source = await read('src/components/AdaptiveEventCardGrid.astro');
  const optimized = await read('src/components/OptimizedEventCardGrid.astro');

  for (const prop of [
    'runtimeManaged?',
    'runtimeVisibleOnly?',
    'runtimeSourcePolicy?',
    'ariaLive?',
    'ariaAtomic?',
    'ariaBusy?',
    'ariaLabel?',
    'ariaLabelledby?',
  ]) {
    assert.ok(source.includes(prop), `missing adaptive runtime/live-region API: ${prop}`);
  }
  assert.ok(source.includes("type AdaptiveGridRemainderVariant = 'complete' | `stretch-${number}-of-${number}`;"));
  assert.match(source, /const remainderVariantFor = \(count: number, size: number\): AdaptiveGridRemainderVariant/u);
  assert.match(source, /const initialRemainderVariant = remainderVariantFor\(resolved\.length, rowSize\);/u);
  assert.match(source, /const gridState = \[gridBaseState, `remainder-\$\{initialRemainderVariant\}`, initialRuntimeState\]/u);
  assert.match(source, /type AdaptiveGridRuntimeSourcePolicy = 'mirror-rendered' \| 'all-direct' \| 'initial';/u);
  assert.match(source, /const runtimeManagedGrid = runtimeManaged \?\? \(personalFeed \|\| discoveryFeed\);/u);
  assert.match(source, /const runtimeVisibleOnly = requestedRuntimeVisibleOnly \?\? discoveryFeed;/u);
  assert.match(source, /requestedRuntimeSourcePolicy === 'all-direct'/u);
  assert.match(source, /discoveryFeed \? 'all-direct' : 'mirror-rendered'/u);
  assert.match(source, /aria-live=\{ariaLive\}/u);
  assert.match(source, /aria-atomic=\{ariaAtomic === undefined \? undefined : String\(ariaAtomic\)\}/u);
  assert.match(source, /aria-busy=\{ariaBusy === undefined \? undefined : String\(ariaBusy\)\}/u);
  assert.match(source, /aria-labelledby=\{ariaLabelledby\}/u);
  assert.match(source, /'data-adaptive-grid-remainder-variant'/u);
  assert.match(source, /data-adaptive-grid-remainder-variant=\{initialRemainderVariant\}/u);
  assert.match(source, /data-adaptive-grid-runtime-managed=\{runtimeManagedGrid \? 'true' : undefined\}/u);
  assert.match(source, /data-adaptive-grid-runtime-visible-only=\{runtimeManagedGrid && runtimeVisibleOnly \? 'true' : undefined\}/u);
  assert.match(source, /data-adaptive-grid-runtime-source-policy=\{runtimeManagedGrid \? runtimeSourcePolicy : undefined\}/u);
  assert.match(source, /data-adaptive-grid-item-root-contract=\{hasItemRoots/u);
  assert.match(source, /rootClassName=\{itemRootFor\(item\)\?\.className\}/u);
  assert.match(source, /rootAttributes=\{itemRootFor\(item\)\?\.attributes\}/u);
  assert.match(source, /!ADAPTIVE_ROOT_RESERVED_ATTRIBUTES\.has\(name\)/u);
  assert.match(source, /function bindAdaptiveEventCardGridRuntime\(root = document\)/u);
  assert.match(source, /const cards = visibleOnly \? directCards\.filter\(\(card\) => !card\.hidden\) : directCards;/u);
  assert.match(source, /else if \(sourcePolicy === 'all-direct'\) \{/u);
  assert.match(source, /grid\.dataset\.adaptiveGridSourceCount = String\(directCards\.length\)/u);
  assert.match(source, /const runtimeRemainderVariant = runtimeRemainderCount === 0/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRemainderCount = String\(runtimeRemainderCount\)/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRemainderVariant = runtimeRemainderVariant/u);
  assert.match(source, /`remainder-\$\{runtimeRemainderVariant\}`/u);
  assert.match(source, /\{ childList:true, subtree:true, attributes:true, attributeFilter:\['hidden'\] \}/u);

  assert.match(optimized, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro';/u);
  assert.match(optimized, /legacyOptimizedContract/u);
  assert.doesNotMatch(optimized, /import EventCard/u);
  assert.doesNotMatch(optimized, /packRelatedCardRows/u);
});

test('EventCard and ListingEventCard use one MediaFrame protocol and one structural CSS owner', async () => {
  const eventCard = await read('src/components/EventCard.astro');
  const listingCard = await read('src/components/listings/ListingEventCard.astro');
  const mediaFrame = await read('src/components/media-frame.css');

  for (const source of [eventCard, listingCard]) {
    assert.match(source, /rootClassName\?: string/u);
    assert.match(source, /rootAttributes\?:/u);
    assert.match(source, /\.\.\.safeRootAttributes/u);
    assert.match(source, /data-media-frame-contract="v1"/u);
    for (const field of MEDIA_FRAME_FIELDS) {
      assert.ok(source.includes(field), `missing card MediaFrame field: ${field}`);
    }
  }

  assert.match(eventCard, /import '\.\/media-frame\.css';/u);
  assert.match(eventCard, /const mediaFrameCropPermission = mediaFrameFit === 'contain'/u);
  assert.match(eventCard, /!EVENT_CARD_RESERVED_ROOT_ATTRIBUTES\.has\(name\)/u);
  assert.match(eventCard, /const cardClass = \['event-card', `event-card--\$\{variant\}`, rootClassName\]/u);
  assert.doesNotMatch(eventCard, /const cardImageStyle/u);
  assert.doesNotMatch(eventCard, /style=\{cardImageStyle\}/u);

  assert.match(listingCard, /import '\.\.\/media-frame\.css';/u);
  assert.match(listingCard, /const mediaFrameCropPermission = mediaFrameFit === 'cover'/u);
  assert.match(listingCard, /hidden\?: boolean/u);
  assert.match(listingCard, /hidden=\{hidden\}/u);
  assert.match(listingCard, /!LISTING_EVENT_CARD_RESERVED_ROOT_ATTRIBUTES\.has\(name\)/u);
  assert.match(listingCard, /const cardClass = \['ke-listing-card', rootClassName\]/u);
  assert.doesNotMatch(listingCard, /style="display:block;width:100%;height:100%;overflow:hidden"/u);

  assert.match(mediaFrame, /Canonical MediaFrame v1 structural and fit owner/u);
  assert.match(mediaFrame, /\[data-media-frame\]\[data-media-frame-contract="v1"\]/u);
  assert.match(mediaFrame, /\[data-media-frame-fill="true"\]/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="cover"\]/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="contain"\]/u);
  assert.match(mediaFrame, /object-position: var\(--media-frame-object-position, 50% 50%\)/u);
});

test('current DesktopEventPage rail consumers are either migrated or fully covered by EventMediaRail API', async () => {
  const rail = await read('src/components/EventMediaRail.astro');
  const desktop = await read('src/components/DesktopEventPage.astro');
  const requiredItemFields = [
    'src:',
    'thumbnailSrc:',
    'thumbnailSrcset?',
    'thumbnailWidth?',
    'thumbnailHeight?',
    'width?',
    'height?',
    'alt?',
    'galleryIndex:',
    'sourceIndex?',
    'imageTextMode:',
    'mediaRole:',
    'kind:',
    'fit:',
    'objectPosition?',
    'cropReason:',
    'roleLabel?',
    'actionLabel:',
    'rotationEligible?',
    'railAspect?',
    'railRatio?',
    'slotWidth?',
  ];
  for (const field of requiredItemFields) assert.ok(rail.includes(field), `rail item API gap for current consumer: ${field}`);

  const requiredHooks = [
    'data-clean-hero-thumb',
    'data-responsive-rail-item',
    'data-responsive-rail-more',
    'data-responsive-split-item',
    'data-responsive-split-more',
    'data-rotation-eligible',
    'data-efficient-viewer-open',
    'data-efficient-viewer-start',
    'data-hero-gallery-open',
    'data-hero-gallery-index',
    'data-rail-thumbnail',
    'data-thumbnail-src',
    'data-thumbnail-srcset',
    'data-image-text-mode',
    'data-media-role',
    'data-source-index',
    'data-rail-aspect',
  ];
  for (const hook of requiredHooks) assert.ok(rail.includes(hook), `rail API gap for current consumer: ${hook}`);

  for (const preservedClass of [
    'desktop-prototype__media-rail',
    'desktop-prototype__media-rail--hero',
    'desktop-prototype__media-rail--poster',
    'desktop-prototype__media-rail-more',
  ]) assert.ok(rail.includes(preservedClass), `rail migration must preserve runtime/CSS class: ${preservedClass}`);

  assert.match(rail, /variant === 'hero-selector' && index === 0 && 'is-current'/u);
  assert.match(rail, /aria-pressed=\{variant === 'hero-selector' \? \(index === 0 \? 'true' : 'false'\) : undefined\}/u);
  assert.match(rail, /data-src=\{variant === 'hero-selector' \? item\.src : undefined\}/u);
  assert.match(rail, /data-crop-fit=\{variant === 'hero-selector' \? item\.fit : undefined\}/u);
  assert.match(rail, /data-crop-reason=\{variant === 'hero-selector' \? item\.cropReason : undefined\}/u);
  assert.match(rail, /data-efficient-viewer-start=\{variant === 'poster-strip' && splitPortraitViewer \? viewerStart : undefined\}/u);
  assert.match(rail, /data-hero-gallery-open=\{variant === 'poster-strip' && splitPortraitViewer \? undefined : galleryId\}/u);
  assert.match(rail, /data-thumbnail-src=\{item\.thumbnailSrc\}/u);
  assert.match(rail, /data-thumbnail-srcset=\{item\.thumbnailSrcset\}/u);

  const migrated = /import EventMediaRail(?:,| from)/u.test(desktop) && /<EventMediaRail/u.test(desktop);
  if (!migrated) {
    assert.match(desktop, /desktop-prototype__media-rail--hero/u);
    assert.match(desktop, /desktop-prototype__media-rail--poster/u);
    for (const hook of requiredHooks) assert.ok(desktop.includes(hook), `unexpected pre-migration consumer drift: ${hook}`);
  }
});

test('M0 source and regression surfaces match contract v1.9.0 assignments', async () => {
  const files = [
    'src/lib/relatedCardLayout.mjs',
    'src/components/OptimizedEventCardGrid.astro',
    'src/components/AdaptiveEventCardGrid.astro',
    'src/components/EventCard.astro',
    'src/components/listings/ListingEventCard.astro',
    'src/components/listings/MobileListingRailRow.astro',
    'src/components/EventMediaRail.astro',
    'src/components/media-frame.css',
    'scripts/check-preview.mjs',
  ];
  await Promise.all(files.map(read));
});
