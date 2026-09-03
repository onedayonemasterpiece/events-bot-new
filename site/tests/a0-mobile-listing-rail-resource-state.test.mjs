import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), 'utf8');

test('mobile listing rail consumes the canonical FR0 resource-state protocol', async () => {
  const [row, surface, mediaFrame] = await Promise.all([
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/listings/MobileListingRailSurface.astro'),
    read('src/components/media-frame.css'),
  ]);

  assert.match(row, /import '\.\.\/media-frame\.css';/u);
  assert.match(row, /data-media-frame-surface="mobile-listing-rail"/u);
  assert.match(row, /data-media-frame-resource-state="pending"/u);
  assert.match(row, /data-media-frame-source-width=\{sourceWidth \|\| undefined\}/u);
  assert.match(row, /data-media-frame-source-height=\{sourceHeight \|\| undefined\}/u);
  assert.match(row, /data-media-frame-source-ratio=\{sourceRatio \? sourceRatio\.toFixed\(5\) : undefined\}/u);
  assert.match(row, /<span class="media-frame__fallback" data-media-frame-fallback aria-hidden="true" hidden><\/span>/u);
  assert.doesNotMatch(row, /data-media-state=|dataset\.mediaState/u);
  assert.doesNotMatch(surface, /data-media-state=|dataset\.mediaState/u);

  for (const loadedInvariant of [
    "shell.dataset.mediaFrameResourceState = 'loaded'",
    "shell.removeAttribute('data-media-frame-fallback')",
    'restoreResolvedFrame()',
    'img.hidden = false',
    'fallback.hidden = true',
  ]) assert.ok(surface.includes(loadedInvariant), `missing loaded invariant: ${loadedInvariant}`);

  for (const brokenInvariant of [
    "shell.dataset.mediaFrameResourceState = 'broken'",
    "shell.setAttribute('data-media-frame-fallback', '')",
    "shell.dataset.mediaFrameKind = 'fallback'",
    "shell.dataset.mediaFrameFit = 'contain'",
    "shell.dataset.mediaFrameCropPermission = 'forbidden'",
    "shell.dataset.mediaFrameCropReason = 'resource_load_error'",
    "shell.dataset.mediaFrameObjectPosition = '50% 50%'",
    "shell.dataset.mediaFrameFocalPosition = '50% 50%'",
    "img.removeAttribute('src')",
    "img.removeAttribute('srcset')",
    "img.removeAttribute('sizes')",
    'img.hidden = true',
    'fallback.hidden = false',
  ]) assert.ok(surface.includes(brokenInvariant), `missing broken invariant: ${brokenInvariant}`);

  assert.match(surface, /let terminalBroken = shell\.dataset\.mediaFrameResourceState === 'broken'/u);
  assert.match(surface, /if \(terminalBroken && ok\) return/u);
  assert.match(surface, /img\.decode\(\)\.then\([\s\S]*\)\.catch\(\(\) => done\(false\)\)/u);
  assert.doesNotMatch(surface, /catch\(\(\) => done\(img\.naturalWidth > 0\)\)/u);

  assert.match(row, /\['broken', 'fallback'\]\.includes\(item\.dataset\.mediaFrameResourceState/u);
  assert.match(row, /item\.dataset\.mediaFrameResourceState === 'pending'/u);
  assert.match(row, /media\.every\(\(item\) => item\.dataset\.mediaFrameResourceState === 'loaded'\)/u);
  assert.match(row, /'data-media-frame-resource-state'/u);

  assert.match(mediaFrame, /\[data-media-frame\]\[data-media-frame-contract="v1"\][\s\S]*overflow: hidden;/u);
  assert.match(mediaFrame, /data-media-frame-fit="cover"[\s\S]*object-fit: cover;/u);
  assert.match(mediaFrame, /data-media-frame-fit="contain"[\s\S]*object-fit: contain;/u);
  assert.doesNotMatch(
    surface,
    /\.event-media[^{}]*\{[^}]*(?:object-fit|object-position|clip-path)\s*:/iu,
    'A0 mobile rail must not become a competing fit, focal or clip owner',
  );
});

test('mobile rail resource-state migration preserves geometry, gestures and actions', async () => {
  const [row, surface] = await Promise.all([
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/listings/MobileListingRailSurface.astro'),
  ]);

  for (const invariant of [
    '.event-row{height:112px',
    '.rail-window{--dislike-progress:0;--like-progress:0;--rail-max:0px;--rail-view-width:100vw;position:relative;display:block;width:100vw;height:112px',
    '.event-summary{position:relative;isolation:isolate;flex:0 0 296px;width:296px;height:112px',
    'setDislike',
    'setLikePull',
    'finishLike',
    'touchstart',
    'touchmove',
    'pointerdown',
    'pointermove',
    'pointercancel',
    'syncTodayTemporalMedia',
  ]) assert.ok(surface.includes(invariant), `missing preserved surface invariant: ${invariant}`);

  for (const invariant of [
    'data-feedback-action="like"',
    'data-feedback-action="not_interested"',
    'data-gallery-open',
    'event-medallion-slot',
    'event-digest',
    'AmberRailArtifact',
    'loading={loading}',
    "fetchpriority={priority && mediaIndex === 0 ? 'high' : 'auto'}",
  ]) assert.ok(row.includes(invariant), `missing preserved row invariant: ${invariant}`);

  assert.match(surface, /\.event-media>\[data-media-frame-fallback\]\{position:absolute;inset:0;display:block/u);
  assert.match(surface, /\.event-media>\[data-media-frame-fallback\]\[hidden\]\{display:none\}/u);
  assert.match(surface, /\.event-media\.is-loaded\.is-temporally-muted>img\{opacity:\.46;filter:grayscale\(\.72\) saturate\(\.32\)\}/u);
});
