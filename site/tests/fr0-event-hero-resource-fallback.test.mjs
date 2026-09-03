import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const occurrences = (source, token) => source.split(token).length - 1;

test('EventHero primary MediaFrame fail-closes a broken network resource', async () => {
  const [source, mediaFrame] = await Promise.all([
    read('src/components/EventHero.astro'),
    read('src/components/media-frame.css'),
  ]);

  assert.equal(occurrences(source, 'data-media-frame-resource-state="pending"'), 1);
  assert.equal(
    occurrences(source, 'class="event-hero__media-fallback" data-media-frame-fallback aria-hidden="true"'),
    1,
    'the primary frame must expose one direct, non-interactive fallback child',
  );
  assert.equal(occurrences(source, 'onload={primaryMediaFrameImageLoadHandler}'), 1);
  assert.equal(occurrences(source, 'onerror={primaryMediaFrameImageErrorHandler}'), 1);
  assert.match(
    source,
    /class="event-hero__media-frame"[\s\S]*?data-media-frame-image[\s\S]*?class="event-hero__media-fallback" data-media-frame-fallback/u,
    'the fallback anatomy must remain inside the accepted primary EventHero MediaFrame adapter',
  );

  for (const invariant of [
    "this.dataset.mediaFrameFailed==='true'",
    "frame.dataset.mediaFrameResourceState='loaded'",
    "frame.dataset.mediaFrameResourceState='broken'",
    "frame.dataset.mediaFrameKind='fallback'",
    "frame.dataset.mediaFrameFit='contain'",
    "frame.dataset.mediaFrameCropPermission='forbidden'",
    "frame.dataset.mediaFrameCropReason='resource_load_error'",
    "frame.setAttribute('data-media-frame-fallback','')",
  ]) assert.ok(source.includes(invariant), `missing EventHero resource invariant: ${invariant}`);

  assert.doesNotMatch(
    source,
    /mediaFrameResourceState='broken'[\s\S]{0,240}mediaFrameFit='cover'/u,
    'a broken EventHero resource must never retain or regain cover',
  );

  const fallbackChildSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="event-hero"] > [data-media-frame-fallback]';
  const brokenImageSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="event-hero"][data-media-frame-resource-state="broken"] > [data-media-frame-image]';
  const brokenFallbackSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="event-hero"][data-media-frame-resource-state="broken"] > [data-media-frame-fallback]';
  assert.ok(mediaFrame.includes(fallbackChildSelector));
  assert.ok(mediaFrame.includes(brokenImageSelector));
  assert.ok(mediaFrame.includes(brokenFallbackSelector));
  assert.match(mediaFrame, /data-media-frame-surface="event-hero"\]\[data-media-frame-resource-state="broken"\][^}]+visibility: hidden;/su);
  assert.match(mediaFrame, /data-media-frame-surface="event-hero"\]\[data-media-frame-resource-state="broken"\][^}]+display: block;/su);
});
