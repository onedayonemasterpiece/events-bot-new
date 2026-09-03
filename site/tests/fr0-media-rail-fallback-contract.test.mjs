import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const occurrences = (source, token) => source.split(token).length - 1;

test('EventMediaRail fail-closes broken resources into canonical fallback anatomy', async () => {
  const [source, mediaFrame] = await Promise.all([
    read('src/components/EventMediaRail.astro'),
    read('src/components/media-frame.css'),
  ]);

  assert.equal(
    occurrences(source, "data-media-frame-resource-state={item.kind === 'fallback' ? 'fallback' : 'pending'}"),
    2,
    'both direct and deferred rail frames must publish resource state',
  );
  assert.equal(
    occurrences(source, 'class="event-media-rail__fallback" data-media-frame-fallback aria-hidden="true"'),
    2,
    'both rail frame branches must expose a real fallback child',
  );
  assert.equal(occurrences(source, 'onload={mediaFrameImageLoadHandler}'), 2);
  assert.equal(occurrences(source, 'onerror={mediaFrameImageErrorHandler}'), 2);

  for (const invariant of [
    "this.dataset.mediaFrameFailed==='true'",
    "this.hasAttribute('data-rail-thumbnail')&&this.dataset.railThumbnailActivated!=='true'",
    "frame.dataset.mediaFrameResourceState='loaded'",
    "frame.dataset.mediaFrameResourceState='broken'",
    "frame.dataset.mediaFrameKind='fallback'",
    "frame.dataset.mediaFrameFit='contain'",
    "frame.dataset.mediaFrameCropPermission='forbidden'",
    "frame.dataset.mediaFrameCropReason='resource_load_error'",
    "frame.setAttribute('data-media-frame-fallback','')",
  ]) assert.ok(source.includes(invariant), `missing fail-closed resource invariant: ${invariant}`);

  assert.doesNotMatch(source, /mediaFrameResourceState='broken'[\s\S]{0,240}mediaFrameFit='cover'/u,
    'a broken resource must never retain or regain cover');

  const fallbackChildSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="media-rail"] > [data-media-frame-fallback]';
  const brokenImageSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="media-rail"][data-media-frame-resource-state="broken"] > [data-media-frame-image]';
  const brokenFallbackSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="media-rail"][data-media-frame-resource-state="broken"] > [data-media-frame-fallback]';
  assert.ok(mediaFrame.includes(fallbackChildSelector));
  assert.ok(mediaFrame.includes(brokenImageSelector));
  assert.ok(mediaFrame.includes(brokenFallbackSelector));
  assert.match(mediaFrame, /data-media-frame-resource-state="broken"\][^}]+visibility: hidden;/su);
  assert.match(mediaFrame, /data-media-frame-resource-state="broken"\][^}]+display: block;/su);
});
