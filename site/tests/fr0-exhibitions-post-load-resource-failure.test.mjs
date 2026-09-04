import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const source = await readFile(new URL('../src/components/exhibitionsMediaFrameBridge.mjs', import.meta.url), 'utf8');

function block(startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(start >= 0, `missing block start: ${startMarker}`);
  assert.ok(end > start, `missing block end: ${endMarker}`);
  return source.slice(start, end);
}

test('exhibitions bridge keeps durable post-load resource listeners in the canonical FR0 owner', () => {
  const lifecycle = block('const bindImageResourceLifecycle =', 'const deckManifestFor =');

  for (const invariant of [
    "image.dataset.mediaFrameResourceLifecycleBound === 'true'",
    "image.dataset.mediaFrameResourceLifecycleBound = 'true'",
    "image.addEventListener('load', () => settle('loaded'))",
    "image.addEventListener('error', () => settle('error'))",
    "frame.dataset.mediaFrameResourceState === 'broken'",
    "publishResourceState(frame, loaded ? 'loaded' : 'error', 'resource_load_error')",
    'if (!loaded) clearFailedImageResource(image)',
  ]) assert.ok(lifecycle.includes(invariant), `missing durable lifecycle invariant: ${invariant}`);

  assert.doesNotMatch(lifecycle, /once\s*:\s*true/u,
    'post-load failure listeners must survive the first successful resource');
  assert.doesNotMatch(source, /image\.style\.(?:objectFit|objectPosition)\s*=/u,
    'the bridge must not become a second fit/focal paint owner');
});

test('deck failure becomes broken and removes the failed resource without waiting for caller rebinding', () => {
  const cleanup = block('const clearFailedImageResource =', 'const bindImageResourceLifecycle =');
  for (const attribute of ['srcset', 'sizes', 'src']) {
    assert.ok(cleanup.includes(`image.removeAttribute('${attribute}')`), `cleanup misses ${attribute}`);
  }

  const deck = block('const syncDeckFrame =', 'const bindDeckFrames =');
  assert.ok(deck.includes("bindImageResourceLifecycle(frame, image, frame, () => frame.dataset.deckVisual === 'media')"));
  assert.ok(deck.includes("if (state === 'error') clearFailedImageResource(image)"));
  assert.ok(deck.includes("publishResourceState(frame, state, visual === 'depth-tail' ? 'deck_depth_tail' : undefined)"));
});

test('gallery failure cannot have srcset or sizes republished after the error transition', () => {
  const apply = block('const applyGalleryResource =', 'const bindGallery =');
  const errorGate = apply.indexOf("if (resourceState === 'error')");
  const responsiveWrite = apply.indexOf("image.setAttribute('srcset', srcset)");

  assert.ok(errorGate >= 0, 'gallery resource application misses the error gate');
  assert.ok(responsiveWrite > errorGate, 'gallery error must be handled before responsive attributes are written');
  assert.ok(apply.includes('clearFailedImageResource(image)'));
  assert.ok(apply.includes("publishResourceState(media, 'error', 'resource_load_error')"));
  assert.match(apply, /publishResourceState\(media, 'error', 'resource_load_error'\);\s*return;/u);

  const gallery = block('const bindGallery =', 'export function hydrateExhibitionsMediaFrames');
  assert.ok(gallery.includes('bindImageResourceLifecycle(media, image, media)'));
  assert.ok(gallery.includes("attributeFilter: ['data-image-state']"));
  assert.ok(gallery.includes("attributeFilter: ['src']"));
});

test('canonical broken state remains terminal until a caller explicitly publishes a new pending resource', () => {
  const lifecycle = block('const bindImageResourceLifecycle =', 'const deckManifestFor =');
  assert.match(lifecycle, /if \(loaded && frame\.dataset\.mediaFrameResourceState === 'broken'\) return;/u);

  const resourceState = block('const publishResourceState =', 'const clearFailedImageResource =');
  for (const invariant of [
    "frame.dataset.mediaFrameResourceState = normalized",
    "frame.dataset.mediaFrameKind = 'fallback'",
    "frame.dataset.mediaFrameFit = 'contain'",
    "frame.dataset.mediaFrameCropPermission = 'forbidden'",
    "frame.dataset.mediaFrameObjectPosition = '50% 50%'",
    "frame.dataset.mediaFrameFocalPosition = '50% 50%'",
    "frame.setAttribute('data-media-frame-fallback', '')",
  ]) assert.ok(resourceState.includes(invariant), `broken state misses ${invariant}`);
});
