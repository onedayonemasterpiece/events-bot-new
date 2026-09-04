import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

const templateHandler = (source, name) => {
  const marker = `const ${name} = \``;
  const start = source.indexOf(marker);
  const end = source.indexOf('`;', start + marker.length);
  assert.ok(start >= 0, `missing handler ${name}`);
  assert.ok(end > start, `unterminated handler ${name}`);
  return source.slice(start + marker.length, end);
};

const block = (source, startMarker, endMarker) => {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(start >= 0, `missing block ${startMarker}`);
  assert.ok(end > start, `unterminated block ${startMarker}`);
  return source.slice(start, end);
};

test('EventHero and EventMediaRail clear failed URL candidates before publishing terminal fallback', async () => {
  const [hero, rail] = await Promise.all([
    read('src/components/EventHero.astro'),
    read('src/components/EventMediaRail.astro'),
  ]);
  const heroError = templateHandler(hero, 'primaryMediaFrameImageErrorHandler');
  const railError = templateHandler(rail, 'mediaFrameImageErrorHandler');
  const heroLoad = templateHandler(hero, 'primaryMediaFrameImageLoadHandler');
  const railLoad = templateHandler(rail, 'mediaFrameImageLoadHandler');

  for (const [label, handler] of [['EventHero', heroError], ['EventMediaRail', railError]]) {
    const cleanup = "for(const attribute of ['srcset','sizes','src'])this.removeAttribute(attribute)";
    assert.ok(handler.includes(cleanup), `${label} must remove failed responsive resources`);
    assert.ok(handler.indexOf(cleanup) < handler.indexOf("frame.dataset.mediaFrameResourceState='broken'"),
      `${label} cleanup must precede terminal broken publication`);
    for (const invariant of [
      "frame.dataset.mediaFrameKind='fallback'",
      "frame.dataset.mediaFrameFit='contain'",
      "frame.dataset.mediaFrameCropPermission='forbidden'",
      "frame.dataset.mediaFrameCropReason='resource_load_error'",
      "frame.dataset.mediaFrameObjectPosition='50% 50%'",
      "frame.dataset.mediaFrameFocalPosition='50% 50%'",
      "frame.style.setProperty('--media-frame-object-position','50% 50%')",
      "frame.setAttribute('data-media-frame-fallback','')",
    ]) assert.ok(handler.includes(invariant), `${label} missing ${invariant}`);
    assert.doesNotMatch(handler, /\.style\.(?:objectFit|objectPosition)\s*=/u,
      `${label} error handling must not become a second paint owner`);
  }

  assert.ok(heroLoad.includes("this.dataset.mediaFrameFailed==='true'"));
  assert.ok(railLoad.includes("this.dataset.mediaFrameFailed==='true'"));
  assert.ok(railError.includes("this.removeAttribute('data-thumbnail-src')"));
  assert.ok(railError.includes("this.removeAttribute('data-thumbnail-srcset')"));
});

test('exhibitions medallion reuses the durable lifecycle binder and clears picture candidates', async () => {
  const [bridge, row] = await Promise.all([
    read('src/components/exhibitionsMediaFrameBridge.mjs'),
    read('src/components/ExhibitionPrototypeRow.astro'),
  ]);
  const cleanup = block(bridge, 'const clearFailedImageResource =', 'const bindImageResourceLifecycle =');
  const medallions = block(bridge, 'const bindMedallions =', 'const galleryState = new WeakMap();');

  for (const invariant of [
    "image.closest('picture')?.querySelectorAll('source')",
    "source.removeAttribute('srcset')",
    "source.removeAttribute('sizes')",
    "image.removeAttribute('srcset')",
    "image.removeAttribute('sizes')",
    "image.removeAttribute('src')",
  ]) assert.ok(cleanup.includes(invariant), `picture cleanup missing ${invariant}`);

  for (const invariant of [
    'bindImageResourceLifecycle(seal, image, seal)',
    'image.naturalWidth > 0 && image.naturalHeight > 0',
    "seal.dataset.imageState = loaded ? 'loaded' : 'error'",
    'if (!loaded) clearFailedImageResource(image)',
    "publishResourceState(seal, loaded ? 'loaded' : 'error', 'resource_load_error')",
    "publishResourceState(seal, 'loading', 'resource_load_error')",
  ]) assert.ok(medallions.includes(invariant), `medallion lifecycle missing ${invariant}`);
  assert.doesNotMatch(medallions, /image\.addEventListener\(/u,
    'medallion must not keep a second bespoke listener implementation');
  assert.match(row, /data-exhibition-medallion[\s\S]*?<span class="ex-deck__medallion-fallback" data-media-frame-fallback[\s\S]*?<picture>/u,
    'medallion fallback must remain a direct child before picture resources');
});
