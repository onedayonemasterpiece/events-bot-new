import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const DIRECT_FRAMING_DECLARATION = /(?:^|[;{])\s*object-(?:fit|position)\s*:/mu;

function createListingRuntimeHarness(source, { deferredDecode = false, initiallyComplete = true } = {}) {
  class FakeClassList {
    values = new Set(['ke-skeleton']);
    remove(...names) { names.forEach((name) => this.values.delete(name)); }
  }

  class FakeStyle {
    values = new Map();
    setProperty(name, value) { this.values.set(name, String(value)); }
  }

  class FakeHTMLElement {
    dataset = {};
    attributes = new Map();
    classList = new FakeClassList();
    style = new FakeStyle();
    hidden = false;
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    removeAttribute(name) { this.attributes.delete(name); }
    hasAttribute(name) { return this.attributes.has(name); }
  }

  let resolveDecode;
  class FakeHTMLImageElement extends FakeHTMLElement {
    complete = initiallyComplete;
    naturalWidth = 640;
    naturalHeight = 480;
    listeners = new Map();
    constructor() {
      super();
      this.attributes.set('src', '/ok.webp');
      this.attributes.set('srcset', '/ok-320.webp 320w, /ok.webp 640w');
      this.attributes.set('sizes', '320px');
    }
    addEventListener(type, listener, options = {}) {
      const listeners = this.listeners.get(type) || [];
      listeners.push({ listener, once: options?.once === true });
      this.listeners.set(type, listeners);
    }
    dispatch(type) {
      const listeners = [...(this.listeners.get(type) || [])];
      listeners.forEach((entry) => {
        entry.listener.call(this, { type, target: this });
        if (entry.once) {
          const current = this.listeners.get(type) || [];
          this.listeners.set(type, current.filter((candidate) => candidate !== entry));
        }
      });
    }
    listenerCount(type) { return (this.listeners.get(type) || []).length; }
    decode() {
      if (!deferredDecode) return Promise.resolve();
      return new Promise((resolve) => { resolveDecode = resolve; });
    }
  }

  const fallback = new FakeHTMLElement();
  fallback.hidden = true;
  const mediaFrame = new FakeHTMLElement();
  mediaFrame.dataset.mediaFrameResourceState = 'pending';
  mediaFrame.dataset.mediaFrameKind = 'visual';
  mediaFrame.dataset.mediaFrameFit = 'cover';
  mediaFrame.dataset.mediaFrameCropPermission = 'reviewed';
  mediaFrame.dataset.mediaFrameCropReason = 'listing_reviewed_crop:test';
  mediaFrame.querySelector = (selector) => selector === ':scope > [data-media-frame-fallback]' ? fallback : null;

  const image = new FakeHTMLImageElement();
  const frame = new FakeHTMLElement();
  frame.querySelector = (selector) => {
    if (selector === '[data-media-frame]') return mediaFrame;
    if (selector === '[data-media-frame-image]') return image;
    return null;
  };
  const document = { querySelectorAll: () => [frame] };
  const script = source.match(/<script>\s*([\s\S]*?)\s*<\/script>/u)?.[1];
  assert.ok(script, 'ListingEventCard runtime script is missing');
  vm.runInNewContext(script, { document, HTMLElement: FakeHTMLElement, HTMLImageElement: FakeHTMLImageElement });

  return {
    frame,
    mediaFrame,
    image,
    fallback,
    resolveDecode: () => resolveDecode?.(),
  };
}

const nextTurn = () => new Promise((resolve) => setImmediate(resolve));

test('ListingEventCard keeps one binder, one root and the canonical resource-state channel', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.equal((source.match(/<article\b/gu) || []).length, 1, 'ListingEventCard must keep one component root');
  assert.equal((source.match(/function initListingMediaSkeletons\(/gu) || []).length, 1,
    'ListingEventCard must keep one lifecycle binder');
  assert.equal((source.match(/initListingMediaSkeletons\(\);/gu) || []).length, 1,
    'ListingEventCard binder must initialize once');
  assert.match(source, /data-ds-family="ListingEventCard"/u);
  assert.match(source, /const mediaFrameInitialResourceState = imageUrl \? 'pending' : 'fallback';/u);
  assert.match(source, /data-media-frame-resource-state=\{mediaFrameInitialResourceState\}/u);
  assert.match(source, /data-media-frame-fallback=\{imageUrl \? undefined : ''\}/u);
  assert.match(source, /data-media-frame-source-ratio=\{mediaFrameSourceRatio\}/u);
  assert.doesNotMatch(source, /data-media-frame-state|dataset\.mediaFrameState/u);
});

test('ListingEventCard keeps a permanent direct fallback layer', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /\{imageUrl \? \([\s\S]*data-media-frame-image[\s\S]*\) : null\}[\s\S]*class="ke-listing-card__fallback"[\s\S]*data-media-frame-fallback[\s\S]*hidden=\{Boolean\(imageUrl\)\}/u,
    'fallback must remain in the DOM even when an image URL exists');
  assert.match(source, /const fallback = mediaFrame\?\.querySelector\(':scope > \[data-media-frame-fallback\]'\);/u,
    'runtime must address the direct fallback child rather than the root marker');
});

test('ListingEventCard distinguishes fallback, loaded and terminal broken resources', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /let terminalResourceState = '';/u);
  assert.match(source, /const finish = \(resourceState\) =>/u);
  assert.match(source, /if \(terminalResourceState === 'broken'\) return;/u);
  assert.match(source, /const loaded = resourceState === 'loaded';/u);
  assert.match(source, /const broken = resourceState === 'broken';/u);
  assert.match(source, /if \(broken\) terminalResourceState = 'broken';/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameResourceState = resourceState/u);
  assert.match(source, /finish\('fallback'\)/u);
  assert.match(source, /finish\('loaded'\)/u);
  assert.match(source, /finish\('broken'\)/u);
  assert.match(source, /if \(!image\.naturalWidth \|\| !image\.naturalHeight\) \{\s*finish\('broken'\);/u);
  assert.match(source, /catch\(\(\) => finish\(image\.naturalWidth > 0 && image\.naturalHeight > 0 \? 'loaded' : 'broken'\)\)/u);
});

test('loaded ListingEventCard becomes terminal broken after a later resource failure', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');
  const harness = createListingRuntimeHarness(source, { initiallyComplete: false });

  assert.equal(harness.image.listenerCount('load'), 1);
  assert.equal(harness.image.listenerCount('error'), 1);
  harness.image.dispatch('load');
  await nextTurn();

  assert.equal(harness.mediaFrame.dataset.mediaFrameResourceState, 'loaded');
  assert.equal(harness.fallback.hidden, true);
  assert.equal(harness.image.hidden, false);
  assert.equal(harness.image.listenerCount('load'), 0);
  assert.equal(harness.image.listenerCount('error'), 1,
    'post-load error listener must remain durable');

  harness.image.setAttribute('src', '/forced-missing.webp');
  harness.image.setAttribute('srcset', '/forced-missing-320.webp 320w');
  harness.image.setAttribute('sizes', '320px');
  harness.image.dispatch('error');

  assert.equal(harness.mediaFrame.dataset.mediaFrameResourceState, 'broken');
  assert.equal(harness.mediaFrame.dataset.mediaFrameKind, 'fallback');
  assert.equal(harness.mediaFrame.dataset.mediaFrameFit, 'contain');
  assert.equal(harness.mediaFrame.dataset.mediaFrameCropPermission, 'forbidden');
  assert.equal(harness.mediaFrame.dataset.mediaFrameCropReason, 'resource_load_error');
  assert.equal(harness.mediaFrame.dataset.mediaFrameObjectPosition, '50% 50%');
  assert.equal(harness.mediaFrame.dataset.mediaFrameFocalPosition, '50% 50%');
  assert.equal(harness.mediaFrame.style.values.get('--media-frame-object-position'), '50% 50%');
  assert.equal(harness.mediaFrame.hasAttribute('data-media-frame-fallback'), true);
  assert.equal(harness.fallback.hidden, false);
  assert.equal(harness.image.hidden, true);
  assert.equal(harness.image.hasAttribute('src'), false);
  assert.equal(harness.image.hasAttribute('srcset'), false);
  assert.equal(harness.image.hasAttribute('sizes'), false);

  harness.image.naturalWidth = 640;
  harness.image.naturalHeight = 480;
  harness.image.dispatch('load');
  await nextTurn();
  assert.equal(harness.mediaFrame.dataset.mediaFrameResourceState, 'broken',
    'a late load event must not restore loaded');
  assert.equal(harness.mediaFrame.dataset.mediaFrameFit, 'contain',
    'a late load event must not restore cover');
  assert.equal(harness.fallback.hidden, false);
  assert.equal(harness.image.hidden, true);
});

test('a late decode callback cannot restore loaded after ListingEventCard broke', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');
  const harness = createListingRuntimeHarness(source, { deferredDecode: true, initiallyComplete: false });

  harness.image.dispatch('load');
  harness.image.dispatch('error');
  assert.equal(harness.mediaFrame.dataset.mediaFrameResourceState, 'broken');
  harness.resolveDecode();
  await nextTurn();
  assert.equal(harness.mediaFrame.dataset.mediaFrameResourceState, 'broken');
  assert.equal(harness.mediaFrame.dataset.mediaFrameFit, 'contain');
  assert.equal(harness.fallback.hidden, false);
  assert.equal(harness.image.hidden, true);
});

test('ListingEventCard installs a durable error listener before the complete fast path', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');
  const listenerAt = source.indexOf("image.addEventListener('error', onImageError)");
  const completeAt = source.indexOf('if (image.complete) decodeAndFinish()');

  assert.match(source, /const onImageError = \(\) => finish\('broken'\);/u);
  assert.ok(listenerAt >= 0, 'ListingEventCard must install a persistent image error listener');
  assert.ok(completeAt > listenerAt,
    'error listener must be installed before the image.complete fast path');
  assert.doesNotMatch(
    source,
    /image\.addEventListener\('error',[\s\S]{0,120}\{ once: true \}\)/u,
    'a once-only conditional listener misses source failures after initial load',
  );
  assert.match(source, /image\.addEventListener\('load', decodeAndFinish, \{ once: true \}\)/u);
});

test('ListingEventCard broken resources fail closed and cannot retain responsive sources or cover', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /mediaFrame\.dataset\.mediaFrameKind = 'fallback'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameFit = 'contain'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameCropPermission = 'forbidden'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameCropReason = broken \? 'resource_load_error' : 'listing_fallback'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameObjectPosition = '50% 50%'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameFocalPosition = '50% 50%'/u);
  assert.match(source, /mediaFrame\.setAttribute\('data-media-frame-fallback', ''\)/u);
  assert.match(source, /if \(fallback instanceof HTMLElement\) fallback\.hidden = loaded;/u);
  assert.match(
    source,
    /if \(broken && image instanceof HTMLImageElement\) \{[\s\S]*image\.hidden = true;[\s\S]*image\.removeAttribute\('sizes'\);[\s\S]*image\.removeAttribute\('srcset'\);[\s\S]*image\.removeAttribute\('src'\);/u,
  );
  assert.doesNotMatch(source, DIRECT_FRAMING_DECLARATION,
    'ListingEventCard must not reimplement object-fit/object-position declarations');
});

test('ListingEventCard preserves density, derivatives, links, proof, identity, metadata and anatomy', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  for (const marker of [
    "density?: 'regular' | 'weekend' | 'popular'",
    'data-listing-density={density}',
    'srcset={responsiveSrcset}',
    'sizes={responsiveSizes}',
    'class="ke-listing-card__media ke-skeleton"',
    'href={href}',
    'data-listing-proof-placement=',
    'class="ke-listing-card__identity-rail"',
    'data-listing-temporal-status',
    'class="ke-listing-card__title"',
    '{place ? <p>{place}</p> : null}',
    'LISTING_EVENT_CARD_RESERVED_ROOT_ATTRIBUTES',
  ]) assert.ok(source.includes(marker), `ListingEventCard lost preserved contract marker: ${marker}`);

  assert.match(source, /srcset=\{responsiveSrcset\}/u);
  assert.match(source, /sizes=\{responsiveSizes\}/u);
  assert.match(source, /const hasSocialProof = likesCount > 0 \|\| sharesCount > 0;/u);
  assert.match(source, /const hasRailProof = hasSocialProof && !proofInside;/u);
  assert.match(source, /const hasSideRail = visibleIdentityCount > 0 \|\| hasRailProof;/u);
  assert.match(source, /const tailWidth = splitIdentityProofRail \? 96 : hasSideRail \? \(visibleIdentityCount === 0 \? 40 : 64\) : 0;/u);
  assert.match(source, /data-listing-proof-placement=\{hasSocialProof \? \(proofInside \? 'inside' : 'rail'\) : 'none'\}/u);
});
