import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const DIRECT_FRAMING_DECLARATION = /(?:^|[;{])\s*object-(?:fit|position)\s*:/mu;

test('ListingEventCard keeps one root and the canonical resource-state channel', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.equal((source.match(/<article\b/gu) || []).length, 1, 'ListingEventCard must keep one component root');
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

test('ListingEventCard distinguishes fallback, loaded and broken resources', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /const finish = \(resourceState\) =>/u);
  assert.match(source, /const loaded = resourceState === 'loaded';/u);
  assert.match(source, /const broken = resourceState === 'broken';/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameResourceState = resourceState/u);
  assert.match(source, /finish\('fallback'\)/u);
  assert.match(source, /finish\('loaded'\)/u);
  assert.match(source, /finish\('broken'\)/u);
  assert.match(source, /if \(!image\.naturalWidth \|\| !image\.naturalHeight\) \{\s*finish\('broken'\);/u);
  assert.match(source, /image\.addEventListener\('error', \(\) => finish\('broken'\), \{ once: true \}\)/u);
  assert.match(source, /catch\(\(\) => finish\(image\.naturalWidth > 0 && image\.naturalHeight > 0 \? 'loaded' : 'broken'\)\)/u);
});

test('ListingEventCard broken resources fail closed and cannot retain cover', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /mediaFrame\.dataset\.mediaFrameKind = 'fallback'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameFit = 'contain'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameCropPermission = 'forbidden'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameCropReason = broken \? 'resource_load_error' : 'listing_fallback'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameObjectPosition = '50% 50%'/u);
  assert.match(source, /mediaFrame\.dataset\.mediaFrameFocalPosition = '50% 50%'/u);
  assert.match(source, /mediaFrame\.setAttribute\('data-media-frame-fallback', ''\)/u);
  assert.match(source, /if \(broken && image instanceof HTMLImageElement\) \{[\s\S]*image\.hidden = true;[\s\S]*image\.removeAttribute\('srcset'\);[\s\S]*image\.removeAttribute\('src'\);/u);
  assert.doesNotMatch(source, DIRECT_FRAMING_DECLARATION,
    'ListingEventCard must not reimplement object-fit/object-position declarations');
});

test('ListingEventCard preserves responsive resources and no-proof spacing', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /srcset=\{responsiveSrcset\}/u);
  assert.match(source, /sizes=\{responsiveSizes\}/u);
  assert.match(source, /const hasSocialProof = likesCount > 0 \|\| sharesCount > 0;/u);
  assert.match(source, /const hasRailProof = hasSocialProof && !proofInside;/u);
  assert.match(source, /const hasSideRail = visibleIdentityCount > 0 \|\| hasRailProof;/u);
  assert.match(source, /const tailWidth = splitIdentityProofRail \? 96 : hasSideRail \? \(visibleIdentityCount === 0 \? 40 : 64\) : 0;/u);
  assert.match(source, /data-listing-proof-placement=\{hasSocialProof \? \(proofInside \? 'inside' : 'rail'\) : 'none'\}/u);
});
