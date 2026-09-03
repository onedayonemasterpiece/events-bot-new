import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('ListingEventCard keeps a permanent fallback layer for missing and broken media', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /const mediaFrameInitialState = imageUrl \? 'loading' : 'fallback';/u);
  assert.match(source, /data-media-frame-state=\{mediaFrameInitialState\}/u);
  assert.match(source, /data-media-frame-fallback[\s\S]*hidden=\{Boolean\(imageUrl\)\}/u);
  assert.match(source, /\{imageUrl \? \([\s\S]*data-media-frame-image[\s\S]*\) : null\}[\s\S]*data-media-frame-fallback/u,
    'fallback must remain in the DOM even when an image URL exists');
});

test('ListingEventCard distinguishes successful decode from a broken complete image', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /if \(!image\.naturalWidth \|\| !image\.naturalHeight\) \{\s*finish\(false\);/u);
  assert.match(source, /image\.decode\(\)\.then\(\(\) => finish\(true\)\)\.catch\(\(\) => finish\(image\.naturalWidth > 0 && image\.naturalHeight > 0\)\)/u);
  assert.match(source, /image\.addEventListener\('error', \(\) => finish\(false\), \{ once: true \}\)/u);
  assert.doesNotMatch(source, /image\.decode\?\.\(\)\.catch\(\(\) => \{\}\)\.finally\(finish\)/u,
    'decode failure must not be converted into an unconditional loaded state');
});

test('ListingEventCard exposes loaded/fallback state and removes failed network sources', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /mediaFrame\.dataset\.mediaFrameState = loaded \? 'loaded' : 'fallback'/u);
  assert.match(source, /fallback\.hidden = loaded/u);
  assert.match(source, /if \(!loaded && image instanceof HTMLImageElement\) \{[\s\S]*image\.hidden = true;[\s\S]*image\.removeAttribute\('srcset'\);[\s\S]*image\.removeAttribute\('src'\);/u);
  assert.match(source, /if \(!\(image instanceof HTMLImageElement\)\) \{\s*finish\(false\);/u);
});
