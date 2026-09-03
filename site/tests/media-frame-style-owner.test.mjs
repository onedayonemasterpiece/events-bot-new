import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const blockAfter = (source, selector) => {
  const start = source.indexOf(`${selector} {`);
  assert.notEqual(start, -1, `missing CSS selector: ${selector}`);
  const bodyStart = source.indexOf('{', start) + 1;
  const end = source.indexOf('\n}', bodyStart);
  assert.notEqual(end, -1, `unterminated CSS selector: ${selector}`);
  return source.slice(bodyStart, end);
};

test('MediaFrame owns inner anatomy without overriding surface inline sizing', async () => {
  const [mediaFrame, mobileRow, mobileSurface] = await Promise.all([
    read('src/components/media-frame.css'),
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/listings/MobileListingRailSurface.astro'),
  ]);

  const rootBlock = blockAfter(mediaFrame, '[data-media-frame][data-media-frame-contract="v1"]');
  assert.match(rootBlock, /position: relative/u);
  assert.match(rootBlock, /isolation: isolate/u);
  assert.match(rootBlock, /display: block/u);
  assert.match(rootBlock, /min-width: 0/u);
  assert.match(rootBlock, /overflow: hidden/u);
  assert.doesNotMatch(rootBlock, /(?:^|\n)\s*(?:width|max-width|inline-size)\s*:/u,
    'frame root must not override a rail/card surface width');

  assert.match(mediaFrame, /\[data-media-frame-fill="true"\] > \[data-media-frame-image\][\s\S]*width: 100%;[\s\S]*height: 100%/u);
  assert.match(mediaFrame, /object-position: var\(--media-frame-object-position, 50% 50%\)/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="cover"\] > \[data-media-frame-image\][\s\S]*object-fit: cover/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="contain"\] > \[data-media-frame-image\][\s\S]*object-fit: contain/u);

  assert.match(mobileSurface, /\.ke-mobile-listing-rails--v23 \.event-media\{[^}]*flex:0 0 var\(--media-width\);width:var\(--media-width\);height:112px/u);
  assert.match(mobileRow, /import '\.\.\/media-frame\.css';/u);
  assert.match(mobileRow, /--media-width:\$\{railMedia\.width\}px/u);
  assert.doesNotMatch(mobileRow, /--rail-media-fit|--focus-x|--focus-y/u);
});
