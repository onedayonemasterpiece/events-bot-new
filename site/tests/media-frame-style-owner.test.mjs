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
  assert.doesNotMatch(rootBlock, /(?:^|\n)\s*(?:width|max-width|inline-size|height|block-size)\s*:/u,
    'frame root must not override a rail/card surface box');

  assert.match(
    mediaFrame,
    /\[data-media-frame-fill="true"\]:is\([\s\S]*\[data-media-frame-surface="listing-card"\],[\s\S]*\[data-media-frame-surface="media-rail"\][\s\S]*\) \{\s*height: 100%;\s*\}/u,
  );
  assert.doesNotMatch(
    mediaFrame,
    /\[data-media-frame-fill="true"\]\s*\{\s*height: 100%;/u,
    'root height must not be applied to EventCard or mobile-listing-rail frames',
  );
  assert.doesNotMatch(
    mediaFrame,
    /\[data-media-frame-surface="mobile-listing-rail"\][\s\S]{0,120}height: 100%/u,
  );
  assert.match(mediaFrame, /\[data-media-frame-fill="true"\] > \[data-media-frame-image\][\s\S]*width: 100%;[\s\S]*height: 100%/u);
  assert.match(mediaFrame, /object-position: var\(--media-frame-object-position, 50% 50%\)/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="cover"\] > \[data-media-frame-image\][\s\S]*object-fit: cover/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="contain"\] > \[data-media-frame-image\][\s\S]*object-fit: contain/u);

  const naturalDocumentSelector = '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="event-card"][data-media-frame-fit="contain"]:not([data-media-frame-fill="true"]) > [data-media-frame-image]';
  const naturalDocumentBlock = blockAfter(mediaFrame, naturalDocumentSelector);
  assert.match(naturalDocumentBlock, /width: 100%/u);
  assert.match(naturalDocumentBlock, /height: auto/u);
  assert.doesNotMatch(
    mediaFrame,
    /\[data-media-frame-fit="contain"\] > \[data-media-frame-image\] \{[^}]*width: 100%[^}]*height: auto/usu,
    'natural-height width fill must remain scoped to non-fill EventCard documents',
  );

  assert.match(mobileSurface, /\.ke-mobile-listing-rails--v23 \.event-media\{[^}]*flex:0 0 var\(--media-width\);width:var\(--media-width\);height:112px/u);
  assert.match(mobileRow, /import '\.\.\/media-frame\.css';/u);
  assert.match(mobileRow, /--media-width:\$\{railMedia\.width\}px/u);
  assert.doesNotMatch(mobileRow, /--rail-media-fit|--focus-x|--focus-y/u);
});

test('EventLayout may remove duplicate natural document image sizing after MediaFrame integration', async () => {
  const [mediaFrame, layout] = await Promise.all([
    read('src/components/media-frame.css'),
    read('src/layouts/EventLayout.astro'),
  ]);
  const legacyNaturalRule = /\.event-card__media-shell--document \.event-card__media\s*\{[^}]*width:\s*100%[^}]*height:\s*auto[^}]*\}/u;
  const canonicalNaturalRule = /data-media-frame-surface="event-card"\]\[data-media-frame-fit="contain"\]:not\(\[data-media-frame-fill="true"\]\)[\s\S]*width: 100%;[\s\S]*height: auto;/u;

  assert.match(mediaFrame, canonicalNaturalRule);
  if (legacyNaturalRule.test(layout)) {
    assert.match(layout, legacyNaturalRule, 'current A0 boundary is this duplicate rule only');
  }
});
