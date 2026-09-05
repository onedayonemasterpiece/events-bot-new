import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('exhibition deck frames consume the canonical large media and card radii', async () => {
  const [row, surface] = await Promise.all([
    read('src/components/ExhibitionPrototypeRow.astro'),
    read('src/components/ExhibitionsPersonalSurface.astro'),
  ]);

  assert.match(row, /data-media-frame-surface="exhibitions-deck"[\s\S]*?data-media-frame-radius="hero"/u);
  assert.match(surface, /\.ex-row \{[\s\S]*?--ex-row-radius:var\(--ke-shape-radius-card\);/u);
  assert.match(surface, /\.ex-row::after \{[\s\S]*?border-radius:var\(--ex-row-radius\);/u);
  assert.match(surface, /\.ex-deck \{[\s\S]*?--media-frame-radius:var\(--ke-shape-radius-hero\);[\s\S]*?border-radius:var\(--ke-shape-radius-hero\);/u);
  const deckBlock = surface.match(/\.ex-deck \{([\s\S]*?)\n  \}/u)?.[1] || '';
  assert.doesNotMatch(deckBlock, /border-radius:6px;/u);
});

test('ListingPageHeader hides screenshot date chips only on desktop date-family routes', async () => {
  const [header, css] = await Promise.all([
    read('src/components/listings/ListingPageHeader.astro'),
    read('src/styles/design-system.css'),
  ]);

  assert.match(header, /current: 'today' \| 'tomorrow' \| 'weekend' \| 'popular' \| 'date'/u);
  assert.match(header, /data-ds-variant=\{current\}/u);
  const desktopRule = css.slice(css.indexOf('@media (min-width: 981px) {', css.indexOf('ke-listing-date-nav')));
  assert.match(desktopRule, /\.ke-listing-head\[data-ds-variant="today"\] \.ke-listing-date-nav,/u);
  assert.match(desktopRule, /\.ke-listing-head\[data-ds-variant="tomorrow"\] \.ke-listing-date-nav,/u);
  assert.match(desktopRule, /\.ke-listing-head\[data-ds-variant="date"\] \.ke-listing-date-nav \{ display:none; \}/u);
  assert.doesNotMatch(desktopRule, /data-ds-variant="(?:weekend|popular)"/u);
});

 test('listing cards use shared no-fields fallback rather than a local contain override', async () => {
 const source = await read('src/components/listings/ListingEventCard.astro');
 const css = await read('src/styles/design-system.css');
 assert.match(source, /const mediaFrameFit = image.asset \? 'cover' : 'contain'/u);
 assert.match(source, /'fallback-minimal'/u);
 assert.match(source, /'unverified-text'/u);
 assert.doesNotMatch(css, /ke-listing-card:not\([^\n]+object-fit: contain/u);
 assert.match(source, /mediaFrame.dataset.mediaFrameFit = 'contain'/u);
 });

test('natural mobile rail dimensions retain no-fields rendering and truthful fallback permission', async () => {
 const source = await read('src/components/listings/MobileListingRailRow.astro');
 assert.match(source, /data-media-frame-fit="cover"/u);
 assert.match(source, /natural_rounding_fallback:/u);
 assert.match(source, /'fallback-minimal'/u);
 assert.match(source, /--media-width:\$\{railMedia.width\}px/u);
});
