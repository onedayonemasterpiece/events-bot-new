import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const occurrences = (source, token) => source.split(token).length - 1;

const cssRule = (source, selector, fromIndex = 0) => {
  const marker = `${selector} {`;
  const start = source.indexOf(marker, fromIndex);
  assert.notEqual(start, -1, `missing CSS rule: ${selector}`);
  const open = source.indexOf('{', start);
  let depth = 0;
  for (let index = open; index < source.length; index += 1) {
    if (source[index] === '{') depth += 1;
    if (source[index] === '}') {
      depth -= 1;
      if (depth === 0) return {
        body: source.slice(open + 1, index),
        end: index + 1,
      };
    }
  }
  assert.fail(`unterminated CSS rule: ${selector}`);
};

test('EventMediaRail delegates image clipping to canonical MediaFrame', async () => {
  const [rail, mediaFrame] = await Promise.all([
    read('src/components/EventMediaRail.astro'),
    read('src/components/media-frame.css'),
  ]);

  const canonicalRoot = cssRule(mediaFrame, '[data-media-frame][data-media-frame-contract="v1"]');
  assert.match(canonicalRoot.body, /overflow:\s*hidden;/u);

  const canonicalRail = cssRule(
    mediaFrame,
    '[data-media-frame][data-media-frame-contract="v1"][data-media-frame-surface="media-rail"]',
  );
  assert.match(canonicalRail.body, /border-radius:\s*inherit;/u);

  assert.equal(occurrences(rail, 'data-media-frame-clip="frame"'), 2);
  assert.equal(occurrences(rail, 'data-media-frame-radius="surface"'), 2);

  const gallerySharedSelector = '.event-media-rail--gallery-thumbnails > .event-media-rail__item,\n  .event-media-rail--gallery-thumbnails > .event-media-rail__more';
  const galleryShared = cssRule(rail, gallerySharedSelector);
  assert.doesNotMatch(galleryShared.body, /overflow:\s*hidden;/u);

  const galleryMoreSelector = '.event-media-rail--gallery-thumbnails > .event-media-rail__more';
  const galleryMoreInSharedRule = cssRule(rail, galleryMoreSelector);
  const galleryMore = cssRule(rail, galleryMoreSelector, galleryMoreInSharedRule.end);
  assert.match(galleryMore.body, /overflow:\s*hidden;/u,
    'the non-MediaFrame overflow control must preserve its previous clipping');

  const heroItem = cssRule(rail, '.event-media-rail--hero-selector > .event-media-rail__item');
  assert.doesNotMatch(heroItem.body, /overflow:\s*hidden;/u);

  const posterItemSelector = '.event-media-rail--poster-strip > .event-media-rail__item,\n  .event-media-rail--poster-strip > .event-media-rail__item[data-rail-aspect="portrait"],\n  .event-media-rail--poster-strip > .event-media-rail__item[data-rail-aspect="landscape"]';
  const posterItem = cssRule(rail, posterItemSelector);
  assert.doesNotMatch(posterItem.body, /overflow:\s*hidden;/u);

  const railContainerSelector = '.event-media-rail.event-media-rail--hero-selector,\n  .event-media-rail.event-media-rail--poster-strip';
  const railContainer = cssRule(rail, railContainerSelector);
  assert.match(railContainer.body, /overflow:\s*hidden;/u,
    'rail viewport overflow remains a surface-layout responsibility');

  const styleStart = rail.indexOf('<style>');
  const styleEnd = rail.indexOf('</style>', styleStart);
  assert.ok(styleStart >= 0 && styleEnd > styleStart);
  const localStyles = rail.slice(styleStart, styleEnd);
  assert.doesNotMatch(localStyles, /object-(?:fit|position)\s*:/u,
    'EventMediaRail styles must not regain fit or focal-position ownership');
});
