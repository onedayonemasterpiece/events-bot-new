import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const protectedSources = {
  row: {
    url: new URL('../src/components/ExhibitionPrototypeRow.astro', import.meta.url),
    sha256: '2d7695dc2383790906818287f912016ffdd449a2ad927728ccd0b7b9245a1629',
  },
  surface: {
    url: new URL('../src/components/ExhibitionsPersonalSurface.astro', import.meta.url),
    sha256: '02ae12f1d16c0fbe4174dd0f3b8c33354bbd571ed2aeaa06221d59b54c79bf84',
  },
};

const sha256 = (value) => createHash('sha256').update(value).digest('hex');

test('owner-protected exhibition slider sources stay Astro-authoritative', async () => {
  const row = await readFile(protectedSources.row.url, 'utf8');
  const surface = await readFile(protectedSources.surface.url, 'utf8');

  assert.equal(sha256(row), protectedSources.row.sha256,
    'ExhibitionPrototypeRow slider markup changed: require a superseding owner decision and update the authority contract');
  assert.equal(sha256(surface), protectedSources.surface.sha256,
    'ExhibitionsPersonalSurface slider runtime changed: require a superseding owner decision and update the authority contract');
});

test('protected slider retains source order, measured paging, states, keyboard, focus and accessibility behavior', async () => {
  const [row, surface] = await Promise.all([
    readFile(protectedSources.row.url, 'utf8'),
    readFile(protectedSources.surface.url, 'utf8'),
  ]);

  assert.match(row, /\[\.\.\.media\]\.sort\(\(a, b\) => a\.sourceIndex - b\.sourceIndex\)/u);
  for (const marker of ['data-deck', 'data-deck-frame', 'data-deck-cursor="0"', 'data-deck-phase="idle"', 'data-deck-count', 'data-gallery-images']) {
    assert.ok(row.includes(marker), `missing protected deck marker: ${marker}`);
  }
  assert.match(row, /aria-keyshortcuts="ArrowLeft ArrowRight Enter"/u);

  assert.match(surface, /const layoutDeck\s*=|function layoutDeck/u);
  assert.match(surface, /const pageDeck\s*=|function pageDeck/u);
  assert.match(surface, /data-deck-phase="forward"|dataset\.deckPhase\s*=\s*['"]forward['"]/u);
  assert.match(surface, /data-deck-phase="backward"|dataset\.deckPhase\s*=\s*['"]backward['"]/u);
  assert.match(surface, /new ResizeObserver/u);
  assert.match(surface, /new IntersectionObserver/u);
  assert.match(surface, /event\.key === 'ArrowLeft'[\s\S]*event\.key === 'ArrowRight'/u);
  assert.match(surface, /galleryOpener\.focus\(\)/u);
  assert.match(surface, /prefers-reduced-motion: reduce/u);
  assert.match(surface, /dataset\.imageState = 'loading'/u);
  assert.match(surface, /finish\('loaded'\)/u);
  assert.match(surface, /finish\('error'\)/u);
  assert.match(surface, /aria-live="polite"/u);
});
