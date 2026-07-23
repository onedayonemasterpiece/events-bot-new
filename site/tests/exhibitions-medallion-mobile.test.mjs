import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const sourceUrl = new URL('../src/components/ExhibitionsPersonalSurface.astro', import.meta.url);
const labUrl = new URL('../src/pages/lab/exhibitions-personal/index.astro', import.meta.url);
const rowUrl = new URL('../src/components/ExhibitionPrototypeRow.astro', import.meta.url);

function cssFrom(source) {
  const start = source.indexOf('<style is:global>');
  const end = source.indexOf('</style>', start);
  assert.notEqual(start, -1, 'exhibitions surface must keep its global style block');
  assert.notEqual(end, -1, 'exhibitions surface style block must be closed');
  return source.slice(start, end);
}

function declarations(css, selector, from = 0) {
  const selectorIndex = css.indexOf(selector, from);
  assert.notEqual(selectorIndex, -1, `missing CSS selector: ${selector}`);
  const open = css.indexOf('{', selectorIndex);
  const close = css.indexOf('}', open);
  assert.notEqual(open, -1, `missing opening brace for: ${selector}`);
  assert.notEqual(close, -1, `missing closing brace for: ${selector}`);
  return {
    body: css.slice(open + 1, close),
    end: close + 1,
  };
}

test('public and lab exhibition surfaces preserve the owner-accepted 44px seal geometry', async () => {
  const [surfaceSource, labSource] = await Promise.all([
    readFile(sourceUrl, 'utf8'),
    readFile(labUrl, 'utf8'),
  ]);

  for (const [name, css] of [
    ['public', cssFrom(surfaceSource)],
    ['lab', cssFrom(labSource)],
  ]) {
    const desktop = declarations(css, '.ex-deck__medallion {');
    assert.match(desktop.body, /\btop:\s*8px\s*;/u, `${name}: desktop seal must retain the donor offset`);
    assert.match(desktop.body, /\bleft:\s*8px\s*;/u, `${name}: desktop seal must retain the donor offset`);
    assert.match(desktop.body, /\bwidth:\s*44px\s*;/u, `${name}: desktop seal must remain 44px`);
    assert.match(desktop.body, /\bheight:\s*44px\s*;/u, `${name}: desktop seal must remain 44px`);
    assert.match(desktop.body, /\bz-index:\s*260\s*;/u, `${name}: seal must remain below the photo counter`);

    const mobileMedia = css.indexOf('@media (max-width:820px)');
    assert.notEqual(mobileMedia, -1, `${name}: mobile exhibitions breakpoint must exist`);
    const mobile = declarations(css, '.ex-deck__medallion {', mobileMedia);
    assert.match(mobile.body, /\btop:\s*7px\s*;/u, `${name}: mobile seal must retain the donor offset`);
    assert.match(mobile.body, /\bleft:\s*7px\s*;/u, `${name}: mobile seal must retain the donor offset`);
    assert.match(mobile.body, /\bwidth:\s*44px\s*;/u, `${name}: mobile seal must use the owner-accepted readable 44px`);
    assert.match(mobile.body, /\bheight:\s*44px\s*;/u, `${name}: mobile seal must remain square`);

    const mobileDeck = declarations(css, '.ex-deck {', mobileMedia);
    assert.match(mobileDeck.body, /\binline-size:\s*100%\s*;/u, `${name}: seal sizing must not narrow the mobile deck`);
    assert.match(mobileDeck.body, /\bmax-inline-size:\s*100%\s*;/u, `${name}: mobile deck must remain overflow-safe`);

    const counter = declarations(css, '.ex-deck__count {');
    assert.match(counter.body, /\bz-index:\s*300\s*;/u, `${name}: +N counter must remain above the seal`);
  }
});

test('exhibition seal remains one fail-closed image outside photo-deck semantics', async () => {
  const rowSource = await readFile(rowUrl, 'utf8');

  assert.match(rowSource, /const listingMedallion = medallionResolution\.identities\.find/u);
  assert.match(rowSource, /category === 'venue_brand' \|\| category === 'organizer'/u);
  assert.equal((rowSource.match(/data-exhibition-medallion/gu) || []).length, 1);
  assert.match(rowSource, /aria-hidden="true"/u);
  assert.match(rowSource, /width="44"\s+height="44"/u);
  assert.doesNotMatch(rowSource, /deckMedia\.push\(listingMedallion\)/u);
});
