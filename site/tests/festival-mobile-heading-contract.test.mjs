import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const festivalRoute = await readFile(
  new URL('../src/pages/festivali/index.astro', import.meta.url),
  'utf8',
);

test('mobile festival heading keeps Russian words intact inside the route-owned hero', () => {
  const mobileBlock = festivalRoute.match(
    /@media \(max-width: 760px\) \{(?<css>[\s\S]*?)\n  \}/u,
  )?.groups?.css;

  assert.ok(mobileBlock, 'expected the festival mobile style block');
  assert.match(
    mobileBlock,
    /\.festival-hero h1 \{[^}]*max-width:\s*100%;[^}]*overflow-wrap:\s*normal;[^}]*word-break:\s*normal;/u,
  );
  assert.doesNotMatch(mobileBlock, /\.festival-hero h1 \{[^}]*max-width:\s*12ch/u);
  assert.doesNotMatch(mobileBlock, /\.festival-hero h1 em \{[^}]*font-size:/u);
  assert.match(festivalRoute, /\.festival-hero h1 em \{[\s\S]*?font-size:\s*0\.72em;/u);
});
