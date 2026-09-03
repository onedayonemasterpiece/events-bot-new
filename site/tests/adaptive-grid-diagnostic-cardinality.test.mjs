import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('adaptive grid publishes full input, admitted source and rendered populations without count/order ambiguity', async () => {
  const source = await read('src/components/AdaptiveEventCardGrid.astro');

  assert.match(source, /const inputOrder = events\.map\(\(item\) => item\.id\)\.join\(','\);/u);
  assert.match(source, /const admittedSource = resolved[\s\S]*sourceIndex: sourceIndexByItem\.get\(item\)[\s\S]*\.filter\(\(\{ sourceIndex \}\) => sourceIndex >= 0\)[\s\S]*\.sort\(\(left, right\) => left\.sourceIndex - right\.sourceIndex\);/u);
  assert.match(source, /const sourceOrder = admittedSource\.map\(\(\{ item \}\) => item\.id\)\.join\(','\);/u);
  assert.match(source, /const renderedOrder = resolved\.map\(\(\{ item \}\) => item\.id\)\.join\(','\);/u);

  for (const reserved of [
    "'data-adaptive-grid-input-count'",
    "'data-adaptive-grid-input-order'",
    "'data-adaptive-grid-source-count'",
    "'data-adaptive-grid-source-order'",
    "'data-adaptive-grid-rendered-count'",
    "'data-adaptive-grid-rendered-order'",
  ]) assert.ok(source.includes(reserved), `canonical diagnostic must be reserved: ${reserved}`);

  assert.match(source, /data-adaptive-grid-input-count=\{events\.length\}/u);
  assert.match(source, /data-adaptive-grid-input-order=\{inputOrder\}/u);
  assert.match(source, /data-adaptive-grid-source-count=\{admittedSource\.length\}/u);
  assert.match(source, /data-adaptive-grid-source-order=\{sourceOrder\}/u);
  assert.match(source, /data-adaptive-grid-rendered-count=\{resolved\.length\}/u);
  assert.match(source, /data-adaptive-grid-rendered-order=\{renderedOrder\}/u);
  assert.doesNotMatch(source, /data-adaptive-grid-source-count=\{events\.length\}/u,
    'source count must not describe the full input while source order describes an admitted subset');
});

test('runtime source policies always mutate each count/order pair together', async () => {
  const source = await read('src/components/AdaptiveEventCardGrid.astro');

  assert.match(source, /if \(sourcePolicy === 'mirror-rendered'\) \{\s*grid\.dataset\.adaptiveGridSourceCount = String\(count\);\s*grid\.dataset\.adaptiveGridSourceOrder = order;\s*\}/u);
  assert.match(source, /else if \(sourcePolicy === 'all-direct'\) \{\s*grid\.dataset\.adaptiveGridSourceCount = String\(directCards\.length\);\s*grid\.dataset\.adaptiveGridSourceOrder = directOrder;\s*\}/u);
  assert.match(source, /grid\.dataset\.adaptiveGridRenderedCount = String\(count\);\s*grid\.dataset\.adaptiveGridRenderedOrder = order;/u);
});
