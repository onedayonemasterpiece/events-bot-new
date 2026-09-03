import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('EventCard keeps row diagnostics but does not write obsolete grid placement', async () => {
  const [card, adaptive] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/AdaptiveEventCardGrid.astro'),
  ]);

  assert.match(card, /data-lab-row-index=\{desktopRelatedLayout \? String\(desktopRelatedLayout\.rowIndex\) : undefined\}/u);
  assert.match(card, /data-lab-row-column=\{desktopRelatedLayout \? String\(desktopRelatedLayout\.rowColumn\) : undefined\}/u);
  assert.match(card, /style=\{desktopRelatedLayout \? `--lab-row-media-ratio:\$\{desktopRelatedLayout\.rowRatio\.toFixed\(5\)\}` : undefined\}/u);
  assert.doesNotMatch(card, /grid-row:\$\{desktopRelatedLayout\.rowIndex/u);
  assert.doesNotMatch(card, /grid-column:\$\{desktopRelatedLayout\.rowColumn/u);

  assert.match(adaptive, /data-adaptive-grid-layout-engine="flex-lines"/u);
  assert.match(adaptive, /display: flex;/u);
  assert.match(adaptive, /flex-wrap: wrap;/u);
  assert.doesNotMatch(adaptive, /grid-template-columns:\s*repeat/u);
});

test('A0 may retain or remove the bounded runtime placement bridge without losing row diagnostics', async () => {
  const layout = await read('src/layouts/EventLayout.astro');
  const rowSetter = /card\.style\.setProperty\('grid-row', String\(Number\(relatedLayout\.rowIndex\) \+ 1\)\)/u;
  const columnSetter = /card\.style\.setProperty\('grid-column', String\(Number\(relatedLayout\.rowColumn\) \+ 1\)\)/u;
  const rowRemoval = /card\.style\.removeProperty\('grid-row'\)/u;
  const columnRemoval = /card\.style\.removeProperty\('grid-column'\)/u;
  const bridgePresent = rowSetter.test(layout) || columnSetter.test(layout) || rowRemoval.test(layout) || columnRemoval.test(layout);

  assert.match(layout, /setRuntimeCardDataset\(card, 'labRowIndex'/u);
  assert.match(layout, /setRuntimeCardDataset\(card, 'labRowColumn'/u);
  if (bridgePresent) {
    assert.match(layout, rowSetter);
    assert.match(layout, columnSetter);
    assert.match(layout, rowRemoval);
    assert.match(layout, columnRemoval);
  }
});
