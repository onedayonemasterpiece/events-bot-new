import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const page = await readFile(new URL('../src/pages/lab/design-system/event-card-container/index.astro', import.meta.url), 'utf8');
const festivalLayout = await readFile(new URL('../src/lib/festivalTimelineLayout.ts', import.meta.url), 'utf8');

test('design-system fixture calls both production row packers', () => {
  assert.match(page, /packRelatedCardRows\(relatedEvents/u);
  assert.match(page, /packFestivalTimeline\(items\)/u);
  for (const id of ['related-3', 'festival-2', 'festival-3', 'festival-4']) {
    assert.match(page, new RegExp(id));
  }
  for (const attribute of ['data-source-ratio', 'data-target-ratio', 'data-crop-fraction']) {
    assert.match(page, new RegExp(attribute));
  }
  assert.match(page, /data-column-weight/u);
  assert.match(page, /data-row-width-fraction/u);
});

test('every two-card festival row fills the container and only a singleton may remain compact', () => {
  assert.match(festivalLayout, /if \(rowSize === 2\) \{[\s\S]*?widthFraction: 1/u);
  assert.doesNotMatch(festivalLayout, /rowSize === 2[\s\S]{0,120}?widthFraction: 0\.62/u);
  assert.match(festivalLayout, /only a final singleton may intentionally stay compact/u);
});
