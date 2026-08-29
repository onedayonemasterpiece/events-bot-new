import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { packRelatedCardRows } from '../src/lib/relatedCardLayout.mjs';

const page = await readFile(new URL('../src/pages/lab/design-system/event-card-container/index.astro', import.meta.url), 'utf8');
const festivalLayout = await readFile(new URL('../src/lib/festivalTimelineLayout.ts', import.meta.url), 'utf8');
const optimizedGrid = await readFile(new URL('../src/components/OptimizedEventCardGrid.astro', import.meta.url), 'utf8');

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

test('a collection row never drops an incompatible square OCR card and every row fills its own width', () => {
  const event = (id, width, height, imageTextMode, safeCrop = false) => ({
    id,
    image_url: `https://static.kenigevents.ru/test/${id}.webp`,
    image_text_mode: imageTextMode,
    safe_crop: safeCrop,
    image_assets: [{
      src: `https://static.kenigevents.ru/test/${id}.webp`,
      width,
      height,
      image_text_mode: imageTextMode,
      media_role: imageTextMode === 'visual_only' ? 'event_photo' : 'event_identity_poster',
      media_semantic_status: 'classified',
      safe_crop: safeCrop,
    }],
  });
  const packed = packRelatedCardRows([
    event(2182, 1280, 853, 'visual_only', true),
    event(7907, 2363, 1442, 'ocr_text'),
    event(7609, 1254, 1254, 'ocr_text'),
  ], { limit:3, rowSize:3, mediaTreatment:'hybrid', preserveAll:true });

  assert.deepEqual(new Set(packed.map(({ item }) => item.id)), new Set([2182, 7907, 7609]));
  assert.equal(packed.length, 3, 'the heading/cardinality contract must not silently lose a card');
  assert.match(optimizedGrid, /preserveAll:\s*true/u);
  assert.match(optimizedGrid, /data-optimized-event-card-row/u);
  assert.match(optimizedGrid, /grid-template-columns:\s*minmax\(0,\s*1fr\)/u);
  assert.match(optimizedGrid, /grid-column:1\s*\/\s*-1/u);
  assert.match(optimizedGrid, /grid-template-columns:\s*repeat\(var\(--row-card-count\),\s*minmax\(0,\s*1fr\)\)/u);
});
