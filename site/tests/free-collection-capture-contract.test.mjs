import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const capture = await readFile(
  new URL('../evidence/recovery-20260829/free-collection-september-v2/capture.mjs', import.meta.url),
  'utf8',
);

test('free collection proof capture decodes all five lazy card images before screenshots', () => {
  assert.match(capture, /expectedCardCount=5/u);
  assert.match(capture, /image\.complete&&image\.naturalWidth>0/u);
  assert.match(capture, /image\.decode\(\)/u);
  assert.match(capture, /all_card_images_decoded:true/u);
  assert.match(capture, /incomplete event-card image readiness/u);
});

test('capture disables smooth scrolling and proves the actual top viewport', () => {
  assert.match(capture, /documentElement\.style\.scrollBehavior='auto'/u);
  assert.match(capture, /waitForFunction\(\(\)=>scrollY===0/u);
  assert.match(capture, /top_geometry:topGeometry/u);
});
