import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const capture = await readFile(new URL('../evidence/free-collection-page-closure/capture.mjs', import.meta.url), 'utf8');

test('free collection proof capture decodes all five lazy card images before screenshots', () => {
  assert.match(capture, /expected exactly five canonical cards/u);
  assert.match(capture, /image\.complete && image\.naturalWidth > 0/u);
  assert.match(capture, /image\.decode\(\)/u);
  assert.match(capture, /all_card_images_decoded:true/u);
  assert.match(capture, /incomplete event-card image readiness/u);
});

test('capture disables smooth scrolling and proves the actual top viewport', () => {
  assert.match(capture, /scroll-behavior:auto!important/u);
  assert.match(capture, /waitForFunction\(\(\) => scrollY === 0/u);
  assert.match(capture, /const topEvidence = await snapshot\(\)/u);
});

test('capture freezes the reference clock and rejects fixture identity drift', () => {
  assert.match(capture, /2026-08-29T14:00:00\+02:00/u);
  assert.match(capture, /timezoneId:'Europe\/Kaliningrad'/u);
  assert.match(capture, /locale:'ru-RU'/u);
  assert.match(capture, /scenario mismatch/u);
  assert.match(capture, /reference clock mismatch/u);
  assert.match(capture, /render order mismatch/u);
  assert.match(capture, /group mismatch/u);
});

test('capture produces all six route and bounded component/group source PNGs without networkidle', () => {
  assert.doesNotMatch(capture, /networkidle/u);
  assert.match(capture, /astro-top\.png/u);
  assert.match(capture, /astro-scrolled\.png/u);
  assert.match(capture, /astro-full\.png/u);
  assert.match(capture, /astro-group-/u);
  assert.match(capture, /astro-card-/u);
  assert.match(capture, /geometry-and-style\.json/u);
});
