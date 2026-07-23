import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { festivalTimeline } from '../src/data/festivalTimeline.ts';
import { festivalTimelineMedia } from '../src/data/festivalTimelineMedia.ts';
import { packFestivalTimeline } from '../src/lib/festivalTimelineLayout.ts';

function item(index, overrides = {}) {
  return {
    slug: `festival-${index}`,
    title: `Festival ${index}`,
    dateLabel: `${index + 1} августа`,
    monthKey: 'august',
    place: 'Калининград',
    category: 'Музыка',
    description: 'Fixture',
    status: 'announced',
    statusLabel: 'Даты объявлены',
    sourceHref: 'https://example.test/',
    sourceLabel: 'Fixture',
    image: `/fixture-${index}.webp`,
    imageWidth: 1600,
    imageHeight: 1067,
    mediaMode: 'visual',
    ...overrides,
  };
}

test('seven visual festivals use the compact 4 + 3 reference formation', () => {
  const rows = packFestivalTimeline(Array.from({ length: 7 }, (_, index) => item(index)));
  assert.deepEqual(rows.map((row) => row.items.length), [4, 3]);
  assert.ok(rows.every((row) => row.widthFraction === 1));
  assert.ok(rows.every((row) => Math.max(...row.columnWeights) / Math.min(...row.columnWeights) <= 1.09));
});

test('five items fill the first row and keep only the final solo compact', () => {
  const rows = packFestivalTimeline(Array.from({ length: 5 }, (_, index) => item(index)));
  assert.deepEqual(rows.map((row) => row.items.length), [4, 1]);
  assert.equal(rows[0].widthFraction, 1);
  assert.equal(rows[0].isRemainder, false);
  assert.equal(rows[1].widthFraction, 0.4);
  assert.equal(rows[1].isRemainder, true);
  assert.ok(rows[1].normalizedMediaHeight <= rows[1].widthFraction / (16 / 9));
});

test('a document anchors its row at natural aspect without crop', () => {
  const items = [
    item(0),
    item(1),
    item(2, { mediaMode: 'document', imageWidth: 1000, imageHeight: 1400 }),
    item(3),
  ];
  const [row] = packFestivalTimeline(items);
  assert.equal(row.items.length, 4);
  const documentIndex = row.items.findIndex((entry) => entry.mediaMode === 'document');
  assert.ok(documentIndex >= 0);
  assert.ok(row.cropFractions[documentIndex] <= 0.000001);
});

test('unknown semantic media fails closed instead of entering a packed cover row', () => {
  const rows = packFestivalTimeline([item(0, { mediaMode: 'unknown' })]);
  assert.deepEqual(rows, []);
});

test('every published card has hash-bound reviewed provenance from a first-party source', async () => {
  assert.equal(festivalTimeline.length, 21);
  assert.equal(Object.keys(festivalTimelineMedia).length, 21);
  for (const festival of festivalTimeline) {
    const media = festivalTimelineMedia[festival.slug];
    assert.ok(media, `missing media review for ${festival.slug}`);
    assert.equal(media.semanticClass, festival.mediaMode);
    assert.doesNotMatch(media.sourceHref, /afisha80let\.visit-kaliningrad\.ru/iu);
    const asset = await readFile(new URL(`../public${festival.image}`, import.meta.url));
    const digest = createHash('sha256').update(asset).digest('hex');
    assert.equal(digest, media.assetSha256, `stale media hash for ${festival.slug}`);
  }
});
