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

test('category chips use the expanded, attributed SVGRepo family', async () => {
  const page = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');
  const lucideIconFiles = [
    '389003-anchor.svg',
    '389049-book-open.svg',
    '389059-camera.svg',
    '389063-carrot.svg',
    '389241-history.svg',
    '389291-map-pin.svg',
    '389302-mic.svg',
    '389324-music.svg',
    '389330-palette.svg',
    '389439-star.svg',
    '389461-ticket.svg',
    '389494-users.svg',
  ];
  const semanticIconFiles = ['120598-saxophone.svg', '103262-theatre-masks.svg'];

  assert.doesNotMatch(page, /categorySymbol/);
  assert.match(page, /--festival-category-icon/);
  for (const filename of lucideIconFiles) {
    assert.match(page, new RegExp(filename.replace('.', '\\.')));
    const svg = await readFile(new URL(`../public/assets/icons/festival-categories/${filename}`, import.meta.url), 'utf8');
    assert.match(svg, /viewBox="0 0 24 24"/);
    assert.match(svg, /stroke-width="2"/);
  }
  for (const filename of semanticIconFiles) {
    assert.match(page, new RegExp(filename.replace('.', '\\.')));
    const svg = await readFile(new URL(`../public/assets/icons/festival-categories/${filename}`, import.meta.url), 'utf8');
    assert.match(svg, /<svg[\s>]/);
    assert.match(svg, /viewBox=/);
  }
  assert.match(page, /'city-jazz': \['120598-saxophone\.svg', '389324-music\.svg'\]/);
  assert.match(page, /'zhili-byli': \['103262-theatre-masks\.svg'\]/);
  assert.match(page, /festival-month__categories/);
  assert.match(page, /festival-card__theme-icon--secondary/);
});

test('festival likes are local, edition-scoped, and separate from event feedback', async () => {
  const page = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');

  assert.match(page, /festival-edition:2026:\$\{item\.slug\}/);
  assert.match(page, /ke_festival_likes_v1/);
  assert.match(page, /data-festival-like/);
  assert.match(page, /Сохранено только в этом браузере/);
  assert.match(page, /Сейчас подписка не оформляется и уведомления не отправляются/);
  assert.match(page, /href: item\.sourceHref/);
  assert.doesNotMatch(page, /eventHref\(/);
  assert.doesNotMatch(page, /data-feedback-action="like"/);
  assert.doesNotMatch(page, /liked_event_ids/);
});

test('desktop metadata scale and dense labels stay explicit', async () => {
  const page = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');

  assert.match(page, /'program-pending': 'прогр\. позже'/);
  assert.match(page, /shedevry: '1\.10–29\.11'/);
  assert.match(page, /'jazz-v-filarmonii': '13–18\.11'/);
  assert.match(page, /font-size: clamp\(10px, 0\.76vw, 11px\)/);
  assert.match(page, /font-size: clamp\(10\.5px, 0\.83vw, 12\.25px\)/);
  assert.match(page, /-webkit-line-clamp: 2/);
  assert.match(page, /categoryInventory\.length >= 5[\s\S]*?categoryInventory\.slice\(0, 3\)/);
  assert.match(page, /festival-month__categories-more/);
});

test('desktop cards keep the donor single-canvas overlay contract', async () => {
  const page = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');

  assert.match(page, /\.festival-card__caption\s*\{[\s\S]*?background:\s*transparent;/);
  assert.match(page, /\.festival-card__date\s*\{[\s\S]*?background:\s*var\(--primary\);/);
  assert.match(page, /linear-gradient\([\s\S]*?rgba\(15,\s*11,\s*9,\s*0\.92\)[\s\S]*?transparent 72%/);
  assert.match(page, /\.festival-card__theme > span:last-child\s*\{[\s\S]*?text-transform:\s*lowercase;/);
  const captionBlock = page.match(/\.festival-card__caption\s*\{([^}]*)\}/)?.[1] ?? '';
  assert.ok(captionBlock);
  assert.doesNotMatch(captionBlock, /backdrop-filter/);
});
