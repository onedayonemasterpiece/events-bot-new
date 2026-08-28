import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { festivalTimelineMedia } from '../src/data/festivalTimelineMedia.ts';
import { packFestivalTimeline } from '../src/lib/festivalTimelineLayout.ts';

const festivalProjection = JSON.parse(
  await readFile(new URL('../src/data/festival-timeline.json', import.meta.url), 'utf8'),
);
const festivalTimeline = festivalProjection.festivals;

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

test('two events form a full-width container instead of a compact remainder', () => {
  const [row] = packFestivalTimeline([
    item(0, { imageWidth:1600, imageHeight:900 }),
    item(1, { imageWidth:900, imageHeight:1200 }),
  ]);
  assert.equal(row.items.length, 2);
  assert.equal(row.widthFraction, 1);
  assert.equal(row.isRemainder, false);
  assert.equal(row.columnWeights.length, 2);
  assert.ok(Math.abs(row.columnWeights.reduce((sum, value) => sum + value, 0) * row.normalizedMediaHeight - (1 - 0.014)) < 0.000001);
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
  assert.equal(festivalProjection.schema_version, 'festival-timeline-static-v1');
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
  const semanticIconFiles = ['480248-saxophone-2.svg', '103262-theatre-masks.svg'];

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
  assert.match(page, /'Джаз': \['480248-saxophone-2\.svg', '389324-music\.svg'\]/);
  assert.match(page, /'Театр': \['103262-theatre-masks\.svg'\]/);
  assert.match(page, /festivalCategoryIcons\[item\.category\]/);
  assert.match(page, /festival-month__categories/);
  assert.match(page, /festival-card__theme-icon--secondary/);
  assert.doesNotMatch(page, /120598-saxophone\.svg/);
});

test('festival likes are local, edition-scoped, and separate from event feedback', async () => {
  const page = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');

  assert.match(page, /festival-edition:2026:\$\{item\.slug\}/);
  assert.match(page, /ke_festival_likes_v1/);
  assert.match(page, /data-festival-like/);
  assert.match(page, /Сохранено только в этом браузере/);
  assert.match(page, /Позже отметка станет основой уведомлений; сейчас это локальная закладка/);
  assert.match(page, /href: item\.sourceHref/);
  assert.doesNotMatch(page, /eventHref\(/);
  assert.doesNotMatch(page, /data-feedback-action="like"/);
  assert.doesNotMatch(page, /liked_event_ids/);
});

test('compact hero gives the regional festival calendar a clear SEO and GEO identity', async () => {
  const page = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');
  const layout = await readFile(new URL('../src/layouts/EventLayout.astro', import.meta.url), 'utf8');

  assert.match(page, /Фестивали Калининградской области 2026 — календарь/);
  assert.match(page, /<h1 id="festival-title">Фестивали <em>Калининградской области<\/em><\/h1>/);
  assert.match(page, /Календарь фестивалей Калининграда и Калининградской области на 2026 год/);
  assert.match(page, /официальным сайтам и сообществам организаторов/);
  assert.match(page, /'@type': 'CollectionPage'/);
  assert.match(page, /'@type': 'ItemList'/);
  assert.match(page, /'@type': 'Festival'/);
  assert.match(page, /itemListOrder: 'https:\/\/schema\.org\/ItemListOrderAscending'/);
  assert.match(page, /dateModified: lastReviewed/);
  assert.match(page, /lastReviewed,/);
  assert.match(page, /inLanguage: 'ru-RU'/);
  assert.match(page, /ogType="website"/);
  assert.match(page, /ogImageAlt="Фестивали Калининградской области 2026"/);
  assert.match(page, /alt=\{`\$\{item\.title\} — фестиваль, \$\{item\.place\}`\}/);
  assert.match(page, /loading=\{item\.slug === 'city-jazz' \? 'eager' : 'lazy'\}/);
  assert.match(page, /fetchpriority=\{item\.slug === 'city-jazz' \? 'high' : undefined\}/);
  assert.match(layout, /ogType\?: 'article' \| 'website'/);
  assert.match(layout, /<meta property="og:type" content=\{ogType\} \/>/);
  assert.match(layout, /<meta property="og:image:alt" content=\{ogImageAlt\} \/>/);
  assert.match(page, /\.festival-hero\s*\{[\s\S]*?min-height:\s*0;[\s\S]*?padding:\s*clamp\(1\.35rem, 2\.15vw, 2rem\);/);
  assert.match(page, /\.festival-month-nav\s*\{[\s\S]*?margin:\s*0\.55rem auto 1rem;/);
  assert.match(page, /\.festival-guide\s*\{[\s\S]*?margin:\s*0 0 1\.25rem;/);
});

test('desktop metadata scale and dense labels stay explicit', async () => {
  const page = await readFile(new URL('../src/pages/festivali/index.astro', import.meta.url), 'utf8');

  assert.match(page, /'program-pending': 'прогр\. позже'/);
  assert.match(page, /shedevry: '1\.10–29\.11'/);
  assert.match(page, /'jazz-v-filarmonii': '13–18\.11'/);
  assert.match(page, /font-size: clamp\(12px, 0\.91vw, 13\.2px\)/);
  assert.match(page, /font-size: clamp\(12\.6px, 1vw, 14\.7px\)/);
  assert.match(page, /font-size: clamp\(21px, 1\.7vw, 25\.2px\)/);
  assert.match(page, /-webkit-line-clamp: 2/);
  assert.match(page, /month\.categoryInventory\.map/);
  assert.match(page, /\.festival-month__categories li\s*\{[\s\S]*?width:\s*28px;[\s\S]*?height:\s*28px;/);
  assert.match(page, /\.festival-month__categories i\s*\{[\s\S]*?width:\s*21px;[\s\S]*?height:\s*21px;/);
  assert.doesNotMatch(page, /visibleCategoryIcons|hiddenCategoryCount|festival-month__categories-more/);
  assert.doesNotMatch(page, /festival-month__symbol/);
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
  const likeBlock = page.match(/\.festival-card__like\s*\{([^}]*)\}/)?.[1] ?? '';
  assert.match(likeBlock, /bottom:\s*clamp/);
  assert.doesNotMatch(likeBlock, /\btop:/);
});

test('jazz is a stable semantic category instead of a slug-specific icon exception', () => {
  const jazzItems = festivalTimeline.filter((festival) =>
    ['city-jazz', 'jazz-v-filarmonii'].includes(festival.slug));
  assert.equal(jazzItems.length, 2);
  assert.ok(jazzItems.every((festival) => festival.category === 'Джаз'));
  assert.equal(festivalTimeline.find((festival) => festival.slug === 'territoriya-mira')?.category, 'Музыка');
});
