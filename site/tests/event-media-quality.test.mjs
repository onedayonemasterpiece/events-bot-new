import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { buildDesktopEventPresentation } from '../src/lib/desktopEventPresentation.ts';
import { isLowResolutionPortraitEventMedia, selectEventMediaByQuality } from '../src/lib/eventMediaQuality.ts';

const preview = JSON.parse(await readFile(new URL('../src/data/preview-events.json', import.meta.url), 'utf8'));
const examples = JSON.parse(await readFile(new URL('../src/data/desktop-event-examples.json', import.meta.url), 'utf8'));
const frozenEvents = new Map([...examples.events, ...preview.events].map((event) => [event.id, event]));
const eventById = (id) => frozenEvents.get(id);

test('quality gate drops weak photo renditions when event-local strong media exists', () => {
  const selection = selectEventMediaByQuality([
    { src:'strong.webp', width:1280, height:853, image_text_mode:'visual_only', quality_score:14 },
    { src:'weak.webp', width:300, height:199, image_text_mode:'visual_only', quality_score:7 },
    { src:'map.webp', width:360, height:480, image_text_mode:'ocr_text', media_semantic_status:'classified', media_role:'wayfinding', quality_score:8 },
  ]);
  assert.deepEqual(selection.admittedSourceIndexes, [0, 2]);
  assert.deepEqual(selection.hiddenSourceIndexes, [1]);
});

test('quality gate keeps weak originals when no strong alternative exists', () => {
  const selection = selectEventMediaByQuality([
    { src:'only.webp', width:180, height:320, image_text_mode:'visual_only', quality_score:7 },
  ]);
  assert.deepEqual(selection.admittedSourceIndexes, [0]);
  assert.deepEqual(selection.hiddenSourceIndexes, []);
  assert.equal(isLowResolutionPortraitEventMedia({ src:'only.webp', width:180, height:320, image_text_mode:'visual_only', quality_score:7 }), true);
  assert.equal(isLowResolutionPortraitEventMedia({ src:'wide.webp', width:320, height:180, image_text_mode:'visual_only', quality_score:7 }), false);
});

test('Alye parusa contract keeps seven strong sources and excludes five weak ones', () => {
  const event = eventById(4783);
  assert.ok(event);
  const selection = selectEventMediaByQuality(event.image_assets);
  assert.deepEqual(selection.admittedSourceIndexes, [0, 4, 6, 8, 9, 10, 11]);
  assert.deepEqual(selection.hiddenSourceIndexes, [1, 2, 3, 5, 7]);
  const presentation = buildDesktopEventPresentation(event);
  assert.equal(presentation.splitPortraitViewer, true);
  assert.deepEqual(presentation.splitPortraitSourceIndexes, [0, 4, 6, 8, 9, 10, 11]);
  assert.deepEqual(presentation.splitViewerHiddenSourceIndexes, [1, 2, 3, 5, 7]);
});

test('accepted media/CTA design-system examples remain frozen after the live event expires', () => {
  for (const eventId of [4783, 5374, 6551, 6815]) assert.ok(examples.events.some((event) => event.id === eventId), `missing frozen event ${eventId}`);
});

test('a lone low-resolution portrait stays available in viewport-contain efficient viewer', () => {
  const event = eventById(6815);
  assert.ok(event);
  const presentation = buildDesktopEventPresentation(event);
  assert.equal(presentation.candidate, 'split');
  assert.equal(presentation.splitMediaFit, 'viewport-contain');
  assert.equal(presentation.splitPortraitViewer, true);
  assert.deepEqual(presentation.splitPortraitSourceIndexes, [0]);
  assert.equal(presentation.reason, 'split-low-resolution-portrait-viewer');
});

test('a constrained landscape primary may promote a stronger classified landscape photo', () => {
  const event = {
    id:5756,
    image_url:'primary.webp',
    image_text_mode:'visual_only',
    image_assets:[
      {
        src:'primary.webp', width:1200, height:800,
        image_text_mode:'visual_only', media_semantic_status:'classified',
        media_role:'event_photo', safe_crop:true, recommended_hero_fit:'cover',
        quality_score:14,
      },
      {
        src:'stronger.webp', width:1280, height:853,
        image_text_mode:'visual_only', media_semantic_status:'classified',
        media_role:'event_photo', safe_crop:true, recommended_hero_fit:'cover',
        quality_score:14.5,
      },
    ],
  };
  const presentation = buildDesktopEventPresentation(event);
  assert.equal(presentation.candidate, 'editorial');
  assert.equal(presentation.heroImageIndex, 1);
  assert.equal(presentation.reason, 'editorial-promotes-qualified-landscape-photo');
});

test('mobile hero marks a lone weak portrait for native-size contain rendering', async () => {
  const hero = await readFile(new URL('../src/components/EventHero.astro', import.meta.url), 'utf8');
  const layout = await readFile(new URL('../src/layouts/EventLayout.astro', import.meta.url), 'utf8');
  assert.match(hero, /data-hero-low-resolution-portrait/);
  assert.match(hero, /data-low-resolution-portrait/);
  assert.match(layout, /event-hero--low-resolution-portrait\.event-hero--photo-cover/);
  assert.match(layout, /hero-gallery__image\[data-low-resolution-portrait="true"\]/);
});
