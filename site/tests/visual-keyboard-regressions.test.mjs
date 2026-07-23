import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  MOBILE_EVENT_CARD_VISUAL_RATIO,
  packRelatedCardRows,
  resolveMobileEventCardMedia,
  resolveRelatedCardMediaTreatment,
} from '../src/lib/relatedCardLayout.mjs';
import {
  footerViewportShortcutOwnership,
  keyboardGalleryDestination,
  visualCardRows,
} from '../src/lib/keyboardEventNavigation.mjs';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');
const cropCanaries = JSON.parse(readFileSync(new URL('./fixtures/related-card-crop-canaries.json', import.meta.url), 'utf8'));

const imageEvent = (id, width, height, asset = {}) => ({
  id,
  image_url:`/${id}.jpg`,
  image_text_mode:asset.image_text_mode || 'visual_only',
  image_assets:[{
    src:`/${id}.jpg`,
    width,
    height,
    image_text_mode:'visual_only',
    ...asset,
  }],
});

const classifiedPhoto = (overrides = {}) => ({
  media_role:'event_photo',
  media_semantic_status:'classified',
  safe_crop:true,
  current_pixel_sha256:'pixel-a',
  geometry_pixel_sha256:'pixel-a',
  geometry_status:'classified',
  geometry_coordinate_space:'normalized_0_1',
  face_boxes:[],
  valuable_region:{ x:.4, y:.4, w:.2, h:.2 },
  ...overrides,
});

test('row packing serializes the same unified treatment that EventCard resolves', () => {
  const items = [
    imageEvent(1, 1600, 1000, classifiedPhoto()),
    imageEvent(2, 800, 1000, classifiedPhoto({ geometry_pixel_sha256:'older-pixel' })),
    imageEvent(3, 800, 1000, { image_text_mode:'ocr_text' }),
  ];
  const packed = packRelatedCardRows(items, { rowSize:3, mediaTreatment:'hybrid' });

  assert.deepEqual(packed.map(({ layout }) => layout.mediaTreatment), [
    'visual-cover',
    'visual-cover',
    'document-safe-cover',
  ]);
  for (const { item, layout } of packed) {
    const cardDecision = resolveRelatedCardMediaTreatment(item, layout.rowRatio);
    assert.equal(layout.fit, cardDecision.fit);
    assert.equal(layout.mediaTreatment, cardDecision.mediaTreatment);
    assert.equal(layout.cropReason, cardDecision.cropReason);
    assert.equal(layout.objectPosition, cardDecision.objectPosition);
  }
});

test('visual-only photos retain the accepted compact cover contract when bbox metadata is stale', () => {
  const stale = classifiedPhoto({ geometry_pixel_sha256:'older-pixel' });
  const packed = packRelatedCardRows([
    imageEvent(1, 500, 1000, stale),
    imageEvent(2, 800, 1000, stale),
    imageEvent(3, 1600, 1000, stale),
  ], { rowSize:3, mediaTreatment:'hybrid' });

  assert.ok(packed.every(({ layout }) => layout.fit === 'cover'));
  assert.ok(packed.every(({ layout }) => layout.mediaTreatment === 'visual-cover'));
  assert.ok(packed.every(({ layout }) => layout.cropReason.startsWith('visual_only_focal_fallback:')));
  assert.ok(packed[0].layout.rowRatio >= 1 && packed[0].layout.rowRatio <= 4 / 3);
});

test('the Dog-page photo canaries cannot regress to contain bands and may reorder for the global minimum', () => {
  const packed = packRelatedCardRows([
    imageEvent(5757, 1200, 800, { media_role:'unknown_document', media_semantic_status:'classified', safe_crop:false }),
    imageEvent(6586, 1000, 410, classifiedPhoto({
      valuable_region:{ x:.01, y:.1, w:.98, h:.8 },
    })),
    imageEvent(6318, 1200, 675, classifiedPhoto({ geometry_pixel_sha256:'older-pixel' })),
    imageEvent(6652, 1170, 1755, { media_role:'event_photo', media_semantic_status:'classified', safe_crop:true }),
  ], { rowSize:3, mediaTreatment:'hybrid' });

  assert.deepEqual(packed.map(({ layout }) => layout.mediaTreatment), [
    'visual-cover', 'visual-cover', 'visual-cover', 'visual-cover',
  ]);
  assert.ok(packed.every(({ layout }) => layout.fit === 'cover'));
  assert.ok(packed.every(({ layout }) => layout.rowRatio >= 5 / 4 && layout.rowRatio <= 8 / 5));
  assert.notDeepEqual(packed.map(({ item }) => item.id), [5757, 6586, 6318, 6652], 'packing may reorder cards across rows');
  assert.deepEqual([...new Set(packed.map(({ layout }) => layout.rowIndex))].map((rowIndex) => packed.filter(({ layout }) => layout.rowIndex === rowIndex).length), [3, 1]);
});

test('row packing permits an incomplete row only as the final row for every supported count', () => {
  for (let count = 1; count <= 10; count += 1) {
    const packed = packRelatedCardRows(
      Array.from({ length:count }, (_, index) => imageEvent(index + 1, 1200 + index * 10, 900, classifiedPhoto())),
      { rowSize:3, mediaTreatment:'hybrid' },
    );
    const sizes = [...new Set(packed.map(({ layout }) => layout.rowIndex))]
      .map((rowIndex) => packed.filter(({ layout }) => layout.rowIndex === rowIndex).length);
    assert.ok(sizes.slice(0, -1).every((size) => size === 3), `${count} cards emitted an early incomplete row: ${sizes}`);
    assert.equal(sizes.at(-1), count % 3 || 3, `${count} cards emitted the wrong final remainder: ${sizes}`);
  }
});

test('one-column flow packing preserves search rank and carries no grid presentation intent', () => {
  const ranked = [
    imageEvent(31, 1600, 1000, classifiedPhoto()),
    imageEvent(32, 800, 1200, { image_text_mode:'ocr_text' }),
    imageEvent(33, 1200, 900, classifiedPhoto()),
  ];
  const packed = packRelatedCardRows(ranked, {
    rowSize:1,
    mediaTreatment:'hybrid',
    presentation:'flow',
  });

  assert.deepEqual(packed.map(({ item }) => item.id), [31, 32, 33]);
  assert.ok(packed.every(({ layout }) => layout.presentation === 'flow'));
  assert.deepEqual(packed.map(({ layout }) => layout.rowColumn), [0, 0, 0]);
});

test('unknown OCR dimensions fail closed instead of claiming a measured 20% crop', () => {
  const unknownDocument = imageEvent(41, 0, 0, { image_text_mode:'ocr_text' });
  const decision = resolveRelatedCardMediaTreatment(unknownDocument, 4 / 5);
  const [packed] = packRelatedCardRows([unknownDocument], { rowSize:1, presentation:'flow' });

  assert.equal(decision.fit, 'contain');
  assert.equal(decision.mediaTreatment, 'document-contain');
  assert.equal(decision.cropReason, 'document_dimensions_unknown');
  assert.equal(decision.coverCrop, null);
  assert.equal(decision.potentialCoverCrop, null);
  assert.equal(packed.layout.rowWorstCrop, null);
});

test('mobile large cards keep visual media at horizontal 5:4 and documents intrinsic', () => {
  const frog = imageEvent(4785, 2000, 660, {
    image_text_mode:'visual_only',
    media_role:'unknown_document',
    safe_crop:false,
  });
  const visual = resolveMobileEventCardMedia(frog);
  assert.equal(MOBILE_EVENT_CARD_VISUAL_RATIO, 5 / 4);
  assert.equal(visual.rowRatio, 5 / 4);
  assert.equal(visual.fit, 'cover');
  assert.equal(visual.rowMode, 'mobile-visual-fixed-5x4');

  const knownPoster = resolveMobileEventCardMedia(imageEvent(6998, 1200, 1800, { image_text_mode:'ocr_text' }));
  assert.equal(knownPoster.rowRatio, 2 / 3);
  assert.equal(knownPoster.fit, 'contain');
  assert.equal(knownPoster.useNaturalAspect, true);

  const unknownPoster = resolveMobileEventCardMedia(imageEvent(6999, 0, 0, { image_text_mode:'ocr_text' }));
  assert.equal(unknownPoster.rowRatio, 4 / 5, 'unknown posters reserve a portrait skeleton until decode');
  assert.equal(unknownPoster.cropReason, 'mobile_document_decode_natural');
  assert.equal(unknownPoster.useNaturalAspect, true);
});

test('semantic errors and unknown modes contain while classified visual-only controls cover', () => {
  const semanticError = imageEvent(6686, 1080, 1350, {
    image_text_mode:'visual_only',
    media_semantic_status:'error',
    media_role:'event_photo',
    safe_crop:true,
  });
  const missingMode = imageEvent(6687, 1080, 1350, {
    image_text_mode:'unknown',
    media_semantic_status:'pending',
    media_role:'unknown_document',
    safe_crop:false,
  });
  const classifiedVisual = imageEvent(6529, 1600, 1000, classifiedPhoto());

  for (const event of [semanticError, missingMode]) {
    const related = resolveRelatedCardMediaTreatment(event, 5 / 4);
    const mobile = resolveMobileEventCardMedia(event);
    assert.equal(related.mediaKind, 'document');
    assert.equal(related.fit, 'contain');
    assert.equal(mobile.mediaKind, 'document');
    assert.equal(mobile.fit, 'contain');
  }

  const visual = resolveMobileEventCardMedia(classifiedVisual);
  assert.equal(visual.mediaKind, 'visual');
  assert.equal(visual.fit, 'cover');
});

test('hero and desktop gallery share the semantic-error contain contract', async () => {
  const [hero, desktop, personal, optimizedGrid] = await Promise.all([
    read('src/components/EventHero.astro'),
    read('src/components/DesktopEventPage.astro'),
    read('src/pages/dlya-menya/index.astro'),
    read('src/components/OptimizedEventCardGrid.astro'),
  ]);

  assert.match(hero, /if \(semanticStatus === 'error'\) return 'unknown'/u);
  assert.match(hero, /galleryImageTextMode = failClosedImageTextMode\(asset\.image_text_mode, asset\.media_semantic_status\)/u);
  assert.match(hero, /asset\.media_semantic_status === 'classified' \? asset\.media_role : 'unknown_document'/u);
  assert.match(desktop, /if \(semanticStatus === 'error'\) return 'unknown'/u);
  assert.match(desktop, /failClosedImageTextMode\(asset\?\.image_text_mode \|\| fallbackMode, asset\?\.media_semantic_status\)/u);
  assert.match(desktop, /semanticStatus === 'classified' && classifiedRole/u);
  assert.doesNotMatch(desktop, /fallbackMediaRole[\s\S]{0,320}mode === 'visual_only' \? 'event_photo'/u);
  assert.match(personal, /<OptimizedEventCardGrid/u);
  assert.match(personal, /className="personal-page__feed-list"/u);
  assert.match(desktop, /<OptimizedEventCardGrid/u);
  assert.match(optimizedGrid, /packRelatedCardRows\(events/u);
  assert.match(optimizedGrid, /responsiveMobile/u);
  assert.match(optimizedGrid, /data-optimized-event-card-grid/u);
  assert.doesNotMatch(personal, /repeat\(3,\s*minmax\(0,\s*1fr\)\)/u);
  assert.doesNotMatch(personal, /repeat\(2,\s*minmax\(0,\s*1fr\)\)/u);
});

test('desktop recommendation packing remains adaptive and independent from mobile flow', () => {
  const frog = imageEvent(4785, 2000, 660, { image_text_mode:'visual_only' });
  const [desktop] = packRelatedCardRows([frog], { rowSize:1, presentation:'related-grid' });
  const mobile = resolveMobileEventCardMedia(frog);
  assert.equal(desktop.layout.rowRatio, 8 / 5);
  assert.equal(mobile.rowRatio, 5 / 4);
});
test('optimizer may place the first OCR item in the final remainder to keep full rows feasible and compact', () => {
  const document = (id, width, height) => imageEvent(id, width, height, { image_text_mode:'ocr_text' });
  const packed = packRelatedCardRows([
    document(10, 500, 1000),
    document(11, 1000, 1000),
    imageEvent(12, 1600, 1000, classifiedPhoto()),
    imageEvent(13, 1500, 1000, classifiedPhoto()),
  ], { rowSize:3, mediaTreatment:'hybrid' });
  const rowSizes = [...new Set(packed.map(({ layout }) => layout.rowIndex))]
    .map((rowIndex) => packed.filter(({ layout }) => layout.rowIndex === rowIndex).length);
  assert.deepEqual(rowSizes, [3, 1]);
  assert.equal(packed.find(({ item }) => item.id === 10)?.layout.rowIndex, 1, 'source anchor may move to the final remainder');
});

test('incompatible ranked OCR prefix uses a bounded alternate instead of opening a middle hole', () => {
  const document = (id, width, height) => imageEvent(id, width, height, { image_text_mode:'ocr_text' });
  const packed = packRelatedCardRows([
    document(20, 500, 1000),
    document(21, 1000, 1000),
    document(22, 1400, 1000),
    imageEvent(23, 1600, 1000, classifiedPhoto()),
    imageEvent(24, 1500, 1000, classifiedPhoto()),
    imageEvent(25, 1400, 1000, classifiedPhoto()),
    imageEvent(26, 1300, 1000, classifiedPhoto()),
  ], { limit:6, rowSize:3, mediaTreatment:'hybrid' });
  assert.equal(packed.length, 6, 'one bounded alternate preserves the finite six-card surface');
  assert.deepEqual([...new Set(packed.map(({ layout }) => layout.rowIndex))]
    .map((rowIndex) => packed.filter(({ layout }) => layout.rowIndex === rowIndex).length), [3, 3]);
  assert.ok([20, 21, 22].some((id) => !packed.some(({ item }) => item.id === id)), 'one incompatible OCR candidate is replaced, not put in a singleton middle row');
});

test('captured Dog-page production payloads reject the exact 22%/32% photo-band regression', () => {
  const packed = packRelatedCardRows(cropCanaries.visuals, { rowSize:3, mediaTreatment:'hybrid' });
  assert.deepEqual(new Set(packed.map(({ item }) => item.id)), new Set([5757, 6586, 6318, 5756]));
  const classified = packed.filter(({ item }) => item.image_assets[0].media_semantic_status === 'classified');
  const semanticError = packed.find(({ item }) => item.image_assets[0].media_semantic_status === 'error');
  assert.ok(classified.every(({ layout }) => layout.mediaKind === 'visual'));
  assert.ok(classified.every(({ layout }) => layout.mediaTreatment === 'visual-cover' && layout.fit === 'cover'));
  assert.equal(packed.find(({ item }) => item.id === 5757)?.layout.objectPosition, '50% 25%', 'asset focal metadata must survive the restored cover path');
  assert.equal(semanticError?.layout.mediaKind, 'document');
  assert.equal(semanticError?.layout.mediaTreatment, 'document-contain');
  assert.equal(semanticError?.layout.fit, 'contain');
  assert.equal(semanticError?.layout.cropReason, 'semantic_error_fail_closed');

  const document = resolveRelatedCardMediaTreatment(cropCanaries.document, 1);
  assert.equal(document.mediaKind, 'document');
  assert.equal(document.mediaTreatment, 'document-safe-cover');
  assert.equal(document.fit, 'cover');
  assert.equal(document.coverCrop, 0);
});

test('global row optimizer separates incompatible OCR ratios and keeps every document within 20%', () => {
  const document = (id, width, height) => imageEvent(id, width, height, { image_text_mode:'ocr_text' });
  const items = [
    document(10, 600, 1200),
    document(11, 1000, 1000),
    imageEvent(12, 1600, 900, classifiedPhoto()),
    imageEvent(13, 700, 1000, classifiedPhoto()),
    imageEvent(14, 1500, 1000, classifiedPhoto()),
    imageEvent(15, 900, 1000, classifiedPhoto()),
  ];
  const packed = packRelatedCardRows(items, { rowSize:3, mediaTreatment:'hybrid' });
  const tallRow = packed.find(({ item }) => item.id === 10)?.layout.rowIndex;
  const squareRow = packed.find(({ item }) => item.id === 11)?.layout.rowIndex;
  assert.notEqual(tallRow, squareRow, 'incompatible OCR posters must be placed in different rows');
  assert.equal(new Set(packed.map(({ layout }) => layout.rowIndex)).size, 2, 'enumeration finds the two-row global minimum instead of greedy overflow');
  assert.ok([...new Set(packed.map(({ layout }) => layout.rowIndex))].every((rowIndex) => packed.filter(({ layout }) => layout.rowIndex === rowIndex).length === 3));
  assert.ok(packed.every(({ layout }) => layout.fit === 'cover'));
  assert.ok(packed.filter(({ layout }) => layout.mediaKind === 'document').every(({ layout }) => layout.coverCrop <= 0.2 + 1e-9));
  assert.ok(new Set(packed.map(({ layout }) => layout.rowCost)).size >= 1, 'optimizer serializes its page-height objective');

  const combinations = (values, size) => {
    const output = [];
    const visit = (start, chosen) => {
      if (chosen.length === size) {
        output.push(chosen);
        return;
      }
      for (let index = start; index < values.length; index += 1) visit(index + 1, [...chosen, values[index]]);
    };
    visit(0, []);
    return output;
  };
  const exhaustiveMinimum = (remaining) => {
    if (!remaining.length) return 0;
    const [anchor, ...rest] = remaining;
    let best = Number.POSITIVE_INFINITY;
    for (const tail of combinations(rest, 2)) {
      const group = [anchor, ...tail];
      let plan;
      try {
        plan = packRelatedCardRows(group, { rowSize:3, mediaTreatment:'hybrid' });
      } catch {
        continue;
      }
      if (plan.length !== 3 || new Set(plan.map(({ layout }) => layout.rowIndex)).size !== 1) continue;
      const tailIds = new Set(tail.map((item) => item.id));
      best = Math.min(best, plan[0].layout.rowCost + exhaustiveMinimum(rest.filter((item) => !tailIds.has(item.id))));
    }
    return best;
  };
  const selectedCost = [...new Map(packed.map(({ layout }) => [layout.rowIndex, layout.rowCost])).values()]
    .reduce((sum, value) => sum + value, 0);
  assert.ok(Math.abs(selectedCost - exhaustiveMinimum(items)) < 1e-9, 'DP must equal an independent exhaustive page-height search');
});

test('related and personal continuation surfaces cannot override EventCard fit', async () => {
  const [card, desktop, personal, layout] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/DesktopEventPage.astro'),
    read('src/components/PersonalFeedSlot.astro'),
    read('src/layouts/EventLayout.astro'),
  ]);

  assert.match(card, /resolveRelatedCardMediaTreatment\(event, cardTargetAspect\)/u);
  assert.match(card, /data-card-authoritative-fit=\{cardCrop\.fit\}/u);
  assert.doesNotMatch(desktop, /data-lab-media-treatment="[^"]+"[^}]*object-fit:/su);
  assert.doesNotMatch(personal, /data-lab-media-treatment="[^"]+"[^}]*object-fit:/su);
  assert.doesNotMatch(personal, /data-lab-media-kind="visual"[^}]*object-fit:\s*cover/su);
  assert.match(layout, /image\.style\.objectFit = authoritativeFit/u);
  assert.match(layout, /relatedLayout\?\.cropReason \|\| 'responsive_target_unknown'/u);
});

test('gallery handoff accepts only an exact same-origin event destination', () => {
  const current = 'https://example.test/_review/token/sobytiya/source-1/';
  assert.equal(
    keyboardGalleryDestination('../destination-2/?from=gallery', current),
    '/_review/token/sobytiya/destination-2/?from=gallery',
  );
  assert.equal(keyboardGalleryDestination('https://other.test/sobytiya/destination-2/', current), '');
  assert.equal(keyboardGalleryDestination('/afisha/', current), '');
});

test('visual keyboard rows follow CSS geometry instead of reordered DOM adjacency', () => {
  const cards = ['dom-a', 'dom-b', 'dom-c', 'dom-d', 'dom-e', 'dom-f', 'dom-g'];
  const rectangles = new Map([
    ['dom-a', { top:200, left:400, width:300 }],
    ['dom-b', { top:100, left:700, width:300 }],
    ['dom-c', { top:100, left:100, width:300 }],
    ['dom-d', { top:300, left:100, width:300 }],
    ['dom-e', { top:200, left:100, width:300 }],
    ['dom-f', { top:100, left:400, width:300 }],
    ['dom-g', { top:200, left:700, width:300 }],
  ]);
  const rows = visualCardRows(cards, { rectFor:(card) => rectangles.get(card) });
  assert.deepEqual(rows.map((row) => row.cards.map(({ card }) => card)), [
    ['dom-c', 'dom-f', 'dom-b'],
    ['dom-e', 'dom-a', 'dom-g'],
    ['dom-d'],
  ]);
});

test('card K hint is reserved in layout but visible only inside the focused card', async () => {
  const component = await read('src/components/KeyboardEventNavigationPrototype.astro');
  assert.match(component, /\.related-calendar-shortcut[\s\S]*visibility:hidden;[\s\S]*opacity:0;/u);
  assert.match(component, /\.related-calendar-shortcut:not\(\[hidden\]\) \{ display:inline-grid; \}/u,
    'the hidden keycap keeps its horizontal space so focus does not shift card actions');
  assert.match(component, /\[data-event-card\]:focus-within \.related-calendar-shortcut:not\(\[hidden\]\)[\s\S]*visibility:visible;[\s\S]*opacity:\.58;/u);
});

test('visible footer owns P/S from body or an offscreen managed event owner', () => {
  const viewport = { viewportWidth:1200, viewportHeight:800 };
  const footerRect = { left:0, right:1200, top:610, bottom:850, width:1200, height:240 };
  const visibleCard = { left:20, right:360, top:100, bottom:500, width:340, height:400 };
  const offscreenCard = { left:20, right:360, top:-700, bottom:-300, width:340, height:400 };

  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'body', ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'managed-card', targetRect:offscreenCard, ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'event-surface', targetRect:offscreenCard, ...viewport }), true);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'event-surface', targetRect:visibleCard, ...viewport }), false);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'managed-card', targetRect:visibleCard, ...viewport }), false);
  assert.equal(footerViewportShortcutOwnership({ footerRect, targetKind:'other', ...viewport }), false);
  assert.equal(footerViewportShortcutOwnership({
    footerRect:{ ...footerRect, top:900, bottom:1140 },
    targetKind:'body',
    ...viewport,
  }), false);
});

test('router persists one bounded gallery handoff and consumes it for body arrows only', async () => {
  const router = await read('src/lib/keyboardEventNavigation.mjs');
  assert.match(router, /GALLERY_HANDOFF_TTL_MS = 30_000/u);
  assert.match(router, /win\.sessionStorage\.setItem\(GALLERY_HANDOFF_STORAGE_KEY/u);
  assert.match(router, /value\.destination === current/u);
  assert.match(router, /galleryDestinationHandoffExpiresAt >= Date\.now\(\)[\s\S]*event\.code === 'ArrowLeft'[\s\S]*target === doc\.body[\s\S]*selectHero/u);
  assert.match(router, /const targetKind = footerShare\.contains\(target\)[\s\S]*managed-card/u);
  assert.match(router, /targetKind === 'event-surface' \? surface\.getBoundingClientRect\(\) : null/u);
  assert.match(router, /footerViewportShortcutOwnership/u);
  assert.match(router, /if \(!legacyBase\) return;[\s\S]*anchor\.href = normalize\(anchor\.href\)/u,
    'root continuation cards must retain the same relative canonical links as static cards');
});
