import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { selectEventContinuation } from '../src/lib/eventContinuation.mjs';
import { packRelatedCardRows, RELATED_CARD_MAX_DOCUMENT_CROP } from '../src/lib/relatedCardLayout.mjs';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('event 3934 crop canary and its continuation use globally packed OCR-safe row geometry', () => {
  // Keep this algorithm test independent from the rolling preview catalog:
  // production events legitimately expire, while the accepted geometry must
  // remain frozen. The ratios mirror the reviewed mixed two-row specimen.
  const cards = [
    { event_id:3934, image_url:'/3934.webp', image_text_mode:'ocr_text', image_width:600, image_height:1200 },
    { event_id:6593, image_url:'/6593.webp', image_text_mode:'ocr_text', image_width:1000, image_height:1000 },
    { event_id:6821, image_url:'/6821.webp', image_text_mode:'visual_only', image_width:1600, image_height:900 },
    { event_id:6907, image_url:'/6907.webp', image_text_mode:'visual_only', image_width:700, image_height:1000 },
    { event_id:4784, image_url:'/4784.webp', image_text_mode:'visual_only', image_width:1500, image_height:1000 },
    { event_id:6407, image_url:'/6407.webp', image_text_mode:'visual_only', image_width:900, image_height:1000 },
  ];
  const ids = cards.map((event) => event.event_id);

  const packed = packRelatedCardRows(cards, { limit: 6, rowSize: 3, mediaTreatment: 'hybrid' });
  assert.equal(packed.length, 6);
  assert.deepEqual(new Set(packed.map(({ item }) => item.event_id)), new Set(ids), 'global packing may reorder but never drops an incompatible poster');
  assert.equal(packed.find(({ item }) => item.event_id === 3934)?.layout.mediaKind, 'document');

  const rowIndexes = [...new Set(packed.map(({ layout }) => layout.rowIndex))];
  for (const rowIndex of rowIndexes) {
    const row = packed.filter(({ layout }) => layout.rowIndex === rowIndex);
    assert.equal(row.length, 3, 'all six canaries form full rows');
    assert.equal(new Set(row.map(({ layout }) => layout.rowRatio.toFixed(5))).size, 1);
    assert.ok(row.every(({ layout }) => layout.paintedFields === false), 'no loaded card may expose fields');
    assert.ok(row.every(({ layout }) => layout.framingStatus === 'satisfied'));
    assert.ok(row.every(({ layout }) => layout.rowWorstCrop <= RELATED_CARD_MAX_DOCUMENT_CROP + 1e-9));
    assert.ok(row.filter(({ layout }) => layout.mediaKind === 'document').every(({ layout }) => layout.coverCrop <= RELATED_CARD_MAX_DOCUMENT_CROP + 1e-9));
  }
});

test('event continuation excludes current, prior, recent and rejected while interleaving with hard caps', () => {
  const item = (eventId, category, venue, extra = {}) => ({
    event_id: eventId,
    personal_score: 1 - eventId / 10000,
    candidate: { event_id: eventId, category, location_name: venue, ...extra },
  });
  const selected = selectEventContinuation({
    currentEventId: 3934,
    excludedIds: [10],
    recentServedIds: [11],
    profileCandidates: [
      item(3934, 'theatre', 'a'),
      item(10, 'theatre', 'a'),
      item(12, 'theatre', 'a'),
      item(13, 'theatre', 'a'),
      item(14, 'theatre', 'b'),
      item(15, 'music', 'c'),
    ],
    adjacentCandidates: [
      item(11, 'music', 'c'),
      item(20, 'exhibition', 'd', { verification_state: 'llm_rejected' }),
      item(21, 'music', 'c', { verification_state: 'not_run', vector_similarity: 0.77 }),
      item(22, 'lecture', 'e', { verification_state: 'llm_approved', vector_similarity: 0.76 }),
      item(23, 'kids', 'f', { verification_state: 'not_run', vector_similarity: 0.74 }),
    ],
    genericCandidates: [
      item(30, 'cinema', 'g'),
      item(31, 'sport', 'h'),
      item(32, 'market', 'i'),
    ],
    limit: 6,
    maxSameCategory: 3,
    maxSameVenue: 2,
  });

  assert.equal(selected.length, 6);
  const ids = selected.map(({ event_id }) => event_id);
  assert.deepEqual(ids.slice(0, 4), [12, 21, 13, 22], 'profile and current-event tail are deterministically interleaved');
  assert.ok(!ids.some((id) => [3934, 10, 11, 20].includes(id)));
  assert.equal(new Set(ids).size, ids.length);
  const categories = selected.map(({ candidate }) => candidate.category);
  const venues = selected.map(({ candidate }) => candidate.location_name);
  assert.ok(Math.max(...[...new Set(categories)].map((value) => categories.filter((item) => item === value).length)) <= 3);
  assert.ok(Math.max(...[...new Set(venues)].map((value) => venues.filter((item) => item === value).length)) <= 2);
  assert.ok(selected.some(({ candidate }) => candidate.verification_state === 'not_run'), 'raw vector candidates may appear only in the mixed continuation');
});

test('broad continuation escapes a same-type bubble while remaining finite and deduplicated', () => {
  const item = (eventId, category, venue) => ({
    event_id: eventId,
    personal_score: 1 - eventId / 1000,
    candidate: { event_id: eventId, category, location_name: venue },
  });
  const selected = selectEventContinuation({
    currentEventId: 1,
    excludedIds: [2],
    profileCandidates: [
      item(2, 'theatre', 'stage-a'),
      item(3, 'theatre', 'stage-a'),
      item(4, 'theatre', 'stage-b'),
      item(5, 'theatre', 'stage-c'),
      item(6, 'theatre', 'stage-d'),
    ],
    adjacentCandidates: [
      item(3, 'theatre', 'stage-a'),
      item(7, 'music', 'hall-a'),
      item(8, 'exhibition', 'museum-a'),
      item(9, 'lecture', 'library-a'),
    ],
    genericCandidates: [
      item(10, 'cinema', 'screen-a'),
      item(11, 'market', 'square-a'),
    ],
    limit: 6,
    maxSameCategory: 3,
    maxSameVenue: 2,
  });

  const ids = selected.map(({ event_id }) => event_id);
  assert.equal(selected.length, 6, 'desktop broad discovery is capped to exactly two three-card rows when supply exists');
  assert.equal(new Set(ids).size, ids.length, 'the same event never appears twice across ranking lanes');
  assert.ok(!ids.includes(2), 'events already offered by the similar section stay excluded');
  assert.ok(selected.filter(({ candidate }) => candidate.category === 'theatre').length <= 3, 'at least half of the finite section escapes the theatre/type bubble');
  assert.ok(new Set(selected.map(({ candidate }) => candidate.category)).size >= 4, 'the continuation deliberately broadens across event types');
});

test('runtime continuation delegates compact row geometry to the canonical adaptive grid', async () => {
  const source = await readFile(path.join(siteRoot, 'src/components/PersonalFeedSlot.astro'), 'utf8');
  assert.match(source, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro'/u);
  assert.match(source, /<AdaptiveEventCardGrid[\s\S]*?mode="packed"[\s\S]*?rowSize=\{3\}[\s\S]*?responsive="progressive"/u);
  assert.match(source, /'data-feed-card-variant':'split-actions'/u);
  assert.doesNotMatch(source, /grid-template-rows:/u);
  assert.doesNotMatch(source, /\[data-lab-related-card\] \.event-card__body[\s\S]*?height:\s*184px/u);
  assert.doesNotMatch(source, /\[data-lab-related-card\] \.event-card__utility-row[\s\S]*?max-height:\s*58px/u);
  assert.doesNotMatch(source, /\[data-lab-related-card\] \.event-card__feedback--under[\s\S]*?max-height:\s*56px/u);
});
