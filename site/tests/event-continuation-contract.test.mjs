import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { selectEventContinuation } from '../src/lib/eventContinuation.mjs';
import { packRelatedCardRows, RELATED_CARD_MAX_DOCUMENT_CROP } from '../src/lib/relatedCardLayout.mjs';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('event 3934 and its continuation use stable three-card OCR-safe row geometry', async () => {
  const payload = JSON.parse(await readFile(path.join(siteRoot, 'src/data/preview-events.json'), 'utf8'));
  const byId = new Map(payload.events.map((event) => [event.id, event]));
  const ids = [3934, 6593, 6821, 6907, 4784, 6610];
  const cards = ids.map((id) => {
    const event = byId.get(id);
    assert.ok(event, `fixture event ${id} exists`);
    const asset = event.image_assets?.find((item) => item.src === event.image_url) || event.image_assets?.[0];
    return {
      event_id: event.id,
      image_text_mode: event.image_text_mode,
      image_width: asset?.width || 0,
      image_height: asset?.height || 0,
    };
  });

  const packed = packRelatedCardRows(cards, { limit: 6, rowSize: 3, mediaTreatment: 'hybrid' });
  assert.equal(packed.length, 6);
  assert.deepEqual(packed.map(({ item }) => item.event_id), ids, 'packing is stable and never drops an incompatible poster');
  assert.equal(packed.find(({ item }) => item.event_id === 3934)?.layout.mediaKind, 'document');

  for (const rowIndex of [0, 1]) {
    const row = packed.filter(({ layout }) => layout.rowIndex === rowIndex);
    assert.equal(row.length, 3);
    assert.equal(new Set(row.map(({ layout }) => layout.rowRatio.toFixed(5))).size, 1);
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
