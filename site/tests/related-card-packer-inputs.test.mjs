import assert from 'node:assert/strict';
import test from 'node:test';

import {
  packRelatedCardRows,
  RELATED_CARD_MAX_DOCUMENT_CROP,
  resolveRelatedCardMediaTreatment,
} from '../src/lib/relatedCardLayout.mjs';

const events = Array.from({ length: 7 }, (_, index) => ({
  id: index + 1,
  title: `Event ${index + 1}`,
  image_url: `/event-${index + 1}.jpg`,
  image_text_mode: 'visual_only',
  image_assets: [{
    src: `/event-${index + 1}.jpg`,
    width: 1250,
    height: 1000,
    image_text_mode: 'visual_only',
    media_semantic_status: 'classified',
    media_role: 'event_photo',
    safe_crop: true,
  }],
}));

const rowSizes = (packed) => [...new Set(packed.map(({ layout }) => layout.rowIndex))]
  .map((rowIndex) => packed.filter(({ layout }) => layout.rowIndex === rowIndex).length);

const classifiedDocumentGeometry = (ratio) => ({
  asset: {
    src: '/document.jpg',
    width: Math.round(ratio * 1000),
    height: 1000,
    image_text_mode: 'ocr_text',
    media_semantic_status: 'classified',
  },
  imageTextMode: 'ocr_text',
  semanticStatus: 'classified',
  documentMedia: true,
  dimensionsKnown: true,
  ratio,
});

test('public resolver enforces the document crop budget for direct callers', () => {
  const decision = resolveRelatedCardMediaTreatment(
    { id: 'over-budget-document' },
    1.25,
    classifiedDocumentGeometry(0.5),
  );

  assert.equal(RELATED_CARD_MAX_DOCUMENT_CROP, 0.2);
  assert.equal(decision.mediaKind, 'document');
  assert.equal(decision.mediaTreatment, 'document-contain');
  assert.equal(decision.fit, 'contain');
  assert.equal(decision.objectPosition, '50% 50%');
  assert.equal(decision.cropReason, 'document_crop_budget_exceeded');
  assert.ok(decision.potentialCoverCrop > RELATED_CARD_MAX_DOCUMENT_CROP);
  assert.equal(decision.coverCrop, 0);
});

test('public resolver permits the exact budget boundary and normalizes invalid targets', () => {
  const boundary = resolveRelatedCardMediaTreatment(
    { id: 'boundary-document' },
    1,
    classifiedDocumentGeometry(0.8),
  );
  assert.equal(boundary.fit, 'cover');
  assert.ok(Math.abs(boundary.coverCrop - RELATED_CARD_MAX_DOCUMENT_CROP) < 1e-9);

  for (const target of [Number.NaN, Number.POSITIVE_INFINITY, 0, -1]) {
    const normalized = resolveRelatedCardMediaTreatment(
      { id: `invalid-target-${String(target)}` },
      target,
      classifiedDocumentGeometry(0.8),
    );
    assert.equal(normalized.fit, 'cover');
    assert.equal(normalized.cropReason, 'document_uncropped');
    assert.equal(normalized.coverCrop, 0);
  }
});

test('public packer floors fractional limit and rowSize values', () => {
  const packed = packRelatedCardRows(events, { limit: 5.9, rowSize: 2.9 });

  assert.equal(packed.length, 5);
  assert.deepEqual(rowSizes(packed), [2, 2, 1]);
  assert.ok(packed.every(({ layout }) => layout.rowColumn === 0 || layout.rowColumn === 1));
});

test('public packer defaults non-finite values instead of producing NaN layout state', () => {
  const packed = packRelatedCardRows(events, { limit: Number.NaN, rowSize: Number.NaN });

  assert.equal(packed.length, events.length);
  assert.deepEqual(rowSizes(packed), [3, 3, 1]);
  assert.ok(packed.every(({ layout }) => Number.isInteger(layout.rowIndex) && Number.isInteger(layout.rowColumn)));
});

test('public packer clamps rowSize and rejects negative limits deterministically', () => {
  const wide = packRelatedCardRows(events, { limit: 7, rowSize: 99 });
  const empty = packRelatedCardRows(events, { limit: -4, rowSize: 3 });

  assert.deepEqual(rowSizes(wide), [6, 1]);
  assert.deepEqual(empty, []);
});

test('packed order and row occupancy are deterministic with one final remainder', () => {
  const first = packRelatedCardRows(events, { limit: 7, rowSize: 3 });
  const second = packRelatedCardRows(events, { limit: 7, rowSize: 3 });

  assert.deepEqual(first.map(({ item }) => item.id), second.map(({ item }) => item.id));
  assert.deepEqual(rowSizes(first), [3, 3, 1]);
  for (const rowIndex of [0, 1, 2]) {
    const columns = first
      .filter(({ layout }) => layout.rowIndex === rowIndex)
      .map(({ layout }) => layout.rowColumn);
    assert.deepEqual(columns, Array.from({ length: columns.length }, (_, index) => index));
  }
  assert.equal(first.at(-1).layout.rowIndex, 2);
  assert.equal(first.at(-1).layout.rowColumn, 0);
});
