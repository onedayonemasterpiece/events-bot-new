import assert from 'node:assert/strict';
import test from 'node:test';
import { packRelatedCardRows, resolveRelatedCardMediaTreatment } from '../src/lib/relatedCardLayout.mjs';

// Synthetic geometry, not a claim about the production snapshot or event 5370.
// The public geometry injection keeps reviewed-media catalog data out of these
// packing tests: none of the synthetic records requires a URL or an override.
const documentItem = (id, ratio) => ({ id, title: `Poster ${id}`, ratio });
const geometryFor = (item, overrides = {}) => ({
  asset: {}, ratio: item.ratio, documentMedia: true, dimensionsKnown: true,
  imageTextMode: 'ocr_text', semanticStatus: 'classified', ...overrides,
});
const pack = (items, options = {}) => packRelatedCardRows(items, { rowSize: 3, geometry: geometryFor, ...options });
const ids = (packed) => packed.map(({ item }) => item.id).sort((a, b) => a - b);

test('AR-04: three eligible documents remain three despite incompatible aspect ratios', () => {
  const items = [documentItem(1, 1), documentItem(2, 1), documentItem(3, 1.2)];
  assert.deepEqual(ids(pack(items)), [1, 2, 3], 'Layout feasibility must not filter admitted content');
});

test('AR-04: mixed documents retain identity/count at 4, 7 and 10 items', () => {
  for (const count of [4, 7, 10]) {
    const items = Array.from({ length: count }, (_, index) => documentItem(index + 1, [0.9, 1.1, 1.6][index % 3]));
    const result = pack(items);
    assert.deepEqual(ids(result), items.map(({ id }) => id), `Lost an item from a ${count}-item batch`);
    const rows = new Map();
    for (const entry of result) {
      const row = rows.get(entry.layout.rowIndex) || [];
      row.push(entry.layout); rows.set(entry.layout.rowIndex, row);
    }
    assert.equal(rows.size, Math.ceil(count / 3));
    for (const row of rows.values()) assert.ok(row.every((entry) => entry.rowRatio === row[0].rowRatio));
  }
});

test('AR-05: a 20% area budget alone cannot authorize cutting unlocated OCR text', () => {
  const item = documentItem(1, 0.5);
  const decision = resolveRelatedCardMediaTreatment(item, 0.625, geometryFor(item));
  assert.equal(decision.fit, 'contain', 'A centered 20% crop can remove the entire top headline');
  assert.equal(decision.coverCrop, 0);
});

test('control: equal-ratio documents still form a deterministic full row', () => {
  const items = [documentItem(1, 1), documentItem(2, 1), documentItem(3, 1)];
  assert.deepEqual(ids(pack(items)), [1, 2, 3]);
  assert.deepEqual(pack(items), pack(items));
  assert.ok(pack(items).every(({ layout }) => layout.coverCrop === 0));
});

test('control: unknown semantic classification stays contained', () => {
  const item = documentItem(1, 0.5);
  const decision = resolveRelatedCardMediaTreatment(item, 0.625, geometryFor(item, { imageTextMode: 'unknown' }));
  assert.equal(decision.fit, 'contain'); assert.equal(decision.coverCrop, null);
});

test('control: unmeasured dimensions do not acquire a numeric crop claim', () => {
  const item = documentItem(1, 0.5);
  const decision = resolveRelatedCardMediaTreatment(item, 0.625, geometryFor(item, { dimensionsKnown: false }));
  assert.equal(decision.fit, 'contain'); assert.equal(decision.coverCrop, null);
});

test('control: empty input and explicit limit remain bounded', () => {
  assert.deepEqual(pack([]), []);
  const items = [documentItem(1, 1), documentItem(2, 1), documentItem(3, 1)];
  assert.deepEqual(pack(items, { limit: 0 }), []);
  assert.deepEqual(ids(pack(items, { limit: 2 })), [1, 2]);
});


test('shared exhibition unread count is identical across routes and excludes seen/rejected items', async () => {
  const { exhibitionsUnreadBadge } = await import('../src/lib/exhibitionsPersonal.ts');
  assert.deepEqual(exhibitionsUnreadBadge([1,2,3,3], null), {count:3,hidden:false,soft:false,text:'3 новых'});
  assert.deepEqual(exhibitionsUnreadBadge([1,2,3], {seenNew:['1'],negative:[2]}), {count:1,hidden:false,soft:false,text:'1 новая'});
  assert.deepEqual(exhibitionsUnreadBadge([1], {seenNew:['1'],hasVisitedExhibitions:true,siteVisits:7}), {count:0,hidden:true,soft:false,text:''});
  assert.deepEqual(exhibitionsUnreadBadge([], {siteVisits:5}), {count:0,hidden:false,soft:true,text:'загляните'});
  assert.equal(exhibitionsUnreadBadge([1], {seenNew:42,negative:'bad'}).count, 1);
});
