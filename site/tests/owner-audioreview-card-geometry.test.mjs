import assert from 'node:assert/strict';
import test from 'node:test';
import { packRelatedCardRows, planRelatedCardRows, relatedCardCropProofPayload, relatedCardMediaFrameBinding, resolveRelatedCardMediaTreatment } from '../src/lib/relatedCardLayout.mjs';

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

const publicImage = (id, width, height, imageTextMode = 'ocr_text') => ({
  id,
  title:`Public canary ${id}`,
  image_url:width > 0 && height > 0 ? `/${id}.webp` : '',
  image_text_mode:imageTextMode,
  image_assets:[{
    src:width > 0 && height > 0 ? `/${id}.webp` : '', width, height,
    image_text_mode:imageTextMode,
    media_semantic_status:'classified',
    media_role:imageTextMode === 'visual_only' ? 'event_photo' : 'event_identity_poster',
    safe_crop:imageTextMode === 'visual_only',
  }],
});

const rowGroups = (packed) => [...new Set(packed.map(({ layout }) => layout.rowIndex))]
  .map((rowIndex) => packed.filter(({ layout }) => layout.rowIndex === rowIndex));

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

test('owner 8673 public corpus reports its irreconcilable natural-frame rows without loss or irregular widths', () => {
  // Exact IDs and decoded source dimensions measured on the immutable public
  // 2fe28b1f8 owner candidate. Visual-only items may cover; documents may not
  // crop because this corpus carries no located OCR/text-safe crop evidence.
  const items = [
    publicImage(8667, 1280, 854),
    publicImage(8215, 2048, 886, 'visual_only'),
    publicImage(8210, 2048, 886, 'visual_only'),
    publicImage(8742, 874, 1280),
    publicImage(8080, 1810, 2560),
    publicImage(8252, 1810, 2560),
    publicImage(8724, 1811, 2560),
    publicImage(8747, 960, 1280),
    publicImage(7915, 1810, 2560),
    publicImage(8209, 2048, 886, 'visual_only'),
  ];
  const packed = packRelatedCardRows(items, { rowSize:3, mediaTreatment:'hybrid' });

  assert.deepEqual(ids(packed), ids(items.map((item) => ({ item }))));
  assert.deepEqual(rowGroups(packed).map((row) => row.length), [3, 3, 3, 1]);
  assert.ok(packed.some(({ layout }) => layout.framingStatus === 'unsatisfied'));
  assert.ok(packed.some(({ layout }) => layout.paintedFields === true));
  assert.deepEqual([...new Set(packed.map(({ layout }) => layout.framingConflict).filter(Boolean))], [
    'document-natural-ratio-mismatch:8742@874x1280=0.68281250|8080@1810x2560=0.70703125',
    'document-natural-ratio-mismatch:8724@1811x2560=0.70742187|8747@960x1280=0.75000000',
  ]);
  for (const row of rowGroups(packed)) {
    assert.equal(new Set(row.map(({ layout }) => layout.rowRatio.toFixed(8))).size, 1);
    assert.equal(new Set(row.map(({ layout }) => layout.framingStatus)).size, 1);
  }
});

test('owner 8673 full 30-candidate authority exhausts compatible rows before its measured six-card residue', () => {
  const corpus = [
    [8667,1280,854,'ocr_text'], [8742,874,1280,'ocr_text'], [8215,2048,886,'visual_only'],
    [8209,2048,886,'visual_only'], [8080,1810,2560,'ocr_text'], [8252,1810,2560,'ocr_text'],
    [8724,1811,2560,'ocr_text'], [8210,2048,886,'visual_only'], [8747,960,1280,'ocr_text'],
    [7915,1810,2560,'ocr_text'], [8083,1810,2560,'ocr_text'], [8661,1364,1919,'ocr_text'],
    [8171,1080,1350,'ocr_text'], [8644,1280,1280,'ocr_text'], [7452,1200,826,'visual_only'],
    [8693,1811,2560,'ocr_text'], [8233,1080,1350,'ocr_text'], [7665,1440,2560,'ocr_text'],
    [8715,2236,1521,'ocr_text'], [8642,1280,1280,'ocr_text'], [8169,1024,1280,'ocr_text'],
    [8308,2560,2560,'ocr_text'], [8005,0,0,'unknown'], [8339,2418,2400,'ocr_text'],
    [8364,1920,1080,'ocr_text'], [8213,1943,886,'visual_only'], [8101,1810,2560,'ocr_text'],
    [7445,1200,800,'visual_only'], [7450,1200,801,'visual_only'], [8375,1812,2560,'ocr_text'],
  ].map(([id,width,height,mode]) => publicImage(id,width,height,mode));
  const planned = planRelatedCardRows(corpus, { rowSize:3 });
  assert.deepEqual(ids(planned), ids(corpus.map((item) => ({ item }))));
  assert.equal(new Set(planned.map(({ item }) => item.id)).size, 30);
  assert.ok(planned.slice(0, 10).every(({ layout }) => layout.framingStatus === 'satisfied'));
  assert.equal(planned.filter(({ layout }) => layout.framingStatus === 'unsatisfied').length, 6);
  assert.deepEqual(rowGroups(planned).filter((row) => row[0].layout.framingStatus === 'unsatisfied')
    .map((row) => row.map(({ item }) => item.id)), [[7665,8375,8661], [8339,8715,8364]]);
});

test('full-pool selection uses later natural buckets before admitting a conflicting initial prefix', () => {
  const corpus = [
    publicImage(1, 1000, 1000), publicImage(2, 700, 1000), publicImage(3, 800, 1000),
    ...[4,5,6].map((id) => publicImage(id,1000,1000)),
    ...[7,8,9].map((id) => publicImage(id,700,1000)),
    ...[10,11,12].map((id) => publicImage(id,800,1000)),
  ];
  const planned = planRelatedCardRows(corpus, { rowSize:3, presentation:'flow' });
  assert.deepEqual(ids(planned), corpus.map(({ id }) => id));
  assert.ok(planned.slice(0, 9).every(({ layout }) => layout.framingStatus === 'satisfied'));
  assert.ok(planned.slice(-3).every(({ layout }) => layout.framingStatus === 'unsatisfied'));
});

test('current normalized OCR boxes authorize only a proven bounded vertical cover', () => {
  const hash = 'a'.repeat(64);
  const item = publicImage(1, 800, 1000);
  const asset = {
    ...item.image_assets[0], safe_crop:true, geometry_status:'classified', geometry_coordinate_space:'normalized_0_1',
    current_pixel_sha256:hash, geometry_pixel_sha256:hash,
    ocr_boxes:[{ x:.1, y:.2, w:.8, h:.6 }],
  };
  item.image_assets = [asset];
  const decision = resolveRelatedCardMediaTreatment(item, 1);
  assert.equal(decision.fit, 'cover');
  assert.equal(decision.cropReason, 'protected_text_regions_fit');
  assert.ok(decision.coverCrop > 0 && decision.coverCrop <= .2);
  assert.equal(decision.cropWindow.x, 0);
  assert.equal(decision.cropWindow.w, 1);
  assert.deepEqual(relatedCardMediaFrameBinding(decision), {
    fit:'cover', objectPosition:decision.objectPosition, cropPermission:'reviewed-bounded',
  });
});

test('the same source-bound proof survives discovery serialization; horizontal and stale proof stay closed', () => {
  const hash = 'b'.repeat(64);
  const source = {
    src:'/proof.webp', width:800, height:1000, image_text_mode:'ocr_text', safe_crop:true,
    geometry_status:'classified', geometry_coordinate_space:'normalized_0_1',
    current_pixel_sha256:hash, geometry_pixel_sha256:hash,
    ocr_boxes:[{ x:.1, y:.2, w:.8, h:.6 }],
  };
  const runtimeCandidate = { id:1, title:'Serialized proof', display:{
    image_url:source.src, image_width:source.width, image_height:source.height,
    image_text_mode:source.image_text_mode, ...relatedCardCropProofPayload(source),
  }};
  const accepted = resolveRelatedCardMediaTreatment(runtimeCandidate, 1);
  assert.equal(accepted.mediaTreatment, 'document-protected-cover');
  assert.equal(relatedCardMediaFrameBinding(accepted).cropPermission, 'reviewed-bounded');
  const runtimeFlow = packRelatedCardRows([
    runtimeCandidate,
    publicImage(2, 1000, 1000),
    publicImage(3, 1000, 1000),
  ], { rowSize:3, presentation:'flow', preserveOrder:true });
  assert.ok(runtimeFlow.every(({ layout }) => layout.framingStatus === 'satisfied' && layout.rowRatio === 1));
  assert.equal(runtimeFlow[0].layout.mediaTreatment, 'document-protected-cover');

  const horizontal = { ...source, width:1000 };
  runtimeCandidate.display = {
    image_url:horizontal.src, image_width:horizontal.width, image_height:horizontal.height,
    image_text_mode:horizontal.image_text_mode, ...relatedCardCropProofPayload(horizontal),
  };
  assert.equal(resolveRelatedCardMediaTreatment(runtimeCandidate, .8).cropReason, 'document_text_crop_unproven');

  const stale = { ...source, geometry_pixel_sha256:'c'.repeat(64) };
  runtimeCandidate.display = {
    image_url:stale.src, image_width:stale.width, image_height:stale.height,
    image_text_mode:stale.image_text_mode, ...relatedCardCropProofPayload(stale),
  };
  const rejected = resolveRelatedCardMediaTreatment(runtimeCandidate, 1);
  assert.equal(rejected.fit, 'contain');
  assert.equal(relatedCardMediaFrameBinding(rejected).cropPermission, 'forbidden');
});

test('Free first public row resolves to its shared 1810x2560 natural frame in stable order', () => {
  const items = [7915, 7916, 7920].map((id) => publicImage(id, 1810, 2560));
  const packed = packRelatedCardRows(items, { rowSize:3, preserveOrder:true });

  assert.deepEqual(packed.map(({ item }) => item.id), [7915, 7916, 7920]);
  assert.ok(packed.every(({ layout }) => layout.rowRatio === 1810 / 2560));
  assert.ok(packed.every(({ layout }) => layout.framingStatus === 'satisfied'));
  assert.ok(packed.every(({ layout }) => layout.paintedFields === false));
});

test('Free stable-order incompatible row reports the smallest measured conflict without dropping events', () => {
  const items = [
    publicImage(8080, 1810, 2560),
    publicImage(8564, 2560, 1706, 'visual_only'),
    publicImage(8308, 2560, 2560),
  ];
  const packed = packRelatedCardRows(items, { rowSize:3, preserveOrder:true });

  assert.deepEqual(packed.map(({ item }) => item.id), [8080, 8564, 8308]);
  assert.ok(packed.every(({ layout }) => layout.framingStatus === 'unsatisfied'));
  assert.ok(packed.every(({ layout }) => layout.framingConflict
    === 'document-natural-ratio-mismatch:8080@1810x2560=0.70703125|8308@2560x2560=1.00000000'));
  assert.equal(packed.find(({ item }) => item.id === 8080).layout.paintedFields, false);
  assert.equal(packed.find(({ item }) => item.id === 8308).layout.paintedFields, true);
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

// Actual restored selector, not a duplicate implementation or a July-only fixture.
const heroEvent = (id, extra={}) => ({id,title:`Событие ${id}`,start_date:'2026-09-05',end_date:null,
  lifecycle_status:'active',other_date_ids:[],image_assets:[],...extra});

test('AR-02: stale editorial IDs fall back to exact current catalogue titles, not an empty hero', async()=>{
  const {buildHomeHeroTalkDeck}=await import('../src/lib/homeHeroTalk.ts');
  const events=Array.from({length:6},(_,i)=>heroEvent(9000+i));
  const stale=[{id:'expired-copy',eventId:1,fragments:[{text:'Не актуально',link:true}]}];
  const deck=buildHomeHeroTalkDeck(events,'2026-09-04','review',4,stale);
  assert.equal(deck.length,4);
  assert.ok(deck.every(s=>s.copySource==='catalog-fact-fallback' && s.fragments.length===1
    && s.fragments[0].text===s.event.title && s.fragments[0].link));
  assert.deepEqual(deck,buildHomeHeroTalkDeck(events,'2026-09-04','review',4,stale));
});

test('AR-02: expired/cancelled scenes are never revived to fill a stale bank', async()=>{
  const {buildHomeHeroTalkDeck}=await import('../src/lib/homeHeroTalk.ts');
  assert.deepEqual(buildHomeHeroTalkDeck([heroEvent(1,{start_date:'2026-07-01'}),heroEvent(2,{lifecycle_status:'cancelled'})],
    '2026-09-04','review'),[]);
  assert.deepEqual(buildHomeHeroTalkDeck([heroEvent(3)],'2026-09-04','review',0),[]);
});

test('AR-02: current editorial fragments stay source-bound and occurrence families are deduplicated', async()=>{
  const {buildHomeHeroTalkDeck}=await import('../src/lib/homeHeroTalk.ts');
  const e=heroEvent(1,{other_date_ids:[2],popularity_signal_score:100});
  const copy=[{id:'approved-one',eventId:1,fragments:[{text:'Точные редакционные слова.',link:true}]}];
  const deck=buildHomeHeroTalkDeck([e,heroEvent(2,{other_date_ids:[1]})],'2026-09-04','review',4,copy);
  assert.equal(deck.length,1);assert.equal(deck[0].copySource,'editorial');
  assert.deepEqual(deck[0].fragments,copy[0].fragments);
});

test('AR-02: unsafe/stale/text assets stay text-only; eligible photos retain the mosaic variant', async()=>{
  const {buildHomeHeroTalkDeck,isHomeHeroAssetEligible}=await import('../src/lib/homeHeroTalk.ts');
  const photo={src:'/photo.jpg',image_kind:'photo',image_text_mode:'visual_only',safe_crop:true,recommended_hero_fit:'cover',
    width:2000,height:1000,focal_point:{x:.5,y:.5},current_pixel_sha256:'a'.repeat(64),geometry_pixel_sha256:'a'.repeat(64),face_boxes:[]};
  assert.equal(isHomeHeroAssetEligible(photo),true);
  for(const override of [{image_text_mode:'ocr_text'},{geometry_pixel_sha256:'b'.repeat(64)},{safe_crop:false}]) {
    const bad={...photo,...override};assert.equal(isHomeHeroAssetEligible(bad),false);
    assert.ok(buildHomeHeroTalkDeck([heroEvent(1,{image_assets:[bad]})],'2026-09-04','review',4,[]).every(s=>s.mode==='text-only'));
  }
  const deck=buildHomeHeroTalkDeck(Array.from({length:6},(_,i)=>heroEvent(i+1,{image_assets:[photo]})),
    '2026-09-04','review',4,[]);
  assert.ok(deck.some(s=>s.mode==='photo-mosaic'));assert.ok(deck.some(s=>s.mode==='text-only'));
});
