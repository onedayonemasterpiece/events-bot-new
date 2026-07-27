import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { resolveUnusualFeed } from '../src/lib/unusualManifest.mjs';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');
const event = (id, slug = `event-${id}`) => ({
  id, slug, title:`Event ${id}`, start_date:'2026-08-10', end_date:null,
  lifecycle_status:'active', ticket:{ kind:'status' }, image_assets:[], topics:[],
});
const item = (eventId, conceptId, overrides = {}) => ({
  event_id:eventId, concept_id:conceptId, representative_event_id:eventId,
  tier:'core_unusual', unusual_score:.91, confidence:.88, families:['format'],
  reason_codes:['rare_format'], prototype_evidence:[], first_published_at:'2026-07-29T10:00:00Z',
  notify_eligible:true, content_hash:`hash-${eventId}`, date:'2026-08-10', lifecycle:'active',
  ...overrides,
});
const manifest = (overrides = {}) => ({
  schema_version:'unusual-events-v1', build_id:'build-1', generated_at:'2026-07-29T11:00:00Z',
  source_snapshot_id:'snapshot-1', hash:'manifest-hash', taxonomy_version:'taxonomy-v1',
  policy_version:'policy-v1', embedding_model:'BAAI/bge-m3', revision:'rev-1', dim:1024,
  doc_kind:'related_v1', document_version:'related_v1', prototype_bank_hash:'prototype-hash',
  classifier_hash:'classifier-hash', rollout_baseline_at:'2026-07-28T00:00:00Z',
  quality_gate:{ status:'approved', metrics:{ precision:.9 } }, items:[item(1,'concept-one')],
  ...overrides,
});

test('approved unusual feed renders at most one card per concept and event through trusted catalog cards', () => {
  const result = resolveUnusualFeed(manifest({ items:[
    item(1,'concept-one'), item(2,'concept-one'), item(1,'concept-two'), item(3,'concept-three'),
  ] }), [event(1),event(2),event(3)], '2026-07-27');
  assert.equal(result.approved, true);
  assert.deepEqual(result.items.map((entry) => [entry.event.id, entry.conceptId]), [[1,'concept-one'],[3,'concept-three']]);
  assert.deepEqual(result.unreadCandidates.map((entry) => entry.conceptId), ['concept-one','concept-three']);
});

test('shadow, migration, failure and missing rollout baseline never create cards or a red dot', () => {
  for (const status of ['shadow','migration','failed','unavailable']) {
    const result = resolveUnusualFeed(manifest({ quality_gate:{ status, metrics:{} } }), [event(1)], '2026-07-27');
    assert.equal(result.approved, false);
    assert.deepEqual(result.items, []);
    assert.deepEqual(result.unreadCandidates, []);
  }
  const noBaseline = resolveUnusualFeed(manifest({ rollout_baseline_at:null }), [event(1)], '2026-07-27');
  assert.equal(noBaseline.items.length, 1);
  assert.deepEqual(noBaseline.unreadCandidates, []);
  const migrationItem = resolveUnusualFeed(manifest({ items:[item(1,'concept-one',{ notify_eligible:false })] }), [event(1)], '2026-07-27');
  assert.deepEqual(migrationItem.unreadCandidates, []);
});

test('the route uses EventCard and device-local seen state clears only on a viewed card or explicit action', async () => {
  const [surface, runtime, page, fallback] = await Promise.all([
    read('src/components/UnusualListingSurface.astro'),
    read('src/components/UnusualUnreadRuntime.astro'),
    read('src/pages/neobychnoe/index.astro'),
    read('src/data/unusual-events.json').then(JSON.parse),
  ]);
  assert.match(page, /<UnusualListingSurface feed=\{feed\}/u);
  assert.match(page, /noindex/u);
  assert.match(surface, /<EventCard event=\{item\.event\} mobileFlowMedia \/>/u);
  assert.match(surface, /IntersectionObserver/u);
  assert.match(surface, /entry\.intersectionRatio >= \.6/u);
  assert.match(surface, /setTimeout\(\(\) => commitViewed\(card\), 900\)/u);
  assert.match(surface, /kenigevents:unusual-viewed/u);
  assert.match(surface, /kenigevents:unusual-mark-seen/u);
  assert.match(runtime, /ke_unusual_seen_v1/u);
  assert.match(runtime, /<script is:inline>/u, 'unread binding must execute after its inline manifest and navigation DOM exist');
  assert.match(runtime, /DOMContentLoaded', syncDots/u, 'the first sync must include footer and page controls parsed after the runtime mount');
  assert.match(runtime, /maxSeen = 256/u);
  assert.match(runtime, /ttlMs = 180 \* 24 \* 60 \* 60 \* 1000/u);
  assert.doesNotMatch(runtime, /markSeen\([^)]*\)[\s\S]{0,80}(?:DOMContentLoaded|pageshow)/u);
  assert.equal(fallback.quality_gate.status, 'unavailable');
  assert.deepEqual(fallback.items, []);
});
