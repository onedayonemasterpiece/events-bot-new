import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { assertFreeCollectionStructuralProjection, stableHash } from '../scripts/current_ui_resource_graph/v1/specimens/validate.mjs';

const expectedSha = 'a'.repeat(40);
const hash = 'b'.repeat(64);
const expectedEventIds = ['1', '2', '3', '4', '5'];
function fixture() {
  const element = (path, family, attributes = {}, children = []) => ({
    kind: 'element', anatomy_path: path, stable_id: `free-collection.${stableHash(path).slice(0, 24)}`,
    parent_id: null, tag: 'div', attributes,
    identity: family ? { family, version: family === 'EventCard' ? '2' : '1', ...(family === 'FreeCollectionSurface' ? { variant: 'standard-free-listing' } : {}) } : null,
    containing_family: family, bounds: { x: 0, y: 0, width: 44, height: 44 }, computed: { display: 'block' }, children,
  });
  const cards = expectedEventIds.map((id) => element(`root/grid/${id}`, 'EventCard', {
    'data-event-card': '', 'data-event-id': id, 'data-calendar-eligible': 'true',
  }, [
    element(`root/grid/${id}/media`, null, { 'data-media-frame': '', 'data-media-frame-contract': 'v1',
      'data-media-frame-resource-state': 'fallback', 'data-media-frame-fallback': '' }),
    element(`root/grid/${id}/like`, null, { 'data-feedback-action': 'like' }),
    element(`root/grid/${id}/not`, null, { 'data-feedback-action': 'not_interested' }),
    element(`root/grid/${id}/share`, null, { 'data-native-share': '' }),
    element(`root/grid/${id}/calendar`, null, { 'data-calendar-action': '', href: '/ics/1.ics' }),
  ]));
  const tree = element('root', 'FreeCollectionSurface', {}, [element('root/grid', 'AdaptiveEventCardGrid', {}, cards)]);
  const parents = (node) => { for (const child of node.children) { child.parent_id = node.stable_id; parents(child); } };
  parents(tree);
  return { schema: 'current_ui_free_collection_structural_projection_v1', tree,
    provenance: { repo_sha: expectedSha, manifest: { repo_sha: expectedSha }, manifest_sha256: hash,
      registry_sha256: hash, snapshot: { id: 'synthetic-unit-test', sha256: hash }, reference_clock: '2026-09-04T12:00:00Z' },
    viewport: { width: 1440, height: 900 }, event_ids: expectedEventIds, sample_size: 5, catalog_total: 12, eligibility_filter: 'confirmed-free',
    source_bindings: ['FreeCollectionSurface', 'AdaptiveEventCardGrid', 'EventCard'].map((id) => ({
      id, version: id === 'EventCard' ? 2 : 1, path: `site/src/components/${id}.astro`, sha256: hash, styles: [],
    })),
  };
}

test('finite structural composition validates independently of native Penpot state', () => {
  assert.equal(assertFreeCollectionStructuralProjection(fixture(), { expectedSha, expectedEventIds, structuralOnly: true }).card_count, 5);
});
for (const [name, mutate] of [
  ['stale SHA', (r) => { r.provenance.repo_sha = 'c'.repeat(40); }],
  ['snapshot absent', (r) => { delete r.provenance.snapshot; }],
  ['clock absent', (r) => { delete r.provenance.reference_clock; }],
  ['changed event order', (r) => { r.event_ids = [...r.event_ids].reverse(); }],
  ['unresolved parent', (r) => { r.tree.children[0].parent_id = 'missing'; }],
  ['missing owner', (r) => { r.source_bindings.pop(); }],
  ['version mismatch', (r) => { r.tree.identity.version = '99'; }],
  ['missing media', (r) => { r.tree.children[0].children[0].children.shift(); }],
  ['negative geometry', (r) => { r.tree.bounds.width = -1; }],
  ['missing calendar href', (r) => { delete r.tree.children[0].children[0].children.at(-1).attributes.href; }],
]) test(`structural export rejects ${name}`, () => {
  const record = fixture(); mutate(record);
  assert.throws(() => assertFreeCollectionStructuralProjection(record, { expectedSha, expectedEventIds, structuralOnly: true }));
});
if (process.env.PROJECTION_PACKET) test('live extracted packet validates', () => {
  const record = JSON.parse(readFileSync(process.env.PROJECTION_PACKET, 'utf8'));
  assert.equal(assertFreeCollectionStructuralProjection(record, {
    expectedSha: record.provenance.repo_sha, expectedEventIds: record.event_ids, repoRoot: process.cwd(),
  }).penpot_round_trip, false);
});

if (process.env.PROJECTION_PACKET) test('live packet rejects forged source binding content', () => {
  const record = JSON.parse(readFileSync(process.env.PROJECTION_PACKET, 'utf8'));
  record.source_bindings[0].sha256 = '0'.repeat(64);
  assert.throws(() => assertFreeCollectionStructuralProjection(record, {
    expectedSha: record.provenance.repo_sha, expectedEventIds: record.event_ids, repoRoot: process.cwd(),
  }), /source binding content mismatch/u);
});
