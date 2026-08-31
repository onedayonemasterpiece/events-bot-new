import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {fileURLToPath} from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = relative => JSON.parse(fs.readFileSync(path.join(root, relative), 'utf8'));
const registry = read('route-archetype-registry.v1.json');
const corpus = read('corpus/shared-event-corpus.v1.json');
const state = read('state-packets/priority-listings.v1.json');
const projections = read('projections/priority-listings.v1.json');
const scenarios = ['desktop', 'mobile'].map(kind => read(`scenarios/priority-listings.${kind}.v1.json`));
const priority = new Set(['date-listing', 'today-listing', 'tomorrow-listing', 'weekend-listing', 'exhibitions']);

test('registry is an observed source-pinned census with all C0 archetype families', () => {
  assert.equal(registry.schema_version, 'kenigevents.ui-conformance.route-archetype-registry.v1');
  assert.equal(registry.source_sha, '64f75d10f7aff33fa616cee212878bd9d03673b1');
  assert.equal(registry.archetypes.length, 23);
  for (const item of registry.archetypes) {
    assert.ok(item.id && item.corpus_status);
    for (const source of item.source_files) assert.ok(fs.existsSync(path.join(root, '..', source)), `${item.id} source exists: ${source}`);
  }
  assert.equal(registry.archetypes.find(item => item.id === 'volunteer').corpus_status, 'BLOCKED_CORPUS_GAP');
});

test('shared corpus is append-only, source-pinned, and does not duplicate fixture identifiers', () => {
  assert.equal(corpus.append_only, true);
  const refs = new Set();
  for (const entity of corpus.entities) {
    assert.match(entity.fixture_id, /^event\.real\.\d+$/);
    assert.ok(!refs.has(entity.fixture_id), `unique fixture ${entity.fixture_id}`);
    refs.add(entity.fixture_id);
    assert.match(entity.source.commit, /^[0-9a-f]{40}$/);
    assert.equal(entity.event.is_public, true);
    assert.equal(entity.event.is_searchable, true);
  }
  const addition = corpus.entities.find(entity => entity.fixture_id === 'event.real.4240');
  assert.equal(addition.append_only_addition, true);
  assert.match(addition.addition_reason, /2026-09-01/);
});

test('priority projections reference only the shared corpus and ready C1 archetypes', () => {
  const entityRefs = new Set(corpus.entities.map(entity => entity.fixture_id));
  const ready = new Set(registry.archetypes.filter(item => item.corpus_status === 'READY_C1').map(item => item.id));
  assert.equal(projections.state_packet, state.packet_id);
  assert.equal(projections.projections.length, priority.size);
  for (const projection of projections.projections) {
    assert.ok(priority.has(projection.archetype_id));
    assert.ok(ready.has(projection.archetype_id));
    assert.ok(projection.entity_refs.length > 0);
    for (const ref of projection.entity_refs) assert.ok(entityRefs.has(ref), `${projection.id} resolves ${ref}`);
    assert.equal(Object.hasOwn(projection, 'entities'), false, 'projection cannot redeclare entities');
  }
});

test('deterministic packets and desktop/mobile scenarios cover every priority projection', () => {
  assert.equal(state.clock.timezone, 'Europe/Kaliningrad');
  assert.equal(state.clock.today, '2026-09-01');
  assert.equal(state.clock.tomorrow, '2026-09-02');
  const expected = projections.projections.map(projection => projection.id).sort();
  for (const scenario of scenarios) {
    assert.equal(scenario.state_packet, state.packet_id);
    assert.deepEqual([...scenario.projection_ids].sort(), expected);
    assert.ok(scenario.viewport.width > 0 && scenario.viewport.height > 0);
    assert.ok(scenario.assertions.every(assertion => !assertion.includes('geometry contract')));
  }
});
