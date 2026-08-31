import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {fileURLToPath} from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repositoryRoot = path.resolve(root, '..', '..');
const read = relative => JSON.parse(fs.readFileSync(path.join(root, relative), 'utf8'));
const registry = read('route-archetype-registry.v1.json');
const corpus = read('corpus/shared-event-corpus.v1.json');
const state = read('state-packets/priority-listings.v1.json');
const projections = read('projections/priority-listings.v1.json');
const receipt = read('receipts/c2-corpus-projection.v1.json');
const scenarios = ['desktop', 'mobile'].map(kind => read(`scenarios/priority-listings.${kind}.v1.json`));
const priority = new Set(['date-listing', 'today-listing', 'tomorrow-listing', 'weekend-listing', 'exhibitions']);
const sha256 = value => createHash('sha256').update(value).digest('hex');
const canonicalJson = value => {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
};
const canonicalSha256 = value => sha256(canonicalJson(value));

function clone(value) {
  return structuredClone(value);
}

function schemaErrors(schema, value, location = '$') {
  const errors = [];
  const fail = message => errors.push(`${location}: ${message}`);
  if (schema.oneOf) {
    const matches = schema.oneOf.filter(candidate => schemaErrors(candidate, value, location).length === 0);
    if (matches.length !== 1) fail(`expected exactly one oneOf match, got ${matches.length}`);
    return errors;
  }
  if (Object.hasOwn(schema, 'const') && canonicalJson(value) !== canonicalJson(schema.const)) fail('const mismatch');
  if (schema.enum && !schema.enum.some(candidate => canonicalJson(candidate) === canonicalJson(value))) fail('enum mismatch');
  if (schema.type) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    const actual = value === null ? 'null' : Array.isArray(value) ? 'array' : Number.isInteger(value) ? 'integer' : typeof value;
    const typeMatches = types.some(type => type === actual || (type === 'number' && typeof value === 'number'));
    if (!typeMatches) {
      fail(`expected type ${types.join('|')}, got ${actual}`);
      return errors;
    }
  }
  if (typeof value === 'string') {
    if (schema.minLength !== undefined && value.length < schema.minLength) fail(`minLength ${schema.minLength}`);
    if (schema.pattern && !(new RegExp(schema.pattern).test(value))) fail(`pattern ${schema.pattern}`);
  }
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) fail(`minItems ${schema.minItems}`);
    if (schema.uniqueItems && new Set(value.map(canonicalJson)).size !== value.length) fail('items must be unique');
    if (schema.items) value.forEach((item, index) => errors.push(...schemaErrors(schema.items, item, `${location}[${index}]`)));
  }
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    for (const key of schema.required ?? []) if (!Object.hasOwn(value, key)) fail(`missing required property ${key}`);
    for (const [key, item] of Object.entries(value)) {
      if (schema.properties?.[key]) errors.push(...schemaErrors(schema.properties[key], item, `${location}.${key}`));
      else if (schema.additionalProperties === false) fail(`unexpected property ${key}`);
    }
  }
  return errors;
}

function assertSchema(schema, value, label) {
  assert.deepEqual(schemaErrors(schema, value), [], `${label} must satisfy its committed JSON Schema`);
}

const pinnedCache = new Map();
function pinnedSource(sourceRef) {
  const spec = `${sourceRef.commit}:${sourceRef.path}`;
  if (!pinnedCache.has(spec)) {
    const bytes = execFileSync('git', ['show', spec], {cwd: repositoryRoot, maxBuffer: 32 * 1024 * 1024});
    pinnedCache.set(spec, {bytes, document: JSON.parse(bytes.toString('utf8'))});
  }
  return pinnedCache.get(spec);
}

function resolvePointer(document, pointer) {
  assert.match(pointer, /^\//);
  return pointer.slice(1).split('/').reduce((value, token) => value[token.replaceAll('~1', '/').replaceAll('~0', '~')], document);
}

function assertProjectionRefs(document) {
  const entityRefs = new Set(corpus.entities.map(entity => entity.fixture_id));
  for (const projection of document.projections) {
    for (const ref of projection.entity_refs) assert.ok(entityRefs.has(ref), `${projection.id} resolves ${ref}`);
  }
}

function assertDerivedFidelity(entity) {
  const source = resolvePointer(pinnedSource(entity.source_ref).document, entity.source_ref.json_pointer);
  assert.deepEqual(Object.keys(entity.projected_event), entity.projection_fields);
  for (const field of entity.projection_fields) assert.deepEqual(entity.projected_event[field], source[field], `${entity.fixture_id}.${field}`);
}

test('every committed JSON Schema executes against its paired document', () => {
  const pairs = new Map([
    ['contract-receipt.v1.schema.json', receipt],
    ['route-archetype-registry.v1.schema.json', registry],
    ['route-projections.v1.schema.json', projections],
    ['shared-event-corpus.v1.schema.json', corpus],
  ]);
  const committedSchemas = fs.readdirSync(path.join(root, 'schemas')).filter(name => name.endsWith('.schema.json')).sort();
  assert.deepEqual(committedSchemas, [...pairs.keys()].sort());
  for (const [schemaName, document] of pairs) assertSchema(read(`schemas/${schemaName}`), document, schemaName);
});

test('registry remains an observed source-pinned census with all C0 archetype families', () => {
  assert.equal(registry.source_sha, '64f75d10f7aff33fa616cee212878bd9d03673b1');
  assert.equal(registry.archetypes.length, 23);
  for (const item of registry.archetypes) {
    assert.ok(item.id && item.corpus_status);
    for (const source of item.source_files) assert.ok(fs.existsSync(path.join(root, '..', source)), `${item.id} source exists: ${source}`);
  }
  assert.equal(registry.archetypes.find(item => item.id === 'volunteer').corpus_status, 'BLOCKED_CORPUS_GAP');
});

test('reused fixtures are immutable source references, never lossy source-record copies', () => {
  assert.equal(corpus.append_only, true);
  assert.equal(corpus.hash_algorithm, 'sha256-jcs-lite-v1');
  const refs = new Set();
  for (const entity of corpus.entities) {
    assert.match(entity.fixture_id, /^event\.real\.\d+$/);
    assert.ok(!refs.has(entity.fixture_id), `unique fixture ${entity.fixture_id}`);
    refs.add(entity.fixture_id);
    assert.equal(Object.hasOwn(entity, 'event'), false);
    if (entity.fixture_id !== 'event.real.4240') {
      assert.equal(entity.record_mode, 'immutable_source_reference');
      assert.equal(Object.hasOwn(entity, 'projected_event'), false);
    }
  }
});

test('all pinned source files and records match their durable hashes', () => {
  for (const entity of corpus.entities) {
    const pinned = pinnedSource(entity.source_ref);
    const sourceRecord = resolvePointer(pinned.document, entity.source_ref.json_pointer);
    assert.equal(sha256(pinned.bytes), entity.source_ref.source_file_sha256, `${entity.fixture_id} source file hash`);
    assert.equal(canonicalSha256(sourceRecord), entity.source_ref.source_record_sha256, `${entity.fixture_id} source record hash`);
    assert.equal(sourceRecord.id, Number(entity.fixture_id.split('.').at(-1)));
  }
});

test('event.real.4240 projection is field-for-field faithful to the exact pinned source record', () => {
  const addition = corpus.entities.find(entity => entity.fixture_id === 'event.real.4240');
  assert.equal(addition.record_mode, 'derived_projection');
  assert.equal(addition.projection_name, 'priority-listing-fields.v1');
  assert.match(addition.addition_reason, /2026-09-01/);
  assertDerivedFidelity(addition);
  const source = resolvePointer(pinnedSource(addition.source_ref).document, addition.source_ref.json_pointer);
  assert.equal(source.venue_name, 'Филиал Третьяковской галереи, Парадная наб. 3, Калининград');
  assert.equal(source.end_date, '2026-10-01');
  assert.equal(addition.projected_event.venue_name, source.venue_name);
  assert.equal(addition.projected_event.end_date, source.end_date);
});

test('priority projections reference only the shared corpus and ready C1 archetypes', () => {
  const ready = new Set(registry.archetypes.filter(item => item.corpus_status === 'READY_C1').map(item => item.id));
  assert.equal(projections.corpus_id, corpus.corpus_id);
  assert.equal(projections.state_packet, state.packet_id);
  assert.equal(projections.projections.length, priority.size);
  for (const projection of projections.projections) {
    assert.ok(priority.has(projection.archetype_id));
    assert.ok(ready.has(projection.archetype_id));
    assert.equal(Object.hasOwn(projection, 'entities'), false);
  }
  assertProjectionRefs(projections);
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

test('cross-file receipt hashes bind every contract file and source record to its projections', () => {
  for (const entry of receipt.files) {
    assert.equal(sha256(fs.readFileSync(path.join(root, entry.path))), entry.sha256, entry.path);
  }
  assert.equal(canonicalSha256(receipt.files), receipt.files_manifest_sha256);
  const binding = {
    corpus_id: corpus.corpus_id,
    corpus_sha256: receipt.files.find(entry => entry.path === 'corpus/shared-event-corpus.v1.json').sha256,
    projection_sha256: receipt.files.find(entry => entry.path === 'projections/priority-listings.v1.json').sha256,
    source_records: corpus.entities.map(entity => ({fixture_id: entity.fixture_id, source_record_sha256: entity.source_ref.source_record_sha256})),
  };
  assert.equal(canonicalSha256(binding), receipt.source_projection_binding_sha256);
});

test('negative schemas reject lossy corpus copies, missing source hashes, and inline projection records', () => {
  const corpusSchema = read('schemas/shared-event-corpus.v1.schema.json');
  const lossy = clone(corpus);
  lossy.entities[0].event = {id: 8006};
  assert.ok(schemaErrors(corpusSchema, lossy).some(error => error.includes('oneOf')));
  const unhashed = clone(corpus);
  delete unhashed.entities[0].source_ref.source_record_sha256;
  assert.ok(schemaErrors(corpusSchema, unhashed).some(error => error.includes('oneOf')));
  const projectionSchema = read('schemas/route-projections.v1.schema.json');
  const inline = clone(projections);
  inline.projections[0].entities = [{id: 4240}];
  assert.ok(schemaErrors(projectionSchema, inline).some(error => error.includes('unexpected property entities')));
});

test('negative fidelity and referential checks reject drift from pinned sources', () => {
  const drifted = clone(corpus.entities.find(entity => entity.fixture_id === 'event.real.4240'));
  drifted.projected_event.end_date = '2026-09-30';
  assert.throws(() => assertDerivedFidelity(drifted), /event\.real\.4240\.end_date/);
  const dangling = clone(projections);
  dangling.projections[0].entity_refs = ['event.real.999999'];
  assert.throws(() => assertProjectionRefs(dangling), /resolves event\.real\.999999/);
});
