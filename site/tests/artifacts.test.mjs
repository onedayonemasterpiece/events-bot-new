import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import {
  AMBER_ARTIFACT_ID,
  AMBER_ARTIFACT_PLACEMENT,
  ARTIFACT_COLLECTION_STORAGE_KEY,
  LEGACY_AMBER_STORAGE_KEY,
  collectAmberArtifact,
  hasAmberArtifact,
  isAmberArtifactResearchEnabled,
  readArtifactCollection,
  selectAmberArtifactEventId,
} from '../src/lib/artifactRuntime.mjs';

class MemoryStorage {
  constructor(entries = {}) {
    this.values = new Map(Object.entries(entries));
  }
  getItem(key) {
    return this.values.has(key) ? this.values.get(key) : null;
  }
  setItem(key, value) {
    this.values.set(key, String(value));
  }
}

const eligibleEvents = [
  { id: 17, title: 'Семнадцать', start_date: '2026-08-01' },
  { id: 4, title: 'Четыре', start_date: '2026-08-02' },
  { id: 92, title: 'Девяносто два', start_date: '2026-08-01' },
  { id: 31, title: 'Тридцать один', start_date: '2026-08-02' },
];

test('immutable build seed selects exactly one eligible current-weekend real event', () => {
  const options = { seed: 'preview-artifact-v1', start: '2026-08-01', end: '2026-08-02' };
  const selected = selectAmberArtifactEventId(eligibleEvents, options);
  assert.ok(eligibleEvents.some((event) => event.id === selected));
  assert.equal(
    selectAmberArtifactEventId([...eligibleEvents].reverse(), options),
    selected,
    'catalog order must not reroll the assignment',
  );
  assert.equal(
    selectAmberArtifactEventId([...eligibleEvents, eligibleEvents[0]], options),
    selected,
    'duplicate event rows must not create extra assignment weight',
  );
  const selections = new Set(Array.from({ length: 32 }, (_, index) =>
    selectAmberArtifactEventId(eligibleEvents, { ...options, seed: `build-${index}` })));
  assert.ok(selections.size > 1, 'different immutable build seeds should exercise multiple candidates');
});

test('assignment rejects non-current, invalid and empty candidates and production stays fail-closed', () => {
  const selected = selectAmberArtifactEventId([
    { id: 1, title: '', start_date: '2026-08-01' },
    { id: -2, title: 'invalid', start_date: '2026-08-01' },
    { id: 3, title: 'other weekend', start_date: '2026-08-08' },
  ], { seed: 'x', start: '2026-08-01', end: '2026-08-02' });
  assert.equal(selected, null);
  assert.equal(isAmberArtifactResearchEnabled('preview', 'tail'), true);
  assert.equal(isAmberArtifactResearchEnabled('secret_candidate', 'tail'), true);
  assert.equal(isAmberArtifactResearchEnabled('production', 'tail'), false);
  assert.equal(isAmberArtifactResearchEnabled('preview', ''), false);
});

test('collection writes one structured device-local find and keeps repeated collection idempotent', () => {
  const storage = new MemoryStorage();
  const now = () => new Date('2026-07-27T12:00:00.000Z');
  const first = collectAmberArtifact({ storage, eventId: 17, placement: AMBER_ARTIFACT_PLACEMENT, now });
  assert.equal(first.collected, true);
  assert.equal(hasAmberArtifact(first.state), true);
  assert.deepEqual(JSON.parse(storage.getItem(ARTIFACT_COLLECTION_STORAGE_KEY)), {
    schemaVersion: 1,
    collectionId: 'kaliningrad_artifacts_v1',
    artifacts: {
      [AMBER_ARTIFACT_ID]: {
        status: 'found',
        foundAt: '2026-07-27T12:00:00.000Z',
        eventId: 17,
        placement: AMBER_ARTIFACT_PLACEMENT,
      },
    },
  });
  const repeated = collectAmberArtifact({ storage, eventId: 92, now });
  assert.equal(repeated.collected, false);
  assert.equal(repeated.state.artifacts[AMBER_ARTIFACT_ID].eventId, 17);
});

test('legacy placement bit migrates without a server and storage failures keep current-page state usable', () => {
  const legacy = new MemoryStorage({ [LEGACY_AMBER_STORAGE_KEY]: 'found' });
  const migrated = readArtifactCollection(legacy, () => new Date('2026-07-27T12:05:00.000Z'));
  assert.equal(hasAmberArtifact(migrated), true);
  assert.equal(migrated.artifacts[AMBER_ARTIFACT_ID].eventId, null);
  assert.ok(legacy.getItem(ARTIFACT_COLLECTION_STORAGE_KEY));
  assert.equal(readArtifactCollection(legacy).artifacts[AMBER_ARTIFACT_ID].eventId, null);

  const failing = {
    getItem() { throw new Error('storage disabled'); },
    setItem() { throw new Error('storage disabled'); },
  };
  const result = collectAmberArtifact({
    storage: failing,
    eventId: 31,
    now: () => new Date('2026-07-27T12:10:00.000Z'),
  });
  assert.equal(result.collected, true);
  assert.equal(hasAmberArtifact(result.state), true);
});

test('collection surface has eight private slots, accessible detail and a feedback route', async () => {
  const [component, page, rail, productionCheck] = await Promise.all([
    readFile(new URL('../src/components/artifacts/ArtifactCollectionProgress.astro', import.meta.url), 'utf8'),
    readFile(new URL('../src/pages/artefakty/kollektsii/znaki-yantarnogo-kraya/index.astro', import.meta.url), 'utf8'),
    readFile(new URL('../src/components/listings/AmberRailArtifact.astro', import.meta.url), 'utf8'),
    readFile(new URL('../scripts/check-production.mjs', import.meta.url), 'utf8'),
  ]);
  assert.match(component, /Array\.from\(\{ length: lockedSlotCount \}/u);
  assert.match(component, /ни его название, ни изображение/u);
  assert.match(component, /aria-haspopup="dialog"/u);
  assert.match(component, /<dialog[\s\S]*aria-labelledby="artifact-detail-title"[\s\S]*aria-describedby="artifact-detail-copy"/u);
  assert.match(component, /mailto:info@kenigevents\.ru/u);
  assert.match(component, /lastTrigger\?\.focus\?\.\(\)/u);
  assert.doesNotMatch(component, /\bfetch\s*\(|supabase|XMLHttpRequest/iu);
  assert.match(page, /\bnoindex\b/u);
  assert.match(page, /ArtifactCollectionProgress collection=\{collection\} demoFound/u);
  assert.match(page, /Показан сценарий «найден 1 из 8»/u);
  assert.match(rail, /data-artifact-detail-url/u);
  assert.match(rail, /location\.assign/u);
  assert.match(productionCheck, /artifact research leaked into production weekend listing/u);
  assert.match(productionCheck, /artifact collection leaked into production/u);
});
