import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  AMBER_ARTIFACT_ID,
  ARTIFACT_COLLECTION_STORAGE_KEY,
  collectAmberArtifact,
  hasAmberArtifact,
  normalizeArtifactCollectionState,
  readArtifactCollection,
} from '../src/lib/artifactRuntime.mjs';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

function memoryStorage(seed = {}) {
  const values = new Map(Object.entries(seed));
  return {
    getItem(key) { return values.has(key) ? values.get(key) : null; },
    setItem(key, value) { values.set(key, String(value)); },
    dump() { return Object.fromEntries(values); },
  };
}

test('collection page renders one worked artifact and seven anonymous locked slots', async () => {
  const page = await read('../src/pages/artefakty/kollektsii/znaki-yantarnogo-kraya/index.astro');
  const component = await read('../src/components/artifacts/ArtifactCollectionProgress.astro');
  assert.match(page, /data-artifact-collection-page/u);
  assert.match(page, /Показан сценарий «найден 1 из 8»/u);
  assert.match(page, /Артефакт не убегает/u);
  assert.match(page, /После назначения артефакт ждёт пользователя в том же интерфейсном месте/u);
  assert.match(component, /Янтарный космонавт/u);
  assert.match(component, /amber-cosmonaut/u);
  assert.match(component, /Array\.from\(\{ length: lockedSlotCount \}/u);
  assert.match(component, /ни его название, ни изображение/u);
  assert.match(component, /aria-haspopup="dialog"/u);
  assert.match(component, /data-artifact-dialog/u);
  assert.doesNotMatch(component, /future_maritime|future_nature|future_city|future_taste/u);
});

test('artifact progress is idempotent and migrates the accepted prototype identifiers', () => {
  const storage = memoryStorage();
  const now = () => new Date('2026-07-27T20:00:00.000Z');
  const first = collectAmberArtifact({ storage, eventId: 6907, now });
  const second = collectAmberArtifact({ storage, eventId: 9999, now });
  assert.equal(first.collected, true);
  assert.equal(second.collected, false);
  assert.equal(hasAmberArtifact(second.state), true);
  assert.equal(second.state.artifacts[AMBER_ARTIFACT_ID].eventId, 6907);
  assert.ok(storage.dump()[ARTIFACT_COLLECTION_STORAGE_KEY]);

  const migrated = normalizeArtifactCollectionState({
    artifacts: {
      amber_cosmonaut: {
        status: 'found',
        foundAt: '2026-07-22T00:00:00.000Z',
        eventId: 5511,
        placement: 'weekend.rail.tail.v1',
      },
    },
  });
  assert.equal(hasAmberArtifact(migrated), true);

  const legacyStorage = memoryStorage({ 'ke_amber_artifact_prototype_v1:tail': 'found' });
  assert.equal(hasAmberArtifact(readArtifactCollection(legacyStorage, now)), true);
});

test('unfound accessible label does not reveal the artifact name', async () => {
  const rail = await read('../src/components/listings/AmberRailArtifact.astro');
  assert.match(rail, /aria-label="Секретный артефакт\. Нажмите, чтобы найти"/u);
  assert.match(rail, /Артефакт «Янтарный космонавт» найден\. Открыть историю/u);
  assert.match(rail, /artifactRuntime\.mjs/u);
  assert.match(rail, /znaki-yantarnogo-kraya/u);
});
