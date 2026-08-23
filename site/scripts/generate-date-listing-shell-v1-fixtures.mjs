import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const args = process.argv.slice(2);
const valueAfter = (flag) => {
  const index = args.indexOf(flag);
  return index >= 0 ? args[index + 1] : null;
};
const designRepo = resolve(valueAfter('--design-repo') || process.env.DESIGN_SYSTEM_REPO || '');
if (!designRepo || designRepo === resolve('')) {
  throw new Error('Pass --design-repo or DESIGN_SYSTEM_REPO; generated fixtures may not invent a second source.');
}

const sourceBase = join(designRepo, 'catalog/page-archetypes/date-listing-shell-v1');
const sourceManifestPath = join(sourceBase, 'fixture-manifest.v1.json');
const sourceManifestBytes = readFileSync(sourceManifestPath);
const sourceManifest = JSON.parse(sourceManifestBytes);
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

const fixtures = sourceManifest.fixtures.map((entry) => {
  const payloadPath = join(designRepo, entry.payload_path);
  const payloadBytes = readFileSync(payloadPath);
  if (sha256(payloadBytes) !== entry.payload_file_sha256) {
    throw new Error(`Fixture payload hash mismatch: ${entry.fixture_id}`);
  }
  const payload = JSON.parse(payloadBytes);
  if (payload.fixture_id !== entry.fixture_id || payload.preview_event_sha256 !== entry.preview_event_sha256) {
    throw new Error(`Fixture identity mismatch: ${entry.fixture_id}`);
  }
  return {
    fixture_id: entry.fixture_id,
    event_id: entry.event_id,
    preview_event_sha256: entry.preview_event_sha256,
    payload_file_sha256: entry.payload_file_sha256,
    preview_event: payload.preview_event,
  };
});

const projection = {
  schema_version: 'date-listing-shell-astro-projection.v1',
  generated: true,
  independently_editable: false,
  source_repository: 'onedayonemasterpiece/lovekgd-design-system',
  source_manifest_path: 'catalog/page-archetypes/date-listing-shell-v1/fixture-manifest.v1.json',
  source_manifest_sha256: sha256(sourceManifestBytes),
  manifest_id: sourceManifest.manifest_id,
  manifest_version: sourceManifest.version,
  source_corpus: sourceManifest.source_corpus,
  representations: sourceManifest.representations,
  fixtures,
};

const output = resolve(valueAfter('--output') || 'src/data/candidate/date-listing-shell-v1.generated.json');
mkdirSync(dirname(output), { recursive: true });
writeFileSync(output, `${JSON.stringify(projection, null, 2)}\n`);
console.log(JSON.stringify({ output, fixtures: fixtures.length, source_manifest_sha256: projection.source_manifest_sha256 }));
