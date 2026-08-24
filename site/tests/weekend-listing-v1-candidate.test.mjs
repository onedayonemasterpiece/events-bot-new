import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const repo = resolve(import.meta.dirname, '..');
const projectionPath = resolve(repo, 'src/data/candidate/weekend-listing-v1.generated.json');
const projectionBytes = readFileSync(projectionPath);
const projection = JSON.parse(projectionBytes);
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

assert.equal(projection.generated, true);
assert.equal(projection.independently_editable, false);
assert.equal(projection.manifest_id, 'archetype.listing.weekend-v1.golden-v1');
assert.deepEqual(projection.fixtures.map((item) => item.fixture_id), ['event.real.7807','event.real.7906']);
assert.deepEqual(projection.representations.map((item) => item.id), ['typical-desktop','typical-mobile','sparse','empty','stress']);
assert.equal(projection.ranges.length, 6);
for (const fixture of projection.fixtures) {
  assert.equal(fixture.event_id, fixture.preview_event.id);
  assert.equal(fixture.preview_event.start_date, projection.weekend_range.start);
  assert.match(fixture.preview_event_sha256, /^[a-f0-9]{64}$/u);
}

const route = readFileSync(resolve(repo, 'src/pages/lab/weekend-listing-v1/[representation].astro'), 'utf8');
const candidate = readFileSync(resolve(repo, 'src/components/candidate/WeekendListingCandidateSurface.astro'), 'utf8');
const statePanel = readFileSync(resolve(repo, 'src/components/candidate/ListingCandidateStatePanel.astro'), 'utf8');
assert.match(route, /noindex=\{true\}/u);
assert.match(route, /data-fixture-source-sha256/u);
assert.doesNotMatch(route, /getWeekendEvents|getAvailableWeekendRanges/u);
assert.match(candidate, /WeekendListingSurface/u);
assert.match(candidate, /ListingCandidateStatePanel/u);
assert.match(candidate, /representation: 'typical-desktop' \| 'typical-mobile' \| 'sparse' \| 'empty' \| 'stress'/u);
assert.match(statePanel, /context: 'date' \| 'weekend'/u);
assert.match(statePanel, /На эти выходные событий пока нет/u);
assert.match(statePanel, /Другие выходные/u);

const sourceRepo = process.env.DESIGN_SYSTEM_REPO;
if (sourceRepo) {
  const source = readFileSync(resolve(sourceRepo, 'catalog/page-archetypes/weekend-listing-v1/fixture-manifest.v1.json'));
  assert.equal(sha256(source), projection.source_manifest_sha256);
}

console.log(`weekend-listing-v1-candidate.test: PASS (${sha256(projectionBytes).slice(0,12)} projection)`);
