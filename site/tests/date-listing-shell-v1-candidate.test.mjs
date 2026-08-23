import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const repo = resolve(import.meta.dirname, '..');
const projectionPath = resolve(repo, 'src/data/candidate/date-listing-shell-v1.generated.json');
const projectionBytes = readFileSync(projectionPath);
const projection = JSON.parse(projectionBytes);
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

assert.equal(projection.generated, true);
assert.equal(projection.independently_editable, false);
assert.equal(projection.manifest_id, 'archetype.listing.date-shell-v1.golden-v1');
assert.equal(projection.fixtures.length, 7);
assert.deepEqual(projection.representations.map((item) => item.order), [1,2,3,4,5,6,7]);
assert.equal(new Set(projection.fixtures.map((item) => item.fixture_id)).size, 7);
for (const fixture of projection.fixtures) {
  assert.equal(fixture.event_id, fixture.preview_event.id);
  assert.match(fixture.preview_event_sha256, /^[a-f0-9]{64}$/u);
  assert.match(fixture.payload_file_sha256, /^[a-f0-9]{64}$/u);
}

const route = readFileSync(resolve(repo, 'src/pages/lab/date-listing-shell-v1/[representation].astro'), 'utf8');
const candidate = readFileSync(resolve(repo, 'src/components/candidate/DateListingCandidateSurface.astro'), 'utf8');
assert.match(route, /noindex=\{true\}/u);
assert.match(route, /data-fixture-source-sha256/u);
assert.match(route, /loading','empty','error/u);
assert.doesNotMatch(route, /getTodayPrimaryEvents|getTomorrowEvents|getDateEvents/u);
assert.doesNotMatch(candidate, /import DateListingSurface/u);
assert.match(candidate, /data-candidate-composition="sot-linked-primitives"/u);
assert.match(candidate, /MobileListingRailSurface/u);
assert.match(candidate, /ExactTimeTimeline/u);
assert.doesNotMatch(candidate, /date-listing-candidate__state[\s\S]*?<header>/u);
assert.match(candidate, /width:360px; height:240px/u);

const sourceRepo = process.env.DESIGN_SYSTEM_REPO;
if (sourceRepo) {
  const source = readFileSync(resolve(sourceRepo, 'catalog/page-archetypes/date-listing-shell-v1/fixture-manifest.v1.json'));
  assert.equal(sha256(source), projection.source_manifest_sha256);
}

console.log(`date-listing-shell-v1-candidate.test: PASS (${sha256(projectionBytes).slice(0,12)} projection)`);
