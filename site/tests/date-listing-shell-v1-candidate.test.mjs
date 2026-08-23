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
const mobileRail = readFileSync(resolve(repo, 'src/components/listings/MobileListingRailSurface.astro'), 'utf8');
const mobileDateAccessory = readFileSync(resolve(repo, 'src/components/listings/MobileDateAccessory.astro'), 'utf8');
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

// These are independently materialized Date chip states, not incidental pixels
// inside a page-level screenshot. Keep the actual Astro state surface explicit
// so Penpot leaf/composite correction cannot silently omit disabled, wide range,
// selected-shadow, or month-boundary behavior.
assert.match(mobileRail, /\.date-chip--disabled\{opacity:\.48/u);
assert.match(mobileRail, /\.date-chip--weekend-range\{flex-basis:74px;width:74px\}/u);
assert.match(mobileRail, /\.date-chip\[aria-current=date\]\{background:var\(--rail-primary\);color:#fff;box-shadow:0 5px 12px rgba\(121,48,20,\.18\);opacity:1\}/u);
assert.match(mobileRail, /\.date-calendar-trigger\{[^}]*border-left:1px solid rgba\(121,48,20,\.12\)[^}]*box-shadow:-8px 0 12px rgba\(255,253,248,\.94\)/u);
assert.match(mobileDateAccessory, /aria-disabled="true"/u);
assert.match(mobileDateAccessory, /index === 0 \|\| item\.day === 1 \? item\.month : ''/u);
assert.match(mobileDateAccessory, /M7 2a1 1 0 0 1 1 1v1h8V3/u);

const sourceRepo = process.env.DESIGN_SYSTEM_REPO;
if (sourceRepo) {
  const source = readFileSync(resolve(sourceRepo, 'catalog/page-archetypes/date-listing-shell-v1/fixture-manifest.v1.json'));
  assert.equal(sha256(source), projection.source_manifest_sha256);
}

console.log(`date-listing-shell-v1-candidate.test: PASS (${sha256(projectionBytes).slice(0,12)} projection)`);
