import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const siteRoot = path.resolve(scriptDir, '..');
const fixturePath = path.join(siteRoot, 'src/data/editorial-collections/unusual-pilot-v1.json');
const pagePath = path.join(siteRoot, 'src/pages/lab/editorial-collections/index.astro');

function fail(message) {
  console.error(`editorial-collections-lab: FAIL: ${message}`);
  process.exitCode = 1;
}

function assert(condition, message) {
  if (!condition) fail(message);
}

const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
const page = fs.readFileSync(pagePath, 'utf8');

assert(fixture.schema_version === 'editorial-collection-pilot-v1', 'unexpected fixture schema');
assert(fixture.collection?.id === 'unusual-events-editorial-pilot-v1', 'unexpected collection id');
assert(fixture.collection?.indexing_status === 'lab_noindex', 'fixture must remain lab_noindex');
assert(Array.isArray(fixture.concepts) && fixture.concepts.length === 9, 'expected exactly 9 concepts');
assert(Array.isArray(fixture.sections) && fixture.sections.length === 4, 'expected exactly 4 editorial sections');

const conceptIds = fixture.concepts.map((concept) => concept.concept_id);
assert(new Set(conceptIds).size === conceptIds.length, 'concept ids must be unique');

const occurrences = fixture.concepts.flatMap((concept) => concept.occurrences || []);
assert(occurrences.length === 21, `expected 21 occurrences, got ${occurrences.length}`);
assert(new Set(occurrences.map((occurrence) => occurrence.event_id)).size === 21, 'occurrence event ids must be unique');

const families = new Set(fixture.concepts.map((concept) => concept.primary_family));
assert(families.size === 7, `expected 7 represented families, got ${families.size}`);

for (const concept of fixture.concepts) {
  assert(typeof concept.fact_summary === 'string' && concept.fact_summary.length > 20, `${concept.concept_id}: missing fact_summary`);
  assert(typeof concept.why_selected === 'string' && concept.why_selected.length > 20, `${concept.concept_id}: missing why_selected`);
  assert(typeof concept.best_for === 'string' && concept.best_for.length > 20, `${concept.concept_id}: missing best_for`);
  assert(typeof concept.caveat === 'string' && concept.caveat.length > 20, `${concept.concept_id}: missing caveat`);
  assert(concept.verification?.status, `${concept.concept_id}: missing verification status`);
}

const assignedIds = fixture.sections.flatMap((section) => section.concept_ids || []);
assert(assignedIds.length === 9, 'sections must assign exactly 9 concepts');
assert(new Set(assignedIds).size === 9, 'a concept is assigned to more than one section');
assert(assignedIds.every((id) => conceptIds.includes(id)), 'section references unknown concept');
assert(conceptIds.every((id) => assignedIds.includes(id)), 'a concept is missing from sections');

const controls = new Map((fixture.controls || []).map((control) => [control.event_id, control.kind]));
assert(controls.get(7153) === 'hard_negative', 'missing organ hard negative 7153');
assert(controls.get(6885) === 'non_event', 'missing non-event control 6885');

const expectedVariants = [
  'editorial-scan',
  'chapter-stream',
  'compact-first',
  'decision-board',
  'magazine-cover',
  'story-deck',
];
const variantIds = fixture.visual_lab?.variants?.map((variant) => variant.id) || [];
assert(JSON.stringify(variantIds) === JSON.stringify(expectedVariants), 'unexpected variant registry');

assert(page.includes('<meta name="robots" content="noindex,nofollow"'), 'Astro lab must remain noindex,nofollow');
assert(page.includes('unusual-pilot-v1.json'), 'Astro lab must import the frozen fixture');
assert(page.includes('data-density="explained"'), 'missing explained/compact density contract');
assert(page.includes('id="full-index"'), 'missing shared full index');
assert(page.includes('prefers-reduced-motion: reduce'), 'missing reduced-motion treatment');
for (const variant of expectedVariants) {
  assert(page.includes(`data-variant-panel="${variant}"`), `missing panel for ${variant}`);
}

if (!process.exitCode) {
  console.log(`editorial-collections-lab: PASS (${fixture.concepts.length} concepts, ${occurrences.length} occurrences, ${families.size} families, ${expectedVariants.length} variants)`);
}
