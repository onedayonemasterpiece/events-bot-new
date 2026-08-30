import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const readJson = async (relative) => JSON.parse(await readFile(new URL(relative, import.meta.url), 'utf8'));
const registry = await readJson('../src/data/design-system-reference-fixtures.json');
const catalog = await readJson('../src/data/preview-events.json');
const route = await readFile(new URL('../src/pages/podborki/[slug]/index.astro', import.meta.url), 'utf8');
const surface = await readFile(new URL('../src/components/FreeCollectionSurface.astro', import.meta.url), 'utf8');
const resolver = await readFile(new URL('../src/data/designSystemReferenceFixtures.ts', import.meta.url), 'utf8');

test('free collection has one explicit five-event archetype scenario backed by factual payloads', () => {
  const scenario = registry.scenarios['free-collection-5-desktop-v1'];
  assert.equal(registry.schema_version, 'design-system-reference-fixtures.v2');
  assert.equal(registry.profile_id, 'design-system-reference-v2');
  assert.equal(registry.authority.ui_sot_registry_id, 'design-system-reference-v1');
  assert.equal(registry.authority.ui_sot_contract, 'lovekgd-design-system:catalog/fixtures/design-system-reference/v1/registry.v1.json');
  assert.match(registry.authority.ui_sot_contract_sha256, /^[0-9a-f]{64}$/u);
  assert.match(registry.authority.ui_sot_scenario_sha256, /^[0-9a-f]{64}$/u);
  assert.deepEqual(scenario.event_ids, [7030, 7006, 6901, 6996, 6997]);
  assert.equal(scenario.expected_card_count, 5);
  assert.equal(new Set(scenario.event_ids).size, 5);
  assert.equal(scenario.route, '/podborki/besplatnye-sobytiya/');
  const byId = new Map(catalog.events.map((event) => [event.id, event]));
  for (const id of scenario.event_ids) {
    assert.ok(byId.has(id), `event ${id} exists in the frozen preview snapshot`);
    assert.equal(byId.get(id).ticket.is_free, true, `event ${id} is factually free`);
  }
  assert.equal(byId.get(6996).image_url, null, 'the set keeps one real missing-media fallback');
});

test('the real collection route activates the scenario through the shared resolver', () => {
  assert.match(route, /getActiveDesignFixtureScenario/u);
  assert.match(route, /selectExactScenarioEvents/u);
  assert.match(route, /PUBLIC_UI_SOT_SCENARIO/u);
  assert.match(route, /fixtureScenarioId=\{activeFixture\?\.id\}/u);
  assert.match(surface, /data-ui-fixture-scenario=\{fixtureScenarioId\}/u);
  assert.match(resolver, /\['production', 'secret_candidate', 'secret-candidate'\]\.includes\(siteMode\)/u);
  assert.doesNotMatch(route, /\[7030,\s*7006,\s*6901,\s*6996,\s*6997\]/u, 'route must not own a page-local fixture array');
});

test('container families remain semantically distinct instead of collapsing into one generic packed row', () => {
  const families = registry.container_families;
  assert.deepEqual(Object.keys(families), [
    'event_card_equal_height_grid',
    'desktop_listing_rows',
    'festival_timeline_rows',
    'interest_club_grid',
  ]);
  assert.equal(families.event_card_equal_height_grid.card_family, 'EventCard@2');
  assert.equal(families.desktop_listing_rows.card_family, 'ListingEventCard@9');
  assert.equal(families.festival_timeline_rows.card_family, 'FestivalCard');
  assert.equal(families.interest_club_grid.card_family, 'InterestClubCard@1');
});
