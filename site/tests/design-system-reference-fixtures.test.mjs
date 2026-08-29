import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const readJson = async (relative) => JSON.parse(await readFile(new URL(relative, import.meta.url), 'utf8'));
const registry = await readJson('../src/data/design-system-reference-fixtures.json');
const frozen = await readJson('../src/data/ui-reference-events-v2.json');
const route = await readFile(new URL('../src/pages/podborki/[slug]/index.astro', import.meta.url), 'utf8');
const surface = await readFile(new URL('../src/components/FreeCollectionSurface.astro', import.meta.url), 'utf8');
const resolver = await readFile(new URL('../src/data/designSystemReferenceFixtures.ts', import.meta.url), 'utf8');
const generator = await readFile(new URL('../scripts/build-design-system-reference-fixtures-v3.py', import.meta.url), 'utf8');

test('September free collection consumes the exact diverse Golden Event Corpus v2 projection', () => {
  const scenario = registry.scenarios['free-collection-september-desktop-v2'];
  assert.equal(registry.schema_version, 'design-system-reference-fixtures.v3');
  assert.equal(registry.profile_id, 'design-system-reference-v3');
  assert.equal(registry.authority.ui_sot_registry_id, 'design-system-reference-v2');
  assert.equal(registry.authority.ui_sot_contract, 'lovekgd-design-system:catalog/fixtures/design-system-reference/v2/registry.v2.json');
  assert.match(registry.authority.ui_sot_contract_sha256, /^[0-9a-f]{64}$/u);
  assert.match(registry.authority.ui_sot_scenario_sha256, /^[0-9a-f]{64}$/u);
  assert.equal(frozen.authority.registry_sha256, registry.authority.ui_sot_contract_sha256);
  assert.equal(frozen.authority.scenario_sha256, registry.authority.ui_sot_scenario_sha256);

  assert.deepEqual(scenario.event_ids, [2182, 6711, 7609, 8006, 8200]);
  assert.deepEqual(scenario.expected_render_order, [8006, 8200, 2182, 6711, 7609]);
  assert.equal(scenario.expected_card_count, 5);
  assert.equal(new Set(scenario.event_ids).size, 5);
  assert.equal(scenario.route, '/podborki/besplatnye-sobytiya/');
  assert.equal(scenario.updated_date, '2026-08-29');
  assert.deepEqual(frozen.projection.fixture_input_order, scenario.event_ids.map((id) => `event.real.${id}`));

  const byId = new Map(frozen.fixtures.map((fixture) => [fixture.event_id, fixture]));
  const registryById = new Map(registry.events.fixtures.map((fixture) => [fixture.event_id, fixture]));
  for (const id of scenario.event_ids) {
    const fixture = byId.get(id);
    assert.ok(fixture, `event ${id} exists in the frozen generated consumer`);
    assert.equal(fixture.preview_event.ticket.is_free, true, `event ${id} is factually free`);
    assert.match(fixture.preview_event_sha256, /^[0-9a-f]{64}$/u);
    assert.equal(fixture.preview_event_sha256, registryById.get(id).preview_event_sha256, `event ${id} payload pin is identical across generated consumers`);
  }

  assert.equal(byId.get(2182).preview_event.image_text_mode, 'visual_only');
  assert.equal(byId.get(2182).preview_event.safe_crop, true);
  assert.equal(byId.get(6711).preview_event.image_assets.length, 4, 'one visual-only gallery is present');
  assert.equal(byId.get(7609).preview_event.image_text_mode, 'ocr_text');
  assert.equal(byId.get(7609).preview_event.image_assets[0].width, byId.get(7609).preview_event.image_assets[0].height, 'one square OCR poster is present');
  assert.ok(byId.get(8006).preview_event.image_assets[0].height > byId.get(8006).preview_event.image_assets[0].width, 'one portrait OCR poster is present');
  assert.ok(byId.get(8200).preview_event.image_assets[0].height > byId.get(8200).preview_event.image_assets[0].width, 'one program/document poster is present');
});

test('the repeated green Chernyakhovsk programme poster is an explicit fail-closed exclusion', () => {
  const excluded = new Set(frozen.projection.explicit_exclusions.flatMap((row) => row.asset_keys));
  assert.equal(excluded.size, 4);
  for (const fixture of frozen.fixtures.filter((row) => registry.events.archetype_fixture_ids.includes(row.event_id))) {
    for (const asset of fixture.preview_event.image_assets) {
      assert.ok(![...excluded].some((key) => asset.src.includes(key)), `event ${fixture.event_id} does not use a rejected green poster`);
    }
  }
});

test('the real collection route activates the scenario through the generated bridge only', () => {
  assert.match(route, /getActiveDesignFixtureScenario/u);
  assert.match(route, /selectExactScenarioEvents/u);
  assert.match(route, /PUBLIC_UI_SOT_SCENARIO/u);
  assert.match(route, /fixtureScenarioId=\{activeFixture\?\.id\}/u);
  assert.match(surface, /data-ui-fixture-scenario=\{fixtureScenarioId\}/u);
  assert.match(resolver, /ui-reference-events-v2\.json/u);
  assert.match(resolver, /\['production', 'secret_candidate', 'secret-candidate'\]\.includes\(siteMode\)/u);
  assert.doesNotMatch(route, /2182|6711|7609|8006|8200/u, 'route must not own fixture IDs');
  assert.match(generator, /Routes select scenario IDs/u);
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
