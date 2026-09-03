import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('post-FR0 ownership manifest fixes the exact cutover and M0 source contour', async () => {
  const manifest = JSON.parse(await read('tests/m0-post-fr0-test-ownership.v1.json'));

  assert.equal(manifest.schema_version, 'kenigevents.m0-post-fr0-test-ownership.v1');
  assert.equal(manifest.contract_version, '1.10.0');
  assert.equal(manifest.role, 'M0');
  assert.equal(manifest.cutover.fr0_cutover_base, 'c71351decdcee02941acb26c5e2fbaf88faf0378');
  assert.equal(manifest.cutover.m0_downstream_base, '5eeaba09b5ec432a77ff899ce98fb8b9f492c133');
  assert.deepEqual(manifest.m0_owned_sources, [
    'site/src/components/EventCard.astro',
    'site/src/components/listings/ListingEventCard.astro',
    'site/src/components/AdaptiveEventCardGrid.astro',
    'site/src/components/OptimizedEventCardGrid.astro',
    'site/src/lib/relatedCardLayout.mjs',
  ]);
});

test('mixed historical suite now reads only M0 card-grid-row-layout sources', async () => {
  const source = await read('tests/ui-normalization-m0-contract.test.mjs');

  for (const retained of [
    'src/lib/relatedCardLayout.mjs',
    'src/components/OptimizedEventCardGrid.astro',
    'src/components/AdaptiveEventCardGrid.astro',
    'src/components/EventCard.astro',
    'src/components/listings/ListingEventCard.astro',
  ]) assert.ok(source.includes(retained), `missing retained M0 source: ${retained}`);

  for (const transferred of [
    'EventMediaRail.astro',
    'media-frame.css',
    'EventHero.astro',
    'DesktopEventPage.astro',
    'MobileListingRailRow.astro',
  ]) assert.ok(!source.includes(transferred), `mixed M0 suite still owns transferred domain: ${transferred}`);
});

test('FR0 equivalents existed before framing assertions left the mixed M0 suite', async () => {
  const manifest = JSON.parse(await read('tests/m0-post-fr0-test-ownership.v1.json'));

  assert.equal(manifest.fr0_equivalent_verification.head, '2231e1d668f896d634e5663b59520bc710d5fea6');
  assert.equal(manifest.fr0_equivalent_verification.result_comment, 5530977687);
  assert.equal(manifest.fr0_equivalent_verification.coverage_exists_before_mixed_test_transfer, true);
  assert.deepEqual(
    manifest.fr0_transferred_test_domains.map((entry) => entry.fr0_equivalent),
    [
      'site/tests/fr0-media-rail-fallback-contract.test.mjs',
      'site/tests/fr0-media-rail-clip-owner.test.mjs',
      'site/tests/fr0-event-hero-resource-fallback.test.mjs',
      'site/tests/fr0-exhibitions-media-frame-contract.test.mjs',
    ],
  );
  assert.equal(manifest.coverage_policy.delete_pre_cutover_coverage, false);
  assert.equal(manifest.coverage_policy.fr0_equivalents_must_be_integrated_before_old_framing_suites_are_retired, true);
});

test('consumer-level assertions leave M0 tests only after equivalent coverage exists', async () => {
  const [manifest, iconTest, targetTest, mobileRailTest] = await Promise.all([
    read('tests/m0-post-fr0-test-ownership.v1.json').then(JSON.parse),
    read('tests/m0-card-icon-role-contract.test.mjs'),
    read('tests/event-card-control-target.test.mjs'),
    read('tests/mobile-listing-rails.test.mjs'),
  ]);

  assert.deepEqual(
    manifest.consumer_transferred_test_domains.map((entry) => entry.external_equivalent),
    [
      'site/tests/mobile-listing-rails.test.mjs',
      'site/tests/n0-v0-golden-drift-acceptance.test.mjs',
    ],
  );
  assert.ok(manifest.consumer_transferred_test_domains.every((entry) => entry.coverage_exists_before_transfer));

  assert.doesNotMatch(iconTest, /MobileListingRailRow|foundations\.ts|semanticRoles\(mobile\)/u);
  assert.ok(mobileRailTest.includes('MobileListingRailRow.astro'));
  assert.ok(mobileRailTest.includes('SemanticIcon'));
  for (const role of ['feature', 'inline', 'action']) {
    assert.ok(mobileRailTest.includes(`role=\"${role}\"`), `mobile rail replacement misses ${role} role coverage`);
  }

  assert.doesNotMatch(targetTest, /EventLayout\.astro|styles\/design-system\.css|legacyCompactRule/u);
  assert.ok(targetTest.includes('--ke-control-min'));
  assert.ok(targetTest.includes('44px'));
  assert.equal(manifest.coverage_policy.consumer_equivalents_must_exist_before_cross_role_assertions_leave_M0_tests, true);
});

test('the retained EventLayout bridge check is explicitly read-only integration evidence', async () => {
  const [manifest, source] = await Promise.all([
    read('tests/m0-post-fr0-test-ownership.v1.json').then(JSON.parse),
    read('tests/event-card-flex-placement.test.mjs'),
  ]);
  const integration = manifest.cross_role_read_only_integration_contracts[0];

  assert.equal(integration.path, 'site/tests/event-card-flex-placement.test.mjs');
  assert.equal(integration.reads_external_path, 'site/src/layouts/EventLayout.astro');
  assert.equal(integration.external_writer_owner, 'A0');
  assert.equal(integration.m0_authority, 'READ_ONLY_NEGATIVE_INTEGRATION_CHECK');
  assert.equal(integration.m0_source_mutation_authorized, false);
  assert.match(source, /read\('src\/layouts\/EventLayout\.astro'\)/u);
});

test('M0 closes card requirements and leaves the mobile rail packet external', async () => {
  const manifest = JSON.parse(await read('tests/m0-post-fr0-test-ownership.v1.json'));

  assert.deepEqual(
    manifest.closed_fr0_api_requirements.map((entry) => entry.comment).sort((a, b) => a - b),
    [5530895109, 5530953025],
  );
  assert.equal(manifest.external_consumer_requirement.comment, 5530928932);
  assert.equal(manifest.external_consumer_requirement.status, 'OUTSIDE_POST_CUTOVER_M0_WRITABLE_CONTOUR');
  assert.match(manifest.external_consumer_requirement.m0_action, /No parallel source edit/u);
});

test('all retained M0 tests named by the ownership manifest exist on this branch', async () => {
  const manifest = JSON.parse(await read('tests/m0-post-fr0-test-ownership.v1.json'));
  const tests = [...new Set(manifest.m0_retained_test_domains.flatMap((entry) => entry.tests))];

  await Promise.all(tests.map((path) => read(path.replace(/^site\//u, ''))));
});
