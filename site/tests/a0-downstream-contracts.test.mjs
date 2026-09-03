import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const readJson = async (relativePath) => JSON.parse(await read(relativePath));

const BINDINGS_PATH = 'src/data/a0-downstream-bindings.v1.json';
const BATCH_PATH = 'src/data/a0-r0-mechanical-batch.v1.json';

test('A0 executable source links one machine-readable thin-S, Penpot, V0 and rollback contour', async () => {
  const index = await readJson(BINDINGS_PATH);

  assert.equal(index.schema, 'kenigevents.a0.downstream-bindings-index.v1');
  assert.equal(index.status, 'SOURCE_BOUND_AWAITING_INTEGRATION');
  assert.equal(index.source.role_branch, 'work/ui-normalization-a0-wave-3-20260903');
  assert.equal(index.source.candidate_branch, 'r0/ui-normalization-current-candidate-20260903');
  assert.equal(index.thin_s.route_bindings, 'bindings/launch-normalization/a0-route-family-bindings.v1.json');
  assert.equal(index.penpot.spec, 'bindings/launch-normalization/a0-penpot-ready-spec.v1.json');
  assert.equal(index.v0.acceptance_matrix, 'bindings/launch-normalization/a0-v0-acceptance-matrix.v1.json');
  assert.equal(index.coverage_and_rollback.spec, 'bindings/launch-normalization/a0-coverage-rollback.v1.json');

  const coverage = index.coverage_and_rollback;
  assert.equal(coverage.source_converged + coverage.mechanical_pending + coverage.product_decision_pending, coverage.total_grouped_route_families);
  assert.equal(coverage.total_grouped_route_families, 9);
  assert.equal(coverage.browser_pass, 0);
  assert.equal(coverage.penpot_linked, 0);
  assert.equal(coverage.strict_a_equals_s_equals_p, 0);

  for (const [boundary, value] of Object.entries(index.evidence_boundaries)) {
    if (boundary === 'v0_pass_requires_exact_kaggle_preview') assert.equal(value, true);
    else assert.equal(value, false, `${boundary} must not claim downstream completion`);
  }
});

test('the R0 mechanical batch is bounded, atomic and contains no product decision', async () => {
  const batch = await readJson(BATCH_PATH);

  assert.equal(batch.schema, 'kenigevents.a0.ready-for-r0-mechanical.v1');
  assert.equal(batch.status, 'READY_FOR_R0_MECHANICAL');
  assert.equal(batch.owner_role, 'A0');
  assert.equal(batch.executor_role, 'R0');
  assert.equal(batch.transaction_policy.resolve_all_refs_immediately_before_apply, true);
  assert.equal(batch.transaction_policy.semantic_changes_allowed, false);
  assert.equal(batch.transaction_policy.product_behavior_changes_allowed, false);
  assert.equal(batch.transaction_policy.palette_redesign_allowed, false);

  assert.deepEqual(
    batch.units.map((unit) => unit.id),
    [
      'R0M-A0-FESTIVALS',
      'R0M-A0-EXHIBITIONS',
      'R0M-A0-EVENT-RAILS',
      'R0M-A0-EVENT-LAYOUT-CSS',
      'R0M-A0-MOBILE-RAIL-CSS',
    ],
  );
  assert.equal(new Set(batch.units.map((unit) => unit.rollback_unit)).size, batch.units.length);
  assert.ok(batch.units.every((unit) => unit.path && unit.preflight_required_markers.length > 0));
  assert.ok(batch.units.every((unit) => unit.operations.length > 0 && unit.focused_tests.length > 0));

  assert.deepEqual(
    batch.excluded_from_mechanical_batch.map((item) => item.id),
    ['A0-CLUB-ROUTE-PALETTE', 'A0-EVENT-HERO-INNER-FRAME'],
  );
});

test('collection catalog route binds foundations explicitly rather than through a transitive component import', async () => {
  const source = await read('src/pages/podborki/index.astro');
  assert.match(source, /import '\.\.\/\.\.\/components\/design-system\/product-contour-foundations\.css';/u);
  assert.match(source, /data-ds-family="CollectionCatalogRouteComposition"/u);
  assert.match(source, /<CollectionCatalog entries=\{collectionCatalogEntries\} \/>/u);
});

test('festival mechanical unit is entirely pending or entirely applied', async () => {
  const source = await read('src/pages/festivali/index.astro');
  const pending = source.includes('>↗</span>')
    && source.includes('>＋</span>')
    && !source.includes('data-ds-family="FestivalTimelineRouteComposition"');
  const applied = source.includes("import SemanticIcon from '../../components/design-system/SemanticIcon.astro';")
    && source.includes('data-ds-family="FestivalTimelineRouteComposition"')
    && source.includes('<SemanticIcon name="link" role="action" />')
    && source.includes('<Icon name="spark" className="ke-icon-role ke-icon-role--action" />')
    && !source.includes('>↗</span>')
    && !source.includes('>＋</span>');
  assert.ok(pending || applied, 'festival unit is partially applied or its source markers drifted');
});

test('exhibitions mechanical unit is entirely pending or entirely applied', async () => {
  const source = await read('src/components/ExhibitionsPersonalSurface.astro');
  const pending = source.includes('--ex-bg:#0d0f10')
    && source.includes('--ex-text:#f4f4f2')
    && source.includes('<main id="main" class="ex-page" data-exhibitions-prototype>');
  const applied = source.includes('product-contour-foundations.css')
    && source.includes('data-ds-family="ExhibitionsPersonalSurface"')
    && source.includes('--ke-color-exhibition-canvas')
    && !/--ex-[a-z0-9-]+\s*:/u.test(source);
  assert.ok(pending || applied, 'exhibitions unit is partially applied or its foundation aliases drifted');
});

test('desktop event rails are both handwritten or both canonical EventMediaRail consumers', async () => {
  const source = await read('src/components/DesktopEventPage.astro');
  const pending = source.includes('editorialRailImages.map')
    && source.includes('splitRailCandidates.map')
    && source.includes('data-hero-rail')
    && source.includes('data-split-media-rail');
  const applied = source.includes("import EventMediaRail from './EventMediaRail.astro';")
    && /<EventMediaRail[\s\S]*variant="hero-selector"/u.test(source)
    && /<EventMediaRail[\s\S]*variant="poster-strip"/u.test(source)
    && !source.includes('editorialRailImages.map')
    && !source.includes('splitRailCandidates.map');
  assert.ok(pending || applied, 'DesktopEventPage contains a partial rail migration');
});

test('mobile rail fit ownership is either pre-migration or wholly delegated to MediaFrame', async () => {
  const source = await read('src/components/listings/MobileListingRailSurface.astro');
  const hasFit = source.includes('object-fit:var(--rail-media-fit)');
  const hasPosition = source.includes('object-position:var(--focus-x,50%) var(--focus-y,50%)');
  assert.equal(hasFit, hasPosition, 'mobile rail fit and focal declarations were only partially removed');
  if (!hasFit) {
    assert.doesNotMatch(source, /\.event-media img\{[^}]*object-(?:fit|position)/u);
  }
});

test('Popular density behavior remains outside mechanical deletion scope', async () => {
  const [batch, popular, density] = await Promise.all([
    readJson(BATCH_PATH),
    read('src/components/listings/PopularListingSurface.astro'),
    read('src/components/listings/ListingMobileDensitySwitch.astro'),
  ]);

  assert.ok(!batch.units.some((unit) => unit.path.endsWith('PopularListingSurface.astro')));
  assert.match(popular, /<ListingMobileDensitySwitch \/>/u);
  assert.match(popular, /ke_listing_density_v2/u);
  assert.match(density, /role="radiogroup"/u);
  assert.match(density, /listing:density-change/u);
  assert.match(density, /touchmove/u);
});
