import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const DIRECT_FRAMING_DECLARATION = /(?:^|[;{])\s*object-(?:fit|position)\s*:/mu;

test('M0 downstream index acknowledges the exact FR0 cutover and current successor gap', async () => {
  const binding = JSON.parse(await read('src/data/m0-downstream-bindings.v1.json'));

  assert.equal(binding.schema, 'kenigevents.m0.downstream-bindings.v1');
  assert.equal(binding.version, '1.1.0');
  assert.equal(binding.contract_version, '1.10.0');
  assert.equal(binding.role, 'M0');
  assert.equal(binding.instruction_comment, 5529661330);
  assert.equal(binding.authority.fr0_cutover_base, 'c71351decdcee02941acb26c5e2fbaf88faf0378');
  assert.equal(binding.authority.m0_downstream_base, '5eeaba09b5ec432a77ff899ce98fb8b9f492c133');
  assert.equal(binding.authority.current_successor_sha_at_review, 'cebeafeee08251a327145ee973ee035cced65204');
  assert.equal(binding.authority.current_successor_contains_m0_downstream_base, true);
  assert.equal(binding.authority.current_successor_contains_post_cutover_consolidation, false);
  assert.equal(binding.cutover.acknowledged, true);
  assert.equal(binding.cutover.no_third_wave, true);
  assert.equal(binding.truth_boundary.fr0_protocol_dependency_is_not_m0_protocol_ownership, true);
  assert.equal(binding.truth_boundary.browser_pass_claimed, false);
});

test('post-cutover M0 families contain only EventCard, ListingEventCard and AdaptiveEventCardGrid', async () => {
  const binding = JSON.parse(await read('src/data/m0-downstream-bindings.v1.json'));
  const expectedFamilies = new Map([
    ['EventCard', 2],
    ['ListingEventCard', 9],
    ['AdaptiveEventCardGrid', 1],
  ]);

  assert.equal(binding.families.length, expectedFamilies.size);
  for (const family of binding.families) {
    assert.equal(family.version, expectedFamilies.get(family.astro_family), `${family.astro_family} version drift`);
    assert.ok(family.source_roots.length > 0, `${family.astro_family} misses source roots`);
    assert.ok(family.penpot_master_target, `${family.astro_family} misses Penpot target`);
  }
  for (const transferred of ['MediaFrame', 'EventMediaRail', 'EventHero']) {
    assert.ok(!binding.families.some((family) => family.astro_family === transferred),
      `${transferred} must not remain a current M0 family after cutover`);
  }

  assert.equal(binding.row_layout_service.source, 'site/src/lib/relatedCardLayout.mjs');
  assert.equal(binding.row_layout_service.document_crop_budget, 0.2);
  assert.deepEqual(binding.compatibility_adapters, [{
    path:'site/src/components/OptimizedEventCardGrid.astro',
    target:'AdaptiveEventCardGrid',
    status:'adapter_only',
    penpot_master:false,
    forbidden_owners:['EventCard DOM', 'packRelatedCardRows', 'layout CSS', 'runtime diagnostics'],
  }]);
});

test('M0 card roots expose one component root, resource states, actions and metadata without framing paint', async () => {
  const [card, listing, actionCss] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/listings/ListingEventCard.astro'),
    read('src/components/event-card.css'),
  ]);

  assert.equal((card.match(/<article\b/gu) || []).length, 1);
  assert.equal((listing.match(/<article\b/gu) || []).length, 1);
  assert.match(card, /data-ds-family="EventCard"[\s\S]*data-ds-version="2"/u);
  assert.match(listing, /data-ds-family="ListingEventCard"[\s\S]*data-ds-version="9"/u);

  for (const source of [card, listing]) {
    assert.match(source, /data-media-frame-resource-state=/u);
    assert.match(source, /data-media-frame-source-ratio=/u);
    assert.doesNotMatch(source, /data-media-frame-state|dataset\.mediaFrameState/u);
    assert.doesNotMatch(source, DIRECT_FRAMING_DECLARATION,
      'card roots may publish framing inputs but cannot own object-fit/object-position declarations');
  }

  assert.match(card, /data-feedback-action="not_interested"/u);
  assert.match(card, /data-calendar-action/u);
  assert.match(card, /data-native-share/u);
  assert.match(card, /data-feedback-action="like"/u);
  assert.match(card, /data-card-type/u);
  assert.match(card, /<EventOccurrenceLabel presentation=\{occurrencePresentation\} \/>/u);
  assert.match(card, /data-card-status/u);
  assert.match(actionCss, /min-height: var\(--ke-control-min, 44px\)/u);

  assert.match(listing, /data-listing-event-type=/u);
  assert.match(listing, /showFree && 'free-admission'/u);
  assert.match(listing, /data-listing-proof-placement=\{hasSocialProof \? \(proofInside \? 'inside' : 'rail'\) : 'none'\}/u);
  assert.match(listing, /const tailWidth = splitIdentityProofRail \? 96 : hasSideRail \? \(visibleIdentityCount === 0 \? 40 : 64\) : 0;/u);
});

test('AdaptiveEventCardGrid and Optimized adapter preserve one diagnostics writer and no phantom tracks', async () => {
  const [adaptive, optimized] = await Promise.all([
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/OptimizedEventCardGrid.astro'),
  ]);

  assert.equal((adaptive.match(/data-adaptive-grid-diagnostics-owner=/gu) || []).length, 1);
  assert.match(adaptive, /data-adaptive-grid-diagnostics-owner="AdaptiveEventCardGrid"/u);
  assert.match(adaptive, /data-adaptive-grid-diagnostics-contract="input-source-rendered-v1"/u);
  for (const field of [
    'data-adaptive-grid-input-count',
    'data-adaptive-grid-input-order',
    'data-adaptive-grid-source-count',
    'data-adaptive-grid-source-order',
    'data-adaptive-grid-rendered-count',
    'data-adaptive-grid-rendered-order',
  ]) assert.ok(adaptive.includes(field), `missing grid field: ${field}`);
  assert.match(adaptive, /itemRoots\[`\$\{item\.id\}:\$\{sourceIndex\}`\]/u);
  assert.match(adaptive, /data-adaptive-grid-remainder-variant=/u);
  assert.match(adaptive, /regular-\$\{remainder\}-of-\$\{size\}/u);
  assert.match(adaptive, /data-adaptive-grid-layout-engine="grid-subgrid"/u);
  assert.match(adaptive, /grid-template-rows: subgrid/u);
  assert.doesNotMatch(adaptive, /repeat\(\s*auto-fit/u);
  assert.match(adaptive, /adaptive-event-card-grid--responsive-stack\[data-adaptive-grid-row-size\]/u);
  assert.match(adaptive, /adaptive-event-card-grid--responsive-progressive\[data-adaptive-grid-row-size\]/u);

  assert.match(optimized, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro'/u);
  assert.match(optimized, /legacyOptimizedContract/u);
  assert.doesNotMatch(optimized, /import EventCard|<EventCard\b|packRelatedCardRows|<style>/u);
  assert.doesNotMatch(optimized, /data-adaptive-grid-(?:input|source|rendered|remainder)/u);
});

test('relatedCardLayout owns normalized inputs, crop budget and deterministic occupancy', async () => {
  const source = await read('src/lib/relatedCardLayout.mjs');

  assert.match(source, /const MAX_DOCUMENT_CROP = 0\.2;/u);
  assert.match(source, /const normalizedTargetAspect = finiteRatio\(targetAspect, mediaRatio\);/u);
  assert.match(source, /if \(potentialCoverCrop > MAX_DOCUMENT_CROP \+ EPSILON\)/u);
  assert.match(source, /cropReason:'document_crop_budget_exceeded'/u);
  assert.match(source, /const limit = Math\.max\(0, Math\.floor\(Number\.isFinite\(requestedLimit\)/u);
  assert.match(source, /const rowSize = Math\.max\(1, Math\.min\(6, Math\.floor\(Number\.isFinite\(requestedRowSize\)/u);
  assert.match(source, /return left\.signature\.localeCompare\(right\.signature\);/u);
  assert.match(source, /rowColumn:index/u);
  assert.match(source, /return rows\.flatMap\(\(row, rowIndex\) => materializeRow\(row, rowIndex, presentation\)\);/u);
});

test('downstream and test ownership are transferred without deleting FR0 coverage', async () => {
  const [binding, ownership] = await Promise.all([
    read('src/data/m0-downstream-bindings.v1.json').then(JSON.parse),
    read('tests/m0-post-fr0-test-ownership.v1.json').then(JSON.parse),
  ]);

  assert.equal(binding.transferred_test_ownership.manifest, 'site/tests/m0-post-fr0-test-ownership.v1.json');
  assert.equal(binding.transferred_test_ownership.m0_mixed_suite_status, 'CARD_GRID_ROW_LAYOUT_ONLY');
  assert.equal(binding.transferred_test_ownership.coverage_deleted, false);
  assert.equal(ownership.coverage_policy.delete_pre_cutover_coverage, false);
  assert.deepEqual(binding.transferred_test_ownership.fr0_replacements, [
    'site/tests/fr0-media-rail-fallback-contract.test.mjs',
    'site/tests/fr0-media-rail-clip-owner.test.mjs',
    'site/tests/fr0-event-hero-resource-fallback.test.mjs',
    'site/tests/fr0-exhibitions-media-frame-contract.test.mjs',
  ]);

  assert.equal(binding.candidate_review.external_consumer_requirement.comment, 5530928932);
  assert.equal(binding.candidate_review.external_consumer_requirement.m0_source_edit, false);
  assert.equal(binding.pm0_items['26'], 'FR0_OWNED_AFTER_CUTOVER');
  assert.equal(binding.pm0_items['27'], 'FR0_AND_V0_OWNED_AFTER_CUTOVER');
  assert.match(binding.next_product_gate.required_source, /exact current head of work\/ui-normalization-m0-continuity-20260903/u);
  assert.match(binding.rollback.forbidden_rollback_targets.join('\n'), /FR0 cutover base c71351de/u);
});
