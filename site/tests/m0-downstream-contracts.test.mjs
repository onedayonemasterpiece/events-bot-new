import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('M0 downstream index covers the complete PM0 20-31 family contour and exact successor gap', async () => {
  const binding = JSON.parse(await read('src/data/m0-downstream-bindings.v1.json'));

  assert.equal(binding.schema, 'kenigevents.m0.downstream-bindings.v1');
  assert.equal(binding.contract_version, '1.9.0');
  assert.equal(binding.role, 'M0');
  assert.equal(binding.instruction_comment, 5526326906);
  assert.equal(binding.authority.current_successor_sha_at_review, '2e8f4dd2393ce0c5100f8b610aae3f01380aad8c');
  assert.equal(binding.authority.source_checkpoint_before_binding_commit, 'c71351decdcee02941acb26c5e2fbaf88faf0378');
  assert.equal(binding.truth_boundary.browser_pass_claimed, false);
  assert.equal(binding.truth_boundary.penpot_materialization_claimed, false);

  const expectedFamilies = new Map([
    ['MediaFrame', 1],
    ['EventCard', 2],
    ['ListingEventCard', 9],
    ['AdaptiveEventCardGrid', 1],
    ['EventMediaRail', 1],
    ['EventHero', 1],
  ]);
  assert.equal(binding.families.length, expectedFamilies.size);
  for (const family of binding.families) {
    assert.equal(family.version, expectedFamilies.get(family.astro_family), `${family.astro_family} version drift`);
    assert.ok(family.source_roots.length > 0, `${family.astro_family} misses source roots`);
    assert.ok(family.penpot_master_target, `${family.astro_family} misses Penpot target`);
  }

  assert.deepEqual(Object.keys(binding.pm0_items).map(Number), Array.from({ length: 12 }, (_, index) => index + 20));
  assert.equal(binding.candidate_review.classification, 'PARTIAL_M0_INCLUDED_CURRENT_TAIL_MISSING');
  assert.ok(binding.candidate_review.missing_current_m0_source.some((item) => item.path === 'site/src/components/EventMediaRail.astro'));
  assert.equal(binding.candidate_review.missing_current_m0_source.find((item) => item.path === 'site/src/components/EventMediaRail.astro')?.required_m0_blob, '1e49e4b765488ef7703473693b6f619d185acfde');
  assert.ok(binding.candidate_review.rejected_replay.some((item) => /temporary MediaFrame exception/u.test(item)));

  assert.deepEqual(binding.downstream_targets, {
    thin_s:'bindings/launch-normalization/m0-family-bindings.v1.json',
    penpot_ready:'bindings/launch-normalization/m0-penpot-ready-spec.v1.json',
    v0_matrix:'bindings/launch-normalization/m0-v0-acceptance-matrix.v1.json',
    integration_rollback:'bindings/launch-normalization/m0-source-integration-and-rollback.v1.json',
  });
});

test('canonical M0 roots expose one family lineage and fail-closed media decisions', async () => {
  const [card, listing, adaptive, optimized, rail, hero, mediaFrame, cardCss] = await Promise.all([
    read('src/components/EventCard.astro'),
    read('src/components/listings/ListingEventCard.astro'),
    read('src/components/AdaptiveEventCardGrid.astro'),
    read('src/components/OptimizedEventCardGrid.astro'),
    read('src/components/EventMediaRail.astro'),
    read('src/components/EventHero.astro'),
    read('src/components/media-frame.css'),
    read('src/components/event-card.css'),
  ]);

  assert.match(card, /data-ds-family="EventCard"[\s\S]*data-ds-version="2"/u);
  assert.match(card, /import '\.\/media-frame\.css';/u);
  assert.match(card, /import '\.\/event-card\.css';/u);
  assert.match(listing, /data-ds-family="ListingEventCard"[\s\S]*data-ds-version="9"/u);
  assert.match(listing, /data-listing-proof-placement=\{hasSocialProof \? \(proofInside \? 'inside' : 'rail'\) : 'none'\}/u);

  assert.equal((adaptive.match(/<EventCard\b/gu) || []).length, 1);
  assert.match(adaptive, /data-ds-family="AdaptiveEventCardGrid"[\s\S]*data-ds-version="1"/u);
  assert.match(adaptive, /data-adaptive-grid-layout-engine="flex-lines"/u);
  assert.match(adaptive, /data-adaptive-grid-diagnostics-contract="input-source-rendered-v1"/u);
  assert.match(adaptive, /adaptive-event-card-grid--responsive-stack\[data-adaptive-grid-row-size\]/u);
  assert.match(adaptive, /adaptive-event-card-grid--responsive-progressive\[data-adaptive-grid-row-size\]/u);

  assert.match(optimized, /import AdaptiveEventCardGrid from '\.\/AdaptiveEventCardGrid\.astro'/u);
  assert.match(optimized, /legacyOptimizedContract/u);
  assert.doesNotMatch(optimized, /import EventCard|<EventCard\b|packRelatedCardRows|<style>/u);

  for (const variant of ['gallery-thumbnails', 'hero-selector', 'poster-strip']) {
    assert.ok(rail.includes(variant), `EventMediaRail misses ${variant}`);
  }
  assert.match(rail, /const contradictoryVisualKind = requestedKind === 'visual' && imageTextMode !== 'visual_only';/u);
  assert.match(rail, /requestedCover && kind === 'visual' && imageTextMode === 'visual_only'/u);
  assert.match(rail, /resolved_visual_kind_mismatch_fail_closed/u);
  assert.match(rail, /resolved_cover_request_fail_closed/u);
  assert.doesNotMatch(rail, /item\.fit === 'cover' && kind === 'visual' \? 'cover' : 'contain'/u);

  assert.match(hero, /data-ds-family="EventHero"[\s\S]*data-ds-version="1"/u);
  assert.match(hero, /class="event-hero__media-frame"[\s\S]*data-media-frame-contract="v1"/u);
  assert.match(hero, /data-media-frame-interaction-owner="caller"/u);
  assert.equal((hero.match(/class="event-hero__media-frame"/gu) || []).length, 1);

  assert.match(mediaFrame, /Canonical MediaFrame v1 structural and fit owner/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="cover"\][\s\S]*object-fit: cover/u);
  assert.match(mediaFrame, /\[data-media-frame-fit="contain"\][\s\S]*object-fit: contain/u);
  assert.match(cardCss, /min-height: var\(--ke-control-min, 44px\)/u);
});

test('M0 bindings name actual consumers without claiming A0, V0 or Penpot completion', async () => {
  const binding = JSON.parse(await read('src/data/m0-downstream-bindings.v1.json'));
  const byFamily = new Map(binding.actual_consumer_census.map((entry) => [entry.family, entry]));

  for (const family of ['EventCard', 'ListingEventCard', 'AdaptiveEventCardGrid', 'EventMediaRail', 'EventHero']) {
    const entry = byFamily.get(family);
    assert.ok(entry, `missing actual-consumer census for ${family}`);
    assert.ok(entry.current_successor_consumers.length > 0, `${family} has no current successor consumers`);
    assert.equal(entry.migration_owner, 'A0');
  }

  assert.equal(binding.published_evidence.fresh_real.current_m0_credit, false);
  assert.equal(binding.published_evidence.golden.verdict, 'DRIFT');
  assert.equal(binding.published_evidence.current_v0_blocker.status, 'NOT_EXECUTED');
  assert.match(binding.next_product_gate.required_source, /current M0 rail semantic tail/u);
  assert.match(binding.next_product_gate.required_runtime, /full fresh-real Kaggle build/u);
  assert.match(binding.next_product_gate.required_browser, /independent V0/u);
});
