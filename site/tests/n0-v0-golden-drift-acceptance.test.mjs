import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const acceptance = JSON.parse(await read('scripts/n0-successor-acceptance.v1.json'));
const strictSourceGate = process.env.N0_REQUIRE_CURRENT_SUCCESSOR === '1'
  || process.env.N0_REQUIRE_V0_GOLDEN_DRIFT_FIXED === '1';

function selectorBody(source, selector) {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  const match = new RegExp(`${escaped}\\s*\\{([^}]*)\\}`, 'u').exec(source);
  return match?.[1] || '';
}

test('Golden DRIFT is retained without preserving a source blocker already closed later', () => {
  const verdict = acceptance.v0_golden_verdict;
  assert.equal(verdict.issue_comment, 5527892153);
  assert.equal(verdict.verdict, 'DRIFT');
  assert.equal(verdict.release_acceptance, false);
  assert.equal(verdict.target.repo_sha, '84504f30eebc334deba46e94365601c3d572c5c0');
  assert.equal(verdict.matrix.document_http_200, 40);

  const target = verdict.findings.event_card_auxiliary_target_height;
  assert.equal(target.classification, 'ACCEPTED_PRODUCT_DRIFT');
  assert.equal(target.observed_height_px, 36.28);
  assert.equal(target.minimum_height_px, 44);
  assert.equal(target.current_source_status, 'CLOSED_IN_D0AD1708_PENDING_V0_RECHECK');
  assert.equal(target.current_source_blocker, false);
});

test('fresh-real V0 platform failure is not represented as PASS or DRIFT', () => {
  const blocker = acceptance.v0_platform_blocker;
  assert.equal(blocker.issue_comment, 5529063082);
  assert.equal(blocker.classification, 'ACTUAL_TOOL_SURFACE_BOUNDARY');
  assert.equal(blocker.target_repo_sha, '4536847f9fbdaa27326ebb3ec9ec1c825736e107');
  assert.equal(blocker.verdict, 'NOT_EXECUTED');
  assert.equal(blocker.browser_pass_claimed, false);
  assert.equal(blocker.browser_drift_claimed, false);

  const trigger = acceptance.v0_triggers.published_fresh_real_baseline;
  assert.equal(trigger.status, 'READY_BUT_V0_BLOCKED_BY_TOOL_SURFACE');
  assert.equal(trigger.blocker_comment, 5529063082);
  assert.equal(trigger.browser_verdict, 'NOT_EXECUTED');
});

test('data-ds identity and safe blank-target rules remain canonical', () => {
  const anchors = acceptance.v0_golden_verdict.findings.stable_dom_anchors;
  assert.equal(anchors.classification, 'REJECTED_AS_V0_SELECTOR_CONTRACT_DRIFT');
  assert.equal(anchors.source_change_required, false);
  assert.deepEqual(anchors.canonical_identity, [
    'data-ds-family',
    'data-ds-version',
    'data-ds-variant',
    'data-ds-state',
  ]);

  const blank = acceptance.v0_golden_verdict.findings.target_blank;
  assert.equal(blank.classification, 'REJECTED_AS_OVERBROAD_V0_NEGATIVE_GATE');
  assert.equal(blank.source_change_required, false);
  assert.deepEqual(blank.required_rel_tokens, ['noopener', 'noreferrer']);
  assert.deepEqual(blank.replacement_negative_selectors, [
    '[target="_blank"]:not([rel~="noopener"])',
    '[target="_blank"]:not([rel~="noreferrer"])',
  ]);
});

test('current candidate boundary lists every unresolved source class', () => {
  const candidate = acceptance.nearest_full_real_candidate;
  assert.equal(candidate.base, '1bc6d9cb4c122046f4782532381de953727c1da6');
  assert.equal(candidate.status,
    'BLOCKED_BY_F0_CONSUMPTION_M0_CURRENT_A0_MECH_06_AND_EXECUTABLE_TESTS');
  assert.deepEqual(candidate.blocking_source_fixes, [
    'F0_ROUTE_THEME_CONSUMPTION',
    'F0_INTEREST_CLUB_CARD_RESIDUAL',
    'A0_FOCUS_ROUTE_IDENTITIES',
  ]);
  assert.ok(candidate.include.some((item) => /0fb2938344cf96b05be0df09dfb9e69525b3717d/u.test(item)));
  assert.ok(candidate.include.some((item) => /c71351decdcee02941acb26c5e2fbaf88faf0378/u.test(item)));
  assert.ok(candidate.include.some((item) => /5eeaba09b5ec432a77ff899ce98fb8b9f492c133/u.test(item)));
  assert.ok(candidate.reject.includes('runtime inheritance from an earlier SHA'));
  assert.ok(candidate.reject.includes('treating 4536847f as current-successor browser acceptance'));
});

test('strict successor closes Golden, F0, M0 and A0 source drift together', async () => {
  if (!strictSourceGate) return;

  const [
    layout,
    popular,
    focusCollection,
    closedHub,
    festivals,
    exhibitions,
    clubDetail,
    clubCard,
    eventHero,
    f0RouteBindingsRaw,
    f0RouteChecker,
    f0ClubDecisionRaw,
    f0ClubChecker,
    m0BindingsRaw,
  ] = await Promise.all([
    read('src/layouts/EventLayout.astro'),
    read('src/components/listings/PopularListingSurface.astro'),
    read('src/pages/fokus-gruppa/kollektsiya/index.astro'),
    read('src/pages/zakrytaya-afisha/index.astro'),
    read('src/pages/festivali/index.astro'),
    read('src/components/ExhibitionsPersonalSurface.astro'),
    read('src/pages/kluby-po-interesam/[slug]/index.astro'),
    read('src/components/InterestClubCard.astro'),
    read('src/components/EventHero.astro'),
    read('src/components/design-system/f0-route-theme-bindings.v1.json'),
    read('src/components/design-system/check-f0-route-theme-bindings.mjs'),
    read('src/components/design-system/f0-interest-club-theme-decision.v1.json'),
    read('src/components/design-system/check-f0-interest-club-theme-decision.mjs'),
    read('src/data/m0-downstream-bindings.v1.json'),
  ]);

  const negativeBody = selectorBody(
    layout,
    '.event-card--split-actions .event-card__utility-row .feedback-button--negative',
  );
  const minimum = /min-height:\s*([0-9.]+)px/u.exec(negativeBody)?.[1];
  assert.ok(minimum !== undefined && Number(minimum) >= 44,
    `EventLayout auxiliary target minimum is ${minimum ?? 'absent'}; required >=44px`);
  assert.doesNotMatch(negativeBody, /min-height:\s*(?:3[0-9](?:\.\d+)?)px/u);

  assert.match(popular, /\.ke-popular-behavior__row\s*\{[^}]*overflow-x:\s*auto/su);
  assert.match(popular, /\.ke-popular-behavior__row\s*\{[^}]*min-width:\s*0/su);

  for (const marker of [
    'data-ds-family="FocusEggCollectionRouteComposition"',
    'data-ds-version="1"',
    'data-ds-variant="collection-prototype"',
  ]) assert.ok(focusCollection.includes(marker), `focus collection misses ${marker}`);
  assert.match(focusCollection, /data-ds-state=/u);
  assert.match(focusCollection, /found-\$\{[^}]+\}-of-\$\{[^}]+\}/u);

  for (const marker of [
    'data-ds-family="ClosedFocusHubRouteComposition"',
    'data-ds-version="1"',
    'data-ds-variant="participant-hub"',
  ]) assert.ok(closedHub.includes(marker), `closed hub misses ${marker}`);
  for (const state of ['checking', 'locked', 'available']) {
    assert.ok(closedHub.includes(state), `closed hub never publishes ${state}`);
  }

  const routeBindings = JSON.parse(f0RouteBindingsRaw);
  assert.equal(routeBindings.schema, 'kenigevents.f0-route-theme-bindings.v1');
  assert.equal(routeBindings.clusters.length, 3);
  assert.match(f0RouteChecker, /F0_REQUIRE_ROUTE_THEME_CONSUMED/u);

  assert.doesNotMatch(festivals,
    /\.festival-guide__icon :global\(svg\)\s*\{\s*width:\s*0\.95rem;\s*height:\s*0\.95rem;/u);
  assert.doesNotMatch(festivals, /width:\s*clamp\(2rem,\s*2\.35vw,\s*2\.2rem\)/u);
  assert.doesNotMatch(festivals, /height:\s*clamp\(2rem,\s*2\.35vw,\s*2\.2rem\)/u);
  assert.match(festivals, /var\(--ke-festival-like-target-min\)/u);
  assert.match(festivals, /<SemanticIcon name="heart" role="control" \/>/u);
  assert.match(festivals, /rel=\{item\.isExternal \? 'noopener noreferrer' : undefined\}/u);

  for (const forbidden of ['--ex-bg:', '--ex-surface:', '--ex-motion-base:', 'var(--ex-bg)', 'var(--ex-surface)']) {
    assert.ok(!exhibitions.includes(forbidden), `exhibitions retains ${forbidden}`);
  }
  assert.match(exhibitions, /var\(--ke-color-exhibitions-background\)/u);
  assert.match(exhibitions, /var\(--ke-exhibitions-motion-base\)/u);

  assert.doesNotMatch(clubDetail, /<span aria-hidden="true">←<\/span>/u);
  assert.match(clubDetail, /<SemanticIcon name="arrow-left" role="inline" \/>/u);
  assert.match(clubDetail, /var\(--ke-club-detail-action-min\)/u);

  const clubDecision = JSON.parse(f0ClubDecisionRaw);
  assert.equal(clubDecision.schema, 'kenigevents.f0-interest-club-theme-decision.v1');
  assert.equal(clubDecision.status, 'PRODUCT_CLASSIFICATION_CLOSED_RESIDUAL_CARD_BINDING_READY');
  assert.equal(clubDecision.residual_card_bindings.length, 4);
  assert.match(f0ClubChecker, /F0_REQUIRE_CLUB_THEME_CONSUMED/u);
  for (const binding of clubDecision.residual_card_bindings) {
    assert.ok(!clubCard.includes(binding.current), `InterestClubCard retains ${binding.current}`);
    assert.ok(clubCard.includes(binding.replacement), `InterestClubCard misses ${binding.replacement}`);
  }

  assert.match(eventHero, /data-ds-family="EventHero"/u);
  assert.match(eventHero, /class="event-hero__media-frame"/u);
  assert.match(eventHero, /data-media-frame-style-owner="media-frame\.css"/u);
  assert.match(eventHero, /data-media-frame-interaction-owner="caller"/u);
  const primaryFrame = /<span\s+[\s\S]*?class="event-hero__media-frame"[\s\S]*?<\/span>/u.exec(eventHero)?.[0] || '';
  assert.doesNotMatch(primaryFrame, /object-fit:|object-position:/u);

  const m0Bindings = JSON.parse(m0BindingsRaw);
  assert.equal(m0Bindings.schema, 'kenigevents.m0.downstream-bindings.v1');
  assert.deepEqual(m0Bindings.downstream_targets, {
    thin_s:'catalog/normalization/m0-family-thin-s-bindings.v1.json',
    penpot_ready:'penpot/candidate/m0-family-master-spec.v1.json',
    v0_matrix:'catalog/normalization/evidence/m0-v0-acceptance-matrix.v1.json',
    integration_rollback:'docs/launch-normalization/m0-source-integration-and-rollback.v1.json',
  });
});
