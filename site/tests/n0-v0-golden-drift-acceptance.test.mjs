import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const acceptance = JSON.parse(await read('scripts/n0-successor-acceptance.v1.json'));
const strictSourceGate = process.env.N0_REQUIRE_V0_GOLDEN_DRIFT_FIXED === '1';

function selectorBody(source, selector) {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
  const match = new RegExp(`${escaped}\\s*\\{([^}]*)\\}`, 'u').exec(source);
  return match?.[1] || '';
}

test('N0 records the exact independent Golden V0 verdict without granting PASS', () => {
  const verdict = acceptance.v0_golden_verdict;
  assert.equal(verdict.issue_comment, 5527892153);
  assert.equal(verdict.verdict, 'DRIFT');
  assert.equal(verdict.release_acceptance, false);
  assert.equal(verdict.target.repo_sha, '84504f30eebc334deba46e94365601c3d572c5c0');
  assert.equal(verdict.target.data_mode, 'golden');
  assert.equal(verdict.matrix.document_http_200, 40);
  assert.equal(verdict.matrix.document_total, 40);
  assert.equal(verdict.matrix.free_collection_visible_cards_at_375, 6);
  assert.equal(acceptance.accepted_golden_transaction.verification.v0_independent_verdict, 'DRIFT');
  assert.equal(acceptance.v0_triggers.golden.status, 'AUDITED_DRIFT');
  assert.equal(acceptance.v0_triggers.golden.v0_pass, false);
});

test('the 44px auxiliary target finding is a real A0 source blocker', async () => {
  const finding = acceptance.v0_golden_verdict.findings.event_card_auxiliary_target_height;
  assert.equal(finding.classification, 'ACCEPTED_PRODUCT_DRIFT');
  assert.equal(finding.owner, 'A0');
  assert.equal(finding.blocks_first_real_candidate, true);
  assert.equal(finding.observed_height_px, 36.28);
  assert.equal(finding.minimum_height_px, 44);
  assert.equal(finding.source_path, 'site/src/layouts/EventLayout.astro');

  if (!strictSourceGate) return;
  const layout = await read('src/layouts/EventLayout.astro');
  const body = selectorBody(
    layout,
    '.event-card--split-actions .event-card__utility-row .feedback-button--negative',
  );
  const explicitMinimum = /min-height:\s*([0-9.]+)px/u.exec(body)?.[1];
  if (explicitMinimum !== undefined) {
    assert.ok(Number(explicitMinimum) >= 44,
      `specific not-interested override is ${explicitMinimum}px; required >=44px`);
  }
  assert.doesNotMatch(body, /min-height:\s*(?:3[0-9](?:\.\d+)?)px/u);
});

test('data-ds identity is canonical and duplicate data-ui anchors are not required', async () => {
  const finding = acceptance.v0_golden_verdict.findings.stable_dom_anchors;
  assert.equal(finding.classification, 'REJECTED_AS_V0_SELECTOR_CONTRACT_DRIFT');
  assert.equal(finding.source_change_required, false);
  assert.deepEqual(finding.canonical_identity, [
    'data-ds-family',
    'data-ds-version',
    'data-ds-variant',
    'data-ds-state',
  ]);
  assert.ok(finding.forbidden_resolution.includes('data-ui-*'));

  const [surface, grid] = await Promise.all([
    read('src/components/FreeCollectionSurface.astro'),
    read('src/components/AdaptiveEventCardGrid.astro'),
  ]);
  for (const marker of [
    'data-ds-family="FreeCollectionSurface"',
    'data-ds-version="1"',
    'data-ds-variant="timed-and-exhibitions"',
    'data-ds-state={surfaceState}',
    'data-free-collection-surface',
  ]) assert.ok(surface.includes(marker), `missing canonical free-collection marker ${marker}`);
  for (const marker of [
    'data-ds-family="AdaptiveEventCardGrid"',
    'data-ds-version="1"',
    'data-ds-variant={gridVariant}',
    'data-ds-state={gridState}',
    'data-adaptive-event-card-grid',
  ]) assert.ok(grid.includes(marker), `missing canonical grid marker ${marker}`);
});

test('Golden external links are allowed when every blank target is safely isolated', async () => {
  const finding = acceptance.v0_golden_verdict.findings.target_blank;
  assert.equal(finding.classification, 'REJECTED_AS_OVERBROAD_V0_NEGATIVE_GATE');
  assert.equal(finding.source_change_required, false);
  assert.deepEqual(finding.required_rel_tokens, ['noopener', 'noreferrer']);
  assert.deepEqual(finding.replacement_negative_selectors, [
    '[target="_blank"]:not([rel~="noopener"])',
    '[target="_blank"]:not([rel~="noreferrer"])',
  ]);

  const footer = await read('src/components/SiteFooter.astro');
  assert.match(footer, /target="_blank"\s+rel="me noopener noreferrer"/u);
  assert.match(footer, /aria-label=\{`\$\{link\.network\}: \$\{link\.label\}`\}/u);
});

test('first-real candidate remains closed until the accepted source fix and integration pass', () => {
  const candidate = acceptance.candidate_plan.nearest_full_real_candidate;
  assert.equal(candidate.status, 'BLOCKED_BY_V0_TARGET_HEIGHT_FIX_AND_PARTIAL_INTEGRATION');
  assert.equal(candidate.blocking_source_fixes.length, 1);
  assert.equal(candidate.blocking_source_fixes[0].code, 'EVENT_CARD_AUXILIARY_TARGET_HEIGHT_BELOW_44PX');
  assert.equal(candidate.blocking_source_fixes[0].owner, 'A0');
  assert.equal(candidate.blocking_source_fixes[0].path, 'site/src/layouts/EventLayout.astro');
  assert.equal(
    candidate.blocking_source_fixes[0].verification,
    'N0_REQUIRE_V0_GOLDEN_DRIFT_FIXED=1 npm run test:n0-v0-golden-drift',
  );
});
