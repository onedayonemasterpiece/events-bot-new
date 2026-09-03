import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('MobileListingRailRow publishes complete identity and mirrors existing runtime states', async () => {
  const row = await read('src/components/listings/MobileListingRailRow.astro');

  for (const marker of [
    'data-ds-component="MobileListingRailRow"',
    'data-ds-family="MobileListingRailRow"',
    'data-ds-version="1"',
    'data-ds-variant={occurrenceMode}',
    'data-ds-state={rowState}',
    'data-mobile-listing-row-base-state={rowState}',
  ]) assert.ok(row.includes(marker), `missing row identity marker: ${marker}`);

  assert.match(row, /function bindMobileListingRailRowDiagnostics\(root = document\)/u);
  assert.match(row, /querySelectorAll\('\[data-mobile-listing-row\]'\)/u);
  assert.match(row, /row\.dataset\.mobileListingRowDiagnosticsBound = 'true'/u);
  assert.match(row, /row\.dataset\.mobileRailTemporalState/u);
  assert.match(row, /row\.dataset\.notInterested === 'true' \? 'not-interested'/u);
  assert.match(row, /row\.classList\.contains\('is-liked'\) \? 'liked'/u);
  for (const state of [
    'is-dragging',
    'is-settling',
    'is-dislike-active',
    'is-armed',
    'is-like-pulling',
    'is-like-armed',
    'media-error',
    'media-loading',
    'media-loaded',
  ]) assert.ok(row.includes(state), `missing reflected runtime state: ${state}`);
  assert.match(row, /row\.hidden \? 'hidden' : 'visible'/u);
  assert.match(row, /row\.dataset\.dsState = \[\.\.\.new Set\(tokens\)\]\.join\(' '\)/u);
  assert.match(row, /new MutationObserver\(sync\)\.observe\(row, \{/u);
  for (const attribute of [
    "'hidden'",
    "'class'",
    "'aria-pressed'",
    "'data-not-interested'",
    "'data-mobile-rail-temporal-state'",
    "'data-media-state'",
  ]) assert.ok(row.includes(attribute), `missing observed state attribute: ${attribute}`);
});
