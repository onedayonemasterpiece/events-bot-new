import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const controls = await readFile(new URL('../src/components/listings/ListingControls.astro', import.meta.url), 'utf8');
const weekend = await readFile(new URL('../src/components/listings/WeekendEditorialTimeline.astro', import.meta.url), 'utf8');
const geometryGate = await readFile(new URL('../scripts/check-listing-desktop-geometry-playwright.sh', import.meta.url), 'utf8');

test('weekend repacking invalidates stale row-end copy measurements', () => {
  assert.match(
    weekend,
    /document\.dispatchEvent\(new CustomEvent\('kenigevents:weekend-listing-packed'\)\);/u,
    'WeekendEditorialTimeline must announce every completed DOM reorder',
  );
  assert.match(
    controls,
    /document\.addEventListener\('kenigevents:weekend-listing-packed', scheduleListingRowEnds\);/u,
    'ListingControls must recompute row-end widths after weekend DOM order changes',
  );
  assert.match(
    controls,
    /window\.KERefreshListingRowEnds\s*=\s*syncListingRowEnds;/u,
    'ListingControls must expose a synchronous post-reorder measurement handshake',
  );
  assert.match(
    weekend,
    /window\.KERefreshListingRowEnds\?\.\(\);/u,
    'WeekendEditorialTimeline must synchronously clear stale row tails after packing',
  );
});

test('desktop geometry gate covers the observed 1440px weekend seam', () => {
  assert.match(
    geometryGate,
    /for width in 721 800 1440; do\s+check_route vyhodnye "\$width" 900 overflow-only/u,
  );
});
