import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  assertRequiredPreviewBrowserJourney,
  staticSpecimenCandidates,
} from './check-browser-release-gate.mjs';

const basePath = '/_review/token';

function makePage(root, slug, target, {
  hero = true,
  related = true,
  imageCount = 2,
} = {}) {
  const dir = join(root, 'sobytiya', slug);
  mkdirSync(dir, { recursive: true });
  const images = Array.from({ length: imageCount }, () => '<div data-gallery-slide-kind="image"></div>').join('');
  writeFileSync(join(dir, 'index.html'), `<!doctype html>
    <main data-desktop-clean-event ${hero ? `data-closed-hero-gallery="gallery-${slug}"` : ''}>
      <img data-clean-hero-image>
      ${related ? '<section data-related-start><article data-event-card></article><article data-event-card></article><article data-event-card></article></section>' : ''}
      ${images}
      ${target ? `<div data-gallery-slide-kind="cta"><a href="${basePath}/sobytiya/${target}/">next</a></div>` : ''}
    </main>`);
}

test('active-data multi-image journey gate prefers historical IDs, falls back honestly, and stays fail-closed', () => {
  const root = mkdtempSync(join(tmpdir(), 'active-data-browser-journey-'));

  makePage(root, 'alpha-100', 'target-200');
  makePage(root, 'dog-6408', 'target-6407');
  makePage(root, 'target-200', null, { related: false });
  makePage(root, 'target-6407', null, { related: false });
  makePage(root, 'thin-300', 'target-200', { related: false });

  const historicalRoutes = [
    'alpha-100',
    'dog-6408',
    'target-200',
    'target-6407',
    'thin-300',
  ].map((slug) => `${basePath}/sobytiya/${slug}/`);

  const historicalCandidates = staticSpecimenCandidates(root, basePath, historicalRoutes);
  assert.deepEqual(
    assertRequiredPreviewBrowserJourney(historicalCandidates),
    {
      route: `${basePath}/sobytiya/dog-6408/`,
      targetPath: `${basePath}/sobytiya/target-6407/`,
    },
    'preferred historical IDs must win while the factual pair is present',
  );
  assert.equal(
    historicalCandidates.some((candidate) => candidate.route.endsWith('/thin-300/')),
    false,
    'an insufficient specimen must not become an active journey',
  );

  const activeRoutes = [
    'alpha-100',
    'target-200',
    'thin-300',
  ].map((slug) => `${basePath}/sobytiya/${slug}/`);
  const activeCandidates = staticSpecimenCandidates(root, basePath, activeRoutes);
  assert.deepEqual(
    assertRequiredPreviewBrowserJourney(activeCandidates),
    {
      route: `${basePath}/sobytiya/alpha-100/`,
      targetPath: `${basePath}/sobytiya/target-200/`,
    },
    'an active generated multi-image specimen must replace expired preferred IDs',
  );

  assert.throws(
    () => assertRequiredPreviewBrowserJourney([]),
    (error) => {
      assert.match(error.message, /has no deterministic multi-image recommendation journey in the generated catalog/u);
      assert.doesNotMatch(error.message, /6408 -> 6407/u);
      return true;
    },
    'a genuinely missing journey must still fail closed without a false hardcoded-ID failure',
  );
});
