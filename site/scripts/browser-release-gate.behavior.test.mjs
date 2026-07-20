import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  expectedObjectFitForTreatment,
  recordBrowserVisualSuccess,
  releaseRootMetadata,
  staticSpecimenCandidates,
  startReleaseServer,
} from './check-browser-release-gate.mjs';

test('R01 crop assertion preserves both document and visual contain decisions', () => {
  assert.equal(expectedObjectFitForTreatment('document-contain'), 'contain');
  assert.equal(expectedObjectFitForTreatment('visual-contain'), 'contain');
  assert.equal(expectedObjectFitForTreatment('visual-cover'), 'cover');
});

const successfulReport = {
  ok: true,
  checks: {
    related_geometry_crop: 'ok',
    canonical_event_cards: 'ok',
    gallery_cross_document: 'ok',
    footer_shortcuts: 'ok',
  },
};

test('R03/R04 browser_visual is written only after every browser assertion passes', () => {
  const root = mkdtempSync(join(tmpdir(), 'static-browser-manifest-'));
  const manifestPath = join(root, 'secret-candidate-manifest.json');
  writeFileSync(manifestPath, `${JSON.stringify({ base_path: '/_review/token', checks: { candidate_contract: 'ok' } }, null, 2)}\n`);
  assert.throws(() => recordBrowserVisualSuccess(manifestPath, { ...successfulReport, ok: false }), /refusing to record/u);
  assert.equal(JSON.parse(readFileSync(manifestPath)).checks.browser_visual, undefined);
  assert.throws(() => recordBrowserVisualSuccess(manifestPath, {
    ...successfulReport,
    checks: { ...successfulReport.checks, footer_shortcuts: 'pending' },
  }), /footer_shortcuts/u);
  assert.equal(JSON.parse(readFileSync(manifestPath)).checks.browser_visual, undefined);
  const manifest = recordBrowserVisualSuccess(manifestPath, successfulReport);
  assert.equal(manifest.checks.browser_visual, 'ok');
  assert.equal(JSON.parse(readFileSync(manifestPath)).checks.browser_visual, 'ok');
});

test('R04 release metadata preserves the immutable candidate prefix', () => {
  const root = mkdtempSync(join(tmpdir(), 'static-browser-root-'));
  writeFileSync(join(root, 'secret-candidate-manifest.json'), JSON.stringify({ base_path: '/_review/abc', checks: {} }));
  const metadata = releaseRootMetadata(root);
  assert.equal(metadata.basePath, '/_review/abc');
  assert.equal(metadata.manifestPath, join(root, 'secret-candidate-manifest.json'));
});

test('R03 static server maps prefixed clean URLs to generated files', async () => {
  const root = mkdtempSync(join(tmpdir(), 'static-browser-server-'));
  mkdirSync(join(root, 'sobytiya', 'event'), { recursive: true });
  writeFileSync(join(root, 'sobytiya', 'event', 'index.html'), '<!doctype html><title>event</title>');
  const server = await startReleaseServer(root, '/_review/abc');
  try {
    const response = await fetch(`${server.origin}/_review/abc/sobytiya/event/`);
    assert.equal(response.status, 200);
    assert.match(await response.text(), /<title>event<\/title>/u);
    assert.equal((await fetch(`${server.origin}/_review/abc/../escape`)).status, 404);
  } finally {
    await server.close();
  }
});

test('R03 production specimen discovery is static, bounded and prefers the reported 6408 journey', () => {
  const root = mkdtempSync(join(tmpdir(), 'static-browser-specimens-'));
  const basePath = '/_review/token';
  const makePage = (slug, target, { related = true } = {}) => {
    const dir = join(root, 'sobytiya', slug);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, 'index.html'), `<!doctype html>
      <main data-desktop-clean-event><img data-clean-hero-image>
      ${related ? '<section data-related-start><article data-event-card></article><article data-event-card></article><article data-event-card></article></section>' : ''}
      <div data-gallery-slide-kind="image"></div><div data-gallery-slide-kind="image"></div>
      ${target ? `<div data-gallery-slide-kind="cta"><a href="${basePath}/sobytiya/${target}/">next</a></div>` : ''}
      </main>`);
  };
  makePage('alpha-100', 'target-200');
  makePage('dog-6408', 'target-200');
  makePage('target-200', null, { related: false });
  const routes = ['alpha-100', 'dog-6408', 'target-200'].map((slug) => `${basePath}/sobytiya/${slug}/`);
  assert.deepEqual(staticSpecimenCandidates(root, basePath, routes), [
    { route: `${basePath}/sobytiya/dog-6408/`, targetPath: `${basePath}/sobytiya/target-200/` },
    { route: `${basePath}/sobytiya/alpha-100/`, targetPath: `${basePath}/sobytiya/target-200/` },
  ]);
});
