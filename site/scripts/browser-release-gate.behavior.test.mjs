import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  expectedObjectFitForTreatment,
  recordBrowserVisualSuccess,
  releaseRootMetadata,
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
