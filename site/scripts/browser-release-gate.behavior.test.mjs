import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  BROWSER_GATE_ACTION_TIMEOUT_MS,
  BROWSER_GATE_NAVIGATION_TIMEOUT_MS,
  expectedObjectFitForTreatment,
  localReleaseAssetPath,
  recordBrowserVisualSuccess,
  releaseRootMetadata,
  staticHeroCropCandidates,
  staticSpecimenCandidates,
  startReleaseServer,
} from './check-browser-release-gate.mjs';

test('R03 mandatory browser gate has bounded action/navigation waits and no network-idle dependency', () => {
  assert.equal(BROWSER_GATE_ACTION_TIMEOUT_MS, 8_000);
  assert.equal(BROWSER_GATE_NAVIGATION_TIMEOUT_MS, 12_000);
  const source = readFileSync(new URL('./check-browser-release-gate.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /waitForLoadState\(['"]networkidle['"]\)/u);
  assert.doesNotMatch(source, /window\.scrollTo\(\{\s*top:\s*document\.documentElement\.scrollHeight/gu);
  assert.match(source, /data-hide-sticky-after/gu);
  assert.match(source, /page\.mouse\.wheel\(0, 320\)/gu);
  assert.match(source, /closeAllConnections/u);
  assert.match(source, /let server = null;[\s\S]*let browser = null;/u);
  assert.match(source, /if \(browser\)[\s\S]*if \(server\)/u);
});

test('R03 prepublication gate maps only immutable CDN Astro runtime back to the checked tree', () => {
  assert.equal(
    localReleaseAssetPath('https://static.kenigevents.ru/build-123/_astro/EventLayout.hash.js'),
    '/_astro/EventLayout.hash.js',
  );
  assert.equal(localReleaseAssetPath('https://static.kenigevents.ru/p/event.webp'), null);
  assert.equal(localReleaseAssetPath('https://kenigevents.ru/_astro/app.js'), null);
  assert.equal(localReleaseAssetPath('not-a-url'), null);
});

test('R01 crop assertion maps treatments, covers visuals and keeps unknown documents fail closed', () => {
  assert.equal(expectedObjectFitForTreatment('document-contain'), 'contain');
  assert.equal(expectedObjectFitForTreatment('document-safe-cover'), 'cover');
  assert.equal(expectedObjectFitForTreatment('visual-contain'), 'contain');
  assert.equal(expectedObjectFitForTreatment('visual-cover'), 'cover');
  const source = readFileSync(new URL('./check-browser-release-gate.mjs', import.meta.url), 'utf8');
  assert.match(source, /item\.mediaKind === 'visual'[\s\S]*item\.treatment === 'visual-cover' && item\.objectFit === 'cover'/u);
  assert.match(source, /unusedFrameRatio[\s\S]*visual card .* leaves .* media frame unused/u);
  assert.match(source, /item\.treatment === 'document-safe-cover'[\s\S]*item\.objectFit === 'cover'/u);
  assert.match(source, /item\.treatment === 'document-contain' && item\.objectFit === 'contain'/u);
  assert.match(source, /semantic_error_fail_closed[\s\S]*unknown_media_fail_closed[\s\S]*document_dimensions_unknown/u);
  assert.match(source, /contains without a fail-closed reason/u);
  assert.match(source, /recommendation row cards do not share one total height/u);
  assert.match(source, /incomplete before the final row/u);
  assert.match(source, /do not share row-local chrome height/u);
  assert.match(source, /reserves excessive body whitespace/u);
});

test('R01 document acceptance is card-local and does not mistake a mixed row potential for applied crop', () => {
  const source = readFileSync(new URL('./check-browser-release-gate.mjs', import.meta.url), 'utf8');
  const eventCard = readFileSync(new URL('../src/components/EventCard.astro', import.meta.url), 'utf8');
  const eventLayout = readFileSync(new URL('../src/layouts/EventLayout.astro', import.meta.url), 'utf8');
  assert.match(source, /item\.coverCrop <= 0\.2001/gu);
  assert.doesNotMatch(source, /item\.coverCrop <= 0\.2001 && item\.rowWorstCrop <= 0\.2001/gu);
  assert.match(source, /item\.coverCrop === 0[\s\S]*numeric crop claim/u);
  assert.match(eventCard, /data-lab-crop-reason=\{desktopRelatedLayout \? cardCrop\.cropReason/u);
  assert.match(eventLayout, /setRuntimeCardDataset\(card, 'labCropReason', relatedLayout\.cropReason\)/u);
});

const successfulReport = {
  ok: true,
  checks: {
    hero_gallery_crop: 'ok',
    related_geometry_crop: 'ok',
    related_loaded_media: 'ok',
    canonical_event_cards: 'ok',
    spatial_card_keyboard: 'ok',
    cold_and_pointer_keyboard: 'ok',
    gallery_cross_document: 'ok',
    footer_shortcuts: 'ok',
    festival_calendar: 'ok',
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

test('R01/R02 release gate blocks fallback bleed and tests cold/mixed keyboard ownership', () => {
  const source = readFileSync(new URL('./check-browser-release-gate.mjs', import.meta.url), 'utf8');
  assert.match(source, /fallbackVisible/gu);
  assert.match(source, /exposes fallback text behind a loaded/gu);
  assert.match(source, /non-OCR hero is letterboxed/gu);
  assert.match(source, /non-OCR gallery slide is letterboxed/gu);
  assert.match(source, /staticHeroCropCandidates\(root, basePath, routes\)/gu);
  assert.match(source, /cold BODY ArrowRight did not advance/gu);
  assert.match(source, /inert click \+ Russian-layout KeyL/gu);
  assert.match(source, /header provenance leaked/gu);
  assert.match(source, /page\.mouse\.click\(point\.x, point\.y\)/gu);
  assert.match(source, /modal dialog leaked current-event KeyL ownership/gu);
  assert.match(source, /single-image cold ArrowRight changed/gu);
  assert.match(source, /ArrowRight did not follow the visual row/gu);
  assert.match(source, /ArrowDown did not choose the nearest card in the ragged final row/gu);
  assert.match(source, /hovered card exposes a K hint without focus/gu);
  assert.match(source, /KeyK acted on a card other than the visually focused owner/gu);
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
      <main data-desktop-clean-event data-closed-hero-gallery="gallery-${slug}"><img data-clean-hero-image>
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

test('R01 hero crop canaries cover every generated visual-only desktop family/fit combination', () => {
  const root = mkdtempSync(join(tmpdir(), 'static-browser-hero-crop-'));
  const basePath = '/_review/token';
  const page = (slug, family, fit, mode = 'visual_only') => {
    const dir = join(root, 'sobytiya', slug);
    mkdirSync(dir, { recursive:true });
    writeFileSync(join(dir, 'index.html'), `<main data-desktop-clean-event data-desktop-family="${family}" data-selected-media-policy="${mode}" data-split-media-fit="${fit}"><img data-clean-hero-image></main>`);
    return `${basePath}/sobytiya/${slug}/`;
  };
  const dog = page('dog-6408', 'editorial', 'none');
  const duplicateEditorial = page('other-1', 'editorial', 'none');
  const split = page('portrait-2', 'split', 'viewport-cover');
  const ocr = page('poster-3', 'split', 'viewport-contain', 'ocr_text');
  assert.deepEqual(staticHeroCropCandidates(root, basePath, [duplicateEditorial, split, ocr, dog]), [dog, split]);
});
