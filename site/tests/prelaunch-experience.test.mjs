import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = resolve(siteDir, '..');

function source(path) {
  return readFileSync(join(repoRoot, path), 'utf8');
}

test('prelaunch scene has one source artwork, one seam mask and glass panes', () => {
  const page = source('site/src/components/PrelaunchPage.astro');
  const scene = source('site/src/scripts/prelaunchScene.ts');
  const css = source('site/src/styles/prelaunch-page.css');
  const prepare = source('site/scripts/prepare-prelaunch-artwork.mjs');

  assert.match(page, /\/assets\/prelaunch\/PWA-icon\.webp/u);
  assert.match(page, /data-prelaunch-artwork/u);
  assert.match(page, /data-prelaunch-artwork-image/u);
  assert.match(page, /data-prelaunch-seams/u);
  assert.match(page, /data-prelaunch-seam-path/u);
  assert.match(page, /fill-rule="evenodd"/u);
  assert.match(page, /Array\.from\(\{ length: 112 \}/u);
  assert.doesNotMatch(page, /prelaunch-fit-v\d+\.css/u);

  assert.match(prepare, /prelaunch-handoff[\s\S]*PWA-icon\.webp/u);
  assert.match(prepare, /copyFileSync/u);

  assert.match(scene, /inverse-svg-rounded-holes/u);
  assert.match(scene, /source-asset-rounded-crop/u);
  assert.match(scene, /roundedRectPath/u);
  assert.match(scene, /seamPath\.setAttribute\('d', path\)/u);
  assert.match(scene, /artworkSize \/ 5\.95/u);
  assert.match(scene, /Math\.min\(width \* \.155, 62\)/u);
  assert.match(scene, /Math\.ceil\(\(width - left/u);
  assert.doesNotMatch(scene, /getImageData|flood|Uint32Array/iu);

  assert.match(css, /\.prelaunch__artwork[\s\S]*overflow:\s*hidden/u);
  assert.match(css, /\.prelaunch__artwork img[\s\S]*121\.52%/u);
  assert.match(css, /\.prelaunch__seams path[\s\S]*fill:\s*var\(--prelaunch-seam\)/u);
  assert.match(css, /\.prelaunch__tile[\s\S]*backdrop-filter/u);
  assert.match(css, /data-depth="sealed"/u);
  assert.match(css, /data-depth="dim"/u);
  assert.match(css, /data-depth="clear"/u);
  assert.match(css, /\.prelaunch__light[\s\S]*radial-gradient/u);
  assert.match(css, /\.prelaunch__dust[\s\S]*radial-gradient/u);
  assert.doesNotMatch(css, /transition:[^;}]*backdrop-filter/su);
  assert.doesNotMatch(css, /0 0 0 calc\(var\(--prelaunch-tile-radius/u);
});

test('prelaunch form uses guarded idempotent direct and relay transport', () => {
  const page = source('site/src/components/PrelaunchPage.astro');
  const form = source('site/src/scripts/prelaunchForm.ts');
  const validator = source('site/src/lib/prelaunchEmail.ts');

  assert.match(page, /data-prelaunch-form/u);
  assert.match(page, /важных обновлениях и полезных подборках/u);
  assert.match(page, /приятный сюрприз/u);
  assert.doesNotMatch(page, /одно письмо/u);

  assert.match(form, /normalizePrelaunchEmail/u);
  assert.match(form, /getResilientDataClient/u);
  assert.match(form, /PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL/u);
  assert.match(form, /prelaunch-updates-2026-v1/u);
  assert.match(form, /register_prelaunch_notification_v1/u);
  assert.match(form, /prelaunch-notification-v2/u);
  assert.match(form, /Вы уже записаны/u);
  assert.match(form, /второй записи не появится/u);

  assert.match(validator, /ASCII_VISIBLE/u);
  assert.match(validator, /EXPLICITLY_UNSAFE/u);
  assert.match(validator, /local\.includes\('\.\.'\)/u);
});

test('prelaunch runtime no longer imports the accumulated visual patch stack', () => {
  const index = source('site/src/pages/index.astro');
  const layout = source('site/src/layouts/PrelaunchLayout.astro');
  const page = source('site/src/components/PrelaunchPage.astro');

  assert.match(index, /import PrelaunchPage/u);
  assert.match(index, /<PrelaunchPage\s*\/>/u);
  assert.doesNotMatch(index, /PrelaunchLanding|PrelaunchExperience|PrelaunchVisualReview/u);
  assert.doesNotMatch(layout, /prelaunch-(?:motion|fit-v\d+)\.css/u);
  assert.match(page, /prelaunch-page\.css/u);
});

test('desktop and mobile reference assets remain stored for manual review', () => {
  const referenceDir = join(repoRoot, 'docs/features/static-site-pages/prelaunch-handoff/reference');
  assert.equal(existsSync(join(referenceDir, 'PWA-icon.webp')), true);
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-desktop-v1.webp')), true);
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-mobile-v1.webp')), true);
});
