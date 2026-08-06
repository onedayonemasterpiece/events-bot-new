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

test('prelaunch uses one shared emitter and no square pane stroke', () => {
  const light = source('site/src/styles/prelaunch-fit-v12.css');
  const visual = source('site/src/styles/prelaunch-fit-v27.css');
  assert.match(light, /\.prelaunch\[data-prelaunch-page\]::before[\s\S]*radial-gradient/u);
  assert.match(light, /Golden powder[\s\S]*radial-gradient/u);
  assert.match(light, /--prelaunch-seam:\s*#07090d/u);
  assert.match(visual, /\.prelaunch__mosaic::before[\s\S]*filter:\s*none\s*!important/u);
  assert.match(visual, /\.prelaunch__tile\s*\{[\s\S]*overflow:\s*hidden\s*!important/u);
  assert.match(visual, /background-image:[\s\S]*radial-gradient\(circle at 100% 100%[\s\S]*radial-gradient\(circle at 0 0/u);
  assert.match(visual, /\.prelaunch__tile::before[\s\S]*inset 0 1px 0/u);
  assert.doesNotMatch(visual, /0 0 0 calc\(var\(--corner-radius\)/u);
  assert.doesNotMatch(visual, /\.prelaunch__tile::before[^}]*radial-gradient/su);
});

test('prelaunch enhancement binds SVG artwork, adaptive grid and guarded form states', () => {
  const script = source('site/src/scripts/prelaunchExperience.ts');
  const guard = source('site/src/scripts/prelaunchEmailGuard.ts');
  const validator = source('site/src/lib/prelaunchEmail.ts');
  const component = source('site/src/components/PrelaunchExperience.astro');
  const index = source('site/src/pages/index.astro');

  assert.match(script, /gridTemplateColumns/u);
  assert.match(script, /dataset\.edge/u);
  assert.match(script, /dataset\.window/u);
  assert.match(script, /dataset\.accent/u);
  assert.match(script, /ke_prelaunch_notification_v1/u);
  assert.match(script, /Вы уже записаны/u);
  assert.match(script, /приятный сюрприз/u);
  assert.match(script, /prelaunch-updates-2026-v1/u);

  assert.match(component, /targetTileCount = 98/u);
  assert.match(component, /createElementNS\(namespace, 'svg'\)/u);
  assert.match(component, /viewBox', '110 95 1034 1034'/u);
  assert.match(component, /svg-rounded-clip/u);
  assert.match(component, /prelaunchEmailGuard/u);
  assert.match(component, /prelaunchExperience/u);
  assert.doesNotMatch(component, /getImageData|willReadFrequently|Uint32Array/u);

  assert.match(guard, /normalizePrelaunchEmail/u);
  assert.match(guard, /stopImmediatePropagation/u);
  assert.match(validator, /ASCII_VISIBLE/u);
  assert.match(validator, /EXPLICITLY_UNSAFE/u);
  assert.match(validator, /local\.includes\('\.\.'\)/u);
  assert.match(index, /PrelaunchExperience/u);
});

test('generated desktop and mobile lighting references are stored in the handoff', () => {
  const referenceDir = join(repoRoot, 'docs/features/static-site-pages/prelaunch-handoff/reference');
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-desktop-v1.webp')), true);
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-mobile-v1.webp')), true);
});
