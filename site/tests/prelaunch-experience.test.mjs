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

test('prelaunch experience uses one shared emitter and opaque rounded-corner masks', () => {
  const css = source('site/src/styles/prelaunch-fit-v12.css');
  assert.match(css, /\.prelaunch\[data-prelaunch-page\]::before[\s\S]*radial-gradient/u);
  assert.match(css, /Golden powder[\s\S]*radial-gradient/u);
  assert.match(css, /--prelaunch-seam:\s*#07090d/u);
  assert.match(css, /overflow:\s*hidden;/u);
  assert.match(css, /border-radius:\s*0\s*!important/u);
  assert.match(css, /0 0 0 calc\(var\(--pane-radius\) \+ 2px\) var\(--prelaunch-seam\)/u);
  assert.doesNotMatch(
    css,
    /\.prelaunch__tile::before[^}]*radial-gradient/su,
    'individual panes must not paint local radial spotlights',
  );
  assert.match(css, /data-edge="hot"/u);
  assert.match(css, /data-accent="true"/u);
  assert.match(css, /grid-template-columns:\s*repeat\(6/u);
  assert.match(css, /width:\s*min\(88vw, 386px\)/u);
  assert.match(css, /transition:[\s\S]*background-color[\s\S]*border-color[\s\S]*box-shadow/u);
  assert.doesNotMatch(
    css,
    /transition:[^;]*backdrop-filter/su,
    'backdrop-filter must not animate across 72 panes',
  );
});

test('prelaunch enhancement binds adaptive grid and complete form states', () => {
  const script = source('site/src/scripts/prelaunchExperience.ts');
  const component = source('site/src/components/PrelaunchExperience.astro');
  const index = source('site/src/pages/index.astro');

  assert.match(script, /WINDOW_COUNT = 8/u);
  assert.match(script, /gridTemplateColumns/u);
  assert.match(script, /dataset\.edge/u);
  assert.match(script, /dataset\.window/u);
  assert.match(script, /dataset\.accent/u);
  assert.match(script, /attributeFilter:\s*\['data-state'\]/u);
  assert.match(script, /ke_prelaunch_notification_v1/u);
  assert.match(script, /Вы уже записаны/u);
  assert.match(script, /приятный сюрприз/u);
  assert.match(script, /Другой e-mail/u);
  assert.match(component, /prelaunch-fit-v12\.css/u);
  assert.match(component, /prelaunchExperience/u);
  assert.match(index, /PrelaunchExperience/u);
});

test('generated desktop and mobile lighting references are stored in the handoff', () => {
  const referenceDir = join(repoRoot, 'docs/features/static-site-pages/prelaunch-handoff/reference');
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-desktop-v1.png')), true);
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-mobile-v1.png')), true);
});
