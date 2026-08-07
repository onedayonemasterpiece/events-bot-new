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

test('prelaunch scene uses responsive generated backgrounds with no live glass grid', () => {
  const page = source('site/src/components/PrelaunchPage.astro');
  const css = source('site/src/styles/prelaunch-static.css');

  assert.match(page, /data-static-background="generated-desktop-mobile-v1"/u);
  assert.match(page, /data-prelaunch-static-picture/u);
  assert.match(page, /data-prelaunch-static-image/u);
  assert.match(page, /data-prelaunch-static-composite/u);
  assert.match(page, /data-prelaunch-static-artwork/u);
  assert.match(page, /prelaunch-scene-desktop\.webp/u);
  assert.match(page, /prelaunch-scene-mobile\.webp/u);
  assert.match(page, /<picture/u);
  assert.match(page, /<source[\s\S]*max-width: 899px/u);
  assert.match(page, /import '\.\.\/styles\/prelaunch-static\.css'/u);
  assert.doesNotMatch(page, /data-prelaunch-mosaic/u);
  assert.doesNotMatch(page, /data-prelaunch-seams/u);
  assert.doesNotMatch(page, /data-prelaunch-tile/u);
  assert.doesNotMatch(page, /prelaunchScene/u);
  assert.doesNotMatch(page, /prelaunch-fit-v\d+\.css/u);

  assert.match(css, /Lightweight prelaunch holder/u);
  assert.match(css, /responsive fallback picture/u);
  assert.match(css, /--prelaunch-static-tile-mask/u);
  assert.match(css, /mask-image:\s*var\(--prelaunch-static-tile-mask\)/u);
  assert.match(css, /background-repeat:\s*repeat/u);
  assert.match(css, /object-fit:\s*cover/u);
  assert.match(css, /contain:\s*strict/u);
  assert.match(css, /touch-action:\s*manipulation/u);
  assert.doesNotMatch(css, /backdrop-filter/u);
  assert.doesNotMatch(css, /animation:/u);
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

test('prelaunch consent is a prominent step before the submit action', () => {
  const page = source('site/src/components/PrelaunchPage.astro');
  const consentCss = source('site/src/styles/prelaunch-consent.css');

  assert.match(page, /prelaunch-consent\.css/u);
  assert.match(
    page,
    /prelaunch-form__row[\s\S]*prelaunch-form__field[\s\S]*prelaunch-form__consent[\s\S]*data-prelaunch-submit/u,
  );
  assert.match(page, /prelaunch-form__consent-copy[\s\S]*Согласие на письма/u);
  assert.match(consentCss, /grid-template-areas:[\s\S]*"field field"[\s\S]*"consent submit"/u);
  assert.match(consentCss, /grid-area:\s*consent/u);
  assert.match(consentCss, /accent-color:\s*#e99b6f/u);
  assert.match(consentCss, /width:\s*22px[\s\S]*height:\s*22px/u);
  assert.match(consentCss, /:focus-within/u);
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
  assert.match(page, /prelaunch-static\.css/u);
});

test('desktop and mobile reference assets remain stored for manual review', () => {
  const referenceDir = join(repoRoot, 'docs/features/static-site-pages/prelaunch-handoff/reference');
  assert.equal(existsSync(join(referenceDir, 'PWA-icon.webp')), true);
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-desktop-v1.webp')), true);
  assert.equal(existsSync(join(referenceDir, 'generated-lighting-mobile-v1.webp')), true);
});
