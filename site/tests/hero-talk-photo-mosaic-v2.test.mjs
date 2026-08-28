import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('HeroTalk v2 is the Home Photo Mosaic owner rather than the legacy event-feature hero', async () => {
  const [component, legacy, home] = await Promise.all([
    read('src/components/HomeHeroTalk.astro'),
    read('src/components/HomeHeroTalkLegacy.astro'),
    read('src/pages/index.astro'),
  ]);
  assert.match(component, /version\?: 2/u);
  assert.match(component, /mode\?: 'photo-mosaic'/u);
  assert.match(component, /data-hero-talk-version=\{version\}/u);
  assert.match(component, /data-ds-component="HeroTalk"/u);
  assert.match(component, /data-ds-version=\{version\}/u);
  assert.match(component, /data-hero-talk-mode=\{mode\}/u);
  assert.match(component, /Array\.from\(\{ length: 20 \}/u);
  assert.match(component, /eventImageUrl\(event\.image_url\)/u);
  assert.match(component, /displayDateTimeWithWeekday\(event\)/u);
  assert.match(component, /root\.classList\.add\('is-exiting'\)/u);
  assert.match(component, /prefers-reduced-motion: reduce/u);
  assert.match(component, /pointerdown/u);
  assert.match(component, /focusin/u);
  assert.match(home, /<HomeHeroTalk events=\{feed\.slice\(0, 3\)\} version=\{2\} mode="photo-mosaic"/u);
  assert.doesNotMatch(home, /HomeHeroTalkLegacy/u);
  assert.match(legacy, /Куда пойти — без лишнего шума/u);
});

test('HeroTalk material redesign is versioned and the rejected v1 is catalog-only', async () => {
  const [catalog, check] = await Promise.all([
    read('src/pages/lab/design-system/index.astro'),
    read('scripts/check-design-system.mjs'),
  ]);
  assert.match(catalog, /data-ds-component="HeroTalk" data-ds-version="2"/u);
  assert.match(catalog, /data-ds-component="HeroTalk" data-ds-version="1" data-ds-replaced-by="HeroTalk@2"/u);
  assert.match(catalog, /<HomeHeroTalkLegacy event=\{photoEvent\}/u);
  assert.match(check, /HeroTalk static v1 -> Photo Mosaic v2 migration/u);
});
