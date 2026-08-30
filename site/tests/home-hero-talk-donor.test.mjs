import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const component = readFileSync(new URL('../src/components/HomeHeroTalk.astro', import.meta.url), 'utf8');
const page = readFileSync(new URL('../src/pages/index.astro', import.meta.url), 'utf8');

test('home uses the accepted 2026-07-30 Hero-talk donor rather than a card-like hero', () => {
  assert.match(page, /buildHomeHeroTalkDeck\(\s*feed,/u);
  assert.match(page, /<HomeHeroTalk scenes=\{heroScenes\} \/>/u);

  assert.match(component, /Array\.from\(\{ length: 100 \}/u);
  assert.match(component, /--mosaic-columns:16/u);
  assert.match(component, /grid-template-rows:repeat\(5/u);
  assert.match(component, /width:75vw/u);
  assert.match(component, /data-home-hero-fragment/u);
  assert.match(component, /home-hero-talk__fragment:last-of-type::after/u);
  assert.match(component, /@keyframes home-hero-cursor/u);
  assert.match(component, /data-home-hero-mosaic/u);
  assert.match(component, /@media \(max-width:1023px\)[\s\S]*home-hero-talk__media \{ display:none; \}/u);

  assert.doesNotMatch(component, /hero-talk__details/u);
  assert.doesNotMatch(component, /hero-talk__scene-controls/u);
  assert.doesNotMatch(component, /class="cta-button"/u);
});
