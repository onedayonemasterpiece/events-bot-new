import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

test('Home HeroTalk is mosaic-led and page-end uses the same renderer', async () => {
  const [home, hero, deck, end] = await Promise.all([
    read('../src/pages/index.astro'),
    read('../src/components/HomeHeroTalk.astro'),
    read('../src/lib/homeHeroTalk.ts'),
    read('../src/components/HeroTalkPageEnd.astro'),
  ]);
  assert.match(home, /buildHomeHeroTalkDeck/u);
  assert.match(home, /<HeroTalkPageEnd/u);
  assert.match(end, /<HomeHeroTalk scenes=\{scenes\}/u);
  assert.match(hero, /data-home-hero-mosaic/u);
  assert.match(hero, /width:100%;/u);
  assert.doesNotMatch(hero, /margin-left:calc\(50% - 50vw\)/u);
  assert.match(deck, /mode:\s*'photo-mosaic'/u);
  assert.doesNotMatch(deck, /mode:\s*'text-only'[\s\S]{0,160}event/u);
});

test('Home navigation is one DOM island that moves from flow to fixed position', async () => {
  const [home, nav, layout] = await Promise.all([
    read('../src/pages/index.astro'),
    read('../src/components/HomeQuickNav.astro'),
    read('../src/layouts/EventLayout.astro'),
  ]);
  assert.match(home, /homeFloatingNav/u);
  assert.equal((nav.match(/<nav class="home-quick-nav"/gu) || []).length, 1);
  assert.match(nav, /classList\.toggle\('is-docked'/u);
  assert.match(nav, /\.home-quick-nav\.is-docked\s*\{[\s\S]*position:\s*fixed/u);
  for (const label of ['Сегодня','Завтра','Выходные','Бесплатно','Выставки','Фестивали','Популярное','Необычное']) {
    assert.match(nav, new RegExp(label, 'u'));
  }
  assert.match(layout, /data-home-floating-nav/u);
  assert.match(layout, /body\[data-home-floating-nav="true"\] \.site-nav \{ display:none; \}/u);
  assert.match(layout, /overflow-x:\s*clip/u);
});
