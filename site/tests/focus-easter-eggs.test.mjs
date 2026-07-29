import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  FOCUS_EGG_PLACEMENT_VERSION,
  capFocusParticipation,
  createFocusEggPrototypeState,
  getFgE12Placement,
  getFocusEggCollectionProgress,
  markFocusEggFound,
  parseFocusEggPrototypeState,
  resolveFocusEggState,
} from '../src/lib/focus-easter-eggs.ts';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

test('egg state exposes locked, eligible, found and unavailable without losing a find', () => {
  assert.equal(resolveFocusEggState({}), 'locked');
  assert.equal(resolveFocusEggState({ eligible: true }), 'eligible');
  assert.equal(resolveFocusEggState({ unavailable: true }), 'unavailable');
  assert.equal(resolveFocusEggState({ found: true, unavailable: true }), 'found');
});

test('collection progress is primary and excludes uniformly unavailable eggs', () => {
  assert.deepEqual(
    getFocusEggCollectionProgress(['found', 'eligible', 'locked', 'unavailable']),
    { found: 1, eligible: 3, coverage: 1 / 3 },
  );
});

test('participation is capped separately at 40 points and 7 categories', () => {
  assert.deepEqual(capFocusParticipation({ points: 999, categories: 99 }), {
    points: 40,
    maxPoints: 40,
    categories: 7,
    maxCategories: 7,
  });
  assert.equal(capFocusParticipation({ points: -4, categories: -1 }).points, 0);
});

test('FG-E12 fails closed for fewer than three distinct renderable events', () => {
  assert.equal(getFgE12Placement([]), null);
  assert.equal(getFgE12Placement(['event-a', 'event-b']), null);
  assert.equal(getFgE12Placement(['event-a', 'event-a', 'event-b']), null);
});

test('FG-E12 has one stable insertion immediately after canonical item three', () => {
  const first = getFgE12Placement(['event-a', 'event-b', 'event-c']);
  const reordered = getFgE12Placement(['event-c', 'event-a', 'event-b', 'event-d']);

  assert.deepEqual(first, {
    eggId: 'FG-E12',
    anchorId: 'focus-egg-FG-E12',
    placementBundleId: `${FOCUS_EGG_PLACEMENT_VERSION}:FG-E12`,
    insertAfterRenderableItem: 3,
    state: 'eligible',
  });
  assert.equal(reordered?.placementBundleId, first?.placementBundleId);
  assert.equal(reordered?.insertAfterRenderableItem, 3);
});

test('FG-E12 remains found after the current saved list later becomes short', () => {
  const initial = createFocusEggPrototypeState();
  const once = markFocusEggFound(initial, 'FG-E12');
  const twice = markFocusEggFound(once, 'FG-E12');

  assert.equal(once.foundEggIds.filter((id) => id === 'FG-E12').length, 1);
  assert.strictEqual(twice, once);
  assert.equal(getFgE12Placement(['a', 'b', 'c'], once.foundEggIds)?.state, 'found');
  assert.deepEqual(parseFocusEggPrototypeState(JSON.stringify(once)), once);
  assert.equal(getFgE12Placement(['a', 'b'], once.foundEggIds), null);
  assert.ok(once.foundEggIds.includes('FG-E12'), 'short list must not erase the prior find');
});

test('collection surface keeps pending prize copy and plain-language boundaries', async () => {
  const page = await read('../src/pages/fokus-gruppa/kollektsiya/index.astro');
  const visibleCopy = page
    .replace(/^---[\s\S]*?---/u, '')
    .replace(/<script>[\s\S]*?<\/script>/gu, '')
    .replace(/<style>[\s\S]*?<\/style>/gu, '');

  assert.match(page, /Правила готовятся/u);
  assert.match(page, /один приз — <strong>два билета в театр<\/strong>/u);
  assert.match(page, /не начисляют[\s\S]*не[\s\n]*дают право/u);
  assert.match(page, /Результат на этом устройстве пока не участвует в розыгрыше/u);
  assert.match(page, /Проверьте сайт на двух экранах/u);
  assert.match(page, /одно полезное действие на телефоне и одно[\s\n]*на компьютере/u);
  assert.match(page, /Результат от этого не уменьшится/u);
  assert.match(page, /не даст преимущества/u);
  assert.match(page, /40/u);
  assert.match(page, /7/u);
  assert.doesNotMatch(page, /Ваш конкурсный балл/u);
  assert.doesNotMatch(page, /на любой спектакль/iu);
  assert.doesNotMatch(visibleCopy, /prototype|handoff|device receipts?|single-device|NPS|локальная иллюстрация/iu);
});

test('saved-list prototype inserts one FG-E12 anchor after source item three only', async () => {
  const demo = await read('../src/components/FocusEggSavedListDemo.astro');

  assert.match(demo, /index === 2[\s\S]*id="focus-egg-FG-E12"/u);
  assert.equal((demo.match(/id="focus-egg-FG-E12"/gu) || []).length, 1);
  assert.match(demo, /getFgE12Placement\(visibleIds/u);
  assert.match(demo, /переносится после последней карточки/u);
  assert.match(demo, /Перейти к пасхалке после третьего события/u);
  assert.match(demo, /клавиатурой и screen reader/u);
  assert.match(demo, /markFocusEggFound/u);
});

test('closed hub links to the collection prototype', async () => {
  const hub = await read('../src/pages/zakrytaya-afisha/index.astro');
  assert.match(hub, /\/fokus-gruppa\/kollektsiya\//u);
  assert.match(hub, /Посмотреть найденные пасхалки/u);
});
