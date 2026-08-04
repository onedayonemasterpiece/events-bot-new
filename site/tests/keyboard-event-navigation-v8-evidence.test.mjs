import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('V8 product model preserves the visual card graph and Enter route', async () => {
  const model = await read('../docs/features/static-site-pages/keyboard-event-navigation-v8-product-model.md');
  assert.match(model, /Визуальный граф похожих карточек/u);
  assert.match(model, /`Enter` на корне карточки/u);
  assert.match(model, /не подлежат удалению без отдельного доказательства/u);
  assert.match(model, /Editorial \/ горизонтальное изображение/u);
  assert.match(model, /Split \/ вертикальная афиша/u);
});

test('owner requirements define universal start, reading, help and cross-page routes', async () => {
  const model = await read('../docs/features/static-site-pages/keyboard-event-navigation-v8-product-model.md');
  for (const phrase of [
    'keyboard_start_reliability',
    'Read stops внутри длинного описания',
    'Universal re-entry',
    'Контекстный help',
    'NEXT EVENT и back route',
    'Browser Back восстанавливают source card owner',
  ]) assert.match(model, new RegExp(phrase, 'u'));
});

test('artifact is an existing first-collection collectible, not a new keyboard entity', async () => {
  const model = await read('../docs/features/static-site-pages/keyboard-event-navigation-v8-product-model.md');
  assert.match(model, /не о новом «клавиатурном артефакте»/u);
  assert.match(model, /migratory-bird-ring/u);
  assert.match(model, /Кольцо перелётной птицы/u);
  assert.match(model, /related→continuation/u);
  assert.match(model, /не блокирует threshold/u);
});

test('scenario registry covers both families and recovery states', async () => {
  const registry = await read('../docs/testing/keyboard-event-navigation-scenarios.v2.yml');
  for (const token of [
    'editorial_horizontal',
    'split_vertical',
    'body_after_blur',
    'body_after_reload',
    'body_after_history_back',
    'body_after_bfcache',
    'body_after_dom_reorder',
    'KN-005',
    'Enter opens the selected card',
    'migratory-bird-ring',
  ]) assert.match(registry, new RegExp(token, 'u'));
});

test('browser evidence runner is characterization-first and emits exact artifacts', async () => {
  const runner = await read('e2e/keyboard-navigation-v8/run.mjs');
  assert.match(runner, /data-desktop-family/u);
  assert.match(runner, /editorial/u);
  assert.match(runner, /split/u);
  assert.match(runner, /keyboard_start_reliability/u);
  assert.match(runner, /context_recovery_accuracy/u);
  assert.match(runner, /KN-005-card-arrow-right/u);
  assert.match(runner, /KN-006-enter-selected-card/u);
  assert.match(runner, /KN-007-history-back-owner/u);
  assert.match(runner, /GAP_TARGET_NOT_IMPLEMENTED/u);
  assert.match(runner, /keyboard-navigation-evidence\.json/u);
  assert.match(runner, /context\.tracing\.start/u);
  assert.doesNotMatch(runner, /page\.waitForTimeout\([^)]*\d{5,}/u, 'runner must not hide unbounded waits');
});
