import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

test('root is a noindex focus testing stub without the old preview CTA', async () => {
  const source = await read('../src/pages/index.astro');
  assert.match(source, /Фокус-группа/u);
  assert.match(source, /noindex,nofollow/u);
  assert.match(source, /не авторизует пользователя/u);
  assert.doesNotMatch(source, /PUBLIC_ROOT_PREVIEW_HREF/u);
  assert.doesNotMatch(source, /Открыть тестовую версию/u);
});

test('invite intake and secret hub state the local marker boundary', async () => {
  const [intake, secret, helper] = await Promise.all([
    read('../src/components/FocusGroupInviteIntake.astro'),
    read('../src/pages/zakrytaya-afisha/index.astro'),
    read('../src/lib/focus-group-prototype.ts'),
  ]);
  assert.match(intake, /history\.replaceState/u);
  assert.match(intake, /временная метка просмотра на 72 часа/u);
  assert.match(intake, /не вход и не защита/u);
  assert.match(secret, /UX-проверка, а не проверка авторизации/u);
  assert.match(secret, /readFocusPreviewMarker/u);
  assert.match(helper, /FOCUS_PREVIEW_MAX_BYTES = 384/u);
  assert.doesNotMatch(helper, /token:/u);
});

test('for-me uses tri-state native radios and separates inferred index', async () => {
  const [component, page] = await Promise.all([
    read('../src/components/InterestProfile.astro'),
    read('../src/pages/dlya-menya/index.astro'),
  ]);
  assert.match(component, /Чаще/u);
  assert.match(component, /Без предпочтения/u);
  assert.match(component, /Реже/u);
  assert.match(component, /type="radio"/u);
  assert.match(component, /<meter/u);
  assert.match(component, /Пока недостаточно данных/u);
  assert.doesNotMatch(component, /type="range"/u);
  assert.match(page, /data-focus-personal-tools/u);
  assert.match(page, /readFocusPreviewMarker/u);
});

test('end-state page clears preview access but preserves personalization continuity', async () => {
  const source = await read('../src/pages/fokus-gruppa/zavershenie/index.astro');
  assert.match(source, /time_elapsed/u);
  assert.match(source, /operator_closed/u);
  assert.match(source, /operator_cancelled/u);
  assert.match(source, /clearFocusPreviewMarker/u);
  assert.match(source, /Локальный профиль «Для меня»/u);
  assert.doesNotMatch(source, /removeItem\([^)]*focus-personalization/u);
});

test('feedback keeps overall NPS, usefulness, improvement and fact issue separate', async () => {
  const source = await read('../src/components/FocusGroupFeedback.astro');
  assert.match(source, /Общий relationship NPS/u);
  assert.match(source, /это не общий NPS/u);
  assert.match(source, /Предложить улучшение/u);
  assert.match(source, /data-feedback-panel="event_issue"/u);
  assert.match(source, /не меняет событие автоматически/u);
});
