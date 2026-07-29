import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');

test('focus programme stays on dedicated noindex/noarchive routes without replacing the public home', async () => {
  const [root, hub, invitation, collection, ending, secretHub, productionCheck] = await Promise.all([
    read('../src/pages/index.astro'),
    read('../src/pages/fokus-gruppa/index.astro'),
    read('../src/pages/fokus-gruppa/priglashenie/index.astro'),
    read('../src/pages/fokus-gruppa/kollektsiya/index.astro'),
    read('../src/pages/fokus-gruppa/zavershenie/index.astro'),
    read('../src/pages/zakrytaya-afisha/index.astro'),
    read('../scripts/check-production.mjs'),
  ]);
  assert.match(root, /HomeHeroTalk/u);
  assert.match(root, /HomeQuickNav/u);
  assert.match(root, /HomeColdStartFeed/u);
  assert.doesNotMatch(root, /Фокус-группа/u);
  assert.match(hub, /Фокус-группа/u);
  for (const source of [hub, invitation, collection, ending, secretHub]) {
    assert.match(source, /noindex,nofollow,noarchive/u);
    assert.match(source, /<meta name="referrer" content="no-referrer"/u);
    assert.match(source, /<link rel="canonical" href=\{absoluteUrl\('/u);
  }
  assert.match(productionCheck, /const focusPrivateRoute = file\.key === 'zakrytaya-afisha\/index\.html'/u);
  assert.match(productionCheck, /!focusPrivateRoute/u);
  assert.match(invitation, /FocusGroupInviteIntake/u);
});

test('invite intake and secret hub state the local marker boundary', async () => {
  const [intake, secret, helper] = await Promise.all([
    read('../src/components/FocusGroupInviteIntake.astro'),
    read('../src/pages/zakrytaya-afisha/index.astro'),
    read('../src/lib/focus-group-prototype.ts'),
  ]);
  assert.match(intake, /history\.replaceState/u);
  assert.match(intake, /метка участия на полные 30 дней/u);
  assert.match(intake, /не зависит от\s+настроек «Для меня»/u);
  assert.match(intake, /не подтверждение личности и не защита/u);
  assert.match(secret, /UX-проверка, а не проверка авторизации/u);
  assert.match(secret, /readFocusParticipationMarker/u);
  assert.match(helper, /FOCUS_PARTICIPATION_DURATION_MS = 30 \* 24/u);
  assert.doesNotMatch(helper, /FOCUS_PREVIEW/u);
  assert.doesNotMatch(helper, /token:/u);
});

test('email identity offers one message with link plus six-digit mobile OTP', async () => {
  const [intake, auth, otp] = await Promise.all([
    read('../src/components/FocusGroupInviteIntake.astro'),
    read('../src/lib/staticSiteAuth.ts'),
    read('../src/lib/emailOtp.ts'),
  ]);
  assert.match(intake, /Получить код и ссылку/u);
  assert.match(intake, /inputmode="numeric"/u);
  assert.match(intake, /autocomplete="one-time-code"/u);
  assert.match(intake, /pattern="\[0-9\]\{6\}"/u);
  assert.match(intake, /После шестой цифры[\s\S]*Enter нажимать не нужно/u);
  assert.match(intake, /window\.setInterval\(tick, 1000\)/u);
  assert.match(auth, /verifyEmailOtp/u);
  assert.match(auth, /verifyOtp/u);
  assert.match(otp, /EMAIL_OTP_LENGTH = 6/u);
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
  assert.match(page, /readFocusParticipationMarker/u);
});

test('end-state page clears participation but preserves personalization continuity', async () => {
  const source = await read('../src/pages/fokus-gruppa/zavershenie/index.astro');
  assert.match(source, /time_elapsed/u);
  assert.match(source, /operator_closed/u);
  assert.match(source, /operator_cancelled/u);
  assert.match(source, /clearFocusParticipationMarker/u);
  assert.match(source, /Локальный профиль «Для меня»/u);
  assert.doesNotMatch(source, /removeItem\([^)]*focus-personalization/u);
});

test('account logout and explicit programme exit stay separate on focus surfaces', async () => {
  const [personal, hub, runtime] = await Promise.all([
    read('../src/pages/dlya-menya/index.astro'),
    read('../src/pages/zakrytaya-afisha/index.astro'),
    read('../src/components/auth/StaticSiteAuthRuntime.astro'),
  ]);
  assert.match(personal, /data-static-auth-logout/u);
  assert.match(personal, /30-дневное участие/u);
  assert.match(hub, /data-secret-clear>Выйти из фокус-группы/u);
  assert.match(hub, /clearFocusParticipationMarker\(focusStorage\)/u);
  assert.doesNotMatch(runtime, /clearFocusParticipationMarker|kenigevents:focus-participation/u);
});

test('feedback keeps overall NPS, usefulness, improvement and fact issue separate', async () => {
  const source = await read('../src/components/FocusGroupFeedback.astro');
  assert.match(source, /Общий relationship NPS/u);
  assert.match(source, /это не общий NPS/u);
  assert.match(source, /Предложить улучшение/u);
  assert.match(source, /data-feedback-panel="event_issue"/u);
  assert.match(source, /не меняет событие автоматически/u);
});
