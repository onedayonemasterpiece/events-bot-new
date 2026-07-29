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

test('invite intake keeps the 30-day boundary internal while user copy stays plain and sequential', async () => {
  const [intake, secret, helper] = await Promise.all([
    read('../src/components/FocusGroupInviteIntake.astro'),
    read('../src/pages/zakrytaya-afisha/index.astro'),
    read('../src/lib/focus-group-prototype.ts'),
  ]);
  assert.match(intake, /history\.replaceState/u);
  assert.match(intake, /data-intake-stage="install"/u);
  assert.match(intake, /data-intake-stage="identity"/u);
  assert.match(intake, /data-intake-stage="done"/u);
  assert.match(intake, /Хотите участвовать в розыгрыше двух билетов/u);
  assert.match(intake, /Афишей можно пользоваться и без подтверждения/u);
  assert.match(intake, /window\.location\.replace\(homeHref\)/u);
  assert.match(secret, /После исследования всё останется на месте/u);
  assert.match(secret, /readFocusParticipationMarker/u);
  assert.match(helper, /FOCUS_PARTICIPATION_DURATION_MS = 30 \* 24/u);
  const visibleIntake = intake
    .split('<script>')[0]
    .replace(/^---[\s\S]*?---/u, '')
    .replace(/<[^>]+>/gu, ' ');
  assert.doesNotMatch(visibleIntake, /локальн|fragment|membership|identity|localStorage|PKCE/iu);
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
  assert.match(intake, /После последней цифры продолжим автоматически/u);
  assert.match(intake, /Отправляем письмо/u);
  assert.match(intake, /aria-busy/u);
  assert.match(intake, /Отправить ещё раз/u);
  assert.match(intake, /window\.setInterval\(tick, 1000\)/u);
  assert.match(intake, /Проверьте почту: письмо могло уже прийти/u);
  assert.match(intake, /Ссылка не подтвердила вход.*шестизначный код/u);
  assert.match(intake, /if \(otpForm\) otpForm\.hidden = false/u);
  assert.match(intake, /data-focus-account-continue/u);
  assert.match(intake, /data-focus-account-logout/u);
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
  assert.match(source, /Ваш выбор новостей и настройки «Для меня»/u);
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

test('feedback keeps distinct questions in plain user language', async () => {
  const source = await read('../src/components/FocusGroupFeedback.astro');
  assert.match(source, /Общее впечатление/u);
  assert.match(source, /Оцените только эту страницу/u);
  assert.match(source, /Предложить улучшение/u);
  assert.match(source, /data-feedback-panel="event_issue"/u);
  assert.match(source, /не меняет событие автоматически/u);
  assert.doesNotMatch(source, /relationship NPS|page family|event_id|specimen|production|серверной проверки/iu);
});

test('active participants get one compact Lab feedback panel through the shared layout', async () => {
  const [panel, layout, surface] = await Promise.all([
    read('../src/components/FocusGroupLabPanel.astro'),
    read('../src/layouts/EventLayout.astro'),
    read('../src/lib/focus-group-surface.ts'),
  ]);
  assert.match(layout, /<FocusGroupLabPanel/u);
  assert.match(layout, /focusGroupPageFamily/u);
  assert.match(panel, /<FocusLabBadge/u);
  assert.match(panel, /Помогла ли вам эта страница/u);
  assert.match(panel, /Сообщить о проблеме/u);
  assert.match(panel, /Добавить скриншот/u);
  assert.match(panel, /Пригласить человека/u);
  assert.match(panel, /submit_focus_group_feedback_v1/u);
  assert.match(panel, /focus-feedback/u);
  assert.match(panel, /readFocusParticipationMarker/u);
  assert.match(surface, /event_detail/u);
  const visiblePanel = panel.split('<script>')[0].replace(/^---[\s\S]*?---/u, '').replace(/<[^>]+>/gu, ' ');
  assert.doesNotMatch(visiblePanel, /NPS|page family|PWA|localStorage|prototype/iu);
});

test('participant hub and prize explanation contain only user-facing copy', async () => {
  const [hub, prize] = await Promise.all([
    read('../src/pages/zakrytaya-afisha/index.astro'),
    read('../src/components/FocusGroupThankYou.astro'),
  ]);
  assert.match(prize, /Пользоваться афишей и участвовать в исследовании можно без подтверждения/u);
  assert.match(prize, /Если хотите участвовать в розыгрыше, подтвердите участие/u);
  assert.match(hub, /Фокус-группа · выбирайте как обычно/u);
  assert.doesNotMatch(
    prize,
    /NPS|дизлайк|проектируемой механике/iu,
  );
  assert.doesNotMatch(
    hub,
    /product prototype|продуктовый прототип|Specimen|production данных|секретный вход для приёмки/iu,
  );
});
