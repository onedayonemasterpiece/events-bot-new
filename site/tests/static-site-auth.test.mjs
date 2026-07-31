import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const auth = read('src/lib/staticSiteAuth.ts');
const runtime = read('src/components/auth/StaticSiteAuthRuntime.astro');
const search = read('src/components/AuthorizedEventSearch.astro');
const menu = read('src/components/Reference4MobileMenu.astro');
const personal = read('src/pages/dlya-menya/index.astro');
const layout = read('src/layouts/EventLayout.astro');

test('one shared static auth controller owns PKCE and never broadcasts tokens through DOM events', () => {
  assert.match(auth, /const CONTROLLER_KEY = '__KENIGEVENTS_STATIC_SITE_AUTH_V1__'/u);
  assert.match(auth, /flowType: 'pkce'/u);
  assert.match(auth, /detectSessionInUrl: false/u);
  assert.match(auth, /exchangeCodeForSession\(code\)/u);
  assert.match(auth, /SameSite=Lax; Secure/u);
  assert.match(auth, /isPkceCodeVerifierKey/u);
  assert.doesNotMatch(auth, /dispatchEvent|CustomEvent/u);
  assert.doesNotMatch(runtime, /access_token|refresh_token/u);
  assert.doesNotMatch(search, /createClient|exchangeCodeForSession|signInWithOAuth/u);
  assert.match(search, /getStaticSiteAuth/u);
});

test('global runtime binds menu and Personal auth views without routing login through Search', () => {
  assert.match(runtime, /data-static-auth-login/u);
  assert.match(runtime, /data-static-auth-signed-in/u);
  assert.match(runtime, /auth\.signIn\(\)/u);
  assert.match(runtime, /auth\.signOut\(\)/u);
  assert.match(menu, /button type="button" data-static-auth-login/u);
  assert.match(menu, /data-static-auth-name/u);
  assert.match(menu, /data-static-auth-logout/u);
  assert.doesNotMatch(menu, />Войти<\/a>/u);
  assert.match(personal, /data-static-auth-login/u);
  assert.match(personal, /data-static-auth-logout/u);
  assert.match(personal, /data-static-auth-name/u);
  assert.doesNotMatch(personal, /Войти через поиск/u);
  assert.match(layout, /import StaticSiteAuthRuntime from '\.\.\/components\/auth\/StaticSiteAuthRuntime\.astro'/u);
  assert.equal((layout.match(/<StaticSiteAuthRuntime \/>/gu) || []).length, 1);
});

test('account logout is explicit and cannot clear focus participation or personalization', () => {
  assert.match(auth, /const \{ error \} = await this\.client\.auth\.signOut\(\)/u);
  assert.match(auth, /if \(error\)[\s\S]*Не удалось выйти из аккаунта/u);
  assert.match(auth, /message: 'Вы вышли из аккаунта\.'/u);
  assert.doesNotMatch(auth, /clearFocusParticipationMarker|FOCUS_PARTICIPATION_STORAGE_KEY/u);
  assert.doesNotMatch(runtime, /clearFocusParticipationMarker|localStorage\.clear|sessionStorage\.clear/u);
  assert.match(personal, /Выход завершает только[\s\S]*Supabase-сессию/u);
  assert.match(personal, /30-дневное участие[\s\S]*остаются/u);
});

test('focus identity supports real email OTP and Yandex linking through the shared controller', () => {
  const auth = readFileSync(new URL('../src/lib/staticSiteAuth.ts', import.meta.url), 'utf8');
  const intake = readFileSync(new URL('../src/components/FocusGroupInviteIntake.astro', import.meta.url), 'utf8');
  const invitation = readFileSync(new URL('../src/pages/fokus-gruppa/priglashenie/index.astro', import.meta.url), 'utf8');
  assert.match(auth, /async signInWithEmailOtp/u);
  assert.match(auth, /this\.client\.auth\.signInWithOtp/u);
  assert.match(auth, /emailRedirectTo:\s*redirectTo/u);
  assert.match(auth, /StaticSiteEmailOtpStatus = 'accepted' \| 'rate_limited' \| 'ambiguous' \| 'request_failed'/u);
  assert.match(auth, /async verifyEmailOtp/u);
  assert.match(auth, /\/\^\\d\{6\}\$\/u\.test\(normalizedToken\)/u);
  assert.match(auth, /token_hash:\s*tokenHash/u);
  assert.match(auth, /async registerFocusGroupParticipant/u);
  assert.match(auth, /p_communication_opt_in:/u);
  assert.match(auth, /async resetForOnboardingTest/u);
  assert.match(auth, /async linkYandexIdentity/u);
  assert.match(auth, /this\.client\.auth\.linkIdentity/u);
  assert.match(intake, /new URL\(intake\.cleanHref, window\.location\.origin\)\.href/u);
  assert.match(intake, /auth\.signInWithEmailOtp\(email, emailRedirectTo\)/u);
  assert.match(intake, /auth\.linkYandexIdentity\(\)/u);
  assert.match(intake, /введённый адрес локально не сохраняется/u);
  assert.match(invitation, /<StaticSiteAuthRuntime \/>/u);
  assert.doesNotMatch(intake, /Макет не отправлял код|провайдер не запускается/u);
});

test('bespoke focus hub exposes auth state while keeping leave-focus as the only membership action', () => {
  const focusHub = read('src/pages/zakrytaya-afisha/index.astro');
  assert.match(focusHub, /<StaticSiteAuthRuntime \/>/u);
  assert.match(focusHub, /data-static-auth-login/u);
  assert.match(focusHub, /data-static-auth-name/u);
  assert.match(focusHub, /data-static-auth-logout/u);
  assert.match(focusHub, /data-secret-clear>Выйти из фокус-группы/u);
  assert.match(focusHub, /data-secret-clear[\s\S]*clearFocusParticipationMarker/u);
  assert.doesNotMatch(focusHub, /data-static-auth-logout[^>]*data-secret-clear|data-secret-clear[^>]*data-static-auth-logout/u);
});

test('Search advertises the mobile search action and submits Enter through the native form path', () => {
  assert.match(search, /<textarea[^>]+enterkeyhint="search"/u);
  assert.match(search, /input\?\.addEventListener\('keydown'/u);
  assert.match(search, /event\.isComposing/u);
  assert.match(search, /event\.keyCode === 229/u);
  assert.match(search, /event\.preventDefault\(\);\s*form\?\.requestSubmit\(\);/u);
});

test('cost-bearing Search POST is selected once and never rescued by a duplicate POST', () => {
  assert.match(search, /Search is a cost-bearing POST/u);
  assert.doesNotMatch(search, /headers_stalled|stream_rescue|json_retry|rescueStalledStream/u);
  assert.match(search, /authController\?\.transport\?\.fetch/u);
});
