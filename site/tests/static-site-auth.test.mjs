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
  assert.doesNotMatch(menu, />Войти<\/a>/u);
  assert.match(personal, /data-static-auth-login/u);
  assert.match(personal, /data-static-auth-logout/u);
  assert.match(personal, /data-static-auth-name/u);
  assert.doesNotMatch(personal, /Войти через поиск/u);
  assert.match(layout, /import StaticSiteAuthRuntime from '\.\.\/components\/auth\/StaticSiteAuthRuntime\.astro'/u);
  assert.equal((layout.match(/<StaticSiteAuthRuntime \/>/gu) || []).length, 1);
});

test('Search advertises the mobile search action and submits Enter through the native form path', () => {
  assert.match(search, /<textarea[^>]+enterkeyhint="search"/u);
  assert.match(search, /input\?\.addEventListener\('keydown'/u);
  assert.match(search, /event\.isComposing/u);
  assert.match(search, /event\.keyCode === 229/u);
  assert.match(search, /event\.preventDefault\(\);\s*form\?\.requestSubmit\(\);/u);
});

test('initial streaming header timeout receives one bounded JSON rescue', () => {
  assert.match(search, /error\?\.message !== 'search_fetch_headers_timeout'/u);
  assert.match(search, /invokeEventSearchJson\(endpoint, body, session, 'headers_stalled'/u);
  assert.match(search, /reason === 'stream_stalled' \|\| reason === 'headers_stalled'/u);
  assert.match(search, /use_llm_verifier: false, stream_rescue: true/u);
});
