import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { createFocusPwaInstallController } from '../src/lib/focus-pwa-install-controller.ts';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');
const readBytes = (path) => readFile(new URL(`../${path}`, import.meta.url));

class FakeTarget {
  constructor() {
    this.listeners = new Map();
  }
  addEventListener(type, listener) {
    this.listeners.set(type, [...(this.listeners.get(type) || []), listener]);
  }
  removeEventListener(type, listener) {
    this.listeners.set(
      type,
      (this.listeners.get(type) || []).filter((candidate) => candidate !== listener),
    );
  }
  dispatch(type, event = {}) {
    for (const listener of this.listeners.get(type) || []) listener(event);
  }
}

function fixture({ userAgent = 'Mozilla/5.0 (Linux; Android 15)', standalone = false } = {}) {
  const windowRef = new FakeTarget();
  windowRef.location = { search: '' };
  windowRef.matchMedia = () => ({ matches: standalone });
  const navigatorRef = { userAgent, platform: '', maxTouchPoints: 0 };
  const root = new FakeTarget();
  root.hidden = false;
  root.dataset = {};
  const button = new FakeTarget();
  button.hidden = false;
  button.disabled = false;
  const openButton = new FakeTarget();
  openButton.hidden = false;
  const status = { textContent: '' };
  const guidance = { hidden: false };
  const controller = createFocusPwaInstallController({
    windowRef,
    navigatorRef,
    root,
    button,
    openButton,
    status,
    guidance,
  });
  return { windowRef, root, button, openButton, status, guidance, controller };
}

test('focus launcher artwork is an exact copy of the supplied reference PNG', async () => {
  const [reference, publicCopy] = await Promise.all([
    readFile(new URL('../../docs/reference/PWA-icon.png', import.meta.url)),
    readBytes('public/assets/pwa/focus-group-icon.png'),
  ]);
  assert.deepEqual(publicCopy, reference);
  assert.deepEqual([...publicCopy.subarray(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10]);
  assert.equal(publicCopy.readUInt32BE(16), 1254);
  assert.equal(publicCopy.readUInt32BE(20), 1254);
});

test('focus compatibility manifest installs the permanent Announcements app', async () => {
  const manifest = await read('src/pages/fokus-gruppa/manifest.webmanifest.ts');
  assert.match(manifest, /id: scope/u);
  assert.match(manifest, /name: 'Анонсы'/u);
  assert.match(manifest, /short_name: 'Анонсы'/u);
  assert.match(manifest, /start_url: withBase\('\/\?launch=pwa'\)/u);
  assert.doesNotMatch(manifest, /Анонсы Lab|focus-group-icon|zakrytaya-afisha/u);
  assert.match(manifest, /sizes: '192x192'/u);
  assert.match(manifest, /sizes: '512x512'/u);
  assert.match(manifest, /purpose: 'maskable'/u);
  assert.match(manifest, /display: 'standalone'/u);
  assert.match(manifest, /prefer_related_applications: false/u);
  assert.match(manifest, /application\/manifest\+json/u);
});

test('mobile onboarding mounts the permanent manifest and explicit no-confirmation path', async () => {
  const [page, intake, action] = await Promise.all([
    read('src/pages/fokus-gruppa/priglashenie/index.astro'),
    read('src/components/FocusGroupInviteIntake.astro'),
    read('src/components/FocusPwaInstallAction.astro'),
  ]);
  assert.match(page, /\/manifest\.webmanifest/u);
  assert.match(page, /apple-mobile-web-app-title" content="Анонсы"/u);
  assert.doesNotMatch(page, /Анонсы Lab/u);
  assert.match(page, /apple-touch-icon/u);
  assert.match(intake, /assets\/pwa\/announcements-brand-v2-192\.png/u);
  assert.match(intake, /width="192" height="192"/u);
  assert.match(intake, />\s*Пропустить\s*</u);
  assert.match(intake, /Пройти подключение заново на этом устройстве/u);
  assert.match(intake, /Выйти и выбрать другой способ/u);
  assert.match(intake, /activateFocusParticipation/u);
  assert.match(intake, /launchFromApp/u);
  assert.doesNotMatch(intake, /авторизац/iu);
  assert.match(action, /Установить «Анонсы»/u);
  assert.match(action, /Открыть «Анонсы»/u);
  assert.match(action, /Продолжить на сайте/u);
  assert.match(action, /Почти не занимает места/u);
});

test('focus install action preserves one-shot beforeinstallprompt and honest installed state', async () => {
  const state = fixture();
  assert.equal(state.root.hidden, false);
  assert.equal(state.button.hidden, true);
  assert.equal(state.openButton.hidden, false);

  const installEvent = {
    prevented: false,
    promptCalls: 0,
    preventDefault() { this.prevented = true; },
    async prompt() {
      this.promptCalls += 1;
      return { outcome: 'accepted' };
    },
  };
  state.windowRef.dispatch('beforeinstallprompt', installEvent);
  assert.equal(installEvent.prevented, true);
  assert.equal(state.button.hidden, false);
  assert.equal(state.openButton.hidden, true);
  state.button.dispatch('click', { preventDefault() {} });
  state.button.dispatch('click', { preventDefault() {} });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(installEvent.promptCalls, 1);

  state.windowRef.dispatch('appinstalled');
  assert.equal(state.root.dataset.focusPwaInstalled, 'true');
  assert.equal(state.openButton.hidden, false);
  assert.match(state.status.textContent, /Открыть “Анонсы”.*главного экрана/u);
});

test('iOS guidance never claims that the page can open a system install prompt', () => {
  const state = fixture({
    userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)',
  });
  assert.equal(state.button.hidden, true);
  assert.match(state.status.textContent, /Поделиться.*На экран Домой/u);
});
