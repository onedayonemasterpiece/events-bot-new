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
  const status = { textContent: '' };
  const guidance = { hidden: false };
  const controller = createFocusPwaInstallController({
    windowRef,
    navigatorRef,
    root,
    button,
    status,
    guidance,
  });
  return { windowRef, root, button, status, guidance, controller };
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

test('focus manifest uses a state-aware start controller and keeps the secret hub shortcut', async () => {
  const manifest = await read('src/pages/fokus-gruppa/manifest.webmanifest.ts');
  assert.match(manifest, /\/fokus-gruppa\/priglashenie\/\?launch=pwa/u);
  assert.match(manifest, /const secretUrl = withBase\('\/zakrytaya-afisha\/'\)/u);
  assert.match(manifest, /focus-group-icon\.png/u);
  assert.match(manifest, /sizes: '192x192'/u);
  assert.match(manifest, /sizes: '512x512'/u);
  assert.match(manifest, /display: 'standalone'/u);
  assert.match(manifest, /prefer_related_applications: false/u);
  assert.match(manifest, /application\/manifest\+json/u);
});

test('mobile onboarding mounts the focus manifest, supplied logo and explicit no-confirmation path', async () => {
  const [page, intake, action] = await Promise.all([
    read('src/pages/fokus-gruppa/priglashenie/index.astro'),
    read('src/components/FocusGroupInviteIntake.astro'),
    read('src/components/FocusPwaInstallAction.astro'),
  ]);
  assert.match(page, /fokus-gruppa\/manifest\.webmanifest/u);
  assert.match(page, /apple-touch-icon/u);
  assert.match(intake, /assets\/pwa\/focus-group-icon\.png/u);
  assert.match(intake, /Продолжить в фокус-группе без подтверждения/u);
  assert.match(intake, /activateFocusParticipation/u);
  assert.match(intake, /launchFromPwa/u);
  assert.doesNotMatch(intake, /авторизац/iu);
  assert.match(action, /Установка и последующий[\s\S]*только по вашей команде/u);
  assert.match(action, /Android:[\s\S]*iPhone\/iPad:/u);
});

test('focus install action preserves one-shot beforeinstallprompt and honest installed state', async () => {
  const state = fixture();
  assert.equal(state.root.hidden, false);
  assert.equal(state.button.hidden, true);

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
  state.button.dispatch('click', { preventDefault() {} });
  state.button.dispatch('click', { preventDefault() {} });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(installEvent.promptCalls, 1);

  state.windowRef.dispatch('appinstalled');
  assert.equal(state.root.dataset.focusPwaInstalled, 'true');
  assert.match(state.status.textContent, /запуск остаётся действием пользователя/u);
});

test('iOS guidance never claims that the page can open a system install prompt', () => {
  const state = fixture({
    userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)',
  });
  assert.equal(state.button.hidden, true);
  assert.match(state.status.textContent, /Поделиться.*На экран Домой/u);
});
