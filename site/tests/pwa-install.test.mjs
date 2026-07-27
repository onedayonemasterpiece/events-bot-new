import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  createPwaInstallController,
  isAndroidPlatform,
  isPresentationInstall,
  isStandaloneDisplay,
} from '../src/lib/pwa-install-controller.js';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');
const readBytes = (path) => readFile(new URL(`../${path}`, import.meta.url));

function pngDimensions(buffer) {
  assert.deepEqual([...buffer.subarray(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10]);
  return {
    width:buffer.readUInt32BE(16),
    height:buffer.readUInt32BE(20),
  };
}

class FakeTarget {
  constructor() {
    this.listeners = new Map();
  }
  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }
  removeEventListener(type, listener) {
    this.listeners.set(type, (this.listeners.get(type) || []).filter((item) => item !== listener));
  }
  dispatch(type, event = {}) {
    for (const listener of this.listeners.get(type) || []) listener(event);
  }
}

function fixture({
  navigatorRef = { userAgent:'Mozilla/5.0 (Linux; Android 15)' },
  standalone = false,
  locationRef = { search:'' },
} = {}) {
  const windowRef = new FakeTarget();
  windowRef.matchMedia = () => ({ matches:standalone });
  windowRef.location = locationRef;
  const button = new FakeTarget();
  button.hidden = false;
  button.disabled = false;
  const root = { hidden:false, dataset:{} };
  const status = { textContent:'' };
  const guidance = { hidden:true };
  const controller = createPwaInstallController({ windowRef, navigatorRef, locationRef, root, button, status, guidance });
  return { windowRef, navigatorRef, button, root, status, guidance, controller };
}

function installEvent(prompt = async () => {}) {
  return {
    prevented:false,
    promptCalls:0,
    preventDefault() { this.prevented = true; },
    async prompt() {
      this.promptCalls += 1;
      return prompt();
    },
  };
}

test('platform helpers accept Android UAData/fallback and reject standalone display', () => {
  assert.equal(isAndroidPlatform({ userAgentData:{ platform:'Android' }, userAgent:'' }), true);
  assert.equal(isAndroidPlatform({ userAgent:'Mozilla/5.0 (Linux; Android 14)' }), true);
  assert.equal(isAndroidPlatform({ userAgentData:{ platform:'iOS' }, userAgent:'iPhone' }), false);
  assert.equal(isStandaloneDisplay({ matchMedia:() => ({ matches:true }) }, {}), true);
  assert.equal(isStandaloneDisplay({ matchMedia:() => ({ matches:false }) }, { standalone:true }), true);
  assert.equal(isPresentationInstall({ search:'?install=presentation' }), true);
  assert.equal(isPresentationInstall({ search:'?install=footer' }), false);
});

test('install CTA remains hidden until a real Android beforeinstallprompt event', () => {
  const android = fixture();
  assert.equal(android.root.hidden, true);
  assert.equal(android.button.hidden, true);
  assert.equal(android.controller.ready, false);

  const event = installEvent();
  android.windowRef.dispatch('beforeinstallprompt', event);
  assert.equal(event.prevented, true);
  assert.equal(android.root.hidden, false);
  assert.equal(android.button.hidden, false);
  assert.equal(android.root.dataset.pwaInstallReady, 'true');
  assert.equal(android.controller.ready, true);

  const desktop = fixture({ navigatorRef:{ userAgent:'Mozilla/5.0 (X11; Linux x86_64)' } });
  const desktopEvent = installEvent();
  desktop.windowRef.dispatch('beforeinstallprompt', desktopEvent);
  assert.equal(desktopEvent.prevented, false);
  assert.equal(desktop.root.hidden, true);

  const standalone = fixture({ standalone:true });
  const standaloneEvent = installEvent();
  standalone.windowRef.dispatch('beforeinstallprompt', standaloneEvent);
  assert.equal(standaloneEvent.prevented, false);
  assert.equal(standalone.root.hidden, true);
});

test('one install event prompts once, hides before await, and a later event can re-arm after uninstall', async () => {
  let resolvePrompt;
  const pending = new Promise((resolve) => { resolvePrompt = resolve; });
  const state = fixture();
  const first = installEvent(() => pending);
  state.windowRef.dispatch('beforeinstallprompt', first);

  state.button.dispatch('click', { preventDefault() {} });
  state.button.dispatch('click', { preventDefault() {} });
  assert.equal(first.promptCalls, 1);
  assert.equal(state.root.hidden, true);
  assert.equal(state.button.hidden, true);
  assert.equal(state.controller.ready, false);
  resolvePrompt();
  await pending;
  await Promise.resolve();

  const second = installEvent();
  state.windowRef.dispatch('beforeinstallprompt', second);
  assert.equal(state.controller.ready, true);
  assert.equal(state.root.hidden, false);
  state.windowRef.dispatch('appinstalled');
  assert.equal(state.controller.ready, false);
  assert.equal(state.root.hidden, true);
  assert.equal(state.status.textContent, 'Приложение установлено.');
});

test('presentation QR flow explains the fallback immediately and upgrades to the native install button', async () => {
  const state = fixture({ locationRef:{ search:'?install=presentation' } });
  assert.equal(state.root.hidden, false);
  assert.equal(state.button.hidden, true);
  assert.equal(state.guidance.hidden, false);
  assert.equal(state.root.dataset.pwaInstallPresentation, 'true');
  assert.match(state.status.textContent, /Подготавливаем установку/u);

  const event = installEvent(async () => ({ outcome:'accepted' }));
  state.windowRef.dispatch('beforeinstallprompt', event);
  assert.equal(event.prevented, true);
  assert.equal(state.button.hidden, false);
  assert.equal(state.root.dataset.pwaInstallReady, 'true');

  state.button.dispatch('click', { preventDefault() {} });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(event.promptCalls, 1);
  assert.equal(state.root.hidden, false);
  assert.equal(state.button.hidden, true);
  assert.match(state.status.textContent, /Установка подтверждена/u);

  const installed = fixture({ standalone:true, locationRef:{ search:'?install=presentation' } });
  assert.equal(installed.root.hidden, false);
  assert.equal(installed.button.hidden, true);
  assert.equal(installed.guidance.hidden, true);
  assert.match(installed.status.textContent, /уже установлено/u);
});

test('site exposes a base-aware installable manifest and footer-owned controller', async () => {
  const [manifest, action, footer, layout, home, release, deploy] = await Promise.all([
    read('src/pages/manifest.webmanifest.ts'),
    read('src/components/PwaInstallAction.astro'),
    read('src/components/SiteFooter.astro'),
    read('src/layouts/EventLayout.astro'),
    read('src/pages/index.astro'),
    read('scripts/release-contract.mjs'),
    read('scripts/deploy-preview-yc.mjs'),
  ]);

  assert.match(manifest, /const scope = withBase\('\/'\)/u);
  assert.match(manifest, /PUBLIC_PWA_START_URL/u);
  assert.match(manifest, /const startUrl = configuredStartUrl \|\| siteHomeHref\(\)/u);
  assert.match(manifest, /start_url:startUrl/u);
  assert.match(manifest, /display:'standalone'/u);
  assert.match(manifest, /prefer_related_applications:false/u);
  assert.match(manifest, /name:'Анонсы'/u);
  assert.match(manifest, /short_name:'Анонсы'/u);
  assert.match(manifest, /announcements-brand-192\.png/u);
  assert.match(manifest, /announcements-brand-512\.png/u);
  assert.match(manifest, /announcements-brand-maskable-192\.png/u);
  assert.match(manifest, /announcements-brand-maskable-512\.png/u);
  assert.match(manifest, /purpose:'maskable'/u);
  assert.match(action, /data-pwa-install-root/u);
  assert.match(action, /data-pwa-install-button hidden/u);
  assert.match(action, /pwa-install-action__button\[hidden\]/u);
  assert.match(action, /Установить приложение/u);
  assert.match(footer, /<PwaInstallAction \/>/u);
  assert.match(layout, /manifest\.webmanifest'\)\}\?v=20260727-brand-icon/u);
  assert.match(home, /manifest\.webmanifest'\)\}\?v=20260727-brand-icon/u);
  assert.match(home, /rel="manifest" href=\{manifestHref\}/u);
  assert.match(home, /<PwaInstallAction \/>/u);
  assert.match(release, /'\.webmanifest': 'application\/manifest\+json; charset=utf-8'/u);
  assert.match(deploy, /manifest\.webmanifest[\s\S]*application\/manifest\+json; charset=utf-8/u);
});

test('brand and maskable launcher PNGs have the declared dimensions', async () => {
  for (const size of [192, 512]) {
    for (const variant of ['', '-maskable']) {
      const icon = await readBytes(`public/assets/pwa/announcements-brand${variant}-${size}.png`);
      assert.deepEqual(pngDimensions(icon), { width:size, height:size });
    }
  }
});
