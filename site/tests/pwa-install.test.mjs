import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  createPwaInstallController,
  isAndroidPlatform,
  isPresentationInstall,
  isStandaloneDisplay,
} from '../src/lib/pwa-install-controller.js';
import { createPwaTelemetryController } from '../src/lib/pwa-telemetry-controller.js';

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

function storageFixture(initial = {}) {
  const values = new Map(Object.entries(initial));
  return {
    getItem(key) { return values.get(key) || null; },
    setItem(key, value) { values.set(key, String(value)); },
    values,
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
  assert.equal(state.button.hidden, false);
  assert.equal(state.button.disabled, true);
  assert.equal(state.button.textContent, 'Устанавливается…');
  assert.match(state.status.textContent, /завершится в течение минуты/u);

  const installed = fixture({ standalone:true, locationRef:{ search:'?install=presentation' } });
  assert.equal(installed.root.hidden, false);
  assert.equal(installed.button.hidden, false);
  assert.equal(installed.button.textContent, 'Открыть Анонсы');
  assert.equal(installed.guidance.hidden, true);
  assert.match(installed.status.textContent, /Приложение открыто/u);
});

test('presentation install never describes an Android shortcut as the installed app', async () => {
  const [controller, focusAction] = await Promise.all([
    read('src/lib/pwa-install-controller.js'),
    read('src/components/FocusPwaInstallAction.astro'),
  ]);
  assert.doesNotMatch(controller, /Добавить на главный экран/u);
  assert.doesNotMatch(focusAction, /Добавить на главный экран/u);
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
  assert.match(manifest, /announcements-brand-v2-192\.png/u);
  assert.match(manifest, /announcements-brand-v2-512\.png/u);
  assert.match(manifest, /announcements-brand-v2-maskable-192\.png/u);
  assert.match(manifest, /announcements-brand-v2-maskable-512\.png/u);
  assert.match(manifest, /purpose:'maskable'/u);
  assert.match(action, /data-pwa-install-root/u);
  assert.match(action, /data-pwa-install-button hidden/u);
  assert.match(action, /pwa-install-action__button\[hidden\]/u);
  assert.match(action, /Установить приложение/u);
  assert.match(footer, /<PwaInstallAction \/>/u);
  assert.match(layout, /<PwaTelemetry \/>/u);
  assert.match(layout, /manifest\.webmanifest'\)\}\?v=20260727-brand-icon-v2/u);
  assert.match(home, /<EventLayout\b/u);
  assert.doesNotMatch(home, /<PwaInstallAction \/>/u);
  assert.doesNotMatch(home, /<PwaTelemetry \/>/u);
  assert.match(release, /'\.webmanifest': 'application\/manifest\+json; charset=utf-8'/u);
  assert.match(deploy, /manifest\.webmanifest[\s\S]*application\/manifest\+json; charset=utf-8/u);
});

test('brand and maskable launcher PNGs have the declared dimensions', async () => {
  for (const size of [192, 512]) {
    for (const variant of ['', '-maskable']) {
      const icon = await readBytes(`public/assets/pwa/announcements-brand-v2${variant}-${size}.png`);
      assert.deepEqual(pngDimensions(icon), { width:size, height:size });
    }
  }
});

test('compact PWA telemetry records install and one standalone open per app window', async () => {
  const windowRef = new FakeTarget();
  windowRef.matchMedia = () => ({ matches:true });
  const localStorageRef = storageFixture();
  const sessionStorageRef = storageFixture();
  const ids = [
    '12345678-1234-4234-8234-123456789abc',
    '87654321-4321-4321-8321-cba987654321',
  ];
  const requests = [];
  const fetchRef = async (url, options) => {
    requests.push({ url, options, body:JSON.parse(options.body) });
    return { ok:true };
  };
  const controller = createPwaTelemetryController({
    windowRef,
    navigatorRef:{ userAgent:'Android' },
    endpoint:'https://project.supabase.co/',
    publishableKey:'sb_publishable_test',
    fetchRef,
    cryptoRef:{ randomUUID:() => ids.shift() },
    localStorageRef,
    sessionStorageRef,
  });
  await new Promise((resolve) => setImmediate(resolve));

  assert.ok(controller);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, 'https://project.supabase.co/rest/v1/rpc/record_pwa_lifecycle_v1');
  assert.equal(requests[0].body.p_event_kind, 'standalone_open');
  assert.equal(requests[0].options.credentials, 'omit');
  assert.equal(requests[0].options.referrerPolicy, 'no-referrer');

  windowRef.dispatch('appinstalled');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(requests.length, 2);
  assert.equal(requests[1].body.p_event_kind, 'install');

  createPwaTelemetryController({
    windowRef,
    navigatorRef:{ userAgent:'Android' },
    endpoint:'https://project.supabase.co',
    publishableKey:'sb_publishable_test',
    fetchRef,
    cryptoRef:{ randomUUID:() => { throw new Error('must reuse ids'); } },
    localStorageRef,
    sessionStorageRef,
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(requests.length, 2);
});

test('PWA telemetry skips browser tabs, automation, and non-deduplicable storage', async () => {
  const fetches = [];
  const fetchRef = async (...args) => {
    fetches.push(args);
    return { ok:true };
  };
  const standardWindow = new FakeTarget();
  standardWindow.matchMedia = () => ({ matches:false });
  createPwaTelemetryController({
    windowRef:standardWindow,
    navigatorRef:{ userAgent:'Android' },
    endpoint:'https://project.supabase.co',
    publishableKey:'public',
    fetchRef,
    cryptoRef:{ randomUUID:() => '12345678-1234-4234-8234-123456789abc' },
    localStorageRef:storageFixture(),
    sessionStorageRef:storageFixture(),
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(fetches.length, 0);

  const automationWindow = new FakeTarget();
  automationWindow.matchMedia = () => ({ matches:true });
  const automation = createPwaTelemetryController({
    windowRef:automationWindow,
    navigatorRef:{ webdriver:true },
    endpoint:'https://project.supabase.co',
    publishableKey:'public',
    fetchRef,
  });
  assert.equal(automation, null);

  const brokenStorage = {
    getItem() { throw new Error('blocked'); },
    setItem() { throw new Error('blocked'); },
  };
  const blocked = createPwaTelemetryController({
    windowRef:automationWindow,
    navigatorRef:{ webdriver:false },
    endpoint:'https://project.supabase.co',
    publishableKey:'public',
    fetchRef,
    cryptoRef:{ randomUUID:() => '12345678-1234-4234-8234-123456789abc' },
    localStorageRef:brokenStorage,
    sessionStorageRef:brokenStorage,
  });
  assert.equal(blocked, null);
  assert.equal(fetches.length, 0);
});
