import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { readFile, readdir } from 'node:fs/promises';
import test from 'node:test';

import {
  createDemoCards,
  createGoogleCalendarUrl,
  createIcs,
  resolvePwaLabRuntimeConfig,
} from '../src/lib/pwaCapabilitiesLab.js';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');
const previewId = 'preview-pwa-capabilities-deadbeef';
const previewBase = `/${previewId}`;
const labPath = `${previewBase}/lab/pwa-capabilities/`;
const imagePaths = [
  `${previewBase}/assets/pwa/announcements-192.png`,
  `${previewBase}/assets/pwa/announcements-brand-192.png`,
  `${previewBase}/assets/pwa/focus-group-icon.png`,
];
const unprefixedRuntimePath = /["'`]\/(?:lab\/pwa-capabilities(?:\/|["'`])|assets\/)/u;

test('PWA capabilities lab keeps its calendar/offline contract', () => {
  const event = {
    title: 'Тест, календарь; lab',
    description: 'Строка 1\nСтрока 2',
    location: 'Калининград',
    url: `https://kenigevents.ru${labPath}`,
    timezone: 'Europe/Kaliningrad',
    start: new Date('2026-08-03T10:00:00Z'),
    end: new Date('2026-08-03T11:30:00Z'),
  };
  const ics = createIcs(event, 'stable@example.test', new Date('2026-08-02T10:00:00Z'));
  assert.match(ics, /\r\n/u);
  assert.match(ics, /UID:stable@example\.test/u);
  assert.match(ics, /SUMMARY:Тест\\, календарь\\; lab/u);
  assert.match(ics, /DESCRIPTION:Строка 1\\nСтрока 2/u);

  const cards = createDemoCards(new Date('2026-08-02T10:00:00Z'), imagePaths);
  assert.equal(cards.length, 30);
  assert.deepEqual(new Set(cards.map((card) => card.imageUrl)), new Set(imagePaths));

  const calendar = new URL(createGoogleCalendarUrl(event));
  assert.equal(calendar.hostname, 'calendar.google.com');
  assert.equal(calendar.searchParams.get('dates'), '20260803T100000Z/20260803T113000Z');
  assert.equal(calendar.searchParams.get('ctz'), 'Europe/Kaliningrad');
});

test('non-empty preview base resolves the lab worker, scope and assets inside that preview', () => {
  const runtime = resolvePwaLabRuntimeConfig({
    labUrl: labPath,
    imageUrls: imagePaths,
  }, 'https://kenigevents.ru/preview-pwa-capabilities-deadbeef/anything/');

  assert.equal(runtime.labUrl.href, `https://kenigevents.ru${labPath}`);
  assert.equal(runtime.workerUrl.href, `https://kenigevents.ru${labPath}sw.js`);
  assert.equal(runtime.scopeUrl.href, `https://kenigevents.ru${labPath}`);
  assert.deepEqual(runtime.imageUrls, imagePaths.map((path) => `https://kenigevents.ru${path}`));
  assert.match(runtime.cacheName, /preview-pwa-capabilities-deadbeef/u);
});

test('Astro sources keep install/runtime paths base-aware and deploy exact lab MIME metadata', async () => {
  const [page, worker, manifest, browserRuntime, installAdapter, installController, deploy] = await Promise.all([
    read('src/pages/lab/pwa-capabilities/index.astro'),
    read('src/pages/lab/pwa-capabilities/sw.js.ts'),
    read('src/pages/lab/pwa-capabilities/manifest.webmanifest.ts'),
    read('src/lib/pwaCapabilitiesLab.js'),
    read('src/lib/pwa-lab-install-controller.ts'),
    read('src/lib/pwa-install-controller.js'),
    read('scripts/deploy-preview-yc.mjs'),
  ]);

  assert.match(page, /noindex,nofollow,noarchive/u);
  assert.match(page, /withBase\('\/lab\/pwa-capabilities\/manifest\.webmanifest'\)/u);
  assert.match(page, /<link rel="manifest" href=\{manifestUrl\}/u);
  assert.match(page, /addEventListener\('beforeinstallprompt'/u);
  assert.match(page, /event\.preventDefault\(\)/u);
  assert.match(page, /__kenigEventsPwaInstallPrompt = event/u);
  assert.match(page, /new URL\('\.\/sw\.js', window\.location\.href\)/u);
  assert.match(page, /data-pwa-install-id=\{labInstallId\}/u);
  assert.match(page, /data-pwa-open-href=\{labOpenHref\}/u);
  assert.match(page, />Установить PWA Lab</u);
  assert.match(installAdapter, /createPwaInstallController\(/u);
  assert.match(installAdapter, /PWA Lab запущена с главного экрана/u);
  assert.match(installAdapter, /display-mode: standalone/u);
  assert.match(installAdapter, /Закройте вкладку и откройте PWA Lab через новую иконку/u);
  assert.match(installController, /texts = \{\}/u);
  assert.doesNotMatch(browserRuntime, unprefixedRuntimePath);
  assert.match(browserRuntime, /new URL\('\.\/sw\.js', labUrl\)/u);
  assert.match(browserRuntime, /serviceWorker\.register\(workerUrl\.href\)/u);
  assert.doesNotMatch(browserRuntime, /scope:\s*['"`]?\//u);

  assert.match(worker, /withBase\('\/lab\/pwa-capabilities\/'\)/u);
  assert.match(worker, /withBase\('\/assets\/pwa\/announcements-192\.png'\)/u);
  assert.doesNotMatch(worker, /Service-Worker-Allowed/u);
  assert.match(manifest, /id:\s*labUrl/u);
  assert.match(manifest, /start_url:\s*labUrl/u);
  assert.match(manifest, /scope:\s*labUrl/u);

  assert.match(deploy, /application\/manifest\+json; charset=utf-8/u);
  assert.match(deploy, /application\/javascript; charset=utf-8/u);
  assert.match(deploy, /no-cache, no-store, must-revalidate/u);
});

test('built preview fixture contains no unprefixed lab runtime paths', async (context) => {
  const buildId = process.env.PREVIEW_BUILD_ID;
  if (!buildId) {
    context.skip('Set PREVIEW_BUILD_ID after npm run build:preview to inspect emitted runtime files.');
    return;
  }
  assert.match(buildId, /^preview-/u);
  const buildDir = new URL(`../dist/${buildId}/`, import.meta.url);
  assert.equal(existsSync(buildDir), true, `missing built preview fixture dist/${buildId}`);

  const page = await readFile(new URL('lab/pwa-capabilities/index.html', buildDir), 'utf8');
  const worker = await readFile(new URL('lab/pwa-capabilities/sw.js', buildDir), 'utf8');
  const manifestText = await readFile(new URL('lab/pwa-capabilities/manifest.webmanifest', buildDir), 'utf8');
  const manifest = JSON.parse(manifestText);
  const expectedLabPath = `/${buildId}/lab/pwa-capabilities/`;

  assert.match(page, /<meta name="robots" content="noindex,nofollow,noarchive"/u);
  assert.match(page, new RegExp(`href="/${buildId}/lab/pwa-capabilities/manifest\\.webmanifest"`, 'u'));
  assert.match(page, new RegExp(`data-pwa-install-id="/${buildId}/lab/pwa-capabilities/"`, 'u'));
  assert.match(page, new RegExp(`data-pwa-open-href="/${buildId}/lab/pwa-capabilities/"`, 'u'));
  assert.match(page, /new URL\('\.\/sw\.js', window\.location\.href\)/u);
  assert.equal(manifest.id, expectedLabPath);
  assert.equal(manifest.start_url, expectedLabPath);
  assert.equal(manifest.scope, expectedLabPath);
  assert.match(worker, new RegExp(`const LAB_PATH = ['"]/${buildId}/lab/pwa-capabilities/['"]`, 'u'));
  assert.doesNotMatch(worker, /Service-Worker-Allowed/u);

  const scriptPaths = [...page.matchAll(/<script[^>]+src="([^"]+\.js)"/gu)].map((match) => match[1]);
  assert.ok(scriptPaths.length > 0, 'lab page must reference a bundled browser runtime');
  const runtimeFiles = await Promise.all(scriptPaths.map((path) => {
    const relative = path.replace(`/${buildId}/`, '');
    return readFile(new URL(relative, buildDir), 'utf8');
  }));
  for (const runtime of [page, worker, manifestText, ...runtimeFiles]) {
    assert.doesNotMatch(runtime, unprefixedRuntimePath);
  }

  const emittedAssets = await readdir(new URL('assets/pwa/', buildDir));
  for (const name of ['announcements-192.png', 'announcements-brand-192.png', 'focus-group-icon.png']) {
    assert.ok(emittedAssets.includes(name), `missing base-aware lab asset ${name}`);
  }
});
