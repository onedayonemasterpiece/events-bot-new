import { test, expect, type Browser, type Page } from '@playwright/test';
import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const distRoot = path.join(repoRoot, 'site/dist-lab');
const requestedBuild = process.env.PREVIEW_BUILD_ID;
const manifests = fs.existsSync(distRoot)
  ? fs.readdirSync(distRoot).map((name) => path.join(distRoot, name, 'lab-manifest.json')).filter(fs.existsSync)
  : [];
if (!manifests.length) throw new Error('Run `npm --prefix site run build:lab` first');
const manifestPath = requestedBuild
  ? path.join(distRoot, requestedBuild, 'lab-manifest.json')
  : manifests.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs)[0];
if (!fs.existsSync(manifestPath)) throw new Error(`Missing requested lab manifest: ${manifestPath}`);
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const buildId = manifest.buildId as string;
const port = 4187;
const baseUrl = `http://127.0.0.1:${port}/${buildId}/lab/briefing/`;
const scenarioIds = [
  'today_count', 'tomorrow_count', 'weekend_count', 'greeting_day', 'local_keska', 'smart_search_education',
  'share_education', 'like_education', 'not_interested_education', 'frequently_forwarded', 'anticipated_person',
  'anticipated_person_named', 'rare_event', 'weather_water_demo', 'festival_demo', 'unusual_format_demo',
];
const viewports = [{ width: 320, height: 568 }, { width: 375, height: 667 }, { width: 390, height: 844 }, { width: 1440, height: 900 }];
let server: ChildProcess;

async function waitForServer() {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    try { if ((await fetch(baseUrl)).ok) return; } catch (_) {}
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Preview server did not start: ${baseUrl}`);
}
async function open(page: Page, variant = 'c', scenario = 'today_count', extra = '') {
  await page.goto(`${baseUrl}?variant=${variant}&scenario=${scenario}${extra}`, { waitUntil: 'domcontentloaded' });
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', scenario);
}
async function visibleFragments(page: Page) {
  return page.locator('[data-reveal-fragment].is-visible').count();
}
async function labState(page: Page) {
  return page.evaluate(() => (window as any).__briefingLab.getState());
}
async function openDock(page: Page) {
  await page.locator('[data-lab-dock] > details').evaluate((node: HTMLDetailsElement) => { node.open = true; });
}

async function freshPage(browser: Browser, viewport = { width: 390, height: 844 }, reducedMotion: 'reduce' | 'no-preference' = 'no-preference') {
  const context = await browser.newContext({ viewport, reducedMotion });
  return { context, page: await context.newPage() };
}

test.describe.configure({ mode: 'serial' });
test.beforeAll(async () => {
  server = spawn(process.execPath, ['site/scripts/preview-briefing-lab.mjs'], {
    cwd: repoRoot,
    env: { ...process.env, PREVIEW_BUILD_ID: buildId, LAB_PREVIEW_PORT: String(port) },
    stdio: 'pipe',
  });
  await waitForServer();
});
test.afterAll(() => server?.kill('SIGTERM'));

test('C has observable semantic-fragment states and B is immediately static', async ({ browser }) => {
  const { context, page } = await freshPage(browser);
  await open(page, 'c', 'today_count', '&replay=1&pace=slow');
  const total = await page.locator('[data-reveal-fragment]').count();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await openDock(page);
  await page.locator('[data-replay]').click();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'running');
  const first = await visibleFragments(page);
  expect(first).toBeGreaterThanOrEqual(1);
  expect(first).toBeLessThan(total);
  await expect(page.locator('[data-reveal-fragment].is-active')).toHaveCount(1);
  await page.waitForTimeout(360);
  const second = await visibleFragments(page);
  expect(second).toBeGreaterThan(first);
  expect(second).toBeLessThanOrEqual(total);
  await page.waitForTimeout(1050);
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await expect(page.locator('[data-reveal-fragment].is-visible')).toHaveCount(total);
  await context.close();

  const staticRun = await freshPage(browser);
  await open(staticRun.page, 'b', 'today_count');
  await expect(staticRun.page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await expect(staticRun.page.locator('[data-reveal-fragment].is-visible')).toHaveCount(total);
  await staticRun.context.close();
});

test('expanded scenario deck is discoverable; Replay, session state, Play all and Pause are explicit', async ({ browser }) => {
  const { context, page } = await freshPage(browser, { width: 1440, height: 900 });
  await open(page, 'c', 'today_count', '&replay=1&pace=fast');
  await openDock(page);
  const options = await page.locator('[data-scenario-select] option').evaluateAll((nodes) => nodes.map((node) => (node as HTMLOptionElement).value));
  expect(options.slice(0, scenarioIds.length)).toEqual(scenarioIds);
  expect(new Set(options).size).toBe(scenarioIds.length + 1); // deck + the explicit fallback
  await expect(page.locator('[data-play-all]')).toContainText(`Показать все ${scenarioIds.length}`);

  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await page.reload({ waitUntil: 'domcontentloaded' });
  await openDock(page);
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await expect(page.locator('[data-playback-status]')).toContainText('Пауза для чтения');
  await expect.poll(async () => (await labState(page)).scenario, { timeout: 5000 }).toBe('frequently_forwarded');
  await expect(page.locator('[data-chain-progress]')).toContainText('2/3');
  await page.locator('[data-pace="slow"]').click();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await page.locator('[data-replay]').click();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'running');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });

  await page.locator('[data-scenario-select]').selectOption('share_education');
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'share_education');
  await expect(page).toHaveURL(/scenario=share_education/u);
  await expect(page.locator('[data-message]')).toContainText('Поделиться');

  await page.locator('[data-play-all]').click();
  await expect.poll(async () => (await labState(page)).scenario, { timeout: 8500 }).toBe('tomorrow_count');
  await page.locator('[data-pause]').click();
  const paused = await labState(page);
  expect(paused.paused).toBeTruthy();
  const frozenScenario = paused.scenario;
  await page.waitForTimeout(3000);
  expect((await labState(page)).scenario).toBe(frozenScenario);
  await page.locator('[data-pause]').click();
  expect((await labState(page)).paused).toBeFalsy();
  await context.close();
});

test('hero, categories and contextual feed geometry hold across every scenario and required viewport', async ({ browser }) => {
  test.setTimeout(150_000);
  for (const viewport of viewports) {
    const { context, page } = await freshPage(browser, viewport);
    for (const scenario of [...scenarioIds, 'neutral_fallback']) {
      for (const variant of ['b', 'c']) {
        await open(page, variant, scenario, variant === 'c' ? '&replay=1&pace=fast' : '');
        if (variant === 'c') await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
        const geometry = await page.evaluate(() => {
          const rect = (selector: string) => document.querySelector(selector)?.getBoundingClientRect().toJSON();
          const stage = document.querySelector('[data-briefing-slot]') as HTMLElement;
          const message = document.querySelector('[data-message]') as HTMLElement;
          const categories = [...document.querySelectorAll('[data-category-route]')].map((node) => ({ text: node.textContent?.trim(), rect: node.getBoundingClientRect().toJSON() }));
          const fragmentLineYs = [...document.querySelectorAll('[data-reveal-fragment]')]
            .flatMap((node) => [...node.getClientRects()].map((rect) => Math.round(rect.y)));
          return {
            stage: rect('[data-briefing-slot]'), categoriesBox: rect('[data-briefing-categories]'), feedHeading: rect('.briefing-context-feed__heading'),
            stageFits: stage.scrollHeight <= stage.clientHeight && (stage.dataset.mediaMode === 'wide' || stage.scrollWidth <= stage.clientWidth),
            messageFits: message.scrollHeight <= message.clientHeight && message.scrollWidth <= message.clientWidth,
            categories, bodyWidth: document.body.scrollWidth, innerWidth,
            lineCount: new Set(fragmentLineYs).size,
            text: message.textContent?.replace(/\s+/gu, ' ').trim(),
          };
        });
        expect(geometry.stage!.height).toBeLessThanOrEqual(viewport.height * .5);
        expect(geometry.stageFits, `${scenario} ${variant} stage ${viewport.width}`).toBeTruthy();
        expect(geometry.messageFits, `${scenario} ${variant} message ${viewport.width}`).toBeTruthy();
        expect(geometry.lineCount, `${scenario} ${variant} lines ${viewport.width}`).toBeLessThanOrEqual(3);
        expect(geometry.categories).toHaveLength(5);
        expect(geometry.categories.every((item) => item.rect.width > 0 && item.rect.height >= 44)).toBeTruthy();
        expect(geometry.categoriesBox!.bottom).toBeLessThanOrEqual(viewport.height);
        expect(geometry.feedHeading!.top).toBeLessThan(viewport.height);
        expect(geometry.bodyWidth).toBe(geometry.innerWidth);
      }
    }
    await context.close();
  }
});

test('hover/focus/pointer finish the sentence; inline link is stable and pace controls work', async ({ browser }) => {
  const { context, page } = await freshPage(browser, { width: 1440, height: 900 });
  await open(page, 'c', 'today_count', '&replay=1&pace=slow');
  await openDock(page);
  await page.waitForTimeout(100);
  await page.locator('[data-briefing]').hover();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 200 });
  expect((await labState(page)).paused).toBeTruthy();

  await page.locator('[data-replay]').click();
  await page.locator('a[data-reveal-fragment]').first().focus();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 200 });
  expect((await labState(page)).paused).toBeTruthy();

  await page.locator('[data-pace="fast"]').click();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'running');
  expect((await labState(page)).pace).toBe('fast');
  await page.dispatchEvent('[data-briefing-slot]', 'pointerdown', { button: 0 });
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 200 });
  expect((await labState(page)).paused).toBeTruthy();

  const href = await page.locator('a[data-reveal-fragment]').first().getAttribute('href');
  expect(href).toBe('/segodnya/');
  await context.close();
});

test('local queue memory, verified actions, pace preference, exact O and non-lingering cursor are deterministic', async ({ browser }) => {
  const { context, page } = await freshPage(browser, { width: 390, height: 844 });
  await page.goto(`${baseUrl}?variant=c&replay=1`, { waitUntil: 'domcontentloaded' });
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'today_count');
  await expect(page).not.toHaveURL(/scenario=/u);
  await page.waitForTimeout(350);
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem('ke-briefing-memory-v1') || '{}').exposures?.today_count?.length)).toBe(1);

  await page.reload({ waitUntil: 'domcontentloaded' });
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'tomorrow_count');
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-ready', 'true');
  await expect(page).not.toHaveURL(/scenario=/u);

  await openDock(page);
  await page.locator('[data-pace="slow"]').click();
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem('ke-briefing-lab-prefs-v1') || '{}').pace)).toBe('slow');
  await page.goto(`${baseUrl}?variant=c`, { waitUntil: 'domcontentloaded' });
  expect((await labState(page)).pace).toBe('slow');
  await expect(page.locator('[data-pace="slow"]')).toHaveAttribute('aria-pressed', 'true');
  await page.goto(`${baseUrl}?variant=c&scenario=today_count&pace=fast&replay=1`, { waitUntil: 'domcontentloaded' });
  expect((await labState(page)).pace).toBe('fast');
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem('ke-briefing-lab-prefs-v1') || '{}').pace)).toBe('slow');

  expect(await page.evaluate(() => (window as any).__briefingLab.getState().memory.actionSuccess.share_event)).toBeUndefined();
  await page.evaluate(() => window.dispatchEvent(new CustomEvent('ke:event-action-success', { detail: { action: 'share' } })));
  expect(await page.evaluate(() => Boolean((window as any).__briefingLab.getState().memory.actionSuccess.share_event))).toBeTruthy();
  expect(await page.evaluate(() => (window as any).__briefingLab.getState().memory.actionSuccess.like_event)).toBeUndefined();

  const wideOSrc = await page.locator('img.briefing-stage__brand-o').getAttribute('src');
  expect(wideOSrc).toContain('/brand/announcements-wide-o-ui.svg');
  expect(await page.locator('.briefing-stage__brand-o use').count()).toBe(0);
  await openDock(page);
  await page.locator('[data-scenario-select]').selectOption('share_education');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-cursor', 'underscore');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await expect(page.locator('[data-briefing][data-motion="complete"] .briefing-fragment.is-active')).toHaveCount(0);
  expect(await page.evaluate(() => (window as any).__briefingLab.getState().memory.exposures.share_education)).toBeUndefined();
  await page.locator('[data-scenario-select]').selectOption('anticipated_person');
  await expect(page.locator('[data-demo-label]')).toContainText('DEMO-СИГНАЛ');

  await page.locator('[data-review-guide] summary').click();
  await page.locator('[data-reset-briefing-memory]').click();
  expect(await page.evaluate(() => localStorage.getItem('ke-briefing-memory-v1'))).toBeNull();
  expect(await page.evaluate(() => localStorage.getItem('ke-briefing-lab-prefs-v1'))).toBeNull();
  await context.close();
});

test('education is satisfied only by a verified action after that narrative was shown', async ({ browser }) => {
  const seed = async (page: Page, likeExposure: number, likeSuccess: number) => {
    const now = Date.now();
    await page.goto(`${baseUrl}?variant=a`, { waitUntil: 'domcontentloaded' });
    await page.evaluate(({ now, likeExposure, likeSuccess, ids }) => {
      const exposures = Object.fromEntries(ids.map((id) => [id, [now]]));
      exposures.like_education = [likeExposure];
      localStorage.setItem('ke-briefing-memory-v1', JSON.stringify({
        version: 1,
        exposures,
        actionSuccess: { like_event: likeSuccess },
      }));
    }, { now, likeExposure, likeSuccess, ids: scenarioIds.filter((id) => id !== 'like_education') });
  };

  const before = await freshPage(browser, { width: 390, height: 844 });
  const beforeNow = Date.now();
  await seed(before.page, beforeNow - 40 * 86400000, beforeNow - 60 * 86400000);
  await before.page.goto(`${baseUrl}?variant=c&replay=1`, { waitUntil: 'domcontentloaded' });
  await expect(before.page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'like_education');
  await before.context.close();

  const after = await freshPage(browser, { width: 390, height: 844 });
  const afterNow = Date.now();
  await seed(after.page, afterNow - 40 * 86400000, afterNow - 10 * 86400000);
  await after.page.goto(`${baseUrl}?variant=c&replay=1`, { waitUntil: 'domcontentloaded' });
  await expect(after.page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'neutral_fallback');
  await after.context.close();
});

test('no-JS and reduced-motion stay useful and manual', async ({ browser }) => {
  const noJs = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 320, height: 568 } });
  const noJsPage = await noJs.newPage();
  await noJsPage.goto(`${baseUrl}?variant=c&scenario=weekend_count`);
  await expect(noJsPage.locator('[data-message]')).toContainText('18 идей');
  await expect(noJsPage.locator('[data-briefing-categories] a')).toHaveCount(5);
  await expect(noJsPage.locator('[data-message]')).toBeVisible();
  await noJs.close();

  const reduced = await freshPage(browser, { width: 390, height: 844 }, 'reduce');
  await open(reduced.page, 'c', 'weekend_count', '&replay=1&autoplay=1');
  await expect(reduced.page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await expect(reduced.page.locator('[data-reveal-fragment].is-visible')).toHaveCount(4);
  const before = (await labState(reduced.page)).scenario;
  await reduced.page.waitForTimeout(3000);
  expect((await labState(reduced.page)).scenario).toBe(before);
  await reduced.context.close();
});

test('lab remains noindex and performs no remote telemetry', async ({ page }) => {
  const requests: Array<{ method: string; type: string; url: string }> = [];
  const beacons: string[] = [];
  await page.addInitScript(() => {
    (window as any).__labBeacons = [];
    navigator.sendBeacon = (url) => { (window as any).__labBeacons.push(String(url)); return false; };
  });
  page.on('request', (request) => requests.push({ method: request.method(), type: request.resourceType(), url: request.url() }));
  await open(page, 'c', 'today_count', '&replay=1');
  await openDock(page);
  await expect(page.locator('meta[name="robots"]')).toHaveAttribute('content', 'noindex,nofollow,noarchive');
  await page.locator('[data-replay]').click();
  await page.locator('[data-review-guide] summary').click();
  const download = page.waitForEvent('download');
  await page.locator('[data-export-telemetry]').click();
  expect((await download).suggestedFilename()).toMatch(/^briefing-lab-reveal-today_count\.json$/u);
  beacons.push(...await page.evaluate(() => (window as any).__labBeacons));
  const forbidden = requests.filter((item) => item.method !== 'GET' || ['xhr', 'fetch'].includes(item.type) || /supabase|analytics|telemetry/iu.test(item.url));
  expect(forbidden).toEqual([]);
  expect(beacons).toEqual([]);
  expect(await page.evaluate(() => (window as any).__briefingTelemetry.length)).toBeLessThanOrEqual(24);
});

test('page is clean, finite narrative chain advances automatically, and public Next appears only after stop', async ({ browser }) => {
  const { context, page } = await freshPage(browser, { width: 1440, height: 900 });
  await open(page, 'c', 'greeting_day', '&replay=1&pace=fast');
  await expect(page.locator('[data-lab-review-bar]')).toHaveCount(0);
  await expect(page.locator('[data-lab-dock] > details')).not.toHaveAttribute('open', '');
  await expect(page.locator('[data-briefing-slot] [data-progress], [data-briefing-slot] [data-demo-label], [data-briefing-slot] [data-pause], [data-briefing-slot] [data-pace], [data-briefing-slot] [data-pace-select]')).toHaveCount(0);
  const publicNext = page.locator('[data-public-next]');
  await expect(publicNext).toHaveText('Показать следующее');
  await expect(publicNext).toBeHidden();
  await expect.poll(async () => (await labState(page)).scenario, { timeout: 5000 }).toBe('today_count');
  await expect(publicNext).toBeHidden();
  await expect.poll(async () => (await labState(page)).scenario, { timeout: 5000 }).toBe('frequently_forwarded');
  await expect(publicNext).toBeVisible({ timeout: 2500 });
  expect((await publicNext.boundingBox())!.height).toBeGreaterThanOrEqual(44);
  await publicNext.click();
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'anticipated_person');
  await expect(publicNext).toBeHidden();

  await open(page, 'b', 'anticipated_person');
  await expect(publicNext).toBeVisible();
  await publicNext.click();
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'anticipated_person_named');
  await expect(page.locator('[data-message]')).toContainText('Татьяна Куртукова');
  const namedLink = page.locator('a[data-reveal-fragment]', { hasText: 'Татьяна Куртукова' });
  await expect(namedLink).toHaveAttribute('href', /sobytiya\/kontsert-tatyany-kurtukovoy-matushka-zemlya-svetlogorsk-6020\//u);
  await expect(page.locator('[data-progress]')).toContainText(`из ${scenarioIds.length}`);
  await context.close();
});

test('selected media is desktop-only, wide media belongs to viewport, exits, and reduced motion stays static', async ({ browser }) => {
  const imageBody = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="500"><rect width="400" height="500" fill="#b56b45"/></svg>';
  const desktop = await freshPage(browser, { width: 1440, height: 900 });
  await desktop.page.route(/https:\/\/(storage\.yandexcloud\.net|sun9-[^.]+\.userapi\.com)\//u, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(desktop.page, 'c', 'anticipated_person_named', '&replay=1&pace=fast');
  const desktopImage = desktop.page.locator('[data-media-image]');
  await expect(desktopImage).toHaveAttribute('data-src', /https:\/\//u);
  await expect(desktopImage).toHaveAttribute('src', /https:\/\//u);
  await expect(desktop.page.locator('[data-narrative-media]')).toHaveAttribute('data-media-mode', 'small');
  await expect(desktop.page.locator('[data-narrative-media]')).toHaveClass(/is-present/u);
  await desktop.context.close();

  const mobile = await freshPage(browser, { width: 390, height: 844 });
  await open(mobile.page, 'b', 'anticipated_person_named');
  await expect(mobile.page.locator('.site-nav')).toBeHidden();
  await expect(mobile.page.locator('[data-media-image]')).toHaveAttribute('data-src', /https:\/\//u);
  await expect(mobile.page.locator('[data-media-image]')).not.toHaveAttribute('src', /.+/u);
  await mobile.context.close();

  const wide = await freshPage(browser, { width: 1440, height: 900 });
  await wide.page.route(/https:\/\/(storage\.yandexcloud\.net|sun9-[^.]+\.userapi\.com)\//u, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(wide.page, 'b', 'rare_event');
  await expect(wide.page.locator('[data-narrative-media]')).toHaveClass(/is-present/u);
  const wideGeometry = await wide.page.evaluate(() => {
    const box = (selector: string) => document.querySelector(selector)!.getBoundingClientRect();
    const stage = box('[data-briefing-slot]');
    const media = box('[data-narrative-media]');
    const message = box('[data-briefing]');
    const style = getComputedStyle(document.querySelector('[data-narrative-media]')!);
    return { stage: stage.toJSON(), media: media.toJSON(), message: message.toJSON(), borderRadius: style.borderRadius, boxShadow: style.boxShadow, innerWidth, innerHeight };
  });
  expect(wideGeometry.stage.x).toBeLessThanOrEqual(1);
  expect(Math.abs(wideGeometry.stage.right - wideGeometry.innerWidth)).toBeLessThanOrEqual(1);
  expect(wideGeometry.stage.height).toBeLessThanOrEqual(wideGeometry.innerHeight * .5);
  expect(wideGeometry.media.width).toBeGreaterThanOrEqual(wideGeometry.innerWidth * .6);
  expect(Math.abs(wideGeometry.media.right - wideGeometry.innerWidth)).toBeLessThanOrEqual(1);
  expect(wideGeometry.message.left).toBeLessThanOrEqual(32);
  expect(wideGeometry.borderRadius).toBe('0px');
  expect(wideGeometry.boxShadow).toBe('none');
  await open(wide.page, 'c', 'rare_event', '&replay=1&pace=fast');
  await expect(wide.page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await wide.page.evaluate(() => (window as any).__briefingLab.pause());
  const categoriesTopWithMedia = (await wide.page.locator('[data-briefing-categories]').boundingBox())!.y;
  await expect(wide.page.locator('[data-narrative-media]')).toHaveClass(/is-exiting/u, { timeout: 5000 });
  const categoriesTopAfterExit = (await wide.page.locator('[data-briefing-categories]').boundingBox())!.y;
  expect(Math.abs(categoriesTopAfterExit - categoriesTopWithMedia)).toBeLessThanOrEqual(1);
  await wide.context.close();

  const staticWide = await freshPage(browser, { width: 1440, height: 900 });
  await staticWide.page.route(/https:\/\/(storage\.yandexcloud\.net|sun9-[^.]+\.userapi\.com)\//u, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(staticWide.page, 'b', 'rare_event');
  await expect(staticWide.page.locator('[data-narrative-media]')).toHaveClass(/is-present/u);
  await staticWide.page.waitForTimeout(4300);
  await expect(staticWide.page.locator('[data-narrative-media]')).not.toHaveClass(/is-exiting/u);
  await staticWide.context.close();

  const reduced = await freshPage(browser, { width: 1440, height: 900 }, 'reduce');
  await reduced.page.route(/https:\/\/(storage\.yandexcloud\.net|sun9-[^.]+\.userapi\.com)\//u, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(reduced.page, 'c', 'rare_event', '&replay=1');
  await expect(reduced.page.locator('[data-narrative-media]')).toHaveClass(/is-present/u);
  await reduced.page.waitForTimeout(4300);
  await expect(reduced.page.locator('[data-narrative-media]')).not.toHaveClass(/is-exiting/u);
  await reduced.context.close();
});
