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
    'anticipated_person_named', 'live_meeting_mosaic', 'rare_event', 'weather_water_demo', 'storm_weekend_demo', 'storm_lecture_science_demo',
  'festival_demo', 'unusual_format_demo',
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
  const rendered = await page.locator('[data-briefing-lab]').getAttribute('data-briefing-scenario-id');
  // A deliberately short fast chain can advance while a loaded CI worker is
  // still returning from goto(). Reset through the public lab selector rather
  // than turning the production-candidate automatic chain off in tests.
  if (rendered !== scenario) {
    await page.locator('[data-scenario-select]').evaluate((node: HTMLSelectElement, value) => {
      node.value = String(value);
      node.dispatchEvent(new Event('change', { bubbles: true }));
    }, scenario);
  }
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
  await page.evaluate(() => (window as any).__briefingLab.pause());
  await expect.poll(async () => (await labState(page)).paused).toBeTruthy();
  await expect(page.locator('[data-pause]')).toHaveAttribute('aria-pressed', 'true');
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
          const fragmentRects = [...document.querySelectorAll('[data-reveal-fragment]')]
            .flatMap((node) => [...node.getClientRects()]);
          const fragmentLineYs = fragmentRects.map((rect) => Math.round(rect.y));
          const originalScrollX = window.scrollX;
          window.scrollTo(999, window.scrollY);
          const horizontalScrollX = window.scrollX;
          window.scrollTo(originalScrollX, window.scrollY);
          return {
            stage: rect('[data-briefing-slot]'), categoriesBox: rect('[data-briefing-categories]'), feedHeading: rect('.briefing-context-feed__heading'),
            // The text-only wash intentionally extends to 100vw outside the
            // shared content shell; bodyWidth below remains the real overflow gate.
            stageFits: stage.scrollHeight <= stage.clientHeight,
            // The pending underscore is intentionally positioned just after
            // the final glyph and may add a few decorative pixels to
            // scrollWidth. Validate the text boxes themselves plus the page's
            // real horizontal-overflow gate below.
            messageFits: message.scrollHeight <= message.clientHeight
              && fragmentRects.every((rect) => rect.left >= -1 && rect.right <= innerWidth + 1),
            categories, bodyWidth: document.body.scrollWidth, innerWidth, horizontalScrollX,
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
        expect(geometry.bodyWidth).toBeLessThanOrEqual(geometry.innerWidth + 16);
        expect(geometry.horizontalScrollX).toBe(0);
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
  // Replay is intentionally deferred until the dock scroll settles; wait for
  // that callback before focus-interrupting it instead of racing its 120 ms timer.
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'running', { timeout: 500 });
  await page.locator('a[data-reveal-fragment]').first().focus();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 500 });
  expect((await labState(page)).paused).toBeTruthy();

  await page.locator('[data-pace="fast"]').click();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'running');
  expect((await labState(page)).pace).toBe('fast');
  // Bring the stage back from the open lab dock before dispatching the blank
  // pointer gesture; otherwise the out-of-view observer can win the race.
  await page.locator('[data-briefing-slot]').evaluate((node) => node.scrollIntoView({ block: 'center' }));
  await page.dispatchEvent('[data-briefing-slot]', 'pointerdown', { button: 0, pointerType: 'touch' });
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 500 });
  expect((await labState(page)).paused).toBeTruthy();

  const href = await page.locator('a[data-reveal-fragment]').first().getAttribute('href');
  expect(href).toBe('/segodnya/');
  await context.close();
});

test('weather, forwarded event, festival and storm chain are concrete and directly actionable', async ({ browser }) => {
  const { context, page } = await freshPage(browser, { width: 1440, height: 900 });

  await open(page, 'b', 'weather_water_demo');
  await expect(page.locator('[data-message]')).toContainText(/Обещают\s+ясные выходные\.\s*Махнём\s+на море\?/u);
  await expect(page.locator('[data-message]')).not.toContainText(/Допустим|на воду/u);
  await expect(page.locator('a[data-reveal-fragment]', { hasText: 'на море?' })).toHaveAttribute('href', '/poisk/');

  await open(page, 'b', 'frequently_forwarded');
  const forwarded = page.locator('a[data-reveal-fragment]', { hasText: 'Часто пересылают' });
  await expect(forwarded).toHaveAttribute('href', /sobytiya\/[^/]+-6466\//u);
  await expect(page.locator('[data-scenario-cta]')).toHaveAttribute('href', await forwarded.getAttribute('href') as string);

  await open(page, 'b', 'festival_demo');
  const festival = page.locator('a[data-reveal-fragment]', { hasText: 'Максим Милославский' });
  await expect(page.locator('[data-message]')).toContainText('Pianissimo');
  await expect(festival).toHaveAttribute('href', /sobytiya\/kontsert-festival-pianissimo-maksim-miloslavskiy-kaliningrad-5294\//u);
  await expect(page.locator('[data-scenario-cta]')).toHaveAttribute('href', await festival.getAttribute('href') as string);

  await open(page, 'c', 'storm_weekend_demo', '&replay=1&pace=fast');
  await expect(page.locator('[data-message]')).toContainText('Если прогнозируют шторм');
  await expect(page.locator('[data-scenario-cta]')).toHaveAttribute('href', /-5803\//u);
  await expect.poll(async () => (await labState(page)).scenario, { timeout: 5000 }).toBe('storm_lecture_science_demo');
  const lecture = page.locator('a[data-reveal-fragment]', { hasText: 'Суперспособности' });
  await expect(lecture).toHaveAttribute('href', /sobytiya\/supersposobnosti-vydumka-i-realnost-kaliningrad-5803\//u);
  await expect(page.locator('[data-scenario-cta]')).toHaveAttribute('href', await lecture.getAttribute('href') as string);
  await expect(page.locator('[data-public-next]')).toBeVisible({ timeout: 5000 });
  await context.close();
});

test('local queue memory, verified actions, pace preference, exact O and transition-aware cursor are deterministic', async ({ browser }) => {
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
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-cursor-linger', 'true');
  await expect(page.locator('.briefing-fragment.is-awaiting-next')).toHaveCount(1);
  expect(await page.evaluate(() => (window as any).__briefingLab.getState().memory.exposures.share_education)).toBeUndefined();
  await page.locator('[data-scenario-select]').selectOption('anticipated_person');
  await expect(page.locator('[data-demo-label]')).toContainText('DEMO-СИГНАЛ');

  await page.locator('[data-review-guide] summary').click();
  await page.locator('[data-reset-briefing-memory]').click();
  expect(await page.evaluate(() => localStorage.getItem('ke-briefing-memory-v1'))).toBeNull();
  expect(await page.evaluate(() => localStorage.getItem('ke-briefing-lab-prefs-v1'))).toBeNull();
  await context.close();
});

test('horizontal cursor signals a timed continuation and retires after terminal blinks', async ({ browser }) => {
  const { context, page } = await freshPage(browser, { width: 1440, height: 900 });
  await open(page, 'c', 'storm_weekend_demo', '&replay=1&pace=slow');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-cursor-linger', 'true');
  const pending = page.locator('.briefing-fragment.is-awaiting-next');
  await expect(pending).toHaveCount(1);
  const cursor = await pending.evaluate((node) => {
    const style = getComputedStyle(node, '::after');
    return { display: style.display, position: style.position, width: Number.parseFloat(style.width), height: Number.parseFloat(style.height), animation: style.animationName };
  });
  // Absolutely positioned generated boxes are blockified by computed style.
  expect(cursor.display).toBe('block');
  expect(cursor.position).toBe('absolute');
  expect(cursor.width).toBeGreaterThan(20);
  expect(cursor.height).toBeLessThan(10);
  expect(cursor.animation).toContain('briefing-cursor');
  await page.dispatchEvent('[data-briefing]', 'mouseenter');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-cursor-linger', 'false');
  expect((await labState(page)).paused).toBeTruthy();
  await page.waitForTimeout(900);
  expect((await labState(page)).scenario).toBe('storm_weekend_demo');
  await page.dispatchEvent('[data-briefing]', 'mouseleave');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-cursor-linger', 'true', { timeout: 1500 });
  await expect.poll(async () => (await labState(page)).scenario, { timeout: 7000 }).toBe('storm_lecture_science_demo');
  const nextState = await page.locator('[data-briefing]').evaluate((node: HTMLElement) => `${node.dataset.motion}:${node.dataset.cursorLinger}`);
  expect(['running:false', 'complete:true']).toContain(nextState);

  await open(page, 'c', 'neutral_fallback', '&replay=1&pace=fast');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await expect(page.locator('[data-public-next]')).toBeVisible();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-cursor-linger', 'true');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-cursor-linger', 'false', { timeout: 3000 });
  await expect(page.locator('.briefing-fragment.is-awaiting-next')).toHaveCount(0);
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

test('eligible scenarios use desktop mosaic media without moving the established text anchor', async ({ browser }) => {
  const imageBody = '<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="450"><rect width="1200" height="450" fill="#705045"/><circle cx="830" cy="170" r="150" fill="#c58f82"/></svg>';
  const desktop = await freshPage(browser, { width: 1440, height: 900 });
  await desktop.page.route(/https:\/\/(storage\.yandexcloud\.net|sun9-[^.]+\.userapi\.com|kaliningrad\.tretyakovgallery\.ru)\//u, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(desktop.page, 'b', 'weather_water_demo');

  const mediaCoverage = await desktop.page.locator('#briefing-lab-scenarios').evaluate((node) => {
    const deck = JSON.parse(node.textContent || '[]');
    return { total: deck.filter((item: any) => item.id !== 'neutral_fallback').length, mosaic: deck.filter((item: any) => item.id !== 'neutral_fallback' && item.media?.mode === 'mosaic').length };
  });
  expect(mediaCoverage).toEqual({ total: 19, mosaic: 7 });
  await expect(desktop.page.locator('[data-narrative-media]')).toHaveAttribute('data-media-mode', 'mosaic');
  await expect(desktop.page.locator('[data-narrative-media]')).toHaveClass(/is-present/u);

  const mediaGeometry = await desktop.page.evaluate(() => {
    const stage = document.querySelector('[data-briefing-slot]') as HTMLElement;
    const message = document.querySelector('[data-briefing]') as HTMLElement;
    const fragment = document.querySelector('[data-reveal-fragment]') as HTMLElement;
    const initial = message.getBoundingClientRect().toJSON();
    const fragmentStyle = getComputedStyle(fragment);
    const stripe = fragmentStyle.backgroundImage;
    const stripeColor = fragmentStyle.backgroundColor;
    const stripeShadow = fragmentStyle.boxShadow;
    stage.dataset.mediaMode = 'none';
    const withoutMedia = message.getBoundingClientRect().toJSON();
    const withoutStripe = getComputedStyle(fragment).backgroundImage;
    stage.dataset.mediaMode = 'mosaic';
    return { initial, withoutMedia, stripe, stripeColor, stripeShadow, withoutStripe };
  });
  expect(Math.abs(mediaGeometry.initial.x - mediaGeometry.withoutMedia.x)).toBeLessThanOrEqual(1);
  expect(Math.abs(mediaGeometry.initial.y - mediaGeometry.withoutMedia.y)).toBeLessThanOrEqual(1);
  expect(Math.abs(mediaGeometry.initial.width - mediaGeometry.withoutMedia.width)).toBeLessThanOrEqual(1);
  expect(mediaGeometry.stripe).toContain('linear-gradient');
  expect(mediaGeometry.stripe).toContain('0.42');
  expect(mediaGeometry.stripeColor).toBe('rgba(0, 0, 0, 0)');
  expect(mediaGeometry.stripeShadow).toBe('none');
  expect(mediaGeometry.withoutStripe).toBe('none');
  await desktop.context.close();

  const mobile = await freshPage(browser, { width: 390, height: 844 });
  await open(mobile.page, 'b', 'weather_water_demo');
  await expect(mobile.page.locator('[data-briefing-slot]')).toHaveAttribute('data-media-mode', 'none');
  await expect(mobile.page.locator('[data-media-image]')).not.toHaveAttribute('src', /.+/u);
  await expect.poll(() => mobile.page.locator('[data-mosaic-tile]').first().evaluate((node) => getComputedStyle(node).backgroundImage)).toBe('none');
  await mobile.context.close();
});

test('narrative media is explicitly OCR-safe, photo-only, high-resolution and abstains for unsafe fixtures', async ({ browser }) => {
  const run = await freshPage(browser, { width: 1440, height: 900 });
  await open(run.page, 'b', 'weather_water_demo');
  const contract = await run.page.locator('#briefing-lab-scenarios').evaluate((node) => {
    const deck = JSON.parse(node.textContent || '[]');
    const enabled = deck.filter((item: any) => item.media?.mode === 'mosaic');
    return {
      enabled: enabled.map((item: any) => ({ id: item.id, media: item.media })),
      abstained: Object.fromEntries(['today_count', 'smart_search_education', 'anticipated_person_named', 'live_meeting_mosaic', 'festival_demo', 'unusual_format_demo'].map((id) => [id, Boolean(deck.find((item: any) => item.id === id)?.media)])),
    };
  });
  expect(contract.enabled).toHaveLength(7);
  expect(contract.abstained).toEqual({ today_count: false, smart_search_education: false, anticipated_person_named: false, live_meeting_mosaic: false, festival_demo: false, unusual_format_demo: false });
  expect(contract.enabled.every(({ media }) => media.ocrSafe === true && media.imageTextMode === 'visual_only' && media.imageKind === 'photo')).toBeTruthy();
  expect(contract.enabled.every(({ media }) => media.sourceWidth >= 1000 && media.sourceWidth * media.sourceHeight >= 1_000_000)).toBeTruthy();
  expect(contract.enabled.every(({ media }) => media.cropStrategy === 'curated-focal-cover' && media.maxUpscale === 1.35)).toBeTruthy();

  await open(run.page, 'b', 'anticipated_person_named');
  await expect(run.page.locator('[data-briefing-slot]')).toHaveAttribute('data-media-mode', 'none');
  await expect(run.page.locator('[data-media-image]')).not.toHaveAttribute('src', /.+/u);
  await expect(run.page.locator('[data-message]')).toContainText('Татьяна Куртукова');
  await run.context.close();
});

test('adaptive 16–20×5 mosaic is dramatic, non-checkerboard, source-faithful, mobile-silent and fail-closed', async ({ browser }) => {
  const mosaicAsset = /401da1cf03c707138f810f094708b7939710e3707c913fa12fd029502b1c7c1e\.webp/u;
  const imageBody = '<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="450"><rect width="1200" height="450" fill="#705045"/><circle cx="830" cy="170" r="150" fill="#c58f82"/></svg>';

  const inspectMosaic = async (page: Page) => page.evaluate(() => {
    const box = (selector: string) => document.querySelector(selector)!.getBoundingClientRect();
    const root = document.querySelector('[data-media-mosaic]') as HTMLElement;
    const allTiles = [...document.querySelectorAll('[data-mosaic-tile]')] as HTMLElement[];
    const tiles = allTiles.filter((node) => !node.hidden).map((node) => ({
      rect: node.getBoundingClientRect().toJSON(),
      opacity: Number.parseFloat(getComputedStyle(node).opacity),
      entryDelay: getComputedStyle(node).getPropertyValue('--tile-in-delay').trim(),
      entryDuration: getComputedStyle(node).getPropertyValue('--tile-in-duration').trim(),
      exitDelay: getComputedStyle(node).getPropertyValue('--tile-out-delay').trim(),
      exitDuration: getComputedStyle(node).getPropertyValue('--tile-out-duration').trim(),
      entryBeat: node.dataset.entryBeat,
      exitBeat: node.dataset.exitBeat,
      contrastAccent: node.dataset.contrastAccent,
    }));
    const mediaStyle = getComputedStyle(document.querySelector('[data-narrative-media]')!);
    const pseudoStyle = getComputedStyle(tiles.length ? allTiles.find((node) => !node.hidden)! : allTiles[0], '::before');
    return {
      stage: box('[data-briefing-slot]').toJSON(),
      media: box('[data-narrative-media]').toJSON(),
      mosaic: box('[data-media-mosaic]').toJSON(),
      message: box('[data-briefing]').toJSON(),
      tiles,
      totalTiles: allTiles.length,
      columns: Number(root.dataset.columns),
      rows: Number(root.dataset.rows),
      naturalWidth: Number(root.dataset.naturalWidth),
      naturalHeight: Number(root.dataset.naturalHeight),
      coverWidth: Number(root.dataset.coverWidth),
      coverHeight: Number(root.dataset.coverHeight),
      focusX: getComputedStyle(root).getPropertyValue('--mosaic-focus-x').trim(),
      focusY: getComputedStyle(root).getPropertyValue('--mosaic-focus-y').trim(),
      upscaleRatio: Number(root.dataset.upscaleRatio),
      cropStrategy: root.dataset.cropStrategy,
      ocrSafe: root.dataset.ocrSafe,
      pseudoBackgroundSize: pseudoStyle.backgroundSize,
      pseudoBackgroundImage: pseudoStyle.backgroundImage,
      borderRadius: mediaStyle.borderRadius,
      boxShadow: mediaStyle.boxShadow,
      bodyWidth: document.body.scrollWidth,
      innerWidth,
    };
  });

  const assertTopology = (geometry: Awaited<ReturnType<typeof inspectMosaic>>) => {
    const { tiles, columns, rows } = geometry;
    expect(tiles).toHaveLength(columns * rows);
    expect(geometry.totalTiles).toBe(100);
    expect(tiles.every((tile) => Math.abs(tile.rect.width - tile.rect.height) <= 1)).toBeTruthy();
    const values = tiles.map((tile) => tile.opacity);
    const lastColumn = values.filter((_, index) => index % columns === columns - 1);
    const penultimateColumn = values.filter((_, index) => index % columns === columns - 2);
    expect(lastColumn).toEqual(Array(rows).fill(1));
    expect(penultimateColumn).toEqual(Array(rows).fill(1));
    expect(new Set(values).size).toBeGreaterThanOrEqual(6);
    expect(tiles.filter((tile) => tile.contrastAccent === 'bright').length).toBeGreaterThanOrEqual(2);
    expect(tiles.filter((tile) => tile.contrastAccent === 'washed').length).toBeGreaterThanOrEqual(2);

    const parity = [0, 1].map((target) => values.filter((_, index) => (Math.floor(index / columns) + index % columns) % 2 === target));
    const parityMean = parity.map((group) => group.reduce((sum, value) => sum + value, 0) / group.length);
    expect(Math.abs(parityMean[0] - parityMean[1])).toBeLessThanOrEqual(.08);

    const deltas: number[] = [];
    let checkerBlocks = 0;
    let blocks = 0;
    for (let row = 0; row < rows; row += 1) {
      const signs: number[] = [];
      for (let column = 0; column < columns; column += 1) {
        const index = row * columns + column;
        if (column < columns - 1) {
          const delta = values[index + 1] - values[index];
          deltas.push(Math.abs(delta));
          signs.push(Math.sign(delta));
        }
        if (row < rows - 1) deltas.push(Math.abs(values[index + columns] - values[index]));
        if (row < rows - 1 && column < columns - 1) {
          const a = values[index]; const b = values[index + 1]; const c = values[index + columns]; const d = values[index + columns + 1];
          if (Math.abs(a - d) < .08 && Math.abs(b - c) < .08 && Math.abs((a + d - b - c) / 2) > .2) checkerBlocks += 1;
          blocks += 1;
        }
      }
      let alternatingRun = 1; let longestAlternatingRun = 1;
      for (let index = 1; index < signs.length; index += 1) {
        alternatingRun = signs[index] && signs[index - 1] && signs[index] !== signs[index - 1] ? alternatingRun + 1 : 1;
        longestAlternatingRun = Math.max(longestAlternatingRun, alternatingRun);
      }
      expect(longestAlternatingRun).toBeLessThan(columns - 2);
    }
    expect(checkerBlocks / blocks).toBeLessThan(.25);
    expect(deltas.filter((delta) => delta > .25).length / deltas.length).toBeGreaterThanOrEqual(.15);
    expect(deltas.filter((delta) => delta < .08).length / deltas.length).toBeGreaterThanOrEqual(.1);

    const columnAverages = Array.from({ length: columns }, (_, column) => values.filter((_, index) => index % columns === column).reduce((sum, value) => sum + value, 0) / rows);
    const direction = columnAverages.slice(1).map((value, index) => Math.sign(value - columnAverages[index]));
    const reversals = direction.slice(1).filter((sign, index) => sign && direction[index] && sign !== direction[index]).length;
    expect(reversals).toBeGreaterThanOrEqual(3);
    const macroAverages = Array.from({ length: 4 }, (_, zone) => {
      const start = Math.round(zone * columns / 4); const end = Math.round((zone + 1) * columns / 4);
      const zoneValues = values.filter((_, index) => index % columns >= start && index % columns < end);
      return zoneValues.reduce((sum, value) => sum + value, 0) / zoneValues.length;
    });
    expect(macroAverages[1] - macroAverages[0]).toBeGreaterThan(.06);
    // The middle field may plateau or nearly reverse: requiring a large step
    // here recreates the smooth gradient the user rejected.
    expect(macroAverages[2] - macroAverages[1]).toBeGreaterThan(.02);
    expect(macroAverages[3] - macroAverages[2]).toBeGreaterThan(.06);
  };

  const desktop = await freshPage(browser, { width: 1440, height: 900 });
  let mosaicRequests = 0;
  desktop.page.on('request', (request) => { if (mosaicAsset.test(request.url())) mosaicRequests += 1; });
  await desktop.page.route(mosaicAsset, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(desktop.page, 'b', 'weather_water_demo');
  const shell = desktop.page.locator('[data-narrative-media]');
  await expect(shell).toHaveAttribute('data-media-mode', 'mosaic');
  await expect(shell).toHaveClass(/is-present/u);
  const geometry = await inspectMosaic(desktop.page);
  expect(geometry.columns).toBe(16);
  expect(geometry.rows).toBe(5);
  expect(Math.abs(geometry.media.left / geometry.innerWidth - .25)).toBeLessThanOrEqual(.015);
  expect(Math.abs(geometry.media.right - geometry.innerWidth)).toBeLessThanOrEqual(1);
  expect(Math.abs(geometry.mosaic.width / geometry.mosaic.height - geometry.columns / geometry.rows)).toBeLessThan(.02);
  expect(geometry.media.left).toBeLessThan(geometry.message.right);
  assertTopology(geometry);
  expect(Math.abs(geometry.coverWidth / geometry.naturalWidth - geometry.coverHeight / geometry.naturalHeight)).toBeLessThan(.001);
  expect(geometry.coverWidth).toBeGreaterThanOrEqual(geometry.mosaic.width - 1);
  expect(geometry.coverHeight).toBeGreaterThanOrEqual(geometry.mosaic.height - 1);
  expect(geometry.pseudoBackgroundSize).toBe('cover');
  expect(geometry.pseudoBackgroundImage).toContain('401da1cf03c707138f810f094708b7939710e3707c913fa12fd029502b1c7c1e.webp');
  expect(geometry.focusX).toBe('58%');
  expect(geometry.focusY).toBe('50%');
  expect(geometry.upscaleRatio).toBeLessThanOrEqual(1.35);
  expect(geometry.cropStrategy).toBe('curated-focal-cover');
  expect(geometry.ocrSafe).toBe('true');
  expect(geometry.borderRadius).toBe('0px');
  expect(geometry.boxShadow).toBe('none');
  expect(geometry.bodyWidth).toBe(geometry.innerWidth);
  expect(mosaicRequests).toBe(1);
  const timingBeforeReload = geometry.tiles.map((tile) => [tile.entryDelay, tile.entryDuration, tile.exitDelay, tile.exitDuration, tile.entryBeat, tile.exitBeat]);
  expect(new Set(geometry.tiles.map((tile) => `${tile.entryDelay}/${tile.entryDuration}`)).size).toBeGreaterThan(60);
  expect(new Set(geometry.tiles.map((tile) => `${tile.exitDelay}/${tile.exitDuration}`)).size).toBeGreaterThan(60);
  expect(geometry.tiles.map((tile) => tile.exitBeat)).not.toEqual(geometry.tiles.map((tile) => tile.entryBeat).reverse());
  await desktop.page.reload({ waitUntil: 'networkidle' });
  const timingAfterReload = (await inspectMosaic(desktop.page)).tiles.map((tile) => [tile.entryDelay, tile.entryDuration, tile.exitDelay, tile.exitDuration, tile.entryBeat, tile.exitBeat]);
  expect(timingAfterReload).toEqual(timingBeforeReload);
  await desktop.context.close();

  for (const viewport of [{ width: 1366, height: 768, columns: 16 }, { width: 1600, height: 900, columns: 18 }, { width: 1920, height: 900, columns: 20 }]) {
    const wide = await freshPage(browser, viewport);
    await wide.page.route(mosaicAsset, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
    await open(wide.page, 'b', 'weather_water_demo');
    const wideGeometry = await inspectMosaic(wide.page);
    expect(wideGeometry.columns).toBe(viewport.columns);
    expect(Math.abs(wideGeometry.media.left / viewport.width - .25)).toBeLessThanOrEqual(.015);
    expect(Math.abs(wideGeometry.media.right - viewport.width)).toBeLessThanOrEqual(1);
    expect(wideGeometry.bodyWidth).toBe(viewport.width);
    assertTopology(wideGeometry);
    await wide.context.close();
  }

  const animated = await freshPage(browser, { width: 1366, height: 768 });
  await animated.page.route(mosaicAsset, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(animated.page, 'c', 'weather_water_demo', '&replay=1&pace=fast');
  await animated.page.waitForTimeout(250);
  const partial = await animated.page.locator('[data-mosaic-tile]:not([hidden])').evaluateAll((nodes) => nodes.filter((node) => Number.parseFloat(getComputedStyle(node).opacity) > .01).length);
  expect(partial).toBeGreaterThan(0);
  expect(partial).toBeLessThan(80);
  await expect(animated.page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await expect(animated.page.locator('[data-narrative-media]')).toHaveClass(/is-exiting/u, { timeout: 4000 });
  await animated.page.waitForTimeout(150);
  const exiting = await animated.page.locator('[data-mosaic-tile]:not([hidden])').evaluateAll((nodes) => nodes.map((node) => Number.parseFloat(getComputedStyle(node).opacity)));
  expect(exiting.some((opacity) => opacity < .02)).toBeTruthy();
  await expect.poll(async () => (await labState(animated.page)).scenario, { timeout: 2000 }).toBe('unusual_format_demo');
  await animated.context.close();

  for (const viewport of [{ width: 320, height: 568 }, { width: 390, height: 844 }, { width: 1024, height: 600 }]) {
    const mobile = await freshPage(browser, viewport);
    let requested = 0;
    mobile.page.on('request', (request) => { if (mosaicAsset.test(request.url())) requested += 1; });
    await open(mobile.page, 'b', 'weather_water_demo');
    await mobile.page.waitForTimeout(250);
    await expect(mobile.page.locator('[data-briefing-slot]')).toHaveAttribute('data-media-mode', 'none');
    await expect(mobile.page.locator('[data-media-image]')).not.toHaveAttribute('src', /.+/u);
    await expect.poll(() => mobile.page.locator('[data-media-mosaic]').evaluate((node: HTMLElement) => getComputedStyle(node).getPropertyValue('--mosaic-image').trim())).toBe('');
    expect(requested).toBe(0);
    expect(await mobile.page.evaluate(() => document.body.scrollWidth)).toBe(viewport.width);
    await mobile.context.close();
  }

  const blocked = await freshPage(browser, { width: 1440, height: 900 });
  await blocked.page.route(mosaicAsset, (route) => route.fulfill({ status: 404, body: '' }));
  await open(blocked.page, 'b', 'weather_water_demo');
  await expect(blocked.page.locator('[data-narrative-media]')).toHaveClass(/is-error/u);
  await expect(blocked.page.locator('[data-message]')).toContainText('на море?');
  await expect(blocked.page.locator('[data-scenario-cta]')).toBeVisible();
  await blocked.context.close();

  const lowResolution = await freshPage(browser, { width: 1440, height: 900 });
  const lowResolutionBody = '<svg xmlns="http://www.w3.org/2000/svg" width="600" height="225"><rect width="600" height="225" fill="#705045"/></svg>';
  await lowResolution.page.route(mosaicAsset, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: lowResolutionBody }));
  await open(lowResolution.page, 'b', 'weather_water_demo');
  await expect(lowResolution.page.locator('[data-narrative-media]')).toHaveClass(/is-error/u);
  await expect(lowResolution.page.locator('[data-media-image]')).not.toHaveAttribute('src', /.+/u);
  await expect(lowResolution.page.locator('[data-message]')).toContainText('на море?');
  await lowResolution.context.close();

  const reduced = await freshPage(browser, { width: 1440, height: 900 }, 'reduce');
  await reduced.page.route(mosaicAsset, (route) => route.fulfill({ status: 200, contentType: 'image/svg+xml', body: imageBody }));
  await open(reduced.page, 'c', 'weather_water_demo', '&replay=1');
  await expect(reduced.page.locator('[data-narrative-media]')).toHaveClass(/is-present/u);
  await reduced.page.waitForTimeout(4300);
  await expect(reduced.page.locator('[data-narrative-media]')).not.toHaveClass(/is-exiting/u);
  await reduced.context.close();
});

test('approved header lockup, lightweight mosaic stripe and weather actions keep visual hierarchy at desktop review widths', async ({ browser }) => {
  for (const viewport of [{ width: 1366, height: 768 }, { width: 1440, height: 900 }, { width: 1920, height: 900 }]) {
    const run = await freshPage(browser, viewport);
    await open(run.page, 'b', 'weather_water_demo');
    const wordmark = run.page.locator('.site-header__desktop-lockup .announcements-lockup__wordmark');
    await expect(wordmark).toBeVisible();
    const useHref = await wordmark.locator('use').getAttribute('href');
    expect(useHref).toContain('/brand/announcements-wordmark-ui.svg#announcements-wordmark-ui');
    const wordmarkBox = await wordmark.boundingBox();
    expect(wordmarkBox!.width).toBeGreaterThanOrEqual(190);
    expect(wordmarkBox!.height).toBeGreaterThanOrEqual(30);

    const geometry = await run.page.evaluate(() => {
      const stage = document.querySelector('[data-briefing-slot]')!.getBoundingClientRect();
      const message = document.querySelector('[data-message]')!.getBoundingClientRect();
      const media = document.querySelector('[data-narrative-media]')!.getBoundingClientRect();
      const longName = document.querySelectorAll('[data-reveal-fragment]')[1] as HTMLElement;
      longName.textContent = 'Христофор Константинопольский.';
      const longMessage = document.querySelector('[data-message]')!.getBoundingClientRect();
      const style = getComputedStyle(longName);
      return { stage: stage.toJSON(), message: message.toJSON(), media: media.toJSON(), longMessage: longMessage.toJSON(), stripe: style.backgroundImage, stripeColor: style.backgroundColor, stripeShadow: style.boxShadow, paddingInline: Number.parseFloat(style.paddingLeft) + Number.parseFloat(style.paddingRight), scrollHeight: (document.querySelector('[data-briefing-slot]') as HTMLElement).scrollHeight, clientHeight: (document.querySelector('[data-briefing-slot]') as HTMLElement).clientHeight };
    });
    expect(geometry.stage.height).toBeLessThanOrEqual(viewport.height * .5);
    expect(Math.abs(geometry.message.left - geometry.stage.left)).toBeLessThanOrEqual(1);
    expect(geometry.media.left).toBeLessThan(geometry.message.right);
    expect(Math.abs(geometry.longMessage.left - geometry.message.left)).toBeLessThanOrEqual(1);
    expect(geometry.stripe).toContain('linear-gradient');
    expect(geometry.stripe).toContain('0.42');
    expect(geometry.stripeColor).toBe('rgba(0, 0, 0, 0)');
    expect(geometry.stripeShadow).toBe('none');
    expect(geometry.paddingInline).toBeLessThan(3);
    expect(geometry.scrollHeight).toBeLessThanOrEqual(geometry.clientHeight);

    await open(run.page, 'b', 'weather_water_demo');
    const actions = await run.page.evaluate(() => {
      const next = document.querySelector('[data-public-next]') as HTMLButtonElement;
      next.hidden = false;
      const cta = document.querySelector('[data-scenario-cta]')!;
      const nextStyle = getComputedStyle(next);
      const ctaStyle = getComputedStyle(cta);
      const lineYs = [...document.querySelectorAll('[data-reveal-fragment]')].flatMap((node) => [...node.getClientRects()].map((rect) => Math.round(rect.y)));
      return { nextBackground: nextStyle.backgroundColor, nextColor: nextStyle.color, nextFont: Number.parseFloat(nextStyle.fontSize), ctaFont: Number.parseFloat(ctaStyle.fontSize), lineCount: new Set(lineYs).size };
    });
    expect(actions.nextBackground).toBe('rgba(0, 0, 0, 0)');
    expect(actions.nextFont).toBeLessThan(actions.ctaFont);
    expect(actions.lineCount).toBeLessThanOrEqual(3);
    await run.context.close();
  }
});

test('text-only briefing uses a full-viewport wash and a consistent desktop bottom anchor', async ({ browser }) => {
  for (const viewport of [{ width: 1366, height: 768 }, { width: 1440, height: 900 }]) {
    const run = await freshPage(browser, viewport);
    for (const scenario of ['weekend_count', 'weather_water_demo', 'frequently_forwarded', 'festival_demo']) {
      await open(run.page, 'b', scenario);
      const geometry = await run.page.evaluate(() => {
        const stageNode = document.querySelector('[data-briefing-slot]') as HTMLElement;
        const stage = stageNode.getBoundingClientRect();
        const actions = document.querySelector('.briefing-stage__actions')!.getBoundingClientRect();
        const wash = getComputedStyle(stageNode, '::before');
        return {
          stage: stage.toJSON(),
          actionBottomGap: stage.bottom - actions.bottom,
          washWidth: Number.parseFloat(wash.width),
          washBackground: wash.backgroundImage,
          bodyWidth: document.body.scrollWidth,
          innerWidth,
        };
      });
      expect(geometry.stage.width).toBeLessThan(geometry.innerWidth);
      expect(Math.abs(geometry.washWidth - geometry.innerWidth)).toBeLessThanOrEqual(1);
      expect(geometry.washBackground).not.toBe('none');
      expect(geometry.actionBottomGap).toBeGreaterThanOrEqual(52);
      expect(geometry.actionBottomGap).toBeLessThanOrEqual(78);
      expect(geometry.bodyWidth).toBe(geometry.innerWidth);
    }
    await run.context.close();
  }
});
