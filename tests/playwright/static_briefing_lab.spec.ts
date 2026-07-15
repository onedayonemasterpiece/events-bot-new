import { test, expect, chromium, type Page } from '@playwright/test';
import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const distRoot = path.join(repoRoot, 'site/dist-lab');
const manifests = fs.existsSync(distRoot)
  ? fs.readdirSync(distRoot).map((name) => path.join(distRoot, name, 'lab-manifest.json')).filter(fs.existsSync)
  : [];
if (!manifests.length) throw new Error('Run `cd site && npm run build:lab && npm run check:lab` first');
const manifestPath = manifests.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs)[0];
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const buildId = manifest.buildId as string;
const port = 4187;
const baseUrl = `http://127.0.0.1:${port}/${buildId}/lab/briefing/`;
const scenarios = ['today_count', 'tomorrow_count', 'weekend_count', 'exhibitions_count', 'free_count', 'tonight_count', 'newly_added_count', 'catalog_generic', 'neutral_fallback'];
const viewports = [{ width: 320, height: 568 }, { width: 375, height: 667 }, { width: 390, height: 844 }, { width: 1440, height: 900 }];
let server: ChildProcess;

async function waitForServer() {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try { if ((await fetch(baseUrl)).ok) return; } catch (_) {}
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Preview server did not start: ${baseUrl}`);
}

async function open(page: Page, variant: string, scenario = 'today_count') {
  await page.goto(`${baseUrl}?variant=${variant}&scenario=${scenario}`, { waitUntil: 'domcontentloaded' });
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-variant', variant === 'a' ? 'control' : variant === 'b' ? 'static' : variant === 'c' ? 'reveal' : variant);
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', scenario);
}

async function serializedSurface(page: Page) {
  return page.evaluate(() => ({
    fixtures: [...document.querySelectorAll('[data-briefing-event-id]')].map((node) => node.getAttribute('data-briefing-event-id')),
    categories: [...document.querySelectorAll('[data-category-route]')].map((node) => [node.textContent?.trim(), node.getAttribute('href')]),
  }));
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

test('all scenarios fit B/C across the required viewport matrix and retain production geometry', async ({ browser }, testInfo) => {
  test.setTimeout(150_000);
  const geometry = [];
  for (const viewport of viewports) {
    for (const scenario of scenarios) {
      const context = await browser.newContext({ viewport });
      const page = await context.newPage();
      await page.route('**/*', (route) => route.request().resourceType() === 'image' ? route.abort() : route.continue());
      let staticBriefing;
      for (const variant of ['b', 'c']) {
        await open(page, variant, scenario);
        if (variant === 'b') {
          await page.waitForTimeout(300);
        } else {
          await page.dispatchEvent('[data-briefing]', 'pointerdown');
          await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 3000 });
        }
        const result = await page.evaluate(() => {
          const rect = (selector) => {
            const box = document.querySelector(selector)?.getBoundingClientRect();
            return box ? { x: box.x, y: box.y, width: box.width, height: box.height, bottom: box.bottom } : null;
          };
          const briefing = document.querySelector('[data-briefing]');
          const shell = document.querySelector('[data-lab-page-shell]');
          const header = document.querySelector('.site-header__inner');
          const card = document.querySelector('.listing-item');
          const title = document.querySelector('.listing-item__title');
          const decision = document.querySelector('.listing-item__body');
          const styles = getComputedStyle(briefing);
          return {
            fitsY: briefing.scrollHeight <= briefing.clientHeight,
            fitsX: briefing.scrollWidth <= briefing.clientWidth,
            shell: rect('[data-lab-page-shell]'), header: rect('.site-header__inner'), card: rect('.listing-item'), title: rect('.listing-item__title'), decision: rect('.listing-item__body'), briefing: rect('[data-briefing]'),
            fontFamily: styles.fontFamily, primary: styles.getPropertyValue('--primary').trim(), cardMinHeight: getComputedStyle(card).minHeight,
          };
        });
        expect(result.fitsY, `${scenario} ${viewport.width}x${viewport.height} vertical`).toBeTruthy();
        expect(result.fitsX, `${scenario} ${viewport.width}x${viewport.height} horizontal`).toBeTruthy();
        const expectedShell = viewport.width <= 560 ? viewport.width - 24 : 1180;
        expect(result.shell!.width).toBeCloseTo(expectedShell, 0);
        expect(result.header!.height).toBe(viewport.width <= 560 ? 64 : 56);
        expect(Number.parseFloat(result.cardMinHeight)).toBeGreaterThanOrEqual(viewport.width <= 560 ? 154 : 168);
        expect(result.fontFamily).toContain('Inter');
        expect(result.primary).toBe('#a54821');
        if (variant === 'b') staticBriefing = result.briefing;
        else expect(result.briefing).toEqual(staticBriefing);
        geometry.push({ viewport, scenario, variant, firstCard: result.card, firstTitle: result.title, firstDecision: result.decision });
      }
      await context.close();
    }
  }
  await testInfo.attach('first-production-card-geometry.json', { body: JSON.stringify(geometry, null, 2), contentType: 'application/json' });
});

test('A/B/C share exact fixtures/categories; links and local telemetry stay lab-safe', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const requests: Array<{ method: string; type: string; url: string }> = [];
  const failures: string[] = [];
  page.on('request', (request) => requests.push({ method: request.method(), type: request.resourceType(), url: request.url() }));
  page.on('response', (response) => { if (response.status() >= 400) failures.push(`${response.status()} ${response.url()}`); });
  await page.addInitScript(() => {
    window.__labBeacons = [];
    const original = navigator.sendBeacon?.bind(navigator);
    navigator.sendBeacon = (url, data) => { window.__labBeacons.push(String(url)); return original ? false : false; };
  });
  const surfaces = [];
  for (const variant of ['a', 'b', 'c']) {
    await open(page, variant);
    surfaces.push(await serializedSurface(page));
  }
  expect(surfaces[1]).toEqual(surfaces[0]);
  expect(surfaces[2]).toEqual(surfaces[0]);
  expect(surfaces[0].fixtures).toEqual(['6607', '5373', '6020']);
  expect(JSON.stringify(surfaces)).not.toContain('6045');
  expect(surfaces[0].categories).toEqual([['Сегодня', '/segodnya/'], ['Завтра', '/zavtra/'], ['Выходные', '/vyhodnye/'], ['Выставки', '/vystavki/'], ['Популярное', '/populyarnoe/']]);
  await expect(page.locator('[data-scenario-cta]')).toHaveAttribute('href', '#events');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 3000 });
  await page.locator('.listing-item__title').first().click();
  const log = await page.evaluate(() => window.__briefingTelemetry);
  expect(log.some((item) => item.event_kind === 'briefing_impression' && item.visible_ms === 250 && item.visible_ratio >= 0.5)).toBeTruthy();
  expect(log.some((item) => item.event_kind === 'first_event_visible' && item.visible_ms === 250 && item.visible_ratio >= 0.9)).toBeTruthy();
  expect(log.some((item) => item.event_kind === 'event_detail_activate' && item.event_id === '6607')).toBeTruthy();
  expect(log.some((item) => item.event_kind === 'event_detail_open')).toBeFalsy();
  await page.locator('[data-briefing-debug] summary').click();
  const download = page.waitForEvent('download');
  await page.locator('[data-export-telemetry]').click();
  expect((await download).suggestedFilename()).toMatch(/^briefing-lab-reveal-today_count\.json$/u);
  const assetStatuses = await page.evaluate(async (prefix) => Promise.all([
    fetch(`${prefix}/favicon.svg`).then((response) => response.status),
    fetch(`${prefix}/brand/announcements-wordmark-ui.svg`).then((response) => response.status),
  ]), `/${buildId}`);
  expect(assetStatuses).toEqual([200, 200]);
  expect(failures).toEqual([]);
  expect(requests.some((item) => item.method !== 'GET' || item.type === 'xhr' || /supabase|analytics|telemetry/iu.test(item.url))).toBeFalsy();
  expect(await page.evaluate(() => window.__labBeacons)).toEqual([]);
});

test('no-JS, reduced motion, input interruption and session non-replay are preserved', async ({ browser }) => {
  const noJs = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 320, height: 568 } });
  const noJsPage = await noJs.newPage();
  await noJsPage.goto(`${baseUrl}?variant=c&scenario=exhibitions_count`);
  await expect(noJsPage.locator('[data-briefing]')).toContainText('Сегодня — события на любой план');
  await expect(noJsPage.locator('.site-header')).toBeVisible();
  await expect(noJsPage.locator('.listing-item')).toHaveCount(3);
  expect(await noJsPage.locator('[data-briefing]').evaluate((node) => node.scrollHeight <= node.clientHeight && node.scrollWidth <= node.clientWidth)).toBeTruthy();
  await noJs.close();

  const reduced = await browser.newContext({ reducedMotion: 'reduce', viewport: { width: 1440, height: 900 } });
  const reducedPage = await reduced.newPage();
  await open(reducedPage, 'c', 'tomorrow_count');
  await expect(reducedPage.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await expect.poll(() => reducedPage.evaluate(() => window.__briefingTelemetry.some((item) => item.event_kind === 'briefing_complete' && item.completion_reason === 'reduced_motion'))).toBeTruthy();
  await reduced.close();

  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await context.newPage();
  await open(page, 'c', 'weekend_count');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', /ready|running/u);
  await page.dispatchEvent('[data-briefing]', 'pointerdown');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await expect.poll(() => page.evaluate(() => window.__briefingTelemetry.some((item) => item.event_kind === 'briefing_interrupt' && item.interrupt_reason === 'pointerdown'))).toBeTruthy();
  await page.reload();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await page.waitForTimeout(1100);
  expect(await page.evaluate(() => window.__briefingTelemetry.filter((item) => item.event_kind === 'briefing_complete').length)).toBe(0);
  await context.close();

  for (const [reason, scenario] of [['focusin', 'free_count'], ['scroll', 'tonight_count']] as const) {
    const inputContext = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const inputPage = await inputContext.newPage();
    await open(inputPage, 'c', scenario);
    await expect(inputPage.locator('[data-briefing]')).toHaveAttribute('data-motion', /ready|running/u);
    if (reason === 'focusin') await inputPage.locator('[data-scenario-cta]').focus();
    else await inputPage.evaluate(() => window.scrollBy(0, 120));
    await expect(inputPage.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
    await expect.poll(() => inputPage.evaluate((expected) => window.__briefingTelemetry.some((item) => item.event_kind === 'briefing_interrupt' && item.interrupt_reason === expected), reason)).toBeTruthy();
    await inputContext.close();
  }
});
