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
const scenarioIds = ['today_count', 'tomorrow_count', 'weekend_count', 'share_education', 'like_education', 'not_interested_education', 'frequently_forwarded', 'anticipated_person'];
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

test('all eight scenarios are discoverable; Replay, session state, Play all and Pause are explicit', async ({ browser }) => {
  const { context, page } = await freshPage(browser, { width: 1440, height: 900 });
  await open(page, 'c', 'today_count', '&replay=1&pace=fast');
  const options = await page.locator('[data-scenario-select] option').evaluateAll((nodes) => nodes.map((node) => (node as HTMLOptionElement).value));
  expect(options.slice(0, 8)).toEqual(scenarioIds);
  expect(new Set(options).size).toBe(9); // eight scenarios + the explicit fallback

  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });
  await page.reload({ waitUntil: 'domcontentloaded' });
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete');
  await expect(page.locator('[data-playback-status]')).toContainText('Уже показано');
  await page.locator('[data-replay]').click();
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'running');
  await expect(page.locator('[data-briefing]')).toHaveAttribute('data-motion', 'complete', { timeout: 2500 });

  await page.locator('[data-scenario-select]').selectOption('share_education');
  await expect(page.locator('[data-briefing-lab]')).toHaveAttribute('data-briefing-scenario-id', 'share_education');
  await expect(page).toHaveURL(/scenario=newly_added_count/u);
  await expect(page.locator('[data-message]')).toContainText('то самое');

  await page.locator('[data-play-all]').click();
  await expect.poll(async () => (await labState(page)).scenario, { timeout: 6000 }).toBe('tomorrow_count');
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
            stageFits: stage.scrollHeight <= stage.clientHeight && stage.scrollWidth <= stage.clientWidth,
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
  await page.waitForTimeout(100);
  await page.locator('[data-briefing-slot]').hover();
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
