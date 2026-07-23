import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { chromium } from 'playwright';

const baseUrl = process.env.R9_RAIL_BASE_URL || 'http://127.0.0.1:4173';
const asset = await readFile(new URL('../public/assets/gamification/amber-cosmonaut-1x.webp', import.meta.url));
const browser = await chromium.launch({ headless: true });

function closeEnough(actual, expected, tolerance = 1) {
  assert.ok(Math.abs(actual - expected) <= tolerance, `expected ${actual} to be within ${tolerance}px of ${expected}`);
}

async function contextFor(width, { failRemoteImages = false, delayRemoteImages = 0, reducedMotion = 'no-preference', hasTouch = false } = {}) {
  const context = await browser.newContext({ viewport: { width, height: width === 320 ? 700 : 844 }, reducedMotion, hasTouch });
  await context.route('**/*', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (request.resourceType() === 'image' && url.hostname === 'static.kenigevents.ru') {
      if (failRemoteImages) return route.abort('failed');
      if (delayRemoteImages) await new Promise((resolve) => setTimeout(resolve, delayRemoteImages));
      return route.fulfill({
        status: 200,
        contentType: 'image/webp',
        body: asset,
        headers: { 'cache-control': 'public,max-age=3600' },
      });
    }
    return route.continue();
  });
  return context;
}

async function assertCoreGeometry(page, route, width) {
  await page.goto(`${baseUrl}${route}`, { waitUntil: 'domcontentloaded' });
  const row = page.locator('.event-row').first();
  await row.waitFor();
  const geometry = await page.locator('.event-row, .rail-window, .event-summary').evaluateAll((nodes) =>
    nodes.slice(0, 3).map((node) => {
      const rect = node.getBoundingClientRect();
      return { className: node.className, width: rect.width, height: rect.height };
    }));
  closeEnough(geometry[0].height, 112);
  closeEnough(geometry[1].width, width);
  closeEnough(geometry[1].height, 112);
  closeEnough(geometry[2].width, 296);
  closeEnough(geometry[2].height, 112);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), width);

  const arrow = await page.locator('.event-cue').first().evaluate((node) => {
    const rect = node.getBoundingClientRect();
    return { width: rect.width, height: rect.height, path: node.querySelector('path')?.getAttribute('d') };
  });
  closeEnough(arrow.width, 48);
  closeEnough(arrow.height, 23);
  assert.equal(arrow.path, 'M3 11.5H40M31 2.5L40 11.5L31 20.5');
}

async function assertStickyHierarchy(page, route, popular = false) {
  await page.goto(`${baseUrl}${route}`, { waitUntil: 'domcontentloaded' });
  const feedPosition = await page.locator('.feed-head').evaluate((node) => getComputedStyle(node).position);
  assert.equal(feedPosition, 'static');
  await page.locator('.group-head').first().evaluate((node) => {
    const naturalTop = node.getBoundingClientRect().top + scrollY;
    scrollTo(0, naturalTop + 80);
  });
  await page.waitForFunction(() => document.body.classList.contains('is-date-pinned'));
  const boxes = await page.evaluate(() => {
    const sticky = document.querySelector('[data-mobile-listing-sticky-title]').getBoundingClientRect();
    const group = document.querySelector('.group-head').getBoundingClientRect();
    return {
      sticky: { top: sticky.top, height: sticky.height, visibility: getComputedStyle(document.querySelector('[data-mobile-listing-sticky-title]')).visibility },
      group: { top: group.top, height: group.height },
    };
  });
  closeEnough(boxes.sticky.top, 0);
  closeEnough(boxes.sticky.height, 64);
  assert.equal(boxes.sticky.visibility, 'visible');
  closeEnough(boxes.group.top, 64);
  if (popular) closeEnough(boxes.group.height, 80);
}

for (const width of [320, 390]) {
  const context = await contextFor(width);
  const page = await context.newPage();
  for (const route of ['/segodnya/', '/vyhodnye/', '/populyarnoe/']) {
    await assertCoreGeometry(page, route, width);
  }
  await assertStickyHierarchy(page, '/vyhodnye/');
  await assertStickyHierarchy(page, '/populyarnoe/', true);

  await page.goto(`${baseUrl}/vyhodnye/`, { waitUntil: 'domcontentloaded' });
  const dateGeometry = await page.locator('[data-mobile-date-accessory]').evaluate((node) => {
    const rect = node.getBoundingClientRect();
    const selected = node.querySelector('[aria-current="date"]').getBoundingClientRect();
    const rail = node.querySelector('.date-rail').getBoundingClientRect();
    return {
      top: rect.top, bottom: rect.bottom, height: rect.height,
      chipCount: node.querySelectorAll('.date-chip').length,
      selectedCenter: selected.left + selected.width / 2,
      railCenter: rail.left + rail.width / 2,
    };
  });
  closeEnough(dateGeometry.bottom, (width === 320 ? 700 : 844) - 64);
  closeEnough(dateGeometry.height, 56);
  assert.equal(dateGeometry.chipCount, 42);
  assert.ok(Math.abs(dateGeometry.selectedCenter - dateGeometry.railCenter) < 42);
  const dateHrefStatuses = await page.locator('[data-mobile-date-accessory] a[href]').evaluateAll(async (links) => {
    const hrefs = [...new Set(links.map((link) => link.href))];
    return Promise.all(hrefs.map(async (href) => ({ href, status: (await fetch(href)).status })));
  });
  assert.equal(await page.locator('[data-mobile-date-accessory] .date-rail a[href]').count(), 42);
  assert.deepEqual(dateHrefStatuses.filter(({ status }) => status !== 200), []);
  assert.equal(await page.locator('[data-mobile-date-accessory] [aria-disabled="true"]').count(), 0);
  await page.locator('[data-calendar-open]').click();
  await page.locator('[data-calendar-sheet]:not([hidden]) .calendar-panel').waitFor();
  assert.equal(await page.locator('[data-calendar-sheet] [data-calendar-date]').count(), 42);

  await context.close();
}

{
  const context = await contextFor(390, { delayRemoteImages: 5000 });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/segodnya/`, { waitUntil: 'domcontentloaded' });
  const shell = page.locator('.event-media').first();
  await shell.waitFor();
  const before = await shell.boundingBox();
  assert.equal(await shell.getAttribute('data-media-state'), 'loading');
  await page.waitForFunction(() => document.querySelector('.event-media')?.dataset.mediaState === 'loaded');
  const after = await shell.boundingBox();
  closeEnough(before.height, after.height);
  closeEnough(before.width, after.width);
  await page.reload({ waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => document.querySelector('.event-media')?.dataset.mediaState === 'loaded');
  await context.close();
}

{
  const context = await contextFor(390, { failRemoteImages: true });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/segodnya/`, { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => document.querySelector('.event-media')?.dataset.mediaState === 'error');
  const state = await page.locator('.event-media').first().evaluate((node) => ({
    height: node.getBoundingClientRect().height,
    background: getComputedStyle(node).backgroundColor,
  }));
  closeEnough(state.height, 112);
  assert.notEqual(state.background, 'rgba(0, 0, 0, 0)');
  await context.close();
}

{
  const context = await contextFor(390);
  const page = await context.newPage();
  await page.goto(`${baseUrl}/vyhodnye/`, { waitUntil: 'domcontentloaded' });
  assert.equal(await page.locator('[data-amber-artifact-research="tail"]').count(), 1);
  const row = page.locator('[data-mobile-listing-row][data-event-id="6939"]');
  const rail = row.locator('.rail-window');
  await rail.evaluate((node) => { node.scrollLeft = node.scrollWidth; });
  const order = await row.locator('.event-track').evaluate((node) =>
    [...node.children]
      .filter((child) => child.matches('.event-link,.event-like-cta,[data-amber-artifact]'))
      .map((child) => child.matches('.event-link') ? 'link' : child.matches('.event-like-cta') ? 'like' : 'artifact'));
  assert.deepEqual(order.slice(-2), ['like', 'artifact']);
  const artifactBox = await row.locator('[data-amber-artifact]').boundingBox();
  closeEnough(artifactBox.width, 94);
  closeEnough(artifactBox.height, 112);
  await page.waitForFunction(() => document.querySelector('[data-event-id="6939"] [data-amber-artifact]')?.classList.contains('is-awake'));
  await row.locator('[data-amber-artifact]').click();
  assert.equal(await row.locator('[data-amber-artifact]').getAttribute('aria-pressed'), 'true');
  assert.equal(await page.evaluate(() => localStorage.getItem('ke_amber_artifact_prototype_v1:tail')), 'found');
  await context.close();
}

{
  const context = await contextFor(390, { reducedMotion: 'reduce' });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/vyhodnye/`, { waitUntil: 'domcontentloaded' });
  const animation = await page.locator('[data-amber-artifact]').evaluate((node) => {
    node.closest('.rail-window').scrollLeft = node.closest('.rail-window').scrollWidth;
    return getComputedStyle(node.querySelector('.amber-artifact__visual')).animationName;
  });
  assert.equal(animation, 'none');
  await context.close();
}

{
  const context = await contextFor(390);
  const page = await context.newPage();
  for (const canary of [
    { route:'/date-2026-07-24/', id:'5296', width:140, reason:'single_safe_visual_landscape_5x4', mode:'visual_only' },
    { route:'/vyhodnye/', id:'6939', width:90, reason:'reviewed_multi_visual_portrait_4x5', mode:'visual_only' },
  ]) {
    await page.goto(`${baseUrl}${canary.route}`, { waitUntil:'domcontentloaded' });
    const row = page.locator(`[data-mobile-listing-row][data-event-id="${canary.id}"]`);
    await row.waitFor();
    const media = await row.locator('.event-media').evaluate((node) => ({
      width: node.getBoundingClientRect().width,
      height: node.getBoundingClientRect().height,
      fit: getComputedStyle(node.querySelector('img')).objectFit,
      reason: node.dataset.railMediaReason,
      mode: node.dataset.imageTextMode,
    }));
    closeEnough(media.width, canary.width);
    closeEnough(media.height, 112);
    assert.equal(media.fit, 'cover');
    assert.equal(media.reason, canary.reason);
    assert.equal(media.mode, canary.mode);
  }

  await page.goto(`${baseUrl}/date-2026-08-08/`, { waitUntil:'domcontentloaded' });
  const more = page.locator('[data-mobile-listing-row][data-event-id="4211"]');
  await more.waitFor();
  assert.equal(await more.locator('img[src*="/assets/festivals/more-vnutri.svg"]').count(), 1);
  assert.equal(await more.locator('.event-media').getAttribute('data-image-text-mode'), 'ocr_text');
  assert.equal(await more.locator('.event-medallion-slot').evaluate((node) => Boolean(node.closest('.event-media'))), false);
  await context.close();
}

{
  const context = await contextFor(390);
  const page = await context.newPage();
  await page.goto(`${baseUrl}/date-2026-07-24/`, { waitUntil:'domcontentloaded' });
  const row = page.locator('[data-mobile-listing-row][data-event-id="5296"]');
  const rail = row.locator('.rail-window');
  const like = row.locator('[data-like]');
  assert.equal(await like.locator('.icon__heart-outline').count(), 1);
  assert.equal(await like.locator('.icon__heart-solid').count(), 1);

  await row.scrollIntoViewIfNeeded();
  const box = await rail.boundingBox();
  const initialUrl = page.url();
  await page.mouse.move(box.x + 42, box.y + 56);
  await page.mouse.down();
  await page.mouse.move(box.x + 350, box.y + 56, { steps:8 });
  assert.equal(await rail.evaluate((node) => node.classList.contains('is-armed')), true);
  assert.ok(Number(await rail.evaluate((node) => getComputedStyle(node).getPropertyValue('--dislike-progress'))) >= .86);
  await page.mouse.up();
  await page.locator('[data-rail-confirm].is-open').waitFor();
  assert.equal(page.url(), initialUrl);
  await page.locator('[data-rail-confirm-cancel]').click();
  await page.locator('[data-rail-confirm]').waitFor({ state:'hidden' });

  await rail.evaluate((node) => { node.scrollLeft = node.scrollWidth; });
  await like.evaluate((button) => {
    button.addEventListener('click', (event) => {
      event.stopImmediatePropagation();
      button.setAttribute('aria-pressed', 'true');
    }, { capture:true, once:true });
  });
  await page.mouse.move(box.x + 280, box.y + 56);
  await page.mouse.down();
  await page.mouse.move(box.x + 130, box.y + 56, { steps:8 });
  assert.equal(await rail.evaluate((node) => node.classList.contains('is-like-armed')), true);
  await page.mouse.up();
  await page.waitForFunction(() => document.querySelector('[data-event-id="5296"] [data-like]')?.getAttribute('aria-pressed') === 'true');
  const heartDisplay = await like.evaluate((button) => ({
    outline: getComputedStyle(button.querySelector('.icon__heart-outline')).display,
    solid: getComputedStyle(button.querySelector('.icon__heart-solid')).display,
  }));
  assert.equal(heartDisplay.outline, 'none');
  assert.notEqual(heartDisplay.solid, 'none');
  assert.equal(page.url(), initialUrl);
  await context.close();
}

{
  const context = await contextFor(390, { hasTouch:true });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/date-2026-07-24/`, { waitUntil:'domcontentloaded' });
  const row = page.locator('[data-mobile-listing-row][data-event-id="5296"]');
  const rail = row.locator('.rail-window');
  await rail.evaluate((node) => {
    const touch = (x) => new Touch({ identifier:1, target:node, clientX:x, clientY:56, pageX:x, pageY:56 });
    node.dispatchEvent(new TouchEvent('touchstart', { touches:[touch(22)], targetTouches:[touch(22)], changedTouches:[touch(22)], bubbles:true }));
    node.dispatchEvent(new TouchEvent('touchmove', { touches:[touch(372)], targetTouches:[touch(372)], changedTouches:[touch(372)], bubbles:true, cancelable:true }));
    node.dispatchEvent(new TouchEvent('touchend', { touches:[], targetTouches:[], changedTouches:[touch(372)], bubbles:true }));
  });
  await page.locator('[data-rail-confirm].is-open').waitFor();
  await page.locator('[data-rail-confirm-cancel]').click();
  await context.close();
}

await browser.close();
console.log('mobile listing rails Playwright acceptance passed at 320px and 390px');
