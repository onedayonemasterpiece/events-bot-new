import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { chromium } from 'playwright';
import preview from '../src/data/preview-events.json' with { type:'json' };

const baseUrl = process.env.R12_TODAY_BASE_URL || 'http://127.0.0.1:4173';
const screenshotDir = process.env.R12_SCREENSHOT_DIR
  ? resolve(process.env.R12_SCREENSHOT_DIR)
  : null;
const browser = await chromium.launch({
  headless:true,
  ...(process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH
    ? { executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH }
    : {}),
});

const eventById = (id) => preview.events.find((event) => event.id === id);
const startedEvent = eventById(7018);
const endedEvent = eventById(6956);
const futureEvent = eventById(7043);

assert.deepEqual(
  {
    started:[startedEvent?.start_date, startedEvent?.starts_at, startedEvent?.end_at],
    ended:[endedEvent?.start_date, endedEvent?.starts_at, endedEvent?.end_at],
    future:[futureEvent?.start_date, futureEvent?.starts_at, futureEvent?.end_at],
  },
  {
    started:['2026-07-26', '2026-07-26T10:00:00+02:00', null],
    ended:['2026-07-26', '2026-07-26T12:00:00+02:00', '2026-07-26T14:00:00+02:00'],
    future:['2026-07-26', '2026-07-26T19:00:00+02:00', null],
  },
  'real 2026-07-26 temporal canaries changed',
);

async function openToday(width, isoTime, expectedListingDate = '2026-07-26') {
  const context = await browser.newContext({ viewport:{ width, height:width <= 720 ? 844 : 900 } });
  const page = await context.newPage();
  const runtimeErrors = [];
  page.on('pageerror', (error) => runtimeErrors.push(String(error)));
  await page.clock.install({ time:new Date(isoTime) });
  await page.goto(`${baseUrl}/segodnya/`, { waitUntil:'domcontentloaded' });
  await page.waitForFunction((date) => document.querySelector('[data-mobile-listing-rails]')?.getAttribute('data-mobile-listing-date') === date, expectedListingDate);
  assert.equal(
    await page.locator('[data-mobile-listing-rails]').getAttribute('data-mobile-listing-date'),
    expectedListingDate,
    'serve with PUBLIC_STATIC_SITE_CURRENT_DATE=2026-07-26',
  );
  return { context, page, runtimeErrors };
}

async function visualState(row) {
  await row.waitFor();
  await row.scrollIntoViewIfNeeded();
  const media = row.locator('.event-media');
  await media.waitFor();
  await row.page().waitForFunction(
    (id) => document.querySelector(`[data-mobile-listing-row][data-event-id="${id}"] .event-media`)?.dataset.mediaState === 'loaded',
    await row.getAttribute('data-event-id'),
  );
  await row.page().waitForTimeout(250);
  return row.evaluate((node) => {
    const image = node.querySelector('.event-media > img');
    return {
      temporalState:node.dataset.mobileRailTemporalState,
      rowFilter:getComputedStyle(node).filter,
      mediaFilter:getComputedStyle(image).filter,
      mediaOpacity:getComputedStyle(image).opacity,
    };
  });
}

{
  // 13:30 Kaliningrad: 10:00 has been behind the clock by more than an
  // hour, 12:00–14:00 is still running, and 19:00 is future.
  const { context, page, runtimeErrors } = await openToday(390, '2026-07-26T11:30:00.000Z');
  const startedRow = page.locator('[data-mobile-listing-row][data-event-id="7018"]');
  const endedRow = page.locator('[data-mobile-listing-row][data-event-id="6956"]');
  const futureRow = page.locator('[data-mobile-listing-row][data-event-id="7043"]');

  assert.deepEqual(await visualState(startedRow), {
    temporalState:'started-earlier',
    rowFilter:'none',
    mediaFilter:'grayscale(0.72) saturate(0.32)',
    mediaOpacity:'0.46',
  });
  assert.deepEqual(await visualState(endedRow), {
    temporalState:'current',
    rowFilter:'none',
    mediaFilter:'none',
    mediaOpacity:'1',
  });
  assert.deepEqual(await visualState(futureRow), {
    temporalState:'current',
    rowFilter:'none',
    mediaFilter:'none',
    mediaOpacity:'1',
  });

  // Advance to 14:30 Kaliningrad without rebuilding. The real explicit end
  // has elapsed; the future row stays vivid.
  await page.clock.fastForward(60 * 60_000);
  await page.waitForTimeout(250);
  assert.deepEqual(await visualState(endedRow), {
    temporalState:'past',
    rowFilter:'none',
    mediaFilter:'grayscale(0.72) saturate(0.32)',
    mediaOpacity:'0.46',
  });
  assert.deepEqual(await visualState(futureRow), {
    temporalState:'current',
    rowFilter:'none',
    mediaFilter:'none',
    mediaOpacity:'1',
  });

  if (screenshotDir) {
    await mkdir(screenshotDir, { recursive:true });
    await endedRow.scrollIntoViewIfNeeded();
    await endedRow.screenshot({ path:resolve(screenshotDir, 'today-ended-6956-mobile.png') });
    await futureRow.scrollIntoViewIfNeeded();
    await futureRow.screenshot({ path:resolve(screenshotDir, 'today-future-7043-mobile.png') });
  }
  assert.deepEqual(runtimeErrors, []);
  await context.close();
}

{
  // At 00:30 Kaliningrad the immutable /segodnya/ review is yesterday.
  // Because the generated manifest proves 27 July exists, it must replace the
  // stale route with the honest date page instead of relabelling old rows as
  // today's programme.
  const { context, page, runtimeErrors } = await openToday(
    390,
    '2026-07-26T22:30:00.000Z',
    '2026-07-27',
  );
  assert.match(page.url(), /\/date-2026-07-27\/$/u);
  assert.notEqual(await page.locator('.page-head h1').textContent(), 'Сегодня');
  assert.deepEqual(runtimeErrors, []);
  await context.close();
}

{
  // If the real Kaliningrad date has no generated event route, fail closed:
  // keep the immutable page reachable but visibly identify its build date.
  const { context, page, runtimeErrors } = await openToday(390, '2026-08-02T22:30:00.000Z');
  const guard = page.locator('[data-today-review-guard]');
  await guard.waitFor({ state:'visible' });
  assert.equal(await guard.getAttribute('data-today-review-state'), 'stale');
  assert.equal(await guard.getAttribute('data-runtime-date'), '2026-08-03');
  assert.match(await guard.textContent(), /сохранённая версия/u);
  assert.match(await page.locator('.page-head h1').textContent(), /26 июля 2026/u);
  assert.match(page.url(), /\/segodnya\/$/u);
  assert.deepEqual(runtimeErrors, []);
  await context.close();
}

{
  // The mobile temporal class may still be computed in the hidden duplicate,
  // but its visual treatment remains scoped to <=720px and cannot leak into
  // desktop.
  const { context, page, runtimeErrors } = await openToday(1366, '2026-07-26T12:30:00.000Z');
  const mobileEndedRow = page.locator('[data-mobile-listing-row][data-event-id="6956"]');
  await mobileEndedRow.waitFor({ state:'attached' });
  const desktopIsolation = await mobileEndedRow.evaluate((node) => {
    const image = node.querySelector('.event-media > img');
    return {
      surfaceDisplay:getComputedStyle(node.closest('[data-mobile-listing-rails]')).display,
      temporalState:node.dataset.mobileRailTemporalState,
      mediaFilter:getComputedStyle(image).filter,
      mediaOpacity:getComputedStyle(image).opacity,
    };
  });
  assert.deepEqual(desktopIsolation, {
    surfaceDisplay:'none',
    temporalState:'past',
    mediaFilter:'none',
    mediaOpacity:'1',
  });
  assert.deepEqual(runtimeErrors, []);
  await context.close();
}

await browser.close();
console.log('Today temporal media Playwright acceptance passed for real 2026-07-26 events');
